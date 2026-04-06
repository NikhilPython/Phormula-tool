from __future__ import annotations
import json
from typing import Any, Dict, List, Optional
from langgraph.graph import END, StateGraph
from pydantic import BaseModel, Field
from langchain_openai import ChatOpenAI
import re
from datetime import datetime
import pandas as pd

from app.ai_agent.email_service import build_summary_html, send_agent_email
from app.ai_agent.db import get_engine,latest_available_month
from app.ai_agent.formula_engine import parse_period,get_metric_for_month,get_metric_for_period,get_metric_last_n_months,compare_periods,pick_top_skus
from app.ai_agent.prompts import ADVISOR_SYSTEM_PROMPT, PLANNER_SYSTEM_PROMPT
from app.ai_agent.state import AgentState
from app.models.user_models import User
from dotenv import load_dotenv
load_dotenv()
import os


OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

class Period(BaseModel):
    start: str
    end: str

    class Config:
        extra = "forbid"

class PlannerResult(BaseModel):
    intent: str = "metric_qa"
    metric_name: str = "profit"
    period_mode: str = "latest_completed_month"
    months_back: int | None = None
    needs_sku: bool = False
    needs_advice: bool = False
    response_mode: str = "short"
    email_requested: bool = False

    custom_range: bool = False
    period_1: Optional[Period] = None
    period_2: Optional[Period] = None

    class Config:
        extra = "forbid"


class AdviceResult(BaseModel):
    advice: List[str]


_llm = ChatOpenAI(model="gpt-4.1", api_key=OPENAI_API_KEY, temperature=0)
_planner = _llm.with_structured_output(PlannerResult)
_advisor = _llm.with_structured_output(AdviceResult)

ALIAS_MAP = {
    # 🔥 SALES
    "gross sales": "gross_sales",
    "gross revenue": "gross_sales",
    "sales before refund": "gross_sales",

    "total sales": "net_sales",
    "net sales": "net_sales",
    "sales": "net_sales",

    # 🔥 TAX / CREDIT
    "total tax": "tax",
    "taxes": "tax",

    "credits total": "credits",
    "credit": "credits",
    "net credits": "net_credits",
    "net credit": "net_credits",

    # 🔥 REIMBURSEMENT
    "reimbursement vs cm2": "reimbursement_vs_cm2_margins",
    "reimb vs cm2": "reimbursement_vs_cm2_margins",
    "reimbursement vs sales": "reimbursement_vs_sales",
    "reimb vs sales": "reimbursement_vs_sales",
    "reimbursements": "reimbursement_fee",
    "reimbursement": "reimbursement_fee",

    # 🔥 PLATFORM / ADS
    "subscription fee": "platform_fee",
    "platform": "platform_fee",

    "ads spend": "advertising_total",
    "ad spend": "advertising_total",
    "ads": "advertising_total",
    "advertising": "advertising_total",

    # 🔥 PROFIT
    "cm2 profit": "cm2_profit",
    "net profit": "cm2_profit",
    "cm2": "cm2_profit",
    "cm2 margin": "cm2_margins",

    "profit %": "profit_margin",
    "margin": "profit_margin",

    # 🔥 ACOS
    "ad cos": "acos",
    "roas": "acos",
    "acos": "acos",

    # 🔥 PRICE
    "average selling price": "asp",
    "avg selling price": "asp",

    "profit per unit": "unit_profitability",
    "ppu": "unit_profitability",

    # 🔥 MIX
    "sales share": "sales_mix",
    "profit share": "profit_mix",

    # 🔥 QUANTITY (IMPORTANT GROUP)
    "total orders": "total_quantity",
    "orders": "total_quantity",
    "ordered units": "total_quantity",
    "total units": "total_quantity",
    "units sold": "total_quantity",
    "sold units": "total_quantity",
    "net units": "total_quantity",
    "units": "total_quantity",
    "quantity sold": "total_quantity",
    "qty sold": "total_quantity",
    "net quantity": "total_quantity",

    # 🔥 FEES
    "amazon fees total": "amazon_fees",
    "amazon fees": "amazon_fees",
    "amazon fee": "amazon_fees",

    "fba fees": "fba_fees",
    "fulfillment fees": "fba_fees",
    "fulfilment fees": "fba_fees",
    "fulfillment fee": "fba_fees",
    "fulfilment fee": "fba_fees",

    "selling fees": "selling_fees",
    "selling fee": "selling_fees",
    "referral fees": "selling_fees",
    "referral fee": "selling_fees",

    # 🔥 REFUNDS
    "refund count": "refunds",
    "returns": "refunds",
    "refund": "refunds",
}


def resolve_metric_from_query(query: str, llm_metric: str = None) -> str:
    q = query.lower()

    # ✅ sort keys by length (longest first → avoids "sales" overriding "net sales")
    for phrase in sorted(ALIAS_MAP.keys(), key=len, reverse=True):
        if phrase in q:
            return ALIAS_MAP[phrase]

    # ✅ fallback to LLM (if valid)
    if llm_metric:
        return llm_metric

    # ✅ final fallback
    return "profit"


def find_best_product_match(query: str, rows: list) -> str | None:
    q = query.lower()

    products = [
        str(r.get("product_name", "")).strip().lower()
        for r in rows
        if r.get("product_name")
    ]

    # 1️⃣ exact phrase match
    for p in products:
        if p and p in q:
            return p

    # 2️⃣ word overlap match
    for p in products:
        words = p.split()
        if any(w in q for w in words):
            return p

    return None

def planner_node(state: AgentState) -> AgentState:
    history = state.get("chat_history", [])

    messages = [
        {"role": "system", "content": PLANNER_SYSTEM_PROMPT},
    ]

    for h in history:
        messages.append({"role": "user", "content": h["message"]})
        messages.append({"role": "assistant", "content": h["response"]})

    messages.append({"role": "user", "content": state["user_query"]})

    result = _planner.invoke(messages)

    # ✅ MUTATE STATE (IMPORTANT)
    state["intent"] = result.intent
    state["metric_name"] = resolve_metric_from_query(
    state["user_query"],
    result.metric_name)
    state["period_mode"] = result.period_mode
    state["months_back"] = result.months_back
    state["needs_sku"] = result.needs_sku
    state["needs_advice"] = result.needs_advice
    state["response_mode"] = result.response_mode
    state["custom_range"] = result.custom_range
    state["period_1"] = result.period_1
    state["period_2"] = result.period_2
    state["email_requested"] = bool(state.get("email_requested") or result.email_requested)

    # ✅ PARSER
    parsed = parse_period(state["user_query"])

    if not parsed or "type" not in parsed:
        parsed = {"type": "latest_month"}

    state["period_parsed"] = parsed

    print("🧠 PARSED PERIOD:", parsed)

    query = state["user_query"].lower()

    if any(word in query for word in [
        "breakdown",
        "productwise",
        "product wise",
        "all columns",
        "raw data",
        "full data",
        "export"
    ]):
        state["data_mode"] = True
    else:
        state["data_mode"] = False

    return state


def fetch_node(state: AgentState) -> AgentState:
    print("\n================= 📦 FETCH NODE =================\n")

    engine = get_engine()

    # 🔍 DEBUG: incoming state
    print("📥 Incoming state keys:", list(state.keys()))

    payload = state.get("period_parsed") or {}

    # 🔥 DEBUG: parser output
    print("🧠 RAW PARSED PERIOD:", payload)

    # -------------------------------
    # SAFETY FALLBACK
    # -------------------------------
    if not payload or "type" not in payload:
        print("⚠️ No valid parsed period, using latest_month")
        period_payload = {"type": "latest_month"}

    # -------------------------------
    # COMPARISON
    # -------------------------------
    elif payload.get("type") == "comparison":
        period_payload = {
            "type": "comparison",
            "p1": payload["left"],
            "p2": payload["right"],
        }

    # -------------------------------
    # RANGE
    # -------------------------------
    elif payload.get("type") == "range":
        period_payload = payload

    # -------------------------------
    # LAST N MONTHS
    # -------------------------------
    elif payload.get("type") == "last_n":
        period_payload = {
            "type": "last_n_months",
            "n": payload["n"],
        }

    # -------------------------------
    # SINGLE MONTH
    # -------------------------------
    elif payload.get("type") == "single":
        period_payload = payload

    # -------------------------------
    # YEAR
    # -------------------------------
    elif payload.get("type") == "year":
        period_payload = {
            "type": "range",
            "start_month": 1,
            "start_year": payload["year"],
            "end_month": 12,
            "end_year": payload["year"],
        }

    # -------------------------------
    # LATEST
    # -------------------------------
    elif payload.get("type") == "latest_month":
        period_payload = {"type": "latest_month"}

    else:
        print("⚠️ Unknown type, fallback to latest_month")
        period_payload = {"type": "latest_month"}

    # 🔥 DEBUG: final payload
    print("📦 FINAL PERIOD PAYLOAD:", period_payload)

    # 🔥 CRITICAL FIX: return NEW state (not mutate)
    state["engine"] = engine
    state["period_payload"] = period_payload

    return state



def metrics_node(state: AgentState) -> AgentState:
    print("\n================= 🔍 NSE METRICS NODE =================\n")

    # 🚨 DATA MODE BYPASS (NEW)
    if state.get("data_mode"):
        print("🟢 DATA MODE ACTIVATED")

        from app.ai_agent.db import fetch_nse_month_df, fetch_non_total_rows

        engine = state.get("engine")
        if engine is None:
            raise ValueError("Engine not found in state")

        user_id = state["user_id"]
        country = state["country"]
        payload = state.get("period_payload", {})

        if payload.get("type") == "single":
            df = fetch_nse_month_df(
                engine,
                user_id,
                country,
                payload["month"],
                payload["year"]
            )
        else:
            raise ValueError("Data mode supports single month only")

        df = fetch_non_total_rows(df)

        # store raw data
        state["raw_df"] = df.to_dict(orient="records")

        return state

    # -------------------------------
    # NORMAL METRIC MODE (UNCHANGED)
    # -------------------------------
    engine = state.get("engine")
    if engine is None:
        raise ValueError("Engine not found in state (fetch_node failed)")

    user_id = state["user_id"]
    country = state["country"]
    metric_name = state.get("metric_name", "profit")

    payload = state.get("period_payload") or {"type": "latest_month"}
    ptype = payload.get("type", "latest_month")

    print("📊 metric:", metric_name)
    print("📊 payload:", payload)

    try:
        if ptype == "latest_month":
            latest = latest_available_month(engine, user_id, country)

            result = get_metric_for_month(
                engine=engine,
                user_id=user_id,
                country=country,
                metric_name=metric_name,
                month=latest.month,
                year=latest.year,
            )

            state["current_metrics"] = result

        elif ptype == "single":
            result = get_metric_for_month(
                engine=engine,
                user_id=user_id,
                country=country,
                metric_name=metric_name,
                month=payload["month"],
                year=payload["year"],
            )

            state["current_metrics"] = result

        elif ptype == "range":
            result = get_metric_for_period(
                engine=engine,
                user_id=user_id,
                country=country,
                metric_name=metric_name,
                start_month=payload["start_month"],
                start_year=payload["start_year"],
                end_month=payload["end_month"],
                end_year=payload["end_year"],
            )

            state["current_metrics"] = result

        elif ptype == "last_n_months":
            n = payload["n"]

            current = get_metric_last_n_months(
                engine=engine,
                user_id=user_id,
                country=country,
                metric_name=metric_name,
                n=n,
                offset=0,
            )

            previous = get_metric_last_n_months(
                engine=engine,
                user_id=user_id,
                country=country,
                metric_name=metric_name,
                n=n,
                offset=n,
            )

            current_total = float(current.get("total", 0))
            previous_total = float(previous.get("total", 0))

            pct_change = (
                ((current_total - previous_total) / previous_total) * 100
                if previous_total != 0
                else None
            )

            state["comparison"] = {
                "left": current,
                "right": previous,
                "pct_change": pct_change,
            }

            state["current_metrics"] = current
            return state

        elif ptype == "comparison":
            p1 = payload["p1"]
            p2 = payload["p2"]

            left = get_metric_for_period(
                engine=engine,
                user_id=user_id,
                country=country,
                metric_name=metric_name,
                start_month=p1["start_month"],
                start_year=p1["start_year"],
                end_month=p1["end_month"],
                end_year=p1["end_year"],
            )

            right = get_metric_for_period(
                engine=engine,
                user_id=user_id,
                country=country,
                metric_name=metric_name,
                start_month=p2["start_month"],
                start_year=p2["start_year"],
                end_month=p2["end_month"],
                end_year=p2["end_year"],
            )

            left_total = float(left.get("total", 0))
            right_total = float(right.get("total", 0))

            pct_change = (
                ((left_total - right_total) / right_total) * 100
                if right_total != 0
                else None
            )

            state["comparison"] = {
                "left": left,
                "right": right,
                "pct_change": pct_change,
            }

            state["current_metrics"] = left
            return state

        else:
            raise ValueError("Unsupported payload type")

        if state.get("needs_sku"):
            state["sku_analysis"] = pick_top_skus(state["current_metrics"], n=10)
        else:
            state["sku_analysis"] = []

    except Exception as e:
        print("❌ METRICS ERROR:", e)
        state["error"] = str(e)

    return state


def advisor_node(state: AgentState) -> AgentState:
    
    if not state.get("needs_advice"):
        state["advice"] = []
        return state
    
    current_metrics = state.get("current_metrics", {})
    comparison = state.get("comparison", {})
    sku_analysis = state.get("sku_analysis", [])
    thresholds = state.get("thresholds", {}) or {}
    prompt_payload = {
        "metric_name": current_metrics.get("metric"),
        "current_total": current_metrics.get("total"),
        "comparison": comparison,
        "top_skus": sku_analysis,
        "thresholds": thresholds,
    }
    result = _advisor.invoke([
        {"role": "system", "content": ADVISOR_SYSTEM_PROMPT},
        {"role": "user", "content": json.dumps(prompt_payload)},
    ])
    state["advice"] = result.advice
    return state


def email_node(state: AgentState) -> AgentState:
    if not state.get("email_requested"):
        return state

    # 🟢 DATA MODE EMAIL (NEW)
    if state.get("data_mode"):
        import pandas as pd
        import tempfile

        df = pd.DataFrame(state.get("raw_df", []))

        metric = state.get("metric_name", "tax")

        METRIC_COLUMN_GROUPS = {
            "tax": [
                "product_name", "sku", "quantity",
                "product_sales_tax",
                "shipping_credits_tax",
                "giftwrap_credits_tax",
                "promotional_rebates_tax",
                "marketplace_facilitator_tax",
                "digital_transaction_tax",
                "sales_tax_refund",
                "net_taxes",
            ],
            "sales": [
                "product_name", "sku", "quantity",
                "product_sales",
                "gross_sales",
                "net_sales",
                "refund_sales",
            ],
            "profit": [
                "product_name", "sku", "quantity",
                "net_sales",
                "cost_of_unit_sold",
                "amazon_fee",
                "fba_fees",
                "profit",
                "profit_percentage",
            ],
            "fees": [
                "product_name", "sku",
                "selling_fees",
                "fba_fees",
                "amazon_fee",
                "other_transaction_fees",
            ],
        }

        columns = METRIC_COLUMN_GROUPS.get(metric, df.columns.tolist())
        columns = [c for c in columns if c in df.columns]

        df = df[columns]

        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
        df.to_csv(tmp.name, index=False)

        state["email_result"] = send_agent_email(
            user_id=state["user_id"],
            subject=f"{metric.upper()} Breakdown Report",
            html_body="<p>Attached is your breakdown report.</p>",
            attachment_path=tmp.name
        )

        return state

    # -------------------------------
    # NORMAL EMAIL (UNCHANGED)
    # -------------------------------
    user = User.query.filter_by(id=state["user_id"]).first()

    current_metrics = state.get("current_metrics", {})
    period_label = current_metrics.get("period_label", "selected period")

    subject = f"Phormula AI Summary - {state.get('country', 'uk').upper()} - {period_label}"

    html = build_summary_html(
        user_name=(user.name if user else "there") or "there",
        title="Phormula AI Business Summary",
        period_label=period_label,
        metric_name=current_metrics.get("metric", "profit"),
        total=float(current_metrics.get("total", 0.0)),
        comparison=state.get("comparison"),
        top_skus=state.get("sku_analysis", []),
        advice=state.get("advice", []),
    )

    state["email_result"] = send_agent_email(
        user_id=state["user_id"],
        subject=subject,
        html_body=html
    )

    return state


def final_node(state: AgentState) -> AgentState:
    print("\n================= 🔍 FINAL NODE START =================\n")

    try:
        metric = state.get("current_metrics", {})
        comparison = state.get("comparison", {})
        sku_data = state.get("sku_analysis", [])
        advice = state.get("advice", [])
        intent = state.get("intent", "")
        query = state.get("user_query", "")

        print("📊 INTENT:", intent)
        print("📊 QUERY:", query)

        period_label = metric.get("period_label", "selected period")
        metric_name = metric.get("metric")

        # labels
        if metric_name in ["quantity", "units", "total_quantity"]:
            label = "units"
        elif metric_name in ["sales", "net_sales", "profit", "advertising", "platform_fee", "cm2_profit", "gross_sales"]:
            label = "£"
        else:
            label = metric_name

        # product match
        per_sku = metric.get("per_sku", [])
        product_filter = find_best_product_match(query, per_sku)

        print("🔍 PRODUCT FILTER:", product_filter)

        # -------------------------------
        # 🔥 COMPARISON WITH INSIGHTS
        # -------------------------------
        if comparison:
            left = comparison.get("left", {})
            right = comparison.get("right", {})

            current_total = float(left.get("total", 0))
            previous_total = float(right.get("total", 0))

            pct = comparison.get("pct_change")
            pct_text = f"{pct:.2f}%" if pct is not None else "N/A"

            # 🔥 DRIVER ANALYSIS
            curr = {r["product_name"]: float(r["__metric__"]) for r in left.get("per_sku", [])}
            prev = {r["product_name"]: float(r["__metric__"]) for r in right.get("per_sku", [])}

            changes = []
            for p in set(curr) | set(prev):
                diff = curr.get(p, 0) - prev.get(p, 0)
                changes.append((p, diff))

            changes.sort(key=lambda x: abs(x[1]), reverse=True)

            top_pos = [c for c in changes if c[1] > 0][:3]
            top_neg = [c for c in changes if c[1] < 0][:3]

            def fmt(lst):
                return ", ".join(f"{p} ({'+' if v>=0 else '-'}{abs(int(v))})" for p, v in lst)

            lines = [
                f"Your {metric_name} was {int(current_total)} in {left.get('period_label')},",
                f"compared to {int(previous_total)} in {right.get('period_label')}.",
                f"Growth: {pct_text}.",
            ]

            if top_pos:
                lines.append(f"Top positive drivers: {fmt(top_pos)}")

            if top_neg:
                lines.append(f"Top negative drivers: {fmt(top_neg)}")

            state["final_response"] = "\n".join(lines)
            return state

        # -------------------------------
        # DIRECT QA
        # -------------------------------
        if intent == "metric_qa":
            if product_filter:
                rows = [
                    r for r in per_sku
                    if str(r.get("product_name", "")).strip().lower() == product_filter
                ]
                value = sum(float(r.get("__metric__", 0.0)) for r in rows)
            else:
                value = metric.get("total", 0.0)

            state["final_response"] = (
                f"In {period_label}, you had {int(value)} {label} in the UK."
            )
            return state

        # -------------------------------
        # FALLBACK
        # -------------------------------
        state["final_response"] = (
            f"In {period_label}, your total {metric_name} was {int(metric.get('total', 0))}."
        )

        return state

    except Exception as e:
        print("❌ FINAL NODE ERROR:", e)
        state["error"] = str(e)
        return state
    

def _needs_email(state: AgentState) -> str:
    return "email" if state.get("email_requested") else "final"


def build_graph():
    graph = StateGraph(AgentState)
    graph.add_node("planner", planner_node)
    graph.add_node("fetch", fetch_node)
    graph.add_node("metrics", metrics_node)
    graph.add_node("advisor", advisor_node)
    graph.add_node("email", email_node)
    graph.add_node("final", final_node)

    graph.set_entry_point("planner")
    graph.add_edge("planner", "fetch")
    graph.add_edge("fetch", "metrics")
    graph.add_edge("metrics", "advisor")
    graph.add_conditional_edges("advisor", _needs_email, {"email": "email", "final": "final"})
    graph.add_edge("email", "final")
    graph.add_edge("final", END)
    return graph.compile()
