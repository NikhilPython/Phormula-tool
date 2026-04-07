from __future__ import annotations
import json
from typing import Any, Dict, List, Optional
from langgraph.graph import END, StateGraph
from pydantic import BaseModel, Field
from langchain_openai import ChatOpenAI
import re
from datetime import datetime
import pandas as pd
from app.utils.agent_utils import build_plan_langgraph, phormula_engine, amazon_engine
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
chat_llm = ChatOpenAI(model="gpt-4.1", api_key=OPENAI_API_KEY, temperature=0.7)
explain_llm = ChatOpenAI(model="gpt-4.1", api_key=OPENAI_API_KEY, temperature=0.2)


class Period(BaseModel):
    start: str
    end: str

    class Config:
        extra = "forbid"

class PlannerResult(BaseModel):
    intent: str = "chat"   # chat | explain | metric_qa | comparison | report | email | clarify | event_planner | pricing_planner | inventory_planner
    metric_name: Optional[str] = None
    period_mode: str = "none"   # none | latest_completed_month | explicit | last_n | comparison
    months_back: Optional[int] = None
    needs_sku: bool = False
    needs_advice: bool = False
    response_mode: str = "short"
    email_requested: bool = False

    custom_range: bool = False
    period_1: Optional[Period] = None
    period_2: Optional[Period] = None

    clarification_question: Optional[str] = None

    class Config:
        extra = "forbid"


class AdviceResult(BaseModel):
    advice: List[str]


_llm = ChatOpenAI(model="gpt-4.1", api_key=OPENAI_API_KEY, temperature=0)
_planner = _llm.with_structured_output(PlannerResult)
_advisor = _llm.with_structured_output(AdviceResult)

def format_metric_value(value, metric_name, country):
    try:
        value = float(value)
    except:
        return value

    # Units → no decimals
    if metric_name in ["units", "quantity", "total_quantity"]:
        return f"{int(value)}"

    # Percentage metrics
    if metric_name in ["acos", "roas", "margin"]:
        return f"{value:.2f}%"

    # Currency (UK)
    if country == "uk":
        return f"£{value:.2f}"

    # Default fallback
    return f"{value:.2f}"

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

def build_ai_email_summary(state: AgentState) -> str:
    from langchain_openai import ChatOpenAI

    llm = ChatOpenAI(model="gpt-4.1", temperature=0.4)

    metric = state.get("current_metrics", {})
    comparison = state.get("comparison", {})
    per_sku = metric.get("per_sku", [])

    trend_3 = state.get("trend_3", [])
    trend_6 = state.get("trend_6", [])

    country = state.get("country", "").upper()
    period = metric.get("period_label", "selected period")

    # top 5 SKUs
    top_skus = sorted(
        per_sku,
        key=lambda x: float(x.get("__metric__", 0)),
        reverse=True
    )[:5]

    # prepare structured payload
    data_payload = {
        "period": period,
        "country": country,
        "net_sales": metric.get("net_sales"),
        "profit": metric.get("profit"),
        "cm2_profit": metric.get("cm2_profit"),
        "units": metric.get("total_quantity"),
        "advertising": metric.get("advertising_total"),
        "platform_fee": metric.get("platform_fee"),
        "acos": metric.get("acos"),
        "comparison": comparison,
        "top_skus": top_skus,
        "trend_3_months": trend_3,
        "trend_6_months": trend_6,
    }

    prompt = f"""
You are a senior ecommerce business analyst.

Write a sharp, professional business summary for Amazon UK.

IMPORTANT RULES:
- Do NOT list raw numbers mechanically
- Explain performance clearly and concisely
- Use comparison (if available)
- Use historical trends (3-month and 6-month)
- Highlight if performance is improving, declining, or stable
- Mention top products and SKU movement
- Keep it executive-level (clear, insightful, not long)
- NO recommendations
- NO bullet points unless necessary
- Write like a business report, not a chatbot

DATA:
{data_payload}
"""

    response = llm.invoke([
        {"role": "system", "content": "You are a sharp ecommerce financial analyst."},
        {"role": "user", "content": prompt},
    ])

    return response.content


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
    user_query = (state.get("user_query") or "").strip()
    query_l = user_query.lower()

    GREETINGS = {
        "hi", "hello", "hey", "yo", "hola", "good morning",
        "good afternoon", "good evening"
    }

    CHAT_ONLY = {
        "thanks", "thank you", "ok", "okay", "cool", "nice", "great"
    }

    EXPLAIN_TRIGGERS = [
        "what is", "what's", "explain", "define", "meaning of",
        "how does", "how do you calculate", "formula for"
    ]

    EMAIL_TRIGGERS = [
        "send email", "send me", "mail me", "email me", "mail the report",
        "send the report", "email the report"
    ]

    PLANNER_TRIGGERS = [
        "forecast", "event", "prime day", "black friday", "pricing",
        "price range", "inventory planning", "stock planning", "procurement",
        "reorder", "target sales", "coverage months"
    ]

    DATA_TRIGGERS = [
        "profit", "sales", "revenue", "tax", "acos", "roas", "margin",
        "orders", "units", "fees", "sku", "compare", "comparison",
        "last month", "this month", "march", "april", "2024", "2025", "2026",
        "report", "breakdown", "trend", "performance"
    ]

    # -------------------------------
    # GREETING / CHAT
    # -------------------------------
    if query_l in GREETINGS or query_l in CHAT_ONLY:
        state["intent"] = "chat"
        state["metric_name"] = None
        state["period_mode"] = "none"
        state["months_back"] = None
        state["needs_sku"] = False
        state["needs_advice"] = False
        state["response_mode"] = "short"
        state["custom_range"] = False
        state["period_1"] = None
        state["period_2"] = None
        state["email_requested"] = False
        state["period_parsed"] = {"type": "none"}
        state["data_mode"] = False
        state["clarification_question"] = None
        state["planner_payload"] = None
        return state

    # -------------------------------
    # EXPLAIN MODE
    # -------------------------------
    if any(trigger in query_l for trigger in EXPLAIN_TRIGGERS) and not any(word in query_l for word in DATA_TRIGGERS):
        state["intent"] = "explain"
        state["metric_name"] = None
        state["period_mode"] = "none"
        state["months_back"] = None
        state["needs_sku"] = False
        state["needs_advice"] = False
        state["response_mode"] = "short"
        state["custom_range"] = False
        state["period_1"] = None
        state["period_2"] = None
        state["email_requested"] = False
        state["period_parsed"] = {"type": "none"}
        state["data_mode"] = False
        state["clarification_question"] = None
        state["planner_payload"] = None
        return state

    # -------------------------------
    # 🔥 PLANNER TRIGGER (FIX)
    # -------------------------------
    if any(trigger in query_l for trigger in PLANNER_TRIGGERS):
        state["intent"] = "pricing_planner"
        state["metric_name"] = None
        state["period_mode"] = "none"
        state["months_back"] = None
        state["needs_sku"] = True
        state["needs_advice"] = True
        state["response_mode"] = "detailed"
        state["custom_range"] = False
        state["period_1"] = None
        state["period_2"] = None
        state["email_requested"] = False
        state["period_parsed"] = {"type": "none"}
        state["data_mode"] = False
        state["clarification_question"] = None

        state["planner_payload"] = {
            "user_id": state["user_id"],
            "country": state["country"],
            "last_event": {"month": 11, "year": 2025},
            "future_event": {"month": 11, "year": 2026},
            "target_sales": None,
        }

        return state

    # -------------------------------
    # EMAIL DETECTION
    # -------------------------------
    hard_email_requested = any(trigger in query_l for trigger in EMAIL_TRIGGERS)

    # -------------------------------
    # LLM PLANNER
    # -------------------------------
    messages = [{"role": "system", "content": PLANNER_SYSTEM_PROMPT}]

    for h in history:
        messages.append({"role": "user", "content": h["message"]})
        messages.append({"role": "assistant", "content": h["response"]})

    messages.append({"role": "user", "content": user_query})

    result = _planner.invoke(messages)

    intent = (result.intent or "chat").strip().lower()

    if intent == "period_comparison":
        intent = "comparison"
    elif intent in ["daily_summary", "weekly_summary"]:
        intent = "report"
    elif intent == "send_email":
        intent = "email"

    allowed_intents = {
        "chat", "explain", "metric_qa", "comparison", "report", "email", "clarify",
        "event_planner", "pricing_planner", "inventory_planner",
        "top_skus", "loss_making_skus", "advice"
    }

    if intent not in allowed_intents:
        intent = "chat"

    if hard_email_requested:
        intent = "email"

    state["intent"] = intent

    if intent in {
        "metric_qa", "comparison", "report", "email",
        "top_skus", "loss_making_skus", "advice"
    }:
        state["metric_name"] = resolve_metric_from_query(user_query, result.metric_name)
    else:
        state["metric_name"] = None

    state["period_mode"] = result.period_mode or "none"
    state["months_back"] = result.months_back
    state["needs_sku"] = bool(result.needs_sku)
    state["needs_advice"] = bool(result.needs_advice)
    state["response_mode"] = result.response_mode or "short"
    state["custom_range"] = bool(result.custom_range)
    state["period_1"] = result.period_1
    state["period_2"] = result.period_2
    state["email_requested"] = bool(hard_email_requested or result.email_requested or intent == "email")
    state["clarification_question"] = getattr(result, "clarification_question", None)

    if intent in {
        "metric_qa", "comparison", "report", "email",
        "top_skus", "loss_making_skus", "advice"
    }:
        parsed = parse_period(user_query)
        if not parsed or "type" not in parsed:
            parsed = {"type": "latest_month"}
        state["period_parsed"] = parsed
    else:
        state["period_parsed"] = {"type": "none"}

    if intent in {"metric_qa", "comparison", "report", "email"} and any(word in query_l for word in [
        "breakdown", "productwise", "product wise", "all columns", "raw data", "full data", "export"
    ]):
        state["data_mode"] = True
    else:
        state["data_mode"] = False

    state["planner_payload"] = None

    if intent in {"metric_qa", "comparison", "report", "email"} and not state.get("metric_name"):
        state["intent"] = "clarify"
        state["clarification_question"] = "Which metric would you like me to use?"
        state["email_requested"] = False
        state["period_parsed"] = {"type": "none"}
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

            result = get_metric_last_n_months(
                engine=engine,
                user_id=user_id,
                country=country,
                metric_name=metric_name,
                n=n,
                offset=0,
            )

            state["current_metrics"] = result
            state["comparison"] = None  # 🔥 IMPORTANT: disable comparison
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

    # 🟢 DATA MODE EMAIL (UNCHANGED)
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
    # 🔥 AI EMAIL (UPDATED)
    # -------------------------------
    user = User.query.filter_by(id=state["user_id"]).first()

    current_metrics = state.get("current_metrics", {})
    period_label = current_metrics.get("period_label", "selected period")

    subject = f"Phormula AI Summary - {state.get('country', 'uk').upper()} - {period_label}"

    # 🔥 NEW: AI-generated summary
    summary_text = build_ai_email_summary(state)

    html = f"""
    <p>Hi {(user.name if user else "there")},</p>

    <p>{summary_text.replace('\n', '<br>')}</p>

    <br>
    <p>— Phormula</p>
    """

    state["email_result"] = send_agent_email(
        user_id=state["user_id"],
        subject=subject,
        html_body=html
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

def planner_agent_node(state: AgentState) -> AgentState:
    try:
        
        payload = state.get("planner_payload") or {}

        result = build_plan_langgraph(
            payload=payload,
            phormula_engine=phormula_engine,
            amazon_engine=amazon_engine,
        )

        state["planner_result"] = {
            "type": "multi_sku_plan",
            "items": result,
        }

        return state

    except Exception as e:
        state["error"] = f"Planner agent failed: {str(e)}"
        return state



def final_node(state: AgentState) -> AgentState:
    print("\n================= 🔍 FINAL NODE START =================\n")

    try:
        # EMAIL RESPONSE
        if state.get("email_requested"):
            if state.get("data_mode"):
                state["final_response"] = "📩 I've emailed you the detailed breakdown report."
            else:
                state["final_response"] = "📩 I've sent you the summary report on email."
            return state

        # CLARIFICATION
        if state.get("intent") == "clarify":
            state["final_response"] = state.get("clarification_question") or "Could you clarify what you'd like me to analyze?"
            return state

        # PLANNER RESULT
        if state.get("planner_result"):
            planner_result = state["planner_result"]
            items = planner_result.get("items", [])

            if not items:
                state["final_response"] = "I analyzed the planning request, but I couldn't generate a planner result."
                return state

            top_items = items[:3]
            lines = ["Here’s the planning summary:"]

            for item in top_items:
                sku = item.get("sku", "Unknown SKU")

                summary = None
                if isinstance(item.get("summary"), list):
                    summary = item["summary"]
                elif isinstance(item.get("plan", {}).get("summary"), list):
                    summary = item["plan"]["summary"]

                if summary:
                    lines.append(f"- {sku}: {summary[0]}")
                else:
                    lines.append(f"- {sku}: planning completed successfully.")

            state["final_response"] = "\n".join(lines)
            return state

        # CHAT MODE
        if state.get("intent") == "chat":
            history = state.get("chat_history", [])

            messages = [
                {
                    "role": "system",
                    "content": "You are a friendly business copilot. Talk naturally and helpfully.",
                }
            ]

            for h in history[-6:]:
                messages.append({"role": "user", "content": h["message"]})
                messages.append({"role": "assistant", "content": h["response"]})

            messages.append({"role": "user", "content": state.get("user_query", "")})

            response = chat_llm.invoke(messages)
            state["final_response"] = response.content
            return state

        # EXPLAIN MODE
        if state.get("intent") == "explain":
            response = explain_llm.invoke([
                {"role": "system", "content": "Explain ecommerce metrics clearly."},
                {"role": "user", "content": state.get("user_query", "")},
            ])
            state["final_response"] = response.content
            return state

        # -------------------------------
        # METRICS
        # -------------------------------
        metric = state.get("current_metrics", {})
        comparison = state.get("comparison", {})
        intent = state.get("intent", "")
        query = state.get("user_query", "")

        country = state.get("country", "").lower()
        metric_name = metric.get("metric")

        # 🔥 TREND ANALYSIS (WITH FORMATTING)
        if metric.get("per_period"):
            series = metric["per_period"]

            if len(series) >= 2:
                values = [float(x.get("__metric__", 0)) for x in series]
                labels = [x.get("period_label") for x in series]

                change = values[-1] - values[0]
                pct = ((change / values[0]) * 100) if values[0] else 0

                trend = "increased" if change > 0 else "decreased"

                lines = [
                    f"Your {metric_name} has {trend} over the last {len(series)} months.",
                    ""
                ]

                for l, v in zip(labels, values):
                    formatted = format_metric_value(v, metric_name, country)
                    lines.append(f"{l}: {formatted}")

                formatted_change = format_metric_value(change, metric_name, country)

                lines.append("")
                lines.append(f"Overall change: {formatted_change} ({pct:.2f}%)")

                state["final_response"] = "\n".join(lines)
                return state

        period_label = metric.get("period_label", "selected period")

        per_sku = metric.get("per_sku", [])
        product_filter = find_best_product_match(query, per_sku)

        # COMPARISON (WITH FORMATTING)
        if comparison:
            left = comparison.get("left", {})
            right = comparison.get("right", {})

            current_total = float(left.get("total", 0))
            previous_total = float(right.get("total", 0))

            pct = comparison.get("pct_change")
            pct_text = f"{pct:.2f}%" if pct is not None else "N/A"

            current_fmt = format_metric_value(current_total, metric_name, country)
            previous_fmt = format_metric_value(previous_total, metric_name, country)

            state["final_response"] = (
                f"Your {metric_name} was {current_fmt} vs {previous_fmt}. Growth: {pct_text}."
            )
            return state

        # STANDARD METRIC QA (WITH FORMATTING)
        if intent in {"metric_qa", "report", "email", "top_skus", "loss_making_skus", "advice"}:
            if product_filter:
                rows = [
                    r for r in per_sku
                    if str(r.get("product_name", "")).strip().lower() == product_filter
                ]
                value = sum(float(r.get("__metric__", 0.0)) for r in rows)
            else:
                value = metric.get("total", 0.0)

            formatted_value = format_metric_value(value, metric_name, country)

            state["final_response"] = f"In {period_label}, you had {formatted_value} in the UK."
            return state

        # FALLBACK
        fallback_value = format_metric_value(metric.get("total", 0), metric_name, country)
        state["final_response"] = f"In {period_label}, your total {metric_name} was {fallback_value}."
        return state

    except Exception as e:
        print("❌ FINAL NODE ERROR:", e)
        state["error"] = str(e)
        return state
    

def _needs_email(state: AgentState) -> str:
    return "email" if state.get("email_requested") else "final"

def _route_after_planner(state: AgentState) -> str:
    intent = state.get("intent")

    if intent in {"chat", "explain", "clarify"}:
        return "final"

    if intent in {"event_planner", "pricing_planner", "inventory_planner"}:
        return "planner_agent"

    return "fetch"


def build_graph():
    graph = StateGraph(AgentState)
    graph.add_node("planner", planner_node)
    graph.add_node("fetch", fetch_node)
    graph.add_node("metrics", metrics_node)
    graph.add_node("advisor", advisor_node)
    graph.add_node("email", email_node)
    graph.add_node("planner_agent", planner_agent_node)
    graph.add_node("final", final_node)

    graph.set_entry_point("planner")

    graph.add_conditional_edges(
        "planner",
        _route_after_planner,
        {
            "fetch": "fetch",
            "planner_agent": "planner_agent",
            "final": "final",
        }
    )

    graph.add_edge("fetch", "metrics")
    graph.add_edge("metrics", "advisor")
    graph.add_conditional_edges("advisor", _needs_email, {"email": "email", "final": "final"})
    graph.add_edge("email", "final")
    graph.add_edge("planner_agent", "final")
    graph.add_edge("final", END)

    return graph.compile()
