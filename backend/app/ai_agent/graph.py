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
from app.ai_agent.email_service import build_summary_html, send_agent_email, build_ai_email_summary
from app.ai_agent.db import get_engine,latest_available_month,fetch_period_dfs, get_metric_def
from app.ai_agent.formula_engine import (
    parse_period,
    get_metric_for_month,
    get_metric_for_period,
    get_metric_last_n_months,
    compare_periods,
    pick_top_skus,
    OVERALL_MONTH_METRICS,
    PRODUCT_MONTH_METRICS,
    get_metric_pack_for_month,
    get_product_metric_pack_for_month,
    get_last_n_month_keys,
    build_time_series_analysis,
    _aggregate_sku_rows,
    get_growth_driver_insights,
)
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
    analysis_type: str = "absolute"   # absolute | trend | summary | comparison
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
    "reimbursements": "rembursement_fee",
    "reimbursement": "rembursement_fee",

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

    "cm1 profit":"profit",
    "profit":"profit",

    "profit %": "profit_percentage",
    "margin": "profit_percentage",

    # 🔥 ACOS
    "ad cos": "acos",
    "roas": "acos",
    "acos": "acos",

    # 🔥 PRICE
    "average selling price": "asp",
    "avg selling price": "asp",

    "profit per unit": "unit_wise_profitability",
    "ppu": "unit_wise_profitability",

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
    "amazon fees total": "amazon_fee",
    "amazon fees": "amazon_fee",
    "amazon fee": "amazon_fee",

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
    "refund count": "refund_sales",
    "returns": "refund_sales",
    "refund": "refund_sales",
}

DISPLAY_NAME_MAP = {
    "net_sales": "Net Sales",
    "gross_sales": "Gross Sales",
    "profit": "CM1 Profit",
    "cm2_profit": "CM2 Profit",
    "total_quantity": "Units Sold",
    "asp": "ASP",
    "advertising_total": "Ad Spend",
    "platform_fee": "Platform Fees",
    "acos": "ACOS",
    "profit_percentage": "CM1 Profit %",
    "amazon_fee": "Marketplace Fees",
    "fba_fees": "FBA Fees",
    "selling_fees": "Selling Fees",
    "refund_sales": "Refunds Sales",
}

def format_metric_name(metric_name: str) -> str:
    return DISPLAY_NAME_MAP.get(metric_name, metric_name.replace("_", " ").title())

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

    # 🔥 Extract words from query
    words = q.split()

    # ✅ 1. EXACT WORD MATCH (best fix)
    for p in products:
        if p in words:
            return p

    # ✅ 2. CONTAINS MATCH (classic inside sentence)
    for p in products:
        if p and p in q:
            return p

    # ✅ 3. PREFIX MATCH (variants)
    matches = [p for p in products if any(w and p.startswith(w) for w in words)]

    if len(matches) == 1:
        return matches[0]

    if len(matches) > 1:
        return matches[0]  # or return group logic

    return None

def extract_products_from_query(query: str):
    # Keep planner lightweight. Product detection should happen in metrics_node
    # using actual DB product names, not regex guesses.
    return []

def _previous_month(month: int, year: int) -> tuple[int, int]:
    if month == 1:
        return 12, year - 1
    return month - 1, year


def _build_monthly_insight_context(
    engine,
    user_id: int,
    country: str,
    month: int,
    year: int,
    product_match: str | None = None,
) -> Dict[str, Any]:
    prev_month, prev_year = _previous_month(month, year)

    if product_match:
        current_pack = get_product_metric_pack_for_month(
            engine=engine,
            user_id=user_id,
            country=country,
            product_match=product_match,
            month=month,
            year=year,
        )
        previous_pack = get_product_metric_pack_for_month(
            engine=engine,
            user_id=user_id,
            country=country,
            product_match=product_match,
            month=prev_month,
            year=prev_year,
        )
        scope = "product"
    else:
        current_pack = get_metric_pack_for_month(
            engine=engine,
            user_id=user_id,
            country=country,
            metric_names=OVERALL_MONTH_METRICS,
            month=month,
            year=year,
        )
        previous_pack = get_metric_pack_for_month(
            engine=engine,
            user_id=user_id,
            country=country,
            metric_names=OVERALL_MONTH_METRICS,
            month=prev_month,
            year=prev_year,
        )
        scope = "overall"

    changes = {}
    for metric_name, curr_value in current_pack["metrics"].items():
        prev_value = float(previous_pack["metrics"].get(metric_name, 0.0))
        delta = float(curr_value) - prev_value
        pct = None if prev_value == 0 else (delta / prev_value) * 100.0
        changes[metric_name] = {
            "current": float(curr_value),
            "previous": prev_value,
            "delta": delta,
            "pct_change": pct,
        }

    driver = None
    if scope == "overall":
        driver = get_growth_driver_insights(
            engine=engine,
            user_id=user_id,
            country=country,
            metric_name="net_sales",
            month=month,
            year=year,
        )

    return {
        "scope": scope,
        "period_label": current_pack["period_label"],
        "previous_period_label": previous_pack["period_label"],
        "product_match": product_match,
        "metrics": current_pack["metrics"],
        "changes": changes,
        "driver": driver,
    }


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
        state["analysis_type"] = "absolute"
        state["multi_metric"] = False
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
    # PLANNER TRIGGER
    # -------------------------------
    if any(trigger in query_l for trigger in PLANNER_TRIGGERS):
        state["intent"] = "pricing_planner"
        state["metric_name"] = None
        state["analysis_type"] = "absolute"
        state["multi_metric"] = False
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

    print("\n================= 🧠 PLANNER DEBUG =================\n")
    print("User Query:", user_query)
    print("Planner Raw Output:", result)

    intent = (result.intent or "chat").strip().lower()

    # 🔥 SMART FALLBACK (NO HARDCODING)
    if intent == "explain":
        parsed = parse_period(user_query)

        if parsed.get("type") != "none" or result.metric_name:
            intent = "metric_qa"

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

    parsed = parse_period(user_query)
    if intent == "comparison" and parsed.get("type") in ["last_n", "last_n_months"]:
        intent = "metric_qa"

    state["intent"] = intent

    if intent in {
        "metric_qa", "comparison", "report", "email",
        "top_skus", "loss_making_skus", "advice"
    }:
        state["metric_name"] = resolve_metric_from_query(user_query, result.metric_name)
    else:
        state["metric_name"] = None

    # 🔥 ROBUST ANALYSIS TYPE NORMALIZATION
    # ✅ NEW (DO NOT BREAK EXISTING)
    # ✅ FIXED VERSION
    raw_analysis = (getattr(result, "analysis_type", "") or "").lower()

    if raw_analysis:

        if raw_analysis in ["breakdown", "product_breakdown", "sku_breakdown"]:
            analysis_type = "breakdown"

        elif any(x in raw_analysis for x in ["top", "bottom", "sku"]):
            analysis_type = "sku_ranking"

        elif any(x in raw_analysis for x in ["best", "worst", "highest", "lowest"]):
            analysis_type = "time_ranking"

        elif raw_analysis in ["trend", "growth", "mom", "change"]:
            analysis_type = "trend"

        elif raw_analysis in ["summary"]:
            analysis_type = "summary"

        else:
            analysis_type = "absolute"
    else:
        analysis_type = "absolute"

    state["analysis_type"] = analysis_type
    state["period_mode"] = result.period_mode or "none"
    state["months_back"] = result.months_back
    state["needs_sku"] = bool(result.needs_sku) or bool(state.get("product_queries"))
    state["needs_advice"] = bool(result.needs_advice)
    state["response_mode"] = result.response_mode or "short"
    state["custom_range"] = bool(result.custom_range)
    state["period_1"] = result.period_1
    state["period_2"] = result.period_2
    state["email_requested"] = bool(hard_email_requested or result.email_requested or intent == "email")
    state["clarification_question"] = getattr(result, "clarification_question", None)

    state["product_queries"] = extract_products_from_query(user_query)

    print("🧠 Extracted Products:", state["product_queries"])

    # 🔥 MULTI-METRIC MODE
    # Broad trend/summary questions without a specific metric should return a business view
    metric_name_value = state.get("metric_name")
    broad_business_query = (
        intent == "metric_qa"
        and analysis_type in {"trend", "summary"}
        and not state.get("needs_sku")
        and parsed.get("type") in {"last_n", "last_n_months", "single", "latest_month"}
    )

    if not result.metric_name and broad_business_query:
        state["multi_metric"] = True
    else:
        state["multi_metric"] = False

    if intent in {
        "metric_qa", "comparison", "report", "email",
        "top_skus", "loss_making_skus", "advice"
    }:
        if not parsed or "type" not in parsed:
            parsed = {"type": "latest_month"}
        state["period_parsed"] = parsed
    else:
        state["period_parsed"] = {"type": "none"}

    if intent in {"metric_qa", "comparison", "report", "email"} and any(word in query_l for word in [
             "all columns", "raw data", "full data", "export"
        ]):
        state["data_mode"] = True
    else:
        state["data_mode"] = False

    state["planner_payload"] = None

    if intent in {"metric_qa", "comparison", "report", "email"} and not state.get("metric_name") and not state.get("multi_metric"):
        state["intent"] = "clarify"
        state["clarification_question"] = "Which metric would you like me to use?"
        state["email_requested"] = False
        state["period_parsed"] = {"type": "none"}
        state["data_mode"] = False

    print("\n🧠 FINAL PLANNER STATE:")
    print({
        "intent": state.get("intent"),
        "metric_name": state.get("metric_name"),
        "analysis_type": state.get("analysis_type"),
        "multi_metric": state.get("multi_metric"),
        "needs_sku": state.get("needs_sku"),
        "products": state.get("product_queries"),
        "period_parsed": state.get("period_parsed"),
    })

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

    print("\n📦 FETCH OUTPUT STATE:")
    print({
        "period_payload": state.get("period_payload"),
    })

    return state


def metrics_node(state: AgentState) -> AgentState:
    print("\n================= 🔍 NSE METRICS NODE =================\n")

    print("\n📊 FULL STATE BEFORE METRICS:")
    print({
        "metric_name": state.get("metric_name"),
        "needs_sku": state.get("needs_sku"),
        "period_payload": state.get("period_payload"),
        "analysis_type": state.get("analysis_type"),
        "multi_metric": state.get("multi_metric"),  # 🔥 NEW DEBUG
    })

    # 🚨 DATA MODE BYPASS (UNCHANGED)
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
        state["raw_df"] = df.to_dict(orient="records")

        return state

    # -------------------------------
    # NORMAL METRIC MODE
    # -------------------------------
    engine = state.get("engine")
    if engine is None:
        raise ValueError("Engine not found in state (fetch_node failed)")

    user_id = state["user_id"]
    country = state["country"]
    metric_name = state.get("metric_name", "profit")

    payload = state.get("period_payload") or {"type": "latest_month"}
    ptype = payload.get("type", "latest_month")

    analysis_type = state.get("analysis_type")
    # ===============================
    # 🔥 RANKING MODE (BEST / WORST MONTH)
    # ===============================
    if analysis_type == "time_ranking":

        user_query = state.get("user_query", "")

        # 👉 get months (default 12 months)
        months = get_last_n_month_keys(
            engine=engine,
            user_id=user_id,
            country=country,
            n=12,
        )

        # -------------------------------
        # PRODUCT MATCH (reuse your logic)
        # -------------------------------
        sample_result = get_metric_for_month(
            engine=engine,
            user_id=user_id,
            country=country,
            metric_name=metric_name,
            month=months[-1].month,
            year=months[-1].year,
        )

        product_match = None

        # ✅ ONLY try product match if user intent suggests it
        if sample_result.get("per_sku"):
            product_match = find_best_product_match(user_query, sample_result["per_sku"])

        state["product_match"] = product_match

        # -------------------------------
        # COMPUTE VALUES
        # -------------------------------
        series = []

        for mk in months:
            try:
                if product_match:
                    res = get_product_metric_pack_for_month(
                        engine=engine,
                        user_id=user_id,
                        country=country,
                        product_match=product_match,
                        month=mk.month,
                        year=mk.year,
                    )
                    value = float(res["metrics"].get(metric_name, 0))
                else:
                    res = get_metric_for_month(
                        engine=engine,
                        user_id=user_id,
                        country=country,
                        metric_name=metric_name,
                        month=mk.month,
                        year=mk.year,
                    )
                    value = float(res.get("total", 0))

                series.append({
                    "period_label": mk.label,
                    "__metric__": value,
                })

            except Exception:
                continue

        if not series:
            raise ValueError("No data found for ranking")

        # -------------------------------
        # BEST / WORST
        # -------------------------------
        best = max(series, key=lambda x: x["__metric__"])
        worst = min(series, key=lambda x: x["__metric__"])

        state["analysis_result"] = {
            "type": "ranking",
            "metric": metric_name,
            "best": best,
            "worst": worst,
            "series": series,
        }

        state["current_metrics"] = {
            "metric": metric_name,
            "total": best["__metric__"],
            "period_label": best["period_label"],
        }

        return state
    
    # ===============================
    # 🔥 SKU RANKING (TOP/BOTTOM PRODUCTS)
    # ===============================
    if analysis_type == "sku_ranking":

        result = get_metric_for_month(
            engine=engine,
            user_id=user_id,
            country=country,
            metric_name=metric_name,
            month=payload["month"],
            year=payload["year"],
        )

        rows = result.get("per_sku", [])

        if not rows:
            raise ValueError("No SKU data found")

        # sort descending
        sorted_rows = sorted(rows, key=lambda x: float(x.get("__metric__", 0)), reverse=True)

        # 🔥 dynamic top_n extraction
        top_n = state.get("top_k")

        if not top_n:
            match = re.search(r"\b(\d+)\b", state.get("user_query", ""))
            top_n = int(match.group(1)) if match else 3

        top = sorted_rows[:top_n]
        bottom = sorted_rows[-top_n:]

        state["analysis_result"] = {
            "type": "sku_ranking",
            "metric": metric_name,
            "top": top,
            "bottom": bottom,
            "period_label": result.get("period_label"),
        }

        state["current_metrics"] = result
        return state
    
    # ===============================
    # 🔥 PRODUCTWISE BREAKDOWN  ← ADD HERE
    # ===============================
    if analysis_type == "breakdown":

        # 🔥 PRODUCT MATCH LOGIC (ADD THIS)
        user_query = state.get("user_query", "")

        latest = latest_available_month(engine, user_id, country)

        sample = get_metric_for_month(
            engine=engine,
            user_id=user_id,
            country=country,
            metric_name=metric_name,
            month=latest.month,
            year=latest.year,
        )

        product_match = None

        if sample.get("per_sku"):
            product_match = find_best_product_match(user_query, sample["per_sku"])

        state["product_match"] = product_match

        print("🔥 PRODUCT MATCH (BREAKDOWN):", product_match)

        if ptype == "single":
            result = get_metric_for_month(
                engine=engine,
                user_id=user_id,
                country=country,
                metric_name=metric_name,
                month=payload["month"],
                year=payload["year"],
            )
            if product_match and result.get("per_sku"):
                result["per_sku"] = [
                    r for r in result["per_sku"]
                    if product_match in str(r.get("product_name", "")).lower()
                ]

        elif ptype == "range":

            try:
                result = get_metric_for_period(
                    engine=engine,
                    user_id=user_id,
                    country=country,
                    metric_name=metric_name,
                    start_month=payload["start_month"],
                    start_year=payload["start_year"],
                    end_month=payload["end_month"],
                    end_year=payload["end_year"],
                    skip_missing=True,
                )
            except Exception as e:
                print("⚠️ get_metric_for_period failed, using fallback:", e)
                result = {"per_sku": []}  # force fallback

            # 🔥 FIX: ensure per_sku exists
            # 🔥 FIX: ensure per_sku exists (SAFE VERSION)
            if not result.get("per_sku"):

                period_dfs = fetch_period_dfs(
                    engine=engine,
                    user_id=user_id,
                    country=country,
                    start_month=payload["start_month"],
                    start_year=payload["start_year"],
                    end_month=payload["end_month"],
                    end_year=payload["end_year"],
                )
                print("🔍 period_dfs length:", len(period_dfs))

                all_rows = []

                metric_def = get_metric_def(metric_name)

                for mk, df in period_dfs:
                    if df is None or df.empty:
                        continue

                    # 🔥 ALWAYS try to extract SKU-level data
                    try:
                        grouped_df = _aggregate_sku_rows([(mk, df)], metric_def.column)

                        if grouped_df is not None and not grouped_df.empty:
                            all_rows.extend(grouped_df.to_dict(orient="records"))

                    except Exception as e:
                        print(f"⚠️ aggregation failed for {mk.label}: {e}")
                        continue

                grouped = {}

                for row in all_rows:
                    key = row.get("sku") or row.get("product_name") or "unknown"

                    if key not in grouped:
                        grouped[key] = {
                            "sku": row.get("sku"),
                            "product_name": row.get("product_name"),
                            "__metric__": 0.0,
                        }

                    grouped[key]["__metric__"] += float(row.get("__metric__", 0.0))

                per_sku = sorted(grouped.values(), key=lambda x: float(x["__metric__"]), reverse=True)

                if product_match:
                    per_sku = [
                        r for r in per_sku
                        if product_match in str(r.get("product_name", "")).lower()
                    ]

                if not per_sku:
                    raise ValueError("No SKU-level data found for breakdown")
                else:
                    result["per_sku"] = per_sku
                    result["total"] = sum(float(x["__metric__"]) for x in per_sku)
                    result["row_count"] = len(per_sku)
                
                months_found = [mk.label for mk, _ in period_dfs] if period_dfs else []

                if months_found:
                    result["period_label"] = f"{months_found[0]} to {months_found[-1]}"
                else:
                    result["period_label"] = "No data"


        elif ptype == "last_n_months":
            months = get_last_n_month_keys(
                engine=engine,
                user_id=user_id,
                country=country,
                n=payload["n"],
            )

            all_rows = []

            for mk in months:
                month_result = get_metric_for_month(
                    engine=engine,
                    user_id=user_id,
                    country=country,
                    metric_name=metric_name,
                    month=mk.month,
                    year=mk.year,
                )
                all_rows.extend(month_result.get("per_sku", []))

            grouped = {}
            for row in all_rows:
                key = row.get("sku") or row.get("product_name") or "unknown"
                if key not in grouped:
                    grouped[key] = {
                        "sku": row.get("sku"),
                        "product_name": row.get("product_name"),
                        "__metric__": 0.0,
                    }
                grouped[key]["__metric__"] += float(row.get("__metric__", 0.0))

            per_sku = sorted(grouped.values(), key=lambda x: float(x["__metric__"]), reverse=True)

            if product_match:
                per_sku = [
                    r for r in per_sku
                    if product_match in str(r.get("product_name", "")).lower()
                ]

            result = {
                "metric": metric_name,
                "period_type": "range",
                "period_label": f"{months[0].label} to {months[-1].label}",
                "total": sum(float(x["__metric__"]) for x in per_sku),
                "per_sku": per_sku,
                "row_count": len(per_sku),
            }

        else:
            raise ValueError(f"Unsupported payload type for breakdown: {ptype}")

        state["analysis_result"] = {
            "type": "breakdown",
            "metric": metric_name,
            "period_label": result.get("period_label"),
            "per_sku": result.get("per_sku", []),
            "total": result.get("total", 0),
        }

        state["current_metrics"] = result
        return state


    multi_metric = state.get("multi_metric", False)

    print("📊 metric:", metric_name)
    print("📊 payload:", payload)

    try:
        # -------------------------------
        # 🔥 LAST N MONTHS (TREND MODE)
        # -------------------------------
        if ptype == "last_n_months" and analysis_type == "trend":
            n = payload["n"]
            user_query = state.get("user_query", "")

            months = get_last_n_month_keys(
                engine=engine,
                user_id=user_id,
                country=country,
                n=n,
            )

            # -------------------------------
            # PRODUCT MATCH
            # -------------------------------
            sample_result = get_metric_for_month(
                engine=engine,
                user_id=user_id,
                country=country,
                metric_name=metric_name,
                month=months[-1].month,
                year=months[-1].year,
            )

            product_match = None

            if sample_result.get("per_sku"):
                rows = sample_result.get("per_sku", [])

                # 🔍 DEBUG: see what products exist
                print("Available products:", [r.get("product_name") for r in rows])

                # ✅ main matching
                product_match = find_best_product_match(user_query, rows)

            # 🔥 HARD FALLBACK (this is what you were missing)
            if not product_match:
                q = user_query.lower()

                for r in sample_result.get("per_sku", []):
                    name = str(r.get("product_name") or r.get("sku") or "").lower()
                    if name and q in name:
                        product_match = q  # treat as group
                        break

            # 🔥 FINAL DEBUG
            print("✅ FINAL PRODUCT MATCH:", product_match)

            state["product_match"] = product_match

            # -------------------------------
            # 🔥 MULTI METRIC MODE
            # -------------------------------
            if multi_metric:
                all_results = {}

                if product_match:
                    metric_list = PRODUCT_MONTH_METRICS
                else:
                    metric_list = OVERALL_MONTH_METRICS

                for m in metric_list:
                    res = build_time_series_analysis(
                        engine=engine,
                        user_id=user_id,
                        country=country,
                        metric_name=m,
                        months=months,
                        product_match=product_match,
                    )
                    all_results[m] = res

                state["analysis_result"] = all_results

                state["current_metrics"] = {
                    "metric": "multi",
                    "per_period": [],
                    "total": None,
                    "period_label": f"{months[0].label} to {months[-1].label}",
                }

            else:
                analysis_result = build_time_series_analysis(
                    engine=engine,
                    user_id=user_id,
                    country=country,
                    metric_name=metric_name,
                    months=months,
                    product_match=product_match,
                )

                analysis_result["product_match"] = product_match

                state["analysis_result"] = analysis_result

                state["current_metrics"] = {
                    "metric": metric_name,
                    "per_period": analysis_result["series"],
                    "total": sum(float(x["__metric__"]) for x in analysis_result["series"]),
                    "period_label": f"{months[0].label} to {months[-1].label}",
                }

            # 🔥 ADD THIS (LATEST MONTH INSIGHT FOR TREND)
            try:
                latest = months[-1]
                state["latest_insight_context"] = _build_monthly_insight_context(
                    engine=engine,
                    user_id=user_id,
                    country=country,
                    month=latest.month,
                    year=latest.year,
                    product_match=product_match,
                )
            except Exception as e:
                print("⚠️ latest insight skipped:", e)
                state["latest_insight_context"] = None

            state["comparison"] = None
            return state

        # -------------------------------
        # (REST OF YOUR CODE UNCHANGED)
        # -------------------------------

        elif ptype == "latest_month":
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

            # 🔥 ADD THIS (INSIGHT)
            try:
                state["insight_context"] = _build_monthly_insight_context(
                    engine=engine,
                    user_id=user_id,
                    country=country,
                    month=latest.month,
                    year=latest.year,
                    product_match=state.get("product_match"),
                )
            except Exception as e:
                print("⚠️ insight skipped:", e)
                state["insight_context"] = None

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

            # 🔥 ADD THIS (INSIGHT)
            try:
                state["insight_context"] = _build_monthly_insight_context(
                    engine=engine,
                    user_id=user_id,
                    country=country,
                    month=payload["month"],
                    year=payload["year"],
                    product_match=state.get("product_match"),
                )
            except Exception as e:
                print("⚠️ insight skipped:", e)
                state["insight_context"] = None

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

            state["analysis_result"] = {
                "type": "comparison",
                "metric": metric_name,
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

    # -------------------------------
    # 🟢 DATA MODE EMAIL
    # -------------------------------
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
    # COMMON SETUP
    # -------------------------------
    user = User.query.filter_by(id=state["user_id"]).first()
    current_metrics = state.get("current_metrics", {})
    period_label = current_metrics.get("period_label", "selected period")

    subject = f"Phormula AI Summary - {state.get('country', 'uk').upper()} - {period_label}"

    # -------------------------------
    # 🔥 AI EMAIL (preferred path)
    # -------------------------------
    if state.get("use_ai_email", True):
        summary_text = build_ai_email_summary(state)
        safe_summary = summary_text.replace("\n", "<br>")

        html = f"""
        <p>Hi {(user.name if user else "there")},</p>
        <p>{safe_summary}</p>
        <br>
        <p>— Phormula AI</p>
        """

    # -------------------------------
    # 🟡 NORMAL EMAIL (fallback)
    # -------------------------------
    else:
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

    # -------------------------------
    # SEND EMAIL (single exit point)
    # -------------------------------
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

    print("\n🧾 FINAL NODE INPUT:")
    print({
        "intent": state.get("intent"),
        "metric_keys": list(state.get("current_metrics", {}).keys()),
        "analysis_type": state.get("analysis_type"),
        "multi_metric": state.get("multi_metric"),  # 🔥 NEW DEBUG
    })

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

            lines = ["Here’s the planning summary:"]
            for item in items[:3]:
                sku = item.get("sku", "Unknown SKU")
                summary = item.get("summary") or item.get("plan", {}).get("summary")

                if isinstance(summary, list) and summary:
                    lines.append(f"- {sku}: {summary[0]}")
                else:
                    lines.append(f"- {sku}: planning completed successfully.")

            state["final_response"] = "\n".join(lines)
            return state

        # CHAT MODE
        if state.get("intent") == "chat":
            history = state.get("chat_history", [])

            messages = [{"role": "system", "content": "You are a friendly business copilot."}]

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
        # 🔥 MULTI METRIC TREND (NEW)
        # -------------------------------
        analysis = state.get("analysis_result")
                # ===============================
        # 🔥 PRODUCTWISE BREAKDOWN RESPONSE
        # ===============================
        if analysis and analysis.get("type") == "breakdown":
            metric_name = analysis["metric"]
            period = analysis["period_label"]
            rows = analysis.get("per_sku", [])

            if not rows:
                state["final_response"] = f"No productwise breakdown found for {format_metric_name(metric_name)} in {period}."
                return state

            lines = [f"Productwise breakdown of {format_metric_name(metric_name)} in {period}:"]

            for r in rows:
                name = r.get("product_name") or r.get("sku") or "Unknown Product"
                val = format_metric_value(r.get("__metric__", 0), metric_name, state.get("country"))
                lines.append(f"- {name}: {val}")

            state["final_response"] = "\n".join(lines)
            return state

        # ===============================
        # 🔥 SKU RANKING RESPONSE
        # ===============================
        if analysis and analysis.get("type") == "sku_ranking":
            metric_name = analysis["metric"]
            period = analysis["period_label"]

            direction = state.get("ranking_direction", "both")

            lines = []

            # 🔥 TOP PRODUCTS
            if direction in ["top", "both"]:
                top_n = len(analysis["top"])
                lines.append(f"Top {top_n} products in {period}:")

                for r in analysis["top"]:
                    name = r.get("product_name") or r.get("sku") or "Unknown Product"
                    val = format_metric_value(r.get("__metric__", 0), metric_name, state.get("country"))
                    lines.append(f"- {name}: {val}")

            # 🔥 BOTTOM PRODUCTS
            if direction in ["bottom", "both"]:
                bottom_n = len(analysis["bottom"])

                if lines:
                    lines.append("")  # spacing

                lines.append(f"Bottom {bottom_n} products:")

                for r in analysis["bottom"]:
                    name = r.get("product_name") or r.get("sku") or "Unknown Product"
                    val = format_metric_value(r.get("__metric__", 0), metric_name, state.get("country"))
                    lines.append(f"- {name}: {val}")

            state["final_response"] = "\n".join(lines)
            return state
        # ===============================
        # 🔥 RANKING RESPONSE
        # ===============================
        if analysis and analysis.get("type") == "ranking":
            metric_name = analysis["metric"]
            best = analysis["best"]
            worst = analysis["worst"]
            product = state.get("product_match")

            best_val = format_metric_value(best["__metric__"], metric_name, state.get("country"))
            worst_val = format_metric_value(worst["__metric__"], metric_name, state.get("country"))

            if product:
                response = (
                    f"Best month for {product} in terms of {format_metric_name(metric_name)} "
                    f"was {best['period_label']} ({best_val}).\n"
                    f"Worst was {worst['period_label']} ({worst_val})."
                )
            else:
                response = (
                    f"Your best month in terms of {format_metric_name(metric_name)} "
                    f"was {best['period_label']} ({best_val}).\n"
                    f"Worst was {worst['period_label']} ({worst_val})."
                )

            state["final_response"] = response
            return state

        if state.get("multi_metric") and isinstance(analysis, dict):
            lines = ["Overall business trends:"]

            for metric_name, res in analysis.items():
                overall = res.get("overall_pct_change")

                if overall is not None:
                    direction = "increased" if overall > 0 else "decreased"
                    clean_name = format_metric_name(metric_name)
                    lines.append(f"- {clean_name}: {direction} by {abs(overall):.2f}%")

            state["final_response"] = "\n".join(lines)
            return state

        # -------------------------------
        # 🔥 SINGLE METRIC TREND
        # -------------------------------
        if analysis and analysis.get("series"):
            metric_name = analysis["metric"]
            series = analysis["series"]
            mom = analysis.get("mom", [])
            product = state.get("product_match")
            overall = analysis.get("overall_pct_change")

            lines = []

            if product:
                lines.append(f"{metric_name} trend for {product}:")
            else:
                lines.append(f"{metric_name} trend:")

            if overall:
                direction = "increased" if overall > 0 else "decreased"
                lines.append(f"Overall, {metric_name} {direction} by {abs(overall):.2f}%.")

            clean_name = format_metric_name(metric_name)

            lines.append(f"\n{clean_name} Timeline:")

            for s in series:
                val = format_metric_value(s["__metric__"], metric_name, state.get("country"))
                lines.append(f"• {s['period_label']} → {val}")

            if mom:
                lines.append("\nMonth-on-month change:")
                for row in mom:
                    delta = format_metric_value(row["delta"], metric_name, state.get("country"))
                    pct = row.get("pct_change")
                    if pct:
                        lines.append(f"- {row['period_label']}: {delta} ({pct:+.2f}%)")
                    else:
                        lines.append(f"- {row['period_label']}: {delta}")

            state["final_response"] = "\n".join(lines)
            return state

        # -------------------------------
        # 🔥 INSIGHT MODE
        # -------------------------------
        insight = state.get("insight_context")
        if insight:
            m = insight["metrics"]
            c = insight["changes"]
            driver = insight.get("driver")

            lines = []

            sales = c.get("net_sales", {}).get("pct_change")
            profit = c.get("profit", {}).get("pct_change")

            if sales:
                lines.append("Performance improved compared to last month." if sales > 0 else "Performance declined compared to last month.")

            qty = c.get("total_quantity", {}).get("pct_change")
            asp = c.get("asp", {}).get("pct_change")

            if qty and asp:
                if qty > asp:
                    lines.append("Growth was volume-driven.")
                else:
                    lines.append("Growth was price-driven.")

            if sales and profit and profit < sales:
                lines.append("Profit grew slower than sales, indicating margin pressure.")

            if driver and driver.get("primary_driver"):
                name = driver["primary_driver"].get("product_name") or driver["primary_driver"].get("sku")
                if name:
                    lines.append(f"Growth was mainly driven by {name}.")

            lines.append("\nKey metrics:")
            for k, v in m.items():
                pct = c.get(k, {}).get("pct_change")
                val = format_metric_value(v, k, state.get("country"))
                if pct:
                    clean_name = format_metric_name(k)
                    lines.append(f"- {clean_name}: {val} ({pct:+.2f}%)")
                else:
                    clean_name = format_metric_name(k)
                    lines.append(f"- {clean_name}: {val}")

            state["final_response"] = "\n".join(lines)
            return state

        # -------------------------------
        # METRICS
        # -------------------------------
        metric = state.get("current_metrics", {})
        comparison = state.get("comparison", {})
        intent = state.get("intent", "")

        country = state.get("country", "").lower()
        metric_name = metric.get("metric")
        period_label = metric.get("period_label", "selected period")

        per_sku = metric.get("per_sku", [])
        product_filter = state.get("product_match")

        print("\n🔍 PRODUCT MATCH DEBUG:")
        print("Stored Product:", product_filter)

        # COMPARISON
        if comparison:
            left = comparison.get("left", {})
            right = comparison.get("right", {})

            curr = format_metric_value(left.get("total", 0), metric_name, country)
            prev = format_metric_value(right.get("total", 0), metric_name, country)
            pct = comparison.get("pct_change")

            if pct:
                direction = "higher" if pct > 0 else "lower"
                state["final_response"] = f"{metric_name} was {curr} vs {prev} ({abs(pct):.2f}% {direction})."
            else:
                state["final_response"] = f"{metric_name} was {curr} vs {prev}."

            return state

        # STANDARD QA
        if intent in {"metric_qa", "report", "email", "top_skus", "loss_making_skus", "advice"}:
            if product_filter:
                # ✅ exact match exists
                if any(product_filter == str(r.get("product_name", "")).lower() for r in per_sku):
                    rows = [
                        r for r in per_sku
                        if str(r.get("product_name", "")).lower() == product_filter
                    ]
                else:
                    # ✅ group match (e.g. "classic" → classic blue, red, etc.)
                    rows = [
                        r for r in per_sku
                        if product_filter in str(r.get("product_name", "")).lower()
                    ]

                value = sum(float(r.get("__metric__", 0)) for r in rows)
            else:
                value = metric.get("total", 0)

            formatted = format_metric_value(value, metric_name, country)
            clean_name = format_metric_name(metric_name)
            state["final_response"] = f"In {period_label}, your {clean_name} was {formatted}."
            return state

        # FALLBACK
        val = format_metric_value(metric.get("total", 0), metric_name, country)
        state["final_response"] = f"In {period_label}, your total {metric_name} was {val}."
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
    print("✅ Graph compiled successfully")
    return graph.compile()
