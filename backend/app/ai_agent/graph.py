from __future__ import annotations
import json
import logging
import os
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Set, Tuple
from types import SimpleNamespace 
from langchain_openai import ChatOpenAI
from pydantic import BaseModel, Field
from sqlalchemy import text
from app.utils.live_bi_utils import generate_sku_inventory_flags
from app.ai_agent.db import _format_value, fetch_period_dfs, get_engine, latest_available_month, get_metric_def, validate_metric_compatibility, MonthKey, INVENTORY_METRICS, get_inventory_snapshot, FINANCE_METRICS, get_amazon_engine, get_inventory_forecast_snapshot, get_pnl_forecast_snapshot
from app.ai_agent.email_service import send_agent_email, build_email_html, build_excel_attachment
from app.ai_agent.formula_engine import (
    OVERALL_MONTH_METRICS,
    build_time_series_analysis,
    compare_periods,
    find_extreme_month,
    get_growth_driver_insights,
    get_last_n_month_keys,
    get_metric_for_month,
    get_metric_for_multiple_months,
    get_metric_for_period,
    get_metric_pack_for_month,
    get_multi_dimensional_data,
    get_product_metric_pack_for_month,
    parse_period,
    rank_skus,
)
from app.ai_agent.business_context import build_business_context
from app.ai_agent.memory import load_last_analysis_from_history
from app.ai_agent.prompts import ADVICE_PROMPT, BUSINESS_ADVISOR_PROMPT, REASONING_PROMPT, REQUEST_PLANNER_PROMPT
from app.ai_agent.state import AgentState

logger = logging.getLogger(__name__)

try:
    from app.utils.agent_utils import amazon_engine, build_plan_langgraph, phormula_engine
    EVENT_PLANNER_AVAILABLE = True
except Exception:
    EVENT_PLANNER_AVAILABLE = False
    build_plan_langgraph = None
    phormula_engine = None
    amazon_engine = None


OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
LLM_ENABLED = bool(OPENAI_API_KEY)

planner_llm = ChatOpenAI(model="gpt-4.1", api_key=OPENAI_API_KEY, temperature=0) if LLM_ENABLED else None
chat_llm = ChatOpenAI(model="gpt-4.1", api_key=OPENAI_API_KEY, temperature=0.5) if LLM_ENABLED else None
explain_llm = ChatOpenAI(model="gpt-4.1", api_key=OPENAI_API_KEY, temperature=0.2) if LLM_ENABLED else None
advisor_llm = ChatOpenAI(model="gpt-4.1", api_key=OPENAI_API_KEY, temperature=0.2) if LLM_ENABLED else None


ALIAS_MAP = {

    "quantity": "total_quantity",
    "return quantity": "return_quantity",
    "return units": "return_quantity",
    "total quantity": "total_quantity",
    "net units" : "total_quantity",
    "units" : "total_quantity",

    "product sales": "product_sales",
    "product sales tax": "product_sales_tax",
    "postage credits": "postage_credits",
    "gift wrap credits": "gift_wrap_credits",
    "gift wrap tax": "giftwrap_credits_tax",
    "shipping credits": "shipping_credits",
    "shipping credits tax": "shipping_credits_tax",

    "promotional rebates": "promotional_rebates",
    "promotional rebates tax": "promotional_rebates_tax",

    "refund sales": "refund_sales",
    "sales tax refund": "sales_tax_refund",
    "sales credit refund": "sales_credit_refund",
    "refund rebate": "refund_rebate",

    "marketplace tax": "marketplace_facilitator_tax",
    "digital tax": "digital_transaction_tax",

    "selling fees": "selling_fees",
    "refund selling fees": "refund_selling_fees",
    "fba fees": "fba_fees",
    "amazon fee": "amazon_fee",

    "platform fee": "platform_fee",
    "platform fee new": "platformfeenew",
    "inventory storage fee": "platform_fee_inventory_storage",
    "inventory storage fees": "platform_fee_inventory_storage",

    "profit": "profit",
    "cm1 profit": "profit",

    # CM2 productwise / total separation
    "productwise cm2 profit": "cm2_profit",
    "product wise cm2 profit": "cm2_profit",
    "by product cm2 profit": "cm2_profit",
    "sku wise cm2 profit": "cm2_profit",
    "sku cm2 profit": "cm2_profit",

    "total cm2 profit": "total_cm2_profit",
    "overall cm2 profit": "total_cm2_profit",
    "monthly cm2 profit": "total_cm2_profit",
    "whole month cm2 profit": "total_cm2_profit",

    # Default CM2 query should use total/month CM2
    "cm2 profit": "total_cm2_profit",

    "reimbursement for lost inventory": "lost_total",

    # Ads productwise / total separation
    "productwise ads": "ads_spend",
    "product wise ads": "ads_spend",
    "by product ads": "ads_spend",
    "per product ads": "ads_spend",
    "sku wise ads": "ads_spend",
    "sku-wise ads": "ads_spend",
    "sku ads": "ads_spend",
    "productwise ad spend": "ads_spend",
    "product wise ad spend": "ads_spend",
    "ad spend by product": "ads_spend",
    "ads spend by product": "ads_spend",

    "total ads": "total_ads",
    "overall ads": "total_ads",
    "monthly ads": "total_ads",
    "whole month ads": "total_ads",
    "total ad spend": "total_ads",
    "overall ad spend": "total_ads",
    "monthly ad spend": "total_ads",
    "whole month ad spend": "total_ads",

    # Sponsored Product spend
    "sponsored product spend": "product_spend",
    "sponsored products spend": "product_spend",
    "sponsor product spend": "product_spend",
    "sponsor products spend": "product_spend",
    "spend on sponsored product": "product_spend",
    "sp ads spend": "product_spend",
    "sp spend": "product_spend",
    "product ads spend": "product_spend",
    "product ad spend": "product_spend",

    # Sponsored Brand spend
    "sponsored brand spend": "brand_spend",
    "sponsored brands spend": "brand_spend",
    "sponsor brand spend": "brand_spend",
    "sponsor brands spend": "brand_spend",
    "sb ads spend": "brand_spend",
    "sb spend": "brand_spend",
    "brand ads spend": "brand_spend",
    "brand ad spend": "brand_spend",

    # Sponsored Display spend
    "sponsored display spend": "display_spend",
    "sponsor display spend": "display_spend",
    "sd ads spend": "display_spend",
    "sd spend": "display_spend",
    "display ads spend": "display_spend",
    "display ad spend": "display_spend",

    # Sponsored Product sales
    "sponsored product sales": "sp_ads_sales",
    "sponsored products sales": "sp_ads_sales",
    "sponsor product sales": "sp_ads_sales",
    "sp ads sales": "sp_ads_sales",
    "sp sales": "sp_ads_sales",
    "product ads sales": "sp_ads_sales",

    # Sponsored Brand sales
    "sponsored brand sales": "sb_ads_sales",
    "sponsored brands sales": "sb_ads_sales",
    "sponsor brand sales": "sb_ads_sales",
    "sb ads sales": "sb_ads_sales",
    "sb sales": "sb_ads_sales",
    "brand ads sales": "sb_ads_sales",

    # Sponsored Display sales
    "sponsored display sales": "sd_ads_sales",
    "sponsor display sales": "sd_ads_sales",
    "sd ads sales": "sd_ads_sales",
    "sd sales": "sd_ads_sales",
    "display ads sales": "sd_ads_sales",

    # Default ads query should use total/month ads
    "ads": "total_ads",
    "ad spend": "total_ads",
    "ads spend": "total_ads",
    "advertisement": "total_ads",
    "advertising": "total_ads",
    "advertising spend": "total_ads",

    # Default ads query should use total/month ads
    "ads": "total_ads",
    "ad spend": "total_ads",
    "ads spend": "total_ads",
    "advertisement": "total_ads",
    "advertising": "total_ads",
    "advertising spend": "total_ads",

    "visible ads": "visible_ads",
    "voucher ads": "dealsvouchar_ads",

    "reimbursement fee": "rembursement_fee",

    "miscellaneous transaction": "misc_transaction",
    "other transactions": "other",

    "shipment charges": "shipment_charges",

   
    "shipment fees": "shipment_fees",
    "shipping fees": "shipment_fees",

    "unit profit": "unit_wise_profitability",
    "cm1 profit per unit": "unit_wise_profitability",
    "profit per unit" : "unit_wise_profitability",
    "asp": "asp",

    "sales mix": "sales_mix",
    "profit mix": "profit_mix",

    "stock": "available",
    "available stock": "available",
    "inventory": "available",
    "available inventory": "available",
    "sell through": "sell_through",
    "days of supply": "days_of_supply",

}

STOPWORDS = {
    "what", "were", "was", "is", "are", "the", "my", "show", "me", "for", "in", "on", "of", "to",
    "and", "or", "with", "by", "did", "go", "up", "down", "how", "has", "have", "compare", "vs",
    "versus", "last", "month", "months", "this", "that", "why", "from", "rest", "business", "top",
    "bottom", "products", "product", "sku", "goal", "target", "sales", "profit", "net", "gross",
    "spend", "advertising", "acos", "asp", "units", "summary", "plan", "event", "build", "underperforming",
}

class RequestPlanModel(BaseModel):
    intent: str = "chat"
    analysis_type: str = "absolute"
    reasoning_mode: str = "lookup"
    task_type: str = "value_lookup"
    metric_name: Optional[str] = None
    product_query: Optional[str] = None
    metric_names: Optional[List[str]] = None
    product_queries: Optional[List[str]] = None
    needs_advice: bool = False
    needs_forecast_data: bool = False
    response_mode: str = "short"
    clarification_question: Optional[str] = None
    top_n: Optional[int] = None
    answer_shape: Optional[str] = None
    subject_scope: Optional[str] = None
    ranking_direction: Optional[str] = None
    extreme_type: Optional[str] = None
    time_granularity: Optional[str] = None
    target_months: Optional[List[Dict[str, int]]] = None
    event_name: Optional[str] = None
    last_event_month: Optional[int] = Field(default=None, ge=1, le=12)
    future_event_month: Optional[int] = Field(default=None, ge=1, le=12)
    target_sales: Optional[float] = None

class FollowupResolutionModel(BaseModel):
    is_followup: bool = False
    reuse_previous_period: bool = False
    metric_name: Optional[str] = None
    analysis_type: Optional[str] = None
    answer_shape: Optional[str] = None
    ranking_direction: Optional[str] = None
    dimension: Optional[str] = None
    top_n: Optional[int] = None
    reason: Optional[str] = None

class CompositeResolutionModel(BaseModel):
    is_composite: bool = False
    base_metric_name: Optional[str] = None
    contribution_metric_name: Optional[str] = None
    extreme_type: Optional[str] = None
    extreme_rank: Optional[int] = None
    dimension: Optional[str] = None
    top_n: Optional[int] = None
    reason: Optional[str] = None


@dataclass
class RequestPlan:
    intent: str
    analysis_type: str
    reasoning_mode: str
    task_type: str
    needs_advice: bool
    response_mode: str
    needs_forecast_data: bool = False
    metric_name: Optional[str] = None
    dimension: Optional[str] = None
    product_query: Optional[str] = None
    metric_names: Optional[List[str]] = None
    product_queries: Optional[List[str]] = None
    clarification_question: Optional[str] = None
    top_n: Optional[int] = None
    answer_shape: Optional[str] = None
    subject_scope: Optional[str] = None
    ranking_direction: Optional[str] = None
    extreme_type: Optional[str] = None
    time_granularity: Optional[str] = None
    target_months: Optional[List[Dict[str, int]]] = None
    event_name: Optional[str] = None
    last_event_month: Optional[int] = None
    future_event_month: Optional[int] = None
    target_sales: Optional[float] = None


class SimpleGraph:
    def invoke(self, state: AgentState) -> AgentState:
        try:
            return _invoke_agent(state)
        except Exception as exc:
            logger.exception("Agent invocation failed")
            state["error"] = str(exc)
            state["final_response"] = f"I couldn't process that request reliably: {exc}"
            return state


def _normalize(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").strip().lower())



def _extract_requested_countries(query: str) -> List[str]:
    q = _normalize(query)

    countries = []

    # ✅ Add global detection first
    if re.search(r"\bglobal\b|\bworldwide\b|\ball countries\b|\boverall\b", q):
        countries.append("global")

    if re.search(r"\buk\b|\bunited kingdom\b", q):
        countries.append("uk")

    if re.search(r"\bus\b|\busa\b|\bunited states\b", q):
        countries.append("us")

    if "both countries" in q or "both country" in q:
        countries = ["uk", "us"]

    return list(dict.fromkeys(countries))

def _safe_div(numerator: float, denominator: float) -> float:
    return 0.0 if denominator == 0 else float(numerator) / float(denominator)


def _canonicalize(text: str) -> str:
    text = _normalize(text)

    # normalize separators
    text = text.replace("-", " ").replace("_", " ")

    # merge split alphanumeric tokens (cm 2 → cm2)
    text = re.sub(r"([a-z])\s+(\d)", r"\1\2", text)
    text = re.sub(r"(\d)\s+([a-z])", r"\1\2", text)

    return text


def _metric_from_query(query: str) -> Optional[str]:
    q = _canonicalize(query)

    best_match = None
    best_len = 0

    for phrase, metric in ALIAS_MAP.items():
        if phrase in q and len(phrase) > best_len:
            best_match = metric
            best_len = len(phrase)

    return best_match


def _match_product(rows: List[Dict[str, Any]], product_query: Optional[str]) -> Optional[str]:
    if not rows or not product_query:
        return None
    pq = product_query.lower().strip()
    exact = [r for r in rows if pq == str(r.get("product_name", "")).lower().strip()]
    if exact:
        return pq
    contains = [r for r in rows if pq in str(r.get("product_name", "")).lower().strip()]
    if contains:
        return str(contains[0].get("product_name", "")).lower().strip() or pq
    return None


def _latest_month_result(
    engine: Any,
    user_id: int,
    country: str,
    metric_name: str
) -> Dict[str, Any]:

    # -------- INVENTORY FLOW --------
    if metric_name in INVENTORY_METRICS:
        now = datetime.utcnow()
        return get_inventory_snapshot(
            user_id=user_id,
            metric_name=metric_name,
            month=now.month,
            year=now.year,
            country=country,
        )

    # -------- FINANCE FLOW --------
    latest = latest_available_month(engine, user_id, country)

    return get_metric_for_month(
        engine,
        user_id,
        country,
        metric_name,
        latest.month,
        latest.year,
    )

def _prepare_period_payload(parsed: Dict[str, Any], analysis_type: str) -> Dict[str, Any]:
    ptype = parsed.get("type", "latest_month")
    if analysis_type == "growth" and ptype in {"latest_month", "single", "range", "year", "last_n", "multi_month"}:
        return {"type": "growth_base", "base": parsed}
    if ptype == "comparison":
        return {"type": "comparison", "p1": parsed["left"], "p2": parsed["right"]}
    if ptype == "range":
        return parsed
    if ptype == "last_n":
        return {"type": "last_n_months", "n": parsed["n"]}
    if ptype == "single":
        return parsed
    if ptype == "multi_month":
        return {"type": "multi_month", "months": parsed.get("months", [])}
    if ptype == "year":
        return {"type": "range", "start_month": 1, "start_year": parsed["year"], "end_month": 12, "end_year": parsed["year"]}
    return {"type": "latest_month"}


def _previous_period_for_base(base: Dict[str, Any]) -> Dict[str, Any]:
    ptype = base.get("type", "latest_month")
    if ptype == "single":
        m = int(base["month"])
        y = int(base["year"])
        pm, py = (12, y - 1) if m == 1 else (m - 1, y)
        return {"type": "single", "month": pm, "year": py}
    if ptype == "year":
        return {"type": "year", "year": int(base["year"]) - 1}
    if ptype == "range":
        sm = int(base["start_month"])
        sy = int(base["start_year"])
        em = int(base["end_month"])
        ey = int(base["end_year"])
        span = (ey - sy) * 12 + (em - sm) + 1
        end_index = (sy * 12 + sm - 1) - 1
        start_index = end_index - span + 1
        prev_sy, prev_sm = divmod(start_index, 12)
        prev_sm += 1
        prev_ey, prev_em = divmod(end_index, 12)
        prev_em += 1
        return {"type": "range", "start_month": prev_sm, "start_year": prev_sy, "end_month": prev_em, "end_year": prev_ey}
    return {"type": "latest_month"}


_MONTH_TOKEN_TO_NUM = {
    "jan": 1,
    "january": 1,
    "feb": 2,
    "february": 2,
    "mar": 3,
    "march": 3,
    "apr": 4,
    "april": 4,
    "may": 5,
    "jun": 6,
    "june": 6,
    "jul": 7,
    "july": 7,
    "aug": 8,
    "august": 8,
    "sep": 9,
    "sept": 9,
    "september": 9,
    "oct": 10,
    "october": 10,
    "nov": 11,
    "november": 11,
    "dec": 12,
    "december": 12,
}


def _explicit_month_comparison_from_query(query: str) -> Optional[Dict[str, Any]]:
    q = _normalize(query)
    if not any(token in q for token in ["compare", "compared", " vs ", " versus ", " than "]):
        return None

    pattern = re.compile(
        r"\b("
        r"jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|"
        r"jul(?:y)?|aug(?:ust)?|sep(?:t|tember)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?"
        r")\b(?:\s*['-]?\s*(20\d{2}|\d{2}))?",
        re.IGNORECASE,
    )
    refs: List[Dict[str, int]] = []
    explicit_years: List[int] = []

    for match in pattern.finditer(q):
        month_token = match.group(1).lower()
        year_text = match.group(2)
        month = _MONTH_TOKEN_TO_NUM.get(month_token)
        if not month:
            continue
        year = None
        if year_text:
            year = int(year_text)
            if year < 100:
                year += 2000
            explicit_years.append(year)
        refs.append({"month": month, "year": year or 0})

    if len(refs) < 2:
        return None

    fallback_year = explicit_years[0] if explicit_years else datetime.today().year
    left = refs[0]
    right = refs[1]
    left_year = left["year"] or fallback_year
    right_year = right["year"] or fallback_year

    return {
        "type": "comparison",
        "left": {
            "start_month": left["month"],
            "start_year": left_year,
            "end_month": left["month"],
            "end_year": left_year,
        },
        "right": {
            "start_month": right["month"],
            "start_year": right_year,
            "end_month": right["month"],
            "end_year": right_year,
        },
    }


def _last_n_window(engine: Any, user_id: int, country: str, n: int, *, extra_for_mom: bool = False) -> List[Any]:
    requested_n = max(int(n or 1), 1)
    count = requested_n + 1 if extra_for_mom else requested_n
    return get_last_n_month_keys(engine, user_id, country, count)


def _overall_metric_for_payload(engine: Any, user_id: int, country: str, metric_name: str, payload: Dict[str, Any]) -> float:
    ptype = payload.get("type")
    if ptype == "single":
        result = get_metric_for_month(engine, user_id, country, metric_name, payload["month"], payload["year"])
        return float(result.get("total", 0.0))
    if ptype == "last_n_months":
        total = 0.0
        for mk in _last_n_window(engine, user_id, country, payload["n"]):
            result = get_metric_for_month(engine, user_id, country, metric_name, mk.month, mk.year)
            total += float(result.get("total", 0.0))
        return total
    if ptype == "range":
        result = get_metric_for_period(engine, user_id, country, metric_name, payload["start_month"], payload["start_year"], payload["end_month"], payload["end_year"], skip_missing=True)
        return float(result.get("total", 0.0))
    latest = latest_available_month(engine, user_id, country)
    result = get_metric_for_month(engine, user_id, country, metric_name, latest.month, latest.year)
    return float(result.get("total", 0.0))


def _enrich_pack_with_mix(engine: Any, user_id: int, country: str, payload: Dict[str, Any], pack: Dict[str, float]) -> Dict[str, float]:
    enriched = dict(pack)
    total_sales = _overall_metric_for_payload(engine, user_id, country, "net_sales", payload)
    total_profit = _overall_metric_for_payload(engine, user_id, country, "profit", payload)
    enriched["sales_mix"] = _safe_div(float(enriched.get("net_sales", 0.0)), total_sales)
    enriched["profit_mix"] = _safe_div(float(enriched.get("profit", 0.0)), total_profit)
    return enriched


def _compute_root_cause(current_pack: Dict[str, float], previous_pack: Dict[str, float]) -> Dict[str, Any]:
    if not previous_pack:
        return {"primary_driver": None, "drivers": [], "summary": []}
    sales_delta = float(current_pack.get("net_sales", 0.0)) - float(previous_pack.get("net_sales", 0.0))
    profit_delta = float(current_pack.get("profit", 0.0)) - float(previous_pack.get("profit", 0.0))
    units_delta = float(current_pack.get("total_quantity", 0.0)) - float(previous_pack.get("total_quantity", 0.0))
    asp_delta = float(current_pack.get("asp", 0.0)) - float(previous_pack.get("asp", 0.0))
    prev_units = float(previous_pack.get("total_quantity", 0.0))
    prev_asp = float(previous_pack.get("asp", 0.0))
    units_effect_on_sales = units_delta * prev_asp
    asp_effect_on_sales = sales_delta - units_effect_on_sales
    prev_margin_per_unit = _safe_div(float(previous_pack.get("profit", 0.0)), prev_units)
    units_effect_on_profit = units_delta * prev_margin_per_unit
    profit_quality_effect = profit_delta - units_effect_on_profit
    drivers = [
        {"driver": "sales", "delta": sales_delta, "direction": "up" if sales_delta >= 0 else "down"},
        {"driver": "profit", "delta": profit_delta, "direction": "up" if profit_delta >= 0 else "down"},
        {"driver": "units", "delta": units_delta, "direction": "up" if units_delta >= 0 else "down", "estimated_sales_impact": units_effect_on_sales, "estimated_profit_impact": units_effect_on_profit},
        {"driver": "asp", "delta": asp_delta, "direction": "up" if asp_delta >= 0 else "down", "estimated_sales_impact": asp_effect_on_sales},
        {"driver": "profit_quality", "delta": profit_delta, "direction": "up" if profit_delta >= 0 else "down", "estimated_profit_impact": profit_quality_effect},
    ]
    ranked = sorted(drivers, key=lambda d: max(abs(float(d.get("estimated_profit_impact", 0.0))), abs(float(d.get("estimated_sales_impact", 0.0))), abs(float(d.get("delta", 0.0)))), reverse=True)
    summary: List[str] = []
    if abs(profit_delta) > 0:
        summary.append(f"Profit changed by {profit_delta:,.2f}.")
    if abs(sales_delta) > 0:
        summary.append(f"Sales changed by {sales_delta:,.2f}.")
    if abs(units_delta) > 0:
        summary.append(f"Units changed by {units_delta:,.2f}, contributing about {units_effect_on_sales:,.2f} to sales movement.")
    if abs(asp_delta) > 0:
        summary.append(f"ASP changed by {asp_delta:,.2f}, contributing about {asp_effect_on_sales:,.2f} to sales movement.")
    return {"primary_driver": ranked[0]["driver"] if ranked else None, "drivers": ranked, "summary": summary}


def _query_ngrams(query: str, max_n: int = 4) -> List[str]:
    words = re.findall(r"[a-z0-9][a-z0-9'&-]*", _normalize(query))
    words = [w for w in words if w not in STOPWORDS]
    out: List[str] = []
    for n in range(max_n, 0, -1):
        for i in range(0, len(words) - n + 1):
            out.append(" ".join(words[i:i+n]))
    seen: Set[str] = set()
    deduped: List[str] = []
    for item in out:
        if item and item not in seen:
            seen.add(item)
            deduped.append(item)
    return deduped


def _resolve_product_queries_from_data(engine: Any, user_id: int, country: str, query: str) -> List[str]:
    try:
        latest = latest_available_month(engine, user_id, country)
        sample = get_metric_for_month(engine, user_id, country, "net_sales", latest.month, latest.year)
        names = []
        for row in sample.get("per_sku", []):
            name = str(row.get("product_name") or "").strip().lower()
            if name:
                names.append(name)
        if not names:
            return []
        q = _normalize(query)
        exact = [name for name in names if re.search(rf"\b{re.escape(name)}\b", q)]
        if exact:
            return list(dict.fromkeys(exact))[:5]
        ngrams = _query_ngrams(query)
        matches: List[str] = []
        for ng in ngrams:
            for name in names:
                if ng == name or ng in name or name in ng:
                    matches.append(name)
        return list(dict.fromkeys(matches))[:5]
    except Exception:
        logger.debug("Product resolution from data failed", exc_info=True)
        return []


def _fallback_plan(query: str, email_requested: bool = False) -> RequestPlan:
    q = _normalize(query)
    metric_name = _metric_from_query(q)
    wants_detailed_response = any(
        x in q for x in ["detailed", "in detail", "full report", "deep dive", "comprehensive"]
    )
    if any(x in q for x in ["hello", "hi", "hey", "thanks", "thank you"]):
        return RequestPlan("chat", "absolute", "lookup", "value_lookup", False, "short")
    if any(x in q for x in ["what is", "what does", "explain", "meaning of"]):
        if metric_name and "my" not in q and "last" not in q and "month" not in q:
            return RequestPlan("explain", "absolute", "lookup", "value_lookup", False, "short", metric_name=metric_name)
    analysis_type = "growth" if any(x in q for x in ["change", "growth", "increase", "decrease", "trend", "drop"]) else "absolute"
    reasoning_mode = "decision" if any(x in q for x in ["improve", "optimize", "should", "recommend", "fix"]) else ("analysis" if any(x in q for x in ["why", "reason", "underperform"]) or analysis_type == "growth" else "lookup")
    answer_shape = "trend" if analysis_type == "growth" else "single_value"
    ranking_direction = None
    top_n = None
    if any(x in q for x in ["top", "best"]):
        answer_shape = "ranking"
        ranking_direction = "top"
    if any(x in q for x in ["worst", "lowest", "bottom"]):
        answer_shape = "ranking"
        ranking_direction = "bottom"
    if any(x in q for x in ["highest", "peak", "maximum"]):
        answer_shape = "extreme"
    if any(x in q for x in ["summary", "how is my business doing"]):
        answer_shape = "summary"
        analysis_type = "summary"
    subject_scope = "product" if any(x in q for x in ["product", "sku", " vs ", " versus "]) else "metric"
    if subject_scope == "product" and answer_shape == "extreme":
        answer_shape = "ranking"
        ranking_direction = "top" if any(x in q for x in ["highest", "peak", "maximum", "best"]) else "bottom"
        top_n = 1
    return RequestPlan(
        intent="email" if email_requested else "metric_qa",
        analysis_type=analysis_type,
        reasoning_mode=reasoning_mode,
        task_type="recommendation" if reasoning_mode == "decision" else ("diagnosis" if reasoning_mode == "analysis" else "value_lookup"),
        needs_advice=reasoning_mode in {"analysis", "decision"},
        response_mode="detailed" if wants_detailed_response else "short",
        metric_name=metric_name,
        answer_shape=answer_shape,
        top_n=top_n,
        ranking_direction=ranking_direction,
        extreme_type="max" if any(x in q for x in ["highest", "peak", "maximum"]) else None,
        dimension="sku" if subject_scope == "product" else "time",
        subject_scope=subject_scope,
    )


def _plan_request(query: str, email_requested: bool = False) -> RequestPlan:
    fallback = _fallback_plan(query, email_requested=email_requested)
    if not planner_llm:
        return fallback
    try:
        planner = planner_llm.with_structured_output(RequestPlanModel)
        result = planner.invoke(REQUEST_PLANNER_PROMPT + "\n\nUser query:\n" + query)
        metric_names = [m for m in (result.metric_names or []) if m]
        product_queries = [p for p in (result.product_queries or []) if p]
        metric_name = None if metric_names else (fallback.metric_name or result.metric_name)
        product_query = None if product_queries else result.product_query
        q = _normalize(query)
        subject_scope = result.subject_scope or fallback.subject_scope or "metric"
        answer_shape = result.answer_shape or fallback.answer_shape
        analysis_type = result.analysis_type or fallback.analysis_type
        reasoning_mode = result.reasoning_mode or fallback.reasoning_mode
        task_type = result.task_type or fallback.task_type
        wants_detailed_response = any(
            x in q for x in ["detailed", "in detail", "full report", "deep dive", "comprehensive"]
        )
        response_mode = result.response_mode or fallback.response_mode
        if not wants_detailed_response and reasoning_mode in {"analysis", "decision"}:
            response_mode = "short"
        if analysis_type != "growth" and any(word in q for word in ["change", "increase", "decrease", "growth", "decline", "drop"]):
            analysis_type = "growth"
        if reasoning_mode != "decision" and any(trigger in q for trigger in ["improve", "optimize", "should", "recommend", "fix"]):
            reasoning_mode = "decision"
            task_type = "recommendation"
        elif reasoning_mode == "lookup" and any(trigger in q for trigger in ["why", "reason", "underperform"]):
            reasoning_mode = "analysis"
            task_type = "diagnosis"
        if not wants_detailed_response and reasoning_mode in {"analysis", "decision"}:
            response_mode = "short"
        if analysis_type == "growth":
            answer_shape = "trend"
        dimension = "sku" if (subject_scope in {"product", "products"} or product_query or product_queries) else fallback.dimension
        ranking_direction = result.ranking_direction or fallback.ranking_direction
        q_asks_sku_ranking = (
            any(phrase in q for phrase in ["which sku", "which skus", "which product", "which products", "top sku", "top product"])
            or (
                any(token in q for token in ["sku", "product"])
                and any(token in q for token in ["highest", "top", "best", "lowest", "bottom", "worst"])
            )
        )
        top_n = result.top_n or fallback.top_n
        if q_asks_sku_ranking:
            dimension = "sku"
            subject_scope = "product"
            answer_shape = "ranking"
            ranking_direction = "bottom" if any(token in q for token in ["lowest", "bottom", "worst"]) else "top"
            if top_n is None and any(token in q for token in ["which", "highest", "lowest", "best", "worst"]):
                top_n = 1
        return RequestPlan(
            intent=result.intent or fallback.intent,
            analysis_type=analysis_type,
            reasoning_mode=reasoning_mode,
            task_type=task_type,
            needs_advice=bool(result.needs_advice or reasoning_mode in {"analysis", "decision"}),
            needs_forecast_data=bool(result.needs_forecast_data or fallback.needs_forecast_data),
            response_mode=response_mode,
            metric_name=metric_name,
            dimension=dimension,
            product_query=product_query,
            metric_names=metric_names or (None if not metric_name else [metric_name]),
            product_queries=product_queries or None,
            clarification_question=result.clarification_question,
            top_n=top_n,
            answer_shape=answer_shape,
            subject_scope=subject_scope,
            ranking_direction=ranking_direction or result.ranking_direction or fallback.ranking_direction,
            extreme_type=result.extreme_type or fallback.extreme_type,
            time_granularity=result.time_granularity,
            target_months=result.target_months,
            event_name=result.event_name,
            last_event_month=result.last_event_month,
            future_event_month=result.future_event_month,
            target_sales=result.target_sales,
        )
    except Exception:
        logger.exception("Planner failed; using heuristic fallback")
        return fallback


def _build_tool_plan(state: AgentState) -> List[str]:

    if _is_forecast_request(state):
        logger.info("[ROUTE_FIX] forecast request -> forecast_analysis")
        return ["forecast_analysis"]

    if state.get("target_countries") and len(state.get("target_countries") or []) > 1:
        return ["multi_country"]
    
    # -------- 🔥 COMPOSITE PRIORITY --------
    # Handles queries like:
    # "Which product contributed most to the second highest profit month?"
    if state.get("analysis_type") == "ranked_extreme_contribution":
        logger.info("[ROUTE_FIX] ranked extreme contribution → ranked_extreme_contribution tool")
        return ["ranked_extreme_contribution"]

    # -------- 🔥 EXTREME PRIORITY --------
    if state.get("dimension") == "sku" and state.get("answer_shape") in {"ranking", "extreme"}:
        state["answer_shape"] = "ranking"
        if not state.get("ranking_direction"):
            state["ranking_direction"] = "bottom" if state.get("extreme_type") == "min" else "top"
        if not state.get("top_n") and (state.get("extreme_type") or "which " in (state.get("user_query") or "").lower()):
            state["top_n"] = 1
        logger.info("[ROUTE_FIX] SKU/product extreme request -> ranking tool")
        return ["ranking"]

    if state.get("answer_shape") == "extreme":
        logger.info("[ROUTE_FIX] extreme → extreme tool")
        return ["extreme"]

    # -------- 🔥 EMAIL PRIORITY (FIXED) --------
    if state.get("intent") == "email":
        if state.get("analysis_type") == "summary":
            logger.info("[ROUTE_FIX] email summary → summary tool")
            return ["summary"]

        logger.info("[ROUTE] email → standard_analysis")
        return ["standard_analysis"]

    if state.get("intent") in {"chat", "explain", "clarify"}:
        return []

    if _requires_business_advisor(state):
        logger.info("[ROUTE_FIX] advanced business request -> business_advisor tool")
        return ["business_advisor"]

    # if state.get("target_countries") and len(state.get("target_countries") or []) > 1:
    #     return ["multi_country"]

    metric_name = state.get("metric_name")

    if metric_name in INVENTORY_METRICS:
        if state.get("analysis_type") == "comparison" or state.get("answer_shape") == "comparison":
            logger.info("[ROUTE_FIX] inventory comparison -> standard_analysis")
            return ["standard_analysis"]
        state["analysis_type"] = "diagnosis"
        return ["standard_analysis"]

    # -------- 🔥 FIX: INVENTORY STATUS QUERIES --------
    user_query = (state.get("user_query") or "").lower()

    # -------- 🔥 SMART INVENTORY ROUTING (NO KEYWORDS) --------
    

    # -------- 🔥 FIX 1: INVENTORY DIAGNOSIS (GLOBAL PRIORITY) --------
    if (
        metric_name in INVENTORY_METRICS
        and state.get("analysis_type") in {"diagnosis", "analysis"}
    ):
        logger.info("[ROUTE_FIX] inventory diagnosis → standard_analysis")
        return ["standard_analysis"]

    # -------- 🔥 GROWTH PRIORITY --------
    if state.get("analysis_type") == "growth":
        logger.info("[ROUTE_FIX] growth → standard_analysis")
        return ["standard_analysis"]

    period_payload = state.get("period_payload") or {}
    period_parsed = state.get("period_parsed") or {}
    has_month_list = bool(
        state.get("target_months")
        or period_payload.get("type") == "multi_month"
        or period_parsed.get("type") == "multi_month"
    )
    if (
        state.get("analysis_type") == "absolute"
        and (state.get("answer_shape") == "multi_month" or has_month_list)
    ):
        logger.info("[ROUTE_FIX] absolute multi-month lookup → multi_month tool")
        return ["multi_month"]

    # -------- 🔥 METRIC-AWARE ROUTING --------
    if metric_name:
        try:
            metric_def = get_metric_def(metric_name)

            logger.info(
                f"[METRIC_BEHAVIOR] metric={metric_name}, "
                f"entity={metric_def.entity_level}, "
                f"product_breakdown={metric_def.supports_product_breakdown}, "
                f"time_breakdown={metric_def.supports_time_breakdown}"
            )

            if state.get("analysis_type") in {"breakdown", "trend", "comparison"}:
                return ["standard_analysis"]

        except Exception:
            logger.exception("[METRIC_BEHAVIOR_ERROR]")

    # -------- INTENT --------
    if state.get("intent") == "event_planner" or state.get("analysis_type") == "event_plan":
        return ["event_plan"]

    if state.get("intent") in {"chat", "explain", "clarify"}:
        return []

    # -------- 🔥 DECISION MODE --------
    if state.get("reasoning_mode") == "decision":
        logger.info("[ROUTE_FIX] decision mode → decision tool")
        return ["decision"]

    # -------- SKU LOGIC --------
    if state.get("dimension") == "sku":

        # multi product → standard
        if state.get("product_queries") and len(state.get("product_queries") or []) > 1:
            return ["standard_analysis"]

        # single product deep dive
        if state.get("product_query") and state.get("reasoning_mode") == "analysis":
            return ["sku_intelligence"]

        # ranking
        if state.get("answer_shape") == "ranking":
            return ["ranking"]

        # trend
        if state.get("analysis_type") == "trend":
            return ["sku_trend"]

        return ["standard_analysis"]

    # -------- FALLBACK --------
    if state.get("analysis_type") == "sku_intelligence":
        return ["sku_intelligence"]

    # -------- 🔥 FIX 3: SAFE SUMMARY --------
    if (
        state.get("analysis_type") == "summary"
        and state.get("metric_name") not in INVENTORY_METRICS
    ):
        return ["summary"]

    if state.get("analysis_type") == "sku_trend":
        return ["sku_trend"]

    if state.get("answer_shape") == "multi_month":
        return ["multi_month"]

    # -------- DEFAULT --------
    return ["standard_analysis"]


def _requires_business_advisor(state: AgentState) -> bool:
    if state.get("intent") in {"chat", "explain", "clarify", "email", "event_planner"}:
        return False

    if state.get("analysis_type") == "event_plan":
        return False

    if state.get("reasoning_mode") == "decision":
        return True

    if state.get("task_type") in {"recommendation", "diagnosis", "planning"}:
        return True

    query = (state.get("user_query") or "").lower()
    business_triggers = [
        "what should i do",
        "how should",
        "how can",
        "improve",
        "optimise",
        "optimize",
        "recommend",
        "suggest",
        "fix",
        "reduce",
        "increase",
        "why",
        "reason",
        "underperform",
        "problem",
        "issue",
        "risk",
        "opportunity",
        "next step",
        "next month",
    ]
    return any(trigger in query for trigger in business_triggers)


def _run_event_planner(state: AgentState) -> AgentState:
    if not EVENT_PLANNER_AVAILABLE:
        raise ValueError("Event planner helper is not available in this deployment.")
    now = datetime.utcnow()
    payload = {
        "user_id": state["user_id"],
        "country": state["country"],
        "last_event": {"month": int(state.get("last_event_month") or 11), "year": now.year - 1},
        "future_event": {"month": int(state.get("future_event_month") or state.get("last_event_month") or 11), "year": now.year},
        "target_sales": state.get("target_sales"),
    }
    result = build_plan_langgraph(payload, phormula_engine, amazon_engine)
    top_items = result[:3] if isinstance(result, list) else []
    summary: List[str] = []
    actions: List[str] = []
    for item in top_items:
        plan = item.get("plan", {}) if isinstance(item, dict) else {}
        if isinstance(plan, dict):
            summary.extend(plan.get("summary", [])[:4])
            actions.extend(plan.get("actions", [])[:4])
    state["event_plan_result"] = {
        "items": result if isinstance(result, list) else [result],
        "summary": summary[:10],
        "actions": actions[:10],
        "raw": result,
    }
    state["analysis_result"] = {"type": "event_plan"}
    state["current_metrics"] = {"metric": "event_plan", "period_label": f"{payload['future_event']['month']}-{payload['future_event']['year']}"}
    return state


def _generate_business_insights(state: AgentState) -> str:
    try:
        metrics = state.get("analysis_result", {}).get("metrics", {})
        top_products = state.get("analysis_result", {}).get("top_products", [])
        period = state.get("current_metrics", {}).get("period_label")

        # -------- CURRENCY HANDLING --------
        country = (state.get("country") or "").strip().lower()

        if country in {"uk", "gb", "gbr", "amazon uk", "united kingdom"}:
            currency_symbol = "\u00a3"
            currency_code = "GBP"
        else:
            currency_symbol = "$"
            currency_code = "USD"

        # -------- FORMAT METRICS (VERY IMPORTANT) --------
        def format_currency(val):
            try:
                return f"{currency_symbol}{float(val):,.2f}"
            except Exception:
                return val

        formatted_metrics = {}
        for k, v in metrics.items():
            if isinstance(v, (int, float)):
                formatted_metrics[k] = format_currency(v)
            else:
                formatted_metrics[k] = v

        # -------- FORMAT TOP PRODUCTS --------
        formatted_products = []
        for p in top_products[:5]:
            try:
                formatted_products.append({
                    "name": p.get("product_name") or p.get("sku") or "Unknown",
                    "sales": format_currency(p.get("__metric__", 0))
                })
            except Exception:
                continue

        # -------- PROMPT --------
        prompt = f"""
You are a senior ecommerce business analyst.

IMPORTANT:
- Currency is {currency_code} ({currency_symbol})
- ALWAYS use {currency_symbol} for all monetary values
- DO NOT use $ unless currency is USD

Metric definitions:
- advertising_total = ad spend (cost)
- platform_fee = Amazon fees (cost)
- rembursement_fee = reimbursements from Amazon (POSITIVE, not a cost)

Generate a business report for {period}.

Metrics:
{formatted_metrics}

Top Products:
{formatted_products}

Output format:
1. Executive summary (2–3 lines)
2. Key drivers
3. Risks


Keep it concise, business-focused, and actionable.
"""

        response = chat_llm.invoke(prompt)
        return response.content if response else ""

    except Exception:
        logger.exception("[BUSINESS_INSIGHTS_ERROR]")
        return ""


def _compute_summary(state: AgentState) -> AgentState:
    engine = state["engine"]
    metric_names = _summary_metric_names_for_state(state)
    row = _build_country_summary_row(engine, state, state["country"], metric_names)
    primary_metric = metric_names[0] if metric_names else "summary"

    state["current_metrics"] = {
        "metric": primary_metric,
        "period_label": row.get("period_label"),
        "metrics": row.get("metrics", {}),
        "total": (row.get("metrics") or {}).get(primary_metric),
    }

    state["analysis_result"] = {
        "type": "summary",
        "metrics": row.get("metrics", {}),
        "formatted_metrics": row.get("formatted_metrics", {}),
        "country": state.get("country"),
        "country_label": row.get("country_label"),
        "period_label": row.get("period_label"),
        "product": row.get("product"),
        "top_product": row.get("top_product"),
        "rows": [row],
    }

    return state


def _highest_month_for_metric(
    engine: Any,
    state: AgentState,
    metric_name: str,
    months: List[MonthKey],
) -> Optional[Dict[str, Any]]:
    month_scores: List[Dict[str, Any]] = []

    for month_key in months or []:
        try:
            monthly_result = get_metric_for_month(
                engine,
                state["user_id"],
                state["country"],
                metric_name,
                month_key.month,
                month_key.year,
            )
        except Exception:
            logger.debug("Ranking highest-month lookup failed", exc_info=True)
            continue

        value = monthly_result.get("total")
        if value is None:
            continue

        try:
            numeric_value = float(value)
        except Exception:
            continue

        month_scores.append(
            {
                "month": month_key.month,
                "year": month_key.year,
                "period_label": monthly_result.get("period_label") or month_key.label,
                "value": numeric_value,
                "metric": metric_name,
            }
        )

    if not month_scores:
        return None

    return max(month_scores, key=lambda row: row["value"])


def _compute_ranking(state: AgentState) -> AgentState:
    engine = state["engine"]
    metric_name = state.get("metric_name") or "profit"
    payload = state["period_payload"]
    direction = state.get("ranking_direction") or "top"
    period_months: List[MonthKey] = []

    # -------- 🔥 INVENTORY FIX (NEW BLOCK) --------
    if metric_name in INVENTORY_METRICS:
        logger.info("[RANKING_FIX] Using inventory snapshot")

        if payload.get("type") == "single":
            month = int(payload.get("month"))
            year = int(payload.get("year"))
        else:
            now = datetime.utcnow()
            month = now.month
            year = now.year

        result = get_inventory_snapshot(
            user_id=state["user_id"],
            metric_name=metric_name,
            month=month,
            year=year,
            country=state["country"],
        )

        ranked = rank_skus(
            result,
            direction=direction,
            limit=state.get("top_n") or 5
        )

        state["current_metrics"] = result
        state["analysis_result"] = {
            "type": "ranking",
            "metric": metric_name,
            "period_label": result.get("period_label"),
            "per_sku": ranked,
            "total": result.get("total", 0.0),
            "ranking_direction": direction,
        }

        return state

    # -------- EXISTING FINANCE LOGIC (UNCHANGED) --------
    ptype = payload.get("type")

    if ptype == "latest_month":
        latest = latest_available_month(
            engine,
            state["user_id"],
            state["country"]
        )
        period_months = [latest]
        result = get_metric_for_month(
            engine,
            state["user_id"],
            state["country"],
            metric_name,
            latest.month,
            latest.year
        )

    elif ptype == "single":
        result = get_metric_for_month(
            engine,
            state["user_id"],
            state["country"],
            metric_name,
            payload["month"],
            payload["year"]
        )
        period_months = [MonthKey(year=int(payload["year"]), month=int(payload["month"]))]

    elif ptype == "range":
        try:
            period_months = [
                month_key
                for month_key, _ in fetch_period_dfs(
                    engine,
                    state["user_id"],
                    state["country"],
                    payload["start_month"],
                    payload["start_year"],
                    payload["end_month"],
                    payload["end_year"],
                    skip_missing=True,
                )
            ]
        except Exception:
            logger.debug("Ranking period month list unavailable", exc_info=True)
            period_months = []
        result = get_metric_for_period(
            engine,
            state["user_id"],
            state["country"],
            metric_name,
            payload["start_month"],
            payload["start_year"],
            payload["end_month"],
            payload["end_year"],
            skip_missing=True
        )

    elif ptype == "last_n_months":
        months = _last_n_window(
            engine,
            state["user_id"],
            state["country"],
            payload["n"]
        )

        if months:
            period_months = months
            result = get_metric_for_period(
                engine,
                state["user_id"],
                state["country"],
                metric_name,
                months[0].month,
                months[0].year,
                months[-1].month,
                months[-1].year,
                skip_missing=True
            )
        else:
            latest = latest_available_month(engine, state["user_id"], state["country"])
            period_months = [latest]
            result = _latest_month_result(
                engine,
                state["user_id"],
                state["country"],
                metric_name
            )

    else:
        latest = latest_available_month(engine, state["user_id"], state["country"])
        period_months = [latest]
        result = _latest_month_result(
            engine,
            state["user_id"],
            state["country"],
            metric_name
        )

    ranked = rank_skus(
        result,
        direction=direction,
        limit=state.get("top_n") or 5
    )
    highest_month = _highest_month_for_metric(engine, state, metric_name, period_months)

    state["current_metrics"] = result
    state["analysis_result"] = {
        "type": "ranking",
        "metric": metric_name,
        "period_label": result.get("period_label"),
        "highest_month": highest_month,
        "per_sku": ranked,
        "total": result.get("total", 0.0),
        "ranking_direction": direction,
    }

    return state

def _compute_extreme(state: AgentState) -> AgentState:
    engine = state["engine"]
    metric_name = state.get("metric_name") or "net_sales"
    extreme_type = state.get("extreme_type") or "max"
    payload = state["period_payload"]

    # -------- 🔥 PRODUCT EXTREME FIX --------
    if state.get("dimension") == "sku":
        logger.info("[EXTREME_FIX] SKU-level extreme")

        if payload.get("type") == "single":
            result = get_metric_for_month(
                engine,
                state["user_id"],
                state["country"],
                metric_name,
                payload["month"],
                payload["year"],
            )

            rows = result.get("per_sku", [])

            if not rows:
                logger.warning("[EXTREME_FIX] No SKU rows found")
                state["analysis_result"] = {"type": "extreme"}
                state["current_metrics"] = {}
                return state

            best = max(rows, key=lambda r: float(r.get("__metric__", 0.0)))

            state["current_metrics"] = {
                "metric": metric_name,
                "extreme_type": extreme_type,
                "month": payload["month"],
                "year": payload["year"],
                "period_label": result.get("period_label"),
                "product": best.get("product_name"),
                "value": float(best.get("__metric__", 0.0)),
            }

            state["analysis_result"] = {
                "type": "extreme",
                **state["current_metrics"]
            }
            return state

    # -------- ORIGINAL TIME-BASED EXTREME (UNCHANGED) --------
    now = datetime.utcnow()

    dfs = fetch_period_dfs(
        engine,
        state["user_id"],
        state["country"],
        1,
        now.year - 5,
        now.month,
        now.year,
        skip_missing=True,
    )

    months = [mk for mk, _ in dfs]

    if payload.get("type") == "last_n_months":
        months = months[-int(payload.get("n", 1)) :]

    elif payload.get("type") == "range":
        months = [
            mk
            for mk in months
            if (mk.year, mk.month)
            >= (payload["start_year"], payload["start_month"])
            and (mk.year, mk.month)
            <= (payload["end_year"], payload["end_month"])
        ]

    product_match = None

    if state.get("product_query") and months:
        sample = get_metric_for_month(
            engine,
            state["user_id"],
            state["country"],
            metric_name,
            months[-1].month,
            months[-1].year,
        )

        product_match = _match_product(
            sample.get("per_sku", []),
            state.get("product_query"),
        )

        state["product_match"] = product_match

    result = find_extreme_month(
        engine,
        state["user_id"],
        state["country"],
        metric_name,
        months,
        extreme_type=extreme_type,
        product_match=product_match,
    )

    state["current_metrics"] = result or {}
    state["analysis_result"] = {"type": "extreme", **(result or {})}

    return state


def _compute_multi_month(state: AgentState) -> AgentState:
    engine = state["engine"]
    metric_names = state.get("metric_names") or [state.get("metric_name") or "net_sales"]
    product_queries = state.get("product_queries")
    if not product_queries and state.get("product_query"):
        product_queries = [state.get("product_query")]
        state["product_queries"] = product_queries

    period_payload = state.get("period_payload") or {}
    month_year_pairs = (
        state.get("target_months")
        or state.get("period_parsed", {}).get("months")
        or period_payload.get("months")
        or []
    )

    if not month_year_pairs and period_payload.get("type") == "range":
        dfs = fetch_period_dfs(
            engine,
            state["user_id"],
            state["country"],
            period_payload["start_month"],
            period_payload["start_year"],
            period_payload["end_month"],
            period_payload["end_year"],
            skip_missing=True,
        )
        month_year_pairs = [{"month": mk.month, "year": mk.year} for mk, _ in dfs]

    if not month_year_pairs and period_payload.get("type") == "single":
        month_year_pairs = [{"month": period_payload["month"], "year": period_payload["year"]}]

    logger.info(
        f"[MULTI_MONTH_INPUT] metrics={metric_names}, months={month_year_pairs}, "
        f"product_queries={product_queries}"
    )

    all_results = []
    for metric in metric_names:
        all_results.append(
            get_metric_for_multiple_months(
                engine,
                state["user_id"],
                state["country"],
                metric,
                month_year_pairs,
                product_queries=product_queries,
            )
        )
    result = {"metrics": metric_names, "results": all_results}
    state["current_metrics"] = result
    state["analysis_result"] = {"type": "multi_month", **result}
    return state


def _compute_multi_dimensional(state: AgentState) -> AgentState:
    result = get_multi_dimensional_data(
        engine=state["engine"],
        user_id=state["user_id"],
        country=state["country"],
        metric_names=state.get("metric_names") or [state.get("metric_name") or "net_sales"],
        months=state.get("target_months") or [],
        product_queries=state.get("product_queries"),
    )
    state["current_metrics"] = result
    state["analysis_result"] = {"type": "multi_dimensional", **result}
    return state


def _compute_decision(state: AgentState) -> AgentState:
    engine = state["engine"]
    latest = latest_available_month(engine, state["user_id"], state["country"])
    metric_name = state.get("metric_name")
    metric_names = state.get("metric_names") or ([metric_name] if metric_name else ["net_sales", "profit", "asp"])
    product_query = state.get("product_query")
    product_match = None
    current_pack: Dict[str, Any]
    history: Dict[str, Any] = {}
    growth_driver = None
    if product_query:
        sample = get_metric_for_month(engine, state["user_id"], state["country"], "net_sales", latest.month, latest.year)
        product_match = _match_product(sample.get("per_sku", []), product_query)
        state["product_match"] = product_match
        if product_match:
            current_pack = get_product_metric_pack_for_month(engine, state["user_id"], state["country"], product_match, latest.month, latest.year)
            trend_months = _last_n_window(engine, state["user_id"], state["country"], 6)
            for metric in metric_names:
                try:
                    history[metric] = build_time_series_analysis(engine, state["user_id"], state["country"], metric, trend_months, product_match=product_match, time_unit=state.get("period_parsed", {}).get("unit"))
                except Exception:
                    logger.debug("Decision history unavailable for %s", metric, exc_info=True)
        else:
            current_pack = {"period_label": latest.label, "metrics": {}}
    else:
        current_pack = get_metric_pack_for_month(engine, state["user_id"], state["country"], metric_names, latest.month, latest.year)
        trend_months = _last_n_window(engine, state["user_id"], state["country"], 6)
        for metric in metric_names:
            try:
                history[metric] = build_time_series_analysis(engine, state["user_id"], state["country"], metric, trend_months, product_match=None, time_unit=state.get("period_parsed", {}).get("unit"))
            except Exception:
                logger.debug("Decision history unavailable for %s", metric, exc_info=True)
    if metric_name:
        try:
            growth_driver = get_growth_driver_insights(engine, state["user_id"], state["country"], "net_sales" if metric_name in {"sales_mix", "profit_mix"} else metric_name, latest.month, latest.year)
        except Exception:
            logger.debug("Growth driver unavailable", exc_info=True)
    state["current_metrics"] = {"metric": metric_name or "decision", "period_label": latest.label, "total": None}
    state["analysis_result"] = {
        "type": "decision",
        "question": state.get("user_query"),
        "reasoning_mode": state.get("reasoning_mode"),
        "task_type": state.get("task_type"),
        "metric_name": metric_name,
        "metric_names": metric_names,
        "product_query": product_query,
        "product_match": product_match,
        "focus_scope": "product" if product_match else "overall",
        "payload": state.get("period_payload"),
        "current_pack": current_pack,
        "history": history,
        "growth_driver": growth_driver,
    }
    return state


def _compute_business_advisor(state: AgentState) -> AgentState:
    context = build_business_context(
        engine=state["engine"],
        user_id=state["user_id"],
        country=state["country"],
        period_payload=state.get("period_payload"),
        user_query=state.get("user_query", ""),
        metric_name=state.get("metric_name"),
        metric_names=state.get("metric_names"),
        product_query=state.get("product_query"),
    )

    metric_name = state.get("metric_name") or "business"
    totals = context.get("totals", {})
    state["business_context"] = context
    state["current_metrics"] = {
        "metric": metric_name,
        "period_label": context.get("period", {}).get("label"),
        "total": totals.get(metric_name),
    }
    state["analysis_result"] = {
        "type": "business_advisor",
        "question": state.get("user_query"),
        "reasoning_mode": state.get("reasoning_mode"),
        "task_type": state.get("task_type"),
        "metric_name": state.get("metric_name"),
        "metric_names": state.get("metric_names"),
        "product_query": state.get("product_query"),
        "context": context,
    }
    return state


def _compute_sku_intelligence(state: AgentState) -> AgentState:
    engine = state["engine"]
    metric_name = state.get("metric_name") or "profit"
    payload = state["period_payload"]
    latest = latest_available_month(engine, state["user_id"], state["country"])
    sample = get_metric_for_month(engine, state["user_id"], state["country"], "net_sales", latest.month, latest.year)
    product_match = _match_product(sample.get("per_sku", []), state.get("product_query"))
    if not product_match:
        raise ValueError("I could not confidently match that product name in your data.")
    state["product_match"] = product_match

    def pack_from_payload(pl: Dict[str, Any]) -> Dict[str, float]:
        ptype = pl.get("type")
        if ptype == "single":
            base_pack = get_product_metric_pack_for_month(engine, state["user_id"], state["country"], product_match, pl["month"], pl["year"])["metrics"]
            return _enrich_pack_with_mix(engine, state["user_id"], state["country"], pl, base_pack)
        if ptype == "last_n_months":
            months = _last_n_window(engine, state["user_id"], state["country"], pl["n"])
            acc = {"total_quantity": 0.0, "net_sales": 0.0, "profit": 0.0}
            for mk in months:
                metrics = get_product_metric_pack_for_month(engine, state["user_id"], state["country"], product_match, mk.month, mk.year)["metrics"]
                for key in ["total_quantity", "net_sales", "profit"]:
                    acc[key] += float(metrics.get(key, 0.0))
            acc["asp"] = _safe_div(acc["net_sales"], acc["total_quantity"])
            return _enrich_pack_with_mix(engine, state["user_id"], state["country"], pl, acc)
        if ptype == "range":
            months = get_last_n_month_keys(engine, state["user_id"], state["country"], 36)
            filtered = [mk for mk in months if (mk.year, mk.month) >= (pl["start_year"], pl["start_month"]) and (mk.year, mk.month) <= (pl["end_year"], pl["end_month"])]
            acc = {"total_quantity": 0.0, "net_sales": 0.0, "profit": 0.0}
            for mk in filtered:
                try:
                    metrics = get_product_metric_pack_for_month(engine, state["user_id"], state["country"], product_match, mk.month, mk.year)["metrics"]
                except Exception:
                    continue
                for key in ["total_quantity", "net_sales", "profit"]:
                    acc[key] += float(metrics.get(key, 0.0))
            acc["asp"] = _safe_div(acc["net_sales"], acc["total_quantity"])
            return _enrich_pack_with_mix(engine, state["user_id"], state["country"], pl, acc)
        base_pack = get_product_metric_pack_for_month(engine, state["user_id"], state["country"], product_match, latest.month, latest.year)["metrics"]
        effective = {"type": "single", "month": latest.month, "year": latest.year}
        return _enrich_pack_with_mix(engine, state["user_id"], state["country"], effective, base_pack)

    effective_payload = payload if payload.get("type") != "growth_base" else _prepare_period_payload(payload["base"], "absolute")
    current_pack = pack_from_payload(effective_payload)
    previous_payload = None
    base_for_prev = payload["base"] if payload.get("type") == "growth_base" else effective_payload
    if base_for_prev.get("type") in {"single", "range"}:
        previous_payload = _previous_period_for_base(base_for_prev)
    elif base_for_prev.get("type") in {"last_n", "last_n_months"}:
        n = int(base_for_prev["n"])
        months = _last_n_window(engine, state["user_id"], state["country"], n * 2)
        if len(months) >= n * 2:
            previous_payload = {"type": "range", "start_month": months[0].month, "start_year": months[0].year, "end_month": months[n - 1].month, "end_year": months[n - 1].year}
    previous_pack: Dict[str, float] = {}
    if previous_payload:
        try:
            previous_pack = pack_from_payload(previous_payload)
        except Exception:
            logger.debug("Previous comparable period unavailable", exc_info=True)
    deltas = {key: float(current_pack.get(key, 0.0)) - float(previous_pack.get(key, 0.0) if previous_pack else 0.0) for key in ["net_sales", "profit", "total_quantity", "asp", "sales_mix", "profit_mix"]}
    root_cause = _compute_root_cause(current_pack, previous_pack)
    summary_points = [
        f"Current sales are {float(current_pack.get('net_sales', 0.0)):,.2f}.",
        f"Current profit is {float(current_pack.get('profit', 0.0)):,.2f}.",
        f"Current units are {float(current_pack.get('total_quantity', 0.0)):,.2f} and ASP is {float(current_pack.get('asp', 0.0)):,.2f}.",
        f"Current sales mix is {float(current_pack.get('sales_mix', 0.0)):.2%} and profit mix is {float(current_pack.get('profit_mix', 0.0)):.2%}.",
    ]
    if previous_pack:
        summary_points.append(f"Sales changed by {deltas['net_sales']:,.2f}, profit changed by {deltas['profit']:,.2f}, and units changed by {deltas['total_quantity']:,.2f} vs the previous comparable period.")
        summary_points.extend(root_cause.get("summary", [])[:3])
    trend_n = payload.get("n", 6) if payload.get("type") == "last_n_months" else 6
    trend_months = _last_n_window(engine, state["user_id"], state["country"], trend_n)
    trend_metric = metric_name if metric_name not in {"sales_mix", "profit_mix"} else ("net_sales" if metric_name == "sales_mix" else "profit")
    trend = build_time_series_analysis(engine, state["user_id"], state["country"], trend_metric, trend_months, product_match=product_match)
    result = {"product_match": product_match, "current": current_pack, "previous": previous_pack, "deltas": deltas, "summary_points": summary_points, "trend": trend, "root_cause": root_cause}
    state["sku_intelligence_result"] = result
    state["current_metrics"] = {"metric": metric_name, "period_label": payload.get("type", "selected period"), "total": float(current_pack.get(metric_name, current_pack.get("profit", 0.0)))}
    state["analysis_result"] = {"type": "sku_intelligence", **result}
    if previous_pack:
        current_value = float(current_pack.get(metric_name, 0.0))
        previous_value = float(previous_pack.get(metric_name, 0.0))
        delta = current_value - previous_value
        pct = None if previous_value == 0 else (delta / previous_value) * 100.0
        state["comparison"] = {"left": {"label": "current", "total": current_value}, "right": {"label": "previous", "total": previous_value}, "delta": delta, "pct_change": pct}
    return state


def _compute_sku_trend(state: AgentState) -> AgentState:
    engine = state["engine"]
    metric_name = state.get("metric_name") or "net_sales"
    payload = state.get("period_payload") or {"type": "latest_month"}
    months = _last_n_window(engine, state["user_id"], state["country"], payload.get("n", 12) if payload.get("type") == "last_n_months" else 12)
    if not months:
        state["analysis_result"] = {"type": "sku_trend", "results": []}
        return state
    latest = months[-1]
    base = get_metric_for_month(engine, state["user_id"], state["country"], metric_name, latest.month, latest.year)
    results = []
    for row in base.get("per_sku", []):
        name = str(row.get("product_name", "")).lower().strip()
        if not name:
            continue
        trend = build_time_series_analysis(engine, state["user_id"], state["country"], metric_name, months, product_match=name, time_unit=state.get("period_parsed", {}).get("unit"))
        if trend.get("movement") == "consistently_down":
            results.append({"product": row.get("product_name"), "trend": trend})
    state["analysis_result"] = {"type": "sku_trend", "results": results, "months_used": [m.label for m in months]}
    state["current_metrics"] = {"metric": metric_name, "period_label": f"{months[0].label} to {months[-1].label}", "total": None}
    return state


def _period_end_month_key(period: Dict[str, Any]) -> MonthKey:
    return MonthKey(
        year=int(period.get("end_year") or period.get("year")),
        month=int(period.get("end_month") or period.get("month")),
    )


def _inventory_snapshot_value(
    snapshot: Dict[str, Any],
    product_query: Optional[str],
) -> Tuple[Optional[float], Optional[str], Optional[str]]:
    rows = snapshot.get("per_sku") or []
    if not product_query:
        total = snapshot.get("total")
        return (float(total) if total is not None else None), None, None

    pq = str(product_query).strip().lower()
    exact_matches = [
        row for row in rows
        if pq == str(row.get("product_name") or "").strip().lower()
        or pq == str(row.get("sku") or "").strip().lower()
    ]
    contains_matches = [
        row for row in rows
        if pq in str(row.get("product_name") or "").strip().lower()
        or pq in str(row.get("sku") or "").strip().lower()
    ]
    matches = exact_matches or contains_matches
    if not matches:
        return None, None, None

    value = sum(float(row.get("__metric__", 0.0) or 0.0) for row in matches)
    first = matches[0]
    return value, first.get("product_name") or product_query, first.get("sku")


def _format_product_with_sku(product_name: Optional[str], sku: Optional[str], fallback: Optional[str]) -> str:
    product_display = product_name or fallback or "selected product"
    if sku and str(product_display).strip().lower() != str(sku).strip().lower():
        return f"{product_display} ({sku})"
    return str(product_display)


def _compute_inventory_comparison(state: AgentState, left_period: Dict[str, Any], right_period: Dict[str, Any]) -> AgentState:
    metric_name = state.get("metric_name") or "available"
    product_query = state.get("product_query")
    left_month = _period_end_month_key(left_period)
    right_month = _period_end_month_key(right_period)

    left_snapshot = get_inventory_snapshot(
        user_id=state["user_id"],
        metric_name=metric_name,
        month=left_month.month,
        year=left_month.year,
        country=state["country"],
    )
    right_snapshot = get_inventory_snapshot(
        user_id=state["user_id"],
        metric_name=metric_name,
        month=right_month.month,
        year=right_month.year,
        country=state["country"],
    )

    left_value, left_product, left_sku = _inventory_snapshot_value(left_snapshot, product_query)
    right_value, right_product, right_sku = _inventory_snapshot_value(right_snapshot, product_query)
    product_display = _format_product_with_sku(
        left_product or right_product,
        left_sku or right_sku,
        product_query,
    )

    delta = None
    pct_change = None
    if left_value is not None and right_value is not None:
        delta = left_value - right_value
        pct_change = None if right_value == 0 else (delta / right_value) * 100.0

    comparison = {
        "left": {
            "label": left_snapshot.get("period_label") or left_month.label,
            "total": left_value,
        },
        "right": {
            "label": right_snapshot.get("period_label") or right_month.label,
            "total": right_value,
        },
        "delta": delta,
        "pct_change": pct_change,
    }

    state["comparison"] = comparison
    state["current_metrics"] = {
        "metric": metric_name,
        "period_label": f"{comparison['left']['label']} vs {comparison['right']['label']}",
        "total": left_value,
    }
    state["analysis_result"] = {
        "type": "inventory_comparison",
        "metric": metric_name,
        "country": state.get("country"),
        "product_query": product_query,
        "product_display": product_display,
        "left": comparison["left"],
        "right": comparison["right"],
        "delta": delta,
        "pct_change": pct_change,
        "missing": {
            "left": left_value is None,
            "right": right_value is None,
        },
    }
    return state


def _compute_standard_analysis(state: AgentState) -> AgentState:
    try:
        engine = state["engine"]
        metric_name = state.get("metric_name") or "profit"
        payload = state["period_payload"]
        analysis_type = state.get("analysis_type") or "absolute"
        metric_names = state.get("metric_names")
        product_queries = state.get("product_queries")
        months = state.get("target_months") or state.get("period_parsed", {}).get("months")

        # -------- 🔥 FIX: NORMALIZE PRODUCT FILTER --------
        if not product_queries and state.get("product_query"):
            product_queries = [state.get("product_query")]

        state["product_queries"] = product_queries

        logger.info(
            f"[STANDARD_ANALYSIS_START] metric={metric_name}, "
            f"analysis_type={analysis_type}, payload={payload}, "
            f"metric_names={metric_names}, product_queries={product_queries}"
        )

        # -------- MULTI-DIMENSION PREP --------
        if (product_queries and len(product_queries) > 1) and not months:
            if payload.get("type") == "single":
                months = [{"month": payload["month"], "year": payload["year"]}]
            elif payload.get("type") == "last_n_months":
                months = [
                    {"month": mk.month, "year": mk.year}
                    for mk in _last_n_window(engine, state["user_id"], state["country"], payload["n"])
                ]
            elif payload.get("type") == "range":
                dfs = fetch_period_dfs(
                    engine,
                    state["user_id"],
                    state["country"],
                    payload["start_month"],
                    payload["start_year"],
                    payload["end_month"],
                    payload["end_year"],
                    skip_missing=True,
                )
                months = [{"month": mk.month, "year": mk.year} for mk, _ in dfs]

            if months:
                state["target_months"] = months
                logger.info(f"[MULTI_DIMENSION_MONTHS] {months}")

        if ((metric_names and len(metric_names) > 1) or (product_queries and len(product_queries) > 1)) and months:
            logger.info("[ROUTE] Multi-dimensional analysis")
            return _compute_multi_dimensional(state)

        if analysis_type == "absolute" and payload.get("type") == "multi_month":
            logger.info("[ROUTE] Absolute multi-month analysis")
            return _compute_multi_month(state)

        # -------- COMPARISON --------
        if payload.get("type") == "comparison":
            logger.info("[ROUTE] Comparison analysis")

            p1, p2 = payload["p1"], payload["p2"]

            if metric_name in INVENTORY_METRICS:
                logger.info("[ROUTE] Inventory comparison analysis")
                return _compute_inventory_comparison(state, p1, p2)

            comp = compare_periods(
                engine=engine,
                user_id=state["user_id"],
                country=state["country"],
                metric_name=metric_name,
                left_start_month=p1["start_month"],
                left_start_year=p1["start_year"],
                left_end_month=p1["end_month"],
                left_end_year=p1["end_year"],
                right_start_month=p2["start_month"],
                right_start_year=p2["start_year"],
                right_end_month=p2["end_month"],
                right_end_year=p2["end_year"],
                skip_missing=True,
            )

            logger.info(f"[COMPARISON_RESULT] {comp}")

            state["comparison"] = comp
            state["current_metrics"] = {
                "metric": metric_name,
                "period_label": f"{comp['left']['label']} vs {comp['right']['label']}",
                "total": comp["left"]["total"],
            }
            state["analysis_result"] = {"type": "comparison"}
            return state

        # -------- GROWTH ONLY --------
        if analysis_type == "growth":
            base = payload.get("base", payload)

            if base.get("type") in {"last_n", "last_n_months"}:
                n = base.get("n", 6)
                n = min(n, 12)

                months_full = _last_n_window(
                    engine,
                    state["user_id"],
                    state["country"],
                    n,
                    extra_for_mom=True,
                )

            elif base.get("type") in {"range", "multi_month"}:
                logger.info("[GROWTH] Using parsed range")

                if "months" in base:
                    months_full = [
                        MonthKey(year=m["year"], month=m["month"])
                        for m in base.get("months", [])
                    ]
                else:
                    months_full = []
                    cur_month = base["start_month"]
                    cur_year = base["start_year"]

                    while (
                        cur_year < base["end_year"]
                        or (cur_year == base["end_year"] and cur_month <= base["end_month"])
                    ):
                        months_full.append(MonthKey(year=cur_year, month=cur_month))

                        cur_month += 1
                        if cur_month > 12:
                            cur_month = 1
                            cur_year += 1

            elif base.get("type") == "single":
                logger.info("[GROWTH] Single month → expanding for growth")

                months_full = _last_n_window(
                    engine,
                    state["user_id"],
                    state["country"],
                    2,
                    extra_for_mom=True,
                )

            else:
                logger.info("[GROWTH] Fallback window")

                months_full = _last_n_window(
                    engine,
                    state["user_id"],
                    state["country"],
                    6,
                    extra_for_mom=True,
                )

            if len(months_full) < 2:
                logger.warning("[GROWTH] Not enough months")
                state["analysis_result"] = {"type": "growth", "series": []}
                return state

            logger.info(f"[GROWTH_MONTHS] {[m.label for m in months_full]}")

            series_result = build_time_series_analysis(
                engine,
                state["user_id"],
                state["country"],
                metric_name,
                months_full,
                time_unit=state.get("period_parsed", {}).get("unit")
            )

            # -------- PRODUCT-AWARE GROWTH --------
            product_query = state.get("product_query")

            if product_query:
                logger.info(f"[GROWTH_PRODUCT] Filtering for product: {product_query}")

                series = []
                for mk in months_full:
                    result_month = get_unified_metric(
                        engine,
                        state,
                        metric_name,
                        mk.month,
                        mk.year,
                    )

                    rows = result_month.get("per_sku", [])
                    value = 0.0

                    for r in rows:
                        name = str(r.get("product_name", "")).strip().lower()
                        pq = str(product_query).strip().lower()

                        if name == pq:
                            value = float(r.get("__metric__", 0.0))
                            break

                    series.append({
                        "period_label": mk.label,
                        "__metric__": value,
                        "month": mk.month,
                        "year": mk.year,
                    })

                series_result["series"] = series

                # -------- RECOMPUTE MOM FOR PRODUCT SERIES --------
                mom = []
                for i in range(1, len(series)):
                    curr = series[i]["__metric__"]
                    prev = series[i - 1]["__metric__"]

                    pct = ((curr - prev) / prev * 100) if prev else 0.0

                    mom.append({
                        "period_label": series[i]["period_label"],
                        "current": curr,
                        "pct_change": pct,
                    })

                series_result["mom"] = mom

            state["analysis_result"] = {
                "type": "growth",
                **series_result,
            }

            state["current_metrics"] = {
                "metric": metric_name,
                "period_label": f"{months_full[0].label} to {months_full[-1].label}",
                "total": sum(float(x.get("__metric__", 0.0)) for x in series_result.get("series", [])),
            }

            return state

        # -------- TREND --------
        if analysis_type == "trend":
            logger.info("[ROUTE] Trend analysis")

            if payload.get("type") == "last_n_months":
                months_full = _last_n_window(engine, state["user_id"], state["country"], payload.get("n", 6))
            else:
                months_full = _last_n_window(engine, state["user_id"], state["country"], 6)

            if not months_full:
                logger.warning("[TREND] No months found")
                state["analysis_result"] = {"type": "trend", "series": []}
                return state

            logger.info(f"[TREND_MONTHS] {[m.label for m in months_full]}")

            # 🔥 ADD THIS BLOCK
            product_query = state.get("product_query")
            product_match = None

            if product_query:
                latest = months_full[-1]

                sample = get_unified_metric(
                    engine,
                    state,
                    metric_name,
                    latest.month,
                    latest.year,
                )

                product_match = _match_product(
                    sample.get("per_sku", []),
                    product_query
                )

                state["product_match"] = product_match

            # 🔥 MODIFY THIS CALL
            series_result = build_time_series_analysis(
                engine,
                state["user_id"],
                state["country"],
                metric_name,
                months_full,
                product_match=product_match,
                time_unit=state.get("period_parsed", {}).get("unit")
            )

            logger.info(f"[TREND_RESULT] {series_result}")

            state["analysis_result"] = {"type": "trend", **series_result}
            state["current_metrics"] = {
                "metric": metric_name,
                "period_label": f"{months_full[0].label} to {months_full[-1].label}",
                "total": sum(float(x.get("__metric__", 0.0)) for x in series_result.get("series", [])),
            }

            return state

        # -------- BREAKDOWN --------
        if analysis_type == "breakdown":
            logger.info("[ROUTE] Breakdown analysis")

            metric_def = get_metric_def(metric_name)

            if payload.get("type") == "last_n_months":
                months = _last_n_window(engine, state["user_id"], state["country"], payload.get("n", 6))

            elif payload.get("type") == "single":
                months = [
                    SimpleNamespace(
                        month=payload["month"],
                        year=payload["year"],
                        label=f"{payload['month']}/{payload['year']}",
                    )
                ]

            elif payload.get("type") == "range":
                dfs = fetch_period_dfs(
                    engine,
                    state["user_id"],
                    state["country"],
                    payload["start_month"],
                    payload["start_year"],
                    payload["end_month"],
                    payload["end_year"],
                    skip_missing=True,
                )
                months = [mk for mk, _ in dfs]

            else:
                months = _last_n_window(engine, state["user_id"], state["country"], 6)

            period_label = f"{months[0].label} to {months[-1].label}" if months else "No data"

            if metric_def.supports_product_breakdown:
                logger.info("[BREAKDOWN_MODE] SKU breakdown")

                rows = []
                for mk in months:
                    result_month = get_unified_metric(
                        engine,
                        state,
                        metric_name,
                        mk.month,
                        mk.year,
                    )
                    rows.extend(result_month.get("per_sku", []))

                total = sum(float(r.get("__metric__", 0.0)) for r in rows)

                state["current_metrics"] = {
                    "metric": metric_name,
                    "period_label": period_label,
                    "total": total,
                }

                state["analysis_result"] = {
                    "type": "breakdown",
                    "per_sku": rows,
                    "total": total,
                }

                return state

            elif metric_def.supports_time_breakdown:
                logger.info("[BREAKDOWN_MODE] TOTAL → time breakdown")

                series = []
                for mk in months:
                    result_month = get_unified_metric(
                        engine,
                        state,
                        metric_name,
                        mk.month,
                        mk.year,
                    )
                    series.append({
                        "period_label": mk.label,
                        "__metric__": result_month.get("total", 0.0),
                    })

                state["analysis_result"] = {
                    "type": "trend",
                    "series": series,
                }

                state["current_metrics"] = {
                    "metric": metric_name,
                    "period_label": period_label,
                    "total": sum(float(x["__metric__"]) for x in series),
                }

                return state

            else:
                logger.warning("[BREAKDOWN_INVALID] metric does not support any breakdown")

                state["analysis_result"] = {
                    "type": "invalid",
                    "message": f"{metric_name} does not support breakdown analysis",
                }

                return state

        # -------- ABSOLUTE MULTI-METRIC + PRODUCT --------
        if analysis_type == "absolute" and metric_names and len(metric_names) > 1 and product_queries and len(product_queries) == 1:
            logger.info("[ROUTE] Absolute single-product multi-metric analysis")

            month = None
            year = None

            if payload.get("type") == "single":
                month = payload["month"]
                year = payload["year"]
            else:
                latest = latest_available_month(engine, state["user_id"], state["country"])
                month = latest.month
                year = latest.year

            product_query = product_queries[0]
            rows_out = []

            for metric in metric_names:
                result_month = get_unified_metric(
                    engine,
                    state,
                    metric,
                    month,
                    year,
                )

                matched_value = 0.0
                matched_name = product_query

                for row in result_month.get("per_sku", []):
                    name = str(row.get("product_name", "")).strip().lower()
                    pq = str(product_query).strip().lower()

                    if name == pq:
                        matched_value = float(row.get("__metric__", 0.0))
                        matched_name = row.get("product_name") or product_query
                        break

                rows_out.append({
                    "month": datetime(year, month, 1).strftime("%b %Y"),
                    "product": matched_name,
                    "metric": metric,
                    "value": matched_value,
                })

            result = {
                "data": rows_out,
                "metrics": metric_names,
                "products": [product_queries[0]],
                "months": [{"month": month, "year": year}],
            }

            state["current_metrics"] = result
            state["analysis_result"] = {"type": "multi_dimensional", **result}
            return state

        # -------- DEFAULT --------
        logger.info("[ROUTE] Standard absolute analysis")

        payload = state.get("period_payload", {})

# -------- 🔥 FIX: HANDLE RANGE PROPERLY --------
        if payload.get("type") == "range":
            logger.info("[ABSOLUTE_RANGE_FIX] Aggregating full range")

            total_value = 0.0
            all_rows = []

            dfs = fetch_period_dfs(
                engine,
                state["user_id"],
                state["country"],
                payload["start_month"],
                payload["start_year"],
                payload["end_month"],
                payload["end_year"],
                skip_missing=True,
            )

            for mk, _ in dfs:
                result_month = get_unified_metric(
                    engine,
                    state,
                    metric_name,
                    mk.month,
                    mk.year,
                )

                total_value += float(result_month.get("total", 0.0))
                all_rows.extend(result_month.get("per_sku", []))

            result = {
                "metric": metric_name,
                "period_label": f"{payload['start_year']}",
                "total": total_value,
                "per_sku": all_rows,
            }

        # -------- SINGLE MONTH --------
        elif payload.get("type") == "single":
            result = get_unified_metric(
                engine,
                state,
                metric_name,
                payload["month"],
                payload["year"],
            )

        # -------- DEFAULT --------
        else:
            result = _latest_month_result(
                engine,
                state["user_id"],
                state["country"],
                metric_name,
            )

        # -------- 🔥 PASTE HERE --------
        product_query = state.get("product_query")

        if product_query:
            logger.info(f"[ABSOLUTE_PRODUCT_FILTER] {product_query}")

            rows = result.get("per_sku", [])
            value = 0.0

            pq = product_query.strip().lower()

            for r in rows:
                name = str(r.get("product_name", "")).strip().lower()

                if name == pq:
                    value = float(r.get("__metric__", 0.0))
                    break

            # fallback contains match
            if value == 0.0:
                for r in rows:
                    name = str(r.get("product_name", "")).lower()
                    if pq in name:
                        value += float(r.get("__metric__", 0.0))

            result["total"] = value
        # -------- 🔥 END --------

        logger.info(
            f"[STANDARD_RESULT] total={result.get('total')}, "
            f"rows={len(result.get('per_sku', []))}"
        )

        state["current_metrics"] = result

        # -------- 🔥 INVENTORY DIAGNOSIS FIX --------
        if (
            state.get("analysis_type") == "diagnosis"
            and metric_name in INVENTORY_METRICS
        ):
            logger.info("[INVENTORY_DIAGNOSIS] Running inventory checks")

            rows = result.get("per_sku", [])

            top_n = state.get("top_n")
            product_query = state.get("product_query")

            # -------- 🔥 STEP 1: PRODUCT FILTER --------
            if product_query:
                pq = str(product_query).strip().lower()

                rows = [
                    r for r in rows
                    if pq in str(r.get("product_name") or "").strip().lower()
                    or pq == str(r.get("sku") or "").strip().lower()
                ]

            # -------- 🔥 STEP 2: TOP N USING NET SALES --------
            elif top_n:
                try:
                    logger.info("[TOP_N] Using net sales ranking")

                    latest = latest_available_month(
                        state["engine"],
                        state["user_id"],
                        state["country"]
                    )

                    sales_result = get_metric_for_month(
                        state["engine"],
                        state["user_id"],
                        state["country"],
                        "net_sales",   # 🔥 IMPORTANT
                        latest.month,
                        latest.year
                    )

                    sales_rows = sales_result.get("per_sku", [])

                    # sort by net sales
                    sales_rows = sorted(
                        sales_rows,
                        key=lambda r: float(r.get("__metric__", 0)),
                        reverse=True
                    )

                    top_skus = set(
                        r.get("sku") for r in sales_rows[:top_n]
                    )

                    # filter inventory rows using top SKUs
                    rows = [
                        r for r in rows
                        if r.get("sku") in top_skus
                    ]

                except Exception:
                    logger.exception("[TOP_N_FALLBACK] fallback to simple slicing")
                    rows = rows[:top_n]

            # -------- 🔥 STEP 3: ELSE → KEEP ALL --------

            # -------- 🔥 FIRST: GET FLAGS --------
            flags = generate_sku_inventory_flags(
                user_id=state["user_id"],
                country=state["country"],
            )

            # -------- 🔥 BUILD INSIGHTS FROM FLAGS --------
            high_alert = 0
            warning = 0
            overaged = 0

            for r in rows:
                sku = r.get("sku")
                flag = flags.get(sku, {})
                alert = flag.get("inventory_alert")

                if alert == "High alert":
                    high_alert += 1
                elif alert == "Please send shipment":
                    warning += 1
                elif alert == "Long-term aged inventory":
                    overaged += 1

            insights = []

            if high_alert:
                insights.append(f"{high_alert} products are at critical stock-out risk")

            if warning:
                insights.append(f"{warning} products need replenishment soon")

            if overaged:
                insights.append(f"{overaged} products have ageing inventory")

            if not insights:
                insights.append("Inventory looks healthy")

            # -------- 🔥 ADD FLAGS --------
            flags = generate_sku_inventory_flags(
                user_id=state["user_id"],
                country=state["country"],
            )

            # -------- 🔥 BUILD SKU MAP (IMPORTANT) --------
            sku_map = {}
            for r in rows:
                name = r.get("product_name")
                sku = r.get("sku") or name  # fallback if sku missing
                if name:
                    sku_map[name] = sku

            state["analysis_result"] = {
                "type": "inventory_diagnosis",
                "metric": metric_name,
                "period_label": result.get("period_label"),
                "source_table": result.get("source_table"),
                "snapshot_date": result.get("snapshot_date"),
                "country": result.get("country") or state.get("country"),
                "total": result.get("total"),
                "insights": insights,
                "rows": rows,
                "flags": flags,      # ✅ NEW
                "sku_map": sku_map,  # ✅ NEW (CRITICAL)
            }

        else:
            state["analysis_result"] = {"type": "absolute"}

        return state
    except Exception as e:
        logger.exception("[STANDARD_ANALYSIS_ERROR]")
        state["error"] = str(e)
        return state

def get_unified_metric(engine, state, metric_name, month=None, year=None):

    # -------- SAFE PERIOD --------
    if month is None or year is None:
        payload = state.get("period_payload", {})
        month = payload.get("month")
        year = payload.get("year")

    # -------- VALIDATION --------
    if metric_name not in INVENTORY_METRICS and metric_name not in FINANCE_METRICS:
        raise ValueError(f"Unsupported metric: {metric_name}")

    # -------- INVENTORY --------
    if metric_name in INVENTORY_METRICS:
        return get_inventory_snapshot(
            user_id=state["user_id"],
            metric_name=metric_name,
            month=month,
            year=year,
            country=state["country"],
        )

    # -------- FINANCE --------
    return get_metric_for_month(
        engine=engine,
        user_id=state["user_id"],
        country=state["country"],
        metric_name=metric_name,
        month=month,
        year=year,
    )


LOWER_IS_BETTER_METRICS = {
    "acos",
    "ads_acos",
    "ads_spend",
    "total_ads",
    "advertising_total",
    "platform_fee",
    "platformfeenew",
    "platform_fee_inventory_storage",
    "amazon_fee",
    "amazon_fees",
    "fba_fees",
    "selling_fees",
    "refund_sales",
    "return_quantity",
    "lost_total",
    "misc_transaction",
}

PRODUCT_SCOPED_METRIC_MAP = {
    "total_ads": "ads_spend",
    "advertising_total": "ads_spend",
    "advertising": "ads_spend",
    "total_cm2_profit": "cm2_profit",
}


def _unique_metrics_from_state(state: AgentState) -> List[str]:
    metrics: List[str] = []
    for metric in state.get("metric_names") or []:
        if metric and metric not in metrics:
            metrics.append(metric)
    metric_name = state.get("metric_name")
    if metric_name and metric_name not in metrics:
        metrics.insert(0, metric_name)
    return metrics or ["net_sales"]


def _is_multi_country_time_query(state: AgentState) -> bool:
    if _is_summary_request(state):
        return False

    query = (state.get("user_query") or "").lower()
    payload_type = (state.get("period_payload") or {}).get("type")
    time_movement_phrases = [
        "month on month",
        "month-on-month",
        "mom",
        "change",
        "changed",
        "growth",
        "increase",
        "decrease",
        "trend",
        "over time",
        "from ",
        " to ",
    ]
    if state.get("target_countries") and len(state.get("target_countries") or []) > 1 and payload_type == "single":
        return any(phrase in query for phrase in time_movement_phrases)
    if payload_type in {"last_n_months", "range", "multi_month"}:
        return True
    if state.get("analysis_type") in {"growth", "trend", "comparison"}:
        return True
    if state.get("answer_shape") in {"trend", "multi_month", "comparison"}:
        return True
    return any(
        phrase in query
        for phrase in time_movement_phrases + [
            "performing well",
            "performing better",
            "which country",
        ]
    )


def _previous_month(month_key: MonthKey) -> MonthKey:
    if month_key.month == 1:
        return MonthKey(year=month_key.year - 1, month=12)
    return MonthKey(year=month_key.year, month=month_key.month - 1)


def _multi_country_months_for_country(
    engine: Any,
    state: AgentState,
    country: str,
    *,
    wants_time: bool,
) -> List[MonthKey]:
    payload = state.get("period_payload") or {}
    ptype = payload.get("type")

    if ptype == "single":
        current = MonthKey(year=int(payload["year"]), month=int(payload["month"]))
        return [_previous_month(current), current] if wants_time else [current]

    if ptype == "last_n_months":
        return get_last_n_month_keys(engine, state["user_id"], country, int(payload.get("n") or 6))

    if ptype == "multi_month":
        months = payload.get("months") or state.get("target_months") or []
        return [
            MonthKey(year=int(item["year"]), month=int(item["month"]))
            for item in months
            if item.get("month") and item.get("year")
        ]

    if ptype == "range":
        period_dfs = fetch_period_dfs(
            engine=engine,
            user_id=state["user_id"],
            country=country,
            start_month=int(payload["start_month"]),
            start_year=int(payload["start_year"]),
            end_month=int(payload["end_month"]),
            end_year=int(payload["end_year"]),
            skip_missing=True,
        )
        return [month_key for month_key, _ in period_dfs]

    query = (state.get("user_query") or "").lower()
    if wants_time:
        n = 2 if any(x in query for x in ["month on month", "month-on-month", "mom"]) else 6
        return get_last_n_month_keys(engine, state["user_id"], country, n)

    latest = latest_available_month(engine, state["user_id"], country)
    return [latest]


def _sum_product_rows(rows: List[Dict[str, Any]], product_query: Optional[str]) -> Tuple[float, Optional[str]]:
    if not product_query:
        return 0.0, None

    product_match = _match_product(rows, product_query)
    if not product_match:
        return 0.0, None

    total = 0.0
    for row in rows:
        name = str(row.get("product_name") or "").strip().lower()
        sku = str(row.get("sku") or "").strip().lower()
        if name == product_match.lower() or sku == product_match.lower():
            total += float(row.get("__metric__", 0.0) or 0.0)

    return total, product_match


def _country_metric_value(
    engine: Any,
    state: AgentState,
    country: str,
    metric_name: str,
    month: int,
    year: int,
    product_query: Optional[str],
) -> Tuple[float, Optional[str]]:
    if product_query and metric_name in PRODUCT_SCOPED_METRIC_MAP:
        metric_name = PRODUCT_SCOPED_METRIC_MAP[metric_name]

    if metric_name in {"sales_mix", "profit_mix"}:
        base_metric = "net_sales" if metric_name == "sales_mix" else "profit"
        base_result = get_metric_for_month(
            engine,
            state["user_id"],
            country,
            base_metric,
            month,
            year,
        )
        denominator = float(base_result.get("total", 0.0) or 0.0)

        if product_query:
            numerator, product_match = _sum_product_rows(base_result.get("per_sku", []), product_query)
        else:
            numerator = denominator
            product_match = None

        value = 0.0 if denominator == 0 else (numerator / denominator) * 100.0
        return value, product_match

    if metric_name == "asp" and product_query:
        sales, product_match = _country_metric_value(
            engine,
            state,
            country,
            "net_sales",
            month,
            year,
            product_query,
        )
        units, _ = _country_metric_value(
            engine,
            state,
            country,
            "total_quantity",
            month,
            year,
            product_query,
        )
        return (0.0 if units == 0 else sales / units), product_match

    if metric_name in INVENTORY_METRICS:
        result = get_inventory_snapshot(
            user_id=state["user_id"],
            metric_name=metric_name,
            month=month,
            year=year,
            country=country,
        )
        value, product_match, _ = _inventory_snapshot_value(result, product_query)
        if value is None:
            return 0.0, product_match
        return value, product_match

    result = get_metric_for_month(
        engine,
        state["user_id"],
        country,
        metric_name,
        month,
        year,
    )

    if product_query:
        return _sum_product_rows(result.get("per_sku", []), product_query)

    return float(result.get("total", 0.0) or 0.0), None


def _format_country_delta(value: float, metric_name: str, country: str) -> str:
    sign = "+" if value >= 0 else "-"
    if metric_name in {"sales_mix", "profit_mix", "acos", "ads_acos", "profit_percentage", "cm2_profit_percentage", "cm2_margins"}:
        return f"{sign}{abs(value):.2f} pp"
    if metric_name in INVENTORY_METRICS:
        return f"{sign}{_format_inventory_value(abs(value), metric_name)}"
    return f"{sign}{_format_value(abs(value), metric_name, country)}"


def _metric_performance_score(metric_name: str, latest_change: Optional[Dict[str, Any]]) -> Optional[float]:
    if not latest_change:
        return None
    delta = float(latest_change.get("delta", 0.0) or 0.0)
    return -delta if metric_name in LOWER_IS_BETTER_METRICS else delta


FORECAST_QUERY_TERMS = [
    "forecast",
    "forecasted",
    "forecasting",
    "projected",
    "projection",
    "predict",
    "predicted",
    "future demand",
    "future sales",
    "dispatch",
    "purchase order",
    "po planning",
]


def _is_forecast_request(state: AgentState) -> bool:
    if state.get("needs_forecast_data"):
        return True

    query = (state.get("user_query") or "").lower()
    return any(term in query for term in FORECAST_QUERY_TERMS)


def _forecast_period_from_state(state: AgentState) -> Tuple[Optional[int], Optional[int], str]:
    payload = state.get("period_payload") or state.get("period_parsed") or {}

    if payload.get("type") == "single":
        month = int(payload.get("month"))
        year = int(payload.get("year"))
        return month, year, datetime(year, month, 1).strftime("%b %Y")

    if payload.get("type") == "range":
        month = int(payload.get("start_month"))
        year = int(payload.get("start_year"))
        start_label = datetime(year, month, 1).strftime("%b %Y")
        end_label = datetime(int(payload.get("end_year")), int(payload.get("end_month")), 1).strftime("%b %Y")
        return month, year, f"{start_label} to {end_label}"

    return None, None, "latest available forecast"


def _forecast_query_has_contextual_period_reference(query: str) -> bool:
    q = _normalize(query)
    return any(
        phrase in q
        for phrase in [
            "same period",
            "same month",
            "that period",
            "that month",
            "this period",
            "this month",
            "above period",
            "previous period",
        ]
    )


def _clean_forecast_product_query(state: AgentState) -> Optional[str]:
    product_query = state.get("product_query")
    if not product_query:
        return None

    pq = _normalize(str(product_query))
    countries = set(state.get("target_countries") or [])
    countries.add(str(state.get("country") or "").lower())

    country_aliases = {
        "uk", "united kingdom", "amazon uk",
        "us", "usa", "united states", "amazon us",
        "global", "all countries", "both countries",
    }
    generic_terms = set(FORECAST_QUERY_TERMS) | {
        "forecast",
        "forecasting",
        "projection",
        "projected",
        "demand",
        "future",
        "stock",
        "inventory",
        "country",
        "market",
    }

    if pq in countries or pq in country_aliases or pq in generic_terms:
        return None

    return product_query


def _format_forecast_money(value: Any, country: Optional[str], metric_name: str = "net_sales") -> str:
    try:
        return _format_value(float(value or 0.0), metric_name, country)
    except Exception:
        return str(value)


def _compute_forecast_analysis(state: AgentState) -> AgentState:
    countries = state.get("target_countries") or [state.get("country") or "uk"]
    month, year, period_label = _forecast_period_from_state(state)
    product_query = _clean_forecast_product_query(state)

    results = []
    for country in countries:
        inventory = get_inventory_forecast_snapshot(
            user_id=state["user_id"],
            country=country,
            month=month,
            year=year,
            product_query=product_query,
        )

        if product_query and inventory.get("available") and not inventory.get("row_count"):
            inventory = get_inventory_forecast_snapshot(
                user_id=state["user_id"],
                country=country,
                month=month,
                year=year,
                product_query=None,
            )

        pnl = get_pnl_forecast_snapshot(
            user_id=state["user_id"],
            country=country,
            month=month,
            year=year,
            product_query=product_query,
        )
        if product_query and pnl.get("available") and not pnl.get("row_count"):
            pnl = get_pnl_forecast_snapshot(
                user_id=state["user_id"],
                country=country,
                month=month,
                year=year,
                product_query=None,
            )

        inventory_rows = inventory.get("rows") or []
        pnl_rows = pnl.get("rows") or []
        top_inventory = inventory_rows[:3]
        top_pnl = pnl_rows[:3]

        results.append({
            "country": country,
            "country_label": _country_display_name(country),
            "period_label": period_label,
            "inventory": inventory,
            "pnl": pnl,
            "top_inventory": top_inventory,
            "top_pnl": top_pnl,
        })

    state["current_metrics"] = {
        "metric": "forecast",
        "period_label": period_label,
        "total": None,
    }
    state["analysis_result"] = {
        "type": "forecast",
        "countries": countries,
        "period_label": period_label,
        "requested_month": month,
        "requested_year": year,
        "product": product_query,
        "results": results,
    }
    return state


def _is_summary_request(state: AgentState) -> bool:
    query = (state.get("user_query") or "").lower()
    return (
        state.get("analysis_type") == "summary"
        or state.get("answer_shape") == "summary"
        or "summary" in query
    )


def _query_has_mtd(state: AgentState) -> bool:
    query = (state.get("user_query") or "").lower()
    return bool(re.search(r"\bmtd\b|\bmonth\s+to\s+date\b", query))


def _month_keys_between(start_month: int, start_year: int, end_month: int, end_year: int) -> List[MonthKey]:
    months: List[MonthKey] = []
    cur_month = int(start_month)
    cur_year = int(start_year)

    while (cur_year, cur_month) <= (int(end_year), int(end_month)):
        months.append(MonthKey(year=cur_year, month=cur_month))
        cur_month += 1
        if cur_month > 12:
            cur_month = 1
            cur_year += 1

    return months


def _summary_months_for_country(engine: Any, state: AgentState, country: str) -> List[MonthKey]:
    payload = state.get("period_payload") or {}
    if payload.get("type") == "growth_base":
        payload = _prepare_period_payload(payload.get("base") or {}, "absolute")

    ptype = payload.get("type")

    if ptype == "single":
        return [MonthKey(year=int(payload["year"]), month=int(payload["month"]))]

    if ptype == "multi_month":
        months = payload.get("months") or state.get("target_months") or []
        return [
            MonthKey(year=int(item["year"]), month=int(item["month"]))
            for item in months
            if item.get("month") and item.get("year")
        ]

    if ptype == "range":
        return _month_keys_between(
            int(payload["start_month"]),
            int(payload["start_year"]),
            int(payload["end_month"]),
            int(payload["end_year"]),
        )

    if ptype == "last_n_months":
        return _last_n_window(engine, state["user_id"], country, int(payload.get("n") or 6))

    latest = latest_available_month(engine, state["user_id"], country)
    return [latest]


def _summary_period_label(state: AgentState, months: List[MonthKey]) -> str:
    if not months:
        return "selected period"

    if len(months) == 1:
        label = months[0].label
        return f"{label} MTD" if _query_has_mtd(state) else label

    return f"{months[0].label} to {months[-1].label}"


def _summary_metric_names_for_state(state: AgentState) -> List[str]:
    query = (state.get("user_query") or "").lower()
    requested = _unique_metrics_from_state(state)

    if any(metric in INVENTORY_METRICS for metric in requested):
        base = ["available", "inbound_quantity", "days_of_supply"]
    elif any(metric in {"total_ads", "ads_spend", "product_spend", "brand_spend", "display_spend"} for metric in requested) or any(word in query for word in ["ads", "ad spend", "advertising"]):
        base = ["total_ads", "net_sales", "acos", "total_cm2_profit"]
    elif any(metric in {"profit", "total_cm2_profit", "cm2_profit"} for metric in requested) or any(word in query for word in ["profit", "margin", "cm2"]):
        base = ["profit", "net_sales", "total_cm2_profit", "total_ads", "total_quantity", "asp"]
    elif any(metric in {"net_sales", "gross_sales", "product_sales"} for metric in requested) or any(word in query for word in ["sales", "revenue"]):
        base = ["net_sales", "profit", "total_cm2_profit", "total_ads", "total_quantity", "asp"]
    else:
        base = ["net_sales", "profit", "total_cm2_profit", "total_ads", "total_quantity", "asp"]

    out: List[str] = []
    for metric in requested + base:
        if metric and metric not in out:
            out.append(metric)

    return out[:6]


def _aggregate_summary_metric(
    engine: Any,
    state: AgentState,
    country: str,
    metric_name: str,
    months: List[MonthKey],
    product_query: Optional[str],
) -> Tuple[Optional[float], Optional[str]]:
    if not months:
        return None, None

    if metric_name == "asp":
        sales, product_match = _aggregate_summary_metric(
            engine,
            state,
            country,
            "net_sales",
            months,
            product_query,
        )
        units, _ = _aggregate_summary_metric(
            engine,
            state,
            country,
            "total_quantity",
            months,
            product_query,
        )
        if not units:
            return 0.0, product_match
        return float(sales or 0.0) / float(units), product_match

    total = 0.0
    found_value = False
    product_match = None

    for month_key in months:
        try:
            value, matched = _country_metric_value(
                engine,
                state,
                country,
                metric_name,
                month_key.month,
                month_key.year,
                product_query,
            )
        except Exception:
            logger.debug(
                "Skipping summary metric %s for %s %s",
                metric_name,
                country,
                month_key.label,
                exc_info=True,
            )
            continue

        found_value = True
        product_match = product_match or matched
        total += float(value or 0.0)

    if not found_value:
        return None, product_match

    return total, product_match


def _top_sales_product_for_summary(
    engine: Any,
    state: AgentState,
    country: str,
    months: List[MonthKey],
) -> Optional[Dict[str, Any]]:
    if state.get("product_query"):
        return None

    totals: Dict[Tuple[str, str], float] = {}
    labels: Dict[Tuple[str, str], Tuple[str, str]] = {}

    for month_key in months:
        try:
            result = get_metric_for_month(
                engine,
                state["user_id"],
                country,
                "net_sales",
                month_key.month,
                month_key.year,
            )
        except Exception:
            logger.debug("Summary top product unavailable for %s %s", country, month_key.label, exc_info=True)
            continue

        for row in result.get("per_sku", []):
            product_name = str(row.get("product_name") or row.get("sku") or "Unknown").strip()
            sku = str(row.get("sku") or "").strip()
            key = (product_name.lower(), sku.lower())
            labels[key] = (product_name, sku)
            totals[key] = totals.get(key, 0.0) + float(row.get("__metric__", 0.0) or 0.0)

    if not totals:
        return None

    best_key, best_value = max(totals.items(), key=lambda item: item[1])
    product_name, sku = labels.get(best_key, ("Unknown", ""))
    product_label = _format_product_with_sku(product_name, sku, product_name)

    return {
        "product": product_label,
        "value": best_value,
        "formatted": _format_metric_for_display(best_value, "net_sales", country),
    }


def _build_country_summary_row(
    engine: Any,
    state: AgentState,
    country: str,
    metric_names: List[str],
) -> Dict[str, Any]:
    months = _summary_months_for_country(engine, state, country)
    period_label = _summary_period_label(state, months)
    product_query = state.get("product_query")
    metrics: Dict[str, float] = {}
    formatted_metrics: Dict[str, str] = {}
    product_match = None

    for metric_name in metric_names:
        value, matched = _aggregate_summary_metric(
            engine,
            state,
            country,
            metric_name,
            months,
            product_query,
        )
        if value is None:
            continue
        product_match = product_match or matched
        metrics[metric_name] = float(value)
        formatted_metrics[metric_name] = _format_metric_for_display(float(value), metric_name, country)

    return {
        "country": country,
        "country_label": _country_display_name(country),
        "period_label": period_label,
        "months": [{"month": mk.month, "year": mk.year, "label": mk.label} for mk in months],
        "metrics": metrics,
        "formatted_metrics": formatted_metrics,
        "product": product_match or product_query,
        "top_product": _top_sales_product_for_summary(engine, state, country, months),
    }


def _compute_multi_country(state: AgentState) -> AgentState:
    engine = state["engine"]
    metric_names = _unique_metrics_from_state(state)
    countries = state.get("target_countries") or []
    payload = state.get("period_payload") or {}
    wants_time = _is_multi_country_time_query(state)
    product_query = state.get("product_query")

    if _is_summary_request(state):
        summary_metrics = _summary_metric_names_for_state(state)
        rows = [
            _build_country_summary_row(engine, state, country, summary_metrics)
            for country in countries
        ]
        period_labels = [row.get("period_label") for row in rows if row.get("period_label")]

        state["current_metrics"] = {
            "metric": ", ".join(summary_metrics),
            "period_label": period_labels[0] if len(set(period_labels)) == 1 else "selected period",
            "total": None,
        }
        state["analysis_result"] = {
            "type": "multi_country_summary",
            "mode": "summary",
            "metrics": summary_metrics,
            "countries": countries,
            "results": rows,
        }
        return state

    if wants_time:
        trend_results = []
        winners = []

        for metric_name in metric_names:
            metric_rows = []

            for country in countries:
                try:
                    months = _multi_country_months_for_country(
                        engine,
                        state,
                        country,
                        wants_time=True,
                    )
                except Exception as exc:
                    trend_results.append({
                        "country": country,
                        "metric": metric_name,
                        "error": str(exc),
                        "series": [],
                        "mom": [],
                    })
                    continue

                series = []
                product_match = None
                for month_key in months:
                    try:
                        value, matched = _country_metric_value(
                            engine,
                            state,
                            country,
                            metric_name,
                            month_key.month,
                            month_key.year,
                            product_query,
                        )
                        product_match = product_match or matched
                        series.append({
                            "month": month_key.month,
                            "year": month_key.year,
                            "period_label": month_key.label,
                            "value": float(value),
                            "formatted": _format_metric_for_display(float(value), metric_name, country),
                        })
                    except Exception as exc:
                        series.append({
                            "month": month_key.month,
                            "year": month_key.year,
                            "period_label": month_key.label,
                            "value": None,
                            "error": str(exc),
                        })

                valid_series = [row for row in series if row.get("value") is not None]
                mom = []
                for idx in range(1, len(valid_series)):
                    prev = float(valid_series[idx - 1]["value"])
                    curr = float(valid_series[idx]["value"])
                    delta = curr - prev
                    pct_change = None if prev == 0 else (delta / prev) * 100.0
                    mom.append({
                        "period_label": valid_series[idx]["period_label"],
                        "previous": prev,
                        "current": curr,
                        "delta": delta,
                        "pct_change": pct_change,
                        "delta_formatted": _format_country_delta(delta, metric_name, country),
                        "current_formatted": _format_metric_for_display(curr, metric_name, country),
                        "previous_formatted": _format_metric_for_display(prev, metric_name, country),
                    })

                latest_change = mom[-1] if mom else None
                score = _metric_performance_score(metric_name, latest_change)
                result_row = {
                    "country": country,
                    "metric": metric_name,
                    "product": product_match or product_query,
                    "series": series,
                    "mom": mom,
                    "latest_change": latest_change,
                    "score": score,
                }
                trend_results.append(result_row)
                metric_rows.append(result_row)

            scorable = [row for row in metric_rows if row.get("score") is not None]
            if scorable:
                winner = max(scorable, key=lambda row: row["score"])
                winners.append({
                    "metric": metric_name,
                    "country": winner["country"],
                    "product": winner.get("product") or product_query,
                    "latest_change": winner.get("latest_change"),
                    "reason": "lower movement is better" if metric_name in LOWER_IS_BETTER_METRICS else "higher improvement is better",
                })

        state["current_metrics"] = {
            "metric": ", ".join(metric_names),
            "period_label": "multi-country trend",
            "total": None,
        }
        state["analysis_result"] = {
            "type": "multi_country",
            "mode": "trend",
            "metrics": metric_names,
            "countries": countries,
            "product": product_query,
            "results": trend_results,
            "winners": winners,
        }
        return state

    if payload.get("type") == "single":
        month = payload["month"]
        year = payload["year"]
    else:
        month = None
        year = None

    results = []

    for metric_name in metric_names:
        for country in countries:
            country_month = month
            country_year = year

            if country_month is None or country_year is None:
                latest = latest_available_month(engine, state["user_id"], country)
                country_month = latest.month
                country_year = latest.year

            value, product_match = _country_metric_value(
                engine,
                state,
                country,
                metric_name,
                country_month,
                country_year,
                product_query,
            )

            results.append({
                "country": country,
                "metric": metric_name,
                "value": value,
                "formatted": _format_metric_for_display(value, metric_name, country),
                "period_label": datetime(country_year, country_month, 1).strftime("%b %Y"),
                "product": product_match or product_query,
            })

    state["current_metrics"] = {
        "metric": ", ".join(metric_names),
        "period_label": results[0]["period_label"] if results else "selected period",
        "total": None,
    }

    state["analysis_result"] = {
        "type": "multi_country",
        "mode": "single",
        "metrics": metric_names,
        "countries": countries,
        "results": results,
    }

    return state

def _ordinal_label(n: int) -> str:
    try:
        n = int(n)
    except Exception:
        return "selected"

    if 10 <= n % 100 <= 20:
        suffix = "th"
    else:
        suffix = {1: "st", 2: "nd", 3: "rd"}.get(n % 10, "th")

    return f"{n}{suffix}"


def _compute_ranked_extreme_contribution(state: AgentState) -> AgentState:
    engine = state["engine"]

    base_metric_name = state.get("base_metric_name") or state.get("metric_name") or "profit"
    contribution_metric_name = state.get("metric_name") or base_metric_name

    payload = state.get("period_payload") or {"type": "latest_month"}

    extreme_type = state.get("extreme_type") or "max"
    extreme_rank = int(state.get("extreme_rank") or 1)
    top_n = int(state.get("top_n") or 1)

    # -------- GET MONTH WINDOW --------
    if payload.get("type") == "last_n_months":
        months = _last_n_window(
            engine,
            state["user_id"],
            state["country"],
            payload.get("n", 12),
        )

    elif payload.get("type") == "single":
        months = [
            SimpleNamespace(
                month=payload["month"],
                year=payload["year"],
                label=datetime(payload["year"], payload["month"], 1).strftime("%b %Y"),
            )
        ]

    elif payload.get("type") == "range":
        dfs = fetch_period_dfs(
            engine,
            state["user_id"],
            state["country"],
            payload["start_month"],
            payload["start_year"],
            payload["end_month"],
            payload["end_year"],
            skip_missing=True,
        )
        months = [mk for mk, _ in dfs]

    else:
        latest = latest_available_month(
            engine,
            state["user_id"],
            state["country"],
        )
        months = [latest]

    if not months:
        state["analysis_result"] = {
            "type": "invalid",
            "message": "No months found for the selected period.",
        }
        return state

    # -------- RANK MONTHS BY BASE METRIC --------
    month_scores = []

    for mk in months:
        result_month = get_metric_for_month(
            engine,
            state["user_id"],
            state["country"],
            base_metric_name,
            mk.month,
            mk.year,
        )

        month_scores.append({
            "month": mk.month,
            "year": mk.year,
            "period_label": getattr(
                mk,
                "label",
                datetime(mk.year, mk.month, 1).strftime("%b %Y"),
            ),
            "value": float(result_month.get("total", 0.0) or 0.0),
        })

    month_scores = sorted(
        month_scores,
        key=lambda x: x["value"],
        reverse=(extreme_type != "min"),
    )

    if extreme_rank > len(month_scores):
        extreme_rank = len(month_scores)

    selected_month = month_scores[extreme_rank - 1]

    logger.info(
        f"[RANKED_EXTREME_SELECTED] "
        f"rank={extreme_rank}, "
        f"month={selected_month['month']}, "
        f"year={selected_month['year']}, "
        f"value={selected_month['value']}"
    )

    # -------- RANK PRODUCTS INSIDE SELECTED MONTH --------
    contribution_result = get_metric_for_month(
        engine,
        state["user_id"],
        state["country"],
        contribution_metric_name,
        selected_month["month"],
        selected_month["year"],
    )

    ranked = rank_skus(
        contribution_result,
        direction="top",
        limit=top_n,
    )

    state["current_metrics"] = {
        "metric": contribution_metric_name,
        "base_metric": base_metric_name,
        "period_label": selected_month["period_label"],
        "month": selected_month["month"],
        "year": selected_month["year"],
        "total": contribution_result.get("total", 0.0),
        "selected_month_rank": extreme_rank,
        "selected_month_value": selected_month["value"],
    }

    state["analysis_result"] = {
        "type": "ranking",
        "metric": contribution_metric_name,
        "base_metric": base_metric_name,
        "period_label": selected_month["period_label"],
        "per_sku": ranked,
        "total": contribution_result.get("total", 0.0),
        "ranking_direction": "top",
        "context": {
            "type": "ranked_extreme_contribution",
            "extreme_type": extreme_type,
            "extreme_rank": extreme_rank,
            "selected_month": selected_month,
        },
    }

    return state


def _execute_tool(state: AgentState, tool_name: str) -> AgentState:
    registry: Dict[str, Callable[[AgentState], AgentState]] = {
        "event_plan": _run_event_planner,
        "summary": _compute_summary,
        "ranking": _compute_ranking,
        "ranked_extreme_contribution": _compute_ranked_extreme_contribution,
        "extreme": _compute_extreme,
        "multi_month": _compute_multi_month,
        "business_advisor": _compute_business_advisor,
        "decision": _compute_decision,
        "sku_intelligence": _compute_sku_intelligence,
        "sku_trend": _compute_sku_trend,
        "standard_analysis": _compute_standard_analysis,
        "multi_country": _compute_multi_country,
        "forecast_analysis": _compute_forecast_analysis,
    }

    try:
        # -------- TOOL START --------
        logger.info(f"[TOOL_EXEC] Starting tool: {tool_name}")

        # Log key state inputs (safe subset)
        logger.info(
            f"[TOOL_INPUT] tool={tool_name}, "
            f"metric={state.get('metric_name')}, "
            f"metrics={state.get('metric_names')}, "
            f"product={state.get('product_query')}, "
            f"products={state.get('product_queries')}, "
            f"analysis_type={state.get('analysis_type')}, "
            f"period={state.get('period_payload')}"
        )

        # Track execution
        state.setdefault("tool_trace", []).append(tool_name)

        # -------- EXECUTE TOOL --------
        if tool_name not in registry:
            raise ValueError(f"Unknown tool: {tool_name}")

        result_state = registry[tool_name](state)

        # -------- TOOL OUTPUT --------
        logger.info(
            f"[TOOL_DONE] tool={tool_name}, "
            f"current_metrics={result_state.get('current_metrics')}, "
            f"analysis_type={result_state.get('analysis_result', {}).get('type')}"
        )

        return result_state

    except Exception as e:
        logger.exception(f"[TOOL_ERROR] tool={tool_name} failed")

        # Attach error but don't crash agent
        state["tool_error"] = {
            "tool": tool_name,
            "error": str(e)
        }

        return state

def _verify_and_replan(state: AgentState) -> List[str]:
    if state.get("analysis_result") or state.get("event_plan_result"):
        return []
    if state.get("intent") in {"chat", "clarify", "explain"}:
        return []
    return ["standard_analysis"]


def _generate_advice(state: AgentState) -> AgentState:
    if not state.get("needs_advice"):
        state["advice"] = []
        return state
    analysis = {
        "current_metrics": state.get("current_metrics"),
        "comparison": state.get("comparison"),
        "analysis_result": state.get("analysis_result"),
        "event_plan_result": state.get("event_plan_result"),
        "sku_intelligence_result": state.get("sku_intelligence_result"),
    }
    if advisor_llm:
        try:
            resp = advisor_llm.invoke(ADVICE_PROMPT + "\n\n" + json.dumps(analysis, default=str))
            lines = [line.strip("- ").strip() for line in resp.content.splitlines() if line.strip()]
            state["advice"] = [line for line in lines[:5]]
            return state
        except Exception:
            logger.exception("Advice generation failed; using deterministic fallback")
    advice: List[str] = []
    comp = state.get("comparison") or {}
    root_cause = (state.get("sku_intelligence_result") or {}).get("root_cause") or {}
    if comp and comp.get("pct_change") is not None and comp["pct_change"] < 0:
        advice.append("Review the biggest negative period-over-period drivers and isolate the weak SKUs.")
    if root_cause.get("primary_driver") == "units":
        advice.append("The biggest driver appears to be units, so check demand, stock position, and conversion first.")
    elif root_cause.get("primary_driver") == "asp":
        advice.append("The biggest driver appears to be ASP, so review pricing, discounting, and product mix.")
    if state.get("analysis_result", {}).get("type") == "breakdown":
        advice.append("Focus budget on the top profitable SKUs and trim low-contribution products.")
    if state.get("analysis_result", {}).get("type") == "summary":
        advice.append("Review ads, marketplace fees, and refunds together before changing pricing.")
    if state.get("analysis_result", {}).get("type") == "sku_intelligence":
        advice.append("Check whether sales, units, ASP, and mix are aligned before changing promotion strategy.")
    if state.get("event_plan_result"):
        advice.append("Validate inventory coverage and keep pricing guardrails aligned to expected uplift.")
    state["advice"] = advice[:5]
    return state


def _send_email_if_requested(state: AgentState) -> AgentState:
    # -------- ENTRY LOG --------
    logger.info("[EMAIL] Entered _send_email_if_requested")

    if not state.get("email_requested"):
        logger.info("[EMAIL] Skipped (email_requested=False)")
        return state

    # -------- SUBJECT BUILD --------
    subject_metric = (
        state.get("metric_name")
        or (state.get("current_metrics") or {}).get("metric")
        or "summary"
    )

    subject_period = (
        (state.get("current_metrics") or {}).get("period_label")
        or "selected period"
    )

    if state.get("event_plan_result"):
        subject_metric = "event plan"

    if state.get("sku_intelligence_result"):
        subject_metric = f"sku intelligence - {state.get('product_match') or 'product'}"

    subject = f"Phormula AI {subject_metric.replace('_', ' ').title()} - {subject_period}"

    logger.info(f"[EMAIL] Subject: {subject}")

    # -------- BODY BUILD (STRUCTURED HTML) --------
    try:
        html = build_email_html(state)
        logger.info("[EMAIL] HTML report built successfully")
    except Exception:
        logger.exception("[EMAIL] Failed to build HTML report")
        html = "<p>Failed to build report.</p>"

    # -------- ATTACHMENTS --------
    attachments = []

    if state.get("include_csv"):
        logger.info("[EMAIL] CSV requested → building attachment")
        try:
            csv_file = build_excel_attachment(state)
            attachments.append(csv_file)
            logger.info("[EMAIL] CSV attachment created")
        except Exception:
            logger.exception("[EMAIL] Failed to build CSV attachment")

    logger.info(f"[EMAIL] Attachments: {[a['filename'] for a in attachments]}")

    # -------- SEND EMAIL --------
    try:
        logger.info("[EMAIL] Sending email...")

        result = send_agent_email(
            user_id=state["user_id"],
            subject=subject,
            html_body=html,
            attachments=attachments,
        )

        logger.info(f"[EMAIL] SUCCESS: {result}")
        state["email_result"] = result

    except Exception as e:
        logger.exception("[EMAIL_ERROR] Failed to send email")

        state["email_result"] = {
            "status": "failed",
            "error": str(e)
        }

    # -------- EXIT LOG --------
    logger.info(f"[EMAIL] Final email_result: {state.get('email_result')}")

    # -------- FINAL RESPONSE --------
    if state.get("email_result", {}).get("status") == "sent":
        state["final_response"] = f"📩 Email sent to {state['email_result'].get('recipient')}"
    else:
        state["final_response"] = "❌ Failed to send email. Please try again."

    return state



def humanize_metric(metric: str) -> str:
    if not metric:
        return ""

    metric = str(metric).lower().strip()

    replacements = {

        # -------- COMMON --------
        "acos": "ACOS",
        "asp": "ASP",
        "roi": "ROI",
        "cpc": "CPC",
        "ctr": "CTR",
        "cvr": "CVR",
        "roas": "ROAS",

        # -------- PROFITS --------
        "cm2_profit": "Productwise CM2 Profit",
        "total_cm2_profit": "CM2 Profit",
        "cm2_profit_percentage": "CM2 Profit %",
        "cm2_margins": "CM2 Margins",

        # -------- TYPO FIXES --------
        "rembursement_fee": "Reimbursement Fee",
        "rembursment_vs_cm2_margins": "Reimbursement vs CM2 Margins",
        "dealsvouchar_ads": "Deals Voucher Ads",
        "tex_and_credits": "Tax and Credits",
        "platformfeenew": "Platform Fee",

        # -------- ADS --------
        "visible_ads": "Visible Ads",
        "ads_spend": "Productwise Ad Spend",
        "total_ads": "Ad Spend",
        "advertising_total": "Advertising Total",

        "product_spend": "Sponsored Product Spend",
        "brand_spend": "Sponsored Brand Spend",
        "display_spend": "Sponsored Display Spend",

        "sp_ads_sales": "Sponsored Product Ad Sales",
        "sb_ads_sales": "Sponsored Brand Ad Sales",
        "sd_ads_sales": "Sponsored Display Ad Sales",
        "ads_sale_amount": "Ad Sales",

        # -------- SALES --------
        "net_sales": "Net Sales",
        "gross_sales": "Gross Sales",
        "refund_sales": "Refund Sales",
        "product_sales": "Product Sales",

        # -------- FEES --------
        "platform_fee_inventory_storage": "Platform Fee Inventory Storage",
        "other_transaction_fees": "Other Transaction Fees",
        
        "shipment_fees": "Shipment Fees",

        # -------- INVENTORY --------
        "available": "Available Inventory",
        "inbound_quantity": "Inbound Inventory",
        "total_reserved_quantity": "Reserved Inventory",
        "unfulfillable_quantity": "Unfulfillable Inventory",
        "units_shipped_t30": "Units Shipped (30 Days)",
        "units_shipped_t60": "Units Shipped (60 Days)",
        "units_shipped_t90": "Units Shipped (90 Days)",
        "sell_through": "Sell Through",
        "days_of_supply": "Days of Supply",
        "estimated_excess_quantity": "Estimated Excess Inventory",

        # -------- MISC --------
        "sales_mix": "Sales Mix",
        "profit_mix": "Profit Mix",
    }

    if metric in replacements:
        return replacements[metric]

    # fallback auto formatter
    return (
        metric
        .replace("_", " ")
        .title()
    )


def _country_display_name(country: Optional[str]) -> str:
    code = (country or "").strip().lower()
    labels = {
        "uk": "Amazon UK",
        "gb": "Amazon UK",
        "us": "Amazon US",
        "usa": "Amazon US",
        "de": "Amazon Germany",
        "fr": "Amazon France",
        "it": "Amazon Italy",
        "es": "Amazon Spain",
        "ca": "Amazon Canada",
        "in": "Amazon India",
    }
    return labels.get(code, f"Amazon {code.upper()}" if code else "Selected marketplace")


def _display_product_name(product: Optional[str]) -> str:
    if not product:
        return ""
    value = str(product).strip()
    if not value:
        return ""
    if value == value.lower() or value == value.upper():
        return value.title()
    return value

#new

# def _render_response(state: AgentState) -> AgentState:

#     current = state.get("current_metrics") or {}
#     metric_name = current.get("metric") or state.get("metric_name") or "metric"
#     period_label = current.get("period_label") or "selected period"
#     analysis = state.get("analysis_result") or {}
#     comp = state.get("comparison") or {}

#     metric_clean = humanize_metric(metric_name)

#     response = None

#     # -------- CHAT / EXPLAIN --------
#     if state.get("intent") in {"chat", "explain"}:
#         response = {
#             "type": "text",
#             "message": "General conversation response"
#         }

#     # -------- GROWTH / TREND --------
#     elif analysis.get("type") in {"growth", "trend"}:

#         raw_mom = analysis.get("mom", [])

#         mom_data = []

#         for m in raw_mom:
#             pct = float(m.get("pct_change", 0))

#             sign = "+" if pct > 0 else "-" if pct < 0 else ""

#             period = (
#                 m.get("period_label")
#                 or f"{m.get('month', '')} {m.get('year', '')}".strip()
#             )

#             mom_data.append({
#                 "period": period,
#                 "pct_change": pct,
#                 "formatted": f"{sign}{abs(pct):.2f}%"
#             })

#         # -------- TOP GROWTH / DECLINE --------
#         top_growth = []
#         top_decline = []

#         try:
#             series = analysis.get("series", [])

#             if len(series) >= 2:

#                 latest_month = series[-1]
#                 prev_month = series[-2]

#                 latest_data = get_metric_for_month(
#                     state["engine"],
#                     state["user_id"],
#                     state["country"],
#                     metric_name,
#                     latest_month["month"],
#                     latest_month["year"]
#                 )

#                 prev_data = get_metric_for_month(
#                     state["engine"],
#                     state["user_id"],
#                     state["country"],
#                     metric_name,
#                     prev_month["month"],
#                     prev_month["year"]
#                 )

#                 latest_map = {
#                     r.get("product_name"): float(r.get("__metric__", 0))
#                     for r in latest_data.get("per_sku", [])
#                 }

#                 prev_map = {
#                     r.get("product_name"): float(r.get("__metric__", 0))
#                     for r in prev_data.get("per_sku", [])
#                 }

#                 growth_rows = []

#                 for p in set(latest_map) | set(prev_map):

#                     delta = latest_map.get(p, 0.0) - prev_map.get(p, 0.0)

#                     growth_rows.append({
#                         "product": p,
#                         "change": delta
#                     })

#                 # TOP GROWERS
#                 sorted_growth = sorted(
#                     growth_rows,
#                     key=lambda x: x["change"],
#                     reverse=True
#                 )

#                 for g in sorted_growth[:3]:

#                     val = float(g["change"])

#                     top_growth.append({
#                         "product": g["product"],
#                         "change": val,
#                         "formatted": f"+{_format_value(abs(val), metric_name, state.get('country'))}"
#                     })

#                 # TOP DECLINERS
#                 sorted_decline = sorted(
#                     growth_rows,
#                     key=lambda x: x["change"]
#                 )

#                 for g in sorted_decline[:3]:

#                     val = float(g["change"])

#                     top_decline.append({
#                         "product": g["product"],
#                         "change": val,
#                         "formatted": f"-{_format_value(abs(val), metric_name, state.get('country'))}"
#                     })

#         except Exception as e:
#             logger.exception("[TOP_GROWTH_DRIVER_ERROR]")

#         response = {
#             "type": "trend",
#             "metric": metric_clean,
#             "period": period_label,
#             "series": analysis.get("series", []),
#             "mom": mom_data,
#             "top_growth": top_growth,
#             "top_decline": top_decline
#         }

#     # -------- RANKING --------
#     elif analysis.get("type") == "ranking":
#         rows = analysis.get("per_sku", [])

#         response = {
#             "type": "ranking",
#             "metric": metric_clean,
#             "direction": analysis.get("ranking_direction"),
#             "items": [
#                 {
#                     "rank": i + 1,
#                     "product": r.get("product_name"),
#                     "value": r.get("__metric__"),
#                     "formatted": _format_value(
#                         float(r.get("__metric__", 0)),
#                         metric_name,
#                         state.get("country")
#                     )
#                 }
#                 for i, r in enumerate(rows)
#             ]
#         }

#     # -------- SUMMARY --------
#     elif analysis.get("type") == "summary":
#         response = {
#             "type": "summary",
#             "period": period_label,
#             "metrics": analysis.get("metrics", {}),
#             "top_products": analysis.get("top_products", []),
#             "insights": state.get("insights")
#         }

#     # -------- SKU INTELLIGENCE --------
#     elif state.get("sku_intelligence_result"):
#         res = state["sku_intelligence_result"]

#         response = {
#             "type": "sku_intelligence",
#             "product": res.get("product_match"),
#             "current": res.get("current"),
#             "previous": res.get("previous"),
#             "summary": res.get("summary_points"),
#             "trend": res.get("trend")
#         }

#     # -------- COMPARISON --------
#     elif comp:
#         response = {
#             "type": "comparison",
#             "metric": metric_clean,
#             "current": comp.get("left"),
#             "previous": comp.get("right"),
#             "change_pct": comp.get("pct_change")
#         }

#     # -------- BREAKDOWN --------
#     elif analysis.get("type") == "breakdown":
#         response = {
#             "type": "breakdown",
#             "metric": metric_clean,
#             "items": analysis.get("per_sku", [])
#         }

#     # -------- EXTREME --------
#     elif analysis.get("type") == "extreme":
#         response = {
#             "type": "extreme",
#             "metric": metric_clean,
#             "value": analysis.get("value"),
#             "period": analysis.get("period_label"),
#             "product": analysis.get("product")
#         }

#     elif analysis.get("type") == "multi_country":
#         response = {
#             "type": "multi_country",
#             "metric": metric_clean,
#             "period": period_label,
#             "items": analysis.get("results", []),
#         }    

#     # -------- SINGLE VALUE --------
#     elif current.get("total") is not None:
#         total = current.get("total")

#         response = {
#             "type": "single_value",
#             "metric": metric_clean,
#             "period": period_label,
#             "value": total,
#             "formatted": _format_value(
#                 float(total),
#                 metric_name,
#                 state.get("country")
#             )
#         }

#     # -------- FALLBACK --------
#     else:
#         response = {
#             "type": "error",
#             "message": "Unable to process request"
#         }

#     # 🔥 FINAL STEP — ALWAYS CONVERT TO JSON STRING
#     state["final_response"] = json.dumps(response)

#     return state


#old

def _fmt_business_money(state: AgentState, value: Any, metric_name: str = "net_sales") -> str:
    try:
        return _format_value(float(value or 0.0), metric_name, state.get("country"))
    except Exception:
        return str(value)


def _format_inventory_value(value: Any, metric_name: str) -> str:
    try:
        numeric_value = float(value or 0.0)
    except Exception:
        return str(value)

    if metric_name == "sell_through":
        return f"{numeric_value:.2f}%"
    if metric_name == "days_of_supply":
        return f"{numeric_value:,.1f} days"
    return f"{numeric_value:,.0f} units"


def _format_metric_for_display(value: Any, metric_name: str, country: Optional[str]) -> str:
    if metric_name in INVENTORY_METRICS:
        return _format_inventory_value(value, metric_name)
    return _format_value(float(value or 0.0), metric_name, country)


def _first_product(rows: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    return rows[0] if rows else None


def _render_business_advisor_fallback(state: AgentState, analysis: Dict[str, Any]) -> str:
    context = analysis.get("context") or {}
    period = (context.get("period") or {}).get("label") or "selected period"
    totals = context.get("totals") or {}
    derived = context.get("derived") or {}
    rankings = context.get("rankings") or {}
    history = context.get("history") or {}
    movement = history.get("movement") or {}

    lines = [f"Business read for {period}:", "", "What I see:"]
    lines.append(
        "- Sales were "
        f"{_fmt_business_money(state, totals.get('net_sales'), 'net_sales')}, "
        f"profit was {_fmt_business_money(state, totals.get('profit'), 'profit')}, "
        f"and CM2 profit was {_fmt_business_money(state, totals.get('total_cm2_profit') or totals.get('cm2_profit'), 'cm2_profit')}."
    )

    if totals.get("ads_spend") or totals.get("total_ads"):
        lines.append(
            "- Advertising spend was "
            f"{_fmt_business_money(state, totals.get('ads_spend') or totals.get('total_ads'), 'ads_spend')} "
            f"({float(derived.get('ad_to_sales_pct') or 0.0):.2f}% of sales), "
            f"with ROAS {float(derived.get('ad_roas') or 0.0):.2f}."
        )

    if totals.get("selling_fees") or totals.get("fba_fees") or totals.get("platform_fee"):
        lines.append(
            "- Fee burden was "
            f"{float(derived.get('fee_ratio_pct') or 0.0):.2f}% of sales across selling, FBA, and platform fees."
        )

    if movement:
        sales_move = movement.get("net_sales") or {}
        profit_move = movement.get("profit") or {}
        if sales_move or profit_move:
            lines.append(
                "- Latest movement: sales "
                f"{float(sales_move.get('pct_change') or 0.0):.2f}% and profit "
                f"{float(profit_move.get('pct_change') or 0.0):.2f}% versus the previous available month."
            )

    lines.append("")
    lines.append("What to do next:")

    weak_cm2 = _first_product(rankings.get("weak_cm2_profit") or [])
    if weak_cm2:
        lines.append(
            "- Review "
            f"{weak_cm2.get('product_name') or weak_cm2.get('sku')} first because CM2 profit is "
            f"{_fmt_business_money(state, weak_cm2.get('cm2_profit'), 'cm2_profit')}."
        )

    inefficient_ad = _first_product(rankings.get("inefficient_ad_spend") or [])
    if inefficient_ad:
        lines.append(
            "- Tighten spend on "
            f"{inefficient_ad.get('product_name') or inefficient_ad.get('sku')} because it has "
            f"{_fmt_business_money(state, inefficient_ad.get('ads_spend'), 'ads_spend')} ad spend "
            f"and CM2 profit of {_fmt_business_money(state, inefficient_ad.get('cm2_profit'), 'cm2_profit')}."
        )

    top_cm2 = _first_product(rankings.get("top_cm2_profit") or [])
    if top_cm2:
        lines.append(
            "- Protect availability and visibility for "
            f"{top_cm2.get('product_name') or top_cm2.get('sku')}; it is one of the strongest CM2 contributors at "
            f"{_fmt_business_money(state, top_cm2.get('cm2_profit'), 'cm2_profit')}."
        )

    high_fee = _first_product(rankings.get("highest_fee_burden") or [])
    if high_fee:
        lines.append(
            "- Check fee drivers on "
            f"{high_fee.get('product_name') or high_fee.get('sku')} because its fee burden is "
            f"{float(high_fee.get('fee_ratio_pct') or 0.0):.2f}% of sales."
        )

    if len(lines) <= 9:
        lines.append("- Ask for a specific metric, product, period, or country if you want a deeper drill-down.")

    return "\n".join(lines)


def _summary_metric_parts(row: Dict[str, Any], metric_names: List[str]) -> List[str]:
    formatted = row.get("formatted_metrics") or {}
    parts: List[str] = []

    for metric_name in metric_names:
        if metric_name not in formatted:
            continue
        label = "Units" if metric_name == "total_quantity" else humanize_metric(metric_name)
        parts.append(f"{label} **{formatted[metric_name]}**")

    return parts


def _summary_metric_names_from_row(row: Dict[str, Any]) -> List[str]:
    preferred_order = [
        "net_sales",
        "profit",
        "total_cm2_profit",
        "total_ads",
        "total_quantity",
        "asp",
        "acos",
        "available",
        "inbound_quantity",
        "days_of_supply",
    ]
    available = list((row.get("formatted_metrics") or {}).keys())
    ordered = [metric for metric in preferred_order if metric in available]
    ordered.extend(metric for metric in available if metric not in ordered)
    return ordered


def _forecast_file_period_text(snapshot: Dict[str, Any]) -> str:
    month = snapshot.get("stored_month")
    year = snapshot.get("stored_year")
    if month and year:
        return f"{str(month).title()} {year}"
    return "latest available"


def _format_forecast_units_map(units_by_month: Dict[str, Any]) -> str:
    parts = []
    for label, value in (units_by_month or {}).items():
        try:
            parts.append(f"{label}: {float(value or 0.0):,.0f} units")
        except Exception:
            parts.append(f"{label}: {value}")
    return ", ".join(parts)


def _render_response(state: AgentState) -> AgentState:
    if state.get("intent") == "clarify":
        state["final_response"] = state.get("clarification_question") or "Could you clarify what you'd like me to analyze?"
        return state
    
    if state.get("intent") == "chat":
        if chat_llm:
            logger.info("[LLM_FALLBACK_TRIGGERED]")
            try:
                state["final_response"] = chat_llm.invoke(state.get("user_query", "")).content
                return state
            except Exception:
                logger.exception("Chat render failed")
        state["final_response"] = "Hi! Tell me which metric or report you want me to analyze."
        return state
    
    if state.get("intent") == "explain":
        if explain_llm:
            try:
                state["final_response"] = explain_llm.invoke(state.get("user_query", "")).content
                return state
            except Exception:
                logger.exception("Explain render failed")
        state["final_response"] = "Ask me about a metric like profit, sales, ACOS, or ASP and I’ll explain it."
        return state
    
    if state.get("event_plan_result"):
        summary = state["event_plan_result"].get("summary") or []
        actions = state["event_plan_result"].get("actions") or []
        raw_items = state["event_plan_result"].get("items") or []
        lines = ["Event plan generated."]
        if summary:
            lines.append("Summary:")
            lines.extend(f"- {x}" for x in summary[:8])
        if actions:
            lines.append("Actions:")
            lines.extend(f"- {x}" for x in actions[:8])
        if not summary and not actions and raw_items:
            lines.append("Planner details:")
            for item in raw_items[:3]:
                try:
                    lines.append(f"- {json.dumps(item, default=str)[:400]}")
                except Exception:
                    lines.append(f"- {str(item)[:400]}")
        state["final_response"] = "📩 I've sent the event plan email." if state.get("email_requested") else "\n".join(lines)
        return state
    
    if state.get("sku_intelligence_result"):
        res = state["sku_intelligence_result"]
        lines = [f"SKU intelligence for {res.get('product_match') or 'selected product'}:"]
        current = res.get("current", {})
        lines.append(f"- Current sales: {_format_value(float(current.get('net_sales', 0.0)), 'net_sales', state.get('country'))}")
        lines.append(f"- Current profit: {_format_value(float(current.get('profit', 0.0)), 'profit', state.get('country'))}")
        lines.append(f"- Current units: {float(current.get('total_quantity', 0.0)):,.0f}")
        lines.append(f"- Current ASP: {_format_value(float(current.get('asp', 0.0)), 'asp', state.get('country'))}")
        lines.append(f"- Current sales mix: {float(current.get('sales_mix', 0.0)):.2%}")
        lines.append(f"- Current profit mix: {float(current.get('profit_mix', 0.0)):.2%}")
        previous = res.get("previous") or {}
        if previous:
            lines.append(f"- Previous sales: {_format_value(float(previous.get('net_sales', 0.0)), 'net_sales', state.get('country'))}")
            lines.append(f"- Previous profit: {_format_value(float(previous.get('profit', 0.0)), 'profit', state.get('country'))}")
        for point in (res.get("summary_points") or [])[:5]:
            lines.append(f"- {point}")
        if state.get("advice"):
            lines.append("")
            lines.extend(f"- {a}" for a in state["advice"])
        state["final_response"] = "📩 I've sent the SKU intelligence email." if state.get("email_requested") else "\n".join(lines)
        return state
    
    # -------- 🔥 EMAIL RESPONSE HANDLING (FIXED) --------
    # Do NOT override final_response here
    # Let _send_email_if_requested handle confirmation after sending
    pass
    
    current = state.get("current_metrics") or {}
    metric_name = current.get("metric") or state.get("metric_name") or "metric"
    period_label = current.get("period_label") or "selected period"
    comp = state.get("comparison") or {}
    analysis = state.get("analysis_result") or {}

    if analysis.get("type") == "business_advisor":
        if advisor_llm:
            try:
                prompt = BUSINESS_ADVISOR_PROMPT + "\n\n" + json.dumps(
                    {
                        "question": analysis.get("question"),
                        "reasoning_mode": analysis.get("reasoning_mode"),
                        "task_type": analysis.get("task_type"),
                        "response_mode": state.get("response_mode"),
                        "metric_name": analysis.get("metric_name"),
                        "metric_names": analysis.get("metric_names"),
                        "product_query": analysis.get("product_query"),
                        "business_context": analysis.get("context"),
                    },
                    default=str,
                )
                state["final_response"] = advisor_llm.invoke(prompt).content
                return state
            except Exception:
                logger.exception("Business advisor render failed")

        state["final_response"] = _render_business_advisor_fallback(state, analysis)
        return state

    
    # -------- 🔥 INVENTORY DIAGNOSIS RENDER (UPGRADED) --------
    if analysis.get("type") == "inventory_comparison":
        metric_key = analysis.get("metric") or metric_name
        product_display = analysis.get("product_display") or _display_product_name(state.get("product_query") or "selected product")
        country_label = _country_display_name(analysis.get("country") or state.get("country"))
        left = analysis.get("left") or {}
        right = analysis.get("right") or {}
        left_value = left.get("total")
        right_value = right.get("total")

        if left_value is None or right_value is None:
            missing_labels = []
            if left_value is None:
                missing_labels.append(str(left.get("label") or "first period"))
            if right_value is None:
                missing_labels.append(str(right.get("label") or "second period"))
            state["final_response"] = (
                f"I could not compare **{product_display}** inventory in **{country_label}** because "
                f"data is missing for **{', '.join(missing_labels)}**."
            )
            return state

        delta = float(analysis.get("delta") or 0.0)
        pct_change = analysis.get("pct_change")
        direction = "increased" if delta > 0 else "decreased" if delta < 0 else "stayed the same"
        change_text = _format_inventory_value(abs(delta), metric_key)
        pct_text = "" if pct_change is None else f" ({abs(float(pct_change)):.2f}%)"
        lines = [
            f"**{product_display}** inventory in **{country_label}**:",
            f"- **{left.get('label')}**: **{_format_inventory_value(left_value, metric_key)}**",
            f"- **{right.get('label')}**: **{_format_inventory_value(right_value, metric_key)}**",
            f"- Change: **{direction} by {change_text}{pct_text}**",
        ]
        state["final_response"] = "\n".join(lines)
        return state

    if analysis.get("type") == "inventory_diagnosis":
        insights = analysis.get("insights", [])
        rows = analysis.get("rows", [])

        product_query = state.get("product_query")
        top_n = state.get("top_n")

        # If planner missed "top 3", recover it from user query.
        if not top_n:
            m = re.search(r"\btop\s+(\d+)\b", (state.get("user_query") or "").lower())
            if m:
                top_n = int(m.group(1))

        # 1) Specific product wins
        if product_query:
            pq = str(product_query).strip().lower()
            rows = [
                r for r in rows
                if pq in str(r.get("product_name") or "").strip().lower()
                or pq == str(r.get("sku") or "").strip().lower()
            ]

        # 2) Else top N if requested
        elif top_n:
            rows = rows[:int(top_n)]

        # 3) Else keep all rows

        metric_key = analysis.get("metric") or metric_name
        metric_label = humanize_metric(metric_key)
        inventory_period = analysis.get("period_label") or period_label
        inventory_country = _country_display_name(analysis.get("country") or state.get("country"))
        source_table = analysis.get("source_table")

        if product_query:
            if not rows:
                state["final_response"] = (
                    f"I could not find inventory data for **{_display_product_name(product_query)}** "
                    f"in **{inventory_country}** for **{inventory_period}**."
                )
                return state

            row = rows[0]
            product_name = row.get("product_name") or product_query
            sku = row.get("sku")
            if sku and str(product_name).strip().lower() != str(sku).strip().lower():
                product_display = f"{product_name} ({sku})"
            else:
                product_display = str(product_name)

            value = _format_inventory_value(row.get("__metric__", 0), metric_key)
            lines = [
                f"**{product_display}** inventory in **{inventory_country}** for **{inventory_period}**:",
                f"- **{metric_label}**: **{value}**",
            ]

            flags = analysis.get("flags", {})
            sku_map = analysis.get("sku_map", {})
            flag = flags.get(sku or sku_map.get(product_name), {})
            alert = flag.get("inventory_alert")
            recommendation = flag.get("inventory_recommendation")
            if source_table != "monthwise_inventory" and alert and alert != "No alert":
                lines.append(f"- Status: **{alert}**")
            if recommendation and alert and alert != "No alert" and source_table != "monthwise_inventory":
                lines.append(f"- Action: {recommendation}")

            state["final_response"] = "\n".join(lines)
            return state

        flags = analysis.get("flags", {})
        sku_map = analysis.get("sku_map", {})

        lines = [f"Inventory health check for **{inventory_country}** in **{inventory_period}**:\n"]

        # summary insights
        for i in insights:
            lines.append(f"- {i}")

        # detailed per SKU
        for r in rows:
            name = r.get("product_name")
            value = float(r.get("__metric__", 0))

            sku = sku_map.get(name)
            flag = flags.get(sku, {})

            logger.info(f"[DEBUG] name={name}, sku={sku}, flag={flag}")

            coverage = flag.get("inventory_coverage_ratio")
            recommendation = flag.get("inventory_recommendation")

            
            # -------- 🔥 CORRECT SEVERITY (USE FLAGS) --------
            alert = flag.get("inventory_alert")
            alert_type = flag.get("inventory_alert_type")

            if alert_type == "supply":
                if alert == "High alert":
                    emoji = "🚨"  # critical
                else:
                    emoji = "⚠"  # warning

            elif alert_type == "overaged":
                emoji = "📦"

            elif alert_type == "cost":
                emoji = "💸"

            else:
                continue  # skip only truly healthy SKUs

            lines.append(f"\n{emoji} {name}")

            if alert_type == "supply":
                if alert == "High alert":
                    lines.append("- Critical stock-out risk")
                else:
                    lines.append("- Low coverage (supply risk)")

            elif alert_type == "overaged":
                lines.append("- Long-term aged inventory")

            elif alert_type == "cost":
                lines.append("- High storage cost")

            if coverage is not None:
                lines.append(f"- Coverage: {round(coverage,1)} months")

            if recommendation:
                lines.append(f"→ {recommendation}")

        state["final_response"] = "\n".join(lines)
        return state


    if analysis.get("type") == "decision":
        if advisor_llm:
            try:
                prompt = REASONING_PROMPT + "\n\n" + json.dumps({
                    "question": analysis.get("question"),
                    "reasoning_mode": analysis.get("reasoning_mode"),
                    "task_type": analysis.get("task_type"),
                    "metric_name": analysis.get("metric_name"),
                    "metric_names": analysis.get("metric_names"),
                    "product_query": analysis.get("product_query"),
                    "product_match": analysis.get("product_match"),
                    "current_pack": analysis.get("current_pack"),
                    "history": analysis.get("history"),
                    "growth_driver": analysis.get("growth_driver"),
                }, default=str)
                state["final_response"] = advisor_llm.invoke(prompt).content
                return state
            except Exception:
                logger.exception("Decision render failed")
        state["final_response"] = "I could not build a reliable recommendation."
        return state
    if comp:
        pct = comp.get("pct_change")
        curr = float(comp.get("left", {}).get("total", 0.0))
        prev = float(comp.get("right", {}).get("total", 0.0))
        metric_label = humanize_metric(metric_name)
        if pct is None:
            msg = f"**{metric_label}** was **{_format_value(curr, metric_name, state.get('country'))}** vs **{_format_value(prev, metric_name, state.get('country'))}**."
        else:
            direction = "higher" if pct > 0 else "lower"
            msg = f"**{metric_label}** was **{_format_value(curr, metric_name, state.get('country'))}** vs **{_format_value(prev, metric_name, state.get('country'))}**, which is **{abs(pct):.2f}% {direction}**."
        if analysis.get("growth_driver"):
            gp = analysis["growth_driver"]
            primary = gp.get("primary_driver")
            if primary:
                msg += f" Primary driver: {primary}."
        if state.get("advice"):
            msg += "\n" + "\n".join(f"- {a}" for a in state["advice"])
        state["final_response"] = msg
        return state
    
    if analysis.get("type") in {"trend", "growth"}:
        series = analysis.get("series_display") or analysis.get("series", [])
        mom = analysis.get("mom_display") or analysis.get("mom", [])
        metric_label = humanize_metric(metric_name)
        if not series:
            state["final_response"] = "No trend data found."
            return state
        
        
        
        # -------- 🔥 SMART GROWTH RENDER --------
        if state.get("analysis_type") == "growth" and mom:
            logger.info("[RENDER] Rendering intelligent growth")

            latest = series[-1]["__metric__"]
            previous = series[-2]["__metric__"] if len(series) > 1 else 0

            change = latest - previous
            pct = (change / previous * 100) if previous else 0

            direction = "increased" if change >= 0 else "decreased"

            lines = []
            lines.append(f"Your **{metric_label}** is **{_format_value(latest, metric_name, state.get('country'))}**.")
            lines.append(
                f"It has **{direction}** by **{_format_value(abs(change), metric_name, state.get('country'))}** (**{pct:.2f}%**)."
            )

            # -------- 🔥 FIX 3: ROLLING MOM --------
            if mom:
                lines.append("\nMonth-on-month change:")
                for m in mom:
                    pct_m = m.get("pct_change", 0)
                    arrow = "↑" if pct_m > 0 else "↓" if pct_m < 0 else "→"
                    lines.append(f"- {m['period_label']}: {arrow} {abs(pct_m):.2f}%")

            
            # -------- 🔥 PRODUCT DRIVER ANALYSIS --------
            if not state.get("product_query"):  # ✅ ONLY for overall queries
                try:
                    latest_month = series[-1]
                    prev_month = series[-2] if len(series) > 1 else None

                    if prev_month:
                        latest_data = get_metric_for_month(
                            state["engine"],
                            state["user_id"],
                            state["country"],
                            metric_name,
                            latest_month["month"],
                            latest_month["year"]
                        )

                        prev_data = get_metric_for_month(
                            state["engine"],
                            state["user_id"],
                            state["country"],
                            metric_name,
                            prev_month["month"],
                            prev_month["year"]
                        )

                        latest_map = {
                            r.get("product_name"): float(r.get("__metric__", 0))
                            for r in latest_data.get("per_sku", [])
                        }
                        prev_map = {
                            r.get("product_name"): float(r.get("__metric__", 0))
                            for r in prev_data.get("per_sku", [])
                        }

                        growth_rows = []
                        all_products = set(latest_map) | set(prev_map)

                        for p in all_products:
                            curr = latest_map.get(p, 0.0)
                            prev = prev_map.get(p, 0.0)
                            delta = curr - prev

                            if abs(delta) > 0:
                                growth_rows.append({
                                    "product_name": p,
                                    "change": delta
                                })

                        growth_rows = sorted(growth_rows, key=lambda x: x["change"], reverse=True)

                        top_growth = growth_rows[:3]
                        top_decline = sorted(growth_rows, key=lambda x: x["change"])[:3]

                        if top_growth:
                            lines.append("\nTop growing products:")
                            for g in top_growth:
                                lines.append(
                                    f"- **{g['product_name']}** (+**{_format_value(g['change'], metric_name, state.get('country'))}**)"
                                )

                        if top_decline:
                            lines.append("\nTop declining products:")
                            for g in top_decline:
                                lines.append(
                                    f"- **{g['product_name']}** (**{_format_value(g['change'], metric_name, state.get('country'))}**)"
                                )

                except Exception:
                    logger.exception("[GROWTH_DRIVER_ERROR]")

            state["final_response"] = "\n".join(lines)
            return state
        
        
        latest = series[-1]
        prev = series[-2] if len(series) > 1 else None
        lines = [f"{metric_label} trend ({period_label})", f"Latest: **{_format_value(latest['__metric__'], metric_name, state.get('country'))}**"]
        if prev:
            delta = latest["__metric__"] - prev["__metric__"]
            pct = (delta / prev["__metric__"] * 100) if prev["__metric__"] else 0
            direction = "↑" if pct > 0 else "↓" if pct < 0 else "→"
            lines.append(f"Change vs previous month: **{direction} {abs(pct):.2f}%**")
        lines.append("")
        lines.append("Breakdown:")
        for row in series:
            label = row.get("period_label", "Unknown")
            val = float(row.get("__metric__", 0.0))
            lines.append(f"- {label}: **{_format_value(val, metric_name, state.get('country'))}**")
        state["final_response"] = "\n".join(lines)
        return state
    
    
    # -------- 🔥 HANDLE SKU BREAKDOWN + RANKING --------
    if analysis.get("type") in {"ranking", "breakdown"} and analysis.get("per_sku"):
        logger.info("[RENDER] Rendering SKU breakdown")

        rows = analysis.get("per_sku", [])
        direction = analysis.get("ranking_direction", "top")
        metric_label = humanize_metric(metric_name)

        # Sort descending safely
        rows = sorted(rows, key=lambda x: x.get("__metric__", 0), reverse=(direction == "top"))

        period_text = f" in {period_label}" if period_label else ""
        header = f"{'Top' if direction == 'top' else 'Bottom'} products for {metric_label}{period_text}:"

        context = analysis.get("context") or {}

        if context.get("type") == "ranked_extreme_contribution":
            selected_month = context.get("selected_month") or {}
            extreme_rank = context.get("extreme_rank") or 1
            base_metric = analysis.get("base_metric") or metric_name
            base_metric_label = humanize_metric(base_metric)

            header = (
                f"Top products for {metric_label} in "
                f"{_ordinal_label(extreme_rank)} highest {base_metric_label} month "
                f"({selected_month.get('period_label', period_label)}):"
            )
        else:
            header = f"{'Top' if direction == 'top' else 'Bottom'} products for {metric_label}{period_text}:"

        lines = [header, ""]

        for idx, row in enumerate(rows, 1):
            product_name = row.get("product_name")
            sku = row.get("sku")
            if product_name and sku and str(product_name).strip().lower() != str(sku).strip().lower():
                name = f"{product_name} ({sku})"
            else:
                name = product_name or sku or "Unknown"
            value = float(row.get("__metric__", 0.0))
            lines.append(f"{idx}. **{name}** - **{_format_value(value, metric_name, state.get('country'))}**")

        highest_month = analysis.get("highest_month") or {}
        if highest_month.get("period_label") and highest_month.get("value") is not None:
            lines.append("")
            lines.append(
                f"Highest **{metric_label}** month in this period: **{highest_month.get('period_label')}** - "
                f"**{_format_value(float(highest_month.get('value', 0.0)), metric_name, state.get('country'))}**"
            )

        total = analysis.get("total")
        if total is not None:
            lines.append("")
            lines.append(f"Total: **{_format_value(float(total), metric_name, state.get('country'))}**")

        state["final_response"] = "\n".join(lines)
        return state
    
    
    if analysis.get("type") == "extreme":
        label = analysis.get("period_label", "Unknown")
        value = float(analysis.get("value", 0.0))
        descriptor = "highest" if analysis.get("extreme_type", "max") == "max" else "lowest"
        metric_label = humanize_metric(metric_name)

        product = analysis.get("product")

        if product:
            state["final_response"] = (
                f"**{_display_product_name(product)}** had the {descriptor} **{metric_label}** in **{label}** "
                f"at **{_format_value(value, metric_name, state.get('country'))}**."
            )
        else:
            state["final_response"] = (
                f"The {descriptor} **{metric_label}** was in **{label}** "
                f"at **{_format_value(value, metric_name, state.get('country'))}**."
            )

        return state
    
    if analysis.get("type") == "forecast":
        requested_month = analysis.get("requested_month")
        requested_year = analysis.get("requested_year")
        title_period = analysis.get("period_label") or period_label
        lines = [f"**Forecast summary for {title_period}**"]

        for result in analysis.get("results", []):
            country = result.get("country")
            country_label = result.get("country_label") or _country_display_name(country)
            inventory = result.get("inventory") or {}
            pnl = result.get("pnl") or {}

            country_parts = []
            if inventory.get("available"):
                units_text = _format_forecast_units_map((inventory.get("totals") or {}).get("forecast_units") or {})
                projected_total = (inventory.get("totals") or {}).get("projected_sales_total")
                if units_text:
                    country_parts.append(f"forecast units **{units_text}**")
                if projected_total:
                    country_parts.append(f"projected total **{float(projected_total):,.0f} units**")

            show_pnl = bool(
                pnl.get("available")
                and (pnl.get("exact_period_match", True) or not requested_month or not requested_year)
            )
            if show_pnl:
                totals = pnl.get("totals") or {}
                country_parts.extend([
                    f"sales **{_format_forecast_money(totals.get('forecast_sales'), country, 'net_sales')}**",
                    f"profit **{_format_forecast_money(totals.get('forecast_profit'), country, 'profit')}**",
                    f"CM2 **{_format_forecast_money(totals.get('forecast_cm2_profit'), country, 'total_cm2_profit')}**",
                    f"ad spend **{_format_forecast_money(totals.get('forecast_ad_spend'), country, 'total_ads')}**",
                ])

            if country_parts:
                lines.append(f"- **{country_label}**: {', '.join(country_parts)}.")
            else:
                lines.append(f"- **{country_label}**: no forecast data found.")

            if inventory.get("available") and not inventory.get("exact_period_match", True):
                lines.append(f"  Latest inventory forecast available: **{_forecast_file_period_text(inventory)}**.")
            if pnl.get("available") and not show_pnl:
                lines.append(f"  P&L forecast for requested month is not available; latest stored P&L forecast is **{_forecast_file_period_text(pnl)}**.")

            top_row = None
            top_source = result.get("top_pnl") or []
            if show_pnl and top_source:
                top_row = top_source[0]
                lines.append(
                    f"  Top forecasted product: **{_format_product_with_sku(top_row.get('product_name'), top_row.get('sku'), top_row.get('sku'))}** "
                    f"({_format_forecast_money(top_row.get('forecast_sales'), country, 'net_sales')} sales)."
                )
            elif result.get("top_inventory"):
                top_row = result["top_inventory"][0]
                lines.append(
                    f"  Top forecasted product: **{_format_product_with_sku(top_row.get('product_name'), top_row.get('sku'), top_row.get('sku'))}** "
                    f"({float(top_row.get('projected_sales_total') or 0.0):,.0f} units)."
                )

        state["final_response"] = "\n".join(lines)
        return state

    if analysis.get("type") == "multi_country_summary":
        rows = analysis.get("results") or []
        metric_names = analysis.get("metrics") or []
        period_labels = [row.get("period_label") for row in rows if row.get("period_label")]
        unique_periods = list(dict.fromkeys(period_labels))
        title_period = unique_periods[0] if len(unique_periods) == 1 else "Selected period"
        lines = [f"**{title_period} summary**"]

        for row in rows:
            country_label = row.get("country_label") or _country_display_name(row.get("country"))
            parts = _summary_metric_parts(row, metric_names or _summary_metric_names_from_row(row))
            if parts:
                lines.append(f"- **{country_label}**: {', '.join(parts)}.")
            else:
                lines.append(f"- **{country_label}**: no data available for the requested period.")

        top_products = [
            (row.get("country_label") or _country_display_name(row.get("country")), row.get("top_product"))
            for row in rows
            if row.get("top_product")
        ]
        if top_products:
            lines.append("")
            for country_label, top_product in top_products:
                lines.append(
                    f"- Top product in **{country_label}**: "
                    f"**{top_product.get('product')}** ({top_product.get('formatted')})."
                )

        state["final_response"] = "\n".join(lines)
        return state

    if analysis.get("type") == "multi_country":
        mode = analysis.get("mode") or "single"
        product = analysis.get("product")
        metrics = analysis.get("metrics") or [state.get("metric_name") or "metric"]

        if mode == "trend":
            title = "Multi-country trend comparison"
            if product:
                title += f" for {product}"
            lines = [title + ":"]

            winners = {
                row.get("metric"): row
                for row in analysis.get("winners", [])
                if row.get("metric")
            }

            for metric in metrics:
                lines.append("")
                lines.append(f"{humanize_metric(metric)}:")
                metric_rows = [
                    row for row in analysis.get("results", [])
                    if row.get("metric") == metric
                ]

                for row in metric_rows:
                    country = str(row.get("country") or "").upper()
                    latest_change = row.get("latest_change") or {}
                    series = [item for item in row.get("series", []) if item.get("value") is not None]
                    error = row.get("error")

                    if latest_change:
                        lines.append(
                            f"- **{country}**: **{latest_change.get('previous_formatted')}** -> "
                            f"**{latest_change.get('current_formatted')}** "
                            f"(**{latest_change.get('delta_formatted')}**, "
                            f"**{(latest_change.get('pct_change') or 0):.2f}% change**)"
                        )
                        if len(series) > 2:
                            trend_points = ", ".join(
                                f"{item.get('period_label')}: {item.get('formatted')}"
                                for item in series[-6:]
                            )
                            lines.append(f"  Trend: {trend_points}")
                    elif series:
                        latest = series[-1]
                        lines.append(
                            f"- **{country}**: **{latest.get('formatted')}** in **{latest.get('period_label')}** "
                            "(not enough previous-period data for MoM change)"
                        )
                    elif error:
                        lines.append(f"- {country}: data unavailable ({error})")
                    else:
                        lines.append(f"- {country}: data unavailable")

                winner = winners.get(metric)
                if winner:
                    change = winner.get("latest_change") or {}
                    reason = winner.get("reason") or "best latest movement"
                    lines.append(
                        f"Best performing country for {humanize_metric(metric)}: "
                        f"**{str(winner.get('country')).upper()}** "
                        f"(**{change.get('delta_formatted')}**, {reason})."
                    )

            state["final_response"] = "\n".join(lines)
            return state

        lines = ["Multi-country comparison:"]
        for metric in metrics:
            lines.append("")
            lines.append(f"{humanize_metric(metric)}:")
            metric_rows = [
                row for row in analysis.get("results", [])
                if row.get("metric") == metric
            ]
            metric_rows = sorted(metric_rows, key=lambda row: float(row.get("value", 0.0)), reverse=metric not in LOWER_IS_BETTER_METRICS)
            for row in metric_rows:
                country = str(row.get("country") or "").upper()
                product_label = f" | **{row.get('product')}**" if row.get("product") else ""
                lines.append(
                    f"- **{country}**{product_label}: **{row.get('formatted')}** "
                    f"(**{row.get('period_label')}**)"
                )
            if metric_rows:
                winner = metric_rows[0]
                lines.append(f"Best performing country for {humanize_metric(metric)}: **{str(winner.get('country')).upper()}**.")

        state["final_response"] = "\n".join(lines)
        return state

    if analysis.get("type") == "multi_month":
        lines = []
        country_label = _country_display_name(state.get("country"))
        requested_product = state.get("product_query")
        if not requested_product and state.get("product_queries"):
            requested_product = ", ".join(_display_product_name(p) for p in state.get("product_queries") or [])

        for metric_block in analysis.get("results", []):
            metric = metric_block.get("metric")
            metric_label = humanize_metric(metric)
            months_data = metric_block.get("months", [])
            period_labels = [row.get("period_label") for row in months_data if row.get("period_label")]
            period_text = (
                period_labels[0]
                if len(period_labels) == 1
                else f"{period_labels[0]} to {period_labels[-1]}"
                if period_labels
                else period_label
            )
            metric_total = 0.0
            product_totals: Dict[str, float] = {}

            if lines:
                lines.append("")

            title = metric_label
            if requested_product:
                title = f"{metric_label} for {_display_product_name(requested_product)}"

            lines.append(f"{title}:")
            lines.append(f"Country: **{country_label}**")
            lines.append(f"Period: **{period_text}**")
            lines.append("")
            lines.append("Monthly breakdown:")

            for row in months_data:
                label = row.get("period_label", "Unknown")
                value = float(row.get("total", 0.0))
                if row.get("product_breakdown"):
                    breakdown_items = list(row["product_breakdown"].items())
                    for product, product_value in breakdown_items:
                        numeric_value = float(product_value or 0.0)
                        product_name = _display_product_name(product)
                        product_totals[product_name] = product_totals.get(product_name, 0.0) + numeric_value
                        if len(breakdown_items) == 1:
                            lines.append(f"- {label}: **{_format_value(numeric_value, metric, state.get('country'))}**")
                        else:
                            lines.append(f"- {label} | **{product_name}**: **{_format_value(numeric_value, metric, state.get('country'))}**")
                else:
                    metric_total += value
                    lines.append(f"- {label}: **{_format_value(value, metric, state.get('country'))}**")

            if product_totals:
                lines.append("")
                lines.append("Total:")
                for product, total in product_totals.items():
                    lines.append(f"- **{product}**: **{_format_value(total, metric, state.get('country'))}**")
            else:
                lines.append("")
                lines.append(f"Total: **{_format_value(metric_total, metric, state.get('country'))}**")
        state["final_response"] = "\n".join(lines)
        return state
    
    if analysis.get("type") == "multi_dimensional":
        rows = analysis.get("data", [])
        metrics = analysis.get("metrics", [])
        products = analysis.get("products") or []
        lines = ["Comparison summary:"]
        if len(metrics) == 1 and products:
            metric = metrics[0]
            totals: Dict[str, float] = {}
            for row in rows:
                totals[row["product"]] = totals.get(row["product"], 0.0) + float(row["value"])
            for product, total in sorted(totals.items(), key=lambda x: x[1], reverse=True):
                lines.append(f"- **{product}**: **{_format_value(total, metric, state.get('country'))}**")
            lines.append("")
            lines.append("Breakdown:")
            for row in rows[:50]:
                lines.append(f"- {row['month']} | **{row['product']}**: **{_format_value(row['value'], row['metric'], state.get('country'))}**")
        else:
            for row in rows[:50]:
                lines.append(f"- {row['month']} | **{row['product']}** | **{humanize_metric(row['metric'])}**: **{_format_value(row['value'], row['metric'], state.get('country'))}**")
        state["final_response"] = "\n".join(lines)
        return state
    
    if analysis.get("type") == "summary":
        row = (analysis.get("rows") or [{}])[0]
        country_label = row.get("country_label") or analysis.get("country_label") or _country_display_name(state.get("country"))
        summary_period = row.get("period_label") or analysis.get("period_label") or period_label
        metric_names = _summary_metric_names_from_row(row)
        parts = _summary_metric_parts(row, metric_names)
        lines = [f"**{country_label} {summary_period} summary**"]

        if parts:
            lines.append(f"- {', '.join(parts)}.")
        else:
            lines.append("- No data available for the requested period.")

        top_product = row.get("top_product") or analysis.get("top_product")
        if top_product:
            lines.append(
                f"- Top product: **{top_product.get('product')}** "
                f"({top_product.get('formatted')})."
            )

        state["final_response"] = "\n".join(lines)
        return state

    if False and analysis.get("type") == "summary":
        metrics = analysis.get("metrics", {})

        # -------- BASE METRICS --------
        lines = [
            f"Business Report for {period_label}",
            "",
            f"Revenue: **{_format_value(metrics.get('net_sales', 0), 'net_sales', state.get('country'))}**",
            f"Profit: **{_format_value(metrics.get('profit', 0), 'profit', state.get('country'))}**",
            f"Units: **{metrics.get('total_quantity', 0):,.0f}**",
            f"ASP: **{_format_value(metrics.get('asp', 0), 'asp', state.get('country'))}**",
            f"ACOS: **{metrics.get('acos', 0):.2f}%**",
            f"Ad Spend: **{_format_value(metrics.get('total_ads', 0), 'total_ads', state.get('country'))}**",
            f"Platform Fees: **{_format_value(metrics.get('platform_fee', 0), 'platform_fee', state.get('country'))}**",
            f"CM2 Profit: **{_format_value(metrics.get('total_cm2_profit', 0), 'total_cm2_profit', state.get('country'))}**",
            f"Reimbursements (Amazon): **{_format_value(metrics.get('rembursement_fee', 0), 'rembursement_fee', state.get('country'))}**",
        ]

        # -------- TOP PRODUCTS --------
        top_products = analysis.get("top_products", [])[:5]
        if top_products:
            lines.append("\nTop products:")
            for row in top_products:
                name = row.get("product_name") or row.get("sku") or "Unknown"
                lines.append(
                    f"- **{name}**: **{_format_value(float(row.get('__metric__', 0.0)), 'net_sales', state.get('country'))}**"
                )

        # -------- 🔥 AI INSIGHTS (YOUR NEW LAYER) --------
        try:
            insights = _generate_business_insights(state)

            state["insights"] = insights

            if insights:
                lines.append("\n--- Insights ---\n")
                lines.append(insights)
        except Exception:
            logger.exception("[BUSINESS_INSIGHTS_ERROR]")

        state["final_response"] = "\n".join(lines)
        return state
    
    if analysis.get("type") == "sku_trend":
        results = analysis.get("results", [])
        if not results:
            state["final_response"] = f"No consistently declining SKUs found for {metric_name}."
            return state
        lines = [f"Products with consistently declining {metric_name} trend:"]
        for item in results[:10]:
            lines.append(f"- {item.get('product')}")
        state["final_response"] = "\n".join(lines)
        return state
    total = current.get("total")
    if total is not None:
        product_label = ""
        if state.get("product_query"):
            product_label = f" for **{_display_product_name(state.get('product_query'))}**"
        state["final_response"] = (
            f"In **{period_label}**, **{humanize_metric(metric_name)}**{product_label} "
            f"for **{_country_display_name(state.get('country'))}** was "
            f"**{_format_value(float(total), metric_name, state.get('country'))}**."
        )
        return state


    # -------- 🔥 LLM FALLBACK (ADD THIS BLOCK) --------
    if chat_llm:
        try:
            prompt = f"""
    You are an Amazon business analyst AI.

    User question:
    {state.get("user_query")}

    Available context (may be incomplete):
    {json.dumps({
        "current_metrics": state.get("current_metrics"),
        "analysis_result": state.get("analysis_result")
    }, default=str)}

    Instructions:
    - Try to answer helpfully even if data is missing
    - Do NOT hallucinate exact numbers
    - Give reasoning, possible causes, or guidance
    - Use Markdown bold sparingly for only the most important product, metric, value, or action
    """

            response = chat_llm.invoke(prompt)
            state["final_response"] = response.content
            return state

        except Exception:
            logger.exception("[LLM_FALLBACK_FAILED]")


    # -------- SAFE FINAL FALLBACK --------
    state["final_response"] = "I couldn’t fully analyze this. Try asking with a metric like sales, profit, or units."
    return state

def _restore_memory_email(state: AgentState, plan: RequestPlan, history: List[Dict[str, Any]]) -> Optional[AgentState]:
    last_meta = load_last_analysis_from_history(history)

    # -------- SMART RESTORE CONDITION (FINAL FIX) --------
    query = (state.get("user_query") or "").lower()

    has_new_info = any([
        plan.metric_name,
        plan.product_query,
        state.get("period_parsed") and state.get("period_parsed").get("type") != "latest_month",
        "report" in query,
    ])

    if not (
        last_meta
        and plan.intent == "email"
        and not has_new_info
    ):
        return None

    # -------- 🔥 DEBUG LOG (STEP 2) --------
    logger.info("[MEMORY_RESTORE] Condition passed → restoring previous result")

    # -------- RESTORE STATE --------
    state["intent"] = "email"
    state["email_requested"] = True
    state["restored_from_memory"] = True

    state["metric_name"] = last_meta.get("metric_name")
    state["period_parsed"] = last_meta.get("period_parsed") or {"type": "latest_month"}
    state["analysis_type"] = last_meta.get("analysis_type") or "absolute"

    state["period_payload"] = _prepare_period_payload(
        state["period_parsed"],
        state["analysis_type"]
    )

    state["current_metrics"] = last_meta.get("current_metrics") or {}
    state["comparison"] = last_meta.get("comparison") or {}
    state["analysis_result"] = last_meta.get("analysis_result") or {}
    state["event_plan_result"] = last_meta.get("event_plan_result") or {}
    state["sku_intelligence_result"] = last_meta.get("sku_intelligence_result") or {}
    state["advice"] = last_meta.get("advice") or []

    state["engine"] = get_engine()

    logger.info(
        f"[MEMORY_RESTORE] Restored metric={state['metric_name']}, "
        f"type={state['analysis_type']}, period={state['period_parsed']}"
    )

    # -------- RENDER RESPONSE --------
    state = _render_response(state)

    # -------- SEND EMAIL --------
    state = _send_email_if_requested(state)

    return state

FOLLOWUP_RESOLUTION_PROMPT = """
You are resolving whether the user's latest question is a follow-up to the previous analytics answer.

You will receive:
1. Latest user query
2. Previous analysis metadata

Decide if the latest query depends on the previous answer's time period or context.

Rules:
- If the user asks a continuation question like asking for drivers, SKUs, products, contributors, reasons, breakdown, or details of the previous result, mark is_followup=true.
- If the latest query does not mention a clear new time period, and it depends on the previous result, set reuse_previous_period=true.
- Do NOT rely on exact wording. Infer intent semantically.
- If the user asks which SKU/product contributed most to sales, use:
  metric_name = net_sales
  analysis_type = absolute
  answer_shape = ranking
  ranking_direction = top
  dimension = sku
  top_n = 1
- If the user asks for top/bottom products, use answer_shape=ranking.
- If the user asks for a breakdown, use analysis_type=breakdown.
- If unsure, keep fields null rather than guessing.

Return only structured fields.
"""


def _resolve_followup_with_llm(
    state: AgentState,
    history: List[Dict[str, Any]]
) -> Optional[FollowupResolutionModel]:
    if not planner_llm:
        return None

    try:
        last_meta = load_last_analysis_from_history(history)
    except Exception:
        logger.exception("[FOLLOWUP_RESOLUTION] Failed to load last analysis")
        return None

    if not last_meta:
        logger.info("[FOLLOWUP_RESOLUTION] No previous analysis found")
        return None

    try:
        resolver = planner_llm.with_structured_output(FollowupResolutionModel)

        payload = {
            "latest_user_query": state.get("user_query"),
            "previous_current_metrics": last_meta.get("current_metrics"),
            "previous_analysis_result": last_meta.get("analysis_result"),
            "previous_metric_name": last_meta.get("metric_name"),
            "previous_period_parsed": last_meta.get("period_parsed"),
        }

        result = resolver.invoke(
            FOLLOWUP_RESOLUTION_PROMPT
            + "\n\nInput:\n"
            + json.dumps(payload, default=str)
        )

        logger.info(
            f"[FOLLOWUP_RESOLUTION] "
            f"is_followup={result.is_followup}, "
            f"reuse_previous_period={result.reuse_previous_period}, "
            f"metric={result.metric_name}, "
            f"analysis_type={result.analysis_type}, "
            f"answer_shape={result.answer_shape}, "
            f"reason={result.reason}"
        )

        return result

    except Exception:
        logger.exception("[FOLLOWUP_RESOLUTION] LLM resolver failed")
        return None


def _previous_single_period_from_history(
    history: List[Dict[str, Any]]
) -> Optional[Dict[str, Any]]:
    try:
        last_meta = load_last_analysis_from_history(history)
    except Exception:
        logger.exception("[FOLLOWUP_PERIOD] Failed to load last analysis")
        return None

    if not last_meta:
        return None

    current = last_meta.get("current_metrics") or {}
    analysis = last_meta.get("analysis_result") or {}
    parsed = last_meta.get("period_parsed") or {}

    month = (
        current.get("month")
        or analysis.get("month")
        or parsed.get("month")
    )

    year = (
        current.get("year")
        or analysis.get("year")
        or parsed.get("year")
    )

    if not month or not year:
        logger.info("[FOLLOWUP_PERIOD] No single month/year found in previous result")
        return None

    try:
        return {
            "type": "single",
            "month": int(month),
            "year": int(year),
        }
    except Exception:
        logger.exception("[FOLLOWUP_PERIOD] Invalid previous month/year")
        return None


COMPOSITE_RESOLUTION_PROMPT = """
You are detecting multi-step analytics questions.

A multi-step analytics question asks for one calculation first, then asks for a product/SKU ranking, breakdown, or contribution inside that result.

Examples:
- Which product contributed most to the second highest profit month?
- Which SKU drove the lowest sales month?
- Top product in the highest profit month.
- Which product contributed most during the best sales month?
- Which SKU contributed most in the worst profit month?

If the query asks for:
1. a highest/lowest/best/worst/second highest/third highest month or period
AND
2. product/SKU contribution/ranking/breakdown inside that selected month or period

then set is_composite=true.

Rules:
- base_metric_name is the metric used to select the month or period.
- contribution_metric_name is the metric used to rank products/SKUs inside that month.
- Usually contribution_metric_name is the same as base_metric_name.
- If the user asks for profit contribution, use profit.
- If the user asks for sales/revenue contribution, use net_sales.
- extreme_type should be max for highest/best/top, min for lowest/worst/bottom.
- extreme_rank should be 1 for highest/best, 2 for second highest/second best, 3 for third highest/third best.
- dimension should be sku when the user asks for product/SKU contribution.
- top_n should be 1 when the user asks "which product", "which SKU", or "most".
- If it is not a multi-step question, set is_composite=false.

Return only structured fields.
"""


def _resolve_composite_with_llm(state: AgentState) -> Optional[CompositeResolutionModel]:
    if not planner_llm:
        return None

    try:
        resolver = planner_llm.with_structured_output(CompositeResolutionModel)

        payload = {
            "latest_user_query": state.get("user_query"),
            "planner_state": {
                "metric_name": state.get("metric_name"),
                "metric_names": state.get("metric_names"),
                "analysis_type": state.get("analysis_type"),
                "answer_shape": state.get("answer_shape"),
                "dimension": state.get("dimension"),
                "ranking_direction": state.get("ranking_direction"),
                "extreme_type": state.get("extreme_type"),
            },
        }

        result = resolver.invoke(
            COMPOSITE_RESOLUTION_PROMPT
            + "\n\nInput:\n"
            + json.dumps(payload, default=str)
        )

        logger.info(
            f"[COMPOSITE_RESOLUTION] "
            f"is_composite={result.is_composite}, "
            f"base_metric={result.base_metric_name}, "
            f"contribution_metric={result.contribution_metric_name}, "
            f"extreme_type={result.extreme_type}, "
            f"extreme_rank={result.extreme_rank}, "
            f"dimension={result.dimension}, "
            f"top_n={result.top_n}, "
            f"reason={result.reason}"
        )

        return result

    except Exception:
        logger.exception("[COMPOSITE_RESOLUTION] LLM resolver failed")
        return None



def _invoke_agent(state: AgentState) -> AgentState:
    try:
        query = state.get("user_query", "")
        history = state.get("chat_history", [])

        logger.info(f"[START] Query: {query}")

        # -------- 🔥 SMART INTENT + METRIC NORMALIZATION --------
        q_lower = query.lower()

        target_countries = _extract_requested_countries(q_lower)

        # ✅ Single-country query: "in US", "in UK"
        if len(target_countries) == 1:
            state["country"] = target_countries[0]
            logger.info(f"[COUNTRY_OVERRIDE] country={state['country']}")

        # ✅ Multi-country query: "US and UK", "both countries"
        elif len(target_countries) > 1:
            state["target_countries"] = target_countries
            logger.info(f"[MULTI_COUNTRY] countries={target_countries}")

        elif any(x in q_lower for x in ["which country", "both countries", "compare them", "performing better", "performing well"]):
            try:
                last_meta = load_last_analysis_from_history(history)
                previous_countries = last_meta.get("target_countries") if last_meta else None
                if previous_countries and len(previous_countries) > 1:
                    state["target_countries"] = previous_countries
                    logger.info(f"[MULTI_COUNTRY_FOLLOWUP] countries={previous_countries}")
            except Exception:
                logger.debug("[MULTI_COUNTRY_FOLLOWUP] No previous country scope", exc_info=True)

        # -------- CSV / DOWNLOAD DETECTION --------
        if any(word in q_lower for word in ["csv", "download", "excel", "sheet"]):
            logger.info("[EXPORT] CSV/Download requested")
            state["include_csv"] = True

        # SMART BREAKDOWN DETECTION
        has_breakdown = "breakdown" in q_lower
        has_product = any(w in q_lower for w in ["productwise", "by product", "per product", "sku"])
        has_time = any(w in q_lower for w in ["last", "months", "month", "year", "over time", "trend"])

        if has_breakdown:
            if has_product:
                logger.info("[SMART_INTENT] breakdown → SKU")
                state["analysis_type"] = "breakdown"
                state["dimension"] = "sku"
            elif has_time:
                logger.info("[SMART_INTENT] breakdown → TREND")
                state["analysis_type"] = "trend"
            else:
                logger.info("[SMART_INTENT] breakdown → default SKU")
                state["analysis_type"] = "breakdown"

        # METRIC NORMALIZATION
        is_productwise_query = any(
            x in q_lower
            for x in [
                "productwise",
                "product wise",
                "by product",
                "per product",
                "sku wise",
                "sku-wise",
                "by sku",
                "per sku",
                "breakdown",
            ]
        )

        if "cm2" in q_lower:
            if is_productwise_query:
                logger.info("[METRIC_FIX] productwise cm2 → cm2_profit")
                state["metric_name"] = "cm2_profit"
                state["dimension"] = "sku"
                state["analysis_type"] = state.get("analysis_type") or "breakdown"
            else:
                logger.info("[METRIC_FIX] total cm2 → total_cm2_profit")
                state["metric_name"] = "total_cm2_profit"

        elif any(x in q_lower for x in ["sponsored product", "sponsor product", "sp ads", "sp spend", "product ads spend", "product ad spend"]):
            logger.info("[METRIC_FIX] sponsored product spend → product_spend")
            state["metric_name"] = "product_spend"
            if is_productwise_query:
                state["dimension"] = "sku"
                state["analysis_type"] = state.get("analysis_type") or "breakdown"

        elif any(x in q_lower for x in ["sponsored brand", "sponsor brand", "sb ads", "sb spend", "brand ads spend", "brand ad spend"]):
            logger.info("[METRIC_FIX] sponsored brand spend → brand_spend")
            state["metric_name"] = "brand_spend"
            if is_productwise_query:
                state["dimension"] = "sku"
                state["analysis_type"] = state.get("analysis_type") or "breakdown"

        elif any(x in q_lower for x in ["sponsored display", "sponsor display", "sd ads", "sd spend", "display ads spend", "display ad spend"]):
            logger.info("[METRIC_FIX] sponsored display spend → display_spend")
            state["metric_name"] = "display_spend"
            if is_productwise_query:
                state["dimension"] = "sku"
                state["analysis_type"] = state.get("analysis_type") or "breakdown"

        elif any(x in q_lower for x in ["sp ads sales", "sp sales", "sponsored product sales", "sponsor product sales", "product ads sales"]):
            logger.info("[METRIC_FIX] sponsored product sales → sp_ads_sales")
            state["metric_name"] = "sp_ads_sales"
            if is_productwise_query:
                state["dimension"] = "sku"
                state["analysis_type"] = state.get("analysis_type") or "breakdown"

        elif any(x in q_lower for x in ["sb ads sales", "sb sales", "sponsored brand sales", "sponsor brand sales", "brand ads sales"]):
            logger.info("[METRIC_FIX] sponsored brand sales → sb_ads_sales")
            state["metric_name"] = "sb_ads_sales"
            if is_productwise_query:
                state["dimension"] = "sku"
                state["analysis_type"] = state.get("analysis_type") or "breakdown"

        elif any(x in q_lower for x in ["sd ads sales", "sd sales", "sponsored display sales", "sponsor display sales", "display ads sales"]):
            logger.info("[METRIC_FIX] sponsored display sales → sd_ads_sales")
            state["metric_name"] = "sd_ads_sales"
            if is_productwise_query:
                state["dimension"] = "sku"
                state["analysis_type"] = state.get("analysis_type") or "breakdown"

        elif any(x in q_lower for x in ["ads", "ad spend", "advertising"]):
            if is_productwise_query:
                logger.info("[METRIC_FIX] productwise ads → ads_spend")
                state["metric_name"] = "ads_spend"
                state["dimension"] = "sku"
                state["analysis_type"] = state.get("analysis_type") or "breakdown"
            else:
                logger.info("[METRIC_FIX] total ads → total_ads")
                state["metric_name"] = "total_ads"

        elif "acos" in q_lower:
            logger.info("[METRIC_FIX] acos detected")
            state["metric_name"] = "acos"

        elif "asp" in q_lower:
            logger.info("[METRIC_FIX] asp detected")
            state["metric_name"] = "asp"

        # -------- PLAN --------
        plan = _plan_request(q_lower, email_requested=bool(state.get("email_requested")))

        logger.info(
            f"[PLAN] intent={plan.intent}, analysis_type={plan.analysis_type}, "
            f"metric={plan.metric_name}, metric_names={plan.metric_names}, "
            f"product_query={plan.product_query}, product_queries={plan.product_queries}, "
            f"answer_shape={plan.answer_shape}, reasoning_mode={plan.reasoning_mode}, "
            f"needs_forecast_data={plan.needs_forecast_data}"
        )
        # -------- 🔥 LLM-BASED FOLLOW-UP RESOLUTION --------
        followup_resolution = _resolve_followup_with_llm(state, history)
        composite_resolution = _resolve_composite_with_llm(state)

        # -------- MEMORY RESTORE --------
        restored = _restore_memory_email(state, plan, history)

        if restored is not None:
            logger.info("[MEMORY] Restored previous analysis from history")

            # 🔥 FORCE email intent
            restored["email_requested"] = True

            # 🔥 SKIP EVERYTHING → DIRECT EMAIL
            return restored

        # -------- STATE SETUP --------
        state["intent"] = plan.intent

        if not state.get("analysis_type"):
            state["analysis_type"] = plan.analysis_type
        else:
            logger.info(f"[ANALYSIS_LOCKED] {state['analysis_type']}")

        state["dimension"] = plan.dimension
        state["reasoning_mode"] = plan.reasoning_mode or "lookup"
        state["task_type"] = plan.task_type or "value_lookup"

        if not state.get("metric_name"):
            state["metric_name"] = plan.metric_name
        else:
            logger.info(f"[METRIC_LOCKED] {state['metric_name']}")

        state["product_query"] = plan.product_query
        state["metric_names"] = plan.metric_names
        state["product_queries"] = plan.product_queries
        state["needs_advice"] = plan.needs_advice
        state["needs_forecast_data"] = plan.needs_forecast_data
        state["clarification_question"] = plan.clarification_question
        state["response_mode"] = plan.response_mode
        state["email_requested"] = bool(state.get("email_requested") or plan.intent == "email")
        state["answer_shape"] = plan.answer_shape
        state["subject_scope"] = plan.subject_scope
        state["ranking_direction"] = plan.ranking_direction
        state["extreme_type"] = plan.extreme_type
        state["top_n"] = plan.top_n
        state["time_granularity"] = plan.time_granularity
        state["target_months"] = plan.target_months
        state["event_name"] = plan.event_name
        state["last_event_month"] = plan.last_event_month
        state["future_event_month"] = plan.future_event_month
        state["target_sales"] = plan.target_sales

        # -------- 🔥 APPLY LLM FOLLOW-UP OVERRIDES --------
        if followup_resolution and followup_resolution.is_followup:
            logger.info("[FOLLOWUP_APPLY] Applying follow-up resolution")

            if _requires_business_advisor(state):
                logger.info("[FOLLOWUP_APPLY] Preserving business advisor routing")
                if followup_resolution.top_n:
                    state["top_n"] = followup_resolution.top_n
            else:
                if followup_resolution.metric_name:
                    state["metric_name"] = followup_resolution.metric_name
                    state["metric_names"] = [followup_resolution.metric_name]

                if followup_resolution.analysis_type:
                    state["analysis_type"] = followup_resolution.analysis_type

                if followup_resolution.answer_shape:
                    state["answer_shape"] = followup_resolution.answer_shape

                if followup_resolution.ranking_direction:
                    state["ranking_direction"] = followup_resolution.ranking_direction

                if followup_resolution.dimension:
                    state["dimension"] = followup_resolution.dimension

                if followup_resolution.top_n:
                    state["top_n"] = followup_resolution.top_n

        # -------- 🔥 APPLY COMPOSITE OVERRIDES --------
        if composite_resolution and composite_resolution.is_composite:
            logger.info("[COMPOSITE_APPLY] Applying composite resolution")

            state["analysis_type"] = "ranked_extreme_contribution"

            state["base_metric_name"] = (
                composite_resolution.base_metric_name
                or state.get("metric_name")
                or "profit"
            )

            state["metric_name"] = (
                composite_resolution.contribution_metric_name
                or composite_resolution.base_metric_name
                or state.get("metric_name")
                or "profit"
            )

            state["metric_names"] = [state["metric_name"]]
            state["answer_shape"] = "ranking"
            state["ranking_direction"] = "top"
            state["dimension"] = composite_resolution.dimension or "sku"
            state["subject_scope"] = "product"

            state["extreme_type"] = composite_resolution.extreme_type or "max"
            state["extreme_rank"] = composite_resolution.extreme_rank or 1
            state["top_n"] = composite_resolution.top_n or 1

        if state["intent"] in {"chat", "explain", "clarify"}:
            logger.info(f"[SHORT-CIRCUIT] intent={state['intent']}")
            return _render_response(state)

        # -------- 🔥 METRIC RECOVERY (CRITICAL FIX) --------
        if not state.get("metric_name") and not state.get("metric_names"):

            # 🔥 STEP 1: Try alias-based resolution
            resolved_metric = _metric_from_query(q_lower)

            if resolved_metric:
                logger.info(f"[METRIC_RECOVERED] {resolved_metric}")
                state["metric_name"] = resolved_metric

            # 🔥 STEP 2: Decision fallback
            elif state.get("reasoning_mode") == "decision":
                logger.info("[DECISION] No metric → defaulting intelligently")

                state["metric_name"] = "profit"
                state["metric_names"] = ["profit", "net_sales"]
                state["use_multi_metric"] = True

            # 🔥 STEP 3: Product fallback (SAFE NOW)
            elif state.get("product_query"):
                logger.info("[PRODUCT_QUERY] trying metric recovery")

                resolved_metric = _metric_from_query(q_lower)

                if resolved_metric:
                    state["metric_name"] = resolved_metric
                    logger.info(f"[METRIC_FROM_PRODUCT_QUERY] {resolved_metric}")
                else:
                    state["metric_name"] = "profit"
                    logger.info("[METRIC] Defaulted to 'profit'")

            # 🔥 STEP 4: Final clarify
            elif (
                state.get("analysis_type") not in {"summary"}
                and state.get("answer_shape") not in {"summary"}
                and state.get("intent") != "event_planner"
                and not _is_forecast_request(state)
            ):
                logger.warning("[CLARIFY] No metric found, asking user")
                state["intent"] = "clarify"
                state["clarification_question"] = "Which metric would you like me to analyze?"
                return _render_response(state)

        # -------- ENGINE --------
        state["engine"] = get_engine()
        logger.info("[ENGINE] Database engine initialized")

        # -------- PERIOD PARSING --------
        parsed_period = parse_period(q_lower)
        explicit_comparison = _explicit_month_comparison_from_query(q_lower)
        if explicit_comparison:
            parsed_period = explicit_comparison
            state["analysis_type"] = "comparison"
            state["answer_shape"] = "comparison"
            state["intent"] = "comparison"
            logger.info(f"[PERIOD_COMPARISON_OVERRIDE] {parsed_period}")
        logger.info(f"[PERIOD_PARSED_RAW] {parsed_period}")

        inherited_period = None

        if (
            followup_resolution
            and followup_resolution.is_followup
            and followup_resolution.reuse_previous_period
        ):
            if (
                _is_forecast_request(state)
                and parsed_period.get("type") == "latest_month"
                and not _forecast_query_has_contextual_period_reference(q_lower)
            ):
                logger.info("[FORECAST_PERIOD] Generic forecast request -> using latest available forecast, not inherited chat period")
            else:
                inherited_period = _previous_single_period_from_history(history)

        if inherited_period:
            state["period_parsed"] = inherited_period
            logger.info(f"[PERIOD_INHERITED] {state['period_parsed']}")
        else:
            state["period_parsed"] = parsed_period
            logger.info(f"[PERIOD_PARSED] {state['period_parsed']}")

        if state["period_parsed"].get("type") in {"single", "range"}:
            state["period_payload"] = state["period_parsed"]
        else:
            state["period_payload"] = _prepare_period_payload(
                state["period_parsed"],
                state["analysis_type"]
            )

        logger.info(f"[PERIOD_PAYLOAD] {state['period_payload']}")

        # -------- 🔥 INVENTORY DIAGNOSIS FIX --------
        if (
            state.get("metric_name") in INVENTORY_METRICS
            and state.get("analysis_type") in {"diagnosis", "analysis"}
        ):
            logger.info("[PRODUCT_SKIP] inventory analysis → keeping product/top_n context")

            # Keep product_query and top_n because inventory status may be scoped.
            state["dimension"] = None

        else:
            # -------- PRODUCT RESOLUTION --------
            if not state.get("product_query") and not state.get("product_queries"):
                resolved_products = _resolve_product_queries_from_data(
                    state["engine"], state["user_id"], state["country"], q_lower
                )

                logger.info(f"[PRODUCT_RESOLUTION] Found: {resolved_products}")

                if len(resolved_products) == 1:
                    state["product_query"] = resolved_products[0]
                    state["dimension"] = "sku"
                    state["subject_scope"] = "product"
                    logger.info(f"[PRODUCT_SELECTED] {resolved_products[0]}")

                elif len(resolved_products) > 1:
                    state["product_queries"] = resolved_products
                    state["dimension"] = "sku"
                    state["subject_scope"] = "products"
                    logger.info(f"[MULTI_PRODUCT_SELECTED] {resolved_products}")

        # -------- METRIC COMPATIBILITY CHECK --------
        metric_name = state.get("metric_name")
        product_query = state.get("product_query")
        analysis_type = state.get("analysis_type")

        if metric_name:
            try:
                require_breakdown = analysis_type == "breakdown"

                is_valid, reason = validate_metric_compatibility(
                    metric_name,
                    product_query=product_query,
                    require_breakdown=require_breakdown,
                )

                logger.info(
                    f"[COMPATIBILITY_CHECK] metric={metric_name}, "
                    f"product={product_query}, breakdown={require_breakdown}, valid={is_valid}"
                )

                if not is_valid:
                    logger.warning(f"[COMPATIBILITY_FAIL] {reason}")
                    state["intent"] = "clarify"
                    state["clarification_question"] = (
                        reason + ". Do you want a time-based analysis instead?"
                    )
                    return _render_response(state)

            except Exception:
                logger.exception("[COMPATIBILITY_CHECK_ERROR]")        

        # -------- TOOL PLAN --------
        planned_tools = _build_tool_plan(state)
        logger.info(f"[TOOLS] Planned tools: {planned_tools}")

        # -------- TOOL EXECUTION --------
        for tool_name in planned_tools:
            logger.info(f"[TOOL_EXEC] Running: {tool_name}")
            state = _execute_tool(state, tool_name)
            logger.info(f"[TOOL_DONE] Completed: {tool_name}")

        # -------- FINAL --------
        state = _render_response(state)

        # -------- EMAIL --------
        state = _send_email_if_requested(state)

        logger.info("[END] Response generated successfully")
        return state

    except Exception as e:
        logger.exception("[FATAL ERROR] Agent execution failed")
        state["error"] = str(e)
        state["final_response"] = f"Agent failed: {str(e)}"
        return state


def build_graph() -> SimpleGraph:
    return SimpleGraph()
