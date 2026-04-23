from __future__ import annotations

import calendar
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

from app.ai_agent.db import _format_value, fetch_period_dfs, get_engine, latest_available_month, get_metric_def, validate_metric_compatibility, MonthKey, INVENTORY_METRICS, get_inventory_snapshot, FINANCE_METRICS
from app.ai_agent.email_service import send_agent_email, build_email_html
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
from app.ai_agent.memory import load_last_analysis_from_history
from app.ai_agent.prompts import ADVICE_PROMPT, REASONING_PROMPT, REQUEST_PLANNER_PROMPT
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
    "cm2 profit": "cm2_profit",

    "reimbursement for lost inventory": "lost_total",

    "ads": "advertising_total",
    "advertisement":"advertising_total",
    "visible ads": "visible_ads",
    "voucher ads": "dealsvouchar_ads",

    "reimbursement fee": "rembursement_fee",

    "miscellaneous transaction": "misc_transaction",
    "other transactions": "other",

    "shipment charges": "shipment_charges",

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


@dataclass
class RequestPlan:
    intent: str
    analysis_type: str
    reasoning_mode: str
    task_type: str
    needs_advice: bool
    response_mode: str
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


def _latest_month_result(engine: Any, user_id: int, country: str, metric_name: str) -> Dict[str, Any]:
    latest = latest_available_month(engine, user_id, country)
    return get_metric_for_month(engine, user_id, country, metric_name, latest.month, latest.year)


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
    if any(x in q for x in ["hello", "hi", "hey", "thanks", "thank you"]):
        return RequestPlan("chat", "absolute", "lookup", "value_lookup", False, "short")
    if any(x in q for x in ["what is", "what does", "explain", "meaning of"]):
        if metric_name and "my" not in q and "last" not in q and "month" not in q:
            return RequestPlan("explain", "absolute", "lookup", "value_lookup", False, "short", metric_name=metric_name)
    analysis_type = "growth" if any(x in q for x in ["change", "growth", "increase", "decrease", "trend", "drop"]) else "absolute"
    reasoning_mode = "decision" if any(x in q for x in ["improve", "optimize", "should", "recommend", "fix"]) else ("analysis" if any(x in q for x in ["why", "reason", "underperform"]) or analysis_type == "growth" else "lookup")
    answer_shape = "trend" if analysis_type == "growth" else "single_value"
    ranking_direction = None
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
    return RequestPlan(
        intent="email" if email_requested else "metric_qa",
        analysis_type=analysis_type,
        reasoning_mode=reasoning_mode,
        task_type="recommendation" if reasoning_mode == "decision" else ("diagnosis" if reasoning_mode == "analysis" else "value_lookup"),
        needs_advice=reasoning_mode in {"analysis", "decision"},
        response_mode="detailed" if reasoning_mode in {"analysis", "decision"} else "short",
        metric_name=metric_name,
        answer_shape=answer_shape,
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
        if analysis_type != "growth" and any(word in q for word in ["change", "increase", "decrease", "growth", "decline", "drop"]):
            analysis_type = "growth"
        if reasoning_mode != "decision" and any(trigger in q for trigger in ["improve", "optimize", "should", "recommend", "fix"]):
            reasoning_mode = "decision"
            task_type = "recommendation"
        elif reasoning_mode == "lookup" and any(trigger in q for trigger in ["why", "reason", "underperform"]):
            reasoning_mode = "analysis"
            task_type = "diagnosis"
        if analysis_type == "growth":
            answer_shape = "trend"
        dimension = "sku" if (subject_scope in {"product", "products"} or product_query or product_queries) else fallback.dimension
        return RequestPlan(
            intent=result.intent or fallback.intent,
            analysis_type=analysis_type,
            reasoning_mode=reasoning_mode,
            task_type=task_type,
            needs_advice=bool(result.needs_advice or reasoning_mode in {"analysis", "decision"}),
            response_mode=result.response_mode or fallback.response_mode,
            metric_name=metric_name,
            dimension=dimension,
            product_query=product_query,
            metric_names=metric_names or (None if not metric_name else [metric_name]),
            product_queries=product_queries or None,
            clarification_question=result.clarification_question,
            top_n=result.top_n,
            answer_shape=answer_shape,
            subject_scope=subject_scope,
            ranking_direction=result.ranking_direction or fallback.ranking_direction,
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

    # -------- 🔥 EXTREME PRIORITY (ADD THIS FIRST) --------
    if state.get("answer_shape") == "extreme":
        logger.info("[ROUTE_FIX] extreme → extreme tool")
        return ["extreme"]
    

    # -------- 🔥 EMAIL PRIORITY (ADD HERE) --------
    if state.get("intent") == "email":
        logger.info("[ROUTE] email → standard_analysis + email")
        return ["standard_analysis"]

    
    metric_name = state.get("metric_name")

    # -------- 🔥 GLOBAL PRIORITY FIX: GROWTH FIRST --------
    if state.get("analysis_type") == "growth":
        logger.info("[ROUTE_FIX] growth → standard_analysis")
        return ["standard_analysis"]

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

            # -------- BREAKDOWN --------
            if state.get("analysis_type") == "breakdown":
                return ["standard_analysis"]

            # -------- TREND --------
            if state.get("analysis_type") == "trend":
                return ["standard_analysis"]

            # -------- COMPARISON --------
            if state.get("analysis_type") == "comparison":
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

    if state.get("answer_shape") == "summary" or state.get("analysis_type") == "summary":
        return ["summary"]

    if state.get("analysis_type") == "sku_trend":
        return ["sku_trend"]

    if state.get("answer_shape") == "multi_month":
        return ["multi_month"]

    return ["standard_analysis"]


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


def _compute_summary(state: AgentState) -> AgentState:
    engine = state["engine"]
    latest = latest_available_month(engine, state["user_id"], state["country"])
    metrics: Dict[str, float] = {}
    for metric in OVERALL_MONTH_METRICS:
        try:
            metrics[metric] = float(get_metric_for_month(engine, state["user_id"], state["country"], metric, latest.month, latest.year).get("total", 0.0))
        except Exception:
            logger.debug("Skipping summary metric %s", metric, exc_info=True)
    growth_driver = None
    try:
        growth_driver = get_growth_driver_insights(engine, state["user_id"], state["country"], "net_sales", latest.month, latest.year)
    except Exception:
        logger.debug("Summary growth drivers unavailable", exc_info=True)
    top_products = []
    try:
        latest_sales = get_metric_for_month(engine, state["user_id"], state["country"], "net_sales", latest.month, latest.year)
        top_products = latest_sales.get("per_sku", [])[:5]
    except Exception:
        logger.debug("Summary top products unavailable", exc_info=True)
    state["current_metrics"] = {"metric": "summary", "period_label": latest.label, "metrics": metrics, "total": metrics.get("profit")}
    state["analysis_result"] = {"type": "summary", "metrics": metrics, "growth_driver": growth_driver, "top_products": top_products}
    return state


def _compute_ranking(state: AgentState) -> AgentState:
    engine = state["engine"]
    metric_name = state.get("metric_name") or "profit"
    payload = state["period_payload"]
    direction = state.get("ranking_direction") or "top"
    ptype = payload.get("type")
    if ptype == "latest_month":
        latest = latest_available_month(engine, state["user_id"], state["country"])
        result = get_metric_for_month(engine, state["user_id"], state["country"], metric_name, latest.month, latest.year)
    elif ptype == "single":
        result = get_metric_for_month(engine, state["user_id"], state["country"], metric_name, payload["month"], payload["year"])
    elif ptype == "range":
        result = get_metric_for_period(engine, state["user_id"], state["country"], metric_name, payload["start_month"], payload["start_year"], payload["end_month"], payload["end_year"], skip_missing=True)
    elif ptype == "last_n_months":
        months = _last_n_window(engine, state["user_id"], state["country"], payload["n"])
        result = get_metric_for_period(engine, state["user_id"], state["country"], metric_name, months[0].month, months[0].year, months[-1].month, months[-1].year, skip_missing=True) if months else _latest_month_result(engine, state["user_id"], state["country"], metric_name)
    else:
        result = _latest_month_result(engine, state["user_id"], state["country"], metric_name)
    ranked = rank_skus(result, direction=direction, limit=state.get("top_n") or 5)
    state["current_metrics"] = result
    state["analysis_result"] = {"type": "ranking", "metric": metric_name, "period_label": result.get("period_label"), "per_sku": ranked, "total": result.get("total", 0.0), "ranking_direction": direction}
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
    month_year_pairs = state.get("target_months") or state.get("period_parsed", {}).get("months") or []
    all_results = []
    for metric in metric_names:
        all_results.append(get_metric_for_multiple_months(engine, state["user_id"], state["country"], metric, month_year_pairs, product_queries=state.get("product_queries")))
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

        # -------- COMPARISON --------
        if payload.get("type") == "comparison":
            logger.info("[ROUTE] Comparison analysis")

            p1, p2 = payload["p1"], payload["p2"]

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

def _execute_tool(state: AgentState, tool_name: str) -> AgentState:
    registry: Dict[str, Callable[[AgentState], AgentState]] = {
        "event_plan": _run_event_planner,
        "summary": _compute_summary,
        "ranking": _compute_ranking,
        "extreme": _compute_extreme,
        "multi_month": _compute_multi_month,
        "decision": _compute_decision,
        "sku_intelligence": _compute_sku_intelligence,
        "sku_trend": _compute_sku_trend,
        "standard_analysis": _compute_standard_analysis,
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

    # -------- BODY BUILD --------
    final_text = state.get("final_response", "")

    if not final_text:
        logger.warning("[EMAIL] final_response is EMPTY → email will be blank")

    logger.info(f"[EMAIL] Body preview (first 200 chars): {final_text[:200]}")

    html = f"""
    <div style="font-family: Arial, sans-serif; line-height: 1.6;">
        <h2>Phormula AI Report</h2>
        <div style="white-space: pre-line; font-size: 14px;">
            {final_text}
        </div>
    </div>
    """

    # -------- SEND EMAIL --------
    try:
        logger.info("[EMAIL] Sending email...")

        result = send_agent_email(
            user_id=state["user_id"],
            subject=subject,
            html_body=html,
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

    # -------- 🔥 FINAL RESPONSE AFTER EMAIL --------
    if state.get("email_result", {}).get("status") == "sent":
        state["final_response"] = f"📩 Email sent to {state['email_result'].get('recipient')}."
    else:
        state["final_response"] = "❌ Failed to send email. Please try again."

    return state


def _render_response(state: AgentState) -> AgentState:
    if state.get("intent") == "clarify":
        state["final_response"] = state.get("clarification_question") or "Could you clarify what you'd like me to analyze?"
        return state
    
    if state.get("intent") == "chat":
        if chat_llm:
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
        if pct is None:
            msg = f"{metric_name} was {_format_value(curr, metric_name, state.get('country'))} vs {_format_value(prev, metric_name, state.get('country'))}."
        else:
            direction = "higher" if pct > 0 else "lower"
            msg = f"{metric_name} was {_format_value(curr, metric_name, state.get('country'))} vs {_format_value(prev, metric_name, state.get('country'))}, which is {abs(pct):.2f}% {direction}."
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
            lines.append(f"Your {metric_name} is {_format_value(latest, metric_name, state.get('country'))}.")
            lines.append(
                f"It has {direction} by {_format_value(abs(change), metric_name, state.get('country'))} ({pct:.2f}%)."
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
                                    f"- {g['product_name']} (+{_format_value(g['change'], metric_name, state.get('country'))})"
                                )

                        if top_decline:
                            lines.append("\nTop declining products:")
                            for g in top_decline:
                                lines.append(
                                    f"- {g['product_name']} ({_format_value(g['change'], metric_name, state.get('country'))})"
                                )

                except Exception:
                    logger.exception("[GROWTH_DRIVER_ERROR]")

            state["final_response"] = "\n".join(lines)
            return state
        
        
        latest = series[-1]
        prev = series[-2] if len(series) > 1 else None
        lines = [f"{metric_name} trend ({period_label})", f"Latest: {_format_value(latest['__metric__'], metric_name, state.get('country'))}"]
        if prev:
            delta = latest["__metric__"] - prev["__metric__"]
            pct = (delta / prev["__metric__"] * 100) if prev["__metric__"] else 0
            direction = "↑" if pct > 0 else "↓" if pct < 0 else "→"
            lines.append(f"Change vs previous month: {direction} {abs(pct):.2f}%")
        lines.append("")
        lines.append("Breakdown:")
        for row in series:
            label = row.get("period_label", "Unknown")
            val = float(row.get("__metric__", 0.0))
            lines.append(f"- {label}: {_format_value(val, metric_name, state.get('country'))}")
        state["final_response"] = "\n".join(lines)
        return state
    
    
    # -------- 🔥 HANDLE SKU BREAKDOWN + RANKING --------
    if analysis.get("type") in {"ranking", "breakdown"} and analysis.get("per_sku"):
        logger.info("[RENDER] Rendering SKU breakdown")

        rows = analysis.get("per_sku", [])
        direction = analysis.get("ranking_direction", "top")

        # Sort descending safely
        rows = sorted(rows, key=lambda x: x.get("__metric__", 0), reverse=True)

        header = f"{'Top' if direction == 'top' else 'Bottom'} products for {metric_name}:"
        lines = [header, ""]

        for idx, row in enumerate(rows, 1):
            name = row.get("product_name") or row.get("sku") or "Unknown"
            value = float(row.get("__metric__", 0.0))
            lines.append(f"{idx}. {name} — {_format_value(value, metric_name, state.get('country'))}")

        total = analysis.get("total")
        if total is not None:
            lines.append("")
            lines.append(f"Total: {_format_value(float(total), metric_name, state.get('country'))}")

        state["final_response"] = "\n".join(lines)
        return state
    
    
    if analysis.get("type") == "extreme":
        label = analysis.get("period_label", "Unknown")
        value = float(analysis.get("value", 0.0))
        descriptor = "highest" if analysis.get("extreme_type", "max") == "max" else "lowest"

        product = analysis.get("product")

        if product:
            state["final_response"] = (
                f"{product} had the {descriptor} {metric_name} in {label} "
                f"at {_format_value(value, metric_name, state.get('country'))}."
            )
        else:
            state["final_response"] = (
                f"The {descriptor} {metric_name} was in {label} "
                f"at {_format_value(value, metric_name, state.get('country'))}."
            )

        return state
    
    if analysis.get("type") == "multi_month":
        lines = ["Requested month breakdown:"]
        for metric_block in analysis.get("results", []):
            metric = metric_block.get("metric")
            lines.append(f"\n{metric}:")
            for row in metric_block.get("months", []):
                label = row.get("period_label", "Unknown")
                value = float(row.get("total", 0.0))
                if row.get("product_breakdown"):
                    lines.append(f"- {label}:")
                    for product, product_value in row["product_breakdown"].items():
                        lines.append(f"    {product}: {_format_value(product_value, metric, state.get('country'))}")
                else:
                    lines.append(f"- {label}: {_format_value(value, metric, state.get('country'))}")
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
                lines.append(f"- {product}: {_format_value(total, metric, state.get('country'))}")
            lines.append("")
            lines.append("Breakdown:")
            for row in rows[:50]:
                lines.append(f"- {row['month']} | {row['product']}: {_format_value(row['value'], row['metric'], state.get('country'))}")
        else:
            for row in rows[:50]:
                lines.append(f"- {row['month']} | {row['product']} | {row['metric']}: {_format_value(row['value'], row['metric'], state.get('country'))}")
        state["final_response"] = "\n".join(lines)
        return state
    
    if analysis.get("type") == "summary":
        metrics = analysis.get("metrics", {})
        lines = [
            f"Summary for {period_label}",
            "",
            f"Revenue: {_format_value(metrics.get('net_sales', 0), 'net_sales', state.get('country'))}",
            f"Profit: {_format_value(metrics.get('profit', 0), 'profit', state.get('country'))}",
            f"Orders: {metrics.get('total_quantity', 0):,.0f}",
            f"ASP: {_format_value(metrics.get('asp', 0), 'asp', state.get('country'))}",
            f"ACOS: {metrics.get('acos', 0):.2f}%",
        ]
        top_products = analysis.get("top_products", [])[:5]
        if top_products:
            lines.append("")
            lines.append("Top products:")
            for row in top_products:
                name = row.get("product_name") or row.get("sku") or "Unknown"
                lines.append(f"- {name}: {_format_value(float(row.get('__metric__', 0.0)), 'net_sales', state.get('country'))}")
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
        state["final_response"] = f"In {period_label}, your {metric_name} was {_format_value(float(total), metric_name, state.get('country'))}."
        return state
    state["final_response"] = "I could not build a reliable answer for that request."
    return state


def _restore_memory_email(state: AgentState, plan: RequestPlan, history: List[Dict[str, Any]]) -> Optional[AgentState]:
    last_meta = load_last_analysis_from_history(history)

    if not (
        state.get("email_requested")
        and last_meta
        and plan.intent == "email"
        and not plan.metric_name
        and not plan.product_query
        and plan.analysis_type in {"absolute", "summary"}
    ):
        return None

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

    # -------- 🔥 FIX 1: BUILD RESPONSE FIRST --------
    state = _render_response(state)

    # -------- 🔥 FIX 2: THEN SEND EMAIL --------
    state = _send_email_if_requested(state)

    return state


def _invoke_agent(state: AgentState) -> AgentState:
    try:
        query = state.get("user_query", "")
        history = state.get("chat_history", [])

        logger.info(f"[START] Query: {query}")

        # -------- 🔥 SMART INTENT + METRIC NORMALIZATION --------
        q_lower = query.lower()

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
        if "cm2" in q_lower:
            logger.info("[METRIC_FIX] cm2 → cm2_profit")
            state["metric_name"] = "cm2_profit"
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
            f"answer_shape={plan.answer_shape}, reasoning_mode={plan.reasoning_mode}"
        )

        # -------- MEMORY RESTORE --------
        restored = _restore_memory_email(state, plan, history)
        if restored is not None:
            logger.info("[MEMORY] Restored previous analysis from history")
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
        state["clarification_question"] = plan.clarification_question
        state["response_mode"] = plan.response_mode
        state["email_requested"] = bool(state.get("email_requested") or plan.intent == "email")
        state["answer_shape"] = plan.answer_shape
        state["subject_scope"] = plan.subject_scope
        state["ranking_direction"] = plan.ranking_direction
        state["extreme_type"] = plan.extreme_type
        state["time_granularity"] = plan.time_granularity
        state["target_months"] = plan.target_months
        state["event_name"] = plan.event_name
        state["last_event_month"] = plan.last_event_month
        state["future_event_month"] = plan.future_event_month
        state["target_sales"] = plan.target_sales

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
            ):
                logger.warning("[CLARIFY] No metric found, asking user")
                state["intent"] = "clarify"
                state["clarification_question"] = "Which metric would you like me to analyze?"
                return _render_response(state)

        # -------- ENGINE --------
        state["engine"] = get_engine()
        logger.info("[ENGINE] Database engine initialized")

        # -------- PERIOD PARSING --------
        state["period_parsed"] = parse_period(q_lower)
        logger.info(f"[PERIOD_PARSED] {state['period_parsed']}")

        if state["period_parsed"].get("type") in {"single", "range", "comparison"}:
            state["period_payload"] = state["period_parsed"]
        else:
            state["period_payload"] = _prepare_period_payload(
                state["period_parsed"], state["analysis_type"]
            )

        logger.info(f"[PERIOD_PAYLOAD] {state['period_payload']}")

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
