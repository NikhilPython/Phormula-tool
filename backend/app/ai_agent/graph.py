from __future__ import annotations
import json
import logging
import os
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable, Dict, Iterable, List, Optional, Set, Tuple
from types import SimpleNamespace 
from langchain_openai import ChatOpenAI
from pydantic import BaseModel, Field
from openai import RateLimitError
from sqlalchemy import text
from app.utils.live_bi_utils import generate_sku_inventory_flags
from app.ai_agent.db import _format_value, fetch_period_dfs, fetch_raw_line_item_breakdown, get_engine, latest_available_month, get_metric_def, validate_metric_compatibility, MonthKey, INVENTORY_METRICS, get_inventory_snapshot, FINANCE_METRICS, get_amazon_engine, get_inventory_forecast_snapshot, get_pnl_forecast_snapshot, iter_months
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
    query_wants_current_incomplete_period,
    rank_skus,
)
from app.ai_agent.business_context import build_business_context
from app.ai_agent.memory import load_last_analysis_from_history
from app.ai_agent.prompts import ADVICE_PROMPT, BUSINESS_ADVISOR_PROMPT, REASONING_PROMPT, REQUEST_PLANNER_PROMPT
from app.ai_agent.semantic_layer import (
    anomaly_scan_metrics,
    default_business_analysis_metrics,
    normalize_metric_name,
    resolve_query_semantics,
    sanitize_metric_list,
)
from app.ai_agent.state import AgentState

logger = logging.getLogger(__name__)


INSUFFICIENT_BALANCE_MESSAGE = (
    "Insufficient balance. Your AI credits have been exhausted. "
    "Please recharge your OpenAI account to continue using the chatbot."
)


def _is_insufficient_quota_error(exc: Exception) -> bool:
    error_text = str(exc or "").lower()

    return (
        isinstance(exc, RateLimitError)
        and (
            "insufficient_quota" in error_text
            or "exceeded your current quota" in error_text
            or "billing details" in error_text
        )
    )


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

    "net sales": "net_sales",
    "sales": "net_sales",
    "revenue": "net_sales",
    "net revenue": "net_sales",
    "turnover": "net_sales",
    "gross sales": "gross_sales",

    "quantity": "total_quantity",
    "units sold": "total_quantity",
    "sold units": "total_quantity",
    "ordered units": "quantity",
    "gross units": "quantity",
    "gross quantity": "quantity",
    "refund quantity": "return_quantity",
    "refund units": "return_quantity",
    "refunded quantity": "return_quantity",
    "refunded units": "return_quantity",
    "return quantity": "return_quantity",
    "return units": "return_quantity",
    "returns": "return_quantity",
    "return rate": "return_quantity",
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
    "promotional rebate": "promotional_rebates",
    "promo rebates": "promotional_rebates",
    "promo rebate": "promotional_rebates",
    "discounts": "promotional_rebates",
    "discount": "promotional_rebates",
    "coupons": "promotional_rebates",
    "coupon": "promotional_rebates",
    "deals": "promotional_rebates",
    "deal": "promotional_rebates",
    "promotional rebates tax": "promotional_rebates_tax",

    "refund sales": "refund_sales",
    "sales tax refund": "sales_tax_refund",
    "sales credit refund": "sales_credit_refund",
    "refund rebate": "refund_rebate",

    "marketplace tax": "marketplace_facilitator_tax",
    "digital tax": "digital_transaction_tax",

    "selling fees": "selling_fees",
    "selling fee": "selling_fees",
    "seller fees": "selling_fees",
    "seller fee": "selling_fees",
    "referral fees": "selling_fees",
    "referral fee": "selling_fees",
    "amazon referral fees": "selling_fees",
    "amazon referral fee": "selling_fees",
    "refund selling fees": "refund_selling_fees",
    "refund selling fee": "refund_selling_fees",
    "fba fee": "fba_fees",
    "fba fees": "fba_fees",
    "fba charges": "fba_fees",
    "fba charge": "fba_fees",
    "fulfillment fees": "fba_fees",
    "fulfillment fee": "fba_fees",
    "fulfilment fees": "fba_fees",
    "fulfilment fee": "fba_fees",
    "amazon fulfillment fees": "fba_fees",
    "amazon fulfillment fee": "fba_fees",
    "amazon fulfilment fees": "fba_fees",
    "amazon fulfilment fee": "fba_fees",
    "amazon fee": "amazon_fee",
    "amazon fees": "amazon_fee",

    "platform fee": "platform_fee",
    "platform fee new": "platformfeenew",
    "inventory storage fee": "platform_fee_inventory_storage",
    "inventory storage fees": "platform_fee_inventory_storage",

    "profit": "profit",
    "cm1 profit": "profit",

    # CM2 productwise / total separation
    "productwise cm2 profit": "cm2_profit",
    "productwise cm2profit": "cm2_profit",
    "product wise cm2 profit": "cm2_profit",
    "product wise cm2profit": "cm2_profit",
    "by product cm2 profit": "cm2_profit",
    "by product cm2profit": "cm2_profit",
    "sku wise cm2 profit": "cm2_profit",
    "sku wise cm2profit": "cm2_profit",
    "sku cm2 profit": "cm2_profit",
    "sku cm2profit": "cm2_profit",

    "total cm2 profit": "total_cm2_profit",
    "total cm2profit": "total_cm2_profit",
    "overall cm2 profit": "total_cm2_profit",
    "overall cm2profit": "total_cm2_profit",
    "monthly cm2 profit": "total_cm2_profit",
    "monthly cm2profit": "total_cm2_profit",
    "whole month cm2 profit": "total_cm2_profit",
    "whole month cm2profit": "total_cm2_profit",

    # Default CM2 query should use total/month CM2
    "cm2 profit": "total_cm2_profit",
    "cm2profit": "total_cm2_profit",
    "cm2": "total_cm2_profit",

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
    "miscellaneous transactions": "misc_transaction",
    "miscellaneous": "misc_transaction",
    "misc charges": "misc_transaction",
    "misc charge": "misc_transaction",
    "miscellaneous charges": "misc_transaction",
    "miscellaneous charge": "misc_transaction",
    "other transaction fee": "other_transaction_fees",
    "other transaction fees": "other_transaction_fees",
    "other charges": "other_transaction_fees",
    "other charge": "other_transaction_fees",
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
    "acos": "acos",
    "a cos": "acos",
    "ad cost of sales": "acos",

    "stock": "available",
    "available stock": "available",
    "inventory": "available",
    "available inventory": "available",
    "inbound stock": "inbound_quantity",
    "inbound inventory": "inbound_quantity",
    "reserved stock": "total_reserved_quantity",
    "reserved inventory": "total_reserved_quantity",
    "unfulfillable stock": "unfulfillable_quantity",
    "unfulfillable inventory": "unfulfillable_quantity",
    "sell through": "sell_through",
    "days of supply": "days_of_supply",

}

STOPWORDS = {
    "what", "were", "was", "is", "are", "the", "my", "show", "me", "for", "in", "on", "of", "to",
    "and", "or", "with", "by", "did", "go", "up", "down", "how", "has", "have", "compare", "vs",
    "versus", "last", "month", "months", "this", "that", "why", "from", "rest", "business", "top",
    "bottom", "products", "product", "sku", "goal", "target", "sales", "profit", "net", "gross",
    "spend", "advertising", "acos", "asp", "units", "summary", "plan", "event", "build", "underperforming",
    "impact", "promotional", "promotion", "rebate", "rebates", "discount", "discounts", "coupon", "coupons",
    "margin", "margins", "amazon", "marketplace", "marketplaces", "country", "countries", "us", "uk",
}

PRODUCT_ALIAS_BLOCKLIST = {
    "us",
    "uk",
    "usa",
    "gb",
    "gbr",
    "amazon",
    "amazon us",
    "amazon uk",
    "mtd",
    "qtd",
    "ytd",
    "month",
    "months",
    "sales",
    "profit",
    "margin",
    "margins",
    "discount",
    "discounts",
    "rebate",
    "rebates",
    "promotional rebates",
    "net sales",
    "gross sales",
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
    expected_result_shape: Optional[str] = None
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
    expected_result_shape: Optional[str] = None
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

            if _is_insufficient_quota_error(exc):
                state["error"] = "insufficient_quota"
                state["insufficient_balance"] = True
                state["final_response"] = INSUFFICIENT_BALANCE_MESSAGE
                return state

            state["error"] = str(exc)
            state["final_response"] = (
                f"I couldn't process that request reliably: {exc}"
            )
            return state


def _plan_hint_from_plan(plan: RequestPlan) -> Dict[str, Any]:
    return {
        "intent": plan.intent,
        "analysis_type": plan.analysis_type,
        "reasoning_mode": plan.reasoning_mode,
        "task_type": plan.task_type,
        "metric_name": plan.metric_name,
        "metric_names": plan.metric_names,
        "product_query": plan.product_query,
        "product_queries": plan.product_queries,
        "answer_shape": plan.answer_shape,
        "expected_result_shape": plan.expected_result_shape,
        "subject_scope": plan.subject_scope,
        "dimension": plan.dimension,
        "needs_advice": plan.needs_advice,
        "needs_forecast_data": plan.needs_forecast_data,
    }


def _queryable_metric(metric_name: Optional[str]) -> Optional[str]:
    raw = str(metric_name or "").strip().lower()
    if raw and (raw in FINANCE_METRICS or raw in INVENTORY_METRICS):
        return raw
    metric = normalize_metric_name(metric_name)
    if metric and (metric in FINANCE_METRICS or metric in INVENTORY_METRICS):
        return metric
    return None


def _first_queryable_metric(metrics: List[str]) -> Optional[str]:
    for metric in metrics or []:
        queryable = _queryable_metric(metric)
        if queryable:
            return queryable
    return None


def _apply_semantic_resolution(state: AgentState, semantic: Any) -> None:
    if not semantic:
        return

    semantic_dict = semantic.to_dict() if hasattr(semantic, "to_dict") else dict(semantic)
    state["semantic_resolution"] = semantic_dict
    logger.info(
        "[SEMANTIC] primary=%s metrics=%s broad=%s anomaly=%s confidence=%s reason=%s",
        semantic_dict.get("primary_metric_name"),
        semantic_dict.get("metric_names"),
        semantic_dict.get("is_broad_business_analysis"),
        semantic_dict.get("needs_anomaly_scan"),
        semantic_dict.get("confidence"),
        semantic_dict.get("reason"),
    )

    semantic_metrics = sanitize_metric_list(semantic_dict.get("metric_names") or [], allow_derived=True)
    semantic_primary = normalize_metric_name(semantic_dict.get("primary_metric_name"))
    queryable_primary = _queryable_metric(semantic_primary) or _first_queryable_metric(semantic_metrics)

    if semantic_dict.get("needs_forecast_data"):
        state["needs_forecast_data"] = True

    if semantic_dict.get("needs_anomaly_scan"):
        state["intent"] = "report"
        state["analysis_type"] = "anomaly_scan"
        state["answer_shape"] = "summary"
        state["reasoning_mode"] = "analysis"
        state["task_type"] = "diagnosis"
        state["needs_advice"] = True
        state["subject_scope"] = "business"
        state["metric_name"] = queryable_primary or "profit"
        state["metric_names"] = semantic_metrics or ANOMALY_SCAN_METRICS
        state["use_multi_metric"] = True
        return

    direct_product_breakdown_metric = queryable_primary or _queryable_metric(state.get("metric_name"))
    if _query_requests_product_breakdown(state) and direct_product_breakdown_metric:
        state["intent"] = "report"
        state["analysis_type"] = "breakdown"
        state["answer_shape"] = "ranking"
        state["expected_result_shape"] = "ranking"
        state["dimension"] = "sku"
        state["subject_scope"] = "product"
        state["metric_name"] = direct_product_breakdown_metric
        state["metric_names"] = [direct_product_breakdown_metric]
        state["needs_advice"] = False
        state["use_multi_metric"] = False
        if state.get("reasoning_mode") == "decision":
            state["reasoning_mode"] = "analysis"
        if state.get("task_type") in {"diagnosis", "recommendation", "planning"}:
            state["task_type"] = "value_lookup"
        return

    broad = bool(semantic_dict.get("is_broad_business_analysis"))
    existing_queryable_metrics = [
        metric for metric in sanitize_metric_list(state.get("metric_names") or [], allow_derived=False)
        if _queryable_metric(metric)
    ]
    existing_primary = _queryable_metric(state.get("metric_name"))
    if existing_primary and existing_primary not in existing_queryable_metrics:
        existing_queryable_metrics.insert(0, existing_primary)
    direct_metric_report = bool(
        not broad
        and existing_queryable_metrics
        and state.get("reasoning_mode") != "decision"
        and state.get("task_type") not in {"diagnosis", "recommendation", "planning"}
        and not _query_asks_recommendation(state)
        and not _query_asks_diagnosis(state)
        and (
            state.get("analysis_type") in {"absolute", "comparison", "growth", "trend", "breakdown"}
            or state.get("answer_shape") in {"comparison", "trend", "ranking", "table", "multi_month"}
        )
    )
    if direct_metric_report:
        state["metric_names"] = existing_queryable_metrics
        state["metric_name"] = existing_queryable_metrics[0]
        state["needs_advice"] = False
        state["use_multi_metric"] = len(existing_queryable_metrics) > 1
        if semantic_dict.get("subject_scope") and not state.get("subject_scope"):
            state["subject_scope"] = semantic_dict["subject_scope"]
        return

    is_analytical = (
        state.get("reasoning_mode") in {"analysis", "decision"}
        or state.get("task_type") in {"diagnosis", "recommendation", "planning", "summary"}
        or state.get("analysis_type") in {"comparison", "growth", "trend", "summary", "diagnosis"}
        or state.get("answer_shape") in {"comparison", "trend", "summary"}
    )

    if broad:
        state["intent"] = "report"
        state["metric_name"] = queryable_primary or "profit"
        state["metric_names"] = semantic_metrics or BUSINESS_ANALYSIS_DEFAULT_METRICS
        state["subject_scope"] = "business"
        state["reasoning_mode"] = "analysis"
        state["task_type"] = "diagnosis"
        state["needs_advice"] = True
        state["use_multi_metric"] = True
        if semantic_dict.get("analysis_type"):
            state["analysis_type"] = semantic_dict["analysis_type"]
        if semantic_dict.get("answer_shape"):
            state["answer_shape"] = semantic_dict["answer_shape"]
        return

    focused_metric_query = bool(
        queryable_primary
        and len(state.get("metric_names") or []) <= 1
        and state.get("reasoning_mode") != "decision"
        and state.get("task_type") not in {"diagnosis", "recommendation", "planning", "summary"}
        and state.get("analysis_type") in {"absolute", "comparison", "growth", "trend"}
    )
    if focused_metric_query:
        state["metric_name"] = queryable_primary
        state["metric_names"] = [queryable_primary]
        state["use_multi_metric"] = False
        if semantic_dict.get("subject_scope") and not state.get("subject_scope"):
            state["subject_scope"] = semantic_dict["subject_scope"]
        return

    if semantic_metrics and is_analytical:
        state["metric_names"] = semantic_metrics
        state["metric_name"] = queryable_primary or state.get("metric_name")
        state["needs_advice"] = True
        state["use_multi_metric"] = len(semantic_metrics) > 1
        if len(semantic_metrics) > 1:
            state["intent"] = "report"
            state["task_type"] = state.get("task_type") or "diagnosis"
            state["reasoning_mode"] = "analysis" if state.get("reasoning_mode") == "lookup" else state.get("reasoning_mode")
        if semantic_dict.get("subject_scope") and not state.get("subject_scope"):
            state["subject_scope"] = semantic_dict["subject_scope"]
        return

    if not state.get("metric_name") and queryable_primary:
        state["metric_name"] = queryable_primary
        state["metric_names"] = [queryable_primary]


def _prepare_metrics_for_direct_tools(state: AgentState) -> None:
    if _requires_business_advisor(state) or state.get("analysis_type") == "anomaly_scan":
        return

    metrics = state.get("metric_names") or []
    if metrics:
        queryable_metrics: List[str] = []
        dropped_metrics: List[str] = []
        for metric in metrics:
            queryable = _queryable_metric(metric)
            if queryable and queryable not in queryable_metrics:
                queryable_metrics.append(queryable)
            elif metric:
                dropped_metrics.append(str(metric))

        if dropped_metrics:
            semantic = dict(state.get("semantic_resolution") or {})
            semantic["direct_tool_dropped_derived_metrics"] = dropped_metrics
            state["semantic_resolution"] = semantic
            logger.info("[SEMANTIC_DIRECT_GUARD] dropped non-queryable metrics=%s", dropped_metrics)

        if queryable_metrics:
            state["metric_names"] = queryable_metrics
            if not _queryable_metric(state.get("metric_name")):
                state["metric_name"] = queryable_metrics[0]
        else:
            fallback_metric = _queryable_metric(state.get("metric_name"))
            state["metric_names"] = [fallback_metric] if fallback_metric else []

    if state.get("metric_name") and not _queryable_metric(state.get("metric_name")):
        replacement = _first_queryable_metric(state.get("metric_names") or [])
        if replacement:
            logger.info("[SEMANTIC_DIRECT_GUARD] metric %s -> %s", state.get("metric_name"), replacement)
            state["metric_name"] = replacement


def _normalize(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").strip().lower())



def _extract_requested_countries(query: str) -> List[str]:
    q = _normalize(query)
    q_no_dots = q.replace(".", "")

    countries = []

    # ✅ Add global detection first
    if re.search(r"\bglobal\b|\bworldwide\b|\ball countries\b|\ball marketplaces\b|\boverall\b", q_no_dots):
        countries.append("global")

    if re.search(r"\buk\b|\bgreat britain\b|\bbritain\b|\bunited kingdom\b|\bamazon uk\b", q_no_dots):
        countries.append("uk")

    if re.search(r"\bus\b|\busa\b|\bunited states\b|\bamerica\b|\bamazon us\b", q_no_dots):
        countries.append("us")

    if (
        "both countries" in q_no_dots
        or "both country" in q_no_dots
        or "both marketplaces" in q_no_dots
        or "both markets" in q_no_dots
    ):
        countries = ["uk", "us"]

    return list(dict.fromkeys(countries))

def _safe_div(numerator: float, denominator: float) -> float:
    return 0.0 if denominator == 0 else float(numerator) / float(denominator)


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


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
    exact = [
        r for r in rows
        if pq == str(r.get("product_name", "")).lower().strip()
        or pq == str(r.get("sku", "")).lower().strip()
    ]
    if exact:
        row = exact[0]
        return str(row.get("product_name") or row.get("sku") or product_query).lower().strip()
    contains = [
        r for r in rows
        if pq in str(r.get("product_name", "")).lower().strip()
        or pq in str(r.get("sku", "")).lower().strip()
    ]
    if contains:
        row = contains[0]
        return str(row.get("product_name") or row.get("sku") or product_query).lower().strip()
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
        return {
            "type": "last_n_months",
            "n": parsed["n"],
            "include_current_incomplete": bool(parsed.get("include_current_incomplete", False)),
        }
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


def _payload_includes_current_incomplete(payload: Optional[Dict[str, Any]]) -> bool:
    return bool((payload or {}).get("include_current_incomplete", False))


def _state_includes_current_incomplete(state: AgentState) -> bool:
    payload = state.get("period_payload") or {}
    if "include_current_incomplete" in payload:
        return _payload_includes_current_incomplete(payload)
    return query_wants_current_incomplete_period(state.get("user_query") or "")


def _last_n_window(
    engine: Any,
    user_id: int,
    country: str,
    n: int,
    *,
    extra_for_mom: bool = False,
    include_current_incomplete: bool = False,
) -> List[Any]:
    requested_n = max(int(n or 1), 1)
    count = requested_n + 1 if extra_for_mom else requested_n
    return get_last_n_month_keys(
        engine,
        user_id,
        country,
        count,
        include_current_incomplete=include_current_incomplete,
    )


def _overall_metric_for_payload(engine: Any, user_id: int, country: str, metric_name: str, payload: Dict[str, Any]) -> float:
    ptype = payload.get("type")
    if ptype == "single":
        result = get_metric_for_month(engine, user_id, country, metric_name, payload["month"], payload["year"])
        return float(result.get("total", 0.0))
    if ptype == "last_n_months":
        total = 0.0
        for mk in _last_n_window(
            engine,
            user_id,
            country,
            payload["n"],
            include_current_incomplete=_payload_includes_current_incomplete(payload),
        ):
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


def _is_valid_product_alias(alias: str, *, is_sku: bool = False) -> bool:
    cleaned = _normalize(alias)
    if not cleaned:
        return False
    if cleaned in PRODUCT_ALIAS_BLOCKLIST or cleaned in STOPWORDS:
        return False
    if cleaned.isdigit():
        return False
    if len(cleaned) < (4 if is_sku else 3):
        return False
    return True


def _resolve_product_queries_from_data(engine: Any, user_id: int, country: str, query: str) -> List[str]:
    try:
        latest = latest_available_month(engine, user_id, country)
        sample = get_metric_for_month(engine, user_id, country, "net_sales", latest.month, latest.year)
        alias_to_product: Dict[str, str] = {}
        for row in sample.get("per_sku", []):
            name = str(row.get("product_name") or "").strip().lower()
            sku = str(row.get("sku") or "").strip().lower()
            if _is_valid_product_alias(name):
                alias_to_product[name] = name
            if _is_valid_product_alias(sku, is_sku=True):
                alias_to_product[sku] = name or sku
        if not alias_to_product:
            return []
        q = _normalize(query)
        exact = [
            product
            for alias, product in alias_to_product.items()
            if re.search(rf"\b{re.escape(alias)}\b", q)
        ]
        if exact:
            return list(dict.fromkeys(exact))[:5]
        ngrams = _query_ngrams(query)
        matches: List[str] = []
        for ng in ngrams:
            if len(ng) < 4 or ng in PRODUCT_ALIAS_BLOCKLIST or ng in STOPWORDS:
                continue
            for alias, product in alias_to_product.items():
                if ng == alias or ng in alias:
                    matches.append(product)
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
    if "trend" in q:
        analysis_type = "trend"
    elif any(x in q for x in ["change", "growth", "increase", "decrease", "drop"]):
        analysis_type = "growth"
    else:
        analysis_type = "absolute"
    reasoning_mode = "decision" if any(x in q for x in ["improve", "optimize", "should", "recommend", "fix"]) else ("analysis" if any(x in q for x in ["why", "reason", "underperform"]) or analysis_type in {"growth", "trend"} else "lookup")
    answer_shape = "trend" if analysis_type in {"growth", "trend"} else "single_value"
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
        expected_result_shape = result.expected_result_shape or fallback.expected_result_shape
        analysis_type = result.analysis_type or fallback.analysis_type
        reasoning_mode = result.reasoning_mode or fallback.reasoning_mode
        task_type = result.task_type or fallback.task_type
        wants_detailed_response = any(
            x in q for x in ["detailed", "in detail", "full report", "deep dive", "comprehensive"]
        )
        response_mode = result.response_mode or fallback.response_mode
        if not wants_detailed_response and reasoning_mode in {"analysis", "decision"}:
            response_mode = "short"
        if "trend" in q and not any(word in q for word in ["change", "increase", "decrease", "growth", "decline", "drop"]):
            analysis_type = "trend"
            answer_shape = "trend"
        if analysis_type not in {"growth", "trend"} and any(word in q for word in ["change", "increase", "decrease", "growth", "decline", "drop"]):
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
            expected_result_shape=expected_result_shape,
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
    except Exception as exc:
        logger.exception("Planner failed")

        if _is_insufficient_quota_error(exc):
            raise

        logger.warning("Planner failed; using heuristic fallback")
        return fallback


def _ranking_query_targets_products(state: AgentState) -> bool:
    query = _normalize(state.get("user_query") or "")
    if state.get("dimension") == "sku":
        return True
    if state.get("subject_scope") in {"product", "products", "sku", "skus"}:
        return True
    product_phrases = [
        "which product",
        "which products",
        "which sku",
        "which skus",
        "top product",
        "top products",
        "top sku",
        "top skus",
        "bottom product",
        "bottom products",
        "bottom sku",
        "bottom skus",
        "lowest product",
        "lowest products",
        "lowest sku",
        "lowest skus",
        "highest product",
        "highest products",
        "highest sku",
        "highest skus",
        "product wise",
        "product-wise",
        "productwise",
        "per product",
        "by product",
        "breakdown by product",
        "product breakdown",
        "sku wise",
        "sku-wise",
        "skuwise",
        "per sku",
        "by sku",
        "breakdown by sku",
        "sku breakdown",
    ]
    return any(phrase in query for phrase in product_phrases)


def _query_requests_product_breakdown(state: AgentState) -> bool:
    query = _normalize(state.get("user_query") or "")
    if not query:
        return False

    if _query_asks_raw_line_items(state) or _is_anomaly_request(state):
        return False

    product_breakdown_phrases = [
        "product wise",
        "product-wise",
        "productwise",
        "per product",
        "by product",
        "breakdown by product",
        "product breakdown",
        "product level",
        "product-level",
        "sku wise",
        "sku-wise",
        "skuwise",
        "per sku",
        "by sku",
        "breakdown by sku",
        "sku breakdown",
        "sku level",
        "sku-level",
    ]
    explicit_product_breakdown = any(phrase in query for phrase in product_breakdown_phrases)
    if explicit_product_breakdown and (
        state.get("analysis_type") == "breakdown"
        or "breakdown" in query
        or state.get("answer_shape") in {"table", "ranking"}
    ):
        return True

    if state.get("analysis_type") == "breakdown" and state.get("dimension") == "sku":
        return True

    if state.get("analysis_type") == "breakdown" and state.get("subject_scope") in {"product", "products", "sku", "skus"}:
        return True

    if state.get("answer_shape") in {"table", "ranking"} and _ranking_query_targets_products(state):
        return True

    return False


EXPECTED_RESULT_SHAPES = {
    "none",
    "single_value",
    "monthly_series",
    "comparison",
    "ranking",
    "extreme",
    "diagnosis",
    "recommendation",
    "summary",
    "multi_country",
    "forecast",
    "anomaly_scan",
    "pricing_advisor",
    "raw_line_items",
}


def _normalize_expected_result_shape(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    normalized = str(value).strip().lower().replace("-", "_").replace(" ", "_")
    aliases = {
        "trend": "monthly_series",
        "time_series": "monthly_series",
        "multi_month": "monthly_series",
        "table": "ranking",
        "product_table": "ranking",
        "product_breakdown": "ranking",
        "sku_table": "ranking",
        "sku_breakdown": "ranking",
        "value": "single_value",
        "lookup": "single_value",
        "business_diagnosis": "diagnosis",
        "advice": "recommendation",
        "forecast_analysis": "forecast",
        "anomaly": "anomaly_scan",
        "pricing": "pricing_advisor",
        "asp_advisor": "pricing_advisor",
    }
    normalized = aliases.get(normalized, normalized)
    return normalized if normalized in EXPECTED_RESULT_SHAPES else None


def _query_has_any(state: AgentState, terms: List[str]) -> bool:
    query = _normalize(state.get("user_query") or "")
    return any(term in query for term in terms)


RAW_LINE_ITEM_METRICS = {
    "misc_transaction",
    "other_transaction_fees",
    "other",
    "platform_fee",
    "platformfeenew",
    "platform_fee_inventory_storage",
    "amazon_fee",
    "amazon_fees",
    "selling_fees",
    "fba_fees",
    "ads_spend",
    "total_ads",
    "advertising_total",
    "promotional_rebates",
    "refund_sales",
}


def _query_asks_raw_line_items(state: AgentState) -> bool:
    query = _normalize(state.get("user_query") or "")
    if not query:
        return False

    detail_terms = [
        "line item",
        "line items",
        "itemized",
        "itemised",
        "raw transaction",
        "raw transactions",
        "transaction details",
        "charge details",
        "charges detail",
        "exact charges",
        "what are the charges",
        "which charges",
        "list charges",
        "list the charges",
    ]
    breakdown_terms = ["breakdown", "details", "detail", "list", "show", "give"]
    raw_subject_terms = [
        "misc",
        "miscellaneous",
        "other transaction",
        "other charge",
        "charges",
        "charge",
        "fees",
        "fee",
        "settlement",
        "transaction",
    ]

    metric_name = normalize_metric_name(state.get("metric_name"))
    metric_names = [normalize_metric_name(metric) for metric in (state.get("metric_names") or [])]
    has_raw_metric = bool(
        metric_name in RAW_LINE_ITEM_METRICS
        or any(metric in RAW_LINE_ITEM_METRICS for metric in metric_names)
    )

    explicit_detail = any(term in query for term in detail_terms)
    detail_breakdown = any(term in query for term in breakdown_terms) and any(term in query for term in raw_subject_terms)

    return bool((explicit_detail or detail_breakdown) and (has_raw_metric or any(term in query for term in raw_subject_terms)))


def _query_asks_recommendation(state: AgentState) -> bool:
    return (
        state.get("reasoning_mode") == "decision"
        or state.get("task_type") in {"recommendation", "planning"}
        or _query_has_any(
            state,
            [
                "what should i do",
                "how should",
                "how can",
                "recommend",
                "suggest",
                "optimize",
                "optimise",
                "improve",
                "fix",
                "reduce",
                "increase",
                "plan",
                "strategy",
            ],
        )
    )


def _query_asks_diagnosis(state: AgentState) -> bool:
    return (
        state.get("task_type") == "diagnosis"
        or state.get("analysis_type") in {"diagnosis", "anomaly_scan"}
        or _query_has_any(
            state,
            [
                "why",
                "reason",
                "root cause",
                "what happened",
                "what changed",
                "driver",
                "drivers",
                "impact",
                "effect",
                "affect",
                "affected",
                "influence",
                "problem",
                "issue",
                "risk",
                "underperform",
                "down",
                "drop",
                "decline",
            ],
        )
    )


def _query_asks_monthly_series(state: AgentState) -> bool:
    if _is_plain_metric_movement_query(state):
        return True
    return (
        state.get("analysis_type") in {"trend", "growth"}
        or state.get("answer_shape") in {"trend", "multi_month"}
        or _query_has_any(
            state,
            [
                "trend",
                "movement",
                "over time",
                "increase or decrease",
                "increased or decreased",
                "month by month",
                "month-on-month",
                "month on month",
                "mom",
                "last 2 months",
                "last 3 months",
                "last 4 months",
                "last 5 months",
                "last 6 months",
                "past 2 months",
                "past 3 months",
                "past 4 months",
                "past 5 months",
                "past 6 months",
            ],
        )
    )


def _infer_expected_result_shape(state: AgentState) -> str:
    explicit = _normalize_expected_result_shape(state.get("expected_result_shape"))
    answer_shape = _normalize_expected_result_shape(state.get("answer_shape"))

    if state.get("intent") in {"chat", "explain", "clarify"}:
        return "none"

    if _is_pricing_recommendation_request(state):
        return "pricing_advisor"

    if _query_asks_raw_line_items(state):
        return "raw_line_items"

    if _is_forecast_request(state):
        return "forecast"

    if state.get("target_countries") and len(state.get("target_countries") or []) > 1:
        return "multi_country"

    if state.get("analysis_type") == "ranked_extreme_contribution":
        return "ranking"

    if _is_anomaly_request(state):
        return "anomaly_scan"

    if _query_requests_product_breakdown(state):
        return "ranking"

    if answer_shape == "ranking" and _ranking_query_targets_products(state):
        return "ranking"

    if state.get("answer_shape") in {"ranking", "extreme"} and _ranking_query_targets_products(state):
        return "ranking"

    if state.get("dimension") == "sku" and state.get("answer_shape") in {"ranking", "extreme"}:
        return "ranking"

    if state.get("answer_shape") == "extreme":
        return "extreme"

    if _is_broad_business_analysis_request(state):
        return "recommendation" if _query_asks_recommendation(state) else "diagnosis"

    if _query_asks_recommendation(state) and not _is_plain_metric_movement_query(state):
        return "recommendation"

    if _query_asks_diagnosis(state) and not _is_plain_metric_movement_query(state):
        return "diagnosis"

    if _is_summary_request(state):
        return "summary"

    payload_type = (state.get("period_payload") or {}).get("type")
    if payload_type == "comparison" or state.get("analysis_type") == "comparison" or state.get("answer_shape") == "comparison":
        return "comparison"

    if _query_asks_monthly_series(state):
        return "monthly_series"

    if explicit:
        return explicit

    return "single_value"


def _ensure_expected_result_shape(state: AgentState) -> str:
    shape = _infer_expected_result_shape(state)
    state["expected_result_shape"] = shape
    _apply_expected_result_shape_to_state(state, shape)
    return shape


def _apply_expected_result_shape_to_state(state: AgentState, shape: Optional[str]) -> None:
    if shape == "monthly_series":
        if state.get("analysis_type") not in {"trend", "growth"}:
            state["analysis_type"] = "trend"
        state["answer_shape"] = "trend"
        return

    if shape == "comparison":
        state["analysis_type"] = "comparison"
        state["answer_shape"] = "comparison"
        return

    if shape == "ranking":
        state["answer_shape"] = "ranking"
        if _ranking_query_targets_products(state) or state.get("dimension") == "sku":
            state["dimension"] = "sku"
            state["subject_scope"] = "product"
        if not state.get("ranking_direction"):
            query = _normalize(state.get("user_query") or "")
            state["ranking_direction"] = "bottom" if any(token in query for token in ["lowest", "bottom", "worst"]) else "top"
        if not state.get("top_n") and "which " in _normalize(state.get("user_query") or ""):
            state["top_n"] = 1
        return

    if shape == "extreme":
        state["answer_shape"] = "extreme"
        if not state.get("extreme_type"):
            query = _normalize(state.get("user_query") or "")
            state["extreme_type"] = "min" if any(token in query for token in ["lowest", "least", "minimum", "bottom", "worst"]) else "max"
        return

    if shape == "anomaly_scan":
        state["intent"] = "report"
        state["analysis_type"] = "anomaly_scan"
        state["answer_shape"] = "summary"
        state["reasoning_mode"] = "analysis"
        state["task_type"] = "diagnosis"
        state["needs_advice"] = True
        state["subject_scope"] = state.get("subject_scope") or "business"
        if not state.get("metric_names"):
            state["metric_names"] = ANOMALY_SCAN_METRICS
        if not state.get("metric_name"):
            state["metric_name"] = "profit"
        state["use_multi_metric"] = True
        return

    if shape == "raw_line_items":
        state["intent"] = "report"
        state["analysis_type"] = "breakdown"
        state["answer_shape"] = "raw_line_items"
        state["reasoning_mode"] = "lookup"
        state["subject_scope"] = state.get("subject_scope") or "metric"
        query = _normalize(state.get("user_query") or "")
        metric_name = normalize_metric_name(state.get("metric_name"))
        if not metric_name or metric_name not in RAW_LINE_ITEM_METRICS:
            if any(term in query for term in ["misc", "miscellaneous"]):
                state["metric_name"] = "misc_transaction"
            elif "other transaction" in query or "other charge" in query:
                state["metric_name"] = "other_transaction_fees"
        return

    if shape in {"diagnosis", "recommendation"}:
        state["intent"] = "report"
        state["reasoning_mode"] = "decision" if shape == "recommendation" else "analysis"
        state["task_type"] = "recommendation" if shape == "recommendation" else "diagnosis"
        state["needs_advice"] = True
        if not state.get("product_query") and not state.get("product_queries"):
            state["subject_scope"] = "business"
        if not state.get("metric_names"):
            primary_metric = state.get("metric_name") or "profit"
            state["metric_name"] = primary_metric
            state["metric_names"] = [
                primary_metric,
                *[metric for metric in BUSINESS_ANALYSIS_DEFAULT_METRICS if metric != primary_metric],
            ]
        state["use_multi_metric"] = True
        return

    if shape == "summary":
        state["analysis_type"] = "summary"
        state["answer_shape"] = "summary"
        return


def _tool_plan_from_expected_shape(state: AgentState) -> Optional[List[str]]:
    shape = _ensure_expected_result_shape(state)

    if shape == "none":
        return []

    if state.get("intent") == "email":
        return None

    if state.get("intent") == "event_planner" or state.get("analysis_type") == "event_plan":
        return ["event_plan"]

    if shape == "pricing_advisor":
        return ["pricing_advisor"]
    if shape == "forecast":
        return ["forecast_analysis"]
    if shape == "multi_country":
        return ["multi_country"]
    if shape == "raw_line_items":
        return ["raw_line_items"]
    if shape == "anomaly_scan":
        return ["anomaly_scan"]
    if shape == "ranking":
        return ["ranked_extreme_contribution"] if state.get("analysis_type") == "ranked_extreme_contribution" else ["ranking"]
    if shape == "extreme":
        return ["extreme"]
    if shape in {"diagnosis", "recommendation"}:
        return ["business_advisor"]
    if shape == "summary":
        return ["summary"]
    if shape in {"monthly_series", "comparison", "single_value"}:
        return ["standard_analysis"]

    return None


def _actual_result_shapes(state: AgentState) -> Set[str]:
    shapes: Set[str] = set()
    analysis = state.get("analysis_result") or {}
    current = state.get("current_metrics") or {}
    analysis_type = str(analysis.get("type") or "").lower()

    if state.get("comparison"):
        shapes.add("comparison")

    if state.get("event_plan_result"):
        shapes.add("recommendation")

    if analysis_type in {"trend", "growth"} and isinstance(analysis.get("series"), list):
        shapes.add("monthly_series")
    elif analysis_type in {"multi_month", "multi_dimensional"}:
        shapes.add("monthly_series")
    elif analysis_type in {"comparison", "inventory_comparison"}:
        shapes.add("comparison")
    elif analysis_type == "multi_metric_comparison":
        shapes.add("comparison")
    elif analysis_type == "ranking" and isinstance(analysis.get("per_sku"), list):
        shapes.add("ranking")
    elif analysis_type == "extreme":
        shapes.add("extreme")
    elif analysis_type == "summary":
        shapes.add("summary")
    elif analysis_type.startswith("multi_country"):
        shapes.add("multi_country")
        if "summary" in analysis_type:
            shapes.add("summary")
    elif analysis_type == "forecast":
        shapes.add("forecast")
    elif analysis_type == "raw_line_items":
        shapes.add("raw_line_items")
    elif analysis_type == "anomaly_scan":
        shapes.add("anomaly_scan")
        shapes.add("diagnosis")
    elif analysis_type == "pricing_advisor":
        shapes.add("pricing_advisor")
        shapes.add("recommendation")
    elif analysis_type == "business_advisor":
        shapes.add("diagnosis")
        if state.get("reasoning_mode") == "decision" or state.get("task_type") == "recommendation":
            shapes.add("recommendation")
        context = analysis.get("context") or {}
        if (context.get("comparison") or {}).get("requested") or state.get("comparison"):
            shapes.add("comparison")
    elif analysis_type == "decision":
        shapes.add("recommendation")
    elif analysis_type in {"absolute", "inventory_diagnosis"}:
        shapes.add("single_value")
        if analysis_type == "inventory_diagnosis":
            shapes.add("diagnosis")

    if current.get("total") is not None:
        shapes.add("single_value")

    return shapes or {"none"}


def _result_shape_matches(expected: str, actual_shapes: Set[str]) -> bool:
    if expected == "none":
        return True
    if expected in actual_shapes:
        return True
    compatible = {
        "diagnosis": {"anomaly_scan", "business_advisor"},
        "recommendation": {"diagnosis", "pricing_advisor"},
        "summary": {"multi_country"},
        "multi_country": {"summary"},
    }
    return bool(actual_shapes & compatible.get(expected, set()))


def _fallback_tools_for_shape(expected: str, state: AgentState) -> List[str]:
    mapping = {
        "single_value": ["standard_analysis"],
        "monthly_series": ["standard_analysis"],
        "comparison": ["standard_analysis"],
        "ranking": ["ranking"],
        "extreme": ["extreme"],
        "diagnosis": ["business_advisor"],
        "recommendation": ["business_advisor"],
        "summary": ["summary"],
        "multi_country": ["multi_country"],
        "forecast": ["forecast_analysis"],
        "anomaly_scan": ["anomaly_scan"],
        "pricing_advisor": ["pricing_advisor"],
        "raw_line_items": ["raw_line_items"],
    }
    return list(mapping.get(expected, []))


def _clear_execution_payload_for_replan(state: AgentState) -> None:
    for key in [
        "current_metrics",
        "comparison",
        "analysis_result",
        "business_context",
        "event_plan_result",
        "sku_intelligence_result",
        "advice",
        "tool_error",
    ]:
        state.pop(key, None)


def _validate_and_replan_execution(state: AgentState) -> AgentState:
    expected = _ensure_expected_result_shape(state)
    actual_shapes = _actual_result_shapes(state)
    valid = _result_shape_matches(expected, actual_shapes)
    validation = {
        "expected": expected,
        "actual": sorted(actual_shapes),
        "valid": valid,
        "tool_trace": list(state.get("tool_trace") or []),
    }
    state["execution_validation"] = validation
    logger.info("[EXECUTION_VALIDATION] %s", validation)

    if valid or state.get("contract_replan_attempted"):
        return state

    fallback_tools = _fallback_tools_for_shape(expected, state)
    already_ran = set(state.get("tool_trace") or [])
    fallback_tools = [tool for tool in fallback_tools if tool not in already_ran]
    if not fallback_tools:
        logger.warning("[EXECUTION_VALIDATION] Invalid shape but no fallback tool is available")
        return state

    logger.warning(
        "[CONTRACT_REPLAN] expected=%s actual=%s fallback=%s",
        expected,
        sorted(actual_shapes),
        fallback_tools,
    )
    state["contract_replan_attempted"] = True
    _clear_execution_payload_for_replan(state)
    _apply_expected_result_shape_to_state(state, expected)

    for tool_name in fallback_tools:
        state = _execute_tool(state, tool_name)

    actual_shapes = _actual_result_shapes(state)
    state["execution_validation"] = {
        "expected": expected,
        "actual": sorted(actual_shapes),
        "valid": _result_shape_matches(expected, actual_shapes),
        "tool_trace": list(state.get("tool_trace") or []),
        "replanned": True,
    }
    logger.info("[EXECUTION_VALIDATION_AFTER_REPLAN] %s", state["execution_validation"])
    return state


def _build_tool_plan(state: AgentState) -> List[str]:

    contract_plan = _tool_plan_from_expected_shape(state)
    if contract_plan is not None:
        logger.info(
            "[CONTRACT_ROUTE] expected_shape=%s tools=%s",
            state.get("expected_result_shape"),
            contract_plan,
        )
        return contract_plan

    if _is_pricing_recommendation_request(state):
        logger.info("[ROUTE_FIX] pricing/ASP recommendation -> pricing_advisor tool")
        return ["pricing_advisor"]

    if _is_forecast_request(state):
        logger.info("[ROUTE_FIX] forecast request -> forecast_analysis")
        return ["forecast_analysis"]

    if state.get("target_countries") and len(state.get("target_countries") or []) > 1:
        logger.info("[ROUTE_FIX] multi-country request -> multi_country tool")
        return ["multi_country"]
    
    # -------- 🔥 COMPOSITE PRIORITY --------
    # Handles queries like:
    # "Which product contributed most to the second highest profit month?"
    if state.get("analysis_type") == "ranked_extreme_contribution":
        logger.info("[ROUTE_FIX] ranked extreme contribution → ranked_extreme_contribution tool")
        return ["ranked_extreme_contribution"]

    # -------- 🔥 EXTREME PRIORITY --------
    if state.get("answer_shape") in {"ranking", "extreme"} and _ranking_query_targets_products(state):
        state["dimension"] = "sku"
        state["subject_scope"] = "product"
        state["answer_shape"] = "ranking"
        if not state.get("ranking_direction"):
            query = _normalize(state.get("user_query") or "")
            state["ranking_direction"] = "bottom" if any(token in query for token in ["lowest", "bottom", "worst"]) else "top"
        if not state.get("top_n") and "which " in _normalize(state.get("user_query") or ""):
            state["top_n"] = 1
        logger.info("[ROUTE_FIX] product ranking query -> ranking tool")
        return ["ranking"]

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

    if state.get("analysis_type") == "anomaly_scan" or _is_anomaly_request(state):
        logger.info("[ROUTE_FIX] anomaly request -> anomaly_scan tool")
        return ["anomaly_scan"]

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

        # product-specific trend/growth questions need a monthly series, not a SKU deep dive
        if state.get("analysis_type") in {"trend", "growth"} or state.get("answer_shape") == "trend":
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

    if _query_requests_product_breakdown(state):
        return False

    semantic = state.get("semantic_resolution") or {}
    if semantic.get("is_broad_business_analysis") or semantic.get("needs_anomaly_scan"):
        return True

    if state.get("use_multi_metric") and state.get("subject_scope") == "business":
        return True

    if state.get("reasoning_mode") == "decision":
        return True

    if _is_plain_metric_movement_query(state):
        return False

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


def _is_plain_metric_movement_query(state: AgentState) -> bool:
    if not (state.get("metric_name") or state.get("metric_names")):
        return False

    if (state.get("semantic_resolution") or {}).get("is_broad_business_analysis"):
        return False

    query = (state.get("user_query") or "").lower()
    advice_or_diagnosis_terms = [
        "why",
        "reason",
        "root cause",
        "what should",
        "how should",
        "how can",
        "what can i do",
        "recommend",
        "suggest",
        "fix",
        "improve",
        "optimise",
        "optimize",
        "problem",
        "issue",
        "impact",
        "effect",
        "affect",
        "affected",
        "influence",
        "risk",
        "opportunity",
        "underperform",
        "next step",
        "action",
    ]
    if any(term in query for term in advice_or_diagnosis_terms):
        return False

    analysis_type = state.get("analysis_type")
    answer_shape = state.get("answer_shape")
    movement_terms = [
        "trend",
        "change",
        "changed",
        "increase",
        "decrease",
        "increased",
        "decreased",
        "growth",
        "decline",
        "drop",
        "over the last",
        "last ",
        "from ",
        " to ",
        "vs",
        "versus",
        "compare",
    ]

    return (
        analysis_type in {"growth", "trend", "comparison"}
        or answer_shape in {"trend", "comparison", "change"}
        or any(term in query for term in movement_terms)
    )


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


def _period_label_for_month_keys(months: List[MonthKey]) -> str:
    if not months:
        return "selected period"
    if len(months) == 1:
        return months[0].label
    return f"{months[0].label} to {months[-1].label}"


def _metric_for_exact_month_keys(
    engine: Any,
    state: AgentState,
    country: str,
    metric_name: str,
    months: List[MonthKey],
) -> Dict[str, Any]:
    metric_def = get_metric_def(metric_name)
    loaded_months: List[MonthKey] = []
    monthly_results: List[Dict[str, Any]] = []

    for month_key in months:
        try:
            result = get_metric_for_month(
                engine,
                state["user_id"],
                country,
                metric_name,
                month_key.month,
                month_key.year,
            )
        except Exception as exc:
            logger.warning("[METRIC_MONTH_SKIP] metric=%s month=%s reason=%s", metric_name, month_key.label, exc)
            continue

        loaded_months.append(month_key)
        monthly_results.append(result)

    combined: Dict[str, Any] = {
        "metric": metric_def.name,
        "column": metric_def.column,
        "metric_kind": metric_def.kind,
        "period_type": "multi_month",
        "period_label": _period_label_for_month_keys(loaded_months),
        "months_found": [month_key.label for month_key in loaded_months],
        "requested_months": [month_key.label for month_key in months],
        "total": 0.0,
        "per_sku": [],
        "row_count": 0,
    }

    if not monthly_results:
        return combined

    if metric_def.kind not in {"sku_additive", "sku_precomputed"}:
        values = [_safe_float(result.get("total")) for result in monthly_results]
        combined["total"] = sum(values)
        combined["row_count"] = len(monthly_results)
        return combined

    product_rows: Dict[str, Dict[str, Any]] = {}
    for result in monthly_results:
        for row in result.get("per_sku") or []:
            sku = str(row.get("sku") or "").strip()
            product_name = str(row.get("product_name") or "").strip()
            key = sku or product_name
            if not key:
                continue

            bucket = product_rows.setdefault(
                key,
                {
                    "sku": sku,
                    "product_name": product_name,
                    "__metric__": 0.0,
                    "__count__": 0,
                },
            )
            if not bucket.get("product_name") and product_name:
                bucket["product_name"] = product_name
            if not bucket.get("sku") and sku:
                bucket["sku"] = sku
            bucket["__metric__"] += _safe_float(row.get("__metric__"))
            bucket["__count__"] += 1

    rows: List[Dict[str, Any]] = []
    for bucket in product_rows.values():
        value = _safe_float(bucket.get("__metric__"))
        if metric_def.kind == "sku_precomputed":
            value = _safe_float(value) / max(int(bucket.get("__count__") or 1), 1)
        rows.append(
            {
                "sku": bucket.get("sku") or "",
                "product_name": bucket.get("product_name") or "",
                "__metric__": value,
            }
        )

    combined["per_sku"] = rows
    combined["row_count"] = len(rows)
    monthly_totals = [_safe_float(result.get("total")) for result in monthly_results]
    combined["total"] = (
        sum(monthly_totals) / len(monthly_totals)
        if metric_def.kind == "sku_precomputed" and monthly_totals
        else sum(monthly_totals)
    )

    return combined


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
            payload["n"],
            include_current_incomplete=_payload_includes_current_incomplete(payload),
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

    elif ptype == "multi_month":
        months = [
            MonthKey(year=int(item["year"]), month=int(item["month"]))
            for item in payload.get("months", [])
            if item.get("month") and item.get("year")
        ]
        period_months = months
        result = _metric_for_exact_month_keys(
            engine,
            state,
            state["country"],
            metric_name,
            months,
        )

    elif ptype == "comparison":
        period_parts = [payload.get("p1") or {}, payload.get("p2") or {}]
        bounds: List[Tuple[int, int, int, int]] = []

        for part in period_parts:
            try:
                if part.get("type") == "single":
                    start_month = end_month = int(part["month"])
                    start_year = end_year = int(part["year"])
                else:
                    start_month = int(part["start_month"])
                    start_year = int(part["start_year"])
                    end_month = int(part["end_month"])
                    end_year = int(part["end_year"])
                bounds.append((start_year, start_month, end_year, end_month))
            except Exception:
                logger.debug("[RANKING_COMPARISON_PERIOD] Skipping invalid comparison part: %s", part)

        if bounds:
            starts = sorted((start_year, start_month) for start_year, start_month, _, _ in bounds)
            ends = sorted((end_year, end_month) for _, _, end_year, end_month in bounds)
            start_year, start_month = starts[0]
            end_year, end_month = ends[-1]

            try:
                period_months = [
                    month_key
                    for month_key, _ in fetch_period_dfs(
                        engine,
                        state["user_id"],
                        state["country"],
                        start_month,
                        start_year,
                        end_month,
                        end_year,
                        skip_missing=True,
                    )
                ]
            except Exception:
                logger.debug("Ranking comparison period month list unavailable", exc_info=True)
                period_months = []

            result = get_metric_for_period(
                engine,
                state["user_id"],
                state["country"],
                metric_name,
                start_month,
                start_year,
                end_month,
                end_year,
                skip_missing=True,
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
    successful_metrics: List[str] = []
    skipped_metrics: List[Dict[str, str]] = []
    for metric in metric_names:
        metric_name = _queryable_metric(metric)
        if not metric_name:
            skipped_metrics.append({"metric": str(metric), "reason": "not queryable"})
            continue
        try:
            all_results.append(
                get_metric_for_multiple_months(
                    engine,
                    state["user_id"],
                    state["country"],
                    metric_name,
                    month_year_pairs,
                    product_queries=product_queries,
                )
            )
            successful_metrics.append(metric_name)
        except Exception as exc:
            logger.warning("[MULTI_MONTH_SKIP] metric=%s reason=%s", metric_name, exc)
            skipped_metrics.append({"metric": metric_name, "reason": str(exc)})
            continue
    result = {
        "metrics": successful_metrics,
        "results": all_results,
        "skipped_metrics": skipped_metrics,
    }
    if not all_results:
        state["tool_error"] = "; ".join(
            f"{row.get('metric')}: {row.get('reason')}" for row in skipped_metrics
        )
        state["current_metrics"] = result
        state["analysis_result"] = {"type": "multi_month", **result}
        return state
    state["current_metrics"] = result
    state["analysis_result"] = {"type": "multi_month", **result}
    return state


def _anomaly_metric_is_bad_when_up(metric_name: str) -> bool:
    return metric_name in ANOMALY_BAD_WHEN_UP_METRICS


def _anomaly_business_delta(metric_name: str, current_value: float, previous_value: float) -> float:
    if _anomaly_metric_is_bad_when_up(metric_name):
        return abs(current_value) - abs(previous_value)
    return current_value - previous_value


def _format_anomaly_delta(value: float, metric_name: str, country: Optional[str]) -> str:
    if metric_name in ANOMALY_PERCENTAGE_METRICS:
        return f"{abs(value):.2f} pts"
    return _format_metric_for_display(abs(value), metric_name, country)


def _anomaly_change_text(metric_name: str, business_delta: float) -> str:
    if _anomaly_metric_is_bad_when_up(metric_name):
        return "burden up" if business_delta > 0 else "burden down"
    return "down" if business_delta < 0 else "up"


def _is_material_anomaly(metric_name: str, current_value: float, previous_value: float, business_delta: float) -> bool:
    if abs(business_delta) < 0.005:
        return False

    if metric_name in ANOMALY_PERCENTAGE_METRICS:
        return abs(business_delta) >= 5.0

    if abs(previous_value) < 0.005:
        return abs(current_value) >= 1.0

    pct_change = abs(business_delta) / max(abs(previous_value), 1.0) * 100.0
    return pct_change >= 15.0


def _row_identity(row: Dict[str, Any]) -> str:
    return str(row.get("sku") or row.get("product_name") or "").strip().lower()


def _top_sku_anomaly_contributors(
    metric_name: str,
    current_result: Dict[str, Any],
    previous_result: Dict[str, Any],
    *,
    adverse: bool,
    limit: int = 2,
) -> List[Dict[str, Any]]:
    current_rows = current_result.get("per_sku") or []
    previous_rows = previous_result.get("per_sku") or []
    if not current_rows and not previous_rows:
        return []

    previous_lookup = {
        _row_identity(row): row
        for row in previous_rows
        if _row_identity(row)
    }
    current_lookup = {
        _row_identity(row): row
        for row in current_rows
        if _row_identity(row)
    }

    contributors: List[Dict[str, Any]] = []
    for key in sorted(set(previous_lookup) | set(current_lookup)):
        current_row = current_lookup.get(key) or {}
        previous_row = previous_lookup.get(key) or {}
        current_value = _safe_float(current_row.get("__metric__"))
        previous_value = _safe_float(previous_row.get("__metric__"))
        business_delta = _anomaly_business_delta(metric_name, current_value, previous_value)
        if abs(business_delta) < 0.005:
            continue

        is_unfavorable = business_delta > 0 if _anomaly_metric_is_bad_when_up(metric_name) else business_delta < 0
        if adverse and not is_unfavorable:
            continue
        if not adverse and is_unfavorable:
            continue

        row = current_row or previous_row
        contributors.append(
            {
                "sku": row.get("sku"),
                "product_name": row.get("product_name"),
                "current": current_value,
                "previous": previous_value,
                "business_delta": business_delta,
                "change_text": _anomaly_change_text(metric_name, business_delta),
                "score": abs(business_delta),
            }
        )

    return sorted(contributors, key=lambda row: _safe_float(row.get("score")), reverse=True)[:limit]


def _matching_product_rows(rows: List[Dict[str, Any]], product_query: Optional[str]) -> List[Dict[str, Any]]:
    if not rows or not product_query:
        return []

    query = str(product_query).strip().lower()
    exact = [
        row for row in rows
        if query == str(row.get("product_name") or "").strip().lower()
        or query == str(row.get("sku") or "").strip().lower()
    ]
    if exact:
        return exact

    return [
        row for row in rows
        if query in str(row.get("product_name") or "").strip().lower()
        or query in str(row.get("sku") or "").strip().lower()
    ]


def _resolve_anomaly_product_scope(
    engine: Any,
    state: AgentState,
    country: str,
    months: List[MonthKey],
) -> Optional[Dict[str, Any]]:
    product_query = state.get("product_match") or state.get("product_query")
    if not product_query or not months:
        return None

    sample_metrics = ["net_sales", "profit", "total_quantity", "cm2_profit", "ads_spend"]
    for month_key in reversed(months):
        for metric_name in sample_metrics:
            try:
                sample = get_metric_for_month(
                    engine,
                    state["user_id"],
                    country,
                    metric_name,
                    month_key.month,
                    month_key.year,
                )
            except Exception:
                continue

            match_key = _match_product(sample.get("per_sku", []), str(product_query))
            if not match_key:
                continue

            rows = _matching_product_rows(sample.get("per_sku", []), match_key)
            first = rows[0] if rows else {}
            display_name = first.get("product_name") or match_key or product_query
            sku = first.get("sku")
            return {
                "query": product_query,
                "match": match_key,
                "display_name": str(display_name),
                "sku": str(sku) if sku else None,
                "matched": True,
            }

    return {
        "query": product_query,
        "match": str(product_query),
        "display_name": str(product_query),
        "sku": None,
        "matched": False,
    }


def _product_scoped_metric_result(
    result: Dict[str, Any],
    product_scope: Optional[Dict[str, Any]],
    *,
    value: Optional[float] = None,
) -> Dict[str, Any]:
    if not product_scope:
        return result

    scoped = dict(result or {})
    match_key = product_scope.get("match") or product_scope.get("query")
    rows = _matching_product_rows(result.get("per_sku") or [], str(match_key or ""))
    scoped["per_sku"] = rows
    scoped["row_count"] = len(rows)
    scoped["product_match"] = product_scope.get("match")
    scoped["product_name"] = product_scope.get("display_name")
    scoped["sku"] = product_scope.get("sku")

    if value is not None:
        scoped["total"] = float(value)
    elif rows:
        scoped["total"] = sum(_safe_float(row.get("__metric__")) for row in rows)
    else:
        scoped["total"] = 0.0

    scoped["product_scope_unavailable"] = not bool(rows)
    return scoped


def _compute_anomaly_scan(state: AgentState) -> AgentState:
    engine = state["engine"]
    country = state["country"]
    requested_metrics = state.get("metric_names") or []
    if not requested_metrics:
        requested_metrics = [state.get("metric_name")] if state.get("metric_name") else ANOMALY_SCAN_METRICS
    metric_names = [metric for metric in requested_metrics if metric]

    months = _months_from_period_payload(
        engine,
        state,
        country,
        state.get("period_payload") or {},
        default_n=3,
    )
    if len(months) < 2:
        months = _last_n_window(
            engine,
            state["user_id"],
            country,
            3,
            include_current_incomplete=_state_includes_current_incomplete(state),
        )

    product_scope = _resolve_anomaly_product_scope(engine, state, country, months)
    if product_scope:
        state["product_match"] = product_scope.get("match")

    metric_blocks: List[Dict[str, Any]] = []
    anomalies: List[Dict[str, Any]] = []
    product_unavailable_metrics: List[str] = []

    for metric_name in metric_names:
        month_results: List[Dict[str, Any]] = []
        for month_key in months:
            try:
                if metric_name in INVENTORY_METRICS:
                    result = get_inventory_snapshot(
                        user_id=state["user_id"],
                        metric_name=metric_name,
                        month=month_key.month,
                        year=month_key.year,
                        country=country,
                    )
                else:
                    result = get_metric_for_month(
                        engine,
                        state["user_id"],
                        country,
                        metric_name,
                        month_key.month,
                        month_key.year,
                    )
            except Exception:
                logger.debug("[ANOMALY_METRIC_SKIP] metric=%s month=%s", metric_name, month_key.label, exc_info=True)
                continue

            value = _safe_float(result.get("total"))
            if product_scope:
                scoped_value, matched_product = _country_metric_value(
                    engine,
                    state,
                    country,
                    metric_name,
                    month_key.month,
                    month_key.year,
                    product_scope.get("match") or product_scope.get("query"),
                )
                if not matched_product:
                    if metric_name not in product_unavailable_metrics:
                        product_unavailable_metrics.append(metric_name)
                    continue
                value = _safe_float(scoped_value)
                result = _product_scoped_metric_result(result, product_scope, value=value)

            month_results.append(
                {
                    "month": month_key.month,
                    "year": month_key.year,
                    "period_label": result.get("period_label") or month_key.label,
                    "value": value,
                    "formatted": _format_metric_for_display(value, metric_name, country),
                    "raw": result,
                }
            )

        metric_blocks.append({"metric": metric_name, "months": [{k: v for k, v in row.items() if k != "raw"} for row in month_results]})

        for idx in range(1, len(month_results)):
            previous = month_results[idx - 1]
            current = month_results[idx]
            current_value = _safe_float(current.get("value"))
            previous_value = _safe_float(previous.get("value"))
            raw_delta = current_value - previous_value
            business_delta = _anomaly_business_delta(metric_name, current_value, previous_value)
            if not _is_material_anomaly(metric_name, current_value, previous_value, business_delta):
                continue

            pct_change = None
            if abs(previous_value) >= 0.005:
                pct_change = (business_delta / abs(previous_value)) * 100.0

            adverse = business_delta > 0 if _anomaly_metric_is_bad_when_up(metric_name) else business_delta < 0
            contributors = _top_sku_anomaly_contributors(
                metric_name,
                current.get("raw") or {},
                previous.get("raw") or {},
                adverse=adverse,
            )

            anomalies.append(
                {
                    "metric": metric_name,
                    "label": humanize_metric(metric_name),
                    "period_label": current.get("period_label"),
                    "previous_period_label": previous.get("period_label"),
                    "current": current_value,
                    "previous": previous_value,
                    "raw_delta": raw_delta,
                    "business_delta": business_delta,
                    "business_effect": "unfavorable" if adverse else "favorable",
                    "change_text": _anomaly_change_text(metric_name, business_delta),
                    "pct_change": pct_change,
                    "delta_formatted": _format_anomaly_delta(business_delta, metric_name, country),
                    "current_formatted": _format_metric_for_display(current_value, metric_name, country),
                    "previous_formatted": _format_metric_for_display(previous_value, metric_name, country),
                    "contributors": contributors,
                    "severity_score": abs(pct_change) if pct_change is not None else abs(business_delta),
                }
            )

    anomalies = sorted(
        anomalies,
        key=lambda row: (row.get("business_effect") != "unfavorable", -_safe_float(row.get("severity_score"))),
    )
    period_label = (
        f"{months[0].label} to {months[-1].label}"
        if months
        else "selected period"
    )
    state["current_metrics"] = {
        "metric": "anomaly_scan",
        "period_label": period_label,
        "metrics": metric_names,
        "total": None,
        "focus_scope": "product" if product_scope else "overall",
        "product": product_scope.get("display_name") if product_scope else None,
        "product_match": product_scope.get("match") if product_scope else None,
        "sku": product_scope.get("sku") if product_scope else None,
    }
    state["analysis_result"] = {
        "type": "anomaly_scan",
        "country": country,
        "period_label": period_label,
        "metrics": metric_names,
        "focus_scope": "product" if product_scope else "overall",
        "product_scope": product_scope,
        "product_unavailable_metrics": product_unavailable_metrics,
        "metric_blocks": metric_blocks,
        "anomalies": anomalies[:10],
    }
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
            trend_months = _last_n_window(
                engine,
                state["user_id"],
                state["country"],
                6,
                include_current_incomplete=_state_includes_current_incomplete(state),
            )
            for metric in metric_names:
                try:
                    history[metric] = build_time_series_analysis(engine, state["user_id"], state["country"], metric, trend_months, product_match=product_match, time_unit=state.get("period_parsed", {}).get("unit"))
                except Exception:
                    logger.debug("Decision history unavailable for %s", metric, exc_info=True)
        else:
            current_pack = {"period_label": latest.label, "metrics": {}}
    else:
        current_pack = get_metric_pack_for_month(engine, state["user_id"], state["country"], metric_names, latest.month, latest.year)
        trend_months = _last_n_window(
            engine,
            state["user_id"],
            state["country"],
            6,
            include_current_incomplete=_state_includes_current_incomplete(state),
        )
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
    comparison_context = context.get("comparison") or {}
    comparison_metric = (comparison_context.get("metrics") or {}).get(metric_name) or {}
    period_label = context.get("period", {}).get("label")
    current_total = totals.get(metric_name)

    if comparison_context.get("requested") and comparison_metric:
        left = comparison_context.get("left") or {}
        right = comparison_context.get("right") or {}
        period_label = f"{left.get('label') or 'current period'} vs {right.get('label') or 'previous period'}"
        current_total = comparison_metric.get("left")
        state["comparison"] = {
            "left": {"label": left.get("label"), "total": comparison_metric.get("left")},
            "right": {"label": right.get("label"), "total": comparison_metric.get("right")},
            "delta": comparison_metric.get("delta"),
            "pct_change": comparison_metric.get("pct_change"),
        }
        logger.info(
            "[BUSINESS_COMPARISON] metric=%s period=%s drivers=%s unfavorable=%s",
            metric_name,
            period_label,
            len(comparison_context.get("metric_drivers") or []),
            len(comparison_context.get("unfavorable_metric_drivers") or []),
        )

    state["business_context"] = context
    state["current_metrics"] = {
        "metric": metric_name,
        "period_label": period_label,
        "total": current_total,
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
            months = _last_n_window(
                engine,
                state["user_id"],
                state["country"],
                pl["n"],
                include_current_incomplete=_payload_includes_current_incomplete(pl),
            )
            acc = {"total_quantity": 0.0, "net_sales": 0.0, "profit": 0.0}
            for mk in months:
                metrics = get_product_metric_pack_for_month(engine, state["user_id"], state["country"], product_match, mk.month, mk.year)["metrics"]
                for key in ["total_quantity", "net_sales", "profit"]:
                    acc[key] += float(metrics.get(key, 0.0))
            acc["asp"] = _safe_div(acc["net_sales"], acc["total_quantity"])
            return _enrich_pack_with_mix(engine, state["user_id"], state["country"], pl, acc)
        if ptype == "range":
            months = get_last_n_month_keys(
                engine,
                state["user_id"],
                state["country"],
                36,
                include_current_incomplete=_state_includes_current_incomplete(state),
            )
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
        months = _last_n_window(
            engine,
            state["user_id"],
            state["country"],
            n * 2,
            include_current_incomplete=_payload_includes_current_incomplete(base_for_prev),
        )
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
    trend_months = _months_from_period_payload(
        engine,
        state,
        state["country"],
        effective_payload,
        default_n=6,
    )
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
    months = _last_n_window(
        engine,
        state["user_id"],
        state["country"],
        payload.get("n", 12) if payload.get("type") == "last_n_months" else 12,
        include_current_incomplete=_state_includes_current_incomplete(state),
    )
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


def _markdown_table_cell(value: Any) -> str:
    text = str(value if value is not None else "").replace("\n", " ").strip()
    return text.replace("|", "\\|") or "-"


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


def _compute_multi_metric_comparison(state: AgentState) -> AgentState:
    engine = state["engine"]
    payload = state.get("period_payload") or {}
    p1 = payload.get("p1") or {}
    p2 = payload.get("p2") or {}
    requested_metrics = state.get("metric_names") or [state.get("metric_name") or "net_sales"]
    metrics: List[str] = []
    skipped: List[Dict[str, str]] = []
    results: List[Dict[str, Any]] = []

    for metric in requested_metrics:
        metric_name = _queryable_metric(metric)
        if not metric_name:
            skipped.append({"metric": str(metric), "reason": "not queryable"})
            continue
        if metric_name in metrics:
            continue
        metrics.append(metric_name)

    for metric_name in metrics:
        try:
            if metric_name in INVENTORY_METRICS:
                left_month = _period_end_month_key(p1)
                right_month = _period_end_month_key(p2)
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
                left_value, _, _ = _inventory_snapshot_value(left_snapshot, state.get("product_query"))
                right_value, _, _ = _inventory_snapshot_value(right_snapshot, state.get("product_query"))
                if left_value is None or right_value is None:
                    skipped.append({"metric": metric_name, "reason": "inventory data missing for one period"})
                    continue
                comparison = {
                    "metric": metric_name,
                    "left": {"label": left_snapshot.get("period_label") or left_month.label, "total": left_value},
                    "right": {"label": right_snapshot.get("period_label") or right_month.label, "total": right_value},
                }
                comparison["delta"] = float(left_value) - float(right_value)
                comparison["pct_change"] = None if float(right_value) == 0 else (comparison["delta"] / float(right_value)) * 100.0
            else:
                comparison = compare_periods(
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
            left_total = _safe_float((comparison.get("left") or {}).get("total"))
            right_total = _safe_float((comparison.get("right") or {}).get("total"))
            display_delta = right_total - left_total
            comparison["display_delta"] = display_delta
            comparison["display_pct_change"] = None if abs(left_total) < 0.005 else (display_delta / left_total) * 100.0
            comparison["display_delta_basis"] = "right_period_minus_left_period"
            results.append(comparison)
        except Exception as exc:
            logger.warning("[MULTI_METRIC_COMPARISON_SKIP] metric=%s reason=%s", metric_name, exc)
            skipped.append({"metric": metric_name, "reason": str(exc)})

    state["comparison"] = {
        "type": "multi_metric",
        "metrics": metrics,
        "results": results,
        "skipped": skipped,
    }
    state["current_metrics"] = {
        "metric": metrics[0] if metrics else state.get("metric_name"),
        "metrics": metrics,
        "period_label": (
            f"{(results[0].get('left') or {}).get('label')} vs {(results[0].get('right') or {}).get('label')}"
            if results else "selected periods"
        ),
        "total": (results[0].get("right") or {}).get("total") if results else None,
    }
    state["analysis_result"] = {
        "type": "multi_metric_comparison",
        "country": state.get("country"),
        "metrics": metrics,
        "results": results,
        "skipped": skipped,
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

        # -------- MULTI-PERIOD PREP --------
        needs_month_expansion = bool(
            (metric_names and len(metric_names) > 1)
            or (product_queries and len(product_queries) > 1)
        )
        if needs_month_expansion and not months:
            if payload.get("type") == "single":
                months = [{"month": payload["month"], "year": payload["year"]}]
            elif payload.get("type") == "last_n_months":
                months = [
                    {"month": mk.month, "year": mk.year}
                    for mk in _last_n_window(
                        engine,
                        state["user_id"],
                        state["country"],
                        payload["n"],
                        include_current_incomplete=_payload_includes_current_incomplete(payload),
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
                months = [{"month": mk.month, "year": mk.year} for mk, _ in dfs]

            if months:
                state["target_months"] = months
                logger.info(f"[MULTI_PERIOD_MONTHS] {months}")

        if payload.get("type") == "comparison" and metric_names and len(metric_names) > 1:
            logger.info("[ROUTE] Multi-metric comparison analysis")
            return _compute_multi_metric_comparison(state)

        if metric_names and len(metric_names) > 1 and months and not (product_queries and len(product_queries) > 1):
            logger.info("[ROUTE] Multi-metric monthly analysis")
            return _compute_multi_month(state)

        if product_queries and len(product_queries) > 1 and months:
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
                    include_current_incomplete=_payload_includes_current_incomplete(base),
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
                    include_current_incomplete=_state_includes_current_incomplete(state),
                )

            else:
                logger.info("[GROWTH] Fallback window")

                months_full = _last_n_window(
                    engine,
                    state["user_id"],
                    state["country"],
                    6,
                    extra_for_mom=True,
                    include_current_incomplete=_state_includes_current_incomplete(state),
                )

            if len(months_full) < 2:
                logger.warning("[GROWTH] Not enough months")
                state["analysis_result"] = {"type": "growth", "series": []}
                return state

            logger.info(f"[GROWTH_MONTHS] {[m.label for m in months_full]}")

            # -------- PRODUCT-AWARE GROWTH --------
            product_query = state.get("product_query")
            product_match = None

            if product_query:
                logger.info(f"[GROWTH_PRODUCT] Filtering for product: {product_query}")
                sample_month = months_full[-1]
                sample = get_unified_metric(
                    engine,
                    state,
                    metric_name,
                    sample_month.month,
                    sample_month.year,
                )
                product_match = _match_product(sample.get("per_sku", []), product_query)
                state["product_match"] = product_match

            series_result = build_time_series_analysis(
                engine,
                state["user_id"],
                state["country"],
                metric_name,
                months_full,
                product_match=product_match,
                time_unit=state.get("period_parsed", {}).get("unit")
            )

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

            months_full = _months_from_period_payload(
                engine,
                state,
                state["country"],
                payload,
                default_n=6,
            )

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
                months = _last_n_window(
                    engine,
                    state["user_id"],
                    state["country"],
                    payload.get("n", 6),
                    include_current_incomplete=_payload_includes_current_incomplete(payload),
                )

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

            elif payload.get("type") == "multi_month":
                months = [
                    MonthKey(year=int(item["year"]), month=int(item["month"]))
                    for item in payload.get("months", [])
                    if item.get("month") and item.get("year")
                ]

            else:
                months = _last_n_window(
                    engine,
                    state["user_id"],
                    state["country"],
                    6,
                    include_current_incomplete=_state_includes_current_incomplete(state),
                )

            period_label = f"{months[0].label} to {months[-1].label}" if months else "No data"

            if metric_def.supports_product_breakdown:
                logger.info("[BREAKDOWN_MODE] SKU breakdown")

                combined = _metric_for_exact_month_keys(
                    engine,
                    state,
                    state["country"],
                    metric_name,
                    months,
                )

                state["current_metrics"] = {
                    "metric": metric_name,
                    "period_label": combined.get("period_label") or period_label,
                    "total": combined.get("total", 0.0),
                }

                state["analysis_result"] = {
                    "type": "breakdown",
                    "metric": metric_name,
                    "period_label": combined.get("period_label") or period_label,
                    "per_sku": combined.get("per_sku", []),
                    "total": combined.get("total", 0.0),
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
    "misc_transaction",
}

ANOMALY_QUERY_TERMS = [
    "anomaly",
    "anomalies",
    "abnormal",
    "unusual",
    "outlier",
    "spike",
    "dip",
    "sudden",
    "red flag",
    "red flags",
    "anything wrong",
    "something wrong",
    "what went wrong",
]

ANOMALY_SCAN_METRICS = [
    "profit",
    "net_sales",
    "gross_sales",
    "total_quantity",
    "return_quantity",
    "refund_sales",
    "promotional_rebates",
    "total_cm2_profit",
    "cm2_profit",
    "cm2_profit_per",
    "platform_fee",
    "platformfeenew",
    "platform_fee_inventory_storage",
    "lost_total",
    "selling_fees",
    "fba_fees",
    "amazon_fees",
    "cogs",
    "ads_spend",
    "total_ads",
    "tacos_total_advertising_cost_of_sale",
    "ads_acos",
    "available",
    "days_of_supply",
]

BUSINESS_ANALYSIS_DEFAULT_METRICS = [
    "profit",
    "net_sales",
    "gross_sales",
    "total_quantity",
    "return_quantity",
    "refund_sales",
    "promotional_rebates",
    "total_cm2_profit",
    "cm2_profit",
    "cm2_profit_per",
    "platform_fee",
    "selling_fees",
    "fba_fees",
    "amazon_fees",
    "cogs",
    "ads_spend",
    "tacos_total_advertising_cost_of_sale",
    "ads_acos",
]

# Keep the production metric packs in the semantic layer so request routing,
# diagnosis, anomaly scans, and answer verification share the same meaning.
ANOMALY_SCAN_METRICS = anomaly_scan_metrics()
BUSINESS_ANALYSIS_DEFAULT_METRICS = default_business_analysis_metrics()

ANOMALY_BAD_WHEN_UP_METRICS = {
    *LOWER_IS_BETTER_METRICS,
    "promotional_rebates",
    "promotional_rebates_tax",
    "cogs",
    "marketplace_fees",
    "ads_acos",
    "tacos_total_advertising_cost_of_sale",
}

ANOMALY_PERCENTAGE_METRICS = {
    "profit_percentage",
    "cm2_profit_per",
    "total_cm2_margins",
    "return_rate",
    "ads_acos",
    "tacos_total_advertising_cost_of_sale",
    "ads_ctr",
    "ads_conversion_rate",
}

PRODUCT_SCOPED_METRIC_MAP = {
    "ad_spend": "ads_spend",
    "total_ads": "ads_spend",
    "advertising_total": "ads_spend",
    "advertising": "ads_spend",
    "total_cm2_profit": "cm2_profit",
}


def _needs_product_scoped_metric(state: AgentState) -> bool:
    if state.get("product_query") or state.get("product_queries"):
        return True
    if state.get("dimension") == "sku":
        return True
    if state.get("subject_scope") in {"product", "products", "sku", "skus"}:
        return True
    return bool(state.get("answer_shape") == "ranking" and _ranking_query_targets_products(state))


def _apply_product_scoped_metric_mapping(state: AgentState) -> None:
    if not _needs_product_scoped_metric(state):
        return

    metric_name = state.get("metric_name")
    mapped_metric = PRODUCT_SCOPED_METRIC_MAP.get(str(metric_name or "").strip().lower())
    if mapped_metric and mapped_metric != metric_name:
        logger.info("[PRODUCT_SCOPE_METRIC] metric %s -> %s", metric_name, mapped_metric)
        state["metric_name"] = mapped_metric

    mapped_metrics: List[str] = []
    changed = False
    for metric in state.get("metric_names") or []:
        normalized = str(metric or "").strip().lower()
        mapped = PRODUCT_SCOPED_METRIC_MAP.get(normalized, normalized)
        if mapped != metric:
            changed = True
        if mapped and mapped not in mapped_metrics:
            mapped_metrics.append(mapped)

    if mapped_metrics:
        state["metric_names"] = mapped_metrics
        if changed:
            logger.info("[PRODUCT_SCOPE_METRICS] metrics -> %s", mapped_metrics)


def _is_anomaly_request(state: AgentState) -> bool:
    query = (state.get("user_query") or "").lower()
    analysis_type = str(state.get("analysis_type") or "").lower()
    task_type = str(state.get("task_type") or "").lower()
    answer_shape = str(state.get("answer_shape") or "").lower()
    if any(term in query for term in ANOMALY_QUERY_TERMS):
        return True
    return "anomal" in analysis_type or "anomal" in task_type or "anomal" in answer_shape


def _is_broad_business_analysis_request(state: AgentState) -> bool:
    semantic = state.get("semantic_resolution") or {}
    if semantic.get("is_broad_business_analysis"):
        return True

    query = (state.get("user_query") or "").lower()
    if state.get("metric_name") or state.get("metric_names"):
        return False
    if state.get("intent") in {"chat", "explain", "clarify", "email", "event_planner"}:
        return False
    if state.get("product_query") or state.get("product_queries"):
        return False

    broad_terms = [
        "business",
        "overall",
        "account",
        "performance",
        "health",
        "analyse",
        "analyze",
        "analysis",
        "review",
        "diagnose",
        "compare",
        "comparison",
        "versus",
        " vs ",
        "month over month",
        "mom",
    ]
    analysis_type = state.get("analysis_type")
    answer_shape = state.get("answer_shape")
    if analysis_type in {"comparison", "growth", "trend", "diagnosis", "summary"}:
        return any(term in query for term in broad_terms)
    if answer_shape in {"comparison", "trend", "summary"}:
        return any(term in query for term in broad_terms)
    return False


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
        return get_last_n_month_keys(
            engine,
            state["user_id"],
            country,
            int(payload.get("n") or 6),
            include_current_incomplete=_payload_includes_current_incomplete(payload),
        )

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
        return get_last_n_month_keys(
            engine,
            state["user_id"],
            country,
            n,
            include_current_incomplete=_state_includes_current_incomplete(state),
        )

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
    "expected",
    "expectation",
    "projected",
    "projection",
    "predict",
    "predicted",
    "upcoming",
    "next month",
    "next 2 months",
    "next 3 months",
    "future",
    "future demand",
    "expected demand",
    "demand forecast",
    "future sales",
    "stock should",
    "stock needed",
    "stock need",
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


def _forecast_alignment_summary(
    inventory: Dict[str, Any],
    current_inventory: Dict[str, Any],
    country: str,
) -> Optional[Dict[str, Any]]:
    if not inventory.get("available") or not inventory.get("requested_forecast_available"):
        return None

    requested_col = inventory.get("requested_forecast_column")
    forecast_units_by_month = (inventory.get("totals") or {}).get("forecast_units") or {}
    if requested_col:
        forecast_units = _safe_float(forecast_units_by_month.get(requested_col))
    else:
        forecast_units = sum(_safe_float(value) for value in forecast_units_by_month.values())

    current_available = current_inventory.get("total")
    if current_available is None:
        return {
            "available": False,
            "reason": "current_inventory_missing",
            "forecast_units": forecast_units,
            "forecast_column": requested_col,
        }

    current_available_value = _safe_float(current_available)
    gap = current_available_value - forecast_units
    coverage = _safe_div(current_available_value, forecast_units) if forecast_units else 0.0

    return {
        "available": True,
        "forecast_column": requested_col,
        "forecast_units": forecast_units,
        "forecast_units_formatted": f"{forecast_units:,.0f} units",
        "current_available": current_available_value,
        "current_available_formatted": _format_inventory_value(current_available_value, "available"),
        "current_period_label": current_inventory.get("period_label"),
        "gap": gap,
        "gap_formatted": _format_inventory_value(abs(gap), "available"),
        "coverage_ratio": coverage,
        "status": "covered" if gap >= 0 else "short",
        "dispatch": _safe_float((inventory.get("totals") or {}).get("dispatch")),
        "dispatch_formatted": _format_inventory_value(_safe_float((inventory.get("totals") or {}).get("dispatch")), "available"),
        "inventory_at_month_end": _safe_float((inventory.get("totals") or {}).get("inventory_at_month_end")),
        "current_inventory_plus_dispatch": _safe_float((inventory.get("totals") or {}).get("current_inventory_plus_dispatch")),
        "country": country,
    }


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

        current_inventory = get_inventory_snapshot(
            user_id=state["user_id"],
            metric_name="available",
            month=month or datetime.utcnow().month,
            year=year or datetime.utcnow().year,
            country=country,
        )
        alignment = _forecast_alignment_summary(inventory, current_inventory, country)

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
            "current_inventory": current_inventory,
            "inventory_alignment": alignment,
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


PRICING_INTENT_TERMS = [
    "suggest",
    "recommend",
    "recommendation",
    "target",
    "should",
    "set",
    "profitable",
    "profitability",
    "increase sales",
    "sales increases",
    "sales growth",
    "balance",
    "optimize",
    "optimise",
]


def _is_pricing_recommendation_request(state: AgentState) -> bool:
    query = _normalize(state.get("user_query") or "")
    semantic = state.get("semantic_resolution") or {}
    primary_metric = str(state.get("metric_name") or semantic.get("primary_metric_name") or "").strip().lower()

    asks_price = bool(
        "asp" in query
        or "average selling price" in query
        or re.search(r"\b(price|pricing|selling price)\b", query)
        or primary_metric == "asp"
    )
    if not asks_price:
        return False

    has_recommendation_intent = any(term in query for term in PRICING_INTENT_TERMS)
    is_plain_lookup = (
        state.get("analysis_type") in {"absolute", "trend", "growth", "comparison"}
        and state.get("task_type") in {"value_lookup", "trend_analysis", "comparison"}
        and not has_recommendation_intent
    )
    if is_plain_lookup:
        return False

    return bool(has_recommendation_intent)


def _pricing_find_product_row(rows: List[Dict[str, Any]], product_query: Optional[str]) -> Optional[Dict[str, Any]]:
    if not rows or not product_query:
        return None

    pq = str(product_query).strip().lower()
    exact = [
        row for row in rows
        if pq == str(row.get("product_name") or "").strip().lower()
        or pq == str(row.get("sku") or "").strip().lower()
    ]
    if exact:
        return exact[0]

    contains = [
        row for row in rows
        if pq in str(row.get("product_name") or "").strip().lower()
        or pq in str(row.get("sku") or "").strip().lower()
    ]
    return contains[0] if contains else None


def _pricing_row_matches(row: Dict[str, Any], identity: Dict[str, Any], product_query: Optional[str]) -> bool:
    row_sku = str(row.get("sku") or "").strip().lower()
    row_name = str(row.get("product_name") or "").strip().lower()
    sku = str(identity.get("sku") or "").strip().lower()
    name = str(identity.get("product_name") or "").strip().lower()

    if sku and row_sku == sku:
        return True
    if name and row_name == name:
        return True

    pq = str(product_query or "").strip().lower()
    return bool(pq and (pq == row_sku or pq == row_name))


def _pricing_product_value(
    result: Dict[str, Any],
    identity: Dict[str, Any],
    product_query: Optional[str],
) -> float:
    rows = result.get("per_sku") or []
    matched_rows = [row for row in rows if _pricing_row_matches(row, identity, product_query)]

    if not matched_rows and product_query:
        fallback_row = _pricing_find_product_row(rows, product_query)
        if fallback_row:
            matched_rows = [fallback_row]

    return sum(_safe_float(row.get("__metric__")) for row in matched_rows)


def _pricing_percentile(values: List[float], pct: float) -> float:
    clean = sorted(float(value) for value in values if value is not None)
    if not clean:
        return 0.0
    if len(clean) == 1:
        return clean[0]
    position = (len(clean) - 1) * max(0.0, min(1.0, pct))
    lower = int(position)
    upper = min(lower + 1, len(clean) - 1)
    fraction = position - lower
    return clean[lower] + (clean[upper] - clean[lower]) * fraction


def _pricing_resolve_product_identity(
    engine: Any,
    state: AgentState,
    months: List[MonthKey],
) -> Optional[Dict[str, Any]]:
    product_query = state.get("product_query") or state.get("product_match")
    if not product_query:
        return None

    lookup_months = list(reversed(months or []))
    try:
        latest = latest_available_month(engine, state["user_id"], state["country"])
        lookup_months.append(latest)
    except Exception:
        pass

    seen: set[tuple[int, int]] = set()
    for month_key in lookup_months:
        key = (month_key.year, month_key.month)
        if key in seen:
            continue
        seen.add(key)
        try:
            result = get_metric_for_month(
                engine,
                state["user_id"],
                state["country"],
                "net_sales",
                month_key.month,
                month_key.year,
            )
        except Exception:
            continue

        row = _pricing_find_product_row(result.get("per_sku") or [], product_query)
        if row:
            return {
                "sku": row.get("sku"),
                "product_name": row.get("product_name") or product_query,
            }

    return None


def _pricing_month_record(
    engine: Any,
    state: AgentState,
    month_key: MonthKey,
    identity: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    values: Dict[str, float] = {}
    product_query = state.get("product_query") or identity.get("product_name") or identity.get("sku")

    for metric in [
        "net_sales",
        "profit",
        "cm2_profit",
        "total_quantity",
        "quantity",
        "return_quantity",
        "promotional_rebates",
        "ads_spend",
    ]:
        try:
            result = get_metric_for_month(
                engine,
                state["user_id"],
                state["country"],
                metric,
                month_key.month,
                month_key.year,
            )
            values[metric] = _pricing_product_value(result, identity, product_query)
        except Exception:
            values[metric] = 0.0

    units = values.get("total_quantity", 0.0)
    if units <= 0:
        return None

    net_sales = values.get("net_sales", 0.0)
    profit = values.get("profit", 0.0)
    cm2_profit = values.get("cm2_profit", 0.0)
    promo_rebates = values.get("promotional_rebates", 0.0)

    return {
        "month": month_key.month,
        "year": month_key.year,
        "period_label": month_key.label,
        **values,
        "asp": _safe_div(net_sales, units),
        "cm1_profit_per_unit": _safe_div(profit, units),
        "cm2_profit_per_unit": _safe_div(cm2_profit, units),
        "promo_rebate_per_unit": _safe_div(promo_rebates, units),
        "estimated_cm1_cost_per_unit": _safe_div(net_sales, units) - _safe_div(profit, units),
    }


def _pricing_best_forecast_row(rows: List[Dict[str, Any]], identity: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if not rows:
        return None

    sku = str(identity.get("sku") or "").strip().lower()
    name = str(identity.get("product_name") or "").strip().lower()

    for row in rows:
        if sku and str(row.get("sku") or "").strip().lower() == sku:
            return row
    for row in rows:
        if name and str(row.get("product_name") or "").strip().lower() == name:
            return row
    for row in rows:
        row_name = str(row.get("product_name") or "").strip().lower()
        if name and (name in row_name or row_name in name):
            return row
    return rows[0]


def _pricing_forecast_context(state: AgentState, identity: Dict[str, Any]) -> Dict[str, Any]:
    month, year, period_label = _forecast_period_from_state(state)
    query_candidates = [
        identity.get("sku"),
        identity.get("product_name"),
        state.get("product_query"),
    ]
    query_candidates = [str(item) for item in query_candidates if str(item or "").strip()]
    context: Dict[str, Any] = {
        "period_label": period_label,
        "forecast_units": 0.0,
        "forecast_sales": 0.0,
        "forecast_profit": 0.0,
        "forecast_asp": 0.0,
        "available": False,
    }

    for candidate in query_candidates or [None]:
        try:
            pnl = get_pnl_forecast_snapshot(
                user_id=state["user_id"],
                country=state["country"],
                month=month,
                year=year,
                product_query=candidate,
            )
            row = _pricing_best_forecast_row(pnl.get("rows") or [], identity)
            if row:
                context.update(
                    {
                        "available": True,
                        "source": "pnl_forecast",
                        "forecast_units_source": "P&L forecast forecast_sum",
                        "forecast_filename": pnl.get("filename"),
                        "forecast_units": _safe_float(row.get("forecast_units")),
                        "forecast_sales": _safe_float(row.get("forecast_sales")),
                        "forecast_profit": _safe_float(row.get("forecast_profit")),
                        "forecast_period_label": _forecast_file_period_text(pnl),
                    }
                )
                break
        except Exception:
            logger.debug("[PRICING_FORECAST] P&L forecast unavailable", exc_info=True)

    for candidate in query_candidates or [None]:
        try:
            inventory = get_inventory_forecast_snapshot(
                user_id=state["user_id"],
                country=state["country"],
                month=month,
                year=year,
                product_query=candidate,
            )
            row = _pricing_best_forecast_row(inventory.get("rows") or [], identity)
            if row:
                forecast_units = _safe_float(row.get("requested_forecast_units")) or _safe_float(row.get("projected_sales_total"))
                if forecast_units and not context.get("forecast_units"):
                    context["forecast_units"] = forecast_units
                    context["forecast_units_source"] = (
                        f"inventory forecast {inventory.get('requested_forecast_column')}"
                        if inventory.get("requested_forecast_column")
                        else "inventory forecast Projected Sales Total"
                    )
                context["inventory_forecast_units"] = forecast_units
                context["inventory_forecast_period_label"] = _forecast_file_period_text(inventory)
                context["inventory_forecast_filename"] = inventory.get("filename")
                context["available"] = bool(context.get("available") or forecast_units)
                break
        except Exception:
            logger.debug("[PRICING_FORECAST] Inventory forecast unavailable", exc_info=True)

    if context.get("forecast_units") and context.get("forecast_sales"):
        context["forecast_asp"] = _safe_div(context["forecast_sales"], context["forecast_units"])

    return context


def _compute_pricing_advisor(state: AgentState) -> AgentState:
    engine = state["engine"]
    country = state["country"]
    months = _months_from_period_payload(
        engine,
        state,
        country,
        state.get("period_payload") or {},
        default_n=6,
    )
    identity = _pricing_resolve_product_identity(engine, state, months)

    if not identity:
        state["analysis_result"] = {
            "type": "pricing_advisor",
            "status": "missing_product",
        }
        state["current_metrics"] = {
            "metric": "asp",
            "period_label": _summary_period_label(state, months),
            "total": None,
        }
        return state

    records: List[Dict[str, Any]] = []
    for month_key in months:
        record = _pricing_month_record(engine, state, month_key, identity)
        if record:
            records.append(record)

    if not records:
        state["analysis_result"] = {
            "type": "pricing_advisor",
            "status": "no_sales_history",
            "product": identity,
        }
        state["current_metrics"] = {
            "metric": "asp",
            "period_label": _summary_period_label(state, months),
            "total": None,
        }
        return state

    current = records[-1]
    recent = records[-3:] if len(records) >= 3 else records
    weighted_units = sum(_safe_float(row.get("total_quantity")) for row in recent)
    weighted_sales = sum(_safe_float(row.get("net_sales")) for row in recent)
    weighted_profit = sum(_safe_float(row.get("profit")) for row in recent)
    weighted_cm2 = sum(_safe_float(row.get("cm2_profit")) for row in recent)
    weighted_rebates = sum(_safe_float(row.get("promotional_rebates")) for row in recent)

    weighted_asp = _safe_div(weighted_sales, weighted_units)
    weighted_cm1_unit = _safe_div(weighted_profit, weighted_units)
    weighted_cm2_unit = _safe_div(weighted_cm2, weighted_units)
    weighted_rebate_unit = _safe_div(weighted_rebates, weighted_units)

    unit_values = [_safe_float(row.get("total_quantity")) for row in records if _safe_float(row.get("total_quantity")) > 0]
    median_units = _pricing_percentile(unit_values, 0.5)
    profitable_asps = [
        _safe_float(row.get("asp"))
        for row in records
        if _safe_float(row.get("cm1_profit_per_unit")) > 0 and _safe_float(row.get("asp")) > 0
    ]
    sales_friendly_asps = [
        _safe_float(row.get("asp"))
        for row in records
        if _safe_float(row.get("cm1_profit_per_unit")) > 0
        and _safe_float(row.get("total_quantity")) >= median_units
        and _safe_float(row.get("asp")) > 0
    ]

    current_asp = _safe_float(current.get("asp"))
    current_cm1_unit = _safe_float(current.get("cm1_profit_per_unit"))
    estimated_cm1_cost_unit = max(current_asp - current_cm1_unit, 0.0)
    if weighted_asp > 0 and weighted_cm1_unit:
        estimated_cm1_cost_unit = max((current_asp - current_cm1_unit + weighted_asp - weighted_cm1_unit) / 2, 0.0)

    base_target_cm1_unit = max(current_cm1_unit, weighted_cm1_unit, current_asp * 0.03, 0.01)
    low_target_cm1_unit = max(base_target_cm1_unit * 0.75, current_asp * 0.03)
    high_target_cm1_unit = max(base_target_cm1_unit * 1.15, current_asp * 0.06)

    recommended_low = estimated_cm1_cost_unit + low_target_cm1_unit
    recommended_high = estimated_cm1_cost_unit + high_target_cm1_unit

    if profitable_asps:
        recommended_low = max(recommended_low, min(profitable_asps) * 0.95)
        recommended_high = min(max(recommended_high, recommended_low * 1.04), max(profitable_asps) * 1.08)

    if sales_friendly_asps:
        demand_low = min(sales_friendly_asps) * 0.98
        demand_high = max(sales_friendly_asps) * 1.05
        recommended_low = max(recommended_low, demand_low)
        recommended_high = min(max(recommended_high, recommended_low * 1.04), max(demand_high, recommended_low * 1.04))

    query = _normalize(state.get("user_query") or "")
    if any(term in query for term in ["increase sales", "sales increases", "sales growth", "more sales"]):
        recommended_high = min(recommended_high, max(current_asp * 1.08, recommended_low * 1.04))

    if recommended_high < recommended_low:
        recommended_high = recommended_low * 1.05

    forecast = _pricing_forecast_context(state, identity)
    scenario_units = _safe_float(forecast.get("forecast_units")) or _safe_div(weighted_units, len(recent))
    scenario_units_source = "forecast" if _safe_float(forecast.get("forecast_units")) else "recent_average"

    low_cm1_unit = recommended_low - estimated_cm1_cost_unit
    high_cm1_unit = recommended_high - estimated_cm1_cost_unit

    recommendation = {
        "low_asp": recommended_low,
        "high_asp": recommended_high,
        "estimated_cm1_cost_per_unit": estimated_cm1_cost_unit,
        "low_cm1_profit_per_unit": low_cm1_unit,
        "high_cm1_profit_per_unit": high_cm1_unit,
        "scenario_units": scenario_units,
        "scenario_units_source": scenario_units_source,
        "low_estimated_cm1_profit": low_cm1_unit * scenario_units,
        "high_estimated_cm1_profit": high_cm1_unit * scenario_units,
        "confidence": "medium" if len(records) >= 3 and scenario_units > 0 else "low",
    }

    state["current_metrics"] = {
        "metric": "asp",
        "period_label": _summary_period_label(state, months),
        "total": current_asp,
    }
    state["analysis_result"] = {
        "type": "pricing_advisor",
        "status": "ok",
        "country": country,
        "product": identity,
        "period_label": _summary_period_label(state, months),
        "records": records,
        "current": current,
        "weighted_recent": {
            "asp": weighted_asp,
            "cm1_profit_per_unit": weighted_cm1_unit,
            "cm2_profit_per_unit": weighted_cm2_unit,
            "promo_rebate_per_unit": weighted_rebate_unit,
            "units": weighted_units,
            "net_sales": weighted_sales,
            "profit": weighted_profit,
            "cm2_profit": weighted_cm2,
        },
        "forecast": forecast,
        "recommendation": recommendation,
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


def _months_from_period_payload(
    engine: Any,
    state: AgentState,
    country: str,
    payload: Optional[Dict[str, Any]] = None,
    *,
    default_n: int = 6,
) -> List[MonthKey]:
    payload = payload or state.get("period_payload") or {}
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
        try:
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
        except Exception:
            logger.warning("[PERIOD_MONTHS] No available month tables for range payload=%s", payload)
            return []

    if ptype in {"last_n", "last_n_months"}:
        return _last_n_window(
            engine,
            state["user_id"],
            country,
            int(payload.get("n") or default_n),
            include_current_incomplete=_payload_includes_current_incomplete(payload),
        )

    return _last_n_window(
        engine,
        state["user_id"],
        country,
        default_n,
        include_current_incomplete=_state_includes_current_incomplete(state),
    )


def _summary_months_for_country(engine: Any, state: AgentState, country: str) -> List[MonthKey]:
    return _months_from_period_payload(engine, state, country, default_n=1)


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
            include_current_incomplete=_payload_includes_current_incomplete(payload),
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


def _month_keys_from_period_part(part: Dict[str, Any]) -> List[MonthKey]:
    if not part:
        return []
    try:
        if part.get("type") == "single":
            return [MonthKey(year=int(part["year"]), month=int(part["month"]))]
        if part.get("type") == "range":
            return list(
                iter_months(
                    int(part["start_year"]),
                    int(part["start_month"]),
                    int(part["end_year"]),
                    int(part["end_month"]),
                )
            )
    except Exception:
        logger.debug("[RAW_LINE_ITEMS_PERIOD] Could not parse period part: %s", part)
    return []


def _raw_line_item_months_for_state(state: AgentState) -> List[MonthKey]:
    payload = state.get("period_payload") or {}
    ptype = payload.get("type")
    engine = state["engine"]
    user_id = state["user_id"]
    country = state["country"]

    try:
        if ptype == "single":
            return [MonthKey(year=int(payload["year"]), month=int(payload["month"]))]

        if ptype == "range":
            return list(
                iter_months(
                    int(payload["start_year"]),
                    int(payload["start_month"]),
                    int(payload["end_year"]),
                    int(payload["end_month"]),
                )
            )

        if ptype == "last_n_months":
            return _last_n_window(
                engine,
                user_id,
                country,
                int(payload.get("n") or 1),
                include_current_incomplete=_payload_includes_current_incomplete(payload),
            )

        if ptype == "multi_month":
            months = []
            for item in state.get("target_months") or payload.get("months") or []:
                month = item.get("month")
                year = item.get("year")
                if month and year:
                    months.append(MonthKey(year=int(year), month=int(month)))
            return months

        if ptype == "comparison":
            months = []
            months.extend(_month_keys_from_period_part(payload.get("p1") or {}))
            months.extend(_month_keys_from_period_part(payload.get("p2") or {}))
            unique: Dict[Tuple[int, int], MonthKey] = {}
            for month_key in months:
                unique[(month_key.year, month_key.month)] = month_key
            return sorted(unique.values(), key=lambda item: (item.year, item.month))

        if ptype == "latest_month":
            return [latest_available_month(engine, user_id, country)]
    except Exception:
        logger.exception("[RAW_LINE_ITEMS_PERIOD] Failed to resolve requested period")

    return [latest_available_month(engine, user_id, country)]


def _raw_line_item_amount_sign(state: AgentState) -> str:
    query = _normalize(state.get("user_query") or "")
    credit_terms = ["credit", "credits", "received", "receive", "reimbursement", "reimbursements", "income", "positive"]
    charge_terms = ["charge", "charges", "fee", "fees", "cost", "costs", "paid", "spend", "expense", "expenses", "negative"]
    if any(term in query for term in credit_terms) and not any(term in query for term in charge_terms):
        return "credits"
    if any(term in query for term in charge_terms):
        return "charges"
    return "nonzero"


def _raw_line_item_metric_for_state(state: AgentState) -> str:
    metric_name = normalize_metric_name(state.get("metric_name"))
    if metric_name in RAW_LINE_ITEM_METRICS:
        return str(metric_name)

    for metric in state.get("metric_names") or []:
        normalized = normalize_metric_name(metric)
        if normalized in RAW_LINE_ITEM_METRICS:
            return str(normalized)

    query = _normalize(state.get("user_query") or "")
    if any(term in query for term in ["misc", "miscellaneous"]):
        return "misc_transaction"
    if "other transaction" in query or "other charge" in query:
        return "other_transaction_fees"
    if "promo" in query or "discount" in query or "rebate" in query or "coupon" in query:
        return "promotional_rebates"
    if "ad" in query or "advertising" in query:
        return "total_ads"
    return "misc_transaction"


def _compute_raw_line_items(state: AgentState) -> AgentState:
    metric_name = _raw_line_item_metric_for_state(state)
    months = _raw_line_item_months_for_state(state)
    amount_sign = _raw_line_item_amount_sign(state)
    top_n = min(int(state.get("top_n") or 5), 5)

    result = fetch_raw_line_item_breakdown(
        state["engine"],
        state["user_id"],
        state["country"],
        metric_name,
        months,
        product_query=state.get("product_query"),
        amount_sign=amount_sign,
        top_n=top_n,
        row_limit=top_n,
        skip_missing=True,
    )

    try:
        if result.get("available") and months:
            if len(months) == 1:
                cm2_result = get_metric_for_month(
                    state["engine"],
                    state["user_id"],
                    state["country"],
                    "total_cm2_profit",
                    months[0].month,
                    months[0].year,
                )
                sales_result = get_metric_for_month(
                    state["engine"],
                    state["user_id"],
                    state["country"],
                    "net_sales",
                    months[0].month,
                    months[0].year,
                )
            else:
                ordered_months = sorted(months, key=lambda item: (item.year, item.month))
                cm2_result = get_metric_for_period(
                    state["engine"],
                    state["user_id"],
                    state["country"],
                    "total_cm2_profit",
                    ordered_months[0].month,
                    ordered_months[0].year,
                    ordered_months[-1].month,
                    ordered_months[-1].year,
                    skip_missing=True,
                )
                sales_result = get_metric_for_period(
                    state["engine"],
                    state["user_id"],
                    state["country"],
                    "net_sales",
                    ordered_months[0].month,
                    ordered_months[0].year,
                    ordered_months[-1].month,
                    ordered_months[-1].year,
                    skip_missing=True,
                )

            cm2_total = _safe_float(cm2_result.get("total"))
            net_sales_total = _safe_float(sales_result.get("total"))
            burden = abs(_safe_float(result.get("charges_total"))) if _safe_float(result.get("charges_total")) else abs(_safe_float(result.get("total")))
            result["business_impact"] = {
                "burden": burden,
                "cm2_profit": cm2_total,
                "net_sales": net_sales_total,
                "cm2_percentage": (burden / abs(cm2_total) * 100.0) if cm2_total else None,
                "sales_percentage": (burden / abs(net_sales_total) * 100.0) if net_sales_total else None,
            }
    except Exception:
        logger.exception("[RAW_LINE_ITEMS_IMPACT] Failed to calculate business impact")

    state["current_metrics"] = {
        "metric": metric_name,
        "period_label": result.get("period_label"),
        "total": result.get("total"),
        "row_count": result.get("row_count"),
    }
    state["analysis_result"] = {
        "type": "raw_line_items",
        "metric": metric_name,
        "period_label": result.get("period_label"),
        "country": state.get("country"),
        "result": result,
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
        "anomaly_scan": _compute_anomaly_scan,
        "business_advisor": _compute_business_advisor,
        "decision": _compute_decision,
        "sku_intelligence": _compute_sku_intelligence,
        "sku_trend": _compute_sku_trend,
        "standard_analysis": _compute_standard_analysis,
        "multi_country": _compute_multi_country,
        "forecast_analysis": _compute_forecast_analysis,
        "pricing_advisor": _compute_pricing_advisor,
        "raw_line_items": _compute_raw_line_items,
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
        "profit": "CM1 Profit",
        "profit_percentage": "CM1 Profit Margin",
        "unit_wise_profitability": "CM1 Profit Per Unit",
        "cm2_profit": "Product CM2",
        "total_cm2_profit": "CM2 Profit",
        "cm2_profit_percentage": "CM2 Profit %",
        "cm2_margins": "CM2 Margins",

        # -------- TYPO FIXES --------
        "rembursement_fee": "Reimbursement Fee",
        "rembursment_vs_cm2_margins": "Reimbursement vs CM2 Margins",
        "dealsvouchar_ads": "Deals Voucher Ads",
        "tex_and_credits": "Tax and Credits",
        "platformfeenew": "Subscription Fees",

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
        "total_quantity": "Net Sold Units",
        "quantity": "Gross Units",
        "return_quantity": "Refund Quantity",
        "net_sales": "Net Sales",
        "gross_sales": "Gross Sales",
        "refund_sales": "Refund Sales",
        "product_sales": "Product Sales",
        "promotional_rebates": "Promo Rebates",

        # -------- FEES --------
        "platform_fee_inventory_storage": "Platform Fee Inventory Storage",
        "lost_total": "Lost Inventory Reimbursement",
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
        "profit_mix": "CM1 Profit Mix",
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


PERCENTAGE_DISPLAY_METRICS = {
    "profit_percentage",
    "cm2_profit_per",
    "total_cm2_margins",
    "return_rate",
    "return_rate_pct",
    "ads_acos",
    "acos",
    "tacos_total_advertising_cost_of_sale",
    "ads_ctr",
    "ctr_pct",
    "ads_conversion_rate",
    "ad_conversion_rate_pct",
}


BURDEN_DISPLAY_METRICS = {
    "promotional_rebates",
    "promotional_rebates_tax",
    "refund_sales",
    "return_quantity",
    "return_rate",
    "cogs",
    "selling_fees",
    "fba_fees",
    "marketplace_fees",
    "amazon_fees",
    "platform_fee",
    "platformfeenew",
    "platform_fee_inventory_storage",
    "tax_and_credits",
    "other",
    "ads_spend",
    "total_ads",
    "product_spend",
    "display_spend",
    "brand_spend",
    "ads_acos",
    "tacos_total_advertising_cost_of_sale",
    "ads_cpc",
    "misc_transaction",
    "debt_payment",
}


SIGN_AWARE_BURDEN_DISPLAY_METRICS = {
    "promotional_rebates",
    "promotional_rebates_tax",
}


def _display_burden_delta(metric: str, left_value: float, right_value: float) -> float:
    if metric in SIGN_AWARE_BURDEN_DISPLAY_METRICS:
        return (-left_value) - (-right_value)
    return abs(left_value) - abs(right_value)


COMPARISON_CHANGE_GROUPS = [
    (
        "Demand",
        [
            ("total_quantity", "net sold units"),
            ("return_quantity", "refund quantity"),
        ],
    ),
    (
        "Sales",
        [
            ("gross_sales", "gross sales"),
            ("net_sales", "net sales"),
            ("product_sales", "product sales"),
        ],
    ),
    (
        "Margin",
        [
            ("cm2_profit", "product CM2"),
            ("total_cm2_profit", "total CM2"),
            ("cm2_profit_per", "CM2 margin"),
            ("total_cm2_margins", "total CM2 margin"),
            ("profit_percentage", "CM1 profit margin"),
        ],
    ),
    (
        "Costs and discounts",
        [
            ("promotional_rebates", "promo discount burden"),
            ("platform_fee", "platform fees"),
            ("platformfeenew", "subscription fees"),
            ("platform_fee_inventory_storage", "storage fees"),
            ("selling_fees", "selling fees"),
            ("fba_fees", "FBA fees"),
            ("amazon_fees", "Amazon fees"),
            ("cogs", "COGS"),
        ],
    ),
    (
        "Ads",
        [
            ("ads_spend", "ad spend"),
            ("product_spend", "SP spend"),
            ("display_spend", "SD spend"),
            ("brand_spend", "SB spend"),
            ("tacos_total_advertising_cost_of_sale", "TACOS"),
            ("ads_acos", "ACOS"),
            ("ads_roas", "ad ROAS"),
            ("ads_sale_amount", "ad sales"),
        ],
    ),
    (
        "Returns",
        [
            ("refund_sales", "refunds"),
            ("return_rate", "return rate"),
        ],
    ),
]


def _format_metric_for_display(value: Any, metric_name: str, country: Optional[str]) -> str:
    if metric_name in INVENTORY_METRICS:
        return _format_inventory_value(value, metric_name)
    if metric_name in PERCENTAGE_DISPLAY_METRICS:
        return f"{float(value or 0.0):.2f}%"
    return _format_value(float(value or 0.0), metric_name, country)


def _format_signed_metric_value(value: Any, metric_name: str, country: Optional[str]) -> str:
    numeric_value = _safe_float(value)
    sign = "-" if numeric_value < 0 else ""
    return f"{sign}{_format_metric_for_display(abs(numeric_value), metric_name, country)}"


def _first_product(rows: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    return rows[0] if rows else None


def _format_driver_change_value(state: AgentState, driver: Dict[str, Any]) -> str:
    metric = str(driver.get("metric") or "")
    raw_value = driver.get("business_delta")
    if raw_value is None:
        raw_value = driver.get("delta")

    try:
        value = abs(float(raw_value or 0.0))
    except Exception:
        return str(raw_value)

    if metric in PERCENTAGE_DISPLAY_METRICS:
        return f"{value:.2f} pts"
    return _format_metric_for_display(value, metric, state.get("country"))


def _render_metric_driver_line(state: AgentState, driver: Dict[str, Any]) -> str:
    label = driver.get("label") or humanize_metric(driver.get("metric") or "")
    direction = driver.get("direction") or "changed"
    change_text = _format_driver_change_value(state, driver)
    suffix = " burden" if driver.get("value_basis") == "absolute burden" else ""
    return f"- **{label}** {direction}{suffix} by **{change_text}**."


def _driver_action(driver: Dict[str, Any]) -> str:
    metric = str(driver.get("metric") or "").strip().lower()
    label = driver.get("label") or humanize_metric(metric)

    if metric in {"net_sales", "gross_sales", "product_sales"}:
        return "Check the SKUs with the biggest sales drop and review stock, Buy Box, listing conversion, and pricing."
    if metric in {"total_quantity", "quantity"}:
        return "Find which SKUs lost units first; lower units usually means demand, stock, conversion, or price friction."
    if metric == "asp":
        return "Review price and discount mix by SKU; ASP movement can hide whether volume or discounting caused the change."
    if metric in {"promotional_rebates", "promotional_rebates_tax"}:
        return "Audit coupons, deals, and rebates; stop discounts that did not increase net sold units or protect margin."
    if metric in {"platform_fee", "platformfeenew", "platform_fee_inventory_storage", "amazon_fees", "selling_fees", "fba_fees", "marketplace_fees"}:
        return f"Reconcile the {label.lower()} spike by transaction/SKU and check for one-off charges or fee rule changes."
    if metric in {"ads_spend", "product_spend", "brand_spend", "display_spend", "ads_acos", "tacos_total_advertising_cost_of_sale", "ads_cpc"}:
        return "Review campaigns where spend rose while sales or profit fell; reduce wasted spend before scaling."
    if metric in {"refund_sales", "return_quantity", "return_rate"}:
        return "Check returned SKUs and reasons; returns can erase margin even when orders look healthy."
    if metric == "cogs":
        return "Check landed cost and purchase cost changes for the SKUs with the largest profit drop."
    return f"Drill into {label.lower()} by SKU to confirm whether it is recurring or a one-off issue."


def _driver_family(metric: str) -> str:
    metric = str(metric or "").strip().lower()
    if metric in {"quantity", "total_quantity"}:
        return "units"
    if metric in {"cm2_profit", "total_cm2_profit"}:
        return "cm2_profit"
    if metric in {"ads_spend", "total_ads", "product_spend", "brand_spend", "display_spend"}:
        return "ads"
    if metric in {"platform_fee", "platformfeenew", "platform_fee_inventory_storage", "amazon_fees", "selling_fees", "fba_fees", "marketplace_fees"}:
        return "fees"
    return metric


def _combined_driver_lines(state: AgentState, ranked_drivers: List[Dict[str, Any]], limit: int = 4) -> List[str]:
    drivers = [driver for driver in ranked_drivers if isinstance(driver, dict)]
    used_families: set[str] = set()
    lines: List[str] = []

    ordered_units = next((driver for driver in drivers if driver.get("metric") == "quantity"), None)
    sold_units = next((driver for driver in drivers if driver.get("metric") == "total_quantity"), None)
    if ordered_units or sold_units:
        parts = []
        if sold_units:
            parts.append(f"net sold {_format_driver_change_value(state, sold_units)}")
        if ordered_units and not sold_units:
            parts.append(f"gross {_format_driver_change_value(state, ordered_units)}")
        lines.append(f"- **Units changed**: {', '.join(parts)}.")
        used_families.add("units")

    for driver in drivers:
        family = _driver_family(str(driver.get("metric") or ""))
        if family in used_families:
            continue
        lines.append(_render_metric_driver_line(state, driver))
        used_families.add(family)
        if len(lines) >= limit:
            break

    return lines


def _driver_lookup(comparison_context: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    lookup: Dict[str, Dict[str, Any]] = {}
    for group_name in ["unfavorable_metric_drivers", "favorable_metric_drivers", "metric_drivers"]:
        for driver in comparison_context.get(group_name) or []:
            if not isinstance(driver, dict):
                continue
            metric = str(driver.get("metric") or "").strip().lower()
            if metric and metric not in lookup:
                lookup[metric] = driver
    return lookup


def _driver_change_amount(driver: Dict[str, Any]) -> float:
    raw_value = driver.get("business_delta")
    if raw_value is None:
        raw_value = driver.get("delta")
    try:
        return float(raw_value or 0.0)
    except Exception:
        return 0.0


def _driver_change_phrase(state: AgentState, driver: Dict[str, Any], label: str) -> str:
    metric = str(driver.get("metric") or "").strip().lower()
    change = _driver_change_amount(driver)
    if abs(change) < 0.005:
        return ""

    if driver.get("value_basis") == "absolute burden" or metric in BURDEN_DISPLAY_METRICS:
        direction = "burden up" if change > 0 else "burden down"
        pct = driver.get("business_pct_change")
    else:
        direction = "up" if change > 0 else "down"
        pct = driver.get("pct_change")

    pct_text = ""
    try:
        if pct is not None and abs(float(pct)) >= 1:
            pct_text = f" ({float(pct):+.1f}%)"
    except Exception:
        pct_text = ""

    return f"{label} {direction} **{_format_driver_change_value(state, driver)}**{pct_text}"


def _render_change_group_lines(state: AgentState, comparison_context: Dict[str, Any], limit: int = 6) -> List[str]:
    lookup = _driver_lookup(comparison_context)
    lines: List[str] = []
    shown_metrics: set[str] = set()

    for title, metric_specs in COMPARISON_CHANGE_GROUPS:
        parts: List[str] = []
        for metric, label in metric_specs:
            driver = lookup.get(metric)
            if not driver:
                continue
            phrase = _driver_change_phrase(state, driver, label)
            if not phrase:
                continue
            parts.append(phrase)
            shown_metrics.add(metric)
            if len(parts) >= 3:
                break
        if parts:
            lines.append(f"- **{title}:** {'; '.join(parts)}.")
        if len(lines) >= limit:
            break

    if len(lines) < limit:
        for driver in comparison_context.get("metric_drivers") or []:
            if not isinstance(driver, dict):
                continue
            metric = str(driver.get("metric") or "").strip().lower()
            if metric == "quantity":
                continue
            if not metric or metric in shown_metrics:
                continue
            phrase = _driver_change_phrase(state, driver, str(driver.get("label") or humanize_metric(metric)))
            if phrase:
                lines.append(f"- **Other signal:** {phrase}.")
                shown_metrics.add(metric)
            if len(lines) >= limit:
                break

    return lines


COMPARISON_TABLE_DEFAULT_ORDER = [
    "profit",
    "net_sales",
    "gross_sales",
    "total_quantity",
    "quantity",
    "asp",
    "profit_percentage",
    "total_cm2_profit",
    "cm2_profit",
    "cm2_profit_per",
    "total_cm2_margins",
    "promotional_rebates",
    "refund_sales",
    "return_quantity",
    "return_rate",
    "ads_spend",
    "total_ads",
    "product_spend",
    "display_spend",
    "brand_spend",
    "ads_sale_amount",
    "ads_roas",
    "ads_acos",
    "tacos_total_advertising_cost_of_sale",
    "platform_fee",
    "platformfeenew",
    "platform_fee_inventory_storage",
    "lost_total",
    "selling_fees",
    "fba_fees",
    "amazon_fees",
    "cogs",
    "misc_transaction",
    "other",
    "available",
    "inbound_quantity",
    "days_of_supply",
]

COMPARISON_TABLE_GOOD_WHEN_UP = {
    "net_sales",
    "gross_sales",
    "product_sales",
    "total_quantity",
    "quantity",
    "asp",
    "profit",
    "profit_percentage",
    "cm2_profit",
    "total_cm2_profit",
    "cm2_profit_per",
    "total_cm2_margins",
    "ads_sale_amount",
    "ads_sale_units",
    "ads_roas",
    "lost_total",
    "ads_ctr",
    "ads_conversion_rate",
    "available",
    "inbound_quantity",
    "days_of_supply",
}

CM1_SECONDARY_ONLY_METRICS = {
    "ads_spend",
    "total_ads",
    "product_spend",
    "display_spend",
    "brand_spend",
    "ads_acos",
    "tacos_total_advertising_cost_of_sale",
    "ads_cpc",
    "platform_fee",
    "platformfeenew",
    "platform_fee_inventory_storage",
    "cm2_profit",
    "total_cm2_profit",
    "cm2_profit_per",
    "total_cm2_margins",
    "lost_total",
}

COMPARISON_TABLE_ALIASES = {
    "advertising_total": "ads_spend",
    "ad_spend": "ads_spend",
    "total_ads": "ads_spend",
    "amazon_fee": "amazon_fees",
    "refund_quantity": "return_quantity",
}

COMPARISON_TABLE_MAX_ROWS = 9

NET_REPLACEMENT_METRICS = {
    "gross_sales": "net_sales",
    "quantity": "total_quantity",
}

REFUND_IMPACT_METRICS = {"refund_sales", "return_quantity", "return_rate"}

SALES_DEPENDENT_FEE_METRICS = {"selling_fees", "fba_fees"}

INVENTORY_CONTEXT_METRICS = {"available", "inbound_quantity", "days_of_supply"}

SALES_DEPENDENT_FEE_PCT_BUFFER = 10.0
SALES_DEPENDENT_FEE_RATIO_BUFFER = 1.5
REFUND_IMPACT_MIN_PRIMARY_RATIO = 0.10
FEE_IMPACT_MIN_PRIMARY_RATIO = 0.05
LOW_DAYS_OF_SUPPLY_THRESHOLD = 21.0


def _canonical_comparison_metric(metric: Any) -> str:
    key = str(metric or "").strip().lower()
    return COMPARISON_TABLE_ALIASES.get(key, key)


def _resolve_available_comparison_metric(metric: Any, available: Dict[str, Any]) -> str:
    raw = str(metric or "").strip().lower()
    canonical = _canonical_comparison_metric(raw)
    if canonical in available:
        return canonical
    if raw in available:
        return raw
    return canonical


def _period_label_sort_key(label: Any) -> Optional[Tuple[int, int]]:
    text = str(label or "")
    month_pattern = (
        r"\b(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|"
        r"jul(?:y)?|aug(?:ust)?|sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)"
        r"[\s'/-]*(20\d{2}|\d{2})\b"
    )
    month_lookup = {
        "jan": 1,
        "feb": 2,
        "mar": 3,
        "apr": 4,
        "may": 5,
        "jun": 6,
        "jul": 7,
        "aug": 8,
        "sep": 9,
        "oct": 10,
        "nov": 11,
        "dec": 12,
    }
    match = re.search(month_pattern, text, re.IGNORECASE)
    if match:
        month = month_lookup[match.group(1)[:3].lower()]
        year = int(match.group(2))
        if year < 100:
            year += 2000
        return year, month

    quarter = re.search(r"\bq(?:uarter)?\s*([1-4])[\s'/-]*(20\d{2}|\d{2})\b", text, re.IGNORECASE)
    if quarter:
        year = int(quarter.group(2))
        if year < 100:
            year += 2000
        return year, ((int(quarter.group(1)) - 1) * 3) + 1

    year_match = re.search(r"\b(20\d{2})\b", text)
    if year_match:
        return int(year_match.group(1)), 1

    return None


def _ordered_period_labels(
    left_label: Any,
    right_label: Any,
) -> Tuple[str, str, str, str]:
    left = _clean_period_label(left_label) or "Period 1"
    right = _clean_period_label(right_label) or "Period 2"
    left_key = _period_label_sort_key(left)
    right_key = _period_label_sort_key(right)
    if left_key and right_key and left_key > right_key:
        return "right", right, "left", left
    return "left", left, "right", right


def _period_metric_value(comp: Dict[str, Any], side: str) -> float:
    return _safe_float(comp.get(side))


def _format_period_metric_value(value: Any, metric_name: str, country: Optional[str]) -> str:
    if metric_name == "lost_total":
        return _format_metric_for_display(abs(_safe_float(value)), metric_name, country)
    return _format_signed_metric_value(value, metric_name, country)


def _metric_burden_value(metric_name: str, value: float) -> float:
    if metric_name in SIGN_AWARE_BURDEN_DISPLAY_METRICS:
        return -value
    if metric_name in BURDEN_DISPLAY_METRICS:
        return abs(value)
    return value


def _comparison_business_delta(metric_name: str, first_value: float, second_value: float) -> float:
    if metric_name == "lost_total":
        return abs(second_value) - abs(first_value)
    if metric_name in BURDEN_DISPLAY_METRICS:
        return _metric_burden_value(metric_name, second_value) - _metric_burden_value(metric_name, first_value)
    return second_value - first_value


def _format_comparison_change_amount(metric_name: str, delta: float, country: Optional[str]) -> str:
    if metric_name in PERCENTAGE_DISPLAY_METRICS:
        return f"{abs(delta):.2f} pts"
    return _format_metric_for_display(abs(delta), metric_name, country)


def _comparison_effect(metric_name: str, business_delta: float) -> str:
    if abs(business_delta) < 0.005:
        return "neutral"
    if metric_name == "lost_total":
        return "favorable" if business_delta > 0 else "unfavorable"
    if metric_name in BURDEN_DISPLAY_METRICS:
        return "unfavorable" if business_delta > 0 else "favorable"
    if metric_name in COMPARISON_TABLE_GOOD_WHEN_UP:
        return "favorable" if business_delta > 0 else "unfavorable"
    return "neutral"


def _format_comparison_change_text(
    metric_name: str,
    first_value: float,
    second_value: float,
    country: Optional[str],
) -> Tuple[str, float, Optional[float], str]:
    business_delta = _comparison_business_delta(metric_name, first_value, second_value)
    if abs(business_delta) < 0.005:
        return "Flat", business_delta, 0.0, "neutral"

    if metric_name == "lost_total":
        base = abs(first_value)
        direction = f"Recovery {'up' if business_delta > 0 else 'down'}"
    elif metric_name in BURDEN_DISPLAY_METRICS:
        base = abs(_metric_burden_value(metric_name, first_value))
        direction = "Discount burden" if metric_name in SIGN_AWARE_BURDEN_DISPLAY_METRICS else "Burden"
        direction = f"{direction} {'up' if business_delta > 0 else 'down'}"
    else:
        base = abs(first_value)
        direction = "Up" if business_delta > 0 else "Down"

    pct_change = None if base < 0.005 else (business_delta / base) * 100.0
    amount = _format_comparison_change_amount(metric_name, business_delta, country)
    pct_suffix = "" if pct_change is None else f" ({pct_change:+.1f}%)"
    return f"{direction} {amount}{pct_suffix}", business_delta, pct_change, _comparison_effect(metric_name, business_delta)


def _comparison_requested_metric_set(
    available: Dict[str, Any],
    metrics: Iterable[Any],
) -> set:
    return {
        key for key in (_resolve_available_comparison_metric(metric, available) for metric in metrics)
        if key
    }


def _comparison_user_requested_metric_candidates(
    state: AgentState,
    primary_metric: Optional[str] = None,
    extra_metrics: Iterable[Any] = (),
) -> List[Any]:
    semantic = state.get("semantic_resolution") or {}
    broad_evidence_mode = bool(
        semantic.get("is_broad_business_analysis")
        or semantic.get("needs_anomaly_scan")
    )
    scope = (state.get("business_context") or {}).get("scope") or {}
    candidates: List[Any] = [
        primary_metric,
        state.get("metric_name"),
        semantic.get("primary_metric_name"),
        scope.get("metric_name"),
        *extra_metrics,
    ]
    if not broad_evidence_mode:
        candidates.extend(state.get("metric_names") or [])
        candidates.extend(scope.get("metric_names") or [])
    return candidates


def _comparison_row_lookup(rows: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    return {
        str(row.get("metric") or ""): row
        for row in rows
        if row.get("metric")
    }


def _comparison_row_is_material(
    row: Dict[str, Any],
    primary_row: Optional[Dict[str, Any]],
    *,
    min_primary_ratio: float,
) -> bool:
    delta = abs(_safe_float(row.get("business_delta")))
    if delta < 0.005:
        return False

    if not primary_row:
        return True

    primary_delta = abs(_safe_float(primary_row.get("business_delta")))
    if primary_delta < 0.005:
        return True
    return delta >= primary_delta * min_primary_ratio


def _sales_dependent_fee_is_anomalous(
    row: Dict[str, Any],
    row_lookup: Dict[str, Dict[str, Any]],
) -> bool:
    fee_delta = _safe_float(row.get("business_delta"))
    if abs(fee_delta) < 0.005:
        return False

    net_sales = row_lookup.get("net_sales")
    if not net_sales:
        return fee_delta > 0

    sales_delta = _safe_float(net_sales.get("business_delta"))
    fee_pct = abs(_safe_float(row.get("pct_change")))
    sales_pct = abs(_safe_float(net_sales.get("pct_change")))

    if sales_delta < -0.005:
        return fee_delta > 0.005

    if sales_delta > 0.005:
        return fee_delta > 0.005 and fee_pct > max(
            sales_pct * SALES_DEPENDENT_FEE_RATIO_BUFFER,
            sales_pct + SALES_DEPENDENT_FEE_PCT_BUFFER,
        )

    return fee_delta > 0.005


def _inventory_context_is_actionable(
    row: Dict[str, Any],
    row_lookup: Dict[str, Dict[str, Any]],
) -> bool:
    metric_name = str(row.get("metric") or "")
    second_value = _safe_float(row.get("second_value"))
    if metric_name == "days_of_supply":
        return 0 < second_value < LOW_DAYS_OF_SUPPLY_THRESHOLD
    if metric_name == "available":
        days_row = row_lookup.get("days_of_supply")
        days_value = _safe_float(days_row.get("second_value")) if days_row else 0.0
        return second_value <= 0 or (0 < days_value < LOW_DAYS_OF_SUPPLY_THRESHOLD)
    if metric_name == "inbound_quantity":
        available_row = row_lookup.get("available")
        days_row = row_lookup.get("days_of_supply")
        available_value = _safe_float(available_row.get("second_value")) if available_row else 0.0
        days_value = _safe_float(days_row.get("second_value")) if days_row else 0.0
        return available_value <= 0 or (0 < days_value < LOW_DAYS_OF_SUPPLY_THRESHOLD)
    return False


def _comparison_row_is_relevant(
    row: Dict[str, Any],
    row_lookup: Dict[str, Dict[str, Any]],
    requested: set,
    primary_metric: Optional[str],
) -> bool:
    metric_name = str(row.get("metric") or "")
    if not metric_name:
        return False

    if primary_metric == "profit" and metric_name in CM1_SECONDARY_ONLY_METRICS:
        return False

    if metric_name == primary_metric or metric_name in requested:
        return True

    replacement = NET_REPLACEMENT_METRICS.get(metric_name)
    if replacement and replacement in row_lookup:
        return False

    primary_row = row_lookup.get(str(primary_metric or ""))

    if metric_name in INVENTORY_CONTEXT_METRICS:
        return _inventory_context_is_actionable(row, row_lookup)

    if metric_name in REFUND_IMPACT_METRICS:
        return (
            row.get("effect") == "unfavorable"
            and _comparison_row_is_material(
                row,
                primary_row,
                min_primary_ratio=REFUND_IMPACT_MIN_PRIMARY_RATIO,
            )
        )

    if metric_name in SALES_DEPENDENT_FEE_METRICS:
        return (
            _sales_dependent_fee_is_anomalous(row, row_lookup)
            and _comparison_row_is_material(
                row,
                primary_row,
                min_primary_ratio=FEE_IMPACT_MIN_PRIMARY_RATIO,
            )
        )

    return True


def _filter_comparison_table_rows(
    rows: List[Dict[str, Any]],
    requested: set,
    primary_metric: Optional[str],
    *,
    limit: int,
) -> List[Dict[str, Any]]:
    row_lookup = _comparison_row_lookup(rows)
    filtered = [
        row for row in rows
        if _comparison_row_is_relevant(row, row_lookup, requested, primary_metric)
    ]
    if filtered:
        return filtered[:limit]
    return rows[:limit]


def _comparison_row_plain_change(row: Dict[str, Any]) -> str:
    metric_name = str(row.get("metric") or "")
    label = str(row.get("label") or humanize_metric(metric_name))
    delta = _safe_float(row.get("business_delta"))
    amount = str(row.get("change_amount") or "").strip()
    if not amount:
        amount = str(row.get("change_text") or "").strip()
    if abs(delta) < 0.005:
        return f"**{label}** stayed flat"
    if metric_name == "lost_total":
        verb = "increased" if delta > 0 else "decreased"
        return f"**{label}** recovery {verb} by **{amount}**"
    if metric_name in BURDEN_DISPLAY_METRICS:
        verb = "increased" if delta > 0 else "decreased"
        return f"**{label}** burden {verb} by **{amount}**"
    verb = "rose" if delta > 0 else "fell"
    return f"**{label}** {verb} by **{amount}**"


def _comparison_row_by_metric(rows: List[Dict[str, Any]], *metric_names: str) -> Optional[Dict[str, Any]]:
    wanted = {str(metric or "") for metric in metric_names if metric}
    return next((row for row in rows if str(row.get("metric") or "") in wanted), None)


def _comparison_change_amount(row: Optional[Dict[str, Any]]) -> str:
    if not row:
        return ""
    amount = str(row.get("change_amount") or "").strip()
    return amount or str(row.get("change_text") or "").strip()


def _cm1_profit_comparison_summary(
    rows: List[Dict[str, Any]],
    primary: Dict[str, Any],
    first_label: str,
    second_label: str,
) -> Optional[str]:
    if primary.get("metric") != "profit":
        return None

    profit_delta = _safe_float(primary.get("business_delta"))
    profit_amount = _comparison_change_amount(primary)
    if not profit_amount:
        return None

    net_sales = _comparison_row_by_metric(rows, "net_sales")
    net_units = _comparison_row_by_metric(rows, "total_quantity")
    asp = _comparison_row_by_metric(rows, "asp")
    promo = _comparison_row_by_metric(rows, "promotional_rebates")

    if profit_delta < -0.005:
        reason_parts: List[str] = []
        if net_units and _safe_float(net_units.get("business_delta")) < -0.005:
            reason_parts.append(f"**Net Sold Units** fell by **{_comparison_change_amount(net_units)}**")
        if net_sales and _safe_float(net_sales.get("business_delta")) < -0.005:
            reason_parts.append(f"**Net Sales** fell by **{_comparison_change_amount(net_sales)}**")

        if reason_parts:
            summary = (
                f"CM1 Profit **fell by {profit_amount}** from **{first_label}** to **{second_label}** "
                f"because " + " and ".join(reason_parts) + "."
            )
        else:
            summary = f"CM1 Profit **fell by {profit_amount}** from **{first_label}** to **{second_label}**."

        offsets: List[str] = []
        if asp and _safe_float(asp.get("business_delta")) > 0.005:
            offsets.append(f"ASP improved by **{_comparison_change_amount(asp)}**, but not enough to offset lower sales")
        if promo and promo.get("effect") == "unfavorable" and abs(_safe_float(promo.get("business_delta"))) > 0.005:
            offsets.append(f"promo rebate burden also increased by **{_comparison_change_amount(promo)}**")
        if offsets:
            summary += " " + "; ".join(offsets) + "."
        return summary

    if profit_delta > 0.005:
        driver_parts: List[str] = []
        if net_sales and _safe_float(net_sales.get("business_delta")) > 0.005:
            driver_parts.append(f"**Net Sales** rose by **{_comparison_change_amount(net_sales)}**")
        if net_units and _safe_float(net_units.get("business_delta")) > 0.005:
            driver_parts.append(f"**Net Sold Units** rose by **{_comparison_change_amount(net_units)}**")
        if asp and _safe_float(asp.get("business_delta")) > 0.005:
            driver_parts.append(f"**ASP** improved by **{_comparison_change_amount(asp)}**")

        summary = f"CM1 Profit **improved by {profit_amount}** from **{first_label}** to **{second_label}**."
        if driver_parts:
            summary += " Main reason: " + " and ".join(driver_parts[:2]) + "."
        return summary

    return None


CM1_PROFIT_SUMMARY_DRIVER_PRIORITY = {
    "net_sales": 0,
    "total_quantity": 1,
    "asp": 2,
    "profit_percentage": 3,
    "promotional_rebates": 4,
    "refund_sales": 5,
    "return_quantity": 6,
    "amazon_fees": 7,
    "cogs": 8,
    "misc_transaction": 9,
    "other": 10,
}


def _comparison_summary_driver_sort_key(
    row: Dict[str, Any],
    primary_metric: Optional[str],
) -> Tuple[float, float, float]:
    metric_name = str(row.get("metric") or "")
    delta_weight = -abs(_safe_float(row.get("business_delta")))
    pct_weight = -abs(_safe_float(row.get("pct_change")))
    if primary_metric == "profit":
        return (
            float(CM1_PROFIT_SUMMARY_DRIVER_PRIORITY.get(metric_name, 50)),
            delta_weight,
            pct_weight,
        )
    return (0.0, delta_weight, pct_weight)


def _comparison_metric_order(
    state: AgentState,
    comparison_context: Dict[str, Any],
    primary_metric: Optional[str],
) -> List[str]:
    metrics = comparison_context.get("metrics") or {}
    ordered: List[str] = []

    def add(metric: Any) -> None:
        key = _resolve_available_comparison_metric(metric, metrics)
        if key and key in metrics and key not in ordered:
            ordered.append(key)

    add(primary_metric)
    add(state.get("metric_name"))
    for metric in state.get("metric_names") or []:
        add(metric)
    scope = (state.get("business_context") or {}).get("scope") or {}
    add(scope.get("metric_name"))
    for metric in scope.get("metric_names") or []:
        add(metric)

    for group in ["unfavorable_metric_drivers", "favorable_metric_drivers", "metric_drivers"]:
        for driver in comparison_context.get(group) or []:
            if isinstance(driver, dict):
                add(driver.get("metric"))

    for metric in COMPARISON_TABLE_DEFAULT_ORDER:
        add(metric)
    for metric in metrics:
        add(metric)

    return ordered


def _select_primary_comparison_metric(
    state: AgentState,
    analysis: Dict[str, Any],
    comparison_context: Dict[str, Any],
) -> Optional[str]:
    metrics = comparison_context.get("metrics") or {}
    candidates = [
        ((comparison_context.get("scope") or {}).get("metric_name")),
        ((analysis.get("context") or {}).get("scope") or {}).get("metric_name"),
        analysis.get("metric_name"),
        state.get("metric_name"),
        *((analysis.get("metric_names") or [])),
        *((state.get("metric_names") or [])),
        "total_cm2_profit",
        "profit",
        "net_sales",
    ]
    for candidate in candidates:
        key = _resolve_available_comparison_metric(candidate, metrics)
        if key in metrics:
            return key
    return next(iter(metrics.keys()), None) if metrics else None


def _build_comparison_table_rows(
    state: AgentState,
    comparison_context: Dict[str, Any],
    primary_metric: Optional[str],
    *,
    limit: int = COMPARISON_TABLE_MAX_ROWS,
) -> Tuple[List[Dict[str, Any]], str, str]:
    metrics = comparison_context.get("metrics") or {}
    left = comparison_context.get("left") or {}
    right = comparison_context.get("right") or {}
    first_side, first_label, second_side, second_label = _ordered_period_labels(left.get("label"), right.get("label"))
    rows: List[Dict[str, Any]] = []
    requested = _comparison_requested_metric_set(
        metrics,
        _comparison_user_requested_metric_candidates(state, primary_metric),
    )

    for metric_name in _comparison_metric_order(state, comparison_context, primary_metric):
        comp = metrics.get(metric_name)
        if not isinstance(comp, dict):
            continue
        first_value = _period_metric_value(comp, first_side)
        second_value = _period_metric_value(comp, second_side)
        if metric_name not in requested and abs(first_value) < 0.005 and abs(second_value) < 0.005:
            continue

        change_text, business_delta, pct_change, effect = _format_comparison_change_text(
            metric_name,
            first_value,
            second_value,
            state.get("country"),
        )
        rows.append(
            {
                "metric": metric_name,
                "label": humanize_metric(metric_name),
                "first_value": first_value,
                "second_value": second_value,
                "first_formatted": _format_period_metric_value(first_value, metric_name, state.get("country")),
                "second_formatted": _format_period_metric_value(second_value, metric_name, state.get("country")),
                "change_text": change_text,
                "change_amount": _format_comparison_change_amount(metric_name, business_delta, state.get("country")),
                "business_delta": business_delta,
                "pct_change": pct_change,
                "effect": effect,
            }
        )
    return _filter_comparison_table_rows(rows, requested, primary_metric, limit=limit), first_label, second_label


def _comparison_table_summary(
    rows: List[Dict[str, Any]],
    primary_metric: Optional[str],
    first_label: str,
    second_label: str,
) -> str:
    if not rows:
        return "The selected periods were compared, but no material metric movement was found."

    primary = next((row for row in rows if row.get("metric") == primary_metric), None) or rows[0]
    primary_delta = _safe_float(primary.get("business_delta"))
    if abs(primary_delta) < 0.005:
        return f"From **{first_label}** to **{second_label}**, **{primary.get('label')}** was broadly flat."

    cm1_summary = _cm1_profit_comparison_summary(rows, primary, first_label, second_label)
    if cm1_summary:
        return cm1_summary

    summary = f"From **{first_label}** to **{second_label}**, {_comparison_row_plain_change(primary)}."
    if primary.get("effect") in {"unfavorable", "favorable"}:
        driver_effect = str(primary.get("effect"))
    else:
        driver_effect = "unfavorable" if primary_delta < 0 else "favorable"
    drivers = []
    for row in rows:
        if row is primary:
            continue
        if row.get("effect") != driver_effect:
            continue
        if abs(_safe_float(row.get("business_delta"))) < 0.005:
            continue
        if primary.get("metric") == "profit" and row.get("metric") in CM1_SECONDARY_ONLY_METRICS:
            continue
        drivers.append(row)
    drivers = sorted(drivers, key=lambda row: _comparison_summary_driver_sort_key(row, primary.get("metric")))
    if drivers:
        summary += " Main signal: " + " and ".join(_comparison_row_plain_change(row) for row in drivers[:2]) + "."
    return summary


def _render_comparison_table_markdown(
    title: str,
    rows: List[Dict[str, Any]],
    first_label: str,
    second_label: str,
    *,
    summary: Optional[str] = None,
    next_check: Optional[str] = None,
) -> str:
    lines = [
        f"**{title}**",
        "",
        "| Metric | " + _markdown_table_cell(first_label) + " | " + _markdown_table_cell(second_label) + " | Change |",
        "|---|---:|---:|---|",
    ]
    for row in rows:
        lines.append(
            f"| {_markdown_table_cell(row.get('label'))} | "
            f"{_markdown_table_cell(row.get('first_formatted'))} | "
            f"{_markdown_table_cell(row.get('second_formatted'))} | "
            f"{_markdown_table_cell(row.get('change_text'))} |"
        )

    if summary:
        lines.extend(["", f"**Summary:** {summary}"])
    if next_check:
        lines.append(f"**Next check:** {next_check}")
    return "\n".join(lines)


def _build_single_metric_comparison_row(
    state: AgentState,
    metric_name: str,
    first_value: Any,
    second_value: Any,
) -> Dict[str, Any]:
    first = _safe_float(first_value)
    second = _safe_float(second_value)
    change_text, business_delta, pct_change, effect = _format_comparison_change_text(
        metric_name,
        first,
        second,
        state.get("country"),
    )
    return {
        "metric": metric_name,
        "label": humanize_metric(metric_name),
        "first_value": first,
        "second_value": second,
        "first_formatted": _format_period_metric_value(first, metric_name, state.get("country")),
        "second_formatted": _format_period_metric_value(second, metric_name, state.get("country")),
        "change_text": change_text,
        "change_amount": _format_comparison_change_amount(metric_name, business_delta, state.get("country")),
        "business_delta": business_delta,
        "pct_change": pct_change,
        "effect": effect,
    }


def _render_single_metric_comparison_table_response(
    state: AgentState,
    *,
    metric_name: str,
    left_label: Any,
    left_value: Any,
    right_label: Any,
    right_value: Any,
    title: str,
    summary_metric: Optional[str] = None,
) -> str:
    first_side, first_label, second_side, second_label = _ordered_period_labels(left_label, right_label)
    value_map = {"left": left_value, "right": right_value}
    row = _build_single_metric_comparison_row(
        state,
        metric_name,
        value_map[first_side],
        value_map[second_side],
    )
    summary = _comparison_table_summary([row], summary_metric or metric_name, first_label, second_label)
    return _render_comparison_table_markdown(title, [row], first_label, second_label, summary=summary)


def _render_business_comparison_table_response(state: AgentState, analysis: Dict[str, Any]) -> Optional[str]:
    context = analysis.get("context") or {}
    comparison_context = context.get("comparison") or {}
    if not comparison_context.get("metrics"):
        return None

    primary_metric = _select_primary_comparison_metric(state, analysis, comparison_context)
    rows, first_label, second_label = _build_comparison_table_rows(state, comparison_context, primary_metric)
    if not rows:
        return None

    country_label = _country_display_name(analysis.get("country") or state.get("country"))
    title_metric = humanize_metric(primary_metric or rows[0].get("metric") or "business")
    title = f"{country_label}: {title_metric} change ({first_label} to {second_label})"

    summary = _comparison_table_summary(rows, primary_metric, first_label, second_label)
    ranked = comparison_context.get("unfavorable_metric_drivers") or comparison_context.get("metric_drivers") or []
    actions = _render_diagnosis_actions(state, comparison_context, [driver for driver in ranked if isinstance(driver, dict)][:6])
    next_check = actions[0] if actions else None
    return _render_comparison_table_markdown(title, rows, first_label, second_label, summary=summary, next_check=next_check)


def _product_label_from_record(record: Dict[str, Any]) -> str:
    product = str(record.get("product_name") or "").strip()
    sku = str(record.get("sku") or "").strip()
    if product and sku and sku.lower() not in product.lower():
        return f"{product} ({sku})"
    return product or sku or "Unknown SKU"


def _metric_delta_text(state: AgentState, record: Dict[str, Any], metric: str) -> Optional[str]:
    key = f"{metric}_delta"
    if key not in record:
        return None

    try:
        delta = float(record.get(key) or 0.0)
        left_value = float(record.get(f"{metric}_left") or 0.0)
        right_value = float(record.get(f"{metric}_right") or 0.0)
    except Exception:
        return None

    if metric in BURDEN_DISPLAY_METRICS:
        burden_delta = _display_burden_delta(metric, left_value, right_value)
        if abs(burden_delta) < 0.005:
            return None
        direction = "discount burden up" if metric in SIGN_AWARE_BURDEN_DISPLAY_METRICS and burden_delta > 0 else "discount burden down" if metric in SIGN_AWARE_BURDEN_DISPLAY_METRICS else "burden up" if burden_delta > 0 else "burden down"
        value = _format_metric_for_display(abs(burden_delta), metric, state.get("country"))
        return f"{direction} {value}"

    if abs(delta) < 0.005:
        return None

    direction = "down" if delta < 0 else "up"
    value = _format_metric_for_display(abs(delta), metric, state.get("country"))
    return f"{direction} {value}"


def _record_identity(record: Dict[str, Any]) -> tuple[str, str]:
    return (
        str(record.get("sku") or "").strip().lower(),
        str(record.get("product_name") or "").strip().lower(),
    )


def _unique_records(*groups: List[Dict[str, Any]], limit: int = 3) -> List[Dict[str, Any]]:
    seen: set[tuple[str, str]] = set()
    out: List[Dict[str, Any]] = []
    for group in groups:
        for record in group or []:
            identity = _record_identity(record)
            if not any(identity) or identity in seen:
                continue
            seen.add(identity)
            out.append(record)
            if len(out) >= limit:
                return out
    return out


def _find_inventory_signal(inventory: Dict[str, Any], record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    sku, product = _record_identity(record)
    for row in inventory.get("rows") or []:
        row_sku, row_product = _record_identity(row)
        if sku and row_sku == sku:
            return row
        if product and (row_product == product or product in row_product or row_product in product):
            return row
    return None


def _inventory_signal_text(row: Optional[Dict[str, Any]]) -> Optional[str]:
    if not row:
        return None
    metrics = row.get("metrics") or {}
    parts = []
    if "available" in metrics:
        parts.append(f"{float(metrics.get('available') or 0.0):,.0f} available")
    if "inbound_quantity" in metrics:
        parts.append(f"{float(metrics.get('inbound_quantity') or 0.0):,.0f} inbound")
    if "days_of_supply" in metrics:
        parts.append(f"{float(metrics.get('days_of_supply') or 0.0):,.1f} days of supply")
    if not parts:
        return None
    period = row.get("period_label")
    suffix = f" ({period})" if period else ""
    return ", ".join(parts) + suffix


def _render_sku_diagnosis_lines(state: AgentState, comparison_context: Dict[str, Any]) -> List[str]:
    inventory = comparison_context.get("diagnosis_inventory") or {}
    records = _unique_records(
        comparison_context.get("top_negative_profit_drivers") or [],
        comparison_context.get("top_unit_loss_drivers") or [],
        comparison_context.get("top_order_loss_drivers") or [],
        comparison_context.get("top_cm2_loss_drivers") or [],
        comparison_context.get("top_sales_loss_drivers") or [],
        comparison_context.get("top_rebate_burden_drivers") or [],
        limit=3,
    )
    lines: List[str] = []

    for record in records:
        parts = []
        for metric in [
            "profit",
            "total_quantity",
            "return_quantity",
            "promotional_rebates",
            "cm2_profit",
            "net_sales",
            "asp",
            "platform_fee",
            "platformfeenew",
            "platform_fee_inventory_storage",
            "ads_spend",
        ]:
            text = _metric_delta_text(state, record, metric)
            if text:
                parts.append(f"{humanize_metric(metric)} {text}")
            if len(parts) >= 5:
                break

        inventory_text = _inventory_signal_text(_find_inventory_signal(inventory, record))
        if inventory_text:
            parts.append(f"inventory {inventory_text}")

        if parts:
            lines.append(f"- **{_product_label_from_record(record)}**: {', '.join(parts)}.")

    return lines


def _render_diagnosis_actions(state: AgentState, comparison_context: Dict[str, Any], shown_drivers: List[Dict[str, Any]]) -> List[str]:
    actions: List[str] = []
    top_profit = _first_product(comparison_context.get("top_negative_profit_drivers") or [])
    top_units = _first_product(comparison_context.get("top_unit_loss_drivers") or [])
    top_cm2 = _first_product(comparison_context.get("top_cm2_loss_drivers") or [])
    top_rebate = _first_product(comparison_context.get("top_rebate_burden_drivers") or [])
    inventory = comparison_context.get("diagnosis_inventory") or {}
    lookup = _driver_lookup(comparison_context)
    total_only_metrics = set(comparison_context.get("total_only_metrics") or [])

    if top_units:
        inventory_text = _inventory_signal_text(_find_inventory_signal(inventory, top_units))
        extra = f" Inventory shows {inventory_text}." if inventory_text else ""
        actions.append(
            f"Open **{_product_label_from_record(top_units)}** first; check sessions, conversion, price/Buy Box, and stock because it had the biggest unit drop.{extra}"
        )

    if top_profit and _record_identity(top_profit) != _record_identity(top_units or {}):
        actions.append(
            f"Review **{_product_label_from_record(top_profit)}** as the biggest profit drag; compare its sales, units, CM2, fees, rebates, and ads."
        )

    if top_cm2:
        actions.append(
            f"Check CM2 on **{_product_label_from_record(top_cm2)}** to confirm whether margin loss is SKU-specific or portfolio-wide."
        )

    rebate_driver = lookup.get("promotional_rebates")
    if rebate_driver or (top_rebate and abs(float(top_rebate.get("promotional_rebates_delta") or 0.0)) > 0.005):
        product_text = f" on **{_product_label_from_record(top_rebate)}**" if top_rebate else ""
        actions.append(f"Audit coupons/deals/rebates{product_text}; stop discounts that did not lift units or protect margin.")

    fee_driver = next((lookup.get(metric) for metric in ["platform_fee", "platformfeenew", "platform_fee_inventory_storage", "selling_fees", "fba_fees", "amazon_fees"] if lookup.get(metric)), None)
    if fee_driver:
        label = str(fee_driver.get("label") or "fees").lower()
        fee_metric = str(fee_driver.get("metric") or "").strip().lower()
        if fee_metric in total_only_metrics:
            actions.append(f"Reconcile the **{label}** monthly total by invoice/transaction and separate one-off charges from recurring cost increases.")
        else:
            actions.append(f"Reconcile the **{label}** change by SKU/transaction and separate one-off charges from recurring cost increases.")

    ads_driver = next((lookup.get(metric) for metric in ["ads_spend", "product_spend", "display_spend", "brand_spend", "tacos_total_advertising_cost_of_sale"] if lookup.get(metric)), None)
    if ads_driver:
        actions.append("Review campaigns where spend rose while sales or CM2 fell; pause wasted spend before scaling budgets.")

    if not actions:
        for driver in shown_drivers[:3]:
            action = _driver_action(driver)
            if action not in actions:
                actions.append(action)

    deduped: List[str] = []
    for action in actions:
        normalized = re.sub(r"\s+", " ", action).strip().lower()
        if normalized and normalized not in {re.sub(r"\s+", " ", item).strip().lower() for item in deduped}:
            deduped.append(action)
    return deduped[:4]


def _render_interpretation_lines(state: AgentState, comparison_context: Dict[str, Any], metric_name: str) -> List[str]:
    lookup = _driver_lookup(comparison_context)
    inventory = comparison_context.get("diagnosis_inventory") or {}
    lines: List[str] = []

    unit_driver = lookup.get("total_quantity") or lookup.get("quantity")
    if unit_driver and unit_driver.get("business_effect") == "unfavorable":
        lines.append("- Lower units point to a demand, stock, conversion, price, or Buy Box problem. The SKU checks below show where to start.")

    inventory_rows = inventory.get("rows") or []
    if inventory_rows:
        constrained = []
        for row in inventory_rows:
            metrics = row.get("metrics") or {}
            available = metrics.get("available")
            days = metrics.get("days_of_supply")
            try:
                is_constrained = (available is not None and float(available) <= 0) or (days is not None and float(days) < 21)
            except Exception:
                is_constrained = False
            if is_constrained:
                constrained.append(_product_label_from_record(row))
        if constrained:
            lines.append(f"- Inventory may be constraining **{constrained[0]}**; fix availability before pushing ads or discounts.")
        else:
            lines.append("- Inventory does not look like the main blocker for the checked SKUs; focus on demand, conversion, pricing, and margin leakage.")

    if lookup.get("promotional_rebates") and lookup["promotional_rebates"].get("business_effect") == "unfavorable":
        lines.append("- Promotions/rebates worsened margin. Confirm the discounts created extra units; otherwise they are reducing profit without enough upside.")

    if not lines and metric_name == "profit":
        lines.append("- Treat this as a profit bridge: first confirm units and sales, then check CM2 margin, discounts, fees, ads, and returns.")

    return lines[:3]


def _render_business_comparison_diagnosis(state: AgentState, analysis: Dict[str, Any]) -> str:
    table_response = _render_business_comparison_table_response(state, analysis)
    if table_response:
        return table_response

    context = analysis.get("context") or {}
    comparison_context = context.get("comparison") or {}
    scope = context.get("scope") or {}
    metric_name = scope.get("metric_name") or analysis.get("metric_name") or state.get("metric_name") or "profit"
    metrics = comparison_context.get("metrics") or {}
    metric_comp = metrics.get(metric_name) or metrics.get("profit") or next(iter(metrics.values()))
    left = comparison_context.get("left") or {}
    right = comparison_context.get("right") or {}
    left_label = left.get("label") or "current period"
    right_label = right.get("label") or "previous period"
    country_label = _country_display_name(state.get("country"))
    left_value = float(metric_comp.get("left") or 0.0)
    right_value = float(metric_comp.get("right") or 0.0)
    if metric_name in BURDEN_DISPLAY_METRICS:
        delta = _display_burden_delta(metric_name, left_value, right_value)
        pct_base = abs(_display_burden_delta(metric_name, right_value, 0.0))
        pct_change = None if pct_base < 0.005 else (delta / pct_base) * 100.0
        direction = "burden down" if delta < 0 else "burden up" if delta > 0 else "flat"
    else:
        delta = float(metric_comp.get("delta") or 0.0)
        pct_change = metric_comp.get("pct_change")
        direction = "down" if delta < 0 else "up" if delta > 0 else "flat"
    delta_text = _format_metric_for_display(abs(delta), metric_name, state.get("country"))
    sign = "-" if delta < 0 else "+" if delta > 0 else ""
    pct_text = "" if pct_change is None else f", {float(pct_change):+.2f}%"

    if delta < 0:
        ranked_drivers = comparison_context.get("unfavorable_metric_drivers") or comparison_context.get("metric_drivers") or []
    elif delta > 0:
        ranked_drivers = comparison_context.get("favorable_metric_drivers") or comparison_context.get("metric_drivers") or []
    else:
        ranked_drivers = comparison_context.get("metric_drivers") or []

    shown_drivers = [driver for driver in ranked_drivers if isinstance(driver, dict)][:8]
    lines = [
        (
            f"Direct answer: **{humanize_metric(metric_name)}** is **{direction}** in **{country_label}**: "
            f"**{_format_metric_for_display(metric_comp.get('left'), metric_name, state.get('country'))}** in **{left_label}** "
            f"vs **{_format_metric_for_display(metric_comp.get('right'), metric_name, state.get('country'))}** in **{right_label}** "
            f"(**{sign}{delta_text}{pct_text}**)."
        )
    ]

    change_lines = _render_change_group_lines(state, comparison_context, limit=6)
    if change_lines:
        lines.append("")
        lines.append("What changed:")
        lines.extend(change_lines)

    sku_lines = _render_sku_diagnosis_lines(state, comparison_context)
    if sku_lines:
        lines.append("")
        lines.append("SKU checks:")
        lines.extend(sku_lines[:3])

    meaning_lines = _render_interpretation_lines(state, comparison_context, str(metric_name))
    if meaning_lines:
        lines.append("")
        lines.append("What it means:")
        lines.extend(meaning_lines)

    if shown_drivers:
        actions = _render_diagnosis_actions(state, comparison_context, shown_drivers)
        lines.append("")
        lines.append("Do next:")
        for idx, action in enumerate(actions, start=1):
            lines.append(f"{idx}. {action}")

        top_labels = []
        for driver in shown_drivers:
            label = str(driver.get("label") or humanize_metric(driver.get("metric") or "")).lower()
            family = _driver_family(str(driver.get("metric") or ""))
            if family == "units":
                label = "units"
            if label not in top_labels:
                top_labels.append(label)
            if len(top_labels) == 2:
                break
        if delta < 0:
            lines.append("")
            lines.append(f"Bottom line: the issue is mainly **{', '.join(top_labels)}**, so fix those before increasing spend or promotions.")
        elif delta > 0:
            lines.append("")
            lines.append(f"Bottom line: the improvement is mainly from **{', '.join(top_labels)}**; protect those drivers.")

    return "\n".join(lines)


def _render_business_advisor_fallback(state: AgentState, analysis: Dict[str, Any]) -> str:
    context = analysis.get("context") or {}
    period = (context.get("period") or {}).get("label") or "selected period"
    totals = context.get("totals") or {}
    derived = context.get("derived") or {}
    rankings = context.get("rankings") or {}
    history = context.get("history") or {}
    movement = history.get("movement") or {}
    comparison_context = context.get("comparison") or {}

    if comparison_context.get("requested") and comparison_context.get("metrics"):
        return _render_business_comparison_diagnosis(state, analysis)

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


def _format_anomaly_pct(value: Any) -> str:
    try:
        return f"{float(value):+.1f}%"
    except Exception:
        return ""


def _render_anomaly_contributor(state: AgentState, metric_name: str, contributor: Dict[str, Any]) -> str:
    label = _product_label_from_record(contributor)
    change_text = contributor.get("change_text") or _anomaly_change_text(metric_name, _safe_float(contributor.get("business_delta")))
    delta_text = _format_anomaly_delta(_safe_float(contributor.get("business_delta")), metric_name, state.get("country"))
    return f"**{label}**: {humanize_metric(metric_name)} {change_text} **{delta_text}**"


ANOMALY_DRIVER_SNAPSHOT_METRICS = [
    "profit",
    "net_sales",
    "asp",
    "total_quantity",
    "promotional_rebates",
]


def _anomaly_driver_snapshot_lines(state: AgentState, analysis: Dict[str, Any]) -> List[str]:
    blocks = {
        str(block.get("metric") or ""): block
        for block in analysis.get("metric_blocks") or []
        if isinstance(block, dict)
    }
    lines: List[str] = []

    for metric_name in ANOMALY_DRIVER_SNAPSHOT_METRICS:
        block = blocks.get(metric_name)
        if not block:
            continue

        months = [
            row for row in block.get("months") or []
            if isinstance(row, dict) and row.get("period_label") is not None
        ]
        if len(months) < 2:
            continue

        points: List[str] = []
        for row in months[-4:]:
            value = row.get("formatted")
            if value is None:
                value = _format_metric_for_display(row.get("value"), metric_name, state.get("country"))
            points.append(f"{row.get('period_label')}: {value}")

        if points:
            lines.append(f"- **{humanize_metric(metric_name)}:** {' -> '.join(points)}.")

    return lines


def _anomaly_action_for_metric(metric_name: str) -> str:
    if metric_name in {"profit", "net_sales", "gross_sales", "total_quantity", "total_cm2_profit", "cm2_profit", "cm2_profit_per"}:
        return "Start with the product clues; check stock, Buy Box, conversion, price, and SKU-level CM2."
    if metric_name in {"promotional_rebates", "promotional_rebates_tax"}:
        return "Audit coupons/deals/rebates and keep only discounts that lift sold units or protect margin."
    if metric_name in {"platform_fee", "selling_fees", "fba_fees", "amazon_fees", "cogs"}:
        return "Reconcile fee and cost spikes by SKU/transaction to separate one-off charges from recurring leakage."
    if metric_name in {"ads_spend", "total_ads", "ads_acos", "tacos_total_advertising_cost_of_sale"}:
        return "Review campaigns where spend or TACOS increased without matching sales or CM2 growth."
    if metric_name in {"refund_sales", "return_quantity", "return_rate"}:
        return "Check returned/refunded SKUs and reasons because returns can hide inside sales performance."
    if metric_name in INVENTORY_METRICS:
        return "Check inventory availability and days of supply for affected SKUs before changing ads or discounts."
    return f"Drill into {humanize_metric(metric_name).lower()} by SKU and month."


CM2_DRIVER_REQUEST_METRICS = {"total_cm2_profit", "cm2_profit", "cm2_profit_per"}

CM2_DRIVER_TABLE_GROUPS = [
    ("total_cm2_profit", ("total_cm2_profit", "cm2_profit")),
    ("cm2_profit_per", ("cm2_profit_per",)),
    ("profit", ("profit",)),
    ("ad_spend", ("total_ads", "ads_spend")),
    ("platform_fee", ("platform_fee",)),
    ("platformfeenew", ("platformfeenew",)),
    ("platform_fee_inventory_storage", ("platform_fee_inventory_storage",)),
    ("lost_total", ("lost_total",)),
]

CM2_DRIVER_ALWAYS_SHOW = {
    "total_cm2_profit",
    "cm2_profit",
    "cm2_profit_per",
    "profit",
    "total_ads",
    "ads_spend",
    "platform_fee",
    "platformfeenew",
    "platform_fee_inventory_storage",
    "lost_total",
}


def _anomaly_scan_targets_cm2(state: AgentState, analysis: Dict[str, Any]) -> bool:
    query = (state.get("user_query") or "").lower()
    if "cm2" in query or "contribution margin 2" in query:
        return True

    primary = str(state.get("metric_name") or analysis.get("metric_name") or "").strip().lower()
    if primary in CM2_DRIVER_REQUEST_METRICS:
        return True

    explicit_metrics = [
        str(metric or "").strip().lower()
        for metric in (state.get("metric_names") or [])
        if metric
    ]
    return len(explicit_metrics) == 1 and explicit_metrics[0] in CM2_DRIVER_REQUEST_METRICS


def _anomaly_metric_block_lookup(analysis: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    return {
        str(block.get("metric") or ""): block
        for block in analysis.get("metric_blocks") or []
        if isinstance(block, dict)
    }


def _metric_block_edge_points(block: Dict[str, Any]) -> Optional[Tuple[Dict[str, Any], Dict[str, Any]]]:
    months = [
        row for row in block.get("months") or []
        if isinstance(row, dict) and row.get("period_label") is not None
    ]
    if len(months) < 2:
        return None
    return months[0], months[-1]


def _select_cm2_driver_block(
    blocks: Dict[str, Dict[str, Any]],
    metric_candidates: Tuple[str, ...],
) -> Optional[Tuple[str, Dict[str, Any], Dict[str, Any]]]:
    fallback: Optional[Tuple[str, Dict[str, Any], Dict[str, Any]]] = None
    for metric in metric_candidates:
        block = blocks.get(metric)
        if not block:
            continue
        points = _metric_block_edge_points(block)
        if not points:
            continue
        left, right = points
        selected = (metric, left, right)
        if fallback is None:
            fallback = selected
        if abs(_safe_float(left.get("value"))) > 0.005 or abs(_safe_float(right.get("value"))) > 0.005:
            return selected
    return fallback


def _format_cm2_driver_change(
    metric_name: str,
    left_value: float,
    right_value: float,
    country: Optional[str],
) -> Tuple[str, float, Optional[float], str]:
    if metric_name == "lost_total":
        business_delta = abs(right_value) - abs(left_value)
        if abs(business_delta) < 0.005:
            return "Flat", business_delta, 0.0, "neutral"
        pct_change = None if abs(left_value) < 0.005 else (business_delta / abs(left_value)) * 100.0
        amount = _format_metric_for_display(abs(business_delta), metric_name, country)
        pct_suffix = "" if pct_change is None else f" ({pct_change:+.1f}%)"
        effect = "favorable" if business_delta > 0 else "unfavorable"
        direction = "Recovery up" if business_delta > 0 else "Recovery down"
        return f"{direction} {amount}{pct_suffix}", business_delta, pct_change, effect

    business_delta = _anomaly_business_delta(metric_name, right_value, left_value)
    if abs(business_delta) < 0.005:
        return "Flat", business_delta, 0.0, "neutral"

    pct_change = None
    if abs(left_value) >= 0.005:
        pct_change = (business_delta / abs(left_value)) * 100.0

    is_cost = _anomaly_metric_is_bad_when_up(metric_name)
    if is_cost:
        direction = "Burden up" if business_delta > 0 else "Burden down"
        effect = "unfavorable" if business_delta > 0 else "favorable"
    else:
        direction = "Down" if business_delta < 0 else "Up"
        effect = "unfavorable" if business_delta < 0 else "favorable"

    amount = _format_anomaly_delta(business_delta, metric_name, country)
    pct_suffix = "" if pct_change is None else f" ({pct_change:+.1f}%)"
    return f"{direction} {amount}{pct_suffix}", business_delta, pct_change, effect


def _format_cm2_driver_value(value: Any, metric_name: str, country: Optional[str]) -> str:
    if metric_name == "lost_total":
        return _format_metric_for_display(abs(_safe_float(value)), metric_name, country)
    return _format_metric_for_display(value, metric_name, country)


def _build_cm2_driver_table_rows(state: AgentState, analysis: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], str, str]:
    blocks = _anomaly_metric_block_lookup(analysis)
    rows: List[Dict[str, Any]] = []
    left_label = "Period 1"
    right_label = "Period 2"

    for _, candidates in CM2_DRIVER_TABLE_GROUPS:
        selected = _select_cm2_driver_block(blocks, candidates)
        if not selected:
            continue

        metric_name, left, right = selected
        left_value = _safe_float(left.get("value"))
        right_value = _safe_float(right.get("value"))
        if metric_name not in CM2_DRIVER_ALWAYS_SHOW and abs(left_value) < 0.005 and abs(right_value) < 0.005:
            continue

        left_label = _clean_period_label(left.get("period_label")) or left_label
        right_label = _clean_period_label(right.get("period_label")) or right_label
        change_text, business_delta, pct_change, effect = _format_cm2_driver_change(
            metric_name,
            left_value,
            right_value,
            state.get("country"),
        )
        rows.append(
            {
                "metric": metric_name,
                "label": humanize_metric(metric_name),
                "left_value": left_value,
                "right_value": right_value,
                "left_formatted": _format_cm2_driver_value(left_value, metric_name, state.get("country")),
                "right_formatted": _format_cm2_driver_value(right_value, metric_name, state.get("country")),
                "change_text": change_text,
                "business_delta": business_delta,
                "pct_change": pct_change,
                "effect": effect,
            }
        )

    return rows, left_label, right_label


def _cm2_driver_plain_change(row: Dict[str, Any]) -> str:
    return f"**{row.get('label')}** {str(row.get('change_text') or '').lower()}"


def _cm2_driver_change_amount(row: Dict[str, Any], country: Optional[str]) -> str:
    metric_name = str(row.get("metric") or "")
    delta = abs(_safe_float(row.get("business_delta")))
    return _format_metric_for_display(delta, metric_name, country)


def _cm2_driver_summary_phrase(row: Dict[str, Any], country: Optional[str]) -> str:
    metric_name = str(row.get("metric") or "")
    amount = _cm2_driver_change_amount(row, country)
    delta = _safe_float(row.get("business_delta"))

    if metric_name == "profit":
        verb = "improved" if delta > 0 else "fell"
        return f"CM1 Profit {verb} by **{amount}**"
    if metric_name in {"total_ads", "ads_spend"}:
        verb = "added" if delta > 0 else "removed"
        return f"Ad Spend {verb} **{amount}** of cost"
    if metric_name == "platform_fee":
        verb = "added" if delta > 0 else "removed"
        return f"Platform Fee {verb} **{amount}** of cost"
    if metric_name == "platformfeenew":
        verb = "added" if delta > 0 else "removed"
        return f"Subscription Fees {verb} **{amount}** of cost"
    if metric_name == "platform_fee_inventory_storage":
        verb = "added" if delta > 0 else "removed"
        return f"Storage Fees {verb} **{amount}** of cost"
    if metric_name == "lost_total":
        verb = "increased" if delta > 0 else "decreased"
        return f"Lost Inventory Reimbursement {verb} by **{amount}**"

    return _cm2_driver_plain_change(row)


CM2_SUMMARY_DRIVER_PRIORITY = {
    "profit": 0,
    "total_ads": 1,
    "ads_spend": 1,
    "platform_fee": 2,
    "platformfeenew": 3,
    "platform_fee_inventory_storage": 4,
    "lost_total": 5,
}


def _cm2_driver_sort_key(row: Dict[str, Any]) -> Tuple[float, float, float]:
    metric_name = str(row.get("metric") or "")
    return (
        float(CM2_SUMMARY_DRIVER_PRIORITY.get(metric_name, 50)),
        -abs(_safe_float(row.get("business_delta"))),
        -abs(float(row.get("pct_change") or 0.0)),
    )


def _cm2_driver_summary(
    rows: List[Dict[str, Any]],
    left_label: str,
    right_label: str,
    country: Optional[str],
) -> str:
    outcome = next((row for row in rows if row.get("metric") in {"total_cm2_profit", "cm2_profit"}), None)
    if not outcome:
        return "This table compares the main CM2 drivers for the selected periods."

    outcome_delta = _safe_float(outcome.get("business_delta"))
    if abs(outcome_delta) < 0.005:
        return f"CM2 profit was broadly flat from **{left_label}** to **{right_label}**."

    outcome_amount = _format_anomaly_delta(outcome_delta, str(outcome.get("metric")), country)
    target_effect = "unfavorable" if outcome_delta < 0 else "favorable"
    pressure_rows = [
        row for row in rows
        if row.get("metric") not in {"total_cm2_profit", "cm2_profit", "cm2_profit_per"}
        and row.get("effect") == target_effect
    ]
    offset_rows = [
        row for row in rows
        if row.get("metric") not in {"total_cm2_profit", "cm2_profit", "cm2_profit_per"}
        and row.get("effect") not in {"neutral", target_effect}
    ]
    pressure_rows = sorted(pressure_rows, key=_cm2_driver_sort_key)
    offset_rows = sorted(offset_rows, key=_cm2_driver_sort_key)

    pressure_text = ""
    if pressure_rows:
        pressure_text = " Main pressure: " + " and ".join(
            _cm2_driver_summary_phrase(row, country) for row in pressure_rows[:2]
        ) + "."

    offset_text = ""
    if offset_rows:
        if outcome_delta < 0:
            offset_text = " Helpful offsets: " + " and ".join(
                _cm2_driver_summary_phrase(row, country) for row in offset_rows[:2]
            ) + ", but they were not enough."
        else:
            offset_text = " Remaining pressure: " + " and ".join(
                _cm2_driver_summary_phrase(row, country) for row in offset_rows[:2]
            ) + "."

    if outcome_delta < 0:
        return (
            f"CM2 profit **fell by {outcome_amount}** from **{left_label}** to **{right_label}**."
            f"{pressure_text}{offset_text}"
        )

    return (
        f"CM2 profit **improved by {outcome_amount}** from **{left_label}** to **{right_label}**."
        f"{pressure_text}{offset_text}"
    )


def _render_cm2_driver_table_response(state: AgentState, analysis: Dict[str, Any]) -> Optional[str]:
    rows, left_label, right_label = _build_cm2_driver_table_rows(state, analysis)
    if len(rows) < 2:
        return None

    country_label = _country_display_name(analysis.get("country") or state.get("country"))
    lines = [
        f"**{country_label}: CM2 profit bridge ({left_label} to {right_label})**",
        "",
        "| Metric | " + _markdown_table_cell(left_label) + " | " + _markdown_table_cell(right_label) + " | Change |",
        "|---|---:|---:|---|",
    ]
    for row in rows[:12]:
        lines.append(
            f"| {_markdown_table_cell(row.get('label'))} | "
            f"{_markdown_table_cell(row.get('left_formatted'))} | "
            f"{_markdown_table_cell(row.get('right_formatted'))} | "
            f"{_markdown_table_cell(row.get('change_text'))} |"
        )

    lines.extend(["", f"**Summary:** {_cm2_driver_summary(rows, left_label, right_label, state.get('country'))}"])
    return "\n".join(lines)


def _anomaly_scan_has_two_period_table_context(state: AgentState, analysis: Dict[str, Any]) -> bool:
    query = (state.get("user_query") or "").lower()
    if any(word in query for word in [" vs ", " versus ", "compare", "compared", "change", "changed", "down", "up", "decrease", "decreased", "increase", "increased", "fluctuation", "from "]):
        return True
    for block in analysis.get("metric_blocks") or []:
        if not isinstance(block, dict):
            continue
        months = [
            row for row in block.get("months") or []
            if isinstance(row, dict) and row.get("period_label") is not None
        ]
        if len(months) == 2:
            return True
    return False


def _anomaly_metric_order(state: AgentState, analysis: Dict[str, Any]) -> List[str]:
    blocks = _anomaly_metric_block_lookup(analysis)
    ordered: List[str] = []

    def add(metric: Any) -> None:
        key = _resolve_available_comparison_metric(metric, blocks)
        if key and key in blocks and key not in ordered:
            ordered.append(key)

    add(state.get("metric_name"))
    for metric in state.get("metric_names") or []:
        add(metric)
    for metric in analysis.get("metrics") or []:
        add(metric)
    for anomaly in analysis.get("anomalies") or []:
        if isinstance(anomaly, dict):
            add(anomaly.get("metric"))
    for metric in COMPARISON_TABLE_DEFAULT_ORDER:
        add(metric)
    for metric in blocks:
        add(metric)

    return ordered


def _build_anomaly_change_table_rows(
    state: AgentState,
    analysis: Dict[str, Any],
    *,
    limit: int = COMPARISON_TABLE_MAX_ROWS,
) -> Tuple[List[Dict[str, Any]], str, str, Optional[str]]:
    blocks = _anomaly_metric_block_lookup(analysis)
    rows: List[Dict[str, Any]] = []
    first_label = "Period 1"
    second_label = "Period 2"
    primary_metric: Optional[str] = None

    requested_metrics = _comparison_user_requested_metric_candidates(
        state,
        extra_metrics=[analysis.get("metric_name"), analysis.get("metric")],
    )
    requested = _comparison_requested_metric_set(blocks, requested_metrics)
    for metric in requested_metrics:
        key = _resolve_available_comparison_metric(metric, blocks)
        if key in blocks:
            primary_metric = key
            break

    for metric_name in _anomaly_metric_order(state, analysis):
        block = blocks.get(metric_name)
        if not block:
            continue
        points = _metric_block_edge_points(block)
        if not points:
            continue

        raw_first, raw_second = points
        first_side, first_label_candidate, second_side, second_label_candidate = _ordered_period_labels(
            raw_first.get("period_label"),
            raw_second.get("period_label"),
        )
        point_map = {"left": raw_first, "right": raw_second}
        first = point_map[first_side]
        second = point_map[second_side]
        first_label = first_label_candidate or first_label
        second_label = second_label_candidate or second_label

        first_value = _safe_float(first.get("value"))
        second_value = _safe_float(second.get("value"))
        if metric_name != primary_metric and abs(first_value) < 0.005 and abs(second_value) < 0.005:
            continue

        change_text, business_delta, pct_change, effect = _format_comparison_change_text(
            metric_name,
            first_value,
            second_value,
            state.get("country"),
        )
        rows.append(
            {
                "metric": metric_name,
                "label": humanize_metric(metric_name),
                "first_value": first_value,
                "second_value": second_value,
                "first_formatted": _format_period_metric_value(first_value, metric_name, state.get("country")),
                "second_formatted": _format_period_metric_value(second_value, metric_name, state.get("country")),
                "change_text": change_text,
                "change_amount": _format_comparison_change_amount(metric_name, business_delta, state.get("country")),
                "business_delta": business_delta,
                "pct_change": pct_change,
                "effect": effect,
            }
        )
        if primary_metric is None:
            primary_metric = metric_name

    return _filter_comparison_table_rows(rows, requested, primary_metric, limit=limit), first_label, second_label, primary_metric


def _render_anomaly_change_table_response(state: AgentState, analysis: Dict[str, Any]) -> Optional[str]:
    if not _anomaly_scan_has_two_period_table_context(state, analysis):
        return None

    rows, first_label, second_label, primary_metric = _build_anomaly_change_table_rows(state, analysis)
    if not rows:
        return None

    country_label = _country_display_name(analysis.get("country") or state.get("country"))
    product_scope = analysis.get("product_scope") or {}
    product_label = None
    if product_scope:
        product_label = _format_product_with_sku(
            product_scope.get("display_name"),
            product_scope.get("sku"),
            product_scope.get("query"),
        )
    scope_label = f"{product_label} in {country_label}" if product_label else country_label
    title_metric = humanize_metric(primary_metric or rows[0].get("metric") or "business")
    title = f"{scope_label}: {title_metric} change ({first_label} to {second_label})"
    summary = _comparison_table_summary(rows, primary_metric, first_label, second_label)
    return _render_comparison_table_markdown(title, rows, first_label, second_label, summary=summary)


def _render_anomaly_scan_response(state: AgentState, analysis: Dict[str, Any]) -> str:
    if _anomaly_scan_targets_cm2(state, analysis):
        cm2_table_response = _render_cm2_driver_table_response(state, analysis)
        if cm2_table_response:
            return cm2_table_response

    anomaly_table_response = _render_anomaly_change_table_response(state, analysis)
    if anomaly_table_response:
        return anomaly_table_response

    country_label = _country_display_name(analysis.get("country") or state.get("country"))
    period_label = analysis.get("period_label") or (state.get("current_metrics") or {}).get("period_label") or "selected period"
    product_scope = analysis.get("product_scope") or {}
    product_label = None
    if product_scope:
        product_label = _format_product_with_sku(
            product_scope.get("display_name"),
            product_scope.get("sku"),
            product_scope.get("query"),
        )
    anomalies = analysis.get("anomalies") or []
    risks = [row for row in anomalies if row.get("business_effect") == "unfavorable"]
    positives = [row for row in anomalies if row.get("business_effect") == "favorable"]

    scope_text = f"{product_label} in {country_label}" if product_label else country_label
    lines = [f"**Anomaly scan: {scope_text}, {period_label}**"]

    if product_scope and not product_scope.get("matched", True):
        lines.append("")
        lines.append(f"I could not confidently match **{product_label}** to a product or SKU in this period, so I did not run an account-wide scan as a substitute.")
        lines.append("Please use the exact product name or SKU shown in Phormula.")
        return "\n".join(lines)

    if not anomalies:
        lines.append("")
        if product_label:
            lines.append(f"No major anomalies found for **{product_label}** in the main product-level metrics I scanned.")
        else:
            lines.append("No major anomalies found across the main business metrics I scanned.")
        lines.append("I checked sales, CM1 profit, units, CM2, fees, rebates, ads, and returns where product-level data is available.")
        if product_label:
            snapshot_lines = _anomaly_driver_snapshot_lines(state, analysis)
            if snapshot_lines:
                lines.append("")
                lines.append("Driver snapshot:")
                lines.extend(snapshot_lines)
        return "\n".join(lines)

    if risks:
        lines.append("")
        lines.append("Main anomalies:")
        for anomaly in risks[:5]:
            pct_text = _format_anomaly_pct(anomaly.get("pct_change"))
            pct_suffix = f", {pct_text}" if pct_text else ""
            lines.append(
                f"- **{anomaly.get('period_label')}**: **{anomaly.get('label')}** "
                f"{anomaly.get('change_text')} **{anomaly.get('delta_formatted')}**"
                f"{pct_suffix} vs **{anomaly.get('previous_period_label')}** "
                f"({anomaly.get('current_formatted')} vs {anomaly.get('previous_formatted')})."
            )

    if product_label:
        snapshot_lines = _anomaly_driver_snapshot_lines(state, analysis)
        if snapshot_lines:
            lines.append("")
            lines.append("Driver snapshot:")
            lines.extend(snapshot_lines)

    if positives:
        lines.append("")
        lines.append("Helpful offsets:")
        for anomaly in positives[:2]:
            pct_text = _format_anomaly_pct(anomaly.get("pct_change"))
            pct_suffix = f", {pct_text}" if pct_text else ""
            lines.append(
                f"- **{anomaly.get('label')}** {anomaly.get('change_text')} "
                f"**{anomaly.get('delta_formatted')}**{pct_suffix} in **{anomaly.get('period_label')}**."
            )

    clue_lines: List[str] = []
    seen_clues: set[tuple[str, str, str]] = set()
    for anomaly in risks[:6]:
        metric_name = anomaly.get("metric")
        for contributor in anomaly.get("contributors") or []:
            identity = (
                str(metric_name),
                str(contributor.get("sku") or "").lower(),
                str(contributor.get("product_name") or "").lower(),
            )
            if identity in seen_clues:
                continue
            seen_clues.add(identity)
            clue_lines.append(f"- {_render_anomaly_contributor(state, str(metric_name), contributor)}.")
            if len(clue_lines) >= 4:
                break
        if len(clue_lines) >= 4:
            break

    if clue_lines:
        lines.append("")
        lines.append("Product clues:")
        lines.extend(clue_lines)

    action_lines: List[str] = []
    seen_actions: set[str] = set()
    for anomaly in risks:
        action = _anomaly_action_for_metric(str(anomaly.get("metric") or ""))
        key = action.lower()
        if key not in seen_actions:
            seen_actions.add(key)
            action_lines.append(action)
        if len(action_lines) >= 3:
            break

    if action_lines:
        lines.append("")
        lines.append("Do next:")
        for idx, action in enumerate(action_lines, start=1):
            lines.append(f"{idx}. {action}")

    if risks:
        top_metrics = []
        for anomaly in risks[:3]:
            label = str(anomaly.get("label") or "").lower()
            if label and label not in top_metrics:
                top_metrics.append(label)
        lines.append("")
        lines.append(f"Bottom line: investigate **{', '.join(top_metrics[:3])}** first; these are the clearest risk signals in the period.")

    return "\n".join(lines)


def _format_signed_metric(value: Any, metric_name: str, country: Optional[str]) -> str:
    amount = _safe_float(value)
    formatted = _format_metric_for_display(abs(amount), metric_name, country)
    if amount < 0:
        return f"-{formatted}"
    return formatted


def _render_pricing_advisor_response(state: AgentState, analysis: Dict[str, Any]) -> str:
    status = analysis.get("status")
    country_label = _country_display_name(analysis.get("country") or state.get("country"))

    if status == "missing_product":
        return "Please tell me which product or SKU you want the ASP recommendation for."

    product = analysis.get("product") or {}
    product_label = _format_product_with_sku(
        product.get("product_name"),
        product.get("sku"),
        state.get("product_query") or "selected product",
    )

    if status == "no_sales_history":
        return f"I found **{product_label}**, but there is no sold-unit history to estimate ASP or CM1 profit per unit."

    current = analysis.get("current") or {}
    weighted = analysis.get("weighted_recent") or {}
    forecast = analysis.get("forecast") or {}
    rec = analysis.get("recommendation") or {}
    records = analysis.get("records") or []

    low_asp = _safe_float(rec.get("low_asp"))
    high_asp = _safe_float(rec.get("high_asp"))
    country = state.get("country")
    period_label = analysis.get("period_label") or "recent completed months"

    lines = [
        f"**ASP recommendation for {product_label} in {country_label}**",
        f"Suggested ASP range: **{_format_metric_for_display(low_asp, 'asp', country)} to {_format_metric_for_display(high_asp, 'asp', country)}**.",
        "",
        "Why this range:",
        (
            f"- Current ASP is **{_format_metric_for_display(current.get('asp'), 'asp', country)}** "
            f"with CM1 profit/unit of **{_format_signed_metric(current.get('cm1_profit_per_unit'), 'profit', country)}**."
        ),
        (
            f"- Recent weighted ASP is **{_format_metric_for_display(weighted.get('asp'), 'asp', country)}** "
            f"and recent CM1 profit/unit is **{_format_signed_metric(weighted.get('cm1_profit_per_unit'), 'profit', country)}**."
        ),
        (
            f"- Estimated CM1 cost/unit is **{_format_metric_for_display(rec.get('estimated_cm1_cost_per_unit'), 'asp', country)}** "
            f"(ASP minus CM1 profit/unit)."
        ),
    ]

    if weighted.get("cm2_profit_per_unit") is not None:
        lines.append(
            f"- Recent CM2/unit is **{_format_signed_metric(weighted.get('cm2_profit_per_unit'), 'total_cm2_profit', country)}**; "
            "ads/platform costs still need watching after CM1."
        )

    if abs(_safe_float(weighted.get("promo_rebate_per_unit"))) > 0.005:
        lines.append(
            f"- Promo rebate impact is **{_format_signed_metric(weighted.get('promo_rebate_per_unit'), 'promotional_rebates', country)} per unit**."
        )

    scenario_units = _safe_float(rec.get("scenario_units"))
    if scenario_units > 0:
        unit_label = f"{scenario_units:,.0f} units"
        if rec.get("scenario_units_source") == "forecast":
            source_label = forecast.get("forecast_units_source") or "forecast"
            unit_label += f" from {source_label} ({forecast.get('forecast_period_label') or forecast.get('period_label')})"
        else:
            unit_label += " based on recent average"

        lines.extend(
            [
                "",
                "Estimated CM1 outcome:",
                (
                    f"- At **{_format_metric_for_display(low_asp, 'asp', country)}**: "
                    f"CM1/unit about **{_format_signed_metric(rec.get('low_cm1_profit_per_unit'), 'profit', country)}**, "
                    f"CM1 about **{_format_signed_metric(rec.get('low_estimated_cm1_profit'), 'profit', country)}** on {unit_label}."
                ),
                (
                    f"- At **{_format_metric_for_display(high_asp, 'asp', country)}**: "
                    f"CM1/unit about **{_format_signed_metric(rec.get('high_cm1_profit_per_unit'), 'profit', country)}**, "
                    f"CM1 about **{_format_signed_metric(rec.get('high_estimated_cm1_profit'), 'profit', country)}** on {unit_label}."
                ),
            ]
        )

    if forecast.get("forecast_asp"):
        lines.append(
            f"- Forecast implied ASP is **{_format_metric_for_display(forecast.get('forecast_asp'), 'asp', country)}**."
        )
    if rec.get("scenario_units_source") == "forecast" and forecast.get("forecast_units_source"):
        lines.append(f"- Forecast units source: **{forecast.get('forecast_units_source')}**.")

    if records:
        latest_label = current.get("period_label") or records[-1].get("period_label")
        lines.extend(
            [
                "",
                "Do next:",
                f"1. Test the low end first if the goal is sales growth; use the high end only if stock is limited or margin is weak.",
                f"2. Track **{latest_label}** conversion, sold units, promo rebates, and CM2 after the price change.",
                "3. Avoid deeper discounts unless sold units rise enough to protect CM1 profit/unit.",
            ]
        )

    lines.append("")
    lines.append(f"Basis: **{period_label}**. This is an estimate from sales, units, CM1 profit, CM2, rebates, and forecast units where available.")

    return "\n".join(lines)


def _month_label_from_parts(month: Any, year: Any) -> Optional[str]:
    try:
        return datetime(int(year), int(month), 1).strftime("%b %Y")
    except Exception:
        return None


def _clean_period_label(label: Any) -> Optional[str]:
    if label is None:
        return None
    cleaned = str(label).strip()
    if not cleaned:
        return None
    lowered = cleaned.lower()
    if lowered in {
        "selected period",
        "unknown",
        "no data",
        "multi-country trend",
        "latest_month",
        "last_n",
        "last_n_months",
        "single",
        "range",
    }:
        return None
    return cleaned


def _period_label_from_payload_for_response(state: AgentState, payload: Optional[Dict[str, Any]]) -> Optional[str]:
    payload = payload or {}
    ptype = payload.get("type")

    if ptype == "growth_base":
        return _period_label_from_payload_for_response(state, payload.get("base") or {})

    if ptype == "single":
        label = _month_label_from_parts(payload.get("month"), payload.get("year"))
        if label and _query_has_mtd(state):
            return f"{label} MTD"
        return label

    if ptype == "range":
        start = _month_label_from_parts(payload.get("start_month"), payload.get("start_year"))
        end = _month_label_from_parts(payload.get("end_month"), payload.get("end_year"))
        if start and end:
            return f"{start} to {end}"

    if ptype == "multi_month":
        months = payload.get("months") or state.get("target_months") or []
        labels = [
            _month_label_from_parts(item.get("month"), item.get("year"))
            for item in months
            if item.get("month") and item.get("year")
        ]
        labels = [label for label in labels if label]
        if len(labels) == 1:
            return labels[0]
        if labels:
            return f"{labels[0]} to {labels[-1]}"

    if ptype == "comparison":
        left = _period_label_from_payload_for_response(state, payload.get("p1") or {})
        right = _period_label_from_payload_for_response(state, payload.get("p2") or {})
        if left and right:
            return f"{left} vs {right}"

    if ptype in {"last_n", "last_n_months"} and payload.get("n"):
        try:
            n = int(payload.get("n"))
        except (TypeError, ValueError):
            return None
        qualifier = "including current month" if _payload_includes_current_incomplete(payload) else "completed months"
        return f"last {n} {qualifier}"

    return None


def _period_label_from_context_for_response(context: Optional[Dict[str, Any]]) -> Optional[str]:
    context = context or {}
    comparison = context.get("comparison") or {}
    left = comparison.get("left") or {}
    right = comparison.get("right") or {}
    left_label = _clean_period_label(left.get("label"))
    right_label = _clean_period_label(right.get("label"))
    if comparison.get("requested") and left_label and right_label:
        return f"{left_label} vs {right_label}"

    period = context.get("period") or {}
    return _clean_period_label(period.get("label"))


def _period_label_for_final_response(state: AgentState) -> Optional[str]:
    comp = state.get("comparison") or {}
    left_label = _clean_period_label((comp.get("left") or {}).get("label"))
    right_label = _clean_period_label((comp.get("right") or {}).get("label"))
    if left_label and right_label:
        return f"{left_label} vs {right_label}"

    analysis = state.get("analysis_result") or {}
    current = state.get("current_metrics") or {}
    for candidate in [
        analysis.get("period_label"),
        current.get("period_label"),
        _period_label_from_context_for_response(analysis.get("context") or {}),
        _period_label_from_context_for_response(state.get("business_context") or {}),
        _period_label_from_payload_for_response(state, state.get("period_payload") or {}),
    ]:
        cleaned = _clean_period_label(candidate)
        if cleaned:
            return cleaned

    return None


def _response_already_mentions_period(response: str, period_label: str) -> bool:
    def normalize(value: str) -> str:
        value = re.sub(r"[*_`]", "", value or "")
        value = re.sub(r"\s+", " ", value)
        return value.strip().lower()

    response_norm = normalize(response)
    label_norm = normalize(period_label)
    if not label_norm:
        return True
    if label_norm in response_norm:
        return True

    for separator in (" vs ", " to "):
        if separator in label_norm:
            parts = [part.strip() for part in label_norm.split(separator) if part.strip()]
            if parts and all(part in response_norm for part in parts):
                return True

    return False


def _ensure_period_context(state: AgentState) -> AgentState:
    response = state.get("final_response")
    if not isinstance(response, str) or not response.strip():
        return state
    if state.get("intent") in {"chat", "explain", "clarify"}:
        return state
    if response.lstrip().lower().startswith("period:"):
        return state

    period_label = _period_label_for_final_response(state)
    if not period_label:
        return state
    if _response_already_mentions_period(response, period_label):
        return state

    state["final_response"] = f"Period: **{period_label}**.\n\n{response}"
    return state


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

    if analysis.get("type") == "anomaly_scan":
        state["final_response"] = _render_anomaly_scan_response(state, analysis)
        return state

    if analysis.get("type") == "pricing_advisor":
        state["final_response"] = _render_pricing_advisor_response(state, analysis)
        return state

    if analysis.get("type") == "raw_line_items":
        result = analysis.get("result") or {}
        metric_key = result.get("metric") or analysis.get("metric") or metric_name
        metric_label = humanize_metric(metric_key)
        if metric_key == "misc_transaction":
            metric_label = "Miscellaneous transactions"
        country_label = _country_display_name(result.get("country") or state.get("country"))
        raw_period = result.get("period_label") or period_label

        if not result.get("available"):
            state["final_response"] = (
                f"I could not find raw **{metric_label}** line items in **{country_label}** "
                f"for **{raw_period}**."
            )
            return state

        amount_sign = result.get("amount_sign") or "nonzero"
        total = _safe_float(result.get("total"))
        charges_total = _safe_float(result.get("charges_total"))
        credits_total = _safe_float(result.get("credits_total"))
        row_count = int(result.get("row_count") or 0)

        if amount_sign == "charges":
            direct_value = charges_total
            direct_label = "charges"
        elif amount_sign == "credits":
            direct_value = credits_total
            direct_label = "credits"
        else:
            direct_value = total
            direct_label = "net amount"

        lines = [
            (
                f"Direct answer: **{metric_label}** {direct_label} in **{country_label}** "
                f"for **{raw_period}** total **{_format_signed_metric_value(direct_value, metric_key, state.get('country'))}** "
                f"from **{row_count}** transactions."
            )
        ]

        if amount_sign == "nonzero":
            lines.append(
                f"Charges: **{_format_signed_metric_value(charges_total, metric_key, state.get('country'))}**; "
                f"credits: **{_format_signed_metric_value(credits_total, metric_key, state.get('country'))}**."
            )

        groups = result.get("groups") or []
        if groups:
            lines.append("")
            lines.append("Main charges:")
            for row in groups[:5]:
                label_parts = [
                    str(row.get("type") or "Unspecified type"),
                    str(row.get("description") or "Unspecified description"),
                ]
                label = " / ".join(part for part in label_parts if part)
                first_date = str(row.get("first_date") or "").strip()
                last_date = str(row.get("last_date") or "").strip()
                if first_date and last_date and first_date != last_date:
                    date_text = f"{first_date} to {last_date}"
                else:
                    date_text = first_date or last_date or raw_period
                lines.append(
                    f"- **{label}**: **{_format_signed_metric_value(row.get('amount'), metric_key, state.get('country'))}** "
                    f"({int(row.get('rows') or 0)} rows, {date_text})"
                )

        impact = result.get("business_impact") or {}
        if impact:
            burden = _safe_float(impact.get("burden"))
            cm2_pct = impact.get("cm2_percentage")
            sales_pct = impact.get("sales_percentage")
            top_group = ""
            groups = result.get("groups") or []
            if groups:
                top_row = groups[0] or {}
                top_group = " / ".join(
                    part
                    for part in [
                        str(top_row.get("type") or "").strip(),
                        str(top_row.get("description") or "").strip(),
                    ]
                    if part
                )
            impact_bits = [
                "Business impact: CM2 profit is the money left after ads and platform costs. "
                f"These charges took **{_format_metric_for_display(burden, metric_key, state.get('country'))}** "
                f"out of that margin in **{raw_period}**."
            ]
            size_bits = []
            if cm2_pct is not None:
                size_bits.append(f"**{float(cm2_pct):.1f}% of CM2 profit**")
            if sales_pct is not None:
                size_bits.append(f"**{float(sales_pct):.1f}% of net sales**")
            if size_bits:
                impact_bits.append(f"That is {' and '.join(size_bits)}, so it is worth reviewing.")
            if top_group:
                impact_bits.append(f"Start by checking **{top_group}**, because it is the biggest charge group")
            lines.append("")
            lines.append(" ".join(impact_bits).rstrip(".") + ".")

        state["final_response"] = "\n".join(lines)
        return state

    if analysis.get("type") == "business_advisor":
        comparison_context = (analysis.get("context") or {}).get("comparison") or {}
        if comparison_context.get("requested") and comparison_context.get("metrics"):
            state["final_response"] = _render_business_comparison_diagnosis(state, analysis)
            return state

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

        state["final_response"] = _render_single_metric_comparison_table_response(
            state,
            metric_name=metric_key,
            left_label=left.get("label"),
            left_value=left_value,
            right_label=right.get("label"),
            right_value=right_value,
            title=f"{product_display} inventory in {country_label}",
            summary_metric=metric_key,
        )
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

    if analysis.get("type") == "multi_metric_comparison":
        results = analysis.get("results") or []
        country_label = _country_display_name(analysis.get("country") or state.get("country"))
        if not results:
            skipped = analysis.get("skipped") or []
            skipped_text = ", ".join(humanize_metric(row.get("metric")) for row in skipped if row.get("metric"))
            state["final_response"] = (
                f"I could not find comparable data for **{country_label}**"
                + (f" for **{skipped_text}**." if skipped_text else ".")
            )
            return state

        first = results[0]
        left_label = _clean_period_label((first.get("left") or {}).get("label")) or "first period"
        right_label = _clean_period_label((first.get("right") or {}).get("label")) or "second period"
        lines = [
            f"**{country_label}: {left_label} to {right_label}**",
            "",
            "| Metric | " + _markdown_table_cell(left_label) + " | " + _markdown_table_cell(right_label) + " | Change |",
            "|---|---:|---:|---:|",
        ]

        summary_parts: List[str] = []
        for row in results:
            metric = row.get("metric") or "metric"
            left_total = _safe_float((row.get("left") or {}).get("total"))
            right_total = _safe_float((row.get("right") or {}).get("total"))
            change = row.get("display_delta")
            if change is None:
                change = right_total - left_total
            change = _safe_float(change)
            pct_change = row.get("display_pct_change")
            if pct_change is None and abs(left_total) >= 0.005:
                pct_change = (change / left_total) * 100.0
            change_text = _format_signed_metric_value(change, metric, state.get("country"))
            if pct_change is not None:
                change_text = f"{change_text} ({pct_change:+.2f}%)"
            lines.append(
                f"| {_markdown_table_cell(humanize_metric(metric))} | "
                f"{_markdown_table_cell(_format_value(left_total, metric, state.get('country')))} | "
                f"{_markdown_table_cell(_format_value(right_total, metric, state.get('country')))} | "
                f"{_markdown_table_cell(change_text)} |"
            )
            if abs(change) < 0.005:
                summary_parts.append(f"**{humanize_metric(metric)}** stayed flat")
            else:
                direction = "rose" if change > 0 else "fell"
                summary_value = _format_signed_metric_value(abs(change), metric, state.get("country"))
                pct_suffix = "" if pct_change is None else f" ({abs(float(pct_change)):.2f}%)"
                summary_parts.append(f"**{humanize_metric(metric)}** {direction} by **{summary_value}{pct_suffix}**")

        if summary_parts:
            if len(summary_parts) == 1:
                summary_text = f"{summary_parts[0]} from **{left_label}** to **{right_label}**."
            elif len(summary_parts) == 2:
                summary_text = (
                    f"From **{left_label}** to **{right_label}**, "
                    f"{summary_parts[0]} and {summary_parts[1]}."
                )
            else:
                summary_text = (
                    f"From **{left_label}** to **{right_label}**: "
                    + "; ".join(summary_parts)
                    + "."
                )
            lines.extend(["", summary_text])

        skipped = analysis.get("skipped") or []
        if skipped:
            skipped_names = ", ".join(humanize_metric(row.get("metric")) for row in skipped if row.get("metric"))
            if skipped_names:
                lines.append("")
                lines.append(f"Skipped unavailable metrics: **{skipped_names}**.")

        state["final_response"] = "\n".join(lines)
        return state

    if comp:
        left = comp.get("left", {}) or {}
        right = comp.get("right", {}) or {}
        metric_label = humanize_metric(metric_name)
        country_label = _country_display_name(analysis.get("country") or state.get("country"))
        msg = _render_single_metric_comparison_table_response(
            state,
            metric_name=metric_name,
            left_label=left.get("label"),
            left_value=left.get("total", 0.0),
            right_label=right.get("label"),
            right_value=right.get("total", 0.0),
            title=f"{country_label}: {metric_label} comparison",
            summary_metric=metric_name,
        )
        if analysis.get("growth_driver"):
            gp = analysis["growth_driver"]
            primary = gp.get("primary_driver")
            if primary:
                msg += f"\n**Primary driver:** {primary}."
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
            latest_label = series[-1].get("period_label") or period_label
            previous_label = series[-2].get("period_label") if len(series) > 1 else None

            lines = []
            lines.append(f"Your **{metric_label}** in **{latest_label}** is **{_format_value(latest, metric_name, state.get('country'))}**.")
            lines.append(
                f"It has **{direction}** by **{_format_value(abs(change), metric_name, state.get('country'))}** (**{pct:.2f}%**) vs **{previous_label or 'previous month'}**."
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
        first = series[0]
        prev = series[-2] if len(series) > 1 else None
        product_query = state.get("product_match") or state.get("product_query")
        product_text = f" for {_display_product_name(product_query)}" if product_query else ""
        lines = [
            f"{metric_label} trend{product_text} ({period_label})",
            f"Latest: **{_format_value(latest['__metric__'], metric_name, state.get('country'))}**",
        ]
        if len(series) > 1:
            first_value = float(first["__metric__"])
            latest_value = float(latest["__metric__"])
            overall_delta = latest_value - first_value
            overall_pct = (overall_delta / first_value * 100) if first_value else 0.0
            overall_direction = "increased" if overall_delta > 0 else "decreased" if overall_delta < 0 else "stayed flat"
            lines.append(
                f"Overall: **{overall_direction}** from **{_format_value(first_value, metric_name, state.get('country'))}** "
                f"in **{first.get('period_label', 'first month')}** to **{_format_value(latest_value, metric_name, state.get('country'))}** "
                f"in **{latest.get('period_label', 'latest month')}** (**{overall_pct:+.2f}%**)."
            )
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
        if metric_name == "ads_spend":
            metric_label = "Ad Spend"
        elif metric_name == "cm2_profit":
            metric_label = "CM2 Profit"

        # Sort descending safely
        rows = sorted(rows, key=lambda x: _safe_float(x.get("__metric__")), reverse=(direction == "top"))

        country_label = _country_display_name(state.get("country"))
        display_period_text = f" in **{country_label}**"
        if period_label:
            display_period_text += f" for **{period_label}**"
        descriptor = "highest" if direction == "top" else "lowest"
        header = f"{'Top' if direction == 'top' else 'Bottom'} products for **{metric_label}**{display_period_text}:"

        context = analysis.get("context") or {}

        if context.get("type") == "ranked_extreme_contribution":
            selected_month = context.get("selected_month") or {}
            extreme_rank = context.get("extreme_rank") or 1
            base_metric = analysis.get("base_metric") or metric_name
            base_metric_label = humanize_metric(base_metric)
            selected_period_label = selected_month.get("period_label") or period_label
            display_period_text = f" in **{country_label}**"
            if selected_period_label:
                display_period_text += f" for **{selected_period_label}**"

            header = (
                f"Top products for {metric_label} in "
                f"{_ordinal_label(extreme_rank)} highest {base_metric_label} month "
                f"({selected_period_label}):"
            )
        else:
            header = f"{'Top' if direction == 'top' else 'Bottom'} products for **{metric_label}**{display_period_text}:"

        query = _normalize(state.get("user_query") or "")
        requested_top_n = state.get("top_n")
        breakdown_requested = (
            analysis.get("type") == "breakdown"
            or state.get("analysis_type") == "breakdown"
            or any(
                phrase in query
                for phrase in [
                    "breakdown",
                    "product wise",
                    "product-wise",
                    "productwise",
                    "sku wise",
                    "sku-wise",
                    "by product",
                    "by sku",
                    "all products",
                    "all skus",
                    "full data",
                    "complete breakdown",
                    "full breakdown",
                ]
            )
        )
        ranking_requested = any(
            phrase in query
            for phrase in [
                "highest",
                "lowest",
                "top ",
                "bottom ",
                "best",
                "worst",
                "which product",
                "which sku",
                "most ",
                "least ",
            ]
        )
        show_full_breakdown = breakdown_requested and not requested_top_n and not ranking_requested
        if any(
            phrase in query
            for phrase in ["all products", "all skus", "full data", "complete breakdown", "full breakdown"]
        ):
            show_full_breakdown = True

        if show_full_breakdown:
            lines = [
                f"**{metric_label} by product{display_period_text}**",
                "",
                f"| Product | SKU | {metric_label} |",
                "|---|---|---:|",
            ]
            for row in rows:
                product_name = str(row.get("product_name") or "").strip()
                if not product_name or product_name == "0":
                    product_name = "Unnamed product"
                sku = str(row.get("sku") or "").strip()
                value = _safe_float(row.get("__metric__"))
                lines.append(
                    f"| {_markdown_table_cell(product_name)} | {_markdown_table_cell(sku)} | "
                    f"{_markdown_table_cell(_format_value(value, metric_name, state.get('country')))} |"
                )

            total = analysis.get("total")
            if total is not None:
                lines.append("")
                lines.append(
                    f"Total **{metric_label}** for **{period_label}**: "
                    f"**{_format_value(_safe_float(total), metric_name, state.get('country'))}**"
                )

            state["final_response"] = "\n".join(lines)
            return state

        try:
            display_limit = int(requested_top_n) if requested_top_n else 0
        except Exception:
            display_limit = 0
        if display_limit <= 0:
            display_limit = 1 if any(phrase in query for phrase in ["which product", "which sku", "which products", "which skus"]) else 5
        display_limit = max(1, min(display_limit, len(rows)))
        display_rows = rows[:display_limit]

        winner = rows[0]
        winner_name = _format_product_with_sku(
            winner.get("product_name"),
            winner.get("sku"),
            winner.get("sku") or winner.get("product_name") or "Unknown",
        )
        winner_value = _safe_float(winner.get("__metric__"))
        if len(display_rows) == 1:
            lines = [
                (
                    f"**{winner_name}** had the {descriptor} **{metric_label}**"
                    f"{display_period_text}: **{_format_value(winner_value, metric_name, state.get('country'))}**."
                )
            ]
        else:
            lines = [header]
            for idx, row in enumerate(display_rows, 1):
                name = _format_product_with_sku(
                    row.get("product_name"),
                    row.get("sku"),
                    row.get("sku") or row.get("product_name") or "Unknown",
                )
                value = _safe_float(row.get("__metric__"))
                lines.append(f"{idx}. **{name}** - **{_format_value(value, metric_name, state.get('country'))}**")

        highest_month = analysis.get("highest_month") or {}
        payload_type = (state.get("period_payload") or {}).get("type")
        if (
            payload_type in {"range", "last_n_months", "comparison"}
            and highest_month.get("period_label")
            and highest_month.get("value") is not None
        ):
            lines.append("")
            lines.append(
                f"Highest **{metric_label}** month in this period: **{highest_month.get('period_label')}** - "
                f"**{_format_value(float(highest_month.get('value', 0.0)), metric_name, state.get('country'))}**"
            )

        total = analysis.get("total")
        if total is not None and len(display_rows) > 1:
            lines.append("")
            lines.append(
                f"All-product total **{metric_label}** for **{period_label}**: "
                f"**{_format_value(_safe_float(total), metric_name, state.get('country'))}**"
            )

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
        inventory_focused = any(term in (state.get("user_query") or "").lower() for term in ["inventory", "stock", "dispatch"])
        lines = [f"**Forecast summary for {title_period}**"]

        for result in analysis.get("results", []):
            country = result.get("country")
            country_label = result.get("country_label") or _country_display_name(country)
            inventory = result.get("inventory") or {}
            alignment = result.get("inventory_alignment") or {}
            pnl = result.get("pnl") or {}

            country_parts = []
            if inventory.get("available"):
                forecast_units_map = (inventory.get("totals") or {}).get("forecast_units") or {}
                requested_col = inventory.get("requested_forecast_column")
                requested_missing = bool(requested_month and requested_year and not inventory.get("requested_forecast_available"))
                if requested_missing:
                    units_text = ""
                elif requested_col and requested_col in forecast_units_map:
                    units_text = f"{requested_col}: {float(forecast_units_map.get(requested_col) or 0.0):,.0f} units"
                else:
                    units_text = _format_forecast_units_map(forecast_units_map)
                projected_total = (inventory.get("totals") or {}).get("projected_sales_total")
                if units_text:
                    country_parts.append(f"forecast units **{units_text}**")
                if projected_total and not requested_col:
                    country_parts.append(f"projected total **{float(projected_total):,.0f} units**")

                if alignment.get("available"):
                    country_parts.append(f"current available **{alignment.get('current_available_formatted')}**")
                    if alignment.get("status") == "covered":
                        country_parts.append(f"status **covered with {alignment.get('gap_formatted')} surplus**")
                    else:
                        country_parts.append(f"status **short by {alignment.get('gap_formatted')}**")
                elif alignment and alignment.get("available") is False:
                    country_parts.append("current inventory unavailable to confirm update status")

            show_pnl = bool(
                not inventory_focused
                and
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

            if (
                inventory.get("available")
                and requested_month
                and requested_year
                and not inventory.get("requested_forecast_available")
            ):
                available_months = ", ".join(inventory.get("forecast_columns") or []) or "none"
                lines.append(f"  Forecast column for **{title_period}** was not found. Available forecast months: **{available_months}**.")
            if not inventory_focused and pnl.get("available") and not show_pnl:
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
                top_units = top_row.get("requested_forecast_units")
                if not top_units:
                    top_units = top_row.get("projected_sales_total")
                unit_label = f" in {inventory.get('requested_forecast_column')}" if inventory.get("requested_forecast_column") else ""
                lines.append(
                    f"  Top forecasted product: **{_format_product_with_sku(top_row.get('product_name'), top_row.get('sku'), top_row.get('sku'))}** "
                    f"({float(top_units or 0.0):,.0f} units{unit_label})."
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
                lines.append(f"Total for **{period_text}**:")
                for product, total in product_totals.items():
                    lines.append(f"- **{product}**: **{_format_value(total, metric, state.get('country'))}**")
            else:
                lines.append("")
                lines.append(f"Total for **{period_text}**: **{_format_value(metric_total, metric, state.get('country'))}**")
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


def _compact_followup_prompt_value(value: Any, max_chars: int = 5000) -> Any:
    try:
        encoded = json.dumps(value, default=str)
    except Exception:
        encoded = str(value)

    if len(encoded) <= max_chars:
        return value

    if not isinstance(value, dict):
        return f"{encoded[:max_chars]}...[truncated]"

    compact: Dict[str, Any] = {}
    keep_keys = [
        "type",
        "metric",
        "metric_name",
        "period_label",
        "label",
        "month",
        "year",
        "months_used",
        "months_found",
        "total",
        "totals",
        "series",
        "mom",
        "items",
        "top_products",
        "results",
        "comparison",
        "product_match",
        "country",
        "target_countries",
    ]

    for key in keep_keys:
        if key not in value:
            continue
        item = value.get(key)
        if isinstance(item, list):
            compact[key] = item[:8]
        elif isinstance(item, dict):
            compact[key] = {
                child_key: (child_value[:8] if isinstance(child_value, list) else child_value)
                for child_key, child_value in list(item.items())[:18]
            }
        else:
            compact[key] = item

    compact["_truncated_for_followup_prompt"] = True
    return compact


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
            "previous_current_metrics": _compact_followup_prompt_value(last_meta.get("current_metrics")),
            "previous_analysis_result": _compact_followup_prompt_value(last_meta.get("analysis_result")),
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


_MONTH_NAME_TO_NUMBER = {
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

_MONTH_YEAR_RE = re.compile(
    r"\b("
    r"jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|"
    r"jul(?:y)?|aug(?:ust)?|sep(?:t|tember)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?"
    r")\s+(\d{4})\b",
    re.IGNORECASE,
)


def _coerce_month_year(value: Any) -> Optional[Dict[str, int]]:
    if not value:
        return None

    if isinstance(value, MonthKey):
        return {"month": int(value.month), "year": int(value.year)}

    if isinstance(value, dict):
        month = value.get("month")
        year = value.get("year")
        if month and year:
            try:
                return {"month": int(month), "year": int(year)}
            except Exception:
                return None
        label = value.get("label") or value.get("period_label")
        if label:
            matches = _month_year_matches(label)
            return matches[0] if matches else None
        return None

    month = getattr(value, "month", None)
    year = getattr(value, "year", None)
    if month and year:
        try:
            return {"month": int(month), "year": int(year)}
        except Exception:
            return None

    matches = _month_year_matches(value)
    return matches[0] if matches else None


def _month_year_matches(text_value: Any) -> List[Dict[str, int]]:
    text_value = str(text_value or "")
    matches: List[Dict[str, int]] = []
    for month_name, year in _MONTH_YEAR_RE.findall(text_value):
        month = _MONTH_NAME_TO_NUMBER.get(month_name.lower())
        if month:
            matches.append({"month": month, "year": int(year)})
    return matches


def _range_period_from_months(months: List[Dict[str, int]]) -> Optional[Dict[str, Any]]:
    cleaned: List[Dict[str, int]] = []
    seen: Set[Tuple[int, int]] = set()
    for item in months:
        try:
            month = int(item["month"])
            year = int(item["year"])
        except Exception:
            continue
        key = (year, month)
        if key in seen:
            continue
        seen.add(key)
        cleaned.append({"month": month, "year": year})

    if not cleaned:
        return None

    cleaned.sort(key=lambda item: (item["year"], item["month"]))

    if len(cleaned) == 1:
        only = cleaned[0]
        return {"type": "single", "month": only["month"], "year": only["year"]}

    first = cleaned[0]
    last = cleaned[-1]
    return {
        "type": "range",
        "start_month": first["month"],
        "start_year": first["year"],
        "end_month": last["month"],
        "end_year": last["year"],
    }


def _period_from_month_entries(entries: Any) -> Optional[Dict[str, Any]]:
    if not isinstance(entries, list):
        return None
    months = []
    for entry in entries:
        parsed = _coerce_month_year(entry)
        if parsed:
            months.append(parsed)
    return _range_period_from_months(months)


def _period_from_label(label: Any) -> Optional[Dict[str, Any]]:
    matches = _month_year_matches(label)
    return _range_period_from_months(matches)


def _previous_period_from_history(
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

    for source_name, source in [("current_metrics", current), ("analysis_result", analysis)]:
        for key in ["months_found", "months_used"]:
            period = _period_from_month_entries(source.get(key))
            if period:
                logger.info(f"[FOLLOWUP_PERIOD] Reusing previous {source_name}.{key}: {period}")
                return period

    for source_name, source in [("current_metrics", current), ("analysis_result", analysis)]:
        for key in ["period_label", "label"]:
            period = _period_from_label(source.get(key))
            if period:
                logger.info(f"[FOLLOWUP_PERIOD] Reusing previous {source_name}.{key}: {period}")
                return period

    if parsed.get("type") and parsed.get("type") != "latest_month":
        logger.info(f"[FOLLOWUP_PERIOD] Reusing previous parsed period: {parsed}")
        return parsed

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
        logger.info("[FOLLOWUP_PERIOD] No reusable month/range found in previous result")
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


def _previous_single_period_from_history(
    history: List[Dict[str, Any]]
) -> Optional[Dict[str, Any]]:
    return _previous_period_from_history(history)


def _query_has_explicit_period(parsed_period: Optional[Dict[str, Any]], query: str) -> bool:
    parsed_period = parsed_period or {}
    period_type = parsed_period.get("type")

    if period_type and period_type != "latest_month":
        return True

    normalized = _normalize(query)
    explicit_latest_terms = [
        "latest month",
        "latest period",
        "current month",
        "current period",
        "current data",
        "this month",
        "mtd",
        "month to date",
        "today",
        "now",
        "to date",
    ]
    return any(term in normalized for term in explicit_latest_terms)


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
        has_product = any(
            w in q_lower
            for w in [
                "productwise",
                "product-wise",
                "product wise",
                "by product",
                "per product",
                "product level",
                "product-level",
                "sku",
            ]
        )
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
                "product-wise",
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
            f"answer_shape={plan.answer_shape}, expected_shape={plan.expected_result_shape}, "
            f"reasoning_mode={plan.reasoning_mode}, "
            f"needs_forecast_data={plan.needs_forecast_data}"
        )
        # -------- 🔥 LLM-BASED FOLLOW-UP RESOLUTION --------
        semantic_resolution = resolve_query_semantics(
            q_lower,
            llm=planner_llm,
            plan_hint=_plan_hint_from_plan(plan),
        )
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

        state["dimension"] = plan.dimension or state.get("dimension")
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
        state["expected_result_shape"] = plan.expected_result_shape
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

        _apply_semantic_resolution(state, semantic_resolution)

        # -------- 🔥 APPLY LLM FOLLOW-UP OVERRIDES --------
        if followup_resolution and followup_resolution.is_followup:
            logger.info("[FOLLOWUP_APPLY] Applying follow-up resolution")

            current_has_explicit_metric = bool(
                plan.metric_name
                or plan.metric_names
                or state.get("metric_name")
                or (semantic_resolution and getattr(semantic_resolution, "primary_metric_name", None))
            )
            current_has_explicit_series_shape = _query_asks_monthly_series(state)
            current_has_explicit_analysis = bool(
                (plan.analysis_type and plan.analysis_type != "absolute")
                or plan.answer_shape
                or plan.expected_result_shape
                or state.get("answer_shape")
                or state.get("expected_result_shape")
                or current_has_explicit_series_shape
            )

            if _requires_business_advisor(state):
                logger.info("[FOLLOWUP_APPLY] Preserving business advisor routing")
                if followup_resolution.top_n:
                    state["top_n"] = followup_resolution.top_n
            else:
                if followup_resolution.metric_name:
                    if current_has_explicit_metric:
                        logger.info(
                            "[FOLLOWUP_APPLY] Keeping current metric=%s; resolver suggested=%s",
                            state.get("metric_name"),
                            followup_resolution.metric_name,
                        )
                    else:
                        state["metric_name"] = followup_resolution.metric_name
                        state["metric_names"] = [followup_resolution.metric_name]

                if followup_resolution.analysis_type:
                    if current_has_explicit_analysis:
                        logger.info(
                            "[FOLLOWUP_APPLY] Keeping current analysis_type=%s; resolver suggested=%s",
                            state.get("analysis_type"),
                            followup_resolution.analysis_type,
                        )
                    else:
                        state["analysis_type"] = followup_resolution.analysis_type

                if followup_resolution.answer_shape:
                    if current_has_explicit_analysis:
                        logger.info(
                            "[FOLLOWUP_APPLY] Keeping current answer_shape=%s; resolver suggested=%s",
                            state.get("answer_shape"),
                            followup_resolution.answer_shape,
                        )
                    else:
                        state["answer_shape"] = followup_resolution.answer_shape

                if followup_resolution.ranking_direction:
                    if not current_has_explicit_series_shape:
                        state["ranking_direction"] = followup_resolution.ranking_direction

                if followup_resolution.dimension:
                    if not current_has_explicit_series_shape and not state.get("dimension"):
                        state["dimension"] = followup_resolution.dimension

                if followup_resolution.top_n:
                    if not current_has_explicit_series_shape:
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

        if _is_anomaly_request(state):
            logger.info("[ANOMALY_ROUTE] Broad anomaly scan enabled")
            state["intent"] = "report"
            state["analysis_type"] = "anomaly_scan"
            state["answer_shape"] = "summary"
            state["reasoning_mode"] = "analysis"
            state["task_type"] = "diagnosis"
            state["needs_advice"] = True
            if not state.get("metric_names"):
                state["metric_names"] = ANOMALY_SCAN_METRICS
            if not state.get("metric_name"):
                state["metric_name"] = "profit"

        if _is_broad_business_analysis_request(state):
            primary_metric = state.get("metric_name") or "profit"
            metric_pack = [primary_metric] + [
                metric for metric in BUSINESS_ANALYSIS_DEFAULT_METRICS
                if metric != primary_metric
            ]
            logger.info("[BUSINESS_ANALYSIS_ROUTE] Using broad metric pack with primary=%s", primary_metric)
            state["intent"] = "report"
            state["metric_name"] = primary_metric
            state["metric_names"] = metric_pack
            state["subject_scope"] = "business"
            state["reasoning_mode"] = "analysis"
            state["task_type"] = "diagnosis"
            state["needs_advice"] = True
            state["use_multi_metric"] = True

        _ensure_expected_result_shape(state)

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
            elif _is_broad_business_analysis_request(state):
                logger.info("[BUSINESS_ANALYSIS_ROUTE] Recovered broad business metric pack")
                state["metric_name"] = "profit"
                state["metric_names"] = BUSINESS_ANALYSIS_DEFAULT_METRICS
                state["subject_scope"] = "business"
                state["task_type"] = "diagnosis"
                state["needs_advice"] = True
                state["use_multi_metric"] = True

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
            if _query_has_explicit_period(parsed_period, q_lower):
                logger.info("[FOLLOWUP_PERIOD] Explicit period in current query; not inheriting previous period")
            elif (
                _is_forecast_request(state)
                and parsed_period.get("type") == "latest_month"
                and not _forecast_query_has_contextual_period_reference(q_lower)
            ):
                logger.info("[FORECAST_PERIOD] Generic forecast request -> using latest available forecast, not inherited chat period")
            else:
                inherited_period = _previous_period_from_history(history)

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
        _ensure_expected_result_shape(state)

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

        _apply_product_scoped_metric_mapping(state)
        _prepare_metrics_for_direct_tools(state)

        # -------- METRIC COMPATIBILITY CHECK --------
        metric_name = state.get("metric_name")
        product_query = state.get("product_query")
        analysis_type = state.get("analysis_type")

        if metric_name and state.get("expected_result_shape") != "raw_line_items":
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

        state = _validate_and_replan_execution(state)

        # -------- FINAL --------
        state = _render_response(state)
        state = _ensure_period_context(state)

        # -------- EMAIL --------
        state = _send_email_if_requested(state)

        logger.info("[END] Response generated successfully")
        return state

    except Exception as exc:
        logger.exception("[FATAL ERROR] Agent execution failed")

        if _is_insufficient_quota_error(exc):
            state["error"] = "insufficient_quota"
            state["insufficient_balance"] = True
            state["final_response"] = INSUFFICIENT_BALANCE_MESSAGE
            return state

        state["error"] = str(exc)
        state["final_response"] = f"Agent failed: {str(exc)}"
        return state


def build_graph() -> SimpleGraph:
    return SimpleGraph()
