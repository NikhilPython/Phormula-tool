from __future__ import annotations

import json
import logging
import os
import re
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeout
from copy import deepcopy
from typing import Any, Dict, List, Optional
from uuid import uuid4

from langchain_openai import ChatOpenAI
from pydantic import BaseModel, Field
from openai import RateLimitError

from app.ai_agent.graph import build_graph
from app.ai_agent.memory import recent_chat_history, save_chat_turn

try:
    from flask import current_app, has_app_context
except Exception:  # pragma: no cover - keeps this service importable outside Flask
    current_app = None
    has_app_context = None

logger = logging.getLogger(__name__)

_graph = build_graph()


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default


_agent_executor = ThreadPoolExecutor(
    max_workers=max(1, _env_int("AI_AGENT_WORKERS", 4)),
    thread_name_prefix="ai-agent-run",
)
_postprocess_executor = ThreadPoolExecutor(
    max_workers=max(1, _env_int("AI_AGENT_POSTPROCESS_WORKERS", 4)),
    thread_name_prefix="ai-agent-postprocess",
)
_agent_timeout_seconds = _env_float("AI_AGENT_RUN_TIMEOUT_SECONDS", 90.0)
_verification_timeout_seconds = _env_float("AI_AGENT_VERIFICATION_TIMEOUT_SECONDS", 0.0)
_suggestion_timeout_seconds = _env_float("AI_AGENT_SUGGESTION_TIMEOUT_SECONDS", 8.0)


def _run_with_timeout(
    *,
    label: str,
    executor: ThreadPoolExecutor,
    timeout_seconds: float,
    func,
    default: Any,
) -> Any:
    if timeout_seconds <= 0:
        return func()

    future = executor.submit(func)
    try:
        return future.result(timeout=timeout_seconds)
    except FutureTimeout:
        logger.warning("[%s_TIMEOUT] exceeded %.1fs; returning fallback", label, timeout_seconds)
        return default
    except Exception:
        logger.exception("[%s_FAILED] returning fallback", label)
        return default


def _current_flask_app():
    try:
        if has_app_context and has_app_context():
            return current_app._get_current_object()
    except Exception:
        logger.debug("No Flask app context available for AI agent worker", exc_info=True)
    return None


def _invoke_graph_with_optional_app_context(
    state: Dict[str, Any],
    app: Any = None,
) -> Dict[str, Any]:
    graph_state = deepcopy(state)
    if app is not None:
        with app.app_context():
            return _graph.invoke(graph_state)
    return _graph.invoke(graph_state)


def _strip_runtime_state(result: Dict[str, Any]) -> Dict[str, Any]:
    clean_result = dict(result or {})
    for key in ("engine",):
        clean_result.pop(key, None)
    return clean_result


def _safe_result_copy(result: Dict[str, Any]) -> Dict[str, Any]:
    clean_result = _strip_runtime_state(result)
    try:
        return deepcopy(clean_result)
    except Exception:
        logger.exception("Failed to deep-copy agent result; using shallow runtime-stripped copy")
        return dict(clean_result)


_suggestion_llm = (
    ChatOpenAI(
        model=os.getenv("AI_AGENT_SUGGESTION_MODEL", "gpt-4.1-mini"),
        api_key=os.getenv("OPENAI_API_KEY"),
        temperature=0.3,
    )
    if os.getenv("OPENAI_API_KEY")
    else None
)
_verifier_llm = (
    ChatOpenAI(
        model=os.getenv("AI_AGENT_VERIFIER_MODEL", "gpt-4.1-mini"),
        api_key=os.getenv("OPENAI_API_KEY"),
        temperature=0,
    )
    if os.getenv("OPENAI_API_KEY")
    else None
)

DEFAULT_THRESHOLDS = {
    "low_profit_margin": 10.0,
    "high_amazon_fee_ratio": 25.0,
    "high_advertising_ratio": 15.0,
}


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


def _strip_nul_text(value: Any) -> str:
    return str(value or "").replace("\x00", "")


def _enforce_cm1_profit_terms(value: Any) -> str:
    text = _strip_nul_text(value)
    replacements = [
        (r"(?<!CM1 )(?<!CM2 )\bprofit margins\b", "CM1 profit margins"),
        (r"(?<!CM1 )(?<!CM2 )\bprofit margin\b", "CM1 profit margin"),
        (r"(?<!CM1 )(?<!CM2 )\bprofit shares\b", "CM1 profit shares"),
        (r"(?<!CM1 )(?<!CM2 )\bprofit share\b", "CM1 profit share"),
        (r"(?<!CM1 )(?<!CM2 )\bprofit mixes\b", "CM1 profit mixes"),
        (r"(?<!CM1 )(?<!CM2 )\bprofit mix\b", "CM1 profit mix"),
        (r"(?<!CM1 )(?<!CM2 )\bprofit percentages\b", "CM1 profit percentages"),
        (r"(?<!CM1 )(?<!CM2 )\bprofit percentage\b", "CM1 profit percentage"),
        (r"(?<!CM1 )(?<!CM2 )\bprofit %(?=\W|$)", "CM1 profit %"),
        (r"(?<!CM1 )(?<!CM2 )\bprofits\b", "CM1 profits"),
        (r"(?<!CM1 )(?<!CM2 )\bprofit\b", "CM1 profit"),
    ]
    for pattern, replacement in replacements:
        text = re.sub(pattern, replacement, text, flags=re.IGNORECASE)
    return text


class SuggestedQuestionModel(BaseModel):
    questions: List[str] = Field(default_factory=list, max_length=3)


class AnswerVerificationModel(BaseModel):
    is_valid: bool = True
    confidence: float = Field(default=1.0, ge=0.0, le=1.0)
    issues: List[str] = Field(default_factory=list)
    corrected_answer: Optional[str] = None


def _humanize_metric(metric: Optional[str]) -> str:
    if not metric:
        return "business performance"

    key = str(metric).strip().lower()
    replacements = {
        "net_sales": "net sales",
        "gross_sales": "gross sales",
        "quantity": "gross units",
        "total_quantity": "net sold units",
        "return_quantity": "refund quantity",
        "profit": "CM1 profit",
        "cm2_profit": "SKU CM2 profit",
        "total_cm2_profit": "account CM2 profit",
        "profit_percentage": "CM1 profit margin",
        "profit_mix": "CM1 profit mix",
        "acos": "ACOS",
        "asp": "ASP",
        "ads_spend": "ad spend",
        "total_ads": "ad spend",
        "promotional_rebates": "promo rebates",
        "platformfeenew": "subscription fees",
        "refund_sales": "refund sales",
        "product_spend": "Sponsored Product spend",
        "brand_spend": "Sponsored Brand spend",
        "display_spend": "Sponsored Display spend",
        "sp_ads_sales": "Sponsored Product ad sales",
        "sb_ads_sales": "Sponsored Brand ad sales",
        "sd_ads_sales": "Sponsored Display ad sales",
        "available": "available inventory",
        "inbound_quantity": "inbound inventory",
        "total_reserved_quantity": "reserved inventory",
        "unfulfillable_quantity": "unfulfillable inventory",
        "days_of_supply": "days of supply",
        "sell_through": "sell-through",
        "estimated_excess_quantity": "excess inventory",
    }
    return replacements.get(key, key.replace("_", " "))


def _country_label(country: Optional[str]) -> str:
    code = (country or "").strip().lower()
    labels = {
        "uk": "Amazon UK",
        "gb": "Amazon UK",
        "us": "Amazon US",
        "usa": "Amazon US",
        "global": "all marketplaces",
    }
    return labels.get(code, f"Amazon {code.upper()}" if code else "this marketplace")


def _product_phrase(result: Dict[str, Any]) -> str:
    product = result.get("product_query")
    if not product:
        products = result.get("product_queries") or []
        if products:
            product = products[0]
    if not product:
        analysis = result.get("analysis_result") or {}
        product = analysis.get("product")
    return f" for {str(product).strip()}" if product else ""


def _period_phrase(result: Dict[str, Any]) -> str:
    current = result.get("current_metrics") or {}
    period = current.get("period_label")
    if period:
        return f" in {period}"

    parsed = result.get("period_parsed") or {}
    if parsed.get("type") == "multi_month":
        months = parsed.get("months") or []
        if months:
            first = months[0]
            last = months[-1]
            return f" from {first.get('month')}/{first.get('year')} to {last.get('month')}/{last.get('year')}"

    return ""


def _fallback_suggested_questions(result: Dict[str, Any], user_query: str) -> List[str]:
    intent = result.get("intent")
    if intent in {"clarify", "chat", "explain"} or result.get("error"):
        return []

    metric_names = result.get("metric_names") or []
    metric = result.get("metric_name") or (metric_names[0] if metric_names else "net_sales")
    metric_label = _humanize_metric(metric)
    country = _country_label(result.get("country"))
    product = _product_phrase(result)
    period = _period_phrase(result)
    analysis = result.get("analysis_result") or {}
    analysis_type = analysis.get("type") or result.get("analysis_type")

    suggestions: List[str] = []

    def add(question: str) -> None:
        cleaned = " ".join(str(question).split()).strip()
        if not cleaned:
            return
        if cleaned.lower() == (user_query or "").strip().lower():
            return
        if cleaned not in suggestions:
            suggestions.append(cleaned)

    inventory_metrics = {
        "available",
        "inbound_quantity",
        "total_reserved_quantity",
        "unfulfillable_quantity",
        "sell_through",
        "days_of_supply",
        "units_shipped_t30",
        "units_shipped_t60",
        "units_shipped_t90",
        "estimated_excess_quantity",
    }
    ad_metrics = {
        "ads_spend",
        "total_ads",
        "product_spend",
        "brand_spend",
        "display_spend",
        "sp_ads_sales",
        "sb_ads_sales",
        "sd_ads_sales",
        "ads_sale_amount",
        "acos",
    }

    if metric in inventory_metrics or analysis_type == "inventory_diagnosis":
        add(f"Which SKUs are at stock-out risk in {country}?")
        add(f"Which products have excess inventory in {country}?")
        add(f"How much stock should I plan for next month in {country}?")
    elif metric in ad_metrics:
        add(f"Which SKUs are wasting ad spend in {country}{period}?")
        add(f"How should I optimize ads to improve sales in {country}?")
        add(f"Compare Sponsored Product, Brand, and Display spend in {country}{period}.")
    elif analysis_type in {"business_advisor", "decision"}:
        add(f"What are the top 3 actions I should take next month in {country}?")
        add(f"Which products are hurting profit the most in {country}?")
        add(f"Where can I reduce fees or ad waste in {country}?")
    elif analysis_type in {"multi_month", "trend", "growth"}:
        add(f"What is the month-on-month change in {metric_label}{product} in {country}?")
        add(f"Which product contributed most to {metric_label} in {country}{period}?")
        add(f"Why did {metric_label}{product} change during this period?")
    elif analysis_type in {"ranking", "breakdown"}:
        add(f"Show bottom 5 products by {metric_label} in {country}{period}.")
        add(f"Why are the lowest {metric_label} products underperforming?")
        add(f"What should I do to improve profit for these products?")
    else:
        add(f"Show {metric_label}{product} trend for the last 6 months in {country}.")
        add(f"Compare {metric_label}{product} with CM1 profit in {country}{period}.")
        add(f"What should I do next to improve {metric_label}{product}?")

    return suggestions[:3]


def _clean_suggested_questions(questions: List[Any], user_query: str) -> List[str]:
    cleaned_questions: List[str] = []
    original = (user_query or "").strip().lower()

    for item in questions or []:
        question = _enforce_cm1_profit_terms(" ".join(str(item or "").replace("\n", " ").split()).strip())
        if not question:
            continue
        if question.lower() == original:
            continue
        if len(question) > 140:
            question = question[:137].rstrip() + "..."
        if not question.endswith("?"):
            question = question.rstrip(".") + "?"
        if question not in cleaned_questions:
            cleaned_questions.append(question)
        if len(cleaned_questions) == 3:
            break

    return cleaned_questions


def _compact_json(value: Any, max_chars: int = 4000) -> str:
    try:
        text = json.dumps(value, default=str, ensure_ascii=True)
    except Exception:
        text = str(value)
    if len(text) > max_chars:
        return text[:max_chars] + "...[truncated]"
    return text


def _compact_business_context_for_verification(context: Dict[str, Any]) -> Dict[str, Any]:
    context = context or {}
    comparison = context.get("comparison") or {}
    totals = context.get("totals") or {}
    left = comparison.get("left") or {}
    right = comparison.get("right") or {}
    important_total_keys = [
        "profit",
        "net_sales",
        "quantity",
        "total_quantity",
        "asp",
        "gross_sales",
        "product_sales",
        "promotional_rebates",
        "refund_sales",
        "return_quantity",
        "cogs",
        "selling_fees",
        "fba_fees",
        "amazon_fees",
        "platform_fee",
        "platformfeenew",
        "platform_fee_inventory_storage",
        "ads_spend",
        "total_ads",
        "product_spend",
        "display_spend",
        "brand_spend",
        "total_cm2_profit",
        "cm2_profit",
    ]

    return {
        "scope": context.get("scope"),
        "period": context.get("period"),
        "totals": {key: totals.get(key) for key in important_total_keys if key in totals},
        "derived": context.get("derived"),
        "comparison": {
            "requested": comparison.get("requested"),
            "left": {
                "label": left.get("label"),
                "months": left.get("months"),
                "data_available": left.get("data_available"),
            },
            "right": {
                "label": right.get("label"),
                "months": right.get("months"),
                "data_available": right.get("data_available"),
            },
            "metrics": comparison.get("metrics"),
            "metric_drivers": (comparison.get("metric_drivers") or [])[:8],
            "unfavorable_metric_drivers": (comparison.get("unfavorable_metric_drivers") or [])[:8],
            "favorable_metric_drivers": (comparison.get("favorable_metric_drivers") or [])[:5],
            "driver_summary": comparison.get("driver_summary"),
            "total_only_metrics": comparison.get("total_only_metrics"),
            "productwise_available_metrics": comparison.get("productwise_available_metrics"),
            "top_negative_profit_drivers": (comparison.get("top_negative_profit_drivers") or [])[:5],
            "top_positive_profit_drivers": (comparison.get("top_positive_profit_drivers") or [])[:3],
            "top_unit_loss_drivers": (comparison.get("top_unit_loss_drivers") or [])[:5],
            "top_order_loss_drivers": (comparison.get("top_order_loss_drivers") or [])[:5],
            "top_cm2_loss_drivers": (comparison.get("top_cm2_loss_drivers") or [])[:5],
            "top_sales_loss_drivers": (comparison.get("top_sales_loss_drivers") or [])[:5],
            "top_rebate_burden_drivers": (comparison.get("top_rebate_burden_drivers") or [])[:5],
            "diagnosis_inventory": comparison.get("diagnosis_inventory"),
            "driver_scan_note": comparison.get("driver_scan_note"),
        },
        "inventory": context.get("inventory"),
        "data_quality": context.get("data_quality"),
    }


def _compact_analysis_for_verification(analysis: Dict[str, Any]) -> Dict[str, Any]:
    analysis = analysis or {}

    if analysis.get("type") == "anomaly_scan":
        compact_blocks = []
        for block in analysis.get("metric_blocks") or []:
            compact_blocks.append({
                "metric": block.get("metric"),
                "months": (block.get("months") or [])[:6],
            })
            if len(compact_blocks) >= 8:
                break
        return {
            "type": "anomaly_scan",
            "country": analysis.get("country"),
            "period_label": analysis.get("period_label"),
            "metrics": analysis.get("metrics"),
            "focus_scope": analysis.get("focus_scope"),
            "product_scope": analysis.get("product_scope"),
            "product_unavailable_metrics": analysis.get("product_unavailable_metrics"),
            "anomalies": (analysis.get("anomalies") or [])[:8],
            "metric_blocks": compact_blocks,
        }

    if analysis.get("type") == "forecast":
        compact_results = []
        for result in analysis.get("results") or []:
            inventory = result.get("inventory") or {}
            pnl = result.get("pnl") or {}
            compact_results.append({
                "country": result.get("country"),
                "country_label": result.get("country_label"),
                "period_label": result.get("period_label"),
                "inventory": {
                    "available": inventory.get("available"),
                    "stored_month": inventory.get("stored_month"),
                    "stored_year": inventory.get("stored_year"),
                    "exact_period_match": inventory.get("exact_period_match"),
                    "requested_forecast_column": inventory.get("requested_forecast_column"),
                    "requested_forecast_available": inventory.get("requested_forecast_available"),
                    "forecast_columns": inventory.get("forecast_columns"),
                    "totals": inventory.get("totals"),
                    "row_count": inventory.get("row_count"),
                },
                "inventory_alignment": result.get("inventory_alignment"),
                "top_inventory": (result.get("top_inventory") or [])[:3],
                "pnl": {
                    "available": pnl.get("available"),
                    "stored_month": pnl.get("stored_month"),
                    "stored_year": pnl.get("stored_year"),
                    "exact_period_match": pnl.get("exact_period_match"),
                    "totals": pnl.get("totals"),
                    "row_count": pnl.get("row_count"),
                },
            })
        return {
            "type": "forecast",
            "period_label": analysis.get("period_label"),
            "requested_month": analysis.get("requested_month"),
            "requested_year": analysis.get("requested_year"),
            "product": analysis.get("product"),
            "results": compact_results,
        }

    if analysis.get("type") == "multi_metric_comparison":
        compact_results = []
        for row in analysis.get("results") or []:
            left = row.get("left") or {}
            right = row.get("right") or {}
            compact_results.append({
                "metric": row.get("metric"),
                "left": {
                    "label": left.get("label"),
                    "total": left.get("total"),
                },
                "right": {
                    "label": right.get("label"),
                    "total": right.get("total"),
                },
                "display_delta": row.get("display_delta"),
                "display_pct_change": row.get("display_pct_change"),
                "display_delta_basis": row.get("display_delta_basis"),
            })
        return {
            "type": "multi_metric_comparison",
            "country": analysis.get("country"),
            "metrics": analysis.get("metrics"),
            "results": compact_results,
            "skipped": analysis.get("skipped"),
        }

    compact = {
        key: value
        for key, value in analysis.items()
        if key != "context"
    }
    if isinstance(analysis.get("context"), dict):
        compact["context"] = _compact_business_context_for_verification(analysis.get("context") or {})
    return compact


def _answer_verification_context(result: Dict[str, Any], user_query: str) -> Dict[str, Any]:
    analysis = result.get("analysis_result") or {}
    business_context = result.get("business_context") or {}

    return {
        "user_query": user_query,
        "final_answer": result.get("final_response"),
        "intent": result.get("intent"),
        "analysis_type": result.get("analysis_type"),
        "answer_shape": result.get("answer_shape"),
        "country": result.get("country"),
        "target_countries": result.get("target_countries"),
        "metric_name": result.get("metric_name"),
        "metric_names": result.get("metric_names"),
        "semantic_resolution": result.get("semantic_resolution"),
        "product_query": result.get("product_query"),
        "product_queries": result.get("product_queries"),
        "period_parsed": result.get("period_parsed"),
        "period_payload": result.get("period_payload"),
        "current_metrics": result.get("current_metrics"),
        "comparison": result.get("comparison"),
        "analysis_result_type": analysis.get("type"),
        "analysis_result": _compact_analysis_for_verification(analysis),
        "business_context": _compact_business_context_for_verification(business_context),
        "advice": result.get("advice"),
        "event_plan_result": result.get("event_plan_result"),
        "sku_intelligence_result": result.get("sku_intelligence_result"),
        "tool_trace": result.get("tool_trace"),
        "error": result.get("error"),
    }


def _clean_verification_issues(issues: List[Any]) -> List[str]:
    cleaned: List[str] = []
    for item in issues or []:
        issue = " ".join(str(item or "").split()).strip()
        if issue and issue not in cleaned:
            cleaned.append(issue[:240])
        if len(cleaned) == 5:
            break
    return cleaned


def _validate_deterministic_comparison(result: Dict[str, Any]) -> bool:
    analysis = result.get("analysis_result") or {}
    if analysis.get("type") != "multi_metric_comparison":
        return False

    rows = analysis.get("results") or []
    if not rows:
        return False

    issues: List[str] = []
    for row in rows:
        left = row.get("left") or {}
        right = row.get("right") or {}
        try:
            left_total = float(left.get("total") or 0.0)
            right_total = float(right.get("total") or 0.0)
        except (TypeError, ValueError):
            issues.append(f"{row.get('metric') or 'metric'} has non-numeric comparison totals")
            continue

        actual_delta = row.get("display_delta")
        if actual_delta is None:
            issues.append(f"{row.get('metric') or 'metric'} is missing display_delta")
            continue

        try:
            actual_delta = float(actual_delta)
        except (TypeError, ValueError):
            issues.append(f"{row.get('metric') or 'metric'} has non-numeric display_delta")
            continue

        expected_delta = right_total - left_total
        if abs(expected_delta - actual_delta) > 0.01:
            issues.append(f"{row.get('metric') or 'metric'} display_delta does not match right minus left")

    result["answer_validation"] = {
        "status": "passed" if not issues else "flagged",
        "reason": "deterministic_multi_metric_comparison",
        "is_valid": not issues,
        "confidence": 1.0 if not issues else 0.0,
        "issues": issues[:5],
        "corrected": False,
    }
    logger.info(
        "[ANSWER_VALIDATION] status=%s corrected=False reason=deterministic_multi_metric_comparison issues=%s",
        result["answer_validation"]["status"],
        issues[:2],
    )
    return True


def _validate_deterministic_table_answer(result: Dict[str, Any]) -> bool:
    analysis = result.get("analysis_result") or {}
    answer = result.get("final_response") or ""
    result_type = analysis.get("type")
    structured_types = {
        "anomaly_scan",
        "business_advisor",
        "comparison",
        "inventory_comparison",
        "multi_metric_comparison",
    }
    has_markdown_table = "| Metric |" in answer and "|---" in answer
    if result_type not in structured_types or not has_markdown_table:
        return False

    primary_metric = (
        ((result.get("semantic_resolution") or {}).get("primary_metric_name"))
        or result.get("metric_name")
        or ""
    )
    answer_lower = answer.lower()
    issues: List[str] = []
    if str(primary_metric).strip().lower() == "profit" and "cm2 profit analysis" in answer_lower:
        issues.append("CM1 profit request was labelled as CM2 profit analysis")

    result["answer_validation"] = {
        "status": "passed" if not issues else "flagged",
        "reason": "deterministic_table_response",
        "is_valid": not issues,
        "confidence": 1.0 if not issues else 0.0,
        "issues": issues,
        "corrected": False,
    }
    logger.info(
        "[ANSWER_VALIDATION] status=%s corrected=False reason=deterministic_table_response issues=%s",
        result["answer_validation"]["status"],
        issues[:2],
    )
    return not issues


def verify_and_correct_answer(result: Dict[str, Any], user_query: str) -> Dict[str, Any]:
    answer = (result.get("final_response") or "").strip()
    if not answer:
        result["answer_validation"] = {
            "status": "skipped",
            "reason": "empty_answer",
            "corrected": False,
        }
        return result

    if result.get("error"):
        result["answer_validation"] = {
            "status": "skipped",
            "reason": "agent_error",
            "corrected": False,
        }
        return result

    if result.get("intent") in {"clarify", "chat", "explain"}:
        result["answer_validation"] = {
            "status": "skipped",
            "reason": "non_business_data_answer",
            "corrected": False,
        }
        return result

    if _validate_deterministic_comparison(result):
        return result

    if _validate_deterministic_table_answer(result):
        return result

    if not _verifier_llm:
        result["answer_validation"] = {
            "status": "skipped",
            "reason": "verifier_llm_unavailable",
            "corrected": False,
        }
        return result

    prompt = """
You are Phormula's answer verification agent for Amazon seller finance, ads, and inventory analytics.

Your job is to check whether the chatbot's final answer is factually supported by the computed source data in the context.

Check for:
- Wrong metric, product/SKU, marketplace/country, or time period.
- Wrong interpretation of the user's business question compared with semantic_resolution.
- Incorrect arithmetic, totals, deltas, percentages, rankings, or comparisons.
- Numbers or claims that are not present in or supported by the computed data.
- Business advice that contradicts the available metrics.
- Missing data presented as if it exists.

Rules:
- If the answer is correct enough, set is_valid=true and leave corrected_answer empty.
- If the answer is wrong, set is_valid=false and provide a corrected_answer.
- Correct only factual or business-logic problems. Do not rewrite style just to make it prettier.
- Do not invent numbers, products, countries, time periods, or causes that are not supported by the context.
- If business_context.comparison.metric_drivers or unfavorable_metric_drivers contains rows, the comparison has driver evidence. Do not say the data lacks month-over-month breakdowns; use those drivers if correction is needed.
- If semantic_resolution.is_broad_business_analysis=true, the answer must not focus on only one narrow metric unless the data shows that metric is the dominant issue.
- Accounting definitions: always call profit "CM1 profit" in user-facing text. CM1 profit does not subtract ads or platform fees; CM2 profit subtracts ads and platform fees. Do not rename CM2 profit as CM1 profit. total_quantity is net sold units after refund quantity. promotional_rebates are sign-aware: negative means discount/rebate paid out, positive means amount received back.
- If business_context.data_quality.total_only_metrics contains a metric, monthly totals are usable but SKU-level attribution for that metric is unavailable.
- For inventory forecasts, `stored_month`/`stored_year` identify when the forecast file was stored/generated, not every target month inside the workbook. If `requested_forecast_available=true` or `requested_forecast_column` is present, do not say the requested forecast month is unavailable only because `exact_period_match=false`.
- Preserve useful headings, bullets, and numbered actions when correcting. Do not collapse a structured business diagnosis into one paragraph.
- If the data is insufficient, the corrected answer must clearly say what is unavailable and avoid unsupported numbers.
- Keep corrected_answer concise and user-facing.

Context JSON:
""" + _compact_json(_answer_verification_context(result, user_query), max_chars=12000)

    try:
        structured_llm = _verifier_llm.with_structured_output(AnswerVerificationModel)
        verdict = structured_llm.invoke(prompt)
        issues = _clean_verification_issues(verdict.issues)
        corrected_answer = (verdict.corrected_answer or "").strip()
        corrected = False

        if not verdict.is_valid and corrected_answer and corrected_answer != answer:
            result["final_response"] = corrected_answer
            corrected = True

        result["answer_validation"] = {
            "status": "corrected" if corrected else ("passed" if verdict.is_valid else "flagged"),
            "is_valid": bool(verdict.is_valid),
            "confidence": float(verdict.confidence),
            "issues": issues,
            "corrected": corrected,
        }
        logger.info(
            "[ANSWER_VALIDATION] status=%s corrected=%s issues=%s",
            result["answer_validation"]["status"],
            corrected,
            issues[:2],
        )
    except Exception as exc:
        logger.exception("Answer verification failed")

        if _is_insufficient_quota_error(exc):
            result["final_response"] = INSUFFICIENT_BALANCE_MESSAGE
            result["error"] = "insufficient_quota"
            result["insufficient_balance"] = True

            result["answer_validation"] = {
                "status": "failed",
                "reason": "insufficient_quota",
                "corrected": False,
            }

            return result

        result["answer_validation"] = {
            "status": "failed",
            "reason": "verifier_error",
            "corrected": False,
        }

    return result


def _suggestion_context(result: Dict[str, Any], user_query: str) -> Dict[str, Any]:
    analysis = result.get("analysis_result") or {}
    current = result.get("current_metrics") or {}

    return {
        "user_query": user_query,
        "answer": result.get("final_response"),
        "intent": result.get("intent"),
        "analysis_type": result.get("analysis_type"),
        "result_type": analysis.get("type"),
        "country": result.get("country"),
        "target_countries": result.get("target_countries"),
        "metric_name": result.get("metric_name"),
        "metric_names": result.get("metric_names"),
        "semantic_resolution": result.get("semantic_resolution"),
        "product_query": result.get("product_query"),
        "product_queries": result.get("product_queries"),
        "period_parsed": result.get("period_parsed"),
        "current_metrics": current,
        "comparison": result.get("comparison"),
        "tool_trace": result.get("tool_trace"),
    }


def _llm_suggested_questions(
    result: Dict[str, Any],
    user_query: str
) -> List[str]:
    if (
        result.get("insufficient_balance")
        or result.get("error") == "insufficient_quota"
    ):
        return []

    if not _suggestion_llm:
        return []

    intent = result.get("intent")
    if intent in {"clarify", "chat", "explain"} or result.get("error"):
        return []

    prompt = """
You generate the next suggested questions for Phormula, a finance SaaS chatbot for Amazon sellers, accountants, and ecommerce managers.

Return exactly 3 useful follow-up questions.

Rules:
- Return questions only, no explanations.
- Questions must be answerable from the seller's own Amazon/SP-API/PostgreSQL data or from business advice grounded in that data.
- Use the actual context: metric, product, period, country, and result type.
- Prefer a mix of: drill-down, comparison/trend, and business action.
- Do not repeat the user's exact question.
- Do not invent exact numbers, dates, products, or countries not present in the context.
- Keep each question short enough for a clickable chip.

Context JSON:
""" + _compact_json(_suggestion_context(result, user_query))

    try:
        structured_llm = _suggestion_llm.with_structured_output(SuggestedQuestionModel)
        response = structured_llm.invoke(prompt)
        return _clean_suggested_questions(response.questions, user_query)
    except Exception:
        logger.exception("LLM suggested-question generation failed")
        return []


def build_suggested_questions(result: Dict[str, Any], user_query: str) -> List[str]:
    llm_questions = _llm_suggested_questions(result, user_query)
    if llm_questions:
        return [_enforce_cm1_profit_terms(question) for question in llm_questions]
    return [_enforce_cm1_profit_terms(question) for question in _fallback_suggested_questions(result, user_query)]


def run_agent(
    *,
    user_id: int,
    country: str,
    user_query: str,
    email_requested: bool = False,
    thresholds: Optional[Dict[str, float]] = None,
    conversation_id: Optional[str] = None,
    include_suggested_questions: bool = True,
) -> Dict[str, Any]:
    conversation_id = conversation_id or str(uuid4())
    history = recent_chat_history(user_id, limit=6)
    state = {
        "user_id": int(user_id),
        "country": (country or "uk").strip().lower(),
        "conversation_id": conversation_id,
        "user_query": (user_query or "").strip(),
        "email_requested": bool(email_requested),
        "thresholds": {**DEFAULT_THRESHOLDS, **(thresholds or {})},
        "chat_history": history,
    }

    flask_app = _current_flask_app()
    result = _run_with_timeout(
        label="AGENT_GRAPH",
        executor=_agent_executor,
        timeout_seconds=_agent_timeout_seconds,
        func=lambda: _invoke_graph_with_optional_app_context(state, flask_app),
        default=None,
    )
    if not isinstance(result, dict):
        result = {
            "conversation_id": conversation_id,
            "intent": "error",
            "country": state["country"],
            "user_query": state["user_query"],
            "final_response": (
                "This analysis took too long to finish. Please try again, or ask for a narrower "
                "product, metric, country, or time period."
            ),
            "error": "agent_timeout",
            "answer_validation": {
                "status": "skipped",
                "reason": "agent_timeout",
                "corrected": False,
            },
        }
    else:
        result = _strip_runtime_state(result)

    validation_default = _safe_result_copy(result)
    validation_default["answer_validation"] = {
        "status": "skipped",
        "reason": "verifier_timeout",
        "corrected": False,
    }
    result = _run_with_timeout(
        label="ANSWER_VALIDATION",
        executor=_postprocess_executor,
        timeout_seconds=_verification_timeout_seconds,
        func=lambda: verify_and_correct_answer(_safe_result_copy(result), user_query),
        default=validation_default,
    )
    result = _strip_runtime_state(result)
    result["final_response"] = _enforce_cm1_profit_terms(result.get("final_response"))

    if (
        result.get("insufficient_balance")
        or result.get("error") == "insufficient_quota"
    ):
        suggested_questions = []

    elif include_suggested_questions:
        suggestion_default = [
            _enforce_cm1_profit_terms(question)
            for question in _fallback_suggested_questions(result, user_query)
        ]

        suggested_questions = _run_with_timeout(
            label="SUGGESTED_QUESTIONS",
            executor=_postprocess_executor,
            timeout_seconds=_suggestion_timeout_seconds,
            func=lambda: build_suggested_questions(
                _safe_result_copy(result),
                user_query,
            ),
            default=suggestion_default,
        )

        if not isinstance(suggested_questions, list):
            suggested_questions = suggestion_default

    else:
        suggested_questions = []

    history_id = None
    try:
        history_id = save_chat_turn(
            user_id=user_id,
            message=user_query,
            response=result.get("final_response", ""),
            meta={
                "intent": result.get("intent"),
                "country": result.get("country"),
                "target_countries": result.get("target_countries"),
                "metric_name": result.get("metric_name"),
                "metric_names": result.get("metric_names"),
                "semantic_resolution": result.get("semantic_resolution"),
                "analysis_type": result.get("analysis_type"),
                "period_parsed": result.get("period_parsed"),
                "analysis_result": result.get("analysis_result"),
                "business_context": result.get("business_context"),
                "current_metrics": result.get("current_metrics"),
                "comparison": result.get("comparison"),
                "advice": result.get("advice", []),
                "event_plan_result": result.get("event_plan_result"),
                "sku_intelligence_result": result.get("sku_intelligence_result"),
                "tool_trace": result.get("tool_trace", []),
                "answer_validation": result.get("answer_validation"),
                "suggested_questions": suggested_questions,
            },
        )
    except Exception:
        logger.exception("Failed to persist agent chat turn")
    return {
        "conversation_id": conversation_id,
        "response": result.get("final_response"),
        "intent": result.get("intent"),
        "country": result.get("country"),
        "target_countries": result.get("target_countries"),
        "metric_name": result.get("metric_name"),
        "metric_names": result.get("metric_names"),
        "semantic_resolution": result.get("semantic_resolution"),
        "current_metrics": result.get("current_metrics"),
        "comparison": result.get("comparison"),
        "analysis_result": result.get("analysis_result"),
        "advice": result.get("advice", []),
        "email_result": result.get("email_result"),
        "event_plan_result": result.get("event_plan_result"),
        "sku_intelligence_result": result.get("sku_intelligence_result"),
        "answer_validation": result.get("answer_validation"),
        "suggested_questions": suggested_questions,
        "history_id": history_id,
        "memory": history,
        "error": result.get("error"),
        "insufficient_balance": bool(result.get("insufficient_balance")),
    }
    
