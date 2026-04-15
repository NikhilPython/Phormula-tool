from __future__ import annotations

import calendar
import json
import os
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field
from langchain_openai import ChatOpenAI

from app.ai_agent.prompts import ADVICE_PROMPT, REQUEST_PLANNER_PROMPT
from app.ai_agent.db import get_engine, latest_available_month, _format_currency
from app.ai_agent.email_service import build_email_html, send_agent_email
from app.ai_agent.formula_engine import (
    OVERALL_MONTH_METRICS,
    build_time_series_analysis,
    compare_periods,
    get_growth_driver_insights,
    get_last_n_month_keys,
    get_metric_for_month,
    get_metric_for_period,
    get_product_metric_pack_for_month,
    parse_period,
)
from app.ai_agent.memory import load_last_analysis_from_history
from app.ai_agent.state import AgentState

try:
    from app.utils.agent_utils import build_plan_langgraph, phormula_engine, amazon_engine
    EVENT_PLANNER_AVAILABLE = True
except Exception:
    EVENT_PLANNER_AVAILABLE = False
    build_plan_langgraph = None
    phormula_engine = None
    amazon_engine = None


OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
LLM_ENABLED = bool(OPENAI_API_KEY)

planner_llm = ChatOpenAI(
    model="gpt-4.1",
    api_key=OPENAI_API_KEY,
    temperature=0,
) if LLM_ENABLED else None

chat_llm = ChatOpenAI(
    model="gpt-4.1",
    api_key=OPENAI_API_KEY,
    temperature=0.5,
) if LLM_ENABLED else None

explain_llm = ChatOpenAI(
    model="gpt-4.1",
    api_key=OPENAI_API_KEY,
    temperature=0.2,
) if LLM_ENABLED else None

advisor_llm = ChatOpenAI(
    model="gpt-4.1",
    api_key=OPENAI_API_KEY,
    temperature=0.2,
) if LLM_ENABLED else None


# -------------------------------------------------------------------
# KEEP: metric mapping
# -------------------------------------------------------------------
ALIAS_MAP = {
    "gross sales": "gross_sales",
    "gross revenue": "gross_sales",
    "net sales": "net_sales",
    "sales": "net_sales",
    "revenue": "net_sales",

    "profit": "profit",
    "cm1 profit": "profit",
    "cm1": "profit",

    "cm2 profit": "cm2_profit",
    "cm2": "cm2_profit",

    "sales mix": "sales_mix",
    "sales contribution": "sales_mix",
    "revenue mix": "sales_mix",

    "profit mix": "profit_mix",
    "profit contribution": "profit_mix",

    "ad spend": "advertising_total",
    "ads": "advertising_total",
    "advertising": "advertising_total",

    "platform fee": "platform_fee",
    "amazon fees": "amazon_fee",
    "fees": "amazon_fee",

    "fba fees": "fba_fees",
    "selling fees": "selling_fees",

    "refunds": "refund_sales",
    "refund": "refund_sales",

    "units": "total_quantity",
    "orders": "total_quantity",
    "quantity": "total_quantity",

    "margin": "profit_percentage",
    "profit %": "profit_percentage",

    "acos": "acos",
    "asp": "asp",
}


# -------------------------------------------------------------------
# MODELS
# -------------------------------------------------------------------
class RequestPlanModel(BaseModel):
    intent: str = "chat"
    analysis_type: str = "absolute"
    metric_name: Optional[str] = None
    product_query: Optional[str] = None
    needs_advice: bool = False
    response_mode: str = "short"
    clarification_question: Optional[str] = None

    event_name: Optional[str] = None
    last_event_month: Optional[int] = Field(default=None, ge=1, le=12)
    future_event_month: Optional[int] = Field(default=None, ge=1, le=12)
    target_sales: Optional[float] = None


@dataclass
class RequestPlan:
    intent: str
    analysis_type: str
    metric_name: Optional[str]
    product_query: Optional[str]
    needs_advice: bool
    response_mode: str
    clarification_question: Optional[str] = None

    event_name: Optional[str] = None
    last_event_month: Optional[int] = None
    future_event_month: Optional[int] = None
    target_sales: Optional[float] = None


# -------------------------------------------------------------------
# HELPERS
# -------------------------------------------------------------------
def _normalize(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").strip().lower())



def _safe_div(numerator: float, denominator: float) -> float:
    return 0.0 if denominator == 0 else float(numerator) / float(denominator)


def _metric_from_query(query: str) -> Optional[str]:
    q = _normalize(query)
    for phrase in sorted(ALIAS_MAP.keys(), key=len, reverse=True):
        if phrase in q:
            return ALIAS_MAP[phrase]
    return None


# KEEP: product matching
def _match_product(rows: List[Dict[str, Any]], product_query: Optional[str]) -> Optional[str]:
    if not rows or not product_query:
        return None
    pq = product_query.lower().strip()

    exact = [r for r in rows if pq == str(r.get("product_name", "")).lower()]
    if exact:
        return pq

    contains = [r for r in rows if pq in str(r.get("product_name", "")).lower()]
    if contains:
        return str(contains[0].get("product_name", "")).lower().strip() or pq

    return None


def _latest_month_result(engine: Any, user_id: int, country: str, metric_name: str) -> Dict[str, Any]:
    latest = latest_available_month(engine, user_id, country)
    return get_metric_for_month(engine, user_id, country, metric_name, latest.month, latest.year)


def _prepare_period_payload(parsed: Dict[str, Any], analysis_type: str) -> Dict[str, Any]:
    ptype = parsed.get("type", "latest_month")

    if analysis_type == "growth" and ptype in {"latest_month", "single", "range", "year"}:
        return {"type": "growth_base", "base": parsed}

    if ptype == "comparison":
        return {"type": "comparison", "p1": parsed["left"], "p2": parsed["right"]}

    if ptype == "range":
        return parsed

    if ptype == "last_n":
        return {"type": "last_n_months", "n": parsed["n"]}

    if ptype == "single":
        return parsed

    if ptype == "year":
        return {
            "type": "range",
            "start_month": 1,
            "start_year": parsed["year"],
            "end_month": 12,
            "end_year": parsed["year"],
        }

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

        return {
            "type": "range",
            "start_month": prev_sm,
            "start_year": prev_sy,
            "end_month": prev_em,
            "end_year": prev_ey,
        }

    return {"type": "latest_month"}


def _compute_growth_from_base(
    engine: Any,
    user_id: int,
    country: str,
    metric_name: str,
    base: Dict[str, Any],
) -> Dict[str, Any]:
    prev = _previous_period_for_base(base)

    if base.get("type") == "single":
        current = get_metric_for_month(engine, user_id, country, metric_name, base["month"], base["year"])
    elif base.get("type") == "year":
        current = get_metric_for_period(
            engine, user_id, country, metric_name, 1, base["year"], 12, base["year"], skip_missing=True
        )
    elif base.get("type") == "range":
        current = get_metric_for_period(
            engine,
            user_id,
            country,
            metric_name,
            base["start_month"],
            base["start_year"],
            base["end_month"],
            base["end_year"],
            skip_missing=True,
        )
    else:
        current = _latest_month_result(engine, user_id, country, metric_name)
        latest = latest_available_month(engine, user_id, country)
        base = {"type": "single", "month": latest.month, "year": latest.year}
        prev = _previous_period_for_base(base)

    if prev.get("type") == "single":
        previous = get_metric_for_month(engine, user_id, country, metric_name, prev["month"], prev["year"])
    elif prev.get("type") == "year":
        previous = get_metric_for_period(
            engine, user_id, country, metric_name, 1, prev["year"], 12, prev["year"], skip_missing=True
        )
    else:
        previous = get_metric_for_period(
            engine,
            user_id,
            country,
            metric_name,
            prev["start_month"],
            prev["start_year"],
            prev["end_month"],
            prev["end_year"],
            skip_missing=True,
        )

    curr = float(current.get("total", 0.0))
    prv = float(previous.get("total", 0.0))
    delta = curr - prv
    pct_change = None if prv == 0 else (delta / prv) * 100.0

    return {
        "metric": metric_name,
        "left": {"label": current.get("period_label"), "total": curr},
        "right": {"label": previous.get("period_label"), "total": prv},
        "delta": delta,
        "pct_change": pct_change,
    }


def _overall_metric_for_payload(
    engine: Any,
    user_id: int,
    country: str,
    metric_name: str,
    payload: Dict[str, Any],
) -> float:
    if payload["type"] == "single":
        result = get_metric_for_month(engine, user_id, country, metric_name, payload["month"], payload["year"])
        return float(result.get("total", 0.0))

    if payload["type"] == "last_n_months":
        months = get_last_n_month_keys(engine, user_id, country, payload["n"])
        total = 0.0
        for mk in months:
            result = get_metric_for_month(engine, user_id, country, metric_name, mk.month, mk.year)
            total += float(result.get("total", 0.0))
        return total

    if payload["type"] == "range":
        result = get_metric_for_period(
            engine,
            user_id,
            country,
            metric_name,
            payload["start_month"],
            payload["start_year"],
            payload["end_month"],
            payload["end_year"],
            skip_missing=True,
        )
        return float(result.get("total", 0.0))

    latest = latest_available_month(engine, user_id, country)
    result = get_metric_for_month(engine, user_id, country, metric_name, latest.month, latest.year)
    return float(result.get("total", 0.0))


def _enrich_pack_with_mix(
    engine: Any,
    user_id: int,
    country: str,
    payload: Dict[str, Any],
    pack: Dict[str, float],
) -> Dict[str, float]:
    enriched = dict(pack)
    total_sales = _overall_metric_for_payload(engine, user_id, country, "net_sales", payload)
    total_profit = _overall_metric_for_payload(engine, user_id, country, "profit", payload)

    enriched["sales_mix"] = _safe_div(float(enriched.get("net_sales", 0.0)), total_sales)
    enriched["profit_mix"] = _safe_div(float(enriched.get("profit", 0.0)), total_profit)
    return enriched


def _compute_root_cause(
    current_pack: Dict[str, float],
    previous_pack: Dict[str, float],
) -> Dict[str, Any]:
    if not previous_pack:
        return {
            "primary_driver": None,
            "drivers": [],
            "summary": [],
        }

    sales_delta = float(current_pack.get("net_sales", 0.0)) - float(previous_pack.get("net_sales", 0.0))
    profit_delta = float(current_pack.get("profit", 0.0)) - float(previous_pack.get("profit", 0.0))
    units_delta = float(current_pack.get("total_quantity", 0.0)) - float(previous_pack.get("total_quantity", 0.0))
    asp_delta = float(current_pack.get("asp", 0.0)) - float(previous_pack.get("asp", 0.0))
    sales_mix_delta = float(current_pack.get("sales_mix", 0.0)) - float(previous_pack.get("sales_mix", 0.0))
    profit_mix_delta = float(current_pack.get("profit_mix", 0.0)) - float(previous_pack.get("profit_mix", 0.0))

    # Approximate effect decomposition
    prev_units = float(previous_pack.get("total_quantity", 0.0))
    prev_asp = float(previous_pack.get("asp", 0.0))
    units_effect_on_sales = units_delta * prev_asp
    asp_effect_on_sales = sales_delta - units_effect_on_sales

    prev_margin_per_unit = _safe_div(
        float(previous_pack.get("profit", 0.0)),
        float(previous_pack.get("total_quantity", 0.0)),
    )
    units_effect_on_profit = units_delta * prev_margin_per_unit
    profit_quality_effect = profit_delta - units_effect_on_profit

    drivers = [
        {
            "driver": "sales",
            "delta": sales_delta,
            "direction": "up" if sales_delta >= 0 else "down",
        },
        {
            "driver": "profit",
            "delta": profit_delta,
            "direction": "up" if profit_delta >= 0 else "down",
        },
        {
            "driver": "units",
            "delta": units_delta,
            "direction": "up" if units_delta >= 0 else "down",
            "estimated_sales_impact": units_effect_on_sales,
            "estimated_profit_impact": units_effect_on_profit,
        },
        {
            "driver": "asp",
            "delta": asp_delta,
            "direction": "up" if asp_delta >= 0 else "down",
            "estimated_sales_impact": asp_effect_on_sales,
        },
        {
            "driver": "sales_mix",
            "delta": sales_mix_delta,
            "direction": "up" if sales_mix_delta >= 0 else "down",
        },
        {
            "driver": "profit_mix",
            "delta": profit_mix_delta,
            "direction": "up" if profit_mix_delta >= 0 else "down",
            "estimated_profit_impact": profit_quality_effect,
        },
    ]

    ranked = sorted(
        drivers,
        key=lambda d: max(
            abs(float(d.get("estimated_profit_impact", 0.0))),
            abs(float(d.get("estimated_sales_impact", 0.0))),
            abs(float(d.get("delta", 0.0))),
        ),
        reverse=True,
    )

    summary: List[str] = []
    if abs(profit_delta) > 0:
        summary.append(f"Profit changed by {profit_delta:,.2f}.")
    if abs(sales_delta) > 0:
        summary.append(f"Sales changed by {sales_delta:,.2f}.")
    if abs(units_delta) > 0:
        summary.append(f"Units changed by {units_delta:,.2f}, contributing about {units_effect_on_sales:,.2f} to sales movement.")
    if abs(asp_delta) > 0:
        summary.append(f"ASP changed by {asp_delta:,.2f}, contributing about {asp_effect_on_sales:,.2f} to sales movement.")
    if abs(sales_mix_delta) > 0:
        summary.append(f"Sales mix changed by {sales_mix_delta:.2%}.")
    if abs(profit_mix_delta) > 0:
        summary.append(f"Profit mix changed by {profit_mix_delta:.2%}.")

    primary_driver = ranked[0]["driver"] if ranked else None

    return {
        "primary_driver": primary_driver,
        "drivers": ranked,
        "summary": summary,
    }


# -------------------------------------------------------------------
# LLM-FIRST PLANNER
# -------------------------------------------------------------------
def _plan_request(query: str, email_requested: bool = False) -> RequestPlan:
    mapped_metric = _metric_from_query(query)

    if planner_llm:
        try:
            planner = planner_llm.with_structured_output(RequestPlanModel)
            result = planner.invoke(
                REQUEST_PLANNER_PROMPT
                + "\n\nUser query:\n"
                + query
            )

            # Prioritize exact user text mapping over model guess
            metric_name = mapped_metric if mapped_metric else result.metric_name

            return RequestPlan(
                intent=result.intent,
                analysis_type=result.analysis_type,
                metric_name=metric_name,
                product_query=result.product_query,
                needs_advice=bool(result.needs_advice),
                response_mode=result.response_mode or "short",
                clarification_question=result.clarification_question,
                event_name=result.event_name,
                last_event_month=result.last_event_month,
                future_event_month=result.future_event_month,
                target_sales=result.target_sales,
            )
        except Exception:
            pass

    if not mapped_metric:
        return RequestPlan(
            intent="clarify",
            analysis_type="absolute",
            metric_name=None,
            product_query=None,
            needs_advice=False,
            response_mode="short",
            clarification_question="Which metric would you like me to analyze?",
        )

    return RequestPlan(
        intent="email" if email_requested else "metric_qa",
        analysis_type="absolute",
        metric_name=mapped_metric,
        product_query=None,
        needs_advice=False,
        response_mode="short",
    )


# -------------------------------------------------------------------
# SKU INTELLIGENCE
# -------------------------------------------------------------------
def _compute_sku_intelligence(state: AgentState) -> AgentState:
    engine = state["engine"]
    user_id = state["user_id"]
    country = state["country"]
    product_query = state.get("product_query")
    metric_name = state.get("metric_name") or "profit"
    payload = state["period_payload"]

    latest = latest_available_month(engine, user_id, country)
    sample = get_metric_for_month(engine, user_id, country, "net_sales", latest.month, latest.year)
    product_match = _match_product(sample.get("per_sku", []), product_query)
    if not product_match:
        raise ValueError("I could not confidently match that product name in your data.")
    state["product_match"] = product_match

    def pack_from_payload(pl: Dict[str, Any]) -> Dict[str, float]:
        if pl["type"] == "single":
            base_pack = get_product_metric_pack_for_month(
                engine, user_id, country, product_match, pl["month"], pl["year"]
            )["metrics"]
            return _enrich_pack_with_mix(engine, user_id, country, pl, base_pack)

        if pl["type"] == "last_n_months":
            months = get_last_n_month_keys(engine, user_id, country, pl["n"])
            acc = {
                "total_quantity": 0.0,
                "net_sales": 0.0,
                "profit": 0.0,
            }
            for mk in months:
                p = get_product_metric_pack_for_month(
                    engine, user_id, country, product_match, mk.month, mk.year
                )["metrics"]
                for k in ["total_quantity", "net_sales", "profit"]:
                    acc[k] += float(p.get(k, 0.0))
            acc["asp"] = _safe_div(acc["net_sales"], acc["total_quantity"])
            return _enrich_pack_with_mix(engine, user_id, country, pl, acc)

        if pl["type"] == "range":
            months = get_last_n_month_keys(engine, user_id, country, 36)
            filtered = [
                mk for mk in months
                if (mk.year, mk.month) >= (pl["start_year"], pl["start_month"])
                and (mk.year, mk.month) <= (pl["end_year"], pl["end_month"])
            ]
            acc = {
                "total_quantity": 0.0,
                "net_sales": 0.0,
                "profit": 0.0,
            }
            for mk in filtered:
                try:
                    p = get_product_metric_pack_for_month(
                        engine, user_id, country, product_match, mk.month, mk.year
                    )["metrics"]
                except Exception:
                    continue
                for k in ["total_quantity", "net_sales", "profit"]:
                    acc[k] += float(p.get(k, 0.0))
            acc["asp"] = _safe_div(acc["net_sales"], acc["total_quantity"])
            return _enrich_pack_with_mix(engine, user_id, country, pl, acc)

        base_pack = get_product_metric_pack_for_month(
            engine, user_id, country, product_match, latest.month, latest.year
        )["metrics"]
        effective = {"type": "single", "month": latest.month, "year": latest.year}
        return _enrich_pack_with_mix(engine, user_id, country, effective, base_pack)

    effective_payload = payload if payload["type"] != "growth_base" else _prepare_period_payload(payload["base"], "absolute")
    current_pack = pack_from_payload(effective_payload)

    previous_payload = None
    base_for_prev = payload["base"] if payload["type"] == "growth_base" else effective_payload

    if base_for_prev["type"] in {"single", "range"}:
        previous_payload = _previous_period_for_base(base_for_prev)
    elif base_for_prev["type"] == "last_n_months":
        n = base_for_prev["n"]

        months = get_last_n_month_keys(engine, user_id, country, n * 2)

        if len(months) >= n * 2:
            previous_months = months[:n]

            previous_payload = {
                "type": "range",
                "start_month": previous_months[0].month,
                "start_year": previous_months[0].year,
                "end_month": previous_months[-1].month,
                "end_year": previous_months[-1].year,
            }

    previous_pack: Dict[str, float] = {}
    if previous_payload:
        try:
            previous_pack = pack_from_payload(previous_payload)
        except Exception:
            previous_pack = {}

    deltas = {}
    for k in ["net_sales", "profit", "total_quantity", "asp", "sales_mix", "profit_mix"]:
        deltas[k] = float(current_pack.get(k, 0.0)) - float(previous_pack.get(k, 0.0) if previous_pack else 0.0)

    root_cause = _compute_root_cause(current_pack, previous_pack)

    summary_points = [
        f"Current sales are {float(current_pack.get('net_sales', 0.0)):,.2f}.",
        f"Current profit is {float(current_pack.get('profit', 0.0)):,.2f}.",
        f"Current units are {float(current_pack.get('total_quantity', 0.0)):,.2f} and ASP is {float(current_pack.get('asp', 0.0)):,.2f}.",
        f"Current sales mix is {float(current_pack.get('sales_mix', 0.0)):.2%} and profit mix is {float(current_pack.get('profit_mix', 0.0)):.2%}.",
    ]
    if previous_pack:
        summary_points.append(
            f"Sales changed by {deltas['net_sales']:,.2f}, profit changed by {deltas['profit']:,.2f}, and units changed by {deltas['total_quantity']:,.2f} vs the previous comparable period."
        )
        summary_points.append(
            f"ASP changed by {deltas['asp']:,.2f}, sales mix changed by {deltas['sales_mix']:.2%}, and profit mix changed by {deltas['profit_mix']:.2%}."
        )
        summary_points.extend(root_cause.get("summary", [])[:3])

    trend_months = get_last_n_month_keys(engine, user_id, country, 6)
    trend_metric = metric_name if metric_name not in {"sales_mix", "profit_mix"} else ("net_sales" if metric_name == "sales_mix" else "profit")
    trend = build_time_series_analysis(
        engine,
        user_id,
        country,
        trend_metric,
        trend_months,
        product_match=product_match,
    )

    result = {
        "product_match": product_match,
        "current": current_pack,
        "previous": previous_pack,
        "deltas": deltas,
        "summary_points": summary_points,
        "trend": trend,
        "root_cause": root_cause,
    }

    state["sku_intelligence_result"] = result
    state["current_metrics"] = {
        "metric": metric_name,
        "period_label": (
            f"{payload.get('type', 'selected period')}"
            if payload.get("type") != "range"
            else f"{payload.get('start_month')}/{payload.get('start_year')} to {payload.get('end_month')}/{payload.get('end_year')}"
        ),
        "total": float(current_pack.get(metric_name, current_pack.get("profit", 0.0))),
    }
    state["analysis_result"] = {"type": "sku_intelligence", **result}

    if previous_pack:
        current_value = float(current_pack.get(metric_name, 0.0))
        previous_value = float(previous_pack.get(metric_name, 0.0))
        delta = current_value - previous_value
        pct = None if previous_value == 0 else (delta / previous_value) * 100.0
        state["comparison"] = {
            "left": {"label": "current", "total": current_value},
            "right": {"label": "previous", "total": previous_value},
            "delta": delta,
            "pct_change": pct,
        }

    return state


# -------------------------------------------------------------------
# EVENT PLANNER
# -------------------------------------------------------------------
def _run_event_planner(state: AgentState) -> AgentState:
    if not EVENT_PLANNER_AVAILABLE:
        raise ValueError("Event planner helper is not available in this deployment.")

    now = datetime.utcnow()

    last_event_month = state.get("last_event_month") or 11
    future_event_month = state.get("future_event_month") or last_event_month
    target_sales = state.get("target_sales")

    payload = {
        "user_id": state["user_id"],
        "country": state["country"],
        "last_event": {"month": int(last_event_month), "year": now.year - 1},
        "future_event": {"month": int(future_event_month), "year": now.year},
        "target_sales": target_sales,
    }

    result = build_plan_langgraph(payload, phormula_engine, amazon_engine)

    top_items = result[:3] if isinstance(result, list) else []
    summary: List[str] = []
    actions: List[str] = []

    for item in top_items:
        plan = item.get("plan", {}) if isinstance(item, dict) else {}
        if isinstance(plan, dict):
            summary.extend(plan.get("summary", [])[:2])
            actions.extend(plan.get("actions", [])[:2])

    state["event_plan_result"] = {
        "items": result if isinstance(result, list) else [result],
        "summary": summary[:10],
        "actions": actions[:10],
    }
    state["analysis_result"] = {"type": "event_plan"}
    state["current_metrics"] = {
        "metric": "event_plan",
        "period_label": f"{payload['future_event']['month']}-{payload['future_event']['year']}",
    }
    return state


# -------------------------------------------------------------------
# CORE EXECUTION
# -------------------------------------------------------------------
def _compute_mix_time_series(
    engine: Any,
    user_id: int,
    country: str,
    metric_name: str,
    months: List[Any],
    product_match: Optional[str],
) -> Dict[str, Any]:
    base_metric = "net_sales" if metric_name == "sales_mix" else "profit"
    series: List[Dict[str, Any]] = []

    for mk in months:
        month_result = get_metric_for_month(engine, user_id, country, base_metric, mk.month, mk.year)
        overall_total = float(month_result.get("total", 0.0))

        value = 0.0
        if product_match:
            rows = month_result.get("per_sku", [])
            matched = None
            for row in rows:
                name = str(row.get("product_name", "")).lower()
                if name == product_match or product_match in name:
                    matched = row
                    break
            product_value = float((matched or {}).get("__metric__", 0.0))
            value = _safe_div(product_value, overall_total)
        else:
            value = 1.0 if overall_total else 0.0

        series.append({
            "month": mk.month,
            "year": mk.year,
            "__metric__": value,
        })

    movement = "flat"
    if len(series) >= 2:
        first = float(series[0]["__metric__"])
        last = float(series[-1]["__metric__"])
        if last > first:
            movement = "upward"
        elif last < first:
            movement = "downward"
        else:
            movement = "flat"

    return {"series": series, "movement": movement}


def _compute_mix_breakdown(
    engine: Any,
    user_id: int,
    country: str,
    metric_name: str,
    payload: Dict[str, Any],
) -> Dict[str, Any]:
    base_metric = "net_sales" if metric_name == "sales_mix" else "profit"

    if payload["type"] == "latest_month":
        latest = latest_available_month(engine, user_id, country)
        month_result = get_metric_for_month(engine, user_id, country, base_metric, latest.month, latest.year)
        total = float(month_result.get("total", 0.0))
        per_sku = []
        for row in month_result.get("per_sku", []):
            per_sku.append({
                "sku": row.get("sku"),
                "product_name": row.get("product_name"),
                "__metric__": _safe_div(float(row.get("__metric__", 0.0)), total),
            })
        return {
            "metric": metric_name,
            "period_label": month_result.get("period_label"),
            "total": sum(float(r["__metric__"]) for r in per_sku),
            "per_sku": sorted(per_sku, key=lambda x: x["__metric__"], reverse=True),
        }

    if payload["type"] == "single":
        month_result = get_metric_for_month(engine, user_id, country, base_metric, payload["month"], payload["year"])
        total = float(month_result.get("total", 0.0))
        per_sku = []
        for row in month_result.get("per_sku", []):
            per_sku.append({
                "sku": row.get("sku"),
                "product_name": row.get("product_name"),
                "__metric__": _safe_div(float(row.get("__metric__", 0.0)), total),
            })
        return {
            "metric": metric_name,
            "period_label": month_result.get("period_label"),
            "total": sum(float(r["__metric__"]) for r in per_sku),
            "per_sku": sorted(per_sku, key=lambda x: x["__metric__"], reverse=True),
        }

    if payload["type"] in {"range", "last_n_months"}:
        if payload["type"] == "last_n_months":
            months = get_last_n_month_keys(engine, user_id, country, payload["n"])
        else:
            months = get_last_n_month_keys(engine, user_id, country, 36)
            months = [
                mk for mk in months
                if (mk.year, mk.month) >= (payload["start_year"], payload["start_month"])
                and (mk.year, mk.month) <= (payload["end_year"], payload["end_month"])
            ]

        grouped: Dict[str, Dict[str, Any]] = {}
        overall_total = 0.0
        for mk in months:
            month_result = get_metric_for_month(engine, user_id, country, base_metric, mk.month, mk.year)
            month_total = float(month_result.get("total", 0.0))
            overall_total += month_total
            for row in month_result.get("per_sku", []):
                key = str(row.get("sku") or row.get("product_name") or "unknown")
                grouped.setdefault(
                    key,
                    {"sku": row.get("sku"), "product_name": row.get("product_name"), "_raw_total": 0.0},
                )
                grouped[key]["_raw_total"] += float(row.get("__metric__", 0.0))

        per_sku = []
        for row in grouped.values():
            per_sku.append({
                "sku": row.get("sku"),
                "product_name": row.get("product_name"),
                "__metric__": _safe_div(float(row.get("_raw_total", 0.0)), overall_total),
            })

        label = (
            f"{months[0].label} to {months[-1].label}"
            if months
            else "selected period"
        )
        return {
            "metric": metric_name,
            "period_label": label,
            "total": sum(float(r["__metric__"]) for r in per_sku),
            "per_sku": sorted(per_sku, key=lambda x: x["__metric__"], reverse=True),
        }

    return _compute_mix_breakdown(engine, user_id, country, metric_name, {"type": "latest_month"})


def _compute_result(state: AgentState) -> AgentState:
    engine = state["engine"]
    user_id = state["user_id"]
    country = state["country"]
    metric_name = state.get("metric_name") or "profit"
    payload = state["period_payload"]
    analysis_type = state.get("analysis_type") or "absolute"

    if analysis_type == "sku_intelligence":
        return _compute_sku_intelligence(state)

    if analysis_type == "growth" and payload["type"] == "growth_base":
        comp = _compute_growth_from_base(engine, user_id, country, metric_name, payload["base"])
        state["comparison"] = comp
        state["current_metrics"] = {
            "metric": metric_name,
            "period_label": f"{comp['left']['label']} vs {comp['right']['label']}",
            "total": comp["left"]["total"],
        }
        state["analysis_result"] = {"type": "growth"}
        return state

    if payload["type"] == "comparison":
        p1 = payload["p1"]
        p2 = payload["p2"]
        comp = compare_periods(
            engine=engine,
            user_id=user_id,
            country=country,
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
        state["comparison"] = comp
        state["current_metrics"] = {
            "metric": metric_name,
            "period_label": f"{comp['left']['label']} vs {comp['right']['label']}",
            "total": comp["left"]["total"],
        }
        state["analysis_result"] = {"type": "comparison"}
        return state

    if analysis_type == "breakdown" and state.get("product_query"):
        analysis_type = "trend"

    # ✅ Force product + time queries to trend (except root cause)
    if state.get("product_query") and analysis_type in {"trend", "growth"}:
        analysis_type = "trend"    

    if analysis_type == "trend":
        if payload["type"] == "last_n_months":
            months = get_last_n_month_keys(engine, user_id, country, payload["n"])
        else:
            months = get_last_n_month_keys(engine, user_id, country, 6)

        if metric_name in {"sales_mix", "profit_mix"}:
            sample_metric = "net_sales" if metric_name == "sales_mix" else "profit"
            sample = get_metric_for_month(engine, user_id, country, sample_metric, months[-1].month, months[-1].year)
            product_match = _match_product(sample.get("per_sku", []), state.get("product_query"))
            state["product_match"] = product_match
            series_result = _compute_mix_time_series(
                engine=engine,
                user_id=user_id,
                country=country,
                metric_name=metric_name,
                months=months,
                product_match=product_match,
            )
        else:
            sample = get_metric_for_month(engine, user_id, country, metric_name, months[-1].month, months[-1].year)
            product_match = _match_product(sample.get("per_sku", []), state.get("product_query"))
            state["product_match"] = product_match
            series_result = build_time_series_analysis(
                engine=engine,
                user_id=user_id,
                country=country,
                metric_name=metric_name,
                months=months,
                product_match=product_match,
            )

        state["analysis_result"] = {"type": "trend", **series_result}
        state["current_metrics"] = {
            "metric": metric_name,
            "period_label": f"{months[0].label} to {months[-1].label}",
            "total": sum(float(x.get("__metric__", 0.0)) for x in series_result.get("series", [])),
            "per_period": series_result.get("series", []),
        }
        return state

    if analysis_type == "breakdown":
        if metric_name in {"sales_mix", "profit_mix"}:
            result = _compute_mix_breakdown(engine, user_id, country, metric_name, payload)
        else:
            if payload["type"] == "latest_month":
                result = _latest_month_result(engine, user_id, country, metric_name)
            elif payload["type"] == "single":
                result = get_metric_for_month(engine, user_id, country, metric_name, payload["month"], payload["year"])
            elif payload["type"] == "range":
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
            elif payload["type"] == "last_n_months":
                months = get_last_n_month_keys(engine, user_id, country, payload["n"])
                rows: List[Dict[str, Any]] = []
                for mk in months:
                    rows.extend(
                        get_metric_for_month(engine, user_id, country, metric_name, mk.month, mk.year).get("per_sku", [])
                    )
                grouped: Dict[str, Dict[str, Any]] = {}
                for row in rows:
                    key = str(row.get("sku") or row.get("product_name") or "unknown")
                    grouped.setdefault(
                        key,
                        {"sku": row.get("sku"), "product_name": row.get("product_name"), "__metric__": 0.0},
                    )
                    grouped[key]["__metric__"] += float(row.get("__metric__", 0.0))
                result = {
                    "metric": metric_name,
                    "period_label": f"{months[0].label} to {months[-1].label}",
                    "total": sum(v["__metric__"] for v in grouped.values()),
                    "per_sku": sorted(grouped.values(), key=lambda x: x["__metric__"], reverse=True),
                }
            else:
                result = _latest_month_result(engine, user_id, country, metric_name)

        product_match = _match_product(result.get("per_sku", []), state.get("product_query"))
        if product_match:
            result["per_sku"] = [
                r for r in result.get("per_sku", [])
                if str(r.get("product_name", "")).lower() == product_match
            ]
            result["total"] = sum(float(r.get("__metric__", 0.0)) for r in result["per_sku"])
            state["product_match"] = product_match

        state["current_metrics"] = result
        state["analysis_result"] = {
            "type": "breakdown",
            "metric": metric_name,
            "period_label": result.get("period_label"),
            "per_sku": result.get("per_sku", []),
            "total": result.get("total", 0.0),
        }
        return state

    if analysis_type == "summary":
        latest = latest_available_month(engine, user_id, country)
        metrics: Dict[str, float] = {}
        for m in OVERALL_MONTH_METRICS:
            try:
                metrics[m] = float(
                    get_metric_for_month(engine, user_id, country, m, latest.month, latest.year).get("total", 0.0)
                )
            except Exception:
                continue

        growth_driver = None
        try:
            growth_driver = get_growth_driver_insights(engine, user_id, country, "net_sales", latest.month, latest.year)
        except Exception:
            growth_driver = None

        state["current_metrics"] = {
            "metric": "summary",
            "period_label": latest.label,
            "metrics": metrics,
            "total": metrics.get("profit"),
        }
        state["analysis_result"] = {"type": "summary", "metrics": metrics, "growth_driver": growth_driver}
        return state

    if metric_name in {"sales_mix", "profit_mix"}:
        if state.get("product_query") and payload["type"] in {"last_n_months", "range"}:
            # ✅ product + mix + time → trend
            analysis_type = "trend"
        elif state.get("product_query"):
            # single snapshot → sku intelligence
            state["analysis_type"] = "sku_intelligence"
            return _compute_sku_intelligence(state)

        result = _compute_mix_breakdown(engine, user_id, country, metric_name, payload)
        state["current_metrics"] = result
        state["analysis_result"] = {
            "type": "breakdown",
            "metric": metric_name,
            "period_label": result.get("period_label"),
            "per_sku": result.get("per_sku", []),
            "total": result.get("total", 0.0),
        }
        return state

    if payload["type"] == "latest_month":
        result = _latest_month_result(engine, user_id, country, metric_name)
    elif payload["type"] == "single":
        result = get_metric_for_month(engine, user_id, country, metric_name, payload["month"], payload["year"])
    elif payload["type"] == "range":
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
    elif payload["type"] == "last_n_months":
        months = get_last_n_month_keys(engine, user_id, country, payload["n"])
        result = get_metric_for_period(
            engine=engine,
            user_id=user_id,
            country=country,
            metric_name=metric_name,
            start_month=months[0].month,
            start_year=months[0].year,
            end_month=months[-1].month,
            end_year=months[-1].year,
            skip_missing=True,
        )
    else:
        result = _latest_month_result(engine, user_id, country, metric_name)

    state["current_metrics"] = result
    state["analysis_result"] = {"type": "absolute"}
    return state


# -------------------------------------------------------------------
# ADVICE
# -------------------------------------------------------------------
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
            pass

    advice: List[str] = []
    comp = state.get("comparison") or {}
    root_cause = (state.get("sku_intelligence_result") or {}).get("root_cause") or {}

    if comp and comp.get("pct_change") is not None and comp["pct_change"] < 0:
        advice.append("Review the biggest negative period-over-period drivers and isolate the weak SKUs.")

    if root_cause.get("primary_driver") == "units":
        advice.append("The biggest driver appears to be units, so check demand, stock position, and conversion first.")
    elif root_cause.get("primary_driver") == "asp":
        advice.append("The biggest driver appears to be ASP, so review pricing, discounting, and product mix.")
    elif root_cause.get("primary_driver") in {"sales_mix", "profit_mix"}:
        advice.append("Mix contribution changed materially, so review how this SKU is contributing relative to the rest of the portfolio.")

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


# -------------------------------------------------------------------
# EMAIL
# -------------------------------------------------------------------
def _send_email_if_requested(state: AgentState) -> AgentState:
    if not state.get("email_requested"):
        return state

    subject_metric = state.get("metric_name") or (state.get("current_metrics") or {}).get("metric") or "summary"
    subject_period = (state.get("current_metrics") or {}).get("period_label") or "selected period"

    if state.get("event_plan_result"):
        subject_metric = "event plan"

    if state.get("sku_intelligence_result"):
        subject_metric = f"sku intelligence - {state.get('product_match') or 'product'}"

    html = build_email_html(state)
    state["email_result"] = send_agent_email(
        user_id=state["user_id"],
        subject=f"Phormula AI {subject_metric.replace('_', ' ').title()} - {subject_period}",
        html_body=html,
    )
    return state


# -------------------------------------------------------------------
# RENDER RESPONSE
# -------------------------------------------------------------------
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
                pass
        state["final_response"] = "Hi! Tell me which metric or report you want me to analyze."
        return state

    if state.get("intent") == "explain":
        if explain_llm:
            try:
                state["final_response"] = explain_llm.invoke(state.get("user_query", "")).content
                return state
            except Exception:
                pass
        state["final_response"] = "Ask me about a metric like profit, sales, ACOS, or ASP and I’ll explain it."
        return state

    if state.get("event_plan_result"):
        summary = state["event_plan_result"].get("summary") or []
        actions = state["event_plan_result"].get("actions") or []
        lines = ["Event plan generated."]
        if summary:
            lines.append("Summary:")
            lines.extend(f"- {x}" for x in summary[:8])
        if actions:
            lines.append("Actions:")
            lines.extend(f"- {x}" for x in actions[:8])

        if state.get("email_requested"):
            state["final_response"] = "📩 I've sent the event plan email."
        else:
            state["final_response"] = "\n".join(lines)
        return state

    if state.get("sku_intelligence_result"):
        res = state["sku_intelligence_result"]
        lines = [f"SKU intelligence for {res.get('product_match') or 'selected product'}:"]
        current = res.get("current", {})
        lines.append(f"- Current sales: {_format_currency(float(current.get('net_sales', 0.0)), state.get('country'))}")
        lines.append(f"- Current profit: {_format_currency(float(current.get('profit', 0.0)),state.get('country'))}")
        lines.append(f"- Current units: {float(current.get('total_quantity', 0.0)):,.2f}")
        lines.append(f"- Current ASP: {_format_currency(float(current.get('asp', 0.0)),state.get('country'))}")
        lines.append(f"- Current sales mix: {float(current.get('sales_mix', 0.0)):.2%}")
        lines.append(f"- Current profit mix: {float(current.get('profit_mix', 0.0)):.2%}")

        previous = res.get("previous") or {}
        if previous:
            lines.append(f"- Previous sales: {_format_currency(float(current.get('net_sales', 0.0)), state.get('country'))}")
            lines.append(f"- Previous profit: {_format_currency(float(current.get('profit', 0.0)),state.get('country'))}")
            lines.append(f"- Previous units: {float(previous.get('total_quantity', 0.0)):,.2f}")
            lines.append(f"- Previous ASP: {_format_currency(float(current.get('asp', 0.0)),state.get('country'))}")
            lines.append(f"- Previous sales mix: {float(previous.get('sales_mix', 0.0)):.2%}")
            lines.append(f"- Previous profit mix: {float(previous.get('profit_mix', 0.0)):.2%}")

        for point in (res.get("summary_points") or [])[:5]:
            lines.append(f"- {point}")

        root_cause = res.get("root_cause") or {}
        drivers = root_cause.get("drivers") or []
        if drivers:
            lines.append("")
            lines.append("Root cause drivers:")
            for driver in drivers[:4]:
                lines.append(f"- {driver['driver']}: {float(driver.get('delta', 0.0)):,.2f}")

        if state.get("advice"):
            lines.append("")
            lines.extend(f"- {a}" for a in state["advice"])

        if state.get("email_requested"):
            state["final_response"] = "📩 I've sent the SKU intelligence email."
        else:
            state["final_response"] = "\n".join(lines)
        return state

    if state.get("email_requested"):
        state["final_response"] = "📩 I've sent the email."
        return state

    current = state.get("current_metrics") or {}
    metric_name = current.get("metric") or state.get("metric_name") or "metric"
    period_label = current.get("period_label") or "selected period"
    comp = state.get("comparison") or {}
    analysis = state.get("analysis_result") or {}

    if comp:
        pct = comp.get("pct_change")
        curr = float(comp.get("left", {}).get("total", 0.0))
        prev = float(comp.get("right", {}).get("total", 0.0))
        if pct is None:
            msg = f"{metric_name} was {_format_currency(curr, state.get('country'))} vs {_format_currency(prev, state.get('country'))}"
        else:
            direction = "higher" if pct > 0 else "lower"
            msg = f"{metric_name} was {_format_currency(curr, state.get('country'))} vs {_format_currency(prev, state.get('country'))}, which is {abs(pct):.2f}% {direction}."
        if state.get("advice"):
            msg += "\n" + "\n".join(f"- {a}" for a in state["advice"])
        state["final_response"] = msg
        return state

    if analysis.get("type") == "trend":
        series = analysis.get("series", [])
        if not series:
            state["final_response"] = "No trend data found."
            return state

        lines = []
        if state.get("product_match"):
            lines.append(f"{metric_name} monthly breakdown for {state['product_match']}:")
        else:
            lines.append(f"{metric_name} monthly breakdown:")

        for row in series:
            month = row.get("month")
            year = row.get("year")

            if month and year:
                label = f"{calendar.month_abbr[int(month)]} {int(year)}"
            else:
                label = row.get("period_label", "Unknown")
            val = float(row.get("__metric__", 0.0))

            if metric_name in {"sales_mix", "profit_mix"}:
                lines.append(f"- {label}: {val:.2%}")
            else:
                lines.append(f"- {label}: {_format_currency(val, state.get('country'))}")

        if state.get("advice"):
            lines.append("")
            lines.extend(f"- {a}" for a in state["advice"])

        state["final_response"] = "\n".join(lines)
        return state

    if analysis.get("type") == "breakdown":
        rows = analysis.get("per_sku", [])
        if not rows:
            state["final_response"] = f"No productwise data found for {metric_name} in {period_label}."
            return state

        header = f"Top products for {metric_name} in {period_label}:"
        lines = [header]
        for row in rows:
            name = row.get("product_name") or row.get("sku") or "Unknown"
            value = float(row.get("__metric__", 0.0))
            if metric_name in {"sales_mix", "profit_mix"}:
                lines.append(f"- {name}: {value:.2%}")
            else:
                lines.append(f"- {name}: {_format_currency(value, state.get('country'))}")

        if state.get("advice"):
            lines.append("")
            lines.extend(f"- {a}" for a in state["advice"])

        state["final_response"] = "\n".join(lines)
        return state

    if analysis.get("type") == "summary":
        metrics = analysis.get("metrics", {})
        parts = []
        for key in [
            "net_sales",
            "profit",
            "total_quantity",
            "advertising_total",
            "platform_fee",
            "acos",
            "cm2_profit",
        ]:
            if key in metrics:
                parts.append(f"{key}: {_format_currency(metrics[key], state.get('country'))}")

        msg = f"Overall summary for {period_label}: " + ", ".join(parts)
        if state.get("advice"):
            msg += "\n" + "\n".join(f"- {a}" for a in state["advice"])
        state["final_response"] = msg
        return state

    total = current.get("total")
    if total is not None:
        if metric_name in {"sales_mix", "profit_mix"}:
            msg = f"In {period_label}, your {metric_name} was {float(total):.2%}."
        else:
            msg = f"In {period_label}, your {metric_name} was {float(total):,.2f}."
        if state.get("advice"):
            msg += "\n" + "\n".join(f"- {a}" for a in state["advice"])
        state["final_response"] = msg
        return state

    state["final_response"] = "I could not build a reliable answer for that request."
    return state


# -------------------------------------------------------------------
# MAIN GRAPH
# -------------------------------------------------------------------
class SimpleGraph:
    def invoke(self, state: AgentState) -> AgentState:
        query = state.get("user_query", "")
        history = state.get("chat_history", [])
        last_meta = load_last_analysis_from_history(history)

        plan = _plan_request(query, email_requested=bool(state.get("email_requested")))

        if (
            state.get("email_requested")
            and last_meta
            and plan.intent == "email"
            and not plan.metric_name
            and not plan.product_query
            and plan.analysis_type in {"absolute", "summary"}
        ):
            state["intent"] = "email"
            state["email_requested"] = True
            state["restored_from_memory"] = True
            state["metric_name"] = last_meta.get("metric_name")
            state["period_parsed"] = last_meta.get("period_parsed") or {"type": "latest_month"}
            state["analysis_type"] = last_meta.get("analysis_type") or "absolute"
            state["period_payload"] = _prepare_period_payload(state["period_parsed"], state["analysis_type"])
            state["current_metrics"] = last_meta.get("current_metrics") or {}
            state["comparison"] = last_meta.get("comparison") or {}
            state["analysis_result"] = last_meta.get("analysis_result") or {}
            state["event_plan_result"] = last_meta.get("event_plan_result") or {}
            state["sku_intelligence_result"] = last_meta.get("sku_intelligence_result") or {}
            state["advice"] = last_meta.get("advice") or []
            state["engine"] = get_engine()
            state = _send_email_if_requested(state)
            return _render_response(state)

        state["intent"] = plan.intent
        state["analysis_type"] = plan.analysis_type
        state["metric_name"] = plan.metric_name
        state["product_query"] = plan.product_query
        state["needs_advice"] = plan.needs_advice
        state["clarification_question"] = plan.clarification_question
        state["response_mode"] = plan.response_mode
        state["email_requested"] = bool(state.get("email_requested") or plan.intent == "email")

        state["event_name"] = plan.event_name
        state["last_event_month"] = plan.last_event_month
        state["future_event_month"] = plan.future_event_month
        state["target_sales"] = plan.target_sales

        if state["intent"] == "event_planner" or state["analysis_type"] == "event_plan":
            try:
                state = _run_event_planner(state)
                state = _generate_advice(state)
                state = _send_email_if_requested(state)
                return _render_response(state)
            except Exception as e:
                state["error"] = str(e)
                state["final_response"] = f"I couldn't generate the event plan reliably: {e}"
                return state

        if state["intent"] in {"chat", "explain", "clarify"}:
            return _render_response(state)

        # ✅ Default metric for product queries
        if not state.get("metric_name"):
            if state.get("product_query"):
                state["metric_name"] = "profit"
            elif state.get("analysis_type") not in {"summary"}:
                state["intent"] = "clarify"
            state["clarification_question"] = "Which metric would you like me to analyze?"
            return _render_response(state)

        state["engine"] = get_engine()
        state["period_parsed"] = parse_period(query)
        state["period_payload"] = _prepare_period_payload(state["period_parsed"], state["analysis_type"])
        state["comparison"] = {}
        state["analysis_result"] = {}
        state["current_metrics"] = {}
        state["sku_intelligence_result"] = {}

        try:
            state = _compute_result(state)
            state = _generate_advice(state)
            state = _send_email_if_requested(state)
            state = _render_response(state)
            return state
        except Exception as e:
            state["error"] = str(e)
            state["final_response"] = f"I couldn't process that request reliably: {e}"
            return state


def build_graph() -> SimpleGraph:
    return SimpleGraph()