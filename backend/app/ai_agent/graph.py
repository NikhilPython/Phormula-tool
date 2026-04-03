from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

import pandas as pd
from langgraph.graph import END, StateGraph
from pydantic import BaseModel, Field
from langchain_openai import ChatOpenAI

from config import Config
from app.ai_agent.db import (
    fetch_month_df,
    get_engine,
    get_latest_completed_month,
    previous_month,
    resolve_table_name,
)
from app.ai_agent.email_service import build_summary_html, send_agent_email
from app.ai_agent.formula_engine import compare_metric, compute_metric, pick_top_skus
from app.ai_agent.prompts import ADVISOR_SYSTEM_PROMPT, PLANNER_SYSTEM_PROMPT
from app.ai_agent.state import AgentState
from app.models.user_models import User
from dotenv import load_dotenv
load_dotenv()
import os
from config import Config

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

class PlannerResult(BaseModel):
    intent: str = Field(default="metric_qa")
    metric_name: str = Field(default="profit")
    period_mode: str = Field(default="latest_completed_month")
    email_requested: bool = Field(default=False)


class AdviceResult(BaseModel):
    advice: List[str]


_llm = ChatOpenAI(model="gpt-4.1", api_key=OPENAI_API_KEY, temperature=0)
_planner = _llm.with_structured_output(PlannerResult)
_advisor = _llm.with_structured_output(AdviceResult)


def _df_to_json(df: pd.DataFrame) -> str:
    return df.to_json(orient="records", date_format="iso")


def _df_from_json(raw: str) -> pd.DataFrame:
    if not raw:
        return pd.DataFrame()
    return pd.read_json(raw, orient="records")


def planner_node(state: AgentState) -> AgentState:
    result = _planner.invoke([
        {"role": "system", "content": PLANNER_SYSTEM_PROMPT},
        {"role": "user", "content": state["user_query"]},
    ])
    state["intent"] = result.intent
    state["metric_name"] = result.metric_name
    state["period_mode"] = result.period_mode
    state["email_requested"] = bool(state.get("email_requested") or result.email_requested)
    return state


def fetch_node(state: AgentState) -> AgentState:
    engine = get_engine()
    table_name = resolve_table_name(state["user_id"], state["country"])
    latest = get_latest_completed_month(engine, table_name)
    prev_year, prev_month_num = previous_month(latest.year, latest.month)

    current_df = fetch_month_df(engine, table_name, latest.year, latest.month)
    previous_df = fetch_month_df(engine, table_name, prev_year, prev_month_num)

    state["latest_completed_month"] = {
        "year": latest.year,
        "month": latest.month,
        "month_label": f"{latest.year}-{latest.month:02d}",
        "previous_year": prev_year,
        "previous_month": prev_month_num,
    }
    state["current_df_json"] = _df_to_json(current_df)
    state["previous_df_json"] = _df_to_json(previous_df)
    return state


def metrics_node(state: AgentState) -> AgentState:
    current_df = _df_from_json(state.get("current_df_json", ""))
    previous_df = _df_from_json(state.get("previous_df_json", ""))

    current_metrics = compute_metric(current_df, state.get("metric_name", "profit"), state["country"])
    state["current_metrics"] = current_metrics

    if state.get("intent") in {"period_comparison", "daily_summary", "weekly_summary", "advice", "send_email"}:
        previous_metrics = compute_metric(previous_df, state.get("metric_name", "profit"), state["country"])
        state["previous_metrics"] = previous_metrics
        state["comparison"] = compare_metric(current_metrics, previous_metrics)

    metric_name = current_metrics["metric"]
    if state.get("intent") == "loss_making_skus":
        sku_rows = sorted(current_metrics.get("per_sku", []), key=lambda x: float(x.get("__metric__", 0.0)))[:10]
    else:
        descending = metric_name != "tax"
        sku_rows = pick_top_skus(current_metrics, n=10, reverse=descending)
    state["sku_analysis"] = sku_rows
    return state


def advisor_node(state: AgentState) -> AgentState:
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

    user = User.query.filter_by(id=state["user_id"]).first()
    period = state.get("latest_completed_month", {})
    period_label = period.get("month_label", "latest completed month")
    current_metrics = state.get("current_metrics", {})
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
    state["email_result"] = send_agent_email(user_id=state["user_id"], subject=subject, html_body=html)
    return state


def final_node(state: AgentState) -> AgentState:
    metric = state.get("current_metrics", {})
    metric_name = metric.get("metric", state.get("metric_name", "profit"))
    total = float(metric.get("total", 0.0))
    lines = [f"Latest completed month {metric_name}: {total:,.2f}"]

    if state.get("comparison"):
        comparison = state["comparison"]
        pct = comparison.get("pct_change")
        pct_text = "N/A" if pct is None else f"{pct:.2f}%"
        lines.append(
            f"Previous month: {comparison.get('previous_total', 0.0):,.2f} | Delta: {comparison.get('delta', 0.0):,.2f} | Change: {pct_text}"
        )

    if state.get("sku_analysis"):
        sku_bits = [f"{row.get('sku')}: {float(row.get('__metric__', 0.0)):,.2f}" for row in state["sku_analysis"][:5]]
        lines.append("Top SKUs -> " + "; ".join(sku_bits))

    if state.get("advice"):
        lines.append("Recommendations:")
        lines.extend([f"- {item}" for item in state["advice"]])

    if state.get("email_result"):
        lines.append(f"Email sent to {state['email_result'].get('recipient')}")

    state["final_response"] = "\n".join(lines)
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
