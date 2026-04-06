from __future__ import annotations
import json
from typing import Any, Dict, List, Optional
from langgraph.graph import END, StateGraph
from pydantic import BaseModel, Field
from langchain_openai import ChatOpenAI
import re
from datetime import datetime
import pandas as pd
from app.ai_agent.db import (
    fetch_month_df,
    get_engine,
    get_latest_completed_month,
    previous_month,
    resolve_table_name,
    fetch_range_df,
    fetch_between_dates_df
)
from app.ai_agent.email_service import build_summary_html, send_agent_email
from app.ai_agent.formula_engine import compare_metric, compute_metric, pick_top_skus
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


def _df_to_json(df: pd.DataFrame) -> str:
    return df.to_json(orient="records", date_format="iso")


def _df_from_json(raw: str) -> pd.DataFrame:
    if not raw:
        return pd.DataFrame()
    return pd.read_json(raw, orient="records")


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

    state["intent"] = result.intent
    state["metric_name"] = result.metric_name
    state["period_mode"] = result.period_mode
    state["months_back"] = result.months_back
    state["needs_sku"] = result.needs_sku
    state["needs_advice"] = result.needs_advice
    state["response_mode"] = result.response_mode
    state["custom_range"] = result.custom_range
    state["period_1"] = result.period_1
    state["period_2"] = result.period_2
    state["email_requested"] = bool(state.get("email_requested") or result.email_requested)

    return state


def fetch_node(state: AgentState) -> AgentState:
    engine = get_engine()
    table_name = resolve_table_name(state["user_id"], state["country"])
    latest = get_latest_completed_month(engine, table_name)

    # 1) explicit custom comparison periods
    if state.get("custom_range") and state.get("period_1") and state.get("period_2"):
        try:
            p1 = state["period_1"]
            p2 = state["period_2"]

            previous_df = fetch_between_dates_df(
                engine,
                table_name,
                start_date=p1["start"],
                end_date=p1["end"],
            )
            current_df = fetch_between_dates_df(
                engine,
                table_name,
                start_date=p2["start"],
                end_date=p2["end"],
            )

            state["latest_completed_month"] = {
                "year": latest.year,
                "month": latest.month,
                "month_label": f"{latest.year}-{latest.month:02d}",
                "custom_period_1": p1,
                "custom_period_2": p2,
            }
            state["current_df_json"] = _df_to_json(current_df)
            state["previous_df_json"] = _df_to_json(previous_df)
            return state

        except Exception as e:
            print(f"[ERROR] Custom range fetch failed: {e}")

    # 2) rolling last X months
    months_back = state.get("months_back")
    if months_back:
        try:
            end_date = datetime(latest.year, latest.month, 1)
            start_date = end_date - pd.DateOffset(months=months_back)

            df = fetch_range_df(
                engine,
                table_name,
                start_iso=start_date.isoformat(),
                end_iso=end_date.isoformat(),
            )

            state["latest_completed_month"] = {
                "year": latest.year,
                "month": latest.month,
                "month_label": f"{latest.year}-{latest.month:02d}",
            }
            state["current_df_json"] = _df_to_json(df)
            state["previous_df_json"] = ""
            return state

        except Exception as e:
            print(f"[ERROR] Range fetch failed: {e}")

    # 3) default latest month + previous month
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

    metric_name = state.get("metric_name", "profit")

    # ✅ compute current metrics
    current_metrics = compute_metric(current_df, metric_name, state["country"])
    state["current_metrics"] = current_metrics

    # ✅ optional comparison (only if previous_df exists)
    if (
        state.get("intent") in {"period_comparison", "daily_summary", "weekly_summary", "advice", "send_email"}
        and not previous_df.empty
    ):
        try:
            previous_metrics = compute_metric(previous_df, metric_name, state["country"])
            state["previous_metrics"] = previous_metrics
            state["comparison"] = compare_metric(current_metrics, previous_metrics)
        except Exception as e:
            print(f"[ERROR] comparison failed: {e}")

    # ✅ NEW: SKU only when needed
    if state.get("needs_sku"):
        try:
            if state.get("intent") == "loss_making_skus":
                sku_rows = sorted(
                    current_metrics.get("per_sku", []),
                    key=lambda x: float(x.get("__metric__", 0.0))
                )[:10]
            else:
                descending = metric_name != "tax"
                sku_rows = pick_top_skus(current_metrics, n=10, reverse=descending)

            state["sku_analysis"] = sku_rows

        except Exception as e:
            print(f"[ERROR] SKU analysis failed: {e}")
            state["sku_analysis"] = []

    else:
        state["sku_analysis"] = []

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
    sku_data = state.get("sku_analysis", [])
    advice = state.get("advice", [])
    history = state.get("chat_history", [])
    comparison = state.get("comparison", {})
    response_mode = state.get("response_mode", "short")
    latest_period = state.get("latest_completed_month", {})

    history_text = ""
    for h in history:
        history_text += f"User: {h['message']}\nAssistant: {h['response']}\n"

    prompt = f"""
Conversation so far:
{history_text}

User asked: {state.get("user_query")}
Response mode: {response_mode}

Data:
Metric: {metric.get("metric")}
Total: {metric.get("total")}
Comparison: {comparison}
Top SKUs: {sku_data[:5]}
Advice: {advice}
Latest period context: {latest_period}

Rules:
- If response_mode = short, answer directly in 1-3 lines
- If response_mode = detailed, explain clearly
- If the user asked for growth, state both period values and growth percentage
- If product_name exists in SKU rows, prefer product_name over raw sku code
- Use previous conversation context when relevant
- Do not add unnecessary sections
- Be concise and business-friendly
"""

    llm_response = _llm.invoke(prompt)
    state["final_response"] = llm_response.content.strip()
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
