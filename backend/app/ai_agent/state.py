from __future__ import annotations

from typing import Any, Dict, List, Literal, Optional, TypedDict


Intent = Literal[
    "chat",
    "explain",
    "clarify",

    "metric_qa",
    "comparison",
    "report",
    "email",

    "top_skus",
    "loss_making_skus",
    "advice",

    "event_planner",
    "pricing_planner",
    "inventory_planner",
]


class AgentState(TypedDict, total=False):
    user_id: int
    country: str
    conversation_id: str
    user_query: str
    intent: Intent
    metric_name: str
    period_mode: str
    filters: Dict[str, Any]
    thresholds: Dict[str, float]

    months_back: Optional[int]
    needs_sku: bool
    needs_advice: bool
    response_mode: str
    chat_history: List[Dict[str, Any]]

    custom_range: bool
    period_1: Dict[str, Any]
    period_2: Dict[str, Any]

    latest_completed_month: Dict[str, Any]
    current_df_json: str
    previous_df_json: str
    current_metrics: Dict[str, Any]
    previous_metrics: Dict[str, Any]
    comparison: Dict[str, Any]
    sku_analysis: List[Dict[str, Any]]
    product_query: Optional[str]
    product_queries: Optional[List[str]]
    advice: List[str]
    email_requested: bool
    email_result: Dict[str, Any]
    final_response: str
    citations: List[str]
    error: Optional[str]
    data_mode: bool
    raw_df: Optional[list]

    # 🔥 ADD THESE (CRITICAL FIX)
    period_parsed: Optional[Dict[str, Any]]
    period_payload: Optional[Dict[str, Any]]
    engine: Optional[Any]

    clarification_question: Optional[str]
    planner_payload: Optional[Dict[str, Any]]
    planner_result: Optional[Dict[str, Any]]

    # 🔥 NEW: analysis + insight layer
    analysis_type: Optional[str]
    analysis_result: Optional[Dict[str, Any]]
    insight_context: Optional[Dict[str, Any]]
    product_match: Optional[str]