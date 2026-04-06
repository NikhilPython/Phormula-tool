from __future__ import annotations

from typing import Any, Dict, List, Literal, Optional, TypedDict


Intent = Literal[
    "metric_qa",
    "period_comparison",
    "top_skus",
    "loss_making_skus",
    "advice",
    "daily_summary",
    "weekly_summary",
    "send_email",
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