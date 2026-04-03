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
