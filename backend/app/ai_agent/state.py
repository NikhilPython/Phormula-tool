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
    "event_planner",
]

AnalysisType = Literal[
    "absolute",
    "comparison",
    "growth",
    "trend",
    "breakdown",
    "summary",
    "event_plan",
    "sku_intelligence",
]


class AgentState(TypedDict, total=False):
    user_id: int
    country: str
    conversation_id: str
    user_query: str
    chat_history: List[Dict[str, Any]]

    # -------- Core Intent --------
    intent: Intent
    analysis_type: AnalysisType
    metric_name: Optional[str]

    # -------- Product --------
    product_match: Optional[str]
    product_query: Optional[str]

    # -------- LLM behavior --------
    response_mode: str
    needs_advice: bool
    email_requested: bool
    restored_from_memory: bool
    clarification_question: Optional[str]

    # -------- NEW: Event Planner Fields --------
    event_name: Optional[str]
    last_event_month: Optional[int]
    future_event_month: Optional[int]
    target_sales: Optional[float]

    # -------- Time / Engine --------
    period_parsed: Dict[str, Any]
    period_payload: Dict[str, Any]
    engine: Any

    # -------- Results --------
    current_metrics: Dict[str, Any]
    comparison: Dict[str, Any]
    analysis_result: Dict[str, Any]
    advice: List[str]
    final_response: str
    email_result: Dict[str, Any]
    error: Optional[str]

    # -------- Specialized Outputs --------
    event_plan_result: Dict[str, Any]
    sku_intelligence_result: Dict[str, Any]