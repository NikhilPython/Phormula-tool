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
    "sku_trend",
]

AnswerShape = Literal[
    "single_value",
    "trend",
    "comparison",
    "ranking",
    "summary",
    "extreme",
    "multi_month",
    "multi_dimensional",
    "multi_country",
]

SubjectScope = Literal["business", "product", "products", "metric"]
RankingDirection = Literal["top", "bottom"]
ExtremeType = Literal["max", "min"]
TimeGranularity = Literal["month", "quarter", "year"]


class AgentState(TypedDict, total=False):
    user_id: int
    country: str
    conversation_id: str
    target_countries: Optional[List[str]]

    user_query: str
    chat_history: List[Dict[str, Any]]

    intent: Intent
    analysis_type: AnalysisType
    metric_name: Optional[str]
    metric_names: Optional[List[str]]

    product_match: Optional[str]
    product_query: Optional[str]
    product_queries: Optional[List[str]]

    response_mode: str
    needs_advice: bool
    email_requested: bool
    restored_from_memory: bool
    clarification_question: Optional[str]

    answer_shape: AnswerShape
    subject_scope: SubjectScope
    ranking_direction: Optional[RankingDirection]
    extreme_type: Optional[ExtremeType]
    time_granularity: Optional[TimeGranularity]
    target_months: Optional[List[Dict[str, int]]]

    reasoning_mode: str
    task_type: str
    dimension: Optional[str]
    top_n: Optional[int]

    event_name: Optional[str]
    last_event_month: Optional[int]
    future_event_month: Optional[int]
    target_sales: Optional[float]

    period_parsed: Dict[str, Any]
    period_payload: Dict[str, Any]
    engine: Any

    current_metrics: Dict[str, Any]
    comparison: Dict[str, Any]
    analysis_result: Dict[str, Any]
    business_context: Dict[str, Any]
    advice: List[str]
    final_response: str
    email_result: Dict[str, Any]
    error: Optional[str]

    event_plan_result: Dict[str, Any]
    sku_intelligence_result: Dict[str, Any]
    thresholds: Dict[str, float]
    tool_trace: List[str]
