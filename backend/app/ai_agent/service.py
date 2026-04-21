from __future__ import annotations

import logging
from typing import Any, Dict, Optional
from uuid import uuid4

from app.ai_agent.graph import build_graph
from app.ai_agent.memory import recent_chat_history, save_chat_turn

logger = logging.getLogger(__name__)

_graph = build_graph()

DEFAULT_THRESHOLDS = {
    "low_profit_margin": 10.0,
    "high_amazon_fee_ratio": 25.0,
    "high_advertising_ratio": 15.0,
}


def run_agent(
    *,
    user_id: int,
    country: str,
    user_query: str,
    email_requested: bool = False,
    thresholds: Optional[Dict[str, float]] = None,
    conversation_id: Optional[str] = None,
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
    result = _graph.invoke(state)
    try:
        save_chat_turn(
            user_id=user_id,
            message=user_query,
            response=result.get("final_response", ""),
            meta={
                "intent": result.get("intent"),
                "metric_name": result.get("metric_name"),
                "analysis_type": result.get("analysis_type"),
                "period_parsed": result.get("period_parsed"),
                "analysis_result": result.get("analysis_result"),
                "current_metrics": result.get("current_metrics"),
                "comparison": result.get("comparison"),
                "advice": result.get("advice", []),
                "event_plan_result": result.get("event_plan_result"),
                "sku_intelligence_result": result.get("sku_intelligence_result"),
                "tool_trace": result.get("tool_trace", []),
            },
        )
    except Exception:
        logger.exception("Failed to persist agent chat turn")
    return {
        "conversation_id": conversation_id,
        "response": result.get("final_response"),
        "intent": result.get("intent"),
        "metric_name": result.get("metric_name"),
        "current_metrics": result.get("current_metrics"),
        "comparison": result.get("comparison"),
        "analysis_result": result.get("analysis_result"),
        "advice": result.get("advice", []),
        "email_result": result.get("email_result"),
        "event_plan_result": result.get("event_plan_result"),
        "sku_intelligence_result": result.get("sku_intelligence_result"),
        "memory": history,
        "error": result.get("error"),
    }
