from __future__ import annotations

from typing import Any, Dict, Optional
from uuid import uuid4

from app.ai_agent.graph import build_graph
from app.ai_agent.memory import recent_chat_history, save_chat_turn

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

    # ✅ NEW: fetch recent memory
    history = recent_chat_history(user_id, limit=5)

    state = {
        "user_id": int(user_id),
        "country": (country or "uk").strip().lower(),
        "conversation_id": conversation_id,
        "user_query": user_query.strip(),
        "email_requested": bool(email_requested),
        "thresholds": {**DEFAULT_THRESHOLDS, **(thresholds or {})},

        # 🔥 NEW: pass memory into agent
        "chat_history": history,
    }

    result = _graph.invoke(state)

    # ✅ save conversation
    save_chat_turn(
        user_id=user_id,
        message=user_query,
        response=result.get("final_response", "")
    )

    return {
        "conversation_id": conversation_id,
        "response": result.get("final_response"),
        "intent": result.get("intent"),
        "metric_name": result.get("metric_name"),
        "latest_completed_month": result.get("latest_completed_month"),
        "current_metrics": result.get("current_metrics"),
        "comparison": result.get("comparison"),
        "sku_analysis": result.get("sku_analysis", []),
        "advice": result.get("advice", []),
        "email_result": result.get("email_result"),

        # optional: frontend ke liye
        "memory": history,
    }
