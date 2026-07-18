from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

from app import db
from app.models.user_models import ChatHistory


def _strip_nul(value: Any) -> Any:
    if isinstance(value, str):
        return value.replace("\x00", "")
    if isinstance(value, list):
        return [_strip_nul(item) for item in value]
    if isinstance(value, dict):
        return {str(key).replace("\x00", ""): _strip_nul(item) for key, item in value.items()}
    return value


def save_chat_turn(
    user_id: int,
    message: str,
    response: str,
    meta: Optional[Dict[str, Any]] = None,
) -> int:
    clean_message = _strip_nul(message or "")
    clean_response = _strip_nul(response or "")
    clean_meta = _strip_nul(meta or {})

    row = ChatHistory(
        user_id=user_id,
        message=str(clean_message)[:1000],
        response=str(clean_response),
        meta=json.dumps(clean_meta, default=str),
    )

    db.session.add(row)
    db.session.commit()
    return int(row.id)


def recent_chat_history(user_id: int, limit: int = 8) -> List[Dict[str, Any]]:
    rows = (
        ChatHistory.query.filter_by(user_id=user_id)
        .order_by(ChatHistory.timestamp.desc())
        .limit(limit)
        .all()
    )
    items: List[Dict[str, Any]] = []
    for row in reversed(rows):
        items.append(
            {
                "message": row.message,
                "response": row.response,
                "meta": row.meta,
                "timestamp": row.timestamp.isoformat() if row.timestamp else None,
            }
        )
    return items


def load_last_analysis_from_history(history: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    for h in reversed(history or []):
        raw = h.get("meta")
        if not raw:
            continue
        try:
            meta = json.loads(raw) if isinstance(raw, str) else raw
        except Exception:
            continue
        if meta.get("current_metrics") or meta.get("comparison") or meta.get("analysis_result") or meta.get("event_plan_result"):
            return meta
    return None
