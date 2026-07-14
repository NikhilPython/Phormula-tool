
from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

from app import db
from app.models.user_models import ChatHistory


def save_chat_turn(
    user_id: int,
    message: str,
    response: str,
    meta: Optional[Dict[str, Any]] = None,
) -> int:
    row = ChatHistory(
        user_id=user_id,
        message=(message or "")[:1000],   # keep this (safe)
        response=response or "",          # ✅ no slicing needed now
        meta=json.dumps(meta or {}, default=str),
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
