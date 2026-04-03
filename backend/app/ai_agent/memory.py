from __future__ import annotations

from typing import List, Dict, Any

from app import db
from app.models.user_models import ChatHistory


def save_chat_turn(user_id: int, message: str, response: str) -> None:
    row = ChatHistory(user_id=user_id, message=message[:1000], response=response[:2000])
    db.session.add(row)
    db.session.commit()


def recent_chat_history(user_id: int, limit: int = 8) -> List[Dict[str, Any]]:
    rows = (
        ChatHistory.query.filter_by(user_id=user_id)
        .order_by(ChatHistory.timestamp.desc())
        .limit(limit)
        .all()
    )
    items = []
    for row in reversed(rows):
        items.append({"message": row.message, "response": row.response, "timestamp": row.timestamp.isoformat() if row.timestamp else None})
    return items
