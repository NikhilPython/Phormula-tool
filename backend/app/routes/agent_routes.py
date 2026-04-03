from __future__ import annotations

from datetime import datetime

from flask import Blueprint, jsonify, request

from app import db
from app.ai_agent.memory import recent_chat_history
from app.ai_agent.service import run_agent
from app.models.user_models import AgentEmailSchedule
from app.utils.token_utils import get_effective_user_id_from_token

planning_bp = Blueprint("planning_bp", __name__)


def _get_auth_user_id():
    auth_header = request.headers.get("Authorization")
    if not auth_header or not auth_header.startswith("Bearer "):
        raise PermissionError("Authorization token is missing or invalid")
    token = auth_header.split(" ", 1)[1]
    payload, effective_user_id, member_id = get_effective_user_id_from_token(token)
    return payload, effective_user_id, member_id


@planning_bp.route("/api/agent/chat", methods=["POST"])
def agent_chat():
    try:
        payload, effective_user_id, member_id = _get_auth_user_id()
        data = request.get_json(silent=True) or {}
        message = (data.get("message") or "").strip()
        if not message:
            return jsonify({"error": "message is required"}), 400

        result = run_agent(
            user_id=effective_user_id,
            country=(data.get("country") or payload.get("country") or "uk").strip().lower(),
            user_query=message,
            email_requested=bool(data.get("email_requested", False)),
            thresholds=data.get("thresholds") or {},
            conversation_id=data.get("conversation_id"),
        )
        return jsonify({"status": "success", **result}), 200
    except PermissionError as e:
        return jsonify({"error": str(e)}), 401
    except Exception as e:
        return jsonify({"error": "Failed to process AI agent request", "details": str(e)}), 500


@planning_bp.route("/api/agent/email-summary", methods=["POST"])
def agent_email_summary():
    try:
        payload, effective_user_id, member_id = _get_auth_user_id()
        data = request.get_json(silent=True) or {}
        message = (data.get("message") or "Send me the latest completed month summary").strip()
        result = run_agent(
            user_id=effective_user_id,
            country=(data.get("country") or payload.get("country") or "uk").strip().lower(),
            user_query=message,
            email_requested=True,
            thresholds=data.get("thresholds") or {},
            conversation_id=data.get("conversation_id"),
        )
        return jsonify({"status": "success", **result}), 200
    except PermissionError as e:
        return jsonify({"error": str(e)}), 401
    except Exception as e:
        return jsonify({"error": "Failed to send AI summary email", "details": str(e)}), 500


@planning_bp.route("/api/agent/history", methods=["GET"])
def agent_history():
    try:
        payload, effective_user_id, member_id = _get_auth_user_id()
        limit = min(int(request.args.get("limit", 20)), 100)
        return jsonify({"status": "success", "items": recent_chat_history(effective_user_id, limit=limit)}), 200
    except PermissionError as e:
        return jsonify({"error": str(e)}), 401
    except Exception as e:
        return jsonify({"error": "Failed to fetch history", "details": str(e)}), 500


@planning_bp.route("/api/agent/schedules", methods=["GET"])
def get_agent_schedules():
    try:
        payload, effective_user_id, member_id = _get_auth_user_id()
        rows = AgentEmailSchedule.query.filter_by(user_id=effective_user_id).order_by(AgentEmailSchedule.country.asc()).all()
        items = [
            {
                "id": row.id,
                "country": row.country,
                "frequency": row.frequency,
                "enabled": row.enabled,
                "preferred_hour": row.preferred_hour,
                "preferred_minute": row.preferred_minute,
                "metric_name": row.metric_name,
                "updated_at": row.updated_at.isoformat() if row.updated_at else None,
            }
            for row in rows
        ]
        return jsonify({"status": "success", "items": items}), 200
    except PermissionError as e:
        return jsonify({"error": str(e)}), 401
    except Exception as e:
        return jsonify({"error": "Failed to fetch schedules", "details": str(e)}), 500


@planning_bp.route("/api/agent/schedules", methods=["POST"])
def upsert_agent_schedule():
    try:
        payload, effective_user_id, member_id = _get_auth_user_id()
        data = request.get_json(silent=True) or {}
        country = (data.get("country") or payload.get("country") or "uk").strip().lower()
        frequency = (data.get("frequency") or "daily").strip().lower()
        if frequency not in {"daily", "weekly"}:
            return jsonify({"error": "frequency must be daily or weekly"}), 400

        row = AgentEmailSchedule.query.filter_by(
            user_id=effective_user_id,
            country=country,
            frequency=frequency,
        ).first()
        if row is None:
            row = AgentEmailSchedule(
                user_id=effective_user_id,
                country=country,
                frequency=frequency,
            )
            db.session.add(row)

        row.enabled = bool(data.get("enabled", True))
        row.preferred_hour = int(data.get("preferred_hour", 9))
        row.preferred_minute = int(data.get("preferred_minute", 0))
        row.metric_name = (data.get("metric_name") or "profit").strip().lower()
        row.updated_at = datetime.utcnow()
        db.session.commit()

        return jsonify({
            "status": "success",
            "item": {
                "id": row.id,
                "country": row.country,
                "frequency": row.frequency,
                "enabled": row.enabled,
                "preferred_hour": row.preferred_hour,
                "preferred_minute": row.preferred_minute,
                "metric_name": row.metric_name,
                "updated_at": row.updated_at.isoformat() if row.updated_at else None,
            }
        }), 200
    except PermissionError as e:
        return jsonify({"error": str(e)}), 401
    except Exception as e:
        db.session.rollback()
        return jsonify({"error": "Failed to save schedule", "details": str(e)}), 500
