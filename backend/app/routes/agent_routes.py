
from __future__ import annotations

import logging
import math
from datetime import datetime

from flask import Blueprint, jsonify, request

from app import db
from app.ai_agent.memory import recent_chat_history
from app.ai_agent.service import run_agent, generate_what_if_scenario_analysis
from app.models.user_models import AgentEmailSchedule
from app.utils.token_utils import get_effective_user_id_from_token

agent_bp = Blueprint("agent_bp", __name__)
logger = logging.getLogger(__name__)


def _normalize_country(value):
    raw = str(value or "").strip().lower().replace("_", " ").replace("-", " ")
    raw = " ".join(raw.split())
    aliases = {
        "uk": "uk",
        "gb": "uk",
        "gbr": "uk",
        "amazon uk": "uk",
        "united kingdom": "uk",
        "us": "us",
        "usa": "us",
        "amazon us": "us",
        "united states": "us",
        "united states of america": "us",
        "global": "global",
        "all": "global",
        "all countries": "global",
    }
    return aliases.get(raw, raw.replace(" ", "")) or None


def _request_country(data, token_payload):
    for key in ("country", "selected_country", "selectedCountry", "countryName"):
        country = _normalize_country(data.get(key))
        if country:
            return country
    return _normalize_country(token_payload.get("country")) or "uk"


def _as_bool(value, default=False):
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y", "on"}
    return bool(value)


def _get_auth_user_id():
    auth_header = request.headers.get("Authorization")
    if not auth_header or not auth_header.startswith("Bearer "):
        raise PermissionError("Authorization token is missing or invalid")
    token = auth_header.split(" ", 1)[1]
    payload, effective_user_id, member_id = get_effective_user_id_from_token(token)
    return payload, effective_user_id, member_id


def _scenario_snapshot(value, label):
    if not isinstance(value, dict):
        raise ValueError(f"{label} scenario data is required")

    numeric_fields = (
        "units",
        "asp",
        "ads",
        "net_sales",
        "cm1",
        "cm2",
        "cm2_margin",
    )
    snapshot = {}
    for field in numeric_fields:
        try:
            number = float(value[field])
        except (KeyError, TypeError, ValueError, OverflowError):
            raise ValueError(f"{label}.{field} must be a number")
        if not math.isfinite(number):
            raise ValueError(f"{label}.{field} must be finite")
        snapshot[field] = number

    if any(snapshot[field] < 0 for field in ("units", "asp", "ads", "net_sales")):
        raise ValueError(f"{label} units, ASP, ads, and net sales cannot be negative")

    for field in (
        "target_roas",
        "ad_sales_at_target_roas",
        "estimated_break_even_roas",
        "ad_sales_at_break_even_roas",
        "target_roas_goal",
        "ad_sales_at_target_roas_goal",
    ):
        raw_value = value.get(field)
        if raw_value is None:
            continue
        try:
            number = float(raw_value)
        except (TypeError, ValueError, OverflowError):
            raise ValueError(f"{label}.{field} must be a number")
        if not math.isfinite(number) or number < 0:
            raise ValueError(f"{label}.{field} must be a finite non-negative number")
        snapshot[field] = number

    snapshot["month"] = str(value.get("month") or "")[:40]
    return snapshot


def _scenario_internal_costs(value, label):
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise ValueError(f"{label} internal cost context must be an object")

    fields = (
        "cogs",
        "amazon_fees",
        "other_transaction_fees",
        "inventory_charges_and_reimbursement",
        "short_term_storage_fees",
        "long_term_storage_fees",
        "other_fees",
        "shipping_charges",
    )
    signed_fields = {"other_transaction_fees", "inventory_charges_and_reimbursement"}
    snapshot = {}
    for field in fields:
        try:
            number = float(value.get(field, 0))
        except (TypeError, ValueError, OverflowError):
            raise ValueError(f"{label}.{field} must be a number")
        if not math.isfinite(number):
            raise ValueError(f"{label}.{field} must be finite")
        if field not in signed_fields and number < 0:
            raise ValueError(f"{label}.{field} cannot be negative")
        snapshot[field] = number
    return snapshot


def _chat_response_payload(result, include_debug=False):
    payload = {
        "status": "success",
        "conversation_id": result.get("conversation_id"),
        "response": result.get("response"),
        "intent": result.get("intent"),
        "country": result.get("country"),
        "target_countries": result.get("target_countries"),
        "metric_name": result.get("metric_name"),
        "metric_names": result.get("metric_names"),
        "analysis_type": result.get("analysis_type"),
        "answer_validation": result.get("answer_validation"),
        "suggested_questions": result.get("suggested_questions") or [],
        "history_id": result.get("history_id"),
        "error": result.get("error"),
    }

    if include_debug:
        payload.update(
            {
                "semantic_resolution": result.get("semantic_resolution"),
                "current_metrics": result.get("current_metrics"),
                "comparison": result.get("comparison"),
                "analysis_result": result.get("analysis_result"),
                "advice": result.get("advice", []),
                "email_result": result.get("email_result"),
                "event_plan_result": result.get("event_plan_result"),
                "sku_intelligence_result": result.get("sku_intelligence_result"),
                "memory": result.get("memory"),
            }
        )

    return payload


@agent_bp.route("/api/agent/chat", methods=["POST"])
def agent_chat():
    try:
        payload, effective_user_id, member_id = _get_auth_user_id()
        data = request.get_json(silent=True) or {}
        message = (data.get("message") or "").strip()
        if not message:
            return jsonify({"error": "message is required"}), 400

        result = run_agent(
            user_id=effective_user_id,
            country=_request_country(data, payload),
            user_query=message,
            email_requested=bool(data.get("email_requested", False)),
            thresholds=data.get("thresholds") or {},
            conversation_id=data.get("conversation_id"),
            include_suggested_questions=_as_bool(data.get("include_suggested_questions"), True),
        )
        return jsonify(_chat_response_payload(result, _as_bool(data.get("include_debug"), False))), 200
    except PermissionError as e:
        return jsonify({"error": str(e)}), 401
    except Exception as e:
        logger.exception("Failed to process AI agent request")
        return jsonify({"error": "Failed to process AI agent request", "details": str(e)}), 500


@agent_bp.route("/api/agent/what-if-analysis", methods=["POST"])
def agent_what_if_analysis():
    try:
        payload, effective_user_id, member_id = _get_auth_user_id()
        data = request.get_json(silent=True) or {}
        country = _request_country(data, payload)
        baseline = _scenario_snapshot(data.get("baseline"), "baseline")
        scenario = _scenario_snapshot(data.get("scenario"), "scenario")
        currency = str(data.get("currency") or "").strip().upper()[:8]
        internal_cost_context = data.get("internal_cost_context") or {}
        if not isinstance(internal_cost_context, dict):
            raise ValueError("internal_cost_context must be an object")
        baseline_internal_costs = _scenario_internal_costs(
            internal_cost_context.get("baseline"), "baseline"
        )
        scenario_internal_costs = _scenario_internal_costs(
            internal_cost_context.get("scenario"), "scenario"
        )

        context = {
            "country": country,
            "currency": currency,
            "baseline": baseline,
            "scenario": scenario,
            "internal_cost_context": {
                "baseline": baseline_internal_costs,
                "scenario": scenario_internal_costs,
            },
            "change": {
                "units": scenario["units"] - baseline["units"],
                "asp": scenario["asp"] - baseline["asp"],
                "ads": scenario["ads"] - baseline["ads"],
                "net_sales": scenario["net_sales"] - baseline["net_sales"],
                "cm1": scenario["cm1"] - baseline["cm1"],
                "cm2": scenario["cm2"] - baseline["cm2"],
                "cm2_margin_percentage_points": (
                    scenario["cm2_margin"] - baseline["cm2_margin"]
                ),
                "asp_effect_on_sales_and_cm_at_scenario_units": (
                    scenario["units"] * (scenario["asp"] - baseline["asp"])
                ),
                "ads_effect_on_cm2_vs_baseline_spend": -(
                    scenario["ads"] - baseline["ads"]
                ),
                "target_roas": (
                    scenario.get("target_roas") - baseline.get("target_roas")
                    if scenario.get("target_roas") is not None
                    and baseline.get("target_roas") is not None
                    else None
                ),
                "estimated_break_even_roas": (
                    scenario.get("estimated_break_even_roas") - baseline.get("estimated_break_even_roas")
                    if scenario.get("estimated_break_even_roas") is not None
                    and baseline.get("estimated_break_even_roas") is not None
                    else None
                ),
                "target_roas_goal": scenario.get("target_roas_goal"),
            },
        }
        analysis = generate_what_if_scenario_analysis(context)
        return jsonify({"status": "success", "analysis": analysis}), 200
    except PermissionError as e:
        return jsonify({"error": str(e)}), 401
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    except RuntimeError as e:
        return jsonify({"error": str(e)}), 503
    except Exception:
        logger.exception("Failed to generate what-if AI analysis")
        return jsonify({"error": "AI scenario analysis is temporarily unavailable."}), 503


@agent_bp.route("/api/agent/email-summary", methods=["POST"])
def agent_email_summary():
    try:
        payload, effective_user_id, member_id = _get_auth_user_id()
        data = request.get_json(silent=True) or {}
        message = (data.get("message") or "Send me the latest completed month profit summary").strip()
        result = run_agent(
            user_id=effective_user_id,
            country=_request_country(data, payload),
            user_query=message,
            email_requested=True,
            thresholds=data.get("thresholds") or {},
            conversation_id=data.get("conversation_id"),
        )
        return jsonify({"status": "success", **result}), 200
    except PermissionError as e:
        return jsonify({"error": str(e)}), 401
    except Exception as e:
        logger.exception("Failed to send AI summary email")
        return jsonify({"error": "Failed to send AI summary email", "details": str(e)}), 500


@agent_bp.route("/api/agent/history", methods=["GET"])
def agent_history():
    try:
        payload, effective_user_id, member_id = _get_auth_user_id()
        limit = min(int(request.args.get("limit", 20)), 100)
        return jsonify({"status": "success", "items": recent_chat_history(effective_user_id, limit=limit)}), 200
    except PermissionError as e:
        return jsonify({"error": str(e)}), 401
    except Exception as e:
        return jsonify({"error": "Failed to fetch history", "details": str(e)}), 500


@agent_bp.route("/api/agent/schedules", methods=["GET"])
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
                "query": row.query,
                "analysis_type": row.analysis_type,
                "period_payload": row.period_payload,
                "day_of_month": row.day_of_month,
                "last_run_at": row.last_run_at.isoformat() if row.last_run_at else None,
                "updated_at": row.updated_at.isoformat() if row.updated_at else None,
            }
            for row in rows
        ]
        return jsonify({"status": "success", "items": items}), 200
    except PermissionError as e:
        return jsonify({"error": str(e)}), 401
    except Exception as e:
        return jsonify({"error": "Failed to fetch schedules", "details": str(e)}), 500


@agent_bp.route("/api/agent/schedules", methods=["POST"])
def upsert_agent_schedule():
    try:
        payload, effective_user_id, member_id = _get_auth_user_id()
        data = request.get_json(silent=True) or {}
        country = _request_country(data, payload)
        frequency = (data.get("frequency") or "daily").strip().lower()
        if frequency not in {"daily", "weekly", "monthly"}:
            return jsonify({"error": "frequency must be daily, weekly, or monthly"}), 400

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
        row.query = data.get("query")
        row.analysis_type = data.get("analysis_type")
        row.period_payload = data.get("period_payload")
        row.day_of_month = data.get("day_of_month")
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
                "query": row.query,
                "analysis_type": row.analysis_type,
                "period_payload": row.period_payload,
                "day_of_month": row.day_of_month,
                "last_run_at": row.last_run_at.isoformat() if row.last_run_at else None,
                "updated_at": row.updated_at.isoformat() if row.updated_at else None,
            },
        }), 200
    except PermissionError as e:
        return jsonify({"error": str(e)}), 401
    except Exception as e:
        db.session.rollback()
        return jsonify({"error": "Failed to save schedule", "details": str(e)}), 500
