from flask import Blueprint, request, jsonify
import jwt
from datetime import datetime
from config import Config
from app import db
from app.models.user_models import HistoricAISummary
from app.utils.monthwise_ai_summary_utils import get_or_create_summary
from app.utils.history_graph_utils import get_performance_trend

summary_bp = Blueprint("summary_bp", __name__)
SECRET_KEY = Config.SECRET_KEY

ALLOWED_PRIMARY_GOALS = {"profit", "growth", "rank", "inventory_clearance", "balanced"}
ALLOWED_RISK_LEVELS = {"conservative", "balanced", "aggressive"}

def _safe_bool(v, default=False):
    if v is None:
        return default
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return bool(v)
    if isinstance(v, str):
        return v.strip().lower() in ("true", "1", "yes", "y", "on")
    return default

def _norm_objective(payload: dict) -> dict:
    """
    Returns normalized objective dict for DB + prompt usage.
    """
    primary_goal = (payload.get("primary_goal") or "profit").strip().lower()
    if primary_goal not in ALLOWED_PRIMARY_GOALS:
        primary_goal = "profit"

    risk_level = (payload.get("risk_level") or "balanced").strip().lower()
    if risk_level not in ALLOWED_RISK_LEVELS:
        risk_level = "balanced"

    # constraints
    max_tacos = payload.get("max_tacos")
    try:
        max_tacos = int(max_tacos) if max_tacos is not None else None
    except Exception:
        max_tacos = None

    max_price_increase_pct = payload.get("max_price_increase_pct")
    try:
        max_price_increase_pct = float(max_price_increase_pct) if max_price_increase_pct is not None else None
    except Exception:
        max_price_increase_pct = None

    ad_budget_cap = payload.get("ad_budget_cap")
    try:
        ad_budget_cap = float(ad_budget_cap) if ad_budget_cap is not None else None
    except Exception:
        ad_budget_cap = None

    dont_change_price = _safe_bool(payload.get("dont_change_price"), default=False)
    notes = payload.get("notes")
    if notes is not None:
        notes = str(notes).strip() or None

    return {
        "primary_goal": primary_goal,
        "risk_level": risk_level,
        "max_tacos": max_tacos,
        "max_price_increase_pct": max_price_increase_pct,
        "ad_budget_cap": ad_budget_cap,
        "dont_change_price": dont_change_price,
        "notes": notes,
    }

@summary_bp.route("/summary", methods=["POST"])
def summary():
    auth_header = request.headers.get("Authorization")
    if not auth_header or not auth_header.startswith("Bearer "):
        return jsonify({"error": "Authorization token is missing or invalid"}), 401

    token = auth_header.split(" ")[1]

    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=["HS256"])
        user_id = payload.get("user_id")
        if not user_id:
            return jsonify({"error": "Invalid token payload: user_id missing"}), 401

        # Query params
        country = (request.args.get("country", "uk") or "uk").strip().lower()
        marketplace_id = request.args.get("marketplace_id", type=int)

        period = (request.args.get("period") or "").strip().lower()
        timeline = (request.args.get("timeline") or "").strip().upper()
        year = request.args.get("year", type=int)

        if not period or not timeline or not year:
            return jsonify({"error": "Missing required query params: period, timeline, year"}), 400
        if period not in ("monthly", "quarterly", "yearly"):
            return jsonify({"error": "Invalid period. Allowed: monthly, quarterly, yearly"}), 400

        if period == "monthly":
            if not timeline.isdigit() or not (1 <= int(timeline) <= 12):
                return jsonify({"error": "Invalid timeline for monthly. Use '1'..'12'"}), 400
        elif period == "quarterly":
            if timeline not in ("Q1", "Q2", "Q3", "Q4"):
                return jsonify({"error": "Invalid timeline for quarterly. Use 'Q1'..'Q4'"}), 400

        # Body JSON (objective from user)
        body = request.get_json(silent=True) or {}
        objective = _norm_objective(body)

        # Find existing row
        row = HistoricAISummary.query.filter_by(
            user_id=user_id,
            country=country,
            marketplace_id=marketplace_id,
            period=period,
            timeline=timeline,
            year=year,
        ).first()

        # Decide whether to regenerate summary if objective changed
        objective_changed = False
        if row:
            objective_changed = any([
                row.primary_goal != objective["primary_goal"],
                row.risk_level != objective["risk_level"],
                row.max_tacos != objective["max_tacos"],
                (float(row.max_price_increase_pct) if row.max_price_increase_pct is not None else None) != objective["max_price_increase_pct"],
                (float(row.ad_budget_cap) if row.ad_budget_cap is not None else None) != objective["ad_budget_cap"],
                bool(row.dont_change_price) != bool(objective["dont_change_price"]),
                (row.notes or None) != (objective["notes"] or None),
            ])

        # Generate / fetch summary (pass objective into your generator)
        # IMPORTANT: you need to update get_or_create_summary to accept objective (see section 3)
        result = get_or_create_summary(
            user_id=user_id,
            country=country,
            marketplace_id=marketplace_id,
            period=period,
            timeline=timeline,
            year=year,
            objective=objective,
            force_regenerate=objective_changed,  # optional control
        )

        # Save objective + summary in DB (ensure DB is source of truth)
        if not row:
            row = HistoricAISummary(
                user_id=user_id,
                country=country,
                marketplace_id=marketplace_id,
                period=period,
                timeline=timeline,
                year=year,
                summary=result.get("summary", "") or "",
                recommendations=result.get("recommendations"),
            )
            db.session.add(row)

        # update fields
        row.primary_goal = objective["primary_goal"]
        row.risk_level = objective["risk_level"]
        row.max_tacos = objective["max_tacos"]
        row.max_price_increase_pct = objective["max_price_increase_pct"]
        row.ad_budget_cap = objective["ad_budget_cap"]
        row.dont_change_price = objective["dont_change_price"]
        row.notes = objective["notes"]

        row.summary = result.get("summary", "") or row.summary
        row.recommendations = result.get("recommendations", row.recommendations)

        db.session.commit()

        # Performance trend (unchanged)
        performance_trend = get_performance_trend(
            user_id=user_id,
            country=country,
            period=period,
            timeline=timeline,
            year=year,
        )

        # Response
        result["performance_trend"] = performance_trend
        result["objective"] = objective

        return jsonify(result), 200

    except jwt.ExpiredSignatureError:
        return jsonify({"error": "Token has expired"}), 401
    except jwt.InvalidTokenError:
        return jsonify({"error": "Invalid token"}), 401
    except Exception as e:
        print("Unexpected error in /summary:", e)
        db.session.rollback()
        return jsonify({"error": "Server error", "details": str(e)}), 500

