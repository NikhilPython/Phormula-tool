import traceback
from flask import Blueprint, json, request, jsonify
import jwt
from datetime import datetime, date
from config import Config
from app import db
from app.models.user_models import HistoricAISummary, UserObjective
from app.utils.monthwise_ai_summary_utils import get_or_create_summary
from app.utils.token_utils import get_effective_user_id_from_token
from app.utils.history_graph_utils import get_performance_trend

summary_bp = Blueprint("summary_bp", __name__)


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
    Returns normalized objective dict (with NESTED constraints)
    for DB + prompt usage.
    """

    primary_goal = (payload.get("primary_goal") or "profit").strip().lower()
    if primary_goal not in ALLOWED_PRIMARY_GOALS:
        primary_goal = "profit"

    risk_level = (payload.get("risk_level") or "balanced").strip().lower()
    if risk_level not in ALLOWED_RISK_LEVELS:
        risk_level = "balanced"

    raw_constraints = payload.get("constraints") or {}

    # constraints
    max_tacos = raw_constraints.get("max_tacos")
    try:
        max_tacos = int(max_tacos) if max_tacos is not None else None
    except Exception:
        max_tacos = None

    max_price_increase_pct = raw_constraints.get("max_price_increase_pct")
    try:
        max_price_increase_pct = (
            float(max_price_increase_pct)
            if max_price_increase_pct is not None else None
        )
    except Exception:
        max_price_increase_pct = None

    ad_budget_cap = raw_constraints.get("ad_budget_cap")
    try:
        ad_budget_cap = float(ad_budget_cap) if ad_budget_cap is not None else None
    except Exception:
        ad_budget_cap = None

    dont_change_price = _safe_bool(
        raw_constraints.get("dont_change_price"),
        default=False
    )

    notes = payload.get("notes")
    if notes is not None:
        notes = str(notes).strip() or None

    return {
        "primary_goal": primary_goal,
        "risk_level": risk_level,
        "constraints": {
            "max_tacos": max_tacos,
            "max_price_increase_pct": max_price_increase_pct,
            "ad_budget_cap": ad_budget_cap,
            "dont_change_price": dont_change_price,
        },
        "notes": notes,
    }


def _objective_from_row(row):
    if not row:
        return None

    return {
        "primary_goal": row.primary_goal,
        "risk_level": row.risk_level,
        "constraints": {
            "max_tacos": row.max_tacos,
            "max_price_increase_pct": (
                float(row.max_price_increase_pct)
                if row.max_price_increase_pct is not None else None
            ),
            "ad_budget_cap": (
                float(row.ad_budget_cap)
                if row.ad_budget_cap is not None else None
            ),
            "dont_change_price": bool(row.dont_change_price),
        },
        "notes": row.notes or None,
    }



@summary_bp.route("/summary", methods=["GET"])
def summary():
    auth_header = request.headers.get("Authorization")
    if not auth_header or not auth_header.startswith("Bearer "):
        return jsonify({"error": "Authorization token is missing or invalid"}), 401

    token = auth_header.split(" ")[1]

    try:
        payload, user_id, member_id = get_effective_user_id_from_token(token)
        user_id = payload.get("user_id")

        if not user_id:
            return jsonify({"error": "Invalid token payload: user_id missing"}), 401

        # ---------------- Query params ----------------
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

        elif period == "yearly":
            if timeline not in ("ALL", ""):
                return jsonify({"error": "Invalid timeline for yearly. Use 'ALL'"}), 400

        # ==========================================================
        # Generate or fetch summary
        # ==========================================================
        result = get_or_create_summary(
            user_id=user_id,
            country=country,
            marketplace_id=marketplace_id,
            period=period,
            timeline=timeline,
            year=year,
            force_regenerate=False
        )

        # Attach objective snapshot to response
    
    

        return jsonify(result), 200

    except jwt.ExpiredSignatureError:
        return jsonify({"error": "Token has expired"}), 401

    except jwt.InvalidTokenError:
        return jsonify({"error": "Invalid token"}), 401

    except Exception as e:
        traceback.print_exc()
        db.session.rollback()
        return jsonify({
            "error": "Server error",
            "details": str(e)
        }), 500




def get_month_start(value=None):
    if not value:
        now = datetime.utcnow()
        return date(now.year, now.month, 1)

    if isinstance(value, date):
        return date(value.year, value.month, 1)

    value = str(value).strip()
    try:
        if len(value) == 7:
            dt = datetime.strptime(value, "%Y-%m")
        else:
            dt = datetime.strptime(value, "%Y-%m-%d")
        return date(dt.year, dt.month, 1)
    except ValueError:
        raise ValueError("month must be in 'YYYY-MM' or 'YYYY-MM-DD'")
    

@summary_bp.route("/objective", methods=["POST"])
def save_user_objective():
    auth_header = request.headers.get("Authorization")
    if not auth_header or not auth_header.startswith("Bearer "):
        return jsonify({"error": "Authorization token is missing or invalid"}), 401

    token = auth_header.split(" ")[1]

    try:
        payload, user_id, member_id = get_effective_user_id_from_token(token)
        user_id = payload.get("user_id")

        if not user_id:
            return jsonify({"error": "Invalid token payload"}), 401

        body = request.get_json() or {}

        # -------- Country --------
        country = (body.get("country") or "").strip().lower()
        if not country:
            return jsonify({"error": "country is required"}), 400

        # -------- Month --------
        try:
            objective_month = get_month_start(body.get("month"))
        except ValueError as e:
            return jsonify({"error": str(e)}), 400

        # -------- Fields --------
        growth_intent = body.get("growth_intent", "balanced")
        profit_priority = body.get("profit_priority", "protect_growth")
        inventory_clearance_priority = body.get("inventory_clearance_priority", False)
        business_context = body.get("business_context")
        website_url = body.get("website_url")
        ppt_file_name = body.get("ppt_file_name")

        # -------- CHECK EXISTING --------
        existing = UserObjective.query.filter_by(
            user_id=user_id,
            country=country,
            objective_month=objective_month
        ).first()

        if existing:
            # UPDATE
            existing.growth_intent = growth_intent
            existing.profit_priority = profit_priority
            existing.inventory_clearance_priority = inventory_clearance_priority
            existing.business_context = business_context
            existing.website_url = website_url
            existing.ppt_file_name = ppt_file_name

            db.session.commit()

            return jsonify({
                "message": "Objective updated successfully",
                "objective": {
                    "id": existing.id,
                    "month": existing.objective_month.strftime("%Y-%m"),
                }
            }), 200

        # -------- CREATE NEW --------
        new_obj = UserObjective(
            user_id=user_id,
            country=country,
            objective_month=objective_month,
            growth_intent=growth_intent,
            profit_priority=profit_priority,
            inventory_clearance_priority=inventory_clearance_priority,
            business_context=business_context,
            website_url=website_url,
            ppt_file_name=ppt_file_name,
        )

        db.session.add(new_obj)
        db.session.commit()

        return jsonify({
            "message": "Objective created successfully",
            "objective": {
                "id": new_obj.id,
                "month": new_obj.objective_month.strftime("%Y-%m"),
            }
        }), 201

    except Exception as e:
        traceback.print_exc()
        db.session.rollback()
        return jsonify({"error": "Server error", "details": str(e)}), 500
    

@summary_bp.route("/objective", methods=["GET"])
def get_user_objective():
    auth_header = request.headers.get("Authorization")
    if not auth_header or not auth_header.startswith("Bearer "):
        return jsonify({"error": "Authorization token is missing or invalid"}), 401

    token = auth_header.split(" ")[1]

    try:
        payload, user_id, member_id = get_effective_user_id_from_token(token)
        user_id = payload.get("user_id")

        if not user_id:
            return jsonify({"error": "Invalid token payload"}), 401

        country = (request.args.get("country") or "").strip().lower()
        if not country:
            return jsonify({"error": "country is required"}), 400

        month = request.args.get("month")
        get_all = str(request.args.get("all", "false")).lower() in ("true", "1", "yes")

        query = UserObjective.query.filter_by(user_id=user_id, country=country)

        # -------- ALL DATA --------
        if get_all:
            rows = query.order_by(UserObjective.objective_month.desc()).all()

            return jsonify({
                "objectives": [
                    {
                        "id": r.id,
                        "month": r.objective_month.strftime("%Y-%m"),
                        "growth_intent": r.growth_intent,
                        "profit_priority": r.profit_priority,
                        "inventory_clearance_priority": r.inventory_clearance_priority,
                        "business_context": r.business_context,
                    }
                    for r in rows
                ]
            }), 200

        # -------- SINGLE MONTH --------
        if month:
            objective_month = get_month_start(month)
        else:
            objective_month = get_month_start()

        row = query.filter_by(objective_month=objective_month).first()

        if not row:
            return jsonify({"message": "No data found"}), 404

        return jsonify({
            "objective": {
                "id": row.id,
                "month": row.objective_month.strftime("%Y-%m"),
                "growth_intent": row.growth_intent,
                "profit_priority": row.profit_priority,
                "inventory_clearance_priority": row.inventory_clearance_priority,
                "business_context": row.business_context,
            }
        }), 200

    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": "Server error", "details": str(e)}), 500
    

