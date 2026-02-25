import traceback
from flask import Blueprint, json, request, jsonify
import jwt
from datetime import datetime
from config import Config
from app import db
from app.models.user_models import HistoricAISummary, UserObjective
from app.utils.monthwise_ai_summary_utils import get_or_create_summary
from app.utils.token_utils import get_effective_user_id_from_token
from app.utils.history_graph_utils import get_performance_trend

summary_bp = Blueprint("summary_bp", __name__)
objective_bp = Blueprint("objective_bp", __name__)
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




# @summary_bp.route("/summary", methods=["GET"])
# def summary():
#     auth_header = request.headers.get("Authorization")
#     if not auth_header or not auth_header.startswith("Bearer "):
#         return jsonify({"error": "Authorization token is missing or invalid"}), 401

#     token = auth_header.split(" ")[1]

#     try:
#         payload, user_id, member_id = get_effective_user_id_from_token(token)
#         user_id = payload.get("user_id")
#         if not user_id:
#             return jsonify({"error": "Invalid token payload: user_id missing"}), 401

#         # ---------------- Query params ----------------
#         country = (request.args.get("country", "uk") or "uk").strip().lower()
#         marketplace_id = request.args.get("marketplace_id", type=int)

#         period = (request.args.get("period") or "").strip().lower()
#         timeline = (request.args.get("timeline") or "").strip().upper()
#         year = request.args.get("year", type=int)

#         if not period or not timeline or not year:
#             return jsonify({"error": "Missing required query params: period, timeline, year"}), 400
#         if period not in ("monthly", "quarterly", "yearly"):
#             return jsonify({"error": "Invalid period. Allowed: monthly, quarterly, yearly"}), 400

#         if period == "monthly":
#             if not timeline.isdigit() or not (1 <= int(timeline) <= 12):
#                 return jsonify({"error": "Invalid timeline for monthly. Use '1'..'12'"}), 400
#         elif period == "quarterly":
#             if timeline not in ("Q1", "Q2", "Q3", "Q4"):
#                 return jsonify({"error": "Invalid timeline for quarterly. Use 'Q1'..'Q4'"}), 400

#         elif period == "yearly":
#             # 🔒 Production safety: enforce deterministic yearly key
#             if timeline not in ("ALL", ""):
#                 return jsonify({"error": "Invalid timeline for yearly. Use 'ALL'"}), 400    


#         # ---------------- Body JSON (objective from user – OPTIONAL) ----------------
#         body = request.get_json(silent=True) or {}
#         request_objective = _norm_objective(body) if body else None

#         # ---------------- Fetch existing summary row ----------------
#         row = HistoricAISummary.query.filter_by(
#             user_id=user_id,
#             country=country,
#             marketplace_id=marketplace_id,
#             period=period,
#             timeline=timeline,
#             year=year,
#         ).first()

#         # =====================================================================
#         # NEW: Load LATEST objective from UserObjective table (source of truth)
#         # =====================================================================
#         latest_objective_row = (
#             UserObjective.query
#             .filter_by(user_id=user_id, country=country)
#             .order_by(UserObjective.created_at.desc())
#             .first()
#         )

#         db_objective = None
#         if latest_objective_row:
#             db_objective = {
#                 "primary_goal": latest_objective_row.primary_goal,
#                 "risk_level": latest_objective_row.risk_level,
#                 "constraints": DEFAULT_USER_OBJECTIVE["constraints"],
#                 "notes": latest_objective_row.notes,
#             }

#         # ---------------- FINAL objective resolution ----------------
#         final_objective = (
#             request_objective
#             or db_objective
#             or DEFAULT_USER_OBJECTIVE
#         )

#         # =====================================================================
#         # Decide whether to regenerate summary
#         # Regenerate ONLY if objective snapshot changed
#         # =====================================================================
#         # =====================================================================
#         stored_objective = _objective_from_row(row) if row else None
#         objective_changed = stored_objective != final_objective

       

#         # ---------------- Generate / fetch summary ----------------
#         result = get_or_create_summary(
#             user_id=user_id,
#             country=country,
#             marketplace_id=marketplace_id,
#             period=period,
#             timeline=timeline,
#             year=year,
#             objective=final_objective,
#             force_regenerate=objective_changed,
#         )

#         # ---------------- Save summary + objective snapshot ----------------
#         if not row:
#             row = HistoricAISummary(
#                 user_id=user_id,
#                 country=country,
#                 marketplace_id=marketplace_id,
#                 period=period,
#                 timeline=timeline,
#                 year=year,
#             )
#             db.session.add(row)

#         # Objective SNAPSHOT (frozen for determinism)
#         row.primary_goal = final_objective["primary_goal"]
#         row.risk_level = final_objective["risk_level"]
#         row.max_tacos = final_objective["constraints"]["max_tacos"]
#         row.max_price_increase_pct = final_objective["constraints"]["max_price_increase_pct"]
#         row.ad_budget_cap = final_objective["constraints"]["ad_budget_cap"]
#         row.dont_change_price = final_objective["constraints"]["dont_change_price"]
#         row.notes = final_objective.get("notes")

#         # Summary output
#         row.summary = result.get("summary", "") or row.summary
#         reco = result.get("recommendations")
#         if isinstance(reco, dict):
#             row.recommendations = json.dumps(reco)
#         elif isinstance(reco, str):
#             row.recommendations = reco

#         db.session.commit()

#         # ---------------- Response ----------------
#         result["objective"] = {
#             "primary_goal": final_objective["primary_goal"],
#             "risk_level": final_objective["risk_level"],
#         }

#         result["objective_changed"] = objective_changed

#         return jsonify(result), 200

#     except jwt.ExpiredSignatureError:
#         return jsonify({"error": "Token has expired"}), 401
#     except jwt.InvalidTokenError:
#         return jsonify({"error": "Invalid token"}), 401
#     except Exception as e:
      
#         traceback.print_exc()
       
#         db.session.rollback()
#         return jsonify({
#             "error": "Server error",
#             "details": str(e)
#         }), 500

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



# @summary_bp.route("/objective", methods=["POST"])
# def save_user_objective():
#     auth_header = request.headers.get("Authorization")
#     if not auth_header or not auth_header.startswith("Bearer "):
#         return jsonify({"error": "Authorization token is missing or invalid"}), 401

#     token = auth_header.split(" ")[1]

#     try:
#         payload, user_id, member_id = get_effective_user_id_from_token(token)
#         user_id = payload.get("user_id")
#         if not user_id:
#             return jsonify({"error": "Invalid token payload"}), 401

#         body = request.get_json() or {}

#         country = (body.get("country")).strip().lower()
#         primary_goal = body.get("primary_goal")
#         risk_level = body.get("risk_level")
#         notes = body.get("notes")

#         if not primary_goal:
#             return jsonify({"error": "primary_goal is required"}), 400

#         objective = UserObjective(
#             user_id=user_id,
#             country=country,
#             primary_goal=primary_goal,
#             risk_level=risk_level,
#             notes=notes,
#         )

#         db.session.add(objective)
#         db.session.commit()

#         return jsonify({
#             "message": "Objective saved successfully",
#             "objective": {
#                 "user_id": user_id,
#                 "country": country,
#                 "primary_goal": primary_goal,
#                 "risk_level": risk_level,
#                 "notes": notes,
#             }
#         }), 200

#     except Exception as e:
#         db.session.rollback()
#         return jsonify({
#             "error": "Server error",
#             "details": str(e)
#         }), 500


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

        # ✅ safe country handling
        country_raw = body.get("country", "")
        country = country_raw.strip().lower()
        if not country:
            return jsonify({"error": "country is required"}), 400

        # ✅ match your model fields
        growth_intent = body.get("growth_intent", "balanced")
        profit_priority = body.get("profit_priority", "protect_growth")
        inventory_clearance_priority = body.get("inventory_clearance_priority", False)
        business_context = body.get("business_context")

        # ✅ upsert (because unique constraint user_id+country)
        objective = UserObjective.query.filter_by(user_id=user_id, country=country).first()

        if objective:
            objective.growth_intent = growth_intent
            objective.profit_priority = profit_priority
            objective.inventory_clearance_priority = inventory_clearance_priority
            objective.business_context = business_context
        else:
            objective = UserObjective(
                user_id=user_id,
                country=country,
                growth_intent=growth_intent,
                profit_priority=profit_priority,
                inventory_clearance_priority=inventory_clearance_priority,
                business_context=business_context,
            )
            db.session.add(objective)

        db.session.commit()

        return jsonify({
            "message": "Objective saved successfully",
            "objective": {
                "user_id": user_id,
                "country": objective.country,
                "growth_intent": objective.growth_intent,
                "profit_priority": objective.profit_priority,
                "inventory_clearance_priority": objective.inventory_clearance_priority,
                "business_context": objective.business_context,
            }
        }), 200

    except Exception as e:
        db.session.rollback()
        return jsonify({"error": "Server error", "details": str(e)}), 500