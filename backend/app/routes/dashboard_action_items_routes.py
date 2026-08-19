import jwt
from flask import Blueprint, jsonify, request

from app.utils.dashboard_action_items_utils import get_dashboard_action_items
from app.utils.token_utils import get_effective_user_id_from_token


dashboard_action_items_bp = Blueprint("dashboard_action_items", __name__)


@dashboard_action_items_bp.route("/dashboard/action-items", methods=["GET", "OPTIONS"])
def dashboard_action_items():
    if request.method == "OPTIONS":
        return jsonify({"message": "CORS Preflight OK"}), 200

    auth_header = request.headers.get("Authorization")
    if not auth_header or not auth_header.startswith("Bearer "):
        return jsonify({"success": False, "error": "Authorization token is missing or invalid"}), 401

    try:
        payload, _effective_user_id, _member_id = get_effective_user_id_from_token(
            auth_header.split(" ", 1)[1]
        )
        user_id = payload.get("user_id")
        if not user_id:
            return jsonify({"success": False, "error": "Invalid token payload: user_id missing"}), 401
    except jwt.ExpiredSignatureError:
        return jsonify({"success": False, "error": "Token has expired"}), 401
    except jwt.InvalidTokenError:
        return jsonify({"success": False, "error": "Invalid token"}), 401

    country = str(request.args.get("country_key") or "").strip().lower()
    month = str(request.args.get("month_name") or "").strip().lower()
    year_raw = str(request.args.get("year") or "").strip()
    start_day_raw = str(request.args.get("start_day") or "").strip()
    end_day_raw = str(request.args.get("end_day") or "").strip()

    if not country or not month or not year_raw:
        return jsonify({
            "success": False,
            "message": "Missing required params: country_key, month_name, year",
        }), 400

    try:
        year = int(year_raw)
        if year < 2000 or year > 2100:
            raise ValueError("year must be between 2000 and 2100")
        start_day = int(start_day_raw) if start_day_raw else None
        end_day = int(end_day_raw) if end_day_raw else None
        if start_day is not None and not 1 <= start_day <= 31:
            raise ValueError("start_day must be between 1 and 31")
        if end_day is not None and not 1 <= end_day <= 31:
            raise ValueError("end_day must be between 1 and 31")
        result = get_dashboard_action_items(
            user_id=user_id,
            country=country,
            month=month,
            year=year,
            start_day=start_day,
            end_day=end_day,
        )
        return jsonify(result), 200
    except ValueError as exc:
        return jsonify({"success": False, "message": str(exc)}), 400
    except RuntimeError as exc:
        return jsonify({"success": False, "message": str(exc)}), 503
    except Exception as exc:
        return jsonify({
            "success": False,
            "message": "Failed to build dashboard action items",
            "error": str(exc),
        }), 500
