from flask import Blueprint, jsonify, request
import jwt

from config import Config
from app.utils.token_utils import get_effective_user_id_from_token
from app.utils.live_bi_utils import (
    generate_inventory_alerts_for_all_skus,
    compute_inventory_coverage_ratio,
)

notification_bp = Blueprint("notification_bp", __name__)
SECRET_KEY = Config.SECRET_KEY


@notification_bp.route("/notification", methods=["GET", "POST"])
def notification():
    try:
        # 1) Authorization
        auth_header = request.headers.get("Authorization")
        if not auth_header or not auth_header.startswith("Bearer "):
            return jsonify({
                "success": False,
                "error": "Authorization token is missing or invalid"
            }), 401

        token = auth_header.split(" ")[1]

        try:
            payload, user_id, member_id = get_effective_user_id_from_token(token)
            user_id = payload["user_id"]
        except jwt.ExpiredSignatureError:
            return jsonify({
                "success": False,
                "error": "Token has expired"
            }), 401
        except jwt.InvalidTokenError:
            return jsonify({
                "success": False,
                "error": "Invalid token"
            }), 401

        # 2) Get country from request
        data = request.get_json(silent=True) or {}
        country = (
            data.get("country")
            or request.args.get("country")
            or request.form.get("country")
            or "uk"
        ).strip().lower()

        # 3) Generate alerts
        alerts = generate_inventory_alerts_for_all_skus(user_id, country)

        # 4) Get inventory coverage ratio
        coverage_df = compute_inventory_coverage_ratio(user_id, country)

        coverage_map = {
            str(row["sku"]).strip(): {
                "inventory_coverage_ratio": row["inventory_coverage_ratio"],
                "product_name": row.get("product_name")
            }
            for _, row in coverage_df.iterrows()
            if row.get("sku") is not None
        }

        # 5) Merge ratio into alerts response
        final_data = {}
        for sku, alert_info in alerts.items():
            sku_data = coverage_map.get(sku, {})
            product_name = sku_data.get("product_name") or sku

            final_data[product_name] = {
                "sku": sku,
                "product_name": product_name,
                "inventory_coverage_ratio": sku_data.get("inventory_coverage_ratio"),
                "alert": alert_info.get("alert"),
                "alert_type": alert_info.get("alert_type"),
            }

        return jsonify({
            "success": True,
            "message": "Notifications fetched successfully",
            "user_id": user_id,
            "country": country,
            "data": final_data
        }), 200

    except Exception as e:
        print(f"[ERROR] /notification failed: {e}")
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

        