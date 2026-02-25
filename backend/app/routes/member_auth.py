from flask import Blueprint, request, jsonify, current_app
from werkzeug.security import check_password_hash
from datetime import datetime, timedelta
import jwt

from app.models.user_models import Member

member_auth_bp = Blueprint("member_auth", __name__)

@member_auth_bp.route("/member_login", methods=["POST"])
def member_login():
    try:
        data = request.get_json(silent=True) or {}

        email = (data.get("email") or "").strip().lower()
        password = data.get("password") or ""

        if not email or not password:
            return jsonify({"message": "Missing email or password"}), 400

        # ✅ Find member
        member = Member.query.filter_by(email=email).first()
        if not member:
            return jsonify({"message": "Invalid email or password"}), 401

        # ✅ Password check
        if not check_password_hash(member.password, password):
            return jsonify({"message": "Invalid email or password"}), 401

        # ✅ Verified check
        if not member.is_verified:
            return jsonify({"message": "Member not verified"}), 403

        # ✅ Build JWT payload (member token)
        payload = {
            "member_id": member.id,
            "owner_user_id": member.owner_user_id,
            "email": member.email,
            "is_member": True,

            # ✅ NEW: role
            "role": getattr(member, "role", None),

            # permissions
            "modules": member.modules or [],
            "marketplaces": member.marketplace_ids or [],
            "countries": member.countries or [],

            "exp": datetime.utcnow() + timedelta(hours=24),
        }

        token = jwt.encode(payload, current_app.config["SECRET_KEY"], algorithm="HS256")

        return jsonify(
            {
                "message": "Member login successful",
                "token": token,
                "is_member": True,
                "member_id": member.id,
                "owner_user_id": member.owner_user_id,

                # ✅ NEW: role
                "role": getattr(member, "role", None),

                "modules": member.modules or [],
                "marketplaces": member.marketplace_ids or [],
                "countries": member.countries or [],
            }
        ), 200

    except Exception as e:
        return jsonify({"message": f"Login failed: {str(e)}"}), 500
    
    