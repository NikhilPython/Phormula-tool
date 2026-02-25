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

        # ✅ Find member (email should be unique per owner; but login uses email only)
        member = Member.query.filter_by(email=email).first()
        if not member:
            return jsonify({"message": "Invalid email or password"}), 401

        if not check_password_hash(member.password, password):
            return jsonify({"message": "Invalid email or password"}), 401

        if not member.is_verified:
            return jsonify({"message": "Member not verified"}), 403

        owner_user_id = int(member.owner_user_id)

        # ✅ IMPORTANT:
        # user_id MUST always be the OWNER id because all your data tables use user_id = owner
        payload = {
            "user_id": owner_user_id,          # ✅ ADD THIS (owner scope)
            "owner_user_id": owner_user_id,    # ✅ keep (optional, but fine)
            "member_id": int(member.id),       # ✅ member identity
            "is_member": True,

            "email": member.email,
            "member_name": getattr(member, "member_name", None),  # ✅ NEW

            # ✅ Member role (their job role)
            "member_role": getattr(member, "role", None),

            # permissions
            "modules": member.modules or [],
            "marketplaces": member.marketplace_ids or [],
            "countries": member.countries or [],

            "exp": datetime.utcnow() + timedelta(hours=24),
        }

        token = jwt.encode(payload, current_app.config["SECRET_KEY"], algorithm="HS256")

        return jsonify({
            "message": "Member login successful",
            "token": token,
            "is_member": True,

            # ✅ FE-friendly fields
            "user_id": owner_user_id,  # ✅ ADD THIS so FE can treat same as owner
            "owner_user_id": owner_user_id,
            "member_id": int(member.id),

            "email": member.email,
            "member_name": getattr(member, "member_name", None),  # ✅ NEW

            "role": getattr(member, "role", None),
            "modules": member.modules or [],
            "marketplaces": member.marketplace_ids or [],
            "countries": member.countries or [],
        }), 200

    except Exception as e:
        return jsonify({"message": f"Login failed: {str(e)}"}), 500
    
    