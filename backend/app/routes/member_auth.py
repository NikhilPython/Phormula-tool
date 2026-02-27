import jwt
from flask import Blueprint, request, jsonify, current_app
from werkzeug.security import check_password_hash, generate_password_hash
from datetime import datetime, timedelta
from flask_mail import Message

from app import db, mail
from app.models.user_models import Member

member_auth_bp = Blueprint("member_auth", __name__)

# ==========================================================
# Helpers
# ==========================================================

def _error(message, status=400, **extra):
    payload = {"message": message}
    if extra:
        payload.update(extra)
    return jsonify(payload), status

def _get_member_from_token():
    """
    Member-only endpoints must receive a MEMBER token (is_member == True).
    """
    auth_header = request.headers.get("Authorization", "")
    if not auth_header.startswith("Bearer "):
        return None, _error("Authorization token required", 401)

    token = auth_header.split(" ", 1)[1].strip()
    try:
        payload = jwt.decode(
            token,
            current_app.config["SECRET_KEY"],
            algorithms=["HS256"],
            options={"require": ["exp"]},
        )

        if payload.get("is_member") is not True:
            return None, _error("Member token required", 403)

        member_id = payload.get("member_id")
        if not member_id:
            return None, _error("Invalid token: missing member_id", 401)

        member = Member.query.filter_by(id=int(member_id)).first()
        if not member:
            return None, _error("Member not found", 404)

        return member, None

    except jwt.ExpiredSignatureError:
        return None, _error("Token expired", 401)
    except jwt.InvalidTokenError:
        return None, _error("Invalid token", 401)

def _get_frontend_base_url():
    # set FRONTEND_BASE_URL in config/env for production, fallback to localhost
    return current_app.config.get("FRONTEND_BASE_URL", "http://localhost:3000")

# ==========================================================
# Email (Reset Password)
# ==========================================================

def send_member_reset_email(email, member_name, reset_link):
    msg = Message(
        subject="Reset Your Phormula Password",
        sender=("Phormula Care Team", "care@phormula.io"),
        recipients=[email],
    )

    greet = f"Hi {member_name}," if member_name else "Hi,"
    year = datetime.utcnow().year

    msg.html = f"""
<!DOCTYPE html>
<html>
  <body style="margin:0;padding:0;background:#f5f7fb;font-family:Arial,Helvetica,sans-serif;">
    <div style="max-width:640px;margin:0 auto;padding:24px;">
      <div style="background:#ffffff;border:1px solid #e6eaf2;border-radius:14px;overflow:hidden;box-shadow:0 6px 20px rgba(16,24,40,0.08);">
        <div style="padding:18px 22px;background:linear-gradient(135deg,#37455F 0%,#5EA68E 100%);color:#fff;">
          <div style="font-weight:700;font-size:16px;">Phormula</div>
          <div style="opacity:.9;font-size:12px;margin-top:2px;">Password reset request</div>
        </div>

        <div style="padding:22px;">
          <h2 style="margin:0 0 10px;color:#101828;font-size:20px;">Reset your password</h2>
          <p style="margin:0 0 14px;color:#475467;font-size:14px;line-height:1.6;">
            {greet}<br/>
            We received a request to reset your Phormula password.
          </p>

          <div style="text-align:center;margin:18px 0;">
            <a href="{reset_link}"
               style="display:inline-block;background:#37455F;color:#F8EDCF;text-decoration:none;
                      padding:12px 22px;border-radius:12px;font-size:14px;font-weight:700;">
              Reset Password
            </a>
          </div>

          <p style="margin:0;color:#667085;font-size:12px;line-height:1.6;text-align:center;">
            This link will expire in <b>10 minutes</b>.
          </p>

          <p style="margin:16px 0 0;color:#667085;font-size:12px;line-height:1.6;">
            If the button doesn’t work, copy and paste this link:<br/>
            <span style="color:#344054;word-break:break-all;">{reset_link}</span>
          </p>

          <div style="margin-top:16px;padding-top:14px;border-top:1px solid #eef2f7;color:#667085;font-size:12px;line-height:1.6;">
            If you did not request this, you can safely ignore this email.
          </div>
        </div>
      </div>

      <div style="text-align:center;color:#98a2b3;font-size:12px;margin-top:14px;">
        © {year} Phormula. All rights reserved.
      </div>
    </div>
  </body>
</html>
"""
    mail.send(msg)

# ==========================================================
# === Member LOGIN ===
# ==========================================================

@member_auth_bp.route("/member_login", methods=["POST"])
def member_login():
    try:
        data = request.get_json(silent=True) or {}

        email = (data.get("email") or "").strip().lower()
        password = data.get("password") or ""

        if not email or not password:
            return jsonify({"message": "Missing email or password"}), 400

        member = Member.query.filter_by(email=email).first()
        if not member:
            return jsonify({"message": "Invalid email or password"}), 401

        if not check_password_hash(member.password, password):
            return jsonify({"message": "Invalid email or password"}), 401

        if not member.is_verified:
            return jsonify({"message": "Member not verified"}), 403

        owner_user_id = int(member.owner_user_id)

        payload = {
            "user_id": owner_user_id,
            "owner_user_id": owner_user_id,
            "member_id": int(member.id),
            "is_member": True,

            "email": member.email,
            "member_name": getattr(member, "member_name", None),
            "member_role": getattr(member, "role", None),

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

            "user_id": owner_user_id,
            "owner_user_id": owner_user_id,
            "member_id": int(member.id),

            "email": member.email,
            "member_name": getattr(member, "member_name", None),

            "role": getattr(member, "role", None),
            "modules": member.modules or [],
            "marketplaces": member.marketplace_ids or [],
            "countries": member.countries or [],
        }), 200

    except Exception as e:
        return jsonify({"message": f"Login failed: {str(e)}"}), 500

# ==========================================================
# === Member PASSWORD RESET - REQUEST (NO DB OTP) ===
# Requires column: member.password_changed_at (datetime)
# ==========================================================

@member_auth_bp.route("/member_password_reset_request", methods=["POST"])
def member_password_reset_request():
    """
    Body:
    { "email": "analyst@skinelements.com" }

    Creates a short-lived reset token (JWT) and emails a reset link.
    No OTP stored in DB.
    """
    try:
        data = request.get_json(silent=True) or {}
        email = (data.get("email") or "").strip().lower()

        if not email:
            return _error("Email is required", 400)

        member = Member.query.filter_by(email=email).first()

        # ✅ Security: do not reveal existence
        if not member or not member.is_verified:
            return jsonify({"message": "If that email exists, a reset link has been sent."}), 200

        issued_at = datetime.utcnow()
        reset_payload = {
            "purpose": "member_password_reset",
            "member_id": int(member.id),
            "iat": int(issued_at.timestamp()),
            "exp": int((issued_at + timedelta(minutes=10)).timestamp()),
        }

        reset_token = jwt.encode(
            reset_payload,
            current_app.config["SECRET_KEY"],
            algorithm="HS256",
        )

        reset_link = f"{_get_frontend_base_url()}/member-reset-password?token={reset_token}"

        send_member_reset_email(
            email=member.email,
            member_name=getattr(member, "member_name", None),
            reset_link=reset_link,
        )

        return jsonify({"message": "If that email exists, a reset link has been sent."}), 200

    except Exception as e:
        return _error(f"Failed to request password reset: {str(e)}", 500)

# ==========================================================
# === Member PASSWORD RESET - CONFIRM (VERIFY TOKEN) ===
# Requires column: member.password_changed_at (datetime)
# ==========================================================

@member_auth_bp.route("/member_password_reset_confirm", methods=["POST"])
def member_password_reset_confirm():
    """
    Body:
    {
      "reset_token": "<token from email link>",
      "new_password": "NewPass123"
    }
    """
    try:
        data = request.get_json(silent=True) or {}
        reset_token = data.get("reset_token")
        new_password = data.get("new_password") or ""

        if not reset_token or not new_password:
            return _error("reset_token and new_password are required", 400)

        if len(new_password) < 6:
            return _error("Password must be at least 6 characters", 400)

        try:
            payload = jwt.decode(
                reset_token,
                current_app.config["SECRET_KEY"],
                algorithms=["HS256"],
                options={"require": ["exp", "iat"]},
            )
        except jwt.ExpiredSignatureError:
            return _error("Reset link expired", 401)
        except jwt.InvalidTokenError:
            return _error("Invalid reset token", 401)

        if payload.get("purpose") != "member_password_reset":
            return _error("Invalid reset token purpose", 400)

        member_id = payload.get("member_id")
        iat = payload.get("iat")

        if not member_id or not iat:
            return _error("Invalid reset token", 400)

        issued_at = datetime.utcfromtimestamp(int(iat))

        member = Member.query.filter_by(id=int(member_id)).first()
        if not member:
            return _error("Member not found", 404)

        # ✅ Single-use protection:
        # If password was changed after token issued, token is invalid.
        if getattr(member, "password_changed_at", None) and member.password_changed_at > issued_at:
            return _error("This reset link has already been used.", 400)

        member.password = generate_password_hash(new_password)
        member.password_changed_at = datetime.utcnow()

        if hasattr(member, "updated_at"):
            member.updated_at = datetime.utcnow()

        db.session.commit()

        return jsonify({"message": "Password reset successful"}), 200

    except Exception as e:
        db.session.rollback()
        return _error(f"Failed to reset password: {str(e)}", 500)

# ==========================================================
# === Member CHANGE PASSWORD (for logged-in) ===
# Requires column: member.password_changed_at (datetime)
# ==========================================================

@member_auth_bp.route("/member_change_password", methods=["POST"])
def member_change_password():
    """
    Header: Authorization: Bearer <MEMBER_TOKEN>
    Body:
    {
      "old_password": "OldPass123",
      "new_password": "NewPass123"
    }
    """
    try:
        member, auth_err = _get_member_from_token()
        if auth_err:
            return auth_err

        data = request.get_json(silent=True) or {}
        old_password = data.get("old_password") or ""
        new_password = data.get("new_password") or ""

        if not old_password or not new_password:
            return _error("old_password and new_password are required", 400)

        if len(new_password) < 6:
            return _error("Password must be at least 6 characters", 400)

        if not check_password_hash(member.password, old_password):
            return _error("Old password is incorrect", 401)

        member.password = generate_password_hash(new_password)
        member.password_changed_at = datetime.utcnow()

        if hasattr(member, "updated_at"):
            member.updated_at = datetime.utcnow()

        db.session.commit()

        return jsonify({"message": "Password changed successfully"}), 200

    except Exception as e:
        db.session.rollback()
        return _error(f"Failed to change password: {str(e)}", 500)

# ==========================================================
# === Member LOGOUT ===
# ==========================================================

@member_auth_bp.route("/member_logout", methods=["POST"])
def member_logout():
    """
    JWT logout is frontend-based (remove token).
    Server-side logout requires token revocation (blacklist/session_version).
    """
    return jsonify({"message": "Logged out successfully"}), 200

