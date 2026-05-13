from flask import Blueprint, request, jsonify, current_app
from werkzeug.security import generate_password_hash
from flask_mail import Message
from sqlalchemy.exc import IntegrityError
from datetime import datetime
import jwt, secrets, string
from app import db, mail
from app.models.user_models import Member, User, UserAdmin, SuperAdmin
from sqlalchemy import func

add_member_bp = Blueprint("add_member", __name__)

# Marketplace -> Country mapping
ALLOWED_MARKETPLACES = {
    "ATVPDKIKX0DER": "US",
    "A1F83G8C2ARO7P": "UK",
    "A2EUQ1WTGCTBG2": "CA",
    "A1PA6795UKMFR9": "DE",
}

# Modules you allow for members (UI sections)
ALLOWED_MODULES = {
    "LIVE_DASHBOARD",
    "FINANCE_DASHBOARDS",
    "BUSINESS_INTELLIGENCE",
    "INVENTORY_PLANNING",
}

DEFAULT_MODULES = ["LIVE_DASHBOARD"]

# Roles you allow for members
ALLOWED_ROLES = {"MARKETING", "ACCOUNTANT", "INVENTORY"}
DEFAULT_ROLE = "MARKETING"


# ==========================================================
# Helpers
# ==========================================================

def _random_token(n: int = 10) -> str:
    return "".join(secrets.choice(string.ascii_letters + string.digits) for _ in range(n))


def _generate_temp_password(length: int = 10) -> str:
    """
    Generate a temporary password with at least:
    - 1 uppercase
    - 1 lowercase
    - 1 digit
    """
    if length < 6:
        length = 6

    chars = string.ascii_letters + string.digits

    password = [
        secrets.choice(string.ascii_uppercase),
        secrets.choice(string.ascii_lowercase),
        secrets.choice(string.digits),
    ]

    password += [secrets.choice(chars) for _ in range(length - 3)]
    secrets.SystemRandom().shuffle(password)
    return "".join(password)


def _normalize_list(val):
    """
    Supports:
      - ["a","b"]
      - "a,b"
      - None
    """
    if val is None:
        return []
    if isinstance(val, list):
        return [str(x).strip() for x in val if str(x).strip()]
    if isinstance(val, str):
        return [x.strip() for x in val.split(",") if x.strip()]
    return []


def _normalize_role(val):
    return (val or "").strip().upper()


def _error(message: str, status: int = 400, **extra):
    payload = {"error": message}
    if extra:
        payload.update(extra)
    return jsonify(payload), status


def _get_owner_user_id_from_token():
    """
    Expects OWNER user's token in Authorization: Bearer <token>
    Token must contain user_id.
    Blocks member tokens (is_member == True).
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

        if payload.get("is_member") is True:
            return None, _error("Members are not allowed to add other members", 403)

        user_id = payload.get("user_id")
        if not user_id:
            return None, _error("Invalid token: missing user_id", 401)

        return int(user_id), None

    except jwt.ExpiredSignatureError:
        return None, _error("Token expired", 401)
    except jwt.InvalidTokenError:
        return None, _error("Invalid token", 401)


def _derive_countries_from_marketplaces(marketplaces):
    countries = []
    for mp in marketplaces:
        c = ALLOWED_MARKETPLACES.get(mp)
        if c and c not in countries:
            countries.append(c)
    return countries


def _validate_marketplaces(marketplaces):
    invalid = [mp for mp in marketplaces if mp not in ALLOWED_MARKETPLACES]
    if invalid:
        return False, invalid
    return True, []


def _validate_modules(modules):
    invalid = [m for m in modules if m not in ALLOWED_MODULES]
    if invalid:
        return False, invalid
    return True, []

def _email_exists_globally(email: str):
    """
    Check whether email already exists in any auth/account table:
    user, member, admin, superadmin
    """
    normalized_email = (email or "").strip().lower()

    if not normalized_email:
        return False, None

    if db.session.query(User.id).filter(func.lower(User.email) == normalized_email).first():
        return True, "user"

    if db.session.query(Member.id).filter(func.lower(Member.email) == normalized_email).first():
        return True, "member"

    if db.session.query(UserAdmin.id).filter(func.lower(UserAdmin.email) == normalized_email).first():
        return True, "admin"

    if db.session.query(SuperAdmin.id).filter(func.lower(SuperAdmin.email) == normalized_email).first():
        return True, "superadmin"

    return False, None

# ==========================================================
# Email
# ==========================================================

def send_member_invite_email(member_name, email, password, token_name, countries, marketplaces, modules, role):
    """
    Invite email with role + access summary.
    """
    msg = Message(
        subject="Welcome to Phormula — Your Member Account Access",
        sender=("Phormula Care Team", "care@phormula.io"),
        recipients=[email],
    )

    login_url = "http://localhost:3000/signin"

    countries_str = ", ".join(countries) if countries else "-"
    marketplaces_str = ", ".join(marketplaces) if marketplaces else "-"
    role_str = role or "-"

    module_labels = {
        "LIVE_DASHBOARD": "Live Dashboard",
        "FINANCE_DASHBOARDS": "Finance Dashboards",
        "BUSINESS_INTELLIGENCE": "Business Intelligence",
        "INVENTORY_PLANNING": "Inventory Planning",
    }
    modules_pretty = ", ".join([module_labels.get(m, m) for m in modules]) if modules else "-"

    year = datetime.utcnow().year
    greet = f"Welcome, {member_name}!" if member_name else "Welcome!"

    msg.html = f"""
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Welcome to Phormula</title>
</head>
<body style="margin:0; padding:0; background:#f5f7fb; font-family:Arial, Helvetica, sans-serif;">
  <div style="max-width:640px; margin:0 auto; padding:24px;">
    <div style="background:#ffffff; border-radius:14px; overflow:hidden; box-shadow:0 6px 20px rgba(16,24,40,0.08); border:1px solid #e6eaf2;">
      <div style="padding:22px 24px; background:linear-gradient(135deg, #37455F 0%, #5EA68E 100%);">
        <div style="display:flex; align-items:center; justify-content:space-between; gap:12px;">
          <div style="color:#ffffff;">
            <div style="font-size:18px; font-weight:700; letter-spacing:0.2px;">Phormula</div>
            <div style="font-size:13px; opacity:0.9;">Your member account has been created</div>
          </div>
          <div style="color:#ffffff; font-size:12px; opacity:0.85;">
            {datetime.utcnow().strftime("%b %d, %Y")}
          </div>
        </div>
      </div>

      <div style="padding:24px;">
        <h2 style="margin:0 0 12px; color:#101828; font-size:20px;">{greet}</h2>
        <p style="margin:0 0 14px; color:#475467; font-size:14px; line-height:1.6;">
          An administrator has added you as a <b>Member</b> in Phormula. Below are your login details and the access you’ve been granted.
        </p>

        <div style="background:#f8fafc; border:1px solid #e6eaf2; border-radius:12px; padding:16px; margin:18px 0;">
          <div style="font-size:14px; font-weight:700; color:#101828; margin-bottom:10px;">
            Your Login Credentials
          </div>

          <div style="display:flex; flex-wrap:wrap; gap:10px;">
            <div style="flex:1; min-width:220px; background:#ffffff; border:1px solid #e6eaf2; border-radius:10px; padding:12px;">
              <div style="color:#667085; font-size:12px; margin-bottom:4px;">Email</div>
              <div style="color:#101828; font-size:13px; font-weight:600; word-break:break-all;">{email}</div>
            </div>

            <div style="flex:1; min-width:220px; background:#ffffff; border:1px solid #e6eaf2; border-radius:10px; padding:12px;">
              <div style="color:#667085; font-size:12px; margin-bottom:4px;">Temporary Password</div>
              <div style="color:#101828; font-size:13px; font-weight:600;">{password}</div>
            </div>
          </div>

          <div style="margin-top:10px; color:#667085; font-size:12px; line-height:1.5;">
            <b>Security tip:</b> Please change your password after your first login.
          </div>
        </div>

        <div style="background:#ffffff; border:1px solid #e6eaf2; border-radius:12px; padding:16px; margin:18px 0;">
          <div style="font-size:14px; font-weight:700; color:#101828; margin-bottom:10px;">
            Your Access
          </div>

          <table width="100%" cellpadding="0" cellspacing="0" style="border-collapse:collapse;">
            <tr>
              <td style="padding:10px 0; color:#667085; font-size:12px; width:140px;">Role</td>
              <td style="padding:10px 0; color:#101828; font-size:13px; font-weight:600;">{role_str}</td>
            </tr>
            <tr style="border-top:1px solid #eef2f7;">
              <td style="padding:10px 0; color:#667085; font-size:12px; width:140px;">Countries</td>
              <td style="padding:10px 0; color:#101828; font-size:13px; font-weight:600;">{countries_str}</td>
            </tr>
            <tr style="border-top:1px solid #eef2f7;">
              <td style="padding:10px 0; color:#667085; font-size:12px;">Marketplaces</td>
              <td style="padding:10px 0; color:#101828; font-size:13px; font-weight:600; word-break:break-word;">
                {marketplaces_str}
              </td>
            </tr>
            <tr style="border-top:1px solid #eef2f7;">
              <td style="padding:10px 0; color:#667085; font-size:12px;">Modules</td>
              <td style="padding:10px 0; color:#101828; font-size:13px; font-weight:600;">
                {modules_pretty}
              </td>
            </tr>
          </table>
        </div>

        <div style="text-align:center; margin:22px 0 6px;">
          <a href="{login_url}"
             style="display:inline-block; background:#37455F; color:#F8EDCF; text-decoration:none; padding:12px 22px;
                    border-radius:12px; font-size:14px; font-weight:700; box-shadow:0 6px 14px rgba(55,69,95,0.18);">
            Login to Phormula
          </a>
        </div>

        <div style="text-align:center; color:#667085; font-size:12px; margin-top:10px;">
          If the button doesn’t work, copy and paste this link:<br/>
          <span style="color:#344054; word-break:break-all;">{login_url}</span>
        </div>

        <div style="margin-top:18px; padding-top:14px; border-top:1px solid #eef2f7; color:#667085; font-size:12px; line-height:1.6;">
          If you did not expect this email, please contact support at
          <a href="mailto:care@phormula.io" style="color:#5EA68E; text-decoration:none; font-weight:700;">care@phormula.io</a>.
          <br/>
          Token reference: <span style="color:#344054; font-weight:700;">{token_name}</span>
        </div>
      </div>
    </div>

    <div style="text-align:center; color:#98a2b3; font-size:12px; margin-top:14px;">
      © {year} Phormula. All rights reserved.
    </div>
  </div>
</body>
</html>
    """
    mail.send(msg)


# ==========================================================
# Route
# ==========================================================

@add_member_bp.route("/add_member", methods=["POST"])
def add_member():
    try:
        owner_user_id, auth_err = _get_owner_user_id_from_token()
        if auth_err:
            return auth_err

        data = request.get_json(silent=True) or {}

        member_name = (data.get("member_name") or "").strip()
        email = (data.get("email") or "").strip().lower()
        role = _normalize_role(data.get("role")) or DEFAULT_ROLE
        marketplaces = _normalize_list(data.get("marketplaces"))
        modules = _normalize_list(data.get("modules")) or list(DEFAULT_MODULES)

        # Auto-generate temporary password
        temp_password = _generate_temp_password(10)

        if not member_name:
            return _error("member_name is required", 400)

        if not email or not marketplaces:
            return _error("email and marketplaces are required", 400)

        if "@" not in email or "." not in email:
            return _error("Invalid email", 400)

        # Global email uniqueness check
        exists, existing_in = _email_exists_globally(email)
        if exists:
            return _error(
                f"Email already exists. Please use a different email.",
                409
            )
        if role not in ALLOWED_ROLES:
            return _error(
                "Invalid role",
                400,
                invalid_role=role,
                allowed_roles=sorted(list(ALLOWED_ROLES)),
            )

        ok, invalid_mps = _validate_marketplaces(marketplaces)
        if not ok:
            return _error(
                "Invalid marketplace IDs",
                400,
                invalid_marketplaces=invalid_mps,
                allowed_marketplaces=list(ALLOWED_MARKETPLACES.keys()),
            )

        countries = _derive_countries_from_marketplaces(marketplaces)
        if not countries:
            return _error("Could not derive countries from marketplaces", 400)

        ok, invalid_modules = _validate_modules(modules)
        if not ok:
            return _error(
                "Invalid modules",
                400,
                invalid_modules=invalid_modules,
                allowed_modules=sorted(list(ALLOWED_MODULES)),
            )

        token_name = f"m{owner_user_id}_{'_'.join([c.lower() for c in countries])}_{_random_token(10)}"

        new_member = Member(
            owner_user_id=owner_user_id,
            member_name=member_name,
            email=email,
            password=generate_password_hash(temp_password),
            role=role,
            marketplace_ids=marketplaces,
            countries=countries,
            modules=modules,
            token_name=token_name,
            is_verified=True,
        )

        db.session.add(new_member)
        db.session.commit()

        email_sent = False
        email_message = ""
        try:
            send_member_invite_email(
                member_name,
                email,
                temp_password,
                token_name,
                countries,
                marketplaces,
                modules,
                role,
            )
            email_sent = True
            email_message = "Invitation email sent successfully."
        except Exception as e:
            current_app.logger.exception("Member invite email failed")
            email_message = f"Member created but invite email failed: {str(e)}"

        return jsonify({
            "message": "Member added successfully",
            "member_id": int(new_member.id),
            "owner_user_id": int(owner_user_id),
            "member_name": member_name,
            "email": email,
            "role": role,
            "countries": countries,
            "marketplaces": marketplaces,
            "modules": modules,
            "email_sent": email_sent,
            "email_message": email_message,
        }), 201

    except IntegrityError:
        db.session.rollback()
        return _error("Member already exists for this owner (or token collision). Try again.", 409)

    except Exception as e:
        db.session.rollback()
        return _error(str(e), 500)


@add_member_bp.route("/delete_member", methods=["DELETE"])
def delete_member():
    try:
        owner_user_id, auth_err = _get_owner_user_id_from_token()
        if auth_err:
            return auth_err

        data = request.get_json(silent=True) or {}
        member_id = data.get("member_id")
        email = (data.get("email") or "").strip().lower()

        if not member_id and not email:
            return _error("member_id or email is required", 400)

        q = Member.query.filter_by(owner_user_id=int(owner_user_id))
        if member_id:
            q = q.filter_by(id=int(member_id))
        else:
            q = q.filter_by(email=email)

        member = q.first()
        if not member:
            return _error("Member not found for this owner", 404)

        db.session.delete(member)
        db.session.commit()

        return jsonify({
            "message": "Member deleted successfully",
            "member_id": int(member.id),
            "email": member.email,
            "owner_user_id": int(owner_user_id),
        }), 200

    except Exception as e:
        db.session.rollback()
        return _error(f"Failed to delete member: {str(e)}", 500)


@add_member_bp.route("/update_member_access", methods=["PUT", "PATCH"])
def update_member_access():
    try:
        owner_user_id, auth_err = _get_owner_user_id_from_token()
        if auth_err:
            return auth_err

        data = request.get_json(silent=True) or {}

        member_id = data.get("member_id")
        email = (data.get("email") or "").strip().lower()

        if not member_id and not email:
            return _error("member_id or email is required", 400)

        q = Member.query.filter_by(owner_user_id=int(owner_user_id))
        if member_id:
            q = q.filter_by(id=int(member_id))
        else:
            q = q.filter_by(email=email)

        member = q.first()
        if not member:
            return _error("Member not found for this owner", 404)

        if "marketplace_ids" in data:
            marketplace_ids = _normalize_list(data.get("marketplace_ids"))
            if not marketplace_ids:
                return _error("marketplace_ids cannot be empty", 400)

            ok, invalid_mps = _validate_marketplaces(marketplace_ids)
            if not ok:
                return _error(
                    "Invalid marketplace IDs",
                    400,
                    invalid_marketplaces=invalid_mps,
                    allowed_marketplaces=list(ALLOWED_MARKETPLACES.keys()),
                )

            member.marketplace_ids = marketplace_ids
            member.countries = _derive_countries_from_marketplaces(marketplace_ids)

        if "modules" in data:
            modules = _normalize_list(data.get("modules"))
            if not modules:
                return _error("modules cannot be empty", 400)

            ok, invalid_modules = _validate_modules(modules)
            if not ok:
                return _error(
                    "Invalid modules",
                    400,
                    invalid_modules=invalid_modules,
                    allowed_modules=sorted(list(ALLOWED_MODULES)),
                )

            member.modules = modules

        if "role" in data:
            role = _normalize_role(data.get("role")) or DEFAULT_ROLE
            if role not in ALLOWED_ROLES:
                return _error(
                    "Invalid role",
                    400,
                    invalid_role=role,
                    allowed_roles=sorted(list(ALLOWED_ROLES)),
                )
            member.role = role

        if "is_verified" in data:
            member.is_verified = bool(data.get("is_verified"))

        if hasattr(member, "updated_at"):
            member.updated_at = datetime.utcnow()

        db.session.commit()

        return jsonify({
            "message": "Member access updated successfully",
            "member_id": int(member.id),
            "owner_user_id": int(owner_user_id),
            "member_name": member.member_name,
            "email": member.email,
            "role": member.role,
            "marketplace_ids": member.marketplace_ids,
            "countries": member.countries,
            "modules": member.modules,
            "token_name": member.token_name,
        }), 200

    except Exception as e:
        db.session.rollback()
        return _error(f"Failed to update member access: {str(e)}", 500)
    
    