from flask import Blueprint, request, jsonify, current_app
from werkzeug.security import generate_password_hash
from flask_mail import Message
from sqlalchemy.exc import IntegrityError
from app import db, mail
from app.models.user_models import Member
import jwt
import secrets, string
from datetime import datetime, timedelta

add_member_bp = Blueprint("add_member", __name__)

# ✅ Marketplace -> Country mapping
ALLOWED_MARKETPLACES = {
    "ATVPDKIKX0DER": "US",
    "A1F83G8C2ARO7P": "UK",
    "A2EUQ1WTGCTBG2": "CA",
}

# ✅ Modules you allow for members (UI sections)
ALLOWED_MODULES = {
    "LIVE_DASHBOARD",
    "FINANCE_DASHBOARDS",
    "BUSINESS_INTELLIGENCE",
    "INVENTORY_PLANNING",
}

# Optional: set defaults if frontend doesn't send modules
DEFAULT_MODULES = ["LIVE_DASHBOARD"]

# ==========================================================
# Helpers
# ==========================================================

def _random_token(n: int = 8) -> str:
    return "".join(secrets.choice(string.ascii_letters + string.digits) for _ in range(n))

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

def _error(message: str, status: int = 400, **extra):
    payload = {"error": message}
    if extra:
        payload.update(extra)
    return jsonify(payload), status

def _get_owner_user_id_from_token():
    """
    Expects main user's token in Authorization: Bearer <token>
    Token must contain user_id.
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
        user_id = payload.get("user_id")
        if not user_id:
            return None, _error("Invalid token: missing user_id", 401)
        return int(user_id), None
    except jwt.ExpiredSignatureError:
        return None, _error("Token expired", 401)
    except jwt.InvalidTokenError:
        return None, _error("Invalid token", 401)

def _derive_countries_from_marketplaces(marketplaces):
    # keep insertion order, unique
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



def send_member_invite_email(email, password, token_name, countries, marketplaces, modules):
    """
    Improved HTML invite email:
    - Looks professional
    - Clear access summary (countries / marketplaces / modules)
    - Includes security note
    - Includes token_name for support/debug
    """
    try:
        msg = Message(
            subject="Welcome to Phormula — Your Member Account Access",
            sender=("Phormula Care Team", "care@phormula.io"),
            recipients=[email],
        )

        # ✅ Update to your real frontend URL
        login_url = "http://localhost:3000/member-login"

        countries_str = ", ".join(countries) if countries else "-"
        marketplaces_str = ", ".join(marketplaces) if marketplaces else "-"
        modules_str = ", ".join(modules) if modules else "-"

        # Optional: map module keys -> friendly names
        module_labels = {
            "LIVE_DASHBOARD": "Live Dashboard",
            "FINANCE_DASHBOARDS": "Finance Dashboards",
            "BUSINESS_INTELLIGENCE": "Business Intelligence",
            "INVENTORY_PLANNING": "Inventory Planning",
        }
        modules_pretty = ", ".join([module_labels.get(m, m) for m in modules]) if modules else "-"

        year = datetime.utcnow().year

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
    
    <!-- Header -->
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

      <!-- Body -->
      <div style="padding:24px;">
        <h2 style="margin:0 0 12px; color:#101828; font-size:20px;">Welcome!</h2>
        <p style="margin:0 0 14px; color:#475467; font-size:14px; line-height:1.6;">
          An administrator has added you as a <b>Member</b> in Phormula. Below are your login details and the access you’ve been granted.
        </p>

        <!-- Credentials Card -->
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

        <!-- Access Card -->
        <div style="background:#ffffff; border:1px solid #e6eaf2; border-radius:12px; padding:16px; margin:18px 0;">
          <div style="font-size:14px; font-weight:700; color:#101828; margin-bottom:10px;">
            Your Access
          </div>

          <table width="100%" cellpadding="0" cellspacing="0" style="border-collapse:collapse;">
            <tr>
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

        <!-- CTA Button -->
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

        <!-- Footer Note -->
        <div style="margin-top:18px; padding-top:14px; border-top:1px solid #eef2f7; color:#667085; font-size:12px; line-height:1.6;">
          If you did not expect this email, please contact support at
          <a href="mailto:care@phormula.io" style="color:#5EA68E; text-decoration:none; font-weight:700;">care@phormula.io</a>.
          <br/>
        </div>
      </div>
    </div>

    <!-- Bottom -->
    <div style="text-align:center; color:#98a2b3; font-size:12px; margin-top:14px;">
      © {year} Phormula. All rights reserved.
    </div>

  </div>
</body>
</html>
        """
        mail.send(msg)

    except Exception as e:
        # Re-raise so API can return "email failed" without breaking member creation
        raise e
    

# ==========================================================
# Route
# ==========================================================

@add_member_bp.route("/add_member", methods=["POST"])
def add_member():
    """
    POST /add_member
    Header: Authorization: Bearer <OWNER_USER_JWT>

    Body:
    {
      "email": "analyst@skinelements.com",
      "password": "Test1234",
      "marketplaces": ["ATVPDKIKX0DER","A1F83G8C2ARO7P"],
      "modules": ["LIVE_DASHBOARD","INVENTORY_PLANNING"]
    }
    """
    try:
        owner_user_id, auth_err = _get_owner_user_id_from_token()
        if auth_err:
            return auth_err

        data = request.get_json(silent=True) or {}

        email = (data.get("email") or "").strip().lower()
        password = data.get("password") or ""

        marketplaces = _normalize_list(data.get("marketplaces"))
        modules = _normalize_list(data.get("modules")) or list(DEFAULT_MODULES)

        # ✅ required validation
        if not email or not password or not marketplaces:
            return _error(
                "email, password and marketplaces are required",
                400,
                example={
                    "email": "analyst@skinelements.com",
                    "password": "Test1234",
                    "marketplaces": ["ATVPDKIKX0DER", "A1F83G8C2ARO7P"],
                    "modules": ["LIVE_DASHBOARD", "INVENTORY_PLANNING"],
                },
            )

        if "@" not in email or "." not in email:
            return _error("Invalid email", 400)

        if len(password) < 6:
            return _error("Password must be at least 6 characters", 400)

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

        # ✅ token_name: add owner id to reduce collision chance
        token_name = f"m{owner_user_id}_{'_'.join([c.lower() for c in countries])}_{_random_token(10)}"

        new_member = Member(
            owner_user_id=owner_user_id,
            email=email,
            password=generate_password_hash(password),
            marketplace_ids=marketplaces,
            countries=countries,
            modules=modules,
            token_name=token_name,
            is_verified=True,  # you set true for direct login
        )

        db.session.add(new_member)
        db.session.commit()

        # ✅ email sending should not break creation
        email_sent = False
        email_message = ""
        try:
            send_member_invite_email(email, password, token_name, countries, marketplaces, modules)
            email_sent = True
            email_message = "Invitation email sent successfully."
        except Exception as e:
            email_message = f"Member created but invite email failed: {str(e)}"

        return jsonify(
            {
                "message": "Member added successfully",
                "member_id": new_member.id,
                "owner_user_id": owner_user_id,
                "email": email,
                "countries": countries,
                "marketplaces": marketplaces,
                "modules": modules,
                "token_name": token_name,
                "email_sent": email_sent,
                "email_message": email_message,
            }
        ), 201

    except IntegrityError:
        db.session.rollback()
        # ✅ handles uq_member_owner_email or token_name unique collisions
        return _error("Member already exists for this owner (or token collision). Try again.", 409)

    except Exception as e:
        db.session.rollback()
        return _error(str(e), 500)

