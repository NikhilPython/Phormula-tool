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

def _normalize_country_access(val):
    """
    Expected:
    {
      "UK": ["LIVE_DASHBOARD"],
      "US": ["FINANCE_DASHBOARDS"]
    }
    """
    if not isinstance(val, dict):
        return {}

    normalized = {}

    for country, modules in val.items():
        country_key = str(country).strip().upper()
        module_list = _normalize_list(modules)

        if not country_key or not module_list:
            continue

        normalized[country_key] = module_list

    return normalized


def _validate_country_access(country_access):
    invalid_countries = []
    invalid_modules = []

    allowed_countries = set(ALLOWED_MARKETPLACES.values())

    for country, modules in country_access.items():
        if country not in allowed_countries:
            invalid_countries.append(country)

        for module in modules:
            if module not in ALLOWED_MODULES:
                invalid_modules.append(module)

    return invalid_countries, invalid_modules


def _flatten_modules_from_country_access(country_access):
    return sorted({
        module
        for modules in country_access.values()
        for module in modules
    })

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

    login_url = "http://www.phormula.io/signin"

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
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">

  <style>
    @media only screen and (max-width: 600px) {{
      .email-container {{
        width: 100% !important;
        max-width: 100% !important;
      }}

      .top-report-title {{
        font-size: 14px !important;
        line-height: 18px !important;
      }}

      .content-cell {{
        padding: 22px 24px 26px 24px !important;
      }}

      .note-cell {{
        padding: 14px 24px 16px 24px !important;
      }}

      .cta-wrap {{
        text-align: center !important;
      }}

      .cta-button {{
        display: inline-block !important;
        margin: 0 auto !important;
        text-align: center !important;
      }}

      .cred-col {{
        display: block !important;
        width: 100% !important;
        box-sizing: border-box !important;
      }}

      .cred-spacer {{
        display: none !important;
      }}
    }}
  </style>
</head>

<body style="margin:0; padding:0; font-family:Arial, Helvetica, sans-serif;">
  <table width="100%" cellpadding="0" cellspacing="0" border="0" style="padding:16px 0;">
    <tr>
      <td align="center">

        <table class="email-container" width="600" cellpadding="0" cellspacing="0" border="0" style="
          background:#ffffff;
          width:600px;
          max-width:600px;
          border-collapse:collapse;
        ">

          <!-- top green bar -->
          <tr>
            <td style="background:#5ea68e; padding:18px 24px; color:#ffffff;">
              <table width="100%" cellpadding="0" cellspacing="0" border="0" style="table-layout:fixed; border-collapse:collapse;">
                <tr>
                  <td width="110" style="text-align:left; vertical-align:middle; white-space:nowrap;">
                    <img
                      src="https://res.cloudinary.com/du58s6gdz/image/upload/f_auto,q_auto/output-onlinepngtools_ypplvv"
                      alt="Phormula"
                      width="40"
                      style="display:block; width:40px; max-width:40px; height:auto; border:0;"
                    />
                  </td>
                  <td width="382" align="right" class="top-report-title" style="
                    font-size:16px;
                    line-height:18px;
                    color:#f8edce;
                    text-align:right;
                    vertical-align:middle;
                    white-space:nowrap;
                  ">
                    Member Account Access
                  </td>
                </tr>
              </table>
            </td>
          </tr>

          <!-- logo/title -->
          <tr>
            <td align="center" style="
              padding:28px 30px 18px 30px;
              background:#ffffff;
              border-left:1px solid #e4e7ec;
              border-right:1px solid #e4e7ec;
            ">
              <img
                src="https://res.cloudinary.com/du58s6gdz/image/upload/f_auto,q_auto/Logo_Phormula_pmbp8q"
                alt="Phormula Logo"
                width="220"
                style="display:block; width:220px; max-width:220px; height:auto; margin:0 auto 14px auto; border:0;"
              />
              <div style="font-size:18px; color:#4a4a4a; line-height:1.4;">
                Your member account has been created
              </div>
            </td>
          </tr>

          <!-- divider -->
          <tr>
            <td style="border-top:1px solid #dddddd; font-size:1px; line-height:1px;">&nbsp;</td>
          </tr>

          <!-- body -->
          <tr>
            <td class="content-cell" style="
              padding:22px 32px 26px 32px;
              color:#444444;
              font-size:14px;
              line-height:1.7;
              text-align:left;
              border-left:1px solid #e4e7ec;
              border-right:1px solid #e4e7ec;
            ">
              <p style="margin:0 0 18px 0; text-align:left;">
                <strong>{greet}</strong>
              </p>

              <p style="margin:0 0 14px 0; text-align:justify; text-justify:inter-word;">
                An administrator has added you as a <strong>Member</strong> in Phormula.
                Below are your login credentials and the access permissions granted to your account.
              </p>

              <!-- ============================================================
                   ACCOUNT DETAILS CARD
              ============================================================ -->
              <table width="100%" cellpadding="0" cellspacing="0" border="0" style="
                margin:22px 0;
                border-collapse:separate;
                border-spacing:0;
                border:1px solid #d9d9d9;
                border-radius:8px;
                background:#F8FBFA;
              ">

                <!-- Credentials section -->
                <tr>
                  <td style="background:#F8FBFA; padding:20px 22px 18px 22px; border-radius:8px 8px 0 0;">

                    <div style="font-size:11px; letter-spacing:0.08em; text-transform:uppercase; color:#999999; margin-bottom:14px; font-family:Arial, Helvetica, sans-serif;">
                      Login Credentials
                    </div>

                    <!-- Two-column credential boxes -->
                    <table width="100%" cellpadding="0" cellspacing="0" border="0" style="border-collapse:collapse;">
                      <tr>
                        <td class="cred-col" width="48%" style="
                          background:#ffffff;
                          border:1px solid #dddddd;
                          border-radius:6px;
                          padding:14px 16px;
                          vertical-align:top;
                        ">
                          <div style="font-size:11px; letter-spacing:0.06em; text-transform:uppercase; color:#999999; margin-bottom:6px; font-family:Arial, Helvetica, sans-serif;">
                            Email address
                          </div>
                          <div style="font-size:14px; font-weight:bold; color:#37455f; word-break:break-all; font-family:Arial, Helvetica, sans-serif;">
                            {email}
                          </div>
                        </td>

                        <td class="cred-spacer" width="4%" style="font-size:1px;">&nbsp;</td>

                        <td class="cred-col" width="48%" style="
                          background:#ffffff;
                          border:1px solid #dddddd;
                          border-radius:6px;
                          padding:14px 16px;
                          vertical-align:top;
                        ">
                          <div style="font-size:11px; letter-spacing:0.06em; text-transform:uppercase; color:#999999; margin-bottom:6px; font-family:Arial, Helvetica, sans-serif;">
                            Temporary password
                          </div>
                          <div style="font-size:14px; font-weight:bold; color:#444444; letter-spacing:0.04em; font-family:'Courier New', Courier, monospace;">
                            {password}
                          </div>
                        </td>
                      </tr>
                    </table>

                    <!-- Security tip -->
                    <table width="100%" cellpadding="0" cellspacing="0" border="0" style="border-collapse:collapse; margin-top:12px;">
                    <tr>
                        <td style="
                        background:#fff8e8;
                        border:1px solid #f0d891;
                        padding:12px 16px;
                        ">
                        <span style="font-size:13px; color:#8a5a00; line-height:1.6; font-family:Arial, Helvetica, sans-serif;">
                            <strong>Security tip:</strong> Please change your password after your first login.
                        </span>
                        </td>
                    </tr>
                    </table>

                  </td>
                </tr>

                <!-- Section divider -->
                <tr>
                  <td style="background:#ffffff; padding:0 22px;">
                    <div style="border-top:1px solid #dddddd; font-size:1px; line-height:1px;">&nbsp;</div>
                  </td>
                </tr>

                <!-- Access Summary section -->
                <tr>
                  <td style="background:#F8FBFA; padding:18px 22px 22px 22px; border-radius:0 0 8px 8px;">

                    <div style="font-size:11px; letter-spacing:0.08em; text-transform:uppercase; color:#999999; margin-bottom:12px; font-family:Arial, Helvetica, sans-serif;">
                      Access Summary
                    </div>

                    <table width="100%" cellpadding="0" cellspacing="0" border="0" style="border-collapse:separate; border-spacing:0 4px;">

                      <!-- Role -->
                      <tr>
                        <td style="background:#ffffff; border:1px solid #dddddd; border-radius:6px; padding:10px 14px;">
                          <table width="100%" cellpadding="0" cellspacing="0" border="0" style="border-collapse:collapse;">
                            <tr>
                              <td style="font-size:13px; color:#444444; font-family:Arial, Helvetica, sans-serif; vertical-align:middle;">
                                Role
                              </td>
                              <td align="right" style="vertical-align:middle;">
                                <span style="
                                  font-size:12px; font-weight:bold;
                                  color:#444444; background:#f1f1f1;
                                  padding:3px 10px; border-radius:20px;
                                  font-family:Arial, Helvetica, sans-serif;
                                ">
                                  {role_str}
                                </span>
                              </td>
                            </tr>
                          </table>
                        </td>
                      </tr>

                      <!-- Countries -->
                      <tr>
                        <td style="background:#ffffff; border:1px solid #dddddd; border-radius:6px; padding:10px 14px;">
                          <table width="100%" cellpadding="0" cellspacing="0" border="0" style="border-collapse:collapse;">
                            <tr>
                              <td style="font-size:13px; color:#444444; font-family:Arial, Helvetica, sans-serif; vertical-align:middle;">
                                Countries
                              </td>
                              <td align="right" style="vertical-align:middle;">
                                <span style="
                                  font-size:12px; font-weight:bold;
                                  color:#444444; background:#f1f1f1;
                                  padding:3px 10px; border-radius:20px;
                                  font-family:Arial, Helvetica, sans-serif;
                                ">
                                  {countries_str}
                                </span>
                              </td>
                            </tr>
                          </table>
                        </td>
                      </tr>

                      <!-- Marketplaces -->
                      <tr>
                        <td style="background:#ffffff; border:1px solid #dddddd; border-radius:6px; padding:10px 14px;">
                          <table width="100%" cellpadding="0" cellspacing="0" border="0" style="border-collapse:collapse;">
                            <tr>
                              <td style="font-size:13px; color:#444444; font-family:Arial, Helvetica, sans-serif; vertical-align:middle;">
                                Marketplaces
                              </td>
                              <td align="right" style="vertical-align:middle;">
                                <span style="
                                  font-size:12px; font-weight:500;
                                  color:#444444; background:#f1f1f1;
                                  padding:3px 10px; border-radius:20px;
                                  font-family:'Courier New', Courier, monospace;
                                  letter-spacing:0.04em;
                                ">
                                  {marketplaces_str}
                                </span>
                              </td>
                            </tr>
                          </table>
                        </td>
                      </tr>

                      <!-- Modules -->
                      <tr>
                        <td style="background:#ffffff; border:1px solid #dddddd; border-radius:6px; padding:10px 14px;">
                          <table width="100%" cellpadding="0" cellspacing="0" border="0" style="border-collapse:collapse;">
                            <tr>
                              <td style="font-size:13px; color:#444444; font-family:Arial, Helvetica, sans-serif; vertical-align:middle;">
                                Modules
                              </td>
                              <td align="right" style="vertical-align:middle;">
                                <span style="
                                  font-size:12px; font-weight:bold;
                                  color:#444444; background:#f1f1f1;
                                  padding:3px 10px; border-radius:20px;
                                  font-family:Arial, Helvetica, sans-serif;
                                ">
                                  {modules_pretty}
                                </span>
                              </td>
                            </tr>
                          </table>
                        </td>
                      </tr>

                    </table>
                  </td>
                </tr>

              </table>
              <!-- ============================================================
                   END ACCOUNT DETAILS CARD
              ============================================================ -->

              <p style="margin:0 0 14px 0; text-align:justify; text-justify:inter-word;">
                You can now sign in to Phormula using the credentials above. After logging in,
                please update your temporary password to keep your account secure.
              </p>

              <!-- CTA -->
              <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="
                width:100%;
                margin:26px 0 24px 0;
                border-collapse:collapse;
              ">
                <tr>
                  <td align="center" class="cta-wrap" style="text-align:center !important; padding:0; margin:0;">
                    <a href="{login_url}" class="cta-button" style="
                      display:inline-block;
                      background:#37455f;
                      color:#f8edce;
                      padding:12px 30px;
                      text-decoration:none;
                      font-size:14px;
                      font-weight:bold;
                      border-radius:10px;
                      text-align:center;
                      line-height:20px;
                      margin:0 auto;
                    ">
                      Login to Phormula
                    </a>
                  </td>
                </tr>
              </table>

              <p style="margin:18px 0 0 0; text-align:left;">
                Warm regards,
              </p>

              <p style="margin:0; text-align:left;">
                <strong>The Phormula Team</strong>
              </p>

              <p style="margin:0; text-align:left;">
                <a href="mailto:care@phormula.io" style="color:#37455f; text-decoration:none;">
                  care@phormula.io
                </a>
              </p>
            </td>
          </tr>

          <!-- full-width note section -->
          <tr>
            <td class="note-cell" style="
              border-top:1px solid #dddddd;
              padding:14px 32px 16px 32px;
              background:#ffffff;
              font-size:12px;
              color:#999999;
              line-height:1.6;
              text-align:left;
              border-left:1px solid #e4e7ec;
              border-right:1px solid #e4e7ec;
            ">
              This email was generated automatically by Phormula.
            </td>
          </tr>

          <!-- footer -->
          <tr>
            <td align="center" style="
              background:#5ea68e;
              padding:12px 18px;
              color:#f8edce;
              font-size:12px;
              line-height:1.5;
              text-align:center;
            ">
              &copy; {year} Phormula. All rights reserved.
            </td>
          </tr>

        </table>

      </td>
    </tr>
  </table>
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
        country_access = _normalize_country_access(data.get("country_access"))
        marketplaces = _normalize_list(data.get("marketplaces"))

        # Auto-generate temporary password
        temp_password = _generate_temp_password(10)

        if not member_name:
            return _error("member_name is required", 400)

        if not email:
            return _error("email is required", 400)

        if not country_access:
            return _error("country_access is required", 400)

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

        invalid_countries, invalid_modules = _validate_country_access(country_access)

        if invalid_countries:
            return _error(
                "Invalid countries",
                400,
                invalid_countries=invalid_countries,
                allowed_countries=sorted(list(set(ALLOWED_MARKETPLACES.values()))),
            )

        if invalid_modules:
            return _error(
                "Invalid modules",
                400,
                invalid_modules=invalid_modules,
                allowed_modules=sorted(list(ALLOWED_MODULES)),
            )

        countries = list(country_access.keys())
        modules = _flatten_modules_from_country_access(country_access)

        token_name = f"m{owner_user_id}_{'_'.join([c.lower() for c in countries])}_{_random_token(10)}"

        new_member = Member(
            owner_user_id=owner_user_id,
            member_name=member_name,
            email=email,
            password=generate_password_hash(temp_password),
            role=role,

            # keep old fields for display/backward compatibility
            marketplace_ids=marketplaces,
            countries=countries,
            modules=modules,

            # new actual permission source
            country_access=country_access,

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
            "country_access": country_access,
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

        # ----------------------------------------------------------
        # Role update
        # ----------------------------------------------------------
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

        # ----------------------------------------------------------
        # Verification update
        # ----------------------------------------------------------
        if "is_verified" in data:
            member.is_verified = bool(data.get("is_verified"))

        # ----------------------------------------------------------
        # Main source of truth: country_access
        # ----------------------------------------------------------
        if "country_access" in data:
            country_access = _normalize_country_access(data.get("country_access"))

            if not country_access:
                return _error("country_access cannot be empty", 400)

            invalid_countries, invalid_modules = _validate_country_access(country_access)

            if invalid_countries:
                return _error(
                    "Invalid countries",
                    400,
                    invalid_countries=invalid_countries,
                    allowed_countries=sorted(list(set(ALLOWED_MARKETPLACES.values()))),
                )

            if invalid_modules:
                return _error(
                    "Invalid modules",
                    400,
                    invalid_modules=invalid_modules,
                    allowed_modules=sorted(list(ALLOWED_MODULES)),
                )

            countries = list(country_access.keys())
            modules = _flatten_modules_from_country_access(country_access)

            country_to_marketplace = {
                country: marketplace
                for marketplace, country in ALLOWED_MARKETPLACES.items()
            }

            marketplace_ids = [
                country_to_marketplace[country]
                for country in countries
                if country in country_to_marketplace
            ]

            member.country_access = country_access
            member.countries = countries
            member.modules = modules
            member.marketplace_ids = marketplace_ids

        # ----------------------------------------------------------
        # Backward compatibility only:
        # If old frontend sends marketplace_ids/modules without
        # country_access, still allow it.
        # ----------------------------------------------------------
        else:
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

                # If old frontend is used, build country_access from countries + modules
                if member.countries:
                    member.country_access = {
                        str(country).upper(): modules
                        for country in member.countries
                    }

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
            "marketplace_ids": member.marketplace_ids or [],
            "countries": member.countries or [],
            "modules": member.modules or [],
            "country_access": member.country_access or {},
            "token_name": member.token_name,
        }), 200

    except Exception as e:
        db.session.rollback()
        return _error(f"Failed to update member access: {str(e)}", 500)
    
    