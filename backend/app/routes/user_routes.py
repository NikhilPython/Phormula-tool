from werkzeug.security import generate_password_hash, check_password_hash
from flask import Blueprint, request, session, jsonify, redirect 
from app.utils.token_utils import (
    decode_token, generate_reset_token, generate_token, get_effective_user_id_from_token
)
import os 
from datetime import datetime, timezone, timedelta 
import pandas as pd
from sqlalchemy import inspect
from sqlalchemy import and_, or_
import numpy as np 
from app.utils.data_utils import  create_user_session
from app.utils.email_utils import send_verification_otp_email, send_welcome_email, send_reset_email
from app import db
from app.models.user_models import User, CountryProfile, Category , amazon_user, Member
import jwt
import secrets
import string
from config import Config
SECRET_KEY = Config.SECRET_KEY
from werkzeug.utils import secure_filename
from sqlalchemy import create_engine
from sqlalchemy import MetaData, Table, Column, Integer, String, Float, text
from dotenv import load_dotenv
from sqlalchemy.orm import sessionmaker
from sqlalchemy import func
import json

load_dotenv()
db_url = os.getenv('DATABASE_URL')
db_url1= os.getenv('DATABASE_ADMIN_URL')
db_url2= os.getenv('DATABASE_AMAZON_URL')

# Shared SQLAlchemy engines - create once, reuse everywhere
user_engine = create_engine(
    db_url,
    pool_pre_ping=True,
    pool_size=5,
    max_overflow=10,
    pool_recycle=1800,
)

admin_engine = create_engine(
    db_url1,
    pool_pre_ping=True,
    pool_size=5,
    max_overflow=10,
    pool_recycle=1800,
)

amazon_engine = create_engine(
    db_url2,
    pool_pre_ping=True,
    pool_size=5,
    max_overflow=10,
    pool_recycle=1800,
)


user_bp = Blueprint('user', __name__)

ALLOWED_MARKETPLACES = {
    "ATVPDKIKX0DER",  # US
    "A1F83G8C2ARO7P", # UK
    "A2EUQ1WTGCTBG2", # CA
}

COUNTRY_TO_MARKETPLACE = {
    "us": "ATVPDKIKX0DER",
    "usa": "ATVPDKIKX0DER",
    "united states": "ATVPDKIKX0DER",

    "uk": "A1F83G8C2ARO7P",
    "united kingdom": "A1F83G8C2ARO7P",

    "ca": "A2EUQ1WTGCTBG2",
    "canada": "A2EUQ1WTGCTBG2",
}


EMAIL_OTP_EXPIRY_MINUTES = 10
EMAIL_OTP_RESEND_COOLDOWN_SECONDS = 60
EMAIL_OTP_MAX_ATTEMPTS = 5


def _generate_email_otp() -> str:
    return f"{secrets.randbelow(1_000_000):06d}"


def _assign_email_verification_otp(user) -> str:
    otp = _generate_email_otp()
    now = datetime.now(timezone.utc)
    user.email_verification_otp_hash = generate_password_hash(otp)
    user.email_verification_otp_expires_at = now + timedelta(minutes=EMAIL_OTP_EXPIRY_MINUTES)
    user.email_verification_otp_attempts = 0
    user.email_verification_otp_sent_at = now
    return otp


def _utc_value(value):
    """Normalize a DB datetime to aware UTC for safe comparisons."""
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _otp_resend_retry_after(user) -> int:
    sent_at = _utc_value(getattr(user, "email_verification_otp_sent_at", None))
    if not sent_at:
        return 0
    elapsed = (datetime.now(timezone.utc) - sent_at).total_seconds()
    return max(0, int(EMAIL_OTP_RESEND_COOLDOWN_SECONDS - elapsed))

def compute_marketplace_ids_from_country(country_value: str) -> str:
    if not country_value:
        return ""

    countries = [c.strip().lower() for c in country_value.split(",") if c.strip()]
    ids = []

    for c in countries:
        if c == "unitedstates":
            c = "united states"
        elif c == "unitedkingdom":
            c = "united kingdom"

        mp = COUNTRY_TO_MARKETPLACE.get(c)
        if mp and mp in ALLOWED_MARKETPLACES:
            ids.append(mp)

    seen = set()
    ids = [x for x in ids if not (x in seen or seen.add(x))]

    return ",".join(ids)

def update_amazon_connection_summary(user_id):
    from app.models.user_models import User, amazon_user
    from app import db

    # count only connected marketplaces
    count = amazon_user.query.filter_by(
        user_id=user_id,
        is_connected=True
    ).count()

    user = User.query.get(user_id)
    if not user:
        return

    user.amazon_connected = count > 0
    user.connected_marketplaces_count = count

    db.session.commit()

@user_bp.route('/register', methods=['POST'])
def register():
    try:
        data = request.get_json(silent=True) or {}
        name = (data.get('name') or '').strip() or None
        email = (data.get('email') or '').strip().lower()
        password = data.get('password')
        phone_number = data.get('phone_number')

        if not email or not password:
            return jsonify({'success': False, 'message': 'Email and password are required'}), 400

        existing_user = User.query.filter_by(email=email).first()

        if existing_user and existing_user.is_verified:
            return jsonify({
                'success': False,
                'message': 'Email already exists. Please login.'
            }), 409

        hashed_password = generate_password_hash(
            password, method='pbkdf2:sha256', salt_length=8
        )

        if existing_user:
            # Safe recovery for an unfinished signup. Email ownership is still
            # required because the account remains unverified until OTP success.
            user = existing_user
            if name:
                user.name = name
            if phone_number:
                user.phone_number = phone_number
            user.password = hashed_password
        else:
            token = ''.join(
                secrets.choice(string.ascii_letters + string.digits)
                for _ in range(8)
            )
            token_name = f"user_{token}"

            user = User(
                name=name,
                email=email,
                password=hashed_password,
                phone_number=phone_number,
                token_name=token_name,
                is_verified=False,
                created_at=datetime.now(timezone.utc),
            )
            db.session.add(user)

        otp = _assign_email_verification_otp(user)
        db.session.commit()

        try:
            send_verification_otp_email(
                user.email,
                user.name,
                otp,
                EMAIL_OTP_EXPIRY_MINUTES,
            )
        except Exception as e:
            # Keep the unverified user so resend can recover without creating duplicates.
            print(f"Verification OTP email failed for {email}: {e}")
            return jsonify({
                'success': False,
                'message': 'Account created, but the verification code could not be sent. Please use resend.',
                'requires_verification': True,
                'email': email,
            }), 503

        return jsonify({
            'success': True,
            'message': 'Verification code sent to your email.',
            'requires_verification': True,
            'email': user.email,
            'user_id': user.id,
            'token_name': user.token_name,
            'otp_expires_in_seconds': EMAIL_OTP_EXPIRY_MINUTES * 60,
            'resend_available_in_seconds': EMAIL_OTP_RESEND_COOLDOWN_SECONDS,
        }), 201

    except Exception as e:
        db.session.rollback()
        print(f"Registration error: {str(e)}")
        return jsonify({
            'success': False,
            'message': 'Server error during registration',
            'error': str(e),
        }), 500


@user_bp.route('/login', methods=['POST'])
def login():
    data = request.get_json()
    if data is None:
        return jsonify({'success': False, 'message': 'Invalid input'}), 400

    email = (data.get('email') or '').strip().lower()
    password = data.get('password')

    if not email or not password:
        return jsonify({'success': False, 'message': 'Email and password are required'}), 400

    user = User.query.filter_by(email=email).first()
    if not user:
        return jsonify({'success': False, 'message': 'User not found'}), 404

    if not user.is_verified:
        return jsonify({
            'success': False,
            'error': 'Your email is not verified. Please verify your email first.',
            'message': 'Your email is not verified. Please verify your email first.',
            'requires_verification': True,
            'email': user.email,
        }), 403

    if check_password_hash(user.password, password):
        session['user_id'] = user.id
        token = generate_token(user.id)

        return jsonify({
            'success': True,
            'message': 'Valid email and password',
            'token': token,
            'is_member': False,     # ✅ add
            'user_id': user.id      # ✅ add
        }), 200

    return jsonify({'success': False, 'message': 'Invalid email or password'}), 401


@user_bp.route('/forgot_password', methods=['POST'])
def forgot_password():
    data = request.get_json()
    email = data.get('email')

    if not email:
        return jsonify({'success': False, 'message': 'Email is required.'}), 400

    user = User.query.filter_by(email=email).first()

    if not user:
        return jsonify({'success': False, 'message': 'Email not found.'}), 404

    # Generate and send email only if user exists
    token = generate_reset_token(user.id)
    reset_url = f"http://localhost:3000/reset_password/{token}"
    send_reset_email(user.email, reset_url, user.name)

    return jsonify({'success': True, 'message': 'Password reset email sent.'}), 200


@user_bp.route('/reset_password/<token>', methods=['POST'])
def reset_password(token):
    data = request.get_json() or {}
    password = data.get('password')

    if not password:
        return jsonify({'success': False, 'message': 'Password is required.'}), 400

    decoded_data = decode_token(token)
    if not decoded_data:
        return jsonify({'success': False, 'message': 'Invalid or expired token.'}), 400

    user_id = decoded_data.get('user_id')
    if user_id is None:
        return jsonify({'success': False, 'message': 'Invalid token payload.'}), 400

    user = db.session.get(User, user_id)   # preferred in newer SQLAlchemy
    if user is None:
        return jsonify({'success': False, 'message': 'User not found.'}), 404

    user.password = generate_password_hash(password, method='pbkdf2:sha256', salt_length=8)
    db.session.commit()

    return jsonify({'success': True, 'message': 'Password reset successfully.'}), 200



@user_bp.route('/google_register', methods=['POST'])
def google_register():
    try:
        data = request.get_json(silent=True) or {}

        email = data.get('email')
        name = data.get('name')  # ✅ NEW

        if not email:
            return jsonify({'success': False, 'message': 'Email is required'}), 400

        # ✅ set phone to None if you don't want dummy numbers
        phone_number = data.get('phone_number',"0000000000")  # None if not provided

        password = data.get('password', "default_password")

        user = User.query.filter_by(email=email).first()

        if user and not user.is_google_user:
            return jsonify({
                'success': False,
                'message': 'Email already exists with regular account. Please use regular login.'
            }), 409

        created = False

        if not user:
            random_token = ''.join(secrets.choice(string.ascii_letters + string.digits) for _ in range(8))
            token_name = f"user_{random_token}"

            password_hash = generate_password_hash(password, method='pbkdf2:sha256', salt_length=8)

            user = User(
                email=email,
                name=name,              # ✅ NEW
                phone_number=phone_number,  # ✅ None allowed
                password=password_hash,
                is_google_user=True,
                is_verified=True,
                token_name=token_name
            )
            db.session.add(user)
            db.session.commit()
            created = True

            try:
                send_welcome_email(email, name)
            except Exception as e:
                print(f"Failed to send welcome email to {email}: {e}")

        # ✅ backfill name for existing google users if empty
        if user and user.is_google_user and name and (not getattr(user, "name", None)):
            user.name = name
            db.session.commit()

        if not user.is_verified:
            user.is_verified = True
            db.session.commit()

        token = generate_token(user.id)
        session['user_id'] = user.id

        return jsonify({
            'success': True,
            'message': 'Google login successful' if not created else 'Google user registered successfully',
            'token': token,
            'user_id': user.id,
            'show_country_selection': True if created else False
        }), 200

    except Exception as e:
        db.session.rollback()
        print(f"Google registration error: {str(e)}")
        return jsonify({'success': False, 'message': 'Server error during Google auth', 'error': str(e)}), 500


@user_bp.route('/resend_verification', methods=['POST'])
@user_bp.route('/resend-verification-otp', methods=['POST'])
def resend_verification_email():
    data = request.get_json(silent=True) or {}
    email = (data.get('email') or '').strip().lower()

    if not email:
        return jsonify({'success': False, 'message': 'Email is required'}), 400

    user = User.query.filter_by(email=email).first()
    if not user:
        return jsonify({'success': False, 'message': 'User not found'}), 404

    if user.is_verified:
        return jsonify({
            'success': True,
            'message': 'Email is already verified.',
            'already_verified': True,
        }), 200

    retry_after = _otp_resend_retry_after(user)
    if retry_after > 0:
        return jsonify({
            'success': False,
            'message': f'Please wait {retry_after} seconds before requesting another code.',
            'retry_after_seconds': retry_after,
        }), 429

    otp = _assign_email_verification_otp(user)
    db.session.commit()

    try:
        send_verification_otp_email(
            user.email,
            user.name,
            otp,
            EMAIL_OTP_EXPIRY_MINUTES,
        )
    except Exception as e:
        print(f"Failed to resend verification OTP to {email}: {e}")
        return jsonify({
            'success': False,
            'message': 'Failed to resend verification code.',
        }), 503

    return jsonify({
        'success': True,
        'message': 'A new verification code has been sent.',
        'otp_expires_in_seconds': EMAIL_OTP_EXPIRY_MINUTES * 60,
        'resend_available_in_seconds': EMAIL_OTP_RESEND_COOLDOWN_SECONDS,
    }), 200



def _has_token(val):
    return bool(val and str(val).strip())


@user_bp.route('/get_user_data', methods=['GET'])
def get_user_data():
    auth_header = request.headers.get('Authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        return jsonify({'error': 'Authorization token is missing or invalid'}), 401

    token = auth_header.split(' ')[1]

    try:
        payload, user_id, member_id = get_effective_user_id_from_token(token)
    except jwt.ExpiredSignatureError:
        return jsonify({'error': 'Token has expired'}), 401
    except jwt.InvalidTokenError:
        return jsonify({'error': 'Invalid token'}), 401

    is_member = payload.get("is_member") is True
    member_id = payload.get("member_id")
    marketplace_ids = payload.get("marketplace_ids") or []
    modules = payload.get("modules") or []
    countries = payload.get("countries") or []
    country_access = payload.get("country_access") or {}
    token_email = payload.get("email")

    member_name = None
    member_role = None

    user = User.query.filter_by(id=int(user_id)).first()
    if not user:
        return jsonify({'error': 'User not found'}), 404

    if is_member and member_id:
        m = Member.query.filter_by(id=int(member_id), owner_user_id=int(user_id)).first()
        if m:
            member_name = getattr(m, "member_name", None)
            member_role = getattr(m, "role", None)

            # Always use latest DB access for members
            marketplace_ids = m.marketplace_ids or []
            countries = m.countries or []
            modules = m.modules or []
            country_access = m.country_access or {}

    arow = amazon_user.query.filter_by(user_id=int(user_id)).first()

    spapi_connected = False
    ads_connected = False
    rows = amazon_user.query.filter_by(user_id=int(user_id)).all()

    countries_list = [r.country_name for r in rows if r.country_name]
    marketplace_ids_list = [r.marketplace_id for r in rows if r.marketplace_id]

    if arow:
        spapi_connected = _has_token(arow.refresh_token)
        ads_connected = _has_token(arow.amazon_ads_refresh_token)

    amazon_user_exists = bool(spapi_connected and ads_connected)

    user.amazon_user_exists = amazon_user_exists
    user.amazon_ads_exists = ads_connected

    # ✅ Check if SKU table exists
    sku_table_name = f"sku_{user_id}_data_table"
    sku_sheet_exists = False

    try:
        inspector = inspect(user_engine)
        sku_sheet_exists = inspector.has_table(sku_table_name)
    except Exception:
        sku_sheet_exists = False

    user.sku_sheet_exists = sku_sheet_exists

    try:
        db.session.commit()
    except Exception as e:
        db.session.rollback()
        return jsonify({'error': 'Failed to update flags', 'details': str(e)}), 500

    effective_name = member_name if is_member else user.name
    effective_email = token_email if is_member else user.email

    response = {
        "is_member": is_member,
        "user_id": int(user_id),
        "status": user.status,
        "steps_exists" : user.steps_exists,
        "owner_user_id": int(user_id),
        "member_id": int(member_id) if member_id else None,

        "member_name": member_name,
        "member_role": member_role,

        "name": effective_name,
        "email": effective_email,
        "owner_email": user.email,
        "member_email": token_email if is_member else None,

        "countries": countries if is_member else countries_list,
        "marketplace_ids": marketplace_ids if is_member else marketplace_ids_list,
        "modules": modules,
        "country_access": country_access,
       

        "amazon_user_exists": user.amazon_user_exists,
        "amazon_ads_exists": user.amazon_ads_exists,
        "sku_sheet_exists": user.sku_sheet_exists,

        "company_name": user.company_name,
        "brand_name": user.brand_name,
        "phone_number": user.phone_number,
        "annual_sales_range": user.annual_sales_range,
        "homeCurrency": user.homeCurrency,
        "target_sales": float(user.target_sales) if user.target_sales is not None else None,
        "tax_id": user.tax_id,
        "address": user.address,

        # Workspace activity timestamps. Keep them as ISO UTC values; frontend chooses display timezone.
        "company_updated_at": user.company_updated_at.isoformat() if user.company_updated_at else None,
        "sku_updated_at": user.sku_updated_at.isoformat() if user.sku_updated_at else None,
        "integration_updated_at": user.integration_updated_at.isoformat() if user.integration_updated_at else None,
    }

    if not is_member:
        response["name"] = user.name

        members = Member.query.filter_by(owner_user_id=int(user_id)).all()
        response["members"] = [
            {
                "id": int(m.id),
                "member_name": getattr(m, "member_name", None),
                "email": getattr(m, "email", None),
                "role": getattr(m, "role", None),
                "is_active": getattr(m, "is_active", None),

                "countries": m.countries or [],
                "marketplace_ids": m.marketplace_ids or [],
                "modules": m.modules or [],
                "country_access": m.country_access or {},

                "created_at": getattr(m, "created_at", None),
                "updated_at": getattr(m, "updated_at", None),
            }
            for m in members
        ]

    return jsonify(response), 200



@user_bp.route('/passcountry', methods=['GET'])
def get_user_countries():
    auth_header = request.headers.get('Authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        return jsonify({'error': 'Authorization token is missing or invalid'}), 401

    token = auth_header.split(' ')[1]
    try:
        payload, user_id, member_id = get_effective_user_id_from_token(token)
    except jwt.ExpiredSignatureError:
        return jsonify({'error': 'Token has expired'}), 401
    except jwt.InvalidTokenError:
        return jsonify({'error': 'Invalid token'}), 401

    user = User.query.filter_by(id=user_id).first()
    if not user:
        return jsonify({'error': 'User not found.'}), 404

    rows = amazon_user.query.filter_by(user_id=user_id).all()

    country_list = [r.country_name for r in rows if r.country_name]  

    return jsonify({'countries': country_list}), 200



@user_bp.route('/selectform', methods=['POST'])
def add_sales():
    auth_header = request.headers.get('Authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        return jsonify({'error': 'Authorization token is missing or invalid'}), 401

    token = auth_header.split(' ')[1]
    try:
        payload, user_id, member_id = get_effective_user_id_from_token(token)
    except jwt.ExpiredSignatureError:
        return jsonify({'error': 'Token has expired'}), 401
    except jwt.InvalidTokenError:
        return jsonify({'error': 'Invalid token'}), 401

    data = request.get_json(silent=True) or {}

    country = (data.get('country') or '').strip().lower()
    annual_sales_range = (data.get('annual_sales_range') or '').strip()
    brand_name = data.get('brand_name')
    company_name = data.get('company_name')
    homeCurrency = data.get('homeCurrency')

    if not country:
        return jsonify({'success': False, 'message': 'Country is required.'}), 400

    user = User.query.filter_by(id=user_id).first()
    if not user:
        return jsonify({'success': False, 'message': 'User not found.'}), 404

    marketplace_ids = compute_marketplace_ids_from_country(country).split(',')
    for mp_id in marketplace_ids:
        if not mp_id:
            continue

        au = amazon_user.query.filter_by(
            user_id=user_id,
            marketplace_id=mp_id
        ).first()

        if not au:
            au = amazon_user(
                user_id=user_id,
                marketplace_id=mp_id,
                country_name=country,
                is_connected=True
            )
            db.session.add(au)
        else:
            au.country_name = country
            au.is_connected = True

    if company_name is not None and str(company_name).strip():
        user.company_name = str(company_name).strip()

    if brand_name is not None and str(brand_name).strip():
        user.brand_name = str(brand_name).strip()

    if homeCurrency is not None and str(homeCurrency).strip():
        user.homeCurrency = str(homeCurrency).strip()

    if annual_sales_range:
        user.annual_sales_range = annual_sales_range

    # =========================================================
    # AMAZON MARKETPLACE INTEGRATION UPDATED TIME
    # =========================================================
    # Only update this timestamp when the marketplace involved in this
    # /selectform request has a real Amazon refresh token and is connected.
    # This avoids changing the time on simple status/read requests.
    valid_marketplace_ids = [mp_id for mp_id in marketplace_ids if mp_id]

    if valid_marketplace_ids:
        connected_amazon = amazon_user.query.filter(
            amazon_user.user_id == user_id,
            amazon_user.marketplace_id.in_(valid_marketplace_ids),
            amazon_user.is_connected.is_(True),
            amazon_user.refresh_token.isnot(None),
            amazon_user.refresh_token != ""
        ).first()

        if connected_amazon:
            user.integration_updated_at = datetime.now(timezone.utc)

    db.session.commit()
    db.session.refresh(user)
    update_amazon_connection_summary(user_id)

    return jsonify({
        'success': True,
        'message': 'Sales data submitted successfully.',
        'marketplace_id': marketplace_ids,
        'integration_updated_at': (
            user.integration_updated_at.isoformat()
            if user.integration_updated_at
            else None
        ),
    }), 201


@user_bp.route('/verify-email-otp', methods=['POST'])
def verify_email_otp():
    data = request.get_json(silent=True) or {}
    email = (data.get('email') or '').strip().lower()
    otp = ''.join(ch for ch in str(data.get('otp') or '') if ch.isdigit())

    if not email or not otp:
        return jsonify({
            'success': False,
            'message': 'Email and verification code are required.',
        }), 400

    if len(otp) != 6:
        return jsonify({
            'success': False,
            'message': 'Verification code must be 6 digits.',
        }), 400

    user = User.query.filter_by(email=email).first()
    if not user:
        return jsonify({'success': False, 'message': 'User not found.'}), 404

    if user.is_verified:
        return jsonify({
            'success': True,
            'message': 'Email is already verified.',
            'already_verified': True,
            'user_id': user.id,
        }), 200

    otp_hash = user.email_verification_otp_hash
    expires_at = _utc_value(user.email_verification_otp_expires_at)

    if not otp_hash or not expires_at:
        return jsonify({
            'success': False,
            'message': 'No active verification code. Please request a new code.',
            'code': 'OTP_NOT_FOUND',
        }), 400

    if datetime.now(timezone.utc) > expires_at:
        user.email_verification_otp_hash = None
        user.email_verification_otp_expires_at = None
        db.session.commit()
        return jsonify({
            'success': False,
            'message': 'Verification code has expired. Please request a new code.',
            'code': 'OTP_EXPIRED',
        }), 400

    attempts = int(user.email_verification_otp_attempts or 0)
    if attempts >= EMAIL_OTP_MAX_ATTEMPTS:
        return jsonify({
            'success': False,
            'message': 'Too many incorrect attempts. Please request a new code.',
            'code': 'OTP_TOO_MANY_ATTEMPTS',
        }), 429

    if not check_password_hash(otp_hash, otp):
        user.email_verification_otp_attempts = attempts + 1
        db.session.commit()
        remaining = max(0, EMAIL_OTP_MAX_ATTEMPTS - user.email_verification_otp_attempts)
        return jsonify({
            'success': False,
            'message': 'Invalid verification code.',
            'code': 'OTP_INVALID',
            'attempts_remaining': remaining,
        }), 400

    user.is_verified = True

    user.email_verification_otp_hash = None
    user.email_verification_otp_expires_at = None
    user.email_verification_otp_attempts = 0
    user.email_verification_otp_sent_at = None

    db.session.commit()

    session["user_id"] = user.id
    token = generate_token(user.id)

    return jsonify({
        "success": True,
        "message": "Email verified successfully.",
        "token": token,
        "user_id": user.id,
        "is_member": False,
        "show_country_selection": True
    }), 200


@user_bp.route('/switch_profile/<int:profile_id>', methods=['GET'])
def switch_profile(profile_id):
    auth_header = request.headers.get('Authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        return jsonify({'error': 'Authorization token is missing or invalid'}), 401

    token = auth_header.split(' ')[1]
    user_id = decode_token(token)
    if not user_id:
        return jsonify({'error': 'Invalid token'}), 401

    user = User.query.get(user_id)
    if not user:
        return jsonify({'error': 'User not found'}), 404

    profile = CountryProfile.query.get(profile_id)
    if profile and profile.user_id == user_id:
        session['profile_id'] = profile.id
        session['last_profile'] = {
            'profile_id': profile.id,
            'country': profile.country,
            'transit_time': profile.transit_time,
            'stock_unit': profile.stock_unit
        }
        return jsonify({'message': 'Profile switched successfully', 'profile_id': profile.id, 'country': profile.country, 'marketplace': profile.marketplace}), 200

    return jsonify({'error': 'Profile not found or unauthorized access'}), 404



@user_bp.route('/profileupdate', methods=['POST'])
def profileupdate():
    auth_header = request.headers.get('Authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        return jsonify({'error': 'Authorization token is missing or invalid'}), 401

    token = auth_header.split(' ')[1]
    try:
        payload, user_id, member_id = get_effective_user_id_from_token(token)
    except jwt.ExpiredSignatureError:
        return jsonify({'error': 'Token has expired'}), 401
    except jwt.InvalidTokenError:
        return jsonify({'error': 'Invalid token'}), 401

    user = User.query.get(user_id)
    if not user:
        return jsonify({'error': 'User not found'}), 404

    data = request.get_json(silent=True) or {}

    def update_if_present(instance, field_name, data, key, strip=True, ignore_blank=True):
        if key not in data:
            return

        value = data.get(key)

        if isinstance(value, str) and strip:
            value = value.strip()

        if ignore_blank and value == "":
            return

        setattr(instance, field_name, value)

    # ---------- SAFE FIELD UPDATES ----------
    update_if_present(user, 'name', data, 'name')
    update_if_present(user, 'email', data, 'email')
    update_if_present(user, 'phone_number', data, 'phone_number')
    update_if_present(user, 'annual_sales_range', data, 'annual_sales_range')
    update_if_present(user, 'company_name', data, 'company_name')
    update_if_present(user, 'brand_name', data, 'brand_name')
    update_if_present(user, 'homeCurrency', data, 'homeCurrency')

    # ---------- TAX ID (JSON MERGE SAFE) ----------
    if 'tax_id' in data:
        tax_val = data.get('tax_id')

        if isinstance(tax_val, str):
            try:
                tax_val = json.loads(tax_val)
            except Exception:
                return jsonify({'error': 'Invalid tax_id format'}), 400

        if isinstance(tax_val, dict):
            existing_tax = user.tax_id or {}
            existing_tax.update({k: v for k, v in tax_val.items() if v not in [None, ""]})
            user.tax_id = existing_tax

    # ---------- ADDRESS (JSON MERGE SAFE) ----------
    if 'address' in data:
        addr_val = data.get('address')

        if isinstance(addr_val, str):
            try:
                addr_val = json.loads(addr_val)
            except Exception:
                return jsonify({'error': 'Invalid address format'}), 400

        if isinstance(addr_val, dict):
            existing_addr = user.address or {}
            existing_addr.update({k: v for k, v in addr_val.items() if v not in [None, ""]})
            user.address = existing_addr

    # ---------- PASSWORD (HASHED) ----------
    new_password = data.get('password')
    if new_password:
        user.password = generate_password_hash(
            new_password,
            method='pbkdf2:sha256',
            salt_length=8
        )

    # ---------- COUNTRY + MARKETPLACE ----------
    new_country = data.get('country')

    if new_country:
        marketplace_ids = compute_marketplace_ids_from_country(new_country).split(',')

        for mp_id in marketplace_ids:
            if not mp_id:
                continue

            au = amazon_user.query.filter_by(
                user_id=user_id,
                marketplace_id=mp_id
            ).first()

            if not au:
                au = amazon_user(
                    user_id=user_id,
                    marketplace_id=mp_id,
                    country_name=new_country,
                    is_connected=True
                )
                db.session.add(au)
            else:
                au.country_name = new_country
                au.is_connected = True

    # ---------- TARGET SALES (VALIDATED) ----------
    target_sales = data.get('target_sales')
    if target_sales not in [None, ""]:
        try:
            user.target_sales = float(target_sales)
        except (TypeError, ValueError):
            return jsonify({'error': 'target_sales must be a number'}), 400
        
    # ---------- STEPS EXISTS ----------
    if 'steps_exists' in data:
        user.steps_exists = bool(data.get('steps_exists'))

    # ---------- COMPANY ACTIVITY TIMESTAMP ----------
    # Update only for company/business fields, not for unrelated profile changes.
    company_activity_fields = {
        'company_name',
        'brand_name',
        'homeCurrency',
        'tax_id',
        'address',
    }
    if any(field in data for field in company_activity_fields):
        user.company_updated_at = datetime.now(timezone.utc)


    # ---------- COMMIT ----------
    try:
        db.session.commit()
        update_amazon_connection_summary(user_id)
        return jsonify({'message': 'Profile updated successfully'}), 200
    except Exception as e:
        db.session.rollback()
        return jsonify({'error': str(e)}), 500


@user_bp.route('/feepreviewupload', methods=['POST'])
def feepreviewupload():
    auth_header = request.headers.get('Authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        return jsonify({'error': 'Authorization token is missing or invalid'}), 401

    token = auth_header.split(' ')[1]
    try:
        payload, user_id, member_id = get_effective_user_id_from_token(token)
    except jwt.ExpiredSignatureError:
        return jsonify({'error': 'Token has expired'}), 401
    except jwt.InvalidTokenError:
        return jsonify({'error': 'Invalid token'}), 401
    user = User.query.get(user_id)
    
    

    country = request.form.get('country').lower()
    marketplace = request.form.get('marketplace')
    file = request.files.get('file')
    transit_time = int(request.form.get('transit_time'))  # Transit time in months  # Transit time in months
    stock_unit = request.form.get('stock_unit')

    
    
    if not country or not marketplace or not file or not transit_time or not stock_unit :
        return jsonify({'error': 'Country, marketplace, and file , transit_time, stock_unit are required'}), 400
    
    existing_profile = CountryProfile.query.filter_by(
            user_id=user_id, 
            country=country, 
            marketplace=marketplace, 
    ).first()

    if existing_profile:
            db.session.delete(existing_profile)
            db.session.commit()  # Commit the deletion

    
    
    existing_profile = CountryProfile.query.filter_by(user_id=user_id, country=country, marketplace=marketplace, transit_time=transit_time, stock_unit=stock_unit ).first()
    if existing_profile:
        return jsonify({'message': f'Profile already exists for country: {country} and marketplace: {marketplace}'}), 409  # Conflict status code

    if file:
        filename = secure_filename(file.filename)
        file_path = os.path.join( filename)
        file.save(file_path)


        # Read the Excel file using pandas
        df = pd.read_excel(file_path)
        def clean_numeric(value):
            if value == '--' or pd.isna(value):
                return 0  # You can use 0 if you prefer
            try:
                return float(value)
            except ValueError:
                return 0

        float_cols = [
            'price', 'estimated_fees', 'estimated_referral_fee', 'sales_price', 'longest_side', 'median_side',
            'shortest_side', 'length_and_girth', 'item_package_weight', 'estimated_variable_closing_fee',
            'expected_domestic_fulfilment_fee_per_unit', 'expected_efn_fulfilment_fee_per_unit_uk',
            'expected_efn_fulfilment_fee_per_unit_de', 'expected_efn_fulfilment_fee_per_unit_fr',
            'expected_efn_fulfilment_fee_per_unit_it', 'expected_efn_fulfilment_fee_per_unit_es',
            'expected_efn_fulfilment_fee_per_unit_se'
        ]

        for column in float_cols:
            if column in df.columns:
                df[column] = df[column].apply(clean_numeric)
         # Create user-specific database session
        user_session = create_user_session(db_url)

        AdminSession = sessionmaker(bind=admin_engine)
        admin_session = AdminSession()




        country = country.lower()
        table_name = f'user_{user_id}_{country.lower()}_table'
        metadata = MetaData()
        metadata.bind = user_engine

        user_specific_table = Table(
               table_name, metadata,
             Column('id', Integer, primary_key=True),
        Column('user_id', Integer, nullable=False),
        Column('country', String(255), nullable=False),
        Column('transit_time', Integer, nullable=False),
        Column('stock_unit', Integer, nullable=False),
        Column('product_group', String(255), nullable=False),
        Column('estimated_fees', Float, nullable=False),
        Column('referral_fee', Float, nullable=True) ,
        Column('estimated_referral_fee', Float, nullable=True) ,
        Column('marketplace', String(255), nullable=False),
        Column('file_name', String(255), nullable=False),
        Column('sku', String(255), nullable=False),
        Column('fnsku', String(255), nullable=False),
        Column('amazon_store', String(255), nullable=False),
        Column('asin', String(255), nullable=False),
        Column('product_barcode', String(255), nullable=True),  # New column
        Column('sku_cost_price', Float, nullable=True),  # New column   
        Column('product_name', String(255), nullable=False),
        Column('brand', String(255), nullable=False),
        Column('price', Float, nullable=False),
        Column('fulfilled_by', String(255), nullable=True),
        Column('has_local_inventory', String(255), nullable=True),
        Column('sales_price', Float, nullable=True),
        Column('longest_side', Float, nullable=True),
        Column('median_side', Float, nullable=True),
        Column('shortest_side', Float, nullable=True),
        Column('length_and_girth', Float, nullable=True),
        Column('unit_of_dimension', String(50), nullable=True),
        Column('item_package_weight', Float, nullable=True),
        Column('unit_of_weight', String(50), nullable=True),
        Column('product_size_weight_band', String(255), nullable=True),
        Column('currency', String(10), nullable=True),
        Column('estimated_variable_closing_fee', Float, nullable=True),
        Column('expected_domestic_fulfilment_fee_per_unit', Float, nullable=True),
        Column('expected_efn_fulfilment_fee_per_unit_uk', Float, nullable=True),
        Column('expected_efn_fulfilment_fee_per_unit_de', Float, nullable=True),
        Column('expected_efn_fulfilment_fee_per_unit_fr', Float, nullable=True),
        Column('expected_efn_fulfilment_fee_per_unit_it', Float, nullable=True),
        Column('expected_efn_fulfilment_fee_per_unit_es', Float, nullable=True),
        Column('expected_efn_fulfilment_fee_per_unit_se', Float, nullable=True),


    )
        metadata.create_all(user_engine)
        
        with user_engine.connect() as conn:
            conn.execute(text(f"DELETE FROM {table_name} WHERE user_id = :user_id AND country = :country AND marketplace = :marketplace"),
                            {"user_id": user_id, "country": country, "marketplace": marketplace})
            conn.commit()


        df = df.fillna('')
        df.replace('--', np.nan, inplace=True)
    
        # Iterate over the rows and save data to the database
        for _, row in df.iterrows():
            # fnsku = row['Fnsku'] if pd.notna(row['Fnsku']) else ''
            insert_stmt = user_specific_table.insert().values(
                user_id=user_id,
                country=country,
                transit_time=transit_time,
                stock_unit=stock_unit,
                marketplace=marketplace,
                file_name=filename,
                sku=row.get('sku', ''),
                fnsku=row.get('fnsku', ''),
                asin=row.get('asin', ''),
                amazon_store=row.get('amazon-store', ''),
                product_name=row.get('product-name', ''),
                product_group=row.get('product-group', ''),
                brand=row.get('brand', ''),
                price=row.get('your-price', 0),
                estimated_fees=row.get('estimated-fee-total', 0),
                estimated_referral_fee=row.get('estimated-referral-fee-per-unit', None),
                fulfilled_by=row.get('fulfilled-by', ''),
                has_local_inventory=row.get('has-local-inventory', ''),
                sales_price=row.get('sales-price', None),
                longest_side=row.get('longest-side', None),
                median_side=row.get('median-side', None),
                shortest_side=row.get('shortest-side', None),
                length_and_girth=row.get('length-and-girth', None),
                unit_of_dimension=row.get('unit-of-dimension', ''),
                item_package_weight=row.get('item-package-weight', None),
                unit_of_weight=row.get('unit-of-weight', ''),
                product_size_weight_band=row.get('product-size-weight-band', ''),
                currency=row.get('currency', ''),
                estimated_variable_closing_fee=row.get('estimated-variable-closing-fee', None),
                expected_domestic_fulfilment_fee_per_unit=row.get('expected-domestic-fulfilment-fee-per-unit', None),
                expected_efn_fulfilment_fee_per_unit_uk=row.get('expected-efn-fulfilment-fee-per-unit-uk', None),
                expected_efn_fulfilment_fee_per_unit_de=row.get('expected-efn-fulfilment-fee-per-unit-de', None),
                expected_efn_fulfilment_fee_per_unit_fr=row.get('expected-efn-fulfilment-fee-per-unit-fr', None),
                expected_efn_fulfilment_fee_per_unit_it=row.get('expected-efn-fulfilment-fee-per-unit-it', None),
                expected_efn_fulfilment_fee_per_unit_es=row.get('expected-efn-fulfilment-fee-per-unit-es', None),
                expected_efn_fulfilment_fee_per_unit_se=row.get('expected-efn-fulfilment-fee-per-unit-se', None),
        
                referral_fee=None,
                product_barcode=None,  # Will update later
                sku_cost_price=None  # Initialize with None, will update later

            )
            user_session.execute(insert_stmt)
        user_session.commit()


        query = user_specific_table.select()

        with user_engine.begin() as conn:
            results = conn.execute(query).mappings().all()

            for result in results:
                country_value = result['country']
                product_group_value = result['product_group']
                price_value = result['price']

                product_group_cleaned = (product_group_value or "").strip()

                COUNTRY_MAP = {
                    "uk": "United Kingdom",
                    "us": "United States",
                    "uae": "United Arab Emirates",
                    "in": "India"
                }

                normalized_country = COUNTRY_MAP.get(country.lower(), country)

                queery = admin_session.query(Category).filter_by(
                    country=normalized_country,
                    category=product_group_cleaned
                )

                Category_obj = queery.first()

                if not Category_obj:
                    first_word = product_group_cleaned.split('&')[0].split('/')[0].split('-')[0].strip().upper()

                    queery = admin_session.query(Category).filter_by(
                        country=normalized_country.strip(),
                        category=first_word
                    )

                if price_value is not None:
                    queery = queery.filter(
                        and_(
                            or_(Category.price_from == 0, Category.price_from <= price_value),
                            or_(Category.price_to == 0, Category.price_to >= price_value)
                        )
                    )

                Category_obj = queery.first()

                if Category_obj:
                    update_stmt = (
                        user_specific_table.update()
                        .where(user_specific_table.c.id == result['id'])
                        .values(referral_fee=Category_obj.referral_fee)
                    )

                    conn.execute(update_stmt)
                else:
                    print(f"No matching Category found for country {country_value} and Category {product_group_value}")


         # Verify update
        with user_engine.connect() as conn:
         updated_results = conn.execute(query).mappings().all()
         for result in updated_results:
            print(f"ID: {result['id']}, Referral Fee: {result['referral_fee']}")  # Verify update
        
        



        sku_table_name = f"sku_{user_id}_data_table"
        with user_engine.connect() as conn:
            sku_data_table = Table(sku_table_name, metadata, autoload_with=user_engine)

            query = text(f"""
                SELECT asin, product_barcode, price 
                FROM {sku_table_name}
                WHERE asin IN (SELECT asin FROM {table_name} WHERE user_id = :user_id)
            """)

            result = conn.execute(query, {"user_id": user_id})

            # Fetch rows as dictionaries
            rows = [dict(zip(result.keys(), row)) for row in result.fetchall()]

            # Now safely access dictionary keys
            asin_mapping = {row['asin']: (row['product_barcode'], row['price']) for row in rows}

        # Update user_{country}_table with fetched data
        with user_engine.begin() as conn:
            for asin, (barcode, price) in asin_mapping.items():
                update_stmt = text(f"""
                    UPDATE {table_name}
                    SET product_barcode = :barcode, sku_cost_price = :price
                    WHERE asin = :asin AND user_id = :user_id
                """)
                conn.execute(update_stmt, {
                    "barcode": barcode,
                    "price": price,
                    "asin": asin,
                    "user_id": user_id
                })

        new_profile = CountryProfile(
            user_id=user_id,
            country=country, 
            marketplace=marketplace,
            transit_time=transit_time,
            stock_unit=stock_unit

        )
        db.session.add(new_profile)
        db.session.commit()

        user_session.close()
        admin_session.close()

        return jsonify({
            'message': 'New profile created successfully',
            'profile_id': new_profile.id,
            'country': new_profile.country
        }), 201

    return jsonify({'message': 'File successfully uploaded and data added to the database.'})
    

@user_bp.route('/check-user-country-table', methods=['POST'])
def check_user_country_table_exists():
    try:
        data = request.get_json(silent=True) or {}

        user_id = data.get('user_id')

        if not user_id:
            return jsonify({
                "success": False,
                "message": "user_id is required"
            }), 400

        # Fetch user
        user = User.query.filter_by(id=user_id).first()
        if not user:
            return jsonify({
                "success": False,
                "message": "User not found"
            }), 404


        # Use country from DB
        rows = amazon_user.query.filter_by(user_id=user_id).all()
        countries = [r.country_name for r in rows if r.country_name]

        if not countries:
            return jsonify({
                "success": False,
                "message": "No countries connected"
            }), 400

        # Example: pick first or loop
        country = countries[0].strip().lower()

        table_name = f"user_{user_id}_{country}_merge_data_of_all_months"

        inspector = inspect(user_engine)

        table_exists = inspector.has_table(table_name)

        # Update column
        user.user_table_exists = table_exists
        db.session.commit()

        return jsonify({
            "success": True,
            "user_id": user_id,
            "country": country,
            "table_name": table_name,
            "exists": table_exists
        }), 200

    except Exception as e:
        db.session.rollback()
        return jsonify({
            "success": False,
            "message": str(e)
        }), 500
     
