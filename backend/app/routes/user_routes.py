from werkzeug.security import generate_password_hash, check_password_hash
from flask import Blueprint, request, session, jsonify, redirect 
from app.utils.token_utils import (
    decode_token, generate_reset_token, confirm_verification_token,
    generate_token, generate_verification_token, get_effective_user_id_from_token
)
import os  
import pandas as pd
from sqlalchemy import and_, or_
import numpy as np 
from app.utils.data_utils import  create_user_session
from app.utils.email_utils import send_welcome_and_verification_emails , send_reset_email
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


user_bp = Blueprint('user', __name__)

ALLOWED_MARKETPLACES = {
    "ATVPDKIKX0DER",  # US
    "A1F83G8C2ARO7P", # UK
    "A2EUQ1WTGCTBG2", # CA
}

COUNTRY_TO_MARKETPLACE = {
    "us": "ATVPDKIKX0DER",
    "uk": "A1F83G8C2ARO7P",
    "ca": "A2EUQ1WTGCTBG2",
}

def compute_marketplace_ids_from_country(country_value: str) -> str:
    """
    country_value can be:
      - 'uk'
      - 'uk,us'
      - 'UK, US'
    returns comma-separated marketplace ids in stable order.
    """
    if not country_value:
        return ""

    countries = [c.strip().lower() for c in country_value.split(",") if c.strip()]
    ids = []

    for c in countries:
        mp = COUNTRY_TO_MARKETPLACE.get(c)
        if mp and mp in ALLOWED_MARKETPLACES:
            ids.append(mp)

    # remove duplicates while preserving order
    seen = set()
    ids = [x for x in ids if not (x in seen or seen.add(x))]

    return ",".join(ids)




# @user_bp.route('/register', methods=['POST'])
# def register():
#     try:
#         data = request.get_json()
#         name = data.get('name')
#         email = data['email']
#         password = data['password']
#         phone_number = data['phone_number']

#         hashed_password = generate_password_hash(password, method='pbkdf2:sha256', salt_length=8)

#         # Check if user already exists
#         existing_user = User.query.filter_by(email=email).first()
#         if existing_user:
#             return jsonify({'success': False, 'message': 'Email already exists. Please choose a different email. Please Login.'})

#         # Generate token_name for new user
#         token = ''.join(secrets.choice(string.ascii_letters + string.digits) for _ in range(8))
#         token_name = f"user_{token}"

#         # Register new user with token_name
#         new_user = User(
#             name=name,
#             email=email, 
#             password=hashed_password, 
#             phone_number=phone_number,
#             token_name=token_name
#         )
#         db.session.add(new_user)
#         db.session.commit()

#         # Generate token and verification link
#         token = generate_token(new_user.id)
#         verification_token = generate_verification_token(email)
#         verification_link = f'http://localhost:3000/verify-email/{verification_token}'

#         # Send welcome and verification emails
#         try:
#             # test_send_email()
#             send_welcome_and_verification_emails(email, name, verification_link)
#         except Exception as e:
#             return jsonify({'success': False, 'message': 'Failed to send verification or welcome email', 'error': str(e)})

#         # ✅ Return success and instruct frontend to show country selection UI
#         return jsonify({
#             'success': True,
#             'message': 'User registered successfully. Please check your email to verify your account.',
#             'show_country_selection': True,
#             'user_id': new_user.id,  # optional: can be used to store in frontend state
#             'token_name': token_name  # Include token_name in response
#         })

#     except Exception as e:
#         db.session.rollback()
#         print(f"Registration error: {str(e)}")
#         return jsonify({'success': False, 'message': 'Server error during registration', 'error': str(e)}), 500

@user_bp.route('/register', methods=['POST'])
def register():
    try:
        data = request.get_json() or {}
        name = data.get('name')
        email = (data.get('email') or "").strip().lower()
        password = data.get('password')
        phone_number = data.get('phone_number')

        if not email or not password:
            return jsonify({'success': False, 'message': 'Email and password are required'}), 400

        hashed_password = generate_password_hash(
            password, method='pbkdf2:sha256', salt_length=8
        )

        # Check if user already exists
        existing_user = User.query.filter_by(email=email).first()
        if existing_user:
            return jsonify({
                'success': False,
                'message': 'Email already exists. Please choose a different email. Please Login.'
            }), 409

        # Generate token_name for new user
        token = ''.join(secrets.choice(string.ascii_letters + string.digits) for _ in range(8))
        token_name = f"user_{token}"

        # Register new user with token_name
        new_user = User(
            name=name,
            email=email,
            password=hashed_password,
            phone_number=phone_number,
            token_name=token_name,
            is_verified=False  # ✅ ensure default false
        )
        db.session.add(new_user)
        db.session.commit()

        # ✅ Generate verification token and link (IMPORTANT: link should hit BACKEND)
        verification_token = generate_verification_token(email)

        # change port if your flask runs somewhere else
        verification_link = f'http://127.0.0.1:5000/verify-email/{verification_token}'

        # Send welcome and verification emails
        try:
            send_welcome_and_verification_emails(email, name, verification_link)
        except Exception as e:
            return jsonify({
                'success': False,
                'message': 'Failed to send verification or welcome email',
                'error': str(e)
            }), 500

        return jsonify({
            'success': True,
            'message': 'User registered successfully. Please check your email to verify your account.',
            'show_country_selection': True,
            'user_id': new_user.id,
            'token_name': token_name
        }), 201

    except Exception as e:
        db.session.rollback()
        print(f"Registration error: {str(e)}")
        return jsonify({
            'success': False,
            'message': 'Server error during registration',
            'error': str(e)
        }), 500

@user_bp.route('/login', methods=['POST'])
def login():
    data = request.get_json()
    if data is None:
        return jsonify({'success': False, 'message': 'Invalid input'}), 400

    email = data.get('email')
    password = data.get('password')

    if not email or not password:
        return jsonify({'success': False, 'message': 'Email and password are required'}), 400

    user = User.query.filter_by(email=email).first()
    if not user:
        return jsonify({'success': False, 'message': 'User not found'}), 404

    if not user.is_verified:
        return jsonify({'error': 'Your email is not verified. Please verify your email first.'}), 403

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
    send_reset_email(user.email, reset_url)

    return jsonify({'success': True, 'message': 'Password reset email sent.'}), 200


@user_bp.route('/reset_password/<token>', methods=['POST'])
def reset_password(token):
    data = request.get_json()
    password = data.get('password')
    user_id = decode_token(token)

    if not user_id:
        return jsonify({'success': False, 'message': 'Invalid or expired token.'})

    user = User.query.get(user_id)
    if user:
        hashed_password = generate_password_hash(password, method='pbkdf2:sha256', salt_length=8)
        user.password = hashed_password
        user.is_verified = True
        db.session.commit()
        return jsonify({'success': True, 'message': 'Password reset successfully.'})
    else:
        return jsonify({'success': False, 'message': 'User not found.'})


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
                verification_link = 'http://127.0.0.1:5000/signin'
                send_welcome_and_verification_emails(email, name, verification_link)
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
def resend_verification_email():
    data = request.get_json()
    email = data.get('email')
    name = data.get('name')  

    if not email:
        return jsonify({'success': False, 'message': 'Email is required'}), 400  # If email is not provided

    # Check if the user exists
    user = User.query.filter_by(email=email).first()
    if not user:
        return jsonify({'success': False, 'message': 'User not found'}), 404

    # Generate new verification token
    verification_token = generate_verification_token(email)
    verification_link = f'http://127.0.0.1:5000/verify_email/{verification_token}'

    # Try sending the verification email
    try:
        send_welcome_and_verification_emails(email, name, verification_link)
        return jsonify({'success': True, 'message': 'Verification email resent successfully.'})
    except Exception as e:
        return jsonify({'success': False, 'message': 'Failed to resend verification email', 'error': str(e)}), 500



def _has_token(val):
    return bool(val and str(val).strip())

# @user_bp.route('/get_user_data', methods=['GET'])
# def get_user_data():
#     auth_header = request.headers.get('Authorization')
#     if not auth_header or not auth_header.startswith('Bearer '):
#         return jsonify({'error': 'Authorization token is missing or invalid'}), 401

#     token = auth_header.split(' ')[1]
#     try:
#         payload, user_id, member_id = get_effective_user_id_from_token(token)
#     except jwt.ExpiredSignatureError:
#         return jsonify({'error': 'Token has expired'}), 401
#     except jwt.InvalidTokenError:
#         return jsonify({'error': 'Invalid token'}), 401

#     user = User.query.filter_by(id=user_id).first()
#     if not user:
#         return jsonify({'error': 'User not found'}), 404

#     # ✅ Fetch amazon tokens row (can be None if not connected)
#     arow = amazon_user.query.filter_by(user_id=user_id).first()

#     spapi_connected = False
#     ads_connected = False

#     if arow:
#         spapi_connected = _has_token(arow.refresh_token)
#         ads_connected = _has_token(arow.amazon_ads_refresh_token)

#     # ✅ Your rule: amazon_user_exists True only if BOTH tokens exist
#     amazon_user_exists = bool(spapi_connected and ads_connected)

#     # ✅ Save in User table (optional but you asked “add column in User class and fill it”)
#     user.amazon_user_exists = amazon_user_exists
#     user.amazon_ads_exists = ads_connected

#     try:
#         db.session.commit()
#     except Exception as e:
#         db.session.rollback()
#         return jsonify({'error': 'Failed to update amazon flags', 'details': str(e)}), 500

#     return jsonify({
#         'name': user.name,
#         'company_name': user.company_name,
#         'brand_name': user.brand_name,
#         'email': user.email,
#         'phone_number': user.phone_number,
#         'annual_sales_range': user.annual_sales_range,
#         'password': user.password,
#         'marketplace_id': user.marketplace_id,
#         'country': user.country,
#         'homeCurrency': user.homeCurrency,
#         'target_sales': float(user.target_sales) if user.target_sales is not None else None,
#         'tax_id': user.tax_id,
#         'address': user.address,

#         # ✅ NEW FIELDS returned to frontend
#         'amazon_user_exists': user.amazon_user_exists,   # both tokens
#         'amazon_ads_exists': user.amazon_ads_exists,     # ads token only
#     }), 200


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

    # ✅ Identify if member login
    is_member = payload.get("is_member") is True
    member_id = payload.get("member_id")  
    marketplace_ids = payload.get("marketplace_ids")
    modules = payload.get("modules")
    countries = payload.get("countries")
    email = payload.get("email")


    member_name = None
    member_role = None

    # ✅ If member token, load member record (optional but useful)
    if is_member and member_id:
        m = Member.query.filter_by(id=int(member_id), owner_user_id=int(user_id)).first()
        if m:
            member_name = getattr(m, "member_name", None)
            member_role = getattr(m, "role", None)

    # ✅ Owner scoped user record
    user = User.query.filter_by(id=user_id).first()
    if not user:
        return jsonify({'error': 'User not found'}), 404

    # ✅ Fetch amazon tokens row (can be None if not connected)
    arow = amazon_user.query.filter_by(user_id=user_id).first()

    spapi_connected = False
    ads_connected = False

    if arow:
        spapi_connected = _has_token(arow.refresh_token)
        ads_connected = _has_token(arow.amazon_ads_refresh_token)

    amazon_user_exists = bool(spapi_connected and ads_connected)

    # ✅ Save flags
    user.amazon_user_exists = amazon_user_exists
    user.amazon_ads_exists = ads_connected

    try:
        db.session.commit()
    except Exception as e:
        db.session.rollback()
        return jsonify({'error': 'Failed to update amazon flags', 'details': str(e)}), 500

    # ✅ pick the correct email for FE display
    effective_email = email if is_member else user.email

    return jsonify({
        # identity
        "is_member": is_member,
        "user_id": int(user_id),
        "owner_user_id": int(user_id),
        "member_id": int(member_id) if member_id else None,

        "member_name": member_name,
        "member_role": member_role,

        # ✅ emails (no overwriting)
        "email": effective_email,                 # ✅ FE should use this
        "owner_email": user.email,                # always owner email
        "member_email": email if is_member else None,

        "marketplace_ids": marketplace_ids,
        "modules": modules,
        "countries": countries,

        # user fields
        "name": user.name,
        "company_name": user.company_name,
        "brand_name": user.brand_name,
        "phone_number": user.phone_number,
        "annual_sales_range": user.annual_sales_range,
        "password": user.password,
        "marketplace_id": user.marketplace_id,
        "country": user.country,
        "homeCurrency": user.homeCurrency,
        "target_sales": float(user.target_sales) if user.target_sales is not None else None,
        "tax_id": user.tax_id,
        "address": user.address,

        # amazon flags
        "amazon_user_exists": user.amazon_user_exists,
        "amazon_ads_exists": user.amazon_ads_exists,
    }), 200



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

    if not user.country:
        country_list = []
    else:
        country_list = [c.strip() for c in user.country.split(',')]  

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

    data = request.get_json()
    country = data.get('country')  # can be 'uk' or 'uk,us'
    annual_sales_range = data.get('annual_sales_range')
    brand_name = data.get('brand_name')
    company_name = data.get('company_name')
    homeCurrency = data.get('homeCurrency')

    if not country:
        return jsonify({'success': False, 'message': 'Country is required.'}), 400

    user = User.query.filter_by(id=user_id).first()
    if not user:
        return jsonify({'success': False, 'message': 'User not found.'}), 404

    user.country = country
    user.marketplace_id = compute_marketplace_ids_from_country(country)

    user.company_name = company_name
    user.brand_name = brand_name
    user.homeCurrency = homeCurrency
    if annual_sales_range:
        user.annual_sales_range = annual_sales_range

    db.session.commit()
    db.session.refresh(user)

    return jsonify({
        'success': True,
        'message': 'Sales data submitted successfully.',
        'marketplace_id': user.marketplace_id
    }), 201




# @user_bp.route('/verify-email/<token>', methods=['GET'])
# def verify_email(token):
#     try:
#         email = confirm_verification_token(token)
#         user = User.query.filter_by(email=email).first()
#         if not user:
#             return jsonify({'success': False, 'message': 'Invalid verification token.'})

#         # Update user's email verification status
#         user.is_verified = True
#         db.session.commit()

#         # Create user-specific database after successful email verification
        
#         create_user_session(db_url)

#         # Store user session after verification
#         session['user_id'] = user.id

#         return redirect('http://localhost:3000/verify-email?status=success')
#     except Exception as e:
#         print(f"Email verification error: {str(e)}")
#         return jsonify({'success': False, 'message': 'Invalid or expired verification token.', 'error': str(e)})
    
@user_bp.route('/verify-email/<token>', methods=['GET'])
def verify_email(token):
    try:
        email = confirm_verification_token(token)
        email = (email or "").strip().lower()

        if not email:
            return redirect('http://localhost:3000/verify-email?status=failed')

        user = User.query.filter_by(email=email).first()
        if not user:
            return redirect('http://localhost:3000/verify-email?status=failed')

        # ✅ mark verified (idempotent)
        user.is_verified = True
        db.session.commit()

        # ✅ Create user-specific database after successful email verification
        try:
            create_user_session(db_url)
        except Exception as e:
            print(f"create_user_session error: {e}")

        # Store user session after verification
        session['user_id'] = user.id

        # ✅ redirect to frontend page that you already have
        return redirect(f'http://localhost:3000/verify-email?status=success&email={email}')

    except Exception as e:
        print(f"Email verification error: {str(e)}")
        return redirect('http://localhost:3000/verify-email?status=failed')


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

    # ---------- SAFE FIELD UPDATES ----------
    user.name = data.get('name', user.name) 
    user.email = data.get('email', user.email)
    user.phone_number = data.get('phone_number', user.phone_number)
    user.annual_sales_range = data.get('annual_sales_range', user.annual_sales_range)
    user.company_name = data.get('company_name', user.company_name)
    user.brand_name = data.get('brand_name', user.brand_name)
    user.homeCurrency = data.get('homeCurrency', user.homeCurrency)

    # ---------- TAX ID (JSON SAFE) ----------
    tax_val = data.get('tax_id', user.tax_id)

    if isinstance(tax_val, str):
        try:
            tax_val = json.loads(tax_val)
        except Exception:
            return jsonify({'error': 'Invalid tax_id format'}), 400

    user.tax_id = tax_val

    # ---------- ADDRESS (JSON SAFE) ----------
    addr_val = data.get('address', user.address)

    if isinstance(addr_val, str):
        try:
            addr_val = json.loads(addr_val)
        except Exception:
            return jsonify({'error': 'Invalid address format'}), 400

    user.address = addr_val

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
        user.country = new_country
        user.marketplace_id = compute_marketplace_ids_from_country(new_country)

    # ---------- TARGET SALES (VALIDATED) ----------
    target_sales = data.get('target_sales')
    if target_sales is not None:
        try:
            user.target_sales = float(target_sales)
        except (TypeError, ValueError):
            return jsonify({'error': 'target_sales must be a number'}), 400

    # ---------- COMMIT ----------
    try:
        db.session.commit()
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
        user_engine = create_engine(db_url)
        user_session = create_user_session(db_url)
        admin_engine = create_engine(db_url1)
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


        conn = user_engine.connect()
        query = user_specific_table.select()
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
                # add more if needed
            }

            # Convert frontend country to DB country
            normalized_country = COUNTRY_MAP.get(country.lower(), country)


            queery = admin_session.query(Category).filter_by(
                country=normalized_country,
                category=product_group_cleaned
            )
            
            Category_obj = queery.first()

            if not Category_obj:
                # Extract first word before special characters like & / - etc.
                first_word = product_group_cleaned.split('&')[0].split('/')[0].split('-')[0].strip().upper()
    
            
                queery = admin_session.query(Category).filter_by(
                    country=normalized_country.strip(),
                    category=first_word
                )

            # Add conditional filtering based on price_from and price_to
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
                print(f"No matching Category found for country {country_value} and Category {product_group_value}")  # Debugging statement

        conn.commit()  # Ensure all updates are committed

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
        with user_engine.connect() as conn:
            for asin, (barcode, price) in asin_mapping.items():
                update_stmt = text(f"""
                    UPDATE {table_name}
                    SET product_barcode = :barcode, sku_cost_price = :price
                    WHERE asin = :asin AND user_id = :user_id
                """)
                conn.execute(update_stmt, {"barcode": barcode, "price": price, "asin": asin, "user_id": user_id})

            conn.commit()

        new_profile = CountryProfile(
            user_id=user_id,
            country=country, 
            marketplace=marketplace,
            transit_time=transit_time,
            stock_unit=stock_unit

        )
        db.session.add(new_profile)
        db.session.commit()

        return jsonify({'message': 'New profile created successfully','profile_id': new_profile.id,'country':new_profile.country}), 201  # Created status code

    return jsonify({'message': 'File successfully uploaded and data added to the database.'})
    
