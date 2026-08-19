from flask import Blueprint, request, jsonify, current_app
from werkzeug.security import generate_password_hash, check_password_hash
from datetime import datetime, timedelta
from config import Config
SECRET_KEY = Config.SECRET_KEY
from app import db, mail
from app.models.user_models import SuperAdmin, UserAdmin, User
from flask_mail import Message
import jwt
import random
import string
import os

admin_bp = Blueprint('admin', __name__)


otp_store = {}

def require_superadmin_token():
    auth_header = request.headers.get('Authorization')

    if not auth_header or not auth_header.startswith('Bearer '):
        return None, (jsonify({'message': 'Authorization token required'}), 401)

    token = auth_header.split(' ')[1]

    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=['HS256'])
    except jwt.ExpiredSignatureError:
        return None, (jsonify({'message': 'Token expired'}), 401)
    except jwt.InvalidTokenError:
        return None, (jsonify({'message': 'Invalid token'}), 401)

    if not payload.get('is_superadmin'):
        return None, (jsonify({'message': 'SuperAdmin access required'}), 403)

    return payload, None

def generate_otp():
    """Generate a 6-digit OTP"""
    return ''.join(random.choices(string.digits, k=6))

def send_otp_email_superadmin(email, otp, purpose="verification"):
    """Send OTP via email"""
    try:
        if purpose == "setup":
            subject = "Phormula SuperAdmin - First Time Setup"
            body = f"""
            Welcome to Phormula SuperAdmin Panel!
            
            This is your first time accessing the system. Please use the following OTP to set up your password:
            
            OTP: {otp}
            
            This OTP will expire in 10 minutes.
            
            Best regards,
            Phormula Team
            """
        elif purpose == "reset":
            subject = "Phormula SuperAdmin - Password Reset"
            body = f"""
            You have requested to reset your SuperAdmin password.
            
            Please use the following OTP to reset your password:
            
            OTP: {otp}
            
            This OTP will expire in 10 minutes.
            
            If you didn't request this, please ignore this email.
            
            Best regards,
            Phormula Team
            """
        
        msg = Message(
            subject=subject,
            sender=current_app.config['MAIL_DEFAULT_SENDER'],
            recipients=[email],
            body=body
        )
        mail.send(msg)
        return True
    except Exception as e:
        print(f"Failed to send OTP email: {e}")
        return False



def send_otp_email_admin(email, otp, purpose="verification"):
    """Send OTP via email for Admin"""
    try:
        if purpose == "reset":
            subject = "Phormula Admin - Password Reset"
            body = f"""
            You have requested to reset your Admin password.
            
            Please use the following OTP to reset your password:
            
            OTP: {otp}
            
            This OTP will expire in 10 minutes.
            
            If you didn't request this, please ignore this email.
            
            Best regards,
            Phormula Team
            """
        
        msg = Message(
            subject=subject,
            sender=current_app.config['MAIL_DEFAULT_SENDER'],
            recipients=[email],
            body=body
        )
        mail.send(msg)
        return True
    except Exception as e:
        print(f"Failed to send OTP email: {e}")
        return False


# === CHECK SUPERADMIN STATUS ===
@admin_bp.route('/superadmin_status', methods=['GET'])
def check_superadmin_status():
    """Check if SuperAdmin account exists and is set up"""
    superadmin_email = os.getenv('SUPERADMIN_EMAIL')
    
    if not superadmin_email:
        return jsonify({'message': 'SuperAdmin email not configured'}), 500
    
    superadmin = SuperAdmin.query.filter_by(email=superadmin_email).first()
    
    if not superadmin:
        return jsonify({
            'exists': False,
            'needs_setup': True,
            'email': superadmin_email
        }), 200
    
    return jsonify({
        'exists': True,
        'needs_setup': not superadmin.is_verified,
        'is_verified': superadmin.is_verified
    }), 200



# === SUPERADMIN FIRST TIME SETUP - REQUEST OTP ===
@admin_bp.route('/superadmin_setup_otp', methods=['POST'])
def superadmin_setup_otp():
    """Send OTP for first-time SuperAdmin setup"""
    try:
        data = request.get_json()
        entered_email = data.get('email')

        superadmin_email = os.getenv('SUPERADMIN_EMAIL')
        if not superadmin_email or entered_email != superadmin_email:
            return jsonify({'message': 'SuperAdmin email not configured or does not match'}), 400
        
        # Check if SuperAdmin already exists and is verified
        existing_superadmin = SuperAdmin.query.filter_by(email=superadmin_email).first()
        if existing_superadmin and existing_superadmin.is_verified:
            return jsonify({'message': 'SuperAdmin already set up. Please use login instead.'}), 400
        
        # Generate and store OTP
        otp = generate_otp()
        otp_key = f"setup_{superadmin_email}"
        otp_store[otp_key] = {
            'otp': otp,
            'expires': datetime.utcnow() + timedelta(minutes=10),
            'email': superadmin_email
        }
        
        # Send OTP email
        if send_otp_email_superadmin(superadmin_email, otp, "setup"):
            return jsonify({
                'message': 'OTP sent to your email',
                'email': superadmin_email
            }), 200
        else:
            return jsonify({'message': 'Failed to send OTP email'}), 500
    
    except Exception as e:
        return jsonify({'message': f'Server error: {str(e)}'}), 500




# === SUPERADMIN FIRST TIME SETUP - VERIFY OTP AND CREATE PASSWORD ===
@admin_bp.route('/superadmin_setup', methods=['POST'])
def superadmin_setup():
    """Complete SuperAdmin setup with OTP verification and password creation"""
    try:
        data = request.get_json()
        
        if not data or not data.get('otp') or not data.get('password'):
            return jsonify({'message': 'OTP and password are required'}), 400
        
        superadmin_email = os.getenv('SUPERADMIN_EMAIL')
        if not superadmin_email:
            return jsonify({'message': 'SuperAdmin email not configured'}), 500
        
        otp_key = f"setup_{superadmin_email}"
        
        # Verify OTP
        if otp_key not in otp_store:
            return jsonify({
                'message': 'No OTP request found or OTP expired. Please request a new OTP.',
                'debug_key': otp_key,
                'debug_store_keys': list(otp_store.keys())
            }), 400
        
        stored_otp_data = otp_store[otp_key]
        current_time = datetime.utcnow()
        
        if current_time > stored_otp_data['expires']:
            del otp_store[otp_key]
            return jsonify({'message': 'OTP expired. Please request a new OTP.'}), 400
        
        received_otp = str(data['otp']).strip()
        stored_otp = str(stored_otp_data['otp']).strip()
        
        if received_otp != stored_otp:
            return jsonify({
                'message': 'Invalid OTP. Please check and try again.',
                'debug_received': received_otp,
                'debug_stored': stored_otp
            }), 400
        
        # Check password strength
        if len(data['password']) < 8:
            return jsonify({'message': 'Password must be at least 8 characters long'}), 400
        
        # Create or update SuperAdmin
        superadmin = SuperAdmin.query.filter_by(email=superadmin_email).first()
        
        if superadmin:
            # Update existing record
            superadmin.password = generate_password_hash(data['password'], method='pbkdf2:sha256')
            superadmin.is_verified = True
            superadmin.is_superadmin = True
        else:
            # Create new SuperAdmin
            superadmin = SuperAdmin(
                email=superadmin_email,
                password=generate_password_hash(data['password'], method='pbkdf2:sha256'),
                is_superadmin=True,
                is_verified=True
            )
            db.session.add(superadmin)
        
        db.session.commit()
        # Clean up OTP
        del otp_store[otp_key]
        
        return jsonify({'message': 'SuperAdmin setup completed successfully'}), 201
        
    except Exception as e:
        db.session.rollback()
        return jsonify({'message': f'Failed to create SuperAdmin account: {str(e)}'}), 500



# === SUPERADMIN LOGIN ===
@admin_bp.route('/superadmin_login', methods=['POST'])
def superadmin_login():
    try:
        data = request.get_json()
        
        if not data or not data.get('email') or not data.get('password'):
            return jsonify({'message': 'Missing email or password'}), 400

        superadmin = SuperAdmin.query.filter_by(email=data['email']).first()

        if not superadmin or not check_password_hash(superadmin.password, data['password']):
            return jsonify({'message': 'Invalid email or password'}), 401

        if not superadmin.is_verified:
            return jsonify({'message': 'Superadmin not verified'}), 403

        # Set is_superadmin to True
        superadmin.is_superadmin = True
        db.session.commit()

        token = jwt.encode({
            'admin_id': superadmin.id,
            'is_superadmin': True,
            'exp': datetime.utcnow() + timedelta(hours=24)
        }, current_app.config['SECRET_KEY'])

        return jsonify({
            'message': 'Superadmin login successful',
            'token': token,
            'is_superadmin': True
        }), 200
    
    except Exception as e:
        return jsonify({'message': f'Login failed: {str(e)}'}), 500



# === SUPERADMIN PASSWORD RESET - REQUEST OTP ===
@admin_bp.route('/superadmin_reset_otp', methods=['POST'])
def superadmin_reset_otp():
    """Send OTP for SuperAdmin password reset"""
    try:
        data = request.get_json()
        
        if not data or not data.get('email'):
            return jsonify({'message': 'Email is required'}), 400
        
        email = data['email']
        superadmin_email = os.getenv('SUPERADMIN_EMAIL')
        
        # Verify the email matches the configured SuperAdmin email
        if email != superadmin_email:
            return jsonify({'message': 'Invalid email address'}), 400
        
        # Check if SuperAdmin exists
        superadmin = SuperAdmin.query.filter_by(email=email).first()
        if not superadmin:
            return jsonify({'message': 'SuperAdmin account not found'}), 404
        
        # Generate and store OTP
        otp = generate_otp()
        otp_key = f"reset_{email}"
        otp_store[otp_key] = {
            'otp': otp,
            'expires': datetime.utcnow() + timedelta(minutes=10),
            'email': email,
            'admin_id': superadmin.id
        }
        
        # Send OTP email
        if send_otp_email_superadmin(email, otp, "reset"):
            return jsonify({
                'message': 'Password reset OTP sent to your email',
                'email': email
            }), 200
        else:
            return jsonify({'message': 'Failed to send OTP email'}), 500
    
    except Exception as e:
        return jsonify({'message': f'Server error: {str(e)}'}), 500


# === SUPERADMIN PASSWORD RESET - VERIFY OTP AND UPDATE PASSWORD ===
@admin_bp.route('/superadmin_reset_password', methods=['POST'])
def superadmin_reset_password():
    """Reset SuperAdmin password with OTP verification"""
    try:
        data = request.get_json()
        
        if not data or not data.get('email') or not data.get('otp') or not data.get('new_password'):
            return jsonify({'message': 'Email, OTP, and new password are required'}), 400
        
        email = data['email']
        otp_key = f"reset_{email}"
        
        # Verify OTP
        if otp_key not in otp_store:
            return jsonify({'message': 'No password reset request found'}), 400
        
        stored_otp_data = otp_store[otp_key]
        if datetime.utcnow() > stored_otp_data['expires']:
            del otp_store[otp_key]
            return jsonify({'message': 'OTP expired'}), 400
        
        if str(stored_otp_data['otp']).strip() != str(data['otp']).strip():
            return jsonify({'message': 'Invalid OTP'}), 400
        
        # Check password strength
        if len(data['new_password']) < 8:
            return jsonify({'message': 'Password must be at least 8 characters long'}), 400
        
        # Update password
        superadmin = SuperAdmin.query.get(stored_otp_data['admin_id'])
        if not superadmin:
            return jsonify({'message': 'SuperAdmin account not found'}), 404
        
        superadmin.password = generate_password_hash(data['new_password'], method='pbkdf2:sha256')
        
        db.session.commit()
        # Clean up OTP
        del otp_store[otp_key]
        
        return jsonify({'message': 'Password reset successfully'}), 200
    
    except Exception as e:
        db.session.rollback()
        return jsonify({'message': f'Failed to reset password: {str(e)}'}), 500


# === SUPERADMIN CHANGE PASSWORD (for logged-in) ===
@admin_bp.route('/superadmin_change_password', methods=['POST'])
def superadmin_change_password():
    """Change SuperAdmin password when logged in"""
    try:
        data = request.get_json()
        
        if not data or not data.get('current_password') or not data.get('new_password'):
            return jsonify({'message': 'Current password and new password are required'}), 400
        
        # Get token from Authorization header
        auth_header = request.headers.get('Authorization')
        if not auth_header or not auth_header.startswith('Bearer '):
            return jsonify({'message': 'Authorization token required'}), 401
        
        token = auth_header.split(' ')[1]
        
        try:
            payload = jwt.decode(token, SECRET_KEY, algorithms=['HS256'])
            admin_id = payload['admin_id']
            is_superadmin = payload.get('is_superadmin', False)
            
            if not is_superadmin:
                return jsonify({'message': 'SuperAdmin access required'}), 403
            
        except jwt.ExpiredSignatureError:
            return jsonify({'message': 'Token expired'}), 401
        except jwt.InvalidTokenError:
            return jsonify({'message': 'Invalid token'}), 401
        
        # Get SuperAdmin
        superadmin = SuperAdmin.query.get(admin_id)
        if not superadmin:
            return jsonify({'message': 'SuperAdmin not found'}), 404
        
        # Verify current password
        if not check_password_hash(superadmin.password, data['current_password']):
            return jsonify({'message': 'Current password is incorrect'}), 400
        
        # Check new password strength
        if len(data['new_password']) < 8:
            return jsonify({'message': 'New password must be at least 8 characters long'}), 400
        
        # Update password
        superadmin.password = generate_password_hash(data['new_password'], method='pbkdf2:sha256')
        
        db.session.commit()
        return jsonify({'message': 'Password changed successfully'}), 200
    
    except Exception as e:
        db.session.rollback()
        return jsonify({'message': f'Failed to change password: {str(e)}'}), 500



# === SUPERADMIN LOGOUT ===
@admin_bp.route('/superadmin_logout', methods=['POST'])
def superadmin_logout():
    try:
        auth_header = request.headers.get('Authorization')
        if not auth_header or not auth_header.startswith('Bearer '):
            return jsonify({'message': 'Authorization token required'}), 401

        token = auth_header.split(' ')[1]
        decoded = jwt.decode(token, current_app.config['SECRET_KEY'], algorithms=["HS256"])
        superadmin_id = decoded.get('admin_id')

        # Update is_superadmin flag to False
        superadmin = SuperAdmin.query.get(superadmin_id)
        if superadmin:
            superadmin.is_superadmin = False
            db.session.commit()

        return jsonify({'message': 'SuperAdmin logged out successfully'}), 200
    
    except Exception as e:
        return jsonify({'message': f'Logout failed: {str(e)}'}), 500




# === SUPERADMIN CREATES ADMIN ===
@admin_bp.route('/create_admin', methods=['POST'])
def create_admin():
    try:
        _, auth_error = require_superadmin_token()
        if auth_error:
            return auth_error

        data = request.get_json()

        if not data or not data.get('email') or not data.get('password'):
            return jsonify({'message': 'Missing required fields'}), 400

        email = data['email']

        # Check if email already exists in SuperAdmin table
        existing_superadmin = SuperAdmin.query.filter_by(email=email).first()
        if existing_superadmin:
            return jsonify({'message': 'Email is already registered as a SuperAdmin'}), 409

        # Check if email already exists in UserAdmin table
        existing_admin = UserAdmin.query.filter_by(email=email).first()
        if existing_admin:
            return jsonify({'message': 'Admin with this email already exists'}), 409

        hashed_password = generate_password_hash(data['password'], method='pbkdf2:sha256')

        new_admin = UserAdmin(
            email=email,
            password=hashed_password,
            is_admin=True,
            is_superadmin=False,  # Make this explicit
            is_verified=True
        )


        db.session.add(new_admin)
        db.session.commit()

        return jsonify({
            'message': 'Admin created successfully',
            'admin': {
                'id': new_admin.id,
                'email': new_admin.email,
                'is_admin': new_admin.is_admin,
                'is_superadmin': new_admin.is_superadmin,
                'is_verified': new_admin.is_verified,
            }
        }), 201

    except Exception as e:
        db.session.rollback()
        return jsonify({'message': f'Failed to create admin: {str(e)}'}), 500


# === SUPERADMIN DELETE ADMIN BY EMAIL ===
@admin_bp.route('/delete_admin', methods=['DELETE'])
def delete_admin_by_email():
    try:
        _, auth_error = require_superadmin_token()
        if auth_error:
            return auth_error

        data = request.get_json()

        if not data or not data.get('email'):
            return jsonify({'message': 'Admin email is required'}), 400

        email = data['email']

        # Keep backward compatibility: this endpoint was historically used for
        # user-brand accounts and admin accounts despite the route name.
        user = User.query.filter_by(email=email).first()
        user_admin = UserAdmin.query.filter_by(email=email).first()

        if not user and not user_admin:
            return jsonify({'message': 'Account not found'}), 404

        if user:
            db.session.delete(user)

        if user_admin:
            db.session.delete(user_admin)

        db.session.commit()

        return jsonify({'message': 'Account deleted successfully'}), 200

    except Exception as e:
        db.session.rollback()
        return jsonify({'message': f'Failed to delete account: {str(e)}'}), 500



# === ADMIN LOGIN ===
@admin_bp.route('/admin_login', methods=['POST'])
def admin_login():
    try:
        data = request.get_json()

        if not data or not data.get('email') or not data.get('password'):
            return jsonify({'message': 'Missing email or password'}), 400

        admin = UserAdmin.query.filter_by(email=data['email']).first()

        if not admin or not check_password_hash(admin.password, data['password']):
            return jsonify({'message': 'Invalid email or password'}), 401

        if not admin.is_verified:
            return jsonify({'message': 'Admin account not verified'}), 403

        token = jwt.encode({
            'admin_id': admin.id,
            'is_superadmin': False,
            'exp': datetime.utcnow() + timedelta(hours=24)
        }, current_app.config['SECRET_KEY'])

        return jsonify({
            'message': 'Admin login successful',
            'token': token,
            'is_superadmin': False,
            'is_admin': admin.is_admin
        }), 200
    
    except Exception as e:
        return jsonify({'message': f'Login failed: {str(e)}'}), 500


# === ADMIN PASSWORD RESET - REQUEST OTP ===
@admin_bp.route('/admin_reset_otp', methods=['POST'])
def admin_reset_otp():
    """Send OTP for Admin password reset"""
    try:
        data = request.get_json()
        
        if not data or not data.get('email'):
            return jsonify({'message': 'Email is required'}), 400
        
        email = data['email']
        
        # Check if Admin exists
        admin = UserAdmin.query.filter_by(email=email).first()
        if not admin:
            return jsonify({'message': 'Admin account not found'}), 404
        
        # Generate and store OTP
        otp = generate_otp()
        otp_key = f"reset_{email}"
        otp_store[otp_key] = {
            'otp': otp,
            'expires': datetime.utcnow() + timedelta(minutes=10),
            'email': email,
            'admin_id': admin.id
        }
        
        # Send OTP email
        if send_otp_email_admin(email, otp, "reset"):
            return jsonify({
                'message': 'Password reset OTP sent to your email',
                'email': email
            }), 200
        else:
            return jsonify({'message': 'Failed to send OTP email'}), 500
    
    except Exception as e:
        return jsonify({'message': f'Server error: {str(e)}'}), 500


# === ADMIN PASSWORD RESET - VERIFY OTP AND UPDATE PASSWORD ===
@admin_bp.route('/admin_reset_password', methods=['POST'])
def admin_reset_password():
    """Reset Admin password with OTP verification"""
    try:
        data = request.get_json()
        
        if not data or not data.get('email') or not data.get('otp') or not data.get('new_password'):
            return jsonify({'message': 'Email, OTP, and new password are required'}), 400
        
        email = data['email']
        otp_key = f"reset_{email}"
        
        # Verify OTP
        if otp_key not in otp_store:
            return jsonify({'message': 'No password reset request found'}), 400
        
        stored_otp_data = otp_store[otp_key]
        if datetime.utcnow() > stored_otp_data['expires']:
            del otp_store[otp_key]
            return jsonify({'message': 'OTP expired'}), 400
        
        if str(stored_otp_data['otp']).strip() != str(data['otp']).strip():
            return jsonify({'message': 'Invalid OTP'}), 400
        
        # Check password strength
        if len(data['new_password']) < 8:
            return jsonify({'message': 'Password must be at least 8 characters long'}), 400
        
        # Update password
        admin = UserAdmin.query.get(stored_otp_data['admin_id'])
        if not admin:
            return jsonify({'message': 'Admin account not found'}), 404
        
        admin.password = generate_password_hash(data['new_password'], method='pbkdf2:sha256')
        
        db.session.commit()
        # Clean up OTP
        del otp_store[otp_key]
        
        return jsonify({'message': 'Password reset successfully'}), 200
    
    except Exception as e:
        db.session.rollback()
        return jsonify({'message': f'Failed to reset password: {str(e)}'}), 500


# === ADMIN CHANGE PASSWORD (for logged-in) ===
@admin_bp.route('/admin_change_password', methods=['POST'])
def admin_change_password():
    """Change Admin password when logged in"""
    try:
        data = request.get_json()
        
        if not data or not data.get('current_password') or not data.get('new_password'):
            return jsonify({'message': 'Current password and new password are required'}), 400
        
        # Get token from Authorization header
        auth_header = request.headers.get('Authorization')
        if not auth_header or not auth_header.startswith('Bearer '):
            return jsonify({'message': 'Authorization token required'}), 401
        
        token = auth_header.split(' ')[1]
        
        try:
            payload = jwt.decode(token, SECRET_KEY, algorithms=['HS256'])
            
            # Check if 'admin_id' exists in payload
            if 'admin_id' not in payload:
                return jsonify({'message': 'Invalid token: missing admin ID'}), 401
            
            admin_id = payload['admin_id']
            is_superadmin = payload.get('is_superadmin', False)
            
            
        except jwt.ExpiredSignatureError:
            return jsonify({'message': 'Token expired'}), 401
        except jwt.InvalidTokenError as e:
            return jsonify({'message': 'Invalid token'}), 401
        
        # Get Admin using the correct database session
        admin = UserAdmin.query.get(admin_id)
        if not admin:
            return jsonify({'message': 'Admin not found'}), 404
        
        # Verify current password
        if not check_password_hash(admin.password, data['current_password']):
            return jsonify({'message': 'Current password is incorrect'}), 400
        
        # Check new password strength
        if len(data['new_password']) < 8:
            return jsonify({'message': 'New password must be at least 8 characters long'}), 400
        
        # Update password
        admin.password = generate_password_hash(data['new_password'], method='pbkdf2:sha256')
        
        # Commit to the correct database
        db.session.commit()
        return jsonify({'message': 'Password changed successfully'}), 200
    
    except Exception as e:
        db.session.rollback()
        return jsonify({'message': f'Failed to change password: {str(e)}'}), 500


# === ADMIN LOGOUT ===
@admin_bp.route('/admin_logout', methods=['POST'])
def admin_logout():
    """Logout Admin by invalidating the token"""
    try:
        # Invalidate the token (in production, use a blacklist or similar mechanism)
        auth_header = request.headers.get('Authorization')
        if not auth_header or not auth_header.startswith('Bearer '):
            return jsonify({'message': 'Authorization token required'}), 401
        
        token = auth_header.split(' ')[1]
        
        # Here you would typically add the token to a blacklist
        return jsonify({'message': 'Admin logged out successfully'}), 200
    
    except Exception as e:
        return jsonify({'message': f'Logout failed: {str(e)}'}), 500

