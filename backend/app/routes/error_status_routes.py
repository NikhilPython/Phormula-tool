from flask import Blueprint, request, jsonify 
from flask_mail import Message
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from app.utils.token_utils import get_effective_user_id_from_token
import jwt
import os
import base64
from datetime import datetime 
import pandas as pd
from config import basedir
from config import Config
SECRET_KEY = Config.SECRET_KEY
from app.models.user_models import User
from app.models.user_models import CountryProfile
from app import db, mail  
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()
db_url = os.getenv('DATABASE_URL')

error_status_bp = Blueprint('error_status_bp', __name__)

def send_error_status_notification_email(email, error_details=None):
    """Send error status notification email to user"""
    try:
        # Determine subject and content based on error details
        if error_details:
            subject = f'Error Status Update - Action Required'
            error_section = f"""
            <div style="background-color: #fff3cd; padding: 20px; border-radius: 8px; margin: 20px 0; border-left: 4px solid #ffc107;">
                <h3 style="color: #856404; font-size: 16px; margin: 0 0 10px 0;">Error Details:</h3>
                <p style="font-size: 14px; line-height: 1.6; color: #856404; margin: 0;">
                    {error_details}
                </p>
            </div>
            """
            action_message = "Please review the error details above and take appropriate action to resolve the issue."
        else:
            subject = 'Error Status Monitoring - System Update'
            error_section = """
            <div style="background-color: #d4edda; padding: 20px; border-radius: 8px; margin: 20px 0; border-left: 4px solid #28a745;">
                <h3 style="color: #155724; font-size: 16px; margin: 0 0 10px 0;">System Status:</h3>
                <p style="font-size: 14px; line-height: 1.6; color: #155724; margin: 0;">
                    All systems are operating normally. Error monitoring is active and functioning properly.
                </p>
            </div>
            """
            action_message = "No action required at this time. We'll continue monitoring your account for any issues."

        msg = Message(
            subject, 
            sender=("Phormula Care Team", "care@phormula.io"),
            recipients=[email]
        )
        
        msg.html = f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>Error Status Notification</title>
</head>
<body style="font-family: 'Lato', Arial, sans-serif; background-color: #f4f4f4; padding: 20px; margin: 0;">
    <div style="max-width: 600px; margin: 0 auto; background-color: #fff; padding: 30px; border-radius: 8px; border: 2px solid #5EA68E; box-shadow: 0 0 20px rgba(0, 0, 0, 0.1);">
        <img src="https://i.postimg.cc/43T3k86Z/logo.png" alt="Phormula Logo" style="width: 200px; height: auto; display: block; margin: 0 auto 20px;" />
        
        <h2 style="color: #5EA68E; font-size: 24px; font-weight: 600; text-align: center; margin-bottom: 20px;">
            {'Error Status Alert' if error_details else 'System Status Update'}
        </h2>
        
        <p style="font-size: 14px; line-height: 1.6; color: #555;">Hello,</p>
        
        <p style="font-size: 14px; line-height: 1.6; color: #555;">
            {'We detected an issue that requires your attention.' if error_details else 'This is an automated status update from your Phormula account.'}
        </p>
        
        {error_section}
        
        <p style="font-size: 14px; line-height: 1.6; color: #555;">{action_message}</p>
        
        <div style="text-align: center; margin: 30px 0;">
            <a href="https://phormula.io/dashboard" style="background-color: #5EA68E; color: white; padding: 12px 30px; text-decoration: none; border-radius: 6px; font-weight: bold; display: inline-block;">
                View Dashboard
            </a>
        </div>
        
        <p style="font-size: 14px; color: #555;">If you need assistance, please contact our support team at <a href="mailto:care@phormula.io" style="color: #5EA68E; text-decoration: none;">care@phormula.io</a></p>
        
        <p style="font-size: 14px; color: #555; margin-top: 30px;">Best regards, <br>The Phormula Team</p>
        
        <div style="text-align: center; margin-top: 30px; padding-top: 20px; border-top: 1px solid #eee;">
            <p style="font-size: 12px; color: #999;">Automated system notification - {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}</p>
        </div>
    </div>
</body>
</html>
        """
        
        # Send the notification email
        mail.send(msg)
        print(f"Error status notification email sent successfully to {email}")
        return True
        
    except Exception as e:
        print(f"Failed to send error status notification email to {email}: {e}")
        return False

@error_status_bp.route('/error_status_email', methods=['POST', 'OPTIONS'])
def error_status_email_notification():
    """Handle error status notification requests"""
    
    # Handle CORS preflight request
    if request.method == 'OPTIONS':
        response = jsonify({'status': 'success'})
        response.headers.add('Access-Control-Allow-Origin', '*')
        response.headers.add('Access-Control-Allow-Headers', 'Content-Type,Authorization')
        response.headers.add('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE,OPTIONS')
        return response
    
    try:
        # Extract user information from JWT token
        auth_header = request.headers.get('Authorization')
        user_email = None
        
        if auth_header and auth_header.startswith('Bearer '):
            token = auth_header.split(' ')[1]
            try:
                payload, user_id = get_effective_user_id_from_token(token)
                # Get user email from database
                user = User.query.get(user_id)
                if user:
                    user_email = user.email
                else:
                    response = jsonify({'error': 'User not found'})
                    response.headers.add('Access-Control-Allow-Origin', '*')
                    response.headers.add('Access-Control-Allow-Headers', 'Content-Type,Authorization')
                    response.headers.add('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE,OPTIONS')
                    return response, 404
            except jwt.ExpiredSignatureError:
                response = jsonify({'error': 'Token has expired'})
                response.headers.add('Access-Control-Allow-Origin', '*')
                response.headers.add('Access-Control-Allow-Headers', 'Content-Type,Authorization')
                response.headers.add('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE,OPTIONS')
                return response, 401
            except jwt.InvalidTokenError:
                response = jsonify({'error': 'Invalid token'})
                response.headers.add('Access-Control-Allow-Origin', '*')
                response.headers.add('Access-Control-Allow-Headers', 'Content-Type,Authorization')
                response.headers.add('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE,OPTIONS')
                return response, 401
        else:
            response = jsonify({'error': 'Authorization token required'})
            response.headers.add('Access-Control-Allow-Origin', '*')
            response.headers.add('Access-Control-Allow-Headers', 'Content-Type,Authorization')
            response.headers.add('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE,OPTIONS')
            return response, 401
        
        # Get error details from request body (optional)
        data = request.get_json() or {}
        error_details = data.get('error_details')
        
        # Send error status notification email
        if send_error_status_notification_email(user_email, error_details):
            response_data = {
                'status': 'success',
                'message': 'Error status notification email sent successfully!',
                'email': user_email
            }
            response = jsonify(response_data)
        else:
            response_data = {
                'status': 'error',
                'message': 'Failed to send error status notification email'
            }
            response = jsonify(response_data)
            response.status_code = 500
            
        # Add CORS headers
        response.headers.add('Access-Control-Allow-Origin', '*')
        response.headers.add('Access-Control-Allow-Headers', 'Content-Type,Authorization')
        response.headers.add('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE,OPTIONS')
        
        return response
        
    except Exception as e:
        response = jsonify({
            'status': 'error',
            'message': 'Internal server error'
        })
        response.status_code = 500
        
        response.headers.add('Access-Control-Allow-Origin', '*')
        response.headers.add('Access-Control-Allow-Headers', 'Content-Type,Authorization')
        response.headers.add('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE,OPTIONS')
        
        return response