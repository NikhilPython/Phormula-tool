from flask import Blueprint, request, jsonify 
from flask_mail import Message
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import jwt
import os
import base64
from datetime import datetime 
import pandas as pd
from config import basedir
from config import Config
SECRET_KEY = Config.SECRET_KEY
#UPLOAD_FOLDER = Config.UPLOAD_FOLDER
from app.models.user_models import User
from app.models.user_models import CountryProfile
from app import db, mail  
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()
db_url = os.getenv('DATABASE_URL', 'postgresql://postgres:password@localhost:5432/phormula')

fba_bp = Blueprint('fba_bp', __name__)

def send_fba_notification_email(email):
    """Send FBA feature notification email to user"""
    try:
        msg = Message(
            'FBA Fees Feature - Coming Soon!', 
            sender=("Phormula Care Team", "care@phormula.io"),
            recipients=[email]
        )
        
        msg.html = f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>FBA Fees Feature Notification</title>
</head>
<body style="font-family: 'Lato', Arial, sans-serif; background-color: #f4f4f4; padding: 20px; margin: 0;">
    <div style="max-width: 600px; margin: 0 auto; background-color: #fff; padding: 30px; border-radius: 8px; border: 2px solid #5EA68E; box-shadow: 0 0 20px rgba(0, 0, 0, 0.1);">
        <img src="https://i.postimg.cc/43T3k86Z/logo.png" alt="Phormula Logo" style="width: 200px; height: auto; display: block; margin: 0 auto 20px;" />
        
        <h2 style="color: #5EA68E; font-size: 24px; font-weight: 600; text-align: center; margin-bottom: 20px;">FBA Fees Feature - Coming Soon!</h2>
        
        <p style="font-size: 14px; line-height: 1.6; color: #555;">Hey {email},</p>
        
        <p style="font-size: 14px; line-height: 1.6; color: #555;">Thank you for your interest in our upcoming FBA Fees Reconciliation feature!</p>
        
        <div style="background-color: #f8f9fa; padding: 20px; border-radius: 8px; margin: 20px 0; border-left: 4px solid #5EA68E;">
            <h3 style="color: #37455F; font-size: 16px; margin: 0 0 10px 0;">What's Coming:</h3>
            <p style="font-size: 14px; line-height: 1.6; color: #555; margin: 0;">
                Our new FBA Fees Reconciliation tool will help you easily track discrepancies in FBA charges, identify overcharges, and view detailed error statuses for quick resolutions. You'll get a clear summary of all FBA fee components, mismatches, and actionable insights to help you recover lost revenue and stay audit-ready.
            </p>
        </div>
        
        <p style="font-size: 14px; line-height: 1.6; color: #555;">We're working hard to bring this feature to you as soon as possible. You'll be among the first to know when it's ready!</p>
        
        <p style="font-size: 14px; line-height: 1.6; color: #555;">In the meantime, continue to make the most of your existing Phormula tools to optimize your Amazon business.</p>
        
        <p style="font-size: 14px; color: #555;">If you have any questions or suggestions for the FBA Fees feature, feel free to reach out to our support team at <a href="mailto:care@phormula.io" style="color: #5EA68E; text-decoration: none;">care@phormula.io</a></p>
        
        <p style="font-size: 14px; color: #555; margin-top: 30px;">Best regards, <br>The Phormula Team</p>
        
        <div style="text-align: center; margin-top: 30px; padding-top: 20px; border-top: 1px solid #eee;">
            <p style="font-size: 12px; color: #999;">Stay tuned for more exciting updates!</p>
        </div>
    </div>
</body>
</html>
        """
        
        # Send the notification email
        mail.send(msg)
        print(f"FBA notification email sent successfully to {email}")
        return True
        
    except Exception as e:
        print(f"Failed to send FBA notification email to {email}: {e}")
        return False

@fba_bp.route('/fba_email', methods=['POST', 'OPTIONS', 'GET'])
def fba_email_notification():
    """Handle FBA feature notification requests"""
    
    # Handle CORS preflight request
    if request.method == 'OPTIONS':
        response = jsonify({'status': 'success'})
        response.headers.add('Access-Control-Allow-Origin', '*')
        response.headers.add('Access-Control-Allow-Headers', 'Content-Type,Authorization')
        response.headers.add('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE,OPTIONS')
        return response
    
    try:
        # Extract user information from JWT token or request data
        auth_header = request.headers.get('Authorization')
        user_email = None
        
        if auth_header and auth_header.startswith('Bearer '):
            token = auth_header.split(' ')[1]
            try:
                # Decode JWT token to get user info
                payload = jwt.decode(token, SECRET_KEY, algorithms=['HS256'])
                user_id = payload.get('user_id')
                
                # Get user email from database
                user = User.query.get(user_id)
                if user:
                    user_email = user.email
                else:
                    return jsonify({'error': 'User not found'}), 404
            except jwt.ExpiredSignatureError:
                return jsonify({'error': 'Token has expired'}), 401
            except jwt.InvalidTokenError:
                return jsonify({'error': 'Invalid token'}), 401
        else:
            return jsonify({'error': 'Authorization token required'}), 401
        
        # Send notification email
        if send_fba_notification_email(user_email):
            response_data = {
                'status': 'success',
                'message': 'Notification email sent successfully!',
                'email': user_email
            }
            response = jsonify(response_data)
        else:
            response_data = {
                'status': 'error',
                'message': 'Failed to send notification email'
            }
            response = jsonify(response_data), 500
            
        # Add CORS headers
        response.headers.add('Access-Control-Allow-Origin', '*')
        response.headers.add('Access-Control-Allow-Headers', 'Content-Type,Authorization')
        response.headers.add('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE,OPTIONS')
        
        return response
        
    except Exception as e:
        response = jsonify({
            'status': 'error',
            'message': 'Internal server error'
        }), 500
        
        response[0].headers.add('Access-Control-Allow-Origin', '*')
        response[0].headers.add('Access-Control-Allow-Headers', 'Content-Type,Authorization')
        response[0].headers.add('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE,OPTIONS')
        
        return response