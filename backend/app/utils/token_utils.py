import jwt
from datetime import datetime, timedelta
from config import Config
from flask import current_app
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from sqlalchemy.engine.url import make_url
SECRET_KEY = Config.SECRET_KEY




def create_database_if_not_exists(db_url):
    url = make_url(db_url)
    db_name = url.database

    # Connect to the default 'postgres' database to run CREATE DATABASE
    default_url = url.set(database="postgres")

    try:
        conn = psycopg2.connect(
            dbname=default_url.database,
            user=default_url.username,
            password=default_url.password,
            host=default_url.host,
            port=default_url.port
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

        cursor = conn.cursor()
        cursor.execute(f"SELECT 1 FROM pg_database WHERE datname = %s", (db_name,))
        exists = cursor.fetchone()
        if not exists:
            cursor.execute(f'CREATE DATABASE "{db_name}"')

        cursor.close()
        conn.close()
    except Exception as e:
        print(f"❌ Error creating database: {e}")



def generate_admin_token(admin_id):
    """
    Generate a time-sensitive token for admin verification or password reset
    """
    # Token valid for 1 hour
    payload = {
        'admin_id': admin_id,
        'exp': datetime.utcnow() + timedelta(hours=1),
        'iat': datetime.utcnow()
    }
    
    token = jwt.encode(
        payload,
        current_app.config['SECRET_KEY'],
        algorithm='HS256'
    )
    
    return token



def verify_admin_token(token):
    """
    Verify the admin token and return the admin ID
    """
    try:
        data = jwt.decode(
            token,
            current_app.config['SECRET_KEY'],
            algorithms=['HS256']
        )
        return data['admin_id']
    except:
        raise Exception("Invalid or expired token")



def generate_token(user_id):
    """Generate a JWT token for user authentication"""
    payload = {
        'user_id': user_id,
        'exp': datetime.utcnow() + timedelta(days=12)
    }
    return jwt.encode(payload, Config.SECRET_KEY, algorithm='HS256')



def decode_token(token):
    """Decode a JWT token and return the user_id"""
    try:
        payload = jwt.decode(token, Config.SECRET_KEY, algorithms=['HS256'])
        return payload.get('user_id')
    except:
        return None



def generate_verification_token(email):
    """Generate a token for email verification"""
    payload = {
        'email': email,
        'exp': datetime.utcnow() + timedelta(hours=12)
    }
    return jwt.encode(payload, Config.SECRET_KEY, algorithm='HS256')



def confirm_verification_token(token):
    """Confirm a verification token and return the email"""
    try:
        payload = jwt.decode(token, Config.SECRET_KEY, algorithms=['HS256'])
        return payload.get('email')
    except:
        return None



def generate_reset_token(user_id):
    """Generate a token for password reset"""
    payload = {
        'user_id': user_id,
        'exp': datetime.utcnow() + timedelta(hours=12)
    }
    return jwt.encode(payload, Config.SECRET_KEY, algorithm='HS256')


def get_effective_user_id_from_token(token: str):
    payload = jwt.decode(token, SECRET_KEY, algorithms=["HS256"])

    # owner login -> payload["user_id"]
    # member login -> payload["owner_user_id"] (or whatever you set)
    effective_user_id = payload.get("user_id") or payload.get("owner_user_id")

    if not effective_user_id:
        # token is valid but missing required identity fields
        raise jwt.InvalidTokenError("Token payload missing user_id/owner_user_id")

    return payload, effective_user_id

