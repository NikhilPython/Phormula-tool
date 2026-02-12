from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from config import Config
SECRET_KEY = Config.SECRET_KEY
#UPLOAD_FOLDER = Config.UPLOAD_FOLDER
import os
import pandas as pd
import numpy as np 
from app.models.user_models import User
from flask_mail import Message
from flask import current_app
from app import mail

from dotenv import load_dotenv

import warnings
warnings.filterwarnings("ignore") 


load_dotenv()
db_url = os.getenv('DATABASE_URL', 'postgresql://postgres:password@localhost:5432/phormula')


MONTHS_REVERSE_MAP = {
    1: "january", 2: "february", 3: "march", 4: "april", 5: "may", 6: "june",
    7: "july", 8: "august", 9: "september", 10: "october", 11: "november", 12: "december"
}


MONTHS_MAP = {
    'january': 1, 'february': 2, 'march': 3, 'april': 4,
    'may': 5, 'june': 6, 'july': 7, 'august': 8,
    'september': 9, 'october': 10, 'november': 11, 'december': 12
}


def create_user_session(db_url):
    user_engine = create_engine(db_url)
    UserSession = sessionmaker(bind=user_engine)
    return UserSession()



from app.models.user_models import StoredFile  

XLSX_MIME = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"

def _guess_kind_from_filename(file_name: str) -> str:
    name = (file_name or "").lower()
    if name.startswith("inventory_forecast_"):
        return "inventory_forecast"
    if name.startswith("forecasts_for_"):
        return "forecasts_for"
    if name.startswith("forecastpnl_"):
        return "pnl_forecast"
    if name.startswith("pnlforecast_"):
        return "pnl_forecast_upload"
    return "file"

def send_forecast_email(user_id, file_name, month, year, *, country=None):
    try:
        user = User.query.filter_by(id=user_id).first()
        if not user:
            raise ValueError(f"No user found with ID {user_id}")

        user_email = user.email

        msg = Message(
            'Your Forecast Report',
            sender=current_app.config.get('MAIL_DEFAULT_SENDER'),
            recipients=[user_email]
        )

        msg.body = f"""
Dear {user.email},

Please find attached the forecast report for {month} {year} that you requested.

Best regards,
The Phormula Team
"""

        # ✅ Load bytes from DB
        q = StoredFile.query.filter_by(user_id=user_id, filename=file_name)
        if country:
            q = q.filter_by(country=country.lower())
        stored = q.first()

        if not stored:
            # helpful debug: try to locate by user+kind if filename differs
            raise FileNotFoundError(f"File not found in DB for user_id={user_id}, filename={file_name}")

        content_type = stored.content_type or XLSX_MIME
        msg.attach(file_name, content_type, stored.data)

        mail.send(msg)
        print(f"✅ Forecast email sent to {user_email} (attached from DB: {file_name})")

    except Exception as e:
        print(f"Failed to send forecast email: {e}")
        raise



def generate_pnl_report(year: int, month: str) -> dict:
    """
    Generate a simple P&L (profit and Loss) report for a given month and year.

    Parameters:
        year (int): The year for the report.
        month (str): The month for the report.

    Returns:
        dict: A dictionary containing sales, expenses, FBA fees, and calculated profit.
    """
    
    # Placeholder data - replace this with actual database queries in a real scenario
    sales_data = {
        'sales': 10000,
        'expenses': 5000,
        'fba_fees': 1000,
    }
    
    # Calculate profit
    profit = sales_data['sales'] - sales_data['expenses'] - sales_data['fba_fees']
    
    return {
        'year': year,
        'month': month,
        'sales': sales_data['sales'],
        'expenses': sales_data['expenses'],
        'fba_fees': sales_data['fba_fees'],
        'profit': profit,
    }



def send_pnlforecast_email(user_id, file_name, month, year, *, country=None):
    try:
        user = User.query.filter_by(id=user_id).first()
        if not user:
            raise ValueError(f"No user found with ID {user_id}")

        user_email = user.email

        msg = Message(
            'Your PnL Forecast Report',
            sender=current_app.config.get('MAIL_DEFAULT_SENDER'),
            recipients=[user_email]
        )

        msg.body = f"""
Dear {user.email},

Please find attached the PNL forecast report for next 3 months of {month} {year} that you requested.

Best regards,
The Phormula Team
"""

        # ✅ Load bytes from DB
        q = StoredFile.query.filter_by(user_id=user_id, filename=file_name)
        if country:
            q = q.filter_by(country=country.lower())
        stored = q.first()

        if not stored:
            raise FileNotFoundError(f"PnL file not found in DB for user_id={user_id}, filename={file_name}")

        content_type = stored.content_type or XLSX_MIME
        msg.attach(file_name, content_type, stored.data)

        mail.send(msg)
        print(f"✅ PnL email sent to {user_email} (attached from DB: {file_name})")

    except Exception as e:
        print(f"Failed to send pnl forecast email: {e}")
        raise



def get_previous_month_year(month, year):
    """Calculate the previous month and year."""

    year = int(year)
    prev_month_num = MONTHS_MAP[month] - 1
    if prev_month_num == 0:
        prev_month_num = 12
        year -= 1
    prev_month = MONTHS_REVERSE_MAP[prev_month_num]

    return prev_month, year


