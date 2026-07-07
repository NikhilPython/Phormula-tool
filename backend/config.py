import http
import os
import logging
from dotenv import load_dotenv
from datetime import timedelta
from celery.schedules import crontab

load_dotenv()

# Helpers to read env vars safely
def _env(name, default=None, strip=True):
    v = os.getenv(name, default)
    if v is None:
        return None
    return v.strip() if (strip and isinstance(v, str)) else v

def _env_bool(name, default=False):
    return (_env(name, str(default)) or "").lower() in ("1", "true", "yes", "y", "on")

def _env_int(name, default=None):
    v = _env(name)
    try:
        return int(v) if v is not None else default
    except (TypeError, ValueError):
        return default


basedir = os.path.abspath(os.path.dirname(__file__))

class Config:
    # --- Env flags ---
    ENV = _env("FLASK_ENV")  # production|development
    DEBUG = _env_bool("FLASK_DEBUG", False)
    TESTING = _env_bool("FLASK_TESTING", False)

    # --- Secret Key ---
    SECRET_KEY = _env("SECRET_KEY")  # Must be set in AWS

    # --- CORS ---
    CORS_ORIGINS = [
        o.strip()
        for o in (
            _env(
                "CORS_ORIGINS",
                "https://phormula.io,https://www.phormula.io,https://admin.phormula.io"
            )
        ).split(",")
        if o.strip()
    ]
    CORS_SUPPORTS_CREDENTIALS = True

    # --- Database ---
    SQLALCHEMY_DATABASE_URI = _env("DATABASE_URL")
    SQLALCHEMY_DATABASE_ADMIN_URL = _env("DATABASE_ADMIN_URL")
    SQLALCHEMY_DATABASE_SHOPIFY_URL = _env("DATABASE_SHOPIFY_URL")
    SQLALCHEMY_DATABASE_CHATBOT_URL = _env("DATABASE_CHATBOT_URL")
    SQLALCHEMY_DATABASE_AMAZON_URL = _env("DATABASE_AMAZON_URL")
    SQLALCHEMY_TRACK_MODIFICATIONS = False

    SQLALCHEMY_ENGINE_OPTIONS = {
        "pool_pre_ping": True,
        "pool_recycle": 280,
        "pool_size": 5,
        "max_overflow": 10,
    }

    

    # --- Email ---
    MAIL_SERVER = _env("MAIL_SERVER", "smtp.gmail.com")
    MAIL_PORT = _env_int("MAIL_PORT", 587)
    MAIL_USE_TLS = _env_bool("MAIL_USE_TLS", True)
    MAIL_USE_SSL = _env_bool("MAIL_USE_SSL", False)
    MAIL_USERNAME = _env("MAIL_USERNAME")
    MAIL_PASSWORD = _env("MAIL_PASSWORD")
    MAIL_DEFAULT_SENDER = (_env("MAIL_DEFAULT_SENDER_NAME", "Phormula Care"), MAIL_USERNAME)
    MAIL_MAX_EMAILS = _env_int("MAIL_MAX_EMAILS", 100) or 100
    MAIL_SUPPRESS_SEND = _env_bool("MAIL_SUPPRESS_SEND", False)

     # Flask-Session
    SESSION_TYPE = _env("SESSION_TYPE", "filesystem")
    SESSION_PERMANENT = False
    SESSION_USE_SIGNER = True
    SESSION_FILE_DIR = os.path.join(basedir, "flask_session")

    # Redis / Celery
    REDIS_URL = _env("REDIS_URL")

    CELERY = {
        "broker_url": REDIS_URL,
        "result_backend": REDIS_URL,
        "task_ignore_result": True,
        "timezone": "Asia/Kolkata",
        "enable_utc": False,
        "beat_schedule": {
            "seed-currency-rates-every-month-1st-11am": {
                 "task": "tasks.seed_currency_rates_monthly",
                 "schedule": crontab(day_of_month=1, hour=9, minute=0),
            },
            # UK - existing time
            "sync-amazon-monthly-transactions-uk-every-month-1st": {
                "task": "tasks.sync_amazon_monthly_transactions",
                "schedule": crontab(day_of_month=1, hour=10, minute=0),
                "args": ("uk",),
            },

            "generate-forecast-uk-every-month-1st": {
                "task": "tasks.generate_monthly_forecast_files",
                "schedule": crontab(day_of_month=1, hour=14, minute=0),
                "args": ("uk",),
            },

            "send-live-bi-email-uk-weekly": {
                "task": "tasks.send_live_bi_email_daily",
                "schedule": crontab(day_of_month="1,8,15,22,29", hour=14, minute=0),
                "args": ("uk",),
            },

            "refresh-dashboard-uk-daily-11am": {
                "task": "tasks.refresh_dashboard_daily",
                "schedule": crontab(hour=12, minute=30),
                "args": ("uk",),
            },


            # US - 4 PM
            "sync-amazon-monthly-transactions-us-every-month-1st-4pm": {
                "task": "tasks.sync_amazon_monthly_transactions",
                "schedule": crontab(day_of_month=1, hour=16, minute=0),
                "args": ("us",),
            },

            "generate-forecast-us-every-month-1st-4pm": {
                "task": "tasks.generate_monthly_forecast_files",
                "schedule": crontab(day_of_month=1, hour=16, minute=20),
                "args": ("us",),
            },

            "send-live-bi-email-us-weekly-4pm": {
                "task": "tasks.send_live_bi_email_daily",
                "schedule": crontab(day_of_month="1,8,15,22,29", hour=22, minute=0),
                "args": ("us",),
            },

            "refresh-dashboard-us-daily-3pm": {
                "task": "tasks.refresh_dashboard_daily",
                "schedule": crontab(hour=20, minute=30),
                "args": ("us",),
            },
        }
        # "beat_schedule": {
        #     "seed-currency-rates-every-month-1st-11am": {
        #         "task": "tasks.seed_currency_rates_monthly",
        #         "schedule": crontab(day_of_month=1, hour=9, minute=0),
        #     },
        #     "sync-amazon-monthly-transactions-every-month-1st-11am": {
        #         "task": "tasks.sync_amazon_monthly_transactions",
        #         "schedule": crontab(day_of_month=1, hour=10, minute=0),
        #     },
        #     "generate-forecast-every-month-1st-11-30am": {
        #         "task": "tasks.generate_monthly_forecast_files",
        #         "schedule": crontab(day_of_month=1, hour=12, minute=15),
        #     },
        #     "send-live-bi-email-weekly": {
        #         "task": "tasks.send_live_bi_email_daily",
        #         "schedule": crontab(day_of_month="1,8,15,22,29", hour=11, minute=0),
        #     },
        #     # "send-live-bi-email-weekly": {
        #     #     "task": "tasks.send_live_bi_email_daily",
        #     #     "schedule": crontab(hour=16, minute=25),
        #     # },
        #     # "run-agent-summary-every-hour": {
        #     #     "task": "tasks.run_agent_schedules",
        #     #     "schedule": 300.0,
        #     # },
        # }
        
    }

    # --- Amazon Ads (LWA OAuth) ---
    AMAZON_ADS_CLIENT_ID = _env("AMAZON_ADS_CLIENT_ID")
    AMAZON_ADS_CLIENT_SECRET = _env("AMAZON_ADS_CLIENT_SECRET")
    AMAZON_ADS_REDIRECT_URI = _env("AMAZON_ADS_REDIRECT_URI")

    # --- Production switches ---
    # In AWS set: AUTO_DB_CREATE=0 and AUTO_DB_SCHEMA=0
    AUTO_DB_CREATE = _env_bool("AUTO_DB_CREATE", False)
    AUTO_DB_SCHEMA = _env_bool("AUTO_DB_SCHEMA", False)
