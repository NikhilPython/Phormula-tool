import os
import logging
from dotenv import load_dotenv
from datetime import timedelta
from celery.schedules import crontab

# Load environment variables from .env file (early)
load_dotenv()

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

FRONTEND_BASE_URL = _env("FRONTEND_BASE_URL", "http://localhost:3000")

basedir = os.path.abspath(os.path.dirname(__file__))

class Config:
    SECRET_KEY = _env("SECRET_KEY")
    AMAZON_ADS_CLIENT_ID = _env("AMAZON_ADS_CLIENT_ID")
    AMAZON_ADS_CLIENT_SECRET = _env("AMAZON_ADS_CLIENT_SECRET")
    AMAZON_ADS_REDIRECT_URI = _env("AMAZON_ADS_REDIRECT_URI", f"{FRONTEND_BASE_URL}/api/ads/callback")

    SQLALCHEMY_DATABASE_URI         = _env("DATABASE_URL")
    SQLALCHEMY_DATABASE_ADMIN_URL   = _env("DATABASE_ADMIN_URL")
    SQLALCHEMY_DATABASE_SHOPIFY_URL = _env("DATABASE_SHOPIFY_URL")
    SQLALCHEMY_DATABASE_CHATBOT_URL = _env("DATABASE_CHATBOT_URL")
    SQLALCHEMY_DATABASE_AMAZON_URL  = _env("DATABASE_AMAZON_URL")
    SQLALCHEMY_TRACK_MODIFICATIONS  = False

    MAIL_SERVER         = _env("MAIL_SERVER", "smtp.gmail.com")
    MAIL_PORT           = _env_int("MAIL_PORT", 587)
    MAIL_USE_TLS        = _env_bool("MAIL_USE_TLS", True)
    MAIL_USE_SSL        = _env_bool("MAIL_USE_SSL", False)
    MAIL_USERNAME       = _env("MAIL_USERNAME")
    MAIL_PASSWORD       = _env("MAIL_PASSWORD")
    MAIL_DEFAULT_SENDER = (
        _env("MAIL_DEFAULT_SENDER_NAME", "Phormula Care"),
        MAIL_USERNAME
    )
    MAIL_MAX_EMAILS     = _env_int("MAIL_MAX_EMAILS", 100) or 100
    MAIL_SUPPRESS_SEND  = _env_bool("MAIL_SUPPRESS_SEND", False)

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
                "schedule": crontab(day_of_month=1, hour=11, minute=0),
            },
            "sync-amazon-monthly-transactions-every-month-1st-11am": {
                "task": "tasks.sync_amazon_monthly_transactions",
                "schedule": crontab(day_of_month=1, hour=11, minute=0),
            },
            "generate-forecast-every-month-1st-11-30am": {
                "task": "tasks.generate_monthly_forecast_files",
                "schedule": crontab(day_of_month=1, hour=11, minute=30),
            },
            "send-live-bi-email-test-1-22pm": {
                "task": "tasks.send_live_bi_email_daily",
                "schedule": crontab(hour=11, minute=30),
            },
            # "generate-forecast-every-month-1st-11-30am": {
            #     "task": "tasks.generate_monthly_forecast_files",
            #     "schedule": crontab(hour=13, minute=23),
            # },
            
        }
    }

    @staticmethod
    def validate_mail():
        u = Config.MAIL_USERNAME or ""
        p = Config.MAIL_PASSWORD or ""
        logging.info(f"MAIL_USERNAME: {repr(u)}")
        logging.info(f"MAIL_PASSWORD length: {len(p)}")
        logging.info(f"MAIL_PORT: {Config.MAIL_PORT}")
        logging.info(f"TLS/SSL: {Config.MAIL_USE_TLS}/{Config.MAIL_USE_SSL}")