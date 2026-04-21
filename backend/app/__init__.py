from dotenv import load_dotenv
import os
from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from flask_mail import Mail
from flask_cors import CORS
from config import Config
from sqlalchemy import text
from flask_session import Session
from datetime import timedelta
from app.utils.celery_utils import celery_init_app

# Load environment variables
load_dotenv()

# Create extension instances
db = SQLAlchemy()
mail = Mail()
sess = Session()

def create_app():
    app = Flask(__name__)

    # ---------------- LOGGING FIX (CRITICAL) ----------------
    import logging

    root_logger = logging.getLogger()

    # Remove existing handlers (Flask/Werkzeug adds its own)
    if root_logger.handlers:
        for handler in root_logger.handlers[:]:
            root_logger.removeHandler(handler)

    # Add fresh handler
    handler = logging.StreamHandler()
    handler.setLevel(logging.INFO)

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )
    handler.setFormatter(formatter)

    root_logger.addHandler(handler)
    root_logger.setLevel(logging.INFO)

    # Make Flask logger use same handlers
    app.logger.handlers = root_logger.handlers
    app.logger.setLevel(logging.INFO)
    # -------------------------------------------------------

    app.config.from_object(Config)

    # Database configuration
    app.config['SQLALCHEMY_BINDS'] = {
        'superadmin': app.config['SQLALCHEMY_DATABASE_ADMIN_URL'],
        'shopify': app.config['SQLALCHEMY_DATABASE_SHOPIFY_URL'],
        'chatbot': app.config['SQLALCHEMY_DATABASE_CHATBOT_URL'],
        'amazon': app.config['SQLALCHEMY_DATABASE_AMAZON_URL']
    }

    # Create databases if they don't exist
    from app.utils.token_utils import create_database_if_not_exists
    create_database_if_not_exists(app.config['SQLALCHEMY_DATABASE_URI'])
    create_database_if_not_exists(app.config['SQLALCHEMY_DATABASE_ADMIN_URL'])
    create_database_if_not_exists(app.config['SQLALCHEMY_DATABASE_SHOPIFY_URL'])
    create_database_if_not_exists(app.config['SQLALCHEMY_DATABASE_CHATBOT_URL'])
    create_database_if_not_exists(app.config['SQLALCHEMY_DATABASE_AMAZON_URL'])

    # Initialize extensions
    db.init_app(app)
    mail.init_app(app)
    sess.init_app(app)

    # CORS
    CORS(
        app,
        resources={r"/*": {"origins": ["http://localhost:3000", "http://127.0.0.1:3000"]}},
        supports_credentials=True,
        allow_headers=["Content-Type", "Authorization", "Cookie"],
        methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"]
    )

    # Register blueprints
    from app.routes.user_routes import user_bp
    from app.routes.upload_routes import upload_bp
    from app.routes.dashboard_routes import dashboard_bp
    from app.routes.chatbot_routes import chatbot_bp
    from app.routes.forecast_routes import forecast_bp
    from app.routes.current_inventory_routes import current_inventory_bp
    from app.routes.product_routes import product_bp
    from app.routes.admin_routes import admin_bp
    from app.routes.admin_dashboard_routes import admin_dashboard_bp
    from app.routes.superadmin_dashboard_routes import superadmin_dashboard_bp
    from app.routes.business_intelligence import business_intelligence_bp
    from app.routes.shopify_routes import shopify_bp
    from app.routes.pie_chart_routes import pie_chart_bp
    from app.routes.add_member_routes import add_member_bp
    from app.routes.amazon_api_routes import amazon_api_bp
    from app.routes.skuwise_profit_routes import skuwise_bp
    from app.routes.fba_routes import fba_bp
    from app.routes.error_status_routes import error_status_bp
    from app.routes.referral_fee_routes import referral_fee_bp
    from app.routes.fee_preview_routes import fee_preview_bp
    from app.routes.inventory_routes import inventory_bp
    from app.routes.conversion_rate_routes import conversion_bp
    from app.routes.amazon_sales_api_routes import amazon_sales_api_bp
    from app.routes.live_data_bi_routes import live_data_bi_bp
    from app.routes.monthwise_ai_summary_routes import summary_bp
    from app.routes.advertisement_api_routes import advertisement_api_routes_bp
    from app.routes.member_auth import member_auth_bp
    from app.routes.inventory_breakup_routes import inventory_breakup_bp
    from app.routes.website_scrapper_routes import website_scrapper_bp
    from app.routes.business_journey_routes import business_journey_bp
    from app.routes.agent_routes import agent_bp
    from app.routes.email_routes import email_bp
    from app.routes.notification_routes import notification_bp

    app.register_blueprint(user_bp)
    app.register_blueprint(upload_bp)
    app.register_blueprint(dashboard_bp)
    app.register_blueprint(chatbot_bp)
    app.register_blueprint(forecast_bp)
    app.register_blueprint(current_inventory_bp)
    app.register_blueprint(product_bp)
    app.register_blueprint(admin_bp)
    app.register_blueprint(pie_chart_bp)
    app.register_blueprint(admin_dashboard_bp)
    app.register_blueprint(superadmin_dashboard_bp)
    app.register_blueprint(business_intelligence_bp)
    app.register_blueprint(shopify_bp)
    app.register_blueprint(add_member_bp)
    app.register_blueprint(amazon_api_bp)
    app.register_blueprint(skuwise_bp)
    app.register_blueprint(fba_bp)
    app.register_blueprint(error_status_bp)
    app.register_blueprint(referral_fee_bp)
    app.register_blueprint(fee_preview_bp)
    app.register_blueprint(inventory_bp)
    app.register_blueprint(conversion_bp)
    app.register_blueprint(amazon_sales_api_bp)
    app.register_blueprint(live_data_bi_bp)
    app.register_blueprint(summary_bp)
    app.register_blueprint(advertisement_api_routes_bp)
    app.register_blueprint(member_auth_bp)
    app.register_blueprint(inventory_breakup_bp)
    app.register_blueprint(website_scrapper_bp)
    app.register_blueprint(business_journey_bp)
    app.register_blueprint(agent_bp)
    app.register_blueprint(email_bp)
    app.register_blueprint(notification_bp)

    with app.app_context():
        db.create_all()

        from sqlalchemy import inspect
        inspector = inspect(db.engine)
        if 'upload_history' in inspector.get_table_names():
            try:
                with db.engine.connect() as conn:
                    conn.execute(text("ALTER TABLE upload_history ALTER COLUMN month TYPE VARCHAR(20);"))
                    conn.commit()
            except Exception as e:
                print(f"[WARNING] Could not alter upload_history table: {e}")

    # Initialize Celery after app setup
    celery_init_app(app)

    return app