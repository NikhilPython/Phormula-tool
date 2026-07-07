import os
from dotenv import load_dotenv
from flask import Flask, jsonify
from flask_sqlalchemy import SQLAlchemy
from flask_mail import Mail
from flask_cors import CORS
from sqlalchemy import text
from config import Config
from flask_session import Session
from datetime import timedelta
from app.utils.celery_utils import celery_init_app

db = SQLAlchemy()
mail = Mail()
sess = Session()

def create_app():
    app = Flask(__name__)
    app.config.from_object(Config)

    # ------------------------
    # CORS
    # ------------------------
    CORS(
        app,
        origins=app.config.get("CORS_ORIGINS", [
            "https://phormula.io",
            "https://www.phormula.io",
            "https://admin.phormula.io",
        ]),
        supports_credentials=True,
        allow_headers=["Content-Type", "Authorization", "Cookie"],
        methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    )


    # ------------------------
    # DATABASE BINDS
    # ------------------------
    binds = {}

    if app.config.get("SQLALCHEMY_DATABASE_ADMIN_URL"):
        binds["superadmin"] = app.config["SQLALCHEMY_DATABASE_ADMIN_URL"]

    if app.config.get("SQLALCHEMY_DATABASE_SHOPIFY_URL"):
        binds["shopify"] = app.config["SQLALCHEMY_DATABASE_SHOPIFY_URL"]

    if app.config.get("SQLALCHEMY_DATABASE_CHATBOT_URL"):
        binds["chatbot"] = app.config["SQLALCHEMY_DATABASE_CHATBOT_URL"]

    if app.config.get("SQLALCHEMY_DATABASE_AMAZON_URL"):
        binds["amazon"] = app.config["SQLALCHEMY_DATABASE_AMAZON_URL"]

    if binds:
        app.config["SQLALCHEMY_BINDS"] = binds

    if not app.config.get("SQLALCHEMY_DATABASE_URI"):
        raise RuntimeError("DATABASE_URL is missing")

    db.init_app(app)
    mail.init_app(app)
    sess.init_app(app)

    # ------------------------
    # HEALTH CHECK
    # ------------------------
    @app.get("/health")
    def health():
        return jsonify({"status": "ok"}), 200

    # ------------------------
    # AUTO DB CREATE (DEV ONLY)
    # ------------------------
    if app.config.get("AUTO_DB_SCHEMA"):
        with app.app_context():
            # ✅ THIS IS THE KEY FIX
            from app.models import user_models  # registers all models

            db.create_all()

            # Optional legacy patch
            try:
                with db.engine.begin() as conn:
                    conn.execute(
                        text(
                            "ALTER TABLE upload_history "
                            "ALTER COLUMN month TYPE VARCHAR(20);"
                        )
                    )
            except Exception:
                pass

    # ------------------------
    # BLUEPRINTS
    # ------------------------
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
    from app.routes.inventory_current_routes import inventory_current_bp

    for bp in [
        user_bp, upload_bp, dashboard_bp, chatbot_bp, forecast_bp,
        current_inventory_bp, product_bp, admin_bp, pie_chart_bp,
        admin_dashboard_bp, superadmin_dashboard_bp, business_intelligence_bp,
        shopify_bp, add_member_bp, amazon_api_bp,
        skuwise_bp, fba_bp, error_status_bp, referral_fee_bp,
        fee_preview_bp, inventory_bp, conversion_bp, inventory_breakup_bp, member_auth_bp,
        amazon_sales_api_bp, live_data_bi_bp, summary_bp, advertisement_api_routes_bp,
        website_scrapper_bp, business_journey_bp, agent_bp, email_bp, notification_bp, inventory_current_bp

    ]:
        app.register_blueprint(bp)
    celery_init_app(app)

    return app
