from datetime import datetime
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import Column, Integer, String, Boolean, Float, DateTime, Enum, Date
from app import db
from sqlalchemy.sql import func
from sqlalchemy import Text
from datetime import datetime
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy import UniqueConstraint, Index
from sqlalchemy import Numeric
from sqlalchemy import JSON

# ------------------------------------------------- SuperAdmin Models -------------------------------------------------

class SuperAdmin(db.Model):
    __tablename__ = 'superadmin'
    __bind_key__ = 'superadmin'

    id = Column(Integer, primary_key=True)
    email = Column(String(150), unique=True, nullable=False)
    password = Column(String(500), nullable=False)
    is_superadmin = Column(Boolean, nullable=False, default=True)
    is_verified = Column(Boolean, nullable=False, default=True) 

class UserAdmin(db.Model):
    __tablename__ = 'admin'
    __bind_key__ = 'superadmin'  # Use same DB as superadmin
    id = Column(Integer, primary_key=True)
    email = Column(String(150), unique=True, nullable=False)
    password = Column(String(500), nullable=False)
    is_admin = Column(Boolean, default=True)  # Assuming all entries here are admins
    is_superadmin = Column(Boolean, default=False)  # Ensure it's never null
    is_verified = Column(Boolean, default=False)

class CurrencyConversion(db.Model):
    __tablename__ = 'currency_conversion'
    __bind_key__ = 'superadmin'  # Using same DB as UserAdmin

    id = Column(Integer, primary_key=True)
    user_currency = Column(String(10), nullable=False)
    country = Column(String(100), nullable=False)
    selected_currency = Column(String(10), nullable=False)
    month = Column(String(15), nullable=False)
    year = Column(Integer, nullable=False)
    conversion_rate = Column(Float, nullable=False)

# ------------------------------------------------- Member Models -------------------------------------------------

class Member(db.Model):
    __tablename__ = "member"
    __bind_key__ = "superadmin"

    id = Column(Integer, primary_key=True)
    owner_user_id = Column(Integer, nullable=False, index=True)
    member_name = Column(String(150), nullable=True)
    email = Column(String(150), nullable=False)
    password = Column(String(500), nullable=False)
    is_verified = Column(Boolean, default=False)
    role = Column(String(50), nullable=False, default="Marketing")
    marketplace_ids = Column(JSON, nullable=True)
    countries = Column(JSON, nullable=True)
    modules = Column(JSON, nullable=True)
    token_name = Column(String(80), unique=True, nullable=False, index=True)
    country_access = db.Column(db.JSON, nullable=True, default=dict)
    created_at = Column(DateTime, default=datetime.utcnow)
    # ✅ NEW (for secure reset single-use)
    password_changed_at = Column(DateTime, nullable=True)

    __table_args__ = (
        UniqueConstraint("owner_user_id", "email", name="uq_member_owner_email"),
    )

# ------------------------------------------------- Notification Models -------------------------------------------------

class Notification(db.Model):
    __tablename__ = "notification"
    __bind_key__ = "amazon"

    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, nullable=False, index=True)
    country = db.Column(db.String(20), nullable=False, index=True)

    success = db.Column(db.Boolean, nullable=False, default=True)
    message = db.Column(db.Text, nullable=True)

    data = db.Column(JSONB, nullable=False)
    full_response = db.Column(JSONB, nullable=False)

    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
    updated_at = db.Column(
        db.DateTime,
        default=datetime.utcnow,
        onupdate=datetime.utcnow,
        nullable=False
    )

    __table_args__ = (
        db.UniqueConstraint("user_id", "country", name="uq_notification_user_country"),
    )


class NotificationAlertSKU(db.Model):
    __tablename__ = "notification_alert_sku"
    __bind_key__ = "amazon"

    id = db.Column(db.Integer, primary_key=True)

    user_id = db.Column(db.Integer, nullable=False, index=True)
    country = db.Column(db.String(20), nullable=False, index=True)

    sku = db.Column(db.String(255), nullable=False, index=True)
    product_name = db.Column(db.String(255), nullable=True)

    alert = db.Column(db.String(100), nullable=True)
    alert_type = db.Column(db.String(100), nullable=True)

    first_alert_time = db.Column(db.DateTime, nullable=True)
    first_alert_date = db.Column(db.Date, nullable=True)
    first_alert_day_name = db.Column(db.String(20), nullable=True)

    last_alert_time = db.Column(db.DateTime, nullable=True)
    last_alert_date = db.Column(db.Date, nullable=True)
    last_alert_day_name = db.Column(db.String(20), nullable=True)

    days_since_first_alert = db.Column(db.Integer, default=0)
    is_active = db.Column(db.Boolean, default=True)

    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
    updated_at = db.Column(
        db.DateTime,
        default=datetime.utcnow,
        onupdate=datetime.utcnow,
        nullable=False
    )

    __table_args__ = (
        db.UniqueConstraint(
            "user_id",
            "country",
            "sku",
            name="uq_notification_alert_user_country_sku"
        ),
    )


# ------------------------------------------------- User Models -------------------------------------------------

class User(db.Model):
    __tablename__ = 'user'
    id = db.Column(db.Integer, primary_key=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    status = db.Column(db.Boolean, default=True)
    name = db.Column(db.String(150), nullable=True)
    password = db.Column(db.String(500), nullable=False)
    email = db.Column(db.String(150), unique=True, nullable=False)
    phone_number = db.Column(db.String(20), nullable=False)
    annual_sales_range = db.Column(db.String(50), nullable=True)   
    company_name = db.Column(db.String(50), nullable=True)
    target_sales = db.Column(Numeric(12, 2), nullable=True)  # ✅ FIXED   
    brand_name = db.Column(db.String(50), nullable=True)       
    country = db.Column(db.String(50), nullable=True)    
    marketplace_id = db.Column(db.String(200), nullable=True)
    is_google_user = db.Column(db.Boolean, default=False)
    is_verified = db.Column(db.Boolean, default=False)
    homeCurrency = db.Column(db.String(50), nullable=True)
    tax_id = db.Column(JSON, nullable=True)
    address = db.Column(JSON, nullable=True)
    token_name = db.Column(db.String(50), unique=True, nullable=False, index=True)  # Uncommented this line
    # ✅ NEW columns
    amazon_user_exists = db.Column(db.Boolean, default=False)  # both tokens exist
    amazon_ads_exists = db.Column(db.Boolean, default=False)   # ads token exists
    user_table_exists = db.Column(db.Boolean, default=False)
    # ✅ NEW
    sku_sheet_exists = db.Column(db.Boolean, default=False)
    steps_exists = db.Column(db.Boolean, default=False)
    amazon_connected = db.Column(db.Boolean, default=False)
    connected_marketplaces_count = db.Column(db.Integer, default=0)


class Category(db.Model):
    __tablename__ = 'category'
    __bind_key__ = 'superadmin'

    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, nullable=False)

    country = Column(String(255), nullable=False)
    category = Column(String(255), nullable=False)

    referral_fee = Column(Float, nullable=False)
    price_from = Column(Float, nullable=True)
    price_to = Column(Float, nullable=True)

    referral_fee_percent_est = Column(Float, nullable=True)
    brand = Column(String(255), nullable=True)


class CountryProfile(db.Model):
    __tablename__ = 'country_profile'
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, nullable=False)
    country = Column(String(255), nullable=False)
    marketplace = Column(String(255), nullable=False)
    transit_time = Column(Integer, nullable=False)
    stock_unit = Column(Integer, nullable=False)

class UploadHistory(db.Model):
    __tablename__ = 'upload_history'  
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, nullable=False)
    year = Column(Integer)
    month = Column(String(20), nullable=False)
    country = Column(String(50)) 
    file_name = Column(String(255))
    sales_chart_img =  Column(db.Text, nullable=True)
    expense_chart_img = Column(db.Text)
    qtd_pie_chart = Column(db.Text)
    ytd_pie_chart = Column(db.Text)
    profit_chart_img = Column(db.Text)
    total_sales = Column(Float)
    total_product_sales = Column(Float)
    total_profit = Column(Float)
    otherwplatform = Column(Float)
    taxncredit = Column(Float, nullable=True)
    total_expense = Column(Float)
    total_fba_fees = Column(Float)
    platform_fee = Column(Float, nullable=True)
    rembursement_fee = Column(Float, nullable=True)
    cm2_profit = Column(Float, nullable=True)
    cm2_margins = Column(Float, nullable=True)
    acos = Column(Float, nullable=True)
    rembursment_vs_cm2_margins = Column(Float, nullable=True)
    advertising_total = Column(Float, nullable=True)
    reimbursement_vs_sales = Column(Float, nullable=True)
    unit_sold = Column(Integer, nullable=True)
    total_cous = Column(Float, nullable=True)
    total_amazon_fee = Column(Float, nullable=True)
    pnl_email_sent = db.Column(db.Boolean, default=False)
    created_at = Column(DateTime, default=datetime.utcnow)

class Email(db.Model):
    __tablename__ = "email"

    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, nullable=False, index=True)
    country = Column(String(255), nullable=False, index=True)

    # last time BI email was sent
    sent_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())

    __table_args__ = (
        UniqueConstraint("user_id", "country", name="uq_email_user_country"),
    )



class AgentEmailSchedule(db.Model):
    __tablename__ = "agent_email_schedule"

    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, nullable=False, index=True)
    country = Column(String(16), nullable=False, index=True)

    frequency = Column(String(16), nullable=True)
    enabled = Column(Boolean, nullable=False, default=False)

    preferred_hour = Column(Integer, nullable=False, default=9)
    preferred_minute = Column(Integer, nullable=False, default=0)

    metric_name = Column(String(32), nullable=True)

    query = Column(Text, nullable=True)
    analysis_type = Column(String(32), nullable=True)
    period_payload = Column(JSON, nullable=True)

    day_of_month = Column(Integer, nullable=True)
    last_run_at = Column(DateTime, nullable=True)

    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("user_id", "country", "frequency", name="uq_agent_email_schedule"),
    )

class StoredFile(db.Model):
    __tablename__ = "stored_files"

    id = db.Column(db.Integer, primary_key=True)

    user_id = db.Column(db.Integer, nullable=False, index=True)
    country = db.Column(db.String(32), nullable=False, index=True)

    # file category
    kind = db.Column(db.String(64), nullable=False, index=True)
    # example: inventory_forecast, purchase_order, pnl_forecast

    month = db.Column(db.String(16), nullable=True, index=True)
    year = db.Column(db.String(8), nullable=True, index=True)

    filename = db.Column(db.String(255), nullable=False)

    content_type = db.Column(
        db.String(128),
        nullable=False,
        default="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

    data = db.Column(db.LargeBinary, nullable=False)

    created_at = db.Column(
        db.DateTime,
        default=datetime.utcnow,
        nullable=False,
        index=True
    )

    __table_args__ = (
        db.UniqueConstraint(
            "user_id",
            "country",
            "kind",
            "month",
            "year",
            name="uq_stored_files_period"
        ),
    )

    def __repr__(self):
        return f"<StoredFile {self.filename}>"
    

# -----------------------------  Chat History -----------------------------

class ChatHistory(db.Model):
    __tablename__ = 'chat_history'
    __bind_key__ = 'chatbot'  

    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, nullable=False)

    message = db.Column(db.String(1000), nullable=False)
    response = db.Column(db.Text, nullable=False)   # ✅ FIXED

    like_response = db.Column(db.String(2000))
    dislike_response = db.Column(db.String(2000))

    meta = db.Column(db.Text, nullable=True)

    timestamp = Column(DateTime, default=datetime.utcnow)

class AgentConversationState(db.Model):
    __tablename__ = "agent_conversation_state"
    __bind_key__ = "chatbot"

    id = db.Column(db.Integer, primary_key=True)
    conversation_id = db.Column(db.String(64), nullable=False, unique=True, index=True)
    user_id = db.Column(db.Integer, nullable=False, index=True)
    country = db.Column(db.String(8), nullable=True)

    active_intent = db.Column(db.String(64), nullable=True)
    active_analysis_type = db.Column(db.String(64), nullable=True)
    active_reasoning_mode = db.Column(db.String(64), nullable=True)
    active_task_type = db.Column(db.String(64), nullable=True)

    active_metric_name = db.Column(db.String(128), nullable=True)
    active_metric_names = db.Column(db.Text, nullable=True)      # json
    active_product_query = db.Column(db.String(256), nullable=True)
    active_product_queries = db.Column(db.Text, nullable=True)   # json

    active_period_parsed = db.Column(db.Text, nullable=True)     # json
    active_period_payload = db.Column(db.Text, nullable=True)    # json
    active_scope = db.Column(db.Text, nullable=True)             # json

    pending_slot = db.Column(db.String(64), nullable=True)
    pending_question = db.Column(db.String(512), nullable=True)

    last_current_metrics = db.Column(db.Text, nullable=True)     # json
    last_comparison = db.Column(db.Text, nullable=True)          # json
    last_analysis_result = db.Column(db.Text, nullable=True)     # json
    last_event_plan_result = db.Column(db.Text, nullable=True)   # json
    last_sku_intelligence_result = db.Column(db.Text, nullable=True)  # json

    inherit_context = db.Column(db.Boolean, default=True, nullable=False)
    turn_count = db.Column(db.Integer, default=0, nullable=False)

    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)    

class improvment(db.Model):
    __tablename__ = 'improvment'
    __bind_key__ = 'chatbot'
    
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, nullable=False)
    country = Column(String(255), nullable=False)
    
    # Product information fields
    product = Column(String(255), nullable=False)
    response = Column(Text, nullable=True)

    # Feedback fields
    feedback_type = Column(String(50), nullable=False)
    feedback_text = Column(String(500), nullable=True)
    is_liked = Column(Boolean, default=False)
    is_disliked = Column(Boolean, default=False)
    timestamp = Column(DateTime, default=datetime.utcnow)
    
    # Add these if you want to track tab and row
    tab_number = Column(Integer, nullable=True)
    row_index = Column(Integer, nullable=True)



class HistoricAISummary(db.Model):
    __tablename__ = 'historic_ai_summary'
    __bind_key__ = 'chatbot'

    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, nullable=False)
    country = Column(String(255), nullable=False)
    marketplace_id = Column(Integer, nullable=True)
    period = Column(
        Enum('monthly', 'quarterly', 'yearly', name='period_enum'),
        nullable=False
    )
    timeline = Column(String(50), nullable=False)
    year = Column(Integer, nullable=False)

    # NEW: objective fields
    primary_goal = Column(String(50), nullable=True)              # e.g. profit|growth|...
    risk_level = Column(String(50), nullable=True)                # conservative|balanced|aggressive
    max_tacos = Column(Integer, nullable=True)
    max_price_increase_pct = Column(Numeric(10, 2), nullable=True)
    ad_budget_cap = Column(Numeric(12, 2), nullable=True)
    dont_change_price = Column(Boolean, nullable=True, default=False)
    notes = Column(Text, nullable=True)
    
    summary = Column(Text, nullable=False)
    recommendations = Column(Text, nullable=True)

    def __repr__(self):
        return (
            f"<HistoricAISummary user_id={self.user_id}, "
            f"period={self.period}, timeline={self.timeline}, year={self.year}>"
        )

class LiveAISummary(db.Model):
    __tablename__ = "live_ai_summary"
    __bind_key__ = "chatbot"

    id = Column(Integer, primary_key=True)

    user_id = Column(Integer, nullable=False)
    country = Column(String(20), nullable=False)

    start_date = Column(Date, nullable=False)
    end_date = Column(Date, nullable=False)

    # objective tracking
    objective_hash = Column(String(255), nullable=True)

    # AI outputs
    analysis = Column(Text, nullable=True)
    summary = Column(Text, nullable=True)
    strategy = Column(Text, nullable=True)

    # ✅ Weekly email-only short JSON
    weekly_email_summary_json = Column(Text, nullable=True)

    created_at = Column(DateTime, default=datetime.utcnow)

    def __repr__(self):
        return f"<LiveAISummary user_id={self.user_id}, start={self.start_date}, end={self.end_date}>"


class UserObjective(db.Model):
    __tablename__ = "user_objectives"
    __bind_key__ = "chatbot"

    id = Column(Integer, primary_key=True)

    user_id = Column(Integer, nullable=False)
    country = Column(String(50), nullable=False)

    growth_intent = Column(String(50), nullable=False, default="balanced")
    profit_priority = Column(String(100), nullable=False, default="protect_growth")
    inventory_clearance_priority = Column(Boolean, nullable=False, default=False)

    business_context = Column(Text, nullable=True)
    ai_business_journey = Column(Text, nullable=True)
    website_url = Column(String(500), nullable=True)
    ppt_file_data = Column(db.LargeBinary, nullable=True)
    ppt_file_name = Column(String(255), nullable=True)

    # first day of month, e.g. 2026-03-01
    objective_month = Column(Date, nullable=False)

    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)

    __table_args__ = (
        db.UniqueConstraint(
            "user_id",
            "country",
            "objective_month",
            name="unique_user_country_objective_month"
        ),
    )

    def __repr__(self):
        return (
            f"<UserObjective user_id={self.user_id}, "
            f"country={self.country}, "
            f"objective_month={self.objective_month}, "
            f"growth_intent={self.growth_intent}>"
        )
    
class ProductAISummary(db.Model):
    __tablename__ = "product_ai_summary"
    __bind_key__ = "chatbot"

    id = Column(Integer, primary_key=True)

    user_id = Column(Integer, nullable=False, index=True)

    product_name = Column(Text, nullable=False)
    sku = Column(String(255), nullable=True)

    # us, uk, global, global_inr, global_gbp, global_cad
    country = Column(String(50), nullable=False, index=True)

    home_currency = Column(String(10), nullable=False)

    # Example: 2026_Q1, 2026_Q2
    quarter_key = Column(String(20), nullable=False, index=True)

    summary = Column(Text, nullable=False)
    summary_payload = Column(JSONB, nullable=True)

    generated_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(
        DateTime,
        nullable=False,
        default=datetime.utcnow,
        onupdate=datetime.utcnow
    )

    __table_args__ = (
        UniqueConstraint(
            "user_id",
            "product_name",
            "country",
            "home_currency",
            "quarter_key",
            name="uq_product_ai_summary_user_product_country_currency_quarter"
        ),
        Index(
            "idx_product_ai_summary_lookup",
            "user_id",
            "country",
            "home_currency",
            "quarter_key"
        ),
    )

    def __repr__(self):
        return (
            f"<ProductAISummary user_id={self.user_id}, "
            f"product_name={self.product_name}, "
            f"country={self.country}, "
            f"quarter_key={self.quarter_key}>"
        )



# ------------------------------------------------- Shopify Models -------------------------------------------------

class ShopifyStore(db.Model):
    __tablename__ = 'shopify_stores'
    __bind_key__ = 'shopify'  

    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, nullable=False)
    shop_name = Column(String(255), unique=True, nullable=False)  # e.g., myshop.myshopify.com
    access_token = Column(String(500), nullable=False)
    email = Column(String(150), nullable=True)
    installed_at = Column(DateTime(timezone=True), server_default=func.now())
    is_active = Column(Boolean, default=True, nullable=False)

class UploadShopify(db.Model):
    __bind_key__ = 'shopify'
    __tablename__ = 'upload_shopify'

    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer)
    month = db.Column(db.String)
    year = db.Column(db.Integer)
    total_discounts = db.Column(db.Float)
    total_price = db.Column(db.Float)
    total_tax = db.Column(db.Float)
    total_orders = db.Column(db.Integer)
    net_sales = db.Column(db.Float)

    __table_args__ = (db.UniqueConstraint('user_id', 'month', 'year'),)


# ------------------------------------------------- Amazon Models -------------------------------------------------


class SettlementTransaction(db.Model):
    __tablename__ = "settlement_transactions"
    __bind_key__ = "amazon"

    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, index=True)
    settlement_id = db.Column(db.String(255), index=True)

    # columns from the report
    date_time = db.Column(db.DateTime, index=True)
    transaction_type = db.Column(db.String(100))
    order_id = db.Column(db.String(50), index=True)
    sku = db.Column(db.String(100), index=True)
    description = db.Column(db.Text)
    quantity = db.Column(db.Integer)
    marketplace = db.Column(db.String(100))
    fulfilment = db.Column(db.String(50))
    order_city = db.Column(db.String(100))
    order_state = db.Column(db.String(100))
    order_postal = db.Column(db.String(20))
    tax_collection_model = db.Column(db.String(50))

    # amounts
    product_sales = db.Column(db.Numeric(12, 2))
    product_sales_tax = db.Column(db.Numeric(12, 2))
    postage_credits = db.Column(db.Numeric(12, 2))
    shipping_credits_tax = db.Column(db.Numeric(12, 2))
    gift_wrap_credits = db.Column(db.Numeric(12, 2))
    giftwrap_credits_tax = db.Column(db.Numeric(12, 2))
    promotional_rebates = db.Column(db.Numeric(12, 2))
    promotional_rebates_tax = db.Column(db.Numeric(12, 2))
    marketplace_withheld_tax = db.Column(db.Numeric(12, 2))
    selling_fees = db.Column(db.Numeric(12, 2))
    fba_fees = db.Column(db.Numeric(12, 2))
    other_transaction_fees = db.Column(db.Numeric(12, 2))
    other = db.Column(db.Numeric(12, 2))
    total = db.Column(db.Numeric(12, 2))

    # NEW fields
    advertising_cost = db.Column(db.Numeric(12, 2))   # Cost of Advertisement
    platform_fees = db.Column(db.Numeric(12, 2))      # Selling + FBA + Other Txn Fees
    net_reimbursement = db.Column(db.Numeric(12, 2))  # Equal to 'total' after all sums

    # housekeeping
    currency = db.Column(db.String(10))
    synced_at = db.Column(db.DateTime, default=datetime.utcnow, index=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)


class Fee(db.Model):
    __tablename__ = 'fees'
    __bind_key__ = 'amazon'

    id = db.Column(db.Integer, primary_key=True)

    # who/where
    user_id = db.Column(db.Integer, index=True)
    sku = db.Column(db.String(255), nullable=False, index=True)
    marketplace_id = db.Column(db.String(255), nullable=False, index=True)

    # identifiers / product meta
    fnsku = db.Column(db.String(255))
    asin = db.Column(db.String(255), index=True)
    amazon_store = db.Column(db.String(255))              # "amazon-store"
    product_name = db.Column(db.Text)                     # "product-name"
    product_group = db.Column(db.String(255))             # "product-group"
    brand = db.Column(db.String(255))
    fulfilled_by = db.Column(db.String(50))               # "fulfilled-by" (FBA/FBM)
    has_local_inventory = db.Column(db.Boolean)           # "has-local-inventory"

    # prices
    your_price = db.Column(db.Numeric(12, 2))
    sales_price = db.Column(db.Numeric(12, 2))

    # dimensions & weight
    longest_side = db.Column(db.Numeric(12, 3))
    median_side = db.Column(db.Numeric(12, 3))
    shortest_side = db.Column(db.Numeric(12, 3))
    length_and_girth = db.Column(db.Numeric(12, 3))
    unit_of_dimension = db.Column(db.String(50))
    item_package_weight = db.Column(db.Numeric(12, 3))
    unit_of_weight = db.Column(db.String(50))
    product_size_weight_band = db.Column(db.String(255))

    # currency (row currency for estimates)
    currency = db.Column(db.String(10))

    # estimates / fees
    estimated_fee_total = db.Column(db.Numeric(12, 2))
    estimated_referral_fee_per_unit = db.Column(db.Numeric(12, 2))
    estimated_variable_closing_fee = db.Column(db.Numeric(12, 2))
    estimated_order_handling_fee_per_order = db.Column(db.Numeric(12, 2))
    expected_domestic_fulfilment_fee_per_unit = db.Column(db.Numeric(12, 2))
    expected_efn_fulfilment_fee_per_unit_uk = db.Column(db.Numeric(12, 2))
    expected_efn_fulfilment_fee_per_unit_de = db.Column(db.Numeric(12, 2))
    expected_efn_fulfilment_fee_per_unit_fr = db.Column(db.Numeric(12, 2))
    expected_efn_fulfilment_fee_per_unit_it = db.Column(db.Numeric(12, 2))
    expected_efn_fulfilment_fee_per_unit_es = db.Column(db.Numeric(12, 2))
    expected_efn_fulfilment_fee_per_unit_se = db.Column(db.Numeric(12, 2))

    # bookkeeping
    synced_at = db.Column(db.DateTime, default=datetime.utcnow, index=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

    __table_args__ = (
        db.UniqueConstraint('user_id', 'sku', 'marketplace_id', name='uq_fee_user_sku_mkt'),
    ) 

# --------------------------------- Inventory model ---------------------------------

class Inventory(db.Model):
    __tablename__ = 'inventory'
    __bind_key__ = 'amazon'

    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer)
    asin = db.Column(db.String(255), index=True)
    seller_sku = db.Column(db.String(255), index=True)
    marketplace_id = db.Column(db.String(255), index=True)
    product_name = db.Column(db.String(512)) # <-- NEW
    total_quantity = db.Column(db.Integer, default=0)
    inbound_quantity = db.Column(db.Integer, default=0)
    available_quantity = db.Column(db.Integer, default=0)
    reserved_quantity = db.Column(db.Integer, default=0)
    fulfillable_quantity = db.Column(db.Integer, default=0)
    synced_at = db.Column(db.DateTime, default=datetime.utcnow, index=True)
    inventory_age_days = db.Column(db.Integer, default=0)

    __table_args__ = (
        UniqueConstraint('seller_sku', 'marketplace_id', name='uq_inventory_sku_mkt'),
    )


# --------------------------------- InventoryAged model ---------------------------------

class InventoryAged(db.Model):
    __tablename__ = "inventory_aged"
    __bind_key__ = "amazon"

    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, index=True)

    # ----- basic identifiers -----
    snapshot_date = db.Column("snapshot-date", db.Date, index=True)
    sku = db.Column("sku", db.String(255), index=True)
    fnsku = db.Column("fnsku", db.String(255))
    asin = db.Column("asin", db.String(255), index=True)
    product_name = db.Column("product-name", db.String(512))
    condition = db.Column("condition", db.String(50))

    # ----- quantities & age buckets (main ones) -----
    available = db.Column("available", db.Integer, default=0)
    pending_removal_quantity = db.Column(
        "pending-removal-quantity", db.Integer, default=0
    )

    inv_age_0_90 = db.Column("inv-age-0-to-90-days", db.Integer, default=0)
    inv_age_91_180 = db.Column("inv-age-91-to-180-days", db.Integer, default=0)
    inv_age_181_270 = db.Column("inv-age-181-to-270-days", db.Integer, default=0)
    inv_age_271_365 = db.Column("inv-age-271-to-365-days", db.Integer, default=0)
    inv_age_365_plus = db.Column("inv-age-365-plus-days", db.Integer, default=0)

    currency = db.Column("currency", db.String(10))

    # ----- shipped units (time windows) -----
    units_shipped_t7 = db.Column("units-shipped-t7", db.Integer, default=0)
    units_shipped_t30 = db.Column("units-shipped-t30", db.Integer, default=0)
    units_shipped_t60 = db.Column("units-shipped-t60", db.Integer, default=0)
    units_shipped_t90 = db.Column("units-shipped-t90", db.Integer, default=0)

    # ----- pricing & alerts -----
    alert = db.Column("alert", db.String(255))
    your_price = db.Column("your-price", db.Float)
    sales_price = db.Column("sales-price", db.Float)
    lowest_price_new_plus_shipping = db.Column(
        "lowest-price-new-plus-shipping", db.Float
    )
    lowest_price_used = db.Column("lowest-price-used", db.Float)
    recommended_action = db.Column("recommended-action", db.String(255))
    healthy_inventory_level = db.Column("healthy-inventory-level", db.Float)
    recommended_sales_price = db.Column("recommended-sales-price", db.Float)
    recommended_sale_duration_days = db.Column(
        "recommended-sale-duration-days", db.Integer
    )
    recommended_removal_quantity = db.Column(
        "recommended-removal-quantity", db.Integer, default=0
    )
    estimated_cost_savings_recommended_actions = db.Column(
        "estimated-cost-savings-of-recommended-actions", db.Float
    )

    sell_through = db.Column("sell-through", db.Float)

    # ----- volume & storage -----
    item_volume = db.Column("item-volume", db.Float)
    volume_unit_measurement = db.Column("volume-unit-measurement", db.String(50))
    storage_type = db.Column("storage-type", db.String(50))
    storage_volume = db.Column("storage-volume", db.Float)

    # ----- catalog / marketplace info -----
    marketplace = db.Column("marketplace", db.String(50))
    product_group = db.Column("product-group", db.String(255))
    sales_rank = db.Column("sales-rank", db.Integer)

    # ----- supply / excess / cover -----
    days_of_supply = db.Column("days-of-supply", db.Float)
    estimated_excess_quantity = db.Column("estimated-excess-quantity", db.Integer)
    weeks_of_cover_t30 = db.Column("weeks-of-cover-t30", db.Float)
    weeks_of_cover_t90 = db.Column("weeks-of-cover-t90", db.Float)

    featuredoffer_price = db.Column("featuredoffer-price", db.Float)

    sales_shipped_last_7_days = db.Column(
        "sales-shipped-last-7-days", db.Integer, default=0
    )
    sales_shipped_last_30_days = db.Column(
        "sales-shipped-last-30-days", db.Integer, default=0
    )
    sales_shipped_last_60_days = db.Column(
        "sales-shipped-last-60-days", db.Integer, default=0
    )
    sales_shipped_last_90_days = db.Column(
        "sales-shipped-last-90-days", db.Integer, default=0
    )

    # ----- more detailed age buckets -----
    inv_age_0_30 = db.Column("inv-age-0-to-30-days", db.Integer, default=0)
    inv_age_31_60 = db.Column("inv-age-31-to-60-days", db.Integer, default=0)
    inv_age_61_90 = db.Column("inv-age-61-to-90-days", db.Integer, default=0)
    inv_age_181_330 = db.Column("inv-age-181-to-330-days", db.Integer, default=0)
    inv_age_331_365 = db.Column("inv-age-331-to-365-days", db.Integer, default=0)

    estimated_storage_cost_next_month = db.Column(
        "estimated-storage-cost-next-month", db.Float
    )

    # ----- inbound / reserved / unfulfillable -----
    inbound_quantity = db.Column("inbound-quantity", db.Integer, default=0)
    inbound_working = db.Column("inbound-working", db.Integer, default=0)
    inbound_shipped = db.Column("inbound-shipped", db.Integer, default=0)
    inbound_received = db.Column("inbound-received", db.Integer, default=0)

    total_reserved_quantity = db.Column(
        "Total Reserved Quantity", db.Integer, default=0
    )
    unfulfillable_quantity = db.Column(
        "unfulfillable-quantity", db.Integer, default=0
    )

    qty_charged_ais_241_270 = db.Column(
        "quantity-to-be-charged-ais-241-270-days", db.Integer, default=0
    )
    est_ais_241_270 = db.Column("estimated-ais-241-270-days", db.Float)

    qty_charged_ais_271_300 = db.Column(
        "quantity-to-be-charged-ais-271-300-days", db.Integer, default=0
    )
    est_ais_271_300 = db.Column("estimated-ais-271-300-days", db.Float)

    qty_charged_ais_301_330 = db.Column(
        "quantity-to-be-charged-ais-301-330-days", db.Integer, default=0
    )
    est_ais_301_330 = db.Column("estimated-ais-301-330-days", db.Float)

    qty_charged_ais_331_365 = db.Column(
        "quantity-to-be-charged-ais-331-365-days", db.Integer, default=0
    )
    est_ais_331_365 = db.Column("estimated-ais-331-365-days", db.Float)

    qty_charged_ais_365_plus = db.Column(
        "quantity-to-be-charged-ais-365-plus-days", db.Integer, default=0
    )
    est_ais_365_plus = db.Column("estimated-ais-365-plus-days", db.Float)

    # ----- historical supply / recommendations -----
    historical_days_of_supply = db.Column(
        "historical-days-of-supply", db.Float
    )
    recommended_ship_in_quantity = db.Column(
        "Recommended ship-in quantity", db.Integer
    )
    recommended_ship_in_date = db.Column(
        "Recommended ship-in date", db.Date
    )
    last_updated_historical_dos = db.Column(
        "Last updated date for Historical Days of Supply", db.Date
    )
    short_term_historical_dos = db.Column(
        "Short term historical days of supply", db.Float
    )
    long_term_historical_dos = db.Column(
        "Long term historical days of supply", db.Float
    )
    inventory_age_snapshot_date = db.Column(
        "Inventory age snapshot date", db.Date
    )

    # ----- inventory / reserved at FBA -----
    inventory_supply_at_fba = db.Column(
        "Inventory Supply at FBA", db.Integer, default=0
    )
    reserved_fc_transfer = db.Column(
        "Reserved FC Transfer", db.Integer, default=0
    )
    # ----- Seller Central extra columns not currently stored -----
    fc_transfer = db.Column("fc-transfer", db.Integer, default=0)

    inv_age_366_455 = db.Column("inv-age-366-to-455-days", db.Integer, default=0)
    inv_age_456_plus = db.Column("inv-age-456-plus-days", db.Integer, default=0)

    deprecated_healthy_inventory_level = db.Column(
        "DEPRECATED healthy-inventory-level", db.Float
    )

    no_sale_last_6_months = db.Column("no-sale-last-6-months", db.String(50))

    qty_charged_ais_181_210 = db.Column(
        "quantity-to-be-charged-ais-181-210-days", db.Integer, default=0
    )
    est_ais_181_210 = db.Column("estimated-ais-181-210-days", db.Float)

    qty_charged_ais_211_240 = db.Column(
        "quantity-to-be-charged-ais-211-240-days", db.Integer, default=0
    )
    est_ais_211_240 = db.Column("estimated-ais-211-240-days", db.Float)

    qty_charged_ais_366_455 = db.Column(
        "quantity-to-be-charged-ais-366-455-days", db.Integer, default=0
    )
    est_ais_366_455 = db.Column("estimated-ais-366-455-days", db.Float)

    qty_charged_ais_456_plus = db.Column(
        "quantity-to-be-charged-ais-456-plus-days", db.Integer, default=0
    )
    est_ais_456_plus = db.Column("estimated-ais-456-plus-days", db.Float)

    fba_minimum_inventory_level = db.Column(
        "fba-minimum-inventory-level", db.Integer, default=0
    )
    fba_inventory_level_health_status = db.Column(
        "fba-inventory-level-health-status", db.String(255)
    )

    exempted_low_inventory_fee = db.Column(
        "Exempted from Low-Inventory-Level fee?", db.String(50)
    )
    low_inventory_fee_current_week = db.Column(
        "Low-Inventory-Level fee applied in current week?", db.String(50)
    )

    reserved_staging = db.Column("Reserved Staging", db.Integer, default=0)

    supplier = db.Column("supplier", db.String(255))
    is_seasonal_next_3_months = db.Column(
        "is-seasonal-in-next-3-months", db.String(50)
    )
    season_name = db.Column("season-name", db.String(255))
    season_start_date = db.Column("season-start-date", db.String(50))
    season_end_date = db.Column("season-end-date", db.String(50))
    reserved_fc_processing = db.Column(
        "Reserved FC Processing", db.Integer, default=0
    )
    reserved_customer_order = db.Column(
        "Reserved Customer Order", db.Integer, default=0
    )
    total_days_of_supply_incl_open_shipments = db.Column(
        "Total Days of Supply (including units from open shipments)",
        db.Float,
    )

#---------------------------------- InventoryAgedHistory model -------------------------------------

class InventoryAgedHistory(db.Model):
    __tablename__ = "inventory_aged_history"
    __bind_key__  = "amazon"

    id = db.Column(db.BigInteger, primary_key=True)

    user_id = db.Column(db.Integer, nullable=False, index=True)
    marketplace_id = db.Column(db.String(50), nullable=False, index=True)

    report_id = db.Column(db.String(100), nullable=True)
    document_id = db.Column(db.String(255), nullable=True)

    snapshot_date = db.Column(db.DateTime, nullable=True, index=True)

    sku = db.Column(db.String(255), nullable=True, index=True)
    fnsku = db.Column(db.String(255), nullable=True)
    asin = db.Column(db.String(50), nullable=True, index=True)
    product_name = db.Column(db.Text, nullable=True)
    condition = db.Column(db.String(100), nullable=True)

    per_unit_volume = db.Column(db.Float, nullable=True)
    currency = db.Column(db.String(20), nullable=True)
    volume_unit = db.Column(db.String(50), nullable=True)
    country = db.Column(db.String(20), nullable=True)

    qty_charged = db.Column(db.Integer, nullable=False, default=0)
    amount_charged = db.Column(db.Float, nullable=False, default=0.0)
    surcharge_age_tier = db.Column(db.String(50), nullable=True)
    rate_surcharge = db.Column(db.Float, nullable=False, default=0.0)

    synced_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
    updated_at = db.Column(
        db.DateTime,
        nullable=False,
        default=datetime.utcnow,
        onupdate=datetime.utcnow,
    )

    __table_args__ = (
        db.UniqueConstraint(
            "user_id",
            "marketplace_id",
            "snapshot_date",
            "sku",
            "fnsku",
            "asin",
            "country",
            "currency",
            "surcharge_age_tier",
            name="uq_inventory_aged_history_key",
        ),
    )

# --------------------------------- InventoryAWD model ---------------------------------

class InventoryAWD(db.Model):
    __tablename__ = "inventory_awd"
    __bind_key__ = "amazon"

    id = db.Column(db.Integer, primary_key=True)

    user_id = db.Column(db.Integer, nullable=False, index=True)
    marketplace_id = db.Column(db.String(50), nullable=False, index=True)

    sku = db.Column(db.String(255), nullable=False, index=True)

    total_onhand_quantity = db.Column(db.Integer, default=0)
    total_inbound_quantity = db.Column(db.Integer, default=0)

    available_distributable_quantity = db.Column(db.Integer, default=0)
    reserved_distributable_quantity = db.Column(db.Integer, default=0)
    replenishment_quantity = db.Column(db.Integer, default=0)

    # Store Amazon's nested expirationDetails array as JSON
    expiration_details = db.Column(JSON, nullable=True)

    synced_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False, index=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
    updated_at = db.Column(
        db.DateTime,
        default=datetime.utcnow,
        onupdate=datetime.utcnow,
        nullable=False,
    )

    __table_args__ = (
        db.UniqueConstraint(
            "user_id",
            "marketplace_id",
            "sku",
            name="uq_inventory_awd_user_marketplace_sku",
        ),
    )
    
# --------------------------------- MonthwiseInventory model ---------------------------------

class MonthwiseInventory(db.Model):
    __tablename__ = "monthwise_inventory"
    __bind_key__ = "amazon"

    id = db.Column(db.Integer, primary_key=True)

    user_id = db.Column(db.Integer, index=True, nullable=True)
    marketplace_id = db.Column(db.String(50), index=True, nullable=False)

    # Report columns
    date = db.Column(db.Date, index=True, nullable=False)
    fnsku = db.Column(db.String(64), index=True, nullable=True)
    asin = db.Column(db.String(64), index=True, nullable=True)
    msku = db.Column(db.String(255), index=True, nullable=False)
    title = db.Column(db.Text)
    disposition = db.Column(db.String(64), nullable=True)
    product_name = db.Column(db.String(255))

    starting_warehouse_balance = db.Column(db.Integer, default=0)
    in_transit_between_warehouses = db.Column(db.Integer, default=0)
    receipts = db.Column(db.Integer, default=0)
    customer_shipments = db.Column(db.Integer, default=0)
    customer_returns = db.Column(db.Integer, default=0)
    vendor_returns = db.Column(db.Integer, default=0)
    warehouse_transfer_in_out = db.Column(db.Integer, default=0)
    found = db.Column(db.Integer, default=0)
    lost = db.Column(db.Integer, default=0)
    damaged = db.Column(db.Integer, default=0)
    disposed = db.Column(db.Integer, default=0)
    other_events = db.Column(db.Integer, default=0)
    ending_warehouse_balance = db.Column(db.Integer, default=0)
    unknown_events = db.Column(db.Integer, default=0)

    location = db.Column(db.String(32), nullable=True)  # GB, US, FC code etc.
    synced_at = db.Column(db.DateTime, default=datetime.utcnow, index=True)

    __table_args__ = (
        # ✅ IMPORTANT: include msku + asin so rows don't overwrite
        UniqueConstraint(
            "user_id",
            "marketplace_id",
            "date",
            "msku",
            "asin",
            "disposition",
            "location",
            name="uq_monthwise_inv_key",
        ),
    )


# --------------------------------- Order model ---------------------------------


class Liveorder(db.Model):
    __tablename__ = 'liveorders'
    __bind_key__ = 'amazon'

    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, index=True)

    # ✅ allow NULL + duplicates
    amazon_order_id = db.Column(db.String(255), nullable=True, index=True)

    # ✅ new transaction key (unique per user)
    tx_key = db.Column(db.Text, nullable=False, index=True)

    __table_args__ = (
        db.UniqueConstraint('user_id', 'tx_key', name='uq_liveorders_user_tx_key'),
    )

    purchase_date = db.Column(db.DateTime, index=True)
    order_status = db.Column(db.String(50), index=True)
    sku = db.Column(db.String(255), index=True)
    quantity = db.Column(db.Integer)
    cogs = db.Column(db.Float, default=0.0)
    profit = db.Column(db.Float, default=0.0)

    type = db.Column(db.String(100))
    description = db.Column(db.Text)
    marketplace = db.Column(db.String(255))

    product_sales = db.Column(db.Float, default=0.0)
    gross_sales = db.Column(db.Float, default=0.0, nullable=False)
    product_sales_tax = db.Column(db.Float, default=0.0)
    postage_credits = db.Column(db.Float, default=0.0)
    shipping_credits = db.Column(db.Float, default=0.0)
    shipping_credits_tax = db.Column(db.Float, default=0.0)
    gift_wrap_credits = db.Column(db.Float, default=0.0)
    giftwrap_credits_tax = db.Column(db.Float, default=0.0)
    promotional_rebates = db.Column(db.Float, default=0.0)
    promotional_rebates_tax = db.Column(db.Float, default=0.0)
    marketplace_facilitator_tax = db.Column(db.Float, default=0.0)
    selling_fees = db.Column(db.Float, default=0.0)
    fba_fees = db.Column(db.Float, default=0.0)
    other_transaction_fees = db.Column(db.Float, default=0.0)
    other = db.Column(db.Float, default=0.0)
    total = db.Column(db.Float, default=0.0)
    bucket = db.Column(db.String(50))


class amazon_user(db.Model):
    __tablename__ = 'amazon_user'
    __bind_key__ = 'amazon'

    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, nullable=False, index=True)

    refresh_token = db.Column(db.Text, nullable=False)
    region = db.Column(db.String(50), nullable=False)
    marketplace_id = db.Column(db.String(20), nullable=False)
    marketplace_name = db.Column(db.String(255), nullable=True)
    currency = db.Column(db.String(10), nullable=True)
    seller_id = db.Column(db.String(64), nullable=True)

    country_code = db.Column(db.String(10), nullable=True)
    country_name = db.Column(db.String(50), nullable=True)

    stock_unit = db.Column(db.Integer, nullable=True)
    transit_time = db.Column(db.Integer, nullable=True)

    is_connected = db.Column(db.Boolean, default=False)

    amazon_ads_refresh_token = db.Column(db.Text, nullable=True)
    amazon_ads_refresh_token_updated_at = db.Column(db.DateTime, nullable=True)

    amazon_ads_profile_id = db.Column(db.String(32), nullable=True)
    amazon_ads_manager_profile_id = db.Column(db.String(32), nullable=True)

    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    __table_args__ = (
        db.UniqueConstraint('user_id', 'marketplace_id', name='uq_user_marketplace'),
    )

class amazon_sponsored_products(db.Model):
    __tablename__ = "amazon_sponsored_products"
    __bind_key__ = "amazon"

    id = db.Column(db.Integer, primary_key=True)

    # ownership / traceability
    user_id = db.Column(db.Integer, nullable=False, index=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

    # report dimensions
    start_date = db.Column(db.Date, nullable=False, index=True)
    end_date = db.Column(db.Date, nullable=False, index=True)
    time_unit = db.Column(db.String(20), nullable=True, index=True)
    country = db.Column(db.String(8), nullable=True, index=True)
    profile_id = db.Column(db.String(32), nullable=True, index=True)

    portfolio_id = db.Column(db.String(64), nullable=True)
    currency = db.Column(db.String(16), nullable=True)

    campaign_id = db.Column(db.String(64), nullable=True, index=True)
    campaign_name = db.Column(db.String(512), nullable=True)

    ad_group_id = db.Column(db.String(64), nullable=True, index=True)
    ad_group_name = db.Column(db.String(512), nullable=True)

    advertised_sku = db.Column(db.String(128), nullable=True, index=True)
    advertised_asin = db.Column(db.String(32), nullable=True, index=True)

    # metrics
    impressions = db.Column(db.BigInteger, nullable=True)
    clicks = db.Column(db.BigInteger, nullable=True)

    ctr = db.Column(db.Float, nullable=True)   # Click-Thru Rate (CTR)
    cpc = db.Column(db.Float, nullable=True)   # Cost Per Click (CPC)

    spend = db.Column(db.Float, nullable=True)

    sales_7d = db.Column(db.Float, nullable=True)
    orders_7d = db.Column(db.Float, nullable=True)
    units_7d = db.Column(db.Float, nullable=True)

    acos = db.Column(db.Float, nullable=True)
    roas = db.Column(db.Float, nullable=True)
    conv_rate_7d = db.Column(db.Float, nullable=True)

    adv_sku_sales_7d = db.Column(db.Float, nullable=True)
    other_sku_sales_7d = db.Column(db.Float, nullable=True)

    adv_sku_orders_7d = db.Column(db.Float, nullable=True)
    adv_sku_units_7d = db.Column(db.Float, nullable=True)
    other_sku_units_7d = db.Column(db.Float, nullable=True)

    # optional: prevent duplicates
    __table_args__ = (
        db.UniqueConstraint(
            "user_id",
            "start_date",
            "end_date",
            "country",
            "profile_id",
            "campaign_id",
            "ad_group_id",
            "advertised_sku",
            "advertised_asin",
            name="uq_sp_report_row",
        ),
    )


class amazon_sponsored_display_advertised_products(db.Model):
    __tablename__ = "amazon_sponsored_display_advertised_products"
    __bind_key__ = "amazon"

    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, nullable=False, index=True)

    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

    start_date = db.Column(db.Date, nullable=False, index=True)
    end_date = db.Column(db.Date, nullable=False, index=True)
    # SUMMARY or DAILY
    time_unit = db.Column(db.String(20), nullable=True, index=True)
    country = db.Column(db.String(8), nullable=True)
    profile_id = db.Column(db.String(32), nullable=True, index=True)

    campaign_id = db.Column(db.String(32), nullable=True)
    campaign_name = db.Column(db.String(512), nullable=True)

    ad_group_id = db.Column(db.String(32), nullable=True)
    ad_group_name = db.Column(db.String(512), nullable=True)

    advertised_sku = db.Column(db.String(64), nullable=True)
    advertised_asin = db.Column(db.String(32), nullable=True)

    currency = db.Column(db.String(16), nullable=True)

    impressions = db.Column(db.BigInteger)
    clicks = db.Column(db.BigInteger)
    spend = db.Column(db.Float)
    cpc = db.Column(db.Float)
    ctr = db.Column(db.Float)

    sales_14d = db.Column(db.Float)
    orders_14d = db.Column(db.BigInteger)
    units_14d = db.Column(db.BigInteger)

    acos = db.Column(db.Float)
    roas = db.Column(db.Float)

    __table_args__ = (
        db.UniqueConstraint(
            "user_id",
            "start_date",
            "end_date",
            "profile_id",
            "campaign_id",
            "advertised_sku",
            name="uq_sd_adv_product",
        ),
    )


class amazon_sponsored_brands_keywords(db.Model):
    __tablename__ = "amazon_sponsored_brands_keywords"
    __bind_key__ = "amazon"

    id = db.Column(db.Integer, primary_key=True)

    # ownership / traceability
    user_id = db.Column(db.Integer, nullable=False, index=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

    # report dimensions
    start_date = db.Column(db.Date, nullable=False, index=True)
    end_date = db.Column(db.Date, nullable=False, index=True)
    # SUMMARY or DAILY
    time_unit = db.Column(db.String(20), nullable=True, index=True)
    country = db.Column(db.String(8), nullable=True, index=True)
    profile_id = db.Column(db.String(32), nullable=True, index=True)

    # IDs
    campaign_id = db.Column(db.String(64), nullable=True, index=True)
    ad_group_id = db.Column(db.String(64), nullable=True, index=True)
    keyword_id = db.Column(db.String(64), nullable=True, index=True)
    targeting_id = db.Column(db.String(64), nullable=True, index=True)

    portfolio_name = db.Column(db.String(255), nullable=True)
    currency = db.Column(db.String(16), nullable=True)

    campaign_name = db.Column(db.String(512), nullable=True, index=True)
    campaign_status = db.Column(db.String(64), nullable=True)
    campaign_budget_amount = db.Column(db.Float, nullable=True)
    campaign_budget_type = db.Column(db.String(64), nullable=True)

    ad_group_name = db.Column(db.String(512), nullable=True)

    targeting = db.Column(db.Text, nullable=True)
    targeting_type = db.Column(db.String(64), nullable=True)

    match_type = db.Column(db.String(64), nullable=True)
    cost_type = db.Column(db.String(64), nullable=True)

    keyword_bid = db.Column(db.Float, nullable=True)
    keyword_status = db.Column(db.String(64), nullable=True)
    keyword_type = db.Column(db.String(64), nullable=True)

    # traffic metrics
    impressions = db.Column(db.BigInteger, nullable=True)
    top_of_search_impression_share = db.Column(db.Float, nullable=True)
    viewable_impressions = db.Column(db.BigInteger, nullable=True)
    viewability_rate = db.Column(db.Float, nullable=True)
    view_click_through_rate = db.Column(db.Float, nullable=True)

    clicks = db.Column(db.BigInteger, nullable=True)
    ctr = db.Column(db.Float, nullable=True)
    spend = db.Column(db.Float, nullable=True)
    cpc = db.Column(db.Float, nullable=True)

    # sales / conversion metrics
    sales = db.Column(db.Float, nullable=True)
    sales_clicks = db.Column(db.Float, nullable=True)
    sales_promoted = db.Column(db.Float, nullable=True)

    orders = db.Column(db.Float, nullable=True)
    orders_clicks = db.Column(db.Float, nullable=True)
    orders_promoted = db.Column(db.Float, nullable=True)

    units = db.Column(db.Float, nullable=True)
    units_clicks = db.Column(db.Float, nullable=True)

    acos = db.Column(db.Float, nullable=True)
    roas = db.Column(db.Float, nullable=True)
    conversion_rate = db.Column(db.Float, nullable=True)

    # brand/search/product interaction metrics
    branded_searches = db.Column(db.Float, nullable=True)
    branded_searches_clicks = db.Column(db.Float, nullable=True)

    detail_page_views = db.Column(db.Float, nullable=True)
    detail_page_views_clicks = db.Column(db.Float, nullable=True)

    add_to_cart = db.Column(db.Float, nullable=True)
    add_to_cart_clicks = db.Column(db.Float, nullable=True)
    add_to_cart_rate = db.Column(db.Float, nullable=True)
    ecp_add_to_cart = db.Column(db.Float, nullable=True)

    # new-to-brand metrics
    new_to_brand_sales = db.Column(db.Float, nullable=True)
    new_to_brand_sales_clicks = db.Column(db.Float, nullable=True)
    new_to_brand_sales_percentage = db.Column(db.Float, nullable=True)

    new_to_brand_purchases = db.Column(db.Float, nullable=True)
    new_to_brand_purchases_clicks = db.Column(db.Float, nullable=True)
    new_to_brand_purchases_percentage = db.Column(db.Float, nullable=True)
    new_to_brand_purchases_rate = db.Column(db.Float, nullable=True)

    new_to_brand_units_sold = db.Column(db.Float, nullable=True)
    new_to_brand_units_sold_clicks = db.Column(db.Float, nullable=True)
    new_to_brand_units_sold_percentage = db.Column(db.Float, nullable=True)

    new_to_brand_detail_page_views = db.Column(db.Float, nullable=True)
    new_to_brand_detail_page_views_clicks = db.Column(db.Float, nullable=True)
    new_to_brand_detail_page_view_rate = db.Column(db.Float, nullable=True)
    new_to_brand_ecp_detail_page_view = db.Column(db.Float, nullable=True)

    # video metrics
    video_5_second_views = db.Column(db.Float, nullable=True)
    video_5_second_view_rate = db.Column(db.Float, nullable=True)
    video_first_quartile_views = db.Column(db.Float, nullable=True)
    video_midpoint_views = db.Column(db.Float, nullable=True)
    video_third_quartile_views = db.Column(db.Float, nullable=True)
    video_complete_views = db.Column(db.Float, nullable=True)
    video_unmutes = db.Column(db.Float, nullable=True)

    # book / list metrics
    qualified_borrows = db.Column(db.Float, nullable=True)
    qualified_borrows_from_clicks = db.Column(db.Float, nullable=True)
    royalty_qualified_borrows = db.Column(db.Float, nullable=True)
    royalty_qualified_borrows_from_clicks = db.Column(db.Float, nullable=True)

    add_to_list = db.Column(db.Float, nullable=True)
    add_to_list_from_clicks = db.Column(db.Float, nullable=True)

    __table_args__ = (
        db.UniqueConstraint(
            "user_id",
            "start_date",
            "end_date",
            "country",
            "profile_id",
            "campaign_id",
            "ad_group_id",
            "keyword_id",
            "targeting_id",
            name="uq_sb_keyword_row",
        ),
    )

    
from datetime import datetime
from sqlalchemy import UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
from app import db


class Product(db.Model):
    __tablename__ = 'products'
    __bind_key__ = 'amazon'

    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, index=True)

    sku = db.Column(db.String(255), nullable=False, index=True)
    asin = db.Column(db.String(255), index=True)
    fn_sku = db.Column(db.String(255), index=True)

    marketplace_id = db.Column(db.String(255), nullable=False, index=True)

    product_type = db.Column(db.String(150), index=True)
    condition_type = db.Column(db.String(100))

    status = db.Column(db.String(255), default='Active', index=True)

    title = db.Column(db.Text)
    brand = db.Column(db.String(255), index=True)
    category = db.Column(db.String(255), index=True)
    manufacturer = db.Column(db.String(255))

    description = db.Column(db.Text)
    bullet_points = db.Column(JSONB)
    generic_keywords = db.Column(JSONB)

    main_image_url = db.Column(db.Text)
    image_urls = db.Column(JSONB)

    parent_sku = db.Column(db.String(255), index=True)
    parentage_level = db.Column(db.String(50), index=True)
    variation_theme = db.Column(db.String(255))

    external_product_id = db.Column(db.String(255), index=True)
    external_product_id_type = db.Column(db.String(50))

    price_amount = db.Column(db.Numeric(12, 2))
    price_currency = db.Column(db.String(10))
    list_price_amount = db.Column(db.Numeric(12, 2))
    list_price_currency = db.Column(db.String(10))

    fulfillment_channel = db.Column(db.String(255))
    fulfillment_availability = db.Column(JSONB)

    quantity = db.Column(db.Integer)

    country_of_origin = db.Column(db.String(50))
    item_form = db.Column(db.String(255))
    size = db.Column(db.String(255))
    color = db.Column(db.String(255))
    scent = db.Column(db.String(255))
    unit_count = db.Column(db.String(100))

    item_weight_value = db.Column(db.Numeric(12, 3))
    item_weight_unit = db.Column(db.String(50))
    package_weight_value = db.Column(db.Numeric(12, 3))
    package_weight_unit = db.Column(db.String(50))

    item_dimensions = db.Column(JSONB)
    package_dimensions = db.Column(JSONB)

    is_expiration_dated_product = db.Column(db.Boolean)
    is_heat_sensitive = db.Column(db.Boolean)
    contains_liquid_contents = db.Column(db.Boolean)

    fc_shelf_life_days = db.Column(db.Integer)

    issues = db.Column(JSONB)
    offers = db.Column(JSONB)
    attributes = db.Column(JSONB)
    summaries = db.Column(JSONB)

    # Store complete normalized API object here, so no data is lost
    product_data = db.Column(JSONB)

    # Amazon listing creation date
    open_date = db.Column(db.DateTime, index=True)

    amazon_last_updated_at = db.Column(db.DateTime, index=True)
    synced_at = db.Column(db.DateTime, default=datetime.utcnow, index=True)

    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
    updated_at = db.Column(
        db.DateTime,
        default=datetime.utcnow,
        onupdate=datetime.utcnow,
        nullable=False
    )

    __table_args__ = (
        UniqueConstraint('sku', 'marketplace_id', name='uq_products_sku_mkt'),
    )
    