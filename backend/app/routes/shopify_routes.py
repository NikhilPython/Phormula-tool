from flask import Blueprint, redirect, request, jsonify, current_app
import os, jwt, math, json, calendar, requests, pandas as pd
from dotenv import load_dotenv
from sqlalchemy import text
from app.utils.token_utils import get_effective_user_id_from_token
from datetime import datetime, timedelta, timezone
from app.models.user_models import ShopifyStore
from config import Config
from app import db

load_dotenv()

shopify_bp = Blueprint('shopify', __name__)
SECRET_KEY = Config.SECRET_KEY

SHOPIFY_API_KEY = os.getenv("SHOPIFY_API_KEY")
SHOPIFY_API_SECRET = os.getenv("SHOPIFY_API_SECRET")
REDIRECT_URI = os.getenv("REDIRECT_URI")
SCOPES = os.getenv("SCOPES")
FRONTEND_BASE_URL = os.environ.get("FRONTEND_BASE_URL")

# -------------------------------- OAuth --------------------------------

@shopify_bp.route('/shopify/install', methods=['GET'])
def install():
    auth_header = request.headers.get('Authorization')
    if auth_header and auth_header.startswith('Bearer '):
        token = auth_header.split(' ')[1]
    else:
        token = request.args.get('user_token')
        if not token:
            return jsonify({'error': 'Authorization token is missing or invalid'}), 401

    try:
        payload, user_id, member_id = get_effective_user_id_from_token(token)
    except jwt.ExpiredSignatureError:
        return jsonify({'error': 'Token has expired'}), 401
    except jwt.InvalidTokenError:
        return jsonify({'error': 'Invalid token'}), 401

    shop = request.args.get("shop")
    if not shop:
        return jsonify({"error": "Shop parameter missing (use ?shop=storename.myshopify.com)"}), 400

    oauth_url = (
        f"https://{shop}/admin/oauth/authorize"
        # f"?client_id={SHOPIFY_API_KEY}&scope={SCOPES}&redirect_uri={REDIRECT_URI}"
        f"?client_id={SHOPIFY_API_KEY}&scope={SCOPES}&redirect_uri={REDIRECT_URI}"
        f"&grant_options[]=per-user"
        f"&state={token}"

    )
    return redirect(oauth_url)


@shopify_bp.route('/shopify/callback', methods=['GET'])
def callback():
    shop = request.args.get("shop")
    code = request.args.get("code")
    user_token = request.args.get("state")  # <-- JWT comes here
    if not shop or not code:
        error_message = "Shopify authorization failed. Please log in with the correct account."
        return redirect(f"{FRONTEND_BASE_URL}/shopify-error?message={error_message}")

    # Decode user_id from JWT
    try:
        payload = jwt.decode(user_token, SECRET_KEY, algorithms=['HS256'])
        user_id = payload.get('user_id')

        if not user_id:
            print("ERROR: user_id missing in JWT!")
    except Exception as e:
        error_message = "Invalid or expired user token."
        return redirect(f"{FRONTEND_BASE_URL}/shopify-error?message={error_message}")

    token_url = f"https://{shop}/admin/oauth/access_token"
    payload = {"client_id": SHOPIFY_API_KEY, "client_secret": SHOPIFY_API_SECRET, "code": code}
    response = requests.post(token_url, json=payload)
    token_data = response.json()

    if 'access_token' not in token_data:
        error_message = "Unauthorized Access. Please log in using the Shopify store owner account."
        return redirect(f"{FRONTEND_BASE_URL}/shopify-error?message={error_message}")

    access_token = token_data['access_token']

    headers = {"X-Shopify-Access-Token": access_token}
    shop_info_res = requests.get(f"https://{shop}/admin/api/2024-07/shop.json", headers=headers)

    if shop_info_res.status_code != 200:
        error_message = "Unable to fetch shop details. Please try again."
        return redirect(f"{FRONTEND_BASE_URL}/shopify-error?message={error_message}")

    shop_info = shop_info_res.json()
    email = shop_info.get('shop', {}).get('email')

    try:
        existing_store = ShopifyStore.query.filter_by(shop_name=shop).first()
        if existing_store:
            existing_store.access_token = access_token
            existing_store.email = email
            existing_store.user_id = user_id
            existing_store.is_active = True
        else:
            db.session.add(ShopifyStore(
                user_id=user_id,
                shop_name=shop,
                access_token=access_token,
                email=email
            ))
        db.session.commit()
    except Exception as e:
        db.session.rollback()
        error_message = "Database error while saving store details."
        return redirect(f"{FRONTEND_BASE_URL}/shopify-error?message={error_message}")

    frontend_url = f"https://phormula.io/orders?shop={shop}&token={access_token}&email={email}"
    return redirect(frontend_url)

# -------------------------------- Helpers --------------------------------

def safe_float(v):
    return 0.0 if v is None or (isinstance(v, float) and math.isnan(v)) else float(v)

def month_name_to_num(name: str) -> int:
    return list(calendar.month_name).index(name.capitalize())

def month_bounds_utc(year: int, month: int):
    start = datetime(year, month, 1, tzinfo=timezone.utc)
    if month == 12:
        end = datetime(year + 1, 1, 1, tzinfo=timezone.utc)
    else:
        end = datetime(year, month + 1, 1, tzinfo=timezone.utc)
    return start, end

def quarter_bounds_utc(year: int, quarter: int):
    month_start = (quarter - 1) * 3 + 1
    start = datetime(year, month_start, 1, tzinfo=timezone.utc)
    end_month = month_start + 3
    if end_month == 13:
        end = datetime(year + 1, 1, 1, tzinfo=timezone.utc)
    else:
        end = datetime(year, end_month, 1, tzinfo=timezone.utc)
    return start, end

def to_shopify_dt(dt: datetime) -> str:
    return dt.replace(microsecond=0).isoformat().replace("+00:00", "Z")

def upload_summary_to_shopify_db(user_id, month, year, total_discounts, total_price, total_tax, total_orders, net_sales):
    try:
        engine = db.get_engine(bind='shopify')
        with engine.begin() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS upload_shopify (
                    id SERIAL PRIMARY KEY,
                    user_id INTEGER,
                    month TEXT,
                    year INTEGER,
                    total_discounts FLOAT,
                    total_price FLOAT,
                    total_tax FLOAT,
                    total_orders INTEGER,
                    net_sales FLOAT,
                    UNIQUE (user_id, month, year)
                )
            """))
            upsert = text("""
                INSERT INTO upload_shopify (user_id, month, year, total_discounts, total_price, total_tax, total_orders, net_sales)
                VALUES (:user_id, :month, :year, :total_discounts, :total_price, :total_tax, :total_orders, :net_sales)
                ON CONFLICT (user_id, month, year) DO UPDATE SET
                    total_discounts = EXCLUDED.total_discounts,
                    total_price     = EXCLUDED.total_price,
                    total_tax       = EXCLUDED.total_tax,
                    total_orders    = EXCLUDED.total_orders,
                    net_sales       = EXCLUDED.net_sales
            """)
            conn.execute(upsert, {
                "user_id": user_id,
                "month": month,
                "year": year,
                "total_discounts": total_discounts,
                "total_price": total_price,
                "total_tax": total_tax,
                "total_orders": total_orders,
                "net_sales": net_sales
            })
    except Exception as e:
        print(f"[ERROR] upload_summary_to_shopify_db failed: {e}")

@shopify_bp.route('/shopify/dropdown', methods=['GET'])
def get_dropdown_filters():
    # auth
    auth_header = request.headers.get('Authorization')
    if auth_header and auth_header.startswith('Bearer '):
        user_jwt = auth_header.split(' ')[1]
    else:
        user_jwt = request.args.get('user_token')
        if not user_jwt:
            return jsonify({'error': 'Authorization token is missing or invalid'}), 401

    try:
        payload = jwt.decode(user_jwt, SECRET_KEY, algorithms=['HS256'])
        user_id = payload['user_id']
    except jwt.ExpiredSignatureError:
        return jsonify({'error': 'Token has expired'}), 401
    except jwt.InvalidTokenError:
        return jsonify({'error': 'Invalid token'}), 401

    # params
    range_type = (request.args.get("range") or "").lower()        # monthly / quarterly / yearly
    month = request.args.get("month")                              # "january"
    quarter = request.args.get("quarter")                          # "1".."4"
    year = request.args.get("year")                                # "2025"
    # shop = request.args.get("shop")
    # token = request.args.get("token")

    # if not shop or not token:
    #     return jsonify({"error": "Shop and token are required"}), 400

    # ---- FETCH SHOPIFY STORE DETAILS FROM DATABASE ----

    store = ShopifyStore.query.filter_by(user_id=user_id).first()

    if not store:
        return jsonify({"error": "No Shopify store connected for this user"}), 404

    shop = store.shop_name
    token = store.access_token

    if not shop or not token:
        return jsonify({"error": "Shop or access token missing in DB"}), 500

    if not year or not year.isdigit():
        return jsonify({"error": "Valid year is required"}), 400

    year_i = int(year)

    # ---- compute time window for Shopify request
    if range_type == "monthly":
        if not month:
            return jsonify({"error": "Month is required for monthly range"}), 400
        start, end = month_bounds_utc(year_i, month_name_to_num(month))
        table_name = f"shopify_{month.lower()}_{year_i}_table"

    elif range_type == "quarterly":
        if not quarter or not quarter.isdigit() or int(quarter) not in (1,2,3,4):
            return jsonify({"error": "Quarter must be 1..4 for quarterly range"}), 400
        q = int(quarter)
        start, end = quarter_bounds_utc(year_i, q)
        table_name = f"shopify_quarter{q}_{year_i}_table"

    elif range_type == "yearly":
        start = datetime(year_i, 1, 1, tzinfo=timezone.utc)
        end   = datetime(year_i + 1, 1, 1, tzinfo=timezone.utc)
        table_name = f"shopify_{year_i}_table"
    else:
        return jsonify({"error": "Invalid range"}), 400

    created_at_min = to_shopify_dt(start)
    created_at_max = to_shopify_dt(end)

    # ---- FIXED: Properly fetch all orders with pagination ----
    all_orders = []
    page_info = None
    
    while True:
        # Build URL with proper pagination
        url = (
            f"https://{shop}/admin/api/2024-07/orders.json"
            f"?limit=250&status=any"
            f"&created_at_min={created_at_min}&created_at_max={created_at_max}"
            f"&fields=id,name,email,financial_status,created_at,"
            f"fulfillment_status,subtotal_price,total_price,total_discounts,"
            f"total_line_items_price,total_tax,total_price_usd,currency,"
            f"shipping_lines,line_items,discount_codes,billing_address,shipping_address"
        )
        
        # Add pagination parameter if available
        if page_info:
            url += f"&page_info={page_info}"
            
        headers = {"X-Shopify-Access-Token": token}
        
        try:
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()  # Raise exception for bad status codes
            
            data = response.json()
            orders = data.get("orders", [])
            
            if not orders:
                break  # No more orders to fetch
                
            all_orders.extend(orders)
            
            # Check for pagination using page_info (GraphQL style pagination)
            link_header = response.headers.get("Link")
            page_info = None
            
            if link_header:
                # Parse Link header for next page
                links = {}
                for link in link_header.split(','):
                    parts = link.strip().split(';')
                    if len(parts) == 2:
                        url_part = parts[0].strip('<>')
                        rel_part = parts[1].strip()
                        if 'rel="next"' in rel_part:
                            # Extract page_info from the URL
                            if 'page_info=' in url_part:
                                page_info = url_part.split('page_info=')[1].split('&')[0]
                
            if not page_info:
                break  # No more pages
                
        except requests.exceptions.RequestException as e:
            return jsonify({"error": "Failed to fetch orders from Shopify"}), 500

    # ------ FIXED: Handle empty results properly ------
    if not all_orders:
        # Always ensure data exists for requested time periods
        months_to_populate = []
        
        if range_type == "monthly":
            months_to_populate = [month_name_to_num(month)]
        elif range_type == "quarterly":
            q = int(quarter)
            months_to_populate = [(q - 1) * 3 + i for i in range(1, 4)]
        elif range_type == "yearly":
            months_to_populate = list(range(1, 13))
            
        for m in months_to_populate:
            upload_summary_to_shopify_db(
                user_id=user_id,
                month=calendar.month_name[m],
                year=year_i,
                total_discounts=0.0,
                total_price=0.0,
                total_tax=0.0,
                total_orders=0,
                net_sales=0.0,
            )

        return jsonify({
            "message": "No data for selected range",
            "table_name": table_name,
            "orders": 0,
            "total_sales": 0.0,
            "net_sales": 0.0,
            "last_row_data": {
                "currency": "N/A",
                "total_discounts": 0.0,
                "total_tax": 0.0,
                "net_sales": 0.0,
                "total_price": 0.0,
                "total_orders": 0 
            }
        }), 200

    # ------ FIXED: Proper data processing and aggregation ------
    df = pd.json_normalize(all_orders)
    
    # Ensure datetime conversion
    df['created_at'] = pd.to_datetime(df['created_at'], utc=True)
    
    # Convert numeric columns properly
    numeric_columns = ['total_price', 'total_discounts', 'total_tax']
    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

    # Calculate totals for the entire period
    total_sales = df['total_price'].sum()
    total_discounts = df['total_discounts'].sum()
    total_tax = df['total_tax'].sum() if 'total_tax' in df.columns else 0.0
    total_orders = len(df)
    net_sales = total_sales - total_discounts

    # ------ FIXED: Proper monthly breakdown for all scenarios ------
    def process_monthly_data(year_i, months_to_process, df):
        """Process and upload monthly data for given months"""
        for m in months_to_process:
            m_start, m_end = month_bounds_utc(year_i, m)
            
            # Filter orders for this specific month
            month_mask = (
                (df['created_at'] >= m_start) & 
                (df['created_at'] < m_end)
            )
            m_df = df[month_mask]
            
            if not m_df.empty:
                m_sales = m_df['total_price'].sum()
                m_disc = m_df['total_discounts'].sum()
                m_tax = m_df['total_tax'].sum() if 'total_tax' in m_df.columns else 0.0
                m_orders = len(m_df)
                m_net_sales = m_sales - m_disc
            else:
                # No orders for this month
                m_sales = m_disc = m_tax = m_orders = m_net_sales = 0.0
                
            upload_summary_to_shopify_db(
                user_id=user_id,
                month=calendar.month_name[m],
                year=year_i,
                total_discounts=safe_float(m_disc),
                total_price=safe_float(m_sales),
                total_tax=safe_float(m_tax),
                total_orders=int(m_orders),
                net_sales=safe_float(m_net_sales)
            )

    # Process data based on range type
    if range_type == "monthly":
        process_monthly_data(year_i, [month_name_to_num(month)], df)
    elif range_type == "quarterly":
        q = int(quarter)
        months_in_quarter = [(q - 1) * 3 + i for i in range(1, 4)]
        process_monthly_data(year_i, months_in_quarter, df)
    elif range_type == "yearly":
        process_monthly_data(year_i, list(range(1, 13)), df)

    # ------ Prepare response ------
    currency = df.iloc[0]['currency'] if 'currency' in df.columns and len(df) > 0 else 'N/A'
    
    response_data = {
        "currency": currency,
        "total_price": round(float(total_sales), 2),
        "total_discounts": round(float(total_discounts), 2),
        "total_tax": round(float(total_tax), 2),
        "net_sales": round(float(net_sales), 2),
        "total_orders": int(total_orders),
    }

    return jsonify({
        "message": f"Data saved in {table_name}",
        "table_name": table_name,
        "orders": total_orders,
        "total_sales": round(total_sales, 2),
        "net_sales": round(net_sales, 2),
        "last_row_data": response_data
    }), 200


# ------ ADDITIONAL HELPER: Enhanced data retrieval ------

@shopify_bp.route('/shopify/get_monthly_data', methods=['GET'])
def get_monthly_data():
    """Get specific monthly data with better filtering"""
    auth_header = request.headers.get('Authorization')
    if auth_header and auth_header.startswith('Bearer '):
        user_jwt = auth_header.split(' ')[1]
    else:
        user_jwt = request.args.get('user_token')
        if not user_jwt:
            return jsonify({'error': 'Authorization token is missing'}), 401

    try:
        payload = jwt.decode(user_jwt, SECRET_KEY, algorithms=['HS256'])
        user_id = payload['user_id']
    except (jwt.ExpiredSignatureError, jwt.InvalidTokenError):
        return jsonify({'error': 'Invalid token'}), 401

    year = request.args.get('year')
    months = request.args.getlist('months[]')  # Allow multiple months
    
    if not year or not year.isdigit():
        return jsonify({'error': 'Valid year required'}), 400
    
    year = int(year)

    try:
        engine = db.get_engine(bind='shopify')
        with engine.connect() as conn:
            if months:
                # Filter by specific months
                query = """
                    SELECT * FROM upload_shopify 
                    WHERE user_id = :user_id AND year = :year AND month = ANY(:months)
                    ORDER BY 
                        CASE month
                            WHEN 'January' THEN 1 WHEN 'February' THEN 2 WHEN 'March' THEN 3
                            WHEN 'April' THEN 4 WHEN 'May' THEN 5 WHEN 'June' THEN 6
                            WHEN 'July' THEN 7 WHEN 'August' THEN 8 WHEN 'September' THEN 9
                            WHEN 'October' THEN 10 WHEN 'November' THEN 11 WHEN 'December' THEN 12
                        END
                """
                result = conn.execute(text(query), {"user_id": user_id, "year": year, "months": months})
            else:
                # Get all months for the year
                query = """
                    SELECT * FROM upload_shopify 
                    WHERE user_id = :user_id AND year = :year
                    ORDER BY 
                        CASE month
                            WHEN 'January' THEN 1 WHEN 'February' THEN 2 WHEN 'March' THEN 3
                            WHEN 'April' THEN 4 WHEN 'May' THEN 5 WHEN 'June' THEN 6
                            WHEN 'July' THEN 7 WHEN 'August' THEN 8 WHEN 'September' THEN 9
                            WHEN 'October' THEN 10 WHEN 'November' THEN 11 WHEN 'December' THEN 12
                        END
                """
                result = conn.execute(text(query), {"user_id": user_id, "year": year})
            
            columns = result.keys()
            rows = [dict(zip(columns, row)) for row in result.fetchall()]
            
        return jsonify({"data": rows}), 200
        
    except Exception as e:
        return jsonify({"error": "Database error"}), 500


# ------------------------- 365d raw fetch (unchanged) -------------------------

@shopify_bp.route('/shopify/get_orders', methods=['GET'])
def get_orders():
    shop = request.args.get("shop")
    token = request.args.get("token")
    if not shop or not token:
        return jsonify({"error": "Missing shop or token"}), 400

    end_date = datetime.now(timezone.utc)
    start_date = end_date - timedelta(days=365)
    created_at_min = to_shopify_dt(start_date)
    created_at_max = to_shopify_dt(end_date)

    url = (
        f"https://{shop}/admin/api/2024-07/orders.json"
        f"?limit=250&status=any"
        f"&created_at_min={created_at_min}&created_at_max={created_at_max}"
        f"&fields=id,name,email,financial_status,created_at,fulfillment_status,subtotal_price,total_price,total_discounts,"
        f"total_line_items_price,total_tax,total_price_usd,currency,shipping_lines,line_items,discount_codes"
    )
    headers = {"X-Shopify-Access-Token": token}

    all_orders, total_sales, total_discounts, total_tax, total_shipping = [], 0.0, 0.0, 0.0, 0.0
    while url:
        resp = requests.get(url, headers=headers)
        data = resp.json()
        orders = data.get("orders", [])
        for order in orders:
            total_sales += float(order.get("total_price", 0) or 0)
            total_discounts += float(order.get("total_discounts", 0) or 0)
            total_tax += float(order.get("total_tax", 0) or 0)
            for ship in order.get("shipping_lines", []):
                total_shipping += float(ship.get("price", 0) or 0)
        all_orders.extend(orders)

        link_header = resp.headers.get("Link")
        next_url = None
        if link_header:
            for part in link_header.split(","):
                if 'rel="next"' in part:
                    next_url = part.split(";")[0].strip("<>")
                    break
        url = next_url

    summary = {
        "total_sales": round(total_sales, 2),
        "total_discounts": round(total_discounts, 2),
        "total_tax": round(total_tax, 2),
        "total_shipping": round(total_shipping, 2),
        "net_sales": round(total_sales - total_discounts, 2),
        "currency": all_orders[0]["currency"] if all_orders else "N/A",
        "total_orders": len(all_orders),
    }
    return jsonify({"summary": summary, "orders": all_orders})

# --------------------------- History for charts ---------------------------

@shopify_bp.route('/upload_historyofshopify', methods=['GET'])
def get_shopify_upload_history():
    # auth
    auth_header = request.headers.get('Authorization')
    if auth_header and auth_header.startswith('Bearer '):
        token = auth_header.split(' ')[1]
    else:
        token = request.args.get('user_token')
        if not token:
            return jsonify({'error': 'Authorization token is missing or invalid'}), 401

    try:
        payload, user_id, member_id = get_effective_user_id_from_token(token)
    except jwt.ExpiredSignatureError:
        return jsonify({'error': 'Token has expired'}), 401
    except jwt.InvalidTokenError:
        return jsonify({'error': 'Invalid token'}), 401

    year = request.args.get('year')
    quarter = request.args.get('quarter')  # "Q1".."Q4"
    if not year or not str(year).isdigit():
        return jsonify({'error': 'Year is required and must be a number'}), 400
    year = int(year)

    try:
        engine = db.get_engine(bind='shopify')
        with engine.connect() as conn:
            base_q = """
                SELECT month, year, total_discounts, total_price, total_tax, total_orders, net_sales
                FROM upload_shopify
                WHERE user_id = :user_id AND year = :year
            """
            params = {"user_id": user_id, "year": year}

            if quarter:
                quarters = {
                    "Q1": ["January", "February", "March"],
                    "Q2": ["April", "May", "June"],
                    "Q3": ["July", "August", "September"],
                    "Q4": ["October", "November", "December"]
                }
                months = quarters.get(quarter)
                if not months:
                    return jsonify({"error": "Invalid quarter"}), 400
                base_q += " AND month = ANY(:months)"
                params["months"] = months

            result = conn.execute(text(base_q), params)
            cols = result.keys()
            rows = [dict(zip(cols, r)) for r in result]

        return jsonify({"data": rows}), 200
    except Exception as e:
        return jsonify({"error": "Internal server error"}), 500




@shopify_bp.route('/shopify/store', methods=['GET'])
def get_shopify_store():
    # 1. Get JWT token
    auth_header = request.headers.get('Authorization')
    if auth_header and auth_header.startswith('Bearer '):
        token = auth_header.split(' ')[1]
    else:
        token = request.args.get('user_token')
        if not token:
            return jsonify({'error': 'Authorization token missing'}), 401

    # 2. Decode JWT → get user_id
    try:
        payload, user_id, member_id = get_effective_user_id_from_token(token)
    except Exception:
        return jsonify({'error': 'Invalid or expired token'}), 401

    # 3. Fetch Shopify store
    store = ShopifyStore.query.filter_by(user_id=user_id).first()

    if not store:
        return jsonify({"error": "No Shopify store found"}), 404

    # 4. Return needed fields (shop_name + access_token)
    return jsonify({
        "shop_name": store.shop_name,
        "access_token": store.access_token,
        "email": store.email,
        "is_active": store.is_active,
        "installed_at": store.installed_at.isoformat() if store.installed_at else None
    }), 200
