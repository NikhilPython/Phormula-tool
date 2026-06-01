from flask import Blueprint, request, jsonify , send_file 
import jwt
import os
import re
import traceback
from sqlalchemy import create_engine, text
from config import Config
from config import basedir
SECRET_KEY = Config.SECRET_KEY
from dotenv import load_dotenv
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from werkzeug.utils import secure_filename
from sqlalchemy import MetaData, Table, inspect, select
import logging
from app.routes.amazon_sales_api_routes import _normalize_sku_row
from app.utils.token_utils import get_effective_user_id_from_token
from sqlalchemy import text
import pandas as pd
from decimal import Decimal



# Setup logger
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

load_dotenv()
db_url = os.getenv('DATABASE_URL')
db_url1 = os.getenv('DATABASE_ADMIN_URL')

user_engine = create_engine(
    db_url,
    pool_pre_ping=True,
    pool_size=5,
    max_overflow=10,
    pool_recycle=1800,
)

admin_engine = create_engine(
    db_url1,
    pool_pre_ping=True,
    pool_size=5,
    max_overflow=10,
    pool_recycle=1800,
)


product_bp = Blueprint('product_bp', __name__)

MONTHS = [
    "january", "february", "march", "april", "may", "june",
    "july", "august", "september", "october", "november", "december"
]

def get_previous_month(month, year):
    month = str(month).strip().lower()
    year = int(year)

    if month not in MONTHS:
        return None, None

    idx = MONTHS.index(month)

    if idx == 0:
        return "december", str(year - 1)

    return MONTHS[idx - 1], str(year)


def get_previous_quarter(quarter, year):
    q = str(quarter).strip().lower()
    year = int(year)

    if q in ("q1", "quarter1", "1"):
        return "quarter4", str(year - 1)
    if q in ("q2", "quarter2", "2"):
        return "quarter1", str(year)
    if q in ("q3", "quarter3", "3"):
        return "quarter2", str(year)
    if q in ("q4", "quarter4", "4"):
        return "quarter3", str(year)

    return None, None


def get_previous_year(year):
    return str(int(year) - 1)

@product_bp.route('/getConversionRate', methods=['GET'])
def get_conversion_rate():
    try:
        # Step 1: Get query params
        home_currency = (request.args.get('homecurrency') or '').strip()
        month = (request.args.get('month') or '').strip()
        year = (request.args.get('year') or '').strip()

        # Step 2: Validate
        if not home_currency or not month or not year:
            return jsonify({"error": "homecurrency, month, and year are required"}), 400

        # Step 3: Query admin_db.currency_conversion (case-insensitive)
        with admin_engine.connect() as conn:
            query = text("""
                SELECT conversion_rate
                FROM currency_conversion
                WHERE lower(selected_currency) = 'usd'
                  AND lower(user_currency) = :home_currency
                  AND lower(month) = :month
                  AND year = :year
                ORDER BY id DESC
                LIMIT 1
            """)
            row = conn.execute(query, {
                "home_currency": home_currency.lower(),
                "month": month.lower(),
                "year": int(year)
            }).fetchone()

        # Step 4: Not found
        if not row:
            return jsonify({"error": "Conversion rate not found"}), 404

        conversion_rate = float(row.conversion_rate)

        # Step 6: Send to frontend
        return jsonify({
            "from_currency": "USD",
            "homecurrency": home_currency.upper(),
            "month": month,
            "year": year,
            "conversion_rate": conversion_rate
        }), 200

    except Exception as e:
        return jsonify({"error": "Internal server error"}), 500


def resolve_country(country, currency):
    country = (country or "").lower()
    currency = (currency or "").lower()

    # 1. If country = global
    if country == "global":
        if currency == "usd":
            return "global"
        elif currency == "inr":
            return "global_inr"
        elif currency == "gbp":
            return "global_gbp"
        elif currency == "cad":
            return "global_cad"
        else:
            return "global"  # default fallback

    # 2. If country = uk
    if country == "uk":
        if currency == "usd":
            return "uk_usd"
        else:
            return "uk"  # default for all other currencies

    # 3. Default (no special logic)
    return country


def get_month_tokens_present_for_year(conn, user_id, country, year):
    """
    Finds monthly table month tokens for current selected year.

    Normal country example:
    skuwisemonthly_1_uk_may2026

    Global example:
    skuwisemonthly_1_global_may2026_table

    Returns:
    ["jan", "feb", "mar", ...]
    """

    prefix = f"skuwisemonthly_{user_id}_{country}_"

    valid_months = {
        "jan": 1,
        "january": 1,
        "feb": 2,
        "february": 2,
        "mar": 3,
        "march": 3,
        "apr": 4,
        "april": 4,
        "may": 5,
        "jun": 6,
        "june": 6,
        "jul": 7,
        "july": 7,
        "aug": 8,
        "august": 8,
        "sep": 9,
        "sept": 9,
        "september": 9,
        "oct": 10,
        "october": 10,
        "nov": 11,
        "november": 11,
        "dec": 12,
        "december": 12,
    }

    query = text("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_name LIKE :pattern
        ORDER BY table_name
    """)

    rows = conn.execute(query, {
        "pattern": f"{prefix}%"
    }).mappings().all()

    month_tokens = []

    for row in rows:
        table_name = row["table_name"]

        if not table_name.startswith(prefix):
            continue

        middle = table_name[len(prefix):]

        # For global:
        # may2026_table -> may2026
        if middle.endswith("_table"):
            middle = middle[:-len("_table")]

        # Now both formats become:
        # may2026
        if not middle.endswith(str(year)):
            continue

        month_token = middle[:-len(str(year))].lower()

        if month_token in valid_months:
            month_tokens.append(month_token)

    return sorted(set(month_tokens), key=lambda month: valid_months[month])




def aggregate_monthly_sku_rows(rows):
    grouped = {}

    preferred_key_columns = [
        "sku",
        "asin",
        "msku",
        "fnsku",
        "product_name",
        "product",
        "title"
    ]


    for row in rows:
        normalized_row = _normalize_sku_row(dict(row))

        available_key_columns = [
            col for col in preferred_key_columns
            if col in normalized_row
        ]

        if available_key_columns:
            key = tuple(
                (col, normalized_row.get(col))
                for col in available_key_columns
            )
        else:
            key = tuple(
                (col, normalized_row.get(col))
                for col, value in normalized_row.items()
                if not isinstance(value, (int, float, Decimal)) or isinstance(value, bool)
            )

        if key not in grouped:
            grouped[key] = dict(normalized_row)
        else:
            for col, value in normalized_row.items():
                if isinstance(value, (int, float, Decimal)) and not isinstance(value, bool):
                    grouped[key][col] = (grouped[key].get(col) or 0) + (value or 0)


    return list(grouped.values())


def get_previous_year_monthly_aggregated_data(conn, engine, metadata, user_id, country, year):
    previous_year = get_previous_year(year)

    current_year_month_tokens = get_month_tokens_present_for_year(
        conn=conn,
        user_id=user_id,
        country=country,
        year=year
    )

    all_previous_month_rows = []
    used_previous_tables = []

    for month_token in current_year_month_tokens:

        if country == "global":
            previous_monthly_table_name = (
                f"skuwisemonthly_{user_id}_{country}_{month_token}{previous_year}_table"
            )
        else:
            previous_monthly_table_name = (
                f"skuwisemonthly_{user_id}_{country}_{month_token}{previous_year}"
            )

        try:
            previous_monthly_table = Table(
                previous_monthly_table_name,
                metadata,
                autoload_with=engine
            )

            monthly_results = conn.execute(
                select(*previous_monthly_table.columns)
            ).mappings().all()

            all_previous_month_rows.extend(monthly_results)
            used_previous_tables.append(previous_monthly_table_name)

        except Exception as e:
            print("Missing previous monthly table:", previous_monthly_table_name)
            print("Error:", str(e))
            continue

    previous_data = aggregate_monthly_sku_rows(all_previous_month_rows)

    return previous_data, used_previous_tables


@product_bp.route('/YearlySKU', methods=['GET'])
def YearlySKU():
    country = (request.args.get('country') or '').lower()
    country_param = request.args.get('country', '').lower()
    currency_param = (request.args.get('homeCurrency') or '').lower()

    country = resolve_country(country_param, currency_param)

    year = (request.args.get('year') or '').strip()

    # Validate the query parameters
    if not country or not year:
        return jsonify({'error': 'Country and year are required'}), 400

    auth_header = request.headers.get('Authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        return jsonify({'error': 'Authorization token is missing or invalid'}), 401

    token = auth_header.split(' ')[1]
    try:
        payload, user_id, member_id = get_effective_user_id_from_token(token)
    except jwt.ExpiredSignatureError:
        return jsonify({'error': 'Token has expired'}), 401
    except jwt.InvalidTokenError:
        return jsonify({'error': 'Invalid token'}), 401

    try:
        engine = user_engine
        metadata = MetaData(schema='public')  # align with other routes
        table_name = f"skuwiseyearly_{user_id}_{country}_{year}_table"

        try:
            user_specific_table = Table(table_name, metadata, autoload_with=engine)
        except Exception:
            return jsonify({'error': f"Table '{table_name}' not found for user {user_id}"}), 404

        with engine.connect() as conn:
            results = conn.execute(select(*user_specific_table.columns)).mappings().all()

        # 🔒 Normalize all rows so the UI gets true numbers, not strings
        current_data = [_normalize_sku_row(dict(row)) for row in results]

        previous_year = get_previous_year(year)
        previous_table_name = f"skuwisemonthly_{user_id}_{country}_aggregated_till_current_months_{previous_year}"
        previous_data = []

        try:
            with engine.connect() as conn:
                previous_data, used_previous_tables = get_previous_year_monthly_aggregated_data(
                    conn=conn,
                    engine=engine,
                    metadata=metadata,
                    user_id=user_id,
                    country=country,
                    year=year
                )

        except Exception:
            previous_data = []
            used_previous_tables = []

        return jsonify({
            "current_table_name": table_name,
            "current_data": current_data,
            "previous_table_name": previous_table_name,
            "previous_data": previous_data
        }), 200

    except SQLAlchemyError as e:
        return jsonify({'error': 'Error accessing the database'}), 500
    except Exception as e:
        return jsonify({'error': 'An error occurred while fetching table data'}), 500




# @product_bp.route('/YearlySKU', methods=['GET'])
# def YearlySKU():
#     country = (request.args.get('country') or '').lower()
#     country_param = request.args.get('country', '').lower()
#     currency_param = (request.args.get('homeCurrency') or '').lower()

#     country = resolve_country(country_param, currency_param)

#     year = (request.args.get('year') or '').strip()

#     # Validate the query parameters
#     if not country or not year:
#         return jsonify({'error': 'Country and year are required'}), 400

#     auth_header = request.headers.get('Authorization')
#     if not auth_header or not auth_header.startswith('Bearer '):
#         return jsonify({'error': 'Authorization token is missing or invalid'}), 401

#     token = auth_header.split(' ')[1]
#     try:
#         payload, user_id, member_id = get_effective_user_id_from_token(token)
#     except jwt.ExpiredSignatureError:
#         return jsonify({'error': 'Token has expired'}), 401
#     except jwt.InvalidTokenError:
#         return jsonify({'error': 'Invalid token'}), 401

#     try:
#         engine = user_engine
#         metadata = MetaData(schema='public')  # align with other routes
#         table_name = f"skuwiseyearly_{user_id}_{country}_{year}_table"

#         try:
#             user_specific_table = Table(table_name, metadata, autoload_with=engine)
#         except Exception:
#             return jsonify({'error': f"Table '{table_name}' not found for user {user_id}"}), 404

#         with engine.connect() as conn:
#             results = conn.execute(select(*user_specific_table.columns)).mappings().all()

#         # 🔒 Normalize all rows so the UI gets true numbers, not strings
#         current_data = [_normalize_sku_row(dict(row)) for row in results]

#         previous_year = get_previous_year(year)
#         previous_table_name = f"skuwiseyearly_{user_id}_{country}_{previous_year}_table"
#         previous_data = []

#         try:
#             previous_table = Table(previous_table_name, metadata, autoload_with=engine)

#             with engine.connect() as conn:
#                 prev_results = conn.execute(select(*previous_table.columns)).mappings().all()

#             previous_data = [_normalize_sku_row(dict(row)) for row in prev_results]
#         except Exception:
#             previous_data = []

#         return jsonify({
#             "current_table_name": table_name,
#             "current_data": current_data,
#             "previous_table_name": previous_table_name,
#             "previous_data": previous_data
#         }), 200

#     except SQLAlchemyError as e:
#         return jsonify({'error': 'Error accessing the database'}), 500
#     except Exception as e:
#         return jsonify({'error': 'An error occurred while fetching table data'}), 500

def resolve_country(country, currency):
    country = (country or "").lower()
    currency = (currency or "").lower()   # '' if missing

    # 1) Global: default to USD only here
    if country == "global":
        if currency in ("", "usd"):
            return "global"
        elif currency == "inr":
            return "global_inr"
        elif currency == "gbp":
            return "global_gbp"
        elif currency == "cad":
            return "global_cad"
        else:
            return "global"

    # 2) UK: only go to uk_usd if explicitly requested
    if country == "uk":
        if currency == "usd":
            return "uk_usd"
        return "uk"

    return country

@product_bp.route('/quarterlyskutable', methods=['GET'])
def quarterlyskutable():
    quarter = request.args.get('quarter')
    country_param = request.args.get('country', '')
    currency_param = (request.args.get('homeCurrency') or '').lower()

    country = resolve_country(country_param, currency_param)
    year = (request.args.get('year') or '').strip()

    if not quarter or not country or not year:
        return jsonify({'error': 'Quarter, country, and year are required'}), 400

    auth_header = request.headers.get('Authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        return jsonify({'error': 'Authorization token is missing or invalid'}), 401

    token = auth_header.split(' ')[1]
    try:
        payload, user_id, member_id = get_effective_user_id_from_token(token)
    except jwt.ExpiredSignatureError:
        return jsonify({'error': 'Token has expired'}), 401
    except jwt.InvalidTokenError:
        return jsonify({'error': 'Invalid token'}), 401

    def normalize_quarter(q):
        q = str(q).strip().lower()

        if q in ("q1", "quarter1", "1"):
            return "quarter1"
        if q in ("q2", "quarter2", "2"):
            return "quarter2"
        if q in ("q3", "quarter3", "3"):
            return "quarter3"
        if q in ("q4", "quarter4", "4"):
            return "quarter4"

        return None

    try:
        engine = user_engine
        metadata = MetaData(schema='public')

        current_quarter = normalize_quarter(quarter)

        if not current_quarter:
            return jsonify({'error': 'Invalid quarter value'}), 400

        table_name = f"{current_quarter}_{user_id}_{country}_{year}_table".lower()

        try:
            user_specific_table = Table(table_name, metadata, autoload_with=engine)

            with engine.connect() as conn:
                results = conn.execute(
                    select(*user_specific_table.columns)
                ).mappings().all()

            current_data = [_normalize_sku_row(dict(row)) for row in results]

        except Exception:
            return jsonify({
                'error': f"Table '{table_name}' not found for user {user_id}"
            }), 404

        previous_table_name = None
        previous_data = []

        prev_quarter, prev_year = get_previous_quarter(current_quarter, year)

        if prev_quarter and prev_year:
            previous_table_name = f"{prev_quarter}_{user_id}_{country}_{prev_year}_table".lower()

            try:
                previous_table = Table(previous_table_name, metadata, autoload_with=engine)

                with engine.connect() as conn:
                    prev_results = conn.execute(
                        select(*previous_table.columns)
                    ).mappings().all()

                previous_data = [_normalize_sku_row(dict(row)) for row in prev_results]

            except Exception:
                previous_data = []

        return jsonify({
            "current_table_name": table_name,
            "current_data": current_data,
            "previous_table_name": previous_table_name,
            "previous_data": previous_data
        }), 200

    except Exception as e:
        return jsonify({
            'error': 'An unexpected error occurred',
            'message': str(e)
        }), 500
    
    

@product_bp.route('/currency-rates', methods=['GET'])
def get_currency_rates():
    auth_header = request.headers.get('Authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        return jsonify({'error': 'Authorization token is missing or invalid'}), 401

    token = auth_header.split(' ')[1]
    try:
        payload, user_id, member_id = get_effective_user_id_from_token(token)
    except jwt.ExpiredSignatureError:
        return jsonify({'error': 'Token has expired'}), 401
    except jwt.InvalidTokenError:
        return jsonify({'error': 'Invalid token'}), 401

    try:
        with admin_engine.connect() as conn:
            query = text("""
                SELECT DISTINCT ON (user_currency, country)
                    user_currency, country, selected_currency, conversion_rate, month, year
                FROM currency_conversion
                ORDER BY user_currency, country, year DESC,
                    CASE month
                        WHEN 'january' THEN 1 WHEN 'february' THEN 2 WHEN 'march' THEN 3
                        WHEN 'april' THEN 4 WHEN 'may' THEN 5 WHEN 'june' THEN 6
                        WHEN 'july' THEN 7 WHEN 'august' THEN 8 WHEN 'september' THEN 9
                        WHEN 'october' THEN 10 WHEN 'november' THEN 11 WHEN 'december' THEN 12
                    END DESC
            """)
            results = conn.execute(query).mappings().all()

        currency_rates = []
        for row in results:
            d = dict(row)
            # normalize for frontend matching
            d["user_currency"] = str(d.get("user_currency", "")).strip().lower()
            d["country"] = str(d.get("country", "")).strip().lower()
            d["selected_currency"] = str(d.get("selected_currency", "")).strip().lower()
            currency_rates.append(d)

        return jsonify(currency_rates), 200

    except SQLAlchemyError as e:
        return jsonify({'error': 'Database error', 'message': str(e)}), 500
    except Exception as e:
        return jsonify({'error': 'An error occurred while fetching currency rates', 'message': str(e)}), 500



MONTHS = [
    "january", "february", "march", "april", "may", "june",
    "july", "august", "september", "october", "november", "december"
]

def get_previous_month(month: str, year: str):
    m = month.strip().lower()
    y = int(year)

    if m not in MONTHS:
        return None, None

    idx = MONTHS.index(m)
    if idx == 0:
        return "december", str(y - 1)
    return MONTHS[idx - 1], str(y)

def is_valid_product_name(name):
    if name is None:
        return False
    s = str(name).strip().lower()
    return s not in ("", "nan", "none", "null", "total")

def build_table_candidates(user_id, country, month, year):
    """Return [requested_table, fallback_prev_month_table] (fallback may be None)."""
    requested = f"skuwisemonthly_{user_id}_{country}_{month}{year}"
    pm, py = get_previous_month(month, year)
    fallback = f"skuwisemonthly_{user_id}_{country}_{pm}{py}" if pm and py else None
    return requested, fallback

def select_asp_query(asp_table):
    """Return a SQLAlchemy select query based on available ASP-like columns."""
    if hasattr(asp_table.c, 'asp'):
        return select(asp_table.c.product_name, asp_table.c.asp)
    if hasattr(asp_table.c, 'net_credits'):
        return select(asp_table.c.product_name, asp_table.c.net_credits.label('asp'))
    if hasattr(asp_table.c, 'average_selling_price'):
        return select(asp_table.c.product_name, asp_table.c.average_selling_price.label('asp'))
    return None

@product_bp.route('/asp-data', methods=['GET'])
def get_asp_data():
    # ---------- AUTH ----------
    auth_header = request.headers.get('Authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        return jsonify({'error': 'Authorization token is missing or invalid'}), 401

    token = auth_header.split(' ')[1]
    try:
        payload, user_id, member_id = get_effective_user_id_from_token(token)
    except jwt.ExpiredSignatureError:
        return jsonify({'error': 'Token has expired'}), 401
    except jwt.InvalidTokenError:
        return jsonify({'error': 'Invalid token'}), 401

    # ---------- PARAMS ----------
    country = request.args.get('country', '').strip().lower()
    month = request.args.get('month', '').strip().lower()
    year = request.args.get('year', '').strip()

    if not all([country, month, year]):
        return jsonify({'error': 'Country, month, and year parameters are required'}), 400

    if month not in MONTHS:
        return jsonify({'error': 'Invalid month', 'allowed': MONTHS}), 400

    try:
        engine = user_engine
        inspector = inspect(engine)
        all_tables = set(inspector.get_table_names())

        # ---------- GLOBAL ----------
        if country == 'global':
            asp_data = []
            countries_to_try = ['uk', 'us', 'canada']

            for c in countries_to_try:
                requested, fallback = build_table_candidates(user_id, c, month, year)

                table_to_use = None
                if requested in all_tables:
                    table_to_use = requested
                elif fallback and fallback in all_tables:
                    table_to_use = fallback
                else:
                    continue

                metadata = MetaData()
                asp_table = Table(table_to_use, metadata, autoload_with=engine)

                query = select_asp_query(asp_table)
                if query is None:
                    continue

                with engine.connect() as conn:
                    results = conn.execute(query).mappings().all()

                for row in results:
                    row_dict = dict(row)
                    if not is_valid_product_name(row_dict.get("product_name")):
                        continue
                    row_dict['source_country'] = c
                    asp_data.append(row_dict)

            if not asp_data:
                return jsonify({
                    'error': 'No ASP data found for global view',
                    'details': f'No data available for {month} {year}'
                }), 404

            return jsonify(asp_data), 200

        # ---------- SINGLE COUNTRY ----------
        requested, fallback = build_table_candidates(user_id, country, month, year)

        if requested in all_tables:
            table_to_use = requested
        elif fallback and fallback in all_tables:
            table_to_use = fallback
        else:
            return jsonify({
                'error': f'ASP data table "{requested}" not found',
                'details': f'Also checked fallback "{fallback}"'
            }), 404

        metadata = MetaData()
        asp_table = Table(table_to_use, metadata, autoload_with=engine)

        query = select_asp_query(asp_table)
        if query is None:
            return jsonify({
                'error': 'Cannot determine ASP column',
                'available_columns': [c.name for c in asp_table.columns],
                'table_name': table_to_use
            }), 404

        with engine.connect() as conn:
            results = conn.execute(query).mappings().all()

        asp_data = []
        for r in results:
            d = dict(r)
            if not is_valid_product_name(d.get("product_name")):
                continue
            asp_data.append(d)

        return jsonify(asp_data), 200

    except SQLAlchemyError as e:
        return jsonify({'error': 'Database error', 'message': str(e)}), 500
    except Exception as e:
        return jsonify({'error': 'Unexpected error', 'message': str(e)}), 500

    

@product_bp.route('/skup', methods=['POST'])
def skup():
    auth_header = request.headers.get('Authorization')

    if not auth_header or not auth_header.startswith('Bearer '):
        return jsonify({'error': 'Authorization token is missing or invalid'}), 401

    token = auth_header.split(' ')[1]
    try:
        payload, user_id, member_id = get_effective_user_id_from_token(token)
    except jwt.ExpiredSignatureError:
        return jsonify({'error': 'Token has expired'}), 401
    except jwt.InvalidTokenError:
        return jsonify({'error': 'Invalid token'}), 401

    if 'file' not in request.files:
        return jsonify({'error': 'No file part'}), 400

    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'No selected file'}), 400

    filename = secure_filename(file.filename)
    file_path = os.path.join(filename)
    file.save(file_path)

    # =========================
    # 1️⃣ UPDATE EXCEL FILE
    # =========================
    try:
        sheet_name = "SKU Information Tab"
        df = pd.read_excel(file_path, sheet_name=sheet_name)

        if "Local Stock" not in df.columns:
            df["Local Stock"] = 0

        if "In Transit Units" not in df.columns:
            df["In Transit Units"] = 0

        with pd.ExcelWriter(file_path, engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
            df.to_excel(writer, sheet_name=sheet_name, index=False)

    except Exception as e:
        return jsonify({'error': f'Excel update failed: {str(e)}'}), 500

    # =========================
    # 2️⃣ UPDATE DATABASE TABLE
    # =========================
    try:
        table_name = f"sku_{user_id}_data_table"

        inspector = inspect(user_engine)

        if table_name not in inspector.get_table_names():
            return jsonify({'error': f'Table {table_name} not found'}), 404

        with user_engine.begin() as conn:
            existing_columns = {col["name"] for col in inspect(conn).get_columns(table_name)}

            if "local_stock" not in existing_columns:
                conn.execute(text(f'''
                    ALTER TABLE "{table_name}"
                    ADD COLUMN local_stock INTEGER DEFAULT 0;
                '''))

            if "in_transit_units" not in existing_columns:
                conn.execute(text(f'''
                    ALTER TABLE "{table_name}"
                    ADD COLUMN in_transit_units INTEGER DEFAULT 0;
                '''))

    except SQLAlchemyError as e:
        return jsonify({'error': 'Database error', 'message': str(e)}), 500
    except Exception as e:
        return jsonify({'error': str(e)}), 500

    return jsonify({
        "success": True,
        "message": "File uploaded and stock columns ensured"
    }), 200




@product_bp.route('/updatePrices', methods=['POST'])
def update_prices():
    # Authorization
    auth_header = request.headers.get('Authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        return jsonify({'error': 'Authorization token is missing or invalid'}), 401

    token = auth_header.split(' ')[1]
    try:
        payload, user_id, member_id = get_effective_user_id_from_token(token)
    except jwt.ExpiredSignatureError:
        return jsonify({'error': 'Token has expired'}), 401
    except jwt.InvalidTokenError:
        return jsonify({'error': 'Invalid token'}), 401

    # Parse update payload
    data = request.get_json() or {}
    rows = data.get('rows', {})

    if not rows:
        return jsonify({'error': 'No rows provided to update'}), 400

    # DB setup
    Session = sessionmaker(bind=user_engine)
    user_session = Session()

    sku_table_name = f"sku_{user_id}_data_table"
    sku_data_table = Table(sku_table_name, MetaData(), autoload_with=user_engine)

    failed_products = []
    updated_products = []

    try:
        # Fetch all existing product_names for validation
        existing_products_result = user_session.execute(
            select(sku_data_table.c.product_name)
        ).fetchall()
        existing_products = set(row[0] for row in existing_products_result)

        for product_name, updates in rows.items():
            if product_name not in existing_products:
                failed_products.append(product_name)
                continue

            if not isinstance(updates, dict):
                failed_products.append(product_name)
                continue

            update_data = {}

            # price (optional)
            if "price" in updates and updates["price"] is not None:
                try:
                    update_data["price"] = float(updates["price"])
                except Exception:
                    failed_products.append(product_name)
                    continue

            # local_stock (optional)
            if "local_stock" in updates and updates["local_stock"] is not None:
                try:
                    update_data["local_stock"] = int(updates["local_stock"])
                except Exception:
                    failed_products.append(product_name)
                    continue

            # in_transit_units (optional)
            if "in_transit_units" in updates and updates["in_transit_units"] is not None:
                try:
                    update_data["in_transit_units"] = int(updates["in_transit_units"])
                except Exception:
                    failed_products.append(product_name)
                    continue

            # If nothing valid to update
            if not update_data:
                failed_products.append(product_name)
                continue

            result = user_session.execute(
                sku_data_table.update()
                .where(sku_data_table.c.product_name == product_name)
                .values(**update_data)
            )

            if result.rowcount == 0:
                failed_products.append(product_name)
            else:
                updated_products.append(product_name)

        user_session.commit()

        # Return updated table
        select_stmt = select(sku_data_table).order_by(sku_data_table.c.id.asc())
        result = user_session.execute(select_stmt)
        updated_data = [dict(row._mapping) for row in result]

        return jsonify({
            'message': 'Update completed (price + stock)',
            'updated_products': updated_products,
            'not_updated_products': failed_products,
            'data': updated_data
        }), 200

    except Exception as e:
        user_session.rollback()
        return jsonify({'error': f'Error updating rows: {str(e)}'}), 500
    finally:
        user_session.close()



@product_bp.route('/skuprice', methods=['GET'])
def skuprice():
    auth_header = request.headers.get('Authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        return jsonify({'error': 'Authorization token is missing or invalid'}), 401

    token = auth_header.split(' ')[1]
    try:
        payload, user_id, member_id = get_effective_user_id_from_token(token)
    except jwt.ExpiredSignatureError:
        return jsonify({'error': 'Token has expired'}), 401
    except jwt.InvalidTokenError:
        return jsonify({'error': 'Invalid token'}), 401



    table_name = f"sku_{user_id}_data_table"

    try:
        # Connect to PostgreSQL
        inspector = inspect(user_engine)

        # Check if the table exists
        if table_name not in inspector.get_table_names():
            return jsonify({'error': f'Table "{table_name}" not found'}), 404

        # Load table metadata
        metadata = MetaData()
        sku_data_table = Table(table_name, metadata, autoload_with=user_engine)

        # Query the table
        with user_engine.connect() as conn:
            query = sku_data_table.select()
            results = conn.execute(query).mappings().all()

        # Convert to dict and return
        result_dicts = [dict(row) for row in results]
        return jsonify(result_dicts), 200

    except SQLAlchemyError as e:
        return jsonify({'error': 'Database error', 'message': str(e)}), 500
    except Exception as e:
        return jsonify({'error': 'An error occurred while fetching SKU data', 'message': str(e)}), 500



@product_bp.route('/get_error_file/<string:country>/<string:month>/<string:year>', methods=['GET'])
def get_error_file(country, month, year):
    auth_header = request.headers.get('Authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        return jsonify({'error': 'Authorization token is missing or invalid'}), 401

    token = auth_header.split(' ')[1]
    try:
        payload, user_id, member_id = get_effective_user_id_from_token(token)
    except jwt.ExpiredSignatureError:
        return jsonify({'error': 'Token has expired'}), 401
    except jwt.InvalidTokenError:
        return jsonify({'error': 'Invalid token'}), 401

    # Construct the filename for the error file (which is actually inventory_forecast)
    error_filename = f"error_file_{user_id}{country}{month}_{year}.xlsx"
    error_file_path = os.path.join( error_filename)

    # Check if the file exists
    if not os.path.exists(error_file_path):
        return jsonify({'error': 'Error file not found'}), 404

    try:
        # Send the existing forecast file as a download
        return send_file(error_file_path, as_attachment=True)

    except Exception as e:
        return jsonify({'error': 'An error occurred while sending the error file'}), 500
   

@product_bp.route('/get_consolidated_table_name/<string:country_name>', methods=['GET'])
def get_consolidated_table_name(country_name):
    # Authorization
    auth_header = request.headers.get('Authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        return jsonify({'error': 'Authorization token is missing or invalid'}), 401

    token = auth_header.split(' ')[1]
    try:
        payload, user_id, member_id = get_effective_user_id_from_token(token)
    except jwt.ExpiredSignatureError:
        return jsonify({'error': 'Token has expired'}), 401
    except jwt.InvalidTokenError:
        return jsonify({'error': 'Invalid token'}), 401

    # Sanitize country name to form a safe table name
    def sanitize_identifier(identifier):
        return re.sub(r'\W|^(?=\d)', '_', identifier)

    safe_country_name = sanitize_identifier(country_name)
    consolidated_table_name = f"user_{user_id}_{safe_country_name}_merge_data_of_all_months"

    try:
        # Create engine
        inspector = inspect(user_engine)

        # Check if the table exists
        if consolidated_table_name not in inspector.get_table_names():
            return jsonify({'error': f'Table "{consolidated_table_name}" not found for user {user_id}'}), 404

        # Define and query the table
        metadata = MetaData()
        consolidated_table = Table(consolidated_table_name, metadata, autoload_with=user_engine)

        with user_engine.connect() as conn:
            results = conn.execute(consolidated_table.select()).mappings().all()

        result_dicts = [dict(row) for row in results]
        return jsonify(result_dicts), 200

    except SQLAlchemyError as e:
        return jsonify({'error': 'Database error', 'message': str(e)}), 500
    except Exception as e:
        return jsonify({'error': 'An unexpected error occurred', 'message': str(e)}), 500

def resolve_country(country, currency):
    country = (country or "").lower()
    currency = (currency or "").lower()

    # 1. If country = global
    if country == "global":
        if currency == "usd":
            return "global"
        elif currency == "inr":
            return "global_inr"
        elif currency == "gbp":
            return "global_gbp"
        elif currency == "cad":
            return "global_cad"
        else:
            return "global"  # default fallback

    # 2. If country = uk
    if country == "uk":
        if currency == "usd":
            return "uk_usd"
        else:
            return "uk"  # default for all other currencies

    # 3. Default (no special logic)
    return country

# @product_bp.route('/skutableprofit/<string:skuwise_file_name>', methods=['GET'])
# def skutableprofit(skuwise_file_name):
#     auth_header = request.headers.get('Authorization')
#     if not auth_header or not auth_header.startswith('Bearer '):
#         return jsonify({'error': 'Authorization token is missing or invalid'}), 401

#     token = auth_header.split(' ')[1]
#     try:
#         payload, user_id, member_id = get_effective_user_id_from_token(token)
#     except jwt.ExpiredSignatureError:
#         return jsonify({'error': 'Token has expired'}), 401
#     except jwt.InvalidTokenError:
#         return jsonify({'error': 'Invalid token'}), 401

#     try:
#         engine = user_engine

#         country_param = request.args.get('country', '')
#         currency_param = (request.args.get('homeCurrency') or '').lower()

#         country = resolve_country(country_param, currency_param)
#         month = (request.args.get('month') or '').strip().lower()
#         year = (request.args.get('year') or '').strip()

#         # Current table name
#         if country and month and year:
#             table_name = f"skuwisemonthly_{user_id}_{country}_{month}{year}".lower()
#         else:
#             table_name = skuwise_file_name

#         metadata = MetaData(schema='public')

#         def _fetch_as_dicts(tbl_name):
#             user_specific_table = Table(tbl_name, metadata, autoload_with=engine)
#             with engine.connect() as conn:
#                 results = conn.execute(
#                     select(*user_specific_table.columns)
#                 ).mappings().all()

#             return [_normalize_sku_row(dict(row)) for row in results]

#         try:
#             current_data = _fetch_as_dicts(table_name)
#         except Exception:
#             if table_name != skuwise_file_name:
#                 try:
#                     table_name = skuwise_file_name
#                     current_data = _fetch_as_dicts(table_name)
#                 except Exception:
#                     return jsonify({
#                         'error': f"Table '{table_name}' or '{skuwise_file_name}' not found for user {user_id}"
#                     }), 404
#             else:
#                 return jsonify({
#                     'error': f"Table '{table_name}' not found for user {user_id}"
#                 }), 404

#         previous_table_name = None
#         previous_data = []

#         if country and month and year:
#             prev_month, prev_year = get_previous_month(month, year)

#             if prev_month and prev_year:
#                 previous_table_name = f"skuwisemonthly_{user_id}_{country}_{prev_month}{prev_year}".lower()

#                 try:
#                     previous_data = _fetch_as_dicts(previous_table_name)
#                 except Exception:
#                     previous_data = []

#         return jsonify({
#             "current_table_name": table_name,
#             "current_data": current_data,
#             "previous_table_name": previous_table_name,
#             "previous_data": previous_data
#         }), 200

#     except Exception as e:
#         return jsonify({
#             'error': 'An unexpected error occurred',
#             'message': str(e)
#         }), 500

def build_skuwise_table_name(user_id, country, month, year):
    month = month.strip().lower()
    year = str(year).strip()

    if country == "global":
        return f"skuwisemonthly_{user_id}_global_{month}{year}_table".lower()

    return f"skuwisemonthly_{user_id}_{country}_{month}{year}".lower()

@product_bp.route('/skutableprofit', methods=['GET'])
def skutableprofit():
    auth_header = request.headers.get('Authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        return jsonify({'error': 'Authorization token is missing or invalid'}), 401

    token = auth_header.split(' ')[1]

    try:
        payload, user_id, member_id = get_effective_user_id_from_token(token)
    except jwt.ExpiredSignatureError:
        return jsonify({'error': 'Token has expired'}), 401
    except jwt.InvalidTokenError:
        return jsonify({'error': 'Invalid token'}), 401

    try:
        engine = user_engine

        country_param = request.args.get('country', '')
        currency_param = (request.args.get('homeCurrency') or '').lower()

        country = resolve_country(country_param, currency_param)
        month = (request.args.get('month') or '').strip().lower()
        year = (request.args.get('year') or '').strip()

        if not country or not month or not year:
            return jsonify({
                'error': 'country, month, and year are required'
            }), 400

        # Main SKU monthly table:
        # example: skuwisemonthly_2_uk_may2026
        table_name = build_skuwise_table_name(user_id, country, month, year)

        # Ads table:
        requested_ads_table_name = f"skuwisemonthly_{user_id}_{country}_{month}_{year}".lower()

        inspector = inspect(engine)
        existing_tables = set(inspector.get_table_names(schema="public"))

        ads_table_name = requested_ads_table_name if requested_ads_table_name in existing_tables else None

        metadata = MetaData(schema='public')

        def safe_float(value):
            try:
                if value is None:
                    return 0.0
                return float(value)
            except Exception:
                return 0.0

        def safe_divide(numerator, denominator):
            numerator = safe_float(numerator)
            denominator = safe_float(denominator)
            if denominator == 0:
                return 0.0
            return numerator / denominator

        def _fetch_profit_data(main_tbl_name, ads_tbl_name=None):
            main_table = Table(main_tbl_name, metadata, autoload_with=engine)

            with engine.connect() as conn:
                main_rows = conn.execute(
                    select(*main_table.columns)
                ).mappings().all()

                ads_rows = []

                if ads_tbl_name:
                    ads_table = Table(ads_tbl_name, metadata, autoload_with=engine)
                    ads_rows = conn.execute(
                        select(*ads_table.columns)
                    ).mappings().all()

            final_data = []

            for index, row in enumerate(main_rows):
                row_dict = _normalize_sku_row(dict(row))

                ads_spend = 0.0

                # Existing row-by-row ads_spend matching
                if index < len(ads_rows):
                    ads_dict = _normalize_sku_row(dict(ads_rows[index]))

                    ads_spend = safe_float(
                        ads_dict.get("ads_spend")
                        if ads_dict.get("ads_spend") is not None
                        else ads_dict.get("ads_spend_raw")
                        if ads_dict.get("ads_spend_raw") is not None
                        else ads_dict.get("product_spend")
                    )

                profit = safe_float(row_dict.get("profit"))
                net_sales = safe_float(row_dict.get("net_sales"))
                total_quantity = safe_float(row_dict.get("total_quantity"))

                cm2_profit = profit - ads_spend
                acos = safe_divide(ads_spend, net_sales) * 100
                cm2_profit_per = safe_divide(cm2_profit, net_sales) * 100
                cm2_profit_per_unit = safe_divide(cm2_profit, total_quantity)

                row_dict["ads_spend"] = round(ads_spend, 2)
                row_dict["cm2_profit"] = round(cm2_profit, 2)
                row_dict["acos"] = round(acos, 2)
                row_dict["cm2_profit_per"] = round(cm2_profit_per, 2)
                row_dict["cm2_profit_per_unit"] = round(cm2_profit_per_unit, 2)

                final_data.append(row_dict)

            # ----------------------------------------------------
            # Add final TOTAL row values only if ads table exists.
            # If ads table is missing, keep values 0 and do not fail.
            # ----------------------------------------------------
            if ads_rows:
                def get_total_ads_row(ads_rows):
                    for ads_row in ads_rows:
                        ads_dict = _normalize_sku_row(dict(ads_row))

                        sku = str(ads_dict.get("sku") or "").strip().lower()
                        product_name = str(ads_dict.get("product_name") or "").strip().lower()

                        if sku == "total" or product_name == "total":
                            return ads_dict

                    return _normalize_sku_row(dict(ads_rows[-1]))

                ads_total_row = get_total_ads_row(ads_rows)

                brand_spend_total = safe_float(ads_total_row.get("brand_spend"))

                dealsvouchar_ads_total = abs(safe_float(
                    ads_total_row.get("dealsvouchar_ads")
                    if ads_total_row.get("dealsvouchar_ads") is not None
                    else ads_total_row.get("dealsvoucher_ads")
                    if ads_total_row.get("dealsvoucher_ads") is not None
                    else ads_total_row.get("deals_voucher_ads")
                ))

                advertising_total = brand_spend_total + dealsvouchar_ads_total
                advertising_total_final = advertising_total + ads_spend 

                for row_dict in final_data:
                    sku = str(row_dict.get("sku") or "").strip().lower()
                    product_name = str(row_dict.get("product_name") or "").strip().lower()

                    if sku == "total" or product_name == "total":
                        cm2_profit = safe_float(row_dict.get("cm2_profit"))
                        platform_fee = safe_float(row_dict.get("platform_fee"))

                        cm2_profit_total = cm2_profit - advertising_total - platform_fee

                        row_dict["brand_spend"] = round(brand_spend_total, 2)
                        row_dict["dealsvouchar_ads"] = round(dealsvouchar_ads_total, 2)
                        row_dict["advertising_total"] = round(advertising_total, 2)
                        row_dict["advertising_total_final"] = round(advertising_total_final, 2)
                        row_dict["cm2_profit_total"] = round(cm2_profit_total, 2)

                        break

            return final_data

        try:
            current_data = _fetch_profit_data(table_name, ads_table_name)
        except Exception as e:
            return jsonify({
                "error": "Failed to calculate SKU profit data",
                "current_table_name": table_name,
                "ads_table_name": ads_table_name,
                "message": str(e)
            }), 500

        previous_table_name = None
        previous_ads_table_name = None
        previous_data = []

        prev_month, prev_year = get_previous_month(month, year)

        if prev_month and prev_year:
            previous_table_name = build_skuwise_table_name(
                user_id,
                country,
                prev_month,
                prev_year
            )

            # Do NOT load previous ads table
            previous_ads_table_name = None

            try:
                previous_data = _fetch_profit_data(
                    previous_table_name,
                    None
                )
            except Exception as e:
                previous_data = []
                print("Previous data error:", str(e))

        return jsonify({
            "current_table_name": table_name,
            "current_ads_table_name": ads_table_name,
            "requested_ads_table_name": requested_ads_table_name,
            "current_data": current_data,
            "previous_table_name": previous_table_name,
            "previous_ads_table_name": previous_ads_table_name,
            "previous_data": previous_data
        }), 200

    except Exception as e:
        return jsonify({
            'error': 'An unexpected error occurred',
            'message': str(e)
        }), 500
    


@product_bp.route('/get_table_data/<string:file_name>', methods=['GET'])
def get_table_data(file_name):
    # --- Auth ---
    auth_header = request.headers.get('Authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        return jsonify({'error': 'Authorization token missing'}), 401

    token = auth_header.split(' ')[1]
    try:
        payload, user_id, member_id = get_effective_user_id_from_token(token)
    except:
        return jsonify({'error': 'Invalid or expired token'}), 401

    country = request.args.get('country')
    month   = request.args.get('month')   # monthly: "Jan" / "01" etc, quarterly: "Q1"/"Q2" etc (or month inside quarter)
    year    = request.args.get('year')    # "2025"
    qtd = (request.args.get("qtd") or "").strip().lower() == "true"
    ytd = (request.args.get("ytd") or "").strip().lower() == "true"
    quarter = request.args.get("quarter")  # e.g. Q4

    # decide range
    if qtd:
        range_ = "quarterly"
    elif ytd:
        range_ = "yearly"
    else:
        range_ = "monthly"

    
    def _month_str_to_int(m):
        if m is None:
            return None
        m = str(m).strip()
        # numeric month
        if m.isdigit():
            mi = int(m)
            return mi if 1 <= mi <= 12 else None

        # short/long month names
        mm = m.lower()[:3]
        mapping = {
            "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
            "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12
        }
        return mapping.get(mm)

    def _quarter_to_months(q):
        q = (q or "").strip().upper()
        if q in ("Q1", "1"):
            return [1, 2, 3]
        if q in ("Q2", "2"):
            return [4, 5, 6]
        if q in ("Q3", "3"):
            return [7, 8, 9]
        if q in ("Q4", "4"):
            return [10, 11, 12]
        return None

    try:
        engine = user_engine
        conn = engine.connect()
        inspector = inspect(engine)
        tables = inspector.get_table_names()

        # ---------------------------------------------
        # ✅ DATA SOURCE SELECTION (monthly vs quarter/year)
        # ---------------------------------------------
        source_table = None

        if range_ == "monthly":
            # monthly => use passed file_name table only
            if file_name not in tables:
                return jsonify({'error': f'Table {file_name} not found'}), 404
            source_table = file_name

        elif range_ in ("quarterly", "yearly"):
            # quarter/year => use merged table
            merged_table = f"user_{user_id}_{country}_merge_data_of_all_months".lower()
            if merged_table not in tables:
                return jsonify({'error': f'Merged table {merged_table} not found'}), 404
            source_table = merged_table

        else:
            return jsonify({"error": "Invalid range. Use monthly/quarterly/yearly"}), 400

        raw_df = pd.read_sql(text(f'SELECT * FROM "{source_table}"'), conn)
        raw_table_data = raw_df.to_dict(orient="records")

        # ---------------------------------------------
        # ✅ FILTER MONTHS/YEAR IF quarterly/yearly
        # ---------------------------------------------
        df = raw_df.copy()

        if range_ in ("quarterly", "yearly"):
            # Expect merged table to have month/year columns.
            # We’ll try to filter if columns exist; otherwise we keep whole df (safe fallback).
            year_val = None
            try:
                year_val = int(str(year).strip()) if year is not None else None
            except:
                year_val = None

            if "year" in df.columns and year_val is not None:
                df["year"] = pd.to_numeric(df["year"], errors="coerce")
                df = df[df["year"] == year_val]

            # month filtering
            if "month" in df.columns:
                # convert month col to int 1-12 where possible
                df["month_num"] = df["month"].apply(_month_str_to_int)
            elif "month_num" in df.columns:
                df["month_num"] = pd.to_numeric(df["month_num"], errors="coerce")
            else:
                df["month_num"] = None  # can't filter by month

            if range_ == "quarterly":
                q = quarter or month  # quarter might be in quarter param OR in month param (Q1/Q2..)
                months_list = _quarter_to_months(q)

                # if quarter not provided, but month is a real month => derive quarter
                if months_list is None:
                    m_int = _month_str_to_int(month)
                    if m_int:
                        if 1 <= m_int <= 3:
                            months_list = [1, 2, 3]
                        elif 4 <= m_int <= 6:
                            months_list = [4, 5, 6]
                        elif 7 <= m_int <= 9:
                            months_list = [7, 8, 9]
                        else:
                            months_list = [10, 11, 12]

                if months_list and df["month_num"].notna().any():
                    df = df[df["month_num"].isin(months_list)]

            elif range_ == "yearly":
                # yearly => all months of that year (already filtered by year if possible)
                pass

            # drop helper column if present
            if "month_num" in df.columns:
                # keep it if you want; here we drop
                df = df.drop(columns=["month_num"], errors="ignore")

        
        
        




        df["other"] = pd.to_numeric(df.get("other", 0), errors="coerce").fillna(0)
        other_total = float(df["other"].sum())

        # ✅ advertising_total sum
        if "advertising_total" in df.columns:
            df["advertising_total"] = pd.to_numeric(df["advertising_total"], errors="coerce").fillna(0)
            advertising_total_sum = float(df["advertising_total"].sum())
        else:
            advertising_total_sum = 0.0

        # ✅ adjust other for frontend
        other_total_adjusted = other_total - advertising_total_sum


        # ✅ Keep real NaN as <NA>, don't convert to "nan" string
        df["sku"] = df["sku"].astype("string").str.strip()

        # ✅ Remove invalid SKUs
        invalid_skus = {"", "0", "0.0", "nan", "none", "<na>"}
        df = df[df["sku"].notna() & (~df["sku"].str.lower().isin(invalid_skus))]


        # ✅ RAW ROW-LEVEL split (after SKU cleanup)
        df["errorstatus"] = df["errorstatus"].astype(str).str.strip().str.lower()

        raw_ok_df    = df[df["errorstatus"] == "ok"]
        raw_under_df = df[df["errorstatus"] == "undercharged"]
        raw_over_df  = df[df["errorstatus"] == "overcharged"]
        raw_ref_df   = df[~df["errorstatus"].isin(["ok", "undercharged", "overcharged"])]

        numeric_cols = [
            "product_sales", "promotional_rebates", "other",
            "selling_fees", "answer", "difference", "quantity", "total_value", "fba_fees",
            "platform_fee"
        ]

        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
        # ================= FIX QUANTITY (exclude LOST descriptions) =================
        if "quantity" in df.columns:
            df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce").fillna(0)

            # description normalize (case-insensitive)
            desc_str = df.get("description", pd.Series("", index=df.index)).astype(str).str.strip().str.upper()

            EXCLUDE_QTY_DESCRIPTIONS = {
                "REVERSAL_REIMBURSEMENT",
                "WAREHOUSE_LOST",
                "WAREHOUSE_DAMAGE",
                "MISSING_FROM_INBOUND",
            }

            exclude_qty_mask = desc_str.isin(EXCLUDE_QTY_DESCRIPTIONS)

            # ✅ un rows ki quantity count hi nahi hogi
            df.loc[exclude_qty_mask, "quantity"] = 0
        # ======================================================================


        df["net_sales_total_value"] = (
            df.get("product_sales", 0) +
            df.get("promotional_rebates", 0) +
            df.get("other", 0)
        )

        def status_row(row):
            es = str(row.get("errorstatus", "")).lower()
            if es == "ok":
                return "Accurate"
            if es == "overcharged":
                return "Overcharged"
            if es == "undercharged":
                return "Undercharged"
            return "noreferallfee"

        df["status"] = df.apply(status_row, axis=1)

        req_cols = [
            "sku", "product_name", "product_sales",
            "net_sales_total_value", "selling_fees", "fba_fees",
            "answer", "errorstatus", "difference", "status", "quantity", "total_value"
        ]
        # keep only available cols
        req_cols = [c for c in req_cols if c in df.columns]
        final_df = df[req_cols].copy()

        # --- SKU wise aggregation ---
        agg_cols = [
            "product_sales",
            "net_sales_total_value",
            "selling_fees",
            "fba_fees",
            "answer",
            "difference",
            "quantity",
            "total_value"
        ]
        agg_cols = [c for c in agg_cols if c in final_df.columns]

        

        final_df = final_df.groupby(["sku", "product_name", "status"], as_index=False)[agg_cols].sum()

        accurate_df = final_df[final_df["status"] == "Accurate"]
        under_df    = final_df[final_df["status"] == "Undercharged"]
        over_df     = final_df[final_df["status"] == "Overcharged"]
        ref_df      = final_df[final_df["status"] == "noreferallfee"]

        def create_total_row(_df, label):
            row = {
                "sku": f"Charge - {label}",
                "product_name": "",
                "errorstatus": "",
                "status": label
            }
            for c in agg_cols:
                row[c] = float(_df[c].sum())
            return pd.DataFrame([row])

        acc_total   = create_total_row(accurate_df, "Accurate")
        under_total = create_total_row(under_df, "Undercharged")
        over_total  = create_total_row(over_df, "Overcharged")
        ref_total   = create_total_row(ref_df, "noreferallfee")

        grand_row = {
            "sku": "Grand Total",
            "product_name": "",
            "errorstatus": "",
            "status": "Total"
        }
        for c in agg_cols:
            grand_row[c] = float(final_df[c].sum())
        grand_total = pd.DataFrame([grand_row])

        final_display_df = pd.concat(
            [acc_total, accurate_df, under_total, under_df, over_total, over_df, ref_total, ref_df, grand_total],
            ignore_index=True
        )

        final_df = final_display_df

        # ---------------------------------------------
        # ✅ SAVE SKUWISE TABLES (monthly / quarter / year)
        # ---------------------------------------------
        skutable = None
        if country and year and (range_ != "monthly" or month):
            if range_ == "monthly":
                skutable = f"skuwise_{user_id}_{country}_{month}{year}".lower()

            elif range_ == "quarterly":
                q = (quarter or month or "").strip().upper()
                if not q.startswith("Q"):
                    # derive from month if month is like "Jan"/"2"
                    m_int = _month_str_to_int(month)
                    if m_int:
                        q = "Q1" if 1 <= m_int <= 3 else "Q2" if 4 <= m_int <= 6 else "Q3" if 7 <= m_int <= 9 else "Q4"
                skutable = f"skuwisequarter_{user_id}_{country}_{q}{year}".lower()

            elif range_ == "yearly":
                skutable = f"skuwiseyear_{user_id}_{country}_{year}".lower()

            if skutable:
                final_df.to_sql(skutable, engine, if_exists="replace", index=False)

        # ---------------------------------------------
        # ✅ PLATFORM FEE TOTAL (monthly/quarter/year)
        # ---------------------------------------------
        # ---------------------------------------------
        # ✅ PLATFORM FEE TOTAL (monthly/quarter/year)
        # ---------------------------------------------
        platform_fee_total = 0.0
        try:
            # ✅ Best: filtered df se sum (works for monthly/quarterly/yearly)
            if "platform_fee" in df.columns:
                platform_fee_total = float(pd.to_numeric(df["platform_fee"], errors="coerce").fillna(0).sum())
            else:
                # ✅ Fallback: direct table se sum (as you asked)
                table_for_fee = None

                if range_ == "monthly" and country and month and year:
                    # NOTE: agar aapke monthly ka actual name _table suffix ke saath hai to yahan add kar do
                    table_for_fee = f"skuwisemonthly_{user_id}_{country}_{month}{year}".lower()

                elif range_ == "quarterly" and country and year:
                    q = (quarter or month or "").strip().upper()

                    # derive quarter number (1–4)
                    if q.startswith("Q"):
                        q_num = q.replace("Q", "")
                    else:
                        m_int = _month_str_to_int(month)
                        if m_int:
                            q_num = "1" if 1 <= m_int <= 3 else "2" if 4 <= m_int <= 6 else "3" if 7 <= m_int <= 9 else "4"
                        else:
                            q_num = None

                    if q_num:
                        table_for_fee = f"quarter{q_num}_{user_id}_{country}_{year}_table".lower()


                elif range_ == "yearly" and country and year:
                    # ✅ your required format
                    table_for_fee = f"skuwiseyearly_{user_id}_{country}_{year}_table".lower()

                if table_for_fee and table_for_fee in inspector.get_table_names():
                    res = conn.execute(text(f'''
                        SELECT COALESCE(SUM(platform_fee), 0) AS total_platform_fee
                        FROM "{table_for_fee}"
                    ''')).fetchone()
                    platform_fee_total = float(res[0] or 0)

        except Exception as e:
            platform_fee_total = 0.0


        conn.close()

        import numpy as np
        final_df    = final_df.replace({np.nan: 0})
        accurate_df = accurate_df.replace({np.nan: 0})
        under_df    = under_df.replace({np.nan: 0})
        over_df     = over_df.replace({np.nan: 0})
        ref_df      = ref_df.replace({np.nan: 0})

        return jsonify({
            "success": True,
            "message": "SKU wise table generated successfully.",
            "range": range_,
            "table": final_df.to_dict(orient="records"),
            "accurate_data": raw_ok_df.to_dict(orient="records"),
            "undercharged_data": raw_under_df.to_dict(orient="records"),
            "overcharged_data": raw_over_df.to_dict(orient="records"),
            "no_ref_fee_data": raw_ref_df.to_dict(orient="records"),
            "created_table_name": skutable,
            "raw_table": raw_table_data,     # raw of source table (monthly or merged)
            "table_name": source_table,      # which table was used

            "platform_fee_total": platform_fee_total,
            "other_total": other_total_adjusted,
            "advertising_total": advertising_total_sum,

        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@product_bp.route('/uploadWarehouseData', methods=['POST', 'GET'])
def upload_warehouse_data():
    # ---------- AUTH ----------
    auth_header = request.headers.get('Authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        return jsonify({'error': 'Authorization token is missing or invalid'}), 401

    token = auth_header.split(' ')[1]
    try:
        payload, user_id, member_id = get_effective_user_id_from_token(token)
    except jwt.ExpiredSignatureError:
        return jsonify({'error': 'Token has expired'}), 401
    except jwt.InvalidTokenError:
        return jsonify({'error': 'Invalid token'}), 401

    # ---------- HELPERS ----------
    def sanitize_identifier(value):
        value = str(value).strip().lower()
        value = re.sub(r'[^a-zA-Z0-9_]+', '_', value)
        value = re.sub(r'_+', '_', value).strip('_')
        return value or "default"

    def normalize_column_name(col):
        col = str(col).strip().lower()
        col = re.sub(r'[^a-zA-Z0-9_]+', '_', col)
        col = re.sub(r'_+', '_', col).strip('_')
        return col

    country = (request.form.get('country') or request.args.get('country') or '').strip().lower()
    if not country:
        return jsonify({'error': 'country is required'}), 400

    safe_country = sanitize_identifier(country)
    warehouse_table_name = f"warehouse_{user_id}_{safe_country}_data"
    sku_table_name = f"sku_{user_id}_data_table"


    # ---------- GET ----------
    if request.method == 'GET':
        try:
            df = pd.read_sql(f'SELECT * FROM "{warehouse_table_name}"', user_engine)
            return jsonify({
                'success': True,
                'message': 'Warehouse data fetched successfully',
                'table_name': warehouse_table_name,
                'columns': df.columns.tolist(),
                'row_count': int(len(df)),
                'data': df.to_dict(orient="records")
            }), 200

        except Exception as e:
            return jsonify({
                'error': 'Warehouse data not found',
                'message': str(e)
            }), 404

    # ---------- POST ----------
    if 'file' not in request.files:
        return jsonify({'error': 'No file part found'}), 400

    file = request.files['file']
    if not file or file.filename == '':
        return jsonify({'error': 'No selected file'}), 400

    filename = secure_filename(file.filename)
    temp_path = os.path.join(basedir, filename)

    try:
        file.save(temp_path)

        # Read uploaded Excel
        df = pd.read_excel(temp_path)

        if df.empty:
            return jsonify({'error': 'Uploaded Excel file is empty'}), 400

        # Normalize column names
        df.columns = [normalize_column_name(c) for c in df.columns]
        df = df.dropna(how='all')

        # Required stock columns
        if 'local_stock' not in df.columns:
            df['local_stock'] = 0
        if 'in_transit_units' not in df.columns:
            df['in_transit_units'] = 0

        # Numeric conversions
        for col in ['local_stock', 'in_transit_units', 'year', 's_no', 'price']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        # Clean object columns
        for col in df.columns:
            if df[col].dtype == object:
                df[col] = df[col].astype(str).str.strip()
                df[col] = df[col].replace({'nan': None, 'None': None, '': None})

        # Save uploaded warehouse data table
        df.to_sql(warehouse_table_name, user_engine, if_exists='replace', index=False)

        # ---------------------------------------------------
        # UPDATE public.sku_{user_id}_data_table
        # ---------------------------------------------------
        inspector = inspect(user_engine)
        existing_tables = inspector.get_table_names()

        if sku_table_name not in existing_tables:
            return jsonify({
                'success': True,
                'message': f'Warehouse data uploaded, but SKU table "{sku_table_name}" not found',
                'warehouse_table_name': warehouse_table_name,
                'file_name': filename,
                'columns': df.columns.tolist(),
                'row_count': int(len(df)),
                'data': df.to_dict(orient="records")
            }), 200

        with user_engine.begin() as conn:
            # Ensure stock columns exist in SKU table
            existing_columns = {col["name"] for col in inspect(conn).get_columns(sku_table_name)}

            if "local_stock" not in existing_columns:
                conn.execute(text(f'''
                    ALTER TABLE "{sku_table_name}"
                    ADD COLUMN local_stock INTEGER DEFAULT 0
                '''))

            if "in_transit_units" not in existing_columns:
                conn.execute(text(f'''
                    ALTER TABLE "{sku_table_name}"
                    ADD COLUMN in_transit_units INTEGER DEFAULT 0
                '''))

            # Load SKU table
            sku_df = pd.read_sql(f'SELECT * FROM "{sku_table_name}"', conn)
            sku_df.columns = [normalize_column_name(c) for c in sku_df.columns]

            # Decide matching key
            possible_keys = ['asin', 'product_barcode', 'sku_uk', 'sku_us']
            match_key = None
            for key in possible_keys:
                if key in df.columns and key in sku_df.columns:
                    match_key = key
                    break

            if not match_key:
                return jsonify({
                    'success': False,
                    'message': 'Warehouse uploaded, but no common matching column found to update SKU table',
                    'possible_keys_checked': possible_keys,
                    'warehouse_columns': df.columns.tolist(),
                    'sku_columns': sku_df.columns.tolist()
                }), 400

            # Prepare update dataframe
            update_df = df[[match_key, 'local_stock', 'in_transit_units']].copy()
            update_df = update_df.dropna(subset=[match_key])
            update_df[match_key] = update_df[match_key].astype(str).str.strip()

            update_df['local_stock'] = pd.to_numeric(update_df['local_stock'], errors='coerce').fillna(0).astype(int)
            update_df['in_transit_units'] = pd.to_numeric(update_df['in_transit_units'], errors='coerce').fillna(0).astype(int)

            # If duplicate keys exist in uploaded file, keep last one
            update_df = update_df.drop_duplicates(subset=[match_key], keep='last')

            # Update rows in sku table
            updated_count = 0
            for _, row in update_df.iterrows():
                result = conn.execute(
                    text(f'''
                        UPDATE "{sku_table_name}"
                        SET local_stock = :local_stock,
                            in_transit_units = :in_transit_units
                        WHERE CAST({match_key} AS TEXT) = :match_value
                    '''),
                    {
                        "local_stock": int(row["local_stock"]),
                        "in_transit_units": int(row["in_transit_units"]),
                        "match_value": str(row[match_key]).strip()
                    }
                )
                updated_count += result.rowcount

        return jsonify({
            'success': True,
            'message': 'Warehouse Excel uploaded and SKU stock values updated successfully',
            'warehouse_table_name': warehouse_table_name,
            'sku_table_name': sku_table_name,
            'matched_on': match_key,
            'updated_rows': int(updated_count),
            'file_name': filename,
            'columns': df.columns.tolist(),
            'row_count': int(len(df)),
            'data': df.to_dict(orient="records")
        }), 200

    except Exception as e:
        traceback.print_exc()
        return jsonify({
            'error': 'Failed to upload warehouse data',
            'message': str(e)
        }), 500

    finally:
        try:
            if os.path.exists(temp_path):
                os.remove(temp_path)
        except Exception:
            pass



        