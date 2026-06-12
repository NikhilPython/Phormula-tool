from datetime import datetime
from flask import Blueprint, request, jsonify
import jwt
from sqlalchemy import create_engine, text, inspect
import os
import base64
from config import Config
import json
from openai import OpenAI
from dotenv import load_dotenv
from app.routes.business_intelligence import get_sku_monthly_history
from app.utils.token_utils import get_effective_user_id_from_token
from app.utils.monthwise_ai_summary_utils import (
    run_prompt_2_strategy,
    build_sku_inventory_flags,
    fetch_inventory_aged_by_user,
    get_or_create_summary,
    get_or_create_global_summary,
    build_remaining_skus_time_series, 
)
from app.models.user_models import UserObjective

load_dotenv()

SECRET_KEY = Config.SECRET_KEY
db_url = os.getenv('DATABASE_URL')
db_url1 = os.getenv('DATABASE_ADMIN_URL')
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

client = OpenAI(api_key=OPENAI_API_KEY)
skuwise_bp = Blueprint('skuwise_bp', __name__)

# Create engines once only
user_engine = create_engine(
    db_url,
    pool_pre_ping=True,
    pool_recycle=1800,
    pool_size=1,
    max_overflow=1,
    pool_timeout=30,
)

admin_engine = create_engine(
    db_url1,
    pool_pre_ping=True,
    pool_recycle=1800,
    pool_size=1,
    max_overflow=0,
    pool_timeout=30,
)

def encode_file_to_base64(file_path):
    with open(file_path, 'rb') as file:
        return base64.b64encode(file.read()).decode('utf-8')


def get_countries_for_currency(currency):
    currency = currency.lower()

    if currency == "usd":
        return ["uk_usd", "us", "global"]
    elif currency == "inr":
        return ["uk", "us", "global_inr"]
    elif currency == "gbp":
        return ["uk", "us", "global_gbp"]
    elif currency == "cad":
        return ["uk", "us", "global_cad"]

    return ["uk", "us", "global"]


def get_conversion_rate(conn1, source_currency, target_currency, month, year):
    try:
        query = text("""
            SELECT conversion_rate
            FROM currency_conversion
            WHERE LOWER(user_currency) = :source
              AND LOWER(selected_currency) = :target
              AND LOWER(month) = :month
              AND year = :year
            LIMIT 1
        """)
        result = conn1.execute(query, {
            "source": source_currency.lower(),
            "target": target_currency.lower(),
            "month": month.lower(),
            "year": year
        }).fetchone()

        return float(result[0]) if result else 1.0
    except Exception:
        return 1.0


country_currency_map = {
    "uk": "gbp",
    "us": "usd",
    "global": None
}


def normalize_sku_country(country):
    country = (country or "").lower()

    if country.startswith("uk"):
        return "uk"

    if country.startswith("us"):
        return "us"

    return country


def resolve_product_sku_for_country(conn, user_id, product_name, country):
    """
    ProductwisePerformance receives product_name,
    but Other SKUs aggregation must exclude by SKU.
    """
    sku_country = normalize_sku_country(country)

    if sku_country not in ("uk", "us"):
        return None

    table_name = f"sku_{user_id}_data_table"
    sku_column = "sku_uk" if sku_country == "uk" else "sku_us"

    query = text(f"""
        SELECT {sku_column}
        FROM "{table_name}"
        WHERE LOWER(TRIM(product_name)) = LOWER(TRIM(:product_name))
        LIMIT 1
    """)

    row = conn.execute(query, {"product_name": product_name}).fetchone()

    return row[0] if row and row[0] else None

def get_monthly_total_net_sales_and_profit(conn, inspector, all_tables, user_id, country, month_name, year):
    """
    Returns total account net_sales and profit for a country/month/year.
    Used to calculate Other SKUs sales_mix and profit_mix.
    """
    country = normalize_sku_country(country)

    if country not in ("uk", "us"):
        return 0.0, 0.0

    table_pattern = f"skuwisemonthly_{user_id}_{country}_{month_name}{year}"

    matching_tables = [
        table for table in all_tables
        if table.lower() == table_pattern.lower()
    ]

    if not matching_tables:
        return 0.0, 0.0

    table_name = matching_tables[0]

    try:
        columns = [col["name"] for col in inspector.get_columns(table_name)]

        if "net_sales" not in columns or "profit" not in columns:
            return 0.0, 0.0

        # Prefer Total row if available
        if "product_name" in columns:
            total_query = text(f"""
                SELECT net_sales, profit
                FROM "{table_name}"
                WHERE LOWER(TRIM(product_name)) = 'total'
                LIMIT 1
            """)

            total_row = conn.execute(total_query).fetchone()

            if total_row:
                return float(total_row[0] or 0), float(total_row[1] or 0)

        # Fallback: sum all non-total rows
        query = text(f"""
            SELECT
                COALESCE(SUM(net_sales), 0) AS total_net_sales,
                COALESCE(SUM(profit), 0) AS total_profit
            FROM "{table_name}"
            WHERE LOWER(TRIM(COALESCE(product_name, ''))) != 'total'
        """)

        row = conn.execute(query).fetchone()

        return float(row[0] or 0), float(row[1] or 0)

    except Exception as e:
        print(f"Error calculating monthly total for {table_name}: {str(e)}")
        return 0.0, 0.0

def get_exact_other_skus_month_row(
    conn,
    inspector,
    all_tables,
    user_id,
    country,
    month_name,
    month_num,
    year,
    product_names,
    home_currency,
):
    """
    Aggregate only the exact product names shown in Other SKUs chips.
    This fixes mismatch where graph was using all SKUs except one anchor SKU.
    """
    sku_country = normalize_sku_country(country)

    if sku_country not in ("uk", "us"):
        return None

    if not product_names:
        return None

    table_pattern = f"skuwisemonthly_{user_id}_{sku_country}_{month_name}{year}"

    matching_tables = [
        table for table in all_tables
        if table.lower() == table_pattern.lower()
    ]

    if not matching_tables:
        return {
            "month": month_name.capitalize(),
            "month_num": month_num,
            "year": int(year),
            "net_sales": 0,
            "quantity": 0,
            "profit": 0,
            "asp": 0,
            "sales_mix": 0,
            "profit_mix": 0,
            "unit_wise_profitability": 0,
        }

    table_name = matching_tables[0]

    try:
        columns = [col["name"] for col in inspector.get_columns(table_name)]

        required_cols = {
            "product_name",
            "net_sales",
            "total_quantity",
            "profit",
            "asp",
        }

        if not required_cols.issubset(columns):
            return None

        placeholders = ", ".join([f":p{i}" for i in range(len(product_names))])

        params = {
            f"p{i}": product_names[i].strip().lower()
            for i in range(len(product_names))
        }

        query = text(f"""
            SELECT
                COALESCE(SUM(net_sales), 0) AS net_sales,
                COALESCE(SUM(total_quantity), 0) AS total_quantity,
                COALESCE(SUM(profit), 0) AS profit
            FROM "{table_name}"
            WHERE LOWER(TRIM(product_name)) IN ({placeholders})
        """)

        row = conn.execute(query, params).fetchone()

        other_net_sales = float(row[0] or 0)
        other_quantity = float(row[1] or 0)
        other_profit = float(row[2] or 0)

        # Currency conversion for UK/US if needed
        source_currency = country_currency_map.get(sku_country)
        target_currency = (home_currency or "").lower()

        conversion_rate = 1.0

        if source_currency and target_currency:
            with admin_engine.connect() as conn1:
                conversion_rate = get_conversion_rate(
                    conn1,
                    source_currency,
                    target_currency,
                    month_name,
                    year
                )

        other_net_sales *= conversion_rate
        other_profit *= conversion_rate

        asp = other_net_sales / other_quantity if other_quantity else 0
        unit_wise_profitability = other_profit / other_quantity if other_quantity else 0

        total_net_sales, total_profit = get_monthly_total_net_sales_and_profit(
            conn=conn,
            inspector=inspector,
            all_tables=all_tables,
            user_id=user_id,
            country=sku_country,
            month_name=month_name,
            year=year
        )

        total_net_sales *= conversion_rate
        total_profit *= conversion_rate

        sales_mix = (
            (other_net_sales / total_net_sales) * 100
            if total_net_sales
            else 0
        )

        profit_mix = (
            (other_profit / total_profit) * 100
            if total_profit
            else 0
        )

        return {
            "month": month_name.capitalize(),
            "month_num": month_num,
            "year": int(year),
            "net_sales": other_net_sales,
            "quantity": other_quantity,
            "profit": other_profit,
            "asp": asp,
            "sales_mix": sales_mix,
            "profit_mix": profit_mix,
            "unit_wise_profitability": unit_wise_profitability,
        }

    except Exception as e:
        print(f"Exact Other SKUs aggregate error for {table_name}: {str(e)}")
        return None

@skuwise_bp.route('/ProductwisePerformance', methods=['POST'])
def productwise_performance():
    try:
        # Authentication
        auth_header = request.headers.get('Authorization')
        if not auth_header or not auth_header.startswith('Bearer '):
            return jsonify({'error': 'Authorization token missing or invalid'}), 401

        token = auth_header.split(' ')[1]
        try:
            payload, user_id, member_id = get_effective_user_id_from_token(token)
            user_id = str(payload.get('user_id'))
        except jwt.ExpiredSignatureError:
            return jsonify({'error': 'Token expired'}), 401
        except jwt.InvalidTokenError:
            return jsonify({'error': 'Invalid token'}), 401

        # Request data
        data = request.get_json() or {}
        product_name = data.get('product_name')
        time_range = data.get('time_range', 'Yearly')
        year = data.get('year', datetime.now().year)
        quarter = data.get('quarter')
        home_currency = (data.get('home_currency') or 'USD').lower()

        # ✅ NEW: exact products shown under Other SKUs chips
        other_sku_product_names = data.get("other_sku_product_names") or []
        other_sku_product_names = [
            str(p).strip()
            for p in other_sku_product_names
            if str(p).strip()
        ]

        requested_countries = get_countries_for_currency(home_currency)

        if not product_name:
            return jsonify({'error': 'Product name is required'}), 400

        # Month definitions
        month_mapping = {
            'january': '01', 'february': '02', 'march': '03',
            'april': '04', 'may': '05', 'june': '06',
            'july': '07', 'august': '08', 'september': '09',
            'october': '10', 'november': '11', 'december': '12'
        }

        quarter_months = {
            '1': ['january', 'february', 'march'],
            '2': ['april', 'may', 'june'],
            '3': ['july', 'august', 'september'],
            '4': ['october', 'november', 'december']
        }

        if time_range == 'Quarterly' and quarter:
            months_to_fetch = quarter_months.get(str(quarter), [])
        else:
            months_to_fetch = list(month_mapping.keys())

        result_data = {}
        other_skus_graph_data = {}

        with user_engine.connect() as conn:
            inspector = inspect(conn)
            all_tables = inspector.get_table_names()

            # Iterate over requested countries
            for country in requested_countries:
                country_data = []

                for month in months_to_fetch:
                    month_num = month_mapping[month]

                    if country.lower().startswith("global"):
                        table_patterns = [
                            f"skuwisemonthly_{user_id}_{country}_{month}{year}_table"
                        ]
                    else:
                        table_patterns = [
                            f"skuwisemonthly_{user_id}_{country}_{month}{year}"
                        ]

                    total_sales = 0.0
                    total_quantity = 0
                    total_profit = 0.0
                    total_asp = 0.0
                    total_sales_mix = 0.0
                    total_profit_mix = 0.0
                    total_cost_of_unit_sold = 0.0

                    table_found = False
                    conversion_rate_applied = None

                    for table_pattern in table_patterns:
                        matching_tables = [
                            table for table in all_tables
                            if table.lower() == table_pattern.lower()
                        ]

                        if not matching_tables:
                            continue

                        table_found = True
                        table_name = matching_tables[0]

                        try:
                            columns = [
                                col['name'] for col in inspector.get_columns(table_name)
                            ]

                            required_cols = {
                                'product_name',
                                'net_sales',
                                'total_quantity',
                                'profit',
                                'asp',
                                'sales_mix',
                                'profit_mix',
                                'cost_of_unit_sold',
                            }

                            if not required_cols.issubset(columns):
                                print(f"Skipping table {table_name}: required columns missing")
                                continue

                            query = text(f"""
                                SELECT net_sales, total_quantity, profit, asp, sales_mix, profit_mix, cost_of_unit_sold
                                FROM "{table_name}"
                                WHERE LOWER(TRIM(product_name)) = LOWER(TRIM(:product_name))
                            """)

                            rows = conn.execute(
                                query, {'product_name': product_name}
                            ).fetchall()

                            table_sales = sum(float(row[0] or 0) for row in rows)
                            table_total_quantity = sum(int(row[1] or 0) for row in rows)
                            table_profit = sum(float(row[2] or 0) for row in rows)
                            table_asp = sum(float(row[3] or 0) for row in rows)
                            table_sales_mix = sum(float(row[4] or 0) for row in rows)
                            table_profit_mix = sum(float(row[5] or 0) for row in rows)
                            table_cost_of_unit_sold = sum(float(row[6] or 0) for row in rows)

                            if country.lower() in ('uk', 'us'):
                                source_currency = country_currency_map.get(country.lower())
                                target_currency = home_currency.lower()

                                if source_currency and target_currency:
                                    with admin_engine.connect() as conn1:
                                        conversion_rate = get_conversion_rate(
                                            conn1,
                                            source_currency,
                                            target_currency,
                                            month,
                                            year
                                        )
                                else:
                                    conversion_rate = 1.0

                                table_sales *= conversion_rate
                                table_profit *= conversion_rate
                                table_asp *= conversion_rate
                                table_cost_of_unit_sold *= conversion_rate

                                conversion_rate_applied = conversion_rate
                            else:
                                conversion_rate_applied = 1.0

                            total_sales += table_sales
                            total_quantity += table_total_quantity
                            total_profit += table_profit
                            total_asp += table_asp
                            total_sales_mix += table_sales_mix
                            total_profit_mix += table_profit_mix
                            total_cost_of_unit_sold += table_cost_of_unit_sold

                        except Exception as e:
                            print(f"Error querying table {table_name}: {str(e)}")
                            continue

                    gross_margin = (
                        (total_profit / total_sales) * 100 if total_sales > 0 else 0.0
                    )

                    country_data.append({
                        'month': month.capitalize(),
                        'month_num': month_num,
                        'net_sales': total_sales if table_found else 0.0,
                        'quantity': total_quantity if table_found else 0,
                        'profit': total_profit if table_found else 0.0,
                        'asp': total_asp if table_found else 0.0,
                        'sales_mix': total_sales_mix if table_found else 0.0,
                        'profit_mix': total_profit_mix if table_found else 0.0,
                        'gross_margin': gross_margin,
                        'year': year,
                        'conversion_rate_applied': conversion_rate_applied,
                    })

                country_data.sort(key=lambda x: x['month_num'])
                result_data[country] = country_data

                # =====================================================
                # Other SKUs graph data
                # Uses monthwise_ai_summary_utils aggregate series
                # =====================================================
                try:
                    sku_country = normalize_sku_country(country)

                    # ✅ BEST PATH:
                    # If frontend sends exact Other SKUs chip names,
                    # aggregate those products directly.
                    if other_sku_product_names and sku_country in ("uk", "us"):
                        exact_other_rows = []

                        for month in months_to_fetch:
                            month_num = month_mapping[month]

                            row = get_exact_other_skus_month_row(
                                conn=conn,
                                inspector=inspector,
                                all_tables=all_tables,
                                user_id=user_id,
                                country=sku_country,
                                month_name=month,
                                month_num=month_num,
                                year=year,
                                product_names=other_sku_product_names,
                                home_currency=home_currency,
                            )

                            if row:
                                exact_other_rows.append(row)

                        exact_other_rows.sort(key=lambda x: x["month_num"])
                        other_skus_graph_data[country] = exact_other_rows
                        continue

                    # fallback old behavior
                    selected_sku = resolve_product_sku_for_country(
                        conn=conn,
                        user_id=user_id,
                        product_name=product_name,
                        country=sku_country
                    )

                    if selected_sku and sku_country in ("uk", "us"):
                        if time_range == "Quarterly" and quarter and months_to_fetch:
                            anchor_month = int(month_mapping[months_to_fetch[-1]])
                        else:
                            anchor_month = 12

                        other_series = build_remaining_skus_time_series(
                            user_id=int(user_id),
                            country=sku_country,
                            focus_skus=[str(selected_sku)],
                            anchor_year=int(year),
                            anchor_month=anchor_month,
                            months=24
                        )

                        allowed_month_nums = {
                            month_mapping[m] for m in months_to_fetch
                        }

                        other_rows = []

                        for item in other_series:
                            item_year = int(item["year"])
                            item_month_num = str(item["month"]).zfill(2)

                            if item_year != int(year):
                                continue

                            if item_month_num not in allowed_month_nums:
                                continue

                            month_name = datetime(
                                item_year,
                                int(item["month"]),
                                1
                            ).strftime("%B").lower()

                            other_net_sales = float(item.get("net_sales") or 0)
                            other_profit = float(item.get("cm1_profit") or 0)

                            total_net_sales, total_profit = get_monthly_total_net_sales_and_profit(
                                conn=conn,
                                inspector=inspector,
                                all_tables=all_tables,
                                user_id=user_id,
                                country=sku_country,
                                month_name=month_name,
                                year=item_year
                            )

                            sales_mix = (
                                (other_net_sales / total_net_sales) * 100
                                if total_net_sales
                                else 0
                            )

                            profit_mix = (
                                (other_profit / total_profit) * 100
                                if total_profit
                                else 0
                            )

                            other_rows.append({
                                "month": datetime(
                                    item_year,
                                    int(item["month"]),
                                    1
                                ).strftime("%B"),
                                "month_num": item_month_num,
                                "year": item_year,

                                "net_sales": other_net_sales,
                                "quantity": item.get("units") or 0,
                                "profit": other_profit,
                                "asp": item.get("asp") or 0,

                                # ✅ calculated here
                                "sales_mix": sales_mix,
                                "profit_mix": profit_mix,

                                "unit_wise_profitability": item.get("unit_wise_profitability") or 0,
                            })

                        other_skus_graph_data[country] = other_rows
                    else:
                        other_skus_graph_data[country] = []

                except Exception as e:
                    print(f"Other SKUs graph error for {country}: {str(e)}")
                    other_skus_graph_data[country] = []

        return jsonify({
            'success': True,
            'product_name': product_name,
            'time_range': time_range,
            'year': year,
            'quarter': quarter if time_range == 'Quarterly' else None,

            # Selected product graph data
            'data': result_data,

            # Other SKUs aggregate graph data
            'other_skus_graph_data': other_skus_graph_data,

            'available_countries': list(result_data.keys())
        })

    except Exception as e:
        return jsonify({'error': f'Internal server error: {str(e)}'}), 500


@skuwise_bp.route('/Product_search', methods=['GET'])
def product_search():
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

    search_query = request.args.get('query', '').strip()
    if not search_query:
        return jsonify({'error': 'Search query is required'}), 400

    try:
        table_name = f"sku_{user_id}_data_table"

        with user_engine.connect() as conn:
            inspector = inspect(conn)

            if not inspector.has_table(table_name):
                return jsonify({'error': 'No data found for this user.'}), 404

            query = text(f"""
                SELECT DISTINCT product_name
                FROM "{table_name}"
                WHERE LOWER(product_name) LIKE LOWER(:search_query)
                ORDER BY product_name
                LIMIT 10
            """)

            results = conn.execute(
                query, {'search_query': f'%{search_query}%'}
            ).fetchall()

        products = [{'product_name': row[0]} for row in results]
        return jsonify({'products': products}), 200

    except Exception as e:
        return jsonify({'error': f'Error searching products: {str(e)}'}), 500


@skuwise_bp.route('/Product_names', methods=['GET'])
def product_names():
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
        table_name = f"sku_{user_id}_data_table"

        with user_engine.connect() as conn:
            inspector = inspect(conn)

            if not inspector.has_table(table_name):
                return jsonify({'error': 'No data table found for this user.'}), 404

            query = text(f"""
                SELECT DISTINCT product_name
                FROM "{table_name}"
                ORDER BY product_name ASC
            """)

            rows = conn.execute(query).fetchall()

        product_list = [row[0] for row in rows]
        return jsonify({'product_names': product_list}), 200

    except Exception as e:
        return jsonify({'error': str(e)}), 500
    
    


############################################################################################################################################
##DJ's NEW CODE FOR GROWTH + AI INSIGHTS Donot change anything here unless discussed with DJ##

# ===== Growth + AI Insights (NEW PART) =====

CURRENT_MONTH = datetime.now().month
CURRENT_YEAR = datetime.now().year

def month_key(month, year):
    return f"{month[:3].lower()}_{year}"


def calculate_growth(new, old):
    if old == 0:
        return 0.0
    return round(((new - old) / old) * 100, 2)


def get_latest_two_tables(all_tables, user_id, country):
    candidates = []
    now = datetime.now()

    for table in all_tables:
        prefix = f"skuwisemonthly_{user_id}_{country}_"
        if not table.lower().startswith(prefix.lower()):
            continue

        try:
            suffix = table.replace(prefix, "").replace("_table", "")
            month = ''.join(filter(str.isalpha, suffix))
            year = int(''.join(filter(str.isdigit, suffix)))
            month_num = datetime.strptime(month.capitalize(), "%B").month

            # ❌ EXCLUDE CURRENT MONTH (MTD)
            if year == now.year and month_num == now.month:
                continue

            candidates.append({
                "table": table,
                "month": month,
                "year": year,
                "month_num": month_num
            })
        except:
            continue

    # Sort by year + month descending
    candidates.sort(key=lambda x: (x["year"], x["month_num"]), reverse=True)

    return candidates[:2]



def fetch_metrics(conn, table_name, product_name=None, sku=None):
    where_clause = ""
    params = {}

    if product_name:
        where_clause = "LOWER(TRIM(product_name)) = LOWER(TRIM(:product_name))"
        params["product_name"] = product_name
    else:
        where_clause = "sku = :sku"
        params["sku"] = sku

    query = text(f"""
        SELECT
            quantity,
            net_sales,
            profit,
            asp
        FROM "{table_name}"
        WHERE {where_clause}
        LIMIT 1
    """)

    row = conn.execute(query, params).fetchone()

    if not row:
        return {
            "quantity": 0.0,
            "asp": 0.0,
            "net_sales": 0.0,
            "sales_mix": 0.0,
            "unit_wise_profitability": 0.0,
            "profit": 0.0
        }

    quantity = float(row.quantity or 0)
    net_sales = float(row.net_sales or 0)
    profit = float(row.profit or 0)
    asp = float(row.asp or 0)

    unit_wise_profitability = profit / quantity if quantity else 0
    sales_mix = net_sales  # kept explicit for AI prompt

    return {
        "quantity": quantity,
        "asp": asp,
        "net_sales": net_sales,
        "sales_mix": sales_mix,
        "unit_wise_profitability": unit_wise_profitability,
        "profit": profit
    }


def generate_ai_insights(prompt):
    try:
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": "You are a senior ecommerce analyst. Use plain text and bullet points only. Do not use Markdown formatting."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.2,
            max_tokens=900
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        print("OpenAI Error:", str(e))
        return None


@skuwise_bp.route('/ProductwiseGrowthAI', methods=['POST'])
def productwise_growth_ai():
    try:

        # ==============================
        # AUTH
        # ==============================
        auth_header = request.headers.get('Authorization')
        if not auth_header or not auth_header.startswith('Bearer '):
            return jsonify({'error': 'Unauthorized'}), 401

        token = auth_header.split(' ')[1]
        payload, user_id, member_id = get_effective_user_id_from_token(token)
        user_id = str(payload['user_id'])

        data = request.get_json()

        product_name = data.get('product_name')
        sku = data.get('sku')
        country = data.get('country', 'us')

        country = (country or "us").strip().lower()

        if country.startswith("uk"):
            country = "uk"
        elif country.startswith("us"):
            country = "us"
        elif country.startswith("global"):
            country = "global"

        if not product_name and not sku:
            return jsonify({'error': 'product_name or sku required'}), 400


        # ==============================
        # LOAD OBJECTIVE
        # ==============================
        user_objective_row = UserObjective.query.filter_by(
            user_id=int(user_id),
            country=country
        ).first()

        if user_objective_row:
            objective_v2 = {
                "growth_intent": user_objective_row.growth_intent,
                "profit_priority": user_objective_row.profit_priority,
                "inventory_clearance_priority": user_objective_row.inventory_clearance_priority,
                "business_context": user_objective_row.business_context,
                "country": country,
                "time_horizon": "1_month"
            }
        else:
            objective_v2 = {
                "growth_intent": "balanced",
                "profit_priority": "protect_growth",
                "inventory_clearance_priority": False,
                "business_context": None,
                "country": country,
                "time_horizon": "1_month"
            }


        # ==============================
        # RESOLVE SKU
        # ==============================

        if country == "global":
            key = product_name
        else:

            if not sku:
                table_name = f"sku_{user_id}_data_table"

                if country == "uk":
                    sku_column = "sku_uk"
                elif country == "us":
                    sku_column = "sku_us"
                else:
                    return jsonify({'error': 'Unsupported country'}), 400

                resolve_query = text(f"""
                    SELECT {sku_column}
                    FROM {table_name}
                    WHERE LOWER(TRIM(product_name)) = LOWER(TRIM(:product_name))
                    LIMIT 1
                """)

                with user_engine.connect() as conn:
                    result = conn.execute(
                        resolve_query,
                        {"product_name": product_name}
                    ).fetchone()

                if not result:
                    return jsonify({'error': 'SKU not found'}), 404

                key = result[0]

            else:
                key = sku


        # ==============================
        # GET SUMMARY ENGINE OUTPUT
        # ==============================
        country_actions = {}

        if country == "global":
            # ✅ GLOBAL FLOW:
            # Use the new global architecture.
            # Do not pass target_sku because global works product-wise through mapped US/UK journeys.
            summary_result = get_or_create_global_summary(
                user_id=int(user_id),
                marketplace_id=None,
                period=None,
                timeline=None,
                year=None,
                objective=objective_v2,
                target_sku=None,
                force_regenerate=False
            )

            global_ai = summary_result.get("global_ai") or {}
            product_items = global_ai.get("product_journey_comparison") or []

            matched_product = None

            def norm(v):
                return str(v or "").strip().lower()

            for product_item in product_items:
                if not isinstance(product_item, dict):
                    continue

                item_product_name = product_item.get("product_name")

                if norm(item_product_name) == norm(product_name):
                    matched_product = product_item
                    break

            if matched_product:
                product_journey = matched_product.get("journey_comparison") or []
                country_actions = matched_product.get("country_actions") or {}

                us_actions = country_actions.get("us") or {}
                uk_actions = country_actions.get("uk") or {}

                us_rec = us_actions.get("recommendation")
                uk_rec = uk_actions.get("recommendation")

                us_inv = us_actions.get("inventory_recommendation")
                uk_inv = uk_actions.get("inventory_recommendation")

                recommendation_parts = []
                if us_rec:
                    recommendation_parts.append(f"US: {us_rec}")
                if uk_rec:
                    recommendation_parts.append(f"UK: {uk_rec}")

                inventory_parts = []
                if us_inv:
                    inventory_parts.append(f"US: {us_inv}")
                if uk_inv:
                    inventory_parts.append(f"UK: {uk_inv}")

                recommendation = " | ".join(recommendation_parts) if recommendation_parts else None
                inventory_recommendation = " | ".join(inventory_parts) if inventory_parts else None

            else:
                product_journey = []
                recommendation = None
                inventory_recommendation = None
                country_actions = {}

        else:
            # ✅ COUNTRY FLOW: existing SKU-wise behavior
            summary_result = get_or_create_summary(
                user_id=int(user_id),
                country=country,
                marketplace_id=None,
                period=None,
                timeline=None,
                year=None,
                objective=None,
                target_sku=key,
                force_regenerate=True
            )

            sku_actions = summary_result.get("sku_actions") or {}
            sku_block = sku_actions.get(key) or {}

            product_journey = sku_block.get("journey_summary") or []
            recommendation = sku_block.get("recommendation")
            inventory_recommendation = sku_block.get("inventory_recommendation")


        # ==============================
        # SKU INVENTORY FLAGS
        # ==============================
        sku_inventory_flags = {}

        if country in ("uk", "us"):

            inventory_aged_df = fetch_inventory_aged_by_user(int(user_id), country=country)

            if inventory_aged_df is not None and not inventory_aged_df.empty:

                all_flags = build_sku_inventory_flags(
                    inventory_aged_df,
                    user_id=int(user_id),
                    country=country
                )

                if key in all_flags:
                    sku_inventory_flags = all_flags[key]


        # ==============================
        # GET 24M HISTORY (for chart)
        # ==============================
        history_24m = []

        try:
            with user_engine.connect() as conn:
                inspector = inspect(conn)
                all_tables = inspector.get_table_names()

            latest_tables = get_latest_two_tables(all_tables, user_id, country)

            if latest_tables:

                latest_tbl = latest_tables[0]
                end_year = latest_tbl["year"]
                end_month_num = latest_tbl["month_num"]

                history_24m = get_sku_monthly_history(
                    user_id,
                    country,
                    key,
                    end_year,
                    end_month_num,
                    24
                )

        except:
            history_24m = []


        # ==============================
        # RESPONSE
        # ==============================
        return jsonify({
            "success": True,
            "product_name": product_name,
            "historical_trend": history_24m,

            # 🔥 SAME JOURNEY AS AI ANALYST
            "product_journey": product_journey,

            # STRATEGY ENGINE OUTPUT
            "recommendation": recommendation,
            "inventory_recommendation": inventory_recommendation,

            # ✅ For global, contains separate US/UK recommendation, inventory, ads actions
            "country_actions": country_actions,

            # INVENTORY FLAG
            "sku_inventory_alert": sku_inventory_flags,

            "objective": objective_v2
        })

    except Exception as e:
        print("ProductwiseGrowthAI ERROR:", str(e))
        return jsonify({'error': str(e)}), 500

