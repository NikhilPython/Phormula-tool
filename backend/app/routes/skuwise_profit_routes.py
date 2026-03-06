
from datetime import datetime
from flask import Blueprint, request, jsonify
import jwt
from sqlalchemy import create_engine, text
from sqlalchemy import inspect
import os
import base64
from config import Config
import json
from openai import OpenAI
from sqlalchemy import text
SECRET_KEY = Config.SECRET_KEY
from dotenv import load_dotenv
from app.routes.business_intelligence import get_sku_monthly_history
from app.utils.token_utils import get_effective_user_id_from_token
from app.utils.monthwise_ai_summary_utils import run_prompt_2_strategy, build_sku_inventory_flags, fetch_inventory_aged_by_user, get_or_create_summary
from app.models.user_models  import UserObjective

load_dotenv()
db_url = os.getenv('DATABASE_URL')
db_url1= os.getenv('DATABASE_ADMIN_URL')
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
client = OpenAI(api_key=OPENAI_API_KEY)
skuwise_bp = Blueprint('skuwise_bp', __name__)

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

    # default fallback
    return ["uk", "us", "global"]


def get_conversion_rate(conn1,source_currency, target_currency, month, year):
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
    except:
        return 1.0


country_currency_map = {
    "uk": "gbp",
    "us": "usd",
    "global": None   # global stays as it is (optional)
}


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
        data = request.get_json()
        product_name = data.get('product_name')
        time_range = data.get('time_range', 'Yearly')
        year = data.get('year', datetime.now().year)
        quarter = data.get('quarter')

        home_currency = (data.get('home_currency') or 'USD').lower()

        requested_countries = get_countries_for_currency(home_currency)

        if not product_name:
            return jsonify({'error': 'Product name is required'}), 400

        # DB connections
        engine = create_engine(db_url)
        engine1 = create_engine(db_url1)
        conn = engine.connect()
        conn1 = engine1.connect()
        inspector = inspect(engine)
        all_tables = inspector.get_table_names()

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

        # Iterate over requested countries
        for country in requested_countries:

            country_data = []

            for month in months_to_fetch:
                month_num = month_mapping[month]

                # global / global_gbp / global_inr ... all have *_table suffix
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
                total_cost_of_unit_sold = 0.0
                table_found = False
                conversion_rate_applied = None

                for i, table_pattern in enumerate(table_patterns):
                    matching_tables = [
                        table for table in all_tables
                        if table.lower() == table_pattern.lower()
                    ]

                    if not matching_tables:
                        print(f"    ✗ No table found for: {table_pattern}")
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
                            'quantity',
                            'profit',
                            'asp',
                            'cost_of_unit_sold',
                        }
                        if not required_cols.issubset(columns):
                            print(f"    Skipping table {table_name}: required columns missing")
                            continue

                        query = text(f"""
                            SELECT net_sales, quantity, profit, asp, cost_of_unit_sold
                            FROM "{table_name}"
                            WHERE LOWER(TRIM(product_name)) = LOWER(TRIM(:product_name))
                        """)

                        rows = conn.execute(
                            query, {'product_name': product_name}
                        ).fetchall()

                        table_sales = sum(float(row[0] or 0) for row in rows)
                        table_quantity = sum(int(row[1] or 0) for row in rows)
                        table_profit = sum(float(row[2] or 0) for row in rows)
                        table_asp = sum(float(row[3] or 0) for row in rows)
                        table_cost_of_unit_sold = sum(
                            float(row[4] or 0) for row in rows
                        )
                        if country.lower() in ('uk', 'us'):
                            source_currency = country_currency_map.get(country.lower())
                            target_currency = home_currency.lower()

                            if source_currency and target_currency:
                                conversion_rate = get_conversion_rate(
                                    conn1,
                                    source_currency,
                                    target_currency,
                                    month,
                                    year
                                )
                            else:
                                conversion_rate = 1.0

                            # Apply conversion only for UK / US
                            table_sales *= conversion_rate
                            table_profit *= conversion_rate
                            table_asp *= conversion_rate
                            table_cost_of_unit_sold *= conversion_rate

                            conversion_rate_applied = conversion_rate
                        else:
                            conversion_rate_applied = 1.0
                           
                        total_sales += table_sales
                        total_quantity += table_quantity
                        total_profit += table_profit
                        total_asp += table_asp
                        total_cost_of_unit_sold += table_cost_of_unit_sold
                    except Exception as e:
                        conn.rollback()
                        print(f"Error querying table {table_name}: {str(e)}")

                gross_margin = (
                    (total_profit / total_sales) * 100 if total_sales > 0 else 0.0
                )

                country_data.append({
                    'month': month.capitalize(),
                    'month_num': month_num,
                    'net_sales': total_sales if table_found else 0.0,
                    'quantity': total_quantity if table_found else 0,
                    'profit': total_profit if table_found else 0.0,
                    'gross_margin': gross_margin,
                    'year': year,
                    'conversion_rate_applied': conversion_rate_applied,
                })

            country_data.sort(key=lambda x: x['month_num'])
            result_data[country] = country_data

        conn.close()
        conn1.close()

        return jsonify({
            'success': True,
            'product_name': product_name,
            'time_range': time_range,
            'year': year,
            'quarter': quarter if time_range == 'Quarterly' else None,
            'data': result_data,
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
        db_url = os.getenv('DATABASE_URL')
        engine = create_engine(db_url)
        conn = engine.connect()

        table_name = f"sku_{user_id}_data_table"
        
        # Check if the table exists
        inspector = inspect(engine)
        if not inspector.has_table(table_name):
            return jsonify({'error': 'No data found for this user.'}), 404

        # Keep LIKE for search suggestions but add DISTINCT to avoid duplicates
        query = text(f"""
            SELECT DISTINCT product_name
            FROM {table_name}
            WHERE LOWER(product_name) LIKE LOWER(:search_query)
            ORDER BY product_name
            LIMIT 10
        """)
        
        results = conn.execute(query, {'search_query': f'%{search_query}%'}).fetchall()
        
        products = [{'product_name': row[0]} for row in results]

        conn.close()

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
        db_url = os.getenv('DATABASE_URL')
        engine = create_engine(db_url)
        conn = engine.connect()

        table_name = f"sku_{user_id}_data_table"

        # Check if the table exists
        inspector = inspect(engine)
        if not inspector.has_table(table_name):
            conn.close()
            return jsonify({'error': 'No data table found for this user.'}), 404

        # Fetch only product_name
        query = text(f"""
            SELECT DISTINCT product_name
            FROM {table_name}
            ORDER BY product_name ASC
        """)

        rows = conn.execute(query).fetchall()
        conn.close()

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



    
# @skuwise_bp.route('/ProductwiseGrowthAI', methods=['POST'])
# def productwise_growth_ai():
#     try:
#         auth_header = request.headers.get('Authorization')
#         if not auth_header or not auth_header.startswith('Bearer '):
#             return jsonify({'error': 'Unauthorized'}), 401

#         token = auth_header.split(' ')[1]
#         payload, user_id, member_id = get_effective_user_id_from_token(token)
#         user_id = str(payload['user_id'])

#         data = request.get_json()
#         product_name = data.get('product_name')
#         sku = data.get('sku')
#         country = data.get('country', 'us')

#         if not country:
#             country = "us"

#         country = country.strip().lower()

#         # Force exact base market names to match table naming
#         if country.startswith("uk"):
#             country = "uk"
#         elif country.startswith("us"):
#             country = "us"
#         elif country.startswith("global"):
#             country = "global"


#         if not product_name and not sku:
#             return jsonify({'error': 'product_name or sku required'}), 400
        
#         # ============================================================
#         # LOAD OBJECTIVE FROM DB (SAME AS MAIN SUMMARY CODE)
#         # ============================================================
#         user_objective_row = UserObjective.query.filter_by(
#             user_id=int(user_id),
#             country=country
#         ).first()

#         if user_objective_row:
#             objective_v2 = {
#                 "growth_intent": user_objective_row.growth_intent,
#                 "profit_priority": user_objective_row.profit_priority,
#                 "inventory_clearance_priority": user_objective_row.inventory_clearance_priority,
#                 "business_context": user_objective_row.business_context,
#                 "country": country,
#                 "time_horizon": "1_month"
#             }
#         else:
#             objective_v2 = {
#                 "growth_intent": "balanced",
#                 "profit_priority": "protect_growth",
#                 "inventory_clearance_priority": False,
#                 "business_context": None,
#                 "country": country,
#                 "time_horizon": "1_month"
#             }

#         engine = create_engine(db_url)
#         inspector = inspect(engine)
#         all_tables = inspector.get_table_names()

#         latest_tables = get_latest_two_tables(all_tables, user_id, country)
#         if not latest_tables:
#             return jsonify({'error': 'No historical data available'}), 404

#         latest_tbl = latest_tables[0]
#         end_year = latest_tbl["year"]
#         end_month_num = latest_tbl["month_num"]

#         # --- Resolve correct key based on country ---
#         if country == "global":
#             if not product_name:
#                 return jsonify({'error': 'product_name required for global'}), 400
#             key = product_name
#         else:
#             # For US/UK etc we MUST use SKU
#             if not sku:
#                 # Auto-resolve SKU from product_name
#                 table_name = f"sku_{user_id}_data_table"

#                 # Decide correct SKU column based on country
#                 if country == "uk":
#                     sku_column = "sku_uk"
#                 elif country == "us":
#                     sku_column = "sku_us"
#                 else:
#                     return jsonify({'error': 'Unsupported country for SKU resolution'}), 400

#                 resolve_query = text(f"""
#                     SELECT {sku_column}
#                     FROM {table_name}
#                     WHERE LOWER(TRIM(product_name)) = LOWER(TRIM(:product_name))
#                     LIMIT 1
#                 """)


#                 with engine.connect() as conn_resolve:
#                     result = conn_resolve.execute(
#                         resolve_query, {"product_name": product_name}
#                     ).fetchone()

#                 if not result:
#                     return jsonify({'error': 'SKU not found for this product'}), 404

#                 key = result[0]
#             else:
#                 key = sku

#         # ============================================================
#         # SKU-LEVEL INVENTORY FLAGS ONLY (NO PORTFOLIO TOTALS)
#         # ============================================================
#         sku_inventory_flags = {}

#         # Inventory flags only make sense when the key is a SKU (US/UK)
#         if country in ("uk", "us") and key:
#             inventory_aged_df = fetch_inventory_aged_by_user(int(user_id))

#             if inventory_aged_df is not None and not inventory_aged_df.empty:
#                 all_flags = build_sku_inventory_flags(
#                     inventory_aged_df,
#                     user_id=int(user_id),
#                     country=country
#                 )

#                 if key in all_flags:
#                     sku_inventory_flags = {key: all_flags[key]}

#         # ---- Pull full 24-month history ----
#         history_24m = get_sku_monthly_history(
#             user_id,
#             country,
#             key,
#             end_year,
#             end_month_num,
#             24
#         )

#         if len(history_24m) < 2:
#             return jsonify({'error': 'Not enough data for analysis'}), 404

#         # ---- Last 2 months from history ----
#         prev_month = history_24m[-2]
#         curr_month = history_24m[-1]

#         def pct_change(new, old):
#             if old == 0:
#                 return 0.0
#             return round(((new - old) / old) * 100, 2)

#         # ---- Compute unit-wise profitability ----
#         prev_unit_profit = prev_month["profit"] / prev_month["units"] if prev_month["units"] else 0
#         curr_unit_profit = curr_month["profit"] / curr_month["units"] if curr_month["units"] else 0

#         # ---- Build metrics structure EXACTLY like before ----
#         item = {
#             "product_name": product_name,
#             "months": {
#                 "previous": prev_month["period"],
#                 "current": curr_month["period"]
#             },

#             "quantity": {
#                 "quantity_prev": prev_month["units"],
#                 "quantity_curr": curr_month["units"],
#                 "quantity_growth_pct": pct_change(curr_month["units"], prev_month["units"])
#             },

#             "net_sales": {
#                 "net_sales_prev": prev_month["sales"],
#                 "net_sales_curr": curr_month["sales"],
#                 "net_sales_growth_pct": pct_change(curr_month["sales"], prev_month["sales"])
#             },

#             "profit": {
#                 "profit_prev": prev_month["profit"],
#                 "profit_curr": curr_month["profit"],
#                 "profit_growth_pct": pct_change(curr_month["profit"], prev_month["profit"])
#             },

#             "asp": {
#                 "asp_prev": prev_month["asp"],
#                 "asp_curr": curr_month["asp"],
#                 "asp_growth_pct": pct_change(curr_month["asp"], prev_month["asp"])
#             },

#             "unit_wise_profitability": {
#                 "unit_wise_profitability_prev": round(prev_unit_profit, 2),
#                 "unit_wise_profitability_curr": round(curr_unit_profit, 2),
#                 "unit_wise_profitability_growth_pct": pct_change(curr_unit_profit, prev_unit_profit)
#             },

#             "historical_trend": history_24m
#         }

#         # ---- USE YOUR SAME ORIGINAL PROMPT UNCHANGED ----
#         prompt = f"""
# You are a Senior Amazon Business Analyst performing a
# CAUSAL PERFORMANCE DIAGNOSIS for a single product.

# Product under analysis: "{product_name}"
# Marketplace: "{country}"

# You are given:
# - Final, pre-calculated monthly performance data
# - A rolling historical trend of up to 24 months
# - Month-over-month movement already computed

# Your responsibility is to identify:

# WHAT materially changed,
# WHY it changed,
# and WHAT business impact it created
# for "{product_name}".

# STRICT ANALYTICAL RULES:

# 1) MATERIALITY FIRST  
# - Ignore minor or normal fluctuations.  
# - Focus only on movements that are:
#   • extreme  
#   • trend-defining  
#   • profitability-impacting  
#   • abnormal versus history  

# 2) CAUSE → EFFECT DISCIPLINE  
# Every insight must clearly follow:

# Movement → Primary Driver → Business Impact

# Examples of valid causal logic:
# - ASP decline → unit growth → CM1 profit pressure  
# - Stable pricing → unit decline → demand weakness  
# - Unit growth with stable CM1/unit → healthy expansion  

# 3) LONG-TERM TREND INTERPRETATION  
# Using the historical_trend:

# - Classify the trajectory of "{product_name}" as:
#   • sustained growth  
#   • structural decline  
#   • volatility  
#   • flat/stagnant  

# - Identify **clear turning points** in the trend.

# 4) RECENT MOVEMENT DIAGNOSIS  
# For the latest month vs previous month:

# - State the **dominant commercial change**.
# - Explain the **single strongest driver**:
#   • pricing movement  
#   • unit movement  
#   • sales mix shift  
#   • per-unit profitability change  

# - Conclude with the **business quality impact**:
#   • profitability strengthened  
#   • margin pressure emerged  
#   • efficiency deteriorated  
#   • stable but weak growth  

# METRIC INTERPRETATION RULES (SKU LEVEL)

# - total_quantity represents net units sold after returns.
# - net_sales represents realised topline revenue.
# - asp represents realised selling price per unit.
# - profit represents CM1 profit.
# - unit_wise_profitability represents CM1 profit per unit.

# CM2 ATTRIBUTION CONSTRAINT:

# - CM2 profit movement can ONLY be driven by:
#   • advertising_total
#   • platform fees
#   • storage fees
#   • reimbursements

# - Do NOT attribute CM2 change to any other cost component.
# - If CM2 movement is unexplained by the allowed drivers,
#   do NOT infer additional causes.


# FORBIDDEN CONTENT (ABSOLUTE):

# - No recommendations  
# - No actions  
# - No strategy  
# - No future suggestions  
# - No operational or inventory commentary  

# OUTPUT FORMAT:

# - Plain text bullet points only  
# - Maximum 5 bullets  
# - Each bullet must reference "{product_name}" naturally  
# - Each bullet must follow **Movement → Driver → Impact** reasoning  
# - No headings, no markdown, no narrative paragraphs  

# Data:
# {json.dumps(item, indent=2)}
# """

#         ai_insights = generate_ai_insights(prompt)

#         # ============================================================
#         # REUSE MAIN SUMMARY ENGINE (NO DRIFT GUARANTEE)
#         # ============================================================

#         recommendation = None
#         inventory_recommendation = None

#         try:
#             summary_result = get_or_create_summary(
#                 user_id=int(user_id),
#                 country=country,
#                 marketplace_id=None,
#                 period=None,        # 🔥 LET ENGINE AUTO-RESOLVE LATEST
#                 timeline=None,
#                 year=None,
#                 objective=None,
#                 target_sku=key,
#                 force_regenerate=False
#             )

#             sku_actions = summary_result.get("sku_actions") or {}
#             sku_block = sku_actions.get(key) or {}

#             recommendation = sku_block.get("recommendation")
#             inventory_recommendation = sku_block.get("inventory_recommendation")

#         except Exception:
#             recommendation = None
#             inventory_recommendation = None

#         return jsonify({
#             "success": True,
#             "product_name": product_name,
#             "historical_trend": history_24m,
#             "ai_insights": ai_insights,

#             # SKU-only inventory intelligence
#             "sku_inventory_alert": sku_inventory_flags.get(key, {}) if isinstance(sku_inventory_flags, dict) else {},

#             # MUST match main summary strategy engine output
#             "recommendation": recommendation,
#             "inventory_recommendation": inventory_recommendation,

#             # objective for transparency
#             "objective": objective_v2
#         })

#     except Exception as e:
#         print("ProductwiseGrowthAI ERROR:", str(e))
#         return jsonify({'error': str(e)}), 500

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
        engine = create_engine(db_url)

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

                with engine.connect() as conn:
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

            inventory_aged_df = fetch_inventory_aged_by_user(int(user_id))

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

            engine = create_engine(db_url)
            inspector = inspect(engine)
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

            # INVENTORY FLAG
            "sku_inventory_alert": sku_inventory_flags,

            "objective": objective_v2
        })

    except Exception as e:
        print("ProductwiseGrowthAI ERROR:", str(e))
        return jsonify({'error': str(e)}), 500