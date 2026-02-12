
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
#UPLOAD_FOLDER = Config.UPLOAD_FOLDER
from dotenv import load_dotenv

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
            payload = jwt.decode(token, SECRET_KEY, algorithms=["HS256"])
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
        payload = jwt.decode(token, SECRET_KEY, algorithms=['HS256'])
        user_id = payload['user_id']
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
        payload = jwt.decode(token, SECRET_KEY, algorithms=['HS256'])
        user_id = payload['user_id']
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
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "You are a senior business analyst."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.2,
            max_tokens=700
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        print("OpenAI Error:", str(e))
        return None


@skuwise_bp.route('/ProductwiseGrowthAI', methods=['POST'])
def productwise_growth_ai():
    try:
        auth_header = request.headers.get('Authorization')
        if not auth_header or not auth_header.startswith('Bearer '):
            return jsonify({'error': 'Unauthorized'}), 401

        token = auth_header.split(' ')[1]
        payload = jwt.decode(token, SECRET_KEY, algorithms=["HS256"])
        user_id = str(payload['user_id'])

        data = request.get_json()
        product_name = data.get('product_name')
        sku = data.get('sku')
        country = data.get('country', 'us').lower()

        if not product_name and not sku:
            return jsonify({'error': 'product_name or sku required'}), 400

        engine = create_engine(db_url)
        conn = engine.connect()
        inspector = inspect(engine)

        all_tables = inspector.get_table_names()
        latest_tables = get_latest_two_tables(all_tables, user_id, country)

        if len(latest_tables) < 2:
            return jsonify({'error': 'Not enough historical data'}), 404

        old_tbl, new_tbl = latest_tables[1], latest_tables[0]

        old_data = fetch_metrics(conn, old_tbl["table"], product_name, sku)
        new_data = fetch_metrics(conn, new_tbl["table"], product_name, sku)

        metrics_payload = {}

        for metric in old_data.keys():
            metrics_payload[metric] = {
                f"{metric}_prev": round(old_data[metric], 2),
                f"{metric}_curr": round(new_data[metric], 2),
                f"{metric}_growth_pct": calculate_growth(
                    new_data[metric], old_data[metric]
                )
            }

        item = {
            "product_name": product_name,
            "months": {
                "previous": f"{old_tbl['month'].capitalize()} {old_tbl['year']}",
                "current": f"{new_tbl['month'].capitalize()} {new_tbl['year']}"
            },
            **metrics_payload
        }

        prompt = f"""
You are a senior business analyst. Based on the following 2-month comparison data of the product, generate insights in this format:



Observations:
- List the 2–3 most important changes using ONLY the given metrics:
  • quantity_prev vs quantity_curr
  • net_sales_prev vs net_sales_curr
  • profit_prev vs profit_curr
  • asp_prev vs asp_curr
  • unit_wise_profitability_prev vs unit_wise_profitability_curr
  • and % fields like "Unit Growth (%)", "Net Sales Growth (%)", etc.
- Use the exact causal tone wherever % values exist:
  "The increase/decrease in ASP by X% resulted in a dip/growth in units by Y%, which also resulted in sales falling/increasing by Z%."
- In at least one observation, mention Sales Mix Change (%) direction if present (up/down).
- Do NOT add assumptions like stock issues, supply constraints, replenishment, OOS, or fulfillment problems.

Improvements:
- Provide exactly 3–5 action bullets.
- Each action bullet MUST be exactly ONE sentence and MUST be chosen ONLY from the list below, verbatim (no edits):
  • "Check ads and visibility campaigns for this product."
  • "Review the visibility setup for this product."
  • "Reduce ASP slightly to improve traction."
  • "Increase ASP slightly to strengthen margins."
  • "Monitor performance closely and reassess next steps."
  • "Monitor performance closely for now."
- Do NOT add any other recommendations, explanations, or extra words.
- Do NOT mention stock, inventory, supply, operations, OOS, logistics, replenishment, or warehousing.
- Decision guidance:
  • If ASP is strongly up and units are down: prefer "Reduce ASP slightly to improve traction."
  • If units and sales are down and ASP is flat or slightly up: prefer visibility lines.
  • If profit/unit profit is very strong and units are stable/slightly down: prefer maintain/increase ASP.

Then, for each metric, add:

Unit Growth:
• [Explain reasons for the growth/decline using ONLY available signals like unit trend vs ASP trend and what that implies about demand/visibility/conversion.]
• [Choose ONE action bullet from the Improvements list that best fits the unit pattern and paste it verbatim.]

ASP:
• [Explain why ASP changed using ONLY available signals like pricing changes, discounting intensity, or product/pack/channel mix shifts (premium vs value) without referencing costs.]
• [Choose ONE action bullet from the Improvements list that best fits the ASP direction and paste it verbatim.]

Sales:
• [Describe sales trend by explicitly tying it to Units × ASP.]
• [Choose ONE action bullet from the Improvements list that best fits the sales pattern and paste it verbatim.]

Profit:
• [Explain profit change using ONLY available signals like sales movement plus realized pricing/discounting/mix impact.]
• [Choose ONE action bullet from the Improvements list that best aligns with protecting/improving profitability and paste it verbatim.]

Unit Profitability:
• [Explain per-unit profit change using ONLY available signals like realized price/discounting and mix impact.]
• [Choose ONE action bullet from the Improvements list that best fits per-unit profit trend and paste it verbatim.]

Instructions:
- Use plain text with bullets only.
- DO NOT use Markdown.
- Use % values and trends from the data.
- Make insights easy for business teams to act on.

Data:
{json.dumps(item, indent=2)}
"""

        ai_insights = generate_ai_insights(prompt)
        conn.close()

        return jsonify({
            "success": True,
            "product_name": product_name,
            "months_compared": [
                item["months"]["previous"],
                item["months"]["current"]
            ],
            "metrics": metrics_payload,
            "ai_insights": ai_insights
        })

    except Exception as e:
        return jsonify({'error': 'Internal server error'}), 500
    

    
