from flask import request, jsonify, send_file, Blueprint
import os
import psycopg2
import pandas as pd
import matplotlib.pyplot as plt
from app.utils.token_utils import get_effective_user_id_from_token
import matplotlib
matplotlib.use('Agg')  # Use non-interactive backend
import io
import base64
from datetime import datetime
import jwt
from config import Config
SECRET_KEY = Config.SECRET_KEY

db_url = os.getenv('DATABASE_URL')

pie_chart_bp = Blueprint('pie_chart_bp', __name__)

# Define quarters
QUARTER_MONTHS = {
    "quarter1": ["january", "february", "march"],
    "quarter2": ["april", "may", "june"],
    "quarter3": ["july", "august", "september"],
    "quarter4": ["october", "november", "december"]
}

# Quarter mapping for Q1, Q2, Q3, Q4 format
QUARTER_MAPPING = {
    "Q1": "quarter1",
    "Q2": "quarter2", 
    "Q3": "quarter3",
    "Q4": "quarter4"
}

def get_quarter_from_month(month):
    """Get quarter name from month"""
    if not month:
        return None
    
    month_lower = month.lower().strip()
    for quarter, months in QUARTER_MONTHS.items():
        if month_lower in months:
            return quarter
    return None


def get_quarter_from_quarter_param(quarter_param):
    """Convert Q1, Q2, Q3, Q4 to quarter1, quarter2, quarter3, quarter4"""
    if not quarter_param:
        return None
    
    quarter_upper = quarter_param.upper().strip()
    return QUARTER_MAPPING.get(quarter_upper)


def is_quarter_format(param):
    """Check if parameter is in Q1, Q2, Q3, Q4 format"""
    if not param:
        return False
    return param.upper().strip() in QUARTER_MAPPING


def get_db_connection():
    """Establish database connection"""
    try:
        conn = psycopg2.connect(db_url)
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return None


def generate_table_names(user_id, country, month=None, year=None, quarter=None, range_type=None):
    """Generate all possible table names based on the parameters"""
    tables = []
    
    # Handle quarterly range specifically
    if range_type == 'quarterly':
        if quarter and year:
            quarter_name = get_quarter_from_quarter_param(quarter)
            if quarter_name:
                quarterly_table = f"{quarter_name}_{user_id}_{country.lower()}_{year}_table"
                tables.append(quarterly_table)
                return tables
        elif month:
            # Check if month is actually a quarter (Q1, Q2, etc.)
            if is_quarter_format(month):
                quarter_name = get_quarter_from_quarter_param(month)
                if quarter_name and year:
                    quarterly_table = f"{quarter_name}_{user_id}_{country.lower()}_{year}_table"
                    tables.append(quarterly_table)
                    return tables
            else:
                # Regular month, get its quarter
                quarter_name = get_quarter_from_month(month)
                if quarter_name and year:
                    quarterly_table = f"{quarter_name}_{user_id}_{country.lower()}_{year}_table"
                    tables.append(quarterly_table)
                    return tables
    
    # Handle when month is provided
    if month and year and not is_quarter_format(month):
        # Monthly table (country-specific)
        monthly_table = f"skuwisemonthly_{user_id}_{country.lower()}_{month.lower()}{year}"
        tables.append(monthly_table)
        
        # Monthly table (global)
        global_monthly_table = f"skuwisemonthly_{user_id}_{country.lower()}_{month.lower()}{year}_table"
        tables.append(global_monthly_table)
        
        # Quarterly table from month
        quarter_name = get_quarter_from_month(month)
        if quarter_name:
            quarterly_table = f"{quarter_name}_{user_id}_{country.lower()}_{year}_table"
            tables.append(quarterly_table)
    
    # Handle when quarter is provided directly (Q1, Q2, etc.)
    elif month and is_quarter_format(month) and year:
        quarter_name = get_quarter_from_quarter_param(month)
        if quarter_name:
            quarterly_table = f"{quarter_name}_{user_id}_{country.lower()}_{year}_table"
            tables.append(quarterly_table)
    
    # Handle when quarter parameter is provided directly
    elif quarter and year:
        quarter_name = get_quarter_from_quarter_param(quarter)
        if quarter_name:
            quarterly_table = f"{quarter_name}_{user_id}_{country.lower()}_{year}_table"
            tables.append(quarterly_table)
    
    # Yearly table
    if year:
        yearly_table = f"skuwiseyearly_{user_id}_{country.lower()}_{year}_table"
        tables.append(yearly_table)
    
    return tables


def get_table_name_by_type(user_id, country, table_type, month=None, year=None, quarter=None):
    """Get specific table name based on table type"""
    
    if table_type == 'monthly' and month and year and not is_quarter_format(month):
        # Return both country-specific and global monthly tables
        tables = []
        # Country-specific monthly table
        country_monthly = f"skuwisemonthly_{user_id}_{country.lower()}_{month.lower()}{year}"
        tables.append(country_monthly)
        
        # Global monthly table
        global_monthly = f"skuwisemonthly_{user_id}_{country.lower()}_{month.lower()}{year}_table"
        tables.append(global_monthly)
        
        return tables
    
    elif table_type == 'yearly' and year:
        return [f"skuwiseyearly_{user_id}_{country.lower()}_{year}_table"]
    
    elif table_type == 'quarterly':
        # Handle quarterly by quarter parameter (Q1, Q2, Q3, Q4)
        if quarter and year:
            quarter_name = get_quarter_from_quarter_param(quarter)
            if quarter_name:
                return [f"{quarter_name}_{user_id}_{country.lower()}_{year}_table"]
        
        # Handle quarterly by month parameter (could be Q1, Q2, etc. or actual month)
        elif month and year:
            if is_quarter_format(month):
                quarter_name = get_quarter_from_quarter_param(month)
            else:
                quarter_name = get_quarter_from_month(month)
            
            if quarter_name:
                return [f"{quarter_name}_{user_id}_{country.lower()}_{year}_table"]
        
        return None  # Invalid parameters for quarterly
    
    return None  # Invalid parameters

def check_table_exists(cursor, table_name):
    """Check if table exists in database"""
    try:
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = %s
            );
        """, (table_name,))
        return cursor.fetchone()[0]
    except Exception as e:
        print(f"Error checking table existence: {e}")
        return False

def fetch_data_from_table(table_name):
    """Fetch product data from specified table"""
    conn = get_db_connection()
    if not conn:
        return None
    
    try:
        cursor = conn.cursor()
        
        # Check if table exists
        if not check_table_exists(cursor, table_name):
            return None
        
        # Fetch data - adjust column names as per your actual table structure
        query = f"""
            SELECT product_name, profit 
            FROM {table_name} 
            WHERE profit IS NOT NULL 
            AND product_name IS NOT NULL
            AND LOWER(product_name) != 'total'
            AND profit > 0
            ORDER BY profit DESC
        """
        
        cursor.execute(query)
        data = cursor.fetchall()
        
        if data:
            df = pd.DataFrame(data, columns=['product_name', 'profit'])
            return df
        else:
            return None
            
    except Exception as e:
        print(f"Error fetching data from {table_name}: {e}")
        return None
    finally:
        if conn:
            conn.close()

def prepare_pie_chart_data(df):
    """Prepare data for pie chart - top 5 products + others"""
    if df is None or df.empty:
        return None, None
    
    # Sort by profit in descending order
    df_sorted = df.sort_values('profit', ascending=False)
    
    # Get top 5 products
    top_5 = df_sorted.head(5)
    
    # Calculate sum of remaining products
    remaining = df_sorted.iloc[5:]
    
    # Prepare data for pie chart
    labels = top_5['product_name'].tolist()
    values = top_5['profit'].tolist()
    
    # Add "Others" if there are more than 5 products
    if len(remaining) > 0:
        others_sum = remaining['profit'].sum()
        if others_sum > 0:  # Only add if sum is positive
            labels.append('Others')
            values.append(others_sum)
    
    return labels, values

def create_pie_chart(labels, values, title="Top 5 Products by Profit"):
    """Create pie chart and return as base64 image"""
    if not labels or not values:
        return None
    
    # Create figure and axis
    plt.figure(figsize=(12, 8))
    
    # Define colors
    colors = ['#FF6B6B', '#4ECDC4', '#45B7D1', '#96CEB4', '#FFEAA7', '#DDA0DD']
    
    # Create pie chart
    wedges, texts, autotexts = plt.pie(
        values, 
        labels=labels, 
        autopct='%1.1f%%',
        startangle=90,
        colors=colors[:len(labels)],
        explode=[0.05] * len(labels)  # Slightly separate all slices
    )
    
    # Customize the chart
    plt.title(title, fontsize=16, fontweight='bold', pad=20)
    
    # Customize text
    for autotext in autotexts:
        autotext.set_color('white')
        autotext.set_fontweight('bold')
        autotext.set_fontsize(10)
    
    for text in texts:
        text.set_fontsize(9)
    
    # Add legend with values
    legend_labels = [f'{label}: ${value:,.2f}' for label, value in zip(labels, values)]
    plt.legend(wedges, legend_labels, title="Products", loc="center left", bbox_to_anchor=(1, 0, 0.5, 1))
    
    plt.tight_layout()
    
    # Save to bytes
    img_buffer = io.BytesIO()
    plt.savefig(img_buffer, format='png', dpi=300, bbox_inches='tight')
    img_buffer.seek(0)
    
    # Convert to base64
    img_base64 = base64.b64encode(img_buffer.getvalue()).decode('utf-8')
    
    plt.close()  # Close the figure to free memory
    
    return img_base64

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



def _prev_month_year(month: str, year: str) -> tuple[str, str]:
    # month can be "jan" / "January" / "12" / "2025-12" etc.
    # In your system month is usually text (like "december") OR numeric.
    m = str(month).strip()

    # normalize month to number 1..12
    if "-" in m:  # "2025-12"
        y_str, m_str = m.split("-", 1)
        year = year or y_str
        month_num = int(m_str)
    else:
        try:
            month_num = int(m)
        except ValueError:
            month_num = datetime.strptime(m[:3].title(), "%b").month  # "dec", "december"

    y = int(year)
    if month_num == 1:
        return "12", str(y - 1)
    return str(month_num - 1), str(y)


def _prev_quarter_year(quarter: str, year: str) -> tuple[str, str]:
    q = str(quarter).strip().upper()
    if q.startswith("Q"):
        qnum = int(q[1:])
    elif "-Q" in q:
        y_str, q_str = q.split("-Q", 1)
        year = year or y_str
        qnum = int(q_str)
    else:
        qnum = int(q)

    y = int(year)
    if qnum == 1:
        return "Q4", str(y - 1)
    return f"Q{qnum - 1}", str(y)


def _prev_year(year: str) -> str:
    return str(int(year) - 1)

def _fetch_best_table_for_mode(user_id, country, month=None, year=None, quarter=None, mode=None):
    """
    mode: 'monthly' | 'quarterly' | 'yearly'
    This prevents falling back to yearly when you want previous monthly/quarterly.
    """
    candidates = []

    if mode == "monthly":
        # Only monthly names (no quarterly, no yearly)
        if month and year and not is_quarter_format(month):
            candidates.append(f"skuwisemonthly_{user_id}_{country.lower()}_{str(month).lower()}{year}")
            candidates.append(f"skuwisemonthly_{user_id}_{country.lower()}_{str(month).lower()}{year}_table")

    elif mode == "quarterly":
        # Only quarterly name
        if quarter and year:
            qname = get_quarter_from_quarter_param(quarter)
            if qname:
                candidates.append(f"{qname}_{user_id}_{country.lower()}_{year}_table")
        elif month and year and is_quarter_format(month):
            qname = get_quarter_from_quarter_param(month)
            if qname:
                candidates.append(f"{qname}_{user_id}_{country.lower()}_{year}_table")
        elif month and year:
            qname = get_quarter_from_month(month)
            if qname:
                candidates.append(f"{qname}_{user_id}_{country.lower()}_{year}_table")

    elif mode == "yearly":
        if year:
            candidates.append(f"skuwiseyearly_{user_id}_{country.lower()}_{year}_table")

    # Try in order
    for t in candidates:
        df = fetch_data_from_table(t)
        if df is not None and not df.empty:
            return df, t, candidates

    return None, None, candidates

def _fetch_best_table_auto(user_id, country, month=None, year=None, quarter=None, range_type=None):
    """
    Auto mode used for CURRENT period.
    Tries monthly -> quarterly -> yearly (as your generate_table_names returns).
    """
    table_names = generate_table_names(user_id, country, month, year, quarter, range_type)

    for t in table_names:
        df = fetch_data_from_table(t)
        if df is not None and not df.empty:
            return df, t

    return None, None

def build_skuwise_table_name(user_id, country, month, year):
    month = str(month).strip().lower()
    year = str(year).strip()

    if country == "global":
        return f"skuwisemonthly_{user_id}_global_{month}{year}_table".lower()

    return f"skuwisemonthly_{user_id}_{country}_{month}{year}".lower()

@pie_chart_bp.route('/pie-chart', methods=['GET', 'POST'])
def generate_pie_chart():

    # ---------------- Auth ----------------
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
        body = request.get_json(silent=True) or {}

        country_param = (body.get('country') or request.args.get('country') or '').strip().lower()

        currency_param = None
        if country_param == "global":
            currency_param = (body.get('homeCurrency') or request.args.get('homeCurrency') or 'USD').strip().lower()

        country = resolve_country(country_param, currency_param)

        month = (body.get('month') if request.method == 'POST' else request.args.get('month')) or body.get('month') or request.args.get('month')
        year = (body.get('year') if request.method == 'POST' else request.args.get('year')) or body.get('year') or request.args.get('year')
        quarter = (body.get('quarter') if request.method == 'POST' else request.args.get('quarter')) or body.get('quarter') or request.args.get('quarter')
        range_type = (body.get('range') if request.method == 'POST' else request.args.get('range')) or body.get('range') or request.args.get('range')

        table_type = (body.get('table_type') or request.args.get('table_type') or 'auto')
        response_format = (body.get('format') or request.args.get('format') or 'json')

        # ✅ NEW flag: include previous values (default true)
        include_previous = (body.get('include_previous') or request.args.get('include_previous') or "true")
        include_previous = str(include_previous).lower() != "false"

        if not user_id or not country:
            return jsonify({'error': 'user_id and country are required parameters'}), 400

        if year:
            year = str(year)

        # ---------------- Fetch current data ----------------
        df = None
        used_table = None

        # ---------------- Fetch current data ----------------
        df = None
        used_table = None

        rt = (range_type or "").strip().lower()
        month_str = str(month or "").strip().lower()
        year_str = str(year or "").strip()

        is_monthly_request = (
            rt == "monthly"
            or (month_str and year_str and not quarter and not is_quarter_format(month_str))
        )

        if table_type == 'auto':
            # ✅ IMPORTANT:
            # If user explicitly passes month + year, do not use "best available" fallback.
            # Fetch only the exact monthly table for that month/year.
            if is_monthly_request:
                if not month_str or not year_str:
                    return jsonify({
                        "error": "month and year are required for monthly pie chart"
                    }), 400

                # If frontend sends month number like "4", convert to "april"
                if month_str.isdigit():
                    month_str = datetime(2000, int(month_str), 1).strftime("%B").lower()

                table_name = build_skuwise_table_name(user_id, country, month_str, year_str)

                df = fetch_data_from_table(table_name)

                if df is not None and not df.empty:
                    used_table = table_name
                else:
                    return jsonify({
                        "error": "No data available for selected month and year",
                        "table_checked": table_name,
                        "parameters": {
                            "user_id": user_id,
                            "country": country,
                            "month": month_str,
                            "year": year_str,
                            "range_type": range_type
                        }
                    }), 404

            else:
                # Keep existing auto behavior for quarterly/yearly
                df, used_table = _fetch_best_table_auto(
                    user_id,
                    country,
                    month,
                    year,
                    quarter,
                    range_type
                )

        else:
            table_names = get_table_name_by_type(
                user_id,
                country,
                table_type,
                month,
                year,
                quarter
            )

            if not table_names:
                return jsonify({'error': 'Invalid parameters for specified table type'}), 400

            for table_name in table_names:
                df = fetch_data_from_table(table_name)
                if df is not None and not df.empty:
                    used_table = table_name
                    break

        if df is None or df.empty:
            tables_checked = (
                generate_table_names(user_id, country, month, year, quarter, range_type)
                if table_type == 'auto'
                else get_table_name_by_type(user_id, country, table_type, month, year, quarter)
            )
            return jsonify({
                'error': 'No data found in any of the available tables',
                'tables_checked': tables_checked,
                'parameters': {
                    'user_id': user_id,
                    'country': country,
                    'month': month,
                    'year': year,
                    'quarter': quarter,
                    'range_type': range_type
                }
            }), 404

        # ---------------- Prepare CURRENT pie data ----------------
        labels, values = prepare_pie_chart_data(df)
        if not labels or not values:
            return jsonify({'error': 'No valid data available for pie chart'}), 404

                # ---------------- Prepare PREVIOUS pie data ----------------
        prev_labels, prev_values = [], []
        prev_table = None
        prev_period_meta = None
        prev_candidates = []

        if include_previous:
            rt = (range_type or "").strip().lower()

            # QUARTERLY previous
            if rt == "quarterly" or quarter or (month and is_quarter_format(month)):
                if not year:
                    return jsonify({'error': 'year is required for quarterly previous comparison'}), 400

                q_display = quarter if quarter else month
                prev_q, prev_y = _prev_quarter_year(q_display, year)
                prev_period_meta = {"type": "quarterly", "quarter": prev_q, "year": prev_y}

                prev_df, prev_table, prev_candidates = _fetch_best_table_for_mode(
                    user_id=user_id,
                    country=country,
                    quarter=prev_q,
                    year=prev_y,
                    mode="quarterly",
                )

            # YEARLY previous
            elif rt == "yearly" or (year and not month and not quarter):
                if not year:
                    return jsonify({'error': 'year is required for yearly previous comparison'}), 400

                prev_y = _prev_year(year)
                prev_period_meta = {"type": "yearly", "year": prev_y}

                prev_df, prev_table, prev_candidates = _fetch_best_table_for_mode(
                    user_id=user_id,
                    country=country,
                    year=prev_y,
                    mode="yearly",
                )

            # MONTHLY previous
            else:
                if month and year:
                    pm, py = _prev_month_year(month, year)
                    prev_period_meta = {"type": "monthly", "month": pm, "year": py}

                    # IMPORTANT: convert prev month number to month-name if your tables use month name
                    # Your monthly tables are like "..._{month.lower()}{year}"
                    # If you pass "11" it becomes "..._112025" which may not exist.
                    # If your schema uses names like "november2025", convert numeric month -> monthname.
                    pm_str = str(pm).strip()
                    if pm_str.isdigit():
                        pm_str = datetime(2000, int(pm_str), 1).strftime("%B").lower()


                    prev_df, prev_table, prev_candidates = _fetch_best_table_for_mode(
                        user_id=user_id,
                        country=country,
                        month=pm_str,
                        year=py,
                        mode="monthly",
                    )
                else:
                    prev_df = None

            if prev_df is not None and not prev_df.empty:
                prev_labels, prev_values = prepare_pie_chart_data(prev_df)


        # ---------------- Title ----------------
        title_parts = ["Top 5 Products by Profit"]

        if range_type == 'quarterly' or quarter:
            quarter_display = quarter if quarter else (month if is_quarter_format(month) else None)
            if quarter_display and year:
                title_parts.append(f"({quarter_display} {year})")
        elif month and not is_quarter_format(month):
            if year:
                title_parts.append(f"({str(month).title()} {year})")
            else:
                title_parts.append(f"({str(month).title()})")
        elif year:
            title_parts.append(f"({year})")

        if used_table and 'global' in used_table.lower():
            title_parts.append("- Global")
        elif country:
            title_parts.append(f"- {country.title()}")

        title = " ".join(title_parts)

        # ---------------- Generate chart ----------------
        chart_base64 = create_pie_chart(labels, values, title)
        if not chart_base64:
            return jsonify({'error': 'Failed to generate chart'}), 500

        # ---------------- OPTIONAL: product-by-product comparison ----------------
        # same_products: current top5 labels matched with previous values
        prev_map = {k: v for k, v in zip(prev_labels, prev_values)} if prev_labels else {}
        compare = []
        for k, v in zip(labels, values):
            compare.append({
                "product": k,
                "current_profit": v,
                "previous_profit": prev_map.get(k, 0)
            })

        # ---------------- Response ----------------
        response_data = {
            'success': True,
            'data': {
                'labels': labels,
                'values': values,
                'total_products': len(df),
                'top_5_count': min(5, len(df)),
                'others_count': max(0, len(df) - 5),
                'total_profit': sum(values),
                'table_used': used_table,
                'title': title,
                'is_global_data': ('global' in used_table.lower()) if used_table else False,

                # ✅ NEW previous block
                'previous': {
                    "available": bool(prev_labels),
                    "period": prev_period_meta,
                    "table_used": prev_table,
                    "labels": prev_labels,
                    "values": prev_values,
                    "total_profit": sum(prev_values) if prev_values else 0,
                },

                # ✅ NEW comparison block
                "compare_top5": compare,
            }
        }

        if response_format == 'image':
            response_data['data']['chart_image'] = f"data:image/png;base64,{chart_base64}"
        else:
            response_data['data']['chart_base64'] = chart_base64

        return jsonify(response_data), 200

    except Exception as e:
        return jsonify({'error': f'An error occurred: {str(e)}'}), 500


@pie_chart_bp.route('/pie-chart/image', methods=['GET', 'POST'])
def get_pie_chart_image():
    """
    Generate and return pie chart as image file
    """
    try:
        # Get chart data
        response = generate_pie_chart()
        
        if response[1] != 200:  # If there's an error
            return response
        
        # Extract base64 image from response
        response_data = response[0].get_json()
        chart_base64 = response_data['data']['chart_base64']
        
        # Convert base64 to bytes
        img_data = base64.b64decode(chart_base64)
        img_buffer = io.BytesIO(img_data)
        img_buffer.seek(0)
        
        return send_file(
            img_buffer,
            mimetype='image/png',
            as_attachment=True,
            download_name='pie_chart.png'
        )
        
    except Exception as e:
        return jsonify({
            'error': f'An error occurred: {str(e)}'
        }), 500
    

