from flask import Blueprint, request, jsonify, current_app
import jwt
import os
from openai import OpenAI
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from config import Config
from calendar import month_name 
import json
from sqlalchemy import text
from datetime import date
from calendar import month_name
from calendar import month_name, month_abbr
from concurrent.futures import ThreadPoolExecutor, as_completed
from openai import OpenAIError
from app.utils.monthwise_ai_summary_utils import get_or_create_summary, resolve_latest_available_month, get_or_create_global_summary
from app.utils.token_utils import get_effective_user_id_from_token
from app.models.user_models import UserObjective



# Load environment variables
load_dotenv()
SECRET_KEY = Config.SECRET_KEY
OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')
db_url = os.getenv('DATABASE_URL')
db_url2 = os.getenv("DATABASE_AMAZON_URL")
oa_client = OpenAI(api_key=OPENAI_API_KEY)

user_engine = create_engine(
    db_url,
    pool_pre_ping=True,
    pool_recycle=1800,
    pool_size=3,
    max_overflow=2,
    pool_timeout=30,
)

amazon_engine = create_engine(
    db_url2,
    pool_pre_ping=True,
    pool_recycle=1800,
    pool_size=3,
    max_overflow=2,
    pool_timeout=30,
)

business_intelligence_bp = Blueprint('business_intelligence_bp', __name__)

#########################################################################################
def month_name_to_num(month_str: str):
    """Convert 'january' -> 1 ... 'december' -> 12"""
    month_str = month_str.lower().strip()
    for i in range(1, 13):
        if month_name[i].lower() == month_str:
            return i
    return None

def last_n_months_set(end_year, end_month, n: int = 6):
    """
    Returns set like {'2025-07','2025-08',...} ending at end_year-end_month inclusive.
    Accepts year/month as int or str.
    """
    y, m = int(end_year), int(end_month)
    periods = set()
    for _ in range(n):
        periods.add(f"{y:04d}-{m:02d}")
        m -= 1
        if m == 0:
            m = 12
            y -= 1
    return periods


def get_tables_in_last_6_months(user_id: int, country_lower: str, end_year: int, end_month: int):
    """
    Finds actual table_names available in DB that fall inside last 6 months window.
    Returns list of table names.
    """
    if country_lower == 'global':
        pattern = f"skuwisemonthly_{user_id}_global_%"
    else:
        pattern = f"skuwisemonthly_{user_id}_{country_lower}_%"

    q = text("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_name LIKE :pattern
    """)

    last6 = last_n_months_set(end_year, end_month, 6)

    tables = []
    with user_engine.connect() as conn:
        res = conn.execute(q, {"pattern": pattern})
        for row in res:
            tname = row[0]
            p = parse_period_from_table_name(tname, user_id, country_lower)  # 'YYYY-MM'
            if p and p in last6:
                tables.append(tname)

    return tables


    
@business_intelligence_bp.route('/MonthsforBI', methods=['GET'])
def print_comparison_range():
    auth_header = request.headers.get('Authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        return jsonify({'error': 'Authorization token is missing or invalid'}), 401

    token = auth_header.split(' ')[1]

    try:
        payload, user_id, member_id = get_effective_user_id_from_token(token)
        month1 = request.args.get('month1')
        year1 = request.args.get('year1')
        month2 = request.args.get('month2')
        year2 = request.args.get('year2')
        country = request.args.get('countryName')

        if not all([month1, year1, month2, year2, country]):
            return jsonify({'error': 'Missing required query parameters'}), 400

        # Ensure month1/year1 is earlier than month2/year2
        m1, m2 = int(month1), int(month2)
        y1, y2 = int(year1), int(year2)
        if (y1 > y2) or (y1 == y2 and m1 > m2):
            month1, month2 = month2, month1
            year1, year2 = year2, year1

        def month_num_to_name(m):
            try:
                m_int = int(m)
                return month_name[m_int].lower() if 1 <= m_int <= 12 else None
            except Exception:
                return None

        def construct_table_name(user_id, country, month, year):
            month_str = month_num_to_name(month)
            if not month_str:
                raise ValueError("Invalid month")
            if country.lower() == "global":
                return f"skuwisemonthly_{user_id}_global_{month_str}{year}_table"
            else:
                return f"skuwisemonthly_{user_id}_{country.lower()}_{month_str}{year}"

        is_global = country.lower() == "global"
        key_column = 'product_name' if is_global else 'sku'

        # ✅ backend scanning key (sku for non-global; product_name for global)
        scan_key = 'sku' if not is_global else 'product_name'

        # Build table names
        try:
            table1 = construct_table_name(user_id, country, month1, year1)
            table2 = construct_table_name(user_id, country, month2, year2)
        except ValueError as ve:
            return jsonify({'error': str(ve)}), 400

        # Dynamic column list
        columns = ['asp', 'total_quantity', 'profit', 'sales_mix', 'product_name', 'net_sales', 'unit_wise_profitability', 'gross_sales','profit_percentage','rembursement_fee','advertising_total']
        if not is_global:
            columns.append('sku')
        cols_str = ', '.join(columns)

        def query_table_columns(table_name):
            query = text(f"SELECT {cols_str} FROM {table_name} LIMIT 100000")
            with user_engine.connect() as conn:
                result = conn.execute(query)
                keys = result.keys()
                rows = []
                for row in result:
                    row_dict = dict(zip(keys, row))
                    if row_dict.get('product_name') and str(row_dict['product_name']).strip().lower() == 'total':
                        continue
                    rows.append({col: row_dict.get(col) for col in columns})
                return rows
            
        
        def query_reimbursement_total(table_name):
            q = text(f"""
                SELECT rembursement_fee
                FROM {table_name}
                WHERE LOWER(TRIM(product_name)) = 'total'
                LIMIT 1
            """)
            with user_engine.connect() as conn:
                r = conn.execute(q).first()
                if not r:
                    return 0.0
                try:
                    return float(r[0] or 0)
                except Exception:
                    return 0.0
                
        def query_advertising_total(table_name):
            q = text(f"""
                SELECT advertising_total
                FROM {table_name}
                WHERE LOWER(TRIM(product_name)) = 'total'
                LIMIT 1
            """)
            with user_engine.connect() as conn:
                r = conn.execute(q).first()
                if not r:
                    return 0.0
                try:
                    return float(r[0] or 0)
                except Exception:
                    return 0.0
                

        def query_expense_total(table_name):
            q = text(f"""
                SELECT
                    ABS(COALESCE(net_taxes, 0)) +
                    ABS(COALESCE(fba_fees, 0)) +
                    ABS(COALESCE(selling_fees, 0)) +
                    ABS(COALESCE(cost_of_unit_sold, 0)) +
                    ABS(COALESCE(advertising_total, 0)) +
                    ABS(COALESCE(platform_fee, 0)) -
                    ABS(COALESCE(net_credits, 0)) AS expense
                FROM {table_name}
                WHERE LOWER(TRIM(product_name)) = 'total'
                LIMIT 1
            """)
            with user_engine.connect() as conn:
                r = conn.execute(q).first()
                if not r:
                    return 0.0
                try:
                    return float(r[0] or 0)
                except Exception:
                    return 0.0
        
        

        
            

        data1 = query_table_columns(table1)  # Month 1 (earlier)
        data2 = query_table_columns(table2)  # Month 2 (later)
        
        reimbursement_total_month1 = query_reimbursement_total(table1)
        reimbursement_total_month2 = query_reimbursement_total(table2)

        advertising_total_month1 = query_advertising_total(table1)
        advertising_total_month2 = query_advertising_total(table2)
        
        expense_total_month1 = query_expense_total(table1)
        expense_total_month2 = query_expense_total(table2)

        # ✅ query only keys safely (uses scan_key)
        def query_only_keys(table_name):
            if scan_key == "sku":
                cols = "sku, product_name"
            else:
                cols = "product_name"

            q = text(f"SELECT {cols} FROM {table_name} LIMIT 200000")
            keys_set = set()

            with user_engine.connect() as conn:
                result = conn.execute(q)
                rkeys = result.keys()
                for r in result:
                    row_dict = dict(zip(rkeys, r))
                    pn = row_dict.get("product_name")
                    if pn and str(pn).strip().lower() == "total":
                        continue

                    k = row_dict.get(scan_key)
                    if k is not None:
                        k = str(k).strip()
                        if k:
                            keys_set.add(k)

            return keys_set

        growth_field_mapping = {
            'total_quantity': 'Unit Growth',
            'asp': 'ASP Growth',
            'gross_sales':'Gross Sales Growth',
            'net_sales': 'Net Sales Growth',
            'sales_mix': 'Sales Mix Change',
            'unit_wise_profitability': 'Profit Per Unit',
            'profit': 'CM1 Profit Impact'
        }

        def categorize_growth(value):
            if value is None:
                return "No Data"
            if value >= 5:
                return "High Growth"
            elif value > 0.5:
                return "Low Growth"
            elif value < -0.5:
                return "Negative Growth"
            else:
                return "No Growth"

        def safe_float(val, ndigits=2):
            try:
                if val is None:
                    return None
                f = float(val)
                return round(f, ndigits) if ndigits is not None else f
            except (ValueError, TypeError):
                return None


        def calculate_growth(data1, data2, key=scan_key, numeric_fields=None, non_growth_fields=None):
            if non_growth_fields is None:
                non_growth_fields = ["profit_percentage","rembursement_fee","advertising_total" ]  # ✅ add more raw-only fields here later if needed

            if numeric_fields is None:
                numeric_fields = list(growth_field_mapping.keys())  # ✅ only fields that have growth metrics

            data1_dict = {}
            for row in data1:
                k = row.get(key)
                if k is None:
                    continue
                k = str(k).strip()
                if not k:
                    continue
                data1_dict[k] = row

            growth_results = []

            for row2 in data2:
                item_key = row2.get(key)
                if item_key is None:
                    continue
                item_key = str(item_key).strip()
                if not item_key:
                    continue

                growth_row = {
                    'product_name': row2.get('product_name'),
                    key: item_key
                }

                if not is_global:
                    growth_row['sku'] = row2.get('sku')

                # Month2 sales mix (frontend fallback)
                sales_mix_val_month2 = (row2.get('sales_mix'))
                growth_row['Sales Mix (Month2)'] = sales_mix_val_month2

                # ✅ set month1 row (if exists)
                row1 = data1_dict.get(item_key)

                # ----------------------------
                # RAW ONLY (no growth metric)
                # ----------------------------
                for f in non_growth_fields:
                    growth_row[f"{f}_month2"] = safe_float(row2.get(f))
                    growth_row[f"{f}_month1"] = safe_float(row1.get(f)) if row1 else None

                # ----------------------------
                # Growth fields (with metrics)
                # ----------------------------
                for field in numeric_fields:
                    # raw values for calculation
                    val2_raw = safe_float(row2.get(field), ndigits=None)
                    val1_raw = safe_float(row1.get(field), ndigits=None) if row1 else None

                    # rounded values for output fields
                    val2 = safe_float(row2.get(field), ndigits=2)
                    val1 = safe_float(row1.get(field), ndigits=2) if row1 else None

                    growth_row[f"{field}_month2"] = val2
                    growth_row[f"{field}_month1"] = val1

                    if val1_raw is None or val2_raw is None:
                        growth = None
                    else:
                        if field == "sales_mix":
                            # percentage points
                            diff = val2_raw - val1_raw

                            # If diff would display as 0.00, force it to 0.0
                            growth = 0.0 if abs(diff) < 0.005 else round(diff, 2)

                        elif val1_raw == 0:
                            growth = 0.0
                        else:
                            growth = round(((val2_raw - val1_raw) / val1_raw) * 100, 2)

                    output_label = growth_field_mapping[field]
                    growth_row[output_label] = {
                        "category": categorize_growth(growth),
                        "value": growth
                    }
                # ----------------------------

                if row1 is None:
                    growth_row['new_or_reviving'] = True

                growth_results.append(growth_row)

            return growth_results



        growth_data = calculate_growth(data1, data2)

        # ✅ Total "summary" object (DO NOT append to rows; frontend already has total row UI)
        def build_total_row(rows, key_column, bucket_label="Total"):
            metrics = list(growth_field_mapping.values())
            categories = ["High Growth", "Low Growth", "Negative Growth", "No Growth", "No Data"]

            total = {
                "product_name": bucket_label,
                "is_total_row": True,
                "sku_count": len(rows)
            }

            if key_column != "product_name":
                total[key_column] = "TOTAL"
            else:
                total["total_key"] = "TOTAL"

            for metric in metrics:
                total[metric] = {c: 0 for c in categories}

            for r in rows:
                for metric in metrics:
                    cell = r.get(metric)
                    if isinstance(cell, dict):
                        cat = cell.get("category")
                        if cat in total[metric]:
                            total[metric][cat] += 1
                        else:
                            total[metric]["No Data"] += 1
                    else:
                        total[metric]["No Data"] += 1

            for metric in metrics:
                total[metric] = {"category": "Total Count", "value": total[metric]}

            return total

        # Label for Month 2 (later)
        month2_label = f"{month_abbr[int(month2)].capitalize()}'{str(year2)[-2:]}"

        # Identify SKU/Product groups
        country_lower = country.lower()

        def marketplace_id_for_country(country_lower):
            if country_lower == "uk":
                return "A1F83G8C2ARO7P"
            if country_lower == "us":
                return "ATVPDKIKX0DER"
            return None


        def get_new_skus_from_products_table(country_lower, sku_keys, end_year, end_month):
            """
            Uses products table from amazon DB to identify new SKUs.

            New SKU rule:
            - UK marketplace_id = A1F83G8C2ARO7P
            - US marketplace_id = ATVPDKIKX0DER
            - open_date must fall within the last 6 months ending at selected month2/year2
            - only checks SKUs present in month2
            """
            marketplace_id = marketplace_id_for_country(country_lower)

            if not marketplace_id or not sku_keys:
                return set()

            last6_periods_for_open_date = last_n_months_set(end_year, end_month, 6)

            q = text("""
                SELECT sku, open_date
                FROM products
                WHERE marketplace_id = :marketplace_id
                AND sku = ANY(:sku_list)
                AND open_date IS NOT NULL
            """)

            new_skus = set()

            with amazon_engine.connect() as conn:
                rows = conn.execute(q, {
                    "marketplace_id": marketplace_id,
                    "sku_list": list(sku_keys)
                }).fetchall()

            for row in rows:
                sku = row[0]
                open_date = row[1]

                if not sku or not open_date:
                    continue

                try:
                    # open_date may be datetime or string like "2025-05-23 12:51:52.722"
                    if hasattr(open_date, "strftime"):
                        open_period = open_date.strftime("%Y-%m")
                    else:
                        open_period = str(open_date).strip()[:7]

                    if open_period in last6_periods_for_open_date:
                        new_skus.add(str(sku).strip())

                except Exception as e:
                    print(f"Warning parsing open_date for sku={sku}: {e}")

            return new_skus

        last6_periods = last_n_months_set(int(year2), int(month2), 6)

        if country_lower == 'global':
            pattern = f"skuwisemonthly_{user_id}_global_%"
        else:
            pattern = f"skuwisemonthly_{user_id}_{country_lower}_%"

        q_tables = text("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
              AND table_name LIKE :pattern
        """)

        last6_tables = []
        with user_engine.connect() as conn:
            res = conn.execute(q_tables, {"pattern": pattern})
            for row in res:
                tname = row[0]
                p = parse_period_from_table_name(tname, user_id, country_lower)
                if p and p in last6_periods:
                    last6_tables.append(tname)


        # (optional / debug)
        historical_keys_last6m = set()
        for t in last6_tables:
            try:
                historical_keys_last6m |= query_only_keys(t)
            except Exception as e:
                print(f"Warning reading keys from {t}: {e}")

        month1_keys = {str(row.get(scan_key)).strip() for row in data1 if row.get(scan_key) is not None and str(row.get(scan_key)).strip()}
        month2_keys = {str(row.get(scan_key)).strip() for row in data2 if row.get(scan_key) is not None and str(row.get(scan_key)).strip()}

        # ✅ Reviving
        # ✅ New / Reviving split
        if country_lower == "global":
            # For global, keep current logic.
            reviving_keys = month2_keys - month1_keys

            def shift_month(year, month, offset):
                y, m = int(year), int(month)
                m += int(offset)

                while m <= 0:
                    m += 12
                    y -= 1

                while m > 12:
                    m -= 12
                    y += 1

                return y, m

            def table_period(tname):
                return parse_period_from_table_name(tname, user_id, country_lower) or "9999-99"

            last6_tables_sorted = sorted(last6_tables, key=table_period)

            baseline_keys = set()
            baseline_table = None
            baseline_period = None

            pre_y, pre_m = shift_month(year2, month2, -6)

            try:
                pre_table = construct_table_name(user_id, country, pre_m, pre_y)
            except Exception:
                pre_table = None

            pre_exists = False

            if pre_table:
                q_exists = text("""
                    SELECT 1
                    FROM information_schema.tables
                    WHERE table_schema='public' AND table_name=:t
                    LIMIT 1
                """)

                with user_engine.connect() as conn:
                    pre_exists = bool(conn.execute(q_exists, {"t": pre_table}).first())

            if pre_exists:
                baseline_table = pre_table
                baseline_period = f"{int(pre_y):04d}-{int(pre_m):02d}"

                try:
                    baseline_keys = query_only_keys(baseline_table)
                except Exception as e:
                    print(f"Warning reading keys from baseline pre-window table {baseline_table}: {e}")
                    baseline_keys = set()

            else:
                if last6_tables_sorted:
                    baseline_table = last6_tables_sorted[0]
                    baseline_period = table_period(baseline_table)

                    try:
                        baseline_keys = query_only_keys(baseline_table)
                    except Exception as e:
                        print(f"Warning reading keys from baseline oldest table {baseline_table}: {e}")
                        baseline_keys = set()

                    print(
                        f"[NEW-LAUNCH] Pre-window missing. "
                        f"Using oldest-available baseline: {baseline_period} "
                        f"table={baseline_table} keys={len(baseline_keys)}"
                    )

                else:
                    print("[NEW-LAUNCH] No last6 tables available; baseline empty.")
                    baseline_keys = set()

            new_sku_keys = (month2_keys - baseline_keys) if baseline_keys else set()

            # Mutual exclusivity:
            # if SKU is new, it should not appear in reviving.
            reviving_keys = reviving_keys - new_sku_keys

        else:
            # For US / UK, use products.open_date from amazon DB for new SKUs.
            new_sku_keys = get_new_skus_from_products_table(
                country_lower=country_lower,
                sku_keys=month2_keys,
                end_year=int(year2),
                end_month=int(month2)
            )

            # Current reviving logic remains same:
            # SKU exists in month2 but not in month1.
            # But remove SKUs already classified as new.
            reviving_keys = (month2_keys - month1_keys) - new_sku_keys


        new_skus_growth = [
            row for row in growth_data
            if (
                row.get(scan_key) is not None
                and str(row.get(scan_key)).strip() in new_sku_keys
            )
        ]

        reviving_skus_growth = [
            row for row in growth_data
            if (
                row.get(scan_key) is not None
                and str(row.get(scan_key)).strip() in reviving_keys
            )
        ]

        # Internal-only combined key set.
        # This is only for excluding new + reviving SKUs from Top 80% and Other SKUs.
        # Do not return this as JSON.
        new_reviving_keys = new_sku_keys | reviving_keys

        # ✅ DEDUPE: exclude New/Reviving SKUs from existing buckets
        existing_growth = [
            row for row in growth_data
            if (
                row.get(scan_key) in month1_keys
                and row.get("Sales Mix (Month2)") is not None
                and str(row.get(scan_key)).strip() not in new_reviving_keys
            )
        ]
        existing_growth_sorted = sorted(existing_growth, key=lambda x: x["Sales Mix (Month2)"], reverse=True)

        # ✅ Split existing into Top80/Other by Sales Mix cumulative
        total_sales_mix = sum(
            row["Sales Mix (Month2)"] for row in existing_growth_sorted
            if row.get("Sales Mix (Month2)") is not None
        )

        cumulative = 0.0
        top_80_skus, other_skus = [], []

        for row in existing_growth_sorted:
            mix = row.get("Sales Mix (Month2)")
            if mix is None:
                continue
            proportion = (cumulative / total_sales_mix) if total_sales_mix else 0
            if proportion <= 0.8:
                top_80_skus.append(row)
                cumulative += mix
            else:
                other_skus.append(row)

       
        # ✅ All SKUs tab: union of all four buckets
        # ✅ All SKUs tab: union of all four buckets
        all_skus = top_80_skus + new_skus_growth + reviving_skus_growth + other_skus

        # ✅ Totals returned separately
        top_80_total = build_total_row(
            top_80_skus,
            key_column,
            bucket_label="Total (Top 80%)"
        ) if top_80_skus else None

        new_skus_total = build_total_row(
            new_skus_growth,
            key_column,
            bucket_label="Total (New SKUs)"
        ) if new_skus_growth else None

        reviving_skus_total = build_total_row(
            reviving_skus_growth,
            key_column,
            bucket_label="Total (Reviving SKUs)"
        ) if reviving_skus_growth else None

        other_total = build_total_row(
            other_skus,
            key_column,
            bucket_label="Total (Other SKUs)"
        ) if other_skus else None

        all_skus_total = build_total_row(
            all_skus,
            key_column,
            bucket_label="Total (All SKUs)"
        ) if all_skus else None

        return jsonify({
            'message': 'Comparison range received',
            'comparison_range': {
                'from': {'month': month_num_to_name(month1), 'year': year1, 'table_name': table1},
                'to': {'month': month_num_to_name(month2), 'year': year2, 'table_name': table2},
                'month2_label': month2_label
            },
            'categorized_growth': {
                'top_80_skus': top_80_skus,
                'new_skus': new_skus_growth,
                'reviving_skus': reviving_skus_growth,
                'other_skus': other_skus,
                'all_skus': all_skus,

                # ✅ totals separate
                'top_80_total': top_80_total,
                'new_skus_total': new_skus_total,
                'reviving_skus_total': reviving_skus_total,
                'other_total': other_total,
                'all_skus_total': all_skus_total,
            },
            'reimbursement_totals': {
                'month1': round(reimbursement_total_month1, 2),
                'month2': round(reimbursement_total_month2, 2)
            },
            'advertising_totals': {
                'month1': round(advertising_total_month1, 2),
                'month2': round(advertising_total_month2, 2)
            },
            'expense_totals': {
                'month1': round(expense_total_month1, 2),
                'month2': round(expense_total_month2, 2)
            },
        }), 200

    except jwt.ExpiredSignatureError:
        return jsonify({'error': 'Token has expired'}), 401
    except jwt.InvalidTokenError:
        return jsonify({'error': 'Invalid token'}), 401
    except Exception as e:
        print("Unexpected error:", e)
        return jsonify({'error': 'Server error', 'details': str(e)}), 500

def get_sku_monthly_history(user_id, country_lower, sku_key, end_year, end_month, n=24):
    history = []
    periods = last_n_months_set(end_year, end_month, n)

    if country_lower == "global":
        pattern = f"skuwisemonthly_{user_id}_global_%"
        where_col = "product_name"
    else:
        pattern = f"skuwisemonthly_{user_id}_{country_lower}_%"
        where_col = "sku"

    q_tables = text("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema='public'
          AND table_name LIKE :pattern
    """)

    with user_engine.connect() as conn:
        tables = [r[0] for r in conn.execute(q_tables, {"pattern": pattern})]

    for t in tables:
        p = parse_period_from_table_name(t, user_id, country_lower)
        if not p or p not in periods:
            continue

        try:
            q = text(f"""
                SELECT total_quantity, net_sales, profit, asp
                FROM {t}
                WHERE {where_col} = :k
                LIMIT 1
            """)
            with user_engine.connect() as conn:
                r = conn.execute(q, {"k": sku_key}).first()

            if r:
                history.append({
                    "period": p,
                    "units": float(r.total_quantity or 0),
                    "sales": float(r.net_sales or 0),
                    "profit": float(r.profit or 0),
                    "asp": float(r.asp or 0),
                })

        except Exception as e:
            print("History read error:", e)

    return sorted(history, key=lambda x: x["period"])


def resolve_latest_available_month_for_bi(user_id: int, country_lower: str):
    """
    Country tables and global tables have different names.
    For global, latest table is:
        skuwisemonthly_{user_id}_global_{month}{year}_table

    For country, use existing resolve_latest_available_month.
    """

    country_lower = str(country_lower).lower().strip()

    if country_lower != "global":
        return resolve_latest_available_month(user_id, country_lower)

    pattern = f"skuwisemonthly_{user_id}_global_%"

    q = text("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_name LIKE :pattern
    """)

    latest_period = None

    with user_engine.connect() as conn:
        rows = conn.execute(q, {"pattern": pattern}).fetchall()

    for row in rows:
        table_name = row[0]
        period_key = parse_period_from_table_name(
            table_name,
            user_id,
            "global"
        )

        if not period_key:
            continue

        if latest_period is None or period_key > latest_period:
            latest_period = period_key

    if latest_period:
        y, m = latest_period.split("-")
        return int(y), int(m)

    # fallback: use US latest if no global table found
    return resolve_latest_available_month(user_id, "us")


@business_intelligence_bp.route('/analyze_skus', methods=['POST'])
def analyze_skus():
    auth_header = request.headers.get('Authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        return jsonify({'error': 'Authorization token is missing or invalid'}), 401

    token = auth_header.split(' ')[1]

    try:
        payload, user_id, member_id = get_effective_user_id_from_token(token)
        data = request.get_json()
        enriched_skus = data.get('skus', [])
        month1 = data.get('month1')
        month2 = data.get('month2')
        country = data.get('country', '')  # Get country for debugging

        if not enriched_skus or not isinstance(enriched_skus, list):
            return jsonify({'error': 'Invalid or missing SKU data'}), 400

        

        def generate_insight(item, app, objective_v2):

            sku = item.get('sku')
            product_name = item.get('product_name', 'this product').strip()

            is_global = country.lower() == 'global'

            # -------------------------------------------------
            # DEFINE KEY
            # -------------------------------------------------
            if is_global:
                # For global, product journey is product_name based
                key = product_name
            elif sku:
                key = sku
            else:
                key = product_name

            recommendation = None
            inventory_recommendation = None
            product_journey = []
            country_actions = {}

            try:

                # -------------------------------------------------
                # GET LATEST MONTH
                # -------------------------------------------------
                latest_year, latest_month = resolve_latest_available_month_for_bi(
                    int(user_id),
                    country.lower()
                )

                # -------------------------------------------------
                # CALL MAIN ENGINE
                # -------------------------------------------------
                with app.app_context():

                    if is_global:
                        # GLOBAL FLOW:
                        # Do NOT pass target_sku here.
                        # Global journey is created by mapped_product_journeys,
                        # then we pick the matching product from global_ai.
                        summary_result = get_or_create_global_summary(
                            user_id=int(user_id),
                            marketplace_id=None,
                            period="monthly",
                            timeline=str(latest_month),
                            year=int(latest_year),
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

                            # Keep old frontend fields working, but include both countries
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
                        # COUNTRY FLOW: existing behavior
                        summary_result = get_or_create_summary(
                            user_id=int(user_id),
                            country=country.lower(),
                            marketplace_id=None,
                            period="monthly",
                            timeline=str(latest_month),
                            year=int(latest_year),
                            objective=objective_v2,
                            target_sku=key,
                            force_regenerate=True
                        )

                        sku_actions = summary_result.get("sku_actions") or {}
                        sku_block = sku_actions.get(key) or {}

                        recommendation = sku_block.get("recommendation")
                        inventory_recommendation = sku_block.get("inventory_recommendation")

                        performance_journey = sku_block.get("journey_summary") or []
                        inventory_journey = sku_block.get("inventory_journey_summary") or []

                        product_journey = performance_journey + inventory_journey

            except Exception as e:
                print("Strategy engine failed:", e)

            return key, {
                "sku": sku,
                "product_name": product_name,
                "product_journey": product_journey,
                "recommendation": recommendation,
                "inventory_recommendation": inventory_recommendation,
                "country_actions": country_actions,
                "key_used": key,
                "is_global": is_global,
                "objective": objective_v2
            }

        insights = {}
        processed_count = 0
        error_count = 0

        app = current_app._get_current_object()

        # 🔥 ADD OBJECTIVE BUILD HERE
        with app.app_context():
            user_objective_row = UserObjective.query.filter_by(
                user_id=int(user_id),
                country=country.lower()
            ).first()

        if user_objective_row:
            objective_v2 = {
                "growth_intent": user_objective_row.growth_intent,
                "profit_priority": user_objective_row.profit_priority,
                "inventory_clearance_priority": user_objective_row.inventory_clearance_priority,
                "business_context": user_objective_row.business_context,
                "country": country.lower(),
                "time_horizon": "1_month"
            }
        else:
            objective_v2 = {
                "growth_intent": "balanced",
                "profit_priority": "protect_growth",
                "inventory_clearance_priority": False,
                "business_context": None,
                "country": country.lower(),
                "time_horizon": "1_month"
            }
        
        with ThreadPoolExecutor(max_workers=3) as executor:
            future_to_sku = {executor.submit(generate_insight, item, app, objective_v2): item
                for item in enriched_skus
                    }
            for future in as_completed(future_to_sku):
                try:
                    key, result = future.result()
                    insights[key] = result
                    processed_count += 1
                    
                except Exception as e:
                    error_count += 1
                    item = future_to_sku[future]
                    sku = item.get('sku', 'N/A')
                    product_name = item.get('product_name', 'N/A')
                     # ✅ PRINT REAL ERROR (CRITICAL)
                    print("THREAD ERROR → SKU:", sku, "| PRODUCT:", product_name)
                    print("ERROR MESSAGE:", str(e))


            # ✅ ADD DEBUG PRINTS HERE
        print("Insights generated:", len(insights))
        print("Errors:", error_count)

        return jsonify({'insights': insights}), 200

    except jwt.ExpiredSignatureError:
        return jsonify({'error': 'Token has expired'}), 401
    except jwt.InvalidTokenError:
        return jsonify({'error': 'Invalid token'}), 401
    except Exception as e:
        return jsonify({'error': 'Server error', 'details': str(e)}), 500

def parse_period_from_table_name(table_name, user_id, country_lower):
    """
    Table name patterns:
    - global:  skuwisemonthly_{user_id}_global_{month}{year}_table
    - country: skuwisemonthly_{user_id}_{country_lower}_{month}{year}
    Returns 'YYYY-MM' ya None.
    """
    global_prefix = f"skuwisemonthly_{user_id}_global_"
    country_prefix = f"skuwisemonthly_{user_id}_{country_lower}_"

    suffix = None
    if table_name.startswith(global_prefix):
        suffix = table_name[len(global_prefix):]
        if suffix.endswith("_table"):
            suffix = suffix[:-6]  # '_table' hata do
    elif table_name.startswith(country_prefix):
        suffix = table_name[len(country_prefix):]
    else:
        return None

    if len(suffix) < 6:
        return None

    year_str = suffix[-4:]
    month_str = suffix[:-4]  # e.g. 'january'

    try:
        year = int(year_str)
    except ValueError:
        return None

    month_num = None
    for i in range(1, 13):
        if month_name[i].lower() == month_str.lower():
            month_num = i
            break

    if not month_num:
        return None

    return f"{year:04d}-{month_num:02d}"


@business_intelligence_bp.route('/MonthsforBI/available-periods', methods=['GET'])
def get_available_periods_for_bi():
    auth_header = request.headers.get('Authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        return jsonify({'error': 'Authorization token is missing or invalid'}), 401

    token = auth_header.split(' ')[1]

    try:
        payload, user_id, member_id = get_effective_user_id_from_token(token)
        country = request.args.get('countryName')
        if not country:
            return jsonify({'error': 'Missing required query parameter: countryName'}), 400

        country_lower = country.lower()

        if country_lower == 'global':
            pattern = f"skuwisemonthly_{user_id}_global_%"
        else:
            pattern = f"skuwisemonthly_{user_id}_{country_lower}_%"

        query = text("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
              AND table_name LIKE :pattern
        """)

        with user_engine.connect() as conn:
            result = conn.execute(query, {'pattern': pattern})
            table_names = [row[0] for row in result]

        periods = set()
        for name in table_names:
            p = parse_period_from_table_name(name, user_id, country_lower)
            if p:
                periods.add(p)

        sorted_periods = sorted(periods)  # 'YYYY-MM'

        return jsonify({'periods': sorted_periods}), 200

    except jwt.ExpiredSignatureError:
        return jsonify({'error': 'Token has expired'}), 401
    except jwt.InvalidTokenError:
        return jsonify({'error': 'Invalid token'}), 401
    except Exception as e:
        return jsonify({'error': 'Server error', 'details': str(e)}), 500