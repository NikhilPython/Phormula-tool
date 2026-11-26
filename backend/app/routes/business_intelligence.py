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
from decimal import Decimal
from app.utils.monthwise_ai_summary_utils import get_or_create_summary, resolve_latest_available_month, get_or_create_global_summary
from app.utils.dashboard_card_metrics import (
    add_cashflow_summary_fields,
    build_cashflow_card_metrics,
    build_pnl_card_metrics,
    calculate_pnl_cost_of_advertisement,
    calculate_pnl_tacos,
    calculate_pnl_total_advertising_cost,
)
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

    if suffix.endswith("_table"):
        suffix = suffix[:-6]

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


def _shift_month(year, month, offset):
    month_index = int(year) * 12 + int(month) - 1 + int(offset)
    shifted_year, zero_based_month = divmod(month_index, 12)
    return shifted_year, zero_based_month + 1


def _rolling_month_periods(end_year, end_month, count):
    return [
        f"{year:04d}-{month:02d}"
        for year, month in (
            _shift_month(end_year, end_month, offset)
            for offset in range(1 - count, 1)
        )
    ]


def _aggregate_sku_month_tables(table_names, key_column):
    aggregates = {}
    all_columns = set()
    card_total_fields = {
        "total_quantity",
        "quantity", "units_sold", "unit_sold",
        "return_quantity", "return_units", "units_returned",
        "refund_sales", "sales_refund", "refund_amount",
        "net_taxes", "net_tax", "taxes_net",
        "net_credits", "net_credit", "credits_net",
        "taxncredit", "tax_and_credits", "misc_transaction",
        "product_spend", "display_spend", "brand_spend", "visible_ads",
        "dealsvouchar_ads", "dealsvoucher_ads", "dealvouchars_ads",
        "gross_sales",
        "net_sales",
        "profit",
        "cost_of_unit_sold",
        "platform_fee",
        "tex_and_credits",
        "rembursement_fee",
        "selling_fees",
        "fba_fees",
        "amazon_fee",
        "amazon_fees",
        "shipping_charges",
        "storage_fee",
        "inventory_charges_and_reimbursement",
        "fba_disposal",
        "lost_total",
        "current_net_reimbursement",
        "net_reimbursement",
        "net_reimbursement_total",
        "disbursement",
        "debt_payment",
        "platform_management_fees",
        "platform_managment_fees",
        "other_adjustment",
        "other_transaction_fees",
        "other_transactions",
        "advertising_total_final",
        "total_ads",
        "advertising_fees",
        "advertising_total",
        "ads_spend",
        "total_cm2_profit",
        "cm2_profit_total",
        "cm2_profit",
        "promotional_rebates",
    }
    card_totals = {}

    for period, table_name in table_names:
        query = text(f'SELECT * FROM public."{table_name}"')

        with user_engine.connect() as conn:
            result = conn.execute(query)
            all_columns.update(result.keys())
            for row in result.mappings():
                product_name = row.get("product_name")
                raw_key = row.get(key_column)
                is_total = (
                    product_name
                    and str(product_name).strip().lower() in {"total", "totals", "grand total"}
                ) or (
                    raw_key
                    and str(raw_key).strip().lower() in {"total", "totals", "grand_total"}
                )
                if is_total:
                    for column in card_total_fields:
                        value = row.get(column)
                        if isinstance(value, (int, float, Decimal)) and not isinstance(value, bool):
                            card_totals[column] = card_totals.get(column, 0.0) + float(value)
                    continue

                if raw_key is None:
                    continue
                key = str(raw_key).strip()
                if not key:
                    continue

                aggregate = aggregates.setdefault(
                    key,
                    {
                        key_column: key,
                        "_numeric_sums": {},
                        "_weighted_sums": {},
                        "_weights": {},
                        "_ad_types": set(),
                        "_periods": set(),
                    },
                )
                if product_name:
                    aggregate["product_name"] = product_name
                aggregate["_periods"].add(period)

                for column, value in row.items():
                    if value is None:
                        continue
                    if column == key_column:
                        aggregate[column] = key
                    elif column in {"id", "user_id"}:
                        aggregate[column] = value
                    elif column == "ad_type":
                        aggregate["_ad_types"].update(
                            item.strip()
                            for item in str(value).split(",")
                            if item.strip()
                        )
                    elif isinstance(value, (int, float, Decimal)) and not isinstance(value, bool):
                        numeric_value = float(value)
                        if _is_rolling_derived_column(column):
                            weight = (
                                abs(float(row.get("total_quantity") or 0))
                                if "per_unit" in column or column.endswith("_per")
                                else abs(float(row.get("net_sales") or 0))
                            )
                            if weight == 0:
                                weight = 1.0
                            aggregate["_weighted_sums"][column] = (
                                aggregate["_weighted_sums"].get(column, 0.0)
                                + numeric_value * weight
                            )
                            aggregate["_weights"][column] = (
                                aggregate["_weights"].get(column, 0.0) + weight
                            )
                        else:
                            aggregate["_numeric_sums"][column] = (
                                aggregate["_numeric_sums"].get(column, 0.0)
                                + numeric_value
                            )
                    else:
                        aggregate[column] = value

    output_rows = []
    for aggregate in aggregates.values():
        if aggregate["_ad_types"]:
            aggregate["ad_type"] = ", ".join(sorted(aggregate["_ad_types"]))

        row = {
            column: aggregate.get(column)
            for column in all_columns
        }
        row.update(aggregate["_numeric_sums"])

        for column, weighted_sum in aggregate["_weighted_sums"].items():
            weight = aggregate["_weights"][column]
            row[column] = weighted_sum / weight if weight else 0.0

        row["periods_with_data"] = sorted(aggregate["_periods"])

        total_quantity = float(row.get("total_quantity") or 0)
        _set_rolling_ratio(row, "asp", "net_sales", "total_quantity")
        _set_rolling_ratio(row, "unit_wise_profitability", "profit", "total_quantity")
        _set_rolling_ratio(row, "cm1_profit_per_unit", "profit", "total_quantity")
        _set_rolling_ratio(row, "cm1_profit_per", "profit", "net_sales", 100)
        _set_rolling_ratio(row, "profit_percentage", "profit", "net_sales", 100)
        _set_rolling_ratio(row, "cm2_profit_per_unit", "cm2_profit", "total_quantity")
        _set_rolling_ratio(row, "cm2_profit_per", "cm2_profit", "net_sales", 100)
        _set_rolling_ratio(row, "cm2_profit_percentage", "cm2_profit", "net_sales", 100)
        _set_rolling_ratio(row, "cm2_margins", "cm2_profit", "net_sales", 100)
        _set_rolling_ratio(row, "total_cm2_margins", "total_cm2_profit", "net_sales", 100)
        _set_rolling_ratio(row, "promotional_rebates_percentage", "promotional_rebates", "net_sales", 100)
        reimbursement = _rolling_number(
            row.get("rembursement_fee"),
            row.get("current_net_reimbursement"),
            row.get("net_reimbursement"),
            row.get("net_reimbursement_total"),
        )

        net_sales = _rolling_number(row.get("net_sales"))
        cm2_profit = _rolling_number(
            row.get("total_cm2_profit"),
            row.get("cm2_profit_total"),
            row.get("cm2_profit"),
        )
        row["reimbursement_vs_sales"] = (
            abs(reimbursement) / abs(net_sales) * 100 if net_sales else 0.0
        )
        reimbursement_vs_cm2 = (
            abs(reimbursement) / abs(cm2_profit) * 100 if cm2_profit else 0.0
        )
        row["rembursment_vs_cm2_margins"] = reimbursement_vs_cm2
        row["reimbursement_vs_cm2_margins"] = reimbursement_vs_cm2
        _set_rolling_ratio(row, "ads_conversion_rate", "ads_sale_units", "ads_clicks", 100)
        _set_rolling_ratio(row, "ads_roas", "ads_sale_amount", "ads_spend")
        _set_rolling_ratio(row, "acos", "ads_spend", "ads_sale_amount", 100)
        _set_rolling_ratio(row, "ads_acos", "ads_spend", "ads_sale_amount", 100)
        _set_rolling_ratio(row, "tacos_total_advertising_cost_of_sale", "total_ads", "net_sales", 100)
        _set_rolling_ratio(row, "marketplace_fees_per_unit", "marketplace_fees", "total_quantity")
        _set_rolling_ratio(row, "cost_of_ads_per_unit", "ads_spend", "total_quantity")
        _set_rolling_ratio(row, "gross_sales_per_unit", "gross_sales", "total_quantity")
        _set_rolling_ratio(row, "net_sales_per_unit", "net_sales", "total_quantity")
        _set_rolling_ratio(row, "net_reimbursement_per_unit", "current_net_reimbursement", "total_quantity")

        if "others_per_unit" in row:
            other_total = row.get("other_transaction_fees", row.get("other"))
            if other_total is not None:
                row["others_per_unit"] = (
                    float(other_total) / total_quantity if total_quantity else 0.0
                )

        for column in ("total_quantity", "net_sales", "profit", "gross_sales"):
            if column in row and row[column] is not None:
                row[column] = round(float(row[column]), 2)

        for column, value in row.items():
            if isinstance(value, (int, float, Decimal)) and not isinstance(value, bool):
                row[column] = round(float(value), 2)
        output_rows.append(row)

    total_sales = abs(sum(float(row.get("net_sales") or 0) for row in output_rows))
    total_profit = abs(sum(float(row.get("profit") or 0) for row in output_rows))
    for row in output_rows:
        net_sales = float(row.get("net_sales") or 0)
        profit = float(row.get("profit") or 0)
        if "sales_mix" in row:
            row["sales_mix"] = round(net_sales / total_sales * 100, 2) if total_sales else 0.0
        if "profit_mix" in row:
            row["profit_mix"] = round(profit / total_profit * 100, 2) if total_profit else 0.0

    return sorted(output_rows, key=lambda row: row.get("product_name") or ""), card_totals


def _is_rolling_derived_column(column):
    return (
        "percentage" in column
        or "_per" in column
        or column.endswith("_mix")
        or "margin" in column
        or column.endswith("_margins")
        or column == "reimbursement_vs_sales"
        or column in {"asp", "unit_wise_profitability", "acos", "ads_roas"}
    )


def _set_rolling_ratio(row, output_column, numerator_column, denominator_column, multiplier=1):
    if output_column not in row:
        return
    numerator = row.get(numerator_column)
    denominator = row.get(denominator_column)
    if numerator is not None and denominator is not None:
        denominator = float(denominator)
        row[output_column] = (
            float(numerator) / denominator * multiplier if denominator else 0.0
        )


def _rolling_number(*values):
    for value in values:
        if value is None:
            continue
        try:
            number = float(value)
        except (TypeError, ValueError):
            continue
        if number != 0:
            return number
    return 0.0


def _rolling_rows_have_cm2_data(rows):
    for row in rows:
        name = str(row.get("product_name") or "").strip().lower()
        sku = str(row.get("sku") or "").strip().lower()
        if name in {"total", "totals", "grand total", "others"} or sku in {
            "total",
            "totals",
            "grand_total",
            "others",
        }:
            continue

        ads_spend = _rolling_number(
            row.get("ads_spend"),
            row.get("advertising_total"),
            row.get("advertising_total_final"),
        )
        acos = _rolling_number(row.get("acos"))
        net_sales = _rolling_number(row.get("net_sales"))
        if not acos and net_sales:
            acos = ads_spend / net_sales * 100

        cm1_profit = _rolling_number(row.get("profit"))
        cm2_profit = _rolling_number(
            row.get("cm2_profit"),
            row.get("cm2_profit_total"),
            row.get("total_cm2_profit"),
        )
        if ads_spend != 0 or acos != 0 or abs(cm2_profit - cm1_profit) > 0.01:
            return True

    return False


def _apply_rolling_promotions_placement(rows, placement):
    for row in rows:
        adjustment = abs(_rolling_number(row.get("promotional_rebates")))
        net_sales = _rolling_number(row.get("net_sales")) + (
            adjustment if placement != "in_net_sales" else 0
        )
        profit = _rolling_number(row.get("profit"), row.get("cm1_profit"), row.get("total_profit"))
        if placement == "in_ads_spend":
            profit += adjustment
            ads = _rolling_number(row.get("ads_spend"), row.get("advertising_total"), row.get("advertising_total_final")) + adjustment
            row["ads_spend"] = ads
            for key in (
                "advertising_total",
                "advertising_total_final",
                "advertising_fees",
                "total_ads",
            ):
                if key in row:
                    row[key] = _rolling_number(row.get(key)) + adjustment
            row["acos"] = round(ads / net_sales * 100, 2) if net_sales else 0
        quantity = _rolling_number(row.get("total_quantity"))

        row["net_sales"] = round(net_sales, 2)
        row["profit"] = round(profit, 2)
        row["asp"] = round(net_sales / quantity, 2) if quantity else 0
        row["profit_percentage"] = (
            round(profit / net_sales * 100, 2) if net_sales else 0
        )
        row["unit_wise_profitability"] = (
            round(profit / quantity, 2) if quantity else 0
        )
        row["promotional_rebates_percentage"] = (
            round(adjustment / net_sales * 100, 2) if net_sales else 0
        )
        row["cost_of_advertisement"] = calculate_pnl_cost_of_advertisement(row)
        row["total_advertising_cost"] = calculate_pnl_total_advertising_cost(row)
        tacos = calculate_pnl_tacos(row)
        row["tacos_total_advertising_cost_of_sale"] = tacos
        row["tacos"] = tacos

        if "Net_Sales" in row:
            row["Net_Sales"] = row["net_sales"]
        if "Profit" in row:
            row["Profit"] = row["profit"]
        if "total_profit" in row:
            row["total_profit"] = row["profit"]
        if "cm1_profit" in row:
            row["cm1_profit"] = row["profit"]
        if "cm1_profit_per_unit" in row:
            row["cm1_profit_per_unit"] = row["unit_wise_profitability"]
        if "cm1_profit_per" in row:
            row["cm1_profit_per"] = row["profit_percentage"]
        if "cm1_profit_percentage" in row:
            row["cm1_profit_percentage"] = row["profit_percentage"]

        ads_spend = _rolling_number(
            row.get("ads_spend"),
            row.get("advertising_total"),
            row.get("advertising_total_final"),
        )
        if "acos" in row:
            row["acos"] = round(ads_spend / net_sales * 100, 2) if net_sales else 0
        cm2_profit = _rolling_number(
            row.get("total_cm2_profit"),
            row.get("cm2_profit_total"),
            row.get("cm2_profit"),
        )
        for field in ("cm2_margins", "cm2_profit_percentage", "cm2_profit_per"):
            if field in row:
                row[field] = round(cm2_profit / net_sales * 100, 2) if net_sales else 0
        if "total_cm2_margins" in row:
            row["total_cm2_margins"] = (
                round(cm2_profit / net_sales * 100, 2) if net_sales else 0
            )
        if "cm2_profit_per_unit" in row:
            row["cm2_profit_per_unit"] = (
                round(cm2_profit / quantity, 2) if quantity else 0
            )
        if "reimbursement_vs_sales" in row:
            reimbursement = abs(
                _rolling_number(
                    row.get("rembursement_fee"),
                    row.get("net_reimbursement"),
                    row.get("net_reimbursement_total"),
                )
            )
            row["reimbursement_vs_sales"] = (
                round(reimbursement / net_sales * 100, 2) if net_sales else 0
            )

    return rows


def _apply_rolling_amazon_fees_placement(rows, placement):
    if placement != "in_net_sales":
        return rows

    for row in rows:
        amazon_fees = abs(_rolling_number(row.get("amazon_fee"), row.get("amazon_fees")))
        net_sales = _rolling_number(row.get("net_sales")) - amazon_fees
        quantity = _rolling_number(row.get("total_quantity"))
        profit = _rolling_number(row.get("profit"))

        row["net_sales"] = round(net_sales, 2)
        row["net_sales_per_unit"] = round(net_sales / quantity, 2) if quantity else 0
        row["asp"] = row["net_sales_per_unit"]
        row["acos"] = round(
            _rolling_number(row.get("ads_spend"), row.get("advertising_total"), row.get("advertising_total_final"))
            / net_sales * 100,
            2,
        ) if net_sales else 0
        row["profit_percentage"] = (
            round(profit / net_sales * 100, 2) if net_sales else 0
        )
        row["unit_wise_profitability"] = (
            round(profit / quantity, 2) if quantity else 0
        )
        if "Profit" in row:
            row["Profit"] = profit
        if "total_profit" in row:
            row["total_profit"] = profit
        if "cm1_profit" in row:
            row["cm1_profit"] = profit
        if "cm1_profit_per_unit" in row:
            row["cm1_profit_per_unit"] = row["unit_wise_profitability"]
        if "cm1_profit_per" in row:
            row["cm1_profit_per"] = row["profit_percentage"]
        if "cm1_profit_percentage" in row:
            row["cm1_profit_percentage"] = row["profit_percentage"]
        if "cm2_profit_per" in row:
            row["cm2_profit_per"] = round(
                _rolling_number(row.get("cm2_profit"), row.get("total_cm2_profit")) / net_sales * 100,
                2,
            ) if net_sales else 0
        row["tacos_total_advertising_cost_of_sale"] = calculate_pnl_tacos(row)
        row["tacos"] = row["tacos_total_advertising_cost_of_sale"]

    return rows


def _build_rolling_card_total(rows):
    if isinstance(rows, dict):
        total = dict(rows)
    else:
        total_fields = (
            "total_quantity",
            "quantity", "units_sold", "unit_sold",
            "return_quantity", "return_units", "units_returned",
            "refund_sales", "sales_refund", "refund_amount",
            "net_taxes", "net_tax", "taxes_net",
            "net_credits", "net_credit", "credits_net",
            "taxncredit", "tax_and_credits", "misc_transaction",
            "product_spend", "display_spend", "brand_spend", "visible_ads",
            "dealsvouchar_ads", "dealsvoucher_ads", "dealvouchars_ads",
            "gross_sales",
            "net_sales",
            "profit",
            "cost_of_unit_sold",
            "platform_fee",
            "tex_and_credits",
            "rembursement_fee",
            "selling_fees",
            "fba_fees",
            "amazon_fee",
            "amazon_fees",
            "shipping_charges",
            "storage_fee",
            "inventory_charges_and_reimbursement",
            "fba_disposal",
            "lost_total",
            "current_net_reimbursement",
            "net_reimbursement",
            "net_reimbursement_total",
            "disbursement",
            "debt_payment",
            "platform_management_fees",
            "platform_managment_fees",
            "other_adjustment",
            "other_transaction_fees",
            "other_transactions",
            "advertising_total_final",
            "total_ads",
            "advertising_fees",
            "advertising_total",
            "ads_spend",
            "total_cm2_profit",
            "cm2_profit_total",
            "cm2_profit",
            "promotional_rebates",
        )
        total = {
            field: sum(float(row.get(field) or 0) for row in rows)
            for field in total_fields
            if any(field in row for row in rows)
        }

    quantity = _rolling_number(total.get("quantity"), total.get("units_sold"), total.get("unit_sold"))
    total["quantity"] = quantity
    total["units_sold"] = quantity
    return_units = _rolling_number(total.get("return_quantity"), total.get("return_units"), total.get("units_returned"))
    total["return_quantity"] = return_units
    total["return_units"] = return_units
    total["refund_sales"] = _rolling_number(total.get("refund_sales"), total.get("sales_refund"), total.get("refund_amount"))
    total["net_taxes"] = _rolling_number(total.get("net_taxes"), total.get("net_tax"), total.get("taxes_net"))
    total["net_credits"] = _rolling_number(total.get("net_credits"), total.get("net_credit"), total.get("credits_net"))
    total["product_spend"] = _rolling_number(total.get("product_spend"), total.get("sponsored_product"), total.get("Sponsored_Product"))
    total["display_spend"] = _rolling_number(total.get("display_spend"), total.get("sponsored_display"), total.get("Sponsored_Display"))
    brand_spend = _rolling_number(total.get("brand_spend"), total.get("visible_ads"), total.get("sponsored_brand"))
    total["brand_spend"] = brand_spend
    total["visible_ads"] = _rolling_number(total.get("visible_ads"), brand_spend)
    deals_spend = _rolling_number(total.get("dealsvouchar_ads"), total.get("dealsvoucher_ads"), total.get("dealvouchars_ads"))
    total["dealsvouchar_ads"] = deals_spend
    total["dealsvoucher_ads"] = deals_spend
    other_transactions = float(
        total.get("other_transaction_fees")
        or total.get("other_transactions")
        or 0
    )
    total["other_transaction_fees"] = other_transactions
    total["other_transactions"] = other_transactions

    units = float(total.get("total_quantity") or 0)
    net_sales = float(total.get("net_sales") or 0)
    gross_sales = float(total.get("gross_sales") or 0)
    ads_cost = next(
        (
            abs(float(total.get(field) or 0))
            for field in (
                "advertising_total_final",
                "total_ads",
                "advertising_fees",
                "advertising_total",
                "ads_spend",
            )
            if total.get(field)
        ),
        0.0,
    )
    cm2_profit = float(
        total.get("total_cm2_profit")
        or total.get("cm2_profit_total")
        or total.get("cm2_profit")
        or 0
    )
    cm1_profit = _rolling_number(
        total.get("profit"),
        total.get("total_profit"),
        total.get("cm1_profit"),
    )
    cm1_profit_percentage = (
        cm1_profit / abs(net_sales) * 100 if net_sales else 0.0
    )
    total["profit_percentage"] = cm1_profit_percentage
    total["cm1_profit_percentage"] = cm1_profit_percentage
    total["cm1_profit_per"] = cm1_profit_percentage
    total["asp"] = net_sales / units if units else 0.0
    total["gross_sales_per_unit"] = gross_sales / units if units else 0.0
    total["net_sales_per_unit"] = net_sales / units if units else 0.0
    total["cost_of_ads_per_unit"] = ads_cost / units if units else 0.0
    monthly_ads_cost = next(
        (
            abs(float(total.get(field) or 0))
            for field in (
                "total_ads", "ads_spend", "advertising_total_final",
                "advertising_fees", "advertising_total",
            )
            if total.get(field)
        ),
        0.0,
    )
    ads_ratio = monthly_ads_cost / abs(net_sales) * 100 if net_sales else 0.0
    total["acos"] = ads_ratio
    total["ads_acos"] = ads_ratio
    total["tacos_total_advertising_cost_of_sale"] = ads_ratio
    total["total_cm2_margins"] = (
        cm2_profit / net_sales * 100 if net_sales else 0.0
    )
    promotions = abs(float(total.get("promotional_rebates") or 0))
    total["promotional_rebates_percentage"] = (
        promotions / abs(net_sales) * 100 if net_sales else 0.0
    )
    reimbursement = _rolling_number(
        total.get("rembursement_fee"),
        total.get("current_net_reimbursement"),
        total.get("net_reimbursement"),
        total.get("net_reimbursement_total"),
    )
    if reimbursement == 0:
        disbursement = _rolling_number(total.get("disbursement"))
        debt_payment = _rolling_number(total.get("debt_payment"))
        if disbursement or debt_payment:
            reimbursement = abs(disbursement - debt_payment)
    cm2_profit = _rolling_number(
        total.get("total_cm2_profit"),
        total.get("cm2_profit_total"),
        total.get("cm2_profit"),
    )
    total["reimbursement_vs_sales"] = (
        abs(reimbursement) / abs(net_sales) * 100 if net_sales else 0.0
    )
    reimbursement_vs_cm2 = (
        abs(reimbursement) / abs(cm2_profit) * 100 if cm2_profit else 0.0
    )
    total["rembursment_vs_cm2_margins"] = reimbursement_vs_cm2
    total["reimbursement_vs_cm2_margins"] = reimbursement_vs_cm2
    return total


def _rolling_finance_table_country(country, currency):
    return "uk_usd" if country == "uk" and currency.strip().lower() == "usd" else country


@business_intelligence_bp.route('/MonthsforBI/rolling-12-months', methods=['GET'])
def get_rolling_12_month_sku_data():
    auth_header = request.headers.get('Authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        return jsonify({'error': 'Authorization token is missing or invalid'}), 401

    token = auth_header.split(' ')[1]

    try:
        payload, user_id, member_id = get_effective_user_id_from_token(token)
        country = (request.args.get('countryName') or '').strip().lower()
        if country not in {'uk', 'us', 'global'}:
            return jsonify({'error': 'countryName must be one of: uk, us, global'}), 400
        amazon_fees_placement = (
            request.args.get("amazonFeesPlacement") or "before_cm1"
        ).strip().lower()
        if amazon_fees_placement not in {"before_cm1", "in_net_sales"}:
            return jsonify({'error': 'Invalid amazonFeesPlacement'}), 400
        promotions_placement = (
            request.args.get("promotionsPlacement") or "in_net_sales"
        ).strip().lower()
        if promotions_placement not in {"in_net_sales", "in_ads_spend"}:
            return jsonify({'error': 'Invalid promotionsPlacement'}), 400
        end_month_arg = request.args.get('endMonth')
        end_year_arg = request.args.get('endYear')
        if bool(end_month_arg) != bool(end_year_arg):
            return jsonify({'error': 'endMonth and endYear must be provided together'}), 400

        table_country = _rolling_finance_table_country(country, request.args.get("homeCurrency", ""))
        table_pattern = f"skuwisemonthly_{user_id}_{table_country}_%"
        tables_query = text("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
              AND table_name LIKE :pattern
        """)
        with user_engine.connect() as conn:
            table_names = [
                row[0]
                for row in conn.execute(tables_query, {"pattern": table_pattern})
            ]

        available_tables = {}
        for table_name in table_names:
            period = parse_period_from_table_name(table_name, user_id, table_country)
            if not period:
                continue
            # Prefer the established naming convention when both variants exist.
            has_table_suffix = table_name.endswith("_table")
            preferred_suffix = country == "global"
            if (
                period not in available_tables
                or has_table_suffix == preferred_suffix
            ):
                available_tables[period] = table_name

        if end_month_arg:
            try:
                end_month = int(end_month_arg)
                end_year = int(end_year_arg)
            except ValueError:
                return jsonify({'error': 'endMonth and endYear must be integers'}), 400
            if not 1 <= end_month <= 12 or not 1 <= end_year <= 9999:
                return jsonify({'error': 'Invalid endMonth or endYear'}), 400
        else:
            today = date.today()
            latest_completed_year, latest_completed_month = _shift_month(today.year, today.month, -1)
            completed_period = f"{latest_completed_year:04d}-{latest_completed_month:02d}"
            available_completed_periods = [
                period for period in available_tables if period <= completed_period
            ]
            if not available_completed_periods:
                return jsonify({
                    'error': 'No completed monthly SKU data is available',
                    'countryName': country,
                }), 404
            end_year, end_month = map(int, max(available_completed_periods).split("-"))

        current_periods = _rolling_month_periods(end_year, end_month, 12)
        previous_year, previous_month = _shift_month(end_year, end_month, -12)
        previous_periods = _rolling_month_periods(previous_year, previous_month, 12)

        key_column = "product_name" if country == "global" else "sku"

        def period_response(periods, include_monthly_breakdown=False):
            def build_period_total(card_totals, sku_rows):
                total = _build_rolling_card_total(card_totals or sku_rows)
                sku_total = _build_rolling_card_total(sku_rows)
                for field in (
                    "quantity", "units_sold", "unit_sold",
                    "return_quantity", "return_units", "units_returned",
                    "refund_sales", "sales_refund", "refund_amount",
                    "net_taxes", "net_tax", "taxes_net",
                    "net_credits", "net_credit", "credits_net",
                    "taxncredit", "tax_and_credits", "misc_transaction",
                    "product_spend", "display_spend", "brand_spend", "visible_ads",
                    "dealsvouchar_ads", "dealsvoucher_ads", "dealvouchars_ads",
                    "other_transaction_fees",
                    "other_transactions",
                    "rembursement_fee",
                    "current_net_reimbursement",
                    "net_reimbursement",
                    "net_reimbursement_total",
                    "disbursement",
                    "debt_payment",
                ):
                    if not total.get(field) and sku_total.get(field):
                        total[field] = sku_total[field]
                return _build_rolling_card_total(total)

            selected_tables = [
                (period, available_tables[period])
                for period in periods
                if period in available_tables
            ]
            rows, card_totals = _aggregate_sku_month_tables(
                selected_tables,
                key_column,
            )
            effective_promotions_placement = promotions_placement
            effective_amazon_placement = amazon_fees_placement
            adjusted_card_totals = dict(card_totals)
            _apply_rolling_promotions_placement(
                rows, effective_promotions_placement
            )
            if adjusted_card_totals:
                _apply_rolling_promotions_placement(
                    [adjusted_card_totals], effective_promotions_placement
                )
            _apply_rolling_amazon_fees_placement(
                rows, effective_amazon_placement
            )
            if adjusted_card_totals:
                _apply_rolling_amazon_fees_placement(
                    [adjusted_card_totals], effective_amazon_placement
                )
            monthly_breakdown = {}
            if include_monthly_breakdown:
                selected_table_by_period = dict(selected_tables)
                for period in periods:
                    table_name = selected_table_by_period.get(period)
                    if not table_name:
                        monthly_breakdown[period] = {}
                        continue
                    monthly_rows, monthly_card_totals = _aggregate_sku_month_tables(
                        [(period, table_name)],
                        key_column,
                    )
                    effective_month_promotions = promotions_placement
                    effective_month_placement = amazon_fees_placement
                    adjusted_month_card_totals = dict(monthly_card_totals)
                    _apply_rolling_promotions_placement(
                        monthly_rows, effective_month_promotions
                    )
                    if adjusted_month_card_totals:
                        _apply_rolling_promotions_placement(
                            [adjusted_month_card_totals], effective_month_promotions
                        )
                    _apply_rolling_amazon_fees_placement(
                        monthly_rows, effective_month_placement
                    )
                    if adjusted_month_card_totals:
                        _apply_rolling_amazon_fees_placement(
                            [adjusted_month_card_totals], effective_month_placement
                        )
                    monthly_breakdown[period] = build_period_total(
                        adjusted_month_card_totals,
                        monthly_rows,
                    )
            response = {
                "from": periods[0],
                "to": periods[-1],
                "months_requested": len(periods),
                "months_with_tables": [period for period, _ in selected_tables],
                "missing_months": [
                    period for period in periods if period not in available_tables
                ],
                "sku_count": len(rows),
                "skus": rows,
                "total": build_period_total(adjusted_card_totals, rows),
                "monthly_breakdown": monthly_breakdown,
            }
            return response, adjusted_card_totals

        current_response, current_card_total = period_response(
            current_periods,
            include_monthly_breakdown=True,
        )
        previous_response, previous_card_total = period_response(
            previous_periods,
            include_monthly_breakdown=True,
        )

        return jsonify({
            "message": "Rolling 12-month SKU data",
            "countryName": country,
            "current_period": current_response,
            "previous_period": previous_response,
            "card_metrics": build_pnl_card_metrics(
                [
                    _build_rolling_card_total(
                        current_card_total or current_response["skus"]
                    )
                ],
                [
                    _build_rolling_card_total(
                        previous_card_total or previous_response["skus"]
                    )
                ],
            ),
        }), 200

    except jwt.ExpiredSignatureError:
        return jsonify({'error': 'Token has expired'}), 401
    except jwt.InvalidTokenError:
        return jsonify({'error': 'Invalid token'}), 401
    except Exception as e:
        current_app.logger.exception("Failed to aggregate rolling 12-month SKU data")
        return jsonify({'error': 'Server error', 'details': str(e)}), 500


@business_intelligence_bp.route('/MonthsforBI/rolling-12-months/cashflow', methods=['GET'])
def get_rolling_12_month_cashflow_data():
    auth_header = request.headers.get('Authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        return jsonify({'error': 'Authorization token is missing or invalid'}), 401

    token = auth_header.split(' ')[1]

    try:
        payload, user_id, member_id = get_effective_user_id_from_token(token)
        country = (request.args.get('countryName') or '').strip().lower()
        if country not in {'uk', 'us', 'global'}:
            return jsonify({'error': 'countryName must be one of: uk, us, global'}), 400

        end_month_arg = request.args.get('endMonth')
        end_year_arg = request.args.get('endYear')
        if bool(end_month_arg) != bool(end_year_arg):
            return jsonify({'error': 'endMonth and endYear must be provided together'}), 400

        table_country = _rolling_finance_table_country(country, request.args.get("homeCurrency", ""))
        table_pattern = f"skuwisemonthly_{user_id}_{table_country}_%"
        tables_query = text("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
              AND table_name LIKE :pattern
        """)
        with user_engine.connect() as conn:
            table_names = [
                row[0]
                for row in conn.execute(tables_query, {"pattern": table_pattern})
            ]

        available_tables = {}
        for table_name in table_names:
            period = parse_period_from_table_name(table_name, user_id, table_country)
            if not period:
                continue
            has_table_suffix = table_name.endswith("_table")
            preferred_suffix = country == "global"
            if (
                period not in available_tables
                or has_table_suffix == preferred_suffix
            ):
                available_tables[period] = table_name

        if end_month_arg:
            try:
                end_month = int(end_month_arg)
                end_year = int(end_year_arg)
            except ValueError:
                return jsonify({'error': 'endMonth and endYear must be integers'}), 400
            if not 1 <= end_month <= 12 or not 1 <= end_year <= 9999:
                return jsonify({'error': 'Invalid endMonth or endYear'}), 400
        else:
            today = date.today()
            latest_completed_year, latest_completed_month = _shift_month(
                today.year,
                today.month,
                -1,
            )
            completed_period = f"{latest_completed_year:04d}-{latest_completed_month:02d}"
            available_completed_periods = [
                period for period in available_tables if period <= completed_period
            ]
            if not available_completed_periods:
                return jsonify({
                    'error': 'No completed monthly cash flow data is available',
                    'countryName': country,
                }), 404
            end_year, end_month = map(int, max(available_completed_periods).split("-"))

        current_periods = _rolling_month_periods(end_year, end_month, 12)
        previous_year, previous_month = _shift_month(end_year, end_month, -12)
        previous_periods = _rolling_month_periods(previous_year, previous_month, 12)

        def empty_summary():
            return {
                "quantity_total": 0.0,
                "gross_sales": 0.0,
                "net_sales": 0.0,
                "amazon_fee": 0.0,
                "advertising_total": 0.0,
                "taxncredit": 0.0,
                "otherwplatform": 0.0,
                "rembursement_fee": 0.0,
                "promotional_rebates": 0.0,
                "selling_fees": 0.0,
                "fba_fees": 0.0,
                "cost_of_unit_sold": 0.0,
                "cm2_profit": 0.0,
                "total_cm2_profit": 0.0,
                "cashflow": 0.0,
            }

        def numeric(value):
            try:
                return float(value or 0)
            except (TypeError, ValueError):
                return 0.0

        def summarize_table(table_name):
            with user_engine.connect() as conn:
                result = conn.execute(text(f'SELECT * FROM public."{table_name}"'))
                rows = [dict(row) for row in result.mappings()]
            if not rows:
                return None

            def is_total(row):
                return any(
                    str(row.get(field) or '').strip().lower()
                    in {'total', 'totals', 'grand total', 'grand_total'}
                    for field in ('product_name', 'sku')
                )

            total_row = next((row for row in rows if is_total(row)), None)
            source_rows = [total_row] if total_row else rows

            def total_for(*columns):
                column = next(
                    (candidate for candidate in columns
                     if any(candidate in row for row in source_rows)),
                    None,
                )
                if not column:
                    return 0.0
                return sum(numeric(row.get(column)) for row in source_rows)

            totals = {
                "quantity_total": total_for("total_quantity", "quantity_total"),
                "gross_sales": total_for("gross_sales"),
                "net_sales": total_for("net_sales"),
                "amazon_fee": -abs(total_for("amazon_fee", "amazon_fees")),
                "advertising_total": total_for(
                    "advertising_total",
                    "advertising_total_final",
                    "total_ads",
                    "advertising_fees",
                ),
                "taxncredit": total_for("tex_and_credits", "taxncredit"),
                "otherwplatform": total_for("platform_fee", "otherwplatform"),
                "rembursement_fee": total_for(
                    "rembursement_fee",
                    "current_net_reimbursement",
                    "net_reimbursement",
                ),
                "promotional_rebates": total_for("promotional_rebates"),
                "selling_fees": total_for("selling_fees"),
                "fba_fees": total_for("fba_fees"),
                "cost_of_unit_sold": total_for("cost_of_unit_sold", "cogs"),
                "cm2_profit": total_for("cm2_profit", "profit"),
                "total_cm2_profit": total_for(
                    "total_cm2_profit",
                    "cm2_profit_total",
                    "cm2_profit",
                    "profit",
                ),
            }
            totals["cashflow"] = (
                totals["cost_of_unit_sold"] + totals["total_cm2_profit"]
            )
            return totals

        def summarize_period(periods):
            summary = empty_summary()
            monthly_breakdown = {}
            months_with_tables = []
            for period in periods:
                table_name = available_tables.get(period)
                if not table_name:
                    monthly_breakdown[period] = empty_summary()
                    continue
                monthly_summary = summarize_table(table_name)
                if monthly_summary is None:
                    continue
                months_with_tables.append(period)
                monthly_breakdown[period] = {
                    key: round(value, 2)
                    for key, value in monthly_summary.items()
                }
                for key, value in monthly_summary.items():
                    summary[key] += value

            for key, value in summary.items():
                summary[key] = round(value, 2)
            add_cashflow_summary_fields(summary)
            return {
                "from": periods[0],
                "to": periods[-1],
                "months_requested": len(periods),
                "months_with_tables": months_with_tables,
                "missing_months": [
                    period for period in periods if period not in months_with_tables
                ],
                "monthly_breakdown": monthly_breakdown,
                "summary": summary,
            }, summary

        current_period, current_summary = summarize_period(current_periods)
        previous_period, previous_summary = summarize_period(previous_periods)
        if not current_period["months_with_tables"]:
            return jsonify({
                'error': 'No cash flow data found for the rolling 12-month period',
                'countryName': country,
                'current_period': current_period,
            }), 404

        return jsonify({
            "message": "Rolling 12-month cash flow data",
            "period_type": "last12months",
            "countryName": country,
            "current_period": current_period,
            "previous_period": previous_period,
            "summary": current_summary,
            "previous_summary": previous_summary,
            "monthlyBreakdown": current_period["monthly_breakdown"],
            "card_metrics": build_cashflow_card_metrics(
                current_summary,
                previous_summary,
            ),
        }), 200

    except jwt.ExpiredSignatureError:
        return jsonify({'error': 'Token has expired'}), 401
    except jwt.InvalidTokenError:
        return jsonify({'error': 'Invalid token'}), 401
    except Exception as e:
        current_app.logger.exception("Failed to aggregate rolling 12-month cash flow data")
        return jsonify({'error': 'Server error', 'details': str(e)}), 500


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
