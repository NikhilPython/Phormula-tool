from flask import Blueprint, request, jsonify
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


# Load environment variables
load_dotenv()
SECRET_KEY = Config.SECRET_KEY
OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')
db_url = os.getenv('DATABASE_URL')
oa_client = OpenAI(api_key=OPENAI_API_KEY)

# Initialize database engine
engine = create_engine(db_url)

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
    with engine.connect() as conn:
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
        payload = jwt.decode(token, SECRET_KEY, algorithms=['HS256'])
        user_id = payload.get('user_id')
        if not user_id:
            return jsonify({'error': 'Invalid token payload: user_id missing'}), 401

        # Extract query parameters
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

        print("========== /MonthsforBI DEBUG ==========")
        print(f"User: {user_id} | Country: {country} | is_global: {is_global}")
        print(f"Month1: {month1}-{year1} | Month2: {month2}-{year2}")
        print(f"Tables -> Month1: {table1} | Month2: {table2}")
        print(f"key_column (FE): {key_column} | scan_key (backend): {scan_key}")

        # Dynamic column list
        columns = ['asp', 'quantity', 'profit', 'sales_mix', 'product_name', 'net_sales', 'unit_wise_profitability', 'product_sales','profit_percentage','rembursement_fee']
        if not is_global:
            columns.append('sku')
        cols_str = ', '.join(columns)

        def query_table_columns(table_name):
            query = text(f"SELECT {cols_str} FROM {table_name} LIMIT 100000")
            with engine.connect() as conn:
                result = conn.execute(query)
                keys = result.keys()
                rows = []
                for row in result:
                    row_dict = dict(zip(keys, row))
                    if row_dict.get('product_name') and str(row_dict['product_name']).strip().lower() == 'total':
                        continue
                    rows.append({col: row_dict.get(col) for col in columns})
                return rows

        data1 = query_table_columns(table1)  # Month 1 (earlier)
        data2 = query_table_columns(table2)  # Month 2 (later)

        print(f"Rows fetched -> data1: {len(data1)} | data2: {len(data2)}")
        if data2:
            print("Sample Month2 row:", {k: data2[0].get(k) for k in ['sku', 'product_name', 'sales_mix', 'quantity', 'net_sales'] if k in data2[0]})

        # ✅ query only keys safely (uses scan_key)
        def query_only_keys(table_name):
            if scan_key == "sku":
                cols = "sku, product_name"
            else:
                cols = "product_name"

            q = text(f"SELECT {cols} FROM {table_name} LIMIT 200000")
            keys_set = set()

            with engine.connect() as conn:
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
            'quantity': 'Unit Growth',
            'asp': 'ASP Growth',
            'product_sales':'Gross Sales Growth',
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

        def safe_float(val):
            try:
                if val is None:
                    return None
                return round(float(val), 2)
            except (ValueError, TypeError):
                return None

        def calculate_growth(data1, data2, key=scan_key, numeric_fields=None, non_growth_fields=None):
            if non_growth_fields is None:
                non_growth_fields = ["profit_percentage","rembursement_fee" ]  # ✅ add more raw-only fields here later if needed

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
                sales_mix_val_month2 = safe_float(row2.get('sales_mix'))
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
                    val2 = safe_float(row2.get(field))
                    val1 = safe_float(row1.get(field)) if row1 else None

                    growth_row[f"{field}_month2"] = val2
                    growth_row[f"{field}_month1"] = val1

                    if val1 is None or val2 is None:
                        growth = None
                    else:
                        if field == "sales_mix":
                            # sales_mix already in % -> change in percentage points
                            growth = round(val2 - val1, 2)
                        elif val1 == 0:
                            growth = 0.0  # avoid infinity
                        else:
                            growth = round(((val2 - val1) / val1) * 100, 2)

                    output_label = growth_field_mapping[field]
                    growth_row[output_label] = {
                        'category': categorize_growth(growth),
                        'value': growth
                    }

                if row1 is None:
                    growth_row['new_or_reviving'] = True

                growth_results.append(growth_row)

            return growth_results



        growth_data = calculate_growth(data1, data2)
        print(f"Growth rows: {len(growth_data)}")

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
        with engine.connect() as conn:
            res = conn.execute(q_tables, {"pattern": pattern})
            for row in res:
                tname = row[0]
                p = parse_period_from_table_name(tname, user_id, country_lower)
                if p and p in last6_periods:
                    last6_tables.append(tname)

        print(f"Last6 tables found: {len(last6_tables)} -> {last6_tables}")

        # (optional / debug)
        historical_keys_last6m = set()
        for t in last6_tables:
            try:
                historical_keys_last6m |= query_only_keys(t)
            except Exception as e:
                print(f"Warning reading keys from {t}: {e}")

        month1_keys = {str(row.get(scan_key)).strip() for row in data1 if row.get(scan_key) is not None and str(row.get(scan_key)).strip()}
        month2_keys = {str(row.get(scan_key)).strip() for row in data2 if row.get(scan_key) is not None and str(row.get(scan_key)).strip()}

        print(f"Unique keys -> Month1: {len(month1_keys)} | Month2: {len(month2_keys)}")
        print("Month1 keys sample:", list(month1_keys)[:10])
        print("Month2 keys sample:", list(month2_keys)[:10])

        # ✅ Reviving
        reviving_keys = month2_keys - month1_keys
        print(f"Reviving keys (Month2 - Month1): {len(reviving_keys)}")
        print("Reviving keys sample:", list(reviving_keys)[:10])

        # ✅ Newly launched (baseline based)
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

        last6_tables_sorted = sorted(last6_tables, key=table_period)  # oldest -> newest
        print("Last6 tables sorted (oldest->newest):", last6_tables_sorted)

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
            with engine.connect() as conn:
                pre_exists = bool(conn.execute(q_exists, {"t": pre_table}).first())

        if pre_exists:
            baseline_table = pre_table
            baseline_period = f"{int(pre_y):04d}-{int(pre_m):02d}"
            try:
                baseline_keys = query_only_keys(baseline_table)
            except Exception as e:
                print(f"Warning reading keys from baseline pre-window table {baseline_table}: {e}")
                baseline_keys = set()
            print(f"[NEW-LAUNCH] Using pre-window baseline: {baseline_period} table={baseline_table} keys={len(baseline_keys)}")
        else:
            if last6_tables_sorted:
                baseline_table = last6_tables_sorted[0]
                baseline_period = table_period(baseline_table)
                try:
                    baseline_keys = query_only_keys(baseline_table)
                except Exception as e:
                    print(f"Warning reading keys from baseline oldest table {baseline_table}: {e}")
                    baseline_keys = set()
                print(f"[NEW-LAUNCH] Pre-window missing. Using oldest-available baseline: {baseline_period} table={baseline_table} keys={len(baseline_keys)}")
            else:
                print("[NEW-LAUNCH] No last6 tables available; baseline empty.")
                baseline_keys = set()

        newly_launched_keys = (month2_keys - baseline_keys) if baseline_keys else set()
        print(f"[NEW-LAUNCH] Newly launched keys computed: {len(newly_launched_keys)}")
        print("[NEW-LAUNCH] Newly launched sample:", list(newly_launched_keys)[:10])

        # New/Reviving = reviving OR newly launched
        new_reviving_keys = reviving_keys | newly_launched_keys
        print(f"Final New/Reviving keys (union): {len(new_reviving_keys)}")
        print("Final New/Reviving sample:", list(new_reviving_keys)[:10])
        print("=======================================")

        # ✅ Build bucket rows
        new_reviving_growth = [
            row for row in growth_data
            if (row.get(scan_key) is not None and str(row.get(scan_key)).strip() in new_reviving_keys)
        ]

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

        # ✅ All SKUs tab: union of all three buckets (no duplicates due to dedupe above)
        all_skus = top_80_skus + new_reviving_growth + other_skus

        # ✅ Totals returned separately (frontend renders its own total row)
        top_80_total = build_total_row(top_80_skus, key_column, bucket_label="Total (Top 80%)") if top_80_skus else None
        new_or_reviving_total = build_total_row(new_reviving_growth, key_column, bucket_label="Total (New/Reviving)") if new_reviving_growth else None
        other_total = build_total_row(other_skus, key_column, bucket_label="Total (Other SKUs)") if other_skus else None
        all_skus_total = build_total_row(all_skus, key_column, bucket_label="Total (All SKUs)") if all_skus else None

        return jsonify({
            'message': 'Comparison range received',
            'comparison_range': {
                'from': {'month': month_num_to_name(month1), 'year': year1, 'table_name': table1},
                'to': {'month': month_num_to_name(month2), 'year': year2, 'table_name': table2},
                'month2_label': month2_label
            },
            'categorized_growth': {
                'top_80_skus': top_80_skus,
                'new_or_reviving_skus': new_reviving_growth,
                'other_skus': other_skus,
                'all_skus': all_skus,

                # ✅ totals separate
                'top_80_total': top_80_total,
                'new_or_reviving_total': new_or_reviving_total,
                'other_total': other_total,
                'all_skus_total': all_skus_total,
            }
        }), 200

    except jwt.ExpiredSignatureError:
        return jsonify({'error': 'Token has expired'}), 401
    except jwt.InvalidTokenError:
        return jsonify({'error': 'Invalid token'}), 401
    except Exception as e:
        print("Unexpected error:", e)
        return jsonify({'error': 'Server error', 'details': str(e)}), 500









@business_intelligence_bp.route('/analyze_skus', methods=['POST'])
def analyze_skus():
    auth_header = request.headers.get('Authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        return jsonify({'error': 'Authorization token is missing or invalid'}), 401

    token = auth_header.split(' ')[1]

    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=['HS256'])
        user_id = payload.get('user_id')
        if not user_id:
            return jsonify({'error': 'Invalid token payload: user_id missing'}), 401

        data = request.get_json()
        enriched_skus = data.get('skus', [])
        month1 = data.get('month1')
        month2 = data.get('month2')
        country = data.get('country', '')  # Get country for debugging

        if not enriched_skus or not isinstance(enriched_skus, list):
            return jsonify({'error': 'Invalid or missing SKU data'}), 400

        

        def generate_insight(item):
            sku = item.get('sku')
            product_name = item.get('product_name', 'this product').strip()
            
           
            is_global = country.lower() == 'global'
            
            if is_global and not sku:
                key = product_name  # Use product name as key for global data
            elif sku:
                key = sku  # Use SKU as key for country-specific data
            else:
                key = product_name  # Fallback to product name
            
            

            if item.get('new_or_reviving'):  # New/reviving SKU
                prompt = f"""
You are a senior business analyst. The following is a new or reviving product (not present in the previous period, or newly launched). Based on the latest available data, generate AI insights as follows:

Details for '{product_name}'

Observations:
- List the 2-3 most important observations about this product's current launch/return, using absolute values (units sold, ASP, profit, etc.) from the data.
- Comment on launch/return momentum—e.g., strong debut, moderate start, etc.
- Highlight any potential (or warning) sign based on first-period performance (high ASP but low volume, etc.)

Improvements:
- Recommend focused next actions for Marketing, Sales, or Operations to maximize this product's success in the coming months (e.g., increase awareness, optimize pricing, gather early customer reviews).
- Make each suggestion clear and actionable.

Then, add a bullet each for:

Sales Volume:
• Comment on units sold and what it means for launch traction. Suggest a commercial action.

ASP:
• Note the price point; suggest if it should be tested higher/lower.

Profitability:
• Note profit per unit or overall. Suggest if costs or pricing can be optimized.

⚠️ End with a one-line verdict: Should this product be scaled quickly, tested more, or repositioned? Justify your answer.

Instructions:
- Use plain text with bullets only.
- DO NOT use Markdown formatting (no **bold**, no italics, no headers).
- Do NOT compare to previous periods (since there's no baseline).
- Use actual values from the data.
- Make all advice easy for business teams to implement.

Data:
{json.dumps(item, indent=2)}
"""
            else:  # Regular SKU
                prompt = f"""
You are a senior business analyst. Based on the following 2-month comparison data of the product, generate insights in this format:

Details for '{product_name}'

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
• [Describe sales trend by explicitly tying it to Units × ASP (e.g., “sales down mainly due to units decline while ASP was flat/up” or “sales up driven by ASP lift with stable units”).]
• [Choose ONE action bullet from the Improvements list that best fits the sales pattern and paste it verbatim.]

Profit:
• [Explain profit change using ONLY available signals like sales movement plus realized pricing/discounting/mix impact, and avoid any mention of COGS or cost changes.]
• [Choose ONE action bullet from the Improvements list that best aligns with protecting/improving profitability given the observed trend and paste it verbatim.]

Unit Profitability:
• [Explain per-unit profit change using ONLY available signals like realized price/discounting and mix (higher-priced variants) impact, without mentioning COGS.]
• [Choose ONE action bullet from the Improvements list that best fits per-unit profit strength/weakness and paste it verbatim.]



Instructions:
- Use plain text with bullets only.
- DO NOT use Markdown formatting (no **bold**, no italics, no headers).
- Avoid labels like "Root cause:" or "Action item:". Just use bullet points.
- Use % values and trends from the data for every observation.
- Make all insights easy for business teams to act on.

Data:
{json.dumps(item, indent=2)}
"""

            try:
                ai_response = oa_client.chat.completions.create(  # ✅ call the instance you created
                    model="gpt-4o",
                    messages=[
                        {
                            "role": "system",
                            "content": "You are a senior ecommerce analyst. Use plain text and bullet points only. Do not use Markdown formatting."
                        },
                        {"role": "user", "content": prompt}
                    ],
                    max_tokens=900,
                    temperature=0.4
                )

                ai_text = ai_response.choices[0].message.content.strip()

                

                return key, {
                    'sku': sku,
                    'product_name': product_name,
                    'insight': ai_text,
                    'key_used': key,  # Debug info
                    'is_global': is_global  # Debug info
                }
            except Exception as e:
                return key, {
                    'sku': sku,
                    'product_name': product_name,
                    'insight': f"Error generating insight: {str(e)}",
                    'key_used': key,
                    'is_global': is_global
                }

        insights = {}
        processed_count = 0
        error_count = 0
        
        with ThreadPoolExecutor(max_workers=10) as executor:
            future_to_sku = {executor.submit(generate_insight, item): item for item in enriched_skus}
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
                    print(f"Error processing item - SKU: {sku}, Product: {product_name}, Error: {e}")


        return jsonify({'insights': insights}), 200

    except jwt.ExpiredSignatureError:
        return jsonify({'error': 'Token has expired'}), 401
    except jwt.InvalidTokenError:
        return jsonify({'error': 'Invalid token'}), 401
    except Exception as e:
        print("Unexpected error in /analyze_skus:", e)
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
        payload = jwt.decode(token, SECRET_KEY, algorithms=['HS256'])
        user_id = payload.get('user_id')
        if not user_id:
            return jsonify({'error': 'Invalid token payload: user_id missing'}), 401

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

        with engine.connect() as conn:
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
        print("Unexpected error in /MonthsforBI/available-periods:", e)
        return jsonify({'error': 'Server error', 'details': str(e)}), 500