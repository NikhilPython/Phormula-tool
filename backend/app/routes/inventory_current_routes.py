import os
import re
import math
import jwt
import pandas as pd
import numpy as np
from datetime import date, datetime, timedelta
from calendar import monthrange
from flask import Blueprint, request, jsonify
from sqlalchemy import create_engine, text, inspect
from dotenv import load_dotenv

from app.utils.token_utils import get_effective_user_id_from_token
from config import Config


SECRET_KEY = Config.SECRET_KEY

load_dotenv()
db_url = os.getenv("DATABASE_URL")
primary_engine = create_engine(db_url, pool_pre_ping=True)
DATABASE_AMAZON_URL = os.getenv("DATABASE_AMAZON_URL")
amazon_engine = create_engine(DATABASE_AMAZON_URL, pool_pre_ping=True)

inventory_current_bp = Blueprint("inventory_current", __name__)


def is_safe_identifier(value):
    return bool(re.fullmatch(r"[A-Za-z0-9_]+", str(value)))


def clean_value(value):
    if value is None:
        return None

    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None

    return value


def to_number(value):
    if value is None:
        return 0

    if isinstance(value, (int, float)):
        if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
            return 0
        return value

    value = str(value).strip()

    if not value:
        return 0

    value = value.replace(",", "")
    value = re.sub(r"[^\d.\-]", "", value)

    try:
        return float(value)
    except ValueError:
        return 0

def get_previous_month_range(month_number, year_int):
    """
    Given current selected month/year, return full previous month range.
    Example:
      current = June 2026
      previous = May 1, 2026 to May 31, 2026
    """
    month_number = int(month_number)
    year_int = int(year_int)

    if month_number == 1:
        prev_month = 12
        prev_year = year_int - 1
    else:
        prev_month = month_number - 1
        prev_year = year_int

    last_day_prev = monthrange(prev_year, prev_month)[1]

    return {
        "start": date(prev_year, prev_month, 1),
        "end": date(prev_year, prev_month, last_day_prev),
        "month": prev_month,
        "year": prev_year,
    }


def construct_prev_platform_fee_table_name(user_id, country_key, month_number, year_int):
    """
    Matches historic monthly table naming used elsewhere:
      user_{user_id}_{country}_{month}_{year}_data

    If your actual historic table naming is different, update this one function only.
    """
    month_text = calendar_month_name[int(month_number)].lower()
    return f"user_{user_id}_{country_key}_{month_text}{year_int}_data"


def fetch_previous_platform_fee_as_storage_cost(user_id, country_key, month_number, year_int):
    """
    Calculates previous month platform fees using the same logic as Live BI previous-period logic.

    It:
    - Reads previous month historical table
    - Uses other_transaction_fees if present and non-zero, otherwise total
    - Excludes transfer/disbursement/order payment rows
    - Excludes advertising rows
    - Includes service/platform fee rows containing:
      fba, storage, disposal, subscription, longterm, referral, commission
    - Returns positive absolute total
    """

    prev_range = get_previous_month_range(month_number, year_int)

    table_name = construct_prev_platform_fee_table_name(
        user_id=user_id,
        country_key=country_key,
        month_number=prev_range["month"],
        year_int=prev_range["year"],
    )

    if not is_safe_identifier(table_name):
        return {
            "value": 0,
            "source_table": table_name,
            "period": {
                "start": prev_range["start"].isoformat(),
                "end": prev_range["end"].isoformat(),
            },
            "error": "Invalid historical table name",
        }

    inspector = inspect(primary_engine)

    if table_name not in inspector.get_table_names():
        return {
            "value": 0,
            "source_table": table_name,
            "period": {
                "start": prev_range["start"].isoformat(),
                "end": prev_range["end"].isoformat(),
            },
            "error": f"Historical table not found: {table_name}",
        }

    query = text(f"""
        SELECT *
        FROM (
            SELECT
                *,
                NULLIF(NULLIF(date_time, '0'), '')::timestamp AS date_ts
            FROM "{table_name}"
        ) t
        WHERE date_ts >= :start_date
          AND date_ts < :end_date_plus_one
    """)

    params = {
        "start_date": datetime.combine(prev_range["start"], datetime.min.time()),
        "end_date_plus_one": datetime.combine(
            prev_range["end"] + timedelta(days=1),
            datetime.min.time()
        ),
    }

    with primary_engine.connect() as connection:
        df = pd.read_sql(query, connection, params=params)

    if df is None or df.empty:
        return {
            "value": 0,
            "source_table": table_name,
            "period": {
                "start": prev_range["start"].isoformat(),
                "end": prev_range["end"].isoformat(),
            },
            "error": None,
        }

    for col in ["type", "description"]:
        if col not in df.columns:
            df[col] = ""

    t = df["type"].fillna("").astype(str).str.lower()
    d = df["description"].fillna("").astype(str).str.lower()

    if "other_transaction_fees" in df.columns:
        amount = df["other_transaction_fees"].apply(to_number)
        if float(np.nansum(amount.values)) == 0.0:
            amount = df["total"].apply(to_number) if "total" in df.columns else pd.Series([0] * len(df))
    else:
        amount = df["total"].apply(to_number) if "total" in df.columns else pd.Series([0] * len(df))

    ignore = (
        t.str.contains("transfer|disbursement", na=False)
        | d.str.contains("disbursement", na=False)
        | d.str.contains("order payment", na=False)
    )

    is_ads = (
        t.str.contains(r"productadspayment|sellerdealpayment", na=False)
        | d.str.contains(r"productadspayment|sellerdealcomplete", na=False)
        | d.str.contains(r"dealperformanceevent|dealparticipationevent", na=False)
        | d.str.contains(r"couponparticipationevent|couponperformanceevent", na=False)
        | d.str.contains(r"\bcoupon\b", na=False)
    ) & (~ignore)

    is_platform_fee = (
        (
            t.str.contains("servicefee", na=False)
            | d.str.contains(r"\bfee\b", na=False)
        )
        & d.str.contains(
            r"fba|storage|disposal|subscription|longterm|long term|referral|commission",
            na=False
        )
    ) & (~ignore) & (~is_ads)

    previous_platform_fee = float(np.nansum(amount[is_platform_fee].values))

    return {
        "value": abs(previous_platform_fee),
        "source_table": table_name,
        "period": {
            "start": prev_range["start"].isoformat(),
            "end": prev_range["end"].isoformat(),
        },
        "error": None,
    }





def is_total_row(row):
    sku = str(row.get("SKU") or "").strip()
    product_name = str(row.get("Product Name") or "").strip().lower()

    return product_name == "total" or sku == ""


def build_inventory_item(row, trigger_columns):
    item = {
        "sku": clean_value(row.get("SKU")),
        "product_name": clean_value(row.get("Product Name"))
    }

    for column in trigger_columns:
        item[column] = clean_value(row.get(column))

    return item

def build_inventory_categories(rows):
    categories = {
        "liquidate": {
            "sku_count": 0,
            "product_count": 0,
            "items": []
        },
        "discount": {
            "sku_count": 0,
            "product_count": 0,
            "items": []
        },
        "monitor": {
            "sku_count": 0,
            "product_count": 0,
            "items": []
        },
        "unfulfillable": {
            "sku_count": 0,
            "product_count": 0,
            "items": []
        },
        "estimated_storage_cost": {
            "items": []
        }
    }

    for row in rows:
        is_total = is_total_row(row)

        inv_0_90 = to_number(row.get("inv-age-0-to-90-days"))
        inv_91_180 = to_number(row.get("inv-age-91-to-180-days"))
        inv_181_270 = to_number(row.get("inv-age-181-to-270-days"))
        inv_271_365 = to_number(row.get("inv-age-271-to-365-days"))
        inv_365_plus = to_number(row.get("inv-age-365-plus-days"))
        unfulfillable_qty = to_number(row.get("unfulfillable-quantity"))
        estimated_storage_cost = to_number(row.get("estimated-storage-cost-next-month"))

        if not is_total and (inv_271_365 > 0 or inv_365_plus > 0):
            trigger_columns = []

            if inv_271_365 > 0:
                trigger_columns.append("inv-age-271-to-365-days")

            if inv_365_plus > 0:
                trigger_columns.append("inv-age-365-plus-days")

            categories["liquidate"]["items"].append(
                build_inventory_item(row, trigger_columns)
            )

        if not is_total and inv_181_270 > 0:
            categories["discount"]["items"].append(
                build_inventory_item(row, ["inv-age-181-to-270-days"])
            )

        if not is_total and (inv_0_90 > 0 or inv_91_180 > 0):
            trigger_columns = []

            if inv_0_90 > 0:
                trigger_columns.append("inv-age-0-to-90-days")

            if inv_91_180 > 0:
                trigger_columns.append("inv-age-91-to-180-days")

            categories["monitor"]["items"].append(
                build_inventory_item(row, trigger_columns)
            )

        if not is_total and unfulfillable_qty > 0:
            categories["unfulfillable"]["items"].append(
                build_inventory_item(row, ["unfulfillable-quantity"])
            )

        # Total row is included only in estimated_storage_cost
        if estimated_storage_cost > 0:
            categories["estimated_storage_cost"]["items"].append(
                build_inventory_item(row, ["estimated-storage-cost-next-month"])
            )

    for category_name in ["liquidate", "discount", "monitor", "unfulfillable"]:
        items = categories[category_name]["items"]

        categories[category_name]["sku_count"] = len([
            item for item in items
            if item.get("sku")
        ])

        categories[category_name]["product_count"] = len([
            item for item in items
            if item.get("product_name")
        ])

    return categories

INVENTORY_AGE_COLUMNS = [
    "inv-age-0-to-90-days",
    "inv-age-91-to-180-days",
    "inv-age-181-to-270-days",
    "inv-age-271-to-365-days",
    "inv-age-365-plus-days",
]


def build_inventory_age_summary(rows):
    totals = {
        column: 0
        for column in INVENTORY_AGE_COLUMNS
    }

    for row in rows:
        # Skip total row so we calculate from actual SKU/product rows
        if is_total_row(row):
            continue

        for column in INVENTORY_AGE_COLUMNS:
            totals[column] += to_number(row.get(column))

    grand_total = sum(totals.values())

    percentages = {}
    for column, total in totals.items():
        if grand_total > 0:
            percentages[column] = round((total / grand_total) * 100, 2)
        else:
            percentages[column] = 0

    return {
        "total": grand_total,
        "columns": {
            column: {
                "total": totals[column],
                "percentage_share": percentages[column]
            }
            for column in INVENTORY_AGE_COLUMNS
        }
    }


QUARTER_MONTHS = {
    "q1": ["march", "february", "january"],
    "q2": ["june", "may", "april"],
    "q3": ["september", "august", "july"],
    "q4": ["december", "november", "october"],
}

AGE_TIER_TO_COLUMN = {
    # 181-270
    "181-210": "inv-age-181-to-270-days",
    "181-210 days": "inv-age-181-to-270-days",
    "181 to 210 days": "inv-age-181-to-270-days",
    "181-210-days": "inv-age-181-to-270-days",
    "211-240": "inv-age-181-to-270-days",
    "211-240 days": "inv-age-181-to-270-days",
    "211 to 240 days": "inv-age-181-to-270-days",
    "211-240-days": "inv-age-181-to-270-days",
    "241-270": "inv-age-181-to-270-days",
    "241-270 days": "inv-age-181-to-270-days",
    "241 to 270 days": "inv-age-181-to-270-days",
    "241-270-days": "inv-age-181-to-270-days",

    # 271-365
    "271-300": "inv-age-271-to-365-days",
    "271-300 days": "inv-age-271-to-365-days",
    "271 to 300 days": "inv-age-271-to-365-days",
    "271-300-days": "inv-age-271-to-365-days",
    "301-330": "inv-age-271-to-365-days",
    "301-330 days": "inv-age-271-to-365-days",
    "301 to 330 days": "inv-age-271-to-365-days",
    "301-330-days": "inv-age-271-to-365-days",
    "331-365": "inv-age-271-to-365-days",
    "331-365 days": "inv-age-271-to-365-days",
    "331 to 365 days": "inv-age-271-to-365-days",
    "331-365-days": "inv-age-271-to-365-days",

    # 365+
    "365+": "inv-age-365-plus-days",
    "365+ days": "inv-age-365-plus-days",
    "365 plus days": "inv-age-365-plus-days",
    "365-plus-days": "inv-age-365-plus-days",
    "366-455": "inv-age-365-plus-days",
    "366-455 days": "inv-age-365-plus-days",
    "366 to 455 days": "inv-age-365-plus-days",
    "366-455-days": "inv-age-365-plus-days",
    "456+": "inv-age-365-plus-days",
    "456+ days": "inv-age-365-plus-days",
    "456 plus days": "inv-age-365-plus-days",
    "456-plus-days": "inv-age-365-plus-days",
}


def get_previous_completed_month(today=None):
    today = today or date.today()

    if today.month == 1:
        return 12, today.year - 1

    return today.month - 1, today.year


def get_inventory_current_candidate_months(range_type, month_name=None, quarter=None, year=None):
    """
    Returns months in priority order.

    monthly:
      requested month only

    quarter_months / quarterly:
      Q2 => june, may, april

    yearly:
      previous completed month based on current date
      example: current month June => May
    """

    range_type = str(range_type or "monthly").strip().lower()
    month_name = str(month_name or "").strip().lower()
    quarter = str(quarter or "").strip().lower()

    if range_type in ("quarter_months", "quarterly", "quarter"):
        if not quarter and month_name:
            month_num = MONTH_NAME_TO_NUMBER.get(month_name)

            if month_num in (1, 2, 3):
                quarter = "q1"
            elif month_num in (4, 5, 6):
                quarter = "q2"
            elif month_num in (7, 8, 9):
                quarter = "q3"
            elif month_num in (10, 11, 12):
                quarter = "q4"

        return QUARTER_MONTHS.get(quarter, [])

    if range_type == "yearly":
        prev_month_num, prev_year = get_previous_completed_month()
        return [calendar_month_name[prev_month_num].lower()]

    if month_name:
        return [month_name]

    return []


def build_current_inventory_table_name(user_id, country_key, month_name, year):
    return f"currentinventory_{user_id}_{country_key}_{month_name}{year}_table"


def fetch_rows_from_current_inventory_table(table_name):
    query = text(f'SELECT * FROM "{table_name}"')

    with primary_engine.connect() as connection:
        result = connection.execute(query)
        columns = list(result.keys())

        rows = []
        for row in result.fetchall():
            row_dict = dict(zip(columns, row))
            cleaned_row = {
                key: clean_value(value)
                for key, value in row_dict.items()
            }
            rows.append(cleaned_row)

    return columns, rows


def fetch_rows_from_inventory_aged_history(user_id, country_key, month_name, year):
    """
    Fallback source:
      public.inventory_aged_history

    Converts history rows into the same shape as currentinventory table rows:
      SKU
      Product Name
      inv-age-0-to-90-days
      inv-age-91-to-180-days
      inv-age-181-to-270-days
      inv-age-271-to-365-days
      inv-age-365-plus-days
      unfulfillable-quantity
      estimated-storage-cost-next-month
    """

    month_number = MONTH_NAME_TO_NUMBER.get(month_name)

    if not month_number:
        return [], []

    marketplace_id = get_marketplace_id(country_key)

    where_country_sql = ""
    params = {
        "user_id": int(user_id),
        "year": int(year),
        "month": int(month_number),
    }

    if marketplace_id:
        where_country_sql = "AND marketplace_id = :marketplace_id"
        params["marketplace_id"] = marketplace_id
    else:
        # fallback if marketplace_id is not mapped
        where_country_sql = "AND LOWER(COALESCE(country, '')) = :country_key"
        params["country_key"] = country_key.lower()

    query = text(f"""
        SELECT
            sku,
            COALESCE(product_name, '') AS product_name,
            surcharge_age_tier,
            COALESCE(SUM(qty_charged), 0) AS qty_charged,
            COALESCE(SUM(amount_charged), 0) AS amount_charged,
            MAX(snapshot_date) AS snapshot_date
        FROM public.inventory_aged_history
        WHERE user_id = :user_id
          {where_country_sql}
          AND snapshot_date IS NOT NULL
          AND EXTRACT(YEAR FROM snapshot_date)::int = :year
          AND EXTRACT(MONTH FROM snapshot_date)::int = :month
        GROUP BY sku, product_name, surcharge_age_tier
    """)

    with amazon_engine.connect() as connection:
        history_rows = connection.execute(query, params).mappings().all()

    if not history_rows:
        return [], []

    grouped = {}

    for row in history_rows:
        sku = str(row["sku"] or "").strip()
        product_name = str(row["product_name"] or "").strip()
        age_tier = str(row["surcharge_age_tier"] or "").strip().lower()

        key = (sku, product_name)

        if key not in grouped:
            grouped[key] = {
                "SKU": sku,
                "Product Name": product_name,
                "inv-age-0-to-90-days": 0,
                "inv-age-91-to-180-days": 0,
                "inv-age-181-to-270-days": 0,
                "inv-age-271-to-365-days": 0,
                "inv-age-365-plus-days": 0,
                "unfulfillable-quantity": 0,
                "estimated-storage-cost-next-month": 0,
                "snapshot_date": clean_value(row["snapshot_date"]),
            }

        target_column = AGE_TIER_TO_COLUMN.get(age_tier)

        if target_column:
            grouped[key][target_column] += to_number(row["qty_charged"])

        grouped[key]["estimated-storage-cost-next-month"] += to_number(row["amount_charged"])

    rows = list(grouped.values())

    total_row = {
        "SKU": "",
        "Product Name": "Total",
        "inv-age-0-to-90-days": sum(to_number(r.get("inv-age-0-to-90-days")) for r in rows),
        "inv-age-91-to-180-days": sum(to_number(r.get("inv-age-91-to-180-days")) for r in rows),
        "inv-age-181-to-270-days": sum(to_number(r.get("inv-age-181-to-270-days")) for r in rows),
        "inv-age-271-to-365-days": sum(to_number(r.get("inv-age-271-to-365-days")) for r in rows),
        "inv-age-365-plus-days": sum(to_number(r.get("inv-age-365-plus-days")) for r in rows),
        "unfulfillable-quantity": 0,
        "estimated-storage-cost-next-month": sum(
            to_number(r.get("estimated-storage-cost-next-month")) for r in rows
        ),
        "snapshot_date": None,
    }

    rows.append(total_row)

    columns = [
        "SKU",
        "Product Name",
        "inv-age-0-to-90-days",
        "inv-age-91-to-180-days",
        "inv-age-181-to-270-days",
        "inv-age-271-to-365-days",
        "inv-age-365-plus-days",
        "unfulfillable-quantity",
        "estimated-storage-cost-next-month",
        "snapshot_date",
    ]

    return columns, rows


def resolve_inventory_current_source(user_id, country_key, range_type, month_name, year, quarter):
    """
    Priority:
      1. currentinventory dynamic table
      2. inventory_aged_history fallback

    For quarter:
      Q2 checks june, then may, then april.

    For yearly:
      checks previous completed month.
    """

    inspector = inspect(primary_engine)
    existing_tables = set(inspector.get_table_names(schema="public"))

    candidate_months = get_inventory_current_candidate_months(
        range_type=range_type,
        month_name=month_name,
        quarter=quarter,
        year=year,
    )

    tried_sources = []

    # 1. Try dynamic currentinventory tables first
    for candidate_month in candidate_months:
        table_name = build_current_inventory_table_name(
            user_id=user_id,
            country_key=country_key,
            month_name=candidate_month,
            year=year,
        )

        tried_sources.append(table_name)

        if table_name in existing_tables:
            columns, rows = fetch_rows_from_current_inventory_table(table_name)

            return {
                "found": True,
                "source_type": "currentinventory_table",
                "source_name": table_name,
                "selected_month": candidate_month,
                "columns": columns,
                "rows": rows,
                "tried_sources": tried_sources,
            }

    # 2. Fallback to public.inventory_aged_history
    for candidate_month in candidate_months:
        columns, rows = fetch_rows_from_inventory_aged_history(
            user_id=user_id,
            country_key=country_key,
            month_name=candidate_month,
            year=year,
        )

        tried_sources.append(f"inventory_aged_history:{candidate_month}{year}")

        if rows:
            return {
                "found": True,
                "source_type": "inventory_aged_history",
                "source_name": "public.inventory_aged_history",
                "selected_month": candidate_month,
                "columns": columns,
                "rows": rows,
                "tried_sources": tried_sources,
            }

    return {
        "found": False,
        "source_type": None,
        "source_name": None,
        "selected_month": None,
        "columns": [],
        "rows": [],
        "tried_sources": tried_sources,
    }


@inventory_current_bp.route("/inventory_current", methods=["GET", "OPTIONS"])
def get_inventory_current_table():
    if request.method == "OPTIONS":
        return jsonify({"message": "CORS Preflight OK"}), 200

    auth_header = request.headers.get("Authorization")
    if not auth_header or not auth_header.startswith("Bearer "):
        return jsonify({"error": "Authorization token is missing or invalid"}), 401

    token = auth_header.split(" ")[1]

    try:
        payload, effective_user_id, member_id = get_effective_user_id_from_token(token)
        user_id = payload["user_id"]
    except jwt.ExpiredSignatureError:
        return jsonify({"error": "Token has expired"}), 401
    except jwt.InvalidTokenError:
        return jsonify({"error": "Invalid token"}), 401

    try:
        country_key = request.args.get("country_key")
        month_name = request.args.get("month_name")
        year = request.args.get("year")

        # Supports:
        # range_type=monthly
        # range_type=quarter_months&quarter=Q2
        # range_type=yearly
        range_type = (
            request.args.get("range_type")
            or request.args.get("range")
            or request.args.get("period")
            or "monthly"
        )

        quarter = request.args.get("quarter")

        if not country_key or not year:
            return jsonify({
                "success": False,
                "message": "Missing required params: country_key, year"
            }), 400

        if str(range_type).lower() == "monthly" and not month_name:
            return jsonify({
                "success": False,
                "message": "Missing required param for monthly: month_name"
            }), 400

        if str(range_type).lower() in ("quarter_months", "quarterly", "quarter") and not quarter and not month_name:
            return jsonify({
                "success": False,
                "message": "Missing required param for quarter_months: quarter or month_name"
            }), 400

        user_id = str(user_id).strip()
        country_key = str(country_key).strip().lower()
        month_name = str(month_name or "").strip().lower()
        year = str(year).strip()
        range_type = str(range_type or "monthly").strip().lower()
        quarter = str(quarter or "").strip().lower()

        identifiers_to_check = [user_id, country_key, year]

        if month_name:
            identifiers_to_check.append(month_name)

        if quarter:
            identifiers_to_check.append(quarter)

        if not all(is_safe_identifier(value) for value in identifiers_to_check):
            return jsonify({
                "success": False,
                "message": "Invalid table parameters"
            }), 400

        if month_name and month_name not in MONTH_NAME_TO_NUMBER:
            return jsonify({
                "success": False,
                "message": f"Invalid month_name: {month_name}"
            }), 400

        if range_type in ("quarter_months", "quarterly", "quarter"):
            if quarter and quarter not in QUARTER_MONTHS:
                return jsonify({
                    "success": False,
                    "message": "Invalid quarter. Use Q1, Q2, Q3, or Q4"
                }), 400

        source_result = resolve_inventory_current_source(
            user_id=user_id,
            country_key=country_key,
            range_type=range_type,
            month_name=month_name,
            year=year,
            quarter=quarter,
        )

        if not source_result["found"]:
            return jsonify({
                "success": False,
                "message": "No current inventory table or inventory_aged_history data found",
                "source_type": None,
                "source_name": None,
                "selected_month": None,
                "columns": [],
                "rows": [],
                "tried_sources": source_result["tried_sources"],
            }), 404

        rows = source_result["rows"]
        columns = source_result["columns"]

        categories = build_inventory_categories(rows)
        inventory_age_summary = build_inventory_age_summary(rows)

        selected_month_number = MONTH_NAME_TO_NUMBER.get(source_result["selected_month"])

        previous_storage_cost = {
            "value": 0,
            "source_table": None,
            "period": None,
            "error": None,
        }

        if selected_month_number:
            previous_storage_cost = fetch_previous_platform_fee_as_storage_cost(
                user_id=user_id,
                country_key=country_key,
                month_number=selected_month_number,
                year_int=int(year),
            )

        categories["estimated_storage_cost"]["previous_storage_cost"] = previous_storage_cost["value"]
        categories["estimated_storage_cost"]["previous_storage_cost_source"] = previous_storage_cost["source_table"]
        categories["estimated_storage_cost"]["previous_storage_cost_period"] = previous_storage_cost["period"]
        categories["estimated_storage_cost"]["previous_storage_cost_error"] = previous_storage_cost["error"]

        return jsonify({
            "success": True,
            "range_type": range_type,
            "requested_month": month_name or None,
            "requested_quarter": quarter or None,
            "selected_month": source_result["selected_month"],
            "year": int(year),
            "country_key": country_key,

            "source_type": source_result["source_type"],
            "source_name": source_result["source_name"],
            "table_name": source_result["source_name"] if source_result["source_type"] == "currentinventory_table" else None,
            "tried_sources": source_result["tried_sources"],

            "columns": columns,
            "rows": rows,
            "total_rows": len(rows),

            "categories": categories,
            "inventory_age_summary": inventory_age_summary,
            "category_counts": {
                "liquidate": len(categories["liquidate"]["items"]),
                "discount": len(categories["discount"]["items"]),
                "monitor": len(categories["monitor"]["items"]),
                "unfulfillable": len(categories["unfulfillable"]["items"]),
                "estimated_storage_cost": len(categories["estimated_storage_cost"]["items"])
            }
        }), 200

    except Exception as e:
        return jsonify({
            "success": False,
            "message": "Error fetching inventory current table",
            "error": str(e)
        }), 500
    
       

from calendar import month_name as calendar_month_name


MONTH_NAME_TO_NUMBER = {
    name.lower(): index
    for index, name in enumerate(calendar_month_name)
    if name
}


MARKETPLACE_BY_COUNTRY = {
    "uk": "A1F83G8C2ARO7P",
    "gb": "A1F83G8C2ARO7P",
    "us": "ATVPDKIKX0DER",
    "usa": "ATVPDKIKX0DER",
}


def get_marketplace_id(country_key):
    country_key = str(country_key or "").strip().lower()
    return MARKETPLACE_BY_COUNTRY.get(country_key)


def month_label(month_number):
    return calendar_month_name[month_number]


@inventory_current_bp.route("/inventory_current_age_summary", methods=["GET", "OPTIONS"])
def get_inventory_current_age_summary():
    if request.method == "OPTIONS":
        return jsonify({"message": "CORS Preflight OK"}), 200

    auth_header = request.headers.get("Authorization")
    if not auth_header or not auth_header.startswith("Bearer "):
        return jsonify({"error": "Authorization token is missing or invalid"}), 401

    token = auth_header.split(" ")[1]

    try:
        payload, effective_user_id, member_id = get_effective_user_id_from_token(token)
        user_id = payload["user_id"]
    except jwt.ExpiredSignatureError:
        return jsonify({"error": "Token has expired"}), 401
    except jwt.InvalidTokenError:
        return jsonify({"error": "Invalid token"}), 401

    try:
        country_key = request.args.get("country_key")
        month_name = request.args.get("month_name")
        year = request.args.get("year")

        if not country_key or not month_name or not year:
            return jsonify({
                "success": False,
                "message": "Missing required params: country_key, month_name, year"
            }), 400

        user_id = str(user_id).strip()
        country_key = str(country_key).strip().lower()
        month_name = str(month_name).strip().lower()
        year = str(year).strip()

        if not all([
            is_safe_identifier(user_id),
            is_safe_identifier(country_key),
            is_safe_identifier(month_name),
            is_safe_identifier(year)
        ]):
            return jsonify({
                "success": False,
                "message": "Invalid table parameters"
            }), 400

        if month_name not in MONTH_NAME_TO_NUMBER:
            return jsonify({
                "success": False,
                "message": f"Invalid month_name: {month_name}"
            }), 400

        marketplace_id = get_marketplace_id(country_key)

        if not marketplace_id:
            return jsonify({
                "success": False,
                "message": f"Unsupported country_key: {country_key}"
            }), 400

        current_month_number = MONTH_NAME_TO_NUMBER[month_name]
        year_int = int(year)

        table_name = f"currentinventory_{user_id}_{country_key}_{month_name}{year}_table"

        inspector = inspect(primary_engine)

        if table_name not in inspector.get_table_names():
            return jsonify({
                "success": False,
                "message": f"Table not found: {table_name}",
                "table_name": table_name,
                "age_summary": [],
                "month_summary": [],
                "totals": {}
            }), 404

        previous_month_summary = []

        # ---------------------------------------------
        # 1. Previous months from AMAZON DB
        # ---------------------------------------------
        if current_month_number > 1:
            history_query = text("""
                SELECT
                    EXTRACT(MONTH FROM snapshot_date)::int AS month_number,

                    COALESCE(SUM(
                        CASE
                            WHEN surcharge_age_tier IN (
                                '181-210',
                                '181-210 days',
                                '181 to 210 days',
                                '181-210-days',
                                '211-240',
                                '211-240 days',
                                '211 to 240 days',
                                '211-240-days',
                                '241-270',
                                '241-270 days',
                                '241 to 270 days',
                                '241-270-days'
                            )
                            THEN qty_charged
                            ELSE 0
                        END
                    ), 0) AS inv_age_181_to_270_days,

                    COALESCE(SUM(
                        CASE
                            WHEN surcharge_age_tier IN (
                                '271-300',
                                '271-300 days',
                                '271 to 300 days',
                                '271-300-days',
                                '301-330',
                                '301-330 days',
                                '301 to 330 days',
                                '301-330-days',
                                '331-365',
                                '331-365 days',
                                '331 to 365 days',
                                '331-365-days'
                            )
                            THEN qty_charged
                            ELSE 0
                        END
                    ), 0) AS inv_age_271_to_365_days,

                    COALESCE(SUM(
                        CASE
                            WHEN surcharge_age_tier IN (
                                '365+',
                                '365+ days',
                                '365 plus days',
                                '365-plus-days',
                                '366-455',
                                '366-455 days',
                                '366 to 455 days',
                                '366-455-days',
                                '456+',
                                '456+ days',
                                '456 plus days',
                                '456-plus-days'
                            )
                            THEN qty_charged
                            ELSE 0
                        END
                    ), 0) AS inv_age_365_plus_days

                FROM inventory_aged_history
                WHERE user_id = :user_id
                  AND marketplace_id = :marketplace_id
                  AND snapshot_date IS NOT NULL
                  AND EXTRACT(YEAR FROM snapshot_date)::int = :year
                  AND EXTRACT(MONTH FROM snapshot_date)::int >= 1
                  AND EXTRACT(MONTH FROM snapshot_date)::int < :current_month_number
                GROUP BY EXTRACT(MONTH FROM snapshot_date)::int
                ORDER BY month_number ASC
            """)

            with amazon_engine.connect() as amazon_connection:
                history_rows = amazon_connection.execute(history_query, {
                    "user_id": int(user_id),
                    "marketplace_id": marketplace_id,
                    "year": year_int,
                    "current_month_number": current_month_number
                }).mappings().all()

            history_by_month = {
                int(row["month_number"]): row
                for row in history_rows
            }

            for month_number in range(1, current_month_number):
                row = history_by_month.get(month_number)

                previous_month_summary.append({
                    "month": month_label(month_number),
                    "month_number": month_number,
                    "year": year_int,
                    "source": "inventory_aged_history",
                    "totals": {
                        "inv-age-181-to-270-days": float(row["inv_age_181_to_270_days"] or 0) if row else 0,
                        "inv-age-271-to-365-days": float(row["inv_age_271_to_365_days"] or 0) if row else 0,
                        "inv-age-365-plus-days": float(row["inv_age_365_plus_days"] or 0) if row else 0
                    }
                })

        # ---------------------------------------------
        # 2. Current month from PRIMARY DB dynamic table
        # ---------------------------------------------
        current_query = text(f'''
            SELECT
                COALESCE(SUM(CAST(NULLIF("inv-age-181-to-270-days"::text, '') AS NUMERIC)), 0) AS inv_age_181_to_270_days,
                COALESCE(SUM(CAST(NULLIF("inv-age-271-to-365-days"::text, '') AS NUMERIC)), 0) AS inv_age_271_to_365_days,
                COALESCE(SUM(CAST(NULLIF("inv-age-365-plus-days"::text, '') AS NUMERIC)), 0) AS inv_age_365_plus_days
            FROM "{table_name}"
            WHERE LOWER(COALESCE("Product Name"::text, '')) != 'total'
        ''')

        with primary_engine.connect() as primary_connection:
            current_result = primary_connection.execute(current_query).mappings().first()

        current_month_summary = {
            "month": month_name.capitalize(),
            "month_number": current_month_number,
            "year": year_int,
            "source": table_name,
            "totals": {
                "inv-age-181-to-270-days": float(current_result["inv_age_181_to_270_days"] or 0),
                "inv-age-271-to-365-days": float(current_result["inv_age_271_to_365_days"] or 0),
                "inv-age-365-plus-days": float(current_result["inv_age_365_plus_days"] or 0)
            }
        }

        month_summary = previous_month_summary + [current_month_summary]

        age_summary = []

        for month_row in month_summary:
            age_summary.extend([
                {
                    "month": month_row["month"],
                    "month_number": month_row["month_number"],
                    "year": month_row["year"],
                    "source": month_row["source"],
                    "age_bucket": "181-270 days",
                    "column": "inv-age-181-to-270-days",
                    "units": month_row["totals"]["inv-age-181-to-270-days"]
                },
                {
                    "month": month_row["month"],
                    "month_number": month_row["month_number"],
                    "year": month_row["year"],
                    "source": month_row["source"],
                    "age_bucket": "271-365 days",
                    "column": "inv-age-271-to-365-days",
                    "units": month_row["totals"]["inv-age-271-to-365-days"]
                },
                {
                    "month": month_row["month"],
                    "month_number": month_row["month_number"],
                    "year": month_row["year"],
                    "source": month_row["source"],
                    "age_bucket": "365+ days",
                    "column": "inv-age-365-plus-days",
                    "units": month_row["totals"]["inv-age-365-plus-days"]
                }
            ])

        grand_totals = {
            "inv-age-181-to-270-days": sum(
                row["totals"]["inv-age-181-to-270-days"]
                for row in month_summary
            ),
            "inv-age-271-to-365-days": sum(
                row["totals"]["inv-age-271-to-365-days"]
                for row in month_summary
            ),
            "inv-age-365-plus-days": sum(
                row["totals"]["inv-age-365-plus-days"]
                for row in month_summary
            )
        }

        return jsonify({
            "success": True,
            "table_name": table_name,
            "month": month_name.capitalize(),
            "year": year_int,
            "country_key": country_key,
            "marketplace_id": marketplace_id,
            "history_range": {
                "from_month": "January",
                "to_month": month_label(current_month_number - 1) if current_month_number > 1 else None
            },
            "current_month_source": table_name,
            "historical_source": "inventory_aged_history",
            "totals": grand_totals,
            "month_summary": month_summary,
            "age_summary": age_summary
        }), 200

    except Exception as e:
        return jsonify({
            "success": False,
            "message": "Error fetching inventory age summary",
            "error": str(e)
        }), 500
    

