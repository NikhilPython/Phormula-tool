import os
import re
import math
import jwt
import pandas as pd
import numpy as np
from datetime import date, datetime, timedelta
from calendar import monthrange, month_name as calendar_month_name
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

GLOBAL_COUNTRIES = ["uk", "us"]


def is_global_country(country_key):
    return str(country_key or "").strip().lower() == "global"

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


def is_percentage_row(row):
    sku = str(row.get("SKU") or "").strip().lower()
    product_name = str(row.get("Product Name") or "").strip().lower()
    row_type = str(row.get("row_type") or "").strip().lower()

    return (
        bool(row.get("is_percentage_row")) or
        row_type == "percentage" or
        product_name in {"percentage", "% of total"} or
        sku in {"percentage", "% of total"}
    )


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
        inv_0_180 = to_number(row.get("inv-age-0-to-180-days"))
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

        if not is_total and inv_0_180 > 0:
            categories["monitor"]["items"].append(
                build_inventory_item(row, ["inv-age-0-to-180-days"])
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
    "inv-age-0-to-180-days",
    "inv-age-181-to-270-days",
    "inv-age-271-to-365-days",
    "inv-age-365-plus-days",
]


def build_inventory_age_summary(rows):
    totals = {
        column: 0
        for column in INVENTORY_AGE_COLUMNS
    }

    unfulfillable_total = 0
    current_month_units_sold_total = 0

    current_month_units_sold_column = None

    # Find dynamic column like:
    # Current Month Units Sold (June)
    # Current Month Units Sold (May)
    for row in rows:
        for key in row.keys():
            if str(key).startswith("Current Month Units Sold"):
                current_month_units_sold_column = key
                break

        if current_month_units_sold_column:
            break

    for row in rows:
        # Skip total row so we calculate from actual SKU/product rows
        if is_total_row(row):
            continue

        for column in INVENTORY_AGE_COLUMNS:
            totals[column] += to_number(row.get(column))

        unfulfillable_total += to_number(row.get("unfulfillable-quantity"))

        if current_month_units_sold_column:
            current_month_units_sold_total += to_number(
                row.get(current_month_units_sold_column)
            )

    sellable_total = (
        totals.get("inv-age-0-to-180-days", 0)
        + totals.get("inv-age-181-to-270-days", 0)
        + totals.get("inv-age-271-to-365-days", 0)
        + totals.get("inv-age-365-plus-days", 0)
    )

    # Existing denominator for ageing bucket %:
    # sellable ageing units + unfulfillable units
    percentage_base_total = sellable_total + unfulfillable_total

    
    total_units_with_sold_base = percentage_base_total

    percentages = {}
    for column, total in totals.items():
        if percentage_base_total > 0:
            percentages[column] = round((total / percentage_base_total) * 100, 2)
        else:
            percentages[column] = 0

    sold_percentage_share = (
        round((current_month_units_sold_total / total_units_with_sold_base) * 100, 2)
        if total_units_with_sold_base > 0
        else 0
    )

    sellable_percentage_share = (
        round((sellable_total / total_units_with_sold_base) * 100, 2)
        if total_units_with_sold_base > 0
        else 0
    )

    unfulfillable_percentage_share = (
        round((unfulfillable_total / total_units_with_sold_base) * 100, 2)
        if total_units_with_sold_base > 0
        else 0
    )

    return {
        # Existing ageing denominator
        "total": percentage_base_total,

        "sellable_total": sellable_total,
        "unfulfillable_total": unfulfillable_total,
        "percentage_base_total": percentage_base_total,

        # New totals with sold units
        "current_month_units_sold_total": current_month_units_sold_total,
        "total_units_with_sold_base": total_units_with_sold_base,

        "total_units_summary": {
            "current_month_units_sold": {
                "total": current_month_units_sold_total,
                "percentage_share": sold_percentage_share,
            },
            "sellable": {
                "total": sellable_total,
                "percentage_share": sellable_percentage_share,
            },
            "unfulfillable": {
                "total": unfulfillable_total,
                "percentage_share": unfulfillable_percentage_share,
            },
        },

        "columns": {
            column: {
                "total": totals[column],
                "percentage_share": percentages[column]
            }
            for column in INVENTORY_AGE_COLUMNS
        }
    }

def construct_skuwise_monthly_table_candidates(user_id, country_key, month_name, year):
    """
    Supports both naming styles:
      skuwisemonthly_2_uk_april2026
      skuwisemonthly_2_uk_april2026_table
    """
    base_name = f"skuwisemonthly_{user_id}_{country_key}_{month_name}{year}"

    return [
        base_name,
        f"{base_name}_table",
    ]


def fetch_current_month_units_sold_map(user_id, country_key, month_name, year):
    """
    Reads monthly SKU sales table and returns units sold by SKU.

    Preferred sold value:
      total_quantity

    Fallback:
      quantity - return_quantity

    Final fallback:
      quantity

    Also returns product_name by SKU so we can append missing SKUs
    when inventory_aged_history only has aged inventory rows.
    """

    inspector = inspect(primary_engine)
    existing_tables = set(inspector.get_table_names(schema="public"))

    table_name = None

    for candidate in construct_skuwise_monthly_table_candidates(
        user_id=user_id,
        country_key=country_key,
        month_name=month_name,
        year=year,
    ):
        if candidate in existing_tables:
            table_name = candidate
            break

    if not table_name:
        return {
            "table_name": None,
            "units_by_sku": {},
            "product_by_sku": {},
            "total_units": 0,
            "error": "SKU wise monthly table not found",
        }

    if not is_safe_identifier(table_name):
        return {
            "table_name": table_name,
            "units_by_sku": {},
            "product_by_sku": {},
            "total_units": 0,
            "error": "Invalid SKU wise monthly table name",
        }

    query = text(f'SELECT * FROM "{table_name}" ORDER BY id ASC')

    with primary_engine.connect() as connection:
        df = pd.read_sql(query, connection)

    if df is None or df.empty:
        return {
            "table_name": table_name,
            "units_by_sku": {},
            "product_by_sku": {},
            "total_units": 0,
            "error": None,
        }

    if "sku" not in df.columns:
        return {
            "table_name": table_name,
            "units_by_sku": {},
            "product_by_sku": {},
            "total_units": 0,
            "error": "sku column not found",
        }

    units_by_sku = {}
    product_by_sku = {}
    total_units = 0

    for _, row in df.iterrows():
        sku = normalize_sku_key(row.get("sku"))
        product_name = clean_text_value(row.get("product_name"))

        is_total = sku == "" or product_name.lower() in {"total", "grand total"}

        quantity = to_number(row.get("quantity"))
        return_quantity = to_number(row.get("return_quantity"))
        total_quantity = to_number(row.get("total_quantity"))

        if total_quantity:
            units_sold = total_quantity
        elif quantity or return_quantity:
            units_sold = quantity - return_quantity
        else:
            units_sold = quantity

        if is_total:
            total_units = units_sold
            continue

        if sku:
            units_by_sku[sku] = units_by_sku.get(sku, 0) + units_sold

            if sku not in product_by_sku:
                product_by_sku[sku] = product_name

    if total_units == 0:
        total_units = sum(units_by_sku.values())

    return {
        "table_name": table_name,
        "units_by_sku": units_by_sku,
        "product_by_sku": product_by_sku,
        "total_units": total_units,
        "error": None,
    }


def fetch_product_name_map_from_sku_master(user_id, country_key):
    """
    Read product names from DATABASE_URL table:
      public.sku_{user_id}_data_table

    Country-specific SKU columns are preferred, for example:
      US -> sku_us
      UK -> sku_uk
      CA -> sku_canada / sku_ca
    """
    table_name = f"sku_{user_id}_data_table"

    if not is_safe_identifier(table_name):
        return {
            "table_name": table_name,
            "product_by_sku": {},
            "sku_column": None,
            "error": "Invalid SKU master table name",
        }

    inspector = inspect(primary_engine)
    existing_tables = set(inspector.get_table_names(schema="public"))

    if table_name not in existing_tables:
        return {
            "table_name": table_name,
            "product_by_sku": {},
            "sku_column": None,
            "error": f"SKU master table not found: {table_name}",
        }

    columns = {
        column["name"]
        for column in inspector.get_columns(table_name, schema="public")
    }

    country = str(country_key or "").strip().lower()
    sku_candidates_by_country = {
        "us": ["sku_us", "sku_usa", "sku"],
        "usa": ["sku_us", "sku_usa", "sku"],
        "uk": ["sku_uk", "sku_gb", "sku"],
        "gb": ["sku_uk", "sku_gb", "sku"],
        "ca": ["sku_canada", "sku_ca", "sku"],
        "canada": ["sku_canada", "sku_ca", "sku"],
    }
    sku_candidates = sku_candidates_by_country.get(
        country,
        [f"sku_{country}", "sku"],
    )
    sku_column = next(
        (candidate for candidate in sku_candidates if candidate in columns),
        None,
    )

    if not sku_column:
        return {
            "table_name": table_name,
            "product_by_sku": {},
            "sku_column": None,
            "error": f"Country SKU column not found for: {country_key}",
        }

    if "product_name" not in columns:
        return {
            "table_name": table_name,
            "product_by_sku": {},
            "sku_column": sku_column,
            "error": "product_name column not found in SKU master table",
        }

    query = text(
        f'SELECT "{sku_column}" AS sku, product_name '
        f'FROM public."{table_name}" '
        f'WHERE "{sku_column}" IS NOT NULL'
    )

    with primary_engine.connect() as connection:
        result_rows = connection.execute(query).mappings().all()

    product_by_sku = {}

    for row in result_rows:
        sku = normalize_sku_key(row.get("sku"))
        product_name = clean_text_value(row.get("product_name"))

        if not sku or not product_name:
            continue

        product_by_sku.setdefault(sku, product_name)

    return {
        "table_name": table_name,
        "product_by_sku": product_by_sku,
        "sku_column": sku_column,
        "error": None,
    }


def attach_current_month_units_sold(rows, columns, user_id, country_key, month_name, year):
    """
    Adds Current Month Units Sold (<Month>) to inventory rows.

    If source is inventory_aged_history, it only contains SKUs with aged inventory.
    So this function also appends missing SKUs from skuwisemonthly table.
    """

    month_display = month_label(MONTH_NAME_TO_NUMBER[month_name])
    units_column = f"Current Month Units Sold ({month_display})"

    existing_units_column = None

    for column in columns:
        if str(column).startswith("Current Month Units Sold"):
            existing_units_column = column
            break

    # If currentinventory table already has units sold, do not overwrite it.
    if existing_units_column:
        return {
            "rows": rows,
            "columns": columns,
            "units_column": existing_units_column,
            "units_source_table": None,
            "units_source_error": None,
        }

    sold_result = fetch_current_month_units_sold_map(
        user_id=user_id,
        country_key=country_key,
        month_name=month_name,
        year=year,
    )

    units_by_sku = sold_result["units_by_sku"]
    monthly_product_by_sku = sold_result["product_by_sku"]
    total_units = sold_result["total_units"]

    # Fill product names from DATABASE_URL master SKU table.
    sku_master_result = fetch_product_name_map_from_sku_master(
        user_id=user_id,
        country_key=country_key,
    )
    master_product_by_sku = sku_master_result["product_by_sku"]

    # SKU master has priority; monthly table is the fallback.
    product_by_sku = dict(monthly_product_by_sku)
    product_by_sku.update(master_product_by_sku)

    updated_rows = []
    existing_skus = set()
    total_row = None

    # First update existing inventory rows
    for row in rows:
        new_row = dict(row)

        if is_total_row(new_row):
            total_row = new_row
            continue

        sku = normalize_sku_key(get_row_sku(new_row))

        if sku:
            existing_skus.add(sku)

        # Fill missing product names from the SKU-wise monthly table.
        # This fixes currentinventory rows where Product Name is SQL NULL.
        current_product_name = clean_text_value(
            new_row.get("Product Name") or new_row.get("product_name")
        )
        mapped_product_name = clean_text_value(product_by_sku.get(sku))

        if not current_product_name and mapped_product_name:
            new_row["Product Name"] = mapped_product_name
        elif current_product_name:
            new_row["Product Name"] = current_product_name

        new_row[units_column] = units_by_sku.get(sku, 0)
        updated_rows.append(new_row)

    # Append SKUs that exist in skuwisemonthly but not in inventory_aged_history
    for sku, units_sold in units_by_sku.items():
        if sku in existing_skus:
            continue

        updated_rows.append({
            "SKU": sku,
            "Product Name": clean_text_value(product_by_sku.get(sku)),
            "inv-age-0-to-90-days": 0,
            "inv-age-91-to-180-days": 0,
            "inv-age-181-to-270-days": 0,
            "inv-age-271-to-365-days": 0,
            "inv-age-365-plus-days": 0,
            "unfulfillable-quantity": 0,
            "estimated-storage-cost-next-month": 0,
            "snapshot_date": None,
            units_column: units_sold,
        })

    # Add total row at the end
    if total_row is None:
        total_row = {
            "SKU": "",
            "Product Name": "Total",
            "inv-age-0-to-90-days": sum(to_number(r.get("inv-age-0-to-90-days")) for r in updated_rows),
            "inv-age-91-to-180-days": sum(to_number(r.get("inv-age-91-to-180-days")) for r in updated_rows),
            "inv-age-181-to-270-days": sum(to_number(r.get("inv-age-181-to-270-days")) for r in updated_rows),
            "inv-age-271-to-365-days": sum(to_number(r.get("inv-age-271-to-365-days")) for r in updated_rows),
            "inv-age-365-plus-days": sum(to_number(r.get("inv-age-365-plus-days")) for r in updated_rows),
            "unfulfillable-quantity": sum(to_number(r.get("unfulfillable-quantity")) for r in updated_rows),
            "estimated-storage-cost-next-month": sum(to_number(r.get("estimated-storage-cost-next-month")) for r in updated_rows),
            "snapshot_date": None,
        }
    else:
        total_row = dict(total_row)

    total_row[units_column] = total_units
    updated_rows.append(total_row)

    updated_columns = list(columns)

    if units_column not in updated_columns:
        updated_columns.append(units_column)

    return {
        "rows": updated_rows,
        "columns": updated_columns,
        "units_column": units_column,
        "units_source_table": sold_result["table_name"],
        "units_source_error": sold_result["error"],
        "product_name_source_table": sku_master_result["table_name"],
        "product_name_source_column": sku_master_result["sku_column"],
        "product_name_source_error": sku_master_result["error"],
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


def get_inventory_current_candidate_months(range_type, month_name=None, quarter=None, year=None, today=None):
    
    today = today or date.today()

    range_type = str(range_type or "monthly").strip().lower()
    month_name = str(month_name or "").strip().lower()
    quarter = str(quarter or "").strip().lower()

    year_int = None
    if year:
        try:
            year_int = int(year)
        except (TypeError, ValueError):
            year_int = None

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

        candidate_months = list(QUARTER_MONTHS.get(quarter, []))

        current_month_name = calendar_month_name[today.month].lower()

        # Skip ongoing/current month only when selected year is current year
        # and current month belongs to selected quarter.
        if year_int == today.year and current_month_name in candidate_months:
            candidate_months = [
                month
                for month in candidate_months
                if month != current_month_name
            ]

        return candidate_months

    if range_type == "yearly":
        prev_month_num, prev_year = get_previous_completed_month(today=today)
        return [calendar_month_name[prev_month_num].lower()]

    if month_name:
        requested_month_number = MONTH_NAME_TO_NUMBER.get(month_name)

        # For the ongoing month, try the requested month first and then
        # automatically fall back to earlier months in the same year.
        # Example on 1 August 2026:
        #   august -> july -> june -> ... -> january
        # This allows the dashboard to keep showing the latest available
        # currentinventory table until the new month's table is created.
        if (
            requested_month_number
            and year_int == today.year
            and requested_month_number == today.month
        ):
            return [
                calendar_month_name[month_number].lower()
                for month_number in range(requested_month_number, 0, -1)
            ]

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

    Important:
      Use only latest snapshot date inside the selected month.
      This avoids 5x / duplicate monthly snapshot totals.
    """

    month_number = MONTH_NAME_TO_NUMBER.get(month_name)

    if not month_number:
        return [], []

    marketplace_id = get_marketplace_id(country_key)

    params = {
        "user_id": int(user_id),
        "year": int(year),
        "month": int(month_number),
    }

    if marketplace_id:
        latest_country_filter_sql = "AND marketplace_id = :marketplace_id"
        row_country_filter_sql = "AND h.marketplace_id = :marketplace_id"
        params["marketplace_id"] = marketplace_id
    else:
        latest_country_filter_sql = "AND LOWER(COALESCE(country, '')) = :country_key"
        row_country_filter_sql = "AND LOWER(COALESCE(h.country, '')) = :country_key"
        params["country_key"] = country_key.lower()

    query = text(f"""
        WITH latest_snapshot AS (
            SELECT MAX(snapshot_date::date) AS latest_snapshot_day
            FROM public.inventory_aged_history
            WHERE user_id = :user_id
              {latest_country_filter_sql}
              AND snapshot_date IS NOT NULL
              AND EXTRACT(YEAR FROM snapshot_date)::int = :year
              AND EXTRACT(MONTH FROM snapshot_date)::int = :month
        )
        SELECT
            h.sku,
            COALESCE(h.product_name, '') AS product_name,
            h.surcharge_age_tier,
            COALESCE(SUM(h.qty_charged), 0) AS qty_charged,
            COALESCE(SUM(h.amount_charged), 0) AS amount_charged,
            MAX(h.snapshot_date) AS snapshot_date
        FROM public.inventory_aged_history h
        CROSS JOIN latest_snapshot ls
        WHERE h.user_id = :user_id
          {row_country_filter_sql}
          AND h.snapshot_date IS NOT NULL
          AND h.snapshot_date::date = ls.latest_snapshot_day
          AND EXTRACT(YEAR FROM h.snapshot_date)::int = :year
          AND EXTRACT(MONTH FROM h.snapshot_date)::int = :month
        GROUP BY h.sku, h.product_name, h.surcharge_age_tier
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

        grouped[key]["estimated-storage-cost-next-month"] += to_number(
            row["amount_charged"]
        )

    rows = list(grouped.values())

    total_row = {
        "SKU": "",
        "Product Name": "Total",
        "inv-age-0-to-90-days": sum(
            to_number(r.get("inv-age-0-to-90-days")) for r in rows
        ),
        "inv-age-91-to-180-days": sum(
            to_number(r.get("inv-age-91-to-180-days")) for r in rows
        ),
        "inv-age-181-to-270-days": sum(
            to_number(r.get("inv-age-181-to-270-days")) for r in rows
        ),
        "inv-age-271-to-365-days": sum(
            to_number(r.get("inv-age-271-to-365-days")) for r in rows
        ),
        "inv-age-365-plus-days": sum(
            to_number(r.get("inv-age-365-plus-days")) for r in rows
        ),
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


def build_inventory_monthly_table_name(user_id, country_key, month_name, year):
    month_number = MONTH_NAME_TO_NUMBER.get(str(month_name).lower())

    if not month_number:
        return None

    return f"inventorymonthly_{user_id}_{country_key}_{month_number:02d}_{year}"


def sync_inventory_monthly_product_names(user_id, country_key, month_name, year):
    """
    Permanently fill blank product_name values in the Amazon DB table:
      inventorymonthly_{user_id}_{country}_{MM}_{YYYY}

    Names are read from DATABASE_URL table:
      sku_{user_id}_data_table

    Only NULL/blank/null-like product_name values are updated.
    """
    table_name = build_inventory_monthly_table_name(
        user_id=user_id,
        country_key=country_key,
        month_name=month_name,
        year=year,
    )

    if not table_name or not is_safe_identifier(table_name):
        return {
            "success": False,
            "updated_rows": 0,
            "source_table": None,
            "target_table": table_name,
            "error": "Invalid inventory monthly table name",
        }

    master_result = fetch_product_name_map_from_sku_master(
        user_id=user_id,
        country_key=country_key,
    )
    product_by_sku = master_result.get("product_by_sku", {})

    if master_result.get("error"):
        return {
            "success": False,
            "updated_rows": 0,
            "source_table": master_result.get("table_name"),
            "target_table": table_name,
            "error": master_result.get("error"),
        }

    if not product_by_sku:
        return {
            "success": True,
            "updated_rows": 0,
            "source_table": master_result.get("table_name"),
            "target_table": table_name,
            "error": None,
        }

    inspector = inspect(amazon_engine)
    existing_tables = set(inspector.get_table_names(schema="public"))
    if table_name not in existing_tables:
        return {
            "success": False,
            "updated_rows": 0,
            "source_table": master_result.get("table_name"),
            "target_table": table_name,
            "error": f"Inventory monthly table not found: {table_name}",
        }

    columns = {
        column["name"]
        for column in inspector.get_columns(table_name, schema="public")
    }
    target_sku_column = next(
        (candidate for candidate in ["msku", "sku", "seller_sku", "SKU"] if candidate in columns),
        None,
    )

    if not target_sku_column or "product_name" not in columns:
        return {
            "success": False,
            "updated_rows": 0,
            "source_table": master_result.get("table_name"),
            "target_table": table_name,
            "error": "Required SKU or product_name column not found in inventory monthly table",
        }

    update_query = text(f"""
        UPDATE public."{table_name}"
        SET product_name = :product_name
        WHERE UPPER(TRIM(CAST("{target_sku_column}" AS TEXT))) = :normalized_sku
          AND (
                product_name IS NULL
                OR TRIM(CAST(product_name AS TEXT)) = ''
                OR LOWER(TRIM(CAST(product_name AS TEXT)))
                   IN ('nan', 'none', 'null', '[null]', '<na>')
          )
    """)

    updated_rows = 0
    with amazon_engine.begin() as connection:
        for normalized_sku, product_name in product_by_sku.items():
            result = connection.execute(update_query, {
                "normalized_sku": normalize_sku_key(normalized_sku),
                "product_name": clean_text_value(product_name),
            })
            updated_rows += max(result.rowcount or 0, 0)

    return {
        "success": True,
        "updated_rows": updated_rows,
        "source_table": master_result.get("table_name"),
        "source_sku_column": master_result.get("sku_column"),
        "target_table": table_name,
        "target_sku_column": target_sku_column,
        "error": None,
    }


def fetch_inventory_monthly_qty_map(user_id, country_key, month_name, year):
    """
    Reads public.inventorymonthly_{user}_{country}_{MM}_{YYYY}

    Mapping:
      Sellable Units = sellable_sum_last
      Inbound Units = transit_total
      Unfulfillable Units =
          defective_sum_last
        + warehouse_damaged_sum_last
        + expired_sum_last
        + customer_damaged_sum_last
        + distributor_damaged_sum_last
    """

    table_name = build_inventory_monthly_table_name(
        user_id=user_id,
        country_key=country_key,
        month_name=month_name,
        year=year,
    )

    if not table_name:
        return {
            "table_name": None,
            "data_by_sku": {},
            "totals": {
                "sellable": 0,
                "inbound": 0,
                "unfulfillable": 0,
                "inventory_coverage_ratio": 0,
            },
            "error": "Invalid month",
        }

    if not is_safe_identifier(table_name):
        return {
            "table_name": table_name,
            "data_by_sku": {},
            "totals": {
                "sellable": 0,
                "inbound": 0,
                "unfulfillable": 0,
                "inventory_coverage_ratio": 0,
            },
            "error": "Invalid inventory monthly table name",
        }

    inspector = inspect(amazon_engine)
    existing_tables = set(inspector.get_table_names(schema="public"))

    if table_name not in existing_tables:
        return {
            "table_name": table_name,
            "data_by_sku": {},
            "totals": {
                "sellable": 0,
                "inbound": 0,
                "unfulfillable": 0,
                "inventory_coverage_ratio": 0,
            },
            "error": f"Inventory monthly table not found: {table_name}",
        }

    # Permanently backfill missing names in the physical inventorymonthly table.
    product_name_sync = sync_inventory_monthly_product_names(
        user_id=user_id,
        country_key=country_key,
        month_name=month_name,
        year=year,
    )

    query = text(f"""
        SELECT *
        FROM public.{table_name}
        ORDER BY id ASC
    """)

    with amazon_engine.connect() as connection:
        df = pd.read_sql(query, connection)

    if df is None or df.empty:
        return {
            "table_name": table_name,
            "data_by_sku": {},
            "totals": {
                "sellable": 0,
                "inbound": 0,
                "unfulfillable": 0,
                "inventory_coverage_ratio": 0,
            },
            "error": None,
        }

    sku_col = None
    for candidate in ["msku", "sku", "seller_sku", "SKU"]:
        if candidate in df.columns:
            sku_col = candidate
            break

    if not sku_col:
        return {
            "table_name": table_name,
            "data_by_sku": {},
            "totals": {
                "sellable": 0,
                "inbound": 0,
                "unfulfillable": 0,
                "inventory_coverage_ratio": 0,
            },
            "error": "SKU column not found in inventory monthly table",
        }

    required_columns = [
        "sellable_sum_last",
        "transit_total",
        "inventory_coverage_ratio",
        "defective_sum_last",
        "warehouse_damaged_sum_last",
        "expired_sum_last",
        "customer_damaged_sum_last",
        "distributor_damaged_sum_last",
    ]

    for col in required_columns:
        if col not in df.columns:
            df[col] = 0

    data_by_sku = {}

    for _, row in df.iterrows():
        sku = normalize_sku_key(row.get(sku_col))
        product_name = str(row.get("product_name") or "").strip().lower()

        if (
            not sku
            or sku in ["TOTAL", "GRAND TOTAL"]
            or product_name in ["total", "grand total"]
        ):
            continue

        sellable_units = to_number(row.get("sellable_sum_last"))
        inbound_units = to_number(row.get("transit_total"))
        inventory_coverage_ratio = to_number(row.get("inventory_coverage_ratio"))

        unfulfillable_units = (
            to_number(row.get("defective_sum_last"))
            + to_number(row.get("warehouse_damaged_sum_last"))
            + to_number(row.get("expired_sum_last"))
            + to_number(row.get("customer_damaged_sum_last"))
            + to_number(row.get("distributor_damaged_sum_last"))
        )

        data_by_sku[sku] = {
            "sellable_units": sellable_units,
            "inbound_units": inbound_units,
            "unfulfillable_units": unfulfillable_units,
            "inventory_coverage_ratio": inventory_coverage_ratio,
        }

    coverage_values = [
        to_number(v.get("inventory_coverage_ratio"))
        for v in data_by_sku.values()
        if to_number(v.get("inventory_coverage_ratio")) > 0
    ]

    totals = {
        "sellable": sum(v["sellable_units"] for v in data_by_sku.values()),
        "inbound": sum(v["inbound_units"] for v in data_by_sku.values()),
        "unfulfillable": sum(v["unfulfillable_units"] for v in data_by_sku.values()),
        "inventory_coverage_ratio": (
            sum(coverage_values) / len(coverage_values)
            if coverage_values
            else 0
        ),
    }

    return {
        "table_name": table_name,
        "data_by_sku": data_by_sku,
        "totals": totals,
        "product_name_sync": product_name_sync,
        "error": None,
    }


def attach_inventory_monthly_qty(
    rows,
    columns,
    user_id,
    country_key,
    month_name,
    year,
    use_currentinventory_sellable=False,
):
    monthly_result = fetch_inventory_monthly_qty_map(
        user_id=user_id,
        country_key=country_key,
        month_name=month_name,
        year=year,
    )

    data_by_sku = monthly_result["data_by_sku"]

    updated_rows = []

    for row in rows:
        new_row = dict(row)

        if is_total_row(new_row):
            # Do not overwrite total row from inventorymonthly.
            # Final total values are recalculated after SKU rows are processed.
            updated_rows.append(new_row)
            continue

        sku = normalize_sku_key(get_row_sku(new_row))
        monthly_data = data_by_sku.get(sku)

        if use_currentinventory_sellable:
            # When currentinventory table exists:
            # Sellable Units = available + fc-transfer
            existing_sellable_units = (
                to_number(new_row.get("available"))
                + to_number(new_row.get("fc-transfer"))
            )
        else:
            # When currentinventory table does not exist:
            # fallback uses inventorymonthly sellable_sum_last
            existing_sellable_units = get_first_number(
                new_row,
                [
                    "Sellable Units",
                    "sellable_sum_last",
                    "available",
                    "ending_total",
                ],
            )

        existing_inbound_units = get_first_number(
            new_row,
            [
                "Inbound Units",
                "inbound_quantity",
                "inboundQuantity",
                "Inbound Quantity",
                "transit_total",
            ],
        )

        existing_unfulfillable_units = get_first_number(
            new_row,
            [
                "unfulfillable-quantity",
                "Unfulfillable Units",
                "unfulfillable_quantity",
            ],
        )

        existing_coverage_ratio = get_first_number(
            new_row,
            [
                "inventory_coverage_ratio",
                "Coverage Ratio (In Months)",
                "coverage_ratio_months",
            ],
        )

        if monthly_data:
            if use_currentinventory_sellable:
                sellable_units = existing_sellable_units
            else:
                sellable_units = monthly_data.get("sellable_units", existing_sellable_units)

            if use_currentinventory_sellable:
                inbound_units = existing_inbound_units
            else:
                inbound_units = monthly_data.get("inbound_units", existing_inbound_units)

            if use_currentinventory_sellable:
                unfulfillable_units = existing_unfulfillable_units
            else:
                unfulfillable_units = monthly_data.get(
                    "unfulfillable_units",
                    existing_unfulfillable_units,
                )

            if use_currentinventory_sellable:
                # When currentinventory table exists:
                # keep Coverage Ratio from currentinventory table
                coverage_ratio = existing_coverage_ratio
            else:
                # When currentinventory table does not exist:
                # fallback uses inventorymonthly coverage ratio
                coverage_ratio = monthly_data.get(
                    "inventory_coverage_ratio",
                    existing_coverage_ratio,
                )
        else:
            sellable_units = existing_sellable_units
            inbound_units = existing_inbound_units
            unfulfillable_units = existing_unfulfillable_units
            coverage_ratio = existing_coverage_ratio

        new_row["Sellable Units"] = sellable_units
        new_row["Inbound Units"] = inbound_units
        new_row["unfulfillable-quantity"] = unfulfillable_units
        new_row["inventory_coverage_ratio"] = coverage_ratio
        new_row["Coverage Ratio (In Months)"] = coverage_ratio

        updated_rows.append(new_row)

    sellable_total = sum(
        to_number(r.get("Sellable Units"))
        for r in updated_rows
        if not is_total_row(r)
    )

    inbound_total = sum(
        to_number(r.get("Inbound Units"))
        for r in updated_rows
        if not is_total_row(r)
    )

    unfulfillable_total = sum(
        to_number(r.get("unfulfillable-quantity"))
        for r in updated_rows
        if not is_total_row(r)
    )

    coverage_values = [
        to_number(r.get("inventory_coverage_ratio"))
        for r in updated_rows
        if not is_total_row(r)
        and to_number(r.get("inventory_coverage_ratio")) > 0
    ]

    coverage_ratio_avg = (
        sum(coverage_values) / len(coverage_values)
        if coverage_values
        else 0
    )

    for row in updated_rows:
        if is_total_row(row):
            row["Sellable Units"] = sellable_total
            row["Inbound Units"] = inbound_total
            row["unfulfillable-quantity"] = unfulfillable_total

            if use_currentinventory_sellable:
                # keep total coverage ratio from currentinventory table
                total_coverage_ratio = get_first_number(
                    row,
                    [
                        "Coverage Ratio (In Months)",
                        "inventory_coverage_ratio",
                        "coverage_ratio_months",
                    ],
                )
                row["inventory_coverage_ratio"] = total_coverage_ratio
                row["Coverage Ratio (In Months)"] = total_coverage_ratio
            else:
                row["inventory_coverage_ratio"] = coverage_ratio_avg
                row["Coverage Ratio (In Months)"] = coverage_ratio_avg

    updated_columns = list(columns or [])

    for col in [
        "Sellable Units",
        "Inbound Units",
        "unfulfillable-quantity",
        "inventory_coverage_ratio",
        "Coverage Ratio (In Months)",
    ]:
        if col not in updated_columns:
            updated_columns.append(col)

    return {
        "rows": updated_rows,
        "columns": updated_columns,
        "inventory_monthly_source_table": monthly_result["table_name"],
        "inventory_monthly_source_error": monthly_result["error"],
    }



def attach_zero_to_180_days(rows, columns, use_currentinventory_split=False):
    updated_rows = []

    for row in rows:
        new_row = dict(row)

        inv_0_90 = to_number(new_row.get("inv-age-0-to-90-days"))
        inv_91_180 = to_number(new_row.get("inv-age-91-to-180-days"))

        inv_181_270 = to_number(new_row.get("inv-age-181-to-270-days"))
        inv_271_365 = to_number(new_row.get("inv-age-271-to-365-days"))
        inv_365_plus = to_number(new_row.get("inv-age-365-plus-days"))

        if use_currentinventory_split:
            # Currentinventory table has real split buckets.
            zero_to_180 = inv_0_90 + inv_91_180
        else:
            # inventory_aged_history fallback does not have 0-90 / 91-180.
            # So infer 0-180 from sellable units minus older ageing buckets.
            sellable_units = to_number(new_row.get("Sellable Units"))

            zero_to_180 = (
                sellable_units
                - inv_181_270
                - inv_271_365
                - inv_365_plus
            )

            if zero_to_180 < 0:
                zero_to_180 = 0

        new_row["inv-age-0-to-180-days"] = zero_to_180
        updated_rows.append(new_row)

    total_0_90 = sum(
        to_number(r.get("inv-age-0-to-90-days"))
        for r in updated_rows
        if not is_total_row(r)
    )

    total_91_180 = sum(
        to_number(r.get("inv-age-91-to-180-days"))
        for r in updated_rows
        if not is_total_row(r)
    )

    total_0_180 = sum(
        to_number(r.get("inv-age-0-to-180-days"))
        for r in updated_rows
        if not is_total_row(r)
    )

    for row in updated_rows:
        if is_total_row(row):
            row["inv-age-0-to-90-days"] = total_0_90
            row["inv-age-91-to-180-days"] = total_91_180
            row["inv-age-0-to-180-days"] = total_0_180

    updated_columns = list(columns or [])

    for col in [
        "inv-age-0-to-90-days",
        "inv-age-91-to-180-days",
        "inv-age-0-to-180-days",
    ]:
        if col not in updated_columns:
            insert_before = "inv-age-181-to-270-days"

            if insert_before in updated_columns:
                updated_columns.insert(updated_columns.index(insert_before), col)
            else:
                updated_columns.append(col)

    return {
        "rows": updated_rows,
        "columns": updated_columns,
    }


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

            monthly_attach = attach_inventory_monthly_qty(
                rows=rows,
                columns=columns,
                user_id=user_id,
                country_key=country_key,
                month_name=candidate_month,
                year=year,
                use_currentinventory_sellable=True,
            )

            return {
                "found": True,
                "source_type": "currentinventory_table",
                "source_name": table_name,
                "selected_month": candidate_month,
                "columns": monthly_attach["columns"],
                "rows": monthly_attach["rows"],
                "tried_sources": tried_sources,
                "inventory_monthly_source_table": monthly_attach["inventory_monthly_source_table"],
                "inventory_monthly_source_error": monthly_attach["inventory_monthly_source_error"],
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
            monthly_attach = attach_inventory_monthly_qty(
                rows=rows,
                columns=columns,
                user_id=user_id,
                country_key=country_key,
                month_name=candidate_month,
                year=year,
            )

            return {
                "found": True,
                "source_type": "inventory_aged_history",
                "source_name": "public.inventory_aged_history",
                "selected_month": candidate_month,
                "columns": monthly_attach["columns"],
                "rows": monthly_attach["rows"],
                "tried_sources": tried_sources,
                "inventory_monthly_source_table": monthly_attach["inventory_monthly_source_table"],
                "inventory_monthly_source_error": monthly_attach["inventory_monthly_source_error"],
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

def fetch_high_alert_threshold(user_id: int, country_key: str):
    """
    Return the main High Alert threshold in months.

    Country profile values are stored in weeks:
      Sea threshold = ship_time_weeks + stock_unit_weeks

    The current inventory coverage column is still expressed in months,
    so the weekly threshold is converted to months before comparison.
    """
    query = text("""
        SELECT
            ship_time_weeks,
            air_time_weeks,
            stock_unit_weeks
        FROM public.country_profile
        WHERE user_id = :user_id
          AND LOWER(country) = :country
        LIMIT 1
    """)

    try:
        with primary_engine.connect() as conn:
            row = conn.execute(query, {
                "user_id": int(user_id),
                "country": str(country_key or "").strip().lower(),
            }).mappings().first()
    except Exception as exc:
        print("[WARN] Failed to fetch high alert threshold:", exc)
        return None

    if not row:
        return None

    ship_time_weeks = pd.to_numeric(
        row.get("ship_time_weeks"),
        errors="coerce",
    )
    stock_unit_weeks = pd.to_numeric(
        row.get("stock_unit_weeks"),
        errors="coerce",
    )

    if pd.isna(ship_time_weeks) or pd.isna(stock_unit_weeks):
        return None

    threshold_weeks = float(ship_time_weeks) + float(stock_unit_weeks)
    threshold_months = threshold_weeks / 4.345

    return round(threshold_months, 2)

def build_high_alert_coverage_summary(rows, user_id, country_key):
    """
    Calculates average coverage ratio only for High Alert SKUs.

    High Alert logic:
      coverage_ratio <= (ship_time_weeks + stock_unit_weeks) / 4.345

    Fallback:
      if the weekly policy is missing, threshold = 2.0 months
    """

    high_alert_threshold = fetch_high_alert_threshold(user_id, country_key)

    if high_alert_threshold is None:
        high_alert_threshold = 2.0

    high_alert_items = []
    coverage_values = []

    for row in rows:
        if is_total_row(row):
            continue

        sku = str(row.get("SKU") or "").strip()
        product_name = str(row.get("Product Name") or "").strip()

        coverage_ratio = None

        # Main column from currentinventory table
        for col in [
            "Coverage Ratio (In Months)",
            "coverage_ratio_months",
            "inventory_coverage_ratio",
        ]:
            if col in row:
                cov = pd.to_numeric(row.get(col), errors="coerce")
                if pd.notna(cov):
                    coverage_ratio = float(cov)
                    break

        if coverage_ratio is None:
            continue

        if coverage_ratio > 0 and coverage_ratio <= high_alert_threshold:
            coverage_values.append(coverage_ratio)

            high_alert_items.append({
                "sku": clean_value(sku),
                "product_name": clean_value(product_name),
                "coverage_ratio_months": round(float(coverage_ratio), 2),
                "high_alert_threshold": round(float(high_alert_threshold), 2),
                "alert": "High alert",
            })

    average_coverage_ratio = (
        round(float(sum(coverage_values) / len(coverage_values)), 2)
        if coverage_values
        else 0
    )

    return {
        "high_alert_sku_count": len(high_alert_items),
        "average_coverage_ratio": average_coverage_ratio,
        "high_alert_threshold": round(float(high_alert_threshold), 2),
        "items": high_alert_items,
    }

def merge_inventory_columns(existing_columns, new_columns):
    merged = list(existing_columns or [])

    for col in new_columns or []:
        if col not in merged:
            merged.append(col)

    return merged


def rebuild_total_row(rows):
    data_rows = [
        r for r in rows
        if not is_total_row(r)
    ]

    total_row = {
        "SKU": "",
        "Product Name": "Total",
    }

    numeric_columns = [
        "inv-age-0-to-180-days",
        "inv-age-0-to-90-days",
        "inv-age-91-to-180-days",
        "inv-age-181-to-270-days",
        "inv-age-271-to-365-days",
        "inv-age-365-plus-days",
        "Sellable Units",
        "Inbound Units",
        "unfulfillable-quantity",
        "estimated-storage-cost-next-month",
    ]

    for col in numeric_columns:
        total_row[col] = sum(to_number(r.get(col)) for r in data_rows)

    coverage_values = [
        to_number(r.get("inventory_coverage_ratio"))
        for r in data_rows
        if to_number(r.get("inventory_coverage_ratio")) > 0
    ]

    coverage_ratio_avg = (
        sum(coverage_values) / len(coverage_values)
        if coverage_values
        else 0
    )

    total_row["inventory_coverage_ratio"] = coverage_ratio_avg
    total_row["Coverage Ratio (In Months)"] = coverage_ratio_avg

    for row in data_rows:
        for col in row.keys():
            if str(col).startswith("Current Month Units Sold"):
                total_row[col] = sum(to_number(r.get(col)) for r in data_rows)

    total_row["snapshot_date"] = None

    return total_row


def resolve_inventory_current_source_global(user_id, range_type, month_name, year, quarter):
    combined_rows = []
    combined_columns = []
    combined_sources = []
    tried_sources = []
    selected_month = None

    for child_country in GLOBAL_COUNTRIES:
        source_result = resolve_inventory_current_source(
            user_id=user_id,
            country_key=child_country,
            range_type=range_type,
            month_name=month_name,
            year=year,
            quarter=quarter,
        )

        tried_sources.extend(source_result.get("tried_sources", []))

        if not source_result["found"]:
            continue

        rows = source_result["rows"]
        columns = source_result["columns"]

        selected_month_for_units = source_result["selected_month"]

        if selected_month_for_units:
            units_result = attach_current_month_units_sold(
                rows=rows,
                columns=columns,
                user_id=user_id,
                country_key=child_country,
                month_name=selected_month_for_units,
                year=year,
            )

            rows = units_result["rows"]
            columns = units_result["columns"]

        for row in rows:
            if is_total_row(row):
                continue

            new_row = dict(row)
            new_row["country_key"] = child_country
            combined_rows.append(new_row)

        combined_columns = merge_inventory_columns(combined_columns, columns)

        if "country_key" not in combined_columns:
            combined_columns.insert(0, "country_key")

        combined_sources.append({
            "country_key": child_country,
            "source_type": source_result["source_type"],
            "source_name": source_result["source_name"],
            "selected_month": source_result["selected_month"],
        })

        if selected_month is None:
            selected_month = source_result["selected_month"]

    if not combined_rows:
        return {
            "found": False,
            "source_type": None,
            "source_name": None,
            "selected_month": None,
            "columns": [],
            "rows": [],
            "tried_sources": tried_sources,
            "combined_sources": combined_sources,
        }

    combined_rows.append(rebuild_total_row(combined_rows))

    return {
        "found": True,
        "source_type": "global_combined",
        "source_name": "uk+us combined",
        "selected_month": selected_month,
        "columns": combined_columns,
        "rows": combined_rows,
        "tried_sources": tried_sources,
        "combined_sources": combined_sources,
    }


def resolve_inventory_current_source_global_separated(user_id, range_type, month_name, year, quarter):
    country_results = {}
    tried_sources = []

    for child_country in GLOBAL_COUNTRIES:
        source_result = resolve_inventory_current_source(
            user_id=user_id,
            country_key=child_country,
            range_type=range_type,
            month_name=month_name,
            year=year,
            quarter=quarter,
        )

        tried_sources.extend(source_result.get("tried_sources", []))

        if not source_result["found"]:
            country_results[child_country] = {
                "success": False,
                "country_key": child_country,
                "message": "No current inventory table or inventory_aged_history data found",
                "source_type": None,
                "source_name": None,
                "selected_month": None,
                "columns": [],
                "rows": [],
                "tried_sources": source_result.get("tried_sources", []),
            }
            continue

        rows = source_result["rows"]
        columns = source_result["columns"]
        selected_month_for_units = source_result["selected_month"]

        units_sold_result = {
            "units_column": None,
            "units_source_table": None,
            "units_source_error": None,
        }

        previous_sales_rank_result = {
            "previous_sales_rank_column": None,
            "previous_sales_rank_source_table": None,
            "previous_sales_rank_error": None,
        }

        if selected_month_for_units:
            units_sold_result = attach_current_month_units_sold(
                rows=rows,
                columns=columns,
                user_id=user_id,
                country_key=child_country,
                month_name=selected_month_for_units,
                year=year,
            )

            rows = units_sold_result["rows"]
            columns = units_sold_result["columns"]

            monthly_attach_after_units = attach_inventory_monthly_qty(
                rows=rows,
                columns=columns,
                user_id=user_id,
                country_key=child_country,
                month_name=selected_month_for_units,
                year=year,
                use_currentinventory_sellable=source_result["source_type"] == "currentinventory_table",
            )

            rows = monthly_attach_after_units["rows"]
            columns = monthly_attach_after_units["columns"]

            zero_to_180_attach = attach_zero_to_180_days(
                rows=rows,
                columns=columns,
                use_currentinventory_split=source_result["source_type"] == "currentinventory_table",
            )

            rows = zero_to_180_attach["rows"]
            columns = zero_to_180_attach["columns"]

            previous_sales_rank_result = attach_previous_month_sales_rank(
                rows=rows,
                columns=columns,
                user_id=user_id,
                country_key=child_country,
                month_name=selected_month_for_units,
                year=year,
            )

            rows = previous_sales_rank_result["rows"]
            columns = previous_sales_rank_result["columns"]

        categories = build_inventory_categories(rows)
        inventory_age_summary = build_inventory_age_summary(rows)

        high_alert_coverage_summary = build_high_alert_coverage_summary(
            rows=rows,
            user_id=user_id,
            country_key=child_country,
        )

        # Add Percentage row for global country-wise result also
        percentage_row_result = append_inventory_percentage_row(
            rows=rows,
            columns=columns,
        )

        rows = percentage_row_result["rows"]
        columns = percentage_row_result["columns"]

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
                country_key=child_country,
                month_number=selected_month_number,
                year_int=int(year),
            )

        categories["estimated_storage_cost"]["previous_storage_cost"] = previous_storage_cost["value"]
        categories["estimated_storage_cost"]["previous_storage_cost_source"] = previous_storage_cost["source_table"]
        categories["estimated_storage_cost"]["previous_storage_cost_period"] = previous_storage_cost["period"]
        categories["estimated_storage_cost"]["previous_storage_cost_error"] = previous_storage_cost["error"]

        country_results[child_country] = {
            "success": True,
            "country_key": child_country,
            "range_type": range_type,
            "requested_month": month_name or None,
            "requested_quarter": quarter or None,
            "selected_month": source_result["selected_month"],
            "year": int(year),

            "source_type": source_result["source_type"],
            "source_name": source_result["source_name"],
            "table_name": source_result["source_name"] if source_result["source_type"] == "currentinventory_table" else None,
            "units_sold_column": units_sold_result["units_column"],
            "units_sold_source_table": units_sold_result["units_source_table"],
            "units_sold_source_error": units_sold_result["units_source_error"],
            "previous_sales_rank_column": previous_sales_rank_result["previous_sales_rank_column"],
            "previous_sales_rank_source_table": previous_sales_rank_result["previous_sales_rank_source_table"],
            "previous_sales_rank_error": previous_sales_rank_result["previous_sales_rank_error"],
            "tried_sources": source_result["tried_sources"],

            "columns": columns,
            "rows": rows,
            "total_rows": len(rows),

            "categories": categories,
            "inventory_age_summary": inventory_age_summary,
            "high_alert_coverage_summary": high_alert_coverage_summary,
            "category_counts": {
                "liquidate": len(categories["liquidate"]["items"]),
                "discount": len(categories["discount"]["items"]),
                "monitor": len(categories["monitor"]["items"]),
                "unfulfillable": len(categories["unfulfillable"]["items"]),
                "estimated_storage_cost": len(categories["estimated_storage_cost"]["items"]),
            },
        }

    found_any = any(
        result.get("success") is True
        for result in country_results.values()
    )

    return {
        "found": found_any,
        "country_results": country_results,
        "tried_sources": tried_sources,
    }

def get_previous_month_name_year(month_name, year):
    current_month_number = MONTH_NAME_TO_NUMBER.get(str(month_name).strip().lower())

    if not current_month_number:
        return None, None, None

    year_int = int(year)

    if current_month_number == 1:
        previous_month_number = 12
        previous_year = year_int - 1
    else:
        previous_month_number = current_month_number - 1
        previous_year = year_int

    previous_month_name = calendar_month_name[previous_month_number].lower()

    return previous_month_name, previous_month_number, previous_year


def find_sales_rank_column(columns):
    sales_rank_candidates = [
        "sales-rank",
        "sales_rank",
        "Sales Rank",
        "Sales-Rank",
        "sales rank",
    ]

    normalized_map = {
        str(col).strip().lower().replace("_", "-"): col
        for col in columns
    }

    for candidate in sales_rank_candidates:
        key = candidate.strip().lower().replace("_", "-")
        if key in normalized_map:
            return normalized_map[key]

    for col in columns:
        col_text = str(col).strip().lower()
        if "sales" in col_text and "rank" in col_text:
            return col

    return None


def fetch_previous_month_sales_rank_map(user_id, country_key, month_name, year):
    previous_month_name, previous_month_number, previous_year = get_previous_month_name_year(
        month_name=month_name,
        year=year,
    )

    if not previous_month_name:
        return {
            "table_name": None,
            "previous_month": None,
            "previous_year": None,
            "sales_rank_by_sku": {},
            "error": "Invalid month_name",
        }

    previous_table_name = build_current_inventory_table_name(
        user_id=user_id,
        country_key=country_key,
        month_name=previous_month_name,
        year=previous_year,
    )

    if not is_safe_identifier(previous_table_name):
        return {
            "table_name": previous_table_name,
            "previous_month": previous_month_name,
            "previous_year": previous_year,
            "sales_rank_by_sku": {},
            "error": "Invalid previous current inventory table name",
        }

    inspector = inspect(primary_engine)
    existing_tables = set(inspector.get_table_names(schema="public"))

    if previous_table_name not in existing_tables:
        return {
            "table_name": previous_table_name,
            "previous_month": previous_month_name,
            "previous_year": previous_year,
            "sales_rank_by_sku": {},
            "error": f"Previous current inventory table not found: {previous_table_name}",
        }

    query = text(f'SELECT * FROM "{previous_table_name}"')

    with primary_engine.connect() as connection:
        df = pd.read_sql(query, connection)

    if df is None or df.empty:
        return {
            "table_name": previous_table_name,
            "previous_month": previous_month_name,
            "previous_year": previous_year,
            "sales_rank_by_sku": {},
            "error": None,
        }

    if "SKU" not in df.columns:
        return {
            "table_name": previous_table_name,
            "previous_month": previous_month_name,
            "previous_year": previous_year,
            "sales_rank_by_sku": {},
            "error": "SKU column not found in previous current inventory table",
        }

    sales_rank_column = find_sales_rank_column(df.columns)

    if not sales_rank_column:
        return {
            "table_name": previous_table_name,
            "previous_month": previous_month_name,
            "previous_year": previous_year,
            "sales_rank_by_sku": {},
            "error": "sales-rank column not found in previous current inventory table",
        }

    sales_rank_by_sku = {}

    for _, row in df.iterrows():
        sku = str(row.get("SKU") or "").strip()
        product_name = str(row.get("Product Name") or "").strip().lower()

        if not sku or product_name == "total":
            continue

        sales_rank_by_sku[sku] = clean_value(row.get(sales_rank_column))

    return {
        "table_name": previous_table_name,
        "previous_month": previous_month_name,
        "previous_year": previous_year,
        "sales_rank_column": sales_rank_column,
        "sales_rank_by_sku": sales_rank_by_sku,
        "error": None,
    }


def attach_previous_month_sales_rank(rows, columns, user_id, country_key, month_name, year):
    previous_month_name, previous_month_number, previous_year = get_previous_month_name_year(
        month_name=month_name,
        year=year,
    )

    if not previous_month_name:
        return {
            "rows": rows,
            "columns": columns,
            "previous_sales_rank_column": None,
            "previous_sales_rank_source_table": None,
            "previous_sales_rank_error": "Invalid month_name",
        }

    previous_month_display = month_label(previous_month_number)
    previous_sales_rank_column = f"Previous Month Sales Rank ({previous_month_display})"

    if previous_sales_rank_column in columns:
        return {
            "rows": rows,
            "columns": columns,
            "previous_sales_rank_column": previous_sales_rank_column,
            "previous_sales_rank_source_table": None,
            "previous_sales_rank_error": None,
        }

    previous_rank_result = fetch_previous_month_sales_rank_map(
        user_id=user_id,
        country_key=country_key,
        month_name=month_name,
        year=year,
    )

    sales_rank_by_sku = previous_rank_result["sales_rank_by_sku"]

    updated_rows = []

    for row in rows:
        new_row = dict(row)

        if is_total_row(new_row):
            new_row[previous_sales_rank_column] = None
        else:
            sku = str(new_row.get("SKU") or "").strip()
            new_row[previous_sales_rank_column] = sales_rank_by_sku.get(sku)

        updated_rows.append(new_row)

    updated_columns = list(columns)

    if previous_sales_rank_column not in updated_columns:
        updated_columns.append(previous_sales_rank_column)

    return {
        "rows": updated_rows,
        "columns": updated_columns,
        "previous_sales_rank_column": previous_sales_rank_column,
        "previous_sales_rank_source_table": previous_rank_result["table_name"],
        "previous_sales_rank_error": previous_rank_result["error"],
    }

def normalize_sku_key(value):
    return str(value or "").strip().upper()


def clean_text_value(value):
    """Return a usable text value and treat DB/Pandas null-like values as blank."""
    if value is None:
        return ""

    try:
        if pd.isna(value):
            return ""
    except (TypeError, ValueError):
        pass

    text_value = str(value).strip()

    if text_value.lower() in {"nan", "none", "null", "[null]", "<na>"}:
        return ""

    return text_value


def get_row_sku(row):
    return (
        row.get("SKU")
        or row.get("sku")
        or row.get("msku")
        or row.get("seller_sku")
        or row.get("seller-sku")
        or ""
    )


def get_first_number(row, keys, default=0):
    for key in keys:
        if key in row and row.get(key) is not None:
            value = to_number(row.get(key))

            # Return even if value is 0, because 0 can be valid.
            return value

    return default

def append_inventory_percentage_row(rows, columns):
    updated_rows = list(rows or [])
    updated_columns = list(columns or [])

    total_row = None

    # Find the final total row
    for row in updated_rows:
        if is_total_row(row):
            total_row = row

    if not total_row:
        return {
            "rows": updated_rows,
            "columns": updated_columns,
        }

    sellable_units = to_number(total_row.get("Sellable Units"))
    unfulfillable_units = to_number(total_row.get("unfulfillable-quantity"))

    # User required denominator:
    # Sellable Units + unfulfillable-quantity
    percentage_base = sellable_units + unfulfillable_units

    def pct(value):
        if percentage_base <= 0:
            return 0
        return round((to_number(value) / percentage_base) * 100, 2)

    percentage_row = {
        "row_type": "percentage",
        "is_percentage_row": True,

        "Sno.": "",
        "SKU": "",
        "Product Name": "Percentage",

        "inv-age-0-to-90-days": pct(total_row.get("inv-age-0-to-90-days")),
        "inv-age-91-to-180-days": pct(total_row.get("inv-age-91-to-180-days")),
        "inv-age-0-to-180-days": pct(total_row.get("inv-age-0-to-180-days")),
        "inv-age-181-to-270-days": pct(total_row.get("inv-age-181-to-270-days")),
        "inv-age-271-to-365-days": pct(total_row.get("inv-age-271-to-365-days")),
        "inv-age-365-plus-days": pct(total_row.get("inv-age-365-plus-days")),

        "Sellable Units": pct(sellable_units),
        "unfulfillable-quantity": pct(unfulfillable_units),

        # Optional reference values
        "percentage_base_total": percentage_base,
        "percentage_base_formula": "Sellable Units + unfulfillable-quantity",
    }

    updated_rows.append(percentage_row)

    for col in [
        "row_type",
        "is_percentage_row",
        "percentage_base_total",
        "percentage_base_formula",
    ]:
        if col not in updated_columns:
            updated_columns.append(col)

    return {
        "rows": updated_rows,
        "columns": updated_columns,
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

        if is_global_country(country_key):
            global_result = resolve_inventory_current_source_global_separated(
                user_id=user_id,
                range_type=range_type,
                month_name=month_name,
                year=year,
                quarter=quarter,
            )

            if not global_result["found"]:
                return jsonify({
                    "success": False,
                    "country_key": "global",
                    "message": "No current inventory table or inventory_aged_history data found for UK or US",
                    "combined_countries": GLOBAL_COUNTRIES,
                    "country_results": global_result["country_results"],
                    "tried_sources": global_result["tried_sources"],
                }), 404

            return jsonify({
                "success": True,
                "country_key": "global",
                "range_type": range_type,
                "requested_month": month_name or None,
                "requested_quarter": quarter or None,
                "year": int(year),
                "combined_countries": GLOBAL_COUNTRIES,

                "country_results": global_result["country_results"],

                "columns": [],
                "rows": [],
                "total_rows": 0,
                "categories": None,
                "inventory_age_summary": None,
                "high_alert_coverage_summary": None,
                "category_counts": None,
                "tried_sources": global_result["tried_sources"],
            }), 200

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

        selected_month_for_units = source_result["selected_month"]

        units_sold_result = {
            "units_column": None,
            "units_source_table": None,
            "units_source_error": None,
        }

        previous_sales_rank_result = {
            "previous_sales_rank_column": None,
            "previous_sales_rank_source_table": None,
            "previous_sales_rank_error": None,
        }

        if selected_month_for_units and not is_global_country(country_key):
            units_sold_result = attach_current_month_units_sold(
                rows=rows,
                columns=columns,
                user_id=user_id,
                country_key=country_key,
                month_name=selected_month_for_units,
                year=year,
            )

            rows = units_sold_result["rows"]
            columns = units_sold_result["columns"]

            # IMPORTANT:
            # attach_current_month_units_sold can append missing SKUs from skuwisemonthly.
            monthly_attach_after_units = attach_inventory_monthly_qty(
                rows=rows,
                columns=columns,
                user_id=user_id,
                country_key=country_key,
                month_name=selected_month_for_units,
                year=year,
                use_currentinventory_sellable=source_result["source_type"] == "currentinventory_table",
            )

            rows = monthly_attach_after_units["rows"]
            columns = monthly_attach_after_units["columns"]

            zero_to_180_attach = attach_zero_to_180_days(
                rows=rows,
                columns=columns,
                use_currentinventory_split=source_result["source_type"] == "currentinventory_table",
            )

            rows = zero_to_180_attach["rows"]
            columns = zero_to_180_attach["columns"]

            previous_sales_rank_result = attach_previous_month_sales_rank(
                rows=rows,
                columns=columns,
                user_id=user_id,
                country_key=country_key,
                month_name=selected_month_for_units,
                year=year,
            )

            rows = previous_sales_rank_result["rows"]
            columns = previous_sales_rank_result["columns"]

        categories = build_inventory_categories(rows)
        inventory_age_summary = build_inventory_age_summary(rows)

        high_alert_coverage_summary = (
            {
                "high_alert_sku_count": 0,
                "average_coverage_ratio": 0,
                "high_alert_threshold": 0,
                "items": [],
            }
            if is_global_country(country_key)
            else build_high_alert_coverage_summary(
                rows=rows,
                user_id=user_id,
                country_key=country_key,
            )
        )

        selected_month_number = MONTH_NAME_TO_NUMBER.get(source_result["selected_month"])

        previous_storage_cost = {
            "value": 0,
            "source_table": None,
            "period": None,
            "error": None,
        }

        if selected_month_number and not is_global_country(country_key):
            previous_storage_cost = fetch_previous_platform_fee_as_storage_cost(
                user_id=user_id,
                country_key=country_key,
                month_number=selected_month_number,
                year_int=int(year),
            )
        elif selected_month_number and is_global_country(country_key):
            previous_total = 0
            previous_sources = []

            for child_country in GLOBAL_COUNTRIES:
                child_previous = fetch_previous_platform_fee_as_storage_cost(
                    user_id=user_id,
                    country_key=child_country,
                    month_number=selected_month_number,
                    year_int=int(year),
                )

                previous_total += to_number(child_previous.get("value"))
                previous_sources.append(child_previous)

            previous_storage_cost = {
                "value": previous_total,
                "source_table": "uk+us combined",
                "period": None,
                "error": None,
                "sources": previous_sources,
            }

        categories["estimated_storage_cost"]["previous_storage_cost"] = previous_storage_cost["value"]
        categories["estimated_storage_cost"]["previous_storage_cost_source"] = previous_storage_cost["source_table"]
        categories["estimated_storage_cost"]["previous_storage_cost_period"] = previous_storage_cost["period"]
        categories["estimated_storage_cost"]["previous_storage_cost_error"] = previous_storage_cost["error"]

        percentage_row_result = append_inventory_percentage_row(
            rows=rows,
            columns=columns,
        )

        rows = percentage_row_result["rows"]
        columns = percentage_row_result["columns"]

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
            "combined_sources": source_result.get("combined_sources", []),
            "table_name": source_result["source_name"] if source_result["source_type"] == "currentinventory_table" else None,
            "units_sold_column": units_sold_result["units_column"],
            "units_sold_source_table": units_sold_result["units_source_table"],
            "units_sold_source_error": units_sold_result["units_source_error"],
            "previous_sales_rank_column": previous_sales_rank_result["previous_sales_rank_column"],
            "previous_sales_rank_source_table": previous_sales_rank_result["previous_sales_rank_source_table"],
            "previous_sales_rank_error": previous_sales_rank_result["previous_sales_rank_error"],
            "tried_sources": source_result["tried_sources"],

            "columns": columns,
            "rows": rows,
            "total_rows": len(rows),

            "categories": categories,
            "inventory_age_summary": inventory_age_summary,
            "high_alert_coverage_summary": high_alert_coverage_summary,
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

def get_age_summary_for_single_country(user_id, country_key, month_name, year):
    marketplace_id = get_marketplace_id(country_key)

    empty_totals = {
        "inv-age-0-to-90-days": 0,
        "inv-age-91-to-180-days": 0,
        "inv-age-0-to-180-days": 0,
        "inv-age-181-to-270-days": 0,
        "inv-age-271-to-365-days": 0,
        "inv-age-365-plus-days": 0,
        "unfulfillable-quantity": 0,
    }

    if not marketplace_id:
        return {
            "success": False,
            "message": f"Unsupported country_key: {country_key}",
            "month_summary": [],
            "age_summary": [],
            "selected_month_totals": empty_totals,
            "grand_totals": empty_totals,
            "tried_sources": [],
        }

    current_month_number = MONTH_NAME_TO_NUMBER[month_name]
    year_int = int(year)

    age_columns = [
        "inv-age-0-to-90-days",
        "inv-age-91-to-180-days",
        "inv-age-0-to-180-days",
        "inv-age-181-to-270-days",
        "inv-age-271-to-365-days",
        "inv-age-365-plus-days",
    ]
    summary_columns = [
        *age_columns,
        "unfulfillable-quantity",
    ]

    month_summary = []
    tried_sources = []

    for month_number in range(1, current_month_number + 1):
        month_text = calendar_month_name[month_number].lower()
        month_display = month_label(month_number)

        source_result = resolve_inventory_current_source(
            user_id=user_id,
            country_key=country_key,
            range_type="monthly",
            month_name=month_text,
            year=year,
            quarter=None,
        )

        tried_sources.extend(source_result.get("tried_sources", []))

        if not source_result.get("found"):
            month_summary.append({
                "country_key": country_key,
                "month": month_display,
                "month_number": month_number,
                "year": year_int,
                "source": None,
                "source_type": None,
                "sellable_total": 0,
                "totals": dict(empty_totals),
            })
            continue

        rows = source_result["rows"]
        columns = source_result["columns"]
        selected_month_for_units = source_result["selected_month"]

        if selected_month_for_units:
            units_sold_result = attach_current_month_units_sold(
                rows=rows,
                columns=columns,
                user_id=user_id,
                country_key=country_key,
                month_name=selected_month_for_units,
                year=year,
            )

            rows = units_sold_result["rows"]
            columns = units_sold_result["columns"]

            monthly_attach_after_units = attach_inventory_monthly_qty(
                rows=rows,
                columns=columns,
                user_id=user_id,
                country_key=country_key,
                month_name=selected_month_for_units,
                year=year,
                use_currentinventory_sellable=source_result["source_type"] == "currentinventory_table",
            )

            rows = monthly_attach_after_units["rows"]
            columns = monthly_attach_after_units["columns"]

            zero_to_180_attach = attach_zero_to_180_days(
                rows=rows,
                columns=columns,
                use_currentinventory_split=source_result["source_type"] == "currentinventory_table",
            )

            rows = zero_to_180_attach["rows"]
            columns = zero_to_180_attach["columns"]

        data_rows = [
            row for row in rows
            if not is_total_row(row) and not is_percentage_row(row)
        ]

        totals = {
            column: sum(
                to_number(row.get(column))
                for row in data_rows
            )
            for column in summary_columns
        }

        sellable_total = sum(totals.get(column, 0) for column in age_columns)

        month_summary.append({
            "country_key": country_key,
            "month": month_display,
            "month_number": month_number,
            "year": year_int,
            "source": source_result["source_name"],
            "source_type": source_result["source_type"],
            "sellable_total": sellable_total,
            "totals": totals,
        })

    age_summary = []

    bucket_labels = {
        "inv-age-0-to-90-days": "0-90 days",
        "inv-age-91-to-180-days": "91-180 days",
        "inv-age-0-to-180-days": "0-180 days",
        "inv-age-181-to-270-days": "181-270 days",
        "inv-age-271-to-365-days": "271-365 days",
        "inv-age-365-plus-days": "365+ days",
        "unfulfillable-quantity": "Unsellable",
    }

    for month_row in month_summary:
        for column in summary_columns:
            age_summary.append({
                "country_key": country_key,
                "month": month_row["month"],
                "month_number": month_row["month_number"],
                "year": month_row["year"],
                "source": month_row["source"],
                "source_type": month_row["source_type"],
                "age_bucket": bucket_labels[column],
                "column": column,
                "units": month_row["totals"][column],
            })

    selected_month_row = next(
        (
            row for row in month_summary
            if int(row["month_number"]) == int(current_month_number)
        ),
        None,
    )

    selected_month_totals = (
        selected_month_row["totals"]
        if selected_month_row
        else dict(empty_totals)
    )

    grand_totals = {
        column: sum(
            row["totals"][column]
            for row in month_summary
        )
        for column in summary_columns
    }

    return {
        "success": True,
        "country_key": country_key,
        "marketplace_id": marketplace_id,
        "tried_sources": tried_sources,
        "month_summary": month_summary,
        "age_summary": age_summary,
        "selected_month_totals": selected_month_totals,
        "grand_totals": grand_totals,
        "totals": selected_month_totals,
    }



def fetch_monthly_sellable_total(user_id, country_key, month_name, year):
    monthly_result = fetch_inventory_monthly_qty_map(
        user_id=user_id,
        country_key=country_key,
        month_name=month_name,
        year=year,
    )

    return to_number(monthly_result.get("totals", {}).get("sellable"))


@inventory_current_bp.route("/inventory_current_age_summary", methods=["GET", "OPTIONS"])
def get_inventory_current_age_summary():
    if request.method == "OPTIONS":
        return jsonify({"message": "CORS Preflight OK"}), 200

    auth_header = request.headers.get("Authorization")

    if not auth_header or not auth_header.startswith("Bearer "):
        return jsonify({
            "success": False,
            "error": "Authorization token is missing or invalid"
        }), 401

    token = auth_header.split(" ")[1]

    try:
        payload, effective_user_id, member_id = get_effective_user_id_from_token(token)
        user_id = payload["user_id"]

    except jwt.ExpiredSignatureError:
        return jsonify({
            "success": False,
            "error": "Token has expired"
        }), 401

    except jwt.InvalidTokenError:
        return jsonify({
            "success": False,
            "error": "Invalid token"
        }), 401

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
            is_safe_identifier(year),
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

        # --------------------------------------------------
        # GLOBAL: return separate UK and US results
        # --------------------------------------------------
        if is_global_country(country_key):
            country_results = {}

            for child_country in GLOBAL_COUNTRIES:
                child_result = get_age_summary_for_single_country(
                    user_id=user_id,
                    country_key=child_country,
                    month_name=month_name,
                    year=year,
                )

                country_results[child_country] = child_result

            tried_sources = []

            for child_result in country_results.values():
                tried_sources.extend(child_result.get("tried_sources", []))

            return jsonify({
                "success": True,
                "requested_month": month_name.capitalize(),
                "year": int(year),
                "country_key": "global",
                "marketplace_id": None,
                "combined_countries": GLOBAL_COUNTRIES,
                "history_range": {
                    "from_month": "January",
                    "to_month": month_name.capitalize(),
                },
                "source_priority": [
                    "currentinventory dynamic table",
                    "inventory_aged_history fallback",
                ],
                "tried_sources": tried_sources,

                "country_results": country_results,

                # For global, totals are available inside each country result.
                "totals": None,
                "selected_month_totals": None,
                "grand_totals": None,
                "month_summary": [],
                "age_summary": [],
            }), 200

        # --------------------------------------------------
        # SINGLE COUNTRY: use helper, no duplicate route logic
        # --------------------------------------------------
        single_result = get_age_summary_for_single_country(
            user_id=user_id,
            country_key=country_key,
            month_name=month_name,
            year=year,
        )

        if not single_result.get("success"):
            return jsonify(single_result), 400

        return jsonify({
            "success": True,
            "requested_month": month_name.capitalize(),
            "year": int(year),
            "country_key": country_key,
            "marketplace_id": single_result.get("marketplace_id"),

            "history_range": {
                "from_month": "January",
                "to_month": month_name.capitalize(),
            },

            "source_priority": [
                "currentinventory dynamic table",
                "inventory_aged_history fallback",
            ],

            "tried_sources": single_result.get("tried_sources", []),

            # Selected month only.
            # Example: if requested_month = March,
            # totals will be March only, not January + February + March.
            "totals": single_result.get("selected_month_totals", {}),
            "selected_month_totals": single_result.get("selected_month_totals", {}),

            # Cumulative January to selected month.
            "grand_totals": single_result.get("grand_totals", {}),

            "month_summary": single_result.get("month_summary", []),
            "age_summary": single_result.get("age_summary", []),
        }), 200

    except Exception as e:
        return jsonify({
            "success": False,
            "message": "Error fetching inventory age summary",
            "error": str(e),
        }), 500



