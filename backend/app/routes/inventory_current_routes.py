import os
import re
import math
import jwt
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

        table_name = f"currentinventory_{user_id}_{country_key}_{month_name}{year}_table"

        inspector = inspect(primary_engine)

        if table_name not in inspector.get_table_names():
            return jsonify({
                "success": False,
                "message": f"Table not found: {table_name}",
                "table_name": table_name,
                "columns": [],
                "rows": []
            }), 404

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

        categories = build_inventory_categories(rows)
        inventory_age_summary = build_inventory_age_summary(rows)

        return jsonify({
            "success": True,
            "table_name": table_name,
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
    

    