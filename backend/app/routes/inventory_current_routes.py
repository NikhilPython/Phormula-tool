# import os
# import re
# import math
# from flask import Blueprint, request, jsonify
# from sqlalchemy import create_engine, text, inspect
# from dotenv import load_dotenv
# import jwt
# from flask import request, jsonify
# from sqlalchemy import text, inspect
# from app.utils.token_utils import get_effective_user_id_from_token
# from config import Config


# SECRET_KEY = Config.SECRET_KEY

# load_dotenv()
# db_url = os.getenv("DATABASE_URL")
# primary_engine = create_engine(db_url, pool_pre_ping=True)

# inventory_current_bp = Blueprint("inventory_current",__name__,)


# def is_safe_identifier(value):
#     return bool(re.fullmatch(r"[A-Za-z0-9_]+", str(value)))


# def clean_value(value):
#     if value is None:
#         return None

#     if isinstance(value, float):
#         if math.isnan(value) or math.isinf(value):
#             return None

#     return value

# @inventory_current_bp.route("/inventory_current", methods=["GET", "OPTIONS"])
# def get_inventory_current_table():
#     if request.method == "OPTIONS":
#         return jsonify({"message": "CORS Preflight OK"}), 200

#     auth_header = request.headers.get("Authorization")
#     if not auth_header or not auth_header.startswith("Bearer "):
#         return jsonify({"error": "Authorization token is missing or invalid"}), 401

#     token = auth_header.split(" ")[1]

#     try:
#         payload, effective_user_id, member_id = get_effective_user_id_from_token(token)
#         user_id = payload["user_id"]
#     except jwt.ExpiredSignatureError:
#         return jsonify({"error": "Token has expired"}), 401
#     except jwt.InvalidTokenError:
#         return jsonify({"error": "Invalid token"}), 401

#     try:
#         country_key = request.args.get("country_key")
#         month_name = request.args.get("month_name")
#         year = request.args.get("year")

#         if not country_key or not month_name or not year:
#             return jsonify({
#                 "success": False,
#                 "message": "Missing required params: country_key, month_name, year"
#             }), 400

#         user_id = str(user_id).strip()
#         country_key = str(country_key).strip().lower()
#         month_name = str(month_name).strip().lower()
#         year = str(year).strip()

#         if not all([
#             is_safe_identifier(user_id),
#             is_safe_identifier(country_key),
#             is_safe_identifier(month_name),
#             is_safe_identifier(year)
#         ]):
#             return jsonify({
#                 "success": False,
#                 "message": "Invalid table parameters"
#             }), 400

#         table_name = f"currentinventory_{user_id}_{country_key}_{month_name}{year}_table"

#         inspector = inspect(primary_engine)

#         if table_name not in inspector.get_table_names():
#             return jsonify({
#                 "success": False,
#                 "message": f"Table not found: {table_name}",
#                 "table_name": table_name,
#                 "columns": [],
#                 "rows": []
#             }), 404

#         query = text(f'SELECT * FROM "{table_name}"')

#         with primary_engine.connect() as connection:
#             result = connection.execute(query)
#             columns = list(result.keys())

#             rows = []
#             for row in result.fetchall():
#                 row_dict = dict(zip(columns, row))
#                 cleaned_row = {
#                     key: clean_value(value)
#                     for key, value in row_dict.items()
#                 }
#                 rows.append(cleaned_row)

#         return jsonify({
#             "success": True,
#             "table_name": table_name,
#             "columns": columns,
#             "rows": rows,
#             "total_rows": len(rows)
#         }), 200

#     except Exception as e:
#         return jsonify({
#             "success": False,
#             "message": "Error fetching inventory current table",
#             "error": str(e)
#         }), 500   


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

        return jsonify({
            "success": True,
            "table_name": table_name,
            "columns": columns,
            "rows": rows,
            "total_rows": len(rows),
            "categories": categories,
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

        table_name = f"currentinventory_{user_id}_{country_key}_{month_name}{year}_table"

        inspector = inspect(primary_engine)

        if table_name not in inspector.get_table_names():
            return jsonify({
                "success": False,
                "message": f"Table not found: {table_name}",
                "table_name": table_name,
                "age_summary": [],
                "totals": {}
            }), 404

        query = text(f'''
            SELECT
                COALESCE(SUM(CAST(NULLIF("inv-age-0-to-90-days"::text, '') AS NUMERIC)), 0) AS inv_age_0_to_90_days,
                COALESCE(SUM(CAST(NULLIF("inv-age-91-to-180-days"::text, '') AS NUMERIC)), 0) AS inv_age_91_to_180_days,
                COALESCE(SUM(CAST(NULLIF("inv-age-181-to-270-days"::text, '') AS NUMERIC)), 0) AS inv_age_181_to_270_days,
                COALESCE(SUM(CAST(NULLIF("inv-age-271-to-365-days"::text, '') AS NUMERIC)), 0) AS inv_age_271_to_365_days,
                COALESCE(SUM(CAST(NULLIF("inv-age-365-plus-days"::text, '') AS NUMERIC)), 0) AS inv_age_365_plus_days
            FROM "{table_name}"
            WHERE LOWER(COALESCE("Product Name"::text, '')) != 'total'
        ''')

        with primary_engine.connect() as connection:
            result = connection.execute(query).mappings().first()

        totals = {
            "inv-age-0-to-90-days": clean_value(float(result["inv_age_0_to_90_days"] or 0)),
            "inv-age-91-to-180-days": clean_value(float(result["inv_age_91_to_180_days"] or 0)),
            "inv-age-181-to-270-days": clean_value(float(result["inv_age_181_to_270_days"] or 0)),
            "inv-age-271-to-365-days": clean_value(float(result["inv_age_271_to_365_days"] or 0)),
            "inv-age-365-plus-days": clean_value(float(result["inv_age_365_plus_days"] or 0))
        }

        age_summary = [
            {
                "month": month_name.capitalize(),
                "year": int(year),
                "age_bucket": "0-90 days",
                "column": "inv-age-0-to-90-days",
                "units": totals["inv-age-0-to-90-days"]
            },
            {
                "month": month_name.capitalize(),
                "year": int(year),
                "age_bucket": "91-180 days",
                "column": "inv-age-91-to-180-days",
                "units": totals["inv-age-91-to-180-days"]
            },
            {
                "month": month_name.capitalize(),
                "year": int(year),
                "age_bucket": "181-270 days",
                "column": "inv-age-181-to-270-days",
                "units": totals["inv-age-181-to-270-days"]
            },
            {
                "month": month_name.capitalize(),
                "year": int(year),
                "age_bucket": "271-365 days",
                "column": "inv-age-271-to-365-days",
                "units": totals["inv-age-271-to-365-days"]
            },
            {
                "month": month_name.capitalize(),
                "year": int(year),
                "age_bucket": "365+ days",
                "column": "inv-age-365-plus-days",
                "units": totals["inv-age-365-plus-days"]
            }
        ]

        return jsonify({
            "success": True,
            "table_name": table_name,
            "month": month_name.capitalize(),
            "year": int(year),
            "country_key": country_key,
            "totals": totals,
            "age_summary": age_summary
        }), 200

    except Exception as e:
        return jsonify({
            "success": False,
            "message": "Error fetching inventory age summary",
            "error": str(e)
        }), 500    