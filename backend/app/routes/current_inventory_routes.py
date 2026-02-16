# current_inventory_routes.py

from flask import Blueprint, request, jsonify
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
import jwt
import os
import base64
import pandas as pd
from config import Config
import logging
from app.models.user_models import User, CountryProfile
from app import db
from io import BytesIO
from dotenv import load_dotenv
from datetime import datetime
from calendar import monthrange
from sqlalchemy.exc import ProgrammingError
from app.utils.live_bi_utils import generate_inventory_alerts_for_all_skus

# ===== Setup =====
SECRET_KEY = Config.SECRET_KEY

load_dotenv()
db_url = os.getenv("DATABASE_URL")
logger = logging.getLogger(__name__)

current_inventory_bp = Blueprint("current_inventory_bp", __name__)


def encode_file_to_base64(file_path: str) -> str:
    with open(file_path, "rb") as f:
        return base64.b64encode(f.read()).decode("utf-8")


def norm_sku(x) -> str:
    if x is None:
        return ""
    return str(x).strip().upper()


# ✅ inventory table uses marketplace_id (SP-API ID)
MARKETPLACE_ID_BY_COUNTRY = {
    "us": "ATVPDKIKX0DER",
    "uk": "A1F83G8C2ARO7P",
}

# ✅ liveorders table uses marketplace (NAME)
MARKETPLACE_NAME_BY_COUNTRY = {
    "us": "Amazon.com",
    "uk": "Amazon.co.uk",
}


@current_inventory_bp.route("/current_inventory", methods=["POST", "OPTIONS"])
def current_inventory():
    if request.method == "OPTIONS":
        return jsonify({"message": "CORS Preflight OK"}), 200

    # --- Auth ---
    auth_header = request.headers.get("Authorization")
    if not auth_header or not auth_header.startswith("Bearer "):
        return jsonify({"error": "Authorization token is missing or invalid"}), 401

    token = auth_header.split(" ")[1]
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=["HS256"])
        user_id = payload["user_id"]
    except jwt.ExpiredSignatureError:
        return jsonify({"error": "Token has expired"}), 401
    except jwt.InvalidTokenError:
        return jsonify({"error": "Invalid token"}), 401

    # --- Inputs ---
    data = request.get_json() or {}
    month = (data.get("month") or "").strip()
    year = data.get("year")
    country = (data.get("country") or "").strip()

    if not month or not year or not country:
        return jsonify({"error": "Month, year, and country must be provided"}), 400

    try:
        month_name = datetime.strptime(month.capitalize(), "%B").strftime("%B")
        year = int(year)
    except ValueError:
        return jsonify({"error": "Invalid month or year format"}), 400

    # --- DB session (primary) ---
    SessionLocal = sessionmaker(bind=db.engine)
    db_session = SessionLocal()

    try:
        user = db_session.get(User, user_id)
        if user is None:
            return jsonify({"error": f"User not found for ID {user_id}"}), 404

        country_key = country.lower().strip()

        # =========================================================
        # Resolve marketplace_name (for liveorders) and marketplace_id (for inventory)
        # IMPORTANT: Do NOT use profile.marketplace for liveorders, because it might store IDs.
        # =========================================================
        marketplace_id = None
        try:
            profile = (
                db_session.query(CountryProfile)
                .filter_by(user_id=user_id, country=country_key)
                .first()
            )
            if profile and getattr(profile, "marketplace_id", None):
                marketplace_id = profile.marketplace_id
        except Exception:
            pass

        if not marketplace_id:
            marketplace_id = MARKETPLACE_ID_BY_COUNTRY.get(country_key)

        marketplace_name = MARKETPLACE_NAME_BY_COUNTRY.get(country_key)

        if not marketplace_id or not marketplace_name:
            return jsonify({"error": f'Unknown marketplace for country "{country}"'}), 400

        # --- Load SKU master from per-user table (primary DB) ---
        engine_primary = create_engine(db_url)
        sku_table_name = f"sku_{user_id}_data_table"

        try:
            sku_df = pd.read_sql_table(sku_table_name, engine_primary)
        except Exception as e:
            return jsonify({"error": f'Could not read SKU table "{sku_table_name}": {e}'}), 500

        sku_column_name = f"sku_{country_key}"
        if sku_column_name not in sku_df.columns:
            return jsonify({"error": f"SKU column '{sku_column_name}' not found in {sku_table_name}"}), 400

        sku_df = sku_df[[sku_column_name, "product_name"]].copy()
        sku_df.rename(columns={sku_column_name: "sku"}, inplace=True)
        sku_df["sku"] = sku_df["sku"].apply(norm_sku)

        # =========================
        # Date range for selected month
        # =========================
        month_number = datetime.strptime(month_name, "%B").month
        month_start = datetime(year, month_number, 1)
        month_end = datetime(year, month_number, monthrange(year, month_number)[1], 23, 59, 59)

        current_month_col = f"Current Month Units Sold ({month_name})"

        # use the amazon engine bind
        amazon_engine = db.get_engine(bind="amazon")

        # =========================================================
        # SALES (liveorders.marketplace is NAME like Amazon.co.uk)
        # =========================================================
        sales_sql = text(f"""
            SELECT
                sku,
                SUM(quantity) AS "{current_month_col}"
            FROM liveorders
            WHERE user_id = :uid
              AND marketplace = :mkt_name
              AND purchase_date >= :start
              AND purchase_date <= :end
            GROUP BY sku
        """)

        current_month_sales_df = pd.read_sql_query(
            sales_sql,
            amazon_engine,
            params={
                "uid": user_id,
                "mkt_name": marketplace_name,
                "start": month_start,
                "end": month_end,
            },
        )
        if not current_month_sales_df.empty:
            current_month_sales_df["sku"] = current_month_sales_df["sku"].apply(norm_sku)

        # =========================================================
        # INVENTORY (inventory table - optional, used for inbound/synced_at/etc.)
        # inventory.marketplace_id is ID like A1F83...
        # =========================================================
        inv_df = pd.DataFrame()
        try:
            inv_sql = text("""
                SELECT seller_sku,
                       total_quantity,
                       inbound_quantity,
                       available_quantity,
                       reserved_quantity,
                       fulfillable_quantity,
                       synced_at
                FROM inventory
                WHERE user_id = :uid
                  AND marketplace_id = :mkt_id
            """)
            inv_df = pd.read_sql_query(
                inv_sql,
                amazon_engine,
                params={"uid": user_id, "mkt_id": marketplace_id},
            )
            if not inv_df.empty:
                inv_df["seller_sku"] = inv_df["seller_sku"].apply(norm_sku)
        except Exception:
            inv_df = pd.DataFrame()

        # =========================================================
        # AGED INVENTORY (PRIMARY inventory source, joins on sku)
        # inventory_aged has column "sku"
        # =========================================================
        aged_df = pd.DataFrame()
        try:
            aged_sql = text("""
                SELECT
                    sku,
                    available,
                    "inv-age-0-to-90-days",
                    "inv-age-91-to-180-days",
                    "inv-age-181-to-270-days",
                    "inv-age-271-to-365-days",
                    "inv-age-365-plus-days",
                    "sales-rank",
                    "estimated-storage-cost-next-month"
                FROM inventory_aged
                WHERE user_id = :uid
            """)
            aged_df = pd.read_sql_query(aged_sql, amazon_engine, params={"uid": user_id})
            if not aged_df.empty:
                aged_df["sku"] = aged_df["sku"].apply(norm_sku)
        except ProgrammingError:
            aged_df = pd.DataFrame()
        except Exception:
            aged_df = pd.DataFrame()

        # =========================================================
        # MERGE master + sales + aged inventory (JOIN KEY = sku)
        # =========================================================
        final_df = sku_df[["sku", "product_name"]].merge(
            current_month_sales_df[["sku", current_month_col]]
            if not current_month_sales_df.empty
            else pd.DataFrame({"sku": sku_df["sku"], current_month_col: 0}),
            on="sku",
            how="left"
        )

        # Join aged inventory ON sku
        if not aged_df.empty:
            final_df = final_df.merge(aged_df, on="sku", how="left")
        else:
            for c in [
                "available",
                "inv-age-0-to-90-days",
                "inv-age-91-to-180-days",
                "inv-age-181-to-270-days",
                "inv-age-271-to-365-days",
                "inv-age-365-plus-days",
                "sales-rank",
                "estimated-storage-cost-next-month",
            ]:
                final_df[c] = pd.NA

        # Join inventory table (optional) using sku -> seller_sku
        if not inv_df.empty:
            final_df = final_df.merge(inv_df, left_on="sku", right_on="seller_sku", how="left")
        else:
            for c in [
                "total_quantity",
                "inbound_quantity",
                "available_quantity",
                "reserved_quantity",
                "fulfillable_quantity",
                "synced_at",
            ]:
                final_df[c] = pd.NA
            final_df["seller_sku"] = pd.NA

        # =========================================================
        # Business logic
        # =========================================================
        final_df[current_month_col] = pd.to_numeric(final_df[current_month_col], errors="coerce").fillna(0)

        # inventory inwarded
        final_df["inbound_quantity"] = pd.to_numeric(final_df.get("inbound_quantity"), errors="coerce").fillna(0)
        final_df["Inventory Inwarded"] = final_df["inbound_quantity"]

        # End-of-month inventory: prefer aged inventory "available"
        final_df["available"] = pd.to_numeric(final_df.get("available"), errors="coerce").fillna(0)
        final_df["Inventory at the end of the month"] = final_df["available"]

        # =========================
        # compute "Others" from previous-month user_{id}_{country}_{month}{year}_data
        # =========================
        today = datetime.now().date()
        today_day = today.day

        if month_number == 1:
            prev_month_number = 12
            prev_year = year - 1
        else:
            prev_month_number = month_number - 1
            prev_year = year

        prev_month_name_full = datetime(prev_year, prev_month_number, 1).strftime("%B")
        prev_month_name_lower = prev_month_name_full.lower()

        prev_last_day = monthrange(prev_year, prev_month_number)[1]
        start_day = today_day + 1
        if start_day > prev_last_day:
            start_day = prev_last_day

        prev_start = datetime(prev_year, prev_month_number, start_day, 0, 0, 0)
        prev_end = datetime(prev_year, prev_month_number, prev_last_day, 23, 59, 59)

        prev_table_name = f"user_{user_id}_{country_key}_{prev_month_name_lower}{prev_year}_data"

        final_df["Others"] = 0
        try:
            prev_sql = text(f"""
                SELECT sku,
                       SUM(quantity) AS others_qty
                FROM {prev_table_name}
                WHERE date_time <> '0'
                  AND replace(date_time, 'Z', '+00')::timestamptz >= :start
                  AND replace(date_time, 'Z', '+00')::timestamptz <= :end
                GROUP BY sku
            """)
            prev_df = pd.read_sql_query(prev_sql, engine_primary, params={"start": prev_start, "end": prev_end})
            if not prev_df.empty:
                prev_df["sku"] = prev_df["sku"].apply(norm_sku)
                final_df = final_df.merge(prev_df[["sku", "others_qty"]], on="sku", how="left")
                final_df["Others"] = pd.to_numeric(final_df["others_qty"], errors="coerce").fillna(0)
                final_df.drop(columns=["others_qty"], inplace=True, errors="ignore")
        except Exception:
            pass

        # Beginning inventory
        final_df["Inventory at the beginning of the month"] = (
            final_df["Inventory at the end of the month"]
            - final_df["Inventory Inwarded"]
            + final_df[current_month_col]
            - final_df["Others"]
        ).fillna(0).clip(lower=0)

        # --- Rename & tidy up for Excel / UI ---
        final_df.rename(columns={"sku": "SKU", "product_name": "Product Name"}, inplace=True)

        if "seller_sku" in final_df.columns:
            final_df.drop(columns=["seller_sku"], inplace=True)

        final_df.insert(0, "Sno.", range(1, len(final_df) + 1))

        desired_order = [
            "Sno.",
            "SKU",
            "Product Name",

            "total_quantity",
            "inbound_quantity",
            "available_quantity",
            "reserved_quantity",
            "fulfillable_quantity",
            "synced_at",

            "available",
            "inv-age-0-to-90-days",
            "inv-age-91-to-180-days",
            "inv-age-181-to-270-days",
            "inv-age-271-to-365-days",
            "inv-age-365-plus-days",
            "sales-rank",
            "estimated-storage-cost-next-month",

            "Inventory at the beginning of the month",
            current_month_col,
            "Inventory Inwarded",
            "Others",
            "Inventory at the end of the month",
        ]
        final_df = final_df.reindex(
            columns=[c for c in desired_order if c in final_df.columns]
            + [c for c in final_df.columns if c not in desired_order]
        )

        # --- Total row ---
        numeric_columns = final_df.select_dtypes(include=["number"]).columns
        total_row = {
            col: (final_df[col].sum() if col in numeric_columns and col != "Sno." else "")
            for col in final_df.columns
        }
        total_row["Product Name"] = "Total"
        final_df = pd.concat([final_df, pd.DataFrame([total_row])], ignore_index=True)

        # ---------------- INVENTORY ALERTS ----------------
        try:
            inventory_alerts = generate_inventory_alerts_for_all_skus(
                user_id=user_id,
                country=country_key,
            )
        except Exception:
            logger.exception("Failed to generate inventory alerts in current_inventory")
            inventory_alerts = {}

        # =========================================================
        # ✅ IN-MEMORY EXCEL (NO SAVING TO UPLOAD FOLDER)
        # =========================================================
        filename = f"currentinventory_{user_id}_{country_key}_{month_name.lower()}{year}.xlsx"

        output = BytesIO()
        final_df.to_excel(output, index=False, engine="openpyxl")
        output.seek(0)

        excel_b64 = base64.b64encode(output.read()).decode("utf-8")

        return jsonify({
            "message": "Current inventory report generated successfully",
            "data": excel_b64,
            "filename": filename,
            "inventory_alerts": inventory_alerts,
        }), 200

    finally:
        db_session.close()