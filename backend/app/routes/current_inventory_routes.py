from flask import Blueprint, request, jsonify
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.orm import sessionmaker
import jwt
import os
import base64
import math
import numpy as np
import pandas as pd
from config import Config
import logging
from app.models.user_models import User, CountryProfile
from app import db
from io import BytesIO
from dotenv import load_dotenv
from datetime import datetime, timezone, date, timedelta
from calendar import monthrange
from sqlalchemy.exc import ProgrammingError
from app.utils.live_bi_utils import generate_inventory_alerts_for_all_skus
from app.utils.token_utils import get_effective_user_id_from_token
from app.utils.uk_coverage_ratio_utils import fetch_last_30_days_units

SECRET_KEY = Config.SECRET_KEY

load_dotenv()
db_url = os.getenv("DATABASE_URL")
logger = logging.getLogger(__name__)

current_inventory_bp = Blueprint("current_inventory_bp", __name__)

# Create once, reuse
primary_engine = create_engine(db_url, pool_pre_ping=True)

MARKETPLACE_ID_BY_COUNTRY = {
    "us": "ATVPDKIKX0DER",
    "uk": "A1F83G8C2ARO7P",
}

MARKETPLACE_NAME_BY_COUNTRY = {
    "us": "Amazon.com",
    "uk": "Amazon.co.uk",
}


def norm_sku(x) -> str:
    if x is None:
        return ""
    return str(x).strip().upper()


def safe_numeric(series_or_value, default=0):
    return pd.to_numeric(series_or_value, errors="coerce").fillna(default)

def clean_json_value(value):
    if value is None:
        return None

    # pandas missing values: pd.NA, NaT, np.nan
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass

    # numpy scalar types
    if isinstance(value, np.integer):
        return int(value)

    if isinstance(value, np.floating):
        if np.isnan(value) or np.isinf(value):
            return None
        return float(value)

    # normal Python float
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None
        return value

    # datetime/date values
    if isinstance(value, (datetime, date)):
        return value.isoformat()

    # nested values
    if isinstance(value, dict):
        return {k: clean_json_value(v) for k, v in value.items()}

    if isinstance(value, list):
        return [clean_json_value(v) for v in value]

    return value

def table_exists(engine, table_name: str) -> bool:
    try:
        insp = inspect(engine)
        return insp.has_table(table_name)
    except Exception:
        logger.exception("Failed checking existence of table %s", table_name)
        return False


def resolve_marketplace(db_session, user_id: int, country_key: str):
    marketplace_id = None
    profile = None

    try:
        profile = (
            db_session.query(CountryProfile)
            .filter_by(user_id=user_id, country=country_key)
            .first()
        )
    except Exception:
        logger.exception("Failed loading CountryProfile for user=%s country=%s", user_id, country_key)

    if profile and getattr(profile, "marketplace_id", None):
        marketplace_id = profile.marketplace_id

    if not marketplace_id:
        marketplace_id = MARKETPLACE_ID_BY_COUNTRY.get(country_key)

    marketplace_name = MARKETPLACE_NAME_BY_COUNTRY.get(country_key)

    return marketplace_id, marketplace_name, profile


def load_aged_inventory(amazon_engine, user_id: int, country_key: str, marketplace_id: str) -> pd.DataFrame:
    """
    Tries multiple filter combinations depending on columns available in inventory_aged.
    This prevents US/UK data mixing.
    """
    try:
        probe_sql = text("""
            SELECT *
            FROM inventory_aged
            WHERE 1=0
        """)
        empty_df = pd.read_sql_query(probe_sql, amazon_engine)
        cols = set(empty_df.columns)
    except Exception:
        logger.exception("Failed probing inventory_aged schema")
        return pd.DataFrame()

    select_cols = [
        "sku",
        "available",
        "inv-age-0-to-90-days",
        "inv-age-91-to-180-days",
        "inv-age-181-to-270-days",
        "inv-age-271-to-365-days",
        "inv-age-365-plus-days",
        "sales-rank",
        "estimated-storage-cost-next-month",
    ]

    if "snapshot_date" in cols:
        select_cols.append("snapshot_date")
    if "marketplace_id" in cols:
        select_cols.append("marketplace_id")
    if "country" in cols:
        select_cols.append("country")
    if "user_id" in cols:
        select_cols.append("user_id")
    if "currency" in cols:
        select_cols.append("currency")

    where_clauses = []
    params = {}

    if "user_id" in cols:
        where_clauses.append("user_id = :uid")
        params["uid"] = user_id

    if "marketplace_id" in cols:
        where_clauses.append("marketplace_id = :mkt_id")
        params["mkt_id"] = marketplace_id

    if "country" in cols:
        where_clauses.append("LOWER(country) = :country")
        params["country"] = country_key

    expected_currency = CURRENCY_BY_COUNTRY.get(country_key)

    if "currency" in cols and expected_currency:
        where_clauses.append("UPPER(currency) = :currency")
        params["currency"] = expected_currency

    where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"

    sql = text(f"""
        SELECT {', '.join(f'"{c}"' if "-" in c else c for c in select_cols)}
        FROM inventory_aged
        WHERE {where_sql}
    """)

    try:
        aged_df = pd.read_sql_query(sql, amazon_engine, params=params)
        if aged_df.empty:
            return aged_df

        aged_df["sku"] = aged_df["sku"].apply(norm_sku)

        # Keep latest snapshot if available
        if "snapshot_date" in aged_df.columns:
            aged_df["snapshot_date"] = pd.to_datetime(aged_df["snapshot_date"], errors="coerce")
            latest_snapshot = aged_df["snapshot_date"].max()
            if pd.notna(latest_snapshot):
                aged_df = aged_df[aged_df["snapshot_date"] == latest_snapshot].copy()

        # Deduplicate same SKU if multiple rows still exist
        aged_df = aged_df.sort_values(
            by=[c for c in ["snapshot_date"] if c in aged_df.columns]
        ).drop_duplicates(subset=["sku"], keep="last")

        return aged_df

    except ProgrammingError:
        logger.exception("ProgrammingError while reading inventory_aged")
        return pd.DataFrame()
    except Exception:
        logger.exception(
            "Failed loading inventory_aged for user=%s country=%s marketplace_id=%s",
            user_id, country_key, marketplace_id
        )
        return pd.DataFrame()

CURRENCY_BY_COUNTRY = {
    "us": "USD",
    "uk": "GBP",
}

def return_saved_current_inventory_table(user_id, country_key, month_name, year, error_msg=None):
    table_name = f"currentinventory_{user_id}_{country_key}_{month_name.lower()}{year}_table"

    if not table_exists(primary_engine, table_name):
        return jsonify({
            "error": error_msg or "Current inventory generation failed",
            "fallback_found": False,
            "table_name": table_name,
        }), 500

    try:
        saved_df = pd.read_sql_table(table_name, primary_engine)

        output = BytesIO()
        saved_df.to_excel(output, index=False, engine="openpyxl")
        output.seek(0)

        excel_b64 = base64.b64encode(output.read()).decode("utf-8")

        return jsonify({
            "message": "Current inventory loaded from saved table",
            "data": excel_b64,
            "filename": f"currentinventory_{user_id}_{country_key}_{month_name.lower()}{year}.xlsx",
            "inventory_alerts": {},
            "warnings": [
                "Live current inventory generation failed. Showing saved database table instead."
            ],
            "meta": {
                "user_id": user_id,
                "country": country_key,
                "month": month_name,
                "year": year,
                "table_name": table_name,
                "source": "saved_table",
                "original_error": str(error_msg) if error_msg else None,
            }
        }), 200

    except Exception as e:
        logger.exception("Failed loading fallback table %s", table_name)
        return jsonify({
            "error": "Current inventory generation failed and saved table could not be loaded",
            "details": str(e),
            "original_error": str(error_msg) if error_msg else None,
            "table_name": table_name,
        }), 500
    

@current_inventory_bp.route("/current_inventory", methods=["POST", "OPTIONS"])
def current_inventory():
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

    country_key = country.lower().strip()

    SessionLocal = sessionmaker(bind=db.engine)
    db_session = SessionLocal()

    warnings = []

    try:
        user = db_session.get(User, user_id)
        if user is None:
            return jsonify({"error": f"User not found for ID {user_id}"}), 404

        marketplace_id, marketplace_name, profile = resolve_marketplace(db_session, user_id, country_key)

        if not marketplace_id or not marketplace_name:
            return jsonify({"error": f'Unknown marketplace for country "{country}"'}), 400

        sku_table_name = f"sku_{user_id}_data_table"
        if not table_exists(primary_engine, sku_table_name):
            return jsonify({"error": f'SKU table "{sku_table_name}" does not exist'}), 500

        try:
            sku_df = pd.read_sql_table(sku_table_name, primary_engine)
        except Exception as e:
            logger.exception("Could not read SKU table %s", sku_table_name)
            return jsonify({"error": f'Could not read SKU table "{sku_table_name}": {e}'}), 500

        sku_column_name = f"sku_{country_key}"
        if sku_column_name not in sku_df.columns:
            return jsonify({"error": f"SKU column '{sku_column_name}' not found in {sku_table_name}"}), 400

        sku_df = sku_df[[sku_column_name, "product_name"]].copy()
        sku_df.rename(columns={sku_column_name: "sku"}, inplace=True)
        sku_df["sku"] = sku_df["sku"].apply(norm_sku)
        sku_df = sku_df[sku_df["sku"] != ""].drop_duplicates(subset=["sku"])

        month_number = datetime.strptime(month_name, "%B").month

        # UTC-safe boundaries
        month_start = datetime(year, month_number, 1, 0, 0, 0, tzinfo=timezone.utc)
        month_end = datetime(
            year,
            month_number,
            monthrange(year, month_number)[1],
            23, 59, 59,
            tzinfo=timezone.utc
        )

        current_month_col = f"Current Month Units Sold ({month_name})"
        amazon_engine = db.get_engine(bind="amazon")

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

        try:
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
                current_month_sales_df = current_month_sales_df.groupby("sku", as_index=False)[current_month_col].sum()
        except Exception:
            logger.exception("Failed loading liveorders sales")
            current_month_sales_df = pd.DataFrame(columns=["sku", current_month_col])
            warnings.append("Sales data could not be loaded.")

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
                inv_df = inv_df.sort_values(by=["synced_at"] if "synced_at" in inv_df.columns else ["seller_sku"])
                inv_df = inv_df.drop_duplicates(subset=["seller_sku"], keep="last")
        except Exception:
            logger.exception("Failed loading inventory table")
            warnings.append("Inventory snapshot data could not be loaded.")
            inv_df = pd.DataFrame()

        aged_df = load_aged_inventory(amazon_engine, user_id, country_key, marketplace_id)
        if aged_df.empty:
            warnings.append(
                f"Aged inventory data is not available for {country_key.upper()}. "
                f"This usually means the upstream SP-API aged inventory sync did not complete."
            )

        base_sales_df = (
            current_month_sales_df[["sku", current_month_col]]
            if not current_month_sales_df.empty
            else pd.DataFrame({"sku": sku_df["sku"], current_month_col: 0})
        )

        final_df = sku_df[["sku", "product_name"]].merge(
            base_sales_df,
            on="sku",
            how="left"
        )

        # Fetch last 30 days sales
        # Example for May report:
        # as_of = May 6
        # window = April 6 to May 5
        sales_30_as_of = date(year, month_number, 6)

        sales_30_df = fetch_last_30_days_units(
            user_id=user_id,
            country=country_key,
            as_of=sales_30_as_of,
            marketplace_name=marketplace_name
        )

        if not sales_30_df.empty:
            sales_30_df["sku"] = sales_30_df["sku"].apply(norm_sku)

            final_df = final_df.merge(
                sales_30_df.rename(
                    columns={"last_30_days_units": "Sales Last 30 Days"}
                ),
                on="sku",
                how="left"
            )

            final_df["Sales Last 30 Days"] = safe_numeric(
                final_df["Sales Last 30 Days"],
                0
            )
        else:
            final_df["Sales Last 30 Days"] = 0

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

        final_df[current_month_col] = safe_numeric(final_df[current_month_col], 0)
        final_df["inbound_quantity"] = safe_numeric(final_df.get("inbound_quantity"), 0)
        final_df["available"] = safe_numeric(final_df.get("available"), 0)

        final_df["Inventory Inwarded"] = final_df["inbound_quantity"]
        final_df["Inventory at the end of the month"] = final_df["available"]

        # Previous month window
        if month_number == 1:
            prev_month_number = 12
            prev_year = year - 1
        else:
            prev_month_number = month_number - 1
            prev_year = year

        prev_month_name_full = datetime(prev_year, prev_month_number, 1).strftime("%B")
        prev_month_name_lower = prev_month_name_full.lower()

        prev_table_name = f"user_{user_id}_{country_key}_{prev_month_name_lower}{prev_year}_data"
        final_df["Others"] = 0

        if table_exists(primary_engine, prev_table_name):
            # Use selected month context instead of server "today"
            selected_day = min(
                datetime.now(timezone.utc).day if (year == datetime.now(timezone.utc).year and month_number == datetime.now(timezone.utc).month)
                else monthrange(prev_year, prev_month_number)[1],
                monthrange(prev_year, prev_month_number)[1]
            )

            start_day = min(selected_day + 1, monthrange(prev_year, prev_month_number)[1])

            prev_start = datetime(prev_year, prev_month_number, start_day, 0, 0, 0, tzinfo=timezone.utc)
            prev_end = datetime(
                prev_year, prev_month_number, monthrange(prev_year, prev_month_number)[1],
                23, 59, 59, tzinfo=timezone.utc
            )

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
                prev_df = pd.read_sql_query(
                    prev_sql,
                    primary_engine,
                    params={"start": prev_start, "end": prev_end}
                )
                if not prev_df.empty:
                    prev_df["sku"] = prev_df["sku"].apply(norm_sku)
                    final_df = final_df.merge(prev_df[["sku", "others_qty"]], on="sku", how="left")
                    final_df["Others"] = safe_numeric(final_df["others_qty"], 0)
                    final_df.drop(columns=["others_qty"], inplace=True, errors="ignore")
            except Exception:
                logger.exception("Failed calculating Others from %s", prev_table_name)
                warnings.append("Previous month 'Others' calculation failed.")
        else:
            warnings.append(f"Previous month table {prev_table_name} not found; Others set to 0.")

        final_df["Inventory at the beginning of the month"] = (
            final_df["Inventory at the end of the month"]
            - final_df["Inventory Inwarded"]
            + final_df[current_month_col]
            - final_df["Others"]
        ).fillna(0).clip(lower=0)

        final_df.rename(columns={"sku": "SKU", "product_name": "Product Name"}, inplace=True)

        if "seller_sku" in final_df.columns:
            final_df.drop(columns=["seller_sku"], inplace=True)

        final_df.insert(0, "Sno.", range(1, len(final_df) + 1))

        desired_order = [
            "Sno.",
            "SKU",
            "Product Name",
            "inbound_quantity",
            "available",
            "inv-age-0-to-90-days",
            "inv-age-91-to-180-days",
            "inv-age-181-to-270-days",
            "inv-age-271-to-365-days",
            "inv-age-365-plus-days",
            "sales-rank",
            "estimated-storage-cost-next-month",
            "Coverage Ratio (In Months)",
            "Inventory Alerts",
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

        numeric_columns = final_df.select_dtypes(include=["number"]).columns
        total_row = {
            col: (final_df[col].sum() if col in numeric_columns and col != "Sno." else "")
            for col in final_df.columns
        }
        total_row["Product Name"] = "Total"
        final_df = pd.concat([final_df, pd.DataFrame([total_row])], ignore_index=True)
        # ---------------- NEW COLUMNS ---------------- #


        final_df["Coverage Ratio (In Months)"] = (
            final_df["Inventory at the end of the month"]
            / final_df["Sales Last 30 Days"].replace(0, pd.NA)
        ).fillna(0).round(2)

        inventory_alerts = generate_inventory_alerts_for_all_skus(
            user_id=user_id,
            country=country_key,
            coverage_df=final_df
        )

        final_df["Inventory Alerts"] = final_df["SKU"].map(
            lambda sku: inventory_alerts.get(str(sku).strip().upper(), {}).get("alert", "")
        )

        filename = f"currentinventory_{user_id}_{country_key}_{month_name.lower()}{year}.xlsx"
        # Save current inventory report into database table
        current_inventory_table_name = (
            f"currentinventory_{user_id}_{country_key}_{month_name.lower()}{year}_table"
        )

        try:
            final_df.to_sql(
                current_inventory_table_name,
                primary_engine,
                if_exists="replace",
                index=False
            )
        except Exception as e:
            logger.exception(
                "Failed saving current inventory table %s",
                current_inventory_table_name
            )
            warnings.append(
                f"Current inventory database table could not be saved: {e}"
            )

        output = BytesIO()
        final_df.to_excel(output, index=False, engine="openpyxl")
        output.seek(0)
        excel_b64 = base64.b64encode(output.read()).decode("utf-8")
        skuwise_df = final_df[
            final_df["Product Name"].astype(str).str.lower() != "total"
        ].copy()

        # Replace infinities first, then pandas/numpy missing values
        skuwise_df = skuwise_df.replace([np.inf, -np.inf], np.nan)
        skuwise_df = skuwise_df.astype(object).where(pd.notnull(skuwise_df), None)

        # Convert pandas/numpy/NaN values into valid JSON-safe Python values
        skuwise_items = clean_json_value(skuwise_df.to_dict(orient="records"))

        # Also clean alerts/meta in case any numpy values are inside
        inventory_alerts = clean_json_value(inventory_alerts)
        warnings = clean_json_value(warnings)

        response_payload = {
            "message": "Current inventory report generated successfully",
            "data": excel_b64,
            "filename": filename,
            "inventory_alerts": inventory_alerts,
            "skuwise_items": skuwise_items,
            "warnings": warnings,
            "meta": {
                "user_id": int(user_id),
                "country": country_key,
                "marketplace_id": marketplace_id,
                "marketplace_name": marketplace_name,
                "month": month_name,
                "year": int(year),
                "table_name": current_inventory_table_name,
                "aged_inventory_rows": 0 if aged_df.empty else int(len(aged_df)),
                "inventory_rows": 0 if inv_df.empty else int(len(inv_df)),
                "sales_rows": 0 if current_month_sales_df.empty else int(len(current_month_sales_df)),
            }
        }

        return jsonify(clean_json_value(response_payload)), 200
    except Exception as e:
        logger.exception("Current inventory generation failed; trying saved table fallback")
        return return_saved_current_inventory_table(
            user_id=user_id,
            country_key=country_key,
            month_name=month_name,
            year=year,
            error_msg=e,
        )

    finally:
        db_session.close()