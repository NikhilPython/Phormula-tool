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
        "asin",
        "available",
        "fc-transfer",
        "unfulfillable-quantity",
        "inbound-quantity",
        "inbound-working",
        "inbound-shipped",
        "inbound-received",
        "inv-age-0-to-90-days",
        "inv-age-91-to-180-days",
        "inv-age-181-to-270-days",
        "inv-age-271-to-365-days",
        "inv-age-365-plus-days",
        "sales-rank",
        "estimated-storage-cost-next-month",
    ]
    select_cols = [c for c in select_cols if c in cols]

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

    if "marketplace" in cols:
        where_clauses.append("marketplace = :mkt_id")
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
    

def generate_inventory_for_country(user_id, country_key, month_name, year):
    SessionLocal = sessionmaker(bind=db.engine)
    db_session = SessionLocal()
    warnings = []

    try:
        user = db_session.get(User, user_id)
        if user is None:
            raise Exception(f"User not found for ID {user_id}")

        marketplace_id, marketplace_name, profile = resolve_marketplace(
            db_session, user_id, country_key
        )

        if not marketplace_id or not marketplace_name:
            raise Exception(f'Unknown marketplace for country "{country_key}"')

        sku_table_name = f"sku_{user_id}_data_table"
        if not table_exists(primary_engine, sku_table_name):
            raise Exception(f'SKU table "{sku_table_name}" does not exist')

        sku_df = pd.read_sql_table(sku_table_name, primary_engine)

        sku_column_name = f"sku_{country_key}"
        if sku_column_name not in sku_df.columns:
            raise Exception(f"SKU column '{sku_column_name}' not found in {sku_table_name}")

        sku_df = sku_df[[sku_column_name, "product_name"]].copy()
        sku_df.rename(columns={sku_column_name: "sku"}, inplace=True)
        sku_df["sku"] = sku_df["sku"].apply(norm_sku)
        sku_df = sku_df[sku_df["sku"] != ""].drop_duplicates(subset=["sku"])

        month_number = datetime.strptime(month_name, "%B").month

        month_start = datetime(year, month_number, 1, 0, 0, 0, tzinfo=timezone.utc)
        month_end = datetime(
            year,
            month_number,
            monthrange(year, month_number)[1],
            23, 59, 59,
            tzinfo=timezone.utc
        )

        now_utc = datetime.now(timezone.utc)

        if year == now_utc.year and month_number == now_utc.month:
            month_end = now_utc

        current_month_col = f"Current Month Units Sold ({month_name})"
        amazon_engine = db.get_engine(bind="amazon")

        try:
            skuwise_monthly_table = (
                f"skuwisemonthly_{user_id}_{country_key}_{month_name.lower()}_{year}"
            )

            if table_exists(primary_engine, skuwise_monthly_table):
                sales_sql = text(f"""
                    SELECT
                        sku,
                        SUM(
                            COALESCE(
                                total_quantity,
                                quantity - COALESCE(return_quantity, 0),
                                quantity,
                                0
                            )
                        ) AS "{current_month_col}"
                    FROM public.{skuwise_monthly_table}
                    WHERE LOWER(COALESCE(product_name, '')) != 'total'
                    GROUP BY sku
                """)

                current_month_sales_df = pd.read_sql_query(
                    sales_sql,
                    primary_engine,
                )

            else:
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
                current_month_sales_df = current_month_sales_df.groupby(
                    "sku", as_index=False
                )[current_month_col].sum()

        except Exception:
            logger.exception("Failed loading current month sales")
            current_month_sales_df = pd.DataFrame(columns=["sku", current_month_col])
            warnings.append("Sales data could not be loaded.")

        try:
            inv_sql = text("""
                SELECT seller_sku,
                       asin,
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
                inv_df = inv_df.sort_values(
                    by=["synced_at"] if "synced_at" in inv_df.columns else ["seller_sku"]
                )
                inv_df = inv_df.drop_duplicates(subset=["seller_sku"], keep="last")
        except Exception:
            logger.exception("Failed loading inventory table")
            warnings.append("Inventory snapshot data could not be loaded.")
            inv_df = pd.DataFrame()

        aged_df = load_aged_inventory(amazon_engine, user_id, country_key, marketplace_id)

        if aged_df.empty:
            warnings.append(
                f"Aged inventory data is not available for {country_key.upper()}."
            )

        base_sales_df = (
            current_month_sales_df[["sku", current_month_col]]
            if not current_month_sales_df.empty
            else pd.DataFrame({"sku": sku_df["sku"], current_month_col: 0})
        )

        # ------------------------------------------------------------
        # Base rows:
        # 1) SKU master rows first
        # 2) Amazon-only SKUs appended at bottom
        # No extra output column
        # ------------------------------------------------------------

        sku_master_df = sku_df[["sku", "product_name"]].copy()
        sku_master_df["row_order"] = 1

        extra_skus = []

        # SKUs from aged inventory that are not in SKU master
        if not aged_df.empty and "sku" in aged_df.columns:
            aged_extra = aged_df[["sku"]].copy()

            if "product-name" in aged_df.columns:
                aged_extra["product_name"] = aged_df["product-name"]
            elif "product_name" in aged_df.columns:
                aged_extra["product_name"] = aged_df["product_name"]
            else:
                aged_extra["product_name"] = ""

            extra_skus.append(aged_extra)

        # SKUs from inventory summary that are not in SKU master
        if not inv_df.empty and "seller_sku" in inv_df.columns:
            inv_extra = inv_df[["seller_sku"]].copy()
            inv_extra.rename(columns={"seller_sku": "sku"}, inplace=True)

            if "product_name" in inv_df.columns:
                inv_extra["product_name"] = inv_df["product_name"]
            else:
                inv_extra["product_name"] = ""

            extra_skus.append(inv_extra)

        if extra_skus:
            amazon_skus_df = pd.concat(extra_skus, ignore_index=True)
            amazon_skus_df["sku"] = amazon_skus_df["sku"].apply(norm_sku)
            amazon_skus_df = amazon_skus_df[amazon_skus_df["sku"] != ""]

            master_sku_set = set(sku_master_df["sku"].apply(norm_sku))

            # Keep only Amazon SKUs not already in SKU master
            amazon_skus_df = amazon_skus_df[
                ~amazon_skus_df["sku"].isin(master_sku_set)
            ].copy()

            amazon_skus_df["row_order"] = 2

            base_skus_df = pd.concat(
                [sku_master_df, amazon_skus_df],
                ignore_index=True
            )
        else:
            base_skus_df = sku_master_df.copy()

        # Clean + remove duplicates
        base_skus_df["sku"] = base_skus_df["sku"].apply(norm_sku)
        base_skus_df["product_name"] = base_skus_df["product_name"].fillna("")

        base_skus_df["_has_name"] = (
            base_skus_df["product_name"].astype(str).str.strip() != ""
        )

        base_skus_df = (
            base_skus_df
            .sort_values(
                by=["row_order", "sku", "_has_name"],
                ascending=[True, True, False]
            )
            .drop_duplicates(subset=["sku"], keep="first")
            .drop(columns=["_has_name"])
        )

        final_df = base_skus_df.merge(
            base_sales_df,
            on="sku",
            how="left"
        )

        today_utc = datetime.now(timezone.utc).date()

        # If selected month is current month, use today.
        # If selected month is a past month, use that month's last day.
        # If selected month is future, also cap to today to avoid future data.
        selected_month_last_day = date(
            year,
            month_number,
            monthrange(year, month_number)[1]
        )

        if year == today_utc.year and month_number == today_utc.month:
            sales_30_as_of = today_utc
        elif selected_month_last_day < today_utc:
            sales_30_as_of = selected_month_last_day
        else:
            sales_30_as_of = today_utc

        sales_30_df = fetch_last_30_days_units(
            user_id=user_id,
            country=country_key,
            as_of=sales_30_as_of,
            marketplace_name=marketplace_name
        )

        if not sales_30_df.empty:
            sales_30_df["sku"] = sales_30_df["sku"].apply(norm_sku)
            final_df = final_df.merge(
                sales_30_df.rename(columns={"last_30_days_units": "Sales Last 30 Days"}),
                on="sku",
                how="left"
            )
            final_df["Sales Last 30 Days"] = safe_numeric(final_df["Sales Last 30 Days"], 0)
        else:
            final_df["Sales Last 30 Days"] = 0

        if not aged_df.empty:
            final_df = final_df.merge(aged_df, on="sku", how="left")

            # Fill missing product names from Amazon aged report, but do not add new column
            if "product-name" in final_df.columns:
                final_df["product_name"] = final_df["product_name"].where(
                    final_df["product_name"].astype(str).str.strip() != "",
                    final_df["product-name"]
                )

            if "product_name_y" in final_df.columns:
                final_df["product_name"] = final_df["product_name"].where(
                    final_df["product_name"].astype(str).str.strip() != "",
                    final_df["product_name_y"]
                )

            final_df.drop(
                columns=["product-name", "product_name_y"],
                inplace=True,
                errors="ignore"
            )
        else:
            for c in [
                "available",
                "fc-transfer",
                "unfulfillable-quantity",
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

            # Normalize ASIN after merging aged inventory + inventory summary.
            # Same ASIN should be counted only one time.
            if "asin_x" in final_df.columns or "asin_y" in final_df.columns:
                final_df["asin"] = final_df.get("asin_x")

                if "asin_y" in final_df.columns:
                    final_df["asin"] = final_df["asin"].where(
                        final_df["asin"].fillna("").astype(str).str.strip() != "",
                        final_df["asin_y"]
                    )

                final_df.drop(columns=["asin_x", "asin_y"], inplace=True, errors="ignore")
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

        # ✅ inbound should come from inventory_aged first
        aged_inbound_quantity = safe_numeric(final_df.get("inbound-quantity"), 0)
        aged_inbound_working = safe_numeric(final_df.get("inbound-working"), 0)
        aged_inbound_shipped = safe_numeric(final_df.get("inbound-shipped"), 0)
        aged_inbound_received = safe_numeric(final_df.get("inbound-received"), 0)

        # inventory summary fallback
        summary_inbound_quantity = safe_numeric(final_df.get("inbound_quantity"), 0)

        # Amazon aged report sometimes gives inbound-quantity directly.
        # If inbound-quantity is 0, calculate it from working + shipped + received.
        calculated_aged_inbound = aged_inbound_quantity.where(
            aged_inbound_quantity > 0,
            aged_inbound_working + aged_inbound_shipped + aged_inbound_received
        )

        # Final inbound: aged first, then inventory summary fallback
        final_df["inbound_quantity"] = calculated_aged_inbound.where(
            calculated_aged_inbound > 0,
            summary_inbound_quantity
        )

        aged_available = safe_numeric(final_df.get("available"), 0)
        fc_transfer = safe_numeric(final_df.get("fc-transfer"), 0)
        summary_available = safe_numeric(final_df.get("available_quantity"), 0)

        # Current Inventory = Amazon available + fc-transfer.
        # If aged available is missing/zero, fallback to inventory summary available_quantity.
        final_df["available"] = (
            aged_available.where(aged_available > 0, summary_available)
            + fc_transfer
        )

        final_df["Inventory Inwarded"] = final_df["inbound_quantity"]
        final_df["Inventory at the end of the month"] = final_df["available"]

        if month_number == 1:
            prev_month_number = 12
            prev_year = year - 1
        else:
            prev_month_number = month_number - 1
            prev_year = year

        prev_month_name_lower = datetime(prev_year, prev_month_number, 1).strftime("%B").lower()
        prev_table_name = f"user_{user_id}_{country_key}_{prev_month_name_lower}{prev_year}_data"

        final_df["Others"] = 0

        if table_exists(primary_engine, prev_table_name):
            selected_day = min(
                datetime.now(timezone.utc).day
                if (year == datetime.now(timezone.utc).year and month_number == datetime.now(timezone.utc).month)
                else monthrange(prev_year, prev_month_number)[1],
                monthrange(prev_year, prev_month_number)[1]
            )

            start_day = min(selected_day + 1, monthrange(prev_year, prev_month_number)[1])

            prev_start = datetime(prev_year, prev_month_number, start_day, 0, 0, 0, tzinfo=timezone.utc)
            prev_end = datetime(
                prev_year,
                prev_month_number,
                monthrange(prev_year, prev_month_number)[1],
                23, 59, 59,
                tzinfo=timezone.utc
            )

            try:
                prev_sql = text(f"""
                    SELECT sku, SUM(quantity) AS others_qty
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
                warnings.append("Previous month Others calculation failed.")
        else:
            warnings.append(f"Previous month table {prev_table_name} not found; Others set to 0.")

        final_df["Inventory at the beginning of the month"] = (
            final_df["Inventory at the end of the month"]
            - final_df["Inventory Inwarded"]
            + final_df[current_month_col]
            - final_df["Others"]
        ).fillna(0).clip(lower=0)

        # ------------------------------------------------------------
        # Remove duplicate product rows BEFORE total row calculation.
        # Case: same ASIN / same product appears with 2 SKUs in same country
        # Example: SEWIPESLIDCO and SEWIPESLIDS both mapped to Wipes + Wipes.
        # We keep only one row so totals are not counted twice.
        # ------------------------------------------------------------
        if "asin" in final_df.columns:
            final_df["_asin_clean"] = (
                final_df["asin"]
                .fillna("")
                .astype(str)
                .str.strip()
                .str.upper()
            )
        else:
            final_df["_asin_clean"] = ""

        final_df["_product_clean"] = (
            final_df["product_name"]
            .fillna("")
            .astype(str)
            .str.strip()
            .str.lower()
        )

        final_df["_sku_clean"] = final_df["sku"].fillna("").astype(str).str.strip().str.upper()

        # Prefer ASIN for dedupe. If ASIN is missing, fallback to product_name.
        final_df["_dedupe_key"] = np.where(
            final_df["_asin_clean"] != "",
            "ASIN::" + final_df["_asin_clean"],
            np.where(
                final_df["_product_clean"] != "",
                "PRODUCT::" + final_df["_product_clean"],
                "SKU::" + final_df["_sku_clean"]
            )
        )

        # Prefer master SKU rows first, then row with higher available inventory.
        sort_cols = []
        ascending = []

        if "row_order" in final_df.columns:
            sort_cols.append("row_order")
            ascending.append(True)

        if "available" in final_df.columns:
            sort_cols.append("available")
            ascending.append(False)

        sort_cols.append("_sku_clean")
        ascending.append(True)

        final_df = (
            final_df
            .sort_values(by=sort_cols, ascending=ascending)
            .drop_duplicates(subset=["_dedupe_key"], keep="first")
            .drop(columns=[
                "_asin_clean",
                "_product_clean",
                "_sku_clean",
                "_dedupe_key",
            ], errors="ignore")
            .reset_index(drop=True)
        )

        final_df.rename(columns={"sku": "SKU", "product_name": "Product Name"}, inplace=True)

        if "seller_sku" in final_df.columns:
            final_df.drop(columns=["seller_sku"], inplace=True)

        final_df.insert(0, "Sno.", range(1, len(final_df) + 1))

        if "row_order" in final_df.columns:
            final_df = final_df.sort_values(
                by=["row_order", "SKU"],
                ascending=[True, True]
            ).reset_index(drop=True)

        # Use the same value that is displayed as "Current Inventory"
        current_inventory_for_coverage = safe_numeric(final_df["available"], 0)
        sales_last_30_days = safe_numeric(final_df["Sales Last 30 Days"], 0)

        final_df["Coverage Ratio (In Months)"] = (
            current_inventory_for_coverage
            / sales_last_30_days.replace(0, pd.NA)
        ).fillna(0).round(2)

        inventory_alerts = generate_inventory_alerts_for_all_skus(
            user_id=user_id,
            country=country_key,
            coverage_df=final_df
        )

        final_df["Inventory Alerts"] = final_df["SKU"].map(
            lambda sku: inventory_alerts.get(str(sku).strip().upper(), {}).get("alert", "")
        )

        desired_order = [
            "Sno.",
            "SKU",
            "Product Name",
            "inbound_quantity",
            "inbound-working",
            "inbound-shipped",
            "inbound-received",
            "available",
            "unfulfillable-quantity",
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
            "Sales Last 30 Days",
        ]

        final_df = final_df.reindex(
            columns=[c for c in desired_order if c in final_df.columns]
            + [c for c in final_df.columns if c not in desired_order]
        )
        final_df.drop(columns=["row_order"], inplace=True, errors="ignore")

        numeric_columns = final_df.select_dtypes(include=["number"]).columns
        total_row = {
            col: (final_df[col].sum() if col in numeric_columns and col != "Sno." else "")
            for col in final_df.columns
        }

        total_row["Product Name"] = "Total"

        total_current_inventory = float(safe_numeric(final_df["available"], 0).sum())
        total_sales_30 = float(safe_numeric(final_df["Sales Last 30 Days"], 0).sum())

        total_row["Coverage Ratio (In Months)"] = (
            round(total_current_inventory / total_sales_30, 2)
            if total_sales_30 > 0
            else 0
        )

        final_df = pd.concat([final_df, pd.DataFrame([total_row])], ignore_index=True)

        filename = f"currentinventory_{user_id}_{country_key}_{month_name.lower()}{year}.xlsx"
        current_inventory_table_name = f"currentinventory_{user_id}_{country_key}_{month_name.lower()}{year}_table"

        try:
            final_df.to_sql(
                current_inventory_table_name,
                primary_engine,
                if_exists="replace",
                index=False
            )
        except Exception as e:
            logger.exception("Failed saving current inventory table %s", current_inventory_table_name)
            warnings.append(f"Current inventory database table could not be saved: {e}")

        output = BytesIO()
        final_df.to_excel(output, index=False, engine="openpyxl")
        output.seek(0)
        excel_b64 = base64.b64encode(output.read()).decode("utf-8")

        skuwise_df = final_df[
            final_df["Product Name"].astype(str).str.lower() != "total"
        ].copy()

        skuwise_df = skuwise_df.replace([np.inf, -np.inf], np.nan)
        skuwise_df = skuwise_df.astype(object).where(pd.notnull(skuwise_df), None)

        skuwise_items = clean_json_value(skuwise_df.to_dict(orient="records"))
        inventory_alerts = clean_json_value(inventory_alerts)
        warnings = clean_json_value(warnings)

        return {
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

    finally:
        db_session.close()


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

    try:
        if country_key == "global":
            uk_data = generate_inventory_for_country(user_id, "uk", month_name, year)
            us_data = generate_inventory_for_country(user_id, "us", month_name, year)

            return jsonify(clean_json_value({
                "message": "Global inventory fetched successfully",
                "skuwise_items_uk": uk_data.get("skuwise_items", []),
                "skuwise_items_us": us_data.get("skuwise_items", []),
                "inventory_alerts_uk": uk_data.get("inventory_alerts", {}),
                "inventory_alerts_us": us_data.get("inventory_alerts", {}),
                "warnings_uk": uk_data.get("warnings", []),
                "warnings_us": us_data.get("warnings", []),
                "meta": {
                    "uk": uk_data.get("meta", {}),
                    "us": us_data.get("meta", {})
                }
            })), 200

        if country_key not in ["uk", "us"]:
            return jsonify({"error": "Country must be uk, us, or global"}), 400

        result = generate_inventory_for_country(user_id, country_key, month_name, year)

        return jsonify(clean_json_value(result)), 200

    except Exception as e:
        logger.exception("Current inventory generation failed")
        return jsonify({"error": str(e)}), 500
    
    