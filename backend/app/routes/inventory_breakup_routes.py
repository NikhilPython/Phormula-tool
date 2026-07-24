from __future__ import annotations

import os, re, io, csv, time, gzip, logging , jwt, calendar, requests
from datetime import datetime, date
from typing import Optional
from dotenv import find_dotenv, load_dotenv
from sqlalchemy.orm import load_only
from flask import request, jsonify, Blueprint
from sqlalchemy.dialects.postgresql import insert, insert as pg_insert
from sqlalchemy.exc import SQLAlchemyError
from app import db
from sqlalchemy import delete, text
from app.models.user_models import Inventory, CountryProfile, MonthwiseInventory , InventoryAged
from app.utils.token_utils import get_effective_user_id_from_token
from app.utils.amazon_utils import amazon_client, _apply_region_and_marketplace_from_request
from app.utils.live_bi_utils import generate_inventory_alerts_for_all_skus
from config import Config
from contextlib import contextmanager
from sqlalchemy import create_engine, text

# ---------------------------------------------------------------------------
# basic config
# ---------------------------------------------------------------------------

SECRET_KEY = Config.SECRET_KEY

dotenv_path = find_dotenv(filename=".env", usecwd=True)
load_dotenv(dotenv_path, override=True)
db_url = os.getenv("DATABASE_URL")
db_url1 = os.getenv("DATABASE_ADMIN_URL") or db_url
db_url_amazon = os.getenv("DATABASE_AMAZON_URL") or db_url
if not db_url:
    raise RuntimeError("DATABASE_URL is not set")
if not db_url1:
    print("[WARN] DATABASE_ADMIN_URL not set; falling back to DATABASE_URL")
if not db_url_amazon:
    print("[WARN] DATABASE_AMAZON_URL not set; falling back to DATABASE_URL")

# ---------------------------------------------------------------------
# AMAZON DB ENGINE (amazon_db)
# ---------------------------------------------------------------------

DATABASE_AMAZON_URL = os.getenv("DATABASE_AMAZON_URL") 
if not DATABASE_AMAZON_URL:
    raise RuntimeError("DATABASE_AMAZON_URL is missing in .env")

amazon_engine = create_engine(DATABASE_AMAZON_URL, pool_pre_ping=True)

inventory_breakup_bp = Blueprint("inventory_breakup", __name__)



def _quarter_range(year: int, quarter: int) -> tuple[date, date]:
    if quarter not in (1, 2, 3, 4):
        raise ValueError("quarter must be 1, 2, 3, or 4")
    if year < 2000 or year > 2100:
        raise ValueError("year must be between 2000 and 2100")

    start_month = {1: 1, 2: 4, 3: 7, 4: 10}[quarter]
    end_month = start_month + 2

    start_date = date(year, start_month, 1)
    end_dom = calendar.monthrange(year, end_month)[1]
    end_date = date(year, end_month, end_dom)  # inclusive end
    return start_date, end_date


def _year_range(year: int) -> tuple[date, date]:
    if year < 2000 or year > 2100:
        raise ValueError("year must be between 2000 and 2100")
    return date(year, 1, 1), date(year, 12, 31)


@inventory_breakup_bp.route("/api/inventory_breakup", methods=["GET"])
def inventory_breakup():
    # ---------------------------
    # Auth (same as your code)
    # ---------------------------
    auth_header = request.headers.get("Authorization")
    user_id = None
    if auth_header and auth_header.startswith("Bearer "):
        token = auth_header.split(" ")[1]
        try:
            payload, user_id, member_id = get_effective_user_id_from_token(token)
            user_id = payload.get("user_id")
        except jwt.ExpiredSignatureError:
            return jsonify({"success": False, "error": "Token has expired"}), 401
        except jwt.InvalidTokenError:
            return jsonify({"success": False, "error": "Invalid token"}), 401

    if not user_id:
        return jsonify({"success": False, "error": "Missing/invalid Authorization token"}), 401

    _apply_region_and_marketplace_from_request()

    # ---------------------------
    # Params
    # ---------------------------
    mode = (request.args.get("mode") or "month").strip().lower()  # month | quarter | year
    month_str = (request.args.get("month") or "").strip()
    quarter_str = (request.args.get("quarter") or "").strip().lower()  # q1/q2/q3/q4
    year_str = (request.args.get("year") or "").strip()

    marketplace_id = (request.args.get("marketplace_id") or amazon_client.marketplace_id or "").strip()

    if not year_str:
        return jsonify({"success": False, "error": "year is required"}), 400

    try:
        year_num = int(year_str)
    except Exception:
        return jsonify({"success": False, "error": "Invalid year. Example year=2025"}), 400

    # ---------------------------
    # Decide date range
    # ---------------------------
    try:
        if mode == "month":
            if not month_str:
                return jsonify({"success": False, "error": "month is required when mode=month"}), 400
            month_num = datetime.strptime(month_str[:3].title(), "%b").month
            start_date = date(year_num, month_num, 1)
            # end = first day next month (exclusive)
            if month_num == 12:
                end_date = date(year_num + 1, 1, 1)
            else:
                end_date = date(year_num, month_num + 1, 1)

        elif mode == "quarter":
            if quarter_str not in ("q1", "q2", "q3", "q4"):
                return jsonify({"success": False, "error": "quarter must be q1, q2, q3, or q4 when mode=quarter"}), 400

            q = int(quarter_str[1])
            start_date, end_inclusive = _quarter_range(year_num, q)
            # make exclusive end by adding 1 day
            end_date = end_inclusive.replace(day=end_inclusive.day)  # just to be explicit
            end_date = end_inclusive.toordinal()  # convert to ordinal then add 1
            end_date = date.fromordinal(end_date + 1)

        elif mode == "year":
            start_date = date(year_num, 1, 1)
            end_date = date(year_num + 1, 1, 1)  # exclusive

        else:
            return jsonify({"success": False, "error": "mode must be month, quarter, or year"}), 400

    except Exception as e:
        return jsonify({"success": False, "error": f"Invalid date inputs: {str(e)}"}), 400

    # ---------------------------
    # SQL (range -> last_date -> totals on that date)
    # ---------------------------
    sql = text("""
        WITH range_rows AS (
            SELECT
                date,
                disposition,
                COALESCE(ending_warehouse_balance, 0) AS qty
            FROM public.monthwise_inventory
            WHERE user_id = :user_id
              AND (:marketplace_id = '' OR marketplace_id = :marketplace_id)
              AND date >= :start_date
              AND date < :end_date
        ),
        last_day AS (
            SELECT MAX(date) AS last_date
            FROM range_rows
        )
        SELECT
            ld.last_date AS date,

            SUM(CASE WHEN UPPER(rr.disposition) = 'SELLABLE'
                     THEN rr.qty ELSE 0 END) AS sellable,

            SUM(CASE WHEN UPPER(rr.disposition) = 'DEFECTIVE'
                     THEN rr.qty ELSE 0 END) AS defective,

            SUM(CASE WHEN UPPER(rr.disposition) = 'EXPIRED'
                     THEN rr.qty ELSE 0 END) AS expired,

            SUM(CASE WHEN UPPER(rr.disposition) IN ('CUSTOMER_DAMAGED', 'CUSTOMER DAMAGED')
                     THEN rr.qty ELSE 0 END) AS customer_damaged,

            SUM(CASE WHEN UPPER(rr.disposition) IN (
                        'WAREHOUSE_DAMAGED', 'WAREHOUSE DAMAGED',
                        'WAREHOUSE_DAMAHED', 'WAREHOUSE DAMAHED'
                     )
                     THEN rr.qty ELSE 0 END) AS warehouse_damaged,

            SUM(CASE WHEN UPPER(rr.disposition) IN ('DISTRIBUTOR_DAMAGED', 'DISTRIBUTOR DAMAGED')
                     THEN rr.qty ELSE 0 END) AS distributor_damaged

        FROM range_rows rr
        CROSS JOIN last_day ld
        WHERE rr.date = ld.last_date
        GROUP BY ld.last_date;
    """)

    try:
        with amazon_engine.connect() as conn:
            row = conn.execute(sql, {
                "user_id": user_id,
                "marketplace_id": marketplace_id,
                "start_date": start_date,
                "end_date": end_date,
            }).mappings().first()
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

    # no data in range
    if not row or row["date"] is None:
        return jsonify({
            "success": True,
            "mode": mode,
            "user_id": user_id,
            "marketplace_id": marketplace_id or None,
            "year": year_num,
            "month": month_str or None,
            "quarter": quarter_str or None,
            "range": {"start_date": str(start_date), "end_date_exclusive": str(end_date)},
            "last_date": None,
            "totals": {
                "sellable": 0,
                "defective": 0,
                "expired": 0,
                "customer_damaged": 0,
                "warehouse_damaged": 0,
                "distributor_damaged": 0,
            }
        }), 200

    totals = {
        "sellable": int(row["sellable"] or 0),
        "defective": int(row["defective"] or 0),
        "expired": int(row["expired"] or 0),
        "customer_damaged": int(row["customer_damaged"] or 0),
        "warehouse_damaged": int(row["warehouse_damaged"] or 0),
        "distributor_damaged": int(row["distributor_damaged"] or 0),
    }

    return jsonify({
        "success": True,
        "mode": mode,
        "user_id": user_id,
        "marketplace_id": marketplace_id or None,
        "year": year_num,
        "month": month_str or None,
        "quarter": quarter_str or None,
        "range": {"start_date": str(start_date), "end_date_exclusive": str(end_date)},
        "last_date": str(row["date"]),
        "totals": totals
    }), 200


@inventory_breakup_bp.route("/api/inventory_ageing", methods=["GET"])
def inventory_ageing():
    # --- auth (same pattern) ---
    auth_header = request.headers.get("Authorization")
    user_id = None
    if auth_header and auth_header.startswith("Bearer "):
        token = auth_header.split(" ")[1]
        try:
            payload, user_id, member_id = get_effective_user_id_from_token(token)
            user_id = payload.get("user_id")
        except jwt.ExpiredSignatureError:
            return jsonify({"success": False, "error": "Token has expired"}), 401
        except jwt.InvalidTokenError:
            return jsonify({"success": False, "error": "Invalid token"}), 401

    if not user_id:
        return jsonify({"success": False, "error": "Missing/invalid Authorization token"}), 401

    _apply_region_and_marketplace_from_request()

    # Keep param name marketplace_id for API, but map it to DB column "marketplace"
    marketplace = (request.args.get("marketplace_id") or amazon_client.marketplace_id or "").strip()

    sql = text("""
        SELECT
            SUM(COALESCE("inv-age-0-to-90-days", 0))     AS age_0_90,
            SUM(COALESCE("inv-age-91-to-180-days", 0))   AS age_91_180,
            SUM(COALESCE("inv-age-181-to-270-days", 0))  AS age_181_270,
            SUM(COALESCE("inv-age-271-to-365-days", 0))  AS age_271_365,
            SUM(COALESCE("inv-age-365-plus-days", 0))    AS age_365_plus
        FROM public.inventory_aged
        WHERE user_id = :user_id
          AND (:marketplace = '' OR marketplace = :marketplace);
    """)

    try:
        with amazon_engine.connect() as conn:
            row = conn.execute(sql, {
                "user_id": user_id,
                "marketplace": marketplace,
            }).mappings().first()
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

    if not row:
        return jsonify({
            "success": True,
            "user_id": user_id,
            "marketplace": marketplace or None,
            "totals": {
                "age_0_90": 0,
                "age_91_180": 0,
                "age_181_270": 0,
                "age_271_365": 0,
                "age_365_plus": 0,
                "total_units": 0
            }
        }), 200

    totals = {
        "age_0_90": int(row["age_0_90"] or 0),
        "age_91_180": int(row["age_91_180"] or 0),
        "age_181_270": int(row["age_181_270"] or 0),
        "age_271_365": int(row["age_271_365"] or 0),
        "age_365_plus": int(row["age_365_plus"] or 0),
    }
    totals["total_units"] = sum(totals.values())

    return jsonify({
        "success": True,
        "user_id": user_id,
        "marketplace": marketplace or None,
        "totals": totals
    }), 200


LOST_DESCRIPTIONS = {
    "REVERSAL_REIMBURSEMENT",
    "WAREHOUSE_LOST",
    "WAREHOUSE_DAMAGE",
    "MISSING_FROM_INBOUND",
    "MISSING_FROM_INBOUND_CLAWBACK",
    "COMPENSATED_CLAWBACK",
}

MONTH_MAP = {
    "jan": "january",
    "feb": "february",
    "mar": "march",
    "apr": "april",
    "may": "may",
    "jun": "june",
    "jul": "july",
    "aug": "august",
    "sep": "september",
    "oct": "october",
    "nov": "november",
    "dec": "december",
}

MONTHS_FULL = [
    "january", "february", "march", "april", "may", "june",
    "july", "august", "september", "october", "november", "december"
]


@inventory_breakup_bp.route("/api/inventory_lost_compensation", methods=["GET"])
def inventory_lost_compensation():
    # ---------------------------
    # Auth
    # ---------------------------
    auth_header = request.headers.get("Authorization")
    user_id = None

    if auth_header and auth_header.startswith("Bearer "):
        token = auth_header.split(" ")[1]
        try:
            payload, user_id, member_id = get_effective_user_id_from_token(token)
            user_id = payload.get("user_id")
        except jwt.ExpiredSignatureError:
            return jsonify({"success": False, "error": "Token has expired"}), 401
        except jwt.InvalidTokenError:
            return jsonify({"success": False, "error": "Invalid token"}), 401

    if not user_id:
        return jsonify({"success": False, "error": "Missing/invalid Authorization token"}), 401

    # ---------------------------
    # Params
    # ---------------------------
    country = (request.args.get("country") or "").strip().lower()
    year = (request.args.get("year") or "").strip()
    mode = (request.args.get("mode") or "month").strip().lower()   # month | quarter | year
    month = (request.args.get("month") or "").strip().lower()
    quarter = (request.args.get("quarter") or "").strip().lower()  # q1 | q2 | q3 | q4

    if not country:
        return jsonify({"success": False, "error": "country is required"}), 400
    if not year:
        return jsonify({"success": False, "error": "year is required"}), 400
    if mode not in {"month", "quarter", "year"}:
        return jsonify({"success": False, "error": "mode must be month, quarter or year"}), 400

    month_num = None
    if mode == "month":
        if not month:
            return jsonify({"success": False, "error": "month is required when mode=month"}), 400
        if len(month) == 3:
            month = MONTH_MAP.get(month, month)
        try:
            month_num = datetime.strptime(month[:3].title(), "%b").month
        except Exception:
            return jsonify({"success": False, "error": "Invalid month"}), 400

    quarter_months_map = {
        "q1": ["january", "february", "march"],
        "q2": ["april", "may", "june"],
        "q3": ["july", "august", "september"],
        "q4": ["october", "november", "december"],
    }

    if mode == "quarter":
        if quarter not in quarter_months_map:
            return jsonify({"success": False, "error": "quarter must be q1, q2, q3, or q4 when mode=quarter"}), 400

    # ---------------------------
    # Safe table names
    # ---------------------------
    safe_country = re.sub(r"[^a-z0-9_]", "", country)
    safe_year = re.sub(r"[^0-9]", "", year)
    safe_user_id = re.sub(r"[^0-9]", "", str(user_id))
    safe_month = re.sub(r"[^a-z0-9_]", "", month) if month else ""
    safe_quarter = re.sub(r"[^a-z0-9_]", "", quarter) if quarter else ""

    if not safe_country or not safe_year or not safe_user_id:
        return jsonify({"success": False, "error": "Invalid table name inputs"}), 400
    if mode == "month" and not safe_month:
        return jsonify({"success": False, "error": "Invalid month"}), 400
    if mode == "quarter" and not safe_quarter:
        return jsonify({"success": False, "error": "Invalid quarter"}), 400

    sku_table_name = f"sku_{safe_user_id}_data_table"
    sku_table = f"public.{sku_table_name}"

    if mode == "month":
        settlement_table_names = [f"user_{safe_user_id}_{safe_country}_{safe_month}{safe_year}_data"]
        inventory_table_name = f"inventorymonthly_{safe_user_id}_{safe_country}_{month_num:02d}_{safe_year}"
        result_table_name = f"inventory_lost_compensation_{safe_user_id}_{safe_month}_{safe_country}_{safe_year}"

    elif mode == "quarter":
        quarter_months = quarter_months_map[safe_quarter]
        settlement_table_names = [
            f"user_{safe_user_id}_{safe_country}_{m}{safe_year}_data" for m in quarter_months
        ]
        result_table_name = f"inventory_lost_compensation_{safe_user_id}_{safe_quarter}_{safe_country}_{safe_year}"

        # adjust this if your quarterly inventory table uses a different naming pattern
        inventory_table_name = f"inventoryquarterly_{safe_user_id}_{safe_country}_{safe_quarter}_{safe_year}"

    else:
        settlement_table_names = [
            f"user_{safe_user_id}_{safe_country}_{m}{safe_year}_data" for m in MONTHS_FULL
        ]
        inventory_table_name = f"inventoryyearly_{safe_user_id}_{safe_country}_{safe_year}"
        result_table_name = f"inventory_lost_compensation_{safe_user_id}_{safe_country}_{safe_year}"

    inventory_table = f"public.{inventory_table_name}"
    result_table = f"public.{result_table_name}"
    sku_country_col = "sku_uk" if safe_country == "uk" else "sku_us"

    try:
        # -------------------------------------------------
        # MAIN DB: settlement + sku mapping
        # -------------------------------------------------
        with db.engine.connect() as conn:
            exists_sql = text("""
                SELECT EXISTS (
                    SELECT 1
                    FROM information_schema.tables
                    WHERE table_schema = 'public'
                      AND table_name = :table_name
                ) AS exists_flag
            """)

            sku_exists = conn.execute(exists_sql, {"table_name": sku_table_name}).mappings().first()
            if not sku_exists or not sku_exists["exists_flag"]:
                return jsonify({
                    "success": False,
                    "error": f"Table public.{sku_table_name} does not exist in main database",
                    "table_name": f"public.{sku_table_name}"
                }), 404

            existing_settlement_tables = []
            for tbl in settlement_table_names:
                row = conn.execute(exists_sql, {"table_name": tbl}).mappings().first()
                if row and row["exists_flag"]:
                    existing_settlement_tables.append(tbl)

            if not existing_settlement_tables:
                return jsonify({
                    "success": False,
                    "error": "No MTD data available for the requested period",
                    "settlement_tables_checked": [f"public.{t}" for t in settlement_table_names]
                }), 404

            settlement_union_sql = "\nUNION ALL\n".join([
                f"""
                SELECT
                    COALESCE(sku, '') AS sku,
                    COALESCE(description, '') AS description,
                    COALESCE(quantity, 0) AS quantity,
                    COALESCE(total, 0) AS total
                FROM public.{tbl}
                """
                for tbl in existing_settlement_tables
            ])

            main_sql = text(f"""
                WITH sku_map AS (
                    SELECT DISTINCT ON (UPPER(TRIM(COALESCE({sku_country_col}, ''))))
                        COALESCE(asin, '') AS asin,
                        COALESCE(product_barcode, '') AS product_barcode,
                        COALESCE(product_name, '') AS product_name,
                        TRIM(COALESCE({sku_country_col}, '')) AS msku,
                        COALESCE(price, 0) AS price,
                        COALESCE(currency, '') AS currency
                    FROM {sku_table}
                    WHERE user_id = :user_id
                      AND TRIM(COALESCE({sku_country_col}, '')) <> ''
                    ORDER BY
                        UPPER(TRIM(COALESCE({sku_country_col}, ''))),
                        CASE WHEN TRIM(COALESCE(product_name, '')) <> '' THEN 0 ELSE 1 END,
                        CASE WHEN TRIM(COALESCE(asin, '')) <> '' THEN 0 ELSE 1 END
                ),
                settlement_source AS (
                    {settlement_union_sql}
                ),
                settlement_comp AS (
                    SELECT
                        TRIM(COALESCE(sku, '')) AS msku,
                        SUM(
                            CASE
                                WHEN UPPER(TRIM(COALESCE(description, ''))) IN (
                                    'REVERSAL_REIMBURSEMENT',
                                    'COMPENSATED_CLAWBACK'
                                )
                                THEN ABS(COALESCE(quantity, 0))
                                ELSE 0
                            END
                        ) AS compensation_units,
                        SUM(
                            CASE
                                WHEN UPPER(TRIM(COALESCE(description, ''))) IN (
                                    'REVERSAL_REIMBURSEMENT',
                                    'COMPENSATED_CLAWBACK'
                                )
                                THEN ABS(COALESCE(total, 0))
                                ELSE 0
                            END
                        ) AS compensation_reimbursement_amount
                    FROM settlement_source
                    WHERE TRIM(COALESCE(sku, '')) <> ''
                      AND UPPER(TRIM(COALESCE(description, ''))) IN (
                            'REVERSAL_REIMBURSEMENT',
                            'COMPENSATED_CLAWBACK'
                      )
                    GROUP BY TRIM(COALESCE(sku, ''))
                ),
                settlement_loss_events AS (
                    SELECT
                        TRIM(COALESCE(sku, '')) AS msku,
                        SUM(
                            CASE
                                WHEN UPPER(TRIM(COALESCE(description, ''))) IN (
                                    'WAREHOUSE_LOST',
                                    'WAREHOUSE_DAMAGE',
                                    'MISSING_FROM_INBOUND',
                                    'MISSING_FROM_INBOUND_CLAWBACK'
                                )
                                THEN ABS(COALESCE(quantity, 0))
                                ELSE 0
                            END
                        ) AS settlement_loss_event_units,
                        SUM(
                            CASE
                                WHEN UPPER(TRIM(COALESCE(description, ''))) IN (
                                    'WAREHOUSE_LOST',
                                    'WAREHOUSE_DAMAGE',
                                    'MISSING_FROM_INBOUND',
                                    'MISSING_FROM_INBOUND_CLAWBACK'
                                )
                                THEN ABS(COALESCE(total, 0))
                                ELSE 0
                            END
                        ) AS settlement_loss_event_amount
                    FROM settlement_source
                    WHERE TRIM(COALESCE(sku, '')) <> ''
                      AND UPPER(TRIM(COALESCE(description, ''))) IN (
                            'WAREHOUSE_LOST',
                            'WAREHOUSE_DAMAGE',
                            'MISSING_FROM_INBOUND',
                            'MISSING_FROM_INBOUND_CLAWBACK'
                      )
                    GROUP BY TRIM(COALESCE(sku, ''))
                ),
                all_keys AS (
                    SELECT msku FROM sku_map
                    UNION
                    SELECT msku FROM settlement_comp
                    UNION
                    SELECT msku FROM settlement_loss_events
                )
                SELECT
                    COALESCE(sm.asin, '') AS asin,
                    ak.msku,
                    COALESCE(sm.product_barcode, '') AS product_barcode,
                    COALESCE(sm.product_name, '') AS product_name,
                    COALESCE(sm.price, 0) AS price,
                    COALESCE(sm.currency, '') AS currency,
                    COALESCE(sc.compensation_units, 0) AS compensation_units,
                    COALESCE(sc.compensation_reimbursement_amount, 0) AS compensation_reimbursement_amount,
                    COALESCE(sl.settlement_loss_event_units, 0) AS settlement_loss_event_units,
                    COALESCE(sl.settlement_loss_event_amount, 0) AS settlement_loss_event_amount
                FROM all_keys ak
                LEFT JOIN sku_map sm
                    ON UPPER(ak.msku) = UPPER(sm.msku)
                LEFT JOIN settlement_comp sc
                    ON UPPER(ak.msku) = UPPER(sc.msku)
                LEFT JOIN settlement_loss_events sl
                    ON UPPER(ak.msku) = UPPER(sl.msku)
            """)

            main_rows = conn.execute(main_sql, {"user_id": user_id}).mappings().all()

        # -------------------------------------------------
        # AMAZON DB: inventory monthly / quarterly / yearly
        # -------------------------------------------------
        with amazon_engine.connect() as conn:
            exists_sql = text("""
                SELECT EXISTS (
                    SELECT 1
                    FROM information_schema.tables
                    WHERE table_schema = 'public'
                      AND table_name = :table_name
                ) AS exists_flag
            """)
            exists_row = conn.execute(exists_sql, {"table_name": inventory_table_name}).mappings().first()

            if not exists_row or not exists_row["exists_flag"]:
                return jsonify({
                    "success": False,
                    "error": f"Table {inventory_table} does not exist in amazon database",
                    "table_name": inventory_table
                }), 404

            inventory_sql = text(f"""
                SELECT
                    TRIM(COALESCE(msku, '')) AS msku,
                    MAX(COALESCE(product_name, '')) AS inventory_product_name,
                    SUM(ABS(COALESCE(sum_lost, 0))) AS lost_units,
                    SUM(ABS(COALESCE(sum_damaged, 0))) AS damaged_units
                FROM {inventory_table}
                WHERE TRIM(COALESCE(msku, '')) <> ''
                  AND UPPER(TRIM(COALESCE(msku, ''))) <> 'GRAND TOTAL'
                GROUP BY UPPER(TRIM(COALESCE(msku, ''))), TRIM(COALESCE(msku, ''))
            """)

            inventory_rows = conn.execute(inventory_sql).mappings().all()

    except Exception as e:
        return jsonify({
            "success": False,
            "error": str(e),
            "sku_table": sku_table,
            "inventory_table": inventory_table,
            "settlement_tables": [f"public.{t}" for t in settlement_table_names]
        }), 500

    inventory_map = {}
    for row in inventory_rows:
        msku = (row["msku"] or "").strip()
        inventory_map[msku.upper()] = {
            "lost_units": float(row["lost_units"] or 0),
            "damaged_units": float(row["damaged_units"] or 0),
            "inventory_product_name": row["inventory_product_name"] or ""
        }

    data = []
    rows_to_store = []
    summary = {
        "lost_units": 0.0,
        "damaged_units": 0.0,
        "total_lost_units": 0.0,
        "compensation_units": 0.0,
        "compensation_reimbursement_amount": 0.0,
        "settlement_loss_event_units": 0.0,
        "settlement_loss_event_amount": 0.0,
        "loss_value": 0.0,
        "compensation_value": 0.0,
        "net_units": 0.0,
        "net_value": 0.0,
    }

    for row in main_rows:
        msku = (row["msku"] or "").strip()
        inv = inventory_map.get(msku.upper(), {})

        lost_units = float(inv.get("lost_units", 0))
        damaged_units = float(inv.get("damaged_units", 0))
        total_lost_units = lost_units + damaged_units

        compensation_units = float(row["compensation_units"] or 0)
        compensation_reimbursement_amount = float(row["compensation_reimbursement_amount"] or 0)

        settlement_loss_event_units = float(row["settlement_loss_event_units"] or 0)
        settlement_loss_event_amount = float(row["settlement_loss_event_amount"] or 0)

        loss_value = settlement_loss_event_amount
        compensation_value = compensation_reimbursement_amount
        net_units = compensation_units - total_lost_units
        net_value = compensation_value - loss_value

        product_name = (
            row["product_name"]
            or inv.get("inventory_product_name", "")
            or row["asin"]
            or msku
        )

        if (
            total_lost_units <= 0
            and compensation_units <= 0
            and settlement_loss_event_units <= 0
        ):
            continue

        item = {
            "asin": row["asin"],
            "msku": msku,
            "product_barcode": row["product_barcode"],
            "product_name": product_name,
            "price": round(float(row["price"] or 0), 2),
            "currency": row["currency"],
            "lost_units": lost_units,
            "damaged_units": damaged_units,
            "total_lost_units": total_lost_units,
            "compensation_units": compensation_units,
            "compensation_reimbursement_amount": round(compensation_reimbursement_amount, 2),
            "settlement_loss_event_units": settlement_loss_event_units,
            "settlement_loss_event_amount": round(settlement_loss_event_amount, 2),
            "loss_value": round(loss_value, 2),
            "compensation_value": round(compensation_value, 2),
            "net_units": net_units,
            "net_value": round(net_value, 2),
        }

        data.append(item)
        rows_to_store.append(item)

        summary["lost_units"] += lost_units
        summary["damaged_units"] += damaged_units
        summary["total_lost_units"] += total_lost_units
        summary["compensation_units"] += compensation_units
        summary["compensation_reimbursement_amount"] += compensation_reimbursement_amount
        summary["settlement_loss_event_units"] += settlement_loss_event_units
        summary["settlement_loss_event_amount"] += settlement_loss_event_amount
        summary["loss_value"] += loss_value
        summary["compensation_value"] += compensation_value

    summary["net_units"] = summary["compensation_units"] - summary["total_lost_units"]
    summary["net_value"] = summary["compensation_value"] - summary["loss_value"]

    summary = {
        "lost_units": summary["lost_units"],
        "damaged_units": summary["damaged_units"],
        "total_lost_units": summary["total_lost_units"],
        "compensation_units": summary["compensation_units"],
        "compensation_reimbursement_amount": round(summary["compensation_reimbursement_amount"], 2),
        "settlement_loss_event_units": summary["settlement_loss_event_units"],
        "settlement_loss_event_amount": round(summary["settlement_loss_event_amount"], 2),
        "loss_value": round(summary["loss_value"], 2),
        "compensation_value": round(summary["compensation_value"], 2),
        "net_units": summary["net_units"],
        "net_value": round(summary["net_value"], 2),
    }

    grand_total_row = {
        "asin": "Grand Total",
        "msku": "Grand Total",
        "product_barcode": "",
        "product_name": "Grand Total",
        "price": 0.0,
        "currency": "",
        "lost_units": summary["lost_units"],
        "damaged_units": summary["damaged_units"],
        "total_lost_units": summary["total_lost_units"],
        "compensation_units": summary["compensation_units"],
        "compensation_reimbursement_amount": summary["compensation_reimbursement_amount"],
        "settlement_loss_event_units": summary["settlement_loss_event_units"],
        "settlement_loss_event_amount": summary["settlement_loss_event_amount"],
        "loss_value": summary["loss_value"],
        "compensation_value": summary["compensation_value"],
        "net_units": summary["net_units"],
        "net_value": summary["net_value"],
    }

    data.append(grand_total_row)
    rows_to_store.append(grand_total_row)

    try:
        with amazon_engine.begin() as conn:
            create_sql = text(f"""
                CREATE TABLE IF NOT EXISTS {result_table} (
                    id SERIAL PRIMARY KEY,
                    sort_order INTEGER DEFAULT 1,
                    user_id BIGINT,
                    country TEXT,
                    year TEXT,
                    mode TEXT,
                    month TEXT,
                    quarter TEXT,
                    asin TEXT,
                    msku TEXT,
                    product_barcode TEXT,
                    product_name TEXT,
                    price DOUBLE PRECISION,
                    currency TEXT,
                    lost_units DOUBLE PRECISION,
                    damaged_units DOUBLE PRECISION,
                    total_lost_units DOUBLE PRECISION,
                    compensation_units DOUBLE PRECISION,
                    compensation_reimbursement_amount DOUBLE PRECISION,
                    settlement_loss_event_units DOUBLE PRECISION,
                    settlement_loss_event_amount DOUBLE PRECISION,
                    loss_value DOUBLE PRECISION,
                    compensation_value DOUBLE PRECISION,
                    net_units DOUBLE PRECISION,
                    net_value DOUBLE PRECISION,
                    created_at TIMESTAMP DEFAULT NOW()
                )
            """)
            conn.execute(create_sql)

            alter_statements = [
                f"ALTER TABLE {result_table} ADD COLUMN IF NOT EXISTS sort_order INTEGER DEFAULT 1",
                f"ALTER TABLE {result_table} ADD COLUMN IF NOT EXISTS quarter TEXT",
                f"ALTER TABLE {result_table} ADD COLUMN IF NOT EXISTS loss_value DOUBLE PRECISION",
                f"ALTER TABLE {result_table} ADD COLUMN IF NOT EXISTS compensation_value DOUBLE PRECISION",
                f"ALTER TABLE {result_table} ADD COLUMN IF NOT EXISTS net_value DOUBLE PRECISION",
            ]
            for stmt in alter_statements:
                conn.execute(text(stmt))

            delete_sql = text(f"""
                DELETE FROM {result_table}
                WHERE user_id = :user_id
                  AND country = :country
                  AND year = :year
                  AND mode = :mode
                  AND (
                        (:month IS NULL AND month IS NULL)
                        OR month = :month
                  )
                  AND (
                        (:quarter IS NULL AND quarter IS NULL)
                        OR quarter = :quarter
                  )
            """)
            conn.execute(delete_sql, {
                "user_id": user_id,
                "country": safe_country,
                "year": safe_year,
                "mode": mode,
                "month": safe_month if mode == "month" else None,
                "quarter": safe_quarter if mode == "quarter" else None,
            })

            insert_sql = text(f"""
                INSERT INTO {result_table} (
                    sort_order,
                    user_id, country, year, mode, month, quarter,
                    asin, msku, product_barcode, product_name, price, currency,
                    lost_units, damaged_units, total_lost_units,
                    compensation_units, compensation_reimbursement_amount,
                    settlement_loss_event_units, settlement_loss_event_amount,
                    loss_value, compensation_value, net_units, net_value
                )
                VALUES (
                    :sort_order,
                    :user_id, :country, :year, :mode, :month, :quarter,
                    :asin, :msku, :product_barcode, :product_name, :price, :currency,
                    :lost_units, :damaged_units, :total_lost_units,
                    :compensation_units, :compensation_reimbursement_amount,
                    :settlement_loss_event_units, :settlement_loss_event_amount,
                    :loss_value, :compensation_value, :net_units, :net_value
                )
            """)

            for row in rows_to_store:
                conn.execute(insert_sql, {
                    "sort_order": 999 if row["asin"] == "Grand Total" else 1,
                    "user_id": user_id,
                    "country": safe_country,
                    "year": safe_year,
                    "mode": mode,
                    "month": safe_month if mode == "month" else None,
                    "quarter": safe_quarter if mode == "quarter" else None,
                    "asin": row["asin"],
                    "msku": row["msku"],
                    "product_barcode": row["product_barcode"],
                    "product_name": row["product_name"],
                    "price": row["price"],
                    "currency": row["currency"],
                    "lost_units": row["lost_units"],
                    "damaged_units": row["damaged_units"],
                    "total_lost_units": row["total_lost_units"],
                    "compensation_units": row["compensation_units"],
                    "compensation_reimbursement_amount": row["compensation_reimbursement_amount"],
                    "settlement_loss_event_units": row["settlement_loss_event_units"],
                    "settlement_loss_event_amount": row["settlement_loss_event_amount"],
                    "loss_value": row["loss_value"],
                    "compensation_value": row["compensation_value"],
                    "net_units": row["net_units"],
                    "net_value": row["net_value"],
                })

    except Exception as e:
        return jsonify({
            "success": False,
            "error": f"Result generated but failed to store table: {str(e)}",
            "result_table": result_table
        }), 500

    return jsonify({
        "success": True,
        "user_id": user_id,
        "country": safe_country,
        "mode": mode,
        "month": safe_month if mode == "month" else None,
        "quarter": safe_quarter if mode == "quarter" else None,
        "year": safe_year,
        "sku_table": sku_table,
        "inventory_table": inventory_table,
        "settlement_tables_used": [f"public.{t}" for t in existing_settlement_tables],
        "result_table": result_table,
        "summary": summary,
        "data": data
    }), 200


