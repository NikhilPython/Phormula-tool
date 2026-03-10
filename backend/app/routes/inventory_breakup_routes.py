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

logger = logging.getLogger("amazon_sp_api")
logging.basicConfig(level=logging.INFO)

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
        logger.exception("DB error in inventory_breakup (amazon_engine)")
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
        logger.exception("DB error in inventory_ageing (amazon_engine)")
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

from sqlalchemy import text
import re
from datetime import datetime

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
    month = (request.args.get("month") or "").strip().lower()
    year = (request.args.get("year") or "").strip()

    if not country:
        return jsonify({"success": False, "error": "country is required"}), 400
    if not month:
        return jsonify({"success": False, "error": "month is required"}), 400
    if not year:
        return jsonify({"success": False, "error": "year is required"}), 400

    if len(month) == 3:
        month = MONTH_MAP.get(month, month)

    try:
        month_num = datetime.strptime(month[:3].title(), "%b").month
    except Exception:
        return jsonify({"success": False, "error": "Invalid month"}), 400

    safe_country = re.sub(r"[^a-z0-9_]", "", country)
    safe_month = re.sub(r"[^a-z0-9_]", "", month)
    safe_year = re.sub(r"[^0-9]", "", year)
    safe_user_id = re.sub(r"[^0-9]", "", str(user_id))

    if not safe_country or not safe_month or not safe_year or not safe_user_id:
        return jsonify({"success": False, "error": "Invalid table name inputs"}), 400

    settlement_table_name = f"user_{safe_user_id}_{safe_country}_{safe_month}{safe_year}_data"
    sku_table_name = f"sku_{safe_user_id}_data_table"
    inventory_table_name = f"inventorymonthly_{safe_user_id}_{safe_country}_{month_num:02d}_{safe_year}"

    settlement_table = f"public.{settlement_table_name}"
    sku_table = f"public.{sku_table_name}"
    inventory_table = f"public.{inventory_table_name}"

    sku_country_col = "sku_uk" if safe_country == "uk" else "sku_us"

    try:
        # -------------------------------------------------
        # MAIN DB: settlement + sku mapping
        # -------------------------------------------------
        with db.engine.connect() as conn:
            for table_name in [settlement_table_name, sku_table_name]:
                exists_sql = text("""
                    SELECT EXISTS (
                        SELECT 1
                        FROM information_schema.tables
                        WHERE table_schema = 'public'
                          AND table_name = :table_name
                    ) AS exists_flag
                """)
                exists_row = conn.execute(exists_sql, {"table_name": table_name}).mappings().first()

                if not exists_row or not exists_row["exists_flag"]:
                    return jsonify({
                        "success": False,
                        "error": f"Table public.{table_name} does not exist in main database",
                        "table_name": f"public.{table_name}"
                    }), 404

            main_sql = text(f"""
                WITH sku_map AS (
                    SELECT
                        COALESCE(asin, '') AS asin,
                        COALESCE(product_barcode, '') AS product_barcode,
                        COALESCE(product_name, '') AS product_name,
                        COALESCE({sku_country_col}, '') AS msku,
                        COALESCE(price, 0) AS price,
                        COALESCE(currency, '') AS currency
                    FROM {sku_table}
                    WHERE user_id = :user_id
                ),
                settlement_comp AS (
                    SELECT
                        COALESCE(sku, '') AS asin,
                        SUM(
                            CASE
                                WHEN UPPER(COALESCE(description, '')) IN (
                                    'REVERSAL_REIMBURSEMENT',
                                    'COMPENSATED_CLAWBACK'
                                )
                                THEN ABS(COALESCE(quantity, 0))
                                ELSE 0
                            END
                        ) AS compensation_units,
                        SUM(
                            CASE
                                WHEN UPPER(COALESCE(description, '')) IN (
                                    'REVERSAL_REIMBURSEMENT',
                                    'COMPENSATED_CLAWBACK'
                                )
                                THEN ABS(COALESCE(total, 0))
                                ELSE 0
                            END
                        ) AS compensation_reimbursement_amount
                    FROM {settlement_table}
                    WHERE UPPER(COALESCE(description, '')) IN (
                        'REVERSAL_REIMBURSEMENT',
                        'COMPENSATED_CLAWBACK'
                    )
                    GROUP BY COALESCE(sku, '')
                ),
                settlement_loss_events AS (
                    SELECT
                        COALESCE(sku, '') AS asin,
                        SUM(
                            CASE
                                WHEN UPPER(COALESCE(description, '')) IN (
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
                                WHEN UPPER(COALESCE(description, '')) IN (
                                    'WAREHOUSE_LOST',
                                    'WAREHOUSE_DAMAGE',
                                    'MISSING_FROM_INBOUND',
                                    'MISSING_FROM_INBOUND_CLAWBACK'
                                )
                                THEN ABS(COALESCE(total, 0))
                                ELSE 0
                            END
                        ) AS settlement_loss_event_amount
                    FROM {settlement_table}
                    WHERE UPPER(COALESCE(description, '')) IN (
                        'WAREHOUSE_LOST',
                        'WAREHOUSE_DAMAGE',
                        'MISSING_FROM_INBOUND',
                        'MISSING_FROM_INBOUND_CLAWBACK'
                    )
                    GROUP BY COALESCE(sku, '')
                )
                SELECT
                    sm.asin,
                    sm.product_barcode,
                    sm.product_name,
                    sm.msku,
                    sm.price,
                    sm.currency,
                    COALESCE(sc.compensation_units, 0) AS compensation_units,
                    COALESCE(sc.compensation_reimbursement_amount, 0) AS compensation_reimbursement_amount,
                    COALESCE(sl.settlement_loss_event_units, 0) AS settlement_loss_event_units,
                    COALESCE(sl.settlement_loss_event_amount, 0) AS settlement_loss_event_amount
                FROM sku_map sm
                LEFT JOIN settlement_comp sc
                    ON sm.asin = sc.asin
                LEFT JOIN settlement_loss_events sl
                    ON sm.asin = sl.asin
            """)

            main_rows = conn.execute(main_sql, {"user_id": user_id}).mappings().all()

        # -------------------------------------------------
        # AMAZON DB: inventorymonthly
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
                    COALESCE(msku, '') AS msku,
                    COALESCE(product_name, '') AS inventory_product_name,
                    SUM(ABS(COALESCE(sum_lost, 0))) AS lost_units,
                    SUM(ABS(COALESCE(sum_damaged, 0))) AS damaged_units
                FROM {inventory_table}
                WHERE COALESCE(msku, '') <> ''
                  AND COALESCE(msku, '') <> 'Grand Total'
                GROUP BY COALESCE(msku, ''), COALESCE(product_name, '')
            """)

            inventory_rows = conn.execute(inventory_sql).mappings().all()

    except Exception as e:
        logger.exception("DB error in inventory_lost_compensation")
        return jsonify({
            "success": False,
            "error": str(e),
            "settlement_table": settlement_table,
            "sku_table": sku_table,
            "inventory_table": inventory_table
        }), 500

    # -------------------------------------------------
    # Merge inventory rows by msku
    # -------------------------------------------------
    inventory_map = {}
    for row in inventory_rows:
        msku = row["msku"]
        inventory_map[msku] = {
            "lost_units": float(row["lost_units"] or 0),
            "damaged_units": float(row["damaged_units"] or 0),
            "inventory_product_name": row["inventory_product_name"] or ""
        }

    # -------------------------------------------------
    # Build final response
    # -------------------------------------------------
    data = []
    summary = {
        "lost_units": 0.0,
        "damaged_units": 0.0,
        "total_lost_units": 0.0,
        "compensation_units": 0.0,
        "lost_sale_amount": 0.0,
        "compensation_sale_amount": 0.0,
        "net_sale_amount": 0.0,
        "compensation_reimbursement_amount": 0.0,
        "settlement_loss_event_units": 0.0,
        "settlement_loss_event_amount": 0.0,
        "net_units": 0.0,
    }

    for row in main_rows:
        msku = row["msku"]
        inv = inventory_map.get(msku, {})

        price = float(row["price"] or 0)
        lost_units = float(inv.get("lost_units", 0))
        damaged_units = float(inv.get("damaged_units", 0))
        total_lost_units = lost_units + damaged_units

        compensation_units = float(row["compensation_units"] or 0)
        compensation_reimbursement_amount = float(row["compensation_reimbursement_amount"] or 0)

        settlement_loss_event_units = float(row["settlement_loss_event_units"] or 0)
        settlement_loss_event_amount = float(row["settlement_loss_event_amount"] or 0)

        lost_sale_amount = total_lost_units * price
        compensation_sale_amount = compensation_units * price
        net_units = compensation_units + settlement_loss_event_units
        net_sale_amount = compensation_reimbursement_amount + settlement_loss_event_amount

        # choose product name from sku table, fallback inventory table
        product_name = row["product_name"] or inv.get("inventory_product_name", "")

        # only include useful rows
        if (
            total_lost_units <= 0
            and compensation_units <= 0
            and settlement_loss_event_units <= 0
        ):
            continue

        data.append({
            "asin": row["asin"],
            "msku": msku,
            "product_barcode": row["product_barcode"],
            "product_name": product_name,
            "price": round(price, 2),
            "currency": row["currency"],

            "lost_units": lost_units,
            "damaged_units": damaged_units,
            "total_lost_units": total_lost_units,
            "lost_sale_amount": round(lost_sale_amount, 2),

            "compensation_units": compensation_units,
            "compensation_sale_amount": round(compensation_sale_amount, 2),
            "compensation_reimbursement_amount": round(compensation_reimbursement_amount, 2),

            "settlement_loss_event_units": settlement_loss_event_units,
            "settlement_loss_event_amount": round(settlement_loss_event_amount, 2),

            "net_units": net_units,
            "net_sale_amount": round(net_sale_amount, 2),
        })

        summary["lost_units"] += lost_units
        summary["damaged_units"] += damaged_units
        summary["total_lost_units"] += total_lost_units
        summary["compensation_units"] += compensation_units
        summary["lost_sale_amount"] += lost_sale_amount
        summary["compensation_sale_amount"] += compensation_sale_amount
        summary["compensation_reimbursement_amount"] += compensation_reimbursement_amount
        summary["settlement_loss_event_units"] += settlement_loss_event_units
        summary["settlement_loss_event_amount"] += settlement_loss_event_amount

    summary["net_units"] = summary["compensation_units"] + summary["settlement_loss_event_units"]
    summary["net_sale_amount"] = summary["compensation_reimbursement_amount"] + summary["settlement_loss_event_amount"]

    summary = {
        "lost_units": summary["lost_units"],
        "damaged_units": summary["damaged_units"],
        "total_lost_units": summary["total_lost_units"],
        "compensation_units": summary["compensation_units"],
        "lost_sale_amount": round(summary["lost_sale_amount"], 2),
        "compensation_sale_amount": round(summary["compensation_sale_amount"], 2),
        "net_sale_amount": round(summary["net_sale_amount"], 2),
        "compensation_reimbursement_amount": round(summary["compensation_reimbursement_amount"], 2),
        "settlement_loss_event_units": summary["settlement_loss_event_units"],
        "settlement_loss_event_amount": round(summary["settlement_loss_event_amount"], 2),
        "net_units": summary["net_units"],
    }

    return jsonify({
        "success": True,
        "user_id": user_id,
        "country": safe_country,
        "month": safe_month,
        "year": safe_year,
        "settlement_table": settlement_table,
        "sku_table": sku_table,
        "inventory_table": inventory_table,
        "summary": summary,
        "data": data
    }), 200

