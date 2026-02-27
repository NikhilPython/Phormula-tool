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
if not db_url:
    raise RuntimeError("DATABASE_URL is not set")
if not db_url1:
    print("[WARN] DATABASE_ADMIN_URL not set; falling back to DATABASE_URL")

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
