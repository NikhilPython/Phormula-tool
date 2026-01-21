from __future__ import annotations

import os
import io
import csv
import time
import gzip
import logging
from datetime import datetime, date
from typing import Optional
import jwt
import calendar
import requests
from dotenv import find_dotenv, load_dotenv
from sqlalchemy.orm import load_only
from flask import request, jsonify, Blueprint
from sqlalchemy.dialects.postgresql import insert, insert as pg_insert
from sqlalchemy.exc import SQLAlchemyError
from app import db
from sqlalchemy import delete, text
from app.models.user_models import Inventory, CountryProfile, MonthwiseInventory , InventoryAged
from app.utils.amazon_utils import amazon_client, _apply_region_and_marketplace_from_request
from app.utils.live_bi_utils import generate_inventory_alerts_for_all_skus

from config import Config

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

inventory_bp = Blueprint("inventory", __name__)

# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _safe_int(val) -> int:
    if val is None:
        return 0
    if isinstance(val, dict):
        return sum(int(v or 0) for v in val.values() if isinstance(v, (int, float)))
    try:
        return int(val)
    except Exception:
        return 0


# -------------------------- FBA inventory summaries -------------------------

def _fetch_fba_inventory_summaries(mp: str) -> list[dict]:
    """
    Returns a list of normalized inventory rows shaped for the Inventory table.
    Uses the FBA Inventory Summaries API with details=True and paginates via nextToken.
    """
    rows: list[dict] = []

    params = {
        "granularityType": "Marketplace",
        "granularityId": mp,
        "marketplaceIds": [mp],
        "details": "true",
        "pageSize": 100,
    }

    def _normalize(summary: dict) -> dict:
        s = summary or {}
        det = s.get("inventoryDetails") or {}

        inbound_total = (
            _safe_int(det.get("inboundWorkingQuantity")) +
            _safe_int(det.get("inboundShippedQuantity")) +
            _safe_int(det.get("inboundReceivingQuantity"))
        )

        available_qty = s.get("availableQuantity") or det.get("fulfillableQuantity")
        total_qty = s.get("totalQuantity")
        if total_qty is None:
            total_qty = (
                _safe_int(available_qty)
                + _safe_int(det.get("reservedQuantity"))
                + inbound_total
            )

        candidate_name = (
            s.get("productName")
            or s.get("title")
            or s.get("itemName")
        )

        return {
            "asin": s.get("asin"),
            "seller_sku": s.get("sellerSku"),
            "marketplace_id": mp,
            "product_name": candidate_name,
            "total_quantity": _safe_int(total_qty),
            "inbound_quantity": inbound_total,
            "available_quantity": _safe_int(available_qty),
            "reserved_quantity": _safe_int(det.get("reservedQuantity")),
            "fulfillable_quantity": _safe_int(det.get("fulfillableQuantity")),
            "synced_at": datetime.utcnow(),
            "inventory_age_days": 0,
        }

    res = amazon_client.make_api_call("/fba/inventory/v1/summaries", "GET", params)
    if not res or "error" in res:
        logger.warning("Inventory summaries fetch failed: %s", res)
        return rows

    payload = res.get("payload") or res
    for s in (payload.get("inventorySummaries") or []):
        rows.append(_normalize(s))

    nxt = payload.get("pagination", {}).get("nextToken")
    while nxt:
        page = amazon_client.make_api_call(
            "/fba/inventory/v1/summaries",
            "GET",
            {"nextToken": nxt, "marketplaceIds": [mp], "details": "true"},
        )
        if not page or "error" in page:
            logger.warning("Inventory pagination failed: %s", page)
            break
        p2 = page.get("payload") or page
        for s in (p2.get("inventorySummaries") or []):
            rows.append(_normalize(s))
        nxt = p2.get("pagination", {}).get("nextToken")

    # Enrich names via Catalog if missing
    try:
        _enrich_product_names(rows, mp)
    except Exception as e:
        logger.warning("Product-name enrichment failed: %s", e)

    return rows


# ----------------------------- Catalog enrichment ----------------------------

def _extract_catalog_title(item: dict) -> Optional[str]:
    summaries = (item or {}).get("summaries") or []
    if summaries:
        cand = (
            summaries[0].get("itemName")
            or summaries[0].get("title")
            or summaries[0].get("displayName")
        )
        if cand:
            return str(cand)

    attrs = (item or {}).get("attributes") or {}
    for k in ("item_name", "title", "item_name_en", "item_title"):
        v = attrs.get(k)
        if isinstance(v, list) and v and isinstance(v[0], dict):
            val = v[0].get("value")
            if val:
                return str(val)
        elif isinstance(v, str) and v:
            return v
    return None


def _enrich_product_names(rows: list[dict], mp: str) -> None:
    """Mutates rows in-place: fills product_name using Catalog Items v2022 by ASIN."""
    needed = {r["asin"] for r in rows if r.get("asin") and not r.get("product_name")}
    if not needed:
        return

    BATCH = 20
    asin_to_title: dict[str, str] = {}

    pending = list(needed)
    while pending:
        chunk = pending[:BATCH]
        pending = pending[BATCH:]

        params = {
            "identifiers": ",".join(chunk),
            "identifiersType": "ASIN",
            "marketplaceIds": [mp],
            "includedData": "summaries,attributes",
        }
        res = amazon_client.make_api_call("/catalog/2022-04-01/items", "GET", params)
        if not res or "error" in res:
            logger.warning("Catalog items fetch failed for %s: %s", chunk, res)
            continue

        items = res.get("items") or res.get("payload") or []
        for it in items:
            identifiers = it.get("identifiers") or {}
            asin = identifiers.get("asin") or it.get("asin")
            title = _extract_catalog_title(it)
            if asin and title:
                asin_to_title[asin] = title

    for r in rows:
        if not r.get("product_name") and r.get("asin"):
            r["product_name"] = asin_to_title.get(r["asin"])


def backfill_inventory_product_names(mp: str) -> int:
    """Fill product_name for existing rows with NULL/empty name for a marketplace."""
    missing = (
        db.session.query(Inventory.asin)
        .filter(
            Inventory.marketplace_id == mp,
            (Inventory.product_name.is_(None) | (Inventory.product_name == "")),
        )
        .distinct()
        .all()
    )
    asins = [a for (a,) in missing if a]
    if not asins:
        return 0

    temp_rows = [{"asin": a, "marketplace_id": mp, "product_name": None} for a in asins]
    _enrich_product_names(temp_rows, mp)

    updates = {
        r["asin"]: r.get("product_name") for r in temp_rows if r.get("product_name")
    }
    if not updates:
        return 0

    for asin, name in updates.items():
        db.session.query(Inventory).filter(
            Inventory.marketplace_id == mp,
            Inventory.asin == asin,
        ).update({"product_name": name}, synchronize_session=False)
    db.session.commit()
    return len(updates)


# ---------------------------- Inventory health report ------------------------

def _request_inventory_age_report(mp: str, retry_count: int = 0) -> Optional[str]:
    """
    Request FBA Manage Inventory Health Report via Reports API and return reportId.

    Uses GET_FBA_INVENTORY_PLANNING_DATA, which is the modern FBA inventory
    health/age report (replacement for the old GET_FBA_INVENTORY_AGED_DATA).
    """
    body = {
        "reportType": "GET_FBA_INVENTORY_PLANNING_DATA",
        "marketplaceIds": [mp],
    }

    res = amazon_client.make_api_call(
        "/reports/2021-06-30/reports",
        "POST",
        {},
        body,
    )

    if not res or "error" in res:
        error_msg = res.get("error", {}) if isinstance(res, dict) else str(res)
        logger.warning(
            "Error creating inventory health report (attempt %d): %s",
            retry_count + 1,
            error_msg,
        )

        # Retry logic for transient errors
        if retry_count < 2:  # Max 3 attempts total
            time.sleep(5 * (retry_count + 1))  # Exponential backoff
            return _request_inventory_age_report(mp, retry_count + 1)

        return None

    payload = res.get("payload") or res
    report_id = payload.get("reportId")

    if report_id:
        logger.info("Successfully created inventory health report: %s", report_id)

    return report_id


def _wait_for_report(report_id: str, timeout_sec: int = 600, poll_interval: int = 20) -> Optional[str]:
    """
    Poll report until DONE, return reportDocumentId.
    Enhanced with better error handling and logging.
    """
    deadline = time.time() + timeout_sec
    attempts = 0

    while time.time() < deadline:
        attempts += 1
        res = amazon_client.make_api_call(
            f"/reports/2021-06-30/reports/{report_id}",
            "GET",
            {},
        )

        if not res or "error" in res:
            logger.warning(
                "Error polling report %s (attempt %d): %s",
                report_id,
                attempts,
                res,
            )
            time.sleep(poll_interval)
            continue

        payload = res.get("payload") or res
        status = payload.get("processingStatus")

        logger.info(
            "Report %s status: %s (attempt %d)",
            report_id,
            status,
            attempts,
        )

        if status == "DONE":
            doc_id = payload.get("reportDocumentId")
            logger.info("Report %s completed successfully, document: %s", report_id, doc_id)
            return doc_id

        if status in ("FATAL", "CANCELLED"):
            logger.error(
                "Report %s ended with status %s. Full response: %s",
                report_id,
                status,
                payload,
            )

            # Log any error details if available
            if "processingEndTime" in payload:
                logger.error("Processing ended at: %s", payload["processingEndTime"])

            return None

        time.sleep(poll_interval)

    logger.warning("Timed out waiting for report %s after %d attempts", report_id, attempts)
    return None


def _download_report_document(report_document_id: str) -> Optional[bytes]:
    """
    Download the report document and return raw bytes (after de-compression if needed).
    Enhanced with better error handling.
    """
    try:
        meta = amazon_client.make_api_call(
            f"/reports/2021-06-30/documents/{report_document_id}",
            "GET",
            {},
        )

        if not meta or "error" in meta:
            logger.warning("Error getting report document meta: %s", meta)
            return None

        payload = meta.get("payload") or meta
        url = payload.get("url")
        compression = payload.get("compressionAlgorithm")

        if not url:
            logger.warning("No URL found in report document metadata")
            return None

        logger.info("Downloading report document from: %s", url[:100] + "...")

        resp = requests.get(url, timeout=120)
        resp.raise_for_status()
        content = resp.content

        if compression == "GZIP":
            logger.info("Decompressing GZIP content")
            content = gzip.decompress(content)

        logger.info("Successfully downloaded %d bytes", len(content))
        return content

    except requests.RequestException as e:
        logger.error("Failed to download report document: %s", e)
        return None
    except Exception as e:
        logger.error("Unexpected error downloading report: %s", e)
        return None


def _int(val) -> int:
    try:
        return int(val or 0)
    except Exception:
        return 0


def _compute_inventory_age_from_row(row: dict) -> int:
    """
    Compute a single inventory_age_days bucket for one CSV row from the
    FBA Manage Inventory Health Report (GET_FBA_INVENTORY_PLANNING_DATA).

    Primary buckets (coarse) we expect in this report:

      - inv-age-0-to-90-days
      - inv-age-91-to-180-days
      - inv-age-181-to-270-days
      - inv-age-271-to-365-days
      - inv-age-365-plus-days

    We return the LOWER bound of the oldest non-empty bucket:
      365, 271, 181, 91, or 0.

    If those coarse buckets are missing, we fall back to the older,
    fine-grained columns (0–30, 31–60, 61–90, etc.) and return a
    similar "oldest bucket lower bound" value.
    """

    # --- 1) Prefer the new coarse buckets ---
    c_0_90     = _int(row.get("inv-age-0-to-90-days"))
    c_91_180   = _int(row.get("inv-age-91-to-180-days"))
    c_181_270  = _int(row.get("inv-age-181-to-270-days"))
    c_271_365  = _int(row.get("inv-age-271-to-365-days"))
    c_365_plus = _int(row.get("inv-age-365-plus-days"))

    if any([c_0_90, c_91_180, c_181_270, c_271_365, c_365_plus]):
        # Oldest first
        if c_365_plus > 0:
            return 365
        if c_271_365 > 0:
            return 271
        if c_181_270 > 0:
            return 181
        if c_91_180 > 0:
            return 91
        if c_0_90 > 0:
            return 0
        return 0

    # --- 2) Fallback to the old fine-grained buckets if present ---
    a_0_30   = _int(row.get("inv-age-0-to-30-days"))
    a_31_60  = _int(row.get("inv-age-31-to-60-days"))
    a_61_90  = _int(row.get("inv-age-61-to-90-days"))
    a_91_180 = _int(row.get("inv-age-91-to-180-days"))

    a_181_270 = _int(row.get("inv-age-181-to-270-days"))
    a_271_365 = _int(row.get("inv-age-271-to-365-days"))
    a_181_330 = _int(row.get("inv-age-181-to-330-days"))
    a_331_365 = _int(row.get("inv-age-331-to-365-days"))
    a_365_plus = _int(row.get("inv-age-365-plus-days"))

    # Combine into broader ranges (similar to your previous logic)
    b_0_60    = a_0_30 + a_31_60
    b_61_90   = a_61_90
    b_91_180  = a_91_180
    b_181_330 = a_181_330 + a_181_270 + max(0, a_271_365 - a_331_365)
    b_331_365 = a_331_365
    b_365_plus = a_365_plus

    # Check from oldest -> youngest
    if b_365_plus > 0:
        return 365
    if b_331_365 > 0:
        return 331
    if b_181_330 > 0:
        return 181
    if b_91_180 > 0:
        return 91
    if b_61_90 > 0:
        return 61
    if b_0_60 > 0:
        return 0

    # If all zero, treat as 0 days
    return 0


def _fetch_inventory_age_by_sku(mp: str) -> dict[str, int]:
    """
    Returns {seller_sku: inventory_age_days} using GET_FBA_INVENTORY_PLANNING_DATA
    (FBA Manage Inventory Health Report).

    If the report fails or the structure is unexpected, returns {} and
    we leave inventory_age_days = 0.
    """
    logger.info("Starting inventory health (age) report request for marketplace: %s", mp)

    try:
        report_id = _request_inventory_age_report(mp)
        if not report_id:
            logger.warning("No report_id returned for GET_FBA_INVENTORY_PLANNING_DATA")
            return {}

        doc_id = _wait_for_report(report_id)
        if not doc_id:
            logger.warning("No reportDocumentId for GET_FBA_INVENTORY_PLANNING_DATA")
            # Optionally: try some fallback like summaries
            return _estimate_age_from_summaries(mp)

        content = _download_report_document(doc_id)
        if not content:
            logger.warning(
                "No content downloaded for inventory health report document %s",
                doc_id,
            )
            return {}

        text = content.decode("utf-8-sig", errors="replace")
        f = io.StringIO(text)
        reader = csv.DictReader(f, delimiter="\t")

        if not reader.fieldnames:
            logger.warning("Inventory health report has no header row")
            return {}

        age_by_sku: dict[str, int] = {}
        rows_count = 0

        for row in reader:
            rows_count += 1
            sku = (
                row.get("sku")
                or row.get("seller-sku")
                or row.get("seller_sku")
            )
            if not sku:
                continue
            age_by_sku[sku] = _compute_inventory_age_from_row(row)

        logger.info(
            "Processed %d rows from inventory health report, mapped %d SKUs",
            rows_count,
            len(age_by_sku),
        )

        if not age_by_sku:
            logger.warning(
                "Parsed %d rows from inventory health report but did not map any sku -> age",
                rows_count,
            )

        return age_by_sku

    except Exception as e:
        logger.error("Exception while fetching inventory health/age: %s", e, exc_info=True)
        return {}


def _estimate_age_from_summaries(mp: str) -> dict[str, int]:
    """
    Fallback method: estimate age based on inventory summary data.
    This is less accurate but better than nothing. Currently a stub.
    """
    logger.info("Using fallback method to estimate inventory age from summaries")
    return {}


# ------------------------------- DB upsert logic -----------------------------

def _upsert_inventory_rows(rows: list[dict], user_id: int | None) -> int:
    if not rows:
        return 0
    for r in rows:
        r["user_id"] = user_id

    stmt = insert(Inventory).values(rows)
    stmt = stmt.on_conflict_do_update(
        constraint="uq_inventory_sku_mkt",
        set_={
            "user_id": stmt.excluded.user_id,
            "asin": stmt.excluded.asin,
            "product_name": stmt.excluded.product_name,
            "total_quantity": stmt.excluded.total_quantity,
            "inbound_quantity": stmt.excluded.inbound_quantity,
            "available_quantity": stmt.excluded.available_quantity,
            "reserved_quantity": stmt.excluded.reserved_quantity,
            "fulfillable_quantity": stmt.excluded.fulfillable_quantity,
            "inventory_age_days": stmt.excluded.inventory_age_days,
            "synced_at": stmt.excluded.synced_at,
        },
    )
    db.session.execute(stmt)
    db.session.commit()
    return len(rows)


# ----------------------------------- Route -----------------------------------

@inventory_bp.route("/amazon_api/inventory", methods=["GET"])
def inventory_all():
    """
    Fetch FBA inventory summaries (with details) for the given marketplace,
    enrich with Catalog titles AND inventory age (via GET_FBA_INVENTORY_PLANNING_DATA
    / FBA Manage Inventory Health Report), and upsert into the `inventory` table.
    """

    # --- auth ---
    auth_header = request.headers.get("Authorization")
    user_id = None
    if auth_header and auth_header.startswith("Bearer "):
        token = auth_header.split(" ")[1]
        try:
            payload = jwt.decode(token, SECRET_KEY, algorithms=["HS256"])
            user_id = payload.get("user_id")
        except jwt.ExpiredSignatureError:
            return jsonify({"error": "Token has expired"}), 401
        except jwt.InvalidTokenError:
            return jsonify({"error": "Invalid token"}), 401

    _apply_region_and_marketplace_from_request()

    if amazon_client.marketplace_id not in amazon_client.ALLOWED_MARKETPLACES:
        return jsonify({"success": False, "error": "Unsupported marketplace"}), 400

    mp = request.args.get("marketplace_id", amazon_client.marketplace_id)
    store_in_db = request.args.get("store_in_db", "true").lower() != "false"
    refresh_names = request.args.get("refresh_names", "false").lower() == "true"
    skip_age_report = request.args.get("skip_age_report", "false").lower() == "true"

    # --- 1) fetch inventory summaries ---
    rows = _fetch_fba_inventory_summaries(mp)

    # --- 2) fetch inventory health/age data and attach inventory_age_days ---
    inventory_age_notice = None

    if not skip_age_report:
        try:
            age_by_sku = _fetch_inventory_age_by_sku(mp)
            if not age_by_sku:
                inventory_age_notice = (
                    "Inventory health/age report failed or returned no data. "
                    "This may be due to Amazon API issues. "
                    "Inventory age data will be set to 0. "
                    "You can retry this request later or use skip_age_report=true "
                    "to skip this step."
                )
            else:
                for r in rows:
                    sku = r.get("seller_sku")
                    r["inventory_age_days"] = age_by_sku.get(sku, 0)

        except Exception as e:
            logger.error("Failed to fetch inventory health/age report: %s", e, exc_info=True)
            inventory_age_notice = (
                f"Error fetching inventory age data: {str(e)}. "
                "Inventory age will be set to 0."
            )
    else:
        logger.info("Skipping inventory health/age report as requested")
        inventory_age_notice = "Inventory health/age report skipped per request parameter."

    out = {
        "success": True,
        "marketplace_id": mp,
        "count": len(rows),
        "items": rows,
        "db": {"saved_inventory": 0},
    }
    if inventory_age_notice:
        out["inventory_age_notice"] = inventory_age_notice

    # --- 3) persist ---
    if store_in_db and rows:
        try:
            saved = _upsert_inventory_rows(rows, user_id)
            out["db"]["saved_inventory"] = saved
        except Exception as e:
            logger.exception("Failed to upsert inventory rows")
            out["db"]["error"] = str(e)

    if refresh_names:
        try:
            updated = backfill_inventory_product_names(mp)
            out["db"]["backfilled_names"] = updated
        except Exception as e:
            out["db"]["backfill_error"] = str(e)

    if not rows:
        out["empty_message"] = "No inventory found for this seller account."

    return jsonify(out), 200


# --------------------------------------------- helpers ------------------------------------------

def _safe_float(val):
    if val is None or val == "":
        return None
    try:
        return float(val)
    except Exception:
        return None


def _parse_snapshot_date(val) -> Optional[date]:
    """
    snapshot-date is typically 'YYYY-MM-DD' (sometimes with time component).
    """
    if not val:
        return None
    try:
        # strip off any time part if present
        val = str(val).split("T", 1)[0]
        return datetime.strptime(val, "%Y-%m-%d").date()
    except Exception:
        return None


def _row_to_inventory_aged(row: dict) -> "InventoryAged":
    """
    Map one TSV row from GET_FBA_INVENTORY_PLANNING_DATA into an InventoryAged instance.
    Assumes InventoryAged model is defined in this module or imported.
    """
    snapshot_date = _parse_snapshot_date(row.get("snapshot-date"))

    sku = (
        row.get("sku")
        or row.get("seller-sku")
        or row.get("seller_sku")
    )

    inv = InventoryAged(
        # basic identifiers
        snapshot_date=snapshot_date,
        sku=sku,
        fnsku=row.get("fnsku"),
        asin=row.get("asin"),
        product_name=row.get("product-name"),  # will be overwritten by our SKU table
        condition=row.get("condition"),

        # quantities & age buckets (main)
        available=_int(row.get("available")),
        pending_removal_quantity=_int(row.get("pending-removal-quantity")),
        inv_age_0_90=_int(row.get("inv-age-0-to-90-days")),
        inv_age_91_180=_int(row.get("inv-age-91-to-180-days")),
        inv_age_181_270=_int(row.get("inv-age-181-to-270-days")),
        inv_age_271_365=_int(row.get("inv-age-271-to-365-days")),
        inv_age_365_plus=_int(row.get("inv-age-365-plus-days")),
        currency=row.get("currency"),

        # shipped units
        units_shipped_t7=_int(row.get("units-shipped-t7")),
        units_shipped_t30=_int(row.get("units-shipped-t30")),
        units_shipped_t60=_int(row.get("units-shipped-t60")),
        units_shipped_t90=_int(row.get("units-shipped-t90")),

        # pricing & alerts
        alert=row.get("alert"),
        your_price=_safe_float(row.get("your-price")),
        sales_price=_safe_float(row.get("sales-price")),
        lowest_price_new_plus_shipping=_safe_float(
            row.get("lowest-price-new-plus-shipping")
        ),
        lowest_price_used=_safe_float(row.get("lowest-price-used")),
        recommended_action=row.get("recommended-action"),
        healthy_inventory_level=_safe_float(row.get("healthy-inventory-level")),
        recommended_sales_price=_safe_float(row.get("recommended-sales-price")),
        recommended_sale_duration_days=_int(row.get("recommended-sale-duration-days")),
        recommended_removal_quantity=_int(row.get("recommended-removal-quantity")),
        estimated_cost_savings_recommended_actions=_safe_float(
            row.get("estimated-cost-savings-of-recommended-actions")
        ),

        sell_through=_safe_float(row.get("sell-through")),

        # volume & storage
        item_volume=_safe_float(row.get("item-volume")),
        volume_unit_measurement=row.get("volume-unit-measurement"),
        storage_type=row.get("storage-type"),
        storage_volume=_safe_float(row.get("storage-volume")),

        # catalog / marketplace
        marketplace=row.get("marketplace"),
        product_group=row.get("product-group"),
        sales_rank=_int(row.get("sales-rank")),

        # supply / excess / cover
        days_of_supply=_safe_float(row.get("days-of-supply")),
        estimated_excess_quantity=_int(row.get("estimated-excess-quantity")),
        weeks_of_cover_t30=_safe_float(row.get("weeks-of-cover-t30")),
        weeks_of_cover_t90=_safe_float(row.get("weeks-of-cover-t90")),

        featuredoffer_price=_safe_float(row.get("featuredoffer-price")),

        sales_shipped_last_7_days=_int(row.get("sales-shipped-last-7-days")),
        sales_shipped_last_30_days=_int(row.get("sales-shipped-last-30-days")),
        sales_shipped_last_60_days=_int(row.get("sales-shipped-last-60-days")),
        sales_shipped_last_90_days=_int(row.get("sales-shipped-last-90-days")),

        # more detailed age buckets
        inv_age_0_30=_int(row.get("inv-age-0-to-30-days")),
        inv_age_31_60=_int(row.get("inv-age-31-to-60-days")),
        inv_age_61_90=_int(row.get("inv-age-61-to-90-days")),
        inv_age_181_330=_int(row.get("inv-age-181-to-330-days")),
        inv_age_331_365=_int(row.get("inv-age-331-to-365-days")),

        estimated_storage_cost_next_month=_safe_float(
            row.get("estimated-storage-cost-next-month")
        ),

        # inbound / reserved / unfulfillable
        inbound_quantity=_int(row.get("inbound-quantity")),
        inbound_working=_int(row.get("inbound-working")),
        inbound_shipped=_int(row.get("inbound-shipped")),
        inbound_received=_int(row.get("inbound-received")),

        total_reserved_quantity=_int(row.get("Total Reserved Quantity")),
        unfulfillable_quantity=_int(row.get("unfulfillable-quantity")),

        qty_charged_ais_241_270=_int(
            row.get("quantity-to-be-charged-ais-241-270-days")
        ),
        est_ais_241_270=_safe_float(row.get("estimated-ais-241-270-days")),

        qty_charged_ais_271_300=_int(
            row.get("quantity-to-be-charged-ais-271-300-days")
        ),
        est_ais_271_300=_safe_float(row.get("estimated-ais-271-300-days")),

        qty_charged_ais_301_330=_int(
            row.get("quantity-to-be-charged-ais-301-330-days")
        ),
        est_ais_301_330=_safe_float(row.get("estimated-ais-301-330-days")),

        qty_charged_ais_331_365=_int(
            row.get("quantity-to-be-charged-ais-331-365-days")
        ),
        est_ais_331_365=_safe_float(row.get("estimated-ais-331-365-days")),

        qty_charged_ais_365_plus=_int(
            row.get("quantity-to-be-charged-ais-365-plus-days")
        ),
        est_ais_365_plus=_safe_float(row.get("estimated-ais-365-plus-days")),

        # historical supply / recommendations
        historical_days_of_supply=_safe_float(row.get("historical-days-of-supply")),
        recommended_ship_in_quantity=_int(
            row.get("Recommended ship-in quantity")
        ),
        recommended_ship_in_date=_parse_snapshot_date(
            row.get("Recommended ship-in date")
        ),
        last_updated_historical_dos=_parse_snapshot_date(
            row.get("Last updated date for Historical Days of Supply")
        ),
        short_term_historical_dos=_safe_float(
            row.get("Short term historical days of supply")
        ),
        long_term_historical_dos=_safe_float(
            row.get("Long term historical days of supply")
        ),
        inventory_age_snapshot_date=_parse_snapshot_date(
            row.get("Inventory age snapshot date")
        ),

        # inventory / reserved at FBA
        inventory_supply_at_fba=_int(row.get("Inventory Supply at FBA")),
        reserved_fc_transfer=_int(row.get("Reserved FC Transfer")),
        reserved_fc_processing=_int(row.get("Reserved FC Processing")),
        reserved_customer_order=_int(row.get("Reserved Customer Order")),
        total_days_of_supply_incl_open_shipments=_safe_float(
            row.get("Total Days of Supply (including units from open shipments)")
        ),
    )

    return inv


# -------- helper to map SKU -> product_name from public.sku_{user_id}_data_table --------

def _attach_sku_product_names(objs: list["InventoryAged"], user_id: int | None) -> None:
    """
    Overwrite InventoryAged.product_name from public.sku_{user_id}_data_table
    where inventory_aged.sku == sku_uk.
    """
    if not objs or not user_id:
        return

    skus = sorted({obj.sku for obj in objs if obj.sku})
    if not skus:
        return

    sku_table = f"public.sku_{user_id}_data_table"

    placeholders = ", ".join(f":sku{i}" for i in range(len(skus)))
    sql = text(f"""
        SELECT sku_uk, product_name
        FROM {sku_table}
        WHERE sku_uk IN ({placeholders})
    """)

    params = {f"sku{i}": sku for i, sku in enumerate(skus)}
    result = db.session.execute(sql, params)

    mapping = {row.sku_uk: row.product_name for row in result}

    for obj in objs:
        if obj.sku in mapping:
            obj.product_name = mapping[obj.sku]


# ------------------------------- route: sync InventoryAged --------------------------

MARKETPLACE_TO_COUNTRY = {
    "A1F83G8C2ARO7P": "uk",
    "ATVPDKIKX0DER": "us",
    "A2EUQ1WTGCTBG2": "ca",
    "A1PA6795UKMFR9": "de",
    "A13V1IB3VIYZZH": "fr",
    "A1RKKUPIHCS9HS": "es",
    "APJ6JRA9NG5V4": "it",
}

@inventory_bp.route("/amazon_api/inventory/aged", methods=["GET"])
def sync_inventory_aged():
    """
    Fetch GET_FBA_INVENTORY_PLANNING_DATA and store ONLY the latest snapshot_date
    into inventory_aged.

    Behavior:
    - Parses the report TSV
    - Detects the latest snapshot_date present in the report (treated as "current")
    - Deletes ALL previous snapshot_date data for this user + marketplace
    - Deletes any existing rows for the current snapshot_date (to prevent duplicates)
    - Inserts only rows from the current snapshot_date
    """

    # ---------------- AUTH ----------------
    auth_header = request.headers.get("Authorization")
    user_id = None

    if auth_header and auth_header.startswith("Bearer "):
        token = auth_header.split(" ")[1]
        try:
            payload = jwt.decode(token, SECRET_KEY, algorithms=["HS256"])
            user_id = payload.get("user_id")
        except jwt.ExpiredSignatureError:
            return jsonify({"error": "Token has expired"}), 401
        except jwt.InvalidTokenError:
            return jsonify({"error": "Invalid token"}), 401

    if not user_id:
        return jsonify({"error": "Unauthorized"}), 401

    # ---------------- MARKETPLACE ----------------
    _apply_region_and_marketplace_from_request()

    if amazon_client.marketplace_id not in amazon_client.ALLOWED_MARKETPLACES:
        return jsonify({"success": False, "error": "Unsupported marketplace"}), 400

    mp = request.args.get("marketplace_id", amazon_client.marketplace_id)

    logger.info("Starting InventoryAged sync for marketplace %s (user_id=%s)", mp, user_id)

    # ---------------- REQUEST REPORT ----------------
    report_id = _request_inventory_age_report(mp)
    if not report_id:
        return jsonify({
            "success": False,
            "error": "Failed to create inventory health report",
        }), 502

    doc_id = _wait_for_report(report_id)
    if not doc_id:
        return jsonify({
            "success": False,
            "error": "Inventory health report did not complete",
        }), 502

    content = _download_report_document(doc_id)
    if not content:
        return jsonify({
            "success": False,
            "error": "Failed to download inventory health report document",
        }), 502

    # ---------------- PARSE TSV ----------------
    text_content = content.decode("utf-8-sig", errors="replace")
    f = io.StringIO(text_content)
    reader = csv.DictReader(f, delimiter="\t")

    if not reader.fieldnames:
        return jsonify({
            "success": False,
            "error": "Inventory health report has no header row",
        }), 500

    parsed_objs: list[InventoryAged] = []
    rows_count = 0
    snapshot_dates: set[date] = set()

    for row in reader:
        rows_count += 1

        inv = _row_to_inventory_aged(row)
        if not inv or not inv.sku:
            continue

        inv.user_id = user_id
        inv.marketplace = mp

        parsed_objs.append(inv)

        if inv.snapshot_date:
            snapshot_dates.add(inv.snapshot_date)

    # ---------------- DETERMINE "CURRENT" SNAPSHOT DATE ----------------
    # Prefer the report's latest snapshot_date (best definition of "current")
    if snapshot_dates:
        current_snapshot = max(snapshot_dates)
    else:
        # fallback if Amazon doesn't provide snapshot-date in the file
        current_snapshot = date.today()

    # Keep only current snapshot rows
    objs = [o for o in parsed_objs if o.snapshot_date == current_snapshot]

    logger.info(
        "InventoryAged parsed rows=%d, snapshot_dates=%s, current_snapshot=%s, keeping_rows=%d",
        rows_count,
        sorted([d.isoformat() for d in snapshot_dates]),
        current_snapshot.isoformat(),
        len(objs),
    )

    # ---------------- DELETE OLD DATA (KEEP ONLY CURRENT SNAPSHOT) ----------------
    try:
        # 1) delete all other snapshot dates for this user+marketplace
        db.session.execute(
            delete(InventoryAged).where(
                InventoryAged.user_id == user_id,
                InventoryAged.marketplace == mp,
                InventoryAged.snapshot_date != current_snapshot,
            )
        )

        # 2) delete current snapshot too (so re-sync doesn't duplicate)
        db.session.execute(
            delete(InventoryAged).where(
                InventoryAged.user_id == user_id,
                InventoryAged.marketplace == mp,
                InventoryAged.snapshot_date == current_snapshot,
            )
        )

        db.session.commit()
    except Exception as e:
        db.session.rollback()
        logger.exception("Failed to delete old InventoryAged data")
        return jsonify({
            "success": False,
            "error": f"Failed to cleanup old data: {str(e)}",
        }), 500

    # ---------------- ATTACH PRODUCT NAMES ----------------
    try:
        _attach_sku_product_names(objs, user_id)
    except Exception as e:
        logger.exception("Failed to attach SKU product names: %s", e)

    # ---------------- INSERT NEW DATA ----------------
    try:
        if objs:
            db.session.bulk_save_objects(objs)
            db.session.commit()
        saved = len(objs)
    except Exception as e:
        db.session.rollback()
        logger.exception("Failed to save InventoryAged rows")
        return jsonify({
            "success": False,
            "error": f"Failed to save rows: {str(e)}",
        }), 500

    # ---------------- INVENTORY ALERTS ----------------
    try:
        country = MARKETPLACE_TO_COUNTRY.get(mp, "uk")
        inventory_alerts = generate_inventory_alerts_for_all_skus(
            user_id=user_id,
            country=country,
        )
    except Exception:
        logger.exception("Failed to generate inventory alerts")
        inventory_alerts = {}

    # ---------------- RESPONSE ----------------
    return jsonify({
        "success": True,
        "marketplace_id": mp,
        "report_id": report_id,
        "document_id": doc_id,
        "rows_in_report": rows_count,
        "rows_saved": saved,
        "current_snapshot_date": current_snapshot.isoformat(),
        "inventory_alerts": inventory_alerts,
    }), 200



@inventory_bp.route("/amazon_api/inventory/aged/columns", methods=["GET"])
def get_inventory_aged_selected_columns():
    # --- auth ---
    auth_header = request.headers.get("Authorization")
    if not auth_header or not auth_header.startswith("Bearer "):
        return jsonify({"error": "Missing Authorization header"}), 401

    token = auth_header.split(" ")[1]
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=["HS256"])
        user_id = payload.get("user_id")
    except jwt.ExpiredSignatureError:
        return jsonify({"error": "Token has expired"}), 401
    except jwt.InvalidTokenError:
        return jsonify({"error": "Invalid token"}), 401

    if not user_id:
        return jsonify({"error": "Invalid token payload"}), 401

    # --- filters ---
    marketplace_id = request.args.get("marketplace_id")
    snapshot_date = request.args.get("snapshot_date")  # YYYY-MM-DD
    latest = request.args.get("latest", "0") == "1"

    q = InventoryAged.query.filter(InventoryAged.user_id == user_id)

    if marketplace_id:
        q = q.filter(InventoryAged.marketplace == marketplace_id)

    if latest:
        latest_date = (
            db.session.query(InventoryAged.snapshot_date)
            .filter(InventoryAged.user_id == user_id)
            .order_by(InventoryAged.snapshot_date.desc())
            .limit(1)
            .scalar()
        )
        if latest_date:
            q = q.filter(InventoryAged.snapshot_date == latest_date)

    elif snapshot_date:
        q = q.filter(InventoryAged.snapshot_date == snapshot_date)

    rows = q.order_by(InventoryAged.id.asc()).all()

    # --- ONLY required fields ---
    data = []
    for r in rows:
        data.append({
            "fnsku": getattr(r, "fnsku", None),
            "asin": getattr(r, "asin", None),
            "product-name": getattr(r, "product_name", None),
            "condition": getattr(r, "condition", None),
            "available": getattr(r, "available", 0),
            "pending-removal-quantity": getattr(r, "pending_removal_quantity", 0),
            "inv-age-0-to-90-days": getattr(r, "inv_age_0_90", 0),
            "inv-age-91-to-180-days": getattr(r, "inv_age_91_180", 0),
            "inv-age-181-to-270-days": getattr(r, "inv_age_181_270", 0),
            "inv-age-271-to-365-days": getattr(r, "inv_age_271_365", 0),
            "inv-age-365-plus-days": getattr(r, "inv_age_365_plus", 0),
            "currency": getattr(r, "currency", None),
            "estimated-storage-cost-next-month": getattr(r, "estimated_storage_cost_next_month", 0.0),
        })

    return jsonify({
        "success": True,
        "count": len(data),
        "data": data
    }), 200


@inventory_bp.route("/country-profile", methods=["POST"])
def upsert_country_profile():
    # ---- Auth (JWT Bearer) ----
    auth_header = request.headers.get('Authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        return jsonify({'error': 'Authorization token is missing or invalid'}), 401

    token = auth_header.split(' ')[1]
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=['HS256'])
        user_id = payload['user_id']
    except jwt.ExpiredSignatureError:
        return jsonify({'error': 'Token has expired'}), 401
    except jwt.InvalidTokenError:
        return jsonify({'error': 'Invalid token'}), 401

    # ---- Input JSON ----
    data = request.get_json(silent=True) or {}
    country = (data.get("country") or "").strip()
    marketplace = (data.get("marketplace") or "").strip()
    transit_time = data.get("transit_time")
    stock_unit = data.get("stock_unit")

    # ---- Validation ----
    errors = {}
    if not country:
        errors["country"] = "country is required"
    if not marketplace:
        errors["marketplace"] = "marketplace is required"
    try:
        transit_time = int(transit_time)
        if transit_time <= 0:
            raise ValueError
    except Exception:
        errors["transit_time"] = "transit_time must be a positive integer"

    try:
        stock_unit = int(stock_unit)
        if stock_unit < 0:
            raise ValueError
    except Exception:
        errors["stock_unit"] = "stock_unit must be a non-negative integer"

    if errors:
        return jsonify({"errors": errors}), 400

    # ---- Upsert ----
    try:
        profile = CountryProfile.query.filter_by(
            user_id=user_id,
            country=country,
            marketplace=marketplace
        ).first()

        created = False
        if profile is None:
            profile = CountryProfile(
                user_id=user_id,
                country=country,
                marketplace=marketplace,
                transit_time=transit_time,
                stock_unit=stock_unit
            )
            db.session.add(profile)
            created = True
        else:
            profile.transit_time = transit_time
            profile.stock_unit = stock_unit

        db.session.commit()

        return jsonify({
            "created": created,
            "profile": {
                "id": profile.id,
                "user_id": profile.user_id,
                "country": profile.country,
                "marketplace": profile.marketplace,
                "transit_time": profile.transit_time,
                "stock_unit": profile.stock_unit
            }
        }), 201 if created else 200

    except SQLAlchemyError as e:
        db.session.rollback()
        return jsonify({"error": "Database error", "detail": str(e)}), 500
    
#------------------------------------------------------------------------------ MonthwiseInventory upsert logic --------------------------------------
def _safe_int(v):
    try:
        if v is None:
            return 0
        s = str(v).strip()
        if s == "":
            return 0
        s = s.replace(",", "")
        return int(float(s))
    except Exception:
        return 0


def _parse_date_str(value: str) -> date:
    """
    Accepts:
      - '2025-10-31'
      - '10/31/2025'
      - '30/11/2025'
      - ISO like '2025-12-01T00:00:00Z'
    """
    value = (value or "").strip()
    if not value:
        raise ValueError("empty date")

    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%d/%m/%Y"):
        try:
            return datetime.strptime(value, fmt).date()
        except ValueError:
            pass

    # ISO timestamps
    try:
        s2 = value.replace("Z", "+00:00")
        return datetime.fromisoformat(s2).date()
    except Exception:
        pass

    raise ValueError(
        f"Invalid date format: {value}. "
        "Use YYYY-MM-DD, MM/DD/YYYY, DD/MM/YYYY, or ISO timestamp."
    )


def _filter_first_last_dates(rows: list[dict]) -> list[dict]:
    """
    Keep ONLY the first date and last date present in rows.
    """
    if not rows:
        return rows

    dates = sorted({r["date"] for r in rows if isinstance(r.get("date"), date)})
    if not dates:
        return rows

    first_date, last_date = dates[0], dates[-1]
    keep = {first_date, last_date}
    return [r for r in rows if r.get("date") in keep]


def _month_range(year: int, month: int) -> tuple[date, date]:
    if not (1 <= month <= 12):
        raise ValueError("month must be between 1 and 12")
    if year < 2000 or year > 2100:
        raise ValueError("year must be between 2000 and 2100")

    last_dom = calendar.monthrange(year, month)[1]
    return date(year, month, 1), date(year, month, last_dom)


def _quarter_range(year: int, quarter: int) -> tuple[date, date]:
    if quarter not in (1, 2, 3, 4):
        raise ValueError("quarter must be 1, 2, 3, or 4")
    if year < 2000 or year > 2100:
        raise ValueError("year must be between 2000 and 2100")

    start_month = {1: 1, 2: 4, 3: 7, 4: 10}[quarter]
    end_month = start_month + 2

    start_date = date(year, start_month, 1)
    end_dom = calendar.monthrange(year, end_month)[1]
    end_date = date(year, end_month, end_dom)
    return start_date, end_date


def _year_range(year: int) -> tuple[date, date]:
    if year < 2000 or year > 2100:
        raise ValueError("year must be between 2000 and 2100")
    return date(year, 1, 1), date(year, 12, 31)


# =============================================================================
# ENRICH PRODUCT NAME
# =============================================================================

def _attach_product_names_to_rows(rows: list[dict], user_id: int | None) -> None:
    """
    Add product_name from public.sku_{user_id}_data_table where sku_uk = msku.
    Modifies rows in place.
    """
    if not rows or not user_id:
        return

    mskus = sorted({r.get("msku") for r in rows if r.get("msku")})
    if not mskus:
        return

    sku_table = f"public.sku_{user_id}_data_table"
    placeholders = ", ".join(f":sku{i}" for i in range(len(mskus)))

    sql = text(f"""
        SELECT sku_uk, product_name
        FROM {sku_table}
        WHERE sku_uk IN ({placeholders})
    """)

    params = {f"sku{i}": m for i, m in enumerate(mskus)}
    result = db.session.execute(sql, params)

    mapping = {row.sku_uk: row.product_name for row in result}

    for r in rows:
        r["product_name"] = mapping.get(r.get("msku"))


# =============================================================================
# UPSERT
# =============================================================================

def _upsert_monthwise_inventory_rows(rows: list[dict], user_id: int | None) -> int:
    """
    Upsert into MonthwiseInventory.
    IMPORTANT: constraint uq_monthwise_inv_key must match model UniqueConstraint.
    """
    if not rows:
        return 0

    _attach_product_names_to_rows(rows, user_id)

    for r in rows:
        r["user_id"] = user_id

    stmt = pg_insert(MonthwiseInventory).values(rows)

    stmt = stmt.on_conflict_do_update(
        constraint="uq_monthwise_inv_key",
        set_={
            "fnsku": stmt.excluded.fnsku,
            "title": stmt.excluded.title,
            "product_name": stmt.excluded.product_name,

            "starting_warehouse_balance": stmt.excluded.starting_warehouse_balance,
            "in_transit_between_warehouses": stmt.excluded.in_transit_between_warehouses,
            "receipts": stmt.excluded.receipts,
            "customer_shipments": stmt.excluded.customer_shipments,
            "customer_returns": stmt.excluded.customer_returns,
            "vendor_returns": stmt.excluded.vendor_returns,
            "warehouse_transfer_in_out": stmt.excluded.warehouse_transfer_in_out,
            "found": stmt.excluded.found,
            "lost": stmt.excluded.lost,
            "damaged": stmt.excluded.damaged,
            "disposed": stmt.excluded.disposed,
            "other_events": stmt.excluded.other_events,
            "ending_warehouse_balance": stmt.excluded.ending_warehouse_balance,
            "unknown_events": stmt.excluded.unknown_events,
            "synced_at": stmt.excluded.synced_at,
        },
    )

    db.session.execute(stmt)
    db.session.commit()
    return len(rows)


# =============================================================================
# FETCH REPORT
# =============================================================================

def _fetch_ledger_summary_rows(
    mp: str,
    start_date: date,
    end_date: date,
    time_period: str = "DAILY",   # keep DAILY so we can do first+last filtering ourselves
) -> list[dict]:
    body = {
        "reportType": "GET_LEDGER_SUMMARY_VIEW_DATA",
        "marketplaceIds": [mp],
        "dataStartTime": start_date.isoformat() + "T00:00:00Z",
        "dataEndTime": end_date.isoformat() + "T23:59:59Z",
        "reportOptions": {
            "aggregateByLocation": "COUNTRY",
            "aggregatedByTimePeriod": time_period,
        },
    }

    create = amazon_client.make_api_call(
        "/reports/2021-06-30/reports",
        method="POST",
        params=None,
        data=body,
    )

    if not create or create.get("error"):
        msg = str(create)
        if "status_code': 403" in msg or '"status_code": 403' in msg:
            raise RuntimeError(
                "403 Unauthorized: Access denied for GET_LEDGER_SUMMARY_VIEW_DATA. "
                "Fix SP-API roles/permissions in Seller Central for your app."
            )
        raise RuntimeError(f"Failed to create ledger summary report: {create}")

    create_payload = create.get("payload") or create
    report_id = create_payload.get("reportId")
    if not report_id:
        raise RuntimeError(f"Missing reportId in ledger summary response: {create}")

    # poll
    status = None
    meta_payload = None
    report_meta = None

    for _ in range(60):
        report_meta = amazon_client.make_api_call(
            f"/reports/2021-06-30/reports/{report_id}", "GET", None
        )

        if not report_meta or report_meta.get("error"):
            raise RuntimeError(f"Error polling ledger summary report: {report_meta}")

        meta_payload = report_meta.get("payload") or report_meta
        status = meta_payload.get("processingStatus")

        if status in ("DONE", "FATAL", "CANCELLED"):
            break

        time.sleep(10)

    if status != "DONE":
        raise RuntimeError(f"Report not completed. Status={status}, meta={report_meta}")

    doc_id = meta_payload.get("reportDocumentId")
    if not doc_id:
        raise RuntimeError(f"Missing reportDocumentId in meta: {report_meta}")

    doc = amazon_client.make_api_call(
        f"/reports/2021-06-30/documents/{doc_id}", "GET", None
    )
    if not doc or doc.get("error"):
        raise RuntimeError(f"Error getting report document: {doc}")

    doc_payload = doc.get("payload") or doc
    url = doc_payload.get("url")
    if not url:
        raise RuntimeError(f"Missing URL in report document: {doc_payload}")

    resp = requests.get(url, timeout=60)
    resp.raise_for_status()
    raw_bytes = resp.content

    if doc_payload.get("compressionAlgorithm") == "GZIP":
        raw_bytes = gzip.decompress(raw_bytes)

    text_data = raw_bytes.decode("utf-8-sig")
    reader = csv.DictReader(io.StringIO(text_data), delimiter="\t")

    rows: list[dict] = []
    for r in reader:
        raw_date = (r.get("Date") or "").strip()
        if not raw_date:
            continue

        try:
            row_date = _parse_date_str(raw_date)
        except ValueError:
            continue

        rows.append(
            {
                "date": row_date,
                "fnsku": (r.get("FNSKU") or r.get("FnSku") or "").strip(),
                "asin": (r.get("ASIN") or "").strip(),
                "msku": (r.get("MSKU") or "").strip(),
                "title": r.get("Title") or "",
                "disposition": (r.get("Disposition") or "").strip(),
                "location": (r.get("Location") or "").strip(),
                "marketplace_id": mp,
                "synced_at": datetime.utcnow(),

                "starting_warehouse_balance": _safe_int(r.get("Starting Warehouse Balance")),
                "in_transit_between_warehouses": _safe_int(r.get("In Transit Between Warehouses")),
                "receipts": _safe_int(r.get("Receipts")),
                "customer_shipments": _safe_int(r.get("Customer Shipments")),
                "customer_returns": _safe_int(r.get("Customer Returns")),
                "vendor_returns": _safe_int(r.get("Vendor Returns")),
                "warehouse_transfer_in_out": _safe_int(r.get("Warehouse Transfer In/Out")),
                "found": _safe_int(r.get("Found")),
                "lost": _safe_int(r.get("Lost")),
                "damaged": _safe_int(r.get("Damaged")),
                "disposed": _safe_int(r.get("Disposed")),
                "other_events": _safe_int(r.get("Other Events")),
                "ending_warehouse_balance": _safe_int(r.get("Ending Warehouse Balance")),
                "unknown_events": _safe_int(r.get("Unknown Events")),
            }
        )
    return rows


# =============================================================================
# ROUTE
# =============================================================================

@inventory_bp.route("/amazon_api/inventory/ledger-summary", methods=["GET"])
def inventory_ledger_summary():
    # ---- Auth ----
    auth_header = request.headers.get("Authorization")
    user_id = None
    if auth_header and auth_header.startswith("Bearer "):
        token = auth_header.split(" ")[1]
        try:
            payload = jwt.decode(token, SECRET_KEY, algorithms=["HS256"])
            user_id = payload.get("user_id")
        except jwt.ExpiredSignatureError:
            return jsonify({"error": "Token has expired"}), 401
        except jwt.InvalidTokenError:
            return jsonify({"error": "Invalid token"}), 401

    _apply_region_and_marketplace_from_request()

    if amazon_client.marketplace_id not in amazon_client.ALLOWED_MARKETPLACES:
        return jsonify({"success": False, "error": "Unsupported marketplace"}), 400

    mp = request.args.get("marketplace_id", amazon_client.marketplace_id)
    store_in_db = request.args.get("store_in_db", "true").lower() != "false"

    # If true, filter first+last even for custom start/end range
    keep_first_last = request.args.get("keep_first_last", "false").lower() == "true"

    # ---- Params ----
    date_str = request.args.get("date")
    start_str = request.args.get("start_date")
    end_str = request.args.get("end_date")

    month_param = request.args.get("month")      # "12" or "2025-12"
    quarter_param = request.args.get("quarter")  # "1" or "2025-Q1"
    year_param = request.args.get("year")        # "2025"

    mode = None  # "month" | "quarter" | "year" | None

    try:
        # 1) Month mode
        if month_param and not (date_str or start_str or end_str or quarter_param):
            if "-" in month_param:
                y_str, m_str = month_param.split("-", 1)
                year = int(year_param or y_str)
                month = int(m_str)
            else:
                month = int(month_param)
                year = int(year_param) if year_param else datetime.utcnow().year

            start_date, end_date = _month_range(year, month)
            mode = "month"

        # 2) Quarter mode
        elif quarter_param and not (date_str or start_str or end_str or month_param):
            # Allow "2025-Q1" or "Q1" with ?year=2025 or "1" with ?year=2025
            qp = quarter_param.strip().upper()

            if "-Q" in qp:  # "2025-Q1"
                y_str, q_str = qp.split("-Q", 1)
                year = int(year_param or y_str)
                quarter = int(q_str)
            elif qp.startswith("Q"):  # "Q1"
                quarter = int(qp[1:])
                year = int(year_param) if year_param else datetime.utcnow().year
            else:  # "1"
                quarter = int(qp)
                year = int(year_param) if year_param else datetime.utcnow().year

            start_date, end_date = _quarter_range(year, quarter)
            mode = "quarter"

        # 3) Year mode (full year) -> only when year is provided AND no other date params
        elif year_param and not (date_str or start_str or end_str or month_param or quarter_param):
            year = int(year_param)
            start_date, end_date = _year_range(year)
            mode = "year"

        # 4) Single date
        elif date_str:
            start_date = end_date = _parse_date_str(date_str)

        # 5) Explicit range
        else:
            if not (start_str and end_str):
                return jsonify({
                    "error": (
                        "Provide either ?date=..., both ?start_date= and ?end_date=, "
                        "or ?month=..., or ?quarter=...&year=..., or ?year=YYYY."
                    )
                }), 400

            start_date = _parse_date_str(start_str)
            end_date = _parse_date_str(end_str)
            if end_date < start_date:
                raise ValueError("end_date cannot be before start_date")

    except ValueError as e:
        return jsonify({"error": str(e)}), 400

    # ---- Fetch DAILY from Amazon ----
    try:
        rows = _fetch_ledger_summary_rows(mp, start_date, end_date, time_period="DAILY")
    except Exception as e:
        logger.exception("Failed to fetch ledger summary")
        return jsonify({"success": False, "error": str(e)}), 500

    # ---- Filter first+last for month/quarter/year OR flag ----
    if rows and (mode in ("month", "quarter", "year") or keep_first_last):
        rows = _filter_first_last_dates(rows)

        # update response start/end to match filtered data
        dates = sorted({r["date"] for r in rows if isinstance(r.get("date"), date)})
        if dates:
            start_date, end_date = dates[0], dates[-1]

    # ---- Format output ----
    display_rows = []
    for r in rows:
        r2 = dict(r)
        if isinstance(r2.get("date"), date):
            r2["date"] = r2["date"].strftime("%m/%d/%Y")
        display_rows.append(r2)

    out = {
        "success": True,
        "marketplace_id": mp,
        "mode": mode or "range",
        "start_date": start_date.strftime("%m/%d/%Y"),
        "end_date": end_date.strftime("%m/%d/%Y"),
        "count": len(display_rows),
        "db": {"saved_rows": 0},
        "items": display_rows,
    }

    # ---- Store to DB (only filtered rows get stored) ----
    if store_in_db and rows:
        try:
            out["db"]["saved_rows"] = _upsert_monthwise_inventory_rows(rows, user_id)
        except Exception as e:
            logger.exception("Failed to upsert monthwise inventory")
            out["db"]["error"] = str(e)

    if not display_rows:
        out["empty_message"] = "No ledger summary rows for this date range."

    return jsonify(out), 200


# @inventory_bp.route("/amazon_api/inventory/ledger-summary/db", methods=["GET"])
# def inventory_ledger_summary_db():
#     """
#     Ledger summary FROM DB (MonthwiseInventory) with disposition pivoted into 5 columns:
#       SELLABLE, EXPIRED, DEFECTIVE, CUSTOMER_DAMAGED, WAREHOUSE_DAMAGED

#     The 5 disposition columns contain ending_warehouse_balance.

#     Base metrics (starting balance, receipts, shipments, etc.) are taken from the
#     SELLABLE row when available (to avoid picking EXPIRED/DEFECTIVE rows that often have 0/1).
#     """

#     # -------------------- Auth --------------------
#     auth_header = request.headers.get("Authorization")
#     user_id = None
#     if auth_header and auth_header.startswith("Bearer "):
#         token = auth_header.split(" ")[1]
#         try:
#             payload = jwt.decode(token, SECRET_KEY, algorithms=["HS256"])
#             user_id = payload.get("user_id")
#         except jwt.ExpiredSignatureError:
#             return jsonify({"error": "Token has expired"}), 401
#         except jwt.InvalidTokenError:
#             return jsonify({"error": "Invalid token"}), 401

#     if not user_id:
#         return jsonify({"error": "Missing/invalid token"}), 401

#     # -------------------- Params --------------------
#     mp = request.args.get("marketplace_id")  # optional
#     msku_filter = request.args.get("msku")   # optional
#     location_filter = request.args.get("location")  # optional

#     date_str = request.args.get("date")
#     start_str = request.args.get("start_date")
#     end_str = request.args.get("end_date")

#     month_param = request.args.get("month")      # "12" or "2025-12"
#     quarter_param = request.args.get("quarter")  # "1" or "2025-Q1" or "Q1"
#     year_param = request.args.get("year")        # "2025"

#     mode = None  # "month" | "quarter" | "year" | "range" | "single"

#     # -------------------- Compute date window --------------------
#     try:
#         if month_param and not (date_str or start_str or end_str or quarter_param):
#             if "-" in month_param:
#                 y_str, m_str = month_param.split("-", 1)
#                 year = int(year_param or y_str)
#                 month = int(m_str)
#             else:
#                 month = int(month_param)
#                 year = int(year_param) if year_param else datetime.utcnow().year
#             start_date, end_date = _month_range(year, month)
#             mode = "month"

#         elif quarter_param and not (date_str or start_str or end_str or month_param):
#             qp = quarter_param.strip().upper()
#             if "-Q" in qp:
#                 y_str, q_str = qp.split("-Q", 1)
#                 year = int(year_param or y_str)
#                 quarter = int(q_str)
#             elif qp.startswith("Q"):
#                 quarter = int(qp[1:])
#                 year = int(year_param) if year_param else datetime.utcnow().year
#             else:
#                 quarter = int(qp)
#                 year = int(year_param) if year_param else datetime.utcnow().year
#             start_date, end_date = _quarter_range(year, quarter)
#             mode = "quarter"

#         elif year_param and not (date_str or start_str or end_str or month_param or quarter_param):
#             year = int(year_param)
#             start_date, end_date = _year_range(year)
#             mode = "year"

#         elif date_str:
#             start_date = end_date = _parse_date_str(date_str)
#             mode = "single"

#         else:
#             if not (start_str and end_str):
#                 return jsonify({
#                     "error": (
#                         "Provide either ?date=..., or both ?start_date= and ?end_date=, "
#                         "or ?month=..., or ?quarter=...&year=..., or ?year=YYYY."
#                     )
#                 }), 400

#             start_date = _parse_date_str(start_str)
#             end_date = _parse_date_str(end_str)
#             if end_date < start_date:
#                 raise ValueError("end_date cannot be before start_date")
#             mode = "range"

#     except ValueError as e:
#         return jsonify({"error": str(e)}), 400

#     # -------------------- DB Query --------------------
#     q = (
#         db.session.query(MonthwiseInventory)
#         .options(
#             load_only(
#                 MonthwiseInventory.date,
#                 MonthwiseInventory.msku,
#                 MonthwiseInventory.disposition,
#                 MonthwiseInventory.product_name,
#                 MonthwiseInventory.marketplace_id,
#                 MonthwiseInventory.location,

#                 MonthwiseInventory.starting_warehouse_balance,
#                 MonthwiseInventory.in_transit_between_warehouses,
#                 MonthwiseInventory.receipts,
#                 MonthwiseInventory.customer_shipments,
#                 MonthwiseInventory.customer_returns,
#                 MonthwiseInventory.vendor_returns,
#                 MonthwiseInventory.warehouse_transfer_in_out,
#                 MonthwiseInventory.found,
#                 MonthwiseInventory.lost,
#                 MonthwiseInventory.damaged,
#                 MonthwiseInventory.disposed,
#                 MonthwiseInventory.other_events,
#                 MonthwiseInventory.ending_warehouse_balance,
#             )
#         )
#         .filter(MonthwiseInventory.user_id == user_id)
#         .filter(MonthwiseInventory.date >= start_date)
#         .filter(MonthwiseInventory.date <= end_date)
#     )

#     if mp:
#         q = q.filter(MonthwiseInventory.marketplace_id == mp)
#     if msku_filter:
#         q = q.filter(MonthwiseInventory.msku == msku_filter)
#     if location_filter:
#         q = q.filter(MonthwiseInventory.location == location_filter)

#     q = q.order_by(
#         MonthwiseInventory.date.asc(),
#         MonthwiseInventory.msku.asc(),
#         MonthwiseInventory.disposition.asc(),
#     )

#     rows = q.all()

#     # -------------------- Pivot dispositions into 5 columns --------------------
#     DISPOSITIONS = [
#         "SELLABLE",
#         "EXPIRED",
#         "DEFECTIVE",
#         "CUSTOMER_DAMAGED",
#         "WAREHOUSE_DAMAGED",
#     ]

#     def _pick_base_metrics(dst: dict, src_row) -> None:
#         """Copy base metrics from a row into dst."""
#         dst["starting_warehouse_balance"] = int(src_row.starting_warehouse_balance or 0)
#         dst["in_transit_between_warehouses"] = int(src_row.in_transit_between_warehouses or 0)
#         dst["receipts"] = int(src_row.receipts or 0)
#         dst["customer_shipments"] = int(src_row.customer_shipments or 0)
#         dst["customer_returns"] = int(src_row.customer_returns or 0)
#         dst["vendor_returns"] = int(src_row.vendor_returns or 0)
#         dst["warehouse_transfer_in_out"] = int(src_row.warehouse_transfer_in_out or 0)
#         dst["found"] = int(src_row.found or 0)
#         dst["lost"] = int(src_row.lost or 0)
#         dst["damaged"] = int(src_row.damaged or 0)
#         dst["disposed"] = int(src_row.disposed or 0)
#         dst["other_events"] = int(src_row.other_events or 0)

#     grouped = {}

#     for r in rows:
#         key = (r.date, r.msku, r.product_name, r.marketplace_id, r.location)

#         if key not in grouped:
#             grouped[key] = {
#                 "date": r.date.strftime("%Y-%m-%d"),
#                 "msku": r.msku,
#                 "product_name": r.product_name,
#                 "marketplace_id": r.marketplace_id,
#                 "location": r.location,

#                 # base metrics (we'll fill these from SELLABLE if possible)
#                 "starting_warehouse_balance": None,
#                 "in_transit_between_warehouses": None,
#                 "receipts": None,
#                 "customer_shipments": None,
#                 "customer_returns": None,
#                 "vendor_returns": None,
#                 "warehouse_transfer_in_out": None,
#                 "found": None,
#                 "lost": None,
#                 "damaged": None,
#                 "disposed": None,
#                 "other_events": None,

#                 # disposition pivot columns (ending_warehouse_balance)
#                 **{d: 0 for d in DISPOSITIONS},

#                 # internal helpers
#                 "_base_set": False,
#                 "_base_from_sellable": False,
#             }

#         g = grouped[key]

#         # 1) Always pivot ending_warehouse_balance into the right disposition column
#         disp = (r.disposition or "").strip()
#         if disp in DISPOSITIONS:
#             g[disp] += int(r.ending_warehouse_balance or 0)

#         # -------------------- ADD DIFFERENCE COLUMN --------------------
#         for row in grouped.values():
#             total_ending = sum(
#                 row[d] for d in DISPOSITIONS
#             )
#             row["difference"] = (
#                 int(row["starting_warehouse_balance"] or 0) - total_ending
#             )


#         # 2) Set base metrics:
#         #    - Prefer SELLABLE row
#         #    - Otherwise set from first row as fallback
#         if disp == "SELLABLE":
#             _pick_base_metrics(g, r)
#             g["_base_set"] = True
#             g["_base_from_sellable"] = True
#         elif not g["_base_set"]:
#             _pick_base_metrics(g, r)
#             g["_base_set"] = True

#     # cleanup helper keys and None->0
#     items = []
#     for g in grouped.values():
#         g.pop("_base_set", None)
#         g.pop("_base_from_sellable", None)

#         for k in [
#             "starting_warehouse_balance",
#             "in_transit_between_warehouses",
#             "receipts",
#             "customer_shipments",
#             "customer_returns",
#             "vendor_returns",
#             "warehouse_transfer_in_out",
#             "found",
#             "lost",
#             "damaged",
#             "disposed",
#             "other_events",
#         ]:
#             if g.get(k) is None:
#                 g[k] = 0

#         items.append(g)

#     items.sort(key=lambda x: (x["date"], x["msku"], x.get("marketplace_id") or "", x.get("location") or ""))

#     return jsonify({
#         "success": True,
#         "mode": mode,
#         "marketplace_id": mp,
#         "start_date": start_date.strftime("%Y-%m-%d"),
#         "end_date": end_date.strftime("%Y-%m-%d"),
#         "count": len(items),
#         "items": items,
#     }), 200

@inventory_bp.route("/amazon_api/inventory/ledger-summary/db", methods=["GET"])
def inventory_ledger_summary_db():
    """
    Ledger summary FROM DB (MonthwiseInventory) with disposition pivoted into 5 columns:
      SELLABLE, EXPIRED, DEFECTIVE, CUSTOMER_DAMAGED, WAREHOUSE_DAMAGED

    - The 5 disposition columns contain ending_warehouse_balance.
    - Base metrics (starting balance, receipts, shipments, etc.) are taken from the SELLABLE row
      when available (prevents picking EXPIRED/DEFECTIVE rows that can have small/zero values).
    - Adds:
        total_ending_warehouse_balance = sum of the 5 disposition columns
        difference = starting_warehouse_balance - total_ending_warehouse_balance

    Supports:
      1) Month:    ?month=12&year=2025  OR ?month=2025-12
      2) Quarter:  ?quarter=1&year=2025 OR ?quarter=2025-Q1 OR ?quarter=Q1&year=2025
      3) Year:     ?year=2025
      4) Range:    ?start_date=2025-12-01&end_date=2025-12-31
      5) Single:   ?date=2025-12-31

    Optional filters:
      - marketplace_id=...
      - msku=...
      - location=...

    NOTE:
      - We intentionally do NOT apply ?disposition= filter, because we need all dispositions
        to build the 5 columns.
    """

    # -------------------- Auth --------------------
    auth_header = request.headers.get("Authorization")
    user_id = None
    if auth_header and auth_header.startswith("Bearer "):
        token = auth_header.split(" ")[1]
        try:
            payload = jwt.decode(token, SECRET_KEY, algorithms=["HS256"])
            user_id = payload.get("user_id")
        except jwt.ExpiredSignatureError:
            return jsonify({"error": "Token has expired"}), 401
        except jwt.InvalidTokenError:
            return jsonify({"error": "Invalid token"}), 401

    if not user_id:
        return jsonify({"error": "Missing/invalid token"}), 401

    # -------------------- Params --------------------
    mp = request.args.get("marketplace_id")  # optional
    msku_filter = request.args.get("msku")   # optional
    location_filter = request.args.get("location")  # optional

    date_str = request.args.get("date")
    start_str = request.args.get("start_date")
    end_str = request.args.get("end_date")

    month_param = request.args.get("month")      # "12" or "2025-12"
    quarter_param = request.args.get("quarter")  # "1" or "2025-Q1" or "Q1"
    year_param = request.args.get("year")        # "2025"

    mode = None  # "month" | "quarter" | "year" | "range" | "single"

    # -------------------- Compute date window --------------------
    try:
        if month_param and not (date_str or start_str or end_str or quarter_param):
            if "-" in month_param:
                y_str, m_str = month_param.split("-", 1)
                year = int(year_param or y_str)
                month = int(m_str)
            else:
                month = int(month_param)
                year = int(year_param) if year_param else datetime.utcnow().year

            start_date, end_date = _month_range(year, month)
            mode = "month"

        elif quarter_param and not (date_str or start_str or end_str or month_param):
            qp = quarter_param.strip().upper()
            if "-Q" in qp:
                y_str, q_str = qp.split("-Q", 1)
                year = int(year_param or y_str)
                quarter = int(q_str)
            elif qp.startswith("Q"):
                quarter = int(qp[1:])
                year = int(year_param) if year_param else datetime.utcnow().year
            else:
                quarter = int(qp)
                year = int(year_param) if year_param else datetime.utcnow().year

            start_date, end_date = _quarter_range(year, quarter)
            mode = "quarter"

        elif year_param and not (date_str or start_str or end_str or month_param or quarter_param):
            year = int(year_param)
            start_date, end_date = _year_range(year)
            mode = "year"

        elif date_str:
            start_date = end_date = _parse_date_str(date_str)
            mode = "single"

        else:
            if not (start_str and end_str):
                return jsonify({
                    "error": (
                        "Provide either ?date=..., or both ?start_date= and ?end_date=, "
                        "or ?month=..., or ?quarter=...&year=..., or ?year=YYYY."
                    )
                }), 400

            start_date = _parse_date_str(start_str)
            end_date = _parse_date_str(end_str)
            if end_date < start_date:
                raise ValueError("end_date cannot be before start_date")
            mode = "range"

    except ValueError as e:
        return jsonify({"error": str(e)}), 400

    # -------------------- DB Query --------------------
    q = (
        db.session.query(MonthwiseInventory)
        .options(
            load_only(
                MonthwiseInventory.date,
                MonthwiseInventory.msku,
                MonthwiseInventory.disposition,
                MonthwiseInventory.product_name,
                MonthwiseInventory.marketplace_id,
                MonthwiseInventory.location,

                MonthwiseInventory.starting_warehouse_balance,
                MonthwiseInventory.in_transit_between_warehouses,
                MonthwiseInventory.receipts,
                MonthwiseInventory.customer_shipments,
                MonthwiseInventory.customer_returns,
                MonthwiseInventory.vendor_returns,
                MonthwiseInventory.warehouse_transfer_in_out,
                MonthwiseInventory.found,
                MonthwiseInventory.lost,
                MonthwiseInventory.damaged,
                MonthwiseInventory.disposed,
                MonthwiseInventory.other_events,
                MonthwiseInventory.ending_warehouse_balance,
            )
        )
        .filter(MonthwiseInventory.user_id == user_id)
        .filter(MonthwiseInventory.date >= start_date)
        .filter(MonthwiseInventory.date <= end_date)
    )

    if mp:
        q = q.filter(MonthwiseInventory.marketplace_id == mp)
    if msku_filter:
        q = q.filter(MonthwiseInventory.msku == msku_filter)
    if location_filter:
        q = q.filter(MonthwiseInventory.location == location_filter)

    q = q.order_by(
        MonthwiseInventory.date.asc(),
        MonthwiseInventory.msku.asc(),
        MonthwiseInventory.disposition.asc(),
    )

    rows = q.all()

    # -------------------- Pivot dispositions into 5 columns --------------------
    DISPOSITIONS = [
        "SELLABLE",
        "EXPIRED",
        "DEFECTIVE",
        "CUSTOMER_DAMAGED",
        "WAREHOUSE_DAMAGED",
    ]

    def _pick_base_metrics(dst: dict, src_row) -> None:
        dst["starting_warehouse_balance"] = int(src_row.starting_warehouse_balance or 0)
        dst["in_transit_between_warehouses"] = int(src_row.in_transit_between_warehouses or 0)
        dst["receipts"] = int(src_row.receipts or 0)
        dst["customer_shipments"] = int(src_row.customer_shipments or 0)
        dst["customer_returns"] = int(src_row.customer_returns or 0)
        dst["vendor_returns"] = int(src_row.vendor_returns or 0)
        dst["warehouse_transfer_in_out"] = int(src_row.warehouse_transfer_in_out or 0)
        dst["found"] = int(src_row.found or 0)
        dst["lost"] = int(src_row.lost or 0)
        dst["damaged"] = int(src_row.damaged or 0)
        dst["disposed"] = int(src_row.disposed or 0)
        dst["other_events"] = int(src_row.other_events or 0)

    grouped = {}

    for r in rows:
        key = (r.date, r.msku, r.product_name, r.marketplace_id, r.location)

        if key not in grouped:
            grouped[key] = {
                "date": r.date.strftime("%Y-%m-%d"),
                "msku": r.msku,
                "product_name": r.product_name,
                "marketplace_id": r.marketplace_id,
                "location": r.location,

                # base metrics (filled from SELLABLE when available)
                "starting_warehouse_balance": 0,
                "in_transit_between_warehouses": 0,
                "receipts": 0,
                "customer_shipments": 0,
                "customer_returns": 0,
                "vendor_returns": 0,
                "warehouse_transfer_in_out": 0,
                "found": 0,
                "lost": 0,
                "damaged": 0,
                "disposed": 0,
                "other_events": 0,

                # disposition pivot columns (ending_warehouse_balance)
                **{d: 0 for d in DISPOSITIONS},

                # internal helper
                "_base_set": False,
            }

            # fallback: set base metrics from the first seen row
            _pick_base_metrics(grouped[key], r)
            grouped[key]["_base_set"] = True

        g = grouped[key]

        # pivot ending_warehouse_balance
        disp = (r.disposition or "").strip()
        if disp in DISPOSITIONS:
            g[disp] += int(r.ending_warehouse_balance or 0)

        # if SELLABLE row exists, override base metrics with SELLABLE (preferred)
        if disp == "SELLABLE":
            _pick_base_metrics(g, r)

    # -------------------- Add totals + difference --------------------
    items = []
    for g in grouped.values():
        g.pop("_base_set", None)

        total_ending = sum(g[d] for d in DISPOSITIONS)
        g["total_ending_warehouse_balance"] = total_ending
        g["difference"] = int(g["starting_warehouse_balance"] or 0) - total_ending

        items.append(g)

    items.sort(key=lambda x: (x["date"], x["msku"], x.get("marketplace_id") or "", x.get("location") or ""))

    return jsonify({
        "success": True,
        "mode": mode,
        "marketplace_id": mp,
        "start_date": start_date.strftime("%Y-%m-%d"),
        "end_date": end_date.strftime("%Y-%m-%d"),
        "count": len(items),
        "items": items,
    }), 200

