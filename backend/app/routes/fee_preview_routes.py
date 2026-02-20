# ===========================
# Optimized Fee Preview Module (FULL + CORRECT)
# ===========================
from __future__ import annotations

import base64
import csv
import gzip
import io
import logging
import os
import time
from datetime import datetime
from decimal import Decimal, InvalidOperation

import jwt
import pandas as pd
import requests
from Crypto.Cipher import AES
from dotenv import find_dotenv, load_dotenv
from flask import Blueprint, jsonify, request
from sqlalchemy import Column, Float, Integer, MetaData, String, Table, create_engine, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import sessionmaker
from sqlalchemy import inspect as sa_inspect  # IMPORTANT: sqlalchemy inspect

from config import Config
from app import db
from app.models.user_models import Category, CountryProfile, Fee
from app.utils.amazon_utils import amazon_client  # your existing client

SECRET_KEY = Config.SECRET_KEY

# --- load .env robustly ---
dotenv_path = find_dotenv(filename=".env", usecwd=True)
load_dotenv(dotenv_path, override=True)

logger = logging.getLogger("amazon_sp_api")
logging.basicConfig(level=logging.INFO)

db_url = os.getenv("DATABASE_URL")
db_url1 = os.getenv("DATABASE_ADMIN_URL") or db_url  # fallback

if not db_url:
    raise RuntimeError("DATABASE_URL is not set")
if not db_url1:
    print("[WARN] DATABASE_ADMIN_URL not set; falling back to DATABASE_URL")

fee_preview_bp = Blueprint("fee_preview", __name__)

# ============================================================
# MARKETPLACE -> REGION/BASE MAP (used if client has no setters)
# ============================================================
EU_MARKETPLACES = {
    "A1F83G8C2ARO7P",  # UK
    "A13V1IB3VIYZZH",  # FR
    "A1PA6795UKMFR9",  # DE
    "APJ6JRA9NG5V4",   # IT
    "A1RKKUPIHCS9HS",  # ES
    "A2NODRKZP88ZB9",  # SE
    "A1805IZSGTT6HS",  # NL
    "A2VIGQ35RCS4UG",  # AE (often EU endpoint)
}
NA_MARKETPLACES = {
    "ATVPDKIKX0DER",   # US
    "A2EUQ1WTGCTBG2",  # CA
    "A1AM78C64UM0Y8",  # MX
}
FE_MARKETPLACES = {
    "A1VC38T7YXB528",  # JP
    "A19VAU5U5O7RUS",  # SG
    "A39IBJ37TRP1C6",  # AU
    "A2Q3Y263D00KWC",  # BR (sometimes NA, depends)
}

def _region_base_for_marketplace(mp: str):
    mp = (mp or "").strip()
    if mp in EU_MARKETPLACES:
        return "eu-west-1", "https://sellingpartnerapi-eu.amazon.com"
    if mp in NA_MARKETPLACES:
        return "us-east-1", "https://sellingpartnerapi-na.amazon.com"
    if mp in FE_MARKETPLACES:
        return "us-west-2", "https://sellingpartnerapi-fe.amazon.com"
    return None, None

def _apply_region_and_marketplace_from_request(marketplace_id: str | None = None):
    """
    BACKWARD COMPATIBLE + FIXED:
    - Accepts marketplace_id kwarg (prevents your TypeError)
    - If your amazon_client has set_marketplace/set_region -> uses them (best)
    - Else falls back to setting attributes (marketplace_id/region/base_url/api_base_url if present)
    """
    mp = (marketplace_id or "").strip()

    if not mp:
        mp = (
            (request.args.get("marketplace_id") or "")
            or (request.form.get("marketplace_id") or "")
            or ((request.get_json(silent=True) or {}).get("marketplace_id") or "")
        ).strip()

    if not mp and getattr(amazon_client, "marketplace_id", None):
        mp = str(getattr(amazon_client, "marketplace_id"))

    region, base = _region_base_for_marketplace(mp)

    # Preferred: call client methods if they exist (your AmazonSPAPIClient has these)
    if mp and hasattr(amazon_client, "set_marketplace"):
        try:
            amazon_client.set_marketplace(mp)
            # set_marketplace typically sets region/base internally
            logger.info(f"SP-API set_marketplace({mp}) -> region={getattr(amazon_client,'region',None)} base={getattr(amazon_client,'api_base_url',None) or getattr(amazon_client,'base_url',None)}")
            return
        except Exception:
            logger.exception("amazon_client.set_marketplace failed; falling back to attribute set")

    # Fallback: attribute mutation (for older client implementations)
    if mp:
        try:
            amazon_client.marketplace_id = mp
        except Exception:
            pass

    if region:
        # some clients use `region`
        try:
            amazon_client.region = region
        except Exception:
            pass
        # some clients expose set_region
        if hasattr(amazon_client, "set_region"):
            try:
                amazon_client.set_region(region)
            except Exception:
                pass

    if base:
        # different clients use different names
        for attr in ("api_base_url", "base_url"):
            if hasattr(amazon_client, attr):
                try:
                    setattr(amazon_client, attr, base)
                except Exception:
                    pass

    logger.info(
        f"SP-API applied -> region={region or getattr(amazon_client,'region',None)}; "
        f"base={base or getattr(amazon_client,'api_base_url',None) or getattr(amazon_client,'base_url',None)}; "
        f"mkt={mp or getattr(amazon_client,'marketplace_id',None)}"
    )

# ============================================================
# Header map & type coercion
# ============================================================
FEE_PREVIEW_HEADER_MAP = {
    "sku": "sku",
    "fnsku": "fnsku",
    "asin": "asin",
    "amazon-store": "amazon_store",
    "product-name": "product_name",
    "product-group": "product_group",
    "brand": "brand",
    "fulfilled-by": "fulfilled_by",
    "has-local-inventory": "has_local_inventory",
    "your-price": "your_price",
    "sales-price": "sales_price",
    "longest-side": "longest_side",
    "median-side": "median_side",
    "shortest-side": "shortest_side",
    "length-and-girth": "length_and_girth",
    "unit-of-dimension": "unit_of_dimension",
    "item-package-weight": "item_package_weight",
    "unit-of-weight": "unit_of_weight",
    "product-size-weight-band": "product_size_weight_band",
    "currency": "currency",
    "estimated-fee-total": "estimated_fee_total",
    "estimated-referral-fee-per-unit": "estimated_referral_fee_per_unit",
    "estimated-variable-closing-fee": "estimated_variable_closing_fee",
    "estimated-order-handling-fee-per-order": "estimated_order_handling_fee_per_order",
    "expected-domestic-fulfilment-fee-per-unit": "expected_domestic_fulfilment_fee_per_unit",
    "expected-efn-fulfilment-fee-per-unit-uk": "expected_efn_fulfilment_fee_per_unit_uk",
    "expected-efn-fulfilment-fee-per-unit-de": "expected_efn_fulfilment_fee_per_unit_de",
    "expected-efn-fulfilment-fee-per-unit-fr": "expected_efn_fulfilment_fee_per_unit_fr",
    "expected-efn-fulfilment-fee-per-unit-it": "expected_efn_fulfilment_fee_per_unit_it",
    "expected-efn-fulfilment-fee-per-unit-es": "expected_efn_fulfilment_fee_per_unit_es",
    "expected-efn-fulfilment-fee-per-unit-se": "expected_efn_fulfilment_fee_per_unit_se",
}
FEE_PREVIEW_HEADER_MAP.update({
    "seller-sku": "sku",
    "item-name": "product_name",
    "price": "your_price",
    "item-price": "your_price",
})

_NUMERIC_COLS = {
    "your_price", "sales_price", "longest_side", "median_side", "shortest_side", "length_and_girth",
    "item_package_weight", "estimated_fee_total", "estimated_referral_fee_per_unit",
    "estimated_variable_closing_fee", "estimated_order_handling_fee_per_order",
    "expected_domestic_fulfilment_fee_per_unit",
    "expected_efn_fulfilment_fee_per_unit_uk", "expected_efn_fulfilment_fee_per_unit_de",
    "expected_efn_fulfilment_fee_per_unit_fr", "expected_efn_fulfilment_fee_per_unit_it",
    "expected_efn_fulfilment_fee_per_unit_es", "expected_efn_fulfilment_fee_per_unit_se",
}

def _coerce_bool(v):
    if v is None:
        return None
    s = str(v).strip().lower()
    if s in ("true", "t", "1", "yes", "y"):
        return True
    if s in ("false", "f", "0", "no", "n"):
        return False
    return None

def _coerce_decimal(v):
    if v is None or v == "":
        return None
    try:
        return Decimal(str(v).replace(",", "").strip())
    except InvalidOperation:
        return None

# ============================================================
# CSV/TSV parsing (fast)
# ============================================================
def _read_fee_bytes(raw: bytes):
    sample = raw[:4096].decode("utf-8", errors="replace")
    delim = "\t" if sample.count("\t") > sample.count(",") else ","
    rdr = csv.DictReader(io.StringIO(raw.decode("utf-8", errors="replace")), delimiter=delim)

    def _norm_key(k: str) -> str:
        return (k or "").strip().lower().replace("  ", " ").replace(" ", "-")

    rows = []
    for r in rdr:
        rows.append({_norm_key(k): (v or "").strip() for k, v in r.items()})
    return rows

def _row_has_sku(r: dict) -> bool:
    return bool((r.get("sku") or r.get("seller-sku") or "").strip())

# ============================================================
# Normalize & dedupe
# ============================================================
def _normalize_fee_row(r: dict, user_id: int, marketplace_id: str) -> dict:
    sku = (r.get("sku") or r.get("seller-sku") or "").strip()
    mp = (marketplace_id or "").strip()
    out = {
        "user_id": user_id,
        "sku": sku or None,
        "marketplace_id": mp,
        "synced_at": datetime.utcnow(),
    }
    for src, dst in FEE_PREVIEW_HEADER_MAP.items():
        if src not in r:
            continue
        val = r.get(src)
        if dst == "has_local_inventory":
            out[dst] = _coerce_bool(val)
        elif dst in _NUMERIC_COLS:
            out[dst] = _coerce_decimal(val)
        else:
            out[dst] = (val or None)
    return out

def _dedupe_fee_rows(rows: list[dict]) -> list[dict]:
    by_key = {}
    for r in rows:
        k = (r.get("user_id"), r.get("sku"), r.get("marketplace_id"))
        if not k[0] or not k[1] or not k[2]:
            continue
        by_key[k] = r  # last wins
    return list(by_key.values())

# ============================================================
# Upsert Fee table
# ============================================================
_FEE_UPSERT_COLS = sorted(set(FEE_PREVIEW_HEADER_MAP.values()))

def _upsert_fee_preview(rows: list[dict]) -> int:
    if not rows:
        return 0
    unique_rows = _dedupe_fee_rows(rows)
    if not unique_rows:
        return 0

    stmt = pg_insert(Fee).values(unique_rows)
    excluded = stmt.excluded

    update_cols = {}
    for col in _FEE_UPSERT_COLS:
        if hasattr(excluded, col):
            update_cols[col] = getattr(excluded, col)

    if hasattr(excluded, "synced_at"):
        update_cols["synced_at"] = getattr(excluded, "synced_at")

    update_cols["updated_at"] = datetime.utcnow()

    stmt = stmt.on_conflict_do_update(
        constraint="uq_fee_user_sku_mkt",
        set_=update_cols,
    )

    db.session.execute(stmt)
    db.session.commit()
    return len(unique_rows)

# ============================================================
# SP-API document helpers
# ============================================================
REPORTS_BASE = "/reports/2021-06-30"
FEE_PREVIEW_REPORT_TYPES = [
    "GET_REFERRAL_FEE_PREVIEW_REPORT",
    "GET_FBA_ESTIMATED_FBA_FEES_TXT_DATA",
]

def _strip_pkcs7(data: bytes) -> bytes:
    pad = data[-1]
    if 1 <= pad <= 16:
        return data[:-pad]
    return data

def _decrypt_report_bytes(enc_details: dict, raw: bytes) -> bytes:
    key_b = base64.b64decode(enc_details.get("key", ""))
    iv_b = base64.b64decode(enc_details.get("initializationVector", ""))
    cipher = AES.new(key_b, AES.MODE_CBC, iv_b)
    dec = cipher.decrypt(raw)
    return _strip_pkcs7(dec)

def _download_report_document(doc: dict) -> bytes:
    url = doc.get("url")
    if not url:
        raise RuntimeError("Missing report document URL")
    resp = requests.get(url, timeout=120)
    resp.raise_for_status()
    blob = resp.content
    enc = doc.get("encryptionDetails")
    if enc:
        blob = _decrypt_report_bytes(enc, blob)
    if (doc.get("compressionAlgorithm") or "").upper() == "GZIP":
        blob = gzip.decompress(blob)
    return blob

def _assert_marketplace_allowed(marketplace_id: str):
    """
    Optional guard: checks seller participations and warns early.
    Handles payload being dict OR list (some client wrappers return list directly).
    """
    try:
        _apply_region_and_marketplace_from_request(marketplace_id=marketplace_id)
        res = amazon_client.make_api_call("/sellers/v1/marketplaceParticipations", "GET")

        # 1) unwrap various response shapes safely
        raw_payload = None
        if isinstance(res, dict):
            raw_payload = res.get("payload", None)
            if raw_payload is None:
                # sometimes wrapper returns the payload at top-level
                raw_payload = res
        else:
            raw_payload = res  # could be list already

        # 2) normalize to a list of participations
        parts = []
        if isinstance(raw_payload, list):
            parts = raw_payload
        elif isinstance(raw_payload, dict):
            # common shapes: {"participations":[...]} or {"payload":{"participations":[...]}}
            parts = (
                raw_payload.get("participations")
                or raw_payload.get("Participations")
                or []
            )

        # 3) extract marketplace ids robustly
        allowed = set()
        for p in parts:
            if not isinstance(p, dict):
                continue
            mp_obj = p.get("marketplace") or p.get("marketplaceDetails") or {}
            if isinstance(mp_obj, dict):
                mid = mp_obj.get("id") or mp_obj.get("marketplaceId")
                if mid:
                    allowed.add(mid)

        if allowed and marketplace_id not in allowed:
            raise ValueError(
                f"Seller is not registered in marketplace {marketplace_id}. Allowed: {sorted(allowed)}"
            )

    except Exception:
        logger.exception("Marketplace participation check failed (continuing).")

def _create_and_fetch_fee_preview_bytes(marketplace_id: str, timeout_seconds: int = 180) -> bytes:
    """
    Polls report generation. Re-applies region before each call (reduces client-global issues).
    """
    last_err = None
    _assert_marketplace_allowed(marketplace_id)

    for rtype in FEE_PREVIEW_REPORT_TYPES:
        try:
            _apply_region_and_marketplace_from_request(marketplace_id=marketplace_id)

            created = amazon_client.make_api_call(
                f"{REPORTS_BASE}/reports",
                "POST",
                data={"reportType": rtype, "marketplaceIds": [marketplace_id]},
            )

            if not created or "error" in created:
                rj = (created or {}).get("response_json") or {}
                errs = rj.get("errors") or []
                if errs:
                    code = errs[0].get("code")
                    msg = errs[0].get("message", "")
                    if code == "InvalidInput" and "not registered in marketplace" in msg.lower():
                        raise ValueError(msg)
                    if code in ("Unauthorized", "AccessDenied"):
                        raise PermissionError(msg)
                last_err = created
                continue

            report_id = (
                created.get("reportId")
                or (created.get("payload") or {}).get("reportId")
                or created.get("ReportId")
            )
            if not report_id:
                last_err = {"error": "No reportId in create response", "body": created}
                continue

            t0 = time.time()
            sleep_s = 2

            while True:
                _apply_region_and_marketplace_from_request(marketplace_id=marketplace_id)

                status_res = amazon_client.make_api_call(
                    f"{REPORTS_BASE}/reports/{report_id}", "GET"
                )
                if not status_res or "error" in status_res:
                    raise RuntimeError(f"Failed status for {rtype}: {status_res}")

                payload = status_res.get("payload") or status_res
                proc = payload.get("processingStatus")

                if proc == "DONE":
                    doc_id = payload.get("reportDocumentId")
                    if not doc_id:
                        raise RuntimeError(f"Report DONE without document id: {payload}")

                    _apply_region_and_marketplace_from_request(marketplace_id=marketplace_id)

                    doc = amazon_client.make_api_call(
                        f"{REPORTS_BASE}/documents/{doc_id}", "GET"
                    )
                    if not doc or "error" in doc:
                        raise RuntimeError(f"Failed to get report document: {doc}")

                    return _download_report_document(doc.get("payload") or doc)

                if proc in ("CANCELLED", "FATAL"):
                    raise RuntimeError(f"{rtype} failed with status: {proc}")

                if time.time() - t0 > timeout_seconds:
                    raise TimeoutError(f"Timed out waiting for {rtype} to complete.")

                time.sleep(sleep_s)
                sleep_s = min(20, sleep_s + 2)

        except Exception as e:
            last_err = str(e)
            continue

    raise RuntimeError(f"All fee preview report types failed. Last error: {last_err}")

# ============================================================
# OPTIMIZED PIPELINE: Fee -> user table (FULL steps 7/8/9 added)
# ============================================================
def _run_feepreview_upload_pipeline_from_fee_table(
    user_id: int,
    country: str,
    marketplace: str,
    transit_time: int,
    stock_unit: int,
    filename_hint: str = "SPAPI_FEE_PREVIEW",
):
    country = (country or "").lower().strip()
    mp = (marketplace or "").strip()

    fee_q = (
        db.session.query(
            Fee.sku,
            Fee.fnsku,
            Fee.asin,
            Fee.amazon_store,
            Fee.product_name,
            Fee.product_group,
            Fee.brand,
            Fee.your_price,
            Fee.estimated_fee_total,
            Fee.estimated_referral_fee_per_unit,
            Fee.fulfilled_by,
            Fee.has_local_inventory,
            Fee.sales_price,
            Fee.longest_side,
            Fee.median_side,
            Fee.shortest_side,
            Fee.length_and_girth,
            Fee.unit_of_dimension,
            Fee.item_package_weight,
            Fee.unit_of_weight,
            Fee.product_size_weight_band,
            Fee.currency,
            Fee.estimated_variable_closing_fee,
            Fee.expected_domestic_fulfilment_fee_per_unit,
            Fee.expected_efn_fulfilment_fee_per_unit_uk,
            Fee.expected_efn_fulfilment_fee_per_unit_de,
            Fee.expected_efn_fulfilment_fee_per_unit_fr,
            Fee.expected_efn_fulfilment_fee_per_unit_it,
            Fee.expected_efn_fulfilment_fee_per_unit_es,
            Fee.expected_efn_fulfilment_fee_per_unit_se,
        ).filter(Fee.user_id == user_id, Fee.marketplace_id == mp)
    )

    fee_rows = fee_q.all()
    if not fee_rows:
        return {"inserted": 0, "deleted": 0, "message": "No Fee rows for user/marketplace"}

    df = pd.DataFrame(
        [
            {
                "sku": r[0] or "",
                "fnsku": r[1] or "",
                "asin": r[2] or "",
                "amazon_store": r[3] or "",
                "product_name": r[4] or "",
                "product_group": r[5] or "",
                "brand": r[6] or "",
                "price": float(r[7]) if r[7] is not None else None,
                "estimated_fees": float(r[8]) if r[8] is not None else None,
                "estimated_referral_fee": float(r[9]) if r[9] is not None else None,
                "fulfilled_by": r[10] or "",
                "has_local_inventory": (str(r[11]) if r[11] is not None else ""),
                "sales_price": float(r[12]) if r[12] is not None else None,
                "longest_side": float(r[13]) if r[13] is not None else None,
                "median_side": float(r[14]) if r[14] is not None else None,
                "shortest_side": float(r[15]) if r[15] is not None else None,
                "length_and_girth": float(r[16]) if r[16] is not None else None,
                "unit_of_dimension": r[17] or "",
                "item_package_weight": float(r[18]) if r[18] is not None else None,
                "unit_of_weight": r[19] or "",
                "product_size_weight_band": r[20] or "",
                "currency": r[21] or "",
                "estimated_variable_closing_fee": float(r[22]) if r[22] is not None else None,
                "expected_domestic_fulfilment_fee_per_unit": float(r[23]) if r[23] is not None else None,
                "expected_efn_fulfilment_fee_per_unit_uk": float(r[24]) if r[24] is not None else None,
                "expected_efn_fulfilment_fee_per_unit_de": float(r[25]) if r[25] is not None else None,
                "expected_efn_fulfilment_fee_per_unit_fr": float(r[26]) if r[26] is not None else None,
                "expected_efn_fulfilment_fee_per_unit_it": float(r[27]) if r[27] is not None else None,
                "expected_efn_fulfilment_fee_per_unit_es": float(r[28]) if r[28] is not None else None,
                "expected_efn_fulfilment_fee_per_unit_se": float(r[29]) if r[29] is not None else None,
            }
            for r in fee_rows
        ]
    )

    user_engine = create_engine(db_url, pool_pre_ping=True)
    metadata = MetaData()

    admin_engine = create_engine(db_url1, pool_pre_ping=True)
    AdminSession = sessionmaker(bind=admin_engine)
    admin_session = AdminSession()

    table_name = f"user_{user_id}_{country}_table"

    user_specific_table = Table(
        table_name,
        metadata,
        Column("id", Integer, primary_key=True),
        Column("user_id", Integer, nullable=False),
        Column("country", String(255), nullable=False),
        Column("transit_time", Integer, nullable=False),
        Column("stock_unit", Integer, nullable=False),
        Column("product_group", String(255), nullable=False),
        Column("estimated_fees", Float, nullable=True),
        Column("referral_fee", Float, nullable=True),
        Column("estimated_referral_fee", Float, nullable=True),
        Column("marketplace", String(255), nullable=False),
        Column("file_name", String(255), nullable=False),
        Column("sku", String(255), nullable=False),
        Column("fnsku", String(255), nullable=False),
        Column("amazon_store", String(255), nullable=False),
        Column("asin", String(255), nullable=False),
        Column("product_barcode", String(255), nullable=True),
        Column("sku_cost_price", Float, nullable=True),
        Column("product_name", String(255), nullable=False),
        Column("brand", String(255), nullable=False),
        Column("price", Float, nullable=True),
        Column("fulfilled_by", String(255), nullable=True),
        Column("has_local_inventory", String(255), nullable=True),
        Column("sales_price", Float, nullable=True),
        Column("longest_side", Float, nullable=True),
        Column("median_side", Float, nullable=True),
        Column("shortest_side", Float, nullable=True),
        Column("length_and_girth", Float, nullable=True),
        Column("unit_of_dimension", String(50), nullable=True),
        Column("item_package_weight", Float, nullable=True),
        Column("unit_of_weight", String(50), nullable=True),
        Column("product_size_weight_band", String(255), nullable=True),
        Column("currency", String(10), nullable=True),
        Column("estimated_variable_closing_fee", Float, nullable=True),
        Column("expected_domestic_fulfilment_fee_per_unit", Float, nullable=True),
        Column("expected_efn_fulfilment_fee_per_unit_uk", Float, nullable=True),
        Column("expected_efn_fulfilment_fee_per_unit_de", Float, nullable=True),
        Column("expected_efn_fulfilment_fee_per_unit_fr", Float, nullable=True),
        Column("expected_efn_fulfilment_fee_per_unit_it", Float, nullable=True),
        Column("expected_efn_fulfilment_fee_per_unit_es", Float, nullable=True),
        Column("expected_efn_fulfilment_fee_per_unit_se", Float, nullable=True),
        extend_existing=True,
    )
    metadata.create_all(user_engine)

    # indexes
    with user_engine.begin() as conn:
        conn.execute(text(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_user_asin ON {table_name}(user_id, asin)"))
        conn.execute(text(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_user_pg_price ON {table_name}(user_id, product_group, price)"))

    # delete old
    with user_engine.begin() as conn:
        conn.execute(
            text(f"DELETE FROM {table_name} WHERE user_id = :user_id AND country = :country AND marketplace = :marketplace"),
            {"user_id": user_id, "country": country, "marketplace": mp},
        )

    # add fixed cols
    df["user_id"] = user_id
    df["country"] = country
    df["transit_time"] = int(transit_time)
    df["stock_unit"] = int(stock_unit)
    df["marketplace"] = mp
    df["file_name"] = filename_hint
    df["product_barcode"] = None
    df["sku_cost_price"] = None
    df["referral_fee"] = None

    # numeric coercion
    num_cols = [
        "price", "estimated_fees", "estimated_referral_fee", "sales_price", "longest_side", "median_side",
        "shortest_side", "length_and_girth", "item_package_weight", "estimated_variable_closing_fee",
        "expected_domestic_fulfilment_fee_per_unit", "expected_efn_fulfilment_fee_per_unit_uk",
        "expected_efn_fulfilment_fee_per_unit_de", "expected_efn_fulfilment_fee_per_unit_fr",
        "expected_efn_fulfilment_fee_per_unit_it", "expected_efn_fulfilment_fee_per_unit_es",
        "expected_efn_fulfilment_fee_per_unit_se",
    ]
    for c in num_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    insert_cols = [
        "user_id", "country", "transit_time", "stock_unit", "product_group", "estimated_fees", "referral_fee",
        "estimated_referral_fee", "marketplace", "file_name", "sku", "fnsku", "amazon_store", "asin",
        "product_barcode", "sku_cost_price", "product_name", "brand", "price", "fulfilled_by", "has_local_inventory",
        "sales_price", "longest_side", "median_side", "shortest_side", "length_and_girth", "unit_of_dimension",
        "item_package_weight", "unit_of_weight", "product_size_weight_band", "currency", "estimated_variable_closing_fee",
        "expected_domestic_fulfilment_fee_per_unit", "expected_efn_fulfilment_fee_per_unit_uk",
        "expected_efn_fulfilment_fee_per_unit_de", "expected_efn_fulfilment_fee_per_unit_fr",
        "expected_efn_fulfilment_fee_per_unit_it", "expected_efn_fulfilment_fee_per_unit_es",
        "expected_efn_fulfilment_fee_per_unit_se",
    ]
    df_ins = df.reindex(columns=insert_cols)

    CHUNK = 2000
    inserted_rows = 0
    with user_engine.begin() as conn:
        for start in range(0, len(df_ins), CHUNK):
            chunk = df_ins.iloc[start:start + CHUNK]
            conn.execute(user_specific_table.insert(), chunk.to_dict(orient="records"))
            inserted_rows += len(chunk)

    # -----------------------------
    # 7) Referral fee mapping (fast)
    # -----------------------------
    COUNTRY_MAP = {
        "uk": "United Kingdom",
        "us": "United States",
        "uae": "United Arab Emirates",
        "in": "India",
    }
    normalized_country = COUNTRY_MAP.get(country.lower(), country)

    cats = admin_session.query(
        Category.country,
        Category.category,
        Category.price_from,
        Category.price_to,
        Category.referral_fee_percent_est,
    ).filter(Category.country == normalized_country).all()

    if cats:
        df_cat = pd.DataFrame(
            [
                {
                    "category": (c[1] or "").strip(),
                    "price_from": float(c[2]) if c[2] is not None else 0.0,
                    "price_to": float(c[3]) if c[3] is not None else 0.0,
                    "referral_fee": float(c[4]) if c[4] is not None else None,
                }
                for c in cats
            ]
        )

        def _first_word(x: str) -> str:
            x = (x or "").strip()
            if not x:
                return ""
            return x.split("&")[0].split("/")[0].split("-")[0].strip()

        df_cat["category_full"] = df_cat["category"].astype(str)
        df_cat["category_first"] = df_cat["category"].apply(_first_word)

        tmp = f"tmp_cat_map_{user_id}_{int(time.time())}"
        with user_engine.begin() as conn:
            conn.execute(text(f"""
                CREATE TEMP TABLE {tmp}(
                    category_full  text,
                    category_first text,
                    price_from     double precision,
                    price_to       double precision,
                    referral_fee   double precision
                ) ON COMMIT DROP
            """))

            tmp_rows = df_cat[["category_full", "category_first", "price_from", "price_to", "referral_fee"]].to_dict("records")
            for start in range(0, len(tmp_rows), CHUNK):
                conn.execute(
                    text(f"""
                        INSERT INTO {tmp}(category_full, category_first, price_from, price_to, referral_fee)
                        VALUES (:category_full, :category_first, :price_from, :price_to, :referral_fee)
                    """),
                    tmp_rows[start:start + CHUNK],
                )

            conn.execute(
                text(f"""
                    UPDATE {table_name} t
                    SET referral_fee = (
                        SELECT x.referral_fee
                        FROM {tmp} x
                        WHERE
                            (
                                x.category_full = t.product_group
                                OR (
                                    x.category_first <> ''
                                    AND x.category_first =
                                        split_part(
                                            replace(replace(t.product_group, '/', '&'), '-', '&'),
                                            '&', 1
                                        )
                                )
                            )
                            AND (x.price_from = 0 OR x.price_from <= COALESCE(t.price, 0))
                            AND (x.price_to   = 0 OR x.price_to   >= COALESCE(t.price, 0))
                        ORDER BY
                            CASE WHEN x.category_full = t.product_group THEN 0 ELSE 1 END,
                            x.price_from DESC
                        LIMIT 1
                    )
                    WHERE t.user_id = :user_id
                      AND t.marketplace = :mp
                      AND t.country = :country
                """),
                {"user_id": user_id, "mp": mp, "country": country},
            )

    # -----------------------------
    # 8) SKU join update (one SQL)
    # -----------------------------
    sku_table_name = f"sku_{user_id}_data_table"
    with user_engine.begin() as conn:
        try:
            insp = sa_inspect(conn)
            if insp.has_table(sku_table_name):
                conn.execute(
                    text(f"""
                        UPDATE {table_name} t
                        SET product_barcode = s.product_barcode,
                            sku_cost_price   = s.price
                        FROM {sku_table_name} s
                        WHERE t.user_id = :user_id
                          AND t.asin = s.asin
                    """),
                    {"user_id": user_id},
                )
        except Exception:
            logger.exception("SKU join update failed (ignored)")

    # -----------------------------
    # 9) CountryProfile upsert
    # -----------------------------
    existing_profile = CountryProfile.query.filter_by(
        user_id=user_id, country=country, marketplace=mp
    ).first()
    if existing_profile:
        existing_profile.transit_time = transit_time
        existing_profile.stock_unit = stock_unit
        db.session.commit()
    else:
        db.session.add(
            CountryProfile(
                user_id=user_id,
                country=country,
                marketplace=mp,
                transit_time=transit_time,
                stock_unit=stock_unit,
            )
        )
        db.session.commit()

    return {"inserted": inserted_rows, "deleted": 0, "table": table_name}

# ============================================================
# Routes
# ============================================================
@fee_preview_bp.route("/amazon_api/fees/sync", methods=["POST", "GET"])
def fees_sync():
    auth_header = request.headers.get("Authorization")
    if not auth_header or not auth_header.startswith("Bearer "):
        return jsonify({"success": False, "error": "Authorization token is missing or invalid"}), 401

    token = auth_header.split(" ")[1]
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=["HS256"])
        user_id = payload.get("user_id")
    except jwt.ExpiredSignatureError:
        return jsonify({"success": False, "error": "Token has expired"}), 401
    except jwt.InvalidTokenError:
        return jsonify({"success": False, "error": "Invalid token"}), 401

    if request.method == "GET":
        mp = request.args.get("marketplace_id") or getattr(amazon_client, "marketplace_id", None)
        source = (request.args.get("source") or "report").lower()
    else:
        mp = request.form.get("marketplace_id") or request.args.get("marketplace_id") or getattr(amazon_client, "marketplace_id", None)
        source = (request.form.get("source") or request.args.get("source") or "auto").lower()

    mp = (mp or "").strip()
    if not mp:
        return jsonify({"success": False, "error": "marketplace_id is required"}), 400

    _apply_region_and_marketplace_from_request(marketplace_id=mp)

    try:
        rows, used_source = [], None

        if request.method == "POST" and (source in ("auto", "upload")) and "file" in request.files:
            f = request.files["file"]
            raw = f.read()
            if raw[:2] == b"\x1f\x8b":
                raw = gzip.decompress(raw)
            rows = _read_fee_bytes(raw)
            used_source = "upload"

        if not rows and source in ("auto", "report"):
            blob = _create_and_fetch_fee_preview_bytes(mp)
            rows = _read_fee_bytes(blob)
            used_source = "report"

        if not rows:
            return jsonify({"success": False, "error": "No rows found from source"}), 400

        normalized = [_normalize_fee_row(r, user_id, mp) for r in rows if _row_has_sku(r)]
        saved = _upsert_fee_preview(normalized)

        sample = [
            {
                "sku": r.get("sku"),
                "asin": r.get("asin"),
                "currency": r.get("currency"),
                "your_price": (str(r.get("your_price")) if r.get("your_price") is not None else None),
                "estimated_fee_total": (str(r.get("estimated_fee_total")) if r.get("estimated_fee_total") is not None else None),
            }
            for r in normalized[:5]
        ]

        resp = jsonify(
            {
                "success": True,
                "source": used_source,
                "marketplace_id": mp,
                "count_in_file": len(normalized),
                "saved": saved,
                "sample": sample,
            }
        )
        if request.method == "GET":
            resp.headers["Cache-Control"] = "no-store"
        return resp, 200

    except TimeoutError as te:
        db.session.rollback()
        return jsonify({"success": False, "error": f"Timeout: {str(te)}"}), 504
    except Exception as e:
        logger.exception("fees_sync failed")
        db.session.rollback()
        return jsonify({"success": False, "error": str(e)}), 500

@fee_preview_bp.route("/amazon_api/fees", methods=["GET"])
def list_fees():
    auth_header = request.headers.get("Authorization")
    user_id = None
    if auth_header and auth_header.startswith("Bearer "):
        token = auth_header.split(" ")[1]
        try:
            payload = jwt.decode(token, SECRET_KEY, algorithms=["HS256"])
            user_id = payload.get("user_id")
        except Exception:
            pass

    mp = (request.args.get("marketplace_id") or getattr(amazon_client, "marketplace_id", None) or "").strip()
    if not mp:
        return jsonify({"success": False, "error": "marketplace_id is required"}), 400

    _apply_region_and_marketplace_from_request(marketplace_id=mp)

    sku = request.args.get("sku")
    limit = max(1, min(int(request.args.get("limit", 100)), 1000))

    q = Fee.query.filter(Fee.marketplace_id == mp)
    if user_id is not None:
        q = q.filter(Fee.user_id == user_id)

    if sku:
        if sku.endswith("*"):
            q = q.filter(Fee.sku.ilike(sku[:-1] + "%"))
        else:
            q = q.filter(Fee.sku == sku)

    items = q.order_by(Fee.updated_at.desc()).limit(limit).all()

    def _row(x: Fee):
        return {
            "sku": x.sku,
            "asin": x.asin,
            "fnsku": x.fnsku,
            "marketplace_id": x.marketplace_id,
            "product_name": x.product_name,
            "brand": x.brand,
            "fulfilled_by": x.fulfilled_by,
            "has_local_inventory": x.has_local_inventory,
            "currency": x.currency,
            "your_price": str(x.your_price) if x.your_price is not None else None,
            "sales_price": str(x.sales_price) if x.sales_price is not None else None,
            "estimated_fee_total": str(x.estimated_fee_total) if x.estimated_fee_total is not None else None,
            "estimated_referral_fee_per_unit": str(x.estimated_referral_fee_per_unit) if x.estimated_referral_fee_per_unit is not None else None,
            "expected_domestic_fulfilment_fee_per_unit": str(x.expected_domestic_fulfilment_fee_per_unit) if x.expected_domestic_fulfilment_fee_per_unit is not None else None,
            "expected_efn_fulfilment_fee_per_unit_uk": str(x.expected_efn_fulfilment_fee_per_unit_uk) if x.expected_efn_fulfilment_fee_per_unit_uk is not None else None,
            "updated_at": x.updated_at.isoformat() if x.updated_at else None,
        }

    return jsonify(
        {
            "success": True,
            "marketplace_id": mp,
            "count": len(items),
            "items": [_row(i) for i in items],
        }
    ), 200

@fee_preview_bp.route("/amazon_api/fees/sync_and_upload", methods=["POST"])
def fees_sync_and_upload():
    auth_header = request.headers.get("Authorization")
    if not auth_header or not auth_header.startswith("Bearer "):
        return jsonify({"success": False, "error": "Authorization token is missing or invalid"}), 401

    token = auth_header.split(" ")[1]
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=["HS256"])
        user_id = payload.get("user_id")
    except jwt.ExpiredSignatureError:
        return jsonify({"success": False, "error": "Token has expired"}), 401
    except jwt.InvalidTokenError:
        return jsonify({"success": False, "error": "Invalid token"}), 401

    data = request.get_json(silent=True) or {}
    country = (data.get("country") or request.form.get("country") or "").lower().strip()

    mp = (
        data.get("marketplace_id")
        or request.form.get("marketplace_id")
        or request.args.get("marketplace_id")
        or getattr(amazon_client, "marketplace_id", None)
        or ""
    ).strip()
    source = (data.get("source") or request.form.get("source") or request.args.get("source") or "report").lower()

    if not country or not mp:
        return jsonify({"success": False, "error": "country and marketplace_id are required"}), 400

    _apply_region_and_marketplace_from_request(marketplace_id=mp)

    try:
        rows, used_source = [], None

        if source in ("auto", "report"):
            blob = _create_and_fetch_fee_preview_bytes(mp)
            rows = _read_fee_bytes(blob)
            used_source = "report"

        elif source == "upload" and "file" in request.files:
            f = request.files["file"]
            raw = f.read()
            if raw[:2] == b"\x1f\x8b":
                raw = gzip.decompress(raw)
            rows = _read_fee_bytes(raw)
            used_source = "upload"

        if not rows:
            return jsonify({"success": False, "error": "No rows found from source"}), 400

        normalized = [_normalize_fee_row(r, user_id, mp) for r in rows if _row_has_sku(r)]
        saved = _upsert_fee_preview(normalized)

        pip = _run_feepreview_upload_pipeline_from_fee_table(
            user_id=user_id,
            country=country,
            marketplace=mp,
            transit_time=int(data.get("transit_time") or 0),
            stock_unit=int(data.get("stock_unit") or 0),
            filename_hint="SPAPI_FEE_PREVIEW",
        )

        sample = [
            {
                "sku": r.get("sku"),
                "asin": r.get("asin"),
                "currency": r.get("currency"),
                "your_price": (str(r.get("your_price")) if r.get("your_price") is not None else None),
                "estimated_fee_total": (str(r.get("estimated_fee_total")) if r.get("estimated_fee_total") is not None else None),
            }
            for r in normalized[:5]
        ]

        return jsonify(
            {
                "success": True,
                "source": used_source,
                "marketplace_id": mp,
                "count_in_file": len(normalized),
                "saved_into_fee": saved,
                "upload_pipeline": pip,
                "sample": sample,
            }
        ), 200

    except TimeoutError as te:
        db.session.rollback()
        return jsonify({"success": False, "error": f"Timeout: {str(te)}"}), 504
    except Exception as e:
        logger.exception("fees_sync_and_upload failed")
        db.session.rollback()
        return jsonify({"success": False, "error": str(e)}), 500

