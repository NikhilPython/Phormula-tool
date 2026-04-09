#---------------------------------------------------------------------------------------------------------------------------------------------------------------------#
from __future__ import annotations
import base64, io, os, time, calendar, gzip, csv, logging, random, urllib.request, re
import pandas as pd
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from sqlalchemy.dialects.postgresql import insert as pg_insert
import boto3, jwt, requests
from dotenv import find_dotenv, load_dotenv
from flask import Blueprint, Response, jsonify, request
from app import db
from typing import Any, Iterable
from app.utils.amazon_utils import run_upload_pipeline_from_df, amazon_client, _apply_region_and_marketplace_from_request
from app.models.user_models import db, SettlementTransaction
from sqlalchemy import MetaData
from config import Config

# --- App config / auth
SECRET_KEY = Config.SECRET_KEY

# --- load .env robustly (works no matter where you run `flask run`) ---
dotenv_path = find_dotenv(filename=".env", usecwd=True)
load_dotenv(dotenv_path, override=True)

db_url  = os.getenv('DATABASE_URL')
db_url1 = os.getenv('DATABASE_ADMIN_URL') or db_url  # fallback

if not db_url:
    raise RuntimeError("DATABASE_URL is not set")
if not db_url1:
    print("[WARN] DATABASE_ADMIN_URL not set; falling back to DATABASE_URL")

amazon_sales_api_bp = Blueprint("amazon_sales_api", __name__)

# ------------------------------------------------------------- MTD report ----------------------------------------------------------------------

# ---------- tiny utils ----------
def _iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")

def _norm_marketplace(s: str | None) -> str:
    if not s:
        return ""
    return re.sub(r"[^a-z0-9]", "", str(s).lower())

def _get_bool(val: str | None, default: bool) -> bool:
    if val is None:
        return default
    return str(val).strip().lower() in ("1", "true", "yes", "y")

def _parse_month_input(text: str) -> tuple[int, int]:
    if not text or not str(text).strip():
        raise ValueError("Empty month")
    s = str(text).strip()
    y_now = datetime.utcnow().year
    try:
        dt = datetime.strptime(s, "%Y-%m")
        return dt.year, dt.month
    except ValueError:
        pass
    try:
        if "-" in s:
            y_str, m_str = s.split("-", 1)
            y = int(y_str); m = int(m_str)
            if 1 <= m <= 12:
                return y, m
    except Exception:
        pass
    if "-" in s:
        y_str, name = s.split("-", 1)
        try:
            y = int(y_str)
        except Exception:
            raise ValueError("Invalid year in month parameter")
        name_l = name.strip().lower()
        for i in range(1, 13):
            if (calendar.month_name[i].lower() == name_l
                or calendar.month_abbr[i].lower() == name_l):
                return y, i
        raise ValueError(f"Invalid month name: {name}")
    name_l = s.lower()
    for i in range(1, 13):
        if (calendar.month_name[i].lower() == name_l
            or calendar.month_abbr[i].lower() == name_l):
            return y_now, i
    if s.isdigit():
        m = int(s)
        if 1 <= m <= 12:
            return y_now, m
    raise ValueError("Invalid month parameter")

def _month_bounds(month_str: str) -> tuple[datetime, datetime]:
    year, month = _parse_month_input(month_str)
    start = datetime(year, month, 1)
    end = (start.replace(day=28) + timedelta(days=4)).replace(day=1)
    return start, end

def _to_decimal(v):
    try:
        if v is None or v == "":
            return None
        return Decimal(str(v).replace(",", "").strip())
    except InvalidOperation:
        return None

# put near the top of the module
_US_TZ_OFFSETS = {
    "UTC": 0, "GMT": 0,
    "PST": -8, "PDT": -7,
    "MST": -7, "MDT": -6,
    "CST": -6, "CDT": -5,
    "EST": -5, "EDT": -4,
}

def _parse_dt_any(v):
    if not v:
        return None
    s = str(v).strip().replace("\u00A0", " ")
    try:
        s_iso = s.replace("Z", "+00:00") if ("T" in s and ("Z" in s or "+" in s or "-" in s[10:])) else s
        dt = datetime.fromisoformat(s_iso)
        if dt.tzinfo is not None:
            dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
        return dt
    except Exception:
        pass
    for fmt in (
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S.%fZ",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
        "%d.%m.%Y",
        "%d.%m.%Y %H:%M:%S UTC",
    ):
        try:
            dt = datetime.strptime(s, fmt)
            return dt
        except Exception:
            continue
    try:
        if len(s) >= 6 and s[-3] == ":" and s[-6] in "+-":
            s2 = s[:-3] + s[-2:]
            dt = datetime.strptime(s2, "%Y-%m-%dT%H:%M:%S%z")
            return dt.astimezone(timezone.utc).replace(tzinfo=None)
    except Exception:
        pass
    try:
        left, tz_abbr = None, None
        parts = s.rsplit(" ", 1)
        if len(parts) == 2:
            left, tz_abbr = parts[0], parts[1].upper().strip(",")
        if tz_abbr in _US_TZ_OFFSETS:
            left_candidates = [left, left.replace(",", "")]
            fmts = ["%b %d %Y %I:%M:%S %p", "%b %d %Y %I:%M %p"]
            for base in left_candidates:
                for fmt in fmts:
                    try:
                        local = datetime.strptime(base, fmt)
                        offset = _US_TZ_OFFSETS[tz_abbr]
                        return (local - timedelta(hours=offset))
                    except Exception:
                        continue
    except Exception:
        pass
    return None

def _is_settlement_type(rt: str) -> bool:
    return rt in (
        "GET_V2_SETTLEMENT_REPORT_DATA_FLAT_FILE_V2",
        "GET_V2_SETTLEMENT_REPORT_DATA_FLAT_FILE",
        "GET_V2_SETTLEMENT_REPORT_DATA_XML",
    )

def _not_allowed_now(resp: dict | None) -> bool:
    if not resp or not isinstance(resp, dict):
        return False
    if resp.get("status_code") != 400:
        return False
    err = (resp.get("response_json") or {}).get("errors") or []
    msg = (err[0] or {}).get("message", "").lower() if err else ""
    return ("not allowed at this time" in msg) or ("not allowed" in msg)


def _parse_settlement_tsv(tsv_bytes: bytes) -> list[dict]:
    import re

    text = tsv_bytes.decode("utf-8", errors="replace")
    lines = text.splitlines()
    if not lines:
        return []

    # ---------- find the actual header row (skip "Includes...", "Definitions:", etc.) ----------
    def _normkey(s: str) -> str:
        return str(s).strip().lower().replace(" ", "").replace("-", "").replace("_", "")

    def _looks_like_header(line: str) -> bool:
        hs = [h.strip() for h in line.split("\t")]
        keys = {_normkey(h) for h in hs}
        # Require the signature columns to exist
        return (
            any(k in keys for k in ("datetime", "date/time", "posteddatetime", "posteddate", "transactiondate"))
            and any(k in keys for k in ("type", "transactiontype"))
            and any(k in keys for k in ("total", "totalamount"))
        )

    header_idx = None
    for i, ln in enumerate(lines):
        if _looks_like_header(ln):
            header_idx = i
            break
    if header_idx is None:
        # Fallback: treat first line as header (old behavior)
        header_idx = 0

    header_line = lines[header_idx].lstrip("\ufeff")

    # Remove parenthetical suffixes like " (UTC)" then keep visible header text
    def _clean_header(h: str) -> str:
        return re.sub(r"\s*\(.*?\)\s*", "", h).strip()

    visible_headers = [_clean_header(h) for h in header_line.split("\t")]
    norm_to_visible = {_normkey(h): h for h in visible_headers}

    def get(row_map: dict, *names, default=""):
        for n in names:
            vis = norm_to_visible.get(_normkey(n))
            if vis is not None and vis in row_map:
                return row_map[vis]
        return default

    # Accept all known line-level timestamps (do NOT use settlement window dates)
    DATE_KEYS = (
        "posted-date-time",
        "posted-date",
        "transaction-date",
        "transaction-posted-date-time",
        "transaction-posted-date",
        "date/time",
    )

    out = []
    for ln in lines[header_idx + 1:]:
        if not ln.strip():
            continue
        cols = ln.split("\t")
        # Build row map using the *visible* header text we cleaned above
        row = {h: (cols[i] if i < len(cols) else "") for i, h in enumerate(visible_headers)}

        try:
            mapped = {
                "settlement id": get(row, "settlement-id", "settlement id"),
                "order id":      get(row, "order-id", "order id"),
                "sku":           get(row, "sku"),

                # Real per-line timestamp only
                "date/time":     get(row, *DATE_KEYS),

                # Preserve exactly what the file has for transaction type
                "type":          get(row, "transaction-type", "transaction type", "type"),

                "marketplace":   get(row, "marketplace-name", "marketplace"),
                "fulfilment":    get(row, "fulfillment-id", "fulfilment", "fulfillment"),

                "description":   get(row, "product-name", "description"),
                "quantity":      get(row, "quantity-purchased", "quantity", "quantity-shipped", "qty"),

                "order city":    get(row, "ship-city", "shipping-city", "shipment-city", "buyer-city",
                                         "recipient-city", "ship to city", "delivery-city", "order city", "city"),
                "order state":   get(row, "ship-state", "shipping-state", "shipment-state", "buyer-state",
                                         "recipient-state", "ship to state", "delivery-state", "order state", "state"),
                "order postal":  get(row, "ship-postal-code", "shipping-postal-code", "shipment-postal-code",
                                         "buyer-postal-code", "recipient-postal-code", "ship to postal code",
                                         "delivery-postal-code", "postal-code", "zip", "zipcode", "postcode"),

                "tax collection model": get(row, "tax-collection-model", "tax collection model", "tax_collection_model"),

                # Money buckets (Amazon uses various names across templates)
                "product sales":         get(row, "product-sales", "product sales", "total-amount", "total"),
                "product sales tax":     get(row, "product-sales-tax", "product sales tax"),
                "postage credits":       get(row, "shipping-credits", "postage credits", "shipping credits"),
                "shipping credits tax":  get(row, "shipping-credits-tax", "shipping credits tax"),
                "gift wrap credits":     get(row, "gift-wrap-credits", "gift wrap credits"),
                "giftwrap credits tax":  get(row, "gift-wrap-credits-tax", "giftwrap credits tax"),
                "promotional rebates":   get(row, "promotional-rebates", "promotional rebates"),
                "promotional rebates tax": get(row, "promotional-rebates-tax", "promotional rebates tax"),
                "marketplace withheld tax": get(row, "marketplace-withheld-tax", "marketplace withheld tax"),
                "selling fees":          get(row, "selling-fees", "selling fees"),
                "fba fees":              get(row, "fba-fees", "fba fees"),
                "other transaction fees": get(row, "other-transaction-fees", "other transaction fees",
                                                  "regulatory fee"),
                "other":                 get(row, "other"),
                "total":                 get(row, "total", "total-amount"),

                "advertising cost":      get(row, "cost-of-advertising", "cost of advertising", "advertising", "advertisement"),
                "currency":              get(row, "currency", "total-currency", "amount-currency", "currency-code"),

                # Raw amount group (present in some settlement TSVs)
                "_amount_type": get(row, "amount-type", "amount type", "charge-type", "price-type"),
                "_amount_desc": get(row, "amount-description", "amount description", "charge-description", "price-description"),
                "_amount":      get(row, "amount"),
            }

            # Optional: capture "Tax On Regulatory Fee" so the aggregator can treat it as tax later
            torf = get(row, "tax on regulatory fee")
            if torf != "":
                mapped.setdefault("_extras", {})["tax_on_regulatory_fee"] = torf

            out.append(mapped)
        except Exception:
            # skip malformed line
            continue

    return out


def _parse_settlement_xml(xml_bytes: bytes) -> list[dict]:
    try:
        import xml.etree.ElementTree as ET
    except Exception:
        return []
    txt = xml_bytes.decode("utf-8", errors="replace")
    try:
        root = ET.fromstring(txt)
    except Exception:
        return []

    def norm(tag: str) -> str:
        return tag.split('}')[-1].lower().replace("_", "").replace("-", "")

    def find_text_deep(node, *cands):
        want = {c.replace(" ", "").replace("_", "").lower() for c in cands}
        for el in node.iter():
            if norm(el.tag) in want:
                v = (el.text or "").strip()
                if v:
                    return v
        return ""

    def extract_amount_groups(tx):
        groups = []
        for parent in tx.iter():
            amt_el = None; cur_el = None; typ_el = None; desc_el = None
            for ch in list(parent):
                tag = norm(ch.tag)
                if tag in ("amount", "totalamount"):
                    amt_el = ch
                elif tag in ("currency", "currencycode", "amountcurrency"):
                    cur_el = ch
                elif tag in ("amounttype", "chargetype", "pricetype"):
                    typ_el = ch
                elif tag in ("amountdescription", "chargedescription", "pricedescription"):
                    desc_el = ch
            if amt_el is not None:
                amt_txt = (amt_el.text or "").strip()
                if amt_txt != "":
                    groups.append({
                        "amount": amt_txt,
                        "currency": (cur_el.text or "").strip() if cur_el is not None else "",
                        "type": (typ_el.text or "").strip() if typ_el is not None else "",
                        "desc": (desc_el.text or "").strip() if desc_el is not None else "",
                    })
        return groups

    tx_nodes = [el for el in root.iter() if norm(el.tag).endswith("transaction")]
    rows = []
    for tx in tx_nodes:
        sid   = find_text_deep(tx, "settlement-id", "settlementid")
        oid   = find_text_deep(tx, "order-id", "amazon-order-id", "orderid")
        sku   = find_text_deep(tx, "sku", "seller-sku", "sellersku")
        ttype = find_text_deep(tx, "transaction-type", "transactiontype", "type")
        mp    = find_text_deep(tx, "marketplace-name", "marketplacename", "marketplace")
        fulf  = find_text_deep(tx, "fulfillment-id", "fulfilment", "fulfillmentid", "fulfillment")
        dt = find_text_deep(
            tx,
            "posted-date-time","posted-date","posteddatetime","posteddate",
            "transaction-date","transactiondate",
            "transaction-posted-date-time",  # ← add this
            "transaction-posted-date",
            "date"
        )

        for g in extract_amount_groups(tx):
            rows.append({
                "settlement id": sid,
                "order id": oid,
                "sku": sku,
                "date/time": dt,
                "type": ttype,
                "marketplace": mp,
                "fulfilment": fulf,
                "product sales": "",
                "product sales tax": "",
                "postage credits": "",
                "shipping credits tax": "",
                "gift wrap credits": "",
                "giftwrap credits tax": "",
                "promotional rebates": "",
                "promotional rebates tax": "",
                "marketplace withheld tax": "",
                "selling fees": "",
                "fba fees": "",
                "other transaction fees": "",
                "other": "",
                "total": "",
                "advertising cost": "",
                "currency": g.get("currency") or "",
                "_amount_type": g.get("type") or "",
                "_amount_desc": g.get("desc") or "",
                "_amount": g.get("amount") or "",
            })

    if not rows:
        rows = []
        for el in root.iter():
            if norm(el.tag) in ("amount", "totalamount"):
                ctx = el
                for _ in range(3):
                    ctx = ctx.getparent() if hasattr(ctx, "getparent") else None
                    if ctx is None:
                        break
                ctx = ctx or el
                rows.append({
                    "settlement id": find_text_deep(ctx, "settlement-id", "settlementid"),
                    "order id": find_text_deep(ctx, "order-id", "amazon-order-id", "orderid"),
                    "sku": find_text_deep(ctx, "sku", "seller-sku", "sellersku"),
                    "date/time": find_text_deep(ctx, "posted-date-time","posted-date","transaction-date","date"),
                    "type": find_text_deep(ctx, "transaction-type", "transactiontype", "type"),
                    "marketplace": find_text_deep(ctx, "marketplace-name", "marketplacename", "marketplace"),
                    "fulfilment": find_text_deep(ctx, "fulfillment-id", "fulfilment", "fulfillmentid", "fulfillment"),
                    "currency": find_text_deep(ctx, "currency", "currencycode", "amountcurrency"),
                    "_amount_type": find_text_deep(ctx, "amounttype", "chargetype", "pricetype"),
                    "_amount_desc": find_text_deep(ctx, "amountdescription", "chargedescription", "pricedescription"),
                    "_amount": (el.text or "").strip(),
                    "product sales": "", "product sales tax": "", "postage credits": "",
                    "shipping credits tax": "", "gift wrap credits": "", "giftwrap credits tax": "",
                    "promotional rebates": "", "promotional rebates tax": "", "marketplace withheld tax": "",
                    "selling fees": "", "fba fees": "", "other transaction fees": "", "other": "", "total": "",
                    "advertising cost": "",
                })
    return [r for r in rows if any(v for k, v in r.items() if k not in ("total",))]


def _aggregate_to_transactions(rows: list[dict], user_id: int | None) -> tuple[list[dict], set[str]]:
    buckets: dict[tuple, dict] = {}
    settlement_ids: set[str] = set()

    def key_of(r):
        return (
            (r.get("settlement id") or "").strip(),
            (r.get("order id") or "").strip(),
            (r.get("sku") or "").strip(),
            (r.get("type") or "").strip(),
            (r.get("fulfilment") or "").strip(),
            (r.get("date/time") or "").strip(),
        )

    def ensure_bucket(k, r):
        if k not in buckets:
            sid = (r.get("settlement id") or "").strip()
            if sid:
                settlement_ids.add(sid)
            buckets[k] = {
                "user_id": user_id,
                "settlement_id": r.get("settlement id"),
                "date_time": _parse_dt_any(r.get("date/time")),
                "transaction_type": r.get("type"),
                "order_id": r.get("order id"),
                "sku": r.get("sku"),
                "description": (r.get("description") or r.get("_amount_desc") or r.get("type") or r.get("sku") or None),
                "quantity": None,
                "marketplace": r.get("marketplace"),
                "fulfilment": r.get("fulfilment"),
                "order_city": (r.get("order city") or None),
                "order_state": (r.get("order state") or None),
                "order_postal": (r.get("order postal") or None),
                "tax_collection_model": (r.get("tax collection model") or None),
                "product_sales": Decimal("0"),
                "product_sales_tax": Decimal("0"),
                "postage_credits": Decimal("0"),
                "shipping_credits_tax": Decimal("0"),
                "gift_wrap_credits": Decimal("0"),
                "giftwrap_credits_tax": Decimal("0"),
                "promotional_rebates": Decimal("0"),
                "promotional_rebates_tax": Decimal("0"),
                "marketplace_withheld_tax": Decimal("0"),
                "selling_fees": Decimal("0"),
                "fba_fees": Decimal("0"),
                "other_transaction_fees": Decimal("0"),
                "other": Decimal("0"),
                "total": None,
                "advertising_cost": Decimal("0"),
                "platform_fees": Decimal("0"),
                "net_reimbursement": None,
                "currency": r.get("currency") or None,
                "synced_at": datetime.utcnow(),
                "_report_created": r.get("_report_created") if isinstance(r.get("_report_created"), datetime) else None,
            }
        b = buckets[k]
        for fld, src in (
            ("order_city", "order city"),
            ("order_state", "order state"),
            ("order_postal", "order postal"),
            ("tax_collection_model", "tax collection model"),
        ):
            if not b.get(fld) and r.get(src):
                b[fld] = r.get(src)
        if not b.get("currency") and r.get("currency"):
            b["currency"] = r.get("currency")
        if not b.get("description"):
            cand = r.get("description") or r.get("_amount_desc") or r.get("type") or r.get("sku")
            if cand:
                b["description"] = cand
        if not b.get("_report_created") and r.get("_report_created"):
            b["_report_created"] = r.get("_report_created")
        return b

    def add(b, field, amount):
        if amount is None:
            return
        b[field] = (b.get(field) or Decimal("0")) + amount

    for r in rows:
        k = key_of(r)
        b = ensure_bucket(k, r)
        q = r.get("quantity")
        try:
            q = int(q) if (q not in (None, "")) else None
        except Exception:
            q = None
        if q is not None:
            b["quantity"] = q

        # --- NEW: decide whether to trust bucket columns or the generic Amount group ---
        has_bucket_values = any(r.get(k1) for k1 in (
            "product sales","product sales tax","postage credits","shipping credits tax",
            "gift wrap credits","giftwrap credits tax","promotional rebates","promotional rebates tax",
            "marketplace withheld tax","selling fees","fba fees","other transaction fees","other"
        ))
        has_amount_group = bool(r.get("_amount") or r.get("_amount_type") or r.get("_amount_desc"))

        # If we have bucket values AND NO amount group, use the buckets.
        # If an amount group exists, fall through and classify by _amount_desc instead.
        if has_bucket_values and not has_amount_group:
            add(b, "product_sales", _to_decimal(r.get("product sales")))
            add(b, "product_sales_tax", _to_decimal(r.get("product sales tax")))
            add(b, "postage_credits", _to_decimal(r.get("postage credits")))
            add(b, "shipping_credits_tax", _to_decimal(r.get("shipping credits tax")))
            add(b, "gift_wrap_credits", _to_decimal(r.get("gift wrap credits")))
            add(b, "giftwrap_credits_tax", _to_decimal(r.get("giftwrap credits tax")))
            add(b, "promotional_rebates", _to_decimal(r.get("promotional rebates")))
            add(b, "promotional_rebates_tax", _to_decimal(r.get("promotional rebates tax")))
            add(b, "marketplace_withheld_tax", _to_decimal(r.get("marketplace withheld tax")))
            add(b, "selling_fees", _to_decimal(r.get("selling fees")))
            add(b, "fba_fees", _to_decimal(r.get("fba fees")))
            add(b, "other_transaction_fees", _to_decimal(r.get("other transaction fees")))
            add(b, "other", _to_decimal(r.get("other")))
            add(b, "advertising_cost", _to_decimal(r.get("advertising cost")))
            if b["total"] is None:
                b["total"] = _to_decimal(r.get("total"))
            continue

        # otherwise, classify using the generic Amount group
        amt = _to_decimal(r.get("_amount"))
        if amt is None:
            continue

        at = (r.get("_amount_type") or "").strip().lower()
        ad = (r.get("_amount_desc") or "").strip().lower()
        ad_nospace = ad.replace(" ", "")
        tt = (r.get("type") or "").strip().lower()
        # ... (rest of your is_ship / is_fee logic stays exactly the same)


        ship_kw = ("shipping", "delivery", "postage", "carrier", "shippingcharge", "shipping charge", "shippingchargeback", "shipping chargeback")
        selling_fee_kw = ("commission", "referral", "referral fee", "closing fee", "variable closing", "fixed closing", "per item fee", "per-item fee", "service fee", "high volume listing", "listing fee")
        other_fee_kw = ("lightning deal", "best deal", "7-day deal", "deal fee", "coupon redemption fee", "coupon redemptions", "refund administration fee", "refund admin fee", "safe-t", "safet", "saf-t", "low inventory level fee", "low-inventory fee")
        fba_kw = ("fba", "fulfillment", "fulfilment", "pick & pack", "weight based", "fulfillment per unit", "removal order fee", "prep fee", "labeling fee", "bagging fee")
        other_hard_kw = (
            "fba inventory reimbursement", "inventory reimbursement", "reimbursement",
            "customer return", "general adjustment", "subscription", "to account ending with", "to account ending with:",
            "disposal fee", "fba disposal fee", "storage fee", "inventory storage", "long term storage", "long-term storage",
            "aged inventory surcharge", "warehouse storage", "lightning deal", "best deal", "7-day deal", "deal fee",
            "coupon redemption fee", "coupon redemptions", "refund administration fee", "refund admin fee",
            "safe-t", "safet", "saf-t", "low inventory level fee", "low-inventory fee",
        )
        other_soft_kw = ("other", "goodwill", "misc", "rounding", "adjustment")

        is_service_total = (tt in ("servicefee", "service fees", "servicefees", "principal") or ad == "transactiontotalamount" or ad_nospace == "transactiontotalamount")
        is_tax = ("tax" in ad) or (at == "tax")
        is_marketplace_fac = ("marketplace facilitator" in ad) or ("facilitator" in ad)
        is_ship = any(k in ad for k in ship_kw)
        is_gw = ("gift" in ad and "wrap" in ad)
        is_promo = ("promo" in ad) or (at == "promotion")
        is_other_fee = any(k in ad for k in other_fee_kw)
        is_fee_like = (("fee" in at) or (at in ("itemfee", "fee", "servicefee")) or any(k in ad for k in selling_fee_kw))
        is_fba = any(k in ad for k in fba_kw)
        is_principal = ("principal" in ad) or (at in ("itemprice", "price", "productcharges"))
        is_other_hard = any(k in ad for k in other_hard_kw)
        is_other_soft = any(k in ad for k in other_soft_kw)

        if is_promo:
            if is_tax:
                add(b, "promotional_rebates_tax", amt)
            else:
                add(b, "promotional_rebates", amt)
        elif is_marketplace_fac or ("marketplace" in ad and "withheld" in ad and "tax" in ad):
            add(b, "marketplace_withheld_tax", amt)
            if not b.get("tax_collection_model"):
                b["tax_collection_model"] = "MarketplaceFacilitator"
        elif is_ship and is_tax:
            add(b, "shipping_credits_tax", amt)
        elif is_ship:
            add(b, "postage_credits", amt)
        elif is_gw and is_tax:
            add(b, "giftwrap_credits_tax", amt)
        elif is_gw:
            add(b, "gift_wrap_credits", amt)
        elif is_service_total:
            add(b, "other_transaction_fees", amt)
        elif is_other_fee:
            add(b, "other_transaction_fees", amt)
        elif is_fee_like and is_fba:
            add(b, "fba_fees", amt)
        elif is_fee_like:
            add(b, "selling_fees", amt)
        elif is_tax:
            add(b, "product_sales_tax", amt)
        elif is_principal:
            add(b, "product_sales", amt)
        elif is_other_hard:
            add(b, "other", amt)
        elif is_other_soft:
            add(b, "other", amt)
        else:
            add(b, "other", amt)

    mapped = []
    for b in buckets.values():
        # 1) compute total if missing
        if b["total"] is None:
            b["total"] = (
                b["product_sales"] + b["product_sales_tax"] + b["postage_credits"] +
                b["shipping_credits_tax"] + b["gift_wrap_credits"] + b["giftwrap_credits_tax"] +
                b["promotional_rebates"] + b["promotional_rebates_tax"] +
                b["marketplace_withheld_tax"] + b["selling_fees"] + b["fba_fees"] +
                b["other_transaction_fees"] + b["other"] + b["advertising_cost"]
            )

        desc_raw = (b.get("description") or "").strip()
        desc_norm = desc_raw.lower().replace(" ", "")

        tt_raw = (b.get("transaction_type") or "").strip().lower()

        # PLATFORM FEES (3 types)
        platform_keywords = {
            "storagerenewalbilling",  # "StorageRenewalBilling"
            "storagefee",             # "Storage Fee"
        }

        is_other_txn = (tt_raw == "other-transaction")
        is_platform_desc = desc_norm in platform_keywords

        # ALWAYS overwrite platform_fees so nothing stale remains
        if is_other_txn and is_platform_desc:
            b["platform_fees"] = b["total"]
        else:
            b["platform_fees"] = 0



        # ----------------------------------------------------
        # ADVERTISING COST
        # Add 3 rules:
        #   1) promotionfeespecial
        #   2) servicefee + transactiontotalamount
        #   3) AmazonFees + Base fee  <-- NEW
        # ----------------------------------------------------

        is_promotion_special = desc_norm == "promotionfeespecial"
        is_service_fee_tta = (tt_raw == "servicefee" and desc_norm == "transactiontotalamount")

        # NEW RULE: AmazonFees + Base fee
        is_amazonfees_basefee = (tt_raw == "amazonfees" and desc_norm == "basefee")

        if is_promotion_special or is_service_fee_tta or is_amazonfees_basefee:
            b["advertising_cost"] = b["total"]
        else:
            b["advertising_cost"] = None


        # 3) set net_reimbursement = total (unconditional)
        tt_empty = (b.get("transaction_type") or "").strip() == ""
        dt_empty = not b.get("date_time")
        b["net_reimbursement"] = b["total"] if (tt_empty and dt_empty) else None

        

        # 5) tidy zeros to None
        for k in (
            "product_sales","product_sales_tax","postage_credits","shipping_credits_tax",
            "gift_wrap_credits","giftwrap_credits_tax","promotional_rebates","promotional_rebates_tax",
            "marketplace_withheld_tax","selling_fees","fba_fees","other_transaction_fees","other",
            "advertising_cost","platform_fees"
        ):
            if b[k] == 0:
                b[k] = None

        mapped.append(b)

    settlement_ids.discard("")
    return mapped, settlement_ids


# @amazon_sales_api_bp.route('/upload', methods=['POST'])
# def upload():
#     df_in = None
#     if 'file' in request.files and request.files['file'].filename:
#         f = request.files['file']
#         fname = f.filename.lower()
#         try:
#             if fname.endswith(('.xlsx', '.xls')):
#                 df_in = pd.read_excel(f)
#             elif fname.endswith('.csv'):
#                 df_in = pd.read_csv(f)
#             elif fname.endswith('.json'):
#                 df_in = pd.read_json(f)
#             else:
#                 return jsonify({"error": "Unsupported file type. Use .xlsx, .xls, .csv, or .json"}), 400
#         except Exception as e:
#             return jsonify({"error": f"Failed to parse uploaded file: {str(e)}"}), 400
#     else:
#         payload = request.get_json(silent=True) or {}
#         rows = payload.get("rows") or payload.get("data")
#         if isinstance(rows, list):
#             try:
#                 df_in = pd.DataFrame(rows)
#             except Exception as e:
#                 return jsonify({"error": f"Could not build dataframe from 'rows': {str(e)}"}), 400

#     if df_in is None:
#         return jsonify({"error": "Provide a file (xlsx/csv/json) in form-data as 'file' or a JSON body with 'rows' (list of dicts)."}), 400

#     def _param(name, alt=None, required=False, caster=lambda x: x):
#         if name in request.form:
#             raw = request.form.get(name)
#         elif alt and alt in request.form:
#             raw = request.form.get(alt)
#         else:
#             payload = request.get_json(silent=True) or {}
#             raw = payload.get(name)
#             if raw is None and alt:
#                 raw = payload.get(alt)
#         if required and (raw is None or raw == ""):
#             raise KeyError(name)
#         if raw is None:
#             return None
#         try:
#             return caster(raw)
#         except Exception:
#             raise ValueError(name)

#     try:
#         user_id = _param("user_id", required=True)
#         ui_country = _param("ui_country", alt="country", required=True, caster=lambda s: str(s).strip())
#         raw_month = _param("month_num", alt="month")
#         if raw_month is None:
#             month_num = datetime.utcnow().month
#         else:
#             sm = str(raw_month).strip()
#             if sm.isdigit():
#                 month_num = int(sm)
#             else:
#                 month_num = _month_to_num(sm)
#         ui_year = _param("ui_year", alt="year", caster=int) or datetime.utcnow().year
#         if not (1 <= int(month_num) <= 12):
#             return jsonify({"error": "month_num must be between 1 and 12"}), 400
#     except KeyError as e:
#         return jsonify({"error": f"Missing required field '{e.args[0]}'"}), 400
#     except ValueError as e:
#         return jsonify({"error": f"Invalid value for '{e.args[0]}'"}), 400

#     result = run_upload_pipeline_from_df(
#         df_raw=df_in,
#         user_id=user_id,
#         country=ui_country,
#         month_num=int(month_num),
#         year=int(ui_year),
#         db_url=db_url,
#         db_url_aux=db_url1,
#     )
#     if not result.get("success"):
#         return jsonify(result), 400
#     return jsonify(result), 200

def _month_to_num(mname: str) -> int:
    m = mname.strip().lower()
    for i in range(1, 13):
        if calendar.month_name[i].lower() == m or calendar.month_abbr[i].lower() == m:
            return i
    raise ValueError("Invalid month")

# =========================
# Backfill-aware helpers
# =========================
def _list_done_reports_window(rt: str, start: datetime | None, end: datetime | None, page_size: int = 100) -> list[dict]:
    params = {"reportTypes": [rt], "pageSize": page_size, "processingStatuses": ["DONE"]}
    if start:
        params["createdSince"] = _iso(start)
    if end:
        params["createdUntil"] = _iso(end)
    res = amazon_client.make_api_call("/reports/2021-06-30/reports", "GET", params)
    if not res or "error" in res:
        return []
    payload = res.get("payload") or res
    reports = payload.get("reports") or []
    reports.sort(key=lambda r: r.get("createdTime") or "", reverse=True)
    return reports

def _list_done_reports_backfill_deep(
    rt: str,
    month_start: datetime,
    month_end: datetime,
    stop_searching_after: datetime,
    max_pages: int = 300,
    page_size: int = 100
) -> list[dict]:
    out: list[dict] = []
    params = {"reportTypes": [rt], "pageSize": page_size, "processingStatuses": ["DONE"]}
    token = None
    for _ in range(max_pages):
        call_params = dict(params)
        if token:
            call_params["nextToken"] = token
        res = amazon_client.make_api_call("/reports/2021-06-30/reports", "GET", call_params)
        if not res or "error" in res:
            break
        payload = res.get("payload") or res
        batch = payload.get("reports") or []
        if not batch:
            break
        out.extend(batch)
        token = payload.get("nextToken")
        if not token:
            break
    out.sort(key=lambda r: r.get("createdTime") or "", reverse=True)
    return out

MARKETPLACE_NAMES = {
    "ATVPDKIKX0DER": "Amazon.com",     # US
    "A2EUQ1WTGCTBG2": "Amazon.ca",     # Canada
    "A1F83G8C2ARO7P": "Amazon.co.uk",  # UK
}

# 1month, 3 month api
@amazon_sales_api_bp.route("/amazon_api/settlements", methods=["GET"])
def settlements_route_single():
    # ---- optional auth (for run_upload) ----
    auth_header = request.headers.get("Authorization")
    user_id = None
    if auth_header and auth_header.startswith("Bearer "):
        tok = auth_header.split(" ")[1]
        try:
            payload = jwt.decode(tok, SECRET_KEY, algorithms=["HS256"])
            user_id = payload.get("user_id")
        except jwt.ExpiredSignatureError:
            return jsonify({"error": "Token has expired"}), 401
        except jwt.InvalidTokenError:
            return jsonify({"error": "Invalid token"}), 401

    # ---- params ----
    _apply_region_and_marketplace_from_request()
    mp = request.args.get("marketplace_id", amazon_client.marketplace_id)

    # marketplace controls
    target_marketplace_name = MARKETPLACE_NAMES.get(mp, "Amazon.com")
    strict_marketplace = _get_bool(request.args.get("strict_marketplace"), False)
    target_norm = _norm_marketplace(target_marketplace_name)
    _target_aliases = {
        target_norm,
        _norm_marketplace("Amazon.com Services, LLC"),
        _norm_marketplace("Amazon.com, Inc."),
    }
    def _marketplace_match(marketplace_raw: str | None) -> bool:
        norm = _norm_marketplace(marketplace_raw)
        if strict_marketplace:
            return norm in _target_aliases
        if norm in ("", "unknown"):
            return True
        return norm in _target_aliases

    primary_type = request.args.get("type") or "GET_V2_SETTLEMENT_REPORT_DATA_FLAT_FILE_V2"
    fallback_type = "GET_V2_SETTLEMENT_REPORT_DATA_FLAT_FILE"
    out_format = (request.args.get("format") or "json").lower()

    store_in_db = (request.args.get("store_in_db", "true").lower() != "false")
    run_upload = (request.args.get("run_upload_pipeline", "false").lower() == "true")
    ui_country = (request.args.get("country") or "").lower()
    ui_year = (request.args.get("year") or "").strip()
    month_raw = request.args.get("month")
    backfill_pages = int(request.args.get("backfill_pages", "300"))

    month_filter = None
    if month_raw:
        try:
            y, m = _parse_month_input(month_raw)
            month_filter = f"{y}-{str(m).zfill(2)}"
        except ValueError:
            return jsonify({"success": False, "error": "Invalid month. Try '2025-07', '2025-July', 'July', or '07'."}), 400

    limit_param = (request.args.get("limit") or "50").strip().lower()
    json_limit = None if limit_param == "all" else max(0, int(limit_param) if limit_param.isdigit() else 50)

    now = datetime.utcnow()
    end_dt = now
    start_dt = end_dt - timedelta(days=90)
    start_iso = _iso(start_dt); end_iso = _iso(end_dt)

    # ---- choose reports ----
    reports_to_fetch: list[dict] = []
    if month_filter:
        y_sel, m_sel = map(int, month_filter.split("-"))
        month_start = datetime(y_sel, m_sel, 1)
        month_end = (month_start.replace(day=28) + timedelta(days=4)).replace(day=1)
        days_ago = (now - month_start).days
        if days_ago <= 120:
            window_start = month_start - timedelta(days=30)
            window_end = min(month_end + timedelta(days=30), now)
            if (now - window_start).days > 89:
                window_start = now - timedelta(days=89)
            if window_start >= window_end:
                window_start = window_end - timedelta(seconds=1)
            reports_to_fetch = _list_done_reports_window(primary_type, window_start, window_end)
            if not reports_to_fetch:
                reports_to_fetch = _list_done_reports_window(fallback_type, window_start, window_end)
        else:
            stop_searching_after = month_end + timedelta(days=365)
            types_to_pull = [
                request.args.get("type") or "GET_V2_SETTLEMENT_REPORT_DATA_FLAT_FILE_V2",
                "GET_V2_SETTLEMENT_REPORT_DATA_FLAT_FILE",
                "GET_V2_SETTLEMENT_REPORT_DATA_XML",
            ]
            by_id = {}
            for rt in types_to_pull:
                cands = _list_done_reports_backfill_deep(
                    rt, month_start, month_end, stop_searching_after,
                    max_pages=backfill_pages, page_size=100
                )
                for r in cands:
                    rid = r.get("reportId")
                    if rid and rid not in by_id:
                        by_id[rid] = r
            reports_to_fetch = list(by_id.values())
            reports_to_fetch.sort(key=lambda r: r.get("createdTime") or "", reverse=True)

    if not reports_to_fetch:
        reuse = _list_done_reports_window(primary_type, now - timedelta(days=60), None, page_size=20)
        if reuse:
            reports_to_fetch = [reuse[0]]
        else:
            body = {"reportType": primary_type} if _is_settlement_type(primary_type) else {
                "reportType": primary_type, "marketplaceIds": [mp], "dataStartTime": start_iso, "dataEndTime": end_iso
            }
            r1 = amazon_client.make_api_call("/reports/2021-06-30/reports", "POST", data=body, max_retries=5)
            if r1 and "error" not in r1:
                rid = r1.get("reportId") or (r1.get("payload") or {}).get("reportId")
                if rid:
                    reports_to_fetch = [{"reportId": rid}]
            elif _not_allowed_now(r1):
                time.sleep(5)
                reuse2 = _list_done_reports_window(primary_type, now - timedelta(days=60), None, page_size=20)
                if reuse2:
                    reports_to_fetch = [reuse2[0]]

    if not reports_to_fetch:
        return jsonify({"success": False, "step": "list/create", "message": "No reports found"}), 200

    # ---- download / decrypt / unzip / parse  (STREAMING with early-stop) ----
    raw_rows_all = []
    reports_date_coverage = []
    max_reports = int(request.args.get("max_reports", "1000"))
    month_selected = month_filter is not None
    if month_selected:
        start_m, end_m = _month_bounds(month_filter)
    have_any_month_rows = False
    have_passed_month = False
    processed_reports = 0

    def _fetch_plain_blob(doc_meta):
        url = doc_meta.get("url")
        enc = doc_meta.get("encryptionDetails")
        compression = (doc_meta.get("compressionAlgorithm") or "").upper()
        if not url:
            return None
        blob = None
        for i in range(3):
            try:
                r = requests.get(url, timeout=120)
                if r.status_code in (200, 206):
                    blob = r.content
                    break
            except requests.RequestException:
                pass
            time.sleep(2 ** i)
        if blob is None:
            return None
        if enc:
            from Crypto.Cipher import AES
            key = base64.b64decode(enc["key"])
            iv = base64.b64decode(enc["initializationVector"])
            cipher = AES.new(key, AES.MODE_CBC, iv)
            data = cipher.decrypt(blob)
            pad = data[-1] if isinstance(data[-1], int) else ord(data[-1])
            data = data[:-pad]
        else:
            data = blob
        return gzip.decompress(data) if compression == "GZIP" else data

    for rep in reports_to_fetch:
        if processed_reports >= max_reports:
            break
        rid = rep.get("reportId")
        doc_id = rep.get("reportDocumentId")
        if not doc_id:
            meta = amazon_client.make_api_call(f"/reports/2021-06-30/reports/{rid}", "GET")
            if not meta or "error" in meta:
                continue
            doc_id = (meta.get("payload") or meta).get("reportDocumentId")
            if not doc_id:
                continue
        doc = amazon_client.make_api_call(f"/reports/2021-06-30/documents/{doc_id}", "GET")
        if not doc or "error" in doc:
            continue
        d = doc.get("payload") or doc
        plain = _fetch_plain_blob(d)
        if plain is None:
            continue

        snippet = plain.lstrip()[:20]
        rows = _parse_settlement_xml(plain) if (snippet.startswith(b"<?xml") or snippet.startswith(b"<")) else _parse_settlement_tsv(plain)

        rep_created = (rep.get("createdTime") or "").rstrip("Z")
        rep_created_dt = _parse_dt_any(rep_created)

        min_dt = max_dt = None
        dts = []
        if rows:
            dts = [_parse_dt_any(r.get("date/time")) for r in rows]
            dts = [x for x in dts if x]
            if dts:
                min_dt, max_dt = min(dts), max(dts)

        reports_date_coverage.append({
            "report_id": rid,
            "report_created": rep_created_dt,
            "transaction_date_min": min_dt,
            "transaction_date_max": max_dt,
            "row_count": len(rows)
        })

        # Mark each raw row with the report's creation timestamp for fallback month filtering
        for rr in rows:
            rr["_report_created"] = rep_created_dt
        raw_rows_all.extend(rows)

        processed_reports += 1

        if month_selected and dts:
            in_month = any((start_m <= dt < end_m) for dt in dts)
            if in_month:
                have_any_month_rows = True
            if have_any_month_rows and max_dt and max_dt < start_m:
                have_passed_month = True
        if month_selected and have_any_month_rows and have_passed_month:
            break

    # ---- aggregate ----
    aggregated, _sid_set = _aggregate_to_transactions(raw_rows_all, user_id)

    # diagnostics
    marketplace_counts = {}
    for r in aggregated:
        mp_name = (r.get("marketplace") or "Unknown").strip()
        marketplace_counts[mp_name] = marketplace_counts.get(mp_name, 0) + 1

    # ---- filtering (marketplace + month with fallback to report_created when date/time is missing) ----
    filtered = aggregated
    if month_filter:
        start_m, end_m = _month_bounds(month_filter)

        def _in_month_bucket(b: dict) -> bool:
            # marketplace match first
            if not _marketplace_match((b.get("marketplace") or "").strip()):
                return False

            dt = b.get("date_time")  # already parsed to naive UTC datetime or None
            rc = b.get("_report_created")  # datetime | None

            # 1) Prefer real per-line timestamp when present
            if dt is not None:
                return start_m <= dt < end_m

            # 2) Fallback: if no per-line timestamp, use the report's created timestamp
            if isinstance(rc, datetime):
                return start_m <= rc < end_m

            # Otherwise, we don't know—exclude
            return False

        filtered = [b for b in aggregated if _in_month_bucket(b)]

        # Extra safety: if filtering ended up empty but raw rows have coverage in the month by fallback,
        # re-aggregate only those raw rows so nothing is lost for the requested month.
        if not filtered:
            def _raw_in_month(rr: dict) -> bool:
                dt = _parse_dt_any(rr.get("date/time"))
                rc = rr.get("_report_created")
                mp_ok = _marketplace_match((rr.get("marketplace") or "").strip())
                if not mp_ok:
                    return False
                # Accept rows whose per-line dt is in month, or if dt missing then report_created is in month
                if dt is not None:
                    return start_m <= dt < end_m
                return isinstance(rc, datetime) and (start_m <= rc < end_m)

            raw_in_month = [rr for rr in raw_rows_all if _raw_in_month(rr)]
            if raw_in_month:
                filtered, _ = _aggregate_to_transactions(raw_in_month, user_id)
    else:
        filtered = [b for b in aggregated if _marketplace_match((b.get("marketplace") or "").strip())]

    # ---- optional date range filter: ?start_date=YYYY-MM-DD&end_date=YYYY-MM-DD ----
    start_raw = request.args.get("start_date") or request.args.get("from")
    end_raw = request.args.get("end_date") or request.args.get("to")

    if start_raw:
        try:
            start_range = datetime.strptime(start_raw.strip(), "%Y-%m-%d")
        except ValueError:
            return jsonify({
                "success": False,
                "error": "Invalid start_date. Use format YYYY-MM-DD, e.g. 2025-11-01."
            }), 400

        if end_raw:
            try:
                # end is exclusive, so add 1 day
                end_range = datetime.strptime(end_raw.strip(), "%Y-%m-%d") + timedelta(days=1)
            except ValueError:
                return jsonify({
                    "success": False,
                    "error": "Invalid end_date. Use format YYYY-MM-DD, e.g. 2025-11-19."
                }), 400
        else:
            # default end = tomorrow (exclusive), so range is [start_date, today]
            today = datetime.utcnow().date()
            end_range = datetime.combine(today + timedelta(days=1), datetime.min.time())

        def _in_date_range(b: dict) -> bool:
            dt = b.get("date_time")        # real transaction timestamp
            rc = b.get("_report_created")  # fallback to report created time

            if isinstance(dt, datetime):
                return start_range <= dt < end_range
            if isinstance(rc, datetime):
                return start_range <= rc < end_range
            return False

        filtered = [b for b in filtered if _in_date_range(b)]


    # ---- optional upload pipeline ----
    run_upload_pipeline = run_upload
    if run_upload_pipeline:
        if not user_id:
            return jsonify({"success": False, "error": "Auth required"}), 401
        if not month_filter or not ui_country or not ui_year:
            return jsonify({"success": False, "error": "Provide month, country, year"}), 400

        def row_to_dict(r):
            def ts(x): return x.strftime("%Y-%m-%dT%H:%M:%SZ") if x else None
            return {
                "date/time": ts(r["date_time"]),
                "settlement_id": r["settlement_id"],
                "type": r["transaction_type"],
                "order_id": r["order_id"],
                "sku": r["sku"],
                "description": r["description"],
                "quantity": r["quantity"],
                "marketplace": r["marketplace"],
                "fulfilment": r["fulfilment"],
                "order_city": r["order_city"],
                "order_state": r["order_state"],
                "order_postal": r["order_postal"],
                "tax_collection_model": r["tax_collection_model"],
                "product_sales": r["product_sales"],
                "product_sales_tax": r["product_sales_tax"],
                "postage_credits": r["postage_credits"],
                "shipping_credits_tax": r["shipping_credits_tax"],
                "gift_wrap_credits": r["gift_wrap_credits"],
                "giftwrap_credits_tax": r["giftwrap_credits_tax"],
                "promotional_rebates": r["promotional_rebates"],
                "promotional_rebates_tax": r["promotional_rebates_tax"],
                "marketplace_withheld_tax": r["marketplace_withheld_tax"],
                "selling_fees": r["selling_fees"],
                "fba_fees": r["fba_fees"],
                "other_transaction_fees": r["other_transaction_fees"],
                "other": r["other"],
                "total": r["total"],
                "currency": r["currency"],
                "advertising_cost": r["advertising_cost"],
                "net_reimbursement": r["net_reimbursement"],
                "platform_fees": r["platform_fees"],
            }

        df_in = pd.DataFrame([row_to_dict(r) for r in filtered])
        month_num = int(month_filter.split("-")[1])
        result = run_upload_pipeline_from_df(
            df_raw=df_in, user_id=user_id, country=ui_country,
            month_num=month_num, year=str(ui_year),
            db_url=os.getenv("DATABASE_URL"), db_url_aux=os.getenv("DATABASE_URL1"),
        )
        return jsonify(result), (200 if result.get("success") else 400)

    # ---- store ----
    inserted = 0; deleted = 0
    rows_to_store = filtered if month_filter else aggregated
    if (store_in_db and rows_to_store) and not run_upload:
        sid_set_to_store = {(r.get("settlement_id") or "").strip() for r in rows_to_store}
        sid_set_to_store.discard("")
        if sid_set_to_store:
            deleted = SettlementTransaction.query.filter(
                SettlementTransaction.settlement_id.in_(list(sid_set_to_store))
            ).delete(synchronize_session=False)
            db.session.commit()
        db.session.bulk_insert_mappings(SettlementTransaction, rows_to_store)
        db.session.commit()
        inserted = len(rows_to_store)

        


    # ---- CSV / JSON ----
    if out_format == "csv":
        fieldnames = [
            "date_time","settlement_id","transaction_type","order_id","sku","description","quantity",
            "marketplace","fulfilment","order_city","order_state","order_postal","tax_collection_model",
            "product_sales","product_sales_tax","postage_credits","shipping_credits_tax",
            "gift_wrap_credits","giftwrap_credits_tax","promotional_rebates","promotional_rebates_tax",
            "marketplace_withheld_tax","selling_fees","fba_fees","other_transaction_fees","other","total",
            "currency","advertising_cost","platform_fees","net_reimbursement"
        ]
        def to_row(r):
            def S(x): return None if x is None else str(x)
            return {
                "date_time": r["date_time"].strftime("%Y-%m-%dT%H:%M:%SZ") if r["date_time"] else None,
                "settlement_id": r["settlement_id"],
                "transaction_type": r["transaction_type"],
                "order_id": r["order_id"],
                "sku": r["sku"],
                "description": r["description"],
                "quantity": r["quantity"],
                "marketplace": r["marketplace"],
                "fulfilment": r["fulfilment"],
                "order_city": r["order_city"],
                "order_state": r["order_state"],
                "order_postal": r["order_postal"],
                "tax_collection_model": r["tax_collection_model"],
                "product_sales": S(r["product_sales"]),
                "product_sales_tax": S(r["product_sales_tax"]),
                "postage_credits": S(r["postage_credits"]),
                "shipping_credits_tax": S(r["shipping_credits_tax"]),
                "gift_wrap_credits": S(r["gift_wrap_credits"]),
                "giftwrap_credits_tax": S(r["giftwrap_credits_tax"]),
                "promotional_rebates": S(r["promotional_rebates"]),
                "promotional_rebates_tax": S(r["promotional_rebates_tax"]),
                "marketplace_withheld_tax": S(r["marketplace_withheld_tax"]),
                "selling_fees": S(r["selling_fees"]),
                "fba_fees": S(r["fba_fees"]),
                "other_transaction_fees": S(r["other_transaction_fees"]),
                "other": S(r["other"]),
                "total": S(r["total"]),
                "currency": r["currency"],
                "advertising_cost": S(r["advertising_cost"]),
                "platform_fees": S(r["platform_fees"]),
                "net_reimbursement": S(r["net_reimbursement"]),
            }
        def generate_csv(data):
            out_io = io.StringIO()
            w = csv.DictWriter(out_io, fieldnames=fieldnames)
            w.writeheader(); yield out_io.getvalue(); out_io.seek(0); out_io.truncate(0)
            for r in data:
                w.writerow(to_row(r)); yield out_io.getvalue(); out_io.seek(0); out_io.truncate(0)
        fname = "settlement_transactions.csv" if not month_filter else f"settlement_transactions_{month_filter}.csv"
        return Response(generate_csv(filtered), mimetype="text/csv",
                        headers={"Content-Disposition": f"attachment; filename={fname}"})

    def D(x):
        return x if isinstance(x, Decimal) else (Decimal(str(x)) if x not in (None, "") else Decimal("0"))

    totals = {
        "postage_credits": str(sum(D(r.get("postage_credits")) for r in filtered)),
        "selling_fees": str(sum(D(r.get("selling_fees")) for r in filtered)),
        "fba_fees": str(sum(D(r.get("fba_fees")) for r in filtered)),
        "product_sales": str(sum(D(r.get("product_sales")) for r in filtered)),
        "product_sales_tax": str(sum(D(r.get("product_sales_tax")) for r in filtered)),
        "promotional_rebates": str(sum(D(r.get("promotional_rebates")) for r in filtered)),
        "marketplace_withheld_tax": str(sum(D(r.get("marketplace_withheld_tax")) for r in filtered)),
        "other_transaction_fees": str(sum(D(r.get("other_transaction_fees")) for r in filtered)),
        "advertising_cost": str(sum(D(r.get("advertising_cost")) for r in filtered)),
        "platform_fees": str(sum(D(r.get("platform_fees")) for r in filtered)),
        "net_reimbursement": str(sum(D(r.get("net_reimbursement")) for r in filtered)),
    }

    preview = filtered if json_limit is None else filtered[:json_limit]
    def jsonify_row(r):
        def S(x): return None if x is None else str(x)
        return {
            "date/time": r["date_time"].strftime("%Y-%m-%dT%H:%M:%SZ") if r["date_time"] else None,
            "settlement id": r["settlement_id"],
            "type": r["transaction_type"],
            "order id": r["order_id"],
            "sku": r["sku"],
            "description": r["description"],
            "quantity": r["quantity"],
            "marketplace": r["marketplace"],
            "fulfilment": r["fulfilment"],
            "order city": r["order_city"],
            "order state": r["order_state"],
            "order postal": r["order_postal"],
            "tax collection model": r["tax_collection_model"],
            "product sales": S(r["product_sales"]),
            "product sales tax": S(r["product_sales_tax"]),
            "postage credits": S(r["postage_credits"]),
            "shipping credits tax": S(r["shipping_credits_tax"]),
            "gift wrap credits": S(r["gift_wrap_credits"]),
            "giftwrap credits tax": S(r["giftwrap_credits_tax"]),
            "promotional rebates": S(r["promotional_rebates"]),
            "promotional rebates tax": S(r["promotional_rebates_tax"]),
            "marketplace withheld tax": S(r["marketplace_withheld_tax"]),
            "selling fees": S(r["selling_fees"]),
            "fba fees": S(r["fba_fees"]),
            "other transaction fees": S(r["other_transaction_fees"]),
            "other": S(r["other"]),
            "total": S(r["total"]),
            "currency": r["currency"],
            "advertising_cost": S(r["advertising_cost"]),
            "platform_fees": S(r["platform_fees"]),
            "net_reimbursement": S(r["net_reimbursement"]),
        }

    return jsonify({
        "success": True,
        "marketplace_id": mp,
        "marketplace_name": target_marketplace_name,
        "month": month_filter,
        "raw_lines": len(raw_rows_all),
        "aggregated_rows": len(aggregated),
        "filtered_rows": len(filtered),
        "returned_rows": len(preview),
        "reports_processed": processed_reports,
        "reports_date_coverage": reports_date_coverage[:10],
        "stored": {"deleted": int(deleted), "inserted": int(inserted)} if store_in_db else {"skipped": True},
        "totals": totals,
        "items": [jsonify_row(r) for r in preview],
    }), 200



# ===============================
# Finances-only settlements route (GET-only, full-month coverage)
# ===============================



REPORT_TYPES_SETTLEMENT = (
    "GET_V2_SETTLEMENT_REPORT_DATA_FLAT_FILE_V2",
    "GET_V2_SETTLEMENT_REPORT_DATA_FLAT_FILE",
    "GET_V2_SETTLEMENT_REPORT_DATA_XML",
)

# Map SP-API event buckets + descriptions -> settlement-style types
_FBA_FEE_HINTS: set[str] = {
    "fbainventorystoragefee", "fbalongtermstoragefee", "monthlystoragefee",
    "fba long-term storage fee", "fba inventory storage fee",
    "afnperunitsurcharge", "lowinventorylevel", "low inventory level",
    "inboundtransportation", "inbound transportation",
    "inventory placement", "inventory storage",
}

_num_clean_re = re.compile(r"[,\s$£₹€%]")


# ---------------------------------------------------------------------------
# Settlement enrichment helpers
# ---------------------------------------------------------------------------

def _is_settlement_type(rt: str) -> bool:
    """Return True if a report type string is one of the supported V2 settlement types."""
    return rt in REPORT_TYPES_SETTLEMENT


def _get_report_document(report_id: str) -> dict | None:
    """
    Resolve a report document for a given report_id using SP-API Reports 2021-06-30.
    Returns the /documents/{id} response dict or None.
    """
    r = amazon_client.make_api_call(f"/reports/2021-06-30/reports/{report_id}", "GET")
    doc_id = ((r or {}).get("payload") or {}).get("reportDocumentId")
    if not doc_id:
        return None
    return amazon_client.make_api_call(f"/reports/2021-06-30/documents/{doc_id}", "GET")


def _download_report_bytes(doc: dict) -> bytes:
    """
    Download the report document bytes; transparently gunzip if needed.
    Returns raw bytes (possibly decompressed).
    """
    payload = (doc or {}).get("payload") or {}
    url = payload.get("url")
    compression = (payload.get("compressionAlgorithm") or "").upper()
    if not url:
        return b""
    with urllib.request.urlopen(url) as resp:
        raw = resp.read()
    if compression == "GZIP":
        try:
            return gzip.decompress(raw)
        except Exception:
            # Fall back to raw if decompression fails
            pass
    return raw


def _parse_settlement_csv_to_df(content: bytes) -> pd.DataFrame:
    """
    Parse Amazon V2 Settlement flat file (TAB- or CSV-delimited) to a normalized DataFrame.
    Only the columns needed for enrichment are returned (others are dropped).
    """
    from io import StringIO

    text = content.decode("utf-8-sig", errors="ignore")
    first_line = text.splitlines()[0] if text else ""
    sep = "\t" if ("\t" in first_line and first_line.count("\t") >= 5) else ","

    df = pd.read_csv(StringIO(text), sep=sep, dtype=str, keep_default_na=False)

    def norm(h: str) -> str:
        return str(h or "").strip().lower().replace(" ", "_").replace("-", "_")

    df.columns = [norm(c) for c in df.columns]

    # Known variants -> unified target names
    variants = {
        "settlement_id": "settlement_id",
        "amazon_settlement_id": "settlement_id",

        "order_id": "order_id",
        "amazon_order_id": "order_id",
        "orderid": "order_id",

        "sku": "sku",
        "seller_sku": "sku",

        "marketplace": "marketplace",
        "marketplace_name": "marketplace",

        "fulfilment": "fulfilment",
        "fulfillment": "fulfilment",

        "order_city": "order_city",
        "ship_city": "order_city",

        "order_state": "order_state",
        "ship_state": "order_state",
        "ship_state_or_region": "order_state",

        "order_postal": "order_postal",
        "ship_postal_code": "order_postal",
        "ship_postcode": "order_postal",
        "ship_zip": "order_postal",
        "buyer_postal_code": "order_postal",

        "tax_collection_model": "tax_collection_model",
        "tax_collection_model_": "tax_collection_model",

        # Marketplace Withheld Tax (some exports split/rename)
        "marketplace_withheld_tax": "marketplace_withheld_tax",
        "marketplace_withheld_tax_total": "marketplace_withheld_tax",
        "marketplace_withheld_tax_amount": "marketplace_withheld_tax",
        "mwh_tax": "marketplace_withheld_tax",
    }

    target_cols = [
        "settlement_id", "order_id", "sku", "marketplace", "fulfilment",
        "order_city", "order_state", "order_postal", "tax_collection_model",
        "marketplace_withheld_tax",
    ]
    out = {c: [] for c in target_cols}

    # Present columns -> unified target names
    colmap = {col: variants[col] for col in df.columns if col in variants}

    # If there are split MWH tax columns, combine them (pick first non-empty as before)
    mwh_cols = [c for c in df.columns if c.startswith("marketplace_withheld_tax")]

    def get_mwh(row: pd.Series) -> str:
        vals: list[str] = []
        for c in mwh_cols:
            v = (row.get(c) or "").strip()
            if v:
                vals.append(v)
        return vals[0] if vals else ""

    for _, row in df.iterrows():
        rec = {k: "" for k in target_cols}
        for src, dst in colmap.items():
            if dst in rec:
                rec[dst] = (row.get(src) or "").strip()
        if "marketplace_withheld_tax" in target_cols and mwh_cols and not rec["marketplace_withheld_tax"]:
            rec["marketplace_withheld_tax"] = get_mwh(row)
        for k in target_cols:
            out[k].append(rec[k])

    return pd.DataFrame(out)


def _build_settlement_lookup(df: pd.DataFrame) -> dict[tuple[str, str | None], dict]:
    """
    Build order/SKU -> payload lookup.
    Keys:
      (order_id, sku) for item-specific lookups
      (order_id, None) as a fallback when sku not present
    """
    lut: dict[tuple[str, str | None], dict] = {}
    for _, row in df.iterrows():
        oid = str(row.get("order_id") or "").strip()
        sku = str(row.get("sku") or "").strip()
        if not oid:
            continue
        payload = {k: row.get(k) for k in df.columns}
        if sku:
            lut[(oid, sku)] = payload
        lut.setdefault((oid, None), payload)
    return lut


def _enrich_from_settlement_report(
    rows: list[dict],
    start_dt: datetime,
    end_dt: datetime,
    log: logging.Logger | None = None,
 ) -> None:
    """
    Mutates `rows` in place by filling empty fields from the best V2 settlement report
    whose data window overlaps [start_dt, end_dt).
    """
    try:
        reports = _list_settlement_reports_recent(days_back=180)
        if not reports:
            if log:
                log.info("No recent DONE settlement reports; enrichment skipped.")
            return

        chosen = _choose_report_for_window(reports, start_dt, end_dt)
        if not chosen:
            if log:
                log.info("No settlement report overlaps requested window; enrichment skipped.")
            return

        if log:
            log.info(
                "Using settlement report %s | dataStart=%s dataEnd=%s created=%s",
                chosen.get("reportId"),
                chosen.get("dataStartTime"),
                chosen.get("dataEndTime"),
                chosen.get("createdTime"),
            )

        doc = _get_report_document(chosen.get("reportId"))
        if not doc:
            if log:
                log.warning("Settlement report document missing; enrichment skipped.")
            return

        csv_bytes = _download_report_bytes(doc)
        df = _parse_settlement_csv_to_df(csv_bytes)
        if df.empty:
            if log:
                log.warning("Parsed report but required columns missing (delimiter/headers).")
            return

        lut = _build_settlement_lookup(df)

        targets = [
            "settlement_id", "marketplace", "fulfilment",
            "order_city", "order_state", "order_postal",
            "tax_collection_model", "marketplace_withheld_tax",
        ]

        def empty(v: Any) -> bool:
            return v is None or (isinstance(v, str) and v.strip() == "")

        matched = filled = 0
        for r in rows:
            oid = str(r.get("order_id") or "").strip()
            if not oid:
                continue
            sku = str(r.get("sku") or "").strip()
            src = lut.get((oid, sku)) or lut.get((oid, None))
            if not src:
                continue
            matched += 1
            changed = False

            for key in targets:
                cur_val = r.get(key)
                is_empty = empty(cur_val)

                # 👇 special handling: allow 0 / "0" to be overwritten for marketplace_withheld_tax
                if key == "marketplace_withheld_tax":
                    # make sure Decimal is imported at top of file: from decimal import Decimal
                    from decimal import Decimal as _D

                    if isinstance(cur_val, (int, float, _D)) and cur_val == 0:
                        is_empty = True
                    elif isinstance(cur_val, str) and cur_val.strip() in ("0", "0.0"):
                        is_empty = True

                if key not in r or is_empty:
                    val = src.get(key, "")
                    if key == "marketplace_withheld_tax" and val not in ("", None):
                        try:
                            val = str(_D(str(val)))
                        except Exception:
                            val = str(val)
                    if val != "":
                        r[key] = val
                        changed = True

            if changed:
                filled += 1


        if log:
            log.info(
                "Settlement LUT built: rows=%d, unique keys=%d",
                len(df),
                len(lut),
            )

        ...
        if log:
            log.info(
                "Enrichment complete: matched=%d, filled=%d; parsed columns=%s",
                matched, filled, ", ".join(df.columns.tolist()[:12]),
            )

    except Exception as e:
        if log:
            log.exception("Settlement enrichment failed: %s", e)


# ---------------------------------------------------------------------------
# Money & mapping helpers
# ---------------------------------------------------------------------------

def _money_to_tuple(m: dict | None) -> tuple[Decimal, str | None]:
    """Single Money dict -> (Decimal amount, currency or None)."""
    try:
        amt = Decimal(str((m or {}).get("CurrencyAmount") or "0"))
    except Exception:
        amt = Decimal("0")
    cur = (m or {}).get("CurrencyCode")
    return amt, cur


def _get_item_qty(item: dict | None) -> int:
    """Best-effort quantity from a shipment/refund item; never None."""
    for k in ("QuantityShipped", "QuantityOrdered", "Quantity"):
        v = (item or {}).get(k)
        if v is not None:
            try:
                return int(v)
            except Exception:
                try:
                    return int(float(v))
                except Exception:
                    pass
    return 0  # neutral default for non-item rows or missing values


def _fin_event_to_rows(evt: dict, user_id: Any) -> list[dict]:
    """
    Convert Finances 'event container' (we pass one event at a time) into our raw row list.
    Signs:
      - Charges/principal/taxes/shipping -> +ve
      - Promotions/fees/ads -> -ve
      - Adjustments -> sign as-is (Amazon can credit/debit)
    """
    rows: list[dict] = []
    dt = _parse_dt_any(evt.get("PostedDate"))

    # 1) Shipments
    if "ShipmentEventList" in evt:
        for sh in (evt.get("ShipmentEventList") or []):
            order_id = sh.get("AmazonOrderId")
            posted = _parse_dt_any(sh.get("PostedDate")) or dt

            # Shipment-level charges/fees (no item -> quantity=0)
            for ch in (sh.get("ShipmentChargeList") or []):
                a, cur = _money_to_tuple(ch.get("ChargeAmount"))
                if a:
                    _fin_push(rows, a, ch.get("ChargeType") or "shipment charge", "itemprice",
                              posted, order_id, None, cur, "Shipment", quantity=0)

            for fee in (sh.get("ShipmentFeeList") or []):
                a, cur = _money_to_tuple(fee.get("FeeAmount"))
                if a:
                    _fin_push(rows, -a, fee.get("FeeType") or "shipment fee", "fee",
                              posted, order_id, None, cur, "Shipment", quantity=0)

            # Items
            for item in (sh.get("ShipmentItemList") or []):
                sku = item.get("SellerSKU")
                qty = _get_item_qty(item)

                # Item charges
                for ch in (item.get("ItemChargeList") or []):
                    a, cur = _money_to_tuple(ch.get("ChargeAmount"))
                    if not a:
                        continue
                    ctype = (ch.get("ChargeType") or "").lower()
                    if "principal" in ctype:
                        desc, atype = "principal", "itemprice"
                    elif "tax" in ctype:
                        desc, atype = ctype, "tax"
                    elif "shipping" in ctype or "delivery" in ctype:
                        desc, atype = ctype, "itemprice"
                    else:
                        desc, atype = ctype or "charge", "itemprice"
                    _fin_push(rows, a, desc, atype, posted, order_id, sku, cur, "Shipment", quantity=qty)

                # Item fees
                for fee in (item.get("ItemFeeList") or []):
                    a, cur = _money_to_tuple(fee.get("FeeAmount"))
                    if a:
                        desc = (fee.get("FeeType") or "fee")
                        _fin_push(rows, -a, desc, "fee", posted, order_id, sku, cur, "Shipment", quantity=qty)

                # Promotions
                for pr in (item.get("PromotionList") or []):
                    a, cur = _money_to_tuple(pr.get("PromotionAmount"))
                    if a:
                        _fin_push(rows, -a, (pr.get("PromotionType") or "promotion"), "promotion",
                                  posted, order_id, sku, cur, "Shipment", quantity=qty)

                # Fee adjustments at item level
                for adj in (item.get("ItemFeeAdjustmentList") or []):
                    a, cur = _money_to_tuple(adj.get("FeeAmount"))
                    if a:
                        _fin_push(rows, -a, (adj.get("FeeType") or "fee adjustment"), "fee",
                                  posted, order_id, sku, cur, "Shipment", quantity=qty)

    # 2) Refunds
    if "RefundEventList" in evt:
        for rf in (evt.get("RefundEventList") or []):
            order_id = rf.get("AmazonOrderId")
            posted = _parse_dt_any(rf.get("PostedDate")) or dt

            for item in (rf.get("RefundItemList") or []):
                sku = item.get("SellerSKU")
                qty = _get_item_qty(item)

                for ch in (item.get("ItemChargeAdjustmentList") or []):
                    a, cur = _money_to_tuple(ch.get("ChargeAmount"))
                    if not a:
                        continue
                    ctype = (ch.get("ChargeType") or "").lower()
                    if "principal" in ctype:
                        desc, atype = "refund principal", "itemprice"
                    elif "tax" in ctype:
                        desc, atype = ctype, "tax"
                    elif "shipping" in ctype or "delivery" in ctype:
                        desc, atype = ctype, "itemprice"
                    else:
                        desc, atype = ctype or "refund charge", "itemprice"
                    _fin_push(rows, a, desc, atype, posted, order_id, sku, cur, "Refund", quantity=qty)

                for fee in (item.get("ItemFeeAdjustmentList") or []):
                    a, cur = _money_to_tuple(fee.get("FeeAmount"))
                    if a:
                        _fin_push(rows, -a, (fee.get("FeeType") or "refund fee"), "fee",
                                  posted, order_id, sku, cur, "Refund", quantity=qty)

                for pr in (item.get("PromotionAdjustmentList") or []):
                    a, cur = _money_to_tuple(pr.get("PromotionAmount"))
                    if a:
                        _fin_push(rows, -a, (pr.get("PromotionType") or "refund promo"), "promotion",
                                  posted, order_id, sku, cur, "Refund", quantity=qty)

    # 3) Service fees (no item qty available)
    if "ServiceFeeEventList" in evt:
        for sfee in (evt.get("ServiceFeeEventList") or []):
            posted = _parse_dt_any(sfee.get("PostedDate")) or dt
            for fee in (sfee.get("FeeList") or []):
                a, cur = _money_to_tuple(fee.get("FeeAmount"))
                if a:
                    _fin_push(rows, -a, (fee.get("FeeType") or "service fee"), "fee",
                              posted, sfee.get("AmazonOrderId"), None, cur, "ServiceFee", quantity=0)

    # 4) Product Ads payments
    if "ProductAdsPaymentEventList" in evt:
        for ad in (evt.get("ProductAdsPaymentEventList") or []):
            posted = _parse_dt_any(ad.get("PostedDate")) or dt
            a, cur = _money_to_tuple(ad.get("transactionAmount"))
            if a:
                _fin_push(rows, -a, "advertising cost", "advertising", posted, None, None, cur, "Advertising", quantity=0)

    # 5) Adjustments
    if "AdjustmentEventList" in evt:
        for adj in (evt.get("AdjustmentEventList") or []):
            posted = _parse_dt_any(adj.get("PostedDate")) or dt
            for it in (adj.get("AdjustmentItemList") or []):
                a, cur = _money_to_tuple(it.get("AdjustmentAmount"))
                if a:
                    _fin_push(rows, a, "adjustment", "other", posted, None, it.get("SellerSKU"), cur, "Adjustment", quantity=0)

    # 6) Chargebacks / Guarantee claims / Retrocharges
    if "ChargebackEventList" in evt:
        for chb in (evt.get("ChargebackEventList") or []):
            posted = _parse_dt_any(chb.get("PostedDate")) or dt
            a, cur = _money_to_tuple(chb.get("ChargebackAmount"))
            if a:
                _fin_push(rows, -a, "chargeback", "other", posted, chb.get("AmazonOrderId"), None, cur, "Chargeback", quantity=0)

    if "GuaranteeClaimEventList" in evt:
        for gcl in (evt.get("GuaranteeClaimEventList") or []):
            posted = _parse_dt_any(gcl.get("PostedDate")) or dt
            a, cur = _money_to_tuple(gcl.get("ClaimAmount"))
            if a:
                _fin_push(rows, -a, "guarantee claim", "other", posted, gcl.get("AmazonOrderId"), None, cur, "GuaranteeClaim", quantity=0)

    if "RetrochargeEventList" in evt:
        for rc in (evt.get("RetrochargeEventList") or []):
            posted = _parse_dt_any(rc.get("PostedDate")) or dt
            for key in ("RetrochargeAmount", "BaseTax", "ShippingTax"):
                a, cur = _money_to_tuple(rc.get(key))
                if a:
                    _fin_push(rows, -a, f"retrocharge {key}", "other", posted, rc.get("AmazonOrderId"), None, cur, "Retrocharge", quantity=0)

    return rows


# ---------------------------------------------------------------------------
# GET-page utilities (adaptive slicing)
# ---------------------------------------------------------------------------

def _finances_page_to_rows(page: dict, user_id: Any) -> tuple[list[dict], bool]:
    """Turn one /financialEvents page payload into our raw row list."""
    rows: list[dict] = []
    payload = (page.get("payload") or page) or {}
    fe = payload.get("FinancialEvents") or {}

    for sh in fe.get("ShipmentEventList") or []:
        rows.extend(_fin_event_to_rows({"ShipmentEventList": [sh], "PostedDate": sh.get("PostedDate")}, user_id))
    for rf in fe.get("RefundEventList") or []:
        rows.extend(_fin_event_to_rows({"RefundEventList": [rf], "PostedDate": rf.get("PostedDate")}, user_id))
    for sf in fe.get("ServiceFeeEventList") or []:
        rows.extend(_fin_event_to_rows({"ServiceFeeEventList": [sf], "PostedDate": sf.get("PostedDate")}, user_id))
    for ad in fe.get("ProductAdsPaymentEventList") or []:
        rows.extend(_fin_event_to_rows({"ProductAdsPaymentEventList": [ad], "PostedDate": ad.get("PostedDate")}, user_id))
    for aj in fe.get("AdjustmentEventList") or []:
        rows.extend(_fin_event_to_rows({"AdjustmentEventList": [aj], "PostedDate": aj.get("PostedDate")}, user_id))
    for cb in fe.get("ChargebackEventList") or []:
        rows.extend(_fin_event_to_rows({"ChargebackEventList": [cb], "PostedDate": cb.get("PostedDate")}, user_id))
    for gc in fe.get("GuaranteeClaimEventList") or []:
        rows.extend(_fin_event_to_rows({"GuaranteeClaimEventList": [gc], "PostedDate": gc.get("PostedDate")}, user_id))
    for rc in fe.get("RetrochargeEventList") or []:
        rows.extend(_fin_event_to_rows({"RetrochargeEventList": [rc], "PostedDate": rc.get("PostedDate")}, user_id))

    next_token = payload.get("NextToken")
    return rows, bool(next_token)


def _get_financial_events_page(
    start_dt: datetime,
    end_dt: datetime,
    max_results: int = 100,
 ) -> dict | None:
    """Single GET /finances/v0/financialEvents for a window [start, end)."""
    params = {
        "PostedAfter": start_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "PostedBefore": end_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "MaxResultsPerPage": max(1, min(int(max_results), 100)),
    }
    backoff = 1.0
    while True:
        res = amazon_client.make_api_call("/finances/v0/financialEvents", "GET", params)
        if res and "error" not in res:
            return res
        errs = ((res or {}).get("errors") or []) if isinstance(res, dict) else []
        code = errs[0].get("code") if errs else ""
        status = (res or {}).get("status_code")
        if (status == 429 or "QuotaExceeded" in code or "TooManyRequests" in code) and backoff <= 64:
            time.sleep(backoff + random.random())
            backoff *= 2
            continue
        return None  # give up on other errors


def _collect_month_events_events_only(
    start_m: datetime,
    end_m: datetime,
    user_id: Any,
    initial_hours: int = 24,
    min_slice_minutes: int = 30,
    max_per_page: int = 100,
    debug: bool = False,  # reserved for future; we still return slice_debug
 ) -> tuple[list[dict], list[dict]]:
    """
    Cover [start_m, end_m) using ONLY GET pages.

    - Start with 'initial_hours' slices (e.g., 24h).
    - For any slice that returns NextToken, split it into halves and retry,
      recursively, until NextToken disappears or the slice reaches 'min_slice_minutes'
      (then we accept first page only).

    Returns (raw_rows, slice_debug_list).
    """
    raw_rows: list[dict] = []
    slice_debug: list[dict] = []

    cur = start_m
    step = timedelta(hours=initial_hours)
    windows: list[tuple[datetime, datetime]] = []
    while cur < end_m:
        nxt = min(cur + step, end_m)
        windows.append((cur, nxt))
        cur = nxt

    # LIFO so earlier time slices stay near top of the returned debug
    stack = windows[:]
    while stack:
        s, e = stack.pop()
        if s >= e:
            continue

        page = _get_financial_events_page(s, e, max_results=max_per_page)
        page_rows, next_token_seen = _finances_page_to_rows(page or {}, user_id) if page else ([], False)
        raw_rows.extend(page_rows)

        info = {
            "start": s.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "end": e.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "rows": len(page_rows),
            "nextToken_seen": next_token_seen,
            "slice_minutes": int((e - s).total_seconds() / 60),
        }
        slice_debug.append(info)

        if next_token_seen:
            dur_min = (e - s).total_seconds() / 60.0
            if dur_min > min_slice_minutes:
                mid = s + (e - s) / 2
                if mid <= s:
                    mid = s + timedelta(seconds=1)
                if e - mid <= timedelta(0):
                    mid = s + (e - s) * 0.6
                stack.append((mid, e))
                stack.append((s, mid))
                continue

    return raw_rows, slice_debug


# ---------------------------------------------------------------------------
# Report discovery & selection
# ---------------------------------------------------------------------------

def _list_settlement_reports_recent(days_back: int = 90) -> list[dict]:
    """
    Get recent DONE settlement reports regardless of your requested window.
    We'll choose the best one by overlapping dataStartTime/dataEndTime later.
    """
    end_dt = datetime.now(timezone.utc)
    start_dt = end_dt - timedelta(days=max(1, days_back))

    params: dict[str, Any] = {
        "reportTypes": ",".join(REPORT_TYPES_SETTLEMENT),
        "createdSince": start_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "createdUntil": end_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "pageSize": 100,
    }
    out: list[dict] = []
    while True:
        res = amazon_client.make_api_call("/reports/2021-06-30/reports", "GET", params)
        items = ((res or {}).get("payload") or {}).get("reports") or []
        out.extend([i for i in items if i.get("processingStatus") == "DONE"])
        token = ((res or {}).get("payload") or {}).get("nextToken")
        if not token:
            break
        params = {"nextToken": token}
    # newest first by creation time
    out.sort(key=lambda r: r.get("createdTime") or "", reverse=True)
    return out


def _iso_to_dt(s: str) -> datetime | None:
    """Parse an ISO8601 into aware datetime where possible (Z -> +00:00)."""
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None


def _choose_report_for_window(
    reports: list[dict],
    start_dt: datetime,
    end_dt: datetime
 ) -> dict | None:
    """
    Pick the report whose data window overlaps [start_dt, end_dt).
    Falls back to the most recent DONE report if no overlap found.
    """
    best = None
    for r in reports:
        ds = _iso_to_dt(r.get("dataStartTime"))
        de = _iso_to_dt(r.get("dataEndTime"))
        if ds and de and ds < end_dt and de > start_dt:
            # overlap found — prefer the newest by dataEndTime
            if not best or _iso_to_dt(best.get("dataEndTime")) < de:
                best = r
    return best or (reports[0] if reports else None)



# ---------------------------------------------------------------------------
# Shared numeric/string normalizers
# ---------------------------------------------------------------------------

def _to_number(v: Any) -> int | float:
    """
    Convert typical numeric-ish values (Decimal/str with symbols/etc.) into int/float.
    Returns 0 on failure.
    """
    if v is None or v == "":
        return 0
    if isinstance(v, (int, float)):
        return int(v) if float(v).is_integer() else float(v)
    if isinstance(v, Decimal):
        f = float(v)
        return int(f) if f.is_integer() else f
    if isinstance(v, str):
        s = _num_clean_re.sub("", v.strip())
        if s in ("", ".", "-"):
            return 0
        try:
            d = Decimal(s)
        except (InvalidOperation, ValueError):
            return 0
        f = float(d)
        return int(f) if f.is_integer() else f
    try:
        f = float(v)
        return int(f) if f.is_integer() else f
    except Exception:
        return 0


def _normalize_sku_row(row: dict) -> dict:
    """
    Normalize SKU analytics row for UI/aggregations. Preserves keys and coerces numbers.
    """
    r = dict(row or {})
    r["quantity"] = _to_number(r.get("quantity"))
    r["asp"] = _to_number(r.get("asp") or r.get("ASP"))
    r["net_sales"] = _to_number(r.get("net_sales"))
    r["cost_of_unit_sold"] = _to_number(r.get("cost_of_unit_sold"))
    r["amazon_fee"] = _to_number(r.get("amazon_fee"))
    r["selling_fees"] = _to_number(r.get("selling_fees"))
    r["fba_fees"] = _to_number(r.get("fba_fees"))
    r["net_credits"] = _to_number(r.get("net_credits"))
    r["net_taxes"] = _to_number(r.get("net_taxes"))

    # critical for your UI (.toFixed usage)
    r["profit"] = _to_number(r.get("profit"))
    r["profit_percentage"] = _to_number(r.get("profit_percentage"))      # keep as 56.25 (UI adds %)
    r["unit_wise_profitability"] = _to_number(r.get("unit_wise_profitability"))
    r["profit_mix"] = _to_number(r.get("profit_mix"))                     # keep as 56.25 (UI adds %)
    r["sales_mix"] = _to_number(r.get("sales_mix"))                       # keep as 56.25 (UI adds %)

    r["product_name"] = (r.get("product_name") or "").strip()
    r["sku"] = (r.get("sku") or "").strip()
    return r


def _aggregate_to_transactions_full(raw_rows: list[dict], user_id: Any) -> tuple[list[dict], set[str]]:

    from collections import defaultdict

    # Group rows by transaction key (order, sku, datetime, *raw* tx_type)
    groups: dict[tuple, list[dict]] = defaultdict(list)

    for row in raw_rows:
        order_id = (row.get("order id") or "").strip()
        sku = (row.get("sku") or "").strip()
        dt_str = (row.get("date/time") or "").strip()

        # Prefer the raw Finances transaction type if present
        tx_type = (row.get("raw_tx_type") or row.get("type") or "").strip()

        dt = _parse_dt_any(dt_str) if dt_str else None
        key = (order_id, sku, dt, tx_type)
        groups[key].append(row)

    aggregated: list[dict] = []
    settlement_ids: set[str] = set()

    # Helper: pretty description based on Finances tx_type (matches listTransactions docs)
    def _pretty_description_for_tx_type(tx: str) -> str:
        t = (tx or "").strip()
        if t == "Shipment":
            return "Order Payment"
        if t == "Refund":
            return "Refund Order"
        if t == "ServiceFee":
            return "Service Fee"
        if t == "Advertising":
            return "Advertising Cost"
        if t == "Adjustment":
            return "Adjustment"
        if t == "Chargeback":
            return "Chargeback"
        if t == "GuaranteeClaim":
            return "Guarantee Claim"
        if t == "Retrocharge":
            return "Retrocharge"
        # Fallback
        return t or "Other"

    for (order_id, sku, dt, tx_type), group_rows in groups.items():
        totals = {
            "product_sales": Decimal("0"),
            "product_sales_tax": Decimal("0"),
            "postage_credits": Decimal("0"),
            "shipping_credits_tax": Decimal("0"),
            "gift_wrap_credits": Decimal("0"),
            "giftwrap_credits_tax": Decimal("0"),
            "promotional_rebates": Decimal("0"),
            "promotional_rebates_tax": Decimal("0"),
            "marketplace_withheld_tax": Decimal("0"),
            "selling_fees": Decimal("0"),
            "fba_fees": Decimal("0"),
            "other_transaction_fees": Decimal("0"),
            "other": Decimal("0"),
            "advertising_cost": Decimal("0"),
        }

        quantity = 0
        settlement_id = ""
        marketplace = ""
        fulfilment = ""
        order_city = ""
        order_state = ""
        order_postal = ""
        tax_collection_model = ""
        currency = ""

        for row in group_rows:
            amount_str = row.get("_amount", "0")
            amount_type = row.get("_amount_type", "")
            amount_desc = (row.get("_amount_desc") or row.get("description") or "").lower()

            try:
                amount = Decimal(str(amount_str))
            except Exception:
                amount = Decimal("0")

            # quantity: take max within the group
            row_qty = row.get("quantity", 0)
            if isinstance(row_qty, (int, float)):
                quantity = max(quantity, int(row_qty))

            # metadata (first non-empty wins)
            if not settlement_id and row.get("settlement id"):
                settlement_id = row.get("settlement id") or ""
            if not marketplace and row.get("marketplace"):
                marketplace = row.get("marketplace") or ""
            if not fulfilment and row.get("fulfilment"):
                fulfilment = row.get("fulfilment") or ""
            if not order_city and row.get("order_city"):
                order_city = row.get("order_city") or ""
            if not order_state and row.get("order_state"):
                order_state = row.get("order_state") or ""
            if not order_postal and row.get("order_postal"):
                order_postal = row.get("order_postal") or ""
            if not tax_collection_model and row.get("tax_collection_model"):
                tax_collection_model = row.get("tax_collection_model") or ""
            if not currency and row.get("currency"):
                currency = row.get("currency") or ""

            # ---- bucket mapping (same as your existing logic, just kept) ----
            if amount_type == "itemprice":
                if "principal" in amount_desc:
                    totals["product_sales"] += amount
                elif "shipping" in amount_desc or "postage" in amount_desc or "delivery" in amount_desc:
                    totals["postage_credits"] += amount
                elif "giftwrap" in amount_desc or "gift wrap" in amount_desc or "gift_wrap" in amount_desc:
                    totals["gift_wrap_credits"] += amount
                else:
                    totals["product_sales"] += amount

            elif amount_type == "tax":
                norm = amount_desc.replace("_", "").replace("-", "").lower()

                # Marketplace Facilitator / withheld tax
                if "marketplacefacilitatortax" in norm or (
                    "marketplace" in norm and ("withheld" in norm or "facilitator" in norm)
                ):
                    totals["marketplace_withheld_tax"] += amount
                elif "shipping" in norm or "postage" in norm:
                    totals["shipping_credits_tax"] += amount
                elif "giftwrap" in norm or "gift wrap" in norm:
                    totals["giftwrap_credits_tax"] += amount
                else:
                    totals["product_sales_tax"] += amount

            elif amount_type == "promotion":
                if "tax" in amount_desc:
                    totals["promotional_rebates_tax"] += amount
                else:
                    totals["promotional_rebates"] += amount

            elif amount_type == "fee":
                desc_lower = amount_desc.lower().replace(" ", "").replace("_", "")
                if any(hint in desc_lower for hint in ["fba", "fulfillment", "fulfilment", "storage", "disposal"]):
                    totals["fba_fees"] += abs(amount)
                elif any(hint in desc_lower for hint in ["selling", "commission", "referral"]):
                    totals["selling_fees"] += abs(amount)
                else:
                    totals["other_transaction_fees"] += abs(amount)

            elif amount_type == "advertising":
                totals["advertising_cost"] += abs(amount)

            elif amount_type == "other":
                totals["other"] += amount

            else:
                if "fee" in amount_desc:
                    if "fba" in amount_desc or "fulfillment" in amount_desc:
                        totals["fba_fees"] += abs(amount)
                    elif "selling" in amount_desc or "commission" in amount_desc:
                        totals["selling_fees"] += abs(amount)
                    else:
                        totals["other_transaction_fees"] += abs(amount)
                else:
                    totals["other"] += amount

        # Example "total" – adjust if you want something different
        total = (
            totals["postage_credits"]
            + totals["shipping_credits_tax"]
            + totals["selling_fees"]
            + totals["fba_fees"]
        )

        # Force marketplace_withheld_tax to negative
        totals["marketplace_withheld_tax"] = -abs(totals["marketplace_withheld_tax"])

        # Force promotional_rebates to negative
        totals["promotional_rebates"] = -abs(totals["promotional_rebates"])

        pretty_desc = _pretty_description_for_tx_type(tx_type)

        agg_record = {
            "date_time": dt,
            "settlement_id": settlement_id,
            # 👇 NOW this is the *Finances* transaction type (Shipment / Refund / ServiceFee / Advertising / Adjustment ...)
            "transaction_type": tx_type or "Other",
            "order_id": order_id,
            "sku": sku,
            "description": pretty_desc,   # 👈 clean, high-level description
            "quantity": quantity,
            "marketplace": marketplace,
            "fulfilment": fulfilment,
            "order_city": order_city,
            "order_state": order_state,
            "order_postal": order_postal,
            "tax_collection_model": tax_collection_model,
            "currency": currency,

            "product_sales": totals["product_sales"],
            "product_sales_tax": totals["product_sales_tax"],
            "postage_credits": totals["postage_credits"],
            "shipping_credits_tax": totals["shipping_credits_tax"],
            "gift_wrap_credits": totals["gift_wrap_credits"],
            "giftwrap_credits_tax": totals["giftwrap_credits_tax"],
            "promotional_rebates": totals["promotional_rebates"],
            "promotional_rebates_tax": totals["promotional_rebates_tax"],
            "marketplace_withheld_tax": totals["marketplace_withheld_tax"],
            "selling_fees": totals["selling_fees"],
            "fba_fees": totals["fba_fees"],
            "other_transaction_fees": totals["other_transaction_fees"],
            "other": totals["other"],
            "advertising_cost": totals["advertising_cost"],
            "total": total,
            "platform_fees": totals.get("platform_fees", Decimal("0")),
            "net_reimbursement": totals.get("net_reimbursement", Decimal("0")),
        }

        aggregated.append(agg_record)

        if settlement_id:
            settlement_ids.add(settlement_id)

    return aggregated, settlement_ids


# Replace the _map_to_settlement_type function with this improved version:

def _map_to_settlement_type(sp_type: str | None, desc: str | None) -> str:
    """
    Map SP-API event types and descriptions to settlement report transaction types.
    Returns types matching settlement exports: Order, Refund, Adjustment, Amazon Fees, 
    FBA Inventory Fee, Service Fee, Transfer, etc.
    """
    t = (sp_type or "").strip()
    d = (desc or "").strip().lower()
    
    # Normalize description for matching
    d_normalized = d.replace("_", " ").replace("-", " ")
    
    # 1. Orders - Primary revenue transactions
    if t == "Shipment":
        if "principal" in d:
            return "Order"
        elif "shipping" in d or "delivery" in d:
            return "Order"
        elif "gift" in d:
            return "Order"
        elif "tax" in d:
            return "Order"
        elif "promotion" in d or "promo" in d:
            return "Order"
        return "Order"
    
    # 2. Refunds - Returns and refund transactions
    if t == "Refund":
        return "Refund"
    
    # 3. Fees - All fee-related transactions
    if "fee" in d:
        d_clean = d_normalized.replace(" ", "")
        
        # FBA Inventory Fee (storage, long-term, disposal, etc.)
        fba_inventory_keywords = {
            "fbainventorystoragefee", "fbalongtermstoragefee", "monthlystoragefee",
            "fba long-term storage fee", "fba inventory storage fee", "fba storage fee",
            "fbadisposalfee", "fba disposal fee", "storagefee", "inventorystoragefee",
            "longtermstorageefee", "inventoryplacementfee", "inventory placement",
            "lowinventorylevel", "low inventory level"
        }
        
        if any(keyword in d_clean.lower() for keyword in fba_inventory_keywords):
            return "FBA Inventory Fee"
        
        # FBA fulfillment fees
        if "fba" in d_clean or "fulfillment" in d_clean or "fulfilment" in d_clean:
            return "Amazon Fees"
        
        # Selling/referral fees
        if "selling" in d or "referral" in d or "commission" in d:
            return "Amazon Fees"
        
        # Advertising
        if "advertising" in d or "sponsored" in d or "ads" in d:
            return "Service Fee"
        
        # Other service fees
        return "Service Fee"
    
    # 4. Service Fee type transactions
    if t == "ServiceFee":
        d_clean = d_normalized.replace(" ", "")
        
        # FBA Inventory fees
        fba_inventory_keywords = {
            "fbainventorystoragefee", "fbalongtermstoragefee", "monthlystoragefee",
            "inventorystorage", "storagefee", "disposal", "inboundtransportation",
            "inventoryplacement", "lowinventorylevel"
        }
        
        if any(keyword in d_clean.lower() for keyword in fba_inventory_keywords):
            return "FBA Inventory Fee"
        
        return "Service Fee"
    
    # 5. Advertising (treated as Service Fee in settlements)
    if t == "Advertising" or "advertising" in d or "sponsored" in d:
        return "Service Fee"
    
    # 6. Adjustments
    if t == "Adjustment" or "adjustment" in d:
        return "Adjustment"
    
    # 7. Chargebacks, Claims, Retrocharges (like refunds)
    if t in ("Chargeback", "GuaranteeClaim", "Retrocharge"):
        return "Refund"
    
    # 8. Transfers
    if t == "Transfer" or "transfer" in d:
        return "Transfer"
    
    # 9. Vouchers, Deals, Subscriptions
    if "voucher" in d or "deal" in d or "subscription" in d:
        return "Service Fee"
    
    # 10. Reimbursements
    if "reimbursement" in d or "reimburse" in d:
        return "Adjustment"
    
    # Default: try to preserve meaningful type or return "Other"
    if t:
        return t
    
    return "Other"


# Also update the _fin_push function to better handle description mapping:

def _fin_push(
    rows: list[dict],
    amount: Decimal | int | float | None,
    desc: str | None,
    atype: str | None,
    dt: datetime | None,
    order_id: str | None = None,
    sku: str | None = None,
    currency: str | None = None,
    tx_type: str | None = None,
    quantity: int | None = None,
) -> None:
    """
    Append a single normalized finances row to `rows`.

    - `tx_type` is the raw Finances event container type, e.g. Shipment / Refund / ServiceFee / Advertising / Adjustment ...
    - We ALSO derive a settlement-style type (Order, Refund, Service Fee, etc.) for compatibility and store it in `type`.
    """

    if amount is None:
        return
    q = 0 if quantity is None else quantity

    # Map to settlement type (Order / Refund / Service Fee / FBA Inventory Fee / etc.)
    settlement_type = _map_to_settlement_type(tx_type, desc)

    # Clean up description for better readability
    clean_desc = (desc or "").strip()
    if clean_desc:
        # Capitalize first letter of each word for consistency
        clean_desc = " ".join(word.capitalize() for word in clean_desc.split())

    rows.append({
        # settlement-style columns (v1 parity)
        "settlement id": "",
        "order id": order_id or "",
        "sku": sku or "",
        "date/time": dt.strftime("%Y-%m-%dT%H:%M:%SZ") if dt else "",
        "type": settlement_type,              # Settlement-style type (Order, Refund, Service Fee,...)
        "marketplace": "",
        "fulfilment": "",
        "description": clean_desc,            # Raw line-level description (principal, tax, commission,...)
        "quantity": q,
        "currency": currency or "",

        # Legacy V1 columns (kept for upload/CSV parity)
        "product sales": "", "product sales tax": "", "postage credits": "",
        "shipping credits tax": "", "gift wrap credits": "", "giftwrap credits tax": "",
        "promotional rebates": "", "promotional rebates tax": "", "marketplace withheld tax": "",
        "selling fees": "", "fba fees": "", "other transaction fees": "", "other": "", "total": "",
        "advertising cost": "",

        # V2 triplet (actual money)
        "_amount_type": atype or "",
        "_amount_desc": desc or "",
        "_amount": str(amount),

        # 👇 NEW: keep the raw Finances type so we can expose it as transaction_type later
        "raw_tx_type": tx_type or "",        # e.g. Shipment / Refund / ServiceFee / Advertising / Adjustment ...
    })


# # ---------------------------------------------------------------------------
# # Route: GET-only, full-month coverage
# # ---------------------------------------------------------------------------

# @amazon_sales_api_bp.route("/amazon_api/settlements_finances", methods=["GET"])
# def settlements_finances_route():
#     # optional auth (for upload)
#     auth_header = request.headers.get("Authorization")
#     user_id = None
#     if auth_header and auth_header.startswith("Bearer "):
#         try:
#             payload = jwt.decode(auth_header.split(" ")[1], SECRET_KEY, algorithms=["HS256"])
#             user_id = payload.get("user_id")
#         except Exception:
#             user_id = None

#     _apply_region_and_marketplace_from_request()

#     out_format = (request.args.get("format") or "json").lower()
#     store_in_db = (request.args.get("store_in_db", "true").lower() != "false")
#     limit_param = (request.args.get("limit") or "50").strip().lower()
#     json_limit = None if limit_param == "all" else max(0, int(limit_param) if limit_param.isdigit() else 50)
#     debug = (request.args.get("debug", "0").lower() in ("1", "true", "yes"))

#     # upload args
#     run_upload = (request.args.get("run_upload_pipeline", "false").lower() == "true")
#     ui_country = (request.args.get("country") or "").lower()
#     ui_year = (request.args.get("year") or "").strip()

#     # window
#     month_raw = request.args.get("month")
#     start_arg = request.args.get("start")
#     end_arg = request.args.get("end")
#     if month_raw:
#         try:
#             y, m = _parse_month_input(month_raw)
#             month_filter = f"{y}-{str(m).zfill(2)}"
#             start_m, end_m = _month_bounds(month_filter)
#         except Exception:
#             return jsonify({"success": False, "error": "Invalid month parameter"}), 400
#     else:
#         if not (start_arg and end_arg):
#             return jsonify({"success": False, "error": "Provide ?month=... or ?start=...&end=..."}), 400
#         start_m = _parse_dt_any(start_arg)
#         end_m = _parse_dt_any(end_arg)
#         if not start_m or not end_m or end_m <= start_m:
#             return jsonify({"success": False, "error": "Invalid start/end"}), 400
#         month_filter = None  # noqa: F841  (kept for parity/debugging if needed)

#     # tuning knobs
#     slice_hours = int(request.args.get("slice_hours", "24"))
#     slice_hours = max(1, min(slice_hours, 48))
#     min_slice_minutes = int(request.args.get("min_slice_minutes", "30"))
#     min_slice_minutes = max(5, min(min_slice_minutes, slice_hours * 60))

#     # collect raw events
#     raw_rows, slice_debug = _collect_month_events_events_only(
#         start_m, end_m, user_id,
#         initial_hours=slice_hours,
#         min_slice_minutes=min_slice_minutes,
#         max_per_page=100,
#         debug=debug
#     )

#     # ---- aggregate using the improved function (paste the new version above this route) ----
#     aggregated, _sid_set = _aggregate_to_transactions_full(raw_rows, user_id)

#     # ---- enrich (optional) ----
#     if request.args.get("enrich_from_settlement_report", "true").lower() != "false":
#         _enrich_from_settlement_report(aggregated, start_m, end_m, logger)

#     # ---- month filter ----
#     filtered = [b for b in aggregated if b.get("date_time") and start_m <= b["date_time"] < end_m]

#     # ---- serializers (strings for Decimals so JSON/CSV don’t choke) ----
#     def T(x: Any) -> str:  # text (allow empty)
#         return "" if x is None else str(x)

#     def M(x: Any) -> int | float:  # money/decimal → numeric
#         return _to_number(x)

#     def Q(x: Any) -> int:  # quantity → int
#         return int(_to_number(x))

#     # ---- optional upload pipeline (also use T/M here) ----
#     if run_upload:
#         if not user_id:
#             return jsonify({"success": False, "error": "Auth required"}), 401
#         if month_raw is None:
#             return jsonify({"success": False, "error": "Provide ?month=YYYY-MM (or YYYY-Month) when run_upload_pipeline=true"}), 400
#         if not ui_country or not ui_year:
#             return jsonify({"success": False, "error": "Provide &country=uk&year=2025"}), 400

#         def row_to_dict(r: dict) -> dict:
#             return {
#                 "date/time": r["date_time"].strftime("%Y-%m-%dT%H:%M:%SZ") if r["date_time"] else None,
#                 "settlement_id": T(r["settlement_id"]),
#                 "type": r["transaction_type"],
#                 "order_id": r["order_id"],
#                 "sku": r["sku"],
#                 "description": r["description"],
#                 "quantity": Q(r["quantity"]),
#                 "marketplace": T(r["marketplace"]),
#                 "fulfilment": r["fulfilment"],
#                 "order_city": T(r["order_city"]),
#                 "order_state": T(r["order_state"]),
#                 "order_postal": T(r["order_postal"]),
#                 "tax_collection_model": T(r["tax_collection_model"]),
#                 "product_sales": r["product_sales"],
#                 "product_sales_tax": r["product_sales_tax"],
#                 "postage_credits": M(r["postage_credits"]),
#                 "shipping_credits_tax": M(r["shipping_credits_tax"]),
#                 "gift_wrap_credits": r["gift_wrap_credits"],
#                 "giftwrap_credits_tax": r["giftwrap_credits_tax"],
#                 "promotional_rebates": M(r["promotional_rebates"]),
#                 "promotional_rebates_tax": M(r["promotional_rebates_tax"]),
#                 "marketplace_withheld_tax": M(r["marketplace_withheld_tax"]),
#                 "selling_fees": r["selling_fees"],
#                 "fba_fees": r["fba_fees"],
#                 "other_transaction_fees": M(r["other_transaction_fees"]),
#                 "other": M(r["other"]),
#                 "total": r["total"],
#                 "currency": r["currency"],
#                 "advertising_cost": r["advertising_cost"],
#                 "net_reimbursement": r["net_reimbursement"],

#                 # 👇 override: take numeric from r["platform"] if present
#                 "platform_fees": M(r.get("platform", r.get("platform_fees"))),
#             }


#         df_in = pd.DataFrame([row_to_dict(r) for r in filtered])
#         _, mnum = _parse_month_input(month_raw)
#         month_num = int(mnum)

#         result = run_upload_pipeline_from_df(
#             df_raw=df_in,
#             user_id=user_id,
#             country=ui_country,
#             month_num=month_num,
#             year=str(ui_year),
#             db_url=os.getenv("DATABASE_URL"),
#             db_url_aux=os.getenv("DATABASE_URL1"),
#         )
#         return jsonify(result), (200 if result.get("success") else 400)

#     # store?
#     inserted = 0
#     if store_in_db and filtered:
#         db.session.bulk_insert_mappings(SettlementTransaction, filtered)
#         db.session.commit()
#         inserted = len(filtered)

#     # ------------------- CSV -------------------
#     if out_format == "csv":
#         fieldnames = [
#             "date_time", "settlement_id", "transaction_type", "order_id", "sku", "description", "quantity",
#             "marketplace", "fulfilment", "order_city", "order_state", "order_postal", "tax_collection_model",
#             "product_sales", "product_sales_tax", "postage_credits", "shipping_credits_tax",
#             "gift_wrap_credits", "giftwrap_credits_tax", "promotional_rebates", "promotional_rebates_tax",
#             "marketplace_withheld_tax", "selling_fees", "fba_fees", "other_transaction_fees", "other", "total",
#             "currency", "advertising_cost", "platform_fees", "net_reimbursement",
#         ]

#         def to_row(r: dict) -> dict:
#             return {
#                 "date_time": r["date_time"].strftime("%Y-%m-%dT%H:%M:%SZ") if r["date_time"] else None,
#                 "settlement_id": T(r["settlement_id"]),
#                 "transaction_type": r["transaction_type"],
#                 "order_id": r["order_id"],
#                 "sku": r["sku"],
#                 "description": r["description"],
#                 "quantity": Q(r["quantity"]),
#                 "marketplace": T(r["marketplace"]),
#                 "fulfilment": r["fulfilment"],
#                 "order_city": T(r["order_city"]),
#                 "order_state": T(r["order_state"]),
#                 "order_postal": T(r["order_postal"]),
#                 "tax_collection_model": T(r["tax_collection_model"]),
#                 "product_sales": r["product_sales"],
#                 "product_sales_tax": r["product_sales_tax"],
#                 "postage_credits": M(r["postage_credits"]),
#                 "shipping_credits_tax": M(r["shipping_credits_tax"]),
#                 "gift_wrap_credits": r["gift_wrap_credits"],
#                 "giftwrap_credits_tax": r["giftwrap_credits_tax"],
#                 "promotional_rebates": M(r["promotional_rebates"]),
#                 "promotional_rebates_tax": M(r["promotional_rebates_tax"]),
#                 "marketplace_withheld_tax": M(r["marketplace_withheld_tax"]),
#                 "selling_fees": r["selling_fees"],
#                 "fba_fees": r["fba_fees"],
#                 "other_transaction_fees": M(r["other_transaction_fees"]),
#                 "other": M(r["other"]),
#                 "total": r["total"],
#                 "currency": r["currency"],
#                 "advertising_cost": r["advertising_cost"],
#                 # 👇
#                 "platform_fees": M(r.get("platform", r.get("platform_fees"))),
#                 "net_reimbursement": r["net_reimbursement"],
#             }

#         def generate_csv(data: Iterable[dict]):
#             out_io = io.StringIO()
#             w = csv.DictWriter(out_io, fieldnames=fieldnames)
#             w.writeheader()
#             yield out_io.getvalue()
#             out_io.seek(0)
#             out_io.truncate(0)
#             for r in data:
#                 w.writerow(to_row(r))
#                 yield out_io.getvalue()
#                 out_io.seek(0)
#                 out_io.truncate(0)

#         fname = f"settlement_transactions_finances_{start_m.strftime('%Y-%m')}.csv"
#         return Response(
#             generate_csv(filtered),
#             mimetype="text/csv",
#             headers={"Content-Disposition": f"attachment; filename={fname}"},
#         )

#     # ------------------- JSON -------------------
#     preview = filtered if json_limit is None else filtered[:json_limit]

#     def jsonify_row(r: dict) -> dict:
#         return {
#             "date/time": r["date_time"].strftime("%Y-%m-%dT%H:%M:%SZ") if r["date_time"] else None,
#             "settlement id": T(r["settlement_id"]),
#             "type": r["transaction_type"],
#             "order id": r["order_id"],
#             "sku": r["sku"],
#             "description": r["description"],
#             "quantity": Q(r["quantity"]),
#             "marketplace": T(r["marketplace"]),
#             "fulfilment": r["fulfilment"],
#             "order city": T(r["order_city"]),
#             "order state": T(r["order_state"]),
#             "order postal": T(r["order_postal"]),
#             "tax collection model": T(r["tax_collection_model"]),
#             "product sales": r["product_sales"],
#             "product sales tax": r["product_sales_tax"],
#             "postage credits": M(r["postage_credits"]),
#             "shipping credits tax": M(r["shipping_credits_tax"]),
#             "gift wrap credits": r["gift_wrap_credits"],
#             "giftwrap credits tax": r["giftwrap_credits_tax"],
#             "promotional rebates": M(r["promotional_rebates"]),
#             "promotional_rebates tax": M(r["promotional_rebates_tax"]),
#             "marketplace withheld tax": M(r["marketplace_withheld_tax"]),
#             "selling fees": r["selling_fees"],
#             "fba fees": r["fba_fees"],
#             "other transaction fees": M(r["other_transaction_fees"]),
#             "other": M(r["other"]),
#             "total": r["total"],
#             "currency": r["currency"],
#             "advertising_cost": r["advertising_cost"],
#             # 👇
#             "platform_fees": M(r.get("platform", r.get("platform_fees"))),
#             "net_reimbursement": r["net_reimbursement"],

#         }

#     resp: dict[str, Any] = {
#         "success": True,
#         "range_start": start_m.strftime("%Y-%m-%dT%H:%M:%SZ"),
#         "range_end": end_m.strftime("%Y-%m-%dT%H:%M:%SZ"),
#         "raw_events_rows": len(raw_rows),
#         "aggregated_rows": len(aggregated),
#         "filtered_rows": len(filtered),
#         "returned_rows": len(preview),
#         "stored": {"inserted": int(inserted)} if store_in_db else {"skipped": True},
#         "items": [jsonify_row(r) for r in preview],
#     }
#     if debug:
#         resp["slice_debug"] = slice_debug[:500]
#     return jsonify(resp), 200



