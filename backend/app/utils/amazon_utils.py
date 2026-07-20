from __future__ import annotations
import base64, io, os, calendar, logging, inspect
import urllib.parse
from sqlalchemy import text
from datetime import datetime, timedelta, date
import re
import pandas as pd
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, Optional
from datetime import datetime, timezone
from sqlalchemy.dialects.postgresql import insert as pg_insert
from config import Config
from dotenv import find_dotenv, load_dotenv
from flask import request
from sqlalchemy.dialects.postgresql import insert
from app import db
from app.models.user_models import UploadHistory, Liveorder
from app.utils.us_process_utils  import process_skuwise_us_data , process_us_yearly_skuwise_data, process_us_quarterly_skuwise_data
from app.utils.uk_process_utils import process_skuwise_data , process_quarterly_skuwise_data, process_yearly_skuwise_data 
from app.utils.plotting_utils import ( apply_modifications_fatch 
)
from app.utils.currency_utils import (  process_global_yearly_skuwise_data ,
    process_global_quarterly_skuwise_data , 
    process_global_monthly_skuwise_data
)
import boto3, requests, json, time
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from dateutil import parser
from app.models.user_models import db, Product
from app.utils.formulas_utils import uk_profit, safe_num, uk_advertising, uk_platform_fee
from app.routes.live_data_bi_routes import construct_prev_table_name, engine_hist, compute_sku_metrics_from_df
from typing import Optional, Tuple, List, Dict, Any
from datetime import datetime, timezone
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Float, text
from zoneinfo import ZoneInfo
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List, Tuple, Optional
from sqlalchemy import and_, text, create_engine
from config import Config
SECRET_KEY = Config.SECRET_KEY



# ---------------------------------------------------------
# CURRENCY MAP
# ---------------------------------------------------------
COUNTRY_TO_SELECTED_CURRENCY = {
    "uk": "GBP",
    "us": "USD",
    "india": "INR",
}

# sku table currency column is INR (per your screenshot)
DEFAULT_SKU_PRICE_CURRENCY = "INR"

# --- load .env robustly (works no matter where you run `flask run`) ---
dotenv_path = find_dotenv(filename=".env", usecwd=True)
load_dotenv(dotenv_path, override=True)

logger = logging.getLogger("amazon_sp_api")
logging.basicConfig(level=logging.INFO)
load_dotenv()
db_url  = os.getenv('DATABASE_URL')
db_url1 = os.getenv('DATABASE_ADMIN_URL') or db_url  # fallback

PHORMULA_ENGINE = create_engine(db_url, pool_pre_ping=True)
ADMIN_ENGINE = create_engine(db_url1, pool_pre_ping=True)

if not db_url:
    raise RuntimeError("DATABASE_URL is not set")
if not db_url1:
    # optional: log a warning if using fallback
    print("[WARN] DATABASE_ADMIN_URL not set; falling back to DATABASE_URL")


# ------------------------------------------------- Amazon Client -------------------------------------------------

class AmazonSPAPIClient:
    TOKEN_URL = "https://api.amazon.com/auth/o2/token"

    # We only support NA + UK
    PROD_ENDPOINTS = {
        "us-east-1": "https://sellingpartnerapi-na.amazon.com",   # US & CA
        "eu-west-1": "https://sellingpartnerapi-eu.amazon.com",   # UK
    }

    # Marketplaces we allow
    ALLOWED_MARKETPLACES = {
        "ATVPDKIKX0DER",  # US
        "A1F83G8C2ARO7P", # UK
        "A2EUQ1WTGCTBG2", # CA
    }

    # Default marketplace (per region) for convenience
    DEFAULT_MARKETPLACE_BY_REGION = {
        "us-east-1": "ATVPDKIKX0DER",  # default: US
        "eu-west-1": "A1F83G8C2ARO7P", # default: UK
    }

    # Region used per marketplace
    MARKETPLACE_REGION = {
        "ATVPDKIKX0DER": "us-east-1",  # US
        "A2EUQ1WTGCTBG2": "us-east-1", # CA
        "A1F83G8C2ARO7P": "eu-west-1", # UK
    }

    # Seller Central host per marketplace (consent screen)
    SELLER_CENTRAL_BY_MKT = {
        "ATVPDKIKX0DER": "https://sellercentral.amazon.com",    # US
        "A1F83G8C2ARO7P": "https://sellercentral.amazon.co.uk", # UK
        "A2EUQ1WTGCTBG2": "https://sellercentral.amazon.ca",    # CA
    }

    def __init__(self) -> None:
        env_region = os.getenv("AMAZON_REGION", "us-east-1")
        env_marketplace = os.getenv(
            "AMAZON_MARKETPLACE_ID",
            self.DEFAULT_MARKETPLACE_BY_REGION.get(env_region, "ATVPDKIKX0DER"),
        )
        if env_marketplace not in self.ALLOWED_MARKETPLACES:
            env_marketplace = "ATVPDKIKX0DER"
        self.marketplace_id = env_marketplace
        self.region = self.MARKETPLACE_REGION[self.marketplace_id]

        self.client_id = os.getenv("AMAZON_CLIENT_ID", "")
        self.client_secret = os.getenv("AMAZON_CLIENT_SECRET", "")
        self.app_id = os.getenv("AMAZON_APP_ID", "")
        self.redirect_uri = os.getenv("AMAZON_REDIRECT_URI", "http://127.0.0.1:5000/amazon_api/callback")

        self.api_base_url = self.PROD_ENDPOINTS[self.region]

        self.session = boto3.Session(
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            region_name=self.region,
        )

        self._access_token: Optional[str] = None
        self._token_expires_at: Optional[datetime] = None

        # Refresh token from env or .refresh_token file
        self.refresh_token = (os.getenv("AMAZON_REFRESH_TOKEN") or "").strip()
        if not self.refresh_token:
            try:
                with open(".refresh_token", "r") as f:
                    self.refresh_token = f.read().strip()
                    logger.info("Loaded refresh token from .refresh_token")
            except FileNotFoundError:
                pass

        logger.info(
            f"SP-API init (prod) -> region={self.region} base={self.api_base_url} "
            f"mkt={self.marketplace_id} has_refresh={bool(self.refresh_token)}"
        )

    def set_region(self, region: str):
        if region not in self.PROD_ENDPOINTS:
            return
        self.region = region
        self.api_base_url = self.PROD_ENDPOINTS[region]
        self.session = boto3.Session(
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            region_name=region,
        )
        # Ensure marketplace matches region
        if (self.marketplace_id not in self.ALLOWED_MARKETPLACES or
                self.MARKETPLACE_REGION[self.marketplace_id] != region):
            self.marketplace_id = self.DEFAULT_MARKETPLACE_BY_REGION[region]
        logger.info(f"SP-API region -> {region}; base={self.api_base_url}; mkt={self.marketplace_id}")

    def set_marketplace(self, mkt: str):
        if not mkt or mkt not in self.ALLOWED_MARKETPLACES:
            return
        self.marketplace_id = mkt
        self.set_region(self.MARKETPLACE_REGION[mkt])

    def get_oauth_url(self, state: str) -> str:
        base = self.SELLER_CENTRAL_BY_MKT.get(self.marketplace_id, "https://sellercentral.amazon.com")
        params = {
            "application_id": self.app_id,
            "state": state,
            "redirect_uri": self.redirect_uri,
            "version": "beta",
        }
        return f"{base}/apps/authorize/consent?{urllib.parse.urlencode(params)}"

    def exchange_auth_code_for_refresh_token(self, spapi_oauth_code: str) -> Optional[str]:
        try:
            r = requests.post(
                self.TOKEN_URL,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                data={
                    "grant_type": "authorization_code",
                    "code": spapi_oauth_code,
                    "client_id": self.client_id,
                    "client_secret": self.client_secret,
                    "redirect_uri": self.redirect_uri,
                },
                timeout=30,
            )
            r.raise_for_status()
            data = r.json()
            refresh = data.get("refresh_token")
            if refresh:
                self.refresh_token = refresh.strip()
                with open(".refresh_token", "w") as f:
                    f.write(self.refresh_token)
                logger.info("Saved refresh token to .refresh_token")
                return self.refresh_token
            logger.error(f"Auth code exchange ok but no refresh_token: {data}")
        except Exception as e:
            logger.error(f"Auth code exchange failed: {e}")
        return None

    def get_access_token(self):
        if self._access_token and self._token_expires_at and datetime.utcnow() < self._token_expires_at:
            return self._access_token
        if not self.refresh_token:
            logger.error("No refresh token present.")
            return None
        r = requests.post(
            self.TOKEN_URL,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            data={
                "grant_type": "refresh_token",
                "refresh_token": self.refresh_token,
                "client_id": self.client_id,
                "client_secret": self.client_secret,
            },
            timeout=30,
        )
        if r.status_code != 200:
            try:
                logger.error(f"LWA token fetch failed: {r.status_code} {r.json()}")
            except Exception:
                logger.error(f"LWA token fetch failed: {r.status_code} {r.text}")
            return None
        data = r.json()
        self._access_token = data["access_token"]
        self._token_expires_at = datetime.utcnow() + timedelta(seconds=int(data.get("expires_in", 3600)) - 120)
        return self._access_token

    def sign_request(self, method: str, url: str, headers: Dict[str, str], body: str = "") -> Dict[str, str]:
        hdrs = {k: v for k, v in headers.items() if v is not None}
        if method.upper() == "GET":
            hdrs.pop("content-type", None)
        aws_req = AWSRequest(method=method.upper(), url=url, data=body.encode() if body else None, headers=hdrs)
        creds = self.session.get_credentials().get_frozen_credentials()
        SigV4Auth(creds, "execute-api", self.region).add_auth(aws_req)
        return dict(aws_req.headers)

    def make_api_call(
        self,
        endpoint: str,
        method: str = "GET",
        params: Optional[Dict[str, Any]] = None,
        data: Optional[Dict[str, Any]] = None,
        max_retries: int = 3,
    ) -> Dict[str, Any]:
        token = self.get_access_token()
        if not token:
            return {"error": "No access token"}

        base = f"{self.api_base_url}{endpoint}"
        qs = urllib.parse.urlencode(params or {}, doseq=True, safe=":,")
        url = f"{base}?{qs}" if qs else base

        headers = {
            "host": urllib.parse.urlparse(url).netloc,
            "x-amz-access-token": token,
            "user-agent": f"YourApp/1.0 (Python; region={self.region})",
        }
        body = json.dumps(data) if data else ""
        if method.upper() != "GET":
            headers["content-type"] = "application/json"

        def _format_error_response(resp: requests.Response) -> Dict[str, Any]:
            try:
                j = resp.json()
            except Exception:
                j = None
            return {
                "error": "UpstreamError",
                "status_code": resp.status_code,
                "amzn_error_type": resp.headers.get("x-amzn-ErrorType"),
                "amzn_request_id": resp.headers.get("x-amzn-RequestId") or resp.headers.get("x-amz-request-id"),
                "response_json": j,
                "response_text": (None if j is not None else resp.text),
                "method": method.upper(),
                "url": url,
                "endpoint": endpoint,
                "params": params,
                "data": data,
            }

        for attempt in range(1, max_retries + 1):
            try:
                signed = self.sign_request(method, url, headers, body)
                if method.upper() == "GET":
                    resp = requests.get(url, headers=signed, timeout=30)
                elif method.upper() == "DELETE":
                    resp = requests.delete(url, headers=signed, timeout=30)
                else:
                    resp = requests.post(url, headers=signed, data=body, timeout=30)

                if resp.status_code in (429, 503) and attempt < max_retries:
                    time.sleep(2 ** attempt)
                    continue

                if resp.status_code in (401, 403) and attempt == 1:
                    self._access_token = None
                    headers["x-amz-access-token"] = self.get_access_token() or ""
                    continue

                if 200 <= resp.status_code < 300:
                    try:
                        return resp.json()
                    except Exception:
                        return {"status_code": resp.status_code, "ok": True}

                err = _format_error_response(resp)
                logger.warning(
                    "SP-API %s %s -> %s | %s",
                    method.upper(), endpoint, resp.status_code,
                    err.get("response_json") or err.get("response_text"),
                )
                return err

            except requests.RequestException as e:
                if attempt == max_retries:
                    return {"error": "RequestException", "message": str(e), "method": method.upper(), "url": url}
                time.sleep(2 ** attempt)

        return {"error": "Unknown"}
     
    


amazon_client = AmazonSPAPIClient()




COLUMN_MAPPING = {
    'date/time': 'date_time',
    'settlement id': 'settlement_id',
    'type': 'type',
    'order id': 'order_id',
    'sku': 'sku',
    'description': 'description',
    'quantity': 'quantity',
    'marketplace': 'marketplace',
    'fulfilment': 'fulfilment',
    'fulfillment': 'fulfillment',
    'order city': 'order_city',
    'order state': 'order_state',
    'order postal': 'order_postal',
    'tax collection model': 'tax_collection_model',
    'product sales': 'product_sales',
    'product sales tax': 'product_sales_tax',
    'postage credits': 'postage_credits',
    'shipping credits tax': 'shipping_credits_tax',
    'gift wrap credits': 'gift_wrap_credits',
    'giftwrap credits tax': 'giftwrap_credits_tax',
    'promotional rebates': 'promotional_rebates',
    'promotional rebates tax': 'promotional_rebates_tax',
    'sales tax collected': 'sales_tax_collected',
    'marketplace withheld tax': 'marketplace_withheld_tax',
    'marketplace facilitator tax': 'marketplace_facilitator_tax',
    'selling fees': 'selling_fees',
    'fba fees': 'fba_fees',
    'other transaction fees': 'other_transaction_fees',
    'other': 'other',
    'total': 'total',
    'account type': 'account_type',
    'Regulatory Fee': 'regulatory_fee',
    'Tax On Regulatory Fee': 'tax_on_regulatory_fee',
    'Bucket': 'bucket',
    'shipping credits': 'shipping_credits',
    'regulatory fee': 'regulatory_fee',
    'tax on regulatory fee': 'tax_on_regulatory_fee',

}

MONTHS_REVERSE_MAP = {
    1: "january", 2: "february", 3: "march", 4: "april", 5: "may", 6: "june",
    7: "july", 8: "august", 9: "september", 10: "october", 11: "november", 12: "december"
}


MONTHS_MAP = {
    'january': 1, 'february': 2, 'march': 3, 'april': 4,
    'may': 5, 'june': 6, 'july': 7, 'august': 8,
    'september': 9, 'october': 10, 'november': 11, 'december': 12
}


MONTH_NAME = {i: calendar.month_name[i].lower() for i in range(1, 13)}

# =========================================================
# OUTPUT COLUMNS (MATCH YOUR MTD FILE)
# =========================================================
MTD_COLUMNS = [
    "date_time", "settlement_id", "type", "order_id", "sku", "description", "quantity",
    "marketplace", "fulfilment", "order_city", "order_state", "order_postal",
    "tax_collection_model",
    "product_sales", "product_sales_tax", "postage_credits", "shipping_credits",
    "shipping_credits_tax", "gift_wrap_credits", "giftwrap_credits_tax",
    "promotional_rebates", "promotional_rebates_tax",
    "sales_tax_collected", "marketplace_withheld_tax", "marketplace_facilitator_tax",
    "selling_fees", "fba_fees", "other_transaction_fees", "other", "total",
    "account_type", "regulatory_fee", "tax_on_regulatory_fee", "bucket",
]

def dedupe_rows_by_order_id(
    rows: list[dict],
) -> list[dict]:
    if not rows:
        return rows

    status_priority = {
        "DEFERRED_RELEASED": 3,
        "RELEASED": 2,
        "DEFERRED": 1,
    }

    def normalize_text(value: Any) -> str:
        return str(value or "").strip().casefold()

    def normalize_number(value: Any) -> float:
        try:
            if value is None or value == "":
                return 0.0

            if isinstance(value, str):
                value = value.replace(",", "").strip()

            return round(float(value), 6)

        except (TypeError, ValueError):
            return 0.0

    def get_status(row: dict) -> str:
        return str(
            row.get("transaction_status")
            or row.get("status")
            or row.get("bucket")
            or ""
        ).strip().upper()

    def get_posted_date(row: dict) -> pd.Timestamp:
        """
        Used only as a final tie-breaker when two rows have the same status.
        """
        try:
            parsed = pd.to_datetime(
                row.get("date_time"),
                utc=True,
                errors="coerce",
            )

            if pd.isna(parsed):
                return pd.Timestamp.min.tz_localize("UTC")

            return parsed

        except Exception:
            return pd.Timestamp.min.tz_localize("UTC")

    best_by_key: dict[tuple, dict] = {}
    untouched_rows: list[dict] = []

    for row in rows:
        row_type = normalize_text(
            row.get("type")
            or row.get("transaction_type")
        )

        # Dedupe Order, Shipment and Refund financial rows.
        # Amazon can return the same Refund in RELEASED and
        # DEFERRED_RELEASED buckets; keeping both inflates return quantity.
        # ServiceFee, Transfer, Adjustment and other non-order transactions
        # remain untouched.
        if row_type not in {"order", "shipment", "refund"}:
            untouched_rows.append(row)
            continue

        order_id = str(
            row.get("order_id") or ""
        ).strip()

        # Order/Shipment/Refund rows without an order ID must remain.
        if not order_id:
            untouched_rows.append(row)
            continue

        # This key identifies the same financial Order/Shipment/Refund event.
        # Date and status are intentionally excluded.
        key = (
            order_id,
            str(row.get("sku") or "").strip().upper(),
            row_type,
            normalize_text(row.get("description")),
            normalize_number(row.get("quantity")),

            normalize_number(row.get("product_sales")),
            normalize_number(row.get("product_sales_tax")),

            normalize_number(
                row.get("postage_credits")
                or row.get("shipping_credits")
            ),
            normalize_number(row.get("shipping_credits_tax")),

            normalize_number(row.get("gift_wrap_credits")),
            normalize_number(row.get("giftwrap_credits_tax")),

            normalize_number(row.get("promotional_rebates")),
            normalize_number(row.get("promotional_rebates_tax")),

            normalize_number(row.get("marketplace_withheld_tax")),
            normalize_number(row.get("marketplace_facilitator_tax")),

            normalize_number(row.get("selling_fees")),
            normalize_number(row.get("fba_fees")),
            normalize_number(row.get("other_transaction_fees")),
            normalize_number(row.get("other")),
            normalize_number(row.get("total")),
        )

        existing_row = best_by_key.get(key)

        if existing_row is None:
            best_by_key[key] = row
            continue

        current_status = get_status(row)
        existing_status = get_status(existing_row)

        current_priority = status_priority.get(
            current_status,
            0,
        )
        existing_priority = status_priority.get(
            existing_status,
            0,
        )

        # Prefer DEFERRED_RELEASED, then RELEASED, then DEFERRED.
        if current_priority > existing_priority:
            best_by_key[key] = row
            continue

        # Same status: keep the latest posted row.
        if (
            current_priority == existing_priority
            and get_posted_date(row)
            > get_posted_date(existing_row)
        ):
            best_by_key[key] = row

    return list(best_by_key.values()) + untouched_rows


def get_next_month_year(month, year):
    year = int(year)
    month_num = MONTHS_MAP[month]
    if month_num == 12:
        month_num = 1
        month_next = MONTHS_REVERSE_MAP[month_num]
       

        return month_next, year + 1
    month_next = month_num + 1
    month_next1 = MONTHS_REVERSE_MAP[month_next]
    

    return month_next1, year

def table_exists(engine, table_name, schema='public'):
    inspector = inspect(engine)
    return inspector.has_table(table_name, schema=schema)


# ------------------------------------------------- Helpers -------------------------------------------------
def _apply_region_and_marketplace_from_request():
    mkt = request.args.get("marketplace_id")
    if mkt:
        amazon_client.set_marketplace(mkt)
    region = request.args.get("region")
    if region:
        amazon_client.set_region(region)


def _parse_settlement_tsv(tsv_bytes: bytes) -> list[dict]:
    text = tsv_bytes.decode("utf-8", errors="replace")
    lines = text.splitlines()
    if not lines:
        return []
    headers = [h.strip() for h in lines[0].split("\t")]
    out = []
    for ln in lines[1:]:
        if not ln.strip():
            continue
        cols = ln.split("\t")
        row = {headers[i]: cols[i] if i < len(cols) else "" for i in range(len(headers))}
        mapped = {
            "date/time": row.get("transaction-date") or row.get("posted-date") or row.get("settlement-start-date"),
            "settlement id": row.get("settlement-id"),
            "type": row.get("transaction-type") or row.get("type"),
            "order id": row.get("order-id"),
            "sku": row.get("sku"),
            "description": row.get("product-name"),
            "quantity": row.get("quantity-purchased") or row.get("quantity"),
            "marketplace": row.get("marketplace-name") or row.get("marketplace"),
            "fulfilment": row.get("fulfillment-id") or row.get("fulfillment"),
            "order city": row.get("ship-city"),
            "order state": row.get("ship-state"),
            "order postal": row.get("ship-postal-code"),
            "tax collection model": row.get("tax-collection-model"),
            "product sales": row.get("total-amount") or row.get("product-sales"),
            "product sales tax": row.get("product-sales-tax"),
            "postage credits": row.get("shipping-credits"),
            "shipping credits tax": row.get("shipping-credits-tax"),
            "gift wrap credits": row.get("gift-wrap-credits"),
            "giftwrap credits tax": row.get("gift-wrap-credits-tax"),
            "promotional rebates": row.get("promotional-rebates"),
            "promotional rebates tax": row.get("promotional-rebates-tax"),
            "marketplace withheld tax": row.get("marketplace-withheld-tax"),
            "marketplace facilitator tax":row.get("marketplace-withheld-tax"),
            "selling fees": row.get("selling-fees"),
            "fba fees": row.get("fba-fees"),
            "other transaction fees": row.get("other-transaction-fees"),
            "other": row.get("other"),
            "total": row.get("total") or row.get("total-amount"),
        }
        out.append(mapped)
    return out

def _to_decimal(v):
    try:
        if v is None or v == "":
            return None
        return Decimal(str(v))
    except InvalidOperation:
        return None

def _parse_dt_any(v):
    if not v:
        return None
    s = str(v).strip().replace("\u00A0", " ")

    # 1) ISO with trailing Z, with/without fractional
    for fmt in ("%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S.%fZ"):
        try:
            return datetime.strptime(s, fmt)
        except Exception:
            pass

    # 2) Plain space instead of 'T'
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M:%S.%f"):
        try:
            return datetime.strptime(s, fmt)
        except Exception:
            pass

    # 3) RFC3339 with offset (+HH:MM), with/without fractional seconds
    try:
        # Normalize ending ...+HH:MM or -HH:MM -> ...+HHMM for strptime
        if len(s) >= 6 and s[-3] == ":" and s[-6] in "+-":
            s2 = s[:-3] + s[-2:]
            # try with fractional
            try:
                dt = datetime.strptime(s2, "%Y-%m-%dT%H:%M:%S.%f%z")
            except Exception:
                dt = datetime.strptime(s2, "%Y-%m-%dT%H:%M:%S%z")
            # strip tz -> naive UTC
            return dt.astimezone(datetime.timezone.utc).replace(tzinfo=None)
    except Exception:
        pass

    # 4) Settlement formats like "DD.MM.YYYY" or "DD.MM.YYYY HH:MM:SS UTC"
    for fmt in ("%d.%m.%Y", "%d.%m.%Y %H:%M:%S UTC"):
        try:
            return datetime.strptime(s, fmt)
        except Exception:
            pass

    # 5) Fallback: just a date
    for fmt in ("%Y-%m-%d",):
        try:
            return datetime.strptime(s, fmt)
        except Exception:
            pass

    return None


def _extract_addr_field(addr: dict | None, *keys: str) -> Optional[str]:
    if not addr:
        return None
    for k in keys:
        v = addr.get(k)
        if v:
            return str(v)
    return None



def run_upload_pipeline_from_df(
    df_raw: pd.DataFrame,
    *,
    user_id: int,
    country: str,
    month_num: str,
    year: str,
    db_url: str | None = None,
    db_url_aux: str | None = None,
    profile_id: str | None = None,
) -> dict:
    

    # ---------------------------
    # ENV DB URL FALLBACKS
    # ---------------------------
    if not db_url:
        db_url = os.getenv("DATABASE_URL")
    if not db_url_aux:
        db_url_aux = os.getenv("DATABASE_ADMIN_URL") or db_url

    if not db_url:
        return {"success": False, "message": "DATABASE_URL not configured"}

    db_url_amazon = os.getenv("DATABASE_AMAZON_URL")  # amazon_db
    if not db_url_amazon:
        return {"success": False, "message": "DATABASE_AMAZON_URL not configured"}

    amazon_engine = create_engine(db_url_amazon, pool_pre_ping=True)


    # ---------------------------
    # NORMALIZE INPUTS
    # ---------------------------
    country = (country or "").strip().lower()
    year = str(year).strip()          # ✅ avoid varchar=int issue
    month_num = str(month_num).strip()

    # month name
    month = MONTH_NAME[int(month_num)]  # e.g. "october"

    engine = create_engine(db_url)
    engine1 = create_engine(db_url_aux)
    meta = MetaData()

    table_name = f"user_{user_id}_{country}_{month}{year}_data".lower()
    country_table_name = f"sku_{user_id}_data_table"
    consolidated_table_name = f"user_{user_id}_{country}_merge_data_of_all_months".lower()
    global_table_name = f"user_{user_id}_total_country_global_data".lower()
    countris_table_name = f"user_{user_id}_{country}_table"

    # ---------------------------
    # DROP MONTHLY TABLE
    # ---------------------------
    with engine.connect() as connection:
        connection.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
        connection.commit()

    # ---------------------------
    # DEFINE TABLES
    # ---------------------------
    user_monthly_data = Table(
        table_name, meta,
        Column("id", Integer, primary_key=True),
        Column("date_time", String),
        Column("settlement_id", String),
        Column("type", String),
        Column("order_id", String),
        Column("sku", String),
        Column("description", String),
        Column("quantity", Float),
        Column("price_in_gbp", Float),
        Column("cost_of_unit_sold", Float),
        Column("marketplace", String),
        Column("account_type", String),
        Column("fulfilment", String),
        Column("fulfillment", String),
        Column("order_city", String),
        Column("order_state", String),
        Column("order_postal", String),
        Column("tax_collection_model", String),
        Column("regulatory_fee", Float),
        Column("tax_on_regulatory_fee", Float),
        Column("bucket", String),
        Column("product_sales", Float),
        Column("product_sales_tax", Float),
        Column("postage_credits", Float),
        Column("shipping_credits", Float),
        Column("shipping_credits_tax", Float),
        Column("gift_wrap_credits", Float),
        Column("giftwrap_credits_tax", Float),
        Column("promotional_rebates", Float),
        Column("promotional_rebates_tax", Float),
        Column("sales_tax_collected", Float),
        Column("marketplace_withheld_tax", Float),
        Column("marketplace_facilitator_tax", Float),
        Column("selling_fees", Float),
        Column("percentage1", Float),
        Column("fba_fees", Float),
        Column("percentage2", Float),
        Column("other_transaction_fees", Float),
        Column("other", Float),
        Column("total", Float),
        Column("product_name", String),
        Column("currency", String),
        Column("advertising_cost", Float),
        Column("net_reimbursement", Float),
        Column("platform_fees", Float),
        Column("product_group", String),
    )

    user_consolidated_data = Table(
        consolidated_table_name, meta,
        Column("id", Integer, primary_key=True),
        Column("date_time", String),
        Column("settlement_id", String),
        Column("type", String),
        Column("order_id", String),
        Column("sku", String),
        Column("description", String),
        Column("quantity", Float),
        Column("price_in_gbp", Float),
        Column("cost_of_unit_sold", Float),
        Column("marketplace", String),
        Column("account_type", String),
        Column("fulfilment", String),
        Column("fulfillment", String),
        Column("order_city", String),
        Column("order_state", String),
        Column("order_postal", String),
        Column("tax_collection_model", String),
        Column("regulatory_fee", Float),
        Column("tax_on_regulatory_fee", Float),
        Column("bucket", String),
        Column("product_sales", Float),
        Column("product_sales_tax", Float),
        Column("postage_credits", Float),
        Column("shipping_credits", Float),
        Column("shipping_credits_tax", Float),
        Column("gift_wrap_credits", Float),
        Column("giftwrap_credits_tax", Float),
        Column("promotional_rebates", Float),
        Column("promotional_rebates_tax", Float),
        Column("sales_tax_collected", Float),
        Column("marketplace_withheld_tax", Float),
        Column("marketplace_facilitator_tax", Float),
        Column("selling_fees", Float),
        Column("percentage1", Float),
        Column("fba_fees", Float),
        Column("percentage2", Float),
        Column("other_transaction_fees", Float),
        Column("other", Float),
        Column("total", Float),
        Column("month", String),
        Column("year", String),
        Column("product_name", String),
        Column("currency", String),
        Column("advertising_cost", Float),
        Column("net_reimbursement", Float),
        Column("platform_fees", Float),
        Column("product_group", String),
        Column('errorstatus', String),
        # apply_modifications_fatch added/used columns
        Column("referral_fee", Float),
        Column("total_value", Float),
        Column("answer", Float),
        Column("difference", Float),
        Column("fbaanswer", Float),
        Column("fbaerrorstatus", String),
        


    )

    user_global_table = Table(
        global_table_name, meta,
        Column("id", Integer, primary_key=True),
        Column("date_time", String),
        Column("settlement_id", String),
        Column("type", String),
        Column("order_id", String),
        Column("sku", String),
        Column("description", String),
        Column("quantity", Float),
        Column("price_in_gbp", Float),
        Column("cost_of_unit_sold", Float),
        Column("marketplace", String),
        Column("fulfilment", String),
        Column("fulfillment", String),
        Column("order_city", String),
        Column("order_state", String),
        Column("order_postal", String),
        Column("tax_collection_model", String),
        Column("product_sales", Float),
        Column("product_sales_tax", Float),
        Column("postage_credits", Float),
        Column("shipping_credits", Float),
        Column("shipping_credits_tax", Float),
        Column("gift_wrap_credits", Float),
        Column("giftwrap_credits_tax", Float),
        Column("promotional_rebates", Float),
        Column("promotional_rebates_tax", Float),
        Column("sales_tax_collected", Float),
        Column("marketplace_withheld_tax", Float),
        Column("marketplace_facilitator_tax", Float),
        Column("selling_fees", Float),
        Column("percentage1", Float),
        Column("fba_fees", Float),
        Column("percentage2", Float),
        Column("other_transaction_fees", Float),
        Column("other", Float),
        Column("total", Float),
        Column("month", String),
        Column("year", String),
        Column("product_name", String),
        Column("country", String),
        extend_existing=True
    )

    meta.create_all(engine)

    # ---------------------------
    # ✅ FIXED DELETES (Table object -> table name)
    # ---------------------------
    with engine.connect() as connection:
        connection.execute(
            text(f"DELETE FROM {consolidated_table_name} WHERE month = :month AND year = :year"),
            {"month": month, "year": year}
        )
        connection.commit()

    with engine.connect() as connection:
        connection.execute(
            text(f"DELETE FROM {global_table_name} WHERE month = :month AND year = :year AND country = :country"),
            {"month": month, "year": year, "country": country}
        )
        connection.commit()

    # ---------------------------
    # CLEAN/REMAP
    # ---------------------------
    df = df_raw.copy()

    df.columns = [
        str(c).strip().lower().replace("-", " ").replace("_", " ")
        for c in df.columns
    ]

    df.rename(columns=COLUMN_MAPPING, inplace=True)

    # Convert any remaining spaced names to snake_case for DB columns
    df.columns = [
        str(c).strip().lower().replace(" ", "_").replace("/", "_")
        for c in df.columns
    ]


    if "product_sales_tax" not in df.columns:
        df["product_sales_tax"] = 0.0

    df["product_sales_tax"] = pd.to_numeric(
        df["product_sales_tax"],
        errors="coerce"
    ).fillna(0.0)

    if "marketplace_withheld_tax" not in df.columns:
        df["marketplace_withheld_tax"] = 0
    if "marketplace_facilitator_tax" not in df.columns:
        df["marketplace_facilitator_tax"] = 0

    df["marketplace_withheld_tax"] = pd.to_numeric(df["marketplace_withheld_tax"], errors="coerce").fillna(0)
    df["marketplace_facilitator_tax"] = pd.to_numeric(df["marketplace_facilitator_tax"], errors="coerce")

    mask_fac_missing = df["marketplace_facilitator_tax"].isna() | (df["marketplace_facilitator_tax"] == 0)
    df.loc[mask_fac_missing, "marketplace_facilitator_tax"] = df["marketplace_withheld_tax"]

    def clean_numeric_value(val):
        if isinstance(val, str):
            if "," in val:
                val = val.replace(",", "")
            try:
                return float(val)
            except ValueError:
                return None
        return val

    numeric_columns = [
        "quantity", "price_in_gbp", "cost_of_unit_sold", "product_sales", "product_sales_tax",
        "postage_credits", "shipping_credits_tax", "gift_wrap_credits", "giftwrap_credits_tax",
        "promotional_rebates", "promotional_rebates_tax", "sales_tax_collected",
        "marketplace_withheld_tax", "marketplace_facilitator_tax", "selling_fees",
        "percentage1", "fba_fees", "percentage2", "other_transaction_fees", "other", "total",
        "advertising_cost", "net_reimbursement", "platform_fees",
    ]
    numeric_columns = [c for c in numeric_columns if c in df.columns]

    for c in numeric_columns:
        df[c] = df[c].apply(clean_numeric_value)
        df[c] = pd.to_numeric(df[c], errors="coerce")

    for col in COLUMN_MAPPING.values():
        if col not in df.columns:
            df[col] = 0.0 if col in numeric_columns else None

    # ---------------------------
    # ✅ FIXED SKU COLUMN LOGIC (use lowercase only)
    # ---------------------------
    if country == "uk":
        sku_column = "sku_uk"
    elif country == "us":
        sku_column = "sku_us"
    else:
        raise ValueError("Unsupported country")

    # Join SKU data
    # with engine.connect() as conn:
    #     country_df = pd.read_sql(
    #         f"SELECT {sku_column} AS sku, price, currency, product_name FROM {country_table_name}",
    #         conn
    #     )

    # df = df.merge(country_df, on="sku", how="left")

    MONTH_NUM = {
        "january": 1, "february": 2, "march": 3, "april": 4,
        "may": 5, "june": 6, "july": 7, "august": 8,
        "september": 9, "october": 10, "november": 11, "december": 12
    }

    target_month_num = MONTH_NUM.get(month.lower())
    if not target_month_num:
        raise ValueError(f"Invalid month: {month}. Expected january..december")

    target_year = int(year)  # form year string -> int

    month_case_sql = """
    CASE lower(month)
        WHEN 'january' THEN 1
        WHEN 'february' THEN 2
        WHEN 'march' THEN 3
        WHEN 'april' THEN 4
        WHEN 'may' THEN 5
        WHEN 'june' THEN 6
        WHEN 'july' THEN 7
        WHEN 'august' THEN 8
        WHEN 'september' THEN 9
        WHEN 'october' THEN 10
        WHEN 'november' THEN 11
        WHEN 'december' THEN 12
        ELSE NULL
    END
    """

    # year is string in DB, so cast it for comparisons
    price_asof_query = text(f"""
        SELECT sku, price, currency, product_name
        FROM (
            SELECT
                {sku_column} AS sku,
                price,
                currency,
                product_name,
                CAST(year AS INTEGER) AS year_int,
                {month_case_sql} AS month_num,
                ROW_NUMBER() OVER (
                    PARTITION BY {sku_column}
                    ORDER BY CAST(year AS INTEGER) DESC, {month_case_sql} DESC
                ) AS rn
            FROM {country_table_name}
            WHERE
                year IS NOT NULL
                AND trim(year) <> ''
                AND {month_case_sql} IS NOT NULL
                AND (
                    CAST(year AS INTEGER) < :target_year
                    OR (CAST(year AS INTEGER) = :target_year AND {month_case_sql} <= :target_month_num)
                )
        ) x
        WHERE x.rn = 1
    """)

    with engine.connect() as conn:
        country_df = pd.read_sql(
            price_asof_query,
            conn,
            params={"target_year": target_year, "target_month_num": target_month_num}
        )

    df = df.merge(country_df, on="sku", how="left")




    # with engine.connect() as conn:
    #     countries_df = pd.read_sql(f"SELECT sku, product_group FROM {countris_table_name}", conn)

    # df = df.merge(countries_df, on="sku", how="left")

    from sqlalchemy import bindparam

    # sku list jo pipeline me aa rahi hai
    sku_list = df["sku"].dropna().astype(str).str.strip().unique().tolist()

    inv_pg_sql = text("""
        SELECT sku, product_group
        FROM (
            SELECT
                sku,
                "product-group" AS product_group,
                ROW_NUMBER() OVER (
                    PARTITION BY sku
                    ORDER BY "snapshot-date" DESC NULLS LAST, id DESC
                ) AS rn
            FROM inventory_aged
            WHERE user_id = :user_id
            AND sku IN :skus
        ) t
        WHERE t.rn = 1
    """).bindparams(bindparam("skus", expanding=True))

    with amazon_engine.connect() as conn:
        inv_pg_df = pd.read_sql(inv_pg_sql, conn, params={"user_id": user_id, "skus": sku_list})

    # merge (amazon inventory_aged product_group)

    df = df.merge(inv_pg_df, on="sku", how="left")

    def normalize_product_group(v):
        if v is None:
            return None
        s = str(v).strip()
        if not s:
            return None

        s_low = s.lower()

        # examples:
        # "gl_beauty" -> "beauty"
        # "GL_BEAUTY_something" -> "beauty"
        if "gl_" in s_low:
            part = s_low.split("gl_", 1)[1]
            # split by underscore / space, take first word
            part = re.split(r"[_\s]+", part)[0]
            return part

        # if already like "beauty" -> keep first token (optional)
        return re.split(r"[_\s]+", s_low)[0]

    df["product_group"] = df["product_group"].apply(normalize_product_group)



    if "currency" not in df.columns and "currency_x" in df.columns:
        df["currency"] = df.pop("currency_x")

    # ---------------------------
    # ✅ SAFE CURRENCY VALUE (avoid iloc[0] crash)
    # ---------------------------
    currency_value = "usd"
    try:
        if "currency" in country_df.columns and not country_df["currency"].dropna().empty:
            currency_value = str(country_df["currency"].dropna().iloc[0]).lower()
    except Exception:
        currency_value = "usd"

    # conversion lookup
    with engine1.connect() as conn:
        currency_query = text("""
            SELECT conversion_rate
            FROM currency_conversion 
            WHERE lower(user_currency) = :currency 
            AND lower(selected_currency) = :selected_currency
            AND lower(country) = :country 
            AND lower(month) = :month 
            AND year = :year
            ORDER BY id DESC
            LIMIT 1
        """)

        selected_currency = "gbp" if country.lower() == "uk" else "usd"
        conversion_country = "uk" if country.lower() == "uk" else "us"

        result = conn.execute(currency_query, {
            "currency": currency_value,
            "selected_currency": selected_currency,
            "country": conversion_country,
            "month": month.lower(),
            "year": year
        }).fetchone()

    conversion_rate = result[0] if result else None

    # ✅ SAFE: price may not exist
    if conversion_rate and "price" in df.columns:
        df["price_in_gbp"] = pd.to_numeric(df["price"], errors="coerce") * float(conversion_rate)
    else:
        if "price_in_gbp" not in df.columns:
            df["price_in_gbp"] = None

    df["quantity"] = pd.to_numeric(df.get("quantity"), errors="coerce")

    df["cost_of_unit_sold"] = df.apply(
        lambda row: row["price_in_gbp"] * row["quantity"]
        if pd.notnull(row.get("price_in_gbp")) and pd.notnull(row.get("quantity"))
        else None,
        axis=1
    )

    if "price" in df.columns:
        df.drop(columns=["price"], inplace=True)

    # Remove commas in object columns (SAFE: does NOT convert None/NaN to "None"/"nan")
    for col in df.columns:
        if df[col].dtype == "object":
            df[col] = df[col].apply(
                lambda x: x.replace(",", "") if isinstance(x, str) else x
            )

    # Keep numeric coercion logic
    df = df.apply(
        lambda x: pd.to_numeric(x, errors="ignore") if x.dtype == "object" else x
    )

    # ✅ Force selling_fees to always be negative before saving raw monthly table
    if "selling_fees" in df.columns:
        df["selling_fees"] = -pd.to_numeric(
            df["selling_fees"],
            errors="coerce"
        ).fillna(0).abs()

    # consolidated
    # df_cons = df.copy()
    # df_cons["month"] = month
    # df_cons["year"] = year

    # valid_cols = [c.name for c in user_consolidated_data.columns if c.name != "id"]
    # df_cons = df_cons.reindex(columns=valid_cols)

    # df_cons.to_sql(consolidated_table_name, con=engine, if_exists="append", index=False)

    df.to_sql(table_name, con=engine, if_exists="append", index=False)

    df["month"] = month
    df["year"] = year

    df = df.apply(
        lambda x: pd.to_numeric(x, errors="ignore") if x.dtype == "object" else x
    )

    # ✅ Keep selling_fees negative after adding month/year also
    if "selling_fees" in df.columns:
        df["selling_fees"] = -pd.to_numeric(
            df["selling_fees"],
            errors="coerce"
        ).fillna(0).abs()
    # ---------------------------
    # SAFE CURRENCY HANDLING (your block kept)
    # ---------------------------
    if country.lower() == "uk":
        currency1 = "gbp"
    elif country.lower() == "us":
        currency1 = "usd"
    elif country.lower() == "canada":
        currency1 = "cad"
    else:
        currency1 = "usd"

    with engine1.connect() as conn:
        currency_query = text("""
            SELECT conversion_rate
            FROM currency_conversion 
            WHERE lower(user_currency) = :currency1
            AND lower(selected_currency) = 'usd'
            AND lower(country) = 'us'
            AND lower(month) = :month 
            AND year = :year
            LIMIT 1
        """)
        result = conn.execute(currency_query, {
            "currency1": currency1,
            "month": month.lower(),
            "year": year
        }).fetchone()

    currency_rate = float(result[0]) if result and result[0] not in [None, 0, ""] else None

    df_usd = df.copy()

    if currency_rate:
        monetary_columns = [
            "product_sales", "product_sales_tax", "postage_credits",
            "shipping_credits_tax", "gift_wrap_credits", "giftwrap_credits_tax",
            "promotional_rebates", "promotional_rebates_tax", "sales_tax_collected",
            "marketplace_facilitator_tax", "selling_fees", "fba_fees",
            "other_transaction_fees", "other", "total", "price_in_gbp",
            "cost_of_unit_sold"
        ]
        for col in monetary_columns:
            if col in df_usd.columns:
                df_usd[col] = pd.to_numeric(df_usd[col], errors="coerce") * currency_rate

    for col in df_usd.columns:
        if df_usd[col].dtype == "object" and col not in [
            "date_time","settlement_id","type","order_id","sku","description",
            "marketplace","fulfilment","order_city","order_state","order_postal",
            "tax_collection_model","month","year","country","product_name"
        ]:
            df_usd[col] = pd.to_numeric(df_usd[col], errors="coerce")

    df_usd = df_usd.fillna({c: 0.0 for c in df_usd.select_dtypes(include=["float64"]).columns})
    df_usd = df_usd.fillna({c: 0 for c in df_usd.select_dtypes(include=["int64"]).columns})
    df_usd = df_usd.fillna("")

    with engine.begin() as conn:
        conn.execute(user_global_table.insert(), df_usd.to_dict(orient="records"))

    # ---------------------------
    # CONSOLIDATED BASE DF (before modifications)
    # ---------------------------
    df_cons = df.copy()
    df_cons["month"] = month
    df_cons["year"] = year


    # apply modifications
    df_modified = apply_modifications_fatch(df_cons, country)

    if "selling_fees" in df_modified.columns:
        df_modified["selling_fees"] = -pd.to_numeric(
            df_modified["selling_fees"],
            errors="coerce"
        ).fillna(0).abs()

    # preserve product_sales_tax after apply_modifications_fatch
    original_product_sales_tax = pd.to_numeric(
        df_cons.get("product_sales_tax", 0.0),
        errors="coerce"
    ).fillna(0.0)

    if "product_sales_tax" not in df_modified.columns:
        df_modified["product_sales_tax"] = original_product_sales_tax
    else:
        df_modified["product_sales_tax"] = pd.to_numeric(
            df_modified["product_sales_tax"],
            errors="coerce"
        ).fillna(0.0)

        df_modified["product_sales_tax"] = df_modified["product_sales_tax"].where(
            df_modified["product_sales_tax"] != 0,
            original_product_sales_tax
        )

    df_modified["product_sales_tax"] = pd.to_numeric(
        df_modified["product_sales_tax"],
        errors="coerce"
    ).fillna(0.0)

    # ✅ Ensure month/year exist in df_modified
    df_modified["month"] = month
    df_modified["year"] = year

    # ✅ Normalize column names (safety)
    if "ErrorStatus" in df_modified.columns and "errorstatus" not in df_modified.columns:
        df_modified.rename(columns={"ErrorStatus": "errorstatus"}, inplace=True)

    # ✅ Ensure all required new cols exist (if apply_modifications_fatch missed any)
    for col in ["answer", "difference", "errorstatus", "fbaanswer", "fbaerrorstatus", "total_value", "referral_fee"]:
        if col not in df_modified.columns:
            df_modified[col] = "" if col in ["errorstatus", "fbaerrorstatus"] else 0.0

    # ✅ Only insert columns that consolidated table supports
    valid_cols = [c.name for c in user_consolidated_data.columns if c.name != "id"]
    df_merge_insert = df_modified.reindex(columns=valid_cols)

    for col in numeric_columns:
        if col in df_modified.columns:
            df_modified[col] = pd.to_numeric(df_modified[col], errors="coerce")

    df_modified.to_sql(table_name, con=engine, if_exists="replace", index=False, method="multi")

    excel_output = io.BytesIO()
    df_modified.to_excel(excel_output, index=False)
    excel_output.seek(0)

    # ✅ ensure month/year in df_modified too
    df_modified["month"] = month
    df_modified["year"] = year

    # ✅ remove old month/year records from merge table (already doing earlier, ok)
    # ✅ now append modified data to consolidated merge table
    df_modified.to_sql(consolidated_table_name, con=engine, if_exists='append', index=False, method='multi')


    # -------- your analytics remain as-is ----------
    quarter_mapping = {
        "january": "Q1","february":"Q1","march":"Q1","april":"Q2","may":"Q2","june":"Q2",
        "july":"Q3","august":"Q3","september":"Q3","october":"Q4","november":"Q4","december":"Q4"
    }
    quarter = quarter_mapping[month]

    if country == "uk":
        total_cous, total_amazon_fee, cm2_profit, rembursement_fee, platform_fee, total_expense, total_profit, total_fba_fees, advertising_total, taxncredit, reimbursement_vs_sales, cm2_margins, acos, rembursment_vs_cm2_margins, total_sales, unit_sold, total_product_sales = \
            process_skuwise_data(user_id, country, month, year)
        ytd_pie_chart = process_yearly_skuwise_data(user_id, country, year)
        qtd_pie_chart = process_quarterly_skuwise_data(user_id, country, month, year, quarter, db_url)
    else:
        platform_fee, rembursement_fee, total_cous, total_amazon_fee, total_profit, total_expense, total_fba_fees, cm2_profit, cm2_margins, acos, rembursment_vs_cm2_margins, advertising_total, reimbursement_vs_sales, unit_sold, total_sales, otherwplatform, taxncredit, total_product_sales = \
            process_skuwise_us_data(user_id, country, month, year)
        ytd_pie_chart = process_us_yearly_skuwise_data(user_id, country, year)
        qtd_pie_chart = process_us_quarterly_skuwise_data(user_id, country, month, year, quarter, db_url)

    process_global_monthly_skuwise_data(user_id, country, year, month)
    process_global_quarterly_skuwise_data(user_id, country, month, year, quarter, db_url)
    process_global_yearly_skuwise_data(user_id, country, year)

    existing_entry = UploadHistory.query.filter_by(user_id=user_id, country=country, month=month, year=year).first()
    if existing_entry:
        db.session.delete(existing_entry)
        db.session.commit()

    new_upload = UploadHistory(
        user_id=user_id, year=year, month=month, country=country,
        file_name=f"amazon_settlement_{month}{year}.tsv",
        sales_chart_img=None, expense_chart_img=None,
        total_sales=float(total_sales), total_profit=float(total_profit),
        total_product_sales=float(total_product_sales),
        otherwplatform=platform_fee,
        taxncredit=float(taxncredit) if taxncredit is not None else 0.0,
        total_expense=float(total_expense),
        qtd_pie_chart=qtd_pie_chart, ytd_pie_chart=ytd_pie_chart,
        total_cous=float(total_cous), total_amazon_fee=float(total_amazon_fee),
        total_fba_fees=float(total_fba_fees), platform_fee=float(platform_fee),
        rembursement_fee=float(rembursement_fee), cm2_profit=float(cm2_profit),
        cm2_margins=float(cm2_margins), acos=float(acos),
        rembursment_vs_cm2_margins=float(rembursment_vs_cm2_margins),
        advertising_total=float(advertising_total),
        reimbursement_vs_sales=float(reimbursement_vs_sales),
        unit_sold=int(unit_sold)
    )
    db.session.add(new_upload)
    db.session.commit()

    def replace_nan_with_null(v):
        if isinstance(v, dict):
            return {k: replace_nan_with_null(x) for k, x in v.items()}
        if isinstance(v, list):
            return [replace_nan_with_null(x) for x in v]
        try:
            import math
            return None if (isinstance(v, float) and math.isnan(v)) else v
        except Exception:
            return v

    response_data = {
        "success": True,
        "total_sales": total_sales,
        "total_product_sales": total_product_sales,
        "total_profit": total_profit,
        "otherwplatform": platform_fee,
        "taxncredit": taxncredit,
        "total_expense": total_expense,
        "total_fba_fees": total_fba_fees,
        "excel_file": base64.b64encode(excel_output.getvalue()).decode(),
        "platform_fee": platform_fee,
    }
    return replace_nan_with_null(response_data)



# ------------------------------ SKUs (FBA inventory) ------------------------------
def _fetch_fba_skus_all(mp: str) -> list[str]:
    skus: list[str] = []

    params = {
        "granularityType": "Marketplace",
        "granularityId": mp,
        "marketplaceIds": [mp],
        "details": "false",
        "pageSize": 100
    }

    res = amazon_client.make_api_call("/fba/inventory/v1/summaries", "GET", params)
    if not res or "error" in res:
        return []

    payload = res.get("payload") or res
    summaries = (payload.get("inventorySummaries") or [])
    for s in summaries:
        sku = s.get("sellerSku")
        if sku:
            skus.append(sku)

    nxt = payload.get("pagination", {}).get("nextToken")
    while nxt:
        page = amazon_client.make_api_call(
            "/fba/inventory/v1/summaries", "GET",
            {"nextToken": nxt, "marketplaceIds": [mp]}
        )
        p2 = page.get("payload") or page
        for s in (p2.get("inventorySummaries") or []):
            sku = s.get("sellerSku")
            if sku:
                skus.append(sku)
        nxt = p2.get("pagination", {}).get("nextToken")

    return list(dict.fromkeys(skus))


def _upsert_products_to_db(skus_data, marketplace_id, user_id=None) -> int:
    """Build rows and upsert into products table on (sku, marketplace_id)."""
    if not skus_data:
        return 0

    rows = []
    for sku in skus_data:
        if not sku:
            continue
        rows.append({
            "user_id": user_id,
            "sku": sku,
            "marketplace_id": marketplace_id,
            "status": "Active",
            "product_type": "FBA",
            "synced_at": datetime.utcnow(),
        })

    stmt = insert(Product).values(rows)
    # IMPORTANT: reference the *existing* unique constraint
    stmt = stmt.on_conflict_do_update(
        constraint='uq_products_sku_mkt',
        set_={
            "user_id": stmt.excluded.user_id,
            "status": stmt.excluded.status,
            "product_type": stmt.excluded.product_type,
            "synced_at": stmt.excluded.synced_at,
            "updated_at": datetime.utcnow(),
        },
    )

    try:
        db.session.execute(stmt)
        db.session.commit()
        return len(rows)
    except Exception as e:
        db.session.rollback()
        logger.error(f"Failed to upsert products: {e}")
        raise


# -------------------------------------------------------
# 1) Get sellerId (Correct way for your account)
# -------------------------------------------------------
def _get_seller_id() -> str | None:
    """
    Correct sellerId fetch:
    GET /sellers/v1/sellers
    Response shape (typical):
      {"payload":[{"sellerId":"A1XXXX", ...}]}
    """
    res = amazon_client.make_api_call("/sellers/v1/sellers", "GET")

    if not res or "error" in res:
        logger.warning(f"Failed to get sellers: {res}")
        return None

    payload = res.get("payload")
    if isinstance(payload, list) and payload:
        seller_id = payload[0].get("sellerId")
        if seller_id:
            return seller_id

    # fallback: sometimes payload can be dict
    if isinstance(payload, dict):
        seller_id = payload.get("sellerId")
        if seller_id:
            return seller_id

    logger.warning(f"sellerId not found in /sellers/v1/sellers response: {res}")
    return None


# -------------------------------------------------------
# 2) Fetch createdDate using getListingsItem
# -------------------------------------------------------
def _fetch_listings_open_dates_by_sku(
    skus: list[str],
    marketplace_id: str
) -> dict[str, dict]:
    """
    Returns:
      { sku: {"asin": "...", "createdDate": "..."} }
    """
    seller_id = _get_seller_id()
    if not seller_id:
        return {}

    out: dict[str, dict] = {}

    for sku in skus:
        if not sku:
            continue

        sku_encoded = urllib.parse.quote(sku, safe="")
        endpoint = f"/listings/2021-08-01/items/{seller_id}/{sku_encoded}"
        params = {
            "marketplaceIds": marketplace_id,
            "includedData": "summaries",
        }

        resp = amazon_client.make_api_call(endpoint, "GET", params=params)

        if not resp or "error" in resp:
            logger.warning(f"getListingsItem failed for sku={sku}: {resp}")
            continue

        summaries = resp.get("summaries") or []
        s0 = summaries[0] if summaries else {}

        out[sku] = {
            "asin": s0.get("asin"),
            "createdDate": s0.get("createdDate"),
        }

    return out


# -------------------------------------------------------
# 3) Upsert + store Product.open_date
# -------------------------------------------------------
def _upsert_products_to_db_with_open_date(
    skus: list[str],
    marketplace_id: str,
    user_id: int | None
) -> int:
    if not skus:
        return 0

    listings_map = _fetch_listings_open_dates_by_sku(skus, marketplace_id)

    saved = 0
    for sku in skus:
        listing = listings_map.get(sku, {})
        created_date_str = listing.get("createdDate")

        open_date = None
        if created_date_str:
            try:
                open_date = parser.isoparse(created_date_str)
            except Exception:
                open_date = None

        product = Product.query.filter_by(sku=sku, marketplace_id=marketplace_id).first()
        if not product:
            product = Product(
                sku=sku,
                marketplace_id=marketplace_id,
                user_id=user_id,
                status="Active",
                product_type="FBA",
            )

        asin = listing.get("asin")
        if asin:
            product.asin = asin

        product.open_date = open_date
        product.synced_at = datetime.utcnow()
        product.updated_at = datetime.utcnow()

        db.session.add(product)
        saved += 1

    db.session.commit()
    return saved



# =========================================================
# DATE RANGE
# =========================================================
def _month_date_range_utc(year: int, month: int) -> Tuple[str, str]:
    start = datetime(year, month, 1, 0, 0, 0, tzinfo=timezone.utc)
    if month == 12:
        end = datetime(year + 1, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    else:
        end = datetime(year, month + 1, 1, 0, 0, 0, tzinfo=timezone.utc)

    def iso_z(dt: datetime) -> str:
        return dt.isoformat().replace("+00:00", "Z")

    return iso_z(start), iso_z(end)

def _month_date_range_us_pacific_utc(year: int, month: int) -> tuple[str, str]:
    """
    US marketplace month range using Pacific local month boundary,
    converted to UTC for Amazon SP-API.

    Example May 2026:
      2026-05-01 00:00 PDT -> 2026-05-01T07:00:00Z
      2026-06-01 00:00 PDT -> 2026-06-01T07:00:00Z
    """
    us_tz = ZoneInfo("America/Los_Angeles")

    start_local = datetime(year, month, 1, 0, 0, 0, tzinfo=us_tz)

    if month == 12:
        end_local = datetime(year + 1, 1, 1, 0, 0, 0, tzinfo=us_tz)
    else:
        end_local = datetime(year, month + 1, 1, 0, 0, 0, tzinfo=us_tz)

    def iso_z(dt: datetime) -> str:
        return (
            dt.astimezone(timezone.utc)
            .replace(microsecond=0)
            .isoformat()
            .replace("+00:00", "Z")
        )

    return iso_z(start_local), iso_z(end_local)


# =========================================================
# BASIC HELPERS
# =========================================================
def _extract_order_id_from_related_identifiers(
    related_identifiers: Optional[List[Dict[str, Any]]]
) -> Optional[str]:
    if not related_identifiers:
        return None
    for rid in related_identifiers:
        name = (rid or {}).get("relatedIdentifierName")
        val = (rid or {}).get("relatedIdentifierValue")
        if name == "ORDER_ID" and val:
            return str(val)
    return None


def _extract_sku_and_qty_from_contexts(
    contexts: Optional[List[Dict[str, Any]]]
) -> Tuple[Optional[str], Optional[float]]:
    if not contexts:
        return None, None
    for ctx in contexts:
        if (ctx or {}).get("contextType") == "ProductContext":
            sku = ctx.get("sku")
            qty = ctx.get("quantityShipped")
            try:
                qty_f = float(qty) if qty is not None else None
            except (TypeError, ValueError):
                qty_f = None
            return sku, qty_f
    return None, None


# =========================================================
# BREAKDOWN WALK + CLASSIFIERS
# =========================================================
def _walk_leaf_breakdowns(breakdowns: Optional[List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    """
    Return ONLY leaf breakdown nodes (no children) from a breakdown tree.
    Using leaf nodes avoids double counting when Amazon nests totals.
    """
    leaves: List[Dict[str, Any]] = []

    def walk(nodes: List[Dict[str, Any]]):
        for b in nodes or []:
            children = b.get("breakdowns")
            has_children = isinstance(children, list) and len(children) > 0
            if not has_children:
                leaves.append(b)
            else:
                walk(children)

    walk(breakdowns or [])
    return leaves


def _walk_all_breakdowns_with_path(
    breakdowns: Optional[List[Dict[str, Any]]]
) -> List[Tuple[Dict[str, Any], str, List[str]]]:
    """
    Walk all breakdown nodes (including parents) and return:
      (node, normalized_node_type, path_types_including_node)
    The path is a list of normalized breakdownType strings from root->node.
    """
    out: List[Tuple[Dict[str, Any], str, List[str]]] = []

    def walk(nodes: List[Dict[str, Any]], path: List[str]):
        for b in nodes or []:
            t = _btype(b)
            new_path = path + [t]
            out.append((b, t, new_path))
            children = b.get("breakdowns")
            if isinstance(children, list) and children:
                walk(children, new_path)

    walk(breakdowns or [], [])
    return out


def _amt(b: Dict[str, Any]) -> float:
    v = ((b.get("breakdownAmount") or {}).get("currencyAmount")) or 0
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0


def _btype(b: Dict[str, Any]) -> str:
    return str(b.get("breakdownType", "")).lower().replace(" ", "")


def _contains_any(s: str, needles: List[str]) -> bool:
    return any(n in s for n in needles)


# Selling fees keywords (EU often uses "referralfee", "variableclosingfee", etc.)
SELLING_FEE_KEYS = [
    "referral", "commission", "sellingfee",
    "variableclosing", "closingfee", "fixedclosing",
    "peritem", "subscription", "platformfee"
]

# FBA fee keywords (catch the ones that were leaking into other_transaction_fees)
FBA_FEE_KEYS = [
    "fba", "fulfillment", "fulfilment", "pickpack", "pick_pack",
    "outbound", "inbound", "transport", "transportation",
    "storage", "longtermstorage", "ltstorage",
    "removal", "disposal", "returnsprocessing", "returnprocessing",
    "prep", "label", "repack", "warehouse", "handling"
]

# ✅ ServiceFee / storage billing keywords that should NOT be counted as FBA fees
SERVICE_FEE_EXCLUDE_KEYS = [
    "storagebilling", "fbastoragebilling", "storagefee", "inventorystorage",
    "agedinventory", "longtermstorage", "ltstorage", "storage"
]

# Withheld/facilitator tax keywords (marketplace withheld)
WITHHELD_TAX_KEYS = [
    "withheld", "marketplacewithheld", "withheldtax", "taxwithheld"
]

FACILITATOR_TAX_KEYS = [
    "facilitator", "marketplacefacilitator"
]

# Promo keys
PROMO_KEYS = ["promotion", "discount", "rebate", "coupon"]


def _sum_where(
    leaves: List[Dict[str, Any]],
    pred,
) -> float:
    total = 0.0
    for b in leaves:
        t = _btype(b)
        if pred(t):
            total += _amt(b)
    return total

# =========================================================
# FLATTEN TRANSACTION (FULL MTD SCHEMA)
# =========================================================
def _flatten_transaction_to_row(tx: Dict[str, Any]) -> Dict[str, Any]:
    posted_date = tx.get("postedDate")
    ttype = tx.get("transactionType")
    tstatus = tx.get("transactionStatus")
    desc = tx.get("description")

    # -----------------------------
    # total amount (tx-level)
    # -----------------------------
    total_amount_raw = (tx.get("totalAmount") or {}).get("currencyAmount")
    try:
        total_amount = float(total_amount_raw) if total_amount_raw is not None else 0.0
    except (TypeError, ValueError):
        total_amount = 0.0

    marketplace_details = tx.get("marketplaceDetails") or {}
    marketplace = marketplace_details.get("marketplaceName") or marketplace_details.get("marketplaceId")

    order_id = _extract_order_id_from_related_identifiers(tx.get("relatedIdentifiers") or [])

    # ---------- item level ----------
    sku = None
    quantity = None
    item_breakdowns: List[Dict[str, Any]] = []
    items = tx.get("items") or []
    if items:
        item0 = items[0] or {}
        sku, quantity = _extract_sku_and_qty_from_contexts(item0.get("contexts") or [])
        if isinstance(item0.get("breakdowns"), list):
            item_breakdowns = item0["breakdowns"]

    # ---------- tx level ----------
    tx_breakdowns: List[Dict[str, Any]] = tx.get("breakdowns") or []

    # ---------- leaves ----------
    item_leaves = _walk_leaf_breakdowns(item_breakdowns)
    tx_leaves = _walk_leaf_breakdowns(tx_breakdowns)

    eps = 1e-9
    ttype_norm = (ttype or "").lower().replace(" ", "")
    desc_norm = (desc or "").lower().replace(" ", "")

    # =========================================================
    # DEFAULT OUTPUTS
    # =========================================================
    product_sales = 0.0
    product_sales_tax = 0.0
    postage_credits = 0.0
    shipping_credits = 0.0
    shipping_credits_tax = 0.0
    promotional_rebates = 0.0
    promotional_rebates_tax = 0.0
    selling_fees = 0.0
    fba_fees = 0.0
    other_transaction_fees = 0.0
    marketplace_withheld_tax = 0.0
    marketplace_facilitator_tax = 0.0
    sales_tax_collected = 0.0
    other = 0.0

    def _node_has_children(n: Dict[str, Any]) -> bool:
        ch = n.get("breakdowns")
        return isinstance(ch, list) and len(ch) > 0

    has_any_breakdowns = bool(tx_breakdowns) or bool(item_breakdowns)

    # =========================================================
    # ✅ SPECIAL CASES (NO BREAKDOWN / NON-SALES TYPES)
    # =========================================================

    # ProductAdsPayment => fee
    if ttype_norm == "productadspayment":
        other_transaction_fees = -abs(total_amount) if abs(total_amount) > eps else 0.0
        total_calc = other_transaction_fees
        return {
            "date_time": posted_date, "settlement_id": None, "type": ttype,
            "order_id": order_id, "sku": sku, "description": desc, "quantity": quantity,
            "marketplace": marketplace, "fulfilment": None, "order_city": None,
            "order_state": None, "order_postal": None, "tax_collection_model": None,
            "product_sales": 0.0, "product_sales_tax": 0.0, "postage_credits": 0.0,
            "shipping_credits": 0.0, "shipping_credits_tax": 0.0,
            "gift_wrap_credits": 0.0, "giftwrap_credits_tax": 0.0,
            "promotional_rebates": 0.0, "promotional_rebates_tax": 0.0,
            "sales_tax_collected": 0.0, "marketplace_withheld_tax": 0.0,
            "marketplace_facilitator_tax": 0.0,
            "selling_fees": 0.0, "fba_fees": 0.0,
            "other_transaction_fees": other_transaction_fees, "other": 0.0,
            "regulatory_fee": 0.0, "tax_on_regulatory_fee": 0.0, "account_type": None,
            "total": total_calc, "bucket": tstatus,
        }

    # Transfer/Disbursement => payout
    if ttype_norm == "transfer":
        other = total_amount
        total_calc = other
        return {
            "date_time": posted_date, "settlement_id": None, "type": ttype,
            "order_id": order_id, "sku": sku, "description": desc, "quantity": quantity,
            "marketplace": marketplace, "fulfilment": None, "order_city": None,
            "order_state": None, "order_postal": None, "tax_collection_model": None,
            "product_sales": 0.0, "product_sales_tax": 0.0, "postage_credits": 0.0,
            "shipping_credits": 0.0, "shipping_credits_tax": 0.0,
            "gift_wrap_credits": 0.0, "giftwrap_credits_tax": 0.0,
            "promotional_rebates": 0.0, "promotional_rebates_tax": 0.0,
            "sales_tax_collected": 0.0, "marketplace_withheld_tax": 0.0,
            "marketplace_facilitator_tax": 0.0,
            "selling_fees": 0.0, "fba_fees": 0.0,
            "other_transaction_fees": 0.0, "other": other,
            "regulatory_fee": 0.0, "tax_on_regulatory_fee": 0.0, "account_type": None,
            "total": total_calc, "bucket": tstatus,
        }

    # ServiceFee => NOT FBA fulfillment fees (usually storage/aged inventory/etc)
    if ttype_norm == "servicefee":
        other_transaction_fees = total_amount  # keep signed amount
        total_calc = other_transaction_fees
        return {
            "date_time": posted_date, "settlement_id": None, "type": ttype,
            "order_id": order_id, "sku": sku, "description": desc, "quantity": quantity,
            "marketplace": marketplace, "fulfilment": None, "order_city": None,
            "order_state": None, "order_postal": None, "tax_collection_model": None,
            "product_sales": 0.0, "product_sales_tax": 0.0, "postage_credits": 0.0,
            "shipping_credits": 0.0, "shipping_credits_tax": 0.0,
            "gift_wrap_credits": 0.0, "giftwrap_credits_tax": 0.0,
            "promotional_rebates": 0.0, "promotional_rebates_tax": 0.0,
            "sales_tax_collected": 0.0, "marketplace_withheld_tax": 0.0,
            "marketplace_facilitator_tax": 0.0,
            "selling_fees": 0.0, "fba_fees": 0.0,
            "other_transaction_fees": other_transaction_fees, "other": 0.0,
            "regulatory_fee": 0.0, "tax_on_regulatory_fee": 0.0, "account_type": None,
            "total": total_calc, "bucket": tstatus,
        }

    # ✅ DebtRecovery / DebtPayment => map to selling_fees (so UI column shows it)
    if ttype_norm in ("debtrecovery", "debt") or desc_norm == "debtpayment":
        # Selling fees in your reports are expected negative (a cost)
        selling_fees = -abs(total_amount) if abs(total_amount) > eps else 0.0
        total_calc = selling_fees

        return {
            "date_time": posted_date, "settlement_id": None, "type": ttype,
            "order_id": order_id, "sku": sku, "description": desc, "quantity": quantity,
            "marketplace": marketplace, "fulfilment": None, "order_city": None,
            "order_state": None, "order_postal": None, "tax_collection_model": None,

            "product_sales": 0.0, "product_sales_tax": 0.0, "postage_credits": 0.0,
            "shipping_credits": 0.0, "shipping_credits_tax": 0.0,
            "gift_wrap_credits": 0.0, "giftwrap_credits_tax": 0.0,
            "promotional_rebates": 0.0, "promotional_rebates_tax": 0.0,

            "sales_tax_collected": 0.0,
            "marketplace_withheld_tax": 0.0,
            "marketplace_facilitator_tax": 0.0,

            "selling_fees": selling_fees,   # ✅ will show here
            "fba_fees": 0.0,
            "other_transaction_fees": 0.0,
            "other": 0.0,

            "regulatory_fee": 0.0, "tax_on_regulatory_fee": 0.0, "account_type": None,
            "total": total_calc, "bucket": tstatus,
        }


    # SellerDealPayment => put in other
    if ttype_norm == "sellerdealpayment":
        other = total_amount
        total_calc = other
        return {
            "date_time": posted_date, "settlement_id": None, "type": ttype,
            "order_id": order_id, "sku": sku, "description": desc, "quantity": quantity,
            "marketplace": marketplace, "fulfilment": None, "order_city": None,
            "order_state": None, "order_postal": None, "tax_collection_model": None,
            "product_sales": 0.0, "product_sales_tax": 0.0, "postage_credits": 0.0,
            "shipping_credits": 0.0, "shipping_credits_tax": 0.0,
            "gift_wrap_credits": 0.0, "giftwrap_credits_tax": 0.0,
            "promotional_rebates": 0.0, "promotional_rebates_tax": 0.0,
            "sales_tax_collected": 0.0, "marketplace_withheld_tax": 0.0,
            "marketplace_facilitator_tax": 0.0,
            "selling_fees": 0.0, "fba_fees": 0.0,
            "other_transaction_fees": 0.0, "other": other,
            "regulatory_fee": 0.0, "tax_on_regulatory_fee": 0.0, "account_type": None,
            "total": total_calc, "bucket": tstatus,
        }

    # =========================================================
    # NORMAL FLOW
    # =========================================================

    # ------------------- PRODUCT / SHIPPING / TAX KEYS -------------------
    sales_like_types = {"shipment", "refund", "chargebackrefund", "guaranteeclaim"}

    WITHHELD_KEYS_STRONG = [
        "marketplacewithheld", "marketplacewithheldtax",
        "withheldtax", "taxwithheld", "withheld"
    ]

    FACILITATOR_KEYS_STRONG = [
        "marketplacefacilitator", "marketplacefacilitatortax",
        "facilitator"
    ]

    SHIPPING_KEYS = [
        "shipping",
        "shipcharge",
        "shippingcharge",
        "shippingcharges",
        "delivery",
        "postage",
    ]

    PRODUCT_KEYS = [
        "principal",
        "itemprice",
        "itempriceprincipal",
        "baseprice",
    ]

    # ------------------- PRODUCT SALES -------------------
    product_sales = 0.0
    product_sales_tax = 0.0

    def _accumulate_product_from_breakdowns(breakdowns):
        nonlocal product_sales, product_sales_tax

        for node, t, path in _walk_all_breakdowns_with_path(breakdowns):
            children = node.get("breakdowns")
            if isinstance(children, list) and children:
                continue

            amt = _amt(node)
            if abs(amt) < 1e-12:
                continue

            path_str = "".join(path)

            is_product = _contains_any(path_str, PRODUCT_KEYS)
            is_tax = "tax" in t or "tax" in path_str
            is_shipping = _contains_any(path_str, SHIPPING_KEYS)
            is_promo = _contains_any(path_str, PROMO_KEYS)
            is_withheld_or_facilitator = (
                _contains_any(path_str, WITHHELD_KEYS_STRONG)
                or _contains_any(path_str, FACILITATOR_KEYS_STRONG)
            )

            if not is_product:
                continue

            if is_shipping or is_promo or is_withheld_or_facilitator:
                continue

            if is_tax:
                product_sales_tax += amt
            else:
                product_sales += amt

    _accumulate_product_from_breakdowns(item_breakdowns)

    # fallback to tx level only if item level has no product values
    if abs(product_sales) < eps and abs(product_sales_tax) < eps:
        _accumulate_product_from_breakdowns(tx_breakdowns)

    # product tax fallback:
    # sometimes product tax appears as Tax leaf without principal/itemprice in path
    if abs(product_sales_tax) < eps:
        def _accumulate_product_tax_fallback(breakdowns):
            nonlocal product_sales_tax

            for node, t, path in _walk_all_breakdowns_with_path(breakdowns):
                children = node.get("breakdowns")
                if isinstance(children, list) and children:
                    continue

                amt = _amt(node)
                if abs(amt) < 1e-12:
                    continue

                path_str = "".join(path)

                is_tax = "tax" in t or "tax" in path_str
                is_shipping = _contains_any(path_str, SHIPPING_KEYS)
                is_promo = _contains_any(path_str, PROMO_KEYS)
                is_withheld_or_facilitator = (
                    _contains_any(path_str, WITHHELD_KEYS_STRONG)
                    or _contains_any(path_str, FACILITATOR_KEYS_STRONG)
                )

                if is_tax and not is_shipping and not is_promo and not is_withheld_or_facilitator:
                    product_sales_tax += amt

        _accumulate_product_tax_fallback(item_breakdowns)

        if abs(product_sales_tax) < eps:
            _accumulate_product_tax_fallback(tx_breakdowns)

    # IMPORTANT:
    # Do NOT use total_amount as product_sales.
    # total_amount includes shipping/tax/fees and causes product_sales to become too high.

    # ------------------- SHIPPING / POSTAGE -------------------
    shipping_credits = 0.0
    shipping_credits_tax = 0.0

    def _accumulate_shipping_from_breakdowns(breakdowns):
        nonlocal shipping_credits, shipping_credits_tax

        for node, t, path in _walk_all_breakdowns_with_path(breakdowns):
            children = node.get("breakdowns")
            if isinstance(children, list) and children:
                continue

            amt = _amt(node)
            if abs(amt) < 1e-12:
                continue

            path_str = "".join(path)

            is_shipping = _contains_any(path_str, SHIPPING_KEYS)
            is_tax = "tax" in t or "tax" in path_str
            is_promo = _contains_any(path_str, PROMO_KEYS)
            is_withheld_or_facilitator = (
                _contains_any(path_str, WITHHELD_KEYS_STRONG)
                or _contains_any(path_str, FACILITATOR_KEYS_STRONG)
            )

            if not is_shipping:
                continue

            if is_promo or is_withheld_or_facilitator:
                continue

            if is_tax:
                shipping_credits_tax += amt
            else:
                shipping_credits += amt

    _accumulate_shipping_from_breakdowns(item_breakdowns)
    _accumulate_shipping_from_breakdowns(tx_breakdowns)

    postage_credits = shipping_credits 

    

    # ------------------- PROMOTIONS -------------------
    promotional_rebates = _sum_where(item_leaves, lambda t: _contains_any(t, PROMO_KEYS) and ("tax" not in t))
    promotional_rebates_tax = _sum_where(item_leaves, lambda t: _contains_any(t, PROMO_KEYS) and ("tax" in t))

    # ------------------- FEES + WITHHELD / FACILITATOR TAX -------------------
    marketplace_withheld_tax = 0.0
    marketplace_facilitator_tax = 0.0
    selling_fees = 0.0
    fba_fees = 0.0
    other_transaction_fees = 0.0

    
    def _accumulate_withheld_and_facilitator(breakdowns: List[Dict[str, Any]]):
        nonlocal marketplace_withheld_tax, marketplace_facilitator_tax

        for node, t, path in _walk_all_breakdowns_with_path(breakdowns):
            # leaf only
            children = node.get("breakdowns")
            if isinstance(children, list) and children:
                continue

            amt = _amt(node)
            if abs(amt) < 1e-12:
                continue

            path_str = "".join(path)
            if _contains_any(path_str, WITHHELD_KEYS_STRONG):
                marketplace_withheld_tax += amt
            elif _contains_any(path_str, FACILITATOR_KEYS_STRONG):
                marketplace_facilitator_tax += amt

    _accumulate_withheld_and_facilitator(tx_breakdowns)
    _accumulate_withheld_and_facilitator(item_breakdowns)

    # fee nets from tx tree (leaf only)
    for node, t, path in _walk_all_breakdowns_with_path(tx_breakdowns):
        if _node_has_children(node):
            continue

        amt = _amt(node)
        if abs(amt) < 1e-12:
            continue

        path_str = "".join(path)
        is_tax = ("tax" in t) or ("tax" in path_str)

        # skip withheld/facilitator
        if _contains_any(path_str, WITHHELD_KEYS_STRONG) or _contains_any(path_str, FACILITATOR_KEYS_STRONG):
            continue

        is_selling_fee = _contains_any(path_str, SELLING_FEE_KEYS)
        is_fba_fee = _contains_any(path_str, FBA_FEE_KEYS)
        is_service_fee_like = (ttype_norm == "servicefee") or _contains_any(path_str, SERVICE_FEE_EXCLUDE_KEYS)

        if is_selling_fee and (not is_tax) and (not is_fba_fee):
            selling_fees += amt
            continue

        if is_fba_fee and (not is_tax) and (not is_service_fee_like):
            fba_fees += amt
            continue

    # normalize signs
    # normalize signs: selling_fees must always be negative because it is a cost
    selling_fees = -abs(pd.to_numeric(selling_fees, errors="coerce") or 0)
    if fba_fees > 0:
        fba_fees = -abs(fba_fees)
    if marketplace_withheld_tax > 0:
        marketplace_withheld_tax = -abs(marketplace_withheld_tax)
    if marketplace_facilitator_tax > 0:
        marketplace_facilitator_tax = -abs(marketplace_facilitator_tax)

    sales_tax_collected = marketplace_withheld_tax

    total_calc = (
        product_sales
        + postage_credits
        - promotional_rebates
        - promotional_rebates_tax
        + selling_fees
        + fba_fees
        + other_transaction_fees
        + marketplace_withheld_tax
        + marketplace_facilitator_tax
        - sales_tax_collected
    )

    # ✅ fallback even if breakdowns exist (safety)
    if abs(total_calc) < eps and abs(total_amount) > eps and ttype_norm not in sales_like_types:
        other = total_amount
        total_calc = other

    return {
        "date_time": posted_date,
        "settlement_id": None,
        "type": ttype,
        "order_id": order_id,
        "sku": sku,
        "description": desc,
        "quantity": quantity,
        "marketplace": marketplace,

        "fulfilment": None,
        "order_city": None,
        "order_state": None,
        "order_postal": None,
        "tax_collection_model": None,

        "product_sales": product_sales,
        "product_sales_tax": product_sales_tax,
        "postage_credits": postage_credits,
        "shipping_credits": shipping_credits,
        "shipping_credits_tax": shipping_credits_tax,
        "gift_wrap_credits": 0.0,
        "giftwrap_credits_tax": 0.0,

        "promotional_rebates": promotional_rebates,
        "promotional_rebates_tax": promotional_rebates_tax,

        "sales_tax_collected": sales_tax_collected,
        "marketplace_withheld_tax": marketplace_withheld_tax,
        "marketplace_facilitator_tax": marketplace_facilitator_tax,

        "selling_fees": selling_fees,
        "fba_fees": fba_fees,
        "other_transaction_fees": other_transaction_fees,
        "other": other,

        "regulatory_fee": 0.0,
        "tax_on_regulatory_fee": 0.0,
        "account_type": None,

        "total": total_calc,
        "bucket": tstatus,
    }


def _month_to_num(mname: str) -> int:
    m = mname.strip().lower()
    for i in range(1, 13):
        if calendar.month_name[i].lower() == m or calendar.month_abbr[i].lower() == m:
            return i
    raise ValueError("Invalid month")





def _month_to_date_range_utc_safe(now_utc: datetime, safety_minutes: int = 3) -> Tuple[str, str]:
    """
    Amazon rule: postedBefore must be <= (current_time - 2 minutes).
    We subtract 3 minutes by default to avoid edge timing issues.
    """
    start = datetime(now_utc.year, now_utc.month, 1, 0, 0, 0, tzinfo=timezone.utc)
    end = now_utc - timedelta(minutes=safety_minutes)

    # if system clock weird / very early month edge, keep end >= start
    if end < start:
        end = start

    def iso_z(dt: datetime) -> str:
        # strip microseconds (cleaner and avoids weird formats)
        dt = dt.replace(microsecond=0)
        return dt.isoformat().replace("+00:00", "Z")

    return iso_z(start), iso_z(end)

# ---------------------------------------------------------- Live data fetching ----------------------------------------------------------


# ---------------------------------------------------------
# UTILS
# ---------------------------------------------------------
def _parse_amz_dt(s: str):
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        return None

def _f(v):
    try:
        return float(v) if v is not None else 0.0
    except Exception:
        return 0.0

def _i(v):
    try:
        return int(v) if v is not None else None
    except Exception:
        return None

def _month_name_lower(month_num: int) -> str:
    return calendar.month_name[int(month_num)].lower()

def _month_to_date_range_utc_safe(now_utc: datetime, safety_minutes: int = 3) -> Tuple[str, str]:
    start = datetime(now_utc.year, now_utc.month, 1, 0, 0, 0, tzinfo=timezone.utc)
    end = now_utc - timedelta(minutes=safety_minutes)
    if end < start:
        end = start

    def iso_z(dt: datetime) -> str:
        dt = dt.replace(microsecond=0)
        return dt.isoformat().replace("+00:00", "Z")

    return iso_z(start), iso_z(end)


# ---------------------------------------------------------
# SKU PRICE MAP (PHORMULA DB)
# ---------------------------------------------------------
def fetch_sku_price_map(user_id: int, country: str) -> Dict[str, float]:
    """
    Reads from: public."sku_{user_id}_data_table"
    Returns {sku: price}
    Uses sku_uk if country=uk, sku_us if country=us.
    """
    uid = int(user_id)
    country = (country or "").strip().lower()

    table = f'public."sku_{uid}_data_table"'
    sql = text(f"SELECT sku_uk, sku_us, price FROM {table}")

    mp: Dict[str, float] = {}
    use_uk = (country == "uk")
    use_us = (country == "us")

    with PHORMULA_ENGINE.connect() as conn:
        rows = conn.execute(sql).fetchall()

    for sku_uk, sku_us, price in rows:
        if price is None:
            continue
        try:
            p = float(price)
        except Exception:
            continue

        if use_uk and sku_uk:
            mp.setdefault(str(sku_uk).strip(), p)
        elif use_us and sku_us:
            mp.setdefault(str(sku_us).strip(), p)
        else:
            if sku_uk:
                mp.setdefault(str(sku_uk).strip(), p)
            if sku_us:
                mp.setdefault(str(sku_us).strip(), p)

    return mp


# ---------------------------------------------------------
# CONVERSION RATE (ADMIN DB)
# ---------------------------------------------------------
def fetch_conversion_rate(country: str, year: int, month_name: str,
                          user_currency: str, selected_currency: str) -> float:
    sql = text("""
        SELECT conversion_rate
        FROM public.currency_conversion
        WHERE lower(country) = lower(:country)
          AND year = :year
          AND lower(month) = lower(:month)
          AND lower(user_currency) = lower(:user_currency)
          AND lower(selected_currency) = lower(:selected_currency)
        ORDER BY id DESC
        LIMIT 1
    """)

    params = {
        "country": (country or "").strip(),
        "year": int(year),
        "month": (month_name or "").strip(),
        "user_currency": (user_currency or "").strip(),
        "selected_currency": (selected_currency or "").strip(),
    }

    with ADMIN_ENGINE.connect() as conn:
        row = conn.execute(sql, params).fetchone()

    if not row or row[0] is None:
        print("MISSING CONVERSION RATE:", params)
        return 1.0

    return float(row[0])


# ---------------------------------------------------------
# TOTALS (includes cogs)
# ---------------------------------------------------------
TOTAL_FIELDS = [
    "quantity",
    "product_sales",
    "gross_sales",
    "product_sales_tax",
    "postage_credits",
    "shipping_credits",
    "shipping_credits_tax",
    "gift_wrap_credits",
    "giftwrap_credits_tax",
    "promotional_rebates",
    "promotional_rebates_tax",
    "marketplace_facilitator_tax",
    "selling_fees",
    "fba_fees",
    "platform_fees",        # ✅ add (if present in rows)
    "advertising_cost",     # ✅ add (if present in rows)
    "cogs",                   # ✅ include cogs in totals
    "profit",
    "other_transaction_fees",
    "other",
    "total",
    "tax_and_credits",

]

def compute_totals(rows: List[Dict[str, Any]]) -> Dict[str, float]:
    out = {k: 0.0 for k in TOTAL_FIELDS}
    for r in rows or []:
        for k in TOTAL_FIELDS:
            if k == "quantity":
                out[k] += float(_i(r.get(k)) or 0)
            else:
                out[k] += float(_f(r.get(k)))
    return out

def add_profit_column_from_uk_profit(rows: List[Dict[str, Any]], country: str) -> None:
    """
    Adds row["profit"] for each row by:
      - computing sku-level profit using uk_profit(df, want_breakdown=True)
      - converting to per-unit profit
      - assigning profit per row = per_unit_profit * row_qty
    Mutates rows in-place.
    """
    if not rows:
        return

    df = pd.DataFrame(rows)

    if df.empty or "sku" not in df.columns:
        for r in rows:
            r["profit"] = 0.0
        return

    # normalize sku + qty
    df["sku"] = df["sku"].astype(str).str.strip()
    df["quantity"] = df.get("quantity", 0).apply(lambda x: float(_i(x) or 0))

    # uk_profit() expects cost_of_unit_sold, so map your cogs into that
    # (Your cogs is already qty*price*conversion_rate per row)
    df["cost_of_unit_sold"] = safe_num(df.get("cogs", 0.0)).abs()

    # compute sku-level profit breakdown
    profit_total, profit_by_sku, _ = uk_profit(df, country=country, want_breakdown=True)

    # profit_by_sku has columns: sku, __metric__ (=profit per sku)
    if profit_by_sku is None or profit_by_sku.empty:
        for r in rows:
            r["profit"] = 0.0
        return

    profit_map = {str(s).strip(): float(p) for s, p in zip(profit_by_sku["sku"], profit_by_sku["__metric__"])}

    # qty by sku (for per-unit allocation)
    qty_by_sku = df.groupby("sku", as_index=True)["quantity"].sum().to_dict()

    per_unit_profit = {}
    for sku, sku_profit in profit_map.items():
        q = float(qty_by_sku.get(sku, 0.0) or 0.0)
        per_unit_profit[sku] = (sku_profit / q) if q else 0.0

    # assign row-profit
    for r in rows:
        sku = str((r.get("sku") or "")).strip()
        qty = float(_i(r.get("quantity")) or 0)
        r["profit"] = float(per_unit_profit.get(sku, 0.0)) * qty

def build_tx_key(r: dict) -> str:
    dt = (r.get("date_time") or "").strip()
    t  = (r.get("type") or "").strip()
    oid = (r.get("order_id") or "").strip()
    sku = (r.get("sku") or "").strip()
    qty = str(_i(r.get("quantity")) or 0)
    total = f"{_f(r.get('total')):.2f}"
    desc = (r.get("description") or "").strip()
    return f"{dt}|{t}|{oid}|{sku}|{qty}|{total}|{desc}"


# ---------------------------------------------------------
# UPSERT WITH COGS
# ---------------------------------------------------------
def upsert_liveorders_from_rows(rows, user_id: int, country: str, now_utc: datetime):
    if not rows:
        return {"inserted": 0, "updated": 0, "conversion_rate": 1.0}

    country = (country or "").strip().lower()

    sku_price_map = fetch_sku_price_map(user_id=user_id, country=country)

    month_name = _month_name_lower(now_utc.month)
    user_currency = DEFAULT_SKU_PRICE_CURRENCY
    selected_currency = COUNTRY_TO_SELECTED_CURRENCY.get(country, user_currency)

    conversion_rate = fetch_conversion_rate(
        country=country,
        year=now_utc.year,
        month_name=month_name,
        user_currency=user_currency,
        selected_currency=selected_currency
    )

    logger.info(
        f"[COGS] country={country} month={month_name} year={now_utc.year} "
        f"pair={user_currency}->{selected_currency} rate={conversion_rate}"
    )

    # ✅ build tx_key for every row (INCLUDING order_id=None rows)
    for r in rows:
        r["tx_key"] = build_tx_key(r)

    tx_keys = [r["tx_key"] for r in rows if r.get("tx_key")]

    # ✅ load existing rows by tx_key (not amazon_order_id)
    existing = Liveorder.query.filter(
        and_(Liveorder.user_id == user_id, Liveorder.tx_key.in_(tx_keys))
    ).all()
    existing_map = {e.tx_key: e for e in existing}

    inserted = 0
    updated = 0

    for r in rows:
        tx_key = r.get("tx_key")
        if not tx_key:
            continue

        obj = existing_map.get(tx_key)
        if obj is None:
            obj = Liveorder(user_id=user_id, tx_key=tx_key)
            db.session.add(obj)
            existing_map[tx_key] = obj
            inserted += 1
        else:
            updated += 1

        # ✅ order_id can be NULL now
        obj.amazon_order_id = (r.get("order_id") or None)

        obj.purchase_date = _parse_amz_dt(r.get("date_time"))
        obj.order_status = (r.get("bucket") or "")
        obj.sku = (r.get("sku") or "").strip() or None
        obj.quantity = _i(r.get("quantity")) or 0

        # ✅ correct cogs
        price = sku_price_map.get(obj.sku) if obj.sku else None
        if price is None or obj.quantity <= 0:
            obj.cogs = 0.0
        else:
            obj.cogs = float(obj.quantity) * float(price) * float(conversion_rate)

        obj.profit = _f(r.get("profit"))

        obj.type = r.get("type")
        obj.description = r.get("description")
        obj.marketplace = r.get("marketplace")

        obj.product_sales = _f(r.get("product_sales"))
        obj.product_sales_tax = _f(r.get("product_sales_tax"))
        obj.postage_credits = _f(r.get("postage_credits"))
        obj.shipping_credits = _f(r.get("shipping_credits"))
        obj.shipping_credits_tax = _f(r.get("shipping_credits_tax"))
        obj.gift_wrap_credits = _f(r.get("gift_wrap_credits"))
        obj.giftwrap_credits_tax = _f(r.get("giftwrap_credits_tax"))
        obj.promotional_rebates = _f(r.get("promotional_rebates"))
        obj.promotional_rebates_tax = _f(r.get("promotional_rebates_tax"))
        # ✅ NEW: gross_sales
        obj.gross_sales = (
            obj.product_sales
            + obj.product_sales_tax
            + obj.postage_credits
            + obj.gift_wrap_credits
            + obj.shipping_credits_tax
            - obj.promotional_rebates
            - obj.promotional_rebates_tax
        )
        obj.marketplace_facilitator_tax = _f(r.get("marketplace_facilitator_tax"))
        obj.selling_fees = _f(r.get("selling_fees"))
        obj.fba_fees = _f(r.get("fba_fees"))
        obj.other_transaction_fees = _f(r.get("other_transaction_fees"))
        obj.other = _f(r.get("other"))
        obj.total = _f(r.get("total"))
        obj.bucket = r.get("bucket")

    db.session.commit()

    return {
        "inserted": inserted,
        "updated": updated,
        "conversion_rate": conversion_rate,
        "month": month_name,
        "year": now_utc.year,
        "country": country,
        "pair": f"{user_currency}->{selected_currency}"
    }



def previous_month_mtd_range(now_utc: datetime) -> tuple[date, date]:
    """
    Previous-month MTD range (inclusive, same day number as today if possible).

    Example:
      now_utc = 2025-12-17
      -> prev_start = 2025-11-01
      -> prev_end   = 2025-11-17
    """
    today_day = int(now_utc.day)

    # previous month/year
    if now_utc.month == 1:
        py, pm = now_utc.year - 1, 12
    else:
        py, pm = now_utc.year, now_utc.month - 1

    prev_start = date(py, pm, 1)
    last_day_prev_month = calendar.monthrange(py, pm)[1]

    # ✅ inclusive MTD: end at the same day number (clamped to month length)
    prev_end_day = min(today_day, last_day_prev_month)

    prev_end = date(py, pm, prev_end_day)
    return prev_start, prev_end




def _safe_sql_identifier_table(name: str) -> str:
    """
    Allow only safe SQL identifiers for table names.
    Prevents SQL injection when using dynamic table names.
    """
    if not name:
        raise ValueError("Table name is empty")

    # Allow: letters, numbers, underscore, dot, double quotes, hyphen
    if not re.fullmatch(r'[A-Za-z0-9_."-]+', name):
        raise ValueError(f"Unsafe table name: {name}")

    return name


def fetch_previous_period_data(user_id, country, prev_start: date, prev_end: date):
    """
    Return:
      sku_metrics: list (per-SKU metrics)
      prev_totals: {
        quantity, gross_sales, net_sales, profit, asp,
        profit_percentage, platform_fee, advertising_fees, cm2_profit,
        previous_net_reimbursement,
        cogs, amazon_fee, tax_and_credits
      }
      daily_series: list of {date, quantity, net_sales} (optional for chart)
    """

    table_name = construct_prev_table_name(
        user_id=user_id,
        country=country,
        month=prev_start.month,
        year=prev_start.year,
    )
    table_name = _safe_sql_identifier_table(table_name)

    query = text(f"""
        SELECT *
        FROM (
            SELECT
                *,
                NULLIF(NULLIF(date_time, '0'), '')::timestamp AS date_ts
            FROM {table_name}
        ) t
        WHERE date_ts >= :start_ts
          AND date_ts < :end_ts_excl
    """)

    params = {
        "start_ts": datetime.combine(prev_start, datetime.min.time()),
        "end_ts_excl": datetime.combine(prev_end + timedelta(days=1), datetime.min.time()),
    }

    with engine_hist.connect() as conn:
        result = conn.execute(query, params)
        rows = result.fetchall()

        if not rows:
            return [], {
                "quantity": 0.0,
                "gross_sales": 0.0,
                "net_sales": 0.0,
                "profit": 0.0,
                "asp": 0.0,
                "profit_percentage": 0.0,
                "platform_fee": 0.0,
                "advertising_fees": 0.0,
                "cm2_profit": 0.0,
                "promotional_rebates": 0.0,
                "promotional_rebates_percentage": 0.0,
                "previous_net_reimbursement": 0.0,
                "cogs": 0.0,
                "amazon_fees": 0.0,
                "tax_and_credits": 0.0,
            }, []

        df = pd.DataFrame(rows, columns=result.keys())

    # -------------------------
    # Per-SKU metrics (existing)
    # -------------------------
    sku_metrics = compute_sku_metrics_from_df(df) or []

    # -------------------------
    # Totals (NOT day-wise)
    # -------------------------
    quantity_total = float(safe_num(df.get("quantity", 0.0)).sum())

    # Build series (your existing logic)
    ps   = safe_num(df.get("product_sales", 0.0))
    pst  = safe_num(df.get("product_sales_tax", 0.0))
    pc   = safe_num(df.get("postage_credits", 0.0))
    gwc  = safe_num(df.get("gift_wrap_credits", 0.0))
    sct  = safe_num(df.get("shipping_credits_tax", 0.0))
    gwt  = safe_num(df.get("giftwrap_credits_tax", 0.0))
    pr   = safe_num(df.get("promotional_rebates", 0.0))
    prt  = safe_num(df.get("promotional_rebates_tax", 0.0))

    mft  = safe_num(df.get("marketplace_facilitator_tax", 0.0))  # ✅ for tax_and_credits

    # ✅ gross_sales same as live MTD
    gross_sales_total = float((ps + pst + pc + gwc + sct + gwt - pr - prt).sum())

    # ✅ net_sales stays as you had it (product_sales + promotional_rebates)
    net_sales_total = float((ps + pr).sum())
    promotional_rebates_total = float(pr.sum())
    promotional_rebates_percentage = (
        promotional_rebates_total / net_sales_total * 100.0
    ) if net_sales_total else 0.0

    # Profit total from sku_metrics (keeps your current logic)
    profit_total = sum(float(x.get("profit", 0.0) or 0.0) for x in sku_metrics)

    asp = (net_sales_total / quantity_total) if quantity_total else 0.0

    # -------------------------
    # ✅ NEW: COGS (cost_of_unit_sold * quantity)
    # -------------------------
    unit_cost = safe_num(df.get("cost_of_unit_sold", 0.0))
    cogs_total = float((unit_cost).sum())

    # -------------------------
    # ✅ NEW: Amazon fee (selling_fees + fba_fees) with signs preserved
    # -------------------------
    selling_fees_total = float(safe_num(df.get("selling_fees", 0.0)).sum())
    fba_fees_total = float(safe_num(df.get("fba_fees", 0.0)).sum())
    amazon_fee_total = abs(selling_fees_total) + abs(fba_fees_total)  # keeps negatives

    # -------------------------
    # ✅ NEW: Tax and credits (your exact definition)
    # -------------------------
    postage_credits_total = float(pc.sum())
    gift_wrap_credits_total = float(gwc.sum())
    product_sales_tax_total = float(pst.sum())
    shipping_credits_tax_total = float(sct.sum())
    promotional_rebates_tax_total = float(prt.sum())
    marketplace_facilitator_tax_total = float(mft.sum())

    tax_and_credits_total = (
        postage_credits_total
        + gift_wrap_credits_total
        + product_sales_tax_total
        + shipping_credits_tax_total
        + promotional_rebates_tax_total
        + marketplace_facilitator_tax_total
    )

    # -------------------------
    # Platform + Advertising totals (existing)
    # -------------------------
    for col, default in [
        ("description", ""),
        ("total", 0.0),
        ("platform_fees", 0.0),
        ("advertising_cost", 0.0),
    ]:
        if col not in df.columns:
            df[col] = default

    platform_fee_total = 0.0
    advertising_fee_total = 0.0

    if not df.empty:
        platform_fee_total, _, _ = uk_platform_fee(df, country=country, want_breakdown=False)
        advertising_fee_total, _, _ = uk_advertising(df, country=country, want_breakdown=False)

    platform_fee_total = float(platform_fee_total or 0.0)
    advertising_fee_total = float(advertising_fee_total or 0.0)

    cm2_profit = float(profit_total) - advertising_fee_total - platform_fee_total
    profit_percentage = (cm2_profit / net_sales_total * 100) if net_sales_total else 0.0

    previous_net_reimbursement = compute_net_reimbursement_from_df(df)

    prev_totals = {
        "quantity": round(quantity_total, 2),
        "gross_sales": round(gross_sales_total, 2),
        "net_sales": round(net_sales_total, 2),
        "profit": round(profit_total, 2),
        "asp": round(asp, 2),

        "platform_fee": round(platform_fee_total, 2),
        "advertising_fees": round(advertising_fee_total, 2),
        "cm2_profit": round(cm2_profit, 2),
        "profit_percentage": round(profit_percentage, 2),
        "promotional_rebates": round(promotional_rebates_total, 2),
        "promotional_rebates_percentage": round(promotional_rebates_percentage, 6),

        "previous_net_reimbursement": round(previous_net_reimbursement, 2),

        # ✅ NEW fields
        "cogs": round(cogs_total, 2),
        "amazon_fees": round(amazon_fee_total, 2),
        "tax_and_credits": round(tax_and_credits_total, 2),
    }

    # -------------------------
    # Optional daily series
    # -------------------------
    daily_series = []
    date_col = "date_ts" if "date_ts" in df.columns else ("date_time" if "date_time" in df.columns else None)

    if date_col:
        tmp = df.copy()
        tmp["date_only"] = pd.to_datetime(tmp[date_col], errors="coerce").dt.date

        tmp["quantity"] = safe_num(tmp.get("quantity", 0.0))
        tmp["product_sales"] = safe_num(tmp.get("product_sales", 0.0))
        tmp["promotional_rebates"] = safe_num(tmp.get("promotional_rebates", 0.0))
        tmp["net_sales"] = tmp["product_sales"] + tmp["promotional_rebates"]

        g = tmp.groupby("date_only", as_index=False).agg(
            quantity=("quantity", "sum"),
            net_sales=("net_sales", "sum"),
        )

        for d, qd, ns in zip(g["date_only"], g["quantity"], g["net_sales"]):
            if pd.isna(d):
                continue
            daily_series.append({
                "date": d.isoformat(),
                "quantity": float(qd),
                "net_sales": float(ns),
            })

    return sku_metrics, prev_totals, daily_series


def get_previous_month_mtd_payload(user_id: int, country: str, now_utc: datetime) -> dict:
    # MTD range (keep existing behavior)
    prev_start, prev_end = previous_month_mtd_range(now_utc)

    sku_metrics, prev_totals, _daily_series = fetch_previous_period_data(
        user_id=user_id,
        country=country,
        prev_start=prev_start,
        prev_end=prev_end,
    )

    # ✅ Full previous month range (ONLY for reimbursement)
    full_start, full_end = previous_month_full_range(now_utc)
    full_month_reimbursement = fetch_net_reimbursement_for_period(
        user_id=user_id,
        country=country,
        start_d=full_start,
        end_d=full_end,
    )

    # ✅ override only this field
    prev_totals["previous_net_reimbursement"] = round(full_month_reimbursement, 2)

    return {
        "prev_start": prev_start.isoformat(),
        "prev_end": prev_end.isoformat(),
        "reimbursement_start": full_start.isoformat(),
        "reimbursement_end": full_end.isoformat(),
        "totals": prev_totals,
        "sku_metrics": sku_metrics,
    }


def compute_net_reimbursement_from_df(df: pd.DataFrame) -> float:
    """
    Net reimbursement = sum(Transfer/Disbursement totals) - abs(sum(DebtRecovery totals))
    Adjust if your DebtRecovery totals are already negative (then abs() is still safe).
    """
    if df is None or df.empty:
        return 0.0

    tmp = df.copy()

    # ensure cols exist
    for col, default in [("type", ""), ("description", ""), ("total", 0.0)]:
        if col not in tmp.columns:
            tmp[col] = default

    t = tmp["type"].astype(str).str.strip().str.lower()
    d = tmp["description"].astype(str).str.strip().str.lower()
    tot = safe_num(tmp["total"])

    disb = float(tot[(t == "transfer") & (d == "disbursement")].sum())

    # If you only want DebtRecovery/DebtPayment specifically, add (d=="debtpayment") too
    debt = float(tot[t == "debtrecovery"].sum())

    net = disb - abs(debt)
    return float(net or 0.0)

def compute_debt_payment_disbursement_from_df(df: pd.DataFrame) -> dict:
    """
    Separate TOTAL-only values:
    debt_payment = abs(sum total where description = DebtPayment)
    disbursement = abs(sum total where description = Disbursement)
    """
    if df is None or df.empty:
        return {
            "debt_payment": 0.0,
            "disbursement": 0.0,
        }

    tmp = df.copy()

    for col, default in [("type", ""), ("description", ""), ("total", 0.0)]:
        if col not in tmp.columns:
            tmp[col] = default

    t = tmp["type"].astype(str).str.strip().str.lower()
    d = tmp["description"].astype(str).str.strip().str.lower()
    total = safe_num(tmp["total"])

    debt_payment = float(
        total[
            (d == "debtpayment")
            | ((t == "debtrecovery") & (d == "debtpayment"))
        ].sum()
    )

    disbursement = float(
        total[
            (d == "disbursement")
            | ((t == "transfer") & (d == "disbursement"))
        ].sum()
    )

    return {
        "debt_payment": abs(debt_payment),
        "disbursement": abs(disbursement),
    }

def previous_month_full_range(now_utc: datetime) -> tuple[date, date]:
    # previous month/year
    if now_utc.month == 1:
        py, pm = now_utc.year - 1, 12
    else:
        py, pm = now_utc.year, now_utc.month - 1

    start = date(py, pm, 1)
    end = date(py, pm, calendar.monthrange(py, pm)[1])
    return start, end

def fetch_net_reimbursement_for_period(user_id, country, start_d: date, end_d: date) -> float:
    table_name = construct_prev_table_name(
        user_id=user_id,
        country=country,
        month=start_d.month,
        year=start_d.year,
    )
    table_name = _safe_sql_identifier_table(table_name)

    query = text(f"""
        SELECT type, description, total
        FROM (
            SELECT
                type,
                description,
                total,
                NULLIF(NULLIF(date_time, '0'), '')::timestamp AS date_ts
            FROM {table_name}
        ) t
        WHERE date_ts >= :start_ts
        AND date_ts < :end_ts_excl
    """)


    params = {
        "start_ts": datetime.combine(start_d, datetime.min.time()),
        "end_ts_excl": datetime.combine(end_d + timedelta(days=1), datetime.min.time()),
    }

    with engine_hist.connect() as conn:
        result = conn.execute(query, params)
        rows = result.fetchall()
        if not rows:
            return 0.0
        df = pd.DataFrame(rows, columns=result.keys())

    return float(compute_net_reimbursement_from_df(df) or 0.0)

