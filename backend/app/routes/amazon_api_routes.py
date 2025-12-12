from __future__ import annotations
import base64, io, os,time, calendar, json, logging, inspect
import urllib.parse
import pandas as pd
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, Optional
from datetime import datetime, timezone
from sqlalchemy.dialects.postgresql import insert as pg_insert
import boto3, jwt, requests
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from Crypto.Cipher import AES
from config import Config
UPLOAD_FOLDER = Config.UPLOAD_FOLDER
from dotenv import find_dotenv, load_dotenv
from flask import Blueprint, Response, jsonify, make_response, request
from sqlalchemy.dialects.postgresql import JSONB, insert
from app import db
from app.models.user_models import UploadHistory, amazon_user
from app.utils.us_process_utils  import process_skuwise_us_data , process_us_yearly_skuwise_data, process_us_quarterly_skuwise_data
from app.utils.uk_process_utils import process_skuwise_data , process_quarterly_skuwise_data, process_yearly_skuwise_data 
from app.utils.plotting_utils import ( apply_modifications_fatch 
)
from app.utils.currency_utils import (  process_global_yearly_skuwise_data ,
    process_global_quarterly_skuwise_data , 
    process_global_monthly_skuwise_data
)
from app.models.user_models import db, Product

from dotenv import load_dotenv

# App config / auth
from config import Config
SECRET_KEY = Config.SECRET_KEY

# --- load .env robustly (works no matter where you run `flask run`) ---
dotenv_path = find_dotenv(filename=".env", usecwd=True)
load_dotenv(dotenv_path, override=True)

logger = logging.getLogger("amazon_sp_api")
logging.basicConfig(level=logging.INFO)
load_dotenv()
db_url  = os.getenv('DATABASE_URL')
db_url1 = os.getenv('DATABASE_ADMIN_URL') or db_url  # fallback

if not db_url:
    raise RuntimeError("DATABASE_URL is not set")
if not db_url1:
    # optional: log a warning if using fallback
    print("[WARN] DATABASE_ADMIN_URL not set; falling back to DATABASE_URL")

amazon_api_bp = Blueprint("amazon_api", __name__)


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
     
    

    # def make_api_call(
    #     self,
    #     endpoint: str,
    #     method: str = "GET",
    #     params: Optional[Dict[str, Any]] = None,
    #     data: Optional[Dict[str, Any]] = None,
    #     max_retries: int = 3,
    # ) -> Dict[str, Any]:
    #     token = self.get_access_token()
    #     if not token:
    #         return {"error": "No access token"}

    #     base = f"{self.api_base_url}{endpoint}"
    #     qs = urllib.parse.urlencode(params or {}, doseq=True, safe=":,")
    #     url = f"{base}?{qs}" if qs else base

    #     headers = {
    #         "host": urllib.parse.urlparse(url).netloc,
    #         "x-amz-access-token": token,
    #         "user-agent": f"YourApp/1.0 (Python; region={self.region})",
    #     }
    #     body = json.dumps(data) if data else ""
    #     if method.upper() != "GET":
    #         headers["content-type"] = "application/json"

    #     def _format_error_response(resp: requests.Response) -> Dict[str, Any]:
    #         try:
    #             j = resp.json()
    #         except Exception:
    #             j = None

    #         # 🔹 Request ID from response headers
    #         amzn_request_id = (
    #             resp.headers.get("x-amzn-RequestId")
    #             or resp.headers.get("x-amz-request-id")
    #         )

    #         # 🔹 Request timestamp (when WE made the call)
    #         # (Note: we’ll compute it outside and pass in via closure)
    #         return {
    #             "error": "UpstreamError",
    #             "status_code": resp.status_code,
    #             "amzn_error_type": resp.headers.get("x-amzn-ErrorType"),
    #             "amzn_request_id": amzn_request_id,
    #             "response_json": j,
    #             "response_text": (None if j is not None else resp.text),
    #             "method": method.upper(),
    #             "url": url,
    #             "endpoint": endpoint,
    #             "params": params,
    #             "data": data,
    #             "timestamp": request_timestamp,   # <- our request timestamp
    #         }

    #     for attempt in range(1, max_retries + 1):
    #         # 🔹 Timestamp for this attempt
    #         request_timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


    #         try:
    #             signed = self.sign_request(method, url, headers, body)
    #             if method.upper() == "GET":
    #                 resp = requests.get(url, headers=signed, timeout=30)
    #             elif method.upper() == "DELETE":
    #                 resp = requests.delete(url, headers=signed, timeout=30)
    #             else:
    #                 resp = requests.post(url, headers=signed, data=body, timeout=30)

    #             # 🔹 Extract Request ID from SUCCESS or ERROR responses
    #             amzn_request_id = (
    #                 resp.headers.get("x-amzn-RequestId")
    #                 or resp.headers.get("x-amz-request-id")
    #             )

    #             if resp.status_code in (429, 503) and attempt < max_retries:
    #                 time.sleep(2 ** attempt)
    #                 continue

    #             if resp.status_code in (401, 403) and attempt == 1:
    #                 self._access_token = None
    #                 headers["x-amz-access-token"] = self.get_access_token() or ""
    #                 continue

    #             if 200 <= resp.status_code < 300:
    #                 # ✅ SUCCESS: add meta info but keep the original payload structure
    #                 try:
    #                     payload = resp.json()
    #                 except Exception:
    #                     # If no JSON, still return meta
    #                     return {
    #                         "status_code": resp.status_code,
    #                         "ok": True,
    #                         "amzn_request_id": amzn_request_id,
    #                         "timestamp": request_timestamp,
    #                         "method": method.upper(),
    #                         "url": url,
    #                         "endpoint": endpoint,
    #                         "params": params,
    #                         "data": data,
    #                         "response_text": resp.text,
    #                     }

    #                 if isinstance(payload, dict):
    #                     # Attach meta under a reserved key to avoid breaking `.get("payload")`
    #                     meta = payload.setdefault("_meta", {})
    #                     meta.update(
    #                         {
    #                             "status_code": resp.status_code,
    #                             "amzn_request_id": amzn_request_id,
    #                             "timestamp": request_timestamp,
    #                             "method": method.upper(),
    #                             "url": url,
    #                             "endpoint": endpoint,
    #                             "params": params,
    #                             "data": data,
    #                         }
    #                     )
    #                     return payload
    #                 else:
    #                     # Non-dict JSON (list, etc.)
    #                     return {
    #                         "payload": payload,
    #                         "_meta": {
    #                             "status_code": resp.status_code,
    #                             "amzn_request_id": amzn_request_id,
    #                             "timestamp": request_timestamp,
    #                             "method": method.upper(),
    #                             "url": url,
    #                             "endpoint": endpoint,
    #                             "params": params,
    #                             "data": data,
    #                         },
    #                     }

    #             # ❌ Non-2xx -> error formatter (includes request_id & timestamp)
    #             err = _format_error_response(resp)
    #             logger.warning(
    #                 "SP-API %s %s -> %s | %s | request_id=%s",
    #                 method.upper(),
    #                 endpoint,
    #                 resp.status_code,
    #                 err.get("response_json") or err.get("response_text"),
    #                 err.get("amzn_request_id"),
    #             )
    #             return err

    #         except requests.RequestException as e:
    #             if attempt == max_retries:
    #                 return {
    #                     "error": "RequestException",
    #                     "message": str(e),
    #                     "method": method.upper(),
    #                     "url": url,
    #                     "timestamp": request_timestamp,
    #                 }
    #             time.sleep(2 ** attempt)

    #     return {"error": "Unknown"}


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


MONTH_NAME = {i: calendar.month_name[i].lower() for i in range(1, 13)}

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
    # Fallbacks if caller passes None
    if not db_url:
        db_url = os.getenv('DATABASE_URL')
    if not db_url_aux:
        db_url_aux = os.getenv('DATABASE_ADMIN_URL') or db_url

    if not db_url:
        return {"success": False, "message": "DATABASE_URL not configured"}

    from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Float, text
    country = country.lower()
    # first convert month_num to string name
    month = MONTH_NAME[int(month_num)] 
    engine  = create_engine(db_url)
    engine1 = create_engine(db_url_aux)
    meta = MetaData()

    table_name = f"user_{user_id}_{country}_{month}{year}_data".lower()
    country_table_name = f"sku_{user_id}_data_table"
    consolidated_table_name = f"user_{user_id}_{country}_merge_data_of_all_months".lower()
    global_table_name = f"user_{user_id}_total_country_global_data".lower()
    countris_table_name = f"user_{user_id}_{country}_table"

    # ---- define tables (exactly as your upload route) ----
    with engine.connect() as connection:
        connection.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
        connection.commit()
        
    user_monthly_data = Table(
        table_name, meta,
        Column('id', Integer, primary_key=True),
        Column('date_time', String),
        Column('settlement_id', String),
        Column('type', String),
        Column('order_id', String),
        Column('sku', String),
        Column('description', String),
        Column('quantity', Float),
        Column('price_in_gbp', Float),
        Column('cost_of_unit_sold', Float),
        Column('marketplace', String),
        Column('account_type', String),
        Column('fulfilment', String),
        Column('fulfillment', String),
        Column('order_city', String),
        Column('order_state', String),
        Column('order_postal', String),
        Column('tax_collection_model', String),
        Column('regulatory_fee', Float),
        Column('tax_on_regulatory_fee', Float),
        Column('bucket', String),
        Column('product_sales', Float),
        Column('product_sales_tax', Float),
        Column('postage_credits', Float),
        Column('shipping_credits', Float),
        Column('shipping_credits_tax', Float),
        Column('gift_wrap_credits', Float),
        Column('giftwrap_credits_tax', Float),
        Column('promotional_rebates', Float),
        Column('promotional_rebates_tax', Float),
        Column('sales_tax_collected', Float),
        Column('marketplace_withheld_tax', Float),
        Column('marketplace_facilitator_tax', Float),
        Column('selling_fees', Float),
        Column('percentage1', Float),
        Column('fba_fees', Float),
        Column('percentage2', Float),
        Column('other_transaction_fees', Float),
        Column('other', Float),
        Column('total', Float),
        Column('product_name', String),
        Column('currency', String),
        Column('advertising_cost', Float),
        Column('net_reimbursement', Float),
        Column('platform_fees', Float),
        Column('product_group', String),

        # Column('referral_fee', Float),

            
    )
    user_consolidated_data = Table(
        consolidated_table_name, meta,
        Column('id', Integer, primary_key=True),
        Column('date_time', String),
        Column('settlement_id', String),
        Column('type', String),
        Column('order_id', String),
        Column('sku', String),
        Column('description', String),
        Column('quantity', Float),
        Column('price_in_gbp', Float),
        Column('cost_of_unit_sold', Float),
        Column('marketplace', String),
        Column('account_type', String),
        Column('fulfilment', String),
        Column('fulfillment', String),
        Column('order_city', String),
        Column('order_state', String),
        Column('order_postal', String),
        Column('tax_collection_model', String),
        Column('regulatory_fee', Float),
        Column('tax_on_regulatory_fee', Float),
        Column('bucket', String),
        Column('product_sales', Float),
        Column('product_sales_tax', Float),
        Column('postage_credits', Float),
        Column('shipping_credits', Float),
        Column('shipping_credits_tax', Float),
        Column('gift_wrap_credits', Float),
        Column('giftwrap_credits_tax', Float),
        Column('promotional_rebates', Float),
        Column('promotional_rebates_tax', Float),
        Column('sales_tax_collected', Float),
        Column('marketplace_withheld_tax', Float),
        Column('marketplace_facilitator_tax', Float),
        Column('selling_fees', Float),
        Column('percentage1', Float),
        Column('fba_fees', Float),
        Column('percentage2', Float),
        Column('other_transaction_fees', Float),
        Column('other', Float),
        Column('total', Float),
        Column('month', String),
        Column('year', String),
        Column('product_name', String),
        Column('currency', String),
        Column('advertising_cost', Float),
        Column('net_reimbursement', Float),
        Column('platform_fees', Float),
        Column('product_group', String),

        # Column('referral_fee', Float),
    )
    user_global_table = Table(
        global_table_name, meta,
        Column('id', Integer, primary_key=True),
        Column('date_time', String),
        Column('settlement_id', String),
        Column('type', String),
        Column('order_id', String),
        Column('sku', String),
        Column('description', String),
        Column('quantity', Float),
        Column('price_in_gbp', Float),
        Column('cost_of_unit_sold', Float),
        Column('marketplace', String),
        Column('fulfilment', String),
        Column('fulfillment', String),
        Column('order_city', String),
        Column('order_state', String),
        Column('order_postal', String),
        Column('tax_collection_model', String),
        Column('product_sales', Float),
        Column('product_sales_tax', Float),
        Column('postage_credits', Float),
        Column('shipping_credits', Float),
        Column('shipping_credits_tax', Float),
        Column('gift_wrap_credits', Float),
        Column('giftwrap_credits_tax', Float),
        Column('promotional_rebates', Float),
        Column('promotional_rebates_tax', Float),
        Column('sales_tax_collected', Float),
        Column('marketplace_withheld_tax', Float),
        Column('marketplace_facilitator_tax', Float),
        Column('selling_fees', Float),
        Column('percentage1', Float),
        Column('fba_fees', Float),
        Column('percentage2', Float),
        Column('other_transaction_fees', Float),
        Column('other', Float),
        Column('total', Float),
        Column('month', String),
        Column('year', String),
        Column('product_name', String),
        Column('country', String),
        extend_existing=True
    )
    meta.create_all(engine)

    with engine.connect() as connection:
         # Delete previous records
        connection.execute(text(f"DELETE FROM {user_consolidated_data} WHERE month = '{month}' AND year = '{year}'"))
        connection.commit()

    with engine.connect() as connection:
        # Delete previous records from user_total_country_global_data
        connection.execute(
            text(f"DELETE FROM {global_table_name} WHERE month = :month AND year = :year AND country = :country"),
            {"month": month, "year": year, "country": country}
        )
        connection.commit()

    # ---- CLEAN/REMAP just like your upload route ----
    df = df_raw.copy()
    df.columns = [str(c).strip().lower() for c in df.columns]
    # settlement JSON keys already match your mapping names below; still keep:
    df.rename(columns=COLUMN_MAPPING, inplace=True)

    if 'marketplace_withheld_tax' not in df.columns:
        df['marketplace_withheld_tax'] = 0
    if 'marketplace_facilitator_tax' not in df.columns:
        df['marketplace_facilitator_tax'] = 0

    df['marketplace_withheld_tax'] = pd.to_numeric(df['marketplace_withheld_tax'], errors='coerce').fillna(0)
    df['marketplace_facilitator_tax'] = pd.to_numeric(df['marketplace_facilitator_tax'], errors='coerce')

    # If facilitator is missing/zero, copy withheld into it
    mask_fac_missing = df['marketplace_facilitator_tax'].isna() | (df['marketplace_facilitator_tax'] == 0)
    df.loc[mask_fac_missing, 'marketplace_facilitator_tax'] = df['marketplace_withheld_tax']

    # required numeric sanitation
    def clean_numeric_value(val):
        if isinstance(val, str):
            if ',' in val: val = val.replace(',', '')
            try: return float(val)
            except ValueError: return None
        return val

    numeric_columns = [
        'quantity','price_in_gbp','cost_of_unit_sold','product_sales','product_sales_tax',
        'postage_credits','shipping_credits_tax','gift_wrap_credits','giftwrap_credits_tax',
        'promotional_rebates','promotional_rebates_tax','sales_tax_collected',
        'marketplace_withheld_tax','marketplace_facilitator_tax','selling_fees',
        'percentage1','fba_fees','percentage2','other_transaction_fees','other','total','advertising_cost',
        'net_reimbursement',
        'platform_fees'
    ]
    numeric_columns = [c for c in numeric_columns if c in df.columns]
    for c in numeric_columns:
        df[c] = df[c].apply(clean_numeric_value)
        df[c] = pd.to_numeric(df[c], errors='coerce')

    # backfill missing mapped cols
    for col in COLUMN_MAPPING.values():
        if col not in df.columns:
            df[col] = 0.0 if col in numeric_columns else None

    # join SKU price/currency/product_name
    if country.upper() == 'UK':
        sku_column = 'sku_uk'
    elif country.upper() == 'US':
        sku_column = 'sku_us'
    else:
        raise ValueError("Unsupported country")

            
    with engine.connect() as conn:
        country_df = pd.read_sql(f"SELECT {sku_column} AS sku, price,currency, product_name FROM {country_table_name}", conn)

    df = df.merge(country_df, on='sku', how='left')

    with engine.connect() as conn:
        countries_df = pd.read_sql(f"SELECT sku, product_group FROM {countris_table_name}", conn)

    df = df.merge(countries_df, on='sku', how='left')

    # if 'sku' in df.columns:
    #     sku_list = df['sku'].dropna().unique().tolist()

    #     if sku_list:
    #         with engine1.connect() as conn:   # engine1 = admin DB (jaha category hai)
    #             placeholders = ','.join([f":sku_{i}" for i in range(len(sku_list))])

    #             cat_query = text(f"""
    #                 SELECT sku, referral_fee_percent_est
    #                 FROM category
    #                 WHERE sku IN ({placeholders})
    #             """)

    #             params = {f"sku_{i}": sku for i, sku in enumerate(sku_list)}

    #             category_df = pd.read_sql(cat_query, conn, params=params)

    #         # Join category info into df
    #         df = df.merge(category_df, on='sku', how='left')

    #         # 👇 Percent ko as-is referral_fee column me store karo
    #         df.rename(columns={'referral_fee_percent_est': 'referral_fee'}, inplace=True)
    #     else:
    #         # koi asin hi nahi mila
    #         df['referral_fee'] = None
    # else:
    #     # df me asin column hi nahi hai
    #     df['referral_fee'] = None
    # # Drop the temporary merge columns that pandas adds (_x, _y)
    # bad_columns = [c for c in df.columns if c.endswith('_x') or c.endswith('_y')]
    # df = df.drop(columns=bad_columns)

    # Rename the remaining 'currency' column to match the table
    if 'currency' not in df.columns and 'currency_x' in df.columns:
        df['currency'] = df.pop('currency_x')


    with engine1.connect() as conn:
        currency_query = text("""
            SELECT conversion_rate
            FROM currency_conversion 
            WHERE lower(user_currency) = :currency 
            AND lower(country) = :country 
            AND lower(month) = :month 
            AND year = :year
            LIMIT 1
        """)

        result = conn.execute(
            # currency_query,
            # {
            #     "currency": country_df['currency'].dropna().iloc[0].lower(),  # Get any available currency from df
            #     "country": country.lower(),
            #     "month": month.lower(),
            #     "year": year
            # }

            currency_query,
            {
                "currency": (
                    country_df.get("currency", pd.Series()).dropna().iloc[0].lower()
                    if not country_df.get("currency", pd.Series()).dropna().empty
                    else "usd"
                ),
                "country": country.lower(),
                "month": month.lower(),
                "year": year
            }

        ).fetchone()

# Step 3: Apply conversion rate to calculate price_in_gbp
    conversion_rate = result[0] if result else None

    if conversion_rate:
        df['price_in_gbp'] = df['price'] * conversion_rate
    else:
        df['price_in_gbp'] = None  # Or handle fallback logic if conversion rate not found

    
    # Calculate cost_of_unit_sold where both values are not null
    df['cost_of_unit_sold'] = df.apply(lambda row: row['price_in_gbp'] * row['quantity'] 
                                      if pd.notnull(row['price_in_gbp']) and pd.notnull(row['quantity']) 
                                      else None, axis=1)

    df.drop(columns=['price'], inplace=True)

    # Clean the 'total' column specifically since it appears in the error message
    if 'total' in df.columns:
        df['total'] = df['total'].apply(clean_numeric_value)

    # Handle the case where any numeric column might still have commas or other formatting issues
    for col in [c for c in df.columns if c in numeric_columns]:
        if col in df.columns:
            # Convert to float with NaN for any unconvertible values
            df[col] = pd.to_numeric(df[col], errors='coerce')

    for col in df.columns:
                if df[col].dtype == 'object':  # likely a string column
                    if df[col].str.contains(',').any():
                        print(f"Column {col} contains comma-formatted numbers!")
    for col in df.columns:
                if df[col].dtype == 'object':  # likely to have commas or bad data
                    # Remove commas and convert to numeric
                    df[col] = df[col].str.replace(',', '', regex=True)

            # Now convert all possible numeric columns to float
    df = df.apply(lambda x: pd.to_numeric(x, errors='ignore') if x.dtype == 'object' else x)

    # consolidated
    df_cons = df.copy()
    df_cons['month'] = month
    df_cons['year']  = year

    # keep only columns that the consolidated table actually has (exclude autoincrement id)
    valid_cols = [c.name for c in user_consolidated_data.columns if c.name != 'id']
    df_cons = df_cons.reindex(columns=valid_cols)

    df_cons.to_sql(consolidated_table_name, con=engine, if_exists='append', index=False)

    bad_columns = ['currency_x', 'currency_y']
    df = df.drop(columns=[c for c in bad_columns if c in df.columns])


    df.to_sql(table_name, con=engine, if_exists='append', index=False)
    
    df['month'] = month  # Assigning month from form data
    df['year'] = year 

    for col in df.columns:
                if df[col].dtype == 'object':  # likely a string column
                    if df[col].str.contains(',').any():
                        print(f"Column {col} contains comma-formatted numbers!")
    for col in df.columns:
                if df[col].dtype == 'object':  # likely to have commas or bad data
                    # Remove commas and convert to numeric
                    df[col] = df[col].str.replace(',', '', regex=True)

            # Now convert all possible numeric columns to float
    df = df.apply(lambda x: pd.to_numeric(x, errors='ignore') if x.dtype == 'object' else x)

    # df.to_sql(consolidated_table_name, con=engine, if_exists='append', index=False)


    # ✅ Step 1: Currency Rates Dictionary
    # if country.lower() == 'uk':
    #     currency1 = 'gbp'
    # elif country.lower() == 'us':
    #     currency1 = 'usd'
    # elif country.lower() == 'canada':
    #     currency1 = 'cad'
    # else:
    #     currency1 = 'usd'  # fallback/default if unknown

    # # Step 2: Fetch conversion rate from currency_conversion table
    # with engine1.connect() as conn:
    #     currency_query = text("""
    #         SELECT conversion_rate
    #         FROM currency_conversion 
    #         WHERE lower(user_currency) = :currency1
    #         AND lower(country) = 'us'
    #         AND lower(month) = :month 
    #         AND year = :year
    #         LIMIT 1
    #     """)
        
    #     result = conn.execute(currency_query, {
    #         "currency1": currency1,
    #         "country": "us",
    #         "month": month.lower(),
    #         "year": year
    #     }).fetchone()

    # # Step 3: Use the conversion rate
    # currency_rate  = result[0] if result else None

    # # Step 4: Create a USD-converted copy if conversion rate exists
    # df_usd = df.copy()

    # # ✅ Step 3: Convert monetary columns to USD if country found
    # if currency_rate :
    #     monetary_columns = [
    #         'product_sales', 'product_sales_tax', 'postage_credits', 
    #         'shipping_credits_tax', 'gift_wrap_credits', 'giftwrap_credits_tax', 
    #         'promotional_rebates', 'promotional_rebates_tax', 'sales_tax_collected',
    #         'marketplace_facilitator_tax', 'selling_fees', 'fba_fees', 
    #         'other_transaction_fees', 'other', 'total', 'price_in_gbp', 
    #         'cost_of_unit_sold'
    #     ]

    #     for col in monetary_columns:
    #         if col in df_usd.columns:
    #             df_usd[col] = pd.to_numeric(df_usd[col], errors='coerce') * currency_rate 
    # else:
    #     print("⚠️ No conversion rate found for:", currency1, country, month, year)

    
    # # fill NA like your code
    # for col in df_usd.columns:
    #     if df_usd[col].dtype == 'object' and col not in ['date_time','settlement_id','type','order_id','sku','description','marketplace','fulfilment','order_city','order_state','order_postal','tax_collection_model','month','year','country','product_name']:
    #         df_usd[col] = pd.to_numeric(df_usd[col], errors='coerce')
    # df_usd = df_usd.fillna({c:0.0 for c in df_usd.select_dtypes(include=['float64']).columns})
    # df_usd = df_usd.fillna({c:0 for c in df_usd.select_dtypes(include=['int64']).columns})
    # df_usd = df_usd.fillna('')
    # insert global

    # ------------------------------------------
# SAFE CURRENCY HANDLING (NEVER FAILS)
# ------------------------------------------

# Step 1: Set default currency for the country
    if country.lower() == 'uk':
        currency1 = 'gbp'
    elif country.lower() == 'us':
        currency1 = 'usd'
    elif country.lower() == 'canada':
        currency1 = 'cad'
    else:
        currency1 = 'usd'  # Always fallback safely

    # Step 2: Try to fetch conversion rate (NEVER FAIL)
    with engine1.connect() as conn:
        currency_query = text("""
            SELECT conversion_rate
            FROM currency_conversion 
            WHERE lower(user_currency) = :currency1
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

    # Step 3: SAFE fallback
    currency_rate = None
    if result and result[0] not in [None, 0, ""]:
        currency_rate = float(result[0])
    else:
        print(f"⚠️ Conversion rate NOT found for {currency1}, {country}, {month}-{year}. Proceeding without conversion.")
        currency_rate = None   # KEEP NONE (SAFE)

    # Step 4: Create USD df
    df_usd = df.copy()

    # Step 5: SAFE conversion — converts only if rate exists
    if currency_rate:
        monetary_columns = [
            'product_sales', 'product_sales_tax', 'postage_credits', 
            'shipping_credits_tax', 'gift_wrap_credits', 'giftwrap_credits_tax', 
            'promotional_rebates', 'promotional_rebates_tax', 'sales_tax_collected',
            'marketplace_facilitator_tax', 'selling_fees', 'fba_fees', 
            'other_transaction_fees', 'other', 'total', 'price_in_gbp', 
            'cost_of_unit_sold'
        ]

        for col in monetary_columns:
            if col in df_usd.columns:
                df_usd[col] = pd.to_numeric(df_usd[col], errors='coerce') * currency_rate

    # Step 6: SAFE numeric cleanup
    for col in df_usd.columns:
        if df_usd[col].dtype == 'object' and col not in [
            'date_time','settlement_id','type','order_id','sku','description',
            'marketplace','fulfilment','order_city','order_state','order_postal',
            'tax_collection_model','month','year','country','product_name'
        ]:
            df_usd[col] = pd.to_numeric(df_usd[col], errors='coerce')

    df_usd = df_usd.fillna({c:0.0 for c in df_usd.select_dtypes(include=['float64']).columns})
    df_usd = df_usd.fillna({c:0 for c in df_usd.select_dtypes(include=['int64']).columns})
    df_usd = df_usd.fillna('')

    with engine.begin() as conn:
        conn.execute(user_global_table.insert(), df_usd.to_dict(orient='records'))

    
    REQUIRED_COLUMNS = [
            "order_id", "sku", "description", "product_sales", "product_sales_tax", 
            "postage_credits", "shipping_credits_tax", "gift_wrap_credits", "giftwrap_credits_tax", 
            "promotional_rebates", "promotional_rebates_tax", "marketplace_facilitator_tax", 
            "selling_fees", "fba_fees", "other_transaction_fees", "errorstatus", "answer", 
            "difference", "fbaerrorstatus", "fbaanswer"  # Use original capitalization
    ]

    # referral fees + apply_modifications + charts (same as your /upload)
    # sku_list = df['sku'].dropna().tolist()
    # referral_fees = get_referral_fees(user_id, country, sku_list)
    # if referral_fees is None:
    #     return {"success": False, "message": "Referral fee not found for the SKUs in the fetched settlement file."}

    df_modified = apply_modifications_fatch(df_cons, country)
    # enforce numeric
    for col in numeric_columns:
        if col in df_modified.columns:
            df_modified[col] = pd.to_numeric(df_modified[col], errors='coerce')

    # overwrite monthly with modified data (your upload does replace)
    df_modified.to_sql(table_name, con=engine, if_exists='replace', index=False, method='multi')

    # make excel blob + PnL + charts exactly like upload
    excel_output = io.BytesIO()
    df_modified.to_excel(excel_output, index=False); excel_output.seek(0)

    table_name = f"user_{user_id}_{country}_{month}{year}_data"
    
    with engine.connect() as conn:
        query = f"""
        SELECT * FROM {table_name} 
        WHERE errorstatus IN ('cases to be inquired', 'NoReferralFee')
        AND sku <> '0'
        AND sku IS NOT NULL
        AND TRIM(sku) <> ''
        """
        print(f"Executing query on table: {table_name}")  # Debugging
        error_df = pd.read_sql(query, conn)
                
                # Filter error_df to keep only the required columns
    error_df = error_df[[col for col in REQUIRED_COLUMNS if col in error_df.columns]]

                # Save the error file if there are errors
    error_file_path = None
    error_file_base64 = None
    if not error_df.empty:
        error_filename = f"error_file_{user_id}{country}{month}_{year}.xlsx"
        error_file_path = os.path.join(UPLOAD_FOLDER, error_filename)                    
        error_df.to_excel(error_file_path, index=False)
                    
                    # Encode error file to base64
        with open(error_file_path, "rb") as error_file:
            error_file_base64 = base64.b64encode(error_file.read()).decode()


    quarter_mapping = {
        'january': 'Q1','february':'Q1','march':'Q1','april':'Q2','may':'Q2','june':'Q2',
        'july':'Q3','august':'Q3','september':'Q3','october':'Q4','november':'Q4','december':'Q4'
    }
    quarter = quarter_mapping[month]
    # UK/US split like your code
    if country == 'uk':
        
        # result = process_skuwise_data(user_id, country, month, year)
        # if result is None:
        #     total_cous, total_amazon_fee, cm2_profit = 0.0, 0.0, 0.0
        # else:
        # total_cous, total_amazon_fee, cm2_profit, rembursement_fee, platform_fee, total_expense, total_profit, total_fba_fees = process_skuwise_data(user_id, country, month, year)
        # ytd_pie_chart = process_yearly_skuwise_data(user_id, country, year)
        # qtd_pie_chart = process_quarterly_skuwise_data(user_id, country, month, year, quarter, db_url)
        # sales_pie_chart = create_sales_pie_chart(df_modified)
        # expense_pie_chart, cm2_margins, acos, rembursment_vs_cm2_margins, advertising_total, reimbursement_vs_sales, unit_sold, total_sales, otherwplatform, taxncredit = create_expense_pie_chart(df_modified, country, month, year)

        total_cous, total_amazon_fee, cm2_profit, rembursement_fee, platform_fee, total_expense, total_profit, total_fba_fees, advertising_total, taxncredit, reimbursement_vs_sales, cm2_margins, acos, rembursment_vs_cm2_margins, total_sales, unit_sold  = process_skuwise_data(user_id, country, month, year)
        ytd_pie_chart = process_yearly_skuwise_data(user_id, country, year)
        qtd_pie_chart = process_quarterly_skuwise_data(user_id, country, month, year, quarter, db_url)
    else:
        platform_fee, rembursement_fee, total_cous, total_amazon_fee, total_profit, total_expense, total_fba_fees, cm2_profit, cm2_margins, acos, rembursment_vs_cm2_margins, advertising_total, reimbursement_vs_sales, unit_sold, total_sales, otherwplatform, taxncredit = \
            process_skuwise_us_data(user_id, country, month, year)
        ytd_pie_chart = process_us_yearly_skuwise_data(user_id, country, year)
        qtd_pie_chart = process_us_quarterly_skuwise_data(user_id, country, month, year, quarter, db_url)

    process_global_monthly_skuwise_data(user_id, country, year, month)
    process_global_quarterly_skuwise_data(user_id, country, month, year, quarter, db_url)
    process_global_yearly_skuwise_data(user_id, country, year)

    # UploadHistory same as upload (file name N/A here)
    existing_entry = UploadHistory.query.filter_by(user_id=user_id, country=country, month=month, year=year).first()
    if existing_entry:
        db.session.delete(existing_entry); db.session.commit()
    new_upload = UploadHistory(
        user_id=user_id, year=year, month=month, country=country,
        file_name=f"amazon_settlement_{month}{year}.tsv",
        sales_chart_img=None, expense_chart_img=None,
        total_sales=float(total_sales), total_profit=float(total_profit),
        otherwplatform=platform_fee, taxncredit=float(taxncredit) if taxncredit is not None else 0.0,
        total_expense=float(total_expense), qtd_pie_chart=qtd_pie_chart, ytd_pie_chart=ytd_pie_chart,
        total_cous=float(total_cous), total_amazon_fee=float(total_amazon_fee),
        total_fba_fees=float(total_fba_fees), platform_fee=float(platform_fee),
        rembursement_fee=float(rembursement_fee), cm2_profit=float(cm2_profit),
        cm2_margins=float(cm2_margins), acos=float(acos),
        rembursment_vs_cm2_margins=float(rembursment_vs_cm2_margins),
        advertising_total=float(advertising_total), reimbursement_vs_sales=float(reimbursement_vs_sales),
        unit_sold=int(unit_sold)
    )
    db.session.add(new_upload); db.session.commit()

    # build response like /upload
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
        # "sales_chart_img": sales_pie_chart,
        # "expense_chart_img": expense_pie_chart,
        "total_sales": total_sales,
        "total_profit": total_profit,
        "otherwplatform": platform_fee,
        "taxncredit": taxncredit,
        "total_expense": total_expense,
        "total_fba_fees": total_fba_fees,
        "excel_file": base64.b64encode(excel_output.getvalue()).decode(),
        "platform_fee": platform_fee,
    }
    return replace_nan_with_null(response_data)

# ------------------------------------------------- Routes -------------------------------------------------
@amazon_api_bp.route("/amazon_api/login", methods=["GET"])
def amazon_login():

    # -------- auth --------
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
    
    _apply_region_and_marketplace_from_request()

    if amazon_client.marketplace_id not in amazon_client.ALLOWED_MARKETPLACES:
        return jsonify({"success": False, "error": "Unsupported marketplace"}), 400

    # Store in DB (create or update record for this user)
    au = amazon_user.query.filter_by(user_id=user_id).first()
    if not au:
        au = amazon_user(
            user_id=user_id,
            region=amazon_client.region,
            marketplace_id=amazon_client.marketplace_id,
            marketplace_name=amazon_client.marketplace_id,
            currency=None,
            refresh_token=""
        )
        db.session.add(au)
    else:
        au.region = amazon_client.region
        au.marketplace_id = amazon_client.marketplace_id

    db.session.commit()

    # IMPORTANT: encode user_id into state
    state = f"uid_{user_id}_{int(time.time())}"

    return jsonify({
        "success": True,
        "auth_url": amazon_client.get_oauth_url(state),
        "state": state
    })


@amazon_api_bp.route("/amazon_api/callback", methods=["GET"])
def amazon_oauth_callback():
    code = request.args.get("spapi_oauth_code")
    state = request.args.get("state") or ""

    # ------------------ Validate state ------------------
    # Expected: uid_{user_id}_{timestamp}
    if not state.startswith("uid_"):
        return make_response("Invalid state received", 400)

    try:
        parts = state.split("_")  # ["uid", "5", "1732451234"]
        user_id = int(parts[1])
    except Exception:
        return make_response("Invalid state format", 400)

    # ------------------ Exchange code for refresh token ------------------
    r = requests.post(
        AmazonSPAPIClient.TOKEN_URL,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        data={
            "grant_type": "authorization_code",
            "code": code,
            "client_id": amazon_client.client_id,
            "client_secret": amazon_client.client_secret,
            "redirect_uri": amazon_client.redirect_uri,
        },
        timeout=30
    )

    if r.status_code != 200:
        return make_response(f"Token exchange failed: {r.text}", 400)

    refresh = r.json().get("refresh_token")
    if not refresh:
        return make_response("No refresh token returned", 400)

    # ------------------ Save refresh token to DB ------------------
    au = amazon_user.query.filter_by(user_id=user_id).first()

    if not au:
        # In case login row wasn’t created (fallback safety)
        au = amazon_user(
            user_id=user_id,
            region=amazon_client.region,
            marketplace_id=amazon_client.marketplace_id,
            refresh_token=refresh,
        )
        db.session.add(au)
    else:
        au.refresh_token = refresh

    db.session.commit()

    # save to local memory & .refresh_token (optional)
    amazon_client.refresh_token = refresh
    try:
        with open(".refresh_token", "w") as f:
            f.write(refresh)
    except:
        pass

    # ------------------ Success HTML ------------------
    return """
        <html><body style='font-family: system-ui;'>
            <p>✅ Amazon account linked successfully. You may close this window.</p>
            <script>
                try {
                    if (window.opener) {
                        window.opener.postMessage(
                            { type: "amazon_oauth_success", refresh_token: "%s" },
                            "*"
                        );
                    }
                } catch(e) {}
                window.close();
            </script>
        </body></html>
    """ % refresh



@amazon_api_bp.route("/amazon_api/status", methods=["GET"])
def amazon_status():
    # -------- auth (same as amazon_login) --------
    auth_header = request.headers.get('Authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        return jsonify({'success': False, 'error': 'Authorization token is missing or invalid'}), 401

    token = auth_header.split(' ')[1]
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=['HS256'])
        user_id = payload['user_id']
    except jwt.ExpiredSignatureError:
        return jsonify({'success': False, 'error': 'Token has expired'}), 401
    except jwt.InvalidTokenError:
        return jsonify({'success': False, 'error': 'Invalid token'}), 401

    # -------- region + marketplace from request --------
    _apply_region_and_marketplace_from_request()

    if amazon_client.marketplace_id not in amazon_client.ALLOWED_MARKETPLACES:
        return jsonify({"success": False, "error": "Unsupported marketplace"}), 400

    # -------- load from DB for this user & region --------
    # if you support multi-region per user, filter by both user + region
    au = amazon_user.query.filter_by(
        user_id=user_id,
        region=amazon_client.region
    ).first()

    # No row at all: user hasn't even started the connect flow
    if not au:
        return jsonify({
            "success": False,
            "status": "no_record",
            "has_refresh_token": False,
        }), 200

    # Row exists but refresh_token is blank / null: OAuth not finished
    if not au.refresh_token:
        return jsonify({
            "success": False,
            "status": "pending",
            "has_refresh_token": False,
        }), 200

    # We DO have a refresh token in DB → set on client and make the API call
    amazon_client.refresh_token = au.refresh_token

    res = amazon_client.make_api_call("/sellers/v1/marketplaceParticipations", "GET")
    if res and "error" not in res:
        return jsonify({
            "success": True,
            "status": "connected",
            "has_refresh_token": True,
            "payload": res.get("payload") or []
        }), 200

    return jsonify({
        "success": False,
        "status": "sp_api_error",
        "has_refresh_token": True,  # token exists but API call failed
        "error": res
    }), 502

@amazon_api_bp.route("/amazon_api/health", methods=["GET"])
def amazon_health():
    ok = bool(amazon_client.get_access_token())
    return jsonify({"status": "healthy" if ok else "error"}), (200 if ok else 500)


@amazon_api_bp.route("/amazon_api/debug_env", methods=["GET"])
def debug_env():
    return jsonify({
        "region": amazon_client.region,
        "marketplace_id": amazon_client.marketplace_id,
        "api_base_url": amazon_client.api_base_url,
        "has_refresh_token": bool(amazon_client.refresh_token),
    })



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


@amazon_api_bp.route("/amazon_api/skus", methods=["GET"])
def list_skus():
    """
    Return seller SKUs from FBA inventory and store them in database.
    Query:
      - marketplace_id: optional (defaults from client)
      - store_in_db: optional (default: true) - set to false to skip database storage
    """
    auth_header = request.headers.get('Authorization')
    user_id = None

    if auth_header and auth_header.startswith('Bearer '):
        token = auth_header.split(' ')[1]
        try:
            payload = jwt.decode(token, SECRET_KEY, algorithms=['HS256'])
            user_id = payload.get('user_id')
        except jwt.ExpiredSignatureError:
            return jsonify({'error': 'Token has expired'}), 401
        except jwt.InvalidTokenError:
            return jsonify({'error': 'Invalid token'}), 401

    _apply_region_and_marketplace_from_request()

    if amazon_client.marketplace_id not in amazon_client.ALLOWED_MARKETPLACES:
        return jsonify({"success": False, "error": "Unsupported marketplace"}), 400

    mp = request.args.get("marketplace_id", amazon_client.marketplace_id)
    store_in_db = request.args.get("store_in_db", "true").lower() != "false"

    skus = _fetch_fba_skus_all(mp)

    out = {
        "success": True,
        "marketplace_id": mp,
        "count": len(skus),
        "skus": skus,
        "empty_message": "There is no SKU listed in this seller account." if not skus else None,
        "source": "fba-inventory",
        "db": {"saved_products": 0}
    }

    if store_in_db and skus:
        try:
            saved_count = _upsert_products_to_db(skus, mp, user_id)
            out["db"] = {"saved_products": saved_count}
            logger.info(f"Saved {saved_count} products to database for marketplace {mp}")
        except Exception as e:
            logger.error(f"Failed to save products to database: {e}")
            out["db"] = {"saved_products": 0, "error": str(e)}

    return jsonify(out), 200


@amazon_api_bp.route("/amazon_api/account", methods=["GET"])
def amazon_account():
    _apply_region_and_marketplace_from_request()

    if amazon_client.marketplace_id not in amazon_client.ALLOWED_MARKETPLACES:
        return jsonify({"success": False, "error": "Unsupported marketplace"}), 400

    if not amazon_client.refresh_token:
        return jsonify({"success": False, "error": "No refresh token. Complete OAuth."}), 400

    res = amazon_client.make_api_call("/sellers/v1/marketplaceParticipations", "GET")
    if not res or "error" in res:
        logger.error(f"Account fetch failed: {res}")
        return jsonify({
            "success": False,
            "message": "Failed to fetch account info",
            "details": res
        }), 502

    data = res.get("payload") if isinstance(res, dict) else res
    if data is None:
        logger.error(f"Unexpected account response shape: {res}")
        return jsonify({
            "success": False,
            "message": "Unexpected response from Amazon",
            "details": res
        }), 502

    accounts = []
    for item in (data if isinstance(data, list) else data.get("marketplaceParticipations", [])):
        mkt = (item or {}).get("marketplace", {})
        part = (item or {}).get("participation", {})
        accounts.append({
            "marketplaceId": mkt.get("id"),
            "marketplaceName": mkt.get("name"),
            "countryCode": mkt.get("countryCode"),
            "domainName": mkt.get("domainName"),
            "currency": mkt.get("defaultCurrencyCode"),
            "language": mkt.get("defaultLanguageCode"),
            "isParticipating": part.get("isParticipating"),
            "hasSuspendedListings": part.get("hasSuspendedListings"),
        })

    return jsonify({
        "success": True,
        "region": amazon_client.region,
        "marketplace_id": amazon_client.marketplace_id,
        "count": len(accounts),
        "accounts": accounts,
    }), 200



@amazon_api_bp.route("/amazon_api/connections", methods=["GET"])
def list_amazon_connections():
    # -------- auth --------
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

    rows = amazon_user.query.filter_by(user_id=user_id).all()

    return jsonify({
        "success": True,
        "connections": [
            {
                "region": r.region,
                "marketplace_id": r.marketplace_id,
                "marketplace_name": r.marketplace_name,
                "currency": r.currency,
            }
            for r in rows
        ]
    })


# from datetime import datetime, timezone

# def _month_date_range_utc(year: int, month: int) -> tuple[str, str]:
#     """
#     Returns (postedAfter, postedBefore) ISO8601 (UTC) for given month.
#     postedAfter is inclusive, postedBefore is exclusive.
#     """
#     start = datetime(year, month, 1, 0, 0, 0, tzinfo=timezone.utc)

#     if month == 12:
#         end = datetime(year + 1, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
#     else:
#         end = datetime(year, month + 1, 1, 0, 0, 0, tzinfo=timezone.utc)

#     def iso_z(dt: datetime) -> str:
#         return dt.isoformat().replace("+00:00", "Z")

#     return iso_z(start), iso_z(end)


# def _extract_order_id_from_related_identifiers(related_identifiers: list[dict] | None) -> Optional[str]:
#     if not related_identifiers:
#         return None
#     for rid in related_identifiers:
#         name = (rid or {}).get("relatedIdentifierName")
#         val = (rid or {}).get("relatedIdentifierValue")
#         if name == "ORDER_ID" and val:
#             return str(val)
#     return None


# def _extract_sku_and_qty_from_contexts(contexts: list[dict] | None) -> tuple[Optional[str], Optional[float]]:
#     """
#     ProductContext usually has sku, asin, quantityShipped, etc.
#     We'll just take the first one.
#     """
#     if not contexts:
#         return None, None
#     for ctx in contexts:
#         if (ctx or {}).get("contextType") == "ProductContext":
#             sku = ctx.get("sku")
#             qty = ctx.get("quantityShipped")
#             try:
#                 qty_f = float(qty) if qty is not None else None
#             except (TypeError, ValueError):
#                 qty_f = None
#             return sku, qty_f
#     return None, None


# def _sum_breakdowns_by_keyword(breakdowns: list[dict] | None, keyword: str) -> float:
#     """
#     Traverse breakdown tree and sum `breakdownAmount.currencyAmount`
#     where breakdownType contains keyword (case-insensitive).
#     """
#     if not breakdowns:
#         return 0.0

#     total = 0.0

#     def walk(nodes: list[dict]):
#         nonlocal total
#         for b in nodes or []:
#             btype = str(b.get("breakdownType", "")).lower()
#             if keyword in btype:
#                 amount = (((b.get("breakdownAmount") or {}).get("currencyAmount")) or 0)  # may be None
#                 try:
#                     total_amount = float(amount)
#                 except (TypeError, ValueError):
#                     total_amount = 0.0
#                 total += total_amount

#             nested = b.get("breakdowns")
#             if isinstance(nested, list):
#                 walk(nested)

#     walk(breakdowns)
#     return total


# def _flatten_transaction_to_row(tx: dict) -> dict:
#     """
#     Map a Finances v2024-06-19 Transaction object to your internal row schema.

#     Returns a dict using *normalized* column names (values of COLUMN_MAPPING),
#     e.g. 'date_time', 'order_id', 'sku', 'product_sales', 'selling_fees', etc.
#     """
#     posted_date = tx.get("postedDate")  # ISO8601 string
#     ttype = tx.get("transactionType")
#     tstatus = tx.get("transactionStatus")
#     desc = tx.get("description")
#     total_amount = (tx.get("totalAmount") or {}).get("currencyAmount")
#     marketplace_details = tx.get("marketplaceDetails") or {}
#     breakdowns = tx.get("breakdowns") or []

#     related_identifiers = tx.get("relatedIdentifiers") or []
#     order_id = _extract_order_id_from_related_identifiers(related_identifiers)

#     # Try to get SKU + quantity from first item.contexts
#     sku = None
#     quantity = None
#     items = tx.get("items") or []
#     if items:
#         item0 = items[0] or {}
#         ctxs = item0.get("contexts") or []
#         sku, quantity = _extract_sku_and_qty_from_contexts(ctxs)

#     # Marketplace name / id
#     marketplace_name = marketplace_details.get("marketplaceName")
#     marketplace_id = marketplace_details.get("marketplaceId")
#     marketplace = marketplace_name or marketplace_id

#     # Basic numeric amounts (best effort from transaction-level breakdowns)
#     product_sales = _sum_breakdowns_by_keyword(breakdowns, "sales")
#     # If for some transaction there are no "sales" breakdowns, fall back to totalAmount
#     if product_sales == 0.0:
#         try:
#             product_sales = float(total_amount)
#         except (TypeError, ValueError):
#             product_sales = 0.0

#     selling_fees = _sum_breakdowns_by_keyword(breakdowns, "fee")
#     fba_fees = _sum_breakdowns_by_keyword(breakdowns, "fba")
#     promo = _sum_breakdowns_by_keyword(breakdowns, "promotion")
#     tax = _sum_breakdowns_by_keyword(breakdowns, "tax")
#     shipping_credits = _sum_breakdowns_by_keyword(breakdowns, "shipping")

#     # Map into your normalized columns (same names you use in DB)
#     row = {
#         "date_time": posted_date,
#         "settlement_id": None,  # not present in Finances API; comes from legacy settlement reports
#         "type": ttype,
#         "order_id": order_id,
#         "sku": sku,
#         "description": desc,
#         "quantity": quantity,
#         "marketplace": marketplace,
#         "fulfilment": None,     # can be inferred from contexts if needed
#         "fulfillment": None,
#         "order_city": None,
#         "order_state": None,
#         "order_postal": None,
#         "tax_collection_model": None,
#         "product_sales": product_sales,
#         "product_sales_tax": tax,
#         "postage_credits": 0.0,
#         "shipping_credits": shipping_credits,
#         "shipping_credits_tax": 0.0,
#         "gift_wrap_credits": 0.0,
#         "giftwrap_credits_tax": 0.0,
#         "promotional_rebates": promo,
#         "promotional_rebates_tax": 0.0,
#         "sales_tax_collected": tax,
#         "marketplace_withheld_tax": 0.0,
#         "marketplace_facilitator_tax": 0.0,
#         "selling_fees": selling_fees,
#         "fba_fees": fba_fees,
#         "other_transaction_fees": 0.0,
#         "other": 0.0,
#         "total": product_sales - selling_fees - fba_fees,  # naive; tweak as you like
#         "account_type": None,
#         "regulatory_fee": 0.0,
#         "tax_on_regulatory_fee": 0.0,
#         "bucket": tstatus,  # just an example – up to you
#     }

#     return row

# from flask import Blueprint, Response, jsonify, make_response, request, send_file


# @amazon_api_bp.route("/amazon_api/finances/monthly_transactions", methods=["GET"])
# def finances_monthly_transactions():
#     """
#     Fetch Finances API v2024-06-19 transactions for a given month,
#     flatten them into COLUMN_MAPPING-style columns, and return JSON or Excel.

#     Query params:
#       - year (int, default: current UTC year)
#       - month (int, default: 6 for June)
#       - transaction_status (optional: RELEASED, DEFERRED, DEFERRED_RELEASED)
#       - marketplace_id (optional)
#       - format (optional: 'json' (default) or 'excel')
#     """
#     # -------- auth --------
#     auth_header = request.headers.get('Authorization')
#     if not auth_header or not auth_header.startswith('Bearer '):
#         return jsonify({'success': False, 'error': 'Authorization token is missing or invalid'}), 401

#     token = auth_header.split(' ')[1]
#     try:
#         payload = jwt.decode(token, SECRET_KEY, algorithms=['HS256'])
#         user_id = payload['user_id']
#     except jwt.ExpiredSignatureError:
#         return jsonify({'success': False, 'error': 'Token has expired'}), 401
#     except jwt.InvalidTokenError:
#         return jsonify({'success': False, 'error': 'Invalid token'}), 401

#     # -------- params --------
#     now_utc = datetime.now(timezone.utc)
#     try:
#         year = int(request.args.get("year", now_utc.year))
#         month = int(request.args.get("month", 6))  # default = June
#         if month < 1 or month > 12:
#             raise ValueError("month must be 1-12")
#     except ValueError:
#         return jsonify({"success": False, "error": "Invalid year or month"}), 400

#     # default to RELEASED if client doesn’t pass anything
#     transaction_status = request.args.get("transaction_status", "RELEASED")

#     marketplace_id = request.args.get("marketplace_id")

#     # ✅ NEW: choose response format
#     response_format = (request.args.get("format") or "json").lower()

#     # -------- region + marketplace from request (optional overrides) --------
#     _apply_region_and_marketplace_from_request()

#     # -------- load refresh token for this user + region --------
#     au = amazon_user.query.filter_by(
#         user_id=user_id,
#         region=amazon_client.region
#     ).first()

#     if not au or not au.refresh_token:
#         return jsonify({
#             "success": False,
#             "error": "Amazon account not connected for this region",
#             "status": "no_refresh_token"
#         }), 400

#     amazon_client.refresh_token = au.refresh_token

#     # -------- build date range for requested month (e.g. June) --------
#     posted_after, posted_before = _month_date_range_utc(year, month)

#     # -------- call Finances API listTransactions (with pagination) --------
#     params = {
#         "postedAfter": posted_after,
#         "postedBefore": posted_before,
#         "marketplaceId": marketplace_id or amazon_client.marketplace_id,
#     }
#     if transaction_status:
#         params["transactionStatus"] = transaction_status

#     all_rows: list[dict] = []

#     while True:
#         res = amazon_client.make_api_call(
#             "/finances/2024-06-19/transactions",
#             method="GET",
#             params=params,
#         )

#         if not res or "error" in res:
#             # Bubble up SP-API error details so you can debug
#             return jsonify({
#                 "success": False,
#                 "error": res or {"error": "Unknown SP-API error"},
#             }), 502

#         payload = res.get("payload") or res
#         transactions = payload.get("transactions") or []

#         for tx in transactions:
#             tstatus = (tx or {}).get("transactionStatus")
#             # hard filter in code too
#             if tstatus != "RELEASED":
#                 continue

#             row = _flatten_transaction_to_row(tx or {})
#             all_rows.append(row)


#         next_token = payload.get("nextToken")
#         if not next_token:
#             break

#         # Next page only needs nextToken
#         params = {"nextToken": next_token}

#     # ✅ If Excel requested, return file instead of JSON
#     if response_format == "excel":
#         if not all_rows:
#             # still return an empty sheet, but you could also return 204 if you prefer
#             df = pd.DataFrame()
#         else:
#             df = pd.DataFrame(all_rows)

#         output = io.BytesIO()
#         # xlsxwriter or openpyxl, both are fine if installed
#         with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
#             df.to_excel(writer, index=False, sheet_name="Transactions")

#         output.seek(0)
#         filename = f"finances_transactions_{year}_{month:02d}.xlsx"

#         return send_file(
#             output,
#             as_attachment=True,
#             download_name=filename,
#             mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
#         )

#     # 🔁 default JSON response (same as you already have)
#     return jsonify({
#         "success": True,
#         "year": year,
#         "month": month,
#         "count": len(all_rows),
#         "transactions": all_rows,
#     }), 200

from typing import Optional, Tuple, List, Dict, Any
from datetime import datetime, timezone
import io
import pandas as pd
from flask import jsonify, request, send_file

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
    total_amount = (tx.get("totalAmount") or {}).get("currencyAmount")

    marketplace_details = tx.get("marketplaceDetails") or {}
    marketplace = marketplace_details.get("marketplaceName") or marketplace_details.get("marketplaceId")

    order_id = _extract_order_id_from_related_identifiers(tx.get("relatedIdentifiers") or [])

    # Item-level (sku/qty + item breakdowns)
    sku = None
    quantity = None
    item_breakdowns: List[Dict[str, Any]] = []
    items = tx.get("items") or []
    if items:
        item0 = items[0] or {}
        sku, quantity = _extract_sku_and_qty_from_contexts(item0.get("contexts") or [])
        if isinstance(item0.get("breakdowns"), list):
            item_breakdowns = item0["breakdowns"]

    tx_breakdowns: List[Dict[str, Any]] = tx.get("breakdowns") or []

    # Leaf nodes
    item_leaves = _walk_leaf_breakdowns(item_breakdowns)
    tx_leaves = _walk_leaf_breakdowns(tx_breakdowns)

    # ------------------- PRODUCT SALES (VAT-INCLUSIVE AS YOU WANT) -------------------
    product_sales_net = _sum_where(
        item_leaves,
        lambda t: (_contains_any(t, ["principal", "itemprice"]) and ("tax" not in t))
    )

    # If principal missing, fall back to totalAmount
    if abs(product_sales_net) < 1e-9:
        try:
            product_sales_net = float(total_amount or 0.0)
        except (TypeError, ValueError):
            product_sales_net = 0.0

    product_sales_tax = _sum_where(
        item_leaves,
        lambda t: ("tax" in t) and ("shipping" not in t) and (not _contains_any(t, PROMO_KEYS))
    )

    # VAT-inclusive product sales
    product_sales = product_sales_net + product_sales_tax

    # ------------------- SHIPPING / POSTAGE -------------------
    shipping_credits = _sum_where(
        item_leaves,
        lambda t: (("shipping" in t or "shipcharge" in t or "shippingcharges" in t) and ("tax" not in t))
    )
    shipping_credits_tax = _sum_where(
        item_leaves,
        lambda t: ("shipping" in t and "tax" in t)
    )
    postage_credits = shipping_credits  # your UK behavior

    # ------------------- PROMOTIONS (SPLIT: rebates vs rebates_tax) -------------------
    promotional_rebates = _sum_where(
        item_leaves,
        lambda t: _contains_any(t, PROMO_KEYS) and ("tax" not in t)
    )
    promotional_rebates_tax = _sum_where(
        item_leaves,
        lambda t: _contains_any(t, PROMO_KEYS) and ("tax" in t)
    )

    # =========================================================
    # ✅ FEES + WITHHELD TAX (NO 2x) + DIVIDE BY 2 FOR FEES
    # =========================================================
    selling_fees = 0.0
    fba_fees = 0.0
    other_transaction_fees = 0.0
    marketplace_withheld_tax = 0.0
    marketplace_facilitator_tax = 0.0

    def _node_has_children(n: Dict[str, Any]) -> bool:
        ch = n.get("breakdowns")
        return isinstance(ch, list) and len(ch) > 0

    def _has_non_tax_fee_descendant(children: Optional[List[Dict[str, Any]]], fee_keys: List[str]) -> bool:
        """
        True if any descendant has a NON-TAX amount and matches fee_keys.
        If true, parent is a total -> skip parent to avoid 2x.
        """
        for c in children or []:
            ct = _btype(c)
            camt = _amt(c)
            is_tax = ("tax" in ct)
            is_fee = _contains_any(ct, fee_keys)
            if (not is_tax) and is_fee and abs(camt) > 1e-12:
                return True

            gch = c.get("breakdowns")
            if isinstance(gch, list) and gch:
                if _has_non_tax_fee_descendant(gch, fee_keys):
                    return True
        return False

    # Walk ALL nodes (for net fees + withheld/facilitator)
    for node, t, path in _walk_all_breakdowns_with_path(tx_breakdowns):
        amt = _amt(node)
        if abs(amt) < 1e-12:
            continue

        path_str = "".join(path)
        is_tax = ("tax" in t) or ("tax" in path_str)

        is_withheld = _contains_any(path_str, WITHHELD_TAX_KEYS)
        is_facilitator = _contains_any(path_str, FACILITATOR_TAX_KEYS)

        is_selling_fee = _contains_any(path_str, SELLING_FEE_KEYS)
        is_fba_fee = _contains_any(path_str, FBA_FEE_KEYS)

        # ✅ exclude ServiceFee / storage billing from fba_fees
        is_service_fee_like = (
            (ttype or "").lower().replace(" ", "") == "servicefee"
            or _contains_any(path_str, SERVICE_FEE_EXCLUDE_KEYS)
        )

        # --- Withheld / facilitator tax
        if is_withheld:
            marketplace_withheld_tax += amt
            continue
        if is_facilitator:
            marketplace_facilitator_tax += amt
            continue

        # --- NET Selling fee (avoid 2x by skipping parent totals)
        if is_selling_fee and (not is_tax) and (not is_fba_fee):
            children = node.get("breakdowns") if _node_has_children(node) else None
            if _has_non_tax_fee_descendant(children, SELLING_FEE_KEYS):
                continue
            selling_fees += amt
            continue

        # --- NET FBA fee (avoid 2x by skipping parent totals) + ✅ exclude ServiceFee storage rows
        if is_fba_fee and (not is_tax) and (not is_service_fee_like):
            children = node.get("breakdowns") if _node_has_children(node) else None
            if _has_non_tax_fee_descendant(children, FBA_FEE_KEYS):
                continue
            fba_fees += amt
            continue

    # --- OTHER TRANSACTION FEES: take only LEAF tax nodes to avoid 2x totals
    for node, t, path in _walk_all_breakdowns_with_path(tx_breakdowns):
        if _node_has_children(node):
            continue  # ✅ only leaf nodes

        amt = _amt(node)
        if abs(amt) < 1e-12:
            continue

        path_str = "".join(path)

        # skip withheld/facilitator tax from other_transaction_fees
        if _contains_any(path_str, WITHHELD_TAX_KEYS) or _contains_any(path_str, FACILITATOR_TAX_KEYS):
            continue

        is_tax = ("tax" in t) or ("tax" in path_str)
        is_fee_related = (
            _contains_any(path_str, SELLING_FEE_KEYS)
            or _contains_any(path_str, FBA_FEE_KEYS)
            or ("fee" in path_str)
        )

        if is_tax and is_fee_related and (not _contains_any(path_str, PROMO_KEYS)):
            other_transaction_fees += amt
            continue

        if _contains_any(path_str, ["chargeback", "holdback"]):
            other_transaction_fees += amt
            continue

    # Keep your sign normalization behavior
    if selling_fees > 0:
        selling_fees = -abs(selling_fees)
    # =========================================================
    # ✅ MERGE OTHER TRANSACTION FEES INTO FBA FEES
    # =========================================================

    # Move all other_transaction_fees into fba_fees
    if abs(other_transaction_fees) > 1e-12:
        fba_fees += other_transaction_fees
        other_transaction_fees = 0.0

    # Keep FBA fees negative
    if fba_fees > 0:
        fba_fees = -abs(fba_fees)

    if other_transaction_fees > 0:
        other_transaction_fees = -abs(other_transaction_fees)

    

    if marketplace_withheld_tax > 0:
        marketplace_withheld_tax = -abs(marketplace_withheld_tax)
    if marketplace_facilitator_tax > 0:
        marketplace_facilitator_tax = -abs(marketplace_facilitator_tax)

    # ✅ Your requirement: selling_fees and fba_fees should be 1x -> divide by 2
    # (do NOT affect other columns)
    selling_fees = selling_fees / 2.0 if abs(selling_fees) > 1e-12 else selling_fees
    fba_fees = fba_fees / 2.0 if abs(fba_fees) > 1e-12 else fba_fees

    # ------------------- SALES TAX COLLECTED (YOU WANT IT = WITHHELD TAX) -------------------
    sales_tax_collected = marketplace_withheld_tax
    if abs(sales_tax_collected) < 1e-9:
        # fallback if withheld not present
        sales_tax_collected = product_sales_tax + shipping_credits_tax
        if sales_tax_collected > 0:
            sales_tax_collected = -abs(sales_tax_collected)

    # ------------------- TOTAL (KEEP YOUR NET STYLE) -------------------
    total_calc = (
        product_sales
        + shipping_credits
        + postage_credits
        - promotional_rebates
        - promotional_rebates_tax
        + selling_fees
        + fba_fees
        + other_transaction_fees
        + marketplace_withheld_tax
        + marketplace_facilitator_tax
        - sales_tax_collected  # if you want settlement-style net; adjust if needed
    )

    # Build full MTD schema row
    row: Dict[str, Any] = {
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

        "promotional_rebates": promotional_rebates,          # ✅ excludes promo tax
        "promotional_rebates_tax": promotional_rebates_tax,  # ✅ promo tax separate

        "marketplace_withheld_tax": marketplace_withheld_tax,
        "marketplace_facilitator_tax": marketplace_facilitator_tax,

        "selling_fees": selling_fees,
        "fba_fees": fba_fees,
        "other_transaction_fees": other_transaction_fees,
        "other": 0.0,

        "sales_tax_collected": sales_tax_collected,          # ✅ equals withheld tax
        "regulatory_fee": 0.0,
        "tax_on_regulatory_fee": 0.0,
        "account_type": None,

        "total": total_calc,
        "bucket": tstatus,
    }

    return row


# =========================================================
# ROUTE
# =========================================================
@amazon_api_bp.route("/amazon_api/finances/monthly_transactions", methods=["GET"])
def finances_monthly_transactions():
    """
    Query params:
      - year (int, default: current UTC year)
      - month (int, default: 6)
      - transaction_status (optional, default RELEASED)
      - marketplace_id (optional)
      - transaction_type (optional, e.g. Shipment)
      - format (json | excel)
    """
    # -------- auth --------
    auth_header = request.headers.get("Authorization")
    if not auth_header or not auth_header.startswith("Bearer "):
        return jsonify({"success": False, "error": "Authorization token is missing or invalid"}), 401

    token = auth_header.split(" ")[1]
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=["HS256"])
        user_id = payload["user_id"]
    except jwt.ExpiredSignatureError:
        return jsonify({"success": False, "error": "Token has expired"}), 401
    except jwt.InvalidTokenError:
        return jsonify({"success": False, "error": "Invalid token"}), 401

    # -------- params --------
    now_utc = datetime.now(timezone.utc)
    try:
        year = int(request.args.get("year", now_utc.year))
        month = int(request.args.get("month", 6))
        if month < 1 or month > 12:
            raise ValueError("month must be 1-12")
    except ValueError:
        return jsonify({"success": False, "error": "Invalid year or month"}), 400

    transaction_status = request.args.get("transaction_status", "RELEASED")
    marketplace_id = request.args.get("marketplace_id")
    transaction_type_filter = request.args.get("transaction_type")
    response_format = (request.args.get("format") or "json").lower()

    # -------- region + marketplace override --------
    _apply_region_and_marketplace_from_request()

    # -------- load refresh token for this user + region --------
    au = amazon_user.query.filter_by(user_id=user_id, region=amazon_client.region).first()
    if not au or not au.refresh_token:
        return jsonify({
            "success": False,
            "error": "Amazon account not connected for this region",
            "status": "no_refresh_token",
        }), 400

    amazon_client.refresh_token = au.refresh_token

    # -------- date range --------
    posted_after, posted_before = _month_date_range_utc(year, month)

    # -------- call Finances API (pagination) --------
    params: Dict[str, Any] = {
        "postedAfter": posted_after,
        "postedBefore": posted_before,
        "marketplaceId": marketplace_id or amazon_client.marketplace_id,
    }
    if transaction_status:
        params["transactionStatus"] = transaction_status

    all_rows: List[Dict[str, Any]] = []

    while True:
        res = amazon_client.make_api_call(
            "/finances/2024-06-19/transactions",
            method="GET",
            params=params,
        )

        if not res or "error" in res:
            return jsonify({"success": False, "error": res or {"error": "Unknown SP-API error"}}), 502

        payload = res.get("payload") or res
        transactions = payload.get("transactions") or []

        for tx in transactions:
            tstatus = (tx or {}).get("transactionStatus")
            ttype = (tx or {}).get("transactionType")

            # hard filter
            if tstatus != "RELEASED":
                continue

            # optional type filter
            if transaction_type_filter and ttype != transaction_type_filter:
                continue

            all_rows.append(_flatten_transaction_to_row(tx or {}))

        next_token = payload.get("nextToken")
        if not next_token:
            break
        params = {"nextToken": next_token}

    # -------- excel --------
    if response_format == "excel":
        df = pd.DataFrame(all_rows) if all_rows else pd.DataFrame()
        # ✅ force full MTD schema (and ensure missing cols appear)
        df = df.reindex(columns=MTD_COLUMNS, fill_value=0.0)

        output = io.BytesIO()
        with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
            df.to_excel(writer, index=False, sheet_name="Transactions")
        output.seek(0)

        filename = f"finances_transactions_{year}_{month:02d}.xlsx"
        return send_file(
            output,
            as_attachment=True,
            download_name=filename,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

    # -------- json --------
    return jsonify({
        "success": True,
        "year": year,
        "month": month,
        "count": len(all_rows),
        "transactions": all_rows,
    }), 200
