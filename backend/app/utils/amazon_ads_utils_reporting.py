from __future__ import annotations

import gzip
import json
import time, re, uuid
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from config import Config

# LWA endpoints
TOKEN_URL = "https://api.amazon.com/auth/o2/token"
AUTH_URL = "https://www.amazon.com/ap/oa"
TOKENINFO_URL = "https://api.amazon.com/auth/o2/tokeninfo"

# Amazon Ads API endpoints
ADS_ENDPOINTS = {
    "NA": "https://advertising-api.amazon.com",
    "EU": "https://advertising-api-eu.amazon.com",
    # "FE": "https://advertising-api-fe.amazon.com",
}

# Correct Ads scope for reporting/campaign mgmt
ADS_SCOPE = "advertising::campaign_management"


def build_ads_lwa_auth_url(state: str) -> str:
    """
    Build LWA auth URL for Amazon Ads.
    prompt=consent is important to ensure refresh_token is returned again.
    """
    return (
        f"{AUTH_URL}"
        f"?client_id={Config.AMAZON_ADS_CLIENT_ID}"
        f"&scope={requests.utils.quote(ADS_SCOPE)}"
        f"&response_type=code"
        f"&redirect_uri={requests.utils.quote(Config.AMAZON_ADS_REDIRECT_URI)}"
        f"&state={requests.utils.quote(state)}"
        f"&prompt=consent"
    )


def exchange_code_for_tokens(code: str) -> Dict[str, Any]:
    r = requests.post(
        TOKEN_URL,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        data={
            "grant_type": "authorization_code",
            "code": code,
            "client_id": Config.AMAZON_ADS_CLIENT_ID,
            "client_secret": Config.AMAZON_ADS_CLIENT_SECRET,
            "redirect_uri": Config.AMAZON_ADS_REDIRECT_URI,
        },
        timeout=30,
    )
    if r.status_code != 200:
        raise RuntimeError(f"LWA code exchange failed: {r.status_code} {r.text}")
    return r.json()


def get_ads_access_token_from_refresh(refresh_token: str) -> str:
    r = requests.post(
        TOKEN_URL,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        data={
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
            "client_id": Config.AMAZON_ADS_CLIENT_ID,
            "client_secret": Config.AMAZON_ADS_CLIENT_SECRET,
        },
        timeout=30,
    )
    if r.status_code != 200:
        raise RuntimeError(f"Ads token refresh failed: {r.status_code} {r.text}")
    data = r.json()
    return data["access_token"]


def tokeninfo(access_token: str) -> Dict[str, Any]:
    """
    Works for Ads-scoped tokens. Use this instead of /user/profile.
    """
    r = requests.get(TOKENINFO_URL, params={"access_token": access_token}, timeout=30)
    try:
        body = r.json()
    except Exception:
        body = {"raw": r.text}
    return {"status": r.status_code, "body": body}


def list_profiles_region(
    access_token: str,
    region: str,
    scope_profile_id: Optional[str] = None,
) -> Tuple[int, List[Dict[str, Any]], Dict[str, Any]]:
    """
    Calls GET /v2/profiles in a region.
    If scope_profile_id is provided, uses Amazon-Advertising-API-Scope to list child advertiser profiles.
    Returns: (status_code, profiles_list, meta)
    """
    base = ADS_ENDPOINTS[region]
    url = f"{base}/v2/profiles"

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Amazon-Advertising-API-ClientId": Config.AMAZON_ADS_CLIENT_ID,
        "Accept": "application/json",
    }
    if scope_profile_id:
        headers["Amazon-Advertising-API-Scope"] = str(scope_profile_id)

    r = requests.get(url, headers=headers, timeout=30)

    meta = {
        "region": region,
        "url": url,
        "request_id": r.headers.get("x-amzn-RequestId")
        or r.headers.get("Amazon-Advertising-API-RequestId"),
        "content_type": r.headers.get("content-type"),
    }

    # Even for 200, Amazon sometimes returns non-json if something is wrong
    if not (r.headers.get("content-type") or "").startswith("application/json"):
        return r.status_code, [], {**meta, "error": r.text}

    data = r.json()
    if r.status_code != 200:
        return r.status_code, [], {**meta, "error": data}

    # Expected: JSON list
    profiles = data if isinstance(data, list) else []
    return r.status_code, profiles, meta


def list_top_level_profiles_all_regions(access_token: str) -> Dict[str, List[Dict[str, Any]]]:
    out: Dict[str, List[Dict[str, Any]]] = {}
    for region in ADS_ENDPOINTS.keys():
        try:
            _, profiles, _ = list_profiles_region(access_token, region, scope_profile_id=None)
            out[region] = profiles
        except Exception as e:
            # don't kill everything if FE fails DNS
            out[region] = []
    return out



def find_manager_profile_id(top_profiles_by_region: Dict[str, List[Dict[str, Any]]]) -> Optional[str]:
    """
    Finds the first profile with accountType/type == MANAGER.
    """
    for _, profiles in top_profiles_by_region.items():
        for p in profiles or []:
            acct_type = (p.get("accountType") or p.get("type") or "").upper()
            if acct_type == "MANAGER" and p.get("profileId"):
                return str(p["profileId"])
    return None


def list_child_profiles_all_regions(access_token: str, manager_profile_id: str) -> Dict[str, List[Dict[str, Any]]]:
    out: Dict[str, List[Dict[str, Any]]] = {}
    for region in ADS_ENDPOINTS.keys():
        try:
            _, profiles, _ = list_profiles_region(access_token, region, scope_profile_id=manager_profile_id)
            out[region] = profiles
        except Exception:
            out[region] = []
    return out



def pick_profile_id(profiles: List[Dict[str, Any]], wanted_country_codes: set[str]) -> Optional[str]:
    wanted = {c.upper() for c in wanted_country_codes}
    for p in profiles or []:
        pid = p.get("profileId")
        if not pid:
            continue

        cc = (
            p.get("countryCode")
            or (p.get("accountInfo") or {}).get("countryCode")
            or ""
        )
        cc = str(cc).upper()

        if cc in wanted:
            return str(pid)
    return None


@dataclass
class AmazonAdsAuthContext:
    access_token: str
    profile_id: str



class AmazonAdsReportingClient:
    """
    Amazon Ads Reporting v3 client
    - POST /reporting/reports  (create)
    - GET  /reporting/reports/{reportId} (status)
    - download via returned url (S3)
    """

    def __init__(self, base_url: str, auth: "AmazonAdsAuthContext", timeout: int = 60):
        self.base_url = base_url.rstrip("/")
        self.auth = auth

        # We'll use tuple timeouts: (connect_timeout, read_timeout)
        # connect timeout low-ish, read timeout higher
        self.connect_timeout = 15
        self.read_timeout = max(60, int(timeout))

        self.session = requests.Session()
        retry = Retry(
            total=6,
            connect=6,
            read=6,
            status=6,
            backoff_factor=1.0,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset(["GET", "POST"]),
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=20)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

    def list_report_types(self) -> list[dict]:
        """
        GET /reporting/reportTypes
        Returns all report types available for this account.
        """
        resp = self._request("GET", "/reporting/reportTypes")
        data = resp.json()
        # usually list[dict], but keep defensive
        return data if isinstance(data, list) else data.get("reportTypes", [])

    def get_report_type(self, report_type_id: str) -> dict:
        """
        GET /reporting/reportTypes/{reportTypeId}
        Shows allowed columns / groupBy for that report type.
        """
        resp = self._request("GET", f"/reporting/reportTypes/{report_type_id}")
        return resp.json()

    def _headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self.auth.access_token}",
            "Amazon-Advertising-API-ClientId": Config.AMAZON_ADS_CLIENT_ID,
            "Amazon-Advertising-API-Scope": str(self.auth.profile_id),
            # v3 create media type
            "Content-Type": "application/vnd.createasyncreportrequest.v3+json",
            "Accept": "application/json",
        }

    def _timeout(self):
        return (self.connect_timeout, self.read_timeout)

    def _request(self, method: str, path: str, **kwargs) -> requests.Response:
        url = f"{self.base_url}{path}"
        resp = self.session.request(
            method,
            url,
            headers=self._headers(),
            timeout=self._timeout(),
            **kwargs,
        )

        # If retry exhausted, we may still get 4xx/5xx here
        if resp.status_code >= 400:
            raise RuntimeError(f"Amazon Ads API error {resp.status_code}: {resp.text}")

        return resp

    def create_sp_advertised_product_report(
        self, start_date: str, end_date: str, time_unit: str = "SUMMARY"
    ) -> str:
        unique_name = f"SP Advertised Product {start_date} to {end_date} {uuid.uuid4().hex[:8]}"

        payload = {
            "name": unique_name,
            "startDate": start_date,
            "endDate": end_date,
            "configuration": {
                "adProduct": "SPONSORED_PRODUCTS",
                "reportTypeId": "spAdvertisedProduct",
                "timeUnit": time_unit,
                "format": "GZIP_JSON",
                "groupBy": ["advertiser"],
                "columns": [
                    "startDate", "endDate",
                    "campaignId", "campaignName",
                    "adGroupId", "adGroupName",
                    "portfolioId",
                    "campaignBudgetCurrencyCode",
                    "advertisedSku", "advertisedAsin",
                    "impressions", "clicks",
                    "clickThroughRate",
                    "cost", "costPerClick",
                    "sales7d", "purchases7d", "unitsSoldClicks7d",
                    "acosClicks7d", "roasClicks7d",
                    "attributedSalesSameSku7d",
                    "salesOtherSku7d",
                    "purchasesSameSku7d",
                    "unitsSoldSameSku7d",
                    "unitsSoldOtherSku7d",
                ],
            },
        }

        url = f"{self.base_url}/reporting/reports"
        resp = self.session.post(url, headers=self._headers(), json=payload, timeout=self._timeout())

        # success
        if resp.status_code in (200, 202):
            data = resp.json()
            rid = data.get("reportId")
            if not rid:
                raise RuntimeError(f"Missing reportId in response: {data}")
            return str(rid)

        # duplicate -> reuse reportId
        if resp.status_code == 425:
            try:
                data = resp.json()
            except Exception:
                data = {"raw": resp.text}

            detail = ""
            if isinstance(data, dict):
                detail = str(data.get("detail") or data.get("message") or "")

            m = re.search(r"duplicate of\s*:?\s*([0-9a-fA-F-]{16,})", detail)
            if m:
                return m.group(1)

            raise RuntimeError(f"Duplicate report but could not parse reportId. Body: {data}")

        raise RuntimeError(f"Amazon Ads API error {resp.status_code}: {resp.text}")

    def get_report_status(self, report_id: str) -> Dict[str, Any]:
        return self._request("GET", f"/reporting/reports/{report_id}").json()

    def wait_until_ready(
        self,
        report_id: str,
        max_wait_seconds: int = 1800,
        poll_every_seconds: int = 10,
    ) -> str:
        """
        IMPORTANT:
        - v3 typically returns status COMPLETED and a field `url`
        - network timeouts happen; we keep polling instead of failing immediately
        """
        deadline = time.time() + max_wait_seconds
        last: Optional[Dict[str, Any]] = None
        transient_errors = 0

        while time.time() < deadline:
            try:
                last = self.get_report_status(report_id)
                transient_errors = 0
            except (requests.exceptions.RequestException, RuntimeError) as e:
                # If it's a network issue, keep polling (don't 500 immediately)
                msg = str(e)
                if "timed out" in msg.lower() or "connection" in msg.lower():
                    transient_errors += 1
                    time.sleep(min(poll_every_seconds * transient_errors, 60))
                    continue
                raise

            status = (last.get("status") or "").upper()

            if status in {"COMPLETED", "SUCCESS"}:
                url = last.get("url") or last.get("location")
                if url:
                    return url

            if status in {"FAILURE", "FAILED", "CANCELLED"}:
                raise RuntimeError(f"Report failed: {last}")

            time.sleep(poll_every_seconds)

        # last-chance: if completed but we timed out, still return url if present
        if isinstance(last, dict) and (last.get("status") or "").upper() in {"COMPLETED", "SUCCESS"}:
            url = last.get("url") or last.get("location")
            if url:
                return url

        raise TimeoutError(f"Report not ready within {max_wait_seconds}s. Last status: {last}")

    def download_gzip_json(self, location_url: str) -> List[Dict[str, Any]]:
        r = self.session.get(location_url, timeout=self._timeout())
        if r.status_code >= 400:
            raise RuntimeError(f"Download failed {r.status_code}: {r.text}")

        content = r.content
        try:
            content = gzip.decompress(content)
        except OSError:
            pass

        raw = content.decode("utf-8", errors="replace").strip()

        def _normalize(obj: Any) -> List[Dict[str, Any]]:
            # already list of dicts
            if isinstance(obj, list) and (not obj or isinstance(obj[0], dict)):
                return obj

            # dict wrapper
            if isinstance(obj, dict):
                # common: {"rows":[{...},{...}]}
                if isinstance(obj.get("rows"), list) and (not obj["rows"] or isinstance(obj["rows"][0], dict)):
                    return obj["rows"]

                # common: {"columns":[...], "rows":[[...],[...]]}
                cols = obj.get("columns")
                rows = obj.get("rows")
                if isinstance(cols, list) and isinstance(rows, list) and rows and isinstance(rows[0], list):
                    out: List[Dict[str, Any]] = []
                    for row in rows:
                        d = {str(cols[i]): row[i] if i < len(row) else None for i in range(len(cols))}
                        out.append(d)
                    return out

            # ndjson string lines maybe already parsed earlier; fallthrough = unsupported
            raise RuntimeError(f"Unknown report JSON format. Top-level type={type(obj)} keys={getattr(obj,'keys',lambda:[])()}")

        # Try NDJSON first
        ndjson: List[Dict[str, Any]] = []
        ok = True
        for line in raw.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                item = json.loads(line)
            except json.JSONDecodeError:
                ok = False
                break
            # each NDJSON line must be a dict row
            if isinstance(item, dict):
                ndjson.append(item)
            else:
                ok = False
                break
        if ok and ndjson:
            return ndjson

        # Otherwise normal JSON
        data = json.loads(raw)
        return _normalize(data)

    # def create_sb_keyword_report(self, start_date: str, end_date: str, time_unit: str = "SUMMARY") -> str:
    #     """
    #     Sponsored Brands - Keyword report (v3)
    #     NOTE: reportTypeId differs by account; most commonly sbKeywords.
    #     """
    #     payload = {
    #         "name": f"SB Keyword {start_date} to {end_date} {uuid.uuid4().hex[:8]}",
    #         "startDate": start_date,
    #         "endDate": end_date,
    #         "configuration": {
    #             "adProduct": "SPONSORED_BRANDS",
    #             "reportTypeId": "sbTargeting",   # <- if your account uses different id, change here
    #             "timeUnit": time_unit,
    #             "format": "GZIP_JSON",
    #             "groupBy": ["targeting"],
    #             "columns": [
    #                 "startDate", "endDate",
    #                 "portfolioName",
    #                 "currency",
    #                 "campaignName",
    #                 "adGroupName",
    #                 "targeting",
    #                 "matchType",
    #                 "costType",
    #                 "impressions",
    #                 "topOfSearchImpressionShare",
    #                 "viewableImpressions",
    #                 "clicks",
    #                 "clickThroughRate",
    #                 "cost",
    #                 "costPerClick",
    #                 "vCPM",
    #                 "acos",
    #                 "roas",
    #                 "sales14d",
    #                 "purchases14d",
    #                 "unitsSold14d",
    #                 "conversionRate14d",
    #                 "viewThroughRate",
    #                 "vctr",
    #                 "videoFirstQuartileViews",
    #                 "videoMidpointViews",
    #                 "videoThirdQuartileViews",
    #                 "videoCompleteViews",
    #                 "videoUnmutes",
    #                 "views5s",
    #                 "viewRate5s",
    #                 "brandedSearches14d",
    #                 "detailPageViews14d",
    #                 "newToBrandPurchases14d",
    #                 "newToBrandPurchasesPercentage14d",
    #                 "newToBrandSales14d",
    #                 "newToBrandSalesPercentage14d",
    #                 "newToBrandUnitsSold14d",
    #                 "newToBrandUnitsSoldPercentage14d",
    #                 "newToBrandOrderRate14d",
    #                 "acosClicks14d",
    #                 "roasClicks14d",
    #                 "salesClicks14d",
    #                 "purchasesClicks14d",
    #                 "unitsSoldClicks14d",
    #                 "brandTotalDetailPageViewsClicks14d",
    #             ],
    #         },
    #     }
    #     return self._create_report(payload)

    def create_sb_keyword_report(self, start_date: str, end_date: str, time_unit: str = "SUMMARY") -> str:
        payload = {
            "name": f"SB Targeting {start_date} to {end_date} {uuid.uuid4().hex[:8]}",
            "startDate": start_date,
            "endDate": end_date,
            "configuration": {
                "adProduct": "SPONSORED_BRANDS",
                "reportTypeId": "sbTargeting",
                "timeUnit": time_unit,
                "format": "GZIP_JSON",
                "groupBy": ["targeting"],     # keep
                "columns": [
                    "startDate", "endDate",
                    "campaignId", "campaignName",
                    "adGroupId", "adGroupName",

                    # targeting/keyword fields (pick what Amazon actually fills)
                    "keywordText",
                    "targetingText",
                    "targetingExpression",
                    "matchType",
                    "costType",

                    # currency
                    "campaignBudgetCurrencyCode",

                    # metrics
                    "impressions",
                    "viewableImpressions",
                    "topOfSearchImpressionShare",
                    "clicks",
                    "cost",
                ],
            },
        }
        return self._create_report(payload)


    # def create_sd_campaign_report(self, start_date: str, end_date: str, time_unit: str = "SUMMARY") -> str:
    #     payload = {
    #         "name": f"SD Campaign {start_date} to {end_date} {uuid.uuid4().hex[:8]}",
    #         "startDate": start_date,
    #         "endDate": end_date,
    #         "configuration": {
    #             "adProduct": "SPONSORED_DISPLAY",
    #             "reportTypeId": "sdCampaigns",  # might still be wrong for your account, see step 2
    #             "timeUnit": time_unit,
    #             "format": "GZIP_JSON",
    #             # ✅ FIXED
    #             "groupBy": ["campaign"],
    #             # ✅ ONLY allowed columns
    #             "columns": [
    #                 "startDate", "endDate",
    #                 "campaignId", "campaignName",
    #                 "campaignBudgetCurrencyCode",
    #                 "impressions", "clicks", "cost",
    #                 "detailPageViews", "detailPageViewsClicks",
    #                 "purchases", "purchasesClicks",
    #                 "unitsSold", "unitsSoldClicks",
    #                 "sales", "salesClicks",
    #                 "newToBrandPurchases", "newToBrandPurchasesClicks",
    #                 "newToBrandUnitsSold", "newToBrandUnitsSoldClicks",
    #                 "newToBrandSalesClicks",
    #                 "addToCart", "addToCartClicks", "addToCartViews", "addToCartRate", "eCPAddToCart",
    #                 "brandedSearches", "brandedSearchesClicks", "brandedSearchesViews",
    #                 "brandedSearchRate", "eCPBrandSearch",
    #                 "longTermSales", "longTermROAS",
    #             ],
    #         },
    #     }
    #     return self._create_report(payload)
 
    def create_sd_campaign_report(self, start_date: str, end_date: str, time_unit: str = "SUMMARY") -> str:
        payload = {
            "name": f"SD Campaign {start_date} to {end_date} {uuid.uuid4().hex[:8]}",
            "startDate": start_date,
            "endDate": end_date,
            "configuration": {
                "adProduct": "SPONSORED_DISPLAY",
                "reportTypeId": "sdCampaigns",      # keep, but validate via /reportTypes if needed
                "timeUnit": time_unit,
                "format": "GZIP_JSON",
                "groupBy": ["campaign"],           # ✅ advertiser is invalid for this reportType
                "columns": [                       # ✅ only allowed columns (based on your error)
                    "startDate", "endDate",
                    "campaignId", "campaignName",
                    "campaignBudgetCurrencyCode",
                    "impressions", "clicks", "cost",
                    "viewabilityRate",             # use to derive viewable impressions
                    "detailPageViews", "detailPageViewsClicks",

                    "purchases", "purchasesClicks",
                    "unitsSold", "unitsSoldClicks",
                    "sales", "salesClicks",

                    "newToBrandPurchases", "newToBrandPurchasesClicks",
                    "newToBrandUnitsSold", "newToBrandUnitsSoldClicks",
                    "newToBrandSalesClicks",       # note: your allowed list had clicks only

                    "addToCart", "addToCartClicks", "addToCartViews", "addToCartRate", "eCPAddToCart",
                    "brandedSearches", "brandedSearchesClicks", "brandedSearchesViews",
                    "brandedSearchRate", "eCPBrandSearch",

                    "longTermSales", "longTermROAS",
                ],
            },
        }
        return self._create_report(payload)
    
    def list_sd_campaigns(self) -> list[dict]:
        # SD campaigns endpoint is separate from reporting rows
        # Count/startIndex pagination can be added if you have many campaigns
        resp = self._request("GET", "/sd/campaigns", params={"count": 1000, "startIndex": 0})
        data = resp.json()
        return data if isinstance(data, list) else []

    def list_portfolios(self) -> list[dict]:
        # portfolios are usually v2
        resp = self._request("GET", "/v2/portfolios")
        data = resp.json()
        return data if isinstance(data, list) else []

    def create_sd_advertised_product_report(
        self, start_date: str, end_date: str, time_unit: str = "SUMMARY"
    ) -> str:
        payload = {
            "name": f"SD Advertised Product {start_date} {uuid.uuid4().hex[:6]}",
            "startDate": start_date,
            "endDate": end_date,
            "configuration": {
                "adProduct": "SPONSORED_DISPLAY",
                "reportTypeId": "sdAdvertisedProduct",  # keep this, but validate via /reportTypes if needed
                "timeUnit": time_unit,
                "format": "GZIP_JSON",
                "groupBy": ["advertiser"],  # if this errors later, switch to ["campaign"] or whatever /reportTypes says
                "columns": [
                    "startDate", "endDate",
                    "campaignId", "campaignName",
                    "adGroupId", "adGroupName",

                    # ✅ SD uses "promoted*" (per your allowed list)
                    "promotedSku", "promotedAsin",

                    "campaignBudgetCurrencyCode",

                    # ✅ core metrics that ARE allowed
                    "impressions", "clicks", "cost",
                    "sales", "purchases", "unitsSold",

                    # optional (also in your allowed list)
                    "salesClicks", "purchasesClicks", "unitsSoldClicks",
                    "newToBrandSales", "newToBrandPurchases", "newToBrandUnitsSold",
                    "newToBrandSalesClicks", "newToBrandPurchasesClicks", "newToBrandUnitsSoldClicks",
                    "viewabilityRate",
                ],
            },
        }
        return self._create_report(payload)


    def _create_report(self, payload: dict) -> str:
        """
        helper used by all report creators.
        """
        url = f"{self.base_url}/reporting/reports"
        resp = self.session.post(url, headers=self._headers(), json=payload, timeout=self._timeout())

        if resp.status_code in (200, 202):
            data = resp.json()
            rid = data.get("reportId")
            if not rid:
                raise RuntimeError(f"Missing reportId in response: {data}")
            return str(rid)

        if resp.status_code == 425:
            try:
                data = resp.json()
            except Exception:
                data = {"raw": resp.text}
            raise RuntimeError(f"Duplicate report response (425). Body: {data}")

        raise RuntimeError(f"Amazon Ads API error {resp.status_code}: {resp.text}")


