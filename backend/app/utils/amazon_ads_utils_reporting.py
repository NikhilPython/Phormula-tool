from __future__ import annotations

import io
import gzip
import json
import time
import requests
import pandas as pd
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from flask import Blueprint, jsonify, request, send_file
import jwt
from config import Config

# --------------------------------------------------------------------------------------
# 1) YOUR SP-API STYLE CONSTANTS (KEEP)
# --------------------------------------------------------------------------------------

TOKEN_URL = "https://api.amazon.com/auth/o2/token"

# We only support NA + UK
PROD_ENDPOINTS = {
    "us-east-1": "https://sellingpartnerapi-na.amazon.com",   # US & CA
    "eu-west-1": "https://sellingpartnerapi-eu.amazon.com",   # UK
}

ALLOWED_MARKETPLACES = {
    "ATVPDKIKX0DER",  # US
    "A1F83G8C2ARO7P", # UK
    "A2EUQ1WTGCTBG2", # CA
}

DEFAULT_MARKETPLACE_BY_REGION = {
    "us-east-1": "ATVPDKIKX0DER",  # default: US
    "eu-west-1": "A1F83G8C2ARO7P", # default: UK
}

MARKETPLACE_REGION = {
    "ATVPDKIKX0DER": "us-east-1",  # US
    "A2EUQ1WTGCTBG2": "us-east-1", # CA
    "A1F83G8C2ARO7P": "eu-west-1", # UK
}

SELLER_CENTRAL_BY_MKT = {
    "ATVPDKIKX0DER": "https://sellercentral.amazon.com",    # US
    "A1F83G8C2ARO7P": "https://sellercentral.amazon.co.uk", # UK
    "A2EUQ1WTGCTBG2": "https://sellercentral.amazon.ca",    # CA
}

# --------------------------------------------------------------------------------------
# 2) ADS ENDPOINTS + mapping (this is what makes Ads work with your region style)
# --------------------------------------------------------------------------------------

ADS_ENDPOINTS = {
    "NA": "https://advertising-api.amazon.com",
    "EU": "https://advertising-api-eu.amazon.com",
    "FE": "https://advertising-api-fe.amazon.com",
}

SP_REGION_TO_ADS_REGION = {
    "us-east-1": "NA",
    "eu-west-1": "EU",
}

def get_ads_base_url(sp_region: str) -> str:
    ads_region = SP_REGION_TO_ADS_REGION.get(sp_region)
    if not ads_region:
        raise ValueError(f"Unsupported SP region for Ads mapping: {sp_region}")
    return ADS_ENDPOINTS[ads_region]

# --------------------------------------------------------------------------------------
# 3) Helper: your existing function probably does this already
#    Keeping a safe fallback so code is runnable.
# --------------------------------------------------------------------------------------

def _apply_region_and_marketplace_from_request(data: Dict[str, Any]) -> Tuple[str, str]:
    """
    Expects request payload to include either:
      - marketplace_id OR marketplace
      - optional region (sp region like us-east-1/eu-west-1)
    """
    mkt = data.get("marketplace_id") or data.get("marketplace") or DEFAULT_MARKETPLACE_BY_REGION["eu-west-1"]
    if mkt not in ALLOWED_MARKETPLACES:
        raise ValueError("Unsupported marketplace")
    region = data.get("region") or MARKETPLACE_REGION[mkt]
    if region not in PROD_ENDPOINTS:
        raise ValueError("Unsupported region")
    return region, mkt

# --------------------------------------------------------------------------------------
# 4) AMAZON ADS: token refresh + reporting v3
# --------------------------------------------------------------------------------------

def get_ads_access_token_from_refresh(refresh_token: str) -> str:
    """
    Uses same TOKEN_URL host, but requires ADS client id/secret + ADS refresh token.
    """
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
    return r.json()["access_token"]


@dataclass
class AmazonAdsAuthContext:
    access_token: str
    client_id: str
    profile_id: str  # REQUIRED for Sponsored Ads reporting


class AmazonAdsReportingClient:
    def __init__(self, sp_region: str, auth: AmazonAdsAuthContext, timeout: int = 60):
        self.base_url = get_ads_base_url(sp_region)
        self.auth = auth
        self.timeout = timeout

    def _headers(self) -> Dict[str, str]:
        # Correct Sponsored Ads headers
        return {
            "Authorization": f"Bearer {self.auth.access_token}",
            "Amazon-Advertising-API-ClientId": self.auth.client_id,
            "Amazon-Advertising-API-Scope": str(self.auth.profile_id),
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    def _request(self, method: str, path: str, **kwargs) -> requests.Response:
        url = f"{self.base_url}{path}"
        resp = requests.request(method, url, headers=self._headers(), timeout=self.timeout, **kwargs)
        if resp.status_code >= 400:
            raise RuntimeError(f"Amazon Ads API error {resp.status_code}: {resp.text}")
        return resp

    def create_sp_advertised_product_report(self, start_date: str, end_date: str, time_unit: str = "SUMMARY") -> str:
        payload = {
            "name": f"SP Advertised Product {start_date} to {end_date}",
            "startDate": start_date,
            "endDate": end_date,
            "configuration": {
                "adProduct": "SPONSORED_PRODUCTS",
                "reportTypeId": "spAdvertisedProduct",
                "timeUnit": time_unit,  # DAILY or SUMMARY
                "format": "GZIP_JSON",
                "columns": [
                    "campaignName",
                    "adGroupName",
                    "portfolioName",
                    "advertisedSku",
                    "advertisedAsin",
                    "impressions",
                    "clicks",
                    "cost",
                    "attributedSales7d",
                    "acosClicks7d",
                    "roasClicks7d",
                    "attributedConversions7d",
                    "attributedUnitsOrdered7d",
                    "attributedConversionsRate7d",
                    "attributedUnitsOrdered7dSameSku",
                    "attributedUnitsOrdered7dOtherSku",
                    "attributedSales7dSameSku",
                    "attributedSales7dOtherSku",
                    "currency",
                ],
            },
        }

        resp = self._request("POST", "/reporting/reports", json=payload)
        data = resp.json()
        report_id = data.get("reportId")
        if not report_id:
            raise RuntimeError(f"Missing reportId in response: {data}")
        return str(report_id)

    def get_report_status(self, report_id: str) -> Dict[str, Any]:
        return self._request("GET", f"/reporting/reports/{report_id}").json()

    def wait_until_ready(self, report_id: str, max_wait_seconds: int = 240, poll_every_seconds: int = 6) -> str:
        deadline = time.time() + max_wait_seconds
        last = None
        while time.time() < deadline:
            last = self.get_report_status(report_id)
            state = (last.get("status") or last.get("state") or "").upper()
            if state in {"COMPLETED", "SUCCESS"} and last.get("location"):
                return last["location"]
            if state in {"FAILED", "CANCELLED"}:
                raise RuntimeError(f"Report failed: {last}")
            time.sleep(poll_every_seconds)
        raise TimeoutError(f"Report not ready within {max_wait_seconds}s. Last status: {last}")

    def download_gzip_json(self, location_url: str) -> List[Dict[str, Any]]:
        r = requests.get(location_url, timeout=self.timeout)
        if r.status_code >= 400:
            raise RuntimeError(f"Download failed {r.status_code}: {r.text}")

        raw = gzip.decompress(r.content).decode("utf-8", errors="replace").strip()

        # JSON lines or JSON array
        rows: List[Dict[str, Any]] = []
        for line in raw.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                rows = []
                break

        if rows:
            return rows

        data = json.loads(raw)
        if isinstance(data, list):
            return data
        if isinstance(data, dict) and isinstance(data.get("rows"), list):
            return data["rows"]
        raise RuntimeError("Unknown report JSON format")

    @staticmethod
    def to_console_like_dataframe(rows: List[Dict[str, Any]], start_date: str, end_date: str, country: str) -> pd.DataFrame:
        df = pd.DataFrame(rows)

        def sdiv(a, b):
            b = b.replace({0: pd.NA})
            return (a / b).fillna(0.0)

        for col in ["impressions", "clicks", "cost", "attributedSales7d"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

        out = pd.DataFrame()
        out["Start Date"] = start_date
        out["End Date"] = end_date
        out["Portfolio name"] = df.get("portfolioName", "")
        out["Currency"] = df.get("currency", "")
        out["Campaign Name"] = df.get("campaignName", "")
        out["Ad Group Name"] = df.get("adGroupName", "")
        out["Country"] = country
        out["Advertised SKU"] = df.get("advertisedSku", "")
        out["Advertised ASIN"] = df.get("advertisedAsin", "")
        out["Impressions"] = df.get("impressions", 0)
        out["Clicks"] = df.get("clicks", 0)

        out["Click-Thru Rate (CTR)"] = sdiv(df.get("clicks", 0), df.get("impressions", 0))
        out["Cost Per Click (CPC)"] = sdiv(df.get("cost", 0.0), df.get("clicks", 0))
        out["Spend"] = df.get("cost", 0.0)
        out["7 Day Total Sales"] = df.get("attributedSales7d", 0.0)

        out["Total Advertising Cost of Sales (ACOS) "] = pd.to_numeric(
            df.get("acosClicks7d", 0.0), errors="coerce"
        ).fillna(0.0)

        out["Total Return on Advertising Spend (ROAS)"] = pd.to_numeric(
            df.get("roasClicks7d", 0.0), errors="coerce"
        ).fillna(0.0)

        out["7 Day Total Orders (#)"] = df.get("attributedConversions7d", 0)
        out["7 Day Total Units (#)"] = df.get("attributedUnitsOrdered7d", 0)
        out["7 Day Conversion Rate"] = pd.to_numeric(df.get("attributedConversionsRate7d", 0.0), errors="coerce").fillna(0.0)

        out["7 Day Advertised SKU Units (#)"] = df.get("attributedUnitsOrdered7dSameSku", 0)
        out["7 Day Other SKU Units (#)"] = df.get("attributedUnitsOrdered7dOtherSku", 0)
        out["7 Day Advertised SKU Sales"] = df.get("attributedSales7dSameSku", 0.0)
        out["7 Day Other SKU Sales"] = df.get("attributedSales7dOtherSku", 0.0)

        ordered_cols = [
            "Start Date", "End Date", "Portfolio name", "Currency", "Campaign Name", "Ad Group Name",
            "Country", "Advertised SKU", "Advertised ASIN", "Impressions", "Clicks",
            "Click-Thru Rate (CTR)", "Cost Per Click (CPC)", "Spend", "7 Day Total Sales",
            "Total Advertising Cost of Sales (ACOS) ", "Total Return on Advertising Spend (ROAS)",
            "7 Day Total Orders (#)", "7 Day Total Units (#)", "7 Day Conversion Rate",
            "7 Day Advertised SKU Units (#)", "7 Day Other SKU Units (#)",
            "7 Day Advertised SKU Sales", "7 Day Other SKU Sales",
        ]
        return out.reindex(columns=ordered_cols)

