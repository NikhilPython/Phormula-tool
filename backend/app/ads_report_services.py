from datetime import datetime
import io
import time
import hashlib
import pandas as pd
import jwt

from flask import send_file, jsonify
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import func

from app.routes.advertisement_api_routes import (
    get_ads_access_token_from_refresh,
    list_top_level_profiles_all_regions,
    find_manager_profile_id,
    list_child_profiles_all_regions,
    ADS_ENDPOINTS,
    AmazonAdsAuthContext,
    AmazonAdsReportingClient,
)
from app.utils.ads_helpers import (
    _get_user_row,
    _to_date,
    _to_float,
    _to_int,
    _safe_div,
    _norm_key,
    _pick,
)
from app.models.user_models import db , amazon_sponsored_products
def run_sp_advertised_product_report_service(
    user_id,
    start_date,
    end_date,
    time_unit="SUMMARY",
    countries=None,
    return_excel=False,
):
    """
    Shared service for:
    /api/ads/manager/sp_advertised_product_report
    and Celery scheduled execution.
    """
    u = _get_user_row(user_id)

    wanted_countries = countries
    if wanted_countries:
        wanted_countries = {str(x).upper() for x in wanted_countries}
    else:
        wanted_countries = None

    if time_unit not in {"DAILY", "SUMMARY"}:
        raise ValueError("time_unit must be DAILY or SUMMARY")

    sd = _to_date(start_date, strict=True, field_name="start_date")
    ed = _to_date(end_date, strict=True, field_name="end_date")

    if not u.amazon_ads_refresh_token:
        raise ValueError("Amazon Ads not connected for this user.")

    access_token = get_ads_access_token_from_refresh(u.amazon_ads_refresh_token)

    top_profiles = list_top_level_profiles_all_regions(access_token)
    manager_profile_id = find_manager_profile_id(top_profiles)

    if manager_profile_id:
        child_by_region = list_child_profiles_all_regions(access_token, manager_profile_id)
    else:
        child_by_region = top_profiles

    all_profiles = []
    for region, profs in child_by_region.items():
        for p in profs or []:
            cc = (p.get("countryCode") or "").upper()
            label = "UK" if cc == "GB" else cc
            p["_region"] = region
            p["_country_label"] = label
            all_profiles.append(p)

    if wanted_countries:
        all_profiles = [p for p in all_profiles if p.get("_country_label") in wanted_countries]

    if not all_profiles:
        raise ValueError("No advertiser profiles found (or your country filter removed all).")

    merged_rows = []

    for p in all_profiles:
        profile_id = p.get("profileId")
        if not profile_id:
            continue

        region = p["_region"]
        base_url = ADS_ENDPOINTS[region]

        auth = AmazonAdsAuthContext(access_token=access_token, profile_id=str(profile_id))
        ads = AmazonAdsReportingClient(base_url=base_url, auth=auth, timeout=60)

        report_id = ads.create_sp_advertised_product_report(start_date, end_date, time_unit=time_unit)
        location = ads.wait_until_ready(report_id, max_wait_seconds=1800, poll_every_seconds=10)
        rows = ads.download_gzip_json(location)

        if not isinstance(rows, list):
            raise RuntimeError(f"Report returned unexpected type: {type(rows)}")

        for r in rows:
            if isinstance(r, dict):
                r["_profileId"] = str(profile_id)
                r["_country"] = p["_country_label"]
                merged_rows.append(r)

    if not merged_rows:
        raise ValueError("Reports returned no rows")

    df = pd.DataFrame(merged_rows)

    numeric_cols = [
        "impressions", "clicks", "cost", "costPerClick", "clickThroughRate",
        "sales7d", "purchases7d", "unitsSoldClicks7d",
        "acosClicks7d", "roasClicks7d",
        "attributedSalesSameSku7d", "salesOtherSku7d",
        "purchasesSameSku7d", "unitsSoldSameSku7d", "unitsSoldOtherSku7d",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    def sdiv(a, b):
        a = a if isinstance(a, pd.Series) else pd.Series([a] * len(df))
        b = b if isinstance(b, pd.Series) else pd.Series([b] * len(df))
        b = b.replace({0: pd.NA})
        return (a / b).fillna(0.0)

    out = pd.DataFrame()
    out["Start Date"] = start_date
    out["End Date"] = end_date
    out["Country"] = df.get("_country", "")
    out["Profile ID"] = df.get("_profileId", "")
    out["Portfolio ID"] = df.get("portfolioId", "")
    out["Currency"] = df.get("campaignBudgetCurrencyCode", "")
    out["Campaign ID"] = df.get("campaignId", "")
    out["Campaign Name"] = df.get("campaignName", "")
    out["Ad Group ID"] = df.get("adGroupId", "")
    out["Ad Group Name"] = df.get("adGroupName", "")
    out["Advertised SKU"] = df.get("advertisedSku", "")
    out["Advertised ASIN"] = df.get("advertisedAsin", "")
    out["Impressions"] = df.get("impressions", 0)
    out["Clicks"] = df.get("clicks", 0)
    out["Click-Thru Rate (CTR)"] = df.get("clickThroughRate", sdiv(df.get("clicks", 0.0), df.get("impressions", 0.0)))
    out["Cost Per Click (CPC)"] = df.get("costPerClick", sdiv(df.get("cost", 0.0), df.get("clicks", 0.0)))
    out["Spend"] = df.get("cost", 0.0)
    out["7 Day Total Sales"] = df.get("sales7d", 0.0)
    out["7 Day Total Orders (#)"] = df.get("purchases7d", 0.0)
    out["7 Day Total Units (#)"] = df.get("unitsSoldClicks7d", 0.0)
    out["Total Advertising Cost of Sales (ACOS)"] = df.get("acosClicks7d", 0.0)
    out["Total Return on Advertising Spend (ROAS)"] = df.get("roasClicks7d", 0.0)
    out["7 Day Conversion Rate"] = sdiv(df.get("purchases7d", 0.0), df.get("clicks", 0.0))
    out["7 Day Advertised SKU Sales"] = df.get("attributedSalesSameSku7d", 0.0)
    out["7 Day Other SKU Sales"] = df.get("salesOtherSku7d", 0.0)
    out["7 Day Advertised SKU Orders (#)"] = df.get("purchasesSameSku7d", 0.0)
    out["7 Day Advertised SKU Units (#)"] = df.get("unitsSoldSameSku7d", 0.0)
    out["7 Day Other SKU Units (#)"] = df.get("unitsSoldOtherSku7d", 0.0)

    key_cols = [
        "Start Date", "End Date", "Country", "Profile ID",
        "Campaign ID", "Ad Group ID", "Advertised SKU", "Advertised ASIN"
    ]
    for c in ["Country", "Profile ID", "Campaign ID", "Ad Group ID", "Advertised SKU", "Advertised ASIN"]:
        if c in out.columns:
            out[c] = out[c].fillna("").astype(str)

    out = out.drop_duplicates(subset=key_cols, keep="last").reset_index(drop=True)

    now = datetime.utcnow()
    rows_to_insert = []
    for rec in out.to_dict(orient="records"):
        rows_to_insert.append({
            "user_id": user_id,
            "created_at": now,
            "updated_at": now,
            "start_date": sd,
            "end_date": ed,
            "country": (rec.get("Country") or None),
            "profile_id": str(rec.get("Profile ID") or "") or None,
            "portfolio_id": str(rec.get("Portfolio ID") or "") or None,
            "currency": rec.get("Currency") or None,
            "campaign_id": str(rec.get("Campaign ID") or "") or None,
            "campaign_name": rec.get("Campaign Name") or None,
            "ad_group_id": str(rec.get("Ad Group ID") or "") or None,
            "ad_group_name": rec.get("Ad Group Name") or None,
            "advertised_sku": rec.get("Advertised SKU") or None,
            "advertised_asin": rec.get("Advertised ASIN") or None,
            "impressions": _to_int(rec.get("Impressions")),
            "clicks": _to_int(rec.get("Clicks")),
            "ctr": _to_float(rec.get("Click-Thru Rate (CTR)")),
            "cpc": _to_float(rec.get("Cost Per Click (CPC)")),
            "spend": _to_float(rec.get("Spend")),
            "sales_7d": _to_float(rec.get("7 Day Total Sales")),
            "orders_7d": _to_float(rec.get("7 Day Total Orders (#)")),
            "units_7d": _to_float(rec.get("7 Day Total Units (#)")),
            "acos": _to_float(rec.get("Total Advertising Cost of Sales (ACOS)")),
            "roas": _to_float(rec.get("Total Return on Advertising Spend (ROAS)")),
            "conv_rate_7d": _to_float(rec.get("7 Day Conversion Rate")),
            "adv_sku_sales_7d": _to_float(rec.get("7 Day Advertised SKU Sales")),
            "other_sku_sales_7d": _to_float(rec.get("7 Day Other SKU Sales")),
            "adv_sku_orders_7d": _to_float(rec.get("7 Day Advertised SKU Orders (#)")),
            "adv_sku_units_7d": _to_float(rec.get("7 Day Advertised SKU Units (#)")),
            "other_sku_units_7d": _to_float(rec.get("7 Day Other SKU Units (#)")),
        })

    if rows_to_insert:
        table = amazon_sponsored_products.__table__
        stmt = insert(table).values(rows_to_insert)

        conflict_cols = [
            "user_id", "start_date", "end_date", "country", "profile_id",
            "campaign_id", "ad_group_id", "advertised_sku", "advertised_asin",
        ]

        update_cols = {
            "updated_at": stmt.excluded.updated_at,
            "portfolio_id": stmt.excluded.portfolio_id,
            "currency": stmt.excluded.currency,
            "campaign_name": stmt.excluded.campaign_name,
            "ad_group_name": stmt.excluded.ad_group_name,
            "impressions": stmt.excluded.impressions,
            "clicks": stmt.excluded.clicks,
            "ctr": stmt.excluded.ctr,
            "cpc": stmt.excluded.cpc,
            "spend": stmt.excluded.spend,
            "sales_7d": stmt.excluded.sales_7d,
            "orders_7d": stmt.excluded.orders_7d,
            "units_7d": stmt.excluded.units_7d,
            "acos": stmt.excluded.acos,
            "roas": stmt.excluded.roas,
            "conv_rate_7d": stmt.excluded.conv_rate_7d,
            "adv_sku_sales_7d": stmt.excluded.adv_sku_sales_7d,
            "other_sku_sales_7d": stmt.excluded.other_sku_sales_7d,
            "adv_sku_orders_7d": stmt.excluded.adv_sku_orders_7d,
            "adv_sku_units_7d": stmt.excluded.adv_sku_units_7d,
            "other_sku_units_7d": stmt.excluded.other_sku_units_7d,
        }

        stmt = stmt.on_conflict_do_update(index_elements=conflict_cols, set_=update_cols)
        db.session.execute(stmt)
        db.session.commit()

    return {
        "message": "Saved Sponsored Products report rows (UPSERT)",
        "rows_saved": len(rows_to_insert),
        "start_date": start_date,
        "end_date": end_date,
        "time_unit": time_unit,
        "countries": sorted(list(wanted_countries)) if wanted_countries else None,
    }
