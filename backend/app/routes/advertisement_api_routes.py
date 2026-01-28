import io
from datetime import datetime , date
import calendar
import re
from sqlalchemy import text
import jwt
import pandas as pd
from flask import Blueprint, jsonify, request, send_file
from app import db
from config import Config
from app.models.user_models import amazon_user, amazon_sponsored_products , amazon_sponsored_brands_keywords, amazon_sponsored_display_campaigns , amazon_sponsored_display_advertised_products
from app.utils.amazon_ads_utils_reporting import (
    build_ads_lwa_auth_url,
    exchange_code_for_tokens,
    get_ads_access_token_from_refresh,
    list_top_level_profiles_all_regions,
    find_manager_profile_id,
    list_child_profiles_all_regions,
    pick_profile_id,
    ADS_ENDPOINTS,
    tokeninfo,
    AmazonAdsAuthContext,
    AmazonAdsReportingClient,
)

SECRET_KEY = Config.SECRET_KEY
advertisement_api_routes_bp = Blueprint("advertisement_api_routes", __name__)


def _require_jwt_user_id() -> int:
    auth_header = request.headers.get("Authorization")
    if not auth_header or not auth_header.startswith("Bearer "):
        raise PermissionError("Authorization token is missing or invalid")

    token = auth_header.split(" ")[1]
    payload = jwt.decode(token, SECRET_KEY, algorithms=["HS256"])
    return int(payload["user_id"])


def _get_user_row(user_id: int) -> amazon_user:
    u = amazon_user.query.filter_by(user_id=user_id).first()
    if not u:
        raise RuntimeError("User not found")
    return u


@advertisement_api_routes_bp.route("/api/ads/connect_url", methods=["GET"])
def ads_connect_url():
    """
    Returns URL to start Ads LWA OAuth.
    """
    try:
        user_id = _require_jwt_user_id()
        state = f"user:{user_id}"
        url = build_ads_lwa_auth_url(state=state)
        return jsonify({"url": url})
    except Exception as e:
        return jsonify({"error": str(e)}), 400


@advertisement_api_routes_bp.route("/api/ads/callback", methods=["GET"])
def ads_callback():
    """
    - exchange code -> refresh_token
    - store refresh token
    - refresh -> access_token
    - list top profiles (no scope) -> find manager profile id (if any)
    - if manager exists: list child advertiser profiles via scope
      else: child profiles == top profiles
    - save best-effort UK/US/CA advertiser profile IDs
    """
    try:
        code = request.args.get("code")
        state = request.args.get("state") or ""

        if not code:
            return jsonify({"error": "Missing code"}), 400
        if not state.startswith("user:"):
            return jsonify({"error": "Invalid state"}), 400

        user_id = int(state.split("user:")[1])

        tokens = exchange_code_for_tokens(code)
        refresh_token = tokens.get("refresh_token")

        if not refresh_token:
            return jsonify({
                "error": "Missing refresh_token from Amazon. Reconnect with prompt=consent.",
                "token_response": tokens
            }), 400

        u = _get_user_row(user_id)
        u.amazon_ads_refresh_token = refresh_token
        u.amazon_ads_refresh_token_updated_at = datetime.utcnow()

        access_token = get_ads_access_token_from_refresh(refresh_token)

        # 1) Top-level profiles across regions (no scope)
        top_profiles_by_region = list_top_level_profiles_all_regions(access_token)
        manager_profile_id = find_manager_profile_id(top_profiles_by_region)
        u.amazon_ads_manager_profile_id = manager_profile_id

        # 2) Child profiles (advertisers) if manager; otherwise top-level is your advertisers
        if manager_profile_id:
            child_profiles_by_region = list_child_profiles_all_regions(access_token, manager_profile_id)
        else:
            child_profiles_by_region = top_profiles_by_region

        # Save best-effort advertiser profile IDs by country
        eu_child = child_profiles_by_region.get("EU", []) or []
        na_child = child_profiles_by_region.get("NA", []) or []

        # Amazon uses GB for UK
        u.amazon_ads_profile_id_uk = pick_profile_id(eu_child, {"GB", "UK"})
        u.amazon_ads_profile_id_us = pick_profile_id(na_child, {"US"})
        u.amazon_ads_profile_id_ca = pick_profile_id(na_child, {"CA"})

        db.session.commit()

        # helpful counts for debugging
        return jsonify({
            "message": "Amazon Ads connected successfully",
            "saved": {
                "amazon_ads_refresh_token_updated_at": u.amazon_ads_refresh_token_updated_at.isoformat()
                if u.amazon_ads_refresh_token_updated_at else None,
                "amazon_ads_manager_profile_id": u.amazon_ads_manager_profile_id,
                "amazon_ads_profile_id_uk": u.amazon_ads_profile_id_uk,
                "amazon_ads_profile_id_us": u.amazon_ads_profile_id_us,
                "amazon_ads_profile_id_ca": u.amazon_ads_profile_id_ca,
            },
            "counts": {
                "top_level": {k: len(v or []) for k, v in top_profiles_by_region.items()},
                "child": {k: len(v or []) for k, v in child_profiles_by_region.items()},
            },
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 400


@advertisement_api_routes_bp.route("/api/ads/debug_tokeninfo", methods=["GET"])
def debug_tokeninfo():
    """
    Token validation debug that WORKS for Ads tokens.
    (Do NOT use /user/profile with Ads scope.)
    """
    try:
        user_id = _require_jwt_user_id()
        u = _get_user_row(user_id)

        if not u.amazon_ads_refresh_token:
            return jsonify({"error": "No amazon_ads_refresh_token saved"}), 400

        access_token = get_ads_access_token_from_refresh(u.amazon_ads_refresh_token)
        return jsonify(tokeninfo(access_token))
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@advertisement_api_routes_bp.route("/api/ads/debug_profiles_raw", methods=["GET"])
def debug_profiles_raw():
    """
    Raw /v2/profiles response for each region, with request ids and errors if any.
    """
    try:
        user_id = _require_jwt_user_id()
        u = _get_user_row(user_id)

        if not u.amazon_ads_refresh_token:
            return jsonify({"error": "No amazon_ads_refresh_token saved"}), 400

        access_token = get_ads_access_token_from_refresh(u.amazon_ads_refresh_token)

        out = {}
        for region, base_url in ADS_ENDPOINTS.items():
            url = f"{base_url}/v2/profiles"
            r = __import__("requests").get(
                url,
                headers={
                    "Authorization": f"Bearer {access_token}",
                    "Amazon-Advertising-API-ClientId": Config.AMAZON_ADS_CLIENT_ID,
                    "Accept": "application/json",
                },
                timeout=30,
            )
            try:
                body = r.json()
            except Exception:
                body = r.text

            out[region] = {
                "status": r.status_code,
                "request_id": r.headers.get("x-amzn-RequestId") or r.headers.get("Amazon-Advertising-API-RequestId"),
                "content_type": r.headers.get("content-type"),
                "body": body,
            }

        return jsonify(out)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@advertisement_api_routes_bp.route("/api/ads/manager/profiles", methods=["GET"])
def manager_profiles():
    """
    Returns:
    - manager_profile_id (if any)
    - child advertiser profiles grouped by region
    """
    try:
        user_id = _require_jwt_user_id()
        u = _get_user_row(user_id)

        if not u.amazon_ads_refresh_token:
            return jsonify({"error": "No amazon_ads_refresh_token saved"}), 400

        access_token = get_ads_access_token_from_refresh(u.amazon_ads_refresh_token)

        top_profiles = list_top_level_profiles_all_regions(access_token)
        manager_profile_id = find_manager_profile_id(top_profiles)

        if manager_profile_id:
            child_profiles = list_child_profiles_all_regions(access_token, manager_profile_id)
        else:
            child_profiles = top_profiles

        return jsonify({
            "manager_profile_id": manager_profile_id,
            "counts": {k: len(v or []) for k, v in child_profiles.items()},
            "profiles_by_region": child_profiles,
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500




def _norm_key(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(s).lower())

def _pick(df: pd.DataFrame, *candidates, default=None):
    """
    Find a column in df by trying many candidate names,
    matching loosely (case/space/symbol insensitive).
    """
    if df is None or df.empty:
        return default

    norm_map = {_norm_key(c): c for c in df.columns}
    for cand in candidates:
        k = _norm_key(cand)
        if k in norm_map:
            return df[norm_map[k]]
    return default



def _to_date(x):
    if pd.isna(x) or x is None or x == "":
        return None
    return pd.to_datetime(x).date()


def _to_float(x):
    if pd.isna(x) or x is None or x == "":
        return None
    try:
        return float(x)
    except Exception:
        return None


def _to_int(x):
    if pd.isna(x) or x is None or x == "":
        return None
    try:
        return int(float(x))
    except Exception:
        return None


@advertisement_api_routes_bp.route("/api/ads/manager/sp_advertised_product_report", methods=["POST"])
def manager_sp_advertised_product_report():
    """
    Creates SP advertised product report across all accessible advertiser profiles,
    merges into one Excel AND saves rows to DB.

    Body:
      {
        "start_date": "YYYY-MM-DD",
        "end_date": "YYYY-MM-DD",
        "time_unit": "SUMMARY" | "DAILY",
        "countries": ["UK","US"],        # optional
        "return_excel": true            # optional (default true)
      }
    """
    try:
        user_id = _require_jwt_user_id()
        u = _get_user_row(user_id)

        data = request.get_json(force=True) or {}
        start_date = data.get("start_date")
        end_date = data.get("end_date")
        time_unit = (data.get("time_unit") or "SUMMARY").upper()
        return_excel = bool(data.get("return_excel", True))

        wanted_countries = data.get("countries")
        if wanted_countries:
            wanted_countries = {str(x).upper() for x in wanted_countries}
        else:
            wanted_countries = None

        if time_unit not in {"DAILY", "SUMMARY"}:
            return jsonify({"error": "time_unit must be DAILY or SUMMARY"}), 400
        if not start_date or not end_date:
            return jsonify({"error": "start_date and end_date required (YYYY-MM-DD)"}), 400

        if not u.amazon_ads_refresh_token:
            return jsonify({"error": "Amazon Ads not connected for this user."}), 400

        access_token = get_ads_access_token_from_refresh(u.amazon_ads_refresh_token)

        top_profiles = list_top_level_profiles_all_regions(access_token)
        manager_profile_id = find_manager_profile_id(top_profiles)

        if manager_profile_id:
            child_by_region = list_child_profiles_all_regions(access_token, manager_profile_id)
        else:
            child_by_region = top_profiles

        # Flatten profiles with region + normalized country label
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
            return jsonify({
                "error": "No advertiser profiles found (or your country filter removed all). "
                         "This usually means the Amazon login that consented has no API-linked Ads profiles."
            }), 400

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

            if rows and not isinstance(rows[0], dict):
                raise RuntimeError(f"Report returned non-dict rows. first_row_type={type(rows[0])}")

            for r in rows:
                r["_profileId"] = str(profile_id)
                r["_country"] = p["_country_label"]
                merged_rows.append(r)

        if not merged_rows:
            return jsonify({"error": "Reports returned no rows"}), 400

        df = pd.DataFrame(merged_rows)

        # numeric safety (v3 columns)
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
            b = b.replace({0: pd.NA})
            return (a / b).fillna(0.0)

        # Build OUTPUT dataframe (your required columns)
        out = pd.DataFrame()
        out["Start Date"] = df.get("startDate", start_date)
        out["End Date"] = df.get("endDate", end_date)

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

        if "clickThroughRate" in df.columns:
            out["Click-Thru Rate (CTR)"] = df.get("clickThroughRate", 0.0)
        else:
            out["Click-Thru Rate (CTR)"] = sdiv(df.get("clicks", 0.0), df.get("impressions", 0.0))

        if "costPerClick" in df.columns:
            out["Cost Per Click (CPC)"] = df.get("costPerClick", 0.0)
        else:
            out["Cost Per Click (CPC)"] = sdiv(df.get("cost", 0.0), df.get("clicks", 0.0))

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

        # =========================================================
        # ✅ SAVE TO DATABASE (delete + bulk insert)
        # =========================================================
        sd = _to_date(start_date)
        ed = _to_date(end_date)
        now = datetime.utcnow()

        # delete existing for same user/date range (+ optional countries)
        q = amazon_sponsored_products.query.filter(
            amazon_sponsored_products.user_id == user_id,
            amazon_sponsored_products.start_date == sd,
            amazon_sponsored_products.end_date == ed,
        )
        if wanted_countries:
            q = q.filter(amazon_sponsored_products.country.in_(list(wanted_countries)))

        q.delete(synchronize_session=False)
        db.session.commit()

        # prepare rows for bulk insert
        rows_to_insert = []
        for rec in out.to_dict(orient="records"):
            rows_to_insert.append({
                "user_id": user_id,
                "created_at": now,
                "updated_at": now,

                "start_date": _to_date(rec.get("Start Date")),
                "end_date": _to_date(rec.get("End Date")),
                "country": rec.get("Country") or None,
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
            db.session.bulk_insert_mappings(amazon_sponsored_products, rows_to_insert)
            db.session.commit()

        # if you want API-only response without excel
        if not return_excel:
            return jsonify({
                "message": "Saved Sponsored Products report rows",
                "rows_saved": len(rows_to_insert),
                "start_date": start_date,
                "end_date": end_date,
                "time_unit": time_unit,
                "countries": list(wanted_countries) if wanted_countries else None,
            }), 200

        # =========================================================
        # ✅ RETURN EXCEL (same as you already do)
        # =========================================================
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            out.to_excel(writer, index=False, sheet_name="AdvertisedProduct")
        output.seek(0)

        filename = f"SP_Advertised_Product_{start_date}_to_{end_date}.xlsx"
        return send_file(
            output,
            as_attachment=True,
            download_name=filename,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

    except jwt.ExpiredSignatureError:
        return jsonify({"error": "Token expired"}), 401
    except jwt.InvalidTokenError:
        return jsonify({"error": "Invalid token"}), 401
    except PermissionError as e:
        return jsonify({"error": str(e)}), 401
    except Exception as e:
        db.session.rollback()
        return jsonify({"error": str(e)}), 500


# --- helpers (keep once in your file; don't duplicate) ---
def _safe_ident(name: str) -> str:
    """Allow only safe Postgres identifier chars (letters/numbers/_)."""
    name = str(name or "")
    return re.sub(r"[^a-zA-Z0-9_]+", "_", name)

def _safe_div(a, b):
    return 0.0 if not b else (a / b)

# --- ROUTE ---
@advertisement_api_routes_bp.route("/api/ads/monthly_sp_to_db", methods=["POST"])
def monthly_sp_to_db():
    """
    Save monthly aggregated Sponsored Products data into a DB table:
      public.adsmonthly_{user_id}_{country}_{month}_{year}

    Request JSON:
      {
        "month": 12,
        "year": 2025,
        "country": "UK"
      }

    Response:
      {
        "message": "...",
        "table_name": "public.adsmonthly_1_UK_12_2025",
        "country": "UK",
        "month": 12,
        "year": 2025,
        "count": 15,
        "items": [ ... rows incl. Grand Total ... ]
      }
    """
    try:
        user_id = _require_jwt_user_id()
        payload = request.get_json(force=True) or {}

        # ---- inputs ----
        month = int(payload.get("month") or 0)
        year = int(payload.get("year") or 0)
        country = str(payload.get("country") or "").upper().strip()

        if not (1 <= month <= 12):
            return jsonify({"error": "month must be 1..12"}), 400
        if not (2000 <= year <= 2100):
            return jsonify({"error": "year looks invalid"}), 400
        if not country:
            return jsonify({"error": "country is required (e.g. UK/US/CA)"}), 400

        first_day = date(year, month, 1)
        last_day = date(year, month, calendar.monthrange(year, month)[1])

        # ---- fetch monthly rows from amazon_sponsored_products ----
        rows = (
            amazon_sponsored_products.query
            .filter(
                amazon_sponsored_products.user_id == user_id,
                amazon_sponsored_products.country == country,
                amazon_sponsored_products.start_date >= first_day,
                amazon_sponsored_products.start_date <= last_day,
            )
            .all()
        )

        if not rows:
            return jsonify({"error": "No rows found for this user/country/month"}), 404

        # ---- ORM -> DataFrame ----
        df = pd.DataFrame([{
            "advertised_sku": r.advertised_sku or "",
            "advertised_asin": r.advertised_asin or "",

            "impressions": int(r.impressions or 0),
            "clicks": int(r.clicks or 0),
            "spend": float(r.spend or 0.0),

            "sales_7d": float(r.sales_7d or 0.0),
            "orders_7d": float(r.orders_7d or 0.0),
            "units_7d": float(r.units_7d or 0.0),

            "adv_sku_units_7d": float(r.adv_sku_units_7d or 0.0),
            "other_sku_units_7d": float(r.other_sku_units_7d or 0.0),

            "adv_sku_sales_7d": float(r.adv_sku_sales_7d or 0.0),
            "other_sku_sales_7d": float(r.other_sku_sales_7d or 0.0),
        } for r in rows])

        # ---- group by Products(SKU) + ASIN ----
        g = df.groupby(["advertised_sku", "advertised_asin"], as_index=False).agg({
            "impressions": "sum",
            "clicks": "sum",
            "spend": "sum",
            "sales_7d": "sum",
            "orders_7d": "sum",
            "units_7d": "sum",
            "adv_sku_units_7d": "sum",
            "other_sku_units_7d": "sum",
            "adv_sku_sales_7d": "sum",
            "other_sku_sales_7d": "sum",
        })

        # ---- build output rows matching your sheet columns ----
        out = pd.DataFrame()
        out["sno"] = range(1, len(g) + 1)
        out["products"] = g["advertised_sku"]     # product_name
        out["asin"] = g["advertised_asin"]

        # not in your DB currently
        out["ad_type"] = None
        out["match_type"] = None

        out["impressions"] = g["impressions"].astype(int)
        out["clicks"] = g["clicks"].astype(int)

        # percentages (0..100)
        out["ctr"] = [
            _safe_div(c, i) * 100.0
            for c, i in zip(g["clicks"].tolist(), g["impressions"].tolist())
        ]
        out["cpc"] = [
            _safe_div(sp, c)
            for sp, c in zip(g["spend"].tolist(), g["clicks"].tolist())
        ]

        out["spend"] = g["spend"].astype(float)

        # Sale (in Units), Sale (Amount)
        out["sale_units"] = g["units_7d"].astype(float)
        out["sale_amount"] = g["sales_7d"].astype(float)

        # Advertised Unit Sale, Other Unit Sale (units)
        out["advertised_unit_sale"] = g["adv_sku_units_7d"].astype(float)
        out["other_unit_sale"] = g["other_sku_units_7d"].astype(float)

        # Not available in amazon_sponsored_products table
        out["new_to_brand_sales"] = 0.0

        # Conversion rate (0..100)
        out["conversion_rate"] = [
            _safe_div(o, c) * 100.0
            for o, c in zip(g["orders_7d"].tolist(), g["clicks"].tolist())
        ]

        # ROAS & ACOS
        out["roas"] = [
            _safe_div(sa, sp)
            for sa, sp in zip(g["sales_7d"].tolist(), g["spend"].tolist())
        ]
        out["acos"] = [
            _safe_div(sp, sa) * 100.0
            for sp, sa in zip(g["spend"].tolist(), g["sales_7d"].tolist())
        ]

        # ---- Grand Total row ----
        total_impr = int(out["impressions"].sum())
        total_clicks = int(out["clicks"].sum())
        total_spend = float(out["spend"].sum())
        total_sales_amt = float(out["sale_amount"].sum())
        total_orders = float(g["orders_7d"].sum())
        total_units = float(out["sale_units"].sum())

        total_row = {
            "sno": None,
            "products": "Grand Total",
            "asin": None,
            "ad_type": None,
            "match_type": None,
            "impressions": total_impr,
            "clicks": total_clicks,
            "ctr": _safe_div(total_clicks, total_impr) * 100.0,
            "cpc": _safe_div(total_spend, total_clicks),
            "spend": total_spend,
            "sale_units": total_units,
            "sale_amount": total_sales_amt,
            "advertised_unit_sale": float(out["advertised_unit_sale"].sum()),
            "other_unit_sale": float(out["other_unit_sale"].sum()),
            "new_to_brand_sales": float(out["new_to_brand_sales"].sum()),
            "conversion_rate": _safe_div(total_orders, total_clicks) * 100.0,
            "roas": _safe_div(total_sales_amt, total_spend),
            "acos": _safe_div(total_spend, total_sales_amt) * 100.0,
        }

        out = pd.concat([out, pd.DataFrame([total_row])], ignore_index=True)

        # ✅ JSON-safe items (keeps numbers as numbers; nulls as null)
        items = out.where(pd.notnull(out), None).to_dict(orient="records")

        # ---- dynamic table name ----
        table_name = _safe_ident(f"adsmonthly_{user_id}_{country}_{month}_{year}")

        # ---- SQL ----
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS public.{table_name} (
            id BIGSERIAL PRIMARY KEY,
            user_id BIGINT NOT NULL,
            country VARCHAR(10) NOT NULL,
            month INT NOT NULL,
            year INT NOT NULL,

            sno INT,
            products TEXT,
            asin VARCHAR(32),
            ad_type TEXT,
            match_type TEXT,

            impressions BIGINT,
            clicks BIGINT,
            ctr DOUBLE PRECISION,
            cpc DOUBLE PRECISION,
            spend DOUBLE PRECISION,

            sale_units DOUBLE PRECISION,
            sale_amount DOUBLE PRECISION,

            advertised_unit_sale DOUBLE PRECISION,
            other_unit_sale DOUBLE PRECISION,
            new_to_brand_sales DOUBLE PRECISION,

            conversion_rate DOUBLE PRECISION,
            roas DOUBLE PRECISION,
            acos DOUBLE PRECISION,

            created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW()
        );
        """

        delete_sql = f"""
        DELETE FROM public.{table_name}
        WHERE user_id=:user_id AND country=:country AND month=:month AND year=:year;
        """

        insert_sql = f"""
        INSERT INTO public.{table_name} (
            user_id, country, month, year,
            sno, products, asin, ad_type, match_type,
            impressions, clicks, ctr, cpc, spend,
            sale_units, sale_amount,
            advertised_unit_sale, other_unit_sale, new_to_brand_sales,
            conversion_rate, roas, acos
        ) VALUES (
            :user_id, :country, :month, :year,
            :sno, :products, :asin, :ad_type, :match_type,
            :impressions, :clicks, :ctr, :cpc, :spend,
            :sale_units, :sale_amount,
            :advertised_unit_sale, :other_unit_sale, :new_to_brand_sales,
            :conversion_rate, :roas, :acos
        );
        """

        # ---- execute (NO session.begin to avoid "transaction already begun") ----
        try:
            db.session.execute(text(create_sql))
            db.session.execute(text(delete_sql), {
                "user_id": user_id,
                "country": country,
                "month": month,
                "year": year,
            })

            for r in items:
                db.session.execute(text(insert_sql), {
                    "user_id": user_id,
                    "country": country,
                    "month": month,
                    "year": year,

                    "sno": r.get("sno"),
                    "products": r.get("products"),
                    "asin": r.get("asin"),
                    "ad_type": r.get("ad_type"),
                    "match_type": r.get("match_type"),

                    "impressions": int(r.get("impressions") or 0),
                    "clicks": int(r.get("clicks") or 0),
                    "ctr": float(r.get("ctr") or 0.0),
                    "cpc": float(r.get("cpc") or 0.0),
                    "spend": float(r.get("spend") or 0.0),

                    "sale_units": float(r.get("sale_units") or 0.0),
                    "sale_amount": float(r.get("sale_amount") or 0.0),

                    "advertised_unit_sale": float(r.get("advertised_unit_sale") or 0.0),
                    "other_unit_sale": float(r.get("other_unit_sale") or 0.0),
                    "new_to_brand_sales": float(r.get("new_to_brand_sales") or 0.0),

                    "conversion_rate": float(r.get("conversion_rate") or 0.0),
                    "roas": float(r.get("roas") or 0.0),
                    "acos": float(r.get("acos") or 0.0),
                })

            db.session.commit()

        except Exception:
            db.session.rollback()
            raise

        # ---- response ----
        return jsonify({
            "message": "Monthly ads table saved to DB successfully",
            "table_name": f"public.{table_name}",
            "country": country,
            "month": month,
            "year": year,
            "count": len(items),
            "items": items
        }), 200

    except jwt.ExpiredSignatureError:
        return jsonify({"error": "Token expired"}), 401
    except jwt.InvalidTokenError:
        return jsonify({"error": "Invalid token"}), 401
    except PermissionError as e:
        return jsonify({"error": str(e)}), 401
    except Exception as e:
        db.session.rollback()
        return jsonify({"error": str(e)}), 500

#------------------------------------------  "monthly" | "quarterly" | "yearly" routes ------------------------------------------#

def resolve_date_ranges(period: str, year: int, month: int | None, quarter: int | None):
    """
    Returns list of (start_date: date, end_date: date) ranges.
    - monthly: one range for given month
    - quarterly: three month ranges for the quarter
    - yearly: 12 month ranges
    """
    period = (period or "").lower().strip()
    ranges: list[tuple[date, date]] = []

    if period == "monthly":
        if month is None:
            raise ValueError("month is required for period=monthly")
        if not (1 <= int(month) <= 12):
            raise ValueError("month must be 1..12")
        m = int(month)
        start = date(year, m, 1)
        end = date(year, m, calendar.monthrange(year, m)[1])
        ranges.append((start, end))

    elif period == "quarterly":
        if quarter is None:
            raise ValueError("quarter is required for period=quarterly")
        q = int(quarter)
        if q not in {1, 2, 3, 4}:
            raise ValueError("quarter must be 1..4")
        start_month = (q - 1) * 3 + 1
        for m in range(start_month, start_month + 3):
            start = date(year, m, 1)
            end = date(year, m, calendar.monthrange(year, m)[1])
            ranges.append((start, end))

    elif period == "yearly":
        for m in range(1, 13):
            start = date(year, m, 1)
            end = date(year, m, calendar.monthrange(year, m)[1])
            ranges.append((start, end))

    else:
        raise ValueError('period must be one of: "monthly" | "quarterly" | "yearly"')

    return ranges


@advertisement_api_routes_bp.route("/api/ads/manager/sp_advertised_product_report_period", methods=["POST"])
def sp_advertised_product_report_period():
    """
    New route that supports period-based fetch:
      period = "monthly" | "quarterly" | "yearly"

    Body examples:

    Monthly:
      {"period":"monthly","year":2025,"month":12,"countries":["UK"],"time_unit":"SUMMARY","return_excel":false}

    Quarterly:
      {"period":"quarterly","year":2025,"quarter":4,"countries":["UK"],"time_unit":"SUMMARY","return_excel":false}

    Yearly:
      {"period":"yearly","year":2025,"countries":["UK"],"time_unit":"SUMMARY","return_excel":false}

    Notes:
    - This route still respects Ads retention. If you request old months Amazon may 400.
    - It merges all months in the period into ONE aggregated dataframe and saves to amazon_sponsored_products.
    """
    try:
        user_id = _require_jwt_user_id()
        u = _get_user_row(user_id)

        if not u.amazon_ads_refresh_token:
            return jsonify({"error": "Amazon Ads not connected for this user."}), 400

        data = request.get_json(force=True) or {}
        period = (data.get("period") or "monthly").lower()
        year = int(data.get("year") or 0)
        month = data.get("month")
        quarter = data.get("quarter")

        time_unit = (data.get("time_unit") or "SUMMARY").upper()
        return_excel = bool(data.get("return_excel", False))  # this route returns JSON by default

        wanted_countries = data.get("countries")
        if wanted_countries:
            wanted_countries = {str(x).upper() for x in wanted_countries}
        else:
            wanted_countries = None

        if year < 2000 or year > 2100:
            return jsonify({"error": "year looks invalid"}), 400
        if time_unit not in {"SUMMARY", "DAILY"}:
            return jsonify({"error": "time_unit must be SUMMARY or DAILY"}), 400

        # ✅ resolve to month ranges
        try:
            date_ranges = resolve_date_ranges(period, year, int(month) if month is not None else None,
                                              int(quarter) if quarter is not None else None)
        except Exception as ve:
            return jsonify({"error": str(ve)}), 400

        access_token = get_ads_access_token_from_refresh(u.amazon_ads_refresh_token)

        top_profiles = list_top_level_profiles_all_regions(access_token)
        manager_profile_id = find_manager_profile_id(top_profiles)

        if manager_profile_id:
            child_by_region = list_child_profiles_all_regions(access_token, manager_profile_id)
        else:
            child_by_region = top_profiles

        # Flatten profiles
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
            return jsonify({"error": "No advertiser profiles found (or country filter removed all)."}), 400

        merged_rows = []

        # ✅ fetch each month-range for each profile
        for p in all_profiles:
            profile_id = p.get("profileId")
            if not profile_id:
                continue

            region = p["_region"]
            base_url = ADS_ENDPOINTS[region]
            auth = AmazonAdsAuthContext(access_token=access_token, profile_id=str(profile_id))
            ads = AmazonAdsReportingClient(base_url=base_url, auth=auth, timeout=60)

            for start_dt, end_dt in date_ranges:
                start_str = start_dt.isoformat()
                end_str = end_dt.isoformat()

                report_id = ads.create_sp_advertised_product_report(start_str, end_str, time_unit=time_unit)
                location = ads.wait_until_ready(report_id, max_wait_seconds=1800, poll_every_seconds=10)
                rows = ads.download_gzip_json(location)

                if not isinstance(rows, list):
                    raise RuntimeError(f"Report returned unexpected type: {type(rows)}")

                for r in rows:
                    r["_profileId"] = str(profile_id)
                    r["_country"] = p["_country_label"]
                    r["_range_start"] = start_str
                    r["_range_end"] = end_str
                    merged_rows.append(r)

        if not merged_rows:
            return jsonify({"error": "Reports returned no rows"}), 400

        df = pd.DataFrame(merged_rows)

        # numeric safety
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

        # ✅ aggregate period into one "out" table (group by country/profile/campaign/adgroup/sku/asin)
        group_cols = [
            "_country", "_profileId",
            "campaignId", "campaignName",
            "adGroupId", "adGroupName",
            "portfolioId",
            "campaignBudgetCurrencyCode",
            "advertisedSku", "advertisedAsin",
        ]
        for c in group_cols:
            if c not in df.columns:
                df[c] = ""

        g = df.groupby(group_cols, as_index=False).agg({
            "impressions": "sum",
            "clicks": "sum",
            "cost": "sum",
            "sales7d": "sum",
            "purchases7d": "sum",
            "unitsSoldClicks7d": "sum",
            "acosClicks7d": "mean",   # optional - you can recompute later
            "roasClicks7d": "mean",   # optional - you can recompute later
            "attributedSalesSameSku7d": "sum",
            "salesOtherSku7d": "sum",
            "purchasesSameSku7d": "sum",
            "unitsSoldSameSku7d": "sum",
            "unitsSoldOtherSku7d": "sum",
        })

        # recompute ctr/cpc/conv/acos/roas safely based on totals
        g["ctr"] = [_safe_div(c, i) for c, i in zip(g["clicks"], g["impressions"])]
        g["cpc"] = [_safe_div(cost, c) for cost, c in zip(g["cost"], g["clicks"])]
        g["conv_rate_7d"] = [_safe_div(o, c) for o, c in zip(g["purchases7d"], g["clicks"])]
        g["roas"] = [_safe_div(sales, cost) for sales, cost in zip(g["sales7d"], g["cost"])]
        g["acos"] = [_safe_div(cost, sales) for cost, sales in zip(g["cost"], g["sales7d"])]

        # ✅ SAVE: delete + bulk insert into amazon_sponsored_products
        # We save period rows using start_date/end_date = min/max of requested ranges
        sd = min([d[0] for d in date_ranges])
        ed = max([d[1] for d in date_ranges])
        now = datetime.utcnow()

        q = amazon_sponsored_products.query.filter(
            amazon_sponsored_products.user_id == user_id,
            amazon_sponsored_products.start_date == sd,
            amazon_sponsored_products.end_date == ed,
        )
        if wanted_countries:
            q = q.filter(amazon_sponsored_products.country.in_(list(wanted_countries)))

        q.delete(synchronize_session=False)
        db.session.commit()

        rows_to_insert = []
        for rec in g.to_dict(orient="records"):
            rows_to_insert.append({
                "user_id": user_id,
                "created_at": now,
                "updated_at": now,

                "start_date": sd,
                "end_date": ed,
                "country": rec.get("_country") or None,
                "profile_id": str(rec.get("_profileId") or "") or None,

                "portfolio_id": str(rec.get("portfolioId") or "") or None,
                "currency": rec.get("campaignBudgetCurrencyCode") or None,

                "campaign_id": str(rec.get("campaignId") or "") or None,
                "campaign_name": rec.get("campaignName") or None,

                "ad_group_id": str(rec.get("adGroupId") or "") or None,
                "ad_group_name": rec.get("adGroupName") or None,

                "advertised_sku": rec.get("advertisedSku") or None,
                "advertised_asin": rec.get("advertisedAsin") or None,

                "impressions": _to_int(rec.get("impressions")),
                "clicks": _to_int(rec.get("clicks")),

                "ctr": _to_float(rec.get("ctr")),
                "cpc": _to_float(rec.get("cpc")),

                "spend": _to_float(rec.get("cost")),

                "sales_7d": _to_float(rec.get("sales7d")),
                "orders_7d": _to_float(rec.get("purchases7d")),
                "units_7d": _to_float(rec.get("unitsSoldClicks7d")),

                "acos": _to_float(rec.get("acos")),
                "roas": _to_float(rec.get("roas")),
                "conv_rate_7d": _to_float(rec.get("conv_rate_7d")),

                "adv_sku_sales_7d": _to_float(rec.get("attributedSalesSameSku7d")),
                "other_sku_sales_7d": _to_float(rec.get("salesOtherSku7d")),

                "adv_sku_orders_7d": _to_float(rec.get("purchasesSameSku7d")),
                "adv_sku_units_7d": _to_float(rec.get("unitsSoldSameSku7d")),
                "other_sku_units_7d": _to_float(rec.get("unitsSoldOtherSku7d")),
            })

        if rows_to_insert:
            db.session.bulk_insert_mappings(amazon_sponsored_products, rows_to_insert)
            db.session.commit()

        # ✅ JSON items
        items = pd.DataFrame(rows_to_insert).where(pd.notnull(pd.DataFrame(rows_to_insert)), None).to_dict(orient="records")

        return jsonify({
            "message": "Saved Sponsored Products period report rows",
            "period": period,
            "requested_year": year,
            "requested_month": int(month) if month is not None else None,
            "requested_quarter": int(quarter) if quarter is not None else None,
            "start_date": sd.isoformat(),
            "end_date": ed.isoformat(),
            "countries": list(wanted_countries) if wanted_countries else None,
            "rows_saved": len(rows_to_insert),
            "count": len(items),
            "items": items,
        }), 200

    except jwt.ExpiredSignatureError:
        return jsonify({"error": "Token expired"}), 401
    except jwt.InvalidTokenError:
        return jsonify({"error": "Invalid token"}), 401
    except PermissionError as e:
        return jsonify({"error": str(e)}), 401
    except Exception as e:
        db.session.rollback()
        return jsonify({"error": str(e)}), 500


############################################################# Sponsored Brands Keyword Report #############################################################


@advertisement_api_routes_bp.route("/api/ads/manager/sb_keyword_report", methods=["POST"])
def manager_sb_keyword_report():
    """
    Body:
    {
      "start_date": "YYYY-MM-DD",
      "end_date": "YYYY-MM-DD",
      "time_unit": "SUMMARY" | "DAILY",
      "countries": ["UK","US"],      # optional filter on profiles
      "return_excel": true
    }
    """
    try:
        user_id = _require_jwt_user_id()
        u = _get_user_row(user_id)

        data = request.get_json(force=True) or {}
        start_date = data.get("start_date")
        end_date = data.get("end_date")
        time_unit = (data.get("time_unit") or "SUMMARY").upper()
        return_excel = bool(data.get("return_excel", True))

        wanted_countries = data.get("countries")
        wanted_countries = {str(x).upper() for x in wanted_countries} if wanted_countries else None

        if time_unit not in {"DAILY", "SUMMARY"}:
            return jsonify({"error": "time_unit must be DAILY or SUMMARY"}), 400
        if not start_date or not end_date:
            return jsonify({"error": "start_date and end_date required"}), 400
        if not u.amazon_ads_refresh_token:
            return jsonify({"error": "Amazon Ads not connected"}), 400

        access_token = get_ads_access_token_from_refresh(u.amazon_ads_refresh_token)

        top_profiles = list_top_level_profiles_all_regions(access_token)
        manager_profile_id = find_manager_profile_id(top_profiles)
        child_by_region = list_child_profiles_all_regions(access_token, manager_profile_id) if manager_profile_id else top_profiles

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
            return jsonify({"error": "No advertiser profiles found for your filter"}), 400

        merged_rows = []

        for p in all_profiles:
            profile_id = p.get("profileId")
            if not profile_id:
                continue

            region = p["_region"]
            base_url = ADS_ENDPOINTS[region]

            auth = AmazonAdsAuthContext(access_token=access_token, profile_id=str(profile_id))
            ads = AmazonAdsReportingClient(base_url=base_url, auth=auth, timeout=60)

            report_id = ads.create_sb_keyword_report(start_date, end_date, time_unit=time_unit)
            location = ads.wait_until_ready(report_id, max_wait_seconds=1800, poll_every_seconds=10)
            rows = ads.download_gzip_json(location)

            if not isinstance(rows, list):
                raise RuntimeError(f"SB report returned unexpected type: {type(rows)}")

            for r in rows:
                if isinstance(r, dict):
                    r["_profileId"] = str(profile_id)
                    r["_country"] = p["_country_label"]
                    merged_rows.append(r)

        if not merged_rows:
            return jsonify({"error": "SB report returned no rows"}), 400

        df = pd.DataFrame(merged_rows)

        # Build output columns EXACTLY like your Excel
        out = pd.DataFrame()
        out["Start Date"] = _pick(df, "startDate", "Start Date", default=start_date)
        out["End Date"] = _pick(df, "endDate", "End Date", default=end_date)
        out["Portfolio name"] = _pick(df, "portfolioName", "Portfolio name", default="")
        out["Currency"] = _pick(df, "currency", "Currency", "campaignBudgetCurrencyCode", default="")
        out["Campaign Name"] = _pick(df, "campaignName", "Campaign Name", default="")
        out["Ad Group Name"] = _pick(df, "adGroupName", "Ad Group Name", default="")
        out["Targeting"] = _pick(df, "targeting", "Targeting", default="")
        out["Match Type"] = _pick(df, "matchType", "Match Type", default="")
        out["Cost Type"] = _pick(df, "costType", "Cost Type", default="")

        out["Impressions"] = _pick(df, "impressions", "Impressions", default=0)
        out["Top-of-search impression share"] = _pick(df, "topOfSearchImpressionShare", "Top-of-search impression share", default=0.0)
        out["Viewable impressions"] = _pick(df, "viewableImpressions", "Viewable impressions", default=0)

        out["Clicks"] = _pick(df, "clicks", "Clicks", default=0)
        out["Click-Thru Rate (CTR)"] = _pick(df, "clickThroughRate", "Click-Thru Rate (CTR)", default=0.0)

        out["Spend"] = _pick(df, "cost", "Spend", default=0.0)
        out["Cost Per Click (CPC)"] = _pick(df, "costPerClick", "Cost Per Click (CPC)", default=0.0)
        out["Cost per 1,000 viewable impressions (VCPM)"] = _pick(df, "vCPM", "VCPM", "Cost per 1,000 viewable impressions (VCPM)", default=0.0)

        out["Total Advertising Cost of Sales (ACOS) "] = _pick(df, "acos", "Total Advertising Cost of Sales (ACOS) ", default=0.0)
        out["Total Return on Advertising Spend (ROAS)"] = _pick(df, "roas", "Total Return on Advertising Spend (ROAS)", default=0.0)

        out["14 Day Total Sales"] = _pick(df, "sales14d", "14 Day Total Sales", default=0.0)
        out["14 Day Total Orders (#)"] = _pick(df, "purchases14d", "14 Day Total Orders (#)", default=0)
        out["14 Day Total Units (#)"] = _pick(df, "unitsSold14d", "14 Day Total Units (#)", default=0)
        out["14 Day Conversion Rate"] = _pick(df, "conversionRate14d", "14 Day Conversion Rate", default=0.0)

        out["View-through rate (VTR)"] = _pick(df, "viewThroughRate", "View-through rate (VTR)", default=0.0)
        out["Click-through rate for views (vCTR)"] = _pick(df, "vctr", "Click-through rate for views (vCTR)", default=0.0)

        out["Video first quartile views"] = _pick(df, "videoFirstQuartileViews", "Video first quartile views", default=0)
        out["Video midpoint views"] = _pick(df, "videoMidpointViews", "Video midpoint views", default=0)
        out["Video third quartile views"] = _pick(df, "videoThirdQuartileViews", "Video third quartile views", default=0)
        out["Video complete views"] = _pick(df, "videoCompleteViews", "Video complete views", default=0)
        out["Video unmutes"] = _pick(df, "videoUnmutes", "Video unmutes", default=0)

        out["5-second views"] = _pick(df, "views5s", "5-second views", default=0)
        out["5 Second View Rate"] = _pick(df, "viewRate5s", "5 Second View Rate", default=0.0)

        out["14-Day Branded Searches"] = _pick(df, "brandedSearches14d", "14-Day Branded Searches", default=0)
        out["14-day Detail Page Views (DPV)"] = _pick(df, "detailPageViews14d", "14-day Detail Page Views (DPV)", default=0)

        out["14 Day New-to-brand Orders (#)"] = _pick(df, "newToBrandPurchases14d", "14 Day New-to-brand Orders (#)", default=0)
        out["14 Day % of Orders New-to-brand"] = _pick(df, "newToBrandPurchasesPercentage14d", "14 Day % of Orders New-to-brand", default=0.0)

        out["14 Day New-to-brand Sales"] = _pick(df, "newToBrandSales14d", "14 Day New-to-brand Sales", default=0.0)
        out["14 Day % of Sales New-to-brand"] = _pick(df, "newToBrandSalesPercentage14d", "14 Day % of Sales New-to-brand", default=0.0)

        out["14 Day New-to-brand Units (#)"] = _pick(df, "newToBrandUnitsSold14d", "14 Day New-to-brand Units (#)", default=0)
        out["14 Day % of Units New-to-brand"] = _pick(df, "newToBrandUnitsSoldPercentage14d", "14 Day % of Units New-to-brand", default=0.0)

        out["14 Day New-to-brand Order Rate"] = _pick(df, "newToBrandOrderRate14d", "14 Day New-to-brand Order Rate", default=0.0)

        out["Total Advertising Cost of Sales (ACOS) – (Click)"] = _pick(df, "acosClicks14d", "Total Advertising Cost of Sales (ACOS) – (Click)", default=0.0)
        out["Total Return on Advertising Spend (ROAS) – (Click)"] = _pick(df, "roasClicks14d", "Total Return on Advertising Spend (ROAS) – (Click)", default=0.0)
        out["14-Day Total Sales – (Click)"] = _pick(df, "salesClicks14d", "14-Day Total Sales – (Click)", default=0.0)
        out["14-Day Total Orders (#) – (Click)"] = _pick(df, "purchasesClicks14d", "14-Day Total Orders (#) – (Click)", default=0)
        out["14-Day Total Units (#) – (Click)"] = _pick(df, "unitsSoldClicks14d", "14-Day Total Units (#) – (Click)", default=0)
        out["14-day brand total detail page views (#) – (click)"] = _pick(df, "brandTotalDetailPageViewsClicks14d", "14-day brand total detail page views (#) – (click)", default=0)

        # ============ DB SAVE ============
        sd = _to_date(start_date)
        ed = _to_date(end_date)
        now = datetime.utcnow()

        # delete old rows for this report window
        q = amazon_sponsored_brands_keywords.query.filter(
            amazon_sponsored_brands_keywords.user_id == user_id,
            amazon_sponsored_brands_keywords.start_date == sd,
            amazon_sponsored_brands_keywords.end_date == ed,
        )
        q.delete(synchronize_session=False)
        db.session.commit()

        rows_to_insert = []
        for rec in out.to_dict(orient="records"):
            rows_to_insert.append({
                "user_id": user_id,
                "created_at": now,
                "updated_at": now,
                "start_date": _to_date(rec.get("Start Date")),
                "end_date": _to_date(rec.get("End Date")),
                "country": None,  # SB excel doesn't include country, keep optional
                "profile_id": None,  # optional
                "portfolio_name": rec.get("Portfolio name"),
                "currency": rec.get("Currency"),
                "campaign_name": rec.get("Campaign Name"),
                "ad_group_name": rec.get("Ad Group Name"),
                "targeting": rec.get("Targeting"),
                "match_type": rec.get("Match Type"),
                "cost_type": rec.get("Cost Type"),

                "impressions": _to_int(rec.get("Impressions")),
                "top_of_search_impression_share": _to_float(rec.get("Top-of-search impression share")),
                "viewable_impressions": _to_int(rec.get("Viewable impressions")),
                "clicks": _to_int(rec.get("Clicks")),
                "ctr": _to_float(rec.get("Click-Thru Rate (CTR)")),

                "spend": _to_float(rec.get("Spend")),
                "cpc": _to_float(rec.get("Cost Per Click (CPC)")),
                "vcpm": _to_float(rec.get("Cost per 1,000 viewable impressions (VCPM)")),

                "acos": _to_float(rec.get("Total Advertising Cost of Sales (ACOS) ")),
                "roas": _to_float(rec.get("Total Return on Advertising Spend (ROAS)")),

                "total_sales_14d": _to_float(rec.get("14 Day Total Sales")),
                "total_orders_14d": _to_int(rec.get("14 Day Total Orders (#)")),
                "total_units_14d": _to_int(rec.get("14 Day Total Units (#)")),
                "conversion_rate_14d": _to_float(rec.get("14 Day Conversion Rate")),

                "vtr": _to_float(rec.get("View-through rate (VTR)")),
                "vctr": _to_float(rec.get("Click-through rate for views (vCTR)")),

                "video_first_quartile_views": _to_int(rec.get("Video first quartile views")),
                "video_midpoint_views": _to_int(rec.get("Video midpoint views")),
                "video_third_quartile_views": _to_int(rec.get("Video third quartile views")),
                "video_complete_views": _to_int(rec.get("Video complete views")),
                "video_unmutes": _to_int(rec.get("Video unmutes")),

                "views_5s": _to_int(rec.get("5-second views")),
                "view_rate_5s": _to_float(rec.get("5 Second View Rate")),

                "branded_searches_14d": _to_int(rec.get("14-Day Branded Searches")),
                "detail_page_views_14d": _to_int(rec.get("14-day Detail Page Views (DPV)")),

                "ntb_orders_14d": _to_int(rec.get("14 Day New-to-brand Orders (#)")),
                "ntb_orders_pct_14d": _to_float(rec.get("14 Day % of Orders New-to-brand")),
                "ntb_sales_14d": _to_float(rec.get("14 Day New-to-brand Sales")),
                "ntb_sales_pct_14d": _to_float(rec.get("14 Day % of Sales New-to-brand")),
                "ntb_units_14d": _to_int(rec.get("14 Day New-to-brand Units (#)")),
                "ntb_units_pct_14d": _to_float(rec.get("14 Day % of Units New-to-brand")),
                "ntb_order_rate_14d": _to_float(rec.get("14 Day New-to-brand Order Rate")),

                "acos_click": _to_float(rec.get("Total Advertising Cost of Sales (ACOS) – (Click)")),
                "roas_click": _to_float(rec.get("Total Return on Advertising Spend (ROAS) – (Click)")),
                "sales_14d_click": _to_float(rec.get("14-Day Total Sales – (Click)")),
                "orders_14d_click": _to_int(rec.get("14-Day Total Orders (#) – (Click)")),
                "units_14d_click": _to_int(rec.get("14-Day Total Units (#) – (Click)")),
                "brand_total_dpv_click": _to_int(rec.get("14-day brand total detail page views (#) – (click)")),
            })

        if rows_to_insert:
            db.session.bulk_insert_mappings(amazon_sponsored_brands_keywords, rows_to_insert)
            db.session.commit()

        if not return_excel:
            return jsonify({"message": "Saved SB keyword rows", "rows_saved": len(rows_to_insert)}), 200

        output = io.BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            out.to_excel(writer, index=False, sheet_name="SB_Keywords")
        output.seek(0)

        filename = f"SB_Keyword_{start_date}_to_{end_date}.xlsx"
        return send_file(
            output,
            as_attachment=True,
            download_name=filename,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

    except Exception as e:
        db.session.rollback()
        return jsonify({"error": str(e)}), 500


############################################################# Sponsored Display Campaign Report #############################################################

@advertisement_api_routes_bp.route("/api/ads/manager/sd_campaign_report", methods=["POST"])
def manager_sd_campaign_report():
    """
    Body:
    {
      "start_date": "YYYY-MM-DD",
      "end_date": "YYYY-MM-DD",
      "time_unit": "SUMMARY" | "DAILY",
      "countries": ["UK","US"],      # optional filter on profiles
      "return_excel": true
    }
    """
    try:
        import numpy as np

        user_id = _require_jwt_user_id()
        u = _get_user_row(user_id)

        data = request.get_json(force=True) or {}
        start_date = data.get("start_date")
        end_date = data.get("end_date")
        time_unit = (data.get("time_unit") or "SUMMARY").upper()
        return_excel = bool(data.get("return_excel", True))

        wanted_countries = data.get("countries") or []
        wanted_countries = {c.upper() for c in wanted_countries}

        regions_to_use = set()

        # UK/GB -> EU
        if "UK" in wanted_countries or "GB" in wanted_countries:
            regions_to_use.add("EU")

        # US/CA -> NA
        if "US" in wanted_countries or "CA" in wanted_countries:
            regions_to_use.add("NA")

        # If nothing specified, fallback to EU+NA
        if not regions_to_use:
            regions_to_use = {"EU", "NA"}


        if time_unit not in {"DAILY", "SUMMARY"}:
            return jsonify({"error": "time_unit must be DAILY or SUMMARY"}), 400
        if not start_date or not end_date:
            return jsonify({"error": "start_date and end_date required"}), 400
        if not u.amazon_ads_refresh_token:
            return jsonify({"error": "Amazon Ads not connected"}), 400

        access_token = get_ads_access_token_from_refresh(u.amazon_ads_refresh_token)

        top_profiles = list_top_level_profiles_all_regions(access_token)
        manager_profile_id = find_manager_profile_id(top_profiles)
        child_by_region = (
            list_child_profiles_all_regions(access_token, manager_profile_id)
            if manager_profile_id else top_profiles
        )

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
            return jsonify({"error": "No advertiser profiles found for your filter"}), 400

        merged_rows = []

        # Enrichment maps (keyed by profileId)
        campaigns_by_profile = {}   # {profileId: {campaignId: campaign_obj}}
        portfolios_by_profile = {}  # {profileId: {portfolioId: portfolioName}}

        def _safe_div(a, b):
            a = pd.to_numeric(a, errors="coerce").fillna(0.0)
            b = pd.to_numeric(b, errors="coerce").replace({0: np.nan})
            return (a / b).fillna(0.0)

        for p in all_profiles:
            profile_id = p.get("profileId")
            if not profile_id:
                continue

            region = p["_region"]
            base_url = ADS_ENDPOINTS[region]

            auth = AmazonAdsAuthContext(access_token=access_token, profile_id=str(profile_id))
            ads = AmazonAdsReportingClient(base_url=base_url, auth=auth, timeout=60)

            # ---- (A) pull campaign metadata for Status/Budget/PortfolioId/CostType ----
            # If your client doesn't have these methods yet, add them (see below)
            try:
                sd_campaigns = ads.list_sd_campaigns()
                campaigns_by_profile[str(profile_id)] = {
                    str(c.get("campaignId")): c for c in (sd_campaigns or []) if c.get("campaignId") is not None
                }
            except Exception:
                campaigns_by_profile[str(profile_id)] = {}

            try:
                portfolios = ads.list_portfolios()
                portfolios_by_profile[str(profile_id)] = {
                    str(x.get("portfolioId")): x.get("name") for x in (portfolios or []) if x.get("portfolioId") is not None
                }
            except Exception:
                portfolios_by_profile[str(profile_id)] = {}

            # ---- (B) create + download SD report rows ----
            report_id = ads.create_sd_campaign_report(start_date, end_date, time_unit=time_unit)
            location = ads.wait_until_ready(report_id, max_wait_seconds=1800, poll_every_seconds=10)
            rows = ads.download_gzip_json(location)

            if not isinstance(rows, list):
                raise RuntimeError(f"SD report returned unexpected type: {type(rows)}")

            for r in rows:
                if isinstance(r, dict):
                    r["_profileId"] = str(profile_id)
                    r["_country"] = p["_country_label"]
                    merged_rows.append(r)

        if not merged_rows:
            return jsonify({"error": "SD report returned no rows"}), 400

        df = pd.DataFrame(merged_rows)

        # Ensure numeric columns exist + numeric
        numeric_cols = [
            "impressions", "clicks", "cost",
            "detailPageViews", "detailPageViewsClicks",
            "purchases", "purchasesClicks",
            "unitsSold", "unitsSoldClicks",
            "sales", "salesClicks",
            "newToBrandPurchases", "newToBrandPurchasesClicks",
            "newToBrandUnitsSold", "newToBrandUnitsSoldClicks",
            "newToBrandSalesClicks",
            "addToCart", "addToCartClicks", "addToCartViews", "addToCartRate", "eCPAddToCart",
            "brandedSearches", "brandedSearchesClicks", "brandedSearchesViews",
            "brandedSearchRate", "eCPBrandSearch",
            "longTermSales", "longTermROAS",
            "viewabilityRate",
        ]
        for c in numeric_cols:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)

        # Derived fields
        # viewable impressions not provided => impressions * viewabilityRate (rate is 0..1)
        if "viewabilityRate" in df.columns and "impressions" in df.columns:
            df["viewableImpressionsDerived"] = (df["impressions"] * df["viewabilityRate"]).round(0)
        else:
            df["viewableImpressionsDerived"] = 0

        df["ctrDerived"] = _safe_div(df.get("clicks", 0.0), df.get("impressions", 0.0))
        df["cpcDerived"] = _safe_div(df.get("cost", 0.0), df.get("clicks", 0.0))
        df["vcpmDerived"] = _safe_div(df.get("cost", 0.0) * 1000.0, df.get("viewableImpressionsDerived", 0.0))

        df["acosDerived"] = _safe_div(df.get("cost", 0.0), df.get("sales", 0.0))
        df["roasDerived"] = _safe_div(df.get("sales", 0.0), df.get("cost", 0.0))

        df["acosClickDerived"] = _safe_div(df.get("cost", 0.0), df.get("salesClicks", 0.0))
        df["roasClickDerived"] = _safe_div(df.get("salesClicks", 0.0), df.get("cost", 0.0))

        # Enrich per row using campaign meta
        def _enrich(row):
            pid = str(row.get("_profileId") or "")
            cid = str(row.get("campaignId") or "")
            cmeta = (campaigns_by_profile.get(pid, {}) or {}).get(cid, {}) or {}

            # Amazon SD campaigns usually return: state, budget, portfolioId, costType
            status = cmeta.get("state") or cmeta.get("status") or cmeta.get("campaignStatus")
            budget = cmeta.get("budget")
            cost_type = cmeta.get("costType")
            portfolio_id = cmeta.get("portfolioId")

            portfolio_name = None
            if portfolio_id is not None:
                portfolio_name = (portfolios_by_profile.get(pid, {}) or {}).get(str(portfolio_id))

            return pd.Series({
                "statusMeta": status or "",
                "budgetMeta": budget if budget is not None else 0.0,
                "costTypeMeta": cost_type or "",
                "portfolioNameMeta": portfolio_name or "",
            })

        meta_df = df.apply(_enrich, axis=1)
        df = pd.concat([df, meta_df], axis=1)

        # =========================
        # OUTPUT (correct mapping)
        # =========================
        out = pd.DataFrame()
        out["Start Date"] = _pick(df, "startDate", default=start_date)
        out["End Date"] = _pick(df, "endDate", default=end_date)

        out["Country"] = _pick(df, "_country", default="")
        out["Profile ID"] = _pick(df, "_profileId", default="")

        out["Status"] = _pick(df, "statusMeta", default="")
        out["Currency"] = _pick(df, "campaignBudgetCurrencyCode", default="")
        out["Budget"] = _pick(df, "budgetMeta", default=0.0)

        out["Campaign Name"] = _pick(df, "campaignName", default="")
        out["Portfolio name"] = _pick(df, "portfolioNameMeta", default="")
        out["Cost Type"] = _pick(df, "costTypeMeta", default="")

        out["Impressions"] = _pick(df, "impressions", default=0)
        out["Viewable impressions"] = df.get("viewableImpressionsDerived", 0).fillna(0).astype("int64")
        out["Clicks"] = _pick(df, "clicks", default=0)
        out["Click-Thru Rate (CTR)"] = df.get("ctrDerived", 0.0)

        # DPV: SD uses detailPageViews (not detailPageViews14d)
        out["14-day Detail Page Views (DPV)"] = _pick(df, "detailPageViews", default=0)

        out["Spend"] = _pick(df, "cost", default=0.0)
        out["Cost Per Click (CPC)"] = df.get("cpcDerived", 0.0)
        out["Cost per 1,000 viewable impressions (VCPM)"] = df.get("vcpmDerived", 0.0)

        out["Total Advertising Cost of Sales (ACOS) "] = df.get("acosDerived", 0.0)
        out["Total Return on Advertising Spend (ROAS)"] = df.get("roasDerived", 0.0)

        # Totals: SD uses purchases/unitsSold/sales (not purchases14d/unitsSold14d/sales14d)
        out["14 Day Total Orders (#)"] = _pick(df, "purchases", default=0)
        out["14 Day Total Units (#)"] = _pick(df, "unitsSold", default=0)
        out["14 Day Total Sales"] = _pick(df, "sales", default=0.0)

        out["14 Day New-to-brand Orders (#)"] = _pick(df, "newToBrandPurchases", default=0)
        # Many tenants don't have newToBrandSales (non-click) for SD campaign report. Keep 0 unless your reportType supports it.
        out["14 Day New-to-brand Sales"] = 0.0
        out["14 Day New-to-brand Units (#)"] = _pick(df, "newToBrandUnitsSold", default=0)

        out["Total Advertising Cost of Sales (ACOS) – (Click)"] = df.get("acosClickDerived", 0.0)
        out["Total Return on Advertising Spend (ROAS) – (Click)"] = df.get("roasClickDerived", 0.0)

        out["14-Day Total Orders (#) – (Click)"] = _pick(df, "purchasesClicks", default=0)
        out["14-Day Total Units (#) – (Click)"] = _pick(df, "unitsSoldClicks", default=0)
        out["14-Day Total Sales – (Click)"] = _pick(df, "salesClicks", default=0.0)

        out["14-Day New-to-brand Orders (#) – (Click)"] = _pick(df, "newToBrandPurchasesClicks", default=0)
        out["14-Day New-to-brand Sales – (Click)"] = _pick(df, "newToBrandSalesClicks", default=0.0)
        out["14-Day New-to-Brand Units (#) – (Click)"] = _pick(df, "newToBrandUnitsSoldClicks", default=0)

        # These fields are NOT in your allowed list; keep 0 unless your reportType supports them.
        out["New-to-brand detail page views"] = 0
        out["New-to-brand detail page view view-through conversions"] = 0
        out["New-to-brand detail page view click-through conversions"] = 0
        out["New-to-brand detail page view rate"] = 0.0
        out["Effective cost per new-to-brand detail page view"] = 0.0

        # ATC / Branded Search from SD report (allowed)
        out["14-day ATC"] = _pick(df, "addToCart", default=0)
        out["14-day ATC views"] = _pick(df, "addToCartViews", default=0)
        out["14-day ATC clicks"] = _pick(df, "addToCartClicks", default=0)
        out["14-day ATCR"] = _pick(df, "addToCartRate", default=0.0)
        out["Effective cost per Add to Basket (eCPATB)"] = _pick(df, "eCPAddToCart", default=0.0)

        out["14-Day Branded Searches"] = _pick(df, "brandedSearches", default=0)
        out["Branded Searches view-through conversions"] = 0
        out["Branded Searches click-through conversions"] = 0
        out["Branded Searches Rate"] = _pick(df, "brandedSearchRate", default=0.0)
        out["Effective cost per Branded Search"] = _pick(df, "eCPBrandSearch", default=0.0)

        out["Long-Term Sales"] = _pick(df, "longTermSales", default=0.0)
        out["Long-Term ROAS"] = _pick(df, "longTermROAS", default=0.0)

        # =========================
        # DB SAVE
        # =========================
        sd = _to_date(start_date)
        ed = _to_date(end_date)
        now = datetime.utcnow()

        q = amazon_sponsored_display_campaigns.query.filter(
            amazon_sponsored_display_campaigns.user_id == user_id,
            amazon_sponsored_display_campaigns.start_date == sd,
            amazon_sponsored_display_campaigns.end_date == ed,
        )
        q.delete(synchronize_session=False)
        db.session.commit()

        rows_to_insert = []
        for rec in out.to_dict(orient="records"):
            rows_to_insert.append({
                "user_id": user_id,
                "created_at": now,
                "updated_at": now,
                "start_date": _to_date(rec.get("Start Date")),
                "end_date": _to_date(rec.get("End Date")),

                "country": rec.get("Country"),
                "status": rec.get("Status"),
                "profile_id": str(rec.get("Profile ID") or "") or None,

                "currency": rec.get("Currency"),
                "budget": _to_float(rec.get("Budget")),

                "campaign_name": rec.get("Campaign Name"),
                "portfolio_name": rec.get("Portfolio name"),
                "cost_type": rec.get("Cost Type"),

                "impressions": _to_int(rec.get("Impressions")),
                "viewable_impressions": _to_int(rec.get("Viewable impressions")),
                "clicks": _to_int(rec.get("Clicks")),
                "ctr": _to_float(rec.get("Click-Thru Rate (CTR)")),

                "detail_page_views_14d": _to_int(rec.get("14-day Detail Page Views (DPV)")),

                "spend": _to_float(rec.get("Spend")),
                "cpc": _to_float(rec.get("Cost Per Click (CPC)")),
                "vcpm": _to_float(rec.get("Cost per 1,000 viewable impressions (VCPM)")),

                "acos": _to_float(rec.get("Total Advertising Cost of Sales (ACOS) ")),
                "roas": _to_float(rec.get("Total Return on Advertising Spend (ROAS)")),

                "orders_14d": _to_int(rec.get("14 Day Total Orders (#)")),
                "units_14d": _to_int(rec.get("14 Day Total Units (#)")),
                "sales_14d": _to_float(rec.get("14 Day Total Sales")),

                "ntb_orders_14d": _to_int(rec.get("14 Day New-to-brand Orders (#)")),
                "ntb_sales_14d": _to_float(rec.get("14 Day New-to-brand Sales")),
                "ntb_units_14d": _to_int(rec.get("14 Day New-to-brand Units (#)")),

                "acos_click": _to_float(rec.get("Total Advertising Cost of Sales (ACOS) – (Click)")),
                "roas_click": _to_float(rec.get("Total Return on Advertising Spend (ROAS) – (Click)")),
                "orders_14d_click": _to_int(rec.get("14-Day Total Orders (#) – (Click)")),
                "units_14d_click": _to_int(rec.get("14-Day Total Units (#) – (Click)")),
                "sales_14d_click": _to_float(rec.get("14-Day Total Sales – (Click)")),

                "ntb_orders_14d_click": _to_int(rec.get("14-Day New-to-brand Orders (#) – (Click)")),
                "ntb_sales_14d_click": _to_float(rec.get("14-Day New-to-brand Sales – (Click)")),
                "ntb_units_14d_click": _to_int(rec.get("14-Day New-to-Brand Units (#) – (Click)")),

                "ntb_dpv": _to_int(rec.get("New-to-brand detail page views")),
                "ntb_dpv_vtc": _to_int(rec.get("New-to-brand detail page view view-through conversions")),
                "ntb_dpv_ctc": _to_int(rec.get("New-to-brand detail page view click-through conversions")),
                "ntb_dpv_rate": _to_float(rec.get("New-to-brand detail page view rate")),
                "ecost_ntb_dpv": _to_float(rec.get("Effective cost per new-to-brand detail page view")),

                "atc_14d": _to_int(rec.get("14-day ATC")),
                "atc_views_14d": _to_int(rec.get("14-day ATC views")),
                "atc_clicks_14d": _to_int(rec.get("14-day ATC clicks")),
                "atcr_14d": _to_float(rec.get("14-day ATCR")),
                "ecp_atb": _to_float(rec.get("Effective cost per Add to Basket (eCPATB)")),

                "branded_searches_14d": _to_int(rec.get("14-Day Branded Searches")),
                "bs_vtc": _to_int(rec.get("Branded Searches view-through conversions")),
                "bs_ctc": _to_int(rec.get("Branded Searches click-through conversions")),
                "bs_rate": _to_float(rec.get("Branded Searches Rate")),
                "ecost_bs": _to_float(rec.get("Effective cost per Branded Search")),

                "long_term_sales": _to_float(rec.get("Long-Term Sales")),
                "long_term_roas": _to_float(rec.get("Long-Term ROAS")),
            })

        if rows_to_insert:
            db.session.bulk_insert_mappings(amazon_sponsored_display_campaigns, rows_to_insert)
            db.session.commit()

        if not return_excel:
            return jsonify({"message": "Saved SD campaign rows", "rows_saved": len(rows_to_insert)}), 200

        output = io.BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            out.to_excel(writer, index=False, sheet_name="SD_Campaigns")
        output.seek(0)

        filename = f"SD_Campaign_{start_date}_to_{end_date}.xlsx"
        return send_file(
            output,
            as_attachment=True,
            download_name=filename,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

    except Exception as e:
        db.session.rollback()
        return jsonify({"error": str(e)}), 500


@advertisement_api_routes_bp.route("/api/ads/debug/report_types", methods=["GET"])
def debug_report_types():
    try:
        user_id = _require_jwt_user_id()
        u = _get_user_row(user_id)

        if not u.amazon_ads_refresh_token:
            return jsonify({"error": "Amazon Ads not connected"}), 400

        region = (request.args.get("region") or "EU").upper()

        # pick profile for that region
        if region == "EU":
            profile_id = u.amazon_ads_profile_id_uk
        elif region == "NA":
            profile_id = u.amazon_ads_profile_id_us
        else:
            profile_id = u.amazon_ads_profile_id_uk or u.amazon_ads_profile_id_us

        if not profile_id:
            return jsonify({"error": f"No profile_id found for region={region}"}), 400

        access_token = get_ads_access_token_from_refresh(u.amazon_ads_refresh_token)
        base_url = ADS_ENDPOINTS[region]

        auth = AmazonAdsAuthContext(access_token=access_token, profile_id=str(profile_id))
        ads = AmazonAdsReportingClient(base_url=base_url, auth=auth, timeout=60)

        types = ads.list_report_types()
        # show only SB + SD types to keep output small
        filtered = []
        for t in types:
            adp = (t.get("adProduct") or "").upper()
            if adp in {"SPONSORED_BRANDS", "SPONSORED_DISPLAY"}:
                filtered.append(t)
        return jsonify(filtered)

    except Exception as e:
        return jsonify({"error": str(e)}), 500

@advertisement_api_routes_bp.route("/api/ads/manager/sd_advertised_product_report", methods=["POST"])
def manager_sd_advertised_product_report():
    try:
        user_id = _require_jwt_user_id()
        u = _get_user_row(user_id)

        data = request.get_json(force=True)
        start_date = data["start_date"]
        end_date = data["end_date"]
        time_unit = (data.get("time_unit") or "SUMMARY").upper()

        access_token = get_ads_access_token_from_refresh(u.amazon_ads_refresh_token)

        top_profiles = list_top_level_profiles_all_regions(access_token)
        manager_profile_id = find_manager_profile_id(top_profiles)
        child_by_region = (
            list_child_profiles_all_regions(access_token, manager_profile_id)
            if manager_profile_id else top_profiles
        )

        rows_all = []

        for region, profiles in child_by_region.items():
            for p in profiles or []:
                profile_id = p["profileId"]
                country = "UK" if p.get("countryCode") == "GB" else p.get("countryCode")

                auth = AmazonAdsAuthContext(access_token, str(profile_id))
                ads = AmazonAdsReportingClient(ADS_ENDPOINTS[region], auth)

                report_id = ads.create_sd_advertised_product_report(
                    start_date, end_date, time_unit
                )
                url = ads.wait_until_ready(report_id)
                rows = ads.download_gzip_json(url)

                for r in rows:
                    r["_profileId"] = profile_id
                    r["_country"] = country
                    rows_all.append(r)

        df = pd.DataFrame(rows_all)

        # Save to DB
        db.session.query(amazon_sponsored_display_advertised_products).filter(
            amazon_sponsored_display_advertised_products.user_id == user_id,
            amazon_sponsored_display_advertised_products.start_date == _to_date(start_date),
            amazon_sponsored_display_advertised_products.end_date == _to_date(end_date),
        ).delete(synchronize_session=False)

        now = datetime.utcnow()
        inserts = []

        def _safe_div(a, b):
            a = float(a or 0.0)
            b = float(b or 0.0)
            return (a / b) if b else 0.0

        for r in df.to_dict("records"):
            cost = _to_float(r.get("cost"))
            clicks = _to_int(r.get("clicks"))
            impressions = _to_int(r.get("impressions"))
            sales = _to_float(r.get("sales"))

            inserts.append({
                "user_id": user_id,
                "created_at": now,
                "updated_at": now,
                "start_date": _to_date(start_date),
                "end_date": _to_date(end_date),
                "country": r["_country"],
                "profile_id": r["_profileId"],

                "campaign_id": r.get("campaignId"),
                "campaign_name": r.get("campaignName"),
                "ad_group_id": r.get("adGroupId"),
                "ad_group_name": r.get("adGroupName"),

                # ✅ use promoted*
                "advertised_sku": r.get("promotedSku"),
                "advertised_asin": r.get("promotedAsin"),

                "currency": r.get("campaignBudgetCurrencyCode"),

                "impressions": impressions,
                "clicks": clicks,
                "spend": cost,

                # ✅ derive CPC/CTR (since costPerClick/clickThroughRate are invalid for this SD report)
                "cpc": _safe_div(cost, clicks),
                "ctr": _safe_div(clicks, impressions),

                # ✅ SD provides sales/purchases/unitsSold (no *14d suffix in this report)
                "sales_14d": sales,
                "orders_14d": _to_int(r.get("purchases")),
                "units_14d": _to_int(r.get("unitsSold")),

                # ✅ derive acos/roas (since acos/roas are invalid columns here)
                "acos": _safe_div(cost, sales),
                "roas": _safe_div(sales, cost),
            })


        db.session.bulk_insert_mappings(
            amazon_sponsored_display_advertised_products, inserts
        )
        db.session.commit()

        return jsonify({"rows_saved": len(inserts)})

    except Exception as e:
        db.session.rollback()
        return jsonify({"error": str(e)}), 500


