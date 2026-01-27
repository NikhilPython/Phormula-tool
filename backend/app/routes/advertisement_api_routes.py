import io
from datetime import datetime
import jwt
import pandas as pd
from flask import Blueprint, jsonify, request, send_file
from app import db
from config import Config
from app.models.user_models import amazon_user, amazon_sponsored_products

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
