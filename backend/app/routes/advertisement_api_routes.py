import io
from datetime import datetime , date
import calendar, json, hashlib
import re
from sqlalchemy import func, text
from sqlalchemy.dialects.postgresql import insert
import jwt, time
import pandas as pd
from flask import Blueprint, jsonify, request, send_file, Response
from app import db
from config import Config
from app.models.user_models import amazon_user, amazon_sponsored_products , amazon_sponsored_display_advertised_products
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
from app.models.user_models import amazon_sponsored_brands_keywords
from openpyxl.utils import get_column_letter

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
            payload = {
                "error": "Missing refresh_token from Amazon. Reconnect with prompt=consent.",
                "token_response": tokens
            }
            if request.args.get("format") == "json":
                return jsonify(payload), 400

            html = f"""
            <!doctype html>
            <html>
              <head><meta charset="utf-8" /></head>
              <body>
                <script>
                  (function () {{
                    try {{
                      if (window.opener && !window.opener.closed) {{
                        window.opener.postMessage(
                          {{ type: "amazon_ads_connected", ok: false, error: {json.dumps(payload["error"])} }},
                          "*"
                        );
                      }}
                    }} catch (e) {{}}
                    try {{ window.close(); }} catch (e) {{}}
                    document.body.innerHTML = "Connection failed. You can close this window.";
                  }})();
                </script>
              </body>
            </html>
            """
            return Response(html, mimetype="text/html"), 400

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

        payload = {
            "message": "Amazon Ads connected successfully",
            "saved": {
                "amazon_ads_refresh_token_updated_at": (
                    u.amazon_ads_refresh_token_updated_at.isoformat()
                    if u.amazon_ads_refresh_token_updated_at else None
                ),
                "amazon_ads_manager_profile_id": u.amazon_ads_manager_profile_id,
                "amazon_ads_profile_id_uk": u.amazon_ads_profile_id_uk,
                "amazon_ads_profile_id_us": u.amazon_ads_profile_id_us,
                "amazon_ads_profile_id_ca": u.amazon_ads_profile_id_ca,
            },
            "counts": {
                "top_level": {k: len(v or []) for k, v in top_profiles_by_region.items()},
                "child": {k: len(v or []) for k, v in child_profiles_by_region.items()},
            },
        }

        # Debug mode if you want JSON:
        # /api/ads/callback?code=...&state=...&format=json
        if request.args.get("format") == "json":
            return jsonify(payload)

        # Normal popup flow: notify opener + close popup
        html = f"""
        <!doctype html>
        <html>
          <head><meta charset="utf-8" /></head>
          <body>
            <script>
              (function () {{
                try {{
                  if (window.opener && !window.opener.closed) {{
                    window.opener.postMessage(
                      {{ type: "amazon_ads_connected", ok: true, payload: {json.dumps(payload)} }},
                      "*"
                    );
                  }}
                }} catch (e) {{}}

                try {{ window.close(); }} catch (e) {{}}

                document.body.innerHTML = "Connected. You can close this window.";
              }})();
            </script>
          </body>
        </html>
        """
        return Response(html, mimetype="text/html")

    except Exception as e:
        err_payload = {"error": str(e)}

        if request.args.get("format") == "json":
            return jsonify(err_payload), 400

        html = f"""
        <!doctype html>
        <html>
          <head><meta charset="utf-8" /></head>
          <body>
            <script>
              (function () {{
                try {{
                  if (window.opener && !window.opener.closed) {{
                    window.opener.postMessage(
                      {{ type: "amazon_ads_connected", ok: false, error: {json.dumps(str(e))} }},
                      "*"
                    );
                  }}
                }} catch (e) {{}}

                try {{ window.close(); }} catch (e) {{}}

                document.body.innerHTML = "Connection failed. You can close this window.";
              }})();
            </script>
          </body>
        </html>
        """
        return Response(html, mimetype="text/html"), 400



@advertisement_api_routes_bp.route("/api/ads/status", methods=["GET"])
def ads_status():
    # -------- auth (same as amazon_status) --------
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

    # -------- load from DB for this user --------
    # If your ads tokens are stored on the main User row, this is correct.
    # If you store ads tokens in a separate model, swap this query accordingly.
    u = _get_user_row(user_id)  # or: User.query.filter_by(id=user_id).first()

    # No row at all (very unlikely if users always exist)
    if not u:
        return jsonify({
            "success": False,
            "status": "no_record",
            "has_refresh_token": False,
        }), 200

    # Refresh token missing => OAuth not completed
    if not getattr(u, "amazon_ads_refresh_token", None):
        return jsonify({
            "success": False,
            "status": "pending",
            "has_refresh_token": False,
            "saved": {
                "amazon_ads_refresh_token_updated_at": None,
                "amazon_ads_manager_profile_id": getattr(u, "amazon_ads_manager_profile_id", None),
                "amazon_ads_profile_id_uk": getattr(u, "amazon_ads_profile_id_uk", None),
                "amazon_ads_profile_id_us": getattr(u, "amazon_ads_profile_id_us", None),
                "amazon_ads_profile_id_ca": getattr(u, "amazon_ads_profile_id_ca", None),
            }
        }), 200

    # We DO have a refresh token in DB -> optionally validate by getting access token + calling profiles endpoint
    refresh_token = u.amazon_ads_refresh_token

    try:
        access_token = get_ads_access_token_from_refresh(refresh_token)

        # light validation call: if this succeeds, you're basically connected
        # (you can also skip this call and just return connected if you only care about token existence)
        profiles_by_region = list_top_level_profiles_all_regions(access_token)

        return jsonify({
            "success": True,
            "status": "connected",
            "has_refresh_token": True,
            "saved": {
                "amazon_ads_refresh_token_updated_at": u.amazon_ads_refresh_token_updated_at.isoformat()
                if getattr(u, "amazon_ads_refresh_token_updated_at", None) else None,
                "amazon_ads_manager_profile_id": getattr(u, "amazon_ads_manager_profile_id", None),
                "amazon_ads_profile_id_uk": getattr(u, "amazon_ads_profile_id_uk", None),
                "amazon_ads_profile_id_us": getattr(u, "amazon_ads_profile_id_us", None),
                "amazon_ads_profile_id_ca": getattr(u, "amazon_ads_profile_id_ca", None),
            },
            "counts": {
                "top_level": {k: len(v or []) for k, v in (profiles_by_region or {}).items()}
            }
        }), 200

    except Exception as e:
        # token exists but API call failed
        return jsonify({
            "success": False,
            "status": "ads_api_error",
            "has_refresh_token": True,
            "saved": {
                "amazon_ads_refresh_token_updated_at": u.amazon_ads_refresh_token_updated_at.isoformat()
                if getattr(u, "amazon_ads_refresh_token_updated_at", None) else None,
                "amazon_ads_manager_profile_id": getattr(u, "amazon_ads_manager_profile_id", None),
                "amazon_ads_profile_id_uk": getattr(u, "amazon_ads_profile_id_uk", None),
                "amazon_ads_profile_id_us": getattr(u, "amazon_ads_profile_id_us", None),
                "amazon_ads_profile_id_ca": getattr(u, "amazon_ads_profile_id_ca", None),
            },
            "error": str(e)
        }), 502



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


def _to_date(x, strict: bool = False, field_name: str = "date"):
    """
    Converts x -> datetime.date or None.

    strict=False (default): old behavior (returns None for invalid/blank)
    strict=True: raises ValueError on invalid input (use for request validation)
    """
    if x is None:
        if strict:
            raise ValueError(f"{field_name} is required")
        return None

    # handle pandas NaN/NaT safely
    try:
        if pd.isna(x):
            if strict:
                raise ValueError(f"{field_name} is required")
            return None
    except Exception:
        pass

    if isinstance(x, str):
        s = x.strip()
        if s == "":
            if strict:
                raise ValueError(f"{field_name} is required")
            return None
        x = s

    try:
        dt = pd.to_datetime(x, errors="raise")
        return dt.date()
    except Exception:
        if strict:
            raise ValueError(f"Invalid {field_name}. Use YYYY-MM-DD")
        return None


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

# --- helpers (keep once in your file; don't duplicate) ---
def _safe_ident(name: str) -> str:
    """Allow only safe Postgres identifier chars (letters/numbers/_)."""
    name = str(name or "")
    return re.sub(r"[^a-zA-Z0-9_]+", "_", name)

def _safe_div(a, b):
    a = float(a or 0.0)
    b = float(b or 0.0)
    return 0.0 if b == 0.0 else (a / b)

def _latest_end_date_for_month(model, user_id, country, first_day, last_day):
    """
    Picks latest end_date for rows whose start_date falls within [first_day, last_day].
    This prevents double-counting when you have multiple runs like:
      2026-02-01 -> 2026-02-11
      2026-02-01 -> 2026-02-12   (latest)
    """
    return (
        db.session.query(func.max(model.end_date))
        .filter(
            model.user_id == user_id,
            model.country == country,
            model.start_date >= first_day,
            model.start_date <= last_day,
        )
        .scalar()
    )

# ------------------------------------------ Sponsored Products Advertised Product Report Route ------------------------------------------

@advertisement_api_routes_bp.route("/api/ads/manager/sp_advertised_product_report", methods=["POST"])
def manager_sp_advertised_product_report():
    """
    Creates SP advertised product report across all accessible advertiser profiles,
    merges into one Excel AND saves rows to DB (idempotent UPSERT).

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

        # ✅ HARD VALIDATE REQUEST DATES ONCE
        try:
            sd = _to_date(start_date, strict=True, field_name="start_date")
            ed = _to_date(end_date, strict=True, field_name="end_date")
        except ValueError as ve:
            return jsonify({"error": str(ve)}), 400

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
            # convert scalars to series
            a = a if isinstance(a, pd.Series) else pd.Series([a] * len(df))
            b = b if isinstance(b, pd.Series) else pd.Series([b] * len(df))
            b = b.replace({0: pd.NA})
            return (a / b).fillna(0.0)

        # =========================================================
        # ✅ BUILD OUTPUT (DATES ALWAYS FROM REQUEST)
        # =========================================================
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
        # ✅ DEDUPE IN-BATCH ON UNIQUE KEY
        # =========================================================
        key_cols = [
            "Start Date", "End Date", "Country", "Profile ID",
            "Campaign ID", "Ad Group ID", "Advertised SKU", "Advertised ASIN"
        ]

        for c in ["Country", "Profile ID", "Campaign ID", "Ad Group ID", "Advertised SKU", "Advertised ASIN"]:
            if c in out.columns:
                out[c] = out[c].fillna("").astype(str)

        out = out.drop_duplicates(subset=key_cols, keep="last").reset_index(drop=True)

        # =========================================================
        # ✅ SAVE TO DATABASE (UPSERT) — PERMANENT NOT-NULL FIX
        # =========================================================
        now = datetime.utcnow()

        rows_to_insert = []
        for rec in out.to_dict(orient="records"):
            rows_to_insert.append({
                "user_id": user_id,
                "created_at": now,
                "updated_at": now,

                # ✅ HARD-BIND request dates: never pull from rec
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

        # ✅ extra safety: never let null dates reach SQL
        if any(r["start_date"] is None or r["end_date"] is None for r in rows_to_insert):
            return jsonify({"error": "Sanity check failed: start_date/end_date became null before insert"}), 500

        if rows_to_insert:
            table = amazon_sponsored_products.__table__
            stmt = insert(table).values(rows_to_insert)

            conflict_cols = [
                "user_id",
                "start_date",
                "end_date",
                "country",
                "profile_id",
                "campaign_id",
                "ad_group_id",
                "advertised_sku",
                "advertised_asin",
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

            stmt = stmt.on_conflict_do_update(
                index_elements=conflict_cols,
                set_=update_cols
            )

            db.session.execute(stmt)
            db.session.commit()

        # =========================================================
        # ✅ API JSON RESPONSE (no excel)
        # =========================================================
        if not return_excel:
            return jsonify({
                "message": "Saved Sponsored Products report rows (UPSERT)",
                "rows_saved": len(rows_to_insert),
                "start_date": start_date,
                "end_date": end_date,
                "time_unit": time_unit,
                "countries": list(wanted_countries) if wanted_countries else None,
            }), 200

        # =========================================================
        # ✅ RETURN EXCEL
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


# ------------------------------------------- Combine Sponsored product and display routes ------------------------------------------------
@advertisement_api_routes_bp.route("/api/ads/monthly_sp_sd_to_db", methods=["POST"])
def monthly_sp_sd_to_db():
    """
    Save monthly aggregated Sponsored Products + Sponsored Display + Sponsored Brands (keywords) into:
      public.adsmonthly_{user_id}_{country}_{month}_{year}

    ✅ Adds 3 columns:
      - product_spend (SP spend)
      - display_spend (SD spend)
      - brand_spend   (SB spend)

    IMPORTANT:
    - Does NOT delete previous raw report rows in amazon_sponsored_* tables.
    - To avoid double-counting, it aggregates ONLY the latest run for that month
      (latest end_date for the month/user/country) *when end_date exists*.
    - SB keywords table often cannot map to SKU/ASIN. In that case, SB spend is still included
      as a grouped row with empty sku/asin and included in Grand Total.
    """
    try:
        user_id = _require_jwt_user_id()
        payload = request.get_json(force=True) or {}

        # ---- inputs ----
        month = int(payload.get("month") or 0)
        year = int(payload.get("year") or 0)
        country = str(payload.get("country") or "").upper().strip()

        include = payload.get("include") or ["SP", "SD", "SB"]
        include = {str(x).upper().strip() for x in include}

        if not (1 <= month <= 12):
            return jsonify({"error": "month must be 1..12"}), 400
        if not (2000 <= year <= 2100):
            return jsonify({"error": "year looks invalid"}), 400
        if not country:
            return jsonify({"error": "country is required (e.g. UK/US/CA)"}), 400
        if not include.intersection({"SP", "SD", "SB"}):
            return jsonify({"error": "include must contain SP and/or SD and/or SB"}), 400

        first_day = date(year, month, 1)
        last_day = date(year, month, calendar.monthrange(year, month)[1])

        frames = []

        # ---------------- helpers ----------------
        def _safe_div(a, b):
            try:
                a = float(a or 0.0)
                b = float(b or 0.0)
                return (a / b) if b else 0.0
            except Exception:
                return 0.0

        def _pick_attr(obj, names):
            """Return first non-empty attribute value among candidates."""
            for n in names:
                if hasattr(obj, n):
                    v = getattr(obj, n)
                    if v is not None and str(v).strip() != "":
                        return str(v).strip()
            return ""

        def _apply_filters_if_exist(q, model):
            """Apply user/country/date filters only if those columns exist on the model."""
            if hasattr(model, "user_id"):
                q = q.filter(model.user_id == user_id)
            if hasattr(model, "country"):
                q = q.filter(model.country == country)
            if hasattr(model, "start_date"):
                q = q.filter(model.start_date >= first_day, model.start_date <= last_day)
            return q

        # =========================
        # 1) Sponsored Products (SP)
        # =========================
        if "SP" in include:
            sp_latest_end = None
            try:
                sp_latest_end = _latest_end_date_for_month(
                    amazon_sponsored_products, user_id, country, first_day, last_day
                )
            except Exception:
                sp_latest_end = None

            q = amazon_sponsored_products.query
            q = _apply_filters_if_exist(q, amazon_sponsored_products)
            if sp_latest_end and hasattr(amazon_sponsored_products, "end_date"):
                q = q.filter(amazon_sponsored_products.end_date == sp_latest_end)

            sp_rows = q.all()

            if sp_rows:
                sp_df = pd.DataFrame([{
                    "source": "SP",
                    "advertised_sku": (getattr(r, "advertised_sku", None) or ""),
                    "advertised_asin": (getattr(r, "advertised_asin", None) or ""),
                    "currency": getattr(r, "currency", None),

                    "impressions": int(getattr(r, "impressions", 0) or 0),
                    "clicks": int(getattr(r, "clicks", 0) or 0),
                    "spend": float(getattr(r, "spend", 0.0) or 0.0),

                    # SP uses 7d attribution in your schema (keep safe)
                    "sales": float(getattr(r, "sales_7d", 0.0) or 0.0),
                    "orders": float(getattr(r, "orders_7d", 0.0) or 0.0),
                    "units": float(getattr(r, "units_7d", 0.0) or 0.0),

                    "new_to_brand_sales": float(getattr(r, "new_to_brand_sales", 0.0) or 0.0),
                    "advertised_unit_sale": float(getattr(r, "adv_sku_units_7d", 0.0) or 0.0),
                    "other_unit_sale": float(getattr(r, "other_sku_units_7d", 0.0) or 0.0),

                    # ✅ new spend columns
                    "product_spend": float(getattr(r, "spend", 0.0) or 0.0),
                    "display_spend": 0.0,
                    "brand_spend": 0.0,
                } for r in sp_rows])
                frames.append(sp_df)

        # =========================
        # 2) Sponsored Display (SD)
        # =========================
        if "SD" in include:
            sd_latest_end = None
            try:
                sd_latest_end = _latest_end_date_for_month(
                    amazon_sponsored_display_advertised_products, user_id, country, first_day, last_day
                )
            except Exception:
                sd_latest_end = None

            q = amazon_sponsored_display_advertised_products.query
            q = _apply_filters_if_exist(q, amazon_sponsored_display_advertised_products)
            if sd_latest_end and hasattr(amazon_sponsored_display_advertised_products, "end_date"):
                q = q.filter(amazon_sponsored_display_advertised_products.end_date == sd_latest_end)

            sd_rows = q.all()

            if sd_rows:
                sd_df = pd.DataFrame([{
                    "source": "SD",
                    "advertised_sku": (getattr(r, "advertised_sku", None) or ""),
                    "advertised_asin": (getattr(r, "advertised_asin", None) or ""),
                    "currency": getattr(r, "currency", None),

                    "impressions": int(getattr(r, "impressions", 0) or 0),
                    "clicks": int(getattr(r, "clicks", 0) or 0),
                    "spend": float(getattr(r, "spend", 0.0) or 0.0),

                    # SD uses 14d attribution in your schema (keep safe)
                    "sales": float(getattr(r, "sales_14d", 0.0) or 0.0),
                    "orders": float(getattr(r, "orders_14d", 0.0) or 0.0),
                    "units": float(getattr(r, "units_14d", 0.0) or 0.0),

                    "new_to_brand_sales": float(getattr(r, "new_to_brand_sales", 0.0) or 0.0),

                    "advertised_unit_sale": 0.0,
                    "other_unit_sale": 0.0,

                    # ✅ new spend columns
                    "product_spend": 0.0,
                    "display_spend": float(getattr(r, "spend", 0.0) or 0.0),
                    "brand_spend": 0.0,
                } for r in sd_rows])
                frames.append(sd_df)

        # =========================
        # 3) Sponsored Brands (SB) - keywords
        # =========================
        if "SB" in include:
            sb_latest_end = None
            try:
                sb_latest_end = _latest_end_date_for_month(
                    amazon_sponsored_brands_keywords, user_id, country, first_day, last_day
                )
            except Exception:
                sb_latest_end = None

            q = amazon_sponsored_brands_keywords.query
            q = _apply_filters_if_exist(q, amazon_sponsored_brands_keywords)
            if sb_latest_end and hasattr(amazon_sponsored_brands_keywords, "end_date"):
                q = q.filter(amazon_sponsored_brands_keywords.end_date == sb_latest_end)

            sb_rows = q.all()

            if sb_rows:
                sb_df = pd.DataFrame([{
                    "source": "SB",

                    # SB keywords often cannot map to SKU/ASIN; try common field names if present
                    "advertised_sku": _pick_attr(r, ["advertised_sku", "sku", "product_sku"]) or "",
                    "advertised_asin": _pick_attr(r, ["advertised_asin", "asin", "product_asin"]) or "",

                    "currency": getattr(r, "currency", None),

                    "impressions": int(getattr(r, "impressions", 0) or 0),
                    "clicks": int(getattr(r, "clicks", 0) or 0),
                    "spend": float(getattr(r, "spend", 0.0) or 0.0),

                    # keyword report may not have these -> keep 0
                    "sales": float(getattr(r, "sales", 0.0) or 0.0),
                    "orders": float(getattr(r, "orders", 0.0) or 0.0),
                    "units": float(getattr(r, "units", 0.0) or 0.0),

                    "new_to_brand_sales": float(getattr(r, "new_to_brand_sales", 0.0) or 0.0),
                    "advertised_unit_sale": 0.0,
                    "other_unit_sale": 0.0,

                    # ✅ new spend columns
                    "product_spend": 0.0,
                    "display_spend": 0.0,
                    "brand_spend": float(getattr(r, "spend", 0.0) or 0.0),
                } for r in sb_rows])
                frames.append(sb_df)

        if not frames:
            return jsonify({
                "error": "No rows found for this user/country/month (or no latest end_date found)."
            }), 404

        df = pd.concat(frames, ignore_index=True)

        # Ensure missing columns exist
        for col in [
            "impressions", "clicks", "spend", "sales", "orders", "units",
            "advertised_unit_sale", "other_unit_sale", "new_to_brand_sales",
            "product_spend", "display_spend", "brand_spend",
        ]:
            if col not in df.columns:
                df[col] = 0.0

        df["advertised_sku"] = df.get("advertised_sku", "").fillna("").astype(str).str.strip()
        df["advertised_asin"] = df.get("advertised_asin", "").fillna("").astype(str).str.strip()

        # ---- group by SKU + ASIN ----
        g = df.groupby(["advertised_sku", "advertised_asin"], as_index=False).agg({
            "impressions": "sum",
            "clicks": "sum",
            "spend": "sum",
            "sales": "sum",
            "orders": "sum",
            "units": "sum",
            "advertised_unit_sale": "sum",
            "other_unit_sale": "sum",
            "new_to_brand_sales": "sum",

            # ✅ new spend columns
            "product_spend": "sum",
            "display_spend": "sum",
            "brand_spend": "sum",

            # ✅ keep which sources contributed
            "source": lambda s: ",".join(sorted(set([str(x).upper() for x in s if x])))
        })

        # ---- output in your monthly table shape ----
        out = pd.DataFrame()
        out["sno"] = range(1, len(g) + 1)
        out["products"] = g["advertised_sku"]
        out["asin"] = g["advertised_asin"]

        def _ad_type_from_source(src: str) -> str:
            parts = set((src or "").split(","))
            labels = []
            if "SD" in parts:
                labels.append("sponsored_display")
            if "SP" in parts:
                labels.append("sponsored_product")
            if "SB" in parts:
                labels.append("sponsored_brands")
            return ", ".join(labels) if labels else None

        out["ad_type"] = g["source"].apply(_ad_type_from_source)
        out["match_type"] = None

        out["impressions"] = g["impressions"].astype(int)
        out["clicks"] = g["clicks"].astype(int)

        # percentages 0..100
        out["ctr"] = [
            _safe_div(c, i) * 100.0
            for c, i in zip(g["clicks"].tolist(), g["impressions"].tolist())
        ]
        out["cpc"] = [
            _safe_div(sp, c)
            for sp, c in zip(g["spend"].tolist(), g["clicks"].tolist())
        ]

        out["spend"] = g["spend"].astype(float)
        out["sale_units"] = g["units"].astype(float)
        out["sale_amount"] = g["sales"].astype(float)

        out["advertised_unit_sale"] = g["advertised_unit_sale"].astype(float)
        out["other_unit_sale"] = g["other_unit_sale"].astype(float)
        out["new_to_brand_sales"] = g["new_to_brand_sales"].astype(float)

        # ✅ NEW columns
        out["product_spend"] = g["product_spend"].astype(float)
        out["display_spend"] = g["display_spend"].astype(float)
        out["brand_spend"] = g["brand_spend"].astype(float)

        out["conversion_rate"] = [
            _safe_div(o, c) * 100.0
            for o, c in zip(g["orders"].tolist(), g["clicks"].tolist())
        ]
        out["roas"] = [
            _safe_div(sa, sp)
            for sa, sp in zip(g["sales"].tolist(), g["spend"].tolist())
        ]
        out["acos"] = [
            _safe_div(sp, sa) * 100.0
            for sp, sa in zip(g["spend"].tolist(), g["sales"].tolist())
        ]

        # ---- Grand Total row ----
        total_impr = int(out["impressions"].sum())
        total_clicks = int(out["clicks"].sum())
        total_spend = float(out["spend"].sum())
        total_sales_amt = float(out["sale_amount"].sum())
        total_orders = float(g["orders"].sum())
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

            # ✅ totals for new columns
            "product_spend": float(out["product_spend"].sum()),
            "display_spend": float(out["display_spend"].sum()),
            "brand_spend": float(out["brand_spend"].sum()),

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

            -- ✅ NEW
            product_spend DOUBLE PRECISION,
            display_spend DOUBLE PRECISION,
            brand_spend DOUBLE PRECISION,

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

        insert_sql = f"""
        INSERT INTO public.{table_name} (
            user_id, country, month, year,
            sno, products, asin, ad_type, match_type,
            impressions, clicks, ctr, cpc, spend,

            product_spend, display_spend, brand_spend,

            sale_units, sale_amount,
            advertised_unit_sale, other_unit_sale, new_to_brand_sales,
            conversion_rate, roas, acos
        ) VALUES (
            :user_id, :country, :month, :year,
            :sno, :products, :asin, :ad_type, :match_type,
            :impressions, :clicks, :ctr, :cpc, :spend,

            :product_spend, :display_spend, :brand_spend,

            :sale_units, :sale_amount,
            :advertised_unit_sale, :other_unit_sale, :new_to_brand_sales,
            :conversion_rate, :roas, :acos
        );
        """

        try:
            db.session.execute(text(create_sql))

            # ✅ if table existed previously, add missing columns safely
            db.session.execute(text(f'ALTER TABLE public.{table_name} ADD COLUMN IF NOT EXISTS product_spend DOUBLE PRECISION;'))
            db.session.execute(text(f'ALTER TABLE public.{table_name} ADD COLUMN IF NOT EXISTS display_spend DOUBLE PRECISION;'))
            db.session.execute(text(f'ALTER TABLE public.{table_name} ADD COLUMN IF NOT EXISTS brand_spend DOUBLE PRECISION;'))

            # ✅ wipe monthly output table before inserting
            db.session.execute(text(f"TRUNCATE TABLE public.{table_name};"))

            params = []
            for r in items:
                params.append({
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

                    # ✅ NEW
                    "product_spend": float(r.get("product_spend") or 0.0),
                    "display_spend": float(r.get("display_spend") or 0.0),
                    "brand_spend": float(r.get("brand_spend") or 0.0),

                    "sale_units": float(r.get("sale_units") or 0.0),
                    "sale_amount": float(r.get("sale_amount") or 0.0),

                    "advertised_unit_sale": float(r.get("advertised_unit_sale") or 0.0),
                    "other_unit_sale": float(r.get("other_unit_sale") or 0.0),
                    "new_to_brand_sales": float(r.get("new_to_brand_sales") or 0.0),

                    "conversion_rate": float(r.get("conversion_rate") or 0.0),
                    "roas": float(r.get("roas") or 0.0),
                    "acos": float(r.get("acos") or 0.0),
                })

            if params:
                db.session.execute(text(insert_sql), params)

            db.session.commit()

        except Exception:
            db.session.rollback()
            raise

        return jsonify({
            "message": "Monthly ads table saved to DB successfully (SP + SD + SB) using ONLY latest end_date run (when available)",
            "table_name": f"public.{table_name}",
            "country": country,
            "month": month,
            "year": year,
            "include": sorted(list(include)),
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
        # ✅ show real error in terminal + return message
        import traceback
        traceback.print_exc()
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

#------------------------------------------ Sponsored Display Advertised Product Report ------------------------------------------#

@advertisement_api_routes_bp.route("/api/ads/manager/sd_advertised_product_report/sync",methods=["POST"])
def manager_sd_advertised_product_report_sync_one_hit_country_only():
    """
    ONE-HIT (blocking) + COUNTRY ONLY:
    - Requires countries[]: ["UK"] / ["US"] / ["UK","US"]
    - Creates report(s) only for those countries
    - Polls until COMPLETED/SUCCESS or timeout
    - Downloads + saves to DB
    - Never returns 202
    """

    try:
        user_id = _require_jwt_user_id()
        u = _get_user_row(user_id)

        data = request.get_json(force=True) or {}

        start_date = data.get("start_date")
        end_date = data.get("end_date")
        time_unit = (data.get("time_unit") or "SUMMARY").upper()

        # ✅ Increase default window for real "one hit"
        max_wait_seconds = int(data.get("max_wait_seconds") or 900)  # 15 min
        poll_every_seconds = int(data.get("poll_every_seconds") or 10)

        if time_unit not in {"DAILY", "SUMMARY"}:
            return jsonify({"error": "time_unit must be DAILY or SUMMARY"}), 400
        if not start_date or not end_date:
            return jsonify({"error": "start_date and end_date required"}), 400
        if not u.amazon_ads_refresh_token:
            return jsonify({"error": "Amazon Ads not connected"}), 400

        # ✅ REQUIRE countries
        wanted = {str(c).upper().strip() for c in (data.get("countries") or []) if str(c).strip()}
        if not wanted:
            return jsonify({"error": "countries[] is required. Example: {\"countries\":[\"UK\"]}"}), 400

        # normalize
        if "GB" in wanted:
            wanted.discard("GB")
            wanted.add("UK")

        # map countries -> regions (your logic)
        regions_to_use = set()
        if "UK" in wanted:
            regions_to_use.add("EU")
        if "US" in wanted:
            regions_to_use.add("NA")

        if not regions_to_use:
            return jsonify({"error": "Unsupported countries. Use UK and/or US (or GB for UK)."}), 400

        access_token = get_ads_access_token_from_refresh(u.amazon_ads_refresh_token)

        # -----------------------------
        # Find profiles (manager -> child)
        # -----------------------------
        top_profiles = list_top_level_profiles_all_regions(access_token)
        manager_profile_id = find_manager_profile_id(top_profiles)
        child_by_region = (
            list_child_profiles_all_regions(access_token, manager_profile_id)
            if manager_profile_id else top_profiles
        )

        # -----------------------------
        # Create report(s) ONLY for wanted countries
        # -----------------------------
        reports = []
        for region, profiles in (child_by_region or {}).items():
            if region not in regions_to_use:
                continue

            for p in profiles or []:
                profile_id = p.get("profileId")
                if not profile_id:
                    continue

                cc = (p.get("countryCode") or "").upper()
                country_label = "UK" if cc == "GB" else cc

                # strict filter
                if country_label not in wanted:
                    continue

                auth = AmazonAdsAuthContext(access_token=access_token, profile_id=str(profile_id))
                ads = AmazonAdsReportingClient(base_url=ADS_ENDPOINTS[region], auth=auth, timeout=60)

                report_id = ads.create_sd_advertised_product_report(start_date, end_date, time_unit)

                reports.append({
                    "region": region,
                    "country": country_label,
                    "profile_id": str(profile_id),
                    "report_id": str(report_id),
                })

        if not reports:
            return jsonify({
                "error": "No advertiser profiles found for requested countries.",
                "countries": sorted(list(wanted)),
                "regions_used": sorted(list(regions_to_use)),
            }), 400

        # -----------------------------
        # Poll (robust): backoff + fail-fast
        # -----------------------------
        deadline = time.time() + max_wait_seconds
        status_map = {}

        # start with poll_every_seconds, then back off up to 60s
        interval = max(2, poll_every_seconds)
        max_interval = 60

        DONE = {"COMPLETED", "SUCCESS"}
        FAIL = {"FAILURE", "FAILED", "CANCELLED", "CANCELED"}

        while True:
            all_done = True
            any_failed = False
            failed_list = []

            for r in reports:
                auth = AmazonAdsAuthContext(access_token=access_token, profile_id=r["profile_id"])
                ads = AmazonAdsReportingClient(base_url=ADS_ENDPOINTS[r["region"]], auth=auth, timeout=60)

                st = ads.get_report_status(r["report_id"]) or {}
                status_map[r["report_id"]] = st

                status = (st.get("status") or "").upper()

                if status in FAIL:
                    any_failed = True
                    failed_list.append({**r, "status": status, "report": st})

                if status not in DONE:
                    all_done = False

            if any_failed:
                return jsonify({
                    "error": "One or more reports failed.",
                    "start_date": start_date,
                    "end_date": end_date,
                    "time_unit": time_unit,
                    "failed": failed_list,
                }), 502

            if all_done:
                break

            if time.time() >= deadline:
                pending_debug = []
                for r in reports:
                    st = status_map.get(r["report_id"]) or {}
                    pending_debug.append({
                        "region": r["region"],
                        "country": r["country"],
                        "profile_id": r["profile_id"],
                        "report_id": r["report_id"],
                        "status": (st.get("status") or "UNKNOWN"),
                        "report": st,
                    })

                return jsonify({
                    "error": "Report not ready within max_wait_seconds. Increase max_wait_seconds.",
                    "start_date": start_date,
                    "end_date": end_date,
                    "time_unit": time_unit,
                    "countries": sorted(list(wanted)),
                    "max_wait_seconds": max_wait_seconds,
                    "last_poll_interval_seconds": interval,
                    "pending": pending_debug,
                }), 504

            time.sleep(interval)
            interval = min(max_interval, int(interval * 1.5))  # backoff

        # -----------------------------
        # Download
        # -----------------------------
        rows_all = []
        for r in reports:
            st = status_map.get(r["report_id"]) or {}
            url = st.get("url") or st.get("location")
            if not url:
                return jsonify({
                    "error": f"Report completed but url missing for report_id={r['report_id']}",
                    "report": st
                }), 500

            auth = AmazonAdsAuthContext(access_token=access_token, profile_id=r["profile_id"])
            ads = AmazonAdsReportingClient(base_url=ADS_ENDPOINTS[r["region"]], auth=auth, timeout=60)

            rows = ads.download_gzip_json(url)
            for row in (rows or []):
                if isinstance(row, dict):
                    row["_profileId"] = r["profile_id"]
                    row["_country"] = r["country"]
                    rows_all.append(row)

        if not rows_all:
            return jsonify({"error": "Report completed but returned no rows"}), 400

        df = pd.DataFrame(rows_all)

        # Delete existing
        db.session.query(amazon_sponsored_display_advertised_products).filter(
            amazon_sponsored_display_advertised_products.user_id == user_id,
            amazon_sponsored_display_advertised_products.start_date == _to_date(start_date),
            amazon_sponsored_display_advertised_products.end_date == _to_date(end_date),
        ).delete(synchronize_session=False)
        db.session.commit()

        # numeric conversions
        for col in ["impressions", "clicks", "cost", "sales", "purchases", "unitsSold"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

        def _safe_div(a, b):
            a = float(a or 0.0)
            b = float(b or 0.0)
            return (a / b) if b else 0.0

        now = datetime.utcnow()
        inserts = []

        for rec in df.to_dict("records"):
            cost = _to_float(rec.get("cost"))
            clicks = _to_int(rec.get("clicks"))
            impressions = _to_int(rec.get("impressions"))
            sales = _to_float(rec.get("sales"))

            inserts.append({
                "user_id": user_id,
                "created_at": now,
                "updated_at": now,
                "start_date": _to_date(start_date),
                "end_date": _to_date(end_date),

                "country": rec.get("_country"),
                "profile_id": str(rec.get("_profileId") or ""),

                "campaign_id": str(rec.get("campaignId") or ""),
                "campaign_name": rec.get("campaignName"),
                "ad_group_id": str(rec.get("adGroupId") or ""),
                "ad_group_name": rec.get("adGroupName"),

                "advertised_sku": rec.get("promotedSku"),
                "advertised_asin": rec.get("promotedAsin"),

                "currency": rec.get("campaignBudgetCurrencyCode"),

                "impressions": _to_int(rec.get("impressions")),
                "clicks": clicks,
                "spend": cost,

                "cpc": _safe_div(cost, clicks),
                "ctr": _safe_div(clicks, impressions),

                "sales_14d": sales,
                "orders_14d": _to_int(rec.get("purchases")),
                "units_14d": _to_int(rec.get("unitsSold")),

                "acos": _safe_div(cost, sales),
                "roas": _safe_div(sales, cost),
            })

        if inserts:
            db.session.bulk_insert_mappings(amazon_sponsored_display_advertised_products, inserts)
            db.session.commit()

        return jsonify({
            "message": "SD advertised product report synced and saved (one-hit, country-only)",
            "start_date": start_date,
            "end_date": end_date,
            "time_unit": time_unit,
            "countries": sorted(list(wanted)),
            "profiles_used": len(reports),
            "rows_saved": len(inserts),
        }), 200

    except Exception as e:
        db.session.rollback()
        return jsonify({"error": str(e)}), 500


@advertisement_api_routes_bp.route("/api/ads/manager/sb_keyword_report", methods=["POST"])
def manager_sb_keyword_report():
    """
    Body:
      {
        "start_date": "YYYY-MM-DD",
        "end_date": "YYYY-MM-DD",
        "time_unit": "SUMMARY" | "DAILY",
        "countries": ["UK","US"],      # optional
        "return_excel": true           # optional (default true)
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
            wanted_countries = {str(x).upper().strip() for x in wanted_countries if str(x).strip()}
            if "GB" in wanted_countries:
                wanted_countries.discard("GB")
                wanted_countries.add("UK")
        else:
            wanted_countries = None

        if time_unit not in {"DAILY", "SUMMARY"}:
            return jsonify({"error": "time_unit must be DAILY or SUMMARY"}), 400
        if not start_date or not end_date:
            return jsonify({"error": "start_date and end_date required (YYYY-MM-DD)"}), 400

        # validate dates once
        try:
            sd = _to_date(start_date, strict=True, field_name="start_date")
            ed = _to_date(end_date, strict=True, field_name="end_date")
        except ValueError as ve:
            return jsonify({"error": str(ve)}), 400

        if not u.amazon_ads_refresh_token:
            return jsonify({"error": "Amazon Ads not connected for this user."}), 400

        access_token = get_ads_access_token_from_refresh(u.amazon_ads_refresh_token)

        # manager -> child advertiser profiles
        top_profiles = list_top_level_profiles_all_regions(access_token)
        manager_profile_id = find_manager_profile_id(top_profiles)
        child_by_region = (
            list_child_profiles_all_regions(access_token, manager_profile_id)
            if manager_profile_id else top_profiles
        )

        # flatten profiles with region + normalized country label
        all_profiles = []
        for region, profs in (child_by_region or {}).items():
            for p in profs or []:
                cc = (p.get("countryCode") or "").upper()
                label = "UK" if cc == "GB" else cc
                p["_region"] = region
                p["_country_label"] = label
                all_profiles.append(p)

        if wanted_countries:
            all_profiles = [p for p in all_profiles if p.get("_country_label") in wanted_countries]

        if not all_profiles:
            return jsonify({"error": "No advertiser profiles found (or your country filter removed all)."}), 400

        merged_rows = []
        join_maps = {}  # profileId -> {"campaign_to_portfolio":{}, "portfolioid_to_name":{}}
        download_errors = []

        # ---------------------------
        # 1) Fetch report for profiles
        # ---------------------------
        for p in all_profiles:
            profile_id = p.get("profileId")
            if not profile_id:
                continue

            region = p["_region"]
            base_url = ADS_ENDPOINTS[region]

            auth = AmazonAdsAuthContext(access_token=access_token, profile_id=str(profile_id))
            ads = AmazonAdsReportingClient(base_url=base_url, auth=auth, timeout=60)

            # build joins (best-effort)
            try:
                sb_campaigns = ads.list_sb_campaigns()
                portfolios = ads.list_portfolios()

                campaign_to_portfolio = {
                    str(c.get("campaignId")): str(c.get("portfolioId"))
                    for c in (sb_campaigns or [])
                    if c.get("campaignId") and c.get("portfolioId") is not None
                }
                portfolioid_to_name = {
                    str(po.get("portfolioId")): (po.get("name") or "")
                    for po in (portfolios or [])
                    if po.get("portfolioId")
                }
                join_maps[str(profile_id)] = {
                    "campaign_to_portfolio": campaign_to_portfolio,
                    "portfolioid_to_name": portfolioid_to_name,
                }
            except Exception as e:
                join_maps[str(profile_id)] = {"campaign_to_portfolio": {}, "portfolioid_to_name": {}}
                download_errors.append({
                    "profile_id": str(profile_id),
                    "step": "join_maps",
                    "error": str(e),
                })

            # create + download report
            try:
                report_id = ads.create_sb_keyword_report(start_date, end_date, time_unit=time_unit)
                location = ads.wait_until_ready(report_id, max_wait_seconds=1800, poll_every_seconds=10)
                rows = ads.download_gzip_json(location)
            except Exception as e:
                download_errors.append({
                    "profile_id": str(profile_id),
                    "country": p.get("_country_label"),
                    "region": region,
                    "step": "report_download",
                    "error": str(e),
                })
                continue

            if not isinstance(rows, list):
                download_errors.append({
                    "profile_id": str(profile_id),
                    "step": "rows_type",
                    "error": f"Report returned unexpected type: {type(rows)}",
                })
                continue

            for r in rows:
                if not isinstance(r, dict):
                    continue
                r["_profileId"] = str(profile_id)
                r["_country"] = p["_country_label"]
                merged_rows.append(r)

        if not merged_rows:
            return jsonify({
                "error": "Reports returned no rows",
                "download_errors": download_errors[:50],
            }), 400

        df = pd.DataFrame(merged_rows)

        # ---------------------------
        # 2) Normalize / compute fields
        # ---------------------------
        # safe numeric conversions (ONLY columns we requested)
        for col in [
            "impressions", "clicks",
            "viewableImpressions",
            "topOfSearchImpressionShare",
            "cost",
        ]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

        # targeting text (keywordText preferred)
        for c in ["keywordText", "targetingText", "targetingExpression"]:
            if c not in df.columns:
                df[c] = ""
        df["__targeting_value"] = (
            df["keywordText"].fillna("").astype(str)
              .where(df["keywordText"].fillna("").astype(str) != "", None)
        )
        df["__targeting_value"] = df["__targeting_value"].fillna(df["targetingText"].fillna("").astype(str))
        df["__targeting_value"] = df["__targeting_value"].fillna(df["targetingExpression"].fillna("").astype(str))
        df["__targeting_value"] = df["__targeting_value"].fillna("")

        # currency
        if "campaignBudgetCurrencyCode" not in df.columns:
            df["campaignBudgetCurrencyCode"] = ""
        df["__currency_value"] = df["campaignBudgetCurrencyCode"].fillna("").astype(str)

        # portfolio join (per profileId + campaignId)
        if "campaignId" not in df.columns:
            df["campaignId"] = ""
        df["__portfolio_name"] = ""

        for pid, sub_idx in df.groupby("_profileId").groups.items():
            maps = join_maps.get(str(pid)) or {}
            campaign_to_portfolio = maps.get("campaign_to_portfolio") or {}
            portfolioid_to_name = maps.get("portfolioid_to_name") or {}

            sub = df.loc[sub_idx]
            portfolio_id_series = sub["campaignId"].astype(str).map(campaign_to_portfolio).fillna("")
            portfolio_name_series = portfolio_id_series.astype(str).map(portfolioid_to_name).fillna("")
            df.loc[sub_idx, "__portfolio_name"] = portfolio_name_series

        # ensure ID columns exist (Amazon often omits keywordId/targetingId for sbTargeting)
        for id_col in ["campaignId", "adGroupId", "keywordId", "targetingId"]:
            if id_col not in df.columns:
                df[id_col] = ""

        # ---------------------------
        # 3) Build output (your exact columns)
        # ---------------------------
        out = pd.DataFrame()
        out["Start Date"] = start_date
        out["End Date"] = end_date

        out["Country"] = df.get("_country", "")
        out["Profile ID"] = df.get("_profileId", "")

        out["Portfolio name"] = df.get("__portfolio_name", "")
        out["Currency"] = df.get("__currency_value", "")

        out["Campaign Name"] = df.get("campaignName", "")
        out["Ad Group Name"] = df.get("adGroupName", "")
        out["Targeting"] = df.get("__targeting_value", "")
        out["Match Type"] = df.get("matchType", "")
        out["Cost Type"] = df.get("costType", "")

        out["Impressions"] = df.get("impressions", 0).astype(float)
        out["Top-of-search impression share"] = df.get("topOfSearchImpressionShare", 0.0).astype(float)
        out["Viewable impressions"] = df.get("viewableImpressions", 0).astype(float)
        out["Clicks"] = df.get("clicks", 0).astype(float)

        # CTR (as % like console)
        out["Click-Thru Rate (CTR)"] = [
            _safe_div(c, i) * 100.0 for c, i in zip(out["Clicks"].tolist(), out["Impressions"].tolist())
        ]

        out["Spend"] = df.get("cost", 0.0).astype(float)

        # CPC
        out["Cost Per Click (CPC)"] = [
            _safe_div(sp, c) for sp, c in zip(out["Spend"].tolist(), out["Clicks"].tolist())
        ]

        # cast ints where appropriate
        out["Impressions"] = out["Impressions"].astype(int)
        out["Clicks"] = out["Clicks"].astype(int)
        out["Viewable impressions"] = out["Viewable impressions"].astype(int)

        # ---------------------------
        # 4) Dedupe + stable synthetic IDs (CRITICAL FIX)
        # ---------------------------
        out["_campaignId"] = df["campaignId"].fillna("").astype(str)
        out["_adGroupId"] = df["adGroupId"].fillna("").astype(str)
        out["_keywordId"] = df["keywordId"].fillna("").astype(str)
        out["_targetingId"] = df["targetingId"].fillna("").astype(str)

        def _stable_targeting_hash(row: pd.Series) -> str:
            parts = [
                row.get("Start Date", ""),
                row.get("End Date", ""),
                row.get("Country", ""),
                row.get("Profile ID", ""),
                row.get("_campaignId", ""),
                row.get("_adGroupId", ""),
                row.get("Campaign Name", ""),
                row.get("Ad Group Name", ""),
                row.get("Targeting", ""),
                row.get("Match Type", ""),
                row.get("Cost Type", ""),
                row.get("Portfolio name", ""),
            ]
            s = "|".join(str(p or "").strip() for p in parts)
            return hashlib.sha1(s.encode("utf-8")).hexdigest()[:24]

        # If Amazon didn’t return keywordId/targetingId, synthesize targetingId
        missing_ids = (
            (out["_keywordId"].str.strip() == "") &
            (out["_targetingId"].str.strip() == "")
        )
        if missing_ids.any():
            out.loc[missing_ids, "_targetingId"] = out.loc[missing_ids].apply(_stable_targeting_hash, axis=1)

        key_cols = [
            "Start Date", "End Date", "Country", "Profile ID",
            "_campaignId", "_adGroupId", "_keywordId", "_targetingId",
            "Match Type", "Cost Type"
        ]

        for c in key_cols:
            out[c] = out[c].fillna("").astype(str)

        out = out.drop_duplicates(subset=key_cols, keep="last").reset_index(drop=True)

        # remove hidden id cols from final export (keep internal if you want)
        out_export = out.drop(columns=["_campaignId", "_adGroupId", "_keywordId", "_targetingId"], errors="ignore")

        # ---------------------------
        # 5) UPSERT into DB (now safe)
        # ---------------------------
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

                # IDs (synthetic targeting_id included if Amazon omitted)
                "campaign_id": str(rec.get("_campaignId") or "") or None,
                "ad_group_id": str(rec.get("_adGroupId") or "") or None,
                "keyword_id": str(rec.get("_keywordId") or "") or None,
                "targeting_id": str(rec.get("_targetingId") or "") or None,

                "portfolio_name": (rec.get("Portfolio name") or None),
                "currency": (rec.get("Currency") or None),

                "campaign_name": (rec.get("Campaign Name") or None),
                "ad_group_name": (rec.get("Ad Group Name") or None),

                "targeting": (rec.get("Targeting") or None),
                "match_type": (rec.get("Match Type") or None),
                "cost_type": (rec.get("Cost Type") or None),

                "impressions": _to_int(rec.get("Impressions")),
                "top_of_search_impression_share": _to_float(rec.get("Top-of-search impression share")),
                "viewable_impressions": _to_int(rec.get("Viewable impressions")),

                "clicks": _to_int(rec.get("Clicks")),
                "ctr": _to_float(rec.get("Click-Thru Rate (CTR)")),  # % value
                "spend": _to_float(rec.get("Spend")),
                "cpc": _to_float(rec.get("Cost Per Click (CPC)")),
            })

        if any(r["start_date"] is None or r["end_date"] is None for r in rows_to_insert):
            return jsonify({"error": "Sanity check failed: start_date/end_date became null before insert"}), 500

        if rows_to_insert:
            table = amazon_sponsored_brands_keywords.__table__
            stmt = insert(table).values(rows_to_insert)

            # conflict key must match your UNIQUE constraint in amazon_sponsored_brands_keywords
            conflict_cols = [
                "user_id", "start_date", "end_date", "country", "profile_id",
                "campaign_id", "ad_group_id", "keyword_id", "targeting_id",
            ]

            update_cols = {
                "updated_at": stmt.excluded.updated_at,
                "currency": stmt.excluded.currency,

                "portfolio_name": stmt.excluded.portfolio_name,
                "campaign_name": stmt.excluded.campaign_name,
                "ad_group_name": stmt.excluded.ad_group_name,
                "targeting": stmt.excluded.targeting,
                "match_type": stmt.excluded.match_type,
                "cost_type": stmt.excluded.cost_type,

                "impressions": stmt.excluded.impressions,
                "top_of_search_impression_share": stmt.excluded.top_of_search_impression_share,
                "viewable_impressions": stmt.excluded.viewable_impressions,

                "clicks": stmt.excluded.clicks,
                "ctr": stmt.excluded.ctr,
                "spend": stmt.excluded.spend,
                "cpc": stmt.excluded.cpc,
            }

            stmt = stmt.on_conflict_do_update(index_elements=conflict_cols, set_=update_cols)
            db.session.execute(stmt)
            db.session.commit()

        # ---------------------------
        # 6) JSON only (optional)
        # ---------------------------
        if not return_excel:
            return jsonify({
                "message": "Saved Sponsored Brands keyword report rows (UPSERT)",
                "rows_saved": len(rows_to_insert),
                "start_date": start_date,
                "end_date": end_date,
                "time_unit": time_unit,
                "countries": sorted(list(wanted_countries)) if wanted_countries else None,
                "download_errors": download_errors[:50],
            }), 200

        # ---------------------------
        # 7) Return Excel
        # ---------------------------
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            out_export.to_excel(writer, index=False, sheet_name="SB_Keywords")
            ws = writer.book["SB_Keywords"]

            # Format Spend as GBP if currency is GBP/GBP-like; otherwise generic currency format.
            if "Spend" in out_export.columns:
                spend_col_idx = list(out_export.columns).index("Spend") + 1
                spend_letter = get_column_letter(spend_col_idx)

                # Decide format from first non-empty currency
                cur = ""
                if "Currency" in out_export.columns:
                    non_empty = [c for c in out_export["Currency"].tolist() if str(c).strip()]
                    cur = (non_empty[0] if non_empty else "")

                money_fmt = u"£#,##0.00" if str(cur).upper() in {"GBP", "UK", "GB"} else u"#,##0.00"
                for r in range(2, ws.max_row + 1):
                    ws[f"{spend_letter}{r}"].number_format = money_fmt

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
    
