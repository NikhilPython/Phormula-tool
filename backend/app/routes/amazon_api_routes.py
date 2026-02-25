from __future__ import annotations
import io, os,time, logging , re
from datetime import datetime
import pandas as pd
import numpy as np
from typing import Any, Dict, List
from datetime import datetime, timezone
from sqlalchemy.dialects.postgresql import insert as pg_insert
import jwt, requests
from config import Config
from dotenv import find_dotenv, load_dotenv
from flask import Blueprint, jsonify, make_response, request
from app import db
from app.models.user_models import amazon_user
from app.utils.token_utils import get_effective_user_id_from_token
from app.utils.formulas_utils import uk_advertising, uk_platform_fee
from app.utils.amazon_utils import (_fetch_fba_skus_all,
_upsert_products_to_db_with_open_date , 
_month_date_range_utc, 
_apply_region_and_marketplace_from_request ,
_flatten_transaction_to_row, 
run_upload_pipeline_from_df, 
_month_name_lower,
_month_to_num,
_month_to_date_range_utc_safe,
compute_totals,
compute_net_reimbursement_from_df, 
upsert_liveorders_from_rows, 
fetch_sku_price_map,
fetch_conversion_rate,
add_profit_column_from_uk_profit,
get_previous_month_mtd_payload,
_i
)
from app.utils.amazon_utils import MTD_COLUMNS, COUNTRY_TO_SELECTED_CURRENCY, DEFAULT_SKU_PRICE_CURRENCY
from app.utils.amazon_utils import AmazonSPAPIClient, amazon_client
from flask import jsonify, request, send_file
from sqlalchemy import create_engine
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

PHORMULA_ENGINE = create_engine(db_url, pool_pre_ping=True)
ADMIN_ENGINE = create_engine(db_url1, pool_pre_ping=True)

if not db_url:
    raise RuntimeError("DATABASE_URL is not set")
if not db_url1:
    # optional: log a warning if using fallback
    print("[WARN] DATABASE_ADMIN_URL not set; falling back to DATABASE_URL")

amazon_api_bp = Blueprint("amazon_api", __name__)




# ------------------------------------------------- Routes -------------------------------------------------
@amazon_api_bp.route("/amazon_api/login", methods=["GET"])
def amazon_login():

    # -------- auth --------
    auth_header = request.headers.get('Authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        return jsonify({'error': 'Authorization token is missing or invalid'}), 401

    token = auth_header.split(' ')[1]
    try:
        payload, user_id, member_id = get_effective_user_id_from_token(token)
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
        payload, user_id, member_id = get_effective_user_id_from_token(token)
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





# -------------------------------------------------------
# 4) Route
# -------------------------------------------------------
@amazon_api_bp.route("/amazon_api/skus", methods=["GET"])
def list_skus():
    auth_header = request.headers.get("Authorization")
    if not auth_header or not auth_header.startswith("Bearer "):
        return jsonify({"error": "Missing Authorization Bearer token"}), 401

    token = auth_header.split(" ")[1]
    try:
        payload, user_id, member_id = get_effective_user_id_from_token(token)
        user_id = payload.get("user_id")
    except jwt.ExpiredSignatureError:
        return jsonify({"error": "Token has expired"}), 401
    except jwt.InvalidTokenError:
        return jsonify({"error": "Invalid token"}), 401

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
        "db": {"saved_products": 0},
        "open_date": {"attempted": False, "updated": 0, "note": None},
    }

    if store_in_db and skus:
        try:
            out["open_date"]["attempted"] = True
            saved_count = _upsert_products_to_db_with_open_date(skus, mp, user_id)
            out["db"] = {"saved_products": saved_count}
            out["open_date"]["updated"] = saved_count
            logger.info(f"Saved {saved_count} products (with open_date) for marketplace {mp}")
        except Exception as e:
            db.session.rollback()
            logger.exception(f"Failed to save products with open_date: {e}")
            out["db"] = {"saved_products": 0, "error": str(e)}
            out["open_date"]["note"] = "Failed while fetching open_date / saving products."

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
        payload, user_id, member_id = get_effective_user_id_from_token(token)
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


# ------------------------------------------------- MTD fetched -------------------------------------------------


# =========================================================
# ROUTE
# =========================================================
@amazon_api_bp.route("/amazon_api/finances/monthly_transactions", methods=["GET"])
def finances_monthly_transactions():
    auth_header = request.headers.get("Authorization")
    if not auth_header or not auth_header.startswith("Bearer "):
        return jsonify({"success": False, "error": "Authorization token is missing or invalid"}), 401

    token = auth_header.split(" ")[1]
    try:
        payload, user_id, member_id = get_effective_user_id_from_token(token)
        user_id = payload["user_id"]
    except jwt.ExpiredSignatureError:
        return jsonify({"success": False, "error": "Token has expired"}), 401
    except jwt.InvalidTokenError:
        return jsonify({"success": False, "error": "Invalid token"}), 401

    now_utc = datetime.now(timezone.utc)
    try:
        year = int(request.args.get("year", now_utc.year))
        month = int(request.args.get("month", 6))
        if month < 1 or month > 12:
            raise ValueError
    except ValueError:
        return jsonify({"success": False, "error": "Invalid year or month"}), 400

    transaction_status = request.args.get("transaction_status", "RELEASED")
    marketplace_id = request.args.get("marketplace_id")
    transaction_type_filter = request.args.get("transaction_type")
    response_format = (request.args.get("format") or "json").lower()

    store_in_db = (request.args.get("store_in_db", "true").lower() != "false")
    run_upload = (request.args.get("run_upload_pipeline", "false").lower() == "true")
    ui_country = (request.args.get("country") or "").strip().lower()
    if not ui_country:
        ui_country = "uk"  # fallback (or map by marketplace_id)


    if run_upload and not ui_country:
        return jsonify({"success": False, "error": "country is required when run_upload_pipeline=true"}), 400

    _apply_region_and_marketplace_from_request()

    au = amazon_user.query.filter_by(user_id=user_id, region=amazon_client.region).first()
    if not au or not au.refresh_token:
        return jsonify({
            "success": False,
            "error": "Amazon account not connected for this region",
            "status": "no_refresh_token",
        }), 400

    amazon_client.refresh_token = au.refresh_token

    posted_after, posted_before = _month_date_range_utc(year, month)

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

        payload_res = res.get("payload") or res
        transactions = payload_res.get("transactions") or []

        for tx in transactions:
            tstatus = (tx or {}).get("transactionStatus")
            ttype = (tx or {}).get("transactionType")

            if tstatus != "RELEASED":
                continue
            if transaction_type_filter and ttype != transaction_type_filter:
                continue

            all_rows.append(_flatten_transaction_to_row(tx or {}))

        next_token = payload_res.get("nextToken")
        if not next_token:
            break
        params = {"nextToken": next_token}

    pipeline_result = None
    if run_upload:
        if not store_in_db:
            pipeline_result = {
                "success": True,
                "skipped": True,
                "message": "run_upload_pipeline skipped because store_in_db=false and pipeline always uses DB."
            }
        else:
            df_in = pd.DataFrame(all_rows) if all_rows else pd.DataFrame()
            try:
                pipeline_result = run_upload_pipeline_from_df(
                    df_raw=df_in,
                    user_id=user_id,
                    country=ui_country,
                    month_num=str(month),
                    year=str(year),
                    db_url=db_url,
                    db_url_aux=db_url1,
                )
            except Exception as e:
                return jsonify({"success": False, "error": f"Upload pipeline failed: {str(e)}"}), 500

            if not pipeline_result or not pipeline_result.get("success"):
                return jsonify({
                    "success": False,
                    "error": "Upload pipeline returned failure",
                    "pipeline_result": pipeline_result,
                }), 400

    if response_format == "excel":
        df = pd.DataFrame(all_rows) if all_rows else pd.DataFrame()
        df = df.reindex(columns=MTD_COLUMNS, fill_value=0.0)

        output = io.BytesIO()
        with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
            df.to_excel(writer, index=False, sheet_name="Transactions")

            if pipeline_result:
                pd.DataFrame([pipeline_result]).to_excel(writer, index=False, sheet_name="PipelineMeta")

        output.seek(0)
        filename = f"finances_transactions_{year}_{month:02d}.xlsx"
        return send_file(
            output,
            as_attachment=True,
            download_name=filename,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

    return jsonify({
        "success": True,
        "year": year,
        "month": month,
        "count": len(all_rows),
        "store_in_db": store_in_db,
        "run_upload_pipeline": run_upload,
        "country": ui_country,
        "pipeline_result": pipeline_result,
        "transactions": all_rows,
    }), 200


@amazon_api_bp.route('/upload', methods=['POST'])
def upload():
    df_in = None
    if 'file' in request.files and request.files['file'].filename:
        f = request.files['file']
        fname = f.filename.lower()
        try:
            if fname.endswith(('.xlsx', '.xls')):
                df_in = pd.read_excel(f)
            elif fname.endswith('.csv'):
                df_in = pd.read_csv(f)
            elif fname.endswith('.json'):
                df_in = pd.read_json(f)
            else:
                return jsonify({"error": "Unsupported file type. Use .xlsx, .xls, .csv, or .json"}), 400
        except Exception as e:
            return jsonify({"error": f"Failed to parse uploaded file: {str(e)}"}), 400
    else:
        payload = request.get_json(silent=True) or {}
        rows = payload.get("rows") or payload.get("data")
        if isinstance(rows, list):
            try:
                df_in = pd.DataFrame(rows)
            except Exception as e:
                return jsonify({"error": f"Could not build dataframe from 'rows': {str(e)}"}), 400

    if df_in is None:
        return jsonify({"error": "Provide a file (xlsx/csv/json) in form-data as 'file' or a JSON body with 'rows' (list of dicts)."}), 400

    def _param(name, alt=None, required=False, caster=lambda x: x):
        if name in request.form:
            raw = request.form.get(name)
        elif alt and alt in request.form:
            raw = request.form.get(alt)
        else:
            payload = request.get_json(silent=True) or {}
            raw = payload.get(name)
            if raw is None and alt:
                raw = payload.get(alt)
        if required and (raw is None or raw == ""):
            raise KeyError(name)
        if raw is None:
            return None
        try:
            return caster(raw)
        except Exception:
            raise ValueError(name)

    try:
        user_id = _param("user_id", required=True)
        ui_country = _param("ui_country", alt="country", required=True, caster=lambda s: str(s).strip())
        raw_month = _param("month_num", alt="month")
        if raw_month is None:
            month_num = datetime.utcnow().month
        else:
            sm = str(raw_month).strip()
            if sm.isdigit():
                month_num = int(sm)
            else:
                month_num = _month_to_num(sm)
        ui_year = _param("ui_year", alt="year", caster=int) or datetime.utcnow().year
        if not (1 <= int(month_num) <= 12):
            return jsonify({"error": "month_num must be between 1 and 12"}), 400
    except KeyError as e:
        return jsonify({"error": f"Missing required field '{e.args[0]}'"}), 400
    except ValueError as e:
        return jsonify({"error": f"Invalid value for '{e.args[0]}'"}), 400

    result = run_upload_pipeline_from_df(
        df_raw=df_in,
        user_id=user_id,
        country=ui_country,
        month_num=int(month_num),
        year=int(ui_year),
        db_url=db_url,
        db_url_aux=db_url1,
    )
    if not result.get("success"):
        return jsonify(result), 400
    return jsonify(result), 200


# ========================================================= Live MTD fetch =========================================================


# ---------------- helpers ----------------
def _safe_ident(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"[^a-z0-9_]+", "_", s)
    return s.strip("_") or "x"


def _build_skuwise_table_name(user_id: int, country: str, month: int, year: int) -> str:
    return f"skuwisemonthly_{int(user_id)}_{_safe_ident(country)}_{int(month)}_{int(year)}"


def _build_adsmonthly_table_name(user_id: int, country: str, month: int, year: int) -> str:
    return f"adsmonthly_{int(user_id)}_{_safe_ident(country)}_{int(month)}_{int(year)}"


def _build_sku_data_table_name(user_id: int) -> str:
    # table: public.sku_{user_id}_data_table
    return f"sku_{int(user_id)}_data_table"


def _country_to_sku_col(country: str) -> str:
    c = (country or "").strip().lower()
    if c in ("uk", "gb", "united_kingdom"):
        return "sku_uk"
    if c in ("us", "usa", "united_states"):
        return "sku_us"
    return "sku_uk"


# @amazon_api_bp.route("/amazon_api/finances/mtd_transactions", methods=["GET"])
# def finances_mtd_transactions():
#     import io
#     import math
#     import re
#     import numpy as np
#     import pandas as pd
#     from datetime import datetime, timezone

#     def _json_safe(obj):
#         """Recursively convert NaN/Inf to None so jsonify returns valid JSON."""
#         if obj is None:
#             return None
#         if isinstance(obj, float):
#             return obj if math.isfinite(obj) else None
#         if isinstance(obj, dict):
#             return {k: _json_safe(v) for k, v in obj.items()}
#         if isinstance(obj, (list, tuple)):
#             return [_json_safe(x) for x in obj]
#         return obj

#     # ---------------- Auth ----------------
#     auth_header = request.headers.get("Authorization")
#     if not auth_header or not auth_header.startswith("Bearer "):
#         return jsonify({"success": False, "error": "Authorization token is missing or invalid"}), 401

#     token = auth_header.split(" ")[1]
#     try:
#         payload, user_id, member_id = get_effective_user_id_from_token(token)
#         user_id = int(payload["user_id"])
#     except jwt.ExpiredSignatureError:
#         return jsonify({"success": False, "error": "Token has expired"}), 401
#     except jwt.InvalidTokenError:
#         return jsonify({"success": False, "error": "Invalid token"}), 401

#     # ---------------- Params ----------------
#     transaction_status = request.args.get("transaction_status", "RELEASED")
#     marketplace_id = request.args.get("marketplace_id")
#     transaction_type_filter = request.args.get("transaction_type")
#     response_format = (request.args.get("format") or "json").lower()
#     store_in_db = (request.args.get("store_in_db", "true").lower() != "false")
#     ui_country = (request.args.get("country") or "").strip().lower() or "uk"

#     # ---------------- Region + marketplace ----------------
#     _apply_region_and_marketplace_from_request()

#     au = amazon_user.query.filter_by(user_id=user_id, region=amazon_client.region).first()
#     if not au or not au.refresh_token:
#         return (
#             jsonify(
#                 {
#                     "success": False,
#                     "error": "Amazon account not connected for this region",
#                     "status": "no_refresh_token",
#                 }
#             ),
#             400,
#         )

#     amazon_client.refresh_token = au.refresh_token

#     now_utc = datetime.now(timezone.utc)
#     posted_after, posted_before = _month_to_date_range_utc_safe(now_utc, safety_minutes=10)

#     # ---------------- Month meta ----------------
#     month_name = _month_name_lower(now_utc.month)  # e.g. "february"

#     # ---------------- COGS meta ----------------
#     user_currency = DEFAULT_SKU_PRICE_CURRENCY
#     selected_currency = COUNTRY_TO_SELECTED_CURRENCY.get(ui_country, user_currency)

#     sku_price_map = fetch_sku_price_map(user_id=user_id, country=ui_country)
#     conversion_rate_fx = fetch_conversion_rate(
#         country=ui_country,
#         year=now_utc.year,
#         month_name=month_name,
#         user_currency=user_currency,
#         selected_currency=selected_currency,
#     )

#     # ---------------- Fetch MTD ----------------
#     params = {
#         "postedAfter": posted_after,
#         "postedBefore": posted_before,
#         "marketplaceId": marketplace_id or amazon_client.marketplace_id,
#     }
#     if transaction_status:
#         params["transactionStatus"] = transaction_status

#     all_rows = []

#     while True:
#         res = amazon_client.make_api_call(
#             "/finances/2024-06-19/transactions",
#             method="GET",
#             params=params,
#         )
#         if not res or "error" in res:
#             return jsonify({"success": False, "error": res or {"error": "Unknown SP-API error"}}), 502

#         payload_res = res.get("payload") or res
#         transactions = payload_res.get("transactions") or []

#         for tx in transactions:
#             tstatus = (tx or {}).get("transactionStatus")
#             ttype = (tx or {}).get("transactionType")

#             if tstatus != "RELEASED":
#                 continue
#             if transaction_type_filter and ttype != transaction_type_filter:
#                 continue

#             row = _flatten_transaction_to_row(tx or {})

#             sku = (row.get("sku") or "").strip()
#             qty = _i(row.get("quantity")) or 0
#             price = sku_price_map.get(sku) if sku else None
#             row["cogs"] = (
#                 float(qty) * float(price) * float(conversion_rate_fx)
#                 if (price is not None and qty > 0)
#                 else 0.0
#             )

#             all_rows.append(row)

#         next_token = payload_res.get("nextToken")
#         if not next_token:
#             break
#         params = {"nextToken": next_token}

#     # ✅ profit per row
#     add_profit_column_from_uk_profit(all_rows, country=ui_country)

#     # ✅ gross_sales per row
#     for r in all_rows:
#         r["gross_sales"] = (
#             float(r.get("product_sales", 0.0))
#             + float(r.get("product_sales_tax", 0.0))
#             + float(r.get("postage_credits", 0.0))
#             + float(r.get("gift_wrap_credits", 0.0))
#             + float(r.get("shipping_credits_tax", 0.0))
#             + float(r.get("giftwrap_credits_tax", 0.0))
#             - float(r.get("promotional_rebates", 0.0))
#             - float(r.get("promotional_rebates_tax", 0.0))
#         )

#     # ---------------- Store raw liveorders ----------------
#     db_result = None
#     if store_in_db:
#         try:
#             db_result = upsert_liveorders_from_rows(
#                 all_rows,
#                 user_id=user_id,
#                 country=ui_country,
#                 now_utc=now_utc,
#             )
#         except Exception as e:
#             db.session.rollback()
#             return jsonify({"success": False, "error": f"DB store failed: {str(e)}"}), 500

#     # ---------------- totals ----------------
#     totals = compute_totals(all_rows)

#     tax_and_credits_total = (
#         float(totals.get("postage_credits", 0.0))
#         + float(totals.get("gift_wrap_credits", 0.0))
#         + float(totals.get("product_sales_tax", 0.0))
#         + float(totals.get("shipping_credits_tax", 0.0))
#         + float(totals.get("promotional_rebates_tax", 0.0))
#         + float(totals.get("marketplace_facilitator_tax", 0.0))
#     )
#     totals["tax_and_credits"] = round(tax_and_credits_total, 2)

#     selling_fees = float(totals.get("selling_fees", 0.0))
#     fba_fees = float(totals.get("fba_fees", 0.0))
#     amazon_fees = abs(selling_fees) + abs(fba_fees)

#     gross_sales_total = float(totals.get("gross_sales", 0.0))
#     net_sales = float(totals.get("product_sales", 0.0)) + float(totals.get("promotional_rebates", 0.0))
#     qty_total = float(totals.get("quantity", 0.0)) or 0.0
#     asp = (net_sales / qty_total) if qty_total else 0.0
#     profit_total = float(totals.get("profit", 0.0))

#     # ✅ ADD this NEW block in your "platform + advertising fees (dashboard)" section
#     # right where you are already calculating:
#     # platformfeenew_total, platform_fee_inventory_storage_total, lost_total_df

#     # ---------------- platform + advertising fees (dashboard) ----------------
#     df_all = pd.DataFrame(all_rows) if all_rows else pd.DataFrame()

#     platform_fee_total = 0.0
#     advertising_fee_total = 0.0

#     # ✅ NEW totals you store in SKU-wise table
#     platformfeenew_total = 0.0
#     platform_fee_inventory_storage_total = 0.0
#     dealsvouchar_ads_total = 0.0  # ✅ NEW
#     lost_total_df = pd.DataFrame(columns=["sku", "lost_total"])

#     if not df_all.empty:
#         for col, default in [
#             ("description", ""),
#             ("total", 0.0),
#             ("platform_fees", 0.0),
#             ("advertising_cost", 0.0),
#             ("sku", ""),
#             ("type", ""),
#         ]:
#             if col not in df_all.columns:
#                 df_all[col] = default

#         # existing dashboard fees
#         platform_fee_total, _, _ = uk_platform_fee(df_all, country=ui_country, want_breakdown=False)
#         advertising_fee_total, _, _ = uk_advertising(df_all, country=ui_country, want_breakdown=False)

#         desc_all = df_all["description"].fillna("").astype(str)

#         def sum_total_where_desc_contains(keywords):
#             """
#             keywords: list[str]
#             Returns sum of df_all['total'] where description contains any keyword (case-insensitive)
#             """
#             if "total" not in df_all.columns:
#                 return 0.0
#             pattern = "|".join([re.escape(k) for k in keywords])
#             mask = desc_all.str.contains(pattern, case=False, na=False, regex=True)
#             return float(pd.to_numeric(df_all.loc[mask, "total"], errors="coerce").fillna(0.0).sum())

#         # platformfeenew = sum(total) where description contains "Subscription"
#         platformfeenew_total = sum_total_where_desc_contains(["Subscription"])

#         # platform_fee_inventory_storage = sum(total) for these keywords
#         platform_fee_inventory_storage_total = sum_total_where_desc_contains(
#             [
#                 "FBA Return Fee",
#                 "FBA Long-Term Storage Fee",
#                 "FBA storage fee",
#                 "FBADisposal",
#                 "FBAStorageBilling",
#                 "FBALongTermStorageBilling",
#                 "INCORRECT_FEES_NON_ITEMIZED",
#                 "StorageReservationBilling",
#             ]
#         )

#         # ✅ NEW: dealsvouchar_ads = sum(total) for these keywords
#         dealsvouchar_ads_total = sum_total_where_desc_contains(
#             [
#                 "Cost of Advertising",
#                 "Coupon Redemption Fee",
#                 "Deals",
#                 "Lightning Deal",
#                 "CouponPerformanceEvent",
#                 "CouponParticipationEvent",
#                 "SellerDealComplete",
#                 "VineCharge",
#                 "SellerPoweredCoupon",
#                 "DealParticipationEvent",
#                 "DealPerformanceEvent",
#             ]
#         )

#         # lost_total per SKU
#         LOST_DESCRIPTIONS = {
#             "REVERSAL_REIMBURSEMENT",
#             "WAREHOUSE_LOST",
#             "WAREHOUSE_DAMAGE",
#             "MISSING_FROM_INBOUND",
#             "MISSING_FROM_INBOUND_CLAWBACK",
#             "COMPENSATED_CLAWBACK",
#         }
#         lost_mask = df_all["description"].fillna("").astype(str).str.strip().isin(LOST_DESCRIPTIONS)

#         tmp = df_all.loc[lost_mask, ["sku", "total"]].copy()
#         tmp["sku"] = tmp["sku"].fillna("").astype(str).str.strip()
#         tmp["total"] = pd.to_numeric(tmp["total"], errors="coerce").fillna(0.0)

#         lost_total_df = (
#             tmp[tmp["sku"] != ""]
#             .groupby("sku", as_index=False)["total"]
#             .sum()
#             .rename(columns={"total": "lost_total"})
#         )
#         lost_total_df["lost_total"] = pd.to_numeric(lost_total_df["lost_total"], errors="coerce").fillna(0.0)


#     platform_fee_total = float(platform_fee_total or 0.0)
#     advertising_fee_total = float(advertising_fee_total or 0.0)

#     cm2_profit_dashboard = profit_total - advertising_fee_total - platform_fee_total
#     profit_percentage = (cm2_profit_dashboard / net_sales * 100) if net_sales else 0.0
#     current_net_reimbursement = compute_net_reimbursement_from_df(df_all) if not df_all.empty else 0.0

#     derived_totals = {
#         "amazon_fees": round(amazon_fees, 2),
#         "platform_fee": round(platform_fee_total, 2),
#         "advertising_fees": round(advertising_fee_total, 2),
#         "net_sales": round(net_sales, 2),
#         "gross_sales": round(gross_sales_total, 2),
#         "asp": round(asp, 2),
#         "profit": round(profit_total, 2),
#         "cm2_profit": round(cm2_profit_dashboard, 2),
#         "profit_percentage": round(profit_percentage, 2),
#         "current_net_reimbursement": round(float(current_net_reimbursement or 0.0), 2),
#     }

#     previous_period = get_previous_month_mtd_payload(user_id=user_id, country=ui_country, now_utc=now_utc)

#     # ============================================================
#     # SKU-WISE TABLE
#     # ============================================================
#     skuwise_table_name = f"skuwisemonthly_{int(user_id)}_{_safe_ident(ui_country)}_{_safe_ident(month_name)}_{int(now_utc.year)}"
#     ads_table_name = _build_adsmonthly_table_name(user_id, ui_country, now_utc.month, now_utc.year)

#     sku_summary_saved = False
#     sku_summary_rows = 0
#     skuwise_items = []

#     if not df_all.empty:
#         df_all["sku"] = df_all.get("sku", "").fillna("").astype(str).str.strip()
#         df_skus = df_all[df_all["sku"] != ""].copy()

#         preferred_sum_cols = [
#             "quantity", "product_sales", "product_sales_tax", "postage_credits", "gift_wrap_credits",
#             "shipping_credits_tax", "giftwrap_credits_tax", "promotional_rebates", "promotional_rebates_tax",
#             "marketplace_facilitator_tax", "selling_fees", "fba_fees", "other", "gross_sales", "cogs", "profit",
#         ]
#         sum_cols = [c for c in preferred_sum_cols if c in df_skus.columns]
#         for c in sum_cols:
#             df_skus[c] = pd.to_numeric(df_skus[c], errors="coerce").fillna(0.0)

#         df_sku = df_skus.groupby("sku", as_index=False)[sum_cols].sum()

#         # ✅ merge lost_total per sku
#         if lost_total_df is not None and not lost_total_df.empty:
#             df_sku = df_sku.merge(lost_total_df, on="sku", how="left")
#         df_sku["lost_total"] = pd.to_numeric(df_sku.get("lost_total", 0.0), errors="coerce").fillna(0.0)

#         # finance derived
#         df_sku["net_sales"] = pd.to_numeric(df_sku.get("product_sales", 0.0), errors="coerce").fillna(0.0) + pd.to_numeric(
#             df_sku.get("promotional_rebates", 0.0), errors="coerce"
#         ).fillna(0.0)

#         df_sku["quantity"] = pd.to_numeric(df_sku.get("quantity", 0.0), errors="coerce").fillna(0.0)
#         df_sku["asp"] = df_sku.apply(
#             lambda r: (float(r["net_sales"]) / float(r["quantity"])) if float(r["quantity"]) else 0.0,
#             axis=1,
#         )

#         def _col(df: pd.DataFrame, name: str) -> pd.Series:
#             return pd.to_numeric(df[name], errors="coerce").fillna(0.0) if name in df.columns else pd.Series([0.0] * len(df), index=df.index)

#         df_sku["credits"] = (_col(df_sku, "postage_credits") + _col(df_sku, "gift_wrap_credits")).fillna(0.0)
#         df_sku["tax"] = (
#             _col(df_sku, "product_sales_tax")
#             + _col(df_sku, "shipping_credits_tax")
#             + _col(df_sku, "giftwrap_credits_tax")
#             + _col(df_sku, "promotional_rebates_tax")
#             + _col(df_sku, "marketplace_facilitator_tax")
#         ).fillna(0.0)
#         df_sku["tax_and_credits"] = (df_sku["credits"] - df_sku["tax"].abs()).round(2)

#         # -------- ADS merge --------
#         ads_total_product_spend = 0.0
#         ads_total_display_spend = 0.0
#         ads_total_brand_spend = 0.0
#         ads_agg = pd.DataFrame()

#         try:
#             sql = f'''
#                 SELECT
#                     products,
#                     ad_type,
#                     impressions,
#                     clicks,
#                     spend,
#                     sale_units,
#                     sale_amount,
#                     product_spend,
#                     display_spend,
#                     brand_spend
#                 FROM public."{ads_table_name}"
#             '''
#             ads_df = pd.read_sql_query(sql, PHORMULA_ENGINE)
#             ads_df["products"] = ads_df["products"].fillna("").astype(str).str.strip()

#             gt_mask = ads_df["products"].str.lower().eq("grand total")
#             if gt_mask.any():
#                 ads_total_product_spend = float(pd.to_numeric(ads_df.loc[gt_mask, "product_spend"], errors="coerce").fillna(0.0).sum()) if "product_spend" in ads_df.columns else 0.0
#                 ads_total_display_spend = float(pd.to_numeric(ads_df.loc[gt_mask, "display_spend"], errors="coerce").fillna(0.0).sum()) if "display_spend" in ads_df.columns else 0.0
#                 ads_total_brand_spend = float(pd.to_numeric(ads_df.loc[gt_mask, "brand_spend"], errors="coerce").fillna(0.0).sum()) if "brand_spend" in ads_df.columns else 0.0
#             else:
#                 ads_total_product_spend = float(pd.to_numeric(ads_df.get("product_spend", 0), errors="coerce").fillna(0.0).sum())
#                 ads_total_display_spend = float(pd.to_numeric(ads_df.get("display_spend", 0), errors="coerce").fillna(0.0).sum())
#                 ads_total_brand_spend = float(pd.to_numeric(ads_df.get("brand_spend", 0), errors="coerce").fillna(0.0).sum())

#             ads_df = ads_df[ads_df["products"] != ""].copy()
#             ads_df["ad_type"] = ads_df.get("ad_type", "").fillna("").astype(str).str.strip()

#             for col in ["impressions","clicks","spend","sale_units","sale_amount","product_spend","display_spend","brand_spend"]:
#                 if col not in ads_df.columns:
#                     ads_df[col] = 0.0
#                 ads_df[col] = pd.to_numeric(ads_df[col], errors="coerce").fillna(0.0)

#             ads_num = ads_df.groupby("products", as_index=False)[
#                 ["impressions","clicks","spend","sale_units","sale_amount","product_spend","display_spend","brand_spend"]
#             ].sum()

#             ads_type = (
#                 ads_df[ads_df["ad_type"] != ""]
#                 .groupby("products")["ad_type"]
#                 .apply(lambda s: ", ".join(sorted(set([str(x).strip() for x in s if str(x).strip()]))))
#                 .reset_index()
#             )

#             ads_agg = ads_num.merge(ads_type, on="products", how="left")
#             ads_agg["ad_type"] = ads_agg["ad_type"].fillna("")

#             ads_agg.rename(
#                 columns={
#                     "impressions": "ads_impressions",
#                     "clicks": "ads_clicks",
#                     "sale_units": "ads_sale_units",
#                     "sale_amount": "ads_sale_amount",
#                 },
#                 inplace=True,
#             )
#             if "spend" in ads_agg.columns:
#                 ads_agg.rename(columns={"spend": "ads_spend_raw"}, inplace=True)

#         except Exception as e:
#             logger.warning(f"Could not read/aggregate ads table {ads_table_name}: {e}")
#             ads_agg = pd.DataFrame()

#         if not ads_agg.empty:
#             df_sku = (
#                 df_sku.merge(ads_agg, how="left", left_on="sku", right_on="products")
#                 .drop(columns=["products"], errors="ignore")
#             )

#         # df_sku["ad_type"] = df_sku.get("ad_type", "").fillna("").astype(str)
#         if "ad_type" not in df_sku.columns:
#             df_sku["ad_type"] = ""
#         else:
#             df_sku["ad_type"] = df_sku["ad_type"].fillna("").astype(str)


#         for col in ["product_spend", "display_spend", "brand_spend"]:
#             if col not in df_sku.columns:
#                 df_sku[col] = 0.0
#             df_sku[col] = pd.to_numeric(df_sku[col], errors="coerce").fillna(0.0)


#         # ✅ ads_spend = product + display + brand
#         df_sku["ads_spend"] = (df_sku["product_spend"] + df_sku["display_spend"]).fillna(0.0)

#         for col in ["ads_impressions", "ads_clicks", "ads_sale_units", "ads_sale_amount"]:
#             if col not in df_sku.columns:
#                 df_sku[col] = 0.0
#             df_sku[col] = pd.to_numeric(df_sku[col], errors="coerce").fillna(0.0)


#         # ✅ NEW: total-only breakup columns (0 for SKU rows)
#         df_sku["platform_fee_inventory_storage"] = 0.0
#         df_sku["platformfeenew"] = 0.0
#         df_sku["dealsvouchar_ads"] = 0.0  # ✅ NEW

#         # ✅ platform_fee per row using your formula
#         # SKU rows: platform_fee = 0 + 0 - lost_total  => negative lost_total
#         df_sku["platform_fee"] = (
#             pd.to_numeric(df_sku["platform_fee_inventory_storage"], errors="coerce").fillna(0.0)
#             + pd.to_numeric(df_sku["platformfeenew"], errors="coerce").fillna(0.0)
#             - pd.to_numeric(df_sku["lost_total"], errors="coerce").fillna(0.0)
#         )

#         # cm2 / metrics
#         if "profit" not in df_sku.columns:
#             df_sku["profit"] = 0.0
#         df_sku["profit"] = pd.to_numeric(df_sku["profit"], errors="coerce").fillna(0.0)

#         df_sku["cm2_profit"] = (df_sku["profit"] - df_sku["ads_spend"]).fillna(0.0)

#         df_sku["cm1_profit_per_unit"] = df_sku.apply(
#             lambda r: (float(r["profit"]) / float(r["quantity"])) if float(r["quantity"]) else 0.0,
#             axis=1,
#         )
#         df_sku["cm1_profit_per"] = df_sku.apply(
#             lambda r: (float(r["profit"]) / float(r["net_sales"]) * 100.0) if float(r["net_sales"]) else 0.0,
#             axis=1,
#         )
#         df_sku["cm2_profit_per_unit"] = df_sku.apply(
#             lambda r: (float(r["cm2_profit"]) / float(r["quantity"])) if float(r["quantity"]) else 0.0,
#             axis=1,
#         )
#         df_sku["cm2_profit_per"] = df_sku.apply(
#             lambda r: (float(r["cm2_profit"]) / float(r["net_sales"]) * 100.0) if float(r["net_sales"]) else 0.0,
#             axis=1,
#         )

#         for col in ["cm2_profit","cm1_profit_per_unit","cm1_profit_per","cm2_profit_per_unit","cm2_profit_per"]:
#             df_sku[col] = pd.to_numeric(df_sku[col], errors="coerce").fillna(0.0)

#         # product_name mapping
#         df_sku["product_name"] = ""
#         sku_data_table = _build_sku_data_table_name(user_id)
#         sku_col = _country_to_sku_col(ui_country)
#         try:
#             map_sql = f'SELECT product_name, "{sku_col}" AS sku_key FROM public."{sku_data_table}"'
#             map_df = pd.read_sql_query(map_sql, PHORMULA_ENGINE)
#             if not map_df.empty:
#                 map_df["sku_key"] = map_df["sku_key"].fillna("").astype(str).str.strip()
#                 map_df["product_name"] = map_df["product_name"].fillna("").astype(str).str.strip()
#                 map_df = (
#                     map_df[map_df["sku_key"] != ""]
#                     .sort_values(by=["sku_key"])
#                     .drop_duplicates(subset=["sku_key"], keep="first")
#                 )
#                 df_sku = df_sku.merge(map_df, how="left", left_on="sku", right_on="sku_key").drop(columns=["sku_key"], errors="ignore")
#                 if "product_name_y" in df_sku.columns and "product_name_x" in df_sku.columns:
#                     df_sku["product_name"] = df_sku["product_name_y"].fillna(df_sku["product_name_x"]).fillna("")
#                     df_sku.drop(columns=["product_name_x","product_name_y"], inplace=True, errors="ignore")
#                 elif "product_name_y" in df_sku.columns:
#                     df_sku.rename(columns={"product_name_y": "product_name"}, inplace=True)
#                 df_sku["product_name"] = df_sku["product_name"].fillna("").astype(str)
#         except Exception as e:
#             logger.warning(f"Could not read/map product_name from {sku_data_table}: {e}")
#             df_sku["product_name"] = df_sku.get("product_name", "").fillna("").astype(str)

#         # meta
#         df_sku["user_id"] = int(user_id)
#         df_sku["country"] = ui_country
#         df_sku["month"] = _safe_ident(month_name)
#         df_sku["year"] = int(now_utc.year)
#         df_sku["generated_at_utc"] = now_utc.isoformat()

#         # -------- GRAND TOTAL row --------
#         total_row = {"sku": "GRAND_TOTAL", "product_name": "Grand Total"}

#         for c in sum_cols:
#             total_row[c] = float(df_sku[c].sum()) if c in df_sku.columns else 0.0

#         total_row["lost_total"] = float(pd.to_numeric(df_sku.get("lost_total", 0.0), errors="coerce").fillna(0.0).sum())

#         total_row["net_sales"] = float(df_sku["net_sales"].sum())
#         total_qty = float(df_sku["quantity"].sum()) or 0.0
#         total_row["asp"] = (total_row["net_sales"] / total_qty) if total_qty else 0.0

#         total_row["ads_impressions"] = float(df_sku["ads_impressions"].sum())
#         total_row["ads_clicks"] = float(df_sku["ads_clicks"].sum())
#         total_row["ads_sale_units"] = float(df_sku["ads_sale_units"].sum())
#         total_row["ads_sale_amount"] = float(df_sku["ads_sale_amount"].sum())

#         total_row["product_spend"] = round(float(ads_total_product_spend or 0.0), 2)
#         total_row["display_spend"] = round(float(ads_total_display_spend or 0.0), 2)
#         total_row["brand_spend"] = round(float(ads_total_brand_spend or 0.0), 2)
#         total_row["ads_spend"] = round(total_row["product_spend"] + total_row["display_spend"] , 2)

#         # ✅ store totals
#         total_row["platform_fee_inventory_storage"] = round(float(platform_fee_inventory_storage_total or 0.0), 2)
#         total_row["platformfeenew"] = round(float(platformfeenew_total or 0.0), 2)
#         total_row["dealsvouchar_ads"] = round(float(dealsvouchar_ads_total or 0.0), 2)  # ✅ NEW

#         # ✅ platform_fee = platform_fee_inventory_storage + platformfeenew - lost_total
#         total_row["platform_fee"] = round(
#             float(total_row["platform_fee_inventory_storage"]) + float(total_row["platformfeenew"]) - float(total_row["lost_total"]),
#             2,
#         )

#         total_row["ad_type"] = "All"

#         g_clicks = float(total_row["ads_clicks"])
#         g_spend = float(total_row["ads_spend"])
#         g_units = float(total_row["ads_sale_units"])
#         g_sales = float(total_row["ads_sale_amount"])

#         total_row["ads_conversion_rate"] = (g_units / g_clicks * 100.0) if g_clicks else 0.0
#         total_row["ads_roas"] = (g_sales / g_spend) if g_spend else 0.0
#         total_row["ads_acos"] = (g_spend / g_sales * 100.0) if g_sales else 0.0

#         g_profit = float(total_row.get("profit", 0.0))
#         g_net_sales = float(total_row.get("net_sales", 0.0))

#         total_row["cm2_profit"] = g_profit - g_spend
#         total_row["cm1_profit_per_unit"] = (g_profit / total_qty) if total_qty else 0.0
#         total_row["cm1_profit_per"] = (g_profit / g_net_sales * 100.0) if g_net_sales else 0.0
#         total_row["cm2_profit_per_unit"] = (total_row["cm2_profit"] / total_qty) if total_qty else 0.0
#         total_row["cm2_profit_per"] = (total_row["cm2_profit"] / g_net_sales * 100.0) if g_net_sales else 0.0

#         total_row["credits"] = float(df_sku["credits"].sum()) if "credits" in df_sku.columns else 0.0
#         total_row["tax"] = float(df_sku["tax"].sum()) if "tax" in df_sku.columns else 0.0
#         total_row["tax_and_credits"] = round(float(total_row["credits"]) - abs(float(total_row["tax"])), 2)

#         total_row["user_id"] = int(user_id)
#         total_row["country"] = ui_country
#         total_row["month"] = _safe_ident(month_name)
#         total_row["year"] = int(now_utc.year)
#         total_row["generated_at_utc"] = now_utc.isoformat()

#         df_sku = pd.concat([df_sku, pd.DataFrame([total_row])], ignore_index=True)

#         # ✅ IMPORTANT: replace NaN/Inf before jsonify/to_dict
#         df_sku = df_sku.replace([np.inf, -np.inf], np.nan).where(pd.notnull(df_sku), None)

#         skuwise_items = df_sku.to_dict(orient="records")

#         # store
#         try:
#             df_sku.to_sql(
#                 skuwise_table_name,
#                 PHORMULA_ENGINE,
#                 schema="public",
#                 if_exists="replace",
#                 index=False,
#                 method="multi",
#                 chunksize=1000,
#             )
#             sku_summary_saved = True
#             sku_summary_rows = int(len(df_sku))
#         except Exception as e:
#             logger.exception(f"Failed to store SKU-wise table {skuwise_table_name}: {e}")
#             sku_summary_saved = False

#     # ---------------- Excel response ----------------
#     if response_format == "excel":
#         df = pd.DataFrame(all_rows) if all_rows else pd.DataFrame()
#         df = df.reindex(columns=MTD_COLUMNS + ["cogs", "profit", "gross_sales"], fill_value=0.0)

#         output = io.BytesIO()
#         with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
#             df.to_excel(writer, index=False, sheet_name="Transactions")
#             pd.DataFrame([totals]).to_excel(writer, index=False, sheet_name="Totals")
#             pd.DataFrame([derived_totals]).to_excel(writer, index=False, sheet_name="DerivedTotals")
#             pd.DataFrame([previous_period]).to_excel(writer, index=False, sheet_name="PrevPeriodMeta")
#             if db_result:
#                 pd.DataFrame([db_result]).to_excel(writer, index=False, sheet_name="DBMeta")
#             if skuwise_items:
#                 pd.DataFrame(skuwise_items).to_excel(writer, index=False, sheet_name="SKUWiseMonthly")

#         output.seek(0)
#         filename = f"finances_transactions_MTD_{now_utc.year}_{now_utc.month:02d}.xlsx"
#         return send_file(
#             output,
#             as_attachment=True,
#             download_name=filename,
#             mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
#         )

#     # ---------------- JSON response (sanitized) ----------------
#     payload_out = {
#         "success": True,
#         "posted_after": posted_after,
#         "posted_before": posted_before,
#         "count": len(all_rows),
#         "stored": bool(store_in_db),
#         "db_result": db_result,
#         "cogs_meta": {
#             "country": ui_country,
#             "month": month_name,
#             "year": now_utc.year,
#             "pair": f"{user_currency}->{selected_currency}",
#             "conversion_rate": conversion_rate_fx,
#         },
#         "totals": totals,
#         "derived_totals": derived_totals,
#         "previous_period": previous_period,
#         "skuwise_table": {
#             "name": skuwise_table_name,
#             "saved": sku_summary_saved,
#             "rows": sku_summary_rows,
#         },
#         "skuwise_items": skuwise_items,
#         "transactions": all_rows,
#     }

#     return jsonify(_json_safe(payload_out)), 200


@amazon_api_bp.route("/amazon_api/finances/mtd_transactions", methods=["GET"])
def finances_mtd_transactions():
    import io
    import math
    import re
    import numpy as np
    import pandas as pd
    from datetime import datetime, timezone

    def _json_safe(obj):
        """Recursively convert NaN/Inf to None so jsonify returns valid JSON."""
        if obj is None:
            return None
        if isinstance(obj, float):
            return obj if math.isfinite(obj) else None
        if isinstance(obj, dict):
            return {k: _json_safe(v) for k, v in obj.items()}
        if isinstance(obj, (list, tuple)):
            return [_json_safe(x) for x in obj]
        return obj

    # ---------------- Auth ----------------
    auth_header = request.headers.get("Authorization")
    if not auth_header or not auth_header.startswith("Bearer "):
        return jsonify({"success": False, "error": "Authorization token is missing or invalid"}), 401

    token = auth_header.split(" ")[1]
    try:
        payload, user_id, member_id = get_effective_user_id_from_token(token)
        user_id = int(payload["user_id"])
    except jwt.ExpiredSignatureError:
        return jsonify({"success": False, "error": "Token has expired"}), 401
    except jwt.InvalidTokenError:
        return jsonify({"success": False, "error": "Invalid token"}), 401

    # ---------------- Params ----------------
    transaction_status = request.args.get("transaction_status", "RELEASED")
    marketplace_id = request.args.get("marketplace_id")
    transaction_type_filter = request.args.get("transaction_type")
    response_format = (request.args.get("format") or "json").lower()
    store_in_db = (request.args.get("store_in_db", "true").lower() != "false")
    ui_country = (request.args.get("country") or "").strip().lower() or "uk"

    # ---------------- Region + marketplace ----------------
    _apply_region_and_marketplace_from_request()

    au = amazon_user.query.filter_by(user_id=user_id, region=amazon_client.region).first()
    if not au or not au.refresh_token:
        return (
            jsonify(
                {
                    "success": False,
                    "error": "Amazon account not connected for this region",
                    "status": "no_refresh_token",
                }
            ),
            400,
        )

    amazon_client.refresh_token = au.refresh_token

    now_utc = datetime.now(timezone.utc)
    posted_after, posted_before = _month_to_date_range_utc_safe(now_utc, safety_minutes=10)

    # ---------------- Month meta ----------------
    month_name = _month_name_lower(now_utc.month)  # e.g. "february"

    # ---------------- COGS meta ----------------
    user_currency = DEFAULT_SKU_PRICE_CURRENCY
    selected_currency = COUNTRY_TO_SELECTED_CURRENCY.get(ui_country, user_currency)

    sku_price_map = fetch_sku_price_map(user_id=user_id, country=ui_country)
    conversion_rate_fx = fetch_conversion_rate(
        country=ui_country,
        year=now_utc.year,
        month_name=month_name,
        user_currency=user_currency,
        selected_currency=selected_currency,
    )

    # ---------------- Fetch MTD ----------------
    params = {
        "postedAfter": posted_after,
        "postedBefore": posted_before,
        "marketplaceId": marketplace_id or amazon_client.marketplace_id,
    }
    if transaction_status:
        params["transactionStatus"] = transaction_status

    all_rows = []

    while True:
        res = amazon_client.make_api_call(
            "/finances/2024-06-19/transactions",
            method="GET",
            params=params,
        )
        if not res or "error" in res:
            return jsonify({"success": False, "error": res or {"error": "Unknown SP-API error"}}), 502

        payload_res = res.get("payload") or res
        transactions = payload_res.get("transactions") or []

        for tx in transactions:
            tstatus = (tx or {}).get("transactionStatus")
            ttype = (tx or {}).get("transactionType")

            if tstatus != "RELEASED":
                continue
            if transaction_type_filter and ttype != transaction_type_filter:
                continue

            row = _flatten_transaction_to_row(tx or {})

            sku = (row.get("sku") or "").strip()
            qty = _i(row.get("quantity")) or 0
            price = sku_price_map.get(sku) if sku else None
            row["cogs"] = (
                float(qty) * float(price) * float(conversion_rate_fx)
                if (price is not None and qty > 0)
                else 0.0
            )

            all_rows.append(row)

        next_token = payload_res.get("nextToken")
        if not next_token:
            break
        params = {"nextToken": next_token}

    # ✅ profit per row
    add_profit_column_from_uk_profit(all_rows, country=ui_country)

    # ✅ gross_sales per row
    for r in all_rows:
        r["gross_sales"] = (
            float(r.get("product_sales", 0.0))
            + float(r.get("product_sales_tax", 0.0))
            + float(r.get("postage_credits", 0.0))
            + float(r.get("gift_wrap_credits", 0.0))
            + float(r.get("shipping_credits_tax", 0.0))
            + float(r.get("giftwrap_credits_tax", 0.0))
            - float(r.get("promotional_rebates", 0.0))
            - float(r.get("promotional_rebates_tax", 0.0))
        )

    # ---------------- Store raw liveorders ----------------
    db_result = None
    if store_in_db:
        try:
            db_result = upsert_liveorders_from_rows(
                all_rows,
                user_id=user_id,
                country=ui_country,
                now_utc=now_utc,
            )
        except Exception as e:
            db.session.rollback()
            return jsonify({"success": False, "error": f"DB store failed: {str(e)}"}), 500

    # ---------------- totals ----------------
    totals = compute_totals(all_rows)

    tax_and_credits_total = (
        float(totals.get("postage_credits", 0.0))
        + float(totals.get("gift_wrap_credits", 0.0))
        + float(totals.get("product_sales_tax", 0.0))
        + float(totals.get("shipping_credits_tax", 0.0))
        + float(totals.get("promotional_rebates_tax", 0.0))
        + float(totals.get("marketplace_facilitator_tax", 0.0))
    )
    totals["tax_and_credits"] = round(tax_and_credits_total, 2)

    selling_fees = float(totals.get("selling_fees", 0.0))
    fba_fees = float(totals.get("fba_fees", 0.0))
    amazon_fees = abs(selling_fees) + abs(fba_fees)

    gross_sales_total = float(totals.get("gross_sales", 0.0))
    net_sales = float(totals.get("product_sales", 0.0)) + float(totals.get("promotional_rebates", 0.0))
    qty_total = float(totals.get("quantity", 0.0)) or 0.0
    asp = (net_sales / qty_total) if qty_total else 0.0
    profit_total = float(totals.get("profit", 0.0))

    # ---------------- platform + advertising fees (dashboard) ----------------
    df_all = pd.DataFrame(all_rows) if all_rows else pd.DataFrame()

    platform_fee_total = 0.0
    advertising_fee_total = 0.0

    # totals stored in SKU-wise table
    platformfeenew_total = 0.0
    platform_fee_inventory_storage_total = 0.0
    dealsvouchar_ads_total = 0.0
    lost_total_df = pd.DataFrame(columns=["sku", "lost_total"])

    if not df_all.empty:
        for col, default in [
            ("description", ""),
            ("total", 0.0),
            ("platform_fees", 0.0),
            ("advertising_cost", 0.0),
            ("sku", ""),
            ("type", ""),
        ]:
            if col not in df_all.columns:
                df_all[col] = default

        platform_fee_total, _, _ = uk_platform_fee(df_all, country=ui_country, want_breakdown=False)
        advertising_fee_total, _, _ = uk_advertising(df_all, country=ui_country, want_breakdown=False)

        desc_all = df_all["description"].fillna("").astype(str)

        def sum_total_where_desc_contains(keywords):
            if "total" not in df_all.columns:
                return 0.0
            pattern = "|".join([re.escape(k) for k in keywords])
            mask = desc_all.str.contains(pattern, case=False, na=False, regex=True)
            return float(pd.to_numeric(df_all.loc[mask, "total"], errors="coerce").fillna(0.0).sum())

        platformfeenew_total = sum_total_where_desc_contains(["Subscription"])

        platform_fee_inventory_storage_total = sum_total_where_desc_contains(
            [
                "FBA Return Fee",
                "FBA Long-Term Storage Fee",
                "FBA storage fee",
                "FBADisposal",
                "FBAStorageBilling",
                "FBALongTermStorageBilling",
                "INCORRECT_FEES_NON_ITEMIZED",
                "StorageReservationBilling",
            ]
        )

        dealsvouchar_ads_total = sum_total_where_desc_contains(
            [
                "Cost of Advertising",
                "Coupon Redemption Fee",
                "Deals",
                "Lightning Deal",
                "CouponPerformanceEvent",
                "CouponParticipationEvent",
                "SellerDealComplete",
                "VineCharge",
                "SellerPoweredCoupon",
                "DealParticipationEvent",
                "DealPerformanceEvent",
            ]
        )

        LOST_DESCRIPTIONS = {
            "REVERSAL_REIMBURSEMENT",
            "WAREHOUSE_LOST",
            "WAREHOUSE_DAMAGE",
            "MISSING_FROM_INBOUND",
            "MISSING_FROM_INBOUND_CLAWBACK",
            "COMPENSATED_CLAWBACK",
        }
        lost_mask = df_all["description"].fillna("").astype(str).str.strip().isin(LOST_DESCRIPTIONS)

        tmp = df_all.loc[lost_mask, ["sku", "total"]].copy()
        tmp["sku"] = tmp["sku"].fillna("").astype(str).str.strip()
        tmp["total"] = pd.to_numeric(tmp["total"], errors="coerce").fillna(0.0)

        lost_total_df = (
            tmp[tmp["sku"] != ""]
            .groupby("sku", as_index=False)["total"]
            .sum()
            .rename(columns={"total": "lost_total"})
        )
        lost_total_df["lost_total"] = pd.to_numeric(lost_total_df["lost_total"], errors="coerce").fillna(0.0)

    platform_fee_total = float(platform_fee_total or 0.0)
    advertising_fee_total = float(advertising_fee_total or 0.0)

    cm2_profit_dashboard = profit_total - advertising_fee_total - platform_fee_total
    profit_percentage = (cm2_profit_dashboard / net_sales * 100) if net_sales else 0.0
    current_net_reimbursement = compute_net_reimbursement_from_df(df_all) if not df_all.empty else 0.0

    derived_totals = {
        "amazon_fees": round(amazon_fees, 2),
        "platform_fee": round(platform_fee_total, 2),
        "advertising_fees": round(advertising_fee_total, 2),
        "net_sales": round(net_sales, 2),
        "gross_sales": round(gross_sales_total, 2),
        "asp": round(asp, 2),
        "profit": round(profit_total, 2),
        "cm2_profit": round(cm2_profit_dashboard, 2),
        "profit_percentage": round(profit_percentage, 2),
        "current_net_reimbursement": round(float(current_net_reimbursement or 0.0), 2),
    }

    previous_period = get_previous_month_mtd_payload(user_id=user_id, country=ui_country, now_utc=now_utc)

    # ============================================================
    # SKU-WISE TABLE
    # ============================================================
    skuwise_table_name = f"skuwisemonthly_{int(user_id)}_{_safe_ident(ui_country)}_{_safe_ident(month_name)}_{int(now_utc.year)}"
    ads_table_name = _build_adsmonthly_table_name(user_id, ui_country, now_utc.month, now_utc.year)

    sku_summary_saved = False
    sku_summary_rows = 0
    skuwise_items = []

    if not df_all.empty:
        # safe sku column
        if "sku" not in df_all.columns:
            df_all["sku"] = ""
        df_all["sku"] = df_all["sku"].fillna("").astype(str).str.strip()

        df_skus = df_all[df_all["sku"] != ""].copy()

        preferred_sum_cols = [
            "quantity", "product_sales", "product_sales_tax", "postage_credits", "gift_wrap_credits",
            "shipping_credits_tax", "giftwrap_credits_tax", "promotional_rebates", "promotional_rebates_tax",
            "marketplace_facilitator_tax", "selling_fees", "fba_fees", "other", "gross_sales", "cogs", "profit",
        ]
        sum_cols = [c for c in preferred_sum_cols if c in df_skus.columns]

        for c in sum_cols:
            df_skus[c] = pd.to_numeric(df_skus[c], errors="coerce").fillna(0.0)

        df_sku = df_skus.groupby("sku", as_index=False)[sum_cols].sum()

        # merge lost_total
        if lost_total_df is not None and not lost_total_df.empty:
            df_sku = df_sku.merge(lost_total_df, on="sku", how="left")

        if "lost_total" not in df_sku.columns:
            df_sku["lost_total"] = 0.0
        df_sku["lost_total"] = pd.to_numeric(df_sku["lost_total"], errors="coerce").fillna(0.0)

        # finance derived
        if "product_sales" not in df_sku.columns:
            df_sku["product_sales"] = 0.0
        if "promotional_rebates" not in df_sku.columns:
            df_sku["promotional_rebates"] = 0.0

        df_sku["net_sales"] = (
            pd.to_numeric(df_sku["product_sales"], errors="coerce").fillna(0.0)
            + pd.to_numeric(df_sku["promotional_rebates"], errors="coerce").fillna(0.0)
        )

        if "quantity" not in df_sku.columns:
            df_sku["quantity"] = 0.0
        df_sku["quantity"] = pd.to_numeric(df_sku["quantity"], errors="coerce").fillna(0.0)

        df_sku["asp"] = df_sku.apply(
            lambda r: (float(r["net_sales"]) / float(r["quantity"])) if float(r["quantity"]) else 0.0,
            axis=1,
        )

        def _col(df: pd.DataFrame, name: str) -> pd.Series:
            return pd.to_numeric(df[name], errors="coerce").fillna(0.0) if name in df.columns else pd.Series([0.0] * len(df), index=df.index)

        df_sku["credits"] = (_col(df_sku, "postage_credits") + _col(df_sku, "gift_wrap_credits")).fillna(0.0)
        df_sku["tax"] = (
            _col(df_sku, "product_sales_tax")
            + _col(df_sku, "shipping_credits_tax")
            + _col(df_sku, "giftwrap_credits_tax")
            + _col(df_sku, "promotional_rebates_tax")
            + _col(df_sku, "marketplace_facilitator_tax")
        ).fillna(0.0)
        df_sku["tax_and_credits"] = (df_sku["credits"] - df_sku["tax"].abs()).round(2)

        # -------- ADS merge --------
        ads_total_product_spend = 0.0
        ads_total_display_spend = 0.0
        ads_total_brand_spend = 0.0
        ads_agg = pd.DataFrame()

        try:
            sql = f'''
                SELECT
                    products,
                    ad_type,
                    impressions,
                    clicks,
                    spend,
                    sale_units,
                    sale_amount,
                    product_spend,
                    display_spend,
                    brand_spend
                FROM public."{ads_table_name}"
            '''
            ads_df = pd.read_sql_query(sql, PHORMULA_ENGINE)

            if "products" not in ads_df.columns:
                ads_df["products"] = ""
            ads_df["products"] = ads_df["products"].fillna("").astype(str).str.strip()

            gt_mask = ads_df["products"].str.lower().eq("grand total")
            if gt_mask.any():
                ads_total_product_spend = float(pd.to_numeric(ads_df.loc[gt_mask, "product_spend"], errors="coerce").fillna(0.0).sum()) if "product_spend" in ads_df.columns else 0.0
                ads_total_display_spend = float(pd.to_numeric(ads_df.loc[gt_mask, "display_spend"], errors="coerce").fillna(0.0).sum()) if "display_spend" in ads_df.columns else 0.0
                ads_total_brand_spend = float(pd.to_numeric(ads_df.loc[gt_mask, "brand_spend"], errors="coerce").fillna(0.0).sum()) if "brand_spend" in ads_df.columns else 0.0
            else:
                # safe sums even if columns missing
                for c in ["product_spend", "display_spend", "brand_spend"]:
                    if c not in ads_df.columns:
                        ads_df[c] = 0.0
                ads_total_product_spend = float(pd.to_numeric(ads_df["product_spend"], errors="coerce").fillna(0.0).sum())
                ads_total_display_spend = float(pd.to_numeric(ads_df["display_spend"], errors="coerce").fillna(0.0).sum())
                ads_total_brand_spend = float(pd.to_numeric(ads_df["brand_spend"], errors="coerce").fillna(0.0).sum())

            ads_df = ads_df[ads_df["products"] != ""].copy()

            # safe ad_type
            if "ad_type" not in ads_df.columns:
                ads_df["ad_type"] = ""
            ads_df["ad_type"] = ads_df["ad_type"].fillna("").astype(str).str.strip()

            for col in ["impressions", "clicks", "spend", "sale_units", "sale_amount", "product_spend", "display_spend", "brand_spend"]:
                if col not in ads_df.columns:
                    ads_df[col] = 0.0
                ads_df[col] = pd.to_numeric(ads_df[col], errors="coerce").fillna(0.0)

            ads_num = ads_df.groupby("products", as_index=False)[
                ["impressions", "clicks", "spend", "sale_units", "sale_amount", "product_spend", "display_spend", "brand_spend"]
            ].sum()

            ads_type = (
                ads_df[ads_df["ad_type"] != ""]
                .groupby("products")["ad_type"]
                .apply(lambda s: ", ".join(sorted(set([str(x).strip() for x in s if str(x).strip()]))))
                .reset_index()
            )

            ads_agg = ads_num.merge(ads_type, on="products", how="left")
            if "ad_type" not in ads_agg.columns:
                ads_agg["ad_type"] = ""
            ads_agg["ad_type"] = ads_agg["ad_type"].fillna("")

            ads_agg.rename(
                columns={
                    "impressions": "ads_impressions",
                    "clicks": "ads_clicks",
                    "sale_units": "ads_sale_units",
                    "sale_amount": "ads_sale_amount",
                },
                inplace=True,
            )
            if "spend" in ads_agg.columns:
                ads_agg.rename(columns={"spend": "ads_spend_raw"}, inplace=True)

        except Exception as e:
            logger.warning(f"Could not read/aggregate ads table {ads_table_name}: {e}")
            ads_agg = pd.DataFrame()

        if not ads_agg.empty:
            df_sku = (
                df_sku.merge(ads_agg, how="left", left_on="sku", right_on="products")
                .drop(columns=["products"], errors="ignore")
            )

        # ensure ad_type exists
        if "ad_type" not in df_sku.columns:
            df_sku["ad_type"] = ""
        else:
            df_sku["ad_type"] = df_sku["ad_type"].fillna("").astype(str)

        # ensure spend cols exist
        for col in ["product_spend", "display_spend", "brand_spend"]:
            if col not in df_sku.columns:
                df_sku[col] = 0.0
            df_sku[col] = pd.to_numeric(df_sku[col], errors="coerce").fillna(0.0)

        # ✅ ads_spend = product + display
        df_sku["ads_spend"] = (df_sku["product_spend"] + df_sku["display_spend"]).fillna(0.0)

        # ensure ads metrics exist
        for col in ["ads_impressions", "ads_clicks", "ads_sale_units", "ads_sale_amount"]:
            if col not in df_sku.columns:
                df_sku[col] = 0.0
            df_sku[col] = pd.to_numeric(df_sku[col], errors="coerce").fillna(0.0)

        # total-only breakup columns
        df_sku["platform_fee_inventory_storage"] = 0.0
        df_sku["platformfeenew"] = 0.0
        df_sku["dealsvouchar_ads"] = 0.0

        # platform_fee per SKU row
        df_sku["platform_fee"] = (
            pd.to_numeric(df_sku["platform_fee_inventory_storage"], errors="coerce").fillna(0.0)
            + pd.to_numeric(df_sku["platformfeenew"], errors="coerce").fillna(0.0)
            - pd.to_numeric(df_sku["lost_total"], errors="coerce").fillna(0.0)
        )

        # profit and cm2
        if "profit" not in df_sku.columns:
            df_sku["profit"] = 0.0
        df_sku["profit"] = pd.to_numeric(df_sku["profit"], errors="coerce").fillna(0.0)

        df_sku["cm2_profit"] = (df_sku["profit"] - df_sku["ads_spend"]).fillna(0.0)

        df_sku["cm1_profit_per_unit"] = df_sku.apply(
            lambda r: (float(r["profit"]) / float(r["quantity"])) if float(r["quantity"]) else 0.0,
            axis=1,
        )
        df_sku["cm1_profit_per"] = df_sku.apply(
            lambda r: (float(r["profit"]) / float(r["net_sales"]) * 100.0) if float(r["net_sales"]) else 0.0,
            axis=1,
        )
        df_sku["cm2_profit_per_unit"] = df_sku.apply(
            lambda r: (float(r["cm2_profit"]) / float(r["quantity"])) if float(r["quantity"]) else 0.0,
            axis=1,
        )
        df_sku["cm2_profit_per"] = df_sku.apply(
            lambda r: (float(r["cm2_profit"]) / float(r["net_sales"]) * 100.0) if float(r["net_sales"]) else 0.0,
            axis=1,
        )

        for col in ["cm2_profit", "cm1_profit_per_unit", "cm1_profit_per", "cm2_profit_per_unit", "cm2_profit_per"]:
            if col not in df_sku.columns:
                df_sku[col] = 0.0
            df_sku[col] = pd.to_numeric(df_sku[col], errors="coerce").fillna(0.0)

        # product_name mapping
        df_sku["product_name"] = ""
        sku_data_table = _build_sku_data_table_name(user_id)
        sku_col = _country_to_sku_col(ui_country)

        try:
            map_sql = f'SELECT product_name, "{sku_col}" AS sku_key FROM public."{sku_data_table}"'
            map_df = pd.read_sql_query(map_sql, PHORMULA_ENGINE)

            if not map_df.empty:
                map_df["sku_key"] = map_df["sku_key"].fillna("").astype(str).str.strip()
                map_df["product_name"] = map_df["product_name"].fillna("").astype(str).str.strip()
                map_df = (
                    map_df[map_df["sku_key"] != ""]
                    .sort_values(by=["sku_key"])
                    .drop_duplicates(subset=["sku_key"], keep="first")
                )

                df_sku = df_sku.merge(map_df, how="left", left_on="sku", right_on="sku_key").drop(columns=["sku_key"], errors="ignore")

                if "product_name_y" in df_sku.columns and "product_name_x" in df_sku.columns:
                    df_sku["product_name"] = df_sku["product_name_y"].fillna(df_sku["product_name_x"]).fillna("")
                    df_sku.drop(columns=["product_name_x", "product_name_y"], inplace=True, errors="ignore")
                elif "product_name_y" in df_sku.columns:
                    df_sku.rename(columns={"product_name_y": "product_name"}, inplace=True)

                if "product_name" not in df_sku.columns:
                    df_sku["product_name"] = ""
                df_sku["product_name"] = df_sku["product_name"].fillna("").astype(str)

        except Exception as e:
            logger.warning(f"Could not read/map product_name from {sku_data_table}: {e}")
            if "product_name" not in df_sku.columns:
                df_sku["product_name"] = ""
            df_sku["product_name"] = df_sku["product_name"].fillna("").astype(str)

        # meta
        df_sku["user_id"] = int(user_id)
        df_sku["country"] = ui_country
        df_sku["month"] = _safe_ident(month_name)
        df_sku["year"] = int(now_utc.year)
        df_sku["generated_at_utc"] = now_utc.isoformat()

        # -------- GRAND TOTAL row --------
        total_row = {"sku": "GRAND_TOTAL", "product_name": "Grand Total"}

        for c in sum_cols:
            total_row[c] = float(df_sku[c].sum()) if c in df_sku.columns else 0.0

        total_row["lost_total"] = float(pd.to_numeric(df_sku["lost_total"], errors="coerce").fillna(0.0).sum()) if "lost_total" in df_sku.columns else 0.0

        total_row["net_sales"] = float(df_sku["net_sales"].sum()) if "net_sales" in df_sku.columns else 0.0
        total_qty = float(df_sku["quantity"].sum()) if "quantity" in df_sku.columns else 0.0
        total_row["asp"] = (total_row["net_sales"] / total_qty) if total_qty else 0.0

        total_row["ads_impressions"] = float(df_sku["ads_impressions"].sum()) if "ads_impressions" in df_sku.columns else 0.0
        total_row["ads_clicks"] = float(df_sku["ads_clicks"].sum()) if "ads_clicks" in df_sku.columns else 0.0
        total_row["ads_sale_units"] = float(df_sku["ads_sale_units"].sum()) if "ads_sale_units" in df_sku.columns else 0.0
        total_row["ads_sale_amount"] = float(df_sku["ads_sale_amount"].sum()) if "ads_sale_amount" in df_sku.columns else 0.0

        total_row["product_spend"] = round(float(ads_total_product_spend or 0.0), 2)
        total_row["display_spend"] = round(float(ads_total_display_spend or 0.0), 2)
        total_row["brand_spend"] = round(float(ads_total_brand_spend or 0.0), 2)
        total_row["ads_spend"] = round(total_row["product_spend"] + total_row["display_spend"], 2)

        # store totals
        total_row["platform_fee_inventory_storage"] = round(float(platform_fee_inventory_storage_total or 0.0), 2)
        total_row["platformfeenew"] = round(float(platformfeenew_total or 0.0), 2)
        total_row["dealsvouchar_ads"] = round(float(dealsvouchar_ads_total or 0.0), 2)

        total_row["platform_fee"] = round(
            float(total_row["platform_fee_inventory_storage"]) + float(total_row["platformfeenew"]) - float(total_row["lost_total"]),
            2,
        )

        total_row["ad_type"] = "All"

        g_clicks = float(total_row["ads_clicks"])
        g_spend = float(total_row["ads_spend"])
        g_units = float(total_row["ads_sale_units"])
        g_sales = float(total_row["ads_sale_amount"])

        total_row["ads_conversion_rate"] = (g_units / g_clicks * 100.0) if g_clicks else 0.0
        total_row["ads_roas"] = (g_sales / g_spend) if g_spend else 0.0
        total_row["ads_acos"] = (g_spend / g_sales * 100.0) if g_sales else 0.0

        g_profit = float(total_row.get("profit", 0.0))
        g_net_sales = float(total_row.get("net_sales", 0.0))

        total_row["cm2_profit"] = g_profit - g_spend
        total_row["cm1_profit_per_unit"] = (g_profit / total_qty) if total_qty else 0.0
        total_row["cm1_profit_per"] = (g_profit / g_net_sales * 100.0) if g_net_sales else 0.0
        total_row["cm2_profit_per_unit"] = (total_row["cm2_profit"] / total_qty) if total_qty else 0.0
        total_row["cm2_profit_per"] = (total_row["cm2_profit"] / g_net_sales * 100.0) if g_net_sales else 0.0

        total_row["credits"] = float(df_sku["credits"].sum()) if "credits" in df_sku.columns else 0.0
        total_row["tax"] = float(df_sku["tax"].sum()) if "tax" in df_sku.columns else 0.0
        total_row["tax_and_credits"] = round(float(total_row["credits"]) - abs(float(total_row["tax"])), 2)

        total_row["user_id"] = int(user_id)
        total_row["country"] = ui_country
        total_row["month"] = _safe_ident(month_name)
        total_row["year"] = int(now_utc.year)
        total_row["generated_at_utc"] = now_utc.isoformat()

        df_sku = pd.concat([df_sku, pd.DataFrame([total_row])], ignore_index=True)

        # replace NaN/Inf before jsonify/to_dict
        df_sku = df_sku.replace([np.inf, -np.inf], np.nan).where(pd.notnull(df_sku), None)

        skuwise_items = df_sku.to_dict(orient="records")

        # store SKU-wise table
        try:
            df_sku.to_sql(
                skuwise_table_name,
                PHORMULA_ENGINE,
                schema="public",
                if_exists="replace",
                index=False,
                method="multi",
                chunksize=1000,
            )
            sku_summary_saved = True
            sku_summary_rows = int(len(df_sku))
        except Exception as e:
            logger.exception(f"Failed to store SKU-wise table {skuwise_table_name}: {e}")
            sku_summary_saved = False

    # ---------------- Excel response ----------------
    if response_format == "excel":
        df = pd.DataFrame(all_rows) if all_rows else pd.DataFrame()
        df = df.reindex(columns=MTD_COLUMNS + ["cogs", "profit", "gross_sales"], fill_value=0.0)

        output = io.BytesIO()
        with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
            df.to_excel(writer, index=False, sheet_name="Transactions")
            pd.DataFrame([totals]).to_excel(writer, index=False, sheet_name="Totals")
            pd.DataFrame([derived_totals]).to_excel(writer, index=False, sheet_name="DerivedTotals")
            pd.DataFrame([previous_period]).to_excel(writer, index=False, sheet_name="PrevPeriodMeta")
            if db_result:
                pd.DataFrame([db_result]).to_excel(writer, index=False, sheet_name="DBMeta")
            if skuwise_items:
                pd.DataFrame(skuwise_items).to_excel(writer, index=False, sheet_name="SKUWiseMonthly")

        output.seek(0)
        filename = f"finances_transactions_MTD_{now_utc.year}_{now_utc.month:02d}.xlsx"
        return send_file(
            output,
            as_attachment=True,
            download_name=filename,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

    # ---------------- JSON response ----------------
    payload_out = {
        "success": True,
        "posted_after": posted_after,
        "posted_before": posted_before,
        "count": len(all_rows),
        "stored": bool(store_in_db),
        "db_result": db_result,
        "cogs_meta": {
            "country": ui_country,
            "month": month_name,
            "year": now_utc.year,
            "pair": f"{user_currency}->{selected_currency}",
            "conversion_rate": conversion_rate_fx,
        },
        "totals": totals,
        "derived_totals": derived_totals,
        "previous_period": previous_period,
        "skuwise_table": {
            "name": skuwise_table_name,
            "saved": sku_summary_saved,
            "rows": sku_summary_rows,
        },
        "skuwise_items": skuwise_items,
        "transactions": all_rows,
    }

    return jsonify(_json_safe(payload_out)), 200





