from __future__ import annotations
import io, os,time, logging 
from datetime import datetime
import pandas as pd
from typing import Any, Dict, List
from datetime import datetime, timezone
from sqlalchemy.dialects.postgresql import insert as pg_insert
import jwt, requests
from config import Config
UPLOAD_FOLDER = Config.UPLOAD_FOLDER
from dotenv import find_dotenv, load_dotenv
from flask import Blueprint, jsonify, make_response, request
from app import db
from app.models.user_models import amazon_user
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
        payload = jwt.decode(token, SECRET_KEY, algorithms=["HS256"])
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
        payload = jwt.decode(token, SECRET_KEY, algorithms=["HS256"])
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

# ---------------------------------------------------------
# ROUTE (MTD)  ✅ includes totals + cogs in JSON
# ---------------------------------------------------------
@amazon_api_bp.route("/amazon_api/finances/mtd_transactions", methods=["GET"])
def finances_mtd_transactions():
    auth_header = request.headers.get("Authorization")
    if not auth_header or not auth_header.startswith("Bearer "):
        return jsonify({"success": False, "error": "Authorization token is missing or invalid"}), 401

    token = auth_header.split(" ")[1]
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=["HS256"])
        user_id = int(payload["user_id"])
    except jwt.ExpiredSignatureError:
        return jsonify({"success": False, "error": "Token has expired"}), 401
    except jwt.InvalidTokenError:
        return jsonify({"success": False, "error": "Invalid token"}), 401

    transaction_status = request.args.get("transaction_status", "RELEASED")
    marketplace_id = request.args.get("marketplace_id")
    transaction_type_filter = request.args.get("transaction_type")
    response_format = (request.args.get("format") or "json").lower()
    store_in_db = (request.args.get("store_in_db", "true").lower() != "false")

    ui_country = (request.args.get("country") or "").strip().lower() or "uk"

    _apply_region_and_marketplace_from_request()

    au = amazon_user.query.filter_by(user_id=user_id, region=amazon_client.region).first()
    if not au or not au.refresh_token:
        return jsonify({
            "success": False,
            "error": "Amazon account not connected for this region",
            "status": "no_refresh_token",
        }), 400

    amazon_client.refresh_token = au.refresh_token

    now_utc = datetime.now(timezone.utc)
    posted_after, posted_before = _month_to_date_range_utc_safe(now_utc, safety_minutes=3)

    # ✅ prepare cogs inputs once
    month_name = _month_name_lower(now_utc.month)
    user_currency = DEFAULT_SKU_PRICE_CURRENCY
    selected_currency = COUNTRY_TO_SELECTED_CURRENCY.get(ui_country, user_currency)

    sku_price_map = fetch_sku_price_map(user_id=user_id, country=ui_country)
    conversion_rate = fetch_conversion_rate(
        country=ui_country,
        year=now_utc.year,
        month_name=month_name,
        user_currency=user_currency,
        selected_currency=selected_currency
    )

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

            row = _flatten_transaction_to_row(tx or {})

            # ✅ add cogs per row
            sku = (row.get("sku") or "").strip()
            qty = _i(row.get("quantity")) or 0
            price = sku_price_map.get(sku) if sku else None
            row["cogs"] = float(qty) * float(price) * float(conversion_rate) if (price is not None and qty > 0) else 0.0

            all_rows.append(row)

        next_token = payload_res.get("nextToken")
        if not next_token:
            break
        params = {"nextToken": next_token}

    # ✅ profit per row
    add_profit_column_from_uk_profit(all_rows, country=ui_country)

    # ✅ store to DB
    db_result = None
    if store_in_db:
        try:
            db_result = upsert_liveorders_from_rows(
                all_rows,
                user_id=user_id,
                country=ui_country,
                now_utc=now_utc
            )
        except Exception as e:
            db.session.rollback()
            return jsonify({"success": False, "error": f"DB store failed: {str(e)}"}), 500
        
    # ✅ NEW: compute gross_sales per row (so totals can sum it)
    for r in all_rows:
        r["gross_sales"] = (
            float(r.get("product_sales", 0.0)) +
            float(r.get("product_sales_tax", 0.0)) +
            float(r.get("postage_credits", 0.0)) +
            float(r.get("gift_wrap_credits", 0.0)) +
            float(r.get("shipping_credits_tax", 0.0)) +
            float(r.get("giftwrap_credits_tax", 0.0)) -
            float(r.get("promotional_rebates", 0.0)) -
            float(r.get("promotional_rebates_tax", 0.0))
        )
    totals = compute_totals(all_rows)

    # ✅ NEW: tax_and_credits
    tax_and_credits = (
        float(totals.get("postage_credits", 0.0)) +
        float(totals.get("gift_wrap_credits", 0.0)) +
        float(totals.get("product_sales_tax", 0.0)) +
        float(totals.get("shipping_credits_tax", 0.0)) +
        float(totals.get("promotional_rebates_tax", 0.0)) +
        float(totals.get("marketplace_facilitator_tax", 0.0))
    )

    totals["tax_and_credits"] = round(tax_and_credits, 2)


    selling_fees = float(totals.get("selling_fees", 0.0))
    fba_fees     = float(totals.get("fba_fees", 0.0))
    amazon_fees  = abs(selling_fees) + abs(fba_fees)   # ✅ positive
    gross_sales = float(totals.get("gross_sales", 0.0))

    net_sales = float(totals.get("product_sales", 0.0)) + float(totals.get("promotional_rebates", 0.0))
    qty = float(totals.get("quantity", 0.0)) or 0.0
    asp = (net_sales / qty) if qty else 0.0

    profit = float(totals.get("profit", 0.0))
    

    df_all = pd.DataFrame(all_rows) if all_rows else pd.DataFrame()

    platform_fee_total = 0.0
    advertising_fee_total = 0.0

    if not df_all.empty:
        for col, default in [("description", ""), ("total", 0.0), ("platform_fees", 0.0), ("advertising_cost", 0.0)]:
            if col not in df_all.columns:
                df_all[col] = default

        platform_fee_total, _, _ = uk_platform_fee(df_all, country=ui_country, want_breakdown=False)
        advertising_fee_total, _, _ = uk_advertising(df_all, country=ui_country, want_breakdown=False)

    platform_fee_total = float(platform_fee_total or 0.0)
    advertising_fee_total = float(advertising_fee_total or 0.0)
    cm2_profit = profit - advertising_fee_total - platform_fee_total
    profit_percentage = (cm2_profit / net_sales * 100) if net_sales else 0.0
    current_net_reimbursement = compute_net_reimbursement_from_df(df_all)





    derived_totals = {
        "amazon_fees": round(amazon_fees, 2),
        "platform_fee": round(platform_fee_total, 2),          # ✅ NEW
        "advertising_fees": round(advertising_fee_total, 2),   # ✅ NEW
        "net_sales": round(net_sales, 2),
        "gross_sales": round(gross_sales, 2),
        "asp": round(asp, 2),
        "profit": round(profit, 2),
        "cm2_profit": round(cm2_profit, 2),   
        "profit_percentage": round(profit_percentage, 2),
        # ✅ NEW
        "current_net_reimbursement": round(current_net_reimbursement, 2),

    }


    # ✅ NEW: previous-month MTD data
    previous_period = get_previous_month_mtd_payload(
        user_id=user_id,
        country=ui_country,
        now_utc=now_utc
    )

    if response_format == "excel":
        df = pd.DataFrame(all_rows) if all_rows else pd.DataFrame()
        df = df.reindex(columns=MTD_COLUMNS + ["cogs", "profit"], fill_value=0.0)

        output = io.BytesIO()
        with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
            df.to_excel(writer, index=False, sheet_name="Transactions")
            pd.DataFrame([totals]).to_excel(writer, index=False, sheet_name="Totals")
            pd.DataFrame([derived_totals]).to_excel(writer, index=False, sheet_name="DerivedTotals")
            pd.DataFrame([previous_period]).to_excel(writer, index=False, sheet_name="PrevPeriodMeta")
            if db_result:
                pd.DataFrame([db_result]).to_excel(writer, index=False, sheet_name="DBMeta")

        output.seek(0)
        filename = f"finances_transactions_MTD_{now_utc.year}_{now_utc.month:02d}.xlsx"
        return send_file(
            output,
            as_attachment=True,
            download_name=filename,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

    return jsonify({
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
            "conversion_rate": conversion_rate,
        },
        "totals": totals,
        "derived_totals": derived_totals,
        "previous_period": previous_period,   # ✅ added
        "transactions": all_rows,
    }), 200

