import io
from flask import Blueprint, jsonify, request, send_file
import jwt
from config import Config
import pandas as pd
from app.utils.amazon_utils import _apply_region_and_marketplace_from_request, amazon_client
from app.utils.amazon_ads_utils_reporting import AmazonAdsReportingClient, AmazonAdsAuthContext 
from app.models.user_models import amazon_user 
from app.utils.amazon_ads_utils_reporting import get_ads_access_token_from_refresh


SECRET_KEY = Config.SECRET_KEY
advertisement_api_routes_bp = Blueprint("advertisement_api_routes", __name__)

@advertisement_api_routes_bp.route("/api/advertisement/sp_advertised_product_report", methods=["POST"])
def sp_advertised_product_report():
    # ---- JWT auth ----
    auth_header = request.headers.get("Authorization")
    if not auth_header or not auth_header.startswith("Bearer "):
        return jsonify({"error": "Authorization token is missing or invalid"}), 401

    token = auth_header.split(" ")[1]
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=["HS256"])
        _user_id = payload["user_id"]  # keep if you want for DB lookup
    except jwt.ExpiredSignatureError:
        return jsonify({"error": "Token has expired"}), 401
    except jwt.InvalidTokenError:
        return jsonify({"error": "Invalid token"}), 401

    data = request.get_json(force=True) or {}

    # Inputs
    start_date = data.get("start_date")  # YYYY-MM-DD
    end_date = data.get("end_date")      # YYYY-MM-DD
    time_unit = (data.get("time_unit") or "SUMMARY").upper()
    if time_unit not in {"DAILY", "SUMMARY"}:
        return jsonify({"error": "time_unit must be DAILY or SUMMARY"}), 400
    if not start_date or not end_date:
        return jsonify({"error": "start_date and end_date are required (YYYY-MM-DD)"}), 400

    # Use your same marketplace/region style
    try:
        _apply_region_and_marketplace_from_request()
        sp_region = amazon_client.region
        marketplace = amazon_client.marketplace_id

    except Exception as e:
        return jsonify({"error": str(e)}), 400


    # Country fill
    country = (data.get("country") or marketplace or "UK").upper()

    # ---- REQUIRED ADS AUTH INPUTS ----
    # For "complete code" we accept these in request:
    ads_refresh_token = data.get("ads_refresh_token")
    ads_profile_id = data.get("ads_profile_id")

    if not ads_refresh_token or not ads_profile_id:
        return jsonify({
            "error": "Missing ads_refresh_token or ads_profile_id. "
                     "These must come from Amazon Ads OAuth (not SP-API)."
        }), 400

    try:
        # 1) refresh Ads access token
        access_token = get_ads_access_token_from_refresh(ads_refresh_token)

        # 2) create reporting client
        auth_ctx = AmazonAdsAuthContext(
            access_token=access_token,
            client_id=Config.AMAZON_ADS_CLIENT_ID,
            profile_id=str(ads_profile_id),
        )
        ads = AmazonAdsReportingClient(sp_region=sp_region, auth=auth_ctx)

        # 3) create report -> wait -> download
        report_id = ads.create_sp_advertised_product_report(start_date, end_date, time_unit=time_unit)
        location = ads.wait_until_ready(report_id, max_wait_seconds=240, poll_every_seconds=6)
        rows = ads.download_gzip_json(location)

        # 4) convert to your excel-like schema
        df = ads.to_console_like_dataframe(rows, start_date=start_date, end_date=end_date, country=country)

        # 5) return as .xlsx
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name="AdvertisedProduct")
        output.seek(0)

        filename = f"Sponsored_Products_Advertised_product_report_{start_date}_to_{end_date}.xlsx"
        return send_file(
            output,
            as_attachment=True,
            download_name=filename,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

    except Exception as e:
        return jsonify({"error": str(e)}), 500

