from flask import Blueprint, request, jsonify
from app.utils.token_utils import get_effective_user_id_from_token
from sqlalchemy import create_engine
from config import Config
from openai import OpenAI
from dotenv import load_dotenv
import os
from datetime import datetime
from app.utils.uk_coverage_ratio_utils import compute_inventory_coverage_ratio

from app.utils.business_journey_utils import (
    fetch_skuwise_monthly,
    fetch_business_context,
    prepare_ai_sku_data,
    generate_business_journey,
    fetch_existing_business_journey,
    save_business_journey_by_id,
    get_previous_month_year,
    fetch_month_end_inventory_lookup,
)

load_dotenv()
SECRET_KEY = Config.SECRET_KEY

db_url = os.getenv("DATABASE_URL")
db_url2 = os.getenv("DATABASE_CHATBOT_URL")
db_url3 = os.getenv("DATABASE_AMAZON_URL")

phormula_engine = create_engine(db_url)
chatbot_engine = create_engine(db_url2)
amazon_engine = create_engine(db_url3)

openai_client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

business_journey_bp = Blueprint("business_journey_bp", __name__)


@business_journey_bp.route("/generate", methods=["GET"])
def generate_business_journey_route():
    auth_header = request.headers.get("Authorization")
    if not auth_header or not auth_header.startswith("Bearer "):
        return jsonify({"error": "Authorization token is missing or invalid"}), 401

    token = auth_header.split(" ")[1]

    try:
        payload, effective_user_id, member_id = get_effective_user_id_from_token(token)

        user_id = payload.get("user_id")
        if not user_id:
            return jsonify({"error": "Invalid token payload"}), 401

        country = (request.args.get("countryName", "uk") or "uk").strip().lower()
        month_name = (request.args.get("month", "february") or "february").strip().lower()
        year = int(request.args.get("year", datetime.utcnow().year))

        print("user_id:", user_id)
        print("country:", country)
        print("objective month:", month_name, year)

        existing_journey = fetch_existing_business_journey(
            chatbot_engine=chatbot_engine,
            user_id=user_id,
            country=country,
            month_name=month_name,
            year=year
        )

        if existing_journey:
            return jsonify({
                "status": "success",
                "source": "database",
                "saved_to_db": True,
                "business_journey": existing_journey
            }), 200

        business_context_df = fetch_business_context(
            chatbot_engine=chatbot_engine,
            user_id=user_id,
            country=country,
            month_name=month_name,
            year=year
        )

        print("business_context_df empty:", business_context_df.empty)
        print(
            "business_context_df:",
            business_context_df.to_dict(orient="records") if not business_context_df.empty else []
        )

        if business_context_df.empty:
            return jsonify({
                "error": "No matching user_objectives row found",
                "details": f"No row found for user_id={user_id}, country={country}, month={month_name}, year={year}"
            }), 404

        sku_month_name, sku_year = get_previous_month_year(month_name, year)

        print("sku source month:", sku_month_name, sku_year)

        try:
            sku_df = fetch_skuwise_monthly(
                phormula_engine=phormula_engine,
                user_id=user_id,
                country=country,
                month_name=sku_month_name,
                year=sku_year
            )
        except Exception as e:
            return jsonify({
                "error": "No SKU monthly data found",
                "details": str(e)
            }), 404

        if sku_df.empty:
            return jsonify({
                "error": "SKU monthly table is empty",
                "details": f"No data found in skuwisemonthly_{user_id}_{country}_{sku_month_name}{sku_year}"
            }), 404

        sku_data, overall_metrics = prepare_ai_sku_data(sku_df)

        # =========================================================
        # ✅ ADD THIS ENTIRE BLOCK (INVENTORY INJECTION)
        # =========================================================

        inventory_lookup = fetch_month_end_inventory_lookup(user_id)

        month_map = {
            "january": 1, "february": 2, "march": 3, "april": 4,
            "may": 5, "june": 6, "july": 7, "august": 8,
            "september": 9, "october": 10, "november": 11, "december": 12
        }

        month_num = month_map.get(month_name.lower(), 1)

        for sku in sku_data:
            key = (str(sku.get("sku")), year, month_num)

            inventory = inventory_lookup.get(key, {
                "sellable_inventory": 0,
                "damaged_inventory": 0,
                "expired_inventory": 0
            })

            # ✅ attach inventory data
            sku["sellable_inventory"] = inventory["sellable_inventory"]
            sku["damaged_inventory"] = inventory["damaged_inventory"]
            sku["expired_inventory"] = inventory["expired_inventory"]

            # ✅ optional but VERY IMPORTANT (for alerts)
            try:
                sku["inventory_coverage_days"] = compute_inventory_coverage_ratio(sku)
            except:
                sku["inventory_coverage_days"] = 0

        # =========================================================

        row = business_context_df.iloc[0] if not business_context_df.empty else None

        


        row = business_context_df.iloc[0]
        business_context = row.to_dict()
        objective_id = int(row["id"])

        print("objective_id:", objective_id)

        business_journey = generate_business_journey(
            business_context=business_context,
            sku_data=sku_data,
            overall_metrics=overall_metrics,
            openai_client=openai_client
        )

        saved = save_business_journey_by_id(
            chatbot_engine=chatbot_engine,
            objective_id=objective_id,
            business_journey=business_journey
        )

        return jsonify({
            "status": "success",
            "source": "generated",
            "saved_to_db": saved,
            "objective_id": int(objective_id),
            "objective_month": month_name,
            "objective_year": year,
            "sku_source_month": sku_month_name,
            "sku_source_year": sku_year,
            "business_journey": business_journey
        }), 200

    except Exception as e:
        return jsonify({
            "error": "Failed to generate business journey",
            "details": str(e)
        }), 500
    
