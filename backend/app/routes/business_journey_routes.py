from flask import Blueprint, request, jsonify
from app.utils.token_utils import get_effective_user_id_from_token
from sqlalchemy import create_engine, text
from config import Config
from openai import OpenAI
from dotenv import load_dotenv
import os
from datetime import datetime
from app.utils.business_journey_utils import (
    fetch_skuwise_monthly,
    fetch_business_context,
    prepare_ai_sku_data,
    generate_business_journey,
    fetch_existing_business_journey,
    save_business_journey_by_id
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
        payload, user_id, member_id = get_effective_user_id_from_token(token)

        user_id = payload.get("user_id")
        if not user_id:
            return jsonify({"error": "Invalid token payload"}), 401

        country = (request.args.get("countryName", "uk") or "uk").strip().lower()
        month_name = (request.args.get("month", "february") or "february").strip().lower()
        year = int(request.args.get("year", datetime.utcnow().year))

        # ---------------------------
        # ✅ STEP 1: CHECK IF EXISTS
        # ---------------------------
        existing_journey = fetch_existing_business_journey(
            chatbot_engine,
            user_id,
            country,
            month_name,
            year
        )

        if existing_journey:
            return jsonify({
                "status": "success",
                "source": "database",
                "business_journey": existing_journey
            }), 200

        # ---------------------------
        # FETCH DATA
        # ---------------------------
        sku_df = fetch_skuwise_monthly(
            phormula_engine,
            user_id,
            country,
            month_name,
            year
        )

        business_context_df = fetch_business_context(
            chatbot_engine,
            user_id,
            country,
            month_name,
            year
        )

        sku_data, overall_metrics = prepare_ai_sku_data(sku_df)

        row = business_context_df.iloc[0] if not business_context_df.empty else None

        business_context = row.to_dict() if row is not None else {}
        objective_id = row["id"] if row is not None else None

        # ---------------------------
        # GENERATE AI
        # ---------------------------
        business_journey = generate_business_journey(
            business_context,
            sku_data,
            overall_metrics,
            openai_client
        )

        # ---------------------------
        # ✅ STEP 2: SAVE TO DB
        # ---------------------------
        if objective_id:
            save_business_journey_by_id(
                objective_id,
                business_journey
            )

        return jsonify({
            "status": "success",
            "source": "generated",
            "business_journey": business_journey
        }), 200

    except Exception as e:
        return jsonify({
            "error": "Failed to generate business journey",
            "details": str(e)
        }), 500



