from flask import Blueprint, request, jsonify
from app.utils.token_utils import get_effective_user_id_from_token
from sqlalchemy import create_engine
from config import Config
from openai import OpenAI
from dotenv import load_dotenv
import os
from app.utils.agent_utils import build_plan_langgraph


load_dotenv()
SECRET_KEY = Config.SECRET_KEY

db_url = os.getenv("DATABASE_URL")
db_url2 = os.getenv("DATABASE_CHATBOT_URL")
db_url3 = os.getenv("DATABASE_AMAZON_URL")

phormula_engine = create_engine(db_url)
chatbot_engine = create_engine(db_url2)
amazon_engine = create_engine(db_url3)

openai_client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

planning_bp = Blueprint("planning_bp", __name__)

@planning_bp.route("/api/event-plan", methods=["POST"])
def event_plan():
    auth_header = request.headers.get("Authorization")
    if not auth_header or not auth_header.startswith("Bearer "):
        return jsonify({"error": "Authorization token is missing or invalid"}), 401

    token = auth_header.split(" ")[1]

    try:
        payload_token, effective_user_id, member_id = get_effective_user_id_from_token(token)

        user_id = payload_token.get("user_id")
        if not user_id:
            return jsonify({"error": "Invalid token payload"}), 401

        payload = request.get_json()
        if not payload:
            return jsonify({"error": "Request body is required"}), 400

        # ✅ attach user + country
        payload["user_id"] = user_id
        payload["country"] = (
            payload.get("country") or request.args.get("countryName") or "uk"
        ).strip().lower()

        # 🔥 OPTIONAL: target_sales handling
        target_sales = payload.get("target_sales")

        if target_sales is not None:
            try:
                target_sales = float(target_sales)

                if target_sales < 0:
                    return jsonify({"error": "target_sales must be non-negative"}), 400

                payload["target_sales"] = target_sales

            except (ValueError, TypeError):
                return jsonify({"error": "target_sales must be a valid number"}), 400
        else:
            # optional cleanup (not required but clean)
            payload.pop("target_sales", None)

        # ✅ validate required fields
        for key in ["last_event", "future_event"]:
            if key not in payload:
                return jsonify({"error": f"Missing {key}"}), 400

            for sub in ["name", "month", "year"]:
                if sub not in payload[key]:
                    return jsonify({"error": f"Missing {key}.{sub}"}), 400

        # 🚀 run planning engine
        result = build_plan_langgraph(
            payload,
            phormula_engine,
            amazon_engine
        )

        return jsonify({
            "status": "success",
            "user_id": user_id,
            "total_skus": len(result),
            "result": result
        }), 200

    except ValueError as e:
        return jsonify({"error": str(e)}), 400

    except Exception as e:
        return jsonify({
            "error": "Failed to generate event plan",
            "details": str(e)
        }), 500
