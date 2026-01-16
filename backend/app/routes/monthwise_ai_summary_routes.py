from flask import Blueprint, request, jsonify
from app.utils.monthwise_ai_summary_utils import get_or_create_summary
from flask import Blueprint, request, jsonify
import jwt
import os
from sqlalchemy import create_engine
from dotenv import load_dotenv
from config import Config
from calendar import month_abbr, monthrange
from datetime import date, datetime, timedelta
from openai import OpenAI
import json
import pandas as pd
from app.models.user_models import HistoricAISummary
from app.utils.formulas_utils import uk_all
from app import db
from app.utils.history_graph_utils import get_performance_trend


summary_bp = Blueprint("summary_bp", __name__)



load_dotenv()
SECRET_KEY = Config.SECRET_KEY




@summary_bp.route("/summary", methods=["GET"])
def summary():
    auth_header = request.headers.get("Authorization")
    if not auth_header or not auth_header.startswith("Bearer "):
        return jsonify({"error": "Authorization token is missing or invalid"}), 401

    token = auth_header.split(" ")[1]

    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=["HS256"])
        user_id = payload.get("user_id")

        if not user_id:
            return jsonify({"error": "Invalid token payload: user_id missing"}), 401

        # ----------- SAFE PARAM EXTRACTION -----------
        country = (request.args.get("country", "uk") or "uk").strip().lower()
        marketplace_id = request.args.get("marketplace_id", type=int)

        period = (request.args.get("period") or "").strip().lower()       # monthly / quarterly / yearly
        timeline = (request.args.get("timeline") or "").strip().upper()   # "12", "Q4", "ALL"
        year = request.args.get("year", type=int)

        # Optional: UI toggle (you can ignore on backend if you return both arrays)
        metric = (request.args.get("metric", "net_sales") or "net_sales").strip().lower()
        if metric not in ("net_sales", "units"):
            metric = "net_sales"

        if not period or not timeline or not year:
            return jsonify({"error": "Missing required query params: period, timeline, year"}), 400

        if period not in ("monthly", "quarterly", "yearly"):
            return jsonify({"error": "Invalid period. Allowed: monthly, quarterly, yearly"}), 400

        # Basic timeline validation (prevents accidental bad inputs)
        if period == "monthly":
            if not timeline.isdigit() or not (1 <= int(timeline) <= 12):
                return jsonify({"error": "Invalid timeline for monthly. Use '1'..'12'"}), 400
        elif period == "quarterly":
            if timeline not in ("Q1", "Q2", "Q3", "Q4"):
                return jsonify({"error": "Invalid timeline for quarterly. Use 'Q1'..'Q4'"}), 400
        elif period == "yearly":
            # You said timeline can be "ALL" for yearly; allow anything but don't block.
            # If you want strict: require timeline == "ALL"
            pass

        # ----------- CORE SUMMARY LOGIC -----------
        result = get_or_create_summary(
            user_id=user_id,
            country=country,
            marketplace_id=marketplace_id,
            period=period,
            timeline=timeline,
            year=year,
        )

        # ----------- PERFORMANCE TREND (NEW) -----------
        # Make sure you have:
        # from utils.performance_trend import get_performance_trend
        performance_trend = get_performance_trend(
            user_id=user_id,
            country=country,
            period=period,
            timeline=timeline,
            year=year,
        )

        # Attach to response (keeping get_or_create_summary untouched)
        result["performance_trend"] = performance_trend

        # Optionally include the requested metric hint for frontend
        result["performance_trend_metric"] = metric

        return jsonify(result), 200

    except jwt.ExpiredSignatureError:
        return jsonify({"error": "Token has expired"}), 401

    except jwt.InvalidTokenError:
        return jsonify({"error": "Invalid token"}), 401

    except Exception as e:
        print("Unexpected error in /summary:", e)
        return jsonify({"error": "Server error", "details": str(e)}), 500



