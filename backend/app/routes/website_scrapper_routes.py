from config import Config
from dotenv import load_dotenv
from app.utils.token_utils import get_effective_user_id_from_token
from flask import Blueprint, jsonify, request
from app.utils.web_scrapping_utils import analyze_business_website
import os
import tempfile
from app.models.user_models import UserObjective
from app import db
from datetime import datetime, date

website_scrapper_bp = Blueprint("website_scrapper_bp", __name__)

load_dotenv()


def get_month_start(value=None):
    if not value:
        now = datetime.utcnow()
        return date(now.year, now.month, 1)

    if isinstance(value, date):
        return date(value.year, value.month, 1)

    value = str(value).strip()
    try:
        if len(value) == 7:
            dt = datetime.strptime(value, "%Y-%m")
        else:
            dt = datetime.strptime(value, "%Y-%m-%d")
        return date(dt.year, dt.month, 1)
    except ValueError:
        raise ValueError("month must be in 'YYYY-MM' or 'YYYY-MM-DD'")


@website_scrapper_bp.route("/analyze-website", methods=["GET", "POST"])
def analyze_website():
    auth_header = request.headers.get("Authorization")

    if not auth_header or not auth_header.startswith("Bearer "):
        return jsonify({"error": "Authorization token is missing or invalid"}), 401

    token = auth_header.split(" ")[1]
    ppt_path = None

    try:
        payload, user_id, member_id = get_effective_user_id_from_token(token)
        user_id = payload.get("user_id")

        if not user_id:
            return jsonify({"error": "Invalid token payload"}), 401

        # =========================
        # GET → Fetch from DB
        # =========================
        if request.method == "GET":
            country = (request.args.get("country") or "uk").strip().lower()

            try:
                objective_month = get_month_start(request.args.get("month"))
            except ValueError as e:
                return jsonify({"error": str(e)}), 400

            objective = UserObjective.query.filter_by(
                user_id=user_id,
                country=country,
                objective_month=objective_month
            ).first()

            if not objective:
                return jsonify({"error": "No data found"}), 404

            return jsonify({
                "type": "website_analysis",
                "country": country,
                "month": objective.objective_month.strftime("%Y-%m"),
                "website": objective.website_url,
                "overview": objective.business_context,
                "ppt_file_name": objective.ppt_file_name,
                "has_ppt": bool(objective.ppt_file_data)
            }), 200

        # =========================
        # POST → Analyze + Save
        # =========================
        elif request.method == "POST":
            country = (request.form.get("country") or "uk").strip().lower()

            try:
                objective_month = get_month_start(request.form.get("month"))
            except ValueError as e:
                return jsonify({"error": str(e)}), 400

            website = request.form.get("website")
            if website:
                website = website.strip()
                if website == "":
                    website = None

            ppt_file = request.files.get("ppt")

            ppt_binary = None
            ppt_filename = None

            if ppt_file and ppt_file.filename:
                ppt_binary = ppt_file.read()
                ppt_filename = ppt_file.filename

                file_ext = os.path.splitext(ppt_filename)[1] or ".pptx"
                with tempfile.NamedTemporaryFile(delete=False, suffix=file_ext) as tmp:
                    tmp.write(ppt_binary)
                    ppt_path = tmp.name

            if not website and not ppt_file:
                return jsonify({
                    "error": "Either website or ppt is required"
                }), 400

            # -------- Analyze safely --------
            if website and ppt_path:
                result = analyze_business_website(website, ppt_path)
            elif website and not ppt_path:
                result = analyze_business_website(website, None)
            elif not website and ppt_path:
                result = analyze_business_website("", ppt_path)
            else:
                return jsonify({
                    "error": "Either website or ppt is required"
                }), 400

            if isinstance(result, dict) and "error" in result:
                return jsonify(result), 400

            overview = result.get("overview") if isinstance(result, dict) else None

            # IMPORTANT: match by user_id + country + objective_month
            objective = UserObjective.query.filter_by(
                user_id=user_id,
                country=country,
                objective_month=objective_month
            ).first()

            if objective:
                if overview:
                    objective.business_context = overview

                if website:
                    objective.website_url = website

                if ppt_binary is not None:
                    objective.ppt_file_data = ppt_binary
                    objective.ppt_file_name = ppt_filename
            else:
                objective = UserObjective(
                    user_id=user_id,
                    country=country,
                    objective_month=objective_month,
                    business_context=overview,
                    website_url=website,
                    ppt_file_data=ppt_binary,
                    ppt_file_name=ppt_filename,
                )
                db.session.add(objective)

            db.session.commit()

            return jsonify({
                "type": "website_analysis",
                "country": country,
                "month": objective_month.strftime("%Y-%m"),
                "website": website,
                "ppt_file_name": ppt_filename,
                "data": result
            }), 200

    except Exception as e:
        db.session.rollback()
        return jsonify({
            "error": "Website analysis failed",
            "details": str(e)
        }), 500

    finally:
        if ppt_path and os.path.exists(ppt_path):
            os.remove(ppt_path)

            