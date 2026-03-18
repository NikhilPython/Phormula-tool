from config import Config
from dotenv import load_dotenv
from app.utils.token_utils import get_effective_user_id_from_token
from flask import Blueprint, Config, jsonify, request
from app.utils.web_scrapping_utils import analyze_business_website
import os
from app.models.user_models import UserObjective
from app import db


website_scrapper_bp = Blueprint("website_scrapper_bp", __name__)

load_dotenv()

# @website_scrapper_bp.route("/analyze-website", methods=["POST"])
# def analyze_website():

#     auth_header = request.headers.get("Authorization")

#     if not auth_header or not auth_header.startswith("Bearer "):
#         return jsonify({"error": "Authorization token is missing or invalid"}), 401

#     token = auth_header.split(" ")[1]

#     try:
#         payload, user_id, member_id = get_effective_user_id_from_token(token)
#         user_id = payload.get("user_id")

#         if not user_id:
#             return jsonify({"error": "Invalid token payload"}), 401

#         # -------- Website param (from form-data now) --------
#         website = request.form.get("website")

#         if not website:
#             return jsonify({"error": "website parameter is required"}), 400

#         # -------- PPT file (optional) --------
#         ppt_file = request.files.get("ppt")
#         ppt_path = None

#         if ppt_file:
#             upload_folder = "uploads"
#             os.makedirs(upload_folder, exist_ok=True)

#             ppt_path = os.path.join(upload_folder, ppt_file.filename)
#             ppt_file.save(ppt_path)

#         # -------- Analyze --------
#         result = analyze_business_website(website, ppt_path)

#         # ✅ cleanup uploaded file
#         if ppt_path and os.path.exists(ppt_path):
#             os.remove(ppt_path)

#         # ✅ If error returned from analyzer
#         if isinstance(result, dict) and "error" in result:
#             return jsonify(result), 400

#         return jsonify({
#             "type": "website_analysis",
#             "website": website,
#             "data": result
#         })

#     except Exception as e:
#         return jsonify({
#             "error": "Website analysis failed",
#             "details": str(e)
#         }), 500
    

@website_scrapper_bp.route("/analyze-website", methods=["POST"])
def analyze_website():

    auth_header = request.headers.get("Authorization")

    if not auth_header or not auth_header.startswith("Bearer "):
        return jsonify({"error": "Authorization token is missing or invalid"}), 401

    token = auth_header.split(" ")[1]

    try:
        payload, user_id, member_id = get_effective_user_id_from_token(token)
        user_id = payload.get("user_id")

        if not user_id:
            return jsonify({"error": "Invalid token payload"}), 401

        # -------- Website --------
        website = request.form.get("website")

        if not website:
            return jsonify({"error": "website parameter is required"}), 400

        # -------- PPT (BYTEA) --------
        ppt_file = request.files.get("ppt")

        ppt_binary = None
        ppt_filename = None
        ppt_path = None

        if ppt_file:
            ppt_binary = ppt_file.read()
            ppt_filename = ppt_file.filename

            import tempfile
            with tempfile.NamedTemporaryFile(delete=False, suffix=".pptx") as tmp:
                tmp.write(ppt_binary)
                ppt_path = tmp.name

        # -------- Analyze --------
        result = analyze_business_website(website, ppt_path)

        if isinstance(result, dict) and "error" in result:
            return jsonify(result), 400

        # -------- Save to DB --------
        overview = result.get("overview")

        objective = UserObjective.query.filter_by(
            user_id=user_id,
            country="uk"
        ).first()

        if objective:
            objective.business_context = overview
            objective.website_url = website
            objective.ppt_file_data = ppt_binary
            objective.ppt_file_name = ppt_filename
        else:
            objective = UserObjective(
                user_id=user_id,
                country="uk",
                business_context=overview,
                website_url=website,
                ppt_file_data=ppt_binary,
                ppt_file_name=ppt_filename
            )
            db.session.add(objective)

        db.session.commit()

        # -------- Cleanup temp file --------
        if ppt_path and os.path.exists(ppt_path):
            os.remove(ppt_path)

        return jsonify({
            "type": "website_analysis",
            "website": website,
            "data": result
        })

    except Exception as e:
        return jsonify({
            "error": "Website analysis failed",
            "details": str(e)
        }), 500