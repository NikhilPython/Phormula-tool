from __future__ import annotations

import os
import re
import jwt
import pandas as pd
from io import BytesIO
from datetime import datetime
from dotenv import load_dotenv
from sqlalchemy import create_engine
from flask import Blueprint, jsonify, request, make_response

from config import Config
from app.utils.token_utils import get_effective_user_id_from_token
from app.utils.email_utils import send_email_with_attachment, get_user_email_by_id

load_dotenv()

db_url = os.getenv("DATABASE_URL")
engine = create_engine(db_url)
SECRET_KEY = Config.SECRET_KEY

email_bp = Blueprint("email_bp", __name__)


def get_previous_month_year():
    now = datetime.utcnow()

    if now.month == 1:
        prev_month = 12
        prev_year = now.year - 1
    else:
        prev_month = now.month - 1
        prev_year = now.year

    month_name = datetime(prev_year, prev_month, 1).strftime("%B").lower()
    return month_name, prev_year


@email_bp.route("/send-report-email", methods=["POST", "OPTIONS"])
def send_report_email():
    if request.method == "OPTIONS":
        response = make_response("", 200)
        response.headers["Access-Control-Allow-Origin"] = request.headers.get("Origin", "*")
        response.headers["Access-Control-Allow-Headers"] = "Content-Type, Authorization"
        response.headers["Access-Control-Allow-Methods"] = "POST, OPTIONS"
        return response

    try:
        # 1) Authorization
        auth_header = request.headers.get("Authorization")
        if not auth_header or not auth_header.startswith("Bearer "):
            response = jsonify({
                "success": False,
                "error": "Authorization token is missing or invalid"
            })
            response.headers["Access-Control-Allow-Origin"] = request.headers.get("Origin", "*")
            return response, 401

        token = auth_header.split(" ")[1]

        try:
            payload, user_id, member_id = get_effective_user_id_from_token(token)
            user_id = payload["user_id"]
        except jwt.ExpiredSignatureError:
            response = jsonify({"success": False, "error": "Token has expired"})
            response.headers["Access-Control-Allow-Origin"] = request.headers.get("Origin", "*")
            return response, 401
        except jwt.InvalidTokenError:
            response = jsonify({"success": False, "error": "Invalid token"})
            response.headers["Access-Control-Allow-Origin"] = request.headers.get("Origin", "*")
            return response, 401

        # 2) Recipient email
        to_email = get_user_email_by_id(user_id)
        if not to_email:
            response = jsonify({
                "success": False,
                "error": "User email not found"
            })
            response.headers["Access-Control-Allow-Origin"] = request.headers.get("Origin", "*")
            return response, 404

        # 3) Get country from request
        data = request.get_json(silent=True) or {}
        country = (
            data.get("country")
            or request.args.get("country")
            or request.form.get("country")
            or "uk"
        ).strip().lower()

        # allow only safe table-name characters
        if not re.fullmatch(r"[a-z0-9_]+", country):
            response = jsonify({
                "success": False,
                "error": "Invalid country value"
            })
            response.headers["Access-Control-Allow-Origin"] = request.headers.get("Origin", "*")
            return response, 400

        # 4) Previous month logic
        month_name, year = get_previous_month_year()

        # table format: skuwisemonthly_{user_id}_{country}_{month}{year}
        table_name = f"skuwisemonthly_{user_id}_{country}_{month_name}{year}"

        # 5) Read table
        query = f'SELECT * FROM public."{table_name}" ORDER BY id ASC'
        df = pd.read_sql(query, engine)

        # Fix negative values
        columns_to_fix = [
            "tax_and_credits",
            "fba_fees",
            "lost_total",
            "promotional_rebates"
        ]

        for col in columns_to_fix:
            if col in df.columns:
                df[col] = df[col].abs()

        # Remove user_id column from email attachment
        if "user_id" in df.columns:
            df = df.drop(columns=["user_id"])

        if df.empty:
            response = jsonify({
                "success": False,
                "error": f"No data found in table {table_name}"
            })
            response.headers["Access-Control-Allow-Origin"] = request.headers.get("Origin", "*")
            return response, 404

        # 6) Create Excel in memory
        output = BytesIO()

        sheet_name = "Monthly P&L Report"
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name=sheet_name)

            ws = writer.sheets[sheet_name]

            for column_cells in ws.columns:
                max_length = 0
                col_letter = column_cells[0].column_letter

                for cell in column_cells:
                    cell_value = "" if cell.value is None else str(cell.value)
                    max_length = max(max_length, len(cell_value))

                ws.column_dimensions[col_letter].width = min(max_length + 2, 30)

        output.seek(0)
        attachment_bytes = output.getvalue()

        # 7) File name / subject
        display_month = month_name.capitalize()
        display_country = country.upper()

        attachment_filename = f"Monthly P&L Report {display_country} {display_month} {year}.xlsx"
        subject = f"Monthly P&L Report {display_country} {display_month} {year}"
        body = f"Please find attached the Monthly P&L Report {display_country} {display_month} {year}."

        mime_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"

        # 8) Send email
        send_email_with_attachment(
            to_email=to_email,
            subject=subject,
            body=body,
            attachment_bytes=attachment_bytes,
            attachment_filename=attachment_filename,
            mime_type=mime_type,
        )

        response = jsonify({
            "success": True,
            "message": "Email sent successfully",
            "sent_to": to_email,
            "table_name": table_name,
            "file_name": attachment_filename
        })
        response.headers["Access-Control-Allow-Origin"] = request.headers.get("Origin", "*")
        return response, 200

    except Exception as e:
        print(f"[ERROR] /send-report-email failed: {e}")
        response = jsonify({
            "success": False,
            "error": str(e)
        })
        response.headers["Access-Control-Allow-Origin"] = request.headers.get("Origin", "*")
        return response, 500 