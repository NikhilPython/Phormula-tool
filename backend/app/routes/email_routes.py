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
from app.utils.email_utils import send_email_with_attachment, get_user_email_and_name_by_id, get_user_email_and_name_by_id
# from app.utils.email_utils import (
#     send_daily_inventory_alert_email,
#     get_user_email_and_name_by_id,
# )
# from app.models.user_models import Notification
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
        to_email, user_name = get_user_email_and_name_by_id(user_id)

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
            "promotional_rebates",
            "promotional_rebates_percentage",
            "visible_ads",
            "dealsvouchar_ads",
            "platformfeenew",
            "platform_fee",
            "platform_fee_inventory_storage"
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
            user_name=user_name,
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
    

# @email_bp.route("/test-inventory-alert-email", methods=["POST"])
# def test_inventory_alert_email():
#     auth_header = request.headers.get("Authorization")

#     if not auth_header or not auth_header.startswith("Bearer "):
#         return jsonify({
#             "success": False,
#             "error": "Authorization token is missing or invalid",
#         }), 401

#     token = auth_header.split(" ")[1]

#     try:
#         payload, user_id, member_id = (
#             get_effective_user_id_from_token(token)
#         )
#         user_id = payload["user_id"]

#     except jwt.ExpiredSignatureError:
#         return jsonify({"error": "Token has expired"}), 401

#     except jwt.InvalidTokenError:
#         return jsonify({"error": "Invalid token"}), 401

#     data = request.get_json(silent=True) or {}

#     country = str(
#         data.get("country") or "us"
#     ).strip().lower()

#     if country not in ["us", "uk"]:
#         return jsonify({
#             "success": False,
#             "error": "country must be us or uk",
#         }), 400

#     to_email, user_name = get_user_email_and_name_by_id(
#         user_id
#     )

#     if not to_email:
#         return jsonify({
#             "success": False,
#             "error": "User email not found",
#         }), 404

#     notification_row = Notification.query.filter_by(
#         user_id=user_id,
#         country=country,
#     ).first()

#     if not notification_row:
#         return jsonify({
#             "success": False,
#             "error": (
#                 "Notification data not found. "
#                 "Call /notification first."
#             ),
#         }), 404

#     notification_data = notification_row.data or {}

#     alerts = []

#     for product_name, item in notification_data.items():
#         if not isinstance(item, dict):
#             continue

#         if (
#             str(item.get("alert") or "")
#             .strip()
#             .lower()
#             != "high alert"
#         ):
#             continue

#         alerts.append({
#             "sku": item.get("sku"),
#             "product_name": (
#                 item.get("product_name")
#                 or product_name
#             ),
#             "alert": item.get("alert"),
#             "alert_type": item.get("alert_type"),

#             "inventory_coverage_ratio": (
#                 item.get("inventory_coverage_ratio")
#             ),
#             "future_coverage_ratio": (
#                 item.get("future_coverage_ratio")
#             ),

#             "current_inventory": (
#                 item.get("current_inventory", 0)
#             ),
#             "in_transit": (
#                 item.get("in_transit", 0)
#             ),
#             "inbound_quantity": (
#                 item.get("inbound_quantity", 0)
#             ),
#             "sales_last_30_days": (
#                 item.get("sales_last_30_days", 0)
#             ),

#             "ship_time_weeks": (
#                 item.get("ship_time_weeks")
#             ),
#             "air_time_weeks": (
#                 item.get("air_time_weeks")
#             ),
#             "stock_unit_weeks": (
#                 item.get("stock_unit_weeks")
#             ),

#             "recommendation": (
#                 item.get("recommendation")
#             ),
#             "air_units_required": (
#                 item.get("air_units_required", 0)
#             ),
#             "sea_units_required": (
#                 item.get("sea_units_required", 0)
#             ),
#         })

#     sent = send_daily_inventory_alert_email(
#         to_email=to_email,
#         user_name=user_name,
#         country=country,
#         alerts=alerts,
#     )

#     return jsonify({
#         "success": bool(sent),
#         "sent_to": to_email,
#         "country": country,
#         "alerts_sent": len(alerts),
#     }), 200 if sent else 500



def _clean_demo_field(value, max_length):
    """Normalize a demo-form value and cap its size."""
    return str(value or "").strip()[:max_length]


@email_bp.route("/demo-request", methods=["POST", "OPTIONS"])
def submit_demo_request():
    """Receive the public website demo form and email it to the Phormula team."""
    if request.method == "OPTIONS":
        return "", 204

    try:
        data = request.get_json(silent=True) or {}

        # Honeypot field: real users never fill this field.
        if _clean_demo_field(data.get("website"), 200):
            return jsonify({
                "success": True,
                "message": "Demo request received successfully.",
            }), 200

        full_name = _clean_demo_field(data.get("full_name"), 120)
        work_email = _clean_demo_field(data.get("work_email"), 254).lower()
        company_name = _clean_demo_field(data.get("company_name"), 160)
        monthly_revenue = _clean_demo_field(data.get("monthly_revenue"), 80)
        manual_tracking = _clean_demo_field(data.get("manual_tracking"), 2000)

        if not full_name or not work_email:
            return jsonify({
                "success": False,
                "error": "Full name and work email are required.",
            }), 400

        email_pattern = r"^[A-Za-z0-9.!#$%&'*+/=?^_`{|}~-]+@[A-Za-z0-9-]+(?:\.[A-Za-z0-9-]+)+$"
        if not re.fullmatch(email_pattern, work_email):
            return jsonify({
                "success": False,
                "error": "Please enter a valid work email.",
            }), 400

        recipients = Config.DEMO_FORM_RECIPIENTS
        if not recipients:
            return jsonify({
                "success": False,
                "error": "Demo form recipient is not configured.",
            }), 500

        from flask_mail import Message
        from app import mail

        subject_company = company_name or "Company not provided"
        subject = f"New Phormula demo request - {subject_company}"

        text_body = "\n".join([
            "A new demo request was submitted on the Phormula website.",
            "",
            f"Full name: {full_name}",
            f"Work email: {work_email}",
            f"Company: {company_name or 'Not provided'}",
            f"Monthly revenue range: {monthly_revenue or 'Not provided'}",
            "",
            "Currently tracking manually:",
            manual_tracking or "Not provided",
            "",
            f"Submitted at (UTC): {datetime.utcnow().isoformat(timespec='seconds')}Z",
        ])

        message = Message(
            subject=subject,
            recipients=recipients,
            body=text_body,
            reply_to=work_email,
        )
        mail.send(message)

        return jsonify({
            "success": True,
            "message": "Thank you. Your demo request has been sent to our team.",
        }), 200

    except Exception as exc:
        print(f"[ERROR] /demo-request failed: {exc}")
        return jsonify({
            "success": False,
            "error": "Unable to submit the demo request right now.",
        }), 500
