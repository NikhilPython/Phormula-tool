from __future__ import annotations

import os
import json
import re
import jwt
import pandas as pd
from io import BytesIO
from datetime import datetime
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from flask import Blueprint, jsonify, request, make_response

from config import Config
from app.utils.token_utils import get_effective_user_id_from_token
from app.utils.email_utils import send_email_with_attachment, get_user_email_and_name_by_id
from app.utils.live_bi_utils import fetch_user_objective
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
    previous = datetime(now.year, now.month, 1) - pd.Timedelta(days=1)
    return previous.strftime("%B").lower(), previous.year


def _read_monthly_recap_request():
    """
    Accept either application/json or multipart/form-data.
    monthly_recap may arrive as a JSON object or a JSON-encoded string.
    """
    if request.is_json:
        data = request.get_json(silent=True) or {}
    else:
        data = request.form.to_dict()

    recap = data.get("monthly_recap")

    if isinstance(recap, str):
        try:
            recap = json.loads(recap)
        except json.JSONDecodeError as exc:
            raise ValueError("monthly_recap must contain valid JSON.") from exc

    if recap is None:
        return data, None, None

    if not isinstance(recap, dict):
        raise ValueError("monthly_recap must be a JSON object.")

    required = ("monthLabel", "actual", "target", "metrics", "skuHighlights", "objectives")
    missing = [key for key in required if key not in recap]
    if missing:
        raise ValueError(
            "monthly_recap is missing required fields: " + ", ".join(missing)
        )

    if not isinstance(recap["metrics"], list):
        raise ValueError("monthly_recap.metrics must be an array.")
    if not isinstance(recap["skuHighlights"], list):
        raise ValueError("monthly_recap.skuHighlights must be an array.")
    if not isinstance(recap["objectives"], list):
        raise ValueError("monthly_recap.objectives must be an array.")

    month_label = str(recap.get("monthLabel") or "").strip()
    parsed_month = None
    for month_format in ("%B %Y", "%b %Y"):
        try:
            parsed_month = datetime.strptime(month_label, month_format)
            break
        except ValueError:
            pass

    if parsed_month is None:
        raise ValueError(
            "monthly_recap.monthLabel must look like 'October 2026'."
        )

    # Keep only fields used by the email renderer; no React functions cross JSON.
    clean_metrics = []
    for metric in recap["metrics"]:
        if not isinstance(metric, dict):
            continue
        clean_metrics.append({
            "label": str(metric.get("label") or ""),
            "value": metric.get("value"),
            "previous": metric.get("previous"),
            "deltaPct": metric.get("deltaPct"),
            "inverseDelta": bool(metric.get("inverseDelta", False)),
        })

    clean_skus = []
    for sku in recap["skuHighlights"]:
        if not isinstance(sku, dict):
            continue
        clean_skus.append({
            "title": sku.get("title"),
            "name": sku.get("name"),
            "sku": sku.get("sku"),
            "netSales": sku.get("netSales"),
            "netSalesChange": sku.get("netSalesChange"),
            "netSalesDeltaPct": sku.get("netSalesDeltaPct"),
            "units": sku.get("units"),
            "asp": sku.get("asp"),
            "cm2Profit": sku.get("cm2Profit"),
        })

    clean_recap = {
        "monthLabel": month_label,
        "actual": recap.get("actual"),
        "target": recap.get("target"),
        "objectives": [str(item) for item in recap["objectives"] if item is not None],
        "metrics": clean_metrics,
        "skuHighlights": clean_skus,
        "currencySymbol": str(recap.get("currencySymbol") or "")[:4],
    }

    return data, clean_recap, parsed_month


def _build_monthly_recap_from_report(df, user_id, country, month_name, year):
    """Recreate the dashboard recap from the current and previous monthly SKU tables."""
    def number(row, *columns):
        if row is None:
            return None
        for column in columns:
            if column in row and pd.notna(row[column]):
                try:
                    return float(row[column])
                except (TypeError, ValueError):
                    continue
        return None

    def split_total(frame):
        total_mask = pd.Series(False, index=frame.index)
        if "sku" in frame.columns:
            total_mask |= frame["sku"].astype(str).str.strip().str.upper().isin({"TOTAL", "GRAND_TOTAL"})
        for name_col in ("product_name", "product_title", "name"):
            if name_col in frame.columns:
                total_mask |= frame[name_col].astype(str).str.strip().str.lower().isin({"total", "grand total"})
        total = frame.loc[total_mask].iloc[0] if total_mask.any() else None
        products = frame.loc[~total_mask] if total_mask.any() else frame
        return total, products

    current_total, current_products = split_total(df)
    month_number = datetime.strptime(month_name, "%B").month
    previous_date = datetime(year, month_number, 1) - pd.Timedelta(days=1)
    previous_table = f"skuwisemonthly_{user_id}_{country}_{previous_date.strftime('%B').lower()}{previous_date.year}"
    try:
        previous_df = pd.read_sql(f'SELECT * FROM public."{previous_table}" ORDER BY id ASC', engine)
        previous_total, previous_products = split_total(previous_df)
    except Exception:
        previous_total, previous_products = None, pd.DataFrame()

    def current_value(*columns):
        value = number(current_total, *columns)
        if value is not None:
            return value
        for column in columns:
            if column in current_products.columns:
                values = pd.to_numeric(current_products[column], errors="coerce").dropna()
                if not values.empty:
                    return float(values.sum())
        return None

    def previous_value(*columns):
        value = number(previous_total, *columns)
        if value is not None:
            return value
        for column in columns:
            if column in previous_products.columns:
                values = pd.to_numeric(previous_products[column], errors="coerce").dropna()
                if not values.empty:
                    return float(values.sum())
        return None

    actual = current_value("net_sales", "total_net_sales") or 0
    units = current_value("total_quantity", "net_quantity", "quantity", "units") or 0
    asp = current_value("asp")
    if asp is None and units:
        asp = actual / units
    current_metrics = [
        ("Units", units, ("total_quantity", "net_quantity", "quantity", "units"), False),
        ("ASP", asp, ("asp",), False),
        ("Net Sales", actual, ("net_sales", "total_net_sales"), False),
        ("Marketplace Fees", abs(current_value("amazon_fees", "amazon_fee", "total_amazon_fees") or 0), ("amazon_fees", "amazon_fee", "total_amazon_fees"), True),
        ("TACoS", current_value("tacos_total_advertising_cost_of_sale", "tacos"), ("tacos_total_advertising_cost_of_sale", "tacos"), True),
        ("CM2 Profit", current_value("total_cm2_profit", "cm2_profit"), ("total_cm2_profit", "cm2_profit"), False),
    ]
    metrics = []
    for label, value, columns, inverse in current_metrics:
        prior = previous_value(*columns)
        if label == "Marketplace Fees" and prior is not None:
            prior = abs(prior)
        delta = ((value - prior) / abs(prior) * 100) if value is not None and prior not in (None, 0) else None
        metrics.append({"label": label, "value": value, "previous": prior, "deltaPct": delta, "inverseDelta": inverse})

    def sku_key(row):
        sku = str(row.get("sku") or "").strip().lower()
        return sku or str(row.get("product_name") or row.get("product_title") or row.get("name") or "").strip().lower()

    previous_by_sku = {sku_key(row): row for _, row in previous_products.iterrows() if sku_key(row)}
    name_col = next((c for c in ("product_name", "product_title", "name") if c in current_products.columns), None)
    ranked = []
    if name_col and "net_sales" in current_products.columns:
        for _, row in current_products.iterrows():
            net_sales = number(row, "net_sales")
            if net_sales is None:
                continue
            old = previous_by_sku.get(sku_key(row))
            old_sales = number(old, "net_sales") if old is not None else None
            change = net_sales - old_sales if old_sales is not None else None
            cm2_values = [number(row, column) for column in ("cm2_profit", "total_cm2_profit", "profit")]
            cm2_profit = next((value for value in cm2_values if value not in (None, 0)), None)
            if cm2_profit is None:
                cm2_profit = next((value for value in cm2_values if value is not None), None)
            ranked.append({
                "name": str(row.get(name_col) or ""),
                "sku": str(row.get("sku") or ""),
                "netSales": net_sales,
                "netSalesChange": change,
                "netSalesDeltaPct": (change / abs(old_sales) * 100) if change is not None and old_sales else None,
                "units": number(row, "total_quantity", "net_quantity", "quantity", "units") or 0,
                "asp": number(row, "asp"),
                "cm2Profit": cm2_profit,
            })
    top = sorted(ranked, key=lambda item: item["netSales"], reverse=True)
    growth = sorted((item for item in ranked if item["netSalesChange"] is not None and item["netSalesChange"] > 0), key=lambda item: item["netSalesChange"], reverse=True)
    declines = sorted((item for item in ranked if item["netSalesChange"] is not None and item["netSalesChange"] < 0), key=lambda item: item["netSalesChange"])
    highlights = []
    if top:
        highlights.append({**top[0], "title": "Top Selling SKU"})
    if growth:
        highlights.append({**growth[0], "title": "Highest Net Sales Growth"})
    if declines:
        highlights.append({**declines[0], "title": "Largest Net Sales Decline"})

    target = 0
    try:
        with engine.connect() as conn:
            target_row = conn.execute(text("""
                SELECT target_sales FROM target_data
                WHERE user_id = :user_id AND LOWER(month) = LOWER(:month)
                  AND year = :year AND LOWER(country) = LOWER(:country)
                ORDER BY updated_at DESC LIMIT 1
            """), {"user_id": user_id, "month": month_name, "year": year, "country": country}).fetchone()
        target = float(target_row[0]) if target_row and target_row[0] is not None else 0
    except Exception:
        pass

    try:
        objective = fetch_user_objective(user_id, country) or {}
    except Exception:
        objective = {}
    title_case = lambda value: re.sub(r"\b\w", lambda match: match.group(0).upper(), re.sub(r"[_-]+", " ", str(value or "")).strip())
    objectives = []
    if objective.get("growth_intent"):
        objectives.append(f"Growth focus: {title_case(objective['growth_intent'])}")
    if objective.get("profit_priority"):
        objectives.append(f"Profit focus: {title_case(objective['profit_priority'])}")
    clearance = objective.get("inventory_clearance_priority")
    if clearance is not None:
        objectives.append("Prioritise clearing ageing inventory" if clearance else "Inventory dilution: No")

    return {
        "monthLabel": f"{month_name.capitalize()} {year}",
        "actual": actual,
        "target": target,
        "objectives": objectives[:4],
        "metrics": metrics,
        "skuHighlights": highlights,
        "currencySymbol": "$" if country == "us" else "£",
    }


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

        # 3) Read country and an optional dashboard recap. Country-only API
        # requests are completed from the user's monthly dashboard data below.
        try:
            data, monthly_recap, recap_period = _read_monthly_recap_request()
        except ValueError as validation_error:
            response = jsonify({
                "success": False,
                "error": str(validation_error)
            })
            response.headers["Access-Control-Allow-Origin"] = request.headers.get("Origin", "*")
            return response, 400

        country = str(
            data.get("country")
            or request.args.get("country")
            or "uk"
        ).strip().lower()

        if country not in {"us", "uk"}:
            response = jsonify({
                "success": False,
                "error": "country must be 'us' or 'uk'"
            })
            response.headers["Access-Control-Allow-Origin"] = request.headers.get("Origin", "*")
            return response, 400

        if recap_period is None:
            month_name, year = get_previous_month_year()
        else:
            month_name = recap_period.strftime("%B").lower()
            year = recap_period.year

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

        if monthly_recap is None:
            monthly_recap = _build_monthly_recap_from_report(
                df, user_id, country, month_name, year
            )

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
            monthly_recap=monthly_recap,
        )

        response = jsonify({
            "success": True,
            "message": "Email sent successfully",
            "sent_to": to_email,
            "table_name": table_name,
            "file_name": attachment_filename,
            "monthly_recap": monthly_recap,
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
