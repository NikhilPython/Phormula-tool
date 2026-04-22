from flask import Blueprint, jsonify, request
import jwt
import pandas as pd
from datetime import datetime

from sqlalchemy import func, case
from sqlalchemy.dialects.postgresql import insert

from config import Config
from app import db
from app.models.user_models import Notification, NotificationAlertSKU
from app.utils.token_utils import get_effective_user_id_from_token
from app.utils.live_bi_utils import (
    generate_inventory_alerts_for_all_skus,
    compute_inventory_coverage_ratio,
)

notification_bp = Blueprint("notification_bp", __name__)
SECRET_KEY = Config.SECRET_KEY


def normalize_sku(value):
    return " ".join(str(value).strip().split())


def save_notification_to_db(user_id, country, success, message, data, full_response):
    existing = Notification.query.filter_by(
        user_id=user_id,
        country=country
    ).first()

    if existing:
        existing.success = success
        existing.message = message
        existing.data = data
        existing.full_response = full_response
    else:
        db.session.add(
            Notification(
                user_id=user_id,
                country=country,
                success=success,
                message=message,
                data=data,
                full_response=full_response
            )
        )


def upsert_high_alert_sku_atomic(
    user_id,
    country,
    sku,
    product_name,
    alert,
    alert_type,
    current_dt
):
    current_date = current_dt.date()
    current_day_name = current_dt.strftime("%A")

    stmt = insert(NotificationAlertSKU).values(
        user_id=user_id,
        country=country,
        sku=sku,
        product_name=product_name,
        alert=alert,
        alert_type=alert_type,
        first_alert_time=current_dt,
        first_alert_date=current_date,
        first_alert_day_name=current_day_name,
        last_alert_time=current_dt,
        last_alert_date=current_date,
        last_alert_day_name=current_day_name,
        days_since_first_alert=0,
        is_active=True,
    )

    stmt = stmt.on_conflict_do_update(
        constraint="uq_notification_alert_user_country_sku",
        set_={
            "product_name": stmt.excluded.product_name,
            "alert": stmt.excluded.alert,
            "alert_type": stmt.excluded.alert_type,
            "last_alert_time": stmt.excluded.last_alert_time,
            "last_alert_date": stmt.excluded.last_alert_date,
            "last_alert_day_name": stmt.excluded.last_alert_day_name,
            "is_active": True,
            # preserve original first alert fields if already present
            "first_alert_time": case(
                (NotificationAlertSKU.first_alert_time.is_(None), stmt.excluded.first_alert_time),
                else_=NotificationAlertSKU.first_alert_time
            ),
            "first_alert_date": case(
                (NotificationAlertSKU.first_alert_date.is_(None), stmt.excluded.first_alert_date),
                else_=NotificationAlertSKU.first_alert_date
            ),
            "first_alert_day_name": case(
                (NotificationAlertSKU.first_alert_day_name.is_(None), stmt.excluded.first_alert_day_name),
                else_=NotificationAlertSKU.first_alert_day_name
            ),
            "days_since_first_alert": case(
                (
                    NotificationAlertSKU.first_alert_date.is_(None),
                    0
                ),
                else_=func.cast(
                    stmt.excluded.last_alert_date - NotificationAlertSKU.first_alert_date,
                    db.Integer
                )
            ),
        }
    ).returning(
        NotificationAlertSKU.id,
        NotificationAlertSKU.user_id,
        NotificationAlertSKU.country,
        NotificationAlertSKU.sku,
        NotificationAlertSKU.product_name,
        NotificationAlertSKU.alert,
        NotificationAlertSKU.alert_type,
        NotificationAlertSKU.first_alert_time,
        NotificationAlertSKU.first_alert_date,
        NotificationAlertSKU.first_alert_day_name,
        NotificationAlertSKU.last_alert_time,
        NotificationAlertSKU.last_alert_date,
        NotificationAlertSKU.last_alert_day_name,
        NotificationAlertSKU.days_since_first_alert,
        NotificationAlertSKU.is_active,
    )

    row = db.session.execute(stmt).mappings().first()
    return dict(row)


@notification_bp.route("/notification", methods=["GET", "POST"])
def notification():
    try:
        auth_header = request.headers.get("Authorization")
        if not auth_header or not auth_header.startswith("Bearer "):
            return jsonify({
                "success": False,
                "error": "Authorization token is missing or invalid"
            }), 401

        token = auth_header.split(" ")[1]

        try:
            payload, user_id, member_id = get_effective_user_id_from_token(token)
            user_id = payload["user_id"]
        except jwt.ExpiredSignatureError:
            return jsonify({
                "success": False,
                "error": "Token has expired"
            }), 401
        except jwt.InvalidTokenError:
            return jsonify({
                "success": False,
                "error": "Invalid token"
            }), 401

        request_data = request.get_json(silent=True) or {}
        country = (
            request_data.get("country")
            or request.args.get("country")
            or request.form.get("country")
            or "uk"
        ).strip().lower()

        alerts = generate_inventory_alerts_for_all_skus(user_id, country)
        coverage_df = compute_inventory_coverage_ratio(user_id, country)

        coverage_map = {
            normalize_sku(row["sku"]): {
                "inventory_coverage_ratio": None
                if pd.isna(row.get("inventory_coverage_ratio"))
                else float(row.get("inventory_coverage_ratio")),
                "product_name": row.get("product_name")
            }
            for _, row in coverage_df.iterrows()
            if row.get("sku") is not None and not pd.isna(row.get("sku"))
        }

        current_dt = datetime.now()
        final_data = {}
        high_alert_seen_skus = set()

        for raw_sku, alert_info in alerts.items():
            sku = normalize_sku(raw_sku)
            sku_data = coverage_map.get(sku, {})

            raw_product_name = sku_data.get("product_name")
            if (
                raw_product_name is None
                or pd.isna(raw_product_name)
                or str(raw_product_name).strip() == ""
            ):
                product_name = sku
            else:
                product_name = str(raw_product_name).strip()

            alert_value = alert_info.get("alert")
            alert_type = alert_info.get("alert_type")
            inventory_coverage_ratio = sku_data.get("inventory_coverage_ratio")

            item_payload = {
                "sku": sku,
                "product_name": product_name,
                "inventory_coverage_ratio": inventory_coverage_ratio,
                "alert": alert_value,
                "alert_type": alert_type,
            }

            if str(alert_value).strip().lower() == "high alert":
                high_alert_seen_skus.add(sku)

                row = upsert_high_alert_sku_atomic(
                    user_id=user_id,
                    country=country,
                    sku=sku,
                    product_name=product_name,
                    alert=alert_value,
                    alert_type=alert_type,
                    current_dt=current_dt
                )

                item_payload["first_alert_time"] = (
                    row["first_alert_time"].isoformat()
                    if row["first_alert_time"] else None
                )
                item_payload["first_alert_date"] = (
                    str(row["first_alert_date"]) if row["first_alert_date"] else None
                )
                item_payload["first_alert_day_name"] = row["first_alert_day_name"]
                item_payload["last_alert_time"] = (
                    row["last_alert_time"].isoformat()
                    if row["last_alert_time"] else None
                )
                item_payload["last_alert_date"] = (
                    str(row["last_alert_date"]) if row["last_alert_date"] else None
                )
                item_payload["last_alert_day_name"] = row["last_alert_day_name"]
                item_payload["days_since_first_alert"] = row["days_since_first_alert"]

            final_data[product_name] = item_payload

        # Mark rows inactive if not high alert in current response
        if high_alert_seen_skus:
            deactivate_stmt = (
                NotificationAlertSKU.__table__.update()
                .where(NotificationAlertSKU.user_id == user_id)
                .where(NotificationAlertSKU.country == country)
                .where(NotificationAlertSKU.is_active.is_(True))
                .where(NotificationAlertSKU.sku.not_in(list(high_alert_seen_skus)))
                .values(is_active=False)
            )
        else:
            deactivate_stmt = (
                NotificationAlertSKU.__table__.update()
                .where(NotificationAlertSKU.user_id == user_id)
                .where(NotificationAlertSKU.country == country)
                .where(NotificationAlertSKU.is_active.is_(True))
                .values(is_active=False)
            )

        db.session.execute(deactivate_stmt)

        response_payload = {
            "success": True,
            "message": "Notifications fetched successfully",
            "user_id": user_id,
            "country": country,
            "data": final_data
        }

        save_notification_to_db(
            user_id=user_id,
            country=country,
            success=True,
            message="Notifications fetched successfully",
            data=final_data,
            full_response=response_payload
        )

        db.session.commit()

        return jsonify(response_payload), 200

    except Exception as e:
        db.session.rollback()
        print(f"[ERROR] /notification failed: {e}")
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500
    
