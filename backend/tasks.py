from datetime import timedelta, datetime
import jwt
from datetime import date
import base64
from app.routes.conversion_rate_routes import ensure_month_seeded, MONTHS_REVERSE_MAP
from app import create_app, db
from config import Config
from app.models.user_models import User
from app.services.live_bi_email_service import build_live_mtd_bi_payload
from app.utils.email_utils import (
    send_live_bi_email,
    get_user_email_by_id,
    has_recent_bi_email,
    mark_bi_email_sent,
)
from app.services.amazon_monthly_sync_service import sync_monthly_transactions_for_user
from app.utils.email_utils import get_user_email_by_id, send_email_with_attachment
from app.services.forecast_service import generate_forecast_for_user
from app.routes.forecast_routes import generate_forecast_core
SECRET_KEY = Config.SECRET_KEY
flask_app = create_app()
celery_app = flask_app.extensions["celery"]
import os
from dotenv import load_dotenv, find_dotenv

dotenv_path = find_dotenv(filename=".env", usecwd=True)
load_dotenv(dotenv_path, override=True)

db_url = os.getenv("DATABASE_URL")
db_url1 = os.getenv("DATABASE_ADMIN_URL") or db_url

#---------------------------------------------  Celery Beat scheduled tasks for live BI email ---------------------------------------------------------#

def get_users_for_live_bi_email():
    try:
        rows = (
            db.session.query(
                User.id.label("user_id"),
                User.email.label("email"),
                User.country.label("country"),
            )
            .filter(User.email.isnot(None))
            .filter(User.amazon_user_exists.is_(True))
            .filter(User.is_verified.is_(True))
            .all()
        )

        users = []
        seen = set()

        for row in rows:
            user_id = row.user_id
            email = row.email
            country = (row.country or "uk").strip().lower()

            key = (user_id, country)
            if not user_id or not email or key in seen:
                continue

            seen.add(key)

            users.append({
                "user_id": user_id,
                "email": email,
                "country": country,
            })

        return users

    except Exception as e:
        print(f"[ERROR] get_users_for_live_bi_email failed: {e}")
        return []
    

@celery_app.task(name="tasks.send_live_bi_email_daily")
def send_live_bi_email_daily():
    with flask_app.app_context():
        try:
            users = get_users_for_live_bi_email()
            processed = set()

            for user in users:
                try:
                    user_id = user["user_id"]
                    country = (user.get("country") or "uk").strip().lower()

                    dedupe_key = (user_id, country)
                    if dedupe_key in processed:
                        print(f"[INFO] Duplicate skipped in same run for user_id={user_id}, country={country}")
                        continue
                    processed.add(dedupe_key)

                    user_email = user.get("email") or get_user_email_by_id(user_id)

                    if not user_email:
                        print(f"[WARN] No email found for user_id={user_id}")
                        continue

                    if has_recent_bi_email(user_id, country, hours=24):
                        print(f"[INFO] Skipping recent BI email for user_id={user_id}, country={country}")
                        continue

                    result = build_live_mtd_bi_payload(
                        user_id=user_id,
                        country=country,
                        as_of=None,
                        start_day=None,
                        end_day=None,
                        generate_ai_insights=False,
                    )

                    if result.get("status") != "ok":
                        print(f"[INFO] Data still loading for user_id={user_id}, country={country}")
                        continue

                    email_token_payload = {
                        "user_id": user_id,
                        "email": user_email,
                        "scope": "live_mtd_bi",
                        "exp": datetime.utcnow() + timedelta(hours=24),
                    }

                    email_token = jwt.encode(
                        email_token_payload,
                        SECRET_KEY,
                        algorithm="HS256",
                    )

                    send_live_bi_email(
                        to_email=user_email,
                        overall_summary=result["overall_summary"],
                        overall_actions=result["overall_actions"],
                        sku_actions=result["recommended_actions_mtd"],
                        sku_to_product=result.get("sku_to_product"),
                        portfolio_recommendation=result.get("portfolio_recommendation"),
                        country=result["country"],
                        prev_label=result["prev_label"],
                        curr_label=result["curr_label"],
                        deep_link_token=email_token,
                    )

                    mark_bi_email_sent(user_id, country)

                except Exception as e:
                    print(f"[ERROR] Failed for user={user}: {e}")

        except Exception as e:
            print(f"[ERROR] send_live_bi_email_daily crashed: {e}")

#---------------------------------------------  Celery Beat scheduled tasks currency rates seeding ---------------------------------------------------------#

@celery_app.task(name="tasks.seed_currency_rates_monthly")
def seed_currency_rates_monthly():
    with flask_app.app_context():
        try:
            today = date.today()
            year = today.year
            month_name = MONTHS_REVERSE_MAP[today.month]

            already_seeded = ensure_month_seeded(
                year=year,
                month=month_name,
            )

            if already_seeded:
                print(f"[INFO] Currency rates already seeded for {month_name} {year}")
            else:
                print(f"[INFO] Currency rates fetched and stored for {month_name} {year}")

        except Exception as e:
            db.session.rollback()
            print(f"[ERROR] seed_currency_rates_monthly failed: {e}")

#---------------------------------------------  Celery Beat scheduled tasks for Amazon monthly sync ---------------------------------------------------------#

def get_users_for_monthly_amazon_sync():
    try:
        rows = (
            db.session.query(
                User.id.label("user_id"),
                User.email.label("email"),
                User.country.label("country"),
            )
            .filter(User.amazon_user_exists.is_(True))
            .filter(User.is_verified.is_(True))
            .all()
        )

        users = []
        seen = set()

        for row in rows:
            user_id = row.user_id
            country = (row.country or "uk").strip().lower()

            key = (user_id, country)
            if not user_id or key in seen:
                continue

            seen.add(key)
            users.append({
                "user_id": user_id,
                "email": row.email,   # ✅ keep email also
                "country": country,
            })

        return users

    except Exception as e:
        print(f"[ERROR] get_users_for_monthly_amazon_sync failed: {e}")
        return []


def _send_monthly_amazon_file_email(*, user_id: int, to_email: str, country: str, year: int, month: int, pipeline_result: dict):
    """
    Decode excel_file from pipeline_result and email it as attachment.
    """
    if not to_email:
        print(f"[WARN] No email found for user_id={user_id}")
        return

    excel_b64 = (pipeline_result or {}).get("excel_file")
    if not excel_b64:
        print(f"[WARN] No excel_file found in pipeline_result for user_id={user_id}")
        return

    try:
        attachment_bytes = base64.b64decode(excel_b64)
    except Exception as e:
        print(f"[ERROR] Failed to decode excel_file for user_id={user_id}: {e}")
        return

    filename = f"amazon_monthly_transactions_{country}_{year}_{month:02d}.xlsx"
    subject = f"Amazon Monthly Transactions - {country.upper()} - {year}-{month:02d}"
    body = (
        f"Hi,\n\n"
        f"Please find attached the Amazon monthly transactions file for "
        f"{country.upper()} ({year}-{month:02d}).\n\n"
        f"Regards,\n"
        f"Skinelements"
    )

    try:
        send_email_with_attachment(
            to_email=to_email,
            subject=subject,
            body=body,
            attachment_bytes=attachment_bytes,
            attachment_filename=filename,
            mime_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
        print(f"[INFO] Email sent successfully to {to_email} for user_id={user_id}")
    except Exception as e:
        print(f"[ERROR] Failed to send monthly file email to {to_email}: {e}")



@celery_app.task(name="tasks.sync_amazon_monthly_transactions")
def sync_amazon_monthly_transactions():
    with flask_app.app_context():
        try:
            today = date.today()

            # always fetch PREVIOUS completed month
            if today.month == 1:
                year = today.year - 1
                month = 12
            else:
                year = today.year
                month = today.month - 1

            print(f"[INFO] Running Amazon monthly sync for year={year}, month={month}")

            users = get_users_for_monthly_amazon_sync()
            processed = set()

            for user in users:
                try:
                    user_id = user["user_id"]
                    email = user.get("email") or get_user_email_by_id(user_id)
                    country = (user.get("country") or "uk").strip().lower()

                    dedupe_key = (user_id, country)
                    if dedupe_key in processed:
                        continue
                    processed.add(dedupe_key)

                    result = sync_monthly_transactions_for_user(
                        user_id=user_id,
                        year=year,
                        month=month,
                        country=country,
                        marketplace_id=None,
                        transaction_status="RELEASED",
                        transaction_type_filter=None,
                        store_in_db=True,
                        run_upload=True,
                        db_url=db_url,
                        db_url_aux=db_url1,
                    )

                    if result.get("success"):
                        print(
                            f"[INFO] Amazon monthly transactions synced for "
                            f"user_id={user_id}, country={country}, year={year}, month={month}, "
                            f"count={result.get('count', 0)}"
                        )

                        # ✅ send fetched file by email
                        pipeline_result = result.get("pipeline_result") or {}
                        _send_monthly_amazon_file_email(
                            user_id=user_id,
                            to_email=email,
                            country=country,
                            year=year,
                            month=month,
                            pipeline_result=pipeline_result,
                        )

                    else:
                        print(
                            f"[ERROR] Amazon monthly sync failed for "
                            f"user_id={user_id}, country={country}: {result}"
                        )

                except Exception as e:
                    print(f"[ERROR] sync_amazon_monthly_transactions failed for user={user}: {e}")

        except Exception as e:
            print(f"[ERROR] sync_amazon_monthly_transactions crashed: {e}")


#---------------------------------------------  Celery Beat scheduled tasks for monthly forecast generation ---------------------------------------------------------#

def get_users_for_monthly_forecast():
    try:
        rows = (
            db.session.query(
                User.id.label("user_id"),
                User.country.label("country"),
            )
            .filter(User.amazon_user_exists.is_(True))
            .filter(User.is_verified.is_(True))
            .all()
        )

        users = []
        seen = set()

        for row in rows:
            user_id = row.user_id
            country = (row.country or "uk").strip().lower()

            key = (user_id, country)
            if not user_id or key in seen:
                continue

            seen.add(key)
            users.append({
                "user_id": user_id,
                "country": country,
            })

        return users

    except Exception as e:
        print(f"[ERROR] get_users_for_monthly_forecast failed: {e}")
        return []
    

@celery_app.task(name="tasks.generate_monthly_forecast_files")
def generate_monthly_forecast_files():
    with flask_app.app_context():
        try:
            today = date.today()
            mv = today.strftime("%B").lower()
            year = str(today.year)

            users = get_users_for_monthly_forecast()
            processed = set()

            for user in users:
                try:
                    user_id = user["user_id"]
                    country = (user.get("country") or "uk").strip().lower()

                    dedupe_key = (user_id, country)
                    if dedupe_key in processed:
                        continue
                    processed.add(dedupe_key)

                    result = generate_forecast_for_user(
                        user_id=user_id,
                        country=country,
                        mv=mv,
                        year=year,
                        send_email=True,
                    )

                    if result.get("success"):
                        print(
                            f"[INFO] Forecast generated for "
                            f"user_id={user_id}, country={country}, month={mv}, year={year}"
                        )
                    else:
                        print(
                            f"[ERROR] Forecast generation failed for "
                            f"user_id={user_id}, country={country}: {result}"
                        )

                except Exception as e:
                    print(f"[ERROR] generate_monthly_forecast_files failed for user={user}: {e}")

        except Exception as e:
            print(f"[ERROR] generate_monthly_forecast_files crashed: {e}")


#---------------------------------------------  Celery Beat scheduled tasks for AI Agent summaries ---------------------------------------------------------#

def get_users_for_agent_schedules():
    try:
        from app.models.user_models import AgentEmailSchedule

        schedules = (
            db.session.query(
                AgentEmailSchedule.user_id,
                AgentEmailSchedule.country,
                AgentEmailSchedule.metric_name,
                AgentEmailSchedule.enabled,
            )
            .filter(AgentEmailSchedule.enabled.is_(True))
            .all()
        )

        return [
            {
                "user_id": s.user_id,
                "country": (s.country or "uk").strip().lower(),
                "metric_name": s.metric_name or "profit",
            }
            for s in schedules
        ]

    except Exception as e:
        print(f"[ERROR] get_users_for_agent_schedules failed: {e}")
        return []


@celery_app.task(name="tasks.run_agent_schedules")
def run_agent_schedules():
    with flask_app.app_context():
        try:
            from app.ai_agent.graph import run_agent_for_schedule

            schedules = get_users_for_agent_schedules()

            processed = set()

            for schedule in schedules:
                try:
                    user_id = schedule["user_id"]
                    country = schedule["country"]
                    metric_name = schedule["metric_name"]

                    dedupe_key = (user_id, country)
                    if dedupe_key in processed:
                        continue
                    processed.add(dedupe_key)

                    run_agent_for_schedule(
                        user_id=user_id,
                        country=country,
                        metric_name=metric_name
                    )

                    print(f"[INFO] Agent schedule run for user_id={user_id}, country={country}")

                except Exception as e:
                    print(f"[ERROR] Agent schedule failed for user={schedule}: {e}")

        except Exception as e:
            print(f"[ERROR] run_agent_schedules crashed: {e}")            
            
