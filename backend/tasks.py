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


from sqlalchemy import text
import pandas as pd
import io
import base64

def _send_monthly_amazon_file_email(*, user_id: int, to_email: str, country: str, year: int, month: int, pipeline_result: dict):

    if not to_email:
        print(f"[WARN] No email found for user_id={user_id}")
        return

    try:
        # 🔥 Build correct table name
        month_name = datetime(year, month, 1).strftime("%B").lower()
        table_name = f"skuwisemonthly_{user_id}_{country}_{month_name}{year}"  

        query = text(f'SELECT * FROM public."{table_name}"')

        df = pd.read_sql(query, db.engine)

        if df.empty:
            print(f"[WARN] SKU-wise table empty for user_id={user_id}")
            return

        # 🔥 Convert to Excel
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
            df.to_excel(writer, index=False, sheet_name="SKUWise")

        output.seek(0)
        attachment_bytes = output.read()

    except Exception as e:
        print(f"[ERROR] Failed to fetch SKU-wise table: {e}")
        return

    # ✅ Updated filename + subject
    filename = f"skuwise_monthly_{country}_{year}_{month:02d}.xlsx"

    subject = f"Amazon SKU-wise Report - {country.upper()} - {year}-{month:02d}"

    body = (
        f"Hi,\n\n"
        f"Please find attached the Amazon SKU-wise performance report for "
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

        print(f"[INFO] SKU-wise email sent to {to_email}")

    except Exception as e:
        print(f"[ERROR] Email send failed: {e}")




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

def should_run_now(schedule, now):
    # -----------------------
    # TIME MATCH
    # -----------------------
    if now.hour != schedule.preferred_hour:
        return False

    if abs(now.minute - schedule.preferred_minute) > 5:
        return False

    # -----------------------
    # DUPLICATE PROTECTION
    # -----------------------
    if schedule.last_run_at:
        last = schedule.last_run_at

        # weekly → only once per week
        if schedule.frequency == "weekly":
            if (
                last.isocalendar().year == now.isocalendar().year
                and last.isocalendar().week == now.isocalendar().week
            ):
                return False

        # monthly → only once per month
        if schedule.frequency == "monthly":
            if last.year == now.year and last.month == now.month:
                return False

    # -----------------------
    # WEEKLY
    # -----------------------
    if schedule.frequency == "weekly":
        return now.weekday() == 0  # Monday

    # -----------------------
    # MONTHLY
    # -----------------------
    if schedule.frequency == "monthly":
        if getattr(schedule, "day_of_month", None):
            return now.day == schedule.day_of_month
        return now.day == 1

    return False

@celery_app.task(name="tasks.run_agent_schedules")
def run_agent_schedules():
    with flask_app.app_context():
        try:
            from datetime import datetime
            from app.models.user_models import AgentEmailSchedule
            from app.ai_agent.service import run_agent
            from app.ai_agent.email_service import send_agent_email
            from app import db

            now = datetime.now()

            # 🔥 FETCH FULL SCHEDULES
            schedules = AgentEmailSchedule.query.filter_by(enabled=True).all()

            for schedule in schedules:
                try:
                    # -----------------------
                    # STEP 1: SHOULD RUN?
                    # -----------------------
                    if not should_run_now(schedule, now):
                        continue

                    # -----------------------
                    # STEP 2: VALIDATE QUERY
                    # -----------------------
                    if not schedule.query:
                        print(f"[WARN] Missing query for schedule_id={schedule.id}")
                        continue

                    # -----------------------
                    # STEP 3: RUN AGENT
                    # -----------------------
                    result = run_agent(
                        user_id=schedule.user_id,
                        country=schedule.country,
                        user_query=schedule.query
                    )

                    # -----------------------
                    # STEP 4: FORMAT EMAIL
                    # -----------------------
                    response_text = result.get("response", "")

                    html_body = f"""
                    <html><body style='font-family:Arial,sans-serif;color:#1f2937;'>
                    <p>{response_text.replace(chr(10), '<br>')}</p>
                    </body></html>
                    """

                    # -----------------------
                    # STEP 5: SEND EMAIL
                    # -----------------------
                    subject = f"Scheduled Report - {schedule.query[:60]}"

                    send_agent_email(
                        user_id=schedule.user_id,
                        subject=subject,
                        html_body=html_body
                    )

                    # -----------------------
                    # STEP 6: MARK AS SENT
                    # -----------------------
                    schedule.last_run_at = now
                    db.session.add(schedule)
                    db.session.commit()

                    print(
                        f"[INFO] Scheduled email sent → user={schedule.user_id}, schedule_id={schedule.id}"
                    )

                except Exception as e:
                    db.session.rollback()
                    print(f"[ERROR] Schedule failed id={schedule.id}: {e}")

        except Exception as e:
            print(f"[ERROR] run_agent_schedules crashed: {e}")

            