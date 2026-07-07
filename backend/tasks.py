from datetime import timedelta, datetime
import jwt
from datetime import date
import base64
import requests
from urllib.parse import urlencode
from app.routes.conversion_rate_routes import ensure_month_seeded, MONTHS_REVERSE_MAP
from app import create_app, db
from config import Config
from app.models.user_models import User, amazon_user as AmazonUser
from app.services.live_bi_email_service import build_live_mtd_bi_payload
from app.utils.email_utils import (
    send_live_bi_email,
    get_user_email_and_name_by_id,
    has_recent_bi_email,
    mark_bi_email_sent,
)
from app.services.amazon_monthly_sync_service import sync_monthly_transactions_for_user
from app.utils.email_utils import get_user_email_and_name_by_id, send_email_with_attachment
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
        amazon_rows = (
            db.session.query(
                AmazonUser.user_id.label("user_id"),
                AmazonUser.country_name.label("country_name"),
                AmazonUser.country_code.label("country_code"),
                AmazonUser.marketplace_id.label("marketplace_id"),
            )
            .filter(AmazonUser.user_id.isnot(None))
            .all()
        )

        user_ids = list({row.user_id for row in amazon_rows if row.user_id})

        if not user_ids:
            return []

        email_rows = (
            db.session.query(
                User.id.label("user_id"),
                User.email.label("email"),
            )
            .filter(User.id.in_(user_ids))
            .filter(User.email.isnot(None))
            .filter(User.is_verified.is_(True))
            .all()
        )

        email_map = {row.user_id: row.email for row in email_rows}

        users = []
        seen = set()

        for row in amazon_rows:
            user_id = row.user_id
            marketplace_id = row.marketplace_id
            email = email_map.get(user_id)

            if not email:
                print(f"[WARN] Skipping unverified/missing email user_id={user_id}")
                continue

            country = (
                row.country_name
                or row.country_code
                or MARKETPLACE_COUNTRY_MAP.get(marketplace_id)
                or ""
            )

            country = str(country).strip().lower()

            if country in ("gb", "uk", "united kingdom"):
                country = "uk"
            elif country in ("us", "usa", "united states", "na"):
                country = "us"

            if country not in ("uk", "us"):
                print(
                    f"[WARN] Skipping unsupported live BI country: "
                    f"user_id={user_id}, marketplace_id={marketplace_id}, country={country}"
                )
                continue

            key = (user_id, country)
            if key in seen:
                continue

            seen.add(key)

            users.append({
                "user_id": user_id,
                "email": email,
                "country": country,
            })

        print(f"[INFO] Live BI users selected from amazon_user: {users}")
        return users

    except Exception as e:
        db.session.rollback()
        print(f"[ERROR] get_users_for_live_bi_email failed: {repr(e)}")
        return []
    

@celery_app.task(name="tasks.send_live_bi_email_daily")
def send_live_bi_email_daily(country_filter=None):
    with flask_app.app_context():
        try:
            users = get_users_for_live_bi_email()
            processed = set()

            for user in users:
                try:
                    user_id = user["user_id"]
                    country = (user.get("country") or "uk").strip().lower()
                    if country_filter and country != country_filter.lower():
                        continue

                    dedupe_key = (user_id, country)
                    if dedupe_key in processed:
                        print(f"[INFO] Duplicate skipped in same run for user_id={user_id}, country={country}")
                        continue
                    processed.add(dedupe_key)

                    user_email = user.get("email") or get_user_email_and_name_by_id(user_id)

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
MARKETPLACE_COUNTRY_MAP = {
    "A1F83G8C2ARO7P": "uk",
    "ATVPDKIKX0DER": "us",
}

def get_users_for_monthly_amazon_sync():
    try:
        amazon_rows = (
            db.session.query(
                AmazonUser.user_id.label("user_id"),
                AmazonUser.country_code.label("country"),
                AmazonUser.marketplace_id.label("marketplace_id"),
            )
            .filter(AmazonUser.refresh_token.isnot(None))
            .filter(AmazonUser.marketplace_id.isnot(None))
            .all()
        )

        user_ids = list({row.user_id for row in amazon_rows if row.user_id})

        if not user_ids:
            return []

        email_rows = (
            db.session.query(
                User.id.label("user_id"),
                User.email.label("email"),
            )
            .filter(User.id.in_(user_ids))
            .filter(User.is_verified.is_(True))
            .all()
        )

        email_map = {row.user_id: row.email for row in email_rows}

        users = []
        seen = set()

        for row in amazon_rows:
            marketplace_id = row.marketplace_id
            country = (row.country or "").strip().lower()

            if country in ("gb", "uk"):
                country = "uk"
            elif country in ("us", "usa", "na"):
                country = "us"

            if not country:
                country = MARKETPLACE_COUNTRY_MAP.get(marketplace_id)

            if country not in ("uk", "us"):
                print(
                    f"[WARN] Skipping unsupported marketplace/country: "
                    f"user_id={row.user_id}, marketplace_id={marketplace_id}, country={country}"
                )
                continue

            email = email_map.get(row.user_id)

            if not email:
                print(f"[WARN] Skipping unverified/missing user_id={row.user_id}")
                continue

            key = (row.user_id, marketplace_id)

            if key in seen:
                continue

            seen.add(key)

            users.append({
                "user_id": row.user_id,
                "email": email,
                "country": country,
                "marketplace_id": marketplace_id,
            })

        return users

    except Exception as e:
        db.session.rollback()
        print(f"[ERROR] get_users_for_monthly_amazon_sync failed: {repr(e)}")
        return []
         

from sqlalchemy import text
import pandas as pd
import io
import base64

def _send_monthly_amazon_file_email(
    *,
    user_id: int,
    to_email: str,
    country: str,
    year: int,
    month: int,
    pipeline_result: dict,
):
    if not to_email:
        print(f"[WARN] No email found for user_id={user_id}, country={country}")
        return

    try:
        month_name = datetime(year, month, 1).strftime("%B").lower()
        table_name = f"skuwisemonthly_{user_id}_{country}_{month_name}{year}"

        query = text(f'SELECT * FROM public."{table_name}"')
        df = pd.read_sql(query, db.engine)

        if df.empty:
            print(
                f"[WARN] SKU-wise table empty for "
                f"user_id={user_id}, country={country}, table={table_name}"
            )
            return

        output = io.BytesIO()
        with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
            df.to_excel(writer, index=False, sheet_name="SKUWise")

        output.seek(0)
        attachment_bytes = output.read()

    except Exception as e:
        db.session.rollback()
        print(
            f"[ERROR] Failed to fetch SKU-wise table for "
            f"user_id={user_id}, country={country}: {repr(e)}"
        )
        return

    filename = f"Monthly P&L Report {country.upper()} {year}-{month:02d}.xlsx"
    subject = f"Monthly P&L Report - {country.upper()} - {year}-{month:02d}"

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

        print(
            f"[INFO] SKU-wise email sent to {to_email} "
            f"for user_id={user_id}, country={country}"
        )

    except Exception as e:
        db.session.rollback()
        print(
            f"[ERROR] Email send failed for "
            f"user_id={user_id}, country={country}: {repr(e)}"
        )




@celery_app.task(name="tasks.sync_amazon_monthly_transactions")
def sync_amazon_monthly_transactions(country_filter=None):
    with flask_app.app_context():
        try:
            today = date.today()

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
                    email = user.get("email") or get_user_email_and_name_by_id(user_id)
                    country = (user.get("country") or "").strip().lower()
                    if country_filter and country != country_filter.lower():
                        continue
                    marketplace_id = user.get("marketplace_id")

                    dedupe_key = (user_id, marketplace_id)

                    if dedupe_key in processed:
                        print(
                            f"[INFO] Duplicate skipped for "
                            f"user_id={user_id}, marketplace_id={marketplace_id}"
                        )
                        continue

                    processed.add(dedupe_key)

                    print(
                        f"[INFO] Sync started for user_id={user_id}, "
                        f"country={country}, marketplace_id={marketplace_id}"
                    )

                    result = sync_monthly_transactions_for_user(
                        user_id=user_id,
                        year=year,
                        month=month,
                        country=country,
                        marketplace_id=marketplace_id,
                        transaction_status="RELEASED",
                        transaction_type_filter=None,
                        store_in_db=True,
                        run_upload=True,
                        db_url=db_url,
                        db_url_aux=db_url1,
                    )

                    if not result.get("success"):
                        raise Exception(f"Pipeline failed: {result}")

                    print(
                        f"[INFO] Amazon monthly transactions synced for "
                        f"user_id={user_id}, country={country}, "
                        f"marketplace_id={marketplace_id}, year={year}, month={month}, "
                        f"count={result.get('count', 0)}"
                    )

                    _send_monthly_amazon_file_email(
                        user_id=user_id,
                        to_email=email,
                        country=country,
                        year=year,
                        month=month,
                        pipeline_result=result.get("pipeline_result") or {},
                    )

                except Exception as e:
                    db.session.rollback()
                    print(f"[ERROR] sync failed for user={user}: {repr(e)}")
                    continue

        except Exception as e:
            db.session.rollback()
            print(f"[ERROR] sync_amazon_monthly_transactions crashed: {repr(e)}")

        finally:
            db.session.remove()

#---------------------------------------------  Celery Beat scheduled tasks for monthly forecast generation ---------------------------------------------------------#

def get_users_for_monthly_forecast():
    try:
        amazon_rows = (
            db.session.query(
                AmazonUser.user_id.label("user_id"),
                AmazonUser.country_name.label("country"),
                AmazonUser.country_code.label("country_code"),
                AmazonUser.marketplace_id.label("marketplace_id"),
            )
            .filter(AmazonUser.user_id.isnot(None))
            .all()
        )

        users = []
        seen = set()

        for row in amazon_rows:
            user_id = row.user_id
            marketplace_id = row.marketplace_id

            country = (
                row.country
                or row.country_code
                or MARKETPLACE_COUNTRY_MAP.get(marketplace_id)
                or ""
            )

            country = str(country).strip().lower()

            if country in ("gb", "uk", "united kingdom"):
                country = "uk"
            elif country in ("us", "usa", "united states", "na"):
                country = "us"

            if country not in ("uk", "us"):
                print(
                    f"[WARN] Skipping unsupported forecast country: "
                    f"user_id={user_id}, marketplace_id={marketplace_id}, country={country}"
                )
                continue

            key = (user_id, country)
            if not user_id or key in seen:
                continue

            seen.add(key)

            users.append({
                "user_id": user_id,
                "country": country,
            })

        print(f"[INFO] Forecast users selected from amazon_user: {users}")
        return users

    except Exception as e:
        db.session.rollback()
        print(f"[ERROR] get_users_for_monthly_forecast failed: {repr(e)}")
        return []
    

@celery_app.task(name="tasks.generate_monthly_forecast_files")
def generate_monthly_forecast_files(country_filter=None):
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
                    if country_filter and country != country_filter.lower():
                        continue

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

#---------------------------------------------- Celery Beat scheduled tasks  for daily ----------------------------------------------------#

def _make_internal_user_token(user_id, email=None):
    payload = {
        "user_id": user_id,
        "email": email,
        "scope": "daily_dashboard_refresh",
        "exp": datetime.utcnow() + timedelta(hours=2),
    }

    return jwt.encode(payload, SECRET_KEY, algorithm="HS256")


@celery_app.task(name="tasks.refresh_dashboard_daily")
def refresh_dashboard_daily(country_filter=None):
    """
    Daily backend refresh, same purpose as Refresh button.
    Runs country-wise and stores dashboard cache in public.live_data.
    """
    with flask_app.app_context():
        try:
            base_url = os.getenv("BACKEND_BASE_URL") or os.getenv("API_BASE_URL")

            if not base_url:
                print("[ERROR] BACKEND_BASE_URL/API_BASE_URL is not set")
                return

            base_url = base_url.rstrip("/")

            users = get_users_for_monthly_amazon_sync()
            processed = set()

            for user in users:
                try:
                    user_id = user["user_id"]
                    email = user.get("email")
                    country = (user.get("country") or "").strip().lower()
                    marketplace_id = user.get("marketplace_id")

                    if country_filter and country != country_filter.lower():
                        continue

                    if country not in ("uk", "us"):
                        continue

                    dedupe_key = (user_id, country, marketplace_id)
                    if dedupe_key in processed:
                        continue

                    processed.add(dedupe_key)

                    token = _make_internal_user_token(user_id, email)

                    headers = {
                        "Accept": "application/json",
                        "Content-Type": "application/json",
                        "Authorization": f"Bearer {token}",
                    }

                    print(
                        f"[INFO] Daily dashboard refresh started: "
                        f"user_id={user_id}, country={country}, marketplace_id={marketplace_id}"
                    )

                    today = date.today()
                    month_name = today.strftime("%B").lower()

                    mtd_payload = None
                    live_bi_payload = None
                    ads_payload = None

                    # 1) Amazon MTD transactions
                    params = {
                        "country": country,
                        "marketplace_id": marketplace_id,
                        "store_in_db": "true",
                        "format": "json",
                    }

                    res = requests.get(
                        f"{base_url}/amazon_api/finances/mtd_transactions?{urlencode(params)}",
                        headers=headers,
                        timeout=900,
                    )

                    if not res.ok:
                        print(
                            f"[ERROR] mtd_transactions failed "
                            f"user_id={user_id}, country={country}, "
                            f"status={res.status_code}, body={res.text[:500]}"
                        )
                    else:
                        mtd_payload = _safe_json_response(res)
                        print(
                            f"[INFO] mtd_transactions refreshed "
                            f"user_id={user_id}, country={country}"
                        )

                    # 2) Live MTD BI
                    live_bi_params = {
                        "countryName": country,
                        "ranged": "MTD",
                        "month": month_name,
                        "year": str(today.year),
                        "generate_ai_insights": "false",
                        "start_day": "1",
                        "end_day": str(today.day),
                    }

                    res = requests.get(
                        f"{base_url}/live_mtd_bi?{urlencode(live_bi_params)}",
                        headers=headers,
                        timeout=900,
                    )

                    if not res.ok:
                        print(
                            f"[ERROR] live_mtd_bi failed "
                            f"user_id={user_id}, country={country}, "
                            f"status={res.status_code}, body={res.text[:500]}"
                        )
                    else:
                        live_bi_payload = _safe_json_response(res)
                        print(
                            f"[INFO] live_mtd_bi refreshed "
                            f"user_id={user_id}, country={country}"
                        )

                    # 3) Ads monthly SP/SD/SB DB refresh
                    include = ["SP", "SD", "SB"]

                    ads_body = {
                        "month": today.month,
                        "year": today.year,
                        "country": country.upper(),
                        "include": include,
                    }

                    res = requests.post(
                        f"{base_url}/api/ads/monthly_sp_sd_to_db",
                        headers=headers,
                        json=ads_body,
                        timeout=900,
                    )

                    if not res.ok:
                        print(
                            f"[ERROR] monthly_sp_sd_to_db failed "
                            f"user_id={user_id}, country={country}, "
                            f"status={res.status_code}, body={res.text[:500]}"
                        )
                    else:
                        ads_payload = _safe_json_response(res)
                        print(
                            f"[INFO] ads monthly refresh done "
                            f"user_id={user_id}, country={country}"
                        )

                    # 4) Save refreshed dashboard cache into public.live_data
                    _post_live_dashboard_cache(
                        base_url=base_url,
                        headers=headers,
                        user_id=user_id,
                        country=country,
                        mtd_payload=mtd_payload,
                        live_bi_payload=live_bi_payload,
                        ads_payload=ads_payload,
                    )

                    print(
                        f"[INFO] Daily dashboard refresh completed and saved: "
                        f"user_id={user_id}, country={country}"
                    )

                except Exception as e:
                    db.session.rollback()
                    print(f"[ERROR] Daily dashboard refresh failed for user={user}: {repr(e)}")
                    continue

        except Exception as e:
            db.session.rollback()
            print(f"[ERROR] refresh_dashboard_daily crashed: {repr(e)}")

        finally:
            db.session.remove()
            

def _country_dashboard_meta(country: str):
    country = (country or "").strip().lower()

    if country == "us":
        return {
            "platform": "amazon-us",
            "region": "US",
            "country": "us",
        }

    return {
        "platform": "amazon-uk",
        "region": "UK",
        "country": "uk",
    }


def _safe_json_response(res):
    try:
        return res.json()
    except Exception:
        return {
            "success": False,
            "raw": res.text[:1000] if hasattr(res, "text") else None,
        }


def _post_live_dashboard_cache(
    *,
    base_url: str,
    headers: dict,
    user_id: int,
    country: str,
    mtd_payload: dict | None,
    live_bi_payload: dict | None,
    ads_payload: dict | None,
):
    today = date.today()
    meta = _country_dashboard_meta(country)

    cache_payload = {
        "data": mtd_payload,
        "liveBiPayload": live_bi_payload,
        "monthlySpPayload": ads_payload,
        "biStatus": "ready" if live_bi_payload else "idle",
        "liveBiReady": bool(live_bi_payload),
        "savedAt": int(datetime.utcnow().timestamp() * 1000),
        "source": "celery_refresh_dashboard_daily",
    }

    save_body = {
        "country": meta["country"],
        "platform": meta["platform"],
        "region": meta["region"],
        "startDay": 1,
        "endDay": today.day,
        "savedAt": int(datetime.utcnow().timestamp() * 1000),
        "cachePayload": cache_payload,
    }

    res = requests.post(
        f"{base_url}/amazon_api/live-dashboard/save",
        headers=headers,
        json=save_body,
        timeout=900,
    )

    if not res.ok:
        print(
            f"[ERROR] live-dashboard/save failed "
            f"user_id={user_id}, country={country}, "
            f"status={res.status_code}, body={res.text[:500]}"
        )
        return None

    json_data = _safe_json_response(res)

    print(
        f"[INFO] live-dashboard cache saved "
        f"user_id={user_id}, country={country}, "
        f"cache_key={json_data.get('cache_key')}"
    )

    return json_data

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
            schedules = db.session.query(AgentEmailSchedule).filter_by(enabled=True).all()

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

            