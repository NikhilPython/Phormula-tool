from flask import Blueprint, request, jsonify
from config import Config
SECRET_KEY = Config.SECRET_KEY
from sqlalchemy import text, func
import json
import pandas as pd
from sqlalchemy import inspect
from app import db
from app.models.user_models import CurrencyConversion, Category, UserAdmin , User, UploadHistory, CountryProfile, Member, UserObjective, amazon_user, Inventory, MonthwiseInventory, SuperAdminIssue
from sqlalchemy.exc import IntegrityError
from flask import current_app
import io,jwt, re
from datetime import datetime, timezone, timedelta
from app.utils.amazon_utils import amazon_client, db_url, db_url1
from app.utils.us_process_utils import (
    process_skuwise_us_data,
    process_us_yearly_skuwise_data,
    process_us_quarterly_skuwise_data,
)

from app.utils.uk_process_utils import (
    process_skuwise_data,
    process_quarterly_skuwise_data,
    process_yearly_skuwise_data,
)

from app.utils.currency_utils import (
    process_global_monthly_skuwise_data,
    process_global_quarterly_skuwise_data,
    process_global_yearly_skuwise_data,
)


superadmin_dashboard_bp = Blueprint('superadmin_dashboard', __name__)


# --------------------------- helpers ---------------------------

MONTH_ORDER = {
    "january": 1,
    "february": 2,
    "march": 3,
    "april": 4,
    "may": 5,
    "june": 6,
    "july": 7,
    "august": 8,
    "september": 9,
    "october": 10,
    "november": 11,
    "december": 12,
}

MONTH_NAMES = {v: k for k, v in MONTH_ORDER.items()}

MARKETPLACE_COUNTRY_LABELS = {
    "A1F83G8C2ARO7P": "UK",
    "ATVPDKIKX0DER": "US",
    "A2EUQ1WTGCTBG2": "CA",
}

AUTOMATED_ISSUE_TYPES = {
    "SKU_COUNT_ZERO",
    "NO_CURRENT_INVENTORY",
    "ADS_TOKEN_EXPIRED",
    "ADS_TOKEN_MISSING",
    "PNL_TABLE_MISSING",
    "COUNTRY_PROFILE_MISSING",
    "SYNC_FAILED",
    "CURRENT_MONTH_DATA_STALE",
}

ADS_TOKEN_STALE_DAYS = 90


def _is_superadmin_authenticated():
    """
    Returns: (True, None) if authenticated as superadmin else (False, (response_json, status_code))
    """
    # Method 1: Authorization header
    auth_header = request.headers.get("Authorization")
    if auth_header and auth_header.startswith("Bearer "):
        token = auth_header.split(" ")[1]
        try:
            decoded = jwt.decode(token, SECRET_KEY, algorithms=["HS256"])
            if decoded.get("is_superadmin"):
                return True, None
            return False, ({"message": "Not a superadmin"}, 403)
        except jwt.ExpiredSignatureError:
            return False, ({"message": "Token has expired"}, 401)
        except jwt.InvalidTokenError:
            return False, ({"message": "Invalid token"}, 401)

    # Method 2: query param fallback
    authenticated_user = request.args.get("authenticated_user")
    if authenticated_user:
        return True, None

    return False, ({"message": "User not authenticated"}, 401)


def _normalize_month(value):
    raw = str(value or "").strip().lower()

    if raw.isdigit():
        number = int(raw)
        if 1 <= number <= 12:
            return MONTH_NAMES[number], number

    if raw in MONTH_ORDER:
        return raw, MONTH_ORDER[raw]

    now = datetime.now(timezone.utc)
    return MONTH_NAMES[now.month], now.month


def _normalize_country(value):
    raw = str(value or "").strip().lower()
    aliases = {
        "united kingdom": "uk",
        "great britain": "uk",
        "gb": "uk",
        "uk": "uk",
        "united states": "us",
        "usa": "us",
        "us": "us",
        "canada": "ca",
        "ca": "ca",
    }
    return aliases.get(raw, raw)


def _safe_iso(value):
    if not value:
        return None

    try:
        return _as_aware_utc(value).isoformat()
    except Exception:
        return None


def _as_aware_utc(value):
    if not value:
        return None

    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)

    return value.astimezone(timezone.utc)


def _table_count(engine, inspector, table_name):
    if not inspector.has_table(table_name):
        return False, 0

    try:
        with engine.connect() as conn:
            count = conn.execute(
                text(f'SELECT COUNT(*) FROM "{table_name}"')
            ).scalar() or 0
        return True, int(count)
    except Exception:
        return True, 0


def _live_dashboard_cache_summary(engine, inspector, user_id, country):
    if not inspector.has_table("live_dashboard_cache"):
        return False, 0, None

    country_key = _normalize_country(country)

    try:
        with engine.connect() as conn:
            row = conn.execute(
                text(
                    """
                    SELECT
                        COUNT(*) AS row_count,
                        MAX(updated_at) AS latest_updated_at,
                        MAX(to_timestamp(saved_at / 1000.0)) AS latest_saved_at
                    FROM public.live_dashboard_cache
                    WHERE user_id = :user_id
                      AND lower(country) = :country
                    """
                ),
                {
                    "user_id": user_id,
                    "country": country_key,
                },
            ).mappings().first()

        count = int((row or {}).get("row_count") or 0)
        latest_values = [
            _as_aware_utc(value)
            for value in [
                (row or {}).get("latest_updated_at"),
                (row or {}).get("latest_saved_at"),
            ]
            if value
        ]

        return True, count, max(latest_values, default=None)
    except Exception:
        return True, 0, None


def _latest_user_activity(user):
    return max(
        [
            _as_aware_utc(value)
            for value in [
                getattr(user, "sku_updated_at", None),
                getattr(user, "integration_updated_at", None),
                getattr(user, "company_updated_at", None),
            ]
            if value
        ],
        default=None,
    )


def _country_display(country):
    country_key = _normalize_country(country)
    if country_key == "uk":
        return "UK"
    if country_key == "us":
        return "US"
    if country_key == "ca":
        return "CA"
    return str(country or "").strip().upper()


def _round_row(columns, raw_row):
    row_dict = {}
    for col, val in zip(columns, raw_row):
        if isinstance(val, (int, float)):
            row_dict[col] = round(val, 2)
        else:
            row_dict[col] = val
    return row_dict


def _connected_amazon_countries(user_id):
    rows = amazon_user.query.filter_by(user_id=user_id).all()
    countries = []
    marketplace_ids = []

    for row in rows:
        country = (getattr(row, "country_name", None) or "").strip()
        marketplace_id = (getattr(row, "marketplace_id", None) or "").strip()

        if country and country.lower() not in {c.lower() for c in countries}:
            countries.append(country)

        if marketplace_id and marketplace_id not in marketplace_ids:
            marketplace_ids.append(marketplace_id)

    return countries, marketplace_ids


def _connections_for_user(user):
    connections = amazon_user.query.filter_by(user_id=user.id).all()
    if connections:
        return connections

    return [
        type(
            "SuperAdminFallbackConnection",
            (),
            {
                "country_name": getattr(user, "country", None),
                "marketplace_id": getattr(user, "marketplace_id", None),
                "refresh_token": None,
                "amazon_ads_refresh_token": None,
                "amazon_ads_refresh_token_updated_at": None,
                "seller_id": None,
                "updated_at": None,
                "is_connected": False,
            },
        )()
    ]


def _period_bounds(year, month_number):
    period_start = datetime(year, month_number, 1)
    if month_number == 12:
        period_end = datetime(year + 1, 1, 1)
    else:
        period_end = datetime(year, month_number + 1, 1)

    return period_start, period_end


def _period_key(month_name, year):
    return f"{month_name}-{year}"


def _connection_country_and_marketplace(user, connection):
    country = _normalize_country(getattr(connection, "country_name", None))
    marketplace_id = (
        getattr(connection, "marketplace_id", None)
        or getattr(user, "marketplace_id", None)
        or ""
    ).strip()

    if not country and marketplace_id:
        country = _normalize_country(MARKETPLACE_COUNTRY_LABELS.get(marketplace_id, ""))

    if not country:
        country = "unknown"

    return country, marketplace_id


def _country_profile_exists(user_id, country, marketplace_id):
    country_key = _normalize_country(country)
    marketplace = str(marketplace_id or "").strip()

    query = CountryProfile.query.filter(CountryProfile.user_id == user_id)
    if marketplace:
        query = query.filter(CountryProfile.marketplace == marketplace)

    profiles = query.all()
    for profile in profiles:
        if _normalize_country(profile.country) == country_key:
            return True

    return False


def _ensure_superadmin_issue_table():
    engine = db.engines["superadmin"]
    SuperAdminIssue.__table__.create(bind=engine, checkfirst=True)


def _issue_to_dict(issue):
    return {
        "id": issue.id,
        "issue_key": issue.issue_key,
        "user_id": issue.user_id,
        "email": issue.email,
        "name": issue.name,
        "brand_name": issue.brand_name,
        "company_name": issue.company_name,
        "country": issue.country,
        "marketplace_id": issue.marketplace_id,
        "period": {
            "month": issue.period_month,
            "year": issue.period_year,
        },
        "issue_type": issue.issue_type,
        "severity": issue.severity,
        "status": issue.status,
        "title": issue.title,
        "description": issue.description,
        "evidence": issue.evidence or {},
        "occurrences": issue.occurrences,
        "detected_at": _safe_iso(issue.detected_at),
        "last_seen_at": _safe_iso(issue.last_seen_at),
        "resolved_at": _safe_iso(issue.resolved_at),
    }


def _make_issue_payload(
    issue_type,
    user,
    country,
    marketplace_id,
    month_name,
    year,
    title,
    description,
    severity="medium",
    evidence=None,
):
    normalized_country = _normalize_country(country)
    period = _period_key(month_name, year)
    issue_key = ":".join(
        [
            str(user.id),
            normalized_country or "unknown",
            marketplace_id or "no-marketplace",
            period,
            issue_type,
        ]
    )

    return {
        "issue_key": issue_key,
        "user_id": user.id,
        "email": user.email,
        "name": user.name,
        "brand_name": user.brand_name,
        "company_name": user.company_name,
        "country": _country_display(normalized_country),
        "marketplace_id": marketplace_id or None,
        "period_month": month_name,
        "period_year": year,
        "period_key": period,
        "issue_type": issue_type,
        "severity": severity,
        "title": title,
        "description": description,
        "evidence": evidence or {},
    }


def _upsert_superadmin_issue(payload, now):
    issue = SuperAdminIssue.query.filter_by(issue_key=payload["issue_key"]).first()

    if not issue:
        issue = SuperAdminIssue(
            **payload,
            status="open",
            occurrences=1,
            detected_at=now,
            last_seen_at=now,
        )
        db.session.add(issue)
        return issue, "created"

    for key, value in payload.items():
        setattr(issue, key, value)

    issue.status = "open"
    issue.resolved_at = None
    issue.last_seen_at = now
    issue.occurrences = int(issue.occurrences or 0) + 1
    return issue, "updated"


def _scan_automated_issues(month_name, month_number, year, requested_user_id=None):
    _ensure_superadmin_issue_table()

    primary_engine = db.engine
    amazon_engine = db.engines["amazon"]
    primary_inspector = inspect(primary_engine)
    amazon_inspector = inspect(amazon_engine)

    period_start, period_end = _period_bounds(year, month_number)
    now = datetime.now(timezone.utc)
    current_period = year == now.year and month_number == now.month
    current_month_start = datetime(now.year, now.month, 1, tzinfo=timezone.utc)
    ads_token_cutoff = now - timedelta(days=ADS_TOKEN_STALE_DAYS)
    period = _period_key(month_name, year)

    query = User.query.order_by(User.id.asc())
    if requested_user_id:
        query = query.filter(User.id == requested_user_id)

    users = query.all()
    detected_payloads = []

    for user in users:
        for connection in _connections_for_user(user):
            country, marketplace_id = _connection_country_and_marketplace(user, connection)

            sku_table = f"sku_{user.id}_data_table"
            sku_table_exists, sku_count = _table_count(
                primary_engine,
                primary_inspector,
                sku_table,
            )

            pnl_table = f"skuwisemonthly_{user.id}_{country}_{month_name}{year}"
            pnl_table_exists, pnl_count = _table_count(
                primary_engine,
                primary_inspector,
                pnl_table,
            )

            current_inventory_table = (
                f"currentinventory_{user.id}_{country}_{month_name}{year}_table"
            )
            current_inventory_exists, current_inventory_count = _table_count(
                primary_engine,
                primary_inspector,
                current_inventory_table,
            )
            live_dashboard_cache_exists, live_dashboard_cache_count, latest_live_dashboard_cache_at = (
                _live_dashboard_cache_summary(
                    primary_engine,
                    primary_inspector,
                    user.id,
                    country,
                )
            )

            inventory_count = 0
            latest_inventory_sync = None
            monthwise_inventory_count = 0
            latest_monthwise_sync = None

            if marketplace_id and amazon_inspector.has_table("inventory"):
                inventory_count = Inventory.query.filter_by(
                    user_id=user.id,
                    marketplace_id=marketplace_id,
                ).count()
                latest_inventory_sync = (
                    db.session.query(func.max(Inventory.synced_at))
                    .filter(
                        Inventory.user_id == user.id,
                        Inventory.marketplace_id == marketplace_id,
                    )
                    .scalar()
                )

            if marketplace_id and amazon_inspector.has_table("monthwise_inventory"):
                monthwise_inventory_count = MonthwiseInventory.query.filter(
                    MonthwiseInventory.user_id == user.id,
                    MonthwiseInventory.marketplace_id == marketplace_id,
                    MonthwiseInventory.date >= period_start.date(),
                    MonthwiseInventory.date < period_end.date(),
                ).count()
                latest_monthwise_sync = (
                    db.session.query(func.max(MonthwiseInventory.synced_at))
                    .filter(
                        MonthwiseInventory.user_id == user.id,
                        MonthwiseInventory.marketplace_id == marketplace_id,
                        MonthwiseInventory.date >= period_start.date(),
                        MonthwiseInventory.date < period_end.date(),
                    )
                    .scalar()
                )

            amazon_connected = bool(
                getattr(connection, "is_connected", False)
                or getattr(connection, "refresh_token", None)
            )
            ads_token = getattr(connection, "amazon_ads_refresh_token", None)
            seller_id = getattr(connection, "seller_id", None)
            ads_connected = bool(ads_token or seller_id)
            ads_token_updated_at = _as_aware_utc(
                getattr(connection, "amazon_ads_refresh_token_updated_at", None)
            )
            pnl_available = bool(pnl_table_exists and pnl_count > 0)
            inventory_available = bool(
                inventory_count > 0
                or monthwise_inventory_count > 0
                or (current_inventory_exists and current_inventory_count > 0)
            )
            country_profile_available = _country_profile_exists(
                user.id,
                country,
                marketplace_id,
            )
            latest_user_activity = _latest_user_activity(user)
            last_sync = max(
                [
                    _as_aware_utc(value)
                    for value in [
                        getattr(connection, "updated_at", None),
                        latest_inventory_sync,
                        latest_monthwise_sync,
                        latest_live_dashboard_cache_at,
                        latest_user_activity,
                    ]
                    if value
                ],
                default=None,
            )

            base_evidence = {
                "sku_table": sku_table if sku_table_exists else None,
                "sku_rows": sku_count,
                "pnl_table": pnl_table if pnl_table_exists else None,
                "pnl_rows": pnl_count,
                "current_inventory_table": (
                    current_inventory_table if current_inventory_exists else None
                ),
                "current_inventory_rows": current_inventory_count,
                "live_dashboard_cache_exists": live_dashboard_cache_exists,
                "live_dashboard_cache_rows": live_dashboard_cache_count,
                "live_dashboard_cache_updated_at": _safe_iso(latest_live_dashboard_cache_at),
                "inventory_rows": inventory_count,
                "monthwise_inventory_rows": monthwise_inventory_count,
                "amazon_connected": amazon_connected,
                "ads_connected": ads_connected,
                "seller_id": seller_id,
                "ads_token_updated_at": _safe_iso(ads_token_updated_at),
                "user_sku_updated_at": _safe_iso(getattr(user, "sku_updated_at", None)),
                "user_integration_updated_at": _safe_iso(
                    getattr(user, "integration_updated_at", None)
                ),
                "user_company_updated_at": _safe_iso(
                    getattr(user, "company_updated_at", None)
                ),
                "latest_user_activity_at": _safe_iso(latest_user_activity),
                "last_sync_at": _safe_iso(last_sync),
            }

            def add_issue(issue_type, title, description, severity="medium", extra=None):
                evidence = dict(base_evidence)
                if extra:
                    evidence.update(extra)

                detected_payloads.append(
                    _make_issue_payload(
                        issue_type=issue_type,
                        user=user,
                        country=country,
                        marketplace_id=marketplace_id,
                        month_name=month_name,
                        year=year,
                        title=title,
                        description=description,
                        severity=severity,
                        evidence=evidence,
                    )
                )

            if not sku_table_exists or sku_count == 0:
                add_issue(
                    "SKU_COUNT_ZERO",
                    "SKU count is 0",
                    "The SKU table is missing or has no SKU rows for this user.",
                    "high",
                    {"table_exists": sku_table_exists},
                )

            if amazon_connected and not inventory_available:
                add_issue(
                    "NO_CURRENT_INVENTORY",
                    "Marketplace connected but no current inventory",
                    "The marketplace is connected, but no current inventory data was found for the selected period.",
                    "high",
                )

            if not ads_connected:
                add_issue(
                    "ADS_TOKEN_MISSING",
                    "Ads token missing",
                    "Amazon Ads is not connected for this marketplace.",
                    "medium",
                )
            elif ads_token and ads_token_updated_at and ads_token_updated_at < ads_token_cutoff:
                add_issue(
                    "ADS_TOKEN_EXPIRED",
                    "Ads token expired",
                    f"Amazon Ads token was last refreshed more than {ADS_TOKEN_STALE_DAYS} days ago.",
                    "high",
                    {"stale_after_days": ADS_TOKEN_STALE_DAYS},
                )

            if not pnl_table_exists or pnl_count == 0:
                add_issue(
                    "PNL_TABLE_MISSING",
                    "P&L table missing",
                    "The monthly P&L table is missing or empty for this country and period.",
                    "high",
                    {"table_exists": pnl_table_exists},
                )

            if not country_profile_available:
                add_issue(
                    "COUNTRY_PROFILE_MISSING",
                    "Country profile missing",
                    "No stock, transit and alert threshold profile is configured for this country and marketplace.",
                    "medium",
                )

            if amazon_connected and not pnl_available and not inventory_available:
                add_issue(
                    "SYNC_FAILED",
                    "Sync failed",
                    "The marketplace is connected, but both P&L and inventory outputs are missing for the selected period.",
                    "critical",
                )

            live_dashboard_current = bool(
                latest_live_dashboard_cache_at
                and latest_live_dashboard_cache_at >= current_month_start
            )
            user_activity_current = bool(
                latest_user_activity
                and latest_user_activity >= current_month_start
            )
            current_month_outputs_available = bool(
                pnl_available
                or inventory_available
                or live_dashboard_current
                or user_activity_current
            )

            if (
                current_period
                and not current_month_outputs_available
                and (not last_sync or last_sync < current_month_start)
            ):
                add_issue(
                    "CURRENT_MONTH_DATA_STALE",
                    "Data not updated for current month",
                    "No current-month P&L or inventory output was found.",
                    "medium",
                    {
                        "current_month_start": current_month_start.isoformat(),
                        "current_month_outputs_available": current_month_outputs_available,
                        "live_dashboard_current": live_dashboard_current,
                        "user_activity_current": user_activity_current,
                    },
                )

    created = 0
    updated = 0
    detected_keys = set()
    issues = []

    for payload in detected_payloads:
        issue, action = _upsert_superadmin_issue(payload, now)
        detected_keys.add(payload["issue_key"])
        issues.append(issue)
        if action == "created":
            created += 1
        else:
            updated += 1

    resolved = 0
    open_query = SuperAdminIssue.query.filter(
        SuperAdminIssue.status == "open",
        SuperAdminIssue.period_key == period,
        SuperAdminIssue.issue_type.in_(AUTOMATED_ISSUE_TYPES),
    )
    if requested_user_id:
        open_query = open_query.filter(SuperAdminIssue.user_id == requested_user_id)

    for issue in open_query.all():
        if issue.issue_key not in detected_keys:
            issue.status = "resolved"
            issue.resolved_at = now
            resolved += 1

    db.session.commit()

    open_count = SuperAdminIssue.query.filter_by(status="open").count()

    return {
        "created": created,
        "updated": updated,
        "resolved": resolved,
        "open_count": open_count,
        "issues": [_issue_to_dict(issue) for issue in issues],
    }


def _safe_user_dict(u: User):
    countries, marketplace_ids = _connected_amazon_countries(u.id)

    return {
        "id": u.id,
        "name": getattr(u, "name", None),
        "email": u.email,
        "company_name": getattr(u, "company_name", None),
        "brand_name": getattr(u, "brand_name", None),
        "country": getattr(u, "country", None),
        "marketplace_id": getattr(u, "marketplace_id", None),
        "countries": countries,
        "marketplace_ids": marketplace_ids,
        "annual_sales_range": getattr(u, "annual_sales_range", None),
        "target_sales": float(u.target_sales) if getattr(u, "target_sales", None) is not None else None,
        "address": getattr(u, "address", None),
        # ✅ ADD THIS LINE
        "status": u.status

    }


def _safe_admin_dict(ua: UserAdmin):
    return {
        "id": ua.id,
        "name": getattr(ua, "name", None),
        "email": ua.email,
        "company_name": getattr(ua, "company_name", None),
        "brand_name": getattr(ua, "brand_name", None),
        "country": getattr(ua, "country", None),
        "marketplace_id": getattr(ua, "marketplace_id", None),
        "annual_sales_range": getattr(ua, "annual_sales_range", None),
    }


# --------------------------- routes ---------------------------

@superadmin_dashboard_bp.route("/superadmin/dashboard", methods=["GET"])
def get_superadmin_dashboard():
    ok, err = _is_superadmin_authenticated()
    if not ok:
        payload, code = err
        return jsonify(payload), code

    email_to_search = request.args.get("email")

    if email_to_search:
        user = User.query.filter_by(email=email_to_search).first()
        user_admin = UserAdmin.query.filter_by(email=email_to_search).first()

        if not user and not user_admin:
            return jsonify({"message": "No user found with that email"}), 404

        selected_user = user if user else user_admin
        user_id = user.id if user else getattr(user_admin, "user_id", None)

        if not user_id:
            return jsonify({"message": "User ID not found for this email"}), 404

        related_upload_history = UploadHistory.query.filter_by(user_id=user_id).all()
        related_profiles = CountryProfile.query.filter_by(user_id=user_id).all()
        related_objectives = UserObjective.query.filter_by(user_id=user_id).order_by(UserObjective.id.asc()).all()

        month_order = {
            "january": 1,
            "february": 2,
            "march": 3,
            "april": 4,
            "may": 5,
            "june": 6,
            "july": 7,
            "august": 8,
            "september": 9,
            "october": 10,
            "november": 11,
            "december": 12,
        }

        base_month_rows = [
            row for row in related_upload_history
            if (row.country or "").lower() == (getattr(selected_user, "country", "") or "").lower()
        ]

        if not base_month_rows:
            excluded_countries = {"global", "global_inr", "global_cad", "global_gbp", "uk_usd"}
            base_month_rows = [
                row for row in related_upload_history
                if (row.country or "").lower() not in excluded_countries
            ]

        unique_months = {}
        for row in base_month_rows:
            month_name = (row.month or "").strip().lower()
            year_val = row.year
            if month_name in month_order and year_val:
                unique_months[(year_val, month_order[month_name])] = {
                    "month": month_name,
                    "year": year_val
                }

        sorted_months = sorted(unique_months.items(), key=lambda x: x[0])

        months_count = len(sorted_months)
        first_month_data = sorted_months[0][1] if sorted_months else None
        last_month_data = sorted_months[-1][1] if sorted_months else None

        first_month_label = (
            f'{first_month_data["month"]} {first_month_data["year"]}'
            if first_month_data else None
        )
        last_month_label = (
            f'{last_month_data["month"]} {last_month_data["year"]}'
            if last_month_data else None
        )
        months_range = (
            f"{first_month_label} to {last_month_label}"
            if first_month_label and last_month_label else None
        )

        quarter_months = {
            "quarter1": ["january", "february", "march"],
            "quarter2": ["april", "may", "june"],
            "quarter3": ["july", "august", "september"],
            "quarter4": ["october", "november", "december"],
        }

        skuwise_data = []
        engine = db.get_engine()
        inspector = inspect(engine)
        sku_table_name = f"sku_{user_id}_data_table"
        sku_table_count = 0
        sku_table_exists = False

        # NEW
        profitability = None
        profitability_table = None

        with engine.connect() as conn:
            for c in related_upload_history:
                country_lower = (c.country or "").lower()
                month_lower = (c.month or "").lower()

                if country_lower == "global":
                    base_table = f"skuwisemonthly_{user_id}_{country_lower}_{month_lower}{c.year}_table"
                else:
                    base_table = f"skuwisemonthly_{user_id}_{country_lower}_{month_lower}{c.year}"

                yearly_base_table = f"skuwiseyearly_{user_id}_{country_lower}_{c.year}_table"

                for table_name in [base_table, yearly_base_table]:
                    if inspector.has_table(table_name):
                        try:
                            result = conn.execute(text(f'SELECT * FROM "{table_name}" LIMIT 15'))
                            columns = result.keys()
                            rows = [_round_row(columns, r) for r in result.fetchall()]
                            skuwise_data.append({"table": table_name, "rows": rows})
                        except Exception as e:
                            skuwise_data.append({"table": table_name, "error": str(e)})
                    else:
                        skuwise_data.append({
                            "table": table_name,
                            "error": f"Table '{table_name}' does not exist"
                        })

                for quarter, months in quarter_months.items():
                    if month_lower in months:
                        quarter_table = f"{quarter}_{user_id}_{country_lower}_{c.year}_table"
                        if inspector.has_table(quarter_table):
                            try:
                                result = conn.execute(text(f'SELECT * FROM "{quarter_table}" LIMIT 15'))
                                columns = result.keys()
                                rows = [_round_row(columns, r) for r in result.fetchall()]
                                skuwise_data.append({"table": quarter_table, "rows": rows})
                            except Exception as e:
                                skuwise_data.append({"table": quarter_table, "error": str(e)})
                        else:
                            skuwise_data.append({
                                "table": quarter_table,
                                "error": f"Table '{quarter_table}' does not exist"
                            })
                        break

            if inspector.has_table(sku_table_name):
                sku_table_exists = True
                count_result = conn.execute(
                    text(f'SELECT COUNT(*) AS total FROM "{sku_table_name}"')
                )
                sku_table_count = count_result.scalar() or 0

            # NEW: fetch last month's profitability from cm2_profit
            if last_month_data:
                user_country = (getattr(selected_user, "country", "") or "").strip().lower()
                last_month_name = last_month_data["month"]
                last_year = last_month_data["year"]

                profitability_table = f"skuwisemonthly_{user_id}_{user_country}_{last_month_name}{last_year}"

                if inspector.has_table(profitability_table):
                    try:
                        profitability_result = conn.execute(
                            text(f'''
                                SELECT COALESCE(SUM(cm2_profit), 0) AS profitability
                                FROM "{profitability_table}"
                            ''')
                        )
                        profitability = profitability_result.scalar()
                        if profitability is not None:
                            profitability = round(float(profitability), 2)
                    except Exception:
                        profitability = None

        latest_objective = (
            UserObjective.query
            .filter_by(user_id=user_id)
            .order_by(UserObjective.id.desc())
            .first()
        )

        countries, marketplace_ids = _connected_amazon_countries(user_id)

        return jsonify({
            "email": email_to_search,
            "user_id": user_id,
            "name": getattr(selected_user, "name", None),
            "company_name": getattr(selected_user, "company_name", None),
            "brand_name": getattr(selected_user, "brand_name", None),
            "country": getattr(selected_user, "country", None),
            "marketplace_id": getattr(selected_user, "marketplace_id", None),
            "countries": countries,
            "marketplace_ids": marketplace_ids,
            "annual_sales_range": getattr(selected_user, "annual_sales_range", None),
            "target_sales": float(selected_user.target_sales) if getattr(selected_user, "target_sales", None) is not None else None,
            "address": getattr(selected_user, "address", None),
            "created_at": selected_user.created_at.isoformat() if getattr(selected_user, "created_at", None) else None,
            "months_of_data_count": months_count,
            "data_from_month": first_month_label,
            "data_to_month": last_month_label,
            "data_month_range": months_range,

            # NEW
            "profitability": profitability,
            "profitability_month": last_month_label,
            "profitability_table": profitability_table,

            "related_country_profiles": [
    {
        "id": cp.id,
        "user_id": cp.user_id,
        "country": cp.country,
        "marketplace": cp.marketplace,

        # Individual configuration values
        "ship_time_weeks": cp.ship_time_weeks,
        "air_time_weeks": cp.air_time_weeks,
        "stock_unit_weeks": cp.stock_unit_weeks,

        # Calculated alert thresholds
        "ship_alert_threshold_weeks": (
            int(cp.ship_time_weeks or 0)
            + int(cp.stock_unit_weeks or 0)
        ),
        "air_alert_threshold_weeks": (
            int(cp.air_time_weeks or 0)
            + int(cp.stock_unit_weeks or 0)
        ),
    }
    for cp in related_profiles
],
            "ai_business_journey": latest_objective.ai_business_journey if latest_objective else None,
            "sku_table_name": sku_table_name,
            "sku_table_exists": sku_table_exists,
            "sku_count": sku_table_count,
        }), 200

    # ✅ CASE 2: No email -> return USERS + ADMINS with requested fields
    try:
        user_admins = UserAdmin.query.all()
        users = User.query.all()

        return jsonify({
            "user_admins": [_safe_admin_dict(ua) for ua in user_admins],
            "users": [_safe_user_dict(u) for u in users],
        }), 200

    except Exception as e:
        return jsonify({"message": f"Error fetching data: {str(e)}"}), 500


@superadmin_dashboard_bp.route("/superadmin/data_availability", methods=["GET"])
def get_superadmin_data_availability():
    ok, err = _is_superadmin_authenticated()
    if not ok:
        payload, code = err
        return jsonify(payload), code

    month_name, month_number = _normalize_month(request.args.get("month"))
    year = request.args.get("year", type=int) or datetime.now(timezone.utc).year
    requested_user_id = request.args.get("user_id", type=int)

    try:
        primary_engine = db.engine
        amazon_engine = db.engines["amazon"]
        primary_inspector = inspect(primary_engine)
        amazon_inspector = inspect(amazon_engine)

        query = User.query.order_by(User.id.asc())
        if requested_user_id:
            query = query.filter(User.id == requested_user_id)

        users = query.all()

        period_start = datetime(year, month_number, 1)
        if month_number == 12:
            period_end = datetime(year + 1, 1, 1)
        else:
            period_end = datetime(year, month_number + 1, 1)

        now = datetime.now(timezone.utc)
        current_period = year == now.year and month_number == now.month
        stale_cutoff = now - timedelta(hours=72)

        rows = []

        for user in users:
            connections = amazon_user.query.filter_by(user_id=user.id).all()

            if not connections:
                fallback_country = getattr(user, "country", None)
                fallback_marketplace = getattr(user, "marketplace_id", None)
                connections = [
                    type(
                        "AvailabilityFallbackConnection",
                        (),
                        {
                            "country_name": fallback_country,
                            "marketplace_id": fallback_marketplace,
                            "refresh_token": None,
                            "amazon_ads_refresh_token": None,
                            "seller_id": None,
                            "updated_at": None,
                            "is_connected": False,
                        },
                    )()
                ]

            for connection in connections:
                country = _normalize_country(getattr(connection, "country_name", None))
                marketplace_id = (
                    getattr(connection, "marketplace_id", None)
                    or getattr(user, "marketplace_id", None)
                    or ""
                ).strip()

                if not country and marketplace_id:
                    country = _normalize_country(
                        MARKETPLACE_COUNTRY_LABELS.get(marketplace_id, "")
                    )

                if not country:
                    country = "unknown"

                sku_table = f"sku_{user.id}_data_table"
                sku_table_exists, sku_count = _table_count(
                    primary_engine,
                    primary_inspector,
                    sku_table,
                )

                pnl_table = f"skuwisemonthly_{user.id}_{country}_{month_name}{year}"
                pnl_table_exists, pnl_count = _table_count(
                    primary_engine,
                    primary_inspector,
                    pnl_table,
                )

                current_inventory_table = (
                    f"currentinventory_{user.id}_{country}_{month_name}{year}_table"
                )
                current_inventory_exists, current_inventory_count = _table_count(
                    primary_engine,
                    primary_inspector,
                    current_inventory_table,
                )
                live_dashboard_cache_exists, live_dashboard_cache_count, latest_live_dashboard_cache_at = (
                    _live_dashboard_cache_summary(
                        primary_engine,
                        primary_inspector,
                        user.id,
                        country,
                    )
                )

                global_table = (
                    f"skuwisemonthly_{user.id}_global_{month_name}{year}_table"
                )
                global_table_exists, global_count = _table_count(
                    primary_engine,
                    primary_inspector,
                    global_table,
                )

                inventory_count = 0
                latest_inventory_sync = None
                monthwise_inventory_count = 0
                latest_monthwise_sync = None

                if marketplace_id and amazon_inspector.has_table("inventory"):
                    inventory_count = Inventory.query.filter_by(
                        user_id=user.id,
                        marketplace_id=marketplace_id,
                    ).count()
                    latest_inventory_sync = (
                        db.session.query(func.max(Inventory.synced_at))
                        .filter(
                            Inventory.user_id == user.id,
                            Inventory.marketplace_id == marketplace_id,
                        )
                        .scalar()
                    )

                if marketplace_id and amazon_inspector.has_table("monthwise_inventory"):
                    monthwise_inventory_count = MonthwiseInventory.query.filter(
                        MonthwiseInventory.user_id == user.id,
                        MonthwiseInventory.marketplace_id == marketplace_id,
                        MonthwiseInventory.date >= period_start.date(),
                        MonthwiseInventory.date < period_end.date(),
                    ).count()
                    latest_monthwise_sync = (
                        db.session.query(func.max(MonthwiseInventory.synced_at))
                        .filter(
                            MonthwiseInventory.user_id == user.id,
                            MonthwiseInventory.marketplace_id == marketplace_id,
                            MonthwiseInventory.date >= period_start.date(),
                            MonthwiseInventory.date < period_end.date(),
                        )
                        .scalar()
                    )

                currency_count = CurrencyConversion.query.filter(
                    func.lower(CurrencyConversion.month) == month_name,
                    CurrencyConversion.year == year,
                ).count()

                amazon_connected = bool(
                    getattr(connection, "is_connected", False)
                    or getattr(connection, "refresh_token", None)
                )
                ads_connected = bool(
                    getattr(connection, "amazon_ads_refresh_token", None)
                    or getattr(connection, "seller_id", None)
                )
                sku_available = bool(sku_table_exists and sku_count > 0)
                pnl_available = bool(pnl_table_exists and pnl_count > 0)
                inventory_connected = bool(
                    inventory_count > 0
                    or monthwise_inventory_count > 0
                    or (current_inventory_exists and current_inventory_count > 0)
                )
                currency_global_available = bool(
                    (global_table_exists and global_count > 0)
                    or currency_count > 0
                )

                last_sync_candidates = [
                    getattr(connection, "updated_at", None),
                    latest_inventory_sync,
                    latest_monthwise_sync,
                    latest_live_dashboard_cache_at,
                    _latest_user_activity(user),
                ]
                last_sync = max(
                    [
                        _as_aware_utc(value)
                        for value in last_sync_candidates
                        if value
                    ],
                    default=None,
                )

                checks = {
                    "sku_data": sku_available,
                    "pnl_data": pnl_available,
                    "ads_connected": ads_connected,
                    "inventory_connected": inventory_connected,
                    "currency_global_data": currency_global_available,
                }

                missing_labels = [
                    label
                    for label, available in {
                        "SKU data": sku_available,
                        "P&L data": pnl_available,
                        "Ads connection": ads_connected,
                        "Inventory data": inventory_connected,
                        "Currency/global data": currency_global_available,
                    }.items()
                    if not available
                ]

                all_available = all(checks.values())
                stale = bool(
                    all_available
                    and current_period
                    and last_sync
                    and last_sync < stale_cutoff
                )

                if all_available and not stale:
                    status = "Complete"
                elif stale:
                    status = "Stale"
                else:
                    status = "Missing"

                rows.append({
                    "user_id": user.id,
                    "name": user.name,
                    "email": user.email,
                    "brand_name": user.brand_name,
                    "company_name": user.company_name,
                    "country": _country_display(country),
                    "country_key": country,
                    "marketplace_id": marketplace_id or None,
                    "period": {
                        "month": month_name,
                        "month_number": month_number,
                        "year": year,
                    },
                    "checks": checks,
                    "counts": {
                        "sku_rows": sku_count,
                        "pnl_rows": pnl_count,
                        "inventory_rows": inventory_count,
                        "monthwise_inventory_rows": monthwise_inventory_count,
                        "current_inventory_rows": current_inventory_count,
                        "live_dashboard_cache_rows": live_dashboard_cache_count,
                        "currency_rows": currency_count,
                        "global_rows": global_count,
                    },
                    "tables": {
                        "sku": sku_table if sku_table_exists else None,
                        "pnl": pnl_table if pnl_table_exists else None,
                        "current_inventory": (
                            current_inventory_table
                            if current_inventory_exists
                            else None
                        ),
                        "global": global_table if global_table_exists else None,
                        "live_dashboard_cache": (
                            "live_dashboard_cache"
                            if live_dashboard_cache_exists
                            else None
                        ),
                    },
                    "live_dashboard_cache_updated_at": _safe_iso(
                        latest_live_dashboard_cache_at
                    ),
                    "user_activity": {
                        "sku_updated_at": _safe_iso(getattr(user, "sku_updated_at", None)),
                        "integration_updated_at": _safe_iso(
                            getattr(user, "integration_updated_at", None)
                        ),
                        "company_updated_at": _safe_iso(
                            getattr(user, "company_updated_at", None)
                        ),
                    },
                    "last_sync_at": _safe_iso(last_sync),
                    "status": status,
                    "missing": missing_labels,
                })

        summary = {
            "total": len(rows),
            "complete": sum(1 for row in rows if row["status"] == "Complete"),
            "missing": sum(1 for row in rows if row["status"] == "Missing"),
            "stale": sum(1 for row in rows if row["status"] == "Stale"),
            "failed": sum(1 for row in rows if row["status"] == "Failed"),
        }

        return jsonify({
            "period": {
                "month": month_name,
                "month_number": month_number,
                "year": year,
            },
            "summary": summary,
            "rows": rows,
        }), 200

    except Exception as e:
        current_app.logger.exception("Data availability check failed")
        return jsonify({
            "message": "Error checking data availability",
            "error": str(e),
        }), 500


@superadmin_dashboard_bp.route("/superadmin/issue_detection/run", methods=["GET", "POST"])
def run_superadmin_issue_detection():
    ok, err = _is_superadmin_authenticated()
    if not ok:
        payload, code = err
        return jsonify(payload), code

    payload = request.get_json(silent=True) or {}
    month_name, month_number = _normalize_month(
        payload.get("month") or request.args.get("month")
    )
    year = (
        payload.get("year")
        or request.args.get("year", type=int)
        or datetime.now(timezone.utc).year
    )
    requested_user_id = payload.get("user_id") or request.args.get("user_id", type=int)

    try:
        year = int(year)
        requested_user_id = int(requested_user_id) if requested_user_id else None

        result = _scan_automated_issues(
            month_name=month_name,
            month_number=month_number,
            year=year,
            requested_user_id=requested_user_id,
        )

        return jsonify({
            "success": True,
            "period": {
                "month": month_name,
                "month_number": month_number,
                "year": year,
            },
            **result,
        }), 200

    except Exception as e:
        db.session.rollback()
        current_app.logger.exception("Automated issue detection failed")
        return jsonify({
            "success": False,
            "message": "Automated issue detection failed",
            "error": str(e),
        }), 500


@superadmin_dashboard_bp.route("/superadmin/issues", methods=["GET"])
def get_superadmin_issues():
    ok, err = _is_superadmin_authenticated()
    if not ok:
        payload, code = err
        return jsonify(payload), code

    try:
        _ensure_superadmin_issue_table()

        status = (request.args.get("status") or "open").strip().lower()
        issue_type = (request.args.get("issue_type") or "").strip()
        user_id = request.args.get("user_id", type=int)
        month = (request.args.get("month") or "").strip().lower()
        year = request.args.get("year", type=int)

        query = SuperAdminIssue.query.order_by(
            SuperAdminIssue.last_seen_at.desc(),
            SuperAdminIssue.id.desc(),
        )

        if status != "all":
            query = query.filter(SuperAdminIssue.status == status)
        if issue_type:
            query = query.filter(SuperAdminIssue.issue_type == issue_type)
        if user_id:
            query = query.filter(SuperAdminIssue.user_id == user_id)
        if month:
            month_name, _ = _normalize_month(month)
            query = query.filter(SuperAdminIssue.period_month == month_name)
        if year:
            query = query.filter(SuperAdminIssue.period_year == year)

        issues = query.limit(500).all()
        summary_query = SuperAdminIssue.query

        return jsonify({
            "success": True,
            "summary": {
                "open": summary_query.filter_by(status="open").count(),
                "resolved": summary_query.filter_by(status="resolved").count(),
                "critical": summary_query.filter_by(
                    status="open",
                    severity="critical",
                ).count(),
                "high": summary_query.filter_by(
                    status="open",
                    severity="high",
                ).count(),
            },
            "issues": [_issue_to_dict(issue) for issue in issues],
        }), 200

    except Exception as e:
        current_app.logger.exception("Fetching Super Admin issues failed")
        return jsonify({
            "success": False,
            "message": "Error fetching issues",
            "error": str(e),
        }), 500


@superadmin_dashboard_bp.route('/superadmin/dashboard/upload_currency_file', methods=['POST'])
def upload_currency_file():
    # Authentication check - prioritize token over query parameter
    authenticated = False
    
    # Method 1: Check Authorization header (recommended)
    auth_header = request.headers.get('Authorization')
    if auth_header and auth_header.startswith('Bearer '):
        token = auth_header.split(' ')[1]
        try:
            # Verify the token
            decoded_token = jwt.decode(token, SECRET_KEY, algorithms=['HS256'])
            if decoded_token.get('is_superadmin'):
                authenticated = True
        except jwt.ExpiredSignatureError:
            return jsonify({'message': 'Token has expired'}), 401
        except jwt.InvalidTokenError:
            return jsonify({'message': 'Invalid token'}), 401
    
    # Method 2: Fallback to query parameter (for backward compatibility)
    if not authenticated:
        authenticated_user = request.args.get('authenticated_user')
        if authenticated_user:
            authenticated = True
    
    # If neither method worked, return unauthorized
    if not authenticated:
        return jsonify({'message': 'User not authenticated'}), 401
        
    try:
        file = request.files.get('file')
        if not file:
            return jsonify({'message': 'No file provided'}), 400
        
        # Check file format and read accordingly
        filename = file.filename.lower()
        
        if filename.endswith('.csv'):
            # Read CSV file
            file_content = file.read().decode('utf-8')
            csv_data = pd.read_csv(io.StringIO(file_content))
        elif filename.endswith(('.xlsx', '.xls')):
            # Read Excel file
            csv_data = pd.read_excel(file)
        elif filename.endswith('.json'):
            # Read JSON file
            file_content = file.read().decode('utf-8')
            json_data = json.loads(file_content)
            csv_data = pd.DataFrame(json_data)
        elif filename.endswith('.txt'):
            # Read tab-separated or pipe-separated text file
            file_content = file.read().decode('utf-8')
            # Try different separators
            try:
                csv_data = pd.read_csv(io.StringIO(file_content), sep='\t')
            except:
                try:
                    csv_data = pd.read_csv(io.StringIO(file_content), sep='|')
                except:
                    csv_data = pd.read_csv(io.StringIO(file_content), sep=',')
        else:
            return jsonify({
                'message': 'Unsupported file format. Supported formats: CSV, Excel (.xlsx, .xls), JSON, TXT'
            }), 400
        
        # Clean column names (remove extra spaces)
        csv_data.columns = csv_data.columns.str.strip()
        
        # Expected columns for currency conversion CSV
        expected_columns = [
            'user_currency', 'country', 'selected_currency', 
            'month', 'year', 'conversion_rate'
        ]
        
        # Check if all expected columns exist
        missing_columns = [col for col in expected_columns if col not in csv_data.columns]
        if missing_columns:
            return jsonify({
                'message': f'Missing required columns: {", ".join(missing_columns)}',
                'expected_columns': expected_columns,
                'available_columns': list(csv_data.columns)
            }), 400
        
        # Process and store data
        success_count = 0
        error_count = 0
        errors = []
        
        for index, row in csv_data.iterrows():
            try:
                # Handle empty/NaN values
                def safe_get(value, default=None):
                    if pd.isna(value) or value == '' or str(value).strip() == '':
                        return default
                    return str(value).strip()
                
                def safe_get_float(value, default=0.0):
                    if pd.isna(value) or value == '':
                        return default
                    try:
                        return float(value)
                    except:
                        return default
                
                def safe_get_int(value, default=None):
                    if pd.isna(value) or value == '':
                        return default
                    try:
                        return int(value)
                    except:
                        return default
                
                # Validate required fields
                user_currency = safe_get(row['user_currency'])
                country = safe_get(row['country'])
                selected_currency = safe_get(row['selected_currency'])
                month = safe_get(row['month'])
                year = safe_get_int(row['year'])
                conversion_rate = safe_get_float(row['conversion_rate'])
                
                # Check for required fields
                if not user_currency or not country or not selected_currency or not month or not year or conversion_rate == 0.0:
                    error_count += 1
                    errors.append(f"Row {index + 2}: Missing required data")
                    continue
                
                # Check if record already exists
                existing_record = CurrencyConversion.query.filter_by(
                    user_currency=user_currency,
                    country=country,
                    selected_currency=selected_currency,
                    month=month,
                    year=year
                ).first()
                
                if existing_record:
                    # Update existing record
                    existing_record.conversion_rate = conversion_rate
                else:
                    # Create new CurrencyConversion instance
                    currency_conversion = CurrencyConversion(
                        user_currency=user_currency,
                        country=country,
                        selected_currency=selected_currency,
                        month=month,
                        year=year,
                        conversion_rate=conversion_rate
                    )
                    
                    # Add to database session
                    db.session.add(currency_conversion)
                
                success_count += 1
                
            except Exception as e:
                error_count += 1
                errors.append(f"Row {index + 2}: {str(e)}")  # +2 because CSV rows start from 1 and we have header
        
        # Commit all changes
        try:
            db.session.commit()
            
            response_data = {
                'message': f'Currency conversion file processed successfully. {success_count} records processed.',
                'success_count': success_count,
                'error_count': error_count,
                'total_rows': len(csv_data)
            }
            
            if errors:
                response_data['errors'] = errors[:10]  # Limit to first 10 errors
                if len(errors) > 10:
                    response_data['additional_errors'] = len(errors) - 10
            
            return jsonify(response_data), 200
            
        except IntegrityError as e:
            db.session.rollback()
            return jsonify({
                'message': 'Database integrity error. Some records might conflict with existing data.',
                'error': str(e.orig) if hasattr(e, 'orig') else str(e)
            }), 400
            
    except (pd.errors.EmptyDataError, pd.errors.ParserError):
        return jsonify({'message': 'Invalid file format or the file is empty'}), 400
    except json.JSONDecodeError:
        return jsonify({'message': 'Invalid JSON format'}), 400
    except Exception as e:
        db.session.rollback()
        return jsonify({
            'message': 'An error occurred while processing the currency conversion file',
            'error': str(e)
        }), 500
        
@superadmin_dashboard_bp.route('/superadmin/dashboard/view_currency_file', methods=['GET'])
def view_currency_file():
    try:
        records = CurrencyConversion.query.all()
        
        result = []
        for record in records:
            result.append({
                'user_currency': record.user_currency,
                'country': record.country,
                'selected_currency': record.selected_currency,
                'month': record.month,
                'year': record.year,
                'conversion_rate': record.conversion_rate
            })

        return jsonify({
            'message': f'{len(result)} records found.',
            'data': result
        }), 200

    except Exception as e:
        return jsonify({
            'message': 'An error occurred while fetching currency conversion data.',
            'error': str(e)
        }), 500
        
        
@superadmin_dashboard_bp.route('/superadmin/dashboard/upload_referral_fee', methods=['POST'])
def upload_referral_fee():
    # Authentication check - prioritize token over query parameter
    authenticated = False
    
    # Method 1: Check Authorization header (recommended)
    auth_header = request.headers.get('Authorization')
    if auth_header and auth_header.startswith('Bearer '):
        token = auth_header.split(' ')[1]
        try:
            # Verify the token
            decoded_token = jwt.decode(token, SECRET_KEY, algorithms=['HS256'])
            if decoded_token.get('is_superadmin'):
                authenticated = True
        except jwt.ExpiredSignatureError:
            return jsonify({'message': 'Token has expired'}), 401
        except jwt.InvalidTokenError:
            return jsonify({'message': 'Invalid token'}), 401
    
    # Method 2: Fallback to query parameter (for backward compatibility)
    if not authenticated:
        authenticated_user = request.args.get('authenticated_user')
        if authenticated_user:
            authenticated = True
    
    # If neither method worked, return unauthorized
    if not authenticated:
        return jsonify({'message': 'User not authenticated'}), 401
    
    try:
        file = request.files.get('file')
        if not file:
            return jsonify({'message': 'No file provided'}), 400
        
        # Check file format and read accordingly
        filename = file.filename.lower()
        
        if filename.endswith('.csv'):
            # Read CSV file
            file_content = file.read().decode('utf-8')
            csv_data = pd.read_csv(io.StringIO(file_content))
        elif filename.endswith(('.xlsx', '.xls')):
            # Read Excel file
            csv_data = pd.read_excel(file)
        elif filename.endswith('.json'):
            # Read JSON file
            file_content = file.read().decode('utf-8')
            json_data = json.loads(file_content)
            csv_data = pd.DataFrame(json_data)
        elif filename.endswith('.txt'):
            # Read tab-separated or pipe-separated text file
            file_content = file.read().decode('utf-8')
            # Try different separators
            try:
                csv_data = pd.read_csv(io.StringIO(file_content), sep='\t')
            except:
                try:
                    csv_data = pd.read_csv(io.StringIO(file_content), sep='|')
                except:
                    csv_data = pd.read_csv(io.StringIO(file_content), sep=',')
        else:
            return jsonify({
                'message': 'Unsupported file format. Supported formats: CSV, Excel (.xlsx, .xls), JSON, TXT'
            }), 400
        
        # Clean column names (remove extra spaces)
        csv_data.columns = csv_data.columns.str.strip()
        
        # Expected columns for referral fee CSV
        expected_columns = [
            'country', 'category', 'subcategory', 
            'referral_fee', 'price_from', 'price_to'
        ]
        
        # Check if all expected columns exist
        missing_columns = [col for col in expected_columns if col not in csv_data.columns]
        if missing_columns:
            return jsonify({
                'message': f'Missing required columns: {", ".join(missing_columns)}',
                'expected_columns': expected_columns,
                'available_columns': list(csv_data.columns)
            }), 400
        
        # Process and store data
        success_count = 0
        error_count = 0
        errors = []
        
        for index, row in csv_data.iterrows():
            try:
                # Handle empty/NaN values
                def safe_get(value, default=None):
                    if pd.isna(value) or value == '' or str(value).strip() == '':
                        return default
                    return value
                
                # Extract and validate data
                country = safe_get(row['country'])
                category = safe_get(row['category'])
                subcategory = safe_get(row['subcategory'])
                referral_fee = safe_get(row['referral_fee'])
                price_from = safe_get(row['price_from'])
                price_to = safe_get(row['price_to'])
                
                # Validate required fields
                if not all([country, category, subcategory]):
                    errors.append(f'Row {index + 1}: Missing required text fields')
                    error_count += 1
                    continue
                
                # Convert numeric fields
                try:
                    referral_fee = float(referral_fee) if referral_fee is not None else 0.0
                    price_from = float(price_from) if price_from is not None else 0.0
                    price_to = float(price_to) if price_to is not None else 0.0
                except (ValueError, TypeError):
                    errors.append(f'Row {index + 1}: Invalid numeric values')
                    error_count += 1
                    continue
                
                # Validate numeric ranges
                if price_from < 0 or price_to < 0 or referral_fee < 0:
                    errors.append(f'Row {index + 1}: Negative values not allowed')
                    error_count += 1
                    continue

# Allow price_to == 0 to mean "no upper limit"
                if price_to != 0 and price_from > price_to:
                    errors.append(f'Row {index + 1}: price_from cannot be greater than price_to (unless price_to = 0)')
                    error_count += 1
                    continue
                
                # Check for existing record (to update or create new)
                existing_record = Category.query.filter_by(
                    country=str(country).strip(),
                    category=str(category).strip(),
                    subcategory=str(subcategory).strip(),
                    price_from=price_from,
                    price_to=price_to
                ).first()
                
                if existing_record:
                    # Update existing record
                    existing_record.referral_fee = referral_fee
                else:
                    # Create new record
                    new_category = Category(
                        country=str(country).strip(),
                        category=str(category).strip(),
                        subcategory=str(subcategory).strip(),
                        referral_fee=referral_fee,
                        price_from=price_from,
                        price_to=price_to
                    )
                    db.session.add(new_category)
                
                success_count += 1
                
            except Exception as e:
                errors.append(f'Row {index + 1}: {str(e)}')
                error_count += 1
                continue
        
        # Commit all changes
        try:
            db.session.commit()
            
            # Prepare response
            response_data = {
                'message': 'File processed successfully',
                'success_count': success_count,
                'error_count': error_count,
                'total_rows': len(csv_data)
            }
            
            if errors:
                response_data['errors'] = errors[:10]  # Limit to first 10 errors
                if len(errors) > 10:
                    response_data['additional_errors'] = len(errors) - 10
            
            status_code = 200 if error_count == 0 else 207  # 207 for partial success
            return jsonify(response_data), status_code
            
        except Exception as e:
            db.session.rollback()
            return jsonify({
                'message': 'Database error occurred',
                'error': str(e)
            }), 500
            
    except Exception as e:
        return jsonify({
            'message': 'An error occurred while processing the file',
            'error': str(e)
        }), 500        



@superadmin_dashboard_bp.route("/superadmin/dashboard/members", methods=["GET"])
def get_members():
    ok, err = _is_superadmin_authenticated()
    if not ok:
        payload, code = err
        return jsonify(payload), code

    try:
        user_email = request.args.get("email")

        if not user_email:
            return jsonify({
                "message": "Email parameter is required"
            }), 400

        # Find user by email
        user = User.query.filter_by(email=user_email).first()

        if not user:
            return jsonify({
                "message": f"No user found with email {user_email}"
            }), 404

        # Get only members belonging to this user
        members = Member.query.filter_by(owner_user_id=user.id).order_by(Member.id.asc()).all()

        return jsonify({
            "message": f"{len(members)} member record(s) found.",
            "user_email": user.email,
            "user_id": user.id,
            "data": [
                {
                    "id": m.id,
                    "owner_user_id": m.owner_user_id,
                    "email": m.email,
                    "member_name": m.member_name,
                    "role": m.role,
                    "marketplace_ids": m.marketplace_ids,
                    "modules": m.modules,
                    "countries": m.countries,
                    "is_verified": m.is_verified,
                    "status": "Active" if m.is_verified else "Inactive"
                }
                for m in members
            ]
        }), 200

    except Exception as e:
        return jsonify({
            "message": "An error occurred while fetching member details.",
            "error": str(e)
        }), 500


@superadmin_dashboard_bp.route('/superadmin/dashboard/update_user_status', methods=['POST'])
def update_user_status():
    ok, err = _is_superadmin_authenticated()
    if not ok:
        payload, code = err
        return jsonify(payload), code

    try:
        data = request.get_json()

        user_id = data.get("user_id")
        status = data.get("status")

        if user_id is None:
            return jsonify({"message": "user_id is required"}), 400

        if status is None:
            return jsonify({"message": "status is required"}), 400

        user = User.query.filter_by(id=user_id).first()

        if not user:
            return jsonify({"message": "User not found"}), 404

        user.status = bool(status)
        db.session.commit()

        return jsonify({
            "message": "User status updated successfully",
            "user_id": user.id,
            "status": user.status
        }), 200

    except Exception as e:
        db.session.rollback()
        return jsonify({
            "message": "Error updating user status",
            "error": str(e)
        }), 500
    


@superadmin_dashboard_bp.route("/amazon_api/formula_update", methods=["GET"])
def formula_update():

    # =========================================================
    # 1. Superadmin authentication
    # =========================================================
    auth_header = request.headers.get("Authorization")

    if not auth_header or not auth_header.startswith("Bearer "):
        return jsonify({
            "success": False,
            "error": "Authorization token is missing or invalid",
        }), 401

    token = auth_header.split(" ", 1)[1]

    try:
        decoded = jwt.decode(
            token,
            SECRET_KEY,
            algorithms=["HS256"],
        )

        if not decoded.get("is_superadmin"):
            return jsonify({
                "success": False,
                "error": "Only superadmin can run formula update",
            }), 403

    except jwt.ExpiredSignatureError:
        return jsonify({
            "success": False,
            "error": "Token has expired",
        }), 401

    except jwt.InvalidTokenError:
        return jsonify({
            "success": False,
            "error": "Invalid token",
        }), 401

    # =========================================================
    # 2. Optional filters
    # =========================================================
    requested_user_id = request.args.get("user_id", type=int)

    requested_country = (
        request.args.get("country") or ""
    ).strip().lower()

    requested_year = request.args.get("year", type=int)
    requested_month = request.args.get("month", type=int)

    if requested_month is not None and not 1 <= requested_month <= 12:
        return jsonify({
            "success": False,
            "error": "month must be between 1 and 12",
        }), 400

    month_names = {
        1: "january",
        2: "february",
        3: "march",
        4: "april",
        5: "may",
        6: "june",
        7: "july",
        8: "august",
        9: "september",
        10: "october",
        11: "november",
        12: "december",
    }

    quarter_mapping = {
        "january": "Q1",
        "february": "Q1",
        "march": "Q1",
        "april": "Q2",
        "may": "Q2",
        "june": "Q2",
        "july": "Q3",
        "august": "Q3",
        "september": "Q3",
        "october": "Q4",
        "november": "Q4",
        "december": "Q4",
    }

    excluded_countries = {
        "global",
        "global_inr",
        "global_cad",
        "global_gbp",
        "uk_usd",
    }

    def normalize_country(value):
        value = (value or "").strip().lower()

        aliases = {
            "united kingdom": "uk",
            "great britain": "uk",
            "gb": "uk",
            "united states": "us",
            "usa": "us",
        }

        return aliases.get(value, value)

    requested_country = normalize_country(requested_country)

    # =========================================================
    # 3. Find active users
    # =========================================================
    user_query = User.query.filter_by(status=True)

    if requested_user_id is not None:
        user_query = user_query.filter(User.id == requested_user_id)

    users = user_query.order_by(User.id.asc()).all()

    if not users:
        return jsonify({
            "success": False,
            "error": "No active users found",
        }), 404

    engine = db.get_engine()
    inspector = inspect(engine)
    all_tables = set(inspector.get_table_names())

    # =========================================================
    # 4. Find existing raw monthly tables
    # =========================================================
    source_pattern = re.compile(
        r"^user_(?P<user_id>\d+)_"
        r"(?P<country>[a-zA-Z]+)_"
        r"(?P<month>"
        r"january|february|march|april|may|june|"
        r"july|august|september|october|november|december"
        r")"
        r"(?P<year>\d{4})_data$",
        re.IGNORECASE,
    )

    active_user_ids = {user.id for user in users}
    months_to_process = []

    for table_name in all_tables:
        match = source_pattern.match(table_name)

        if not match:
            continue

        user_id = int(match.group("user_id"))
        country = normalize_country(match.group("country"))
        month = match.group("month").lower()
        year = int(match.group("year"))

        if user_id not in active_user_ids:
            continue

        if country in excluded_countries:
            continue

        # Your formula functions currently support UK and US.
        if country not in {"uk", "us"}:
            continue

        if requested_country and country != requested_country:
            continue

        if requested_year is not None and year != requested_year:
            continue

        if requested_month is not None:
            requested_month_name = month_names[requested_month]

            if month != requested_month_name:
                continue

        months_to_process.append({
            "user_id": user_id,
            "country": country,
            "month": month,
            "year": year,
            "source_table": table_name,
        })

    months_to_process.sort(
        key=lambda row: (
            row["user_id"],
            row["country"],
            row["year"],
            list(month_names.values()).index(row["month"]),
        )
    )

    if not months_to_process:
        return jsonify({
            "success": False,
            "error": "No existing source tables found",
            "expected_table_format": (
                "user_{user_id}_{country}_{month}{year}_data"
            ),
            "filters": {
                "user_id": requested_user_id,
                "country": requested_country or None,
                "year": requested_year,
                "month": requested_month,
            },
        }), 404

    # =========================================================
    # 5. Process monthly tables first
    # =========================================================
    monthly_results = []
    successful_periods = []
    monthly_success_count = 0
    monthly_failed_count = 0

    for item in months_to_process:
        user_id = item["user_id"]
        country = item["country"]
        month = item["month"]
        year = item["year"]
        source_table = item["source_table"]

        try:
            if country == "uk":
                result = process_skuwise_data(
                    user_id,
                    country,
                    month,
                    year,
                )

            elif country == "us":
                result = process_skuwise_us_data(
                    user_id,
                    country,
                    month,
                    year,
                )

            else:
                raise ValueError(
                    f"Unsupported country: {country}"
                )

            monthly_success_count += 1

            successful_periods.append({
                "user_id": user_id,
                "country": country,
                "month": month,
                "year": year,
            })

            monthly_results.append({
                "success": True,
                "user_id": user_id,
                "country": country,
                "month": month,
                "year": year,
                "source_table": source_table,
                "result_returned": result is not None,
            })

        except Exception as exc:
            monthly_failed_count += 1

            monthly_results.append({
                "success": False,
                "user_id": user_id,
                "country": country,
                "month": month,
                "year": year,
                "source_table": source_table,
                "error": str(exc),
            })

    # =========================================================
    # 6. Rebuild each affected quarter once
    # =========================================================
    affected_quarters = {
        (
            item["user_id"],
            item["country"],
            item["year"],
            item["month"],
            quarter_mapping[item["month"]],
        )
        for item in successful_periods
    }

    # Multiple processed months can belong to the same quarter.
    unique_quarters = {}

    for (
        user_id,
        country,
        year,
        month,
        quarter,
    ) in affected_quarters:
        key = (
            user_id,
            country,
            year,
            quarter,
        )

        # Any month inside the quarter can be passed because your
        # quarterly functions determine the quarter from the month.
        unique_quarters[key] = month

    quarterly_results = []
    quarterly_success_count = 0
    quarterly_failed_count = 0

    for (
        user_id,
        country,
        year,
        quarter,
    ), month in sorted(unique_quarters.items()):

        try:
            if country == "uk":
                result = process_quarterly_skuwise_data(
                    user_id,
                    country,
                    month,
                    year,
                    quarter,
                    db_url,
                )

            elif country == "us":
                result = process_us_quarterly_skuwise_data(
                    user_id,
                    country,
                    month,
                    year,
                    quarter,
                    db_url,
                )

            else:
                raise ValueError(
                    f"Unsupported country: {country}"
                )

            quarterly_success_count += 1

            quarterly_results.append({
                "success": True,
                "user_id": user_id,
                "country": country,
                "year": year,
                "quarter": quarter,
                "month_argument": month,
                "result_returned": result is not None,
            })

        except Exception as exc:
            quarterly_failed_count += 1

            quarterly_results.append({
                "success": False,
                "user_id": user_id,
                "country": country,
                "year": year,
                "quarter": quarter,
                "month_argument": month,
                "error": str(exc),
            })

    # =========================================================
    # 7. Rebuild each affected year once
    # =========================================================
    affected_years = {
        (
            item["user_id"],
            item["country"],
            item["year"],
        )
        for item in successful_periods
    }

    yearly_results = []
    yearly_success_count = 0
    yearly_failed_count = 0

    for user_id, country, year in sorted(affected_years):
        try:
            if country == "uk":
                result = process_yearly_skuwise_data(
                    user_id,
                    country,
                    year,
                )

            elif country == "us":
                result = process_us_yearly_skuwise_data(
                    user_id,
                    country,
                    year,
                )

            else:
                raise ValueError(
                    f"Unsupported country: {country}"
                )

            yearly_success_count += 1

            yearly_results.append({
                "success": True,
                "user_id": user_id,
                "country": country,
                "year": year,
                "result_returned": result is not None,
            })

        except Exception as exc:
            yearly_failed_count += 1

            yearly_results.append({
                "success": False,
                "user_id": user_id,
                "country": country,
                "year": year,
                "error": str(exc),
            })

    # =========================================================
    # 8. Rebuild global formula tables
    # =========================================================
    global_monthly_results = []
    global_quarterly_results = []
    global_yearly_results = []

    global_monthly_success = 0
    global_monthly_failed = 0
    global_quarterly_success = 0
    global_quarterly_failed = 0
    global_yearly_success = 0
    global_yearly_failed = 0

    for item in successful_periods:
        try:
            result = process_global_monthly_skuwise_data(
                item["user_id"],
                item["country"],
                item["year"],
                item["month"],
            )

            global_monthly_success += 1

            global_monthly_results.append({
                "success": True,
                **item,
                "result_returned": result is not None,
            })

        except Exception as exc:
            global_monthly_failed += 1

            global_monthly_results.append({
                "success": False,
                **item,
                "error": str(exc),
            })

    for (
        user_id,
        country,
        year,
        quarter,
    ), month in sorted(unique_quarters.items()):

        try:
            result = process_global_quarterly_skuwise_data(
                user_id,
                country,
                month,
                year,
                quarter,
                db_url,
            )

            global_quarterly_success += 1

            global_quarterly_results.append({
                "success": True,
                "user_id": user_id,
                "country": country,
                "month": month,
                "year": year,
                "quarter": quarter,
                "result_returned": result is not None,
            })

        except Exception as exc:
            global_quarterly_failed += 1

            global_quarterly_results.append({
                "success": False,
                "user_id": user_id,
                "country": country,
                "month": month,
                "year": year,
                "quarter": quarter,
                "error": str(exc),
            })

    for user_id, country, year in sorted(affected_years):
        try:
            result = process_global_yearly_skuwise_data(
                user_id,
                country,
                year,
            )

            global_yearly_success += 1

            global_yearly_results.append({
                "success": True,
                "user_id": user_id,
                "country": country,
                "year": year,
                "result_returned": result is not None,
            })

        except Exception as exc:
            global_yearly_failed += 1

            global_yearly_results.append({
                "success": False,
                "user_id": user_id,
                "country": country,
                "year": year,
                "error": str(exc),
            })

    # =========================================================
    # 9. Response
    # =========================================================
    total_failed = (
        monthly_failed_count
        + quarterly_failed_count
        + yearly_failed_count
        + global_monthly_failed
        + global_quarterly_failed
        + global_yearly_failed
    )

    return jsonify({
        "success": total_failed == 0,
        "message": (
            "Formula update completed using existing database tables. "
            "Amazon SP-API was not called."
        ),
        "amazon_fetch_performed": False,
        "source_table_format": (
            "user_{user_id}_{country}_{month}{year}_data"
        ),
        "filters": {
            "user_id": requested_user_id,
            "country": requested_country or None,
            "year": requested_year,
            "month": requested_month,
        },
        "source_tables_found": len(months_to_process),
        "monthly": {
            "attempted": len(months_to_process),
            "success_count": monthly_success_count,
            "failed_count": monthly_failed_count,
            "results": monthly_results,
        },
        "quarterly": {
            "attempted": len(unique_quarters),
            "success_count": quarterly_success_count,
            "failed_count": quarterly_failed_count,
            "results": quarterly_results,
        },
        "yearly": {
            "attempted": len(affected_years),
            "success_count": yearly_success_count,
            "failed_count": yearly_failed_count,
            "results": yearly_results,
        },
        "global_monthly": {
            "attempted": len(successful_periods),
            "success_count": global_monthly_success,
            "failed_count": global_monthly_failed,
            "results": global_monthly_results,
        },
        "global_quarterly": {
            "attempted": len(unique_quarters),
            "success_count": global_quarterly_success,
            "failed_count": global_quarterly_failed,
            "results": global_quarterly_results,
        },
        "global_yearly": {
            "attempted": len(affected_years),
            "success_count": global_yearly_success,
            "failed_count": global_yearly_failed,
            "results": global_yearly_results,
        },
    }), 200 if total_failed == 0 else 207

