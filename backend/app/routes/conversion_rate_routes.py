# routes/conversion_routes.py
from __future__ import annotations

import re
import logging
import datetime as dt
from typing import Iterator, Tuple, Optional

import requests
from flask import Blueprint, jsonify, request
from sqlalchemy import func

from app import db
from app.models.user_models import CurrencyConversion

logger = logging.getLogger("currency_conversion")
logging.basicConfig(level=logging.INFO)

conversion_bp = Blueprint("conversion", __name__)

# ------------------------------- Constants ------------------------------------

MONTHS_REVERSE_MAP = {
    1: "january", 2: "february", 3: "march", 4: "april", 5: "may", 6: "june",
    7: "july", 8: "august", 9: "september", 10: "october", 11: "november", 12: "december"
}

ISO_CCY_RE = re.compile(r"^[A-Za-z]{3}$")

# The currency combos to upsert each month
# All values are lowercase (user_currency, country, selected_currency)
SEED_COMBOS: list[Tuple[str, str, str]] = [
    # (user_currency, country, selected_currency)
    ("usd", "uk",     "gbp"),
    ("usd", "india",  "inr"),
    ("gbp", "us",     "usd"),
    ("gbp", "india",  "inr"),
    ("inr", "us",     "usd"),
    ("usd", "us",     "usd"),   # identity
    ("inr", "uk",     "gbp"),

    # --------- CAD / CANADA combos ----------
    ("usd", "canada", "cad"),
    ("gbp", "canada", "cad"),
    ("inr", "canada", "cad"),
    ("cad", "us",     "usd"),
    ("cad", "uk",     "gbp"),
    ("cad", "india",  "inr"),
    ("cad", "canada", "cad"),   # identity
]

# ------------------------------- Utilities ------------------------------------

def _normalize_month(m: str | int) -> str:
    if isinstance(m, int) or (isinstance(m, str) and str(m).isdigit()):
        num = int(m)
        if not 1 <= num <= 12:
            raise ValueError("month must be 1..12")
        return MONTHS_REVERSE_MAP[num].lower()
    return str(m).strip().lower()

def _month_to_number(month: str | int) -> int:
    """Return 1..12 for either numeric or month-name inputs."""
    if isinstance(month, int) or (isinstance(month, str) and str(month).isdigit()):
        n = int(month)
        if not 1 <= n <= 12:
            raise ValueError("month must be 1..12")
        return n
    m = str(month).strip().lower()
    name_to_num = {v.lower(): k for k, v in MONTHS_REVERSE_MAP.items()}
    if m not in name_to_num:
        raise ValueError("month must be 1..12 or a valid month name")
    return name_to_num[m]

def _validate_ccy(ccy: str, field: str):
    if not ccy or not ISO_CCY_RE.match(ccy):
        raise ValueError(f"{field} must be a 3-letter currency code (e.g. USD, INR)")

def _month_year_iter(
    start_year: int, start_month: int, end_year: int, end_month: int
) -> Iterator[Tuple[int, int]]:
    """Yield (year, month_number) inclusive from start to end."""
    y, m = start_year, start_month
    while (y < end_year) or (y == end_year and m <= end_month):
        yield y, m
        m += 1
        if m == 13:
            m = 1
            y += 1

# ------------------------- External FX Provider -------------------------------

def fetch_rate_from_provider(base_ccy: str, quote_ccy: str, year: int, month: str | int) -> float:
    """
    Gets a monthly rate (middle of month) from Frankfurter.
    If the target month is in the future, it uses the latest endpoint.
    """
    _validate_ccy(base_ccy, "user_currency")
    _validate_ccy(quote_ccy, "selected_currency")

    month_num = _month_to_number(month)
    target_date = dt.date(int(year), month_num, 15)
    today = dt.date.today()

    if target_date > today:
        url = "https://api.frankfurter.app/latest"
    else:
        url = f"https://api.frankfurter.app/{target_date.isoformat()}"

    params = {"from": base_ccy.upper(), "to": quote_ccy.upper()}

    try:
        resp = requests.get(url, params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()
    except requests.RequestException as e:
        raise RuntimeError(f"FX provider request failed: {e}") from e

    rates = (data or {}).get("rates") or {}
    rate_val = rates.get(quote_ccy.upper())
    if rate_val is None:
        raise RuntimeError(f"FX provider returned unexpected response: {data}")

    rate = float(rate_val)
    if rate <= 0:
        raise RuntimeError("FX provider returned non-positive rate")
    return rate

# ------------------------------ DB Upsert -------------------------------------

def upsert_conversion_rate(
    *,
    user_currency: str,
    country: str,
    selected_currency: str,
    month: str | int,
    year: int,
    rate: float | None = None,
    fetch_if_missing: bool = True,
) -> CurrencyConversion:
    """
    Insert or update a currency rate for the (user_currency, country, selected_currency, month, year) key.
    All fields are stored in lowercase in the database.
    """
    _validate_ccy(user_currency, "user_currency")
    _validate_ccy(selected_currency, "selected_currency")

    month_norm = _normalize_month(month).lower()
    user_currency_norm = str(user_currency).strip().lower()
    selected_currency_norm = str(selected_currency).strip().lower()
    country_norm = str(country).strip().lower()
    year = int(year)

    if not country_norm:
        raise ValueError("country is required")

    if rate is None:
        if not fetch_if_missing:
            raise ValueError("rate is required when fetch_if_missing=False")
        rate = fetch_rate_from_provider(user_currency_norm, selected_currency_norm, year, month_norm)

    rate = float(rate)

    existing = (
        db.session.query(CurrencyConversion)
        .filter(
            func.lower(CurrencyConversion.user_currency) == user_currency_norm,
            func.lower(CurrencyConversion.country) == country_norm,
            func.lower(CurrencyConversion.selected_currency) == selected_currency_norm,
            func.lower(CurrencyConversion.month) == month_norm,
            CurrencyConversion.year == year,
        )
        .order_by(CurrencyConversion.id.desc())
        .first()
    )

    if existing:
        existing.conversion_rate = rate
        db.session.add(existing)
        db.session.commit()
        return existing

    row = CurrencyConversion(
        user_currency=user_currency_norm,
        country=country_norm,
        selected_currency=selected_currency_norm,
        month=month_norm,
        year=year,
        conversion_rate=rate,
    )
    db.session.add(row)
    db.session.commit()
    return row

# --------------------- Auto-seed "new month" helper ---------------------------

def ensure_month_seeded(*, year: int, month: str | int) -> bool:
    """
    Ensure all SEED_COMBOS exist for (year, month).

    Returns:
      True  -> current month was already seeded in DB (NO fetch/store happened)
      False -> some were missing; we fetched/stored them now
    """
    month_norm = _normalize_month(month).lower()
    year = int(year)

    existing_rows = (
        db.session.query(
            func.lower(CurrencyConversion.user_currency),
            func.lower(CurrencyConversion.country),
            func.lower(CurrencyConversion.selected_currency),
        )
        .filter(
            func.lower(CurrencyConversion.month) == month_norm,
            CurrencyConversion.year == year,
        )
        .all()
    )
    existing_keys = {(uc, c, sc) for (uc, c, sc) in existing_rows}
    needed_keys = {(uc.lower(), c.lower(), sc.lower()) for (uc, c, sc) in SEED_COMBOS}

    missing = needed_keys - existing_keys
    if not missing:
        return True

    for user_ccy, country, sel_ccy in missing:
        if user_ccy == sel_ccy:
            upsert_conversion_rate(
                user_currency=user_ccy,
                country=country,
                selected_currency=sel_ccy,
                month=month_norm,
                year=year,
                rate=1.0,
                fetch_if_missing=False,
            )
        else:
            upsert_conversion_rate(
                user_currency=user_ccy,
                country=country,
                selected_currency=sel_ccy,
                month=month_norm,
                year=year,
                rate=None,
                fetch_if_missing=True,
            )

    return False

# ------------------------------- HTTP Route -----------------------------------

@conversion_bp.route("/currency-rate", methods=["POST"])
def api_create_or_update_currency_rate():
    data = request.get_json(silent=True) or request.form

    # ---------------- Auto seed current month (new month logic) ----------------
    # If current month data exists => current_month_already_seeded = True
    # If not, it fetches + stores => current_month_already_seeded = False
    # If seed fails => current_month_already_seeded = "error"
    auto_seed_current_month = str(data.get("auto_seed_current_month", "true")).lower() in (
        "1", "true", "yes", "y", "on"
    )

    current_month_already_seeded: Optional[bool | str] = None
    if auto_seed_current_month:
        today = dt.date.today()
        current_year = today.year
        current_month_name = MONTHS_REVERSE_MAP[today.month]
        try:
            current_month_already_seeded = ensure_month_seeded(
                year=current_year,
                month=current_month_name,
            )
        except Exception:
            db.session.rollback()
            logger.exception("Auto seed current month failed")
            current_month_already_seeded = "error"

    # ---------------- Seed-only mode (so Postman can call only this) ----------
    seed_only = str(data.get("seed_only", "")).lower() in ("1", "true", "yes", "y", "on")
    if seed_only:
        ok = current_month_already_seeded != "error"
        return jsonify({
            "success": ok,
            "message": "Current month seed checked" if ok else "Current month seed failed",
            "current_month_already_seeded": current_month_already_seeded,
        }), 200 if ok else 500

    # ----------------------- Bulk / seed mode ---------------------------------
    seed_all = str(data.get("seed_all", "")).lower() in ("1", "true", "yes", "y", "on")
    if seed_all:
        try:
            start_year = int(data.get("start_year", 2024))
            start_month = data.get("start_month", "january")
            end_year = int(data.get("end_year", dt.date.today().year))
            end_month = data.get("end_month", MONTHS_REVERSE_MAP[dt.date.today().month])

            start_month_num = _month_to_number(start_month)
            end_month_num = _month_to_number(end_month)

            results = []
            errors = []

            for y, mnum in _month_year_iter(start_year, start_month_num, end_year, end_month_num):
                month_name = MONTHS_REVERSE_MAP[mnum]

                for user_ccy, country, sel_ccy in SEED_COMBOS:
                    try:
                        if user_ccy.lower() == sel_ccy.lower():
                            row = upsert_conversion_rate(
                                user_currency=user_ccy,
                                country=country,
                                selected_currency=sel_ccy,
                                month=month_name,
                                year=y,
                                rate=1.0,
                                fetch_if_missing=False,
                            )
                        else:
                            row = upsert_conversion_rate(
                                user_currency=user_ccy,
                                country=country,
                                selected_currency=sel_ccy,
                                month=month_name,
                                year=y,
                                rate=None,
                                fetch_if_missing=True,
                            )

                        results.append({
                            "id": row.id,
                            "user_currency": row.user_currency,
                            "country": row.country,
                            "selected_currency": row.selected_currency,
                            "month": row.month,
                            "year": row.year,
                            "conversion_rate": row.conversion_rate,
                        })
                    except Exception as e:
                        errors.append({
                            "user_currency": user_ccy,
                            "country": country,
                            "selected_currency": sel_ccy,
                            "month": month_name,
                            "year": y,
                            "error": str(e),
                        })

            return jsonify({
                "success": True,
                "message": "Bulk currency conversions upserted",
                "inserted_or_updated": len(results),
                "errors": errors,
                "preview": results[:10],
                "current_month_already_seeded": current_month_already_seeded,
            }), 200

        except Exception as e:
            db.session.rollback()
            logger.exception("Bulk seed failed")
            return jsonify({
                "success": False,
                "error": "Bulk seed failed",
                "details": str(e),
                "current_month_already_seeded": current_month_already_seeded,
            }), 500

    # ----------------------- Single-row mode (original) -----------------------
    user_currency = (data.get("user_currency") or "").strip()
    country = (data.get("country") or "").strip()
    selected_currency = (data.get("selected_currency") or "").strip()
    month = data.get("month")
    year = data.get("year")
    rate = data.get("rate")
    fetch_if_missing = data.get("fetch_if_missing", True)

    missing = [
        k for k, v in {
            "user_currency": user_currency,
            "country": country,
            "selected_currency": selected_currency,
            "month": month,
            "year": year,
        }.items() if v in (None, "", [])
    ]
    if missing:
        return jsonify({
            "success": False,
            "error": f"Missing required fields: {', '.join(missing)}",
            "current_month_already_seeded": current_month_already_seeded,
        }), 400

    try:
        year = int(year)
        if isinstance(fetch_if_missing, str):
            fetch_if_missing = fetch_if_missing.lower() in ("1", "true", "yes", "y", "on")

        if rate is not None and rate != "":
            rate = float(rate)
        else:
            rate = None

        row = upsert_conversion_rate(
            user_currency=user_currency,
            country=country,
            selected_currency=selected_currency,
            month=month,
            year=year,
            rate=rate,
            fetch_if_missing=fetch_if_missing,
        )

        return jsonify({
            "success": True,
            "message": "Conversion rate stored",
            "current_month_already_seeded": current_month_already_seeded,
            "record": {
                "id": row.id,
                "user_currency": row.user_currency,
                "country": row.country,
                "selected_currency": row.selected_currency,
                "month": row.month,
                "year": row.year,
                "conversion_rate": row.conversion_rate,
            }
        }), 200

    except ValueError as ve:
        return jsonify({
            "success": False,
            "error": str(ve),
            "current_month_already_seeded": current_month_already_seeded,
        }), 400
    except requests.HTTPError as he:
        return jsonify({
            "success": False,
            "error": "FX provider HTTP error",
            "details": str(he),
            "current_month_already_seeded": current_month_already_seeded,
        }), 502
    except requests.RequestException as rexc:
        return jsonify({
            "success": False,
            "error": "FX provider request failed",
            "details": str(rexc),
            "current_month_already_seeded": current_month_already_seeded,
        }), 502
    except Exception as e:
        db.session.rollback()
        logger.exception("Single-row upsert failed")
        return jsonify({
            "success": False,
            "error": "Internal error",
            "details": str(e),
            "current_month_already_seeded": current_month_already_seeded,
        }), 500
    

