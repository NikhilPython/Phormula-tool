from flask import Blueprint, request, jsonify , send_file 
from io import BytesIO
import jwt
import logging
import os
import re
import threading
from functools import wraps
from sqlalchemy import create_engine, text
from sqlalchemy.engine.url import make_url
from config import Config
from config import basedir
SECRET_KEY = Config.SECRET_KEY
from dotenv import load_dotenv
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from werkzeug.utils import secure_filename
from sqlalchemy import MetaData, Table, inspect, select
from app.routes.amazon_sales_api_routes import _normalize_sku_row
from app.utils.token_utils import create_database_if_not_exists, get_effective_user_id_from_token
from sqlalchemy import text
import pandas as pd
from decimal import Decimal, ROUND_DOWN, ROUND_HALF_UP
from datetime import date, datetime, timezone
from app.utils.fba_fee_calculator import estimate_fba_fee, number as fba_number, resolve_fba_measurements
from app.utils.dashboard_card_metrics import (
    add_per_unit_fields,
    build_pnl_card_metrics,
    calculate_pnl_cost_of_advertisement,
    calculate_pnl_tacos,
    calculate_pnl_total_advertising_cost,
    persist_per_unit_fields,
)
load_dotenv()
db_url = os.getenv('DATABASE_URL')
db_url1 = os.getenv('DATABASE_ADMIN_URL')
db_url2 = os.getenv('DATABASE_EXPENSE_RECONCILIATION_URL')
EXPENSE_RECONCILIATION_DB_NAME = (
    make_url(db_url2).database if db_url2 else "expense_reconciliation_db"
)

user_engine = create_engine(
    db_url,
    pool_pre_ping=True,
    pool_size=3,
    max_overflow=2,
    pool_recycle=1800,
)

expense_reconciliation_engine = create_engine(
    db_url2,
    pool_pre_ping=True,
    pool_size=3,
    max_overflow=2,
    pool_recycle=1800,
)

admin_engine = create_engine(
    db_url1,
    pool_pre_ping=True,
    pool_size=3,
    max_overflow=2,
    pool_recycle=1800,
)


product_bp = Blueprint('product_bp', __name__)

MONTHS = [
    "january", "february", "march", "april", "may", "june",
    "july", "august", "september", "october", "november", "december"
]
EXPENSE_RECONCILIATION_CACHE_VERSION = "expense_reconciliation_v30_uk_historical_fba_surcharge"
US_MINIMUM_REFERRAL_FEE_PER_UNIT = Decimal("0.30")
UK_FBA_SURCHARGE_START = date(2026, 4, 17)
UK_FBA_SURCHARGE_INCREASE = date(2026, 11, 2)
_expense_reconciliation_locks = {}
_expense_reconciliation_locks_guard = threading.Lock()


def _restore_reconciliation_product_names(frame, user_id, country, monthly_rows=None):
    """Resolve missing/zero product names by SKU without changing financial values."""
    from app.models.user_models import Product, Fee

    frame = frame.copy()
    if "product_name" not in frame.columns:
        frame["product_name"] = ""
    if "sku" not in frame.columns or frame.empty:
        return frame

    invalid = {"", "0", "0.0", "none", "null", "nan", "<na>"}

    def valid(value):
        return pd.notna(value) and str(value).strip().lower() not in invalid

    names = {}
    # Prefer a name already present in the current report/period.
    for item in frame[["sku", "product_name"]].itertuples(index=False, name=None):
        sku, name = item
        if valid(sku) and valid(name):
            names[str(sku).strip()] = str(name).strip()
    for item in monthly_rows or []:
        sku, name = item.get("sku"), item.get("product_name")
        if valid(sku) and valid(name):
            names.setdefault(str(sku).strip(), str(name).strip())

    marketplace = {"us": "ATVPDKIKX0DER", "uk": "A1F83G8C2ARO7P"}.get(
        str(country or "").strip().lower()
    )
    unresolved = sorted({str(v).strip() for v in frame["sku"] if valid(v)} - names.keys())
    if marketplace and unresolved:
        # Fee Preview's product_name is imported directly from Amazon; it is
        # preferred over guessing a name from the SKU.
        try:
            for start in range(0, len(unresolved), 400):
                batch = unresolved[start:start + 400]
                for fee in Fee.query.filter(
                    Fee.user_id == user_id,
                    Fee.marketplace_id == marketplace,
                    Fee.sku.in_(batch),
                ).all():
                    name = getattr(fee, "product_name", None)
                    if valid(name):
                        names.setdefault(str(fee.sku).strip(), str(name).strip())
        except SQLAlchemyError:
            pass
        unresolved = [sku for sku in unresolved if sku not in names]
        if unresolved:
            try:
                for start in range(0, len(unresolved), 400):
                    for product in Product.query.filter(
                        Product.user_id == user_id,
                        Product.marketplace_id == marketplace,
                        Product.sku.in_(unresolved[start:start + 400]),
                    ).all():
                        name = next((value for value in (
                            getattr(product, "product_name", None),
                            getattr(product, "item_name", None),
                            getattr(product, "title", None),
                        ) if valid(value)), None)
                        if name is not None:
                            names.setdefault(str(product.sku).strip(), str(name).strip())
            except SQLAlchemyError:
                pass

    missing = ~frame["product_name"].map(valid)
    frame.loc[missing, "product_name"] = (
        frame.loc[missing, "sku"].astype(str).str.strip().map(names).fillna("")
    )
    # Unresolved titles stay empty; never write numeric zero as a product name.
    return frame


def _recalculate_fba_source_fees(frame, user_id, country):
    """Estimate each source shipment before grouping, preserving price and date bands."""
    from app.models.user_models import Product, Fee

    frame = frame.copy()
    marketplace = {"us": "ATVPDKIKX0DER", "uk": "A1F83G8C2ARO7P"}.get(
        str(country or "").strip().lower(), ""
    )
    products = Product.query.filter(
        Product.user_id == user_id,
        Product.marketplace_id == marketplace,
        Product.sku.in_(frame["sku"].dropna().unique().tolist()),
    ).all() if not frame.empty and marketplace else []
    fields = (
        "user_id", "sku", "asin", "marketplace_id", "product_type", "fulfillment_channel",
        "package_dimensions", "package_weight_value", "package_weight_unit", "attributes",
    )
    catalog = {p.sku: {field: getattr(p, field) for field in fields} for p in products}
    uk_fee_per_unit = {}
    if marketplace == "A1F83G8C2ARO7P" and not frame.empty:
        fee_rows = Fee.query.filter(
            Fee.user_id == user_id,
            Fee.marketplace_id == marketplace,
            Fee.sku.in_(frame["sku"].dropna().astype(str).str.strip().unique().tolist()),
        ).all()
        uk_fee_per_unit = {
            str(fee.sku).strip(): (
                fba_number(fee.expected_domestic_fulfilment_fee_per_unit),
                getattr(fee, "synced_at", None),
            )
            for fee in fee_rows
            if fba_number(fee.expected_domestic_fulfilment_fee_per_unit) is not None
        }
    if catalog and str(country or "").strip().lower() == "us":
        preview_fields = ("user_id", "sku", "asin", "marketplace_id", "longest_side", "median_side",
                          "shortest_side", "unit_of_dimension", "item_package_weight", "unit_of_weight")
        previews = Fee.query.filter(
            Fee.user_id == user_id, Fee.marketplace_id == marketplace,
            Fee.sku.in_(list(catalog)),
        ).all()
        for preview in previews:
            catalog[preview.sku] = resolve_fba_measurements(
                catalog[preview.sku], {field: getattr(preview, field) for field in preview_fields},
            )
    amounts, reasons = [], []
    for row in frame.to_dict("records"):
        quantity = fba_number(row.get("quantity"))
        sales = fba_number(row.get("product_sales"))
        price = sales / quantity if sales is not None and quantity and quantity > 0 else None
        raw_date = next((row.get(key) for key in ("shipment_date", "posted_date", "date_time", "date")
                         if pd.notna(row.get(key)) and row.get(key)), None)
        timestamp = pd.to_datetime(raw_date, errors="coerce")
        shipped_on = timestamp.date() if pd.notna(timestamp) else None
        channel = next((row.get(key) for key in ("fulfillment", "fulfilment", "fulfillment_channel")
                        if pd.notna(row.get(key)) and str(row.get(key)).strip().lower()
                        not in {"", "none", "null", "nan", "0", "0.0"}), None)
        if marketplace == "A1F83G8C2ARO7P":
            transaction_type = str(row.get("type") or "order").strip().lower()
            normalized_channel = str(
                channel or catalog.get(str(row.get("sku", "")).strip(), {}).get("fulfillment_channel") or ""
            ).strip().upper()
            if transaction_type not in {"order", "shipment", "shipped", "sale"}:
                amount, reason = 0.0, "not_fulfillment_sale"
            elif quantity is None or quantity <= 0:
                amount, reason = (0.0, "no_shipped_units") if quantity == 0 else (None, "invalid_quantity")
            elif normalized_channel in {"MERCHANT", "MFN", "FBM", "DEFAULT", "SELLER"}:
                amount, reason = 0.0, "merchant_fulfilled"
            else:
                fee_preview = uk_fee_per_unit.get(str(row.get("sku", "")).strip())
                if fee_preview is None:
                    amount, reason = None, "missing_uk_fee_preview"
                elif shipped_on is None:
                    amount, reason = None, "missing_shipment_date"
                elif fee_preview[1] is None:
                    amount, reason = None, "missing_uk_fee_preview_date"
                else:
                    fee_per_unit, preview_synced_at = fee_preview
                    preview_date = pd.to_datetime(preview_synced_at, errors="coerce")
                    if pd.isna(preview_date):
                        amount, reason = None, "missing_uk_fee_preview_date"
                    else:
                        preview_date = preview_date.date()
                        target_surcharge = (
                            Decimal("0.02") if shipped_on >= UK_FBA_SURCHARGE_INCREASE
                            else Decimal("0.015") if shipped_on >= UK_FBA_SURCHARGE_START
                            else Decimal("0")
                        )
                        preview_surcharge = (
                            Decimal("0.02") if preview_date >= UK_FBA_SURCHARGE_INCREASE
                            else Decimal("0.015") if preview_date >= UK_FBA_SURCHARGE_START
                            else Decimal("0")
                        )
                        base_fee = fee_per_unit / (Decimal("1") + preview_surcharge)
                        historical_fee = base_fee * (Decimal("1") + target_surcharge)
                        amount = float(
                            historical_fee.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
                            * quantity
                        )
                        reason = "uk_fee_preview_historical_surcharge_adjusted"
            amounts.append(amount)
            reasons.append(reason)
            continue
        amount, reason = estimate_fba_fee(
            catalog.get(str(row.get("sku", "")).strip()), quantity=quantity,
            unit_price=price, shipped_on=shipped_on, marketplace_id=marketplace,
            fulfillment_channel=channel, transaction_type=row.get("type", "order"),
        )
        amounts.append(amount)
        reasons.append(reason)
    frame["fbaanswer"] = pd.Series(amounts, index=frame.index, dtype="float64")
    frame["fba_unavailable_count"] = frame["fbaanswer"].isna().astype(int)
    frame["fba_estimate_status"] = reasons
    frame["fba_quantity"] = pd.to_numeric(frame.get("quantity", pd.Series(0, index=frame.index)), errors="coerce").fillna(0)
    frame.loc[frame["fba_estimate_status"].eq("not_fulfillment_sale"), "fba_quantity"] = 0
    return frame


def _fba_refund_records(frame):
    """Preserve return units and FBA credits independently of referral filtering."""
    records = []
    for row in frame.to_dict("records"):
        kind = str(row.get("type", "")).strip().lower()
        if kind != "refund":
            continue
        units = fba_number(row.get("return_quantity")) or fba_number(row.get("quantity")) or Decimal(0)
        charged = -(fba_number(row.get("fba_fees")) or Decimal(0))
        records.append({
            "record_type": "fba_adjustment", "source_order": len(records),
            "order_id": str(row.get("order_id", "") or ""),
            "sku": str(row.get("sku", "") or ""),
            "product_name": str(row.get("product_name", "") or ""),
            "units": -float(abs(units)), "fba_quantity": -float(abs(units)),
            "fba_fees_charged": float(charged), "fba_fees_applicable": None,
            "fba_estimate_status": "refund_credit_not_comparable", "status": "noreferallfee",
        })
    return records


def _nullable_fee(value):
    return None if pd.isna(value) else float(value)


def _sum_expected_fees(values):
    values = list(values)
    return None if any(value is None or pd.isna(value) for value in values) else float(sum(values))


def _calculate_us_referral_fee_applicable(
    product_sales,
    gift_wrap_credits,
    promotional_rebates,
    referral_fee_percent,
    quantity,
):
    sales = Decimal(str(product_sales or 0))
    gift_wrap = Decimal(str(gift_wrap_credits or 0))
    rebates = abs(Decimal(str(promotional_rebates or 0)))
    rate = Decimal(str(referral_fee_percent or 0))
    units = Decimal(str(quantity or 0))
    applicable_base = max(sales + gift_wrap - rebates, Decimal("0"))
    if applicable_base <= 0 or rate <= 0 or units <= 0:
        return 0.0

    percentage_fee = applicable_base * rate / Decimal("100")
    minimum_fee = US_MINIMUM_REFERRAL_FEE_PER_UNIT * units
    return float(
        max(percentage_fee, minimum_fee).quantize(
            Decimal("0.01"),
            rounding=ROUND_HALF_UP,
        )
    )


def _calculate_total_amount(total, quantity):
    total_value = Decimal(str(total or 0))
    units = Decimal(str(quantity or 0))
    if units <= 0:
        return 0.0

    return float(
        (total_value / units).quantize(
            Decimal("0.01"),
            rounding=ROUND_DOWN,
        )
    )


def _resolve_us_referral_fee_applicable(
    source_answer,
    source_status,
    product_sales,
    gift_wrap_credits,
    promotional_rebates,
    referral_fee_percent,
    quantity,
):
    # The caller supplies a freshly calculated fee/status, never saved diagnostics.
    if str(source_status or "").strip().lower() != "undercharged":
        return float(source_answer or 0)

    return _calculate_us_referral_fee_applicable(
        product_sales,
        gift_wrap_credits,
        promotional_rebates,
        referral_fee_percent,
        quantity,
    )

US_BEAUTY_REFERRAL_FEE_BANDS = [
    {
        "country": "United States",
        "category": "beauty",
        "price_from": -50.0,
        "price_to": 9.99,
        "referral_fee_percent_est": 8.0,
    },
    {
        "country": "United States",
        "category": "beauty",
        "price_from": 10.0,
        "price_to": 99.99,
        "referral_fee_percent_est": 15.0,
    },
]


def _serialize_expense_reconciliation_request(view):
    @wraps(view)
    def wrapped(file_name, *args, **kwargs):
        auth_header = request.headers.get("Authorization", "")
        try:
            token = auth_header.split(" ", 1)[1]
            _, effective_user_id, _ = get_effective_user_id_from_token(token)
        except (IndexError, jwt.PyJWTError, TypeError, ValueError):
            return view(file_name, *args, **kwargs)

        qtd = (request.args.get("qtd") or "").strip().lower() == "true"
        ytd = (request.args.get("ytd") or "").strip().lower() == "true"
        range_key = "quarterly" if qtd else "yearly" if ytd else "monthly"
        period_key = (
            request.args.get("quarter")
            if range_key == "quarterly"
            else request.args.get("month") if range_key == "monthly" else "year"
        )
        lock_key = (
            effective_user_id,
            _safe_identifier(request.args.get("country"), "country"),
            _safe_identifier(request.args.get("year"), "year"),
            range_key,
            _safe_identifier(period_key, "period"),
        )
        with _expense_reconciliation_locks_guard:
            report_lock = _expense_reconciliation_locks.setdefault(
                lock_key,
                threading.Lock(),
            )

        with report_lock:
            return view(file_name, *args, **kwargs)

    return wrapped


def _apply_us_referral_fee_price_bands(frame):
    if frame.empty or "product_group" not in frame.columns:
        return frame

    try:
        category_rates = pd.read_sql(
            text(
                """
                SELECT country, category, price_from, price_to,
                       referral_fee_percent_est
                FROM category
                WHERE lower(country) IN ('united states', 'us', 'usa')
                """
            ),
            admin_engine,
        )
    except SQLAlchemyError:
        category_rates = pd.DataFrame()

    default_rates = pd.DataFrame(US_BEAUTY_REFERRAL_FEE_BANDS)
    if category_rates.empty:
        category_rates = default_rates
    else:
        for _, default_rate in default_rates.iterrows():
            category_key = str(default_rate["category"]).strip().lower()
            has_band = (
                category_rates["category"].astype(str).str.strip().str.lower().eq(category_key)
                & pd.to_numeric(category_rates["price_from"], errors="coerce").eq(default_rate["price_from"])
                & pd.to_numeric(category_rates["price_to"], errors="coerce").eq(default_rate["price_to"])
            ).any()
            if not has_band:
                category_rates = pd.concat(
                    [category_rates, pd.DataFrame([default_rate])],
                    ignore_index=True,
                )

    category_rates["category"] = (
        category_rates["category"].fillna("").astype(str).str.strip().str.lower()
    )
    category_rates["price_from"] = pd.to_numeric(
        category_rates["price_from"],
        errors="coerce",
    ).fillna(float("-inf"))
    category_rates["price_to"] = pd.to_numeric(
        category_rates["price_to"],
        errors="coerce",
    ).fillna(float("inf"))
    category_rates["referral_fee_percent_est"] = pd.to_numeric(
        category_rates["referral_fee_percent_est"],
        errors="coerce",
    )

    # Resolve each category/band once, rather than filtering and sorting a
    # DataFrame for every transaction in a historical month.
    categories = frame["product_group"].fillna("").astype(str).str.strip().str.lower()
    unit_values = pd.to_numeric(
        frame.get("total_value", pd.Series(0, index=frame.index)), errors="coerce"
    ).fillna(0)
    rates = pd.to_numeric(
        frame.get("referral_fee", pd.Series(0, index=frame.index)), errors="coerce"
    ).fillna(0).astype(float)
    category_rates["_band_width"] = category_rates["price_to"] - category_rates["price_from"]
    for category_key in categories.unique():
        if not category_key:
            continue
        category_first = re.split(r"[&/\-]", category_key, maxsplit=1)[0].strip()
        category_matches = category_rates[category_rates["category"].eq(category_key)]
        if category_matches.empty and category_first:
            category_matches = category_rates[category_rates["category"].eq(category_first)]
        category_matches = category_matches.sort_values(
            ["_band_width", "price_from"],
            ascending=[True, False],
            kind="stable",
        )
        unmatched = categories.eq(category_key)
        for band in category_matches.itertuples(index=False):
            if pd.isna(band.referral_fee_percent_est):
                continue
            matched = unmatched & unit_values.between(band.price_from, band.price_to)
            rates.loc[matched] = float(band.referral_fee_percent_est)
            unmatched = unmatched & ~matched

    frame["referral_fee"] = rates
    return frame


def _recalculate_us_referral_source_fees(frame):
    """Rebuild current US import diagnostics for every period before grouping.

    Historical tables contain answers produced by older import formulas. Their
    presence (or saved status) must not select the formula used by reconciliation.
    Keep the current import's per-unit rounding and shipping-inclusive base;
    the existing undercharge adjustment is applied after order/SKU grouping.
    """
    frame = frame.copy()
    for column in (
        "product_sales", "shipping_credits", "gift_wrap_credits",
        "promotional_rebates", "quantity", "referral_fee", "selling_fees",
    ):
        if column not in frame.columns:
            frame[column] = 0.0
        frame[column] = pd.to_numeric(frame[column], errors="coerce").fillna(0)

    units = frame["quantity"].where(frame["quantity"] > 0, 1)
    base = (
        frame["product_sales"] + frame["shipping_credits"]
        + frame["gift_wrap_credits"] + frame["promotional_rebates"]
    )
    # Match plotting_utils' current US import calculation, including rounding.
    frame["total_value"] = [
        float(Decimal(value / quantity).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))
        for value, quantity in zip(base, units)
    ]
    frame = _apply_us_referral_fee_price_bands(frame)
    frame["answer"] = [
        float(
            max(
                (Decimal(str(value)) * Decimal(str(rate)) / Decimal("100")).quantize(
                    Decimal("0.01"), rounding=ROUND_HALF_UP
                ),
                US_MINIMUM_REFERRAL_FEE_PER_UNIT,
            ) * Decimal(str(quantity))
        ) if sales != 0 and rate > 0 else 0.0
        for value, rate, quantity, sales in zip(
            frame["total_value"], frame["referral_fee"], units, frame["product_sales"]
        )
    ]
    descriptions = frame.get("description", pd.Series("", index=frame.index))
    tax_rows = descriptions.fillna("").astype(str).str.strip().str.lower().eq("tax")
    frame.loc[tax_rows, "answer"] = 0.0
    return frame


def _safe_identifier(value, fallback="value"):
    cleaned = re.sub(r"[^a-zA-Z0-9_]+", "_", str(value or "").strip().lower())
    cleaned = re.sub(r"_+", "_", cleaned).strip("_")
    return cleaned or fallback


def _expense_reconciliation_table_name(user_id, country, month, year, range_, quarter=None):
    user_key = _safe_identifier(user_id, "user")
    country_key = _safe_identifier(country, "country")
    year_key = _safe_identifier(year, "year")

    if range_ == "quarterly":
        period_key = _safe_identifier(quarter or month, "quarter")
        quarter_match = re.search(r"[1-4]", period_key)
        quarter_key = f"q{quarter_match.group(0)}" if quarter_match else period_key
        return f"quarterly_expense_reconciliation_{user_key}_{country_key}_{quarter_key}{year_key}"

    if range_ == "yearly":
        return f"skuwiseyearly_expense_reconciliation_{user_key}_{country_key}_{year_key}"

    month_key = _safe_identifier(month, "month")
    return f"skuwisemonthly_expense_reconciliation_{user_key}_{country_key}_{month_key}{year_key}"


def _expense_reconciliation_status_table_name(user_id, country, month, year, range_, quarter=None):
    user_key = _safe_identifier(user_id, "user")
    country_key = _safe_identifier(country, "country")
    year_key = _safe_identifier(year, "year")

    if range_ == "quarterly":
        period_key = _safe_identifier(quarter or month, "quarter")
        quarter_match = re.search(r"[1-4]", period_key)
        quarter_key = f"q{quarter_match.group(0)}" if quarter_match else period_key
        return f"quarterly_expense_status_{user_key}_{country_key}_{quarter_key}{year_key}"

    if range_ == "yearly":
        return f"skuwiseyearly_expense_status_{user_key}_{country_key}_{year_key}"

    month_key = _safe_identifier(month, "month")
    return f"skuwisemonthly_expense_status_{user_key}_{country_key}_{month_key}{year_key}"


def _materialize_expense_reconciliation_table(
    *,
    user_id,
    country,
    month,
    year,
    range_,
    quarter,
    final_df,
    sku_monthly_rows=None,
    sku_monthly_summary=None,
    status_source_df=None,
    fba_adjustments_df=None,
    platform_fee_total=0,
    other_fee_total=0,
):
    if expense_reconciliation_engine is None:
        return {
            "success": False,
            "message": "Expense reconciliation database URL is not configured.",
        }

    table_name = _expense_reconciliation_table_name(
        user_id=user_id,
        country=country,
        month=month,
        year=year,
        range_=range_,
        quarter=quarter,
    )
    status_table_name = _expense_reconciliation_status_table_name(
        user_id=user_id,
        country=country,
        month=month,
        year=year,
        range_=range_,
        quarter=quarter,
    )

    source = final_df.copy()
    status_columns = [
        "sku",
        "product_name",
        "status",
        "units",
        "net_sales",
        "referral_fees_applicable",
        "referral_fees_charged",
        "fba_fees_applicable",
        "fba_fees_charged",
        "difference",
    ]
    status_rows = pd.DataFrame(columns=status_columns)
    status_detail_rows = pd.DataFrame(columns=[
        "record_type",
        "source_order",
        "order_id",
        "sku",
        "product_name",
        "status",
        "errorstatus",
        "units",
        "gross_sales",
        "net_sales",
        "total_amount",
        "product_sales",
        "shipping_credits",
        "promotional_rebates",
        "gift_wrap_credits",
        "referral_fee_per",
        "referral_fees_applicable",
        "referral_fees_charged",
        "fba_fees_applicable",
        "fba_fees_charged",
        "difference",
    ])
    if source.empty:
        rows = pd.DataFrame(columns=[
            "sku",
            "product_name",
            "units",
            "net_sales",
            "referral_fees_applicable",
            "referral_fees_charged",
            "fba_fees_applicable",
            "fba_fees_charged",
            "platform_fees_applicable",
            "platform_fees_charged",
            "other_fees_applicable",
            "other_fees_charged",
            "referral_fees_accurate",
            "referral_fees_undercharged",
            "referral_fees_overcharged",
        ])
    else:
        sku_text = source.get("sku", pd.Series("", index=source.index)).astype(str).str.strip()
        detail_df = source[
            (sku_text != "Grand Total") &
            (~sku_text.str.startswith("Charge -", na=False))
        ].copy()

        numeric_defaults = {
            "total_quantity": 0,
            "quantity": 0,
            "net_sales_total_value": 0,
            "net_sales": 0,
            "product_sales": 0,
            "answer": 0,
            "selling_fees": 0,
            "fba_fees": 0,
            "difference": 0,
        }
        for col, default in numeric_defaults.items():
            if col not in detail_df.columns:
                detail_df[col] = default
            detail_df[col] = pd.to_numeric(detail_df[col], errors="coerce").fillna(0)

        status_norm = detail_df.get("status", pd.Series("", index=detail_df.index)).astype(str).str.strip().str.lower()
        detail_df["_units"] = detail_df["total_quantity"].where(
            detail_df["total_quantity"] != 0,
            detail_df["quantity"],
        )
        detail_df["_net_sales"] = detail_df["net_sales_total_value"].where(
            detail_df["net_sales_total_value"] != 0,
            detail_df["net_sales"].where(detail_df["net_sales"] != 0, detail_df["product_sales"]),
        )
        detail_df["_referral_fees_applicable"] = detail_df["answer"]
        detail_df["_referral_fees_charged"] = detail_df["selling_fees"]
        detail_df["_fba_fees_charged"] = -detail_df["fba_fees"]
        detail_df["_fba_fees_applicable"] = pd.to_numeric(
            detail_df.get("fbaanswer", pd.Series(float("nan"), index=detail_df.index)),
            errors="coerce",
        )
        detail_df["_referral_fees_accurate"] = detail_df["answer"].where(status_norm == "accurate", 0)
        detail_df["_referral_fees_undercharged"] = detail_df["difference"].where(
            status_norm == "undercharged",
            0,
        ).abs()
        detail_df["_referral_fees_overcharged"] = detail_df["difference"].where(
            status_norm == "overcharged",
            0,
        ).clip(lower=0)

        status_rows = pd.DataFrame({
            "sku": detail_df.get("sku", ""),
            "product_name": detail_df.get("product_name", ""),
            "status": detail_df.get("status", "noreferallfee"),
            "units": detail_df["_units"],
            "net_sales": detail_df["_net_sales"],
            "referral_fees_applicable": detail_df["answer"],
            "referral_fees_charged": detail_df["selling_fees"],
            "fba_fees_applicable": detail_df["_fba_fees_applicable"],
            "fba_fees_charged": detail_df["_fba_fees_charged"],
            "difference": detail_df["difference"],
        })
        status_rows["status"] = status_rows["status"].fillna("noreferallfee").astype(str)
        status_rows = status_rows.groupby(
            ["sku", "product_name", "status"],
            as_index=False,
        )[
            [
                "units",
                "net_sales",
                "referral_fees_applicable",
                "referral_fees_charged",
                "fba_fees_applicable",
                "fba_fees_charged",
                "difference",
            ]
        ].sum()

        if status_source_df is not None and not status_source_df.empty:
            raw_status = status_source_df.copy().reset_index(drop=True)
            raw_transaction_types = raw_status.get(
                "type",
                pd.Series("", index=raw_status.index),
            ).fillna("").astype(str).str.strip().str.lower()
            raw_descriptions = raw_status.get(
                "description",
                pd.Series("", index=raw_status.index),
            ).fillna("").astype(str).str.strip().str.lower()
            raw_refund_rows = raw_transaction_types.eq("refund") | raw_descriptions.eq("refund")
            raw_status = raw_status.loc[~raw_refund_rows].copy().reset_index(drop=True)

            def _raw_numeric(*columns):
                result = pd.Series(0.0, index=raw_status.index)
                for column in columns:
                    if column not in raw_status.columns:
                        continue
                    candidate = pd.to_numeric(raw_status[column], errors="coerce").fillna(0)
                    result = result.where(result != 0, candidate)
                return result

            raw_errorstatus = raw_status.get(
                "errorstatus",
                pd.Series("", index=raw_status.index),
            ).fillna("").astype(str).str.strip().str.lower()
            if "status" in raw_status.columns:
                raw_status_name = raw_status["status"].fillna("").astype(str)
            else:
                raw_status_name = raw_errorstatus.map({
                    "ok": "Accurate",
                    "undercharged": "Undercharged",
                    "overcharged": "Overcharged",
                }).fillna("noreferallfee")

            raw_units = _raw_numeric("total_quantity", "quantity")
            raw_net_sales = _raw_numeric(
                "net_sales_total_value",
                "net_sales",
                "product_sales",
            )
            raw_total_amount = pd.Series(
                [
                    _calculate_total_amount(total, units)
                    for total, units in zip(raw_net_sales, raw_units)
                ],
                index=raw_status.index,
                dtype="float64",
            )

            status_detail_rows = pd.DataFrame({
                "record_type": "detail",
                "source_order": range(len(raw_status)),
                "order_id": raw_status.get("order_id", pd.Series("", index=raw_status.index)),
                "sku": raw_status.get("sku", pd.Series("", index=raw_status.index)),
                "product_name": raw_status.get(
                    "product_name",
                    pd.Series("", index=raw_status.index),
                ),
                "status": raw_status_name,
                "errorstatus": raw_errorstatus,
                "units": raw_units,
                "gross_sales": _raw_numeric("gross_sales", "product_sales"),
                "net_sales": raw_net_sales,
                "total_amount": raw_total_amount,
                "product_sales": _raw_numeric("product_sales"),
                "shipping_credits": _raw_numeric("shipping_credits"),
                "promotional_rebates": _raw_numeric("promotional_rebates"),
                "gift_wrap_credits": _raw_numeric("gift_wrap_credits"),
                "referral_fee_per": _raw_numeric("referral_fee"),
                "referral_fees_applicable": _raw_numeric("answer"),
                "referral_fees_charged": _raw_numeric("selling_fees"),
                "fba_fees_applicable": _raw_numeric("fbaanswer"),
                "fba_fees_charged": -_raw_numeric("fba_fees"),
                "difference": _raw_numeric("difference"),
            })

        group_cols = ["sku", "product_name"]
        value_cols = [
            "_units",
            "_net_sales",
            "_referral_fees_applicable",
            "_referral_fees_charged",
            "_fba_fees_applicable",
            "_fba_fees_charged",
            "_referral_fees_accurate",
            "_referral_fees_undercharged",
            "_referral_fees_overcharged",
        ]

        if detail_df.empty:
            rows = pd.DataFrame(columns=[
                "sku",
                "product_name",
                "units",
                "net_sales",
                "referral_fees_applicable",
                "referral_fees_charged",
                "fba_fees_applicable",
                "fba_fees_charged",
                "referral_fees_accurate",
                "referral_fees_undercharged",
                "referral_fees_overcharged",
            ])
        else:
            rows = (
                detail_df.groupby(group_cols, as_index=False)[value_cols]
                .sum()
                .rename(columns={
                    "_units": "units",
                    "_net_sales": "net_sales",
                    "_referral_fees_applicable": "referral_fees_applicable",
                    "_referral_fees_charged": "referral_fees_charged",
                    "_fba_fees_applicable": "fba_fees_applicable",
                    "_fba_fees_charged": "fba_fees_charged",
                    "_referral_fees_accurate": "referral_fees_accurate",
                    "_referral_fees_undercharged": "referral_fees_undercharged",
                    "_referral_fees_overcharged": "referral_fees_overcharged",
                })
            )

        if sku_monthly_rows and not rows.empty:
            monthly_df = pd.DataFrame(sku_monthly_rows).copy()
            if "sku" in monthly_df.columns:
                monthly_df["_sku_key"] = (
                    monthly_df["sku"].fillna("").astype(str).str.strip().str.lower()
                )

                def _monthly_numeric(*columns):
                    result = pd.Series(0.0, index=monthly_df.index)
                    for column in columns:
                        if column not in monthly_df.columns:
                            continue
                        candidate = pd.to_numeric(monthly_df[column], errors="coerce").fillna(0)
                        result = result.where(result != 0, candidate)
                    return result

                monthly_df["_expense_units"] = _monthly_numeric("total_quantity", "quantity", "units")
                if "return_quantity" in monthly_df.columns and "total_quantity" not in monthly_df.columns:
                    returns = pd.to_numeric(monthly_df["return_quantity"], errors="coerce").fillna(0)
                    monthly_df["_expense_units"] = (monthly_df["_expense_units"] - returns).clip(lower=0)
                monthly_df["_expense_net_sales"] = _monthly_numeric(
                    "net_sales",
                    "net_sales_total_value",
                    "product_sales",
                )
                monthly_df["_expense_referral_fees_charged"] = _monthly_numeric(
                    "selling_fees",
                    "referral_fees_charged",
                ).abs()

                monthly_totals = monthly_df.groupby("_sku_key", as_index=True)[
                    [
                        "_expense_units",
                        "_expense_net_sales",
                        "_expense_referral_fees_charged",
                    ]
                ].sum()
                row_sku_keys = rows["sku"].fillna("").astype(str).str.strip().str.lower()
                mapped_units = row_sku_keys.map(monthly_totals["_expense_units"])
                mapped_net_sales = row_sku_keys.map(monthly_totals["_expense_net_sales"])
                mapped_charged_fees = row_sku_keys.map(
                    monthly_totals["_expense_referral_fees_charged"]
                )
                rows.loc[mapped_units.notna(), "units"] = mapped_units[mapped_units.notna()]
                rows.loc[mapped_net_sales.notna(), "net_sales"] = mapped_net_sales[mapped_net_sales.notna()]
                rows.loc[
                    mapped_charged_fees.notna(),
                    "referral_fees_charged",
                ] = mapped_charged_fees[mapped_charged_fees.notna()]

        if not status_rows.empty and not rows.empty:
            status_names = (
                status_rows["status"].fillna("").astype(str).str.strip().str.lower()
            )
            status_rows["referral_fees_applicable"] = pd.to_numeric(
                status_rows["referral_fees_applicable"],
                errors="coerce",
            ).fillna(0)
            status_rows["referral_fees_charged"] = pd.to_numeric(
                status_rows["referral_fees_charged"],
                errors="coerce",
            ).fillna(0).abs()
            accurate_status = status_names.eq("accurate")
            status_rows.loc[
                accurate_status,
                "referral_fees_charged",
            ] = status_rows.loc[
                accurate_status,
                "referral_fees_applicable",
            ]
            status_rows["difference"] = (
                status_rows["referral_fees_charged"]
                - status_rows["referral_fees_applicable"]
            ).round(2)

            status_rows["_sku_key"] = (
                status_rows["sku"].fillna("").astype(str).str.strip().str.lower()
            )
            main_targets = rows.copy()
            main_targets["_sku_key"] = (
                main_targets["sku"].fillna("").astype(str).str.strip().str.lower()
            )
            main_targets = main_targets.groupby("_sku_key", as_index=True)[
                ["units", "net_sales"]
            ].sum()

            def _scale_status_values(indexes, column, target, integer=False):
                values = pd.to_numeric(status_rows.loc[indexes, column], errors="coerce").fillna(0)
                source_total = float(values.sum())
                if source_total:
                    scaled = values * (float(target) / source_total)
                else:
                    scaled = pd.Series(0.0, index=indexes)
                    if len(indexes):
                        scaled.loc[indexes[0]] = float(target)

                if integer:
                    scaled = scaled.round().astype("int64")
                    adjustment = int(round(float(target))) - int(scaled.sum())
                else:
                    scaled = scaled.round(2)
                    adjustment = round(float(target) - float(scaled.sum()), 2)

                if adjustment and len(indexes):
                    adjustment_index = values.abs().idxmax()
                    scaled.loc[adjustment_index] += adjustment
                status_rows.loc[indexes, column] = scaled

            for sku_key, indexes in status_rows.groupby("_sku_key").groups.items():
                if sku_key not in main_targets.index:
                    continue
                _scale_status_values(
                    list(indexes),
                    "units",
                    main_targets.at[sku_key, "units"],
                    integer=True,
                )
                _scale_status_values(
                    list(indexes),
                    "net_sales",
                    main_targets.at[sku_key, "net_sales"],
                )

            corrected_charged_by_sku = status_rows.groupby(
                "_sku_key",
                as_index=True,
            )["referral_fees_charged"].sum()
            row_sku_keys = rows["sku"].fillna("").astype(str).str.strip().str.lower()
            mapped_corrected_charged = row_sku_keys.map(corrected_charged_by_sku)
            rows.loc[
                mapped_corrected_charged.notna(),
                "referral_fees_charged",
            ] = mapped_corrected_charged[mapped_corrected_charged.notna()]

            status_rows = status_rows.drop(columns=["_sku_key"])

        if not rows.empty:
            total = {
                "sku": "",
                "product_name": "Grand Total",
            }
            for col in [
                "units",
                "net_sales",
                "referral_fees_applicable",
                "referral_fees_charged",
                "fba_fees_applicable",
                "fba_fees_charged",
                "referral_fees_accurate",
                "referral_fees_undercharged",
                "referral_fees_overcharged",
            ]:
                total[col] = float(pd.to_numeric(rows[col], errors="coerce").fillna(0).sum())

            total["platform_fees_applicable"] = float(platform_fee_total or 0)
            total["platform_fees_charged"] = float(platform_fee_total or 0)
            total["other_fees_applicable"] = float(other_fee_total or 0)
            total["other_fees_charged"] = float(other_fee_total or 0)

            if sku_monthly_summary:
                summary_units = sku_monthly_summary.get(
                    "total_quantity",
                    sku_monthly_summary.get("quantity"),
                )
                summary_net_sales = sku_monthly_summary.get("net_sales")
                if summary_units is not None:
                    total["units"] = float(summary_units or 0)
                if summary_net_sales is not None:
                    total["net_sales"] = float(summary_net_sales or 0)

            rows = pd.concat([rows, pd.DataFrame([total])], ignore_index=True)

    rows["units"] = pd.to_numeric(rows["units"], errors="coerce").fillna(0).round().astype("int64")
    for column in [
        "net_sales",
        "referral_fees_applicable",
        "referral_fees_charged",
        "fba_fees_applicable",
        "fba_fees_charged",
        "platform_fees_applicable",
        "platform_fees_charged",
        "other_fees_applicable",
        "other_fees_charged",
        "referral_fees_accurate",
        "referral_fees_undercharged",
        "referral_fees_overcharged",
    ]:
        rows[column] = pd.to_numeric(rows[column], errors="coerce").fillna(0).round(2)

    if not status_rows.empty:
        status_key = status_rows["status"].astype(str).str.strip().str.lower()
        status_rows["status"] = status_key.map({
            "accurate": "Accurate",
            "undercharged": "Undercharged",
            "overcharged": "Overcharged",
            "noreferallfee": "noreferallfee",
            "noreferralfee": "noreferallfee",
        }).fillna("noreferallfee")
        status_rows["units"] = (
            pd.to_numeric(status_rows["units"], errors="coerce")
            .fillna(0)
            .round()
            .astype("int64")
        )
        for column in [
            "net_sales",
            "referral_fees_applicable",
            "referral_fees_charged",
            "fba_fees_applicable",
            "fba_fees_charged",
            "difference",
        ]:
            status_rows[column] = (
                pd.to_numeric(status_rows[column], errors="coerce").fillna(0).round(2)
            )
        status_rank = {
            "Accurate": 0,
            "Undercharged": 1,
            "Overcharged": 2,
            "noreferallfee": 3,
        }
        status_rows["_status_rank"] = status_rows["status"].map(status_rank).fillna(4)
        status_rows = status_rows.sort_values(
            ["_status_rank", "sku", "product_name"],
            kind="stable",
        ).drop(columns=["_status_rank"])
        status_rows["record_type"] = "summary"
        status_rows["source_order"] = -1
        status_rows["order_id"] = ""
        status_rows["errorstatus"] = status_rows["status"].map({
            "Accurate": "ok",
            "Undercharged": "undercharged",
            "Overcharged": "overcharged",
            "noreferallfee": "noreferallfee",
        })
        status_rows["gross_sales"] = status_rows["net_sales"]

    if not status_detail_rows.empty:
        detail_status_key = status_detail_rows["status"].astype(str).str.strip().str.lower()
        status_detail_rows["status"] = detail_status_key.map({
            "accurate": "Accurate",
            "undercharged": "Undercharged",
            "overcharged": "Overcharged",
            "noreferallfee": "noreferallfee",
            "noreferralfee": "noreferallfee",
        }).fillna("noreferallfee")
        status_detail_rows["units"] = (
            pd.to_numeric(status_detail_rows["units"], errors="coerce")
            .fillna(0)
            .round()
            .astype("int64")
        )
        for column in [
            "gross_sales",
            "net_sales",
            "total_amount",
            "product_sales",
            "shipping_credits",
            "promotional_rebates",
            "gift_wrap_credits",
            "referral_fee_per",
            "referral_fees_applicable",
            "referral_fees_charged",
            "fba_fees_applicable",
            "fba_fees_charged",
            "difference",
        ]:
            status_detail_rows[column] = (
                pd.to_numeric(status_detail_rows[column], errors="coerce").fillna(0).round(2)
            )

    cached_status_rows = pd.concat(
        [status_rows, status_detail_rows],
        ignore_index=True,
        sort=False,
    )

    # Missing estimates must survive legacy numeric filling and group sums.
    # A subtotal is unavailable if any contributing order lacks an estimate.
    if status_source_df is not None and not status_source_df.empty:
        # Match the raw-status filtering above, including for other marketplaces.
        fba_source = raw_status.copy()
        fba_source["_expected"] = pd.to_numeric(fba_source["fbaanswer"], errors="coerce")
        fba_source.loc[fba_source["fba_unavailable_count"].gt(0), "_expected"] = float("nan")
        by_product = fba_source.groupby(["sku", "product_name"], dropna=False)["_expected"].agg(
            lambda s: s.sum(skipna=False)
        )
        by_status = fba_source.groupby(["sku", "product_name", "status"], dropna=False)["_expected"].agg(
            lambda s: s.sum(skipna=False)
        )
        rows["fba_fees_applicable"] = [
            by_product.get((r.sku, r.product_name), float("nan")) for r in rows.itertuples()
        ]
        is_grand = rows["product_name"].eq("Grand Total")
        rows.loc[is_grand, "fba_fees_applicable"] = fba_source["_expected"].sum(skipna=False)
        detail_mask = cached_status_rows["record_type"].eq("detail")
        cached_status_rows.loc[detail_mask, "fba_fees_applicable"] = fba_source["_expected"].to_numpy()
        cached_status_rows.loc[detail_mask, "fba_estimate_status"] = fba_source["fba_estimate_status"].to_numpy()
        if "fba_quantity" in fba_source.columns:
            cached_status_rows.loc[detail_mask, "fba_quantity"] = fba_source["fba_quantity"].to_numpy()
        summary_mask = ~detail_mask
        cached_status_rows.loc[summary_mask, "fba_fees_applicable"] = [
            by_status.get((r.sku, r.product_name, r.status), float("nan"))
            for r in cached_status_rows.loc[summary_mask].itertuples()
        ]
    else:
        rows["fba_fees_applicable"] = float("nan")
        cached_status_rows["fba_fees_applicable"] = float("nan")

    if fba_adjustments_df is not None and not fba_adjustments_df.empty:
        cached_status_rows = pd.concat([
            cached_status_rows, pd.DataFrame(_fba_refund_records(fba_adjustments_df)),
        ], ignore_index=True)

    create_database_if_not_exists(db_url2)
    rows.to_sql(
        table_name,
        expense_reconciliation_engine,
        schema="public",
        if_exists="replace",
        index=False,
    )
    cached_status_rows.to_sql(
        status_table_name,
        expense_reconciliation_engine,
        schema="public",
        if_exists="replace",
        index=False,
    )
    with expense_reconciliation_engine.begin() as conn:
        for cached_table_name in (table_name, status_table_name):
            conn.execute(text(
                f'COMMENT ON TABLE public."{cached_table_name}" '
                f"IS '{EXPENSE_RECONCILIATION_CACHE_VERSION}'"
            ))

    return {
        "success": True,
        "database": EXPENSE_RECONCILIATION_DB_NAME,
        "table_name": table_name,
        "status_table_name": status_table_name,
        "row_count": int(len(rows)),
        "status_row_count": int(len(cached_status_rows)),
        "columns": rows.columns.tolist(),
    }


def _load_expense_reconciliation_table(*, user_id, country, month, year, range_, quarter):
    if expense_reconciliation_engine is None:
        return None

    table_name = _expense_reconciliation_table_name(
        user_id=user_id,
        country=country,
        month=month,
        year=year,
        range_=range_,
        quarter=quarter,
    )
    status_table_name = _expense_reconciliation_status_table_name(
        user_id=user_id,
        country=country,
        month=month,
        year=year,
        range_=range_,
        quarter=quarter,
    )

    try:
        with expense_reconciliation_engine.connect() as conn:
            table_inspector = inspect(conn)
            if not table_inspector.has_table(table_name, schema="public"):
                return None
            if not table_inspector.has_table(status_table_name, schema="public"):
                return None
            for cached_table_name in (table_name, status_table_name):
                table_comment = table_inspector.get_table_comment(
                    cached_table_name,
                    schema="public",
                ).get("text")
                if table_comment != EXPENSE_RECONCILIATION_CACHE_VERSION:
                    return None
            rows = pd.read_sql(text(f'SELECT * FROM "{table_name}"'), conn)
            status_rows = pd.read_sql(text(f'SELECT * FROM "{status_table_name}"'), conn)
    except SQLAlchemyError:
        return None

    return table_name, rows, status_table_name, status_rows


def _load_global_expense_reconciliation_from_countries(*, user_id, month, year, range_):
    """Build a Global reconciliation response from the UK and US cached reports."""
    if range_ != "monthly":
        return None

    uk = _load_expense_reconciliation_table(
        user_id=user_id, country="uk", month=month, year=year,
        range_=range_, quarter=None,
    )
    us = _load_expense_reconciliation_table(
        user_id=user_id, country="us", month=month, year=year,
        range_=range_, quarter=None,
    )
    if uk is None or us is None:
        return None

    try:
        month_name = MONTHS[int(month) - 1] if str(month).isdigit() else str(month).strip().lower()
        target_currency = (request.args.get("homeCurrency") or "USD").strip().lower()
        with admin_engine.connect() as conn:
            rate_rows = conn.execute(text("""
                SELECT lower(user_currency) AS currency, conversion_rate
                  FROM currency_conversion
                 WHERE lower(selected_currency)='usd'
                   AND lower(month)=:month AND year=:year
                 ORDER BY id DESC
            """), {"month": month_name, "year": int(year)}).mappings().all()
        rates = {}
        for rate_row in rate_rows:
            rates.setdefault(str(rate_row["currency"]).lower(), float(rate_row["conversion_rate"]))
        rates["usd"] = 1.0
        target_per_usd = rates.get(target_currency)
        gbp_per_usd = rates.get("gbp")
        if target_per_usd is None or gbp_per_usd is None or gbp_per_usd == 0:
            return None
        multipliers = {"us": target_per_usd, "uk": target_per_usd / gbp_per_usd}

        row_money_columns = (
            "net_sales", "referral_fees_applicable", "referral_fees_charged",
            "fba_fees_applicable", "fba_fees_charged", "platform_fees_applicable",
            "platform_fees_charged", "other_fees_applicable", "other_fees_charged",
            "referral_fees_accurate", "referral_fees_undercharged",
            "referral_fees_overcharged",
        )
        status_money_columns = (
            "gross_sales", "net_sales", "total_amount", "product_sales",
            "shipping_credits", "promotional_rebates", "gift_wrap_credits",
            "referral_fees_applicable", "referral_fees_charged",
            "fba_fees_applicable", "fba_fees_charged", "difference",
        )

        converted_rows = []
        converted_status_rows = []
        for country, cached in (("uk", uk), ("us", us)):
            _, rows, _, status_rows = cached
            factor = multipliers[country]
            country_rows = rows.copy()
            country_status = status_rows.copy()
            for frame, columns in (
                (country_rows, row_money_columns),
                (country_status, status_money_columns),
            ):
                for column in columns:
                    if column in frame.columns:
                        values = pd.to_numeric(frame[column], errors="coerce")
                        frame[column] = values * factor
            if country == "us" and "source_order" in country_status.columns:
                max_source_order = max(
                    (int(pd.to_numeric(previous["source_order"], errors="coerce").max())
                     for previous in converted_status_rows
                     if "source_order" in previous.columns and not previous.empty),
                    default=-1,
                )
                country_status["source_order"] = (
                    pd.to_numeric(country_status["source_order"], errors="coerce").fillna(0)
                    + max_source_order + 1
                )
            converted_rows.append(country_rows)
            converted_status_rows.append(country_status)

        combined_rows = pd.concat(converted_rows, ignore_index=True, sort=False)
        grouping = [column for column in ("sku", "product_name") if column in combined_rows.columns]
        if grouping:
            aggregations = {}
            row_sum_columns = set(row_money_columns) | {"units"}
            for column in combined_rows.columns:
                if column in grouping:
                    continue
                if column == "fba_fees_applicable":
                    aggregations[column] = lambda values: (
                        values.sum() if values.notna().all() else None
                    )
                elif column in row_sum_columns:
                    aggregations[column] = "sum"
                else:
                    aggregations[column] = "first"
            combined_rows = combined_rows.groupby(grouping, as_index=False, dropna=False).agg(aggregations)
        combined_status = pd.concat(converted_status_rows, ignore_index=True, sort=False)
        return _expense_reconciliation_api_response(
            table_name=_expense_reconciliation_table_name(
                user_id=user_id, country="global", month=month, year=year,
                range_=range_, quarter=None,
            ),
            rows=combined_rows,
            status_table_name=_expense_reconciliation_status_table_name(
                user_id=user_id, country="global", month=month, year=year,
                range_=range_, quarter=None,
            ),
            status_rows=combined_status,
            range_=range_,
        )
    except (SQLAlchemyError, TypeError, ValueError, KeyError):
        logging.exception("Failed to build Global expense reconciliation from UK/US reports")
        return None


def _fee_percentage_metrics(
    *,
    net_sales,
    referral_charged,
    referral_applicable,
    fba_charged,
    fba_applicable,
    platform_charged,
    platform_applicable,
    other_charged,
    other_applicable,
):
    def _number(value):
        try:
            number = float(value or 0)
        except (TypeError, ValueError):
            return 0.0
        return 0.0 if pd.isna(number) else number

    sales = _number(net_sales)

    def _net_sales_percentage(value):
        if sales <= 0:
            return 0.0
        return round((_number(value) / sales) * 100, 2)

    def _charged_vs_applicable(charged, applicable):
        applicable_value = _number(applicable)
        if applicable_value == 0:
            return 0.0
        return round(
            ((_number(charged) - applicable_value) / applicable_value) * 100,
            2,
        )

    def _metric(charged, applicable):
        return {
            "charged_net_sales_pct": _net_sales_percentage(charged),
            "applicable_net_sales_pct": None if applicable is None or pd.isna(applicable) else _net_sales_percentage(applicable),
            "charged_vs_applicable_pct": None if applicable is None or pd.isna(applicable) else _charged_vs_applicable(
                charged,
                applicable,
            ),
        }

    return {
        "referral_fees": _metric(referral_charged, referral_applicable),
        "fba_fees": _metric(fba_charged, fba_applicable),
        "platform_fees": _metric(platform_charged, platform_applicable),
        "other_fees": _metric(other_charged, other_applicable),
    }


def _referral_fee_insight_metrics(
    *,
    overcharged_amount,
    overcharged_units,
    accurate_units,
    total_units,
    applicable_fees,
    net_variance,
):
    def _number(value):
        try:
            number = float(value or 0)
        except (TypeError, ValueError):
            return 0.0
        return 0.0 if pd.isna(number) else number

    overcharge = max(0.0, _number(overcharged_amount))
    affected_units = max(0.0, _number(overcharged_units))
    accurate = max(0.0, _number(accurate_units))
    units = max(0.0, _number(total_units))
    applicable = max(0.0, _number(applicable_fees))

    if applicable > 0:
        overcharge_rate = round((overcharge / applicable) * 100, 2)
    elif overcharge > 0:
        overcharge_rate = 100.0
    else:
        overcharge_rate = 0.0

    affected_units_rate = round((affected_units / units) * 100, 2) if units > 0 else 0.0
    accurate_units_rate = round((accurate / units) * 100, 2) if units > 0 else 0.0

    if overcharge <= 0.01:
        status = "clear"
    elif overcharge_rate <= 2:
        status = "low"
    elif overcharge_rate <= 5:
        status = "review"
    else:
        status = "action"

    return {
        "status": status,
        "overcharged_amount": round(overcharge, 2),
        "overcharged_units": round(affected_units),
        "overcharge_rate_pct": overcharge_rate,
        "affected_units_pct": affected_units_rate,
        "accurate_units_pct": accurate_units_rate,
        "net_variance": round(_number(net_variance), 2),
    }


def _expense_reconciliation_api_response(
    *,
    table_name,
    rows,
    status_table_name,
    status_rows,
    range_,
):
    numeric_columns = [
        "units",
        "net_sales",
        "referral_fees_applicable",
        "referral_fees_charged",
        "fba_fees_applicable",
        "fba_fees_charged",
        "platform_fees_applicable",
        "platform_fees_charged",
        "other_fees_applicable",
        "other_fees_charged",
        "referral_fees_accurate",
        "referral_fees_undercharged",
        "referral_fees_overcharged",
    ]
    cached = rows.copy()
    for column in numeric_columns:
        if column not in cached.columns:
            cached[column] = 0
        cached[column] = pd.to_numeric(cached[column], errors="coerce")
        if column != "fba_fees_applicable":
            cached[column] = cached[column].fillna(0)

    if "sku" not in cached.columns:
        cached["sku"] = ""
    if "product_name" not in cached.columns:
        cached["product_name"] = ""

    sku_text = cached["sku"].fillna("").astype(str).str.strip().str.lower()
    product_text = cached["product_name"].fillna("").astype(str).str.strip().str.lower()
    grand_mask = sku_text.eq("grand total") | product_text.eq("grand total")
    detail_rows = cached[~grand_mask].copy()
    stored_grand_rows = cached[grand_mask].copy()

    def _status_for_row(row):
        if float(row.get("referral_fees_overcharged", 0) or 0) != 0:
            return "Overcharged"
        if float(row.get("referral_fees_undercharged", 0) or 0) != 0:
            return "Undercharged"
        if float(row.get("referral_fees_applicable", 0) or 0) == 0:
            return "noreferallfee"
        return "Accurate"

    def _legacy_record(row, *, status=None, sku=None, product_name=None):
        units = float(row.get("units", 0) or 0)
        net_sales = float(row.get("net_sales", 0) or 0)
        applicable = float(row.get("referral_fees_applicable", 0) or 0)
        charged = float(row.get("referral_fees_charged", 0) or 0)
        fba_applicable = _nullable_fee(row.get("fba_fees_applicable"))
        fba_charged = float(row.get("fba_fees_charged", 0) or 0)
        platform_applicable = float(row.get("platform_fees_applicable", 0) or 0)
        platform_charged = float(row.get("platform_fees_charged", 0) or 0)
        other_applicable = float(row.get("other_fees_applicable", 0) or 0)
        other_charged = float(row.get("other_fees_charged", 0) or 0)
        undercharged = float(row.get("referral_fees_undercharged", 0) or 0)
        overcharged = float(row.get("referral_fees_overcharged", 0) or 0)
        resolved_status = status or _status_for_row(row)
        errorstatus = "OK" if resolved_status == "Accurate" else resolved_status.lower()

        return {
            "sku": str(row.get("sku", "") if sku is None else sku),
            "product_name": str(row.get("product_name", "") if product_name is None else product_name),
            "quantity": units,
            "return_quantity": 0,
            "total_quantity": units,
            "product_sales": net_sales,
            "net_sales": net_sales,
            "net_sales_total_value": net_sales,
            "total_amount": _calculate_total_amount(net_sales, units),
            "answer": applicable,
            "selling_fees": charged,
            "fbaanswer": fba_applicable,
            "fba_fees": fba_charged,
            "platform_fees_applicable": platform_applicable,
            "platform_fees_charged": platform_charged,
            "other_fees_applicable": other_applicable,
            "other_fees_charged": other_charged,
            "difference": overcharged - undercharged,
            "overcharged": overcharged,
            "referral_fees_accurate": float(row.get("referral_fees_accurate", 0) or 0),
            "referral_fees_undercharged": undercharged,
            "referral_fees_overcharged": overcharged,
            "status": resolved_status,
            "errorstatus": errorstatus,
        }

    detail_records = sorted(
        [_legacy_record(row, status="Total") for _, row in detail_rows.iterrows()],
        key=lambda record: (
            str(record.get("sku", "")).strip().lower(),
            str(record.get("product_name", "")).strip().lower(),
        ),
    )
    status_order = ["Accurate", "Undercharged", "Overcharged", "noreferallfee"]
    display_records = []
    status_records = {status: [] for status in status_order}
    summary_status_records = {status: [] for status in status_order}

    cached_status = status_rows.copy()
    for column in [
        "source_order",
        "units",
        "gross_sales",
        "net_sales",
        "total_amount",
        "product_sales",
        "shipping_credits",
        "promotional_rebates",
        "gift_wrap_credits",
        "referral_fee_per",
        "referral_fees_applicable",
        "referral_fees_charged",
        "fba_fees_applicable",
        "fba_fees_charged",
        "difference",
    ]:
        if column not in cached_status.columns:
            cached_status[column] = 0
        cached_status[column] = pd.to_numeric(
            cached_status[column],
            errors="coerce",
        )
        if column != "fba_fees_applicable":
            cached_status[column] = cached_status[column].fillna(0)

    status_aliases = {
        "accurate": "Accurate",
        "undercharged": "Undercharged",
        "overcharged": "Overcharged",
        "noreferallfee": "noreferallfee",
        "noreferralfee": "noreferallfee",
    }

    if "record_type" not in cached_status.columns:
        cached_status["record_type"] = "detail"
    cached_status["record_type"] = cached_status["record_type"].fillna("detail").astype(str)
    cached_status = cached_status.sort_values("source_order", kind="stable")

    def _status_record(row):
        resolved_status = status_aliases.get(
            str(row.get("status", "")).strip().lower(),
            "noreferallfee",
        )
        difference = float(row.get("difference", 0) or 0)
        applicable = float(row.get("referral_fees_applicable", 0) or 0)
        units = float(row.get("units", 0) or 0)
        net_sales = float(row.get("net_sales", 0) or 0)
        total_amount = float(
            row.get("total_amount", _calculate_total_amount(net_sales, units)) or 0
        )
        errorstatus = str(row.get("errorstatus", "") or "").strip()
        if not errorstatus:
            errorstatus = "OK" if resolved_status == "Accurate" else resolved_status.lower()
        return resolved_status, {
            "order_id": str(row.get("order_id", "") or ""),
            "sku": str(row.get("sku", "") or ""),
            "product_name": str(row.get("product_name", "") or ""),
            "quantity": units,
            "return_quantity": 0,
            "total_quantity": units,
            "gross_sales": float(row.get("gross_sales", 0) or 0),
            "product_sales": float(row.get("product_sales", row.get("gross_sales", 0)) or 0),
            "shipping_credits": float(row.get("shipping_credits", 0) or 0),
            "promotional_rebates": float(row.get("promotional_rebates", 0) or 0),
            "gift_wrap_credits": float(row.get("gift_wrap_credits", 0) or 0),
            "referral_fee_per": float(row.get("referral_fee_per", 0) or 0),
            "net_sales": net_sales,
            "net_sales_total_value": net_sales,
            "total_amount": total_amount,
            "answer": applicable,
            "selling_fees": float(row.get("referral_fees_charged", 0) or 0),
            "fbaanswer": _nullable_fee(row.get("fba_fees_applicable")),
            "fba_estimate_status": str(row.get("fba_estimate_status") or ""),
            "fba_quantity": float(row.get("fba_quantity", units)) if pd.notna(row.get("fba_quantity", units)) else units,
            "fba_fees": float(row.get("fba_fees_charged", 0) or 0),
            "difference": difference,
            "overcharged": max(difference, 0),
            "referral_fees_accurate": applicable if resolved_status == "Accurate" else 0,
            "referral_fees_undercharged": abs(difference) if resolved_status == "Undercharged" else 0,
            "referral_fees_overcharged": difference if resolved_status == "Overcharged" else 0,
            "status": resolved_status,
            "errorstatus": errorstatus,
        }

    fba_adjustments = []
    for _, row in cached_status.iterrows():
        if str(row.get("record_type", "")).strip().lower() == "fba_adjustment":
            fba_adjustments.append({
                "order_id": str(row.get("order_id", "") or ""),
                "sku": str(row.get("sku", "") or ""),
                "product_name": str(row.get("product_name", "") or ""),
                "fba_quantity": float(row.get("units", 0) or 0),
                "fba_fees": float(row.get("fba_fees_charged", 0) or 0),
                "fbaanswer": None, "fba_estimate_status": "refund_credit_not_comparable",
                "transaction_type": "Refund",
            })
            continue
        resolved_status, record = _status_record(row)
        if str(row.get("record_type", "")).strip().lower() == "summary":
            summary_status_records[resolved_status].append(record)
        else:
            status_records[resolved_status].append(record)

    sum_fields = [
        "quantity",
        "total_quantity",
        "product_sales",
        "net_sales",
        "net_sales_total_value",
        "answer",
        "selling_fees",
        "fbaanswer",
        "fba_fees",
        "platform_fees_applicable",
        "platform_fees_charged",
        "other_fees_applicable",
        "other_fees_charged",
        "difference",
        "overcharged",
        "referral_fees_accurate",
        "referral_fees_undercharged",
        "referral_fees_overcharged",
    ]

    def _summary_record(status, records):
        summary = {
            "sku": f"Charge - {status}",
            "product_name": "",
            "return_quantity": 0,
            "status": status,
            "errorstatus": "",
        }
        for field in sum_fields:
            summary[field] = (
                _sum_expected_fees(record.get(field) for record in records)
                if field == "fbaanswer"
                else float(sum(float(record.get(field, 0) or 0) for record in records))
            )
        return summary

    status_summaries = {}
    for status in status_order:
        status_summaries[status] = _summary_record(
            status,
            summary_status_records[status],
        )
        display_records.append(status_summaries[status])

    display_records.extend(detail_records)

    if not stored_grand_rows.empty:
        grand_record = _legacy_record(
            stored_grand_rows.iloc[-1],
            status="Total",
            sku="Grand Total",
            product_name="",
        )
    else:
        grand_record = _summary_record("Total", detail_records)
        grand_record["sku"] = "Grand Total"
    display_records.append(grand_record)

    fba_data = [record for records in status_records.values() for record in records
                if record.get("fba_estimate_status") != "not_fulfillment_sale"] + fba_adjustments
    fba_charged_by_sku = {}
    for record in fba_data:
        sku = record["sku"]
        fba_charged_by_sku[sku] = fba_charged_by_sku.get(sku, 0) + record["fba_fees"]
    fba_net_charged = round(sum(fba_charged_by_sku.values()), 2)
    for records in (detail_records, display_records):
        for record in records:
            if record.get("sku") in fba_charged_by_sku:
                record["fba_fees"] = round(fba_charged_by_sku[record["sku"]], 2)
            elif record.get("sku") == "Grand Total":
                record["fba_fees"] = fba_net_charged
    grand_record["fba_fees"] = fba_net_charged
    original_records = cached.astype(object).where(pd.notna(cached), None).to_dict(orient="records")
    net_sales_total = float(grand_record.get("net_sales_total_value", 0) or 0)
    platform_fee_total = float(grand_record.get("platform_fees_charged", 0) or 0)
    other_fee_total = float(grand_record.get("other_fees_charged", 0) or 0)
    fee_percentages = _fee_percentage_metrics(
        net_sales=net_sales_total,
        referral_charged=grand_record.get("selling_fees", 0),
        referral_applicable=grand_record.get("answer", 0),
        fba_charged=grand_record.get("fba_fees", 0),
        fba_applicable=grand_record.get("fbaanswer", 0),
        platform_charged=platform_fee_total,
        platform_applicable=grand_record.get("platform_fees_applicable", 0),
        other_charged=other_fee_total,
        other_applicable=grand_record.get("other_fees_applicable", 0),
    )
    referral_fee_insight = _referral_fee_insight_metrics(
        overcharged_amount=status_summaries["Overcharged"].get("difference", 0),
        overcharged_units=status_summaries["Overcharged"].get("quantity", 0),
        accurate_units=status_summaries["Accurate"].get("quantity", 0),
        total_units=sum(
            status_summaries[status].get("quantity", 0)
            for status in ("Accurate", "Undercharged", "Overcharged")
        ),
        applicable_fees=grand_record.get("answer", 0),
        net_variance=grand_record.get("difference", 0),
    )

    return {
        "success": True,
        "message": "Expense reconciliation table loaded successfully.",
        "range": range_,
        "table": display_records,
        "accurate_data": status_records["Accurate"],
        "undercharged_data": status_records["Undercharged"],
        "overcharged_data": status_records["Overcharged"],
        "no_ref_fee_data": status_records["noreferallfee"],
        "fba_data": fba_data,
        "created_table_name": table_name,
        "raw_table": original_records,
        "table_name": table_name,
        "platform_fee_total": platform_fee_total,
        "other_total": other_fee_total,
        "fee_percentages": fee_percentages,
        "referral_fee_insight": referral_fee_insight,
        "advertising_total": 0,
        "sku_monthly_summary": grand_record if range_ == "monthly" else {},
        "sku_monthly_rows": detail_records if range_ == "monthly" else [],
        "sku_monthly_table": table_name if range_ == "monthly" else None,
        "expense_reconciliation": {
            "success": True,
            "cached": True,
            "database": EXPENSE_RECONCILIATION_DB_NAME,
            "table_name": table_name,
            "status_table_name": status_table_name,
            "row_count": int(len(cached)),
            "columns": cached.columns.tolist(),
        },
    }


def get_previous_month(month, year):
    month = str(month).strip().lower()
    year = int(year)

    if month not in MONTHS:
        return None, None

    idx = MONTHS.index(month)

    if idx == 0:
        return "december", str(year - 1)

    return MONTHS[idx - 1], str(year)


def get_previous_quarter(quarter, year):
    q = str(quarter).strip().lower()
    year = int(year)

    if q in ("q1", "quarter1", "1"):
        return "quarter4", str(year - 1)
    if q in ("q2", "quarter2", "2"):
        return "quarter1", str(year)
    if q in ("q3", "quarter3", "3"):
        return "quarter2", str(year)
    if q in ("q4", "quarter4", "4"):
        return "quarter3", str(year)

    return None, None


def get_previous_year(year):
    return str(int(year) - 1)

@product_bp.route('/getConversionRate', methods=['GET'])
def get_conversion_rate():
    try:
        # Step 1: Get query params
        home_currency = (request.args.get('homecurrency') or '').strip()
        month = (request.args.get('month') or '').strip()
        year = (request.args.get('year') or '').strip()

        # Step 2: Validate
        if not home_currency or not month or not year:
            return jsonify({"error": "homecurrency, month, and year are required"}), 400

        # Step 3: Query admin_db.currency_conversion (case-insensitive)
        with admin_engine.connect() as conn:
            query = text("""
                SELECT conversion_rate
                FROM currency_conversion
                WHERE lower(selected_currency) = 'usd'
                  AND lower(user_currency) = :home_currency
                  AND lower(month) = :month
                  AND year = :year
                ORDER BY id DESC
                LIMIT 1
            """)
            row = conn.execute(query, {
                "home_currency": home_currency.lower(),
                "month": month.lower(),
                "year": int(year)
            }).fetchone()

        # Step 4: Not found
        if not row:
            return jsonify({"error": "Conversion rate not found"}), 404

        conversion_rate = float(row.conversion_rate)

        # Step 6: Send to frontend
        return jsonify({
            "from_currency": "USD",
            "homecurrency": home_currency.upper(),
            "month": month,
            "year": year,
            "conversion_rate": conversion_rate
        }), 200

    except Exception as e:
        return jsonify({"error": "Internal server error"}), 500


def resolve_country(country, currency):
    country = (country or "").lower()
    currency = (currency or "").lower()

    # 1. If country = global
    if country == "global":
        if currency == "usd":
            return "global"
        elif currency == "inr":
            return "global_inr"
        elif currency == "gbp":
            return "global_gbp"
        elif currency == "cad":
            return "global_cad"
        else:
            return "global"  # default fallback

    # 2. If country = uk
    if country == "uk":
        if currency == "usd":
            return "uk_usd"
        else:
            return "uk"  # default for all other currencies

    # 3. Default (no special logic)
    return country


def get_month_tokens_present_for_year(conn, user_id, country, year):
    """
    Finds monthly table month tokens for current selected year.

    Normal country example:
    skuwisemonthly_1_uk_may2026

    Global example:
    skuwisemonthly_1_global_may2026_table

    Returns:
    ["jan", "feb", "mar", ...]
    """

    prefix = f"skuwisemonthly_{user_id}_{country}_"

    valid_months = {
        "jan": 1,
        "january": 1,
        "feb": 2,
        "february": 2,
        "mar": 3,
        "march": 3,
        "apr": 4,
        "april": 4,
        "may": 5,
        "jun": 6,
        "june": 6,
        "jul": 7,
        "july": 7,
        "aug": 8,
        "august": 8,
        "sep": 9,
        "sept": 9,
        "september": 9,
        "oct": 10,
        "october": 10,
        "nov": 11,
        "november": 11,
        "dec": 12,
        "december": 12,
    }

    query = text("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_name LIKE :pattern
        ORDER BY table_name
    """)

    rows = conn.execute(query, {
        "pattern": f"{prefix}%"
    }).mappings().all()

    month_tokens = []

    for row in rows:
        table_name = row["table_name"]

        if not table_name.startswith(prefix):
            continue

        middle = table_name[len(prefix):]

        # For global:
        # may2026_table -> may2026
        if middle.endswith("_table"):
            middle = middle[:-len("_table")]

        # Now both formats become:
        # may2026
        if not middle.endswith(str(year)):
            continue

        month_token = middle[:-len(str(year))].lower()

        if month_token in valid_months:
            month_tokens.append(month_token)

    return sorted(set(month_tokens), key=lambda month: valid_months[month])



def aggregate_monthly_sku_rows(rows):
    grouped = {}

    preferred_key_columns = [
        "sku",
        "asin",
        "msku",
        "fnsku",
        "product_name",
        "product",
        "title"
    ]

    derived_fields = {
        "tacos_total_advertising_cost_of_sale",
        "tacos",
        "tacos_pct",
        "acos",
        "asp",
        "average_selling_price",
        "avg_selling_price",
        "cm2_margins",
        "cm2_profit_percentage",
        "cm2_profit_per",
        "cm2_profit_per_unit",
        "profit_percentage",
        "unit_wise_profitability",
        "sales_mix",
        "profit_mix",
        "reimbursement_vs_sales",
        "rembursment_vs_cm2_margins",
        "promotional_rebates_percentage",
        "total_cm2_margins",
        "marketplace_fees_per_unit",
        "cost_of_ads_per_unit",
        "gross_sales_per_unit",
        "net_sales_per_unit",
        "others_per_unit",
        "cash_generated_per_unit",
        "net_reimbursement_per_unit",
    }

    for row in rows:
        normalized_row = _normalize_sku_row(dict(row))

        available_key_columns = [
            col for col in preferred_key_columns
            if col in normalized_row
        ]

        if available_key_columns:
            key = tuple(
                (col, normalized_row.get(col))
                for col in available_key_columns
            )
        else:
            key = tuple(
                (col, normalized_row.get(col))
                for col, value in normalized_row.items()
                if not isinstance(value, (int, float, Decimal)) or isinstance(value, bool)
            )

        if key not in grouped:
            grouped[key] = dict(normalized_row)

            # Do not keep first month's ASP for the yearly previous period.
            # It will be recalculated after all months are summed.
            for field in derived_fields:
                if field in grouped[key]:
                    grouped[key][field] = 0

        else:
            for col, value in normalized_row.items():

                # ASP is not additive, so do not sum it month-to-month.
                if col in derived_fields:
                    continue

                if isinstance(value, (int, float, Decimal)) and not isinstance(value, bool):
                    grouped[key][col] = (grouped[key].get(col) or 0) + (value or 0)

    def safe_number(value):
        try:
            if value is None or value == "":
                return 0.0
            return float(value)
        except Exception:
            return 0.0

    # Recalculate derived values from final aggregated totals.
    for row in grouped.values():
        net_sales = safe_number(row.get("net_sales"))
        total_quantity = safe_number(row.get("total_quantity"))
        profit = safe_number(row.get("profit"))
        cm2_profit = safe_number(row.get("cm2_profit_total") or row.get("cm2_profit"))

        # Reimbursement vs CM2 must use the all-in TOTAL CM2 value.
        total_cm2_source = row.get("total_cm2_profit")
        if total_cm2_source is None or pd.isna(total_cm2_source) or (
            isinstance(total_cm2_source, str) and not total_cm2_source.strip()
        ):
            total_cm2_source = row.get("cm2_profit_total")
        if total_cm2_source is None or pd.isna(total_cm2_source) or (
            isinstance(total_cm2_source, str) and not total_cm2_source.strip()
        ):
            total_cm2_source = row.get("cm2_profit")
        total_cm2_profit = safe_number(total_cm2_source)

        promotional_rebates = safe_number(row.get("promotional_rebates"))

        # Yearly TOTAL reimbursement must be derived from the final yearly
        # disbursement/debt totals, not by adding monthly reimbursement values.
        # Example (UK 2026): abs(15400.64 - 1906.23) = 13494.41.
        row_name = str(row.get("product_name") or row.get("sku") or "").strip().lower()
        is_total_row = row_name == "total"

        if is_total_row:
            disbursement = safe_number(row.get("disbursement"))
            debt_payment = safe_number(row.get("debt_payment"))
            rembursement_fee = abs(disbursement - debt_payment)
            row["rembursement_fee"] = rembursement_fee

            # Keep frontend aliases in sync when these columns are present.
            if "current_net_reimbursement" in row:
                row["current_net_reimbursement"] = rembursement_fee
        else:
            rembursement_fee = safe_number(row.get("rembursement_fee"))

        fba_disposal = safe_number(row.get("fba_disposal"))
        lost_total = safe_number(row.get("lost_total"))

        asp = net_sales / total_quantity if total_quantity else 0
        profit_percentage = (profit / net_sales) * 100 if net_sales else 0
        unit_wise_profitability = profit / total_quantity if total_quantity else 0
        cm2_margin = (cm2_profit / net_sales) * 100 if net_sales else 0
        reimbursement_vs_sales = (
            abs(rembursement_fee / net_sales) * 100 if net_sales else 0
        )
        rembursment_vs_cm2_margins = (
            abs(rembursement_fee / total_cm2_profit) * 100
            if total_cm2_profit else 0
        )
        promotional_rebates_percentage = (
            (promotional_rebates / net_sales) * 100 if net_sales else 0
        )
        inventory_charges_and_reimbursement = (
            abs(fba_disposal) - abs(lost_total)
            if fba_disposal or lost_total
            else safe_number(row.get("inventory_charges_and_reimbursement"))
        )

        row["asp"] = asp
        row["profit_percentage"] = profit_percentage
        row["unit_wise_profitability"] = unit_wise_profitability
        row["cm2_margins"] = cm2_margin
        row["total_cm2_margins"] = cm2_margin
        row["cm2_profit_percentage"] = cm2_margin
        row["cm2_profit_per"] = cm2_margin
        row["cm2_profit_per_unit"] = cm2_profit / total_quantity if total_quantity else 0
        row["reimbursement_vs_sales"] = reimbursement_vs_sales
        row["rembursment_vs_cm2_margins"] = rembursment_vs_cm2_margins
        row["lost_total"] = abs(lost_total)
        row["inventory_charges_and_reimbursement"] = (
            inventory_charges_and_reimbursement
        )
        row["promotional_rebates_percentage"] = promotional_rebates_percentage

        row["_is_total_row"] = is_total_row

        if row["_is_total_row"]:
            visible_ads_total = abs(safe_number(row.get("visible_ads")))
            deals_ads_total = abs(safe_number(row.get("dealsvouchar_ads")))

            if visible_ads_total or deals_ads_total:
                rollup_ad_total = visible_ads_total + deals_ads_total
                row["ads_spend"] = visible_ads_total
                row["ads_spend_raw"] = visible_ads_total
                row["advertising_total"] = visible_ads_total
                row["advertising_total_final"] = rollup_ad_total
                row["advertising_fees"] = rollup_ad_total
                row["total_ads"] = rollup_ad_total

        # Percentages are not additive. Use the final period's ad costs,
        # including the TOTAL ad overrides above, and its combined net sales.
        tacos = calculate_pnl_tacos(row)
        net_sales = safe_float(row.get("net_sales"))
        ads_spend_for_acos = abs(safe_float(row.get("ads_spend")))
        if ads_spend_for_acos == 0:
            ads_spend_for_acos = abs(safe_float(row.get("advertising_total")))
        acos = round((ads_spend_for_acos / net_sales) * 100, 2) if net_sales else 0.0
        row["tacos_total_advertising_cost_of_sale"] = tacos
        row["acos"] = acos
        for alias in ("tacos", "tacos_pct"):
            if alias in row:
                row[alias] = tacos

        if "average_selling_price" in row:
            row["average_selling_price"] = asp

        if "avg_selling_price" in row:
            row["avg_selling_price"] = asp

    aggregated_rows = list(grouped.values())
    product_rows = [
        row for row in aggregated_rows
        if not row.pop("_is_total_row", False)
    ]
    total_sales = sum(abs(safe_number(row.get("net_sales"))) for row in product_rows)
    total_profit = sum(abs(safe_number(row.get("profit"))) for row in product_rows)

    for row in aggregated_rows:
        row_name = str(row.get("product_name") or row.get("sku") or "").strip().lower()
        if row_name == "total":
            row["sales_mix"] = 100
            row["profit_mix"] = 100
            continue

        net_sales = safe_number(row.get("net_sales"))
        profit = safe_number(row.get("profit"))
        row["sales_mix"] = (net_sales / total_sales) * 100 if total_sales else 0
        row["profit_mix"] = (profit / total_profit) * 100 if total_profit else 0

    return add_per_unit_fields(aggregated_rows)


def get_year_monthly_aggregated_data(
    conn,
    engine,
    metadata,
    user_id,
    country,
    year,
    month_limit=None
):
    month_token_to_num = {
        "jan": 1,
        "january": 1,
        "feb": 2,
        "february": 2,
        "mar": 3,
        "march": 3,
        "apr": 4,
        "april": 4,
        "may": 5,
        "jun": 6,
        "june": 6,
        "jul": 7,
        "july": 7,
        "aug": 8,
        "august": 8,
        "sep": 9,
        "sept": 9,
        "september": 9,
        "oct": 10,
        "october": 10,
        "nov": 11,
        "november": 11,
        "dec": 12,
        "december": 12,
    }

    if month_limit is not None:
        month_limit = max(0, min(int(month_limit), 12))

    monthly_rows = []
    used_tables = []

    for month_token in get_month_tokens_present_for_year(
        conn=conn,
        user_id=user_id,
        country=country,
        year=year
    ):
        month_num = month_token_to_num.get(str(month_token).lower())

        if month_limit is not None and month_num and month_num > month_limit:
            continue

        if country == "global":
            monthly_table_name = (
                f"skuwisemonthly_{user_id}_{country}_{month_token}{year}_table"
            )
        else:
            monthly_table_name = (
                f"skuwisemonthly_{user_id}_{country}_{month_token}{year}"
            )

        try:
            monthly_table = Table(
                monthly_table_name,
                metadata,
                autoload_with=engine
            )

            results = conn.execute(
                select(*monthly_table.columns)
            ).mappings().all()

            monthly_rows.extend(results)
            used_tables.append(monthly_table_name)

        except Exception:
            continue

    return aggregate_monthly_sku_rows(monthly_rows), used_tables


def get_previous_year_monthly_aggregated_data(
    conn,
    engine,
    metadata,
    user_id,
    country,
    year,
    month_limit=None
):
    previous_year = get_previous_year(year)

    current_year_month_tokens = get_month_tokens_present_for_year(
        conn=conn,
        user_id=user_id,
        country=country,
        year=year
    )

    month_token_to_num = {
        "jan": 1,
        "january": 1,
        "feb": 2,
        "february": 2,
        "mar": 3,
        "march": 3,
        "apr": 4,
        "april": 4,
        "may": 5,
        "jun": 6,
        "june": 6,
        "jul": 7,
        "july": 7,
        "aug": 8,
        "august": 8,
        "sep": 9,
        "sept": 9,
        "september": 9,
        "oct": 10,
        "october": 10,
        "nov": 11,
        "november": 11,
        "dec": 12,
        "december": 12,
    }

    if month_limit is not None:
        month_limit = max(0, min(int(month_limit), 12))

    all_previous_month_rows = []
    used_previous_tables = []

    for month_token in current_year_month_tokens:
        month_num = month_token_to_num.get(str(month_token).lower())

        # Example:
        # if today is July, month_limit = 6
        # so July and later months will be skipped
        if month_limit is not None and month_num and month_num > month_limit:
            continue

        if country == "global":
            previous_monthly_table_name = (
                f"skuwisemonthly_{user_id}_{country}_{month_token}{previous_year}_table"
            )
        else:
            previous_monthly_table_name = (
                f"skuwisemonthly_{user_id}_{country}_{month_token}{previous_year}"
            )

        try:
            previous_monthly_table = Table(
                previous_monthly_table_name,
                metadata,
                autoload_with=engine
            )

            monthly_results = conn.execute(
                select(*previous_monthly_table.columns)
            ).mappings().all()

            all_previous_month_rows.extend(monthly_results)
            used_previous_tables.append(previous_monthly_table_name)

        except Exception:
            continue

    previous_data = aggregate_monthly_sku_rows(all_previous_month_rows)

    return previous_data, used_previous_tables


def _sync_total_advertising_components(rows):
    """Keep period total-row ad fields consistent with their displayed components."""
    for row in rows or []:
        sku = str(row.get("sku") or "").strip().lower()
        product_name = str(row.get("product_name") or "").strip().lower()
        if sku not in {"total", "totals", "grand_total"} and product_name not in {
            "total", "totals", "grand total"
        }:
            continue

        def number(*values):
            for value in values:
                try:
                    if value not in (None, ""):
                        return float(value)
                except (TypeError, ValueError):
                    continue
            return 0.0

        visibility_ads = abs(
            number(row.get("visible_ads")) or number(row.get("brand_spend"))
        )
        deals_ads = abs(
            number(row.get("dealsvouchar_ads"))
            or number(row.get("dealsvoucher_ads"))
            or number(row.get("dealvouchars_ads"))
        )
        if not visibility_ads and not deals_ads:
            continue

        total_ads = visibility_ads + deals_ads
        row["visible_ads"] = visibility_ads
        row["advertising_total"] = visibility_ads
        row["advertising_total_final"] = total_ads
        row["advertising_fees"] = total_ads
        row["total_ads"] = total_ads
        row["ads_spend"] = visibility_ads

    return rows


_PROMOTIONS_PLACEMENTS = {
    "in_net_sales",
    "in_ads_spend",
}


def _read_promotions_placement(allow_after_cm1=True):
    placement = (request.args.get("promotionsPlacement") or "in_net_sales").strip().lower()
    if placement not in _PROMOTIONS_PLACEMENTS:
        return None
    return placement


def _read_amazon_fees_placement(allow_after_cm1=True):
    placement = (request.args.get("amazonFeesPlacement") or "before_cm1").strip().lower()
    if placement not in {"before_cm1", "after_cm1", "in_ads_spend"}:
        return None
    if not allow_after_cm1 and placement == "after_cm1":
        return "before_cm1"
    return placement


def _has_cm2_data(rows):
    for row in rows:
        name = str(row.get("product_name") or "").strip().lower()
        sku = str(row.get("sku") or "").strip().lower()
        if name in {"total", "others"} or sku in {"total", "others"}:
            continue

        ads_spend = safe_float(
            row.get("ads_spend")
            if row.get("ads_spend") is not None
            else row.get("advertising_total")
        )
        acos = safe_float(row.get("acos"))
        net_sales = safe_float(row.get("net_sales"))
        if not acos and net_sales:
            acos = ads_spend / net_sales * 100

        cm1_profit = safe_float(row.get("profit"))
        cm2_profit = safe_float(
            row.get("cm2_profit")
            if row.get("cm2_profit") is not None
            else row.get("cm2_profit_total")
        )
        if ads_spend != 0 or acos != 0 or abs(cm2_profit - cm1_profit) > 0.01:
            return True

    return False


def _apply_promotions_placement(rows, placement):
    for row in rows:
        promotion = safe_float(row.get("promotional_rebates"))
        adjustment = abs(promotion)
        net_sales = safe_float(row.get("net_sales")) + (adjustment if placement != "in_net_sales" else 0)
        profit = safe_float(row.get("profit"))
        if placement == "in_ads_spend":
            profit += adjustment
        quantity = safe_float(row.get("total_quantity"))
        cm2_profit = safe_float(row.get("cm2_profit"))
        cm2_total_profit = safe_float(
            row.get("total_cm2_profit")
            if row.get("total_cm2_profit") is not None
            else row.get("cm2_profit_total")
            if row.get("cm2_profit_total") is not None
            else row.get("cm2_profit")
        )

        row["net_sales"] = round(net_sales, 2)
        row["profit"] = round(profit, 2)
        row["asp"] = round(net_sales / quantity, 2) if quantity else 0
        row["profit_percentage"] = round(profit / net_sales * 100, 2) if net_sales else 0
        row["unit_wise_profitability"] = round(profit / quantity, 2) if quantity else 0
        row["promotional_rebates_percentage"] = (
            round(adjustment / net_sales * 100, 2) if net_sales else 0
        )
        if placement == "in_ads_spend":
            row["ads_spend"] = safe_float(row.get("ads_spend")) + adjustment
            for key in (
                "advertising_total_final",
                "advertising_total",
                "total_ads",
                "advertising_fees",
            ):
                if key in row:
                    row[key] = safe_float(row.get(key)) + adjustment
            row["acos"] = round(row["ads_spend"] / net_sales * 100, 2) if net_sales else 0
            row["cm2_profit"] = cm2_profit
        row["cost_of_advertisement"] = calculate_pnl_cost_of_advertisement(row)
        row["total_advertising_cost"] = calculate_pnl_total_advertising_cost(row)
        tacos = calculate_pnl_tacos(row)
        row["tacos_total_advertising_cost_of_sale"] = tacos
        row["tacos"] = tacos

        if "Net_Sales" in row:
            row["Net_Sales"] = row["net_sales"]
        if "Profit" in row:
            row["Profit"] = row["profit"]
        if "total_profit" in row:
            row["total_profit"] = row["profit"]
        if "cm1_profit" in row:
            row["cm1_profit"] = row["profit"]
        if "cm1_profit_per_unit" in row:
            row["cm1_profit_per_unit"] = row["unit_wise_profitability"]
        if "cm1_profit_per" in row:
            row["cm1_profit_per"] = row["profit_percentage"]
        if "cm1_profit_percentage" in row:
            row["cm1_profit_percentage"] = row["profit_percentage"]

        if "acos" in row:
            ads_value = row.get("ads_spend")
            if ads_value is None:
                ads_value = row.get("advertising_total")
            ads_spend = safe_float(ads_value)
            row["acos"] = round(ads_spend / net_sales * 100, 2) if net_sales else 0
        if "cm2_margins" in row:
            row["cm2_margins"] = (
                round(cm2_total_profit / net_sales * 100, 2) if net_sales else 0
            )
        if "cm2_profit_percentage" in row:
            row["cm2_profit_percentage"] = (
                round(cm2_total_profit / net_sales * 100, 2) if net_sales else 0
            )
        if "cm2_profit_per" in row:
            row["cm2_profit_per"] = (
                round(cm2_profit / net_sales * 100, 2) if net_sales else 0
            )
        if "cm2_profit_per_unit" in row:
            row["cm2_profit_per_unit"] = (
                round(cm2_profit / quantity, 2) if quantity else 0
            )
        if "reimbursement_vs_sales" in row:
            reimbursement = abs(safe_float(row.get("rembursement_fee")))
            row["reimbursement_vs_sales"] = (
                round(reimbursement / net_sales * 100, 2) if net_sales else 0
            )

    return rows


def _apply_amazon_fees_placement(rows, placement):
    if placement not in {"after_cm1", "in_ads_spend"}:
        return rows

    for row in rows:
        amazon_fees = abs(safe_float(row.get("amazon_fee")))
        if placement == "in_ads_spend":
            net_sales = safe_float(row.get("net_sales"))
            row["ads_spend"] = safe_float(row.get("ads_spend")) + amazon_fees
            row["acos"] = round(row["ads_spend"] / net_sales * 100, 2) if net_sales else 0
            row["cm2_profit"] = safe_float(row.get("cm2_profit")) - amazon_fees
            row["tacos_total_advertising_cost_of_sale"] = calculate_pnl_tacos(row)
            continue
        profit = safe_float(row.get("profit")) + amazon_fees
        net_sales = safe_float(row.get("net_sales"))
        quantity = safe_float(row.get("total_quantity"))

        row["profit"] = round(profit, 2)
        row["profit_percentage"] = (
            round(profit / net_sales * 100, 2) if net_sales else 0
        )
        row["unit_wise_profitability"] = (
            round(profit / quantity, 2) if quantity else 0
        )

        if "Profit" in row:
            row["Profit"] = row["profit"]
        if "total_profit" in row:
            row["total_profit"] = row["profit"]
        if "cm1_profit" in row:
            row["cm1_profit"] = row["profit"]
        if "cm1_profit_per_unit" in row:
            row["cm1_profit_per_unit"] = row["unit_wise_profitability"]
        if "cm1_profit_per" in row:
            row["cm1_profit_per"] = row["profit_percentage"]
        if "cm1_profit_percentage" in row:
            row["cm1_profit_percentage"] = row["profit_percentage"]

    return rows


@product_bp.route('/YearlySKU', methods=['GET'])
def YearlySKU():
    promotions_placement = _read_promotions_placement(allow_after_cm1=False)
    if promotions_placement is None:
        return jsonify({"error": "Invalid promotionsPlacement"}), 400
    amazon_fees_placement = _read_amazon_fees_placement(allow_after_cm1=False)
    if amazon_fees_placement is None:
        return jsonify({"error": "Invalid amazonFeesPlacement"}), 400

    country = (request.args.get('country') or '').lower()
    country_param = request.args.get('country', '').lower()
    currency_param = (request.args.get('homeCurrency') or '').lower()

    country = resolve_country(country_param, currency_param)

    year = (request.args.get('year') or '').strip()

    # Validate the query parameters
    if not country or not year:
        return jsonify({'error': 'Country and year are required'}), 400

    auth_header = request.headers.get('Authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        return jsonify({'error': 'Authorization token is missing or invalid'}), 401

    token = auth_header.split(' ')[1]
    try:
        payload, user_id, member_id = get_effective_user_id_from_token(token)
    except jwt.ExpiredSignatureError:
        return jsonify({'error': 'Token has expired'}), 401
    except jwt.InvalidTokenError:
        return jsonify({'error': 'Invalid token'}), 401

    try:
        engine = user_engine
        metadata = MetaData(schema='public')  # align with other routes
        table_name = f"skuwiseyearly_{user_id}_{country}_{year}_table"

        try:
            user_specific_table = Table(table_name, metadata, autoload_with=engine)
        except Exception:
            return jsonify({'error': f"Table '{table_name}' not found for user {user_id}"}), 404

        with engine.connect() as conn:
            results = conn.execute(select(*user_specific_table.columns)).mappings().all()

        # 🔒 Normalize all rows so the UI gets true numbers, not strings
        current_data = [_normalize_sku_row(dict(row)) for row in results]
        current_data = _sync_total_advertising_components(current_data)
        current_data = add_per_unit_fields(current_data)
        used_current_tables = [table_name]

        try:
            selected_year = int(year)
            today_year = date.today().year
            current_year_month_limit = max(date.today().month - 1, 0)

            # The US yearly table already aggregates finalized monthly reports,
            # including their amounts and recalculated annual rates.
            if country != "us" and selected_year == today_year and current_year_month_limit == 0:
                current_data = []
                used_current_tables = []
            elif country != "us" and selected_year == today_year:
                with engine.connect() as conn:
                    monthly_current_data, used_current_tables_from_months = (
                        get_year_monthly_aggregated_data(
                            conn=conn,
                            engine=engine,
                            metadata=metadata,
                            user_id=user_id,
                            country=country,
                            year=year,
                            month_limit=current_year_month_limit,
                        )
                    )

                if monthly_current_data:
                    current_data = monthly_current_data
                    used_current_tables = used_current_tables_from_months

        except Exception:
            used_current_tables = [table_name]

        # Old/full-year responses come directly from the yearly table, so keep
        # its persisted decimal card columns in sync. Current-year rollups are
        # assembled from monthly source tables and are not written over it.
        if used_current_tables == [table_name]:
            try:
                persist_per_unit_fields(engine, table_name, current_data)
            except Exception:
                pass
        current_data = _apply_promotions_placement(current_data, promotions_placement)
        current_data = _apply_amazon_fees_placement(
            current_data, amazon_fees_placement
        )

        previous_year = get_previous_year(year)
        previous_table_name = f"skuwisemonthly_{user_id}_{country}_aggregated_till_current_months_{previous_year}"
        previous_data = []

        try:
            previous_year_month_limit = max(date.today().month - 1, 0)

            with engine.connect() as conn:
                previous_data, used_previous_tables = get_previous_year_monthly_aggregated_data(
                    conn=conn,
                    engine=engine,
                    metadata=metadata,
                    user_id=user_id,
                    country=country,
                    year=year,
                    month_limit=previous_year_month_limit
                )

        except Exception:
            previous_data = []
            used_previous_tables = []
            previous_year_month_limit = 0

        previous_data = add_per_unit_fields(previous_data)
        previous_data = _apply_promotions_placement(previous_data, promotions_placement)
        previous_data = _apply_amazon_fees_placement(
            previous_data, amazon_fees_placement
        )

        response_payload = {
            "current_table_name": table_name,
            "used_current_tables": used_current_tables,
            "current_data": current_data,
            "previous_table_name": previous_table_name,
            "previous_data": previous_data,
            "card_metrics": build_pnl_card_metrics(current_data, previous_data),
        }

        # Add UK/US slices for Global annual responses. Keep the legacy combined
        # fields above and all country-specific response shapes unchanged.
        if country == "global":
            def load_country_year_rows(country_key):
                country_table_name = f"skuwiseyearly_{user_id}_{country_key}_{year}_table"
                country_current = []
                try:
                    country_table = Table(country_table_name, metadata, autoload_with=engine)
                    with engine.connect() as conn:
                        rows = conn.execute(select(*country_table.columns)).mappings().all()
                    country_current = [_normalize_sku_row(dict(row)) for row in rows]
                    country_current = _sync_total_advertising_components(country_current)
                    country_current = add_per_unit_fields(country_current)
                except Exception:
                    country_current = []

                try:
                    selected_year = int(year)
                    today_year = date.today().year
                    month_limit = max(date.today().month - 1, 0)
                    if country_key != "us" and selected_year == today_year:
                        if month_limit == 0:
                            country_current = []
                        else:
                            with engine.connect() as conn:
                                monthly_rows, _ = get_year_monthly_aggregated_data(
                                    conn=conn,
                                    engine=engine,
                                    metadata=metadata,
                                    user_id=user_id,
                                    country=country_key,
                                    year=year,
                                    month_limit=month_limit,
                                )
                            if monthly_rows:
                                country_current = monthly_rows
                except Exception:
                    pass

                country_previous = []
                try:
                    with engine.connect() as conn:
                        country_previous, _ = get_previous_year_monthly_aggregated_data(
                            conn=conn,
                            engine=engine,
                            metadata=metadata,
                            user_id=user_id,
                            country=country_key,
                            year=year,
                            month_limit=max(date.today().month - 1, 0),
                        )
                except Exception:
                    country_previous = []

                country_current = _apply_promotions_placement(
                    country_current, promotions_placement
                )
                country_current = _apply_amazon_fees_placement(
                    country_current, amazon_fees_placement
                )
                country_previous = add_per_unit_fields(country_previous)
                country_previous = _apply_promotions_placement(
                    country_previous, promotions_placement
                )
                country_previous = _apply_amazon_fees_placement(
                    country_previous, amazon_fees_placement
                )
                return country_current, country_previous

            current_data_uk, previous_data_uk = load_country_year_rows("uk")
            current_data_us, previous_data_us = load_country_year_rows("us")
            response_payload.update({
                "current_data_uk": current_data_uk,
                "current_data_us": current_data_us,
                "previous_data_uk": previous_data_uk,
                "previous_data_us": previous_data_us,
                "card_metrics_uk": build_pnl_card_metrics(current_data_uk, previous_data_uk),
                "card_metrics_us": build_pnl_card_metrics(current_data_us, previous_data_us),
            })

        return jsonify(response_payload), 200

    except SQLAlchemyError as e:
        return jsonify({'error': 'Error accessing the database'}), 500
    except Exception as e:
        return jsonify({'error': 'An error occurred while fetching table data'}), 500




# @product_bp.route('/YearlySKU', methods=['GET'])
# def YearlySKU():
#     country = (request.args.get('country') or '').lower()
#     country_param = request.args.get('country', '').lower()
#     currency_param = (request.args.get('homeCurrency') or '').lower()

#     country = resolve_country(country_param, currency_param)

#     year = (request.args.get('year') or '').strip()

#     # Validate the query parameters
#     if not country or not year:
#         return jsonify({'error': 'Country and year are required'}), 400

#     auth_header = request.headers.get('Authorization')
#     if not auth_header or not auth_header.startswith('Bearer '):
#         return jsonify({'error': 'Authorization token is missing or invalid'}), 401

#     token = auth_header.split(' ')[1]
#     try:
#         payload, user_id, member_id = get_effective_user_id_from_token(token)
#     except jwt.ExpiredSignatureError:
#         return jsonify({'error': 'Token has expired'}), 401
#     except jwt.InvalidTokenError:
#         return jsonify({'error': 'Invalid token'}), 401

#     try:
#         engine = user_engine
#         metadata = MetaData(schema='public')  # align with other routes
#         table_name = f"skuwiseyearly_{user_id}_{country}_{year}_table"

#         try:
#             user_specific_table = Table(table_name, metadata, autoload_with=engine)
#         except Exception:
#             return jsonify({'error': f"Table '{table_name}' not found for user {user_id}"}), 404

#         with engine.connect() as conn:
#             results = conn.execute(select(*user_specific_table.columns)).mappings().all()

#         # 🔒 Normalize all rows so the UI gets true numbers, not strings
#         current_data = [_normalize_sku_row(dict(row)) for row in results]

#         previous_year = get_previous_year(year)
#         previous_table_name = f"skuwiseyearly_{user_id}_{country}_{previous_year}_table"
#         previous_data = []

#         try:
#             previous_table = Table(previous_table_name, metadata, autoload_with=engine)

#             with engine.connect() as conn:
#                 prev_results = conn.execute(select(*previous_table.columns)).mappings().all()

#             previous_data = [_normalize_sku_row(dict(row)) for row in prev_results]
#         except Exception:
#             previous_data = []

#         return jsonify({
#             "current_table_name": table_name,
#             "current_data": current_data,
#             "previous_table_name": previous_table_name,
#             "previous_data": previous_data
#         }), 200

#     except SQLAlchemyError as e:
#         return jsonify({'error': 'Error accessing the database'}), 500
#     except Exception as e:
#         return jsonify({'error': 'An error occurred while fetching table data'}), 500

def resolve_country(country, currency):
    country = (country or "").lower()
    currency = (currency or "").lower()   # '' if missing

    # 1) Global: default to USD only here
    if country == "global":
        if currency in ("", "usd"):
            return "global"
        elif currency == "inr":
            return "global_inr"
        elif currency == "gbp":
            return "global_gbp"
        elif currency == "cad":
            return "global_cad"
        else:
            return "global"

    # 2) UK: only go to uk_usd if explicitly requested
    if country == "uk":
        if currency == "usd":
            return "uk_usd"
        return "uk"

    return country

@product_bp.route('/quarterlyskutable', methods=['GET'])
def quarterlyskutable():
    promotions_placement = _read_promotions_placement(allow_after_cm1=False)
    if promotions_placement is None:
        return jsonify({"error": "Invalid promotionsPlacement"}), 400
    amazon_fees_placement = _read_amazon_fees_placement(allow_after_cm1=False)
    if amazon_fees_placement is None:
        return jsonify({"error": "Invalid amazonFeesPlacement"}), 400

    quarter = request.args.get('quarter')
    country_param = request.args.get('country', '')
    currency_param = (request.args.get('homeCurrency') or '').lower()

    country = resolve_country(country_param, currency_param)
    year = (request.args.get('year') or '').strip()

    if not quarter or not country or not year:
        return jsonify({'error': 'Quarter, country, and year are required'}), 400

    auth_header = request.headers.get('Authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        return jsonify({'error': 'Authorization token is missing or invalid'}), 401

    token = auth_header.split(' ')[1]
    try:
        payload, user_id, member_id = get_effective_user_id_from_token(token)
    except jwt.ExpiredSignatureError:
        return jsonify({'error': 'Token has expired'}), 401
    except jwt.InvalidTokenError:
        return jsonify({'error': 'Invalid token'}), 401

    def normalize_quarter(q):
        q = str(q).strip().lower()

        if q in ("q1", "quarter1", "1"):
            return "quarter1"
        if q in ("q2", "quarter2", "2"):
            return "quarter2"
        if q in ("q3", "quarter3", "3"):
            return "quarter3"
        if q in ("q4", "quarter4", "4"):
            return "quarter4"

        return None

    try:
        engine = user_engine
        metadata = MetaData(schema='public')

        current_quarter = normalize_quarter(quarter)

        if not current_quarter:
            return jsonify({'error': 'Invalid quarter value'}), 400

        table_name = f"{current_quarter}_{user_id}_{country}_{year}_table".lower()

        try:
            user_specific_table = Table(table_name, metadata, autoload_with=engine)

            with engine.connect() as conn:
                results = conn.execute(
                    select(*user_specific_table.columns)
                ).mappings().all()

            current_data = [_normalize_sku_row(dict(row)) for row in results]
            current_data = _sync_total_advertising_components(current_data)
            current_data = add_per_unit_fields(current_data)
            try:
                persist_per_unit_fields(engine, table_name, current_data)
            except Exception:
                pass

        except Exception:
            return jsonify({
                'error': f"Table '{table_name}' not found for user {user_id}"
            }), 404

        previous_table_name = None
        previous_data = []

        prev_quarter, prev_year = get_previous_quarter(current_quarter, year)

        if prev_quarter and prev_year:
            previous_table_name = f"{prev_quarter}_{user_id}_{country}_{prev_year}_table".lower()

            try:
                previous_table = Table(previous_table_name, metadata, autoload_with=engine)

                with engine.connect() as conn:
                    prev_results = conn.execute(
                        select(*previous_table.columns)
                    ).mappings().all()

                previous_data = [_normalize_sku_row(dict(row)) for row in prev_results]
                previous_data = _sync_total_advertising_components(previous_data)
                previous_data = add_per_unit_fields(previous_data)
                try:
                    persist_per_unit_fields(engine, previous_table_name, previous_data)
                except Exception:
                    pass

            except Exception:
                previous_data = []

        current_data = _apply_promotions_placement(current_data, promotions_placement)
        current_data = _apply_amazon_fees_placement(
            current_data, amazon_fees_placement
        )
        previous_data = _apply_promotions_placement(previous_data, promotions_placement)
        previous_data = _apply_amazon_fees_placement(
            previous_data, amazon_fees_placement
        )

        return jsonify({
            "current_table_name": table_name,
            "current_data": current_data,
            "previous_table_name": previous_table_name,
            "previous_data": previous_data,
            "card_metrics": build_pnl_card_metrics(current_data, previous_data),
        }), 200

    except Exception as e:
        return jsonify({
            'error': 'An unexpected error occurred',
            'message': str(e)
        }), 500
    
    


@product_bp.route('/currency-rates', methods=['GET'])
def get_currency_rates():
    auth_header = request.headers.get('Authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        return jsonify({'error': 'Authorization token is missing or invalid'}), 401

    token = auth_header.split(' ')[1]

    try:
        payload, user_id, member_id = get_effective_user_id_from_token(token)
    except jwt.ExpiredSignatureError:
        return jsonify({'error': 'Token has expired'}), 401
    except jwt.InvalidTokenError:
        return jsonify({'error': 'Invalid token'}), 401

    try:
        # Fetch all historical records
        with admin_engine.connect() as conn:
            query = text("""
                SELECT
                    user_currency,
                    country,
                    selected_currency,
                    conversion_rate,
                    month,
                    year
                FROM currency_conversion
                ORDER BY year DESC,
                    CASE lower(month)
                        WHEN 'january' THEN 1
                        WHEN 'february' THEN 2
                        WHEN 'march' THEN 3
                        WHEN 'april' THEN 4
                        WHEN 'may' THEN 5
                        WHEN 'june' THEN 6
                        WHEN 'july' THEN 7
                        WHEN 'august' THEN 8
                        WHEN 'september' THEN 9
                        WHEN 'october' THEN 10
                        WHEN 'november' THEN 11
                        WHEN 'december' THEN 12
                    END DESC
            """)

            results = conn.execute(query).mappings().all()

        all_rates = [dict(row) for row in results]

        # Normalize currency values
        for row in all_rates:
            row["user_currency"] = str(row.get("user_currency") or "").strip().upper()
            row["country"] = str(row.get("country") or "").strip().upper()
            row["selected_currency"] = str(row.get("selected_currency") or "").strip().upper()

        # Excel download: all historical months
        if request.args.get("export", "").lower() == "excel":
            df = pd.DataFrame(all_rates)

            output = BytesIO()

            with pd.ExcelWriter(output, engine="openpyxl") as writer:
                df.to_excel(
                    writer,
                    index=False,
                    sheet_name="Currency Rates"
                )

            output.seek(0)

            return send_file(
                output,
                as_attachment=True,
                download_name="Currency_Rates_History.xlsx",
                mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )

        
        # Frontend Excel export: return ALL historical records as JSON
        if request.args.get("history", "").lower() == "true":
            return jsonify({
                "currency_rates": all_rates,
                "message": "Complete currency rates history."
            }), 200

        # UI: current calendar month only
        from datetime import datetime

        current_month = datetime.now().strftime("%B").lower()
        current_year = datetime.now().year

        current_rates = [
            row for row in all_rates
            if str(row.get("month") or "").strip().lower() == current_month
            and int(row.get("year") or 0) == current_year
        ]

        return jsonify({
            "current_month": current_month,
            "current_year": current_year,
            "currency_rates": current_rates,
            "message": "Download Excel to view complete historical currency rates."
        }), 200

    except SQLAlchemyError as e:
        return jsonify({
            'error': 'Database error',
            'message': str(e)
        }), 500

    except Exception as e:
        return jsonify({
            'error': 'An error occurred while fetching currency rates',
            'message': str(e)
        }), 500


MONTHS = [
    "january", "february", "march", "april", "may", "june",
    "july", "august", "september", "october", "november", "december"
]

def get_previous_month(month: str, year: str):
    m = month.strip().lower()
    y = int(year)

    if m not in MONTHS:
        return None, None

    idx = MONTHS.index(m)
    if idx == 0:
        return "december", str(y - 1)
    return MONTHS[idx - 1], str(y)

def is_valid_product_name(name):
    if name is None:
        return False
    s = str(name).strip().lower()
    return s not in ("", "nan", "none", "null", "total")

def build_table_candidates(user_id, country, month, year):
    """Return [requested_table, fallback_prev_month_table] (fallback may be None)."""
    requested = f"skuwisemonthly_{user_id}_{country}_{month}{year}"
    pm, py = get_previous_month(month, year)
    fallback = f"skuwisemonthly_{user_id}_{country}_{pm}{py}" if pm and py else None
    return requested, fallback

def select_asp_query(asp_table):
    """Return a SQLAlchemy select query based on available ASP-like columns."""
    available_columns = set(asp_table.c.keys())
    metric_columns = [
        column
        for column in (
            'asp',
            'average_selling_price',
            'net_sales',
            'total_quantity',
            'net_units_sold',
            'quantity',
        )
        if column in available_columns
    ]
    if not metric_columns:
        return None

    return select(asp_table.c.product_name, *[asp_table.c[column] for column in metric_columns])


def normalize_asp_row(row):
    """Use a stored positive ASP or derive it from net sales and units."""
    row = dict(row)

    for field in ('asp', 'average_selling_price'):
        try:
            value = float(row.get(field) or 0)
        except (TypeError, ValueError):
            continue
        if value > 0:
            return {
                'product_name': row.get('product_name'),
                'asp': value,
            }

    try:
        net_sales = float(row.get('net_sales') or 0)
    except (TypeError, ValueError):
        net_sales = 0

    units = 0
    for field in ('total_quantity', 'net_units_sold', 'quantity'):
        try:
            units = float(row.get(field) or 0)
        except (TypeError, ValueError):
            continue
        if units > 0:
            break

    return {
        'product_name': row.get('product_name'),
        'asp': net_sales / units if net_sales > 0 and units > 0 else 0,
    }

@product_bp.route('/asp-data', methods=['GET'])
def get_asp_data():
    # ---------- AUTH ----------
    auth_header = request.headers.get('Authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        return jsonify({'error': 'Authorization token is missing or invalid'}), 401

    token = auth_header.split(' ')[1]
    try:
        payload, user_id, member_id = get_effective_user_id_from_token(token)
    except jwt.ExpiredSignatureError:
        return jsonify({'error': 'Token has expired'}), 401
    except jwt.InvalidTokenError:
        return jsonify({'error': 'Invalid token'}), 401

    # ---------- PARAMS ----------
    country = request.args.get('country', '').strip().lower()
    month = request.args.get('month', '').strip().lower()
    year = request.args.get('year', '').strip()

    if not all([country, month, year]):
        return jsonify({'error': 'Country, month, and year parameters are required'}), 400

    if month not in MONTHS:
        return jsonify({'error': 'Invalid month', 'allowed': MONTHS}), 400

    try:
        engine = user_engine
        inspector = inspect(engine)
        all_tables = set(inspector.get_table_names())

        # ---------- GLOBAL ----------
        if country == 'global':
            asp_data = []
            countries_to_try = ['uk', 'us', 'canada']

            for c in countries_to_try:
                requested, fallback = build_table_candidates(user_id, c, month, year)

                table_to_use = None
                if requested in all_tables:
                    table_to_use = requested
                elif fallback and fallback in all_tables:
                    table_to_use = fallback
                else:
                    continue

                metadata = MetaData()
                asp_table = Table(table_to_use, metadata, autoload_with=engine)

                query = select_asp_query(asp_table)
                if query is None:
                    continue

                with engine.connect() as conn:
                    results = conn.execute(query).mappings().all()

                for row in results:
                    row_dict = normalize_asp_row(row)
                    if not is_valid_product_name(row_dict.get("product_name")):
                        continue
                    row_dict['source_country'] = c
                    asp_data.append(row_dict)

            if not asp_data:
                return jsonify({
                    'error': 'No ASP data found for global view',
                    'details': f'No data available for {month} {year}'
                }), 404

            return jsonify(asp_data), 200

        # ---------- SINGLE COUNTRY ----------
        requested, fallback = build_table_candidates(user_id, country, month, year)

        if requested in all_tables:
            table_to_use = requested
        elif fallback and fallback in all_tables:
            table_to_use = fallback
        else:
            return jsonify({
                'error': f'ASP data table "{requested}" not found',
                'details': f'Also checked fallback "{fallback}"'
            }), 404

        metadata = MetaData()
        asp_table = Table(table_to_use, metadata, autoload_with=engine)

        query = select_asp_query(asp_table)
        if query is None:
            return jsonify({
                'error': 'Cannot determine ASP column',
                'available_columns': [c.name for c in asp_table.columns],
                'table_name': table_to_use
            }), 404

        with engine.connect() as conn:
            results = conn.execute(query).mappings().all()

        asp_data = []
        for r in results:
            d = normalize_asp_row(r)
            if not is_valid_product_name(d.get("product_name")):
                continue
            asp_data.append(d)

        return jsonify(asp_data), 200

    except SQLAlchemyError as e:
        return jsonify({'error': 'Database error', 'message': str(e)}), 500
    except Exception as e:
        return jsonify({'error': 'Unexpected error', 'message': str(e)}), 500

    

@product_bp.route('/skup', methods=['POST'])
def skup():
    auth_header = request.headers.get('Authorization')

    if not auth_header or not auth_header.startswith('Bearer '):
        return jsonify({'error': 'Authorization token is missing or invalid'}), 401

    token = auth_header.split(' ')[1]
    try:
        payload, user_id, member_id = get_effective_user_id_from_token(token)
    except jwt.ExpiredSignatureError:
        return jsonify({'error': 'Token has expired'}), 401
    except jwt.InvalidTokenError:
        return jsonify({'error': 'Invalid token'}), 401

    if 'file' not in request.files:
        return jsonify({'error': 'No file part'}), 400

    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'No selected file'}), 400

    filename = secure_filename(file.filename)
    file_path = os.path.join(filename)
    file.save(file_path)

    # =========================
    # 1️⃣ UPDATE EXCEL FILE
    # =========================
    try:
        sheet_name = "SKU Information Tab"
        df = pd.read_excel(file_path, sheet_name=sheet_name)

        if "Local Stock" not in df.columns:
            df["Local Stock"] = 0

        if "In Transit Units" not in df.columns:
            df["In Transit Units"] = 0

        with pd.ExcelWriter(file_path, engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
            df.to_excel(writer, sheet_name=sheet_name, index=False)

    except Exception as e:
        return jsonify({'error': f'Excel update failed: {str(e)}'}), 500

    # =========================
    # 2️⃣ UPDATE DATABASE TABLE
    # =========================
    try:
        table_name = f"sku_{user_id}_data_table"

        inspector = inspect(user_engine)

        if table_name not in inspector.get_table_names():
            return jsonify({'error': f'Table {table_name} not found'}), 404

        with user_engine.begin() as conn:
            existing_columns = {col["name"] for col in inspect(conn).get_columns(table_name)}

            if "last_upload_sku_file" not in existing_columns:
                conn.execute(text(f'''
                    ALTER TABLE "{table_name}"
                    ADD COLUMN last_upload_sku_file TIMESTAMPTZ;
                '''))

            # Update the timestamp on all rows to record this upload event
            conn.execute(text(f'''
                UPDATE "{table_name}"
                SET last_upload_sku_file = :upload_ts;
            '''), {"upload_ts": datetime.now(timezone.utc)})

    except SQLAlchemyError as e:
        return jsonify({'error': 'Database error', 'message': str(e)}), 500
    except Exception as e:
        return jsonify({'error': str(e)}), 500

    return jsonify({
        "success": True,
        "message": "File uploaded and stock columns ensured"
    }), 200




@product_bp.route('/updatePrices', methods=['POST'])
def update_prices():
    # Authorization
    auth_header = request.headers.get('Authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        return jsonify({'error': 'Authorization token is missing or invalid'}), 401

    token = auth_header.split(' ')[1]
    try:
        payload, user_id, member_id = get_effective_user_id_from_token(token)
    except jwt.ExpiredSignatureError:
        return jsonify({'error': 'Token has expired'}), 401
    except jwt.InvalidTokenError:
        return jsonify({'error': 'Invalid token'}), 401

    # Parse update payload
    data = request.get_json() or {}
    rows = data.get('rows', {})

    if not rows:
        return jsonify({'error': 'No rows provided to update'}), 400

    # DB setup
    Session = sessionmaker(bind=user_engine)
    user_session = Session()

    sku_table_name = f"sku_{user_id}_data_table"
    sku_data_table = Table(sku_table_name, MetaData(), autoload_with=user_engine)

    failed_products = []
    updated_products = []

    try:
        # Fetch all existing product_names for validation
        existing_products_result = user_session.execute(
            select(sku_data_table.c.product_name)
        ).fetchall()
        existing_products = set(row[0] for row in existing_products_result)

        for product_name, updates in rows.items():
            if product_name not in existing_products:
                failed_products.append(product_name)
                continue

            if not isinstance(updates, dict):
                failed_products.append(product_name)
                continue

            update_data = {}

            # price (optional)
            if "price" in updates and updates["price"] is not None:
                try:
                    update_data["price"] = float(updates["price"])
                except Exception:
                    failed_products.append(product_name)
                    continue

            # Marketplace-specific product master fields (optional).
            for field in ('asin_uk','asin_us','product_barcode_uk','product_barcode_us'):
                if field in updates:
                    if field not in sku_data_table.c:
                        failed_products.append(product_name)
                        update_data = {}
                        break
                    update_data[field] = (str(updates[field]).strip() if updates[field] is not None else None)
            for field in ('local_stock_uk','local_stock_us'):
                if field in updates:
                    if field not in sku_data_table.c:
                        failed_products.append(product_name)
                        update_data = {}
                        break
                    try:
                        update_data[field] = int(updates[field]) if updates[field] is not None else 0
                    except (ValueError, TypeError):
                        failed_products.append(product_name)
                        update_data = {}
                        break

            # If nothing valid to update
            if not update_data:
                failed_products.append(product_name)
                continue

            result = user_session.execute(
                sku_data_table.update()
                .where(sku_data_table.c.product_name == product_name)
                .values(**update_data)
            )

            if result.rowcount == 0:
                failed_products.append(product_name)
            else:
                updated_products.append(product_name)

        user_session.commit()

        # Return updated table
        select_stmt = select(sku_data_table).order_by(sku_data_table.c.id.asc())
        result = user_session.execute(select_stmt)
        updated_data = [dict(row._mapping) for row in result]

        return jsonify({
            'message': 'Update completed (price + stock)',
            'updated_products': updated_products,
            'not_updated_products': failed_products,
            'data': updated_data
        }), 200

    except Exception as e:
        user_session.rollback()
        return jsonify({'error': f'Error updating rows: {str(e)}'}), 500
    finally:
        user_session.close()



@product_bp.route('/sku-status', methods=['PATCH'])
def update_sku_product_status():
    auth_header = request.headers.get('Authorization', '')
    if not auth_header.startswith('Bearer '):
        return jsonify({'error': 'Authorization token is missing or invalid'}), 401

    try:
        _, user_id, _ = get_effective_user_id_from_token(auth_header.split(' ', 1)[1])
    except jwt.ExpiredSignatureError:
        return jsonify({'error': 'Token has expired'}), 401
    except jwt.InvalidTokenError:
        return jsonify({'error': 'Invalid token'}), 401

    data = request.get_json(silent=True) or {}
    status = {'active': 'Active', 'discontinued': 'Discontinued',
              'discontinuous': 'Discontinued'}.get(
                  str(data.get('product_status') or '').strip().lower())
    if not status:
        return jsonify({'error': 'Product status must be Active or Discontinued'}), 400

    country = str(data.get('country') or request.args.get('country') or '').strip().lower()
    country = {'gb': 'uk', 'usa': 'us', 'amazon us': 'us',
               'amazon uk': 'uk', 'ca': 'canada'}.get(country, country)
    status_columns = {'us': 'product_status_us', 'uk': 'product_status_uk',
                      'canada': 'product_status_canada'}
    sku_columns = {'us': 'sku_us', 'uk': 'sku_uk', 'canada': 'sku_canada'}
    if country not in status_columns:
        return jsonify({'error': 'country is required (us, uk, canada)'}), 400

    raw_id = data.get('id')
    record_id = None
    if raw_id not in (None, ''):
        try:
            record_id = int(raw_id)
        except (TypeError, ValueError):
            return jsonify({'error': 'Invalid SKU record id'}), 400

    # The UI can use a country-specific SKU when it has not received a DB id.
    sku_value = str(data.get('sku') or data.get(sku_columns[country]) or '').strip()
    if not sku_value and record_id is None:
        return jsonify({'error': 'Provide a SKU record id or the selected country SKU'}), 400

    table_name = f'sku_{user_id}_data_table'
    status_column, sku_column = status_columns[country], sku_columns[country]
    try:
        with user_engine.begin() as conn:
            inspector = inspect(conn)
            if not inspector.has_table(table_name):
                return jsonify({'error': 'SKU table was not found'}), 404
            columns = {col['name'] for col in inspector.get_columns(table_name)}
            if sku_column not in columns:
                return jsonify({'error': f'{country.upper()} SKU column not found'}), 400
            conn.execute(text(
                f'ALTER TABLE "{table_name}" ADD COLUMN IF NOT EXISTS '
                f"\"{status_column}\" VARCHAR(32) DEFAULT 'Active'"
            ))

            if sku_value:
                # Match the actual country SKU first; a UI row index is not a DB id.
                matches = conn.execute(text(
                    f'SELECT id FROM "{table_name}" WHERE user_id = :user_id '
                    f'AND UPPER(TRIM("{sku_column}")) = UPPER(:sku) LIMIT 2'
                ), {'user_id': user_id, 'sku': sku_value}).scalars().all()
                if not matches:
                    return jsonify({'error': f'{country.upper()} SKU not found',
                                    'sku': sku_value}), 404
                if len(matches) > 1:
                    return jsonify({'error': 'Multiple SKU records match; use a database id'}), 409
                record_id = matches[0]

            result = conn.execute(text(
                f'UPDATE "{table_name}" SET "{status_column}" = :status '
                f'WHERE id = :id AND user_id = :user_id '
                f"AND NULLIF(TRIM(\"{sku_column}\"), '') IS NOT NULL"
            ), {'status': status, 'id': record_id, 'user_id': user_id})
            if result.rowcount != 1:
                return jsonify({'error': f'SKU record not found or no {country.upper()} SKU assigned'}), 404

        return jsonify({'success': True, 'id': record_id, 'country': country,
                        'product_status': status, status_column: status}), 200
    except SQLAlchemyError as exc:
        return jsonify({'error': 'Database error', 'message': str(exc)}), 500


@product_bp.route('/skuprice', methods=['GET'])
def skuprice():
    auth_header = request.headers.get('Authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        return jsonify({'error': 'Authorization token is missing or invalid'}), 401

    token = auth_header.split(' ')[1]
    try:
        payload, user_id, member_id = get_effective_user_id_from_token(token)
    except jwt.ExpiredSignatureError:
        return jsonify({'error': 'Token has expired'}), 401
    except jwt.InvalidTokenError:
        return jsonify({'error': 'Invalid token'}), 401



    table_name = f"sku_{user_id}_data_table"

    try:
        # Connect to PostgreSQL
        inspector = inspect(user_engine)

        # Check if the table exists
        if table_name not in inspector.get_table_names():
            return jsonify({'error': f'Table "{table_name}" not found'}), 404

        # Load table metadata
        metadata = MetaData()
        sku_data_table = Table(table_name, metadata, autoload_with=user_engine)

        # Query the table
        with user_engine.connect() as conn:
            query = sku_data_table.select()
            results = conn.execute(query).mappings().all()

        # Convert to dict and return
        result_dicts = [dict(row) for row in results]
        return jsonify(result_dicts), 200

    except SQLAlchemyError as e:
        return jsonify({'error': 'Database error', 'message': str(e)}), 500
    except Exception as e:
        return jsonify({'error': 'An error occurred while fetching SKU data', 'message': str(e)}), 500



@product_bp.route('/get_error_file/<string:country>/<string:month>/<string:year>', methods=['GET'])
def get_error_file(country, month, year):
    auth_header = request.headers.get('Authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        return jsonify({'error': 'Authorization token is missing or invalid'}), 401

    token = auth_header.split(' ')[1]
    try:
        payload, user_id, member_id = get_effective_user_id_from_token(token)
    except jwt.ExpiredSignatureError:
        return jsonify({'error': 'Token has expired'}), 401
    except jwt.InvalidTokenError:
        return jsonify({'error': 'Invalid token'}), 401

    # Construct the filename for the error file (which is actually inventory_forecast)
    error_filename = f"error_file_{user_id}{country}{month}_{year}.xlsx"
    error_file_path = os.path.join( error_filename)

    # Check if the file exists
    if not os.path.exists(error_file_path):
        return jsonify({'error': 'Error file not found'}), 404

    try:
        # Send the existing forecast file as a download
        return send_file(error_file_path, as_attachment=True)

    except Exception as e:
        return jsonify({'error': 'An error occurred while sending the error file'}), 500
   

@product_bp.route('/get_consolidated_table_name/<string:country_name>', methods=['GET'])
def get_consolidated_table_name(country_name):
    # Authorization
    auth_header = request.headers.get('Authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        return jsonify({'error': 'Authorization token is missing or invalid'}), 401

    token = auth_header.split(' ')[1]
    try:
        payload, user_id, member_id = get_effective_user_id_from_token(token)
    except jwt.ExpiredSignatureError:
        return jsonify({'error': 'Token has expired'}), 401
    except jwt.InvalidTokenError:
        return jsonify({'error': 'Invalid token'}), 401

    # Sanitize country name to form a safe table name
    def sanitize_identifier(identifier):
        return re.sub(r'\W|^(?=\d)', '_', identifier)

    safe_country_name = sanitize_identifier(country_name)
    consolidated_table_name = f"user_{user_id}_{safe_country_name}_merge_data_of_all_months"

    try:
        # Create engine
        inspector = inspect(user_engine)

        # Check if the table exists
        if consolidated_table_name not in inspector.get_table_names():
            return jsonify({'error': f'Table "{consolidated_table_name}" not found for user {user_id}'}), 404

        # Define and query the table
        metadata = MetaData()
        consolidated_table = Table(consolidated_table_name, metadata, autoload_with=user_engine)

        with user_engine.connect() as conn:
            results = conn.execute(consolidated_table.select()).mappings().all()

        result_dicts = [dict(row) for row in results]
        return jsonify(result_dicts), 200

    except SQLAlchemyError as e:
        return jsonify({'error': 'Database error', 'message': str(e)}), 500
    except Exception as e:
        return jsonify({'error': 'An unexpected error occurred', 'message': str(e)}), 500

def resolve_country(country, currency):
    country = (country or "").lower()
    currency = (currency or "").lower()

    # 1. If country = global
    if country == "global":
        if currency == "usd":
            return "global"
        elif currency == "inr":
            return "global_inr"
        elif currency == "gbp":
            return "global_gbp"
        elif currency == "cad":
            return "global_cad"
        else:
            return "global"  # default fallback

    # 2. If country = uk
    if country == "uk":
        if currency == "usd":
            return "uk_usd"
        else:
            return "uk"  # default for all other currencies

    # 3. Default (no special logic)
    return country

# @product_bp.route('/skutableprofit/<string:skuwise_file_name>', methods=['GET'])
# def skutableprofit(skuwise_file_name):
#     auth_header = request.headers.get('Authorization')
#     if not auth_header or not auth_header.startswith('Bearer '):
#         return jsonify({'error': 'Authorization token is missing or invalid'}), 401

#     token = auth_header.split(' ')[1]
#     try:
#         payload, user_id, member_id = get_effective_user_id_from_token(token)
#     except jwt.ExpiredSignatureError:
#         return jsonify({'error': 'Token has expired'}), 401
#     except jwt.InvalidTokenError:
#         return jsonify({'error': 'Invalid token'}), 401

#     try:
#         engine = user_engine

#         country_param = request.args.get('country', '')
#         currency_param = (request.args.get('homeCurrency') or '').lower()

#         country = resolve_country(country_param, currency_param)
#         month = (request.args.get('month') or '').strip().lower()
#         year = (request.args.get('year') or '').strip()

#         # Current table name
#         if country and month and year:
#             table_name = f"skuwisemonthly_{user_id}_{country}_{month}{year}".lower()
#         else:
#             table_name = skuwise_file_name

#         metadata = MetaData(schema='public')

#         def _fetch_as_dicts(tbl_name):
#             user_specific_table = Table(tbl_name, metadata, autoload_with=engine)
#             with engine.connect() as conn:
#                 results = conn.execute(
#                     select(*user_specific_table.columns)
#                 ).mappings().all()

#             return [_normalize_sku_row(dict(row)) for row in results]

#         try:
#             current_data = _fetch_as_dicts(table_name)
#         except Exception:
#             if table_name != skuwise_file_name:
#                 try:
#                     table_name = skuwise_file_name
#                     current_data = _fetch_as_dicts(table_name)
#                 except Exception:
#                     return jsonify({
#                         'error': f"Table '{table_name}' or '{skuwise_file_name}' not found for user {user_id}"
#                     }), 404
#             else:
#                 return jsonify({
#                     'error': f"Table '{table_name}' not found for user {user_id}"
#                 }), 404

#         previous_table_name = None
#         previous_data = []

#         if country and month and year:
#             prev_month, prev_year = get_previous_month(month, year)

#             if prev_month and prev_year:
#                 previous_table_name = f"skuwisemonthly_{user_id}_{country}_{prev_month}{prev_year}".lower()

#                 try:
#                     previous_data = _fetch_as_dicts(previous_table_name)
#                 except Exception:
#                     previous_data = []

#         return jsonify({
#             "current_table_name": table_name,
#             "current_data": current_data,
#             "previous_table_name": previous_table_name,
#             "previous_data": previous_data
#         }), 200

#     except Exception as e:
#         return jsonify({
#             'error': 'An unexpected error occurred',
#             'message': str(e)
#         }), 500

def build_skuwise_table_name(user_id, country, month, year):
    month = month.strip().lower()
    year = str(year).strip()

    if country == "global":
        return f"skuwisemonthly_{user_id}_global_{month}{year}_table".lower()

    return f"skuwisemonthly_{user_id}_{country}_{month}{year}".lower()

def _ads_key(row):
    sku = str(row.get("sku") or "").strip().lower()
    product_name = str(row.get("product_name") or "").strip().lower()

    # Prefer SKU because product names can sometimes vary slightly
    if sku and sku != "total":
        return ("sku", sku)

    if product_name and product_name != "total":
        return ("product_name", product_name)

    return None

def safe_float(value):
    try:
        if value is None:
            return 0.0
        return float(value)
    except Exception:
        return 0.0

def _get_ads_spend(ads_dict):
    return safe_float(
        ads_dict.get("ads_spend")
        if ads_dict.get("ads_spend") is not None
        else ads_dict.get("ads_spend_raw")
        if ads_dict.get("ads_spend_raw") is not None
        else ads_dict.get("product_spend")
    )

def _get_product_spend(ads_dict):
    return safe_float(ads_dict.get("product_spend"))


def _get_display_spend(ads_dict):
    return safe_float(ads_dict.get("display_spend"))

def _get_dealsvouchar_ads(row):
    return abs(safe_float(
        row.get("dealsvouchar_ads")
        if row.get("dealsvouchar_ads") is not None
        else row.get("dealsvoucher_ads")
        if row.get("dealsvoucher_ads") is not None
        else row.get("deals_voucher_ads")
    ))

@product_bp.route('/skutableprofit', methods=['GET'])
def skutableprofit():
    promotions_placement = _read_promotions_placement()
    if promotions_placement is None:
        return jsonify({"error": "Invalid promotionsPlacement"}), 400
    amazon_fees_placement = _read_amazon_fees_placement()
    if amazon_fees_placement is None:
        return jsonify({"error": "Invalid amazonFeesPlacement"}), 400
    allow_after_cm1 = (
        request.args.get("allowAfterCm1", "").strip().lower() == "true"
    )

    auth_header = request.headers.get('Authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        return jsonify({'error': 'Authorization token is missing or invalid'}), 401

    token = auth_header.split(' ')[1]

    try:
        payload, user_id, member_id = get_effective_user_id_from_token(token)
    except jwt.ExpiredSignatureError:
        return jsonify({'error': 'Token has expired'}), 401
    except jwt.InvalidTokenError:
        return jsonify({'error': 'Invalid token'}), 401

    try:
        engine = user_engine

        country_param = request.args.get('country', '')
        currency_param = (request.args.get('homeCurrency') or '').lower()

        country = resolve_country(country_param, currency_param)
        month = (request.args.get('month') or '').strip().lower()
        year = (request.args.get('year') or '').strip()
        benchmark_only = (request.args.get('benchmarkOnly') or '').strip().lower() in {'1', 'true', 'yes'}

        if not country or not month or not year:
            return jsonify({
                'error': 'country, month, and year are required'
            }), 400

        # Main SKU monthly table:
        # example: skuwisemonthly_2_uk_may2026
        table_name = build_skuwise_table_name(user_id, country, month, year)

        # Ads table:
        # Global monthly SKU-wise tables already contain the merged UK/US ads
        # fields, so use the generated global table itself as the ads source.
        requested_ads_table_name = (
            table_name
            if country == "global"
            else f"skuwisemonthly_{user_id}_{country}_{month}{year}".lower()
        )

        inspector = inspect(engine)
        existing_tables = set(inspector.get_table_names(schema="public"))

        # The frontend can request every month in the selected year.
        # Future or not-yet-generated monthly tables are valid "no data" cases,
        # not server errors. Return a stable empty response instead of 500.
        if table_name not in existing_tables:
            prev_month, prev_year = get_previous_month(month, year)
            previous_table_name = (
                build_skuwise_table_name(user_id, country, prev_month, prev_year)
                if prev_month and prev_year
                else None
            )

            return jsonify({
                "success": True,
                "available": False,
                "message": "No SKU profit data is available for the selected month.",
                "current_table_name": table_name,
                "current_ads_table_name": None,
                "requested_ads_table_name": requested_ads_table_name,
                "current_data": [],
                "previous_table_name": previous_table_name,
                "previous_ads_table_name": None,
                "previous_data": [],
                "card_metrics": build_pnl_card_metrics([], []),
                **({
                    "current_data_uk": [],
                    "current_data_us": [],
                    "previous_data_uk": [],
                    "previous_data_us": [],
                    "card_metrics_uk": build_pnl_card_metrics([], []),
                    "card_metrics_us": build_pnl_card_metrics([], []),
                } if country == "global" else {}),
            }), 200

        ads_table_name = requested_ads_table_name if requested_ads_table_name in existing_tables else None

        metadata = MetaData(schema='public')

        def safe_divide(numerator, denominator):
            numerator = safe_float(numerator)
            denominator = safe_float(denominator)
            if denominator == 0:
                return 0.0
            return numerator / denominator

        def _fetch_profit_data(main_tbl_name, ads_tbl_name=None):
            main_table = Table(main_tbl_name, metadata, autoload_with=engine)

            with engine.connect() as conn:
                main_rows = conn.execute(
                    select(*main_table.columns)
                ).mappings().all()

                ads_rows = []

                if ads_tbl_name:
                    ads_table = Table(ads_tbl_name, metadata, autoload_with=engine)
                    ads_rows = conn.execute(
                        select(*ads_table.columns)
                    ).mappings().all()

            # Build ads lookup by SKU and product_name
            ads_by_sku = {}
            ads_by_product_name = {}

            for ads_row in ads_rows:
                ads_dict = _normalize_sku_row(dict(ads_row))

                sku = str(ads_dict.get("sku") or "").strip().lower()
                product_name = str(ads_dict.get("product_name") or "").strip().lower()

                if sku and sku != "total":
                    ads_by_sku[sku] = ads_dict

                if product_name and product_name != "total":
                    ads_by_product_name[product_name] = ads_dict

            final_data = []

            for row in main_rows:
                row_dict = _normalize_sku_row(dict(row))

                sku = str(row_dict.get("sku") or "").strip().lower()
                product_name = str(row_dict.get("product_name") or "").strip().lower()

                ads_spend = 0.0
                product_spend = 0.0
                display_spend = 0.0


                # Match by SKU first, then product_name
                ads_dict = None

                if sku and sku != "total":
                    ads_dict = ads_by_sku.get(sku)

                if not ads_dict and product_name and product_name != "total":
                    ads_dict = ads_by_product_name.get(product_name)

                if ads_dict:
                    ads_spend = _get_ads_spend(ads_dict)
                    product_spend = _get_product_spend(ads_dict)
                    display_spend = _get_display_spend(ads_dict)

                profit = safe_float(row_dict.get("profit"))
                net_sales = safe_float(row_dict.get("net_sales"))
                total_quantity = safe_float(row_dict.get("total_quantity"))

                cm2_profit = profit - ads_spend
                acos = safe_divide(ads_spend, net_sales) * 100
                cm2_profit_per = safe_divide(cm2_profit, net_sales) * 100
                cm2_profit_per_unit = safe_divide(cm2_profit, total_quantity)

                row_dict["ads_spend"] = round(ads_spend, 2)
                row_dict["product_spend"] = round(product_spend, 2)
                row_dict["display_spend"] = round(display_spend, 2)
                row_dict["cm2_profit"] = round(cm2_profit, 2)
                row_dict["acos"] = round(acos, 2)
                row_dict["cm2_profit_per"] = round(cm2_profit_per, 2)
                row_dict["cm2_profit_per_unit"] = round(cm2_profit_per_unit, 2)

                final_data.append(row_dict)

            brand_spend_total = 0.0
            dealsvouchar_ads_total = 0.0
            total_ads_spend = 0.0
            product_spend_total = 0.0
            display_spend_total = 0.0
            advertising_total = 0.0
            advertising_total_final = 0.0

            if ads_rows:
                def get_total_ads_row(ads_rows):
                    for ads_row in ads_rows:
                        ads_dict = _normalize_sku_row(dict(ads_row))

                        sku = str(ads_dict.get("sku") or "").strip().lower()
                        product_name = str(ads_dict.get("product_name") or "").strip().lower()

                        if sku == "total" or product_name == "total":
                            return ads_dict

                    return _normalize_sku_row(dict(ads_rows[-1]))

                ads_total_row = get_total_ads_row(ads_rows)

                brand_spend_total = safe_float(ads_total_row.get("brand_spend"))

                dealsvouchar_ads_total = _get_dealsvouchar_ads(ads_total_row)

                sku_ads_total = sum(
                    safe_float(row.get("ads_spend"))
                    for row in final_data
                    if str(row.get("sku") or "").strip().lower() != "total"
                    and str(row.get("product_name") or "").strip().lower() != "total"
                )

                total_ads_spend = _get_ads_spend(ads_total_row)
                product_spend_total = _get_product_spend(ads_total_row)
                display_spend_total = _get_display_spend(ads_total_row)

                if total_ads_spend == 0:
                    total_ads_spend = sku_ads_total

                advertising_total = brand_spend_total + dealsvouchar_ads_total
                advertising_total_final = advertising_total + total_ads_spend


            # This part must run even when ads_rows is empty
            for row_dict in final_data:
                sku = str(row_dict.get("sku") or "").strip().lower()
                product_name = str(row_dict.get("product_name") or "").strip().lower()

                if sku == "total" or product_name == "total":
                    profit = safe_float(row_dict.get("profit"))
                    net_sales = safe_float(row_dict.get("net_sales"))
                    total_quantity = safe_float(row_dict.get("total_quantity"))
                    platform_fee = safe_float(row_dict.get("platform_fee"))
                    shipment_fees = safe_float(row_dict.get("shipment_fees"))

                    main_dealsvouchar_ads = _get_dealsvouchar_ads(row_dict)

                    if main_dealsvouchar_ads != 0:
                        dealsvouchar_ads_total = main_dealsvouchar_ads

                    if ads_tbl_name:
                        cm2_profit = profit - total_ads_spend
                        ads_for_acos = total_ads_spend

                        platform_fee_abs = abs(safe_float(row_dict.get("platform_fee")))

                        cm2_profit_total = (
                            cm2_profit
                            - advertising_total
                            - platform_fee_abs
                        )
                    else:
                        main_visible_ads = abs(safe_float(row_dict.get("visible_ads")))
                        main_dealsvouchar_ads = _get_dealsvouchar_ads(row_dict)
                        main_brand_spend = abs(safe_float(row_dict.get("brand_spend")))

                        advertising_total_from_main = safe_float(row_dict.get("advertising_total"))

                        if advertising_total_from_main == 0:
                            advertising_total_from_main = (
                                main_visible_ads
                                + main_dealsvouchar_ads
                                + main_brand_spend
                            )

                        platform_fee_abs = abs(safe_float(row_dict.get("platform_fee")))
                        shipment_fees_abs = abs(safe_float(row_dict.get("shipment_fees")))

                        cm2_profit = (
                            profit
                            - advertising_total_from_main
                            - platform_fee_abs
                            - shipment_fees_abs
                        )

                        ads_for_acos = advertising_total_from_main
                        cm2_profit_total = cm2_profit

                        brand_spend_total = main_brand_spend
                        dealsvouchar_ads_total = main_dealsvouchar_ads
                        advertising_total = advertising_total_from_main
                        advertising_total_final = advertising_total_from_main

                    acos = safe_divide(ads_for_acos, net_sales) * 100

                    # OLD logic - do not change this
                    cm2_profit_per = safe_divide(cm2_profit, net_sales) * 100
                    cm2_profit_per_unit = safe_divide(cm2_profit, total_quantity)

                    # NEW logic only for cm2_margins when ads table exists
                    if ads_tbl_name:
                        cm2_margins_value = safe_divide(cm2_profit_total, net_sales) * 100
                    else:
                        cm2_margins_value = safe_float(row_dict.get("cm2_margins"))

                    row_dict["ads_spend"] = round(ads_for_acos, 2)
                    row_dict["product_spend"] = round(product_spend_total, 2)
                    row_dict["display_spend"] = round(display_spend_total, 2)
                    row_dict["cm2_profit"] = round(cm2_profit, 2)
                    row_dict["acos"] = round(acos, 2)
                    row_dict["cm2_profit_per"] = round(cm2_profit_per, 2)
                    row_dict["cm2_profit_per_unit"] = round(cm2_profit_per_unit, 2)

                    if ads_tbl_name:
                        row_dict["cm2_margins"] = round(cm2_margins_value, 2)
                        row_dict["cm2_profit_percentage"] = round(cm2_margins_value, 2)

                        rembursement_fee = safe_float(row_dict.get("rembursement_fee"))
                        existing_reimbursement_vs_sales = safe_float(
                            row_dict.get("reimbursement_vs_sales")
                        )
                        existing_reimbursement_vs_cm2 = safe_float(
                            row_dict.get("rembursment_vs_cm2_margins")
                        )

                        if existing_reimbursement_vs_sales == 0:
                            row_dict["reimbursement_vs_sales"] = round(
                                safe_divide(rembursement_fee, net_sales) * 100,
                                2
                            )

                        if existing_reimbursement_vs_cm2 == 0:
                            row_dict["rembursment_vs_cm2_margins"] = round(
                                safe_divide(rembursement_fee, cm2_profit_total) * 100,
                                2
                            )

                    row_dict["brand_spend"] = round(brand_spend_total, 2)
                    row_dict["dealsvouchar_ads"] = round(dealsvouchar_ads_total, 2)

                    if ads_tbl_name:
                        row_dict["advertising_total"] = round(advertising_total, 2)
                        row_dict["advertising_total_final"] = round(advertising_total_final, 2)
                    else:
                        row_dict["advertising_total"] = round(ads_for_acos, 2)
                        row_dict["advertising_total_final"] = round(ads_for_acos, 2)

                    row_dict["cm2_profit_total"] = round(cm2_profit_total, 2)

                    break

            return add_per_unit_fields(final_data)

        def _add_product_status(rows, country_override=None):
            if not rows:
                return rows

            sku_table_name = f"sku_{user_id}_data_table"
            inspector = inspect(engine)
            if not inspector.has_table(sku_table_name, schema="public"):
                return rows

            available_columns = {
                column["name"]
                for column in inspector.get_columns(sku_table_name, schema="public")
            }
            marketplace_fields = {
                "uk": ("sku_uk", "product_status_uk"),
                "us": ("sku_us", "product_status_us"),
                "canada": ("sku_canada", "product_status_canada"),
            }
            country_key = str(country_override or country or "").strip().lower().split("_", 1)[0]
            if country_key == "ca":
                country_key = "canada"

            if country_key == "global":
                selected_pairs = list(marketplace_fields.values())
            elif country_key in marketplace_fields:
                selected_pairs = [marketplace_fields[country_key]]
            else:
                selected_pairs = []

            selected_pairs = [
                (sku_column, status_column)
                for sku_column, status_column in selected_pairs
                if sku_column in available_columns and status_column in available_columns
            ]
            if not selected_pairs:
                return rows

            sku_table = Table(sku_table_name, metadata, autoload_with=engine)
            selected_columns = [
                sku_table.c[column_name]
                for column_name in ["product_name"]
                if column_name in available_columns
            ]
            for sku_column, status_column in selected_pairs:
                selected_columns.extend(
                    [sku_table.c[sku_column], sku_table.c[status_column]]
                )

            with engine.connect() as conn:
                sku_rows = conn.execute(
                    select(*selected_columns)
                ).mappings().all()

            statuses_by_sku = {}
            statuses_by_name = {}

            def record_status(status_map, key, status):
                key = str(key or "").strip().lower()
                if not key:
                    return
                normalized_status = str(status or "").strip().lower()
                if normalized_status in ("discontinued", "discontinuous"):
                    status_map[key] = "Discontinued"
                elif normalized_status == "active":
                    status_map.setdefault(key, "Active")

            for sku_row in sku_rows:
                product_name = sku_row.get("product_name")
                for sku_column, status_column in selected_pairs:
                    status = sku_row.get(status_column)
                    record_status(statuses_by_sku, sku_row.get(sku_column), status)
                    record_status(statuses_by_name, product_name, status)

            rows_with_status = []
            for row in rows:
                row_with_status = dict(row)
                sku = row_with_status.get("sku")
                product_name = row_with_status.get("product_name")
                status = (
                    statuses_by_sku.get(str(sku or "").strip().lower())
                    or statuses_by_name.get(str(product_name or "").strip().lower())
                )
                if status:
                    row_with_status["product_status"] = status
                rows_with_status.append(row_with_status)

            return rows_with_status

        try:
            current_data = _fetch_profit_data(table_name, ads_table_name)
            if not benchmark_only:
                try:
                    persist_per_unit_fields(engine, table_name, current_data)
                except Exception:
                    pass
            current_data = _add_product_status(current_data)
            has_current_cm2_data = _has_cm2_data(current_data)
            current_placement = promotions_placement
            if (
                current_placement == "after_cm1"
                and not has_current_cm2_data
                and not allow_after_cm1
            ):
                current_placement = "before_cm1"
            current_data = _apply_promotions_placement(current_data, current_placement)
            current_amazon_fees_placement = amazon_fees_placement
            if (
                current_amazon_fees_placement == "after_cm1"
                and not has_current_cm2_data
                and not allow_after_cm1
            ):
                current_amazon_fees_placement = "before_cm1"
            current_data = _apply_amazon_fees_placement(
                current_data, current_amazon_fees_placement
            )
        except Exception as e:
            return jsonify({
                "error": "Failed to calculate SKU profit data",
                "current_table_name": table_name,
                "ads_table_name": ads_table_name,
                "message": str(e)
            }), 500

        previous_table_name = None
        previous_ads_table_name = None
        previous_data = []

        prev_month, prev_year = get_previous_month(month, year)

        if prev_month and prev_year and not benchmark_only:
            previous_table_name = build_skuwise_table_name(
                user_id,
                country,
                prev_month,
                prev_year
            )

            # Do NOT load previous ads table
            previous_ads_table_name = None

            try:
                previous_data = _fetch_profit_data(
                    previous_table_name,
                    None
                )
                try:
                    persist_per_unit_fields(engine, previous_table_name, previous_data)
                except Exception:
                    pass
            except Exception:
                previous_data = []
            has_previous_cm2_data = _has_cm2_data(previous_data)
            previous_placement = promotions_placement
            if (
                previous_placement == "after_cm1"
                and not has_previous_cm2_data
                and not allow_after_cm1
            ):
                previous_placement = "before_cm1"
            previous_data = _apply_promotions_placement(previous_data, previous_placement)
            previous_amazon_fees_placement = amazon_fees_placement
            if (
                previous_amazon_fees_placement == "after_cm1"
                and not has_previous_cm2_data
                and not allow_after_cm1
            ):
                previous_amazon_fees_placement = "before_cm1"
            previous_data = _apply_amazon_fees_placement(
                previous_data, previous_amazon_fees_placement
            )

        response_payload = {
            "current_table_name": table_name,
            "current_ads_table_name": ads_table_name,
            "requested_ads_table_name": requested_ads_table_name,
            "current_data": current_data,
            "previous_table_name": previous_table_name,
            "previous_ads_table_name": previous_ads_table_name,
            "previous_data": previous_data,
            "card_metrics": build_pnl_card_metrics(current_data, previous_data),
        }

        # Add marketplace-specific views to Global responses without changing
        # the existing combined fields or any country-specific response shape.
        if country == "global":
            prev_month, prev_year = get_previous_month(month, year)

            def load_country_profit_rows(country_key, source_month, source_year, *, include_ads):
                if not source_month or not source_year:
                    return []
                source_table = build_skuwise_table_name(
                    user_id, country_key, source_month, source_year
                )
                if source_table not in existing_tables:
                    return []
                source_ads_table = source_table if include_ads and source_table in existing_tables else None
                rows = _fetch_profit_data(source_table, source_ads_table)
                rows = _add_product_status(rows, country_override=country_key)
                has_cm2 = _has_cm2_data(rows)
                promotion_stage = promotions_placement
                if promotion_stage == "after_cm1" and not has_cm2 and not allow_after_cm1:
                    promotion_stage = "before_cm1"
                rows = _apply_promotions_placement(rows, promotion_stage)
                fees_stage = amazon_fees_placement
                if fees_stage == "after_cm1" and not has_cm2 and not allow_after_cm1:
                    fees_stage = "before_cm1"
                return _apply_amazon_fees_placement(rows, fees_stage)

            current_data_uk = load_country_profit_rows("uk", month, year, include_ads=True)
            current_data_us = load_country_profit_rows("us", month, year, include_ads=True)
            previous_data_uk = [] if benchmark_only else load_country_profit_rows(
                "uk", prev_month, prev_year, include_ads=False
            )
            previous_data_us = [] if benchmark_only else load_country_profit_rows(
                "us", prev_month, prev_year, include_ads=False
            )
            response_payload.update({
                "current_data_uk": current_data_uk,
                "current_data_us": current_data_us,
                "previous_data_uk": previous_data_uk,
                "previous_data_us": previous_data_us,
                "card_metrics_uk": build_pnl_card_metrics(current_data_uk, previous_data_uk),
                "card_metrics_us": build_pnl_card_metrics(current_data_us, previous_data_us),
            })

        return jsonify(response_payload), 200

    except Exception as e:
        return jsonify({
            'error': 'An unexpected error occurred',
            'message': str(e)
        }), 500
    


@product_bp.route('/get_table_data/<string:file_name>', methods=['GET'])
@_serialize_expense_reconciliation_request
def get_table_data(file_name):
    # --- Auth ---
    auth_header = request.headers.get('Authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        return jsonify({'error': 'Authorization token missing'}), 401

    token = auth_header.split(' ')[1]
    try:
        payload, user_id, member_id = get_effective_user_id_from_token(token)
    except:
        return jsonify({'error': 'Invalid or expired token'}), 401

    country = request.args.get('country')
    month   = request.args.get('month')   # monthly: "Jan" / "01" etc, quarterly: "Q1"/"Q2" etc (or month inside quarter)
    year    = request.args.get('year')    # "2025"
    qtd = (request.args.get("qtd") or "").strip().lower() == "true"
    ytd = (request.args.get("ytd") or "").strip().lower() == "true"
    quarter = request.args.get("quarter")  # e.g. Q4

    # decide range
    if qtd:
        range_ = "quarterly"
    elif ytd:
        range_ = "yearly"
    else:
        range_ = "monthly"


    def _month_str_to_int(m):
        if m is None:
            return None
        m = str(m).strip()
        # numeric month
        if m.isdigit():
            mi = int(m)
            return mi if 1 <= mi <= 12 else None

        # short/long month names
        mm = m.lower()[:3]
        mapping = {
            "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
            "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12
        }
        return mapping.get(mm)

    def _quarter_to_months(q):
        q = (q or "").strip().upper()
        if q in ("Q1", "1"):
            return [1, 2, 3]
        if q in ("Q2", "2"):
            return [4, 5, 6]
        if q in ("Q3", "3"):
            return [7, 8, 9]
        if q in ("Q4", "4"):
            return [10, 11, 12]
        return None

    try:
        cached_expense_table = _load_expense_reconciliation_table(
            user_id=user_id,
            country=country,
            month=month,
            year=year,
            range_=range_,
            quarter=quarter,
        )
        if cached_expense_table is not None:
            (
                expense_table_name,
                expense_rows,
                expense_status_table_name,
                expense_status_rows,
            ) = cached_expense_table
            return jsonify(_expense_reconciliation_api_response(
                table_name=expense_table_name,
                rows=expense_rows,
                status_table_name=expense_status_table_name,
                status_rows=expense_status_rows,
                range_=range_,
            ))

        if str(country or "").strip().lower() == "global":
            combined_global_response = _load_global_expense_reconciliation_from_countries(
                user_id=user_id,
                month=month,
                year=year,
                range_=range_,
            )
            if combined_global_response is not None:
                return jsonify(combined_global_response), 200

        engine = user_engine
        conn = engine.connect()
        inspector = inspect(engine)
        tables = inspector.get_table_names()

        # ---------------------------------------------
        # ✅ DATA SOURCE SELECTION (monthly vs quarter/year)
        # ---------------------------------------------
        source_table = None

        if range_ == "monthly":
            # monthly => use passed file_name table only
            if file_name not in tables:
                return jsonify({'error': f'Table {file_name} not found'}), 404
            source_table = file_name

        elif range_ in ("quarterly", "yearly"):
            # quarter/year => use merged table
            merged_table = f"user_{user_id}_{country}_merge_data_of_all_months".lower()
            if merged_table not in tables:
                return jsonify({'error': f'Merged table {merged_table} not found'}), 404
            source_table = merged_table

        else:
            return jsonify({"error": "Invalid range. Use monthly/quarterly/yearly"}), 400

        raw_df = pd.read_sql(text(f'SELECT * FROM "{source_table}"'), conn)
        raw_table_data = raw_df.to_dict(orient="records")
        sku_monthly_summary = {}
        sku_monthly_rows = []
        sku_monthly_table = None

        def _clean_ident_part(value):
            return re.sub(r'[^a-zA-Z0-9_]+', '_', str(value or '').strip().lower()).strip('_')

        def _first_total_or_sum(frame, column, total_rows, detail_rows):
            if column not in frame.columns:
                return 0.0
            if not total_rows.empty:
                values = pd.to_numeric(total_rows[column], errors="coerce").fillna(0.0)
                if not values.empty:
                    return float(values.iloc[-1])
            return float(pd.to_numeric(detail_rows[column], errors="coerce").fillna(0.0).sum())

        def _load_sku_monthly_context():
            if not country or not year:
                return {}, [], None

            country_token = _clean_ident_part(country)
            candidates = []

            if range_ == "monthly":
                if not month:
                    return {}, [], None
                month_token = _clean_ident_part(month)
                if country_token == "global":
                    candidates.append(f"skuwisemonthly_{user_id}_global_{month_token}{year}_table")
                else:
                    candidates.extend([
                        f"skuwisemonthly_{user_id}_{country_token}_{month_token}{year}",
                        f"skuwisemonthly_{user_id}_{country_token}_{month_token}{year}_table",
                    ])
            elif range_ == "quarterly":
                period_token = _clean_ident_part(quarter or month)
                quarter_match = re.search(r"[1-4]", period_token)
                if not quarter_match:
                    month_number = _month_str_to_int(month)
                    if month_number:
                        quarter_number = ((month_number - 1) // 3) + 1
                    else:
                        return {}, [], None
                else:
                    quarter_number = int(quarter_match.group(0))
                candidates.append(
                    f"quarter{quarter_number}_{user_id}_{country_token}_{year}_table"
                )
            elif range_ == "yearly":
                candidates.append(
                    f"skuwiseyearly_{user_id}_{country_token}_{year}_table"
                )
            else:
                return {}, [], None

            table_names = set(inspector.get_table_names())
            matched_table = next((t for t in candidates if t in table_names), None)
            if not matched_table:
                return {}, [], None

            monthly_df = pd.read_sql(text(f'SELECT * FROM "{matched_table}"'), conn)
            if monthly_df.empty:
                return {}, [], matched_table

            sku_text = monthly_df.get("sku", pd.Series("", index=monthly_df.index)).astype(str).str.strip().str.lower()
            product_text = monthly_df.get("product_name", pd.Series("", index=monthly_df.index)).astype(str).str.strip().str.lower()
            total_mask = sku_text.isin({"total", "grand_total", "grand total"}) | product_text.isin({"total", "grand total"})

            detail_df = monthly_df[~total_mask].copy()
            total_rows = monthly_df[total_mask].copy()
            summary = {
                "quantity": _first_total_or_sum(monthly_df, "quantity", total_rows, detail_df),
                "return_quantity": _first_total_or_sum(monthly_df, "return_quantity", total_rows, detail_df),
                "total_quantity": _first_total_or_sum(monthly_df, "total_quantity", total_rows, detail_df),
                "net_sales": _first_total_or_sum(monthly_df, "net_sales", total_rows, detail_df),
                "product_sales": _first_total_or_sum(monthly_df, "product_sales", total_rows, detail_df),
                "gross_sales": _first_total_or_sum(monthly_df, "gross_sales", total_rows, detail_df),
                "selling_fees": _first_total_or_sum(monthly_df, "selling_fees", total_rows, detail_df),
                "fba_fees": _first_total_or_sum(monthly_df, "fba_fees", total_rows, detail_df),
                "fbaanswer": _first_total_or_sum(monthly_df, "fbaanswer", total_rows, detail_df),
                "other_transaction_fees": _first_total_or_sum(monthly_df, "other_transaction_fees", total_rows, detail_df),
                "platform_fee": _first_total_or_sum(monthly_df, "platform_fee", total_rows, detail_df),
                "source_table": matched_table,
            }

            detail_records = detail_df.where(pd.notna(detail_df), None).to_dict(orient="records")
            return summary, detail_records, matched_table

        sku_monthly_summary, sku_monthly_rows, sku_monthly_table = _load_sku_monthly_context()
        raw_df = _restore_reconciliation_product_names(
            raw_df, user_id, country, monthly_rows=sku_monthly_rows
        )

        # ---------------------------------------------
        # ✅ FILTER MONTHS/YEAR IF quarterly/yearly
        # ---------------------------------------------
        df = raw_df.copy()

        if range_ in ("quarterly", "yearly"):
            # Expect merged table to have month/year columns.
            # We’ll try to filter if columns exist; otherwise we keep whole df (safe fallback).
            year_val = None
            try:
                year_val = int(str(year).strip()) if year is not None else None
            except:
                year_val = None

            if "year" in df.columns and year_val is not None:
                df["year"] = pd.to_numeric(df["year"], errors="coerce")
                df = df[df["year"] == year_val]

            # month filtering
            if "month" in df.columns:
                # convert month col to int 1-12 where possible
                df["month_num"] = df["month"].apply(_month_str_to_int)
            elif "month_num" in df.columns:
                df["month_num"] = pd.to_numeric(df["month_num"], errors="coerce")
            else:
                df["month_num"] = None  # can't filter by month

            if range_ == "quarterly":
                q = quarter or month  # quarter might be in quarter param OR in month param (Q1/Q2..)
                months_list = _quarter_to_months(q)

                # if quarter not provided, but month is a real month => derive quarter
                if months_list is None:
                    m_int = _month_str_to_int(month)
                    if m_int:
                        if 1 <= m_int <= 3:
                            months_list = [1, 2, 3]
                        elif 4 <= m_int <= 6:
                            months_list = [4, 5, 6]
                        elif 7 <= m_int <= 9:
                            months_list = [7, 8, 9]
                        else:
                            months_list = [10, 11, 12]

                if months_list and df["month_num"].notna().any():
                    df = df[df["month_num"].isin(months_list)]

            elif range_ == "yearly":
                # yearly => all months of that year (already filtered by year if possible)
                pass

            # drop helper column if present
            if "month_num" in df.columns:
                # keep it if you want; here we drop
                df = df.drop(columns=["month_num"], errors="ignore")

        
        
        




        other_values = df["other"] if "other" in df.columns else pd.Series(0, index=df.index)
        df["other"] = pd.to_numeric(other_values, errors="coerce").fillna(0)
        other_total = float(df["other"].sum())

        # ✅ advertising_total sum
        if "advertising_total" in df.columns:
            df["advertising_total"] = pd.to_numeric(df["advertising_total"], errors="coerce").fillna(0)
            advertising_total_sum = float(df["advertising_total"].sum())
        else:
            advertising_total_sum = 0.0

        # ✅ adjust other for frontend
        other_total_adjusted = other_total - advertising_total_sum


        # ✅ Keep real NaN as <NA>, don't convert to "nan" string
        df["sku"] = df["sku"].astype("string").str.strip()

        # ✅ Remove invalid SKUs
        invalid_skus = {"", "0", "0.0", "nan", "none", "<na>"}
        df = df[df["sku"].notna() & (~df["sku"].str.lower().isin(invalid_skus))]


        if "errorstatus" in df.columns:
            df["errorstatus"] = df["errorstatus"].astype(str).str.strip().str.lower()
        else:
            df["errorstatus"] = ""

        numeric_cols = [
            "product_sales", "shipping_credits", "gift_wrap_credits", "promotional_rebates", "other",
            "selling_fees", "answer", "difference", "quantity", "return_quantity",
            "total_quantity", "total_value", "fba_fees", "fbaanswer", "platform_fee", "referral_fee"
        ]

        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
        # ================= FIX QUANTITY (exclude LOST descriptions) =================
        if "quantity" in df.columns:
            df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce").fillna(0)

            # description normalize (case-insensitive)
            desc_str = df.get("description", pd.Series("", index=df.index)).astype(str).str.strip().str.upper()

            EXCLUDE_QTY_DESCRIPTIONS = {
                "REVERSAL_REIMBURSEMENT",
                "WAREHOUSE_LOST",
                "WAREHOUSE_DAMAGE",
                "MISSING_FROM_INBOUND",
                
            }

            exclude_qty_mask = desc_str.isin(EXCLUDE_QTY_DESCRIPTIONS)

            # ✅ un rows ki quantity count hi nahi hogi
            df.loc[exclude_qty_mask, "quantity"] = 0
        # ======================================================================

        if "return_quantity" not in df.columns:
            df["return_quantity"] = 0
        df["return_quantity"] = pd.to_numeric(df["return_quantity"], errors="coerce").fillna(0)

        if "total_quantity" not in df.columns:
            df["total_quantity"] = df.get("quantity", 0) - df["return_quantity"]
        df["total_quantity"] = pd.to_numeric(df["total_quantity"], errors="coerce").fillna(0).clip(lower=0)

        reconciliation_country = str(country or "").strip().lower()
        source_answer_available = "answer" in df.columns
        fba_adjustments_df = df.loc[
            df.get("type", pd.Series("", index=df.index)).astype(str).str.strip().str.lower().eq("refund")
        ].copy()
        if reconciliation_country in {"us", "uk"}:
            pre_group_net_sales = pd.Series(0.0, index=df.index)
            net_sales_columns = (
                ("product_sales", "shipping_credits", "gift_wrap_credits", "promotional_rebates")
                if reconciliation_country == "us"
                else ("product_sales", "promotional_rebates", "other")
            )
            for column in net_sales_columns:
                if column not in df.columns:
                    df[column] = 0
                df[column] = pd.to_numeric(df[column], errors="coerce").fillna(0)
                pre_group_net_sales = pre_group_net_sales.add(df[column], fill_value=0)
            transaction_types = df.get(
                "type",
                pd.Series("", index=df.index),
            ).fillna("").astype(str).str.strip().str.lower()
            descriptions = df.get(
                "description",
                pd.Series("", index=df.index),
            ).fillna("").astype(str).str.strip().str.lower()
            refund_rows = transaction_types.eq("refund") | descriptions.eq("refund")
            if "answer" in df.columns:
                df.loc[refund_rows, "answer"] = 0
            df = df.loc[(pre_group_net_sales >= 0) & ~refund_rows].copy()

        if reconciliation_country == "us":
            df = _recalculate_us_referral_source_fees(df)

        df = _recalculate_fba_source_fees(df, user_id, country)

        if reconciliation_country in {"us", "uk"} and "order_id" in df.columns:
            order_ids = df["order_id"].fillna("").astype(str).str.strip()
            valid_order_ids = ~order_ids.str.lower().isin({"", "nan", "none", "<na>"})
            row_fallbacks = pd.Series(
                [f"__row_{index}" for index in range(len(df))],
                index=df.index,
            )
            df["_reconciliation_order_key"] = order_ids.where(
                valid_order_ids,
                row_fallbacks,
            )

            order_sum_columns = {
                "product_sales",
                "product_sales_tax",
                "shipping_credits",
                "gift_wrap_credits",
                "promotional_rebates",
                "other",
                "selling_fees",
                "fba_fees",
                "fbaanswer",
                "fba_quantity",
                "fba_unavailable_count",
                "platform_fee",
                "advertising_total",
                "answer",
            }
            order_quantity_columns = {"quantity", "return_quantity", "total_quantity"}
            aggregation_rules = {}
            for column in df.columns:
                if column in {"_reconciliation_order_key", "sku"}:
                    continue
                if column == "fbaanswer":
                    aggregation_rules[column] = lambda amounts: amounts.sum(skipna=False)
                elif column == "fba_estimate_status":
                    aggregation_rules[column] = lambda reasons: "; ".join(dict.fromkeys(reasons.astype(str)))
                elif column in order_sum_columns:
                    aggregation_rules[column] = "sum"
                elif column in order_quantity_columns:
                    aggregation_rules[column] = "max"
                else:
                    aggregation_rules[column] = "first"
            df = (
                df.groupby(
                    ["_reconciliation_order_key", "sku"],
                    as_index=False,
                    sort=False,
                    dropna=False,
                )
                .agg(aggregation_rules)
                .drop(columns=["_reconciliation_order_key"], errors="ignore")
            )

        def _round_half_up(value):
            return float(Decimal(str(value or 0)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))

        if str(country or "").strip().lower() == "us":
            for col in ("product_sales", "shipping_credits", "gift_wrap_credits", "promotional_rebates", "other"):
                if col not in df.columns:
                    df[col] = 0
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

            df["net_sales_total_value"] = (
                df["product_sales"] +
                df["shipping_credits"] +
                df["gift_wrap_credits"] +
                df["promotional_rebates"]
            )

            if "referral_fee" in df.columns:
                fee_units = pd.to_numeric(
                    df.get("total_quantity", pd.Series(0, index=df.index)),
                    errors="coerce",
                ).fillna(0)
                fallback_fee_units = pd.to_numeric(
                    df.get("quantity", pd.Series(0, index=df.index)),
                    errors="coerce",
                ).fillna(0)
                fee_units = fee_units.where(fee_units > 0, fallback_fee_units)
                fee_units = fee_units.where(fee_units > 0, 1)

                # Derive the adjustment decision from the rebuilt order fee,
                # not a legacy status or the first status in a grouped order.
                source_status = pd.Series("OK", index=df.index)
                source_difference = [
                    _round_half_up(abs(charged) - answer)
                    for charged, answer in zip(df["selling_fees"], df["answer"])
                ]
                source_status.loc[pd.Series(source_difference, index=df.index) < 0] = "undercharged"
                df["answer"] = [
                    _resolve_us_referral_fee_applicable(
                        source_answer,
                        status,
                        product_sales,
                        gift_wrap_credits,
                        promotional_rebates,
                        rate,
                        quantity,
                    )
                    for (
                        source_answer,
                        status,
                        product_sales,
                        gift_wrap_credits,
                        promotional_rebates,
                        rate,
                        quantity,
                    ) in zip(
                        df["answer"],
                        source_status,
                        df["product_sales"],
                        df["gift_wrap_credits"],
                        df["promotional_rebates"],
                        df["referral_fee"],
                        fee_units,
                    )
                ]

                charged_for_diff = pd.to_numeric(df["selling_fees"], errors="coerce").fillna(0).abs()
                df["difference"] = [
                    _round_half_up(charged - answer)
                    for charged, answer in zip(charged_for_diff, df["answer"])
                ]

                df["errorstatus"] = "OK"
                df.loc[df["difference"] < 0, "errorstatus"] = "undercharged"
                df.loc[df["difference"] > 0, "errorstatus"] = "overcharged"

                desc_str = df.get("description", pd.Series("", index=df.index)).astype(str).str.strip().str.lower()
                txn_type = df.get("type", pd.Series("", index=df.index)).astype(str).str.strip().str.lower()
                no_fee_mask = (
                    (pd.to_numeric(df["referral_fee"], errors="coerce").fillna(0) == 0)
                    & (desc_str != "tax")
                    & (txn_type != "other-transaction")
                )
                df.loc[no_fee_mask, "errorstatus"] = "NoReferralFee"
                df.loc[txn_type == "adjustment", "errorstatus"] = "NoReferralFee"

        else:
            df["net_sales_total_value"] = (
                df.get("product_sales", 0) +
                df.get("promotional_rebates", 0) +
                df.get("other", 0)
            )

            if reconciliation_country == "uk":
                for column in (
                    "product_sales",
                    "product_sales_tax",
                    "shipping_credits",
                    "promotional_rebates",
                    "referral_fee",
                ):
                    if column not in df.columns:
                        df[column] = 0
                    df[column] = pd.to_numeric(df[column], errors="coerce").fillna(0)

                qty_for_calc = pd.to_numeric(df["quantity"], errors="coerce").fillna(0)
                qty_for_calc = qty_for_calc.where(qty_for_calc != 0, 1)
                applicable_base = (
                    df["product_sales"]
                    + df["product_sales_tax"]
                    + df["shipping_credits"]
                    + df["promotional_rebates"]
                )
                df["total_value"] = [
                    _round_half_up(base / quantity)
                    for base, quantity in zip(applicable_base, qty_for_calc)
                ]
                if source_answer_available:
                    df["answer"] = pd.to_numeric(df["answer"], errors="coerce").fillna(0)
                else:
                    applicable_per_unit = [
                        max(
                            _round_half_up(total_value * (rate / 100.0)),
                            0.25,
                        ) if rate > 0 else 0.0
                        for total_value, rate in zip(df["total_value"], df["referral_fee"])
                    ]
                    df["answer"] = [
                        fee * quantity if product_sales != 0 else 0.0
                        for fee, quantity, product_sales in zip(
                            applicable_per_unit,
                            qty_for_calc,
                            df["product_sales"],
                        )
                    ]
                charged_for_diff = pd.to_numeric(
                    df["selling_fees"],
                    errors="coerce",
                ).fillna(0).abs()
                df["difference"] = [
                    _round_half_up(charged - answer)
                    for charged, answer in zip(charged_for_diff, df["answer"])
                ]
                df["errorstatus"] = "OK"
                df.loc[df["difference"] < 0, "errorstatus"] = "undercharged"
                df.loc[df["difference"] > 0, "errorstatus"] = "overcharged"

                desc_str = df.get(
                    "description",
                    pd.Series("", index=df.index),
                ).astype(str).str.strip().str.lower()
                txn_type = df.get(
                    "type",
                    pd.Series("", index=df.index),
                ).astype(str).str.strip().str.lower()
                referral_fee_values = pd.to_numeric(
                    df.get("referral_fee", pd.Series(0, index=df.index)),
                    errors="coerce",
                ).fillna(0)
                no_fee_mask = (
                    (referral_fee_values == 0)
                    & (desc_str != "tax")
                    & (txn_type != "other-transaction")
                )
                df.loc[no_fee_mask, "errorstatus"] = "NoReferralFee"
                df.loc[txn_type == "adjustment", "errorstatus"] = "NoReferralFee"

        net_sales_values = pd.to_numeric(
            df["net_sales_total_value"],
            errors="coerce",
        ).fillna(0)
        df = df.loc[net_sales_values >= 0].copy()

        total_amount_units = pd.to_numeric(
            df.get("total_quantity", pd.Series(0, index=df.index)),
            errors="coerce",
        ).fillna(0)
        fallback_quantity = pd.to_numeric(
            df.get("quantity", pd.Series(0, index=df.index)),
            errors="coerce",
        ).fillna(0)
        total_amount_units = total_amount_units.where(total_amount_units > 0, fallback_quantity)
        df["total_amount"] = [
            _calculate_total_amount(total, units)
            for total, units in zip(df["net_sales_total_value"], total_amount_units)
        ]

        def status_row(row):
            es = str(row.get("errorstatus", "")).lower()
            if es == "ok":
                return "Accurate"
            if es == "overcharged":
                return "Overcharged"
            if es == "undercharged":
                return "Undercharged"
            return "noreferallfee"

        df["status"] = df.apply(status_row, axis=1)

        # ✅ RAW ROW-LEVEL split (after any recalculation)
        errorstatus_norm = df["errorstatus"].astype(str).str.strip().str.lower()
        raw_ok_df    = df[errorstatus_norm == "ok"]
        raw_under_df = df[errorstatus_norm == "undercharged"]
        raw_over_df  = df[errorstatus_norm == "overcharged"]
        raw_ref_df   = df[~errorstatus_norm.isin(["ok", "undercharged", "overcharged"])]

        req_cols = [
            "sku", "product_name", "product_sales",
            "net_sales_total_value", "total_amount", "selling_fees", "fba_fees",
            "fbaanswer", "fba_unavailable_count", "answer", "errorstatus", "difference", "status",
            "quantity", "return_quantity", "total_quantity", "total_value"
        ]
        # keep only available cols
        req_cols = [c for c in req_cols if c in df.columns]
        final_df = df[req_cols].copy()

        # --- SKU wise aggregation ---
        agg_cols = [
            "product_sales",
            "net_sales_total_value",
            "selling_fees",
            "fba_fees",
            "fbaanswer",
            "fba_unavailable_count",
            "answer",
            "difference",
            "quantity",
            "return_quantity",
            "total_quantity",
            "total_value"
        ]
        agg_cols = [c for c in agg_cols if c in final_df.columns]

        

        final_df = final_df.groupby(["sku", "product_name", "status"], as_index=False)[agg_cols].sum()
        final_df["total_amount"] = [
            _calculate_total_amount(total, units)
            for total, units in zip(
                final_df["net_sales_total_value"],
                final_df["total_quantity"],
            )
        ]

        accurate_df = final_df[final_df["status"] == "Accurate"]
        under_df    = final_df[final_df["status"] == "Undercharged"]
        over_df     = final_df[final_df["status"] == "Overcharged"]
        ref_df      = final_df[final_df["status"] == "noreferallfee"]

        def create_total_row(_df, label):
            row = {
                "sku": f"Charge - {label}",
                "product_name": "",
                "errorstatus": "",
                "status": label
            }
            for c in agg_cols:
                if c == "selling_fees":
                    if label == "Accurate":
                        row[c] = float(_df["answer"].sum())
                    else:
                        row[c] = float(_df[c].abs().sum())
                else:
                    row[c] = float(_df[c].sum())
            row["total_amount"] = _calculate_total_amount(
                row.get("net_sales_total_value", 0),
                row.get("total_quantity", 0),
            )
            return pd.DataFrame([row])

        acc_total   = create_total_row(accurate_df, "Accurate")
        under_total = create_total_row(under_df, "Undercharged")
        over_total  = create_total_row(over_df, "Overcharged")
        ref_total   = create_total_row(ref_df, "noreferallfee")

        grand_row = {
            "sku": "Grand Total",
            "product_name": "",
            "errorstatus": "",
            "status": "Total"
        }
        for c in agg_cols:
            if c == "selling_fees":
                grand_row[c] = float(
                    acc_total.iloc[0][c]
                    + under_total.iloc[0][c]
                    + over_total.iloc[0][c]
                    + ref_total.iloc[0][c]
                )
            else:
                grand_row[c] = float(final_df[c].sum())
        grand_row["total_amount"] = _calculate_total_amount(
            grand_row.get("net_sales_total_value", 0),
            grand_row.get("total_quantity", 0),
        )
        grand_total = pd.DataFrame([grand_row])

        final_display_df = pd.concat(
            [acc_total, accurate_df, under_total, under_df, over_total, over_df, ref_total, ref_df, grand_total],
            ignore_index=True
        )

        final_df = final_display_df

        # ---------------------------------------------
        # ✅ SAVE SKUWISE TABLES (monthly / quarter / year)
        # ---------------------------------------------
        skutable = None
        if country and year and (range_ != "monthly" or month):
            if range_ == "monthly":
                skutable = f"skuwise_{user_id}_{country}_{month}{year}".lower()

            elif range_ == "quarterly":
                q = (quarter or month or "").strip().upper()
                if not q.startswith("Q"):
                    # derive from month if month is like "Jan"/"2"
                    m_int = _month_str_to_int(month)
                    if m_int:
                        q = "Q1" if 1 <= m_int <= 3 else "Q2" if 4 <= m_int <= 6 else "Q3" if 7 <= m_int <= 9 else "Q4"
                skutable = f"skuwisequarter_{user_id}_{country}_{q}{year}".lower()

            elif range_ == "yearly":
                skutable = f"skuwiseyear_{user_id}_{country}_{year}".lower()

            if skutable:
                final_df.to_sql(skutable, engine, if_exists="replace", index=False)

        # ---------------------------------------------
        # ✅ PLATFORM FEE TOTAL (monthly/quarter/year)
        # ---------------------------------------------
        # ---------------------------------------------
        # ✅ PLATFORM FEE TOTAL (monthly/quarter/year)
        # ---------------------------------------------
        platform_fee_total = 0.0
        try:
            # ✅ Best: filtered df se sum (works for monthly/quarterly/yearly)
            if "platform_fee" in df.columns:
                platform_fee_total = float(pd.to_numeric(df["platform_fee"], errors="coerce").fillna(0).sum())
            else:
                # ✅ Fallback: direct table se sum (as you asked)
                table_for_fee = None

                if range_ == "monthly" and country and month and year:
                    # NOTE: agar aapke monthly ka actual name _table suffix ke saath hai to yahan add kar do
                    table_for_fee = f"skuwisemonthly_{user_id}_{country}_{month}{year}".lower()

                elif range_ == "quarterly" and country and year:
                    q = (quarter or month or "").strip().upper()

                    # derive quarter number (1–4)
                    if q.startswith("Q"):
                        q_num = q.replace("Q", "")
                    else:
                        m_int = _month_str_to_int(month)
                        if m_int:
                            q_num = "1" if 1 <= m_int <= 3 else "2" if 4 <= m_int <= 6 else "3" if 7 <= m_int <= 9 else "4"
                        else:
                            q_num = None

                    if q_num:
                        table_for_fee = f"quarter{q_num}_{user_id}_{country}_{year}_table".lower()


                elif range_ == "yearly" and country and year:
                    # ✅ your required format
                    table_for_fee = f"skuwiseyearly_{user_id}_{country}_{year}_table".lower()

                if table_for_fee and table_for_fee in inspector.get_table_names():
                    res = conn.execute(text(f'''
                        SELECT COALESCE(SUM(platform_fee), 0) AS total_platform_fee
                        FROM "{table_for_fee}"
                    ''')).fetchone()
                    platform_fee_total = float(res[0] or 0)

        except Exception as e:
            platform_fee_total = 0.0


        conn.close()

        import numpy as np
        final_df    = final_df.replace({np.nan: 0})
        accurate_df = accurate_df.replace({np.nan: 0})
        under_df    = under_df.replace({np.nan: 0})
        over_df     = over_df.replace({np.nan: 0})
        ref_df      = ref_df.replace({np.nan: 0})

        for result_frame in (final_df, accurate_df, under_df, over_df, ref_df, grand_total):
            if "fba_unavailable_count" in result_frame:
                result_frame.loc[result_frame["fba_unavailable_count"].gt(0), "fbaanswer"] = None

        try:
            expense_reconciliation_result = _materialize_expense_reconciliation_table(
                user_id=user_id,
                country=country,
                month=month,
                year=year,
                range_=range_,
                quarter=quarter,
                final_df=final_df,
                sku_monthly_rows=sku_monthly_rows,
                sku_monthly_summary=sku_monthly_summary,
                status_source_df=df,
                fba_adjustments_df=fba_adjustments_df,
                platform_fee_total=platform_fee_total,
                other_fee_total=other_total_adjusted,
            )
        except Exception as e:
            expense_reconciliation_result = {
                "success": False,
                "message": str(e),
            }

        if expense_reconciliation_result.get("success"):
            refreshed_expense_table = _load_expense_reconciliation_table(
                user_id=user_id,
                country=country,
                month=month,
                year=year,
                range_=range_,
                quarter=quarter,
            )
            if refreshed_expense_table is not None:
                (
                    expense_table_name,
                    expense_rows,
                    expense_status_table_name,
                    expense_status_rows,
                ) = refreshed_expense_table
                return jsonify(_expense_reconciliation_api_response(
                    table_name=expense_table_name,
                    rows=expense_rows,
                    status_table_name=expense_status_table_name,
                    status_rows=expense_status_rows,
                    range_=range_,
                ))

        fallback_grand = grand_total.iloc[-1].to_dict() if not grand_total.empty else {}
        fallback_net_sales = float(fallback_grand.get("net_sales_total_value", 0) or 0)
        fallback_referral_charged = float(fallback_grand.get("selling_fees", 0) or 0)
        fallback_fba_charged = abs(float(fallback_grand.get("fba_fees", 0) or 0))
        if sku_monthly_summary:
            fallback_net_sales = float(
                sku_monthly_summary.get("net_sales", fallback_net_sales) or 0
            )
            fallback_referral_charged = abs(float(
                sku_monthly_summary.get("selling_fees", fallback_referral_charged) or 0
            ))
            fallback_fba_charged = abs(float(
                sku_monthly_summary.get("fba_fees", fallback_fba_charged) or 0
            ))

        fee_percentages = _fee_percentage_metrics(
            net_sales=fallback_net_sales,
            referral_charged=fallback_referral_charged,
            referral_applicable=fallback_grand.get("answer", 0),
            fba_charged=fallback_fba_charged,
            fba_applicable=_nullable_fee(fallback_grand.get("fbaanswer")),
            platform_charged=platform_fee_total,
            platform_applicable=platform_fee_total,
            other_charged=other_total_adjusted,
            other_applicable=other_total_adjusted,
        )
        fallback_overcharged = over_total.iloc[-1].to_dict() if not over_total.empty else {}
        fallback_accurate = acc_total.iloc[-1].to_dict() if not acc_total.empty else {}
        fallback_undercharged = under_total.iloc[-1].to_dict() if not under_total.empty else {}
        referral_fee_insight = _referral_fee_insight_metrics(
            overcharged_amount=fallback_overcharged.get("difference", 0),
            overcharged_units=fallback_overcharged.get("quantity", 0),
            accurate_units=fallback_accurate.get("quantity", 0),
            total_units=sum(
                row.get("quantity", 0)
                for row in (
                    fallback_accurate,
                    fallback_undercharged,
                    fallback_overcharged,
                )
            ),
            applicable_fees=fallback_grand.get("answer", 0),
            net_variance=fallback_grand.get("difference", 0),
        )

        return jsonify({
            "success": True,
            "message": "SKU wise table generated successfully.",
            "range": range_,
            "table": final_df.astype(object).where(pd.notna(final_df), None).to_dict(orient="records"),
            "accurate_data": raw_ok_df.astype(object).where(pd.notna(raw_ok_df), None).to_dict(orient="records"),
            "undercharged_data": raw_under_df.astype(object).where(pd.notna(raw_under_df), None).to_dict(orient="records"),
            "overcharged_data": raw_over_df.astype(object).where(pd.notna(raw_over_df), None).to_dict(orient="records"),
            "no_ref_fee_data": raw_ref_df.astype(object).where(pd.notna(raw_ref_df), None).to_dict(orient="records"),
            "created_table_name": skutable,
            "raw_table": raw_table_data,     # raw of source table (monthly or merged)
            "table_name": source_table,      # which table was used

            "platform_fee_total": platform_fee_total,
            "other_total": other_total_adjusted,
            "advertising_total": advertising_total_sum,
            "fee_percentages": fee_percentages,
            "referral_fee_insight": referral_fee_insight,
            "sku_monthly_summary": sku_monthly_summary,
            "sku_monthly_rows": sku_monthly_rows,
            "sku_monthly_table": sku_monthly_table,
            "expense_reconciliation": expense_reconciliation_result,

        })

    except Exception as e:
        logging.exception(
            "Expense reconciliation table load failed (country=%s, month=%s, year=%s, range=%s)",
            country,
            month,
            year,
            range_,
        )
        return jsonify({"error": str(e)}), 500


@product_bp.route('/uploadWarehouseData', methods=['POST', 'GET'])
def upload_warehouse_data():
    # ---------- AUTH ----------
    auth_header = request.headers.get('Authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        return jsonify({'error': 'Authorization token is missing or invalid'}), 401

    token = auth_header.split(' ')[1]
    try:
        payload, user_id, member_id = get_effective_user_id_from_token(token)
    except jwt.ExpiredSignatureError:
        return jsonify({'error': 'Token has expired'}), 401
    except jwt.InvalidTokenError:
        return jsonify({'error': 'Invalid token'}), 401

    # ---------- HELPERS ----------
    def sanitize_identifier(value):
        value = str(value).strip().lower()
        value = re.sub(r'[^a-zA-Z0-9_]+', '_', value)
        value = re.sub(r'_+', '_', value).strip('_')
        return value or "default"

    def normalize_column_name(col):
        col = str(col).strip().lower()
        col = re.sub(r'[^a-zA-Z0-9_]+', '_', col)
        col = re.sub(r'_+', '_', col).strip('_')
        return col

    def normalize_warehouse_country(value, fallback=None):
        normalized = str(value or '').strip().lower()
        if normalized in {'', 'nan', 'none', 'null'}:
            normalized = str(fallback or '').strip().lower()
        normalized = re.sub(r'^amazon\s+', '', normalized)
        aliases = {
            'gb': 'uk',
            'great britain': 'uk',
            'united kingdom': 'uk',
            'usa': 'us',
            'united states': 'us',
        }
        return aliases.get(normalized, normalized)

    def has_warehouse_value(value):
        if value is None or pd.isna(value):
            return False
        return str(value).strip().lower() not in {'', 'nan', 'none', 'null'}

    country = (request.form.get('country') or request.args.get('country') or '').strip().lower()
    if not country:
        return jsonify({'error': 'country is required'}), 400
    marketplace_country = normalize_warehouse_country(
        request.form.get('marketplace_country') or request.args.get('marketplace_country')
    )
    default_warehouse_country = (
        marketplace_country if country == 'global' else normalize_warehouse_country(country)
    )
    if country == 'global' and default_warehouse_country not in {'uk', 'us'}:
        return jsonify({
            'error': 'Marketplace selection is required',
            'message': 'Select UK or US before uploading Global warehouse data.',
        }), 400

    safe_country = sanitize_identifier(country)
    warehouse_table_name = f"warehouse_{user_id}_{safe_country}_data"
    sku_table_name = f"sku_{user_id}_data_table"


    # ---------- GET ----------
    if request.method == 'GET':
        try:
            # Ensure last_update column exists in the warehouse table
            try:
                with user_engine.begin() as conn:
                    existing_cols = {col['name'] for col in inspect(conn).get_columns(warehouse_table_name)}
                    if 'last_update' not in existing_cols:
                        conn.execute(text(f'ALTER TABLE "{warehouse_table_name}" ADD COLUMN last_update VARCHAR(64)'))
            except Exception:
                pass

            df = pd.read_sql(f'SELECT * FROM "{warehouse_table_name}"', user_engine)

            if 'last_update' not in df.columns:
                df['last_update'] = None

            # Convert datetime columns to strings for json serialization
            for col in df.columns:
                if pd.api.types.is_datetime64_any_dtype(df[col]):
                    df[col] = df[col].astype(str)

            last_update_val = None
            if not df.empty and 'last_update' in df.columns:
                valid_ts = df['last_update'].dropna()
                if not valid_ts.empty:
                    last_update_val = str(valid_ts.iloc[-1])

            return jsonify({
                'success': True,
                'message': 'Warehouse data fetched successfully',
                'table_name': warehouse_table_name,
                'last_update': last_update_val,
                'columns': df.columns.tolist(),
                'row_count': int(len(df)),
                'data': df.to_dict(orient="records")
            }), 200

        except Exception as e:
            return jsonify({
                'error': 'Warehouse data not found',
                'message': str(e)
            }), 404

    # ---------- POST ----------
    if 'file' not in request.files:
        return jsonify({'error': 'No file part found'}), 400

    file = request.files['file']
    if not file or file.filename == '':
        return jsonify({'error': 'No selected file'}), 400

    filename = secure_filename(file.filename)
    temp_path = os.path.join(basedir, filename)

    try:
        file.save(temp_path)

        # Read uploaded Excel
        df = pd.read_excel(temp_path)

        if df.empty:
            return jsonify({'error': 'Uploaded Excel file is empty'}), 400

        # Normalize column names
        df.columns = [normalize_column_name(c) for c in df.columns]
        df = df.dropna(how='all')

        # If no country column, default to the page country
        if 'country' not in df.columns:
            df['country'] = default_warehouse_country

        # -------------------------------------------------------------------
        # Step 1 – Collect flat rows: {product_name, country, sku, local_stock}
        # Supports both "sku" column (new format) and legacy "sku_uk"/"sku_us"
        # -------------------------------------------------------------------
        legacy_sku_columns = [col for col in ('sku_uk', 'sku_us') if col in df.columns]
        flat_rows = []
        for _, source_row in df.iterrows():
            row = source_row.to_dict()
            raw_country = row.get('country')
            if has_warehouse_value(raw_country):
                row_country = normalize_warehouse_country(raw_country, default_warehouse_country)
            else:
                row_country = default_warehouse_country

            if has_warehouse_value(row.get('sku')):
                flat_rows.append({
                    's_no': row.get('s_no'),
                    'product_name': str(row.get('product_name') or '').strip(),
                    'country': row_country,
                    'sku': str(row.get('sku')).strip(),
                    'local_stock': row.get('local_stock', 0),
                })
                continue

            if legacy_sku_columns:
                legacy_countries = (
                    [row_country] if country != 'global'
                    else [c.removeprefix('sku_') for c in legacy_sku_columns]
                )
                for leg_country in legacy_countries:
                    leg_sku = row.get(f'sku_{leg_country}')
                    if has_warehouse_value(leg_sku):
                        flat_rows.append({
                            's_no': row.get('s_no'),
                            'product_name': str(row.get('product_name') or '').strip(),
                            'country': leg_country,
                            'sku': str(leg_sku).strip(),
                            'local_stock': row.get('local_stock', 0),
                        })

        if not flat_rows:
            return jsonify({
                'error': 'Invalid warehouse format',
                'message': 'Each row must include a Product Name, Country, SKU, and Local Stock value.',
            }), 400

        # -------------------------------------------------------------------
        # Step 2 – Pivot flat rows into one row per product_name
        # DB schema: s_no, product_name, country, sku_uk, sku_us, local_stock_uk, local_stock_us
        # -------------------------------------------------------------------
        merged_pivot = {}  # key = product_name.lower()
        for flat in flat_rows:
            pname = flat['product_name']
            pkey = pname.lower()
            row_country = normalize_warehouse_country(flat['country'], default_warehouse_country)
            try:
                stock_val = int(float(flat.get('local_stock') or 0))
            except (ValueError, TypeError):
                stock_val = 0

            if pkey not in merged_pivot:
                merged_pivot[pkey] = {
                    's_no': flat.get('s_no'),
                    'product_name': pname,
                    'country': None,
                    'sku_uk': None,
                    'sku_us': None,
                    'local_stock_uk': None,
                    'local_stock_us': None,
                    '_countries': [],
                }

            c_upper = row_country.upper()
            if c_upper and c_upper not in merged_pivot[pkey]['_countries']:
                merged_pivot[pkey]['_countries'].append(c_upper)

            if row_country == 'uk':
                merged_pivot[pkey]['sku_uk'] = flat['sku']
                merged_pivot[pkey]['local_stock_uk'] = stock_val
            elif row_country == 'us':
                merged_pivot[pkey]['sku_us'] = flat['sku']
                merged_pivot[pkey]['local_stock_us'] = stock_val

        pivoted_rows = []
        for idx, r in enumerate(merged_pivot.values(), start=1):
            if not r.get('s_no'):
                r['s_no'] = idx
            countries_list = r.pop('_countries', [])
            r['country'] = ', '.join(countries_list) if countries_list else (country.upper() if country != 'global' else 'UK, US')
            # For warehouse display, default unset stock to 0
            if r['local_stock_uk'] is None:
                r['local_stock_uk'] = 0
            if r['local_stock_us'] is None:
                r['local_stock_us'] = 0
            pivoted_rows.append(r)

        now_ts = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
        warehouse_df = pd.DataFrame(pivoted_rows, columns=[
            's_no', 'product_name', 'country', 'sku_uk', 'sku_us', 'local_stock_uk', 'local_stock_us'
        ])
        for col in ['s_no', 'local_stock_uk', 'local_stock_us']:
            warehouse_df[col] = pd.to_numeric(warehouse_df[col], errors='coerce').fillna(0).astype(int)
        for col in ['product_name', 'country', 'sku_uk', 'sku_us']:
            warehouse_df[col] = (
                warehouse_df[col].astype(str).str.strip()
                .replace({'nan': None, 'None': None, '': None})
            )
        warehouse_df = warehouse_df[
            warehouse_df['sku_uk'].notna() | warehouse_df['sku_us'].notna()
        ].reset_index(drop=True)

        # Add last_update as the last column
        warehouse_df['last_update'] = now_ts

        # Save warehouse table in new pivoted schema
        warehouse_df.to_sql(warehouse_table_name, user_engine, if_exists='replace', index=False)

        # Keep country and global warehouse tables in sync if multi-country data was uploaded
        if any(',' in str(c) for c in warehouse_df['country']):
            for sync_c in ('global', 'uk', 'us'):
                sync_tbl = f"warehouse_{user_id}_{sync_c}_data"
                if sync_tbl != warehouse_table_name:
                    try:
                        warehouse_df.to_sql(sync_tbl, user_engine, if_exists='replace', index=False)
                    except Exception:
                        pass

        # -------------------------------------------------------------------
        # Step 3 – UPDATE sku_{user_id}_data_table
        # Match on product_name or sku_uk/sku_us; update local_stock_uk / local_stock_us independently
        # -------------------------------------------------------------------
        inspector = inspect(user_engine)
        existing_tables = inspector.get_table_names()

        if sku_table_name not in existing_tables:
            return jsonify({
                'success': True,
                'message': f'Warehouse data uploaded, but SKU table "{sku_table_name}" not found',
                'warehouse_table_name': warehouse_table_name,
                'file_name': filename,
                'columns': warehouse_df.columns.tolist(),
                'row_count': int(len(warehouse_df)),
                'data': warehouse_df.to_dict(orient='records'),
            }), 200

        updated_count = 0
        with user_engine.begin() as conn:
            existing_columns = {col['name'] for col in inspect(conn).get_columns(sku_table_name)}

            # Ensure country-specific stock columns exist
            for stock_col in ('local_stock_uk', 'local_stock_us'):
                if stock_col not in existing_columns:
                    conn.execute(text(f'''
                        ALTER TABLE "{sku_table_name}"
                        ADD COLUMN {stock_col} INTEGER DEFAULT 0
                    '''))
                    existing_columns.add(stock_col)

            for _, wrow in warehouse_df.iterrows():
                pname = str(wrow.get('product_name') or '').strip()
                sku_uk_val = str(wrow.get('sku_uk') or '').strip()
                sku_us_val = str(wrow.get('sku_us') or '').strip()

                has_uk = bool(sku_uk_val and sku_uk_val not in ('nan', 'None'))
                has_us = bool(sku_us_val and sku_us_val not in ('nan', 'None'))

                set_clauses = []
                params = {}

                # Only update UK stock if this upload specifically provided UK data for this product
                if has_uk and 'local_stock_uk' in existing_columns:
                    stock_uk = int(wrow.get('local_stock_uk') or 0)
                    set_clauses.append('local_stock_uk = :stock_uk')
                    params['stock_uk'] = stock_uk

                # Only update US stock if this upload specifically provided US data for this product
                if has_us and 'local_stock_us' in existing_columns:
                    stock_us = int(wrow.get('local_stock_us') or 0)
                    set_clauses.append('local_stock_us = :stock_us')
                    params['stock_us'] = stock_us

                if not set_clauses:
                    continue

                set_sql = ', '.join(set_clauses)
                row_updated = False

                # 1. Update by matching exact product_name (case-insensitive)
                if pname:
                    res = conn.execute(
                        text(
                            f'UPDATE "{sku_table_name}" '
                            f'SET {set_sql} '
                            f'WHERE LOWER(TRIM(product_name)) = LOWER(:pname)'
                        ),
                        {**params, 'pname': pname}
                    )
                    if res.rowcount > 0:
                        updated_count += res.rowcount
                        row_updated = True

                # 2. Fallback: match by SKU if product_name was not matched
                if not row_updated:
                    sku_where_parts = []
                    sku_params = {**params}
                    if has_uk and 'sku_uk' in existing_columns:
                        sku_where_parts.append('TRIM(CAST(sku_uk AS TEXT)) = :sku_uk')
                        sku_params['sku_uk'] = sku_uk_val
                    if has_us and 'sku_us' in existing_columns:
                        sku_where_parts.append('TRIM(CAST(sku_us AS TEXT)) = :sku_us')
                        sku_params['sku_us'] = sku_us_val

                    if sku_where_parts:
                        res = conn.execute(
                            text(
                                f'UPDATE "{sku_table_name}" '
                                f'SET {set_sql} '
                                f'WHERE {" OR ".join(sku_where_parts)}'
                            ),
                            sku_params
                        )
                        updated_count += res.rowcount

            if 'last_upload_sku_file' in existing_columns:
                conn.execute(
                    text(f'UPDATE "{sku_table_name}" SET last_upload_sku_file = :upload_ts'),
                    {"upload_ts": datetime.now(timezone.utc)}
                )

        return jsonify({
            'success': True,
            'message': 'Warehouse data uploaded and SKU stock values updated successfully',
            'warehouse_table_name': warehouse_table_name,
            'sku_table_name': sku_table_name,
            'updated_rows': int(updated_count),
            'file_name': filename,
            'last_update': now_ts,
            'columns': warehouse_df.columns.tolist(),
            'row_count': int(len(warehouse_df)),
            'data': warehouse_df.to_dict(orient='records'),
        }), 200

    except Exception as e:
        return jsonify({
            'error': 'Failed to upload warehouse data',
            'message': str(e)
        }), 500

    finally:
        try:
            if os.path.exists(temp_path):
                os.remove(temp_path)
        except Exception:
            pass



        
