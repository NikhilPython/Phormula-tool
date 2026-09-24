from flask import Blueprint, request, jsonify , send_file 
import jwt
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
from decimal import Decimal, ROUND_HALF_UP
from datetime import date
from app.utils.dashboard_card_metrics import (
    add_per_unit_fields,
    build_pnl_card_metrics,
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
EXPENSE_RECONCILIATION_CACHE_VERSION = "expense_reconciliation_v18_backend_fee_percentages"
_expense_reconciliation_locks = {}
_expense_reconciliation_locks_guard = threading.Lock()


def _calculate_us_referral_fee_applicable(
    product_sales,
    promotional_rebates,
    referral_fee_percent,
):
    sales = Decimal(str(product_sales or 0))
    rebates = abs(Decimal(str(promotional_rebates or 0)))
    rate = Decimal(str(referral_fee_percent or 0))
    applicable_base = max(sales - rebates, Decimal("0"))
    return float(
        (applicable_base * rate / Decimal("100")).quantize(
            Decimal("0.01"),
            rounding=ROUND_HALF_UP,
        )
    )


def _resolve_us_referral_fee_applicable(
    source_answer,
    source_status,
    product_sales,
    promotional_rebates,
    referral_fee_percent,
):
    if str(source_status or "").strip().lower() != "undercharged":
        return float(source_answer or 0)

    return _calculate_us_referral_fee_applicable(
        product_sales,
        promotional_rebates,
        referral_fee_percent,
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

    def _resolve_rate(row):
        existing_rate_value = pd.to_numeric(row.get("referral_fee", 0), errors="coerce")
        existing_rate = 0.0 if pd.isna(existing_rate_value) else float(existing_rate_value)
        category_key = str(row.get("product_group", "") or "").strip().lower()
        if not category_key:
            return existing_rate

        category_first = re.split(r"[&/\-]", category_key, maxsplit=1)[0].strip()
        category_matches = category_rates[category_rates["category"].eq(category_key)]
        if category_matches.empty and category_first:
            category_matches = category_rates[category_rates["category"].eq(category_first)]
        if category_matches.empty:
            return existing_rate

        unit_value_value = pd.to_numeric(row.get("total_value", 0), errors="coerce")
        unit_value = 0.0 if pd.isna(unit_value_value) else float(unit_value_value)
        band_matches = category_matches[
            (category_matches["price_from"] <= unit_value)
            & (category_matches["price_to"] >= unit_value)
            & category_matches["referral_fee_percent_est"].notna()
        ].copy()
        if band_matches.empty:
            return existing_rate

        band_matches["_band_width"] = (
            band_matches["price_to"] - band_matches["price_from"]
        )
        selected = band_matches.sort_values(
            ["_band_width", "price_from"],
            ascending=[True, False],
            kind="stable",
        ).iloc[0]
        return float(selected["referral_fee_percent_est"])

    frame["referral_fee"] = frame.apply(_resolve_rate, axis=1)
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
        "product_sales",
        "shipping_credits",
        "promotional_rebates",
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
        detail_df["_fba_fees_charged"] = detail_df["fba_fees"].abs()
        detail_df["_fba_fees_applicable"] = detail_df["_fba_fees_charged"]
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
                "units": _raw_numeric("total_quantity", "quantity"),
                "gross_sales": _raw_numeric("gross_sales", "product_sales"),
                "net_sales": _raw_numeric(
                    "net_sales_total_value",
                    "net_sales",
                    "product_sales",
                ),
                "product_sales": _raw_numeric("product_sales"),
                "shipping_credits": _raw_numeric("shipping_credits"),
                "promotional_rebates": _raw_numeric("promotional_rebates"),
                "referral_fee_per": _raw_numeric("referral_fee"),
                "referral_fees_applicable": _raw_numeric("answer"),
                "referral_fees_charged": _raw_numeric("selling_fees"),
                "fba_fees_applicable": _raw_numeric("fbaanswer", "fba_fees").abs(),
                "fba_fees_charged": _raw_numeric("fba_fees").abs(),
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
            "product_sales",
            "shipping_credits",
            "promotional_rebates",
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
            "applicable_net_sales_pct": _net_sales_percentage(applicable),
            "charged_vs_applicable_pct": _charged_vs_applicable(
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
        cached[column] = pd.to_numeric(cached[column], errors="coerce").fillna(0)

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
        fba_applicable = float(row.get("fba_fees_applicable", 0) or 0)
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
        "product_sales",
        "shipping_credits",
        "promotional_rebates",
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
        ).fillna(0)

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
        errorstatus = str(row.get("errorstatus", "") or "").strip()
        if not errorstatus:
            errorstatus = "OK" if resolved_status == "Accurate" else resolved_status.lower()
        return resolved_status, {
            "order_id": str(row.get("order_id", "") or ""),
            "sku": str(row.get("sku", "") or ""),
            "product_name": str(row.get("product_name", "") or ""),
            "quantity": float(row.get("units", 0) or 0),
            "return_quantity": 0,
            "total_quantity": float(row.get("units", 0) or 0),
            "gross_sales": float(row.get("gross_sales", 0) or 0),
            "product_sales": float(row.get("product_sales", row.get("gross_sales", 0)) or 0),
            "shipping_credits": float(row.get("shipping_credits", 0) or 0),
            "promotional_rebates": float(row.get("promotional_rebates", 0) or 0),
            "referral_fee_per": float(row.get("referral_fee_per", 0) or 0),
            "net_sales": float(row.get("net_sales", 0) or 0),
            "net_sales_total_value": float(row.get("net_sales", 0) or 0),
            "answer": applicable,
            "selling_fees": float(row.get("referral_fees_charged", 0) or 0),
            "fbaanswer": float(row.get("fba_fees_applicable", 0) or 0),
            "fba_fees": float(row.get("fba_fees_charged", 0) or 0),
            "difference": difference,
            "overcharged": max(difference, 0),
            "referral_fees_accurate": applicable if resolved_status == "Accurate" else 0,
            "referral_fees_undercharged": abs(difference) if resolved_status == "Undercharged" else 0,
            "referral_fees_overcharged": difference if resolved_status == "Overcharged" else 0,
            "status": resolved_status,
            "errorstatus": errorstatus,
        }

    for _, row in cached_status.iterrows():
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
            summary[field] = float(sum(float(record.get(field, 0) or 0) for record in records))
        return summary

    for status in status_order:
        display_records.append(_summary_record(status, summary_status_records[status]))

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

    original_records = cached.where(pd.notna(cached), None).to_dict(orient="records")
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
    return {
        "success": True,
        "message": "Expense reconciliation table loaded successfully.",
        "range": range_,
        "table": display_records,
        "accurate_data": status_records["Accurate"],
        "undercharged_data": status_records["Undercharged"],
        "overcharged_data": status_records["Overcharged"],
        "no_ref_fee_data": status_records["noreferallfee"],
        "created_table_name": table_name,
        "raw_table": original_records,
        "table_name": table_name,
        "platform_fee_total": platform_fee_total,
        "other_total": other_fee_total,
        "fee_percentages": fee_percentages,
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
            abs(rembursement_fee / cm2_profit) * 100 if cm2_profit else 0
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


@product_bp.route('/YearlySKU', methods=['GET'])
def YearlySKU():
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
        current_data = add_per_unit_fields(current_data)
        used_current_tables = [table_name]

        try:
            selected_year = int(year)
            today_year = date.today().year
            current_year_month_limit = max(date.today().month - 1, 0)

            # US yearly processing reconciles events across months. Summing
            # monthly reports again would restore duplicate released refunds.
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

        return jsonify({
            "current_table_name": table_name,
            "used_current_tables": used_current_tables,
            "current_data": current_data,
            "previous_table_name": previous_table_name,
            "previous_data": previous_data,
            "card_metrics": build_pnl_card_metrics(current_data, previous_data),
        }), 200

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
                previous_data = add_per_unit_fields(previous_data)
                try:
                    persist_per_unit_fields(engine, previous_table_name, previous_data)
                except Exception:
                    pass

            except Exception:
                previous_data = []

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
        with admin_engine.connect() as conn:
            query = text("""
                SELECT DISTINCT ON (user_currency, country)
                    user_currency, country, selected_currency, conversion_rate, month, year
                FROM currency_conversion
                ORDER BY user_currency, country, year DESC,
                    CASE month
                        WHEN 'january' THEN 1 WHEN 'february' THEN 2 WHEN 'march' THEN 3
                        WHEN 'april' THEN 4 WHEN 'may' THEN 5 WHEN 'june' THEN 6
                        WHEN 'july' THEN 7 WHEN 'august' THEN 8 WHEN 'september' THEN 9
                        WHEN 'october' THEN 10 WHEN 'november' THEN 11 WHEN 'december' THEN 12
                    END DESC
            """)
            results = conn.execute(query).mappings().all()

        currency_rates = []
        for row in results:
            d = dict(row)
            # normalize for frontend matching
            d["user_currency"] = str(d.get("user_currency", "")).strip().lower()
            d["country"] = str(d.get("country", "")).strip().lower()
            d["selected_currency"] = str(d.get("selected_currency", "")).strip().lower()
            currency_rates.append(d)

        return jsonify(currency_rates), 200

    except SQLAlchemyError as e:
        return jsonify({'error': 'Database error', 'message': str(e)}), 500
    except Exception as e:
        return jsonify({'error': 'An error occurred while fetching currency rates', 'message': str(e)}), 500



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
    if hasattr(asp_table.c, 'asp'):
        return select(asp_table.c.product_name, asp_table.c.asp)
    if hasattr(asp_table.c, 'net_credits'):
        return select(asp_table.c.product_name, asp_table.c.net_credits.label('asp'))
    if hasattr(asp_table.c, 'average_selling_price'):
        return select(asp_table.c.product_name, asp_table.c.average_selling_price.label('asp'))
    return None

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
                    row_dict = dict(row)
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
            d = dict(r)
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

            if "local_stock" not in existing_columns:
                conn.execute(text(f'''
                    ALTER TABLE "{table_name}"
                    ADD COLUMN local_stock INTEGER DEFAULT 0;
                '''))

            if "in_transit_units" not in existing_columns:
                conn.execute(text(f'''
                    ALTER TABLE "{table_name}"
                    ADD COLUMN in_transit_units INTEGER DEFAULT 0;
                '''))

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

            # local_stock (optional)
            if "local_stock" in updates and updates["local_stock"] is not None:
                try:
                    update_data["local_stock"] = int(updates["local_stock"])
                except Exception:
                    failed_products.append(product_name)
                    continue

            # in_transit_units (optional)
            if "in_transit_units" in updates and updates["in_transit_units"] is not None:
                try:
                    update_data["in_transit_units"] = int(updates["in_transit_units"])
                except Exception:
                    failed_products.append(product_name)
                    continue

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

        try:
            current_data = _fetch_profit_data(table_name, ads_table_name)
            try:
                persist_per_unit_fields(engine, table_name, current_data)
            except Exception:
                pass
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

        if prev_month and prev_year:
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

        return jsonify({
            "current_table_name": table_name,
            "current_ads_table_name": ads_table_name,
            "requested_ads_table_name": requested_ads_table_name,
            "current_data": current_data,
            "previous_table_name": previous_table_name,
            "previous_ads_table_name": previous_ads_table_name,
            "previous_data": previous_data,
            "card_metrics": build_pnl_card_metrics(current_data, previous_data),
        }), 200

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

        
        
        




        df["other"] = pd.to_numeric(df.get("other", 0), errors="coerce").fillna(0)
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
                "platform_fee",
                "advertising_total",
                "answer",
            }
            order_quantity_columns = {"quantity", "return_quantity", "total_quantity"}
            aggregation_rules = {}
            for column in df.columns:
                if column in {"_reconciliation_order_key", "sku"}:
                    continue
                if column in order_sum_columns:
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

        if "fbaanswer" not in df.columns:
            df["fbaanswer"] = df.get("fba_fees", 0)

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
                if source_answer_available:
                    df["answer"] = pd.to_numeric(
                        df["answer"],
                        errors="coerce",
                    ).fillna(0)
                    source_status = df.get(
                        "errorstatus",
                        pd.Series("", index=df.index),
                    ).fillna("").astype(str).str.strip().str.lower()
                    df["answer"] = [
                        _resolve_us_referral_fee_applicable(
                            source_answer,
                            status,
                            product_sales,
                            promotional_rebates,
                            rate,
                        )
                        for source_answer, status, product_sales, promotional_rebates, rate in zip(
                            df["answer"],
                            source_status,
                            df["product_sales"],
                            df["promotional_rebates"],
                            df["referral_fee"],
                        )
                    ]
                else:
                    qty_for_calc = pd.to_numeric(
                        df["quantity"],
                        errors="coerce",
                    ).fillna(0)
                    qty_for_calc = qty_for_calc.where(qty_for_calc != 0, 1)
                    applicable_base = (
                        df["product_sales"] - df["promotional_rebates"].abs()
                    ).clip(lower=0)
                    df["total_value"] = [
                        _round_half_up(base / qty)
                        for base, qty in zip(applicable_base, qty_for_calc)
                    ]
                    df = _apply_us_referral_fee_price_bands(df)
                    df["answer"] = [
                        _calculate_us_referral_fee_applicable(
                            product_sales,
                            promotional_rebates,
                            rate,
                        )
                        for product_sales, promotional_rebates, rate in zip(
                            df["product_sales"],
                            df["promotional_rebates"],
                            df["referral_fee"],
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
                        _round_half_up(total_value * (rate / 100.0))
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
            "net_sales_total_value", "selling_fees", "fba_fees",
            "fbaanswer", "answer", "errorstatus", "difference", "status",
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
            "answer",
            "difference",
            "quantity",
            "return_quantity",
            "total_quantity",
            "total_value"
        ]
        agg_cols = [c for c in agg_cols if c in final_df.columns]

        

        final_df = final_df.groupby(["sku", "product_name", "status"], as_index=False)[agg_cols].sum()

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
            fba_applicable=fallback_fba_charged,
            platform_charged=platform_fee_total,
            platform_applicable=platform_fee_total,
            other_charged=other_total_adjusted,
            other_applicable=other_total_adjusted,
        )

        return jsonify({
            "success": True,
            "message": "SKU wise table generated successfully.",
            "range": range_,
            "table": final_df.to_dict(orient="records"),
            "accurate_data": raw_ok_df.to_dict(orient="records"),
            "undercharged_data": raw_under_df.to_dict(orient="records"),
            "overcharged_data": raw_over_df.to_dict(orient="records"),
            "no_ref_fee_data": raw_ref_df.to_dict(orient="records"),
            "created_table_name": skutable,
            "raw_table": raw_table_data,     # raw of source table (monthly or merged)
            "table_name": source_table,      # which table was used

            "platform_fee_total": platform_fee_total,
            "other_total": other_total_adjusted,
            "advertising_total": advertising_total_sum,
            "fee_percentages": fee_percentages,
            "sku_monthly_summary": sku_monthly_summary,
            "sku_monthly_rows": sku_monthly_rows,
            "sku_monthly_table": sku_monthly_table,
            "expense_reconciliation": expense_reconciliation_result,

        })

    except Exception as e:
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

    country = (request.form.get('country') or request.args.get('country') or '').strip().lower()
    if not country:
        return jsonify({'error': 'country is required'}), 400

    safe_country = sanitize_identifier(country)
    warehouse_table_name = f"warehouse_{user_id}_{safe_country}_data"
    sku_table_name = f"sku_{user_id}_data_table"


    # ---------- GET ----------
    if request.method == 'GET':
        try:
            df = pd.read_sql(f'SELECT * FROM "{warehouse_table_name}"', user_engine)
            return jsonify({
                'success': True,
                'message': 'Warehouse data fetched successfully',
                'table_name': warehouse_table_name,
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

        # Required stock columns
        if 'local_stock' not in df.columns:
            df['local_stock'] = 0
        if 'in_transit_units' not in df.columns:
            df['in_transit_units'] = 0

        # Numeric conversions
        for col in ['local_stock', 'in_transit_units', 'year', 's_no', 'price']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        # Clean object columns
        for col in df.columns:
            if df[col].dtype == object:
                df[col] = df[col].astype(str).str.strip()
                df[col] = df[col].replace({'nan': None, 'None': None, '': None})

        # Save uploaded warehouse data table
        df.to_sql(warehouse_table_name, user_engine, if_exists='replace', index=False)

        # ---------------------------------------------------
        # UPDATE public.sku_{user_id}_data_table
        # ---------------------------------------------------
        inspector = inspect(user_engine)
        existing_tables = inspector.get_table_names()

        if sku_table_name not in existing_tables:
            return jsonify({
                'success': True,
                'message': f'Warehouse data uploaded, but SKU table "{sku_table_name}" not found',
                'warehouse_table_name': warehouse_table_name,
                'file_name': filename,
                'columns': df.columns.tolist(),
                'row_count': int(len(df)),
                'data': df.to_dict(orient="records")
            }), 200

        with user_engine.begin() as conn:
            # Ensure stock columns exist in SKU table
            existing_columns = {col["name"] for col in inspect(conn).get_columns(sku_table_name)}

            if "local_stock" not in existing_columns:
                conn.execute(text(f'''
                    ALTER TABLE "{sku_table_name}"
                    ADD COLUMN local_stock INTEGER DEFAULT 0
                '''))

            if "in_transit_units" not in existing_columns:
                conn.execute(text(f'''
                    ALTER TABLE "{sku_table_name}"
                    ADD COLUMN in_transit_units INTEGER DEFAULT 0
                '''))

            # Load SKU table
            sku_df = pd.read_sql(f'SELECT * FROM "{sku_table_name}"', conn)
            sku_df.columns = [normalize_column_name(c) for c in sku_df.columns]

            # Decide matching key
            possible_keys = ['asin', 'product_barcode', 'sku_uk', 'sku_us']
            match_key = None
            for key in possible_keys:
                if key in df.columns and key in sku_df.columns:
                    match_key = key
                    break

            if not match_key:
                return jsonify({
                    'success': False,
                    'message': 'Warehouse uploaded, but no common matching column found to update SKU table',
                    'possible_keys_checked': possible_keys,
                    'warehouse_columns': df.columns.tolist(),
                    'sku_columns': sku_df.columns.tolist()
                }), 400

            # Prepare update dataframe
            update_df = df[[match_key, 'local_stock', 'in_transit_units']].copy()
            update_df = update_df.dropna(subset=[match_key])
            update_df[match_key] = update_df[match_key].astype(str).str.strip()

            update_df['local_stock'] = pd.to_numeric(update_df['local_stock'], errors='coerce').fillna(0).astype(int)
            update_df['in_transit_units'] = pd.to_numeric(update_df['in_transit_units'], errors='coerce').fillna(0).astype(int)

            # If duplicate keys exist in uploaded file, keep last one
            update_df = update_df.drop_duplicates(subset=[match_key], keep='last')

            # Update rows in sku table
            updated_count = 0
            for _, row in update_df.iterrows():
                result = conn.execute(
                    text(f'''
                        UPDATE "{sku_table_name}"
                        SET local_stock = :local_stock,
                            in_transit_units = :in_transit_units
                        WHERE CAST({match_key} AS TEXT) = :match_value
                    '''),
                    {
                        "local_stock": int(row["local_stock"]),
                        "in_transit_units": int(row["in_transit_units"]),
                        "match_value": str(row[match_key]).strip()
                    }
                )
                updated_count += result.rowcount

        return jsonify({
            'success': True,
            'message': 'Warehouse Excel uploaded and SKU stock values updated successfully',
            'warehouse_table_name': warehouse_table_name,
            'sku_table_name': sku_table_name,
            'matched_on': match_key,
            'updated_rows': int(updated_count),
            'file_name': filename,
            'columns': df.columns.tolist(),
            'row_count': int(len(df)),
            'data': df.to_dict(orient="records")
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



        
