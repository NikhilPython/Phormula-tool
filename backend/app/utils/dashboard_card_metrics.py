"""Backend-owned calculations for the financial dashboard KPI cards.

The React clients should only format these values.  Keeping the calculations
here also gives monthly, quarterly, and yearly responses one stable contract.
"""

from __future__ import annotations

import math
import re
from typing import Any, Iterable, Mapping, MutableMapping

from sqlalchemy import inspect, text


PER_UNIT_COLUMNS = (
    "marketplace_fees_per_unit",
    "cost_of_ads_per_unit",
    "gross_sales_per_unit",
    "net_sales_per_unit",
    "others_per_unit",
    "cash_generated_per_unit",
    "net_reimbursement_per_unit",
)


def dashboard_number(value: Any) -> float:
    """Return a finite number rounded to the precision used by the cards."""
    try:
        number = float(value or 0)
    except (TypeError, ValueError):
        return 0.0
    return round(number, 2) if math.isfinite(number) else 0.0


def dashboard_delta(current: Any, previous: Any) -> float:
    current_value = dashboard_number(current)
    previous_value = dashboard_number(previous)
    if previous_value == 0:
        return 0.0
    return dashboard_number(
        ((current_value - previous_value) / abs(previous_value)) * 100
    )


def _number(value: Any) -> float:
    try:
        number = float(value or 0)
    except (TypeError, ValueError):
        return 0.0
    return number if math.isfinite(number) else 0.0


def _first(source: Mapping[str, Any], *keys: str) -> float:
    for key in keys:
        value = source.get(key)
        if value not in (None, ""):
            return _number(value)
    return 0.0


def _first_non_zero(source: Mapping[str, Any], *keys: str) -> float:
    fallback = 0.0
    for key in keys:
        value = source.get(key)
        if value in (None, ""):
            continue
        number = _number(value)
        fallback = number
        if number != 0:
            return number
    return fallback


def _units(source: Mapping[str, Any]) -> float:
    return _first(source, "total_quantity", "quantity_total", "quantity", "net_quantity")


def _ads(source: Mapping[str, Any]) -> float:
    return abs(_first_non_zero(
        source,
        "advertising_total_final",
        "total_ads",
        "advertising_fees",
        "advertising_total",
        "ads_spend",
    ))


def find_total_row(rows: Iterable[Mapping[str, Any]] | None) -> MutableMapping[str, Any]:
    materialized = list(rows or [])
    for row in reversed(materialized):
        sku = str(row.get("sku") or "").strip().lower()
        name = str(row.get("product_name") or "").strip().lower()
        if sku in {"total", "totals", "grand_total"} or name in {
            "total", "totals", "grand total"
        }:
            return row  # type: ignore[return-value]
    return materialized[-1] if materialized else {}


def add_per_unit_fields(rows: Iterable[MutableMapping[str, Any]] | None):
    """Add the persisted/displayed per-unit values to each response row."""
    materialized = list(rows or [])
    for row in materialized:
        units = _units(row)
        marketplace_fees = abs(_first_non_zero(row, "amazon_fee", "amazon_fees"))
        cost_of_ads = _ads(row)
        gross_sales = _first(row, "gross_sales", "product_sales")
        net_sales = _first(row, "net_sales")
        others = abs(_first_non_zero(row, "platform_fee", "otherwplatform"))
        cash_generated = _first(row, "cashflow", "cash_generated")
        if cash_generated == 0:
            cash_generated = _first(row, "cost_of_unit_sold", "cogs") + _first(
                row, "cm2_profit"
            )
        reimbursement = abs(_first_non_zero(
            row, "rembursement_fee", "reimbursement_fee", "current_net_reimbursement"
        ))

        row["marketplace_fees_per_unit"] = dashboard_number(
            marketplace_fees / units if units else 0
        )
        row["cost_of_ads_per_unit"] = dashboard_number(
            cost_of_ads / units if units else 0
        )
        row["gross_sales_per_unit"] = dashboard_number(
            gross_sales / units if units else 0
        )
        row["net_sales_per_unit"] = dashboard_number(
            net_sales / units if units else 0
        )
        row["others_per_unit"] = dashboard_number(others / units if units else 0)
        row["cash_generated_per_unit"] = dashboard_number(
            cash_generated / units if units else 0
        )
        row["net_reimbursement_per_unit"] = dashboard_number(
            reimbursement / units if units else 0
        )
    return materialized


def _period_values(source: Mapping[str, Any]) -> dict[str, float]:
    units = _units(source)
    gross_sales = _first(source, "gross_sales", "product_sales")
    net_sales = _first(source, "net_sales", "total_sales")
    marketplace_fees = abs(_first_non_zero(source, "amazon_fee", "amazon_fees"))
    cost_of_ads = _ads(source)
    cm2_profit = _first(
        source, "total_cm2_profit", "cm2_profit_total", "cm2_profit"
    )
    promotions = abs(_first(source, "promotional_rebates", "promotions"))

    # These two percentages are intentionally read from the data source.  They
    # are already produced by the financial processing pipeline and must not be
    # re-derived from rounded card amounts.
    cm2_margin_pct = _first(
        source, "total_cm2_margins", "cm2_margins", "cm2_profit_percentage"
    )
    promotions_pct = abs(_first(
        source, "promotional_rebates_percentage", "promotions_pct"
    ))

    asp = _first(source, "asp")
    if asp == 0 and units:
        asp = net_sales / units

    tacos_pct = _first_non_zero(
        source, "tacos_total_advertising_cost_of_sale", "tacos", "acos"
    )
    if tacos_pct == 0 and net_sales:
        tacos_pct = cost_of_ads / net_sales * 100

    return {
        "units": dashboard_number(units),
        "asp": dashboard_number(asp),
        "gross_sales": dashboard_number(gross_sales),
        "gross_sales_per_unit": dashboard_number(
            _first(source, "gross_sales_per_unit") or (gross_sales / units if units else 0)
        ),
        "net_sales": dashboard_number(net_sales),
        "net_sales_per_unit": dashboard_number(
            _first(source, "net_sales_per_unit") or (net_sales / units if units else 0)
        ),
        "marketplace_fees": dashboard_number(marketplace_fees),
        "marketplace_fees_per_unit": dashboard_number(
            _first(source, "marketplace_fees_per_unit")
            or (marketplace_fees / units if units else 0)
        ),
        "cost_of_ads": dashboard_number(cost_of_ads),
        "cost_of_ads_per_unit": dashboard_number(
            _first(source, "cost_of_ads_per_unit") or (cost_of_ads / units if units else 0)
        ),
        "tacos_pct": dashboard_number(tacos_pct),
        "cm2_profit": dashboard_number(cm2_profit),
        "cm2_margin_pct": dashboard_number(cm2_margin_pct),
        "promotions": dashboard_number(promotions),
        "promotions_pct": dashboard_number(promotions_pct),
    }


def build_pnl_card_metrics(
    current_rows: Iterable[Mapping[str, Any]] | None,
    previous_rows: Iterable[Mapping[str, Any]] | None = None,
):
    current = _period_values(find_total_row(current_rows))
    previous = _period_values(find_total_row(previous_rows))
    return {
        "current": current,
        "previous": previous,
        "deltas": {
            key: dashboard_delta(current[key], previous[key])
            for key in current
        },
    }


def _cashflow_values(source: Mapping[str, Any]) -> dict[str, float]:
    units = _units(source)
    gross_sales = _first(source, "gross_sales")
    net_sales = _first(source, "net_sales")
    promotions = abs(_first(source, "promotional_rebates", "promotions"))
    marketplace_fees = abs(_first(source, "amazon_fee", "marketplace_fees"))
    others = abs(_first(source, "otherwplatform", "platform_fee", "others"))
    cash_generated = _first(source, "cashflow", "cash_generated")
    reimbursement = abs(_first(
        source, "rembursement_fee", "net_reimbursement", "current_net_reimbursement"
    ))
    promotions_pct = abs(_first(
        source,
        "promotional_rebates_percentage",
        "promotions_percentage",
        "promotions_pct",
    ))
    if promotions_pct == 0 and net_sales:
        promotions_pct = promotions / abs(net_sales) * 100

    def per_unit(field: str, amount: float) -> float:
        return dashboard_number(
            _first(source, field) or (amount / units if units else 0)
        )

    return {
        "units": dashboard_number(units),
        "gross_sales": dashboard_number(gross_sales),
        "gross_sales_per_unit": per_unit("gross_sales_per_unit", gross_sales),
        "net_sales": dashboard_number(net_sales),
        "net_sales_per_unit": per_unit("net_sales_per_unit", net_sales),
        "promotions": dashboard_number(promotions),
        "promotions_pct": dashboard_number(promotions_pct),
        "marketplace_fees": dashboard_number(marketplace_fees),
        "marketplace_fees_per_unit": per_unit(
            "marketplace_fees_per_unit", marketplace_fees
        ),
        "others": dashboard_number(others),
        "others_per_unit": per_unit("others_per_unit", others),
        "cash_generated": dashboard_number(cash_generated),
        "cash_generated_per_unit": per_unit(
            "cash_generated_per_unit", cash_generated
        ),
        "net_reimbursement": dashboard_number(reimbursement),
        "net_reimbursement_per_unit": per_unit(
            "net_reimbursement_per_unit", reimbursement
        ),
    }


def add_cashflow_summary_fields(summary: MutableMapping[str, Any] | None):
    if summary is None:
        return summary
    values = _cashflow_values(summary)
    summary.update({
        key: value
        for key, value in values.items()
        if key.endswith("_per_unit") or key == "promotions_pct"
    })
    return summary


def build_cashflow_card_metrics(
    current_summary: Mapping[str, Any] | None,
    previous_summary: Mapping[str, Any] | None = None,
):
    current = _cashflow_values(current_summary or {})
    previous = _cashflow_values(previous_summary or {})
    return {
        "current": current,
        "previous": previous,
        "deltas": {
            key: dashboard_delta(current[key], previous[key])
            for key in current
        },
    }


def persist_per_unit_fields(engine, table_name: str, rows: Iterable[Mapping[str, Any]] | None):
    """Add/update decimal per-unit columns on a generated metric table.

    Generated table names are internal, but validate them before interpolating
    because PostgreSQL cannot bind identifiers.
    """
    if not table_name or not re.fullmatch(r"[A-Za-z0-9_]+", table_name):
        raise ValueError("Unsafe dashboard table name")

    materialized = list(rows or [])
    if not materialized:
        return

    existing_columns = {
        column["name"]
        for column in inspect(engine).get_columns(table_name, schema="public")
    }

    quoted_table = f'public."{table_name}"'
    with engine.begin() as conn:
        for column in PER_UNIT_COLUMNS:
            if column in existing_columns:
                continue
            conn.execute(text(
                f'ALTER TABLE {quoted_table} '
                f'ADD COLUMN IF NOT EXISTS "{column}" NUMERIC(18, 2)'
            ))

        key_columns = (
            ["id"]
            if "id" in existing_columns
            else [column for column in ("sku", "product_name") if column in existing_columns]
        )
        if not key_columns:
            return

        update_sql = text(
            f'UPDATE {quoted_table} SET '
            + ", ".join(f'"{column}" = :{column}' for column in PER_UNIT_COLUMNS)
            + " WHERE "
            + " AND ".join(
                f'"{column}" IS NOT DISTINCT FROM :key_{column}'
                for column in key_columns
            )
        )
        params = []
        for row in materialized:
            if not any(row.get(column) is not None for column in key_columns):
                continue
            params.append({
                **{f"key_{column}": row.get(column) for column in key_columns},
                **{column: dashboard_number(row.get(column)) for column in PER_UNIT_COLUMNS},
            })
        if params:
            conn.execute(update_sql, params)
