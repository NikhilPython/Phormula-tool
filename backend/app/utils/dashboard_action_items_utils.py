from __future__ import annotations

from calendar import month_name, monthrange
from datetime import date, datetime, timezone
from io import BytesIO
from typing import Any, Iterable

import pandas as pd
from sqlalchemy import func

from app.models.user_models import StoredFile
from app.routes.inventory_current_routes import (
    GLOBAL_COUNTRIES,
    build_high_alert_coverage_summary,
    build_inventory_categories,
    get_age_summary_for_single_country,
    resolve_inventory_current_source,
)
from app.utils.live_bi_utils import (
    fetch_current_ai_values_from_skuwisemonthly,
    fetch_current_mtd_data,
    totals_from_daily_series,
)


ACTION_THRESHOLDS = {
    "inventory_coverage_months": 2.0,
    "tacos_percent": 20.0,
    "promotional_rebate_percent": 5.0,
}

PRIORITY_ORDER = {"Critical": 0, "High": 1, "Medium": 2, "Opportunity": 3}


def _number(value: Any) -> float:
    if value is None or value == "":
        return 0.0
    try:
        if isinstance(value, str):
            value = value.replace(",", "").replace("%", "").strip()
        number = float(value)
        return number if number == number and number not in (float("inf"), float("-inf")) else 0.0
    except (TypeError, ValueError):
        return 0.0


def _first_number(row: dict[str, Any], *keys: str) -> float:
    zero = 0.0
    for key in keys:
        if key not in row or row.get(key) in (None, ""):
            continue
        value = _number(row.get(key))
        if value != 0:
            return value
        zero = value
    return zero


def _money(value: float, symbol: str) -> str:
    absolute = abs(value)
    if absolute >= 1_000_000:
        return f"{symbol}{absolute / 1_000_000:.1f}M"
    if absolute >= 1_000:
        return f"{symbol}{absolute / 1_000:.1f}K"
    if absolute < 100:
        return f"{symbol}{absolute:,.2f}"
    return f"{symbol}{absolute:,.0f}"


def _month_number(month: str) -> int:
    normalized = str(month or "").strip().lower()
    for index in range(1, 13):
        if month_name[index].lower() == normalized:
            return index
    raise ValueError(f"Invalid month_name: {month}")


def _period_end(month: str, year: int) -> date:
    month_number = _month_number(month)
    today = date.today()
    if year == today.year and month_number == today.month:
        day = today.day
    else:
        day = monthrange(year, month_number)[1]
    return date(year, month_number, day)


def _period_bounds(
    month: str,
    year: int,
    start_day: int | None = None,
    end_day: int | None = None,
) -> tuple[date, date]:
    maximum_end = _period_end(month, year)
    resolved_start_day = max(1, min(int(start_day or 1), maximum_end.day))
    resolved_end_day = max(
        resolved_start_day,
        min(int(end_day or maximum_end.day), maximum_end.day),
    )
    return (
        maximum_end.replace(day=resolved_start_day),
        maximum_end.replace(day=resolved_end_day),
    )


def _is_total_row(row: dict[str, Any]) -> bool:
    sku = str(row.get("sku") or row.get("SKU") or "").strip().lower()
    product = str(row.get("product_name") or row.get("Product Name") or "").strip().lower()
    return sku in {"total", "totals", "grand_total", "grand total"} or product in {
        "total",
        "totals",
        "grand_total",
        "grand total",
    }


def _load_inventory_facts(
    *, user_id: str, country: str, month: str, year: int
) -> dict[str, Any]:
    countries = GLOBAL_COUNTRIES if country == "global" else [country]
    high_alert_items: list[dict[str, Any]] = []
    high_alert_coverage_weighted_sum = 0.0
    high_alert_sku_count = 0
    high_alert_thresholds: list[float] = []
    liquidate_items: list[dict[str, Any]] = []
    age_totals: dict[str, float] = {}
    loaded_countries: list[str] = []
    age_loaded_countries: list[str] = []
    errors: list[str] = []

    for child_country in countries:
        try:
            source = resolve_inventory_current_source(
                user_id=user_id,
                country_key=child_country,
                range_type="monthly",
                month_name=month,
                year=str(year),
                quarter=None,
            )
            if not source.get("found"):
                errors.append(f"{child_country}: inventory source unavailable")
                continue

            rows = source.get("rows") or []
            # Reuse the inventory API's coverage-summary logic with the Action
            # Items policy threshold supplied explicitly.
            high_alert = build_high_alert_coverage_summary(
                rows,
                user_id,
                child_country,
                high_alert_threshold_override=ACTION_THRESHOLDS["inventory_coverage_months"],
            )
            summary_items = high_alert.get("items") or []
            summary_count = (
                int(_number(high_alert.get("high_alert_sku_count")))
                or len(summary_items)
            )
            summary_average = _number(high_alert.get("average_coverage_ratio"))
            if summary_count > 0:
                high_alert_coverage_weighted_sum += summary_average * summary_count
                high_alert_sku_count += summary_count

            for item in summary_items:
                enriched = dict(item)
                enriched["country"] = child_country
                high_alert_items.append(enriched)

            threshold = _number(high_alert.get("high_alert_threshold"))
            if threshold:
                high_alert_thresholds.append(threshold)

            categories = build_inventory_categories(rows)

            # Build a lookup of monthly/30-day units sold from the original
            # inventory rows before they are reduced to category items.
            units_sold_by_sku: dict[str, float] = {}

            for row in rows:
                sku = str(
                    row.get("SKU")
                    or row.get("sku")
                    or ""
                ).strip()

                if not sku:
                    continue

                units_sold = 0.0

                # The inventory API uses a dynamic column such as:
                # "Current Month Units Sold (August)"
                for key, value in row.items():
                    if str(key).startswith("Current Month Units Sold"):
                        units_sold = _number(value)
                        break

                units_sold_by_sku[sku] = units_sold


            for item in (categories.get("liquidate") or {}).get("items") or []:
                enriched = dict(item)
                enriched["country"] = child_country

                sku = str(
                    item.get("sku")
                    or item.get("SKU")
                    or ""
                ).strip()

                enriched["current_month_units_sold"] = units_sold_by_sku.get(
                    sku,
                    0.0,
                )

                liquidate_items.append(enriched)

            loaded_countries.append(child_country)

            # Reuse the same service function that powers
            # /inventory_current_age_summary, then retain only selected totals.
            try:
                age_summary = get_age_summary_for_single_country(
                    user_id=user_id,
                    country_key=child_country,
                    month_name=month,
                    year=str(year),
                )
                for key, value in (age_summary.get("selected_month_totals") or {}).items():
                    age_totals[key] = age_totals.get(key, 0.0) + _number(value)
                age_loaded_countries.append(child_country)
            except Exception as exc:
                errors.append(f"{child_country} age summary: {exc}")
        except Exception as exc:
            errors.append(f"{child_country}: {exc}")

    return {
        "loaded": bool(loaded_countries),
        "age_loaded": bool(age_loaded_countries),
        "countries": loaded_countries,
        "errors": errors,
        "high_alert_items": high_alert_items,
        "high_alert_sku_count": high_alert_sku_count,
        "average_coverage_ratio": (
            high_alert_coverage_weighted_sum / high_alert_sku_count
            if high_alert_sku_count
            else 0.0
        ),
        "high_alert_threshold": max(high_alert_thresholds, default=0.0),
        "liquidate_items": liquidate_items,
        "age_totals": age_totals,
    }


def _load_live_rows(
    *,
    user_id: int,
    country: str,
    month: str,
    year: int,
    start_day: int | None = None,
    end_day: int | None = None,
) -> dict[str, Any]:
    period_start, period_end = _period_bounds(
        month,
        year,
        start_day=start_day,
        end_day=end_day,
    )
    errors: list[str] = []

    if country == "global":
        try:
            # This helper produces the same GBP->USD-normalized global rows used
            # by /live_mtd_bi, avoiding incorrect cross-currency aggregation.
            from app.routes.amazon_api_routes import get_current_global_data_for_live_bi

            payload = get_current_global_data_for_live_bi(user_id)
            rows = payload.get("skuwise_items_global") or []
            totals = payload.get("derived_totals_global") or {}
            return {
                "loaded": bool(rows or totals),
                "rows": [row for row in rows if not _is_total_row(row)],
                "totals": totals,
                "aligned_totals": {},
                "fee_totals": {},
                "currency": {"code": "USD", "symbol": "$"},
                "errors": errors,
            }
        except Exception as exc:
            errors.append(str(exc))
            return {
                "loaded": False,
                "rows": [],
                "totals": {},
                "aligned_totals": {},
                "fee_totals": {},
                "currency": {"code": "USD", "symbol": "$"},
                "errors": errors,
            }

    rows, totals, fee_totals = fetch_current_ai_values_from_skuwisemonthly(
        user_id=user_id,
        country=country,
        curr_end=period_end,
    )

    # Use the same aligned current-period facts that /live_mtd_bi uses for
    # portfolio totals. The monthly table remains the productwise source for
    # CM2, returns and promotional-rebate SKU attribution.
    current_mtd_rows: list[dict[str, Any]] = []
    aligned_totals: dict[str, float] = {}
    try:
        current_mtd_rows, current_mtd_daily = fetch_current_mtd_data(
            user_id=user_id,
            country=country,
            curr_start=period_start,
            curr_end=period_end,
        )
        if current_mtd_daily:
            aligned_totals = totals_from_daily_series(current_mtd_daily)
    except Exception as exc:
        errors.append(str(exc))

    # The current-month monthly table may still be warming up. Live rows are a
    # data-only productwise fallback and do not invoke either AI prompt.
    if (
        not rows
        and period_end.year == date.today().year
        and period_end.month == date.today().month
    ):
        rows = current_mtd_rows

    if country == "uk":
        currency = {"code": "GBP", "symbol": "£"}
    elif country == "ca":
        currency = {"code": "CAD", "symbol": "$"}
    else:
        currency = {"code": "USD", "symbol": "$"}
    return {
        "loaded": bool(rows or totals or aligned_totals),
        "rows": [row for row in rows if not _is_total_row(row)],
        "totals": totals or {},
        "aligned_totals": aligned_totals,
        "fee_totals": fee_totals or {},
        "currency": currency,
        "errors": errors,
    }


def _latest_dispatch_file(
    *, user_id: int, country: str, month: str, year: int
) -> StoredFile | None:
    return (
        StoredFile.query
        .filter(
            StoredFile.user_id == int(user_id),
            func.lower(StoredFile.country) == country.lower(),
            StoredFile.kind == "dispatch",
            func.lower(StoredFile.month) == month.lower(),
            StoredFile.year == str(year),
        )
        .order_by(StoredFile.id.desc())
        .first()
    )


def _load_dispatch_facts(
    *, user_id: int, country: str, month: str, year: int
) -> dict[str, Any]:
    errors: list[str] = []
    files: list[StoredFile] = []

    if country == "global":
        global_file = _latest_dispatch_file(
            user_id=user_id,
            country="global",
            month=month,
            year=year,
        )
        if global_file:
            files.append(global_file)
        else:
            for child_country in GLOBAL_COUNTRIES:
                child_file = _latest_dispatch_file(
                    user_id=user_id,
                    country=child_country,
                    month=month,
                    year=year,
                )
                if child_file:
                    files.append(child_file)
    else:
        dispatch_file = _latest_dispatch_file(
            user_id=user_id,
            country=country,
            month=month,
            year=year,
        )
        if dispatch_file:
            files.append(dispatch_file)

    air_dispatch = 0.0
    sea_dispatch = 0.0
    affected_product_keys: set[str] = set()
    affected_skus: set[str] = set()
    loaded_files: list[str] = []

    for stored_file in files:
        try:
            file_bytes = stored_file.data
            if isinstance(file_bytes, memoryview):
                file_bytes = file_bytes.tobytes()
            if not file_bytes:
                raise ValueError("stored dispatch file is empty")

            workbook = pd.ExcelFile(BytesIO(file_bytes))
            dispatch_sheet = next(
                (
                    sheet
                    for sheet in workbook.sheet_names
                    if str(sheet).strip().lower() == "dispatch"
                ),
                workbook.sheet_names[0] if workbook.sheet_names else None,
            )
            if not dispatch_sheet:
                raise ValueError("dispatch worksheet is missing")

            frame = pd.read_excel(workbook, sheet_name=dispatch_sheet)
            frame.columns = [str(column).strip() for column in frame.columns]

            if "AIR" not in frame.columns or "SEA" not in frame.columns:
                raise ValueError("AIR or SEA column is missing")

            for row_index, row in frame.iterrows():
                record = row.to_dict()
                if _is_total_row(record):
                    continue

                air_units = max(0.0, _number(record.get("AIR")))
                sea_units = max(0.0, _number(record.get("SEA")))
                if air_units + sea_units <= 0:
                    continue

                air_dispatch += air_units
                sea_dispatch += sea_units

                sku = str(record.get("sku") or record.get("SKU") or "").strip()
                product_name = str(
                    record.get("Product Name")
                    or record.get("product_name")
                    or ""
                ).strip()
                product_key = (
                    f"sku:{sku.upper()}"
                    if sku
                    else f"product:{product_name.lower()}"
                    if product_name
                    else f"row:{stored_file.id}:{row_index}"
                )
                affected_product_keys.add(product_key)
                if sku:
                    affected_skus.add(sku)

            loaded_files.append(stored_file.filename)
        except Exception as exc:
            errors.append(f"{stored_file.country}: {exc}")

    return {
        "loaded": bool(loaded_files),
        "files": loaded_files,
        "air_dispatch": air_dispatch,
        "sea_dispatch": sea_dispatch,
        "product_count": len(affected_product_keys),
        "affected_skus": sorted(affected_skus),
        "errors": errors,
    }


def _sum(rows: Iterable[dict[str, Any]], *keys: str) -> float:
    return sum(_first_number(row, *keys) for row in rows)


def build_action_items(
    *,
    inventory: dict[str, Any],
    live: dict[str, Any],
    dispatch: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """Build the stable, UI-ready ActionItem contract from compact facts."""
    items: list[dict[str, Any]] = []
    rows = live.get("rows") or []
    totals = live.get("totals") or {}
    aligned_totals = live.get("aligned_totals") or {}
    fee_totals = live.get("fee_totals") or {}
    symbol = (live.get("currency") or {}).get("symbol") or "$"

    dispatch = dispatch or {}
    dispatch_product_count = int(_number(dispatch.get("product_count")))
    if dispatch.get("loaded") and dispatch_product_count > 0:
        air_dispatch = _number(dispatch.get("air_dispatch"))
        sea_dispatch = _number(dispatch.get("sea_dispatch"))
        items.append({
            "id": "dispatch-required",
            "category": "Inventory & Dispatch",
            "priority": "Critical",
            "title": "Dispatch plan requires action",
            "reason": (
                f"{dispatch_product_count} "
                f"product{'s' if dispatch_product_count != 1 else ''} require "
                "air or sea dispatch."
            ),
            "metrics": [
                {"value": f"{air_dispatch:,.0f}", "label": "Air dispatch"},
                {"value": f"{sea_dispatch:,.0f}", "label": "Sea dispatch"},
                {"value": str(dispatch_product_count), "label": "Products"},
            ],
            "action": "Review dispatch",
            "affected_skus": dispatch.get("affected_skus") or [],
        })

    high_alert_items = inventory.get("high_alert_items") or []
    if high_alert_items:
        affected_average = _number(inventory.get("average_coverage_ratio"))
        affected_count = int(_number(inventory.get("high_alert_sku_count"))) or len(high_alert_items)
        threshold = _number(inventory.get("high_alert_threshold"))
        items.append({
            "id": "inventory-coverage-risk",
            "category": "Inventory & Dispatch",
            "priority": "Critical" if affected_average < 1 else "High",
            "title": "Inventory coverage needs action",
            "reason": (
                f"{affected_count} "
                f"{'SKUs are' if affected_count != 1 else 'SKU is'} at or below "
                f"the {threshold:.2f}-month replenishment threshold."
            ),
            "metrics": [
                {"value": f"{affected_average:.2f} mo", "label": "Affected avg"},
                {"value": str(affected_count), "label": "Affected SKUs"},
                {"value": f"{threshold:.2f} mo", "label": "Threshold"},
            ],
            "action": "Plan replenishment",
            "affected_skus": [item.get("sku") for item in high_alert_items if item.get("sku")],
        })

    age_totals = inventory.get("age_totals") or {}
    liquidate_items = inventory.get("liquidate_items") or []

    units_181_270 = _number(
        age_totals.get("inv-age-181-to-270-days")
    )

    units_271_365 = _number(
        age_totals.get("inv-age-271-to-365-days")
    )

    units_365_plus = _number(
        age_totals.get("inv-age-365-plus-days")
    )

    total_aged_units = units_181_270 + units_271_365 + units_365_plus

    # Average coverage ratio for products in the aged/liquidate bucket.
    # Total sales only for products that are in the aged/liquidate bucket.
    aged_products_30_day_sales = sum(
        _number(item.get("current_month_units_sold"))
        for item in liquidate_items
    )

    # Coverage ratio =
    # total aged units / combined sales of those aged products
    aged_coverage_ratio = (
        total_aged_units / aged_products_30_day_sales
        if aged_products_30_day_sales > 0
        else 0.0
    )

    if liquidate_items and total_aged_units > 0:
        items.append({
            "id": "aged-inventory",
            "category": "Inventory & Dispatch",
            "priority": "High" if units_365_plus > 0 else "Medium",
            "title": "Aged inventory requires a clearance plan",
            "reason": (
                f"{len(liquidate_items)} "
                f"product{'s' if len(liquidate_items) != 1 else ''} "
                "contain inventory aged 181 days or more."
            ),
            "metrics": [
                {
                    "value": f"{aged_coverage_ratio:.2f} mo",
                    "label": "Coverage ratio",
                },
                {
                    "value": f"{total_aged_units:,.0f}",
                    "label": "Aged units",
                },
                {
                    "value": str(len(liquidate_items)),
                    "label": "Products",
                },
            ],
            "action": "Plan liquidation",
            "affected_skus": [
                item.get("sku") or item.get("SKU")
                for item in liquidate_items
                if item.get("sku") or item.get("SKU")
            ],
        })

    # Match the P&L dashboard cards exactly: these fields come from the
    # skuwisemonthly GRAND TOTAL populated by mtd_transactions. Live BI totals
    # are fallbacks only because they intentionally use a different definition.
    net_sales = (
        _first_number(totals, "net_sales")
        or _number(aligned_totals.get("net_sales"))
        or _sum(rows, "net_sales")
    )
    ads_spend = (
        _first_number(totals, "total_ads", "ads_spend", "advertising")
        or _first_number(fee_totals, "advertising")
        or abs(_number(aligned_totals.get("advertising")))
        or _sum(rows, "total_ads", "ads_spend", "advertising")
    )
    tacos = abs(
    _first_number(
        totals,
        "tacos_total_advertising_cost_of_sale",
        "tacos",
    )
)
    if ads_spend and tacos >= ACTION_THRESHOLDS["tacos_percent"]:
        items.append({
            "id": "ads-efficiency",
            "category": "Ads",
            "priority": "Critical" if tacos >= 25 else "High",
            "title": "Advertising efficiency needs attention",
            "reason": f"TACoS is {tacos:.2f}%, so advertising is taking a high share of net sales.",
            "metrics": [
                {"value": f"{tacos:.2f}%", "label": "TACoS"},
                {"value": _money(ads_spend, symbol), "label": "Ad spend"},
                {"value": _money(net_sales, symbol), "label": "Net sales"},
            ],
            "action": "Optimize campaigns",
            "affected_skus": [],
        })

    # Find the product with the highest promotional rebate percentage.
    top_rebate = max(
        rows,
        key=lambda row: abs(
            _first_number(
                row,
                "promotional_rebates_percentage",
            )
        ),
        default={},
    )

    top_rebate_percent = abs(
        _first_number(
            top_rebate,
            "promotional_rebates_percentage",
        )
    )

    # Show the action only when at least one product exceeds 10%.
    # Since top_rebate is the product with the highest percentage,
    # checking this one product is enough.
    if top_rebate and top_rebate_percent > 10:
        top_rebate_amount = abs(
            _first_number(
                top_rebate,
                "promotional_rebates",
            )
        )

        top_rebate_net_sales = abs(
            _first_number(
                top_rebate,
                "net_sales",
            )
        )

        product_name = str(
            top_rebate.get("product_name")
            or top_rebate.get("Product Name")
            or "—"
        )

        items.append({
            "id": "promotional-rebates",
            "category": "Finance",
            "priority": "High",
            "title": "Promotional rebate leakage",
            "reason": (
                f"{product_name} has promotional rebates of "
                f"{top_rebate_percent:.2f}% of net sales."
            ),
            "metrics": [
                {
                    "value": _money(top_rebate_net_sales, symbol),
                    "label": "Net sales",
                },
                {
                    "value": f"{top_rebate_percent:.2f}%",
                    "label": "Promo %",
                },
                {
                    "value": str(
                        top_rebate.get("product_name")
                        or top_rebate.get("Product Name")
                        or "—"
                    ),
                    "label": "Top rebate product",
                },
            ],

            "action": "Review promotions",
            "affected_skus": [
                str(top_rebate.get("sku"))
            ] if top_rebate.get("sku") else [],
        })
        
    negative_profit = sorted(
        [row for row in rows if _number(row.get("cm2_profit")) < 0],
        key=lambda row: _number(row.get("cm2_profit")),
    )

    if negative_profit:
        total_loss = abs(
            sum(
                _number(row.get("cm2_profit"))
                for row in negative_profit
            )
        )

        # negative_profit is sorted from most negative CM2 to least negative.
        # Therefore the first row is the worst CM2 product.
        worst_product = negative_profit[0]

        items.append({
            "id": "negative-profit-skus",
            "category": "Finance",
            "priority": (
                "High"
                if len(negative_profit) >= 5 or total_loss >= 1000
                else "Medium"
            ),
            "title": "Negative-CM2 SKUs",
            "reason": (
                f"{len(negative_profit)} "
                f"SKU{'s are' if len(negative_profit) != 1 else ' is'} "
                "currently below zero CM2 profit."
            ),
            "metrics": [
                {
                    "value": str(len(negative_profit)),
                    "label": "SKUs",
                },
                {
                    "value": _money(total_loss, symbol),
                    "label": "Total loss",
                },
                {
                    "value": str(
                        worst_product.get("product_name")
                        or worst_product.get("Product Name")
                        or "—"
                    ),
                    "label": "Lowest CM2 Product",
                },
            ],
            "action": "Reprice / pause",
            "affected_skus": [
                str(row.get("sku"))
                for row in negative_profit
                if row.get("sku")
            ],
        })
    return_rows: list[tuple[dict[str, Any], float, float, float]] = []

    for row in rows:
        quantity = abs(
            _first_number(
                row,
                "return_rate_base_quantity",
                "quantity",
                "total_quantity",
            )
        )

        returns = abs(
            _first_number(
                row,
                "return_quantity",
                "returned_quantity",
            )
        )

        rate = returns / quantity * 100.0 if quantity else 0.0

        if rate > 2.0:
            return_rows.append((row, returns, rate, quantity))


    if return_rows:
        product_count = len(return_rows)
        return_quantity_sum = sum(
            returns for _row, returns, _rate, _quantity in return_rows
        )
        return_quantity_base = sum(
            quantity for _row, _returns, _rate, quantity in return_rows
        )
        aggregate_return_rate = (
            return_quantity_sum / return_quantity_base * 100.0
            if return_quantity_base
            else 0.0
        )

        items.append({
            "id": "high-return-rate",
            "category": "Returns",
            "priority": "High" if aggregate_return_rate >= 5 else "Medium",
            "title": "Product returns",
            "reason": (
                f"{product_count} "
                f"product{'s are' if product_count != 1 else ' is'} "
                "above the 2.00% return-rate threshold."
            ),
            "metrics": [
                {
                    "value": str(product_count),
                    "label": "Products",
                },
                {
                    "value": f"{aggregate_return_rate:.2f}%",
                    "label": "Return rate",
                },
                {
                    "value": f"{return_quantity_sum:,.0f}",
                    "label": "Return qty",
                },
            ],
            "action": "Inspect root cause",
            "affected_skus": [
                str(row.get("sku"))
                for row, _returns, _rate, _quantity in return_rows
                if row.get("sku")
            ],
        })

    items.sort(key=lambda item: PRIORITY_ORDER.get(item.get("priority"), 99))
    return items[:7]


def get_dashboard_action_items(
    *,
    user_id: int | str,
    country: str,
    month: str,
    year: int,
    start_day: int | None = None,
    end_day: int | None = None,
) -> dict[str, Any]:
    normalized_country = str(country or "").strip().lower()
    if normalized_country not in {"uk", "us", "ca", "global"}:
        raise ValueError("country_key must be one of: uk, us, ca, global")

    normalized_month = month_name[_month_number(month)].lower()
    inventory = _load_inventory_facts(
        user_id=str(user_id),
        country=normalized_country,
        month=normalized_month,
        year=int(year),
    )
    live = _load_live_rows(
        user_id=int(user_id),
        country=normalized_country,
        month=normalized_month,
        year=int(year),
        start_day=start_day,
        end_day=end_day,
    )
    dispatch = _load_dispatch_facts(
        user_id=int(user_id),
        country=normalized_country,
        month=normalized_month,
        year=int(year),
    )
    if (
        not inventory.get("loaded")
        and not live.get("loaded")
        and not dispatch.get("loaded")
    ):
        raise RuntimeError("No action-item source data is available for the selected period")

    items = build_action_items(
        inventory=inventory,
        live=live,
        dispatch=dispatch,
    )

    return {
        "success": True,
        "period": {
            "country": normalized_country,
            "month": normalized_month,
            "year": int(year),
            "start_day": start_day,
            "end_day": end_day,
        },
        "currency": live.get("currency") or {"code": "USD", "symbol": "$"},
        "items": items,
        "total_items": len(items),
        "partial": not (inventory.get("loaded") and live.get("loaded")),
        "source_status": {
            "inventory_current": bool(inventory.get("loaded")),
            "inventory_current_age_summary": bool(inventory.get("age_loaded")),
            "live_mtd_bi": bool(live.get("loaded")),
            "dispatch": bool(dispatch.get("loaded")),
        },
        "source_errors": {
            "inventory": inventory.get("errors") or [],
            "live_bi": live.get("errors") or [],
            "dispatch": dispatch.get("errors") or [],
        },
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }
