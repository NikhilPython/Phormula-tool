from __future__ import annotations

from typing import Any, Iterable

from app.utils.dashboard_card_metrics import (
    calculate_pnl_cost_of_advertisement,
    calculate_pnl_tacos,
    calculate_pnl_total_advertising_cost,
)


VALID_PROMOTIONS_PLACEMENTS = {"in_net_sales", "in_ads_spend"}
VALID_AMAZON_FEES_PLACEMENTS = {"before_cm1", "after_cm1", "in_ads_spend"}


def _number(row: dict[str, Any], key: str) -> float:
    try:
        value = row.get(key)
        return float(value) if value is not None else 0.0
    except (TypeError, ValueError):
        return 0.0


def apply_pnl_column_placements(
    rows: Iterable[dict[str, Any]],
    promotions_placement: str = "in_net_sales",
    amazon_fees_placement: str = "before_cm1",
) -> None:
    """Recalculate productwise values for a detached Promotions/Amazon Fees layout."""
    if promotions_placement not in VALID_PROMOTIONS_PLACEMENTS:
        raise ValueError("Invalid promotions placement")
    if amazon_fees_placement not in VALID_AMAZON_FEES_PLACEMENTS:
        raise ValueError("Invalid Amazon Fees placement")

    promotions_detached = promotions_placement != "in_net_sales"
    promotions_in_ads = promotions_placement == "in_ads_spend"
    fees_in_ads = amazon_fees_placement == "in_ads_spend"
    amazon_fees_after_cm1 = amazon_fees_placement == "after_cm1"
    if not promotions_detached and not amazon_fees_after_cm1 and not fees_in_ads:
        return

    for row in rows:
        if not isinstance(row, dict):
            continue

        promotions = abs(
            _number(row, "promotional_rebates")
            or _number(row, "promotions")
        )
        net_sales_delta = promotions if promotions_detached else 0.0
        amazon_fees = (
            abs(_number(row, "amazon_fees"))
            or abs(_number(row, "marketplace_fees"))
            or abs(_number(row, "marketplace_total"))
            or (abs(_number(row, "selling_fees")) + abs(_number(row, "fba_fees")))
        )
        cm1_delta = 0.0
        if promotions_in_ads:
            cm1_delta += promotions
        if amazon_fees_after_cm1:
            cm1_delta += amazon_fees

        net_sales = _number(row, "net_sales") + net_sales_delta
        cm1_profit = _number(row, "profit") + cm1_delta
        quantity = _number(row, "total_quantity") or _number(row, "quantity")
        cm2_profit = _number(row, "cm2_profit")
        ads_addition = (promotions if promotions_in_ads else 0.0) + (amazon_fees if fees_in_ads else 0.0)
        if ads_addition:
            row["net_sales"] = net_sales
            ads_spend = _number(row, "ads_spend") + ads_addition
            row["ads_spend"] = ads_spend
            row["ads_spend_raw"] = _number(row, "ads_spend_raw") + ads_addition
            for key in (
                "advertising_total_final",
                "advertising_total",
                "total_ads",
                "advertising_fees",
            ):
                if key in row:
                    row[key] = _number(row, key) + ads_addition
            row["acos"] = ads_spend / net_sales * 100 if net_sales else 0.0
            row["cm2_profit"] = cm2_profit - ads_addition + cm1_delta
            row["cm2_profit_per"] = row["cm2_profit"] / net_sales * 100 if net_sales else 0.0
            if "total_cm2_profit" in row:
                row["total_cm2_profit"] = (
                    _number(row, "total_cm2_profit") - ads_addition + cm1_delta
                )
            row["cost_of_advertisement"] = calculate_pnl_cost_of_advertisement(row)
            row["total_advertising_cost"] = calculate_pnl_total_advertising_cost(row)
            tacos = calculate_pnl_tacos(row)
            for key in (
                "tacos_total_advertising_cost_of_sale",
                "tacos",
            ):
                if key in row:
                    row[key] = tacos
            cm2_profit = row["cm2_profit"]

        row["net_sales"] = net_sales
        row["profit"] = cm1_profit
        row["cm1_profit"] = cm1_profit
        row["cm1_profit_per_unit"] = cm1_profit / quantity if quantity else 0.0
        row["cm1_profit_per"] = cm1_profit / net_sales * 100 if net_sales else 0.0
        row["profit_percentage"] = row["cm1_profit_per"]
        row["promotional_rebates_percentage"] = (
            promotions / net_sales * 100 if net_sales else 0.0
        )
        row["net_sales_per_unit"] = net_sales / quantity if quantity else 0.0
        row["asp"] = row["net_sales_per_unit"]
        row["cm2_profit_per"] = cm2_profit / net_sales * 100 if net_sales else 0.0

        if "net_sales_tax_and_credits" in row:
            row["net_sales_tax_and_credits"] = (
                _number(row, "net_sales_tax_and_credits") + net_sales_delta
            )


def apply_pnl_column_placements_to_payload(
    payload: dict[str, Any],
    row_to_totals_keys: dict[str, tuple[str, ...]],
    promotions_placement: str = "in_net_sales",
    amazon_fees_placement: str = "before_cm1",
) -> None:
    for row_key, totals_keys in row_to_totals_keys.items():
        rows = payload.get(row_key)
        if not isinstance(rows, list):
            continue

        apply_pnl_column_placements(
            rows,
            promotions_placement,
            amazon_fees_placement,
        )
        total_row = next(
            (
                row
                for row in rows
                if isinstance(row, dict)
                and (
                    str(row.get("sku") or "").strip().upper()
                    in {"TOTAL", "GRAND_TOTAL"}
                    or str(row.get("product_name") or "").strip().lower()
                    in {"total", "grand total"}
                )
            ),
            None,
        )
        if not total_row:
            continue

        for totals_key in totals_keys:
            totals = payload.get(totals_key)
            if not isinstance(totals, dict):
                continue
            for key in (
                "net_sales",
                "profit",
                "cm1_profit",
                "cm1_profit_per",
                "cm1_profit_per_unit",
                "cm2_profit_per",
                "promotional_rebates_percentage", "ads_spend", "ads_spend_raw",
                "acos", "cm2_profit", "cm2_profit_per", "total_cm2_profit",
                "tacos_total_advertising_cost_of_sale", "tacos", "total_ads",
                "advertising_total_final", "advertising_total", "advertising_fees",
                "cost_of_advertisement", "total_advertising_cost",
            ):
                if key in total_row:
                    totals[key] = total_row[key]
            cm2_profit = _number(total_row, "cm2_profit")
            net_sales = _number(total_row, "net_sales")
            if "total_cm2_margins" in totals:
                totals["total_cm2_margins"] = (
                    cm2_profit / net_sales * 100 if net_sales else 0.0
                )
