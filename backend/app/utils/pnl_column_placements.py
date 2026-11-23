from __future__ import annotations

from typing import Any, Iterable


VALID_PROMOTIONS_PLACEMENTS = {"in_net_sales", "before_cm1", "after_cm1"}
VALID_AMAZON_FEES_PLACEMENTS = {"before_cm1", "after_cm1"}


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
    promotions_after_cm1 = promotions_placement == "after_cm1"
    amazon_fees_after_cm1 = amazon_fees_placement == "after_cm1"
    if not promotions_detached and not amazon_fees_after_cm1:
        return

    for row in rows:
        if not isinstance(row, dict):
            continue

        promotions = abs(
            _number(row, "promotional_rebates")
            or _number(row, "promotions")
        )
        net_sales_delta = promotions if promotions_detached else 0.0
        cm1_delta = 0.0
        if promotions_after_cm1:
            cm1_delta += promotions
        if amazon_fees_after_cm1:
            cm1_delta += (
                abs(_number(row, "amazon_fees"))
                or abs(_number(row, "marketplace_fees"))
                or abs(_number(row, "marketplace_total"))
                or (
                    abs(_number(row, "selling_fees"))
                    + abs(_number(row, "fba_fees"))
                )
            )

        net_sales = _number(row, "net_sales") + net_sales_delta
        cm1_profit = _number(row, "profit") + cm1_delta
        quantity = _number(row, "total_quantity") or _number(row, "quantity")
        cm2_profit = _number(row, "cm2_profit")

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
                "promotional_rebates_percentage",
            ):
                if key in total_row:
                    totals[key] = total_row[key]
            cm2_profit = _number(total_row, "cm2_profit")
            net_sales = _number(total_row, "net_sales")
            if "total_cm2_margins" in totals:
                totals["total_cm2_margins"] = (
                    cm2_profit / net_sales * 100 if net_sales else 0.0
                )
