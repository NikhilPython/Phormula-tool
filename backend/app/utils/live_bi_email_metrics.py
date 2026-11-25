"""Numeric email context from the same P&L rows used by the dashboard."""

import math


def first_number(row, *keys):
    for key in keys:
        try:
            value = float(row.get(key))
        except (TypeError, ValueError):
            continue
        if math.isfinite(value):
            return value
    return None


def change(current, previous):
    return round((current - previous) / abs(previous) * 100, 2) if previous else 0.0


def build_email_metric_rows(action_rows, current_rows, previous_rows):
    """Keep actions/inventory, replacing financial metrics with P&L values.

    Work on copies: adsdaily metrics remain available to dashboard ads analysis.
    Only current P&L SKUs are eligible. Previous-only comparison rows must not
    become email priorities merely because their generated decline is -100%.
    Missing P&L values retain the existing period value; explicit zero is valid.
    """
    def by_sku(rows):
        return {str(r.get("sku") or "").strip().upper(): r for r in rows or []}

    sources = {"curr": by_sku(current_rows), "prev": by_sku(previous_rows)}
    result = []
    for original in action_rows or []:
        row = dict(original)
        sku = str(row.get("sku") or "").strip().upper()
        if not sku or sku in {"TOTAL", "GRAND_TOTAL"}:
            continue
        if sku not in sources["curr"]:
            continue
        for period, mapping in sources.items():
            source = mapping.get(sku)
            if source is None:
                continue
            fields = {
                "quantity": ("total_quantity", "quantity", "net_quantity"),
                "net_sales": ("net_sales",),
                "asp": ("asp",),
                "ads_spend": ("ads_spend",),
                "cm2_profit": ("cm2_profit",),
            }
            for field, keys in fields.items():
                value = first_number(source, *keys)
                if value is not None:
                    row[f"{field}_{period}"] = value
            quantity = first_number(row, f"quantity_{period}")
            sales = first_number(row, f"net_sales_{period}")
            profit = first_number(row, f"cm2_profit_{period}")
            if first_number(source, "asp") is None and sales is not None and quantity is not None:
                row[f"asp_{period}"] = round(sales / quantity, 2) if quantity else 0.0
            if profit is not None and quantity is not None:
                row[f"cm2_profit_per_unit_{period}"] = round(profit / quantity, 2) if quantity else 0.0
            spend = first_number(row, f"ads_spend_{period}")
            if sales is not None and spend is not None:
                row[f"acos_{period}"] = round(spend / sales * 100, 2) if sales else 0.0

        for field, label in (("quantity", "Unit Growth (%)"), ("net_sales", "Net Sales Growth (%)"), ("asp", "ASP Growth (%)")):
            current = first_number(row, f"{field}_curr")
            previous = first_number(row, f"{field}_prev")
            if current is not None and previous is not None:
                row[label] = {"value": change(current, previous)}
        current = first_number(row, "cm2_profit_curr")
        previous = first_number(row, "cm2_profit_prev")
        if current is not None and previous is not None:
            row["cm2_profit_growth_pct"] = change(current, previous)
        result.append(row)
    return result


def build_email_dashboard_recap(current_totals, previous_totals):
    def period(totals):
        values = {
            "units": first_number(totals, "total_quantity", "quantity"),
            "asp": first_number(totals, "asp", "total_asp"),
            "net_sales": first_number(totals, "net_sales"),
            "advertising": first_number(totals, "advertising_total_final", "total_ads", "advertising_fees", "advertising"),
            "cm2_profit": first_number(totals, "total_cm2_profit", "cm2_profit"),
        }
        sales, ads = values["net_sales"], values["advertising"]
        if sales is not None and ads is not None:
            values["tacos"] = round(ads / sales * 100, 2) if sales else 0.0
        return {key: value for key, value in values.items() if value is not None}

    return {"current": period(current_totals or {}), "previous": period(previous_totals or {})}
