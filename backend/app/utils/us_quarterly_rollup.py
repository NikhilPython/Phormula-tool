"""Yearly and quarterly amounts use finalized months; rates are recomputed."""

import numpy as np
import pandas as pd


TEXT_COLUMNS = {"sku", "product_name", "ad_type", "generated_at_utc"}
METADATA_COLUMNS = {"id", "month", "year", "country", "user_id"}
RATE_COLUMNS = {
    "asp", "price_in_gbp", "unit_wise_profitability", "sales_mix", "profit_mix",
    "promotional_rebates_percentage", "profit_percentage",
    "cm2_profit_percentage", "cm2_margins", "acos", "ads_acos",
    "rembursment_vs_cm2_margins", "reimbursement_vs_sales",
    "total_cm2_margins", "reimbursement_vs_cm2_margins",
    "tacos_total_advertising_cost_of_sale", "ads_conversion_rate", "ads_roas",
}


def aggregate_us_monthly_reports(reports):
    """Sum each month's product rows and authoritative TOTAL separately.

    TOTAL can contain account charges and hidden inventory SKUs that are not
    present in product rows. Do not reconstruct it from visible products.
    """
    details, totals = [], []
    for report in reports:
        if report.empty:
            raise ValueError("A monthly report is empty; rebuild it before aggregation")
        frame = report.copy()
        frame["sku"] = frame["sku"].fillna("").astype(str).str.strip()
        total_mask = frame["sku"].str.upper().isin({"TOTAL", "TOTALS", "GRAND_TOTAL", "GRAND TOTAL"})
        if total_mask.sum() != 1:
            raise ValueError("Each monthly report must contain exactly one TOTAL row")
        details.append(frame.loc[~total_mask])
        totals.append(frame.loc[total_mask])
    if not totals:
        raise ValueError("No monthly reports available for period aggregation")

    combined = pd.concat(details + totals, ignore_index=True)
    amount_columns = [
        col for col in combined.columns
        if col not in TEXT_COLUMNS | METADATA_COLUMNS | RATE_COLUMNS
        and not col.startswith("previous_")
        and not col.endswith(("_per_unit", "_per", "_analysis", "_growth"))
    ]
    for col in amount_columns:
        combined[col] = pd.to_numeric(combined[col], errors="coerce").replace([np.inf, -np.inf], 0).fillna(0)

    def amounts(frame):
        return frame.reindex(columns=combined.columns)[amount_columns].apply(
            pd.to_numeric, errors="coerce"
        ).fillna(0)

    products = pd.concat(details, ignore_index=True)
    product_amounts = amounts(products)
    product_amounts["sku"] = products["sku"]
    result = product_amounts.groupby("sku", as_index=False, sort=False).sum()
    if "product_name" in products:
        names = products.groupby("sku")["product_name"].first()
        result["product_name"] = result["sku"].map(names)
    if "ad_type" in products:
        ad_types = products.groupby("sku")["ad_type"].first()
        result["ad_type"] = result["sku"].map(ad_types)

    total = amounts(pd.concat(totals, ignore_index=True)).sum().to_dict()
    total.update(sku="TOTAL", product_name="TOTAL", ad_type="")
    result = pd.concat([result, pd.DataFrame([total])], ignore_index=True)

    def num(name):
        if name in result:
            return pd.to_numeric(result[name], errors="coerce").fillna(0)
        return pd.Series(0.0, index=result.index)

    def ratio(numerator, denominator, scale=1):
        return numerator.div(denominator.replace(0, np.nan)).mul(scale).fillna(0)

    net_sales, net_units = num("net_sales"), num("total_quantity")
    profit, cm2 = num("profit"), num("cm2_profit")
    ads, reimbursement = num("advertising_total"), num("rembursement_fee")
    result["asp"] = ratio(net_sales, net_units)
    result["price_in_gbp"] = ratio(num("cost_of_unit_sold"), net_units)
    result["unit_wise_profitability"] = ratio(profit, num("quantity"))
    result["promotional_rebates_percentage"] = ratio(num("promotional_rebates"), net_sales, 100)
    result["profit_percentage"] = ratio(profit, net_sales, 100)
    result["cm2_profit_percentage"] = ratio(cm2, net_sales, 100)
    result["cm2_margins"] = result["cm2_profit_percentage"]
    result["total_cm2_margins"] = result["cm2_profit_percentage"]
    result["acos"] = ratio(ads, net_sales, 100)
    result["ads_acos"] = result["acos"]
    result["reimbursement_vs_sales"] = ratio(reimbursement, net_sales, 100).abs()
    result["rembursment_vs_cm2_margins"] = ratio(reimbursement, cm2, 100).abs()
    result["reimbursement_vs_cm2_margins"] = result["rembursment_vs_cm2_margins"]
    for name, denominator in (("sales_mix", total.get("net_sales", 0)), ("profit_mix", total.get("profit", 0))):
        numerator = net_sales if name == "sales_mix" else profit
        result[name] = numerator / abs(denominator) * 100 if denominator else 0.0
    for field, amount in (
        ("gross_sales_per_unit", num("gross_sales")),
        ("net_sales_per_unit", net_sales),
        ("marketplace_fees_per_unit", num("amazon_fee").abs()),
        ("cost_of_ads_per_unit", ads.abs()),
        ("others_per_unit", num("platform_fee").abs()),
        ("cash_generated_per_unit", num("cost_of_unit_sold") + cm2),
        ("net_reimbursement_per_unit", reimbursement.abs()),
    ):
        result[field] = ratio(amount, net_units).round(2)
    return result
