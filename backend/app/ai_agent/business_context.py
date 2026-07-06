from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional

import pandas as pd
from sqlalchemy.engine import Engine

from app.ai_agent.db import (
    INVENTORY_METRICS,
    MonthKey,
    fetch_non_total_rows,
    fetch_nse_month_df,
    fetch_period_dfs,
    fetch_total_row,
    get_inventory_snapshot,
    latest_available_month,
)
from app.ai_agent.formula_engine import get_last_n_month_keys


CORE_BUSINESS_COLUMNS = [
    "quantity",
    "return_quantity",
    "total_quantity",
    "asp",
    "gross_sales",
    "net_sales",
    "product_sales",
    "refund_sales",
    "promotional_rebates",
    "cogs",
    "selling_fees",
    "fba_fees",
    "marketplace_fees",
    "amazon_fees",
    "platform_fee",
    "platform_fee_inventory_storage",
    "tax_and_credits",
    "other",
    "profit",
    "profit_percentage",
    "cm2_profit",
    "total_cm2_profit",
    "cm2_profit_per",
    "total_cm2_margins",
    "ads_spend",
    "total_ads",
    "product_spend",
    "display_spend",
    "brand_spend",
    "ads_impressions",
    "ads_clicks",
    "ads_sale_units",
    "ads_sale_amount",
    "ads_roas",
    "ads_acos",
    "tacos_total_advertising_cost_of_sale",
    "lost_total",
    "misc_transaction",
    "debt_payment",
    "disbursement",
    "current_net_reimbursement",
]

ADDITIVE_TOTAL_COLUMNS = {
    "quantity",
    "return_quantity",
    "total_quantity",
    "gross_sales",
    "net_sales",
    "product_sales",
    "refund_sales",
    "promotional_rebates",
    "cogs",
    "selling_fees",
    "fba_fees",
    "marketplace_fees",
    "amazon_fees",
    "platform_fee",
    "platform_fee_inventory_storage",
    "tax_and_credits",
    "other",
    "profit",
    "cm2_profit",
    "total_cm2_profit",
    "ads_spend",
    "total_ads",
    "product_spend",
    "display_spend",
    "brand_spend",
    "ads_impressions",
    "ads_clicks",
    "ads_sale_units",
    "ads_sale_amount",
    "lost_total",
    "misc_transaction",
    "debt_payment",
    "disbursement",
    "current_net_reimbursement",
}


def _safe_float(value: Any) -> float:
    if value is None:
        return 0.0
    if isinstance(value, str) and value.strip().upper() in {"", "NULL", "NONE", "NAN"}:
        return 0.0
    try:
        return float(pd.to_numeric(pd.Series([value]), errors="coerce").fillna(0.0).iloc[0])
    except Exception:
        return 0.0


def _safe_div(numerator: float, denominator: float, multiplier: float = 1.0) -> Optional[float]:
    if denominator == 0:
        return None
    return (numerator / denominator) * multiplier


def _round(value: Any, digits: int = 4) -> Optional[float]:
    if value is None:
        return None
    try:
        return round(float(value), digits)
    except Exception:
        return None


def _clean_record(record: Dict[str, Any]) -> Dict[str, Any]:
    cleaned: Dict[str, Any] = {}
    for key, value in record.items():
        if isinstance(value, float):
            cleaned[key] = _round(value)
        elif (pd.isna(value) if not isinstance(value, (str, int, bool, type(None))) else False):
            cleaned[key] = None
        else:
            cleaned[key] = value
    return cleaned


def _period_label(months: List[MonthKey]) -> str:
    if not months:
        return "Latest period"
    if len(months) == 1:
        return months[0].label
    return f"{months[0].label} to {months[-1].label}"


def _months_from_payload(
    engine: Engine,
    user_id: int,
    country: str,
    payload: Optional[Dict[str, Any]],
) -> List[MonthKey]:
    payload = payload or {}
    ptype = payload.get("type")

    if ptype == "single" and payload.get("month") and payload.get("year"):
        return [MonthKey(year=int(payload["year"]), month=int(payload["month"]))]

    if ptype == "last_n_months":
        return get_last_n_month_keys(engine, user_id, country, int(payload.get("n") or 6))

    if ptype == "range":
        period_dfs = fetch_period_dfs(
            engine=engine,
            user_id=user_id,
            country=country,
            start_month=int(payload["start_month"]),
            start_year=int(payload["start_year"]),
            end_month=int(payload["end_month"]),
            end_year=int(payload["end_year"]),
            skip_missing=True,
        )
        return [month_key for month_key, _ in period_dfs]

    latest = latest_available_month(engine, user_id, country)
    return [latest]


def _load_period_frames(
    engine: Engine,
    user_id: int,
    country: str,
    months: Iterable[MonthKey],
) -> List[tuple[MonthKey, pd.DataFrame]]:
    frames: List[tuple[MonthKey, pd.DataFrame]] = []
    for month_key in months:
        try:
            frames.append(
                (
                    month_key,
                    fetch_nse_month_df(
                        engine,
                        user_id,
                        country,
                        month_key.month,
                        month_key.year,
                    ),
                )
            )
        except Exception:
            continue
    return frames


def _totals_from_frames(period_frames: List[tuple[MonthKey, pd.DataFrame]]) -> Dict[str, float]:
    totals: Dict[str, float] = {column: 0.0 for column in CORE_BUSINESS_COLUMNS}

    for _, frame in period_frames:
        try:
            total_row = fetch_total_row(frame)
        except Exception:
            total_row = pd.Series(dtype="object")

        sku_rows = fetch_non_total_rows(frame)

        for column in CORE_BUSINESS_COLUMNS:
            if column in ADDITIVE_TOTAL_COLUMNS:
                source = total_row.get(column, None) if column in total_row else None
                if source is None and column in sku_rows.columns:
                    source = pd.to_numeric(sku_rows[column], errors="coerce").fillna(0.0).sum()
                totals[column] += _safe_float(source)
            elif column in total_row:
                totals[column] = _safe_float(total_row.get(column))

    net_sales = totals.get("net_sales", 0.0)
    units = totals.get("total_quantity", 0.0)
    ads_spend = totals.get("ads_spend", 0.0) or totals.get("total_ads", 0.0)
    ads_sale_amount = totals.get("ads_sale_amount", 0.0)
    profit = totals.get("profit", 0.0)
    cm2_profit = totals.get("total_cm2_profit", 0.0) or totals.get("cm2_profit", 0.0)

    totals["asp"] = _safe_div(net_sales, units) or totals.get("asp", 0.0)
    totals["profit_percentage"] = _safe_div(profit, net_sales, 100.0) or totals.get("profit_percentage", 0.0)
    totals["cm2_profit_per"] = _safe_div(cm2_profit, net_sales, 100.0) or totals.get("cm2_profit_per", 0.0)
    totals["ads_roas"] = _safe_div(ads_sale_amount, ads_spend) or totals.get("ads_roas", 0.0)
    totals["ads_acos"] = _safe_div(ads_spend, ads_sale_amount, 100.0) or totals.get("ads_acos", 0.0)
    totals["tacos_total_advertising_cost_of_sale"] = (
        _safe_div(ads_spend, net_sales, 100.0)
        or totals.get("tacos_total_advertising_cost_of_sale", 0.0)
    )
    totals["ads_conversion_rate"] = _safe_div(
        totals.get("ads_sale_units", 0.0),
        totals.get("ads_clicks", 0.0),
        100.0,
    ) or 0.0
    totals["ads_ctr"] = _safe_div(totals.get("ads_clicks", 0.0), totals.get("ads_impressions", 0.0), 100.0) or 0.0
    totals["ads_cpc"] = _safe_div(ads_spend, totals.get("ads_clicks", 0.0)) or 0.0
    totals["return_rate"] = _safe_div(totals.get("return_quantity", 0.0), totals.get("quantity", 0.0), 100.0) or 0.0
    totals["fee_ratio"] = _safe_div(
        abs(totals.get("selling_fees", 0.0)) + abs(totals.get("fba_fees", 0.0)) + abs(totals.get("platform_fee", 0.0)),
        net_sales,
        100.0,
    ) or 0.0
    return {key: _round(value) or 0.0 for key, value in totals.items()}


def _sku_frame(period_frames: List[tuple[MonthKey, pd.DataFrame]]) -> pd.DataFrame:
    parts: List[pd.DataFrame] = []

    for month_key, frame in period_frames:
        rows = fetch_non_total_rows(frame).copy()
        rows["period_label"] = month_key.label
        for column in CORE_BUSINESS_COLUMNS:
            if column in rows.columns:
                rows[column] = pd.to_numeric(rows[column], errors="coerce").fillna(0.0)
        if "product_name" not in rows.columns:
            rows["product_name"] = ""
        rows["sku"] = rows["sku"].astype(str)
        rows["product_name"] = rows["product_name"].astype(str)
        parts.append(rows)

    if not parts:
        return pd.DataFrame(columns=["sku", "product_name"])

    raw = pd.concat(parts, ignore_index=True)
    agg_map: Dict[str, Any] = {"product_name": ("product_name", "first")}
    for column in CORE_BUSINESS_COLUMNS:
        if column in raw.columns:
            if column in ADDITIVE_TOTAL_COLUMNS:
                agg_map[column] = (column, "sum")
            else:
                agg_map[column] = (column, "mean")

    grouped = raw.groupby("sku", as_index=False).agg(**agg_map)
    return _add_sku_derived_metrics(grouped)


def _add_sku_derived_metrics(rows: pd.DataFrame) -> pd.DataFrame:
    for column in CORE_BUSINESS_COLUMNS:
        if column not in rows.columns:
            rows[column] = 0.0

    rows["profit_margin_pct"] = rows.apply(
        lambda r: _safe_div(_safe_float(r["profit"]), _safe_float(r["net_sales"]), 100.0) or 0.0,
        axis=1,
    )
    rows["cm2_margin_pct"] = rows.apply(
        lambda r: _safe_div(_safe_float(r["cm2_profit"]), _safe_float(r["net_sales"]), 100.0) or 0.0,
        axis=1,
    )
    rows["ad_to_sales_pct"] = rows.apply(
        lambda r: _safe_div(_safe_float(r["ads_spend"]), _safe_float(r["net_sales"]), 100.0) or 0.0,
        axis=1,
    )
    rows["ad_roas"] = rows.apply(
        lambda r: _safe_div(_safe_float(r["ads_sale_amount"]), _safe_float(r["ads_spend"])) or 0.0,
        axis=1,
    )
    rows["ad_acos_pct"] = rows.apply(
        lambda r: _safe_div(_safe_float(r["ads_spend"]), _safe_float(r["ads_sale_amount"]), 100.0) or 0.0,
        axis=1,
    )
    rows["ctr_pct"] = rows.apply(
        lambda r: _safe_div(_safe_float(r["ads_clicks"]), _safe_float(r["ads_impressions"]), 100.0) or 0.0,
        axis=1,
    )
    rows["cpc"] = rows.apply(
        lambda r: _safe_div(_safe_float(r["ads_spend"]), _safe_float(r["ads_clicks"])) or 0.0,
        axis=1,
    )
    rows["ad_conversion_rate_pct"] = rows.apply(
        lambda r: _safe_div(_safe_float(r["ads_sale_units"]), _safe_float(r["ads_clicks"]), 100.0) or 0.0,
        axis=1,
    )
    rows["fee_ratio_pct"] = rows.apply(
        lambda r: _safe_div(
            abs(_safe_float(r["selling_fees"])) + abs(_safe_float(r["fba_fees"])) + abs(_safe_float(r["platform_fee"])),
            _safe_float(r["net_sales"]),
            100.0,
        ) or 0.0,
        axis=1,
    )
    rows["return_rate_pct"] = rows.apply(
        lambda r: _safe_div(_safe_float(r["return_quantity"]), _safe_float(r["quantity"]), 100.0) or 0.0,
        axis=1,
    )
    return rows


def _records(frame: pd.DataFrame, columns: List[str], limit: int = 5) -> List[Dict[str, Any]]:
    existing = [column for column in columns if column in frame.columns]
    if not existing:
        return []
    out: List[Dict[str, Any]] = []
    for record in frame[existing].head(limit).to_dict(orient="records"):
        out.append(_clean_record(record))
    return out


def _rankings(sku_rows: pd.DataFrame) -> Dict[str, List[Dict[str, Any]]]:
    if sku_rows.empty:
        return {}

    base_cols = [
        "sku",
        "product_name",
        "total_quantity",
        "net_sales",
        "profit",
        "cm2_profit",
        "profit_margin_pct",
        "cm2_margin_pct",
        "ads_spend",
        "ads_sale_amount",
        "ad_to_sales_pct",
        "ad_roas",
        "ad_acos_pct",
        "fee_ratio_pct",
        "return_rate_pct",
    ]

    inefficient_ads = sku_rows[
        (sku_rows["ads_spend"] > 0)
        & ((sku_rows["cm2_profit"] <= 0) | (sku_rows["ads_sale_amount"] <= 0))
    ].sort_values(["ads_spend", "ad_to_sales_pct"], ascending=[False, False])

    conversion_gaps = sku_rows[
        (sku_rows["ads_clicks"] > 0)
        & (sku_rows["ads_sale_units"] <= 0)
    ].sort_values(["ads_clicks", "ads_spend"], ascending=[False, False])

    profitable_low_ad = sku_rows[
        (sku_rows["cm2_profit"] > 0)
        & (sku_rows["net_sales"] > 0)
    ].sort_values(["ad_to_sales_pct", "cm2_profit"], ascending=[True, False])

    return {
        "top_sales": _records(sku_rows.sort_values("net_sales", ascending=False), base_cols),
        "top_profit": _records(sku_rows.sort_values("profit", ascending=False), base_cols),
        "top_cm2_profit": _records(sku_rows.sort_values("cm2_profit", ascending=False), base_cols),
        "weak_profit": _records(sku_rows.sort_values("profit", ascending=True), base_cols),
        "weak_cm2_profit": _records(sku_rows.sort_values("cm2_profit", ascending=True), base_cols),
        "highest_fee_burden": _records(sku_rows.sort_values("fee_ratio_pct", ascending=False), base_cols),
        "highest_return_rate": _records(sku_rows.sort_values("return_rate_pct", ascending=False), base_cols),
        "highest_ad_burden": _records(sku_rows[sku_rows["ads_spend"] > 0].sort_values("ad_to_sales_pct", ascending=False), base_cols),
        "inefficient_ad_spend": _records(inefficient_ads, base_cols),
        "ad_conversion_gaps": _records(conversion_gaps, base_cols + ["ads_clicks", "ads_impressions", "ads_sale_units"]),
        "profitable_low_ad_support": _records(profitable_low_ad, base_cols),
    }


def _history(
    engine: Engine,
    user_id: int,
    country: str,
    selected_months: List[MonthKey],
    periods: int = 6,
) -> Dict[str, Any]:
    try:
        latest = selected_months[-1] if selected_months else latest_available_month(engine, user_id, country)
        all_months = get_last_n_month_keys(engine, user_id, country, periods)
        months = [month_key for month_key in all_months if (month_key.year, month_key.month) <= (latest.year, latest.month)]
        frames = _load_period_frames(engine, user_id, country, months)
    except Exception:
        return {"months": [], "movement": {}, "note": "Trend history is unavailable."}

    monthly_rows: List[Dict[str, Any]] = []
    for month_key, frame in frames:
        totals = _totals_from_frames([(month_key, frame)])
        monthly_rows.append(
            {
                "month": month_key.month,
                "year": month_key.year,
                "period_label": month_key.label,
                "net_sales": totals.get("net_sales", 0.0),
                "profit": totals.get("profit", 0.0),
                "cm2_profit": totals.get("cm2_profit", 0.0),
                "total_quantity": totals.get("total_quantity", 0.0),
                "ads_spend": totals.get("ads_spend", 0.0),
                "fee_ratio": totals.get("fee_ratio", 0.0),
                "return_rate": totals.get("return_rate", 0.0),
            }
        )

    movement: Dict[str, Any] = {}
    if len(monthly_rows) >= 2:
        previous = monthly_rows[-2]
        current = monthly_rows[-1]
        for metric in ["net_sales", "profit", "cm2_profit", "total_quantity", "ads_spend"]:
            delta = _safe_float(current.get(metric)) - _safe_float(previous.get(metric))
            movement[metric] = {
                "from": previous.get(metric),
                "to": current.get(metric),
                "delta": _round(delta),
                "pct_change": _round(_safe_div(delta, _safe_float(previous.get(metric)), 100.0)),
            }

    return {"months": monthly_rows, "movement": movement}


def _inventory_context(user_id: int, month_key: MonthKey, metric_name: Optional[str], user_query: str) -> Dict[str, Any]:
    query = (user_query or "").lower()
    wants_inventory = (
        metric_name in INVENTORY_METRICS
        or any(word in query for word in ["stock", "inventory", "coverage", "sell through", "days of supply", "reserved"])
    )
    if not wants_inventory:
        return {"requested": False}

    metrics = [
        "available",
        "inbound_quantity",
        "total_reserved_quantity",
        "unfulfillable_quantity",
        "units_shipped_t30",
        "units_shipped_t60",
        "units_shipped_t90",
        "sell_through",
        "days_of_supply",
        "estimated_excess_quantity",
    ]
    snapshots: Dict[str, Any] = {}
    for metric in metrics:
        try:
            snapshots[metric] = get_inventory_snapshot(user_id, metric, month_key.month, month_key.year)
        except Exception:
            snapshots[metric] = {"metric": metric, "note": "Unavailable"}
    return {"requested": True, "period_label": month_key.label, "snapshots": snapshots}


def _focus_products(sku_rows: pd.DataFrame, product_query: Optional[str]) -> List[Dict[str, Any]]:
    if not product_query or sku_rows.empty:
        return []
    query = product_query.strip().lower()
    matched = sku_rows[
        sku_rows["product_name"].astype(str).str.lower().str.contains(query, na=False)
        | sku_rows["sku"].astype(str).str.lower().str.contains(query, na=False)
    ]
    return _records(
        matched.sort_values("net_sales", ascending=False),
        [
            "sku",
            "product_name",
            "total_quantity",
            "net_sales",
            "profit",
            "cm2_profit",
            "profit_margin_pct",
            "cm2_margin_pct",
            "ads_spend",
            "ads_sale_amount",
            "ad_to_sales_pct",
            "ad_roas",
            "fee_ratio_pct",
            "return_rate_pct",
        ],
        limit=10,
    )


def build_business_context(
    *,
    engine: Engine,
    user_id: int,
    country: str,
    period_payload: Optional[Dict[str, Any]],
    user_query: str,
    metric_name: Optional[str] = None,
    metric_names: Optional[List[str]] = None,
    product_query: Optional[str] = None,
) -> Dict[str, Any]:
    months = _months_from_payload(engine, user_id, country, period_payload)
    period_frames = _load_period_frames(engine, user_id, country, months)
    if not period_frames:
        latest = latest_available_month(engine, user_id, country)
        period_frames = _load_period_frames(engine, user_id, country, [latest])
        months = [latest]

    loaded_months = [month_key for month_key, _ in period_frames]
    totals = _totals_from_frames(period_frames)
    sku_rows = _sku_frame(period_frames)
    selected_metric_names = [metric for metric in (metric_names or []) if metric]
    if metric_name and metric_name not in selected_metric_names:
        selected_metric_names.insert(0, metric_name)

    latest_month = loaded_months[-1] if loaded_months else latest_available_month(engine, user_id, country)
    available_columns = sorted({column for _, frame in period_frames for column in frame.columns})

    return {
        "scope": {
            "user_id": user_id,
            "country": country,
            "question": user_query,
            "metric_name": metric_name,
            "metric_names": selected_metric_names,
            "product_query": product_query,
        },
        "period": {
            "label": _period_label(loaded_months),
            "months": [
                {"month": month_key.month, "year": month_key.year, "label": month_key.label}
                for month_key in loaded_months
            ],
            "requested_payload": period_payload or {},
        },
        "available_columns": available_columns,
        "totals": totals,
        "derived": {
            "profit_margin_pct": totals.get("profit_percentage", 0.0),
            "cm2_margin_pct": totals.get("cm2_profit_per", 0.0),
            "ad_to_sales_pct": totals.get("tacos_total_advertising_cost_of_sale", 0.0),
            "ad_roas": totals.get("ads_roas", 0.0),
            "ad_acos_pct": totals.get("ads_acos", 0.0),
            "ctr_pct": totals.get("ads_ctr", 0.0),
            "cpc": totals.get("ads_cpc", 0.0),
            "ad_conversion_rate_pct": totals.get("ads_conversion_rate", 0.0),
            "fee_ratio_pct": totals.get("fee_ratio", 0.0),
            "return_rate_pct": totals.get("return_rate", 0.0),
            "ads_spend_reconciliation": {
                "ads_spend": totals.get("ads_spend", 0.0),
                "product_plus_display_plus_brand": _round(
                    totals.get("product_spend", 0.0)
                    + totals.get("display_spend", 0.0)
                    + totals.get("brand_spend", 0.0)
                ),
                "product_spend": totals.get("product_spend", 0.0),
                "display_spend": totals.get("display_spend", 0.0),
                "brand_spend": totals.get("brand_spend", 0.0),
            },
        },
        "rankings": _rankings(sku_rows),
        "focus_products": _focus_products(sku_rows, product_query),
        "history": _history(engine, user_id, country, loaded_months),
        "inventory": _inventory_context(user_id, latest_month, metric_name, user_query),
        "data_quality": {
            "row_count": int(len(sku_rows)),
            "has_sku_rows": bool(not sku_rows.empty),
            "notes": [
                "Use general ecommerce guidance only when the required metric is absent or has no rows.",
                "Channel-wise ad spend is available through product_spend, display_spend, and brand_spend. Channel-wise ad sales should not be inferred unless matching sales columns exist.",
            ],
        },
    }
