from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Optional, Tuple
from typing import Any, Dict, List, Tuple
import re
import pandas as pd
from sqlalchemy.engine import Engine
from app.ai_agent.db import (MonthKey, fetch_non_total_rows, fetch_period_dfs, fetch_total_row, fetch_nse_month_df, latest_available_month, MetricDef, get_metric_def)



def safe_num(series_or_value) -> pd.Series | float:
    if isinstance(series_or_value, pd.Series):
        return pd.to_numeric(series_or_value, errors="coerce").fillna(0.0)
    return float(pd.to_numeric(pd.Series([series_or_value]), errors="coerce").fillna(0.0).iloc[0])


def _to_float(value: Any) -> float:
    return float(pd.to_numeric(pd.Series([value]), errors="coerce").fillna(0.0).iloc[0])


def _period_label(start_month: int, start_year: int, end_month: int, end_year: int) -> str:
    start_label = datetime(start_year, start_month, 1).strftime("%b %Y")
    end_label = datetime(end_year, end_month, 1).strftime("%b %Y")
    if start_label == end_label:
        return start_label
    return f"{start_label} to {end_label}"


def _validate_required_columns(df: pd.DataFrame, cols: list[str]) -> None:
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(f"missing required columns: {missing}")


def _ratio_value(numerator: float, denominator: float, multiplier: float = 100.0, absolute: bool = False) -> float:
    if denominator == 0:
        return 0.0
    value = (numerator / denominator) * multiplier
    if absolute:
        value = abs(value)
    return float(value)


def _monthly_total_value(df: pd.DataFrame, column: str) -> float:
    total_row = fetch_total_row(df)
    return _to_float(total_row.get(column, 0.0))


def _aggregate_total_rows(period_dfs: list[tuple[MonthKey, pd.DataFrame]], column: str) -> float:
    total = 0.0
    for _, df in period_dfs:
        total += _monthly_total_value(df, column)
    return float(total)


def _aggregate_sku_rows(period_dfs: list[tuple[MonthKey, pd.DataFrame]], column: str) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []

    for mk, df in period_dfs:
        work = fetch_non_total_rows(df)
        _validate_required_columns(work, ["sku", column])

        if "product_name" not in work.columns:
            work["product_name"] = ""

        part = work[["sku", "product_name", column]].copy()
        part[column] = pd.to_numeric(part[column], errors="coerce").fillna(0.0)
        part["sku"] = part["sku"].astype(str).str.strip()
        part["product_name"] = part["product_name"].astype(str).fillna("")
        part["month_label"] = mk.label
        frames.append(part)

    if not frames:
        return pd.DataFrame(columns=["sku", "product_name", "__metric__"])

    merged = pd.concat(frames, ignore_index=True)

    grouped = (
        merged.groupby(["sku"], as_index=False)
        .agg(
            product_name=("product_name", "first"),
            __metric__=(column, "sum"),
        )
        .sort_values("__metric__", ascending=False)
        .reset_index(drop=True)
    )
    return grouped


def _compute_ratio_single_month(df: pd.DataFrame, metric_def: MetricDef) -> float:
    total_row = fetch_total_row(df)
    numerator = _to_float(total_row.get(metric_def.numerator or "", 0.0))
    denominator = _to_float(total_row.get(metric_def.denominator or "", 0.0))
    return _ratio_value(
        numerator=numerator,
        denominator=denominator,
        multiplier=metric_def.multiplier,
        absolute=metric_def.absolute,
    )


def _compute_ratio_period(period_dfs: list[tuple[MonthKey, pd.DataFrame]], metric_def: MetricDef) -> float:
    numerator = _aggregate_total_rows(period_dfs, metric_def.numerator or "")
    denominator = _aggregate_total_rows(period_dfs, metric_def.denominator or "")
    return _ratio_value(
        numerator=numerator,
        denominator=denominator,
        multiplier=metric_def.multiplier,
        absolute=metric_def.absolute,
    )


def get_metric_for_month(
    engine: Engine,
    user_id: int,
    country: str,
    metric_name: str,
    month: int | str,
    year: int,
) -> Dict[str, Any]:

    metric_def = get_metric_def(metric_name)

    # 🔥 Normalize month
    if isinstance(month, str):
        if str(month).isdigit():
            month_int = int(month)
        else:
            month_int = datetime.strptime(str(month), "%B").month
    else:
        month_int = month

    # 🔥 Fetch data
    df = fetch_nse_month_df(engine, user_id, country, month_int, year)

    # 🔥 Build base result
    result: Dict[str, Any] = {
        "metric": metric_def.name,
        "column": metric_def.column,
        "metric_kind": metric_def.kind,
        "period_type": "single_month",
        "period_label": datetime(year, month_int, 1).strftime("%b %Y"),
    }

    # =========================
    # PRECOMPUTED SKU METRICS
    # =========================
    if metric_def.kind == "sku_precomputed":
        rows = fetch_non_total_rows(df).copy()

        if "product_name" not in rows.columns:
            rows["product_name"] = ""

        rows["sku"] = rows["sku"].astype(str).str.strip()
        rows["product_name"] = rows["product_name"].astype(str).fillna("")

        rows[metric_def.column] = pd.to_numeric(
            rows.get(metric_def.column, 0),
            errors="coerce"
        ).fillna(0.0)

        per_sku = rows[["sku", "product_name", metric_def.column]].copy()
        per_sku = per_sku.rename(columns={metric_def.column: "__metric__"})

        # 🔥 IMPORTANT: total comes from TOTAL ROW (NOT SUM)
        total_value = _monthly_total_value(df, metric_def.column)

        result["total"] = float(total_value)
        result["per_sku"] = per_sku.to_dict(orient="records")
        result["row_count"] = int(len(per_sku))

        return result

    # =========================
    # SKU LEVEL METRICS
    # =========================
    if metric_def.kind == "sku_additive":
        grouped = _aggregate_sku_rows(
            [(MonthKey(year=year, month=month_int), df)],
            metric_def.column
        )

        # 🔥 total (only meaningful for additive metrics like sales/profit)
        total_value = float(grouped["__metric__"].sum()) if not grouped.empty else 0.0

        result["total"] = total_value
        result["per_sku"] = grouped.to_dict(orient="records")
        result["row_count"] = int(len(grouped))

        return result

    # =========================
    # TOTAL LEVEL METRICS
    # =========================
    if metric_def.kind == "total_additive":
        total = _monthly_total_value(df, metric_def.column)

        result["total"] = total
        result["per_sku"] = []
        result["row_count"] = 1

        return result

    # =========================
    # SAFETY FALLBACK
    # =========================
    raise ValueError(f"unsupported metric kind: {metric_def.kind}")


def get_metric_for_period(
    engine: Engine,
    user_id: int,
    country: str,
    metric_name: str,
    start_month: int,
    start_year: int,
    end_month: int,
    end_year: int,
    *,
    skip_missing: bool = False,
) -> Dict[str, Any]:
    metric_def = get_metric_def(metric_name)
    period_dfs = fetch_period_dfs(
        engine=engine,
        user_id=user_id,
        country=country,
        start_month=start_month,
        start_year=start_year,
        end_month=end_month,
        end_year=end_year,
        skip_missing=skip_missing,
    )

    result: Dict[str, Any] = {
        "metric": metric_def.name,
        "column": metric_def.column,
        "metric_kind": metric_def.kind,
        "period_type": "range",
        "period_label": _period_label(start_month, start_year, end_month, end_year),
        "months_found": [mk.label for mk, _ in period_dfs],
    }

    # =========================
    # PRECOMPUTED SKU METRICS (PERIOD)
    # =========================
    if metric_def.kind == "sku_precomputed":
        frames = []

        for mk, df in period_dfs:
            rows = fetch_non_total_rows(df).copy()

            if "product_name" not in rows.columns:
                rows["product_name"] = ""

            rows["sku"] = rows["sku"].astype(str).str.strip()
            rows["product_name"] = rows["product_name"].astype(str).fillna("")

            rows[metric_def.column] = pd.to_numeric(
                rows.get(metric_def.column, 0),
                errors="coerce"
            ).fillna(0.0)

            part = rows[["sku", "product_name", metric_def.column]].copy()
            part["month_label"] = mk.label

            frames.append(part)

        if not frames:
            result["total"] = 0.0
            result["per_sku"] = []
            result["row_count"] = 0
            return result

        merged = pd.concat(frames, ignore_index=True)

        # 🔥 CRITICAL: average (NOT sum)
        grouped = (
            merged.groupby("sku", as_index=False)
            .agg(
                product_name=("product_name", "first"),
                __metric__=(metric_def.column, "mean"),
            )
        )

        totals = [
            _monthly_total_value(df, metric_def.column)
            for _, df in period_dfs
        ]

        result["total"] = float(sum(totals) / len(totals)) if totals else 0.0
        result["per_sku"] = grouped.to_dict(orient="records")
        result["row_count"] = len(grouped)

        return result

    if metric_def.kind == "sku_additive":
        grouped = _aggregate_sku_rows(period_dfs, metric_def.column)
        result["total"] = float(grouped["__metric__"].sum()) if not grouped.empty else 0.0
        result["per_sku"] = grouped.to_dict(orient="records")
        result["row_count"] = int(len(grouped))
        return result

    if metric_def.kind == "total_additive":
        total = _aggregate_total_rows(period_dfs, metric_def.column)
        result["total"] = total
        result["per_sku"] = []
        result["row_count"] = len(period_dfs)
        return result

    if metric_def.kind == "ratio":
        total = _compute_ratio_period(period_dfs, metric_def)
        result["total"] = total
        result["per_sku"] = []
        result["row_count"] = len(period_dfs)
        return result

    raise ValueError(f"unsupported metric kind: {metric_def.kind}")


def get_metric_for_quarter(
    engine: Engine,
    user_id: int,
    country: str,
    metric_name: str,
    year: int,
    quarter: int,
    *,
    skip_missing: bool = False,
) -> Dict[str, Any]:
    start_month = (quarter - 1) * 3 + 1
    end_month = start_month + 2
    return get_metric_for_period(
        engine=engine,
        user_id=user_id,
        country=country,
        metric_name=metric_name,
        start_month=start_month,
        start_year=year,
        end_month=end_month,
        end_year=year,
        skip_missing=skip_missing,
    )


def get_metric_for_year(
    engine: Engine,
    user_id: int,
    country: str,
    metric_name: str,
    year: int,
    *,
    skip_missing: bool = False,
) -> Dict[str, Any]:
    return get_metric_for_period(
        engine=engine,
        user_id=user_id,
        country=country,
        metric_name=metric_name,
        start_month=1,
        start_year=year,
        end_month=12,
        end_year=year,
        skip_missing=skip_missing,
    )


def get_metric_last_n_months(
    engine: Engine,
    user_id: int,
    country: str,
    metric_name: str,
    n: int,
    offset: int = 0,
) -> Dict[str, Any]:

    if not isinstance(n, int) or n <= 0:
        raise ValueError("n must be a positive integer")

    latest = latest_available_month(engine, user_id, country)

    months: list[MonthKey] = []

    y, m = latest.year, latest.month

    # 🔥 APPLY OFFSET
    for _ in range(offset):
        if m == 1:
            y -= 1
            m = 12
        else:
            m -= 1

    # 🔥 COLLECT MONTHS
    for _ in range(n):
        months.append(MonthKey(year=y, month=m))
        if m == 1:
            y -= 1
            m = 12
        else:
            m -= 1

    months.reverse()

    # 🔥 FETCH MONTH-BY-MONTH DATA
    per_period = []

    for mk in months:
        result = get_metric_for_month(
            engine=engine,
            user_id=user_id,
            country=country,
            metric_name=metric_name,
            month=mk.month,
            year=mk.year,
        )

        value = float(result.get("total", 0))

        per_period.append({
            "period_label": mk.label,
            "month": mk.month,
            "year": mk.year,
            "__metric__": value,
        })

    total = sum(x["__metric__"] for x in per_period)

    return {
        "metric": metric_name,
        "per_period": per_period,
        "total": total,
    }


def compare_periods(
    engine: Engine,
    user_id: int,
    country: str,
    metric_name: str,
    left_start_month: int,
    left_start_year: int,
    left_end_month: int,
    left_end_year: int,
    right_start_month: int,
    right_start_year: int,
    right_end_month: int,
    right_end_year: int,
    *,
    skip_missing: bool = False,
) -> Dict[str, Any]:
    left = get_metric_for_period(
        engine=engine,
        user_id=user_id,
        country=country,
        metric_name=metric_name,
        start_month=left_start_month,
        start_year=left_start_year,
        end_month=left_end_month,
        end_year=left_end_year,
        skip_missing=skip_missing,
    )
    right = get_metric_for_period(
        engine=engine,
        user_id=user_id,
        country=country,
        metric_name=metric_name,
        start_month=right_start_month,
        start_year=right_start_year,
        end_month=right_end_month,
        end_year=right_end_year,
        skip_missing=skip_missing,
    )

    left_total = float(left.get("total", 0.0))
    right_total = float(right.get("total", 0.0))
    delta = left_total - right_total
    pct_change = None if right_total == 0 else (delta / right_total) * 100.0

    return {
        "metric": metric_name,
        "column": left.get("column"),
        "metric_kind": left.get("metric_kind"),
        "left": {
            "label": left.get("period_label"),
            "total": left_total,
            "months_found": left.get("months_found", []),
        },
        "right": {
            "label": right.get("period_label"),
            "total": right_total,
            "months_found": right.get("months_found", []),
        },
        "delta": delta,
        "pct_change": pct_change,
    }


def pick_top_skus(metric_result: Dict[str, Any], n: int = 10, reverse: bool = True) -> List[Dict[str, Any]]:
    rows = list(metric_result.get("per_sku", []))
    rows.sort(key=lambda x: float(x.get("__metric__", 0.0)), reverse=reverse)
    return rows[:n]


def pick_bottom_skus(metric_result: Dict[str, Any], n: int = 10) -> List[Dict[str, Any]]:
    rows = list(metric_result.get("per_sku", []))
    rows.sort(key=lambda x: float(x.get("__metric__", 0.0)))
    return rows[:n]

OVERALL_MONTH_METRICS = [
    "total_quantity",
    "net_sales",
    "profit",
    "asp",
    "advertising_total",
    "platform_fee",
    "acos",
    "cm2_profit",
]

OVERALL_MONTH_METRICS_2 = [
    "asp",
    "advertising_total",
    "platform_fee",
    "acos",
    "cm2_profit",
]

PRODUCT_MONTH_METRICS = [
    "total_quantity",
    "net_sales",
    "profit",
    "asp",
]

def get_metric_pack_for_month(
    engine: Engine,
    user_id: int,
    country: str,
    metric_names: list[str],
    month: int,
    year: int,
) -> Dict[str, Any]:
    out = {
        "period_label": datetime(year, month, 1).strftime("%b %Y"),
        "metrics": {},
    }

    for metric_name in metric_names:
        result = get_metric_for_month(
            engine=engine,
            user_id=user_id,
            country=country,
            metric_name=metric_name,
            month=month,
            year=year,
        )
        out["metrics"][metric_name] = float(result.get("total", 0.0))

    return out

def get_last_n_month_keys(
    engine: Engine,
    user_id: int,
    country: str,
    n: int,
    offset: int = 0,
) -> list[MonthKey]:
    if not isinstance(n, int) or n <= 0:
        raise ValueError("n must be a positive integer")

    latest = latest_available_month(engine, user_id, country)

    months: list[MonthKey] = []
    y, m = latest.year, latest.month

    for _ in range(offset):
        if m == 1:
            y -= 1
            m = 12
        else:
            m -= 1

    for _ in range(n):
        months.append(MonthKey(year=y, month=m))
        if m == 1:
            y -= 1
            m = 12
        else:
            m -= 1

    months.reverse()
    return months

def get_product_metric_pack_for_month(
    engine: Engine,
    user_id: int,
    country: str,
    product_match: str,
    month: int,
    year: int,
) -> Dict[str, Any]:
    df = fetch_nse_month_df(engine, user_id, country, month, year)
    rows = fetch_non_total_rows(df).copy()

    if "product_name" not in rows.columns:
        rows["product_name"] = ""

    rows["product_name"] = rows["product_name"].astype(str).fillna("")
    matched = rows[
        rows["product_name"].str.lower().str.contains(product_match.lower(), na=False)
    ].copy()

    if matched.empty:
        raise ValueError(f"no product rows found for: {product_match}")

    total_quantity = pd.to_numeric(matched.get("total_quantity", 0), errors="coerce").fillna(0).sum()
    net_sales = pd.to_numeric(matched.get("net_sales", 0), errors="coerce").fillna(0).sum()
    profit = pd.to_numeric(matched.get("profit", 0), errors="coerce").fillna(0).sum()
    asp = 0.0 if total_quantity == 0 else float(net_sales / total_quantity)

    return {
        "period_label": datetime(year, month, 1).strftime("%b %Y"),
        "product_match": product_match,
        "metrics": {
            "total_quantity": float(total_quantity),
            "net_sales": float(net_sales),
            "profit": float(profit),
            "asp": float(asp),
        },
    }

def build_time_series_analysis(
    engine: Engine,
    user_id: int,
    country: str,
    metric_name: str,
    months: list[MonthKey],
    product_match: str | None = None,
) -> Dict[str, Any]:
    series: list[dict] = []

    for mk in months:
        result = get_metric_for_month(
            engine=engine,
            user_id=user_id,
            country=country,
            metric_name=metric_name,
            month=mk.month,
            year=mk.year,
        )

        if product_match:
            rows = result.get("per_sku", [])

            # ✅ check if exact match exists
            exact_rows = [
                r for r in rows
                if product_match.lower() == str(r.get("product_name", "")).lower()
            ]

            if exact_rows:
                value = sum(float(r.get("__metric__", 0.0)) for r in exact_rows)
            else:
                # ✅ fallback → group match
                group_rows = [
                    r for r in rows
                    if product_match.lower() in str(r.get("product_name", "")).lower()
                ]
                value = sum(float(r.get("__metric__", 0.0)) for r in group_rows)
        else:
            value = float(result.get("total", 0.0))

        series.append({
            "period_label": mk.label,
            "month": mk.month,
            "year": mk.year,
            "__metric__": float(value),
        })

    mom: list[dict] = []
    for i in range(1, len(series)):
        prev_val = float(series[i - 1]["__metric__"])
        curr_val = float(series[i]["__metric__"])
        delta = curr_val - prev_val
        pct_change = None if prev_val == 0 else (delta / prev_val) * 100.0

        mom.append({
            "period_label": series[i]["period_label"],
            "current": curr_val,
            "previous": prev_val,
            "delta": delta,
            "pct_change": pct_change,
        })

    values = [float(x["__metric__"]) for x in series]
    overall_delta = values[-1] - values[0] if len(values) >= 2 else 0.0
    overall_pct_change = None if len(values) < 2 or values[0] == 0 else (overall_delta / values[0]) * 100.0

    if mom:
        deltas = [x["delta"] for x in mom]
        if all(d > 0 for d in deltas):
            movement = "consistently_up"
        elif all(d < 0 for d in deltas):
            movement = "consistently_down"
        else:
            movement = "mixed"
    else:
        movement = "insufficient_data"

    return {
        "metric": metric_name,
        "series": series,
        "mom": mom,
        "overall_delta": overall_delta,
        "overall_pct_change": overall_pct_change,
        "movement": movement,
        "product_match": product_match,
    }

def get_growth_driver_insights(
    engine: Engine,
    user_id: int,
    country: str,
    metric_name: str,
    month: int,
    year: int,
    top_n: int = 3,
) -> Dict[str, Any] | None:
    current = get_metric_for_month(
        engine=engine,
        user_id=user_id,
        country=country,
        metric_name=metric_name,
        month=month,
        year=year,
    )

    if month == 1:
        prev_month = 12
        prev_year = year - 1
    else:
        prev_month = month - 1
        prev_year = year

    previous = get_metric_for_month(
        engine=engine,
        user_id=user_id,
        country=country,
        metric_name=metric_name,
        month=prev_month,
        year=prev_year,
    )

    curr_df = pd.DataFrame(current.get("per_sku", []))
    prev_df = pd.DataFrame(previous.get("per_sku", []))

    if curr_df.empty and prev_df.empty:
        return None

    if curr_df.empty:
        curr_df = pd.DataFrame(columns=["sku", "product_name", "__metric__"])
    if prev_df.empty:
        prev_df = pd.DataFrame(columns=["sku", "product_name", "__metric__"])

    merged = curr_df.merge(
        prev_df,
        on="sku",
        how="outer",
        suffixes=("_curr", "_prev"),
    ).fillna(0)

    if "product_name_curr" not in merged.columns:
        merged["product_name_curr"] = ""
    if "product_name_prev" not in merged.columns:
        merged["product_name_prev"] = ""

    merged["product_name"] = merged["product_name_curr"].replace("", pd.NA).fillna(merged["product_name_prev"])
    merged["delta"] = (
        pd.to_numeric(merged["__metric___curr"], errors="coerce").fillna(0.0)
        - pd.to_numeric(merged["__metric___prev"], errors="coerce").fillna(0.0)
    )

    positive = (
        merged.sort_values("delta", ascending=False)
        .head(top_n)[["sku", "product_name", "delta"]]
        .to_dict(orient="records")
    )
    negative = (
        merged.sort_values("delta", ascending=True)
        .head(top_n)[["sku", "product_name", "delta"]]
        .to_dict(orient="records")
    )

    lead = positive[0] if positive else None

    return {
        "metric": metric_name,
        "top_positive_drivers": positive,
        "top_negative_drivers": negative,
        "primary_driver": lead,
    }

def rank_skus(
    per_sku,
    direction="top",
    limit=5,
):
    if direction == "top":
        return pick_top_skus(per_sku, limit)
    else:
        return pick_bottom_skus(per_sku, limit)

def find_extreme_month(
    engine,
    user_id,
    country,
    metric_name,
    months,
    extreme_type="max",
    product_match=None,
):
    if not months:
        return None

    series = build_time_series_analysis(
        engine,
        user_id,
        country,
        metric_name,
        months,
        product_match=product_match,
    )

    rows = series.get("series", [])

    if not rows:
        return None

    if extreme_type == "max":
        best = max(rows, key=lambda x: x["__metric__"])
    else:
        best = min(rows, key=lambda x: x["__metric__"])

    return {
        "metric": metric_name,
        "extreme_type": extreme_type,
        "month": best.get("month"),
        "year": best.get("year"),
        "period_label": best.get("period_label"),
        "value": best.get("__metric__"),
    }

def get_metric_for_multiple_months(
    engine,
    user_id,
    country,
    metric_name,
    month_year_pairs,
    product_queries=None,
):
    results = []

    for item in month_year_pairs:
        res = get_metric_for_month(
            engine,
            user_id,
            country,
            metric_name,
            item["month"],
            item["year"],
        )

        if not res:
            continue

        # 🔥 APPLY PRODUCT FILTER HERE
        if product_queries:
            product_map = {}
            rows = res.get("per_sku", [])

            for pq in product_queries:
                pq_norm = str(pq).strip().lower()

                # 1. exact match first
                exact_rows = [
                    row for row in rows
                    if str(row.get("product_name", "")).strip().lower() == pq_norm
                ]

                if exact_rows:
                    product_map[pq] = sum(
                            float(row.get("__metric__", 0.0))
                            for row in exact_rows
                        )
                    continue

                # 2. fallback only if exact match does not exist
                contains_rows = [
                    row for row in rows
                    if pq_norm in str(row.get("product_name", "")).strip().lower()
                ]

                if contains_rows:
                    product_map[pq] = sum(
                        float(row.get("__metric__", 0.0))
                        for row in contains_rows
                    )

            res["product_breakdown"] = product_map

        results.append(res)

    return {
        "metric": metric_name,
        "months": results
    }


def get_multi_dimensional_data(
    engine,
    user_id,
    country,
    metric_names,
    months,
    product_queries=None,
):
    data = []

    for item in months:
        month = item["month"]
        year = item["year"]

        for metric in metric_names:
            result = get_metric_for_month(
                engine,
                user_id,
                country,
                metric,
                month,
                year,
            )

            rows = result.get("per_sku", [])

            # filter products if needed
            if product_queries:
                filtered = []
                for pq in product_queries:
                    for row in rows:
                        name = str(row.get("product_name", "")).lower()
                        if pq.lower() in name:
                            filtered.append((pq, row))
            else:
                filtered = [("all", r) for r in rows]

            for pq, row in filtered:
                data.append({
                    "month": result.get("period_label"),
                    "product": pq,
                    "metric": metric,
                    "value": float(row.get("__metric__", 0.0)),
                })

    return {
        "data": data,
        "metrics": metric_names,
        "products": product_queries,
        "months": [m for m in months],
    }

####period parser file########


MONTH_MAP = {
    "jan": 1, "january": 1,
    "feb": 2, "february": 2,
    "mar": 3, "march": 3,
    "apr": 4, "april": 4,
    "may": 5,
    "jun": 6, "june": 6,
    "jul": 7, "july": 7,
    "aug": 8, "august": 8,
    "sep": 9, "september": 9,
    "oct": 10, "october": 10,
    "nov": 11, "november": 11,
    "dec": 12, "december": 12,
}


# -------------------------------
# HELPERS
# -------------------------------

def normalize_text(text: str) -> str:
    return re.sub(r"\s+", " ", text.lower().strip())


def extract_year(text: str) -> Optional[int]:
    match = re.search(r"\b(20\d{2}|\d{2})\b", text)
    if not match:
        return None

    year = int(match.group(1))

    # convert short year (26 → 2026)
    if year < 100:
        year += 2000

    return year

def extract_month(text: str) -> Optional[int]:
    for k, v in MONTH_MAP.items():
        if re.search(rf"\b{k}\b", text):
            return v
    return None


def extract_month_year(text: str, default_year: Optional[int] = None) -> Optional[Tuple[int, int]]:
    year = extract_year(text) or default_year
    month = extract_month(text)
    if year and month:
        return year, month
    return None


# -------------------------------
# QUARTER
# -------------------------------

def parse_quarter(text: str) -> Optional[Tuple[int, int, int]]:
    text = text.lower()

    # -------------------------------
    # STANDARD Q FORMAT (q1 2026)
    # -------------------------------
    match = re.search(r"\bq([1-4])\s*(20\d{2})?\b", text)
    if match:
        q = int(match.group(1))
        year = int(match.group(2)) if match.group(2) else extract_year(text)

        if not year:
            return None

        start_month = (q - 1) * 3 + 1
        end_month = start_month + 2
        return year, start_month, end_month

    # -------------------------------
    # TEXTUAL QUARTERS
    # -------------------------------
    quarter_map = {
        "first quarter": 1,
        "1st quarter": 1,
        "second quarter": 2,
        "2nd quarter": 2,
        "third quarter": 3,
        "3rd quarter": 3,
        "fourth quarter": 4,
        "4th quarter": 4,
    }

    for phrase, q in quarter_map.items():
        if phrase in text:
            year = extract_year(text)
            if not year:
                return None

            start_month = (q - 1) * 3 + 1
            end_month = start_month + 2
            return year, start_month, end_month

    # -------------------------------
    # RELATIVE QUARTERS
    # -------------------------------
    today = datetime.today()
    current_q = (today.month - 1) // 3 + 1
    year = today.year

    if "this quarter" in text:
        q = current_q
    elif "last quarter" in text:
        q = current_q - 1
        if q == 0:
            q = 4
            year -= 1
    else:
        return None

    start_month = (q - 1) * 3 + 1
    end_month = start_month + 2

    return year, start_month, end_month
# -------------------------------
# LAST N MONTHS
# -------------------------------

def parse_last_n(text: str) -> Optional[int]:
    match = re.search(r"last\s+(\d+)\s+months?", text)
    if match:
        return int(match.group(1))

    if "last month" in text:
        return 1

    return None


# -------------------------------
# RANGE
# -------------------------------

def parse_range(text: str):
    parts = re.split(r"\bto\b|\s*-\s*", text)

    if len(parts) != 2:
        return None

    left = extract_month_year(parts[0])
    right = extract_month_year(parts[1])

    if left and right:
        start_year, start_month = left
        end_year, end_month = right

        months = []
        cur_month = start_month
        cur_year = start_year

        while (cur_year < end_year) or (cur_year == end_year and cur_month <= end_month):
            months.append({
                "month": cur_month,
                "year": cur_year
            })

            cur_month += 1
            if cur_month > 12:
                cur_month = 1
                cur_year += 1

        return {
            "type": "multi_month",
            "months": months
        }

    return None


# -------------------------------
# COMPARISON
# -------------------------------

def parse_comparison(text: str):
    if "vs" not in text:
        return None

    left, right = text.split("vs", 1)

    # month vs month
    inferred_year = extract_year(text) or datetime.today().year

    m1 = extract_month_year(left, default_year=inferred_year)
    m2 = extract_month_year(right, default_year=inferred_year)

    if m1 and m2:
        return {
            "type": "comparison",
            "left": {
                "start_month": m1[1],
                "start_year": m1[0],
                "end_month": m1[1],
                "end_year": m1[0],
            },
            "right": {
                "start_month": m2[1],
                "start_year": m2[0],
                "end_month": m2[1],
                "end_year": m2[0],
            },
        }

    # quarter vs quarter
    q1 = parse_quarter(left)
    q2 = parse_quarter(right)

    if q1 and q2:
        return {
            "type": "comparison",
            "left": {
                "start_month": q1[1],
                "start_year": q1[0],
                "end_month": q1[2],
                "end_year": q1[0],
            },
            "right": {
                "start_month": q2[1],
                "start_year": q2[0],
                "end_month": q2[2],
                "end_year": q2[0],
            },
        }

    # year vs year
    y1 = extract_year(left)
    y2 = extract_year(right)

    if y1 and y2:
        return {
            "type": "comparison",
            "left": {
                "start_month": 1,
                "start_year": y1,
                "end_month": 12,
                "end_year": y1,
            },
            "right": {
                "start_month": 1,
                "start_year": y2,
                "end_month": 12,
                "end_year": y2,
            },
        }

    return None

# -------------------------------
# SINGLE PERIOD
# -------------------------------

def parse_single(text: str):
    # month-year
    my = extract_month_year(text)
    if my:
        return {
            "type": "single",
            "month": my[1],
            "year": my[0],
        }

    # 🔥 FIX: quarter BEFORE year
    q = parse_quarter(text)
    if q:
        return {
            "type": "range",
            "start_month": q[1],
            "start_year": q[0],
            "end_month": q[2],
            "end_year": q[0],
        }

    # year
    year = extract_year(text)
    if year:
        return {
            "type": "year",
            "year": year,
        }

    return None


# -------------------------------
# MAIN ENTRY
# -------------------------------

def parse_period(query: str) -> Dict:
    text = normalize_text(query)

    # -------- RANGE DETECTION (FIXED) --------
    rng = parse_range(text)
    if rng:
        return rng

    # -------- MULTI MONTH (LIST, NOT RANGE) --------
    month_matches = re.findall(
        r"(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)",
        text
    )

    year = extract_year(text)

    # only treat as multi-month if NO range words
    if len(month_matches) >= 2 and not re.search(r"\b(to|from|-)\b", text):
        months = []
        for m in month_matches:
            months.append({
                "month": MONTH_MAP.get(m[:3]),
                "year": year or datetime.today().year
            })

        return {
            "type": "multi_month",
            "months": months
        }

    # 1️⃣ comparison first (highest priority)
    cmp = parse_comparison(text)
    if cmp:
        return cmp

    # 2️⃣ relative periods
    rel = parse_relative_period(text)
    if rel:
        return rel

    # 3️⃣ last n months
    last_n = parse_last_n(text)
    if last_n:
        return {
            "type": "last_n",
            "n": last_n,
        }

    # 4️⃣ range
    rng = parse_range(text)
    if rng:
        return rng

    # 5️⃣ single
    single = parse_single(text)
    if single:
        return single

    # 6️⃣ fallback
    return {
        "type": "latest_month"
    }

def parse_relative_period(text: str):
    today = datetime.today()

    # THIS MONTH
    if "this month" in text:
        return {
            "type": "single",
            "month": today.month,
            "year": today.year,
        }

    # LAST MONTH
    if "last month" in text:
        month = today.month - 1
        year = today.year
        if month == 0:
            month = 12
            year -= 1
        return {
            "type": "single",
            "month": month,
            "year": year,
        }

    # THIS YEAR
    if "this year" in text:
        return {
            "type": "year",
            "year": today.year,
        }

    # LAST YEAR
    if "last year" in text:
        return {
            "type": "year",
            "year": today.year - 1,
        }

    # YTD
    if "ytd" in text or "year to date" in text:
        return {
            "type": "range",
            "start_month": 1,
            "start_year": today.year,
            "end_month": today.month,
            "end_year": today.year,
        }

    return None