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
    *,
    include_current_incomplete: bool = False,
) -> Dict[str, Any]:

    if not isinstance(n, int) or n <= 0:
        raise ValueError("n must be a positive integer")

    latest = latest_available_month(engine, user_id, country)
    offset += _offset_for_completed_months(latest, include_current_incomplete)

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
    *,
    include_current_incomplete: bool = False,
) -> list[MonthKey]:
    if not isinstance(n, int) or n <= 0:
        raise ValueError("n must be a positive integer")

    latest = latest_available_month(engine, user_id, country)
    offset += _offset_for_completed_months(latest, include_current_incomplete)

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
    time_unit: str | None = None,  # 🔥 NEW
) -> Dict[str, Any]:

    series: list[dict] = []

    # -------- BUILD RAW MONTHLY SERIES --------
    for mk in months:
        result = get_metric_for_month(
            engine=engine,
            user_id=user_id,
            country=country,
            metric_name=metric_name,
            month=mk.month,
            year=mk.year,
        )

        # -------- PRODUCT FILTER --------
        if product_match:
            rows = result.get("per_sku", [])

            matched_row = next(
                (
                    r for r in rows
                    if str(r.get("product_name", "")).strip().lower()
                    == product_match.strip().lower()
                ),
                None,
            )

            value = float(matched_row.get("__metric__", 0.0)) if matched_row else 0.0

        else:
            value = float(result.get("total", 0.0))

        series.append({
            "period_label": mk.label,
            "month": mk.month,
            "year": mk.year,
            "__metric__": float(value),
        })

    # =========================================================
    # 🔥 CONDITIONAL AGGREGATION (CORE FIX)
    # =========================================================

    if time_unit == "quarter" and len(series) >= 3:
        grouped = []

        for i in range(0, len(series), 3):
            chunk = series[i:i+3]

            if len(chunk) < 3:
                continue

            total = sum(x["__metric__"] for x in chunk)

            year = chunk[-1]["year"]
            month = chunk[-1]["month"]
            q = (month - 1) // 3 + 1

            grouped.append({
                "period_label": f"Q{q} {year}",
                "__metric__": total,
                "year": year,
                "quarter": q,
            })

        series = grouped

    elif time_unit == "year" and len(series) >= 12:
        grouped = {}

        for item in series:
            y = item["year"]
            grouped.setdefault(y, 0.0)
            grouped[y] += item["__metric__"]

        series = [
            {
                "period_label": str(y),
                "__metric__": v,
                "year": y,
            }
            for y, v in sorted(grouped.items())
        ]

    # =========================================================
    # 🔥 MOM CALCULATION (AFTER aggregation if any)
    # =========================================================

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

    # -------- OVERALL CHANGE --------
    values = [float(x["__metric__"]) for x in series]

    overall_delta = values[-1] - values[0] if len(values) >= 2 else 0.0
    overall_pct_change = (
        None if len(values) < 2 or values[0] == 0
        else (overall_delta / values[0]) * 100.0
    )

    # -------- MOVEMENT --------
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

            # -------- 🔥 FIXED PRODUCT FILTER --------
            if product_queries:
                for pq in product_queries:
                    pq_clean = pq.strip().lower()

                    matched = False

                    for row in rows:
                        name = str(row.get("product_name", "")).strip().lower()

                        # ✅ STRICT MATCH ONLY
                        if name == pq_clean:
                            data.append({
                                "month": result.get("period_label"),
                                "product": row.get("product_name"),
                                "metric": metric,
                                "value": float(row.get("__metric__", 0.0)),
                            })
                            matched = True
                            break  # 🔥 STOP after first match

                    # optional: handle no match case
                    if not matched:
                        # you can skip or add 0
                        pass

            else:
                # fallback (no product filter)
                for row in rows:
                    data.append({
                        "month": result.get("period_label"),
                        "product": row.get("product_name"),
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

PERIOD_NUMBER_WORDS = {
    "one": 1, "first": 1, "1st": 1,
    "two": 2, "second": 2, "2nd": 2,
    "three": 3, "third": 3, "3rd": 3,
    "four": 4, "fourth": 4, "4th": 4,
    "five": 5, "fifth": 5, "5th": 5,
    "six": 6, "sixth": 6, "6th": 6,
    "seven": 7, "seventh": 7, "7th": 7,
    "eight": 8, "eighth": 8, "8th": 8,
    "nine": 9, "ninth": 9, "9th": 9,
    "ten": 10, "tenth": 10, "10th": 10,
    "eleven": 11, "eleventh": 11, "11th": 11,
    "twelve": 12, "twelfth": 12, "12th": 12,
}

PERIOD_NUMBER_PATTERN = (
    r"\d+|one|two|three|four|five|six|seven|eight|nine|ten|eleven|twelve|"
    r"first|second|third|fourth|fifth|sixth|seventh|eighth|ninth|tenth|"
    r"eleventh|twelfth|1st|2nd|3rd|4th|5th|6th|7th|8th|9th|10th|11th|12th"
)

QUARTER_VALUE_PATTERN = r"[1-4]|one|two|three|four|first|second|third|fourth|1st|2nd|3rd|4th"
HALF_VALUE_PATTERN = r"[12]|one|two|first|second|1st|2nd"


# -------------------------------
# HELPERS
# -------------------------------

def normalize_text(text: str) -> str:
    return re.sub(r"\s+", " ", text.lower().strip())


def extract_year(text: str) -> Optional[int]:
    text = normalize_text(text)
    patterns = [
        r"\b(?:fy|fiscal\s+year|financial\s+year|calendar\s+year|cy)\s*['-]?\s*(20\d{2}|\d{2})\b",
        r"\b(?:year|yr)\s*['-]?\s*(20\d{2}|\d{2})\b",
        r"(?<![a-z0-9])['’](\d{2})\b",
        r"\b(20\d{2})\b",
        r"\b(\d{2})\b",
    ]

    match = None
    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            break

    if not match:
        return None

    year = int(match.group(1))

    # convert short year (26 → 2026)
    if year < 100:
        year += 2000

    return year


def _normalize_year_value(raw: Optional[str]) -> Optional[int]:
    if raw is None:
        return None
    year = int(raw)
    if year < 100:
        year += 2000
    return year


def _parse_period_number(raw: Optional[str]) -> Optional[int]:
    if raw is None:
        return None
    value = raw.lower().strip()
    if value.isdigit():
        return int(value)
    return PERIOD_NUMBER_WORDS.get(value)


def _period_from_year_months(year: int, start_month: int, end_month: int) -> Dict[str, int]:
    return {
        "start_month": start_month,
        "start_year": year,
        "end_month": end_month,
        "end_year": year,
    }


def _months_between(start_year: int, start_month: int, end_year: int, end_month: int) -> list[Dict[str, int]]:
    if (start_year, start_month) > (end_year, end_month):
        return []

    months = []
    cur_month = start_month
    cur_year = start_year

    while (cur_year < end_year) or (cur_year == end_year and cur_month <= end_month):
        months.append({"month": cur_month, "year": cur_year})
        cur_month += 1
        if cur_month > 12:
            cur_month = 1
            cur_year += 1

    return months


def _split_period_range_text(text: str) -> Optional[Tuple[str, str]]:
    text = normalize_text(text).replace("half-year", "half year")
    between_match = re.search(r"\bbetween\s+(.+?)\s+and\s+(.+)$", text)
    if between_match:
        return between_match.group(1).strip(), between_match.group(2).strip()

    separators = [
        r"\bup\s+to\b",
        r"\bupto\b",
        r"\bthrough\b",
        r"\buntil\b",
        r"\btill\b",
        r"\bto\b",
        r"\s*-\s*",
    ]
    for separator in separators:
        parts = re.split(separator, text, maxsplit=1)
        if len(parts) == 2:
            return parts[0].strip(), parts[1].strip()

    return None


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
    text = normalize_text(text)

    def _quarter_result(raw_q: str, raw_year: Optional[str]) -> Optional[Tuple[int, int, int]]:
        q = _parse_period_number(raw_q)
        year = _normalize_year_value(raw_year) if raw_year else extract_year(text)
        if not year or q not in {1, 2, 3, 4}:
            return None

        start_month = (q - 1) * 3 + 1
        end_month = start_month + 2
        return year, start_month, end_month

    # q1, q1 2026, qtr1, qtr 1, q 1
    match = re.search(r"\bq(?:tr)?\s*([1-4])\s*(?:of|in|for)?\s*(20\d{2}|\d{2})?\b", text)
    if match:
        result = _quarter_result(match.group(1), match.group(2))
        if result:
            return result

    spoken_patterns = [
        rf"\b(?:quarter|qtr)\s*(?:number|no\.?|#)?\s*({QUARTER_VALUE_PATTERN})(?:\s+(?:of|in|for))?\s*(20\d{{2}}|\d{{2}})?\b",
        rf"\b(20\d{{2}}|\d{{2}})\s+(?:quarter|qtr)\s*(?:number|no\.?|#)?\s*({QUARTER_VALUE_PATTERN})\b",
        rf"\b({QUARTER_VALUE_PATTERN})\s+(?:quarter|qtr)(?:\s+(?:of|in|for))?\s*(20\d{{2}}|\d{{2}})?\b",
    ]
    for index, pattern in enumerate(spoken_patterns):
        match = re.search(pattern, text)
        if not match:
            continue

        if index == 1:
            result = _quarter_result(match.group(2), match.group(1))
        else:
            result = _quarter_result(match.group(1), match.group(2))
        if result:
            return result

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
# HALF YEAR
# -------------------------------

def parse_half_year(text: str) -> Optional[Tuple[int, int, int, int]]:
    text = normalize_text(text).replace("half-year", "half year")

    def _half_result(raw_half: str, raw_year: Optional[str]) -> Optional[Tuple[int, int, int, int]]:
        half = _parse_period_number(raw_half)
        year = _normalize_year_value(raw_year) if raw_year else extract_year(text)
        if not year or half not in {1, 2}:
            return None

        start_month, end_month = (1, 6) if half == 1 else (7, 12)
        return year, start_month, end_month, half

    patterns = [
        rf"\bh\s*({HALF_VALUE_PATTERN})\s*(?:of|in|for)?\s*(20\d{{2}}|\d{{2}})?\b",
        rf"\b({HALF_VALUE_PATTERN})\s*h\s*(?:of|in|for)?\s*(20\d{{2}}|\d{{2}})?\b",
        rf"\b(20\d{{2}}|\d{{2}})\s+h\s*({HALF_VALUE_PATTERN})\b",
        rf"\b({HALF_VALUE_PATTERN})\s+half(?:\s+year)?(?:\s+(?:of|in|for))?\s*(20\d{{2}}|\d{{2}})?\b",
        rf"\bhalf\s+year(?:ly)?\s+({HALF_VALUE_PATTERN})(?:\s+(?:of|in|for))?\s*(20\d{{2}}|\d{{2}})?\b",
        rf"\b(20\d{{2}}|\d{{2}})\s+({HALF_VALUE_PATTERN})\s+half(?:\s+year)?\b",
        rf"\b(20\d{{2}}|\d{{2}})\s+half\s+year(?:ly)?\s+({HALF_VALUE_PATTERN})\b",
    ]
    for index, pattern in enumerate(patterns):
        match = re.search(pattern, text)
        if not match:
            continue

        if index in {2, 5, 6}:
            result = _half_result(match.group(2), match.group(1))
        else:
            result = _half_result(match.group(1), match.group(2))
        if result:
            return result

    return None


def _half_year_period(year: int, half: int) -> Dict[str, int]:
    start_month, end_month = (1, 6) if int(half) == 1 else (7, 12)
    return _period_from_year_months(int(year), start_month, end_month)


def _looks_like_quarter_period(text: str) -> bool:
    text = normalize_text(text)
    return bool(
        re.search(rf"\bq(?:tr)?\s*[1-4]\b", text)
        or re.search(rf"\b(?:quarter|qtr)\s*(?:number|no\.?|#)?\s*({QUARTER_VALUE_PATTERN})\b", text)
        or re.search(rf"\b({QUARTER_VALUE_PATTERN})\s+(?:quarter|qtr)\b", text)
    )


def _looks_like_half_year_period(text: str) -> bool:
    text = normalize_text(text).replace("half-year", "half year")
    return bool(
        re.search(rf"\bh\s*({HALF_VALUE_PATTERN})\b", text)
        or re.search(rf"\b({HALF_VALUE_PATTERN})\s*h\b", text)
        or re.search(rf"\b({HALF_VALUE_PATTERN})\s+half(?:\s+year)?\b", text)
        or re.search(rf"\bhalf\s+year(?:ly)?\s+({HALF_VALUE_PATTERN})\b", text)
    )


def parse_quarter_range(text: str) -> Optional[Dict[str, Any]]:
    parts = _split_period_range_text(text)
    if not parts:
        return None

    left_text, right_text = parts
    if not (_looks_like_quarter_period(left_text) and _looks_like_quarter_period(right_text)):
        return None

    default_year = extract_year(text) or datetime.today().year
    left_has_year = extract_year(left_text) is not None
    right_has_year = extract_year(right_text) is not None

    left_q = parse_quarter(left_text if left_has_year else f"{left_text} {default_year}")
    right_q = parse_quarter(right_text if right_has_year else f"{right_text} {default_year}")
    if not left_q or not right_q:
        return None

    if (left_q[0], left_q[1]) > (right_q[0], right_q[2]):
        if not left_has_year and right_has_year:
            left_q = (right_q[0] - 1, left_q[1], left_q[2])
        elif left_has_year and not right_has_year:
            right_q = (left_q[0] + 1, right_q[1], right_q[2])

    months = _months_between(left_q[0], left_q[1], right_q[0], right_q[2])
    if not months:
        return None

    return {"type": "multi_month", "months": months}


def parse_half_year_range(text: str) -> Optional[Dict[str, Any]]:
    parts = _split_period_range_text(text)
    if not parts:
        return None

    left_text, right_text = parts
    if not (_looks_like_half_year_period(left_text) and _looks_like_half_year_period(right_text)):
        return None

    default_year = extract_year(text) or datetime.today().year
    left_has_year = extract_year(left_text) is not None
    right_has_year = extract_year(right_text) is not None

    left_half = parse_half_year(left_text if left_has_year else f"{left_text} {default_year}")
    right_half = parse_half_year(right_text if right_has_year else f"{right_text} {default_year}")
    if not left_half or not right_half:
        return None

    if (left_half[0], left_half[1]) > (right_half[0], right_half[2]):
        if not left_has_year and right_has_year:
            left_half = (right_half[0] - 1, left_half[1], left_half[2], left_half[3])
        elif left_has_year and not right_has_year:
            right_half = (left_half[0] + 1, right_half[1], right_half[2], right_half[3])

    months = _months_between(left_half[0], left_half[1], right_half[0], right_half[2])
    if not months:
        return None

    return {"type": "multi_month", "months": months}


def parse_fixed_month_span(text: str) -> Optional[Dict[str, Any]]:
    text = normalize_text(text)
    quality = r"(?:(?:complete|completed|closed|full|calendar)\s+){0,3}"
    match = re.search(
        rf"\b(first|1st|second|2nd|third|3rd|fourth|4th|initial|opening|last|final|ending)\s+({PERIOD_NUMBER_PATTERN})\s+{quality}months?(?:\s+(?:of|in|for))?\s*(20\d{{2}}|\d{{2}})?\b",
        text,
    )
    if not match:
        return None

    qualifier = match.group(1)
    count = _parse_period_number(match.group(2))
    year = _normalize_year_value(match.group(3)) if match.group(3) else extract_year(text)
    if not year or not count or count < 1 or count > 12:
        return None

    if qualifier in {"last", "final", "ending"}:
        start_month = 13 - count
        end_month = 12
    else:
        order = {
            "first": 1, "1st": 1, "initial": 1, "opening": 1,
            "second": 2, "2nd": 2,
            "third": 3, "3rd": 3,
            "fourth": 4, "4th": 4,
        }.get(qualifier)
        if not order:
            return None
        start_month = ((order - 1) * count) + 1
        end_month = start_month + count - 1

    if start_month < 1 or end_month > 12:
        return None

    return {
        "type": "multi_month",
        "months": _months_between(year, start_month, year, end_month),
    }


def _split_comparison_text(text: str) -> Optional[Tuple[str, str]]:
    direct_patterns = [
        r"\bcompare\s+(.+?)\s+(?:with|against|versus|vs\.?)\s+(.+)$",
        r"\bcompare\s+(.+?)\s+to\s+(.+)$",
    ]
    for pattern in direct_patterns:
        direct_compare = re.search(pattern, text)
        if direct_compare:
            return direct_compare.group(1).strip(), direct_compare.group(2).strip()

    compare_and = re.search(r"\bcompare\s+(.+?)\s+and\s+(.+)$", text)
    if compare_and:
        left = compare_and.group(1).strip()
        right = compare_and.group(2).strip()
        left_has_period = bool(
            extract_month(left)
            or _looks_like_quarter_period(left)
            or _looks_like_half_year_period(left)
            or extract_year(left)
        )
        right_has_period = bool(
            extract_month(right)
            or _looks_like_quarter_period(right)
            or _looks_like_half_year_period(right)
            or extract_year(right)
        )
        if left_has_period and right_has_period:
            return left, right

    patterns = [
        r"\bvs\.?\b",
        r"\bversus\b",
        r"\bcompared\s+(?:to|with|against)\b",
        r"\bwith\b",
    ]
    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            return text[:match.start()].strip(), text[match.end():].strip()

    # Handles "first half 2025 and 2026" without treating ordinary prose as comparison.
    match = re.search(
        r"\b((?:first|1st|second|2nd)\s+half(?:\s+of)?\s*(?:20\d{2}|\d{2})|h[12]\s*(?:20\d{2}|\d{2}))\s+and\s+((?:20\d{2}|\d{2})|(?:first|1st|second|2nd)\s+half(?:\s+of)?\s*(?:20\d{2}|\d{2})|h[12]\s*(?:20\d{2}|\d{2}))\b",
        text,
    )
    if match:
        return match.group(1).strip(), match.group(2).strip()

    return None


# -------------------------------
# LAST N MONTHS
# -------------------------------

def parse_last_n(text: str):
    normalized = normalize_text(text)
    number_words = {
        "one": 1,
        "two": 2,
        "three": 3,
        "four": 4,
        "five": 5,
        "six": 6,
        "seven": 7,
        "eight": 8,
        "nine": 9,
        "ten": 10,
        "eleven": 11,
        "twelve": 12,
    }

    def _parse_count(raw: str) -> Optional[int]:
        if raw.isdigit():
            return int(raw)
        return number_words.get(raw)

    count_pattern = r"(\d+|one|two|three|four|five|six|seven|eight|nine|ten|eleven|twelve)"
    # Users often qualify relative periods in natural language, e.g.
    # "last 6 complete months" or "past three full quarters".
    period_quality = r"(?:(?:complete|completed|closed|full|calendar)\s+){0,3}"

    # -------- MONTHS --------
    match = re.search(
        rf"\b(last|past|previous|prior|recent|trailing|latest)\s+{count_pattern}\s+{period_quality}months?\b",
        normalized,
    )
    if match:
        return {"unit": "month", "n": _parse_count(match.group(2))}

    if any(phrase in normalized for phrase in ["last month", "previous month", "prior month"]):
        return {"unit": "month", "n": 1}

    # -------- QUARTERS --------
    match = re.search(
        rf"\b(last|past|previous|prior|recent|trailing|latest)\s+{count_pattern}\s+{period_quality}quarters?\b",
        normalized,
    )
    if match:
        return {"unit": "quarter", "n": _parse_count(match.group(2))}

    if any(phrase in normalized for phrase in ["last quarter", "previous quarter", "prior quarter"]):
        return {"unit": "quarter", "n": 1}

    # -------- YEARS --------
    match = re.search(
        rf"\b(last|past|previous|prior|recent|trailing|latest)\s+{count_pattern}\s+{period_quality}years?\b",
        normalized,
    )
    if match:
        return {"unit": "year", "n": _parse_count(match.group(2))}

    if any(phrase in normalized for phrase in ["last year", "previous year", "prior year"]):
        return {"unit": "year", "n": 1}

    return None


def query_wants_current_incomplete_period(text: str) -> bool:
    normalized = normalize_text(text)
    if any(
        phrase in normalized
        for phrase in [
            "latest completed",
            "last completed",
            "completed month",
            "complete month",
            "closed month",
            "full month",
            "full calendar month",
        ]
    ):
        return False

    return bool(
        re.search(
            r"\b(mtd|month\s+to\s+date|this\s+month|current\s+month|current\s+data|current\s+period|latest|today|now|as\s+of|to\s+date|including\s+current|include\s+current|including\s+this\s+month)\b",
            normalized,
        )
    )


def _is_current_calendar_month(month_key: MonthKey, today: Optional[datetime] = None) -> bool:
    today = today or datetime.today()
    return int(month_key.year) == int(today.year) and int(month_key.month) == int(today.month)


def _offset_for_completed_months(latest: MonthKey, include_current_incomplete: bool) -> int:
    if include_current_incomplete:
        return 0
    return 1 if _is_current_calendar_month(latest) else 0


# -------------------------------
# RANGE
# -------------------------------

def parse_range(text: str):
    parts = _split_period_range_text(text)
    if not parts:
        return None

    default_year = extract_year(text)
    left_text, right_text = parts
    left_has_year = extract_year(left_text) is not None
    right_has_year = extract_year(right_text) is not None
    left = extract_month_year(left_text, default_year=default_year)
    right = extract_month_year(right_text, default_year=default_year)

    if left and right:
        start_year, start_month = left
        end_year, end_month = right

        if (start_year, start_month) > (end_year, end_month):
            if not left_has_year and right_has_year:
                start_year = end_year - 1
            elif left_has_year and not right_has_year:
                end_year = start_year + 1

        months = _months_between(start_year, start_month, end_year, end_month)
        if not months:
            return None

        return {
            "type": "multi_month",
            "months": months
        }

    return None


def _comparison_bounds_from_period(period: Optional[Dict[str, Any]]) -> Optional[Dict[str, int]]:
    if not period:
        return None

    if period.get("type") == "multi_month":
        months = period.get("months") or []
        if not months:
            return None
        first = months[0]
        last = months[-1]
        return {
            "start_month": int(first["month"]),
            "start_year": int(first["year"]),
            "end_month": int(last["month"]),
            "end_year": int(last["year"]),
        }

    if period.get("type") == "range":
        return {
            "start_month": int(period["start_month"]),
            "start_year": int(period["start_year"]),
            "end_month": int(period["end_month"]),
            "end_year": int(period["end_year"]),
        }

    if period.get("type") == "single":
        return {
            "start_month": int(period["month"]),
            "start_year": int(period["year"]),
            "end_month": int(period["month"]),
            "end_year": int(period["year"]),
        }

    if period.get("type") == "year":
        return {
            "start_month": 1,
            "start_year": int(period["year"]),
            "end_month": 12,
            "end_year": int(period["year"]),
        }

    return None


# -------------------------------
# COMPARISON
# -------------------------------

def parse_comparison(text: str):
    split = _split_comparison_text(text)
    if not split:
        return None

    left, right = split

    # month vs month
    inferred_year = extract_year(text) or datetime.today().year

    range_left = _comparison_bounds_from_period(parse_range(left))
    range_right = _comparison_bounds_from_period(parse_range(right))
    if range_left and range_right:
        return {
            "type": "comparison",
            "left": range_left,
            "right": range_right,
        }

    # half-year vs half-year, or "first half 2025 with 2026"
    h1 = parse_half_year(left)
    h2 = parse_half_year(right)

    if h1 and h2:
        return {
            "type": "comparison",
            "left": _period_from_year_months(h1[0], h1[1], h1[2]),
            "right": _period_from_year_months(h2[0], h2[1], h2[2]),
        }

    if h1:
        right_year = extract_year(right)
        if right_year:
            return {
                "type": "comparison",
                "left": _period_from_year_months(h1[0], h1[1], h1[2]),
                "right": _half_year_period(right_year, h1[3]),
            }

    if h2:
        left_year = extract_year(left)
        if left_year:
            return {
                "type": "comparison",
                "left": _half_year_period(left_year, h2[3]),
                "right": _period_from_year_months(h2[0], h2[1], h2[2]),
            }

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
    q1 = parse_quarter(left if extract_year(left) else f"{left} {inferred_year}")
    q2 = parse_quarter(right if extract_year(right) else f"{right} {inferred_year}")

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

    if q1:
        right_year = extract_year(right)
        if right_year:
            q = ((q1[1] - 1) // 3) + 1
            start_month = (q - 1) * 3 + 1
            return {
                "type": "comparison",
                "left": {
                    "start_month": q1[1],
                    "start_year": q1[0],
                    "end_month": q1[2],
                    "end_year": q1[0],
                },
                "right": {
                    "start_month": start_month,
                    "start_year": right_year,
                    "end_month": start_month + 2,
                    "end_year": right_year,
                },
            }

    if q2:
        left_year = extract_year(left)
        if left_year:
            q = ((q2[1] - 1) // 3) + 1
            start_month = (q - 1) * 3 + 1
            return {
                "type": "comparison",
                "left": {
                    "start_month": start_month,
                    "start_year": left_year,
                    "end_month": start_month + 2,
                    "end_year": left_year,
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

# def parse_period(query: str) -> Dict:
#     text = normalize_text(query)

#     # -------- RANGE DETECTION (FIXED) --------
#     rng = parse_range(text)
#     if rng:
#         return rng

#     # -------- MULTI MONTH (LIST, NOT RANGE) --------
#     month_matches = re.findall(
#         r"(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)",
#         text
#     )

#     year = extract_year(text)

#     # only treat as multi-month if NO range words
#     if len(month_matches) >= 2 and not re.search(r"\b(to|from|-)\b", text):
#         months = []
#         for m in month_matches:
#             months.append({
#                 "month": MONTH_MAP.get(m[:3]),
#                 "year": year or datetime.today().year
#             })

#         return {
#             "type": "multi_month",
#             "months": months
#         }

#     # 1️⃣ comparison first (highest priority)
#     cmp = parse_comparison(text)
#     if cmp:
#         return cmp

#     # 2️⃣ relative periods
#     rel = parse_relative_period(text)
#     if rel:
#         return rel

#     # 3️⃣ last n (month / quarter / year) 🔥 FIX
#     last_n = parse_last_n(text)

#     if last_n:
#         unit = last_n["unit"]
#         n = last_n["n"]

#         # -------- NORMALIZE TO MONTHS --------
#         if unit == "month":
#             months = n
#         elif unit == "quarter":
#             months = n * 3
#         elif unit == "year":
#             months = n * 12
#         else:
#             months = n  # safety fallback

#         return {
#             "type": "last_n",
#             "n": months,
#             "unit": unit,  # optional (future use)
#         }

#     # 4️⃣ range (secondary safety)
#     rng = parse_range(text)
#     if rng:
#         return rng

#     # 5️⃣ single
#     single = parse_single(text)
#     if single:
#         return single

#     # 6️⃣ fallback
#     return {
#         "type": "latest_month"
#     }

def parse_period(query: str) -> Dict:
    text = normalize_text(query)

    # -------- RANGE DETECTION --------
    # 1️⃣ comparison first (before generic multi-month detection)
    cmp = parse_comparison(text)
    if cmp:
        return cmp

    # -------- RANGE DETECTION --------
    rng = parse_range(text)
    if rng:
        return rng

    # -------- QUARTER / HALF-YEAR RANGE DETECTION --------
    quarter_range = parse_quarter_range(text)
    if quarter_range:
        return quarter_range

    half_year_range = parse_half_year_range(text)
    if half_year_range:
        return half_year_range

    # -------- QUARTER DETECTION --------
    q = parse_quarter(text)
    if q:
        return {
            "type": "range",
            "start_month": q[1],
            "start_year": q[0],
            "end_month": q[2],
            "end_year": q[0],
        }

    # -------- HALF-YEAR DETECTION --------
    half_year = parse_half_year(text)
    if half_year:
        return {
            "type": "range",
            "start_month": half_year[1],
            "start_year": half_year[0],
            "end_month": half_year[2],
            "end_year": half_year[0],
        }

    fixed_month_span = parse_fixed_month_span(text)
    if fixed_month_span:
        return fixed_month_span

    # -------- MONTH DETECTION (FIXED) --------
    month_pattern = (
        r"\b("
        r"january|jan|february|feb|march|mar|april|apr|may|"
        r"june|jun|july|jul|august|aug|september|sep|"
        r"october|oct|november|nov|december|dec"
        r")\b"
    )
    month_matches = re.findall(month_pattern, text)

    year = extract_year(text)

    # -------- SINGLE MONTH (CRITICAL FIX) --------
    if len(month_matches) == 1:
        m = month_matches[0][:3]  # normalize to short form
        return {
            "type": "single",
            "month": MONTH_MAP.get(m),
            "year": year or datetime.today().year
        }

    # -------- MULTI MONTH (LIST, NOT RANGE) --------
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

    # 2️⃣ relative periods
    rel = parse_relative_period(text)
    if rel:
        return rel

    # 3️⃣ last n (month / quarter / year)
    last_n = parse_last_n(text)

    if last_n:
        unit = last_n["unit"]
        n = last_n["n"]

        # -------- NORMALIZE TO MONTHS --------
        if unit == "month":
            months = n
        elif unit == "quarter":
            months = n * 3
        elif unit == "year":
            months = n * 12
        else:
            months = n  # safety fallback

        return {
            "type": "last_n",
            "n": months,
            "unit": unit,
            "include_current_incomplete": query_wants_current_incomplete_period(text),
        }

    # 4️⃣ range (secondary safety)
    rng = parse_range(text)
    if rng:
        return rng

    # 5️⃣ single (fallback parser)
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
