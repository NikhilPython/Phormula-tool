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
    df = fetch_nse_month_df(engine, user_id, country, month, year)

    result: Dict[str, Any] = {
        "metric": metric_def.name,
        "column": metric_def.column,
        "metric_kind": metric_def.kind,
        "period_type": "single_month",
        "period_label": datetime(year, int(month) if str(month).isdigit() else datetime.strptime(str(month), "%B").month, 1).strftime("%b %Y")
        if isinstance(month, str) and str(month).isdigit()
        else datetime(year, month if isinstance(month, int) else datetime.strptime(str(month), "%B").month, 1).strftime("%b %Y"),
    }

    if metric_def.kind == "sku_additive":
        grouped = _aggregate_sku_rows([(MonthKey(year=year, month=(month if isinstance(month, int) else datetime.strptime(str(month), "%B").month)), df)], metric_def.column)
        result["total"] = float(grouped["__metric__"].sum()) if not grouped.empty else 0.0
        result["per_sku"] = grouped.to_dict(orient="records")
        result["row_count"] = int(len(grouped))
        return result

    if metric_def.kind == "total_additive":
        total = _monthly_total_value(df, metric_def.column)
        result["total"] = total
        result["per_sku"] = []
        result["row_count"] = 1
        return result

    if metric_def.kind == "ratio":
        total = _compute_ratio_single_month(df, metric_def)
        result["total"] = total
        result["per_sku"] = []
        result["row_count"] = 1
        return result

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
            "period_label": f"{mk.month:02d}-{mk.year}",
            "__metric__": value
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
    match = re.search(r"\b(20\d{2})\b", text)
    return int(match.group(1)) if match else None


def extract_month(text: str) -> Optional[int]:
    for k, v in MONTH_MAP.items():
        if re.search(rf"\b{k}\b", text):
            return v
    return None


def extract_month_year(text: str) -> Optional[Tuple[int, int]]:
    year = extract_year(text)
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
    parts = re.split(r"\bto\b|-", text)

    if len(parts) != 2:
        return None

    left = extract_month_year(parts[0])
    right = extract_month_year(parts[1])

    if left and right:
        return {
            "type": "range",
            "start_month": left[1],
            "start_year": left[0],
            "end_month": right[1],
            "end_year": right[0],
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
    m1 = extract_month_year(left)
    m2 = extract_month_year(right)

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

    # 1️⃣ comparison first (highest priority)
    cmp = parse_comparison(text)
    if cmp:
        return cmp

    # 2️⃣ relative periods (NEW 🔥)
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

    # 5️⃣ single (includes quarter fix already)
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