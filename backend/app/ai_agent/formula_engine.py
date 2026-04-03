from __future__ import annotations

from typing import Any, Dict, List, Tuple

import pandas as pd

from app.utils import formulas_utils as futils

FORMULA_REGISTRY = {
    "sales": futils.uk_sales,
    "gross_sales": futils.uk_gross_sales,
    "tax": futils.uk_tax,
    "credits": futils.uk_credits,
    "tax_and_credits": futils.uk_tax_and_credits,
    "cogs": futils.uk_cogs,
    "amazon_fee": futils.uk_amazon_fee,
    "platform_fee": futils.uk_platform_fee,
    "advertising": futils.uk_advertising,
    "profit": futils.uk_profit,
}

DEFAULT_METRIC = "profit"


def ensure_supported_country(country: str) -> None:
    if (country or "").strip().lower() != "uk":
        raise ValueError("Formula engine currently supports only UK data")


def available_metrics() -> List[str]:
    return sorted(FORMULA_REGISTRY.keys())


def compute_metric(df: pd.DataFrame, metric_name: str, country: str) -> Dict[str, Any]:
    ensure_supported_country(country)
    metric_name = (metric_name or DEFAULT_METRIC).strip().lower()
    if metric_name not in FORMULA_REGISTRY:
        raise ValueError(f"Unsupported metric '{metric_name}'. Supported metrics: {', '.join(available_metrics())}")

    fn = FORMULA_REGISTRY[metric_name]
    total, per_sku_df, components = fn(df, country=country)
    per_sku_df = per_sku_df.sort_values("__metric__", ascending=False).reset_index(drop=True)

    return {
        "metric": metric_name,
        "total": float(total),
        "components": components,
        "per_sku": per_sku_df.to_dict(orient="records"),
        "row_count": int(len(df)),
    }


def compute_all(df: pd.DataFrame, country: str) -> Dict[str, Any]:
    ensure_supported_country(country)
    return {name: compute_metric(df, name, country) for name in FORMULA_REGISTRY}


def pick_top_skus(metric_result: Dict[str, Any], n: int = 10, reverse: bool = True) -> List[Dict[str, Any]]:
    per_sku = list(metric_result.get("per_sku", []))
    rows = sorted(per_sku, key=lambda x: float(x.get("__metric__", 0.0)), reverse=reverse)
    return rows[:n]


def compare_metric(current: Dict[str, Any], previous: Dict[str, Any]) -> Dict[str, Any]:
    current_total = float(current.get("total", 0.0))
    previous_total = float(previous.get("total", 0.0))
    delta = current_total - previous_total
    pct_change = None if previous_total == 0 else (delta / previous_total) * 100.0
    return {
        "metric": current.get("metric"),
        "current_total": current_total,
        "previous_total": previous_total,
        "delta": delta,
        "pct_change": pct_change,
    }
