from __future__ import annotations

import json
import re
from calendar import monthrange
from datetime import date, datetime
from typing import Any, Dict, Iterable, List, Optional

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from config import Config
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
    "platformfeenew",
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
    "platformfeenew",
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

COLUMN_ALIASES = {
    "advertising_total": ("ads_spend", "total_ads"),
    "amazon_fee": ("amazon_fees",),
    "refund_quantity": ("return_quantity",),
}

COMPARISON_DRIVER_METRICS = [
    "net_sales",
    "gross_sales",
    "product_sales",
    "total_quantity",
    "quantity",
    "asp",
    "profit",
    "profit_percentage",
    "cm2_profit",
    "total_cm2_profit",
    "cm2_profit_per",
    "total_cm2_margins",
    "promotional_rebates",
    "refund_sales",
    "return_quantity",
    "return_rate",
    "cogs",
    "selling_fees",
    "fba_fees",
    "marketplace_fees",
    "amazon_fees",
    "platform_fee",
    "platformfeenew",
    "platform_fee_inventory_storage",
    "tax_and_credits",
    "other",
    "ads_spend",
    "total_ads",
    "product_spend",
    "display_spend",
    "brand_spend",
    "ads_sale_amount",
    "ads_sale_units",
    "ads_roas",
    "ads_acos",
    "tacos_total_advertising_cost_of_sale",
    "ads_cpc",
    "ads_ctr",
    "ads_conversion_rate",
    "lost_total",
    "misc_transaction",
    "debt_payment",
    "current_net_reimbursement",
]

GOOD_WHEN_UP_METRICS = {
    "net_sales",
    "gross_sales",
    "product_sales",
    "total_quantity",
    "quantity",
    "asp",
    "profit",
    "profit_percentage",
    "cm2_profit",
    "total_cm2_profit",
    "cm2_profit_per",
    "total_cm2_margins",
    "ads_sale_amount",
    "ads_sale_units",
    "ads_roas",
    "ads_ctr",
    "ads_conversion_rate",
    "current_net_reimbursement",
}

BAD_WHEN_UP_METRICS = {
    "promotional_rebates",
    "refund_sales",
    "return_quantity",
    "return_rate",
    "cogs",
    "selling_fees",
    "fba_fees",
    "marketplace_fees",
    "amazon_fees",
    "platform_fee",
    "platformfeenew",
    "platform_fee_inventory_storage",
    "tax_and_credits",
    "other",
    "ads_spend",
    "total_ads",
    "product_spend",
    "display_spend",
    "brand_spend",
    "ads_acos",
    "tacos_total_advertising_cost_of_sale",
    "ads_cpc",
    "lost_total",
    "misc_transaction",
    "debt_payment",
}

SIGN_AWARE_BURDEN_METRICS = {
    "promotional_rebates",
    "promotional_rebates_tax",
}

PERCENTAGE_DRIVER_METRICS = {
    "profit_percentage",
    "cm2_profit_per",
    "total_cm2_margins",
    "return_rate",
    "ads_acos",
    "tacos_total_advertising_cost_of_sale",
    "ads_ctr",
    "ads_conversion_rate",
}

UNIT_DRIVER_METRICS = {
    "total_quantity",
    "quantity",
    "return_quantity",
    "ads_sale_units",
}

METRIC_LABELS = {
    "net_sales": "Net sales",
    "gross_sales": "Gross sales",
    "product_sales": "Product sales",
    "total_quantity": "Net sold units",
    "quantity": "Gross units",
    "asp": "ASP",
    "profit": "CM1 profit",
    "profit_percentage": "CM1 profit margin",
    "cm2_profit": "Product CM2 profit",
    "total_cm2_profit": "CM2 profit",
    "cm2_profit_per": "CM2 margin",
    "total_cm2_margins": "CM2 margin",
    "promotional_rebates": "Promo rebates",
    "refund_sales": "Refund sales",
    "return_quantity": "Refund quantity",
    "return_rate": "Return rate",
    "cogs": "COGS",
    "selling_fees": "Selling fees",
    "fba_fees": "FBA fees",
    "marketplace_fees": "Marketplace fees",
    "amazon_fees": "Amazon fees",
    "platform_fee": "Platform fees",
    "platformfeenew": "Subscription fees",
    "platform_fee_inventory_storage": "Inventory storage fees",
    "tax_and_credits": "Tax and credits",
    "other": "Other charges",
    "ads_spend": "Ad spend",
    "total_ads": "Ad spend",
    "product_spend": "Sponsored product spend",
    "display_spend": "Sponsored display spend",
    "brand_spend": "Sponsored brand spend",
    "ads_sale_amount": "Ad sales",
    "ads_sale_units": "Ad sales units",
    "ads_roas": "Ad ROAS",
    "ads_acos": "Ad ACOS",
    "tacos_total_advertising_cost_of_sale": "TACOS",
    "ads_cpc": "Ad CPC",
    "ads_ctr": "Ad CTR",
    "ads_conversion_rate": "Ad conversion rate",
    "lost_total": "Lost total",
    "misc_transaction": "Misc transactions",
    "debt_payment": "Debt payment",
    "current_net_reimbursement": "Current net reimbursement",
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


def _with_business_aliases(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame

    out = frame.copy()
    for source, targets in COLUMN_ALIASES.items():
        if source not in out.columns:
            continue

        source_values = pd.to_numeric(out[source], errors="coerce").fillna(0.0)
        for target in targets:
            if target not in out.columns:
                out[target] = source_values
                continue

            target_values = pd.to_numeric(out[target], errors="coerce")
            if target_values.fillna(0.0).abs().sum() == 0 and source_values.abs().sum() > 0:
                out[target] = source_values
            else:
                out[target] = target_values.fillna(source_values).fillna(0.0)

    return out


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


def _dedupe_months(months: List[MonthKey]) -> List[MonthKey]:
    unique = {(month.year, month.month): month for month in months}
    return [unique[key] for key in sorted(unique)]


def _period_part_as_payload(part: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    part = part or {}
    if part.get("type"):
        return part
    if all(part.get(key) for key in ("start_month", "start_year", "end_month", "end_year")):
        return {"type": "range", **part}
    if part.get("month") and part.get("year"):
        return {"type": "single", "month": part["month"], "year": part["year"]}
    return part


def _months_from_payload(
    engine: Engine,
    user_id: int,
    country: str,
    payload: Optional[Dict[str, Any]],
) -> List[MonthKey]:
    payload = payload or {}
    if payload.get("type") == "growth_base":
        payload = payload.get("base") or {}

    ptype = payload.get("type")

    if ptype == "single" and payload.get("month") and payload.get("year"):
        return [MonthKey(year=int(payload["year"]), month=int(payload["month"]))]

    if ptype == "comparison":
        months: List[MonthKey] = []
        for key in ("p2", "right", "p1", "left"):
            part = payload.get(key)
            if part:
                months.extend(_months_from_payload(engine, user_id, country, _period_part_as_payload(part)))
        return _dedupe_months(months)

    if ptype == "multi_month":
        return [
            MonthKey(year=int(item["year"]), month=int(item["month"]))
            for item in payload.get("months", [])
            if item.get("month") and item.get("year")
        ]

    if ptype in {"last_n", "last_n_months"}:
        return get_last_n_month_keys(
            engine,
            user_id,
            country,
            int(payload.get("n") or 6),
            include_current_incomplete=bool(payload.get("include_current_incomplete", False)),
        )

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
                    _with_business_aliases(
                        fetch_nse_month_df(
                            engine,
                            user_id,
                            country,
                            month_key.month,
                            month_key.year,
                        )
                    ),
                )
            )
        except Exception:
            continue
    return frames


def _period_date_bounds(months: List[MonthKey]) -> tuple[Optional[date], Optional[date]]:
    if not months:
        return None, None
    first = months[0]
    last = months[-1]
    return (
        date(first.year, first.month, 1),
        date(last.year, last.month, monthrange(last.year, last.month)[1]),
    )


def _filter_frame_for_product(frame: pd.DataFrame, product_query: Optional[str]) -> pd.DataFrame:
    query = str(product_query or "").strip().lower()
    if not query:
        return frame

    try:
        rows = fetch_non_total_rows(frame).copy()
    except Exception:
        return frame.iloc[0:0].copy()

    if rows.empty:
        return rows

    mask = pd.Series(False, index=rows.index)
    for column in ["sku", "product_name"]:
        if column not in rows.columns:
            continue
        mask = mask | rows[column].astype(str).str.lower().str.contains(query, na=False, regex=False)
    return rows[mask].copy()


def _filter_period_frames_for_product(
    period_frames: List[tuple[MonthKey, pd.DataFrame]],
    product_query: Optional[str],
) -> List[tuple[MonthKey, pd.DataFrame]]:
    if not product_query:
        return period_frames
    return [
        (month_key, _filter_frame_for_product(frame, product_query))
        for month_key, frame in period_frames
    ]


def _sku_row_count(period_frames: List[tuple[MonthKey, pd.DataFrame]]) -> int:
    count = 0
    for _, frame in period_frames:
        try:
            count += int(len(fetch_non_total_rows(frame)))
        except Exception:
            continue
    return count


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
                total_value = _safe_float(total_row.get(column, None)) if column in total_row else 0.0
                sku_sum = (
                    _safe_float(pd.to_numeric(sku_rows[column], errors="coerce").fillna(0.0).sum())
                    if column in sku_rows.columns
                    else 0.0
                )
                source_value = sku_sum if abs(total_value) < 0.005 and abs(sku_sum) > 0.005 else total_value
                totals[column] += source_value
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


def _safe_json_load(value: Any) -> Any:
    if isinstance(value, (dict, list)):
        return value
    if value is None:
        return {}
    try:
        return json.loads(str(value))
    except Exception:
        return {}


def _plain_action_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (list, tuple)):
        parts = []
        for item in value:
            text_value = _plain_action_text(item)
            if text_value:
                parts.append(text_value)
        return " ".join(parts).strip()
    if isinstance(value, dict):
        parts = []
        for key in ["recommendation", "action", "weekly_action", "ads_recommendation", "inventory_recommendation"]:
            text_value = _plain_action_text(value.get(key))
            if text_value:
                parts.append(text_value)
        return " ".join(parts).strip()

    text_value = re.sub(r"<[^>]+>", " ", str(value))
    text_value = re.sub(r"\s+", " ", text_value).strip()
    text_value = re.sub(r"^\s*[-*\u2022]\s*", "", text_value).strip()
    return text_value


def _append_unique_action(actions: List[Dict[str, Any]], action: Dict[str, Any]) -> None:
    text_value = _plain_action_text(action.get("action"))
    if not text_value:
        return
    scope = str(action.get("scope") or "").strip().lower()
    sku = str(action.get("sku") or "").strip().lower()
    product_name = str(action.get("product_name") or "").strip().lower()
    identity = (sku or product_name) if scope == "sku" else ""
    normalized = re.sub(r"\W+", "", f"{scope}:{identity}:{text_value}").lower()
    if not normalized:
        return
    if any(item.get("_key") == normalized for item in actions):
        return
    clean_action = dict(action)
    clean_action["action"] = text_value
    clean_action["_key"] = normalized
    actions.append(clean_action)


def _sku_action_metric_lookup(sku_rows: pd.DataFrame) -> Dict[str, Dict[str, Any]]:
    if sku_rows.empty or "sku" not in sku_rows.columns:
        return {}

    metric_columns = [
        "sku",
        "product_name",
        "net_sales",
        "total_quantity",
        "profit",
        "cm2_profit",
        "ads_spend",
        "ad_roas",
    ]
    existing = [column for column in metric_columns if column in sku_rows.columns]
    lookup: Dict[str, Dict[str, Any]] = {}
    for row in sku_rows[existing].to_dict(orient="records"):
        sku = str(row.get("sku") or "").strip()
        if not sku:
            continue
        lookup[sku.lower()] = _clean_record(row)
    return lookup


def _enrich_live_ai_action(
    action: Dict[str, Any],
    *,
    product_name_lookup: Dict[str, str],
    sku_metric_lookup: Dict[str, Dict[str, Any]],
) -> Dict[str, Any]:
    enriched = dict(action)
    sku_key = str(enriched.get("sku") or "").strip().lower()
    metrics = sku_metric_lookup.get(sku_key) or {}

    if not enriched.get("product_name"):
        enriched["product_name"] = metrics.get("product_name") or product_name_lookup.get(sku_key)

    for metric in ["net_sales", "total_quantity", "profit", "cm2_profit", "ads_spend", "ad_roas"]:
        if metric in metrics and enriched.get(metric) in (None, ""):
            enriched[metric] = metrics.get(metric)

    return enriched


def _merge_live_ai_sku_actions(actions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    merged: List[Dict[str, Any]] = []
    by_sku: Dict[str, Dict[str, Any]] = {}

    for action in actions:
        if action.get("scope") != "sku":
            merged.append(action)
            continue

        sku_key = str(action.get("sku") or "").strip().lower()
        product_key = str(action.get("product_name") or "").strip().lower()
        key = sku_key or product_key
        if not key:
            merged.append(action)
            continue

        existing = by_sku.get(key)
        if not existing:
            by_sku[key] = action
            merged.append(action)
            continue

        current_text = str(existing.get("action") or "")
        new_text = str(action.get("action") or "")
        if len(new_text) > len(current_text):
            existing["action"] = new_text

        for field in ["product_name", "net_sales", "total_quantity", "profit", "cm2_profit", "ads_spend", "ad_roas"]:
            if existing.get(field) in (None, "") and action.get(field) not in (None, ""):
                existing[field] = action.get(field)

    return merged


def _sort_live_ai_actions_by_sales(actions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    portfolio_actions = [action for action in actions if action.get("scope") == "portfolio"]
    sku_actions = [action for action in actions if action.get("scope") == "sku"]
    remaining_actions = [action for action in actions if action.get("scope") not in {"portfolio", "sku"}]

    sku_actions = sorted(
        sku_actions,
        key=lambda action: (
            _safe_float(action.get("net_sales")),
            _safe_float(action.get("total_quantity")),
            str(action.get("product_name") or action.get("sku") or "").lower(),
        ),
        reverse=True,
    )
    return [*portfolio_actions, *sku_actions, *remaining_actions]


def _sku_action_matches_product(
    action: Dict[str, Any],
    product_query: Optional[str],
    product_skus: Optional[List[str]] = None,
) -> bool:
    sku = str(action.get("sku") or "").strip().lower()
    if sku and sku in {str(item or "").strip().lower() for item in product_skus or []}:
        return True

    query = str(product_query or "").strip().lower()
    if not query:
        return False
    haystack = " ".join(
        str(action.get(key) or "")
        for key in ["sku", "product_name", "action"]
    ).lower()
    return query in haystack


def _extract_live_ai_actions(
    *,
    record: Dict[str, Any],
    product_query: Optional[str],
    product_skus: Optional[List[str]] = None,
    sku_product_names: Optional[Dict[str, str]] = None,
    sku_metric_lookup: Optional[Dict[str, Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    strategy = _safe_json_load(record.get("strategy"))
    weekly = _safe_json_load(record.get("weekly_email_summary_json"))
    summary = _safe_json_load(record.get("summary"))
    product_name_lookup = {
        str(sku or "").strip().lower(): str(name or "").strip()
        for sku, name in (sku_product_names or {}).items()
        if str(sku or "").strip()
    }
    sku_metric_lookup = sku_metric_lookup or {}

    actions: List[Dict[str, Any]] = []
    portfolio_action = (
        _plain_action_text((weekly.get("portfolio_summary") or {}).get("weekly_action"))
        or _plain_action_text(strategy.get("portfolio_recommendation"))
        or _plain_action_text(strategy.get("recommendation"))
        or _plain_action_text(summary.get("recommended_action"))
    )
    if portfolio_action:
        _append_unique_action(actions, {"scope": "portfolio", "action": portfolio_action})

    for item in weekly.get("priority_skus") or []:
        if not isinstance(item, dict):
            continue
        _append_unique_action(
            actions,
            _enrich_live_ai_action(
                {
                    "scope": "sku",
                    "sku": item.get("sku"),
                    "product_name": item.get("product_name") or product_name_lookup.get(str(item.get("sku") or "").strip().lower()),
                    "action": item.get("action"),
                    "severity": item.get("severity"),
                },
                product_name_lookup=product_name_lookup,
                sku_metric_lookup=sku_metric_lookup,
            ),
        )

    for sku, block in (strategy.get("sku_actions") or {}).items():
        if not isinstance(block, dict):
            continue
        action_parts = [
            _plain_action_text(block.get("recommendation")),
            _plain_action_text(block.get("ads_recommendation")),
            _plain_action_text(block.get("inventory_recommendation")),
        ]
        _append_unique_action(
            actions,
            _enrich_live_ai_action(
                {
                    "scope": "sku",
                    "sku": sku,
                    "product_name": block.get("product_name") or product_name_lookup.get(str(sku or "").strip().lower()),
                    "action": " ".join(part for part in action_parts if part).strip(),
                },
                product_name_lookup=product_name_lookup,
                sku_metric_lookup=sku_metric_lookup,
            ),
        )

    remaining_action_parts = [
        _plain_action_text(strategy.get("remaining_skus_recommendation")),
        _plain_action_text(strategy.get("remaining_skus_ads_recommendation")),
        _plain_action_text(strategy.get("remaining_skus_inventory_recommendation")),
    ]
    _append_unique_action(
        actions,
        {
            "scope": "remaining_skus",
            "action": " ".join(part for part in remaining_action_parts if part).strip(),
        },
    )

    actions = _sort_live_ai_actions_by_sales(_merge_live_ai_sku_actions(actions))

    for action in actions:
        action.pop("_key", None)

    product_actions = [
        action
        for action in actions
        if action.get("scope") == "sku" and _sku_action_matches_product(action, product_query, product_skus)
    ]
    missing_product_action = bool(product_query and actions and not product_actions)
    selected_actions = _sort_live_ai_actions_by_sales(product_actions if product_query else actions)

    return {
        "available": bool(selected_actions or missing_product_action),
        "source": "live_ai_summary",
        "record": {
            "id": record.get("id"),
            "start_date": str(record.get("start_date") or ""),
            "end_date": str(record.get("end_date") or ""),
            "created_at": str(record.get("created_at") or ""),
        },
        "portfolio_action": portfolio_action,
        "actions": selected_actions,
        "all_actions_available": bool(actions),
        "product_actions": product_actions,
        "is_product_specific": bool(product_actions),
        "missing_product_action": missing_product_action,
        "product_query": product_query,
    }


def _fetch_live_ai_summary_actions(
    *,
    user_id: int,
    country: str,
    months: List[MonthKey],
    product_query: Optional[str],
    product_skus: Optional[List[str]] = None,
    sku_product_names: Optional[Dict[str, str]] = None,
    sku_metric_lookup: Optional[Dict[str, Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    database_url = getattr(Config, "SQLALCHEMY_DATABASE_CHATBOT_URL", None)
    if not database_url:
        return {"available": False}

    start_date, end_date = _period_date_bounds(months)
    country_key = (country or "").strip().lower()
    params = {"user_id": user_id, "country": country_key}

    overlap_filter = ""
    if start_date and end_date:
        params.update({"start_date": start_date, "end_date": end_date})
        overlap_filter = """
          AND end_date >= :start_date
          AND start_date <= :end_date
        """

    query = text(f"""
        SELECT id, start_date, end_date, strategy, summary, weekly_email_summary_json, created_at
        FROM public.live_ai_summary
        WHERE user_id = :user_id
          AND LOWER(country) = :country
          {overlap_filter}
        ORDER BY end_date DESC, created_at DESC, id DESC
        LIMIT 1
    """)

    fallback_query = text("""
        SELECT id, start_date, end_date, strategy, summary, weekly_email_summary_json, created_at
        FROM public.live_ai_summary
        WHERE user_id = :user_id
          AND LOWER(country) = :country
        ORDER BY end_date DESC, created_at DESC, id DESC
        LIMIT 1
    """)

    try:
        engine = create_engine(database_url, pool_pre_ping=True)
        with engine.connect() as conn:
            row = conn.execute(query, params).mappings().first()
            if not row and overlap_filter:
                row = conn.execute(fallback_query, {"user_id": user_id, "country": country_key}).mappings().first()
        if not row:
            return {"available": False}
        return _extract_live_ai_actions(
            record=dict(row),
            product_query=product_query,
            product_skus=product_skus,
            sku_product_names=sku_product_names,
            sku_metric_lookup=sku_metric_lookup,
        )
    except Exception:
        return {"available": False}


def _product_breakdown_availability(period_frames: List[tuple[MonthKey, pd.DataFrame]]) -> Dict[str, Any]:
    total_only: List[str] = []
    productwise_available: List[str] = []

    for column in ADDITIVE_TOTAL_COLUMNS:
        total_abs = 0.0
        sku_abs = 0.0
        seen = False

        for _, frame in period_frames:
            try:
                total_row = fetch_total_row(frame)
            except Exception:
                total_row = pd.Series(dtype="object")
            sku_rows = fetch_non_total_rows(frame)

            if column in total_row:
                total_abs += abs(_safe_float(total_row.get(column)))
                seen = True
            if column in sku_rows.columns:
                sku_abs += abs(_safe_float(pd.to_numeric(sku_rows[column], errors="coerce").fillna(0.0).sum()))
                seen = True

        if not seen:
            continue
        if total_abs > 0.005 and sku_abs < 0.005:
            total_only.append(column)
        elif sku_abs > 0.005:
            productwise_available.append(column)

    return {
        "total_only_metrics": sorted(total_only),
        "productwise_available_metrics": sorted(productwise_available),
    }


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
        "promotional_rebates",
        "platform_fee",
        "platformfeenew",
        "platform_fee_inventory_storage",
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


def _metric_comparison(left_totals: Dict[str, float], right_totals: Dict[str, float], metric: str) -> Dict[str, Any]:
    left_value = _safe_float(left_totals.get(metric))
    right_value = _safe_float(right_totals.get(metric))
    delta = left_value - right_value
    return {
        "left": _round(left_value),
        "right": _round(right_value),
        "delta": _round(delta),
        "pct_change": _round(_safe_div(delta, right_value, 100.0)),
    }


def _metric_label(metric: str) -> str:
    metric_key = str(metric or "").strip().lower()
    return METRIC_LABELS.get(metric_key, metric_key.replace("_", " ").title())


def _canonical_driver_metric(metric: str) -> str:
    aliases = {
        "total_ads": "ads_spend",
        "advertising_total": "ads_spend",
        "amazon_fee": "amazon_fees",
    }
    return aliases.get(metric, metric)


def _burden_value(metric: str, value: float) -> float:
    if metric in SIGN_AWARE_BURDEN_METRICS:
        return -_safe_float(value)
    if metric in BAD_WHEN_UP_METRICS:
        return abs(_safe_float(value))
    return _safe_float(value)


def _business_delta(metric: str, left_value: float, right_value: float) -> float:
    if metric in BAD_WHEN_UP_METRICS:
        return _burden_value(metric, left_value) - _burden_value(metric, right_value)
    return left_value - right_value


def _driver_business_effect(metric: str, change: float) -> str:
    if abs(change) < 0.005:
        return "neutral"
    if metric in BAD_WHEN_UP_METRICS:
        return "unfavorable" if change > 0 else "favorable"
    if metric in GOOD_WHEN_UP_METRICS:
        return "unfavorable" if change < 0 else "favorable"
    return "neutral"


def _estimated_impact_score(
    metric: str,
    change: float,
    left_totals: Dict[str, float],
    right_totals: Dict[str, float],
) -> float:
    abs_change = abs(change)
    if metric in UNIT_DRIVER_METRICS:
        asp = max(
            abs(_safe_float(left_totals.get("asp"))),
            abs(_safe_float(right_totals.get("asp"))),
            abs(_safe_div(_safe_float(left_totals.get("net_sales")), _safe_float(left_totals.get("total_quantity"))) or 0.0),
            abs(_safe_div(_safe_float(right_totals.get("net_sales")), _safe_float(right_totals.get("total_quantity"))) or 0.0),
            1.0,
        )
        return abs_change * asp
    if metric == "asp":
        units = max(
            abs(_safe_float(left_totals.get("total_quantity"))),
            abs(_safe_float(right_totals.get("total_quantity"))),
            1.0,
        )
        return abs_change * units
    if metric in PERCENTAGE_DRIVER_METRICS:
        sales_base = max(
            abs(_safe_float(left_totals.get("net_sales"))),
            abs(_safe_float(right_totals.get("net_sales"))),
            1.0,
        )
        return (abs_change / 100.0) * sales_base
    if metric == "ads_roas":
        spend_base = max(
            abs(_safe_float(left_totals.get("ads_spend"))),
            abs(_safe_float(left_totals.get("total_ads"))),
            abs(_safe_float(right_totals.get("ads_spend"))),
            abs(_safe_float(right_totals.get("total_ads"))),
            1.0,
        )
        return abs_change * spend_base
    return abs_change


def _metric_driver_record(
    left_totals: Dict[str, float],
    right_totals: Dict[str, float],
    metric: str,
    primary_metric: Optional[str],
) -> Optional[Dict[str, Any]]:
    metric = _canonical_driver_metric(metric)
    if metric == primary_metric:
        return None

    left_value = _safe_float(left_totals.get(metric))
    right_value = _safe_float(right_totals.get(metric))
    if abs(left_value) < 0.005 and abs(right_value) < 0.005:
        return None

    raw_delta = left_value - right_value
    change = _business_delta(metric, left_value, right_value)
    if abs(change) < 0.005 and abs(raw_delta) < 0.005:
        return None

    effect = _driver_business_effect(metric, change)
    direction = "increased" if change > 0 else "decreased" if change < 0 else "was flat"
    score = _estimated_impact_score(metric, change, left_totals, right_totals)
    pct_base = abs(_burden_value(metric, right_value)) if metric in BAD_WHEN_UP_METRICS else right_value

    return {
        "metric": metric,
        "label": _metric_label(metric),
        "left": _round(left_value),
        "right": _round(right_value),
        "delta": _round(raw_delta),
        "pct_change": _round(_safe_div(raw_delta, right_value, 100.0)),
        "business_delta": _round(change),
        "business_pct_change": _round(_safe_div(change, pct_base, 100.0)),
        "direction": direction,
        "business_effect": effect,
        "unfavorable": effect == "unfavorable",
        "favorable": effect == "favorable",
        "impact_score": _round(score),
        "value_basis": "absolute burden" if metric in BAD_WHEN_UP_METRICS else "raw value",
        "sign_convention": "negative values are discount paid; positive values are rebate received" if metric in SIGN_AWARE_BURDEN_METRICS else None,
    }


def _rank_metric_drivers(
    left_totals: Dict[str, float],
    right_totals: Dict[str, float],
    metrics: List[str],
    primary_metric: Optional[str],
) -> List[Dict[str, Any]]:
    records: List[Dict[str, Any]] = []
    seen: set[str] = set()

    for metric in metrics:
        canonical = _canonical_driver_metric(metric)
        if not canonical or canonical in seen:
            continue
        seen.add(canonical)
        record = _metric_driver_record(left_totals, right_totals, canonical, primary_metric)
        if record:
            records.append(record)

    return sorted(records, key=lambda row: _safe_float(row.get("impact_score")), reverse=True)


def _sku_delta_records(
    left_frames: List[tuple[MonthKey, pd.DataFrame]],
    right_frames: List[tuple[MonthKey, pd.DataFrame]],
    *,
    sort_metric: str = "profit",
    reverse: bool = False,
    limit: int = 5,
) -> List[Dict[str, Any]]:
    left_rows = _sku_frame(left_frames)
    right_rows = _sku_frame(right_frames)
    if left_rows.empty and right_rows.empty:
        return []

    left_index = left_rows.set_index("sku", drop=False) if not left_rows.empty else pd.DataFrame()
    right_index = right_rows.set_index("sku", drop=False) if not right_rows.empty else pd.DataFrame()
    sku_values = sorted(set(left_index.index.tolist()) | set(right_index.index.tolist()))
    metrics = [
        "profit",
        "net_sales",
        "quantity",
        "total_quantity",
        "asp",
        "cm2_profit",
        "promotional_rebates",
        "refund_sales",
        "return_quantity",
        "cogs",
        "selling_fees",
        "fba_fees",
        "amazon_fees",
        "platform_fee",
        "platformfeenew",
        "ads_spend",
        "product_spend",
        "display_spend",
        "brand_spend",
        "ads_sale_amount",
    ]
    records: List[Dict[str, Any]] = []

    for sku in sku_values:
        left_row = left_index.loc[sku] if sku in left_index.index else None
        right_row = right_index.loc[sku] if sku in right_index.index else None
        product_name = ""
        if left_row is not None:
            product_name = str(left_row.get("product_name") or "")
        if not product_name and right_row is not None:
            product_name = str(right_row.get("product_name") or "")

        record: Dict[str, Any] = {"sku": str(sku), "product_name": product_name}
        for metric in metrics:
            left_value = _safe_float(left_row.get(metric)) if left_row is not None else 0.0
            right_value = _safe_float(right_row.get(metric)) if right_row is not None else 0.0
            record[f"{metric}_left"] = _round(left_value)
            record[f"{metric}_right"] = _round(right_value)
            record[f"{metric}_delta"] = _round(left_value - right_value)
        records.append(record)

    return sorted(records, key=lambda row: _safe_float(row.get(f"{sort_metric}_delta")), reverse=reverse)[:limit]


def _sku_burden_delta_records(
    left_frames: List[tuple[MonthKey, pd.DataFrame]],
    right_frames: List[tuple[MonthKey, pd.DataFrame]],
    *,
    metric: str,
    limit: int = 5,
) -> List[Dict[str, Any]]:
    records = _sku_delta_records(
        left_frames,
        right_frames,
        sort_metric=metric,
        reverse=False,
        limit=250,
    )

    def burden_delta(record: Dict[str, Any]) -> float:
        left_value = _burden_value(metric, _safe_float(record.get(f"{metric}_left")))
        right_value = _burden_value(metric, _safe_float(record.get(f"{metric}_right")))
        return left_value - right_value

    burdened_records = [
        record
        for record in records
        if burden_delta(record) > 0.005
    ]
    return sorted(burdened_records, key=burden_delta, reverse=True)[:limit]


def _candidate_identity(record: Dict[str, Any]) -> tuple[str, str]:
    return (
        str(record.get("sku") or "").strip().lower(),
        str(record.get("product_name") or "").strip().lower(),
    )


def _unique_sku_candidates(*groups: List[Dict[str, Any]], limit: int = 6) -> List[Dict[str, Any]]:
    seen: set[tuple[str, str]] = set()
    candidates: List[Dict[str, Any]] = []
    for group in groups:
        for record in group or []:
            identity = _candidate_identity(record)
            if not any(identity) or identity in seen:
                continue
            seen.add(identity)
            candidates.append({
                "sku": record.get("sku"),
                "product_name": record.get("product_name"),
            })
            if len(candidates) >= limit:
                return candidates
    return candidates


def _match_inventory_row(snapshot: Dict[str, Any], candidate: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    rows = snapshot.get("per_sku") or []
    sku = str(candidate.get("sku") or "").strip().lower()
    product_name = str(candidate.get("product_name") or "").strip().lower()

    if sku:
        for row in rows:
            if str(row.get("sku") or "").strip().lower() == sku:
                return row

    if product_name:
        for row in rows:
            row_name = str(row.get("product_name") or "").strip().lower()
            if row_name == product_name or product_name in row_name or row_name in product_name:
                return row

    return None


def _diagnosis_inventory_context(
    user_id: int,
    country: str,
    month_key: Optional[MonthKey],
    candidates: List[Dict[str, Any]],
) -> Dict[str, Any]:
    if not month_key or not candidates:
        return {"requested": False}

    snapshots: Dict[str, Dict[str, Any]] = {}
    for metric in ["available", "inbound_quantity", "days_of_supply"]:
        try:
            snapshots[metric] = get_inventory_snapshot(
                user_id=user_id,
                metric_name=metric,
                month=month_key.month,
                year=month_key.year,
                country=country,
            )
        except Exception:
            snapshots[metric] = {"metric": metric, "per_sku": [], "note": "Unavailable"}

    rows: List[Dict[str, Any]] = []
    for candidate in candidates:
        metric_values: Dict[str, Any] = {}
        source_period = None
        source_table = None
        for metric, snapshot in snapshots.items():
            matched = _match_inventory_row(snapshot, candidate)
            if not matched:
                continue
            metric_values[metric] = _round(matched.get("__metric__"))
            source_period = source_period or snapshot.get("period_label")
            source_table = source_table or snapshot.get("source_table")

        if metric_values:
            rows.append({
                "sku": candidate.get("sku"),
                "product_name": candidate.get("product_name"),
                "metrics": metric_values,
                "period_label": source_period,
                "source_table": source_table,
            })

    return {
        "requested": True,
        "country": country,
        "period_label": month_key.label,
        "rows": rows,
        "metrics_checked": list(snapshots.keys()),
        "available": bool(rows),
        "notes": [
            snapshot.get("note")
            for snapshot in snapshots.values()
            if snapshot.get("note")
        ],
    }


def _comparison_context(
    engine: Engine,
    user_id: int,
    country: str,
    payload: Optional[Dict[str, Any]],
    metric_names: List[str],
    product_query: Optional[str] = None,
) -> Dict[str, Any]:
    payload = payload or {}
    if payload.get("type") != "comparison":
        return {"requested": False}

    left_payload = _period_part_as_payload(payload.get("p1") or payload.get("left"))
    right_payload = _period_part_as_payload(payload.get("p2") or payload.get("right"))
    left_months = _months_from_payload(engine, user_id, country, left_payload)
    right_months = _months_from_payload(engine, user_id, country, right_payload)
    left_frames = _load_period_frames(engine, user_id, country, left_months)
    right_frames = _load_period_frames(engine, user_id, country, right_months)
    left_loaded = [month_key for month_key, _ in left_frames]
    right_loaded = [month_key for month_key, _ in right_frames]
    left_frames = _filter_period_frames_for_product(left_frames, product_query)
    right_frames = _filter_period_frames_for_product(right_frames, product_query)
    left_totals = _totals_from_frames(left_frames)
    right_totals = _totals_from_frames(right_frames)
    breakdown_availability = _product_breakdown_availability([*left_frames, *right_frames])

    comparison_metrics: List[str] = []
    for metric in [*metric_names, *COMPARISON_DRIVER_METRICS]:
        canonical = _canonical_driver_metric(metric)
        if canonical and canonical not in comparison_metrics:
            comparison_metrics.append(canonical)

    primary_metric = _canonical_driver_metric(metric_names[0]) if metric_names else None
    metric_drivers = _rank_metric_drivers(left_totals, right_totals, comparison_metrics, primary_metric)
    unfavorable_drivers = [driver for driver in metric_drivers if driver.get("unfavorable")]
    favorable_drivers = [driver for driver in metric_drivers if driver.get("favorable")]
    top_negative_profit_drivers = _sku_delta_records(left_frames, right_frames, sort_metric="profit", reverse=False)
    top_positive_profit_drivers = _sku_delta_records(left_frames, right_frames, sort_metric="profit", reverse=True)
    top_unit_loss_drivers = _sku_delta_records(left_frames, right_frames, sort_metric="total_quantity", reverse=False)
    top_order_loss_drivers = _sku_delta_records(left_frames, right_frames, sort_metric="quantity", reverse=False)
    top_cm2_loss_drivers = _sku_delta_records(left_frames, right_frames, sort_metric="cm2_profit", reverse=False)
    top_sales_loss_drivers = _sku_delta_records(left_frames, right_frames, sort_metric="net_sales", reverse=False)
    top_rebate_burden_drivers = _sku_burden_delta_records(left_frames, right_frames, metric="promotional_rebates")
    diagnosis_month = left_loaded[-1] if left_loaded else left_months[-1] if left_months else None
    diagnosis_inventory = _diagnosis_inventory_context(
        user_id,
        country,
        diagnosis_month,
        _unique_sku_candidates(
            top_negative_profit_drivers,
            top_unit_loss_drivers,
            top_order_loss_drivers,
            top_cm2_loss_drivers,
        ),
    )

    return {
        "requested": True,
        "left": {
            "label": _period_label(left_loaded or left_months),
            "months": [{"month": month.month, "year": month.year, "label": month.label} for month in left_loaded],
            "data_available": bool(left_frames),
        },
        "right": {
            "label": _period_label(right_loaded or right_months),
            "months": [{"month": month.month, "year": month.year, "label": month.label} for month in right_loaded],
            "data_available": bool(right_frames),
        },
        "metrics": {
            metric: _metric_comparison(left_totals, right_totals, metric)
            for metric in comparison_metrics
        },
        "metric_drivers": metric_drivers[:12],
        "unfavorable_metric_drivers": unfavorable_drivers[:8],
        "favorable_metric_drivers": favorable_drivers[:5],
        "driver_summary": [
            {
                "metric": driver.get("metric"),
                "label": driver.get("label"),
                "direction": driver.get("direction"),
                "business_delta": driver.get("business_delta"),
                "business_effect": driver.get("business_effect"),
                "impact_score": driver.get("impact_score"),
            }
            for driver in unfavorable_drivers[:5]
        ],
        "driver_scan_note": "Drivers are ranked across sales, units, ASP, rebates/discounts, refunds, returns, COGS, Amazon/FBA/selling/platform fees, ad spend/performance, CM2, and margins.",
        "total_only_metrics": breakdown_availability.get("total_only_metrics", []),
        "productwise_available_metrics": breakdown_availability.get("productwise_available_metrics", []),
        "product_scope": {
            "requested": bool(product_query),
            "query": product_query,
            "left_matched_rows": _sku_row_count(left_frames),
            "right_matched_rows": _sku_row_count(right_frames),
        },
        "top_negative_profit_drivers": top_negative_profit_drivers,
        "top_positive_profit_drivers": top_positive_profit_drivers,
        "top_unit_loss_drivers": top_unit_loss_drivers,
        "top_order_loss_drivers": top_order_loss_drivers,
        "top_cm2_loss_drivers": top_cm2_loss_drivers,
        "top_sales_loss_drivers": top_sales_loss_drivers,
        "top_rebate_burden_drivers": top_rebate_burden_drivers,
        "diagnosis_inventory": diagnosis_inventory,
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
        today = datetime.today()
        latest_is_current_request = bool(
            selected_months
            and selected_months[-1].year == today.year
            and selected_months[-1].month == today.month
        )
        all_months = get_last_n_month_keys(
            engine,
            user_id,
            country,
            periods,
            include_current_incomplete=latest_is_current_request,
        )
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
                "gross_sales": totals.get("gross_sales", 0.0),
                "net_sales": totals.get("net_sales", 0.0),
                "profit": totals.get("profit", 0.0),
                "profit_percentage": totals.get("profit_percentage", 0.0),
                "cm2_profit": totals.get("cm2_profit", 0.0),
                "total_cm2_profit": totals.get("total_cm2_profit", 0.0),
                "cm2_profit_per": totals.get("cm2_profit_per", 0.0),
                "quantity": totals.get("quantity", 0.0),
                "total_quantity": totals.get("total_quantity", 0.0),
                "return_quantity": totals.get("return_quantity", 0.0),
                "promotional_rebates": totals.get("promotional_rebates", 0.0),
                "asp": totals.get("asp", 0.0),
                "ads_spend": totals.get("ads_spend", 0.0),
                "ads_sale_amount": totals.get("ads_sale_amount", 0.0),
                "ads_roas": totals.get("ads_roas", 0.0),
                "tacos_total_advertising_cost_of_sale": totals.get("tacos_total_advertising_cost_of_sale", 0.0),
                "selling_fees": totals.get("selling_fees", 0.0),
                "fba_fees": totals.get("fba_fees", 0.0),
                "platform_fee": totals.get("platform_fee", 0.0),
                "platformfeenew": totals.get("platformfeenew", 0.0),
                "platform_fee_inventory_storage": totals.get("platform_fee_inventory_storage", 0.0),
                "other": totals.get("other", 0.0),
                "misc_transaction": totals.get("misc_transaction", 0.0),
                "fee_ratio": totals.get("fee_ratio", 0.0),
                "return_rate": totals.get("return_rate", 0.0),
            }
        )

    movement: Dict[str, Any] = {}
    if len(monthly_rows) >= 2:
        previous = monthly_rows[-2]
        current = monthly_rows[-1]
        for metric in [
            "gross_sales",
            "net_sales",
            "profit",
            "profit_percentage",
            "cm2_profit",
            "total_cm2_profit",
            "cm2_profit_per",
            "quantity",
            "total_quantity",
            "return_quantity",
            "promotional_rebates",
            "asp",
            "ads_spend",
            "ads_sale_amount",
            "ads_roas",
            "tacos_total_advertising_cost_of_sale",
            "selling_fees",
            "fba_fees",
            "platform_fee",
            "platformfeenew",
            "platform_fee_inventory_storage",
            "other",
            "misc_transaction",
            "fee_ratio",
            "return_rate",
        ]:
            delta = _safe_float(current.get(metric)) - _safe_float(previous.get(metric))
            movement[metric] = {
                "from": previous.get(metric),
                "to": current.get(metric),
                "delta": _round(delta),
                "pct_change": _round(_safe_div(delta, _safe_float(previous.get(metric)), 100.0)),
            }

    return {"months": monthly_rows, "movement": movement}


def _compact_inventory_snapshot(
    snapshot: Dict[str, Any],
    product_query: Optional[str],
    *,
    limit: int = 8,
) -> Dict[str, Any]:
    rows = snapshot.get("per_sku") or []
    product_text = (product_query or "").strip().lower()
    matched_rows: List[Dict[str, Any]] = []

    if product_text:
        matched_rows = [
            row for row in rows
            if product_text in str(row.get("product_name", "")).lower()
            or product_text in str(row.get("sku", "")).lower()
        ]

    selected_rows = matched_rows if matched_rows else rows[:limit]
    selected_rows = selected_rows[:limit]
    matched_total = sum(_safe_float(row.get("__metric__")) for row in matched_rows) if matched_rows else None

    return {
        "metric": snapshot.get("metric"),
        "total": _round(snapshot.get("total")),
        "period_label": snapshot.get("period_label"),
        "snapshot_date": snapshot.get("snapshot_date"),
        "source_table": snapshot.get("source_table"),
        "note": snapshot.get("note"),
        "row_count": len(rows),
        "matched_product_query": product_query,
        "matched_row_count": len(matched_rows),
        "matched_total": _round(matched_total) if matched_total is not None else None,
        "rows": [_clean_record(row) for row in selected_rows],
    }


def _inventory_context(
    user_id: int,
    country: str,
    month_key: MonthKey,
    metric_name: Optional[str],
    user_query: str,
    product_query: Optional[str] = None,
) -> Dict[str, Any]:
    query = (user_query or "").lower()
    business_inventory_triggers = [
        "increase",
        "improve",
        "grow",
        "sales",
        "underperform",
        "stockout",
        "stock out",
        "replenish",
        "reorder",
        "dispatch",
        "planning",
        "forecast",
    ]
    wants_inventory = (
        metric_name in INVENTORY_METRICS
        or any(word in query for word in ["stock", "inventory", "coverage", "sell through", "days of supply", "reserved"])
        or bool(product_query and any(word in query for word in business_inventory_triggers))
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
            snapshot = get_inventory_snapshot(
                user_id=user_id,
                metric_name=metric,
                month=month_key.month,
                year=month_key.year,
                country=country,
            )
            snapshots[metric] = _compact_inventory_snapshot(snapshot, product_query)
        except Exception:
            snapshots[metric] = {"metric": metric, "note": "Unavailable"}
    return {
        "requested": True,
        "period_label": month_key.label,
        "country": country,
        "product_query": product_query,
        "snapshots": snapshots,
    }


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
            "promotional_rebates",
            "platform_fee",
            "platformfeenew",
            "platform_fee_inventory_storage",
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
    requested_months = list(months)
    period_frames = _load_period_frames(engine, user_id, country, months)
    if not period_frames:
        latest = latest_available_month(engine, user_id, country)
        period_frames = _load_period_frames(engine, user_id, country, [latest])
        months = [latest]

    loaded_months = [month_key for month_key, _ in period_frames]
    scoped_period_frames = _filter_period_frames_for_product(period_frames, product_query)
    totals = _totals_from_frames(scoped_period_frames)
    sku_rows = _sku_frame(scoped_period_frames)
    action_lookup_rows = _sku_frame(period_frames)
    selected_metric_names = [metric for metric in (metric_names or []) if metric]
    if metric_name and metric_name not in selected_metric_names:
        selected_metric_names.insert(0, metric_name)
    comparison = _comparison_context(
        engine,
        user_id,
        country,
        period_payload,
        selected_metric_names,
        product_query=product_query,
    )

    latest_month = loaded_months[-1] if loaded_months else latest_available_month(engine, user_id, country)
    inventory_month = requested_months[-1] if requested_months else latest_month
    available_columns = sorted({column for _, frame in period_frames for column in frame.columns})
    product_breakdown = _product_breakdown_availability(scoped_period_frames)
    sku_product_names = {
        str(row.get("sku") or ""): str(row.get("product_name") or "")
        for row in action_lookup_rows[["sku", "product_name"]].to_dict(orient="records")
        if str(row.get("sku") or "").strip()
    } if not action_lookup_rows.empty and {"sku", "product_name"}.issubset(action_lookup_rows.columns) else {}
    sku_metric_lookup = _sku_action_metric_lookup(action_lookup_rows)
    product_skus = (
        [
            str(sku or "")
            for sku in sku_rows["sku"].tolist()
            if str(sku or "").strip()
        ]
        if product_query and not sku_rows.empty and "sku" in sku_rows.columns
        else []
    )
    live_ai_actions = _fetch_live_ai_summary_actions(
        user_id=user_id,
        country=country,
        months=requested_months or loaded_months,
        product_query=product_query,
        product_skus=product_skus,
        sku_product_names=sku_product_names,
        sku_metric_lookup=sku_metric_lookup,
    )

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
        "comparison": comparison,
        "focus_products": _focus_products(sku_rows, product_query),
        "live_ai_actions": live_ai_actions,
        "history": _history(engine, user_id, country, loaded_months),
        "inventory": _inventory_context(user_id, country, inventory_month, metric_name, user_query, product_query),
        "data_quality": {
            "row_count": int(len(sku_rows)),
            "has_sku_rows": bool(not sku_rows.empty),
            "product_scope": {
                "requested": bool(product_query),
                "query": product_query,
                "matched_row_count": int(len(sku_rows)),
            },
            "total_only_metrics": product_breakdown.get("total_only_metrics", []),
            "productwise_available_metrics": product_breakdown.get("productwise_available_metrics", []),
            "notes": [
                "Use general ecommerce guidance only when the required metric is absent or has no rows.",
                "Channel-wise ad spend is available through product_spend, display_spend, and brand_spend. Channel-wise ad sales should not be inferred unless matching sales columns exist.",
                "If a metric is listed in total_only_metrics, use the monthly total but do not claim a product/SKU breakdown for that metric.",
            ],
        },
    }
