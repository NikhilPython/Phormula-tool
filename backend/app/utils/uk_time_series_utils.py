from flask import Blueprint, request, jsonify
import jwt
import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from config import Config
from calendar import month_abbr, monthrange
from datetime import date, datetime, timedelta
from openai import OpenAI
import json
import pandas as pd
import numpy as np
from app.models.user_models import HistoricAISummary, UserObjective
from app.utils.formulas_utils import safe_num
from app.utils.uk_prompts_utils import AI_SYSTEM_PROMPT_1, AI_SYSTEM_PROMPT_2, AI_SYSTEM_PROMPT_3_POLISHER
from app import db
from openai import OpenAIError
from app.utils.uk_coverage_ratio_utils import compute_inventory_coverage_ratio



load_dotenv()
SECRET_KEY = Config.SECRET_KEY




db_url = os.getenv("DATABASE_URL")
phormula_engine = create_engine(db_url)


def rolling_months(anchor_year: int, anchor_month: int, max_months: int = 24):
    months = []
    y, m = anchor_year, anchor_month
    for _ in range(max_months):
        months.append((y, m))
        if m == 1:
            y -= 1
            m = 12
        else:
            m -= 1
    return list(reversed(months))  # oldest → newest


MONTH_NUM_TO_NAME = {
    1: "january",
    2: "february",
    3: "march",
    4: "april",
    5: "may",
    6: "june",
    7: "july",
    8: "august",
    9: "september",
    10: "october",
    11: "november",
    12: "december",
}


def month_name_from_timeline(timeline: str) -> str:
    # timeline is like "12"
    return MONTH_NUM_TO_NAME[int(timeline)]  # returns "december"

def build_table_name(user_id: int, country: str, period: str, timeline: str, year: int) -> str:
    c = str(country).lower()

    if period == "monthly":
        mn = month_name_from_timeline(timeline)   # "december"
        return f"skuwisemonthly_{user_id}_{c}_{mn}{year}"

    if period == "quarterly":
        q = int(str(timeline).replace("Q", ""))   # Q1 -> 1
        return f"quarter{q}_{user_id}_{c}_{year}_table"

    if period == "yearly":
        return f"skuwiseyearly_{user_id}_{c}_{year}_table"

    raise ValueError("Invalid period")


def fetch_precalc_table(user_id: int, country: str, period: str, timeline: str, year: int) -> pd.DataFrame:
    table = build_table_name(user_id, country, period, timeline, year)
    query = f'SELECT * FROM public."{table}"'

    try:
        return pd.read_sql(query, phormula_engine)
    except Exception as e:
        print(f"[WARN] Could not read table {table}: {e}")
        return pd.DataFrame()
    


def _normalize_sku_col(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    df = df.copy()
    if "sku" not in df.columns and "SKU" in df.columns:
        df.rename(columns={"SKU": "sku"}, inplace=True)
    return df



def build_rolling_sku_series(
    user_id: int,
    country: str,
    sku: str,
    anchor_year: int,
    anchor_month: int
):
    series = []

    for y, m in rolling_months(anchor_year, anchor_month, 24):
        df = fetch_precalc_table(
            user_id=user_id,
            country=country,
            period="monthly",
            timeline=str(m),
            year=y
        )

        if df.empty:
            continue

        df = _normalize_sku_col(df)
        row = df[df["sku"] == sku]

        if row.empty:
            continue

        series.append({
            "year": y,
            "month": m,
            "units": safe_num(row["total_quantity"].iloc[0]),
            "asp": safe_num(row["asp"].iloc[0]),
            "cm1_profit": safe_num(row["profit"].iloc[0]),
        })

    return series


METRIC_COLUMNS = {
    "quantity",
    "return_quantity",
    "total_quantity",

    "gross_sales",
    "refund_sales",
    "net_sales",


    "platformfeenew",
    "platform_fee_inventory_storage",
    "other_transaction_fees",
    "misc_transaction",


    "net_taxes",
    "net_credits",


    "advertising_total",

    "lost_total",
    "rembursement_fee",

    "profit",
    "cm2_profit",
 
    
}


def get_metric_columns(df: pd.DataFrame) -> list[str]:
    return [c for c in df.columns if c.lower() in METRIC_COLUMNS]



def compute_sku_precalc(df: pd.DataFrame) -> dict:
    if df.empty:
        return {}
    df = _normalize_sku_col(df)
    if "sku" not in df.columns:
        return {}

    num_cols = get_metric_columns(df)

    # Metrics that should NOT be summed at SKU level
    non_additive_cols = {"asp"}

    other_cols = [
        c for c in df.columns
        if c not in num_cols and c not in non_additive_cols and c != "sku"
    ]

    agg = {c: "sum" for c in num_cols}

    # SKU-level ASP should be kept as-is (not summed)
    if "asp" in df.columns:
        agg["asp"] = "first"

    for c in other_cols:
        agg[c] = "first"

    g = df.groupby("sku", dropna=False).agg(agg).reset_index()

    out = {}
    for r in g.to_dict(orient="records"):

        sku = str(r["sku"])

        # ✅ product_name fallback logic
        raw_name = r.get("product_name")
        product_name = (
            str(raw_name).strip()
            if raw_name not in [None, "", "0", 0]
            else sku
        )

        out[sku] = {
            "product_name": product_name  # 👈 ADD THIS
        }

        for col in g.columns:
            if col in ["sku", "product_name"]:
                continue

            val = r[col]
            if isinstance(val, (int, float)) and pd.notna(val):
                out[sku][col.lower()] = round(float(val), 2)
            else:
                out[sku][col.lower()] = None if pd.isna(val) else val

    return out




def build_remaining_skus_time_series(
    user_id: int,
    country: str,
    focus_skus: list[str],
    anchor_year: int,
    anchor_month: int,
    months: int = 24
) -> list[dict]:

    series = []
    for y, m in rolling_months(anchor_year, anchor_month, months):
        df = fetch_precalc_table(user_id, country, "monthly", str(m), y)
        if df.empty:
            continue

        df_detail, _ = _split_total_row(df)
        sku_month = compute_sku_precalc(df_detail)

        # aggregate "remaining" SKUs for this month (no previous needed)
        # we only need current month snapshot metrics for the journey
        agg = build_remaining_skus_aggregate(
            sku_current=sku_month,
            sku_prev={},
            focus_skus=focus_skus
        )
        if not agg:
            continue

        series.append({
            "year": y,
            "month": m,
            "units": agg.get("total_quantity", {}).get("current"),
            "asp": agg.get("asp", {}).get("current"),
            "cm1_profit": agg.get("profit", {}).get("current"),
        })

    return series

TOTAL_LABELS = {"total", "grand total", "overall", "all"}

def _split_total_row(df: pd.DataFrame):
    """
    Returns: (detail_rows_df, total_row_df_or_empty)
    """
    if df.empty:
        return df, pd.DataFrame()

    df = _normalize_sku_col(df.copy())

    if "sku" not in df.columns:
        return df, pd.DataFrame()

    sku_norm = df["sku"].astype(str).str.strip().str.lower()
    is_total = sku_norm.isin(TOTAL_LABELS)

    df_total = df[is_total].copy()
    df_detail = df[~is_total].copy()

    # if multiple total rows exist, keep just 1 to avoid double count
    if not df_total.empty:
        df_total = df_total.head(1)

    return df_detail, df_total


def build_remaining_skus_aggregate(
    sku_current: dict,
    sku_prev: dict,
    focus_skus: list[str],
) -> dict:

    focus_set = set(str(s) for s in (focus_skus or []))

    remaining = [
        sku for sku in (set(sku_current.keys()) | set(sku_prev.keys()))
        if str(sku) not in focus_set
        and str(sku).strip().lower() not in TOTAL_LABELS
    ]

    if not remaining:
        return {}

    def sum_metric(source: dict, metric: str) -> float:
        total = 0.0
        for sku in remaining:
            try:
                total += float(source.get(sku, {}).get(metric, 0.0) or 0.0)
            except (TypeError, ValueError):
                continue
        return round(total, 2)

    # --- additive ---
    cur_units = sum_metric(sku_current, "total_quantity")
    prev_units = sum_metric(sku_prev, "total_quantity")

    cur_sales = sum_metric(sku_current, "net_sales")
    prev_sales = sum_metric(sku_prev, "net_sales")

    cur_profit = sum_metric(sku_current, "profit")
    prev_profit = sum_metric(sku_prev, "profit")

    # --- recalculated ---
    cur_asp = round(cur_sales / cur_units, 2) if cur_units else None
    prev_asp = round(prev_sales / prev_units, 2) if prev_units else None

    cur_ppu = round(cur_profit / cur_units, 2) if cur_units else None
    prev_ppu = round(prev_profit / prev_units, 2) if prev_units else None

    def mk(cur, prev):
        if cur is None or prev is None:
            return {
                "current": cur,
                "previous": prev,
                "delta": None,
                "delta_pct": None
            }

        delta = round(cur - prev, 2)
        pct = round((delta / prev) * 100, 2) if prev != 0 else None

        return {
            "current": round(cur, 2),
            "previous": round(prev, 2),
            "delta": delta,
            "delta_pct": pct
        }

    return {
        "product_name": "Other SKUs",
        "total_quantity": mk(cur_units, prev_units),
        "net_sales": mk(cur_sales, prev_sales),
        "profit": mk(cur_profit, prev_profit),
        "asp": mk(cur_asp, prev_asp),
        "unit_wise_profitability": mk(cur_ppu, prev_ppu),
    }
