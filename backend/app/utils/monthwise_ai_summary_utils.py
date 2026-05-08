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
from app.utils.uk_prompts_utils import AI_SYSTEM_PROMPT_1, AI_SYSTEM_PROMPT_2, AI_SYSTEM_PROMPT_3_POLISHER, AI_GLOBAL_COMPARISON_PROMPT, get_excel_recommendation_from_metrics
from app import db
from openai import OpenAIError
from app.utils.uk_coverage_ratio_utils import compute_inventory_coverage_ratio




load_dotenv()
SECRET_KEY = Config.SECRET_KEY

db_url = os.getenv("DATABASE_URL")
db_url2 = os.getenv("DATABASE_CHATBOT_URL")
db_url3 = os.getenv("DATABASE_AMAZON_URL")
phormula_engine = create_engine(db_url)
chatbot_engine = create_engine(db_url2)
amazon_engine = create_engine(db_url3)
openai_client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))


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


DEFAULT_OBJECTIVE_V2 = {
    "growth_intent": "aggressive",  # conservative | balanced | aggressive
    "profit_priority": "high",  # high | protect_growth | sacrifice_short_term
    "inventory_clearance_priority": False,
    "business_context": None,
    "country": None,
    "time_horizon": "1_month"
}


def resolve_yearly_analysis_anchor(user_id: int, country: str, year: int):
    """
    For a yearly selection, anchor insights to the latest available month
    within that year (e.g., Dec if present, else latest month with data).
    Returns (year, month) or None if no monthly data exists for that year.
    """
    for m in range(12, 0, -1):
        df = fetch_precalc_table(
            user_id=user_id,
            country=country,
            period="monthly",
            timeline=str(m),
            year=year
        )
        if not df.empty:
            return year, m
    return None


def severity_suffix(severity: str | None, *, period: str | None = None) -> str:
    """
    Suppress rolling 24-month severity labels for YEARLY reports.
    """
    if period in ("yearly", "quarterly"):
        return ""

    if not severity or severity == "normal":
        return ""

    return f" ({severity.replace('_', ' ').replace('24m', '24 months')})"


def resolve_latest_available_month(user_id: int, country: str):
    """
    Returns the latest (year, month) for which a precalc table actually exists.
    Falls back safely to latest completed month.
    """
    y, m = get_latest_completed_month()

    # Try current completed month first, then walk backwards
    for _ in range(24):
        table = build_table_name(
            user_id=user_id,
            country=country,
            period="monthly",
            timeline=str(m),
            year=y
        )
        try:
            pd.read_sql(
                f'SELECT 1 FROM public."{table}" LIMIT 1',
                phormula_engine
            )
            return y, m
        except Exception:
            pass

        if m == 1:
            y -= 1
            m = 12
        else:
            m -= 1

    # Absolute fallback (should never happen)
    return get_latest_completed_month()


def get_latest_completed_month(today=None):
    today = today or date.today()
    if today.month == 1:
        return today.year - 1, 12
    return today.year, today.month - 1

def resolve_latest_two_months():
    """
    Canonical advisor time resolver.
    Always returns latest completed month for MoM analysis.
    """
    year, month = get_latest_completed_month()
    return "monthly", str(month), year


def get_latest_completed_quarter(today=None):
    today = today or date.today()
    q = (today.month - 1) // 3 + 1
    if q == 1:
        return today.year - 1, 4
    return today.year, q - 1


def is_latest_period(period, timeline, year, *, user_id, country):
    """
    Returns True if the requested period corresponds to the
    latest finalized precalc data available for the user.
    """

    # ---------------- MONTHLY (DB-driven) ----------------
    if period == "monthly":
        latest_year, latest_month = resolve_latest_available_month(
            user_id=user_id,
            country=country
        )
        return year == latest_year and int(timeline) == latest_month


    # ---------------- QUARTERLY (DB-driven FIX) ----------------
    if period == "quarterly":
        # scan recent years & quarters to find latest existing table
        for test_year in range(year + 1, year - 3, -1):
            for q in (4, 3, 2, 1):
                table = build_table_name(user_id, country, "quarterly", f"Q{q}", test_year)
                try:
                    pd.read_sql(
                        f'SELECT 1 FROM public."{table}" LIMIT 1',
                        phormula_engine
                    )
                    return timeline == f"Q{q}" and year == test_year
                except Exception:
                    continue
        return False


    # ---------------- YEARLY (keep simple rule) ----------------
    if period == "yearly":
        # Detect latest yearly table that actually exists in DB
        for test_year in range(date.today().year + 1, date.today().year - 5, -1):
            table = build_table_name(user_id, country, "yearly", "ALL", test_year)
            try:
                pd.read_sql(
                    f'SELECT 1 FROM public."{table}" LIMIT 1',
                    phormula_engine
                )
                # First existing table = TRUE latest year
                return year == test_year
            except Exception:
                continue

        return False


def fetch_existing_summary(user_id, country, marketplace_id, period, timeline, year):
    return HistoricAISummary.query.filter_by(
        user_id=user_id,
        country=country,
        marketplace_id=marketplace_id,
        period=period,
        timeline=timeline,
        year=year
    ).first()

def save_summary_to_db(data: dict):
    # ---------------- HARD SANITIZE INPUT ----------------
    recos = data.get("recommendations")
    if isinstance(recos, dict):
        recos = json.dumps(recos)
    elif recos is None:
        recos = json.dumps({})

    # 🔒 DETACH any previously loaded instance (CRITICAL)
    db.session.expire_all()

    row = HistoricAISummary.query.filter_by(
        user_id=data["user_id"],
        country=data["country"],
        marketplace_id=data.get("marketplace_id"),
        period=data["period"],
        timeline=data["timeline"],
        year=data["year"],
    ).first()

    if not row:
        row = HistoricAISummary(
            user_id=data["user_id"],
            country=data["country"],
            marketplace_id=data.get("marketplace_id"),
            period=data["period"],
            timeline=data["timeline"],
            year=data["year"],
        )
        db.session.add(row)

    # ---------------- OBJECTIVE ----------------
    row.primary_goal = data.get("primary_goal")
    row.risk_level = data.get("risk_level")
    row.max_tacos = data.get("max_tacos")
    row.max_price_increase_pct = data.get("max_price_increase_pct")
    row.ad_budget_cap = data.get("ad_budget_cap")
    row.dont_change_price = data.get("dont_change_price")
    row.notes = data.get("notes")

    # ---------------- OUTPUT ----------------
    row.summary = data.get("summary") or row.summary

    # 🔒 ABSOLUTE GUARANTEE: STRING ONLY
    row.recommendations = recos

    db.session.commit()


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
        
        return pd.DataFrame()

def build_rolling_monthly_series(
    user_id: int,
    country: str,
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

        _, df_total = _split_total_row(df)
        if df_total.empty:
            continue

        snapshot = extract_total_snapshot(df_total)

        # TEMPORARY SAFE CHECK (prevents crash so we can see prints)
        if not isinstance(snapshot, dict) or not snapshot:
            print("❌ Skipping due to invalid snapshot")
            continue


        series.append({
            "year": y,
            "month": m,
            "values": snapshot
        })

    return series

def fetch_month_end_inventory_lookup(user_id: int):

    query = text("""
        SELECT
            msku,
            disposition,
            date,
            ending_warehouse_balance
        FROM monthwise_inventory
        WHERE user_id = :user_id
    """)

    with amazon_engine.connect() as conn:
        df = pd.read_sql(query, conn, params={"user_id": user_id})

    if df.empty:
        return {}

    df["date"] = pd.to_datetime(df["date"])
    df["year"] = df["date"].dt.year
    df["month"] = df["date"].dt.month

    df = df.sort_values("date")

    # take LAST snapshot per sku + disposition + month
    month_end = (
        df.groupby(["msku", "disposition", "year", "month"], as_index=False)
        .last()
    )

    lookup = {}

    for _, r in month_end.iterrows():

        key = (str(r["msku"]), int(r["year"]), int(r["month"]))

        if key not in lookup:
            lookup[key] = {
                "sellable_inventory": 0,
                "damaged_inventory": 0,
                "expired_inventory": 0
            }

        disp = str(r["disposition"]).upper()
        units = float(r["ending_warehouse_balance"])

        if disp == "SELLABLE":
            lookup[key]["sellable_inventory"] += units

        elif disp in ["DEFECTIVE", "WAREHOUSE_DAMAGED", "CUSTOMER_DAMAGED"]:
            lookup[key]["damaged_inventory"] += units

        elif disp in ["EXPIRED"]:
            lookup[key]["expired_inventory"] += units

    return lookup

def build_rolling_sku_series(
    user_id: int,
    country: str,
    sku: str,
    anchor_year: int,
    anchor_month: int
):

    series = []

    # 🔹 fetch inventory once
    inventory_lookup = fetch_month_end_inventory_lookup(user_id)

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

        # 🔹 inventory lookup
        inv = inventory_lookup.get((sku, y, m), {})

        series.append({
            "year": y,
            "month": m,

            "units": float(safe_num(row["total_quantity"].iloc[0]).iloc[0]),
            "asp": round(float(safe_num(row["asp"].iloc[0]).iloc[0]), 2),
            "cm1_profit": float(safe_num(row["profit"].iloc[0]).iloc[0]),
            "net_sales": float(safe_num(row["net_sales"].iloc[0]).iloc[0]),
            "unit_wise_profitability": float(safe_num(row["unit_wise_profitability"].iloc[0]).iloc[0]),
            "sales_mix": float(safe_num(row["sales_mix"].iloc[0]).iloc[0]),
            "profit_mix": float(safe_num(row["profit_mix"].iloc[0]).iloc[0]),

            # 🔹 NEW inventory fields
            "sellable_inventory": inv.get("sellable_inventory"),
            "damaged_inventory": inv.get("damaged_inventory"),
            "expired_inventory": inv.get("expired_inventory")
        })

    
    return series

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
             "net_sales": agg.get("net_sales", {}).get("current"),
            "unit_wise_profitability": agg.get("unit_wise_profitability", {}).get("current"),
            "sales_mix": agg.get("sales_mix", {}).get("current"),
            "profit_mix": agg.get("profit_mix", {}).get("current"),
        })

    return series

def compute_generic_movement(series: list, col: str):
    points = []

    # Build MoM % series WITH month identity
    for i in range(1, len(series)):
        prev = series[i - 1]["values"].get(col)
        cur = series[i]["values"].get(col)

        if prev is None or prev == 0 or cur is None:
            continue

        if col == "platform_fee_inventory_storage":
            pct = (abs(cur) - abs(prev)) / abs(prev) * 100
        else:
            pct = (cur - prev) / abs(prev) * 100

        points.append({
            "year": series[i]["year"],
            "month": series[i]["month"],
            "pct_change": pct
        })

    if len(points) < 1:
        return None

    current = points[-1]
    mom_pct = current["pct_change"]

    # 🔑 GLOBAL extreme detection (not rolling-rank)
    max_point = max(points, key=lambda x: abs(x["pct_change"]))
    min_point = min(points, key=lambda x: abs(x["pct_change"]))

    severity = "normal"
    if mom_pct == max_point["pct_change"]:
        severity = "highest_24m"
    elif mom_pct == min_point["pct_change"]:
        severity = "lowest_24m"

    direction = "up" if mom_pct > 0 else "down" if mom_pct < 0 else "flat"

    # Pattern logic (unchanged)
    pattern = None
    if len(points) >= 2:
        prev_change = points[-2]["pct_change"]
        if prev_change > 0 and mom_pct < 0:
            pattern = "reversal_down"
        elif prev_change < 0 and mom_pct > 0:
            pattern = "reversal_up"
        elif prev_change > 0 and mom_pct > 0:
            pattern = "continued_up"
        elif prev_change < 0 and mom_pct < 0:
            pattern = "continued_down"

    return {
        "delta_pct": round(mom_pct, 2),
        "direction": direction,
        "severity": severity,
        "pattern": pattern
    }

def extract_rolling_extremes(rolling_series: list):
    """
    Returns month-level extreme movements for executive synthesis.
    """
    extremes = {}

    for col in MOVEMENT_COLUMNS:
        points = []

        for i in range(1, len(rolling_series)):
            prev = rolling_series[i - 1]["values"].get(col)
            cur = rolling_series[i]["values"].get(col)

            if prev is None or prev == 0 or cur is None:
                continue

            if col == "platform_fee_inventory_storage":
                pct = (abs(cur) - abs(prev)) / abs(prev) * 100
            else:
                pct = (cur - prev) / abs(prev) * 100

            points.append({
                "year": rolling_series[i]["year"],
                "month": rolling_series[i]["month"],
                "pct_change": round(pct, 2)
            })

        if not points:
            continue

        max_point = max(points, key=lambda x: abs(x["pct_change"]))

        extremes[col] = {
            "year": max_point["year"],
            "month": max_point["month"],
            "pct_change": max_point["pct_change"]
        }

    return extremes

def build_yearly_temporal_signals(rolling_series: list) -> dict:
    """
    Derives executive-level yearly storytelling signals from
    monthly rolling totals.

    SAFE:
    - Purely additive
    - No mutation of existing logic
    - Returns empty dict if insufficient data
    """

    if not rolling_series or len(rolling_series) < 6:
        return {}

    def _get(col, item):
        v = item["values"].get(col)
        return float(v) if isinstance(v, (int, float)) else None

    # ---------- Peak & Weak Month (Net Sales) ----------
    sales_points = [
        (r["year"], r["month"], _get("net_sales", r))
        for r in rolling_series
        if _get("net_sales", r) is not None
    ]

    if not sales_points:
        return {}

    peak = max(sales_points, key=lambda x: x[2])
    weak = min(sales_points, key=lambda x: x[2])

    # ---------- H1 vs H2 Trend ----------
    mid = len(rolling_series) // 2

    def _avg(col, subset):
        vals = [_get(col, r) for r in subset if _get(col, r) is not None]
        return sum(vals) / len(vals) if vals else None

    h1_sales = _avg("net_sales", rolling_series[:mid])
    h2_sales = _avg("net_sales", rolling_series[mid:])

    if h1_sales is None or h2_sales is None:
        h1_h2_direction = None
    elif h2_sales > h1_sales:
        h1_h2_direction = "improving"
    elif h2_sales < h1_sales:
        h1_h2_direction = "softening"
    else:
        h1_h2_direction = "flat"

    # ---------- ACOS Trend ----------
    acos_start = _get("acos", rolling_series[0])
    acos_end = _get("acos", rolling_series[-1])

    if acos_start is None or acos_end is None:
        acos_trend = None
    elif acos_end < acos_start:
        acos_trend = "improving"
    elif acos_end > acos_start:
        acos_trend = "deteriorating"
    else:
        acos_trend = "flat"

    # ---------- CM2 Trend ----------
    cm2_start = _get("cm2_profit", rolling_series[0])
    cm2_end = _get("cm2_profit", rolling_series[-1])

    if cm2_start is None or cm2_end is None:
        cm2_trend = None
    elif cm2_end > cm2_start:
        cm2_trend = "improving"
    elif cm2_end < cm2_start:
        cm2_trend = "declining"
    else:
        cm2_trend = "flat"

    return {
        "peak_month_sales": {"year": peak[0], "month": peak[1]},
        "weak_month_sales": {"year": weak[0], "month": weak[1]},
        "h1_vs_h2_direction": h1_h2_direction,
        "acos_trend_direction": acos_trend,
        "cm2_trend_direction": cm2_trend,
    }

def compute_period_pct_changes(df_current_total, df_prev_total):

    def pct(col):
        cur = _total_value(df_current_total, col)
        prev = _total_value(df_prev_total, col)
        if cur is None or prev in (None, 0):
            return None
        return round((cur - prev) / abs(prev) * 100, 2)

    def pct_point(col):  # ← NEW
        cur = _total_value(df_current_total, col)
        prev = _total_value(df_prev_total, col)
        if cur is None or prev is None:
            return None
        return round(cur - prev, 2)

    # ✅ helper only for storage cost (because DB stores it negative)
    def pct_storage(col):
        cur = _total_value(df_current_total, col)
        prev = _total_value(df_prev_total, col)

        
        if cur is None or prev in (None, 0):
            print("DEBUG STORAGE: missing values or prev=0")
            return None

        cur_abs = abs(cur)
        prev_abs = abs(prev)

        pct_val = round((cur_abs - prev_abs) / prev_abs * 100, 2)



        return pct_val

    return {
        "units": pct("total_quantity"),
        "net_sales": pct("net_sales"),
        "asp": pct("asp"),
        "cm1_profit": pct("profit"),
        "cm1_profit_per_unit": pct("unit_wise_profitability"),
        "cm2_profit": pct("cm2_profit"),
        "advertising": pct("advertising_total"),

        # ✅ FIXED storage calculation
        "storage_fees": pct_storage("platform_fee_inventory_storage"),

        "acos": pct_point("acos"),
    }


def build_movement_context(rolling_series: list):
    ctx = {}
    for col in MOVEMENT_COLUMNS:
        m = compute_generic_movement(rolling_series, col)
        if m:
            ctx[col] = m
    return ctx


def _normalize_sku_col(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    df = df.copy()
    if "sku" not in df.columns and "SKU" in df.columns:
        df.rename(columns={"SKU": "sku"}, inplace=True)
    return df

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


def _total_value(df_total: pd.DataFrame, col: str):
    """
    Returns float value from total row column if present else None
    """
    if df_total.empty or col not in df_total.columns:
        return None
    return float(pd.to_numeric(df_total[col], errors="coerce").fillna(0).iloc[0])

def compute_period_absolute_changes(df_current_total, df_prev_total):
    def diff(col):
        cur = _total_value(df_current_total, col)
        prev = _total_value(df_prev_total, col)
        if cur is None or prev is None:
            return None
        return round(cur - prev, 2)

    def pct_point_diff(col):
        cur = _total_value(df_current_total, col)
        prev = _total_value(df_prev_total, col)
        if cur is None or prev is None:
            return None
        return round(cur - prev, 2)

    return {
        "units": diff("total_quantity"),
        "net_sales": diff("net_sales"),
        "asp": diff("asp"),

        "cm1_profit": diff("profit"),
        "cm1_profit_per_unit": diff("unit_wise_profitability"),
        "cm2_profit": diff("cm2_profit"),

        "advertising": diff("advertising_total"),
        "storage_fees": diff("platform_fee_inventory_storage"),

        
        "misc_transaction": diff("misc_transaction"),

        "acos": pct_point_diff("acos"),
    }



def extract_total_snapshot(df_total: pd.DataFrame) -> dict:
    snapshot = {}
    if df_total.empty:
        return snapshot

    for col in MOVEMENT_COLUMNS:
        if col in df_total.columns:
            val = _total_value(df_total, col)
            if val is not None:
                snapshot[col] = float(val)

    return snapshot



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

PERCENTAGE_COLUMNS = {
    "acos",
    "profit_percentage",
    "cm2_profit_percentage",
    "promotional_rebates_percentage",

    "sales_mix",
    "profit_mix",
}

NON_ADDITIVE_COMPARABLE = {"asp", "unit_wise_profitability"}

MOVEMENT_COLUMNS = METRIC_COLUMNS | PERCENTAGE_COLUMNS | NON_ADDITIVE_COMPARABLE




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

def fetch_inventory_aged_by_user(user_id: int, country: str | None = None) -> pd.DataFrame:
    currency = None

    if country:
        country = str(country).lower()
        if country == "uk":
            currency = "GBP"
        elif country == "us":
            currency = "USD"

    params = {"user_id": user_id}

    where_clause = "WHERE user_id = :user_id"

    if currency:
        where_clause += " AND UPPER(TRIM(currency)) = :currency"
        params["currency"] = currency

    query = text(f"""
        SELECT
            sku,
            currency,
            "inv-age-0-to-90-days"        AS age_0_90,
            "inv-age-91-to-180-days"      AS age_91_180,
            "inv-age-181-to-270-days"     AS age_181_270,
            "inv-age-271-to-365-days"     AS age_271_365,
            "inv-age-365-plus-days"       AS age_365_plus,
            "estimated-storage-cost-next-month" AS storage_cost_next_month,
            "unfulfillable-quantity"      AS unfulfillable_qty
        FROM public.inventory_aged
        {where_clause}
    """)

    with amazon_engine.connect() as conn:
        df = pd.read_sql(query, conn, params=params)

    return df


def build_inventory_alerts(df: pd.DataFrame, user_id: int, country: str) -> dict:

    if df.empty:
        return {}

    df = df.copy()

    # Safe numeric coercion
    for col in [
        "age_0_90", "age_91_180", "age_181_270",
        "age_271_365", "age_365_plus",
        "storage_cost_next_month", "unfulfillable_qty"
    ]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    alerts = {}

    # ==========================================================
    # 1️⃣ AGEING INVENTORY (181+ DAYS)
    # ==========================================================
    df["aged_181_plus"] = df["age_181_270"] + df["age_271_365"]

    ageing_units = int(df["aged_181_plus"].sum())
    ageing_skus = int((df["aged_181_plus"] > 0).sum())

    alerts["ageing_inventory"] = {
        "total_units": ageing_units,
        "total_skus": ageing_skus
    }

    # ==========================================================
    # 2️⃣ HIGH COVERAGE RATIO SKUS (>4)
    # ==========================================================
    coverage_df = compute_inventory_coverage_ratio(user_id, country)

    if not coverage_df.empty:
        high_coverage = coverage_df[
            coverage_df["inventory_coverage_ratio"] > 4
        ][["sku", "inventory_coverage_ratio"]]

        alerts["high_coverage"] = {
            "count": int(len(high_coverage)),
            "skus": high_coverage.to_dict(orient="records")
        }
    else:
        alerts["high_coverage"] = {
            "count": 0,
            "skus": []
        }

    # ==========================================================
    # 3️⃣ UNFULFILLABLE % CHECK
    # ==========================================================
    total_inventory_units = int(
        df[
            [
                "age_0_90", "age_91_180",
                "age_181_270", "age_271_365",
                "age_365_plus"
            ]
        ].sum().sum()
    )

    unfulfillable_units = int(df["unfulfillable_qty"].sum())

    unfulfillable_pct = (
        (unfulfillable_units / total_inventory_units) * 100
        if total_inventory_units > 0 else 0
    )

    alerts["unfulfillable"] = {
        "units": unfulfillable_units,
        "percentage": round(unfulfillable_pct, 2),
        "status": "above_1_percent" if unfulfillable_pct > 1 else "below_1_percent"
    }

    # ==========================================================
    # 4️⃣ ESTIMATED STORAGE COST
    # ==========================================================
    total_storage_cost = float(df["storage_cost_next_month"].sum())

    alerts["estimated_storage_cost"] = {
        "value": round(total_storage_cost, 2)
    }

    return alerts

def compare_sku_metrics(current: dict, previous: dict) -> dict:
    output = {}

    all_skus = set(current.keys()) | set(previous.keys())

    for sku in all_skus:
        curr = current.get(sku, {})
        prev = previous.get(sku, {})

        sku_out = {}

        # ✅ PRESERVE PRODUCT NAME (CRITICAL)
        sku_out["product_name"] = (
            curr.get("product_name")
            or prev.get("product_name")
            or sku
        )

        # ---------------- ADDITIVE METRICS ----------------
        for metric in METRIC_COLUMNS:
            if metric not in curr and metric not in prev:
                continue

            try:
                new = float(curr.get(metric, 0.0) or 0.0)
                old = float(prev.get(metric, 0.0) or 0.0)
            except (TypeError, ValueError):
                continue

            delta = new - old
            pct = (delta / old * 100) if old != 0 else None

            sku_out[metric] = {
                "current": round(new, 2),
                "previous": round(old, 2),
                "delta": round(delta, 2),
                "delta_pct": round(pct, 2) if pct is not None else None
            }

        # ---------------- PERCENTAGE METRICS ----------------
        for metric in PERCENTAGE_COLUMNS:
            if metric not in curr and metric not in prev:
                continue

            try:
                new = float(curr.get(metric))
                old = float(prev.get(metric))
            except (TypeError, ValueError):
                continue

            delta = new - old

            sku_out[metric] = {
                "current": round(new, 2),
                "previous": round(old, 2),
                "delta": round(delta, 2),   # percentage-point change
                "delta_pct": None           # intentionally skipped
            }

                # ---------------- NON-ADDITIVE COMPARABLE METRICS (ASP) ----------------
        for metric in NON_ADDITIVE_COMPARABLE:
            if metric not in curr and metric not in prev:
                continue

            try:
                new = float(curr.get(metric, 0.0) or 0.0)
                old = float(prev.get(metric, 0.0) or 0.0)
            except (TypeError, ValueError):
                continue

            delta = new - old
            pct = (delta / old * 100) if old != 0 else None

            sku_out[metric] = {
                "current": round(new, 2),
                "previous": round(old, 2),
                "delta": round(delta, 2),
                "delta_pct": round(pct, 2) if pct is not None else None
            }
    

        output[sku] = sku_out

    return output

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

def compute_yoy_pct(df_current_total, df_prev_total, col):
    cur = _total_value(df_current_total, col)
    prev = _total_value(df_prev_total, col)
    if cur is None or prev in (None, 0):
        return None
    return round((cur - prev) / abs(prev) * 100, 2)

def period_label(period: str, timeline: str, year: int) -> str:
    if period == "monthly":
        return f"{MONTH_NUM_TO_NAME[int(timeline)].title()} {year}"   # "December 2025"
    if period == "quarterly":
        return f"{timeline} {year}"                                   # "Q4 2025"
    if period == "yearly":
        return f"{year}"                                              # "2025"
    return f"{period} {timeline} {year}"

def resolve_comparison(period, timeline, year):
    if period == "monthly":
        m = int(timeline)
        prev = ("monthly", "12", year - 1) if m == 1 else ("monthly", str(m - 1), year)
        return prev, None   # ✅ NO YoY

    if period == "quarterly":
        q = int(timeline.replace("Q", ""))
        prev = ("quarterly", "Q4", year - 1) if q == 1 else ("quarterly", f"Q{q-1}", year)
        return prev, None   # ✅ NO YoY

    if period == "yearly":
        return ("yearly", "ALL", year - 1), None

    raise ValueError("Invalid period")

def run_prompt_1_analysis(ai_payload):
    resp = openai_client.chat.completions.create(
        model="gpt-4.1",
        messages=[
            {"role": "system", "content": AI_SYSTEM_PROMPT_1},
            {"role": "user", "content": json.dumps(ai_payload, separators=(",", ":"))}
        ],
        temperature=0.2,
    )
    return resp.choices[0].message.content.strip()

def run_prompt_2_strategy(
    analysis_insights: dict,
    objective_v2: dict,
    focus_skus: list,
    sku_time_series: dict,
    inventory_alerts: dict,
    country: str,
    sku_mom: dict | None = None,

    # ✅ NEW (ads context)
    sku_ads_context: list | None = None,
    ads_monthly: dict | None = None,
    sku_live_context: list | None = None,
    sku_inventory_flags: dict | None = None,
    remaining_skus_context: dict | None = None,

):

    # -------------------------------------------------
    # Universal JSON sanitizer (handles pandas/numpy)
    # -------------------------------------------------
    def _make_json_safe(obj):
        if isinstance(obj, pd.Series):
            return _make_json_safe(obj.iloc[0] if not obj.empty else None)

        if isinstance(obj, (np.integer,)):
            return int(obj)

        if isinstance(obj, (np.floating,)):
            return float(obj)

        if isinstance(obj, float) and np.isnan(obj):
            return None

        if isinstance(obj, dict):
            return {k: _make_json_safe(v) for k, v in obj.items()}

        if isinstance(obj, (list, tuple)):
            return [_make_json_safe(v) for v in obj]

        return obj

    # -------------------------------------------------
    # Build payload (ADS INCLUDED)
    # -------------------------------------------------
    payload = {
        "analysis_insights": analysis_insights,
        "objective_v2": objective_v2,
        "focus_skus": focus_skus,
        "sku_time_series": sku_time_series,
        "inventory_alerts": inventory_alerts,
        "country": country,
        "sku_inventory_flags": sku_inventory_flags or {},
        "sku_ads_context": sku_ads_context or [],
        "ads_monthly": ads_monthly or {},
        "sku_live_context": sku_live_context or [],
        "remaining_skus_context": remaining_skus_context or {},
        "sku_mom": sku_mom or {},
    }

   
    # -------------------------------------------------
    # 🔐 SANITIZE before json.dumps
    # -------------------------------------------------
    safe_payload = _make_json_safe(payload)

    resp = openai_client.chat.completions.create(
        model="gpt-4.1",
        messages=[
            {"role": "system", "content": AI_SYSTEM_PROMPT_2},
            {"role": "user", "content": json.dumps(safe_payload, separators=(",", ":"))},
        ],
        temperature=0.1,
    )

    return resp.choices[0].message.content.strip()

def run_prompt_3_polish(bullets: dict) -> dict:
    resp = openai_client.chat.completions.create(
        model="gpt-4.1",
        messages=[
            {"role": "system", "content": AI_SYSTEM_PROMPT_3_POLISHER},
            {"role": "user", "content": json.dumps(bullets, separators=(",", ":"))}
        ],
        temperature=0.2,
    )
    return json.loads(resp.choices[0].message.content)

def build_excel_sku_recommendations(sku_mom: dict, objective_v2: dict) -> dict:
    """
    Deterministic SKU recommendation lookup from Excel-derived rule engine.
    Uses latest period current vs previous metrics already present in sku_mom.
    """

    growth_intent = str(objective_v2.get("growth_intent", "balanced")).lower()
    profit_priority = str(objective_v2.get("profit_priority", "protect_growth")).lower()

    output = {}

    for sku, metrics in (sku_mom or {}).items():
        if not isinstance(metrics, dict):
            continue

        asp = metrics.get("asp", {}) or {}
        units = metrics.get("total_quantity", {}) or {}
        net_sales = metrics.get("net_sales", {}) or {}
        profit = metrics.get("profit", {}) or {}

        rec = get_excel_recommendation_from_metrics(
            asp_current=asp.get("current"),
            asp_previous=asp.get("previous"),
            units_current=units.get("current"),
            units_previous=units.get("previous"),
            net_sales_current=net_sales.get("current"),
            net_sales_previous=net_sales.get("previous"),
            cm1_profit_current=profit.get("current"),
            cm1_profit_previous=profit.get("previous"),
            growth_intent=growth_intent,
            profit_priority=profit_priority,
        )

        output[sku] = rec

    return output

def select_focus_skus_by_sales_mix(sku_current: dict, threshold: float = 80.0) -> list[str]:
    
    ranked = []

    for sku, data in sku_current.items():
        sales_mix = data.get("sales_mix")

        if sales_mix is None:
            continue

        try:
            ranked.append((sku, float(sales_mix)))
        except (TypeError, ValueError):
            continue

    # Sort descending
    ranked.sort(key=lambda x: x[1], reverse=True)

    if not ranked:
        return []

    cumulative = 0.0
    selected = []

    for sku, mix in ranked:
        cumulative += mix
        selected.append(sku)

        if cumulative >= threshold:
            break

    # 🎯 Decision logic
    if len(selected) < 5:
        # Return top 5 by sales mix
        return [sku for sku, _ in ranked[:5]]

    return selected

def build_comparison_label(period: str, timeline: str, year: int):
    if period == "monthly":
        m = int(timeline)
        prev_year = year if m > 1 else year - 1
        prev_month = m - 1 if m > 1 else 12

        cur = f"{MONTH_NUM_TO_NAME[m].title()} {year}"
        prev = f"{MONTH_NUM_TO_NAME[prev_month].title()} {prev_year}"
        return f"{cur} vs {prev}"

    if period == "quarterly":
        q = int(timeline.replace("Q", ""))
        prev_year = year if q > 1 else year - 1
        prev_q = q - 1 if q > 1 else 4
        return f"Q{q} {year} vs Q{prev_q} {prev_year}"

    if period == "yearly":
        return f"{year} vs {year - 1}"

    return ""

def build_sku_inventory_flags(inventory_df: pd.DataFrame, user_id: int, country: str) -> dict:
    """
    Returns SKU-level inventory risk flags + classified alert.
    """

    if inventory_df.empty:
        return {}

    df = inventory_df.copy()

    # -------------------------------
    # Safe numeric coercion
    # -------------------------------
    numeric_cols = [
        "age_0_90", "age_91_180", "age_181_270",
        "age_271_365", "age_365_plus",
        "storage_cost_next_month", "unfulfillable_qty"
    ]

    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    df["aged_181_plus"] = df["age_181_270"] + df["age_271_365"]

    # -------------------------------
    # Coverage ratio
    # -------------------------------
    coverage_df = compute_inventory_coverage_ratio(user_id, country)

    coverage_map = {}
    if not coverage_df.empty:
        coverage_map = dict(
            zip(
                coverage_df["sku"],
                coverage_df["inventory_coverage_ratio"]
            )
        )

    sku_inventory_flags = {}

    for _, row in df.iterrows():

        sku = str(row["sku"])
        coverage_ratio = coverage_map.get(sku)

        aged_181_plus = int(row["aged_181_plus"])
        long_term_aged = int(row["age_365_plus"])
        estimated_storage_cost = float(row["storage_cost_next_month"])
        unfulfillable_qty = int(row["unfulfillable_qty"])

        overaged = long_term_aged > 0

        # =========================================================
        # INVENTORY ALERT CLASSIFICATION LOGIC
        # =========================================================

        alert = None
        alert_type = None

        # 1️⃣ SUPPLY (highest priority)
        if coverage_ratio is not None and coverage_ratio <= 2:
            alert = "High alert"
            alert_type = "supply"

        elif coverage_ratio is not None and coverage_ratio <= 5:
            alert = "Please send shipment"
            alert_type = "supply"

        # 2️⃣ EXCESS INVENTORY
        elif coverage_ratio is not None and coverage_ratio >= 6 and not overaged:
            alert = "High inventory coverage ratio"
            alert_type = "excess"

        # 3️⃣ HIGH STORAGE COST
        elif estimated_storage_cost > 100:
            alert = "High storage cost"
            alert_type = "cost"

        # 4️⃣ OVERAGED PRIORITY
        if overaged:
            alert = "Long-term aged inventory"
            alert_type = "overaged"

        # =========================================================

        sku_inventory_flags[sku] = {
            "aged_181_plus_units": aged_181_plus,
            "long_term_aged_units": long_term_aged,
            "unfulfillable_qty": unfulfillable_qty,
            "inventory_coverage_ratio": coverage_ratio,
            "estimated_storage_cost": estimated_storage_cost,
            "inventory_alert": alert,
            "inventory_alert_type": alert_type
        }

    return sku_inventory_flags

def render_month_end_summary(
    *,
    period: str,
    timeline: str,
    year: int,
    analysis_insights: dict | None,
    mom: dict,
    sku_mom: dict,
    focus_skus: list,
    inventory_alerts: dict,
    inventory_lost: float,
    currency_symbol: str,
    strategy_actions: dict | None = None,
    portfolio_recommendation: str | None = None,
    remaining_agg: dict | None = None,
) -> str:
    """
    Deterministic executive month-end / year-end summary renderer.
    Presentation-only. Fully None-safe.
    """

    def fmt_pct(x):
        return f"{x:+.2f}%" if isinstance(x, (int, float)) else "N/A"

    def fmt_number(x, decimals=2):
        if not isinstance(x, (int, float)):
            return "N/A"

        if decimals == 0:
            return f"{int(round(x)):,}"   # 👈 no decimals
        return f"{x:,.{decimals}f}"

    def fmt_currency(x):
        return f"{currency_symbol}{x:,.2f}" if isinstance(x, (int, float)) else "N/A"

    def fmt_value_with_pct(metric_dict, is_currency=False, decimals=2):
        if not isinstance(metric_dict, dict):
            return "N/A"

        current = metric_dict.get("current")
        pct = metric_dict.get("delta_pct")

        if is_currency:
            current_str = fmt_currency(current)
        else:
            current_str = fmt_number(current, decimals=decimals)

        pct_str = fmt_pct(pct)

        return f"{current_str} ({pct_str})"    

    is_yearly = period == "yearly"
    comparison = build_comparison_label(period, timeline, year)

    lines: list[str] = []

    # =========================================================
    # REPORT TITLE
    # =========================================================
    if period == "yearly":
        lines.append("Yearly Business Summary")
    elif period == "quarterly":
        lines.append("Quarterly Business Summary")
    else:
        lines.append("Month-end Business Summary")

    # =========================================================
    # SUMMARY — PORTFOLIO LEVEL (PERCENTAGE ONLY)
    # =========================================================
    if analysis_insights:

        lines.append("## SUMMARY")

        es = analysis_insights.get("executive_summary_signals", {})

        if is_yearly:
            lines.append(f"Year-over-Year Performance Summary ({comparison})")
        else:
            lines.append(f"Performance Summary ({comparison})")

        takeaway = analysis_insights.get("executive_takeaway")
        if isinstance(takeaway, str) and takeaway.strip():
            lines.append(takeaway)

        # Units
        u = es.get("units", {})
        units_label = "Units sold (YoY)" if is_yearly else "Units sold"
        lines.append(
            f"• {units_label}: {fmt_pct(u.get('pct_change'))}"
            f"{severity_suffix(u.get('severity'), period=period)}"
        )

        # Net Sales
        ns = es.get("net_sales", {})
        ns_label = "Net sales (YoY)" if is_yearly else "Net sales"
        lines.append(
            f"• {ns_label}: {fmt_pct(ns.get('pct_change'))}"
            f"{severity_suffix(ns.get('severity'), period=period)}"
        )

        # ASP
        asp = es.get("asp", {})
        asp_label = "ASP (YoY)" if is_yearly else "ASP"
        lines.append(
            f"• {asp_label}: {fmt_pct(asp.get('pct_change'))}"
            f"{severity_suffix(asp.get('severity'), period=period)}"
        )

        # CM1 Profit
        cm1 = es.get("cm1_profit", {})
        cm1_label = "CM1 profit (YoY)" if is_yearly else "CM1 profit"
        lines.append(
            f"• {cm1_label}: {fmt_pct(cm1.get('pct_change'))}"
            f"{severity_suffix(cm1.get('severity'), period=period)}"
        )

        # CM1 Profit per Unit
        ppu = es.get("cm1_profit_per_unit", {})
        ppu_label = "CM1 profit per unit (YoY)" if is_yearly else "CM1 profit per unit"
        lines.append(
            f"• {ppu_label}: {fmt_pct(ppu.get('pct_change'))}"
            f"{severity_suffix(ppu.get('severity'), period=period)}"
        )

        # Advertising
        cp = es.get("cost_pressure", {})
        ad = cp.get("advertising", {})
        lines.append(
            f"• Advertising spends: {fmt_pct(ad.get('pct_change'))}"
            f"{severity_suffix(ad.get('severity'), period=period)}, "
            f"ACOS change: {fmt_pct(ad.get('acos_delta'))}"
        )

        # Storage
        st = cp.get("storage_fees", {})
        lines.append(
            f"• Platform inventory storage fees: {fmt_pct(st.get('pct_change'))}"
            f"{severity_suffix(st.get('severity'), period=period)}"
        )

        # CM2 Profit
        cm2 = es.get("cm2_profit", {})
        cm2_label = "CM2 profit (YoY)" if is_yearly else "CM2 profit"
        lines.append(
            f"• {cm2_label}: {fmt_pct(cm2.get('pct_change'))}"
            f"{severity_suffix(cm2.get('severity'), period=period)}"
        )

        # Reimbursements
        reimb = es.get("reimbursements", {})
        if reimb.get("present") and isinstance(reimb.get("amount"), (int, float)):
            lines.append(
                f"• Amazon reimbursements for lost inventory: "
                f"{currency_symbol}{abs(reimb['amount']):.2f} (non-recurring recovery)"
            )

    # =========================================================
    # PORTFOLIO RECOMMENDATION
    # =========================================================
    if portfolio_recommendation and portfolio_recommendation.strip():
        lines.append("\n## PORTFOLIO DIRECTION")
        lines.append(f"• {portfolio_recommendation}")        

    # =========================================================
    # PRODUCT INSIGHTS (PERCENTAGE ONLY)
    # =========================================================
    lines.append("\n## PRODUCT INSIGHTS")

    sku_actions = strategy_actions or {}

    for sku in focus_skus:
        s = sku_mom.get(sku)
        if not isinstance(s, dict):
            continue

        name = s.get("product_name", sku)
        lines.append(f"\n{name}")

        lines.append(
            f"• ASP: {fmt_value_with_pct(s.get('asp'), is_currency=True)}"
        )

        lines.append(
            f"• Units: {fmt_value_with_pct(s.get('total_quantity'), decimals=0)}"
        )

        lines.append(
            f"• Net sales: {fmt_value_with_pct(s.get('net_sales'), is_currency=True)}"
        )

        lines.append(
            f"• CM1 profit: {fmt_value_with_pct(s.get('profit'), is_currency=True)}"
        )

        lines.append(
            f"• CM1 profit per unit: {fmt_value_with_pct(s.get('unit_wise_profitability'), is_currency=True)}"
        )

        # Product Journey
        sku_data = sku_actions.get(sku, {})
        journey = sku_data.get("journey_summary")

        if isinstance(journey, list) and journey:
            lines.append("• Product journey:")
            for point in journey:
                lines.append(f"   - {point}")

        # Recommendation
        recommendation = sku_data.get("recommendation")
        if isinstance(recommendation, str) and recommendation.strip():
            lines.append(f"• Recommendation: {recommendation}")

        # Inventory Recommendation (SKU-level)
        inv_rec = sku_data.get("inventory_recommendation")
        if isinstance(inv_rec, str) and inv_rec.strip():
            lines.append(f"• Inventory action: {inv_rec}")    

    # =========================================================
    # REMAINING SKUS — METRICS + JOURNEY + RECOMMENDATION
    # =========================================================
    remaining_rec = sku_actions.get("remaining_skus_recommendation")
    remaining_journey = sku_actions.get("remaining_skus_journey_summary")

    if remaining_agg:

        lines.append("\nOther SKUs")

        # --- Aggregated Metrics ---
        lines.append(
            f"• ASP: {fmt_value_with_pct(remaining_agg.get('asp'), is_currency=True)}"
        )

        lines.append(
            f"• Units: {fmt_value_with_pct(remaining_agg.get('total_quantity'), decimals=0)}"
        )

        lines.append(
            f"• Net sales: {fmt_value_with_pct(remaining_agg.get('net_sales'), is_currency=True)}"
        )

        lines.append(
            f"• CM1 profit: {fmt_value_with_pct(remaining_agg.get('profit'), is_currency=True)}"
        )

        lines.append(
            f"• CM1 profit per unit: {fmt_value_with_pct(remaining_agg.get('unit_wise_profitability'), is_currency=True)}"
        )

        # --- Journey ---
        if isinstance(remaining_journey, list) and remaining_journey:
            lines.append("• Product journey:")
            for point in remaining_journey:
                lines.append(f"   - {point}")

        # --- Recommendation (optional) ---
        if isinstance(remaining_rec, str) and remaining_rec.strip():
            lines.append(f"• Recommendation: {remaining_rec}")



    # =========================================================
    # INVENTORY (5 EXECUTIVE POINTS ONLY)
    # =========================================================
    if inventory_alerts:
        lines.append("\n## INVENTORY")

        ageing = inventory_alerts.get("ageing_inventory", {})
        lines.append(
            f"• Ageing inventory (181+ days): "
            f"{ageing.get('total_units',0)} units across "
            f"{ageing.get('total_skus',0)} SKUs"
        )

        high_cov = inventory_alerts.get("high_coverage", {})
        lines.append(
            f"• High coverage SKUs: "
            f"{high_cov.get('count',0)} SKUs"
        )

        unful = inventory_alerts.get("unfulfillable", {})
        if unful.get("status") == "above_1_percent":
            lines.append(
                f"• Unfulfillable inventory is above 1% of total inventory "
                f"({unful.get('percentage')}%)"
            )
        else:
            lines.append(
                f"• Unfulfillable inventory remains below 1% "
                f"({unful.get('percentage')}%)"
            )

        storage = inventory_alerts.get("estimated_storage_cost", {})
        lines.append(
            f"• Est. storage cost next month: "
            f"{currency_symbol}{storage.get('value',0):,.2f}"
        )

        lines.append(
            "• For detailed inventory insights, please refer to the Inventory Reconciliation tab."
        )



    return "\n".join(lines)

def get_or_create_summary(
    user_id,
    country,
    marketplace_id,
    period,
    timeline,
    year,
    objective=None,
    target_sku: str | list | None = None,
    force_regenerate=False
):

    

    # ============================================================
    # LOAD OBJECTIVE FROM DB
    # ============================================================
    user_objective_row = UserObjective.query.filter_by(
        user_id=user_id,
        country=country
    ).first()

    if user_objective_row:
        objective_v2 = {
            "growth_intent": user_objective_row.growth_intent,
            "profit_priority": user_objective_row.profit_priority,
            "inventory_clearance_priority": user_objective_row.inventory_clearance_priority,
            "business_context": user_objective_row.business_context,
            "country": str(country).lower(),
            "time_horizon": "1_month"
        }
    else:
        objective_v2 = {
            "growth_intent": "balanced",
            "profit_priority": "protect_growth",
            "inventory_clearance_priority": False,
            "business_context": None,
            "country": str(country).lower(),
            "time_horizon": "1_month"
        }

    # ============================================================
    # PERIOD RESOLUTION
    # ============================================================
    user_selected = bool(period and timeline and year)

    if not user_selected:
        year, month = resolve_latest_available_month(user_id, country)
        timeline = str(month)
        period = "monthly"

    is_latest = is_latest_period(
        period, timeline, year,
        user_id=user_id,
        country=country
    )

    # 🔥 NEW CONTROL FLAGS
    allow_inventory = False
    allow_recommendations = False

    if period in ("monthly", "quarterly"):
        allow_inventory = is_latest
        allow_recommendations = is_latest

    elif period == "yearly":
        allow_inventory = is_latest
        allow_recommendations = False

        

    # ============================================================
    # CACHE CHECK
    # ============================================================
    cached = fetch_existing_summary(
        user_id, country, marketplace_id, period, timeline, year
    )

    if cached and not force_regenerate and not target_sku:
        return {
            "summary": cached.summary,
            "recommendations": (
                json.loads(cached.recommendations)
                if cached.recommendations else {}
            ),
            "source": "db",
            "scope": "portfolio",
            "objective": objective_v2
        }

    # ============================================================
    # CURRENT DATA
    # ============================================================
    df_current = fetch_precalc_table(user_id, country, period, timeline, year)
    df_current_detail, df_current_total = _split_total_row(df_current)

    sku_current = compute_sku_precalc(df_current_detail)
    top_5_skus = select_focus_skus_by_sales_mix(sku_current)

    # ============================================================
    # SINGLE SKU MODE
    # ============================================================
    single_sku_mode = False
    scope = "portfolio"

    if target_sku:
        single_sku_mode = True
        scope = "sku"

        if isinstance(target_sku, list):
            target_sku = target_sku[0]

        target_sku = str(target_sku).strip()

        if target_sku in sku_current:
            top_5_skus = [target_sku]
            sku_current = {target_sku: sku_current[target_sku]}
        else:
            return {
                "summary": f"I couldn’t find SKU '{target_sku}' in the selected period.",
                "recommendations": {},
                "inventory_lost": 0.0,
                "inventory_alerts": {},
                "sku_current": {},
                "sku_mom": {},
                "sku_yoy": None,
                "objective": objective_v2,
                "sku_actions": {},
                "scope": "sku",
                "source": "no_data",
            }

    # ============================================================
    # ROLLING CONTEXT (RUN FOR BOTH PORTFOLIO AND SINGLE SKU)
    # ============================================================
    movement_context = {}
    rolling_extremes = {}
    yearly_temporal_signals = None
    analysis_anchor_year = None
    analysis_anchor_month = None
    rolling_series = []

    if period == "yearly":
        anchor = resolve_yearly_analysis_anchor(user_id, country, year)
        if anchor:
            analysis_anchor_year, analysis_anchor_month = anchor
    else:
        analysis_anchor_year = year

        if period == "monthly":
            analysis_anchor_month = int(timeline)

        elif period == "quarterly":
            QUARTER_TO_MONTH = {"Q1": 3, "Q2": 6, "Q3": 9, "Q4": 12}
            analysis_anchor_month = QUARTER_TO_MONTH.get(timeline)

    if analysis_anchor_year and analysis_anchor_month:

        rolling_series = build_rolling_monthly_series(
            user_id=user_id,
            country=country,
            anchor_year=analysis_anchor_year,
            anchor_month=analysis_anchor_month
        )

        movement_context = build_movement_context(rolling_series)

        rolling_extremes = extract_rolling_extremes(rolling_series)

        if period == "yearly":
            yearly_temporal_signals = build_yearly_temporal_signals(rolling_series) or None


                

    # ============================================================
    # INVENTORY
    # ============================================================

    lost_total_val = _total_value(df_current_total, "lost_total")
    inventory_lost = round(abs(lost_total_val), 2) if lost_total_val is not None else 0.0

    if single_sku_mode:
        inventory_lost = 0.0

    inventory_alerts = {}        # ✅ portfolio-level alerts (unchanged)
    sku_inventory_flags = {}     # ✅ new SKU-level alerts

    if allow_inventory:

        inventory_aged_df = fetch_inventory_aged_by_user(user_id, country=country)

        if not inventory_aged_df.empty:

            # 🔵 PORTFOLIO ALERTS (DO NOT CHANGE LOGIC)
            inventory_alerts = build_inventory_alerts(
                inventory_aged_df,
                user_id=user_id,
                country=country
            )

            # 🟢 SKU-LEVEL FLAGS (NEW ADDITION)
            all_sku_flags = build_sku_inventory_flags(
                inventory_aged_df,
                user_id=user_id,
                country=country
            )

            # Only pass Top 5 SKUs to strategy layer
            sku_inventory_flags = {
                sku: all_sku_flags.get(sku)
                for sku in top_5_skus
                if sku in all_sku_flags
            }     

    # ============================================================
    # PREVIOUS PERIOD
    # ============================================================
    (p_period, p_timeline, p_year), _ = resolve_comparison(period, timeline, year)
    df_prev = fetch_precalc_table(user_id, country, p_period, p_timeline, p_year)
    df_prev_detail, df_prev_total = _split_total_row(df_prev)

    period_absolute_changes = {}
    period_pct_changes = None

    if not df_current_total.empty and not df_prev_total.empty:
        period_absolute_changes = compute_period_absolute_changes(
            df_current_total,
            df_prev_total
        )

        period_pct_changes = compute_period_pct_changes(
            df_current_total,
            df_prev_total
        )

    sku_prev = compute_sku_precalc(df_prev_detail)
    sku_mom = compare_sku_metrics(sku_current, sku_prev)

    remaining_agg = build_remaining_skus_aggregate(
    sku_current=sku_current,
    sku_prev=sku_prev,
    focus_skus=top_5_skus
    )

    # -------------------------------------------------
    # Remaining SKUs time series (for LLM journey)
    # -------------------------------------------------

    remaining_series = []

    if analysis_anchor_year and analysis_anchor_month:
        remaining_series = build_remaining_skus_time_series(
            user_id=user_id,
            country=country,
            focus_skus=top_5_skus,
            anchor_year=analysis_anchor_year,
            anchor_month=analysis_anchor_month,
            months=24
        )

    remaining_skus_context = {
        "aggregated_metrics": remaining_agg,
        "time_series": remaining_series
    }

    if single_sku_mode:
        sku_mom = {k: sku_mom.get(k, {}) for k in top_5_skus}

    # ============================================================
    # EXCEL-BASED SKU RECOMMENDATIONS
    # ============================================================
    excel_sku_recommendations = build_excel_sku_recommendations(
        sku_mom=sku_mom,
        objective_v2=objective_v2
    )    

    # ============================================================
    # PROMPT 1 (ANALYSIS)
    # ============================================================
    analysis_insights = {}
    analysis_raw = ""

    if not single_sku_mode:
        ai_payload = {
            "period": f"{period} {timeline} {year}",
            "period_label": period_label(period, timeline, year),
            "country": str(country).lower(),
            "period_absolute_changes": period_absolute_changes,
            "period_pct_changes": period_pct_changes,
            "inventory_lost": inventory_lost,
            "inventory_alerts": inventory_alerts,
            "sku_mom": sku_mom,
            "focus_skus": top_5_skus,
            "movement_context": movement_context,
            "rolling_extremes": rolling_extremes,
            "yearly_temporal_signals": yearly_temporal_signals,
            "scope": scope,
             # ✅ ADD THIS LINE
            "portfolio_time_series": rolling_series,
        }

        analysis_raw = run_prompt_1_analysis(ai_payload)

        try:
            analysis_insights = json.loads(analysis_raw)
        except Exception:
            print("\n❌ Prompt-1 JSON PARSE FAILED")
            analysis_insights = {}

    # ============================================================
    portfolio_level_narrative = analysis_insights.get("executive_summary_signals", {})

    # ============================================================
    # PROMPT 2 (ALWAYS CALLED)
    # ============================================================
    sku_actions = {}
    strategy_raw = ""

    if analysis_insights or single_sku_mode:

        sku_time_series = {}

        if analysis_anchor_year and analysis_anchor_month:
            for sku in top_5_skus:
                sku_time_series[sku] = build_rolling_sku_series(
                    user_id=user_id,
                    country=country,
                    sku=sku,
                    anchor_year=analysis_anchor_year,
                    anchor_month=analysis_anchor_month
                )

        strategy_raw = run_prompt_2_strategy(
            analysis_insights=analysis_insights,
            sku_mom=sku_mom,
            objective_v2=objective_v2,
            focus_skus=top_5_skus,
            sku_time_series=sku_time_series,
            inventory_alerts=inventory_alerts,
            country=str(country).lower(),
            sku_inventory_flags=sku_inventory_flags,
            remaining_skus_context=remaining_skus_context   # ✅ NEW
        )
       

        try:
            parsed = json.loads(strategy_raw)

            portfolio_recommendation = parsed.get("portfolio_recommendation", "")

            ai_sku_actions = parsed.get("sku_actions") or {}
            sku_actions = {}

            # -------------------------------------------------
            # Merge AI outputs + Excel recommendation
            # -------------------------------------------------
            for sku in top_5_skus:
                ai_data = ai_sku_actions.get(sku, {}) or {}

                sku_actions[sku] = {
                    "journey_summary": ai_data.get("journey_summary", []),

                    # ✅ Excel-based deterministic recommendation
                    "recommendation": excel_sku_recommendations.get(sku, ""),

                    # ✅ Keep AI-generated recommendations
                    "ads_recommendation": ai_data.get("ads_recommendation", ""),
                    "inventory_recommendation": ai_data.get("inventory_recommendation", ""),
                }

            # ✅ Capture consolidated recommendation for remaining SKUs
            remaining_skus_rec = parsed.get("remaining_skus_recommendation")
            if isinstance(remaining_skus_rec, str) and remaining_skus_rec.strip():
                sku_actions["remaining_skus_recommendation"] = remaining_skus_rec

            remaining_journey = parsed.get("remaining_skus_journey_summary")
            if isinstance(remaining_journey, list) and remaining_journey:
                sku_actions["remaining_skus_journey_summary"] = remaining_journey

        except Exception:
            print("\n❌ Prompt-2 JSON PARSE FAILED")
            sku_actions = {}



    # 🔥 SUPPRESS RECOMMENDATIONS WHEN NOT ALLOWED
    if not allow_recommendations:

        # Remove SKU level recommendations
        for key, value in sku_actions.items():
            if isinstance(value, dict) and "recommendation" in value:
                value["recommendation"] = ""

        # Remove remaining SKUs recommendation text but keep card
        if "remaining_skus_recommendation" in sku_actions:
            sku_actions["remaining_skus_recommendation"] = ""

    # final_text = strategy_raw if strategy_raw else analysis_raw

    final_text = render_month_end_summary(
    period=period,
    timeline=timeline,
    year=year,
    analysis_insights=analysis_insights,
    mom=None,
    sku_mom=sku_mom,
    focus_skus=top_5_skus,
    portfolio_recommendation=portfolio_recommendation,
    inventory_alerts=inventory_alerts if allow_inventory else {},
    inventory_lost=inventory_lost,
    currency_symbol="£" if country == "uk" else "$",
    strategy_actions=sku_actions,
    remaining_agg=remaining_agg,
    )


    if not single_sku_mode:
        save_summary_to_db({
            "user_id": user_id,
            "country": country,
            "marketplace_id": marketplace_id,
            "period": period,
            "timeline": timeline,
            "year": year,
            "summary": final_text,
            "recommendations": json.dumps(sku_actions or {}),
            "upsert": True
        })

    return {
        "summary": final_text,
        # "overall_month_summary": overall_month_summary,
        "portfolio_level_narrative": portfolio_level_narrative,
        "portfolio_recommendation": portfolio_recommendation,
        "recommendations": sku_actions if allow_recommendations else {},
        "inventory_lost": inventory_lost,
        "inventory_alerts": inventory_alerts if allow_inventory else {},
        "sku_current": sku_current,
        "sku_mom": sku_mom,
        "sku_yoy": None,
        "objective": objective_v2,
        "sku_actions": sku_actions,
        "scope": scope,
        "source": "ai",
    }


def _global_safe_result(result: dict | None) -> dict:
    """
    Makes global rendering safe even if one country has no data/error-shaped result.
    """
    if not isinstance(result, dict):
        return {}

    return result



def _extract_actions(result: dict) -> dict:
    """
    DB result has recommendations.
    AI regenerated result has sku_actions.
    This supports both.
    """
    return (
        result.get("sku_actions")
        or result.get("recommendations")
        or {}
    )

def _extract_summary_intro(result: dict) -> str:
    """
    Extracts only the main country performance paragraph,
    not product insights or inventory sections.
    """
    summary = result.get("summary") or ""

    if "Performance Summary" not in summary:
        return summary[:1200]

    lines = summary.splitlines()
    output = []
    capture = False

    for line in lines:
        clean = line.strip()

        if clean.startswith("Performance Summary"):
            capture = True
            continue

        if capture:
            if clean.startswith("• ") or clean.startswith("## PRODUCT INSIGHTS"):
                break
            if clean:
                output.append(clean)

    return " ".join(output).strip()

def fetch_global_sku_mapping(user_id: int) -> list[dict]:
    """
    Fetches exact UK-US SKU mapping from sku_{user_id}_data_table.
    This is used only for global product-wise journey comparison.
    """

    table_name = f"sku_{user_id}_data_table"

    query = f'''
        SELECT
            product_name,
            sku_uk,
            sku_us,
            asin,
            product_barcode
        FROM public."{table_name}"
        WHERE user_id = :user_id
    '''

    try:
        with phormula_engine.connect() as conn:
            df = pd.read_sql(text(query), conn, params={"user_id": user_id})
    except Exception as e:
        print(f"❌ Failed to fetch global SKU mapping: {e}")
        return []

    if df.empty:
        return []

    df = df.fillna("")

    mappings = []

    for _, row in df.iterrows():
        product_name = str(row.get("product_name", "")).strip()
        sku_uk = str(row.get("sku_uk", "")).strip()
        sku_us = str(row.get("sku_us", "")).strip()

        if not product_name or not sku_uk or not sku_us:
            continue

        mappings.append({
            "product_name": product_name,
            "sku_uk": sku_uk,
            "sku_us": sku_us,
            "asin": str(row.get("asin", "")).strip(),
            "product_barcode": str(row.get("product_barcode", "")).strip(),
        })

    return mappings


def build_mapped_product_journeys(
    *,
    sku_mapping: list[dict],
    us_result: dict,
    uk_result: dict
) -> list[dict]:
    """
    Builds exact product-wise US vs UK journey input using sku_us and sku_uk mapping.
    The journey_summary values are already generated by each country's get_or_create_summary().
    """

    us_actions = _extract_actions(us_result)
    uk_actions = _extract_actions(uk_result)

    us_sku_mom = us_result.get("sku_mom", {}) or {}
    uk_sku_mom = uk_result.get("sku_mom", {}) or {}

    mapped_products = []

    for item in sku_mapping:
        product_name = item.get("product_name")
        sku_us = item.get("sku_us")
        sku_uk = item.get("sku_uk")

        us_action = us_actions.get(sku_us, {})
        uk_action = uk_actions.get(sku_uk, {})

        us_metrics = us_sku_mom.get(sku_us, {})
        uk_metrics = uk_sku_mom.get(sku_uk, {})

        if not isinstance(us_action, dict):
            us_action = {}

        if not isinstance(uk_action, dict):
            uk_action = {}

        # Skip products where neither side has journey data
        us_journey = us_action.get("journey_summary", [])
        uk_journey = uk_action.get("journey_summary", [])

        if not us_journey and not uk_journey:
            continue

        mapped_products.append({
            "product_name": product_name,
            "sku_us": sku_us,
            "sku_uk": sku_uk,

            "us": {
                "journey_summary": us_journey if isinstance(us_journey, list) else [],
                "metrics": us_metrics,
            },

            "uk": {
                "journey_summary": uk_journey if isinstance(uk_journey, list) else [],
                "metrics": uk_metrics,
            },

            # ✅ NEW: keep product actions unified inside mapped product
            "unified_country_actions": {
                "us": {
                    "recommendation": us_action.get("recommendation", ""),
                    "inventory_recommendation": us_action.get("inventory_recommendation", ""),
                    "ads_recommendation": us_action.get("ads_recommendation", ""),
                },
                "uk": {
                    "recommendation": uk_action.get("recommendation", ""),
                    "inventory_recommendation": uk_action.get("inventory_recommendation", ""),
                    "ads_recommendation": uk_action.get("ads_recommendation", ""),
                },
            },
        })

    return mapped_products

def build_global_numeric_metrics(
    *,
    user_id: int,
    period: str,
    timeline: str,
    year: int
) -> dict:
    """
    Builds selected-period and previous-period GLOBAL metrics
    from the actual global table:

    skuwisemonthly_{user_id}_global_{month}{year}_table
    """

    df_current = fetch_global_precalc_table(
        user_id=user_id,
        period=period,
        timeline=timeline,
        year=year,
    )

    df_current_detail, df_current_total = _split_total_row(df_current)

    (p_period, p_timeline, p_year), _ = resolve_comparison(
        period,
        timeline,
        year,
    )

    df_prev = fetch_global_precalc_table(
        user_id=user_id,
        period=p_period,
        timeline=p_timeline,
        year=p_year,
    )

    df_prev_detail, df_prev_total = _split_total_row(df_prev)

    if df_current.empty:
        return {
            "available": False,
            "source": "global_table",
            "reason": "No selected-period global table found",
            "selected_period": {
                "period": period,
                "timeline": timeline,
                "year": year,
                "period_label": period_label(period, timeline, year),
            },
            "previous_period": {
                "period": p_period,
                "timeline": p_timeline,
                "year": p_year,
                "period_label": period_label(p_period, p_timeline, p_year),
            },
            "portfolio": {},
            "sku_current": {},
            "sku_mom": {},
            "products": {},
        }

    current_values = {
        "units": _total_value(df_current_total, "total_quantity"),
        "net_sales": _total_value(df_current_total, "net_sales"),
        "asp": _total_value(df_current_total, "asp"),
        "cm1_profit": _total_value(df_current_total, "profit"),
        "cm1_profit_per_unit": _total_value(df_current_total, "unit_wise_profitability"),
        "cm2_profit": _total_value(df_current_total, "cm2_profit"),
        "advertising": _total_value(df_current_total, "advertising_total"),
        "storage_fees": _total_value(df_current_total, "platform_fee_inventory_storage"),
        "acos": _total_value(df_current_total, "acos"),
    }

    previous_values = {
        "units": _total_value(df_prev_total, "total_quantity"),
        "net_sales": _total_value(df_prev_total, "net_sales"),
        "asp": _total_value(df_prev_total, "asp"),
        "cm1_profit": _total_value(df_prev_total, "profit"),
        "cm1_profit_per_unit": _total_value(df_prev_total, "unit_wise_profitability"),
        "cm2_profit": _total_value(df_prev_total, "cm2_profit"),
        "advertising": _total_value(df_prev_total, "advertising_total"),
        "storage_fees": _total_value(df_prev_total, "platform_fee_inventory_storage"),
        "acos": _total_value(df_prev_total, "acos"),
    }

    absolute_changes = {}
    pct_changes = {}

    if not df_current_total.empty and not df_prev_total.empty:
        absolute_changes = compute_period_absolute_changes(
            df_current_total,
            df_prev_total,
        )

        pct_changes = compute_period_pct_changes(
            df_current_total,
            df_prev_total,
        )

    global_sku_current = compute_sku_precalc(df_current_detail)
    global_sku_prev = compute_sku_precalc(df_prev_detail)

    global_sku_mom = compare_sku_metrics(
        global_sku_current,
        global_sku_prev,
    )

    return {
        "available": True,
        "source": "global_table",
        "selected_period": {
            "period": period,
            "timeline": timeline,
            "year": year,
            "period_label": period_label(period, timeline, year),
        },
        "previous_period": {
            "period": p_period,
            "timeline": p_timeline,
            "year": p_year,
            "period_label": period_label(p_period, p_timeline, p_year),
        },
        "portfolio": {
            "current_values": current_values,
            "previous_values": previous_values,
            "absolute_changes": absolute_changes,
            "pct_changes": pct_changes,
        },

        # ✅ frontend metrics from skuwisemonthly_{user_id}_global_{mn}{year}_table
        "sku_current": global_sku_current,
        "sku_mom": global_sku_mom,

        # optional backwards compatibility
        "products": global_sku_mom,
    }


def build_country_usd_numeric_metrics(
    *,
    user_id: int,
    period: str,
    timeline: str,
    year: int
) -> dict:
    """
    Builds USD-normalized US and UK selected-period vs previous-period metrics.

    Sources:
    skuwisemonthly_{user_id}_us_usd_{month}{year}
    skuwisemonthly_{user_id}_uk_usd_{month}{year}
    """

    (p_period, p_timeline, p_year), _ = resolve_comparison(
        period,
        timeline,
        year,
    )

    def country_metrics(country: str) -> dict:
        df_current = fetch_country_usd_precalc_table(
            user_id=user_id,
            country=country,
            period=period,
            timeline=timeline,
            year=year,
        )

        df_current_detail, df_current_total = _split_total_row(df_current)

        df_prev = fetch_country_usd_precalc_table(
            user_id=user_id,
            country=country,
            period=p_period,
            timeline=p_timeline,
            year=p_year,
        )

        df_prev_detail, df_prev_total = _split_total_row(df_prev)

        if df_current.empty:
            return {
                "available": False,
                "country": country,
                "currency": "USD",
                "reason": f"No selected-period USD table found for {country}",
                "portfolio": {},
                "products": {},
            }

        current_values = {
            "units": _total_value(df_current_total, "total_quantity"),
            "net_sales": _total_value(df_current_total, "net_sales"),
            "asp": _total_value(df_current_total, "asp"),
            "cm1_profit": _total_value(df_current_total, "profit"),
            "cm1_profit_per_unit": _total_value(df_current_total, "unit_wise_profitability"),
            "cm2_profit": _total_value(df_current_total, "cm2_profit"),
            "advertising": _total_value(df_current_total, "advertising_total"),
            "storage_fees": _total_value(df_current_total, "platform_fee_inventory_storage"),
            "acos": _total_value(df_current_total, "acos"),
        }

        previous_values = {
            "units": _total_value(df_prev_total, "total_quantity"),
            "net_sales": _total_value(df_prev_total, "net_sales"),
            "asp": _total_value(df_prev_total, "asp"),
            "cm1_profit": _total_value(df_prev_total, "profit"),
            "cm1_profit_per_unit": _total_value(df_prev_total, "unit_wise_profitability"),
            "cm2_profit": _total_value(df_prev_total, "cm2_profit"),
            "advertising": _total_value(df_prev_total, "advertising_total"),
            "storage_fees": _total_value(df_prev_total, "platform_fee_inventory_storage"),
            "acos": _total_value(df_prev_total, "acos"),
        }

        absolute_changes = {}
        pct_changes = {}

        if not df_current_total.empty and not df_prev_total.empty:
            absolute_changes = compute_period_absolute_changes(
                df_current_total,
                df_prev_total,
            )

            pct_changes = compute_period_pct_changes(
                df_current_total,
                df_prev_total,
            )

        sku_current = compute_sku_precalc(df_current_detail)
        sku_prev = compute_sku_precalc(df_prev_detail)

        sku_mom = compare_sku_metrics(
            sku_current,
            sku_prev,
        )

        return {
            "available": True,
            "country": country,
            "currency": "USD",
            "portfolio": {
                "current_values": current_values,
                "previous_values": previous_values,
                "absolute_changes": absolute_changes,
                "pct_changes": pct_changes,
            },
            "products": sku_mom,
        }

    return {
        "currency": "USD",
        "currency_note": "US and UK values in this section are USD-normalized for apples-to-apples comparison.",
        "selected_period": {
            "period": period,
            "timeline": timeline,
            "year": year,
            "period_label": period_label(period, timeline, year),
        },
        "previous_period": {
            "period": p_period,
            "timeline": p_timeline,
            "year": p_year,
            "period_label": period_label(p_period, p_timeline, p_year),
        },
        "us": country_metrics("us"),
        "uk": country_metrics("uk"),
    }

def key_metrics_by_product_name(metrics_by_sku: dict) -> dict:
    """
    Converts SKU-keyed metrics into product_name-keyed metrics.

    Input:
        {
            "SEMNIWRF": {
                "product_name": "Refill Pack",
                ...
            }
        }

    Output:
        {
            "Refill Pack": {
                "sku": "SEMNIWRF",
                "product_name": "Refill Pack",
                ...
            }
        }

    If duplicate product names exist, the SKU is appended to avoid overwriting.
    """

    if not isinstance(metrics_by_sku, dict):
        return {}

    output = {}

    for sku, data in metrics_by_sku.items():
        if not isinstance(data, dict):
            continue

        product_name = data.get("product_name") or sku
        product_name = str(product_name).strip() or str(sku)

        key = product_name

        # Prevent duplicate product names from overwriting each other
        if key in output:
            key = f"{product_name} ({sku})"

        row = dict(data)
        row["sku"] = sku
        row["product_name"] = product_name

        output[key] = row

    return output


def run_global_comparison_prompt(global_payload: dict) -> dict:
    resp = openai_client.chat.completions.create(
        model="gpt-4.1",
        messages=[
            {"role": "system", "content": AI_GLOBAL_COMPARISON_PROMPT},
            {"role": "user", "content": json.dumps(global_payload, separators=(",", ":"))},
        ],
        temperature=0.2,
    )

    try:
        return json.loads(resp.choices[0].message.content)
    except Exception:
        print("\n❌ Global comparison JSON parse failed")
        return {
            "global_summary": "",
            "uk_vs_us_comparison": [],
            "product_journey_comparison": [],
            "global_overall_recommendation": "",
        }


def render_global_comparison_summary(
    *,
    global_ai: dict,
    us_result: dict,
    uk_result: dict,
    period: str,
    timeline: str,
    year: int
) -> str:
    """
    AI-written global summary renderer.
    Shows one UK vs US comparison summary, product journey comparison,
    one global recommendation, then separate US and UK product recommendations.
    """

    lines = []

    lines.append("Global Business Summary")
    lines.append(f"Period: {period_label(period, timeline, year)}")

    # ============================================================
    # AI GLOBAL OVERALL SUMMARY
    # ============================================================
    if global_ai.get("global_summary"):
        lines.append("")
        lines.append("## OVERALL SUMMARY")
        lines.append(global_ai["global_summary"])

    # ============================================================
    # AI UK VS US COMPARISON
    # ============================================================
    if global_ai.get("uk_vs_us_comparison"):
        lines.append("")
        lines.append("## UK VS US COMPARISON")
        for point in global_ai["uk_vs_us_comparison"]:
            lines.append(f"• {point}")

    # ============================================================
    # AI PRODUCT-WISE JOURNEY COMPARISON
    # ============================================================
    if global_ai.get("product_journey_comparison"):
        lines.append("")
        lines.append("## PRODUCT JOURNEY")

        for item in global_ai["product_journey_comparison"]:
            if isinstance(item, dict):
                product_name = item.get("product_name") or "Unknown Product"
                sku_us = item.get("sku_us") or "N/A"
                sku_uk = item.get("sku_uk") or "N/A"
                journey_comparison = item.get("journey_comparison") or []

                lines.append("")
                lines.append(f"### {product_name}")
                lines.append(f"• US SKU: {sku_us}")
                lines.append(f"• UK SKU: {sku_uk}")

                if isinstance(journey_comparison, list) and journey_comparison:
                    for point in journey_comparison:
                        if isinstance(point, str) and point.strip():
                            lines.append(f"• {point}")

                elif isinstance(journey_comparison, str) and journey_comparison.strip():
                    lines.append(f"• {journey_comparison}")

                # ✅ NEW: show US/UK actions inside same product block
                country_actions = item.get("country_actions") or {}

                us_actions = country_actions.get("us") or {}
                uk_actions = country_actions.get("uk") or {}

                if us_actions or uk_actions:
                    lines.append("")
                    lines.append("• Country actions:")

                    if isinstance(us_actions, dict) and any(str(v).strip() for v in us_actions.values() if v):
                        lines.append("   - US:")
                        if us_actions.get("recommendation"):
                            lines.append(f"      • Recommendation: {us_actions['recommendation']}")
                        if us_actions.get("inventory_recommendation"):
                            lines.append(f"      • Inventory action: {us_actions['inventory_recommendation']}")
                        if us_actions.get("ads_recommendation"):
                            lines.append(f"      • Ads action: {us_actions['ads_recommendation']}")

                    if isinstance(uk_actions, dict) and any(str(v).strip() for v in uk_actions.values() if v):
                        lines.append("   - UK:")
                        if uk_actions.get("recommendation"):
                            lines.append(f"      • Recommendation: {uk_actions['recommendation']}")
                        if uk_actions.get("inventory_recommendation"):
                            lines.append(f"      • Inventory action: {uk_actions['inventory_recommendation']}")
                        if uk_actions.get("ads_recommendation"):
                            lines.append(f"      • Ads action: {uk_actions['ads_recommendation']}")

            elif isinstance(item, str):
                # fallback if model returns old format
                lines.append(f"• {item}")

    # ============================================================
    # AI GLOBAL OVERALL RECOMMENDATION
    # ============================================================
    if global_ai.get("global_overall_recommendation"):
        lines.append("")
        lines.append("## OVERALL RECOMMENDATION")
        lines.append(f"• {global_ai['global_overall_recommendation']}")

    return "\n".join(lines)

def get_or_create_global_summary(
    user_id,
    marketplace_id,
    period,
    timeline,
    year,
    objective=None,
    target_sku: str | list | None = None,
    force_regenerate=False
):
    """
    Global is not a physical country table.
    It runs US and UK independently, then sends both to a global comparison prompt.
    Stores global output in DB to avoid repeated LLM calls.
    """

    # ============================================================
    # GLOBAL RECOMMENDATION RULE
    # Same behavior as single-country:
    # - latest monthly / latest quarterly => recommendations allowed
    # - old monthly / old quarterly => recommendations hidden
    # - yearly => recommendations hidden
    # ============================================================
    us_is_latest = is_latest_period(
        period,
        timeline,
        year,
        user_id=user_id,
        country="us",
    )

    uk_is_latest = is_latest_period(
        period,
        timeline,
        year,
        user_id=user_id,
        country="uk",
    )

    allow_global_recommendations = False

    if period in ("monthly", "quarterly"):
        # Safer global rule: allow recommendations only if both countries are latest
        allow_global_recommendations = us_is_latest and uk_is_latest

    elif period == "yearly":
        allow_global_recommendations = False

    # ============================================================
    # 1. CHECK GLOBAL CACHE FIRST
    # ============================================================
    cached = fetch_existing_summary(
        user_id=user_id,
        country="global",
        marketplace_id=marketplace_id,
        period=period,
        timeline=timeline,
        year=year,
    )

    if cached and not force_regenerate and not target_sku:
        cached_recommendations = {}

        try:
            cached_recommendations = (
                json.loads(cached.recommendations)
                if cached.recommendations else {}
            )
        except Exception:
            cached_recommendations = {}

        return {
            "summary": cached.summary,
            "scope": "global",
            "source": "db",
            "global_ai": cached_recommendations.get("global_ai", {}),
            "overall_recommendation": cached_recommendations.get("overall_recommendation", ""),
            "mapped_product_count": cached_recommendations.get("mapped_product_count", 0),

            # ✅ same recommendation rule returned from DB
            "allow_recommendations": cached_recommendations.get(
                "allow_global_recommendations",
                allow_global_recommendations,
            ),

            # ✅ cached frontend metrics
            "metrics": cached_recommendations.get("metrics", {}),

            # ✅ cached country-specific values
            "inventory_alerts": cached_recommendations.get("inventory_alerts", {}),
            "objectives": cached_recommendations.get("objectives", {}),

            "comparison": {
                "period": period,
                "timeline": timeline,
                "year": year,
                "period_label": period_label(period, timeline, year),
            },
            "metrics_debug": cached_recommendations.get("metrics_debug", {}),
        }

    # ============================================================
    # 2. GET US + UK COUNTRY SUMMARIES
    # ============================================================
    us_result = get_or_create_summary(
        user_id=user_id,
        country="us",
        marketplace_id=marketplace_id,
        period=period,
        timeline=timeline,
        year=year,
        objective=objective,
        target_sku=target_sku,
        force_regenerate=True,
    )

    uk_result = get_or_create_summary(
        user_id=user_id,
        country="uk",
        marketplace_id=marketplace_id,
        period=period,
        timeline=timeline,
        year=year,
        objective=objective,
        target_sku=target_sku,
        force_regenerate=True,
    )

    us_result = _global_safe_result(us_result)
    uk_result = _global_safe_result(uk_result)

    # ============================================================
    # 3. BUILD PRODUCT MAPPING + GLOBAL METRICS
    # ============================================================
    sku_mapping = fetch_global_sku_mapping(user_id)

    mapped_product_journeys = build_mapped_product_journeys(
        sku_mapping=sku_mapping,
        us_result=us_result,
        uk_result=uk_result,
    )

    # actual global metrics from skuwisemonthly_{user_id}_global_{month}{year}_table
    global_numeric_metrics = build_global_numeric_metrics(
        user_id=user_id,
        period=period,
        timeline=timeline,
        year=year,
    )

    # US/UK USD-normalized metrics from skuwisemonthly_{user_id}_{country}_usd_{month}{year}
    country_usd_metrics = build_country_usd_numeric_metrics(
        user_id=user_id,
        period=period,
        timeline=timeline,
        year=year,
    )

    metrics_debug = {
        "global_metrics_available": bool(global_numeric_metrics.get("available")),
        "country_usd_available": {
            "us": bool((country_usd_metrics.get("us") or {}).get("available")),
            "uk": bool((country_usd_metrics.get("uk") or {}).get("available")),
        },
    }

    # ✅ frontend metrics from global table
    # Product-name-keyed for frontend display
    metrics = {
        "portfolio": global_numeric_metrics.get("portfolio", {}),

        "sku_current": key_metrics_by_product_name(
            global_numeric_metrics.get("sku_current", {})
        ),

        "sku_mom": key_metrics_by_product_name(
            global_numeric_metrics.get("sku_mom", {})
        ),
    }

    # ✅ keep US/UK inventory and objectives separate
    inventory_alerts_by_country = {
        "us": us_result.get("inventory_alerts", {}),
        "uk": uk_result.get("inventory_alerts", {}),
    }

    objectives_by_country = {
        "us": us_result.get("objective", {}),
        "uk": uk_result.get("objective", {}),
    }

    # ============================================================
    # 4. BUILD GLOBAL PROMPT PAYLOAD
    # ============================================================
    global_payload = {
        "period": {
            "period": period,
            "timeline": timeline,
            "year": year,
            "period_label": period_label(period, timeline, year),
        },

        # actual global selected/previous metrics
        "global_numeric_metrics": global_numeric_metrics,

        # US vs UK metrics in same USD currency
        "country_usd_metrics": country_usd_metrics,

        "us": {
            "summary": _extract_summary_intro(us_result),
            "portfolio_level_narrative": us_result.get("portfolio_level_narrative", {}),
            "portfolio_recommendation": us_result.get("portfolio_recommendation", ""),
            "recommendations": _extract_actions(us_result),
        },
        "uk": {
            "summary": _extract_summary_intro(uk_result),
            "portfolio_level_narrative": uk_result.get("portfolio_level_narrative", {}),
            "portfolio_recommendation": uk_result.get("portfolio_recommendation", ""),
            "recommendations": _extract_actions(uk_result),
        },

        # exact mapped product journey input
        "mapped_product_journeys": mapped_product_journeys,
    }

    # ============================================================
    # 5. RUN GLOBAL LLM
    # ============================================================
    global_ai = run_global_comparison_prompt(global_payload)

    # ============================================================
    # 5A. SUPPRESS RECOMMENDATIONS FOR OLD PERIODS / YEARLY
    # ============================================================
    if not allow_global_recommendations:
        global_ai["global_overall_recommendation"] = ""

        # Remove product-level actions if the model returned them
        for item in global_ai.get("product_journey_comparison", []):
            if not isinstance(item, dict):
                continue

            country_actions = item.get("country_actions")
            if not isinstance(country_actions, dict):
                continue

            for country_key in ("us", "uk"):
                actions = country_actions.get(country_key)
                if not isinstance(actions, dict):
                    continue

                actions["recommendation"] = ""
                actions["inventory_recommendation"] = ""
                actions["ads_recommendation"] = ""

    final_text = render_global_comparison_summary(
        global_ai=global_ai,
        us_result=us_result,
        uk_result=uk_result,
        period=period,
        timeline=timeline,
        year=year,
    )

    # ============================================================
    # 6. SAVE GLOBAL SUMMARY TO DB
    # ============================================================
    save_summary_to_db({
        "user_id": user_id,
        "country": "global",
        "marketplace_id": marketplace_id,
        "period": period,
        "timeline": timeline,
        "year": year,
        "summary": final_text,
        "recommendations": json.dumps({
            "global_ai": global_ai,
            "overall_recommendation": global_ai.get("global_overall_recommendation", ""),
            "mapped_product_count": len(mapped_product_journeys),
            "metrics_debug": metrics_debug,

            # ✅ save recommendation rule
            "allow_global_recommendations": allow_global_recommendations,

            # ✅ save frontend metrics from global table
            "metrics": metrics,

            # ✅ save country-specific inventory/objectives
            "inventory_alerts": inventory_alerts_by_country,
            "objectives": objectives_by_country,
        }),
        "upsert": True,
    })

    # ============================================================
    # 7. RETURN FRESH AI RESPONSE
    # ============================================================
    return {
        "summary": final_text,
        "scope": "global",
        "source": "ai",
        "global_ai": global_ai,
        "overall_recommendation": global_ai.get("global_overall_recommendation", ""),
        "mapped_product_count": len(mapped_product_journeys),

        # ✅ same recommendation rule returned fresh
        "allow_recommendations": allow_global_recommendations,

        # ✅ frontend metrics from skuwisemonthly_{user_id}_global_{mn}{year}_table
        "metrics": metrics,

        # ✅ separate country inventory alerts
        "inventory_alerts": inventory_alerts_by_country,

        # ✅ separate country objectives
        "objectives": objectives_by_country,

        "comparison": {
            "period": period,
            "timeline": timeline,
            "year": year,
            "period_label": period_label(period, timeline, year),
        },

        "metrics_debug": metrics_debug,
    }

def build_global_table_name(user_id: int, period: str, timeline: str, year: int) -> str:
    """
    Builds actual GLOBAL table name.

    Monthly:
    skuwisemonthly_{user_id}_global_{month}{year}_table
    Example:
    skuwisemonthly_123_global_april2026_table
    """

    if period == "monthly":
        mn = month_name_from_timeline(timeline)
        return f"skuwisemonthly_{user_id}_global_{mn}{year}_table"

    if period == "quarterly":
        q = int(str(timeline).replace("Q", ""))
        return f"quarter{q}_{user_id}_global_{year}_table"

    if period == "yearly":
        return f"skuwiseyearly_{user_id}_global_{year}_table"

    raise ValueError("Invalid period")


def fetch_global_precalc_table(user_id: int, period: str, timeline: str, year: int) -> pd.DataFrame:
    """
    Fetches actual GLOBAL precalc table.

    Used for:
    - selected-period global numeric values
    - previous-period global numeric values
    """

    table = build_global_table_name(
        user_id=user_id,
        period=period,
        timeline=timeline,
        year=year,
    )

    query = f'SELECT * FROM public."{table}"'

    try:
        return pd.read_sql(query, phormula_engine)
    except Exception:
        return pd.DataFrame()


def build_country_usd_table_name(user_id: int, country: str, period: str, timeline: str, year: int) -> str:
    """
    Builds USD-normalized country table name.

    Monthly:
    skuwisemonthly_{user_id}_uk_usd_{month}{year}
    skuwisemonthly_{user_id}_us_usd_{month}{year}

    Example:
    skuwisemonthly_123_uk_usd_april2026
    skuwisemonthly_123_us_usd_april2026
    """

    c = str(country).lower()

    if period == "monthly":
        mn = month_name_from_timeline(timeline)
        return f"skuwisemonthly_{user_id}_{c}_usd_{mn}{year}"

    if period == "quarterly":
        q = int(str(timeline).replace("Q", ""))
        return f"quarter{q}_{user_id}_{c}_usd_{year}_table"

    if period == "yearly":
        return f"skuwiseyearly_{user_id}_{c}_usd_{year}_table"

    raise ValueError("Invalid period")


def fetch_country_usd_precalc_table(
    user_id: int,
    country: str,
    period: str,
    timeline: str,
    year: int
) -> pd.DataFrame:
    """
    Fetches USD-normalized country precalc table.

    Used only for global US vs UK comparison numbers.
    """

    table = build_country_usd_table_name(
        user_id=user_id,
        country=country,
        period=period,
        timeline=timeline,
        year=year,
    )

    query = f'SELECT * FROM public."{table}"'

    try:
        return pd.read_sql(query, phormula_engine)
    except Exception:
        return pd.DataFrame()

