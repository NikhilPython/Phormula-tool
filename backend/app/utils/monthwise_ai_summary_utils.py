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



load_dotenv()
SECRET_KEY = Config.SECRET_KEY

db_url = os.getenv("DATABASE_URL")
db_url2 = os.getenv("DATABASE_Chatbot_URL")
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
        print(f"[WARN] Could not read table {table}: {e}")
        return pd.DataFrame()


def build_rolling_monthly_series(
    user_id: int,
    country: str,
    anchor_year: int,
    anchor_month: int
):
    
    print("\n=== build_rolling_monthly_series CALLED ===")
    print("anchor_year:", anchor_year, type(anchor_year))
    print("anchor_month:", anchor_month, type(anchor_month))
    series = []

    # ❌ REMOVED auto-latest override
    # anchor_year, anchor_month = resolve_latest_available_month(...)

    for y, m in rolling_months(anchor_year, anchor_month, 24):
        print(f"\n--- Rolling month {y}-{m} ---")
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
        print("df_total type:", type(df_total))
        print("df_total empty:", df_total.empty)
        if df_total.empty:
            continue

        snapshot = extract_total_snapshot(df_total)

        print("snapshot type:", type(snapshot))
        print("snapshot value:", snapshot)

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

def compute_generic_movement(series: list, col: str):
    points = []

    # Build MoM % series WITH month identity
    for i in range(1, len(series)):
        prev = series[i - 1]["values"].get(col)
        cur = series[i]["values"].get(col)

        if prev is None or prev == 0 or cur is None:
            continue

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

    return {
        "units": pct("total_quantity"),
        "net_sales": pct("net_sales"),
        "asp": pct("asp"),
        "cm1_profit": pct("profit"),
        "cm1_profit_per_unit": pct("unit_wise_profitability"),
        "cm2_profit": pct("cm2_profit"),
        "advertising": pct("advertising_total"),
        "storage_fees": pct("platform_fee_inventory_storage"),

        # ✅ FIXED
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








def fetch_inventory_aged_by_user(user_id: int) -> pd.DataFrame:
    query = text("""
        SELECT
            sku,
            "inv-age-0-to-90-days"        AS age_0_90,
            "inv-age-91-to-180-days"      AS age_91_180,
            "inv-age-181-to-270-days"     AS age_181_270,
            "inv-age-271-to-365-days"     AS age_271_365,
            "inv-age-365-plus-days"       AS age_365_plus,
            "estimated-storage-cost-next-month" AS storage_cost_next_month,
            "unfulfillable-quantity"      AS unfulfillable_qty
        FROM public.inventory_aged
        WHERE user_id = :user_id
    """)

    with amazon_engine.connect() as conn:
        df = pd.read_sql(query, conn, params={"user_id": user_id})

    return df

def build_inventory_alerts(df: pd.DataFrame) -> dict:
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

    # ---------------- LONG-TERM AGED INVENTORY (365+ DAYS) ----------------
    long_term_aged_df = df[df["age_365_plus"] > 0]
    if not long_term_aged_df.empty:
        alerts["long_term_aged_inventory"] = {
            "total_units": int(long_term_aged_df["age_365_plus"].sum()),
            "top_skus": (
                long_term_aged_df
                .groupby("sku")["age_365_plus"]
                .sum()
                .sort_values(ascending=False)
                .head(5)
                .to_dict()
            )
        }

    # ---------------- CRITICALLY AGED INVENTORY (181–365 DAYS) ----------------
    df["aged_181_plus"] = df["age_181_270"] + df["age_271_365"]
    aged_critical = df[df["aged_181_plus"] > 0]

    if not aged_critical.empty:
        alerts["aged_inventory_181_plus"] = {
            "total_units": int(aged_critical["aged_181_plus"].sum()),
            "top_skus": (
                aged_critical
                .groupby("sku")["aged_181_plus"]
                .sum()
                .sort_values(ascending=False)
                .head(5)
                .to_dict()
            )
        }

    # ---------------- UNFULFILLABLE INVENTORY ----------------
    unfulfillable = df[df["unfulfillable_qty"] > 0]
    if not unfulfillable.empty:
        alerts["unfulfillable_inventory"] = {
            "total_units": int(unfulfillable["unfulfillable_qty"].sum()),
            "top_skus": (
                unfulfillable
                .groupby("sku")["unfulfillable_qty"]
                .sum()
                .sort_values(ascending=False)
                .head(5)
                .to_dict()
            )
        }

    # ---------------- STORAGE COST RISK ----------------
    total_storage_cost = float(df["storage_cost_next_month"].sum())
    if total_storage_cost > 0:
        alerts["storage_cost_risk"] = {
            "estimated_next_month_cost": round(total_storage_cost, 2)
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
    country: str
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
    # Build payload
    # -------------------------------------------------
    payload = {
        "analysis_insights": analysis_insights,
        "objective_v2": objective_v2,
        "focus_skus": focus_skus,
        "sku_time_series": sku_time_series,
        "inventory_alerts": inventory_alerts,
        "country": country,
    }

    # -------------------------------------------------
    # 🔐 SANITIZE before json.dumps  ← CRITICAL FIX
    # -------------------------------------------------
    safe_payload = _make_json_safe(payload)

    resp = openai_client.chat.completions.create(
        model="gpt-4.1",
        messages=[
            {"role": "system", "content": AI_SYSTEM_PROMPT_2},
            {"role": "user", "content": json.dumps(safe_payload, separators=(",", ":"))},
        ],
        temperature=0.2,
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


def select_top_5_skus_by_current_cm1_profit(sku_current: dict) -> list[str]:
    """
    Select Top 5 SKUs by current month CM1 profit (descending).
    """
    ranked = []

    for sku, data in sku_current.items():
        cm1 = data.get("profit")

        if cm1 is None:
            continue

        try:
            ranked.append((sku, float(cm1)))
        except (TypeError, ValueError):
            continue

    ranked.sort(key=lambda x: x[1], reverse=True)
    return [sku for sku, _ in ranked[:5]]

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





# def get_or_create_summary(
#     user_id,
#     country,
#     marketplace_id,
#     period,
#     timeline,
#     year,
#     objective=None,   # ignored now (kept for backward compatibility)
#     target_sku: str | list | None = None,
#     force_regenerate=False
# ):

#     print("\n=== get_or_create_summary START ===")
#     print("period:", period, type(period))
#     print("timeline:", timeline, type(timeline))
#     print("year:", year, type(year))

#     # ============================================================
#     # LOAD OBJECTIVE FROM DB
#     # ============================================================
#     user_objective_row = UserObjective.query.filter_by(
#         user_id=user_id,
#         country=country
#     ).first()

#     if user_objective_row:
#         objective_v2 = {
#             "growth_intent": user_objective_row.growth_intent,
#             "profit_priority": user_objective_row.profit_priority,
#             "inventory_clearance_priority": user_objective_row.inventory_clearance_priority,
#             "business_context": user_objective_row.business_context,
#             "country": str(country).lower(),
#             "time_horizon": "1_month"
#         }
#     else:
#         objective_v2 = {
#             "growth_intent": "aggressive",
#             "profit_priority": "protect_growth",
#             "inventory_clearance_priority": False,
#             "business_context": None,
#             "country": str(country).lower(),
#             "time_horizon": "1_month"
#         }

#     # ============================================================
#     # PERIOD RESOLUTION
#     # ============================================================
#     user_selected = bool(period and timeline and year)

#     if not user_selected:
#         year, month = resolve_latest_available_month(user_id, country)
#         timeline = str(month)
#         period = "monthly"

#     is_latest = is_latest_period(
#         period, timeline, year,
#         user_id=user_id,
#         country=country
#     )

#     allow_inventory = False
#     allow_actions = False

#     if period in ("monthly", "quarterly"):
#         allow_inventory = is_latest
#         allow_actions = is_latest
#     elif period == "yearly":
#         allow_inventory = is_latest
#         allow_actions = False

#     # ============================================================
#     # CACHE CHECK
#     # ============================================================
#     cached = fetch_existing_summary(
#         user_id, country, marketplace_id, period, timeline, year
#     )

#     if cached and not force_regenerate and not target_sku:
#         return {
#             "summary": cached.summary,
#             "recommendations": (
#                 json.loads(cached.recommendations)
#                 if cached.recommendations else {}
#             ),
#             "source": "db",
#             "scope": "portfolio",
#             "objective": objective_v2
#         }

#     # ============================================================
#     # CURRENT DATA
#     # ============================================================
#     df_current = fetch_precalc_table(user_id, country, period, timeline, year)
#     df_current_detail, df_current_total = _split_total_row(df_current)

#     sku_current = compute_sku_precalc(df_current_detail)
#     top_5_skus = select_top_5_skus_by_current_cm1_profit(sku_current)

#     # ============================================================
#     # SINGLE SKU MODE
#     # ============================================================
#     single_sku_mode = False
#     scope = "portfolio"

#     if target_sku:
#         single_sku_mode = True
#         scope = "sku"

#         if isinstance(target_sku, list):
#             target_sku = target_sku[0]

#         target_sku = str(target_sku).strip()

#         if target_sku in sku_current:
#             top_5_skus = [target_sku]
#             sku_current = {target_sku: sku_current[target_sku]}
#         else:
#             return {
#                 "summary": f"I couldn’t find SKU '{target_sku}' in the selected period.",
#                 "recommendations": {},
#                 "inventory_lost": 0.0,
#                 "inventory_alerts": {},
#                 "sku_current": {},
#                 "sku_mom": {},
#                 "sku_yoy": None,
#                 "objective": objective_v2,
#                 "sku_actions": {},
#                 "scope": "sku",
#                 "source": "no_data",
#             }

#     # ============================================================
#     # ROLLING CONTEXT
#     # ============================================================
#     movement_context = {}
#     rolling_extremes = {}
#     yearly_temporal_signals = None
#     analysis_anchor_year = None
#     analysis_anchor_month = None

#     if not single_sku_mode:

#         if period == "yearly":
#             anchor = resolve_yearly_analysis_anchor(user_id, country, year)
#             if anchor:
#                 analysis_anchor_year, analysis_anchor_month = anchor
#         else:
#             analysis_anchor_year = year
#             if period == "monthly":
#                 analysis_anchor_month = int(timeline)
#             elif period == "quarterly":
#                 QUARTER_TO_MONTH = {"Q1": 3, "Q2": 6, "Q3": 9, "Q4": 12}
#                 analysis_anchor_month = QUARTER_TO_MONTH.get(timeline)

#         if analysis_anchor_year and analysis_anchor_month:
#             rolling_series = build_rolling_monthly_series(
#                 user_id=user_id,
#                 country=country,
#                 anchor_year=analysis_anchor_year,
#                 anchor_month=analysis_anchor_month
#             )

#             movement_context = build_movement_context(rolling_series)
#             rolling_extremes = extract_rolling_extremes(rolling_series)

#             if period == "yearly":
#                 yearly_temporal_signals = build_yearly_temporal_signals(rolling_series) or None

#     # ============================================================
#     # INVENTORY
#     # ============================================================
#     lost_total_val = _total_value(df_current_total, "lost_total")
#     inventory_lost = round(abs(lost_total_val), 2) if lost_total_val is not None else 0.0

#     if single_sku_mode:
#         inventory_lost = 0.0

#     inventory_alerts = {}
#     if allow_inventory and not single_sku_mode:
#         inventory_aged_df = fetch_inventory_aged_by_user(user_id)
#         if not inventory_aged_df.empty:
#             inventory_alerts = build_inventory_alerts(inventory_aged_df)

#     # ============================================================
#     # PREVIOUS PERIOD
#     # ============================================================
#     (p_period, p_timeline, p_year), _ = resolve_comparison(period, timeline, year)
#     df_prev = fetch_precalc_table(user_id, country, p_period, p_timeline, p_year)
#     df_prev_detail, df_prev_total = _split_total_row(df_prev)

#     period_absolute_changes = {}
#     period_pct_changes = None

#     if not df_current_total.empty and not df_prev_total.empty:
#         period_absolute_changes = compute_period_absolute_changes(
#             df_current_total,
#             df_prev_total
#         )

#         period_pct_changes = compute_period_pct_changes(
#             df_current_total,
#             df_prev_total
#         )

#     sku_prev = compute_sku_precalc(df_prev_detail)
#     sku_mom = compare_sku_metrics(sku_current, sku_prev)

#     if single_sku_mode:
#         sku_mom = {k: sku_mom.get(k, {}) for k in top_5_skus}

#     # ============================================================
#     # PROMPT 1 (ANALYSIS)
#     # ============================================================
#     analysis_insights = None
#     analysis_raw = None

#     if not single_sku_mode:
#         ai_payload = {
#             "period": f"{period} {timeline} {year}",
#             "period_label": period_label(period, timeline, year),
#             "country": str(country).lower(),
#             "period_absolute_changes": period_absolute_changes,
#             "period_pct_changes": period_pct_changes,
#             "inventory_lost": inventory_lost,
#             "inventory_alerts": inventory_alerts,
#             "sku_mom": sku_mom,
#             "focus_skus": top_5_skus,
#             "movement_context": movement_context,
#             "rolling_extremes": rolling_extremes,
#             "yearly_temporal_signals": yearly_temporal_signals,
#             "scope": scope,
#             "objective_v2": objective_v2
#         }

#         analysis_raw = run_prompt_1_analysis(ai_payload)

#         try:
#             analysis_insights = json.loads(analysis_raw)
#         except Exception:
#             print("\n❌ Prompt-1 JSON PARSE FAILED")
#             print("RAW OUTPUT:\n", analysis_raw)
#             analysis_insights = {}

#     # ============================================================
#     # EXTRACT PORTFOLIO-LEVEL SIGNALS
#     # ============================================================
#     overall_month_summary = ""
#     portfolio_level_narrative = {}

#     if isinstance(analysis_insights, dict):
#         overall_month_summary = analysis_insights.get("executive_takeaway", "")
#         portfolio_level_narrative = analysis_insights.get("executive_summary_signals", {})
        

#     # ============================================================
#     # PROMPT 2 (STRATEGY) — CALLED ONLY ONCE
#     # ============================================================
#     sku_actions = {}
#     strategy_raw = None

#     if allow_actions and not single_sku_mode and analysis_insights:

#         sku_time_series = {}

#         if analysis_anchor_year and analysis_anchor_month:
#             for sku in top_5_skus:
#                 sku_time_series[sku] = build_rolling_sku_series(
#                     user_id=user_id,
#                     country=country,
#                     sku=sku,
#                     anchor_year=analysis_anchor_year,
#                     anchor_month=analysis_anchor_month
#                 )

#         strategy_raw = run_prompt_2_strategy(
#             analysis_insights=analysis_insights,
#             objective_v2=objective_v2,
#             focus_skus=top_5_skus,
#             sku_time_series=sku_time_series,
#             inventory_alerts=inventory_alerts,
#             country=str(country).lower()
#         )

#         try:
#             sku_actions = json.loads(strategy_raw).get("sku_actions") or {}
#         except Exception:
#             print("\n❌ Prompt-2 JSON PARSE FAILED")
#             print("RAW OUTPUT:\n", strategy_raw)
#             sku_actions = {}

#     # ============================================================
#     # RAW OUTPUT (NO RENDER LAYER)
#     # ============================================================
#     if strategy_raw:
#         final_text = strategy_raw
#     else:
#         final_text = analysis_raw if analysis_raw else ""


#         # ============================================================
#     # BUILD INVENTORY SUMMARY (INLINE)
#     # ============================================================
#     inventory_summary = ""

#     if inventory_alerts:
#         parts = []

#         if "aged_inventory_181_plus" in inventory_alerts:
#             parts.append(
#                 f"Aged inventory (181+ days): "
#                 f"{inventory_alerts['aged_inventory_181_plus']['total_units']} units"
#             )

#         if "long_term_aged_inventory" in inventory_alerts:
#             parts.append(
#                 f"Long-term aged inventory (365+ days): "
#                 f"{inventory_alerts['long_term_aged_inventory']['total_units']} units"
#             )

#         if "unfulfillable_inventory" in inventory_alerts:
#             parts.append(
#                 f"Unfulfillable inventory: "
#                 f"{inventory_alerts['unfulfillable_inventory']['total_units']} units"
#             )

#         if "storage_cost_risk" in inventory_alerts:
#             parts.append(
#                 f"Estimated storage cost next month: "
#                 f"{inventory_alerts['storage_cost_risk']['estimated_next_month_cost']}"
#             )

#         inventory_summary = " | ".join(parts)
    

#     # ============================================================
#     # SAVE
#     # ============================================================
#     save_summary_to_db({
#         "user_id": user_id,
#         "country": country,
#         "marketplace_id": marketplace_id,
#         "period": period,
#         "timeline": timeline,
#         "year": year,
#         "summary": final_text,
#         "recommendations": json.dumps(sku_actions or {}),
#         "upsert": True
#     })

#     return {
#         "summary": final_text,
#         "overall_month_summary": overall_month_summary,
#         "portfolio_level_narrative": portfolio_level_narrative,
#         "recommendations": sku_actions,
#         "inventory_lost": inventory_lost,
#         "inventory_alerts": inventory_alerts,
#         "inventory_summary": inventory_summary,
#         "sku_current": sku_current,
#         "sku_mom": sku_mom,
#         "sku_yoy": None,
#         "objective": objective_v2,
#         "sku_actions": sku_actions,
#         "scope": scope,
#         "source": "ai",
#     }

# def render_month_end_summary(
#     *,
#     period: str,
#     timeline: str,
#     year: int,
#     analysis_insights: dict | None,
#     mom: dict,
#     sku_mom: dict,
#     focus_skus: list,
#     inventory_alerts: dict,
#     inventory_lost: float,
#     currency_symbol: str,
#     strategy_actions: dict | None = None,
# ) -> str:
#     """
#     Deterministic executive month-end / year-end summary renderer.
#     Presentation-only. Fully None-safe.
#     """

#     def fmt_pct(x):
#         return f"{x:+.2f}%" if isinstance(x, (int, float)) else "N/A"

#     def fmt_num(x, decimals=2):
#         return f"{x:+.{decimals}f}" if isinstance(x, (int, float)) else "N/A"

#     def fmt_int(x):
#         return f"{int(x)}" if isinstance(x, (int, float)) else "N/A"

#     is_yearly = period == "yearly"
#     comparison = build_comparison_label(period, timeline, year)

#     lines: list[str] = []

#     # =========================================================
#     # REPORT TITLE
#     # =========================================================
#     if period == "yearly":
#         lines.append("Yearly Business Summary")
#     elif period == "quarterly":
#         lines.append("Quarterly Business Summary")
#     else:
#         lines.append("Month-end Business Summary")

#     # =========================================================
#     # SUMMARY — PORTFOLIO LEVEL
#     # =========================================================
#     if analysis_insights:

#         lines.append("## SUMMARY")

#         es = analysis_insights.get("executive_summary_signals", {})

#         if is_yearly:
#             lines.append(f"Year-over-Year Performance Summary ({comparison})")
#         else:
#             lines.append(f"Performance Summary ({comparison})")

#         takeaway = analysis_insights.get("executive_takeaway")
#         if isinstance(takeaway, str) and takeaway.strip():
#             lines.append(takeaway)

#         # Units
#         u = es.get("units", {})
#         units_label = "Units sold (YoY)" if is_yearly else "Units sold"
#         lines.append(
#             f"• {units_label}: {fmt_pct(u.get('pct_change'))} "
#             f"({fmt_int(u.get('absolute_change'))} units)"
#             f"{severity_suffix(u.get('severity'), period=period)}"
#         )

#         # Net Sales
#         ns = es.get("net_sales", {})
#         ns_label = "Net sales (YoY)" if is_yearly else "Net sales"
#         lines.append(
#             f"• {ns_label}: {fmt_pct(ns.get('pct_change'))} "
#             f"({currency_symbol}{fmt_num(ns.get('absolute_change'))})"
#             f"{severity_suffix(ns.get('severity'), period=period)}"
#         )

#         # ASP
#         asp = es.get("asp", {})
#         asp_label = "ASP (YoY)" if is_yearly else "ASP"
#         lines.append(
#             f"• {asp_label}: {fmt_pct(asp.get('pct_change'))} "
#             f"({currency_symbol}{fmt_num(asp.get('absolute_change'))})"
#             f"{severity_suffix(asp.get('severity'), period=period)}"
#         )

#         # CM1 Profit
#         cm1 = es.get("cm1_profit", {})
#         cm1_label = "CM1 profit (YoY)" if is_yearly else "CM1 profit"
#         lines.append(
#             f"• {cm1_label}: {fmt_pct(cm1.get('pct_change'))} "
#             f"({currency_symbol}{fmt_num(cm1.get('absolute_change'))})"
#             f"{severity_suffix(cm1.get('severity'), period=period)}"
#         )

#         # CM1 Profit per Unit
#         ppu = es.get("cm1_profit_per_unit", {})
#         ppu_label = "CM1 profit per unit (YoY)" if is_yearly else "CM1 profit per unit"
#         lines.append(
#             f"• {ppu_label}: {fmt_pct(ppu.get('pct_change'))} "
#             f"({currency_symbol}{fmt_num(ppu.get('absolute_change'))})"
#             f"{severity_suffix(ppu.get('severity'), period=period)}"
#         )

#         # Cost Pressure
#         cp = es.get("cost_pressure", {})
#         ad = cp.get("advertising", {})
#         lines.append(
#             f"• Advertising spends: {fmt_pct(ad.get('pct_change'))} "
#             f"({currency_symbol}{fmt_num(ad.get('absolute_change'))})"
#             f"{severity_suffix(ad.get('severity'), period=period)}, "
#             f"with ACOS change of {fmt_pct(ad.get('acos_delta'))}"
#         )

#         st = cp.get("storage_fees", {})
#         lines.append(
#             f"• Platform inventory storage fees: {fmt_pct(st.get('pct_change'))} "
#             f"({currency_symbol}{fmt_num(st.get('absolute_change'))})"
#             f"{severity_suffix(st.get('severity'), period=period)}"
#         )

#         # CM2 Profit
#         cm2 = es.get("cm2_profit", {})
#         cm2_label = "CM2 profit (YoY)" if is_yearly else "CM2 profit"
#         lines.append(
#             f"• {cm2_label}: {fmt_pct(cm2.get('pct_change'))} "
#             f"({currency_symbol}{fmt_num(cm2.get('absolute_change'))})"
#             f"{severity_suffix(cm2.get('severity'), period=period)}"
#         )

#         # Reimbursements
#         reimb = es.get("reimbursements", {})
#         if reimb.get("present") and isinstance(reimb.get("amount"), (int, float)):
#             lines.append(
#                 f"• Amazon reimbursements for lost inventory: "
#                 f"{currency_symbol}{abs(reimb['amount']):.2f} (non-recurring recovery)"
#             )

#     # =========================================================
#     # PRODUCT INSIGHTS
#     # =========================================================
#     lines.append("\n## PRODUCT INSIGHTS")

#     sku_actions = strategy_actions or {}

#     for sku in focus_skus:
#         s = sku_mom.get(sku)
#         if not isinstance(s, dict):
#             continue

#         name = s.get("product_name", sku)
#         lines.append(f"\n{name}")

#         def sku_fmt(metric, key="delta", pct_key="delta_pct"):
#             m = s.get(metric, {})
#             if not isinstance(m, dict):
#                 return "N/A (N/A)"
#             return f"{fmt_num(m.get(key))} ({fmt_pct(m.get(pct_key))})"

#         lines.append(f"• ASP: {currency_symbol}{sku_fmt('asp')}")
#         lines.append(f"• Units: {sku_fmt('total_quantity')}")
#         lines.append(f"• Net sales: {currency_symbol}{sku_fmt('net_sales')}")
#         lines.append(f"• CM1 profit: {currency_symbol}{sku_fmt('profit')}")
#         lines.append(f"• CM1 profit per unit: {currency_symbol}{sku_fmt('unit_wise_profitability')}")

#         # -----------------------------
#         # PRODUCT JOURNEY (ALL PERIODS)
#         # -----------------------------
#         sku_data = sku_actions.get(sku, {})
#         journey = sku_data.get("journey_summary")

#         if isinstance(journey, list) and journey:
#             lines.append("• Product journey:")
#             for point in journey:
#                 lines.append(f"   - {point}")

#         # -----------------------------
#         # RECOMMENDATION (ONLY IF EXISTS)
#         # -----------------------------
#         recommendation = sku_data.get("recommendation")

#         if isinstance(recommendation, str) and recommendation.strip():
#             lines.append(f"• Recommendation: {recommendation}")

#     # =========================================================
#     # INVENTORY
#     # =========================================================
#     if inventory_alerts:
#         lines.append("\n## INVENTORY")

#         if "aged_inventory_181_plus" in inventory_alerts:
#             lines.append(
#                 f"• Aged inventory (181+ days): "
#                 f"{inventory_alerts['aged_inventory_181_plus']['total_units']} units"
#             )

#         if "unfulfillable_inventory" in inventory_alerts:
#             lines.append(
#                 f"• Unfulfillable inventory: "
#                 f"{inventory_alerts['unfulfillable_inventory']['total_units']} units"
#             )

#         if "storage_cost_risk" in inventory_alerts:
#             lines.append(
#                 f"• Storage cost risk: Estimated "
#                 f"{currency_symbol}{inventory_alerts['storage_cost_risk']['estimated_next_month_cost']:.2f} "
#                 f"next month"
#             )

#         lines.append(
#             "• For detailed inventory insights, please refer to the Inventory Reconciliation tab."
#         )

#     return "\n".join(lines)

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
) -> str:
    """
    Deterministic executive month-end / year-end summary renderer.
    Presentation-only. Fully None-safe.
    """

    def fmt_pct(x):
        return f"{x:+.2f}%" if isinstance(x, (int, float)) else "N/A"

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

        def sku_pct(metric):
            m = s.get(metric, {})
            if not isinstance(m, dict):
                return "N/A"
            return fmt_pct(m.get("delta_pct"))

        lines.append(f"• ASP: {sku_pct('asp')}")
        lines.append(f"• Units: {sku_pct('total_quantity')}")
        lines.append(f"• Net sales: {sku_pct('net_sales')}")
        lines.append(f"• CM1 profit: {sku_pct('profit')}")
        lines.append(f"• CM1 profit per unit: {sku_pct('unit_wise_profitability')}")

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

    # =========================================================
    # INVENTORY
    # =========================================================
    if inventory_alerts:
        lines.append("\n## INVENTORY")

        if "aged_inventory_181_plus" in inventory_alerts:
            lines.append(
                f"• Aged inventory (181+ days): "
                f"{inventory_alerts['aged_inventory_181_plus']['total_units']} units"
            )

        if "unfulfillable_inventory" in inventory_alerts:
            lines.append(
                f"• Unfulfillable inventory: "
                f"{inventory_alerts['unfulfillable_inventory']['total_units']} units"
            )

        if "storage_cost_risk" in inventory_alerts:
            lines.append(
                f"• Storage cost risk: Estimated "
                f"{currency_symbol}{inventory_alerts['storage_cost_risk']['estimated_next_month_cost']:.2f} "
                f"next month"
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

    print("\n=== get_or_create_summary START ===")
    print("period:", period, type(period))
    print("timeline:", timeline, type(timeline))
    print("year:", year, type(year))

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
            "growth_intent": "aggressive",
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
    top_5_skus = select_top_5_skus_by_current_cm1_profit(sku_current)

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
    # ROLLING CONTEXT
    # ============================================================
    movement_context = {}
    rolling_extremes = {}
    yearly_temporal_signals = None
    analysis_anchor_year = None
    analysis_anchor_month = None

    if not single_sku_mode:

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

    inventory_alerts = {}
    if allow_inventory and not single_sku_mode:
        inventory_aged_df = fetch_inventory_aged_by_user(user_id)
        if not inventory_aged_df.empty:
            inventory_alerts = build_inventory_alerts(inventory_aged_df)

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

    if single_sku_mode:
        sku_mom = {k: sku_mom.get(k, {}) for k in top_5_skus}

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
            "scope": scope
        }

        analysis_raw = run_prompt_1_analysis(ai_payload)

        try:
            analysis_insights = json.loads(analysis_raw)
        except Exception:
            print("\n❌ Prompt-1 JSON PARSE FAILED")
            analysis_insights = {}

    overall_month_summary = analysis_insights.get("executive_takeaway", "")
    portfolio_level_narrative = analysis_insights.get("executive_summary_signals", {})

    # ============================================================
    # PROMPT 2 (ALWAYS CALLED)
    # ============================================================
    sku_actions = {}
    strategy_raw = ""

    if not single_sku_mode and analysis_insights:

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
            objective_v2=objective_v2,
            focus_skus=top_5_skus,
            sku_time_series=sku_time_series,
            inventory_alerts=inventory_alerts,
            country=str(country).lower()
        )

        try:
            parsed = json.loads(strategy_raw)
            sku_actions = parsed.get("sku_actions") or {}
        except Exception:
            print("\n❌ Prompt-2 JSON PARSE FAILED")
            sku_actions = {}

    # 🔥 SUPPRESS RECOMMENDATIONS WHEN NOT ALLOWED
    if not allow_recommendations:
        for sku in sku_actions:
            if "recommendation" in sku_actions[sku]:
                sku_actions[sku]["recommendation"] = ""

    # final_text = strategy_raw if strategy_raw else analysis_raw

    final_text = render_month_end_summary(
    period=period,
    timeline=timeline,
    year=year,
    analysis_insights=analysis_insights,
    mom=None,
    sku_mom=sku_mom,
    focus_skus=top_5_skus,
    inventory_alerts=inventory_alerts if allow_inventory else {},
    inventory_lost=inventory_lost,
    currency_symbol="£" if country == "uk" else "$",
    strategy_actions=sku_actions
    )


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
