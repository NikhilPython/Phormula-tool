import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from config import Config
from calendar import month_name,monthrange
from datetime import date, datetime, timedelta
import pandas as pd
import numpy as np
from openai import OpenAI
import json
from openai import OpenAIError
from app.utils.monthwise_ai_summary_utils import get_or_create_summary, resolve_latest_available_month, get_or_create_global_summary
from app.utils.uk_coverage_ratio_utils import fetch_sku_product_mapping, construct_prev_table_name, compute_inventory_coverage_ratio
from app.utils.uk_prompts_utils import LIVE_BI_PROMPT_1_ANALYSIS, LIVE_BI_INVENTORY_SUMMARY_PROMPT, LIVE_BI_PROMPT_1_5_SUMMARY

from app.utils.formulas_utils import (
    uk_sales,
    uk_credits,
    uk_profit,
    sku_mask,
    safe_num,
    uk_platform_fee,
    uk_advertising,
)


load_dotenv()


db_url = os.getenv("DATABASE_URL")
db_url2 = os.getenv("DATABASE_AMAZON_URL")
db_url3 = os.getenv("DATABASE_CHATBOT_URL")

engine_hist = create_engine(db_url)
engine_live = create_engine(db_url2)
engine_chatbot = create_engine(db_url3)
openai_client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
oa_client = OpenAI(api_key=OPENAI_API_KEY)
# simple process-level debounce (survives hot reload)



# -----------------------------------------------------------------------------
# DATE HELPERS
# -----------------------------------------------------------------------------

def is_blank_str(x):
    return x is None or (isinstance(x, str) and x.strip() == "")


def clamp_near_zero(value, eps=1e-9):
    if value is None:
        return value
    return 0.0 if abs(value) < eps else value


# -----------------------------------------------------------------------------
# HISTORIC ROLLING WINDOW (24 MONTHS) — PIVOT DATA ONLY
# -----------------------------------------------------------------------------

def timeline_to_year_month(timeline: int):
    """
    timeline = YYYYMM
    """
    y = timeline // 100
    m = timeline % 100
    return y, m


def month_name_from_timeline(timeline: int):
    _, m = timeline_to_year_month(timeline)
    return month_name[m].lower()


def build_rolling_monthly_series(
    user_id: int,
    country: str,
    anchor_year: int,
    anchor_month: int,
    months: int = 24,
):
    """
    Builds rolling monthly series using FINALIZED pivot tables
    skuwisemonthly_{user}_{country}_{month}{year}

    Returns list ordered oldest → latest.
    """
    series = []

    y, m = anchor_year, anchor_month

    for _ in range(months):
        mn = month_name[m].lower()
        table = f"skuwisemonthly_{user_id}_{country}_{mn}{y}"

        try:
            with engine_hist.connect() as conn:
                df = pd.read_sql(
                    text(f"SELECT * FROM {table}"),
                    conn,
                )
        except Exception:
            df = pd.DataFrame()

        if not df.empty:
            total_row = df[df["sku"].str.lower() == "total"]
            if not total_row.empty:
                r = total_row.iloc[0]
                series.append({
                    "year": y,
                    "month": m,
                    "values": {
                        "net_sales": safe_num(r.get("net_sales")),
                        "profit": safe_num(r.get("profit")),
                        "quantity": safe_num(r.get("quantity")),
                        "asp": safe_num(r.get("asp")),
                    }
                })

        # move back one month
        m -= 1
        if m == 0:
            m = 12
            y -= 1

    return list(reversed(series))

def build_movement_context(rolling_series: list) -> dict:
    """
    Converts rolling monthly totals into categorical movement context.
    SAME philosophy as Historic BI.
    """
    context = {}

    if len(rolling_series) < 3:
        return context

    metrics = ["net_sales", "profit", "quantity", "asp"]

    for metric in metrics:
        values = [
            safe_float_local(p["values"].get(metric))
            for p in rolling_series
            if p["values"].get(metric) is not None
        ]

        if len(values) < 3:
            continue

        latest = values[-1]
        prev = values[-2]

        if prev == 0 or latest is None or prev is None:
            continue

        delta_pct = ((latest - prev) / prev) * 100.0

        # rank extremity
        deltas = []
        for i in range(1, len(values)):
            if values[i-1] != 0:
                deltas.append(abs((values[i] - values[i-1]) / values[i-1]))

        if deltas:
            target = abs(delta_pct / 100)
            sorted_deltas = sorted(deltas)

            rank = min(
                range(len(sorted_deltas)),
                key=lambda i: abs(sorted_deltas[i] - target)
            ) + 1
        else:
            rank = None

        context[metric] = {
            "direction": "up" if delta_pct > 0 else "down",
            "delta_pct": round(delta_pct, 2),
            "rank_in_rolling_window": rank,
            "total_points": len(deltas),
            "is_extreme": rank is not None and rank <= 3,
        }

    return context


def compute_net_reimbursement_from_df(df: pd.DataFrame) -> float:
    """
    Net reimbursement = sum(Transfer/Disbursement totals) - abs(sum(DebtRecovery totals))
    Adjust if your DebtRecovery totals are already negative (then abs() is still safe).
    """
    if df is None or df.empty:
        return 0.0

    tmp = df.copy()

    # ensure cols exist
    for col, default in [("type", ""), ("description", ""), ("total", 0.0)]:
        if col not in tmp.columns:
            tmp[col] = default

    t = tmp["type"].astype(str).str.strip().str.lower()
    d = tmp["description"].astype(str).str.strip().str.lower()
    tot = safe_num(tmp["total"])

    disb = float(tot[(t == "transfer") & (d == "disbursement")].sum())

    # If you only want DebtRecovery/DebtPayment specifically, add (d=="debtpayment") too
    debt = float(tot[t == "debtrecovery"].sum())

    net = disb - abs(debt)
    return float(net or 0.0)


def get_mtd_and_prev_ranges(as_of=None, start_day=None, end_day=None):
    """
    Default:
      - current: current month 1st -> today (MTD)
      - previous: previous month 1st -> previous month LAST day (full month)

    Agar start_day & end_day diye gaye hain (frontend date range se):
      - current: current month [start_day, end_day] (clamped to month length & today)
      - previous: previous month [start_day, end_day] (clamped to that month length)
    """
    # --- resolve today / as_of ---
    if as_of is None:
        today = date.today()
    else:
        if isinstance(as_of, str):
            today = datetime.strptime(as_of, "%Y-%m-%d").date()
        elif isinstance(as_of, date):
            today = as_of
        else:
            today = date.today()

    # prev month/year
    if today.month == 1:
        prev_month = 12
        prev_year = today.year - 1
    else:
        prev_month = today.month - 1
        prev_year = today.year

    # ---------- custom day range ----------
    if start_day and end_day:
        sd = int(min(start_day, end_day))
        ed = int(max(start_day, end_day))

        # current month clamp
        last_day_curr = monthrange(today.year, today.month)[1]
        sd_curr = max(1, min(sd, last_day_curr))
        # end ko month & today dono se clamp kar do (future days avoid)
        ed_curr = max(1, min(ed, last_day_curr, today.day))

        current_period_start = date(today.year, today.month, sd_curr)
        current_period_end = date(today.year, today.month, ed_curr)

        # previous month clamp
        last_day_prev = monthrange(prev_year, prev_month)[1]
        sd_prev = max(1, min(sd, last_day_prev))
        ed_prev = max(1, min(ed, last_day_prev))

        prev_month_start = date(prev_year, prev_month, sd_prev)
        prev_month_end = date(prev_year, prev_month, ed_prev)

    # ---------- default behaviour (no range) ----------
    else:
        current_period_start = date(today.year, today.month, 1)
        current_period_end = today

        prev_month_start = date(prev_year, prev_month, 1)
        last_day_prev = monthrange(prev_year, prev_month)[1]

        # ✅ Align previous end day to current MTD day (clamp to prev month length)
        prev_end_day = min(today.day, last_day_prev)
        prev_month_end = date(prev_year, prev_month, prev_end_day)

    return {
        "current": {
            "start": current_period_start,
            "end": current_period_end,
        },
        "previous": {
            "start": prev_month_start,
            "end": prev_month_end,
        },
        "meta": {
            "today": today,
            "current_month": today.month,
            "current_year": today.year,
            "previous_month": prev_month,
            "previous_year": prev_year,
        },
    }



# -----------------------------------------------------------------------------
# 🔹 NEW HELPER — HISTORIC BI PARITY (6-MONTH LOOKBACK)
# -----------------------------------------------------------------------------

def fetch_historical_skus_last_6_months(user_id: int, country: str, ref_date: date):
    """
    Mirrors Historic BI logic.
    Returns set of SKUs seen in last 6 months (excluding current month).
    """
    skus = set()
    y, m = ref_date.year, ref_date.month

    for _ in range(6):
        m -= 1
        if m == 0:
            m = 12
            y -= 1

        try:
            table = construct_prev_table_name(
                user_id=user_id,
                country=country,
                month=m,
                year=y
            )
        except Exception:
            continue

        try:
            with engine_hist.connect() as conn:
                res = conn.execute(text(f"SELECT DISTINCT sku FROM {table}"))
                for r in res:
                    if r[0]:
                        skus.add(str(r[0]).strip())
        except Exception:
            continue

    return skus

def fetch_first_seen_sku_date(user_id: int, country: str) -> dict:
    """
    Returns { sku: first_seen_date } by scanning historic monthly tables.
    Safe, explicit, and matches your existing architecture.
    """
    first_seen = {}

    with engine_hist.connect() as conn:
        # 1) get all matching tables
        res = conn.execute(text("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
              AND table_name LIKE :pattern
        """), {
            "pattern": f"user_{user_id}_{country}_%_data"
        })

        table_names = [r[0] for r in res]

        # 2) scan each table
        for table in table_names:
            try:
                q = text(f"""
                    SELECT sku,
                           MIN(NULLIF(NULLIF(date_time, '0'), '')::date) AS first_seen
                    FROM {table}
                    WHERE sku IS NOT NULL
                    GROUP BY sku
                """)

                rows = conn.execute(q).fetchall()

                for sku, d in rows:
                    if not sku or not d:
                        continue

                    sku = str(sku).strip()

                    # keep EARLIEST date across all tables
                    if sku not in first_seen or d < first_seen[sku]:
                        first_seen[sku] = d

            except Exception:
                # skip broken / missing tables safely
                continue

    return first_seen

def last_n_month_periods(end_year, end_month, n=6):
    """
    Returns set like {'2026-04', '2026-03', ...}
    ending at end_year/end_month inclusive.
    """
    y, m = int(end_year), int(end_month)
    periods = set()

    for _ in range(n):
        periods.add(f"{y:04d}-{m:02d}")
        m -= 1
        if m == 0:
            m = 12
            y -= 1

    return periods


def marketplace_id_for_country(country_lower):
    country_lower = str(country_lower or "").strip().lower()

    if country_lower == "uk":
        return "A1F83G8C2ARO7P"

    if country_lower == "us":
        return "ATVPDKIKX0DER"

    return None


def fetch_new_skus_from_products_open_date(country: str, sku_keys: set, ref_date: date):
    """
    Uses products.open_date from DATABASE_AMAZON_URL to identify new SKUs.

    New SKU rule:
    - UK marketplace_id = A1F83G8C2ARO7P
    - US marketplace_id = ATVPDKIKX0DER
    - open_date month must be within last 6 month buckets ending at ref_date month
    - SKU must be present in current MTD SKUs
    """
    country_lower = str(country or "").strip().lower()
    marketplace_id = marketplace_id_for_country(country_lower)

    if not marketplace_id or not sku_keys:
        return set()

    last6_periods = last_n_month_periods(
        ref_date.year,
        ref_date.month,
        6
    )

    q = text("""
        SELECT sku, open_date
        FROM products
        WHERE marketplace_id = :marketplace_id
          AND sku = ANY(:sku_list)
          AND open_date IS NOT NULL
    """)

    new_skus = set()

    with engine_live.connect() as conn:
        rows = conn.execute(q, {
            "marketplace_id": marketplace_id,
            "sku_list": list(sku_keys),
        }).fetchall()

    for sku, open_date in rows:
        if not sku or not open_date:
            continue

        try:
            if hasattr(open_date, "strftime"):
                open_period = open_date.strftime("%Y-%m")
            else:
                open_period = str(open_date).strip()[:7]

            if open_period in last6_periods:
                new_skus.add(str(sku).strip())

        except Exception as e:
            print(f"[WARN] Failed parsing products.open_date for sku={sku}: {e}")

    return new_skus




def normalize_sales_mix(df: pd.DataFrame, mix_col="sales_mix", digits=2):
    """
    Forces sales_mix to sum exactly to 100.00 after rounding.
    """
    if df.empty or mix_col not in df.columns:
        return df

    df = df.copy()

    # Round all values
    df[mix_col] = df[mix_col].round(digits)

    total = df[mix_col].sum()
    diff = round(100.0 - total, digits)

    if abs(diff) > 0:
        # Add residual to the SKU with highest mix (or last row)
        idx = df[mix_col].idxmax()
        df.loc[idx, mix_col] = round(df.loc[idx, mix_col] + diff, digits)

    return df
# -----------------------------------------------------------------------------

# -----------------------------------------------------------------------------
# USER OBJECTIVE (SHARED WITH HISTORIC BI)
# -----------------------------------------------------------------------------



def fetch_user_objective(user_id: int, country: str = None) -> dict:
    """
    Fetch latest objective_v2 from user_objectives table.
    Returns strategy-ready objective schema.
    """

    query = text("""
    SELECT
        growth_intent,
        profit_priority,
        inventory_clearance_priority,
        business_context,
        country
    FROM user_objectives
    WHERE user_id = :user_id
      AND (:country IS NULL OR country = :country)
    ORDER BY created_at DESC
    LIMIT 1
    """)

    try:
        with engine_chatbot.connect() as conn:
            row = conn.execute(query, {
                "user_id": user_id,
                "country": country,
            }).fetchone()
    except Exception as e:
        print("[WARN] Failed to fetch user objective:", e)
        row = None

    # ------------------------------------------------
    # FALLBACK (Safe Default Strategy Objective)
    # ------------------------------------------------
    if not row:
        return {
            "growth_intent": "balanced",
            "profit_priority": "protect_growth",
            "inventory_clearance_priority": False,
            "business_context": "live_mtd",
            "country": country or "uk",
            "time_horizon": "1_month",
        }

    return {
        "growth_intent": row.growth_intent or "balanced",
        "profit_priority": row.profit_priority or "protect_growth",
        "inventory_clearance_priority": row.inventory_clearance_priority or False,
        "business_context": row.business_context or "live_mtd",
        "country": row.country or (country or "uk"),
        "time_horizon": "1_month",

    }




def fetch_transit_time(user_id: int, marketplace: str, country: str):
    query = text("""
        SELECT
            transit_time,
            marketplace,
            country
        FROM public.country_profile
        WHERE user_id = :user_id
          AND marketplace = :marketplace
          AND country = :country
        LIMIT 1
    """)

    params = {
        "user_id": user_id,
        "marketplace": marketplace,
        "country": country.lower(),  # optional normalization
    }

    with engine_hist.connect() as conn:
        row = conn.execute(query, params).fetchone()

    if not row:
        return None

    return {
        "transit_time": row.transit_time,
        "marketplace": row.marketplace,
        "country": row.country,
    }
# -----------------------------------------------------------------------------
def fetch_inventory_aged_by_user(user_id: int, country: str) -> pd.DataFrame:
    marketplace_id = marketplace_id_for_country(country)

    if not marketplace_id:
        return pd.DataFrame()

    query = text("""
        SELECT *
        FROM public.inventory_aged
        WHERE user_id = :user_id
          AND marketplace = :marketplace
        ORDER BY id ASC
    """)

    with engine_live.connect() as conn:
        df = pd.read_sql(
            query,
            conn,
            params={
                "user_id": user_id,
                "marketplace": marketplace_id,
            }
        )

    return df

def build_portfolio_inventory_alerts(df: pd.DataFrame, user_id: int, country: str) -> dict:

    if df is None or df.empty:
        return {}

    df = df.copy()

    # safe numeric
    numeric_cols = [
        "inv-age-0-to-90-days",
        "inv-age-91-to-180-days",
        "inv-age-181-to-270-days",
        "inv-age-271-to-365-days",
        "inv-age-365-plus-days",
        "estimated-storage-cost-next-month",
        "unfulfillable-quantity"
    ]

    for c in numeric_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

    alerts = {}

    # -------------------------------------------------
    # 1️⃣ AGEING INVENTORY
    # -------------------------------------------------

    aged_181_plus = (
        df["inv-age-181-to-270-days"]
        + df["inv-age-271-to-365-days"]
    )

    ageing_units = int(aged_181_plus.sum())
    ageing_skus = int((aged_181_plus > 0).sum())

    alerts["ageing_inventory"] = {
        "total_units": ageing_units,
        "total_skus": ageing_skus
    }

    # -------------------------------------------------
    # 2️⃣ HIGH COVERAGE RATIO
    # -------------------------------------------------

    coverage_df = compute_inventory_coverage_ratio(user_id, country)

    if coverage_df is not None and not coverage_df.empty:

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

    # -------------------------------------------------
    # 3️⃣ UNFULFILLABLE INVENTORY %
    # -------------------------------------------------

    total_inventory_units = int(
        df[
            [
                "inv-age-0-to-90-days",
                "inv-age-91-to-180-days",
                "inv-age-181-to-270-days",
                "inv-age-271-to-365-days",
                "inv-age-365-plus-days",
            ]
        ].sum().sum()
    )

    unfulfillable_units = int(
        df["unfulfillable-quantity"].sum()
    )

    unfulfillable_pct = (
        (unfulfillable_units / total_inventory_units) * 100
        if total_inventory_units > 0 else 0
    )

    alerts["unfulfillable"] = {
        "units": unfulfillable_units,
        "percentage": round(unfulfillable_pct, 2),
        "status": "above_1_percent"
        if unfulfillable_pct > 1 else "below_1_percent"
    }

    # -------------------------------------------------
    # 4️⃣ ESTIMATED STORAGE COST
    # -------------------------------------------------

    total_storage_cost = float(
        df["estimated-storage-cost-next-month"].sum()
    )

    alerts["estimated_storage_cost"] = {
        "value": round(total_storage_cost, 2)
    }

    return alerts

# -----------------------------------------------------------------------------
def fetch_estimated_storage_cost_next_month(user_id: int, country: str) -> float:
    df = fetch_inventory_aged_by_user(user_id, country)

    if df.empty or "estimated-storage-cost-next-month" not in df.columns:
        return 0.0

    return float(
        safe_num(df["estimated-storage-cost-next-month"]).sum()
    )

def totals_from_daily_series(daily_series):
    """
    daily_series: list[dict] with keys like profit/platform_fee/advertising etc.
    Returns totals (float) safely.
    """
    def s(key: str) -> float:
        return float(sum(float((x.get(key, 0) or 0)) for x in (daily_series or [])))

    return {
        "quantity": s("quantity"),
        "net_sales": s("net_sales"),
        "product_sales": s("product_sales"),
        "profit": s("profit"),
        "platform_fee": s("platform_fee"),
        "advertising": s("advertising"),
    }


def compute_sku_metrics_from_df(df: pd.DataFrame) -> list:
    """
    Given a raw settlement-style DataFrame with columns like:
      sku, quantity, product_sales, taxes, credits, rebates, etc.

    compute per-SKU metrics:
      quantity
      product_sales
      gross_sales        ✅ NEW (consistent definition)
      net_sales
      profit
      asp
      unit_wise_profitability
      sales_mix
      product_name
    """
    if df is None or df.empty:
        return []

    df = df.copy()

    # Keep only valid SKUs (same rule as formula_utils)
    if "sku" in df.columns:
        df = df.loc[sku_mask(df)]

    if df.empty:
        return []

    # Ensure numeric columns exist for gross_sales formula
    gross_cols = [
        "product_sales",
        "product_sales_tax",
        "postage_credits",
        "gift_wrap_credits",
        "shipping_credits_tax",
        "giftwrap_credits_tax",
        "promotional_rebates",
        "promotional_rebates_tax",
    ]
    for c in gross_cols:
        if c not in df.columns:
            df[c] = 0.0

    # ✅ Row-level gross_sales (robust: subtract abs rebates)
    df["gross_sales"] = (
        safe_num(df["product_sales"])
        + safe_num(df["product_sales_tax"])
        + safe_num(df["postage_credits"])
        + safe_num(df["gift_wrap_credits"])
        + safe_num(df["shipping_credits_tax"])
        + safe_num(df["giftwrap_credits_tax"])
        - safe_num(df["promotional_rebates"]).abs()
        - safe_num(df["promotional_rebates_tax"]).abs()
    )

    # ---- quantity per SKU ----
    if "quantity" in df.columns:
        qty_df = (
            df.assign(quantity=safe_num(df["quantity"]))
              .groupby("sku", as_index=False)["quantity"]
              .sum()
        )
    else:
        qty_df = pd.DataFrame(columns=["sku", "quantity"])

    # ---- product_sales per SKU ----
    product_sales_df = (
        df.assign(product_sales=safe_num(df["product_sales"]))
          .groupby("sku", as_index=False)["product_sales"]
          .sum()
    )

    # ---- gross_sales per SKU ✅ ----
    gross_sales_df = (
        df.groupby("sku", as_index=False)["gross_sales"].sum()
    )
    # ---- amazon fees per SKU ----
    for c in ["selling_fees", "fba_fees"]:
        if c not in df.columns:
            df[c] = 0.0

    amazon_fees_df = (
        df.assign(
            selling_fees=safe_num(df["selling_fees"]),
            fba_fees=safe_num(df["fba_fees"]),
        )
        .groupby("sku", as_index=False)[["selling_fees", "fba_fees"]]
        .sum()
    )

    # ---- product_name per SKU (first non-null) ----
    if "product_name" in df.columns:
        name_df = (
            df[["sku", "product_name"]]
            .dropna(subset=["sku"])
            .groupby("sku", as_index=False)
            .first()
        )
    else:
        name_df = pd.DataFrame(columns=["sku", "product_name"])

    # ---- sales, credits, profit per SKU via formula_utils ----
    # ---- net_sales per SKU: product_sales + promotional_rebates ----
    df["net_sales"] = (
        safe_num(df.get("product_sales", 0.0))
        + safe_num(df.get("promotional_rebates", 0.0))
    )

    sales_by = (
        df.groupby("sku", as_index=False)["net_sales"]
        .sum()
        .rename(columns={"net_sales": "sales_metric"})
    )

    # ---- credits, profit per SKU via formula_utils ----
    _, credits_by, _ = uk_credits(df)
    _, profit_by, _ = uk_profit(df)

    if credits_by is not None and not credits_by.empty:
        credits_by = credits_by.rename(columns={"__metric__": "credits_metric"})
    else:
        credits_by = pd.DataFrame(columns=["sku", "credits_metric"])

    if profit_by is not None and not profit_by.empty:
        profit_by = profit_by.rename(columns={"__metric__": "profit_metric"})
    else:
        profit_by = pd.DataFrame(columns=["sku", "profit_metric"])

    # ---- merge everything ----
    metrics = (
        qty_df
        .merge(name_df, on="sku", how="left")
        .merge(product_sales_df, on="sku", how="left")
        .merge(gross_sales_df, on="sku", how="left")  # ✅ NEW
        .merge(amazon_fees_df, on="sku", how="left")  # ✅ NEW
        .merge(sales_by[["sku", "sales_metric"]], on="sku", how="left")
        .merge(credits_by[["sku", "credits_metric"]], on="sku", how="left")
        .merge(profit_by[["sku", "profit_metric"]], on="sku", how="left")
    )

    # ---- compute final fields ----
    metrics["quantity"] = safe_num(metrics.get("quantity", 0.0))
    metrics["product_sales"] = safe_num(metrics.get("product_sales", 0.0))
    metrics["gross_sales"] = safe_num(metrics.get("gross_sales", 0.0))  # ✅ NEW
    metrics["selling_fees"] = safe_num(metrics.get("selling_fees", 0.0))
    metrics["fba_fees"] = safe_num(metrics.get("fba_fees", 0.0))
    metrics["sales_metric"] = safe_num(metrics.get("sales_metric", 0.0))
    metrics["credits_metric"] = safe_num(metrics.get("credits_metric", 0.0))
    metrics["profit_metric"] = safe_num(metrics.get("profit_metric", 0.0))

    # ✅ tax_and_credits comes from uk_credits()
    metrics["tax_and_credits"] = metrics["credits_metric"]

    metrics["net_sales"] = metrics["sales_metric"]
    metrics["profit"] = metrics["profit_metric"]

    # asp & per-unit profitability (based on net_sales)
    qty_nonzero = metrics["quantity"].replace(0, np.nan)
    metrics["asp"] = (metrics["net_sales"] / qty_nonzero).replace([np.inf, -np.inf], np.nan)
    metrics["unit_wise_profitability"] = (metrics["profit"] / qty_nonzero).replace([np.inf, -np.inf], np.nan)

    # sales_mix (% of net_sales)
    total_net_sales = float(safe_num(metrics["net_sales"]).sum())
    if total_net_sales != 0:
        metrics["sales_mix"] = (metrics["net_sales"] / total_net_sales) * 100.0
    else:
        metrics["sales_mix"] = 0.0

    metrics = normalize_sales_mix(metrics, "sales_mix", digits=2)

    out_cols = [
        "sku",
        "product_name",
        "quantity",
        "product_sales",
        "gross_sales",  # ✅ NEW
        "selling_fees",  # ✅ NEW
        "fba_fees",  # ✅ NEW
        "tax_and_credits",  # ✅ NEW
        "asp",
        "profit",
        "sales_mix",
        "net_sales",
        "unit_wise_profitability",
    ]

    return (
        metrics[out_cols]
        .replace({np.nan: None})
        .to_dict(orient="records")
    )



def fetch_previous_period_data(user_id, country, prev_start: date, prev_end: date):
    table_name = construct_prev_table_name(
        user_id=user_id,
        country=country,
        month=prev_start.month,
        year=prev_start.year,
    )

    query = text(f"""
        SELECT *
        FROM (
            SELECT
                *,
                NULLIF(NULLIF(date_time, '0'), '')::timestamp AS date_ts
            FROM {table_name}
        ) t
        WHERE date_ts >= :start_date
          AND date_ts < :end_date_plus_one
    """)

    params = {
        "start_date": datetime.combine(prev_start, datetime.min.time()),
        "end_date_plus_one": datetime.combine(prev_end + timedelta(days=1), datetime.min.time()),
    }

    with engine_hist.connect() as conn:
        result = conn.execute(query, params)
        rows = result.fetchall()
        if not rows:
            return [], []

        df = pd.DataFrame(rows, columns=result.keys())

    # 1) per-SKU metrics (now includes gross_sales if you updated compute_sku_metrics_from_df)
    sku_metrics = compute_sku_metrics_from_df(df)

    # ------------------------------------------------------------
    # ✅ Fee extraction for PREVIOUS PERIOD (robust, like liveorders)
    # ------------------------------------------------------------
    def _fee_amount_col(xdf: pd.DataFrame) -> pd.Series:
        """
        Prefer other_transaction_fees if present and non-zero, else fallback to total.
        """
        if xdf is None or xdf.empty:
            return pd.Series(dtype=float)

        if "other_transaction_fees" in xdf.columns:
            s = safe_num(xdf["other_transaction_fees"])
            if float(np.nansum(s.values)) != 0.0:
                return s

        return safe_num(xdf.get("total", 0.0))

    def _calc_fees_from_hist(day_df: pd.DataFrame) -> tuple[float, float]:
        """
        Returns (platform_fee, advertising) as POSITIVE numbers.

        This mirrors your fetch_current_mtd_data() logic.
        """
        if day_df is None or day_df.empty:
            return 0.0, 0.0

        t = day_df.get("type", "").fillna("").astype(str).str.lower()
        d = day_df.get("description", "").fillna("").astype(str).str.lower()
        amt = _fee_amount_col(day_df)

        # ignore cash movement + normal order payments
        ignore = (
            t.str.contains("transfer|disbursement", na=False)
            | d.str.contains("disbursement", na=False)
            | d.str.contains("order payment", na=False)
        )

        # Advertising bucket
        is_ads = (
            t.str.contains(r"productadspayment|sellerdealpayment", na=False)
            | d.str.contains(r"productadspayment|sellerdealcomplete", na=False)
            | d.str.contains(r"couponparticipationevent|couponperformanceevent", na=False)
            | d.str.contains(r"\bcoupon\b", na=False)
        ) & (~ignore)


        # Platform fee bucket
        is_platform_fee = (
            (t.str.contains("servicefee", na=False) | d.str.contains(r"\bfee\b", na=False))
            & d.str.contains(
                r"fba|storage|disposal|subscription|longterm|long term|referral|commission",
                na=False
            )
        ) & (~ignore) & (~is_ads)

        ads_total = float(np.nansum(amt[is_ads].values))
        pf_total  = float(np.nansum(amt[is_platform_fee].values))

        return abs(pf_total), abs(ads_total)

    # ------------------------------------------------------------
    # ✅ Ensure columns exist for gross_sales daily calc
    # ------------------------------------------------------------
    needed = [
        "product_sales",
        "product_sales_tax",
        "postage_credits",
        "gift_wrap_credits",
        "shipping_credits_tax",
        "giftwrap_credits_tax",
        "promotional_rebates",
        "promotional_rebates_tax",
    ]
    for c in needed:
        if c not in df.columns:
            df[c] = 0.0

    # 2) daily series
    daily_series = []
    date_col = "date_ts" if "date_ts" in df.columns else "date_time"

    if date_col in df.columns:
        tmp_all = df.copy()
        tmp_all["date_only"] = pd.to_datetime(tmp_all[date_col], errors="coerce").dt.date
        tmp_all = tmp_all.dropna(subset=["date_only"])

        # For sales/profit you may want SKU-only rows
        tmp_sku = tmp_all.copy()
        if "sku" in tmp_sku.columns:
            tmp_sku = tmp_sku.loc[sku_mask(tmp_sku)].copy()

        for d in sorted(tmp_all["date_only"].unique()):
            day_all = tmp_all[tmp_all["date_only"] == d]
            day_sku = tmp_sku[tmp_sku["date_only"] == d]

            quantity = float(safe_num(day_sku.get("quantity", 0)).sum()) if len(day_sku) else 0.0
            product_sales = float(safe_num(day_sku.get("product_sales", 0)).sum()) if len(day_sku) else 0.0
            cogs = float(
                safe_num(day_sku.get("cost_of_unit_sold", 0)).sum()
            ) if len(day_sku) else 0.0
            selling_fees = float(
                safe_num(day_sku.get("selling_fees", 0)).sum()
            ) if len(day_sku) else 0.0

            fba_fees = float(
                safe_num(day_sku.get("fba_fees", 0)).sum()
            ) if len(day_sku) else 0.0

            # ✅ gross_sales per day (robust rebates)
            gross_sales = float((
                safe_num(day_sku.get("product_sales", 0.0))
                + safe_num(day_sku.get("product_sales_tax", 0.0))
                + safe_num(day_sku.get("postage_credits", 0.0))
                + safe_num(day_sku.get("gift_wrap_credits", 0.0))
                + safe_num(day_sku.get("shipping_credits_tax", 0.0))
                + safe_num(day_sku.get("giftwrap_credits_tax", 0.0))
                - safe_num(day_sku.get("promotional_rebates", 0.0)).abs()
                - safe_num(day_sku.get("promotional_rebates_tax", 0.0)).abs()
            ).sum()) if len(day_sku) else 0.0

            # sales/profit based on SKU rows (keeps your earlier behavior)
            # ✅ net_sales same as other route: product_sales + promotional_rebates
            sales_df = day_sku if len(day_sku) else day_all

            net_sales = float((
                safe_num(sales_df.get("product_sales", 0.0))
                + safe_num(sales_df.get("promotional_rebates", 0.0))
            ).sum())

            profit, _, _ = uk_profit(sales_df)

            # ✅ FIXED: fees computed on ALL rows using hist classifier (NOT uk_platform_fee/uk_advertising)
            platform_fee_total, advertising_total = _calc_fees_from_hist(day_all)

            # ✅ NEW: reimbursement from raw hist transactions
            remb_total = float(compute_net_reimbursement_from_df(day_all))

            daily_series.append({
                "date": d.isoformat(),
                "quantity": float(quantity),
                "product_sales": float(product_sales),
                "gross_sales": float(gross_sales),
                "net_sales": float(net_sales),
                "profit": float(profit),
                "platform_fee": float(platform_fee_total),
                "advertising": float(advertising_total),
                 # ✅ NEW
                "rembursement_fee": float(remb_total),
                "cogs": float(cogs),
                "cost_of_unit_sold": float(cogs),
                "selling_fees": float(selling_fees),
                "fba_fees": float(fba_fees),
            })

    daily_series = sorted(daily_series, key=lambda x: x["date"])
    return sku_metrics, daily_series



def fetch_current_mtd_data(user_id, country, curr_start: date, curr_end: date):
    """
    Returns:
      sku_metrics: list of per-SKU metrics from liveorders
      daily_series: date-wise series with qty/net_sales/product_sales/gross_sales/profit
                   + platform_fee/advertising

    Fees returned as POSITIVE numbers to support:
      CM2 = Profit - Advertising - Platform Fees
    """
    table_live = "liveorders"

    query_live = text(f"""
    SELECT
        sku,
        quantity,
        cogs,
        product_sales,
        promotional_rebates,
        gross_sales,              -- ✅ NEW (read from DB)
        profit,
        total,
        purchase_date,
        order_status,
        description,
        type,
        bucket,
        other_transaction_fees,
        other
    FROM {table_live}
    WHERE user_id = :user_id
        AND purchase_date >= :start_date
        AND purchase_date < :end_date_plus_one
        AND marketplace = :marketplace
""")

    marketplace_name = "Amazon.com" if country == "us" else "Amazon.co.uk"

    params = {
        "user_id": user_id,
        "start_date": datetime.combine(curr_start, datetime.min.time()),
        "end_date_plus_one": datetime.combine(curr_end + timedelta(days=1), datetime.min.time()),
        "marketplace": marketplace_name,
    }

    with engine_live.connect() as conn:
        res = conn.execute(query_live, params)
        rows = res.fetchall()
        if not rows:
            return [], []

        df = pd.DataFrame(rows, columns=res.keys())

    # ----------------------------
    # SKU + mapping logic
    # ----------------------------
    df["sku"] = df["sku"].astype(str).str.strip()
    df.loc[df["sku"].str.lower().isin(["none", "nan", "null", ""]), "sku"] = None

    df["product_name"] = df["sku"].fillna("")

    df["__has_mapping__"] = False
    try:
        sku_map_df = fetch_sku_product_mapping(user_id)
        if not sku_map_df.empty:
            sku_map_df = sku_map_df.copy()
            sku_map_df["sku"] = sku_map_df["sku"].astype(str).str.strip()

            mapped_skus = set(sku_map_df["sku"].dropna())
            df["__has_mapping__"] = df["sku"].astype(str).str.strip().isin(mapped_skus)

            df = df.merge(
                sku_map_df,
                on="sku",
                how="left",
                suffixes=("", "_from_sku_table"),
            )

            if "product_name_from_sku_table" in df.columns:
                df["product_name"] = df["product_name_from_sku_table"].combine_first(df["product_name"])
                df.drop(columns=["product_name_from_sku_table"], inplace=True)
    except Exception as e:
        print("[WARN] Failed to fetch/merge SKU product mapping:", e)

    # ----------------------------
    # Numeric prep
    # ----------------------------
    df["quantity"] = safe_num(df.get("quantity", 0))
    df["profit"] = safe_num(df.get("profit", 0))
    df["cogs"] = safe_num(df.get("cogs", 0))
    df["product_sales"] = safe_num(df.get("product_sales", 0))
    df["promotional_rebates"] = safe_num(df.get("promotional_rebates", 0))
    df["net_sales"] = df["product_sales"] + df["promotional_rebates"]

    # ✅ gross_sales from DB (fallback compute if missing)
    if "gross_sales" in df.columns:
        df["gross_sales"] = safe_num(df.get("gross_sales", 0))
    else:
        # fallback: best-effort = product_sales only
        df["gross_sales"] = safe_num(df.get("product_sales", 0))

    df["total"] = safe_num(df.get("total", 0))

    for col in ["description", "type", "bucket"]:
        if col not in df.columns:
            df[col] = ""

    df["description"] = df["description"].fillna("").astype(str)
    df["type"] = df["type"].fillna("").astype(str)
    df["bucket"] = df["bucket"].fillna("").astype(str)

    # Quantity should count sales rows only.
    # Same refund/return detection logic as SKU-wise monthly table,
    # using bucket instead of transaction_type in liveorders.
    desc_lower = df["description"].str.lower()
    type_lower = df["type"].str.lower()
    bucket_lower = df["bucket"].str.lower()

    return_mask = (
        desc_lower.str.contains("refund|return", case=False, na=False, regex=True)
        | type_lower.str.contains("refund|return", case=False, na=False, regex=True)
        | bucket_lower.str.contains("refund|return", case=False, na=False, regex=True)
    )

    df["quantity_filtered"] = 0.0
    df.loc[~return_mask, "quantity_filtered"] = df.loc[~return_mask, "quantity"].abs()

    # ----------------------------
    # Fee extraction (LIVEORDERS-SPECIFIC)
    # ----------------------------
    def _fee_amount_col(xdf: pd.DataFrame) -> pd.Series:
        if "other_transaction_fees" in xdf.columns:
            s = safe_num(xdf["other_transaction_fees"])
            if float(np.nansum(s.values)) != 0.0:
                return s
        return safe_num(xdf["total"])

    def _calc_fees_from_liveorders(day_df: pd.DataFrame) -> tuple[float, float]:
        if day_df is None or day_df.empty:
            return 0.0, 0.0

        t = day_df["type"].fillna("").astype(str).str.lower()
        d = day_df["description"].fillna("").astype(str).str.lower()
        amt = _fee_amount_col(day_df)

        ignore = (
            t.str.contains("transfer|disbursement", na=False)
            | d.str.contains("disbursement", na=False)
            | d.str.contains("order payment", na=False)
        )

        is_ads = (
            # Product Ads payments
            t.str.contains(r"productadspayment", na=False)

            # Deal-related ads
            | d.str.contains(
                r"dealperformanceevent|dealparticipationevent",
                na=False
            )

            # Coupon-related ads
            | d.str.contains(
                r"couponparticipationevent|couponperformanceevent",
                na=False
            )
        ) & (~ignore)



        is_platform_fee = (
            (t.str.contains("servicefee", na=False) | d.str.contains(r"\bfee\b", na=False))
            & d.str.contains(
                r"fba|storage|disposal|subscription|longterm|long term|referral|commission",
                na=False
            )
        ) & (~ignore) & (~is_ads)

        ads_total = float(np.nansum(amt[is_ads].values))
        pf_total  = float(np.nansum(amt[is_platform_fee].values))

        return abs(pf_total), abs(ads_total)

    # ----------------------------
    # Per-SKU metrics
    # ----------------------------
    df_sku = df.dropna(subset=["sku"]).copy()

    sku_agg = (
        df_sku.groupby("sku", as_index=False)
          .agg(
              product_name=("product_name", "first"),
              quantity=("quantity_filtered", "sum"),
              net_sales=("net_sales", "sum"),
              product_sales=("product_sales", "sum"),
              gross_sales=("gross_sales", "sum"),  # ✅ NEW
              profit=("profit", "sum"),
              cogs=("cogs", "sum"),
              __has_mapping__=("__has_mapping__", "max"),
          )
    )

    qty_nonzero = sku_agg["quantity"].replace(0, np.nan)
    sku_agg["asp"] = (sku_agg["net_sales"] / qty_nonzero).fillna(0.0)
    sku_agg["unit_wise_profitability"] = (sku_agg["profit"] / qty_nonzero).fillna(0.0)

    total_net_sales = float(sku_agg["net_sales"].sum())
    sku_agg["sales_mix"] = (sku_agg["net_sales"] / total_net_sales) * 100.0 if total_net_sales else 0.0
    sku_agg = normalize_sales_mix(sku_agg, "sales_mix", digits=2)

    sku_metrics = sku_agg.to_dict(orient="records")

    # ----------------------------
    # Daily series
    # ----------------------------
    daily_series = []

    df["date_only"] = pd.to_datetime(df["purchase_date"], errors="coerce").dt.date
    df = df.dropna(subset=["date_only"])

    daily_qty = df.groupby("date_only", as_index=False)["quantity_filtered"].sum()
    qty_map = {d: float(v) for d, v in zip(daily_qty["date_only"], daily_qty["quantity_filtered"])}

    daily_ns = df.groupby("date_only", as_index=False)["net_sales"].sum()
    ns_map = {d: float(v) for d, v in zip(daily_ns["date_only"], daily_ns["net_sales"])}

    daily_ps = df.groupby("date_only", as_index=False)["product_sales"].sum()
    ps_map = {d: float(v) for d, v in zip(daily_ps["date_only"], daily_ps["product_sales"])}

    daily_gs = df.groupby("date_only", as_index=False)["gross_sales"].sum()  # ✅ NEW
    gs_map = {d: float(v) for d, v in zip(daily_gs["date_only"], daily_gs["gross_sales"])}

    daily_profit = df.groupby("date_only", as_index=False)["profit"].sum()
    profit_map = {d: float(v) for d, v in zip(daily_profit["date_only"], daily_profit["profit"])}

    pf_map, ad_map, remb_map = {}, {}, {}

    for d, day_df in df.groupby("date_only"):
        pf, ad = _calc_fees_from_liveorders(day_df)
        pf_map[d] = float(pf)
        ad_map[d] = float(ad)

        # ✅ NEW: reimbursement from raw transactions
        remb_map[d] = float(compute_net_reimbursement_from_df(day_df))

    all_days = sorted(set(qty_map) | set(ns_map) | set(ps_map) | set(gs_map) | set(profit_map) | set(pf_map) | set(ad_map) | set(remb_map))
    for d in all_days:
        daily_series.append({
            "date": d.isoformat(),
            "quantity": qty_map.get(d, 0.0),
            "net_sales": ns_map.get(d, 0.0),
            "product_sales": ps_map.get(d, 0.0),
            "gross_sales": gs_map.get(d, 0.0),   # ✅ NEW
            "profit": profit_map.get(d, 0.0),
            "platform_fee": pf_map.get(d, 0.0),
            "advertising": ad_map.get(d, 0.0),
            # ✅ NEW
            "rembursement_fee": remb_map.get(d, 0.0),

        })

    return sku_metrics, daily_series


# ----------------------------------------------------------------------------
# GROWTH METRIC CALCULATION (same formulas as Business Insights)
# -----------------------------------------------------------------------------

growth_field_mapping = {
    "quantity": "Unit Growth (%)",
    "asp": "ASP Growth (%)",
    "net_sales": "Net Sales Growth (%)",
    "product_sales": "Gross Sales Growth (%)",
    "sales_mix": "Sales Mix Change (%)",
    "unit_wise_profitability": "Profit Per Unit (%)",
    "profit": "CM1 Profit Impact (%)",
}


def categorize_growth(value):
    if value is None:
        return "No Data"
    if value >= 5:
        return "High Growth"
    elif value > 0.5:
        return "Low Growth"
    elif value < -0.5:
        return "Negative Growth"
    else:
        return "No Growth"


def safe_float_local(val):
    try:
        if val is None:
            return None
        return float(val)
    except (ValueError, TypeError):
        return None

def round_numeric_values(obj, ndigits=2):
    """
    Recursively walk any dict/list and:
    - round floats to `ndigits`
    - convert None / NaN to 0.0 (so UI blanks don't appear)
    """

    # ✅ None -> 0
    if obj is None:
        return 0.0

    # ✅ NaN (python float) -> 0
    if isinstance(obj, float) and np.isnan(obj):
        return 0.0

    # float -> round
    if isinstance(obj, float):
        return round(obj, ndigits)

    # numpy floats -> round (NaN safe)
    if isinstance(obj, (np.floating,)):
        v = float(obj)
        if np.isnan(v):
            return 0.0
        return round(v, ndigits)

    # numpy ints -> normal int
    if isinstance(obj, (np.integer,)):
        return int(obj)

    # dict -> recurse values
    if isinstance(obj, dict):
        return {k: round_numeric_values(v, ndigits) for k, v in obj.items()}

    # list/tuple -> recurse items
    if isinstance(obj, (list, tuple)):
        return [round_numeric_values(v, ndigits) for v in obj]

    # strings/bool/etc as-is
    return obj


def build_segment_total_row(prev_segment, curr_segment, key="sku", label="Total"):
    """
    prev_segment / curr_segment: subset of prev_data / curr_data
    (sirf woh SKUs jo top_80 me hain, etc.)

    Return: ek row jaisa calculate_growth deta hai, bas aggregated.
    """
    # ---- totals for quantity / net_sales / profit ----
    prev_qty = prev_net = prev_prof = 0.0
    curr_qty = curr_net = curr_prof = 0.0

    # previous
    for r in prev_segment:
        q = safe_float_local(r.get("quantity"))
        s = safe_float_local(r.get("net_sales"))
        p = safe_float_local(r.get("profit"))
        if q is not None: prev_qty += q
        if s is not None: prev_net += s
        if p is not None: prev_prof += p

    # current
    for r in curr_segment:
        q = safe_float_local(r.get("quantity"))
        s = safe_float_local(r.get("net_sales"))
        p = safe_float_local(r.get("profit"))
        if q is not None: curr_qty += q
        if s is not None: curr_net += s
        if p is not None: curr_prof += p

    # ASP / unit profit indexes (portfolio level)
    prev_asp = prev_net / prev_qty if prev_qty else None
    curr_asp = curr_net / curr_qty if curr_qty else None

    prev_up = prev_prof / prev_qty if prev_qty else None
    curr_up = curr_prof / curr_qty if curr_qty else None

    # sales mix (sum of sku-wise mix)
    prev_mix = sum(
        (safe_float_local(r.get("sales_mix")) or 0.0) for r in prev_segment
    )
    curr_mix = sum(
        (safe_float_local(r.get("sales_mix")) or 0.0) for r in curr_segment
    )

    # pseudo SKU rows
    seg_id = f"{label.upper().replace(' ', '_')}_SEGMENT"

    prev_row = {
        key: seg_id,
        "product_name": label,
        "quantity": prev_qty,
        "net_sales": prev_net,
        "profit": prev_prof,
        "asp": prev_asp,
        "unit_wise_profitability": prev_up,
        "sales_mix": prev_mix,
    }

    curr_row = {
        key: seg_id,
        "product_name": label,
        "quantity": curr_qty,
        "net_sales": curr_net,
        "profit": curr_prof,
        "asp": curr_asp,
        "unit_wise_profitability": curr_up,
        "sales_mix": curr_mix,
    }

    # existing logic reuse
    seg_growth_list = calculate_growth([prev_row], [curr_row], key=key)
    return seg_growth_list[0] if seg_growth_list else None

def calc_profit_pct(profit, net_sales):
    profit = safe_float_local(profit)
    net_sales = safe_float_local(net_sales)

    if profit is None or net_sales is None or net_sales == 0:
        return 0.0

    return round((profit / net_sales) * 100.0, 2)

def calculate_growth(prev_data, curr_data, key="sku", numeric_fields=None) -> list:
    """
    prev_data / curr_data: list[dict] with keys
      quantity, asp, net_sales, sales_mix, unit_wise_profitability, profit, product_name, sku

    Returns, per SKU:
      - <field>_prev  (previous period raw value)
      - <field>_curr  (current MTD raw value)
      - % growth mapped via growth_field_mapping
      - Sales Mix (Current)  (for categorization / frontend)
      - new_or_reviving flag
      - profit_pct_prev / profit_pct_curr  ✅ NEW (NO growth)
    """
    if numeric_fields is None:
        numeric_fields = list(growth_field_mapping.keys())

    prev_dict = {row.get(key): row for row in prev_data if row.get(key)}
    results = []

    for row2 in curr_data:
        item_key = row2.get(key)
        if not item_key:
            continue

        growth_row = {
            "product_name": row2.get("product_name"),
            key: item_key,
        }

        # ---- Month2 / current sales mix ----
        sales_mix_curr = safe_float_local(row2.get("sales_mix"))
        growth_row["Sales Mix (Current)"] = round(sales_mix_curr, 2) if sales_mix_curr is not None else 0.0

        # -----------------------------
        # Case 1: Existing SKU (in prev_data)
        # -----------------------------
        if item_key in prev_dict:
            row1 = prev_dict[item_key]

            for field in numeric_fields:
                val1 = safe_float_local(row1.get(field))  # previous period
                val2 = safe_float_local(row2.get(field))  # current period

                # ✅ raw values for Excel/UI (no blanks)
                growth_row[f"{field}_prev"] = 0.0 if val1 is None else val1
                growth_row[f"{field}_curr"] = 0.0 if val2 is None else val2

                # ✅ Growth calculation
                if field == "sales_mix":
                    if val1 is None or val2 is None:
                        growth = 0.0
                    else:
                        raw_change = val2 - val1
                        raw_change = clamp_near_zero(raw_change)
                        growth = round(raw_change, 2)
                else:
                    if val1 is None or val2 is None:
                        growth = 0.0
                    elif val1 != 0:
                        growth = round(((val2 - val1) / val1) * 100.0, 2)
                    else:
                        growth = 0.0

                label = growth_field_mapping[field]
                growth_row[label] = {
                    "category": categorize_growth(growth),
                    "value": growth,
                }

            # ✅ PROFIT % (NO growth, absolute value)
            profit_prev = safe_float_local(row1.get("profit"))
            sales_prev = safe_float_local(row1.get("net_sales"))
            profit_curr = safe_float_local(row2.get("profit"))
            sales_curr = safe_float_local(row2.get("net_sales"))

            growth_row["profit_pct_prev"] = (
                round((profit_prev / sales_prev) * 100.0, 2)
                if profit_prev is not None and sales_prev not in (None, 0)
                else 0.0
            )
            growth_row["profit_pct_curr"] = (
                round((profit_curr / sales_curr) * 100.0, 2)
                if profit_curr is not None and sales_curr not in (None, 0)
                else 0.0
            )

        # -----------------------------
        # Case 2: New / Reviving SKU
        # -----------------------------
        else:
            growth_row["new_or_reviving"] = True
            row1 = prev_dict.get(item_key, {})

            for field in numeric_fields:
                val1 = safe_float_local(row1.get(field))
                val2 = safe_float_local(row2.get(field))

                growth_row[f"{field}_prev"] = 0.0 if val1 is None else val1
                growth_row[f"{field}_curr"] = 0.0 if val2 is None else val2

                label = growth_field_mapping[field]

                if val1 is not None and val1 > 0 and val2 is not None:
                    growth = round(((val2 - val1) / val1) * 100.0, 2)
                    growth_row[label] = {
                        "category": categorize_growth(growth),
                        "value": growth,
                    }
                else:
                    growth_row[label] = {
                        "category": "No Data",
                        "value": 0.0,
                    }

            # ✅ PROFIT % (NO growth, absolute value)
            profit_prev = safe_float_local(row1.get("profit"))
            sales_prev = safe_float_local(row1.get("net_sales"))
            profit_curr = safe_float_local(row2.get("profit"))
            sales_curr = safe_float_local(row2.get("net_sales"))

            growth_row["profit_pct_prev"] = (
                round((profit_prev / sales_prev) * 100.0, 2)
                if profit_prev is not None and sales_prev not in (None, 0)
                else 0.0
            )
            growth_row["profit_pct_curr"] = (
                round((profit_curr / sales_curr) * 100.0, 2)
                if profit_curr is not None and sales_curr not in (None, 0)
                else 0.0
            )

        results.append(growth_row)

    return results

def fmt_metric(value, pct, symbol="£", decimals=2):
    if value is None:
        value = 0.0
    if pct is None:
        pct = 0.0

    formatted_value = f"{value:,.{decimals}f}"
    return f"{symbol}{formatted_value} ({pct:+.2f}%)"

def build_global_journey_comparison_for_product(
    product_name,
    uk_data,
    us_data,
):
    """
    Creates one combined UK+US historical journey comparison for a product.
    """

    def _compact_country_payload(country_data):
        compact = []

        for sku, payload in (country_data or {}).items():
            growth_row = payload.get("growth_row", {}) or {}

            compact.append({
                "sku": sku,
                "journey_summary": payload.get("journey_summary", []),
                "current_mtd": {
                    "units": growth_row.get("quantity_curr"),
                    "net_sales": growth_row.get("net_sales_curr"),
                    "profit": growth_row.get("profit_curr"),
                    "asp": growth_row.get("asp_curr"),
                    "unit_profitability": growth_row.get("unit_wise_profitability_curr"),
                    "sales_mix": growth_row.get("Sales Mix (Current)"),
                },
                "previous_mtd": {
                    "units": growth_row.get("quantity_prev"),
                    "net_sales": growth_row.get("net_sales_prev"),
                    "profit": growth_row.get("profit_prev"),
                    "asp": growth_row.get("asp_prev"),
                    "unit_profitability": growth_row.get("unit_wise_profitability_prev"),
                    "sales_mix": growth_row.get("sales_mix_prev"),
                },
                "movement": {
                    "units": (growth_row.get("Unit Growth (%)") or {}).get("value"),
                    "net_sales": (growth_row.get("Net Sales Growth (%)") or {}).get("value"),
                    "profit": (growth_row.get("CM1 Profit Impact (%)") or {}).get("value"),
                    "asp": (growth_row.get("ASP Growth (%)") or {}).get("value"),
                    "unit_profitability": (growth_row.get("Profit Per Unit (%)") or {}).get("value"),
                },
                "inventory_recommendation": payload.get("inventory_recommendation"),
                "ads_recommendation": payload.get("ads_recommendation"),
            })

        return compact

    uk_payload = _compact_country_payload(uk_data)
    us_payload = _compact_country_payload(us_data)

    if not uk_payload and not us_payload:
        return []

    # If only one country exists, do not fake a UK/US comparison.
    if uk_payload and not us_payload:
        out = []
        for item in uk_payload:
            for bullet in item.get("journey_summary", []):
                out.append(f"In the UK, {bullet}")
        return out[:7]

    if us_payload and not uk_payload:
        out = []
        for item in us_payload:
            for bullet in item.get("journey_summary", []):
                out.append(f"In the US, {bullet}")
        return out[:7]

    prompt = f"""
You are creating a combined UK + US product journey comparison for Amazon BI.

Product:
{product_name}

UK data:
{json.dumps(uk_payload, indent=2, default=str)}

US data:
{json.dumps(us_payload, indent=2, default=str)}

Task:
Create a concise journey_comparison array comparing the historical and current journey of this same product across UK and US.

Rules:
- Return valid JSON only.
- Do not return markdown.
- Output only this shape:
{{
  "journey_comparison": [
    "bullet 1",
    "bullet 2"
  ]
}}
- Write 5 to 7 bullets.
- Mention US and UK explicitly.
- Compare scale, demand, net sales, CM1 profit, ASP, unit profitability, and inventory pressure when available.
- Do not recommend actions.
- Do not say data is unavailable unless both countries are empty.
- Keep bullets executive and factual.
"""

    try:
        resp = openai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {
                    "role": "system",
                    "content": "You return valid JSON only.",
                },
                {
                    "role": "user",
                    "content": prompt,
                },
            ],
            temperature=0.2,
        )

        raw = resp.choices[0].message.content or "{}"
        parsed = json.loads(raw)

        journey_comparison = parsed.get("journey_comparison", [])

        if isinstance(journey_comparison, list):
            return [str(x) for x in journey_comparison if x]

        return []

    except Exception as e:
        print("[AI ERROR] Failed to build global journey comparison:", product_name, e)

        fallback = []

        for item in us_payload:
            for bullet in item.get("journey_summary", []):
                fallback.append(f"In the US, {bullet}")

        for item in uk_payload:
            for bullet in item.get("journey_summary", []):
                fallback.append(f"In the UK, {bullet}")

        return fallback[:7]



def render_live_recommended_action(
    *,
    growth_row: dict,
    recommendation: str,
    ads_recommendation: str | None = None,
    inventory_recommendation: str | None = None,
    journey_summary: list[str] | None = None,
    currency_symbol="£",
    inventory_alerts: dict | None = None,   # portfolio alerts
    render_portfolio_inventory: bool = False,  # control rendering
) -> str:

    def _safe_text(value, fallback=""):
        if value is None:
            return fallback
        try:
            if pd.isna(value):
                return fallback
        except Exception:
            pass
        return str(value)

    lines = []

    name = growth_row.get("product_name") or growth_row.get("sku")
    name = _safe_text(name, fallback=_safe_text(growth_row.get("sku"), "Unknown SKU"))

    lines.append(name)
    lines.append("")

    # ---------- Metrics ----------
    lines.append(
        f"ASP: {fmt_metric(growth_row['asp_curr'], growth_row['ASP Growth (%)']['value'], currency_symbol)}")
    lines.append(
        f"Units: {fmt_metric(growth_row['quantity_curr'], growth_row['Unit Growth (%)']['value'], '', decimals=0)}")
    lines.append(
        f"Net sales: {fmt_metric(growth_row['net_sales_curr'], growth_row['Net Sales Growth (%)']['value'], currency_symbol, decimals=0)}")
    lines.append(
        f"CM1 profit: {fmt_metric(growth_row['profit_curr'], growth_row['CM1 Profit Impact (%)']['value'], currency_symbol, decimals=0)}")
    lines.append(
        f"CM1 profit per unit: {fmt_metric(growth_row['unit_wise_profitability_curr'], growth_row['Profit Per Unit (%)']['value'], currency_symbol)}"
    )

    # ---------- Product Journey ----------
    if journey_summary:
        lines.append("")
        lines.append("Product Journey:")
        for bullet in journey_summary:
            lines.append(f"- {_safe_text(bullet)}")

    # ---------- Commercial Recommendation ----------
    lines.append("")
    lines.append(f"Recommendation: {_safe_text(recommendation, 'Monitor performance')}")

    # ---------- Advertising Recommendation ----------
    if ads_recommendation:
        lines.append("")
        lines.append(f"Advertising: {_safe_text(ads_recommendation)}")

    # ---------- Inventory Recommendation (SKU level) ----------
    if inventory_recommendation:
        lines.append("")
        lines.append(f"• Inventory action: {_safe_text(inventory_recommendation)}")

    return "\n".join(str(line) for line in lines)


def render_portfolio_inventory_block(inventory_alerts, currency_symbol="£"):
    if not inventory_alerts:
        return ""

    lines = []
    lines.append("## INVENTORY")

    ageing = inventory_alerts.get("ageing_inventory", {})
    lines.append(
        f"• Ageing inventory (181+ days): "
        f"{ageing.get('total_units',0)} units across "
        f"{ageing.get('total_skus',0)} SKUs"
    )

    high_cov = inventory_alerts.get("high_coverage", {})
    lines.append(
        f"• High coverage SKUs: {high_cov.get('count',0)} SKUs"
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

#-----------------------------------------------------------------------------
# AI SUMMARY (overall header) – now via ChatGPT with numeric fallback
#-----------------------------------------------------------------------------
summary_numeric_fields = [
    "quantity",
    "net_sales",
    "profit",
    
]


def aggregate_totals(rows, fields=None):
    """
    Sum key numeric fields across a list of SKU dicts.
    Used to get total units / sales / profit for each period.
    """
    if fields is None:
        fields = summary_numeric_fields

    totals = {f: 0.0 for f in fields}
    for row in rows:
        for f in fields:
            v = safe_float_local(row.get(f))
            if v is not None:
                totals[f] += v
    return totals




def pct_change(prev, curr, ndigits=2):
    if prev is None or prev == 0:
        return None
    return round(((curr - prev) / prev) * 100.0, ndigits)

def compute_total_asp(rows):
    total_qty = 0.0
    total_sales = 0.0

    for r in rows:
        q = safe_float_local(r.get("quantity"))
        s = safe_float_local(r.get("net_sales"))

        if q and s is not None:
            total_qty += q
            total_sales += s

    if total_qty == 0:
        return None

    # ❌ no rounding here
    return total_sales / total_qty

def compute_total_unit_profitability(rows):
    total_qty = 0.0
    total_profit = 0.0

    for r in rows:
        q = safe_float_local(r.get("quantity"))
        p = safe_float_local(r.get("profit"))

        if q and p is not None:
            total_qty += q
            total_profit += p

    if total_qty == 0:
        return None

    return total_profit / total_qty



def describe_movement(pct, ndigits=2):
    """
    ex: "down 3.00%" / "up 1.25%" / "roughly flat"
    """
    if pct is None:
        return "roughly flat"

    abs_v = abs(pct)
    if abs_v < 1:
        return "roughly flat"

    direction = "up" if pct > 0 else "down"
    return f"{direction} {abs_v:.{ndigits}f}%"


def safe_strip(x, default=""):
    # handles None, NaN, numbers, etc.
    if x is None:
        return default
    if isinstance(x, float) and np.isnan(x):
        return default
    try:
        s = str(x)
    except Exception:
        return default
    s = s.strip()
    return s if s else default



def build_sku_context(sku_rows, max_items=5):
    """
    Poore portfolio (ya jo bhi SKU list tum pass karo) ko kuch logical buckets
    me todta hai taaki AI summary/action bullets specific products ka naam
    le sake.

    Ye function AI ko product-focused labels deta hai — sirf product_name.
    """

    # ------------------------
    # IMPORTANT: Only product name as label
    # ------------------------
    def make_label(row):
        name = safe_strip(row.get("product_name"), default="")
        if name:
            return name
        return "Unnamed SKU"


    fast_growing_profitable = []
    declining_high_mix = []
    flat_but_large = []

    def get_pct(row, key):
        obj = row.get(key) or {}
        return safe_float_local(obj.get("value"))

    # ------------------------
    # Build list of items with metrics
    # ------------------------
    for row in sku_rows:
        mix = safe_float_local(row.get("Sales Mix (Current)"))
        if mix is None:
            continue

        unit_g = get_pct(row, "Unit Growth (%)")
        asp_g = get_pct(row, "ASP Growth (%)")
        sales_g = get_pct(row, "Sales Growth (%)")
        mix_g = get_pct(row, "Sales Mix Change (%)")
        up_g   = get_pct(row, "Profit Per Unit (%)")
        prof_g = get_pct(row, "CM1 Profit Impact (%)")

        item = {
            "label": make_label(row),            # ✅
            "sku": row.get("sku"),
            "product_name": row.get("product_name"),

            # current mix
            "sales_mix_curr": mix,

            # % growth
            "unit_growth_pct": unit_g,
            "asp_growth_pct": asp_g,
            "sales_growth_pct": sales_g,
            "mix_change_pct": mix_g,
            "unit_profit_pct": up_g,
            "profit_growth_pct": prof_g,

            # raw previous/current values
            "quantity_prev": safe_float_local(row.get("quantity_prev")),
            "quantity_curr": safe_float_local(row.get("quantity_curr")),
            "asp_prev": safe_float_local(row.get("asp_prev")),
            "asp_curr": safe_float_local(row.get("asp_curr")),
            "net_sales_prev": safe_float_local(row.get("net_sales_prev")),
            "net_sales_curr": safe_float_local(row.get("net_sales_curr")),
            "sales_mix_prev": safe_float_local(row.get("sales_mix_prev")),
            "sales_mix_curr_raw": safe_float_local(row.get("sales_mix_curr")),
            "unit_profit_prev": safe_float_local(row.get("unit_wise_profitability_prev")),
            "unit_profit_curr": safe_float_local(row.get("unit_wise_profitability_curr")),
            "profit_prev": safe_float_local(row.get("profit_prev")),
            "profit_curr": safe_float_local(row.get("profit_curr")),
        }

        # ------------------------
        # Categorization
        # ------------------------
        if (
            sales_g is not None
            and prof_g is not None
            and sales_g > 5
            and prof_g > 5
            and mix >= 2
        ):
            fast_growing_profitable.append(item)

        elif sales_g is not None and sales_g < -5 and mix >= 2:
            declining_high_mix.append(item)

        elif mix >= 5 and (sales_g is None or abs(sales_g) <= 2):
            flat_but_large.append(item)

    # ------------------------
    # Sort & trim
    # ------------------------
    fast_growing_profitable = sorted(
        fast_growing_profitable,
        key=lambda x: x["sales_growth_pct"] or 0,
        reverse=True,
    )[:max_items]

    declining_high_mix = sorted(
        declining_high_mix,
        key=lambda x: x["sales_growth_pct"] or 0,
    )[:max_items]

    flat_but_large = sorted(
        flat_but_large,
        key=lambda x: x["sales_mix_curr"] or 0,
        reverse=True,
    )[:max_items]

    return {
        "fast_growing_profitable": fast_growing_profitable,
        "declining_high_mix": declining_high_mix,
        "flat_but_large": flat_but_large,
    }


def _cell_value(x):
    if isinstance(x, pd.Series):
        x = x.iloc[0] if not x.empty else None

    if x is None:
        return None

    if isinstance(x, float) and np.isnan(x):
        return None

    return x



def build_inventory_signals(user_id: int, country: str) -> dict:
    signals = {}

    # --------------------------------------------------
    # 1) Inventory coverage ratio
    # --------------------------------------------------
    coverage_df = compute_inventory_coverage_ratio(user_id, country)

    coverage_map = {}
    for _, r in coverage_df.iterrows():
        sku = _cell_value(r.at["sku"])
        if sku is None:
            continue

        sku = str(sku).strip()
        if not sku:
            continue

        cov = _cell_value(r.at["inventory_coverage_ratio"])
        coverage_map[sku] = float(cov) if cov is not None else None

    # --------------------------------------------------
    # 2) Transit time
    # --------------------------------------------------
    transit = None
    try:
        row = fetch_transit_time(
            user_id=user_id,
            marketplace=None,
            country=country,
        )
        if row:
            tv = _cell_value(row.get("transit_time"))
            if tv is not None:
                transit = float(tv)
    except Exception:
        transit = None

    # --------------------------------------------------
    # 3) Inventory aged table
    # --------------------------------------------------
    inv_df = fetch_inventory_aged_by_user(user_id, country)

    for _, r in inv_df.iterrows():
        sku = _cell_value(r.at["sku"])
        if sku is None:
            continue

        sku = str(sku).strip()
        if not sku:
            continue

        def _num(col):
            v = _cell_value(r.at[col])
            return float(safe_num(v)) if v is not None else 0.0

        aged_181_270 = _num("inv-age-181-to-270-days")
        aged_271_365 = _num("inv-age-271-to-365-days")
        aged_365p    = _num("inv-age-365-plus-days")

        aged_qty = aged_181_270 + aged_271_365 + aged_365p
        overaged = aged_qty > 0

        cover = coverage_map.get(sku)
        low_cover = (
            transit is not None
            and cover is not None
            and cover < transit
        )

        # ---- amazon recommendation (ABSOLUTELY SAFE) ----
        amazon_action = None
        raw = r.at["recommended-action"]

        if raw is not None and not pd.isna(raw):
            txt = str(raw).strip()
            if txt and txt != "NoRestockExcessActionRequired":
                amazon_action = txt

        signals[sku] = {
            "low_cover": bool(low_cover),
            "overaged": bool(overaged),
            "aged_units": int(aged_qty),
            "amazon_recommendation": amazon_action,
        }

    return signals

def sanitize_strings(obj):
    """
    Recursively sanitize strings so LLM output cannot break JSON.
    """
    if isinstance(obj, str):
        return obj.replace('"', "'").replace("\n", " ").strip()

    if isinstance(obj, dict):
        return {k: sanitize_strings(v) for k, v in obj.items()}

    if isinstance(obj, list):
        return [sanitize_strings(v) for v in obj]

    return obj


def safe_json_load(s: str) -> dict:
    """
    Safely parse JSON returned by LLM.
    Repairs common formatting issues.
    """
    try:
        return json.loads(s)
    except json.JSONDecodeError:
        s = s.strip()
        start = s.find("{")
        end = s.rfind("}")
        if start != -1 and end != -1 and end > start:
            return json.loads(s[start:end + 1])
        raise



def safe0(x):
    v = safe_float_local(x)
    return v if v is not None else 0.0

def pct_change_2(prev, curr):
    prev = safe_float_local(prev)
    curr = safe_float_local(curr)
    if prev is None or prev == 0:
        return None
    return round(((curr - prev) / prev) * 100.0, 2)



def run_live_prompt_1_analysis(payload: dict) -> dict:
    resp = oa_client.chat.completions.create(
        model="gpt-4o",
        messages=[
            {"role": "system", "content": LIVE_BI_PROMPT_1_ANALYSIS},
            {"role": "user", "content": json.dumps(payload)},
        ],
        temperature=0,
        response_format={"type": "json_object"},
        max_tokens=700,
    )
    return json.loads(resp.choices[0].message.content)


def run_live_prompt_1_5_summary(
    analysis_output: dict,
    numeric_context: dict,
    user_objective: dict,
) -> dict:
    payload = {
        "analysis_output": analysis_output,
        "numeric_context": numeric_context,
        "user_objective": user_objective,
    }

    try:
        resp = oa_client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": LIVE_BI_PROMPT_1_5_SUMMARY},
                {"role": "user", "content": json.dumps(payload, default=str)},
            ],
            temperature=0,
            response_format={"type": "json_object"},

            
            max_tokens=1600,
        )

        raw = resp.choices[0].message.content or "{}"

        try:
            parsed = json.loads(raw)
        except Exception as parse_error:
            print("[AI ERROR] Prompt-1.5 raw response was:", raw)
            raise parse_error

        summary_text = parsed.get("summary_text", "")
        metric_bullets = parsed.get("metric_bullets", [])

        if not isinstance(metric_bullets, list):
            metric_bullets = []

        return {
            "summary_text": str(summary_text or ""),
            "metric_bullets": [str(x) for x in metric_bullets if x],
        }

    except Exception as e:
        print("[AI ERROR] Prompt-1.5 summary failed:", e)
        return {
            "summary_text": "",
            "metric_bullets": [],
        }


def run_inventory_ai_summary(inventory_summary: dict) -> dict:
    resp = oa_client.chat.completions.create(
        model="gpt-4o",
        messages=[
            {"role": "system", "content": LIVE_BI_INVENTORY_SUMMARY_PROMPT},
            {"role": "user", "content": json.dumps(inventory_summary)},
        ],
        temperature=0,
        response_format={"type": "json_object"},
        max_tokens=200,
    )
    return json.loads(resp.choices[0].message.content)


def fetch_skuwisemonthly_ads_cm2_current_month(
    user_id: int,
    country: str,
    year: int,
    month: int,
) -> tuple[dict, dict]:
    """
    Reads current month's skuwisemonthly table for:
      sku, ads_spend, cm2_profit
    Returns:
      sku_map: {sku: {"ads_spend": float, "cm2_profit": float}}
      totals:  {"ads_spend": float, "cm2_profit": float}  (prefers TOTAL row if present)
    Safe behavior:
      - if table/cols missing -> empty map + 0 totals
      - removes TOTAL from sku_map
    """
    country = (country or "uk").strip().lower()
    month = int(month)
    year = int(year)

    mn = month_name[month].lower()
    table = f"skuwisemonthly_{user_id}_{country}_{mn}_{year}"

    try:
        with engine_hist.connect() as conn:
            df = pd.read_sql(
                text(f"SELECT sku, ads_spend, cm2_profit FROM {table}"),
                conn,
            )
    except Exception as e:
        print(f"[WARN] Could not read ads/cm2 from {table}: {e}")
        return {}, {"ads_spend": 0.0, "cm2_profit": 0.0}

    if df is None or df.empty or "sku" not in df.columns:
        return {}, {"ads_spend": 0.0, "cm2_profit": 0.0}

    df = df.copy()
    df["sku"] = df["sku"].astype(str).str.strip()
    df.loc[df["sku"].str.lower().isin(["none", "nan", "null", ""]), "sku"] = None

    # if cols missing (shouldn't for current month, but safe)
    if "ads_spend" not in df.columns:
        df["ads_spend"] = 0.0
    if "cm2_profit" not in df.columns:
        df["cm2_profit"] = 0.0

    df["ads_spend"] = safe_num(df["ads_spend"])
    df["cm2_profit"] = safe_num(df["cm2_profit"])

    # Totals: prefer TOTAL row if present
    total_row = df[df["sku"].fillna("").str.lower() == "total"]
    if not total_row.empty:
        totals = {
            "ads_spend": float(safe_num(total_row["ads_spend"]).sum()),
            "cm2_profit": float(safe_num(total_row["cm2_profit"]).sum()),
        }
    else:
        totals = {
            "ads_spend": float(safe_num(df["ads_spend"]).sum()),
            "cm2_profit": float(safe_num(df["cm2_profit"]).sum()),
        }

    # SKU map excluding TOTAL + nulls
    sku_df = df[df["sku"].notna() & (df["sku"].str.lower() != "total")]

    sku_map: dict[str, dict] = {}
    for _, r in sku_df.iterrows():
        sku = str(r["sku"]).strip()
        if not sku:
            continue
        sku_map[sku] = {
            "ads_spend": float(r.get("ads_spend") or 0.0),
            "cm2_profit": float(r.get("cm2_profit") or 0.0),
        }

    return sku_map, totals

def build_remaining_skus_aggregate(top_80_skus: list, focus_skus: list):
    focus_set = set([str(x).strip() for x in (focus_skus or [])])

    remaining = [
        r for r in (top_80_skus or [])
        if str(r.get("sku")).strip() not in focus_set
    ]

    if not remaining:
        return None

    prev_qty = sum(safe0(r.get("quantity_prev")) for r in remaining)
    curr_qty = sum(safe0(r.get("quantity_curr")) for r in remaining)

    prev_sales = sum(safe0(r.get("net_sales_prev")) for r in remaining)
    curr_sales = sum(safe0(r.get("net_sales_curr")) for r in remaining)

    prev_profit = sum(safe0(r.get("profit_prev")) for r in remaining)
    curr_profit = sum(safe0(r.get("profit_curr")) for r in remaining)

    prev_asp = prev_sales / prev_qty if prev_qty else None
    curr_asp = curr_sales / curr_qty if curr_qty else None

    prev_ppu = prev_profit / prev_qty if prev_qty else None
    curr_ppu = curr_profit / curr_qty if curr_qty else None

    return {
        "sku": "REMAINING_SEGMENT",
        "product_name": "Remaining SKUs",
        "quantity_prev": prev_qty,
        "quantity_curr": curr_qty,
        "net_sales_prev": prev_sales,
        "net_sales_curr": curr_sales,
        "profit_prev": prev_profit,
        "profit_curr": curr_profit,
        "asp_prev": prev_asp,
        "asp_curr": curr_asp,
        "unit_wise_profitability_prev": prev_ppu,
        "unit_wise_profitability_curr": curr_ppu,
    }




def build_ai_summary(
    prev_totals,
    curr_totals,
    top_80_skus,
    prev_label,
    curr_label,
    sku_context=None,
    inventory_signals=None,
    prev_fee_totals=None,
    curr_fee_totals=None,
    estimated_storage_cost_next_month=0.0,
    portfolio_inventory_alerts=None,
    currency=None,
    user_objective=None,
    movement_context=None,
    sku_to_product=None,
    group_inventory_alerts=True,
    # ✅ NEW
    user_id=None,
    country=None,
    current_year=None,
    current_month=None,

    # ✅ NEW (required for strategy engine)
    analysis_output=None,
    focus_skus=None,
):
  


    # =========================================================
    # Numeric calculations (UNCHANGED)
    # =========================================================
    qty_prev = safe0(prev_totals.get("quantity"))
    qty_curr = safe0(curr_totals.get("quantity"))

    sales_prev = safe0(prev_totals.get("net_sales"))
    sales_curr = safe0(curr_totals.get("net_sales"))

    prof_prev = safe0(prev_totals.get("profit"))
    prof_curr = safe0(curr_totals.get("profit"))

    asp_prev = safe_float_local(prev_totals.get("total_asp"))
    asp_curr = safe_float_local(curr_totals.get("total_asp"))

    up_prev_idx = safe0(prev_totals.get("unit_wise_profitability"))
    up_curr_idx = safe0(curr_totals.get("unit_wise_profitability"))

    qty_pct   = pct_change_2(qty_prev, qty_curr)
    sales_pct = pct_change_2(sales_prev, sales_curr)
    prof_pct  = pct_change_2(prof_prev, prof_curr)
    asp_pct   = pct_change_2(asp_prev, asp_curr)
    up_pct    = pct_change_2(up_prev_idx, up_curr_idx)

    pf_prev = safe_float_local((prev_fee_totals or {}).get("platform_fee"))
    pf_curr = safe_float_local((curr_fee_totals or {}).get("platform_fee"))

    ad_prev = safe_float_local((prev_fee_totals or {}).get("advertising"))
    ad_curr = safe_float_local((curr_fee_totals or {}).get("advertising"))

    total_cost_prev = (pf_prev or 0.0) + (ad_prev or 0.0)
    total_cost_curr = (pf_curr or 0.0) + (ad_curr or 0.0)

    cost_pct = pct_change_2(total_cost_prev, total_cost_curr)
    pf_pct   = pct_change_2(pf_prev, pf_curr)
    ad_pct   = pct_change_2(ad_prev, ad_curr)

    def calc_roas(ad_cost, net_sales):
        if not ad_cost or not net_sales:
            return 0.0
        return round((ad_cost / net_sales) * 100.0, 2)

    roas_prev = calc_roas(ad_prev, sales_prev)
    roas_curr = calc_roas(ad_curr, sales_curr)
    roas_change = round(roas_curr - roas_prev, 2)

    if sku_context is None:
        sku_context = {
            "fast_growing_profitable": [],
            "declining_high_mix": [],
            "flat_but_large": [],
        }

    inv_payload = inventory_signals or {}
    portfolio_inventory_payload = portfolio_inventory_alerts or {}

    # =========================================================
    # 🚫 SKUs to Skip
    # =========================================================
    SKUS_TO_SKIP = {
        "B075HB7GSJ",
        "B0CRYQ6HBH",
        "B0F9FS43K3",
        "X001VGZOM9",
    }

    # Remove them from top_80_skus
    top_80_skus = [
        row for row in (top_80_skus or [])
        if row.get("sku") not in SKUS_TO_SKIP
    ]

    # Remove them from focus_skus (THIS IS THE MISSING PART)
    if focus_skus:
        focus_skus = [
            sku for sku in focus_skus
            if sku not in SKUS_TO_SKIP
        ]

    # ✅ Build aggregated remaining segment
    remaining_segment_raw = build_remaining_skus_aggregate(
        top_80_skus=top_80_skus,
        focus_skus=focus_skus,
    )

    remaining_growth_row = None

    if remaining_segment_raw:
        remaining_growth_row = calculate_growth(
            prev_data=[{
                "sku": remaining_segment_raw["sku"],
                "product_name": remaining_segment_raw["product_name"],
                "quantity": remaining_segment_raw["quantity_prev"],
                "net_sales": remaining_segment_raw["net_sales_prev"],
                "profit": remaining_segment_raw["profit_prev"],
                "asp": remaining_segment_raw["asp_prev"],
                "unit_wise_profitability": remaining_segment_raw["unit_wise_profitability_prev"],
                "sales_mix": 0,
            }],
            curr_data=[{
                "sku": remaining_segment_raw["sku"],
                "product_name": remaining_segment_raw["product_name"],
                "quantity": remaining_segment_raw["quantity_curr"],
                "net_sales": remaining_segment_raw["net_sales_curr"],
                "profit": remaining_segment_raw["profit_curr"],
                "asp": remaining_segment_raw["asp_curr"],
                "unit_wise_profitability": remaining_segment_raw["unit_wise_profitability_curr"],
                "sales_mix": 0,
            }],
            key="sku"
        )[0]    

    # =========================================================
    # ✅ SKU-Level Ads + CM2 Enrichment (Current Month Only)
    # =========================================================
    ads_sku_map = {}
    ads_monthly_totals = {"ads_spend": 0.0, "cm2_profit": 0.0}

    resolved_country = (country or (currency or {}).get("country") or "uk").strip().lower()

    # Resolve year/month safely
    if current_year is None or current_month is None:
        today_local = date.today()
        current_year = current_year or today_local.year
        current_month = current_month or today_local.month

    if user_id:
        try:
            ads_sku_map, ads_monthly_totals = fetch_skuwisemonthly_ads_cm2_current_month(
                user_id=int(user_id),
                country=resolved_country,
                year=int(current_year),
                month=int(current_month),
            )
        except Exception as e:
            print("[WARN] Ads enrichment failed:", e)
            ads_sku_map = {}
            ads_monthly_totals = {"ads_spend": 0.0, "cm2_profit": 0.0}

    # Attach ads data to each SKU row
    for row in (top_80_skus or []):
        sku = safe_strip(row.get("sku"), default="")
        if not sku:
            continue

        ads_data = ads_sku_map.get(sku, {})
        ads_spend = float(ads_data.get("ads_spend", 0.0))
        cm2_profit = float(ads_data.get("cm2_profit", 0.0))

        net_sales_curr = safe_float_local(row.get("net_sales_curr"))
        if net_sales_curr is None:
            net_sales_curr = safe_float_local(row.get("net_sales"))
        net_sales_curr = float(net_sales_curr or 0.0)

        acos = 0.0
        if net_sales_curr != 0.0:
            acos = round((ads_spend / net_sales_curr) * 100.0, 2)

        cm2_margin = 0.0
        if net_sales_curr != 0.0:
            cm2_margin = round((cm2_profit / net_sales_curr) * 100.0, 2)

        row["ads_spend_curr"] = round(ads_spend, 2)
        row["acos_curr"] = acos
        row["cm2_profit_curr"] = round(cm2_profit, 2)
        row["cm2_margin_curr"] = cm2_margin



    # =========================================================
    # PAYLOAD (UNCHANGED STRUCTURE)
    # =========================================================
    payload = {
        "periods": {
            "previous": {
                "label": prev_label,
                "quantity_total": qty_prev,
                "net_sales_total": sales_prev,
                "profit_total": prof_prev,
                "total_asp": asp_prev,
                "unit_profit_sum_index": up_prev_idx,
            },
            "current": {
                "label": curr_label,
                "quantity_total": qty_curr,
                "net_sales_total": sales_curr,
                "profit_total": prof_curr,
                "total_asp": asp_curr,
                "unit_profit_sum_index": up_curr_idx,
            },
        },
        "pct_changes": {
            "quantity_pct": qty_pct,
            "net_sales_pct": sales_pct,
            "profit_pct": prof_pct,
            "asp_pct": asp_pct,
            "unit_profit_index_pct": up_pct,
        },
        "sku_tables": {
            "top_80_skus": top_80_skus,
            
        },
        "sku_context": sku_context,
        "inventory_signals": inv_payload,
        "portfolio_inventory_alerts": portfolio_inventory_payload,
        "selling_costs": {
            "platform_fees": {
                "pct_change": pf_pct,
                "current": pf_curr or 0.0,
            },
            "advertising_cost": {
                "pct_change": ad_pct,
                "current": ad_curr or 0.0,
            },
            "total": {
                "pct_change": cost_pct,
                "current": total_cost_curr,
            },
        },
        "roas": {
            "previous": roas_prev,
            "current": roas_curr,
            "change": roas_change,
        },
        "ads_monthly": {
            "year": int(current_year),
            "month": int(current_month),
            "ads_spend_total": round(ads_monthly_totals.get("ads_spend", 0.0), 2),
            "cm2_profit_total": round(ads_monthly_totals.get("cm2_profit", 0.0), 2),
        },

        "estimated_platform_fees_next_month": estimated_storage_cost_next_month,
        "currency": currency or {},
        "user_objective": user_objective,
        "movement_context": movement_context or {},
    }
   
    

  
    return payload



#-----------------------------------------------------------------------------
# ChatGPT insight generator for live MTD vs previous (per-SKU)
#-----------------------------------------------------------------------------
def get_sku_monthly_history(user_id, country_lower, sku_key, end_year, end_month, n=24):
    history = []

    for i in range(n):
        y = end_year
        m = end_month - i
        while m <= 0:
            m += 12
            y -= 1

        month_str = month_name[m].lower()
        table = f"skuwisemonthly_{user_id}_{country_lower}_{month_str}{y}"

        try:
            q = text(f"""
                SELECT total_quantity, net_sales, profit, asp
                FROM {table}
                WHERE sku = :k
                LIMIT 1
            """)
            with engine_hist.connect() as conn:
                r = conn.execute(q, {"k": sku_key}).first()

            if r:
                history.append({
                    "period": f"{y:04d}-{m:02d}",
                    "units": float(r.total_quantity or 0),
                    "sales": float(r.net_sales or 0),
                    "profit": float(r.profit or 0),
                    "asp": float(r.asp or 0),
                })

        except Exception:
            continue

    return sorted(history, key=lambda x: x["period"])



def club_inventory_alerts_by_type(
    alerts: dict,
    sku_to_product: dict | None = None,
    max_skus_per_bucket: int = 5,
) -> dict:
    """
    Group ALL inventory alerts into buckets (except 'No alert').
    Returns:
      { "summary": [ {type,label,skus,count}, ... ] }
    """

    buckets = {
        "supply": {"label": "Supply risk", "skus": []},
        "cost": {"label": "High storage cost", "skus": []},
        "ageing": {"label": "Ageing inventory", "skus": []},
        "excess": {"label": "High inventory coverage", "skus": []},
    }

    for sku, a in (alerts or {}).items():
        alert_type = a.get("alert_type")

        # ✅ exclude only "none" (No alert)
        if not alert_type or alert_type == "none":
            continue

        if alert_type not in buckets:
            continue

        label = sku_to_product.get(sku, sku) if sku_to_product else sku
        if label:
            buckets[alert_type]["skus"].append(label)

    # keep a consistent order (optional but nice)
    order = ["supply", "cost", "ageing", "excess"]

    summary = []
    for t in order:
        skus = buckets[t]["skus"]
        if not skus:
            continue
        summary.append({
            "type": t,
            "label": buckets[t]["label"],
            "skus": skus[:max_skus_per_bucket],
            "count": len(skus),
        })

    return {"summary": summary}




def generate_inventory_alerts_for_all_skus(user_id: int, country: str, coverage_df: pd.DataFrame = None) -> dict:
    alerts = {}

    # Use passed current-inventory coverage first
    coverage_map = {}

    if coverage_df is not None and not coverage_df.empty:
        coverage_map = {
            str(r["SKU"]).strip().lower(): r["Coverage Ratio (In Months)"]
            for _, r in coverage_df.iterrows()
            if r.get("SKU") not in [None, "", "Total"]
        }
    else:
        coverage_df = compute_inventory_coverage_ratio(user_id, country)
        coverage_map = {
            str(r["sku"]).strip().lower(): r["inventory_coverage_ratio"]
            for _, r in coverage_df.iterrows()
            if r.get("sku") is not None
        }

    inv_df = fetch_inventory_aged_by_user(user_id, country)

    for _, r in inv_df.iterrows():
        sku_raw = r.get("sku")
        if not sku_raw:
            continue

        sku = str(sku_raw).strip().lower()

        def _num(col):
            try:
                return float(r.get(col) or 0)
            except:
                return 0.0

        aged_qty = (
            _num("inv-age-181-to-330-days")
            + _num("inv-age-331-to-365-days")
        )
        overaged = aged_qty > 0

        estimated_storage_cost = _num("estimated-storage-cost-next-month")
        coverage_ratio = pd.to_numeric(coverage_map.get(sku), errors="coerce")

        alert = "No alert"
        alert_type = "none"

        if pd.notna(coverage_ratio):
            if coverage_ratio <= 2 and coverage_ratio > 0:
                alert = "High alert"
                alert_type = "supply_high"

            elif coverage_ratio <= 5 and coverage_ratio > 0:
                alert = "Please send shipment"
                alert_type = "supply_medium"

            elif coverage_ratio >= 6:
                alert = "High inventory coverage ratio."
                alert_type = "excess"

        if alert_type == "none" and overaged:
            alert = "Ageing Inventory. Ref. AI Insights"
            alert_type = "ageing"

        if alert_type == "none" and estimated_storage_cost > 100:
            alert = "High storage cost"
            alert_type = "cost"

        alerts[str(sku_raw).strip().upper()] = {
            "alert": alert,
            "alert_type": alert_type,
        }

    return alerts


def generate_sku_inventory_flags(
    user_id: int,
    country: str,
    focus_skus: list[str] | None = None,
) -> dict:
    """
    Returns SKU-level inventory flags including:
    - alert_type
    - numeric signals
    - inventory_recommendation (ready sentence)
    """

    flags: dict[str, dict] = {}

    coverage_df = compute_inventory_coverage_ratio(user_id, country)

    coverage_map = {}
    if coverage_df is not None and not coverage_df.empty:
        for _, r in coverage_df.iterrows():
            sku = str(r.get("sku") or "").strip()
            if not sku:
                continue
            cov = r.get("inventory_coverage_ratio")
            coverage_map[sku] = float(cov) if cov is not None else None

    inv_df = fetch_inventory_aged_by_user(user_id, country)

    focus_set = set([str(x).strip() for x in (focus_skus or [])]) if focus_skus else None

    def _num(v):
        try:
            if v is None or (isinstance(v, float) and np.isnan(v)):
                return 0.0
            return float(v)
        except:
            return 0.0

    if inv_df is None or inv_df.empty:
        return flags

    for _, r in inv_df.iterrows():
        sku = str(r.get("sku") or "").strip()
        if not sku:
            continue

        if focus_set and sku not in focus_set:
            continue

        coverage_ratio = coverage_map.get(sku)


        # ✅ SUPPORT BOTH SCHEMAS
        aged_1 = _num(r.get("inv-age-181-to-270-days"))
        aged_2 = _num(r.get("inv-age-271-to-365-days"))
        aged_3 = _num(r.get("inv-age-365-plus-days"))

        aged_alt_1 = _num(r.get("inv-age-181-to-330-days"))
        aged_alt_2 = _num(r.get("inv-age-331-to-365-days"))

        long_term_aged_units = aged_1 + aged_2 + aged_3 + aged_alt_1 + aged_alt_2
        overaged = long_term_aged_units > 0

        estimated_storage_cost = _num(r.get("estimated-storage-cost-next-month"))

        alert_type = "none"
        alert = "No alert"

        if coverage_ratio is not None and coverage_ratio <= 2:
            alert_type = "supply"
            alert = "High alert"

        elif coverage_ratio is not None and coverage_ratio <= 5:
            alert_type = "supply"
            alert = "Please send shipment"

        elif coverage_ratio is not None and coverage_ratio >= 6 and not overaged:
            alert_type = "excess"
            alert = "High inventory coverage ratio"

        elif estimated_storage_cost > 100:
            alert_type = "cost"
            alert = "High storage cost"

        elif overaged:
            alert_type = "overaged"
            alert = "Long-term aged inventory"

        # -------------------------
        # Deterministic sentence
        # -------------------------
        cov_str = round(float(coverage_ratio), 1) if coverage_ratio is not None else None

        inventory_recommendation = "Inventory position is stable."

        if alert == "High alert" and cov_str is not None:
            inventory_recommendation = (
                f"Your coverage ratio is {cov_str} months. Please immediately send stock to avoid stock-out."
            )

        elif alert == "Please send shipment" and cov_str is not None:
            inventory_recommendation = (
                f"Your coverage ratio is {cov_str} months. Please supply inventory soon to avoid stock-out risk."
            )

        elif alert == "High inventory coverage ratio" and cov_str is not None:
            inventory_recommendation = (
                f"Your coverage ratio is {cov_str} months, which may increase storage cost. "
                f"Please improve sell-through to avoid excess storage fees."
            )

        elif alert == "Long-term aged inventory":
            inventory_recommendation = (
                f"{int(long_term_aged_units)} units are ageing long-term. "
                f"Review and liquidate this stock to avoid additional storage cost."
            )

        elif alert == "High storage cost":
            inventory_recommendation = (
                f"Estimated storage cost is {round(estimated_storage_cost,2)}. "
                f"Reduce inventory exposure to control storage expense."
            )

        flags[sku] = {
            "inventory_alert": alert,
            "inventory_alert_type": alert_type,
            "inventory_coverage_ratio": coverage_ratio,
            "long_term_aged_units": int(long_term_aged_units),
            "estimated_storage_cost": round(estimated_storage_cost,2),
            "inventory_recommendation": inventory_recommendation,
        }

    return flags




def generate_live_insight_with_app_context(app, item, country, prev_label, curr_label, user_id, month2):
    with app.app_context():
        return generate_live_insight(
            item,
            country,
            prev_label,
            curr_label,
            user_id,
            month2
        )




def generate_live_insight(item, country, prev_label, curr_label, user_id, month2):

    sku = safe_strip(item.get("sku"), default=None)
    product_name = safe_strip(item.get("product_name"), default="this product")

    key = sku or product_name
    is_new_or_reviving = item.get("new_or_reviving", False)

    recommendation = None
    inventory_recommendation = None
    product_journey = []

    try:
        country_key = safe_strip(country, default="").lower()

        COUNTRY_MAP = {
            "usa": "us",
            "united states": "us",
            "united states of america": "us",
            "gb": "uk",
            "great britain": "uk",
            "united kingdom": "uk",
        }

        country_key = COUNTRY_MAP.get(country_key, country_key)

        print("[LIVE INSIGHT START]", {
            "input_country": country,
            "country_key": country_key,
            "sku": sku,
            "product_name": product_name,
            "key": key,
            "user_id": user_id,
        })

        # -------------------------------------------------
        # Resolve latest month for strategy engine
        # -------------------------------------------------
        if country_key == "global":
            # Global uses US + UK internally.
            # If resolve_latest_available_month supports "global", use it.
            # Otherwise fallback to US latest month.
            try:
                latest_year, latest_month = resolve_latest_available_month(
                    int(user_id),
                    "global"
                )
            except Exception:
                latest_year, latest_month = resolve_latest_available_month(
                    int(user_id),
                    "us"
                )
        else:
            latest_year, latest_month = resolve_latest_available_month(
                int(user_id),
                country_key
            )

        # -------------------------------------------------
        # GLOBAL strategy engine
        # -------------------------------------------------
        if country_key == "global":

            summary_result = get_or_create_global_summary(
                user_id=int(user_id),
                marketplace_id=None,
                period="monthly",
                timeline=str(latest_month),
                year=int(latest_year),
                objective=None,
                target_sku=key,
                force_regenerate=True
            )

            global_ai = summary_result.get("global_ai") or {}
            product_comparisons = global_ai.get("product_journey_comparison") or []

            matched_product = None

            for product in product_comparisons:
                if not isinstance(product, dict):
                    continue

                possible_keys = [
                    product.get("sku"),
                    product.get("product_name"),
                    product.get("mapped_product_name"),
                    product.get("global_product_name"),
                    product.get("name"),
                ]

                possible_keys = [
                    safe_strip(value, default=None)
                    for value in possible_keys
                    if value
                ]

                if key in possible_keys or product_name in possible_keys:
                    matched_product = product
                    break

            if matched_product:
                product_journey = (
                    matched_product.get("journey_summary")
                    or matched_product.get("product_journey")
                    or matched_product.get("comparison_journey")
                    or []
                )

                country_actions = matched_product.get("country_actions") or {}

                us_actions = country_actions.get("us") or {}
                uk_actions = country_actions.get("uk") or {}

                recommendation = {
                    "us": us_actions.get("recommendation"),
                    "uk": uk_actions.get("recommendation"),
                    "global": matched_product.get("recommendation")
                }

                inventory_recommendation = {
                    "us": us_actions.get("inventory_recommendation"),
                    "uk": uk_actions.get("inventory_recommendation"),
                    "global": matched_product.get("inventory_recommendation")
                }

            else:
                recommendation = summary_result.get("overall_recommendation")
                inventory_recommendation = None
                product_journey = []

            print("[LIVE INSIGHT MONTH]", {
            "country_key": country_key,
            "latest_year": latest_year,
            "latest_month": latest_month,
            })        

        # -------------------------------------------------
        # NORMAL country strategy engine
        # -------------------------------------------------
        else:

            summary_result = get_or_create_summary(
                user_id=int(user_id),
                country=country_key,
                marketplace_id=None,
                period="monthly",
                timeline=str(latest_month),
                year=int(latest_year),
                objective=None,
                target_sku=key,
                force_regenerate=True
            )


            print("[COUNTRY SUMMARY RAW]", {
                "country_key": country_key,
                "key": key,
                "summary_type": type(summary_result).__name__,
                "summary_keys": list(summary_result.keys()) if isinstance(summary_result, dict) else None,
            })

            sku_actions = (
                summary_result.get("sku_actions")
                or summary_result.get("ai_insights")
                or {}
            )

            # print("[COUNTRY SKU ACTIONS]", {
            #     "country_key": country_key,
            #     "key": key,
            #     "sku_actions_type": type(sku_actions).__name__,
            #     "sku_action_keys": list(sku_actions.keys())[:20] if isinstance(sku_actions, dict) else None,
            #     "has_exact_key": key in sku_actions if isinstance(sku_actions, dict) else False,
            # })

            sku_block = sku_actions.get(key) or {}

            # Fallback matching because global handles multiple keys,
            # but country was only checking exact key match.
            if not sku_block:
                normalized_key = safe_strip(key, default="").lower()
                normalized_product_name = safe_strip(product_name, default="").lower()

                for action_key, block in sku_actions.items():
                    if not isinstance(block, dict):
                        continue

                    possible_keys = [
                        action_key,
                        block.get("sku"),
                        block.get("product_name"),
                        block.get("mapped_product_name"),
                        block.get("global_product_name"),
                        block.get("name"),
                    ]

                    possible_keys = [
                        safe_strip(value, default="").lower()
                        for value in possible_keys
                        if value
                    ]

                    if normalized_key in possible_keys or normalized_product_name in possible_keys:
                        sku_block = block
                        break

            # print("[COUNTRY SKU MATCH]", {
            #     "country_key": country_key,
            #     "requested_key": key,
            #     "requested_product_name": product_name,
            #     "matched": bool(sku_block),
            #     "matched_sku": sku_block.get("sku") if isinstance(sku_block, dict) else None,
            #     "matched_product_name": sku_block.get("product_name") if isinstance(sku_block, dict) else None,
            #     "sku_block_keys": list(sku_block.keys()) if isinstance(sku_block, dict) else None,
            # })

            recommendation = sku_block.get("recommendation")
            inventory_recommendation = sku_block.get("inventory_recommendation")

            performance_journey = (
                sku_block.get("journey_summary")
                or sku_block.get("product_journey")
                or sku_block.get("comparison_journey")
                or []
            )

            inventory_journey = (
                sku_block.get("inventory_journey_summary")
                or sku_block.get("inventory_journey")
                or []
            )

            product_journey = performance_journey + inventory_journey

    except Exception as e:
        import traceback
        print("[LIVE STRATEGY ENGINE ERROR]", e)
        traceback.print_exc()

    print("[LIVE INSIGHT RESULT]", {
    "country_key": country_key if "country_key" in locals() else None,
    "key": key,
    "recommendation": recommendation,
    "inventory_recommendation": inventory_recommendation,
    "product_journey_count": len(product_journey) if isinstance(product_journey, list) else None,
    "is_new_or_reviving": is_new_or_reviving,
})

    return key, {
        "sku": sku,
        "product_name": product_name,
        "product_journey": product_journey,
        "recommendation": recommendation,
        "inventory_recommendation": inventory_recommendation,
        "key_used": key,
        "is_new_or_reviving": is_new_or_reviving
    }
