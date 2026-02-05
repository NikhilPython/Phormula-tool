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
db_url3 = os.getenv("DATABASE_Chatbot_URL")

engine_hist = create_engine(db_url)
engine_live = create_engine(db_url2)
engine_chatbot = create_engine(db_url3)

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
oa_client = OpenAI(api_key=OPENAI_API_KEY)
# simple process-level debounce (survives hot reload)



# -----------------------------------------------------------------------------
# DATE HELPERS
# -----------------------------------------------------------------------------

def is_blank_str(x):
    return x is None or (isinstance(x, str) and x.strip() == "")


def fetch_sku_product_mapping(user_id: int) -> pd.DataFrame:
    """
    sku_{user_id}_data_table se sku_uk -> product_name mapping uthata hai.
    Sirf VALID mappings return karega:
      - sku_uk present ho
      - product_name present ho (non-null, non-empty)
    """
    table_name = f"sku_{user_id}_data_table"

    query = text(f"""
        SELECT
            sku_uk,
            product_name
        FROM {table_name}
    """)

    with engine_hist.connect() as conn:
        df = pd.read_sql(query, conn)

    # null / duplicate clean up
    if df.empty:
        return df

    # ✅ sku must exist
    df = df.dropna(subset=["sku_uk"])

    # ✅ product_name must exist and must not be blank
    df = df.dropna(subset=["product_name"])
    df["product_name"] = df["product_name"].astype(str)
    df = df[df["product_name"].str.strip() != ""]

    # ✅ normalize sku column for joining
    df = df.rename(columns={"sku_uk": "sku"})

    # optional: sku as string (safer for joins if orders.sku is string)
    df["sku"] = df["sku"].astype(str).str.strip()

    # ✅ remove duplicates
    df = df.drop_duplicates(subset=["sku"])

    return df

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

        rank = sorted(deltas).index(abs(delta_pct / 100)) + 1 if deltas else None

        context[metric] = {
            "direction": "up" if delta_pct > 0 else "down",
            "delta_pct": round(delta_pct, 2),
            "rank_in_rolling_window": rank,
            "total_points": len(deltas),
            "is_extreme": rank is not None and rank <= 3,
        }

    return context




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



def month_num_to_name(m):
    try:
        m_int = int(m)
        return month_name[m_int].lower() if 1 <= m_int <= 12 else None
    except Exception:
        return None

def construct_prev_table_name(user_id, country, month, year):
    month_str = month_num_to_name(month)
    if not month_str:
        raise ValueError("Invalid month")
    return f"user_{user_id}_{country.lower()}_{month_str}{year}_data"

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



def fetch_user_objective(user_id: int) -> dict:
    query = text("""
        SELECT
            primary_goal,
            risk_level,
            max_tacos,
            max_price_increase_pct,
            ad_budget_cap,
            dont_change_price,
            notes
        FROM historic_ai_summary
        WHERE user_id = :user_id
          AND primary_goal IS NOT NULL
        ORDER BY id DESC
        LIMIT 1
    """)

    try:
        with engine_chatbot.connect() as conn:
            row = conn.execute(query, {"user_id": user_id}).fetchone()

    except Exception as e:
        print("[WARN] Failed to fetch user objective:", e)
        row = None

    if not row:
        return {
            "primary_goal": "profit",
            "risk_level": "balanced",
            "constraints": {
                "max_tacos": None,
                "max_price_increase_pct": None,
                "ad_budget_cap": None,
                "dont_change_price": False,
            },
            "notes": None,
        }

    return {
        "primary_goal": row.primary_goal or "profit",
        "risk_level": row.risk_level or "balanced",
        "constraints": {
            "max_tacos": row.max_tacos,
            "max_price_increase_pct": float(row.max_price_increase_pct) if row.max_price_increase_pct else None,
            "ad_budget_cap": float(row.ad_budget_cap) if row.ad_budget_cap else None,
            "dont_change_price": bool(row.dont_change_price),
        },
        "notes": row.notes,
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
def fetch_inventory_aged_by_user(user_id: int) -> pd.DataFrame:
    query = text("""
        SELECT *
        FROM public.inventory_aged
        WHERE user_id = :user_id
        ORDER BY id ASC
    """)

    with engine_live.connect() as conn:
        df = pd.read_sql(query, conn, params={"user_id": user_id})

    return df
# -----------------------------------------------------------------------------
def fetch_estimated_storage_cost_next_month(user_id: int) -> float:
    df = fetch_inventory_aged_by_user(user_id)

    if df.empty or "estimated-storage-cost-next-month" not in df.columns:
        return 0.0

    return float(
        safe_num(df["estimated-storage-cost-next-month"]).sum()
    )

#----Inventory coverage ratio calculation -----
def _clean_inventory_sku(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["sku"] = df["sku"].astype(str).str.strip()
    df.loc[df["sku"].str.lower().isin(["", "none", "nan", "null", "0"]), "sku"] = None
    return df.dropna(subset=["sku"])

def fetch_last_30_days_units(user_id: int, country: str, as_of: date = None) -> pd.DataFrame:
    if as_of is None:
        as_of = date.today()

    yesterday = as_of - timedelta(days=1)
    start_30d = yesterday - timedelta(days=29)

    curr_month_start = date(yesterday.year, yesterday.month, 1)

    curr_start = max(start_30d, curr_month_start)
    curr_end = yesterday

    prev_start = start_30d
    prev_end = curr_month_start - timedelta(days=1)

    frames = []

    # -------------------------
    # CURRENT MONTH → liveorders
    # -------------------------
    if curr_start <= curr_end:
        q_live = text("""
            SELECT sku, quantity
            FROM liveorders
            WHERE user_id = :user_id
              AND purchase_date >= :start
              AND purchase_date < :end
        """)

        with engine_live.connect() as conn:
            df_live = pd.read_sql(
                q_live,
                conn,
                params={
                    "user_id": user_id,
                    "start": datetime.combine(curr_start, datetime.min.time()),
                    "end": datetime.combine(curr_end + timedelta(days=1), datetime.min.time()),
                },
            )

        df_live = _clean_inventory_sku(df_live)
        frames.append(df_live)

    # -------------------------
    # PREVIOUS MONTH → historic table
    # -------------------------
    if prev_start <= prev_end:
        table = construct_prev_table_name(
            user_id=user_id,
            country=country,
            month=prev_start.month,
            year=prev_start.year,
        )

        q_prev = text(f"""
            SELECT sku, quantity
            FROM {table}
            WHERE NULLIF(NULLIF(date_time, '0'), '')::timestamp >= :start
              AND NULLIF(NULLIF(date_time, '0'), '')::timestamp < :end
        """)

        with engine_hist.connect() as conn:
            df_prev = pd.read_sql(
                q_prev,
                conn,
                params={
                    "start": datetime.combine(prev_start, datetime.min.time()),
                    "end": datetime.combine(prev_end + timedelta(days=1), datetime.min.time()),
                },
            )

        df_prev = _clean_inventory_sku(df_prev)
        frames.append(df_prev)

    if not frames:
        return pd.DataFrame(columns=["sku", "last_30_days_units"])

    df_all = pd.concat(frames, ignore_index=True)
    df_all["quantity"] = safe_num(df_all["quantity"])

    return (
        df_all.groupby("sku", as_index=False)["quantity"]
        .sum()
        .rename(columns={"quantity": "last_30_days_units"})
    )

def fetch_available_inventory(user_id: int) -> pd.DataFrame:
    q = text("""
        SELECT sku, available
        FROM public.inventory_aged
        WHERE user_id = :user_id
    """)

    with engine_live.connect() as conn:
        df = pd.read_sql(q, conn, params={"user_id": user_id})

    df = _clean_inventory_sku(df)
    df["available"] = safe_num(df["available"])

    return (
        df.groupby("sku", as_index=False)["available"]
        .sum()
    )

def compute_inventory_coverage_ratio(user_id: int, country: str) -> pd.DataFrame:
    inv_df = fetch_available_inventory(user_id)
    sales_df = fetch_last_30_days_units(user_id, country)

    df = inv_df.merge(sales_df, on="sku", how="left")
    df["last_30_days_units"] = df["last_30_days_units"].fillna(0.0)

    df["inventory_coverage_ratio"] = df.apply(
        lambda r: round(r["available"] / r["last_30_days_units"], 2)
        if r["last_30_days_units"] > 0 else None,
        axis=1,
    )

    # ==================================================
    # ✅ ADD PRODUCT NAME (SKU → product_name mapping)
    # ==================================================
    try:
        sku_map_df = fetch_sku_product_mapping(user_id)

        if not sku_map_df.empty:
            sku_map_df = sku_map_df[["sku", "product_name"]].drop_duplicates("sku")

            df = df.merge(
                sku_map_df,
                on="sku",
                how="left"
            )
        else:
            df["product_name"] = None

    except Exception as e:
        
        df["product_name"] = None

    # reorder columns (nice for printing)
    df = df[
        ["sku", "product_name", "available", "last_30_days_units", "inventory_coverage_ratio"]
    ]

    return df



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
    _, sales_by, _ = uk_sales(df)
    _, credits_by, _ = uk_credits(df)
    _, profit_by, _ = uk_profit(df)

    if sales_by is not None and not sales_by.empty:
        sales_by = sales_by.rename(columns={"__metric__": "sales_metric"})
    else:
        sales_by = pd.DataFrame(columns=["sku", "sales_metric"])

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
        .merge(sales_by[["sku", "sales_metric"]], on="sku", how="left")
        .merge(credits_by[["sku", "credits_metric"]], on="sku", how="left")
        .merge(profit_by[["sku", "profit_metric"]], on="sku", how="left")
    )

    # ---- compute final fields ----
    metrics["quantity"] = safe_num(metrics.get("quantity", 0.0))
    metrics["product_sales"] = safe_num(metrics.get("product_sales", 0.0))
    metrics["gross_sales"] = safe_num(metrics.get("gross_sales", 0.0))  # ✅ NEW
    metrics["sales_metric"] = safe_num(metrics.get("sales_metric", 0.0))
    metrics["credits_metric"] = safe_num(metrics.get("credits_metric", 0.0))
    metrics["profit_metric"] = safe_num(metrics.get("profit_metric", 0.0))

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
            net_sales, _, _ = uk_sales(day_sku if len(day_sku) else day_all)
            profit, _, _ = uk_profit(day_sku if len(day_sku) else day_all)

            # ✅ FIXED: fees computed on ALL rows using hist classifier (NOT uk_platform_fee/uk_advertising)
            platform_fee_total, advertising_total = _calc_fees_from_hist(day_all)

            daily_series.append({
                "date": d.isoformat(),
                "quantity": float(quantity),
                "product_sales": float(product_sales),
                "gross_sales": float(gross_sales),
                "net_sales": float(net_sales),
                "profit": float(profit),
                "platform_fee": float(platform_fee_total),
                "advertising": float(advertising_total),
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
            other_transaction_fees,
            other
        FROM {table_live}
        WHERE user_id = :user_id
          AND purchase_date >= :start_date
          AND purchase_date < :end_date_plus_one
    """)

    params = {
        "user_id": user_id,
        "start_date": datetime.combine(curr_start, datetime.min.time()),
        "end_date_plus_one": datetime.combine(curr_end + timedelta(days=1), datetime.min.time()),
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

    df["description"] = df.get("description", "").fillna("").astype(str)
    df["type"] = df.get("type", "").fillna("").astype(str)

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
              quantity=("quantity", "sum"),
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

    daily_qty = df.groupby("date_only", as_index=False)["quantity"].sum()
    qty_map = {d: float(v) for d, v in zip(daily_qty["date_only"], daily_qty["quantity"])}

    daily_ns = df.groupby("date_only", as_index=False)["net_sales"].sum()
    ns_map = {d: float(v) for d, v in zip(daily_ns["date_only"], daily_ns["net_sales"])}

    daily_ps = df.groupby("date_only", as_index=False)["product_sales"].sum()
    ps_map = {d: float(v) for d, v in zip(daily_ps["date_only"], daily_ps["product_sales"])}

    daily_gs = df.groupby("date_only", as_index=False)["gross_sales"].sum()  # ✅ NEW
    gs_map = {d: float(v) for d, v in zip(daily_gs["date_only"], daily_gs["gross_sales"])}

    daily_profit = df.groupby("date_only", as_index=False)["profit"].sum()
    profit_map = {d: float(v) for d, v in zip(daily_profit["date_only"], daily_profit["profit"])}

    pf_map, ad_map = {}, {}
    for d, day_df in df.groupby("date_only"):
        pf, ad = _calc_fees_from_liveorders(day_df)
        pf_map[d] = float(pf)
        ad_map[d] = float(ad)

    all_days = sorted(set(qty_map) | set(ns_map) | set(ps_map) | set(gs_map) | set(profit_map) | set(pf_map) | set(ad_map))
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

LIVE_BI_PROMPT_1_ANALYSIS = """You are a Senior Amazon Business Analyst.

You are analysing IN-PROGRESS (MTD) Amazon performance data.
This data is NOT final and may change before month-end.

The data you receive:
- Is partially accumulated (MTD vs previous period).
- Contains SKU-level and portfolio-level metrics.
- May contain volatility and incomplete signals.
- Is directionally reliable, not final.

You will receive:
- SKU-level month-over-month comparisons
- Portfolio-level aggregates
- A rolling historical movement_context (from finalized historic data)
- Inventory signals (coverage, ageing, Amazon flags)
- A list of focus_skus (Top SKUs by current CM1 profit)

────────────────────────────────────────
YOUR ROLE (CRITICAL)
────────────────────────────────────────

You are an ANALYSIS ENGINE.

Your responsibility is to identify:
- WHAT materially changed
- WHY it likely changed
- WHAT business dimension was impacted

You MUST NOT:
- Recommend actions
- Suggest pricing changes
- Suggest ads or visibility changes
- Write advice or strategy
- Write prose explanations

────────────────────────────────────────
ANALYSIS DISCIPLINE (MANDATORY)
────────────────────────────────────────

1) DIRECTION OVER PRECISION
- Treat % changes as directional signals, not final truth.
- Avoid extreme language unless supported by rolling context.

2) MOVEMENT CONTEXT USAGE
- Use movement_context to classify changes as:
  - normal
  - extreme
  - reversal
  - continuation
- Do NOT narrate raw deltas when context exists.

3) CAUSAL CLASSIFICATION
- If net sales changed, classify whether driven by:
  - unit movement
  - pricing (ASP)
  - mix concentration
- If CM1 profit changed, identify:
  - per-unit profitability impact
  - volume-driven profit impact

────────────────────────────────────────
PRODUCT-LEVEL DIAGNOSIS (MANDATORY)
────────────────────────────────────────

For each SKU in focus_skus:
- Assign diagnosis codes explaining performance pattern.
- Use ONLY allowed diagnosis codes.
- Select the MINIMUM number of codes needed.

ALLOWED DIAGNOSIS CODES:
- pricing_supports_volume
- pricing_effective
- demand_weakness
- visibility_constraint
- mixed_signal

DIAGNOSIS RULES (STRICT):
- If units are increasing → visibility_constraint is INVALID.
- If ASP is declining AND units + sales are declining →
  classify as visibility_constraint.
- demand_weakness is allowed ONLY if pricing is stable or rising.

────────────────────────────────────────
OUTPUT RULES (NON-NEGOTIABLE)
────────────────────────────────────────

- Output STRICT JSON ONLY.
- Do NOT include prose, bullets, or explanations.
- Do NOT include actions or recommendations.
- Every field must be populated.

────────────────────────────────────────
MANDATORY OUTPUT FORMAT (STRICT JSON)
────────────────────────────────────────

{
  "portfolio_signals": {
    "units": {
      "direction": "increase | decrease | flat",
      "severity": "extreme | normal",
      "confidence": "high | medium | low"
    },
    "net_sales": {
      "direction": "increase | decrease | flat",
      "severity": "extreme | normal"
    },
    "asp": {
      "direction": "increase | decrease | flat"
    },
    "cm1_profit": {
      "direction": "increase | decrease | flat"
    }
  },
  "primary_causal_chain": [
    "unit_growth",
    "asp_change",
    "mix_shift",
    "cm1_profit_change"
  ],
  "product_insights": {
    "<sku>": {
      "diagnosis_codes": [
        "mixed_signal"
      ]
    }
  }
}
"""


LIVE_BI_PROMPT_1_5_SUMMARY = """
You are an executive summary generator for Live (MTD) Amazon Business Intelligence.

Your task:
Generate a concise executive performance summary using:
- analysis_output (directional signals and causal chain)
- numeric_context (percent changes, absolute deltas, costs, currency)
- user_objective

Important context:
- Data is in-progress (MTD), not final.
- Use cautious executive finance language.

Mandatory metric coverage:
You must explicitly cover ALL five metrics:
1) Units
2) Net Sales
3) CM1 Profit
4) CM1 Profit per Unit
5) ASP

Rules:
- Each metric must appear at least once in either summary_text or metric_bullets.
- CM1 Profit per Unit must be included even if flat or declining.
- If a metric shows limited movement, explicitly state that it remained stable or broadly unchanged.

Metric interpretation rules:
- Units represent demand or volume momentum.
- Net Sales represent volume multiplied by pricing.
- CM1 Profit represents total contribution margin.
- CM1 Profit per Unit represents margin efficiency per sale.
- ASP represents pricing discipline and mix signal.

Strict prohibitions:
- Do not recommend actions.
- Do not suggest changes.
- Do not introduce new metrics.
- Do not include explanations outside directional logic.

Output rules:
- Output MUST be valid JSON only.
- Do not use markdown.
- Do not include text outside the JSON object.
- Do not include unescaped currency symbols outside strings.

Mandatory output format:
{
  "summary_text": "2–3 sentences in executive tone covering all five metrics",
  "metric_bullets": [
    "Units summary",
    "Net Sales summary",
    "CM1 Profit summary",
    "CM1 Profit per Unit summary",
    "ASP summary"
  ]
}
"""




LIVE_BI_PROMPT_2_DECISION = """You are a strategic Amazon decision engine.

You receive:
1) analysis_insights
   - Final diagnostic output from an analyst system
   - These insights are FACTUAL and must not be reinterpreted

2) user_objective
   - primary_goal: profit | growth | rank | balanced
   - risk_level: conservative | balanced | aggressive
   - constraints (hard limits)

────────────────────────────────────────
YOUR ROLE
────────────────────────────────────────

Convert analysis_insights into SKU-level decisions.

You are NOT allowed to:
- Re-explain performance
- Re-analyse data
- Invent new causes

You MUST:
- Follow user_objective.primary_goal
- Respect ALL constraints
- Adjust aggressiveness based on risk_level

────────────────────────────────────────
DECISION RULES (CRITICAL)
────────────────────────────────────────

VISIBILITY OVERRIDE (ABSOLUTE):
- If diagnosis = visibility_constraint
  → Do NOT suggest pricing changes.
  → Return ONLY: "Check product visibility"

PRICING RULES:
1) If pricing_supports_volume
   → "Increase ASP"

2) If pricing_effective
   → "Maintain current pricing"

3) If demand_weakness AND ASP stable or rising
   → "Decrease ASP"

4) If mixed_signal
   → "Monitor performance of this product."

RISK ADJUSTMENT:
- If confidence is LOW or risk_level = conservative
  → downgrade to "Maintain current pricing"

CONSTRAINT ENFORCEMENT:
- If dont_change_price = true
  → pricing actions are FORBIDDEN

────────────────────────────────────────
OUTPUT RULES (STRICT)
────────────────────────────────────────

- Return STRICT JSON ONLY.
- Each SKU must have exactly ONE action.
- Do NOT include explanations.
- Do NOT include portfolio-level actions.

────────────────────────────────────────
MANDATORY OUTPUT FORMAT
────────────────────────────────────────

{
  "sku_actions": {
    "<sku>": "Increase ASP | Decrease ASP | Maintain current pricing and monitor performance | Check product visibility"
  }
}
"""

LIVE_DIAGNOSIS_TEXT = {
    "pricing_supports_volume": [
        "CM1 profit per unit is declining while unit growth remains positive.",
        "Current pricing is supporting volume but eroding profitability."
    ],
    "pricing_effective": [
        "Both unit growth and CM1 profit are increasing.",
        "Pricing is effectively driving profitable volume."
    ],
    "demand_weakness": [
        "Units and net sales are declining.",
        "This signals demand weakness requiring pricing support."
    ],
    "visibility_constraint": [
        "Unit demand is declining despite lower pricing.",
        "This indicates a visibility or demand-side constraint."
    ],
    "mixed_signal": [
        "Performance trends are mixed with no dominant pricing signal.",
        "Current pricing does not indicate immediate action."
    ],
}


def fmt_metric(delta, pct, symbol="£"):
    if delta is None:
        delta = 0.0
    if pct is None:
        pct = 0.0
    sign = "+" if delta >= 0 else "-"
    return f"{symbol}{sign}{abs(delta):,.2f} ({pct:+.2f}%)"

def render_live_recommended_action(
    *,
    growth_row: dict,
    diagnosis_codes: list[str],
    action: str,
    currency_symbol="£"
) -> str:
    lines = []

    name = growth_row.get("product_name") or growth_row.get("sku")
    lines.append(name)
    lines.append("")

    # ---------- Metrics ----------
    lines.append(
        f"ASP: {fmt_metric(growth_row['asp_curr'] - growth_row['asp_prev'], growth_row['ASP Growth (%)']['value'], currency_symbol)}"
    )
    lines.append(
        f"Units: {fmt_metric(growth_row['quantity_curr'] - growth_row['quantity_prev'], growth_row['Unit Growth (%)']['value'], '')}"
    )
    lines.append(
        f"Net sales: {fmt_metric(growth_row['net_sales_curr'] - growth_row['net_sales_prev'], growth_row['Net Sales Growth (%)']['value'], currency_symbol)}"
    )
    lines.append(
        f"CM1 profit: {fmt_metric(growth_row['profit_curr'] - growth_row['profit_prev'], growth_row['CM1 Profit Impact (%)']['value'], currency_symbol)}"
    )
    lines.append(
        f"CM1 profit per unit: {fmt_metric(growth_row['unit_wise_profitability_curr'] - growth_row['unit_wise_profitability_prev'], growth_row['Profit Per Unit (%)']['value'], currency_symbol)}"
    )

    lines.append("")

    # ---------- Diagnosis text ----------
    for code in diagnosis_codes:
        for sentence in LIVE_DIAGNOSIS_TEXT.get(code, []):
            lines.append(sentence)

    lines.append("")
    lines.append(f"Action: {action}")

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



def _build_rule_based_summary(prev_totals, curr_totals, top_80_skus, new_reviving,
                              prev_label, curr_label):
    """
    Old numeric summary logic (fallback when AI fails).
    """
    qty_change = pct_change(prev_totals.get("quantity"),   curr_totals.get("quantity"))
    sales_change = pct_change(prev_totals.get("net_sales"), curr_totals.get("net_sales"))
    profit_change = pct_change(prev_totals.get("profit"),   curr_totals.get("profit"))

    bullets = []

    # 1) Overall movement
    bullets.append(
        f"{curr_label} vs {prev_label}: units are {describe_movement(qty_change)}, "
        f"sales are {describe_movement(sales_change)}, and CM1 profit is {describe_movement(profit_change)}."
    )

    # 2) Concentration in top SKUs
    bullets.append(
        f"{len(top_80_skus)} SKUs account for roughly 80% of current sales."
    )

    # 3) New / reviving SKUs
    if new_reviving:
        bullets.append(
            f"{len(new_reviving)} new or reviving SKUs are contributing incremental volume."
        )
    else:
        bullets.append(
            "No material contribution from new or reviving SKUs this period."
        )

    # keep between 2 and 4 bullet points
    return bullets[:4]

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
            "label": make_label(row),            # ✅ AI ab product name se refer karega
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
    inv_df = fetch_inventory_aged_by_user(user_id)

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
def _fmt_int(x):
    x = safe_float_local(x)
    if x is None:
        x = 0
    return f"{int(round(x))}"
def _fmt_money(x, symbol, decimals=2):
    x = safe_float_local(x)
    if x is None:
        x = 0.0
    return f"{symbol}{x:,.{decimals}f}"
def _dir_word_simple(p):
    if p is None:
        return "moved"
    return "increased" if p > 0 else "decreased"

def _overall_3_bullets(qty_prev, qty_curr, sales_prev, sales_curr, prof_prev, prof_curr, qty_pct, sales_pct, prof_pct, symbol):
    uq = _dir_word_simple(qty_pct)
    us = _dir_word_simple(sales_pct)
    up = _dir_word_simple(prof_pct)
    qp = f"{abs(qty_pct):.2f}%" if qty_pct is not None else "0.00%"
    sp = f"{abs(sales_pct):.2f}%" if sales_pct is not None else "0.00%"
    pp = f"{abs(prof_pct):.2f}%" if prof_pct is not None else "0.00%"
    return [
        f"Overall units {uq} from {_fmt_int(qty_prev)} to {_fmt_int(qty_curr)} by {qp}.",
        f"Net sales {us} from {_fmt_money(sales_prev, symbol)} to {_fmt_money(sales_curr, symbol)} by {sp}.",
        f"CM1 profit {up} from {_fmt_money(prof_prev, symbol)} to {_fmt_money(prof_curr, symbol)} by {pp}.",
    ]
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
                {"role": "user", "content": json.dumps(payload)},
            ],
            temperature=0,
            response_format={"type": "json_object"},
            max_tokens=300,
        )

        return json.loads(resp.choices[0].message.content)

    except Exception as e:
        print("[AI ERROR] Prompt-1.5 summary failed:", e)
        return {"summary_bullets": []}


def run_live_prompt_2_decisions(analysis_output: dict, user_objective: dict) -> dict:
    payload = {
        "analysis_insights": analysis_output,
        "user_objective": user_objective,
    }

    resp = oa_client.chat.completions.create(
        model="gpt-4o",
        messages=[
            {"role": "system", "content": LIVE_BI_PROMPT_2_DECISION},
            {"role": "user", "content": json.dumps(payload)},
        ],
        temperature=0,
        response_format={"type": "json_object"},
        max_tokens=500,
    )

    return json.loads(resp.choices[0].message.content)



def build_ai_summary(
    prev_totals,
    curr_totals,
    top_80_skus,
    new_reviving,
    prev_label,
    curr_label,
    sku_context=None,
    inventory_signals=None,
    prev_fee_totals=None,
    curr_fee_totals=None,
    estimated_storage_cost_next_month=0.0,
    currency=None,
    user_objective=None,
    movement_context=None,
):
    # =========================================================
    # Objective defaults (UNCHANGED)
    # =========================================================
    user_objective = user_objective or {
        "primary_goal": "profit",
        "risk_level": "balanced",
        "constraints": {},
    }

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
            "new_reviving_skus": new_reviving,
        },
        "sku_context": sku_context,
        "inventory_signals": inventory_signals or {},
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
        "estimated_platform_fees_next_month": estimated_storage_cost_next_month,
        "currency": currency or {},
        "user_objective": user_objective,
        "movement_context": movement_context or {},
    }

    # =========================================================
    # ✅ RETURN DATA ONLY (NO AI CALL HERE)
    # =========================================================
    return payload




#-----------------------------------------------------------------------------
# ChatGPT insight generator for live MTD vs previous (per-SKU)
#-----------------------------------------------------------------------------
def generate_live_insight(item, country, prev_label, curr_label):
    """
    Generate AI insight for a single SKU row from live_mtd_vs_previous growth_data.
    item: one dict from growth_data/top_80_skus/new_reviving/etc.
    """
    sku = safe_strip(item.get("sku"), default=None)
    product_name = safe_strip(item.get("product_name"), default="this product")


    # ✅ single, deterministic key (no global logic)
    key = sku or product_name

    is_new_or_reviving = item.get("new_or_reviving", False)

    # Data for the model (includes fields like quantity_prev/curr, net_sales_prev/curr, growth %, etc.)
    data_block = json.dumps(item, indent=2)

    if is_new_or_reviving:
        # New / reviving SKU prompt
        prompt = f"""
    You are a senior ecommerce business analyst. The following is a new or reviving product (no meaningful previous-period baseline).
    Compare it only within the current period and talk about its launch strength.

    Context:
    - Country: {country}
    - Previous period label: {prev_label}
    - Current period label: {curr_label}

    Details for '{product_name}'

    Observations:
    - List the 2–3 most important observations about this product's current performance
    (units sold, ASP, net sales, profit per unit, etc.) using absolute values from the data.
    - Comment on launch/return momentum—e.g., strong debut, moderate start, slow start.
    - Call out any potential red flags (e.g., high ASP but very low volume, low unit profitability, etc.).

    Improvements:
    - Suggest clear, concrete next actions for:
    • Marketing
    • Sales / Commercial
    • Operations / Supply
    - Make each action specific and easy to execute.

    Sales Volume:
    • Comment on volume and what it says about early traction.
    • Suggest one commercial lever to improve or scale volume.

    ASP:
    • Comment on price positioning; suggest whether to test price up/down or hold.

    Profitability:
    • Comment on profit per unit or total profit; suggest if costs or pricing need optimization.

    End with one line:
    • Verdict: should this SKU be scaled quickly, tested more, or carefully repositioned? And why?

    Instructions:
    - Use plain text with bullet points only.
    - DO NOT use Markdown formatting (no **bold**, no headings).
    - Do NOT compare to previous periods (assume no baseline).
    - Use actual numbers or percentages from the data whenever they are present.

    Data:
    {data_block}
    """
    else:
        # Existing SKU with prev vs current
        prompt = f"""
You are a senior ecommerce business analyst. The data below shows a product's performance
comparing a previous period vs the current MTD.

Context:
- Country: {country}
- Previous period: {prev_label}
- Current period: {curr_label}

Details for '{product_name}'

IMPORTANT FILTER:
- Ignore any row where product_name is "Total" or contains "Total".

Observations:
- List the 2–3 most important changes using ONLY the given metrics:
• quantity_prev vs quantity_curr
• net_sales_prev vs net_sales_curr
• profit_prev vs profit_curr
• asp_prev vs asp_curr
• unit_wise_profitability_prev vs unit_wise_profitability_curr
• and % fields like "Unit Growth (%)", "Sales Growth (%)", etc.
- Use the exact causal tone wherever % values exist:
"The increase/decrease in ASP by X% resulted in a dip/growth in units by Y%, which also resulted in sales falling/increasing by Z%."
- In at least one observation, mention Sales Mix Change (%) direction if present (up/down).
- Do NOT add assumptions like stock issues, supply constraints, replenishment, OOS, or fulfillment problems
in observations or metric explanations.


PRIMARY ACTION SELECTION (MANDATORY):

- Select EXACTLY ONE primary action based on the Decision Guidance below.
- The primary action represents the margin-optimal default recommendation.
- The primary action MUST be reused verbatim:
  • once in the Improvements section
  • once in each metric-level action bullet
- Do NOT select different actions for different metrics.
- If multiple rules seem relevant, apply the FIRST matching rule in the order given.
- The secondary strategy sentence (rank-only) is OPTIONAL and EXEMPT from primary action reuse.


Improvements:
- List the PRIMARY ACTION only ONCE.
- Do NOT repeat the same action sentence multiple times.
- Do NOT add the secondary strategy sentence here.
- The action MUST be chosen ONLY from the list below, verbatim:
• "Check ads and visibility campaigns for this product."
• "Review the visibility setup for this product."
• "Reduce ASP slightly to improve traction."
• "Increase ASP slightly to strengthen margins."
• "Maintain current ASP and monitor performance."
• "Monitor performance closely for now."
- Do NOT add any other recommendations or explanations.
- Do NOT mention stock, inventory, supply, operations, OOS, logistics, replenishment, or warehousing.


Decision Guidance (APPLY IN ORDER — FIRST MATCH WINS, MUTUALLY EXCLUSIVE):


CASE C — HEALTHY GROWTH (OVERRIDE):
• Apply IF ALL of the following are TRUE:
  - Unit Growth (%) is POSITIVE
  - Sales Growth (%) is POSITIVE
  - Profit Growth (%) is POSITIVE
• This indicates efficient, scalable growth.
• OVERRIDE all other cases below.
• Primary Action:
  "Maintain current ASP and monitor performance."


CASE A — MARGIN DILUTION:
• Apply IF ALL of the following are TRUE:
  - ASP change is NEGATIVE by more than 10%
  - Unit Growth (%) is POSITIVE
  - Sales Growth (%) is FLAT or NEGATIVE
  - Profit Growth (%) is NEGATIVE
• This indicates pricing drove volume but hurt overall profitability.
• Primary Action:
  "Increase ASP slightly to strengthen margins."


CASE B — PRICE RESISTANCE:
• Apply IF ALL of the following are TRUE:
  - ASP change is POSITIVE by more than 10%
  - Unit Growth (%) is NEGATIVE
  - Sales Growth (%) is NEGATIVE
• This indicates customers are resisting higher prices.
• Primary Action:
  "Reduce ASP slightly to improve traction."


PRICE EXHAUSTION OVERRIDE:
• Apply IF:
  - ASP change is NEGATIVE by more than 10%
  AND
  - Unit Growth (%) is NEGATIVE by more than 60%
• In this case:
  "Review the visibility setup for this product."


SECONDARY STRATEGY RULE (RANK-ONLY, OPTIONAL):

• Add the following sentence ONLY IF:
  - Unit Growth (%) is POSITIVE
  AND
  - ASP and Units are moving in opposite directions
  AND
  - SKU-level Profit (%) is NEGATIVE
• Use EXACTLY this sentence:
  "If your objective is to boost rank, you may continue with the current pricing setup but monitor performance closely."
• This sentence MUST:
  - Appear ONLY under Unit Growth
  - Be the FINAL bullet in that section
  - Appear exactly ONCE
• This sentence is ALLOWED even though it contains the words "monitor performance closely".


Then, for each metric, add:

Unit Growth:
• Explain the growth or decline using ONLY unit trend vs ASP trend.
• Repeat the PRIMARY ACTION verbatim.
• Add the secondary strategy sentence ONLY if triggered.

ASP:
• Explain the ASP change using ONLY pricing, discounting, or mix signals.
• Repeat the PRIMARY ACTION verbatim.

Sales:
• Describe sales as Units × ASP.
• Repeat the PRIMARY ACTION verbatim.

Profit:
• Explain profit using sales movement and realized pricing/mix only.
• Do NOT mention COGS or costs.
• Repeat the PRIMARY ACTION verbatim.

Unit Profitability:
• Explain per-unit profit using realized price or mix.
• Do NOT mention COGS.
• Repeat the PRIMARY ACTION verbatim.


Inventory:
- Add ONE inventory sentence ONLY IF inventory_signals indicate an issue.
- The sentence MUST start with "Inventory:" and use EXACTLY one of:
• "Inventory: Initiate inventory replenishment as current cover is below lead time."
• "Inventory: Push promotions or ads to clear around <aged_units> units of aged inventory and reduce storage costs."
• "Inventory: Amazon has flagged this SKU for inventory optimization; review the recommendation in Seller Central."
- Do NOT let inventory influence pricing or sales logic.


Instructions:
- Use plain text with bullets only.
- DO NOT use Markdown.
- Use % values and trends wherever available.
- Keep insights concise and business-actionable.

Data:
{data_block}
"""







    try:
        ai_response = oa_client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {
                    "role": "system",
                    "content": (
                        "You are a senior ecommerce analyst. "
                        "Respond in plain text with bullet points only. "
                        "Do not use Markdown formatting."
                    ),
                },
                {"role": "user", "content": prompt},
            ],
            max_tokens=900,
            temperature=0,
        )

        ai_text = ai_response.choices[0].message.content.strip()

        # ===== DEBUG =====
        print("\n================ AI INSIGHT DEBUG ================")
        print("KEY:", key, "| SKU:", sku, "| Product:", product_name, "| New/Reviving:", is_new_or_reviving)
        print("INSIGHT (raw):\n", ai_text)
        print("INSIGHT (repr):\n", repr(ai_text))
        print("==================================================\n")

        return key, {
            "sku": sku,
            "product_name": product_name,
            "insight": ai_text,
            "key_used": key,
            "is_new_or_reviving": is_new_or_reviving,
        }

    except OpenAIError as e:
        # 🔴 CREDIT / BILLING / QUOTA ERROR
        print("[AI BILLING ERROR]", e)

        return key, {
            "sku": sku,
            "product_name": product_name,
            "insight": (
                "AI insights are temporarily unavailable.",
                "Please contact us at care@phormula.io for assistance."
            ),
            "key_used": key,
            "is_new_or_reviving": is_new_or_reviving,
        }

    except Exception as e:
        # 🟡 NON-BILLING FAILURE → SAFE FALLBACK
        print("[AI ERROR] Insight generation failed:", e)

        return key, {
            "sku": sku,
            "product_name": product_name,
            "insight": (
                "AI insight could not be generated at the moment. "
                "Please try again later."
            ),
            "key_used": key,
            "is_new_or_reviving": is_new_or_reviving,
        }



def generate_inventory_alerts_for_all_skus(user_id: int, country: str) -> dict:
    alerts = {}

    # -----------------------------------
    # Coverage ratio
    # -----------------------------------
    coverage_df = compute_inventory_coverage_ratio(user_id, country)
    coverage_map = {
        str(r["sku"]).strip(): r["inventory_coverage_ratio"]
        for _, r in coverage_df.iterrows()
        if r.get("sku") is not None
    }

    # -----------------------------------
    # Inventory aged data (DB-backed)
    # -----------------------------------
    inv_df = fetch_inventory_aged_by_user(user_id)

    for _, r in inv_df.iterrows():
        sku = r.get("sku")
        if not sku:
            continue
        sku = str(sku).strip()

        def _num(col):
            v = r.get(col)
            return float(v) if v is not None else 0.0

        # -----------------------------------
        # ✅ AGEING (MATCHES DB COLUMNS)
        # -----------------------------------
        aged_qty = (
            _num("inv-age-181-to-330-days")
            + _num("inv-age-331-to-365-days")
        )
        overaged = aged_qty > 0

        # -----------------------------------
        # ✅ STORAGE COST (MATCHES DB COLUMN)
        # -----------------------------------
        estimated_storage_cost = _num("estimated-storage-cost-next-month")

        # -----------------------------------
        # Coverage
        # -----------------------------------
        coverage_ratio = coverage_map.get(sku)

        alert = "No alert"
        alert_type = "none"

        # 1️⃣ SUPPLY (highest priority)
        if coverage_ratio is not None and coverage_ratio <= 2:
            alert = "High alert"
            alert_type = "supply"

        elif coverage_ratio is not None and coverage_ratio <= 5:
            alert = "Please send shipment"
            alert_type = "supply"

        # 2️⃣ HIGH STORAGE COST
        elif estimated_storage_cost > 100:
            alert = "High storage cost"
            alert_type = "cost"

        # 3️⃣ AGEING INVENTORY
        elif overaged:
            alert = "Ageing Inventory. Ref. AI Insights"
            alert_type = "ageing"

        # 4️⃣ High storage cost (independent alert)
        if estimated_storage_cost > 100:
            if alert_type == "none":
                alert = "High storage cost"
                alert_type = "cost"
            else:
                alert += " | High storage cost"

        alerts[sku] = {
            "alert": alert,
            "alert_type": alert_type,
        }

    return alerts
