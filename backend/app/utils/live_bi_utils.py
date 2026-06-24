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

AI_REFRESH_DAYS = {1, 8, 15, 22, 29}


def get_ai_refresh_slot(as_of_date: date) -> date:
    """
    AI insights/recommendations refresh only on:
    1st, 8th, 15th, 22nd, 29th.

    On other days, reuse the latest previous AI refresh slot
    from the same month.
    """
    if isinstance(as_of_date, datetime):
        as_of_date = as_of_date.date()

    allowed_day = max(d for d in AI_REFRESH_DAYS if d <= as_of_date.day)
    return date(as_of_date.year, as_of_date.month, allowed_day)

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

def _safe_ads_float(value, default=0.0):
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


def _normalise_ads_country(country: str) -> str:
    country = str(country or "uk").strip().lower()

    aliases = {
        "united kingdom": "uk",
        "gb": "uk",
        "great britain": "uk",
        "usa": "us",
        "united states": "us",
        "united states of america": "us",
    }

    return aliases.get(country, country)


def fetch_adsdaily_sku_mtd_ads(
    user_id: int,
    country: str,
    start_date: date,
    end_date: date,
) -> tuple[dict, dict]:
    """
    Fetch MTD ads from:
      adsdaily_{user_id}_{country}_{month}_{year}

    Example if current date is 24 June:
      current:  adsdaily_1_uk_6_2026, date 1 Jun to 24 Jun
      previous: adsdaily_1_uk_5_2026, date 1 May to 24 May

    Returns:
      sku_map[sku] = product-wise ads metrics
      totals = portfolio ads totals
    """

    country_key = _normalise_ads_country(country)
    table_name = f"adsdaily_{int(user_id)}_{country_key}_{int(start_date.month)}_{int(start_date.year)}"

    empty_totals = {
        "ads_spend": 0.0,
        "ads_sales": 0.0,
        "clicks": 0.0,
        "impressions": 0.0,
        "sale_units": 0.0,
        "roas": 0.0,
        "acos": 0.0,
        "ctr": 0.0,
        "cpc": 0.0,
        "conversion_rate": 0.0,
        "source_table": table_name,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
    }

    try:
        with engine_hist.connect() as conn:
            exists = conn.execute(
                text("SELECT to_regclass(:table_name)"),
                {"table_name": f"public.{table_name}"},
            ).scalar()

            if not exists:
                print(f"[WARN] adsdaily table missing: {table_name}")
                return {}, empty_totals

            df = pd.read_sql(
                text(f"""
                    SELECT *
                    FROM {table_name}
                    WHERE date::date >= :start_date
                      AND date::date <= :end_date
                """),
                conn,
                params={
                    "start_date": start_date,
                    "end_date": end_date,
                },
            )

    except Exception as e:
        print(f"[WARN] failed reading adsdaily MTD table {table_name}: {e}")
        return {}, empty_totals

    if df is None or df.empty or "sku" not in df.columns:
        return {}, empty_totals

    df = df.copy()

    df["sku"] = df["sku"].astype(str).str.strip()
    df = df[
        df["sku"].notna()
        & ~df["sku"].str.lower().isin(["", "none", "nan", "null", "total"])
    ].copy()

    if df.empty:
        return {}, empty_totals

    numeric_cols = [
        "spend",
        "ads_spend_total",
        "product_spend",
        "display_spend",
        "brand_spend",
        "ads_sales_total",
        "sale_amount",
        "sp_ads_sales",
        "sd_ads_sales",
        "sb_ads_sales",
        "clicks",
        "impressions",
        "sale_units",
        "advertised_unit_sale",
        "other_unit_sale",
        "new_to_brand_sales",
    ]

    for col in numeric_cols:
        if col not in df.columns:
            df[col] = 0.0

        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    grouped = (
        df.groupby("sku", as_index=False)
        .agg({
            "spend": "sum",
            "ads_spend_total": "sum",
            "product_spend": "sum",
            "display_spend": "sum",
            "brand_spend": "sum",
            "ads_sales_total": "sum",
            "sale_amount": "sum",
            "sp_ads_sales": "sum",
            "sd_ads_sales": "sum",
            "sb_ads_sales": "sum",
            "clicks": "sum",
            "impressions": "sum",
            "sale_units": "sum",
            "advertised_unit_sale": "sum",
            "other_unit_sale": "sum",
            "new_to_brand_sales": "sum",
        })
    )

    sku_map = {}

    for _, r in grouped.iterrows():
        sku = str(r.get("sku") or "").strip()
        if not sku:
            continue

        ads_spend = _safe_ads_float(r.get("ads_spend_total"))

        # fallback if ads_spend_total is empty
        if ads_spend == 0:
            ads_spend = _safe_ads_float(r.get("spend"))

        ads_sales = _safe_ads_float(r.get("ads_sales_total"))
        clicks = _safe_ads_float(r.get("clicks"))
        impressions = _safe_ads_float(r.get("impressions"))
        sale_units = _safe_ads_float(r.get("sale_units"))

        sku_map[sku] = {
            "ads_spend": round(ads_spend, 2),
            "ads_sales": round(ads_sales, 2),

            "spend": round(_safe_ads_float(r.get("spend")), 2),
            "product_spend": round(_safe_ads_float(r.get("product_spend")), 2),
            "display_spend": round(_safe_ads_float(r.get("display_spend")), 2),
            "brand_spend": round(_safe_ads_float(r.get("brand_spend")), 2),

            "sp_ads_sales": round(_safe_ads_float(r.get("sp_ads_sales")), 2),
            "sd_ads_sales": round(_safe_ads_float(r.get("sd_ads_sales")), 2),
            "sb_ads_sales": round(_safe_ads_float(r.get("sb_ads_sales")), 2),
            "sale_amount": round(_safe_ads_float(r.get("sale_amount")), 2),

            "clicks": round(clicks, 2),
            "impressions": round(impressions, 2),
            "sale_units": round(sale_units, 2),

            "advertised_unit_sale": round(_safe_ads_float(r.get("advertised_unit_sale")), 2),
            "other_unit_sale": round(_safe_ads_float(r.get("other_unit_sale")), 2),
            "new_to_brand_sales": round(_safe_ads_float(r.get("new_to_brand_sales")), 2),

            "roas": round(ads_sales / ads_spend, 2) if ads_spend else 0.0,
            "acos": round((ads_spend / ads_sales) * 100, 2) if ads_sales else 0.0,
            "ctr": round((clicks / impressions) * 100, 2) if impressions else 0.0,
            "cpc": round(ads_spend / clicks, 2) if clicks else 0.0,
            "conversion_rate": round((sale_units / clicks) * 100, 2) if clicks else 0.0,
        }

    total_ads_spend = sum(v["ads_spend"] for v in sku_map.values())
    total_ads_sales = sum(v["ads_sales"] for v in sku_map.values())
    total_clicks = sum(v["clicks"] for v in sku_map.values())
    total_impressions = sum(v["impressions"] for v in sku_map.values())
    total_sale_units = sum(v["sale_units"] for v in sku_map.values())

    totals = {
        "ads_spend": round(total_ads_spend, 2),
        "ads_sales": round(total_ads_sales, 2),
        "clicks": round(total_clicks, 2),
        "impressions": round(total_impressions, 2),
        "sale_units": round(total_sale_units, 2),
        "roas": round(total_ads_sales / total_ads_spend, 2) if total_ads_spend else 0.0,
        "acos": round((total_ads_spend / total_ads_sales) * 100, 2) if total_ads_sales else 0.0,
        "ctr": round((total_clicks / total_impressions) * 100, 2) if total_impressions else 0.0,
        "cpc": round(total_ads_spend / total_clicks, 2) if total_clicks else 0.0,
        "conversion_rate": round((total_sale_units / total_clicks) * 100, 2) if total_clicks else 0.0,
        "source_table": table_name,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
    }

    return sku_map, totals


def attach_adsdaily_mtd_ads_to_growth_rows(
    rows: list[dict],
    prev_ads_sku_map: dict,
    curr_ads_sku_map: dict,
) -> list[dict]:
    """
    Attach previous/current MTD ads to SKU growth rows.
    These rows then go to frontend + AI payload.
    """

    enriched_rows = []

    for row in rows or []:
        if not isinstance(row, dict):
            continue

        row = dict(row)

        sku = str(row.get("sku") or "").strip()
        prev_ads = prev_ads_sku_map.get(sku, {}) if sku else {}
        curr_ads = curr_ads_sku_map.get(sku, {}) if sku else {}

        prev_spend = _safe_ads_float(prev_ads.get("ads_spend"))
        curr_spend = _safe_ads_float(curr_ads.get("ads_spend"))

        prev_sales = _safe_ads_float(prev_ads.get("ads_sales"))
        curr_sales = _safe_ads_float(curr_ads.get("ads_sales"))

        prev_clicks = _safe_ads_float(prev_ads.get("clicks"))
        curr_clicks = _safe_ads_float(curr_ads.get("clicks"))

        prev_impressions = _safe_ads_float(prev_ads.get("impressions"))
        curr_impressions = _safe_ads_float(curr_ads.get("impressions"))

        prev_sale_units = _safe_ads_float(prev_ads.get("sale_units"))
        curr_sale_units = _safe_ads_float(curr_ads.get("sale_units"))

        prev_net_sales = _safe_ads_float(row.get("net_sales_prev"))
        curr_net_sales = _safe_ads_float(row.get("net_sales_curr"))

        prev_roas = round(prev_sales / prev_spend, 2) if prev_spend else 0.0
        curr_roas = round(curr_sales / curr_spend, 2) if curr_spend else 0.0

        prev_ads_acos = round((prev_spend / prev_sales) * 100, 2) if prev_sales else 0.0
        curr_ads_acos = round((curr_spend / curr_sales) * 100, 2) if curr_sales else 0.0

        prev_tacos = round((prev_spend / prev_net_sales) * 100, 2) if prev_net_sales else 0.0
        curr_tacos = round((curr_spend / curr_net_sales) * 100, 2) if curr_net_sales else 0.0

        prev_ctr = round((prev_clicks / prev_impressions) * 100, 2) if prev_impressions else 0.0
        curr_ctr = round((curr_clicks / curr_impressions) * 100, 2) if curr_impressions else 0.0

        prev_cpc = round(prev_spend / prev_clicks, 2) if prev_clicks else 0.0
        curr_cpc = round(curr_spend / curr_clicks, 2) if curr_clicks else 0.0

        prev_conversion = round((prev_sale_units / prev_clicks) * 100, 2) if prev_clicks else 0.0
        curr_conversion = round((curr_sale_units / curr_clicks) * 100, 2) if curr_clicks else 0.0

        row["mtd_ads_prev"] = {
            "ads_spend": round(prev_spend, 2),
            "ads_sales": round(prev_sales, 2),
            "roas": prev_roas,
            "acos": prev_ads_acos,
            "tacos": prev_tacos,
            "clicks": round(prev_clicks, 2),
            "impressions": round(prev_impressions, 2),
            "ctr": prev_ctr,
            "cpc": prev_cpc,
            "sale_units": round(prev_sale_units, 2),
            "conversion_rate": prev_conversion,
        }

        row["mtd_ads_curr"] = {
            "ads_spend": round(curr_spend, 2),
            "ads_sales": round(curr_sales, 2),
            "roas": curr_roas,
            "acos": curr_ads_acos,
            "tacos": curr_tacos,
            "clicks": round(curr_clicks, 2),
            "impressions": round(curr_impressions, 2),
            "ctr": curr_ctr,
            "cpc": curr_cpc,
            "sale_units": round(curr_sale_units, 2),
            "conversion_rate": curr_conversion,
        }

        row["mtd_ads_change"] = {
            "ads_spend_abs": round(curr_spend - prev_spend, 2),
            "ads_spend_pct": round(((curr_spend - prev_spend) / prev_spend) * 100, 2) if prev_spend else 0.0,

            "ads_sales_abs": round(curr_sales - prev_sales, 2),
            "ads_sales_pct": round(((curr_sales - prev_sales) / prev_sales) * 100, 2) if prev_sales else 0.0,

            "roas_change": round(curr_roas - prev_roas, 2),
            "ads_acos_change": round(curr_ads_acos - prev_ads_acos, 2),
            "tacos_change": round(curr_tacos - prev_tacos, 2),

            "clicks_abs": round(curr_clicks - prev_clicks, 2),
            "clicks_pct": round(((curr_clicks - prev_clicks) / prev_clicks) * 100, 2) if prev_clicks else 0.0,

            "conversion_rate_change": round(curr_conversion - prev_conversion, 2),
        }

        # Flat fields for old frontend / AI compatibility
        row["ads_spend_prev"] = row["mtd_ads_prev"]["ads_spend"]
        row["ads_spend_curr"] = row["mtd_ads_curr"]["ads_spend"]

        row["ads_sales_prev"] = row["mtd_ads_prev"]["ads_sales"]
        row["ads_sales_curr"] = row["mtd_ads_curr"]["ads_sales"]

        row["roas_prev"] = row["mtd_ads_prev"]["roas"]
        row["roas_curr"] = row["mtd_ads_curr"]["roas"]

        # ads_acos = spend / ads sales
        row["ads_acos_prev"] = row["mtd_ads_prev"]["acos"]
        row["ads_acos_curr"] = row["mtd_ads_curr"]["acos"]

        # acos/tacos compatibility = spend / total net sales
        row["acos_prev"] = row["mtd_ads_prev"]["tacos"]
        row["acos_curr"] = row["mtd_ads_curr"]["tacos"]
        row["tacos_prev"] = row["mtd_ads_prev"]["tacos"]
        row["tacos_curr"] = row["mtd_ads_curr"]["tacos"]

        row["ads_spend_growth_pct"] = row["mtd_ads_change"]["ads_spend_pct"]
        row["ads_sales_growth_pct"] = row["mtd_ads_change"]["ads_sales_pct"]

        enriched_rows.append(row)

    return enriched_rows



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

def fetch_current_ai_values_from_skuwisemonthly(
    user_id: int,
    country: str,
    curr_end: date,
):
    """
    Fetches current-month AI values from:
      skuwisemonthly_{user_id}_{country_lower}_{month_str}_{year}

    This is ONLY for AI payload.
    Do NOT use this for graph daily_series.
    """

    country_lower = str(country or "uk").strip().lower()
    month_str = month_name[curr_end.month].lower()
    year = curr_end.year

    table_name = f"skuwisemonthly_{user_id}_{country_lower}_{month_str}_{year}"

    try:
        with engine_hist.connect() as conn:
            df = pd.read_sql(
                text(f"SELECT * FROM {table_name}"),
                conn,
            )
    except Exception as e:
        print(f"[WARN] Could not read AI monthly table {table_name}: {e}")
        return [], {}, {}

    if df is None or df.empty or "sku" not in df.columns:
        return [], {}, {}

    df = df.copy()

    df["sku"] = df["sku"].astype(str).str.strip()
    df.loc[df["sku"].str.lower().isin(["none", "nan", "null", ""]), "sku"] = None

    total_mask = df["sku"].fillna("").str.lower().eq("total")
    total_row_df = df[total_mask].copy()

    sku_df = df[
        df["sku"].notna()
        & ~total_mask
    ].copy()

    if sku_df.empty:
        return [], {}, {}

    def first_col(xdf: pd.DataFrame, candidates: list[str]):
        for c in candidates:
            if c in xdf.columns:
                return c
        return None

    def get_series(xdf: pd.DataFrame, candidates: list[str], default=0.0):
        c = first_col(xdf, candidates)
        if c:
            return safe_num(xdf[c])
        return pd.Series([default] * len(xdf), index=xdf.index, dtype=float)

    def get_total(candidates: list[str], fallback_value=0.0):
        if total_row_df.empty:
            return fallback_value

        c = first_col(total_row_df, candidates)
        if not c:
            return fallback_value

        return float(safe_num(total_row_df[c]).sum())

    # -------------------------------------------------
    # Product name
    # -------------------------------------------------
    if "product_name" not in sku_df.columns:
        sku_df["product_name"] = sku_df["sku"]
    else:
        sku_df["product_name"] = sku_df["product_name"].fillna(sku_df["sku"])

    # -------------------------------------------------
    # Normalize monthly-table columns to your AI expected keys
    # calculate_growth() expects:
    # quantity, asp, net_sales, sales_mix,
    # unit_wise_profitability, profit, product_name, sku
    # -------------------------------------------------
    sku_df["quantity"] = get_series(
        sku_df,
        ["total_quantity"]
    )

    sku_df["net_sales"] = get_series(
        sku_df,
        ["net_sales", "sales", "sales_metric"]
    )

    sku_df["profit"] = get_series(
        sku_df,
        ["profit", "cm1_profit"]
    )

    sku_df["product_sales"] = get_series(
        sku_df,
        ["product_sales", "gross_product_sales"]
    )

    if "gross_sales" in sku_df.columns:
        sku_df["gross_sales"] = safe_num(sku_df["gross_sales"])
    else:
        sku_df["gross_sales"] = sku_df["product_sales"]

    # ASP
    if "asp" in sku_df.columns:
        sku_df["asp"] = safe_num(sku_df["asp"])
    else:
        qty_nonzero = sku_df["quantity"].replace(0, np.nan)
        sku_df["asp"] = (
            sku_df["net_sales"] / qty_nonzero
        ).replace([np.inf, -np.inf], np.nan).fillna(0.0)

    # Unit-wise profitability
    if "unit_wise_profitability" in sku_df.columns:
        sku_df["unit_wise_profitability"] = safe_num(sku_df["unit_wise_profitability"])
    elif "profit_per_unit" in sku_df.columns:
        sku_df["unit_wise_profitability"] = safe_num(sku_df["profit_per_unit"])
    else:
        qty_nonzero = sku_df["quantity"].replace(0, np.nan)
        sku_df["unit_wise_profitability"] = (
            sku_df["profit"] / qty_nonzero
        ).replace([np.inf, -np.inf], np.nan).fillna(0.0)

    # Sales mix
    if "sales_mix" in sku_df.columns:
        sku_df["sales_mix"] = safe_num(sku_df["sales_mix"])
    else:
        total_net_sales = float(sku_df["net_sales"].sum())
        sku_df["sales_mix"] = (
            (sku_df["net_sales"] / total_net_sales) * 100.0
            if total_net_sales
            else 0.0
        )

    sku_df = normalize_sales_mix(sku_df, "sales_mix", digits=2)

    # Optional columns used in AI/enrichment
    for c in [
        "selling_fees",
        "fba_fees",
        "tax_and_credits",
        "cogs",
        "ads_spend",
        "cm2_profit",
        "platform_fee",
        "advertising",
        "rembursement_fee",
        "reimbursement_fee",
    ]:
        if c not in sku_df.columns:
            sku_df[c] = 0.0
        else:
            sku_df[c] = safe_num(sku_df[c])

    out_cols = [
        "sku",
        "product_name",
        "quantity",
        "product_sales",
        "gross_sales",
        "selling_fees",
        "fba_fees",
        "tax_and_credits",
        "asp",
        "profit",
        "sales_mix",
        "net_sales",
        "unit_wise_profitability",
        "cogs",
        "ads_spend",
        "cm2_profit",
        "platform_fee",
        "advertising",
        "rembursement_fee",
        "reimbursement_fee",
    ]

    curr_ai_data = (
        sku_df[out_cols]
        .replace({np.nan: None})
        .to_dict(orient="records")
    )

    # -------------------------------------------------
    # AI totals: prefer TOTAL row from monthly table
    # -------------------------------------------------
    fallback_totals = aggregate_totals(curr_ai_data)
    fallback_totals["total_asp"] = compute_total_asp(curr_ai_data)
    fallback_totals["unit_wise_profitability"] = compute_total_unit_profitability(curr_ai_data)

    curr_ai_totals = {
        "quantity": get_total(
            ["total_quantity", "units", "unit_sold"],
            fallback_totals.get("quantity", 0.0),
        ),
        "net_sales": get_total(
            ["net_sales", "sales", "sales_metric"],
            fallback_totals.get("net_sales", 0.0),
        ),
        "profit": get_total(
            ["profit", "cm1_profit"],
            fallback_totals.get("profit", 0.0),
        ),
        "total_asp": get_total(
            ["asp"],
            fallback_totals.get("total_asp", 0.0),
        ),
        "unit_wise_profitability": get_total(
            ["unit_wise_profitability", "profit_per_unit"],
            fallback_totals.get("unit_wise_profitability", 0.0),
        ),
    }

    # If TOTAL row asp/unit profit is missing or zero, recompute portfolio level
    if not curr_ai_totals["total_asp"]:
        curr_ai_totals["total_asp"] = fallback_totals.get("total_asp", 0.0)

    if not curr_ai_totals["unit_wise_profitability"]:
        curr_ai_totals["unit_wise_profitability"] = fallback_totals.get("unit_wise_profitability", 0.0)

    # -------------------------------------------------
    # AI fee totals: prefer TOTAL row, else SKU sum
    # -------------------------------------------------
    curr_ai_fee_totals = {
        "platform_fee": abs(get_total(
            ["platform_fee", "platform_fees"],
            float(sku_df["platform_fee"].sum()) if "platform_fee" in sku_df.columns else 0.0,
        )),
        "advertising": abs(get_total(
            ["ads_spend", "advertising", "advertising_total"],
            float(sku_df["ads_spend"].sum()) if "ads_spend" in sku_df.columns else 0.0,
        )),
        "rembursement_fee": get_total(
            ["rembursement_fee", "reimbursement_fee", "net_reimbursement"],
            float(sku_df["rembursement_fee"].sum()) if "rembursement_fee" in sku_df.columns else 0.0,
        ),
    }

    return curr_ai_data, curr_ai_totals, curr_ai_fee_totals



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


def fetch_skuwisemonthly_ads_cm2_by_month(
    user_id: int,
    country: str,
    year: int,
    month: int,
) -> tuple[dict, dict]:
    """
    Reads skuwisemonthly table for a full month.

    Table format:
      skuwisemonthly_{user_id}_{country}_{month_name}_{year}

    Required columns:
      sku, product_name, cm2_profit

    Optional column:
      ads_spend

    Since this table has no date/time column, this always returns full-month values.
    """

    country = str(country or "uk").strip().lower()
    month = int(month)
    year = int(year)

    mn = month_name[month].lower()
    table = f"skuwisemonthly_{int(user_id)}_{country}_{mn}_{int(year)}"

    try:
        with engine_hist.connect() as conn:
            df = pd.read_sql(
                text(f"""
                    SELECT
                        sku,
                        product_name,
                        cm2_profit,
                        ads_spend
                    FROM {table}
                """),
                conn,
            )
    except Exception as e:
        print(f"[WARN] Could not read CM2 from {table}: {e}")
        return {}, {
            "ads_spend": 0.0,
            "cm2_profit": 0.0,
            "source_table": table,
        }

    if df is None or df.empty or "sku" not in df.columns:
        return {}, {
            "ads_spend": 0.0,
            "cm2_profit": 0.0,
            "source_table": table,
        }

    df = df.copy()

    df["sku"] = df["sku"].astype(str).str.strip()
    df.loc[
        df["sku"].str.lower().isin(["", "none", "nan", "null"]),
        "sku"
    ] = None

    if "product_name" not in df.columns:
        df["product_name"] = None

    if "cm2_profit" not in df.columns:
        df["cm2_profit"] = 0.0
    else:
        df["cm2_profit"] = safe_num(df["cm2_profit"])

    if "ads_spend" not in df.columns:
        df["ads_spend"] = 0.0
    else:
        df["ads_spend"] = safe_num(df["ads_spend"])

    total_row = df[df["sku"].fillna("").str.lower().eq("total")]

    if not total_row.empty:
        totals = {
            "ads_spend": float(safe_num(total_row["ads_spend"]).sum()),
            "cm2_profit": float(safe_num(total_row["cm2_profit"]).sum()),
            "source_table": table,
        }
    else:
        sku_only = df[
            df["sku"].notna()
            & ~df["sku"].str.lower().eq("total")
        ]

        totals = {
            "ads_spend": float(safe_num(sku_only["ads_spend"]).sum()),
            "cm2_profit": float(safe_num(sku_only["cm2_profit"]).sum()),
            "source_table": table,
        }

    sku_df = df[
        df["sku"].notna()
        & ~df["sku"].str.lower().eq("total")
    ]

    sku_map = {}

    for _, r in sku_df.iterrows():
        sku = str(r.get("sku") or "").strip()
        if not sku:
            continue

        sku_map[sku] = {
            "product_name": r.get("product_name"),
            "ads_spend": float(r.get("ads_spend") or 0.0),
            "cm2_profit": float(r.get("cm2_profit") or 0.0),
        }

    return sku_map, totals


def fetch_skuwisemonthly_ads_cm2_current_month(
    user_id: int,
    country: str,
    year: int,
    month: int,
) -> tuple[dict, dict]:
    """
    Backward-compatible wrapper.
    Existing code can still call this for current month.
    """
    return fetch_skuwisemonthly_ads_cm2_by_month(
        user_id=user_id,
        country=country,
        year=year,
        month=month,
    )

def fetch_current_inventory_snapshot(
    user_id: int,
    country: str,
    year: int,
    month: int,
) -> tuple[dict, dict]:
    """
    Reads frontend-only inventory data from:
      currentinventory_{user_id}_{country_key}_{month_name}{year}_table

    Columns:
      SKU
      available
      Coverage Ratio (In Months)

    This is NOT for AI.
    This is only for frontend payload enrichment.
    """

    country_key = str(country or "uk").strip().lower()

    COUNTRY_MAP = {
        "usa": "us",
        "united states": "us",
        "united states of america": "us",
        "gb": "uk",
        "great britain": "uk",
        "united kingdom": "uk",
    }

    country_key = COUNTRY_MAP.get(country_key, country_key)

    month = int(month)
    year = int(year)
    month_str = month_name[month].lower()

    table = f"currentinventory_{int(user_id)}_{country_key}_{month_str}{year}_table"

    try:
        with engine_hist.connect() as conn:
            df = pd.read_sql(
                text(f"""
                    SELECT
                        "SKU" AS sku,
                        "available" AS available,
                        "Coverage Ratio (In Months)" AS coverage_ratio_months
                    FROM {table}
                """),
                conn,
            )
    except Exception as e:
        print(f"[WARN] Could not read frontend inventory table {table}: {e}")
        return {}, {
            "available_total": 0.0,
            "avg_coverage_ratio_months": 0.0,
            "source_table": table,
        }

    if df is None or df.empty or "sku" not in df.columns:
        return {}, {
            "available_total": 0.0,
            "avg_coverage_ratio_months": 0.0,
            "source_table": table,
        }

    df = df.copy()

    df["sku"] = df["sku"].astype(str).str.strip()
    df = df[
        df["sku"].notna()
        & ~df["sku"].str.lower().isin(["", "none", "nan", "null", "total"])
    ].copy()

    if df.empty:
        return {}, {
            "available_total": 0.0,
            "avg_coverage_ratio_months": 0.0,
            "source_table": table,
        }

    df["available"] = safe_num(df.get("available", 0.0))
    df["coverage_ratio_months"] = safe_num(df.get("coverage_ratio_months", 0.0))

    sku_map = {}

    for _, r in df.iterrows():
        sku = str(r.get("sku") or "").strip()
        if not sku:
            continue

        sku_map[sku] = {
            "current_inventory": round(float(r.get("available") or 0.0), 2),
            "coverage_ratio_months": round(float(r.get("coverage_ratio_months") or 0.0), 2),
        }

    avg_cov = float(df["coverage_ratio_months"].replace(0, np.nan).mean())
    if np.isnan(avg_cov):
        avg_cov = 0.0

    totals = {
        "available_total": round(float(df["available"].sum()), 2),
        "avg_coverage_ratio_months": round(avg_cov, 2),
        "source_table": table,
    }

    return sku_map, totals



def select_focus_skus_by_sales_mix_from_rows(
    sku_rows: list[dict],
    threshold: float = 80.0,
    min_focus: int = 5,
) -> list[str]:
    ranked = []

    # -------------------------------------------------
    # Primary ranking: sales_mix / Sales Mix (Current)
    # -------------------------------------------------
    for row in sku_rows or []:
        if not isinstance(row, dict):
            continue

        sku = str(row.get("sku") or "").strip()
        if not sku:
            continue

        sales_mix = row.get("sales_mix_curr")
        if sales_mix is None:
            sales_mix = row.get("Sales Mix (Current)")
        if sales_mix is None:
            sales_mix = row.get("sales_mix")

        if sales_mix is None:
            continue

        try:
            ranked.append((sku, float(sales_mix)))
        except (TypeError, ValueError):
            continue

    using_sales_mix = bool(ranked)

    # -------------------------------------------------
    # Fallback ranking: current net_sales
    # This prevents everything collapsing into Remaining SKUs
    # if sales_mix is missing.
    # -------------------------------------------------
    if not ranked:
        for row in sku_rows or []:
            if not isinstance(row, dict):
                continue

            sku = str(row.get("sku") or "").strip()
            if not sku:
                continue

            net_sales = row.get("net_sales_curr")
            if net_sales is None:
                net_sales = row.get("net_sales")

            if net_sales is None:
                continue

            try:
                ranked.append((sku, float(net_sales)))
            except (TypeError, ValueError):
                continue

    ranked.sort(key=lambda x: x[1], reverse=True)

    if not ranked:
        return []

    cumulative = 0.0
    selected = []

    for sku, value in ranked:
        selected.append(sku)

        if using_sales_mix:
            cumulative += value
            if cumulative >= threshold:
                break
        else:
            if len(selected) >= min_focus:
                break

    # ✅ Always return at least 5 SKUs where available
    if len(selected) < min_focus:
        return [sku for sku, _ in ranked[:min_focus]]

    return selected


def build_remaining_skus_aggregate(top_80_skus: list, focus_skus: list):
    focus_set = set([str(x).strip() for x in (focus_skus or [])])

    remaining = [
        r for r in (top_80_skus or [])
        if str(r.get("sku")).strip() not in focus_set
    ]

    if not remaining:
        return None

    # ✅ NEW: keep product names/SKUs included inside Remaining SKUs
    included_products = []

    for r in remaining:
        sku = str(r.get("sku") or "").strip()
        product_name = (
            r.get("product_name")
            or r.get("Product Name")
            or sku
        )

        included_products.append({
            "sku": sku,
            "product_name": str(product_name).strip() or sku,
        })

    included_products = sorted(
        included_products,
        key=lambda x: x["product_name"].lower()
    )

    prev_qty = sum(safe0(r.get("quantity_prev")) for r in remaining)
    curr_qty = sum(safe0(r.get("quantity_curr")) for r in remaining)

    prev_sales = sum(safe0(r.get("net_sales_prev")) for r in remaining)
    curr_sales = sum(safe0(r.get("net_sales_curr")) for r in remaining)

    prev_profit = sum(safe0(r.get("profit_prev")) for r in remaining)
    curr_profit = sum(safe0(r.get("profit_curr")) for r in remaining)

    # ✅ CM2 values are attached in build_ai_summary before this function is called
    prev_cm2_profit = sum(safe0(r.get("cm2_profit_prev")) for r in remaining)
    curr_cm2_profit = sum(safe0(r.get("cm2_profit_curr")) for r in remaining)

    # ✅ NEW: Remaining SKUs CM2 profit per unit
    prev_cm2_profit_per_unit = (
        round(prev_cm2_profit / prev_qty, 2)
        if prev_qty
        else 0.0
    )

    curr_cm2_profit_per_unit = (
        round(curr_cm2_profit / curr_qty, 2)
        if curr_qty
        else 0.0
    )

    prev_asp = prev_sales / prev_qty if prev_qty else None
    curr_asp = curr_sales / curr_qty if curr_qty else None

    prev_ppu = prev_profit / prev_qty if prev_qty else None
    curr_ppu = curr_profit / curr_qty if curr_qty else None

    cm2_profit_growth_pct = (
        round(((curr_cm2_profit - prev_cm2_profit) / prev_cm2_profit) * 100.0, 2)
        if prev_cm2_profit
        else 0.0
    )

    cm2_margin_prev = (
        round((prev_cm2_profit / prev_sales) * 100.0, 2)
        if prev_sales
        else 0.0
    )

    cm2_margin_curr = (
        round((curr_cm2_profit / curr_sales) * 100.0, 2)
        if curr_sales
        else 0.0
    )

    return {
        "sku": "REMAINING_SEGMENT",
        "product_name": "Remaining SKUs",

        "included_products": included_products,
        "included_product_count": len(included_products),

        "quantity_prev": prev_qty,
        "quantity_curr": curr_qty,

        "net_sales_prev": prev_sales,
        "net_sales_curr": curr_sales,

        "profit_prev": prev_profit,
        "profit_curr": curr_profit,

        # ✅ CM2 values for Remaining SKUs card
        "cm2_profit_prev": prev_cm2_profit,
        "cm2_profit_curr": curr_cm2_profit,

        # ✅ NEW
        "cm2_profit_per_unit_prev": prev_cm2_profit_per_unit,
        "cm2_profit_per_unit_curr": curr_cm2_profit_per_unit,

        "cm2_profit_growth_pct": cm2_profit_growth_pct,
        "cm2_margin_prev": cm2_margin_prev,
        "cm2_margin_curr": cm2_margin_curr,

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

    # ✅ Use rounded ASP values for % change so Business Summary matches ASP card
    asp_prev_for_pct = round(asp_prev, 2) if asp_prev is not None else None
    asp_curr_for_pct = round(asp_curr, 2) if asp_curr is not None else None

    up_prev_idx = safe0(prev_totals.get("unit_wise_profitability"))
    up_curr_idx = safe0(curr_totals.get("unit_wise_profitability"))

    qty_pct   = pct_change_2(qty_prev, qty_curr)
    sales_pct = pct_change_2(sales_prev, sales_curr)
    prof_pct  = pct_change_2(prof_prev, prof_curr)
    asp_pct   = pct_change_2(asp_prev_for_pct, asp_curr_for_pct)
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

    # Remove skipped SKUs from top_80_skus
    top_80_skus = [
        row for row in (top_80_skus or [])
        if row.get("sku") not in SKUS_TO_SKIP
    ]

    # =========================================================
    # ✅ SKU-Level Ads + CM2 Enrichment
    # Current month = current skuwisemonthly full table
    # Previous month = previous skuwisemonthly full table
    # IMPORTANT:
    # This must happen BEFORE focus_skus / Remaining SKUs are built,
    # so Remaining SKUs can aggregate cm2_profit_prev/curr.
    # =========================================================
    curr_ads_sku_map = {}
    curr_ads_monthly_totals = {
        "ads_spend": 0.0,
        "cm2_profit": 0.0,
        "source_table": None,
    }

    prev_ads_sku_map = {}
    prev_ads_monthly_totals = {
        "ads_spend": 0.0,
        "cm2_profit": 0.0,
        "source_table": None,
    }

    # =========================================================
    # ✅ Frontend-only Inventory Enrichment
    # This will NOT be passed into product_action_context / AI prompt.
    # It is only for the recommendation cards on frontend.
    # =========================================================
    frontend_inventory_sku_map = {}
    frontend_inventory_totals = {
        "available_total": 0.0,
        "avg_coverage_ratio_months": 0.0,
        "source_table": None,
    }

    resolved_country = (country or (currency or {}).get("country") or "uk").strip().lower()

    # Resolve year/month safely
    if current_year is None or current_month is None:
        today_local = date.today()
        current_year = current_year or today_local.year
        current_month = current_month or today_local.month

    current_year = int(current_year)
    current_month = int(current_month)

    # Previous month for full-month skuwisemonthly CM2
    previous_year = current_year
    previous_month = current_month - 1

    if previous_month == 0:
        previous_month = 12
        previous_year -= 1

    if user_id:
        try:
            frontend_inventory_sku_map, frontend_inventory_totals = fetch_current_inventory_snapshot(
                user_id=int(user_id),
                country=resolved_country,
                year=int(current_year),
                month=int(current_month),
            )
        except Exception as e:
            print("[WARN] Frontend inventory enrichment failed:", e)
            frontend_inventory_sku_map = {}
            frontend_inventory_totals = {
                "available_total": 0.0,
                "avg_coverage_ratio_months": 0.0,
                "source_table": None,
            }

    if user_id:
        try:
            # Current CM2 from current month's skuwisemonthly table
            curr_ads_sku_map, curr_ads_monthly_totals = fetch_skuwisemonthly_ads_cm2_by_month(
                user_id=int(user_id),
                country=resolved_country,
                year=int(current_year),
                month=int(current_month),
            )

            # Previous CM2 from previous month's skuwisemonthly table
            # No date filtering because skuwisemonthly has no date/time column
            prev_ads_sku_map, prev_ads_monthly_totals = fetch_skuwisemonthly_ads_cm2_by_month(
                user_id=int(user_id),
                country=resolved_country,
                year=int(previous_year),
                month=int(previous_month),
            )

        except Exception as e:
            print("[WARN] Ads/CM2 enrichment failed:", e)

            curr_ads_sku_map = {}
            curr_ads_monthly_totals = {
                "ads_spend": 0.0,
                "cm2_profit": 0.0,
                "source_table": None,
            }

            prev_ads_sku_map = {}
            prev_ads_monthly_totals = {
                "ads_spend": 0.0,
                "cm2_profit": 0.0,
                "source_table": None,
            }

    # Attach previous + current ads/CM2 data to each SKU row
    for row in (top_80_skus or []):
        sku = safe_strip(row.get("sku"), default="")
        if not sku:
            continue

        curr_ads_data = curr_ads_sku_map.get(sku, {})
        prev_ads_data = prev_ads_sku_map.get(sku, {})

        ads_spend_curr = float(curr_ads_data.get("ads_spend", 0.0))
        cm2_profit_curr = float(curr_ads_data.get("cm2_profit", 0.0))

        ads_spend_prev = float(prev_ads_data.get("ads_spend", 0.0))
        cm2_profit_prev = float(prev_ads_data.get("cm2_profit", 0.0))

        net_sales_curr = safe_float_local(row.get("net_sales_curr"))
        if net_sales_curr is None:
            net_sales_curr = safe_float_local(row.get("net_sales"))
        net_sales_curr = float(net_sales_curr or 0.0)

        net_sales_prev = safe_float_local(row.get("net_sales_prev"))
        net_sales_prev = float(net_sales_prev or 0.0)

        acos_curr = (
            round((ads_spend_curr / net_sales_curr) * 100.0, 2)
            if net_sales_curr
            else 0.0
        )

        acos_prev = (
            round((ads_spend_prev / net_sales_prev) * 100.0, 2)
            if net_sales_prev
            else 0.0
        )

        cm2_margin_curr = (
            round((cm2_profit_curr / net_sales_curr) * 100.0, 2)
            if net_sales_curr
            else 0.0
        )

        cm2_margin_prev = (
            round((cm2_profit_prev / net_sales_prev) * 100.0, 2)
            if net_sales_prev
            else 0.0
        )

        cm2_profit_growth_pct = (
            round(((cm2_profit_curr - cm2_profit_prev) / cm2_profit_prev) * 100.0, 2)
            if cm2_profit_prev
            else 0.0
        )

        row["ads_spend_prev"] = round(ads_spend_prev, 2)
        row["ads_spend_curr"] = round(ads_spend_curr, 2)

        row["acos_prev"] = acos_prev
        row["acos_curr"] = acos_curr

        row["cm2_profit_prev"] = round(cm2_profit_prev, 2)
        row["cm2_profit_curr"] = round(cm2_profit_curr, 2)

        # ✅ NEW: CM2 profit per unit
        quantity_prev = float(safe_float_local(row.get("quantity_prev")) or 0.0)
        quantity_curr = float(safe_float_local(row.get("quantity_curr")) or 0.0)

        row["cm2_profit_per_unit_prev"] = (
            round(cm2_profit_prev / quantity_prev, 2)
            if quantity_prev
            else 0.0
        )

        row["cm2_profit_per_unit_curr"] = (
            round(cm2_profit_curr / quantity_curr, 2)
            if quantity_curr
            else 0.0
        )

        row["cm2_margin_prev"] = cm2_margin_prev
        row["cm2_margin_curr"] = cm2_margin_curr

        row["cm2_profit_growth_pct"] = cm2_profit_growth_pct

    # ✅ Build focus_skus from filtered and CM2-enriched top_80_skus
    # Logic:
    # - rank by sales_mix / Sales Mix (Current)
    # - stop once cumulative sales mix reaches 80%
    # - but always return at least 5 SKUs if available
    # - fallback to net_sales top 5 if sales_mix is missing
    focus_skus = select_focus_skus_by_sales_mix_from_rows(
        sku_rows=top_80_skus,
        threshold=80.0,
        min_focus=5,
    )

    # ✅ Build aggregated Remaining SKUs after CM2 enrichment
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
                "cm2_profit": remaining_segment_raw.get("cm2_profit_prev", 0.0),
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
                "cm2_profit": remaining_segment_raw.get("cm2_profit_curr", 0.0),
                "asp": remaining_segment_raw["asp_curr"],
                "unit_wise_profitability": remaining_segment_raw["unit_wise_profitability_curr"],
                "sales_mix": 0,
            }],
            key="sku"
        )[0]

        # Add CM2 fields back to remaining_growth_row because calculate_growth()
        # does not currently include cm2_profit in growth_field_mapping.
        remaining_growth_row["cm2_profit_prev"] = round(
            safe0(remaining_segment_raw.get("cm2_profit_prev")),
            2,
        )
        remaining_growth_row["cm2_profit_curr"] = round(
            safe0(remaining_segment_raw.get("cm2_profit_curr")),
            2,
        )

        # ✅ NEW
        remaining_growth_row["cm2_profit_per_unit_prev"] = round(
            safe0(remaining_segment_raw.get("cm2_profit_per_unit_prev")),
            2,
        )
        remaining_growth_row["cm2_profit_per_unit_curr"] = round(
            safe0(remaining_segment_raw.get("cm2_profit_per_unit_curr")),
            2,
        )

        remaining_growth_row["cm2_profit_growth_pct"] = remaining_segment_raw.get(
            "cm2_profit_growth_pct",
            0.0,
        )
        remaining_growth_row["cm2_margin_prev"] = remaining_segment_raw.get(
            "cm2_margin_prev",
            0.0,
        )
        remaining_growth_row["cm2_margin_curr"] = remaining_segment_raw.get(
            "cm2_margin_curr",
            0.0,
        )  

    # =========================================================
    # ✅ All SKU Action Context
    # This is for product journey + all 3 recommendations.
    # It does NOT change focus_skus or Remaining SKUs logic.
    # =========================================================
    all_sku_action_rows = [
        row for row in (top_80_skus or [])
        if str(row.get("sku") or "").strip()
    ]

    all_individual_skus = [
        str(row.get("sku")).strip()
        for row in all_sku_action_rows
        if str(row.get("sku") or "").strip()
    ]    

    # =========================================================
    # ✅ Frontend-only Inventory Attachment
    # These enriched rows are used only in payload["sku_tables"].
    # Do NOT use these rows in product_action_context.
    # =========================================================

    def _frontend_inventory_lookup(sku_value):
        sku_clean = safe_strip(sku_value, default="")
        if not sku_clean:
            return {
                "current_inventory": 0.0,
                "coverage_ratio_months": 0.0,
            }

        # exact match first
        if sku_clean in frontend_inventory_sku_map:
            return frontend_inventory_sku_map.get(sku_clean) or {
                "current_inventory": 0.0,
                "coverage_ratio_months": 0.0,
            }

        # case-insensitive fallback
        sku_lower = sku_clean.lower()
        for inv_sku, inv_payload in frontend_inventory_sku_map.items():
            if str(inv_sku).strip().lower() == sku_lower:
                return inv_payload or {
                    "current_inventory": 0.0,
                    "coverage_ratio_months": 0.0,
                }

        return {
            "current_inventory": 0.0,
            "coverage_ratio_months": 0.0,
        }


    def _attach_frontend_inventory_to_row(row: dict) -> dict:
        if not isinstance(row, dict):
            return row

        row = dict(row)

        sku = safe_strip(row.get("sku"), default="")
        inv = _frontend_inventory_lookup(sku)

        row["current_inventory"] = round(
            float(inv.get("current_inventory", 0.0) or 0.0),
            2,
        )
        row["coverage_ratio_months"] = round(
            float(inv.get("coverage_ratio_months", 0.0) or 0.0),
            2,
        )

        return row


    def _build_frontend_focus_sku_rows(all_rows: list[dict], focus_sku_keys: list[str]) -> list[dict]:
        focus_set = set(
            str(x).strip()
            for x in (focus_sku_keys or [])
            if str(x).strip()
        )

        rows = []

        for row in all_rows or []:
            if not isinstance(row, dict):
                continue

            sku = safe_strip(row.get("sku"), default="")
            if sku and sku in focus_set:
                rows.append(_attach_frontend_inventory_to_row(row))

        return rows


    def _build_frontend_all_sku_rows(all_rows: list[dict]) -> list[dict]:
        return [
            _attach_frontend_inventory_to_row(row)
            for row in (all_rows or [])
            if isinstance(row, dict)
        ]


    def _attach_frontend_inventory_to_remaining_segment(
        remaining_row: dict | None,
        raw_remaining: dict | None,
    ) -> dict | None:
        if not remaining_row:
            return remaining_row

        remaining_row = dict(remaining_row)

        included_products = []
        total_inventory = 0.0

        weighted_coverage_num = 0.0
        weighted_coverage_den = 0.0

        for product in (raw_remaining or {}).get("included_products", []):
            sku = safe_strip(product.get("sku"), default="")
            product_name = product.get("product_name") or sku

            inv = _frontend_inventory_lookup(sku)

            current_inventory = float(inv.get("current_inventory", 0.0) or 0.0)
            coverage_ratio = float(inv.get("coverage_ratio_months", 0.0) or 0.0)

            total_inventory += current_inventory

            # Weighted coverage ratio by available units
            if current_inventory > 0 and coverage_ratio > 0:
                weighted_coverage_num += coverage_ratio * current_inventory
                weighted_coverage_den += current_inventory

            included_products.append({
                "sku": sku,
                "product_name": product_name,
                "current_inventory": round(current_inventory, 2),
                "coverage_ratio_months": round(coverage_ratio, 2),
            })

        remaining_row["current_inventory"] = round(total_inventory, 2)
        remaining_row["coverage_ratio_months"] = (
            round(weighted_coverage_num / weighted_coverage_den, 2)
            if weighted_coverage_den
            else 0.0
        )

        remaining_row["included_products"] = included_products
        remaining_row["included_product_count"] = len(included_products)

        return remaining_row


    frontend_all_sku_rows = _build_frontend_all_sku_rows(top_80_skus)

    frontend_focus_sku_rows = _build_frontend_focus_sku_rows(
        top_80_skus,
        focus_skus,
    )

    frontend_remaining_skus_aggregate = _attach_frontend_inventory_to_remaining_segment(
        remaining_growth_row,
        remaining_segment_raw,
    )



    # =========================================================
    # PAYLOAD
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
            # Frontend table rows enriched with:
            # current_inventory + coverage_ratio_months
            "top_80_skus": frontend_all_sku_rows,

            # Focus SKU keys
            "focus_skus": focus_skus,

            # Focus SKU rows for frontend cards
            "focus_sku_rows": frontend_focus_sku_rows,

            # Every available SKU for frontend
            "all_individual_skus": all_individual_skus,
            "all_sku_action_rows": frontend_all_sku_rows,

            # Aggregated Remaining SKUs card for frontend
            "remaining_skus_aggregate": frontend_remaining_skus_aggregate,
        },

        "sku_context": sku_context,

        # ✅ NEW: tells AI to create journey + recommendations for every SKU
        "product_action_context": {
            "action_skus": all_individual_skus,
            "action_rows": all_sku_action_rows,

            "focus_skus": focus_skus,
            "remaining_skus": remaining_growth_row,

            "required_sku_outputs": [
                "journey_summary",
                "recommendation",
                "ads_recommendation",
                "inventory_recommendation",
            ],

            "required_remaining_skus_outputs": [
                "remaining_skus_journey_summary",
                "remaining_skus_recommendation",
                "remaining_skus_ads_recommendation",
                "remaining_skus_inventory_recommendation",
            ],
        },
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
            "previous": {
                "year": int(previous_year),
                "month": int(previous_month),
                "ads_spend_total": round(prev_ads_monthly_totals.get("ads_spend", 0.0), 2),
                "cm2_profit_total": round(prev_ads_monthly_totals.get("cm2_profit", 0.0), 2),
                "source_table": prev_ads_monthly_totals.get("source_table"),
            },
            "current": {
                "year": int(current_year),
                "month": int(current_month),
                "ads_spend_total": round(curr_ads_monthly_totals.get("ads_spend", 0.0), 2),
                "cm2_profit_total": round(curr_ads_monthly_totals.get("cm2_profit", 0.0), 2),
                "source_table": curr_ads_monthly_totals.get("source_table"),
            },
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

def fetch_high_alert_threshold(user_id: int, country: str):
    query = text("""
        SELECT
            transit_time,
            stock_unit
        FROM public.country_profile
        WHERE user_id = :user_id
          AND LOWER(country) = :country
        LIMIT 1
    """)

    with engine_hist.connect() as conn:
        row = conn.execute(query, {
            "user_id": user_id,
            "country": str(country).strip().lower(),
        }).fetchone()

    if not row:
        return None

    transit_time = pd.to_numeric(row.transit_time, errors="coerce")
    stock_unit = pd.to_numeric(row.stock_unit, errors="coerce")

    if pd.isna(transit_time) or pd.isna(stock_unit):
        return None

    return float(transit_time) + float(stock_unit)


def generate_inventory_alerts_for_all_skus(user_id: int, country: str, coverage_df: pd.DataFrame = None) -> dict:
    alerts = {}

    high_alert_threshold = fetch_high_alert_threshold(user_id, country)

    # fallback old logic if country_profile is missing
    if high_alert_threshold is None:
        high_alert_threshold = 2

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
            if coverage_ratio <= high_alert_threshold and coverage_ratio > 0:
                alert = "High alert"
                alert_type = "supply"

            elif coverage_ratio <= 5 and coverage_ratio > 0:
                alert = "Please send shipment"
                alert_type = "supply"

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
            "high_alert_threshold": high_alert_threshold,
        }

    return alerts

def generate_sku_inventory_flags(
    user_id: int,
    country: str,
    focus_skus: list[str] | None = None,
    coverage_override_by_sku: dict | None = None,
) -> dict:
    """
    Returns SKU-level inventory flags including:
    - alert_type
    - numeric signals
    - inventory_recommendation

    IMPORTANT:
    - High alert logic uses:
        coverage_ratio <= transit_time + stock_unit
    - Route may pass coverage_override_by_sku only as a data source.
    """

    flags: dict[str, dict] = {}

    # -------------------------------------------------
    # 0. Dynamic High Alert threshold
    # -------------------------------------------------
    high_alert_threshold = fetch_high_alert_threshold(user_id, country)

    # Fallback old logic if country_profile is missing/incomplete
    if high_alert_threshold is None:
        high_alert_threshold = 2.0

    # -------------------------------------------------
    # 1. Build coverage map
    # -------------------------------------------------
    coverage_df = compute_inventory_coverage_ratio(user_id, country)

    coverage_map: dict[str, float | None] = {}

    if coverage_df is not None and not coverage_df.empty:
        for _, r in coverage_df.iterrows():
            sku = str(r.get("sku") or "").strip()
            if not sku:
                continue

            cov = pd.to_numeric(
                r.get("inventory_coverage_ratio"),
                errors="coerce",
            )

            coverage_map[sku.upper()] = float(cov) if pd.notna(cov) else None

    # -------------------------------------------------
    # 2. Override coverage from route/card rows if supplied
    # -------------------------------------------------
    for sku, cov in (coverage_override_by_sku or {}).items():
        sku_key = str(sku or "").strip().upper()
        if not sku_key:
            continue

        cov_num = pd.to_numeric(cov, errors="coerce")

        if pd.notna(cov_num):
            coverage_map[sku_key] = float(cov_num)

    # -------------------------------------------------
    # 3. Fetch inventory aged
    # -------------------------------------------------
    inv_df = fetch_inventory_aged_by_user(user_id, country)

    focus_set = (
        {str(x).strip().upper() for x in focus_skus if str(x).strip()}
        if focus_skus
        else None
    )

    def _num(v):
        try:
            if v is None or pd.isna(v):
                return 0.0
            return float(v)
        except Exception:
            return 0.0

    # -------------------------------------------------
    # 4. Build inventory aged map
    # -------------------------------------------------
    inv_by_sku = {}

    if inv_df is not None and not inv_df.empty:
        for _, r in inv_df.iterrows():
            sku = str(r.get("sku") or "").strip()
            if not sku:
                continue

            inv_by_sku[sku.upper()] = r

    # -------------------------------------------------
    # 5. Candidate SKUs
    # Use union so low-coverage SKUs still get alerts
    # even if inventory_aged row is missing.
    # -------------------------------------------------
    candidate_skus = set(coverage_map.keys()) | set(inv_by_sku.keys())

    if focus_set:
        candidate_skus = candidate_skus & focus_set

    if not candidate_skus:
        return flags

    # -------------------------------------------------
    # 6. Generate flags
    # -------------------------------------------------
    for sku_key in sorted(candidate_skus):
        r = inv_by_sku.get(sku_key)

        coverage_ratio = coverage_map.get(sku_key)

        cov_num = pd.to_numeric(coverage_ratio, errors="coerce")
        coverage_ratio = float(cov_num) if pd.notna(cov_num) else None

        # If no inventory_aged row exists, all age/cost values remain 0.
        if r is None:
            legacy_aged_units = 0.0
            detailed_aged_units = 0.0
            estimated_storage_cost = 0.0
        else:
            # ✅ SUPPORT BOTH SCHEMAS WITHOUT DOUBLE COUNTING
            legacy_aged_units = (
                _num(r.get("inv-age-181-to-270-days"))
                + _num(r.get("inv-age-271-to-365-days"))
                + _num(r.get("inv-age-365-plus-days"))
            )

            detailed_aged_units = (
                _num(r.get("inv-age-181-to-330-days"))
                + _num(r.get("inv-age-331-to-365-days"))
                + _num(r.get("inv-age-366-to-455-days"))
                + _num(r.get("inv-age-456-plus-days"))
            )

            estimated_storage_cost = _num(
                r.get("estimated-storage-cost-next-month")
            )

        # Prefer detailed schema if it exists, else fallback to legacy schema.
        if detailed_aged_units > 0:
            long_term_aged_units = detailed_aged_units
        else:
            long_term_aged_units = legacy_aged_units

        overaged = long_term_aged_units > 0

        # -------------------------------------------------
        # Alert logic
        # -------------------------------------------------
        alert_type = "none"
        alert = "No alert"

        if (
            coverage_ratio is not None
            and coverage_ratio > 0
            and coverage_ratio <= high_alert_threshold
        ):
            alert_type = "supply"
            alert = "High alert"

        elif (
            coverage_ratio is not None
            and coverage_ratio > high_alert_threshold
            and coverage_ratio <= 5
        ):
            alert_type = "supply"
            alert = "Please send shipment"

        elif coverage_ratio is not None and coverage_ratio >= 6:
            alert_type = "excess"
            alert = "High inventory coverage ratio"

        elif overaged:
            alert_type = "overaged"
            alert = "Long-term aged inventory"

        elif estimated_storage_cost > 100:
            alert_type = "cost"
            alert = "High storage cost"

        # -------------------------------------------------
        # Deterministic sentence
        # -------------------------------------------------
        cov_str = (
            round(float(coverage_ratio), 1)
            if coverage_ratio is not None
            else None
        )

        inventory_recommendation = "Inventory position is stable."

        if alert == "High alert" and cov_str is not None:
            inventory_recommendation = (
                f"Your coverage ratio is {cov_str} months. "
                f"Please immediately send stock to avoid stock-out."
            )

        elif alert == "Please send shipment" and cov_str is not None:
            inventory_recommendation = (
                f"Your coverage ratio is {cov_str} months. "
                f"Please supply inventory soon to avoid stock-out risk."
            )

        elif alert == "High inventory coverage ratio" and cov_str is not None:
            if overaged:
                inventory_recommendation = (
                    f"Your coverage ratio is {cov_str} months and "
                    f"{int(long_term_aged_units)} units are ageing long-term. "
                    f"Improve sell-through or reduce replenishment to avoid excess storage fees."
                )
            else:
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
                f"Estimated storage cost is {round(estimated_storage_cost, 2)}. "
                f"Reduce inventory exposure to control storage expense."
            )

        flags[sku_key] = {
            "inventory_alert": alert,
            "inventory_alert_type": alert_type,
            "inventory_coverage_ratio": coverage_ratio,
            "coverage_ratio_months": coverage_ratio,
            "high_alert_threshold": high_alert_threshold,
            "long_term_aged_units": int(long_term_aged_units),
            "estimated_storage_cost": round(estimated_storage_cost, 2),
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

        # print("[LIVE INSIGHT START]", {
        #     "input_country": country,
        #     "country_key": country_key,
        #     "sku": sku,
        #     "product_name": product_name,
        #     "key": key,
        #     "user_id": user_id,
        # })

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
