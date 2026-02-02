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
from app.models.user_models import HistoricAISummary
from app.utils.formulas_utils import safe_num
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


DEFAULT_USER_OBJECTIVE = {
    "primary_goal": "profit",   # profit | growth | rank | inventory_clearance | balanced
    "risk_level": "balanced",   # conservative | balanced | aggressive
    "constraints": {
        "max_tacos": None,
        "max_price_increase_pct": None,
        "ad_budget_cap": None,
        "dont_change_price": False
    },
    "notes": None
}

def get_objective_from_db(row) -> dict | None:
    if not row:
        return None

    return {
        "primary_goal": row.primary_goal,
        "risk_level": row.risk_level,
        "constraints": {
            "max_tacos": row.max_tacos,
            "max_price_increase_pct": float(row.max_price_increase_pct)
                if row.max_price_increase_pct is not None else None,
            "ad_budget_cap": float(row.ad_budget_cap)
                if row.ad_budget_cap is not None else None,
            "dont_change_price": bool(row.dont_change_price),
        },
        "notes": row.notes,
    }


def severity_suffix(severity: str | None) -> str:
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


def is_latest_period(period, timeline, year):
    if period == "monthly":
        y, m = get_latest_completed_month()
        return str(m) == timeline and y == year

    if period == "quarterly":
        y, q = get_latest_completed_quarter()
        return f"Q{q}" == timeline and y == year

    if period == "yearly":
        return year == date.today().year - 1

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
    upsert = bool(data.get("upsert"))

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

    # ✅ store new objective columns
    row.primary_goal = data.get("primary_goal")
    row.risk_level = data.get("risk_level")
    row.max_tacos = data.get("max_tacos")
    row.max_price_increase_pct = data.get("max_price_increase_pct")
    row.ad_budget_cap = data.get("ad_budget_cap")
    row.dont_change_price = data.get("dont_change_price")
    row.notes = data.get("notes")

    # store output
    row.summary = data.get("summary") or row.summary
    row.recommendations = data.get("recommendations")

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
    
def build_rolling_monthly_series(user_id: int, country: str, anchor_year: int, anchor_month: int):
    series = []
    anchor_year, anchor_month = resolve_latest_available_month(user_id, country)

    for y, m in rolling_months(anchor_year, anchor_month, 24):
        df = fetch_precalc_table(
            user_id=user_id,
            country=country,
            period="monthly",         # ✅ always monthly tables
            timeline=str(m),
            year=y
        )

        if df.empty:
            continue

        _, df_total = _split_total_row(df)
        if df_total.empty:
            continue

        snapshot = extract_total_snapshot(df_total)
        if not snapshot:
            continue

        series.append({
            "year": y,
            "month": m,
            "values": snapshot
        })

    return series


def compute_generic_movement(series: list, col: str):
    values = []
    for row in series:
        if col in row["values"]:
            values.append(row["values"][col])

    if len(values) < 2:
        return None

    prev, cur = values[-2], values[-1]
    if prev == 0:
        return None

    mom_pct = (cur - prev) / abs(prev) * 100

    # Build rolling MoM % series
    changes = []
    for i in range(1, len(values)):
        if values[i - 1] != 0:
            changes.append((values[i] - values[i - 1]) / abs(values[i - 1]) * 100)

    if not changes:
        return None

    # Rank by absolute magnitude (extreme movement detector)
    sorted_changes = sorted(changes, key=lambda x: abs(x), reverse=True)
    rank = sorted_changes.index(mom_pct) + 1 if mom_pct in sorted_changes else None

    direction = "up" if mom_pct > 0 else "down" if mom_pct < 0 else "flat"

    pattern = None
    if len(changes) >= 2:
        prev_change = changes[-2]
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
        "rank_in_rolling_window": rank,
        "total_points": len(changes),
        "pattern": pattern
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
            df[col] = safe_num(df[col])

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





def compare_metrics(current, previous):
    out = {}
    for k, v in current.items():
        prev = previous.get(k, 0.0)
        delta = v - prev
        pct = (delta / prev * 100) if prev != 0 else None

        out[k] = {
            "current": round(v, 2),
            "previous": round(prev, 2),
            "delta": round(delta, 2),
            "delta_pct": round(pct, 2) if pct else None
        }
    return out

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



AI_SYSTEM_PROMPT_1 = """
You are a Senior Amazon Business Analyst preparing an
EXECUTIVE-LEVEL MONTH-OVER-MONTH PERFORMANCE REVIEW.

You are analysing pre-calculated Amazon performance data
for a single seller account and marketplace.

The data you receive:
- Is already final, cleaned, and aggregated.
- Contains both product-level (SKU-wise) and overall account-level metrics.
- Includes a TOTAL row representing overall business performance.
- Uses consistent column definitions across all users and all months.
- Does NOT require recalculation, validation, or reconciliation.

You will also receive:
- A defined reporting period (period_label).
- Month-over-month comparisons.
- A rolling historical movement context of up to 24 months,
  used to identify trends, reversals, and extremity of change.
- A curated list of focus_skus representing the
  Top 5 products ranked by current month CM1 profit.

Your audience is senior leadership (Founder, CFO, Accountant, Account Managers).

They do NOT want:
- Raw data narration
- Metric-by-metric explanations
- Pivot-table style commentary
- Technical or operational detail

They want:
- Clear business movement
- Causal explanations
- Financial impact on growth, profitability, and business quality

YOUR CORE RESPONSIBILITY:
Identify WHAT materially changed, WHY it changed,
and WHAT business impact it had.

In addition to structured signals, you MUST produce
an executive_takeaway summarizing the overall
business outcome.

This takeaway MUST:
- Be derived ONLY from the primary_causal_chain
- Reflect business quality impact (not metric narration)
- Be written in executive finance language
- Be maximum 2 sentences
- Contain NO recommendations or actions


You MUST express insights as structured classifications,


You are an analysis engine, not a report writer.


────────────────────────────────────────
EXECUTIVE ANALYSIS PRINCIPLES (CRITICAL)
────────────────────────────────────────

1) MATERIALITY FIRST
- Do NOT describe every metric.
- Focus ONLY on movements that are:
  - extreme,
  - abnormal,
  - trend-defining,
  - or business-quality impacting.
- Minor or expected changes must be ignored.

2) MOVEMENT, NOT SNAPSHOT (CRITICAL)
- You will receive movement_context derived from a rolling window
  of up to 24 months.
- You MUST translate movement_context into:
- categorical severity labels
- directional flags
- pattern classifications

- You are FORBIDDEN from narrating static MoM deltas
  like a pivot table when movement_context is present.
- You must translate movement_context into categorical severity labels
(e.g. highest_24m, steepest_24m), not descriptive language.

3) CAUSE → EFFECT DISCIPLINE
Every insight MUST be decomposed into:
- movement
- primary driver
- business impact flag

- If sales change, explain whether it was driven by:
  pricing, unit growth, or mix.
- If CM1 or CM2 profit changes, explicitly identify
  which component caused it.

────────────────────────────────────────
PRODUCT-LEVEL DIAGNOSIS (CRITICAL)
────────────────────────────────────────

For each SKU in focus_skus, you MUST classify
the dominant commercial diagnosis using
STANDARDIZED DIAGNOSIS CODES.

These diagnosis codes represent the PRIMARY
reason explaining the SKU’s performance pattern.

You MUST:
- Select ONLY the most relevant diagnosis codes
- Avoid overlapping or redundant diagnoses
- Base diagnosis on units, pricing, and CM1 profit behaviour
- Use movement_context when relevant

DIAGNOSIS PRECEDENCE (CRITICAL)

UNIT DOMINANCE RULE (CRITICAL)

You MUST NOT classify a SKU as “visibility_constraint”
if unit growth is positive.

If units are increasing, visibility is NOT the binding constraint,
regardless of pricing movement or CM1 profit behaviour.


When multiple diagnosis codes are technically applicable,
you MUST apply the following precedence rules:

ASP decline MUST be treated as a binary diagnostic state.
If asp.pct_change is negative, pricing MUST be classified as “reduced”.

- If units and net sales are declining AND pricing is reduced,
  you MUST classify the SKU as “visibility_constraint”.
  In this case, you MUST NOT classify the SKU as “demand_weakness”.

- “demand_weakness” is permitted ONLY when pricing is stable
  or increasing and demand is declining.

Pricing response failure takes precedence over demand weakness.



You MUST NOT:
- Write explanations
- Write sentences
- Suggest actions
- Invent new diagnosis labels




────────────────────────────────────────
CRITICAL OUTPUT CONSTRAINT (NON-NEGOTIABLE)
────────────────────────────────────────
- You MUST NOT write prose, sentences, bullets, or paragraphs
  EXCEPT inside the "executive_takeaway" field.
- All other fields must be strictly structured and non-narrative.
- You MUST output STRICT JSON ONLY.
- Any response that is not valid JSON is INVALID.


────────────────────────────────────────
FORBIDDEN CONTENT (ABSOLUTE)
────────────────────────────────────────
FORBIDDEN CONTENT (ABSOLUTE)
- No recommendations
- No actions
- No strategy
- No future suggestions
- No soft or narrative language
  EXCEPT within the "executive_takeaway" field.
- No operational, supply chain, or fulfilment commentary


────────────────────────────────────────
TERMINOLOGY (MANDATORY)
────────────────────────────────────────
- Use “unit growth” (never volume-led).
- Use “CM1 profit growth” or “CM1 profit per unit change”
  (never margin expansion / compression).
- Always refer to profit as **CM1 profit**.

────────────────────────────────────────
METRICS REFERENCE (EXECUTIVE GLOSSARY — REFERENCE ONLY)
────────────────────────────────────────

The following definitions describe the exact business meaning
of each metric used in this analysis.

These definitions are provided for interpretation clarity only.
They do NOT imply priority, importance, or mandatory discussion.

UNITS
- quantity:
  Gross units sold before returns.
- return_quantity:
  Units returned by customers.
- total_quantity:
  Net units sold after subtracting returns from gross units.

SALES & REVENUE
- gross_sales:
  Total gross sales including product sales, sales tax,
  promotional rebates, postage credits, shipping credits,
  and related tax components.
- refund_sales:
  Sales value associated with refunded orders.
- net_sales:
  Gross sales minus refund sales.
  Represents realised topline revenue.

FEES, TAXES & ADJUSTMENTS
- platformfeenew:
  Amazon platform charges (UK).
  Overall account-level charge with no product-wise breakdown.
- platform_fee_inventory_storage:
  Amazon warehouse storage fees.
  Overall account-level charge with no product-wise breakdown.
- other_transaction_fees:
  Amazon Seller Central and account-level fees.
  These are NOT CM2 profit drivers.
- misc_transaction:
  Unclassified or newly introduced Amazon charges.
  These are NOT CM2 profit drivers.
- net_taxes:
  Aggregate tax charges including marketplace taxes,
  sales tax, promotional rebate tax, shipping and giftwrap tax.
- net_credits:
  Credits such as postage and giftwrap credits.

ADVERTISING
- advertising_total:
  Total advertising spend for the month.
  Overall value with no product-wise breakdown.
- acos:
  Advertising cost of sales.
  Treated strictly as a percentage-point efficiency metric.

REIMBURSEMENTS
- lost_total:
  Reimbursement received for lost or damaged inventory.
  Added to CM2 profit.
  Represents recovery, not performance.
- rembursement_fee:
  Reimbursement amounts transferred during Amazon’s
  15-day settlement cycle.

PROFITABILITY
- profit:
  Contribution Margin 1 (CM1) profit.
- cm2_profit:
  Contribution Margin 2 (CM2) profit.
  Derived ONLY from CM1 profit after advertising,
  platform fees, storage fees, and reimbursements.
  Overall value without product-wise breakdown.

PRICING & PER-UNIT ECONOMICS
- asp:
  Average selling price.
- unit_wise_profitability:
  CM1 profit per unit.

IMPORTANT:
- Percentage metrics represent percentage-point values.
- Metrics without product-wise breakdown must never be
  attributed to individual SKUs.

────────────────────────────────────────
CM2 ATTRIBUTION CONSTRAINT (CRITICAL)
────────────────────────────────────────
- CM2 profit movement MUST be attributed ONLY to:
  advertising_total,
  platformfeenew,
  platform_fee_inventory_storage,
  and lost_total.
- other_transaction_fees and misc_transaction
  MUST NEVER be cited as CM2 profit drivers.
- If CM2 movement is not fully explained by the allowed components,
  do NOT infer or introduce additional cost drivers.

────────────────────────────────────────
MANDATORY OUTPUT FORMAT (STRICT JSON ONLY)
────────────────────────────────────────
- ALL fields shown below are REQUIRED.
- If any field is missing, the response is INVALID.
- The "executive_takeaway" field MUST be populated with text.


────────────────────────────────────────
ALLOWED DIAGNOSIS CODES (STRICT)
────────────────────────────────────────

The following diagnosis codes are ALLOWED.
You MUST select from this list only.

- pricing_supports_volume
  (unit growth positive, CM1 profit per unit declining)

- pricing_effective
  (unit growth positive, CM1 profit stable or growing)

- demand_weakness
  (units and net sales declining)

- visibility_constraint
  (units declining despite stable or reduced pricing)

- mixed_signal
  (no dominant pricing or demand signal)

Each SKU may have:
- 1 primary diagnosis
- Maximum 2 diagnosis codes


Return a single JSON object with the following structure (STRICT JSON):

{
  "executive_summary_signals": {
    "units": {
      "direction": "increase | decrease | flat",
      "severity": "highest_24m | lowest_24m | normal",
      "pct_change": "number",
      "absolute_change": "number"
    },
    "net_sales": {
      "pct_change": "number",
      "absolute_change": "number",
      "severity": "highest_24m | lowest_24m | normal"
    },
    "asp": {
      "pct_change": "number",
      "absolute_change": "number",
      "severity": "largest_24m | normal"
    },
    "cm1_profit": {
      "pct_change": "number",
      "absolute_change": "number",
      "severity": "highest_24m | lowest_24m | normal"
    },
    "cm1_profit_per_unit": {
      "pct_change": "number",
      "absolute_change": "number",
      "severity": "largest_24m | normal"
    },
    "cost_pressure": {
      "advertising": {
        "pct_change": "number",
        "absolute_change": "number",
        "acos_delta": "number | null",
        "severity": "largest_24m | normal"
      },
      "storage_fees": {
        "pct_change": "number",
        "absolute_change": "number",
        "severity": "largest_24m | normal"
      }
    },
    "cm2_profit": {
      "pct_change": "number",
      "absolute_change": "number",
      "severity": "largest_24m | normal"
    },
    "reimbursements": {
      "present": true | false,
      "amount": "number | null"
    }
  },
  "primary_causal_chain": [
    "asp_decrease",
    "unit_growth",
    "net_sales_growth",
    "per_unit_profit_decline",
    "cost_pressure",
    "cm2_profit_decline"
  ],
  "executive_takeaway": "string (max 2 sentences, derived ONLY from primary_causal_chain, no actions)",
  "product_insights": {
    "<sku>": {
      "diagnosis_codes": [
        "pricing_supports_volume"
      ]
    }
  }
}




"""



AI_SYSTEM_PROMPT_2 = """
You are a strategic Amazon business decision engine operating at
executive decision-making level.

You are NOT an analyst.
You are NOT a reporting engine.
You do NOT explain performance.
You convert validated analysis into disciplined business decisions.

────────────────────────────────────────
INPUTS YOU WILL RECEIVE
────────────────────────────────────────

1) analysis_insights
- These are final, analyst-grade findings.
- Each insight already contains WHAT changed, WHY it changed, and WHAT it impacted.
- All insights are factual, pre-validated, and must be treated as true.
- You MUST NOT reinterpret, restate, or challenge these insights.
- You MUST NOT introduce new causal language beyond what is explicitly supported.

2) user_objective
A structured decision mandate defining how decisions MUST be made.

The user_objective includes:

- primary_goal:
  profit | growth | rank | inventory_clearance | balanced

- time_horizon:
  2_weeks | 1_month | quarter

- risk_level:
  conservative | balanced | aggressive

- constraints:
  Hard limits that MUST NOT be violated.
  Examples:
  - dont_change_price
  - max_price_increase_pct
  - ad_budget_cap
  - max_tacos

- notes:
  Optional qualitative context.
  Use ONLY if explicitly relevant.

────────────────────────────────────────
YOUR TASK
────────────────────────────────────────

Translate the analysis_insights into a prioritized,
decision-ready ACTION PLAN that:

- STRICTLY follows user_objective.primary_goal
- Respects ALL constraints without exception
- Adjusts aggressiveness based on risk_level
- Prioritizes actions based on time_horizon
- Avoids any action that conflicts with the mandate

You are producing executive decisions,
not explanations, analysis, or strategy discussion.

If an action does not clearly support the user_objective,
it MUST NOT be included.

────────────────────────────────────────
MANDATORY OBJECTIVE ENFORCEMENT (CRITICAL)
────────────────────────────────────────

- Every recommended action MUST explicitly support user_objective.primary_goal.
- Every action MUST reference at least one SKU from focus_skus.
- Generic or portfolio-wide actions without justification are INVALID.
- Each SKU may receive ONLY ONE dominant action.

────────────────────────────────────────
DECISION QUALITY RULES (CRITICAL)
────────────────────────────────────────

- Every action MUST be traceable to a specific driver in analysis_insights
  (e.g., CM1 profit decline, per-unit profitability erosion, demand slowdown).
- Do NOT restate analysis_insights.
- Convert insight → decision directly.
- Focus ONLY on controllable levers:
  pricing direction, portfolio-level advertising, SKU focus, inventory exposure.
- Do NOT include numeric targets, percentages, quantities, or timing.
- All actions MUST be directional only.

────────────────────────────────────────
PRICING ACTION DIRECTION (MANDATORY)
────────────────────────────────────────

If a pricing action is selected, you MUST use EXACTLY ONE
of the following phrases:

- “Increase ASP”
- “Decrease ASP”
- “Maintain current pricing”

All other pricing phrases are STRICTLY FORBIDDEN.

NON-PRICING ACTION DIRECTION (ALLOWED — STRICT)

In specific cases, a SKU requires a non-pricing action.

Allowed non-pricing action (exact phrase):
- “Check product visibility”

This action is allowed ONLY when all of the following are true for a SKU:
- units are declining
- net sales are declining
- CM1 profit is declining
- AND ASP is declining

In this case:
- Do NOT return a pricing action
- Return exactly: “Check product visibility”


────────────────────────────────────────
PRICING DECISION HIERARCHY (CRITICAL)
────────────────────────────────────────

Pricing actions MUST be driven by PROFITABILITY and DEMAND,
not by ASP movement alone.

Apply the following logic exactly:

1) Recommend “Increase ASP” if:
   - CM1 profit per unit is declining, AND
   - unit growth is positive
   (pricing is supporting volume but eroding profitability)

2) Recommend “Increase ASP” if:
   - CM1 profit is declining, AND
   - analysis_insights identify pricing as a contributor to profit erosion

3) Recommend “Maintain current pricing” if:
   - ASP declined, BUT
   - unit growth is positive, AND
   - CM1 profit is stable or growing
   (pricing is effective at driving profitable volume)

4) Recommend “Decrease ASP” ONLY IF:
   - units are declining, AND
   - net sales are declining, AND
   - ASP is stable or increasing

If ASP is already declining, this rule MUST NOT be applied.

ASP is a SUPPORTING signal.
ASP alone must NEVER trigger a pricing action.

────────────────────────────────────────
VISIBILITY VS PRICING RULE (CRITICAL)
────────────────────────────────────────
This rule MUST be evaluated BEFORE any pricing decision.
It OVERRIDES the pricing decision hierarchy.
This rule represents a FAILED PRICING RESPONSE.

If a SKU shows:
- declining units,
- declining net sales,
- declining CM1 profit,
- AND declining ASP,

This means:
- Pricing has already been reduced,
- Demand did NOT respond to lower pricing,
- Pricing is NOT the binding constraint.

In this case:
- Do NOT recommend any pricing action.
- You MUST NOT suggest further ASP reduction.
- You MUST NOT return “Maintain current pricing”.
- Return exactly: “Check product visibility”.


────────────────────────────────────────
PORTFOLIO-LEVEL ADVERTISING RULES
────────────────────────────────────────

- Advertising actions are allowed ONLY at the portfolio level.
- Do NOT reference specific SKUs in advertising actions.
- Do NOT reallocate ad spend between SKUs.
- Advertising actions must be justified by CM2 profit or efficiency erosion.

────────────────────────────────────────
PRIORITIZATION LOGIC
────────────────────────────────────────

- Address margin or cost leakage before pursuing incremental growth.
- Prefer lower-risk actions when primary_goal = profit.
- Avoid actions that materially damage CM1 profit unless explicitly required.

────────────────────────────────────────
CONSTRAINT ENFORCEMENT
────────────────────────────────────────

- If dont_change_price = true → Do NOT suggest pricing actions.
- If max_price_increase_pct is set → Respect it implicitly (no numeric output).
- If ad_budget_cap is set → Do NOT suggest expansion.
- If max_tacos is set → Avoid efficiency deterioration.

OUTPUT FORMAT (MANDATORY — STRICT JSON ONLY)

Return a single JSON object with the following structure:

{
  "sku_actions": {
      "<sku_name>": "Increase ASP | Decrease ASP | Maintain current pricing | Check product visibility"
  }
}

Rules:
- Each SKU from focus_skus may appear at most once.
- Each SKU must have exactly one action.
- Do NOT include explanations, reasoning, or commentary.
- Do NOT include portfolio-level sections.
- Do NOT include markdown.
If no pricing action is appropriate for a SKU, return
“Maintain current pricing”, UNLESS the visibility vs pricing rule applies,
in which case return “Check product visibility”.




"""


AI_SYSTEM_PROMPT_3_POLISHER = """
You are an executive copy POLISHER for a
MONTH-END, EXECUTIVE-LEVEL Amazon business review.

IMPORTANT ROLE CLARIFICATION (CRITICAL):
- You are NOT an analyst.
- You are NOT a strategist.
- You are NOT a report generator.
- The report content has already been fully constructed
  by a deterministic system.
- Your sole responsibility is to POLISH the language
  for executive readability WITHOUT altering meaning.

You are writing for:
Founder, CFO, Finance Leadership, and Senior Account Owners.

They expect:
- CFO-grade clarity
- Sharp, decisive phrasing
- Clean executive flow
- Zero ambiguity

────────────────────────────────────────
ABSOLUTE INPUT CONSTRAINT (CRITICAL)
────────────────────────────────────────
You will receive a STRUCTURED OBJECT containing:
- Pre-written SUMMARY bullets
- Pre-written PRODUCT INSIGHTS bullets
- Pre-written RECOMMENDATIONS bullets (optional)
- Pre-written INVENTORY bullets (optional)

These bullets are FINAL in:
- Logic
- Ordering
- Metrics
- Causality
- Scope

You MUST treat all content as FACTUAL and FINAL.

────────────────────────────────────────
WHAT YOU ARE ALLOWED TO DO
────────────────────────────────────────
- Improve sentence sharpness and concision
- Improve executive tone
- Remove redundant filler words
- Improve grammatical clarity
- Improve financial phrasing consistency

────────────────────────────────────────
WHAT YOU ARE STRICTLY FORBIDDEN TO DO
────────────────────────────────────────
- Do NOT add new bullets
- Do NOT remove bullets
- Do NOT reorder bullets
- Do NOT merge or split bullets
- Do NOT add or remove numbers
- Do NOT change any numeric value
- Do NOT change any percentage
- Do NOT change causal logic
- Do NOT reinterpret insights
- Do NOT soften or exaggerate claims
- Do NOT introduce new risks, drivers, or explanations

If a bullet appears awkward, you may ONLY improve wording —
never substance.

────────────────────────────────────────
NUMERIC INTEGRITY RULE (NON-NEGOTIABLE)
────────────────────────────────────────
- All numbers must remain EXACTLY the same.
- All percentage signs must remain.
- All + / - signs must remain.
- Currency symbols must remain unchanged.
- If any number is changed, the output is INVALID.

────────────────────────────────────────
STYLE REQUIREMENT
────────────────────────────────────────
- Maintain a **Month-End Business Summary** tone.
- Use assertive, finance-review language.
- Prefer short, declarative sentences.
- Avoid narrative or descriptive storytelling.
- Avoid analyst-style hedging.

────────────────────────────────────────
SECTION PRESERVATION RULE (CRITICAL)
────────────────────────────────────────
- Preserve ALL section headings exactly:
  ## SUMMARY
  ## PRODUCT INSIGHTS
  ## RECOMMENDATIONS
  ## INVENTORY
- Do NOT rename sections.
- Do NOT add new sections.
- Do NOT remove empty sections if present.

────────────────────────────────────────
OUTPUT FORMAT (MANDATORY)
────────────────────────────────────────
- Return MARKDOWN only.
- Preserve bullet structure.
- Preserve section order.
- Return the SAME number of bullets per section.
- No emojis.
- No commentary.
- No explanations.
- Output ONLY the polished report.

If you cannot improve a bullet without changing meaning,
return it unchanged.


"""

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

def run_prompt_2_strategy(insights_text, user_objective, focus_skus):
    payload = {
    "analysis_insights": insights_text,
    "user_objective": user_objective,
    "focus_skus": focus_skus,
    }

    resp = openai_client.chat.completions.create(
        model="gpt-4.1",
        messages=[
            {"role": "system", "content": AI_SYSTEM_PROMPT_2},
            {"role": "user", "content": json.dumps(payload, separators=(",", ":"))}
        ],
        temperature=0.3,
    )
    return resp.choices[0].message.content.strip()

def run_prompt_3_polish(bullets: dict) -> dict:
    resp = openai_client.chat.completions.create(
        model="gpt-4.1",
        messages=[
            {"role": "system", "content": AI_SYSTEM_PROMPT_3_POLISHER},
            {"role": "user", "content": json.dumps(bullets, separators=(",", ":"))}
        ],
        temperature=0.0,
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


DIAGNOSIS_TEXT = {
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
    ]
}




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
    Deterministic executive month-end summary renderer.
    Presentation-only. Fully None-safe.
    """

    def fmt_pct(x):
        return f"{x:+.2f}%" if isinstance(x, (int, float)) else "N/A"

    def fmt_num(x, decimals=2):
        return f"{x:+.{decimals}f}" if isinstance(x, (int, float)) else "N/A"

    def fmt_int(x):
        return f"{int(x)}" if isinstance(x, (int, float)) else "N/A"

    comparison = build_comparison_label(period, timeline, year)
    lines: list[str] = []

    # =========================================================
    # SUMMARY — PORTFOLIO ONLY
    # =========================================================
    if analysis_insights is not None:
        lines.append("## SUMMARY")

        es = analysis_insights.get("executive_summary_signals", {})

        lines.append(f"Performance Summary ({comparison})")
        takeaway = analysis_insights.get("executive_takeaway")
        if takeaway:
            lines.append(takeaway)

        # Units
        u = es.get("units", {})
        lines.append(
            f"• Units sold: {fmt_pct(u.get('pct_change'))} "
            f"({fmt_int(u.get('absolute_change'))} units)"
            f"{severity_suffix(u.get('severity'))}"
        )

        # Net sales
        ns = es.get("net_sales", {})
        lines.append(
            f"• Net sales: {fmt_pct(ns.get('pct_change'))} "
            f"({currency_symbol}{fmt_num(ns.get('absolute_change'))})"
            f"{severity_suffix(ns.get('severity'))}"
        )
    

        # ASP
        asp = es.get("asp", {})
        lines.append(
            f"• ASP: {fmt_pct(asp.get('pct_change'))} "
            f"({currency_symbol}{fmt_num(asp.get('absolute_change'))})"
            f"{severity_suffix(asp.get('severity'))}"
        )

        # CM1 profit
        cm1 = es.get("cm1_profit", {})
        lines.append(
            f"• CM1 profit: {fmt_pct(cm1.get('pct_change'))} "
            f"({currency_symbol}{fmt_num(cm1.get('absolute_change'))})"
            f"{severity_suffix(cm1.get('severity'))}"
        )

        # CM1 profit per unit
        ppu = es.get("cm1_profit_per_unit", {})
        lines.append(
            f"• CM1 profit per unit: {fmt_pct(ppu.get('pct_change'))} "
            f"({currency_symbol}{fmt_num(ppu.get('absolute_change'))})"
            f"{severity_suffix(ppu.get('severity'))}"
        )

        # Cost pressure – Advertising
        cp = es.get("cost_pressure", {})
        ad = cp.get("advertising", {})
        lines.append(
            f"• Advertising spends: {fmt_pct(ad.get('pct_change'))} "
            f"({currency_symbol}{fmt_num(ad.get('absolute_change'))})"
            f"{severity_suffix(ad.get('severity'))}, "
            f"with ACOS change of {fmt_pct(ad.get('acos_delta'))}"
        )

        # Cost pressure – Storage
        st = cp.get("storage_fees", {})
        lines.append(
            f"• Platform inventory storage fees: {fmt_pct(st.get('pct_change'))} "
            f"({currency_symbol}{fmt_num(st.get('absolute_change'))})"
            f"{severity_suffix(st.get('severity'))}"
        )

        # CM2 profit
        cm2 = es.get("cm2_profit", {})
        lines.append(
            f"• CM2 profit: {fmt_pct(cm2.get('pct_change'))} "
            f"({currency_symbol}{fmt_num(cm2.get('absolute_change'))})"
            f"{severity_suffix(cm2.get('severity'))}"
        )

        # Reimbursements
        reimb = es.get("reimbursements", {})
        if reimb.get("present"):
            amt = reimb.get("amount")
            if isinstance(amt, (int, float)):
                lines.append(
                    f"• Amazon reimbursements for lost inventory: "
                    f"{currency_symbol}{abs(amt):.2f} (non-recurring recovery)"
                )

    # =========================================================
    # PRODUCT INSIGHTS (ALWAYS SHOWN)
    # =========================================================
    lines.append("\n## PRODUCT INSIGHTS")

    product_insights = (
        analysis_insights.get("product_insights", {})
        if analysis_insights is not None
        else {}
    )

    for sku in focus_skus:
        s = sku_mom.get(sku)
        if not s:
            continue

        name = s.get("product_name", sku)
        lines.append(f"\n{name}")

        def sku_fmt(metric, key="delta", pct_key="delta_pct"):
            m = s.get(metric, {})
            return f"{fmt_num(m.get(key))} ({fmt_pct(m.get(pct_key))})"

        lines.append(f"• ASP: {currency_symbol}{sku_fmt('asp')}")
        lines.append(f"• Units: {sku_fmt('total_quantity', key='delta', pct_key='delta_pct')}")
        lines.append(f"• Net sales: {currency_symbol}{sku_fmt('net_sales')}")
        lines.append(f"• CM1 profit: {currency_symbol}{sku_fmt('profit')}")
        lines.append(
            f"• CM1 profit per unit: {currency_symbol}"
            f"{sku_fmt('unit_wise_profitability')}"
        )

        sku_diag = product_insights.get(sku, {})
        diagnosis_codes = sku_diag.get("diagnosis_codes", ["mixed_signal"])

        for code in diagnosis_codes:
            for line in DIAGNOSIS_TEXT.get(code, []):
                lines.append(line)

        if strategy_actions and sku in strategy_actions:
            lines.append(f"Action: {strategy_actions[sku]} for {name}")

    # =========================================================
    # INVENTORY (PORTFOLIO ONLY)
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




# def get_or_create_summary(
#     user_id,
#     country,
#     marketplace_id,
#     period,
#     timeline,
#     year,
#     objective=None,
#     target_sku: str | list | None = None,
#     force_regenerate=False
# ):
#     # ---------------- Objective defaults / normalize ----------------
#     objective = objective or {}
#     constraints = objective.get("constraints") or {}

#     primary_goal = objective.get("primary_goal", "profit")
#     risk_level = objective.get("risk_level", "balanced")

#     max_tacos = constraints.get("max_tacos")
#     max_price_increase_pct = constraints.get("max_price_increase_pct")
#     ad_budget_cap = constraints.get("ad_budget_cap")
#     dont_change_price = constraints.get("dont_change_price", False)
#     notes = objective.get("notes")

#     # ---------------- Cache lookup ----------------
#     cached = fetch_existing_summary(
#         user_id, country, marketplace_id, period, timeline, year
#     )

#     if cached and not force_regenerate:
#         return {
#             "summary": cached.summary,
#             "recommendations": cached.recommendations,
#             "source": "db",
#             "objective": {
#                 "primary_goal": getattr(cached, "primary_goal", None),
#                 "risk_level": getattr(cached, "risk_level", None),
#                 "constraints": {
#                     "max_tacos": getattr(cached, "max_tacos", None),
#                     "max_price_increase_pct": float(cached.max_price_increase_pct)
#                         if getattr(cached, "max_price_increase_pct", None) is not None else None,
#                     "ad_budget_cap": float(cached.ad_budget_cap)
#                         if getattr(cached, "ad_budget_cap", None) is not None else None,
#                     "dont_change_price": getattr(cached, "dont_change_price", None),
#                 },
#                 "notes": getattr(cached, "notes", None)
#             }
#         }

#     # ---------------- CURRENT PERIOD (SAFE LATEST) ----------------
#     year, month = resolve_latest_available_month(user_id, country)
#     timeline = str(month)
#     period = "monthly"
#     allow_reco = is_latest_period(period, timeline, year)

#     df_current = fetch_precalc_table(user_id, country, period, timeline, year)
#     df_current_detail, df_current_total = _split_total_row(df_current)

#     # ---------------- ROLLING CONTEXT ----------------
#     anchor_month = int(timeline)
#     rolling_series = build_rolling_monthly_series(
#         user_id=user_id,
#         country=country,
#         anchor_year=year,
#         anchor_month=anchor_month
#     )
#     movement_context = build_movement_context(rolling_series)

#     # ---------------- CURRENT METRICS ----------------
#     current = {}
#     if not df_current_detail.empty:
#         for col in get_metric_columns(df_current_detail):
#             current[col.lower()] = round(
#                 float(pd.to_numeric(df_current_detail[col], errors="coerce").fillna(0).sum()), 2
#             )

#     for c in [
#         "platform_fee",
#         "platformfeenew",
#         "platform_fee_inventory_storage",
#         "advertising_total",
#         "acos",
#         "cm2_profit",
#         "misc_transaction",
#         "asp",
#         "unit_wise_profitability",
#     ]:
#         v = _total_value(df_current_total, c)
#         if v is not None:
#             current[c] = round(v, 2)

#     # ---------------- INVENTORY LOSS ----------------
#     lost_total_val = _total_value(df_current_total, "lost_total")
#     inventory_lost = round(abs(lost_total_val), 2) if lost_total_val is not None else 0.0

#     # ---------------- SKU CURRENT ----------------
#     sku_current = compute_sku_precalc(df_current_detail)

#     # ============================================================
#     # 🔒 SINGLE SKU / PRODUCT MODE (CRITICAL FIX)
#     # ============================================================
#     single_sku_mode = False

#     if target_sku:
#         single_sku_mode = True

#         if isinstance(target_sku, list):
#             target_sku = target_sku[0]

#         target_sku = str(target_sku).strip()

#         # CASE 1: Exact SKU
#         if target_sku in sku_current:
#             top_5_skus = [target_sku]
#             sku_current = {target_sku: sku_current[target_sku]}

#         else:
#             # CASE 2: Product name → map to SKUs
#             matched = {
#                 sku: v
#                 for sku, v in sku_current.items()
#                 if v.get("product_name", "").strip().lower() == target_sku.lower()
#             }

#             if not matched:
#                 return {
#                     "summary": f"I couldn’t find '{target_sku}' in the latest available month.",
#                     "recommendations": None,
#                     "inventory_lost": inventory_lost,
#                     "inventory_alerts": {},
#                     "sku_current": {},
#                     "sku_mom": {},
#                     "sku_yoy": None,
#                     "objective": None,
#                     "sku_actions": {},
#                     "source": "no_data",
#                 }

#             top_5_skus = list(matched.keys())
#             sku_current = matched

#     else:
#         top_5_skus = select_top_5_skus_by_current_cm1_profit(sku_current)

#     # ---------------- INVENTORY ALERTS ----------------
#     inventory_alerts = {}
#     if allow_reco:
#         inventory_aged_df = fetch_inventory_aged_by_user(user_id)
#         if not inventory_aged_df.empty:
#             inventory_alerts = build_inventory_alerts(inventory_aged_df)

#     # ---------------- PREVIOUS PERIOD ----------------
#     (p_period, p_timeline, p_year), _ = resolve_comparison(period, timeline, year)
#     df_prev = fetch_precalc_table(user_id, country, p_period, p_timeline, p_year)
#     df_prev_detail, df_prev_total = _split_total_row(df_prev)

#     sku_prev = compute_sku_precalc(df_prev_detail)
#     sku_mom = compare_sku_metrics(sku_current, sku_prev)

#     if single_sku_mode:
#         sku_mom = {k: sku_mom.get(k, {}) for k in top_5_skus}

#     # ---------------- AI PAYLOAD ----------------
#     ai_payload = {
#         "period": f"{period} {timeline} {year}",
#         "period_label": period_label(period, timeline, year),
#         "country": str(country).lower(),
#         "inventory_lost": inventory_lost,
#         "inventory_alerts": inventory_alerts,
#         "sku_mom": sku_mom,
#         "sku_yoy": None,
#         "focus_skus": top_5_skus,
#         "movement_context": movement_context,
#         "objective": {
#             "primary_goal": primary_goal,
#             "risk_level": risk_level,
#             "constraints": {
#                 "max_tacos": max_tacos,
#                 "max_price_increase_pct": max_price_increase_pct,
#                 "ad_budget_cap": ad_budget_cap,
#                 "dont_change_price": dont_change_price,
#             },
#             "notes": notes
#         }
#     }

#     # 🔒 IMPORTANT: prompt-1 ONLY for portfolio
#     if not single_sku_mode:
#         analysis_insights = json.loads(run_prompt_1_analysis(ai_payload))
#     else:
#         analysis_insights = {
#             "executive_takeaway": None,
#             "key_insights": []
#         }

#     sku_actions = {}
#     if allow_reco:
#         strategy_raw = run_prompt_2_strategy(
#             analysis_insights, ai_payload["objective"], top_5_skus
#         )
#         sku_actions = json.loads(strategy_raw).get("sku_actions") or {}

#     final_text = render_month_end_summary(
#         period=period,
#         timeline=timeline,
#         year=year,
#         analysis_insights=None if single_sku_mode else analysis_insights,
#         mom=None,
#         sku_mom=sku_mom,
#         focus_skus=top_5_skus,
#         inventory_alerts=inventory_alerts,
#         inventory_lost=inventory_lost,
#         currency_symbol="£" if country == "uk" else "$",
#         strategy_actions=sku_actions if allow_reco else None
#     )

#     save_summary_to_db({
#         "user_id": user_id,
#         "country": country,
#         "marketplace_id": marketplace_id,
#         "period": period,
#         "timeline": timeline,
#         "year": year,
#         "primary_goal": primary_goal,
#         "risk_level": risk_level,
#         "max_tacos": max_tacos,
#         "max_price_increase_pct": max_price_increase_pct,
#         "ad_budget_cap": ad_budget_cap,
#         "dont_change_price": dont_change_price,
#         "notes": notes,
#         "summary": final_text,
#         "recommendations": None,
#         "upsert": True
#     })

#     return {
#         "summary": final_text,
#         "recommendations": None,
#         "inventory_lost": inventory_lost,
#         "inventory_alerts": inventory_alerts,
#         "sku_current": sku_current,
#         "sku_mom": sku_mom,
#         "sku_yoy": None,
#         "objective": ai_payload["objective"],
#         "source": "ai",
#         "sku_actions": sku_actions,
#     }


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
    # ---------------- Objective defaults / normalize ----------------
    objective = objective or {}
    constraints = objective.get("constraints") or {}

    primary_goal = objective.get("primary_goal", "profit")
    risk_level = objective.get("risk_level", "balanced")

    max_tacos = constraints.get("max_tacos")
    max_price_increase_pct = constraints.get("max_price_increase_pct")
    ad_budget_cap = constraints.get("ad_budget_cap")
    dont_change_price = constraints.get("dont_change_price", False)
    notes = objective.get("notes")

    # ============================================================
    # Cache lookup — ONLY for portfolio
    # ============================================================
    cached = fetch_existing_summary(
        user_id, country, marketplace_id, period, timeline, year
    )

    if cached and not force_regenerate and not target_sku:
        return {
            "summary": cached.summary,
            "recommendations": cached.recommendations,
            "source": "db",
            "scope": "portfolio",
            "objective": {
                "primary_goal": getattr(cached, "primary_goal", None),
                "risk_level": getattr(cached, "risk_level", None),
                "constraints": {
                    "max_tacos": getattr(cached, "max_tacos", None),
                    "max_price_increase_pct": float(cached.max_price_increase_pct)
                        if getattr(cached, "max_price_increase_pct", None) is not None else None,
                    "ad_budget_cap": float(cached.ad_budget_cap)
                        if getattr(cached, "ad_budget_cap", None) is not None else None,
                    "dont_change_price": getattr(cached, "dont_change_price", None),
                },
                "notes": getattr(cached, "notes", None)
            }
        }

    # ---------------- CURRENT PERIOD (LATEST AVAILABLE DATA) ----------------
    year, month = resolve_latest_available_month(user_id, country)
    timeline = str(month)
    period = "monthly"
    allow_reco = is_latest_period(period, timeline, year)

    df_current = fetch_precalc_table(user_id, country, period, timeline, year)
    df_current_detail, df_current_total = _split_total_row(df_current)

    # ---------------- SKU CURRENT ----------------
    sku_current = compute_sku_precalc(df_current_detail)
    top_5_skus = select_top_5_skus_by_current_cm1_profit(sku_current)


    # ============================================================
    # 🔒 SINGLE SKU MODE — TRUST CHATBOT SKU (CRITICAL FIX)
    # ============================================================
    single_sku_mode = False
    scope = "portfolio"

    if target_sku:
        single_sku_mode = True
        scope = "sku"

        if isinstance(target_sku, list):
            target_sku = target_sku[0]

        target_sku = str(target_sku).strip()

        # 🔑 TRUST EXACT SKU ONLY — NO product_name mapping
        if target_sku in sku_current:
            top_5_skus = [target_sku]
            sku_current = {target_sku: sku_current[target_sku]}
        else:
            # ❌ SKU not present in analyst data → fail fast (NO portfolio fallback)
            return {
                "summary": f"I couldn’t find SKU '{target_sku}' in the latest available data.",
                "recommendations": None,
                "inventory_lost": 0.0,
                "inventory_alerts": {},
                "sku_current": {},
                "sku_mom": {},
                "sku_yoy": None,
                "objective": objective,
                "sku_actions": {},
                "scope": "sku",
                "source": "no_data",
            }
    else:
        top_5_skus = select_top_5_skus_by_current_cm1_profit(sku_current)

    # ---------------- ROLLING CONTEXT ----------------
    if single_sku_mode:
        movement_context = {}
    else:
        anchor_month = int(timeline)
        rolling_series = build_rolling_monthly_series(
            user_id=user_id,
            country=country,
            anchor_year=year,
            anchor_month=anchor_month
        )
        movement_context = build_movement_context(rolling_series)

    # ---------------- INVENTORY LOSS ----------------
    lost_total_val = _total_value(df_current_total, "lost_total")
    inventory_lost = round(abs(lost_total_val), 2) if lost_total_val is not None else 0.0
    if single_sku_mode:
        inventory_lost = 0.0

    # ---------------- INVENTORY ALERTS ----------------
    inventory_alerts = {}
    if allow_reco and not single_sku_mode:
        inventory_aged_df = fetch_inventory_aged_by_user(user_id)
        if not inventory_aged_df.empty:
            inventory_alerts = build_inventory_alerts(inventory_aged_df)

    # ---------------- PREVIOUS PERIOD ----------------
    (p_period, p_timeline, p_year), _ = resolve_comparison(period, timeline, year)
    df_prev = fetch_precalc_table(user_id, country, p_period, p_timeline, p_year)
    df_prev_detail, _ = _split_total_row(df_prev)

    sku_prev = compute_sku_precalc(df_prev_detail)
    sku_mom = compare_sku_metrics(sku_current, sku_prev)

    if single_sku_mode:
        sku_mom = {k: sku_mom.get(k, {}) for k in top_5_skus}

    # ---------------- AI PAYLOAD ----------------
    ai_payload = {
        "period": f"{period} {timeline} {year}",
        "period_label": period_label(period, timeline, year),
        "country": str(country).lower(),
        "inventory_lost": inventory_lost,
        "inventory_alerts": inventory_alerts,
        "sku_mom": sku_mom,
        "sku_yoy": None,
        "focus_skus": top_5_skus,
        "movement_context": movement_context,
        "scope": scope,
        "objective": {
            "primary_goal": primary_goal,
            "risk_level": risk_level,
            "constraints": {
                "max_tacos": max_tacos,
                "max_price_increase_pct": max_price_increase_pct,
                "ad_budget_cap": ad_budget_cap,
                "dont_change_price": dont_change_price,
            },
            "notes": notes
        }
    }

    # 🔒 Prompt-1 ONLY for portfolio
    analysis_insights = None
    if not single_sku_mode:
        analysis_insights = json.loads(run_prompt_1_analysis(ai_payload))

    # 🔒 Strategy ONLY for portfolio
    sku_actions = {}
    if allow_reco and not single_sku_mode and analysis_insights:
        strategy_raw = run_prompt_2_strategy(
            analysis_insights, ai_payload["objective"], top_5_skus
        )
        sku_actions = json.loads(strategy_raw).get("sku_actions") or {}

    # ---------------- RENDER ----------------
    final_text = render_month_end_summary(
        period=period,
        timeline=timeline,
        year=year,
        analysis_insights=None if single_sku_mode else analysis_insights,
        mom=None,
        sku_mom=sku_mom,
        focus_skus=top_5_skus,
        inventory_alerts=inventory_alerts,
        inventory_lost=inventory_lost,
        currency_symbol="£" if country == "uk" else "$",
        strategy_actions=sku_actions if not single_sku_mode else None
    )


    analysis_insights = json.loads(run_prompt_1_analysis(ai_payload))
    print("EXECUTIVE TAKEAWAY:", analysis_insights.get("executive_takeaway"))



    strategy = None
    if allow_reco:
        # backend-only objective for now
        strategy_raw = run_prompt_2_strategy(analysis_insights,ai_payload["objective"], top_5_skus)

        strategy_json = json.loads(strategy_raw)
        sku_actions = strategy_json.get("sku_actions")

    final_text = render_month_end_summary(period=period, timeline=timeline,year=year, analysis_insights=analysis_insights, mom=mom, sku_mom=sku_mom,focus_skus=top_5_skus, inventory_alerts=inventory_alerts,
        inventory_lost=inventory_lost,
        currency_symbol="£" if country == "uk" else "$",
        strategy_actions=sku_actions if allow_reco else None

    )


    # ---------------- SPLIT SUMMARY & RECOMMENDATIONS ----------------
    summary = final_text
    recommendations = None

    if allow_reco and "## RECOMMENDATIONS" in final_text:
        parts = final_text.split("## RECOMMENDATIONS", 1)
        summary = parts[0].strip()
        recommendations = parts[1].strip()

    ai_output = {
        "summary": summary,
        "recommendations": recommendations
    }

    # ---------------- SAVE/UPDATE DB (✅ with new columns) ----------------
    # If row exists and we regenerated, update it; else insert new.
    save_summary_to_db({
        "user_id": user_id,
        "country": country,
        "marketplace_id": marketplace_id,
        "period": period,
        "timeline": timeline,
        "year": year,
        "primary_goal": primary_goal,
        "risk_level": risk_level,
        "max_tacos": max_tacos,
        "max_price_increase_pct": max_price_increase_pct,
        "ad_budget_cap": ad_budget_cap,
        "dont_change_price": dont_change_price,
        "notes": notes,
        "summary": final_text,
        "recommendations": None,
        "upsert": True
    })

    print("\n[DEBUG][SUMMARY_RETURN]")
    print("scope =", scope)
    print("focus_skus =", top_5_skus)

    return {
        "summary": final_text,
        "recommendations": None,
        "inventory_lost": inventory_lost,
        "inventory_alerts": inventory_alerts,
        "sku_current": sku_current,
        "sku_mom": sku_mom,
        "sku_yoy": None,
        "objective": ai_payload["objective"],
        "sku_actions": sku_actions,
        "scope": scope,
        "source": "ai",
    }
