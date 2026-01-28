import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from config import Config
from datetime import date
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
    "time_horizon": "1_month",  # 2_weeks | 1_month | quarter
    "risk_level": "balanced",   # conservative | balanced | aggressive
    "constraints": {
        "max_tacos": None,
        "max_price_increase_pct": None,
        "ad_budget_cap": None,
        "dont_change_price": False
    },
    "notes": None
}


def get_latest_completed_month(today=None):
    today = today or date.today()
    if today.month == 1:
        return today.year - 1, 12
    return today.year, today.month - 1


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


def save_summary_to_db(data):
    record = HistoricAISummary(
        user_id=data["user_id"],
        country=data["country"],
        marketplace_id=data["marketplace_id"],
        period=data["period"],
        timeline=data["timeline"],
        year=data["year"],
        summary=data["summary"],
        recommendations=data["recommendations"]
    )
    db.session.add(record)
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

You MUST express insights as structured classifications,
NOT as written sentences.

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
CRITICAL OUTPUT CONSTRAINT (NON-NEGOTIABLE)
────────────────────────────────────────
- You MUST NOT write prose, sentences, bullets, or paragraphs.
- You MUST NOT format output for presentation.
- You MUST NOT explain insights in natural language.
- You MUST output STRICT JSON ONLY.
- Any response that is not valid JSON is INVALID.


────────────────────────────────────────
FORBIDDEN CONTENT (ABSOLUTE)
────────────────────────────────────────
- No recommendations
- No actions
- No strategy
- No future suggestions
- No soft or narrative language
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

Return a single JSON object with the following structure:

{
  "overall_insights": {
    "units": {
      "movement": "increase | decrease | flat",
      "severity": "highest_24m | lowest_24m | normal",
      "driver": "unit_growth | pricing | mix",
      "material": true | false
    },
    "net_sales": {
      "movement": "increase | decrease | flat",
      "severity": "highest_24m | lowest_24m | normal",
      "driver": "unit_growth | pricing | mix",
      "material": true | false
    },

    },
    "asp": {
      "movement": "increase | decrease | flat",
      "severity": "steepest_24m | normal",
      "material": true | false
    },
    "cm1_profit": {
      "movement": "increase | decrease | flat",
      "offset": true | false,
      "material": true | false
    },
    "cm1_profit_per_unit": {
      "movement": "increase | decrease | flat",
      "severity": "largest_24m | normal",
      "material": true | false
    },
    "cm2_profit": {
      "movement": "increase | decrease | flat",
      "drivers": ["advertising_total", "platform_fee_inventory_storage", "platformfeenew"],
      "material": true | false
    },
    "lost_total": {
      "present": true | false,
      "material": true | false
    },
    "acos": {
      "movement": "increase | decrease | flat",
      "material": true | false
    },
    "business_quality_risk": {
     "present": true | false,
        "drivers": ["asp_compression", "per_unit_profit_decline", "cost_pressure"]
    }
  },

  "product_insights": {
    "<sku>": {
      "asp_effective": true | false,
      "units_up": true | false,
      "profit_up": true | false,
      "profit_down": true | false,
      "primary_driver": "pricing | units | visibility"
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

4) Recommend “Decrease ASP” if:
   - units are declining, AND
   - net sales are declining
   (demand recovery required)

ASP is a SUPPORTING signal.
ASP alone must NEVER trigger a pricing action.

────────────────────────────────────────
VISIBILITY VS PRICING RULE (CRITICAL)
────────────────────────────────────────

If a SKU shows:
- declining units,
- declining net sales,
- declining CM1 profit,
- AND declining ASP,

THEN:
- Do NOT recommend pricing actions.
- Classify the issue as demand or visibility related.
- Recommend monitoring or focus actions instead.

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

────────────────────────────────────────
OUTPUT FORMAT (MANDATORY — MARKDOWN ONLY)
────────────────────────────────────────

## ACTION_PLAN
(5-7 bullets maximum)

For each action:
- First line: WHAT to do (directional action only)
- Second line: WHY (single sentence referencing the driver)

No additional explanation is permitted.

## AVOID
(2 bullets maximum)

Actions that conflict with the user_objective
or violate constraints.


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
        "insights": insights_text,
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


def render_month_end_summary(
    *,
    period: str,
    timeline: str,
    year: int,
    analysis_insights: dict,
    mom: dict,
    sku_mom: dict,
    focus_skus: list,
    inventory_alerts: dict,
    inventory_lost: float,
    currency_symbol: str,
    strategy_actions: str | None
) -> str:
    """
    Deterministic executive month-end summary renderer.
    """

    comparison = build_comparison_label(period, timeline, year)
    lines: list[str] = []

    # =========================
    # SUMMARY
    # =========================
    lines.append("## SUMMARY")

    oi = analysis_insights["overall_insights"]

    # Units
    if oi["units"]["material"]:
        lines.append(
            f"- {comparison}, total units sold increased by "
            f"{mom['total_quantity']['delta_pct']:+.2f}%, "
            f"marking the highest unit growth in the last 24 months."
        )

    # Net sales + ASP
    if oi["net_sales"]["material"] and oi["asp"]["material"]:
        lines.append(
            f"- {comparison}, net sales changed by "
            f"{mom['net_sales']['delta_pct']:+.2f}% "
            f"({currency_symbol}{mom['net_sales']['delta']:+.2f}), "
            f"driven by an ASP change of "
            f"{mom['asp']['delta_pct']:+.2f}%, "
            f"the steepest ASP movement in the 24-month rolling window."
        )

    # CM1 profit
    if oi["cm1_profit"]["material"]:
        lines.append(
            f"- {comparison}, CM1 profit changed by "
            f"{mom['profit']['delta_pct']:+.2f}% "
            f"({currency_symbol}{mom['profit']['delta']:+.2f}), "
            f"as pricing and per-unit economics offset unit momentum."
        )

    # CM1 profit per unit
    if oi["cm1_profit_per_unit"]["material"]:
        lines.append(
            f"- {comparison}, CM1 profit per unit declined by "
            f"{mom['unit_wise_profitability']['delta_pct']:+.2f}%, "
            f"representing the largest per-unit profitability erosion "
            f"in the last 24 months."
        )

    # CM2 profit (portfolio level only)
    if "cm2_profit" in mom and oi["cm2_profit"]["material"]:
        ad_delta = mom.get("advertising_total", {}).get("delta")
        storage_delta = mom.get("platform_fee_inventory_storage", {}).get("delta")

        drivers = []
        if ad_delta is not None:
            drivers.append(f"higher advertising spend ({currency_symbol}{ad_delta:+.2f})")
        if storage_delta is not None:
            drivers.append(f"increased storage fees ({currency_symbol}{storage_delta:+.2f})")

        driver_text = " and ".join(drivers) if drivers else "cost increases"

        lines.append(
            f"- {comparison}, CM2 profit declined by "
            f"{mom['cm2_profit']['delta_pct']:+.2f}% "
            f"({currency_symbol}{mom['cm2_profit']['delta']:+.2f}), "
            f"driven by {driver_text}."
        )

    # Reimbursements
    if oi["lost_total"]["present"]:
        lines.append(
            f"- {comparison}, Amazon reimbursed "
            f"{currency_symbol}{inventory_lost:.2f} "
            f"for lost or damaged inventory, representing recovery rather than core performance."
        )

    # Business quality risk
    if oi["business_quality_risk"]["present"]:
        lines.append(
            f"- {comparison}, business quality risk increased as ASP compression "
            f"directly drove a sharp decline in CM1 profit per unit, "
            f"compounded by rising costs."
        )

    # =========================
    # PRODUCT INSIGHTS
    # =========================
    lines.append("\n## PRODUCT INSIGHTS")

    for sku in focus_skus:
        s = sku_mom.get(sku)
        if not s:
            continue

        name = s.get("product_name", sku)

        lines.append(
            f"- **{name}:** "
            f"ASP changed by {currency_symbol}{s['asp']['delta']:+.2f} "
            f"({s['asp']['delta_pct']:+.2f}%), "
            f"units changed by {s['total_quantity']['delta']:+.0f} "
            f"({s['total_quantity']['delta_pct']:+.2f}%), "
            f"net sales changed by {currency_symbol}{s['net_sales']['delta']:+.2f} "
            f"({s['net_sales']['delta_pct']:+.2f}%), "
            f"CM1 profit changed by {currency_symbol}{s['profit']['delta']:+.2f} "
            f"({s['profit']['delta_pct']:+.2f}%)."
        )

    # =========================
    # RECOMMENDATIONS
    # =========================
    if strategy_actions:
        lines.append("\n## RECOMMENDATIONS")
        lines.append(strategy_actions.strip())

    # =========================
    # INVENTORY
    # =========================
    if inventory_alerts:
        lines.append("\n## INVENTORY")

        if "aged_inventory_181_plus" in inventory_alerts:
            lines.append(
                f"- Inventory - Aged inventory 181+ days: "
                f"{inventory_alerts['aged_inventory_181_plus']['total_units']} units present, "
                f"representing elevated working capital risk."
            )

        if "unfulfillable_inventory" in inventory_alerts:
            lines.append(
                f"- Inventory – Unfulfillable inventory: "
                f"{inventory_alerts['unfulfillable_inventory']['total_units']} units at risk of write-off."
            )

        if "storage_cost_risk" in inventory_alerts:
            lines.append(
                f"- Inventory - Storage cost risk: "
                f"Estimated next month storage cost of "
                f"{currency_symbol}{inventory_alerts['storage_cost_risk']['estimated_next_month_cost']:.2f}, "
                f"representing a forward-looking cost risk."
            )

    return "\n".join(lines)




def get_or_create_summary(
    user_id,
    country,
    marketplace_id,
    period,
    timeline,
    year
):
    cached = fetch_existing_summary(
        user_id, country, marketplace_id, period, timeline, year
    )

    if cached:
        return {
            "summary": cached.summary,
            "recommendations": cached.recommendations,
            "source": "db"
        }

    allow_reco = is_latest_period(period, timeline, year)

    # ---------------- CURRENT PERIOD ----------------
    df_current = fetch_precalc_table(user_id, country, period, timeline, year)
    df_current_detail, df_current_total = _split_total_row(df_current)

    # ---------------- ROLLING 24-MONTH MOVEMENT CONTEXT (NEW) ----------------
    if period == "monthly":
        anchor_month = int(timeline)
    elif period == "quarterly":
        anchor_month = int(str(timeline).replace("Q", "")) * 3   # Q1->3, Q2->6, Q3->9, Q4->12
    elif period == "yearly":
        anchor_month = 12
    else:
        anchor_month = int(timeline)

    rolling_series = build_rolling_monthly_series(
        user_id=user_id,
        country=country,
        anchor_year=year,
        anchor_month=anchor_month
    )

    movement_context = build_movement_context(rolling_series)


    # 1) Build overall metrics from DETAIL rows (prevents double counting TOTAL)
    current = {}
    if not df_current_detail.empty:
        for col in get_metric_columns(df_current_detail):
            current[col.lower()] = round(
                float(pd.to_numeric(df_current_detail[col], errors="coerce").fillna(0).sum()), 2
            )

    # 2) Override overall-only metrics from TOTAL row (because detail rows may be zero)
    for c in [
        "platform_fee",
        "platformfeenew",
        "platform_fee_inventory_storage",
        "advertising_total",
        "acos",
        "cm2_profit",
        "misc_transaction",
        "asp",
        "unit_wise_profitability",
    ]:
        v = _total_value(df_current_total, c)
        if v is not None:
            current[c] = round(v, 2)

    print("METRICS USED:", list(current.keys()))

    # 3) SKU breakdown MUST use DETAIL rows only (exclude TOTAL row)
    sku_current = compute_sku_precalc(df_current_detail)
    top_5_skus = select_top_5_skus_by_current_cm1_profit(sku_current)


    # 4) Reimbursements: take from TOTAL row if available, else detail sum
    lost_total_val = _total_value(df_current_total, "lost_total")
    if lost_total_val is None:
        inventory_lost = round(abs(current.get("lost_total", 0.0)), 2)
    else:
        inventory_lost = round(abs(lost_total_val), 2)


    inventory_alerts = {}

    # =====================================================
    # 🔴 INVENTORY AGEING LOGIC (latest period only) 🔴
    # =====================================================
    if allow_reco:
        inventory_aged_df = fetch_inventory_aged_by_user(user_id)
        if not inventory_aged_df.empty:
            inventory_alerts = build_inventory_alerts(inventory_aged_df)
        else:
            inventory_alerts = {}

    # ---------------- PREVIOUS PERIOD (MoM / QoQ) ----------------
    (p_period, p_timeline, p_year), yoy_key = resolve_comparison(period, timeline, year)

    df_prev = fetch_precalc_table(user_id, country, p_period, p_timeline, p_year)
    df_prev_detail, df_prev_total = _split_total_row(df_prev)

    prev = {}
    if not df_prev_detail.empty:
        for col in get_metric_columns(df_prev_detail):
            prev[col.lower()] = round(
                float(pd.to_numeric(df_prev_detail[col], errors="coerce").fillna(0).sum()), 2
            )

    # override overall-only metrics from TOTAL row
    for c in [
        "platform_fee",
        "platformfeenew",
        "platform_fee_inventory_storage",
        "advertising_total",
        "acos",
        "cm2_profit",
        "misc_transaction",
        "asp",
        "unit_wise_profitability",
    ]:
        v = _total_value(df_prev_total, c)
        if v is not None:
            prev[c] = round(v, 2)

    mom = compare_metrics(current, prev)

    sku_prev = compute_sku_precalc(df_prev_detail)
    sku_mom = compare_sku_metrics(sku_current, sku_prev)


   
    # ---------------- YOY (YEARLY ONLY) ----------------
    yoy = None
    sku_yoy = None

    if period == "yearly" and yoy_key:
        y_period, y_timeline, y_year = yoy_key
        df_yoy = fetch_precalc_table(user_id, country, y_period, y_timeline, y_year)

        if not df_yoy.empty:
            df_yoy_detail, df_yoy_total = _split_total_row(df_yoy)

            yoy_base = {}
            if not df_yoy_detail.empty:
                for col in get_metric_columns(df_yoy_detail):
                    yoy_base[col.lower()] = round(
                        float(pd.to_numeric(df_yoy_detail[col], errors="coerce").fillna(0).sum()), 2
                    )

            for c in [
                "platform_fee",
                "platformfeenew",
                "platform_fee_inventory_storage",
                "advertising_total",
                "acos",
                "cm2_profit",
                "misc_transaction",
                "asp",
                "unit_wise_profitability",
            ]:
                v = _total_value(df_yoy_total, c)
                if v is not None:
                    yoy_base[c] = round(v, 2)

            yoy = compare_metrics(current, yoy_base)
            sku_yoy = compare_sku_metrics(
                sku_current,
                compute_sku_precalc(df_yoy_detail)
            )


    # ---- everything below stays same (debug/ai/save/return) ----

    ai_payload = {
        "period": f"{period} {timeline} {year}",
        "period_label": period_label(period, timeline, year),
        "country": str(country).lower(),
        "mom": mom,
        "inventory_lost": inventory_lost,
        "inventory_alerts": inventory_alerts,
        "sku_mom": sku_mom,
        "focus_skus": top_5_skus,
        "movement_context": movement_context,   # 👈 ADD THIS
    }

    # ✅ Include YoY ONLY for yearly
    if period == "yearly":
        ai_payload["yoy"] = yoy
        ai_payload["sku_yoy"] = sku_yoy


    analysis_insights = json.loads(run_prompt_1_analysis(ai_payload))


    strategy = None
    if allow_reco:
        # backend-only objective for now
        strategy = run_prompt_2_strategy(analysis_insights, DEFAULT_USER_OBJECTIVE, top_5_skus)

    final_text = render_month_end_summary(period=period, timeline=timeline,year=year, analysis_insights=analysis_insights, mom=mom, sku_mom=sku_mom,focus_skus=top_5_skus, inventory_alerts=inventory_alerts,
        inventory_lost=inventory_lost,
        currency_symbol="£" if country == "uk" else "$",
        strategy_actions=strategy if allow_reco else None
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



    save_summary_to_db({
        "user_id": user_id,
        "country": country,
        "marketplace_id": marketplace_id,
        "period": period,
        "timeline": timeline,
        "year": year,
        "summary": ai_output["summary"],
        "recommendations": ai_output["recommendations"]
    })

    return {
        "summary": ai_output["summary"],
        "recommendations": ai_output["recommendations"],
        "inventory_lost": inventory_lost,
        "inventory_alerts": inventory_alerts,
        "sku_current": sku_current,
        "sku_mom": sku_mom,
        "sku_yoy": sku_yoy,
        "source": "ai"
    }



