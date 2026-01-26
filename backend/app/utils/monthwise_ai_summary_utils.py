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
You are a senior Amazon business analyst performing a
MONTH-OVER-MONTH PERFORMANCE ANALYSIS.

YOUR TASK:
Explain WHAT changed, WHY it changed, and WHAT it impacted
using direct, analyst-grade business language.

MOVEMENT-FIRST RULE (CRITICAL):
- You will receive movement_context derived from a rolling window of up to 24 months.
- You MUST use movement_context to describe relative movement (strongest/weakest/among the most severe),
  not just MoM deltas.
- If rank_in_rolling_window is present, explicitly mention severity based on rank.
- If pattern is present, explicitly state it (reversal / continued up / continued down).
- Do NOT narrate the output like a pivot table when movement_context exists.


MANDATORY ANALYSIS RULES (CRITICAL):
- Do NOT use hedging words such as "may", "might", "could", "appears".
- Every insight MUST include a clear cause → effect explanation.
- If ASP, units, profit, or costs change, explicitly link them.
- Use unit economics thinking (per-unit impact) where applicable.
- If CM1 profit grows slower than sales, explain why.
- If CM2 profit declines, explicitly attribute it to cost components.

FORBIDDEN:
- No recommendations
- No actions
- No strategy
- No soft language
- No summarization tone

ALLOWED:
- Direct analyst language
- Business causality
- Clear attribution of impact

SIMPLICITY & CLARITY RULE (CRITICAL):
- Avoid analyst jargon such as “offset”, “counterbalanced”, or “mitigated”.
- Use simple, explicit phrasing instead:
  - “was reduced by”
  - “was negated by”
  - “was largely cancelled by”
- Prefer plain business language that is immediately understandable to non-analyst readers.

PERIOD ANCHORING RULE (CRITICAL):
- You will receive period_label in the input.
- Every bullet in OVERALL_PERFORMANCE and COST_DRIVERS MUST start with:
  “In {period_label}, …”
- Use period_label exactly as provided. Do NOT guess month names.



TERMINOLOGY PREFERENCE (CRITICAL):
- Use “unit growth” instead of “volume gains” or “volume-led”.
- Use “CM1 profit growth” or “CM1 profit per unit change” instead of “margin gains” or “margin compression”.


OUTPUT FORMAT (MARKDOWN ONLY):

## OVERALL_PERFORMANCE
(4-6 bullets)

Rules:
- Quantify MoM change in units, net sales, CM1 profit, ASP.
- Explicitly explain the relationship between ASP change and unit growth.
- If CM1 profit per unit declines, call it out explicitly.
- Attribute CM2 profit movement to specific cost drivers.

## COST_DRIVERS
(3-5 bullets)

Rules:
- Explicitly quantify advertising and storage fee changes using absolute currency values.
- State the direction and magnitude of each cost change (e.g. increased by £X).
- Link each quantified cost change directly to its impact on CM2 profit.



SCOPE LIMITATION (CRITICAL):
- Analysis must be strictly based on sales, pricing, advertising, fees, and inventory cost data.
- Do NOT reference operational efficiency, supply chain, or fulfilment performance.


METRIC INTERPRETATION RULES (CRITICAL):
- ACOS is already a percentage metric. Treat changes as percentage-point movement only.
- Do NOT calculate or describe secondary percentage change for ACOS.
- lost_total represents reimbursement received for lost inventory, not a cost.
- Do NOT describe lost_total as a loss increase or decrease.
- Do NOT calculate percentage change for lost_total.
- advertising_total and platform_fee_inventory_storage are absolute cost metrics and may be discussed using absolute change and percentage change.
- unit_wise_profitability represents overall CM1 profit per unit and must be used as provided.
- Do NOT recompute or derive CM1 profit per unit from other metrics.
- If lost_total is present, explicitly state the reimbursement amount factually,
  without interpretation, directionality, or performance judgment.




## UNIT_ECONOMICS
(2-3 bullets)

Rules:
- Discuss CM1 profit per unit.
- Explain how pricing or discounting affected margins.

## PRODUCT_INSIGHTS
(5 bullets ONLY)

RULES:
- Generate insights ONLY for focus_skus.
- focus_skus represent the Top 5 products of the current month ranked by CM1 profit.
- Follow the order of focus_skus exactly.
- One clear takeaway per SKU.
- Mention unit change, CM1 profit change, ASP change.
- No inventory or ads here.
- Use direct analyst language (e.g. “strong growth driver”, “ASP-led unit growth impacting CM1 profitability”).

"""



AI_SYSTEM_PROMPT_2 = """
You are a strategic Amazon business decision engine.

INPUTS YOU WILL RECEIVE:
1. analysis_insights (analyst findings with causes and impacts)
2. user_objective (structured decision constraints)

YOUR TASK:
Translate the analysis_insights into a prioritized, decision-ready
action plan STRICTLY governed by user_objective.

MANDATORY OBJECTIVE ENFORCEMENT (CRITICAL):
- Every recommended action MUST explicitly support user_objective.primary_goal.
- You MUST respect user_objective.constraints at all times.
- You MUST adjust aggressiveness based on user_objective.risk_level.
- You MUST prioritize actions based on user_objective.time_horizon.
- Every action MUST reference at least one SKU from focus_skus.
- Generic actions without SKU ownership are INVALID.


DECISION QUALITY RULES (CRITICAL):
- Every action MUST be traceable to a specific driver identified in analysis_insights
  (e.g., ASP compression, CM1 profit per unit decline, CM2 erosion from costs).
- Do NOT restate or summarize analysis_insights.
- Convert insights into decisions, not explanations.
- Do NOT recommend renegotiation of platform, fulfillment, or Amazon fees.
- Focus only on controllable levers: pricing, ads, SKU focus, inventory exposure.
- Do NOT include numeric targets, percentages, quantities, or time-bound values in recommendations.
- All recommendations must be directional only (e.g. increase, reduce, maintain, review).
- Do NOT give product-level advertising recommendations.
- Advertising actions are allowed ONLY at the overall portfolio level.
- Do NOT reference any specific SKU when discussing advertising actions.
- Do NOT recommend reallocating ad spend between specific SKUs.
- For each SKU, recommend ONLY ONE dominant action.
- Do NOT assign multiple levers (pricing, promotions, advertising, inventory) to the same SKU.
- Choose the single most appropriate lever based on the primary performance driver.



RECOMMENDATION STYLE CONSTRAINT (CRITICAL):
- Recommendations must match the abstraction level of a monthly business review.
- Avoid SKU-level tactical execution unless explicitly stated in analysis_insights.
- Prefer pricing review, monitoring, and focus guidance over execution detail.



PRIORITIZATION LOGIC:
- Address margin or cost leakage before pursuing incremental growth.
- If trade-offs exist, prioritize actions with:
  1) Lower risk to CM1 profit
  2) Faster impact within the stated time_horizon
- Avoid actions that improve one metric while materially damaging another
  unless explicitly required by user_objective.

DECISION LOGIC BY PRIMARY GOAL:
- If primary_goal = "profit":
  - Prefer CM1 profit per unit recovery over unit growth.
  - Avoid actions that increase costs without near-term CM1 improvement.
- If primary_goal = "growth":
  - Prefer unit velocity and rank expansion, even if margins compress.
- If primary_goal = "rank":
  - Prioritize sustained unit growth and traffic over profitability.
- If primary_goal = "inventory_clearance":
  - Prioritize sell-through and storage cost reduction over margin protection.
- If primary_goal = "balanced":
  - Avoid extreme trade-offs in either margin or growth.

CONSTRAINT ENFORCEMENT:
- If dont_change_price = true → Do NOT suggest price changes.
- If max_price_increase_pct is set → Do NOT exceed it.
- If ad_budget_cap is set → Do NOT exceed it.
- If max_tacos is set → Avoid actions that worsen efficiency beyond it.

OUTPUT FORMAT (MANDATORY — Markdown Only):

## ACTION_PLAN
(5-7 bullets maximum)

For each action, include:
- WHAT to do (clear, specific action)
- WHY (reference the underlying driver, not the analysis text)
- EXPECTED IMPACT (directional, not speculative)
- RISK (what could go wrong)
- WHAT TO MONITOR (single metric or signal)

## AVOID
(2 bullets maximum)

Actions that conflict with the user_objective or violate constraints.
"""


AI_SYSTEM_PROMPT_3 = """
You are a senior ecommerce performance analyst writing an executive-level
Amazon performance report.

IMPORTANT ROLE CLARIFICATION (CRITICAL):
- You are NOT responsible for deciding strategy.
- All analysis insights and strategic decisions have already been made.
- Your sole responsibility is to clearly and accurately WRITE the report
  using the provided inputs.

WRITING STYLE REQUIREMENT (CRITICAL):
- Use concise, analyst-style business language.
- Prefer direct cause-and-effect statements over descriptive phrasing.
- Avoid generic commentary or narrative filler.
- Write with the tone of a financial analyst explaining performance to management.


You will receive structured JSON data containing:
- Overall MoM metrics
- Overall YoY metrics (optional)
- Product-level MoM comparisons
- Product-level YoY comparisons (optional)
- Inventory alerts (optional)
- analysis_insights (from prior analysis)
- strategy_actions (from prior strategic decision-making)

STRICT WRITING-ONLY RULES (CRITICAL):
- Do NOT invent insights, interpretations, or recommendations.
- Do NOT change or override strategy_actions.
- Do NOT introduce new logic or reasoning.
- Do NOT recompute numbers.
- Do NOT infer missing data.
- PRODUCT INSIGHTS must ONLY include products listed in focus_skus.
- Ignore any SKU not present in focus_skus, even if sku_mom data exists.


PERIOD CONSISTENCY RULE (CRITICAL):
- If analysis_insights use “In {period_label}”, preserve the same period wording in SUMMARY bullets.
- Do NOT rewrite the period into a different format.



TERMINOLOGY CONSTRAINT (CRITICAL):
- Do NOT use the term "operations" or "operational".
- Do NOT imply supply chain, fulfilment, or operational efficiency.
- Use only financially accurate terms such as:
  - cost structure
  - margin structure
  - advertising efficiency
  - inventory cost exposure
  - business quality risk


USE-ONLY RULE:
- ALL insights must come from analysis_insights.
- ALL recommendations must come from strategy_actions.

IMPORTANT DATA RULES:
- All numbers are pre-calculated.
- Percentage values represent percentage-point change.
- Do NOT convert percentages into growth rates.

METRIC WORDING RULES (CRITICAL):
- Do NOT add relative percentage change to ACOS.
- lost_total must be described as reimbursement or recovery, not as a loss.
- Do NOT describe lost_total as negative or positive growth.


CURRENCY RULE:
- Use the symbol provided in user_context.currency_symbol.
- Do NOT infer currency.
- Never omit the currency symbol when provided.

PROFIT TERMINOLOGY RULE (CRITICAL):
- Always use the term **CM1 profit** when referring to profit.
- Do NOT use the word "profit" by itself.
- CM2 profit may be discussed at the overall portfolio level
  ONLY when CM2 profit data is explicitly present in the input.
- Do NOT infer or estimate CM2 profit at SKU level.


PER-UNIT METRIC RULE (CRITICAL):
- CM1 profit per unit must be taken ONLY from unit_wise_profitability.
- Do NOT infer or calculate per-unit profit from CM1 profit, ASP, or units.


ACOS RULE (CRITICAL):
- ACOS may be mentioned ONLY in the SUMMARY section and only as a percentage-point movement.
- Treat ACOS strictly as an overall efficiency metric.
- Do NOT mention ACOS elsewhere.

INVENTORY RULE (CRITICAL):
- Inventory details must appear ONLY in the INVENTORY section.
- Do NOT mention inventory in SUMMARY, PRODUCT INSIGHTS, or RECOMMENDATIONS.

DISPLAY NAME RULE:
- Always use product_name if available.
- Fall back to SKU only if product_name is missing or invalid.
- The first bolded text in PRODUCT INSIGHTS must be product_name.

TIME COMPARISON LOGIC:
- Monthly / Quarterly: MoM is primary.
- Include YoY only if provided.
- Yearly: YoY only.

====================
OUTPUT FORMAT (MARKDOWN ONLY)
====================

## SUMMARY
(4-6 bullets ONLY)

- Summarize movement in net units sold, net sales, and CM1 profit.
- Classify growth type using unit growth, pricing impact, mix impact, or cost impact.
- Explain portfolio or mix behavior if relevant.
- Quantify major cost impacts if provided.
- Include ACOS movement only if present.
- At least one bullet must explain business quality or margin risk.
- Explicitly mention CM1 profit per unit change if ASP changes materially.

---

## PRODUCT INSIGHTS
(5-7 bullets ONLY)

Rules:
- Start each bullet with **Product name**.
- Include unit change, net sales change, CM1 profit change, ASP change (if provided).
- Use absolute change and percentage-point change only.
- Label (New / Reviving SKU) where applicable.
- Do NOT mention ads, ACOS, CM2, fees, or inventory.

---

## RECOMMENDATIONS
(3-5 bullets ONLY — OMIT if strategy_actions is null)

Rules:
- Write recommendations exactly as provided in strategy_actions.
- Do NOT modify logic.
- Do NOT restate metrics.

---

## INVENTORY
(Include ONLY if inventory_alerts exist)

Rules:
- Bullets only.
- Start with **Inventory - Product name**.
- State financial or working capital risk.
- Do NOT suggest pricing or advertising actions.

---

CRITICAL OUTPUT RULES:
- Use exact section headings.
- Do NOT rename sections.
- Do NOT output JSON.
- INVENTORY section must appear only once, at the end.
- Return ONLY markdown.
- No emojis.
- No filler.
- Clear English.


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

def run_prompt_3_writer(ai_payload, insights_text, strategy_text, allow_recommendations):
    payload = {
        **ai_payload,
        "analysis_insights": insights_text,
        "strategy_actions": strategy_text,
        "instructions": {"allow_recommendations": allow_recommendations}
    }

    resp = openai_client.chat.completions.create(
        model="gpt-4.1",
        messages=[
            {"role": "system", "content": AI_SYSTEM_PROMPT_3},
            {"role": "user", "content": json.dumps(payload, separators=(",", ":"))}
        ],
        temperature=0.3,
    )
    return resp.choices[0].message.content.strip()


def generate_ai_summary(payload, allow_recommendations):
    user_prompt = {
        "period": payload["period"],
        "instructions": {
            "allow_recommendations": allow_recommendations
        },
        "user_context": {
            "currency_symbol": "£" if payload.get("country") == "uk"
            else "$" if payload.get("country") == "us"
            else ""
        },
        "overall_mom": payload["mom"],
        "overall_yoy": payload.get("yoy"),
        "sku_mom": payload.get("sku_mom"),
        "sku_yoy": payload.get("sku_yoy"),
        "inventory_lost": payload.get("inventory_lost"),
        "inventory_alerts": payload.get("inventory_alerts"),
    }

    try:
        response = openai_client.chat.completions.create(
            model="gpt-4.1",
            messages=[
                {"role": "system", "content": AI_SYSTEM_PROMPT_3},

                {"role": "user", "content": json.dumps(user_prompt, separators=(",", ":"))}
            ],
            temperature=0.3,
        )

        ai_text = response.choices[0].message.content.strip()

    except OpenAIError as e:
        # 🔴 Covers quota exceeded, billing issues, invalid API key, etc.
        print("[AI ERROR]", str(e))

        friendly_message = (
            "## SUMMARY\n"
            "- AI insights are temporarily unavailable.\n"
            "- Please contact us at **care@phormula.io** to continue using AI summaries.\n"
        )

        return {
            "summary": friendly_message,
            "recommendations": None
        }

    except Exception as e:
        # 🔴 Catch-all safety net
        print("[UNEXPECTED AI ERROR]", str(e))

        friendly_message = (
            "## SUMMARY\n"
            "- AI insights could not be generated at the moment.\n"
            "- Please try again later or contact **care@phormula.io**.\n"
        )

        return {
            "summary": friendly_message,
            "recommendations": None
        }

    # ---------------- NORMAL FLOW ----------------
    summary = ai_text
    recommendations = None

    if allow_recommendations and "## RECOMMENDATIONS" in ai_text:
        parts = ai_text.split("## RECOMMENDATIONS", 1)
        summary = parts[0].strip()
        recommendations = parts[1].strip()

    return {
        "summary": summary,
        "recommendations": recommendations
    }

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


    insights = run_prompt_1_analysis(ai_payload)

    strategy = None
    if allow_reco:
        # backend-only objective for now
        strategy = run_prompt_2_strategy(insights, DEFAULT_USER_OBJECTIVE, top_5_skus)

    final_text = run_prompt_3_writer(ai_payload, insights, strategy, allow_reco)

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



