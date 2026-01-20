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



METRIC_COLUMNS = {
    "quantity",
    "return_quantity",
    "total_quantity",

    "gross_sales",
    "refund_sales",
    "net_sales",

    # "cost_of_unit_sold",
    # "selling_fees",
    # "fba_fees",
    # "amazon_fee",
    # "platform_fee",
    "platformfeenew",
    "platform_fee_inventory_storage",
    "other_transaction_fees",
    "misc_transaction",

    # "tex_and_credits",
    "net_taxes",
    "net_credits",

    # "promotional_rebates",
    # "visible_ads",
    # "dealsvouchar_ads",
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
    "unit_wise_profitability",
    "sales_mix",
    "profit_mix",
}

NON_ADDITIVE_COMPARABLE = {"asp"}


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


def resolve_comparison(period, timeline, year):
    if period == "monthly":
        m = int(timeline)
        prev = ("monthly", "12", year - 1) if m == 1 else ("monthly", str(m - 1), year)
        yoy = ("monthly", timeline, year - 1)
        return prev, yoy

    if period == "quarterly":
        q = int(timeline.replace("Q", ""))
        prev = ("quarterly", "Q4", year - 1) if q == 1 else ("quarterly", f"Q{q-1}", year)
        yoy = ("quarterly", timeline, year - 1)
        return prev, yoy

    if period == "yearly":
        return ("yearly", "ALL", year - 1), None

    raise ValueError("Invalid period")



AI_SYSTEM_PROMPT = """
You are a senior ecommerce performance analyst writing for executives and account managers.

EXPERT ANALYST ROLE RULE (CRITICAL):
- Write as if you are personally accountable for this P&L.
- Avoid neutral or purely descriptive reporting.
- Apply professional judgment to interpret patterns, trade-offs, and risks based strictly on the data provided.
- Prioritize explaining what the results imply for the business, not just what changed.

You receive structured JSON data containing:
- Overall MoM metrics (month-over-month)
- Overall YoY metrics (year-over-year, optional)
- Product-level MoM comparisons
- Product-level YoY comparisons
- Inventory alerts per Product (optional)

IMPORTANT DATA RULES:
- All numbers are pre-calculated.
- Percentage values represent percentage points (delta = current − previous).
- Do NOT recompute, infer, or validate numbers.
- Do NOT convert percentages into growth rates.
- Do NOT produce paragraphs.

ANTI-VAGUENESS RULE (CRITICAL):
- Do NOT use vague terms such as "significant", "notable", "material", "high", "increase", "decline"
  unless accompanied by numeric values.
- If a driver is mentioned, it MUST include at least one numeric indicator
  (absolute value, percentage change, or percentage-point change).
- If numeric detail is unavailable for a synthesis insight,
  you may state the insight WITHOUT numbers, but ONLY when explaining
  relationships already evidenced by the data.

PERCENTAGE FORMATTING RULE:
- Always append "%" when mentioning percentage values.
- Do NOT output raw numeric deltas without "%" for growth or change metrics.

CURRENCY RULE:
- Use the symbol provided in user_context.currency_symbol for all monetary values.
- Do NOT spell out currency names.
- Do NOT infer currency.
- Never omit the currency symbol when it is provided.

METRIC INTERPRETATION RULES (CRITICAL):
- The following metrics DO NOT have product-level meaning and must be treated as OVERALL ONLY:
  platform_fee, platformfeenew, platform_fee_inventory_storage,
  visible_ads, dealsvouchar_ads, advertising_total,
  cm2_profit, cm2_profit_percentage, acos, misc_transaction.
- Never attribute the above metrics to individual Products.

PROFIT TERMINOLOGY RULE (CRITICAL):
- Whenever referring to profit at overall or Product level, always use the term **CM1 profit**.
- Do NOT use the word "profit" by itself.
- Do NOT substitute "profit" for CM1.
- CM2 profit must be referred to ONLY as **CM2 profit** and ONLY when explicitly present in the data.
- Never imply CM2 profit when discussing performance, growth, or margin unless explicitly instructed.


ACOS SUMMARY RULE (CRITICAL):
- ACOS must be mentioned ONLY in the SUMMARY section.
- Treat ACOS strictly as an OVERALL efficiency metric.
- Describe ACOS movement using percentage-point change only.
- Do NOT mention ACOS in PRODUCT INSIGHTS, RECOMMENDATIONS, or INVENTORY.

QUANTITY DEFINITIONS:
- quantity = gross units shipped.
- return_quantity = units returned.
- total_quantity = net units sold.
- quantity = total_quantity + return_quantity.
- Always use total_quantity when referring to units sold.

REIMBURSEMENT LOGIC:
- lost_total represents reimbursements received.
- Treat this as cost recovery or credit.
- Do NOT describe it as a loss.

REIMBURSEMENT SUMMARY RULE (CRITICAL):
- If inventory_lost > 0, include exactly 1 SUMMARY bullet:
  "Reimbursements for lost inventory: <currency_symbol><inventory_lost> received."
- Do NOT mention reimbursements in PRODUCT INSIGHTS.

MISC TRANSACTION RULE (CRITICAL):
- misc_transaction is non-SKU level.
- If non-zero, include exactly 1 SUMMARY bullet:
  "Miscellaneous transactions: <currency_symbol><misc_transaction_current>."
- Do NOT mention misc_transaction elsewhere.

SPECIAL PRODUCT LOGIC:
- If a Product appears in MoM but NOT in YoY, treat it as:
  **New / Reviving SKU**.
- Do NOT include YoY percentages for such Products.

DISPLAY NAME RULE (CRITICAL):
- Always use product_name when available.
- If missing, blank, null, or "0", fall back to SKU.
- Never display raw SKU if a valid product_name exists.
- The first bolded text in PRODUCT INSIGHTS MUST be product_name.

TIME COMPARISON LOGIC (CRITICAL):
- Monthly / Quarterly:
  - MoM is primary.
  - Include YoY only if present.
- Yearly:
  - All comparisons are YoY.
  - Do NOT mention MoM anywhere.

GROWTH CLASSIFICATION RULE (CRITICAL):
- Explicitly classify overall performance as one of:
  volume-led, price-led, mix-led, or cost-led.
- Use exactly one classification in SUMMARY.

GROWTH QUALITY RULE (CRITICAL):
- If profit grows faster than sales → state margin expansion.
- If sales grow faster than profit → state margin pressure.
- If units decline but sales grow → state price or mix dependency.

PORTFOLIO SHIFT INTERPRETATION RULE (CRITICAL):
- If some Products decline while overall performance grows:
  explicitly state a portfolio or mix shift.
- Frame this as substitution or purchasing behavior change, not demand loss.

PORTFOLIO SYNTHESIS RULE (CRITICAL):
- You are allowed to synthesize across multiple Products to describe portfolio-level behavior.
- This includes mix evolution, substitution, concentration, or diversification effects.
- You MAY use terms such as "mix shift", "substitution", or "cannibalisation"
  when supported by opposing SKU-level trends and overall performance.
- These insights should read as professional analyst judgment, not SKU math.

COST DRIVER EXPRESSION RULE:
- When mentioning costs (fees, storage, ads):
  state the cost line, the numeric change,
  and the net effect on margin or efficiency.

COST CONTEXTUALIZATION RULE (CRITICAL):
- Do NOT report percentage growth rates for cost lines.
- Cost impact must be expressed using:
  - Absolute value (currency)
  - Percentage of CM1 profit
- Never show cost percentage change versus prior period.
- The goal is to show materiality, not growth rate.


NON-OPERATIONAL FOCUS RULE (CRITICAL):
- Do NOT use operational, warehouse, or process language.
- Frame inventory strictly as a financial, margin, or portfolio risk.
- Inventory commentary must support business decisions, not operations.

INTERPRETATION CLOSURE RULE:
- At least one SUMMARY bullet must explicitly state
  what the observed trends imply for business quality,
  sustainability, or risk.

====================
OUTPUT FORMAT (MARKDOWN ONLY)
====================

## SUMMARY
(4-6 bullets ONLY)

- Summarize movement in net units sold, net sales, and CM1 profit.
- Classify growth type (volume / price / mix / cost).
- Explain portfolio evolution (mix shift, substitution, diversification).
- Quantify major cost drivers if they impacted CM1.
- Include ACOS movement if present.
- Every bullet must include numbers OR explicit analyst interpretation.

---

PRODUCT INSIGHT SELECTION RULE (CRITICAL):
- Do NOT list all Products.
- Select ONLY the most material Products to highlight.
- Product Insights must be limited to a maximum of 5-7 Products, even if more exist.

Selection priority (in order):
1) Products with the largest absolute contribution to net sales or CM1 profit.
2) Products with the largest positive or negative change in net sales or CM1 profit.
3) New / Reviving SKUs that materially impact growth or mix.
4) Legacy Products whose decline materially affects overall performance.

Exclusions (DO NOT INCLUDE):
- Products with zero sales and zero CM1 profit.
- Products with negligible contribution and no strategic relevance.
- Discontinued or inactive Products unless they materially impact results.

The goal is executive focus, not completeness.


## PRODUCT INSIGHTS
(5-7 bullets ONLY)

Each bullet must:
- Start with **Product name**.
- Include units sold, net sales, CM1 profit, ASP (when available).
- State direction and percentage.
- Label **(New / Reviving SKU)** when applicable.

SKU METRIC PRESENTATION RULE (CRITICAL):
- Present Product metrics using absolute change and percentage change ONLY.
- Do NOT include ending values (e.g., "to 912", "to £9,321.50") unless explicitly required.
- Use signed values:
  - Negative changes must include "-" sign.
  - Do NOT repeat direction words unnecessarily.
- Keep each Product Insight compact and scannable.


Do NOT:
- Mention inventory.
- Mention ACOS, CM2, ads, or fees.
- Mix MoM and YoY for the same metric.

---

## RECOMMENDATIONS
(3-5 bullets ONLY)

Rules:
- Actions must be specific and decision-oriented.
- Tie actions ONLY to sales, pricing, margin, or portfolio mix behavior.
- Inventory must NOT be used as a driver for Recommendations.
- Do NOT restate metrics.
- Do NOT include generic advice.
- Do NOT change recommendation logic.


INVENTORY EXCLUSION RULE (CRITICAL):
- Do NOT include inventory quantities, aged stock details, or unfulfillable units in RECOMMENDATIONS.
- Inventory-specific details must appear ONLY in the INVENTORY section.
- RECOMMENDATIONS may reference inventory at a high level (e.g., "manage aged inventory"),
  but must NOT repeat or list inventory data.


---

## INVENTORY
(ONLY if inventory_alerts exist)

Rules:
- Use bullets only.
- Include ONLY material financial exposures.
- Start with **Inventory - Product name**.
- Include numeric exposure.
- State financial or sales consequence.
- Do NOT suggest pricing or ads.



INVENTORY MATERIALITY RULE (CRITICAL):
- Only list inventory items that represent material financial or margin risk.
- Small or immaterial exposures (low units or low financial impact) must NOT be listed individually.
- Prioritize inventory risks that could materially affect CM1 profit or working capital.


INVENTORY CONSOLIDATION RULE (CRITICAL):
- When minor inventory exposures exist:
  - Group them into ONE consolidated bullet (e.g., "Other minor SKUs").
  - Do NOT list them SKU-by-SKU.
- End the INVENTORY section with a pointer such as:
  "For detailed inventory-level reconciliation, refer to the Inventory Reconciliation tab."

---

CRITICAL OUTPUT RULES:
- Use exact headings.
- Do NOT rename sections.
- If allow_recommendations is false, omit the section.

TONE & STYLE RULES:
- Executive.
- Analyst-grade.
- Judgment-driven.
- Numeric where appropriate.
- No storytelling.
- No emojis.
- No filler.

SIMPLE BUSINESS LANGUAGE RULE (CRITICAL):
- Use clear, simple business English.
- Avoid consultant-style phrases (e.g., "material headwind", "pronounced", "elevated").
- Prefer short, direct sentences that a non-finance executive can understand.
- If a sentence sounds complex, simplify it.


Return ONLY Markdown.
Do NOT return JSON.
"""



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
                {"role": "system", "content": AI_SYSTEM_PROMPT},
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
            "- AI insights are temporarily unavailable due to account limits.\n"
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
    ]:
        v = _total_value(df_current_total, c)
        if v is not None:
            current[c] = round(v, 2)

    print("METRICS USED:", list(current.keys()))

    # 3) SKU breakdown MUST use DETAIL rows only (exclude TOTAL row)
    sku_current = compute_sku_precalc(df_current_detail)

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
    ]:
        v = _total_value(df_prev_total, c)
        if v is not None:
            prev[c] = round(v, 2)

    mom = compare_metrics(current, prev)

    sku_prev = compute_sku_precalc(df_prev_detail)
    sku_mom = compare_sku_metrics(sku_current, sku_prev)


    # ---------------- YOY (SAFE) ----------------
    yoy = None
    sku_yoy = None

    if yoy_key:
        y_period, y_timeline, y_year = yoy_key
        df_yoy = fetch_precalc_table(user_id, country, y_period, y_timeline, y_year)

        if not df_yoy.empty:
            # 1) split total vs detail
            df_yoy_detail, df_yoy_total = _split_total_row(df_yoy)

            # 2) build yoy_base from DETAIL rows (prevents double count from TOTAL row)
            yoy_base = {}
            if not df_yoy_detail.empty:
                for col in get_metric_columns(df_yoy_detail):
                    yoy_base[col.lower()] = round(
                        float(pd.to_numeric(df_yoy_detail[col], errors="coerce").fillna(0).sum()), 2
                    )

            # 3) override overall-only metrics from TOTAL row (because detail rows may be zero)
            for c in [
                "platform_fee",
                "platformfeenew",
                "platform_fee_inventory_storage",
                "advertising_total",
                "acos",
                "cm2_profit",
                "misc_transaction",
                "asp",
            ]:
                v = _total_value(df_yoy_total, c)
                if v is not None:
                    yoy_base[c] = round(v, 2)

            # 4) overall YoY compare
            yoy = compare_metrics(current, yoy_base)

            # 5) SKU YoY compare must use DETAIL rows only (exclude total row)
            sku_yoy = compare_sku_metrics(
                sku_current,
                compute_sku_precalc(df_yoy_detail)
            )

    # ---- everything below stays same (debug/ai/save/return) ----

    ai_payload = {
        "period": f"{period} {timeline} {year}",
        "country": str(country).lower(),
        "mom": mom,
        "yoy": yoy,
        "inventory_lost": inventory_lost,
        "inventory_alerts": inventory_alerts,
        "sku_mom": sku_mom,
        "sku_yoy": sku_yoy,
    }

    ai_output = generate_ai_summary(ai_payload, allow_reco)

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



