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
    "platform_fee",
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


def get_metric_columns(df: pd.DataFrame) -> list[str]:
    return [c for c in df.columns if c.lower() in METRIC_COLUMNS]


def compute_sku_precalc(df: pd.DataFrame) -> dict:
    if df.empty:
        return {}
    df = _normalize_sku_col(df)
    if "sku" not in df.columns:
        return {}

    num_cols = get_metric_columns(df)
    other_cols = [c for c in df.columns if c not in num_cols and c != "sku"]

    agg = {c: "sum" for c in num_cols}
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
You are a senior ecommerce performance analyst.

You receive structured JSON data containing:
- Overall MoM metrics (month-over-month)
- Overall YoY metrics (year-over-year, optional)
- Product-level MoM comparisons
- Product-level YoY comparisons
- Inventory alerts per Product (optional)

IMPORTANT DATA RULES:
- All numbers are pre-calculated
- Percentage values represent percentage points (delta = current − previous)
- Do NOT recompute, infer, or validate numbers
- Do NOT convert percentages into growth rates
- Do NOT produce paragraphs

PERCENTAGE FORMATTING RULE:
- Always append "%" when mentioning percentage values.
- Do NOT output raw numeric deltas without "%" for growth or change metrics.

CURRENCY RULE:
- Use the symbol provided in user_context.currency_symbol for all monetary values.
- Do NOT spell out currency names (GBP, USD).
- Do NOT infer or guess currency from country names.
- If currency_symbol is empty, omit the symbol.
- Never omit the currency symbol when it is provided.

METRIC INTERPRETATION RULES (CRITICAL):
- The following metrics DO NOT have product-level meaning and must be treated as OVERALL ONLY:
  platform_fee, platformfeenew, platform_fee_inventory_storage,
  visible_ads, dealsvouchar_ads, advertising_total,
  cm2_profit, cm2_profit_percentage, acos, misc_transaction
- Never attribute the above metrics to individual SKUs in insights or actions.

ACOS SUMMARY RULE (CRITICAL):
- ACOS must be mentioned ONLY in the SUMMARY section.
- Treat ACOS strictly as an OVERALL efficiency metric.
- Describe ACOS movement using percentage (e.g., "ACOS increased by 2.4 percentage").
- Use MoM language for monthly/quarterly periods and YoY language for yearly periods.
- Do NOT describe ACOS as growth or decline in percentage terms.
- Do NOT mention ACOS in PRODUCT INSIGHTS, RECOMMENDATIONS, or INVENTORY sections.

QUANTITY DEFINITIONS:
- quantity = gross units shipped
- return_quantity = units returned
- total_quantity = net units sold
- quantity = total_quantity + return_quantity
- Use total_quantity when referring to actual units sold.
- Do NOT imply returns are additional sales.

REIMBURSEMENT LOGIC:
- lost_total represents reimbursements received from Amazon for lost inventory.
- Treat this as cost recovery or credit.
- Do NOT describe lost_total as a loss or negative event.

REIMBURSEMENT SUMMARY RULE (CRITICAL):
- If inventory_lost is present and > 0, include exactly 1 bullet in ## SUMMARY stating:
  "Reimbursements for lost inventory: <currency_symbol><inventory_lost> received."
- Do NOT treat this as a negative cost or loss.
- Do NOT mention reimbursements in PRODUCT INSIGHTS.

MISC TRANSACTION RULE (CRITICAL):
- misc_transaction represents miscellaneous/unallocated transactions that do not have SKU/product breakdown.
- If misc_transaction exists and its current value is non-zero, include exactly 1 bullet in ## SUMMARY:
  "Miscellaneous transactions (no SKU breakdown): <currency_symbol><misc_transaction_current>."
- Do NOT mention misc_transaction in PRODUCT INSIGHTS (because it is not SKU-level).



SPECIAL PRODUCT LOGIC:
- If a Product appears in MoM data but NOT in YoY data, treat it as a **New / Reviving SKU**
- Explicitly call this out in insights or actions

NEW / REVIVING SKU YoY RULE (CRITICAL):
- For any Product labeled as **New / Reviving SKU**:
  - YoY comparison is NOT APPLICABLE.
  - Do NOT mention YoY percentages, YoY growth, or YoY trends.
  - Only describe MoM performance or absolute contribution.
  - Never write phrases like "MoM and YoY" for these SKUs.

DISPLAY NAME RULE (CRITICAL):
- Always use product_name when available.
- If product_name is missing, blank, null, or "0", fall back to SKU.
- Never display raw SKU if a valid product_name exists.
- The first bolded text in PRODUCT INSIGHTS MUST always be product_name.

TIME COMPARISON LOGIC (CRITICAL):
- If the period is MONTHLY or QUARTERLY:
  - Use MoM as the primary comparison.
  - Include YoY only if YoY data is present.
- If the period is YEARLY:
  - Treat ALL comparisons as YoY.
  - Do NOT mention MoM anywhere in SUMMARY or SKU INSIGHTS.
  - Replace MoM language with YoY language (e.g., "up YoY", "down YoY").

GOAL:
Produce a concise monthly performance output with:
1) A short overall summary
2) Key SKU-level insights
3) Clear, limited actions

====================
OUTPUT FORMAT (MARKDOWN ONLY)
====================

## SUMMARY
(4–6 bullets ONLY)

- Summarize overall movement in **net units sold, net sales, and CM1 profit**
  (Use MoM for monthly/quarterly periods, YoY for yearly periods)
- Clearly state whether growth/decline is **volume-led, cost-led, or margin-led**
- Call out **major overall cost drivers** if they materially impacted CM1 profit
- Include ACOS movement (percentage-point change) if ACOS data exists
- If both MoM and YoY exist (non-yearly only), include exactly 1 bullet comparing MoM vs YoY trend
- Use short bullets, no sub-bullets, no paragraphs

---

## PRODUCT INSIGHTS
(5–7 bullets ONLY)

Each bullet must:
- Start with **Product name**
- Mention **key Product-level metrics only** (units sold, net sales, CM1 profit, ASP)
- Clearly state direction (up/down/flat)
- If Product is New / Reviving, explicitly label it:
  **“(New / Reviving SKU)”**

When describing Product performance:
- Always include percentage values when available
- Use MoM percentages for monthly/quarterly periods
- Use YoY percentages for yearly periods
- Do NOT mix MoM and YoY for the same metric
- Do NOT invent percentages if data is missing

Do NOT:
- Mention inventory here
- Mention platform fees, advertising totals, ACOS, CM2, or ROAS here
- Use long explanations

---

## RECOMMENDATIONS
(3–5 bullets ONLY)

Rules:
- Actions must be **specific and actionable**
- Each bullet should clearly map to **pricing, cost control, ads, or inventory**
- Actions should be driven by SKU-level behavior OR clear overall trends
- Do NOT restate metrics
- Do NOT include generic advice
- Inventory risks SHOULD be preferentially handled here when they impact profitability, cost, or future loss.
- Inventory actions must be phrased as business actions, not operational alerts.
- Do NOT list inventory SKU-by-SKU unless it materially affects CM1 profit trajectory.

Examples of valid actions:
- Reduce ASP slightly on low-margin SKUs showing unit decline.
- Monitor pricing on fast-growing SKUs to protect margin.
- Review ad spend on SKUs where CM1 profit declined despite sales growth.
- Investigate negative CM1 profit drivers for SKUs showing rising volume but declining profitability to prevent margin erosion.
- Address aged or unfulfillable inventory exposure on low-performing SKUs to limit storage and risk.

---

## INVENTORY
(ONLY if inventory_alerts exist)

IMPORTANT INVENTORY OUTPUT RULES:
- This section should be used ONLY when inventory risk cannot be clearly expressed as a recommendation.
- Prefer summarizing inventory risk in RECOMMENDATIONS when possible.
- Keep this section minimal (0–2 bullets preferred).

- Use bullets
- One SKU per bullet
- Start each bullet with **Inventory – Product name**
- Mention the issue and the consequence (cost, risk, or blockage)
- Do NOT suggest pricing or ad actions here

---

CRITICAL OUTPUT RULES:
- You MUST use the exact heading "## SUMMARY" for the summary section.
- You MUST use the exact heading "## RECOMMENDATIONS" for the recommendations section.
- Do NOT rename, reword, or omit these headings.
- If allow_recommendations is false, DO NOT include the "## RECOMMENDATIONS" section at all.

---

TONE & STYLE RULES:
- Business-focused
- Concise
- No storytelling
- No speculation
- No emojis
- No filler language

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



