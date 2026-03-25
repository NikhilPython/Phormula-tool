from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from config import Config
from datetime import datetime
from openai import OpenAI, OpenAIError
import os
import json
import pandas as pd

load_dotenv()
SECRET_KEY = Config.SECRET_KEY

db_url = os.getenv("DATABASE_URL")
db_url2 = os.getenv("DATABASE_CHATBOT_URL")
db_url3 = os.getenv("DATABASE_AMAZON_URL")

phormula_engine = create_engine(db_url)
chatbot_engine = create_engine(db_url2)
amazon_engine = create_engine(db_url3)

openai_client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))


def get_previous_month_year(month_name, year):
    month_map = {
        "january": 1, "february": 2, "march": 3, "april": 4,
        "may": 5, "june": 6, "july": 7, "august": 8,
        "september": 9, "october": 10, "november": 11, "december": 12
    }

    reverse_month_map = {v: k for k, v in month_map.items()}

    month_num = month_map[month_name.lower()]

    if month_num == 1:
        prev_month_num = 12
        prev_year = year - 1
    else:
        prev_month_num = month_num - 1
        prev_year = year

    return reverse_month_map[prev_month_num], prev_year


def fetch_skuwise_monthly(phormula_engine, user_id, country, month_name, year, months_back=24):
    from datetime import datetime

    month_map = {
        "january": 1, "february": 2, "march": 3, "april": 4,
        "may": 5, "june": 6, "july": 7, "august": 8,
        "september": 9, "october": 10, "november": 11, "december": 12
    }
    reverse_month_map = {v: k for k, v in month_map.items()}

    country = country.lower().strip()
    month_name = month_name.lower().strip()

    current_month_num = month_map[month_name]
    current_date = datetime(year, current_month_num, 1)

    all_data = []

    check_query = text("""
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = 'public'
              AND table_name = :table_name
        )
    """)

    with phormula_engine.connect() as connection:
        for _ in range(months_back):
            y = current_date.year
            m = current_date.month
            m_name = reverse_month_map[m]

            if country == "global":
                table_name = f"skuwisemonthly_{user_id}_{country}_{m_name}{y}_table"
            else:
                table_name = f"skuwisemonthly_{user_id}_{country}_{m_name}{y}"

            

            table_exists = connection.execute(
                check_query,
                {"table_name": table_name}
            ).scalar()

            if table_exists:
                query = text(f'SELECT * FROM "{table_name}"')
                df = pd.read_sql(query, connection)

                if not df.empty:
                    df["source_year"] = y
                    df["source_month"] = m
                    df["source_month_name"] = m_name
                    df["source_month_label"] = f"{m_name.capitalize()} {y}"
                    all_data.append(df)

            if m == 1:
                current_date = datetime(y - 1, 12, 1)
            else:
                current_date = datetime(y, m - 1, 1)

    if not all_data:
        raise Exception(
            f"No SKU monthly tables found for user_id={user_id}, country={country}, "
            f"ending at {month_name} {year}"
        )

    combined_df = pd.concat(all_data, ignore_index=True)
    combined_df = combined_df.sort_values(["source_year", "source_month"]).reset_index(drop=True)

    return combined_df

def split_latest_month_df(combined_df, month_name, year):
    month_map = {
        "january": 1, "february": 2, "march": 3, "april": 4,
        "may": 5, "june": 6, "july": 7, "august": 8,
        "september": 9, "october": 10, "november": 11, "december": 12
    }

    month_num = month_map[month_name.lower()]

    latest_df = combined_df[
        (combined_df["source_year"] == year) &
        (combined_df["source_month"] == month_num)
    ].copy()

    if latest_df.empty:
        raise Exception(f"No latest month data found for {month_name} {year}")

    return latest_df


def build_monthly_trend_from_combined_df(combined_df):
    trend = []

    grouped = combined_df.groupby(["source_year", "source_month", "source_month_name"], as_index=False)

    for (y, m, m_name), g in grouped:
        total_row = g[
            (g["sku"].astype(str).str.lower() == "total") |
            (g["product_name"].astype(str).str.lower() == "total")
        ]

        if total_row.empty:
            continue

        row = total_row.iloc[0]

        storage_fee = row.get("platform_fee_inventory_storage", 0)
        if pd.notna(storage_fee):
            storage_fee = abs(float(storage_fee or 0))
        else:
            storage_fee = 0

        trend.append({
            "month": f"{str(m_name).capitalize()} {int(y)}",
            "year": int(y),
            "month_num": int(m),
            "gross_sales": float(row.get("gross_sales", 0) or 0),
            "net_sales": float(row.get("net_sales", 0) or 0),
            "profit": float(row.get("profit", 0) or 0),
            "profit_percentage": float(row.get("profit_percentage", 0) or 0),
            "cm2_profit": float(row.get("cm2_profit", 0) or 0),
            "cm2_profit_percentage": float(row.get("cm2_profit_percentage", 0) or 0),
            "total_units": float(row.get("total_quantity", 0) or 0),
            "advertising_total": float(row.get("advertising_total", 0) or 0),
            "platform_fee": abs(float(row.get("platform_fee", 0) or 0)),
            "platform_fee_inventory_storage": storage_fee,
            "acos": float(row.get("acos", 0) or 0)
        })

    trend = sorted(trend, key=lambda x: (x["year"], x["month_num"]))
    return trend

def build_sku_summary(combined_df):
    """
    Aggregate SKU performance across full time window
    """

    df = combined_df.copy()

    # remove total rows
    df = df[
        ~(df["sku"].astype(str).str.lower() == "total") &
        ~(df["product_name"].astype(str).str.lower() == "total")
    ]

    grouped = df.groupby(["sku", "product_name"], as_index=False).agg({
        "net_sales": "sum",
        "gross_sales": "sum",
        "profit": "sum",
        "total_quantity": "sum"
    })

    # derived metrics
    grouped["profit_percentage"] = (
        grouped["profit"] / grouped["net_sales"]
    ).replace([float("inf"), -float("inf")], 0).fillna(0) * 100

    return grouped.sort_values("net_sales", ascending=False).to_dict("records")

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

    # =========================================================
    # ✅ DATE PROCESSING
    # =========================================================
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"])

    df["year"] = df["date"].dt.year
    df["month"] = df["date"].dt.month

    df = df.sort_values("date")

    # =========================================================
    # ✅ TAKE LAST SNAPSHOT PER SKU + DISPOSITION + MONTH
    # =========================================================
    month_end = (
        df.groupby(["msku", "disposition", "year", "month"], as_index=False)
        .last()
    )

    lookup = {}

    for _, r in month_end.iterrows():

        # =========================================================
        # ✅ KEY FIX (VERY IMPORTANT)
        # =========================================================
        sku = str(r["msku"]).strip()
        year = int(r["year"])
        month = int(r["month"])

        key = (sku, year, month)

        if key not in lookup:
            lookup[key] = {
                "sellable_inventory": 0,
                "damaged_inventory": 0,
                "expired_inventory": 0
            }

        # =========================================================
        # ✅ SAFE VALUE HANDLING
        # =========================================================
        disp = str(r["disposition"]).upper().strip()
        units = float(r["ending_warehouse_balance"] or 0)

        # =========================================================
        # ✅ CLASSIFICATION
        # =========================================================
        if disp == "SELLABLE":
            lookup[key]["sellable_inventory"] += units

        elif disp in ["DEFECTIVE", "WAREHOUSE_DAMAGED", "CUSTOMER_DAMAGED"]:
            lookup[key]["damaged_inventory"] += units

        elif disp == "EXPIRED":
            lookup[key]["expired_inventory"] += units

    return lookup    


def compute_inventory_sales_correlation(monthly_trend, inventory_trend):
    """
    Detect relationship between sales and inventory movement
    """

    if not monthly_trend or not inventory_trend:
        return []

    # map inventory by month
    inventory_map = {
        (x["year"], x["month_num"]): x
        for x in inventory_trend
    }

    correlation = []

    for i in range(1, len(monthly_trend)):
        prev = monthly_trend[i - 1]
        curr = monthly_trend[i]

        key_prev = (prev["year"], prev["month_num"])
        key_curr = (curr["year"], curr["month_num"])

        inv_prev = inventory_map.get(key_prev)
        inv_curr = inventory_map.get(key_curr)

        if not inv_prev or not inv_curr:
            continue

        # ============================
        # SALES CHANGE
        # ============================
        sales_diff = (curr["net_sales"] or 0) - (prev["net_sales"] or 0)

        # ============================
        # INVENTORY CHANGE
        # ============================
        inv_prev_total = (
            inv_prev["sellable_inventory"]
            + inv_prev["damaged_inventory"]
            + inv_prev["expired_inventory"]
        )

        inv_curr_total = (
            inv_curr["sellable_inventory"]
            + inv_curr["damaged_inventory"]
            + inv_curr["expired_inventory"]
        )

        inventory_diff = inv_curr_total - inv_prev_total

        # ============================
        # CLASSIFY RELATIONSHIP
        # ============================
        signal = "stable"

        if sales_diff > 0 and inventory_diff < 0:
            signal = "strong_sell_through"

        elif sales_diff > 0 and inventory_diff > 0:
            signal = "scaled_growth"

        elif sales_diff < 0 and inventory_diff > 0:
            signal = "overstock_risk"

        elif sales_diff > 0 and inventory_diff < 0 and abs(inventory_diff) > abs(sales_diff):
            signal = "stockout_risk"

        correlation.append({
            "month": curr["month"],
            "sales_change": sales_diff,
            "inventory_change": inventory_diff,
            "signal": signal
        })

    return correlation

def fetch_business_context(chatbot_engine, user_id, country, month_name, year):
    month_map = {
        "january": 1, "february": 2, "march": 3, "april": 4,
        "may": 5, "june": 6, "july": 7, "august": 8,
        "september": 9, "october": 10, "november": 11, "december": 12
    }

    month_num = month_map[month_name.lower()]

    query = text("""
        SELECT
            id,
            user_id,
            country,
            growth_intent,
            profit_priority,
            inventory_clearance_priority,
            business_context,
            website_url,
            objective_month,
            ai_business_journey,
            created_at,
            updated_at
        FROM user_objectives
        WHERE user_id = :user_id
          AND LOWER(country) = :country
          AND EXTRACT(YEAR FROM objective_month) = :year
          AND EXTRACT(MONTH FROM objective_month) = :month_num
        ORDER BY id DESC
        LIMIT 1
    """)

    with chatbot_engine.connect() as connection:
        df = pd.read_sql(
            query,
            connection,
            params={
                "user_id": user_id,
                "country": country.lower(),
                "year": year,
                "month_num": month_num
            }
        )

    return df

def build_inventory_trend(inventory_lookup):
    """
    Convert inventory lookup into monthly inventory trend
    """

    trend_map = {}

    for (sku, year, month), data in inventory_lookup.items():

        key = (year, month)

        if key not in trend_map:
            trend_map[key] = {
                "year": year,
                "month_num": month,
                "sellable_inventory": 0,
                "damaged_inventory": 0,
                "expired_inventory": 0
            }

        trend_map[key]["sellable_inventory"] += data.get("sellable_inventory", 0) or 0
        trend_map[key]["damaged_inventory"] += data.get("damaged_inventory", 0) or 0
        trend_map[key]["expired_inventory"] += data.get("expired_inventory", 0) or 0

    # convert to list
    trend = []

    month_map = {
        1: "January", 2: "February", 3: "March", 4: "April",
        5: "May", 6: "June", 7: "July", 8: "August",
        9: "September", 10: "October", 11: "November", 12: "December"
    }

    for (year, month), data in trend_map.items():
        trend.append({
            "month": f"{month_map[month]} {year}",
            "year": year,
            "month_num": month,
            "sellable_inventory": int(data["sellable_inventory"]),
            "damaged_inventory": int(data["damaged_inventory"]),
            "expired_inventory": int(data["expired_inventory"])
        })

    # sort properly
    trend = sorted(trend, key=lambda x: (x["year"], x["month_num"]))

    return trend

def prepare_ai_sku_data(sku_df):
    """
    Clean SKU data for AI:
    - Select required columns
    - Separate total row
    - Extract overall metrics
    - Fix negative storage fee (abs)
    """

    if sku_df.empty:
        return [], {}

    AI_COLUMNS = [
        "sku", "product_name", "return_quantity", "total_quantity",
        "asp", "gross_sales", "tex_and_credits", "net_sales",
        "amazon_fee", "profit", "unit_wise_profitability",
        "profit_percentage", "advertising_total", "lost_total",
        "platformfeenew", "platform_fee",
        "platform_fee_inventory_storage", "cm2_profit",
        "cm2_profit_percentage", "rembursement_fee",
        "rembursment_vs_cm2_margins", "reimbursement_vs_sales",
        "sales_mix", "profit_mix"
    ]

    GLOBAL_COLUMNS = [
    "gross_sales",
    "net_sales",
    "profit",
    "profit_percentage",
    "cm2_profit",
    "cm2_profit_percentage",
    "advertising_total",
    "lost_total",
    "platformfeenew",
    "platform_fee",
    "platform_fee_inventory_storage",
    "acos"
    ]

    # ----------- FIND TOTAL ROW -----------
    total_row = sku_df[
        (sku_df["sku"].astype(str).str.lower() == "total") |
        (sku_df["product_name"].astype(str).str.lower() == "total")
    ]

    overall_metrics = (
        total_row.iloc[0].to_dict()
        if not total_row.empty else {}
    )

    # ----------- REMOVE TOTAL ROW -----------
    sku_clean_df = sku_df.drop(total_row.index)

    # ----------- SELECT ONLY REQUIRED COLUMNS -----------
    sku_clean_df = sku_clean_df[AI_COLUMNS]

    # =====================================================
    # ✅ FIX 1: ABS FOR STORAGE FEE (SKU LEVEL)
    # =====================================================
    if "platform_fee_inventory_storage" in sku_clean_df.columns:
        sku_clean_df["platform_fee_inventory_storage"] = (
            sku_clean_df["platform_fee_inventory_storage"].abs()
        )

    # ----------- CONVERT TO DICT -----------
    sku_data = sku_clean_df.to_dict(orient="records")

    # =====================================================
    # ✅ FIX 2: ADD DEFAULT FIELDS (STRUCTURE SAFETY)
    # =====================================================
    for sku in sku_data:
        sku["sellable_inventory"] = None
        sku["damaged_inventory"] = None
        sku["expired_inventory"] = None
        sku["inventory_coverage_days"] = None

    # =====================================================
    # ✅ FIX 3: HANDLE GLOBAL METRICS + ABS
    # =====================================================
    overall_metrics_filtered = {}

    for col in GLOBAL_COLUMNS:
        value = overall_metrics.get(col)

        # ABS fix for storage fee at total level
        if col == "platform_fee_inventory_storage" and value is not None:
            value = abs(value)

        overall_metrics_filtered[col] = value

    # =====================================================
    # ✅ OPTIONAL: ADD STRUCTURE METADATA (FOR AI CLARITY)
    # =====================================================
    overall_metrics_filtered["_note"] = "Monthly aggregated totals (not SKU-level)"

    return sku_data, overall_metrics_filtered

AI_BUSINESS_JOURNEY_PROMPT = """
You are a senior business consultant.

Your task is to generate a BUSINESS JOURNEY using the provided observed monthly business data.

IMPORTANT CONTEXT:
- The available monthly data is a rolling observed period from Amazon SP API.
- It may not represent the full business history.
- Do NOT assume that the first available month is the business start, launch, or foundation period.
- Treat the data strictly as an observed performance window only.

IMPORTANT RULES:
- All metrics are pre-calculated. Do NOT recompute raw source metrics unless deriving change percentages or comparisons directly from the provided monthly_trend or inventory_trend.
- Always use the numbers provided.
- Always explain cause-and-effect relationships clearly.
- Make the output understandable for someone who knows nothing about the business.
- This must feel like a business journey, not a static overview.
- Do not make assumptions when data is missing. Clearly state when something is unavailable.
- Keep the output concise enough to be readable, while still analytical.

PROFIT INTERPRETATION RULE (CRITICAL):

- Whenever referring to "profit", ALWAYS use CM1 Profit (the "profit" field provided in the data).
- Treat CM1 Profit as the primary profitability metric across all sections.

- Do NOT reinterpret profit using CM2 unless explicitly stated.
- CM2 should ONLY be used when specifically referring to:
  - "CM2 Profit"
  - "CM2 Margin"

- Never mix CM1 and CM2 in the same statement.
- When mentioning margins:
  - "profit margin" = CM1 profit_percentage
  - "CM2 margin" = cm2_profit_percentage

- If both are mentioned, clearly differentiate them.

FORMATTING RULES:
- Use exactly 2 decimal places for all monetary values, percentages, ratios, and derived metrics.
- Use whole numbers only for units and inventory quantities.
- Whenever mentioning a metric, always mention the relevant month and year if that metric belongs to a specific point in time.
- Never mention a percentage, sales value, margin, profit, cost, or ratio without tying it to a month/year or clearly stating that it refers to the latest month or observed period.
- If discussing latest SKU-level metrics, explicitly state that they refer to the latest month in the dataset.
- Always explicitly mention the latest_month_label when referring to latest month performance.

OUTPUT FORMATTING RULES (STRICT):
- You MUST strictly follow the numbered section format exactly as provided in OUTPUT STRUCTURE.
- Each section MUST begin with its number and title exactly like:
  "1. Business Journey Across the Observed Period"
  "2. Inventory Journey Across the Observed Period"
- Do NOT skip numbering.
- Do NOT merge sections.
- Add a blank line before and after each section.
- Use clear paragraph spacing between sections.

CURRENCY RULE:
- Use the currency_symbol provided in input.
- Do NOT assume $.
- Format values like: £12,345.67 or $12,345.67.

DATA INTERPRETATION RULES:
- monthly_trend is the most important input for journey analysis.
- monthly_trend is chronological monthly business performance for the observed data window.
- inventory_trend is chronological monthly inventory totals for the observed data window.
- sku_data is the latest month's SKU-level breakdown.
- overall_metrics is the latest month's aggregated total row.
- summary_metrics is the latest month's key headline metrics.
- Do NOT assign overall business-level costs directly to individual SKUs unless explicitly present at SKU level.
- Do NOT assign overall inventory totals directly to individual SKUs.

COST INTERPRETATION RULE:
- All cost values should be interpreted as positive costs.
- Higher values mean higher cost burden, not savings.
- Platform fees and inventory storage fees are both costs.
- Never describe one cost as offsetting another cost.
- Inventory storage fees are a cost, not a benefit or offset.

LOST_TOTAL INTERPRETATION RULE:
- lost_total represents reimbursements received from Amazon for lost or damaged inventory.
- It is NOT a loss or leakage.
- It should be treated as a recovery or compensation, not a cost.
- Do NOT describe lost_total as lost sales or negative business impact.

INVENTORY RULES:
- Use sellable_inventory, damaged_inventory, expired_inventory, and inventory_coverage_days only when present in the latest SKU data.
- Use inventory_trend for monthly inventory history and total inventory movement over time.
- If inventory_coverage_days is null, missing, or unavailable, do NOT assume stockout risk.
- Only comment on stockout risk when coverage is explicitly available and clearly low.
- If sellable inventory is available for a SKU, mention it when it materially supports the analysis.
- If damaged or expired inventory exists, explain the business impact.
- If damaged or expired inventory remains consistently low, mention that clearly as a positive operational signal.
- If inventory data is unavailable for some SKUs, say so clearly rather than making assumptions.
- Always include actual inventory quantities when discussing inventory. Avoid vague phrases like "inventory looks healthy" without numbers.

INVENTORY TREND ANALYSIS RULES:
- Use inventory_trend to explain how total sellable, damaged, and expired inventory changed over time.
- Identify:
  - highest inventory month
  - lowest inventory month
  - inventory build-up periods
  - inventory drawdown periods
- Explain the relationship between inventory movement and sales movement where visible.
- If inventory increased while sales weakened, mention possible overstock risk.
- If inventory declined while sales remained strong, mention improved sell-through or replenishment pressure as appropriate.
- Do not invent inventory conclusions if the trend does not clearly support them.

INVENTORY-SALES CORRELATION RULES:
- Use inventory_sales_correlation to explain the relationship between sales and inventory movement.
- Each entry represents how sales and inventory changed from the previous month.
- Interpret signals as:
  - "strong_sell_through" → sales increased while inventory declined
  - "scaled_growth" → both sales and inventory increased
  - "overstock_risk" → inventory increased while sales declined
  - "stockout_risk" → inventory declined faster than sales increased
- Mention these signals only when they are clearly present.
- Always attach month/year when referencing correlation insights.
- Do NOT invent correlation if data is missing.

TREND ANALYSIS RULES:
- Use monthly_trend to tell the story of how the business changed over time.
- Identify:
  - highest month by net sales
  - highest month by CM1 profit
  - highest month by units
  - lowest month by net sales
  - lowest month by CM1 profit
  - major rises, declines, volatility, and recovery phases
- Mention growth and decline percentages wherever meaningful from the observed monthly data.
- Explicitly mention when the business was strongest and when it was weakest during the observed period.
- If the trend is volatile, say so clearly.
- If the trend improved over time, explain when the improvement started.
- If the trend worsened over time, explain when the slowdown or decline started.

SKU-LEVEL REQUIREMENTS:

- Use sku_summary for overall SKU performance across the full observed period.
- Use sku_data ONLY for latest month inventory and current snapshot.

- Identify:
  - top SKUs by total net sales across the observed period
  - top SKUs by total profit across the observed period
  - weakest SKUs across the observed period

- Clearly differentiate:
  - historical performance (from sku_summary)
  - latest month performance (from sku_data)

- When discussing historical SKU performance:
  - refer to the observed period
  - do NOT attribute it to a single month

- When discussing latest SKU metrics:
  - explicitly mention the latest month and year

- For important SKUs:
  - mention total net sales
  - total CM1 profit
  - overall CM1 margin across the observed period

- Use latest month inventory ONLY as a supporting signal, not as historical data.

COMPETITION ANALYSIS AND COMPARISON RULES:
- Identify the business category using business_overview (e.g., skincare, intimate hygiene, supplements, apparel, etc.).
- Use business_category_hint (if available) to better understand the business type and identify relevant competitors.
- Based on this category, you may mention well-known and widely recognized competitors that operate in a similar product space.

- Only include competitor names if they are:
  - widely known brands
  - relevant to the same category
  - likely to compete for similar customers

- Do NOT fabricate unknown or obscure competitor names.

- If competitor identification is based on general knowledge and not provided data, explicitly state:
  "Competitor identification is indicative based on general market knowledge."



OUTPUT STRUCTURE:

1. Business Journey Across the Observed Period
- Tell the chronological story of the business using monthly_trend.
- Mention the first and last observed month.
- Highlight major phases in the observed period:
  - growth periods
  - decline periods
  - recovery periods
  - volatile periods
- Mention the highest and lowest months for sales, profit, and units.
- Include percentage changes when describing rises and falls.
- Always attach month/year to every key metric mentioned.

2. Inventory Journey Across the Observed Period
- Use inventory_trend to explain how sellable, damaged, and expired inventory changed over time.
- Mention highest and lowest inventory months if available.
- Explain whether inventory movement supports or conflicts with sales movement.
- Mention damaged and expired inventory history if meaningful.
- Mention coverage ratio only where explicitly available.

3. Latest Month Performance Summary
- Analyze the latest month using summary_metrics and overall_metrics.
- Clearly state latest gross sales, net sales, CM1 profit, margin, CM2, advertising cost, platform fees, storage fees, and other major cost pressures.
- Make clear that these are latest-month numbers and mention the month/year.

4. Commercial Performance Analysis
- Merge revenue, sales, profitability, and cost analysis into one single integrated section.
- Explain:
  - revenue drivers
  - sales concentration
  - margin strength or weakness
  - major cost burdens
  - impact of advertising, platform fees, storage fees, and reimbursements (lost_total)
- Tie all major points back to month/year.
- Keep this section concise but analytical.
- This section MUST reflect the FULL observed period, not just the latest month.
- Analyze:
  - how revenue evolved across the period
  - how margins changed over time
  - how cost structure evolved (advertising, platform fees, storage)
- Only reference the latest month as a final state, NOT as the entire analysis.

5. SKU-Level Insights

- This section MUST include BOTH:
  1) Historical SKU performance across the full observed period (using sku_summary)
  2) Latest month SKU snapshot (using sku_data)

- FIRST: Historical Performance (PRIMARY ANALYSIS)
  - Identify top SKUs by total net sales across the observed period
  - Identify top SKUs by total CM1 profit across the observed period
  - Identify weakest SKUs across the observed period
  - Mention:
    - total net sales
    - total CM1 profit
    - overall CM1 margin
  - Clearly state that these refer to the full observed period (NOT a single month)

- SECOND: Latest Month Snapshot (SUPPORTING)
  - Highlight how top SKUs performed in the latest month
  - Include:
    - net sales
    - CM1 profit
    - margin
    - sellable inventory
    - inventory coverage (if available)
  - Explicitly mention the latest month and year

- CRITICAL:
  - Historical analysis MUST dominate the section
  - Latest month should only be used as supporting context
  - Do NOT limit SKU analysis to the latest month

6. Competition Analysis and Comparison
- Identify the business category using business_overview and business_category_hint.
- Based on this category, mention well-known and widely recognized competitors operating in the same product space.

- Competitor identification should follow these rules:
  - Only include widely known and relevant brands
  - Do NOT fabricate unknown or obscure competitors
  - If unsure, avoid naming competitors rather than guessing

- Clearly state:
  "Competitor identification is indicative based on general market knowledge, not dataset-specific."


STYLE:
- Write like a top-tier consultant.
- Use clear paragraphs, not shallow bullet summaries.
- Be specific, numeric, and easy to understand.
- Avoid vague or generic language.
- Do not include recommendations.
- Do not include a separate Key Problems section.
- Do not use markdown symbols like **, #, or bullet markdown.
- You MUST still use plain text numbering (1., 2., 3.) and hyphen (-) bullet points where required.
"""


def generate_business_journey(
    business_context,
    sku_data,
    overall_metrics,
    currency_symbol,
    monthly_trend,
    inventory_trend,
    sku_summary,
    inventory_sales_correlation,
    openai_client
):
    try:
        # =========================================================
        # ✅ SUMMARY METRICS (LATEST MONTH)
        # =========================================================
        summary_metrics = {
            col: (overall_metrics.get(col) or 0)
            for col in [
                "gross_sales",
                "net_sales",
                "profit",
                "profit_percentage",
                "cm2_profit",
                "cm2_profit_percentage",
                "advertising_total",
                "acos"
            ]
        }

        # =========================================================
        # ✅ GET LATEST MONTH LABEL (VERY IMPORTANT FIX)
        # =========================================================
        latest_month_label = None
        if monthly_trend and len(monthly_trend) > 0:
            last_row = monthly_trend[-1]
            latest_month_label = last_row.get("month")

        # =========================================================
        # ✅ TOP SKUs (LATEST MONTH)
        # =========================================================
        top_skus = sorted(
            sku_data,
            key=lambda x: (x.get("net_sales") or 0),
            reverse=True
        )[:5]

        # =========================================================
        # ❌ REMOVE: problem_flags (not needed anymore)
        # ❌ REMOVE: worst_skus (not needed anymore)
        # =========================================================

        # =========================================================
        # ✅ FINAL AI INPUT
        # =========================================================
        ai_input = {
            "business_overview": business_context,
            "business_category_hint": business_context.get("business_context"),

            # Latest month context
            "latest_month_label": latest_month_label,
            "summary_metrics": summary_metrics,
            "overall_metrics": overall_metrics,
            "sku_summary": sku_summary,

            # SKU data
            "top_skus": top_skus,
            "sku_data": sku_data,

            # Trends
            "monthly_trend": monthly_trend,
            "inventory_trend": inventory_trend,

            # Meta
            "currency_symbol": currency_symbol,
            "inventory_sales_correlation": inventory_sales_correlation,
            "data_window_note": "This is a rolling observed period from Amazon SP API, not necessarily the full business history."
        }

        # =========================================================
        # ✅ CALL OPENAI
        # =========================================================
        response = openai_client.chat.completions.create(
            model="gpt-4.1",
            messages=[
                {
                    "role": "system",
                    "content": AI_BUSINESS_JOURNEY_PROMPT
                },
                {
                    "role": "user",
                    "content": json.dumps(ai_input, default=str)
                }
            ],
            temperature=0.3
        )

        return response.choices[0].message.content

    except OpenAIError as e:
        raise Exception(f"OpenAI Error: {str(e)}")
    


def save_business_journey_by_id(chatbot_engine, objective_id, business_journey):
    query = text("""
        UPDATE user_objectives
        SET ai_business_journey = :business_journey,
            updated_at = :updated_at
        WHERE id = :objective_id
    """)

    with chatbot_engine.begin() as conn:
        result = conn.execute(query, {
            "business_journey": str(business_journey),
            "updated_at": datetime.utcnow(),
            "objective_id": int(objective_id)
        })

    print("rowcount:", result.rowcount)

    if result.rowcount > 0:
        print("SAVED SUCCESSFULLY")
        return True
    else:
        print(f"No row found with ID: {objective_id}")
        return False


def fetch_existing_business_journey(
    chatbot_engine,
    user_id,
    country,
    month_name,
    year
):
    month_map = {
        "january": 1, "february": 2, "march": 3, "april": 4,
        "may": 5, "june": 6, "july": 7, "august": 8,
        "september": 9, "october": 10, "november": 11, "december": 12
    }

    month_num = month_map[month_name.lower()]

    query = text("""
        SELECT ai_business_journey
        FROM user_objectives
        WHERE user_id = :user_id
          AND LOWER(country) = :country
          AND EXTRACT(YEAR FROM objective_month) = :year
          AND EXTRACT(MONTH FROM objective_month) = :month_num
        ORDER BY id DESC
        LIMIT 1
    """)

    with chatbot_engine.connect() as conn:
        result = conn.execute(query, {
            "user_id": user_id,
            "country": country.lower(),
            "year": year,
            "month_num": month_num
        }).fetchone()

    if result and result[0]:
        return result[0]

    return None

