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

    current_month_num = month_map[month_name.lower()]
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
            "platform_fee": float(row.get("platform_fee", 0) or 0),
            "platform_fee_inventory_storage": storage_fee,
            "acos": float(row.get("acos", 0) or 0)
        })

    trend = sorted(trend, key=lambda x: (x["year"], x["month_num"]))
    return trend

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

    df["date"] = pd.to_datetime(df["date"])
    df["year"] = df["date"].dt.year
    df["month"] = df["date"].dt.month

    df = df.sort_values("date")

    # take LAST snapshot per sku + disposition + month
    month_end = (
        df.groupby(["msku", "disposition", "year", "month"], as_index=False)
        .last()
    )

    lookup = {}

    for _, r in month_end.iterrows():

        key = (str(r["msku"]), int(r["year"]), int(r["month"]))

        if key not in lookup:
            lookup[key] = {
                "sellable_inventory": 0,
                "damaged_inventory": 0,
                "expired_inventory": 0
            }

        disp = str(r["disposition"]).upper()
        units = float(r["ending_warehouse_balance"])

        if disp == "SELLABLE":
            lookup[key]["sellable_inventory"] += units

        elif disp in ["DEFECTIVE", "WAREHOUSE_DAMAGED", "CUSTOMER_DAMAGED"]:
            lookup[key]["damaged_inventory"] += units

        elif disp in ["EXPIRED"]:
            lookup[key]["expired_inventory"] += units

    return lookup    


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
- Treat the data as an observed performance window only.

IMPORTANT RULES:
- All metrics are pre-calculated. DO NOT recompute raw source metrics.
- Always use the numbers provided.
- Always include percentages and absolute values where relevant.
- Explain cause-and-effect relationships clearly.
- Make the output understandable for someone who knows nothing about the business.
- This must feel like a business journey, not a static overview.

CURRENCY RULE:
- Use the currency_symbol provided in input.
- Do NOT assume $.
- Format values like: £12,345 or $12,345.

DATA INTERPRETATION RULES:
- monthly_trend is the most important input for journey analysis.
- monthly_trend is chronological monthly business performance for the observed data window.
- sku_data is the latest month's SKU-level breakdown.
- overall_metrics is the latest month's aggregated total row.
- summary_metrics is the latest month's key headline metrics.
- Do NOT assign overall business-level costs directly to individual SKUs unless explicitly present at SKU level.

COST INTERPRETATION RULE:
- All cost values should be interpreted as positive costs.
- Higher values mean higher cost burden, not savings.

INVENTORY RULES:
- Use sellable_inventory, damaged_inventory, expired_inventory, and inventory_coverage_days only when present in the latest SKU data.
- If inventory coverage is zero or consistently low, highlight stockout risk.
- If damaged or expired inventory exists, explain the business impact.

TREND ANALYSIS RULES:
- Use monthly_trend to tell the story of how the business changed over time.
- Identify:
  - highest month by net sales
  - highest month by profit
  - highest month by units
  - lowest month by net sales
  - lowest month by profit
  - major rises, declines, volatility, and recovery phases
- Mention growth and decline percentages wherever meaningful from the observed monthly data.
- Explicitly mention when the business was strongest and when it was weakest during the observed period.
- If the trend is volatile, say so clearly.
- If the trend improved over time, explain when the improvement started.
- If the trend worsened over time, explain when the slowdown or decline started.

OUTPUT STRUCTURE:

1. Business Context
- Briefly explain the business, goals, and priorities from business_overview.

2. Business Journey Across the Observed Period
- Tell the chronological story of the business using monthly_trend.
- Mention the first and last observed month.
- Highlight major phases in the observed period:
  - growth periods
  - decline periods
  - recovery periods
  - volatile periods
- Mention the highest and lowest months for sales, profit, and units.
- Include percentage changes when describing rises and falls.

3. Latest Month Performance
- Analyze the latest month using summary_metrics and overall_metrics.
- Clearly state latest gross sales, net sales, profit, margin, CM2, and major cost pressures.

4. Revenue & Sales Analysis
- Explain revenue drivers and sales concentration.
- Use latest month sku_data to identify which SKUs are driving the business now.

5. Profitability Analysis
- Explain margin strength or weakness over the observed period and in the latest month.
- Identify which products are helping or hurting profitability.

6. Cost & Leakage Analysis
- Analyze advertising, platform fees, storage fees, and lost_total.
- Explain how these affect business health.

7. SKU-Level Insights
- Identify top-performing and weakest SKUs in the latest month.
- Explain their contribution to sales and profit.

8. Key Problems
- Clearly prioritize the biggest business issues based on trend + latest month.

9. Strategic Recommendations
- Give practical recommendations on what to scale, fix, reduce, or stop.
- Tie every recommendation back to the data.

STYLE:
- Write like a top-tier consultant.
- Use clear paragraphs, not shallow bullet summaries.
- Be specific, numeric, and easy to understand.
- Avoid vague or generic language.
"""


def generate_business_journey(
    business_context,
    sku_data,
    overall_metrics,
    currency_symbol,
    monthly_trend,
    openai_client
):
    try:
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

        top_skus = sorted(
            sku_data,
            key=lambda x: (x.get("net_sales") or 0),
            reverse=True
        )[:5]

        worst_skus = sorted(
            sku_data,
            key=lambda x: (x.get("profit") or 0)
        )[:5]

        problem_flags = {
            "low_margin": (summary_metrics.get("profit_percentage") or 0) < 10,
            "high_acos": (summary_metrics.get("acos") or 0) > 30,
        }

        ai_input = {
            "business_overview": business_context,
            "summary_metrics": summary_metrics,
            "overall_metrics": overall_metrics,
            "top_skus": top_skus,
            "worst_skus": worst_skus,
            "problem_flags": problem_flags,
            "sku_data": sku_data,
            "currency_symbol": currency_symbol,
            "monthly_trend": monthly_trend,
            "data_window_note": "This is a rolling observed period from Amazon SP API, not necessarily the full business history."
        }

        response = openai_client.chat.completions.create(
            model="gpt-4.1-mini",
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

