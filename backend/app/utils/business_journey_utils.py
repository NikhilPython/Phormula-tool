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


def fetch_skuwise_monthly(phormula_engine, user_id, country, month_name, year):
    table_name = f"skuwisemonthly_{user_id}_{country}_{month_name}{year}"

    check_query = text("""
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = 'public'
              AND table_name = :table_name
        )
    """)

    with phormula_engine.connect() as connection:
        table_exists = connection.execute(
            check_query,
            {"table_name": table_name}
        ).scalar()

        if not table_exists:
            raise Exception(f"SKU monthly table not found: {table_name}")

        query = text(f'SELECT * FROM "{table_name}"')
        df = pd.read_sql(query, connection)

    return df


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
    if sku_df.empty:
        return [], {}

    ai_columns = [
        "sku", "product_name", "return_quantity", "total_quantity",
        "asp", "gross_sales", "tex_and_credits", "net_sales",
        "amazon_fee", "profit", "unit_wise_profitability",
        "profit_percentage", "advertising_total", "lost_total",
        "platformfeenew", "platform_fee",
        "platform_fee_inventory_storage", "cm2_profit",
        "cm2_profit_percentage", "rembursement_fee",
        "rembursment_vs_cm2_margins", "reimbursement_vs_sales",
        "sales_mix", "profit_mix", "acos"
    ]

    available_ai_columns = [col for col in ai_columns if col in sku_df.columns]

    total_row = sku_df[
        (sku_df["sku"].astype(str).str.strip().str.lower() == "total") |
        (sku_df["product_name"].astype(str).str.strip().str.lower() == "total")
    ]

    overall_metrics = total_row.iloc[0].to_dict() if not total_row.empty else {}

    sku_clean_df = sku_df.drop(total_row.index)

    if available_ai_columns:
        sku_clean_df = sku_clean_df[available_ai_columns]

    sku_data = sku_clean_df.to_dict(orient="records")

    return sku_data, overall_metrics


AI_BUSINESS_JOURNEY_PROMPT = """
You are a senior business consultant (like McKinsey/Bain).

Your task is to generate a complete business journey analysis.

IMPORTANT RULES:
- All metrics are pre-calculated. DO NOT recompute anything.
- Always use the numbers provided.
- Always include percentages and absolute values.
- Explain cause and effect relationships.
- Be structured, deep, and analytical.
- Do NOT summarize briefly.

OUTPUT STRUCTURE:

1. Business Overview
2. Revenue & Sales Performance
3. Profitability Analysis
4. Cost & Leakage Analysis
5. SKU-Level Performance
6. Operational Signals
7. Key Problems
8. Strategic Recommendations

STYLE:
- Write like a business consultant
- Use clear paragraphs
- Include numbers in explanations
- Avoid vague statements
"""


def generate_business_journey(
    business_context,
    sku_data,
    overall_metrics,
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
            "sku_data": sku_data
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

