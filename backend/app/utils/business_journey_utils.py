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
import numpy as np
from app.models.user_models import HistoricAISummary, UserObjective
from app.utils.formulas_utils import safe_num
from app.utils.uk_prompts_utils import AI_SYSTEM_PROMPT_1, AI_SYSTEM_PROMPT_2, AI_SYSTEM_PROMPT_3_POLISHER, get_excel_recommendation_from_metrics
from app import db
from openai import OpenAIError
from app.utils.uk_coverage_ratio_utils import compute_inventory_coverage_ratio




load_dotenv()
SECRET_KEY = Config.SECRET_KEY

db_url = os.getenv("DATABASE_URL")
db_url2 = os.getenv("DATABASE_CHATBOT_URL")
db_url3 = os.getenv("DATABASE_AMAZON_URL")
phormula_engine = create_engine(db_url)
chatbot_engine = create_engine(db_url2)
amazon_engine = create_engine(db_url3)
openai_client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))


def fetch_skuwise_monthly(phormula_engine, user_id, country, month_name, year):
    """
    Fetch all data from table:
    skuwisemonthly_{user_id}_{country}_{month_name}{year}
    """

    table_name = f"skuwisemonthly_{user_id}_{country}_{month_name}{year}"

    query = text(f"SELECT * FROM {table_name}")

    with phormula_engine.connect() as connection:
        df = pd.read_sql(query, connection)

    return df


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
    """
    Fetch business context from user_objectives
    """

    month_map = {
        "january": 1, "february": 2, "march": 3, "april": 4,
        "may": 5, "june": 6, "july": 7, "august": 8,
        "september": 9, "october": 10, "november": 11, "december": 12
    }

    month_num = month_map[month_name.lower()]
    objective_month = date(year, month_num, 1)

    query = text("""
        SELECT
            id,      
            growth_intent,
            profit_priority,
            inventory_clearance_priority,
            business_context,
            website_url
        FROM user_objectives
        WHERE user_id = :user_id
          AND country = :country
          AND objective_month = :objective_month
        LIMIT 1
    """)

    with chatbot_engine.connect() as connection:
        df = pd.read_sql(
            query,
            connection,
            params={
                "user_id": user_id,
                "country": country,
                "objective_month": objective_month
            }
        )

    return df

def prepare_ai_sku_data(sku_df):
    """
    Clean SKU data for AI:
    - Select required columns
    - Separate total row
    - Extract overall metrics
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
        "advertising_total",
        "lost_total",
        "platformfeenew",
        "platform_fee",
        "platform_fee_inventory_storage",
        "cm2_profit",
        "cm2_profit_percentage",
        "rembursement_fee",
        "rembursment_vs_cm2_margins",
        "reimbursement_vs_sales",
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

    # ----------- FINAL OUTPUT -----------
    sku_data = sku_clean_df.to_dict(orient="records")

    overall_metrics_filtered = {
        col: overall_metrics.get(col)
        for col in GLOBAL_COLUMNS
    }

    return sku_data, overall_metrics_filtered    

AI_BUSINESS_JOURNEY_PROMPT = """
You are a senior business consultant (like McKinsey/Bain).

Your task is to generate a complete business journey analysis.

IMPORTANT RULES:
- All metrics are pre-calculated. DO NOT recompute anything.
- Always use the numbers provided.
- Always include percentages and absolute values.
- Explain cause → effect relationships.
- Be structured, deep, and analytical (not generic).
- Do NOT summarize briefly — explain in detail.

OUTPUT STRUCTURE:

1. Business Overview
- Understand the business context, goals, and priorities.

2. Revenue & Sales Performance
- Analyze gross_sales, net_sales, sales_mix
- Identify concentration, dependency, and revenue drivers

3. Profitability Analysis
- Use profit, profit_percentage, cm2_profit, cm2_profit_percentage
- Explain margin health and sustainability

4. Cost & Leakage Analysis
- Analyze advertising_total, platform_fee, lost_total
- Identify where money is leaking and why

5. SKU-Level Performance
- Identify top performing SKUs and loss-making SKUs
- Explain contribution to revenue and profit

6. Operational Signals
- Returns, inefficiencies, reimbursement patterns

7. Key Problems
- Clearly highlight major business risks

8. Strategic Recommendations
- Actionable steps: what to scale, fix, or stop

STYLE:
- Write like a business consultant
- Use clear paragraphs (not bullet spam)
- Include numbers in explanations
- Avoid vague statements
"""    

def generate_business_journey(
    business_context,
    sku_data,
    overall_metrics,
    openai_client
):
    """
    Generate full business journey using AI
    """

    try:
        # -------- SUMMARY METRICS FROM TOTAL ROW --------
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

        # -------- SIMPLE RANKING --------
        top_skus = sorted(
            sku_data,
            key=lambda x: (x.get("net_sales") or 0),
            reverse=True
        )[:5]

        worst_skus = sorted(
            sku_data,
            key=lambda x: (x.get("profit") or 0)
        )[:5]

        # -------- FLAGS --------
        problem_flags = {
            "low_margin": (summary_metrics.get("profit_percentage") or 0) < 10,
            "high_acos": (summary_metrics.get("acos") or 0) > 30,
        }

        # -------- FINAL INPUT --------
        ai_input = {
            "business_overview": business_context,
            "summary_metrics": summary_metrics,
            "overall_metrics": overall_metrics,
            "top_skus": top_skus,
            "worst_skus": worst_skus,
            "problem_flags": problem_flags,
            "sku_data": sku_data
        }

        # -------- OPENAI CALL --------
        response = openai_client.chat.completions.create(
            model="gpt-4.1-mini",
            messages=[
                {
                    "role": "system",
                    "content": AI_BUSINESS_JOURNEY_PROMPT
                },
                {
                    "role": "user",
                    "content": json.dumps(ai_input)
                }
            ],
            temperature=0.3
        )

        return response.choices[0].message.content

    except OpenAIError as e:
        return f"OpenAI Error: {str(e)}"
    
def save_business_journey_by_id(chatbot_engine, objective_id, business_journey):

    query = text("""
        UPDATE user_objectives
        SET ai_business_journey = :business_journey,
            updated_at = NOW()
        WHERE id = :id
    """)

    with chatbot_engine.connect() as conn:
        result = conn.execute(query, {
            "business_journey": business_journey,
            "id": objective_id
        })
        print("ROWS UPDATED:", result.rowcount)  # 🔥 DEBUG
        conn.commit()

        
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
    objective_month = date(year, month_num, 1)

    query = text("""
        SELECT ai_business_journey
        FROM user_objectives
        WHERE user_id = :user_id
          AND country = :country
          AND objective_month = :objective_month
        LIMIT 1
    """)

    with chatbot_engine.connect() as conn:
        result = conn.execute(query, {
            "user_id": user_id,
            "country": country,
            "objective_month": objective_month
        }).fetchone()

    if result and result[0]:
        return result[0]

    return None            