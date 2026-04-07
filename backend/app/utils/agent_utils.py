from __future__ import annotations
from dotenv import load_dotenv
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
import json
from openai import OpenAI
from config import Config
import os
from typing import Any, Dict, List, Optional, TypedDict
from pydantic import BaseModel, Field
from langgraph.graph import StateGraph, START, END
from langgraph.checkpoint.memory import MemorySaver
from langchain_openai import ChatOpenAI
from sqlalchemy import create_engine, text
import io
import numpy as np
from concurrent.futures import ThreadPoolExecutor, as_completed

openai_client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

llm = ChatOpenAI(
    model="gpt-4.1",
    api_key=os.getenv("OPENAI_API_KEY"),
    temperature=0.2,
)


load_dotenv()
db_url = os.getenv("DATABASE_URL")
db_url2 = os.getenv("DATABASE_CHATBOT_URL")
db_url3 = os.getenv("DATABASE_AMAZON_URL")

phormula_engine = create_engine(db_url)
chatbot_engine = create_engine(db_url2)
amazon_engine = create_engine(db_url3)

COUNTRY_MAP = {
    "uk": "gb",
    "gb": "gb",
    "us": "us",
    "de": "de",
    "fr": "fr",
    "it": "it",
    "es": "es"
}

def get_currency_symbol(country: str) -> str:
    country = (country or "").strip().lower()
    if country == "uk":
        return "£"
    if country in {"global", "us"}:
        return "$"
    return "$"

def load_latest_inventory_snapshot(amazon_engine, user_id: int, country: str) -> pd.DataFrame:
    country = COUNTRY_MAP.get(country.lower(), country.lower())

    query = text("""
        SELECT *
        FROM monthwise_inventory
        WHERE user_id = :user_id
          AND LOWER(location) = :country
          AND disposition = 'SELLABLE'
    """)

    df = pd.read_sql(
        query,
        amazon_engine,
        params={
            "user_id": user_id,
            "country": country
        }
    )

    if df.empty:
        return pd.DataFrame(columns=["sku", "inventory"])

    # ensure datetime
    df["date"] = pd.to_datetime(df["date"])

    # ✅ only latest snapshot
    latest_date = df["date"].max()
    latest_df = df[df["date"] == latest_date].copy()

    # normalize SKU
    latest_df["sku"] = latest_df["msku"].astype(str).str.strip()

    # inventory
    latest_df["inventory"] = (
        pd.to_numeric(latest_df["ending_warehouse_balance"], errors="coerce")
        .fillna(0)
    )

    # ✅ group by SKU (important — same SKU can have multiple rows)
    final_df = (
        latest_df.groupby("sku", as_index=False)
        .agg(inventory=("inventory", "sum"))
    )

    return final_df

def get_inventory_for_scope(inventory_df, scope_level, scope_value):
    if inventory_df.empty:
        return 0.0

    # normalize inventory SKUs once
    inventory_df["sku"] = inventory_df["sku"].astype(str).str.strip().str.upper()

    if scope_level == "sku" and scope_value:
        # normalize input SKU
        scope_value = str(scope_value).strip().upper()

        row = inventory_df[inventory_df["sku"] == scope_value]

        if row.empty:
            return 0.0

        return float(row["inventory"].sum())

    return float(inventory_df["inventory"].sum())




MONTH_NAMES = {
    1: "january", 2: "february", 3: "march", 4: "april",
    5: "may", 6: "june", 7: "july", 8: "august",
    9: "september", 10: "october", 11: "november", 12: "december"
}


def build_table_name(user_id, country, month, year):
    return f"skuwisemonthly_{user_id}_{country}_{MONTH_NAMES[month]}{year}"


def generate_last_24_months(year, month):
    base = datetime(year, month, 1)
    return [( (base - relativedelta(months=i)).year,
              (base - relativedelta(months=i)).month )
            for i in range(24)][::-1]


def load_sales_data(phormula_engine, user_id, country, year, month):
    periods = generate_last_24_months(year, month)

    frames = []

    for y, m in periods:
        table = build_table_name(user_id, country, m, y)

        try:
            df = pd.read_sql(f'SELECT * FROM "{table}"', phormula_engine)
            df["year"] = y
            df["month"] = m
            frames.append(df)
        except Exception:
            continue

    if not frames:
        raise ValueError("No sales data found")

    df = pd.concat(frames, ignore_index=True)

    # ✅ CLEAN ALL IMPORTANT NUMERIC COLUMNS
    for col in ["quantity", "total_quantity", "net_sales", "profit", "sales_mix", "profit_mix"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    df["sku"] = df["sku"].astype(str).str.strip()
    df = df[~df["sku"].str.upper().isin(["TOTAL", "ALL", "GRAND TOTAL"])]

    return df

def build_history_snapshot(monthly: pd.DataFrame) -> Dict[str, Any]:
    if monthly.empty:
        return {}

    monthly_sorted = monthly.sort_values(["year", "month"]).copy()

    latest_row = monthly_sorted.iloc[-1]
    highest_units_row = monthly_sorted.loc[monthly_sorted["quantity"].idxmax()]

    if "asp" in monthly_sorted.columns and monthly_sorted["asp"].notna().any():
        highest_asp_row = monthly_sorted.loc[monthly_sorted["asp"].idxmax()]
        lowest_asp_row = monthly_sorted.loc[monthly_sorted["asp"].idxmin()]
    else:
        highest_asp_row = latest_row
        lowest_asp_row = latest_row

    return {
        "latest_month": {
            "year": int(latest_row["year"]),
            "month": int(latest_row["month"]),
            "units": float(latest_row["quantity"]),
            "asp": float(latest_row["asp"]) if "asp" in latest_row else 0.0,
            "sales": float(latest_row["net_sales"]),
        },
        "highest_units_month": {
            "year": int(highest_units_row["year"]),
            "month": int(highest_units_row["month"]),
            "units": float(highest_units_row["quantity"]),
            "asp": float(highest_units_row["asp"]) if "asp" in highest_units_row else 0.0,
            "sales": float(highest_units_row["net_sales"]),
        },
        "highest_asp_month": {
            "year": int(highest_asp_row["year"]),
            "month": int(highest_asp_row["month"]),
            "units": float(highest_asp_row["quantity"]),
            "asp": float(highest_asp_row["asp"]) if "asp" in highest_asp_row else 0.0,
            "sales": float(highest_asp_row["net_sales"]),
        },
        "lowest_asp_month": {
            "year": int(lowest_asp_row["year"]),
            "month": int(lowest_asp_row["month"]),
            "units": float(lowest_asp_row["quantity"]),
            "asp": float(lowest_asp_row["asp"]) if "asp" in lowest_asp_row else 0.0,
            "sales": float(lowest_asp_row["net_sales"]),
        },
    }



def filter_scope(df, level, value):
    if level == "sku" and value:
        return df[df["sku"] == value]

    # ❌ remove fallback to overall
    return df.iloc[0:0]  # empty dataframe


def aggregate_monthly(df):
    agg_dict = {
        "quantity": ("quantity", "sum"),
        "net_sales": ("net_sales", "sum"),
        "asp": ("asp", "mean"),
    }

    # 🔥 IMPORTANT: include profit if available
    if "profit" in df.columns:
        agg_dict["profit"] = ("profit", "sum")

    return (
        df.groupby(["year", "month"], as_index=False)
        .agg(**agg_dict)
        .sort_values(["year", "month"])
    )


def event_uplift(monthly, month, year):
    event = monthly[(monthly["month"] == month) & (monthly["year"] == year)]

    if event.empty:
        return {
            "quantity_lift": 0.0,
            "sales_lift": 0.0,
            "event_quantity": 0.0,
            "event_sales": 0.0,
        }

    baseline = monthly[~((monthly["month"] == month) & (monthly["year"] == year))]

    if baseline.empty:
        base_q = 0.0
        base_s = 0.0
    else:
        base_q = float(baseline["quantity"].mean())
        base_s = float(baseline["net_sales"].mean())

    event_q = float(event["quantity"].iloc[0])
    event_s = float(event["net_sales"].iloc[0])

    return {
        "quantity_lift": float(((event_q - base_q) / base_q * 100) if base_q else 0.0),
        "sales_lift": float(((event_s - base_s) / base_s * 100) if base_s else 0.0),
        "event_quantity": float(event_q),
        "event_sales": float(event_s),
    }


def load_inventory_forecast(engine, user_id, country):
    query = text("""
        SELECT data
        FROM public.stored_files
        WHERE user_id = :user_id
          AND LOWER(country) = :country
          AND kind = 'inventory_forecast'
        ORDER BY id DESC
        LIMIT 1
    """)

    with engine.connect() as conn:
        row = conn.execute(query, {
            "user_id": user_id,
            "country": country.lower()
        }).fetchone()

    if not row:
        raise ValueError("No inventory forecast found")

    df = pd.read_excel(io.BytesIO(row[0]), header=6)

    df.columns = df.columns.str.strip()
    df["sku"] = df["sku"].astype(str).str.strip().str.upper()

    df = df[~df["sku"].str.contains("total", case=False, na=False)]

    return df


def load_country_profile(engine, user_id, country):
    query = text("""
        SELECT transit_time, stock_unit
        FROM public.country_profile
        WHERE user_id = :user_id
          AND LOWER(country) = :country
        LIMIT 1
    """)

    with engine.connect() as conn:
        row = conn.execute(query, {
            "user_id": user_id,
            "country": country.lower()
        }).fetchone()

    if not row:
        return {"transit_time": 2, "stock_unit": 2}  # fallback

    return {
        "transit_time": row[0],
        "stock_unit": row[1]
    }

def get_forecast_column(future_event):
    month_map = {
        1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr",
        5: "May", 6: "Jun", 7: "Jul", 8: "Aug",
        9: "Sep", 10: "Oct", 11: "Nov", 12: "Dec"
    }

    month_str = month_map[future_event["month"]]
    year_str = str(future_event["year"])[-2:]

    return f"{month_str}'{year_str}"



       


class ExecutionPlan(BaseModel):
    summary: List[str]
    history_story: List[str]
    current_status: List[str]
    recommended_price_range: List[str]
    range_impact_overview: List[str]
    profitability_guardrail: List[str]
    actions: List[str]
    pricing_decision: List[str]
    target_sales_note: List[str]
    final_recommendation: List[str]
    priority: str


class EventPlannerState(TypedDict, total=False):
    # request
    user_id: int
    country: str
    last_event: Dict[str, Any]
    future_event: Dict[str, Any]
    scope: Dict[str, Any]
    country_profile: Dict[str, Any]
    target_sales: Optional[float]

    # infra
    phormula_engine: Any
    amazon_engine: Any

    # raw data
    sales_df: Any
    inventory_df: Any
    forecast_df: Any
    history: Dict[str, Any]
    sales_mix_pct: float
    profit_mix_pct: float

    # processed
    scoped_sales_df: Any
    monthly_df: Any
    uplift: Dict[str, Any]
    forecast: Dict[str, Any]
    top_skus: List[Dict[str, Any]]
    category_summary: List[Dict[str, Any]]
    risk_signals: Dict[str, Any]
    analytics: Dict[str, Any]
    optimization: Dict[str, Any]

    # plan
    execution_plan: Dict[str, Any]
    validation_errors: List[str]
    retry_count: int
    is_valid: bool


def summarize_top_skus(df: pd.DataFrame, top_n: int = 10) -> List[Dict[str, Any]]:
    if df.empty:
        return []

    cols = [c for c in ["sku", "product_name", "quantity", "net_sales", "profit", "cm2_profit"] if c in df.columns]
    working = df[cols].copy()

    agg_map = {}
    for col in ["quantity", "net_sales", "profit", "cm2_profit"]:
        if col in working.columns:
            agg_map[col] = "sum"

    top = (
        working.groupby(["sku", "product_name"], as_index=False)
        .agg(agg_map)
        .sort_values("net_sales" if "net_sales" in agg_map else "quantity", ascending=False)
        .head(top_n)
    )

    return top.to_dict(orient="records")


def summarize_categories(df: pd.DataFrame, top_n: int = 8) -> List[Dict[str, Any]]:
    if df.empty or "category" not in df.columns:
        return []

    agg_dict = {
        "quantity": ("quantity", "sum"),
        "net_sales": ("net_sales", "sum"),
    }
    if "profit" in df.columns:
        agg_dict["profit"] = ("profit", "sum")

    top = (
        df.groupby("category", as_index=False)
        .agg(**agg_dict)
        .sort_values("net_sales", ascending=False)
        .head(top_n)
    )

    return top.to_dict(orient="records")

def generate_price_candidates(monthly_df):
    asps = monthly_df["asp"].dropna().unique()

    if len(asps) == 0:
        return []

    min_asp = float(min(asps))
    max_asp = float(max(asps))

    return np.linspace(min_asp, max_asp, 8)

def estimate_units_at_price(monthly_df, target_price):
    df = monthly_df.dropna(subset=["asp", "quantity"]).copy()

    if len(df) < 2:
        return float(df["quantity"].mean())

    df = df[df["asp"] > 0]

    x = np.log(df["asp"])
    y = np.log(df["quantity"])

    slope, intercept = np.polyfit(x, y, 1)

    # 🔥 CRITICAL: force negative elasticity
    slope = min(slope, -0.3)

    base_price = df["asp"].mean()
    base_units = df["quantity"].mean()

    # relative model (more stable)
    adjusted_units = base_units * (target_price / base_price) ** slope

    # 🔥 bounds (very important)
    adjusted_units = max(min(adjusted_units, base_units * 2), base_units * 0.3)

    return float(adjusted_units)

def estimate_profit(units, price, margin_pct):
    return float(units * price * margin_pct)

def get_bau_profit(monthly_df):
    if "profit" not in monthly_df.columns:
        return None

    recent = monthly_df.sort_values(["year","month"]).tail(3)
    return float(recent["profit"].mean())




def optimize_price_with_constraint(monthly_df, margin_pct):

    # 🔥 Safety check: basic data required
    if monthly_df.empty or "asp" not in monthly_df.columns:
        # print("WARNING: optimization skipped due to missing data")
        return {
            "price_points": [],
            "bau_profit": None,
            "price_range": {}
        }

    # 🔥 Ensure numeric safety
    monthly_df = monthly_df.copy()
    monthly_df["asp"] = pd.to_numeric(monthly_df["asp"], errors="coerce")
    monthly_df["quantity"] = pd.to_numeric(monthly_df.get("quantity", 0), errors="coerce")

    monthly_df = monthly_df.dropna(subset=["asp", "quantity"])

    if monthly_df.empty:
        # print("WARNING: no valid asp/quantity data after cleaning")
        return {
            "price_points": [],
            "bau_profit": None,
            "price_range": {}
        }

    # 🔥 Generate candidate prices
    candidates = generate_price_candidates(monthly_df)

    if len(candidates) == 0:
        # print("WARNING: no price candidates generated")
        return {
            "price_points": [],
            "bau_profit": None,
            "price_range": {}
        }

    # 🔥 BAU profit
    bau_profit = get_bau_profit(monthly_df)
    # print("DEBUG: BAU profit:", bau_profit)

    results = []

    # 🔥 current ASP (anchor)
    current_asp = float(monthly_df["asp"].mean())

    for price in candidates:
        try:
            # 🔥 PRICE FLOOR GUARD
            if price < current_asp * 0.8:
                continue

            units = estimate_units_at_price(monthly_df, price)

            if units is None or np.isnan(units):
                units = 0.0

            units = max(1, int(round(float(units))))
            sales = units * price
            profit = estimate_profit(units, price, margin_pct)

            if np.isnan(sales):
                sales = 0.0
            if np.isnan(profit):
                profit = 0.0

            # 🔥 Profit guardrail
            is_valid = True
            if bau_profit is not None:
                is_valid = profit >= 1.05 * bau_profit

            results.append({
                "price": round(float(price), 2),
                "units": int(round(float(units))),
                "sales": round(float(sales), 2),
                "profit": round(float(profit), 2),
                "is_valid": is_valid
            })

        except Exception as e:
            # print(f"ERROR in candidate price {price}: {str(e)}")
            continue

    if not results:
        # print("WARNING: no results generated in optimization")
        return {
            "price_points": [],
            "bau_profit": round(bau_profit, 2) if bau_profit else None,
            "price_range": {}
        }

    # 🔥 Separate valid vs all
    valid_results = [r for r in results if r["is_valid"]]

    if not valid_results:
        # print("WARNING: no valid results under BAU constraint → using all results")
        valid_results = results

    # 🔥 SORT for better interpretation
    valid_results = sorted(valid_results, key=lambda x: x["price"])

    # 🔥 PRICE RANGE
    min_price = valid_results[0]["price"]
    max_price = valid_results[-1]["price"]

    # 🔥 MID POINT (important for LLM reasoning)
    if len(valid_results) >= 3:
        mid_point = valid_results[len(valid_results)//2]
    else:
        mid_point = valid_results[0]

    # print("DEBUG: total candidates:", len(results))
    # print("DEBUG: valid candidates:", len(valid_results))
    # print("DEBUG: price range:", min_price, "to", max_price)

    return {
        "price_points": valid_results,   # 🔥 full landscape
        "bau_profit": round(bau_profit, 2) if bau_profit else None,
        "price_range": {
            "min": min_price,
            "max": max_price,
            "mid": mid_point  # optional but VERY useful
        }
    }

def load_context_node(state: EventPlannerState) -> EventPlannerState:
    scope = state.get("scope") or {"level": "sku", "value": None}
    return {
        **state,
        "scope": {
            "level": scope.get("level", "overall"),
            "value": scope.get("value"),
        },
        "retry_count": state.get("retry_count", 0),
        "validation_errors": [],
    }

def make_load_sales_inventory_node(phormula_engine, amazon_engine):
    def load_sales_inventory_node(state: EventPlannerState) -> EventPlannerState:
        future_event = state["future_event"]

        sales_df = load_sales_data(
            phormula_engine,
            state["user_id"],
            state["country"],
            future_event["year"],
            future_event["month"],
        )

        inventory_df = load_latest_inventory_snapshot(
            amazon_engine,
            state["user_id"],
            state["country"],
        )

        # ✅ Forecast from DB
        forecast_df = load_inventory_forecast(
            phormula_engine,
            state["user_id"],
            state["country"],
        )

        # 🔥 NEW: Country profile from DB
        country_profile = load_country_profile(
            phormula_engine,
            state["user_id"],
            state["country"],
        )

        return {
            "sales_df": sales_df.to_dict("records"),
            "inventory_df": inventory_df.to_dict("records"),
            "forecast_df": forecast_df.to_dict("records"),
            "country_profile": country_profile,   # ✅ ADD THIS
        }

    return load_sales_inventory_node

def analyze_scope_node(state: EventPlannerState) -> EventPlannerState:
    scope = state["scope"]

    sales_df = pd.DataFrame(state["sales_df"])
    sales_df = filter_scope(sales_df, scope["level"], scope["value"])

    if sales_df.empty:
        raise ValueError("No sales data found for requested scope")

    monthly_df = aggregate_monthly(sales_df)

    # print("DEBUG: analyze_scope_node")
    # print("monthly_df shape:", monthly_df.shape)
    # print(monthly_df.tail(3))

    top_skus = summarize_top_skus(sales_df, top_n=10)
    category_summary = summarize_categories(sales_df, top_n=8)
    history = build_history_snapshot(monthly_df)

    # 🔥 FIXED: stable sales_mix & profit_mix calculation
    sales_mix_pct = 0.0
    profit_mix_pct = 0.0

    if "sales_mix" in sales_df.columns and not sales_df["sales_mix"].dropna().empty:
        sales_mix_pct = float(sales_df["sales_mix"].mean())

    if "profit_mix" in sales_df.columns and not sales_df["profit_mix"].dropna().empty:
        profit_mix_pct = float(sales_df["profit_mix"].mean())

    # 🔥 EXTRA SAFETY: cap between 0–100
    sales_mix_pct = max(0.0, min(100.0, sales_mix_pct))
    profit_mix_pct = max(0.0, min(100.0, profit_mix_pct))

    # print("sales_mix_pct:", round(sales_mix_pct, 2))
    # print("profit_mix_pct:", round(profit_mix_pct, 2))

    return {
        "scoped_sales_df": sales_df.to_dict("records"),
        "monthly_df": monthly_df.to_dict("records"),
        "top_skus": top_skus,
        "category_summary": category_summary,
        "history": history,

        # 🔥 NEW FIELDS (FIXED)
        "sales_mix_pct": round(sales_mix_pct, 2),
        "profit_mix_pct": round(profit_mix_pct, 2),
    }

def compute_forecast_node(state: EventPlannerState) -> EventPlannerState:
    monthly = pd.DataFrame(state["monthly_df"])

    # 🔥 STEP 1: sort and get recent months
    monthly_sorted = monthly.sort_values(["year", "month"])
    recent = monthly_sorted.tail(min(3, len(monthly_sorted)))

    # 🔥 STEP 2: weighted ASP (recent)
    if "asp" in recent.columns and recent["quantity"].sum() > 0:
        asp = float(
            (recent["asp"] * recent["quantity"]).sum()
            / recent["quantity"].sum()
        )
    else:
        asp = 0.0

    # 🔥 STEP 3: lowest ASP (price floor)
    if "asp" in monthly_sorted.columns and monthly_sorted["asp"].notna().any():
        lowest_asp = float(monthly_sorted["asp"].min())
    else:
        lowest_asp = 0.0

    inventory_df = pd.DataFrame(state["inventory_df"])
    forecast_df = pd.DataFrame(state["forecast_df"])

    last_event = state["last_event"]
    scope = state["scope"]
    sku = str(scope["value"]).strip().upper()

    # 🔥 STEP 4: uplift
    uplift = event_uplift(
        monthly,
        last_event["month"],
        last_event["year"],
    )

    # 🔥 STEP 5: inventory
    inventory_units = float(get_inventory_for_scope(
        inventory_df,
        scope["level"],
        scope["value"],
    ))

    # 🔥 STEP 6: forecast lookup
    future_event = state["future_event"]

    month_map = {
        1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr",
        5: "May", 6: "Jun", 7: "Jul", 8: "Aug",
        9: "Sep", 10: "Oct", 11: "Nov", 12: "Dec"
    }

    forecast_col = f"{month_map[future_event['month']]}'{str(future_event['year'])[-2:]}"

    if forecast_col not in forecast_df.columns:
        raise ValueError(f"{forecast_col} not found in forecast file")

    row = forecast_df[forecast_df["sku"] == sku]

    if row.empty:
        raise ValueError(f"No forecast found for SKU {sku}")

    base_q = float(row[forecast_col].iloc[0])

    if "Projected Sales Total" in row.columns:
        base_s = float(row["Projected Sales Total"].iloc[0])
    else:
        base_s = 0.0

    # 🔥 STEP 7: apply uplift
    adj_q = float(base_q * (1 + uplift["quantity_lift"] / 100))
    adj_s = float(base_s * (1 + uplift["sales_lift"] / 100))

    # 🔥 STEP 8: inventory policy
    country_profile = state["country_profile"]
    transit_time = float(country_profile.get("transit_time", 2))
    stock_unit = float(country_profile.get("stock_unit", 2))

    coverage_months = float(transit_time + stock_unit)
    monthly_demand = float(adj_q)

    recommended = float(monthly_demand * coverage_months)
    safety = float(monthly_demand * stock_unit)
    procurement = float(max(0.0, recommended - inventory_units))

    # 🔥 STEP 9: risk detection
    stock_gap = float(adj_q - inventory_units)

    if stock_gap > 0:
        risk_type = "stockout_risk"
    elif inventory_units > adj_q * 1.5:
        risk_type = "overstock_risk"
    else:
        risk_type = "balanced"

    # 🔥 STEP 10: fallback ASP
    if asp <= 0 and base_q > 0 and base_s > 0:
        asp = float(base_s / base_q)

    # 🔥 STEP 11: pricing range
    if asp > 0:
        if risk_type == "stockout_risk":
            price_min = float(asp * 1.05)
            price_max = float(asp * 1.25)

        elif risk_type == "overstock_risk":
            price_min = float(asp * 0.70)
            price_max = float(asp * 0.95)

        else:
            price_min = float(asp * 0.90)
            price_max = float(asp * 1.10)
    else:
        price_min, price_max = 0.0, 0.0

    # 🔥 STEP 12: enforce lowest ASP floor
    if lowest_asp > 0:
        price_min = max(price_min, lowest_asp)
        price_max = max(price_max, price_min)

    # 🔥 STEP 13: safety check
    if price_min > price_max:
        price_min, price_max = price_max, price_min

    # 🔥 STEP 14: target sales logic
    target_sales = state.get("target_sales")
    target_price = None

    if target_sales is not None and adj_q > 0:
        target_price = float(target_sales) / float(adj_q)
        target_price = float(max(price_min, min(price_max, target_price)))

    # 🔥 STEP 15: forecast output
    forecast = {
        "base_quantity": float(base_q),
        "adjusted_quantity": float(adj_q),
        "base_sales": float(base_s),
        "adjusted_sales": float(adj_s),
        "inventory": float(inventory_units),
        "safety_stock": float(safety),
        "recommended_stock": float(recommended),
        "procurement": float(procurement),
        "coverage_months": float(coverage_months),
        "asp": float(asp),
    }

    # 🔥 STEP 16: pricing output
    pricing_output = {
        "asp": float(round(asp, 2)),
        "price_min": float(round(price_min, 2)),
        "price_max": float(round(price_max, 2)),
        "lowest_asp": float(round(lowest_asp, 2)) if lowest_asp > 0 else 0.0,
        "risk_type": risk_type,
    }

    if target_price is not None:
        pricing_output["target_price"] = float(round(target_price, 2))

    return {
        "uplift": {
            "quantity_lift": float(uplift["quantity_lift"]),
            "sales_lift": float(uplift["sales_lift"]),
            "event_quantity": float(uplift["event_quantity"]),
            "event_sales": float(uplift["event_sales"]),
        },
        "forecast": forecast,
        "pricing": pricing_output,
    }

def optimize_pricing_node(state: EventPlannerState) -> EventPlannerState:

    # print("DEBUG: optimize_pricing_node START")

    # ✅ Safe load
    monthly_df = pd.DataFrame(state.get("monthly_df", []))

    # print("monthly_df received shape:", monthly_df.shape)
    # if not monthly_df.empty:
    #     print(monthly_df.tail(3))
    # else:
    #     print("WARNING: monthly_df is EMPTY")

    # 🚨 If no data, skip optimization but still preserve state
    if monthly_df.empty:
        return {
            "optimization": {},
        }

    # ✅ Margin calculation
    top_skus = state.get("top_skus", [])

    if top_skus:
        total_sales = sum(x.get("net_sales", 0) for x in top_skus)
        total_profit = sum(x.get("profit", 0) for x in top_skus)

        margin_pct = (total_profit / total_sales) if total_sales > 0 else 0.3
    else:
        margin_pct = 0.3

    # print("margin_pct:", round(margin_pct, 4))

    # 🚨 Safety: if asp missing → skip
    if "asp" not in monthly_df.columns:
        # print("WARNING: 'asp' column missing in monthly_df")
        return {
            "optimization": {},
        }

    try:
        optimization = optimize_price_with_constraint(monthly_df, margin_pct)

        # print("optimization output:", optimization)

        # if not optimization.get("price_points"):
        #     print("WARNING: price_points missing in optimization output")

    except Exception as e:
        # print("ERROR in optimization:", str(e))
        optimization = {}

    # ✅ IMPORTANT: ensure optimization survives to next nodes
    return {
        **state,
        "optimization": optimization
    }


def detect_risks_node(state: EventPlannerState) -> EventPlannerState:
    forecast = state["forecast"]
    top_skus = state.get("top_skus", [])

    adjusted_qty = float(forecast.get("adjusted_quantity", 0))
    inventory_units = float(forecast.get("inventory", 0))
    procurement = float(forecast.get("procurement", 0))
    base_sales = float(forecast.get("base_sales", 0))
    adjusted_sales = float(forecast.get("adjusted_sales", 0))

    stock_gap = adjusted_qty - inventory_units
    demand_spike_ratio = (adjusted_sales / base_sales) if base_sales > 0 else 1.0

    risk_type = state.get("pricing", {}).get("risk_type", "balanced")

    high_value_skus = [x.get("sku") for x in top_skus[:5] if x.get("sku")]

    # 🔥 NEW: USE sales_mix instead of fake contribution
    sales_mix_pct = float(state.get("sales_mix_pct", 0.0))
    profit_mix_pct = float(state.get("profit_mix_pct", 0.0))

    # 🔥 FIX: robust optimization fetch
    optimization_data = (
        state.get("optimization")
        or state.get("analytics", {}).get("optimization", {})
    )

    # 🔥 DEBUG
    # print("DEBUG: detect_risks_node")
    # print("sales_mix_pct:", round(sales_mix_pct, 2))
    # print("profit_mix_pct:", round(profit_mix_pct, 2))
    # print("optimization present:", bool(optimization_data))

    risk_signals = {
        "risk_type": risk_type,
        "stock_gap_units": round(stock_gap, 2),
        "demand_spike_ratio": round(demand_spike_ratio, 2),
        "requires_procurement": procurement > 0,
        "high_value_skus": high_value_skus,
    }

    analytics = {
        "scope": state["scope"],
        "last_event": state["last_event"],
        "future_event": state["future_event"],
        "uplift": state["uplift"],
        "forecast": state["forecast"],
        "risk_signals": risk_signals,
        "pricing": state.get("pricing", {}),
        "history": state.get("history", {}),
        "target_sales": state.get("target_sales"),
        "currency_symbol": get_currency_symbol(state.get("country", "us")),
        "top_skus": top_skus,

        # 🔥 FIXED
        "optimization": optimization_data,

        # 🔥 NEW REAL CONTRIBUTION DATA
        "sales_mix_pct": round(sales_mix_pct, 2),
        "profit_mix_pct": round(profit_mix_pct, 2),

        "category_summary": state.get("category_summary", []),
    }

    return {
        **state,
        "risk_signals": risk_signals,
        "analytics": analytics,
    }


def generate_execution_plan_node(state: EventPlannerState) -> EventPlannerState:
    analytics = state["analytics"]
    validation_errors = state.get("validation_errors", [])

    retry_note = ""
    if validation_errors:
        retry_note = (
            "Previous draft was rejected for these reasons: "
            + "; ".join(validation_errors)
            + ". Fix all of them in this new plan."
        )

    optimization = analytics.get("optimization") or {}

    if not optimization.get("price_points"):
        analytics["optimization"] = {
            "price_points": [],
            "price_range": {},
            "bau_profit": None
        }    

    prompt = f"""
You are a high-quality ecommerce business planning assistant.

Write the output so that even a fresher can understand it easily in one reading.

The output must be in simple bullet points.
The output must not sound like a calculator or generic software.
The output must explain the business story clearly.

You are given:
- SKU history
- latest month data
- highest units month
- highest ASP month
- lowest ASP month
- forecast
- inventory
- pricing data
- optimization price points
- optimization price range
- optional target sales
- currency symbol
- sales_mix_pct (percentage contribution to total sales)
- profit_mix_pct (percentage contribution to total profit)

IMPORTANT:
Optimization data is available in:
- analytics.optimization.price_points
- analytics.optimization.price_range
- analytics.optimization.bau_profit

Each price point contains:
- price
- units
- sales
- profit
- is_valid

Your job is to produce a strong business explanation for each SKU.

---

RULES:

- Write only in points
- Be simple, clear, and useful
- Use business-friendly language that a non-technical person can understand
- Avoid jargon, or explain it simply if used
- Keep sentences short and easy to read
- Do not repeat the same number or insight across sections
- Mention exact month/year when talking about history
- Compare past vs latest month
- Use the currency symbol given in the input
- Never recommend a price below lowest historical ASP
- Always explain trade-offs between price, units, sales, and profit
- Clearly explain whether profit is improving or getting diluted
- Do NOT force artificial labels like high price / balanced / volume push
- Always round units to whole numbers (no decimals)
- Use phrases like:
  - "This means..."
  - "In simple terms..."
  - "This is important because..."
- Write in a way that is clean and readable for frontend display
- Priority must be only: HIGH, MEDIUM, or LOW

---

CRITICAL RULES:

- You MUST use optimization data
- Do NOT generate or guess numbers
- ALL price, units, sales, and profit values MUST come from analytics.optimization.price_points or analytics.optimization.price_range
- Use at least 2–3 different price points when explaining range impact
- If optimization is empty, explicitly say: "Price range analysis not available"
- If some price points have is_valid = false, clearly explain they violate the profit guardrail
- Final recommendation must be based on:
  - history
  - current demand
  - stock position
  - profitability guardrail
  - price-response trend

---

OUTPUT FORMAT:

1. summary
- 2 to 4 simple bullet points
- explain overall business situation

---

2. history_story
- "In Dec 2025, ASP was £X and units sold were Y."
- "In the latest month, ASP is £Z and units sold are W."
- "Highest units were in Month Year."
- "Lowest ASP was in Month Year."
- clearly explain how price changes affected units, sales, and profit

---

3. current_status
- current stock position
- demand trend
- whether SKU is understocked, overstocked, or stable
- explain risk clearly

---

4. recommended_price_range
- recommended minimum price
- recommended maximum price
- explain why this range makes sense
- explain what happens if price goes below this range
- explain what happens if price goes above this range

---

5. range_impact_overview

Lower end of range:
- If price is £X:
- expected units: Y
- expected sales: £Z
- expected profit: £P
- explain impact (volume focus vs margin)

Middle of range:
- If price is £X:
- expected units: Y
- expected sales: £Z
- expected profit: £P
- explain if this is best trade-off

Upper end of range:
- If price is £X:
- expected units: Y
- expected sales: £Z
- expected profit: £P
- explain demand risk vs profit gain

---

6. profitability_guardrail
- mention BAU profit
- explain minimum safe profit level
- mention if any price points fall below safe level
- confirm whether recommended range is safe

---

7. actions
- clear actionable steps
- example:
  - increase inventory
  - hold pricing
  - test within range
  - avoid aggressive discounting

---

8. pricing_decision
- current ASP
- lowest ASP
- recommended price range (NOT exact price unless necessary)
- explain why range is better than fixed price

---

9. target_sales_note

- If target_sales exists:
  - clearly mention this is overall business target, not per SKU
  - explain SKU contribution using:
    - analytics.sales_mix_pct
    - analytics.profit_mix_pct

  - classify contribution:
    - >50% → key driver
    - 20–50% → important contributor
    - <20% → smaller contributor

  - explain realistically:
    - this SKU alone cannot achieve full target unless contribution is very high
    - multiple SKUs are needed to reach target

  - explain how this SKU supports target achievement

- If target_sales does not exist:
  - clearly state no target was given
  - pricing is based on demand, inventory, and profitability

---

10. final_recommendation
- final recommended price range
- ideal units target (rounded number)
- expected sales
- expected profit
- explain why this is best-fit based on:
  - history
  - demand
  - stock
  - profitability

---

11. priority
- only one value: HIGH / MEDIUM / LOW

{retry_note}
"""

    structured_llm = llm.with_structured_output(ExecutionPlan)
    plan = structured_llm.invoke(
        [
            {"role": "system", "content": prompt},
            {"role": "user", "content": json.dumps(analytics, default=str)},
        ]
    )

    return {
        "execution_plan": plan.model_dump(),
    }


def validate_plan_node(state: EventPlannerState) -> EventPlannerState:
    plan = state["execution_plan"]

    errors: List[str] = []

    if not plan.get("summary"):
        errors.append("Missing summary")

    if not plan.get("history_story"):
        errors.append("Missing history_story")

    if not plan.get("current_status"):
        errors.append("Missing current_status")

    if not plan.get("recommended_price_range"):
        errors.append("Missing recommended_price_range")

    if not plan.get("range_impact_overview"):
        errors.append("Missing range_impact_overview")

    if not plan.get("profitability_guardrail"):
        errors.append("Missing profitability_guardrail")

    if len(plan.get("actions", [])) < 2:
        errors.append("Need at least 2 action items")

    if not plan.get("pricing_decision"):
        errors.append("Missing pricing_decision")

    if not plan.get("target_sales_note"):
        errors.append("Missing target_sales_note")

    if not plan.get("final_recommendation"):
        errors.append("Missing final_recommendation")

    if plan.get("priority") not in ["HIGH", "MEDIUM", "LOW"]:
        errors.append("Invalid priority")

    return {
        "validation_errors": errors,
        "is_valid": len(errors) == 0,
        "retry_count": state.get("retry_count", 0) + (0 if len(errors) == 0 else 1),
    }


def route_after_validation(state: EventPlannerState) -> str:
    if state.get("is_valid", False):
        return "finalize"
    if state.get("retry_count", 0) >= 2:
        return "finalize"
    return "generate_execution_plan"


def finalize_node(state: EventPlannerState) -> EventPlannerState:
    return {
        "execution_plan": {
            **state["execution_plan"],
            "validation_errors": state.get("validation_errors", []),
            "is_valid": state.get("is_valid", False),
        }
    }


def build_event_planner_graph(phormula_engine, amazon_engine):
    graph = StateGraph(EventPlannerState)

    graph.add_node("load_context", load_context_node)
    graph.add_node("load_sales_inventory", make_load_sales_inventory_node(phormula_engine, amazon_engine))
    graph.add_node("analyze_scope", analyze_scope_node)
    graph.add_node("compute_forecast", compute_forecast_node)

    # 🔥 NEW NODE ADDED
    graph.add_node("optimize_pricing", optimize_pricing_node)

    graph.add_node("detect_risks", detect_risks_node)
    graph.add_node("generate_execution_plan", generate_execution_plan_node)
    graph.add_node("validate_plan", validate_plan_node)
    graph.add_node("finalize", finalize_node)

    graph.add_edge(START, "load_context")
    graph.add_edge("load_context", "load_sales_inventory")
    graph.add_edge("load_sales_inventory", "analyze_scope")

    # 🔥 UPDATED FLOW
    graph.add_edge("analyze_scope", "compute_forecast")
    graph.add_edge("compute_forecast", "optimize_pricing")   # NEW
    graph.add_edge("optimize_pricing", "detect_risks")       # NEW

    graph.add_edge("detect_risks", "generate_execution_plan")
    graph.add_edge("generate_execution_plan", "validate_plan")

    graph.add_conditional_edges(
        "validate_plan",
        route_after_validation,
        {
            "generate_execution_plan": "generate_execution_plan",
            "finalize": "finalize",
        },
    )

    graph.add_edge("finalize", END)

    checkpointer = MemorySaver()
    return graph.compile(checkpointer=checkpointer)

def run_sku(sku, payload):
    try:
        # 🔥 create graph per thread
        local_graph = build_event_planner_graph(phormula_engine, amazon_engine)

        initial_state = {
            "user_id": int(payload["user_id"]),
            "country": str(payload["country"]).lower(),
            "last_event": payload["last_event"],
            "future_event": payload["future_event"],
            "target_sales": payload.get("target_sales"),
            "scope": {"level": "sku", "value": sku},
            "retry_count": 0,
        }

        result = local_graph.invoke(
            initial_state,
            config={
                "configurable": {
                    "thread_id": f"user-{payload['user_id']}-sku-{sku}"
                }
            }
        )

        return {
            "sku": sku,
            "analytics": result.get("analytics", {}),
            "plan": result.get("execution_plan", {}),
        }

    except Exception as e:
        return {"sku": sku, "error": str(e)}


graph = build_event_planner_graph(phormula_engine, amazon_engine)



def build_plan_langgraph(payload, phormula_engine, amazon_engine):

    # Step 1: Load full sales data once
    sales_df = load_sales_data(
        phormula_engine,
        payload["user_id"],
        payload["country"],
        payload["future_event"]["year"],
        payload["future_event"]["month"],
    )

    # 🚨 Remove TOTAL rows
    sales_df = sales_df[~sales_df["sku"].str.contains("total", case=False, na=False)]

    # Get all unique SKUs
    unique_skus = sales_df["sku"].dropna().unique()

    # 🔥 Worker function (parallel-safe)
    def run_sku(sku, payload):
        try:
            # ✅ Create graph per thread (avoids memory collision)
            local_graph = build_event_planner_graph(phormula_engine, amazon_engine)

            initial_state = {
                "user_id": int(payload["user_id"]),
                "country": str(payload["country"]).lower(),
                "last_event": payload["last_event"],
                "future_event": payload["future_event"],
                "target_sales": payload.get("target_sales"),
                "scope": {"level": "sku", "value": sku},
                "retry_count": 0,
            }

            result = local_graph.invoke(
                initial_state,
                config={
                    "configurable": {
                        "thread_id": f"user-{payload['user_id']}-sku-{sku}"
                    }
                }
            )

            return {
                "sku": sku,
                "analytics": result.get("analytics", {}),
                "plan": result.get("execution_plan", {}),
            }

        except Exception as e:
            return {
                "sku": sku,
                "error": str(e)
            }

    results = []

    # 🔥 Parallel execution
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(run_sku, sku, payload) for sku in unique_skus]

        for future in as_completed(futures):
            results.append(future.result())

    return build_ui_output(results)

def build_ui_output(results):
    final_output = []

    for item in results:
        if "error" in item:
            continue

        plan = item.get("plan", {})
        analytics = item.get("analytics", {})

        # 🔥 product name
        product_name = ""
        if analytics.get("top_skus"):
            product_name = analytics["top_skus"][0].get("product_name", "")

        forecast = analytics.get("forecast", {})
        uplift = analytics.get("uplift", {})
        history = analytics.get("history", {})

        final_output.append({
            "sku": item.get("sku"),
            "product_name": product_name,

            # 🔥 EXACT ORDER STARTS HERE

            "base_forecast": [
                f"Base demand is {int(round(forecast.get('base_quantity', 0)))} units with expected sales of {analytics.get('currency_symbol', '£')}{int(round(forecast.get('base_sales', 0)))}."
            ],

            "uplift_percentage": [
                f"Expected unit uplift is {round(uplift.get('quantity_lift', 0), 2)}%.",
                f"Expected sales uplift is {round(uplift.get('sales_lift', 0), 2)}%."
            ],

            "event_forecast": [
                f"Expected event demand is {int(round(forecast.get('adjusted_quantity', 0)))} units.",
                f"Expected event sales are {analytics.get('currency_symbol', '£')}{int(round(forecast.get('adjusted_sales', 0)))}."
            ],

            "history_story": plan.get("history_story", []),

            "current_status": plan.get("current_status", []),

            "actions": plan.get("actions", []),

            "pricing_decision": plan.get("pricing_decision", []),

            "range_impact_overview": plan.get("range_impact_overview", []),

            "recommended_price_range": plan.get("recommended_price_range", []),

            "profitability_guardrail": plan.get("profitability_guardrail", []),

            "target_sales_note": plan.get("target_sales_note", []),

            "summary": plan.get("summary", [])
        })

    return final_output    