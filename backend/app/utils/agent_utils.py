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

    # clean
    for col in ["quantity", "total_quantity", "net_sales"]:
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
    return (
        df.groupby(["year", "month"], as_index=False)
        .agg(
            quantity=("quantity", "sum"),
            net_sales=("net_sales", "sum"),
            asp=("asp", "mean")
        )
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
    actions: List[str]
    pricing_decision: List[str]
    target_sales_note: List[str]
    priority: str

class AgentState(TypedDict, total=False):
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

    # processed
    scoped_sales_df: Any
    monthly_df: Any
    uplift: Dict[str, Any]
    forecast: Dict[str, Any]
    top_skus: List[Dict[str, Any]]
    category_summary: List[Dict[str, Any]]
    risk_signals: Dict[str, Any]
    analytics: Dict[str, Any]

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


def load_context_node(state: AgentState) -> AgentState:
    scope = state.get("scope") or {"level": "sku", "value": None}
    return {
        "scope": {
            "level": scope.get("level", "overall"),
            "value": scope.get("value"),
        },
        "retry_count": state.get("retry_count", 0),
        "validation_errors": [],
    }

def make_load_sales_inventory_node(phormula_engine, amazon_engine):
    def load_sales_inventory_node(state: AgentState) -> AgentState:
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

def analyze_scope_node(state: AgentState) -> AgentState:
    scope = state["scope"]

    sales_df = pd.DataFrame(state["sales_df"])
    sales_df = filter_scope(sales_df, scope["level"], scope["value"])

    if sales_df.empty:
        raise ValueError("No sales data found for requested scope")

    monthly_df = aggregate_monthly(sales_df)
    top_skus = summarize_top_skus(sales_df, top_n=10)
    category_summary = summarize_categories(sales_df, top_n=8)
    history = build_history_snapshot(monthly_df)

    return {
        "scoped_sales_df": sales_df.to_dict("records"),
        "monthly_df": monthly_df.to_dict("records"),
        "top_skus": top_skus,
        "category_summary": category_summary,
        "history": history,
    }

def compute_forecast_node(state: AgentState) -> AgentState:
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


def detect_risks_node(state: AgentState) -> AgentState:
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
    "top_skus": state.get("top_skus", []),
    "category_summary": state.get("category_summary", []),
    }

    return {
        "risk_signals": risk_signals,
        "analytics": analytics,
    }


def generate_execution_plan_node(state: AgentState) -> AgentState:
    analytics = state["analytics"]
    validation_errors = state.get("validation_errors", [])

    retry_note = ""
    if validation_errors:
        retry_note = (
            "Previous draft was rejected for these reasons: "
            + "; ".join(validation_errors)
            + ". Fix all of them in this new plan."
        )

    prompt = f"""
You are a high-quality ecommerce business planning assistant.

Write the output so that even a fresher can understand it easily.

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
- optional target sales
- currency symbol

Your job is to produce a strong business explanation for each SKU.

Rules:
- Write only in points
- Be simple, clear, and useful
- Do not use jargon
- Do not give generic filler
- Mention exact month/year when talking about history
- Compare past vs latest month
- Mention target sales clearly if provided
- Use the currency symbol given in the input
- Never recommend a price below lowest historical ASP
- Explain why the price range makes sense
- Explain clearly whether to order more, stop ordering, or reduce price
- Priority must be only: HIGH, MEDIUM, or LOW

Output format:

1. summary
- 2 to 4 simple bullet points
- explain what is happening overall

2. history_story
- bullet points like:
- "In Dec 2025, ASP was £X and units sold were Y."
- "In the latest month, ASP is £Z and units sold are W."
- "Highest units were in Month Year."
- "Lowest ASP was in Month Year."

3. current_status
- bullet points explaining:
- stock position
- demand direction
- whether this SKU is understocked, overstocked, or okay

4. actions
- bullet points with direct actions
- examples:
- order 2300 units now
- do not order more
- reduce price slightly
- keep price stable

5. pricing_decision
- bullet points explaining:
- current ASP
- lowest ASP
- recommended price range
- why this range makes sense

6. target_sales_note
- if target_sales exists:
  - mention target sales
  - mention target_price
  - explain whether target_price fits inside recommended range
  - mention final recommended selling price
- if target_sales does not exist:
  - say clearly that no target sales was given
  - say pricing is based on history, ASP, inventory, and demand only

7. priority
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


def validate_plan_node(state: AgentState) -> AgentState:
    plan = state["execution_plan"]
    analytics = state["analytics"]

    errors: List[str] = []

    # ✅ NEW STRUCTURE VALIDATION
    if not plan.get("summary"):
        errors.append("Missing summary")

    if not plan.get("history_story"):
        errors.append("Missing history_story")

    if not plan.get("current_status"):
        errors.append("Missing current_status")

    if len(plan.get("actions", [])) < 2:
        errors.append("Need at least 2 action items")

    if not plan.get("pricing_decision"):
        errors.append("Missing pricing_decision")

    if not plan.get("target_sales_note"):
        errors.append("Missing target_sales_note")

    if plan.get("priority") not in ["HIGH", "MEDIUM", "LOW"]:
        errors.append("Invalid priority")

    return {
        "validation_errors": errors,
        "is_valid": len(errors) == 0,
        "retry_count": state.get("retry_count", 0) + (0 if len(errors) == 0 else 1),
    }


def route_after_validation(state: AgentState) -> str:
    if state.get("is_valid", False):
        return "finalize"
    if state.get("retry_count", 0) >= 2:
        return "finalize"
    return "generate_execution_plan"


def finalize_node(state: AgentState) -> AgentState:
    return {
        "execution_plan": {
            **state["execution_plan"],
            "validation_errors": state.get("validation_errors", []),
            "is_valid": state.get("is_valid", False),
        }
    }


def build_event_planner_graph(phormula_engine, amazon_engine):
    graph = StateGraph(AgentState)

    graph.add_node("load_context", load_context_node)
    graph.add_node("load_sales_inventory", make_load_sales_inventory_node(phormula_engine, amazon_engine))
    graph.add_node("analyze_scope", analyze_scope_node)
    graph.add_node("compute_forecast", compute_forecast_node)
    graph.add_node("detect_risks", detect_risks_node)
    graph.add_node("generate_execution_plan", generate_execution_plan_node)
    graph.add_node("validate_plan", validate_plan_node)
    graph.add_node("finalize", finalize_node)

    graph.add_edge(START, "load_context")
    graph.add_edge("load_context", "load_sales_inventory")
    graph.add_edge("load_sales_inventory", "analyze_scope")
    graph.add_edge("analyze_scope", "compute_forecast")
    graph.add_edge("compute_forecast", "detect_risks")
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

    results = []

    for sku in unique_skus:
        try:
            initial_state = {
                "user_id": int(payload["user_id"]),
                "country": str(payload["country"]).lower(),
                "last_event": payload["last_event"],
                "future_event": payload["future_event"],
                "target_sales": payload.get("target_sales"),
                "scope": {"level": "sku", "value": sku},  # ✅ FORCE SKU MODE
                "retry_count": 0,
            }

            result = graph.invoke(
                initial_state,
                config={
                    "configurable": {
                        "thread_id": f"user-{payload['user_id']}-sku-{sku}"
                    }
                }
            )

            results.append({
                "sku": sku,
                "analytics": result.get("analytics", {}),
                "plan": result.get("execution_plan", {}),
            })

        except Exception as e:
            results.append({
                "sku": sku,
                "error": str(e)
            })

    return results