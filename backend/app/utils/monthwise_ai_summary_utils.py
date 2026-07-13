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
from app.utils.uk_prompts_utils import AI_SYSTEM_PROMPT_1, AI_SYSTEM_PROMPT_2, AI_SYSTEM_PROMPT_3_POLISHER, AI_GLOBAL_COMPARISON_PROMPT, get_excel_recommendation_from_metrics
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


DEFAULT_OBJECTIVE_V2 = {
    "growth_intent": "aggressive",  # conservative | balanced | aggressive
    "profit_priority": "high",  # high | protect_growth | sacrifice_short_term
    "inventory_clearance_priority": False,
    "business_context": None,
    "country": None,
    "time_horizon": "1_month"
}


def resolve_yearly_analysis_anchor(user_id: int, country: str, year: int):
    """
    For yearly selection, anchor insights to the latest completed month only.

    Example:
    If today is July 2026 and yearly 2026 is selected,
    anchor should be June 2026, not July 2026.
    """

    max_month = get_period_month_cap(
        year=year,
        period="yearly",
        timeline="ALL",
    )

    if max_month <= 0:
        return None

    for m in range(max_month, 0, -1):
        df = fetch_precalc_table(
            user_id=user_id,
            country=country,
            period="monthly",
            timeline=str(m),
            year=year
        )
        if not df.empty:
            return year, m

    return None


def severity_suffix(severity: str | None, *, period: str | None = None) -> str:
    """
    Suppress rolling 24-month severity labels for YEARLY reports.
    """
    if period in ("yearly", "quarterly"):
        return ""

    if not severity or severity == "normal":
        return ""

    return f" ({severity.replace('_', ' ').replace('24m', '24 months')})"


def resolve_latest_available_month(user_id: int, country: str):
    """
    Returns the latest (year, month) for which a precalc table actually exists.
    Falls back safely to latest completed month.
    """
    y, m = get_latest_completed_month()

    # Try current completed month first, then walk backwards
    for _ in range(24):
        table = build_table_name(
            user_id=user_id,
            country=country,
            period="monthly",
            timeline=str(m),
            year=y
        )
        try:
            pd.read_sql(
                f'SELECT 1 FROM public."{table}" LIMIT 1',
                phormula_engine
            )
            return y, m
        except Exception:
            pass

        if m == 1:
            y -= 1
            m = 12
        else:
            m -= 1

    # Absolute fallback (should never happen)
    return get_latest_completed_month()


def get_latest_completed_month(today=None):
    today = today or date.today()
    if today.month == 1:
        return today.year - 1, 12
    return today.year, today.month - 1

def get_period_month_cap(year: int, period: str, timeline: str | None = None, today=None) -> int:
    """
    Caps yearly/quarterly reports to latest completed month.

    Example:
    If today is July 2026:
      yearly 2026 should use Jan-Jun only
      Q3 2026 should not use July incomplete data
      Q2 2026 can use Apr-Jun
    """

    latest_completed_year, latest_completed_month = get_latest_completed_month(today)

    year = int(year)

    # Past years are fully completed
    if year < latest_completed_year:
        if period == "quarterly":
            q = int(str(timeline).replace("Q", ""))
            return q * 3
        return 12

    # Future years should not include anything
    if year > latest_completed_year:
        return 0

    # Current year: cap to latest completed month
    if period == "yearly":
        return latest_completed_month

    if period == "quarterly":
        q = int(str(timeline).replace("Q", ""))
        quarter_end_month = q * 3
        return min(quarter_end_month, latest_completed_month)

    return latest_completed_month





def resolve_latest_two_months():
    """
    Canonical advisor time resolver.
    Always returns latest completed month for MoM analysis.
    """
    year, month = get_latest_completed_month()
    return "monthly", str(month), year


def get_latest_completed_quarter(today=None):
    today = today or date.today()
    q = (today.month - 1) // 3 + 1
    if q == 1:
        return today.year - 1, 4
    return today.year, q - 1


def is_latest_period(period, timeline, year, *, user_id, country):
    """
    Returns True if the requested period corresponds to the
    latest finalized precalc data available for the user.
    """

    # ---------------- MONTHLY (DB-driven) ----------------
    if period == "monthly":
        latest_year, latest_month = resolve_latest_available_month(
            user_id=user_id,
            country=country
        )
        return year == latest_year and int(timeline) == latest_month


    # ---------------- QUARTERLY (DB-driven FIX) ----------------
    if period == "quarterly":
        # scan recent years & quarters to find latest existing table
        for test_year in range(year + 1, year - 3, -1):
            for q in (4, 3, 2, 1):
                table = build_table_name(user_id, country, "quarterly", f"Q{q}", test_year)
                try:
                    pd.read_sql(
                        f'SELECT 1 FROM public."{table}" LIMIT 1',
                        phormula_engine
                    )
                    return timeline == f"Q{q}" and year == test_year
                except Exception:
                    continue
        return False


    # ---------------- YEARLY (keep simple rule) ----------------
    if period == "yearly":
        # Detect latest yearly table that actually exists in DB
        for test_year in range(date.today().year + 1, date.today().year - 5, -1):
            table = build_table_name(user_id, country, "yearly", "ALL", test_year)
            try:
                pd.read_sql(
                    f'SELECT 1 FROM public."{table}" LIMIT 1',
                    phormula_engine
                )
                # First existing table = TRUE latest year
                return year == test_year
            except Exception:
                continue

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

def save_summary_to_db(data: dict):
    # ---------------- HARD SANITIZE INPUT ----------------
    recos = data.get("recommendations")
    if isinstance(recos, dict):
        recos = json.dumps(recos)
    elif recos is None:
        recos = json.dumps({})

    # 🔒 DETACH any previously loaded instance (CRITICAL)
    db.session.expire_all()

    row = HistoricAISummary.query.filter_by(
        user_id=data["user_id"],
        country=data["country"],
        marketplace_id=data.get("marketplace_id"),
        period=data["period"],
        timeline=data["timeline"],
        year=data["year"],
    ).first()

    if not row:
        row = HistoricAISummary(
            user_id=data["user_id"],
            country=data["country"],
            marketplace_id=data.get("marketplace_id"),
            period=data["period"],
            timeline=data["timeline"],
            year=data["year"],
        )
        db.session.add(row)

    # ---------------- OBJECTIVE ----------------
    row.primary_goal = data.get("primary_goal")
    row.risk_level = data.get("risk_level")
    row.max_tacos = data.get("max_tacos")
    row.max_price_increase_pct = data.get("max_price_increase_pct")
    row.ad_budget_cap = data.get("ad_budget_cap")
    row.dont_change_price = data.get("dont_change_price")
    row.notes = data.get("notes")

    # ---------------- OUTPUT ----------------
    row.summary = data.get("summary") or row.summary

    # 🔒 ABSOLUTE GUARANTEE: STRING ONLY
    row.recommendations = recos

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

def build_ads_table_name(user_id: int, country: str, timeline: str, year: int) -> str:
    """
    Ads table naming pattern used by skutableprofit:
    skuwisemonthly_{user_id}_{country}_{month}_{year}
    Example:
    skuwisemonthly_2_uk_may_2026
    """
    mn = month_name_from_timeline(timeline)
    return f"skuwisemonthly_{user_id}_{str(country).lower()}_{mn}_{year}".lower()


def fetch_precalc_table(user_id: int, country: str, period: str, timeline: str, year: int) -> pd.DataFrame:
    table = build_table_name(user_id, country, period, timeline, year)
    query = f'SELECT * FROM public."{table}"'

    try:
        return pd.read_sql(query, phormula_engine)
    except Exception as e:
        
        return pd.DataFrame()

def fetch_ads_table(user_id: int, country: str, timeline: str, year: int) -> pd.DataFrame:
    """
    Fetches the separate ads table if it exists.
    Returns empty DataFrame if table does not exist.
    """
    table = build_ads_table_name(user_id, country, timeline, year)
    query = f'SELECT * FROM public."{table}"'

    try:
        return pd.read_sql(query, phormula_engine)
    except Exception:
        return pd.DataFrame()

def build_productwise_adsmonthly_table_name(
    user_id: int,
    country: str,
    timeline: str,
    year: int
) -> str:
    """
    Productwise ads spend table:
    adsmonthly_{user_id}_{country}_{month_number}_{year}

    Example:
    adsmonthly_1_uk_6_2026
    """
    month_num = int(timeline)
    return f"adsmonthly_{user_id}_{str(country).lower()}_{month_num}_{year}".lower()

def build_productwise_adsmonthly_table_names(
    user_id: int,
    country: str,
    timeline: str,
    year: int
) -> list[str]:
    """
    Candidate productwise ads table names.

    Primary pattern:
        adsmonthly_{user_id}_{country}_{month_number}_{year}

    Fallbacks cover month-name variants if older data was created that way.
    """
    month_num = int(timeline)
    month_name = month_name_from_timeline(str(month_num))
    country_key = str(country).lower()

    candidates = [
        f"adsmonthly_{user_id}_{country_key}_{month_num}_{year}",
        f"adsmonthly_{user_id}_{country_key}_{month_name}_{year}",
        f"adsmonthly_{user_id}_{country_key}_{month_name}{year}",
    ]

    return list(dict.fromkeys(name.lower() for name in candidates))

def fetch_productwise_adsmonthly_table(
    user_id: int,
    country: str,
    timeline: str,
    year: int
) -> pd.DataFrame:
    """
    Fetch productwise adsmonthly table.
    Returns empty DataFrame if table does not exist.
    """
    tables = build_productwise_adsmonthly_table_names(
        user_id=user_id,
        country=country,
        timeline=timeline,
        year=year,
    )

    for table in tables:
        query = f'SELECT * FROM public."{table}"'

        try:
            return pd.read_sql(query, phormula_engine)
        except Exception:
            continue

    return pd.DataFrame()


def _get_case_insensitive_column(df: pd.DataFrame, names: list[str]) -> str | None:
    lookup = {str(col).strip().lower(): col for col in df.columns}
    for name in names:
        match = lookup.get(str(name).strip().lower())
        if match is not None:
            return match
    return None


def apply_productwise_ads_cm2_from_adsmonthly(
    df_main: pd.DataFrame,
    *,
    user_id: int,
    country: str,
    period: str,
    timeline: str,
    year: int,
) -> pd.DataFrame:
    """
    Adds productwise ads spend and calculated CM2 profit into SKU rows.

    Formula:
        productwise_ads_spend = adsmonthly.spend
        cm2_profit = profit - productwise_ads_spend
        cm2_profit_per_unit = cm2_profit / total_quantity

    Important:
    - This does NOT touch total ads logic.
    - This does NOT overwrite TOTAL row CM2.
    - TOTAL row CM2 should come from total_cm2_profit / cm2_profit in the main table.
    - If adsmonthly table is missing, no error is thrown.
    """

    if df_main.empty:
        return df_main

    df = _normalize_sku_col(df_main.copy())

    if "sku" not in df.columns:
        return df

    # Identify total row early so portfolio/overall CM2 is never overwritten
    total_mask = df["sku"].astype(str).str.strip().str.lower().isin(TOTAL_LABELS)

    # Productwise adsmonthly table is monthly only.
    if period != "monthly":
        if "cm2_profit" not in df.columns:
            df["cm2_profit"] = 0.0
        if "cm2_profit_per_unit" not in df.columns:
            df["cm2_profit_per_unit"] = 0.0
        if "productwise_ads_spend" not in df.columns:
            df["productwise_ads_spend"] = 0.0
        return df

    if "profit" not in df.columns:
        df["profit"] = 0.0

    if "total_quantity" not in df.columns:
        df["total_quantity"] = 0.0

    df["profit"] = pd.to_numeric(df["profit"], errors="coerce").fillna(0)
    df["total_quantity"] = pd.to_numeric(df["total_quantity"], errors="coerce").fillna(0)

    # Create columns if missing, but do not wipe TOTAL row values
    if "productwise_ads_spend" not in df.columns:
        df["productwise_ads_spend"] = 0.0

    if "cm2_profit" not in df.columns:
        df["cm2_profit"] = 0.0

    if "cm2_profit_per_unit" not in df.columns:
        df["cm2_profit_per_unit"] = 0.0

    # Reset only SKU/detail rows.
    # Keep TOTAL row cm2_profit from the main monthly table.
    df.loc[~total_mask, "productwise_ads_spend"] = 0.0

    df_ads = fetch_productwise_adsmonthly_table(
        user_id=user_id,
        country=country,
        timeline=timeline,
        year=year,
    )

    if not df_ads.empty:
        df_ads = _normalize_sku_col(df_ads.copy())

        # adsmonthly.products contains the SKU values used in skuwisemonthly.sku.
        ads_product_col = _get_case_insensitive_column(
            df_ads,
            ["sku", "products", "product"],
        )
        ads_spend_col = _get_case_insensitive_column(df_ads, ["spend"])

        if ads_product_col and ads_spend_col:
            if ads_product_col != "sku":
                df_ads.rename(columns={ads_product_col: "sku"}, inplace=True)
            if ads_spend_col != "spend":
                df_ads.rename(columns={ads_spend_col: "spend"}, inplace=True)

            df["sku_key"] = df["sku"].astype(str).str.strip().str.upper()
            df_ads["sku_key"] = df_ads["sku"].astype(str).str.strip().str.upper()

            df_ads = df_ads[
                ~df_ads["sku_key"].str.lower().isin(TOTAL_LABELS)
            ].copy()

            df_ads["spend"] = pd.to_numeric(df_ads["spend"], errors="coerce").fillna(0).abs()

            ads_by_sku = (
                df_ads.groupby("sku_key", as_index=False)["spend"]
                .sum()
                .rename(columns={"spend": "productwise_ads_spend"})
            )

            df = df.merge(ads_by_sku, on="sku_key", how="left", suffixes=("", "_from_ads"))
            total_mask = df["sku"].astype(str).str.strip().str.lower().isin(TOTAL_LABELS)

            df.loc[~total_mask, "productwise_ads_spend"] = (
                pd.to_numeric(df.loc[~total_mask, "productwise_ads_spend_from_ads"], errors="coerce")
                .fillna(0)
            )

    # Recalculate CM2 only for SKU/detail rows.
    # Do not touch TOTAL row because overall summary AI payload should use table total.
    df.loc[~total_mask, "cm2_profit"] = (
        df.loc[~total_mask, "profit"]
        - df.loc[~total_mask, "productwise_ads_spend"]
    ).round(2)

    df.loc[~total_mask, "cm2_profit_per_unit"] = np.where(
        df.loc[~total_mask, "total_quantity"] != 0,
        (
            df.loc[~total_mask, "cm2_profit"]
            / df.loc[~total_mask, "total_quantity"]
        ).round(2),
        0.0
    )

    df.drop(
        columns=["sku_key", "productwise_ads_spend_from_ads"],
        inplace=True,
        errors="ignore"
    )

    return df

def build_rolling_monthly_series(
    user_id: int,
    country: str,
    anchor_year: int,
    anchor_month: int
):
    
    series = []

  
    for y, m in rolling_months(anchor_year, anchor_month, 24):
        df = fetch_precalc_table(
            user_id=user_id,
            country=country,
            period="monthly",
            timeline=str(m),
            year=y
        )

        df = apply_advertising_total_final_from_ads_table(
            df,
            user_id=user_id,
            country=country,
            period="monthly",
            timeline=str(m),
            year=y,
        )

        if df.empty:
            continue

        _, df_total = _split_total_row(df)
        if df_total.empty:
            continue

        snapshot = extract_total_snapshot(df_total)

        # TEMPORARY SAFE CHECK (prevents crash so we can see prints)
        if not isinstance(snapshot, dict) or not snapshot:
            print("❌ Skipping due to invalid snapshot")
            continue


        series.append({
            "year": y,
            "month": m,
            "values": snapshot
        })

    return series

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

def enrich_sku_current_with_selected_inventory(
    sku_current: dict,
    *,
    user_id: int,
    year: int,
    month: int,
) -> dict:
    """
    Adds current-only selected-period inventory fields to each SKU.

    current_inventory = sellable_inventory + damaged_inventory + expired_inventory
    selected_period_coverage_ratio = current_inventory / selected period total_quantity

    No previous value.
    No delta.
    No delta_pct.
    """

    if not isinstance(sku_current, dict) or not sku_current:
        return sku_current or {}

    inventory_lookup = fetch_month_end_inventory_lookup(user_id)

    output = {}

    for sku, data in sku_current.items():
        if not isinstance(data, dict):
            output[sku] = data
            continue

        row = dict(data)

        sku_key = str(sku).strip()

        inv = inventory_lookup.get(
            (sku_key, int(year), int(month)),
            {}
        ) or {}

        sellable_inventory = safe_float(inv.get("sellable_inventory"))

        current_inventory = sellable_inventory

        total_quantity = safe_float(row.get("total_quantity"))

        selected_period_coverage_ratio = (
            round(current_inventory / total_quantity, 2)
            if total_quantity
            else 0.0
        )

        row["current_inventory"] = round(current_inventory, 2)
        row["selected_period_coverage_ratio"] = selected_period_coverage_ratio

        output[sku] = row

    return output

def build_portfolio_inventory_coverage_summary(
    *,
    user_id: int,
    country: str,
    year: int,
    month: int,
    df_current_total: pd.DataFrame,
) -> dict:
    """
    Portfolio inventory coverage for Performance Summary.

    Formula:
        total_coverage_ratio = total_sellable_inventory / total_quantity

    Monthly only.
    """

    if df_current_total is None or df_current_total.empty:
        return {}

    total_quantity = _total_value(df_current_total, "total_quantity")
    total_quantity = safe_float(total_quantity)

    inventory_lookup = fetch_month_end_inventory_lookup(user_id)

    total_sellable_inventory = 0.0

    for (_sku, inv_year, inv_month), inv in (inventory_lookup or {}).items():
        if int(inv_year) == int(year) and int(inv_month) == int(month):
            total_sellable_inventory += safe_float(inv.get("sellable_inventory"))

    if total_quantity <= 0:
        return {}

    total_coverage_ratio = round(total_sellable_inventory / total_quantity, 2)

    required_coverage = fetch_high_alert_threshold(user_id, country)

    if required_coverage is None:
        required_coverage = 5.0

    required_coverage = round(float(required_coverage), 2)

    if total_coverage_ratio < required_coverage:
        status = "low stock"
    elif total_coverage_ratio >= 6:
        status = "excess stock"
    else:
        status = "healthy stock"

    return {
        "total_quantity": round(total_quantity, 2),
        "total_sellable_inventory": round(total_sellable_inventory, 2),
        "total_coverage_ratio": total_coverage_ratio,
        "required_coverage": required_coverage,
        "status": status,
        "sentence": (
            f"Inventory Coverage: Total coverage ratio is "
            f"{total_coverage_ratio} months, required coverage is "
            f"{required_coverage} months, indicating {status}."
        ),
    }



def build_rolling_sku_series(
    user_id: int,
    country: str,
    sku: str,
    anchor_year: int,
    anchor_month: int
):

    series = []

    # 🔹 fetch inventory once
    inventory_lookup = fetch_month_end_inventory_lookup(user_id)

    sku_key = str(sku).strip()

    for y, m in rolling_months(anchor_year, anchor_month, 24):

        df = fetch_precalc_table(
            user_id=user_id,
            country=country,
            period="monthly",
            timeline=str(m),
            year=y
        )

        if df.empty:
            continue

        df = _normalize_sku_col(df)

        if "sku" not in df.columns:
            continue

        df = df.copy()
        df["sku"] = df["sku"].astype(str).str.strip()

        # ✅ remove total row and use your existing recalculation logic
        df_detail, _ = _split_total_row(df)

        if df_detail.empty:
            continue

        # ✅ this recalculates unit_wise_profitability, asp, sales_mix, profit_mix safely
        sku_month = compute_sku_precalc(df_detail)

        sku_data = sku_month.get(sku_key)

        if not isinstance(sku_data, dict):
            continue

        # 🔹 inventory lookup
        inv = inventory_lookup.get((sku_key, y, m), {})

        series.append({
            "year": y,
            "month": m,

            "units": sku_data.get("total_quantity"),
            "asp": sku_data.get("asp"),
            "cm1_profit": sku_data.get("profit"),
            "net_sales": sku_data.get("net_sales"),
            "unit_wise_profitability": sku_data.get("unit_wise_profitability"),
            "sales_mix": sku_data.get("sales_mix"),
            "profit_mix": sku_data.get("profit_mix"),

            # 🔹 inventory fields
            "sellable_inventory": inv.get("sellable_inventory"),
            "damaged_inventory": inv.get("damaged_inventory"),
            "expired_inventory": inv.get("expired_inventory")
        })

    return series



def build_remaining_skus_time_series(
    user_id: int,
    country: str,
    focus_skus: list[str],
    anchor_year: int,
    anchor_month: int,
    months: int = 24
) -> list[dict]:

    series = []
    for y, m in rolling_months(anchor_year, anchor_month, months):
        df = fetch_precalc_table(user_id, country, "monthly", str(m), y)
        if df.empty:
            continue

        df_detail, _ = _split_total_row(df)
        sku_month = compute_sku_precalc(df_detail)

   
        agg = build_remaining_skus_aggregate(
            sku_current=sku_month,
            sku_prev={},
            focus_skus=focus_skus
        )
        if not agg:
            continue

        series.append({
            "year": y,
            "month": m,
            "units": agg.get("total_quantity", {}).get("current"),
            "asp": agg.get("asp", {}).get("current"),
            "cm1_profit": agg.get("profit", {}).get("current"),
             "net_sales": agg.get("net_sales", {}).get("current"),
            "unit_wise_profitability": agg.get("unit_wise_profitability", {}).get("current"),
            "sales_mix": agg.get("sales_mix", {}).get("current"),
            "profit_mix": agg.get("profit_mix", {}).get("current"),
        })

    return series

def compute_generic_movement(series: list, col: str):
    points = []

    # Build MoM % series WITH month identity
    for i in range(1, len(series)):
        prev = series[i - 1]["values"].get(col)
        cur = series[i]["values"].get(col)

        if prev is None or prev == 0 or cur is None:
            continue

        if col == "platform_fee_inventory_storage":
            pct = (abs(cur) - abs(prev)) / abs(prev) * 100
        else:
            pct = (cur - prev) / abs(prev) * 100

        points.append({
            "year": series[i]["year"],
            "month": series[i]["month"],
            "pct_change": pct
        })

    if len(points) < 1:
        return None

    current = points[-1]
    mom_pct = current["pct_change"]

    # 🔑 GLOBAL extreme detection (not rolling-rank)
    max_point = max(points, key=lambda x: abs(x["pct_change"]))
    min_point = min(points, key=lambda x: abs(x["pct_change"]))

    severity = "normal"
    if mom_pct == max_point["pct_change"]:
        severity = "highest_24m"
    elif mom_pct == min_point["pct_change"]:
        severity = "lowest_24m"

    direction = "up" if mom_pct > 0 else "down" if mom_pct < 0 else "flat"

    # Pattern logic (unchanged)
    pattern = None
    if len(points) >= 2:
        prev_change = points[-2]["pct_change"]
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
        "severity": severity,
        "pattern": pattern
    }

def extract_rolling_extremes(rolling_series: list):
    """
    Returns month-level extreme movements for executive synthesis.
    """
    extremes = {}

    for col in MOVEMENT_COLUMNS:
        points = []

        for i in range(1, len(rolling_series)):
            prev = rolling_series[i - 1]["values"].get(col)
            cur = rolling_series[i]["values"].get(col)

            if prev is None or prev == 0 or cur is None:
                continue

            if col == "platform_fee_inventory_storage":
                pct = (abs(cur) - abs(prev)) / abs(prev) * 100
            else:
                pct = (cur - prev) / abs(prev) * 100

            points.append({
                "year": rolling_series[i]["year"],
                "month": rolling_series[i]["month"],
                "pct_change": round(pct, 2)
            })

        if not points:
            continue

        max_point = max(points, key=lambda x: abs(x["pct_change"]))

        extremes[col] = {
            "year": max_point["year"],
            "month": max_point["month"],
            "pct_change": max_point["pct_change"]
        }

    return extremes

def build_yearly_temporal_signals(rolling_series: list) -> dict:
    """
    Derives executive-level yearly storytelling signals from
    monthly rolling totals.

    SAFE:
    - Purely additive
    - No mutation of existing logic
    - Returns empty dict if insufficient data
    """

    if not rolling_series or len(rolling_series) < 6:
        return {}

    def _get(col, item):
        v = item["values"].get(col)
        return float(v) if isinstance(v, (int, float)) else None

    # ---------- Peak & Weak Month (Net Sales) ----------
    sales_points = [
        (r["year"], r["month"], _get("net_sales", r))
        for r in rolling_series
        if _get("net_sales", r) is not None
    ]

    if not sales_points:
        return {}

    peak = max(sales_points, key=lambda x: x[2])
    weak = min(sales_points, key=lambda x: x[2])

    # ---------- H1 vs H2 Trend ----------
    mid = len(rolling_series) // 2

    def _avg(col, subset):
        vals = [_get(col, r) for r in subset if _get(col, r) is not None]
        return sum(vals) / len(vals) if vals else None

    h1_sales = _avg("net_sales", rolling_series[:mid])
    h2_sales = _avg("net_sales", rolling_series[mid:])

    if h1_sales is None or h2_sales is None:
        h1_h2_direction = None
    elif h2_sales > h1_sales:
        h1_h2_direction = "improving"
    elif h2_sales < h1_sales:
        h1_h2_direction = "softening"
    else:
        h1_h2_direction = "flat"

    # ---------- ACOS Trend ----------
    acos_start = _get("acos", rolling_series[0])
    acos_end = _get("acos", rolling_series[-1])

    if acos_start is None or acos_end is None:
        acos_trend = None
    elif acos_end < acos_start:
        acos_trend = "improving"
    elif acos_end > acos_start:
        acos_trend = "deteriorating"
    else:
        acos_trend = "flat"

    # ---------- CM2 Trend ----------
    cm2_start = _get("cm2_profit", rolling_series[0])
    cm2_end = _get("cm2_profit", rolling_series[-1])

    if cm2_start is None or cm2_end is None:
        cm2_trend = None
    elif cm2_end > cm2_start:
        cm2_trend = "improving"
    elif cm2_end < cm2_start:
        cm2_trend = "declining"
    else:
        cm2_trend = "flat"

    return {
        "peak_month_sales": {"year": peak[0], "month": peak[1]},
        "weak_month_sales": {"year": weak[0], "month": weak[1]},
        "h1_vs_h2_direction": h1_h2_direction,
        "acos_trend_direction": acos_trend,
        "cm2_trend_direction": cm2_trend,
    }

def compute_period_pct_changes(df_current_total, df_prev_total):

    def pct(col):
        cur = _total_value(df_current_total, col)
        prev = _total_value(df_prev_total, col)
        if cur is None or prev in (None, 0):
            return None
        return round((cur - prev) / abs(prev) * 100, 2)

    def pct_growth(col):
        cur = _total_value(df_current_total, col)
        prev = _total_value(df_prev_total, col)

        if cur is None or prev in (None, 0):
            return None

        return round((cur - prev) / abs(prev) * 100, 2)

    # ✅ helper only for storage cost (because DB stores it negative)
    def pct_storage(col):
        cur = _total_value(df_current_total, col)
        prev = _total_value(df_prev_total, col)

        
        if cur is None or prev in (None, 0):
            print("DEBUG STORAGE: missing values or prev=0")
            return None

        cur_abs = abs(cur)
        prev_abs = abs(prev)

        pct_val = round((cur_abs - prev_abs) / prev_abs * 100, 2)



        return pct_val

    return {
        "units": pct("total_quantity"),
        "net_sales": pct("net_sales"),
        "asp": pct("asp"),
        "cm1_profit": pct("profit"),
        "cm1_profit_per_unit": pct("unit_wise_profitability"),
        "cm2_profit": (
            round(
                (_total_cm2_value(df_current_total) - _total_cm2_value(df_prev_total))
                /(_total_cm2_value(df_prev_total))
                * 100,
                2
            )
            if _total_cm2_value(df_current_total) is not None
            and _total_cm2_value(df_prev_total) not in (None, 0)
            else None
        ),
        "advertising": (
            round(
                (
                    _advertising_total_value(df_current_total)
                    - _advertising_total_value(df_prev_total)
                )
                / abs(_advertising_total_value(df_prev_total))
                * 100,
                2
            )
            if _advertising_total_value(df_current_total) is not None
            and _advertising_total_value(df_prev_total) not in (None, 0)
            else None
        ),

        # ✅ FIXED storage calculation
        "storage_fees": pct_storage("platform_fee_inventory_storage"),

        "acos": pct_growth("acos"),
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

def _total_cm2_value(df_total: pd.DataFrame):
    """
    AI portfolio/overall CM2 source.

    Priority:
    1. total_cm2_profit
    2. cm2_profit
    """
    val = _total_value(df_total, "total_cm2_profit")

    if val is not None:
        return val

    return _total_value(df_total, "cm2_profit")


def safe_float(v):
    try:
        if v is None:
            return 0.0
        return float(v)
    except Exception:
        return 0.0


def _get_ads_spend_from_row(row: dict) -> float:
    """
    Same purpose as _get_ads_spend() in skutableprofit.
    Adjust column names here if your ads table uses different names.
    """
    for col in ["ads_spend", "total_ads_spend", "visible_ads"]:
        if col in row:
            return abs(safe_float(row.get(col)))
    return 0.0


def _get_dealsvouchar_ads_from_row(row: dict) -> float:
    """
    Same purpose as _get_dealsvouchar_ads() in skutableprofit.
    Includes spelling variants.
    """
    for col in [
        "dealsvouchar_ads",
        "deals_voucher_ads",
        "dealsvoucher_ads",
        "deals_vouchar_ads",
    ]:
        if col in row:
            return abs(safe_float(row.get(col)))
    return 0.0


def _advertising_total_value(df_total: pd.DataFrame):
    """
    AI portfolio/overall advertising source.

    Priority:
    1. total_ads
    2. advertising_total
    """
    total_ads_val = _total_value(df_total, "total_ads")

    if total_ads_val is not None:
        return total_ads_val

    return _total_value(df_total, "advertising_total")


def get_ads_total_row(df_ads: pd.DataFrame) -> dict:
    """
    Finds total row from ads table.
    Same idea as skutableprofit:
    - prefer sku == total
    - else product_name == total
    - else use last row
    """
    if df_ads.empty:
        return {}

    df = df_ads.copy()

    if "sku" in df.columns:
        total_rows = df[
            df["sku"].astype(str).str.strip().str.lower().isin(TOTAL_LABELS)
        ]
        if not total_rows.empty:
            return total_rows.iloc[0].to_dict()

    if "product_name" in df.columns:
        total_rows = df[
            df["product_name"].astype(str).str.strip().str.lower().isin(TOTAL_LABELS)
        ]
        if not total_rows.empty:
            return total_rows.iloc[0].to_dict()

    return df.iloc[-1].to_dict()


def apply_advertising_total_final_from_ads_table(
    df_main: pd.DataFrame,
    *,
    user_id: int,
    country: str,
    period: str,
    timeline: str,
    year: int,
) -> pd.DataFrame:
    """
    Replicates skutableprofit advertising_total_final logic for reference/debug.

    New AI advertising rule:
        1. Use total_ads if present
        2. Else use advertising_total

    Important:
    - This function should NOT overwrite advertising_total.
    - This function should NOT overwrite total_ads.
    - It only fills advertising_total_final separately.
    """

    if df_main.empty:
        return df_main

    df = df_main.copy()

    if "advertising_total_final" not in df.columns:
        df["advertising_total_final"] = None

    if "advertising_total" not in df.columns:
        df["advertising_total"] = 0.0

    # --------------------------------------------------
    # Non-monthly data:
    # Keep advertising_total untouched.
    # Only fill advertising_total_final if missing.
    # --------------------------------------------------
    if period != "monthly":
        df["advertising_total_final"] = df["advertising_total_final"].fillna(
            df["advertising_total"]
        )
        return df

    df = _normalize_sku_col(df)

    if "sku" not in df.columns:
        return df

    total_mask = df["sku"].astype(str).str.strip().str.lower().isin(TOTAL_LABELS)

    if not total_mask.any():
        return df

    total_idx = df[total_mask].index[0]

    df_ads = fetch_ads_table(
        user_id=user_id,
        country=country,
        timeline=timeline,
        year=year,
    )

    # --------------------------------------------------
    # Case 1: No ads table
    # --------------------------------------------------
    if df_ads.empty:
        main_total = df.loc[total_idx].to_dict()

        advertising_total_from_main = safe_float(
            main_total.get("advertising_total")
        )

        if advertising_total_from_main == 0:
            visible_ads = abs(safe_float(main_total.get("visible_ads")))
            dealsvouchar_ads = _get_dealsvouchar_ads_from_row(main_total)
            brand_spend = abs(safe_float(main_total.get("brand_spend")))

            advertising_total_from_main = (
                visible_ads
                + dealsvouchar_ads
                + brand_spend
            )

        advertising_total_from_main = round(advertising_total_from_main, 2)

        # Keep separate reference value only.
        # Do NOT overwrite advertising_total.
        # Do NOT overwrite total_ads.
        df.loc[total_idx, "advertising_total_final"] = advertising_total_from_main

        return df

    # --------------------------------------------------
    # Case 2: Ads table exists
    # --------------------------------------------------
    ads_total_row = get_ads_total_row(df_ads)

    brand_spend_total = abs(safe_float(ads_total_row.get("brand_spend")))
    dealsvouchar_ads_total = _get_dealsvouchar_ads_from_row(ads_total_row)
    total_ads_spend = _get_ads_spend_from_row(ads_total_row)

    # Fallback: if total ads spend is 0, sum SKU-level ads_spend.
    if total_ads_spend == 0 and "ads_spend" in df_ads.columns:
        ads_detail = df_ads.copy()

        if "sku" in ads_detail.columns:
            ads_detail = ads_detail[
                ~ads_detail["sku"]
                .astype(str)
                .str.strip()
                .str.lower()
                .isin(TOTAL_LABELS)
            ]

        if "product_name" in ads_detail.columns:
            ads_detail = ads_detail[
                ~ads_detail["product_name"]
                .astype(str)
                .str.strip()
                .str.lower()
                .isin(TOTAL_LABELS)
            ]

        total_ads_spend = (
            pd.to_numeric(
                ads_detail["ads_spend"],
                errors="coerce",
            )
            .fillna(0)
            .abs()
            .sum()
        )

    advertising_total_original = brand_spend_total + dealsvouchar_ads_total
    advertising_total_final = advertising_total_original + total_ads_spend

    advertising_total_original = round(advertising_total_original, 2)
    advertising_total_final = round(advertising_total_final, 2)

    df.loc[total_idx, "brand_spend"] = round(brand_spend_total, 2)
    df.loc[total_idx, "dealsvouchar_ads"] = round(dealsvouchar_ads_total, 2)

    # Keep reference values only.
    df.loc[total_idx, "advertising_total_original"] = advertising_total_original
    df.loc[total_idx, "advertising_total_final"] = advertising_total_final

    # Very important:
    # Do NOT overwrite advertising_total.
    # Do NOT overwrite total_ads.
    # AI payload should now use _advertising_total_value():
    # total_ads first, then advertising_total.

    return df


def compute_period_absolute_changes(df_current_total, df_prev_total):
    def diff(col):
        cur = _total_value(df_current_total, col)
        prev = _total_value(df_prev_total, col)
        if cur is None or prev is None:
            return None
        return round(cur - prev, 2)

    def pct_point_diff(col):
        cur = _total_value(df_current_total, col)
        prev = _total_value(df_prev_total, col)
        if cur is None or prev is None:
            return None
        return round(cur - prev, 2)

    return {
        "units": diff("total_quantity"),
        "net_sales": diff("net_sales"),
        "asp": diff("asp"),

        "cm1_profit": diff("profit"),
        "cm1_profit_per_unit": diff("unit_wise_profitability"),
        "cm2_profit": (
            round(_total_cm2_value(df_current_total) - _total_cm2_value(df_prev_total), 2)
            if _total_cm2_value(df_current_total) is not None
            and _total_cm2_value(df_prev_total) is not None
            else None
        ),

        "advertising": (
            round(
                _advertising_total_value(df_current_total)
                - _advertising_total_value(df_prev_total),
                2
            )
            if _advertising_total_value(df_current_total) is not None
            and _advertising_total_value(df_prev_total) is not None
            else None
        ),
        "storage_fees": diff("platform_fee_inventory_storage"),

        
        "misc_transaction": diff("misc_transaction"),

        "acos": pct_point_diff("acos"),
    }



# def extract_total_snapshot(df_total: pd.DataFrame) -> dict:
#     snapshot = {}
#     if df_total.empty:
#         return snapshot

#     for col in MOVEMENT_COLUMNS:
#         if col in df_total.columns:
#             val = _total_value(df_total, col)
#             if val is not None:
#                 snapshot[col] = float(val)

#     return snapshot

def extract_total_snapshot(df_total: pd.DataFrame) -> dict:
    snapshot = {}
    if df_total.empty:
        return snapshot

    for col in MOVEMENT_COLUMNS:
        if col in df_total.columns:
            val = _total_value(df_total, col)
            if val is not None:
                snapshot[col] = float(val)

    advertising_value = _advertising_total_value(df_total)
    if advertising_value is not None:
        snapshot["advertising_total"] = float(advertising_value)

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

    "total_ads",
    "advertising_total",
    "advertising_total_final",

    "lost_total",
    "rembursement_fee",

    "profit",
    "cm2_profit",
    "total_cm2_profit",
    "productwise_ads_spend",
 
    
}

PERCENTAGE_COLUMNS = {
    "acos",
    "profit_percentage",
    "cm2_profit_percentage",
    "promotional_rebates_percentage",

    "sales_mix",
    "profit_mix",
}

NON_ADDITIVE_COMPARABLE = {
    "asp",
    "unit_wise_profitability",
    "cm2_profit_per_unit",
    
}

MOVEMENT_COLUMNS = METRIC_COLUMNS | PERCENTAGE_COLUMNS | NON_ADDITIVE_COMPARABLE




def get_metric_columns(df: pd.DataFrame) -> list[str]:
    return [c for c in df.columns if c.lower() in METRIC_COLUMNS]


def compute_sku_precalc(df: pd.DataFrame) -> dict:
    if df.empty:
        return {}

    df = _normalize_sku_col(df)

    if "sku" not in df.columns:
        return {}

    df = df.copy()

    # -------------------------------------------------
    # Numeric cleanup
    # -------------------------------------------------
    comparable_cols = (
        set(METRIC_COLUMNS)
        | set(PERCENTAGE_COLUMNS)
        | set(NON_ADDITIVE_COMPARABLE)
    )

    for c in df.columns:
        if c.lower() in comparable_cols:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

    # -------------------------------------------------
    # Aggregate only additive metrics
    # -------------------------------------------------
    num_cols = get_metric_columns(df)

    non_additive_cols = {
        "asp",
        "unit_wise_profitability",
        "profit_percentage",
        "cm2_profit_percentage",
        "acos",
        "sales_mix",
        "profit_mix",
    }

    other_cols = [
        c for c in df.columns
        if c not in num_cols
        and c.lower() not in non_additive_cols
        and c != "sku"
    ]

    agg = {c: "sum" for c in num_cols}

    for c in other_cols:
        agg[c] = "first"

    g = df.groupby("sku", dropna=False).agg(agg).reset_index()

    # -------------------------------------------------
    # Recalculate non-additive SKU metrics after grouping
    # -------------------------------------------------

    # ASP = net sales / units
    if "net_sales" in g.columns and "total_quantity" in g.columns:
        g["asp"] = np.where(
            g["total_quantity"] != 0,
            (g["net_sales"] / g["total_quantity"]).round(2),
            None
        )

    # CM1 profit per unit = profit / units
    if "profit" in g.columns and "total_quantity" in g.columns:
        g["unit_wise_profitability"] = np.where(
            g["total_quantity"] != 0,
            (g["profit"] / g["total_quantity"]).round(2),
            None
        )

    # CM2 profit per unit = calculated CM2 profit / units
    if "cm2_profit" in g.columns and "total_quantity" in g.columns:
        g["cm2_profit_per_unit"] = np.where(
            g["total_quantity"] != 0,
            (g["cm2_profit"] / g["total_quantity"]).round(2),
            0.0
        )    

    # CM1 profit percentage = profit / net sales
    if "profit" in g.columns and "net_sales" in g.columns:
        g["profit_percentage"] = np.where(
            g["net_sales"] != 0,
            ((g["profit"] / g["net_sales"]) * 100).round(2),
            None
        )

    # CM2 profit percentage = cm2 profit / net sales
    if "cm2_profit" in g.columns and "net_sales" in g.columns:
        g["cm2_profit_percentage"] = np.where(
            g["net_sales"] != 0,
            ((g["cm2_profit"] / g["net_sales"]) * 100).round(2),
            None
        )

    # ACOS = advertising / net sales
    advertising_col = None

    if "advertising_total_final" in g.columns:
        advertising_col = "advertising_total_final"
    elif "advertising_total" in g.columns:
        advertising_col = "advertising_total"

    if advertising_col and "net_sales" in g.columns:
        g["acos"] = np.where(
            g["net_sales"] != 0,
            ((g[advertising_col] / g["net_sales"]) * 100).round(2),
            None
        )

    # Sales mix = SKU net sales / total net sales
    if "net_sales" in g.columns:
        total_net_sales = g["net_sales"].sum()

        g["sales_mix"] = np.where(
            total_net_sales != 0,
            ((g["net_sales"] / total_net_sales) * 100).round(2),
            None
        )

    # Profit mix = SKU CM1 profit / total CM1 profit
    if "profit" in g.columns:
        total_profit = g["profit"].sum()

        g["profit_mix"] = np.where(
            total_profit != 0,
            ((g["profit"] / total_profit) * 100).round(2),
            None
        )    

    # -------------------------------------------------
    # Convert grouped dataframe to expected dict shape
    # -------------------------------------------------
    out = {}

    for r in g.to_dict(orient="records"):
        sku = str(r["sku"])

        raw_name = r.get("product_name")
        product_name = (
            str(raw_name).strip()
            if raw_name not in [None, "", "0", 0]
            else sku
        )

        out[sku] = {
            "product_name": product_name
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



def aggregate_total_rows_for_partial_year(df_total_rows: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregates monthly total rows into one synthetic total row.

    Used only for yearly partial-period comparison:
    Jan-Apr current year vs Jan-Apr previous year.

    Additive metrics are summed.
    Non-additive metrics are recalculated.
    """

    if df_total_rows.empty:
        return pd.DataFrame()

    df = df_total_rows.copy()

    for col in MOVEMENT_COLUMNS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    row = {"sku": "total"}

    # Sum additive metrics
    for col in METRIC_COLUMNS:
        if col in df.columns:
            row[col] = float(df[col].sum())

    # -------------------------------------------------
    # Advertising alignment for yearly partial aggregation
    # -------------------------------------------------
    # Monthly rows have already passed through:
    # apply_advertising_total_final_from_ads_table()
    #
    # So here we only aggregate the already-corrected final ads value.
    advertising_total_sum = (
        float(df["advertising_total"].sum())
        if "advertising_total" in df.columns
        else 0.0
    )

    advertising_final_sum = (
        float(df["advertising_total_final"].sum())
        if "advertising_total_final" in df.columns
        else 0.0
    )

    # Prefer final value when available.
    # Fallback keeps old tables safe.
    advertising = (
        advertising_final_sum
        if advertising_final_sum != 0
        else advertising_total_sum
    )

    row["advertising_total"] = round(advertising, 2)
    row["advertising_total_final"] = round(advertising, 2)

    total_units = row.get("total_quantity", 0)
    net_sales = row.get("net_sales", 0)
    cm1_profit = row.get("profit", 0)
    cm2_profit = row.get("total_cm2_profit", row.get("cm2_profit", 0))

    # Recalculate non-additive metrics
    row["asp"] = round(net_sales / total_units, 2) if total_units else None

    row["unit_wise_profitability"] = (
        round(cm1_profit / total_units, 2) if total_units else None
    )

    row["profit_percentage"] = (
        round((cm1_profit / net_sales) * 100, 2) if net_sales else None
    )

    row["cm2_profit_percentage"] = (
        round((cm2_profit / net_sales) * 100, 2) if net_sales else None
    )

    row["acos"] = (
        round((advertising / net_sales) * 100, 2) if net_sales else None
    )

    return pd.DataFrame([row])




def aggregate_monthly_tables_for_yearly_comparison_generic(
    *,
    user_id: int,
    year: int,
    fetch_monthly_func,
    country: str | None = None,
    apply_ads_final: bool = False,
    max_month: int | None = None,
) -> dict:
    """
    Finds monthly tables available in selected year,
    then aggregates those same months for selected year and previous year.

    Example:
    If selected year has Jan-Apr data,
    compare Jan-Apr selected year vs Jan-Apr previous year.

    If apply_ads_final=True and country is provided:
    - Applies advertising_total_final logic month by month
    - Then aggregates yearly partial totals
    """

    available_months = []

    current_detail_frames = []
    current_total_frames = []

    prev_detail_frames = []
    prev_total_frames = []

    if max_month is None:
        max_month = get_period_month_cap(
            year=year,
            period="yearly",
            timeline="ALL",
        )

    max_month = max(0, min(int(max_month), 12))

    for m in range(1, max_month + 1):
        df_cur = fetch_monthly_func(
            user_id=user_id,
            timeline=str(m),
            year=year,
        )

        # ✅ Apply ads-table final advertising logic for current year month
        if apply_ads_final and country:
            df_cur = apply_advertising_total_final_from_ads_table(
                df_cur,
                user_id=user_id,
                country=country,
                period="monthly",
                timeline=str(m),
                year=year,
            )

        if df_cur.empty:
            continue

        available_months.append(m)

        cur_detail, cur_total = _split_total_row(df_cur)

        if not cur_detail.empty:
            current_detail_frames.append(cur_detail)

        if not cur_total.empty:
            current_total_frames.append(cur_total)

        df_prev = fetch_monthly_func(
            user_id=user_id,
            timeline=str(m),
            year=year - 1,
        )

        # ✅ Apply ads-table final advertising logic for previous year same month
        if apply_ads_final and country:
            df_prev = apply_advertising_total_final_from_ads_table(
                df_prev,
                user_id=user_id,
                country=country,
                period="monthly",
                timeline=str(m),
                year=year - 1,
            )

        prev_detail, prev_total = _split_total_row(df_prev)

        if not prev_detail.empty:
            prev_detail_frames.append(prev_detail)

        if not prev_total.empty:
            prev_total_frames.append(prev_total)

    def concat_or_empty(frames):
        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

    current_detail_all = concat_or_empty(current_detail_frames)
    current_total_all = concat_or_empty(current_total_frames)

    prev_detail_all = concat_or_empty(prev_detail_frames)
    prev_total_all = concat_or_empty(prev_total_frames)

    return {
        "available_months": available_months,
        "df_current_detail": current_detail_all,
        "df_current_total": aggregate_total_rows_for_partial_year(current_total_all),
        "df_prev_detail": prev_detail_all,
        "df_prev_total": aggregate_total_rows_for_partial_year(prev_total_all),
        "sku_current": compute_sku_precalc(current_detail_all),
        "sku_prev": compute_sku_precalc(prev_detail_all),
    }

def aggregate_monthly_tables_for_quarterly_comparison_generic(
    *,
    user_id: int,
    year: int,
    timeline: str,
    fetch_monthly_func,
    country: str | None = None,
    apply_ads_final: bool = False,
) -> dict:
    """
    Builds quarterly comparison from completed monthly tables only.

    Example:
    If Q3 2026 is selected during July 2026:
    latest completed month is June 2026,
    so Q3 has no completed months and July is not included.
    """

    q = int(str(timeline).replace("Q", ""))

    quarter_start_month = (q - 1) * 3 + 1
    quarter_end_month = q * 3

    max_month = get_period_month_cap(
        year=year,
        period="quarterly",
        timeline=timeline,
    )

    max_month = min(quarter_end_month, max_month)

    available_months = []

    current_detail_frames = []
    current_total_frames = []

    prev_detail_frames = []
    prev_total_frames = []

    if max_month < quarter_start_month:
        return {
            "available_months": [],
            "df_current_detail": pd.DataFrame(),
            "df_current_total": pd.DataFrame(),
            "df_prev_detail": pd.DataFrame(),
            "df_prev_total": pd.DataFrame(),
            "sku_current": {},
            "sku_prev": {},
        }

    for m in range(quarter_start_month, max_month + 1):
        df_cur = fetch_monthly_func(
            user_id=user_id,
            timeline=str(m),
            year=year,
        )

        if apply_ads_final and country:
            df_cur = apply_advertising_total_final_from_ads_table(
                df_cur,
                user_id=user_id,
                country=country,
                period="monthly",
                timeline=str(m),
                year=year,
            )

        if df_cur.empty:
            continue

        available_months.append(m)

        cur_detail, cur_total = _split_total_row(df_cur)

        if not cur_detail.empty:
            current_detail_frames.append(cur_detail)

        if not cur_total.empty:
            current_total_frames.append(cur_total)

        # Previous quarter comparison:
        # Q2 Apr-Jun compares to Q1 Jan-Mar.
        # Q1 Jan-Mar compares to Q4 Oct-Dec previous year.
        if q == 1:
            prev_year = year - 1
            prev_month = m + 9
        else:
            prev_year = year
            prev_month = m - 3

        df_prev = fetch_monthly_func(
            user_id=user_id,
            timeline=str(prev_month),
            year=prev_year,
        )

        if apply_ads_final and country:
            df_prev = apply_advertising_total_final_from_ads_table(
                df_prev,
                user_id=user_id,
                country=country,
                period="monthly",
                timeline=str(prev_month),
                year=prev_year,
            )

        prev_detail, prev_total = _split_total_row(df_prev)

        if not prev_detail.empty:
            prev_detail_frames.append(prev_detail)

        if not prev_total.empty:
            prev_total_frames.append(prev_total)

    def concat_or_empty(frames):
        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

    current_detail_all = concat_or_empty(current_detail_frames)
    current_total_all = concat_or_empty(current_total_frames)

    prev_detail_all = concat_or_empty(prev_detail_frames)
    prev_total_all = concat_or_empty(prev_total_frames)

    return {
        "available_months": available_months,
        "df_current_detail": current_detail_all,
        "df_current_total": aggregate_total_rows_for_partial_year(current_total_all),
        "df_prev_detail": prev_detail_all,
        "df_prev_total": aggregate_total_rows_for_partial_year(prev_total_all),
        "sku_current": compute_sku_precalc(current_detail_all),
        "sku_prev": compute_sku_precalc(prev_detail_all),
    }


def fetch_inventory_aged_by_user(user_id: int, country: str | None = None) -> pd.DataFrame:
    currency = None

    if country:
        country = str(country).lower()
        if country == "uk":
            currency = "GBP"
        elif country == "us":
            currency = "USD"

    params = {"user_id": user_id}

    where_clause = "WHERE user_id = :user_id"

    if currency:
        where_clause += " AND UPPER(TRIM(currency)) = :currency"
        params["currency"] = currency

    query = text(f"""
        SELECT
            sku,
            currency,
            "inv-age-0-to-90-days"        AS age_0_90,
            "inv-age-91-to-180-days"      AS age_91_180,
            "inv-age-181-to-270-days"     AS age_181_270,
            "inv-age-271-to-365-days"     AS age_271_365,
            "inv-age-365-plus-days"       AS age_365_plus,
            "estimated-storage-cost-next-month" AS storage_cost_next_month,
            "unfulfillable-quantity"      AS unfulfillable_qty
        FROM public.inventory_aged
        {where_clause}
    """)

    with amazon_engine.connect() as conn:
        df = pd.read_sql(query, conn, params=params)

    return df


def build_inventory_alerts(df: pd.DataFrame, user_id: int, country: str) -> dict:

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
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    alerts = {}

    # ==========================================================
    # 1️⃣ AGEING INVENTORY (181+ DAYS)
    # ==========================================================
    df["aged_181_plus"] = df["age_181_270"] + df["age_271_365"]

    ageing_units = int(df["aged_181_plus"].sum())
    ageing_skus = int((df["aged_181_plus"] > 0).sum())

    alerts["ageing_inventory"] = {
        "total_units": ageing_units,
        "total_skus": ageing_skus
    }

    # ==========================================================
    # 2️⃣ HIGH COVERAGE RATIO SKUS (>4)
    # ==========================================================
    coverage_df = compute_inventory_coverage_ratio(user_id, country)

    if not coverage_df.empty:
        high_coverage = coverage_df[
            coverage_df["inventory_coverage_ratio"] > 4
        ][["sku", "inventory_coverage_ratio"]]

        alerts["high_coverage"] = {
            "count": int(len(high_coverage)),
            "skus": high_coverage.to_dict(orient="records")
        }
    else:
        alerts["high_coverage"] = {
            "count": 0,
            "skus": []
        }

    # ==========================================================
    # 3️⃣ UNFULFILLABLE % CHECK
    # ==========================================================
    total_inventory_units = int(
        df[
            [
                "age_0_90", "age_91_180",
                "age_181_270", "age_271_365",
                "age_365_plus"
            ]
        ].sum().sum()
    )

    unfulfillable_units = int(df["unfulfillable_qty"].sum())

    unfulfillable_pct = (
        (unfulfillable_units / total_inventory_units) * 100
        if total_inventory_units > 0 else 0
    )

    alerts["unfulfillable"] = {
        "units": unfulfillable_units,
        "percentage": round(unfulfillable_pct, 2),
        "status": "above_1_percent" if unfulfillable_pct > 1 else "below_1_percent"
    }

    # ==========================================================
    # 4️⃣ ESTIMATED STORAGE COST
    # ==========================================================
    total_storage_cost = float(df["storage_cost_next_month"].sum())

    alerts["estimated_storage_cost"] = {
        "value": round(total_storage_cost, 2)
    }

    return alerts

def fetch_high_alert_threshold(user_id: int, country: str) -> float | None:
    """
    High alert threshold = transit_time + stock_unit.
    Fallback is handled inside generate_sku_inventory_flags().
    """

    country_key = str(country or "").strip().lower()

    marketplace = None
    if country_key == "uk":
        marketplace = "A1F83G8C2ARO7P"
    elif country_key == "us":
        marketplace = "ATVPDKIKX0DER"

    try:
        query = text("""
            SELECT
                transit_time,
                stock_unit
            FROM public.country_profile
            WHERE user_id = :user_id
              AND LOWER(TRIM(country)) = :country
              AND (:marketplace IS NULL OR marketplace = :marketplace)
            ORDER BY id DESC
            LIMIT 1
        """)

        with phormula_engine.connect() as conn:
            row = conn.execute(query, {
                "user_id": user_id,
                "country": country_key,
                "marketplace": marketplace,
            }).fetchone()

        if not row:
            return None

        transit_time = safe_float(row.transit_time)
        stock_unit = safe_float(row.stock_unit)

        threshold = transit_time + stock_unit

        return float(threshold) if threshold > 0 else None

    except Exception as e:
        print("[WARN] Failed to fetch high alert threshold:", e)
        return None


def generate_sku_inventory_flags(
    user_id: int,
    country: str,
    focus_skus: list[str] | None = None,
    coverage_override_by_sku: dict | None = None,
) -> dict:
    """
    SKU-level inventory flags including inventory_recommendation.
    Same logic as live BI, but compatible with historic inventory_aged aliases.
    """

    flags = {}

    high_alert_threshold = fetch_high_alert_threshold(user_id, country)

    if high_alert_threshold is None:
        high_alert_threshold = 2.0

    coverage_df = compute_inventory_coverage_ratio(user_id, country)

    coverage_map = {}

    if coverage_df is not None and not coverage_df.empty:
        for _, r in coverage_df.iterrows():
            sku = str(r.get("sku") or "").strip()
            if not sku:
                continue

            cov = pd.to_numeric(
                r.get("inventory_coverage_ratio"),
                errors="coerce"
            )

            coverage_map[sku.upper()] = float(cov) if pd.notna(cov) else None

    # selected period override
    for sku, cov in (coverage_override_by_sku or {}).items():
        sku_key = str(sku or "").strip().upper()
        if not sku_key:
            continue

        cov_num = pd.to_numeric(cov, errors="coerce")
        if pd.notna(cov_num):
            coverage_map[sku_key] = float(cov_num)

    inv_df = fetch_inventory_aged_by_user(user_id, country)

    inv_by_sku = {}

    if inv_df is not None and not inv_df.empty:
        for _, r in inv_df.iterrows():
            sku = str(r.get("sku") or "").strip()
            if not sku:
                continue
            inv_by_sku[sku.upper()] = r

    focus_set = (
        {str(x).strip().upper() for x in focus_skus if str(x).strip()}
        if focus_skus
        else None
    )

    candidate_skus = set(coverage_map.keys()) | set(inv_by_sku.keys())

    if focus_set:
        candidate_skus = candidate_skus & focus_set

    def _num(row, *cols):
        if row is None:
            return 0.0

        for col in cols:
            if col not in row:
                continue

            try:
                value = row.get(col)
                if value is None or pd.isna(value):
                    continue
                return float(value)
            except Exception:
                continue

        return 0.0

    for sku_key in sorted(candidate_skus):
        r = inv_by_sku.get(sku_key)

        coverage_ratio = coverage_map.get(sku_key)

        cov_num = pd.to_numeric(coverage_ratio, errors="coerce")
        coverage_ratio = float(cov_num) if pd.notna(cov_num) else None

        legacy_aged_units = (
            _num(r, "age_181_270", "inv-age-181-to-270-days")
            + _num(r, "age_271_365", "inv-age-271-to-365-days")
            + _num(r, "age_365_plus", "inv-age-365-plus-days")
        )

        detailed_aged_units = (
            _num(r, "inv-age-181-to-330-days")
            + _num(r, "inv-age-331-to-365-days")
            + _num(r, "inv-age-366-to-455-days")
            + _num(r, "inv-age-456-plus-days")
        )

        estimated_storage_cost = _num(
            r,
            "storage_cost_next_month",
            "estimated-storage-cost-next-month",
        )

        long_term_aged_units = (
            detailed_aged_units
            if detailed_aged_units > 0
            else legacy_aged_units
        )

        overaged = long_term_aged_units > 0

        alert_type = "none"
        alert = "No alert"

        if (
            coverage_ratio is not None
            and coverage_ratio > 0
            and coverage_ratio <= high_alert_threshold
        ):
            alert_type = "supply"
            alert = "High alert"

        elif (
            coverage_ratio is not None
            and coverage_ratio > high_alert_threshold
            and coverage_ratio <= 5
        ):
            alert_type = "supply"
            alert = "Please send shipment"

        elif coverage_ratio is not None and coverage_ratio >= 6:
            alert_type = "excess"
            alert = "High inventory coverage ratio"

        elif overaged:
            alert_type = "overaged"
            alert = "Long-term aged inventory"

        elif estimated_storage_cost > 100:
            alert_type = "cost"
            alert = "High storage cost"

        cov_str = (
            round(float(coverage_ratio), 1)
            if coverage_ratio is not None
            else None
        )

        inventory_recommendation = "Inventory looks stable. Continue monitoring stock coverage."

        if alert == "High alert" and cov_str is not None:
            inventory_recommendation = (
                f"Your coverage ratio is {cov_str} months. "
                f"Please immediately send stock to avoid stock-out."
            )

        elif alert == "Please send shipment" and cov_str is not None:
            inventory_recommendation = (
                f"Your coverage ratio is {cov_str} months. "
                f"Please supply inventory soon to avoid stock-out risk."
            )

        elif alert == "High inventory coverage ratio" and cov_str is not None:
            if overaged:
                inventory_recommendation = (
                    f"Your coverage ratio is {cov_str} months and "
                    f"{int(long_term_aged_units)} units are ageing long-term. "
                    f"Improve sell-through or reduce replenishment to avoid excess storage fees."
                )
            else:
                inventory_recommendation = (
                    f"Your coverage ratio is {cov_str} months, which may increase storage cost. "
                    f"Please improve sell-through to avoid excess storage fees."
                )

        elif alert == "Long-term aged inventory":
            inventory_recommendation = (
                f"{int(long_term_aged_units)} units are ageing long-term. "
                f"Review and liquidate this stock to avoid additional storage cost."
            )

        elif alert == "High storage cost":
            inventory_recommendation = (
                f"Estimated storage cost is {round(estimated_storage_cost, 2)}. "
                f"Reduce inventory exposure to control storage expense."
            )

        flags[sku_key] = {
            "inventory_alert": alert,
            "inventory_alert_type": alert_type,
            "inventory_coverage_ratio": coverage_ratio,
            "coverage_ratio_months": coverage_ratio,
            "high_alert_threshold": high_alert_threshold,
            "long_term_aged_units": int(long_term_aged_units),
            "estimated_storage_cost": round(estimated_storage_cost, 2),
            "inventory_recommendation": inventory_recommendation,
        }

    return flags



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



def build_remaining_skus_aggregate(
    sku_current: dict,
    sku_prev: dict,
    focus_skus: list[str],
) -> dict:

    focus_set = set(str(s) for s in (focus_skus or []))

    remaining = [
        sku for sku in (set(sku_current.keys()) | set(sku_prev.keys()))
        if str(sku) not in focus_set
        and str(sku).strip().lower() not in TOTAL_LABELS
    ]

    if not remaining:
        return {}

    # ✅ NEW: capture product names/SKUs that are inside Other SKUs
    included_products = []

    for sku in remaining:
        cur_data = sku_current.get(sku, {}) or {}
        prev_data = sku_prev.get(sku, {}) or {}

        product_name = (
            cur_data.get("product_name")
            or prev_data.get("product_name")
            or sku
        )

        included_products.append({
            "sku": str(sku),
            "product_name": str(product_name).strip() or str(sku)
        })

    included_products = sorted(
        included_products,
        key=lambda x: x["product_name"].lower()
    )

    def sum_metric(source: dict, metric: str) -> float:
        total = 0.0
        for sku in remaining:
            try:
                total += float(source.get(sku, {}).get(metric, 0.0) or 0.0)
            except (TypeError, ValueError):
                continue
        return round(total, 2)

    # --- additive ---
    cur_units = sum_metric(sku_current, "total_quantity")
    prev_units = sum_metric(sku_prev, "total_quantity")

    cur_sales = sum_metric(sku_current, "net_sales")
    prev_sales = sum_metric(sku_prev, "net_sales")

    cur_profit = sum_metric(sku_current, "profit")
    prev_profit = sum_metric(sku_prev, "profit")

    cur_productwise_ads = sum_metric(sku_current, "productwise_ads_spend")
    prev_productwise_ads = sum_metric(sku_prev, "productwise_ads_spend")

    cur_cm2_profit = sum_metric(sku_current, "cm2_profit")
    prev_cm2_profit = sum_metric(sku_prev, "cm2_profit")

    # ✅ Current-only selected-period inventory for Other SKUs
    cur_current_inventory = sum_metric(sku_current, "current_inventory")

    # --- recalculated ---
    cur_asp = round(cur_sales / cur_units, 2) if cur_units else None
    prev_asp = round(prev_sales / prev_units, 2) if prev_units else None

    cur_ppu = round(cur_profit / cur_units, 2) if cur_units else None
    prev_ppu = round(prev_profit / prev_units, 2) if prev_units else None

    cur_cm2_ppu = round(cur_cm2_profit / cur_units, 2) if cur_units else None
    prev_cm2_ppu = round(prev_cm2_profit / prev_units, 2) if prev_units else None

    # ✅ Other SKUs coverage ratio = total current inventory / current total quantity
    cur_selected_period_coverage_ratio = (
        round(cur_current_inventory / cur_units, 2)
        if cur_units
        else 0.0
    )

    def mk(cur, prev):
        if cur is None or prev is None:
            return {
                "current": cur,
                "previous": prev,
                "delta": None,
                "delta_pct": None
            }

        delta = round(cur - prev, 2)
        pct = round((delta / prev) * 100, 2) if prev != 0 else None

        return {
            "current": round(cur, 2),
            "previous": round(prev, 2),
            "delta": delta,
            "delta_pct": pct
        }

    return {
        "product_name": "Other SKUs",

        "included_products": included_products,
        "included_product_count": len(included_products),

        "total_quantity": mk(cur_units, prev_units),
        "net_sales": mk(cur_sales, prev_sales),
        "profit": mk(cur_profit, prev_profit),

        "productwise_ads_spend": mk(cur_productwise_ads, prev_productwise_ads),
        "cm2_profit": mk(cur_cm2_profit, prev_cm2_profit),

        # ✅ Current-only selected-period inventory fields
        "current_inventory": round(cur_current_inventory, 2),
        "selected_period_coverage_ratio": cur_selected_period_coverage_ratio,

        "asp": mk(cur_asp, prev_asp),
        "unit_wise_profitability": mk(cur_ppu, prev_ppu),
        "cm2_profit_per_unit": mk(cur_cm2_ppu, prev_cm2_ppu),
    }

def compute_yoy_pct(df_current_total, df_prev_total, col):
    cur = _total_value(df_current_total, col)
    prev = _total_value(df_prev_total, col)
    if cur is None or prev in (None, 0):
        return None
    return round((cur - prev) / abs(prev) * 100, 2)

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

def run_prompt_2_strategy(
    analysis_insights: dict,
    objective_v2: dict,
    focus_skus: list,
    sku_time_series: dict,
    inventory_alerts: dict,
    country: str,
    sku_mom: dict | None = None,

    # ✅ NEW (ads context)
    sku_ads_context: list | None = None,
    ads_monthly: dict | None = None,
    sku_live_context: list | None = None,
    sku_inventory_flags: dict | None = None,
    remaining_skus_context: dict | None = None,

):

    # -------------------------------------------------
    # Universal JSON sanitizer (handles pandas/numpy)
    # -------------------------------------------------
    def _make_json_safe(obj):
        if isinstance(obj, pd.Series):
            return _make_json_safe(obj.iloc[0] if not obj.empty else None)

        if isinstance(obj, (np.integer,)):
            return int(obj)

        if isinstance(obj, (np.floating,)):
            return float(obj)

        if isinstance(obj, float) and np.isnan(obj):
            return None

        if isinstance(obj, dict):
            return {k: _make_json_safe(v) for k, v in obj.items()}

        if isinstance(obj, (list, tuple)):
            return [_make_json_safe(v) for v in obj]

        return obj

    # -------------------------------------------------
    # Build payload (ADS INCLUDED)
    # -------------------------------------------------
    payload = {
        "analysis_insights": analysis_insights,
        "objective_v2": objective_v2,
        "focus_skus": focus_skus,
        "sku_time_series": sku_time_series,
        "inventory_alerts": inventory_alerts,
        "country": country,
        "sku_inventory_flags": sku_inventory_flags or {},
        "sku_ads_context": sku_ads_context or [],
        "ads_monthly": ads_monthly or {},
        "sku_live_context": sku_live_context or [],
        "remaining_skus_context": remaining_skus_context or {},
        "sku_mom": sku_mom or {},
    }

   
    # -------------------------------------------------
    # 🔐 SANITIZE before json.dumps
    # -------------------------------------------------
    safe_payload = _make_json_safe(payload)

    resp = openai_client.chat.completions.create(
        model="gpt-4.1",
        messages=[
            {"role": "system", "content": AI_SYSTEM_PROMPT_2},
            {"role": "user", "content": json.dumps(safe_payload, separators=(",", ":"))},
        ],
        temperature=0.1,
    )

    return resp.choices[0].message.content.strip()

def run_prompt_3_polish(bullets: dict) -> dict:
    resp = openai_client.chat.completions.create(
        model="gpt-4.1",
        messages=[
            {"role": "system", "content": AI_SYSTEM_PROMPT_3_POLISHER},
            {"role": "user", "content": json.dumps(bullets, separators=(",", ":"))}
        ],
        temperature=0.2,
    )
    return json.loads(resp.choices[0].message.content)

def build_excel_sku_recommendations(sku_mom: dict, objective_v2: dict) -> dict:
    """
    Deterministic SKU recommendation lookup from Excel-derived rule engine.
    Includes ads-led visibility driver before falling back to ASP/Units/Sales/CM1 rule.
    """

    growth_intent = str(objective_v2.get("growth_intent", "balanced")).lower()
    profit_priority = str(objective_v2.get("profit_priority", "protect_growth")).lower()

    output = {}

    for sku, metrics in (sku_mom or {}).items():
        if not isinstance(metrics, dict):
            continue

        asp = metrics.get("asp", {}) or {}
        units = metrics.get("total_quantity", {}) or {}
        net_sales = metrics.get("net_sales", {}) or {}
        profit = metrics.get("profit", {}) or {}

        # ✅ productwise ads spend already exists in sku_mom if current/previous SKU data has it
        ads = (
            metrics.get("productwise_ads_spend")
            or metrics.get("advertising_total")
            or metrics.get("advertising")
            or {}
        )

        ads_current = ads.get("current")
        ads_previous = ads.get("previous")
        ads_growth_pct = ads.get("delta_pct")

        rec = get_excel_recommendation_from_metrics(
            asp_current=asp.get("current"),
            asp_previous=asp.get("previous"),

            units_current=units.get("current"),
            units_previous=units.get("previous"),

            net_sales_current=net_sales.get("current"),
            net_sales_previous=net_sales.get("previous"),

            cm1_profit_current=profit.get("current"),
            cm1_profit_previous=profit.get("previous"),

            # ✅ important: pass ads values into helper
            ads_spend_current=ads_current,
            ads_spend_previous=ads_previous,
            ads_spend_growth_pct=ads_growth_pct,

            # ✅ fallback because your historic table may not have ads sales/clicks
            ads_sales_growth_pct=ads_growth_pct,
            ads_clicks_growth_pct=ads_growth_pct,

            growth_intent=growth_intent,
            profit_priority=profit_priority,
        )

        output[sku] = rec

    return output



def select_focus_skus_by_sales_mix(sku_current: dict, threshold: float = 80.0) -> list[str]:
    ranked = []

    # -------------------------------------------------
    # Primary ranking: sales_mix
    # -------------------------------------------------
    for sku, data in sku_current.items():
        if not isinstance(data, dict):
            continue

        sales_mix = data.get("sales_mix")

        if sales_mix is None:
            continue

        try:
            ranked.append((sku, float(sales_mix)))
        except (TypeError, ValueError):
            continue

    # -------------------------------------------------
    # Fallback ranking: net_sales
    # This prevents all products collapsing into Other SKUs
    # if sales_mix is missing for any reason.
    # -------------------------------------------------
    if not ranked:
        for sku, data in sku_current.items():
            if not isinstance(data, dict):
                continue

            net_sales = data.get("net_sales")

            if net_sales is None:
                continue

            try:
                ranked.append((sku, float(net_sales)))
            except (TypeError, ValueError):
                continue

    ranked.sort(key=lambda x: x[1], reverse=True)

    if not ranked:
        return []

    cumulative = 0.0
    selected = []

    using_sales_mix = any(
        isinstance(data, dict) and data.get("sales_mix") is not None
        for data in sku_current.values()
    )

    for sku, value in ranked:
        selected.append(sku)

        if using_sales_mix:
            cumulative += value

            if cumulative >= threshold:
                break

        # If fallback is net_sales, just use top 5
        else:
            if len(selected) >= 5:
                break

    if len(selected) < 5:
        return [sku for sku, _ in ranked[:5]]

    return selected

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

def fetch_high_alert_threshold(user_id: int, country: str):
    query = text("""
        SELECT
            transit_time,
            stock_unit
        FROM public.country_profile
        WHERE user_id = :user_id
          AND LOWER(country) = :country
        LIMIT 1
    """)

    with phormula_engine.connect() as conn:
        row = conn.execute(query, {
            "user_id": user_id,
            "country": str(country or "").strip().lower(),
        }).fetchone()

    if not row:
        return None

    transit_time = pd.to_numeric(row.transit_time, errors="coerce")
    stock_unit = pd.to_numeric(row.stock_unit, errors="coerce")

    if pd.isna(transit_time) or pd.isna(stock_unit):
        return None

    return float(transit_time) + float(stock_unit)

def build_sku_inventory_flags(inventory_df: pd.DataFrame, user_id: int, country: str) -> dict:
    """
    Returns SKU-level inventory risk flags + classified alert.

    High alert rule:
        coverage_ratio <= transit_time + stock_unit

    Fallback:
        if country_profile is missing, old threshold 2 is used.
    """

    if inventory_df.empty:
        return {}

    df = inventory_df.copy()

    # -------------------------------
    # Dynamic High Alert threshold
    # -------------------------------
    high_alert_threshold = fetch_high_alert_threshold(user_id, country)

    if high_alert_threshold is None:
        high_alert_threshold = 2.0

    # -------------------------------
    # Safe numeric coercion
    # -------------------------------
    numeric_cols = [
        "age_0_90",
        "age_91_180",
        "age_181_270",
        "age_271_365",
        "age_365_plus",
        "storage_cost_next_month",
        "unfulfillable_qty",
    ]

    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    df["aged_181_plus"] = df["age_181_270"] + df["age_271_365"]

    # -------------------------------
    # Coverage ratio
    # -------------------------------
    coverage_df = compute_inventory_coverage_ratio(user_id, country)

    coverage_map = {}

    if coverage_df is not None and not coverage_df.empty:
        for _, r in coverage_df.iterrows():
            sku = str(r.get("sku") or "").strip()
            if not sku:
                continue

            cov = pd.to_numeric(
                r.get("inventory_coverage_ratio"),
                errors="coerce",
            )

            coverage_map[sku.upper()] = float(cov) if pd.notna(cov) else None

    sku_inventory_flags = {}

    for _, row in df.iterrows():
        sku = str(row["sku"] or "").strip()
        if not sku:
            continue

        sku_key = sku.upper()

        coverage_ratio = coverage_map.get(sku_key)

        cov_num = pd.to_numeric(coverage_ratio, errors="coerce")
        coverage_ratio = float(cov_num) if pd.notna(cov_num) else None

        aged_181_plus = int(row["aged_181_plus"])
        long_term_aged = int(row["age_365_plus"])
        estimated_storage_cost = float(row["storage_cost_next_month"])
        unfulfillable_qty = int(row["unfulfillable_qty"])

        overaged = long_term_aged > 0

        # =========================================================
        # INVENTORY ALERT CLASSIFICATION LOGIC
        # =========================================================

        alert = None
        alert_type = None

        # 1. SUPPLY RISK
        if (
            coverage_ratio is not None
            and coverage_ratio > 0
            and coverage_ratio <= high_alert_threshold
        ):
            alert = "High alert"
            alert_type = "supply"

        elif (
            coverage_ratio is not None
            and coverage_ratio > high_alert_threshold
            and coverage_ratio <= 5
        ):
            alert = "Please send shipment"
            alert_type = "supply"

        # 2. EXCESS INVENTORY
        elif coverage_ratio is not None and coverage_ratio >= 6 and not overaged:
            alert = "High inventory coverage ratio"
            alert_type = "excess"

        # 3. HIGH STORAGE COST
        elif estimated_storage_cost > 100:
            alert = "High storage cost"
            alert_type = "cost"

        # 4. OVERAGED PRIORITY
        if overaged:
            alert = "Long-term aged inventory"
            alert_type = "overaged"

        # =========================================================

        sku_inventory_flags[sku] = {
            "aged_181_plus_units": aged_181_plus,
            "long_term_aged_units": long_term_aged,
            "unfulfillable_qty": unfulfillable_qty,
            "inventory_coverage_ratio": coverage_ratio,
            "high_alert_threshold": high_alert_threshold,
            "estimated_storage_cost": estimated_storage_cost,
            "inventory_alert": alert,
            "inventory_alert_type": alert_type,
        }

    return sku_inventory_flags


def render_month_end_summary(
    *,
    period: str,
    timeline: str,
    year: int,
    analysis_insights: dict | None,
    mom: dict,
    sku_mom: dict,
    focus_skus: list,
    inventory_alerts: dict,
    inventory_lost: float,
    currency_symbol: str,
    strategy_actions: dict | None = None,
    portfolio_recommendation: str | None = None,
    remaining_agg: dict | None = None,

    # ✅ Monthly-only portfolio inventory coverage line
    portfolio_inventory_coverage: dict | None = None,

    # ✅ NEW: all SKU individual insights
    all_sku_mom: dict | None = None,
) -> str:
    """
    Deterministic executive month-end / year-end summary renderer.
    Presentation-only. Fully None-safe.
    """

    def fmt_pct(x):
        return f"{x:+.2f}%" if isinstance(x, (int, float)) else "N/A"

    def fmt_number(x, decimals=2):
        if not isinstance(x, (int, float)):
            return "N/A"

        if decimals == 0:
            return f"{int(round(x)):,}"   # 👈 no decimals
        return f"{x:,.{decimals}f}"

    def fmt_currency(x):
        return f"{currency_symbol}{x:,.2f}" if isinstance(x, (int, float)) else "N/A"

    def fmt_value_with_pct(metric_dict, is_currency=False, decimals=2):
        if not isinstance(metric_dict, dict):
            return "N/A"

        current = metric_dict.get("current")
        pct = metric_dict.get("delta_pct")

        if is_currency:
            current_str = fmt_currency(current)
        else:
            current_str = fmt_number(current, decimals=decimals)

        pct_str = fmt_pct(pct)

        return f"{current_str} ({pct_str})"


    def fmt_current_only(value, is_currency=False, decimals=2):
        """
        Formats current-only values like inventory and coverage ratio.
        Does not expect current/previous/delta object.
        """
        if not isinstance(value, (int, float)):
            return "N/A"

        if is_currency:
            return fmt_currency(value)

        return fmt_number(value, decimals=decimals)   

    is_yearly = period == "yearly"
    comparison = build_comparison_label(period, timeline, year)

    lines: list[str] = []

    # =========================================================
    # REPORT TITLE
    # =========================================================
    if period == "yearly":
        lines.append("Yearly Business Summary")
    elif period == "quarterly":
        lines.append("Quarterly Business Summary")
    else:
        lines.append("Month-end Business Summary")

    # =========================================================
    # SUMMARY — PORTFOLIO LEVEL (PERCENTAGE ONLY)
    # =========================================================
    if analysis_insights:

        lines.append("## SUMMARY")

        es = analysis_insights.get("executive_summary_signals", {})

        if is_yearly:
            lines.append(f"Year-over-Year Performance Summary ({comparison})")
        else:
            lines.append(f"Performance Summary ({comparison})")

        takeaway = analysis_insights.get("executive_takeaway")
        if isinstance(takeaway, str) and takeaway.strip():
            lines.append(takeaway)

        # ✅ Monthly-only portfolio inventory coverage line
        if (
            period == "monthly"
            and isinstance(portfolio_inventory_coverage, dict)
            and portfolio_inventory_coverage.get("sentence")
        ):
            lines.append(portfolio_inventory_coverage["sentence"])

       

    # =========================================================
    # PORTFOLIO RECOMMENDATION
    # =========================================================
    if portfolio_recommendation and portfolio_recommendation.strip():
        lines.append("\n## PORTFOLIO DIRECTION")
        lines.append(f"• {portfolio_recommendation}")        

    # =========================================================
    # PRODUCT INSIGHTS (PERCENTAGE ONLY)
    # =========================================================
    lines.append("\n## PRODUCT INSIGHTS")

    sku_actions = strategy_actions or {}

    for sku in focus_skus:
        s = sku_mom.get(sku)
        if not isinstance(s, dict):
            continue

        name = s.get("product_name", sku)
        lines.append(f"\n{name}")

        lines.append(
            f"• ASP: {fmt_value_with_pct(s.get('asp'), is_currency=True)}"
        )

        lines.append(
            f"• Units: {fmt_value_with_pct(s.get('total_quantity'), decimals=0)}"
        )

        lines.append(
            f"• Net sales: {fmt_value_with_pct(s.get('net_sales'), is_currency=True, decimals=0)}"
        )

        lines.append(
            f"• CM1 profit: {fmt_value_with_pct(s.get('profit'), is_currency=True, decimals=0)}"
        )

        lines.append(
            f"• CM1 profit per unit: {fmt_value_with_pct(s.get('unit_wise_profitability'), is_currency=True)}"
        )

        lines.append(
            f"• Productwise ads spend: {fmt_value_with_pct(s.get('productwise_ads_spend'), is_currency=True)}"
        )

        lines.append(
            f"• CM2 profit: {fmt_value_with_pct(s.get('cm2_profit'), is_currency=True, decimals=0)}"
        )

        lines.append(
            f"• CM2 profit per unit: {fmt_value_with_pct(s.get('cm2_profit_per_unit'), is_currency=True)}"
        )

        lines.append(
            f"• Current inventory: {fmt_current_only(s.get('current_inventory'), decimals=0)}"
        )

        lines.append(
            f"• Coverage ratio: {fmt_current_only(s.get('selected_period_coverage_ratio'))}"
        )

        # Product Journey
        sku_data = sku_actions.get(sku, {})
        journey = sku_data.get("journey_summary")

        if isinstance(journey, list) and journey:
            lines.append("• Product journey:")
            for point in journey:
                lines.append(f"   - {point}")

        # Recommendation
        recommendation = sku_data.get("recommendation")
        if isinstance(recommendation, str) and recommendation.strip():
            lines.append(f"• Recommendation: {recommendation}")

        # # Inventory Recommendation (SKU-level)
        # inv_rec = sku_data.get("inventory_recommendation")
        # if isinstance(inv_rec, str) and inv_rec.strip():
        #     lines.append(f"• Inventory action: {inv_rec}")    

    # =========================================================
    # REMAINING SKUS — METRICS + JOURNEY + RECOMMENDATION
    # =========================================================
    remaining_rec = sku_actions.get("remaining_skus_recommendation")
    remaining_journey = sku_actions.get("remaining_skus_journey_summary")

    if remaining_agg:

        lines.append("\nOther SKUs")

        # --- Aggregated Metrics ---
        # IMPORTANT:
        # Keep ASP immediately after "Other SKUs".
        # Frontend parser expects product title -> metric line.
        lines.append(
            f"• ASP: {fmt_value_with_pct(remaining_agg.get('asp'), is_currency=True)}"
        )

        lines.append(
            f"• Units: {fmt_value_with_pct(remaining_agg.get('total_quantity'), decimals=0)}"
        )

        lines.append(
            f"• Net sales: {fmt_value_with_pct(remaining_agg.get('net_sales'), is_currency=True)}"
        )

        lines.append(
            f"• CM1 profit: {fmt_value_with_pct(remaining_agg.get('profit'), is_currency=True)}"
        )

        lines.append(
            f"• CM1 profit per unit: {fmt_value_with_pct(remaining_agg.get('unit_wise_profitability'), is_currency=True)}"
        )

        lines.append(
            f"• Productwise ads spend: {fmt_value_with_pct(remaining_agg.get('productwise_ads_spend'), is_currency=True)}"
        )

        lines.append(
            f"• CM2 profit: {fmt_value_with_pct(remaining_agg.get('cm2_profit'), is_currency=True)}"
        )

        lines.append(
            f"• CM2 profit per unit: {fmt_value_with_pct(remaining_agg.get('cm2_profit_per_unit'), is_currency=True)}"
        )

        lines.append(
            f"• Current inventory: {fmt_current_only(remaining_agg.get('current_inventory'), decimals=0)}"
        )

        lines.append(
            f"• Coverage ratio: {fmt_current_only(remaining_agg.get('selected_period_coverage_ratio'))}"
        )

        # ✅ Product names should come AFTER metrics
        included_products = remaining_agg.get("included_products", [])

        if isinstance(included_products, list) and included_products:
            lines.append("• Products included:")

            for item in included_products:
                if not isinstance(item, dict):
                    continue

                product_name = item.get("product_name")
                sku = item.get("sku")

                if product_name and sku:
                    lines.append(f"   - {product_name} ({sku})")
                elif product_name:
                    lines.append(f"   - {product_name}")

        # --- Journey ---
        if isinstance(remaining_journey, list) and remaining_journey:
            lines.append("• Product journey:")
            for point in remaining_journey:
                lines.append(f"   - {point}")

        # --- Recommendation ---
        if isinstance(remaining_rec, str) and remaining_rec.strip():
            lines.append(f"• Recommendation: {remaining_rec}")



    # =========================================================
    # ALL SKUs — INDIVIDUAL METRICS
    # Does NOT change Top 80% / Other SKUs logic
    # =========================================================
    if all_sku_mom:

        lines.append("\n## ALL SKU INDIVIDUAL INSIGHTS")

        focus_set = set(str(s) for s in (focus_skus or []))

        def _sku_sort_value(item):
            sku, data = item
            if not isinstance(data, dict):
                return 0

            net_sales = data.get("net_sales", {}) or {}
            if not isinstance(net_sales, dict):
                return 0

            return net_sales.get("current") or 0

        for sku, s in sorted(all_sku_mom.items(), key=_sku_sort_value, reverse=True):
            if not isinstance(s, dict):
                continue

            sku_clean = str(sku).strip()

            if sku_clean.lower() in TOTAL_LABELS:
                continue

            name = s.get("product_name", sku_clean)
            sku_bucket = "Top 80% SKU" if sku_clean in focus_set else "Other SKU"

            lines.append(f"\n{name}")
            lines.append(f"• SKU: {sku_clean}")
            lines.append(f"• Bucket: {sku_bucket}")

            lines.append(
                f"• ASP: {fmt_value_with_pct(s.get('asp'), is_currency=True)}"
            )

            lines.append(
                f"• Units: {fmt_value_with_pct(s.get('total_quantity'), decimals=0)}"
            )

            lines.append(
                f"• Net sales: {fmt_value_with_pct(s.get('net_sales'), is_currency=True, decimals=0)}"
            )

            lines.append(
                f"• CM1 profit: {fmt_value_with_pct(s.get('profit'), is_currency=True, decimals=0)}"
            )

            lines.append(
                f"• CM1 profit per unit: {fmt_value_with_pct(s.get('unit_wise_profitability'), is_currency=True)}"
            )

            lines.append(
                f"• Productwise ads spend: {fmt_value_with_pct(s.get('productwise_ads_spend'), is_currency=True)}"
            )

            lines.append(
                f"• CM2 profit: {fmt_value_with_pct(s.get('cm2_profit'), is_currency=True, decimals=0)}"
            )

            lines.append(
                f"• CM2 profit per unit: {fmt_value_with_pct(s.get('cm2_profit_per_unit'), is_currency=True)}"
            )

            lines.append(
                f"• Current inventory: {fmt_current_only(s.get('current_inventory'), decimals=0)}"
            )

            lines.append(
                f"• Coverage ratio: {fmt_current_only(s.get('selected_period_coverage_ratio'))}"
            )

            # ✅ NEW: Individual journey + recommendation for every SKU
            sku_data = sku_actions.get(sku_clean, {}) if isinstance(sku_actions, dict) else {}

            journey = sku_data.get("journey_summary")
            if isinstance(journey, list) and journey:
                lines.append("• Product journey:")
                for point in journey:
                    if isinstance(point, str) and point.strip():
                        lines.append(f"   - {point}")

            recommendation = sku_data.get("recommendation")
            if isinstance(recommendation, str) and recommendation.strip():
                lines.append(f"• Recommendation: {recommendation}")

            



    # =========================================================
    # INVENTORY (5 EXECUTIVE POINTS ONLY)
    # =========================================================
    if inventory_alerts:
        lines.append("\n## INVENTORY")

        ageing = inventory_alerts.get("ageing_inventory", {})
        lines.append(
            f"• Ageing inventory (181+ days): "
            f"{ageing.get('total_units',0)} units across "
            f"{ageing.get('total_skus',0)} SKUs"
        )

        high_cov = inventory_alerts.get("high_coverage", {})
        lines.append(
            f"• High coverage SKUs: "
            f"{high_cov.get('count',0)} SKUs"
        )

        unful = inventory_alerts.get("unfulfillable", {})
        if unful.get("status") == "above_1_percent":
            lines.append(
                f"• Unfulfillable inventory is above 1% of total inventory "
                f"({unful.get('percentage')}%)"
            )
        else:
            lines.append(
                f"• Unfulfillable inventory remains below 1% "
                f"({unful.get('percentage')}%)"
            )

        storage = inventory_alerts.get("estimated_storage_cost", {})
        lines.append(
            f"• Est. storage cost next month: "
            f"{currency_symbol}{storage.get('value',0):,.2f}"
        )

        lines.append(
            "• For detailed inventory insights, please refer to the Inventory Reconciliation tab."
        )



    return "\n".join(lines)

# def get_or_create_summary(
#     user_id,
#     country,
#     marketplace_id,
#     period,
#     timeline,
#     year,
#     objective=None,
#     target_sku: str | list | None = None,
#     force_regenerate=False
# ):

    

#     # ============================================================
#     # LOAD OBJECTIVE FROM DB
#     # ============================================================
#     user_objective_row = UserObjective.query.filter_by(
#         user_id=user_id,
#         country=country
#     ).first()

#     if user_objective_row:
#         objective_v2 = {
#             "growth_intent": user_objective_row.growth_intent,
#             "profit_priority": user_objective_row.profit_priority,
#             "inventory_clearance_priority": user_objective_row.inventory_clearance_priority,
#             "business_context": user_objective_row.business_context,
#             "country": str(country).lower(),
#             "time_horizon": "1_month"
#         }
#     else:
#         objective_v2 = {
#             "growth_intent": "balanced",
#             "profit_priority": "protect_growth",
#             "inventory_clearance_priority": False,
#             "business_context": None,
#             "country": str(country).lower(),
#             "time_horizon": "1_month"
#         }

#     # ============================================================
#     # PERIOD RESOLUTION
#     # ============================================================
#     user_selected = bool(period and timeline and year)

#     if not user_selected:
#         year, month = resolve_latest_available_month(user_id, country)
#         timeline = str(month)
#         period = "monthly"

#     is_latest = is_latest_period(
#         period, timeline, year,
#         user_id=user_id,
#         country=country
#     )

#     # 🔥 NEW CONTROL FLAGS
#     allow_inventory = False
#     allow_recommendations = False

#     if period in ("monthly", "quarterly"):
#         allow_inventory = is_latest
#         allow_recommendations = is_latest

#     elif period == "yearly":
#         allow_inventory = is_latest
#         allow_recommendations = is_latest

        

#     # ============================================================
#     # CACHE CHECK
#     # ============================================================
#     cached = fetch_existing_summary(
#         user_id, country, marketplace_id, period, timeline, year
#     )

#     if cached and not force_regenerate and not target_sku:
#         return {
#             "summary": cached.summary,
#             "recommendations": (
#                 json.loads(cached.recommendations)
#                 if cached.recommendations else {}
#             ),
#             "source": "db",
#             "scope": "portfolio",
#             "objective": objective_v2
#         }

#     # ============================================================
#     # CURRENT DATA
#     # ============================================================
#     quarterly_compare = None

#     df_current = fetch_precalc_table(user_id, country, period, timeline, year)

#     df_current = apply_productwise_ads_cm2_from_adsmonthly(
#         df_current,
#         user_id=user_id,
#         country=country,
#         period=period,
#         timeline=timeline,
#         year=year,
#     )

#     df_current = apply_advertising_total_final_from_ads_table(
#         df_current,
#         user_id=user_id,
#         country=country,
#         period=period,
#         timeline=timeline,
#         year=year,
#     )

#     df_current_detail, df_current_total = _split_total_row(df_current)

#     sku_current = compute_sku_precalc(df_current_detail)

#     # For incomplete current-year quarter, do not use quarterly table
#     # because it may contain current running month data.
#     # Build quarter only from completed monthly tables.
#     if period == "quarterly":
#         quarterly_max_month = get_period_month_cap(
#             year=year,
#             period="quarterly",
#             timeline=timeline,
#         )

#         q = int(str(timeline).replace("Q", ""))
#         quarter_start_month = (q - 1) * 3 + 1
#         quarter_end_month = q * 3

#         if quarterly_max_month < quarter_end_month:
#             quarterly_compare = aggregate_monthly_tables_for_quarterly_comparison_generic(
#                 user_id=user_id,
#                 year=year,
#                 timeline=timeline,
#                 country=country,
#                 apply_ads_final=True,
#                 fetch_monthly_func=lambda user_id, timeline, year: fetch_precalc_table(
#                     user_id=user_id,
#                     country=country,
#                     period="monthly",
#                     timeline=timeline,
#                     year=year,
#                 ),
#             )

#             df_current_detail = quarterly_compare["df_current_detail"]
#             df_current_total = quarterly_compare["df_current_total"]
#             sku_current = quarterly_compare["sku_current"]

#     # ✅ Portfolio-level inventory coverage for Performance Summary
#     # Monthly only.
#     portfolio_inventory_coverage = {}

#     # ✅ Add selected-period current inventory and coverage ratio only
#     if period == "monthly":
#         sku_current = enrich_sku_current_with_selected_inventory(
#             sku_current,
#             user_id=user_id,
#             year=year,
#             month=int(timeline),
#         )

#         portfolio_inventory_coverage = build_portfolio_inventory_coverage_summary(
#             user_id=user_id,
#             country=country,
#             year=year,
#             month=int(timeline),
#             df_current_total=df_current_total,
#         )

#     top_5_skus = select_focus_skus_by_sales_mix(sku_current)

#     # ============================================================
#     # SINGLE SKU MODE
#     # ============================================================
#     single_sku_mode = False
#     scope = "portfolio"

#     if target_sku:
#         single_sku_mode = True
#         scope = "sku"

#         if isinstance(target_sku, list):
#             target_sku = target_sku[0]

#         target_sku = str(target_sku).strip()

#         if target_sku in sku_current:
#             top_5_skus = [target_sku]
#             sku_current = {target_sku: sku_current[target_sku]}
#         else:
#             return {
#                 "summary": f"I couldn’t find SKU '{target_sku}' in the selected period.",
#                 "recommendations": {},
#                 "inventory_lost": 0.0,
#                 "inventory_alerts": {},
#                 "sku_current": {},
#                 "sku_mom": {},
#                 "sku_yoy": None,
#                 "objective": objective_v2,
#                 "sku_actions": {},
#                 "scope": "sku",
#                 "source": "no_data",
#             }

#     # ============================================================
#     # ROLLING CONTEXT (RUN FOR BOTH PORTFOLIO AND SINGLE SKU)
#     # ============================================================
#     movement_context = {}
#     rolling_extremes = {}
#     yearly_temporal_signals = None
#     analysis_anchor_year = None
#     analysis_anchor_month = None
#     rolling_series = []

#     if period == "yearly":
#         anchor = resolve_yearly_analysis_anchor(user_id, country, year)
#         if anchor:
#             analysis_anchor_year, analysis_anchor_month = anchor
#     else:
#         analysis_anchor_year = year

#         if period == "monthly":
#             analysis_anchor_month = int(timeline)

#         elif period == "quarterly":
#             analysis_anchor_month = get_period_month_cap(
#                 year=year,
#                 period="quarterly",
#                 timeline=timeline,
#             )

#             if analysis_anchor_month <= 0:
#                 analysis_anchor_month = None

#     if analysis_anchor_year and analysis_anchor_month:

#         rolling_series = build_rolling_monthly_series(
#             user_id=user_id,
#             country=country,
#             anchor_year=analysis_anchor_year,
#             anchor_month=analysis_anchor_month
#         )

#         movement_context = build_movement_context(rolling_series)

#         rolling_extremes = extract_rolling_extremes(rolling_series)

#         if period == "yearly":
#             yearly_temporal_signals = build_yearly_temporal_signals(rolling_series) or None


                

#     # ============================================================
#     # INVENTORY
#     # ============================================================

#     lost_total_val = _total_value(df_current_total, "lost_total")
#     inventory_lost = round(abs(lost_total_val), 2) if lost_total_val is not None else 0.0

#     if single_sku_mode:
#         inventory_lost = 0.0

#     inventory_alerts = {}        # ✅ portfolio-level alerts
#     sku_inventory_flags = {}     # ✅ SKU-level inventory recommendations

#     if allow_inventory:

#         inventory_aged_df = fetch_inventory_aged_by_user(user_id, country=country)

#         if not inventory_aged_df.empty:

#             # 🔵 PORTFOLIO ALERTS (KEEP SAME)
#             inventory_alerts = build_inventory_alerts(
#                 inventory_aged_df,
#                 user_id=user_id,
#                 country=country
#             )

#         # ✅ IMPORTANT:
#         # Build SKU-level deterministic inventory recommendation
#         # after selected-period inventory has already been added to sku_current.
#         coverage_override_by_sku = {}

#         for sku, row in (sku_current or {}).items():
#             if not isinstance(row, dict):
#                 continue

#             sku_key = str(sku or "").strip().upper()
#             if not sku_key:
#                 continue

#             cov = row.get("selected_period_coverage_ratio")

#             if cov is None:
#                 cov = row.get("inventory_coverage_ratio")

#             if cov is None:
#                 cov = row.get("coverage_ratio_months")

#             if cov is not None:
#                 coverage_override_by_sku[sku_key] = cov

#         sku_inventory_flags = generate_sku_inventory_flags(
#             user_id=user_id,
#             country=country,
#             focus_skus=top_5_skus if single_sku_mode else None,
#             coverage_override_by_sku=coverage_override_by_sku,
#         )   

    

#     # ============================================================
#     # PREVIOUS PERIOD / YEARLY PARTIAL-YEAR COMPARISON
#     # ============================================================
#     period_absolute_changes = {}
#     period_pct_changes = None
#     comparison_months = []

#     if period == "yearly":
#         yearly_compare = aggregate_monthly_tables_for_yearly_comparison_generic(
#             user_id=user_id,
#             year=year,
#             country=country,
#             apply_ads_final=True,
#             max_month=get_period_month_cap(
#                 year=year,
#                 period="yearly",
#                 timeline="ALL",
#             ),
#             fetch_monthly_func=lambda user_id, timeline, year: fetch_precalc_table(
#                 user_id=user_id,
#                 country=country,
#                 period="monthly",
#                 timeline=timeline,
#                 year=year,
#             ),
#         )

#         comparison_months = yearly_compare["available_months"]

#         df_current_total_for_comparison = yearly_compare["df_current_total"]
#         df_prev_total_for_comparison = yearly_compare["df_prev_total"]

#         sku_current_for_comparison = yearly_compare["sku_current"]
#         sku_prev = yearly_compare["sku_prev"]

#     ###############################################
#         # print("\n================ YEARLY COMPARE DEBUG ================")
#         # print("period:", period, "timeline:", timeline, "year:", year)
#         # print("comparison_months:", comparison_months)

#         # print("CURRENT yearly comparison total row:")
#         # print(
#         #     df_current_total_for_comparison[
#         #         [
#         #             c for c in [
#         #                 "advertising_total",
#         #                 "advertising_total_final",
#         #                 "net_sales",
#         #                 "acos"
#         #             ]
#         #             if c in df_current_total_for_comparison.columns
#         #         ]
#         #     ].to_dict(orient="records")
#         # )

#         # print("PREVIOUS yearly comparison total row:")
#         # print(
#         #     df_prev_total_for_comparison[
#         #         [
#         #             c for c in [
#         #                 "advertising_total",
#         #                 "advertising_total_final",
#         #                 "net_sales",
#         #                 "acos"
#         #             ]
#         #             if c in df_prev_total_for_comparison.columns
#         #         ]
#         #     ].to_dict(orient="records")
#         # )

#         # print(
#         #     "current ads final:",
#         #     _advertising_total_final_value(df_current_total_for_comparison)
#         # )
#         # print(
#         #     "previous ads final:",
#         #     _advertising_total_final_value(df_prev_total_for_comparison)
#         # )
#         # print("======================================================\n")    
# ############################################################################################################################
#     else:
#         (p_period, p_timeline, p_year), _ = resolve_comparison(period, timeline, year)

#         df_prev = fetch_precalc_table(user_id, country, p_period, p_timeline, p_year)

#         df_prev = apply_productwise_ads_cm2_from_adsmonthly(
#             df_prev,
#             user_id=user_id,
#             country=country,
#             period=p_period,
#             timeline=p_timeline,
#             year=p_year,
#         )

#         df_prev = apply_advertising_total_final_from_ads_table(
#             df_prev,
#             user_id=user_id,
#             country=country,
#             period=p_period,
#             timeline=p_timeline,
#             year=p_year,
#         )

#         df_prev_detail, df_prev_total = _split_total_row(df_prev)

#         df_current_total_for_comparison = df_current_total
#         df_prev_total_for_comparison = df_prev_total

#         sku_current_for_comparison = sku_current
#         sku_prev = compute_sku_precalc(df_prev_detail)

#     if (
#         not df_current_total_for_comparison.empty
#         and not df_prev_total_for_comparison.empty
#     ):
#         period_absolute_changes = compute_period_absolute_changes(
#             df_current_total_for_comparison,
#             df_prev_total_for_comparison,
#         )

#         period_pct_changes = compute_period_pct_changes(
#             df_current_total_for_comparison,
#             df_prev_total_for_comparison,
#         )
# #####################################################################################
#         # print("\n================ PERIOD CHANGE DEBUG ================")
#         # print("period_absolute_changes:", json.dumps(period_absolute_changes, indent=2))
#         # print("period_pct_changes:", json.dumps(period_pct_changes, indent=2))
#         # print(
#         #     "manual ads pct from final values:",
#         #     round(
#         #         (
#         #             _advertising_total_final_value(df_current_total_for_comparison)
#         #             - _advertising_total_final_value(df_prev_total_for_comparison)
#         #         )
#         #         / abs(_advertising_total_final_value(df_prev_total_for_comparison))
#         #         * 100,
#         #         2
#         #     )
#         #     if _advertising_total_final_value(df_prev_total_for_comparison) not in (None, 0)
#         #     else None
#         # )
#         # print("=====================================================\n")
# #######################################################################################

#         # -------------------------------------------------
#         # -------------------------------------------------
#         # Current / previous values for LLM context
#         # -------------------------------------------------
#         # For yearly:
#         #   - comparison values still come from monthly partial-year aggregation
#         #   - current display values should come from the selected yearly table
#         #
#         # This makes yearly current_values.advertising match the yearly table value
#         # e.g. 8046 instead of Jan-May monthly aggregation 7838.93.
#         # -------------------------------------------------

#         current_values_source = (
#             df_current_total
#             if period == "yearly"
#             else df_current_total_for_comparison
#         )

#         previous_values_source = df_prev_total_for_comparison

#         period_absolute_changes["current_values"] = {
#             "units": _total_value(current_values_source, "total_quantity"),
#             "net_sales": _total_value(current_values_source, "net_sales"),
#             "asp": _total_value(current_values_source, "asp"),
#             "cm1_profit": _total_value(current_values_source, "profit"),
#             "cm1_profit_per_unit": _total_value(
#                 current_values_source,
#                 "unit_wise_profitability"
#             ),
#             "cm2_profit": _total_value(current_values_source, "cm2_profit"),

#             # Important: final ads value
#             # For yearly this now comes from df_current_total, not monthly aggregation.
#             "advertising": _advertising_total_final_value(current_values_source),

#             "storage_fees": _total_value(
#                 current_values_source,
#                 "platform_fee_inventory_storage"
#             ),
#             "acos": _total_value(current_values_source, "acos"),
#         }

#         period_absolute_changes["previous_values"] = {
#             "units": _total_value(previous_values_source, "total_quantity"),
#             "net_sales": _total_value(previous_values_source, "net_sales"),
#             "asp": _total_value(previous_values_source, "asp"),
#             "cm1_profit": _total_value(previous_values_source, "profit"),
#             "cm1_profit_per_unit": _total_value(
#                 previous_values_source,
#                 "unit_wise_profitability"
#             ),
#             "cm2_profit": _total_value(previous_values_source, "cm2_profit"),

#             # Previous still comes from the comparison basis:
#             # same available months of previous year.
#             "advertising": _advertising_total_final_value(previous_values_source),

#             "storage_fees": _total_value(
#                 previous_values_source,
#                 "platform_fee_inventory_storage"
#             ),
#             "acos": _total_value(previous_values_source, "acos"),
#         }

#     # -------------------------------------------------
#     # Yearly advertising override
#     # -------------------------------------------------
#     # Because yearly current advertising should match the selected yearly table,
#     # recompute advertising deltas using the same current source.
#     # -------------------------------------------------

#     if period == "yearly":
#         current_ads = _advertising_total_final_value(current_values_source)
#         previous_ads = _advertising_total_final_value(previous_values_source)

#         if current_ads is not None and previous_ads is not None:
#             period_absolute_changes["advertising"] = round(
#                 current_ads - previous_ads,
#                 2
#             )

#         if current_ads is not None and previous_ads not in (None, 0):
#             period_pct_changes["advertising"] = round(
#                 (current_ads - previous_ads) / abs(previous_ads) * 100,
#                 2
#             )    

#     sku_mom = compare_sku_metrics(
#     sku_current_for_comparison,
#     sku_prev,
#     )

#     # ✅ Add current-only selected-period inventory fields into sku_mom
#     # These are NOT previous/delta metrics.
#     for sku, curr_data in (sku_current_for_comparison or {}).items():
#         if sku not in sku_mom:
#             continue

#         if not isinstance(curr_data, dict):
#             continue

#         sku_mom[sku]["current_inventory"] = curr_data.get("current_inventory", 0)
#         sku_mom[sku]["selected_period_coverage_ratio"] = curr_data.get(
#             "selected_period_coverage_ratio",
#             0
#         )

#     # ✅ NEW: keep full all-SKU metrics before any single-SKU filtering
#     all_sku_mom = dict(sku_mom or {})

#     # ✅ NEW: list of every real SKU for individual AI journeys/actions
#     all_individual_skus = [
#         str(sku)
#         for sku in all_sku_mom.keys()
#         if str(sku).strip().lower() not in TOTAL_LABELS
#     ]

#     remaining_agg = build_remaining_skus_aggregate(
#         sku_current=sku_current_for_comparison,
#         sku_prev=sku_prev,
#         focus_skus=top_5_skus
#     )

#     # -------------------------------------------------
#     # Remaining SKUs time series (for LLM journey)
#     # -------------------------------------------------

#     remaining_series = []

#     if analysis_anchor_year and analysis_anchor_month:
#         remaining_series = build_remaining_skus_time_series(
#             user_id=user_id,
#             country=country,
#             focus_skus=top_5_skus,
#             anchor_year=analysis_anchor_year,
#             anchor_month=analysis_anchor_month,
#             months=24
#         )

#     remaining_skus_context = {
#         "aggregated_metrics": remaining_agg,
#         "time_series": remaining_series
#     }

#     if single_sku_mode:
#         sku_mom = {k: sku_mom.get(k, {}) for k in top_5_skus}

#         # ✅ Do not show all SKU section in single SKU mode
#         all_sku_mom = {}
#         all_individual_skus = []

#     # ============================================================
#     # EXCEL-BASED SKU RECOMMENDATIONS
#     # ============================================================
#     excel_sku_recommendations = build_excel_sku_recommendations(
#         sku_mom=sku_mom,
#         objective_v2=objective_v2
#     )

#     # ============================================================
#     # ADS CONTEXT FOR PROMPT-2 ADS RECOMMENDATIONS
#     # ============================================================
#     sku_ads_context = []

#     for sku, metrics in (sku_mom or {}).items():
#         if not isinstance(metrics, dict):
#             continue

#         product_name = metrics.get("product_name") or sku

#         ads_metric = (
#             metrics.get("productwise_ads_spend")
#             or metrics.get("advertising_total")
#             or metrics.get("advertising")
#             or {}
#         )

#         net_sales = metrics.get("net_sales") or {}
#         cm2_profit = metrics.get("cm2_profit") or {}

#         ads_prev = safe_float((ads_metric or {}).get("previous"))
#         ads_curr = safe_float((ads_metric or {}).get("current"))

#         net_sales_prev = safe_float((net_sales or {}).get("previous"))
#         net_sales_curr = safe_float((net_sales or {}).get("current"))

#         cm2_prev = safe_float((cm2_profit or {}).get("previous"))
#         cm2_curr = safe_float((cm2_profit or {}).get("current"))

#         sku_ads_context.append({
#             "sku": sku,
#             "product_name": product_name,

#             "ads_spend_prev": round(ads_prev, 2),
#             "ads_spend_curr": round(ads_curr, 2),
#             "ads_spend_change_pct": (
#                 round(((ads_curr - ads_prev) / abs(ads_prev)) * 100, 2)
#                 if ads_prev
#                 else None
#             ),

#             "net_sales_prev": round(net_sales_prev, 2),
#             "net_sales_curr": round(net_sales_curr, 2),

#             "tacos_prev": (
#                 round((ads_prev / net_sales_prev) * 100, 2)
#                 if net_sales_prev
#                 else 0.0
#             ),
#             "tacos_curr": (
#                 round((ads_curr / net_sales_curr) * 100, 2)
#                 if net_sales_curr
#                 else 0.0
#             ),

#             "cm2_profit_prev": round(cm2_prev, 2),
#             "cm2_profit_curr": round(cm2_curr, 2),
#         })

#     ads_monthly = {
#         "total_ads_spend": round(
#             sum(float(x.get("ads_spend_curr") or 0) for x in sku_ads_context),
#             2,
#         ),
#         "total_cm2_profit": round(
#             sum(float(x.get("cm2_profit_curr") or 0) for x in sku_ads_context),
#             2,
#         ),
#     }   

#     # ============================================================
#     # PROMPT 1 (ANALYSIS)
#     # ============================================================
#     analysis_insights = {}
#     analysis_raw = ""

#     if not single_sku_mode:
#         ai_payload = {
#             "period": f"{period} {timeline} {year}",
#             "period_label": period_label(period, timeline, year),
#             "country": str(country).lower(),
#             "period_absolute_changes": period_absolute_changes,
#             "period_pct_changes": period_pct_changes,
#             "inventory_lost": inventory_lost,
#             "inventory_alerts": inventory_alerts,
#             "sku_mom": sku_mom,
#             "focus_skus": top_5_skus,
#             "movement_context": movement_context,
#             "rolling_extremes": rolling_extremes,
#             "yearly_temporal_signals": yearly_temporal_signals,
#             "scope": scope,
#              # ✅ ADD THIS LINE
#             "portfolio_time_series": rolling_series,
#             "yearly_comparison_months": comparison_months if period == "yearly" else None,
#             "yearly_comparison_basis": (
#                 f"Compared only months {comparison_months} of {year} "
#                 f"vs same months of {year - 1}"
#                 if period == "yearly"
#                 else None
#             ),
#         }

#         analysis_raw = run_prompt_1_analysis(ai_payload)

#         try:
#             analysis_insights = json.loads(analysis_raw)
#         except Exception:
#             print("\n❌ Prompt-1 JSON PARSE FAILED")
#             analysis_insights = {}

#         # print("\n================ AI PAYLOAD ADS DEBUG ================")
#         # print("period_pct_changes sent to LLM:", json.dumps(period_pct_changes, indent=2))
#         # print(
#         #     "period_absolute_changes sent to LLM:",
#         #     json.dumps(period_absolute_changes, indent=2)
#         # )
#         # print("======================================================\n")

#         # analysis_raw = run_prompt_1_analysis(ai_payload)

#         # print("\n================ PROMPT 1 RAW OUTPUT DEBUG ================")
#         # print(analysis_raw)
#         # print("===========================================================\n")

#         # try:
#         #     analysis_insights = json.loads(analysis_raw)

#         #     print("\n================ PARSED ANALYSIS ADS DEBUG ================")
#         #     print(
#         #         "parsed advertising:",
#         #         analysis_insights
#         #         .get("executive_summary_signals", {})
#         #         .get("cost_pressure", {})
#         #         .get("advertising", {})
#         #     )
#         #     print("===========================================================\n")

#         except Exception:
#             print("\n❌ Prompt-1 JSON PARSE FAILED")
#             analysis_insights = {}
# #############################################################################################

    
#     # ============================================================
#     portfolio_level_narrative = analysis_insights.get("executive_summary_signals", {})

#     # ============================================================
#     # PROMPT 2 (ALWAYS CALLED)
#     # ============================================================
#     portfolio_recommendation = ""
#     sku_actions = {}
#     strategy_raw = ""

#     if analysis_insights or single_sku_mode:

#         sku_time_series = {}

#         # ✅ For normal portfolio mode, generate journeys for all SKUs.
#         # ✅ For single SKU mode, keep only selected SKU.
#         prompt_skus = top_5_skus if single_sku_mode else all_individual_skus

#         if analysis_anchor_year and analysis_anchor_month:
#             for sku in prompt_skus:
#                 sku_time_series[sku] = build_rolling_sku_series(
#                     user_id=user_id,
#                     country=country,
#                     sku=sku,
#                     anchor_year=analysis_anchor_year,
#                     anchor_month=analysis_anchor_month
#                 )

#         strategy_raw = run_prompt_2_strategy(
#             analysis_insights=analysis_insights,
#             sku_mom=sku_mom,
#             objective_v2=objective_v2,

#             # ✅ Send all SKUs for individual journey/recommendation generation
#             focus_skus=prompt_skus,

#             sku_time_series=sku_time_series,
#             inventory_alerts=inventory_alerts,
#             country=str(country).lower(),

#             # ✅ NEW
#             sku_inventory_flags=sku_inventory_flags,
#             sku_ads_context=sku_ads_context,
#             ads_monthly=ads_monthly,

#             remaining_skus_context=remaining_skus_context
#         )
       

#         try:
#             parsed = json.loads(strategy_raw)

#             portfolio_recommendation = parsed.get("portfolio_recommendation", "")

#             ai_sku_actions = parsed.get("sku_actions") or {}
#             sku_actions = {}


#             # -------------------------------------------------
#             # Merge AI outputs + Excel + Ads + Inventory recommendations
#             # ✅ Now stores actions for all individual SKUs
#             # -------------------------------------------------
#             action_skus = top_5_skus if single_sku_mode else all_individual_skus

#             for sku in action_skus:
#                 sku_key = str(sku or "").strip()
#                 sku_lookup_key = sku_key.upper()

#                 ai_data = (
#                     ai_sku_actions.get(sku_key)
#                     or ai_sku_actions.get(sku_lookup_key)
#                     or {}
#                 )

#                 inv_flag = sku_inventory_flags.get(sku_lookup_key) or sku_inventory_flags.get(sku_key) or {}

#                 sku_actions[sku_key] = {
#                     "journey_summary": ai_data.get("journey_summary", []),

#                     # ✅ Excel-based deterministic recommendation
#                     "recommendation": (
#                         excel_sku_recommendations.get(sku_key)
#                         or excel_sku_recommendations.get(sku_lookup_key)
#                         or ""
#                     ),

#                     # ✅ AI-generated ads recommendation from Prompt-2
#                     "ads_recommendation": ai_data.get("ads_recommendation", ""),

#                     # ✅ Deterministic inventory recommendation from live-style logic
#                     "inventory_recommendation": (
#                         inv_flag.get("inventory_recommendation")
#                         or ai_data.get("inventory_recommendation", "")
#                     ),

#                     # ✅ Useful for frontend badges/debug
#                     "inventory_alert": inv_flag.get("inventory_alert"),
#                     "inventory_alert_type": inv_flag.get("inventory_alert_type"),
#                     "inventory_coverage_ratio": inv_flag.get("inventory_coverage_ratio"),
#                     "coverage_ratio_months": inv_flag.get("coverage_ratio_months"),
#                     "high_alert_threshold": inv_flag.get("high_alert_threshold"),
#                     "long_term_aged_units": inv_flag.get("long_term_aged_units"),
#                     "estimated_storage_cost": inv_flag.get("estimated_storage_cost"),
#                 }
#             # ✅ Capture consolidated recommendation for remaining SKUs
#             remaining_skus_rec = parsed.get("remaining_skus_recommendation")
#             if isinstance(remaining_skus_rec, str) and remaining_skus_rec.strip():
#                 sku_actions["remaining_skus_recommendation"] = remaining_skus_rec

#             remaining_journey = parsed.get("remaining_skus_journey_summary")
#             if isinstance(remaining_journey, list) and remaining_journey:
#                 sku_actions["remaining_skus_journey_summary"] = remaining_journey

#             remaining_ads_rec = parsed.get("remaining_skus_ads_recommendation")
#             if isinstance(remaining_ads_rec, str) and remaining_ads_rec.strip():
#                 sku_actions["remaining_skus_ads_recommendation"] = remaining_ads_rec

#             remaining_inventory_rec = parsed.get("remaining_skus_inventory_recommendation")
#             if isinstance(remaining_inventory_rec, str) and remaining_inventory_rec.strip():
#                 sku_actions["remaining_skus_inventory_recommendation"] = remaining_inventory_rec

#         except Exception:
#             print("\n❌ Prompt-2 JSON PARSE FAILED")
#             sku_actions = {}



#     # 🔥 SUPPRESS RECOMMENDATIONS WHEN NOT ALLOWED
#     if not allow_recommendations:

#         # Remove portfolio-level recommendation
#         portfolio_recommendation = ""

#         # Remove SKU-level recommendations/actions
#         for key, value in sku_actions.items():
#             if isinstance(value, dict):
#                 value["recommendation"] = ""
#                 value["ads_recommendation"] = ""
#                 value["inventory_recommendation"] = ""

#         # Remove remaining SKUs recommendation text but keep card
#         if "remaining_skus_recommendation" in sku_actions:
#             sku_actions["remaining_skus_recommendation"] = ""

#     # final_text = strategy_raw if strategy_raw else analysis_raw

#     ######################################################################
   
# ##################################################################################
#     final_text = render_month_end_summary(
#     period=period,
#     timeline=timeline,
#     year=year,
#     analysis_insights=analysis_insights,
#     mom=None,
#     sku_mom=sku_mom,
#     focus_skus=top_5_skus,
#     portfolio_recommendation=portfolio_recommendation,
#     inventory_alerts=inventory_alerts if allow_inventory else {},
#     inventory_lost=inventory_lost,
#     currency_symbol="£" if country == "uk" else "$",
#     strategy_actions=sku_actions,
#     remaining_agg=remaining_agg,

#     # ✅ NEW
#     portfolio_inventory_coverage=portfolio_inventory_coverage,

#     all_sku_mom=all_sku_mom,
# )


#     if not single_sku_mode:
#         save_summary_to_db({
#         "user_id": user_id,
#         "country": country,
#         "marketplace_id": marketplace_id,
#         "period": period,
#         "timeline": timeline,
#         "year": year,
#         "summary": final_text,
#         "recommendations": sku_actions or {},
#         "upsert": True
#     })

#     return {
#         "summary": final_text,
#         # "overall_month_summary": overall_month_summary,
#         "portfolio_level_narrative": portfolio_level_narrative,
#         "portfolio_recommendation": portfolio_recommendation,
#         "recommendations": sku_actions if allow_recommendations else {},
#         "inventory_lost": inventory_lost,
#         "inventory_alerts": inventory_alerts if allow_inventory else {},
#         "sku_current": sku_current,
#         "sku_mom": sku_mom,
#         "all_sku_mom": all_sku_mom,
#         "remaining_agg": remaining_agg,
#         "sku_yoy": None,
#         "objective": objective_v2,
#         "sku_actions": sku_actions,
#         "scope": scope,
#         "source": "ai",
#     }

def get_or_create_summary(
    user_id,
    country,
    marketplace_id,
    period,
    timeline,
    year,
    objective=None,
    target_sku: str | list | None = None,
    force_regenerate=False
):

    # ============================================================
    # LOAD OBJECTIVE FROM DB
    # ============================================================
    user_objective_row = UserObjective.query.filter_by(
        user_id=user_id,
        country=country
    ).first()

    if user_objective_row:
        objective_v2 = {
            "growth_intent": user_objective_row.growth_intent,
            "profit_priority": user_objective_row.profit_priority,
            "inventory_clearance_priority": user_objective_row.inventory_clearance_priority,
            "business_context": user_objective_row.business_context,
            "country": str(country).lower(),
            "time_horizon": "1_month"
        }
    else:
        objective_v2 = {
            "growth_intent": "balanced",
            "profit_priority": "protect_growth",
            "inventory_clearance_priority": False,
            "business_context": None,
            "country": str(country).lower(),
            "time_horizon": "1_month"
        }

    # ============================================================
    # PERIOD RESOLUTION
    # ============================================================
    user_selected = bool(period and timeline and year)

    if not user_selected:
        year, month = resolve_latest_available_month(user_id, country)
        timeline = str(month)
        period = "monthly"

    is_latest = is_latest_period(
        period, timeline, year,
        user_id=user_id,
        country=country
    )

    # 🔥 CONTROL FLAGS
    allow_inventory = False
    allow_recommendations = False

    if period in ("monthly", "quarterly"):
        allow_inventory = is_latest
        allow_recommendations = is_latest

    elif period == "yearly":
        allow_inventory = is_latest
        allow_recommendations = is_latest

    # ============================================================
    # CACHE CHECK
    # ============================================================
    cached = fetch_existing_summary(
        user_id, country, marketplace_id, period, timeline, year
    )

    if cached and not force_regenerate and not target_sku:
        return {
            "summary": cached.summary,
            "recommendations": (
                json.loads(cached.recommendations)
                if cached.recommendations else {}
            ),
            "source": "db",
            "scope": "portfolio",
            "objective": objective_v2
        }

    # ============================================================
    # CURRENT DATA
    # ============================================================
    quarterly_compare = None

    df_current = fetch_precalc_table(user_id, country, period, timeline, year)

    df_current = apply_productwise_ads_cm2_from_adsmonthly(
        df_current,
        user_id=user_id,
        country=country,
        period=period,
        timeline=timeline,
        year=year,
    )

    df_current = apply_advertising_total_final_from_ads_table(
        df_current,
        user_id=user_id,
        country=country,
        period=period,
        timeline=timeline,
        year=year,
    )

    df_current_detail, df_current_total = _split_total_row(df_current)

    sku_current = compute_sku_precalc(df_current_detail)

    # ============================================================
    # QUARTERLY INCOMPLETE-PERIOD FIX
    # ============================================================
    # For incomplete current-year quarter, do not use the quarterly table,
    # because it may contain current running month data.
    # Example:
    # If today is July 2026 and Q3 2026 is selected,
    # July is still incomplete, so Q3 should not include July.
    # ============================================================
    if period == "quarterly":
        quarterly_max_month = get_period_month_cap(
            year=year,
            period="quarterly",
            timeline=timeline,
        )

        q = int(str(timeline).replace("Q", ""))
        quarter_start_month = (q - 1) * 3 + 1
        quarter_end_month = q * 3

        if quarterly_max_month < quarter_end_month:
            quarterly_compare = aggregate_monthly_tables_for_quarterly_comparison_generic(
                user_id=user_id,
                year=year,
                timeline=timeline,
                country=country,
                apply_ads_final=True,
                fetch_monthly_func=lambda user_id, timeline, year: fetch_precalc_table(
                    user_id=user_id,
                    country=country,
                    period="monthly",
                    timeline=timeline,
                    year=year,
                ),
            )

            df_current_detail = quarterly_compare["df_current_detail"]
            df_current_total = quarterly_compare["df_current_total"]
            sku_current = quarterly_compare["sku_current"]

    # ============================================================
    # PORTFOLIO INVENTORY COVERAGE
    # Monthly only.
    # ============================================================
    portfolio_inventory_coverage = {}

    if period == "monthly":
        sku_current = enrich_sku_current_with_selected_inventory(
            sku_current,
            user_id=user_id,
            year=year,
            month=int(timeline),
        )

        portfolio_inventory_coverage = build_portfolio_inventory_coverage_summary(
            user_id=user_id,
            country=country,
            year=year,
            month=int(timeline),
            df_current_total=df_current_total,
        )

    top_5_skus = select_focus_skus_by_sales_mix(sku_current)

    # ============================================================
    # SINGLE SKU MODE
    # ============================================================
    single_sku_mode = False
    scope = "portfolio"

    if target_sku:
        single_sku_mode = True
        scope = "sku"

        if isinstance(target_sku, list):
            target_sku = target_sku[0]

        target_sku = str(target_sku).strip()

        if target_sku in sku_current:
            top_5_skus = [target_sku]
            sku_current = {target_sku: sku_current[target_sku]}
        else:
            return {
                "summary": f"I couldn’t find SKU '{target_sku}' in the selected period.",
                "recommendations": {},
                "inventory_lost": 0.0,
                "inventory_alerts": {},
                "sku_current": {},
                "sku_mom": {},
                "sku_yoy": None,
                "objective": objective_v2,
                "sku_actions": {},
                "scope": "sku",
                "source": "no_data",
            }

    # ============================================================
    # ROLLING CONTEXT
    # ============================================================
    movement_context = {}
    rolling_extremes = {}
    yearly_temporal_signals = None
    analysis_anchor_year = None
    analysis_anchor_month = None
    rolling_series = []

    if period == "yearly":
        anchor = resolve_yearly_analysis_anchor(user_id, country, year)
        if anchor:
            analysis_anchor_year, analysis_anchor_month = anchor
    else:
        analysis_anchor_year = year

        if period == "monthly":
            analysis_anchor_month = int(timeline)

        elif period == "quarterly":
            analysis_anchor_month = get_period_month_cap(
                year=year,
                period="quarterly",
                timeline=timeline,
            )

            if analysis_anchor_month <= 0:
                analysis_anchor_month = None

    if analysis_anchor_year and analysis_anchor_month:
        rolling_series = build_rolling_monthly_series(
            user_id=user_id,
            country=country,
            anchor_year=analysis_anchor_year,
            anchor_month=analysis_anchor_month
        )

        movement_context = build_movement_context(rolling_series)

        rolling_extremes = extract_rolling_extremes(rolling_series)

        if period == "yearly":
            yearly_temporal_signals = build_yearly_temporal_signals(rolling_series) or None

    # ============================================================
    # INVENTORY
    # ============================================================
    lost_total_val = _total_value(df_current_total, "lost_total")
    inventory_lost = round(abs(lost_total_val), 2) if lost_total_val is not None else 0.0

    if single_sku_mode:
        inventory_lost = 0.0

    inventory_alerts = {}
    sku_inventory_flags = {}

    if allow_inventory:
        inventory_aged_df = fetch_inventory_aged_by_user(user_id, country=country)

        if not inventory_aged_df.empty:
            inventory_alerts = build_inventory_alerts(
                inventory_aged_df,
                user_id=user_id,
                country=country
            )

        coverage_override_by_sku = {}

        for sku, row in (sku_current or {}).items():
            if not isinstance(row, dict):
                continue

            sku_key = str(sku or "").strip().upper()
            if not sku_key:
                continue

            cov = row.get("selected_period_coverage_ratio")

            if cov is None:
                cov = row.get("inventory_coverage_ratio")

            if cov is None:
                cov = row.get("coverage_ratio_months")

            if cov is not None:
                coverage_override_by_sku[sku_key] = cov

        sku_inventory_flags = generate_sku_inventory_flags(
            user_id=user_id,
            country=country,
            focus_skus=top_5_skus if single_sku_mode else None,
            coverage_override_by_sku=coverage_override_by_sku,
        )

    # ============================================================
    # PREVIOUS PERIOD / YEARLY + QUARTERLY PARTIAL COMPARISON
    # ============================================================
    period_absolute_changes = {}
    period_pct_changes = None
    comparison_months = []

    if period == "quarterly" and quarterly_compare is not None:
        comparison_months = quarterly_compare["available_months"]

        df_current_total_for_comparison = quarterly_compare["df_current_total"]
        df_prev_total_for_comparison = quarterly_compare["df_prev_total"]

        sku_current_for_comparison = quarterly_compare["sku_current"]
        sku_prev = quarterly_compare["sku_prev"]

    elif period == "yearly":
        yearly_compare = aggregate_monthly_tables_for_yearly_comparison_generic(
            user_id=user_id,
            year=year,
            country=country,
            apply_ads_final=True,
            max_month=get_period_month_cap(
                year=year,
                period="yearly",
                timeline="ALL",
            ),
            fetch_monthly_func=lambda user_id, timeline, year: fetch_precalc_table(
                user_id=user_id,
                country=country,
                period="monthly",
                timeline=timeline,
                year=year,
            ),
        )

        comparison_months = yearly_compare["available_months"]

        df_current_total_for_comparison = yearly_compare["df_current_total"]
        df_prev_total_for_comparison = yearly_compare["df_prev_total"]

        sku_current_for_comparison = yearly_compare["sku_current"]
        sku_prev = yearly_compare["sku_prev"]

    else:
        (p_period, p_timeline, p_year), _ = resolve_comparison(period, timeline, year)

        df_prev = fetch_precalc_table(user_id, country, p_period, p_timeline, p_year)

        df_prev = apply_productwise_ads_cm2_from_adsmonthly(
            df_prev,
            user_id=user_id,
            country=country,
            period=p_period,
            timeline=p_timeline,
            year=p_year,
        )

        df_prev = apply_advertising_total_final_from_ads_table(
            df_prev,
            user_id=user_id,
            country=country,
            period=p_period,
            timeline=p_timeline,
            year=p_year,
        )

        df_prev_detail, df_prev_total = _split_total_row(df_prev)

        df_current_total_for_comparison = df_current_total
        df_prev_total_for_comparison = df_prev_total

        sku_current_for_comparison = sku_current
        sku_prev = compute_sku_precalc(df_prev_detail)

    if (
        not df_current_total_for_comparison.empty
        and not df_prev_total_for_comparison.empty
    ):
        period_absolute_changes = compute_period_absolute_changes(
            df_current_total_for_comparison,
            df_prev_total_for_comparison,
        )

        period_pct_changes = compute_period_pct_changes(
            df_current_total_for_comparison,
            df_prev_total_for_comparison,
        )

        current_values_source = (
            df_current_total
            if period == "yearly"
            else df_current_total_for_comparison
        )

        previous_values_source = df_prev_total_for_comparison

        period_absolute_changes["current_values"] = {
            "units": _total_value(current_values_source, "total_quantity"),
            "net_sales": _total_value(current_values_source, "net_sales"),
            "asp": _total_value(current_values_source, "asp"),
            "cm1_profit": _total_value(current_values_source, "profit"),
            "cm1_profit_per_unit": _total_value(
                current_values_source,
                "unit_wise_profitability"
            ),
            "cm2_profit": _total_cm2_value(current_values_source),
            "advertising": _advertising_total_value(current_values_source),
            "storage_fees": _total_value(
                current_values_source,
                "platform_fee_inventory_storage"
            ),
            "acos": _total_value(current_values_source, "acos"),
        }

        period_absolute_changes["previous_values"] = {
            "units": _total_value(previous_values_source, "total_quantity"),
            "net_sales": _total_value(previous_values_source, "net_sales"),
            "asp": _total_value(previous_values_source, "asp"),
            "cm1_profit": _total_value(previous_values_source, "profit"),
            "cm1_profit_per_unit": _total_value(
                previous_values_source,
                "unit_wise_profitability"
            ),
            "cm2_profit": _total_cm2_value(previous_values_source),
            "advertising": _advertising_total_value(previous_values_source),
            "storage_fees": _total_value(
                previous_values_source,
                "platform_fee_inventory_storage"
            ),
            "acos": _total_value(previous_values_source, "acos"),
        }

        # print("\n================ OVERALL SUMMARY AI TOTAL VALUES DEBUG ================")
        # print("user_id:", user_id)
        # print("country:", country)
        # print("period:", period)
        # print("timeline:", timeline)
        # print("year:", year)

        # print("\nCURRENT VALUES SENT TO AI:")
        # print(json.dumps(period_absolute_changes.get("current_values", {}), indent=2, default=str))

        # print("\nPREVIOUS VALUES SENT TO AI:")
        # print(json.dumps(period_absolute_changes.get("previous_values", {}), indent=2, default=str))

        # print("\nABSOLUTE CHANGES SENT TO AI:")
        # print(json.dumps({
        #     k: v for k, v in period_absolute_changes.items()
        #     if k not in ("current_values", "previous_values")
        # }, indent=2, default=str))

        # print("\nPERCENTAGE CHANGES SENT TO AI:")
        # print(json.dumps(period_pct_changes or {}, indent=2, default=str))

        # print("=======================================================================\n")

    # ============================================================
    # YEARLY ADVERTISING OVERRIDE SAFETY FIX
    # ============================================================
    if (
        period == "yearly"
        and period_pct_changes is not None
        and "current_values_source" in locals()
        and "previous_values_source" in locals()
    ):
        current_ads = _advertising_total_value(current_values_source)
        previous_ads = _advertising_total_value(previous_values_source)

        if current_ads is not None and previous_ads is not None:
            period_absolute_changes["advertising"] = round(
                current_ads - previous_ads,
                2
            )

        if current_ads is not None and previous_ads not in (None, 0):
            period_pct_changes["advertising"] = round(
                (current_ads - previous_ads) / abs(previous_ads) * 100,
                2
            )

    sku_mom = compare_sku_metrics(
        sku_current_for_comparison,
        sku_prev,
    )

    # ============================================================
    # ADD CURRENT-ONLY INVENTORY INTO SKU MOM
    # ============================================================
    for sku, curr_data in (sku_current_for_comparison or {}).items():
        if sku not in sku_mom:
            continue

        if not isinstance(curr_data, dict):
            continue

        sku_mom[sku]["current_inventory"] = curr_data.get("current_inventory", 0)
        sku_mom[sku]["selected_period_coverage_ratio"] = curr_data.get(
            "selected_period_coverage_ratio",
            0
        )

    all_sku_mom = dict(sku_mom or {})

    all_individual_skus = [
        str(sku)
        for sku in all_sku_mom.keys()
        if str(sku).strip().lower() not in TOTAL_LABELS
    ]

    remaining_agg = build_remaining_skus_aggregate(
        sku_current=sku_current_for_comparison,
        sku_prev=sku_prev,
        focus_skus=top_5_skus
    )

    # ============================================================
    # REMAINING SKUS TIME SERIES
    # ============================================================
    remaining_series = []

    if analysis_anchor_year and analysis_anchor_month:
        remaining_series = build_remaining_skus_time_series(
            user_id=user_id,
            country=country,
            focus_skus=top_5_skus,
            anchor_year=analysis_anchor_year,
            anchor_month=analysis_anchor_month,
            months=24
        )

    remaining_skus_context = {
        "aggregated_metrics": remaining_agg,
        "time_series": remaining_series
    }

    if single_sku_mode:
        sku_mom = {k: sku_mom.get(k, {}) for k in top_5_skus}
        all_sku_mom = {}
        all_individual_skus = []

    # ============================================================
    # EXCEL-BASED SKU RECOMMENDATIONS
    # ============================================================
    excel_sku_recommendations = build_excel_sku_recommendations(
        sku_mom=sku_mom,
        objective_v2=objective_v2
    )

    # ============================================================
    # ADS CONTEXT FOR PROMPT-2 ADS RECOMMENDATIONS
    # ============================================================
    sku_ads_context = []

    for sku, metrics in (sku_mom or {}).items():
        if not isinstance(metrics, dict):
            continue

        product_name = metrics.get("product_name") or sku

        ads_metric = (
            metrics.get("productwise_ads_spend")
            or metrics.get("advertising_total")
            or metrics.get("advertising")
            or {}
        )

        net_sales = metrics.get("net_sales") or {}
        cm2_profit = metrics.get("cm2_profit") or {}

        ads_prev = safe_float((ads_metric or {}).get("previous"))
        ads_curr = safe_float((ads_metric or {}).get("current"))

        net_sales_prev = safe_float((net_sales or {}).get("previous"))
        net_sales_curr = safe_float((net_sales or {}).get("current"))

        cm2_prev = safe_float((cm2_profit or {}).get("previous"))
        cm2_curr = safe_float((cm2_profit or {}).get("current"))

        sku_ads_context.append({
            "sku": sku,
            "product_name": product_name,

            "ads_spend_prev": round(ads_prev, 2),
            "ads_spend_curr": round(ads_curr, 2),
            "ads_spend_change_pct": (
                round(((ads_curr - ads_prev) / abs(ads_prev)) * 100, 2)
                if ads_prev
                else None
            ),

            "net_sales_prev": round(net_sales_prev, 2),
            "net_sales_curr": round(net_sales_curr, 2),

            "tacos_prev": (
                round((ads_prev / net_sales_prev) * 100, 2)
                if net_sales_prev
                else 0.0
            ),
            "tacos_curr": (
                round((ads_curr / net_sales_curr) * 100, 2)
                if net_sales_curr
                else 0.0
            ),

            "cm2_profit_prev": round(cm2_prev, 2),
            "cm2_profit_curr": round(cm2_curr, 2),
        })

    ads_monthly = {
        "total_ads_spend": round(
            sum(float(x.get("ads_spend_curr") or 0) for x in sku_ads_context),
            2,
        ),
        "total_cm2_profit": round(
            sum(float(x.get("cm2_profit_curr") or 0) for x in sku_ads_context),
            2,
        ),
    }

    # ============================================================
    # PROMPT 1 ANALYSIS
    # ============================================================
    analysis_insights = {}
    analysis_raw = ""

    if not single_sku_mode:
        ai_payload = {
            "period": f"{period} {timeline} {year}",
            "period_label": period_label(period, timeline, year),
            "country": str(country).lower(),
            "period_absolute_changes": period_absolute_changes,
            "period_pct_changes": period_pct_changes,
            "inventory_lost": inventory_lost,
            "inventory_alerts": inventory_alerts,
            "sku_mom": sku_mom,
            "focus_skus": top_5_skus,
            "movement_context": movement_context,
            "rolling_extremes": rolling_extremes,
            "yearly_temporal_signals": yearly_temporal_signals,
            "scope": scope,
            "portfolio_time_series": rolling_series,

            # yearly only
            "yearly_comparison_months": comparison_months if period == "yearly" else None,
            "yearly_comparison_basis": (
                f"Compared only months {comparison_months} of {year} "
                f"vs same months of {year - 1}"
                if period == "yearly"
                else None
            ),

            # quarterly partial only
            "quarterly_comparison_months": comparison_months if period == "quarterly" else None,
            "quarterly_comparison_basis": (
                f"Compared only completed months {comparison_months} for {timeline} {year}"
                if period == "quarterly" and quarterly_compare is not None
                else None
            ),
        }

        analysis_raw = run_prompt_1_analysis(ai_payload)

        try:
            analysis_insights = json.loads(analysis_raw)
        except Exception:
            print("\n❌ Prompt-1 JSON PARSE FAILED")
            analysis_insights = {}

    # ============================================================
    # PORTFOLIO NARRATIVE
    # ============================================================
    portfolio_level_narrative = analysis_insights.get("executive_summary_signals", {})

    # ============================================================
    # PROMPT 2 STRATEGY
    # ============================================================
    portfolio_recommendation = ""
    sku_actions = {}
    strategy_raw = ""

    if analysis_insights or single_sku_mode:
        sku_time_series = {}

        prompt_skus = top_5_skus if single_sku_mode else all_individual_skus

        if analysis_anchor_year and analysis_anchor_month:
            for sku in prompt_skus:
                sku_time_series[sku] = build_rolling_sku_series(
                    user_id=user_id,
                    country=country,
                    sku=sku,
                    anchor_year=analysis_anchor_year,
                    anchor_month=analysis_anchor_month
                )

        strategy_raw = run_prompt_2_strategy(
            analysis_insights=analysis_insights,
            sku_mom=sku_mom,
            objective_v2=objective_v2,
            focus_skus=prompt_skus,
            sku_time_series=sku_time_series,
            inventory_alerts=inventory_alerts,
            country=str(country).lower(),
            sku_inventory_flags=sku_inventory_flags,
            sku_ads_context=sku_ads_context,
            ads_monthly=ads_monthly,
            remaining_skus_context=remaining_skus_context
        )

        try:
            parsed = json.loads(strategy_raw)

            portfolio_recommendation = parsed.get("portfolio_recommendation", "")

            ai_sku_actions = parsed.get("sku_actions") or {}
            sku_actions = {}

            action_skus = top_5_skus if single_sku_mode else all_individual_skus

            for sku in action_skus:
                sku_key = str(sku or "").strip()
                sku_lookup_key = sku_key.upper()

                ai_data = (
                    ai_sku_actions.get(sku_key)
                    or ai_sku_actions.get(sku_lookup_key)
                    or {}
                )

                inv_flag = (
                    sku_inventory_flags.get(sku_lookup_key)
                    or sku_inventory_flags.get(sku_key)
                    or {}
                )

                sku_actions[sku_key] = {
                    "journey_summary": ai_data.get("journey_summary", []),

                    "recommendation": (
                        excel_sku_recommendations.get(sku_key)
                        or excel_sku_recommendations.get(sku_lookup_key)
                        or ""
                    ),

                    "ads_recommendation": ai_data.get("ads_recommendation", ""),

                    "inventory_recommendation": (
                        inv_flag.get("inventory_recommendation")
                        or ai_data.get("inventory_recommendation", "")
                    ),

                    "inventory_alert": inv_flag.get("inventory_alert"),
                    "inventory_alert_type": inv_flag.get("inventory_alert_type"),
                    "inventory_coverage_ratio": inv_flag.get("inventory_coverage_ratio"),
                    "coverage_ratio_months": inv_flag.get("coverage_ratio_months"),
                    "high_alert_threshold": inv_flag.get("high_alert_threshold"),
                    "long_term_aged_units": inv_flag.get("long_term_aged_units"),
                    "estimated_storage_cost": inv_flag.get("estimated_storage_cost"),
                }

            remaining_skus_rec = parsed.get("remaining_skus_recommendation")
            if isinstance(remaining_skus_rec, str) and remaining_skus_rec.strip():
                sku_actions["remaining_skus_recommendation"] = remaining_skus_rec

            remaining_journey = parsed.get("remaining_skus_journey_summary")
            if isinstance(remaining_journey, list) and remaining_journey:
                sku_actions["remaining_skus_journey_summary"] = remaining_journey

            remaining_ads_rec = parsed.get("remaining_skus_ads_recommendation")
            if isinstance(remaining_ads_rec, str) and remaining_ads_rec.strip():
                sku_actions["remaining_skus_ads_recommendation"] = remaining_ads_rec

            remaining_inventory_rec = parsed.get("remaining_skus_inventory_recommendation")
            if isinstance(remaining_inventory_rec, str) and remaining_inventory_rec.strip():
                sku_actions["remaining_skus_inventory_recommendation"] = remaining_inventory_rec

        except Exception:
            print("\n❌ Prompt-2 JSON PARSE FAILED")
            sku_actions = {}

    # ============================================================
    # SUPPRESS RECOMMENDATIONS WHEN NOT ALLOWED
    # ============================================================
    if not allow_recommendations:
        portfolio_recommendation = ""

        for key, value in sku_actions.items():
            if isinstance(value, dict):
                value["recommendation"] = ""
                value["ads_recommendation"] = ""
                value["inventory_recommendation"] = ""

        if "remaining_skus_recommendation" in sku_actions:
            sku_actions["remaining_skus_recommendation"] = ""

    # ============================================================
    # FINAL RENDER
    # ============================================================
    final_text = render_month_end_summary(
        period=period,
        timeline=timeline,
        year=year,
        analysis_insights=analysis_insights,
        mom=None,
        sku_mom=sku_mom,
        focus_skus=top_5_skus,
        portfolio_recommendation=portfolio_recommendation,
        inventory_alerts=inventory_alerts if allow_inventory else {},
        inventory_lost=inventory_lost,
        currency_symbol="£" if country == "uk" else "$",
        strategy_actions=sku_actions,
        remaining_agg=remaining_agg,
        portfolio_inventory_coverage=portfolio_inventory_coverage,
        all_sku_mom=all_sku_mom,
    )

    if not single_sku_mode:
        save_summary_to_db({
            "user_id": user_id,
            "country": country,
            "marketplace_id": marketplace_id,
            "period": period,
            "timeline": timeline,
            "year": year,
            "summary": final_text,
            "recommendations": sku_actions or {},
            "upsert": True
        })

    return {
        "summary": final_text,
        "portfolio_level_narrative": portfolio_level_narrative,
        "portfolio_recommendation": portfolio_recommendation,
        "recommendations": sku_actions if allow_recommendations else {},
        "inventory_lost": inventory_lost,
        "inventory_alerts": inventory_alerts if allow_inventory else {},
        "sku_current": sku_current,
        "sku_mom": sku_mom,
        "all_sku_mom": all_sku_mom,
        "remaining_agg": remaining_agg,
        "sku_yoy": None,
        "objective": objective_v2,
        "sku_actions": sku_actions,
        "scope": scope,
        "source": "ai",
    }


def _global_safe_result(result: dict | None) -> dict:
    """
    Makes global rendering safe even if one country has no data/error-shaped result.
    """
    if not isinstance(result, dict):
        return {}

    return result



def _extract_actions(result: dict) -> dict:
    """
    DB result has recommendations.
    AI regenerated result has sku_actions.
    This supports both.
    """
    return (
        result.get("sku_actions")
        or result.get("recommendations")
        or {}
    )

def _extract_summary_intro(result: dict) -> str:
    """
    Extracts only the main country performance paragraph,
    not product insights or inventory sections.
    """
    summary = result.get("summary") or ""

    if "Performance Summary" not in summary:
        return summary[:1200]

    lines = summary.splitlines()
    output = []
    capture = False

    for line in lines:
        clean = line.strip()

        if clean.startswith("Performance Summary"):
            capture = True
            continue

        if capture:
            if clean.startswith("• ") or clean.startswith("## PRODUCT INSIGHTS"):
                break
            if clean:
                output.append(clean)

    return " ".join(output).strip()

def fetch_global_sku_mapping(user_id: int) -> list[dict]:
    """
    Fetches exact UK-US SKU mapping from sku_{user_id}_data_table.
    This is used only for global product-wise journey comparison.
    """

    table_name = f"sku_{user_id}_data_table"

    query = f'''
        SELECT
            product_name,
            sku_uk,
            sku_us,
            asin,
            product_barcode
        FROM public."{table_name}"
        WHERE user_id = :user_id
    '''

    try:
        with phormula_engine.connect() as conn:
            df = pd.read_sql(text(query), conn, params={"user_id": user_id})
    except Exception as e:
        print(f"❌ Failed to fetch global SKU mapping: {e}")
        return []

    if df.empty:
        return []

    df = df.fillna("")

    mappings = []

    for _, row in df.iterrows():
        product_name = str(row.get("product_name", "")).strip()
        sku_uk = str(row.get("sku_uk", "")).strip()
        sku_us = str(row.get("sku_us", "")).strip()

        if not product_name or not sku_uk or not sku_us:
            continue

        mappings.append({
            "product_name": product_name,
            "sku_uk": sku_uk,
            "sku_us": sku_us,
            "asin": str(row.get("asin", "")).strip(),
            "product_barcode": str(row.get("product_barcode", "")).strip(),
        })

    return mappings


def build_mapped_product_journeys(
    *,
    sku_mapping: list[dict],
    us_result: dict,
    uk_result: dict
) -> list[dict]:
    """
    Builds exact product-wise US vs UK journey input using sku_us and sku_uk mapping.
    The journey_summary values are already generated by each country's get_or_create_summary().
    """

    us_actions = _extract_actions(us_result)
    uk_actions = _extract_actions(uk_result)

    us_sku_mom = us_result.get("sku_mom", {}) or {}
    uk_sku_mom = uk_result.get("sku_mom", {}) or {}

    mapped_products = []

    for item in sku_mapping:
        product_name = item.get("product_name")
        sku_us = item.get("sku_us")
        sku_uk = item.get("sku_uk")

        us_action = us_actions.get(sku_us, {})
        uk_action = uk_actions.get(sku_uk, {})

        us_metrics = us_sku_mom.get(sku_us, {})
        uk_metrics = uk_sku_mom.get(sku_uk, {})

        if not isinstance(us_action, dict):
            us_action = {}

        if not isinstance(uk_action, dict):
            uk_action = {}

        # Skip products where neither side has journey data
        us_journey = us_action.get("journey_summary", [])
        uk_journey = uk_action.get("journey_summary", [])

        if not us_journey and not uk_journey:
            continue

        mapped_products.append({
            "product_name": product_name,
            "sku_us": sku_us,
            "sku_uk": sku_uk,

            "us": {
                "journey_summary": us_journey if isinstance(us_journey, list) else [],
                "metrics": us_metrics,
            },

            "uk": {
                "journey_summary": uk_journey if isinstance(uk_journey, list) else [],
                "metrics": uk_metrics,
            },

            # ✅ NEW: keep product actions unified inside mapped product
            "unified_country_actions": {
                "us": {
                    "recommendation": us_action.get("recommendation", ""),
                    "inventory_recommendation": us_action.get("inventory_recommendation", ""),
                    "ads_recommendation": us_action.get("ads_recommendation", ""),
                },
                "uk": {
                    "recommendation": uk_action.get("recommendation", ""),
                    "inventory_recommendation": uk_action.get("inventory_recommendation", ""),
                    "ads_recommendation": uk_action.get("ads_recommendation", ""),
                },
            },
        })

    return mapped_products



# def build_global_numeric_metrics(
#     *,
#     user_id: int,
#     period: str,
#     timeline: str,
#     year: int
# ) -> dict:
#     """
#     Builds selected-period and previous-period GLOBAL metrics
#     from the actual global table.

#     For yearly:
#     - Do NOT compare full yearly table vs previous full yearly table.
#     - Compare available monthly tables in selected year vs same months previous year.
#     """

#     df_current = fetch_global_precalc_table(
#         user_id=user_id,
#         period=period,
#         timeline=timeline,
#         year=year,
#     )

#     df_current_detail, df_current_total = _split_total_row(df_current)

#     (p_period, p_timeline, p_year), _ = resolve_comparison(
#         period,
#         timeline,
#         year,
#     )

#     df_prev = fetch_global_precalc_table(
#         user_id=user_id,
#         period=p_period,
#         timeline=p_timeline,
#         year=p_year,
#     )

#     df_prev_detail, df_prev_total = _split_total_row(df_prev)

#     if df_current.empty:
#         return {
#             "available": False,
#             "source": "global_table",
#             "reason": "No selected-period global table found",
#             "selected_period": {
#                 "period": period,
#                 "timeline": timeline,
#                 "year": year,
#                 "period_label": period_label(period, timeline, year),
#             },
#             "previous_period": {
#                 "period": p_period,
#                 "timeline": p_timeline,
#                 "year": p_year,
#                 "period_label": period_label(p_period, p_timeline, p_year),
#             },
#             "portfolio": {},
#             "sku_current": {},
#             "sku_mom": {},
#             "focus_skus": [],
#             "remaining_agg": {},
#             "products": {},
#             "comparison_months": [],
#         }

#     # ============================================================
#     # YEARLY PARTIAL-YEAR COMPARISON OVERRIDE
#     # ============================================================
#     comparison_months = []

#     if period == "yearly":
#         yearly_compare = aggregate_monthly_tables_for_yearly_comparison_generic(
#             user_id=user_id,
#             year=year,
#             fetch_monthly_func=lambda user_id, timeline, year: fetch_global_precalc_table(
#                 user_id=user_id,
#                 period="monthly",
#                 timeline=timeline,
#                 year=year,
#             ),
#         )

#         comparison_months = yearly_compare["available_months"]

#         df_current_total_for_comparison = yearly_compare["df_current_total"]
#         df_prev_total_for_comparison = yearly_compare["df_prev_total"]

#         global_sku_current_for_comparison = yearly_compare["sku_current"]
#         global_sku_prev = yearly_compare["sku_prev"]

#     else:
#         df_current_total_for_comparison = df_current_total
#         df_prev_total_for_comparison = df_prev_total

#         global_sku_current_for_comparison = compute_sku_precalc(df_current_detail)
#         global_sku_prev = compute_sku_precalc(df_prev_detail)

#     # ============================================================
#     # CURRENT / PREVIOUS VALUES
#     # For yearly, these now come from monthly partial-year aggregation.
#     # ============================================================
#     current_values = {
#         "units": _total_value(df_current_total_for_comparison, "total_quantity"),
#         "net_sales": _total_value(df_current_total_for_comparison, "net_sales"),
#         "asp": _total_value(df_current_total_for_comparison, "asp"),
#         "cm1_profit": _total_value(df_current_total_for_comparison, "profit"),
#         "cm1_profit_per_unit": _total_value(df_current_total_for_comparison, "unit_wise_profitability"),
#         "cm2_profit": _total_value(df_current_total_for_comparison, "cm2_profit"),
#         "advertising": _advertising_total_final_value(df_current_total_for_comparison),
#         "storage_fees": _total_value(df_current_total_for_comparison, "platform_fee_inventory_storage"),
#         "acos": _total_value(df_current_total_for_comparison, "acos"),
#     }

#     previous_values = {
#         "units": _total_value(df_prev_total_for_comparison, "total_quantity"),
#         "net_sales": _total_value(df_prev_total_for_comparison, "net_sales"),
#         "asp": _total_value(df_prev_total_for_comparison, "asp"),
#         "cm1_profit": _total_value(df_prev_total_for_comparison, "profit"),
#         "cm1_profit_per_unit": _total_value(df_prev_total_for_comparison, "unit_wise_profitability"),
#         "cm2_profit": _total_value(df_prev_total_for_comparison, "cm2_profit"),
#         "advertising": _advertising_total_final_value(df_prev_total_for_comparison),
#         "storage_fees": _total_value(df_prev_total_for_comparison, "platform_fee_inventory_storage"),
#         "acos": _total_value(df_prev_total_for_comparison, "acos"),
#     }

#     absolute_changes = {}
#     pct_changes = {}

#     if (
#         not df_current_total_for_comparison.empty
#         and not df_prev_total_for_comparison.empty
#     ):
#         absolute_changes = compute_period_absolute_changes(
#             df_current_total_for_comparison,
#             df_prev_total_for_comparison,
#         )

#         pct_changes = compute_period_pct_changes(
#             df_current_total_for_comparison,
#             df_prev_total_for_comparison,
#         )

#     # ============================================================
#     # SKU METRICS
#     # For yearly, sku_mom is actually partial-year YoY:
#     # available months current year vs same months previous year.
#     # ============================================================
#     global_sku_current = compute_sku_precalc(df_current_detail)

#     global_sku_mom = compare_sku_metrics(
#         global_sku_current_for_comparison,
#         global_sku_prev,
#     )

#     # Keep focus SKUs based on selected-period global table
#     global_focus_skus = select_focus_skus_by_sales_mix(global_sku_current)

#     global_remaining_agg = build_remaining_skus_aggregate(
#         sku_current=global_sku_current_for_comparison,
#         sku_prev=global_sku_prev,
#         focus_skus=global_focus_skus,
#     )

#     return {
#         "available": True,
#         "source": "global_table",
#         "selected_period": {
#             "period": period,
#             "timeline": timeline,
#             "year": year,
#             "period_label": period_label(period, timeline, year),
#         },
#         "previous_period": {
#             "period": p_period,
#             "timeline": p_timeline,
#             "year": p_year,
#             "period_label": period_label(p_period, p_timeline, p_year),
#         },
#         "comparison_months": comparison_months,
#         "comparison_basis": (
#             f"Compared months {comparison_months} of {year} "
#             f"vs same months of {year - 1}"
#             if period == "yearly"
#             else None
#         ),
#         "portfolio": {
#             "current_values": current_values,
#             "previous_values": previous_values,
#             "absolute_changes": absolute_changes,
#             "pct_changes": pct_changes,
#         },

#         # Frontend metrics from selected-period global table
#         "sku_current": global_sku_current,

#         # Comparison metrics
#         "sku_mom": global_sku_mom,

#         "focus_skus": global_focus_skus,
#         "remaining_agg": global_remaining_agg,

#         # Optional backwards compatibility
#         "products": global_sku_mom,
#     }

def build_global_numeric_metrics(
    *,
    user_id: int,
    period: str,
    timeline: str,
    year: int
) -> dict:
    """
    Builds selected-period and previous-period GLOBAL metrics
    from the actual global table.

    Yearly:
    - Uses monthly aggregation capped to latest completed month.

    Quarterly:
    - If selected quarter is incomplete, uses completed monthly tables only.
    """

    df_current = fetch_global_precalc_table(
        user_id=user_id,
        period=period,
        timeline=timeline,
        year=year,
    )

    df_current_detail, df_current_total = _split_total_row(df_current)

    (p_period, p_timeline, p_year), _ = resolve_comparison(
        period,
        timeline,
        year,
    )

    df_prev = fetch_global_precalc_table(
        user_id=user_id,
        period=p_period,
        timeline=p_timeline,
        year=p_year,
    )

    df_prev_detail, df_prev_total = _split_total_row(df_prev)

    # ============================================================
    # QUARTERLY INCOMPLETE-PERIOD OVERRIDE
    # ============================================================
    quarterly_compare = None

    if period == "quarterly":
        quarterly_max_month = get_period_month_cap(
            year=year,
            period="quarterly",
            timeline=timeline,
        )

        q = int(str(timeline).replace("Q", ""))
        quarter_end_month = q * 3

        if quarterly_max_month < quarter_end_month:
            quarterly_compare = aggregate_monthly_tables_for_quarterly_comparison_generic(
                user_id=user_id,
                year=year,
                timeline=timeline,
                fetch_monthly_func=lambda user_id, timeline, year: fetch_global_precalc_table(
                    user_id=user_id,
                    period="monthly",
                    timeline=timeline,
                    year=year,
                ),
            )

            df_current_detail = quarterly_compare["df_current_detail"]
            df_current_total = quarterly_compare["df_current_total"]

    if df_current.empty and quarterly_compare is None:
        return {
            "available": False,
            "source": "global_table",
            "reason": "No selected-period global table found",
            "selected_period": {
                "period": period,
                "timeline": timeline,
                "year": year,
                "period_label": period_label(period, timeline, year),
            },
            "previous_period": {
                "period": p_period,
                "timeline": p_timeline,
                "year": p_year,
                "period_label": period_label(p_period, p_timeline, p_year),
            },
            "portfolio": {},
            "sku_current": {},
            "sku_mom": {},
            "focus_skus": [],
            "remaining_agg": {},
            "products": {},
            "comparison_months": [],
        }

    comparison_months = []

    if period == "quarterly" and quarterly_compare is not None:
        comparison_months = quarterly_compare["available_months"]

        df_current_total_for_comparison = quarterly_compare["df_current_total"]
        df_prev_total_for_comparison = quarterly_compare["df_prev_total"]

        global_sku_current_for_comparison = quarterly_compare["sku_current"]
        global_sku_prev = quarterly_compare["sku_prev"]

    elif period == "yearly":
        yearly_compare = aggregate_monthly_tables_for_yearly_comparison_generic(
            user_id=user_id,
            year=year,
            max_month=get_period_month_cap(
                year=year,
                period="yearly",
                timeline="ALL",
            ),
            fetch_monthly_func=lambda user_id, timeline, year: fetch_global_precalc_table(
                user_id=user_id,
                period="monthly",
                timeline=timeline,
                year=year,
            ),
        )

        comparison_months = yearly_compare["available_months"]

        df_current_total_for_comparison = yearly_compare["df_current_total"]
        df_prev_total_for_comparison = yearly_compare["df_prev_total"]

        global_sku_current_for_comparison = yearly_compare["sku_current"]
        global_sku_prev = yearly_compare["sku_prev"]

    else:
        df_current_total_for_comparison = df_current_total
        df_prev_total_for_comparison = df_prev_total

        global_sku_current_for_comparison = compute_sku_precalc(df_current_detail)
        global_sku_prev = compute_sku_precalc(df_prev_detail)

    current_values = {
        "units": _total_value(df_current_total_for_comparison, "total_quantity"),
        "net_sales": _total_value(df_current_total_for_comparison, "net_sales"),
        "asp": _total_value(df_current_total_for_comparison, "asp"),
        "cm1_profit": _total_value(df_current_total_for_comparison, "profit"),
        "cm1_profit_per_unit": _total_value(df_current_total_for_comparison, "unit_wise_profitability"),
        "cm2_profit": _total_value(df_current_total_for_comparison, "cm2_profit"),
        "advertising": _advertising_total_value(df_current_total_for_comparison),
        "storage_fees": _total_value(df_current_total_for_comparison, "platform_fee_inventory_storage"),
        "acos": _total_value(df_current_total_for_comparison, "acos"),
    }

    previous_values = {
        "units": _total_value(df_prev_total_for_comparison, "total_quantity"),
        "net_sales": _total_value(df_prev_total_for_comparison, "net_sales"),
        "asp": _total_value(df_prev_total_for_comparison, "asp"),
        "cm1_profit": _total_value(df_prev_total_for_comparison, "profit"),
        "cm1_profit_per_unit": _total_value(df_prev_total_for_comparison, "unit_wise_profitability"),
        "cm2_profit": _total_value(df_prev_total_for_comparison, "cm2_profit"),
        "advertising": _advertising_total_value(df_prev_total_for_comparison),
        "storage_fees": _total_value(df_prev_total_for_comparison, "platform_fee_inventory_storage"),
        "acos": _total_value(df_prev_total_for_comparison, "acos"),
    }

    absolute_changes = {}
    pct_changes = {}

    if (
        not df_current_total_for_comparison.empty
        and not df_prev_total_for_comparison.empty
    ):
        absolute_changes = compute_period_absolute_changes(
            df_current_total_for_comparison,
            df_prev_total_for_comparison,
        )

        pct_changes = compute_period_pct_changes(
            df_current_total_for_comparison,
            df_prev_total_for_comparison,
        )

    global_sku_current = compute_sku_precalc(df_current_detail)

    global_sku_mom = compare_sku_metrics(
        global_sku_current_for_comparison,
        global_sku_prev,
    )

    global_focus_skus = select_focus_skus_by_sales_mix(global_sku_current)

    global_remaining_agg = build_remaining_skus_aggregate(
        sku_current=global_sku_current_for_comparison,
        sku_prev=global_sku_prev,
        focus_skus=global_focus_skus,
    )

    return {
        "available": True,
        "source": "global_table",
        "selected_period": {
            "period": period,
            "timeline": timeline,
            "year": year,
            "period_label": period_label(period, timeline, year),
        },
        "previous_period": {
            "period": p_period,
            "timeline": p_timeline,
            "year": p_year,
            "period_label": period_label(p_period, p_timeline, p_year),
        },
        "comparison_months": comparison_months,
        "comparison_basis": (
            f"Compared months {comparison_months} of {year} "
            f"vs same months of {year - 1}"
            if period == "yearly"
            else (
                f"Compared only completed months {comparison_months} for {timeline} {year}"
                if period == "quarterly" and quarterly_compare is not None
                else None
            )
        ),
        "portfolio": {
            "current_values": current_values,
            "previous_values": previous_values,
            "absolute_changes": absolute_changes,
            "pct_changes": pct_changes,
        },
        "sku_current": global_sku_current,
        "sku_mom": global_sku_mom,
        "focus_skus": global_focus_skus,
        "remaining_agg": global_remaining_agg,
        "products": global_sku_mom,
    }

# def build_country_usd_numeric_metrics(
#     *,
#     user_id: int,
#     period: str,
#     timeline: str,
#     year: int,
#     available_countries: list[str] | None = None,
# ) -> dict:
#     """
#     Builds USD-normalized US and UK selected-period vs previous-period metrics.

#     Sources:
#     skuwisemonthly_{user_id}_us_usd_{month}{year}
#     skuwisemonthly_{user_id}_uk_usd_{month}{year}

#     For yearly:
#     - Do NOT compare full yearly USD table vs previous full yearly USD table.
#     - Compare available monthly USD tables in selected year
#       vs the same months from previous year.
#     """

#     (p_period, p_timeline, p_year), _ = resolve_comparison(
#         period,
#         timeline,
#         year,
#     )

#     def country_metrics(country: str) -> dict:
#         df_current = fetch_country_usd_precalc_table(
#             user_id=user_id,
#             country=country,
#             period=period,
#             timeline=timeline,
#             year=year,
#         )

#         df_current_detail, df_current_total = _split_total_row(df_current)

#         df_prev = fetch_country_usd_precalc_table(
#             user_id=user_id,
#             country=country,
#             period=p_period,
#             timeline=p_timeline,
#             year=p_year,
#         )

#         df_prev_detail, df_prev_total = _split_total_row(df_prev)

#         if df_current.empty:
#             return {
#                 "available": False,
#                 "country": country,
#                 "currency": "USD",
#                 "reason": f"No selected-period USD table found for {country}",
#                 "portfolio": {},
#                 "products": {},
#                 "comparison_months": [],
#             }

#         # ============================================================
#         # YEARLY PARTIAL-YEAR COMPARISON OVERRIDE
#         # ============================================================
#         comparison_months = []

#         if period == "yearly":
#             yearly_compare = aggregate_monthly_tables_for_yearly_comparison_generic(
#                 user_id=user_id,
#                 year=year,
#                 fetch_monthly_func=lambda user_id, timeline, year: fetch_country_usd_precalc_table(
#                     user_id=user_id,
#                     country=country,
#                     period="monthly",
#                     timeline=timeline,
#                     year=year,
#                 ),
#             )

#             comparison_months = yearly_compare["available_months"]

#             df_current_total_for_comparison = yearly_compare["df_current_total"]
#             df_prev_total_for_comparison = yearly_compare["df_prev_total"]

#             sku_current_for_comparison = yearly_compare["sku_current"]
#             sku_prev = yearly_compare["sku_prev"]

#         else:
#             df_current_total_for_comparison = df_current_total
#             df_prev_total_for_comparison = df_prev_total

#             sku_current_for_comparison = compute_sku_precalc(df_current_detail)
#             sku_prev = compute_sku_precalc(df_prev_detail)

#         # ============================================================
#         # CURRENT / PREVIOUS VALUES
#         # For yearly, these now use partial-year monthly aggregation.
#         # ============================================================
#         current_values = {
#             "units": _total_value(df_current_total_for_comparison, "total_quantity"),
#             "net_sales": _total_value(df_current_total_for_comparison, "net_sales"),
#             "asp": _total_value(df_current_total_for_comparison, "asp"),
#             "cm1_profit": _total_value(df_current_total_for_comparison, "profit"),
#             "cm1_profit_per_unit": _total_value(df_current_total_for_comparison, "unit_wise_profitability"),
#             "cm2_profit": _total_value(df_current_total_for_comparison, "cm2_profit"),
#             "advertising": _advertising_total_final_value(df_current_total_for_comparison),
#             "storage_fees": _total_value(df_current_total_for_comparison, "platform_fee_inventory_storage"),
#             "acos": _total_value(df_current_total_for_comparison, "acos"),
#         }

#         previous_values = {
#             "units": _total_value(df_prev_total_for_comparison, "total_quantity"),
#             "net_sales": _total_value(df_prev_total_for_comparison, "net_sales"),
#             "asp": _total_value(df_prev_total_for_comparison, "asp"),
#             "cm1_profit": _total_value(df_prev_total_for_comparison, "profit"),
#             "cm1_profit_per_unit": _total_value(df_prev_total_for_comparison, "unit_wise_profitability"),
#             "cm2_profit": _total_value(df_prev_total_for_comparison, "cm2_profit"),
#             "advertising": _advertising_total_final_value(df_prev_total_for_comparison),
#             "storage_fees": _total_value(df_prev_total_for_comparison, "platform_fee_inventory_storage"),
#             "acos": _total_value(df_prev_total_for_comparison, "acos"),
#         }

#         absolute_changes = {}
#         pct_changes = {}

#         if (
#             not df_current_total_for_comparison.empty
#             and not df_prev_total_for_comparison.empty
#         ):
#             absolute_changes = compute_period_absolute_changes(
#                 df_current_total_for_comparison,
#                 df_prev_total_for_comparison,
#             )

#             pct_changes = compute_period_pct_changes(
#                 df_current_total_for_comparison,
#                 df_prev_total_for_comparison,
#             )

#         sku_mom = compare_sku_metrics(
#             sku_current_for_comparison,
#             sku_prev,
#         )

#         return {
#             "available": True,
#             "country": country,
#             "currency": "USD",
#             "comparison_months": comparison_months,
#             "comparison_basis": (
#                 f"Compared months {comparison_months} of {year} "
#                 f"vs same months of {year - 1}"
#                 if period == "yearly"
#                 else None
#             ),
#             "portfolio": {
#                 "current_values": current_values,
#                 "previous_values": previous_values,
#                 "absolute_changes": absolute_changes,
#                 "pct_changes": pct_changes,
#             },
#             "products": sku_mom,
#         }

#     available_countries = available_countries or SUPPORTED_GLOBAL_COUNTRIES

#     result = {
#         "currency": "USD",
#         "currency_note": "Country values are USD-normalized where available.",
#         "selected_period": {
#             "period": period,
#             "timeline": timeline,
#             "year": year,
#             "period_label": period_label(period, timeline, year),
#         },
#         "previous_period": {
#             "period": p_period,
#             "timeline": p_timeline,
#             "year": p_year,
#             "period_label": period_label(p_period, p_timeline, p_year),
#         },
#     }

#     for country in available_countries:
#         result[country] = country_metrics(country)

#     return result

def build_country_usd_numeric_metrics(
    *,
    user_id: int,
    period: str,
    timeline: str,
    year: int,
    available_countries: list[str] | None = None,
) -> dict:
    """
    Builds USD-normalized US and UK selected-period vs previous-period metrics.

    Yearly:
    - Uses monthly aggregation capped to latest completed month.

    Quarterly:
    - If selected quarter is incomplete, uses completed monthly USD tables only.
    """

    (p_period, p_timeline, p_year), _ = resolve_comparison(
        period,
        timeline,
        year,
    )

    def country_metrics(country: str) -> dict:
        df_current = fetch_country_usd_precalc_table(
            user_id=user_id,
            country=country,
            period=period,
            timeline=timeline,
            year=year,
        )

        df_current_detail, df_current_total = _split_total_row(df_current)

        df_prev = fetch_country_usd_precalc_table(
            user_id=user_id,
            country=country,
            period=p_period,
            timeline=p_timeline,
            year=p_year,
        )

        df_prev_detail, df_prev_total = _split_total_row(df_prev)

        quarterly_compare = None

        if period == "quarterly":
            quarterly_max_month = get_period_month_cap(
                year=year,
                period="quarterly",
                timeline=timeline,
            )

            q = int(str(timeline).replace("Q", ""))
            quarter_end_month = q * 3

            if quarterly_max_month < quarter_end_month:
                quarterly_compare = aggregate_monthly_tables_for_quarterly_comparison_generic(
                    user_id=user_id,
                    year=year,
                    timeline=timeline,
                    fetch_monthly_func=lambda user_id, timeline, year: fetch_country_usd_precalc_table(
                        user_id=user_id,
                        country=country,
                        period="monthly",
                        timeline=timeline,
                        year=year,
                    ),
                )

                df_current_detail = quarterly_compare["df_current_detail"]
                df_current_total = quarterly_compare["df_current_total"]

        if df_current.empty and quarterly_compare is None:
            return {
                "available": False,
                "country": country,
                "currency": "USD",
                "reason": f"No selected-period USD table found for {country}",
                "portfolio": {},
                "products": {},
                "comparison_months": [],
            }

        comparison_months = []

        if period == "quarterly" and quarterly_compare is not None:
            comparison_months = quarterly_compare["available_months"]

            df_current_total_for_comparison = quarterly_compare["df_current_total"]
            df_prev_total_for_comparison = quarterly_compare["df_prev_total"]

            sku_current_for_comparison = quarterly_compare["sku_current"]
            sku_prev = quarterly_compare["sku_prev"]

        elif period == "yearly":
            yearly_compare = aggregate_monthly_tables_for_yearly_comparison_generic(
                user_id=user_id,
                year=year,
                max_month=get_period_month_cap(
                    year=year,
                    period="yearly",
                    timeline="ALL",
                ),
                fetch_monthly_func=lambda user_id, timeline, year: fetch_country_usd_precalc_table(
                    user_id=user_id,
                    country=country,
                    period="monthly",
                    timeline=timeline,
                    year=year,
                ),
            )

            comparison_months = yearly_compare["available_months"]

            df_current_total_for_comparison = yearly_compare["df_current_total"]
            df_prev_total_for_comparison = yearly_compare["df_prev_total"]

            sku_current_for_comparison = yearly_compare["sku_current"]
            sku_prev = yearly_compare["sku_prev"]

        else:
            df_current_total_for_comparison = df_current_total
            df_prev_total_for_comparison = df_prev_total

            sku_current_for_comparison = compute_sku_precalc(df_current_detail)
            sku_prev = compute_sku_precalc(df_prev_detail)

        current_values = {
            "units": _total_value(df_current_total_for_comparison, "total_quantity"),
            "net_sales": _total_value(df_current_total_for_comparison, "net_sales"),
            "asp": _total_value(df_current_total_for_comparison, "asp"),
            "cm1_profit": _total_value(df_current_total_for_comparison, "profit"),
            "cm1_profit_per_unit": _total_value(df_current_total_for_comparison, "unit_wise_profitability"),
            "cm2_profit": _total_value(df_current_total_for_comparison, "cm2_profit"),
            "advertising": _advertising_total_value(df_current_total_for_comparison),
            "storage_fees": _total_value(df_current_total_for_comparison, "platform_fee_inventory_storage"),
            "acos": _total_value(df_current_total_for_comparison, "acos"),
        }

        previous_values = {
            "units": _total_value(df_prev_total_for_comparison, "total_quantity"),
            "net_sales": _total_value(df_prev_total_for_comparison, "net_sales"),
            "asp": _total_value(df_prev_total_for_comparison, "asp"),
            "cm1_profit": _total_value(df_prev_total_for_comparison, "profit"),
            "cm1_profit_per_unit": _total_value(df_prev_total_for_comparison, "unit_wise_profitability"),
            "cm2_profit": _total_value(df_prev_total_for_comparison, "cm2_profit"),
            "advertising": _advertising_total_value(df_prev_total_for_comparison),
            "storage_fees": _total_value(df_prev_total_for_comparison, "platform_fee_inventory_storage"),
            "acos": _total_value(df_prev_total_for_comparison, "acos"),
        }

        absolute_changes = {}
        pct_changes = {}

        if (
            not df_current_total_for_comparison.empty
            and not df_prev_total_for_comparison.empty
        ):
            absolute_changes = compute_period_absolute_changes(
                df_current_total_for_comparison,
                df_prev_total_for_comparison,
            )

            pct_changes = compute_period_pct_changes(
                df_current_total_for_comparison,
                df_prev_total_for_comparison,
            )

        sku_mom = compare_sku_metrics(
            sku_current_for_comparison,
            sku_prev,
        )

        return {
            "available": True,
            "country": country,
            "currency": "USD",
            "comparison_months": comparison_months,
            "comparison_basis": (
                f"Compared months {comparison_months} of {year} "
                f"vs same months of {year - 1}"
                if period == "yearly"
                else (
                    f"Compared only completed months {comparison_months} for {timeline} {year}"
                    if period == "quarterly" and quarterly_compare is not None
                    else None
                )
            ),
            "portfolio": {
                "current_values": current_values,
                "previous_values": previous_values,
                "absolute_changes": absolute_changes,
                "pct_changes": pct_changes,
            },
            "products": sku_mom,
        }

    available_countries = available_countries or SUPPORTED_GLOBAL_COUNTRIES

    result = {
        "currency": "USD",
        "currency_note": "Country values are USD-normalized where available.",
        "selected_period": {
            "period": period,
            "timeline": timeline,
            "year": year,
            "period_label": period_label(period, timeline, year),
        },
        "previous_period": {
            "period": p_period,
            "timeline": p_timeline,
            "year": p_year,
            "period_label": period_label(p_period, p_timeline, p_year),
        },
    }

    for country in available_countries:
        result[country] = country_metrics(country)

    return result


def build_country_contribution_context(
    *,
    global_numeric_metrics: dict,
    country_usd_metrics: dict,
    available_countries: list[str],
) -> list[dict]:
    """
    Builds deterministic country movement + contribution context
    for the Global summary.

    Source:
    - global_numeric_metrics = actual global table
    - country_usd_metrics = USD-normalized country tables
    """

    global_portfolio = global_numeric_metrics.get("portfolio", {}) or {}

    global_current = global_portfolio.get("current_values", {}) or {}
    global_absolute = global_portfolio.get("absolute_changes", {}) or {}

    global_current_net_sales = float(global_current.get("net_sales") or 0)
    global_current_units = float(global_current.get("units") or 0)

    global_net_sales_delta = float(global_absolute.get("net_sales") or 0)
    global_units_delta = float(global_absolute.get("units") or 0)
    global_cm2_delta = float(global_absolute.get("cm2_profit") or 0)

    output = []

    for country in available_countries or []:
        country_key = str(country).lower()
        country_data = country_usd_metrics.get(country_key) or {}

        if not country_data.get("available"):
            continue

        portfolio = country_data.get("portfolio", {}) or {}

        current_values = portfolio.get("current_values", {}) or {}
        previous_values = portfolio.get("previous_values", {}) or {}
        absolute_changes = portfolio.get("absolute_changes", {}) or {}
        pct_changes = portfolio.get("pct_changes", {}) or {}

        country_current_net_sales = float(current_values.get("net_sales") or 0)
        country_current_units = float(current_values.get("units") or 0)

        country_net_sales_delta = float(absolute_changes.get("net_sales") or 0)
        country_units_delta = float(absolute_changes.get("units") or 0)
        country_cm2_delta = float(absolute_changes.get("cm2_profit") or 0)

        output.append({
            "country": country_key,
            "currency": "USD",

            "current_values": current_values,
            "previous_values": previous_values,
            "absolute_changes": absolute_changes,
            "pct_changes": pct_changes,

            "contribution": {
                "net_sales_share_pct": (
                    round((country_current_net_sales / global_current_net_sales) * 100, 2)
                    if global_current_net_sales else None
                ),
                "units_share_pct": (
                    round((country_current_units / global_current_units) * 100, 2)
                    if global_current_units else None
                ),
                "net_sales_delta_contribution_pct": (
                    round((country_net_sales_delta / global_net_sales_delta) * 100, 2)
                    if global_net_sales_delta else None
                ),
                "units_delta_contribution_pct": (
                    round((country_units_delta / global_units_delta) * 100, 2)
                    if global_units_delta else None
                ),
                "cm2_delta_contribution_pct": (
                    round((country_cm2_delta / global_cm2_delta) * 100, 2)
                    if global_cm2_delta else None
                ),
            }
        })

    return output

# def build_integrated_global_summary_with_country_drivers(
#     *,
#     global_numeric_metrics: dict,
#     country_contribution_context: list[dict] | None,
# ) -> str:
#     """
#     Builds Global Overall Summary with country drivers merged into each metric line.
#     Example:
#     Units sold declined globally, mainly driven by US...
#     """

#     portfolio = (global_numeric_metrics or {}).get("portfolio", {}) or {}

#     current = portfolio.get("current_values", {}) or {}
#     pct = portfolio.get("pct_changes", {}) or {}

#     country_rows = country_contribution_context or []

#     def _is_num(x):
#         return isinstance(x, (int, float)) and not pd.isna(x)

#     def _fmt_pct(x):
#         return f"{abs(x):.2f}%" if _is_num(x) else "N/A"

#     def _fmt_pp(x):
#         return f"{abs(x):.2f} points" if _is_num(x) else "N/A"

#     def _fmt_number(x, decimals=0):
#         if not _is_num(x):
#             return "N/A"
#         if decimals == 0:
#             return f"{int(round(x)):,}"
#         return f"{x:,.{decimals}f}"

#     def _fmt_currency(x):
#         return f"${x:,.2f}" if _is_num(x) else "N/A"

#     def _direction(metric_pct):
#         if not _is_num(metric_pct):
#             return "moved"
#         if metric_pct < 0:
#             return "declined"
#         if metric_pct > 0:
#             return "increased"
#         return "remained flat"

#     def _driver_by_contribution(contribution_key: str):
#         """
#         Uses already calculated country movement contribution.
#         Best for units, net sales, and CM2 profit.
#         """
#         candidates = []

#         for item in country_rows:
#             if not isinstance(item, dict):
#                 continue

#             country = str(item.get("country") or "").upper()
#             contribution = item.get("contribution", {}) or {}
#             value = contribution.get(contribution_key)

#             if _is_num(value):
#                 candidates.append((country, value))

#         if not candidates:
#             return ""

#         country, value = max(candidates, key=lambda x: abs(x[1]))

#         return (
#             f", mainly driven by {country}, which accounted for "
#             f"{abs(value):.2f}% of the global movement"
#         )

#     def _driver_by_abs_change(metric_key: str):
#         """
#         Uses largest absolute movement where contribution % is not separately calculated.
#         Best for ASP, CM1, profit per unit, advertising, storage, ACOS.
#         """
#         candidates = []

#         for item in country_rows:
#             if not isinstance(item, dict):
#                 continue

#             country = str(item.get("country") or "").upper()
#             absolute = item.get("absolute_changes", {}) or {}
#             value = absolute.get(metric_key)

#             if _is_num(value):
#                 candidates.append((country, value))

#         if not candidates:
#             return ""

#         country, _ = max(candidates, key=lambda x: abs(x[1]))
#         return f", with the largest country-level movement coming from {country}"

#     lines = []

#     units_pct = pct.get("units")
#     sales_pct = pct.get("net_sales")

#     lines.append(
#         f"Units sold {_direction(units_pct)} by {_fmt_pct(units_pct)} "
#         f"to {_fmt_number(current.get('units'), 0)}"
#         f"{_driver_by_contribution('units_delta_contribution_pct')}, "
#         f"while net sales {_direction(sales_pct)} by {_fmt_pct(sales_pct)} "
#         f"to {_fmt_currency(current.get('net_sales'))}"
#         f"{_driver_by_contribution('net_sales_delta_contribution_pct')}."
#     )

#     asp_pct = pct.get("asp")

#     lines.append(
#         f"ASP {_direction(asp_pct)} by {_fmt_pct(asp_pct)} "
#         f"to {_fmt_currency(current.get('asp'))}"
#         f"{_driver_by_abs_change('asp')}, but this price improvement was not enough "
#         f"to offset the global volume loss."
#     )

#     cm1_pct = pct.get("cm1_profit")
#     ppu_pct = pct.get("cm1_profit_per_unit")

#     lines.append(
#         f"CM1 profit {_direction(cm1_pct)} by {_fmt_pct(cm1_pct)} "
#         f"to {_fmt_currency(current.get('cm1_profit'))}"
#         f"{_driver_by_abs_change('cm1_profit')}, while CM1 profit per unit "
#         f"{_direction(ppu_pct)} by {_fmt_pct(ppu_pct)} "
#         f"to {_fmt_currency(current.get('cm1_profit_per_unit'))}"
#         f"{_driver_by_abs_change('cm1_profit_per_unit')}."
#     )

#     cm2_pct = pct.get("cm2_profit")
#     ad_pct = pct.get("advertising")

#     lines.append(
#         f"CM2 profit {_direction(cm2_pct)} by {_fmt_pct(cm2_pct)} "
#         f"to {_fmt_currency(current.get('cm2_profit'))}"
#         f"{_driver_by_contribution('cm2_delta_contribution_pct')}, "
#         f"while advertising spend {_direction(ad_pct)} by {_fmt_pct(ad_pct)} "
#         f"to {_fmt_currency(current.get('advertising'))}"
#         f"{_driver_by_abs_change('advertising')}."
#     )

#     storage_pct = pct.get("storage_fees")

#     lines.append(
#         f"Storage fees {_direction(storage_pct)} by {_fmt_pct(storage_pct)} "
#         f"to {_fmt_currency(current.get('storage_fees'))}"
#         f"{_driver_by_abs_change('storage_fees')}."
#     )

#     acos_delta = pct.get("acos")

#     lines.append(
#         f"Advertising efficiency deteriorated, with ACOS rising by "
#         f"{_fmt_pp(acos_delta)} to {_fmt_pct(current.get('acos'))}"
#         f"{_driver_by_abs_change('acos')}."
#     )

#     return "\n".join(f"• {line}" for line in lines)

def build_global_metric_driver_context(
    *,
    global_numeric_metrics: dict,
    country_contribution_context: list[dict] | None,
) -> dict:
    """
    Builds structured country-driver context for the LLM.
    This does NOT write the summary.
    It only tells AI which country drove each global metric movement.
    """

    portfolio = (global_numeric_metrics or {}).get("portfolio", {}) or {}

    current_values = portfolio.get("current_values", {}) or {}
    previous_values = portfolio.get("previous_values", {}) or {}
    pct_changes = portfolio.get("pct_changes", {}) or {}

    country_rows = country_contribution_context or []

    def is_num(x):
        return isinstance(x, (int, float)) and not pd.isna(x)

    def main_driver_by_contribution(contribution_key: str):
        candidates = []

        for item in country_rows:
            if not isinstance(item, dict):
                continue

            country = str(item.get("country") or "").upper()
            contribution = item.get("contribution", {}) or {}
            value = contribution.get(contribution_key)

            if is_num(value):
                candidates.append({
                    "country": country,
                    "movement_contribution_pct": round(value, 2),
                    "absolute_movement_contribution_pct": round(abs(value), 2),
                })

        if not candidates:
            return None

        return max(
            candidates,
            key=lambda x: x["absolute_movement_contribution_pct"]
        )

    def main_driver_by_abs_change(metric_key: str):
        candidates = []

        for item in country_rows:
            if not isinstance(item, dict):
                continue

            country = str(item.get("country") or "").upper()
            absolute_changes = item.get("absolute_changes", {}) or {}
            pct = item.get("pct_changes", {}) or {}

            value = absolute_changes.get(metric_key)

            if is_num(value):
                candidates.append({
                    "country": country,
                    "absolute_change": round(value, 2),
                    "absolute_change_magnitude": round(abs(value), 2),
                    "country_pct_change": pct.get(metric_key),
                })

        if not candidates:
            return None

        return max(
            candidates,
            key=lambda x: x["absolute_change_magnitude"]
        )

    return {
        "units": {
            "current_value": current_values.get("units"),
            "previous_value": previous_values.get("units"),
            "global_pct_change": pct_changes.get("units"),
            "main_driver": main_driver_by_contribution("units_delta_contribution_pct"),
        },
        "net_sales": {
            "current_value": current_values.get("net_sales"),
            "previous_value": previous_values.get("net_sales"),
            "global_pct_change": pct_changes.get("net_sales"),
            "main_driver": main_driver_by_contribution("net_sales_delta_contribution_pct"),
        },
        "asp": {
            "current_value": current_values.get("asp"),
            "previous_value": previous_values.get("asp"),
            "global_pct_change": pct_changes.get("asp"),
            "main_driver": main_driver_by_abs_change("asp"),
        },
        "cm1_profit": {
            "current_value": current_values.get("cm1_profit"),
            "previous_value": previous_values.get("cm1_profit"),
            "global_pct_change": pct_changes.get("cm1_profit"),
            "main_driver": main_driver_by_abs_change("cm1_profit"),
        },
        "cm1_profit_per_unit": {
            "current_value": current_values.get("cm1_profit_per_unit"),
            "previous_value": previous_values.get("cm1_profit_per_unit"),
            "global_pct_change": pct_changes.get("cm1_profit_per_unit"),
            "main_driver": main_driver_by_abs_change("cm1_profit_per_unit"),
        },
        "cm2_profit": {
            "current_value": current_values.get("cm2_profit"),
            "previous_value": previous_values.get("cm2_profit"),
            "global_pct_change": pct_changes.get("cm2_profit"),
            "main_driver": main_driver_by_contribution("cm2_delta_contribution_pct"),
        },
        "advertising": {
            "current_value": current_values.get("advertising"),
            "previous_value": previous_values.get("advertising"),
            "global_pct_change": pct_changes.get("advertising"),
            "main_driver": main_driver_by_abs_change("advertising"),
        },
        "storage_fees": {
            "current_value": current_values.get("storage_fees"),
            "previous_value": previous_values.get("storage_fees"),
            "global_pct_change": pct_changes.get("storage_fees"),
            "main_driver": main_driver_by_abs_change("storage_fees"),
        },
        "acos": {
            "current_value": current_values.get("acos"),
            "previous_value": previous_values.get("acos"),
            "global_point_change": pct_changes.get("acos"),
            "main_driver": main_driver_by_abs_change("acos"),
        },
    }



def key_metrics_by_product_name(metrics_by_sku: dict) -> dict:
    """
    Converts SKU-keyed metrics into product_name-keyed metrics.

    Input:
        {
            "SEMNIWRF": {
                "product_name": "Refill Pack",
                ...
            }
        }

    Output:
        {
            "Refill Pack": {
                "sku": "SEMNIWRF",
                "product_name": "Refill Pack",
                ...
            }
        }

    If duplicate product names exist, the SKU is appended to avoid overwriting.
    """

    if not isinstance(metrics_by_sku, dict):
        return {}

    output = {}

    for sku, data in metrics_by_sku.items():
        if not isinstance(data, dict):
            continue

        product_name = data.get("product_name") or sku
        product_name = str(product_name).strip() or str(sku)

        key = product_name

        # Prevent duplicate product names from overwriting each other
        if key in output:
            key = f"{product_name} ({sku})"

        row = dict(data)
        row["sku"] = sku
        row["product_name"] = product_name

        output[key] = row

    return output


def run_global_comparison_prompt(global_payload: dict) -> dict:
    resp = openai_client.chat.completions.create(
        model="gpt-4.1",
        messages=[
            {"role": "system", "content": AI_GLOBAL_COMPARISON_PROMPT},
            {"role": "user", "content": json.dumps(global_payload, separators=(",", ":"))},
        ],
        temperature=0.2,
    )
    try:
        return json.loads(resp.choices[0].message.content)
    except Exception:
        print("\n❌ Global comparison JSON parse failed")
        return {
            "global_summary": "",
            "country_comparison": [],
            "uk_vs_us_comparison": [],
            "product_journey_comparison": [],
            "other_skus_comparison": {},
            "global_overall_recommendation": "",
        }
    
def fmt_pct(x):
    return f"{x:+.2f}%" if isinstance(x, (int, float)) else "N/A"

def fmt_number(x, decimals=2):
    if not isinstance(x, (int, float)):
        return "N/A"
    if decimals == 0:
        return f"{int(round(x)):,}"
    return f"{x:,.{decimals}f}"

def fmt_currency(x):
    return f"${x:,.2f}" if isinstance(x, (int, float)) else "N/A"

def fmt_value_with_pct(metric_dict, is_currency=False, decimals=2):
    if not isinstance(metric_dict, dict):
        return "N/A"
    current = metric_dict.get("current")
    pct = metric_dict.get("delta_pct")
    current_str = (
        fmt_currency(current)
        if is_currency
        else fmt_number(current, decimals=decimals)
    )
    return f"{current_str} ({fmt_pct(pct)})"



def render_global_comparison_summary(
    *,
    global_ai: dict,
    us_result: dict,
    uk_result: dict,
    period: str,
    timeline: str,
    year: int,
    remaining_agg: dict | None = None,
    available_countries: list[str] | None = None,

    # ✅ NEW: deterministic country movement/contribution
    country_contribution_context: list[dict] | None = None,

    # ✅ NEW: all global SKU individual insights
    all_sku_mom: dict | None = None,
    focus_skus: list | None = None,
) -> str:
    """
    AI-written global summary renderer.
    Shows one UK vs US comparison summary, product journey comparison,
    Other SKUs journey/actions, and one global recommendation.
    """

    lines = []

    available_countries = available_countries or []
    is_multi_country = len(available_countries) >= 2

    # ------------------------------------------------------------
    # Format helpers
    # ------------------------------------------------------------
    def fmt_pct(x):
        return f"{x:+.2f}%" if isinstance(x, (int, float)) else "N/A"

    def fmt_number(x, decimals=2):
        if not isinstance(x, (int, float)):
            return "N/A"

        if decimals == 0:
            return f"{int(round(x)):,}"

        return f"{x:,.{decimals}f}"

    def fmt_currency(x):
        return f"${x:,.2f}" if isinstance(x, (int, float)) else "N/A"

    def fmt_value_with_pct(metric_dict, is_currency=False, decimals=2):
        if not isinstance(metric_dict, dict):
            return "N/A"

        current = metric_dict.get("current")
        pct = metric_dict.get("delta_pct")

        if is_currency:
            current_str = fmt_currency(current)
        else:
            current_str = fmt_number(current, decimals=decimals)

        return f"{current_str} ({fmt_pct(pct)})"

    lines.append("Global Business Summary")
    lines.append(f"Period: {period_label(period, timeline, year)}")

    if global_ai.get("global_summary"):
        lines.append("")
        lines.append("## OVERALL SUMMARY")
        lines.append(global_ai["global_summary"])

  

    # ============================================================
    # AI UK VS US COMPARISON
    # ============================================================
    country_comparison = (
        global_ai.get("country_comparison")
        or global_ai.get("uk_vs_us_comparison")
        or []
    )

    if country_comparison:
        lines.append("")

        if is_multi_country:
            lines.append("## COUNTRY COMPARISON")
        else:
            country_label = available_countries[0].upper() if available_countries else "COUNTRY"
            lines.append(f"## {country_label} PERFORMANCE")

        for point in country_comparison:
            if isinstance(point, str) and point.strip():
                lines.append(f"• {point}")

    # ============================================================
    # AI PRODUCT-WISE JOURNEY COMPARISON
    # ============================================================
    if global_ai.get("product_journey_comparison"):
        lines.append("")
        lines.append("## PRODUCT JOURNEY")

        for item in global_ai["product_journey_comparison"]:
            if isinstance(item, dict):
                product_name = item.get("product_name") or "Unknown Product"
                journey_comparison = item.get("journey_comparison") or []

                lines.append("")
                lines.append(f"### {product_name}")

                if is_multi_country:
                    sku_us = item.get("sku_us")
                    sku_uk = item.get("sku_uk")

                    if sku_us:
                        lines.append(f"• US SKU: {sku_us}")
                    if sku_uk:
                        lines.append(f"• UK SKU: {sku_uk}")

                else:
                    country = available_countries[0] if available_countries else None

                    if country:
                        sku_by_country = item.get("sku_by_country") or {}

                        sku_value = (
                            item.get(f"sku_{country}")
                            or sku_by_country.get(country)
                            or sku_by_country.get(country.upper())
                        )

                        if sku_value:
                            lines.append(f"• {country.upper()} SKU: {sku_value}")

                if isinstance(journey_comparison, list) and journey_comparison:
                    for point in journey_comparison:
                        if isinstance(point, str) and point.strip():
                            lines.append(f"• {point}")

                elif isinstance(journey_comparison, str) and journey_comparison.strip():
                    lines.append(f"• {journey_comparison}")

                country_actions = item.get("country_actions") or {}

                if isinstance(country_actions, dict) and country_actions:
                    rendered_any_action = False
                    action_lines = []

                    for country in available_countries:
                        actions = (
                            country_actions.get(country)
                            or country_actions.get(country.upper())
                            or {}
                        )

                        if not isinstance(actions, dict):
                            continue

                        if not any(str(v).strip() for v in actions.values() if v):
                            continue

                        rendered_any_action = True

                        if is_multi_country:
                            action_lines.append(f"   - {country.upper()}:")

                            if actions.get("recommendation"):
                                action_lines.append(f"      • Recommendation: {actions['recommendation']}")
                            if actions.get("inventory_recommendation"):
                                action_lines.append(f"      • Inventory action: {actions['inventory_recommendation']}")
                            if actions.get("ads_recommendation"):
                                action_lines.append(f"      • Ads action: {actions['ads_recommendation']}")
                        else:
                            if actions.get("recommendation"):
                                action_lines.append(f"   - Recommendation: {actions['recommendation']}")
                            if actions.get("inventory_recommendation"):
                                action_lines.append(f"   - Inventory action: {actions['inventory_recommendation']}")
                            if actions.get("ads_recommendation"):
                                action_lines.append(f"   - Ads action: {actions['ads_recommendation']}")

                    if rendered_any_action:
                        lines.append("")
                        lines.append("• Country actions:")
                        lines.extend(action_lines)
            elif isinstance(item, str):
                lines.append(f"• {item}")

    # ============================================================
    # GLOBAL OTHER SKUs
    # ============================================================
    if remaining_agg:
        lines.append("")
        lines.append("## OTHER SKUs")

        # ✅ NEW: show which products are included in Global Other SKUs
        included_products = remaining_agg.get("included_products", [])

        if isinstance(included_products, list) and included_products:
            lines.append("• Products included:")

            for item in included_products:
                if not isinstance(item, dict):
                    continue

                product_name = item.get("product_name")
                sku = item.get("sku")

                if product_name and sku:
                    lines.append(f"   - {product_name} ({sku})")
                elif product_name:
                    lines.append(f"   - {product_name}")

        lines.append(
            f"• ASP: {fmt_value_with_pct(remaining_agg.get('asp'), is_currency=True)}"
        )

        lines.append(
            f"• Units: {fmt_value_with_pct(remaining_agg.get('total_quantity'), decimals=0)}"
        )

        lines.append(
            f"• Net sales: {fmt_value_with_pct(remaining_agg.get('net_sales'), is_currency=True)}"
        )

        lines.append(
            f"• CM1 profit: {fmt_value_with_pct(remaining_agg.get('profit'), is_currency=True)}"
        )

        lines.append(
            f"• CM1 profit per unit: {fmt_value_with_pct(remaining_agg.get('unit_wise_profitability'), is_currency=True)}"
        )

        # ✅ Other SKUs journey + actions, same behavior as product cards
        other_skus_comparison = global_ai.get("other_skus_comparison") or {}

        other_journey = other_skus_comparison.get("journey_comparison")
        if isinstance(other_journey, list) and other_journey:
            lines.append("• Product journey:")
            for point in other_journey:
                if isinstance(point, str) and point.strip():
                    lines.append(f"   - {point}")

        elif isinstance(other_journey, str) and other_journey.strip():
            lines.append("• Product journey:")
            lines.append(f"   - {other_journey}")

        country_actions = other_skus_comparison.get("country_actions") or {}
        global_actions = country_actions.get("global") or {}

        if isinstance(global_actions, dict) and any(
            str(v).strip() for v in global_actions.values() if v
        ):
            lines.append("")
            lines.append("• Country actions:")

            if global_actions.get("recommendation"):
                lines.append(f"   - Recommendation: {global_actions['recommendation']}")

            if global_actions.get("inventory_recommendation"):
                lines.append(f"   - Inventory action: {global_actions['inventory_recommendation']}")

            if global_actions.get("ads_recommendation"):
                lines.append(f"   - Ads action: {global_actions['ads_recommendation']}")

    # ============================================================
    # GLOBAL ALL SKUs — INDIVIDUAL METRICS
    # Does NOT change Global Product Journey / Other SKUs logic
    # ============================================================
    if all_sku_mom:

        lines.append("")
        lines.append("## ALL SKU INDIVIDUAL INSIGHTS")

        focus_set = set(str(s) for s in (focus_skus or []))

        def _sku_sort_value(item):
            sku, data = item
            if not isinstance(data, dict):
                return 0

            net_sales = data.get("net_sales", {}) or {}
            if not isinstance(net_sales, dict):
                return 0

            return net_sales.get("current") or 0

        for sku, s in sorted(all_sku_mom.items(), key=_sku_sort_value, reverse=True):
            if not isinstance(s, dict):
                continue

            sku_clean = str(sku).strip()

            if sku_clean.lower() in TOTAL_LABELS:
                continue

            name = s.get("product_name", sku_clean)
            sku_bucket = "Top 80% SKU" if sku_clean in focus_set else "Other SKU"

            lines.append("")
            lines.append(name)
            lines.append(f"• SKU: {sku_clean}")
            lines.append(f"• Bucket: {sku_bucket}")

            lines.append(
                f"• ASP: {fmt_value_with_pct(s.get('asp'), is_currency=True)}"
            )

            lines.append(
                f"• Units: {fmt_value_with_pct(s.get('total_quantity'), decimals=0)}"
            )

            lines.append(
                f"• Net sales: {fmt_value_with_pct(s.get('net_sales'), is_currency=True)}"
            )

            lines.append(
                f"• CM1 profit: {fmt_value_with_pct(s.get('profit'), is_currency=True)}"
            )

            lines.append(
                f"• CM1 profit per unit: {fmt_value_with_pct(s.get('unit_wise_profitability'), is_currency=True)}"
            )

    # ============================================================
    # AI GLOBAL OVERALL RECOMMENDATION
    # ============================================================
    if global_ai.get("global_overall_recommendation"):
        lines.append("")
        lines.append("## OVERALL RECOMMENDATION")
        lines.append(f"• {global_ai['global_overall_recommendation']}")

    return "\n".join(lines)




def get_or_create_global_summary(
    user_id,
    marketplace_id,
    period,
    timeline,
    year,
    objective=None,
    target_sku: str | list | None = None,
    force_regenerate=False
):
    """
    Global is not a physical country table.
    It runs US and UK independently, then sends both to a global comparison prompt.
    Stores global output in DB to avoid repeated LLM calls.
    """

    # ============================================================
    # GLOBAL RECOMMENDATION RULE
    # Same behavior as single-country:
    # - latest monthly / latest quarterly => recommendations allowed
    # - old monthly / old quarterly => recommendations hidden
    # - yearly => recommendations hidden
    # ============================================================
    available_countries = get_available_global_countries(
        user_id=user_id,
        period=period,
        timeline=timeline,
        year=year,
    )

    if not available_countries:
        return {
            "summary": "No country data is available for the selected global period.",
            "scope": "global",
            "source": "no_data",
            "global_ai": {},
            "overall_recommendation": "",
            "mapped_product_count": 0,
            "available_countries": [],
            "allow_recommendations": False,
            "metrics": {},
            "inventory_alerts": {},
            "objectives": {},
            "comparison": {
                "period": period,
                "timeline": timeline,
                "year": year,
                "period_label": period_label(period, timeline, year),
            },
            "metrics_debug": {
                "available_countries": [],
                "is_single_country_global": False,
                "global_metrics_available": False,
                "country_usd_available": {},
            },
        }


    country_latest_flags = {
        country: is_latest_period(
            period,
            timeline,
            year,
            user_id=user_id,
            country=country,
        )
        for country in available_countries
    }

    allow_global_recommendations = False

    if period in ("monthly", "quarterly"):
        allow_global_recommendations = any(country_latest_flags.values())

    elif period == "yearly":
        allow_global_recommendations = any(country_latest_flags.values())

    # ============================================================
    # 1. CHECK GLOBAL CACHE FIRST
    # ============================================================
    cached = fetch_existing_summary(
        user_id=user_id,
        country="global",
        marketplace_id=marketplace_id,
        period=period,
        timeline=timeline,
        year=year,
    )

    if cached and not force_regenerate and not target_sku:
        cached_recommendations = {}

        try:
            cached_recommendations = (
                json.loads(cached.recommendations)
                if cached.recommendations else {}
            )
        except Exception:
            cached_recommendations = {}

        cached_available_countries = cached_recommendations.get("available_countries")

        # Important:
        # If old cache does not have available_countries, do NOT trust it.
        # It may contain old US/UK comparison text.
        cache_country_match = cached_available_countries == available_countries

        if cache_country_match:
            cached_global_ai = normalize_global_ai_country_action_keys(
                cached_recommendations.get("global_ai", {})
            )

            cached_global_recommendations = (
                cached_recommendations.get("frontend_recommendations")
                or cached_recommendations.get("recommendations")
                or extract_global_recommendations(
                    global_ai=cached_global_ai,
                    available_countries=available_countries,
                )
            )

            return {
                "summary": cached.summary,
                "scope": "global",
                "source": "db",
                "global_ai": cached_global_ai,
                "overall_recommendation": cached_recommendations.get("overall_recommendation", ""),
                "recommendations": cached_global_recommendations,
                "mapped_product_count": cached_recommendations.get("mapped_product_count", 0),
                "available_countries": cached_available_countries,
                "allow_recommendations": cached_recommendations.get(
                    "allow_global_recommendations",
                    allow_global_recommendations,
                ),
                "metrics": cached_recommendations.get("metrics", {}),

                # ✅ NEW
                "country_contribution_context": (
                    cached_recommendations.get("country_contribution_context")
                    or (cached_recommendations.get("metrics", {}) or {}).get("country_contribution_context", [])
                    or (cached_recommendations.get("metrics_debug", {}) or {}).get("country_contribution_context", [])
                ),

                "inventory_alerts": cached_recommendations.get("inventory_alerts", {}),
                "objectives": cached_recommendations.get("objectives", {}),
                "comparison": {
                    "period": period,
                    "timeline": timeline,
                    "year": year,
                    "period_label": period_label(period, timeline, year),
                },
                "metrics_debug": cached_recommendations.get("metrics_debug", {}),
            }

        # If cache does not match current country availability,
        # continue below and regenerate fresh global summary.

    # ============================================================
    # 2. GET US + UK COUNTRY SUMMARIES
    # ============================================================
    country_results = {}

    for country in available_countries:
        country_results[country] = _global_safe_result(
            get_or_create_summary(
                user_id=user_id,
                country=country,
                marketplace_id=marketplace_id,
                period=period,
                timeline=timeline,
                year=year,
                objective=objective,
                target_sku=target_sku,
                force_regenerate=True,
            )
        )

    # Keep these only for backward compatibility with existing functions.
    us_result = country_results.get("us", {})
    uk_result = country_results.get("uk", {})

    # ============================================================
    # 3. BUILD PRODUCT MAPPING + GLOBAL METRICS
    # ============================================================
    sku_mapping = fetch_global_sku_mapping(user_id)

    if len(available_countries) >= 2:
        mapped_product_journeys = build_mapped_product_journeys(
            sku_mapping=sku_mapping,
            us_result=us_result,
            uk_result=uk_result,
        )
    else:
        mapped_product_journeys = []

    # actual global metrics from skuwisemonthly_{user_id}_global_{month}{year}_table
    global_numeric_metrics = build_global_numeric_metrics(
        user_id=user_id,
        period=period,
        timeline=timeline,
        year=year,
    )

    # US/UK USD-normalized metrics from skuwisemonthly_{user_id}_{country}_usd_{month}{year}
    country_usd_metrics = build_country_usd_numeric_metrics(
        user_id=user_id,
        period=period,
        timeline=timeline,
        year=year,
        available_countries=available_countries,
    )

    country_contribution_context = build_country_contribution_context(
    global_numeric_metrics=global_numeric_metrics,
    country_usd_metrics=country_usd_metrics,
    available_countries=available_countries,
)

    # NEW: facts for AI to use while writing global_summary
    global_metric_driver_context = build_global_metric_driver_context(
        global_numeric_metrics=global_numeric_metrics,
        country_contribution_context=country_contribution_context,
    )

    metrics_debug = {
        "available_countries": available_countries,
        "is_single_country_global": len(available_countries) == 1,
        "global_metrics_available": bool(global_numeric_metrics.get("available")),
        "country_usd_available": {
            country: bool((country_usd_metrics.get(country) or {}).get("available"))
            for country in available_countries
        },

        # ✅ NEW
        "country_contribution_context": country_contribution_context,
        "global_metric_driver_context": global_metric_driver_context,
    }

    # ✅ frontend metrics from global table
    # Product-name-keyed for frontend display
    metrics = {
        "portfolio": global_numeric_metrics.get("portfolio", {}),

        "sku_current": key_metrics_by_product_name(
            global_numeric_metrics.get("sku_current", {})
        ),

        "sku_mom": key_metrics_by_product_name(
            global_numeric_metrics.get("sku_mom", {})
        ),

        # ✅ ADD THESE FOR GLOBAL OTHER SKU CARD
        "focus_skus": global_numeric_metrics.get("focus_skus", []),
        "remaining_agg": global_numeric_metrics.get("remaining_agg", {}),

        # ✅ NEW: all global SKUs individually for frontend
        "all_sku_mom": key_metrics_by_product_name(
            global_numeric_metrics.get("sku_mom", {})
        ),

        # ✅ NEW: country movement/contribution for frontend/debug
            "country_contribution_context": country_contribution_context,
            "global_metric_driver_context": global_metric_driver_context,
    }

    inventory_alerts_by_country = {
        country: result.get("inventory_alerts", {})
        for country, result in country_results.items()
    }

    objectives_by_country = {
        country: result.get("objective", {})
        for country, result in country_results.items()
    }

    # ============================================================
    # 4. BUILD GLOBAL PROMPT PAYLOAD
    # ============================================================
    global_payload = {
        "period": {
            "period": period,
            "timeline": timeline,
            "year": year,
            "period_label": period_label(period, timeline, year),
        },

        "available_countries": available_countries,
        "is_single_country_global": len(available_countries) == 1,

        "global_numeric_metrics": global_numeric_metrics,

        "country_usd_metrics": country_usd_metrics,

        # ✅ NEW: ready-made country movement/contribution numbers for AI
        "country_contribution_context": country_contribution_context,

        # NEW: tells AI which country drove each global metric movement
        "global_metric_driver_context": global_metric_driver_context,

        "countries": {
            country: {
                "summary": _extract_summary_intro(result),
                "portfolio_level_narrative": result.get("portfolio_level_narrative", {}),
                "portfolio_recommendation": result.get("portfolio_recommendation", ""),
                "recommendations": _extract_actions(result),
            }
            for country, result in country_results.items()
        },

        "mapped_product_journeys": mapped_product_journeys,

        "other_skus": {
            "product_name": "Other SKUs",
            "aggregated_metrics": global_numeric_metrics.get("remaining_agg", {}),
            "focus_skus": global_numeric_metrics.get("focus_skus", []),
        },
    }

    # ============================================================
    # 5. RUN GLOBAL LLM
    # ============================================================
    global_ai = run_global_comparison_prompt(global_payload)

    # Keep frontend-compatible lowercase country action keys:
    # country_actions["UK"] becomes country_actions["uk"]
    global_ai = normalize_global_ai_country_action_keys(global_ai)

    # Keep country drivers inside global_summary only.
    # Do not render a separate country comparison block on the frontend.
    global_ai["country_comparison"] = []
    global_ai["uk_vs_us_comparison"] = []

    # ============================================================
    # 5A. SUPPRESS RECOMMENDATIONS FOR OLD PERIODS / YEARLY
    # ============================================================
    if not allow_global_recommendations:
        global_ai["global_overall_recommendation"] = ""

        # Remove product-level actions if the model returned them
        for item in global_ai.get("product_journey_comparison", []):
            if not isinstance(item, dict):
                continue

            country_actions = item.get("country_actions")
            if not isinstance(country_actions, dict):
                continue

            for country_key in available_countries:
                actions = (
                    country_actions.get(country_key)
                    or country_actions.get(country_key.upper())
                )

                if not isinstance(actions, dict):
                    continue

                actions["recommendation"] = ""
                actions["inventory_recommendation"] = ""
                actions["ads_recommendation"] = ""

        # ✅ Remove Other SKUs recommendation/actions also
        other_skus_comparison = global_ai.get("other_skus_comparison")
        if isinstance(other_skus_comparison, dict):
            country_actions = other_skus_comparison.get("country_actions")
            if isinstance(country_actions, dict):
                global_actions = country_actions.get("global")
                if isinstance(global_actions, dict):
                    global_actions["recommendation"] = ""
                    global_actions["inventory_recommendation"] = ""
                    global_actions["ads_recommendation"] = ""

    global_recommendations = extract_global_recommendations(
        global_ai=global_ai,
        available_countries=available_countries,
    )

    final_text = render_global_comparison_summary(
        global_ai=global_ai,
        us_result=us_result,
        uk_result=uk_result,
        period=period,
        timeline=timeline,
        year=year,
        remaining_agg=global_numeric_metrics.get("remaining_agg", {}),
        available_countries=available_countries,

        # ✅ NEW: deterministic country movement/contribution
        country_contribution_context=country_contribution_context,

        # ✅ NEW: all SKU individual global insights
        all_sku_mom=global_numeric_metrics.get("sku_mom", {}),
        focus_skus=global_numeric_metrics.get("focus_skus", []),
    )

    # ============================================================
    # 6. SAVE GLOBAL SUMMARY TO DB
    # ============================================================
    save_summary_to_db({
        "user_id": user_id,
        "country": "global",
        "marketplace_id": marketplace_id,
        "period": period,
        "timeline": timeline,
        "year": year,
        "summary": final_text,
        "recommendations": json.dumps({
        "global_ai": global_ai,
        "overall_recommendation": global_ai.get("global_overall_recommendation", ""),

        # Frontend-compatible recommendation object
        "frontend_recommendations": global_recommendations,

        "mapped_product_count": len(mapped_product_journeys),
        "metrics_debug": metrics_debug,
        "available_countries": available_countries,

        "allow_global_recommendations": allow_global_recommendations,

        "metrics": metrics,

        # ✅ NEW
        "country_contribution_context": country_contribution_context,

        "inventory_alerts": inventory_alerts_by_country,
        "objectives": objectives_by_country,
    }),
        "upsert": True,
    })

    # ============================================================
    # 7. RETURN FRESH AI RESPONSE
    # ============================================================
    return {
        "summary": final_text,
        "scope": "global",
        "source": "ai",
        "global_ai": global_ai,
        "overall_recommendation": global_ai.get("global_overall_recommendation", ""),
        "recommendations": global_recommendations,
        "mapped_product_count": len(mapped_product_journeys),
        "available_countries": available_countries,

        "allow_recommendations": allow_global_recommendations,

        "metrics": metrics,

        # ✅ NEW
        "country_contribution_context": country_contribution_context,

        "inventory_alerts": inventory_alerts_by_country,

        "objectives": objectives_by_country,

        "comparison": {
            "period": period,
            "timeline": timeline,
            "year": year,
            "period_label": period_label(period, timeline, year),
        },

        "metrics_debug": metrics_debug,
    }




def build_global_table_name(user_id: int, period: str, timeline: str, year: int) -> str:
    """
    Builds actual GLOBAL table name.

    Monthly:
    skuwisemonthly_{user_id}_global_{month}{year}_table
    Example:
    skuwisemonthly_123_global_april2026_table
    """

    if period == "monthly":
        mn = month_name_from_timeline(timeline)
        return f"skuwisemonthly_{user_id}_global_{mn}{year}_table"

    if period == "quarterly":
        q = int(str(timeline).replace("Q", ""))
        return f"quarter{q}_{user_id}_global_{year}_table"

    if period == "yearly":
        return f"skuwiseyearly_{user_id}_global_{year}_table"

    raise ValueError("Invalid period")


def fetch_global_precalc_table(user_id: int, period: str, timeline: str, year: int) -> pd.DataFrame:
    """
    Fetches actual GLOBAL precalc table.

    Used for:
    - selected-period global numeric values
    - previous-period global numeric values
    """

    table = build_global_table_name(
        user_id=user_id,
        period=period,
        timeline=timeline,
        year=year,
    )

    query = f'SELECT * FROM public."{table}"'

    try:
        return pd.read_sql(query, phormula_engine)
    except Exception:
        return pd.DataFrame()


def build_country_usd_table_name(user_id: int, country: str, period: str, timeline: str, year: int) -> str:
    """
    Builds USD-normalized country table name.

    Monthly:
    skuwisemonthly_{user_id}_uk_usd_{month}{year}
    skuwisemonthly_{user_id}_us_usd_{month}{year}

    Example:
    skuwisemonthly_123_uk_usd_april2026
    skuwisemonthly_123_us_usd_april2026
    """

    c = str(country).lower()

    if period == "monthly":
        mn = month_name_from_timeline(timeline)
        return f"skuwisemonthly_{user_id}_{c}_usd_{mn}{year}"

    if period == "quarterly":
        q = int(str(timeline).replace("Q", ""))
        return f"quarter{q}_{user_id}_{c}_usd_{year}_table"

    if period == "yearly":
        return f"skuwiseyearly_{user_id}_{c}_usd_{year}_table"

    raise ValueError("Invalid period")


def fetch_country_usd_precalc_table(
    user_id: int,
    country: str,
    period: str,
    timeline: str,
    year: int
) -> pd.DataFrame:
    """
    Fetches USD-normalized country precalc table.

    Used only for global US vs UK comparison numbers.
    """

    table = build_country_usd_table_name(
        user_id=user_id,
        country=country,
        period=period,
        timeline=timeline,
        year=year,
    )

    query = f'SELECT * FROM public."{table}"'

    try:
        return pd.read_sql(query, phormula_engine)
    except Exception:
        return pd.DataFrame()

SUPPORTED_GLOBAL_COUNTRIES = ["us", "uk"]


def get_available_global_countries(
    *,
    user_id: int,
    period: str,
    timeline: str,
    year: int
) -> list[str]:
    """
    Returns only countries that actually have selected-period data.
    Example:
    - only UK connected -> ["uk"]
    - only US connected -> ["us"]
    - both connected -> ["us", "uk"]
    """
    available = []

    for country in SUPPORTED_GLOBAL_COUNTRIES:
        df = fetch_precalc_table(
            user_id=user_id,
            country=country,
            period=period,
            timeline=timeline,
            year=year,
        )

        if not df.empty:
            available.append(country)

    return available

def normalize_global_ai_country_action_keys(global_ai: dict) -> dict:
    """
    Keeps frontend compatibility by forcing country_actions keys to lowercase.
    Example:
    country_actions["UK"] -> country_actions["uk"]
    country_actions["US"] -> country_actions["us"]
    """
    if not isinstance(global_ai, dict):
        return global_ai

    for item in global_ai.get("product_journey_comparison", []):
        if not isinstance(item, dict):
            continue

        country_actions = item.get("country_actions")
        if not isinstance(country_actions, dict):
            continue

        normalized_actions = {}

        for country_key, actions in country_actions.items():
            normalized_actions[str(country_key).lower()] = actions

        item["country_actions"] = normalized_actions

    return global_ai


def extract_global_recommendations(global_ai: dict, available_countries: list[str]) -> dict:
    """
    Returns frontend-friendly recommendations without changing frontend.
    Shape is simple:
    {
      "Product Name": {
        "recommendation": "...",
        "inventory_recommendation": "...",
        "ads_recommendation": "..."
      },
      "Other SKUs": {...}
    }
    """
    if not isinstance(global_ai, dict):
        return {}

    output = {}

    for item in global_ai.get("product_journey_comparison", []):
        if not isinstance(item, dict):
            continue

        product_name = item.get("product_name") or "Unknown Product"
        country_actions = item.get("country_actions") or {}

        for country in available_countries:
            actions = country_actions.get(country) or {}

            if not isinstance(actions, dict):
                continue

            if any(str(v).strip() for v in actions.values() if v):
                output[product_name] = {
                    "recommendation": actions.get("recommendation", ""),
                    "inventory_recommendation": actions.get("inventory_recommendation", ""),
                    "ads_recommendation": actions.get("ads_recommendation", ""),
                }
                break

    other_skus_comparison = global_ai.get("other_skus_comparison") or {}
    other_actions = (
        other_skus_comparison
        .get("country_actions", {})
        .get("global", {})
    )

    if isinstance(other_actions, dict) and any(str(v).strip() for v in other_actions.values() if v):
        output["Other SKUs"] = {
            "recommendation": other_actions.get("recommendation", ""),
            "inventory_recommendation": other_actions.get("inventory_recommendation", ""),
            "ads_recommendation": other_actions.get("ads_recommendation", ""),
        }

    return output
