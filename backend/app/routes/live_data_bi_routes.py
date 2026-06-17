from flask import Blueprint, request, jsonify
import jwt
import os
import numpy as np
from sqlalchemy import create_engine
from dotenv import load_dotenv
from config import Config
from calendar import month_abbr, monthrange
from datetime import date, datetime, timedelta
from openai import OpenAI
import json
from sqlalchemy import text
from calendar import month_name
import pandas as pd
from dateutil.relativedelta import relativedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from app.utils.live_bi_utils import ( build_movement_context, generate_sku_inventory_flags, build_rolling_monthly_series, compute_total_asp, compute_total_unit_profitability, fetch_sku_product_mapping, fetch_skuwisemonthly_ads_cm2_current_month, fetch_user_objective, generate_inventory_alerts_for_all_skus, get_mtd_and_prev_ranges,fetch_previous_period_data,fetch_current_mtd_data,calculate_growth,aggregate_totals,build_segment_total_row,build_sku_context,build_ai_summary,generate_live_insight,fetch_historical_skus_last_6_months, render_live_recommended_action, render_portfolio_inventory_block,round_numeric_values, run_inventory_ai_summary, run_live_prompt_1_5_summary, run_live_prompt_1_analysis, totals_from_daily_series,construct_prev_table_name,compute_sku_metrics_from_df,
compute_inventory_coverage_ratio,fetch_estimated_storage_cost_next_month,fetch_first_seen_sku_date,fetch_inventory_aged_by_user,build_portfolio_inventory_alerts, build_global_journey_comparison_for_product, generate_live_insight_with_app_context, fetch_new_skus_from_products_open_date, fetch_current_ai_values_from_skuwisemonthly,get_ai_refresh_slot)
from app.utils.email_utils import (send_live_bi_email,get_user_email_and_name_by_id,has_recent_bi_email,mark_bi_email_sent,)
from app.utils.monthwise_ai_summary_utils import run_prompt_2_strategy
from app.utils.token_utils import get_effective_user_id_from_token
from app.utils.uk_time_series_utils import build_rolling_sku_series,build_remaining_skus_time_series
from app.utils.uk_prompts_utils import get_excel_recommendation_from_live_context
import hashlib
import json
from flask import current_app
from datetime import datetime, date
from app import db
from app.models.user_models import LiveAISummary

# -----------------------------------------------------------------------------
# ENV / DB SETUP
# -----------------------------------------------------------------------------

load_dotenv()
SECRET_KEY = Config.SECRET_KEY

db_url = os.getenv("DATABASE_URL")
db_url2 = os.getenv("DATABASE_AMAZON_URL")
db_url1 = os.getenv('DATABASE_ADMIN_URL') or db_url  # fallback

ADMIN_ENGINE = create_engine(db_url1, pool_pre_ping=True)

engine_hist = create_engine(db_url)
engine_live = create_engine(db_url2)

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
oa_client = OpenAI(api_key=OPENAI_API_KEY)
# simple process-level debounce (survives hot reload)
_SENT_EMAIL_CACHE = set()


live_data_bi_bp = Blueprint("live_data_bi_bp", __name__)



# -----------------------------------------------------------------------------
# MAIN ROUTE: LIVE MTD vs PREVIOUS-MONTH-SAME-PERIOD BI
# # -----------------------------------------------------------------------------
def align_prev_curr_by_sku(prev_data, curr_data):
    prev_df = pd.DataFrame(prev_data)
    curr_df = pd.DataFrame(curr_data)

    # ---------------------------
    # HARD GUARD: SKU not ready yet
    # ---------------------------
    if "sku" not in prev_df.columns or "sku" not in curr_df.columns:
        # Data is still warming up (async pipelines)
        return [], []

    # Guard: both empty after ensuring sku exists
    if prev_df.empty and curr_df.empty:
        return [], []

    def normalize_sku(x):
        if x is None:
            return None
        try:
            x = str(x)
        except Exception:
            return None
        x = x.strip()
        if x.lower() in ("", "nan", "none", "null"):
            return None
        return x

    # ---- normalize SKU safely ----
    for df in (prev_df, curr_df):
        df["sku"] = df["sku"].apply(normalize_sku)

    prev_df = prev_df[prev_df["sku"].notna()]
    curr_df = curr_df[curr_df["sku"].notna()]

    # ---- UNION of SKUs ----
    all_skus = set(prev_df["sku"]) | set(curr_df["sku"])

    # Guard: no valid SKUs yet
    if not all_skus:
        return [], []

    base = pd.DataFrame({"sku": list(all_skus)})

    prev_df = base.merge(prev_df, on="sku", how="left")
    curr_df = base.merge(curr_df, on="sku", how="left")

    NUM_COLS = [
        "quantity",
        "net_sales",
        "product_sales",
        "profit",
        "asp",
        "unit_wise_profitability",
        "sales_mix",
    ]

    for c in NUM_COLS:
        if c in prev_df.columns:
            prev_df[c] = prev_df[c].fillna(0.0)
        if c in curr_df.columns:
            curr_df[c] = curr_df[c].fillna(0.0)

    return (
        prev_df.to_dict(orient="records"),
        curr_df.to_dict(orient="records"),
    )

def remove_total_rows(items):
    clean = []

    for r in items or []:
        sku = str(r.get("sku") or "").strip().upper()
        product_name = str(r.get("product_name") or "").strip().lower()

        if sku in ("TOTAL", "GRAND_TOTAL"):
            continue

        if product_name in ("total", "grand total"):
            continue

        clean.append(r)

    return clean


def align_prev_curr_by_product_name(prev_data, curr_data):
    prev_df = pd.DataFrame(prev_data or [])
    curr_df = pd.DataFrame(curr_data or [])

    if "product_name" not in prev_df.columns:
        prev_df["product_name"] = None

    if "product_name" not in curr_df.columns:
        curr_df["product_name"] = None

    def clean_name(x):
        if x is None:
            return None

        x = str(x).strip().lower()

        if x in ("", "0", "nan", "none", "null", "total", "grand total"):
            return None

        return x

    prev_df["product_name"] = prev_df["product_name"].apply(clean_name)
    curr_df["product_name"] = curr_df["product_name"].apply(clean_name)

    prev_df = prev_df[prev_df["product_name"].notna()]
    curr_df = curr_df[curr_df["product_name"].notna()]

    all_products = set(prev_df["product_name"]) | set(curr_df["product_name"])

    if not all_products:
        return [], []

    base = pd.DataFrame({"product_name": list(all_products)})

    prev_df = base.merge(prev_df, on="product_name", how="left")
    curr_df = base.merge(curr_df, on="product_name", how="left")

    numeric_cols = [
        "quantity",
        "net_sales",
        "product_sales",
        "gross_sales",
        "profit",
        "asp",
        "unit_wise_profitability",
        "sales_mix",
        "advertising",
        "platform_fee",
        "ads_spend",
        "cm2_profit",
        "selling_fees",
        "fba_fees",
        "tax_and_credits",
    ]

    for c in numeric_cols:
        if c in prev_df.columns:
            prev_df[c] = pd.to_numeric(prev_df[c], errors="coerce").fillna(0.0)

        if c in curr_df.columns:
            curr_df[c] = pd.to_numeric(curr_df[c], errors="coerce").fillna(0.0)

    return (
        prev_df.replace({np.nan: None}).to_dict(orient="records"),
        curr_df.replace({np.nan: None}).to_dict(orient="records"),
    )

def build_global_country_recommendations(
    *,
    user_id,
    country,
    prev_items,
    curr_items,
    user_objective,
    currency_symbol="$",
    anchor_year=None,
    anchor_month=None,
    analysis=None,
):
    """
    Builds country-specific recommendations for global Live BI.

    Important:
    - Recommendations stay separate for UK and US.
    - recommendation comes from Excel rule override, same as single-country flow.
    - ads_recommendation, inventory_recommendation, journey_summary come from Prompt-2.
    - rendered_action is included for UI compatibility.
    """

    prev_aligned, curr_aligned = align_prev_curr_by_sku(
        prev_items,
        curr_items,
    )

    if not curr_aligned:
        return {}

    growth_data = calculate_growth(
        prev_aligned,
        curr_aligned,
        key="sku",
    )

    existing_sorted = sorted(
        [
            r for r in growth_data
            if r.get("Sales Mix (Current)") is not None
        ],
        key=lambda x: x["Sales Mix (Current)"],
        reverse=True,
    )

    total_sales_mix = sum(
        r.get("Sales Mix (Current)") or 0
        for r in existing_sorted
    )

    cumulative = 0.0
    top_80 = []

    for r in existing_sorted:
        mix = r.get("Sales Mix (Current)") or 0
        proportion = cumulative / total_sales_mix if total_sales_mix else 0

        if proportion <= 0.8:
            top_80.append(r)
            cumulative += mix

    growth_intent = user_objective.get("growth_intent", "balanced")
    profit_priority = user_objective.get("profit_priority", "protect_growth")

    # -------------------------------------------------
    # 1. Build live context, same style as single country
    # -------------------------------------------------
    sku_live_context = []

    for growth_row in top_80:
        sku = growth_row.get("sku")
        if not sku:
            continue

        sku_live_context.append({
            "sku": sku,
            "quantity": {
                "previous": growth_row.get("quantity_prev"),
                "current": growth_row.get("quantity_curr"),
            },
            "asp": {
                "previous": growth_row.get("asp_prev"),
                "current": growth_row.get("asp_curr"),
            },
            "net_sales": {
                "previous": growth_row.get("net_sales_prev"),
                "current": growth_row.get("net_sales_curr"),
            },
            "cm1_profit": {
                "previous": growth_row.get("profit_prev"),
                "current": growth_row.get("profit_curr"),
            },
            "profit_per_unit": {
                "previous": growth_row.get("unit_wise_profitability_prev"),
                "current": growth_row.get("unit_wise_profitability_curr"),
            },
            "movement_intensity": {
                "units": (growth_row.get("Unit Growth (%)") or {}).get("value"),
                "asp": (growth_row.get("ASP Growth (%)") or {}).get("value"),
                "net_sales": (growth_row.get("Net Sales Growth (%)") or {}).get("value"),
                "cm1_profit": (growth_row.get("CM1 Profit Impact (%)") or {}).get("value"),
                "profit_per_unit": (growth_row.get("Profit Per Unit (%)") or {}).get("value"),
            },
        })

    # -------------------------------------------------
    # 2. Excel recommendations, same as single-country override
    # -------------------------------------------------
    excel_live_recommendations = {}

    for row in sku_live_context:
        sku = row.get("sku")
        if not sku:
            continue

        rec = get_excel_recommendation_from_live_context(
            sku_live_row=row,
            growth_intent=growth_intent,
            profit_priority=profit_priority,
        )

        if rec is None:
            rec_text = "Monitor performance"
        else:
            try:
                rec_text = "Monitor performance" if pd.isna(rec) else str(rec)
            except Exception:
                rec_text = str(rec)

        excel_live_recommendations[sku] = rec_text

    # -------------------------------------------------
    # 3. Inventory flags for this country
    # -------------------------------------------------
    try:
        sku_inventory_flags = generate_sku_inventory_flags(
            user_id=user_id,
            country=country,
            focus_skus=[r.get("sku") for r in top_80],
        )
    except Exception as e:
        print("[WARN] Failed to build global country inventory flags:", country, e)
        sku_inventory_flags = {}

    # -------------------------------------------------
    # 4. Ads context for this country
    # -------------------------------------------------
    sku_ads_context = []

    for r in top_80:
        sku = r.get("sku")
        if not sku:
            continue

        sku_ads_context.append({
            "sku": sku,
            "ads_spend_curr": r.get("ads_spend_curr", 0),
            "acos_curr": r.get("acos_curr", 0),
            "cm2_profit_curr": r.get("cm2_profit_curr", 0),
            "cm2_margin_curr": r.get("cm2_margin_curr", 0),
            "net_sales_curr": r.get("net_sales_curr", 0),
        })

    ads_monthly = {
        "total_ads_spend": sum(float(r.get("ads_spend_curr") or 0) for r in top_80),
        "total_cm2_profit": sum(float(r.get("cm2_profit_curr") or 0) for r in top_80),
    }

    # -------------------------------------------------
    # 5. SKU time series for journey_summary
    # -------------------------------------------------
    sku_time_series = {}

    for r in top_80:
        sku = r.get("sku")
        if not sku:
            continue

        try:
            sku_time_series[sku] = build_rolling_sku_series(
                user_id=user_id,
                country=country,
                sku=sku,
                anchor_year=anchor_year,
                anchor_month=anchor_month,
            )
        except Exception as e:
            print("[WARN] Failed to build global country time series:", country, sku, e)

    # -------------------------------------------------
    # 6. Prompt-2 strategy for ads/inventory/journey
    # -------------------------------------------------
    strategy_parsed = {}

    try:
        strategy_raw = run_prompt_2_strategy(
            analysis_insights=analysis or {},
            objective_v2=user_objective,
            focus_skus=[r.get("sku") for r in top_80],
            sku_time_series=sku_time_series,
            inventory_alerts={},
            sku_inventory_flags=sku_inventory_flags,
            country=country,
            sku_ads_context=sku_ads_context,
            sku_live_context=sku_live_context,
            ads_monthly=ads_monthly,
            remaining_skus_context={},
        )

        strategy_parsed = json.loads(strategy_raw) if strategy_raw else {}

    except Exception as e:
        print("[AI ERROR] Global country strategy generation failed:", country, e)
        strategy_parsed = {}

    sku_strategy_actions = strategy_parsed.get("sku_actions", {}) or {}

    # -------------------------------------------------
    # 7. Build final country recommendations
    # -------------------------------------------------
    recommended_actions = {}

    for growth_row in top_80:
        sku = growth_row.get("sku")
        if not sku:
            continue

        sku_strategy = sku_strategy_actions.get(sku, {}) or {}

        recommendation_text = excel_live_recommendations.get(
            sku,
            sku_strategy.get("recommendation", "Monitor performance"),
        )

        def _safe_optional_text(value, fallback):
            if value is None:
                return fallback

            try:
                if pd.isna(value):
                    return fallback
            except Exception:
                pass

            value = str(value).strip()

            if value.lower() in ("", "0", "0.0", "nan", "none", "null"):
                return fallback

            return value


        def _safe_journey_list(value):
            if value is None:
                return []

            try:
                if pd.isna(value):
                    return []
            except Exception:
                pass

            if isinstance(value, list):
                return [str(x) for x in value if x]

            if isinstance(value, str):
                value = value.strip()
                if value.lower() in ("", "0", "0.0", "nan", "none", "null"):
                    return []
                return [value]

            return []


        ads_recommendation = _safe_optional_text(
            sku_strategy.get("ads_recommendation"),
            fallback="not coming.",
        )

        inventory_recommendation = _safe_optional_text(
            sku_strategy.get("inventory_recommendation"),
            fallback="not coming.",
        )

        journey_summary = _safe_journey_list(
            sku_strategy.get("journey_summary")
        )

        rendered_action = render_live_recommended_action(
            growth_row=growth_row,
            recommendation=recommendation_text,
            ads_recommendation=ads_recommendation,
            inventory_recommendation=inventory_recommendation,
            journey_summary=journey_summary,
            currency_symbol=currency_symbol,
        )

        recommended_actions[sku] = {
            "country": country,
            "sku": sku,
            "product_name": growth_row.get("product_name"),

            # separate fields
            "recommendation": recommendation_text,
            "ads_recommendation": ads_recommendation,
            "inventory_recommendation": inventory_recommendation,
            "journey_summary": journey_summary,

            # full old-style rendered text
            "rendered_action": rendered_action,

            # useful for frontend cards
            "growth_row": growth_row,
        }

    return recommended_actions

def build_global_product_journey_from_country_actions(
    uk_actions,
    us_actions,
):
    """
    Combines UK + US journey_summary by product_name.

    Output shape:
    {
        "passion fruit": {
            "product_name": "Passion Fruit",
            "journey_comparison": [...],
            "uk": {...},
            "us": {...}
        }
    }
    """

    product_journey = {}

    def _safe_product_name(value, fallback):
        if value is None:
            return fallback

        try:
            if pd.isna(value):
                return fallback
        except Exception:
            pass

        value = str(value).strip()

        if value.lower() in ("", "0", "0.0", "nan", "none", "null"):
            return fallback

        return value

    def _safe_journey_list(value):
        if value is None:
            return []

        try:
            if pd.isna(value):
                return []
        except Exception:
            pass

        if isinstance(value, list):
            return [str(x) for x in value if x]

        if isinstance(value, str):
            value = value.strip()
            if value.lower() in ("", "0", "0.0", "nan", "none", "null"):
                return []
            return [value]

        return []

    def add_country_actions(country, actions):
        for sku, action in (actions or {}).items():
            product_name = _safe_product_name(
                action.get("product_name"),
                fallback=str(sku),
            )

            product_key = product_name.strip().lower()

            if product_key not in product_journey:
                product_journey[product_key] = {
                    "product_name": product_name,
                    "uk": {},
                    "us": {},
                    "journey_comparison": [],
                }

            journey_summary = _safe_journey_list(action.get("journey_summary"))

            product_journey[product_key][country][sku] = {
                "sku": sku,
                "journey_summary": journey_summary,
                "recommendation": action.get("recommendation"),
                "ads_recommendation": action.get("ads_recommendation"),
                "inventory_recommendation": action.get("inventory_recommendation"),
                "growth_row": action.get("growth_row", {}),
            }

    add_country_actions("uk", uk_actions)
    add_country_actions("us", us_actions)

    for product_key, product_data in product_journey.items():
        product_data["journey_comparison"] = build_global_journey_comparison_for_product(
            product_name=product_data.get("product_name"),
            uk_data=product_data.get("uk", {}),
            us_data=product_data.get("us", {}),
        )

    return product_journey


def build_cm1_profit_pie_slices(
    rows,
    min_named=5,
    pareto_threshold=0.8,
    max_named=10,
    others_label="Others",
):
    """
    rows: list of dicts that must contain:
      - product_name (or name)
      - profit_curr
      - profit_prev
    Returns a list of slices:
      [{name, profit_curr, profit_prev, pct, delta_pct}, ...]
    """

    def safe_name(r):
        val = r.get("product_name") or r.get("name")
        if val is None:
            return ""
        return str(val).strip()

    def is_others(n: str) -> bool:
        return (n or "").strip().lower() == others_label.lower()

    items = []
    for r in rows:
        name = safe_name(r)
        if not name:
            continue
        items.append({
            "name": name,
            "profit_curr": float(r.get("profit_curr") or 0),
            "profit_prev": float(r.get("profit_prev") or 0),
        })

    # Total profit used for pct
    total_profit = sum(x["profit_curr"] for x in items) or 0.0

    # Sort by current profit desc
    items_sorted = sorted(items, key=lambda x: x["profit_curr"], reverse=True)

    # --- Pareto pick (80% of profit), BUT don't let a real "Others" be a named slice
    pareto_pick = []
    running = 0.0
    for it in items_sorted:
        if len(pareto_pick) >= max_named:
            break
        if is_others(it["name"]):  # reserve label for aggregate bucket
            continue
        pareto_pick.append(it)
        running += it["profit_curr"]
        if total_profit > 0 and (running / total_profit) >= pareto_threshold:
            break

    # Fallback if pareto yields nothing
    if not pareto_pick:
        pareto_pick = [it for it in items_sorted if not is_others(it["name"])][:min_named]

    chosen = list(pareto_pick)

    # --- Enforce at least min_named named slices (skip real "Others" name)
    if len(chosen) < min_named:
        chosen_names = {x["name"] for x in chosen}
        for it in items_sorted:
            if len(chosen) >= min_named:
                break
            if is_others(it["name"]):
                continue
            if it["name"] not in chosen_names:
                chosen.append(it)
                chosen_names.add(it["name"])

    # If total distinct non-others items <= min_named, just show them all
    non_others = [it for it in items_sorted if not is_others(it["name"])]
    if len(non_others) <= min_named:
        chosen = non_others

    chosen_names = {x["name"] for x in chosen}

    # --- Everything not chosen goes to Others (including any literal "Others" SKU rows)
    others_items = [it for it in items_sorted if it["name"] not in chosen_names]
    others_curr = sum(x["profit_curr"] for x in others_items)
    others_prev = sum(x["profit_prev"] for x in others_items)

    def delta_pct(curr, prev):
        if prev == 0:
            return None
        return ((curr - prev) / abs(prev)) * 100.0

    slices = []
    for it in chosen:
        pct = (it["profit_curr"] / total_profit * 100.0) if total_profit else 0.0
        slices.append({
            "name": it["name"],
            "profit_curr": it["profit_curr"],
            "profit_prev": it["profit_prev"],
            "pct": pct,
            "delta_pct": delta_pct(it["profit_curr"], it["profit_prev"]),
        })

    if others_curr != 0:
        pct = (others_curr / total_profit * 100.0) if total_profit else 0.0
        slices.append({
            "name": others_label,
            "profit_curr": others_curr,
            "profit_prev": others_prev,
            "pct": pct,
            "delta_pct": delta_pct(others_curr, others_prev),
        })

    return {
        "total_profit_curr": total_profit,
        "min_named": min_named,
        "pareto_threshold": pareto_threshold,
        "slices": slices,
    }


def generate_objective_hash(obj):
    return hashlib.md5(
        json.dumps(obj, sort_keys=True).encode()
    ).hexdigest()

def safe_json_load(val):
    try:
        return json.loads(val) if val else {}
    except Exception:
        return {}

# def get_cached_live_ai(user_id, country, start_date, end_date, objective_hash):

#     record = LiveAISummary.query.filter_by(
#         user_id=user_id,
#         country=country,
#         start_date=start_date,
#         end_date=end_date
#     ).first()

#     if not record:
#         return None

#     # objective changed
#     if record.objective_hash != objective_hash:
#         return None

#     # next day rerun
#     if not record.created_at or record.created_at.date() < date.today():
#         return None

#     return {
#     "analysis": safe_json_load(record.analysis),
#     "summary": safe_json_load(record.summary),
#     "strategy": safe_json_load(record.strategy)
#     }

def get_cached_live_ai(user_id, country, start_date, end_date, objective_hash):

    record = LiveAISummary.query.filter_by(
        user_id=user_id,
        country=country,
        start_date=start_date,
        end_date=end_date
    ).first()

    if not record:
        return None

    # objective changed
    if record.objective_hash != objective_hash:
        return None

    # IMPORTANT:
    # Do not expire cache daily.
    # The route controls refresh by using ai_refresh_slot as end_date.
    # Example:
    # 1st-7th  -> end_date = 1st
    # 8th-14th -> end_date = 8th
    # etc.

    return {
        "analysis": safe_json_load(record.analysis),
        "summary": safe_json_load(record.summary),
        "strategy": safe_json_load(record.strategy),
    }



def save_live_ai_cache(
    user_id,
    country,
    start_date,
    end_date,
    objective_hash,
    analysis,
    summary,
    strategy
):

    record = LiveAISummary.query.filter_by(
        user_id=user_id,
        country=country,
        start_date=start_date,
        end_date=end_date
    ).first()

    if record:
        record.analysis = json.dumps(analysis)
        record.summary = json.dumps(summary)
        record.strategy = json.dumps(strategy)
        record.objective_hash = objective_hash
        record.created_at = datetime.utcnow()

    else:
        record = LiveAISummary(
            user_id=user_id,
            country=country,
            start_date=start_date,
            end_date=end_date,
            objective_hash=objective_hash,
            analysis=json.dumps(analysis),
            summary=json.dumps(summary),
            strategy=json.dumps(strategy)
        )

        db.session.add(record)

    db.session.commit()



@live_data_bi_bp.route("/live_mtd_bi", methods=["GET"])
def live_mtd_vs_previous():
    auth_header = request.headers.get("Authorization")
    if not auth_header or not auth_header.startswith("Bearer "):
        return jsonify({"error": "Authorization token is missing or invalid"}), 401

    token = auth_header.split(" ")[1]

    try:
        # ✅ PRE-INITIALIZE (VERY IMPORTANT)
        portfolio_recommendation = None
        sku_strategy_actions = {}
        remaining_skus_reco = None
        remaining_skus_journey = None
        remaining_skus_ads_reco = None
        remaining_skus_inventory_reco = None

        payload, user_id, member_id = get_effective_user_id_from_token(token)
        user_id = payload.get("user_id")
        if not user_id:
            return jsonify({"error": "Invalid token payload: user_id missing"}), 401

        country = (request.args.get("countryName", "uk") or "uk").strip().lower()
        as_of = request.args.get("as_of")

        # ---------------------------
        # USER OBJECTIVE (SHARED WITH HISTORIC BI)
        # ---------------------------
        user_objective = fetch_user_objective(user_id, country)
    
        # ---------------------------
        # DATE RANGE
        # ---------------------------
        start_day_str = request.args.get("start_day")
        end_day_str = request.args.get("end_day")
        try:
            start_day = int(start_day_str) if start_day_str else None
            end_day = int(end_day_str) if end_day_str else None
        except ValueError:
            start_day = None
            end_day = None

        generate_ai_insights = (
            request.args.get("generate_ai_insights", "false").lower()
            in ("true", "1", "yes")
        )

        ranges = get_mtd_and_prev_ranges(
            as_of=as_of,
            start_day=start_day,
            end_day=end_day,
        )
        prev_start = ranges["previous"]["start"]
        prev_end = ranges["previous"]["end"]
        curr_start = ranges["current"]["start"]
        curr_end = ranges["current"]["end"]

        # --------------------------------------------
        # AI REFRESH SLOT
        # AI insights/recommendations refresh only on:
        # 1st, 8th, 15th, 22nd, 29th.
        # Everything else still uses curr_end normally.
        # --------------------------------------------
        today = ranges["meta"]["today"]
        ai_refresh_slot = get_ai_refresh_slot(today)

        # --------------------------------------------
        # HISTORIC MOVEMENT CONTEXT (24 MONTHS)
        # --------------------------------------------

        # Anchor on LAST COMPLETED MONTH (never current MTD)
        anchor_year = today.year
        anchor_month = today.month - 1
        if anchor_month == 0:
            anchor_month = 12
            anchor_year -= 1

        movement_series = build_rolling_monthly_series(
            user_id=user_id,
            country=country,
            anchor_year=anchor_year,
            anchor_month=anchor_month,
        )

        movement_context = build_movement_context(movement_series)



        # FULL previous month (charts)
        prev_full_start = date(
            ranges["meta"]["previous_year"],
            ranges["meta"]["previous_month"],
            1,
        )
        last_day_prev = monthrange(prev_full_start.year, prev_full_start.month)[1]
        prev_full_end = date(prev_full_start.year, prev_full_start.month, last_day_prev)

        key_column = "sku"

        if country == "global":
            key_column = "product_name"

            from app.routes.amazon_api_routes import get_current_global_data_for_live_bi
            current_global_payload = get_current_global_data_for_live_bi(user_id)

            previous_global_payload = get_previous_global_data_for_live_bi(
                user_id=user_id,
                as_of=as_of,
                start_day=start_day,
                end_day=end_day,
            )

            curr_global_items = remove_total_rows(
                current_global_payload.get("skuwise_items_global", [])
            )
            prev_global_items = remove_total_rows(
                previous_global_payload.get("skuwise_items_global", [])
            )

            curr_uk_items = remove_total_rows(
                current_global_payload.get("skuwise_items_uk", [])
            )
            curr_us_items = remove_total_rows(
                current_global_payload.get("skuwise_items_us", [])
            )

            prev_uk_items = remove_total_rows(
                previous_global_payload.get("skuwise_items_uk", [])
            )
            prev_us_items = remove_total_rows(
                previous_global_payload.get("skuwise_items_us", [])
            )

            prev_data_aligned, curr_data = align_prev_curr_by_product_name(
                prev_global_items,
                curr_global_items,
            )

            if not curr_data:
                return jsonify({
                    "status": "loading",
                    "message": "Global data is still syncing. Please wait a few seconds."
                }), 202

            growth_data = calculate_growth(
                prev_data_aligned,
                curr_data,
                key="product_name",
            )

            prev_keys = {
                r.get("product_name")
                for r in prev_data_aligned
                if r.get("product_name")
            }

            existing = [
                r for r in growth_data
                if r.get("product_name") in prev_keys
                and r.get("Sales Mix (Current)") is not None
            ]

            existing_sorted = sorted(
                existing,
                key=lambda x: x["Sales Mix (Current)"],
                reverse=True,
            )

            # -------------------------------------------------
            # GLOBAL DISPLAY RULE:
            # Show top 5 products + one combined Other SKUs card
            # -------------------------------------------------
            top_80_skus = existing_sorted[:5]
            other_skus = existing_sorted[5:]

            # -------------------------------------------------
            # GLOBAL: Combined Other SKUs card
            # -------------------------------------------------
            other_products_total_row = None

            if other_skus:
                other_product_names = {
                    r.get("product_name")
                    for r in other_skus
                    if r.get("product_name")
                }

                prev_other_products = [
                    r for r in prev_data_aligned
                    if r.get("product_name") in other_product_names
                ]

                curr_other_products = [
                    r for r in curr_data
                    if r.get("product_name") in other_product_names
                ]

                other_products_total_row = build_segment_total_row(
                    prev_other_products,
                    curr_other_products,
                    key="product_name",
                    label="Other SKUs",
                )

                if other_products_total_row:
                    other_products_total_row["product_name"] = "Other SKUs"
                    other_products_total_row["is_other_skus_card"] = True


            prev_label = (
                f"{month_abbr[prev_start.month].capitalize()}'"
                f"{str(prev_start.year)[-2:]} {prev_start.day}–{prev_end.day}"
            )

            curr_label = (
                f"{month_abbr[curr_start.month].capitalize()}'"
                f"{str(curr_start.year)[-2:]} {curr_start.day}–{curr_end.day}"
            )

            prev_totals = aggregate_totals(prev_data_aligned)
            curr_totals = aggregate_totals(curr_data)

            prev_totals["total_asp"] = compute_total_asp(prev_data_aligned)
            curr_totals["total_asp"] = compute_total_asp(curr_data)

            prev_totals["unit_wise_profitability"] = compute_total_unit_profitability(
                prev_data_aligned
            )
            curr_totals["unit_wise_profitability"] = compute_total_unit_profitability(
                curr_data
            )

            currency = {
                "symbol": "$",
                "code": "USD",
            }

            # # -------------------------------------------------
            # # GLOBAL: Portfolio inventory blocks by available country only
            # # -------------------------------------------------
            # available_countries = (
            #     current_global_payload.get("available_countries")
            #     or previous_global_payload.get("available_countries")
            #     or []
            # )

            # try:
            #     inv_df = fetch_inventory_aged_by_user(user_id, country)
            #     portfolio_inventory_alerts_uk = {}
            #     portfolio_inventory_alerts_us = {}

            #     portfolio_inventory_block_uk = ""
            #     portfolio_inventory_block_us = ""

            #     if "uk" in available_countries:
            #         portfolio_inventory_alerts_uk = build_portfolio_inventory_alerts(
            #             inv_df,
            #             user_id=user_id,
            #             country="uk",
            #         )

            #         portfolio_inventory_block_uk = render_portfolio_inventory_block(
            #             inventory_alerts=portfolio_inventory_alerts_uk,
            #             currency_symbol="£",
            #         )

            #     if "us" in available_countries:
            #         portfolio_inventory_alerts_us = build_portfolio_inventory_alerts(
            #             inv_df,
            #             user_id=user_id,
            #             country="us",
            #         )

            #         portfolio_inventory_block_us = render_portfolio_inventory_block(
            #             inventory_alerts=portfolio_inventory_alerts_us,
            #             currency_symbol="$",
            #         )

            # except Exception as e:
            #     print("[WARN] Failed to build global portfolio inventory blocks:", e)

            #     portfolio_inventory_alerts_uk = {}
            #     portfolio_inventory_alerts_us = {}

            #     portfolio_inventory_block_uk = ""
            #     portfolio_inventory_block_us = ""

            # -------------------------------------------------
            # GLOBAL: Portfolio inventory blocks by available country only
            # Keep response structure same:
            # portfolio_inventory_block = {"uk": "...", "us": "..."}
            # portfolio_inventory_alerts = {"uk": {...}, "us": {...}}
            # -------------------------------------------------
            available_countries = (
                current_global_payload.get("available_countries")
                or previous_global_payload.get("available_countries")
                or []
            )

            try:
                portfolio_inventory_alerts_uk = {}
                portfolio_inventory_alerts_us = {}

                portfolio_inventory_block_uk = ""
                portfolio_inventory_block_us = ""

                if "uk" in available_countries:
                    inv_df_uk = fetch_inventory_aged_by_user(user_id, "uk")

                    portfolio_inventory_alerts_uk = build_portfolio_inventory_alerts(
                        inv_df_uk,
                        user_id=user_id,
                        country="uk",
                    )

                    portfolio_inventory_block_uk = render_portfolio_inventory_block(
                        inventory_alerts=portfolio_inventory_alerts_uk,
                        currency_symbol="£",
                    )

                if "us" in available_countries:
                    inv_df_us = fetch_inventory_aged_by_user(user_id, "us")

                    portfolio_inventory_alerts_us = build_portfolio_inventory_alerts(
                        inv_df_us,
                        user_id=user_id,
                        country="us",
                    )

                    portfolio_inventory_block_us = render_portfolio_inventory_block(
                        inventory_alerts=portfolio_inventory_alerts_us,
                        currency_symbol="$",
                    )

            except Exception as e:
                print("[WARN] Failed to build global portfolio inventory blocks:", e)

                portfolio_inventory_alerts_uk = {}
                portfolio_inventory_alerts_us = {}

                portfolio_inventory_block_uk = ""
                portfolio_inventory_block_us = ""

            sku_context = build_sku_context(
                top_80_skus + ([other_products_total_row] if other_products_total_row else []),
                max_items=6,
            )

            global_summary_products = (
                top_80_skus + ([other_products_total_row] if other_products_total_row else [])
            )

            payload_ai = build_ai_summary(
                prev_totals,
                curr_totals,
                global_summary_products,
                prev_label,
                curr_label,
                sku_context=sku_context,
                inventory_signals={},
                portfolio_inventory_alerts={
                    "uk": portfolio_inventory_alerts_uk,
                    "us": portfolio_inventory_alerts_us,
                },
                prev_fee_totals=previous_global_payload.get("derived_totals_global", {}),
                curr_fee_totals=current_global_payload.get("derived_totals_global", {}),
                estimated_storage_cost_next_month=fetch_estimated_storage_cost_next_month(user_id, country),
                currency=currency,
                user_objective=user_objective,
                movement_context=movement_context,
                sku_to_product={},
                user_id=user_id,
                country="global",
                current_year=ranges["meta"]["current_year"],
                current_month=ranges["meta"]["current_month"],
            )

           
            # -------------------------------------------------
            # GLOBAL: Country split for deeper UK vs US summary
            # -------------------------------------------------
            prev_uk_aligned_for_summary, curr_uk_aligned_for_summary = align_prev_curr_by_sku(
                prev_uk_items,
                curr_uk_items,
            )

            prev_us_aligned_for_summary, curr_us_aligned_for_summary = align_prev_curr_by_sku(
                prev_us_items,
                curr_us_items,
            )

            uk_growth_for_summary = calculate_growth(
                prev_uk_aligned_for_summary,
                curr_uk_aligned_for_summary,
                key="sku",
            )

            us_growth_for_summary = calculate_growth(
                prev_us_aligned_for_summary,
                curr_us_aligned_for_summary,
                key="sku",
            )

            def _country_driver_summary(growth_rows):
                rows = growth_rows or []

                total_units_curr = sum(float(r.get("quantity_curr") or 0) for r in rows)
                total_units_prev = sum(float(r.get("quantity_prev") or 0) for r in rows)

                total_sales_curr = sum(float(r.get("net_sales_curr") or 0) for r in rows)
                total_sales_prev = sum(float(r.get("net_sales_prev") or 0) for r in rows)

                total_profit_curr = sum(float(r.get("profit_curr") or 0) for r in rows)
                total_profit_prev = sum(float(r.get("profit_prev") or 0) for r in rows)

                total_unit_profit_curr = (
                    total_profit_curr / total_units_curr
                    if total_units_curr else 0
                )

                total_unit_profit_prev = (
                    total_profit_prev / total_units_prev
                    if total_units_prev else 0
                )

                total_asp_curr = (
                    total_sales_curr / total_units_curr
                    if total_units_curr else 0
                )

                total_asp_prev = (
                    total_sales_prev / total_units_prev
                    if total_units_prev else 0
                )

                def pct(curr, prev):
                    if not prev:
                        return None
                    return ((curr - prev) / abs(prev)) * 100

                return {
                    "units": {
                        "previous": total_units_prev,
                        "current": total_units_curr,
                        "absolute_change": total_units_curr - total_units_prev,
                        "pct_change": pct(total_units_curr, total_units_prev),
                    },
                    "net_sales": {
                        "previous": total_sales_prev,
                        "current": total_sales_curr,
                        "absolute_change": total_sales_curr - total_sales_prev,
                        "pct_change": pct(total_sales_curr, total_sales_prev),
                    },
                    "cm1_profit": {
                        "previous": total_profit_prev,
                        "current": total_profit_curr,
                        "absolute_change": total_profit_curr - total_profit_prev,
                        "pct_change": pct(total_profit_curr, total_profit_prev),
                    },
                    "cm1_profit_per_unit": {
                        "previous": total_unit_profit_prev,
                        "current": total_unit_profit_curr,
                        "absolute_change": total_unit_profit_curr - total_unit_profit_prev,
                        "pct_change": pct(total_unit_profit_curr, total_unit_profit_prev),
                    },
                    "asp": {
                        "previous": total_asp_prev,
                        "current": total_asp_curr,
                        "absolute_change": total_asp_curr - total_asp_prev,
                        "pct_change": pct(total_asp_curr, total_asp_prev),
                    },
                }

            def _top_country_products(growth_rows, limit=5):
                return sorted(
                    [
                        r for r in (growth_rows or [])
                        if r.get("Sales Mix (Current)") is not None
                    ],
                    key=lambda x: x.get("Sales Mix (Current)") or 0,
                    reverse=True,
                )[:limit]

            payload_ai["country_split"] = {
                "uk": {
                    "previous": previous_global_payload.get("derived_totals_uk", {}),
                    "current": current_global_payload.get("derived_totals_uk", {}),
                    "driver_summary": _country_driver_summary(uk_growth_for_summary),
                    "top_products": _top_country_products(uk_growth_for_summary),
                },
                "us": {
                    "previous": previous_global_payload.get("derived_totals_us", {}),
                    "current": current_global_payload.get("derived_totals_us", {}),
                    "driver_summary": _country_driver_summary(us_growth_for_summary),
                    "top_products": _top_country_products(us_growth_for_summary),
                },
            }
            # -------------------------------------------------
            # GLOBAL: ACOS / TACoS + CLEAN CM1 METRICS
            # -------------------------------------------------
            def _safe_float(value, default=0.0):
                try:
                    if value is None:
                        return default
                    return float(value)
                except Exception:
                    return default


            def _pct_change(curr, prev):
                curr = _safe_float(curr)
                prev = _safe_float(prev)

                if prev == 0:
                    return 0.0

                return round(((curr - prev) / abs(prev)) * 100, 2)


            curr_global_for_summary = current_global_payload.get("derived_totals_global", {}) or {}
            prev_global_for_summary = previous_global_payload.get("derived_totals_global", {}) or {}
            aligned_global_for_summary = previous_global_payload.get("aligned_totals_global", {}) or {}

            # -----------------------------
            # CM1 CLEAN GLOBAL METRICS
            # -----------------------------
            current_units = _safe_float(curr_global_for_summary.get("quantity"))
            previous_units = _safe_float(prev_global_for_summary.get("quantity"))

            current_net_sales = _safe_float(curr_global_for_summary.get("net_sales"))
            previous_net_sales = _safe_float(
                prev_global_for_summary.get("net_sales")
                or aligned_global_for_summary.get("total_previous_net_sales")
            )

            # IMPORTANT: CM1 Profit only = profit
            current_cm1_profit = _safe_float(curr_global_for_summary.get("profit"))
            previous_cm1_profit = _safe_float(
                prev_global_for_summary.get("profit")
                or aligned_global_for_summary.get("total_previous_profit")
            )

            current_cm1_profit_per_unit = (
                current_cm1_profit / current_units
                if current_units else 0.0
            )

            previous_cm1_profit_per_unit = (
                previous_cm1_profit / previous_units
                if previous_units else 0.0
            )

            current_asp = _safe_float(curr_global_for_summary.get("asp"))
            previous_asp = _safe_float(prev_global_for_summary.get("asp"))

            if not current_asp and current_units:
                current_asp = current_net_sales / current_units

            if not previous_asp and previous_units:
                previous_asp = previous_net_sales / previous_units

            # -----------------------------
            # ACOS / TACoS
            # -----------------------------
            current_ads = _safe_float(
                curr_global_for_summary.get("advertising_fees")
                or curr_global_for_summary.get("total_ads")
            )

            previous_ads = _safe_float(
                prev_global_for_summary.get("advertising_fees")
                or aligned_global_for_summary.get("total_previous_advertising")
            )

            # Prefer dashboard TACoS / ACOS if available
            current_acos = _safe_float(
                curr_global_for_summary.get("tacos_total_advertising_cost_of_sale")
            )

            # Fallback calculation
            if not current_acos and current_net_sales:
                current_acos = round((current_ads / current_net_sales) * 100, 2)

            previous_acos = (
                round((previous_ads / previous_net_sales) * 100, 2)
                if previous_net_sales else 0.0
            )

            acos_pct_change = _pct_change(current_acos, previous_acos)

            if current_acos < previous_acos:
                acos_interpretation = "improved"
                acos_direction_text = (
                    f"ACOS/TACoS decreased from {previous_acos:.2f}% to {current_acos:.2f}%, "
                    f"meaning advertising efficiency improved by {abs(acos_pct_change):.2f}%."
                )
                acos_improvement_pct = abs(acos_pct_change)
            elif current_acos > previous_acos:
                acos_interpretation = "worsened"
                acos_direction_text = (
                    f"ACOS/TACoS increased from {previous_acos:.2f}% to {current_acos:.2f}%, "
                    f"meaning advertising efficiency worsened by {abs(acos_pct_change):.2f}%."
                )
                acos_improvement_pct = 0.0
            else:
                acos_interpretation = "stable"
                acos_direction_text = (
                    f"ACOS/TACoS remained unchanged at {current_acos:.2f}%."
                )
                acos_improvement_pct = 0.0

            acos_context = {
                "metric": "ACOS/TACoS",
                "current": round(current_acos, 2),
                "previous": round(previous_acos, 2),
                "pct_change": round(acos_pct_change, 2),
                "improvement_pct": round(acos_improvement_pct, 2),
                "current_ads": round(current_ads, 2),
                "previous_ads": round(previous_ads, 2),
                "ads_pct_change": _pct_change(current_ads, previous_ads),
                "lower_is_better": True,
                "interpretation": acos_interpretation,
                "direction_text": acos_direction_text,
            }

            clean_global_metrics = {
                "units": {
                    "current": round(current_units, 2),
                    "previous": round(previous_units, 2),
                    "pct_change": _pct_change(current_units, previous_units),
                },
                "net_sales": {
                    "current": round(current_net_sales, 2),
                    "previous": round(previous_net_sales, 2),
                    "pct_change": _pct_change(current_net_sales, previous_net_sales),
                },
                "cm1_profit": {
                    "current": round(current_cm1_profit, 2),
                    "previous": round(previous_cm1_profit, 2),
                    "pct_change": _pct_change(current_cm1_profit, previous_cm1_profit),
                },
                "cm1_profit_per_unit": {
                    "current": round(current_cm1_profit_per_unit, 2),
                    "previous": round(previous_cm1_profit_per_unit, 2),
                    "pct_change": _pct_change(
                        current_cm1_profit_per_unit,
                        previous_cm1_profit_per_unit,
                    ),
                },
                "asp": {
                    "current": round(current_asp, 2),
                    "previous": round(previous_asp, 2),
                    "pct_change": _pct_change(current_asp, previous_asp),
                },
                "acos": acos_context,
            }

            objective_hash = generate_objective_hash(user_objective)

            cached_ai = get_cached_live_ai(
                user_id=user_id,
                country="global",
                start_date=curr_start,
                end_date=ai_refresh_slot,
                objective_hash=objective_hash,
            )

            analysis = {}
            summary_out = {
                "summary_text": "",
                "metric_bullets": [],
            }
            global_strategy_parsed = {}

            if cached_ai:
                analysis = cached_ai["analysis"]
                summary_out = cached_ai["summary"]
                global_strategy_parsed = cached_ai.get("strategy", {}) or {}
            else:
                try:
                    analysis = run_live_prompt_1_analysis(payload_ai)

                    summary_out = run_live_prompt_1_5_summary(
                    analysis_output=analysis,
                    numeric_context={
                    "report_type": "global",
                    "periods": payload_ai["periods"],
                    "pct_changes": payload_ai["pct_changes"],
                    "selling_costs": payload_ai["selling_costs"],
                    "roas": payload_ai["roas"],
                    "movement_context": payload_ai["movement_context"],
                    "currency": payload_ai["currency"],

                    # Important: force country-level global comparison
                    "country_split": payload_ai.get("country_split", {}),
                    "global_top_products": top_80_skus[:5],

                    # Use this as the MAIN source for executive summary
                    "clean_global_metrics": clean_global_metrics,

                    # Keep raw totals only for backup/context
                    "global_totals": {
                        "previous": previous_global_payload.get("derived_totals_global", {}),
                        "current": current_global_payload.get("derived_totals_global", {}),
                        "aligned_previous": previous_global_payload.get("aligned_totals_global", {}),
                    },

                    # Direct ACOS/TACoS context
                    "acos": acos_context,

                    # Important: force CM1 only
                    "profit_metric": "CM1 Profit",

                    "metric_rules": {
                        "primary_source": "Use numeric_context.clean_global_metrics as the primary source for Units, Net Sales, CM1 Profit, CM1 Profit per Unit, ASP, and ACOS/TACoS.",
                        "profit_metric_source": "Use CM1 Profit only from clean_global_metrics.cm1_profit. Do not use CM2 Profit.",
                        "acos_source": "Use numeric_context.acos as the source of truth for ACOS/TACoS.",
                        "acos_interpretation": "Lower ACOS/TACoS is better. If current is lower than previous, advertising efficiency improved.",
                    },
                },
                    user_objective=user_objective,
                )

                    

                except Exception as e:
                    print("[AI ERROR] Failed to generate global summary:", e)


            # -------------------------------------------------
            # GLOBAL: Prompt-2 portfolio strategy
            # -------------------------------------------------
            if not global_strategy_parsed:
                try:
                    global_live_context = []

                    for r in top_80_skus:
                        product_name = r.get("product_name")
                        if not product_name:
                            continue

                        global_live_context.append({
                            "sku": product_name,
                            "product_name": product_name,
                            "quantity": {
                                "previous": r.get("quantity_prev"),
                                "current": r.get("quantity_curr"),
                            },
                            "asp": {
                                "previous": r.get("asp_prev"),
                                "current": r.get("asp_curr"),
                            },
                            "net_sales": {
                                "previous": r.get("net_sales_prev"),
                                "current": r.get("net_sales_curr"),
                            },
                            "cm1_profit": {
                                "previous": r.get("profit_prev"),
                                "current": r.get("profit_curr"),
                            },
                            "profit_per_unit": {
                                "previous": r.get("unit_wise_profitability_prev"),
                                "current": r.get("unit_wise_profitability_curr"),
                            },
                            "movement_intensity": {
                                "units": (r.get("Unit Growth (%)") or {}).get("value"),
                                "asp": (r.get("ASP Growth (%)") or {}).get("value"),
                                "net_sales": (r.get("Net Sales Growth (%)") or {}).get("value"),
                                "cm1_profit": (r.get("CM1 Profit Impact (%)") or {}).get("value"),
                                "profit_per_unit": (r.get("Profit Per Unit (%)") or {}).get("value"),
                            },
                        })

                    global_ads_context = []

                    for r in top_80_skus:
                        product_name = r.get("product_name")
                        if not product_name:
                            continue

                        global_ads_context.append({
                            "sku": product_name,
                            "product_name": product_name,
                            "ads_spend_curr": r.get("ads_spend_curr", 0),
                            "acos_curr": r.get("acos_curr", 0),
                            "cm2_profit_curr": r.get("cm2_profit_curr", 0),
                            "cm2_margin_curr": r.get("cm2_margin_curr", 0),
                            "net_sales_curr": r.get("net_sales_curr", 0),
                        })

                    global_ads_monthly = {
                        "total_ads_spend": sum(float(r.get("ads_spend_curr") or 0) for r in top_80_skus),
                        "total_cm2_profit": sum(float(r.get("cm2_profit_curr") or 0) for r in top_80_skus),
                    }

                    global_inventory_alerts = {
                        "available_countries": available_countries,
                        "uk": portfolio_inventory_alerts_uk if "uk" in available_countries else {},
                        "us": portfolio_inventory_alerts_us if "us" in available_countries else {},
                    }

                    global_strategy_raw = run_prompt_2_strategy(
                        analysis_insights=analysis or {},
                        objective_v2=user_objective,
                        focus_skus=[
                            r.get("product_name")
                            for r in top_80_skus
                            if r.get("product_name")
                        ],
                        sku_time_series={},
                        inventory_alerts=global_inventory_alerts,
                        sku_inventory_flags={},
                        country="global",
                        sku_ads_context=global_ads_context,
                        sku_live_context=global_live_context,
                        ads_monthly=global_ads_monthly,
                        remaining_skus_context={
                            "label": "Other SKUs",
                            "aggregated_metrics": other_products_total_row,
                            "products": other_skus,
                            "instruction": (
                                "You must generate BOTH fields for the aggregated Other SKUs group. "
                                "This is not a single SKU; it represents all products outside the top products. "
                                "Return a concise action recommendation under the exact key remaining_skus_recommendation. "
                                "Return the journey explanation under the exact key remaining_skus_journey_summary. "
                                "Do not omit remaining_skus_recommendation."
                            ),
                        } if other_products_total_row else {},
                    )

                    global_strategy_parsed = json.loads(global_strategy_raw) if global_strategy_raw else {}

                    # print("[DEBUG] global_strategy_parsed keys:", global_strategy_parsed.keys())
                    # print("[DEBUG] remaining_skus_recommendation:", global_strategy_parsed.get("remaining_skus_recommendation"))
                    # print("[DEBUG] remaining_skus_journey_summary:", global_strategy_parsed.get("remaining_skus_journey_summary"))

                except Exception as e:
                    print("[AI ERROR] Global portfolio strategy generation failed:", e)
                    global_strategy_parsed = {}

            portfolio_recommendation = (
                global_strategy_parsed.get("portfolio_recommendation")
                or global_strategy_parsed.get("recommendation")
                or global_strategy_parsed.get("recommended_action")
            )

            if not portfolio_recommendation:
                portfolio_recommendation = "AI portfolio recommendation could not be generated for this refresh."

            if not cached_ai:
                try:
                    save_live_ai_cache(
                        user_id=user_id,
                        country="global",
                        start_date=curr_start,
                        end_date=ai_refresh_slot,
                        objective_hash=objective_hash,
                        analysis=analysis,
                        summary=summary_out,
                        strategy=global_strategy_parsed,
                    )
                except Exception as e:
                    print("[WARN] Failed to save global AI cache:", e)

            
            recommended_actions_uk = build_global_country_recommendations(
                user_id=user_id,
                country="uk",
                prev_items=prev_uk_items,
                curr_items=curr_uk_items,
                user_objective=user_objective,
                currency_symbol="$",
                anchor_year=anchor_year,
                anchor_month=anchor_month,
                analysis=analysis,
            )

            recommended_actions_us = build_global_country_recommendations(
                user_id=user_id,
                country="us",
                prev_items=prev_us_items,
                curr_items=curr_us_items,
                user_objective=user_objective,
                currency_symbol="$",
                anchor_year=anchor_year,
                anchor_month=anchor_month,
                analysis=analysis,
            )

            product_journey = build_global_product_journey_from_country_actions(
                uk_actions=recommended_actions_uk,
                us_actions=recommended_actions_us,
            )

            # -------------------------------------------------
            # GLOBAL: Add Other SKUs recommendation + journey
            # -------------------------------------------------
            other_skus_recommendation = None
            other_skus_journey = []

            if other_products_total_row:
                try:
                    remaining_context = global_strategy_parsed.get("remaining_skus_context") or {}
                    remaining_strategy = global_strategy_parsed.get("remaining_skus_strategy") or {}

                    other_skus_recommendation = (
                        global_strategy_parsed.get("remaining_skus_recommendation")
                        or global_strategy_parsed.get("other_skus_recommendation")
                        or global_strategy_parsed.get("other_products_recommendation")
                        or remaining_context.get("recommendation")
                        or remaining_strategy.get("recommendation")
                    )

                    raw_other_journey = (
                        global_strategy_parsed.get("remaining_skus_journey_summary")
                        or global_strategy_parsed.get("other_skus_journey_summary")
                        or global_strategy_parsed.get("other_products_journey_summary")
                        or []
                    )

                    if isinstance(raw_other_journey, list):
                        other_skus_journey = [str(x) for x in raw_other_journey if x]
                    elif isinstance(raw_other_journey, str) and raw_other_journey.strip():
                        other_skus_journey = [raw_other_journey.strip()]
                    else:
                        other_skus_journey = []

                except Exception as e:
                    print("[WARN] Failed to extract global Other SKUs strategy:", e)
                    other_skus_recommendation = None
                    other_skus_journey = []

                if not other_skus_recommendation:
                    other_skus_recommendation = None

                if not other_skus_journey:
                    other_skus_journey = []

               
                product_journey["other skus"] = {
                    "product_name": "Other SKUs",
                    "journey_comparison": other_skus_journey,
                    "uk": {},
                    "us": {},
                    "recommendation": other_skus_recommendation,
                    "journey_summary": other_skus_journey,
                    "is_other_skus_card": True,
                }

                other_products_total_row["recommendation"] = other_skus_recommendation
                other_products_total_row["journey_summary"] = other_skus_journey
                other_products_total_row["product_journey"] = other_skus_journey

            # -------------------------------------------------
            # GLOBAL: Daily series for charts
            # Best-effort: use whichever country exists.
            # Do NOT crash if UK or US historical/current table is missing.
            # -------------------------------------------------

            uk_to_usd_rate_for_charts = fetch_conversion_rate(
                country="us",
                year=prev_start.year,
                month_name=month_name[prev_start.month].lower(),
                user_currency="gbp",
                selected_currency="usd",
            ) or 1.0


            def safe_fetch_previous_period_data_for_global(user_id, country, start_date, end_date):
                try:
                    items, daily_rows = fetch_previous_period_data(
                        user_id,
                        country,
                        start_date,
                        end_date,
                    )
                    return items or [], daily_rows or []
                except Exception as e:
                    print(
                        f"[WARN] Global chart previous {country.upper()} data unavailable "
                        f"for {start_date} to {end_date}: {e}"
                    )
                    return [], []


            def safe_fetch_current_mtd_data_for_global(user_id, country, start_date, end_date):
                try:
                    items, daily_rows = fetch_current_mtd_data(
                        user_id,
                        country,
                        start_date,
                        end_date,
                    )
                    return items or [], daily_rows or []
                except Exception as e:
                    print(
                        f"[WARN] Global chart current {country.upper()} data unavailable "
                        f"for {start_date} to {end_date}: {e}"
                    )
                    return [], []


            # -------------------------------------------------
            # Previous FULL month daily series
            # Used by daily_series.previous_global / previous_uk / previous_us
            # -------------------------------------------------
            _, previous_uk_raw = safe_fetch_previous_period_data_for_global(
                user_id,
                "uk",
                prev_full_start,
                prev_full_end,
            )

            _, previous_us_raw = safe_fetch_previous_period_data_for_global(
                user_id,
                "us",
                prev_full_start,
                prev_full_end,
            )

            previous_uk = _convert_daily_series_to_usd(
                previous_uk_raw,
                uk_to_usd_rate_for_charts,
            ) if previous_uk_raw else []

            previous_us = _tag_daily_series(
                previous_us_raw,
                "us",
            ) if previous_us_raw else []

            previous_global = _build_global_daily_series(
                previous_us,
                previous_uk,
            )


            # -------------------------------------------------
            # Current MTD daily series
            # Used by daily_series.current_mtd_global / current_mtd_uk / current_mtd_us
            # -------------------------------------------------
            _, current_mtd_uk_raw = safe_fetch_current_mtd_data_for_global(
                user_id,
                "uk",
                curr_start,
                curr_end,
            )

            _, current_mtd_us_raw = safe_fetch_current_mtd_data_for_global(
                user_id,
                "us",
                curr_start,
                curr_end,
            )

            current_mtd_uk = _convert_daily_series_to_usd(
                current_mtd_uk_raw,
                uk_to_usd_rate_for_charts,
            ) if current_mtd_uk_raw else []

            current_mtd_us = _tag_daily_series(
                current_mtd_us_raw,
                "us",
            ) if current_mtd_us_raw else []

            current_mtd_global = _build_global_daily_series(
                current_mtd_us,
                current_mtd_uk,
            )


            # -------------------------------------------------
            # Previous ALIGNED period daily series
            # Used by daily_series_aligned.previous_global / previous_uk / previous_us
            # -------------------------------------------------
            _, previous_aligned_uk_raw = safe_fetch_previous_period_data_for_global(
                user_id,
                "uk",
                prev_start,
                prev_end,
            )

            _, previous_aligned_us_raw = safe_fetch_previous_period_data_for_global(
                user_id,
                "us",
                prev_start,
                prev_end,
            )

            previous_aligned_uk = _convert_daily_series_to_usd(
                previous_aligned_uk_raw,
                uk_to_usd_rate_for_charts,
            ) if previous_aligned_uk_raw else []

            previous_aligned_us = _tag_daily_series(
                previous_aligned_us_raw,
                "us",
            ) if previous_aligned_us_raw else []

            previous_aligned_global = _build_global_daily_series(
                previous_aligned_us,
                previous_aligned_uk,
            )


            # -------------------------------------------------
            # Backward-compatible selected-country chart keys
            # Frontend may still read daily_series.previous/current_mtd
            # -------------------------------------------------
            prev_daily_full = previous_global
            prev_daily_aligned_selected = previous_aligned_global
            curr_daily_selected = current_mtd_global


            response_payload = {
            "message": "Live GLOBAL MTD vs previous-month-same-period comparison",
            "country": "global",
            "currency": currency,

            "portfolio_inventory_block": {
                "uk": portfolio_inventory_block_uk if "uk" in available_countries else "",
                "us": portfolio_inventory_block_us if "us" in available_countries else "",
            },

            "portfolio_inventory_alerts": {
                "uk": portfolio_inventory_alerts_uk if "uk" in available_countries else {},
                "us": portfolio_inventory_alerts_us if "us" in available_countries else {},
            },

            "periods": {
                    "previous": {
                        "label": prev_label,
                        "start": prev_start.isoformat(),
                        "end": prev_end.isoformat(),
                    },
                    "current_mtd": {
                        "label": curr_label,
                        "start": curr_start.isoformat(),
                        "end": curr_end.isoformat(),
                    },
                },

                "overall_summary": {
                    "summary_text": summary_out.get("summary_text", ""),
                    "metric_bullets": summary_out.get("metric_bullets", []),
                },

                "portfolio_recommendation": portfolio_recommendation,

                "categorized_growth": {
                    "top_80_products": (
                        top_80_skus + ([other_products_total_row] if other_products_total_row else [])
                    ),
                    "other_products": other_skus,
                    "other_products_total": other_products_total_row,
                },
                # ✅ ADD THIS BACK FOR GLOBAL CHARTS
                "daily_series": {
                    "current_mtd_global": current_mtd_global,
                    "current_mtd_uk": current_mtd_uk,
                    "current_mtd_us": current_mtd_us,

                    "previous_global": previous_global,
                    "previous_uk": previous_uk,
                    "previous_us": previous_us,

                    # old keys keep frontend compatibility
                    "previous": prev_daily_full,
                    "current_mtd": curr_daily_selected,
                },

                # ✅ ADD THIS BACK FOR GLOBAL CHARTS
                "daily_series_aligned": {
                    "current_mtd_global": current_mtd_global,
                    "current_mtd_uk": current_mtd_uk,
                    "current_mtd_us": current_mtd_us,

                    "previous_global": previous_aligned_global,
                    "previous_uk": previous_aligned_uk,
                    "previous_us": previous_aligned_us,

                    # old keys keep frontend compatibility
                    "previous": prev_daily_aligned_selected,
                    "current_mtd": curr_daily_selected,
                },


                "product_journey": product_journey,

                "other_skus_strategy": {
                    "recommendation": other_skus_recommendation,
                    "journey_summary": other_skus_journey,
                } if other_products_total_row else {},

                "recommended_actions_mtd": {
                    "uk": recommended_actions_uk,
                    "us": recommended_actions_us,
                },

                "skuwise_items": {
                    "current_global": curr_global_items,
                    "previous_global": prev_global_items,
                    "current_uk": curr_uk_items,
                    "previous_uk": prev_uk_items,
                    "current_us": curr_us_items,
                    "previous_us": prev_us_items,
                },

                "aligned_totals": previous_global_payload.get("aligned_totals_global", {}),
                "derived_totals": {
                    "previous_global": previous_global_payload.get("derived_totals_global", {}),
                    "current_global": current_global_payload.get("derived_totals_global", {}),

                    "previous_uk": previous_global_payload.get("derived_totals_uk", {}),
                    "current_uk": current_global_payload.get("derived_totals_uk", {}),

                    "previous_us": previous_global_payload.get("derived_totals_us", {}),
                    "current_us": current_global_payload.get("derived_totals_us", {}),
                },

                "conversion": {
                    "current": current_global_payload.get("conversion"),
                    "previous": previous_global_payload.get("conversion"),
                },
            }

            try:
                response_payload = round_numeric_values(response_payload, ndigits=2)
            except Exception as e:
                print("[WARN] round_numeric_values failed for global:", e)

            return jsonify(response_payload), 200

        # ---------------------------
        # FETCH DATA
        # ---------------------------
        prev_data_aligned, prev_daily_aligned = fetch_previous_period_data(
            user_id, country, prev_start, prev_end
        )

        curr_data, curr_daily = fetch_current_mtd_data(
            user_id, country, curr_start, curr_end
        )

        # ✅ Current values for AI only, from skuwisemonthly monthly table
        curr_ai_data, curr_ai_totals, curr_ai_fee_totals = fetch_current_ai_values_from_skuwisemonthly(
            user_id=user_id,
            country=country,
            curr_end=curr_end,
        )

        # ✅ Fallback: if monthly table missing, AI uses liveorders values
        if not curr_ai_data:
            curr_ai_data = curr_data

            curr_ai_totals = aggregate_totals(curr_ai_data)
            curr_ai_totals["total_asp"] = compute_total_asp(curr_ai_data)
            curr_ai_totals["unit_wise_profitability"] = compute_total_unit_profitability(curr_ai_data)

            curr_ai_fee_totals = totals_from_daily_series(curr_daily)

        # -------------------------------------------------
        # 🔥 Attach SKU-level Ads + CM2 (CURRENT MONTH ONLY)
        # -------------------------------------------------
        ads_sku_map, ads_monthly_totals = fetch_skuwisemonthly_ads_cm2_current_month(
            user_id=user_id,
            country=country,
            year=curr_start.year,
            month=curr_start.month,
        )

        ads_sku_map = {str(k).strip(): v for k, v in (ads_sku_map or {}).items()}

        for row in (curr_ai_data or []):
            sku = str(row.get("sku") or "").strip()
            if not sku:
                continue

            ads_info = ads_sku_map.get(sku, {})
            ads_spend = float(ads_info.get("ads_spend", 0.0) or 0.0)
            cm2_profit = float(ads_info.get("cm2_profit", 0.0) or 0.0)

            net_sales = float(row.get("net_sales", 0.0) or 0.0)

            row["ads_spend_curr"] = ads_spend
            row["cm2_profit_curr"] = cm2_profit

            if net_sales > 0:
                row["acos_curr"] = round((ads_spend / net_sales) * 100.0, 2)
                row["cm2_margin_curr"] = round((cm2_profit / net_sales) * 100.0, 2)
            else:
                row["acos_curr"] = 0.0
                row["cm2_margin_curr"] = 0.0


        # ---------------------------
        # ALIGN SKUs (PREVIOUS + CURRENT)
        # ---------------------------
        prev_data_aligned, curr_ai_data = align_prev_curr_by_sku(
            prev_data_aligned,
            curr_ai_data,
        )
        # ---------------------------
        # DATA STILL WARMING UP
        # ---------------------------
        if not curr_ai_data:
            return jsonify({
                "status": "loading",
                "message": "Data is still syncing. Please wait a few seconds."
            }), 202

        # ---------------------------
        # FULL PREVIOUS MONTH + DAILY SERIES: UK / US / GLOBAL
        # ---------------------------

        uk_to_usd_rate = fetch_conversion_rate(
            country="us",
            year=prev_start.year,
            month_name=month_name[prev_start.month].lower(),
            user_currency="gbp",
            selected_currency="usd",
        ) or 1.0

        # Previous full month
        previous_uk = []
        previous_us = []
        previous_global = []

        if country == "uk":
            _, previous_uk_raw = fetch_previous_period_data(
                user_id, "uk", prev_full_start, prev_full_end
            )
            previous_uk = _convert_daily_series_to_usd(previous_uk_raw, uk_to_usd_rate)

        elif country == "us":
            _, previous_us_raw = fetch_previous_period_data(
                user_id, "us", prev_full_start, prev_full_end
            )
            previous_us = _tag_daily_series(previous_us_raw, "us")

        # ✅ Full previous month total based on selected country
        if country == "uk":
            prev_full_totals = totals_from_daily_series(previous_uk)
        else:
            prev_full_totals = totals_from_daily_series(previous_us)

        total_previous_net_sales_full_month = float(
            prev_full_totals.get("net_sales", 0) or 0
        )

        # Current MTD
        current_mtd_uk = []
        current_mtd_us = []
        current_mtd_global = []

        if country == "uk":
            _, current_mtd_uk_raw = fetch_current_mtd_data(
                user_id, "uk", curr_start, curr_end
            )
            current_mtd_uk = _convert_daily_series_to_usd(current_mtd_uk_raw, uk_to_usd_rate)

        elif country == "us":
            _, current_mtd_us_raw = fetch_current_mtd_data(
                user_id, "us", curr_start, curr_end
            )
            current_mtd_us = _tag_daily_series(current_mtd_us_raw, "us")

        # Previous aligned period
        previous_aligned_uk = []
        previous_aligned_us = []
        previous_aligned_global = []

        if country == "uk":
            _, previous_aligned_uk_raw = fetch_previous_period_data(
                user_id, "uk", prev_start, prev_end
            )
            previous_aligned_uk = _convert_daily_series_to_usd(
                previous_aligned_uk_raw,
                uk_to_usd_rate,
            )

        elif country == "us":
            _, previous_aligned_us_raw = fetch_previous_period_data(
                user_id, "us", prev_start, prev_end
            )
            previous_aligned_us = _tag_daily_series(
                previous_aligned_us_raw,
                "us",
            )

        # ✅ Backward-compatible selected-country series
        if country == "uk":
            prev_daily_full = previous_uk
            prev_daily_aligned_selected = previous_aligned_uk
            curr_daily_selected = current_mtd_uk
        else:
            prev_daily_full = previous_us
            prev_daily_aligned_selected = previous_aligned_us
            curr_daily_selected = current_mtd_us
        # ---------------------------
        # PLATFORM FEES + ADS (SUMMARY ONLY)
        # ---------------------------
        prev_fee_totals = totals_from_daily_series(prev_daily_aligned)

        # ✅ For AI payload only
        curr_fee_totals = curr_ai_fee_totals

        # ---------------------------
        # GROWTH
        # ---------------------------
        growth_data = calculate_growth(
            prev_data_aligned,
            curr_ai_data,
            key=key_column,
        )

       

        prev_keys = {r.get(key_column) for r in prev_data_aligned if r.get(key_column)}
        curr_keys = {r.get(key_column) for r in curr_ai_data if r.get(key_column)}

  
        # ---------------------------
        # NEW / REVIVING SPLIT
        # ---------------------------

        if country == "global":
            # Keep global behavior safe and table/history-based.
            # Global uses product_name as key_column in the global branch.
            first_seen_map = fetch_first_seen_sku_date(user_id, country)

            six_months_cutoff = curr_start - relativedelta(months=6)

            new_sku_keys = {
                sku
                for sku in curr_keys
                if first_seen_map.get(sku) and first_seen_map[sku] >= six_months_cutoff
            }

            new_sku_keys |= {
                sku for sku in curr_keys if sku not in first_seen_map
            }

            reviving_keys = (curr_keys - prev_keys) - new_sku_keys

        else:
            # For UK / US, use products.open_date from DATABASE_AMAZON_URL.
            new_sku_keys = fetch_new_skus_from_products_open_date(
                country=country,
                sku_keys=curr_keys,
                ref_date=curr_start,
            )

            # Reviving = present in current, absent in previous,
            # but not already classified as new.
            reviving_keys = (curr_keys - prev_keys) - new_sku_keys


        # Internal combined key set only for dedupe from Top 80 / Other.
        new_reviving_keys = new_sku_keys | reviving_keys

        new_skus = [
            r for r in growth_data
            if r.get(key_column) in new_sku_keys
        ]

        reviving_skus = [
            r for r in growth_data
            if r.get(key_column) in reviving_keys
        ]

        # Optional compatibility inside backend only.
        # Do not return this as JSON if frontend wants separated buckets.
        new_reviving = new_skus + reviving_skus

        # ---------------------------
        # TOP 80 / OTHER
        # ---------------------------
        existing = [
            r for r in growth_data
            if r.get(key_column) in prev_keys
            and r.get("Sales Mix (Current)") is not None
            and r.get(key_column) not in new_reviving_keys
        ]

        existing_sorted = sorted(
            existing,
            key=lambda x: x["Sales Mix (Current)"],
            reverse=True,
        )

        total_sales_mix = sum(
            r["Sales Mix (Current)"]
            for r in existing_sorted
            if r["Sales Mix (Current)"] is not None
        )

        cumulative = 0.0
        top_80_skus, other_skus = [], []

        for r in existing_sorted:
            mix = r["Sales Mix (Current)"]
            if mix is None:
                continue
            proportion = cumulative / total_sales_mix if total_sales_mix else 0
            if proportion <= 0.8:
                top_80_skus.append(r)
                cumulative += mix
            else:
                other_skus.append(r)

        # ---------------------------
        # SEGMENT TOTALS
        # ---------------------------
        top_keys = {r.get(key_column) for r in top_80_skus}
        other_keys = {r.get(key_column) for r in other_skus}
        new_keys = {r.get(key_column) for r in new_skus}
        reviving_keys_for_total = {r.get(key_column) for r in reviving_skus}

        prev_top = [r for r in prev_data_aligned if r.get(key_column) in top_keys]
        curr_top = [r for r in curr_ai_data if r.get(key_column) in top_keys]

        prev_other = [r for r in prev_data_aligned if r.get(key_column) in other_keys]
        curr_other = [r for r in curr_ai_data if r.get(key_column) in other_keys]

        prev_new = [r for r in prev_data_aligned if r.get(key_column) in new_keys]
        curr_new = [r for r in curr_ai_data if r.get(key_column) in new_keys]

        prev_reviving = [
            r for r in prev_data_aligned
            if r.get(key_column) in reviving_keys_for_total
        ]

        curr_reviving = [
            r for r in curr_ai_data
            if r.get(key_column) in reviving_keys_for_total
        ]

        top_80_total_row = build_segment_total_row(
            prev_top,
            curr_top,
            key=key_column,
            label="Total"
        )

        other_total_row = (
            build_segment_total_row(
                prev_other,
                curr_other,
                key=key_column,
                label="Total"
            )
            if other_skus else None
        )

        new_skus_total_row = (
            build_segment_total_row(
                prev_new,
                curr_new,
                key=key_column,
                label="Total"
            )
            if new_skus else None
        )

        reviving_skus_total_row = (
            build_segment_total_row(
                prev_reviving,
                curr_reviving,
                key=key_column,
                label="Total"
            )
            if reviving_skus else None
        )

        # =========================================================
        # ✅ Build Remaining SKUs (NOT in top_80_skus)
        # =========================================================

        remaining_growth_row = None  # safety

        top_keys = {r.get("sku") for r in top_80_skus}

        remaining_prev = [
            r for r in prev_data_aligned
            if r.get("sku") not in top_keys
        ]

        remaining_curr = [
            r for r in curr_ai_data
            if r.get("sku") not in top_keys
        ]

        if remaining_curr:
            agg_prev = aggregate_totals(remaining_prev)
            agg_curr = aggregate_totals(remaining_curr)

            remaining_growth_row = calculate_growth(
                prev_data=[{
                    "sku": "REMAINING",
                    "product_name": "Other SKUs",
                    **agg_prev
                }],
                curr_data=[{
                    "sku": "REMAINING",
                    "product_name": "Other SKUs",
                    **agg_curr
                }],
                key="sku"
            )[0]

        # ---------------------------
        # CM1 PROFIT PIE (SAFE, NON-BREAKING)
        # ---------------------------
        pie_rows = [
            {
                "product_name": r.get("product_name"),
                "profit_curr": r.get("profit_curr", 0),
                "profit_prev": r.get("profit_prev", 0),
            }
            for r in (top_80_skus + other_skus)
        ]

        cm1_profit_pie = build_cm1_profit_pie_slices(
            pie_rows,
            min_named=5,
            pareto_threshold=0.8,
        )


        # ---------------------------
        # PORTFOLIO INVENTORY ALERTS (HISTORIC-PARITY)
        # ---------------------------
        try:
            inv_df = fetch_inventory_aged_by_user(user_id, country)

            portfolio_inventory_alerts = build_portfolio_inventory_alerts(
                inv_df,
                user_id=user_id,
                country=country,
            )
        except Exception as e:
            print("[WARN] Failed to build portfolio inventory alerts:", e)
            portfolio_inventory_alerts = {}

        # ---------------------------
        # SKU INVENTORY ALERTS (ONLY IF YOU STILL NEED THEM ELSEWHERE)
        # ---------------------------
        inventory_signals = {}



        # ---------------------------
        # LABELS
        # ---------------------------
        prev_label = (
            f"{month_abbr[prev_start.month].capitalize()}'"
            f"{str(prev_start.year)[-2:]} {prev_start.day}–{prev_end.day}"
        )
        curr_label = (
            f"{month_abbr[curr_start.month].capitalize()}'"
            f"{str(curr_start.year)[-2:]} {curr_start.day}–{curr_end.day}"
        )
        prev_label_full = (
            f"{month_abbr[prev_full_start.month].capitalize()}'"
            f"{str(prev_full_start.year)[-2:]} 1–{prev_full_end.day}"
        )

        # ---------------------------
        # AI SUMMARY (ONCE)
        # ---------------------------
        prev_totals = aggregate_totals(prev_data_aligned)
        prev_totals["total_asp"] = compute_total_asp(prev_data_aligned)
        prev_totals["unit_wise_profitability"] = compute_total_unit_profitability(prev_data_aligned)

        # ✅ Current AI totals come from skuwisemonthly TOTAL row / monthly table
        curr_totals = curr_ai_totals

        sku_context = build_sku_context(growth_data, max_items=5)
        estimated_storage_cost_next_month = fetch_estimated_storage_cost_next_month(user_id, country)

        currency_map = {
            "uk": {"symbol": "£", "code": "GBP"},
            "us": {"symbol": "$", "code": "USD"},
            "global": {"symbol": "$", "code": "USD"},
        }
        currency = currency_map.get(country, {"symbol": "$", "code": "USD"})

        # ---------------------------
        # SKU → PRODUCT NAME MAP (FOR INVENTORY CLUBBING)
        # ---------------------------
        try:
            sku_map_df = fetch_sku_product_mapping(user_id)
            sku_to_product = (
                dict(zip(sku_map_df["sku"], sku_map_df["product_name"]))
                if not sku_map_df.empty
                else {}
            )
        except Exception as e:
            print("[WARN] Failed to build SKU→product map:", e)
            sku_to_product = {}


        # ====================================================
        # ✅ ALL SKUs FOR PRODUCT JOURNEY + RECOMMENDATIONS
        # Includes top_80 + other + new + reviving.
        # This does NOT change categorized_growth display buckets.
        # ====================================================
        all_skus_for_actions = []
        _seen_action_skus = set()

        for row in (
            (top_80_skus or [])
            + (other_skus or [])
            + (new_skus or [])
            + (reviving_skus or [])
        ):
            sku = str(row.get("sku") or "").strip()
            if not sku or sku in _seen_action_skus:
                continue

            all_skus_for_actions.append(row)
            _seen_action_skus.add(sku)


        # ---------------------------
        # BUILD PAYLOAD (NO AI HERE)
        # ---------------------------
        payload_ai = build_ai_summary(
            prev_totals,
            curr_totals,

            # ✅ CHANGED: pass all SKU rows, not only top_80_skus
            all_skus_for_actions,

            prev_label,
            curr_label,
            sku_context=sku_context,
            inventory_signals=inventory_signals,
            portfolio_inventory_alerts=portfolio_inventory_alerts, 
            prev_fee_totals=prev_fee_totals,
            curr_fee_totals=curr_fee_totals,
            estimated_storage_cost_next_month=estimated_storage_cost_next_month,
            currency=currency,
            user_objective=user_objective,
            movement_context=movement_context,
            sku_to_product=sku_to_product, 
            user_id=user_id,
            country=country,
            current_year=ranges["meta"]["current_year"],
            current_month=ranges["meta"]["current_month"],
        )

        # ====================================================
        # ✅ ACTION SKU CONTEXT
        # Source of truth from build_ai_summary()
        # - focus_skus_for_cards = only 5 focus SKUs
        # - all_action_skus = every SKU that should get journey + 3 recommendations
        # - all_action_rows = full rows for those SKUs
        # - remaining_growth_row = one aggregated Remaining SKUs row
        # ====================================================
        sku_tables_from_payload = payload_ai.get("sku_tables", {}) or {}
        product_action_context = payload_ai.get("product_action_context", {}) or {}

        focus_skus_for_cards = sku_tables_from_payload.get("focus_skus") or [
            str(r.get("sku")).strip()
            for r in (top_80_skus or [])
            if r.get("sku")
        ][:5]

        all_action_skus = product_action_context.get("action_skus") or [
            str(r.get("sku")).strip()
            for r in (all_skus_for_actions or [])
            if r.get("sku")
        ]

        all_action_rows = product_action_context.get("action_rows") or [
            r for r in (all_skus_for_actions or [])
            if r.get("sku")
        ]

        # Prefer the Remaining SKUs aggregate created inside build_ai_summary()
        remaining_growth_row = (
            sku_tables_from_payload.get("remaining_skus_aggregate")
            or remaining_growth_row
        )

        # ====================================================
        # AI CACHE CHECK
        # ====================================================

        objective_hash = generate_objective_hash(user_objective)

        # default safe values
        analysis = {}
        summary_out = {"summary_text": "", "metric_bullets": []}
        strategy_parsed = {}

        try:

            cached_ai = get_cached_live_ai(
                user_id=user_id,
                country=country,
                start_date=curr_start,
                end_date=ai_refresh_slot,
                objective_hash=objective_hash,
            )

            if cached_ai:
                analysis = cached_ai.get("analysis", {}) or {}
                summary_out = cached_ai.get("summary", {}) or {
                    "summary_text": "",
                    "metric_bullets": [],
                }
                strategy_parsed = cached_ai.get("strategy", {}) or {}
            else:
                # ---------------------------
                # PROMPT-1 (ANALYSIS)
                # ---------------------------
                analysis = run_live_prompt_1_analysis(payload_ai)

                # ---------------------------
                # PROMPT-1.5 (EXECUTIVE SUMMARY)
                # ---------------------------
                summary_numeric_context = {
                    "periods": payload_ai["periods"],
                    "pct_changes": payload_ai["pct_changes"],
                    "selling_costs": payload_ai["selling_costs"],
                    "roas": payload_ai["roas"],
                    "movement_context": payload_ai["movement_context"],
                    "currency": payload_ai["currency"],
                }

                summary_out = run_live_prompt_1_5_summary(
                    analysis_output=analysis,
                    numeric_context=summary_numeric_context,
                    user_objective=user_objective,
                )

        except Exception as e:

            print("[AI ERROR] Failed to generate AI summary:", e)

            # fallback values (graphs still work)
            analysis = {}
            summary_out = {"summary_text": "", "metric_bullets": []}
            strategy_parsed = {}

     
        # ===========================
        # EXTRACT EXECUTIVE SUMMARY  ✅ (THIS IS STEP 3)
        # ===========================
        overall_summary_text = summary_out.get("summary_text", "")
        overall_summary_bullets = summary_out.get("metric_bullets", [])

        # ---------------------------
        # STRATEGY ENGINE (MONTH-END PROMPT 2)
        # ---------------------------
        sku_live_context = []

        for r in all_action_rows:
            sku = r.get("sku")
            if not sku:
                continue

            growth_row = next(
                (g for g in growth_data if g.get("sku") == sku),
                None
            )
            if not growth_row:
                continue

            sku_live_context.append({
            "sku": sku,

            # ---- Raw previous vs current ----
            "quantity": {
                "previous": growth_row.get("quantity_prev"),
                "current": growth_row.get("quantity_curr"),
            },
            "asp": {
                "previous": growth_row.get("asp_prev"),
                "current": growth_row.get("asp_curr"),
            },
            "net_sales": {
                "previous": growth_row.get("net_sales_prev"),
                "current": growth_row.get("net_sales_curr"),
            },
            "cm1_profit": {
                "previous": growth_row.get("profit_prev"),
                "current": growth_row.get("profit_curr"),
            },
            "profit_per_unit": {
                "previous": growth_row.get("unit_wise_profitability_prev"),
                "current": growth_row.get("unit_wise_profitability_curr"),
            },

            # 🔥 NEW — percentage movement (very important)
            "movement_intensity": {
                "units": (growth_row.get("Unit Growth (%)") or {}).get("value"),
                "asp": (growth_row.get("ASP Growth (%)") or {}).get("value"),
                "net_sales": (growth_row.get("Net Sales Growth (%)") or {}).get("value"),
                "cm1_profit": (growth_row.get("CM1 Profit Impact (%)") or {}).get("value"),
                "profit_per_unit": (growth_row.get("Profit Per Unit (%)") or {}).get("value"),
            }
        })
            
        # print("\n========== SKU LIVE CONTEXT GOING TO STRATEGY AI ==========")
        # print(json.dumps(sku_live_context, indent=2, default=str))
        # print("========== END SKU LIVE CONTEXT ==========\n")    

        # -------------------------------------------------
        # EXCEL-BASED LIVE SKU RECOMMENDATIONS
        # -------------------------------------------------

        excel_live_recommendations = {}

        growth_intent = user_objective.get("growth_intent", "balanced")
        profit_priority = user_objective.get("profit_priority", "protect_growth")

        for row in sku_live_context:

            sku = row.get("sku")
            if not sku:
                continue

            rec = get_excel_recommendation_from_live_context(
                sku_live_row=row,
                growth_intent=growth_intent,
                profit_priority=profit_priority
            )

            excel_live_recommendations[sku] = rec

        # -------------------------------------------------
        # 🔥 SKU-LEVEL INVENTORY FLAGS (FOR PROMPT-2)
        # -------------------------------------------------
        try:
            sku_inventory_flags = generate_sku_inventory_flags(
                user_id=user_id,
                country=country,
                focus_skus=all_action_skus,
            )
        except Exception as e:
            print("[WARN] Failed to build SKU inventory flags:", e)
            sku_inventory_flags = {}

            


        # ==========================================
        # 🔥 ADD THIS BLOCK RIGHT HERE
        # ==========================================

        sku_ads_context = []

        for r in all_action_rows:
            sku = r.get("sku")
            if not sku:
                continue

            sku_ads_context.append({
                "sku": sku,
                "ads_spend_curr": r.get("ads_spend_curr", 0),
                "acos_curr": r.get("acos_curr", 0),
                "cm2_profit_curr": r.get("cm2_profit_curr", 0),
                "cm2_margin_curr": r.get("cm2_margin_curr", 0),
                "net_sales_curr": r.get("net_sales_curr", 0),
            })

        ads_monthly = {
            "total_ads_spend": ads_monthly_totals.get("ads_spend", 0),
            "total_cm2_profit": ads_monthly_totals.get("cm2_profit", 0),
        }

        # -------------------------------------------------
        # 🔥 BUILD SKU TIME SERIES (24-month history)
        # -------------------------------------------------
        sku_time_series = {}

        for r in all_action_rows:
            sku = r.get("sku")
            if not sku:
                continue

            try:
                sku_time_series[sku] = build_rolling_sku_series(
                    user_id=user_id,
                    country=country,
                    sku=sku,
                    anchor_year=anchor_year,
                    anchor_month=anchor_month,
                )
            except Exception as e:
                print("[WARN] Failed to build time series for", sku, e)

        # # -------------------------------------------------
        # # 🔥 BUILD REMAINING SKU TIME SERIES (Other SKUs)
        # # -------------------------------------------------
        # remaining_series = []

        # try:
        #     remaining_series = build_remaining_skus_time_series(
        #         user_id=user_id,
        #         country=country,
        #         focus_skus=[r.get("sku") for r in top_80_skus],
        #         anchor_year=anchor_year,
        #         anchor_month=anchor_month,
        #         months=24,
        #     )
        # except Exception as e:
        #     print("[WARN] Remaining SKU series failed:", e)        


        # # ---------------------------------------
        # # STRATEGY ENGINE (SAFE WRAPPED)
        # # ---------------------------------------

        # if not cached_ai:

        #     try:

        #         strategy_raw = run_prompt_2_strategy(
        #             analysis_insights=analysis,
        #             objective_v2=user_objective,
        #             focus_skus=[r.get("sku") for r in top_80_skus],
        #             sku_time_series=sku_time_series,
        #             inventory_alerts=payload_ai.get("inventory_signals", {}),
        #             sku_inventory_flags=sku_inventory_flags,
        #             country=country,
        #             sku_ads_context=sku_ads_context,
        #             sku_live_context=sku_live_context,
        #             ads_monthly=ads_monthly,
        #             remaining_skus_context={
        #                 "aggregated_metrics": remaining_growth_row,
        #                 "time_series": remaining_series
        #             } if remaining_growth_row else {},
        #         )

        # -------------------------------------------------
        # 🔥 BUILD REMAINING SKU TIME SERIES (Other SKUs)
        # -------------------------------------------------
        remaining_series = []

        try:
            remaining_series = build_remaining_skus_time_series(
                user_id=user_id,
                country=country,
                focus_skus=focus_skus_for_cards,
                anchor_year=anchor_year,
                anchor_month=anchor_month,
                months=24,
            )
        except Exception as e:
            print("[WARN] Remaining SKU series failed:", e)        


        # ---------------------------------------
        # STRATEGY ENGINE (SAFE WRAPPED)
        # ---------------------------------------

        if not cached_ai:

            try:

                strategy_debug_payload = {
                    "analysis_insights": analysis,
                    "objective_v2": user_objective,

                    # ✅ Generate journey + recommendations for every action SKU
                    "focus_skus": all_action_skus,

                    "sku_time_series": sku_time_series,
                    "inventory_alerts": payload_ai.get("inventory_signals", {}),
                    "sku_inventory_flags": sku_inventory_flags,
                    "country": country,
                    "sku_ads_context": sku_ads_context,
                    "sku_live_context": sku_live_context,
                    "ads_monthly": ads_monthly,

                    # ✅ One aggregated Remaining SKUs card
                    "remaining_skus_context": {
                        "aggregated_metrics": remaining_growth_row,
                        "time_series": remaining_series,
                        "included_products": (
                            remaining_growth_row.get("included_products", [])
                            if isinstance(remaining_growth_row, dict)
                            else []
                        ),
                        "instruction": (
                            "Generate journey and recommendations for the aggregated Remaining SKUs group. "
                            "Return remaining_skus_journey_summary, remaining_skus_recommendation, "
                            "remaining_skus_ads_recommendation, and remaining_skus_inventory_recommendation."
                        ),
                    } if remaining_growth_row else {},
                }
                # print("\n========== PROMPT 2 STRATEGY PAYLOAD ==========")
                # print(json.dumps(strategy_debug_payload, indent=2, default=str))
                # print("========== END PROMPT 2 STRATEGY PAYLOAD ==========\n")

                strategy_raw = run_prompt_2_strategy(**strategy_debug_payload)

                if strategy_raw:
                    strategy_parsed = json.loads(strategy_raw)
                else:
                    strategy_parsed = {}

            except Exception as e:

                print("[AI ERROR] Strategy generation failed:", e)

                strategy_parsed = {}

                if strategy_raw:
                    strategy_parsed = json.loads(strategy_raw)
                else:
                    strategy_parsed = {}

            except Exception as e:

                print("[AI ERROR] Strategy generation failed:", e)

                strategy_parsed = {}

        # ====================================================
        # SAVE AI CACHE
        # ====================================================
        if not cached_ai:
            save_live_ai_cache(
                user_id=user_id,
                country=country,
                start_date=curr_start,
                end_date=ai_refresh_slot,
                objective_hash=objective_hash,
                analysis=analysis,
                summary=summary_out,
                strategy=strategy_parsed
            )

        # Safe extraction (always executes)
        portfolio_recommendation = strategy_parsed.get("portfolio_recommendation")

        raw_sku_strategy_actions = strategy_parsed.get("sku_actions", {}) or {}

        # ====================================================
        # ✅ FORCE STRATEGY OUTPUT FOR EVERY ACTION SKU
        # Prompt-2 may return only 5 SKUs, but route must return all.
        # ====================================================
        sku_strategy_actions = {}

        for sku in all_action_skus:
            ai_action = raw_sku_strategy_actions.get(sku, {}) or {}

            sku_strategy_actions[sku] = {
                "journey_summary": ai_action.get("journey_summary", []),
                "recommendation": ai_action.get("recommendation", ""),
                "ads_recommendation": ai_action.get("ads_recommendation", ""),
                "inventory_recommendation": ai_action.get("inventory_recommendation", ""),
            }

        # -------------------------------------------------
        # Override AI recommendation with Excel logic
        # -------------------------------------------------
        for sku, action in sku_strategy_actions.items():
            if sku in excel_live_recommendations:
                action["recommendation"] = excel_live_recommendations.get(sku, "")
        remaining_skus_reco = strategy_parsed.get("remaining_skus_recommendation")
        remaining_skus_journey = strategy_parsed.get("remaining_skus_journey_summary")

        remaining_skus_ads_reco = strategy_parsed.get("remaining_skus_ads_recommendation")
        remaining_skus_inventory_reco = strategy_parsed.get("remaining_skus_inventory_recommendation")

        # ===========================
        # BUILD RECOMMENDED ACTIONS
        # ===========================

        recommended_actions_mtd = {}

        for row in all_action_rows:
            sku = row.get("sku")
            if not sku:
                continue

            growth_row = next(
                (g for g in growth_data if g.get("sku") == sku),
                None
            )
            if not growth_row:
                continue

            sku_strategy = sku_strategy_actions.get(sku, {})

            recommended_actions_mtd[sku] = render_live_recommended_action(
                growth_row=growth_row,
                recommendation=sku_strategy.get("recommendation", "Monitor performance"),
                ads_recommendation=sku_strategy.get("ads_recommendation"),
                inventory_recommendation=sku_strategy.get("inventory_recommendation"),
                journey_summary=sku_strategy.get("journey_summary"),
                currency_symbol=currency["symbol"],
                
            )


        # =========================================================
        # ✅ ADD THIS BLOCK RIGHT HERE (AFTER LOOP)
        # =========================================================

        remaining_skus_block = None

        if remaining_growth_row and remaining_skus_reco:
            remaining_skus_block = render_live_recommended_action(
                growth_row=remaining_growth_row,
                recommendation=remaining_skus_reco,
                ads_recommendation=remaining_skus_ads_reco,
                inventory_recommendation=remaining_skus_inventory_reco,
                journey_summary=remaining_skus_journey,
                currency_symbol=currency["symbol"],
            )  


        # ===========================
        # FINAL FIELDS USED BY RESPONSE / EMAIL
        # ===========================

        overall_summary = {
            "summary_text": overall_summary_text,
            "metric_bullets": overall_summary_bullets,
        }

        overall_actions = sku_strategy_actions


      
        # ---------------------------
        # AI INSIGHTS (SKU LEVEL)
        # ---------------------------
        insights = {}

        if generate_ai_insights:
            skus_for_ai = top_80_skus + new_skus + reviving_skus + other_skus

            # month2 = current MTD month in YYYY-MM format
            month2 = f"{curr_start.year}-{curr_start.month:02d}"

            app = current_app._get_current_object()

            with ThreadPoolExecutor(max_workers=10) as executor:
                futures = [
                    executor.submit(
                        generate_live_insight_with_app_context,
                        app,
                        item,
                        country,
                        prev_label,
                        curr_label,
                        user_id,
                        month2,
                    )
                    for item in skus_for_ai
                ]

                for future in as_completed(futures):
                    try:
                        key, res = future.result()
                        insights[key] = res
                    except Exception as e:
                        import traceback
                        print("[LIVE MTD AI INSIGHT THREAD ERROR]", e)
                        traceback.print_exc()
            

        # ---------------------------
        # TOTALS (ALIGNED)
        # ---------------------------
        prev_aligned_totals = totals_from_daily_series(prev_daily_aligned)
        curr_aligned_totals = totals_from_daily_series(curr_daily)
        # ✅ NEW: reimbursement (net) from settlement df
        # curr_daily and prev_daily_aligned are the dataframes you already have
        total_current_rembursement_fee = float(sum((r.get("rembursement_fee", 0) or 0) for r in (curr_daily or [])))
        total_previous_rembursement_fee = float(sum((r.get("rembursement_fee", 0) or 0) for r in (prev_daily_aligned or [])))

        # safely cast everything to float to avoid None / Decimal issues
        total_current_profit = float(curr_aligned_totals.get("profit", 0) or 0)
        total_previous_profit = float(prev_aligned_totals.get("profit", 0) or 0)

        total_current_platform_fees = float(curr_aligned_totals.get("platform_fee", 0) or 0)
        total_previous_platform_fees = float(prev_aligned_totals.get("platform_fee", 0) or 0)

        total_current_advertising = float(curr_aligned_totals.get("advertising", 0) or 0)
        total_previous_advertising = float(prev_aligned_totals.get("advertising", 0) or 0)
        

        # ✅ NEW CM2 totals (portfolio-level)
        total_current_profit_cm2 = total_current_profit - total_current_advertising - total_current_platform_fees
        total_previous_profit_cm2 = total_previous_profit - total_previous_advertising - total_previous_platform_fees

        # ---------------------------
        # PROFIT % (CM2 Margin %)
        # ---------------------------

        total_current_net_sales = float(curr_aligned_totals.get("net_sales", 0) or 0)
        total_previous_net_sales = float(prev_aligned_totals.get("net_sales", 0) or 0)

        if total_current_net_sales != 0:
            total_current_profit_percentage = (
                total_current_profit_cm2 / total_current_net_sales
            ) * 100.0
        else:
            total_current_profit_percentage = 0.0

        if total_previous_net_sales != 0:
            total_previous_profit_percentage = (
                total_previous_profit_cm2 / total_previous_net_sales
            ) * 100.0
        else:
            total_previous_profit_percentage = 0.0

        aligned_totals_payload = {
            "total_current_profit": total_current_profit,
            "total_previous_profit": total_previous_profit,
            "total_current_profit_percentage": total_current_profit_percentage,
            "total_previous_profit_percentage": total_previous_profit_percentage,

            "total_current_platform_fees": total_current_platform_fees,
            "total_previous_platform_fees": total_previous_platform_fees,

            "total_current_advertising": total_current_advertising,
            "total_previous_advertising": total_previous_advertising,

            "total_current_net_sales": float(curr_aligned_totals.get("net_sales", 0) or 0),
            "total_previous_net_sales": float(prev_aligned_totals.get("net_sales", 0) or 0),

            "total_previous_net_sales_full_month": float(total_previous_net_sales_full_month or 0),

            # ✅ NEW FIELDS
            "total_current_profit_cm2": total_current_profit_cm2,
            "total_previous_profit_cm2": total_previous_profit_cm2,
            "total_current_rembursement_fee": total_current_rembursement_fee,
            "total_previous_rembursement_fee": total_previous_rembursement_fee,
        }
        # --- build inventory block ---
        portfolio_inventory_block = render_portfolio_inventory_block(
            inventory_alerts=portfolio_inventory_alerts,
            currency_symbol=currency["symbol"],
        )    

        # ---------------------------
        # FINAL RESPONSE PAYLOAD
        # ---------------------------
        response_payload = {
           
            "message": "Live MTD vs previous-month-same-period comparison",
            "objective_context": {
                "growth_intent": user_objective.get("growth_intent"),
                "profit_priority": user_objective.get("profit_priority"),
                "inventory_clearance_priority": user_objective.get("inventory_clearance_priority"),
                "time_horizon": user_objective.get("time_horizon"),
            },
            "portfolio_inventory_block": portfolio_inventory_block,

            "periods": {
                "previous": {"label": prev_label},
                "previous_full": {"label": prev_label_full},
                "current_mtd": {"label": curr_label},
            },
            "aligned_totals": aligned_totals_payload,
            "categorized_growth": {
                "top_80_skus": top_80_skus,
                "new_skus": new_skus,
                "reviving_skus": reviving_skus,
                "other_skus": other_skus,

                "top_80_total": top_80_total_row,
                "new_skus_total": new_skus_total_row,
                "reviving_skus_total": reviving_skus_total_row,
                "other_total": other_total_row,
            },
            "cm1_profit_pie": cm1_profit_pie,

            "daily_series": {
                "current_mtd_global": current_mtd_global,
                "current_mtd_uk": current_mtd_uk,
                "current_mtd_us": current_mtd_us,

                "previous_global": previous_global,
                "previous_uk": previous_uk,
                "previous_us": previous_us,

                # old keys keep frontend compatibility
                "previous": prev_daily_full,
                "current_mtd": curr_daily_selected,
            },

            "daily_series_aligned": {
                "current_mtd_global": current_mtd_global,
                "current_mtd_uk": current_mtd_uk,
                "current_mtd_us": current_mtd_us,

                "previous_global": previous_aligned_global,
                "previous_uk": previous_aligned_uk,
                "previous_us": previous_aligned_us,

                # old keys keep frontend compatibility
                "previous": prev_daily_aligned_selected,
                "current_mtd": curr_daily_selected,
            },

            
            "ai_insights": insights,
            "overall_summary": overall_summary,
           "overall_actions": overall_actions,
            "portfolio_recommendation": portfolio_recommendation,

            # ✅ All raw SKU actions from Prompt-2
            # Contains journey_summary, recommendation, ads_recommendation, inventory_recommendation
            "sku_strategy_actions": sku_strategy_actions,

            # ✅ SKU rendered cards
            "recommended_actions_mtd": recommended_actions_mtd,

            # ✅ Focus/Action metadata for frontend
            "focus_skus": focus_skus_for_cards,
            "all_action_skus": all_action_skus,

            # 🔥 Remaining SKUs
            "remaining_skus_recommendation": remaining_skus_reco,
            "remaining_skus_ads_recommendation": remaining_skus_ads_reco,
            "remaining_skus_inventory_recommendation": remaining_skus_inventory_reco,
            "remaining_skus_journey_summary": remaining_skus_journey,
            "remaining_skus_block": remaining_skus_block,
        }

        # # ---------------------------
        # # SEND EMAIL
        # # ---------------------------
        # user_email = payload.get("email") or request.args.get("email")
        # user_name = None

        # if not user_email:
        #     user_email, user_name = get_user_email_and_name_by_id(user_id)

        # if isinstance(user_email, tuple):
        #     user_email = user_email[0]

        # user_email = str(user_email).strip() if user_email else None


        # if user_email:
        #     cache_key = (user_id, country)

        #     already_in_cache = cache_key in _SENT_EMAIL_CACHE
        #     recently_sent = has_recent_bi_email(user_id, country, hours=24)

        #     if not already_in_cache and not recently_sent:
        #         try:
        #             email_token_payload = {
        #                 "user_id": user_id,
        #                 "email": user_email,
        #                 "scope": "live_mtd_bi",
        #                 "exp": datetime.utcnow() + timedelta(hours=24),
        #             }

        #             email_token = jwt.encode(
        #                 email_token_payload,
        #                 SECRET_KEY,
        #                 algorithm="HS256",
        #             )

        #             sent = send_live_bi_email(
        #                 to_email=user_email,
        #                 overall_summary=response_payload["overall_summary"],
        #                 overall_actions=response_payload["overall_actions"],
        #                 sku_actions=recommended_actions_mtd,
        #                 country=country,
        #                 prev_label=prev_label,
        #                 curr_label=curr_label,
        #                 deep_link_token=email_token,
        #                 portfolio_recommendation=response_payload.get("portfolio_recommendation"),
        #             )

        #             if sent:
        #                 mark_bi_email_sent(user_id, country)
        #                 _SENT_EMAIL_CACHE.add(cache_key)
        #             else:
        #                 print("[WARN] Live BI email was not sent, so not marking as sent.")

        #         except Exception as e:
        #             print("[WARN] Error sending live BI email:", e)
        # else:
        #     print("[WARN] No user email found, skipping Live BI email.")

        try:
            response_payload = round_numeric_values(response_payload, ndigits=2)
        except Exception as e:
            print("[WARN] round_numeric_values failed:", e)

        return jsonify(response_payload), 200

    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"error": "Server error", "details": str(e)}), 500




def fetch_conversion_rate(
    country: str,
    year: int,
    month_name: str,
    user_currency: str,
    selected_currency: str
) -> float:
    sql = text("""
        SELECT conversion_rate
        FROM public.currency_conversion
        WHERE lower(country) = lower(:country)
          AND year = :year
          AND lower(month) = lower(:month)
          AND lower(user_currency) = lower(:user_currency)
          AND lower(selected_currency) = lower(:selected_currency)
        ORDER BY id DESC
        LIMIT 1
    """)

    params = {
        "country": (country or "").strip(),
        "year": int(year),
        "month": (month_name or "").strip(),
        "user_currency": (user_currency or "").strip(),
        "selected_currency": (selected_currency or "").strip(),
    }

    with ADMIN_ENGINE.connect() as conn:
        row = conn.execute(sql, params).fetchone()

    if not row or row[0] is None:
        print("MISSING CONVERSION RATE:", params)
        return 1.0

    return float(row[0])


def _safe_float(v):
    try:
        return float(v or 0)
    except Exception:
        return 0.0
    
DAILY_MONEY_COLS = [
    "product_sales",
    "gross_sales",
    "net_sales",
    "profit",
    "platform_fee",
    "advertising",
    "rembursement_fee",
    "cogs",
    "cost_of_unit_sold",
    "selling_fees",
    "fba_fees",
]


def _convert_daily_series_to_usd(rows, rate):
    rate = float(rate or 1.0)
    out = []

    for r in rows or []:
        nr = dict(r)

        for col in DAILY_MONEY_COLS:
            if col in nr:
                nr[col] = _safe_float(nr.get(col)) * rate

        nr["country"] = "uk"
        nr["source_country"] = "uk"
        nr["currency"] = "USD"

        out.append(nr)

    return out


def _tag_daily_series(rows, country):
    out = []

    for r in rows or []:
        nr = dict(r)
        nr["country"] = country
        nr["source_country"] = country
        nr["currency"] = "USD"
        out.append(nr)

    return out


def _build_global_daily_series(us_rows, uk_rows_usd):
    by_date = {}

    for r in (us_rows or []) + (uk_rows_usd or []):
        d = r.get("date")
        if not d:
            continue

        if d not in by_date:
            by_date[d] = {
                "date": d,
                "country": "global",
                "currency": "USD",
            }

        for key, val in r.items():
            if key in ("date", "country", "source_country", "currency"):
                continue

            if isinstance(val, (int, float)):
                by_date[d][key] = _safe_float(by_date[d].get(key)) + _safe_float(val)

    return [by_date[d] for d in sorted(by_date.keys())]


def _get_total_row(items):
    for row in items or []:
        if str(row.get("sku", "")).upper() in ("TOTAL", "GRAND_TOTAL"):
            return row
    return {}


def _sum_daily_key(daily_rows, key):
    return sum(_safe_float(row.get(key)) for row in (daily_rows or []))


def _build_extra_totals(uk_daily, us_daily, uk_to_usd_rate):
    rate = float(uk_to_usd_rate or 1.0)

    uk_advertising = _sum_daily_key(uk_daily, "advertising") * rate
    uk_platform_fee = _sum_daily_key(uk_daily, "platform_fee") * rate
    uk_reimbursement = _sum_daily_key(uk_daily, "rembursement_fee") * rate

    us_advertising = _sum_daily_key(us_daily, "advertising")
    us_platform_fee = _sum_daily_key(us_daily, "platform_fee")
    us_reimbursement = _sum_daily_key(us_daily, "rembursement_fee")

    platform_fee_total = uk_platform_fee + us_platform_fee

    uk_amazon_fees = (
        abs(_sum_daily_key(uk_daily, "selling_fees")) * rate
        + abs(_sum_daily_key(uk_daily, "fba_fees")) * rate
    )

    us_amazon_fees = (
        abs(_sum_daily_key(us_daily, "selling_fees"))
        + abs(_sum_daily_key(us_daily, "fba_fees"))
    )

    return {
        "advertising": uk_advertising + us_advertising,
        "platform_fee": platform_fee_total,
        "rembursement_fee": uk_reimbursement + us_reimbursement,

        # amazon fees = selling fees + FBA fees
        "amazon_fees": uk_amazon_fees + us_amazon_fees,

        # keep 0 until cost_of_unit_sold/cogs is added into fetch_previous_period_data daily_series
        "cogs": (
            _sum_daily_key(uk_daily, "cogs") * rate
            + _sum_daily_key(us_daily, "cogs")
        ),
    }

def _build_extra_totals_single(daily_rows, rate=1.0):
    rate = float(rate or 1.0)

    advertising = _sum_daily_key(daily_rows, "advertising") * rate
    platform_fee = _sum_daily_key(daily_rows, "platform_fee") * rate
    reimbursement = _sum_daily_key(daily_rows, "rembursement_fee") * rate

    amazon_fees = (
        abs(_sum_daily_key(daily_rows, "selling_fees")) * rate
        + abs(_sum_daily_key(daily_rows, "fba_fees")) * rate
    )

    return {
        "advertising": advertising,
        "platform_fee": platform_fee,
        "rembursement_fee": reimbursement,
        "amazon_fees": amazon_fees,
        "cogs": _sum_daily_key(daily_rows, "cogs") * rate,
    }

def _build_aligned_totals(skuwise_items_global,extra_totals,total_previous_net_sales_full_month=None,total_previous_rembursement_fee_full_month=None,):
    total = _get_total_row(skuwise_items_global)

    net_sales = _safe_float(total.get("net_sales"))
    profit = _safe_float(total.get("profit"))

    advertising = _safe_float(extra_totals.get("advertising"))
    platform_fee = _safe_float(extra_totals.get("platform_fee"))
    reimbursement = _safe_float(extra_totals.get("rembursement_fee"))

    cm2_profit = profit - advertising - platform_fee
    cm2_percentage = (cm2_profit / net_sales) * 100 if net_sales else 0

    return {
        "total_previous_net_sales": round(net_sales, 2),
        "total_previous_profit": round(profit, 2),
        "total_previous_advertising": round(advertising, 2),
        "total_previous_platform_fees": round(platform_fee, 2),
        "total_previous_profit_cm2": round(cm2_profit, 2),
        "total_previous_profit_percentage": round(cm2_percentage, 2),
        "total_previous_rembursement_fee": round(
            _safe_float(
                total_previous_rembursement_fee_full_month
                if total_previous_rembursement_fee_full_month is not None
                else reimbursement
            ),
            2
        ),
        "total_previous_net_sales_full_month": float(
            total_previous_net_sales_full_month
            if total_previous_net_sales_full_month is not None
            else net_sales
        ),
    }


def _build_derived_totals_from_skuwise(skuwise_items, extra_totals):
    total = _get_total_row(skuwise_items)

    quantity = _safe_float(total.get("quantity"))
    gross_sales = _safe_float(total.get("gross_sales"))
    net_sales = _safe_float(total.get("net_sales"))
    profit = _safe_float(total.get("profit"))
    tax_and_credits = _safe_float(total.get("tax_and_credits"))

    advertising = _safe_float(extra_totals.get("advertising"))
    platform_fee = _safe_float(extra_totals.get("platform_fee"))
    selling_fees = _safe_float(total.get("selling_fees"))
    fba_fees = _safe_float(total.get("fba_fees"))

    amazon_fees_from_skuwise = abs(selling_fees) + abs(fba_fees)

    amazon_fees = (
        amazon_fees_from_skuwise
        if amazon_fees_from_skuwise
        else _safe_float(extra_totals.get("amazon_fees"))
    )
    cogs = _safe_float(extra_totals.get("cogs"))

    cm2_profit = profit - advertising - platform_fee

    return {
        "quantity": round(quantity, 2),
        "gross_sales": round(gross_sales, 2),
        "net_sales": round(net_sales, 2),
        "profit": round(profit, 2),
        "tax_and_credits": round(tax_and_credits, 2),

        "cogs": round(cogs, 2),
        "advertising_fees": round(advertising, 2),
        "amazon_fees": round(amazon_fees, 2),
        "platform_fees": round(platform_fee, 2),

        "cm2_profit": round(cm2_profit, 2),

        "asp": round(net_sales / quantity, 2) if quantity else 0,
        "profit_percentage": round((profit / net_sales) * 100, 2) if net_sales else 0,
        "cm2_profit_percentage": round((cm2_profit / net_sales) * 100, 2) if net_sales else 0,
    }


def _items_to_df(items, country):
    df = pd.DataFrame(items or [])
    if df.empty:
        return df
    df["country"] = country
    df["source_country"] = country
    for col in df.columns:
            if col not in ("sku", "product_name", "country", "source_country"):
                df[col] = pd.to_numeric(df[col], errors="ignore")
    return df

def _convert_uk_to_usd(df, rate):
    if df.empty:
        return df
    money_cols = [
        "product_sales",
        "gross_sales",
        "net_sales",
        "profit",
        "cogs",
        "ads_spend",
        "advertising",
        "platform_fee",
        "cm2_profit",
        "unit_wise_profitability",
        "selling_fees",
        "fba_fees",
        "tax_and_credits",  # ✅ NEW
    ]
    for col in money_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0) * float(rate)
    if "asp" in df.columns:
        df["asp"] = pd.to_numeric(df["asp"], errors="coerce").fillna(0) * float(rate)
    df["currency"] = "USD"
    return df

def _clean_product_name_value(value):
    if value is None:
        return None

    value = str(value).strip()

    if value.lower() in ("", "0", "nan", "none", "null"):
        return None

    return value.lower()


def _build_global_skuwise(us_df, uk_df):
    combined_df = pd.concat([us_df, uk_df], ignore_index=True)
    if combined_df.empty:
        return []
    if "product_name" not in combined_df.columns:
        combined_df["product_name"] = None
    if "sku" not in combined_df.columns:
        combined_df["sku"] = ""
    combined_df["product_name"] = combined_df["product_name"].apply(_clean_product_name_value)
    combined_df["sku"] = combined_df["sku"].fillna("").astype(str).str.strip()
    combined_df["product_name_group"] = combined_df.apply(
        lambda r: r["product_name"] if r["product_name"] else r["sku"],
        axis=1,
    )
    combined_df = combined_df[
        combined_df["product_name_group"].notna()
        & (combined_df["product_name_group"].astype(str).str.strip() != "")
    ].copy()
    if combined_df.empty:
        return []
    sum_cols = combined_df.select_dtypes(include=["number"]).columns.tolist()
    for remove_col in ["user_id", "year"]:
        if remove_col in sum_cols:
            sum_cols.remove(remove_col)
    global_df = combined_df.groupby("product_name_group", as_index=False)[sum_cols].sum()
    global_df.rename(columns={"product_name_group": "product_name"}, inplace=True)
    global_df["sku"] = ""
    global_df["country"] = "global"
    global_df["currency"] = "USD"
    if "quantity" in global_df.columns and "net_sales" in global_df.columns:
        global_df["asp"] = global_df.apply(
            lambda r: float(r["net_sales"]) / float(r["quantity"])
            if float(r.get("quantity", 0) or 0) else 0,
            axis=1,
        )
    if "quantity" in global_df.columns and "profit" in global_df.columns:
        global_df["unit_wise_profitability"] = global_df.apply(
            lambda r: float(r["profit"]) / float(r["quantity"])
            if float(r.get("quantity", 0) or 0) else 0,
            axis=1,
        )
    if "net_sales" in global_df.columns:
        total_net_sales = float(global_df["net_sales"].sum() or 0)
        global_df["sales_mix"] = (
            (global_df["net_sales"] / total_net_sales) * 100
            if total_net_sales else 0
        )
    return global_df.replace({np.nan: None}).to_dict(orient="records")

def _append_total_row(items, country):
    if not items:
        return items
    df = pd.DataFrame(items)
    numeric_cols = df.select_dtypes(include=["number"]).columns.tolist()
    total = {}
    for col in numeric_cols:
        total[col] = float(pd.to_numeric(df[col], errors="coerce").fillna(0).sum())
    qty = total.get("quantity", 0) or 0
    net_sales = total.get("net_sales", 0) or 0
    profit = total.get("profit", 0) or 0
    total["asp"] = net_sales / qty if qty else 0
    total["unit_wise_profitability"] = profit / qty if qty else 0
    total["sales_mix"] = 100.0
    total["sku"] = "TOTAL"
    total["product_name"] = "Total"
    total["country"] = country
    total["currency"] = "USD"
    if country in ("uk", "us"):
        total["source_country"] = country
    return items + [total]



def get_previous_global_data_for_live_bi(
    user_id,
    as_of=None,
    start_day=None,
    end_day=None,
):
    ranges = get_mtd_and_prev_ranges(
        as_of=as_of,
        start_day=start_day,
        end_day=end_day,
    )

    prev_start = ranges["previous"]["start"]
    prev_end = ranges["previous"]["end"]

    prev_month_name = month_name[prev_start.month].lower()
    prev_year = prev_start.year

    prev_full_start = date(prev_start.year, prev_start.month, 1)
    prev_full_end = date(
        prev_start.year,
        prev_start.month,
        monthrange(prev_start.year, prev_start.month)[1]
    )

    # -------------------------------------------------------------------------
    # SAFE FETCH HELPERS
    # -------------------------------------------------------------------------
    def safe_fetch_previous_period_data(user_id, country, start_date, end_date):
        try:
            items, daily = fetch_previous_period_data(
                user_id,
                country,
                start_date,
                end_date,
            )

            return items or [], daily or []

        except Exception as e:
           return [], []

    def safe_items_to_df(items, country):
        try:
            return _items_to_df(items or [], country)
        except Exception as e:
            print(f"[WARN] Failed to convert {country.upper()} items to df: {e}")
            return pd.DataFrame()

    def safe_append_total_row(items, country):
        try:
            return _append_total_row(items or [], country)
        except Exception as e:
            print(f"[WARN] Failed to append total row for {country.upper()}: {e}")
            return items or []

    def empty_aligned_totals():
        return {
            "total_previous_net_sales": 0,
            "total_previous_profit": 0,
            "total_previous_advertising": 0,
            "total_previous_platform_fees": 0,
            "total_previous_profit_cm2": 0,
            "total_previous_profit_percentage": 0,
            "total_previous_rembursement_fee": 0,
            "total_previous_net_sales_full_month": 0,
        }

    def empty_derived_totals():
        return {
            "quantity": 0,
            "gross_sales": 0,
            "net_sales": 0,
            "profit": 0,
            "tax_and_credits": 0,
            "cogs": 0,
            "advertising_fees": 0,
            "amazon_fees": 0,
            "platform_fees": 0,
            "cm2_profit": 0,
            "asp": 0,
            "profit_percentage": 0,
            "cm2_profit_percentage": 0,
        }

    def safe_build_aligned_totals(
        skuwise_items,
        extra_totals,
        total_previous_net_sales_full_month=0,
        total_previous_rembursement_fee_full_month=0,
    ):
        if not skuwise_items:
            return empty_aligned_totals()

        try:
            return _build_aligned_totals(
                skuwise_items,
                extra_totals,
                total_previous_net_sales_full_month=total_previous_net_sales_full_month,
                total_previous_rembursement_fee_full_month=total_previous_rembursement_fee_full_month,
            )
        except Exception as e:
            print(f"[WARN] Failed to build aligned totals: {e}")
            return empty_aligned_totals()

    def safe_build_derived_totals(skuwise_items, extra_totals):
        if not skuwise_items:
            return empty_derived_totals()

        try:
            return _build_derived_totals_from_skuwise(
                skuwise_items,
                extra_totals,
            )
        except Exception as e:
            print(f"[WARN] Failed to build derived totals: {e}")
            return empty_derived_totals()

    # -------------------------------------------------------------------------
    # FETCH PREVIOUS ALIGNED PERIOD SAFELY
    # -------------------------------------------------------------------------
    skuwise_items_uk_raw, uk_daily = safe_fetch_previous_period_data(
        user_id,
        "uk",
        prev_start,
        prev_end,
    )

    skuwise_items_us_raw, us_daily = safe_fetch_previous_period_data(
        user_id,
        "us",
        prev_start,
        prev_end,
    )

    # -------------------------------------------------------------------------
    # FETCH PREVIOUS FULL MONTH SAFELY
    # -------------------------------------------------------------------------
    _, uk_daily_full = safe_fetch_previous_period_data(
        user_id,
        "uk",
        prev_full_start,
        prev_full_end,
    )

    _, us_daily_full = safe_fetch_previous_period_data(
        user_id,
        "us",
        prev_full_start,
        prev_full_end,
    )

    uk_df = safe_items_to_df(skuwise_items_uk_raw, "uk")
    us_df = safe_items_to_df(skuwise_items_us_raw, "us")

    available_countries = []
    if not uk_df.empty:
        available_countries.append("uk")
    if not us_df.empty:
        available_countries.append("us")

    uk_to_usd_rate = fetch_conversion_rate(
        country="us",
        year=prev_year,
        month_name=prev_month_name,
        user_currency="gbp",
        selected_currency="usd",
    ) or 1.0

    # -------------------------------------------------------------------------
    # CONVERT UK TO USD SAFELY
    # -------------------------------------------------------------------------
    if not uk_df.empty:
        try:
            uk_df = _convert_uk_to_usd(uk_df, uk_to_usd_rate)
        except Exception as e:
            print(f"[WARN] Failed to convert UK previous data to USD: {e}")

    if not us_df.empty:
        us_df["currency"] = "USD"

    skuwise_items_uk = (
        uk_df.replace({np.nan: None}).to_dict(orient="records")
        if not uk_df.empty
        else []
    )

    skuwise_items_us = (
        us_df.replace({np.nan: None}).to_dict(orient="records")
        if not us_df.empty
        else []
    )

    # Build global from whichever country exists.
    try:
        skuwise_items_global = _build_global_skuwise(us_df, uk_df)
    except Exception as e:
        print(f"[WARN] Failed to build previous global skuwise items: {e}")
        skuwise_items_global = []

    skuwise_items_uk = safe_append_total_row(skuwise_items_uk, "uk")
    skuwise_items_us = safe_append_total_row(skuwise_items_us, "us")
    skuwise_items_global = safe_append_total_row(skuwise_items_global, "global")

    # -------------------------------------------------------------------------
    # EXTRA TOTALS SAFELY
    # -------------------------------------------------------------------------
    try:
        uk_extra = _build_extra_totals_single(uk_daily, uk_to_usd_rate)
    except Exception as e:
        print(f"[WARN] Failed to build UK extra totals: {e}")
        uk_extra = {}

    try:
        us_extra = _build_extra_totals_single(us_daily, 1.0)
    except Exception as e:
        print(f"[WARN] Failed to build US extra totals: {e}")
        us_extra = {}

    try:
        global_extra = _build_extra_totals(uk_daily, us_daily, uk_to_usd_rate)
    except Exception as e:
        print(f"[WARN] Failed to build global extra totals: {e}")
        global_extra = {}

    try:
        uk_full_totals = totals_from_daily_series(uk_daily_full)
    except Exception as e:
        print(f"[WARN] Failed to build UK full-month totals: {e}")
        uk_full_totals = {}

    try:
        us_full_totals = totals_from_daily_series(us_daily_full)
    except Exception as e:
        print(f"[WARN] Failed to build US full-month totals: {e}")
        us_full_totals = {}

    uk_total_previous_net_sales_full_month = (
        float(uk_full_totals.get("net_sales", 0) or 0)
        * float(uk_to_usd_rate)
    )

    us_total_previous_net_sales_full_month = float(
        us_full_totals.get("net_sales", 0) or 0
    )

    global_total_previous_net_sales_full_month = (
        uk_total_previous_net_sales_full_month
        + us_total_previous_net_sales_full_month
    )

    uk_total_previous_rembursement_fee_full_month = (
        sum(_safe_float(r.get("rembursement_fee")) for r in (uk_daily_full or []))
        * float(uk_to_usd_rate)
    )

    us_total_previous_rembursement_fee_full_month = sum(
        _safe_float(r.get("rembursement_fee")) for r in (us_daily_full or [])
    )

    global_total_previous_rembursement_fee_full_month = (
        uk_total_previous_rembursement_fee_full_month
        + us_total_previous_rembursement_fee_full_month
    )

    # -------------------------------------------------------------------------
    # ALIGNED TOTALS
    # -------------------------------------------------------------------------
    aligned_totals_uk = safe_build_aligned_totals(
        skuwise_items_uk,
        uk_extra,
        total_previous_net_sales_full_month=uk_total_previous_net_sales_full_month,
        total_previous_rembursement_fee_full_month=uk_total_previous_rembursement_fee_full_month,
    )

    aligned_totals_us = safe_build_aligned_totals(
        skuwise_items_us,
        us_extra,
        total_previous_net_sales_full_month=us_total_previous_net_sales_full_month,
        total_previous_rembursement_fee_full_month=us_total_previous_rembursement_fee_full_month,
    )

    aligned_totals_global = safe_build_aligned_totals(
        skuwise_items_global,
        global_extra,
        total_previous_net_sales_full_month=global_total_previous_net_sales_full_month,
        total_previous_rembursement_fee_full_month=global_total_previous_rembursement_fee_full_month,
    )

    # -------------------------------------------------------------------------
    # DERIVED TOTALS
    # -------------------------------------------------------------------------
    derived_totals_uk = safe_build_derived_totals(
        skuwise_items_uk,
        uk_extra,
    )

    derived_totals_us = safe_build_derived_totals(
        skuwise_items_us,
        us_extra,
    )

    derived_totals_global = safe_build_derived_totals(
        skuwise_items_global,
        global_extra,
    )

    payload_country = "global" if len(available_countries) > 1 else (
        available_countries[0] if available_countries else "global"
    )

    return {
        "success": True,
        "country": payload_country,
        "requested_country": "global",
        "available_countries": available_countries,
        "message": (
            "Previous GLOBAL data built from UK and US"
            if len(available_countries) > 1
            else (
                f"Previous GLOBAL fallback data built from {available_countries[0].upper()} only"
                if available_countries
                else "No previous UK or US data available"
            )
        ),
        "previous_period": {
            "prev_start": prev_start.isoformat(),
            "prev_end": prev_end.isoformat(),
            "month": prev_month_name,
            "year": prev_year,
        },
        "conversion": {
            "pair": "GBP->USD",
            "rate": float(uk_to_usd_rate),
        },

        "aligned_totals_global": aligned_totals_global,
        "aligned_totals_uk": aligned_totals_uk,
        "aligned_totals_us": aligned_totals_us,

        "derived_totals_global": derived_totals_global,
        "derived_totals_uk": derived_totals_uk,
        "derived_totals_us": derived_totals_us,

        "skuwise_items_uk": skuwise_items_uk,
        "skuwise_items_us": skuwise_items_us,
        "skuwise_items_global": skuwise_items_global,

        "uk_daily": uk_daily,
        "us_daily": us_daily,
    }



@live_data_bi_bp.route("/live_mtd_bi/previous_skuwise_global", methods=["GET"])
def previous_skuwise_global():
    import math

    def _json_safe(obj):
        if obj is None:
            return None
        if isinstance(obj, float):
            return obj if math.isfinite(obj) else None
        if isinstance(obj, dict):
            return {k: _json_safe(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [_json_safe(x) for x in obj]
        return obj

    # ---------------- Auth ----------------
    auth_header = request.headers.get("Authorization")
    if not auth_header or not auth_header.startswith("Bearer "):
        return jsonify({
            "success": False,
            "error": "Authorization token is missing or invalid"
        }), 401

    token = auth_header.split(" ")[1]

    try:
        payload, user_id, member_id = get_effective_user_id_from_token(token)
        user_id = int(payload.get("user_id"))
    except jwt.ExpiredSignatureError:
        return jsonify({"success": False, "error": "Token has expired"}), 401
    except jwt.InvalidTokenError:
        return jsonify({"success": False, "error": "Invalid token"}), 401
    except Exception:
        return jsonify({"success": False, "error": "Invalid token payload"}), 401

    # ---------------- Params ----------------
    as_of = request.args.get("as_of")
    start_day = request.args.get("start_day")
    end_day = request.args.get("end_day")

    try:
        start_day = int(start_day) if start_day else None
        end_day = int(end_day) if end_day else None
    except ValueError:
        start_day = None
        end_day = None

    # ---------------- Shared previous-global builder ----------------
    try:
        payload_out = get_previous_global_data_for_live_bi(
            user_id=user_id,
            as_of=as_of,
            start_day=start_day,
            end_day=end_day,
        )

        payload_out["success"] = True
        payload_out["message"] = "Previous-period global SKU-wise data"

        payload_out["count"] = {
            "uk": len(payload_out.get("skuwise_items_uk", [])),
            "us": len(payload_out.get("skuwise_items_us", [])),
            "global": len(payload_out.get("skuwise_items_global", [])),
        }

        return jsonify(_json_safe(payload_out)), 200

    except Exception as e:
        return jsonify({
            "success": False,
            "error": "Failed to read previous period UK/US data",
            "details": str(e),
        }), 500