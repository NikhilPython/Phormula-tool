from flask import Blueprint, request, jsonify
import jwt
import os
from sqlalchemy import create_engine
from dotenv import load_dotenv
from config import Config
from calendar import month_abbr, monthrange
from datetime import date, datetime, timedelta
from openai import OpenAI
import json
import pandas as pd
from dateutil.relativedelta import relativedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from app.utils.live_bi_utils import ( build_movement_context, generate_sku_inventory_flags, build_rolling_monthly_series, compute_total_asp, compute_total_unit_profitability, fetch_sku_product_mapping, fetch_skuwisemonthly_ads_cm2_current_month, fetch_user_objective, generate_inventory_alerts_for_all_skus, get_mtd_and_prev_ranges,fetch_previous_period_data,fetch_current_mtd_data,calculate_growth,aggregate_totals,build_segment_total_row,build_sku_context,build_ai_summary,generate_live_insight,fetch_historical_skus_last_6_months, render_live_recommended_action,round_numeric_values, run_inventory_ai_summary, run_live_prompt_1_5_summary, run_live_prompt_1_analysis, totals_from_daily_series,construct_prev_table_name,compute_sku_metrics_from_df,
                                     compute_inventory_coverage_ratio,fetch_estimated_storage_cost_next_month,fetch_first_seen_sku_date,)
from app.utils.email_utils import (send_live_bi_email,get_user_email_by_id,has_recent_bi_email,mark_bi_email_sent,)
from app.utils.monthwise_ai_summary_utils import run_prompt_2_strategy
from app.utils.token_utils import get_effective_user_id_from_token


# -----------------------------------------------------------------------------
# ENV / DB SETUP
# -----------------------------------------------------------------------------

load_dotenv()
SECRET_KEY = Config.SECRET_KEY

db_url = os.getenv("DATABASE_URL")
db_url2 = os.getenv("DATABASE_AMAZON_URL")

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
        # HISTORIC MOVEMENT CONTEXT (24 MONTHS)
        # --------------------------------------------
        today = ranges["meta"]["today"]

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

        # ---------------------------
        # FETCH DATA
        # ---------------------------
        prev_data_aligned, prev_daily_aligned = fetch_previous_period_data(
            user_id, country, prev_start, prev_end
        )

        curr_data, curr_daily = fetch_current_mtd_data(
            user_id, country, curr_start, curr_end
        )

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

        for row in (curr_data or []):
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
        prev_data_aligned, curr_data = align_prev_curr_by_sku(
            prev_data_aligned,
            curr_data,
        )
        # ---------------------------
        # DATA STILL WARMING UP
        # ---------------------------
        if not curr_data:
            return jsonify({
                "status": "loading",
                "message": "Data is still syncing. Please wait a few seconds."
            }), 202

        # ---------------------------
        # FULL PREVIOUS MONTH (for charts)
        # ---------------------------
        _, prev_daily_full = fetch_previous_period_data(
            user_id, country, prev_full_start, prev_full_end
        )

        prev_full_totals = totals_from_daily_series(prev_daily_full)
        total_previous_net_sales_full_month = float(
            prev_full_totals.get("net_sales", 0) or 0
        )

        # ---------------------------
        # PLATFORM FEES + ADS (SUMMARY ONLY)
        # ---------------------------
        prev_fee_totals = totals_from_daily_series(prev_daily_aligned)
        curr_fee_totals = totals_from_daily_series(curr_daily)

        # ---------------------------
        # GROWTH
        # ---------------------------
        growth_data = calculate_growth(
            prev_data_aligned,
            curr_data,
            key=key_column,
            
        )

       

        prev_keys = {r.get(key_column) for r in prev_data_aligned if r.get(key_column)}
        curr_keys = {r.get(key_column) for r in curr_data if r.get(key_column)}

        # ---------------------------
        # NEW / REVIVING (AGE-BASED)
        # ---------------------------

        # 1) First-seen date per SKU
        first_seen_map = fetch_first_seen_sku_date(user_id, country)

        # 2) Cutoff = 6 months before current period start
        six_months_cutoff = curr_start - relativedelta(months=6)

        # 3) New / Reviving = launched within last 6 months
        new_reviving_keys = {
            sku
            for sku in curr_keys
            if first_seen_map.get(sku) and first_seen_map[sku] >= six_months_cutoff
        }

        # safety: always treat current SKUs without history as new
        new_reviving_keys |= {
            sku for sku in curr_keys if sku not in first_seen_map
        }

        new_reviving = [
            r for r in growth_data
            if r.get(key_column) in new_reviving_keys
        ]

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
        new_keys = {r.get(key_column) for r in new_reviving}

        prev_top = [r for r in prev_data_aligned if r.get(key_column) in top_keys]
        curr_top = [r for r in curr_data if r.get(key_column) in top_keys]

        prev_other = [r for r in prev_data_aligned if r.get(key_column) in other_keys]
        curr_other = [r for r in curr_data if r.get(key_column) in other_keys]

        prev_new = [r for r in prev_data_aligned if r.get(key_column) in new_keys]
        curr_new = [r for r in curr_data if r.get(key_column) in new_keys]

        top_80_total_row = build_segment_total_row(
            prev_top, curr_top, key=key_column, label="Total"
        )
        other_total_row = (
            build_segment_total_row(prev_other, curr_other, key=key_column, label="Total")
            if other_skus else None
        )
        new_reviving_total_row = (
            build_segment_total_row(prev_new, curr_new, key=key_column, label="Total")
            if new_reviving else None
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
            r for r in curr_data
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
        # INVENTORY ALERTS (FOR AI SUMMARY)
        # ---------------------------
        try:
            inventory_signals = generate_inventory_alerts_for_all_skus(user_id, country)
        except Exception as e:
            print("[WARN] Failed to build inventory alerts:", e)
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
        curr_totals = aggregate_totals(curr_data)
        prev_totals["total_asp"] = compute_total_asp(prev_data_aligned)
        curr_totals["total_asp"] = compute_total_asp(curr_data)

        # ✅ FIX
        prev_totals["unit_wise_profitability"] = compute_total_unit_profitability(prev_data_aligned)
        curr_totals["unit_wise_profitability"] = compute_total_unit_profitability(curr_data)

        sku_context = build_sku_context(growth_data, max_items=5)
        estimated_storage_cost_next_month = fetch_estimated_storage_cost_next_month(user_id)

        currency_map = {
            "uk": {"symbol": "£", "code": "GBP"},
            "us": {"symbol": "$", "code": "USD"},
        }
        currency = currency_map.get(country, {"symbol": "£", "code": "GBP"})

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


        # ---------------------------
        # BUILD PAYLOAD (NO AI HERE)
        # ---------------------------
        payload_ai = build_ai_summary(
            prev_totals,
            curr_totals,
            top_80_skus,
            # new_reviving,
            prev_label,
            curr_label,
            sku_context=sku_context,
            inventory_signals=inventory_signals,
            prev_fee_totals=prev_fee_totals,
            curr_fee_totals=curr_fee_totals,
            estimated_storage_cost_next_month=estimated_storage_cost_next_month,
            currency=currency,
            user_objective=user_objective,
            movement_context=movement_context,
            sku_to_product=sku_to_product, 
            # ✅ NEW
            user_id=user_id,
            country=country,
            current_year=ranges["meta"]["current_year"],
            current_month=ranges["meta"]["current_month"],
        )

       

        # ---------------------------
        # INVENTORY AI SUMMARY (PORTFOLIO LEVEL)
        # ---------------------------
        inventory_summary = None

        inventory_payload = payload_ai.get("inventory_signals", {})
        inventory_alerts = inventory_payload.get("summary", [])

        if inventory_alerts:
            try:
                inventory_summary = run_inventory_ai_summary({
                    "summary": inventory_alerts
                })

                # attach raw alerts for UI drill-down
                inventory_summary["alerts"] = inventory_alerts

            except Exception as e:
                print("[WARN] Inventory AI summary failed:", e)
                inventory_summary = {
                    "summary_text": "",
                    "alert_bullets": [],
                    "alerts": inventory_alerts,
                }


        # ---------------------------
        # PROMPT-1 (ANALYSIS)
        # ---------------------------
        analysis = run_live_prompt_1_analysis(payload_ai)

       
        # ---------------------------
        # PROMPT-1.5 (EXECUTIVE SUMMARY)
        # ---------------------------
        summary_out = run_live_prompt_1_5_summary(
            analysis_output=analysis,
            numeric_context={
                "periods": payload_ai["periods"],
                "pct_changes": payload_ai["pct_changes"],
                "selling_costs": payload_ai["selling_costs"],
                "roas": payload_ai["roas"],
                "movement_context": payload_ai["movement_context"],
                "currency": payload_ai["currency"],
            },
            user_objective=user_objective,
        )

     
        # ===========================
        # EXTRACT EXECUTIVE SUMMARY  ✅ (THIS IS STEP 3)
        # ===========================
        overall_summary_text = summary_out.get("summary_text", "")
        overall_summary_bullets = summary_out.get("metric_bullets", [])




        # ---------------------------
        # STRATEGY ENGINE (MONTH-END PROMPT 2)
        # ---------------------------
        sku_live_context = []

        for r in top_80_skus:
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

        # -------------------------------------------------
        # 🔥 SKU-LEVEL INVENTORY FLAGS (FOR PROMPT-2)
        # -------------------------------------------------
        try:
            sku_inventory_flags = generate_sku_inventory_flags(
                user_id=user_id,
                country=country,
                focus_skus=[r.get("sku") for r in top_80_skus],
            )
        except Exception as e:
            print("[WARN] Failed to build SKU inventory flags:", e)
            sku_inventory_flags = {}

            


        # ==========================================
        # 🔥 ADD THIS BLOCK RIGHT HERE
        # ==========================================

        sku_ads_context = []

        for r in top_80_skus:
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

        

        # ---------------------------------------
        # STRATEGY ENGINE (SAFE WRAPPED)
        # ---------------------------------------

        try:
            strategy_raw = run_prompt_2_strategy(
                analysis_insights=analysis,
                objective_v2=user_objective,
                focus_skus=[r.get("sku") for r in top_80_skus],
                sku_time_series={},   # optional
                inventory_alerts=payload_ai.get("inventory_signals", {}),
                sku_inventory_flags=sku_inventory_flags,
                country=country,
                sku_ads_context=sku_ads_context,
                sku_live_context=sku_live_context,
                ads_monthly=ads_monthly,
                remaining_skus_context={
                    "aggregated_metrics": remaining_growth_row
                } if remaining_growth_row else {},
            )

            strategy_parsed = json.loads(strategy_raw) if strategy_raw else {}

        except Exception as e:
            print("[STRATEGY ERROR]", e)
            strategy_parsed = {}

        # Safe extraction (always executes)
        portfolio_recommendation = strategy_parsed.get("portfolio_recommendation")
        sku_strategy_actions = strategy_parsed.get("sku_actions", {})
        remaining_skus_reco = strategy_parsed.get("remaining_skus_recommendation")
        remaining_skus_journey = strategy_parsed.get("remaining_skus_journey_summary")

        # ===========================
        # BUILD RECOMMENDED ACTIONS
        # ===========================

        recommended_actions_mtd = {}

        for row in top_80_skus:
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
            skus_for_ai = top_80_skus + new_reviving + other_skus

            # month2 = current MTD month in YYYY-MM format
            month2 = f"{curr_start.year}-{curr_start.month:02d}"

            with ThreadPoolExecutor(max_workers=10) as executor:
                for future in as_completed([
                    executor.submit(
                        generate_live_insight,
                        item,
                        country,
                        prev_label,
                        curr_label,
                        user_id,   # ✅ NEW
                        month2,    # ✅ NEW
                    )
                    for item in skus_for_ai
                ]):
                    key, res = future.result()
                    insights[key] = res
            

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

        aligned_totals_payload = {
            "total_current_profit": total_current_profit,
            "total_previous_profit": total_previous_profit,

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

            "periods": {
                "previous": {"label": prev_label},
                "previous_full": {"label": prev_label_full},
                "current_mtd": {"label": curr_label},
            },
            "aligned_totals": aligned_totals_payload,
            "categorized_growth": {
                "top_80_skus": top_80_skus,
                "top_80_total": top_80_total_row,
                "new_or_reviving_skus": new_reviving,
                "new_or_reviving_total": new_reviving_total_row,
                "other_skus": other_skus,
                "other_total": other_total_row,
            },
            "cm1_profit_pie": cm1_profit_pie,

            "daily_series": {
                "previous": prev_daily_full,
                "current_mtd": curr_daily,
            },
            "daily_series_aligned": {
                "previous": prev_daily_aligned,
                "current_mtd": curr_daily,
            },

            "inventory_summary": inventory_summary,
            "ai_insights": insights,
            "overall_summary": overall_summary,
            "overall_actions": overall_actions,
            "portfolio_recommendation": portfolio_recommendation,

            # 🔥 SKU CARDS
            "recommended_actions_mtd": recommended_actions_mtd,

            # 🔥 Remaining SKUs
            "remaining_skus_recommendation": remaining_skus_reco,
            "remaining_skus_block": remaining_skus_block,   # ✅ ADD THIS
        }

        # ---------------------------
        # SEND EMAIL (USING FINAL RESPONSE PAYLOAD)
        # ---------------------------
        user_email = payload.get("email") or request.args.get("email")
        if not user_email:
            user_email = get_user_email_by_id(user_id)

        if user_email:
            cache_key = (user_id, country)
            if cache_key not in _SENT_EMAIL_CACHE:
                if not has_recent_bi_email(user_id, country, hours=24):
                    try:
                        email_token_payload = {
                            "user_id": user_id,
                            "email": user_email,
                            "scope": "live_mtd_bi",
                            "exp": datetime.utcnow() + timedelta(hours=24),
                        }
                        email_token = jwt.encode(
                            email_token_payload,
                            SECRET_KEY,
                            algorithm="HS256",
                        )

                        send_live_bi_email(
                            to_email=user_email,
                            overall_summary=response_payload["overall_summary"],
                            overall_actions=response_payload["overall_actions"],
                            sku_actions=recommended_actions_mtd,
                            
                            country=country,
                            prev_label=prev_label,
                            curr_label=curr_label,
                            deep_link_token=email_token,
                        )



                        mark_bi_email_sent(user_id, country)
                        _SENT_EMAIL_CACHE.add(cache_key)

                    except Exception as e:
                        print("[WARN] Error sending live BI email:", e)

        return jsonify(round_numeric_values(response_payload, ndigits=2)), 200

    except Exception as e:
        print("Unexpected error in /live_mtd_bi:", e)
        return jsonify({"error": "Server error", "details": str(e)}), 500




