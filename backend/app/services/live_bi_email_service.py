from datetime import datetime, timedelta, date
from dateutil.relativedelta import relativedelta
from calendar import monthrange, month_abbr
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import jwt
from config import Config
SECRET_KEY = Config.SECRET_KEY
from app import db
from app.routes.live_data_bi_routes import (
    fetch_user_objective,
    get_mtd_and_prev_ranges,
    fetch_previous_period_data,
    fetch_current_mtd_data,
    align_prev_curr_by_sku,
    totals_from_daily_series,
    aggregate_totals,
    compute_total_asp,
    compute_total_unit_profitability,
    build_ai_summary,
    run_live_prompt_1_analysis,
    run_live_prompt_1_5_summary,
    build_cm1_profit_pie_slices,
)
from app.utils.live_bi_utils import ( 
    build_rolling_monthly_series,
    build_movement_context,
    fetch_skuwisemonthly_ads_cm2_current_month,
    fetch_first_seen_sku_date,
    calculate_growth,
    build_segment_total_row,
    fetch_inventory_aged_by_user,
    build_portfolio_inventory_alerts,
    build_sku_context,
    fetch_estimated_storage_cost_next_month,
    fetch_sku_product_mapping,
    generate_sku_inventory_flags,
    generate_live_insight,
    render_live_recommended_action,
    monthrange, 

)
from app.utils.uk_time_series_utils import build_remaining_skus_time_series, build_rolling_sku_series
from app.utils.monthwise_ai_summary_utils import run_prompt_2_strategy


def build_live_mtd_bi_payload(
    user_id,
    country="uk",
    as_of=None,
    start_day=None,
    end_day=None,
    generate_ai_insights=False,
):
    # ✅ PRE-INITIALIZE
    portfolio_recommendation = None
    sku_strategy_actions = {}
    remaining_skus_reco = None
    remaining_skus_journey = None

    # ---------------------------
    # USER OBJECTIVE
    # ---------------------------
    user_objective = fetch_user_objective(user_id, country)

    # ---------------------------
    # DATE RANGE
    # ---------------------------
    ranges = get_mtd_and_prev_ranges(
        as_of=as_of,
        start_day=start_day,
        end_day=end_day,
    )
    prev_start = ranges["previous"]["start"]
    prev_end = ranges["previous"]["end"]
    curr_start = ranges["current"]["start"]
    curr_end = ranges["current"]["end"]

    today = ranges["meta"]["today"]

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

    prev_full_start = date(
        ranges["meta"]["previous_year"],
        ranges["meta"]["previous_month"],
        1,
    )
    last_day_prev = monthrange(prev_full_start.year, prev_full_start.month)[1]
    prev_full_end = date(prev_full_start.year, prev_full_start.month, last_day_prev)

    key_column = "sku"

    prev_data_aligned, prev_daily_aligned = fetch_previous_period_data(
        user_id, country, prev_start, prev_end
    )
    curr_data, curr_daily = fetch_current_mtd_data(
        user_id, country, curr_start, curr_end
    )

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

    prev_data_aligned, curr_data = align_prev_curr_by_sku(
        prev_data_aligned,
        curr_data,
    )

    if not curr_data:
        return {
            "status": "loading",
            "response_payload": {
                "status": "loading",
                "message": "Data is still syncing. Please wait a few seconds."
            }
        }

    _, prev_daily_full = fetch_previous_period_data(
        user_id, country, prev_full_start, prev_full_end
    )

    prev_full_totals = totals_from_daily_series(prev_daily_full)
    total_previous_net_sales_full_month = float(
        prev_full_totals.get("net_sales", 0) or 0
    )

    prev_fee_totals = totals_from_daily_series(prev_daily_aligned)
    curr_fee_totals = totals_from_daily_series(curr_daily)

    growth_data = calculate_growth(
        prev_data_aligned,
        curr_data,
        key=key_column,
    )

    prev_keys = {r.get(key_column) for r in prev_data_aligned if r.get(key_column)}
    curr_keys = {r.get(key_column) for r in curr_data if r.get(key_column)}

    first_seen_map = fetch_first_seen_sku_date(user_id, country)
    six_months_cutoff = curr_start - relativedelta(months=6)

    new_reviving_keys = {
        sku
        for sku in curr_keys
        if first_seen_map.get(sku) and first_seen_map[sku] >= six_months_cutoff
    }
    new_reviving_keys |= {sku for sku in curr_keys if sku not in first_seen_map}

    new_reviving = [
        r for r in growth_data
        if r.get(key_column) in new_reviving_keys
    ]

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

    remaining_growth_row = None
    top_keys = {r.get("sku") for r in top_80_skus}

    remaining_prev = [r for r in prev_data_aligned if r.get("sku") not in top_keys]
    remaining_curr = [r for r in curr_data if r.get("sku") not in top_keys]

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

    try:
        inv_df = fetch_inventory_aged_by_user(user_id)
        portfolio_inventory_alerts = build_portfolio_inventory_alerts(
            inv_df,
            user_id=user_id,
            country=country,
        )
    except Exception as e:
        print("[WARN] Failed to build portfolio inventory alerts:", e)
        portfolio_inventory_alerts = {}

    inventory_signals = {}

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

    prev_totals = aggregate_totals(prev_data_aligned)
    curr_totals = aggregate_totals(curr_data)
    prev_totals["total_asp"] = compute_total_asp(prev_data_aligned)
    curr_totals["total_asp"] = compute_total_asp(curr_data)
    prev_totals["unit_wise_profitability"] = compute_total_unit_profitability(prev_data_aligned)
    curr_totals["unit_wise_profitability"] = compute_total_unit_profitability(curr_data)

    sku_context = build_sku_context(growth_data, max_items=5)
    estimated_storage_cost_next_month = fetch_estimated_storage_cost_next_month(user_id)

    currency_map = {
        "uk": {"symbol": "£", "code": "GBP"},
        "us": {"symbol": "$", "code": "USD"},
    }
    currency = currency_map.get(country, {"symbol": "£", "code": "GBP"})

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

    payload_ai = build_ai_summary(
        prev_totals,
        curr_totals,
        top_80_skus,
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

    analysis = run_live_prompt_1_analysis(payload_ai)

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

    overall_summary_text = summary_out.get("summary_text", "")
    overall_summary_bullets = summary_out.get("metric_bullets", [])

    sku_live_context = []
    for r in top_80_skus:
        sku = r.get("sku")
        if not sku:
            continue

        growth_row = next((g for g in growth_data if g.get("sku") == sku), None)
        if not growth_row:
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
            }
        })

    try:
        sku_inventory_flags = generate_sku_inventory_flags(
            user_id=user_id,
            country=country,
            focus_skus=[r.get("sku") for r in top_80_skus],
        )
    except Exception as e:
        print("[WARN] Failed to build SKU inventory flags:", e)
        sku_inventory_flags = {}

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

    sku_time_series = {}
    for r in top_80_skus:
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

    remaining_series = []
    try:
        remaining_series = build_remaining_skus_time_series(
            user_id=user_id,
            country=country,
            focus_skus=[r.get("sku") for r in top_80_skus],
            anchor_year=anchor_year,
            anchor_month=anchor_month,
            months=24,
        )
    except Exception as e:
        print("[WARN] Remaining SKU series failed:", e)

    try:
        strategy_raw = run_prompt_2_strategy(
            analysis_insights=analysis,
            objective_v2=user_objective,
            focus_skus=[r.get("sku") for r in top_80_skus],
            sku_time_series=sku_time_series,
            inventory_alerts=payload_ai.get("inventory_signals", {}),
            sku_inventory_flags=sku_inventory_flags,
            country=country,
            sku_ads_context=sku_ads_context,
            sku_live_context=sku_live_context,
            ads_monthly=ads_monthly,
            remaining_skus_context={
                "aggregated_metrics": remaining_growth_row,
                "time_series": remaining_series
            } if remaining_growth_row else {},
        )
        strategy_parsed = json.loads(strategy_raw) if strategy_raw else {}
    except Exception as e:
        print("[STRATEGY ERROR]", e)
        strategy_parsed = {}

    portfolio_recommendation = strategy_parsed.get("portfolio_recommendation")
    sku_strategy_actions = strategy_parsed.get("sku_actions", {})
    remaining_skus_reco = strategy_parsed.get("remaining_skus_recommendation")
    remaining_skus_journey = strategy_parsed.get("remaining_skus_journey_summary")

    recommended_actions_mtd = {}
    for row in top_80_skus:
        sku = row.get("sku")
        if not sku:
            continue

        growth_row = next((g for g in growth_data if g.get("sku") == sku), None)
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

    remaining_skus_block = None
    if remaining_growth_row and remaining_skus_reco:
        remaining_skus_block = render_live_recommended_action(
            growth_row=remaining_growth_row,
            recommendation=remaining_skus_reco,
            journey_summary=remaining_skus_journey,
            currency_symbol=currency["symbol"],
        )

    overall_summary = {
        "summary_text": overall_summary_text,
        "metric_bullets": overall_summary_bullets,
    }
    overall_actions = sku_strategy_actions

    insights = {}
    if generate_ai_insights:
        skus_for_ai = top_80_skus + new_reviving + other_skus
        month2 = f"{curr_start.year}-{curr_start.month:02d}"

        with ThreadPoolExecutor(max_workers=10) as executor:
            for future in as_completed([
                executor.submit(
                    generate_live_insight,
                    item,
                    country,
                    prev_label,
                    curr_label,
                    user_id,
                    month2,
                )
                for item in skus_for_ai
            ]):
                key, res = future.result()
                insights[key] = res

    prev_aligned_totals = totals_from_daily_series(prev_daily_aligned)
    curr_aligned_totals = totals_from_daily_series(curr_daily)

    total_current_rembursement_fee = float(sum((r.get("rembursement_fee", 0) or 0) for r in (curr_daily or [])))
    total_previous_rembursement_fee = float(sum((r.get("rembursement_fee", 0) or 0) for r in (prev_daily_aligned or [])))

    total_current_profit = float(curr_aligned_totals.get("profit", 0) or 0)
    total_previous_profit = float(prev_aligned_totals.get("profit", 0) or 0)

    total_current_platform_fees = float(curr_aligned_totals.get("platform_fee", 0) or 0)
    total_previous_platform_fees = float(prev_aligned_totals.get("platform_fee", 0) or 0)

    total_current_advertising = float(curr_aligned_totals.get("advertising", 0) or 0)
    total_previous_advertising = float(prev_aligned_totals.get("advertising", 0) or 0)

    total_current_profit_cm2 = total_current_profit - total_current_advertising - total_current_platform_fees
    total_previous_profit_cm2 = total_previous_profit - total_previous_advertising - total_previous_platform_fees

    total_current_net_sales = float(curr_aligned_totals.get("net_sales", 0) or 0)
    total_previous_net_sales = float(prev_aligned_totals.get("net_sales", 0) or 0)

    total_current_profit_percentage = (
        (total_current_profit_cm2 / total_current_net_sales) * 100.0
        if total_current_net_sales != 0 else 0.0
    )
    total_previous_profit_percentage = (
        (total_previous_profit_cm2 / total_previous_net_sales) * 100.0
        if total_previous_net_sales != 0 else 0.0
    )

    aligned_totals_payload = {
        "total_current_profit": total_current_profit,
        "total_previous_profit": total_previous_profit,
        "total_current_profit_percentage": total_current_profit_percentage,
        "total_previous_profit_percentage": total_previous_profit_percentage,
        "total_current_platform_fees": total_current_platform_fees,
        "total_previous_platform_fees": total_previous_platform_fees,
        "total_current_advertising": total_current_advertising,
        "total_previous_advertising": total_previous_advertising,
        "total_current_net_sales": total_current_net_sales,
        "total_previous_net_sales": total_previous_net_sales,
        "total_previous_net_sales_full_month": float(total_previous_net_sales_full_month or 0),
        "total_current_profit_cm2": total_current_profit_cm2,
        "total_previous_profit_cm2": total_previous_profit_cm2,
        "total_current_rembursement_fee": total_current_rembursement_fee,
        "total_previous_rembursement_fee": total_previous_rembursement_fee,
    }

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
        "portfolio_inventory_alerts": payload_ai.get("portfolio_inventory_alerts", {}),
        "ai_insights": insights,
        "overall_summary": overall_summary,
        "overall_actions": overall_actions,
        "portfolio_recommendation": portfolio_recommendation,
        "recommended_actions_mtd": recommended_actions_mtd,
        "remaining_skus_recommendation": remaining_skus_reco,
        "remaining_skus_block": remaining_skus_block,
    }

    return {
        "status": "ok",
        "response_payload": response_payload,
        "overall_summary": overall_summary,
        "overall_actions": overall_actions,
        "recommended_actions_mtd": recommended_actions_mtd,
        "portfolio_recommendation": portfolio_recommendation,
        "prev_label": prev_label,
        "curr_label": curr_label,
        "country": country,
        "sku_to_product": sku_to_product,
    }

