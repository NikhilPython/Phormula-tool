from __future__ import annotations







from calendar import month_name, monthrange



from datetime import date, datetime, timezone



from io import BytesIO



from typing import Any, Iterable







import pandas as pd



from sqlalchemy import func







from app.models.user_models import StoredFile



from app.routes.inventory_current_routes import (



    GLOBAL_COUNTRIES,



    build_high_alert_coverage_summary,



    build_inventory_categories,



    attach_current_month_units_sold,



    attach_historical_storage_cost,



    attach_inventory_monthly_qty,



    attach_zero_to_180_days,



    fetch_current_month_units_sold_map,



    fetch_previous_platform_fee_as_storage_cost,



    get_age_summary_for_single_country,



    get_previous_month_name_year,



    resolve_inventory_current_source,



)



from app.utils.currency_utils import _get_gbp_to_usd_rate



from app.utils.live_bi_utils import (



    fetch_current_ai_values_from_skuwisemonthly,



    fetch_current_mtd_data,



    totals_from_daily_series,



)











ACTION_THRESHOLDS = {



    "inventory_coverage_months": 2.0,



    "tacos_percent": 20.0,



    "promotional_rebate_percent": 5.0,



}







PRIORITY_ORDER = {"Critical": 0, "High": 1, "Medium": 2, "Opportunity": 3}











def _number(value: Any) -> float:



    if value is None or value == "":



        return 0.0



    try:



        if isinstance(value, str):



            value = value.replace(",", "").replace("%", "").strip()



        number = float(value)



        return number if number == number and number not in (float("inf"), float("-inf")) else 0.0



    except (TypeError, ValueError):



        return 0.0











def _first_number(row: dict[str, Any], *keys: str) -> float:



    zero = 0.0



    for key in keys:



        if key not in row or row.get(key) in (None, ""):



            continue



        value = _number(row.get(key))



        if value != 0:



            return value



        zero = value



    return zero











def _money(value: float, symbol: str) -> str:



    absolute = abs(value)



    if absolute >= 1_000_000:



        return f"{symbol}{absolute / 1_000_000:.1f}M"



    if absolute >= 1_000:



        return f"{symbol}{absolute / 1_000:.1f}K"



    if absolute < 100:



        return f"{symbol}{absolute:,.2f}"



    return f"{symbol}{absolute:,.0f}"











def _month_number(month: str) -> int:



    normalized = str(month or "").strip().lower()



    for index in range(1, 13):



        if month_name[index].lower() == normalized:



            return index



    raise ValueError(f"Invalid month_name: {month}")











def _period_end(month: str, year: int) -> date:



    month_number = _month_number(month)



    today = date.today()



    if year == today.year and month_number == today.month:



        day = today.day



    else:



        day = monthrange(year, month_number)[1]



    return date(year, month_number, day)











def _period_bounds(



    month: str,



    year: int,



    start_day: int | None = None,



    end_day: int | None = None,



) -> tuple[date, date]:



    maximum_end = _period_end(month, year)



    resolved_start_day = max(1, min(int(start_day or 1), maximum_end.day))



    resolved_end_day = max(



        resolved_start_day,



        min(int(end_day or maximum_end.day), maximum_end.day),



    )



    return (



        maximum_end.replace(day=resolved_start_day),



        maximum_end.replace(day=resolved_end_day),



    )











def _is_total_row(row: dict[str, Any]) -> bool:



    sku = str(row.get("sku") or row.get("SKU") or "").strip().lower()



    product = str(row.get("product_name") or row.get("Product Name") or "").strip().lower()



    return sku in {"total", "totals", "grand_total", "grand total"} or product in {



        "total",



        "totals",



        "grand_total",



        "grand total",



    }











def _inventory_current_fba(row: dict[str, Any]) -> float:
    return _first_number(
        row,
        "Sellable Units",
        "available",
        "Current Inventory FBA",
        "current_inventory_fba",
        "current-fba",
        "fulfillable_quantity",
        "available_quantity",
    )


def _inventory_current_awd(row: dict[str, Any]) -> float:
    return _first_number(
        row,
        "total_onhand_quantity",
        "Current Inventory AWD",
        "current_inventory_awd",
        "current-awd",
        "available_awd",
        "awd_available",
    )


def _inventory_transit_fba(row: dict[str, Any]) -> float:
    return _first_number(
        row,
        "inbound-shipped\r",
        "inbound-shipped",
        "In Transit FBA",
        "in_transit_fba",
        "in-transit-fba",
        "fc-transfer",
        "fc_transfer",
        "reserved_fc_transfer",
        "inbound-working",
    )


def _inventory_transit_awd(row: dict[str, Any]) -> float:
    return _first_number(
        row,
        "total_inbound_quantity",
        "In Transit AWD",
        "in_transit_awd",
        "in-transit-awd",
        "inbound_quantity",
        "inbound-quantity",
        "Inbound Units",
        "inbound_units",
        "inbound-shipped",
    )


def _inventory_total_in_transit(row: dict[str, Any]) -> float:
    for key in (
        "total_transit",
        "Total Sellable Inventory In Transit",
        "Total Sellable In Transit",
    ):
        if key in row and row.get(key) not in (None, ""):
            return max(0.0, _number(row.get(key)))

    return max(0.0, _inventory_transit_fba(row)) + max(
        0.0, _inventory_transit_awd(row)
    )


def _inventory_storage_cost(rows: list[dict[str, Any]]) -> float:
    total_row = next((row for row in rows if _is_total_row(row)), None)
    if total_row and "estimated-storage-cost-next-month" in total_row:
        return abs(_number(total_row.get("estimated-storage-cost-next-month")))

    return sum(
        abs(_number(row.get("estimated-storage-cost-next-month")))
        for row in rows
        if not _is_total_row(row)
    )


def _load_inventory_facts(



    *, user_id: str, country: str, month: str, year: int



) -> dict[str, Any]:



    countries = GLOBAL_COUNTRIES if country == "global" else [country]



    high_alert_items: list[dict[str, Any]] = []



    high_alert_coverage_weighted_sum = 0.0



    high_alert_sku_count = 0



    high_alert_thresholds: list[float] = []



    liquidate_items: list[dict[str, Any]] = []



    age_totals: dict[str, float] = {}



    # Total inventory represented by the ageing summary. This includes both



    # sellable and unsellable inventory so "% of Total" matches the ageing table.



    age_inventory_total = 0.0

    current_fba_units = 0.0
    current_awd_units = 0.0
    transit_fba_units = 0.0
    transit_awd_units = 0.0
    in_transit_units = 0.0
    healthy_units = 0.0
    ageing_inventory_units = 0.0
    current_month_units_sold = 0.0
    gross_units = 0.0
    returned_units = 0.0
    returned_sku_count = 0
    estimated_storage_cost = 0.0
    previous_storage_cost = 0.0
    storage_components: list[dict[str, Any]] = []
    total_sku_count = 0
    healthy_sku_count = 0
    in_transit_sku_count = 0



    loaded_countries: list[str] = []



    age_loaded_countries: list[str] = []



    errors: list[str] = []







    for child_country in countries:



        try:



            source = resolve_inventory_current_source(



                user_id=user_id,



                country_key=child_country,



                range_type="monthly",



                month_name=month,



                year=str(year),



                quarter=None,



            )



            if not source.get("found"):



                errors.append(f"{child_country}: inventory source unavailable")



                continue







            rows = source.get("rows") or []

            selected_month = source.get("selected_month") or month
            units_result = attach_current_month_units_sold(
                rows=rows,
                columns=source.get("columns") or [],
                user_id=user_id,
                country_key=child_country,
                month_name=selected_month,
                year=str(year),
            )
            rows = units_result.get("rows") or rows
            columns = units_result.get("columns") or source.get("columns") or []

            monthly_result = attach_inventory_monthly_qty(
                rows=rows,
                columns=columns,
                user_id=user_id,
                country_key=child_country,
                month_name=selected_month,
                year=str(year),
                use_currentinventory_sellable=(
                    source.get("source_type") == "currentinventory_table"
                ),
            )
            rows = monthly_result.get("rows") or rows
            columns = monthly_result.get("columns") or columns

            zero_to_180_result = attach_zero_to_180_days(
                rows=rows,
                columns=columns,
                use_currentinventory_split=(
                    source.get("source_type") == "currentinventory_table"
                ),
            )
            rows = zero_to_180_result.get("rows") or rows
            rows = attach_historical_storage_cost(
                rows,
                user_id,
                child_country,
                selected_month,
                str(year),
            )

            sales_metrics = fetch_current_month_units_sold_map(
                user_id=user_id,
                country_key=child_country,
                month_name=selected_month,
                year=str(year),
            )
            current_month_units_sold += max(
                0.0,
                _number(sales_metrics.get("total_units")),
            )
            gross_units += max(0.0, _number(sales_metrics.get("gross_units")))
            returned_units += max(
                0.0,
                _number(sales_metrics.get("returned_units")),
            )
            returned_sku_count += int(
                max(0.0, _number(sales_metrics.get("returned_sku_count")))
            )

            current_storage_cost_native = _inventory_storage_cost(rows)
            previous_result = fetch_previous_platform_fee_as_storage_cost(
                user_id=user_id,
                country_key=child_country,
                month_number=_month_number(selected_month),
                year_int=int(year),
            )
            previous_storage_cost_native = abs(
                _number(previous_result.get("value"))
            )

            current_rate = 1.0
            previous_rate = 1.0

            if country == "global" and child_country == "uk":
                try:
                    current_rate = _get_gbp_to_usd_rate(selected_month, int(year))
                    previous_month, _previous_month_number, previous_year = (
                        get_previous_month_name_year(selected_month, int(year))
                    )
                    previous_rate = _get_gbp_to_usd_rate(
                        previous_month or selected_month,
                        previous_year or int(year),
                    )
                except Exception as exc:
                    errors.append(f"uk storage currency conversion: {exc}")

            current_storage_cost = current_storage_cost_native * current_rate
            child_previous_storage_cost = (
                previous_storage_cost_native * previous_rate
            )

            estimated_storage_cost += current_storage_cost
            previous_storage_cost += child_previous_storage_cost
            storage_components.append(
                {
                    "country": child_country,
                    "native_currency": "GBP" if child_country == "uk" else "USD",
                    "current_native": round(current_storage_cost_native, 2),
                    "previous_native": round(previous_storage_cost_native, 2),
                    "current_to_display_rate": round(current_rate, 8),
                    "previous_to_display_rate": round(previous_rate, 8),
                    "current_display": round(current_storage_cost, 2),
                    "previous_display": round(child_previous_storage_cost, 2),
                }
            )







            # Match the donut chart EXACTLY. The donut does NOT use



            # \`Sellable Units\` as its first-choice denominator. It sums the



            # displayed ageing buckets plus Unsellable.



            #



            # Split source: 0-90 + 91-180 + 181-270 + 271-365 + 365+ + Unsellable



            # Combined source: 0-180 + 181-270 + 271-365 + 365+ + Unsellable



            data_rows = [row for row in rows if not _is_total_row(row)]

            country_current_fba = sum(_inventory_current_fba(row) for row in data_rows)
            country_current_awd = sum(_inventory_current_awd(row) for row in data_rows)
            country_transit_fba = sum(_inventory_transit_fba(row) for row in data_rows)
            country_transit_awd = sum(_inventory_transit_awd(row) for row in data_rows)
            total_row = next((row for row in rows if _is_total_row(row)), None)
            country_in_transit = (
                _inventory_total_in_transit(total_row)
                if total_row is not None
                else sum(_inventory_total_in_transit(row) for row in data_rows)
            )

            current_fba_units += country_current_fba
            current_awd_units += country_current_awd
            transit_fba_units += country_transit_fba
            transit_awd_units += country_transit_awd
            in_transit_units += country_in_transit

            total_sku_count += sum(
                1
                for row in data_rows
                if _inventory_current_fba(row) + _inventory_current_awd(row) > 0
            )
            in_transit_sku_count += sum(
                1
                for row in data_rows
                if _inventory_total_in_transit(row) > 0
                and _inventory_current_fba(row) + _inventory_current_awd(row) > 0
            )







            split_first_180 = sum(



                _number(row.get("inv-age-0-to-90-days"))



                + _number(row.get("inv-age-91-to-180-days"))



                for row in data_rows



            )



            combined_first_180 = sum(



                _number(row.get("inv-age-0-to-180-days"))



                for row in data_rows



            )







            first_180_units = (



                split_first_180



                if split_first_180 > 0



                else combined_first_180



            )







            older_units = sum(



                _number(row.get("inv-age-181-to-270-days"))



                + _number(row.get("inv-age-271-to-365-days"))



                + _number(row.get("inv-age-365-plus-days"))



                for row in data_rows



            )

            use_split_first_180 = split_first_180 > 0
            healthy_column = (
                "inv-age-0-to-90-days"
                if use_split_first_180
                else "inv-age-0-to-180-days"
            )
            healthy_units += sum(
                _number(row.get(healthy_column))
                for row in data_rows
            )
            healthy_sku_count += sum(
                1
                for row in data_rows
                if _number(row.get(healthy_column)) > 0
            )
            ageing_inventory_units += older_units







            unfulfillable_units = sum(



                _number(row.get("unfulfillable-quantity"))



                for row in data_rows



            )







            country_age_inventory_total = (



                first_180_units + older_units + unfulfillable_units



            )







            age_inventory_total += country_age_inventory_total







            # Reuse the inventory API's coverage-summary logic with the Action



            # Items policy threshold supplied explicitly.



            high_alert = build_high_alert_coverage_summary(
                rows,
                user_id,
                child_country,
            )



            summary_items = high_alert.get("items") or []



            summary_count = (



                int(_number(high_alert.get("high_alert_sku_count")))



                or len(summary_items)



            )



            summary_average = _number(high_alert.get("average_coverage_ratio"))



            if summary_count > 0:



                high_alert_coverage_weighted_sum += summary_average * summary_count



                high_alert_sku_count += summary_count







            for item in summary_items:



                enriched = dict(item)



                enriched["country"] = child_country



                high_alert_items.append(enriched)







            threshold = _number(high_alert.get("high_alert_threshold"))



            if threshold:



                high_alert_thresholds.append(threshold)







            categories = build_inventory_categories(rows)







            # Build a lookup of monthly/30-day units sold from the original



            # inventory rows before they are reduced to category items.



            units_sold_by_sku: dict[str, float] = {}







            for row in rows:



                sku = str(



                    row.get("SKU")



                    or row.get("sku")



                    or ""



                ).strip()







                if not sku:



                    continue







                units_sold = 0.0







                # The inventory API uses a dynamic column such as:



                # "Current Month Units Sold (August)"



                for key, value in row.items():



                    if str(key).startswith("Current Month Units Sold"):



                        units_sold = _number(value)



                        break







                units_sold_by_sku[sku] = units_sold











            for item in (categories.get("liquidate") or {}).get("items") or []:



                enriched = dict(item)



                enriched["country"] = child_country







                sku = str(



                    item.get("sku")



                    or item.get("SKU")



                    or ""



                ).strip()







                enriched["current_month_units_sold"] = units_sold_by_sku.get(



                    sku,



                    0.0,



                )







                liquidate_items.append(enriched)







            loaded_countries.append(child_country)







            # Reuse the same service function that powers



            # /inventory_current_age_summary, then retain only selected totals.



            try:



                age_summary = get_age_summary_for_single_country(



                    user_id=user_id,



                    country_key=child_country,



                    month_name=month,



                    year=str(year),



                )



                for key, value in (age_summary.get("selected_month_totals") or {}).items():



                    age_totals[key] = age_totals.get(key, 0.0) + _number(value)







                age_loaded_countries.append(child_country)



            except Exception as exc:



                errors.append(f"{child_country} age summary: {exc}")



        except Exception as exc:



            errors.append(f"{child_country}: {exc}")







    return {



        "loaded": bool(loaded_countries),



        "age_loaded": bool(age_loaded_countries),



        "countries": loaded_countries,



        "errors": errors,



        "high_alert_items": high_alert_items,



        "high_alert_sku_count": high_alert_sku_count,



        "average_coverage_ratio": (



            high_alert_coverage_weighted_sum / high_alert_sku_count



            if high_alert_sku_count



            else 0.0



        ),



        "high_alert_threshold": max(high_alert_thresholds, default=0.0),



        "liquidate_items": liquidate_items,



        "age_totals": age_totals,



        "age_inventory_total": age_inventory_total,

        "current_fba_units": current_fba_units,
        "current_awd_units": current_awd_units,
        "transit_fba_units": transit_fba_units,
        "transit_awd_units": transit_awd_units,
        "in_transit_units": in_transit_units,
        "healthy_units": healthy_units,
        "healthy_sku_count": healthy_sku_count,
        "ageing_inventory_units": ageing_inventory_units,
        "current_month_units_sold": current_month_units_sold,
        "gross_units": gross_units,
        "returned_units": returned_units,
        "returned_sku_count": returned_sku_count,
        "estimated_storage_cost": estimated_storage_cost,
        "previous_storage_cost": previous_storage_cost,
        "storage_components": storage_components,
        "total_sku_count": total_sku_count,
        "in_transit_sku_count": in_transit_sku_count,
        "storage_currency": (
            {"code": "USD", "symbol": "$"}
            if country in {"global", "us"}
            else {"code": "GBP", "symbol": "£"}
            if country == "uk"
            else {"code": "CAD", "symbol": "$"}
        ),



    }











def _load_live_rows(



    *,



    user_id: int,



    country: str,



    month: str,



    year: int,



    start_day: int | None = None,



    end_day: int | None = None,



) -> dict[str, Any]:



    period_start, period_end = _period_bounds(



        month,



        year,



        start_day=start_day,



        end_day=end_day,



    )



    errors: list[str] = []







    if country == "global":



        try:



            # This helper produces the same GBP->USD-normalized global rows used



            # by /live_mtd_bi, avoiding incorrect cross-currency aggregation.



            from app.routes.amazon_api_routes import get_current_global_data_for_live_bi







            payload = get_current_global_data_for_live_bi(user_id)



            rows = payload.get("skuwise_items_global") or []



            totals = payload.get("derived_totals_global") or {}



            return {



                "loaded": bool(rows or totals),



                "rows": [row for row in rows if not _is_total_row(row)],



                "totals": totals,



                "aligned_totals": {},



                "fee_totals": {},



                "currency": {"code": "USD", "symbol": "$"},



                "errors": errors,



            }



        except Exception as exc:



            errors.append(str(exc))



            return {



                "loaded": False,



                "rows": [],



                "totals": {},



                "aligned_totals": {},



                "fee_totals": {},



                "currency": {"code": "USD", "symbol": "$"},



                "errors": errors,



            }







    rows, totals, fee_totals = fetch_current_ai_values_from_skuwisemonthly(



        user_id=user_id,



        country=country,



        curr_end=period_end,



    )







    # Use the same aligned current-period facts that /live_mtd_bi uses for



    # portfolio totals. The monthly table remains the productwise source for



    # CM2, returns and promotional-rebate SKU attribution.



    current_mtd_rows: list[dict[str, Any]] = []



    aligned_totals: dict[str, float] = {}



    try:



        current_mtd_rows, current_mtd_daily = fetch_current_mtd_data(



            user_id=user_id,



            country=country,



            curr_start=period_start,



            curr_end=period_end,



        )



        if current_mtd_daily:



            aligned_totals = totals_from_daily_series(current_mtd_daily)



    except Exception as exc:



        errors.append(str(exc))







    # The current-month monthly table may still be warming up. Live rows are a



    # data-only productwise fallback and do not invoke either AI prompt.



    if (



        not rows



        and period_end.year == date.today().year



        and period_end.month == date.today().month



    ):



        rows = current_mtd_rows







    if country == "uk":



        currency = {"code": "GBP", "symbol": "£"}



    elif country == "ca":



        currency = {"code": "CAD", "symbol": "$"}



    else:



        currency = {"code": "USD", "symbol": "$"}



    return {



        "loaded": bool(rows or totals or aligned_totals),



        "rows": [row for row in rows if not _is_total_row(row)],



        "totals": totals or {},



        "aligned_totals": aligned_totals,



        "fee_totals": fee_totals or {},



        "currency": currency,



        "errors": errors,



    }











def _latest_dispatch_file(



    *, user_id: int, country: str, month: str, year: int



) -> StoredFile | None:



    return (



        StoredFile.query



        .filter(



            StoredFile.user_id == int(user_id),



            func.lower(StoredFile.country) == country.lower(),



            StoredFile.kind == "dispatch",



            func.lower(StoredFile.month) == month.lower(),



            StoredFile.year == str(year),



        )



        .order_by(StoredFile.id.desc())



        .first()



    )











def _load_dispatch_facts(



    *, user_id: int, country: str, month: str, year: int



) -> dict[str, Any]:



    errors: list[str] = []



    files: list[StoredFile] = []







    if country == "global":



        global_file = _latest_dispatch_file(



            user_id=user_id,



            country="global",



            month=month,



            year=year,



        )



        if global_file:



            files.append(global_file)



        else:



            for child_country in GLOBAL_COUNTRIES:



                child_file = _latest_dispatch_file(



                    user_id=user_id,



                    country=child_country,



                    month=month,



                    year=year,



                )



                if child_file:



                    files.append(child_file)



    else:



        dispatch_file = _latest_dispatch_file(



            user_id=user_id,



            country=country,



            month=month,



            year=year,



        )



        if dispatch_file:



            files.append(dispatch_file)







    air_dispatch = 0.0



    sea_dispatch = 0.0



    affected_product_keys: set[str] = set()



    affected_skus: set[str] = set()

    air_dispatch_skus: set[str] = set()

    sea_dispatch_skus: set[str] = set()



    loaded_files: list[str] = []







    for stored_file in files:



        try:



            file_bytes = stored_file.data



            if isinstance(file_bytes, memoryview):



                file_bytes = file_bytes.tobytes()



            if not file_bytes:



                raise ValueError("stored dispatch file is empty")







            workbook = pd.ExcelFile(BytesIO(file_bytes))



            dispatch_sheet = next(



                (



                    sheet



                    for sheet in workbook.sheet_names



                    if str(sheet).strip().lower() == "dispatch"



                ),



                workbook.sheet_names[0] if workbook.sheet_names else None,



            )



            if not dispatch_sheet:



                raise ValueError("dispatch worksheet is missing")







            frame = pd.read_excel(workbook, sheet_name=dispatch_sheet)



            frame.columns = [str(column).strip() for column in frame.columns]







            if "AIR" not in frame.columns or "SEA" not in frame.columns:



                raise ValueError("AIR or SEA column is missing")







            for row_index, row in frame.iterrows():



                record = row.to_dict()



                if _is_total_row(record):



                    continue







                air_units = max(0.0, _number(record.get("AIR")))



                sea_units = max(0.0, _number(record.get("SEA")))



                if air_units + sea_units <= 0:



                    continue







                air_dispatch += air_units



                sea_dispatch += sea_units







                sku = str(record.get("sku") or record.get("SKU") or "").strip()



                product_name = str(



                    record.get("Product Name")



                    or record.get("product_name")



                    or ""



                ).strip()



                product_key = (



                    f"sku:{sku.upper()}"



                    if sku



                    else f"product:{product_name.lower()}"



                    if product_name



                    else f"row:{stored_file.id}:{row_index}"



                )



                affected_product_keys.add(product_key)



                if sku:



                    affected_skus.add(sku)



                    if air_units > 0:



                        air_dispatch_skus.add(sku.upper())



                    if sea_units > 0:



                        sea_dispatch_skus.add(sku.upper())







            loaded_files.append(stored_file.filename)



        except Exception as exc:



            errors.append(f"{stored_file.country}: {exc}")







    return {



        "loaded": bool(loaded_files),



        "files": loaded_files,



        "air_dispatch": air_dispatch,



        "sea_dispatch": sea_dispatch,



        "product_count": len(affected_product_keys),



        "affected_skus": sorted(affected_skus),



        "air_dispatch_sku_count": len(air_dispatch_skus),



        "sea_dispatch_sku_count": len(sea_dispatch_skus),



        "errors": errors,



    }











def _build_inventory_summary_overview(
    inventory: dict[str, Any],
    dispatch: dict[str, Any],
    action_items: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Build the Summary Overview inventory cards entirely on the backend."""
    current_fba = max(0.0, _number(inventory.get("current_fba_units")))
    current_awd = max(0.0, _number(inventory.get("current_awd_units")))
    total_units = current_fba + current_awd
    transit_units = max(0.0, _number(inventory.get("in_transit_units")))
    if "in_transit_units" not in inventory:
        transit_units = max(
            0.0, _number(inventory.get("transit_fba_units"))
        ) + max(0.0, _number(inventory.get("transit_awd_units")))
    units_sold = max(0.0, _number(inventory.get("current_month_units_sold")))
    gross_units = max(0.0, _number(inventory.get("gross_units")))
    returned_units = max(0.0, _number(inventory.get("returned_units")))

    coverage_ratio = (
        (total_units + transit_units) / units_sold if units_sold > 0 else 0.0
    )
    return_rate = returned_units / gross_units * 100.0 if gross_units > 0 else 0.0
    return_rate_skus = int(inventory.get("returned_sku_count") or 0)

    return_action = next(
        (
            item
            for item in action_items or []
            if item.get("id") == "high-return-rate"
        ),
        None,
    )
    if return_action:
        return_rate_metric = next(
            (
                metric
                for metric in return_action.get("metrics") or []
                if str(metric.get("label") or "").strip().lower()
                == "return rate"
            ),
            None,
        )
        if return_rate_metric:
            return_rate = abs(_number(return_rate_metric.get("value")))

        affected_skus = return_action.get("affected_skus") or []
        affected_products = return_action.get("affected_products") or []
        if affected_skus or affected_products:
            return_rate_skus = len(affected_skus or affected_products)

    storage_cost = max(0.0, _number(inventory.get("estimated_storage_cost")))
    previous_storage_cost = max(
        0.0, _number(inventory.get("previous_storage_cost"))
    )
    storage_delta_value = storage_cost - previous_storage_cost
    has_storage_comparison = previous_storage_cost > 0
    storage_delta_percentage = (
        storage_delta_value / previous_storage_cost * 100.0
        if has_storage_comparison
        else None
    )
    storage_currency = inventory.get("storage_currency") or {
        "code": "USD",
        "symbol": "$",
    }
    storage_symbol = str(storage_currency.get("symbol") or "$")

    high_alert_items = inventory.get("high_alert_items") or []
    high_alert_units = sum(
        max(0.0, _number(item.get("unit_count")))
        for item in high_alert_items
        if isinstance(item, dict)
    )

    return {
        "total_units": int(round(total_units)),
        "total_skus": int(inventory.get("total_sku_count") or 0),
        "current_fba_percentage": round(
            current_fba / total_units * 100.0 if total_units > 0 else 0.0, 2
        ),
        "current_awd_percentage": round(
            current_awd / total_units * 100.0 if total_units > 0 else 0.0, 2
        ),
        "inventory_coverage_ratio": round(coverage_ratio, 2),
        "ageing_inventory_units": int(
            round(max(0.0, _number(inventory.get("ageing_inventory_units"))))
        ),
        "healthy_units": int(
            round(max(0.0, _number(inventory.get("healthy_units"))))
        ),
        "healthy_skus": int(inventory.get("healthy_sku_count") or 0),
        "high_alert_units": int(round(high_alert_units)),
        "high_alert_skus": int(inventory.get("high_alert_sku_count") or 0),
        "return_rate": round(return_rate, 2),
        "return_rate_skus": return_rate_skus,
        "in_transit_units": int(round(transit_units)),
        "in_transit_skus": int(inventory.get("in_transit_sku_count") or 0),
        "sea_dispatch_units": int(
            round(max(0.0, _number(dispatch.get("sea_dispatch"))))
        ),
        "sea_dispatch_skus": int(dispatch.get("sea_dispatch_sku_count") or 0),
        "air_dispatch_units": int(
            round(max(0.0, _number(dispatch.get("air_dispatch"))))
        ),
        "air_dispatch_skus": int(dispatch.get("air_dispatch_sku_count") or 0),
        "estimated_storage_cost": round(storage_cost, 2),
        "estimated_storage_cost_display": f"{storage_symbol}{storage_cost:,.0f}",
        "estimated_storage_cost_delta_value": (
            round(storage_delta_value, 2) if has_storage_comparison else None
        ),
        "estimated_storage_cost_delta_display": (
            f"{storage_symbol}{storage_delta_value:,.0f}"
            if has_storage_comparison
            else None
        ),
        "estimated_storage_cost_delta_percentage": (
            round(storage_delta_percentage, 2)
            if storage_delta_percentage is not None
            else None
        ),
        "storage_currency": storage_currency,
        "storage_components": inventory.get("storage_components") or [],
        "calculation_basis": {
            "coverage": "(current FBA + current AWD + in-transit FBA + in-transit AWD) / current-month units sold",
            "return_rate": "returned units / gross units",
            "global_storage": "US USD + (UK GBP × period GBP-to-USD rate)",
        },
    }


def _sum(rows: Iterable[dict[str, Any]], *keys: str) -> float:



    return sum(_first_number(row, *keys) for row in rows)











def _get_action_item_source_metrics(live: dict[str, Any]) -> dict[str, float]:



    """Return the authoritative portfolio metrics used by Action Items.







    TACoS deliberately comes from the skuwisemonthly total/GRAND TOTAL source,



    because this is the database value used by the Action Items contract.



    The same values are also exposed to the frontend via \`source_metrics\` so



    Business Scenario can display the exact same source instead of Live BI.



    """



    rows = live.get("rows") or []



    totals = live.get("totals") or {}



    aligned_totals = live.get("aligned_totals") or {}



    fee_totals = live.get("fee_totals") or {}







    net_sales = (



        _first_number(totals, "net_sales")



        or _number(aligned_totals.get("net_sales"))



        or _sum(rows, "net_sales")



    )







    ads_spend = (



        _first_number(totals, "total_ads", "ads_spend", "advertising")



        or _first_number(fee_totals, "advertising")



        or abs(_number(aligned_totals.get("advertising")))



        or _sum(rows, "total_ads", "ads_spend", "advertising")



    )







    tacos = abs(



        _first_number(



            totals,



            "tacos_total_advertising_cost_of_sale",



            "tacos",



        )



    )







    return {



        "net_sales": net_sales,



        "ads_spend": ads_spend,



        "tacos": tacos,



    }











def build_action_items(



    *,



    inventory: dict[str, Any],



    live: dict[str, Any],



    dispatch: dict[str, Any] | None = None,



) -> list[dict[str, Any]]:



    """Build the stable, UI-ready ActionItem contract from compact facts."""



    items: list[dict[str, Any]] = []



    rows = live.get("rows") or []



    totals = live.get("totals") or {}



    aligned_totals = live.get("aligned_totals") or {}



    fee_totals = live.get("fee_totals") or {}



    symbol = (live.get("currency") or {}).get("symbol") or "$"







    dispatch = dispatch or {}



    dispatch_product_count = int(_number(dispatch.get("product_count")))



    if dispatch.get("loaded") and dispatch_product_count > 0:



        air_dispatch = _number(dispatch.get("air_dispatch"))



        sea_dispatch = _number(dispatch.get("sea_dispatch"))



        items.append({



            "id": "dispatch-required",



            "category": "Inventory & Dispatch",



            "priority": "Critical",



            "title": "Dispatch plan requires action",



            "reason": (



                f"{dispatch_product_count} "



                f"product{'s' if dispatch_product_count != 1 else ''} require "



                "air or sea dispatch."



            ),



            "metrics": [



                {"value": f"{air_dispatch:,.0f}", "label": "Air dispatch"},



                {"value": f"{sea_dispatch:,.0f}", "label": "Sea dispatch"},



                {"value": str(dispatch_product_count), "label": "Products"},



            ],



            "action": "Review dispatch",



            "affected_skus": dispatch.get("affected_skus") or [],



            "air_dispatch_sku_count": int(_number(dispatch.get("air_dispatch_sku_count"))),



            "sea_dispatch_sku_count": int(_number(dispatch.get("sea_dispatch_sku_count"))),



        })







    high_alert_items = inventory.get("high_alert_items") or []



    if high_alert_items:



        affected_average = _number(inventory.get("average_coverage_ratio"))



        affected_count = int(_number(inventory.get("high_alert_sku_count"))) or len(high_alert_items)



        threshold = _number(inventory.get("high_alert_threshold"))



        items.append({



            "id": "inventory-coverage-risk",



            "category": "Inventory & Dispatch",



            "priority": "Critical" if affected_average < 1 else "High",



            "title": "Inventory coverage needs action",



            "reason": (



                f"{affected_count} "



                f"{'SKUs are' if affected_count != 1 else 'SKU is'} at or below "



                f"the {threshold:.2f}-month replenishment threshold."



            ),



            "metrics": [



                {"value": f"{affected_average:.2f} mo", "label": "Affected avg"},



                {"value": str(affected_count), "label": "Affected SKUs"},



                {"value": f"{threshold:.2f} mo", "label": "Threshold"},



            ],



            "action": "Plan replenishment",



            "affected_skus": [item.get("sku") for item in high_alert_items if item.get("sku")],



        })







    age_totals = inventory.get("age_totals") or {}



    liquidate_items = inventory.get("liquidate_items") or []







    units_181_270 = _number(



        age_totals.get("inv-age-181-to-270-days")



    )







    units_271_365 = _number(



        age_totals.get("inv-age-271-to-365-days")



    )







    units_365_plus = _number(



        age_totals.get("inv-age-365-plus-days")



    )







    total_aged_units = units_181_270 + units_271_365 + units_365_plus







    # Match the ageing table's "% of Total" calculation:



    # inventory aged 181+ days / complete inventory (sellable + unsellable) * 100.



    total_inventory_units = _number(inventory.get("age_inventory_total"))



    # Match the percentages displayed in the ageing table exactly:



    # calculate each 181+ bucket percentage, round each to 2 decimals,



    # then add them together. This avoids a 0.01 difference caused by



    # rounding only after aggregating all aged units.



    if total_inventory_units > 0:



        pct_181_270 = round((units_181_270 / total_inventory_units) * 100.0, 2)



        pct_271_365 = round((units_271_365 / total_inventory_units) * 100.0, 2)



        pct_365_plus = round((units_365_plus / total_inventory_units) * 100.0, 2)



        aged_percent_of_total = pct_181_270 + pct_271_365 + pct_365_plus



    else:



        aged_percent_of_total = 0.0







    if liquidate_items and total_aged_units > 0:



        items.append({



            "id": "aged-inventory",



            "category": "Inventory & Dispatch",



            "priority": "High" if units_365_plus > 0 else "Medium",



            "title": "Aged inventory requires a clearance plan",



            "reason": (



                f"{len(liquidate_items)} "



                f"product{'s' if len(liquidate_items) != 1 else ''} "



                "contain inventory aged 181 days or more."



            ),



            "metrics": [



                {



                    "value": f"{aged_percent_of_total:.2f}%",



                    "label": " of Total",



                },



                {



                    "value": f"{total_aged_units:,.0f}",



                    "label": "Aged units",



                },



                {



                    "value": str(len(liquidate_items)),



                    "label": "Products",



                },



            ],



            "action": "Plan liquidation",



            "affected_skus": [



                item.get("sku") or item.get("SKU")



                for item in liquidate_items



                if item.get("sku") or item.get("SKU")



            ],



        })







    # One authoritative source for the Ads action item and the frontend card.



    source_metrics = _get_action_item_source_metrics(live)



    net_sales = source_metrics["net_sales"]



    ads_spend = source_metrics["ads_spend"]



    tacos = source_metrics["tacos"]







    # Product drill-down for the Ads action item.



    # The portfolio action still triggers from the authoritative total TACoS, while



    # the click-through table is narrowed to SKUs whose own TACoS is >= 20%.



    ads_tacos_threshold = ACTION_THRESHOLDS["tacos_percent"]



    ads_affected_skus: list[str] = []
    ads_affected_products: list[str] = []







    for row in rows:



        sku = str(row.get("sku") or row.get("SKU") or "").strip()
        product_name = str(
            row.get("product_name")
            or row.get("Product Name")
            or ""
        ).strip()



        # Country rows are SKU based. Global rows are aggregated by product_name
        # and deliberately have a blank SKU, so keep either identifier.
        if not sku and not product_name:



            continue







        row_net_sales = abs(_first_number(row, "net_sales", "Net Sales"))



        row_ads_spend = abs(



            _first_number(



                row,



                "total_ads",



                "ads_spend",



                "advertising",



            )



        )







        # Some productwise payloads expose the ad components instead of ads_spend.



        if row_ads_spend == 0:



            row_ads_spend = (



                abs(_number(row.get("product_spend")))



                + abs(_number(row.get("display_spend")))



                + abs(_number(row.get("brand_spend")))



            )







        # Product TACoS used by the focused view must match the table/diagnosis:

        # total product ad spend / product net sales * 100.

        # Do not prefer a stored row TACoS field here because it can be stale or

        # calculated from a different source, which previously allowed products

        # below the 20% threshold into affected_skus.

        row_tacos = (

            row_ads_spend / row_net_sales * 100.0

            if row_net_sales > 0

            else 0.0

        )



        if row_tacos >= ads_tacos_threshold:



            if sku:
                ads_affected_skus.append(sku)
            if product_name:
                ads_affected_products.append(product_name)







    # Keep the response stable and avoid duplicate focused-view identifiers.



    ads_affected_skus = list(dict.fromkeys(ads_affected_skus))
    ads_affected_products = list(dict.fromkeys(ads_affected_products))







    if ads_spend and tacos >= ads_tacos_threshold:



        items.append({



            "id": "ads-efficiency",



            "category": "Ads",



            "priority": "Critical" if tacos >= 25 else "High",



            "title": "Advertising efficiency needs attention",



            "reason": f"TACoS is {tacos:.2f}%, so advertising is taking a high share of net sales.",



            "metrics": [



                {"value": f"{tacos:.2f}%", "label": "TACoS"},



                {"value": _money(ads_spend, symbol), "label": "Ad spend"},



                {"value": _money(net_sales, symbol), "label": "Net sales"},



            ],



            "action": "Optimize campaigns",



            "affected_skus": ads_affected_skus,
            "affected_products": ads_affected_products,



        })







    # Find the product with the highest promotional rebate percentage.



    top_rebate = max(



        rows,



        key=lambda row: abs(



            _first_number(



                row,



                "promotional_rebates_percentage",



            )



        ),



        default={},



    )







    top_rebate_percent = abs(



        _first_number(



            top_rebate,



            "promotional_rebates_percentage",



        )



    )







    # Show the action only when at least one product exceeds 10%.



    # Since top_rebate is the product with the highest percentage,



    # checking this one product is enough.



    if top_rebate and top_rebate_percent > 10:



        top_rebate_amount = abs(



            _first_number(



                top_rebate,



                "promotional_rebates",



            )



        )







        top_rebate_net_sales = abs(



            _first_number(



                top_rebate,



                "net_sales",



            )



        )







        product_name = str(



            top_rebate.get("product_name")



            or top_rebate.get("Product Name")



            or "—"



        )







        items.append({



            "id": "promotional-rebates",



            "category": "Finance",



            "priority": "High",



            "title": "Promotional rebate leakage",



            "reason": (



                f"{product_name} has promotional rebates of "



                f"{top_rebate_percent:.2f}% of net sales."



            ),



            "metrics": [



                {



                    "value": _money(top_rebate_net_sales, symbol),



                    "label": "Net sales",



                },



                {



                    "value": f"{top_rebate_percent:.2f}%",



                    "label": "Promo %",



                },



                {



                    "value": str(



                        top_rebate.get("product_name")



                        or top_rebate.get("Product Name")



                        or "—"



                    ),



                    "label": "Top rebate product",



                },



            ],







            "action": "Review promotions",



            "affected_skus": [



                str(top_rebate.get("sku"))



            ] if top_rebate.get("sku") else [],
            "affected_products": [product_name] if product_name and product_name != "—" else [],



        })







    negative_profit = sorted(



        [row for row in rows if _number(row.get("cm2_profit")) < 0],



        key=lambda row: _number(row.get("cm2_profit")),



    )







    if negative_profit:



        total_loss = abs(



            sum(



                _number(row.get("cm2_profit"))



                for row in negative_profit



            )



        )







        # negative_profit is sorted from most negative CM2 to least negative.



        # Therefore the first row is the worst CM2 product.



        worst_product = negative_profit[0]







        items.append({



            "id": "negative-profit-skus",



            "category": "Finance",



            "priority": (



                "High"



                if len(negative_profit) >= 5 or total_loss >= 1000



                else "Medium"



            ),



            "title": "Negative-CM2 SKUs",



            "reason": (



                f"{len(negative_profit)} "



                f"SKU{'s are' if len(negative_profit) != 1 else ' is'} "



                "currently below zero CM2 profit."



            ),



            "metrics": [



                {



                    "value": str(len(negative_profit)),



                    "label": "SKUs",



                },



                {



                    "value": _money(total_loss, symbol),



                    "label": "Total loss",



                },



                {



                    "value": str(



                        worst_product.get("product_name")



                        or worst_product.get("Product Name")



                        or "—"



                    ),



                    "label": "Lowest CM2 Product",



                },



            ],



            "action": "Reprice / pause",



            "affected_skus": [



                str(row.get("sku"))



                for row in negative_profit



                if row.get("sku")



            ],
            "affected_products": list(dict.fromkeys(
                str(row.get("product_name") or row.get("Product Name") or "").strip()
                for row in negative_profit
                if str(row.get("product_name") or row.get("Product Name") or "").strip()
            )),



        })



    return_rows: list[tuple[dict[str, Any], float, float, float]] = []







    for row in rows:



        quantity = abs(



            _first_number(



                row,



                "return_rate_base_quantity",



                "quantity",



                "total_quantity",



            )



        )







        returns = abs(



            _first_number(



                row,



                "return_quantity",



                "returned_quantity",



            )



        )







        rate = returns / quantity * 100.0 if quantity else 0.0







        if rate > 2.0:



            return_rows.append((row, returns, rate, quantity))











    if return_rows:



        product_count = len(return_rows)



        return_quantity_sum = sum(



            returns for _row, returns, _rate, _quantity in return_rows



        )



        return_quantity_base = sum(



            quantity for _row, _returns, _rate, quantity in return_rows



        )



        aggregate_return_rate = (



            return_quantity_sum / return_quantity_base * 100.0



            if return_quantity_base



            else 0.0



        )







        items.append({



            "id": "high-return-rate",



            "category": "Returns",



            "priority": "High" if aggregate_return_rate >= 5 else "Medium",



            "title": "Product returns",



            "reason": (



                f"{product_count} "



                f"product{'s are' if product_count != 1 else ' is'} "



                "above the 2.00% return-rate threshold."



            ),



            "metrics": [



                {



                    "value": str(product_count),



                    "label": "Products",



                },



                {



                    "value": f"{aggregate_return_rate:.2f}%",



                    "label": "Return rate",



                },



                {



                    "value": f"{return_quantity_sum:,.0f}",



                    "label": "Return qty",



                },



            ],



            "action": "Inspect root cause",



            "affected_skus": [



                str(row.get("sku"))



                for row, _returns, _rate, _quantity in return_rows



                if row.get("sku")



            ],
            "affected_products": list(dict.fromkeys(
                str(row.get("product_name") or row.get("Product Name") or "").strip()
                for row, _returns, _rate, _quantity in return_rows
                if str(row.get("product_name") or row.get("Product Name") or "").strip()
            )),



        })







    items.sort(key=lambda item: PRIORITY_ORDER.get(item.get("priority"), 99))



    return items[:7]











def get_dashboard_action_items(



    *,



    user_id: int | str,



    country: str,



    month: str,



    year: int,



    start_day: int | None = None,



    end_day: int | None = None,



) -> dict[str, Any]:



    normalized_country = str(country or "").strip().lower()



    if normalized_country not in {"uk", "us", "ca", "global"}:



        raise ValueError("country_key must be one of: uk, us, ca, global")







    normalized_month = month_name[_month_number(month)].lower()



    inventory = _load_inventory_facts(



        user_id=str(user_id),



        country=normalized_country,



        month=normalized_month,



        year=int(year),



    )



    live = _load_live_rows(



        user_id=int(user_id),



        country=normalized_country,



        month=normalized_month,



        year=int(year),



        start_day=start_day,



        end_day=end_day,



    )



    dispatch = _load_dispatch_facts(



        user_id=int(user_id),



        country=normalized_country,



        month=normalized_month,



        year=int(year),



    )



    if (



        not inventory.get("loaded")



        and not live.get("loaded")



        and not dispatch.get("loaded")



    ):



        raise RuntimeError("No action-item source data is available for the selected period")







    items = build_action_items(



        inventory=inventory,



        live=live,



        dispatch=dispatch,



    )



    source_metrics = _get_action_item_source_metrics(live)
    inventory_summary_overview = _build_inventory_summary_overview(
        inventory,
        dispatch,
        items,
    )







    return {



        "success": True,



        "period": {



            "country": normalized_country,



            "month": normalized_month,



            "year": int(year),



            "start_day": start_day,



            "end_day": end_day,



        },



        "currency": live.get("currency") or {"code": "USD", "symbol": "$"},



        "source_metrics": source_metrics,
        "inventory_summary_overview": inventory_summary_overview,



        "items": items,



        "total_items": len(items),



        "partial": not (inventory.get("loaded") and live.get("loaded")),



        "source_status": {



            "inventory_current": bool(inventory.get("loaded")),



            "inventory_current_age_summary": bool(inventory.get("age_loaded")),



            "live_mtd_bi": bool(live.get("loaded")),



            "dispatch": bool(dispatch.get("loaded")),



        },



        "source_errors": {



            "inventory": inventory.get("errors") or [],



            "live_bi": live.get("errors") or [],



            "dispatch": dispatch.get("errors") or [],



        },



        "generated_at": datetime.now(timezone.utc).isoformat(),



    }
