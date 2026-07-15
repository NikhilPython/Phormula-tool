from __future__ import annotations

import calendar
import os
from typing import Any, Dict, List
from zoneinfo import ZoneInfo

import pandas as pd
from sqlalchemy import create_engine, text

from app.models.user_models import amazon_user
from app.utils.amazon_utils import (
    _month_date_range_utc,
    _month_date_range_us_pacific_utc,
    _flatten_transaction_to_row,
    run_upload_pipeline_from_df,
    dedupe_rows_by_order_id,
    amazon_client,
)


def _convert_us_date_time_to_pacific_display(
    rows: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """
    Amazon returns postedDate/date_time as UTC.

    For US fetch file/display, convert only date_time to
    America/Los_Angeles.

    Example:
      2026-05-01T07:15:00Z -> 2026-05-01 00:15:00 PDT
    """
    if not rows:
        return rows

    us_tz = ZoneInfo("America/Los_Angeles")

    for row in rows:
        raw_dt = row.get("date_time")
        if not raw_dt:
            continue

        try:
            dt = pd.to_datetime(raw_dt, utc=True, errors="coerce")

            if pd.isna(dt):
                continue

            row["date_time"] = dt.tz_convert(us_tz).strftime(
                "%Y-%m-%d %H:%M:%S %Z"
            )

        except Exception:
            # Keep original value if conversion fails.
            continue

    return rows


def _get_previous_month_year(year: int, month: int) -> tuple[int, int]:
    """
    Return the immediately previous month and year.

    Examples:
      June 2026    -> May 2026
      January 2026 -> December 2025
    """
    year = int(year)
    month = int(month)

    if month == 1:
        return year - 1, 12

    return year, month - 1


def _normalize_order_id(value: Any) -> str:
    """
    Normalize an order ID for safe comparison.

    Blank/invalid order IDs return an empty string. Those rows should not
    be removed because platform fees, ads, storage fees and other financial
    transactions may not have an order ID.
    """
    if value is None:
        return ""

    normalized = str(value).strip()

    if normalized.casefold() in {"", "none", "nan", "null", "0"}:
        return ""

    return normalized


def _normalize_transaction_type(row: Dict[str, Any]) -> str:
    """
    Return a normalized transaction type.

    _flatten_transaction_to_row normally stores the value in `type`, but
    transaction_type is kept as a fallback.
    """
    return str(
        row.get("type")
        or row.get("transaction_type")
        or ""
    ).strip().casefold()


def _remove_orders_existing_in_previous_month(
    rows: List[Dict[str, Any]],
    *,
    user_id: int,
    country: str,
    year: int,
    month: int,
    db_url: str | None = None,
) -> tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Remove only duplicate Order/Shipment rows whose order_id already exists
    in the immediately previous month's raw table.

    Important behavior:
      - Duplicate Order rows are removed.
      - Duplicate Shipment rows are removed.
      - Refund rows are always kept.
      - Reimbursement, adjustment, transfer, fee and other rows are kept.
      - Rows without an order_id are always kept.

    Example:
      Fetch June 2026:
        compare against user_{user_id}_{country}_may2026_data
    """

    empty_stats = {
        "previous_month_table": None,
        "previous_table_exists": False,
        "previous_unique_order_ids": 0,
        "current_unique_order_ids_before_filter": 0,
        "matching_order_ids": 0,
        "rows_removed": 0,
        "order_rows_removed": 0,
        "shipment_rows_removed": 0,
        "refund_rows_removed": 0,
        "other_rows_removed": 0,
    }

    if not rows:
        return rows, empty_stats

    database_url = db_url or os.getenv("DATABASE_URL")

    if not database_url:
        raise RuntimeError(
            "DATABASE_URL is required for previous-month order comparison"
        )

    normalized_country = (
        str(country or "")
        .strip()
        .lower()
        .replace("-", "_")
        .replace(" ", "_")
    )

    previous_year, previous_month = _get_previous_month_year(year, month)
    previous_month_name = calendar.month_name[previous_month].lower()

    previous_table = (
        f"user_{int(user_id)}_"
        f"{normalized_country}_"
        f"{previous_month_name}{int(previous_year)}_data"
    ).lower()

    engine = create_engine(database_url, pool_pre_ping=True)

    previous_order_ids: set[str] = set()
    previous_table_exists = False

    try:
        with engine.connect() as connection:
            previous_table_exists = bool(
                connection.execute(
                    text(
                        """
                        SELECT EXISTS (
                            SELECT 1
                            FROM information_schema.tables
                            WHERE table_schema = 'public'
                              AND table_name = :table_name
                        )
                        """
                    ),
                    {"table_name": previous_table},
                ).scalar()
            )

            if previous_table_exists:
                # We only need order IDs that previously had an Order or
                # Shipment row. A previous-month refund by itself should not
                # block a current-month Order or Shipment row.
                query = text(
                    f"""
                    SELECT DISTINCT BTRIM(order_id::text) AS order_id
                    FROM public."{previous_table}"
                    WHERE order_id IS NOT NULL
                      AND BTRIM(order_id::text) <> ''
                      AND LOWER(BTRIM(order_id::text))
                          NOT IN ('none', 'nan', 'null', '0')
                      AND LOWER(BTRIM(COALESCE(type::text, '')))
                          IN ('order', 'shipment')
                    """
                )

                db_order_ids = connection.execute(query).scalars().all()

                previous_order_ids = {
                    normalized
                    for value in db_order_ids
                    if (normalized := _normalize_order_id(value))
                }

    finally:
        engine.dispose()

    current_order_or_shipment_ids = {
        normalized_order_id
        for row in rows
        if _normalize_transaction_type(row) in {"order", "shipment"}
        if (
            normalized_order_id
            := _normalize_order_id(row.get("order_id"))
        )
    }

    matching_order_ids = (
        current_order_or_shipment_ids.intersection(previous_order_ids)
    )

    base_stats = {
        "previous_month_table": previous_table,
        "previous_table_exists": previous_table_exists,
        "previous_unique_order_ids": len(previous_order_ids),
        "current_unique_order_ids_before_filter": len(
            current_order_or_shipment_ids
        ),
        "matching_order_ids": len(matching_order_ids),
        "rows_removed": 0,
        "order_rows_removed": 0,
        "shipment_rows_removed": 0,
        "refund_rows_removed": 0,
        "other_rows_removed": 0,
    }

    if not matching_order_ids:
        return rows, base_stats

    filtered_rows: List[Dict[str, Any]] = []

    removed_order_rows = 0
    removed_shipment_rows = 0

    for row in rows:
        order_id = _normalize_order_id(row.get("order_id"))
        row_type = _normalize_transaction_type(row)

        # Rows without an order ID must remain.
        if not order_id:
            filtered_rows.append(row)
            continue

        # Refunds and every non-Order/non-Shipment transaction must remain.
        if row_type not in {"order", "shipment"}:
            filtered_rows.append(row)
            continue

        # Remove only duplicate Order or Shipment rows.
        if order_id in matching_order_ids:
            if row_type == "order":
                removed_order_rows += 1
            else:
                removed_shipment_rows += 1
            continue

        filtered_rows.append(row)

    stats = {
        **base_stats,
        "rows_removed": len(rows) - len(filtered_rows),
        "order_rows_removed": removed_order_rows,
        "shipment_rows_removed": removed_shipment_rows,
    }

    return filtered_rows, stats


def sync_monthly_transactions_for_user(
    *,
    user_id: int,
    year: int,
    month: int,
    country: str,
    marketplace_id: str | None = None,
    transaction_status: str | None = None,
    transaction_type_filter: str | None = None,
    store_in_db: bool = True,
    run_upload: bool = True,
    db_url: str | None = None,
    db_url_aux: str | None = None,
) -> Dict[str, Any]:
    """
    Fetch monthly transactions from Amazon SP-API and optionally run the
    upload pipeline.

    Processing order:
      1. Fetch every API page/status.
      2. Remove duplicate financial rows within the current API fetch.
      3. Compare with the immediately previous month's raw table.
      4. Remove only repeated Order/Shipment rows.
      5. Keep Refund and every other transaction type.
      6. Convert US display date to Pacific time.
      7. Run the upload pipeline when requested.
    """

    # Use the Amazon connection belonging to the selected marketplace.
    if marketplace_id:
        au = amazon_user.query.filter_by(
            user_id=user_id,
            marketplace_id=marketplace_id,
        ).first()
    else:
        au = amazon_user.query.filter_by(user_id=user_id).first()

    if not au or not au.refresh_token:
        return {
            "success": False,
            "error": "Amazon account not connected for this marketplace",
            "status": "no_refresh_token",
            "marketplace_id": marketplace_id,
        }

    amazon_client.refresh_token = au.refresh_token

    if marketplace_id:
        amazon_client.set_marketplace(marketplace_id)
    elif au.marketplace_id:
        amazon_client.set_marketplace(au.marketplace_id)
    elif au.region:
        amazon_client.set_region(au.region)

    is_us_marketplace = (
        (country or "").strip().lower()
        in {"us", "usa", "united_states"}
        or amazon_client.marketplace_id == "ATVPDKIKX0DER"
    )

    if is_us_marketplace:
        posted_after, posted_before = _month_date_range_us_pacific_utc(
            year,
            month,
        )
    else:
        posted_after, posted_before = _month_date_range_utc(year, month)

    all_rows: List[Dict[str, Any]] = []

    # Amazon accepts one transactionStatus per request.
    status_list: List[str] = []

    if (
        transaction_status
        and str(transaction_status).strip().lower() not in {"all", ""}
    ):
        status_list = [
            status.strip()
            for status in str(transaction_status).split(",")
            if status.strip()
        ]

    statuses_to_fetch: List[str | None] = status_list or [None]

    for status in statuses_to_fetch:
        params: Dict[str, Any] = {
            "postedAfter": posted_after,
            "postedBefore": posted_before,
            "marketplaceId": amazon_client.marketplace_id,
        }

        if status:
            params["transactionStatus"] = status

        while True:
            response = amazon_client.make_api_call(
                "/finances/2024-06-19/transactions",
                method="GET",
                params=params,
            )

            if not response or "error" in response:
                return {
                    "success": False,
                    "error": response
                    or {"error": "Unknown SP-API error"},
                    "failed_status": status,
                    "marketplace_id": amazon_client.marketplace_id,
                }

            payload = response.get("payload") or response
            transactions = payload.get("transactions") or []

            for transaction in transactions:
                transaction = transaction or {}

                transaction_status_value = transaction.get(
                    "transactionStatus"
                )
                transaction_type_value = transaction.get(
                    "transactionType"
                )

                if (
                    status
                    and transaction_status_value != status
                ):
                    continue

                if (
                    transaction_type_filter
                    and transaction_type_value
                    != transaction_type_filter
                ):
                    continue

                all_rows.append(
                    _flatten_transaction_to_row(transaction)
                )

            next_token = payload.get("nextToken")

            if not next_token:
                break

            # For next-token pagination Amazon expects nextToken only.
            params = {"nextToken": next_token}

    # ---------------------------------------------------------
    # Step 1: Remove duplicate financial rows inside this fetch
    # ---------------------------------------------------------
    before_dedupe_count = len(all_rows)

    all_rows = dedupe_rows_by_order_id(all_rows)

    after_dedupe_count = len(all_rows)
    dedupe_count_removed = (
        before_dedupe_count - after_dedupe_count
    )

    # ---------------------------------------------------------
    # Step 2: Previous-month filter
    # Remove only repeated Order/Shipment rows.
    # ---------------------------------------------------------
    before_previous_month_filter_count = len(all_rows)

    try:
        all_rows, previous_month_filter = (
            _remove_orders_existing_in_previous_month(
                all_rows,
                user_id=user_id,
                country=country,
                year=year,
                month=month,
                db_url=db_url,
            )
        )
    except Exception as exc:
        return {
            "success": False,
            "error": "Previous-month order comparison failed",
            "details": str(exc),
            "user_id": user_id,
            "country": country,
            "year": year,
            "month": month,
        }

    after_previous_month_filter_count = len(all_rows)
    previous_month_rows_removed = (
        before_previous_month_filter_count
        - after_previous_month_filter_count
    )

    # Convert US date display after filtering.
    if is_us_marketplace:
        all_rows = _convert_us_date_time_to_pacific_display(
            all_rows
        )

    pipeline_result: Dict[str, Any] | None = None

    if run_upload:
        if not store_in_db:
            pipeline_result = {
                "success": True,
                "skipped": True,
                "message": (
                    "run_upload_pipeline skipped because "
                    "store_in_db=false"
                ),
            }
        else:
            input_df = (
                pd.DataFrame(all_rows)
                if all_rows
                else pd.DataFrame()
            )

            pipeline_result = run_upload_pipeline_from_df(
                df_raw=input_df,
                user_id=user_id,
                country=country,
                month_num=str(month),
                year=str(year),
                db_url=db_url,
                db_url_aux=db_url_aux,
            )

            if (
                not pipeline_result
                or not pipeline_result.get("success")
            ):
                return {
                    "success": False,
                    "error": "Upload pipeline returned failure",
                    "pipeline_result": pipeline_result,
                }

    return {
        "success": True,
        "user_id": user_id,
        "country": country,
        "year": year,
        "month": month,
        "count": len(all_rows),

        "count_before_dedupe": before_dedupe_count,
        "count_after_dedupe": after_dedupe_count,
        "dedupe_count_removed": dedupe_count_removed,

        "count_before_previous_month_filter": (
            before_previous_month_filter_count
        ),
        "count_after_previous_month_filter": (
            after_previous_month_filter_count
        ),
        "previous_month_rows_removed": (
            previous_month_rows_removed
        ),
        "previous_month_filter": previous_month_filter,

        "transactions": all_rows,
        "store_in_db": store_in_db,
        "run_upload_pipeline": run_upload,
        "pipeline_result": pipeline_result,
        "marketplace_id": amazon_client.marketplace_id,
        "transaction_statuses_fetched": statuses_to_fetch,
    }
