from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List

import pandas as pd

from app import db
from app.models.user_models import amazon_user
from app.utils.amazon_utils import (
    _month_date_range_utc,
    _flatten_transaction_to_row,
    run_upload_pipeline_from_df,
)
from app.utils.amazon_utils import amazon_client


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
) -> dict:
    """
    Fetch monthly transactions from Amazon SP-API and optionally run upload pipeline.
    Designed for both route and Celery usage.
    """

    au = amazon_user.query.filter_by(user_id=user_id).first()
    if not au or not au.refresh_token:
        return {
            "success": False,
            "error": "Amazon account not connected",
            "status": "no_refresh_token",
        }

    # set client from saved connection
    amazon_client.refresh_token = au.refresh_token
    if au.region:
        amazon_client.region = au.region
    if marketplace_id:
        amazon_client.marketplace_id = marketplace_id
    elif au.marketplace_id:
        amazon_client.marketplace_id = au.marketplace_id

    posted_after, posted_before = _month_date_range_utc(year, month)

    params: Dict[str, Any] = {
        "postedAfter": posted_after,
        "postedBefore": posted_before,
        "marketplaceId": amazon_client.marketplace_id,
    }
    if transaction_status and str(transaction_status).lower() not in ("all", ""):
        params["transactionStatus"] = transaction_status

    all_rows: List[Dict[str, Any]] = []

    while True:
        res = amazon_client.make_api_call(
            "/finances/2024-06-19/transactions",
            method="GET",
            params=params,
        )

        if not res or "error" in res:
            return {
                "success": False,
                "error": res or {"error": "Unknown SP-API error"},
            }

        payload_res = res.get("payload") or res
        transactions = payload_res.get("transactions") or []

        for tx in transactions:
            tstatus = (tx or {}).get("transactionStatus")
            ttype = (tx or {}).get("transactionType")

            if transaction_status and str(transaction_status).lower() not in ("all", ""):
                if tstatus != transaction_status:
                    continue
            if transaction_type_filter and ttype != transaction_type_filter:
                continue

            all_rows.append(_flatten_transaction_to_row(tx or {}))

        next_token = payload_res.get("nextToken")
        if not next_token:
            break

        params = {"nextToken": next_token}

    pipeline_result = None

    if run_upload:
        if not store_in_db:
            pipeline_result = {
                "success": True,
                "skipped": True,
                "message": "run_upload_pipeline skipped because store_in_db=false",
            }
        else:
            df_in = pd.DataFrame(all_rows) if all_rows else pd.DataFrame()
            pipeline_result = run_upload_pipeline_from_df(
                df_raw=df_in,
                user_id=user_id,
                country=country,
                month_num=str(month),
                year=str(year),
                db_url=db_url,
                db_url_aux=db_url_aux,
            )

            if not pipeline_result or not pipeline_result.get("success"):
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
        "store_in_db": store_in_db,
        "run_upload_pipeline": run_upload,
        "pipeline_result": pipeline_result,
    }
