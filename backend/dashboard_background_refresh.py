"""Durable manual UK/US dashboard refresh task.

Must be imported by the Celery worker through tasks.py. Uses existing HTTP APIs
and preserves the last published snapshot until every required step succeeds.
"""
import copy
import json
import logging
import os
from datetime import date, datetime, timezone
from urllib.parse import urlencode

import requests
from sqlalchemy import text


def register_background_dashboard_refresh(celery_app, flask_app):
    @celery_app.task(name="tasks.refresh_dashboard_background", bind=True, acks_late=True)
    def refresh_dashboard_background(self, refresh_id, user_id, params):
        from app.routes.amazon_api_routes import PHORMULA_ENGINE
        from tasks import _make_internal_user_token

        uid = int(user_id)
        country = params["country"]
        year = int(params["year"])
        month = int(params["month"])
        region = params["region"]
        key = params["cache_key"]
        base = (os.getenv("BACKEND_BASE_URL") or os.getenv("API_BASE_URL") or "").rstrip("/")
        headers = {"Authorization": f"Bearer {_make_internal_user_token(uid)}",
                   "Accept": "application/json", "Content-Type": "application/json"}
        rid = str(refresh_id)

        def update(step, state, error=None):
            with PHORMULA_ENGINE.begin() as conn:
                row = conn.execute(text("""
                    SELECT steps,status FROM public.dashboard_refresh_runs
                    WHERE refresh_id=CAST(:rid AS uuid) AND user_id=:uid FOR UPDATE
                """), {"rid": rid, "uid": uid}).mappings().first()
                if row is None or row["status"] != "running":
                    raise RuntimeError("Refresh run is no longer active")
                steps = dict(row["steps"])
                if step not in steps:
                    raise RuntimeError(f"Unregistered step: {step}")
                steps[step] = state
                conn.execute(text("""
                    UPDATE public.dashboard_refresh_runs SET steps=CAST(:steps AS jsonb),
                    status=:status,error=:error,updated_at=now(),
                    finished_at=CASE WHEN :status='failed' THEN now() ELSE finished_at END
                    WHERE refresh_id=CAST(:rid AS uuid) AND user_id=:uid
                """), {"steps": json.dumps(steps), "status": "failed" if state == "failed" else "running",
                        "error": (str(error)[:500] if error else None), "rid": rid, "uid": uid})

        def fail(message):
            with PHORMULA_ENGINE.begin() as conn:
                conn.execute(text("""
                    UPDATE public.dashboard_refresh_runs SET status='failed',
                    error=:error,finished_at=now(),updated_at=now()
                    WHERE refresh_id=CAST(:rid AS uuid) AND user_id=:uid AND status='running'
                """), {"error": str(message)[:500], "rid": rid, "uid": uid})

        def call(step, method, path, query=None, body=None, timeout=1800):
            update(step, "running")
            url = base + path
            if query:
                url += "?" + urlencode(query)
            response = requests.request(method, url, headers=headers, json=body, timeout=timeout)
            try:
                result = response.json()
            except ValueError as exc:
                raise RuntimeError(f"{step}: non-JSON response (HTTP {response.status_code})") from exc
            if not response.ok or (isinstance(result, dict) and (result.get("success") is False or result.get("error"))):
                detail = result.get("error") or result.get("message") if isinstance(result, dict) else ""
                raise RuntimeError(f"{step}: HTTP {response.status_code}: {detail}")
            update(step, "completed")
            return result

        with flask_app.app_context():
            try:
                if not base.startswith(("http://127.0.0.1:", "http://localhost:", "https://")):
                    raise RuntimeError("BACKEND_BASE_URL/API_BASE_URL must point to the Flask API")
                # Verify matching initial snapshot before making external API requests.
                with PHORMULA_ENGINE.connect() as conn:
                    cache = conn.execute(text("""
                        SELECT payload FROM public.live_dashboard_cache
                        WHERE user_id=:uid AND cache_key=:key
                    """), {"uid": uid, "key": key}).mappings().first()
                if not cache or not isinstance(cache["payload"], dict):
                    raise RuntimeError("No existing dashboard cache template; initial browser load required")
                snapshot = copy.deepcopy(cache["payload"])
                marketplace = "ATVPDKIKX0DER" if country == "us" else "A1F83G8C2ARO7P"
                today = date.today()
                if (today.year, today.month) != (year, month):
                    raise RuntimeError("Selected dashboard period no longer matches current month")
                start = int(params.get("start_day") or 1)
                end = int(params.get("end_day") or max(1, today.day - 1))
                if end < start or end > today.day:
                    raise RuntimeError("Invalid dashboard day range")
                country_code = country.upper()
                date_start = f"{year:04d}-{month:02d}-01"
                date_end = today.isoformat()
                mtd = call("mtd_transactions", "GET", "/amazon_api/finances/mtd_transactions",
                    {"country":country,"marketplace_id":marketplace,"store_in_db":"true","format":"json"}, timeout=900)
                update("inventory_current", "running")
                for endpoint, query in (
                    ("/amazon_api/inventory/aged", {"marketplace_id": marketplace}),
                    ("/amazon_api/inventory/awd", {"marketplace_id": marketplace, "store_in_db": "true"}),
                    ("/inventory_current", {"country_key": country, "month_name": today.strftime("%B").lower(), "year": year}),
                    ("/inventory_current_age_summary", {"country_key": country, "month_name": today.strftime("%B").lower(), "year": year}),
                ):
                    rr = requests.get(base + endpoint + "?" + urlencode(query), headers=headers, timeout=900)
                    if not rr.ok:
                        raise RuntimeError(f"Inventory sync {endpoint}: HTTP {rr.status_code}")
                    try:
                        value = rr.json()
                    except ValueError:
                        raise RuntimeError(f"Inventory sync {endpoint}: invalid JSON")
                    if isinstance(value, dict) and (value.get("success") is False or value.get("error")):
                        raise RuntimeError(f"Inventory sync {endpoint}: {value.get('error')}")
                update("inventory_current", "completed")
                # Stage 1: compute/publish useful sales and inventory figures BEFORE
                # the long-running Amazon Ads reports. The run stays RUNNING.
                bi = call("live_mtd_bi", "GET", "/live_mtd_bi", {
                    "countryName":country,"ranged":"MTD","month":today.strftime("%B").lower(),
                    "year":str(year),"generate_ai_insights":"false","manual_ai_refresh":"false",
                    "data_only_refresh":"true","start_day":start,"end_day":end}, timeout=900)
                if not isinstance(bi, dict):
                    raise RuntimeError("Invalid initial BI data")

                def make_snapshot(mtd_data, bi_data, phase):
                    current = copy.deepcopy(snapshot)
                    current["data"] = mtd_data
                    current["liveBiPayload"] = bi_data
                    current["biPeriods"] = bi_data.get("periods")
                    current["biDailySeries"] = bi_data.get("daily_series")
                    current["biAlignedTotals"] = bi_data.get("aligned_totals")
                    # Keep other existing cache fields. Ads-derived values may
                    # still be stale in the core-stage snapshot.
                    current["biStatus"] = "ready"
                    current["liveBiReady"] = True
                    current["complete"] = True
                    current["refreshRunId"] = rid
                    current["refreshPhase"] = phase
                    current["source"] = "celery_refresh_dashboard_background"
                    current["lastUpdatedAt"] = datetime.now(timezone.utc).isoformat()
                    current["savedAt"] = int(datetime.now(timezone.utc).timestamp() * 1000)
                    return current

                def publish(data, phase):
                    response = requests.post(base + "/amazon_api/live-dashboard/save", headers=headers,
                        json={"country": country, "platform": params["platform"], "region": region,
                              "month": month, "year": year, "startDay": params.get("start_day"),
                              "endDay": params.get("end_day"), "savedAt": data["savedAt"],
                              "cachePayload": data, "refreshId": rid,
                              "refreshPhase": phase}, timeout=900)
                    try:
                        result = response.json()
                    except ValueError:
                        raise RuntimeError(f"{phase} cache save returned HTTP {response.status_code}, non-JSON")
                    if not response.ok or result.get("success") is not True or result.get("stale_ignored"):
                        raise RuntimeError(f"{phase} cache save failed: HTTP {response.status_code}: {result.get('error')}")

                core_snapshot = make_snapshot(mtd, bi, "core")
                publish(core_snapshot, "core")
                snapshot["savedAt"] = core_snapshot["savedAt"]
                logging.info("Core dashboard published; Ads reports will now run user=%s refresh=%s", uid, rid)

                # Stage 2: slow Ads report generation / DB writes LAST.
                call("sp_report", "POST", "/api/ads/manager/sp_advertised_product_report",
                    body={"start_date":date_start,"end_date":date_end,"time_unit":"DAILY",
                          "countries":[country_code],"return_excel":False}, timeout=2000)
                call("sb_report", "POST", "/api/ads/manager/sb_keyword_report",
                    body={"start_date":date_start,"end_date":date_end,"time_unit":"DAILY",
                          "countries":[country_code],"return_excel":False}, timeout=2000)
                call("sd_report", "POST", "/api/ads/manager/sd_advertised_product_report/sync",
                    body={"start_date":date_start,"end_date":date_end,"time_unit":"DAILY",
                          "countries":[country_code],"max_wait_seconds":1800,"poll_every_seconds":10}, timeout=2000)
                ads_body = {"month":month,"year":year,"country":country_code,"include":["SP","SD","SB"]}
                monthly = call("monthly_ads_db", "POST", "/api/ads/monthly_sp_sd_to_db", body=ads_body, timeout=900)
                call("daily_ads_db", "POST", "/api/ads/daily_sp_sd_sb_to_db", body=ads_body, timeout=900)
                # MTD again AFTER Ads DB writes, matching manual frontend sequence.
                mtd_response = requests.get(base + "/amazon_api/finances/mtd_transactions?" + urlencode({
                    "country":country,"marketplace_id":marketplace,"store_in_db":"true","format":"json"}),
                    headers=headers, timeout=900)
                mtd_response.raise_for_status()
                mtd = mtd_response.json()
                if not isinstance(mtd,dict) or mtd.get("error") or mtd.get("success") is False:
                    raise RuntimeError("Post-Ads MTD response invalid")
                # Ads-related totals can change after the DB updates; refresh BI
                # again for the FINAL fully reconciled snapshot. This is a repeat
                # of a completed step, so do not send a second tracker transition.
                bi_response = requests.get(base + "/live_mtd_bi?" + urlencode({
                    "countryName":country,"ranged":"MTD","month":today.strftime("%B").lower(),
                    "year":str(year),"generate_ai_insights":"false","manual_ai_refresh":"false",
                    "data_only_refresh":"true","start_day":start,"end_day":end}),
                    headers=headers,timeout=900)
                bi_response.raise_for_status()
                bi = bi_response.json()
                if not isinstance(bi,dict) or bi.get("error") or bi.get("success") is False:
                    raise RuntimeError("Post-Ads BI response invalid")
                final_snapshot = make_snapshot(mtd, bi, "final")
                # Preserve strictly increasing timestamp even on extremely fast
                # API responses (the DB UPSERT rejects non-increasing saved_at).
                final_snapshot["savedAt"] = max(final_snapshot["savedAt"],
                    int(snapshot.get("savedAt") or 0) + 1)
                publish(final_snapshot, "final")
                return {"success":True,"refresh_id":rid}
            except Exception as exc:
                logging.exception("Celery background refresh failed user=%s run=%s",uid,rid)
                fail(exc)
                return {"success":False,"refresh_id":rid,"error":str(exc)[:500]}
    return refresh_dashboard_background
