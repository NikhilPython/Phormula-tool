
@chatbot_bp.route("/chatbot", methods=["POST", "OPTIONS"])
def chatbot():
    if request.method == "OPTIONS":
        return ("", 200)

    def _stash_context(user_id, plan, country_override, table_records=None, user_msg: str = None):
        """Persist essentials from the last analytics run for follow-ups (plus last table rows)."""
        try:
            import time
            store = globals().setdefault("LAST_CONTEXT", {})

            # extract SKU filter if any
            sku = None
            for f in (plan.get("filters") or []):
                if str(f.get("field", "")).lower() == "sku":
                    sku = f.get("value")
                    break

            # ⛔️ DO NOT coerce mix metrics to 'sales'. Keep exactly what was used.
            metric = (plan.get("metric") or "").strip().lower()

            # pull recent SKUs/products from the shown table (if any)
            last_skus, last_products = [], []
            if isinstance(table_records, list):
                for r in table_records:
                    lvl = (r.get("level") or "").lower()
                    key = (r.get("key") or "").strip()
                    if lvl == "sku" and key:
                        last_skus.append(key)
                    elif lvl == "product" and key:
                        last_products.append(key)

            # ensure plan-level product/SKU are also captured
            if plan.get("product") and plan["product"] not in last_products:
                last_products.insert(0, plan["product"])
            if sku:
                if isinstance(sku, list):
                    for s in sku:
                        if s and s not in last_skus:
                            last_skus.append(s)
                elif sku not in last_skus:
                    last_skus.insert(0, sku)

            store[int(user_id)] = {
                "metric": metric,                                   # ← now preserves sales_mix/profit_mix/etc.
                "product": plan.get("product"),
                "sku": sku,
                "country": country_override or plan.get("country"),
                "group_by": plan.get("group_by"),
                "time_range": plan.get("time_range"),
                "last_skus": last_skus[:50],
                "last_products": last_products[:50],
                "last_user_msg": user_msg,
                "ts": time.time(),                                  # timestamp for TTL
            }
            print(f"[DEBUG][followup] stashed context → {store[int(user_id)]}")
        except Exception as _e:
            print("[DEBUG][followup] failed to stash context:", _e)




    def ok(payload: dict):
        if "success" not in payload:
            payload = {"success": True, **payload}
        safe_payload = _json_sanitize(payload)
        print("[DEBUG][BE][OK Response]:", safe_payload)
        return Response(json.dumps(safe_payload, allow_nan=False), status=200, mimetype="application/json")

    def bad_request(msg: str):
        payload = {"success": False, "message": msg}
        print("[DEBUG][BE][Bad Request]:", payload)
        return Response(json.dumps(payload, allow_nan=False), status=400, mimetype="application/json")

    def _recover_empty_result(df, mode, plan, query_text):
        """Try broader interpretations before giving up; returns (df, mode, plan) possibly updated."""
        Q = query_text
        print("[DEBUG][recover] Empty result → starting recovery attempts")

        def _exec_retry(p: dict, tag: str):
            try:
                _df, _mode = engine_q.exec_plan_via_formula(
                    plan=p, query=Q, user_id=str(user_id), country_override=country_override
                )
                n = 0 if _df is None else len(_df)
                print(f"[DEBUG][recover] {tag}: rows={n} mode={_mode}")
                return _df, _mode
            except Exception as e:
                print(f"[DEBUG][recover] {tag} failed:", e)
                return None, None

        # 0) Drop literal 'all products' equals filters and switch to per-product breakdown
        try:
            cleaned = []
            dropped = False
            for f in (plan.get("filters") or []):
                fld = str(f.get("field","")).lower()
                val = str(f.get("value","")).strip().lower()
                if fld in {"sku","product","product_name"} and val in {
                    "all products","all product","all skus","all sku","everything","all variants","any product"
                }:
                    dropped = True
                    continue
                cleaned.append(f)
            if dropped:
                plan["filters"] = cleaned
                plan["product"] = None
                if not plan.get("group_by"):
                    plan["group_by"] = "product"
                print("[DEBUG][recover] dropped literal 'all products' filter; group_by=product")
        except Exception:
            pass

        # 1) Product resolver (entity resolver → try options)
        if plan.get("product"):
            eff_country = (country_override or plan.get("country") or "UK").upper()
            if eff_country not in ("UK", "US"):
                eff_country = "UK"
            try:
                cands = resolve_product_entities(Q, engine, int(user_id), eff_country, top_k=5)
            except Exception:
                cands = []
            for c in (cands or []):
                pname = (c.get("product_name") or "").strip()
                if not pname:
                    continue
                p2 = dict(plan); p2["product"] = pname
                df2, mode2 = _exec_retry(p2, f"product_resolve:{pname}")
                if df2 is not None and not df2.empty:
                    return df2, mode2, p2

        # 2) Drop exact/in SKU filter if present (over-narrow)
        if plan.get("filters"):
            has_sku = any(
                (str(f.get("field","")).lower() == "sku" and str(f.get("op","")).lower() in {"=","eq","in"})
                for f in plan["filters"]
            )
            if has_sku:
                p2 = dict(plan)
                p2["filters"] = [f for f in plan["filters"] if str(f.get("field","")).lower() != "sku"]
                df2, mode2 = _exec_retry(p2, "drop_sku_filter")
                if df2 is not None and not df2.empty:
                    return df2, mode2, p2

        # 3) Country swap UK <-> US
        eff_ctry = (plan.get("country") or country_override or "").upper()
        if eff_ctry in {"UK","US"}:
            swap = "US" if eff_ctry == "UK" else "UK"
            p2 = dict(plan); p2["country"] = swap
            df2, mode2 = _exec_retry(p2, f"country_swap:{swap}")
            if df2 is not None and not df2.empty:
                return df2, mode2, p2

        # 4) Expand tight time window (<=3 days) to full month; else clamp to available
        def _expand_time_if_tight(p: dict):
            tr = p.get("time_range") or {}
            start = (tr or {}).get("start"); end = (tr or {}).get("end")
            if not (start and end):
                return None
            try:
                sd = dt.date.fromisoformat(start)
                ed = dt.date.fromisoformat(end)
                if (ed - sd).days <= 3:
                    y, m = sd.year, sd.month
                    last = calendar.monthrange(y, m)[1]
                    return {"start": f"{y:04d}-{m:02d}-01", "end": f"{y:04d}-{m:02d}-{last:02d}"}
            except Exception:
                pass
            return None

        p2 = dict(plan)
        expanded = _expand_time_if_tight(p2)
        if expanded:
            p2["time_range"] = expanded
        else:
            clamp = clamp_relative_time_to_available(user_id, country_override, Q)
            if clamp:
                p2["time_range"] = {"start": clamp["start"], "end": clamp["end"]}

        if p2.get("time_range"):
            df2, mode2 = _exec_retry(p2, "expand_time")
            if df2 is not None and not df2.empty:
                return df2, mode2, p2

        # No luck
        return df, mode, plan

    # ---- Auth ---------------------------------------------------------------
    user_id, err = _decode_jwt_or_401(request.headers.get("Authorization"))
    if err:
        return Response(
            json.dumps({"success": False, "message": err}, allow_nan=False),
            status=401,
            mimetype="application/json"
        )

    # ---- Body ---------------------------------------------------------------
    if not request.is_json:
        return Response(
            json.dumps({"success": False, "message": "Request must be in JSON format"}, allow_nan=False),
            status=400,
            mimetype="application/json"
        )

    body = request.get_json(silent=True) or {}
    action = (body.get("action") or "chat").lower()

    # ---- Chat branch only accepts a non-empty query -------------------------
    query = (body.get("query") or body.get("message") or "").strip()
    country_override = body.get("country")
    if action == "chat" and not query:
        return bad_request('Missing "query" for chat action')

    # ------------------------------------------------------------------------
    # INIT branch
    # ------------------------------------------------------------------------
    if action == "init":
        health = {
            "status": "healthy",
            "components": {
                "database": engine is not None,
                "vector_model": st_model is not None,
            },
        }

        data_info: dict = {}
        for country in ["uk", "us", "global"]:
            table = (
                f"user_{user_id}_total_country_global_data"
                if country == "global"
                else f"user_{user_id}_{country}_merge_data_of_all_months"
            )
            try:
                with engine.connect() as conn:
                    count = conn.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar()
                    date_sql = f"""
                        SELECT
                          MIN(CASE WHEN year ~ '^[0-9]+$' THEN CAST(year AS INT) END) AS min_year,
                          MAX(CASE WHEN year ~ '^[0-9]+$' THEN CAST(year AS INT) END) AS max_year
                        FROM {table}
                    """
                    row = conn.execute(text(date_sql)).fetchone()
                data_info[country] = {
                    "table_name": table,
                    "record_count": int(count or 0),
                    "date_range": {
                        "min_year": row[0] if row else None,
                        "max_year": row[1] if row else None,
                    },
                }
            except Exception as e:
                data_info[country] = {
                    "table_name": table,
                    "error": f"Table not accessible: {str(e)}",
                }

        return ok({"data": {"health": health, "available_data": data_info}})

    # ------------------------------------------------------------------------
    # CHAT branch
    # ------------------------------------------------------------------------

    # Keep original user wording and normalized version
    orig_q = query
    query_norm = normalize_user_query(query)

    
    # ------------ STEP 1: Handle *pending clarifications* FIRST --------------
    pending_snapshot = PENDING.get(user_id)
    if pending_snapshot:
        orig_q = pending_snapshot.get("original_query") or query
        applied = apply_reply_to_pending(user_id, query, engine)
        if applied:
            if applied.get("need_more"):
                next_slot = applied["missing"][0]
                prompt = make_ask_prompt(next_slot)
                PENDING.set(
                    user_id,
                    applied["plan"],
                    applied["missing"],
                    reprompt=prompt,
                    original_query=orig_q,
                )
                msg_id = save_chat_to_db(user_id, query, prompt) or None
                return ok({"mode": "clarify", "response": prompt, "message_id": msg_id})

            plan = applied["plan"]

            # Normalize "all products"
            try:
                user_reply = (query or "").strip().lower()
                if any(kw in user_reply for kw in [
                    "all products","all product","all skus","all sku","everything","all variants","any product"
                ]):
                    plan["product"] = None
                    plan["filters"] = [f for f in (plan.get("filters") or [])
                                       if str(f.get("field","")).lower() not in {"sku","product","product_name"}]
                    if not plan.get("group_by"):
                        plan["group_by"] = "product"
                    print("[DEBUG] pending: 'all products' → cleared product/SKU filters; group_by=product")
            except Exception as _e:
                print("[DEBUG][WARN] pending 'all products' normalization failed:", _e)

            # SKU/product wording normalization
            plan = _normalize_plan_for_sku_language(plan, orig_q)

            # Advisor short-circuit (pending)
            if isinstance(plan, dict) and (plan.get("operation") or "").lower() == "advisor":
                advisor = BusinessAdvisor(engine, user_id, country_override)

                product_phrase = (plan.get("product") or "").strip()
                table_name = engine_q.builder.table_for(str(user_id), plan.get("country") or country_override)

                if product_phrase:
                    advice_text = advisor.answer_for_product(product_phrase, table_name)
                else:
                    advice_text = advisor.answer(orig_q)

                msg_id = save_chat_to_db(user_id, orig_q, advice_text) or None
                return ok({"mode": "advisor", "response": advice_text, "message_id": msg_id})


            # Execute
            try:
                df, mode = engine_q.exec_plan_via_formula(
                    plan=plan, query=orig_q, user_id=str(user_id), country_override=country_override
                )
            except Exception as e:
                import traceback; traceback.print_exc()
                return Response(
                    json.dumps({
                        "success": False,
                        "message": "Unexpected error after clarification.",
                        "error": str(e)
                    }, allow_nan=False),
                    status=500,
                    mimetype="application/json"
                )

            # Recovery on empty
            if df is None or df.empty:
                df, mode, plan = _recover_empty_result(df, mode, plan, locals().get("orig_q", query))
                if df is None or df.empty:
                    # --- ADVISOR FALLBACK: try last 90 days if user asked for advice/plan ---
                    try:
                        if wants_advice(orig_q, plan) or ("plan" in (plan.get("operation") or "").lower()):
                            eff_country = country_override or plan.get("country") or "UK"
                            tr_recent = clamp_relative_time_to_available(user_id, eff_country, "last 90 days")

                            if tr_recent and tr_recent.get("start") and tr_recent.get("end"):
                                df_recent, mode_recent = engine_q.exec_plan_via_formula(
                                    plan={
                                        "operation": "trend",
                                        "metric": plan.get("metric") or "sales",
                                        "time_range": tr_recent,
                                        "country": eff_country,
                                        "group_by": plan.get("group_by") or "product",
                                        "filters": [],
                                    },
                                    query=orig_q,
                                    user_id=str(user_id),
                                    country_override=eff_country,
                                )

                                if df_recent is not None and not df_recent.empty:
                                    scope = plan.get("group_by") or "portfolio"
                                    advice_lines = BusinessAdvisor.recommend(
                                        orig_q,
                                        df_recent,
                                        aux={
                                            "country": eff_country,
                                            "time_range": tr_recent,
                                            "scope": scope,
                                            "target": plan.get("product"),
                                        },
                                    )
                                    reply = "\n".join(advice_lines) if advice_lines else \
                                        "I couldn’t derive targeted growth actions from recent data."
                                    msg_id = save_chat_to_db(user_id, query, reply) or None
                                    return ok({"mode": "advisor", "response": reply, "message_id": msg_id})
                    except Exception as _e:
                        print("[DEBUG][advisor fallback (pending)] failed:", _e)

                # --- Default response if still empty ---
                reply = "No data found for your query."
                msg_id = save_chat_to_db(user_id, query, reply) or None
                return ok({"response": reply, "message_id": msg_id, "mode": mode})

            # SKU clarification (pending)
            if mode == "sql_special" and "_" in df.columns and len(df) == 1:
                reply = str(df.iloc[0]["_"])
                if re.search(r"(multiple\s+skus|one\s+specific\s+variant|all\s+variants)", reply, re.I):
                    PENDING.set(
                        user_id,
                        plan,
                        missing=["sku_choice"],
                        reprompt=reply,
                        original_query=orig_q,
                        country_override=country_override,
                    )
                    msg_id = save_chat_to_db(user_id, query, reply) or None
                    return ok({"mode": "clarify", "response": reply, "message_id": msg_id})
                msg_id = save_chat_to_db(user_id, query, reply) or None
                return ok({"response": reply, "message_id": msg_id, "mode": mode})

            # Render formula-mode
            # --- Render formula-mode (PENDING branch) ---
            if mode == "sql_formula":
                table_records = df_to_records_safe(df)
                final_records = _finalize_records(plan, table_records)
                print(f"[DEBUG] wants_advice(pending)={wants_advice(orig_q, plan)}")
                # Decide: prescriptive advice vs descriptive report (no hard-coded keywords)
                if wants_advice(orig_q, plan):
                    advice_lines = BusinessAdvisor.recommend(
                        orig_q,
                        df,
                        aux={
                            "country": country_override or plan.get("country"),
                            "time_range": plan.get("time_range"),
                            "scope": (
                                "sku"
                                if plan.get("force_product_only")
                                and isinstance(plan.get("product"), str)
                                and plan["product"].upper() == plan["product"]
                                else "product"
                                if plan.get("product")
                                else "portfolio"
                            ),
                            "target": plan.get("product"),
                        },
                    )
                    reply = "\n".join(advice_lines) if advice_lines else \
                            "I couldn’t derive targeted growth actions from the latest data."
                    used_mode = "advisor"
                else:
                    reply = generate_openai_answer(
                        user_query=orig_q,
                        mode="sql_formula",
                        analysis=None,
                        table_records=final_records,
                    )
                    used_mode = mode

                try:
                    _stash_context(user_id, plan, country_override, table_records=final_records,user_msg=orig_q,)
                    _local = globals().setdefault("LAST_CONTEXT", {})
                    FOLLOWUP_MEMORY.push(_local.get(int(user_id), {}))

                except Exception:
                    pass

                msg_id = save_chat_to_db(user_id, query, reply) or None
                return ok({
                    "mode": used_mode,
                    "response": reply,
                    "message_id": msg_id,
                    "table": final_records
                })


            # Default: analysis summary
            analysis = analyst.analyze_results(df, orig_q)
            reply = generate_openai_answer(
                user_query=orig_q,
                mode=mode if mode else "sql",
                analysis=analysis,
                table_records=None,
            )
            try:
                _stash_context(
                    user_id,
                    plan,
                    country_override,
                    table_records=None,
                    user_msg=orig_q,
                )
                _local = globals().setdefault("LAST_CONTEXT", {})
                FOLLOWUP_MEMORY.push(_local.get(int(user_id), {}))

            except Exception:
                pass

            msg_id = save_chat_to_db(user_id, query, reply) or None
            return ok({"response": reply, "message_id": msg_id, "mode": mode})

    # ------------ STEP 2: Small-talk / Capability fast-path ------------------
    if is_smalltalk(query):
        reply = "Hey! 👋 I can help you analyze Amazon sales, fees, taxes, profit, and trends. What would you like to explore?"
        msg_id = save_chat_to_db(user_id, query, reply) or None
        return ok({"mode": "smalltalk", "response": reply, "message_id": msg_id})

    if is_capability(query):
        reply = (
            "I’m your finance/RAG copilot for Amazon data. I can:\n"
            "• Summaries: “Overall profit last 30 days (UK).”\n"
            "• Rankings: “Top 5 SKUs by sales in July 2025.”\n"
            "• Fees: “FBA fees for Product X last week.”\n"
            "• Taxes & rebates: “Marketplace facilitator tax in Aug 2025 (US).”\n"
            "• Trends: “MoM sales growth for ASIN B07… in 2025.”\n"
            "• Targets: “Days crossing £1,000 daily sales in June.”\n\n"
            "Tell me the metric + time range + (optional) country/product."
        )
        msg_id = save_chat_to_db(user_id, query, reply) or None
        return ok({"mode": "capabilities", "response": reply, "message_id": msg_id})

    # ------------ STEP 3: trivial-input guard --------------------------------
    if len(query.split()) < 3:
        hint = "Tell me what to analyze (metric + time + optional country/product)."
        msg_id = save_chat_to_db(user_id, query, hint) or None
        return ok({"mode": "hint", "response": hint, "message_id": msg_id})

    # ------------ STEP 3.25: "New product performance" fast-path -------------
    try:
        ql = query.lower()
        looks_new = any(p in ql for p in [" new product", " new sku", " launched", "launch", " debut"])

        fp = FilterParser()
        t = fp.parse_time(query)
        month_num = int(t["months"][0]["number"]) if t.get("months") else None
        year_num = int(t["years"][0]) if t.get("years") else None

        if month_num is None or year_num is None:
            clamped_try = clamp_relative_time_to_available(user_id, country_override, query)
            if clamped_try and "start" in clamped_try:
                try:
                    y_m = clamped_try["start"].split("-")
                    if len(y_m) >= 2:
                        year_num = year_num or int(y_m[0])
                        month_num = month_num or int(y_m[1])
                except Exception:
                    pass

        eff_country = (country_override or parse_country_strict(query) or "UK").upper()
        if eff_country not in ("UK", "US"):
            eff_country = "UK"

        if looks_new and year_num and month_num:
            first_map = first_seen_by_sku(engine, user_id, eff_country)  # {sku_lower: 'YYYY-MM'}
            target_ym = f"{year_num:04d}-{month_num:02d}"

            table = f"user_{user_id}_{eff_country.lower()}_merge_data_of_all_months"
            month_case = """
                CASE
                  WHEN month ~ '^\\d+$' THEN CAST(month AS INT)
                  WHEN LOWER(month) LIKE 'jan%%' THEN 1
                  WHEN LOWER(month) LIKE 'feb%%' THEN 2
                  WHEN LOWER(month) LIKE 'mar%%' THEN 3
                  WHEN LOWER(month) LIKE 'apr%%' THEN 4
                  WHEN LOWER(month) LIKE 'may%%' THEN 5
                  WHEN LOWER(month) LIKE 'jun%%' THEN 6
                  WHEN LOWER(month) LIKE 'jul%%' THEN 7
                  WHEN LOWER(month) LIKE 'aug%%' THEN 8
                  WHEN LOWER(month) LIKE 'sep%%' THEN 9
                  WHEN LOWER(month) LIKE 'oct%%' THEN 10
                  WHEN LOWER(month) LIKE 'nov%%' THEN 11
                  WHEN LOWER(month) LIKE 'dec%%' THEN 12
                  ELSE NULL
                END
            """
            with engine.connect() as conn:
                sql_curr = text(f"""
                    SELECT DISTINCT
                        sku AS sku_original,
                        LOWER(TRIM(sku)) AS sku_lower
                    FROM {table}
                    WHERE sku IS NOT NULL AND TRIM(sku) <> '' AND TRIM(LOWER(sku)) NOT IN ('0','none','null','nan')
                      AND (year ~ '^[0-9]+$' AND CAST(year AS INT) = :y)
                      AND ({month_case}) = :m
                """)
                rows = conn.execute(sql_curr, {"y": year_num, "m": month_num}).fetchall()

            new_skus: list[str] = [r[0] for r in rows if first_map.get((r[1] or "").strip()) == target_ym]

            print(f"[DEBUG][new-products] month={year_num}-{month_num:02d} country={eff_country} "
                  f"candidates={len(rows)} new={len(new_skus)}")

            if new_skus:
                last_day = calendar.monthrange(year_num, month_num)[1]
                start_ymd = f"{year_num:04d}-{month_num:02d}-01"
                end_ymd   = f"{year_num:04d}-{month_num:02d}-{last_day:02d}"

                ov = overview_metrics_for_period(engine, user_id, eff_country, start_ymd, end_ymd, skus=new_skus)
                summary = (
                    f"**New-product overview — {calendar.month_name[month_num]} {year_num} ({eff_country})**\n"
                    f"- Sales: £{ov['sales']:,.2f}\n"
                    f"- Profit: £{ov['profit']:,.2f}\n"
                    f"- Qty Sold: {ov['qty']:,.0f}\n"
                    f"- ASP: £{ov['asp']:,.2f}\n"
                )

                plan_np = {
                    "operation": "aggregate",
                    "metric": "sales",
                    "time_range": {"start": start_ymd, "end": end_ymd},
                    "country": eff_country,
                    "group_by": None,
                    "sort_dir": "desc",
                    "filters": [{"field": "sku", "op": "in", "value": new_skus}],
                    "needs_clarification": False,
                    "clarification_message": None,
                }
                try:
                    df, mode = engine_q.exec_plan_via_formula(
                        plan=plan_np, query=query, user_id=str(user_id), country_override=eff_country
                    )
                    table_records = df_to_records_safe(df) if (df is not None and not df.empty) else []

                    llm = generate_openai_answer(
                        user_query=f"{query} (new SKUs in {calendar.month_name[month_num]} {year_num})",
                        mode="sql_formula",
                        analysis=None,
                        table_records=table_records,
                    ) if table_records else ""

                    reply = summary + ("\n" + llm if llm else "\n_No line-item table available for the selected period._")

                    try:
                        _stash_context(
                            user_id,
                            {
                                "metric": "sales", "product": None, "filters": [],
                                "group_by": None, "time_range": {"start": start_ymd, "end": end_ymd},
                                "country": eff_country
                            },
                            country_override,
                            table_records=table_records,
                            user_msg=orig_q
                        )
                        _local = globals().setdefault("LAST_CONTEXT", {})
                        FOLLOWUP_MEMORY.push(_local.get(int(user_id), {}))
                    except Exception:
                        pass

                    msg_id = save_chat_to_db(user_id, query, reply) or None
                    return ok({
                        "mode": "new_product_overview",
                        "response": reply,
                        "message_id": msg_id,
                        "table": table_records
                    })
                except Exception as e:
                    print("[DEBUG][new-products] overview fast-path failed, falling back:", e)
    except Exception as e:
        print("[DEBUG][new-products] detection failed:", e)

    # ------------ STEP 3.44: Follow-up vs New Query decision ----------------
    # ADDED: Safe defaults so later blocks never crash if decide_* throws.
    decision = {"mode": "new", "synth": None}

    # Use short-term memory (merged context from last few turns)
    store = globals().setdefault("LAST_CONTEXT", {})
    lc = FOLLOWUP_MEMORY.get_recent() or (store.get(int(user_id)) or {})
    last_user_msg = lc.get("last_user_msg")

    try:
        # Re-fetch inside try (harmless if already set above)
        store = globals().setdefault("LAST_CONTEXT", {})
        lc = FOLLOWUP_MEMORY.get_recent() or (store.get(int(user_id)) or {})
        last_user_msg = lc.get("last_user_msg")

        # Run updated decision logic
        decision = decide_followup_or_new(query, lc, last_user_msg, now_ts=time.time())
        print(f"[DEBUG][followup-decision] {decision}")

        if decision.get("mode") == "followup":
            # ⛔️ Do NOT overwrite the user's text.
            # Keep synth around only to *prime* the planner later.
            if decision.get("synth"):
                print(f"[DEBUG][followup] (keeping user text) synth query → {decision['synth']}")
            else:
                print("[DEBUG][followup] no synthesized query; will backfill from LAST_CONTEXT")
    except Exception as _e:
        print("[DEBUG][followup-decision] failed:", _e)


    # ------------ STEP 3.45: Follow-up synthesis (context-driven, no keyword heuristics) ----------
    if decision.get("mode") == "followup" and not decision.get("synth"):
        # Nothing was synthesized in decide_followup_or_new.
        # We'll let the planner pick the operation, but backfill context below.
        print("[DEBUG][followup] no synthesized query; will backfill from LAST_CONTEXT")

    # Build a planning prompt that preserves the user’s wording,
    # and only APPENDS hints (clamped time, synthesized follow-up, and strict context).
    fp = FilterParser()
    _t = fp.parse_time(query)
    _user_mentioned_time = bool(
        _t.get("explicit_day") or _t.get("months") or _t.get("years") or _t.get("date_range")
    )

    if _user_mentioned_time:
        clamped = clamp_relative_time_to_available(user_id, country_override, query)
    else:
        clamped = None

    query_for_plan = f"{query} (period: {clamped['start']} to {clamped['end']})" if clamped else query

    if decision.get("mode") == "followup":
        # Optional synthesized query: append, do not replace
        if decision.get("synth"):
            query_for_plan = f"{query_for_plan} ({decision['synth']})"
        # Append strict context suffix (metric/product/country/time) if available
        try:
            _ctx = _planner_context_suffix(lc)
            # 🚫 don’t leak product unless user is anaphoric
            if _ctx and not _is_anaphoric_to_product(query):
                _ctx = re.sub(r"(;?\s*product=[^)\s]+)", "", _ctx)
        except Exception:
            _ctx = None

        if _ctx:
            query_for_plan = f"{query_for_plan} {_ctx}"
            print("[DEBUG][planner] primed with", _ctx)


    # Normalize the primed query (this becomes our canonical query_norm)
    query_for_plan_norm = normalize_user_query(query_for_plan)
    query_norm = query_for_plan_norm

    # Fresh plan, then slot-by-slot merge with context
    fresh_plan = plan_query(query_norm)

    def _has(v):
        return v is not None and v != "" and v != {}

    plan = dict(fresh_plan)

    prefilled = advisor_preplan(query, user_id=user_id, country_override=country_override)
    plan = prefilled or plan_query(query_norm)
    print("[DEBUG] Plan (raw):", plan)

    plan = _normalize_plan_for_sku_language(plan, query)
    print("[DEBUG] Plan (normalized):", plan)
    lc_for_defaults = (globals().setdefault("LAST_CONTEXT", {}).get(int(user_id)) or {})
    plan, _filled = _auto_fill_defaults(plan, query, user_id, country_override, lc_for_defaults)


    # ---- Sanitize SKU constraint using shape + catalog existence (no keywords) ----
    try:
        country_eff = (country_override or plan.get("country") or parse_country_strict(query) or "UK").upper()
        if country_eff not in {"UK","US"}:
            country_eff = "UK"

        cleaned_filters = []
        dropped = False
        for f in (plan.get("filters") or []):
            if str(f.get("field","")).lower() == "sku":
                val = str(f.get("value","")).strip()
                if not is_valid_sku_token(val, engine, int(user_id), country_eff):
                    dropped = True
                    continue
            cleaned_filters.append(f)
        if dropped:
            plan["filters"] = cleaned_filters
            # If group_by was forced to SKU as a consequence, relax it
            if (plan.get("group_by") or "").lower() == "sku":
                plan["group_by"] = "product" if (plan.get("operation") in {"rank","breakdown"}) else None
            print("[DEBUG] SKU sanitize: removed invalid SKU token(s); relaxed group_by.")
    except Exception as _e:
        print("[DEBUG] SKU sanitize failed:", _e)

    if decision.get("mode") == "followup":
        # Context backfill for missing slots
        if not _has(plan.get("metric")):
            plan["metric"] = lc.get("metric")
        if not _has(plan.get("country")):
            plan["country"] = lc.get("country")
        
        
        if not _has(plan.get("time_range")):
            if _looks_anaphoric_to_time(query):
                plan["time_range"] = lc.get("time_range")

        # 👇 Reuse product only if the text actually refers back; never force here
        if not _has(plan.get("product")):
            if _is_anaphoric_to_product(query):
                plan["product"] = lc.get("product")
            else:
                plan.pop("product", None)
        plan["force_product_only"] = False

        # Safe fill for time if still missing
        try:
            fp_tmp = FilterParser()
            t_tmp = fp_tmp.parse_time(query)
            user_mentioned_time = bool(
                t_tmp.get("explicit_day") or t_tmp.get("months") or
                t_tmp.get("years") or t_tmp.get("date_range")
            )
            if not _has(plan.get("time_range")) and not user_mentioned_time:
                eff_country = plan.get("country") or lc.get("country") or country_override
                tr = clamp_relative_time_to_available(user_id, eff_country, "last 3 months")
                if tr:
                    plan["time_range"] = {"start": tr["start"], "end": tr["end"]}
                    print(f"[DEBUG][followup] clamp filled time_range → {plan['time_range']}")
        except Exception as e:
            print("[DEBUG][followup] clamp fill failed:", e)

    # ------------ STEP 3.5: Automatic intent routing -------------------------
    intent = None
    try:
        # Always run router on the primed normalized query
        r = route_intent(query_norm)
        intent = (r.get("intent") or "").lower()
        conf = float(r.get("confidence") or 0.0)
        print(f"[DEBUG][router] intent={intent} conf={conf:.2f} reason={r.get('reason')}")
    except Exception as e:
        print("[DEBUG][router] intent routing failed:", e)
        intent = intent or "analytics"


    try:
        if intent == "general_finance":
            # --- 1) Try data-driven advisor first (product if uniquely implied, else portfolio) ---
            try:
                eff_country = (country_override or parse_country_strict(query) or "").upper()
                if eff_country not in {"UK", "US"}:
                    # reuse last known country from context if available
                    store__ = globals().setdefault("LAST_CONTEXT", {})
                    lc__ = store__.get(int(user_id)) or {}
                    eff_country = lc__.get("country") or "UK"

                # Soft product inference from DB (no keyword rules):
                picked_product = None
                cands = product_candidates(engine, user_id, eff_country, query, limit=10) or []
                if len(cands) == 1:
                    picked_product = (cands[0].get("product_name") or "").strip()

                # Fallback to last context’s product if present
                lc_safe = (globals().setdefault("LAST_CONTEXT", {}).get(int(user_id)) or {})
                if not picked_product and lc_safe.get("product"):
                    picked_product = lc_safe["product"]
                    eff_country = lc_safe.get("country") or eff_country

                # Prefer recent history; clamp to available data
                tr = (clamp_relative_time_to_available(user_id, eff_country, "last 4 months")
                    or clamp_relative_time_to_available(user_id, eff_country, "last 3 months"))

                # If clamp couldn't determine a window, build safe 3-month span
                if not tr:
                    try:
                        latest_y, latest_m = get_latest_data_year_month(user_id, eff_country)
                        end_last = calendar.monthrange(latest_y, latest_m)[1]
                        end = f"{latest_y:04d}-{latest_m:02d}-{end_last:02d}"
                        start_m = latest_m - 2
                        start_y = latest_y
                        while start_m <= 0:
                            start_m += 12
                            start_y -= 1
                        start = f"{start_y:04d}-{start_m:02d}-01"
                        tr = {"start": start, "end": end}
                    except Exception:
                        tr = None

                plan2 = {
                    "operation": "trend",
                    "metric": "sales",   # advisor baseline; not used for follow-up synthesis
                    "time_range": tr,
                    "country": eff_country,
                    "group_by": None if picked_product else "product",
                    "sort_dir": "desc",
                    "product": picked_product,
                    "filters": [],
                }

                df2, mode2 = engine_q.exec_plan_via_formula(
                    plan=plan2, query=query, user_id=str(user_id), country_override=country_override
                )

                if df2 is not None and not df2.empty:
                    scope = "product" if picked_product else "portfolio"
                    target = picked_product if picked_product else None

                    advice_lines = BusinessAdvisor.recommend(
                        query,
                        df2.copy(),
                        aux={
                            "country": eff_country,
                            "time_range": tr,
                            "scope": scope,
                            "target": target,
                        },
                    )
                    reply = "\n".join(advice_lines) if advice_lines else \
                            "I couldn’t derive targeted growth actions from the latest data."
                    msg_id = save_chat_to_db(user_id, query, reply) or None
                    return ok({"mode": "advisor", "response": reply, "message_id": msg_id})
            except Exception as e:
                print("[DEBUG][advisor-fallback] failed:", e)

            # --- 2) Only if no usable data, fall back to generic LLM ---
            reply = generate_general_answer(query)
            msg_id = save_chat_to_db(user_id, query, reply) or None
            return ok({"mode": "general_finance", "response": reply, "message_id": msg_id})

        if intent == "chit_chat":
            reply = "Hey! 👋 I can help you analyze Amazon sales, fees, taxes, profit, and trends. What would you like to explore?"
            msg_id = save_chat_to_db(user_id, query, reply) or None
            return ok({"mode": "smalltalk", "response": reply, "message_id": msg_id})

        if intent == "out_of_scope":
            # Advisor fallback (data-driven; no hard-coded keywords)
            try:
                eff_country = (country_override or parse_country_strict(query) or "").upper()
                if eff_country not in {"UK", "US"}:
                    lc_ctry = (globals().setdefault("LAST_CONTEXT", {}).get(int(user_id)) or {}).get("country")
                    eff_country = (lc_ctry or "UK")

                picked_product = None
                cands = product_candidates(engine, user_id, eff_country, query, limit=10) or []
                if len(cands) == 1:
                    picked_product = (cands[0].get("product_name") or "").strip()
                if not picked_product and lc and lc.get("product"):
                    picked_product = lc["product"]
                    eff_country = lc.get("country") or eff_country

                tr = clamp_relative_time_to_available(user_id, eff_country, "last 4 months") \
                    or clamp_relative_time_to_available(user_id, eff_country, "last 3 months")

                plan2 = {
                    "operation": "trend",
                    "metric": "sales",   # advisor baseline; not used for follow-up synthesis
                    "time_range": tr,
                    "country": eff_country,
                    "group_by": None if picked_product else "product",
                    "sort_dir": "desc",
                    "product": picked_product,
                    "filters": [],
                }

                df2, mode2 = engine_q.exec_plan_via_formula(
                    plan=plan2, query=query, user_id=str(user_id), country_override=country_override
                )

                if df2 is not None and not df2.empty:
                    scope = "product" if picked_product else "portfolio"
                    target = picked_product if picked_product else None

                    advice_lines = BusinessAdvisor.recommend(
                        query,
                        df2.copy(),
                        aux={
                            "country": eff_country,
                            "time_range": tr,
                            "scope": scope,
                            "target": target,
                        },
                    )
                    reply = "\n".join(advice_lines) if advice_lines else \
                        "I couldn’t derive targeted growth actions from the latest data."
                    msg_id = save_chat_to_db(user_id, query, reply) or None
                    return ok({"mode": "advisor", "response": reply, "message_id": msg_id})
            except Exception as e:
                print("[DEBUG][advisor-fallback] failed:", e)

            reply = "That seems outside my scope. Could you rephrase your question to focus on finance, business, or your Amazon data?"
            msg_id = save_chat_to_db(user_id, query, reply) or None
            return ok({"mode": "out_of_scope", "response": reply, "message_id": msg_id})

        # else intent == analytics → fall through to planner
    except Exception as e:
        print("[DEBUG][router] intent routing failed (outer):", e)
        # Fall through to planner on any unexpected routing error.

    # ------------ STEP 3.48: SKU lookup (router-gated, no early exit on empty) ------------
    if intent == "sku_lookup":
        ql = query.lower()

        # 1) Extract a candidate phrase after “sku/skuS [for|of] …”
        m = re.search(r"\bsku(?:s)?\s*(?:for|of)?\s*(.*)", ql)
        if m and m.group(1).strip():
            phrase = m.group(1).strip(" ?.")
        else:
            # If we didn’t catch a tail phrase, use the whole query as a fuzzy input
            phrase = ql.strip(" ?.")

        # 2) Resolve country (override > explicit in query > default UK)
        eff_country = (country_override or parse_country_strict(query) or "UK").upper()
        if eff_country not in {"UK", "US"}:
            eff_country = "UK"

        # 3) Look up product/SKU candidates using your DB semantic matcher
        rows = product_candidates(engine, user_id, eff_country, phrase, limit=20) or []

        # 4) If we found candidates → return a short SKU list (early success path)
        if rows:
            lines = []
            for r in rows:
                pn = (r.get("product_name") or "(unknown product)").strip()
                sk = (r.get("sku") or "(no SKU)").strip()
                lines.append(f"- {pn}: {sk}")
            reply = "Here are the matching SKUs:\n" + "\n".join(lines)
            msg_id = save_chat_to_db(user_id, query, reply) or None
            return ok({"mode": "sku_lookup", "response": reply, "message_id": msg_id})

        # 5) If NO candidates → DO NOT return. Fall through to planner/advisor.
        print("[DEBUG][sku-lookup] router said sku_lookup but no candidates; falling through.")


    # ---------------------- Plan + slot-filling -------------------------------
    try:
        # Preserve old "clamp to available" ONLY when the user actually mentioned time.
        fp = FilterParser()
        _t = fp.parse_time(query)
        _user_mentioned_time = bool(
            _t.get("explicit_day") or _t.get("months") or _t.get("years") or _t.get("date_range")
        )

        if _user_mentioned_time:
            clamped = clamp_relative_time_to_available(user_id, country_override, query)
        else:
            clamped = None

        query_for_plan = f"{query} (period: {clamped['start']} to {clamped['end']})" if clamped else query

        # 👉 Prime planner only for FOLLOW-UP
        if decision.get("mode") == "followup":
            # re-read latest LAST_CONTEXT just in case
            _store = globals().setdefault("LAST_CONTEXT", {})
            _lc    = _store.get(int(user_id)) or {}
            _ctx   = _planner_context_suffix(_lc)
            if _ctx:
                query_for_plan = f"{query_for_plan} {_ctx}"
                print("[DEBUG][planner] primed with", _ctx)

        prefilled = advisor_preplan(query, user_id=user_id, country_override=country_override)

        # Planner sees normalized query (with any clamped period text)
        query_for_plan_norm = normalize_user_query(query_for_plan)
        plan = prefilled or plan_query(query_for_plan_norm)
        print("[DEBUG] Plan (raw):", plan)

        plan = _normalize_plan_for_sku_language(plan, query)
        print("[DEBUG] Plan (normalized):", plan)
        lc_for_defaults = (globals().setdefault("LAST_CONTEXT", {}).get(int(user_id)) or {})
        plan, _filled = _auto_fill_defaults(plan, query, user_id, country_override, lc_for_defaults)


        # --- BACKFILL CONTEXT FOR FOLLOW-UPS (after plan rebuild) ---
        def _has(v): 
            return v is not None and v != "" and v != {}

        if decision.get("mode") == "followup":
            _lc = (globals().setdefault("LAST_CONTEXT", {}).get(int(user_id)) or {})

            if not _has(plan.get("metric")):
                plan["metric"] = _lc.get("metric")

            # <- this is the one that prevents the country re-ask
            if not _has(plan.get("country")):
                plan["country"] = _lc.get("country")

            if not _has(plan.get("time_range")):
                if _looks_anaphoric_to_time(query):
                    plan["time_range"] = _lc.get("time_range")

            if not _has(plan.get("product")):
                if _is_anaphoric_to_product(query):
                    plan["product"] = _lc.get("product")
                else:
                    plan.pop("product", None)
                plan["force_product_only"] = False


            # If time still missing and user didn't mention one, clamp safely
            try:
                fp_tmp = FilterParser()
                t_tmp = fp_tmp.parse_time(query)
                user_mentioned_time = bool(
                    t_tmp.get("explicit_day") or t_tmp.get("months") or
                    t_tmp.get("years") or t_tmp.get("date_range")
                )
                if not _has(plan.get("time_range")) and not user_mentioned_time:
                    eff_country = plan.get("country") or _lc.get("country") or country_override
                    tr = clamp_relative_time_to_available(user_id, eff_country, "last 3 months")
                    if tr:
                        plan["time_range"] = {"start": tr["start"], "end": tr["end"]}
                        print(f"[DEBUG][followup/main] clamp filled time_range → {plan['time_range']}")
            except Exception as e:
                print("[DEBUG][followup/main] clamp fill failed:", e)



        # Advisor short-circuit (main branch)
        if isinstance(plan, dict) and (plan.get("operation") or "").lower() == "advisor":
            advisor = BusinessAdvisor(engine, user_id, country_override)

            product_phrase = (plan.get("product") or "").strip()
            table_name = engine_q.builder.table_for(str(user_id), plan.get("country") or country_override)

            if product_phrase:
                advice_text = advisor.answer_for_product(product_phrase, table_name)
            else:
                advice_text = advisor.answer(query)

            msg_id = save_chat_to_db(user_id, query, advice_text) or None
            return ok({"mode": "advisor", "response": advice_text, "message_id": msg_id})


        # Natural-language time backfill
        try:
            if not (isinstance(plan.get("time_range"), dict) and plan["time_range"].get("start")):
                fp2 = FilterParser()
                t2 = fp2.parse_time(query)
                if t2.get("months"):
                    months = [m["number"] for m in t2["months"]]
                    years = t2.get("years") or []
                    if len(months) == 1:
                        m = int(months[0])
                        y = int(years[0]) if years else get_latest_data_year_month(user_id, country_override)[0]
                        last = calendar.monthrange(y, m)[1]
                        plan["time_range"] = {"start": f"{y:04d}-{m:02d}-01", "end": f"{y:04d}-{m:02d}-{last:02d}"}
                    else:
                        y = int((years or [get_latest_data_year_month(user_id, country_override)[0]])[0])
                        m1, m2 = min(months), max(months)
                        last = calendar.monthrange(y, m2)[1]
                        plan["time_range"] = {"start": f"{y:04d}-{m1:02d}-01", "end": f"{y:04d}-{m2:02d}-{last:02d}"}
                elif t2.get("date_range"):
                    start_dt, end_dt = t2["date_range"]
                    plan["time_range"] = {
                        "start": start_dt.date().isoformat(),
                        "end":   (end_dt - dt.timedelta(days=1)).date().isoformat()
                    }
                elif t2.get("years"):
                    y = int(t2["years"][0])
                    plan["time_range"] = {"start": f"{y}-01-01", "end": f"{y}-12-31"}
            print("[DEBUG] Plan after NL time fill:", plan)
            # --- Clamp any "future" ranges to last available month ----------------------
            try:
                tr = plan.get("time_range")
                if isinstance(tr, dict) and tr.get("start") and tr.get("end"):
                    # find last full month end
                    last_y, last_m = _last_full_month_today()
                    last_day = calendar.monthrange(last_y, last_m)[1]
                    last_full_end = dt.date(last_y, last_m, last_day)

                    end_dt = dt.date.fromisoformat(tr["end"][:10])
                    start_dt = dt.date.fromisoformat(tr["start"][:10])

                    # If the range goes beyond available data → clamp it
                    if end_dt > last_full_end:
                        end_dt = last_full_end
                    if start_dt > end_dt:
                        start_dt = end_dt - dt.timedelta(days=89)  # roughly last 3 months

                    # Final assignment
                    plan["time_range"] = {
                        "start": start_dt.isoformat(),
                        "end": end_dt.isoformat(),
                    }
                    print(f"[DEBUG] Plan after FUTURE clamp → {plan['time_range']}")
            except Exception as e:
                print("[DEBUG][WARN] Future clamp failed:", e)
        except Exception as _e:
            print("[DEBUG][WARN] NL time backfill failed:", _e)

        # Month-group fallback → span available history
        try:
            if not plan.get("time_range") and (plan.get("group_by") or "").lower() == "month":
                eff_country = country_override or plan.get("country")
                (earliest_y, earliest_m), (latest_y, latest_m) = get_data_span_year_month(user_id, eff_country)
                start_span = f"{earliest_y:04d}-{earliest_m:02d}-01"
                last_day = calendar.monthrange(latest_y, latest_m)[1]
                end_span = f"{latest_y:04d}-{latest_m:02d}-{last_day:02d}"
                plan["time_range"] = {"start": start_span, "end": end_span}
                print("[DEBUG] Filled default time_range for month grouping:", plan["time_range"])
        except Exception as _e:
            print("[DEBUG][WARN] month-group default time fill failed:", _e)

        # Country normalization
        try:
            if not plan.get("country"):
                ctry = (
                    parse_country_strict(query)
                    or parse_country_strict(orig_q)
                    or country_override
                    or (globals().setdefault("LAST_CONTEXT", {}).get(int(user_id), {}).get("country"))
                    or None
                )
                if ctry:
                    plan["country"] = ctry
                    print("[DEBUG] Plan after country normalization:", plan["country"])
        except Exception as _e:
            print("[DEBUG][WARN] country normalization failed:", _e)

        # --- Product disambiguation (with context auto-pick) -----------------
        try:
            from_app_filters = engine_q.filters  # FilterParser instance

            # 1) Guess product phrase, then VALIDATE (no pronouns/short junk; must exist in catalog)
            guessed_raw = None if plan.get("product") else from_app_filters.guess_product_phrase(query)
            guessed = guessed_raw if is_valid_product_phrase(guessed_raw) else None
            if guessed:
                eff_country = (country_override or plan.get("country") or parse_country_strict(query) or "UK").upper()
                if eff_country not in {"UK","US"}:
                    eff_country = "UK"
                _probe = product_candidates(engine, user_id, eff_country, guessed, limit=3) or []
                if not _probe:
                    guessed = None

            wants_split = plan.get("group_by") in {"product", "sku"} or plan.get("operation") == "rank"
            explicit_product = bool(plan.get("product") or guessed)

            cands = []
            product_phrase = ""

            # 2) If there is NO product intent, skip disambiguation and pick portfolio defaults
            if not wants_split and not explicit_product:
                print("[DEBUG] no product intent detected → portfolio-level analysis (no clarify)")
                plan.pop("product", None)
                plan["force_product_only"] = False
                op = (plan.get("operation") or "").lower()
                if op == "trend" and not plan.get("group_by"):
                    plan["group_by"] = "month"        # portfolio trend over time
                elif op == "compare" and not plan.get("group_by"):
                    plan["group_by"] = "product"      # auto-compare top products
                    plan.setdefault("top_k", 5)

            else:
                # 3) Proceed with product resolution if user intent exists
                product_phrase = (plan.get("product") or guessed or "").strip()
                eff_country = (country_override or plan.get("country") or parse_country_strict(query) or "UK").upper()
                if eff_country not in {"UK","US"}:
                    eff_country = "UK"

                norm_pf = product_phrase.lower()
                if any(k in norm_pf for k in ("all products","all product","all skus","all sku","everything","all variants","any product")):
                    plan["product"] = None
                    plan["filters"] = [f for f in (plan.get("filters") or [])
                                    if str(f.get("field","")).lower() not in {"sku","product","product_name"}]
                    if not plan.get("group_by"):
                        plan["group_by"] = "product"
                    print("[DEBUG] main: 'all products' → cleared product/SKU filters; group_by=product")
                else:
                    # Only look up candidates if phrase passes validation
                    if product_phrase and is_valid_product_phrase(product_phrase):
                        cands = product_candidates(engine, user_id, eff_country, product_phrase, limit=20) or []
                        if not cands:
                            try:
                                resolved = resolve_product_entities(
                                    query=product_phrase, engine=engine, user_id=int(user_id),
                                    country=eff_country, top_k=10
                                ) or []
                            except Exception:
                                resolved = []
                            if resolved:
                                cands = [{"product_name": r.get("product_name")} for r in resolved if r.get("product_name")]
                    else:
                        print("[DEBUG] product phrase invalid; skipping candidate lookup")

                # 4) Apply context reuse or clarification logic
                if not cands:
                    # --- Context reuse guard ---
                    store2 = globals().setdefault("LAST_CONTEXT", {})
                    lc2 = store2.get(int(user_id)) or {}

                    is_portfolio_level = plan.get("group_by") in {"month", "year", "country"} \
                        or plan.get("operation") in {"rank", "aggregate_overall"}

                    # Only reuse context if FOLLOW-UP and not portfolio-level
                    if decision and decision.get("mode") == "followup" and not is_portfolio_level:
                        if _is_anaphoric_to_product(query):
                            if lc2.get("last_skus"):
                                plan["product"] = lc2["last_skus"][-1]
                                print(f"[DEBUG][planner] Reusing last_skus (hint) → {plan['product']!r}")
                            elif lc2.get("product"):
                                plan["product"] = lc2["product"]
                                print(f"[DEBUG][planner] Reusing last product (hint) → {plan['product']!r}")
                        else:
                            plan.pop("product", None)
                            print("[DEBUG][planner] Follow-up but no anaphora → not reusing product")
                        plan["force_product_only"] = False
                    else:
                        plan.pop("product", None)
                        plan["force_product_only"] = False
                        print("[DEBUG][planner] Skipping product reuse (new query or portfolio-level)")

                elif len(cands) == 1:
                    plan["product"] = cands[0]["product_name"]
                    plan["force_product_only"] = False  # equality only if user said "only ..."
                    print(f"[DEBUG] Resolved product (hint) → {plan['product']!r}")

                else:
                    picked = False
                    try:
                        # Auto-pick from context among multiple candidates if FOLLOW-UP
                        if decision and decision.get("mode") == "followup":
                            store2 = globals().setdefault("LAST_CONTEXT", {})
                            lc2 = store2.get(int(user_id)) or {}
                            ctx_prod = (lc2.get("product") or "").strip().lower()
                            if ctx_prod:
                                name_map = {
                                    (c.get("product_name") or "").strip().lower():
                                    (c.get("product_name") or "").strip()
                                    for c in cands
                                }
                                if ctx_prod in name_map and _is_anaphoric_to_product(query):
                                    plan["product"] = name_map[ctx_prod]
                                    plan["force_product_only"] = False
                                    picked = True
                                    print(f"[DEBUG] auto-picked product from context (hint) → {plan['product']!r}")
                    except Exception as _e:
                        print("[DEBUG] context auto-pick failed:", _e)

                    if not picked:
                        # ✅ NEW: No clarification, just auto-pick one product

                        chosen = None
                        try:
                            # Simple choice: first candidate
                            chosen = (cands[0].get("product_name") or "").strip()
                        except Exception as _e:
                            print("[DEBUG] auto-pick from cands failed:", _e)

                        if chosen:
                            plan["product"] = chosen
                            plan["force_product_only"] = False
                            print(f"[DEBUG] auto-picked product among multiple candidates → {plan['product']!r}")
                        else:
                            # If kuch valid naam nahi mila to product filter hata do
                            print("[DEBUG] multiple candidates but no usable product name → proceeding without product filter")
                            plan.pop("product", None)
                            plan["force_product_only"] = False


        except Exception as _e:
            print("[DEBUG][WARN] product disambiguation failed:", _e)


        # --- Generic slot detection (top_k, country, time_range) --------------
        # Ensure country is set on follow-ups before we ask for it
        if decision.get("mode") == "followup" and not plan.get("country"):
            plan["country"] = (globals().setdefault("LAST_CONTEXT", {}).get(int(user_id), {}).get("country"))

        # --- Step 4 — Advisor queries skip clarification entirely -------------------
        try:
            if wants_advice(query, plan):
                # Don’t ask any questions — run with safe defaults.
                plan["needs_clarification"] = False

                # Fill minimal, safe defaults so execution won’t fail.
                if not plan.get("country"):
                    # prefer explicit override, then last context, then UK
                    plan["country"] = (country_override
                                    or (globals().setdefault("LAST_CONTEXT", {}).get(int(user_id), {}) or {}).get("country")
                                    or "UK")

                if not plan.get("time_range"):
                    # pick a short recent window that your data almost always has
                    tr = clamp_relative_time_to_available(user_id, plan["country"], "last 90 days") \
                        or clamp_relative_time_to_available(user_id, plan["country"], "last 3 months")
                    if tr:
                        plan["time_range"] = {"start": tr["start"], "end": tr["end"]}

                # Also avoid forcing a product unless the user clearly referred back
                if plan.get("product") is True:
                    plan["product"] = None

                # Skip the slots_missing_for branch entirely by jumping to execution.
                print("[DEBUG][advisor-skip] bypassing clarification for advisor-style request")
                # (Fall through to the normal execution path below)
        except Exception as _e:
            print("[DEBUG][advisor-skip] failed:", _e)

        missing = slots_missing_for(plan, query, country_override, parse_time_fn=FilterParser().parse_time)

        # Ignore “soft” slots (top_k, sort_dir, etc.)
        soft = {"top_k", "sort_dir", "group_by", "product"}
        missing = [m for m in (missing or []) if m not in soft]

        # If we already auto-filled, skip clarifying
        if missing and plan.get("needs_clarification", False):
            first_prompt = make_ask_prompt(missing[0])
            PENDING.set(user_id, plan, missing, reprompt=first_prompt, original_query=query)
            msg_id = save_chat_to_db(user_id, query, first_prompt) or None
            return ok({"mode": "clarify", "response": first_prompt, "message_id": msg_id})


        # Print final plan
        print("[DEBUG] Plan (final):", plan)

        # --- Execute ---------------------------------------------------------
        df, mode = engine_q.exec_plan_via_formula(
            plan=plan, query=query, user_id=str(user_id), country_override=country_override
        )

        
        # Recovery on empty
        if df is None or df.empty:
            df, mode, plan = _recover_empty_result(df, mode, plan, query)

            if df is None or df.empty:
                # --- ADVISOR FALLBACK: try last 90 days if user asked for advice/plan ---
                try:
                    if wants_advice(query, plan) or ("plan" in (plan.get("operation") or "").lower()):
                        eff_country = country_override or plan.get("country") or "UK"
                        tr_recent = clamp_relative_time_to_available(user_id, eff_country, "last 90 days")

                        if tr_recent and tr_recent.get("start") and tr_recent.get("end"):
                            df_recent, mode_recent = engine_q.exec_plan_via_formula(
                                plan={
                                    "operation": "trend",
                                    "metric": plan.get("metric") or "sales",
                                    "time_range": tr_recent,
                                    "country": eff_country,
                                    "group_by": plan.get("group_by") or "product",
                                    "filters": [],
                                },
                                query=query,
                                user_id=str(user_id),
                                country_override=eff_country,
                            )

                            if df_recent is not None and not df_recent.empty:
                                scope = plan.get("group_by") or "portfolio"
                                advice_lines = BusinessAdvisor.recommend(
                                    query,
                                    df_recent,
                                    aux={
                                        "country": eff_country,
                                        "time_range": tr_recent,
                                        "scope": scope,
                                        "target": plan.get("product"),
                                    },
                                )
                                reply = "\n".join(advice_lines) if advice_lines else \
                                    "I couldn’t derive targeted growth actions from recent data."
                                msg_id = save_chat_to_db(user_id, query, reply) or None
                                return ok({"mode": "advisor", "response": reply, "message_id": msg_id})
                except Exception as _e:
                    print("[DEBUG][advisor fallback] failed:", _e)

                # --- Default response if still empty ---
                reply = "No data found for your query."
                msg_id = save_chat_to_db(user_id, query, reply) or None
                return ok({"response": reply, "message_id": msg_id, "mode": mode})


        # SKU clarification (normal)
        if mode == "sql_special" and "_" in df.columns and len(df) == 1:
            reply = str(df.iloc[0]["_"])
            if re.search(r"(multiple\s+skus|one\s+specific\s+variant|all\s+variants)", reply, re.I):
                PENDING.set(
                    user_id,
                    plan,
                    missing=["sku_choice"],
                    reprompt=reply,
                    original_query=query,
                    country_override=country_override,
                )
                msg_id = save_chat_to_db(user_id, query, reply) or None
                return ok({"mode": "clarify", "response": reply, "message_id": msg_id})
            msg_id = save_chat_to_db(user_id, query, reply) or None
            return ok({"response": reply, "message_id": msg_id, "mode": mode})

        
        # Formula mode → render table narrative OR action plan
        if mode == "sql_formula":
            table_records = df_to_records_safe(df)
            final_records = _finalize_records(plan, table_records)

            # decide advice vs report
            if wants_advice(query, plan):
                advice_lines = BusinessAdvisor.recommend(
                    query,
                    df,
                    aux={
                        "country": country_override or plan.get("country"),
                        "time_range": plan.get("time_range"),
                        "scope": (
                            "sku"
                            if plan.get("force_product_only")
                            and isinstance(plan.get("product"), str)
                            and plan["product"].upper() == plan["product"]
                            else "product"
                            if plan.get("product")
                            else "portfolio"
                        ),
                        "target": plan.get("product"),
                    },
                )
                reply = "\n".join(advice_lines) if advice_lines else \
                        "I couldn’t derive targeted growth actions from the latest data."
                used_mode = "advisor"
            else:
                reply = generate_openai_answer(
                    user_query=query,
                    mode="sql_formula",
                    analysis=None,
                    table_records=final_records,
                )
                used_mode = mode

            try:
                _stash_context(user_id, plan, country_override, table_records=final_records,user_msg=orig_q )
                _local = globals().setdefault("LAST_CONTEXT", {})
                FOLLOWUP_MEMORY.push(_local.get(int(user_id), {}))

            except Exception:
                pass

            msg_id = save_chat_to_db(user_id, query, reply) or None
            return ok({
                "mode": used_mode,
                "response": reply,
                "message_id": msg_id,
                "table": final_records
            })


        # Default: analysis narrative
        analysis = analyst.analyze_results(df, query)
        reply = generate_openai_answer(
            user_query=query,
            mode=mode if mode else "sql",
            analysis=analysis,
            table_records=None,
        )
        try:
            _stash_context(
                user_id,
                plan,
                country_override,
                table_records=None,
                user_msg=orig_q,
            )
            _local = globals().setdefault("LAST_CONTEXT", {})
            FOLLOWUP_MEMORY.push(_local.get(int(user_id), {}))

        except Exception:
            pass

        msg_id = save_chat_to_db(user_id, query, reply) or None
        return ok({"response": reply, "message_id": msg_id, "mode": mode})

    except Exception as e:
        import traceback
        traceback.print_exc()
        print("[DEBUG][BE][Error Response]:", str(e))
        return Response(
            json.dumps({
                "success": False,
                "message": "Unexpected error processing your request.",
                "error": str(e)
            }, allow_nan=False),
            status=500,
            mimetype="application/json"
        )





#utils class business advisor



class BusinessAdvisor:
    """
    Keyword-free advisor:
    - Reads monthly series (overall + product) from df_primary
    - Builds a customized growth playbook based on signals in the data
    - Optionally uses aux['ads'] (monthly ads_spend) to compute ACoS deltas
    Returns: list[str] of concise, actionable recommendations
    """

    # ---------- helpers (unchanged from your version, minor robustness) ----------
    @staticmethod
    def _parse_period_series(df: pd.DataFrame, value_col: str, scope="overall") -> pd.DataFrame:
        if df is None or df.empty or value_col not in df.columns or "period" not in df.columns:
            return pd.DataFrame()
        d = df.copy()
        d["_period_dt"] = pd.to_datetime(d["period"], errors="coerce", utc=True)
        if d["_period_dt"].isna().all():
            try:
                d["_period_dt"] = pd.to_datetime(d["period"], format="%b %Y", errors="coerce", utc=True)
            except Exception:
                pass
        if "scope" in d.columns and scope:
            d = d[d["scope"].astype(str).str.lower().eq(scope.lower())]
        return d.dropna(subset=["_period_dt"]).sort_values("_period_dt")

    @staticmethod
    def _last2(df: pd.DataFrame, value_col: str):
        if df.empty or value_col not in df.columns:
            return None, None, None, None
        tail = df[["_period_dt", value_col]].dropna().sort_values("_period_dt").tail(2)
        if len(tail) < 2:
            return None, None, None, None
        prev_dt, last_dt = tail["_period_dt"].iloc[0], tail["_period_dt"].iloc[1]
        prev, last = float(tail[value_col].iloc[0] or 0.0), float(tail[value_col].iloc[1] or 0.0)
        return prev, last, prev_dt.strftime("%b %Y"), last_dt.strftime("%b %Y")

    @staticmethod
    def _ensure_product_col(d: pd.DataFrame) -> pd.DataFrame:
        out = d.copy()
        if "product" not in out.columns:
            if "key" in out.columns: out["product"] = out["key"]
            elif "label" in out.columns: out["product"] = out["label"]
        return out

    @staticmethod
    def _product_rollup(df: pd.DataFrame, cols=("sales","profit","quantity","fba_fees")) -> pd.DataFrame:
        d = BusinessAdvisor._ensure_product_col(df)
        if "product" not in d.columns:
            return pd.DataFrame()
        if "period" in d.columns:
            d["_period_dt"] = pd.to_datetime(d["period"], errors="coerce", utc=True)
            d = d.dropna(subset=["_period_dt"]).sort_values("_period_dt")
            recent = sorted(d["_period_dt"].unique())[-4:]  # last 4 periods
            d = d[d["_period_dt"].isin(recent)]
        keep = [c for c in cols if c in d.columns]
        if not keep:
            return pd.DataFrame()
        return d.groupby("product", dropna=True)[keep].sum().reset_index()

    @staticmethod
    def _growth_by_product(df: pd.DataFrame, value_col="sales") -> pd.DataFrame:
        d = BusinessAdvisor._ensure_product_col(df)
        req = {"product","period", value_col}
        if not req.issubset(set(d.columns)):
            return pd.DataFrame()
        d["_period_dt"] = pd.to_datetime(d["period"], errors="coerce", utc=True)
        d = d.dropna(subset=["_period_dt"])
        periods = sorted(d["_period_dt"].unique())[-2:]
        if len(periods) < 2:
            return pd.DataFrame()
        p0, p1 = periods
        a = d[d["_period_dt"].eq(p0)].groupby("product")[value_col].sum().rename(f"{value_col}_prev")
        b = d[d["_period_dt"].eq(p1)].groupby("product")[value_col].sum().rename(f"{value_col}_last")
        g = pd.concat([a, b], axis=1).fillna(0.0)
        g["growth_abs"] = g[f"{value_col}_last"] - g[f"{value_col}_prev"]
        g["growth_pct"] = np.where(g[f"{value_col}_prev"]>0, (g["growth_abs"]/g[f"{value_col}_prev"])*100.0, np.nan)
        return g.reset_index().sort_values(["growth_abs","growth_pct"], ascending=[False, False])

    # ---------- main: keyword-free, data-driven recommendations ----------
    @staticmethod
    def recommend(query: str, df_primary: pd.DataFrame, aux: dict | None = None) -> list[str]:
        """
        Data-driven action plan.
        - Canonicalizes many Amazon export column names -> {sales, profit, quantity, asp, ...}
        - Constructs a monthly 'period'
        - Computes profit and ASP if missing
        - Produces rollups (totals, by_period, by_entity) to ground the LLM
        - Anchors the 30-day checklist to the latest period in the data/time_range
        Returns: list[str] bullet points (title + actions + 30-day checklist).
        """
        aux = aux or {}
        scope   = aux.get("scope") or "auto"        # "sku" | "product" | "portfolio" | "auto"
        target  = aux.get("target")                 # e.g. "SEWIPESNEW" or "Classic"
        country = aux.get("country") or "US"
        tr      = aux.get("time_range")             # dict or string

      

        def _safe_float(x):
            try:
                f = float(x)
                return f if np.isfinite(f) else 0.0
            except Exception:
                return 0.0

        # ---- 1) Canonicalize columns --------------------------------------------
        alias_map = {
            # identifiers / time
            "product_name": "product",
            "asin": "asin",
            "sku": "sku",
            "date": "date_time",
            "datetime": "date_time",
            "date_time": "date_time",
            "year": "year",
            "month": "month",
            "key": "key",
            "label": "label",
            # core metrics
            "product_sales": "sales",
            "ordered_revenue": "sales",
            "revenue": "sales",
            "quantity": "quantity",
            "ordered_units": "quantity",
            "units": "quantity",
            "profit": "profit",
            "total": "net_total",
            # fees / credits / deductions
            "fba_fees": "fba_fees",
            "fulfillment_fees": "fba_fees",
            "selling_fees": "selling_fees",
            "referral_fees": "selling_fees",
            "promotional_rebates": "promotional_rebates",
            "marketplace_facilitator_tax": "mft",
            "other_transaction_fees": "other_txn_fees",
            "shipping_credits": "shipping_credits",
            "postage_credits": "postage_credits",
            "gift_wrap_credits": "gift_wrap_credits",
            "other": "other",
            # optional taxes
            "product_sales_tax": "product_sales_tax",
            "shipping_credits_tax": "shipping_credits_tax",
            "giftwrap_credits_tax": "giftwrap_credits_tax",
            "promotional_rebates_tax": "promotional_rebates_tax",
        }

        canonical_order = [
            "period", "date_time", "country", "product", "sku",
            "sales", "profit", "quantity", "asp",
            "fba_fees", "selling_fees", "promotional_rebates", "mft",
            "other_txn_fees", "shipping_credits", "postage_credits",
            "gift_wrap_credits", "other", "net_total"
        ]
        allowed_metrics = ["sales", "profit", "quantity", "asp", "fba_fees", "selling_fees"]

        # Defensive copy; handle empty
        if not isinstance(df_primary, pd.DataFrame) or df_primary.empty:
            d = pd.DataFrame()
            payload = {
                "meta": {"scope": scope, "target": target, "country": country, "time_range": tr},
                "columns": [], "samples": [], "rollups": {}
            }
        else:
            d = df_primary.copy()
            # Lowercase map
            d = d.rename(columns={c: alias_map.get(str(c).strip().lower(), str(c).strip().lower()) for c in d.columns})

            # ---- 2) Period construction ------------------------------------------
            if "date_time" in d.columns:
                dt = pd.to_datetime(d["date_time"], errors="coerce")
                d["period"] = dt.dt.to_period("M").astype(str)
            elif {"year", "month"}.issubset(d.columns):
                def _ym_to_date(y, m):
                    try: return pd.Timestamp(year=int(y), month=int(m), day=1)
                    except Exception: return pd.NaT
                d["_tmp_dt"] = [_ym_to_date(y, m) for y, m in zip(d["year"], d["month"])]
                d["period"] = pd.to_datetime(d["_tmp_dt"], errors="coerce").dt.to_period("M").astype(str)
                d.drop(columns=["_tmp_dt"], errors="ignore", inplace=True)

            if "product" not in d.columns and "product_name" in df_primary.columns:
                d["product"] = df_primary["product_name"]

            # ---- 3) Numeric cleaning + derived -----------------------------------
            numeric_like = [
                "sales","profit","quantity","fba_fees","selling_fees",
                "promotional_rebates","mft","other_txn_fees",
                "shipping_credits","postage_credits","gift_wrap_credits","other","net_total",
                "product_sales_tax","shipping_credits_tax","giftwrap_credits_tax","promotional_rebates_tax",
            ]
            for c in numeric_like:
                if c in d.columns: d[c] = d[c].map(_safe_float)

            if "profit" not in d.columns:
                sales = d["sales"] if "sales" in d.columns else 0.0
                pos_add = (
                    (d["shipping_credits"] if "shipping_credits" in d.columns else 0.0) +
                    (d["postage_credits"] if "postage_credits" in d.columns else 0.0) +
                    (d["gift_wrap_credits"] if "gift_wrap_credits" in d.columns else 0.0) +
                    (d["other"] if "other" in d.columns else 0.0)
                )
                neg_add = (
                    (d["fba_fees"] if "fba_fees" in d.columns else 0.0) +
                    (d["selling_fees"] if "selling_fees" in d.columns else 0.0) +
                    (d["promotional_rebates"] if "promotional_rebates" in d.columns else 0.0) +
                    (d["mft"] if "mft" in d.columns else 0.0) +
                    (d["other_txn_fees"] if "other_txn_fees" in d.columns else 0.0)
                )
                d["profit"] = sales + pos_add + neg_add

            if "asp" not in d.columns and {"sales","quantity"}.issubset(d.columns):
                qty = d["quantity"].replace(0, np.nan)
                d["asp"] = (d["sales"] / qty).replace([np.inf, -np.inf], np.nan).fillna(0.0)

            keep_cols = [c for c in canonical_order if c in d.columns]
            if "sku" in d.columns and "sku" not in keep_cols: keep_cols.append("sku")
            if "product" in d.columns and "product" not in keep_cols: keep_cols.append("product")
            d = d[keep_cols].copy()

            # ---- 4) Rollups -------------------------------------------------------
            payload = {
                "meta": {"scope": scope, "target": target, "country": country, "time_range": tr},
                "columns": list(d.columns), "samples": [], "rollups": {}
            }

            present = [c for c in ["sales","profit","quantity","asp","fba_fees","selling_fees"] if c in d.columns]
            if present:
                totals = d[present].sum(numeric_only=True).to_dict()
                payload["rollups"]["totals"] = {k: float(v) for k, v in totals.items()}

            if "period" in d.columns and present:
                try:
                    grp = d.groupby("period", dropna=True)[present].sum()
                    payload["rollups"]["by_period"] = grp.reset_index().to_dict(orient="records")
                except Exception:
                    pass

            key_col = "sku" if "sku" in d.columns else ("product" if "product" in d.columns else None)
            if key_col and "sales" in d.columns:
                top = d.groupby(key_col, dropna=True)["sales"].sum().sort_values(ascending=False).reset_index()
                payload["rollups"]["by_entity"] = top.head(30).to_dict(orient="records")

            try:
                d_sample = d.sort_values("sales", ascending=False).head(80) if "sales" in d.columns else d.head(80)
            except Exception:
                d_sample = d.head(80)
            payload["samples"] = d_sample.fillna("").to_dict(orient="records")

        # ---- 4.5) Derive concrete anchor dates from time_range or data ----------
        latest_period_end = None
        try:
            if isinstance(tr, dict) and tr.get("end"):
                latest_period_end = pd.to_datetime(tr["end"], errors="coerce")
            if latest_period_end is None and isinstance(d, pd.DataFrame) and not d.empty:
                if "period" in d.columns:
                    latest_period_end = pd.to_datetime(d["period"], errors="coerce").max()
                    if not pd.isna(latest_period_end):
                        latest_period_end = latest_period_end.to_period("M").to_timestamp("M")
                elif "date_time" in d.columns:
                    latest_period_end = pd.to_datetime(d["date_time"], errors="coerce").max()
        except Exception:
            latest_period_end = None

        if latest_period_end is None:
            latest_period_end = pd.Timestamp.utcnow().normalize()

        # normalize to month end and compute next-month start
        latest_period_end = latest_period_end.to_period("M").to_timestamp("M")
        next_month_start = (latest_period_end + pd.offsets.MonthBegin(1)).date()

        payload["meta"]["latest_period_end"] = latest_period_end.date().isoformat()
        payload["meta"]["next_month_start"] = next_month_start.isoformat()

        # ---- 5) Compose LLM prompt (force anchoring & ISO dates) -----------------
        system_msg = (
            "You are a senior Amazon marketplace growth strategist. "
            "Use only the provided payload (totals, by_period, by_entity, samples). "
            "Ground every recommendation in the numbers. Do NOT invent months or dates. "
            "Anchor scheduling to the provided dates:\n"
            "• Treat 'latest_period_end' as the last day with data.\n"
            "• Start the 30-day checklist from 'next_month_start'.\n"
            "Output requirements:\n"
            "1) Short, specific title\n"
            "2) 5–8 actionable bullets with numeric targets (prices, ACOS, budgets, % changes)\n"
            "3) 30-day checklist with 3–4 weekly milestones using ISO date ranges "
            "(YYYY-MM-DD to YYYY-MM-DD), derived from 'next_month_start'."
        )

        scope_hint = "SKU" if scope == "sku" else ("Product" if scope == "product" else "Portfolio")
        user_msg = (
            f"User asked: {query}\n\n"
            f"Context:\n"
            f"- Scope: {scope_hint}\n"
            f"- Target: {target or 'ALL'}\n"
            f"- Country: {country}\n"
            f"- Time Range: {tr}\n"
            f"- latest_period_end: {payload['meta']['latest_period_end']}\n"
            f"- next_month_start: {payload['meta']['next_month_start']}\n\n"
            f"DATA (JSON):\n{json.dumps(payload, default=str)[:120000]}"
        )

        try:
            resp = oa_client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": system_msg},
                    {"role": "user", "content": user_msg},
                ],
                temperature=0.25,
                max_tokens=1000,
            )
            text = (resp.choices[0].message.content or "").strip()
        except Exception as e:
            print("[DEBUG][advisor] GPT call failed:", e)
            return ["I wasn’t able to generate a growth plan right now."]

        # ---- 6) Parse compact result back to bullets ----------------------------
        lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
        bullets: list[str] = []
        for ln in lines:
            if ln.startswith(("-", "•", "*")):
                bullets.append(ln.lstrip("-•* ").strip())
            elif len(bullets) == 0 and len(ln) < 120:
                bullets.append(ln)  # Title

        return bullets or [text[:350]]
    
    # --- 1) Fetch trailing monthly panel for a product (no hardcoded keywords)
    @staticmethod
    def fetch_product_history(engine, table_name: str, product_phrase: str, months: int = 12) -> pd.DataFrame:
        if not engine or not table_name or not (product_phrase or "").strip():
            return pd.DataFrame()
        sql = text(f"""
            WITH base AS (
            SELECT
                date_time, product_name, sku, month, year,
                product_sales, product_sales_tax, promotional_rebates,
                postage_credits, gift_wrap_credits, shipping_credits,
                shipping_credits_tax, giftwrap_credits_tax, marketplace_facilitator_tax,
                fba_fees, selling_fees, other_transaction_fees, other,
                cost_of_unit_sold, quantity
            FROM {table_name}
            WHERE product_name ILIKE :p
            )
            SELECT
            to_char(to_date(year||'-'||lpad(month,2,'0')||'-01','YYYY-MM-DD'),'Mon YYYY') AS period,
            product_name AS product,
            SUM(COALESCE(product_sales,0))                                       AS sales_raw,
            SUM(COALESCE(product_sales_tax,0)+COALESCE(marketplace_facilitator_tax,0)+
                COALESCE(shipping_credits_tax,0)+COALESCE(giftwrap_credits_tax,0)+
                COALESCE(promotional_rebates_tax,0)+COALESCE(other_transaction_fees,0)) AS tax_raw,
            SUM(COALESCE(gift_wrap_credits,0)+COALESCE(shipping_credits,0))       AS credits_raw,
            SUM(COALESCE(fba_fees,0))                                             AS fba_fees_raw,
            SUM(COALESCE(selling_fees,0))                                         AS selling_fees_raw,
            SUM(COALESCE(other,0))                                                AS other_raw,
            SUM(COALESCE(cost_of_unit_sold,0))                                    AS cost_raw,
            SUM(COALESCE(quantity,0))                                             AS qty_raw
            FROM base
            GROUP BY 1,2
            ORDER BY MIN(to_date(year||'-'||lpad(month,2,'0')||'-01','YYYY-MM-DD')) DESC
            LIMIT :lim
        """)
        try:
            with engine.connect() as conn:
                df = pd.read_sql(sql, conn, params={"p": f"%{product_phrase}%", "lim": int(months)})
            # chronological
            return df.iloc[::-1].reset_index(drop=True)
        except Exception:
            return pd.DataFrame()

    # --- 2) Compute normalized features (metric-agnostic)
    @staticmethod
    def compute_period_features(df_monthly: pd.DataFrame) -> pd.DataFrame:
        if df_monthly is None or df_monthly.empty:
            return pd.DataFrame()
        d = df_monthly.copy()
        d["sales"]  = pd.to_numeric(d.get("sales_raw"), errors="coerce").fillna(0.0)
        tax         = pd.to_numeric(d.get("tax_raw"), errors="coerce").fillna(0.0)
        credits     = pd.to_numeric(d.get("credits_raw"), errors="coerce").fillna(0.0)
        fba         = pd.to_numeric(d.get("fba_fees_raw"), errors="coerce").fillna(0.0)
        selling     = pd.to_numeric(d.get("selling_fees_raw"), errors="coerce").fillna(0.0)
        other       = pd.to_numeric(d.get("other_raw"), errors="coerce").fillna(0.0)
        cost        = pd.to_numeric(d.get("cost_raw"), errors="coerce").fillna(0.0)
        qty         = pd.to_numeric(d.get("qty_raw"), errors="coerce").fillna(0.0)
        d["profit"] = d["sales"] + credits - tax - fba - selling - other - cost
        d["qty"]    = qty
        d["asp"]    = d.apply(lambda r: (r["sales"]/r["qty"]) if r["qty"] > 0 else np.nan, axis=1)
        return d[["period","product","sales","profit","qty","asp"]]

    # --- 3) Diagnose trends safely (works with short history)
    @staticmethod
    def diagnose_trends(d: pd.DataFrame) -> dict:
        out = {}
        if d is None or d.empty:
            return out
        n = len(d.index)
        idx = np.arange(n)
        for col in ["sales","profit","qty","asp"]:
            if col not in d.columns:
                continue
            ser = pd.to_numeric(d[col], errors="coerce").fillna(0.0)
            if n >= 2:
                try:
                    slope = float(np.polyfit(idx, ser, 1)[0])
                except Exception:
                    slope = 0.0
            else:
                slope = 0.0
            out[f"{col}_last"] = float(ser.iloc[-1]) if n else 0.0
            out[f"{col}_slope"] = slope
            if n >= 2:
                prev = float(ser.iloc[-2])
                out[f"{col}_chg_abs"] = out[f"{col}_last"] - prev
                out[f"{col}_chg_pct"] = (out[f"{col}_chg_abs"] / prev) if prev else None
        if "qty" in d.columns:
            out["qty_zero_share"] = float((pd.to_numeric(d["qty"], errors="coerce").fillna(0.0) <= 0).mean())
        out["months_available"] = int(d["period"].nunique()) if "period" in d.columns else n
        return out

    # --- 4) One-call advisor for a named product (graceful fallbacks)
    def answer_for_product(self, product_phrase: str, table_name: str, horizon: str = "next_3_months") -> str:
        hist = self.fetch_product_history(self.engine, table_name, product_phrase, months=12)
        if hist.empty:
            return f"I couldn’t find history for “{product_phrase}”. It may be new or inactive. Try a wider period."

        panel = self.compute_period_features(hist)
        months_available = int(panel["period"].nunique()) if "period" in panel.columns else len(panel.index)
        diag = self.diagnose_trends(panel)

        # Adaptive message (won’t break with 1–2 months)
        if months_available < 3:
            preface = f"Only {months_available} month(s) of data found for “{product_phrase}”. I’ll use short-term signals."
        else:
            preface = f"Analyzing {months_available} months of history for “{product_phrase}”."

        context = {
            "product": product_phrase,
            "horizon": horizon,
            "periods": panel.tail(12).to_dict(orient="records"),
            "diagnostics": diag,
            "note": preface,
        }

        return generate_openai_answer(
            user_query=f"Give actionable guidance to improve upcoming months for {product_phrase}",
            mode="advisor",
            analysis={"summary": preface, "insights": []},
            table_records=[context],
        )

