from __future__ import annotations
import os
import re
import logging
from openai import OpenAI 
from typing import Any, Dict, List, Optional, Tuple
import numpy as np
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
import time   # <-- ADD THIS LINE
# import app.routes.chatbot_routes as analyst
import json
from typing import Optional
from typing import Optional, List, Dict, Any, Tuple
from typing import Callable
from sqlalchemy import create_engine, text
import datetime as dt
import calendar
import math
import os, re, json
from pathlib import Path
from app.utils.formulas_utils import uk_sales, uk_tax, uk_credits, uk_amazon_fee, uk_profit,uk_platform_fee,uk_advertising

# ---------- env & setup ----------
load_dotenv()
SECRET_KEY = os.getenv("SECRET_KEY")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
DB_URL = os.getenv("DATABASE_URL", "postgresql://postgres:password@localhost:5432/phormula")
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


USE_SPACY = (os.getenv("USE_SPACY", "false").lower() in {"1","true","yes"})
_SPACY_NLP = None

def _ensure_spacy_nlp():
    global _SPACY_NLP
    if (not USE_SPACY) or _SPACY_NLP is not None:
        return _SPACY_NLP
    try:
        import spacy
        model_name = os.getenv("SPACY_MODEL", "en_core_web_sm")
        _SPACY_NLP = spacy.load(model_name)
        print(f"[DEBUG][spacy] Loaded spaCy model: {model_name}")
    except Exception as e:
        print(f"[DEBUG][spacy] Failed to load spaCy: {e}")
        _SPACY_NLP = None
    return _SPACY_NLP

# Default one-liner aliases that help the planner/metric resolution
_DEFAULT_METRIC_ALIASES = {
    # finance-y phrasings → your canonical metric keys used by FormulaEngine/Planner
    "ads spend": "advertising_total",
    "ad spend": "advertising_total",
    "advertisement spend": "advertising_total",
    "refund leakage": "credits",         # treat as refunds/credits
    "refunds leakage": "credits",
    "refunds": "credits",
    "fees": "selling_fees",
    "fba fee": "fba_fees",
    "fba fees": "fba_fees",
    "reimbursements": "reimbursement_fee",
    "acos": "acos",
    "cm2 profit": "cm2_profit",
    "cm2 margin": "cm2_margins",
    "cm1 profit": "cm1_profit",
    "cm1 margin": "cm1_margins",
    "contribution margin": "cm1_margins",
    "gross margin": "cm1_margins",
    # back-compat: if you don’t track CM2, map to CM1 transparently
    
}

_ALIASES_PATH = Path(os.getenv("CHATBOT_ALIASES_PATH", "data/learned_aliases.json"))


def _load_learned_aliases() -> dict:
    try:
        if _ALIASES_PATH.exists():
            return json.loads(_ALIASES_PATH.read_text() or "{}")
    except Exception:
        pass
    return {}

def _save_learned_aliases(data: dict) -> None:
    try:
        _ALIASES_PATH.parent.mkdir(parents=True, exist_ok=True)
        _ALIASES_PATH.write_text(json.dumps(data, ensure_ascii=False, indent=2))
    except Exception:
        pass

def learn_metric_alias(alias: str, canonical: str) -> None:
    """
    Persist a new synonym → canonical mapping so future queries work without code changes.
    """
    alias = (alias or "").strip().lower()
    canonical = (canonical or "").strip().lower()
    if not alias or not canonical:
        return
    data = _load_learned_aliases()
    data.setdefault("metric", {})[alias] = canonical
    _save_learned_aliases(data)

def _apply_aliases(text: str) -> str:
    learned = _load_learned_aliases().get("metric", {})
    # learned overrides defaults
    merged = dict(_DEFAULT_METRIC_ALIASES)
    merged.update(learned)
    # apply as whole-word replacements
    for a, c in merged.items():
        text = re.sub(rf"\b{re.escape(a)}\b", c, text, flags=re.I)
    return text

_COUNTRY_PAT = re.compile(
    r'\b(?:in|for|from|to|the)?\s*\b(?:us|usa|u\.s\.a\.|united states|uk|u\.k\.|united kingdom)\b',
    re.I
)

def parse_country_strict(text: str) -> Optional[str]:
    q = text or ""
    m = _COUNTRY_PAT.search(q)
    if not m:
        return None
    s = m.group(0).lower()
    if "uk" in s or "united kingdom" in s:
        return "UK"
    # Only accept US when context clearly means country (not pronoun “us”)
    if re.search(r'\bUS\b|\bUSA\b|united states|\bin the us\b|\bin us\b', q, re.I):
        return "US"
    return None



def normalize_user_query(text: str) -> str:
    """
    Robust canonical form for intent/planning:
    - trims, lowercases
    - spaCy lemmatizes & drops stopwords/punct if available
    - applies learned/default aliases
    """
    s = (text or "").strip()
    nlp = _ensure_spacy_nlp()
    if not nlp:
        return _apply_aliases(s.lower())
    doc = nlp(s)
    # keep dates/numbers as-is; lemmatize content words
    toks = []
    for t in doc:
        if t.is_space or t.is_punct:
            continue
        if t.is_stop and t.ent_type_ not in {"DATE"}:
            continue
        # keep raw token for dates; otherwise lemma
        val = t.text if t.ent_type_ in {"DATE"} else (t.lemma_ or t.text)
        toks.append(val.lower())
    return _apply_aliases(" ".join(toks))

CANONICAL_METRICS = {
    "sales","profit","tax","credits","net_credits","amazon_fees",
    "quantity_sold","asp","platform_fee","advertising_total",
    "profit_margin","cm1_profit","cm1_margins","acos",
    "reimbursement_fee","reimbursement_vs_sales","reimbursement_vs_cm1_margins",
}

def resolve_metric_from_text(text: str) -> str | None:
    """
    Heuristic resolver used ONLY when the planner leaves metric empty.
    Prefers alias hits and whole-word matches; no hard-coding of phrasing.
    """
    q = _apply_aliases((text or "").lower())
    # direct hits first
    for m in CANONICAL_METRICS:
        if re.search(rf"\b{re.escape(m)}\b", q):
            return m
    # soft fallbacks
    if "reimburse" in q: return "reimbursement_fee"
    if "acos" in q: return "acos"
    if "margin" in q: return "cm1_margins"
    return None

def resolve_product_entities(query: str, engine, user_id: int, country: str, top_k: int = 10) -> List[Dict[str, float]]:
    """
    Resolve user text into product entities.

    Improvements:
    • Exact-match short-circuit (case-insensitive).
    • Embedding similarity with margin threshold.
    • Fuzzy fallback via rapidfuzz.
    • Clarify only if no strong candidate.
    """
    phrases = []
    nlp = _ensure_spacy_nlp()
    if nlp:
        try:
            doc = nlp(query)
            for nc in doc.noun_chunks:
                t = nc.text.strip()
                if len(t) >= 2:
                    phrases.append(t)
        except Exception:
            pass
    phrases.append(query)

    tried = set()
    for p in phrases:
        if not p or p in tried:
            continue
        tried.add(p)
        cands = product_candidates(engine, user_id, country, p, limit=top_k)
        if cands:
            return cands

    names = []
    try:
        with engine.connect() as conn:
            tbl = f"user_{user_id}_{country.lower()}_merge_data_of_all_months"
            rs = conn.execute(text(f"""
                SELECT DISTINCT product_name
                FROM {tbl}
                WHERE product_name IS NOT NULL AND TRIM(product_name) <> ''
                LIMIT 50000
            """)).fetchall()
        names = [r[0] for r in rs]
    except Exception:
        pass
    if not names:
        return []

    qnorm = query.strip().lower()

    # --- Exact match
    for n in names:
        if n.strip().lower() == qnorm:
            return [{"product_name": n, "score": 1.0}]

    # --- Embedding similarity
    try:
        from sentence_transformers import SentenceTransformer
        model = SentenceTransformer(os.getenv("EMB_MODEL", "all-MiniLM-L6-v2"))
        qv = model.encode([query], normalize_embeddings=True)
        nv = model.encode(names, normalize_embeddings=True)
        sims = (nv @ qv.T).ravel()
    except Exception:
        from sklearn.feature_extraction.text import TfidfVectorizer
        from sklearn.metrics.pairwise import cosine_similarity
        vec = TfidfVectorizer(ngram_range=(1, 2), min_df=1)
        X = vec.fit_transform(names + [query])
        sims = cosine_similarity(X[-1], X[:-1]).ravel()

    top_idx = sims.argsort()[-top_k:][::-1]
    top_scores = [(names[i], float(sims[i])) for i in top_idx]

    if top_scores:
        best, best_score = top_scores[0]
        second_best = top_scores[1][1] if len(top_scores) > 1 else 0.0
        if best_score - second_best >= 0.10:
            return [{"product_name": best, "score": best_score}]

    # --- Fuzzy fallback
    try:
        from rapidfuzz import fuzz
        scores = [(n, fuzz.ratio(qnorm, n.lower()) / 100.0) for n in names]
        scores.sort(key=lambda x: x[1], reverse=True)
        if scores and scores[0][1] >= 0.90:
            return [{"product_name": scores[0][0], "score": scores[0][1]}]
    except Exception:
        pass

    return [{"product_name": n, "score": s} for n, s in top_scores]




##############################################################################################################################

# add this helper near the top of chatbot_utils.py (after logger = ...)
def _fallback_natural_response(analysis: dict, user_query: str) -> str:
    """Local, safe formatter to avoid circular imports on failure paths."""
    summary = (analysis or {}).get("summary") or "Here’s what I found."
    insights = (analysis or {}).get("insights") or []
    parts = [summary]
    if insights:
        parts.append("\nKey insights:")
        parts.extend([f"• {i}" for i in insights[:6]])
    return "\n".join(parts)



try:
    oa_client = OpenAI(api_key=OPENAI_API_KEY)
    logger.info("OpenAI client initialized")
except Exception:
    logger.exception("Failed to init OpenAI client")
    oa_client = None


# ---------- helpers ----------


_PLANNER_SYSTEM = (
    "You convert natural-language questions about Amazon Seller Central finance data "
    "into a JSON plan with fields: "
    "{operation, metric, time_range, country, product, group_by, top_k, sort_dir, filters, "
    "needs_clarification, clarification_message}. "
    "Operations: aggregate, rank, trend, breakdown, compare, lookup, explain, clarify. "
    "Important rules: "
    "1) Never require 'product' for simple totals. If the user asks for an overall metric "
    "   like sales, profit, tax, credits, amazon_fees, quantity_sold, asp, etc., and gives a "
    "   time range (and optional country), set needs_clarification=false. "
    "2) Only ask for 'product' when the user asks to group or rank by product/SKU/category "
    "   (e.g., group_by='product' or operation='rank' with product breakdown). "
    "3) time_range can be a string (e.g., 'June 2025', 'last 3 months') or a dict "
    "   {start:'YYYY-MM-DD', end:'YYYY-MM-DD'} if the user provided explicit dates. "
    "4) If the user provides country (UK/US), include it in 'country'. Otherwise leave null. "
    "5) 'filters' is an array of {field, op, value} for extra constraints; keep it minimal. "
    "6) Always return ONLY a JSON object. No prose."
)

def _planner_defaults(plan: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "operation": (plan.get("operation") or "aggregate").lower(),
        "metric": (plan.get("metric") or "").lower() or None,
        "time_range": plan.get("time_range") or None,  # {"start":"YYYY-MM-DD","end":"YYYY-MM-DD"}
        "country": plan.get("country") or None,
        "product": plan.get("product") or None,
        "group_by": plan.get("group_by") or None,
        "top_k": plan.get("top_k") or None,
        "sort_dir": (plan.get("sort_dir") or "desc").lower(),
        "filters": plan.get("filters") or [],
        "needs_clarification": bool(plan.get("needs_clarification", False)),
        "clarification_message": plan.get("clarification_message") or None,
    }

def plan_query(user_query: str) -> Dict[str, Any]:
    """
    Ask the planner LLM for a JSON plan, then normalize it.
    Guarantees:
    - Lowercases operation/metric/sort_dir
    - Never forces product for simple overall totals (sales/profit/…)
    - Adds safe defaults
    """
    simple_overall_metrics = {
        "sales", "profit", "tax", "credits", "net_credits",
        "amazon_fees", "quantity_sold", "asp",
        "platform_fee", "advertising_total",
        "profit_margin", "cm1_profit", "cm1_margins", "acos",
        "reimbursement_fee", "reimbursement_vs_sales", "reimbursement_vs_cm1_margins",
    }

    print("\n[DEBUG][planner] user_query:", repr(user_query))
    client = oa_client or OpenAI()

    raw_json = "{}"
    try:
        resp = client.chat.completions.create(
            model="gpt-4o",
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": _PLANNER_SYSTEM},
                {"role": "user", "content": user_query},
            ],
            temperature=0.1,
            max_tokens=300,
        )
        raw_json = (resp.choices[0].message.content or "{}").strip()
        print("[DEBUG][planner] raw_json:", raw_json[:500])
        data = json.loads(raw_json)
    except Exception as e:
        print("[DEBUG][planner][ERROR] exception:", repr(e))
        data = {
            "operation": "clarify",
            "needs_clarification": True,
            "clarification_message":
                "Tell me the metric (e.g., sales, profit, fees) and time range (e.g., last 30 days, Aug 2025).",
        }

    # -------- normalize / defaults --------
    plan = _planner_defaults(data)

    # Lowercase normalize
    if plan["operation"]:
        plan["operation"] = str(plan["operation"]).lower()
    if plan["metric"]:
        plan["metric"] = str(plan["metric"]).lower()
    if plan["sort_dir"]:
        plan["sort_dir"] = "asc" if str(plan["sort_dir"]).lower().startswith("a") else "desc"
    if plan["group_by"]:
        plan["group_by"] = str(plan["group_by"]).lower()

    # -------- NEW: try resolver if metric still missing --------
    if not plan.get("metric"):
        maybe = resolve_metric_from_text(user_query)
        if maybe:
            plan["metric"] = maybe
            print(f"[DEBUG][planner] resolved missing metric → {maybe}")

    # -------- un-block simple totals --------
    has_time = bool(plan.get("time_range"))
    is_simple_metric = plan.get("metric") in simple_overall_metrics
    is_overall_agg = plan.get("operation") in (None, "", "aggregate", "trend", "compare", "breakdown")

    wants_product_split = plan.get("group_by") in {"product", "sku"} or plan.get("operation") == "rank"

    if is_simple_metric and has_time and is_overall_agg and not wants_product_split:
        plan["needs_clarification"] = False
        plan["clarification_message"] = None

    print("[DEBUG][planner] normalized plan:", plan)
    return plan



    
####################################################################################################################################################

def _last_full_month_today(now: dt.date | None = None) -> tuple[int, int]:
    """Return (year, month) for the last full calendar month (server local time)."""
    now = now or dt.date.today()
    first_this = now.replace(day=1)
    last_prev = first_this - dt.timedelta(days=1)
    return last_prev.year, last_prev.month

def _ym_to_span(y: int, m: int) -> dict:
    last_day = calendar.monthrange(y, m)[1]
    return {"start": f"{y:04d}-{m:02d}-01", "end": f"{y:04d}-{m:02d}-{last_day:02d}"}


def _to_compact_table_preview(rows: List[dict], max_rows: int = 100) -> List[dict]:
    """
    Trim large tables before sending to the LLM. Keeps only a small sample.
    """
    if not rows:
        return []
    return rows[:max_rows]

def _render_prompt_for_llm(*, user_query: str, mode: str,
                           analysis: dict | None,
                           table_records: List[dict] | None) -> tuple[str, str]:
    """
    Build (system, user) messages. We keep the system prompt stable and
    present your computed results as *facts* for the model to write from.
    """
    system_msg = (
        "You are a precise analytics explainer for Amazon marketplace data. "
        "Only use the facts provided. If something is unknown, say so briefly. "
        "Prefer crisp bullet points and a short headline. Numbers should use "
        "two decimals for currency, and thousands separators."
    )

    # Compact payload for the model
    parts: list[str] = []
    parts.append(f"User query: {user_query}")

    if analysis:
        parts.append("\n[Computed analysis]")
        parts.append(f"summary: {analysis.get('summary','')}")
        ins = analysis.get("insights") or []
        if ins:
            parts.append("insights:")
            for i in ins[:20]:  # bound size
                parts.append(f"- {i}")

    has_breakdown = False
    if table_records:
        # Detect if any product/SKU rows exist
        has_breakdown = any(
            (str(r.get("scope") or "").lower() in {"product", "sku"})
            for r in table_records
        )
        parts.append("\n[Table records sample]")
        preview = _to_compact_table_preview(table_records, max_rows=120)
        parts.append(f"(showing up to {len(preview)} rows)")
        import json
        parts.append(json.dumps(preview)[:120000])  # hard cap ~120KB

    parts.append(f"\n[Mode] {mode}")

    # Nudge style based on mode
    if mode == "sql_formula":
        if has_breakdown:
            parts.append(
                "When a 'total' row exists, lead with that number, then add a brief SKU/product breakdown."
            )
        else:
            parts.append(
                "When only overall rows are present, write a concise summary. "
                "Do not mention SKU/product breakdown."
            )
    elif mode == "sql_special":
        parts.append("Return exactly the special sentence calculated, with minimal polish.")
    else:
        parts.append("Write a concise answer with a headline and 3-6 bullets. If helpful, add a 1-line tip at the end.")

    user_msg = "\n".join(parts)

    # ---- DEBUG PRINT ----
    try:
        print(
            f"[DEBUG][llm-prompt] mode={mode} | "
            f"analysis={'y' if analysis else 'n'} | "
            f"table_records={len(table_records) if table_records else 0} | "
            f"user_msg_len={len(user_msg)}"
        )
    except Exception:
        pass

    return system_msg, user_msg




def _fmt_period_label(analysis: Optional[dict]) -> str:
    """
    Build a human-friendly period label like 'August 2025 vs July 2025'
    if period info exists in analysis. Otherwise return ''.
    """
    if not analysis:
        return ""
    curr = analysis.get("period", {}).get("current")
    prev = analysis.get("period", {}).get("previous")
    def _fmt(d):
        # Accept 'YYYY-MM' or date-like strings and prettify; else return as-is.
        if not d:
            return ""
        try:
            if len(d) == 7:  # 'YYYY-MM'
                dt = datetime.strptime(d, "%Y-%m")
                return dt.strftime("%B %Y")
            # Try full date
            dt = datetime.fromisoformat(d)
            return dt.strftime("%B %d, %Y")
        except Exception:
            return str(d)
    curr_s = _fmt(curr)
    prev_s = _fmt(prev)
    if curr_s and prev_s:
        return f"{curr_s} vs {prev_s}"
    return curr_s or prev_s or ""

def _normalize_plan_for_sku_language(plan: dict, query: str) -> dict:
    """Normalize plan so that explicit SKU mentions are turned into filters,
    and sanitize planner outputs (e.g., product=True → group_by=product).
    Also rewrites 'product' filters to 'product_name' for DB compatibility."""

    ql = (query or "").lower()

    # --- Ensure filters is a list -------------------------------------------
    if not isinstance(plan.get("filters"), list):
        plan["filters"] = []

    # --- Defensive normalization for 'product' ------------------------------
    raw_prod = plan.get("product")

    if isinstance(raw_prod, bool):
        if raw_prod:
            if not (plan.get("group_by") or "").strip():
                plan["group_by"] = "product"
        plan["product"] = None
    elif raw_prod is not None and not isinstance(raw_prod, str):
        plan["product"] = None

    # --- Rewrite planner filters: product → product_name --------------------
    new_filters = []
    for f in plan["filters"]:
        if str(f.get("field", "")).lower() == "product":
            f = dict(f)  # copy to avoid mutating original
            f["field"] = "product_name"
            print(f"[DEBUG] Rewriting filter field 'product' → 'product_name' (val={f.get('value')})")
        new_filters.append(f)
    plan["filters"] = new_filters

    # --- Explicit SKU capture (e.g. "SKU BV-6X5T-6CY1") --------------------
    
    m = re.search(r"\bsku\s*[:#]?\s*([A-Za-z0-9][A-Za-z0-9\-]{2,})\b", ql, re.I)
    if m:
        cand = m.group(1).strip().lower()

        # connector/stop words that must NOT become SKUs
        STOP = {"and", "or", "vs", "v", "the", "a", "an", "in", "on", "for", "of", "to", "by", "with"}

        # “looks like a real SKU”: contains a dash OR is reasonably long
        looks_like_sku = ("-" in cand) or (len(cand) >= 5)

        if cand not in STOP and looks_like_sku:
            plan["filters"].append({"field": "sku", "op": "=", "value": cand})
            plan["product"] = None
            plan["group_by"] = "sku"
            print(f"[DEBUG] normalized SKU from query → {cand}")
        else:
            print(f"[DEBUG] ignore 'sku' token after keyword → {cand}")
    elif "sku" in ql:
        # if they just said “sku …” but didn’t provide a value, default to grouping
        if not (plan.get("group_by") or "").strip():
            plan["group_by"] = "sku"


    # --- Clean generic tokens mistakenly put into 'product' -----------------
    prod = (plan.get("product") or "").strip().lower()
    if prod in {"sku", "product", "products", "product name"}:
        plan["product"] = None

    # --- Ranking language without op set → force rank -----------------------
    if any(w in ql for w in ["top", "highest", "best", "lowest", "bottom"]) \
       and not (plan.get("operation") or "").strip():
        plan["operation"] = "rank"

    return plan




def _build_format_guidance(user_query: str, analysis: Optional[dict], has_table: bool) -> str:
    """
    Craft a small, deterministic formatting guide that adapts
    to what data we have. The LLM will follow this to produce consistent answers.
    """
    currency = (analysis or {}).get("currency", "USD")
    unit_hint = (analysis or {}).get("unit_hint", "")  # e.g. "Units", "Orders"
    period_label = _fmt_period_label(analysis)

    # Flags: do we have MoM/YoY, totals, product breakdowns?
    metrics = (analysis or {}).get("metrics", {})
    has_mom = bool(metrics.get("mom_pct") is not None or metrics.get("mom_abs") is not None)
    has_yoy = bool(metrics.get("yoy_pct") is not None or metrics.get("yoy_abs") is not None)
    has_totals = any(k in metrics for k in ("revenue", "net_sales", "units", "orders", "profit"))
    has_breakdown = bool((analysis or {}).get("breakdown", {})) or has_table

    bullets_rule = "3–6 bullets max. Keep each bullet to a single sentence."

    guidance = [
        "Write Markdown.",
        "Structure:",
        "1) Title: 1 line, crisp.",
        f"2) KPI strip (one line): include key totals (currency {currency} where relevant) and any MoM/YoY deltas with ▲/▼ and ± signs.",
    ]
    if period_label:
        guidance.append(f"   Include period: **{period_label}**.")
    guidance.append("3) What changed: bullet points with drivers (price, volume, mix, promo, ads).")
    if has_breakdown:
        guidance.append("4) Breakdown: top 3–5 products or categories with figure and delta.")
    if has_table:
        guidance.append("   If you show a table, keep it ≤ 5 rows and 3–5 columns.")
    guidance.append("5) One-line tip: a short, practical recommendation.")
    guidance.append("")
    guidance.append("Formatting rules:")
    guidance.append("- Currency: two decimals and thousands separators (e.g., $12,345.67).")
    guidance.append("- Percentages: one decimal if <10%, otherwise no more than one decimal; include ±.")
    guidance.append("- Use ▲ for increases and ▼ for decreases next to percentages.")
    if unit_hint:
        guidance.append(f"- Use '{unit_hint}' when referring to counts.")
    guidance.append(f"- Avoid jargon. {bullets_rule}")

    # Optional examples (kept short so we don't bloat tokens)
    examples = []
    if has_totals:
        examples.append(
            "KPI: Net Sales **$1,234,567.89** (MoM ▲ +6.4%, +$74,321) • Units **98,765** (MoM ▼ −2.1%)"
        )
    if has_mom:
        examples.append("MoM = (Current − Previous) / Previous × 100%. Show both % and absolute delta.")
    if has_yoy:
        examples.append("YoY = compare to same period last year. Show % and absolute delta if available.")

    if examples:
        guidance.append("\nExamples (style):\n- " + "\n- ".join(examples))

    return "\n".join(guidance)



def generate_openai_answer(*, user_query: str, mode: str,
                           analysis: dict | None = None,
                           table_records: List[dict] | None = None) -> str:
    """
    Calls OpenAI to turn your computed data into a polished response.
    Falls back to a local formatter if anything goes wrong (no circular import).
    """
    # Local fallback renderer payload
    _analysis = analysis or {"summary": "", "insights": []}

    # If OpenAI client isn't available, fall back immediately
    if oa_client is None:
        logger.warning("OpenAI client not initialized; falling back locally.")
        return _fallback_natural_response(_analysis, user_query)

    # Build messages for the LLM
    system_msg, user_msg = _render_prompt_for_llm(
        user_query=user_query, mode=mode,
        analysis=analysis, table_records=table_records
    )

    try:
        resp = oa_client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": system_msg},
                {"role": "user", "content": user_msg},
            ],
            temperature=0.2,
            max_tokens=600,
        )
        text = (resp.choices[0].message.content or "").strip()
        print(f"[DEBUG][llm-out] chars={len(text)}")
        return text
    except Exception:
        logger.exception("OpenAI call failed; using fallback renderer.")
        return _fallback_natural_response(_analysis, user_query)

def infer_group_by(query: str, plan: dict) -> dict:
    """
    Heuristic: if user query says 'per product' or 'per sku',
    set plan['group_by'] accordingly unless already set.
    """
    ql = (query or "").lower()
    gb = (plan.get("group_by") or "").lower()

    if gb:  # don’t override if explicitly set
        return plan

    if re.search(r"\bper\s+sku\b", ql) or re.search(r"\bby\s+sku\b", ql):
        plan["group_by"] = "sku"
        print("[DEBUG] inferred group_by=sku from query")

    elif re.search(r"\bper\s+product\b", ql) or re.search(r"\bby\s+product\b", ql):
        plan["group_by"] = "product"
        print("[DEBUG] inferred group_by=product from query")

    return plan

# Detect common ways users say "all products"
_ALL_PROD_RE = re.compile(
    r"\b(?:all\s+(?:the\s+)?)?products?\b|(?:every\s+product)\b|(?:overall\s+products?)\b",
    re.I
)

def _is_all_products_phrase(s: str) -> bool:
    return bool(_ALL_PROD_RE.search(s or ""))

# ---------- formula engine ----------
class FormulaEngine:
    

    # ---------- public API ----------
    def __init__(self):
        self.registry = {
            "sales": self._sales,
            "tax": self._tax,
            "credits": self._credits,         # (aka net_credits)
            "profit": self._profit,
            "reimbursement_fee": self._reimbursement_fee,
            "platform_fee": self._platform_fee,
            "advertising_total": self._advertising_total,
            "reimbursement_vs_sales": self._reimbursement_vs_sales,
            "cm2_profit": self._cm2_profit,
            "cm2_margins": self._cm2_margins,
            "acos": self._acos,
            "roas": self._acos,
            "reimbursement_vs_cm2_margins": self._reimbursement_vs_cm2_margins,
            "profit_margin": self._profit_margin,
            "asp": self._asp,
            "unit_profitability": self._unit_profitability,
            "sales_mix": self._sales_mix,
            "profit_mix": self._profit_mix,
            "quantity_sold": self._quantity_sold,
            "amazon_fees": self._amazon_fees,
            "net_credits": self._credits,     # alias to credits
            "refunds": self._refunds, 
            "fba_fees": self._fba_fees,
            "selling_fees": self._selling_fees,
        }
        self.aliases = {
            "total sales": "sales", "net sales": "sales",
            "taxes": "tax", "total tax": "tax",
            "credit": "credits", "credits total": "credits",
            "net credits": "net_credits", "net credit": "net_credits",
            "reimbursement": "reimbursement_fee", "reimbursements": "reimbursement_fee",
            "platform": "platform_fee", "subscription fee": "platform_fee",
            "ads": "advertising_total", "ad cost": "advertising_total", "advertising": "advertising_total",
            "reimb vs sales": "reimbursement_vs_sales", "reimbursement vs sales": "reimbursement_vs_sales",
            "cm2": "cm2_profit", "cm2 profit": "cm2_profit","net profit": "cm2_profit", "cm2 margin": "cm2_margins",
            "acos": "acos", "ad cos": "acos", "roas": "acos",
            "reimb vs cm2": "reimbursement_vs_cm2_margins", "reimbursement vs cm2": "reimbursement_vs_cm2_margins",
            "margin": "profit_margin", "profit %": "profit_margin",
            "average selling price": "asp", "avg selling price": "asp",
            "profit per unit": "unit_profitability", "ppu": "unit_profitability",
            "sales share": "sales_mix", "profit share": "profit_mix",
            "quantity sold": "quantity_sold", "qty sold": "quantity_sold",
            "units sold": "quantity_sold", "sold units": "quantity_sold",
            "total units": "quantity_sold", "ordered units": "quantity_sold","orders": "quantity_sold",
            "amazon fee": "amazon_fees", "amazon fees": "amazon_fees", "amazon fees total": "amazon_fees",
            "ads spend": "advertising_total", "ad spend": "advertising_total", "ads_spend": "advertising_total",
            "refund": "refunds", "refund count": "refunds", "returns": "refunds",
            "fba": "fba_fees", "fulfillment fee": "fba_fees","fulfilment fee": "fba_fees","fulfillment fees": "fba_fees","fulfilment fees": "fba_fees",
            "selling fee": "selling_fees", "selling fees": "selling_fees","referral fee": "selling_fees","referral fees": "selling_fees",
        }

        # --- Merge learned aliases from disk (safe, optional) -----------------
        try:
            # If this class is in the same module as _load_learned_aliases, just call it.
            # Otherwise, fall back to reading the JSON path directly.
            learned_map = {}
            try:
                learned_all = _load_learned_aliases()  # defined earlier in chatbot_utils.py
                learned_map = (learned_all or {}).get("metric", {}) if learned_all else {}
            except NameError:
                import os, json
                from pathlib import Path
                p = Path(os.getenv("CHATBOT_ALIASES_PATH", "data/learned_aliases.json"))
                if p.exists():
                    learned_map = (json.loads(p.read_text() or "{}") or {}).get("metric", {}) or {}

            # normalize keys/values to lowercase, strip spaces
            learned_map = {
                str(k).strip().lower(): str(v).strip().lower()
                for k, v in learned_map.items()
                if str(k).strip() and str(v).strip()
            }

            if learned_map:
                self.aliases.update(learned_map)
                print(f"[DEBUG][formula] merged {len(learned_map)} learned alias(es) into FormulaEngine")
        except Exception as _e:
            print("[DEBUG][formula] learned alias merge failed:", _e)
#############################################################################################
    def _ctx_trace(self, ctx: dict) -> str:
        """Compact trace string for debugging context propagation."""
        if not isinstance(ctx, dict):
            return "<no-ctx>"
        keys = [
            "metric", "country", "group_by", "want_breakdown",
            "product", "sku", "target_product", "target_sku", "raw_query"
        ]
        flat = {k: ctx.get(k) for k in keys}
        return ", ".join(f"{k}={flat[k]!r}" for k in keys)
#############################################################        
        
    def _sr(self, x: float, nd: int = 2) -> float:
        """Safe round: coerce to float, replace non-finite with 0.0, round."""
        try:
            v = float(x)
        except Exception:
            return 0.0
        if not math.isfinite(v):
            return 0.0
        return round(v, nd)

    def _sanitize_df(self, df: pd.DataFrame, nd: int = 2) -> pd.DataFrame:
        """Make every numeric col finite and rounded; leave text cols unchanged."""
        if df is None or df.empty:
            return df if df is not None else pd.DataFrame()
        out = df.copy()
        num_cols = out.select_dtypes(include=["number", "float", "int"]).columns.tolist()
        for c in num_cols:
            out[c] = pd.to_numeric(out[c], errors="coerce")
            out[c] = out[c].replace([np.inf, -np.inf], np.nan).fillna(0.0)
            out[c] = out[c].round(nd)
        return out

    def _month_to_int(self, v: str | int) -> int | None:
        """Normalize month column values to 1..12; supports 'June', 'Jun', '6', '06'."""
        if v is None:
            return None
        s = str(v).strip().lower()
        if s.isdigit():
            try:
                n = int(s)
                return n if 1 <= n <= 12 else None
            except Exception:
                return None
        # names
        names = {
            "jan":1,"january":1,
            "feb":2,"february":2,
            "mar":3,"march":3,
            "apr":4,"april":4,
            "may":5,
            "jun":6,"june":6,
            "jul":7,"july":7,
            "aug":8,"august":8,
            "sep":9,"sept":9,"september":9,
            "oct":10,"october":10,
            "nov":11,"november":11,
            "dec":12,"december":12,
        }
        return names.get(s)

    def _year_int(self, v: str | int) -> int | None:
        try:
            n = int(str(v).strip())
            return n
        except Exception:
            return None

    def _compare_metric(self, df: pd.DataFrame, ctx: Dict[str, Any]) -> Dict[str, Any]:
        """
        Generic comparer: take the planner's single time_range that spans two (possibly non-consecutive) months,
        compute the chosen metric for the earliest month in-range vs the latest month in-range, then return:
        result = % change (latest vs earliest)
        table: month1 value, month2 value, CHANGE, %CHANGE
        Works for ANY metric in self.registry (sales, profit, tax, credits, etc.).
        """

        metric_name = (ctx.get("metric") or "sales").strip().lower()
        metric_name = self.aliases.get(metric_name, metric_name)

        if metric_name not in self.registry:
            return {
                "result": 0.0,
                "explanation": f"Unknown metric '{metric_name}' for compare.",
                "table_df": self._total_only_table("compare", 0.0),
            }

        if df is None or df.empty:
            return {
                "result": 0.0,
                "explanation": "No rows in selected range.",
                "table_df": self._total_only_table("compare", 0.0)
            }

        # --- Derive months ---
        y = df["year"].apply(self._year_int) if "year" in df.columns else pd.Series([None]*len(df), index=df.index)
        if "month" in df.columns:
            m = df["month"].apply(self._month_to_int)
        else:
            return {
                "result": 0.0,
                "explanation": "No 'month' column to split periods.",
                "table_df": self._total_only_table("compare", 0.0)
            }

        ym = pd.DataFrame({"y": y, "m": m}).dropna()
        if ym.empty:
            return {
                "result": 0.0,
                "explanation": "No valid year/month values.",
                "table_df": self._total_only_table("compare", 0.0)
            }

        uniq = ym.drop_duplicates().sort_values(["y", "m"]).values.tolist()
        print(f"[TRACE][FE][compare] metric={metric_name} rows={len(df)} ctx=({self._ctx_trace(ctx)})")
        print(f"[TRACE][FE][compare] uniq_months={uniq}")

        if len(uniq) >= 1:
            (y1, m1) = uniq[0]
            (y2, m2) = uniq[-1]
            print(f"[TRACE][FE][compare] earliest=({int(y1)}, {int(m1)}) latest=({int(y2)}, {int(m2)})")

        if len(uniq) == 1:
            (y1, m1) = uniq[0]
            return {
                "result": 0.0,
                "explanation": f"Only one month present ({int(y1)}-{int(m1):02d}); need two months to compare.",
                "table_df": self._total_only_table("compare", 0.0),
            }

        (y1, m1) = uniq[0]
        (y2, m2) = uniq[-1]

        def _mask_month(df_in, yy, mm):
            ycol = df_in["year"].apply(self._year_int) if "year" in df_in.columns else pd.Series([None]*len(df_in), index=df_in.index)
            mcol = df_in["month"].apply(self._month_to_int) if "month" in df_in.columns else pd.Series([None]*len(df_in), index=df_in.index)
            return (ycol == int(yy)) & (mcol == int(mm))

        df_1 = df.loc[_mask_month(df, y1, m1)]
        df_2 = df.loc[_mask_month(df, y2, m2)]

        # ✅ FIX: Preserve and forward full target context
        base_ctx = {"country": ctx.get("country"), "want_breakdown": False}
        for key in ("target_product", "target_sku", "product", "sku"):
            if ctx.get(key):
                base_ctx[key] = ctx[key]

        print(f"[TRACE][FE][compare] evaluating metric='{metric_name}' for {y1}-{m1} and {y2}-{m2}")
        print(f"[TRACE][FE][compare] forwarded context: product={base_ctx.get('product')} "
            f"target_product={base_ctx.get('target_product')} target_sku={base_ctx.get('target_sku')}")

        val1 = float(self.registry[metric_name](df_1, base_ctx).get("result") or 0.0)
        val2 = float(self.registry[metric_name](df_2, base_ctx).get("result") or 0.0)

        print(f"[TRACE][FE][compare] results: {y1}-{m1}={val1:.2f}, {y2}-{m2}={val2:.2f}")

        change = val2 - val1
        pct = ((change / val1) * 100.0) if val1 else (0.0 if (val1 == 0 and val2 == 0) else float("inf"))
        print(f"[TRACE][FE][compare] Δ={change:.2f}  %Δ={pct:.2f}%")

        import calendar
        def label(yy, mm): return f"{calendar.month_name[int(mm)]} {int(yy)}"

        rows = [
            {"metric": f"{metric_name}_compare", "level": "period",  "key": label(y1, m1), "result": self._sr(val1)},
            {"metric": f"{metric_name}_compare", "level": "period",  "key": label(y2, m2), "result": self._sr(val2)},
            {"metric": f"{metric_name}_compare", "level": "derived", "key": "CHANGE",      "result": self._sr(change)},
            {"metric": f"{metric_name}_compare", "level": "derived", "key": "%CHANGE",     "result": (self._sr(pct) if math.isfinite(pct) else None)},
        ]
        table = self._sanitize_df(pd.DataFrame(rows))

        expl = f"Compare ({metric_name}): latest ({label(y2,m2)}) − earliest ({label(y1,m1)}); %change = CHANGE / earliest × 100."
        return {"result": self._sr(pct) if math.isfinite(pct) else 0.0, "explanation": expl, "table_df": table}

    



    def resolve_name(self, raw_query: str) -> Optional[str]:
        ql = (raw_query or "").lower()
        for alias, canon in self.aliases.items():
            if alias in ql:
                print(f"[DEBUG][formula] resolve_name alias→ {canon}")
                return canon
        for name in self.registry.keys():
            if name in ql or name.replace("_", " ") in ql:
                print(f"[DEBUG][formula] resolve_name direct→ {name}")
                return name
        print("[DEBUG][formula] resolve_name none")
        return None


    # ---------- region & SKU helpers ----------
    def _is_us(self, ctx: Dict[str, Any], df: pd.DataFrame) -> bool:
        c = (ctx or {}).get("country")
        if not c and "country" in df.columns and not df["country"].dropna().empty:
            c = str(df["country"].dropna().iloc[0])
        c = str(c or "").strip().upper()
        return c in ("US", "USA", "UNITED STATES")

    @staticmethod
    def _norm_sku_series(s: pd.Series) -> pd.Series:
        return s.astype(str).str.strip().str.lower()

    def _sku_mask(self, df: pd.DataFrame) -> pd.Series:
        """
        Stricter SKU presence for all SKU-based ops (UK & US):
          drop NaN / "", "0", "none", "null", "nan" (case-insensitive)
        """
        if "sku" not in df.columns or df.empty:
            return pd.Series([False] * len(df), index=df.index)
        norm = self._norm_sku_series(df["sku"])
        bad = norm.eq("") | norm.eq("0") | norm.eq("none") | norm.eq("null") | norm.eq("nan")
        # if original had NaN, norm is the string "nan"; covered above
        print(f"[TRACE][FE][sku] valid={int((~bad).sum())} invalid={int(bad.sum())}")
        return ~bad

    
    def _filter_valid_sku(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Keep only rows with a valid SKU.
        Matches process_skuwise_data / centralized sku_mask:
        drop: NaN, "", "0", "none", "null", "nan" (case-insensitive)
        """
        if df is None or df.empty or "sku" not in df.columns:
            return df
        return df.loc[self._sku_mask(df)].copy()


    def _df_for_region(self, df, ctx):
        """
        Region-specific DataFrame preprocessing.
        - UK: apply the same strict sku_mask as centralized helpers.
        - US: unchanged (also uses strict sku_mask).
        """
        if df.empty:
            return df

        w = df
        if "sku" in w.columns:
            w = w.loc[self._sku_mask(w)].copy()

        print(f"[TRACE][FE][region] {ctx.get('country','?')} scope rows={len(df)}→{len(w)}")
        return w



    # ---------- general helpers ----------
    @staticmethod
    def _safe_num(s: pd.Series) -> pd.Series:
        return pd.to_numeric(s, errors="coerce").fillna(0.0)

    def _col(self, df: pd.DataFrame, name: str) -> pd.Series:
        if df.empty:
            return pd.Series([], dtype=float)
        return self._safe_num(df[name]) if name in df.columns else pd.Series([0.0] * len(df), index=df.index)

    
    def _agg_by(self, df: pd.DataFrame, by_col: str, cols: List[str]) -> pd.DataFrame:
        """
        Group by `by_col` and sum `cols`.

        - Always restrict to rows with a present/non-empty SKU when a `sku` column exists.
        - If by_col == 'sku', also apply the stricter SKU mask via `self._sku_mask`
        (so BOTH UK and US ignore bad SKUs).
        """
        import pandas as pd

        if df.empty or by_col not in df.columns:
            return pd.DataFrame(columns=[by_col] + cols)

        present = [c for c in cols if c in df.columns]
        if not present:
            return pd.DataFrame(columns=[by_col] + cols)

        # --- NEW: keep only rows that actually have a SKU (if column exists) ---
        if "sku" in df.columns:
            df = df.loc[df["sku"].astype(str).str.strip().ne("") & df["sku"].notna()].copy()

        # After filtering, if nothing remains, bail out early
        if df.empty:
            return pd.DataFrame(columns=[by_col] + cols)

        # Build working frame; include `sku` if present (needed below for the strict mask)
        need_cols = [by_col] + present + (["sku"] if "sku" in df.columns and by_col != "sku" else [])
        tmp = df[need_cols].copy()

        # If grouping by SKU, also apply the strict SKU mask the code already had
        if by_col == "sku":
            m = self._sku_mask(tmp.rename(columns={by_col: "sku"}))
            tmp = tmp.loc[m]

        if tmp.empty:
            return pd.DataFrame(columns=[by_col] + cols)

        # Ensure numeric before sum
        for c in present:
            tmp[c] = pd.to_numeric(tmp[c], errors="coerce").fillna(0.0)

        # Drop rows where the group key itself is NA to avoid a NaN group
        tmp = tmp.dropna(subset=[by_col])

        # Final groupby/sum
        out = tmp.groupby(by_col, dropna=True)[present].sum().reset_index()

        # Return only requested columns (exclude the helper 'sku' column if we added it)
        return out


    def _sku_to_product(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty or "sku" not in df.columns or "product_name" not in df.columns:
            return pd.DataFrame(columns=["sku", "product_name"])
        tmp = df[["sku", "product_name"]].dropna()
        m = self._sku_mask(tmp)
        tmp = tmp.loc[m]
        tmp = tmp.astype(str)
        if tmp.empty:
            return pd.DataFrame(columns=["sku", "product_name"])
        m = (
            tmp.groupby(["sku", "product_name"]).size()
               .reset_index(name="cnt")
               .sort_values(["sku", "cnt"], ascending=[True, False])
        )
        m = m.loc[m.groupby("sku")["cnt"].idxmax(), ["sku", "product_name"]]
        return m.reset_index(drop=True)

    def _final_table(self, metric_name: str, total_value: float,
                 per_sku: pd.DataFrame, per_product: pd.DataFrame,
                 component_cols: List[str]) -> pd.DataFrame:
        rows = [{
            "metric": metric_name, "level": "total", "key": "TOTAL",
            "result": self._sr(total_value)
        }]

        if per_sku is not None and not per_sku.empty:
            for _, r in per_sku.iterrows():
                key = str(r.get("sku") or r.get("product_name") or r.get("label") or "UNKNOWN")
                row = {
                    "metric": metric_name, "level": "sku", "key": key,
                    "result": self._sr(r.get("__metric__", 0.0)),
                }
                for c in component_cols:
                    row[f"component_{c}"] = self._sr(r.get(c, 0.0))
                rows.append(row)

        if per_product is not None and not per_product.empty:
            for _, r in per_product.iterrows():
                key = str(r.get("product_name") or r.get("sku") or r.get("label") or "UNKNOWN")
                row = {
                    "metric": metric_name, "level": "product", "key": key,
                    "result": self._sr(r.get("__metric__", 0.0)),
                }
                for c in component_cols:
                    row[f"component_{c}"] = self._sr(r.get(c, 0.0))
                rows.append(row)

        return self._sanitize_df(pd.DataFrame(rows))



    def _total_only_table(self, metric_name: str, total_value: float) -> pd.DataFrame:
        df = pd.DataFrame([{
            "metric": metric_name, "level": "total", "key": "TOTAL",
            "result": self._sr(total_value)
        }])
        return self._sanitize_df(df)


    def _table_from_group_metric(
        self, *, metric_name: str, total_value: float,
        per_sku_df: pd.DataFrame, sku_metric_col: str,
        component_cols: List[str] | None = None
    ) -> pd.DataFrame:
        component_cols = component_cols or []
        if per_sku_df is None or per_sku_df.empty:
            return self._total_only_table(metric_name, total_value)

        sku2prod = self._sku_to_product(per_sku_df) if "sku" in per_sku_df.columns else pd.DataFrame(columns=["sku","product_name"])
        per_prod = per_sku_df.merge(sku2prod, on="sku", how="left")
        if "product_name" in per_prod.columns and not per_prod.empty:
            per_prod = per_prod.groupby(["product_name"], dropna=True)[[sku_metric_col] + component_cols].sum().reset_index()
            per_prod = per_prod.rename(columns={sku_metric_col: "__metric__"})
        else:
            per_prod = pd.DataFrame(columns=["product_name","__metric__"] + component_cols)

        per_sku = per_sku_df.copy()
        if "__metric__" not in per_sku.columns and sku_metric_col in per_sku.columns:
            per_sku["__metric__"] = per_sku[sku_metric_col]

        return self._final_table(
            metric_name, total_value,
            per_sku[["sku","__metric__"] + component_cols] if "sku" in per_sku.columns else pd.DataFrame(),
            per_prod[["product_name","__metric__"] + component_cols] if not per_prod.empty else pd.DataFrame(),
            component_cols
        )

    def _uk_unpack(self, res):
        """
        Accepts return shapes from formula_utils:
        - (total,)
        - (total, per_sku)
        - (total, per_sku, per_product)
        - (total, per_sku, per_product, components)
        Returns: total, per_sku_df, per_product_df, components_list
        """
        import pandas as pd
        total = 0.0
        per_sku = pd.DataFrame()
        per_prod = pd.DataFrame()
        comps = []
        if isinstance(res, tuple):
            if len(res) >= 1: total = float(res[0] or 0.0)
            if len(res) >= 2 and hasattr(res[1], "columns"): per_sku = res[1]
            if len(res) >= 3 and hasattr(res[2], "columns"): per_prod = res[2]
            if len(res) >= 4 and isinstance(res[3], (list, tuple)): comps = list(res[3])
        else:
            total = float(res or 0.0)
        return total, per_sku, per_prod, comps


    # ---------- country-aware components ----------
    def _sales_components(self, df: pd.DataFrame, ctx: Dict[str, Any]) -> Tuple[float, pd.DataFrame, pd.DataFrame, List[str]]:
        """
        US ONLY helper for sales components used by local (non-centralized) paths.
        UK is centralized elsewhere → return empty payload here.
        """
        if not self._is_us(ctx, df):
            return 0.0, pd.DataFrame(), pd.DataFrame(), []

        df2 = self._df_for_region(df, ctx)
        if df2.empty:
            return 0.0, pd.DataFrame(), pd.DataFrame(), []

        parts = ["product_sales", "promotional_rebates"]
        total = sum(float(self._col(df2, c).sum()) for c in parts)

        per_sku = self._agg_by(df2, "sku", parts)
        per_sku["__metric__"] = per_sku[parts].sum(axis=1)

        sku2prod = self._sku_to_product(df2)
        per_prod = per_sku.merge(sku2prod, on="sku", how="left")
        if "product_name" in per_prod.columns and not per_prod.empty:
            per_prod = per_prod.groupby("product_name", dropna=True)[parts + ["__metric__"]].sum().reset_index()
        else:
            per_prod = pd.DataFrame(columns=["product_name","__metric__"] + parts)

        return float(total), per_sku, per_prod, parts


    def _tax_components(self, df: pd.DataFrame, ctx: Dict[str, Any]) -> Tuple[float, pd.DataFrame, pd.DataFrame, List[str]]:
        """
        US ONLY helper for tax components.
        UK is centralized elsewhere → return empty payload here.
        """
        if not self._is_us(ctx, df):
            return 0.0, pd.DataFrame(), pd.DataFrame(), []

        df2 = self._df_for_region(df, ctx)
        if df2.empty:
            return 0.0, pd.DataFrame(), pd.DataFrame(), []

        parts = ["product_sales_tax","marketplace_facilitator_tax","shipping_credits_tax","giftwrap_credits_tax","promotional_rebates_tax"]
        subtotal = sum(float(self._col(df2, c).sum()) for c in parts)

        otf_by_sku = self._agg_by(df2, "sku", ["other_transaction_fees"])
        otf_total = float(otf_by_sku["other_transaction_fees"].sum()) if not otf_by_sku.empty else 0.0
        total = subtotal + otf_total

        per_sku_parts = self._agg_by(df2, "sku", parts)
        per_sku = per_sku_parts.merge(otf_by_sku, on="sku", how="outer").fillna(0.0)
        per_sku["__metric__"] = per_sku[parts].sum(axis=1) + per_sku.get("other_transaction_fees", 0.0)

        sku2prod = self._sku_to_product(df2)
        per_prod = per_sku.merge(sku2prod, on="sku", how="left")
        if "product_name" in per_prod.columns and not per_prod.empty:
            per_prod = per_prod.groupby("product_name", dropna=True)[parts + ["other_transaction_fees","__metric__"]].sum().reset_index()
        else:
            per_prod = pd.DataFrame(columns=["product_name","__metric__"] + parts + ["other_transaction_fees"])

        return float(total), per_sku, per_prod, parts + ["other_transaction_fees"]


     # ---------- country-aware components (patched US logic to match reference) ----------
    def _credits_components(self, df: pd.DataFrame, ctx: Dict[str, Any]) -> Tuple[float, pd.DataFrame, pd.DataFrame, List[str]]:
        """
        US ONLY helper for credits.
        UK is centralized elsewhere → return empty payload here.
        """
        if not self._is_us(ctx, df):
            return 0.0, pd.DataFrame(), pd.DataFrame(), []

        df2 = self._df_for_region(df, ctx)
        if df2.empty:
            return 0.0, pd.DataFrame(), pd.DataFrame(), []

        reimb_keywords = [
            "FBA Inventory Reimbursement - Customer Return",
            "FBA Inventory Reimbursement - Customer Service Issue",
            "FBA Inventory Reimbursement - General Adjustment",
            "FBA Inventory Reimbursement - Damaged:Warehouse",
            "FBA Inventory Reimbursement - Lost:Warehouse",
        ]
        desc = df2["description"].astype(str) if "description" in df2.columns else pd.Series([""] * len(df2), index=df2.index)
        mask = desc.str.contains("|".join(map(re.escape, reimb_keywords)), case=False, na=False)

        # components
        gift_total = float(self._col(df2, "gift_wrap_credits").sum())
        ship_total = float(self._col(df2, "shipping_credits").sum())

        # per-SKU reimbursements = ABS(sum(total)) per SKU
        if "sku" in df2.columns:
            reimb_by_sku = (
                df2.loc[mask, ["sku", "total"]]
                .groupby("sku", dropna=True)["total"].sum()
                .abs()
                .reset_index()
                .rename(columns={"total": "reimbursements"})
            )
        else:
            reimb_by_sku = pd.DataFrame(columns=["sku", "reimbursements"])

        parts = ["gift_wrap_credits", "shipping_credits"]
        per_sku = self._agg_by(df2, "sku", parts)
        per_sku = per_sku.merge(reimb_by_sku, on="sku", how="left").fillna(0.0)

        # metric: ABS reimbursements + gift + shipping
        per_sku["__metric__"] = per_sku.get("reimbursements", 0.0) + per_sku.get("gift_wrap_credits", 0.0) + per_sku.get("shipping_credits", 0.0)
        reimb_total_abs = float(per_sku["reimbursements"].sum())
        total = reimb_total_abs + gift_total + ship_total

        sku2prod = self._sku_to_product(df2)
        per_prod = per_sku.merge(sku2prod, on="sku", how="left")
        if "product_name" in per_prod.columns and not per_prod.empty:
            per_prod = per_prod.groupby("product_name", dropna=True)[["reimbursements"] + parts + ["__metric__"]].sum().reset_index()
        else:
            per_prod = pd.DataFrame(columns=["product_name", "__metric__", "reimbursements"] + parts)

        comps = ["reimbursements"] + parts
        return float(total), per_sku, per_prod, comps

    
    def _simple_fee_components_sku_only(
    self, df: pd.DataFrame, *, col: str
) -> Tuple[float, pd.DataFrame, pd.DataFrame, List[str]]:
        """
        Sum of `col` using ONLY rows that have a valid SKU (both US & UK).
        Produces total + per-SKU + per-product (if product_name available).
        """
        if df is None or df.empty or "sku" not in df.columns:
            return 0.0, pd.DataFrame(), pd.DataFrame(), [col]

        # strict SKU presence (same rule used elsewhere)
        m = self._sku_mask(df)
        w = df.loc[m].copy()
        if w.empty:
            return 0.0, pd.DataFrame(), pd.DataFrame(), [col]

        # make numeric safely
        vals = pd.to_numeric(w.get(col, 0.0), errors="coerce").fillna(0.0)
        total = float(vals.sum())

        # per-SKU
        per_sku = (
            w[["sku"]].assign(**{col: vals.values})
            .groupby("sku", dropna=True)[[col]].sum().reset_index()
        )
        per_sku["__metric__"] = per_sku[col]

        # per-product rollup via current mapping logic
        sku2prod = self._sku_to_product(w)
        if not sku2prod.empty and "product_name" in sku2prod.columns:
            per_prod = (
                per_sku.merge(sku2prod, on="sku", how="left")
                    .groupby("product_name", dropna=True)[[col, "__metric__"]]
                    .sum().reset_index()
            )
        else:
            per_prod = pd.DataFrame(columns=["product_name", "__metric__", col])

        return total, per_sku, per_prod, [col]


    
    def _amazon_fee_components(self, df: pd.DataFrame, ctx: Dict[str, Any]) -> Tuple[float, pd.DataFrame, pd.DataFrame, List[str]]:
        """
        US ONLY helper for Amazon fees.
        UK is centralized elsewhere → return empty payload here.
        """
        if not self._is_us(ctx, df):
            return 0.0, pd.DataFrame(), pd.DataFrame(), []

        # Ensure required cols exist for the TOTAL (all rows)
        cols = ["fba_fees", "selling_fees", "other_transaction_fees", "other"]
        w_all = df.copy() if df is not None else pd.DataFrame(columns=cols)
        for c in cols:
            if c not in w_all.columns:
                w_all[c] = 0.0
            w_all[c] = self._safe_num(w_all[c])

        # US total: |FBA| + |Selling| - |Other|
        fba_abs   = float(w_all["fba_fees"].abs().sum())
        sell_abs  = float(w_all["selling_fees"].abs().sum())
        other_abs = float(w_all["other"].abs().sum())
        total = fba_abs + sell_abs - other_abs

        # ---------- Breakdown from region-scoped SKU rows ----------
        df_sku = self._df_for_region(df, ctx)
        if df_sku is not None and not df_sku.empty and "sku" in df_sku.columns:
            tmp = df_sku.copy()
            for c in cols:
                if c not in tmp.columns:
                    tmp[c] = 0.0
                tmp[c] = self._safe_num(tmp[c])

            tmp["fba_abs"]       = tmp["fba_fees"].abs()
            tmp["selling_abs"]   = tmp["selling_fees"].abs()
            tmp["other_abs"]     = tmp["other"].abs()

            per_sku = (
                tmp.groupby("sku", dropna=True)[["fba_abs","selling_abs","other_abs"]]
                .sum()
                .reset_index()
            )
            per_sku["metric"] = per_sku["fba_abs"] + per_sku["selling_abs"] - per_sku["other_abs"]
            comps = ["fba_abs","selling_abs","other_abs"]
        else:
            per_sku = pd.DataFrame()
            comps = ["fba_abs","selling_abs","other_abs"]

        # Optional per-product rollup
        sku2prod = self._sku_to_product(df_sku) if df_sku is not None else pd.DataFrame(columns=["sku","product_name"])
        if not per_sku.empty and not sku2prod.empty and "product_name" in sku2prod.columns:
            per_prod = (
                per_sku.merge(sku2prod, on="sku", how="left")
                    .groupby("product_name", dropna=True)[["metric"] + comps]
                    .sum().reset_index()
            )
        else:
            per_prod = pd.DataFrame(columns=["product_name","metric"] + comps)

        return float(total), per_sku, per_prod, comps


    # ---------- evaluators ----------
   
    

    # def _sales(self, df: pd.DataFrame, ctx: Dict[str, Any]) -> Dict[str, Any]:
    #     print(
    #         f"[TRACE][FE] FormulaEngine._sales CALLED rows={len(df)} "
    #         f"country={ctx.get('country')} want_breakdown={ctx.get('want_breakdown')}"
    #     )
    #     import pandas as pd

    #     is_us = self._is_us(ctx, df)

    #     # ---------- UK → centralized + per-product like _profit, NO components ----------
    #     if not is_us:
    #         try:
    #             # Region-scoped frame (applies SKU mask etc.)
    #             df_region = self._df_for_region(df, ctx)
    #             if df_region is None:
    #                 df_region = pd.DataFrame()

    #             # Keep only valid SKUs (same as _profit)
    #             dfk = df_region.copy()
    #             dfk["sku"] = dfk.get("sku", "").astype(str).str.strip()
    #             dfk = dfk[
    #                 dfk["sku"].notna()
    #                 & (dfk["sku"] != "")
    #                 & (dfk["sku"] != "0")
    #                 & (dfk["sku"].str.lower() != "none")
    #             ]

    #             # Call centralized UK sales helper (same style as _profit)
    #             sales_total, sales_by_sku, _ = uk_sales(dfk)

    #             # Ensure we have a DataFrame
    #             if not isinstance(sales_by_sku, pd.DataFrame):
    #                 sales_by_sku = pd.DataFrame(columns=["sku", "__metric__"])

    #             # --- per-SKU: only sku + __metric__ (no components) ---
    #             per_sku = sales_by_sku.copy()
    #             if "sku" not in per_sku.columns:
    #                 per_sku["sku"] = ""
    #             if "__metric__" not in per_sku.columns:
    #                 # fallback: if central fn didn't set __metric__, try product_sales
    #                 if "product_sales" in per_sku.columns:
    #                     per_sku["__metric__"] = pd.to_numeric(per_sku["product_sales"], errors="coerce").fillna(0.0)
    #                 else:
    #                     per_sku["__metric__"] = 0.0
    #             per_sku = per_sku[["sku", "__metric__"]]

    #             # --- per-product: roll up SKU totals -> product_name ---
    #             sku2prod = self._sku_to_product(dfk)
    #             if not per_sku.empty and not sku2prod.empty and "product_name" in sku2prod.columns:
    #                 tmp = per_sku.merge(sku2prod, on="sku", how="left")
    #                 per_prod = (
    #                     tmp.groupby("product_name", dropna=True)["__metric__"]
    #                     .sum()
    #                     .reset_index()
    #                 )
    #             else:
    #                 per_prod = pd.DataFrame(columns=["product_name", "__metric__"])

    #             # --- final table: ONLY totals + per-product (no components) ---
    #             if ctx.get("want_breakdown"):
    #                 # component_cols = [] → _final_table will NOT add any component_* columns
    #                 table = self._final_table(
    #                     "sales",
    #                     float(sales_total or 0.0),
    #                     per_sku[["sku", "__metric__"]],
    #                     per_prod[["product_name", "__metric__"]],
    #                     component_cols=[],
    #                 )
    #             else:
    #                 table = self._total_only_table("sales", float(sales_total or 0.0))

    #             expl = "UK sales via centralized uk_sales, with per-SKU and per-product totals only."
    #             return {
    #                 "result": self._sr(sales_total),
    #                 "explanation": expl,
    #                 "table_df": table,
    #             }

    #         except Exception as e:
    #             print("[ERROR][formula][UK]_sales centralized call failed:", e)
    #             # strict: no fallback calc
    #             try:
    #                 nan_val = float("nan")
    #                 table = self._total_only_table("sales", nan_val)
    #                 sr_val = self._sr(nan_val)
    #             except Exception:
    #                 table, sr_val = pd.DataFrame(), None
    #             return {
    #                 "result": sr_val,
    #                 "explanation": f"UK sales failed in centralized formula: {e}. No fallback executed by design.",
    #                 "table_df": table,
    #             }

    #     # ---------- US → keep existing local path ----------
    #     total, per_sku, per_prod, comps = self._sales_components(df, ctx)
    #     table = (
    #         self._final_table("sales", total, per_sku, per_prod, comps)
    #         if ctx.get("want_breakdown")
    #         else self._total_only_table("sales", total)
    #     )
    #     expl = "US sales = product_sales + promotional_rebates (SKU rows only)"
    #     return {"result": self._sr(total), "explanation": expl, "table_df": table}
    
    def _sales(self, df: pd.DataFrame, ctx: Dict[str, Any]) -> Dict[str, Any]:
        print(
            f"[TRACE][FE] FormulaEngine._sales CALLED rows={len(df)} "
            f"country={ctx.get('country')} want_breakdown={ctx.get('want_breakdown')}"
        )
        import pandas as pd

        is_us = self._is_us(ctx, df)

        # ---------- UK → centralized + per-product + optional month breakdown ----------
        if not is_us:
            try:
                # Region-scoped frame (applies SKU mask etc.)
                df_region = self._df_for_region(df, ctx)
                if df_region is None:
                    df_region = pd.DataFrame()

                # Keep only valid SKUs (same as _profit)
                dfk = df_region.copy()
                dfk["sku"] = dfk.get("sku", "").astype(str).str.strip()
                dfk = dfk[
                    dfk["sku"].notna()
                    & (dfk["sku"] != "")
                    & (dfk["sku"] != "0")
                    & (dfk["sku"].str.lower() != "none")
                ]

                # -------------------- NEW: month-wise breakdown path --------------------
                group_by = (ctx.get("group_by") or "").lower()

                if group_by == "month" and not dfk.empty:
                    # Build a normalized monthly period column (__period__)
                    if "date_time" in dfk.columns:
                        dfk["__dt"] = pd.to_datetime(dfk["date_time"], errors="coerce", utc=True)
                        dfk["__period__"] = (
                            dfk["__dt"]
                            .dt.to_period("M")
                            .dt.to_timestamp()
                            .dt.tz_localize("UTC")
                        )
                    elif {"month", "year"}.issubset(dfk.columns):
                        # Reuse helpers used elsewhere (compare/trend code)
                        mnum = dfk["month"].apply(self._month_to_int)
                        yint = dfk["year"].apply(self._year_int)
                        dfk["__period__"] = pd.to_datetime(
                            dict(year=yint, month=mnum, day=1),
                            errors="coerce",
                            utc=True,
                        )
                    else:
                        dfk["__period__"] = pd.NaT  # no usable time columns

                    periods = sorted(dfk["__period__"].dropna().unique())

                    if periods:
                        month_rows = []

                  

                        for p in periods:
                            part = dfk[dfk["__period__"].eq(p)].copy()
                            # Reuse centralized logic so "sales" definition stays identical
                            mon_total, _mon_by_sku, _ = uk_sales(part)
                            month_rows.append(
                                {
                                    "level": "month",
                                    "key": pd.Timestamp(p).strftime("%b %Y"),
                                    "result": float(mon_total or 0.0),
                                    "_period_key": pd.Timestamp(p),
                                }
                            )

                        per_month = (
                            pd.DataFrame(month_rows)
                            .sort_values("_period_key")
                            .drop(columns=["_period_key"])
                            .reset_index(drop=True)
                        )

                        sales_total = float(per_month["result"].sum() or 0.0)

                        # Add a TOTAL row at the top for consistency with other metrics
                        total_row = pd.DataFrame(
                            [{"level": "total", "key": "TOTAL", "result": sales_total}]
                        )
                        table = pd.concat([total_row, per_month], ignore_index=True)

                        expl = (
                            "UK sales via centralized uk_sales, aggregated month-wise "
                            "over the selected period."
                        )
                        return {
                            "result": self._sr(sales_total),
                            "explanation": expl,
                            "table_df": table,
                        }

                    # If for some reason we had no usable periods, fall through to the
                    # normal per-SKU / per-product logic below.

                # -------------------- Existing UK total + product/SKU logic --------------------
        

                # Centralized UK sales helper (same style as _profit)
                sales_total, sales_by_sku, _ = uk_sales(dfk)

                # Ensure we have a DataFrame
                if not isinstance(sales_by_sku, pd.DataFrame):
                    sales_by_sku = pd.DataFrame(columns=["sku", "__metric__"])

                # --- per-SKU: only sku + __metric__ (no components) ---
                per_sku = sales_by_sku.copy()
                if "sku" not in per_sku.columns:
                    per_sku["sku"] = ""
                if "__metric__" not in per_sku.columns:
                    # fallback: if central fn didn't set __metric__, try product_sales
                    if "product_sales" in per_sku.columns:
                        per_sku["__metric__"] = pd.to_numeric(
                            per_sku["product_sales"], errors="coerce"
                        ).fillna(0.0)
                    else:
                        per_sku["__metric__"] = 0.0
                per_sku = per_sku[["sku", "__metric__"]]

                # --- per-product: roll up SKU totals -> product_name ---
                sku2prod = self._sku_to_product(dfk)
                if (
                    not per_sku.empty
                    and not sku2prod.empty
                    and "product_name" in sku2prod.columns
                ):
                    tmp = per_sku.merge(sku2prod, on="sku", how="left")
                    per_prod = (
                        tmp.groupby("product_name", dropna=True)["__metric__"]
                        .sum()
                        .reset_index()
                    )
                else:
                    per_prod = pd.DataFrame(columns=["product_name", "__metric__"])

                # --- final table: ONLY totals + per-product (no components) ---
                if ctx.get("want_breakdown"):
                    # component_cols = [] → _final_table will NOT add any component_* columns
                    table = self._final_table(
                        "sales",
                        float(sales_total or 0.0),
                        per_sku[["sku", "__metric__"]],
                        per_prod[["product_name", "__metric__"]],
                        component_cols=[],
                    )
                else:
                    table = self._total_only_table("sales", float(sales_total or 0.0))

                expl = (
                    "UK sales via centralized uk_sales, with per-SKU and per-product totals only."
                )
                return {
                    "result": self._sr(sales_total),
                    "explanation": expl,
                    "table_df": table,
                }

            except Exception as e:
                print("[ERROR][formula][UK]_sales centralized call failed:", e)
                # strict: no fallback calc
                try:
                    nan_val = float("nan")
                    table = self._total_only_table("sales", nan_val)
                    sr_val = self._sr(nan_val)
                except Exception:
                    table, sr_val = pd.DataFrame(), None
                return {
                    "result": sr_val,
                    "explanation": f"UK sales failed in centralized formula: {e}. No fallback executed by design.",
                    "table_df": table,
                }

        # ---------- US → keep existing local path ----------

        total, per_sku, per_prod, comps = self._sales_components(df, ctx)
        table = (
            self._final_table("sales", total, per_sku, per_prod, comps)
            if ctx.get("want_breakdown")
            else self._total_only_table("sales", total)
        )
        expl = "US sales = product_sales + promotional_rebates (SKU rows only)"
        return {"result": self._sr(total), "explanation": expl, "table_df": table}




   
    def _tax(self, df: pd.DataFrame, ctx: Dict[str, Any]) -> Dict[str, Any]:
        print(f"[TRACE][FE] FormulaEngine._tax CALLED rows={len(df)} "
            f"country={ctx.get('country')} want_breakdown={ctx.get('want_breakdown')}")
        import inspect, numpy as np, pandas as pd

        is_us = self._is_us(ctx, df)

        # ---------- UK → centralized-only (matches _sales style) ----------
        if not is_us:
            def _call_central(fn, df_, ctx_):
                sig = inspect.signature(fn)
                kwargs = {}
                if "want_breakdown" in sig.parameters:
                    kwargs["want_breakdown"] = bool(ctx_.get("want_breakdown"))
                if "country" in sig.parameters:
                    kwargs["country"] = ctx_.get("country", "UK")
                if "debug" in sig.parameters:
                    kwargs["debug"] = True
                return fn(df=df_, **kwargs)

            def _normalize(raw):
                # Accepts tuple/dict/None just like _sales
                if raw is None:
                    return {"total": 0.0, "per_sku": pd.DataFrame(), "per_product": pd.DataFrame(), "components": []}
                if isinstance(raw, tuple) and len(raw) >= 2:
                    return {"total": float(raw[0] or 0.0),
                            "per_sku": raw[1] if isinstance(raw[1], pd.DataFrame) else pd.DataFrame(),
                            "per_product": pd.DataFrame(),
                            "components": raw[2] if len(raw) > 2 else []}
                if isinstance(raw, dict):
                    return {"total": float(raw.get("total", 0.0)),
                            "per_sku": raw.get("per_sku") or pd.DataFrame(),
                            "per_product": raw.get("per_product") or pd.DataFrame(),
                            "components": raw.get("components") or []}
                return {"total": 0.0, "per_sku": pd.DataFrame(), "per_product": pd.DataFrame(), "components": []}

            def _ensure_cols(per_sku: pd.DataFrame, comps: list[str]) -> pd.DataFrame:
                base = ["sku", "__metric__"]
                default_comps = [
                    "product_sales_tax",
                    "marketplace_facilitator_tax",
                    "shipping_credits_tax",
                    "giftwrap_credits_tax",
                    "promotional_rebates_tax",
                    "other_transaction_fees",
                ]
                want = base + (comps or default_comps)
                if not isinstance(per_sku, pd.DataFrame) or per_sku.empty:
                    return pd.DataFrame(columns=want)
                for c in want:
                    if c not in per_sku.columns:
                        per_sku[c] = "" if c == "sku" else 0.0
                return per_sku[want]

            try:
                df_region = self._df_for_region(df, ctx)               # <- SKU-filtered
                raw = _call_central(uk_tax, df_region, ctx)
                payload = _normalize(raw)

                total, per_sku, per_prod, comps = (
                    payload["total"], payload["per_sku"], payload["per_product"], payload["components"]
                )

                if ctx.get("want_breakdown"):
                    table = self._final_table(
                        "tax",
                        total,
                        _ensure_cols(per_sku, comps),
                        per_prod if not per_prod.empty else pd.DataFrame(columns=["product_name", "__metric__"]),
                        comps or [
                            "product_sales_tax","marketplace_facilitator_tax","shipping_credits_tax",
                            "giftwrap_credits_tax","promotional_rebates_tax","other_transaction_fees"
                        ],
                    )
                else:
                    table = self._total_only_table("tax", total)

                expl = "UK tax (centralized via formulas_utils.uk_tax)."
                return {"result": self._sr(total), "explanation": expl, "table_df": table}

            except Exception as e:
                print("[ERROR][formula][UK]_tax centralized call failed:", e)
                # strict: no fallback calc
                try:
                    nan_val = float("nan")
                    table = self._total_only_table("tax", nan_val)
                    sr_val = self._sr(nan_val)
                except Exception:
                    table, sr_val = pd.DataFrame(), None
                return {
                    "result": sr_val,
                    "explanation": f"UK tax failed in centralized formula: {e}. No fallback executed by design.",
                    "table_df": table,
                }

        # ---------- US → local components (matches _sales structure) ----------
        total, per_sku, per_prod, comps = self._tax_components(df, ctx)
        table = (
            self._final_table("tax", total, per_sku, per_prod, comps)
            if ctx.get("want_breakdown") else
            self._total_only_table("tax", total)
        )
        expl = (
            "US tax = product_sales_tax + marketplace_facilitator_tax + shipping_credits_tax + "
            "giftwrap_credits_tax + promotional_rebates_tax + other_transaction_fees (SKU rows only)."
        )
        return {"result": self._sr(total), "explanation": expl, "table_df": table}


    

    def _credits(self, df: pd.DataFrame, ctx: Dict[str, Any]) -> Dict[str, Any]:
        print(f"[TRACE][FE] FormulaEngine._credits CALLED rows={len(df)} "
            f"country={ctx.get('country')} want_breakdown={ctx.get('want_breakdown')}")
        import inspect, numpy as np, pandas as pd

        is_us = self._is_us(ctx, df)

        # ---------- UK → centralized-only (matches _sales / _tax structure) ----------
        if not is_us:
            def _call_central(fn, df_, ctx_):
                sig = inspect.signature(fn)
                kwargs = {}
                if "want_breakdown" in sig.parameters:
                    kwargs["want_breakdown"] = bool(ctx_.get("want_breakdown"))
                if "country" in sig.parameters:
                    kwargs["country"] = ctx_.get("country", "UK")
                if "debug" in sig.parameters:
                    kwargs["debug"] = True
                return fn(df=df_, **kwargs)

            def _normalize(raw):
                if raw is None:
                    return {"total": 0.0, "per_sku": pd.DataFrame(), "per_product": pd.DataFrame(), "components": []}
                if isinstance(raw, tuple) and len(raw) >= 2:
                    return {"total": float(raw[0] or 0.0),
                            "per_sku": raw[1] if isinstance(raw[1], pd.DataFrame) else pd.DataFrame(),
                            "per_product": pd.DataFrame(),
                            "components": raw[2] if len(raw) > 2 else []}
                if isinstance(raw, dict):
                    return {"total": float(raw.get("total", 0.0)),
                            "per_sku": raw.get("per_sku") or pd.DataFrame(),
                            "per_product": raw.get("per_product") or pd.DataFrame(),
                            "components": raw.get("components") or []}
                return {"total": 0.0, "per_sku": pd.DataFrame(), "per_product": pd.DataFrame(), "components": []}

            def _ensure_cols(per_sku: pd.DataFrame, comps: list[str]) -> pd.DataFrame:
                base = ["sku", "__metric__"]
                default_comps = ["postage_credits", "gift_wrap_credits"]
                want = base + (comps or default_comps)
                if not isinstance(per_sku, pd.DataFrame) or per_sku.empty:
                    return pd.DataFrame(columns=want)
                for c in want:
                    if c not in per_sku.columns:
                        per_sku[c] = "" if c == "sku" else 0.0
                return per_sku[want]

            try:
                df_region = self._df_for_region(df, ctx)  # <- SKU-filtered
                raw = _call_central(uk_credits, df_region, ctx)
                payload = _normalize(raw)

                total, per_sku, per_prod, comps = (
                    payload["total"], payload["per_sku"], payload["per_product"], payload["components"]
                )

                if ctx.get("want_breakdown"):
                    table = self._final_table(
                        "credits",
                        total,
                        _ensure_cols(per_sku, comps),
                        per_prod if not per_prod.empty else pd.DataFrame(columns=["product_name", "__metric__"]),
                        comps or ["postage_credits", "gift_wrap_credits"],
                    )
                else:
                    table = self._total_only_table("credits", total)

                expl = "UK credits (centralized via formulas_utils.uk_credits)."
                return {"result": self._sr(total), "explanation": expl, "table_df": table}

            except Exception as e:
                print("[ERROR][formula][UK]_credits centralized call failed:", e)
                # strict: no fallback calc
                try:
                    nan_val = float("nan")
                    table = self._total_only_table("credits", nan_val)
                    sr_val = self._sr(nan_val)
                except Exception:
                    table, sr_val = pd.DataFrame(), None
                return {
                    "result": sr_val,
                    "explanation": f"UK credits failed in centralized formula: {e}. No fallback executed by design.",
                    "table_df": table,
                }

        # ---------- US → local components (matches _sales / _tax structure) ----------
        total, per_sku, per_prod, comps = self._credits_components(df, ctx)
        table = (
            self._final_table("credits", total, per_sku, per_prod, comps)
            if ctx.get("want_breakdown") else
            self._total_only_table("credits", total)
        )
        expl = "US credits = |reimbursements by keyword| + gift_wrap_credits + shipping_credits (SKU rows only)."
        return {"result": self._sr(total), "explanation": expl, "table_df": table}

    def _debug_profit_components(self, df_in: pd.DataFrame, ctx: Dict[str, Any], *, label: str):
        """
        Dumps FE-side component totals (after region/SKU filtering) so you can diff against
        the centralized (UK) result or your per-SKU sum (US). No logic changes.
        """
        import pandas as pd

        if df_in is None or df_in.empty:
            print(f"[DEBUG][profit][{label}] input df empty → nothing to print")
            return

        # These builders already respect region/SKU rules internally.
        sales_total,   _, _, _ = self._sales_components(df_in, ctx)
        tax_total,     _, _, _ = self._tax_components(df_in, ctx)
        credits_total, _, _, _ = self._credits_components(df_in, ctx)
        amazon_total,  _, _, _ = self._amazon_fee_components(df_in, ctx)

        df2 = self._df_for_region(df_in, ctx)
        if "cost_of_unit_sold" not in df2.columns:
            df2["cost_of_unit_sold"] = 0.0
        cogs_total = float(pd.to_numeric(df2["cost_of_unit_sold"], errors="coerce").fillna(0.0).abs().sum())

        naive_profit = (
            abs(float(sales_total))
            + abs(float(credits_total))
            - abs(float(tax_total))
            - float(amazon_total)
            - float(cogs_total)
        )

        print(
            "[DEBUG][profit][{lb}] FE components:\n"
            "  sales_total      = {s:12,.2f}\n"
            "  credits_total    = {c:12,.2f}\n"
            "  tax_total        = {t:12,.2f}\n"
            "  amazon_fee_total = {a:12,.2f}\n"
            "  cogs_total       = {g:12,.2f}\n"
            "  naive_profit     = {p:12,.2f}"
            .format(lb=label, s=sales_total, c=credits_total, t=tax_total,
                    a=amazon_total, g=cogs_total, p=naive_profit)
        )


   
    def _profit(self, df: pd.DataFrame, ctx: Dict[str, Any]) -> Dict[str, Any]:
        """
        Profit entrypoint aligned with _sales/_tax/_credits structure.

        - UK: Uses centralized sales/tax/credits; RE-CALCULATES amazon_fee locally:
            amazon_fee_total = abs(sum(fba_fees + selling_fees)) over valid SKUs only,
            with a single abs at the end (not per component). Profit is then:
            |Sales| + |Credits| − |Taxes| − amazon_fee_total − |COGS|.
            Per-SKU profit computed similarly for breakdowns.
        - US: Local per-SKU build (unchanged).
        """
        print(f"[TRACE][FE] FormulaEngine._profit CALLED rows={len(df)} "
            f"country={ctx.get('country')} want_breakdown={ctx.get('want_breakdown')}")
        import pandas as pd

        is_us = self._is_us(ctx, df)

        # ---------- UK → centralized components + local Amazon-fee recalc ----------
        if not is_us:
            # Region/SKU-scoped view
            df_region = self._df_for_region(df, ctx)
            if df_region is None:
                df_region = pd.DataFrame()

            # Keep only valid SKUs
            dfk = df_region.copy()
            dfk["sku"] = dfk.get("sku", "").astype(str).str.strip()
            dfk = dfk[
                dfk["sku"].notna()
                & (dfk["sku"] != "")
                & (dfk["sku"] != "0")
                & (dfk["sku"].str.lower() != "none")
            ]

            # Ensure numeric columns exist & are numeric
            for col in ["fba_fees", "selling_fees", "cost_of_unit_sold"]:
                if col not in dfk.columns:
                    dfk[col] = 0.0
                dfk[col] = self._safe_num(dfk[col])

            # --- Centralized component helpers (authoritative for these) ---
            # NOTE: we do NOT use centralized amazon_fee; we recalc locally per your rule
            sales_total,   sales_by_sku,   _ = uk_sales(dfk)
            tax_total,     tax_by_sku,     _ = uk_tax(dfk)
            credits_total, credits_by_sku, _ = uk_credits(dfk)

            # --- Local Amazon Fee Recalculation (your rule) ---
            # amazon_fee_total = abs(sum(fba_fees + selling_fees))  (single abs at end)
            amazon_fee_total = abs(dfk["fba_fees"].sum() + dfk["selling_fees"].sum())

            # Per-SKU amazon fee (use abs on the per-sku combined value, not per component)
            fee_by_sku = (
                dfk.groupby("sku", dropna=True)[["fba_fees", "selling_fees"]]
                .sum()
                .reset_index()
            )
            fee_by_sku["amazon_fee"] = (fee_by_sku["fba_fees"] + fee_by_sku["selling_fees"]).abs()
            fee_by_sku = fee_by_sku[["sku", "amazon_fee"]]

            # --- COGS (|cost_of_unit_sold|) ---
            cost_by_sku = (
                dfk.groupby("sku", dropna=True)["cost_of_unit_sold"]
                .sum()
                .abs()
                .reset_index()
            ).rename(columns={"cost_of_unit_sold": "cost"})
            cost_total = float(cost_by_sku["cost"].sum()) if not cost_by_sku.empty else 0.0

            # --- Build per-SKU frame from components ---
            # Normalize component frames and merge
            def _pick(df_, name):
                if isinstance(df_, pd.DataFrame) and not df_.empty and "__metric__" in df_.columns and "sku" in df_.columns:
                    out = df_[["sku", "__metric__"]].copy()
                    out = out.rename(columns={"__metric__": name})
                    out[name] = self._safe_num(out[name])
                    out["sku"] = out["sku"].astype(str)
                    return out
                return pd.DataFrame(columns=["sku", name])

            sales_per_sku   = _pick(sales_by_sku,   "sales")
            credits_per_sku = _pick(credits_by_sku, "credits")
            taxes_per_sku   = _pick(tax_by_sku,     "taxes")

            per_sku = (
                pd.DataFrame({"sku": dfk["sku"].unique()})
                .merge(sales_per_sku,   on="sku", how="left")
                .merge(credits_per_sku, on="sku", how="left")
                .merge(taxes_per_sku,   on="sku", how="left")
                .merge(fee_by_sku,      on="sku", how="left")
                .merge(cost_by_sku,     on="sku", how="left")
            ).fillna(0.0)

            # Profit per SKU:
            # |Sales| + |Credits| − |Taxes| − amazon_fee (local) − |COGS|
            for col in ["sales", "credits", "taxes", "amazon_fee", "cost"]:
                per_sku[col] = self._safe_num(per_sku[col])

            per_sku["__metric__"] = (
                per_sku["sales"].abs()
                + per_sku["credits"].abs()
                + per_sku["taxes"].abs()
                - per_sku["amazon_fee"]
                - per_sku["cost"].abs()
            )

            total = float(per_sku["__metric__"].sum())

            # Debug prints to verify parity with your pipeline
            print("[DEBUG][profit][UK-local] components (totals):")
            print(f"  sales_total        = {float(sales_total):12,.2f}")
            print(f"  credits_total      = {float(credits_total):12,.2f}")
            print(f"  taxes_total        = {float(tax_total):12,.2f}")
            print(f"  amazon_fee_total   = {float(amazon_fee_total):12,.2f}")
            print(f"  cost_total         = {float(cost_total):12,.2f}")
            print(f"  profit_total       = {float(total):12,.2f}")

            # Build output table(s)
            if not ctx.get("want_breakdown"):
                table = self._total_only_table("profit", total)
            else:
                # Optional product roll-up
                sku2prod = self._sku_to_product(dfk)
                if not sku2prod.empty and "product_name" in sku2prod.columns:
                    per_prod = (
                        per_sku.merge(sku2prod, on="sku", how="left")
                            .groupby("product_name", dropna=True)
                            [["sales", "credits", "taxes", "amazon_fee", "cost", "__metric__"]]
                            .sum().reset_index()
                    )
                else:
                    per_prod = pd.DataFrame(
                        columns=["product_name", "__metric__", "sales", "credits", "taxes", "amazon_fee", "cost"]
                    )

                table = self._final_table(
                    "profit",
                    total,
                    per_sku[["sku", "__metric__", "sales", "credits", "taxes", "amazon_fee", "cost"]],
                    per_prod[["product_name", "__metric__", "sales", "credits", "taxes", "amazon_fee", "cost"]],
                    ["sales", "credits", "taxes", "amazon_fee", "cost"],
                )

            expl = (
                "UK profit computed with centralized sales/taxes/credits, "
                "local Amazon fee = abs(sum(fba_fees + selling_fees)) over valid SKUs, "
                "and profit = |Sales| + |Credits| − |Taxes| − AmazonFee − |COGS|."
            )
            return {"result": self._sr(total), "explanation": expl, "table_df": table}

        # ---------- US → local component build (unchanged) ----------
        sales_total,   sales_by_sku,   _, _ = self._sales_components(df, ctx)
        tax_total,     tax_by_sku,     _, _ = self._tax_components(df, ctx)
        credits_total, credits_by_sku, _, _ = self._credits_components(df, ctx)

        df2 = self._df_for_region(df, ctx).copy()

        # Ensure required columns exist & numeric
        needed_cols = [
            "fba_fees","selling_fees","other","cost_of_unit_sold","type","sku",
            "product_name",
            "gift_wrap_credits","shipping_credits",
            "product_sales","promotional_rebates",
            "product_sales_tax","marketplace_facilitator_tax","shipping_credits_tax","giftwrap_credits_tax","promotional_rebates_tax",
            "other_transaction_fees",
        ]
        for col in needed_cols:
            if col not in df2.columns:
                df2[col] = "" if col in ("type","sku","product_name") else 0.0
        num_cols = [c for c in needed_cols if c not in ("type","sku","product_name")]
        for col in num_cols:
            df2[col] = self._safe_num(df2[col])
        df2["sku"] = df2["sku"].astype(str)

        # Strict SKU presence
        df3 = df2.copy()
        df3["sku"] = df3["sku"].astype(str).str.strip()
        df3 = df3[(df3["sku"].notna()) & (df3["sku"] != "") & (df3["sku"] != "0") & (df3["sku"].str.lower() != "none")]

        # Refund selling-fee adjustment per SKU (−2× refunds)
        refund_by_sku = (
            df3.loc[df3["type"].astype(str).str.lower().eq("refund"), ["sku","selling_fees"]]
            .groupby("sku", dropna=True)["selling_fees"].sum()
            .reset_index()
            .rename(columns={"selling_fees":"refund_selling_fees"})
        )

        # Base per-SKU aggregates
        parts = ["fba_fees","selling_fees","other","cost_of_unit_sold"]
        per_sku = self._agg_by(df3, "sku", parts)

        # Merge refunds and centralized component rollups
        per_sku = (
            per_sku.merge(refund_by_sku, on="sku", how="left").fillna(0.0)
                .merge(sales_by_sku.rename(columns={"__metric__":"sales"})[["sku","sales"]], on="sku", how="left")
                .merge(credits_by_sku.rename(columns={"__metric__":"credits"})[["sku","credits"]], on="sku", how="left")
                .merge(tax_by_sku.rename(columns={"__metric__":"taxes"})[["sku","taxes"]], on="sku", how="left")
                .fillna(0.0)
        )

        # Apply refund adjustment to selling_fees before computing Amazon fee
        per_sku["selling_fees_adj"] = per_sku["selling_fees"] - 2.0 * per_sku["refund_selling_fees"]

        # Amazon fee per SKU (US): |FBA| + |Selling_adj| − |Other|
        per_sku["amazon_fee"] = per_sku["fba_fees"].abs() + per_sku["selling_fees_adj"].abs() - per_sku["other"].abs()

        # Profit per SKU
        per_sku["__metric__"] = (
            per_sku["sales"].abs()
            + per_sku["credits"].abs()
            - per_sku["taxes"].abs()
            - per_sku["amazon_fee"]
            - per_sku["cost_of_unit_sold"].abs()
        )
        total = float(per_sku["__metric__"].sum())

        # Build table
        if not ctx.get("want_breakdown"):
            table = self._total_only_table("profit", total)
        else:
            # optional product roll-up
            sku2prod = self._sku_to_product(df3)
            if not sku2prod.empty and "product_name" in sku2prod.columns:
                per_prod = (
                    per_sku.merge(sku2prod, on="sku", how="left")
                        .groupby("product_name", dropna=True)[
                            ["sales","credits","taxes","amazon_fee","cost_of_unit_sold","__metric__"]
                        ].sum().reset_index()
                )
            else:
                per_prod = pd.DataFrame(columns=["product_name","__metric__","sales","credits","taxes","amazon_fee","cost_of_unit_sold"])

            table = self._final_table(
                "profit",
                total,
                per_sku[["sku","__metric__","sales","credits","taxes","amazon_fee","cost_of_unit_sold"]],
                per_prod[["product_name","__metric__","sales","credits","taxes","amazon_fee","cost_of_unit_sold"]],
                ["sales","credits","taxes","amazon_fee","cost_of_unit_sold"],
            )

        expl = ("US profit = |Sales| + |Credits| − |Taxes| − (|FBA| + |Selling_adj| − |Other|) − |COGS|; "
                "Selling_adj = selling_fees − 2×refund_selling_fees; computed per-SKU then summed.")
        return {"result": self._sr(total), "explanation": expl, "table_df": table}


    



    # ---------- derived ratios & mixes ----------
    def _reimbursement_fee(self, df: pd.DataFrame, ctx: Dict[str, Any]) -> Dict[str, Any]:
        if df is None or df.empty or "type" not in df.columns or "total" not in df.columns:
            return {"result": 0.0, "explanation": "No reimbursement rows", "table_df": self._total_only_table("reimbursement_fee", 0.0)}

        totals = pd.to_numeric(df["total"], errors="coerce").fillna(0.0)
        types = df["type"].astype(str).str.strip().str.lower()

        reimb_rows = df.loc[types.eq("transfer")].copy()
        reimb_total = float(totals.loc[reimb_rows.index].sum())

        if not ctx.get("want_breakdown"):
            table = self._total_only_table("reimbursement_fee", reimb_total)
        else:
            # Per-SKU
            if "sku" in reimb_rows.columns:
                per_sku = reimb_rows.groupby("sku", dropna=True)["total"].sum().reset_index()
                per_sku["__metric__"] = per_sku["total"]
            else:
                per_sku = pd.DataFrame(columns=["sku", "__metric__", "total"])

            # Per-product
            sku2prod = self._sku_to_product(reimb_rows)
            if not per_sku.empty and not sku2prod.empty:
                per_prod = (
                    per_sku.merge(sku2prod, on="sku", how="left")
                        .groupby("product_name", dropna=True)[["__metric__", "total"]]
                        .sum().reset_index()
                )
            else:
                per_prod = pd.DataFrame(columns=["product_name", "__metric__", "total"])

            table = self._final_table("reimbursement_fee", reimb_total, per_sku, per_prod, ["total"])

        return {
            "result": self._sr(reimb_total),
            "explanation": "reimbursement_fee = sum(total) where type == 'Transfer'",
            "table_df": table,
        }


    def _platform_fee(self, df: pd.DataFrame, ctx: Dict[str, Any]) -> Dict[str, Any]:
        import pandas as pd

        is_us = self._is_us(ctx, df)

        # ---------- UK → centralized + same prep as process_skuwise_data ----------
        if not is_us:
            w = df.copy()

            # Text cols → str
            for col in ["sku","type","description","marketplace","fulfilment",
                        "order_city","order_state","order_postal","tax_collection_model","product_name"]:
                if col in w.columns:
                    w[col] = w[col].astype(str)

            # Numerics coerced
            if "total" in w.columns:
                w["total"] = pd.to_numeric(w["total"].astype(str).str.replace(",", ""), errors="coerce").fillna(0.0)
            if "other" in w.columns:
                w["other"] = pd.to_numeric(w["other"].astype(str).str.replace(",", ""), errors="coerce").fillna(0.0)

            # Ensure explicit col exists
            if "platform_fees" not in w.columns:
                w["platform_fees"] = 0.0

            # Call centralized helper on RAW df
            total, per_sku, comps = uk_platform_fee(w)
            print(f"[TRACE][FE][UK]_platform total={total:.2f} comps={comps} per_sku_rows={0 if per_sku is None else len(per_sku)}")

            # Build table; product roll-up via SKU-filtered mapping
            if ctx.get("want_breakdown"):
                sku2prod = self._sku_to_product(self._df_for_region(w, ctx))
                if isinstance(per_sku, pd.DataFrame) and not per_sku.empty and not sku2prod.empty:
                    per_prod = (
                        per_sku.merge(sku2prod, on="sku", how="left")
                            .groupby("product_name", dropna=True)[["__metric__", *comps]].sum().reset_index()
                    )
                else:
                    per_prod = pd.DataFrame(columns=["product_name","__metric__", *comps])

                table = self._final_table(
                    "platform_fee",
                    float(total),
                    per_sku[["sku","__metric__", *comps]] if isinstance(per_sku, pd.DataFrame) else pd.DataFrame(),
                    per_prod[["product_name","__metric__", *comps]] if isinstance(per_prod, pd.DataFrame) else pd.DataFrame(),
                    comps
                )
            else:
                table = self._total_only_table("platform_fee", float(total))

            expl = "UK platform_fee via centralized helper; totals from all rows, breakdown on valid SKUs."
            return {"result": self._sr(total), "explanation": expl, "table_df": table}

        # ---------- US → keep existing local logic ----------
        if df is None or df.empty or "description" not in df.columns or "total" not in df.columns:
            return {"result": 0.0, "explanation": "No platform fee rows", "table_df": self._total_only_table("platform_fee", 0.0)}

        totals = pd.to_numeric(df["total"], errors="coerce").fillna(0.0)
        desc = df["description"].astype(str)
        keywords = ["Subscription", "FBA storage fee", "FBA Removal Order:", "FBA Long-Term Storage Fee"]

        mask = desc.str.contains("|".join(keywords), case=False, na=False)
        fee_rows = df.loc[mask].copy()
        fee_total = abs(float(totals.loc[fee_rows.index].sum()))

        if not ctx.get("want_breakdown"):
            table = self._total_only_table("platform_fee", fee_total)
        else:
            if "sku" in fee_rows.columns:
                per_sku = fee_rows.groupby("sku", dropna=True)["total"].sum().abs().reset_index()
                per_sku["__metric__"] = per_sku["total"]
            else:
                per_sku = pd.DataFrame(columns=["sku","__metric__","total"])

            sku2prod = self._sku_to_product(fee_rows)
            if not per_sku.empty and not sku2prod.empty:
                per_prod = (
                    per_sku.merge(sku2prod, on="sku", how="left")
                        .groupby("product_name", dropna=True)[["__metric__","total"]]
                        .sum().reset_index()
                )
            else:
                per_prod = pd.DataFrame(columns=["product_name","__metric__","total"])

            table = self._final_table("platform_fee", fee_total, per_sku, per_prod, ["total"])

        return {
            "result": self._sr(fee_total),
            "explanation": "US platform_fee = abs(sum(total)) where description matches subscription/storage keywords",
            "table_df": table,
        }




    def _advertising_total(self, df: pd.DataFrame, ctx: Dict[str, Any]) -> Dict[str, Any]:
        import pandas as pd

        is_us = self._is_us(ctx, df)

        # ---------- UK → centralized + same prep as process_skuwise_data ----------
        if not is_us:
            w = df.copy()

            # Text cols → str (safe)
            for col in ["sku","type","description","marketplace","fulfilment",
                        "order_city","order_state","order_postal","tax_collection_model","product_name"]:
                if col in w.columns:
                    w[col] = w[col].astype(str)

            # Numerics coerced (strip commas)
            if "total" in w.columns:
                w["total"] = pd.to_numeric(w["total"].astype(str).str.replace(",", ""), errors="coerce").fillna(0.0)
            if "other" in w.columns:
                w["other"] = pd.to_numeric(w["other"].astype(str).str.replace(",", ""), errors="coerce").fillna(0.0)

            # Ensure explicit col exists
            if "advertising_cost" not in w.columns:
                w["advertising_cost"] = 0.0

            # Call centralized helper on RAW df (includes non-SKU rows)
            total, per_sku, comps = uk_advertising(w)
            print(f"[TRACE][FE][UK]_ads total={total:.2f} comps={comps} per_sku_rows={0 if per_sku is None else len(per_sku)}")

            # Build table; use SKU-filtered view only for product rollup mapping
            if ctx.get("want_breakdown"):
                sku2prod = self._sku_to_product(self._df_for_region(w, ctx))
                if isinstance(per_sku, pd.DataFrame) and not per_sku.empty and not sku2prod.empty:
                    per_prod = (
                        per_sku.merge(sku2prod, on="sku", how="left")
                            .groupby("product_name", dropna=True)[["__metric__", *comps]].sum().reset_index()
                    )
                else:
                    per_prod = pd.DataFrame(columns=["product_name","__metric__", *comps])

                table = self._final_table(
                    "advertising_total",
                    float(total),
                    per_sku[["sku","__metric__", *comps]] if isinstance(per_sku, pd.DataFrame) else pd.DataFrame(),
                    per_prod[["product_name","__metric__", *comps]] if isinstance(per_prod, pd.DataFrame) else pd.DataFrame(),
                    comps
                )
            else:
                table = self._total_only_table("advertising_total", float(total))

            expl = "UK advertising_total via centralized helper; totals from all rows, breakdown on valid SKUs."
            return {"result": self._sr(total), "explanation": expl, "table_df": table}

        # ---------- US → keep existing local logic ----------
        if df is None or df.empty or "description" not in df.columns or "total" not in df.columns:
            return {"result": 0.0, "explanation": "No advertising rows", "table_df": self._total_only_table("advertising_total", 0.0)}

        totals = pd.to_numeric(df["total"], errors="coerce").fillna(0.0)
        desc = df["description"].astype(str)
        keywords = ["Cost of Advertising", "Coupon Redemption Fee", "Deals", "Lightning Deal"]

        mask = desc.str.contains("|".join(keywords), case=False, na=False)
        ads_rows = df.loc[mask].copy()
        ads_total = abs(float(totals.loc[ads_rows.index].sum()))

        if not ctx.get("want_breakdown"):
            table = self._total_only_table("advertising_total", ads_total)
        else:
            if "sku" in ads_rows.columns:
                per_sku = ads_rows.groupby("sku", dropna=True)["total"].sum().abs().reset_index()
                per_sku["__metric__"] = per_sku["total"]
            else:
                per_sku = pd.DataFrame(columns=["sku","__metric__","total"])

            sku2prod = self._sku_to_product(ads_rows)
            if not per_sku.empty and not sku2prod.empty:
                per_prod = (
                    per_sku.merge(sku2prod, on="sku", how="left")
                        .groupby("product_name", dropna=True)[["__metric__","total"]]
                        .sum().reset_index()
                )
            else:
                per_prod = pd.DataFrame(columns=["product_name","__metric__","total"])

            table = self._final_table("advertising_total", ads_total, per_sku, per_prod, ["total"])

        return {
            "result": self._sr(ads_total),
            "explanation": "US advertising_total = abs(sum(total)) where description contains advertising keywords",
            "table_df": table,
        }



    def _reimbursement_vs_sales(self, df: pd.DataFrame, ctx: Dict[str, Any]) -> Dict[str, Any]:
        reimb = self._reimbursement_fee(df, ctx)["result"] or 0.0
        sales = self._sales(df, ctx)["result"] or 0.0
        val = abs((reimb / sales) * 100) if sales else 0.0
        table = self._total_only_table("reimbursement_vs_sales", val)
        return {"result": self._sr(val), "explanation": "reimbursement_vs_sales = abs((reimbursement_fee / sales) * 100)", "table_df": table}

    def _cm2_profit(self, df: pd.DataFrame, ctx: Dict[str, Any]) -> Dict[str, Any]:
        profit = self._profit(df, ctx)["result"] or 0.0
        ads = self._advertising_total(df, ctx)["result"] or 0.0
        platform = self._platform_fee(df, ctx)["result"] or 0.0
        val = profit - (ads + platform)
        table = self._total_only_table("cm2_profit", val)
        return {"result": self._sr(val), "explanation": "cm2_profit = profit - (advertising_total + platform_fee)", "table_df": table}

    def _cm2_margins(self, df: pd.DataFrame, ctx: Dict[str, Any]) -> Dict[str, Any]:
        sales = self._sales(df, ctx)["result"] or 0.0
        cm2 = self._cm2_profit(df, ctx)["result"] or 0.0
        val = (cm2 / sales) * 100 if sales else 0.0
        table = self._total_only_table("cm2_margins", val)
        return {"result": self._sr(val), "explanation": "cm2_margins = (cm2_profit / sales) * 100", "table_df": table}

    def _acos(self, df: pd.DataFrame, ctx: Dict[str, Any]) -> Dict[str, Any]:
        sales = self._sales(df, ctx)["result"] or 0.0
        ads = self._advertising_total(df, ctx)["result"] or 0.0
        val = (ads / sales) * 100 if sales else 0.0
        table = self._total_only_table("acos", val)
        return {"result": self._sr(val), "explanation": "acos = (advertising_total / sales) * 100", "table_df": table}

    def _reimbursement_vs_cm2_margins(self, df: pd.DataFrame, ctx: Dict[str, Any]) -> Dict[str, Any]:
        reimb = self._reimbursement_fee(df, ctx)["result"] or 0.0
        cm2 = self._cm2_profit(df, ctx)["result"] or 0.0
        val = abs((reimb / cm2) * 100) if cm2 else 0.0
        table = self._total_only_table("reimbursement_vs_cm2_margins", val)
        return {"result": self._sr(val), "explanation": "reimbursement_vs_cm2_margins = abs((reimbursement_fee / cm2_profit) * 100)", "table_df": table}

    def _profit_margin(self, df: pd.DataFrame, ctx: Dict[str, Any]) -> Dict[str, Any]:
        """
        Profit margin (%):
        total      = (total_profit / total_sales) * 100
        per-SKU    = (profit_i / sales_i) * 100
        per-product= sum_i(profit_i) / sum_i(sales_i) * 100 across SKUs that map to the product

        • Sales TOTAL comes from self._sales (US + UK both, UK centralised)
        • Profit TOTAL comes from self._profit (US + UK both, UK centralised)
        """
        # --- TOTAL margin (US + UK, centrally aligned) -------------------------
        sales_total = self._sales(df, {**ctx, "want_breakdown": False}).get("result") or 0.0
        profit_total = self._profit(df, {**ctx, "want_breakdown": False}).get("result") or 0.0
        margin_total = (profit_total / sales_total * 100.0) if sales_total else 0.0

        # No breakdown requested → just total
        if not ctx.get("want_breakdown"):
            table = self._total_only_table("profit_margin", margin_total)
            return {
                "result": self._sr(margin_total),
                "explanation": "profit_margin = (profit / sales) * 100",
                "table_df": table,
            }

        is_us = self._is_us(ctx, df)
        df2 = self._df_for_region(df, ctx)

        # ----------------------------------------------------------------------
        # 🇺🇸 US BREAKDOWN  (same idea as tumhara existing code)
        # ----------------------------------------------------------------------
        if is_us:
            # US helpers (ye already per-SKU rollups dete hain)
            _, sales_by_sku, _, _ = self._sales_components(df, ctx)
            _, credits_by_sku, _, _ = self._credits_components(df, ctx)
            _, tax_by_sku, _, _ = self._tax_components(df, ctx)

            f = self._agg_by(df2, "sku", ["fba_fees", "selling_fees", "cost_of_unit_sold"])

            per_sku = (
                sales_by_sku[["sku", "__metric__"]].rename(columns={"__metric__": "sales"})
                .merge(
                    credits_by_sku[["sku", "__metric__"]].rename(
                        columns={"__metric__": "credits"}
                    ),
                    on="sku",
                    how="outer",
                )
                .merge(
                    tax_by_sku[["sku", "__metric__"]].rename(columns={"__metric__": "taxes"}),
                    on="sku",
                    how="outer",
                )
                .merge(f, on="sku", how="outer")
                .fillna(0.0)
            )

            # optional 'other' component for US
            if "other" in df2.columns:
                other_by_sku = self._agg_by(df2, "sku", ["other"])
            else:
                other_by_sku = pd.DataFrame(columns=["sku", "other"])

            per_sku = per_sku.merge(other_by_sku, on="sku", how="left").fillna(0.0)

            # US profit per SKU (same as pehle tum use kar rahe the)
            per_sku["profit"] = (
                per_sku["sales"]
                + per_sku["credits"]
                - per_sku["taxes"]
                - per_sku.get("fba_fees", 0.0)
                - per_sku.get("selling_fees", 0.0)
                - per_sku.get("other", 0.0)
                - per_sku.get("cost_of_unit_sold", 0.0)
            )

            per_sku["__metric__"] = per_sku.apply(
                lambda r: (r["profit"] / r["sales"] * 100.0) if r["sales"] else 0.0, axis=1
            )

            table = self._table_from_group_metric(
                metric_name="profit_margin",
                total_value=margin_total,
                per_sku_df=per_sku[["sku", "__metric__", "sales", "profit"]],
                sku_metric_col="__metric__",
                component_cols=["sales", "profit"],
            )

            return {
                "result": self._sr(margin_total),
                "explanation": "profit_margin = (profit / sales) * 100",
                "table_df": table,
            }

        # ----------------------------------------------------------------------
        # 🇬🇧 UK BREAKDOWN (centralised-aligned, same logic as UK _profit)
        # ----------------------------------------------------------------------
        import pandas as pd

        dfk = df2.copy()
        if not dfk.empty:
            dfk["sku"] = dfk.get("sku", "").astype(str).str.strip()
            dfk = dfk[
                dfk["sku"].notna()
                & (dfk["sku"] != "")
                & (dfk["sku"] != "0")
                & (dfk["sku"].str.lower() != "none")
            ]

        if dfk.empty:
            table = self._total_only_table("profit_margin", margin_total)
            return {
                "result": self._sr(margin_total),
                "explanation": "profit_margin = (profit / sales) * 100. No valid SKUs for breakdown.",
                "table_df": table,
            }

        # Ensure numeric cols (same as UK _profit)
        for col in ["fba_fees", "selling_fees", "cost_of_unit_sold"]:
            if col not in dfk.columns:
                dfk[col] = 0.0
            dfk[col] = self._safe_num(dfk[col])

        # Centralised UK helpers
        sales_total_c, sales_by_sku, _ = uk_sales(dfk)
        tax_total_c, tax_by_sku, _ = uk_tax(dfk)
        credits_total_c, credits_by_sku, _ = uk_credits(dfk)

        # Local amazon_fee per SKU: |fba + selling|
        fee_by_sku = (
            dfk.groupby("sku", dropna=True)[["fba_fees", "selling_fees"]]
            .sum()
            .reset_index()
        )
        fee_by_sku["amazon_fee"] = (
            fee_by_sku["fba_fees"] + fee_by_sku["selling_fees"]
        ).abs()
        fee_by_sku = fee_by_sku[["sku", "amazon_fee"]]

        # COGS per SKU = |cost_of_unit_sold|
        cost_by_sku = (
            dfk.groupby("sku", dropna=True)["cost_of_unit_sold"]
            .sum()
            .abs()
            .reset_index()
            .rename(columns={"cost_of_unit_sold": "cost"})
        )

        # Helper to normalize central per-SKU dfs
        def _pick(df_, name: str) -> pd.DataFrame:
            if (
                isinstance(df_, pd.DataFrame)
                and not df_.empty
                and "__metric__" in df_.columns
                and "sku" in df_.columns
            ):
                out = df_[["sku", "__metric__"]].copy()
                out = out.rename(columns={"__metric__": name})
                out[name] = self._safe_num(out[name])
                out["sku"] = out["sku"].astype(str)
                return out
            return pd.DataFrame(columns=["sku", name])

        sales_per_sku = _pick(sales_by_sku, "sales")
        credits_per_sku = _pick(credits_by_sku, "credits")
        taxes_per_sku = _pick(tax_by_sku, "taxes")

        # Build per-SKU profit frame (same formula as UK _profit)
        per_sku = (
            pd.DataFrame({"sku": dfk["sku"].unique()})
            .merge(sales_per_sku, on="sku", how="left")
            .merge(credits_per_sku, on="sku", how="left")
            .merge(taxes_per_sku, on="sku", how="left")
            .merge(fee_by_sku, on="sku", how="left")
            .merge(cost_by_sku, on="sku", how="left")
            .fillna(0.0)
        )

        for col in ["sales", "credits", "taxes", "amazon_fee", "cost"]:
            per_sku[col] = self._safe_num(per_sku[col])

        # Profit per SKU (UK rule): |Sales| + |Credits| − Taxes − AmazonFee − |COGS|
        per_sku["profit"] = (
            per_sku["sales"].abs()
            + per_sku["credits"].abs()
            - per_sku["taxes"]
            - per_sku["amazon_fee"]
            - per_sku["cost"].abs()
        )

        # Margin per SKU
        per_sku["__metric__"] = per_sku.apply(
            lambda r: (r["profit"] / r["sales"] * 100.0) if r["sales"] else 0.0, axis=1
        )

        table = self._table_from_group_metric(
            metric_name="profit_margin",
            total_value=margin_total,
            per_sku_df=per_sku[["sku", "__metric__", "sales", "profit"]],
            sku_metric_col="__metric__",
            component_cols=["sales", "profit"],
        )

        return {
            "result": self._sr(margin_total),
            "explanation": "profit_margin = (profit / sales) * 100",
            "table_df": table,
        }


    # ---------- ASP / unit profitability / mixes ----------
    def _order_units_only(self, df: pd.DataFrame) -> pd.Series:
        n = len(df)
        if n == 0:
            return pd.Series(dtype=float)
        t = df["type"].astype(str).str.strip().str.lower() if "type" in df.columns else pd.Series([""]*n, index=df.index)
        q_raw = df["quantity"].astype(str) if "quantity" in df.columns else pd.Series([""]*n, index=df.index)
        q = pd.to_numeric(q_raw.str.replace(",", "", regex=False), errors="coerce")

        if "sku" in df.columns:
            has_sku = self._sku_mask(df)
        else:
            has_sku = pd.Series([True]*n, index=df.index)

        is_order = t.str.startswith("order")
        mask = is_order & has_sku

        units = pd.Series(0.0, index=df.index)
        q_filled = q.copy()
        q_filled[mask & q_filled.isna()] = 1.0
        units[mask] = q_filled[mask].fillna(0.0)
        return units

    def _order_units_by_sku(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty or "sku" not in df.columns:
            return pd.DataFrame(columns=["sku","quantity"])
        units = self._order_units_only(df)
        tmp = df[["sku"]].copy()
        if len(tmp) != len(units):
            units = units.reindex(tmp.index, fill_value=0.0)
        m = self._sku_mask(tmp)
        tmp = tmp.loc[m]
        units = units.loc[tmp.index]
        tmp["quantity"] = units.values
        return tmp.groupby("sku", dropna=True)["quantity"].sum().reset_index()

    def _asp(self, df: pd.DataFrame, ctx: Dict[str, Any]) -> Dict[str, Any]:
        """
        ASP (Average Selling Price):
        total      = total_sales / total_units
        per-SKU    = sales_i / units_i
        per-product= sum_i(sales_i) / sum_i(units_i) across SKUs that map to the product

        Notes:
        • Units are "orders-only" via _order_units_only (refunds excluded), matching your sales denominator.
        • Per-product ASP is computed from sums (NOT a sum of per-SKU ASPs).
        """
        is_us = self._is_us(ctx, df)

        # Region-scoped frame (SKU mask etc.)
        df2 = self._df_for_region(df, ctx)

        # ---------- 1) SALES SOURCE: US vs UK ----------
        if is_us:
            # US → local components helper
            sales_total, sales_by_sku, _, _ = self._sales_components(df, ctx)
        else:
            # UK → centralized uk_sales (same as _sales / _profit)
            dfk = df2.copy()
            dfk["sku"] = dfk.get("sku", "").astype(str).str.strip()
            dfk = dfk[
                dfk["sku"].notna()
                & (dfk["sku"] != "")
                & (dfk["sku"] != "0")
                & (dfk["sku"].str.lower() != "none")
            ]

            try:
                # uk_sales is the same helper you already use in _sales / _profit
                sales_total, sales_by_sku, _ = uk_sales(dfk)

                if not isinstance(sales_by_sku, pd.DataFrame):
                    sales_by_sku = pd.DataFrame()
            except Exception as e:
                print("[ERROR][asp][UK] centralized uk_sales failed:", repr(e))
                sales_total, sales_by_sku = 0.0, pd.DataFrame()

        # ---------- 2) TOTAL UNITS ----------
        if "type" in df2.columns:
            qty_total = float(self._order_units_only(df2).sum())
        else:
            qty_total = float(self._col(df2, "quantity").sum())

        asp_total = (sales_total / qty_total) if qty_total else 0.0

        # --- total only path (no breakdown) --------------------------
        if not ctx.get("want_breakdown"):
            table = self._total_only_table("asp", asp_total)
            return {
                "result": self._sr(asp_total),
                "explanation": "asp = (calculated sales) / (orders-only quantity)",
                "table_df": table,
            }

        # ---------- 3) BREAKDOWN (per-SKU + per-product) ----------
        try:
            # Per-SKU units
            if "type" in df2.columns:
                per_qty = self._order_units_by_sku(df2)   # sku + quantity
            else:
                per_qty = self._agg_by(df2, "sku", ["quantity"])

            sbs = sales_by_sku.copy()

            # --- Find SKU column in sales_by_sku --------------------
            sku_col = None
            for c in sbs.columns:
                if str(c).lower() in {"sku", "asin", "product_sku"}:
                    sku_col = c
                    break

            # handle case where sku is index
            if sku_col is None and sbs.index.name and str(sbs.index.name).lower() in {"sku", "asin", "product_sku"}:
                sbs = sbs.reset_index()
                sku_col = sbs.columns[0]

            if sku_col is None:
                print("[DEBUG][asp] no SKU-like column in sales_by_sku → breakdown skipped")
                table = self._total_only_table("asp", asp_total)
                return {
                    "result": self._sr(asp_total),
                    "explanation": "asp = (calculated sales) / (orders-only quantity)",
                    "table_df": table,
                }

            # --- Find numeric sales metric column -------------------
            metric_col = None
            for c in sbs.columns:
                if c == sku_col:
                    continue
                if pd.api.types.is_numeric_dtype(sbs[c]):
                    metric_col = c
                    break

            if metric_col is None:
                raise ValueError(f"[ASP] No numeric metric column found in sales_by_sku columns={list(sbs.columns)}")

            # Normalize schema: sku + __metric__
            sbs = sbs.rename(columns={sku_col: "sku", metric_col: "__metric__"})

            # Merge sales + quantity per SKU
            per_sku = (
                sbs[["sku", "__metric__"]]
                .rename(columns={"__metric__": "sales"})
                .merge(per_qty, on="sku", how="left")
                .fillna(0.0)
            )

            per_sku["quantity"] = pd.to_numeric(per_sku.get("quantity", 0.0), errors="coerce").fillna(0.0)

            # ASP per SKU
            per_sku["__metric__"] = per_sku.apply(
                lambda r: (r["sales"] / r["quantity"]) if r.get("quantity", 0.0) else 0.0,
                axis=1,
            )

            # ----- Per-PRODUCT ASP via SKU→product map -----------
            sku2prod = self._sku_to_product(df2)

            if not sku2prod.empty and "product_name" in sku2prod.columns:
                per_prod_base = (
                    per_sku.merge(sku2prod, on="sku", how="left")
                    .groupby("product_name", dropna=True)[["sales", "quantity"]]
                    .sum()
                    .reset_index()
                )
                per_prod_base["__metric__"] = per_prod_base.apply(
                    lambda r: (r["sales"] / r["quantity"]) if r.get("quantity", 0.0) else 0.0,
                    axis=1,
                )
                per_prod = per_prod_base[["product_name", "__metric__", "sales", "quantity"]]
            else:
                per_prod = pd.DataFrame(columns=["product_name", "__metric__", "sales", "quantity"])

            # Final table
            table = self._final_table(
                "asp",
                asp_total,
                per_sku[["sku", "__metric__", "sales", "quantity"]],
                per_prod[["product_name", "__metric__", "sales", "quantity"]],
                component_cols=["sales", "quantity"],
            )

            return {
                "result": self._sr(asp_total),
                "explanation": "asp = (calculated sales) / (orders-only quantity)",
                "table_df": table,
            }

        except Exception as e:
            print("[DEBUG][asp] breakdown failed, falling back to total-only:", repr(e))
            table = self._total_only_table("asp", asp_total)
            return {
                "result": self._sr(asp_total),
                "explanation": "asp = (calculated sales) / (orders-only quantity)",
                "table_df": table,
            }



    def _unit_profitability(self, df: pd.DataFrame, ctx: Dict[str, Any]) -> Dict[str, Any]:
        """
        Unit profitability (PPU):
        total      = total_profit / total_units
        per-SKU    = profit_i / units_i
        per-product= sum_i(profit_i) / sum_i(units_i) across SKUs that map to the product

        Notes:
        • Profit is computed via self._profit (which already handles US vs UK + centralized logic).
        • Units are "orders-only" via _order_units_only (refunds excluded).
        """
        # --- total PPU (US + UK both) -------------------------------------------
        profit_total = self._profit(df, {**ctx, "want_breakdown": False}).get("result") or 0.0
        df2 = self._df_for_region(df, ctx)

        if "type" in df2.columns:
            qty_total = float(self._order_units_only(df2).sum())
        else:
            qty_total = float(self._col(df2, "quantity").sum())

        ppu_total = (profit_total / qty_total) if qty_total else 0.0

        # No breakdown requested → just total row
        if not ctx.get("want_breakdown"):
            table = self._total_only_table("unit_profitability", ppu_total)
            return {
                "result": self._sr(ppu_total),
                "explanation": "unit_profitability = profit / (orders-only quantity)",
                "table_df": table,
            }

        is_us = self._is_us(ctx, df)

        # ----------------------------------------------------------------------
        # 🇺🇸 US BREAKDOWN  (same style as your previous implementation)
        # ----------------------------------------------------------------------
        if is_us:
            # Components based on US helpers
            _, sales_by_sku, _, _ = self._sales_components(df, ctx)
            _, credits_by_sku, _, _ = self._credits_components(df, ctx)
            _, tax_by_sku, _, _ = self._tax_components(df, ctx)

            per_qty = (
                self._order_units_by_sku(df2)
                if "type" in df2.columns
                else self._agg_by(df2, "sku", ["quantity"])
            )
            f = self._agg_by(df2, "sku", ["fba_fees", "selling_fees", "cost_of_unit_sold"])

            per_sku = (
                sales_by_sku[["sku", "__metric__"]].rename(columns={"__metric__": "sales"})
                .merge(
                    credits_by_sku[["sku", "__metric__"]].rename(
                        columns={"__metric__": "credits"}
                    ),
                    on="sku",
                    how="outer",
                )
                .merge(
                    tax_by_sku[["sku", "__metric__"]].rename(columns={"__metric__": "taxes"}),
                    on="sku",
                    how="outer",
                )
                .merge(f, on="sku", how="outer")
                .merge(per_qty, on="sku", how="outer")
                .fillna(0.0)
            )

            # optional 'other' for US
            if "other" in df2.columns:
                other_by_sku = self._agg_by(df2, "sku", ["other"])
            else:
                other_by_sku = pd.DataFrame(columns=["sku", "other"])

            per_sku = per_sku.merge(other_by_sku, on="sku", how="left").fillna(0.0)

            # Profit per SKU (US formula)
            per_sku["profit"] = (
                per_sku["sales"]
                + per_sku["credits"]
                - per_sku["taxes"]
                - per_sku.get("fba_fees", 0.0)
                - per_sku.get("selling_fees", 0.0)
                - per_sku.get("other", 0.0)
                - per_sku.get("cost_of_unit_sold", 0.0)
            )

            # Ensure quantity numeric
            per_sku["quantity"] = pd.to_numeric(
                per_sku.get("quantity", 0.0), errors="coerce"
            ).fillna(0.0)

            # PPU per SKU
            per_sku["__metric__"] = per_sku.apply(
                lambda r: (r["profit"] / r["quantity"]) if r["quantity"] else 0.0, axis=1
            )

            table = self._table_from_group_metric(
                metric_name="unit_profitability",
                total_value=ppu_total,
                per_sku_df=per_sku[["sku", "__metric__", "profit", "quantity"]],
                sku_metric_col="__metric__",
                component_cols=["profit", "quantity"],
            )

            return {
                "result": self._sr(ppu_total),
                "explanation": "unit_profitability = profit / (orders-only quantity)",
                "table_df": table,
            }

        # ----------------------------------------------------------------------
        # 🇬🇧 UK BREAKDOWN (centralized, aligned with UK _profit logic)
        # ----------------------------------------------------------------------
        # Region/SKU-scoped view (same as UK _profit)
        dfk = df2.copy()
        if not dfk.empty:
            dfk["sku"] = dfk.get("sku", "").astype(str).str.strip()
            dfk = dfk[
                dfk["sku"].notna()
                & (dfk["sku"] != "")
                & (dfk["sku"] != "0")
                & (dfk["sku"].str.lower() != "none")
            ]

        if dfk.empty:
            # no valid SKUs → total-only
            table = self._total_only_table("unit_profitability", ppu_total)
            return {
                "result": self._sr(ppu_total),
                "explanation": "unit_profitability = profit / (orders-only quantity). No valid SKUs for breakdown.",
                "table_df": table,
            }

        # Ensure numeric columns exist & are numeric (same as UK _profit)
        for col in ["fba_fees", "selling_fees", "cost_of_unit_sold"]:
            if col not in dfk.columns:
                dfk[col] = 0.0
            dfk[col] = self._safe_num(dfk[col])

        # --- Centralized UK component helpers (same as UK _profit) -------------
        sales_total_c, sales_by_sku, _ = uk_sales(dfk)
        tax_total_c, tax_by_sku, _ = uk_tax(dfk)
        credits_total_c, credits_by_sku, _ = uk_credits(dfk)

        # Local Amazon fee (same rule as UK _profit):
        # amazon_fee_total = abs(sum(fba_fees + selling_fees))
        fee_by_sku = (
            dfk.groupby("sku", dropna=True)[["fba_fees", "selling_fees"]]
            .sum()
            .reset_index()
        )
        fee_by_sku["amazon_fee"] = (
            fee_by_sku["fba_fees"] + fee_by_sku["selling_fees"]
        ).abs()
        fee_by_sku = fee_by_sku[["sku", "amazon_fee"]]

        # COGS per SKU (|cost_of_unit_sold|)
        cost_by_sku = (
            dfk.groupby("sku", dropna=True)["cost_of_unit_sold"]
            .sum()
            .abs()
            .reset_index()
            .rename(columns={"cost_of_unit_sold": "cost"})
        )

        # Units per SKU (orders-only over region-scoped df2)
        per_qty = (
            self._order_units_by_sku(df2)
            if "type" in df2.columns
            else self._agg_by(df2, "sku", ["quantity"])
        )

        # Helper to normalize centralized per-SKU frames (same idea as _profit)
        def _pick(df_, name: str) -> pd.DataFrame:
            import pandas as pd

            if (
                isinstance(df_, pd.DataFrame)
                and not df_.empty
                and "__metric__" in df_.columns
                and "sku" in df_.columns
            ):
                out = df_[["sku", "__metric__"]].copy()
                out = out.rename(columns={"__metric__": name})
                out[name] = self._safe_num(out[name])
                out["sku"] = out["sku"].astype(str)
                return out
            return pd.DataFrame(columns=["sku", name])

        sales_per_sku = _pick(sales_by_sku, "sales")
        credits_per_sku = _pick(credits_by_sku, "credits")
        taxes_per_sku = _pick(tax_by_sku, "taxes")

        # --- Build per-SKU profit frame (same formula as UK _profit) ----------
        import pandas as pd

        per_sku = (
            pd.DataFrame({"sku": dfk["sku"].unique()})
            .merge(sales_per_sku, on="sku", how="left")
            .merge(credits_per_sku, on="sku", how="left")
            .merge(taxes_per_sku, on="sku", how="left")
            .merge(fee_by_sku, on="sku", how="left")
            .merge(cost_by_sku, on="sku", how="left")
            .merge(per_qty, on="sku", how="left")
            .fillna(0.0)
        )

        for col in ["sales", "credits", "taxes", "amazon_fee", "cost"]:
            per_sku[col] = self._safe_num(per_sku[col])

        # Profit per SKU (UK _profit style):
        # profit = |Sales| + |Credits| − Taxes − AmazonFee − |COGS|
        per_sku["profit"] = (
            per_sku["sales"].abs()
            + per_sku["credits"].abs()
            - per_sku["taxes"]
            - per_sku["amazon_fee"]
            - per_sku["cost"].abs()
        )

        # Ensure quantity numeric
        per_sku["quantity"] = pd.to_numeric(
            per_sku.get("quantity", 0.0), errors="coerce"
        ).fillna(0.0)

        # PPU per SKU
        per_sku["__metric__"] = per_sku.apply(
            lambda r: (r["profit"] / r["quantity"]) if r["quantity"] else 0.0, axis=1
        )

        # Final table = TOTAL + per-SKU + per-product (sums)
        table = self._table_from_group_metric(
            metric_name="unit_profitability",
            total_value=ppu_total,
            per_sku_df=per_sku[["sku", "__metric__", "profit", "quantity"]],
            sku_metric_col="__metric__",
            component_cols=["profit", "quantity"],
        )

        return {
            "result": self._sr(ppu_total),
            "explanation": "unit_profitability = profit / (orders-only quantity)",
            "table_df": table,
        }


    
    def _sales_mix(self, df: pd.DataFrame, ctx: Dict[str, Any]) -> Dict[str, Any]:
        """
        Sales mix = (sales of item in the selected period / total sales in the selected period) * 100
        - Returns numeric `result` equal to the selected item's share (ctx['target_product'] or ctx['target_sku'])
        - Table shows % by product (preferred) or by SKU if product names unavailable
        - No TOTAL row
        """
        import pandas as pd, re

        # --- targets (fallbacks preserved) ---
        target_product = (ctx.get("target_product") or ctx.get("product") or "").strip()
        target_sku     = (ctx.get("target_sku") or ctx.get("sku") or "").strip()

        # --- slice to requested period (month/year OR date_from/date_to over date_time) ---
        dfp = df.copy()
        try:
            m_raw, y_raw = ctx.get("month"), ctx.get("year")
            if m_raw is not None and y_raw is not None and "month" in dfp.columns and "year" in dfp.columns:
                m = self._month_to_int(m_raw)
                y = self._year_int(y_raw)
                if m and y:
                    mask = (dfp["month"].apply(self._month_to_int) == m) & (dfp["year"].apply(self._year_int) == y)
                    dfp = dfp.loc[mask]

            date_from, date_to = ctx.get("date_from"), ctx.get("date_to")
            if "date_time" in dfp.columns and (date_from or date_to):
                dt = pd.to_datetime(dfp["date_time"], errors="coerce")
                mask = pd.Series(True, index=dfp.index)
                if date_from:
                    mask &= dt >= pd.to_datetime(date_from)
                if date_to:
                    mask &= dt <= pd.to_datetime(date_to)
                dfp = dfp.loc[mask]
        except Exception as _e:
            print("[TRACE][FE][sales_mix] period slice failed:", _e)

        print(
            f"[TRACE][FE][sales_mix] rows={len(dfp)} country={ctx.get('country')} "
            f"target_product={target_product!r} target_sku={target_sku!r}"
        )

        # --- build components from the *period slice* ---
        is_us = self._is_us(ctx, dfp)

        sales_by_product = pd.DataFrame()
        sales_by_sku     = pd.DataFrame()

        if is_us:
            # US: keep existing components logic
            _, sales_by_sku, sales_by_product, _ = self._sales_components(dfp, ctx)
        else:
            # UK: use centralized _sales (which internally calls uk_sales)
            try:
                # force breakdown and ignore any external group_by like "month"
                uk_ctx = {**ctx, "want_breakdown": True, "group_by": None}
                payload = self._sales(dfp, uk_ctx) or {}
                table = payload.get("table_df")
                if isinstance(table, pd.DataFrame) and not table.empty:
                    # pick product-level sales rows
                    prod_rows = table[
                        (table.get("metric") == "sales") &
                        (table.get("level") == "product")
                    ].copy()

                    if not prod_rows.empty:
                        prod_rows = prod_rows.rename(
                            columns={"key": "product_name", "result": "__metric__"}
                        )
                        sales_by_product = prod_rows[["product_name", "__metric__"]]
            except Exception as e:
                print("[TRACE][FE][sales_mix] UK per-product from _sales failed:", e)

        if (sales_by_product is None or sales_by_product.empty) and (sales_by_sku is None or sales_by_sku.empty):
            print("[TRACE][FE][sales_mix] no sales rows in selected period → returning early")
            return {
                "result": 0.0,
                "explanation": "No sales data in the selected period.",
                "table_df": pd.DataFrame(columns=["metric", "level", "key", "result"]),
            }

        sel_value = 0.0
        table = pd.DataFrame()

        # ---- Prefer product-level breakdown ----
        if sales_by_product is not None and not sales_by_product.empty:
            per_prod = sales_by_product.copy()
            denom = float(per_prod["__metric__"].sum())
            if denom == 0:
                print("[TRACE][FE][sales_mix] denom(product)=0 → returning early")
                return {
                    "result": 0.0,
                    "explanation": "No sales data in the selected period.",
                    "table_df": pd.DataFrame(columns=["metric", "level", "key", "result"]),
                }

            per_prod["result"] = per_prod["__metric__"] / denom * 100.0
            per_prod["metric"] = "sales_mix"
            per_prod["level"]  = "product"
            per_prod["key"]    = per_prod["product_name"].astype(str)
            table = per_prod[["metric", "level", "key", "result"]]

            print(f"[TRACE][FE][sales_mix] using product breakdown target_product={target_product!r}")
            if target_product:
                match = per_prod.loc[
                    per_prod["product_name"].str.lower().str.contains(re.escape(target_product.lower()))
                ]
                if not match.empty:
                    sel_value = float(match["result"].iloc[0])
                    print(f"[TRACE][FE][sales_mix] matched product '{target_product}' → {sel_value:.2f}%")
                else:
                    print(f"[TRACE][FE][sales_mix] product '{target_product}' not found")

        # ---- Fallback: SKU-level breakdown (mostly US) ----
        elif sales_by_sku is not None and not sales_by_sku.empty:
            per_sku = sales_by_sku.copy()
            denom = float(per_sku["__metric__"].sum())
            if denom == 0:
                print("[TRACE][FE][sales_mix] denom(sku)=0 → returning early")
                return {
                    "result": 0.0,
                    "explanation": "No sales data in the selected period.",
                    "table_df": pd.DataFrame(columns=["metric", "level", "key", "result"]),
                }

            per_sku["result"] = per_sku["__metric__"] / denom * 100.0
            per_sku["metric"] = "sales_mix"
            per_sku["level"]  = "sku"
            per_sku["key"]    = per_sku["sku"].astype(str)
            table = per_sku[["metric", "level", "key", "result"]]

            print(f"[TRACE][FE][sales_mix] using SKU breakdown target_sku={target_sku!r}")
            if target_sku:
                match = per_sku.loc[per_sku["sku"].astype(str).str.lower() == target_sku.lower()]
                if not match.empty:
                    sel_value = float(match["result"].iloc[0])
                    print(f"[TRACE][FE][sales_mix] matched SKU '{target_sku}' → {sel_value:.2f}%")
                else:
                    print(f"[TRACE][FE][sales_mix] SKU '{target_sku}' not found")

        print(f"[TRACE][FE][sales_mix] final result={sel_value:.2f}%")
        return {
            "result": self._sr(sel_value),
            "explanation": "sales_mix = (sales of item in the selected period / total sales in the selected period) × 100",
            "table_df": self._sanitize_df(table),
        }




    def _profit_mix(self, df: pd.DataFrame, ctx: Dict[str, Any]) -> Dict[str, Any]:
        """
        Profit mix = (profit of product in the selected period / total profit in the selected period) * 100
        """
        # --- targets (fallbacks preserved) ---
        target_product = (ctx.get("target_product") or ctx.get("product") or "").strip().lower()
        target_sku     = (ctx.get("target_sku") or ctx.get("sku") or "").strip().lower()

        # --- slice to requested period (month/year OR date_from/date_to over date_time) ---
        dfp = df.copy()
        try:
            m_raw, y_raw = ctx.get("month"), ctx.get("year")
            if m_raw is not None and y_raw is not None and "month" in dfp.columns and "year" in dfp.columns:
                m = self._month_to_int(m_raw)
                y = self._year_int(y_raw)
                if m and y:
                    mask = (dfp["month"].apply(self._month_to_int) == m) & (dfp["year"].apply(self._year_int) == y)
                    dfp = dfp.loc[mask]

            date_from, date_to = ctx.get("date_from"), ctx.get("date_to")
            if "date_time" in dfp.columns and (date_from or date_to):
                dt = pd.to_datetime(dfp["date_time"], errors="coerce")
                mask = pd.Series(True, index=dfp.index)
                if date_from:
                    mask &= dt >= pd.to_datetime(date_from)
                if date_to:
                    mask &= dt <= pd.to_datetime(date_to)
                dfp = dfp.loc[mask]
        except Exception as _e:
            print("[TRACE][FE][profit_mix] period slice failed:", _e)

        print(f"[TRACE][FE][profit_mix] rows={len(dfp)} country={ctx.get('country')} "
            f"target_product={target_product!r} target_sku={target_sku!r}")

        if "product_name" not in dfp.columns:
            print("[TRACE][FE][profit_mix] no product_name column")
            return {
                "result": 0.0,
                "explanation": "No product_name column in data.",
                "table_df": self._total_only_table("profit_mix", 0.0),
            }

        # --- compute profit per product within the *period slice* ---
        grouped = []
        for product, g in dfp.groupby("product_name"):
            payload = self._profit(g, {"country": ctx.get("country"), "want_breakdown": False})
            p = float(payload.get("result") or 0.0)
            if p != 0.0:
                grouped.append({"product_name": product, "profit": p})

        print(f"[TRACE][FE][profit_mix] grouped_products={len(grouped)}")
        if not grouped:
            print("[TRACE][FE][profit_mix] grouped empty → returning 0")
            return {
                "result": 0.0,
                "explanation": "No product-level profit breakdown for the selected period.",
                "table_df": self._total_only_table("profit_mix", 0.0),
            }

        per_prod = pd.DataFrame(grouped)
        total_profit = float(per_prod["profit"].sum())
        print(f"[TRACE][FE][profit_mix] total_profit(period)={total_profit}")

        if total_profit == 0:
            return {
                "result": 0.0,
                "explanation": "No profit data in the selected period.",
                "table_df": self._total_only_table("profit_mix", 0.0),
            }

        per_prod["profit_mix"] = per_prod["profit"] / total_profit * 100.0
        table = per_prod.rename(columns={"product_name": "key", "profit_mix": "result"})
        table["metric"] = "profit_mix"
        table["level"]  = "product"
        table = table[["metric", "level", "key", "result"]]

        # --- select value for target (product or sku→product) ---
        selected_value = 0.0
        print(f"[TRACE][FE][profit_mix] target_product={target_product!r} target_sku={target_sku!r}")

        if target_product:
            match = table[table["key"].str.lower().str.contains(target_product)]
            if not match.empty:
                selected_value = float(match["result"].iloc[0])
                print(f"[TRACE][FE][profit_mix] matched product '{target_product}' → {selected_value:.2f}%")
            else:
                print(f"[TRACE][FE][profit_mix] product '{target_product}' not found")
        elif target_sku and "sku" in dfp.columns:
            sku_map = dfp[["sku", "product_name"]].drop_duplicates()
            match_prod = sku_map[sku_map["sku"].astype(str).str.lower() == target_sku]
            if not match_prod.empty:
                prod_name = match_prod["product_name"].iloc[0].lower()
                match = table[table["key"].str.lower() == prod_name]
                if not match.empty:
                    selected_value = float(match["result"].iloc[0])
                    print(f"[TRACE][FE][profit_mix] matched SKU '{target_sku}' → product '{prod_name}' → {selected_value:.2f}%")
                else:
                    print(f"[TRACE][FE][profit_mix] SKU '{target_sku}' mapped product not found in table")

        print(f"[TRACE][FE][profit_mix] final result={selected_value:.2f}%")
        return {
            "result": self._sr(selected_value),
            "explanation": "Profit mix = (profit of product in the selected period / total profit in the selected period) × 100",
            "table_df": self._sanitize_df(table),
        }








    def _quantity_sold(self, df: pd.DataFrame, ctx: Dict[str, Any]) -> Dict[str, Any]:
        """
        Ordered units only:
        • Count rows whose type starts with 'order' (case-insensitive).
        • Default missing order quantity to 1 (same rule as _order_units_only).
        • Refunds/adjustments are NOT subtracted.
        • Total uses region-scoped rows; breakdowns only use rows with a valid SKU.
        """
        df2 = self._df_for_region(df, ctx)
        if df2.empty:
            table = self._total_only_table("quantity_sold", 0.0)
            return {"result": 0.0, "explanation": "quantity_sold = orders only (no refunds/adjustments). No rows.", "table_df": table}

        # Orders-only units (uses 'order' filter + missing qty → 1)
        units = self._order_units_only(df2)
        total_units = float(units.sum())

        if not ctx.get("want_breakdown"):
            table = self._total_only_table("quantity_sold", total_units)
        else:
            # Per-SKU with strict SKU presence
            if "sku" in df2.columns:
                sku_frame = df2[["sku"]].copy()
                m = self._sku_mask(sku_frame)
                idx = sku_frame.index[m]
                per_sku = (
                    pd.DataFrame({"sku": df2.loc[idx, "sku"], "__metric__": units.loc[idx]})
                    .groupby("sku", dropna=True)["__metric__"]
                    .sum()
                    .reset_index()
                )
            else:
                per_sku = pd.DataFrame(columns=["sku", "__metric__"])

            # Per-product rollup
            sku2prod = self._sku_to_product(df2)
            per_prod = per_sku.merge(sku2prod, on="sku", how="left")
            if "product_name" in per_prod.columns and not per_prod.empty:
                per_prod = per_prod.groupby("product_name", dropna=True)["__metric__"].sum().reset_index()
            else:
                per_prod = pd.DataFrame(columns=["product_name", "__metric__"])

            table = self._final_table("quantity_sold", total_units, per_sku, per_prod, component_cols=[])

        return {"result": self._sr(total_units), "explanation": "quantity_sold = orders only (missing qty→1). Refunds/adjustments NOT subtracted.",
            "table_df": table,
        }


    def _refunds(self, df: pd.DataFrame, ctx: Dict[str, Any]) -> Dict[str, Any]:
        """
        Count refund events and (optionally) show refund units.
        - Detect refunds by type == 'refund' (case-insensitive). If 'type' missing, fallback to description contains 'refund'.
        - Result: number of refund rows (lines). Component 'refund_units' sums abs(quantity) when available.
        - Breakdown: per SKU and per product (if want_breakdown=True).
        """
        df2 = self._df_for_region(df, ctx)
        if df2.empty:
            return {"result": 0.0, "explanation": "No refund rows.", "table_df": self._total_only_table("refunds", 0.0)}

        t = df2["type"].astype(str).str.strip().str.lower() if "type" in df2.columns else pd.Series([""]*len(df2), index=df2.index)
        desc = df2["description"].astype(str).str.lower() if "description" in df2.columns else pd.Series([""]*len(df2), index=df2.index)

        mask = t.eq("refund") | desc.str.contains("refund", na=False)
        r = df2.loc[mask].copy()
        if r.empty:
            return {"result": 0.0, "explanation": "No refund rows.", "table_df": self._total_only_table("refunds", 0.0)}

        # Components
        qty = pd.to_numeric(r["quantity"], errors="coerce").fillna(0.0) if "quantity" in r.columns else pd.Series([0.0]*len(r), index=r.index)
        refund_units_total = float(qty.abs().sum())
        refund_lines_total = float(len(r))

        # Per-SKU breakdown (strict SKU presence)
        if "sku" in r.columns:
            tmp = r[["sku"]].copy()
            m = self._sku_mask(tmp)
            r_sku = r.loc[m].copy()
            if not r_sku.empty:
                per_sku = (
                    r_sku.assign(refund_units=(pd.to_numeric(r_sku.get("quantity", 0), errors="coerce").fillna(0.0).abs()))
                        .groupby("sku", dropna=True)
                        .agg(refund_lines=("sku", "size"), refund_units=("refund_units", "sum"))
                        .reset_index()
                )
                per_sku["__metric__"] = per_sku["refund_lines"]
            else:
                per_sku = pd.DataFrame(columns=["sku","refund_lines","refund_units","__metric__"])
        else:
            per_sku = pd.DataFrame(columns=["sku","refund_lines","refund_units","__metric__"])

        # Per-product rollup
        sku2prod = self._sku_to_product(r)
        if not per_sku.empty and not sku2prod.empty:
            per_prod = (
                per_sku.merge(sku2prod, on="sku", how="left")
                    .groupby("product_name", dropna=True)[["refund_lines","refund_units","__metric__"]]
                    .sum()
                    .reset_index()
            )
        else:
            per_prod = pd.DataFrame(columns=["product_name","__metric__","refund_lines","refund_units"])

        comps = ["refund_units"]  # show units as component
        table = self._final_table("refunds", refund_lines_total, per_sku[["sku","__metric__","refund_units"]], per_prod[["product_name","__metric__","refund_units"]], comps)

        expl = "refunds = count(rows where type == 'refund' or description contains 'refund'); component refund_units = sum(abs(quantity))."
        return {"result": self._sr(refund_lines_total), "explanation": expl, "table_df": table}

    def _amazon_fees(self, df: pd.DataFrame, ctx: Dict[str, Any]) -> Dict[str, Any]:
        """
        Amazon fees metric.

        US:
        TOTAL = |fba_fees| + |selling_fees| - |other|  (from _amazon_fee_components)
        Breakdown = per-SKU / per-product from _amazon_fee_components

        UK:
        TOTAL = abs( sum_over_valid_sku( fba_fees + selling_fees ) )
        Per-SKU amazon_fee_i = abs( sum_i( fba_fees + selling_fees ) )
        Per-product = sum of per-SKU amazon_fee_i mapped via product_name
        """
        is_us = self._is_us(ctx, df)

        # -------------------- 🇺🇸 US → existing behaviour --------------------
        if is_us:
            total, per_sku, per_prod, comps = self._amazon_fee_components(df, ctx)
            expl = (
                "US amazon_fees = |fba_fees| + |selling_fees| - |other| "
                "(TOTAL from all rows; breakdown from region-scoped SKU rows)"
            )

            if ctx.get("want_breakdown"):
                table = self._final_table("amazon_fees", total, per_sku, per_prod, comps)
            else:
                table = self._total_only_table("amazon_fees", total)

            return {
                "result": self._sr(total),
                "explanation": expl,
                "table_df": table,
            }

        # -------------------- 🇬🇧 UK → same style as UK _profit() --------------------
        import pandas as pd

        df2 = self._df_for_region(df, ctx)
        if df2 is None or df2.empty or "sku" not in df2.columns:
            total = 0.0
            table = self._total_only_table("amazon_fees", total)
            expl = (
                "UK amazon_fees = abs(sum(fba_fees + selling_fees)) on rows with a valid SKU; "
                "no valid SKU rows found."
            )
            return {
                "result": self._sr(total),
                "explanation": expl,
                "table_df": table,
            }

        # Strict SKU filtering (same mask as elsewhere)
        w = df2.copy()
        w["sku"] = w["sku"].astype(str)
        m = self._sku_mask(w)
        w = w.loc[m]
        if w.empty:
            total = 0.0
            table = self._total_only_table("amazon_fees", total)
            expl = (
                "UK amazon_fees = abs(sum(fba_fees + selling_fees)) on rows with a valid SKU; "
                "no valid SKU rows after filtering."
            )
            return {
                "result": self._sr(total),
                "explanation": expl,
                "table_df": table,
            }

        # Ensure numeric columns exist
        for col in ["fba_fees", "selling_fees"]:
            if col not in w.columns:
                w[col] = 0.0
            w[col] = self._safe_num(w[col])

        # Per-SKU amazon_fee = abs(sum(fba_fees + selling_fees))
        per_sku = (
            w.groupby("sku", dropna=True)[["fba_fees", "selling_fees"]]
            .sum()
            .reset_index()
        )
        per_sku["amazon_fee"] = (per_sku["fba_fees"] + per_sku["selling_fees"]).abs()
        per_sku["__metric__"] = per_sku["amazon_fee"]

        total = float(per_sku["amazon_fee"].sum())
        comps = ["amazon_fee"]

        # Per-product rollup via sku → product_name
        sku2prod = self._sku_to_product(w)
        if not sku2prod.empty and "product_name" in sku2prod.columns:
            per_prod = (
                per_sku.merge(sku2prod, on="sku", how="left")
                    .groupby("product_name", dropna=True)[["amazon_fee", "__metric__"]]
                    .sum()
                    .reset_index()
            )
        else:
            per_prod = pd.DataFrame(columns=["product_name", "__metric__", "amazon_fee"])

        if ctx.get("want_breakdown"):
            table = self._final_table(
                "amazon_fees",
                total,
                per_sku[["sku", "__metric__", "amazon_fee"]],
                per_prod[["product_name", "__metric__", "amazon_fee"]] if not per_prod.empty else per_prod,
                comps,
            )
        else:
            table = self._total_only_table("amazon_fees", total)

        expl = (
            "UK amazon_fees = abs(sum(fba_fees + selling_fees)) over rows with a valid SKU; "
            "per-SKU amazon_fee = abs(sum_sku(fba_fees + selling_fees))."
        )
        return {
            "result": self._sr(total),
            "explanation": expl,
            "table_df": table,
        }

    
    def _fba_fees(self, df: pd.DataFrame, ctx: Dict[str, Any]) -> Dict[str, Any]:
        """
        fba_fees = sum(fba_fees) over rows with a valid SKU (both US & UK).
        """
        total, per_sku, per_prod, comps = self._simple_fee_components_sku_only(df, col="fba_fees")
        if ctx.get("want_breakdown"):
            table = self._final_table("fba_fees", total, per_sku, per_prod, comps)
        else:
            table = self._total_only_table("fba_fees", total)
        return {"result": self._sr(total), "explanation": "fba_fees = sum(fba_fees) on rows with a valid SKU",
            "table_df": table,
        }


    def _selling_fees(self, df: pd.DataFrame, ctx: Dict[str, Any]) -> Dict[str, Any]:
        is_us = self._is_us(ctx, df)

        # ---------- US branch ----------
        if is_us:
            total, per_sku, per_prod, comps = self._simple_fee_components_sku_only(df, col="selling_fees")
            expl = "selling_fees = sum(selling_fees) on rows with a valid SKU"
            table = (
                self._final_table("selling_fees", total, per_sku, per_prod, comps)
                if ctx.get("want_breakdown")
                else self._total_only_table("selling_fees", total)
            )
            return {"result": self._sr(total), "explanation": expl, "table_df": table}

        # ---------- UK branch (local logic like profit) ----------
        df2 = self._df_for_region(df, ctx)
        if df2 is None or df2.empty or "sku" not in df2.columns:
            total = 0.0
            table = self._total_only_table("selling_fees", total)
            return {"result": self._sr(total), "explanation": "UK selling_fees = sum(selling_fees) for valid SKUs", "table_df": table}

        w = df2.copy()
        w["sku"] = w["sku"].astype(str)
        m = self._sku_mask(w)
        w = w.loc[m]

        if w.empty:
            total = 0.0
            table = self._total_only_table("selling_fees", total)
            return {"result": self._sr(total), "explanation": "UK selling_fees = sum(selling_fees) for valid SKUs", "table_df": table}

        # clean numeric
        w["selling_fees"] = self._safe_num(w.get("selling_fees", 0))

        # Per-SKU aggregation
        per_sku = w.groupby("sku", dropna=True)["selling_fees"].sum().reset_index()
        per_sku["__metric__"] = per_sku["selling_fees"]

        total = float(per_sku["selling_fees"].sum())

        # Productwise rollup
        sku2prod = self._sku_to_product(w)
        if not sku2prod.empty and "product_name" in sku2prod.columns:
            per_prod = (
                per_sku.merge(sku2prod, on="sku", how="left")
                .groupby("product_name", dropna=True)[["selling_fees","__metric__"]]
                .sum()
                .reset_index()
            )
        else:
            per_prod = pd.DataFrame(columns=["product_name","__metric__","selling_fees"])

        expl = "UK selling_fees = sum(selling_fees) over rows with a valid SKU"

        if ctx.get("want_breakdown"):
            table = self._final_table(
                "selling_fees",
                total,
                per_sku[["sku","__metric__","selling_fees"]],
                per_prod[["product_name","__metric__","selling_fees"]],
                ["selling_fees"],
            )
        else:
            table = self._total_only_table("selling_fees", total)

        return {"result": self._sr(total), "explanation": expl, "table_df": table}

    
    @staticmethod
    def _ensure_period_month(df: pd.DataFrame) -> pd.DataFrame:
        """Create a monthly period column '__period__' (UTC timestamp at month start)."""
        out = df.copy()

        # Helper that tolerates ints/strings/None and both "Jun"/"June"/"6"/"06"
        def _m_to_int(x):
            s = str(x).strip() if pd.notna(x) else ""
            # numeric?
            try:
                i = int(s)
                return i if 1 <= i <= 12 else np.nan
            except Exception:
                pass
            # text month -> first 3 letters
            s3 = s[:3].lower()
            try:
                return ["","jan","feb","mar","apr","may","jun","jul","aug","sep","oct","nov","dec"].index(s3)
            except ValueError:
                return np.nan

        if "month" in out.columns and "year" in out.columns:
            mnum = out["month"].apply(_m_to_int)
            yint = pd.to_numeric(out["year"], errors="coerce")
            out["__period__"] = pd.to_datetime(
                dict(year=yint, month=mnum, day=1),
                errors="coerce", utc=True
            )
        elif "date_time" in out.columns:
            # If already tz-aware, don't re-localize; convert to UTC then floor to month
            dtc = pd.to_datetime(out["date_time"], errors="coerce", utc=False)
            dtc = dtc.dt.tz_localize("UTC", nonexistent="NaT", ambiguous="NaT") if dtc.dt.tz is None else dtc.dt.tz_convert("UTC")
            out["__period__"] = dtc.dt.to_period("M").dt.to_timestamp().dt.tz_localize("UTC")
        else:
            out["__period__"] = pd.NaT
            # 🧩 ADD THIS DEBUG SECTION (Step 5)
        print(
            f"[TRACE][FE][period] built __period__: total_rows={len(out)}, "
            f"null_periods={int(out['__period__'].isna().sum()) if '__period__' in out.columns else 'n/a'}"
        )
        if "__period__" in out.columns:
            uniq = out["__period__"].dropna().unique()
            preview = ", ".join(map(str, sorted(uniq)[:5]))
            print(f"[TRACE][FE][period] sample_periods=[{preview}]{'...' if len(uniq)>5 else ''}")    

        return out


    def rank_groups(
    self,
    df: pd.DataFrame,
    ctx: Dict[str, Any],
    *,
    metric_name: str,
    group_by: str,
    top_k: int = 1,
    ascending: bool = False
) -> pd.DataFrame:
        """
        Generic ranking over groups (month/product/sku/country).
        Uses existing metric evaluators to ensure the metric logic stays consistent.
        Returns a DataFrame with columns: [group, result] (plus nice labels for month).
        """
        # 🧩 Step 9: initial trace
        print(f"[TRACE][FE][rank] metric={metric_name} group_by={group_by} "
            f"rows={len(df)} ctx=({self._ctx_trace(ctx)})")

        if df is None or df.empty:
            print("[TRACE][FE][rank] input empty → returning")
            return pd.DataFrame({"_": ["No data available for ranking."]})

        group_by = (group_by or "").lower().strip()
        evaluator = self.registry.get(metric_name)
        if evaluator is None:
            print(f"[TRACE][FE][rank] evaluator for {metric_name!r} not found")
            return pd.DataFrame({"_": [f"Metric '{metric_name}' is not supported."]})

        work = df.copy()

        # 🔹 Apply hard filters BEFORE grouping
        def _apply_filter(col, val):
            nonlocal work
            if val and col in work.columns:
                val_l = str(val).lower()
                work = work[work[col].str.lower() == val_l]
                print(f"[TRACE][FE][rank] filter {col}={val_l!r} → rows={len(work)}")

        _apply_filter("product_name", ctx.get("product"))
        _apply_filter("sku", ctx.get("sku"))
        _apply_filter("country", ctx.get("country"))
        _apply_filter("product_name", ctx.get("target_product"))
        _apply_filter("sku", ctx.get("target_sku"))

        if work.empty:
            print("[TRACE][FE][rank] work empty after filters → returning")
            return pd.DataFrame({"_": ["No matching data after applying filters."]})

        # ------------------ Grouping logic ------------------
        label_col = None
        if group_by == "month":
            work = self._ensure_period_month(work)
            if "__period__" not in work.columns or work["__period__"].isna().all():
                print("[TRACE][FE][rank] cannot derive __period__ for month grouping")
                return pd.DataFrame({"_": ["Cannot derive monthly periods from data."]})
            label_col = "__period__"
            keys = sorted(work[label_col].dropna().unique())
            def slicer(k): return work[work[label_col].eq(k)]
            def label_fn(k):
                ts = pd.Timestamp(k)
                return f"{calendar.month_abbr[ts.month]} {ts.year}"

        elif group_by == "product":
            label_col = "product_name"
            if label_col not in work.columns:
                print("[TRACE][FE][rank] no product_name column")
                return pd.DataFrame({"_": ["No product_name column to group by."]})
            keys = sorted(set(map(str, work[label_col].dropna().unique())))
            def slicer(k): return work[work[label_col].astype(str).eq(k)]
            def label_fn(k): return str(k)

        elif group_by == "sku":
            label_col = "sku"
            if label_col not in work.columns:
                print("[TRACE][FE][rank] no sku column")
                return pd.DataFrame({"_": ["No sku column to group by."]})
            keys = sorted(set(map(str, work[label_col].dropna().unique())))
            def slicer(k): return work[work[label_col].astype(str).eq(k)]
            def label_fn(k): return str(k)

        elif group_by == "country":
            label_col = "country"
            if label_col not in work.columns:
                print("[TRACE][FE][rank] no country column")
                return pd.DataFrame({"_": ["No country column to group by."]})
            keys = sorted(set(map(str, work[label_col].dropna().unique())))
            def slicer(k): return work[work[label_col].astype(str).eq(k)]
            def label_fn(k): return str(k)

        else:
            print(f"[TRACE][FE][rank] unsupported group_by={group_by!r}")
            return pd.DataFrame({"_": [f"group_by='{group_by}' is not supported for ranking."]})

        print(f"[TRACE][FE][rank] groups_found={len(keys)} label_col={label_col}")

        # ------------------ Context for evaluator ------------------
        base_ctx = {
            "country": ctx.get("country"),
            "want_breakdown": False,
            "raw_query": ctx.get("raw_query"),
        }
        if ctx.get("target_product"):
            base_ctx["target_product"] = ctx["target_product"]
        if ctx.get("target_sku"):
            base_ctx["target_sku"] = ctx["target_sku"]

        # ------------------ Evaluate per group ------------------
        rows = []
        for k in keys:
            part = slicer(k)
            if part is None or part.empty:
                continue
            payload = evaluator(part, base_ctx.copy())
            val = float(payload.get("result") or 0.0) if isinstance(payload, dict) else float(payload or 0.0)
            rows.append({"group": label_fn(k), "result": val})
            if len(rows) <= 5:  # avoid flooding logs
                print(f"[TRACE][FE][rank] group={label_fn(k)!r} result={val}")

        if not rows:
            print("[TRACE][FE][rank] no rows produced")
            return pd.DataFrame({"_": ["No groups found to rank."]})

        out = pd.DataFrame(rows)

        # 🔹 Handle top_k logic
        k = None if top_k is None else int(top_k)
        if k is None or k <= 0:
            out = out.sort_values("result", ascending=ascending, kind="mergesort").reset_index(drop=True)
        else:
            out = out.sort_values("result", ascending=ascending, kind="mergesort").head(max(1, k)).reset_index(drop=True)

        print(f"[TRACE][FE][rank] out_rows={len(out)} ascending={ascending}")
        return out




def df_to_records_safe(df: pd.DataFrame):
    """Convert DataFrame → JSON-safe records (no NaN/Inf)."""
    if df is None or df.empty:
        return []
    df2 = df.copy()
    df2 = df2.replace([np.inf, -np.inf], np.nan)
    df2 = df2.where(pd.notnull(df2), None)   # convert NaN → None
    return df2.to_dict(orient="records")

def _json_sanitize(x):
    if isinstance(x, float):
        return x if math.isfinite(x) else None
    if isinstance(x, dict):
        return {k: _json_sanitize(v) for k, v in x.items()}
    if isinstance(x, list):
        return [_json_sanitize(v) for v in x]
    return x




# --- Product lookup helpers ---------------------------------------------------
def product_candidates(engine, user_id: int, country: Optional[str], phrase: str, limit: int = 20) -> List[Dict[str, str]]:
    """
    Returns a list of {'product_name': ..., 'sku': ...} matching the phrase.
    Tries per-country table first; falls back to global if needed.
    """
    if not phrase:
        return []
    phrase = phrase.strip().lower()

    def _table_for(user_id: int, country: Optional[str]) -> str:
        if country and str(country).lower() in ["uk", "us"]:
            return f"user_{user_id}_{str(country).lower()}_merge_data_of_all_months"
        return f"user_{user_id}_total_country_global_data"

    tables_to_try = []
    tables_to_try.append(_table_for(user_id, country))
    if country and str(country).lower() in ["uk", "us"]:
        tables_to_try.append(_table_for(user_id, None))  # global fallback

    seen = set()
    out: List[Dict[str, str]] = []
    for table in tables_to_try:
        try:
            sql = f"""
                SELECT DISTINCT product_name, sku
                FROM {table}
                WHERE LOWER(product_name) LIKE :p
                LIMIT :lim
            """
            with engine.connect() as conn:
                rows = conn.execute(text(sql), {"p": f"%{phrase}%", "lim": int(limit)}).fetchall()
            for r in rows:
                name = (r[0] or "").strip()
                sku = (r[1] or "").strip()
                if name and (name.lower(), sku.lower()) not in seen:
                    seen.add((name.lower(), sku.lower()))
                    out.append({"product_name": name, "sku": sku})
        except Exception:
            # swallow table errors silently; try next
            continue
        if len(out) >= limit:
            break
    return out        
    

# ---------- In-memory pending store with TTL ----------
# ---------------- PendingStore ----------------
class PendingStore:
    def __init__(self, ttl_seconds: int = 600):
        self.ttl = ttl_seconds
        self._m: Dict[int, Tuple[float, dict]] = {}

    def set(self, user_id: int, plan: dict, missing: List[str], **extras):
        """
        Store pending plan + missing slots and any extras (e.g., reprompt, original_query).
        Backward-compatible with old calls that didn't pass extras.
        """
        payload = {"plan": plan, "missing": list(missing)}
        if extras:
            payload.update(extras)
        self._m[user_id] = (time.time(), payload)

    def get(self, user_id: int) -> Optional[dict]:
        item = self._m.get(user_id)
        if not item:
            return None
        ts, data = item
        if time.time() - ts > self.ttl:
            self._m.pop(user_id, None)
            return None
        return data

    def clear(self, user_id: int):
        self._m.pop(user_id, None)

# Singleton
PENDING = PendingStore(ttl_seconds=600)

# ---------------- Normalization helpers ----------------
COUNTRY_ALIASES = {
    # UK
    "uk": "UK", "united kingdom": "UK", "gb": "UK", "great britain": "UK", "britain": "UK", "england": "UK",
    # US
    "us": "US", "usa": "US", "u.s.": "US", "u.s.a.": "US", "united states": "US", "america": "US", "american": "US",
    # Global
    "global": "GLOBAL", "all markets": "GLOBAL", "all countries": "GLOBAL", "overall": "GLOBAL", "worldwide": "GLOBAL"
}

def parse_top_k(text: str) -> Optional[int]:
    m = re.search(r"\b(\d{1,3})\b", (text or ""))
    if not m:
        return None
    k = int(m.group(1))
    return k if 1 <= k <= 100 else None

def parse_country(text: str) -> Optional[str]:
    """
    Finds UK/US/Global mentions anywhere in the string.
    Returns 'UK', 'US', or 'GLOBAL' (or None if not found).
    Uses word-boundary matching so 'us' inside 'focus' won't trigger.
    """
    if not text:
        return None
    t = text.lower()

    # Try multi-word phrases first (preserve order of keys)
    multi = [k for k in COUNTRY_ALIASES if " " in k or "." in k]
    for k in multi:
        if k in t:
            return COUNTRY_ALIASES[k]

    # Single-word aliases via word-boundaries
    for k, v in COUNTRY_ALIASES.items():
        if " " in k or "." in k:
            continue
        if re.search(rf"\b{re.escape(k)}\b", t):
            return v

    return None

def parse_time_expr_or_none(text: str) -> Optional[str | dict]:
    """
    Accept:
      - 'June 2025', 'Jun 2025'
      - '2025-06', '2025-06-14'
      - 'Q2 2025', '2025'
    Return the same string (your exec already understands) or None.
    """
    t = (text or "").strip()
    if not t:
        return None
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", t): return t
    if re.fullmatch(r"\d{4}-\d{2}", t): return t
    if re.fullmatch(r"\d{4}", t): return t
    if re.fullmatch(r"[Qq][1-4]\s+\d{4}", t): return t
    if re.fullmatch(
        r"(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|"
        r"sep(?:t|tember)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)\s+\d{4}",
        t, flags=re.I
    ):
        return t
    return None

def choose_best_candidate(cands: List[Dict[str, str]], phrase: str) -> Optional[Dict[str, str]]:
    """
    If there is an exact (case-insensitive) match on product_name, return it.
    Otherwise prefer the shortest name containing the phrase.
    """
    if not cands:
        return None
    ph = phrase.strip().lower()
    exact = [c for c in cands if c["product_name"].strip().lower() == ph]
    if exact:
        return exact[0]
    containing = [c for c in cands if ph in c["product_name"].strip().lower()]
    if containing:
        containing.sort(key=lambda x: len(x["product_name"]))
        return containing[0]
    return cands[0]

def make_ask_prompt(slot: str) -> str:
    s = (slot or "").lower()
    if s == "top_k":
        return "How many results should I return? (e.g., top 3, bottom 5)"
    if s == "country":
        return "Which marketplace should I use? (UK or US)"
    if s == "product":
        return "Which product are you asking about? (give exact product name or a SKU)"
    if s == "sku_choice":
        return "This product family has multiple variants. Do you want **one specific SKU** or **all variants together**?"
    if s == "time_range":
        return "What time period should I use? (e.g., 'June 2025', '2025-06', 'Q2 2025', or '2025-06-14')"
    return "Could you clarify your request?"


# ---------------- Slot logic ----------------
def slots_missing_for(
    plan: dict,
    raw_query: str,
    country_override: Optional[str],
    *,
    parse_time_fn: Callable[[str], Dict[str, any]],
) -> List[str]:
    """
    We require:
      - top_k (when operation=rank)
      - time_range (if neither planner.time_range nor NL time present; EXCEPT when group_by=month)
      - country (if not provided by override/plan)
    """
    missing: List[str] = []

    op = (plan.get("operation") or "").lower()
    if op == "rank" and not plan.get("top_k"):
        missing.append("top_k")

    # ---- Treat planner-supplied time_range as satisfied only if valid ----
    tr = plan.get("time_range")
    has_time = False
    if isinstance(tr, dict) and tr.get("start") and tr.get("end"):
        has_time = True
    elif isinstance(tr, str) and tr.strip():
        # Strings like '2025-06', 'Q2 2025', 'June 2025', '2025-06-14' are valid too.
        has_time = True

    if not has_time:
        nl_time = parse_time_fn(raw_query or "")
        has_time = bool(
            nl_time.get("explicit_day")
            or nl_time.get("months")
            or nl_time.get("date_range")
            or nl_time.get("years")
        )
        # ✅ Allow month grouping to proceed without explicit time; we'll backfill later.
        if (plan.get("group_by") or "").lower() == "month":
            has_time = True

        if not has_time:
            missing.append("time_range")

    eff_ctry = country_override or plan.get("country")
    if not eff_ctry:
        missing.append("country")

    return missing



# ========= INTENT ROUTER (LLM-ONLY, NO KEYWORDS) =========
_INTENT_SYSTEM = (
    "You are an intent classifier for a commerce analytics assistant. "
    "Return ONLY a strict JSON object with fields: "
    "{intent: one of [analytics, general_finance, chit_chat, out_of_scope], "
    "confidence: number 0..1, reason: short string}. "
    "Definitions:\n"
    "- analytics: The user asks about THEIR Amazon/Flipkart/etc. business data, metrics, or time ranges "
    "(e.g., sales/profit/fees/units/CM2/ACOS; mentions 'my/our store', 'UK last month', 'June 2025', "
    "SKU/product/ASIN, reimbursements, Seller Central, breakdowns, ranks, trends).\n"
    "- general_finance: The user asks general marketplace/finance/economics/business questions "
    "not tied to their own data (e.g., comparisons of Amazon vs Flipkart, what is ACOS, how VAT works, "
    "pricing strategies, business model explanations).\n"
    "- chit_chat: Greetings, small talk, non-informational conversation.\n"
    "- out_of_scope: Anything clearly unrelated.\n"
    "Never use external knowledge about the user; decide purely from the text."
)

def route_intent(user_query: str) -> Dict[str, Any]:
    """LLM-based semantic router: analytics vs general_finance vs chit_chat/out_of_scope."""
    client = oa_client or OpenAI(api_key=OPENAI_API_KEY)
    try:
        resp = client.chat.completions.create(
            model="gpt-4o",
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": _INTENT_SYSTEM},
                {"role": "user", "content": user_query.strip()},
            ],
            temperature=0.0,
            max_tokens=150,
        )
        data = json.loads(resp.choices[0].message.content or "{}")
        intent = (data.get("intent") or "").lower().strip()
        conf = float(data.get("confidence") or 0.0)
        reason = data.get("reason") or ""
        if intent not in {"analytics","general_finance","chit_chat","out_of_scope"}:
            intent = "general_finance"  # safe fallback
        return {"intent": intent, "confidence": conf, "reason": reason}
    except Exception as e:
        logger.exception("Intent routing failed; defaulting to general_finance. %r", e)
        return {"intent": "general_finance", "confidence": 0.0, "reason": "router_error"}

# ========= GENERAL FINANCE/BUSINESS FALLBACK =========
_GENERAL_SYSTEM = (
    "You are a senior finance & ecommerce analyst. "
    "Answer clearly and concisely with practical structure (title + 3–6 bullets). "
    "If the user references Amazon/Flipkart/marketplaces in general (not their own data), provide balanced, current best-practice guidance. "
    "If a calculation would need the user's private data, say so briefly and explain how they'd obtain it."
)

def generate_general_answer(user_query: str) -> str:
    """Use ChatGPT-like fallback for general marketplace/finance/business questions."""
    if oa_client is None:
        # Fail-safe local response
        return f"Here’s a quick take:\n- {user_query.strip()} (general explanation requested)\n- I don’t have an OpenAI client configured, so this is a placeholder."
    try:
        resp = oa_client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": _GENERAL_SYSTEM},
                {"role": "user", "content": user_query},
            ],
            temperature=0.2,
            max_tokens=600,
        )
        return (resp.choices[0].message.content or "").strip()
    except Exception:
        logger.exception("General fallback failed; returning safety text.")
        return "I can explain that, but my language model call failed. Please try again."

# ========= ONE-STOP HANDLER (ROUTER → ANALYTICS OR GENERAL) =========
def handle_user_query(
    *,
    user_id: int,
    user_query: str,
    engine,                              # SQLAlchemy engine
    country_override: Optional[str] = None,
    parse_time_fn: Optional[Callable[[str], Dict[str, Any]]] = None,
    run_analytics_fn: Optional[Callable[[dict], Dict[str, Any]]] = None,
) -> str:
    """
    Decides intent first. If 'analytics' → run your existing pipeline.
    Otherwise → general finance/business fallback (ChatGPT-like).
    - run_analytics_fn(plan_bundle) should execute your current path:
      planner → slots_missing_for → ETL/SQL → FormulaEngine → generate_openai_answer
      and return {'text': str} (plus any extras you like).
    """
    parse_time_fn = parse_time_fn or (lambda _: {})
    r = route_intent(user_query)
    intent = r["intent"]
    logger.info("Intent=%s conf=%.2f reason=%s", intent, r.get("confidence", 0.0), r.get("reason",""))

    if intent == "analytics":
        # Build planner plan
        plan = plan_query(user_query)
        missing = slots_missing_for(
            plan, user_query, country_override, parse_time_fn=parse_time_fn
        )
        if missing:
            # Store in your pending cache and ask just one clarifying Q (you already have helpers).
            PENDING.set(user_id, plan, missing, original_query=user_query)
            ask = make_ask_prompt(missing[0])
            return f"Quick clarification needed: {ask}"
        if run_analytics_fn is None:
            # Minimal default: this is where your current analytics execution goes.
            return "Analytics intent detected, but no run_analytics_fn was provided."
        out = run_analytics_fn({
            "user_id": user_id,
            "plan": plan,
            "country_override": country_override,
            "original_query": user_query,
            "engine": engine,
        })
        return (out or {}).get("text") or "I executed your analytics query."
    elif intent in {"general_finance", "chit_chat"}:
        return generate_general_answer(user_query)
    else:
        return "That seems outside my scope. Could you rephrase or provide more detail?"
def first_seen_by_sku(engine, user_id: int, country: str | None) -> dict[str, str]:
    """
    Returns {sku_lower: 'YYYY-MM'} for the earliest month each SKU appears
    in the user's table (country-aware). Handles month like 6/06/Jun/June.
    """
    c = (country or "").strip().lower()
    table = (
        f"user_{user_id}_{c}_merge_data_of_all_months"
        if c in ("uk", "us")
        else f"user_{user_id}_total_country_global_data"
    )

    month_case = """
    CASE
      WHEN month ~ '^[0-9]+$' THEN LPAD(CAST(month AS INT)::text, 2, '0')
      WHEN LOWER(month) IN ('jan','january') THEN '01'
      WHEN LOWER(month) IN ('feb','february') THEN '02'
      WHEN LOWER(month) IN ('mar','march') THEN '03'
      WHEN LOWER(month) IN ('apr','april') THEN '04'
      WHEN LOWER(month) = 'may' THEN '05'
      WHEN LOWER(month) IN ('jun','june') THEN '06'
      WHEN LOWER(month) IN ('jul','july') THEN '07'
      WHEN LOWER(month) IN ('aug','august') THEN '08'
      WHEN LOWER(month) IN ('sep','sept','september') THEN '09'
      WHEN LOWER(month) IN ('oct','october') THEN '10'
      WHEN LOWER(month) IN ('nov','november') THEN '11'
      WHEN LOWER(month) IN ('dec','december') THEN '12'
      ELSE NULL
    END
    """

    sql = f"""
    WITH src AS (
      SELECT
        LOWER(TRIM(sku)) AS sku,
        CASE WHEN year ~ '^[0-9]{{4}}$' THEN CAST(year AS INT) ELSE NULL END AS yy,
        {month_case} AS mm
      FROM {table}
    ),
    clean AS (
      SELECT sku, yy, mm
      FROM src
      WHERE sku IS NOT NULL
        AND sku <> '' AND sku <> '0'
        AND sku NOT IN ('none','null','nan')
        AND yy IS NOT NULL AND mm IS NOT NULL
    ),
    dated AS (
      SELECT sku, make_date(yy, CAST(mm AS INT), 1) AS d
      FROM clean
    )
    SELECT sku, to_char(MIN(d), 'YYYY-MM') AS first_seen_ym
    FROM dated
    GROUP BY sku
    """
    out = {}
    try:
        with engine.connect() as conn:
            rows = conn.execute(text(sql)).fetchall()
        for sku, ym in rows:
            if sku and ym:
                out[str(sku)] = str(ym)
    except Exception as e:
        print("[DEBUG][first_seen_by_sku] failed:", e)
    return out

# ---- Overview helper --------------------------------------------------------
def overview_metrics_for_period(
    engine, user_id: int, country: str, start_ymd: str, end_ymd: str, skus: list[str] | None = None
) -> dict:
    """
    Overview metrics (Sales, Profit, ASP, Qty) computed via FormulaEngine over the given period.
    If `skus` is provided, results are restricted to those SKUs.

    Notes
    -----
    • Uses date_time BETWEEN start..end when date_time exists.
      As a fallback (or to catch rows lacking date_time), also matches (year,month)
      against the month derived from start_ymd.
    • Qty = orders-only units (same rule as FormulaEngine._quantity_sold).
    • ASP = FormulaEngine._asp (sales / orders-only units).
    """
    c = (country or "UK").strip().lower()
    table = (
        f"user_{user_id}_{c}_merge_data_of_all_months" if c in ("uk", "us")
        else f"user_{user_id}_total_country_global_data"
    )

    # Parse Y-M once for the (year, month) fallback filter
    try:
        start_dt = pd.to_datetime(start_ymd).date()
        end_dt   = pd.to_datetime(end_ymd).date()
        ym_year  = start_dt.year
        ym_month = start_dt.month
    except Exception:
        # If parsing fails, still attempt date string compare in SQL; year/month filters may be None.
        ym_year = None
        ym_month = None

    # Optional SKU filter
    sku_filter_sql = "AND sku = ANY(:sku_list)" if skus else ""
    params: dict = {"start": str(start_ymd), "end": str(end_ymd)}
    if skus:
        params["sku_list"] = skus
    if ym_year is not None:
        params["y_fallback"] = int(ym_year)
    if ym_month is not None:
        params["m_full"] = str(ym_month)          # e.g., 6
        params["m_02"]   = f"{ym_month:02d}"      # e.g., "06"

    # Month normalizer (same mapping you use elsewhere) for the (year,month) fallback
    month_case = """
        CASE
          WHEN month ~ '^[0-9]+$' THEN LPAD(CAST(month AS INT)::text, 2, '0')
          WHEN LOWER(month) IN ('jan','january') THEN '01'
          WHEN LOWER(month) IN ('feb','february') THEN '02'
          WHEN LOWER(month) IN ('mar','march') THEN '03'
          WHEN LOWER(month) IN ('apr','april') THEN '04'
          WHEN LOWER(month) = 'may' THEN '05'
          WHEN LOWER(month) IN ('jun','june') THEN '06'
          WHEN LOWER(month) IN ('jul','july') THEN '07'
          WHEN LOWER(month) IN ('aug','august') THEN '08'
          WHEN LOWER(month) IN ('sep','sept','september') THEN '09'
          WHEN LOWER(month) IN ('oct','october') THEN '10'
          WHEN LOWER(month) IN ('nov','november') THEN '11'
          WHEN LOWER(month) IN ('dec','december') THEN '12'
          ELSE NULL
        END
    """

    # Pull rows for the period; prefer date_time when present, otherwise (year, month)
    # We OR them to catch mixed rows where some have date_time and some only year/month.
    sql = text(f"""
    SELECT *
    FROM {table}
    WHERE 1=1
      {sku_filter_sql}
      AND (
            (date_time IS NOT NULL AND date_time::date BETWEEN CAST(:start AS DATE) AND CAST(:end AS DATE))
         OR (
                (:y_fallback IS NOT NULL) AND (:m_02 IS NOT NULL)
            AND (year ~ '^[0-9]+$' AND CAST(year AS INT) = :y_fallback)
            AND ({month_case}) IN (:m_02, :m_full)
         )
      )
""")


    with engine.connect() as conn:
        df = pd.read_sql_query(sql, conn, params=params)

    # Compute via FormulaEngine
    fe = FormulaEngine()
    ctx = {"country": country, "want_breakdown": False}

    sales  = fe._sales(df, ctx).get("result") or 0.0
    profit = fe._profit(df, ctx).get("result") or 0.0
    qty    = fe._quantity_sold(df, ctx).get("result") or 0.0
    asp    = fe._asp(df, ctx).get("result") or 0.0

    return {
        "sales": float(sales),
        "qty": float(qty),
        "asp": float(asp),
        "profit": float(profit),
    }
def _finalize_records(plan: dict, table_records: list[dict]) -> list[dict]:
    """
    Decide which records to send to the LLM and normalize metric field names.
    - If user explicitly asked for a product → return all rows (after relabel).
    - Otherwise, return overall rows plus breakdown (if present).
    - If metric is ads spend (or advertising_total), rename sales* → ads_spend*.
    """
    metric = (plan.get("metric") or "").strip().lower()

    # --- relabel helper ------------------------------------------------------
    def _relabel(records: list[dict], from_key: str, to_key: str) -> list[dict]:
        if not records:
            return records
        suffixes = ["", "_prev", "_chg", "_pct"]
        out = []
        for r in records:
            r2 = dict(r)
            for s in suffixes:
                fk = f"{from_key}{s}"
                tk = f"{to_key}{s}"
                if fk in r2:
                    r2[tk] = r2.pop(fk)
            # For clarity on non-sales metrics, drop unrelated money columns if present
            if to_key != "sales":
                for junk in ("profit", "asp", "quantity"):
                    r2.pop(junk, None)
            out.append(r2)
        return out

    # --- normalize metric naming for ads spend -------------------------------
    # Planner uses 'ads_spend'; FE evaluator is 'advertising_total'. Treat both.
    if metric in {"ads_spend", "advertising_total", "ad spend", "ads"}:
        table_records = _relabel(table_records, from_key="sales", to_key="ads_spend")

    has_product = bool((plan.get("product") or "").strip())
    overall = [r for r in table_records if (str(r.get("scope") or "").lower() == "overall")]
    breakdown = [r for r in table_records if (str(r.get("scope") or "").lower() in {"product", "sku"})]

    if has_product:
        return table_records
    return overall + breakdown if breakdown else (overall or table_records)



class BusinessAdvisor:
    """
    Keyword-free advisor:
    - Reads monthly series (overall + product) from df_primary
    - Builds a customized growth playbook based on signals in the data
    - Optionally uses aux['ads'] (monthly ads_spend) to compute ACoS deltas

    UPDATED (Portfolio-first, Business Insight-aligned):
    - Default output is OVERALL business summary + exactly 5 SKU-wise actions (JSON-driven).
    - Does NOT “divide into products” unless upstream routing/aux explicitly targets a product/SKU.
    - Uses ONLY df_primary-derived metrics (RAG/FormulaEngine), not Business Insight metrics fetching.
    - Returns list[str] to preserve existing route contract (safe for "\n".join()).
    """

    # ==================== Portfolio advisor prompt (JSON output) ====================

    PORTFOLIO_ADVISOR_PROMPT = """
You are a senior ecommerce business analyst.

You receive JSON containing:
- Overall totals and % change for units, net sales, profit, ASP index and unit profit index
- A SKU table in sku_tables.all_skus including product_name and SKU-wise metrics.
  Each SKU row also includes an "urgency_score" where higher means this SKU needs more urgent attention (larger declines and higher impact on the business).


GOAL
Produce:
1) A short overall business summary (3–5 bullets)
2) Exactly 5 detailed SKU-wise recommendations in the EXACT format below.

====================
SUMMARY (3–5 bullets)
====================
Write 3–5 short bullets describing, in simple language:
- How overall units, sales and profit moved (using quantity_pct, net_sales_pct, profit_pct)
- Any big change in ASP or unit profit (asp_index_pct, unit_profit_index_pct)
- Whether performance is coming more from volume, pricing, or a few big SKUs

====================
ACTIONS (exactly 5)
====================

When choosing the 5 products, ALWAYS prioritize the SKUs with the highest urgency_score (most negative performance with meaningful size). Only pick low-urgency SKUs if there are fewer than 5 high-urgency options.


✅ EACH action bullet MUST follow this exact layout with line breaks:

Line 1: "Product name - <product_name>"
Line 2-3: One paragraph of exactly 2 sentences (metrics + causal chain + mix)
Line 4: (blank line)
Line 5: One action sentence ONLY

So the action_bullet string must look like:
Product name - Classic

The increase in ASP by 13.27% resulted in a dip in units by 25.91%, which also resulted in sales falling by 16.08%. The sales mix is down by 15.93%, reducing its contribution, and profit is up by 10.74%.

Reduce ASP slightly to improve traction.

-----------------------------
Metrics paragraph rules (Line 2-3)
-----------------------------
- Exactly 2 sentences.

- Sentence 1: Use ONLY the SKU metrics for ASP, Units and Sales (where values exist).
  Use the same flow as the original causal chain, but adjust tone only in this special case:
  - Default tone:
    "The increase/decrease in ASP by X% resulted in a dip/growth in units by Y%, which also resulted in sales falling/increasing by Z%."
  - If ASP, Units, and Sales are ALL negative (all down), do NOT imply ASP caused units. Use this co-movement tone instead:
    "There is a decrease in ASP by X% and also a dip in units by Y%, which resulted in sales falling by Z%."

- Sentence 2: Must mention Sales Mix Change (%) if available (up or down) in the same original style:
  "The sales mix is up/down by M%, increasing/reducing its contribution."
  If profitability metrics are available for that SKU, append ONLY ONE of them to the SAME sentence without breaking flow and without using "while/since/because":
  Prefer in this order:
  1) Profit (%)
  2) Profit Per Unit (%)
  Append as:
  ", and profit is up/down by A%."
  OR ", and profit per unit is up/down by B%."
  (If profitability metric is not available, skip it.)

- Do NOT invent numbers and do NOT add extra reasons.

-----------------------------
Allowed action sentences (Line 5 only)
-----------------------------
Use exactly ONE of these sentences, verbatim:
- "Check ads and visibility campaigns for this product."
- "Review the visibility setup for this product."
- "Reduce ASP slightly to improve traction."
- "Increase ASP slightly to strengthen margins."
- "Maintain current ASP and monitor performance."
- "Monitor performance closely for now."
- "Check Amazon fees or taxes for this product as profit is down despite growth."

Decision guidance:
- If ASP is strongly up and units are down: prefer "Reduce ASP slightly to improve traction."
- If units and sales are down and ASP is flat or slightly up: prefer visibility lines.
- If profit/unit profit is very strong and units are stable/slightly down: prefer maintain/increase ASP.
- If ASP, units, sales, and sales mix are up but profit is down: prefer "Check Amazon fees or taxes for this product as profit is down despite growth."

Ignore:
- Any row where product_name is "Total" or contains "Total".

OUTPUT FORMAT
Return ONLY valid JSON:
{
  "summary_bullets": ["...", "..."],
  "action_bullets": ["Action 1", "Action 2", "Action 3", "Action 4", "Action 5"]
}
Do not add any extra keys. Do not wrap in Markdown.
""".strip()

    PORTFOLIO_SYSTEM_MSG = (
        "You are a senior ecommerce analyst. Output ONLY valid JSON as specified. "
        "Do not add extra keys. Do not wrap in Markdown."
    )

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
            if "key" in out.columns:
                out["product"] = out["key"]
            elif "label" in out.columns:
                out["product"] = out["label"]
        return out

    @staticmethod
    def _product_rollup(df: pd.DataFrame, cols=("sales", "profit", "quantity", "fba_fees")) -> pd.DataFrame:
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
        req = {"product", "period", value_col}
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
        g["growth_pct"] = np.where(g[f"{value_col}_prev"] > 0, (g["growth_abs"] / g[f"{value_col}_prev"]) * 100.0, np.nan)
        return g.reset_index().sort_values(["growth_abs", "growth_pct"], ascending=[False, False])

    # ------------------------- portfolio metrics builders -------------------------

    @staticmethod
    def _safe_pct(prev_v: float | None, curr_v: float | None) -> float | None:
        try:
            if prev_v is None:
                return None
            prev_v = float(prev_v)
            curr_v = float(curr_v) if curr_v is not None else 0.0
            if not np.isfinite(prev_v) or prev_v == 0.0:
                return None
            out = ((curr_v - prev_v) / prev_v) * 100.0
            return float(out) if np.isfinite(out) else None
        except Exception:
            return None

    @classmethod
    def _build_overall_from_by_period(cls, by_period: list[dict]) -> dict:
        """
        Build overall totals and pct changes from last 2 periods.
        Expects dict rows containing: period, sales, profit, quantity, asp (when available).
        """
        bp = list(by_period or [])
        if not bp:
            return {
                "quantity_prev": 0.0, "quantity_curr": 0.0, "quantity_pct": None,
                "net_sales_prev": 0.0, "net_sales_curr": 0.0, "net_sales_pct": None,
                "profit_prev": 0.0, "profit_curr": 0.0, "profit_pct": None,
                "asp_prev": 0.0, "asp_curr": 0.0, "asp_index_pct": None,
                "unit_profit_prev": 0.0, "unit_profit_curr": 0.0, "unit_profit_index_pct": None,
            }

        prev = bp[-2] if len(bp) >= 2 else {}
        curr = bp[-1] if len(bp) >= 1 else {}

        q_prev = float(prev.get("quantity", 0.0) or 0.0)
        q_curr = float(curr.get("quantity", 0.0) or 0.0)
        s_prev = float(prev.get("sales", 0.0) or 0.0)
        s_curr = float(curr.get("sales", 0.0) or 0.0)
        p_prev = float(prev.get("profit", 0.0) or 0.0)
        p_curr = float(curr.get("profit", 0.0) or 0.0)
        asp_prev = float(prev.get("asp", 0.0) or 0.0)
        asp_curr = float(curr.get("asp", 0.0) or 0.0)

        up_prev = (p_prev / q_prev) if q_prev > 0 else 0.0
        up_curr = (p_curr / q_curr) if q_curr > 0 else 0.0

        return {
            "quantity_prev": q_prev,
            "quantity_curr": q_curr,
            "quantity_pct": cls._safe_pct(q_prev, q_curr),

            "net_sales_prev": s_prev,
            "net_sales_curr": s_curr,
            "net_sales_pct": cls._safe_pct(s_prev, s_curr),

            "profit_prev": p_prev,
            "profit_curr": p_curr,
            "profit_pct": cls._safe_pct(p_prev, p_curr),

            # Treat "ASP index" as ASP % change (prompt expects *_pct)
            "asp_prev": asp_prev,
            "asp_curr": asp_curr,
            "asp_index_pct": cls._safe_pct(asp_prev, asp_curr),

            "unit_profit_prev": float(up_prev),
            "unit_profit_curr": float(up_curr),
            "unit_profit_index_pct": cls._safe_pct(up_prev, up_curr),
        }



    @classmethod
    def _build_sku_tables(cls, d: pd.DataFrame, by_period: list[dict], top_n: int = 80) -> dict:
        """
        Build sku_tables.all_skus.

        Requirements:
        - include product_name and sku-wise metrics.
        - Provide % metrics for ASP, Units, Sales, Sales Mix Change (%) and optional Profit/Profit per unit.
        - Include an urgency_score per SKU so the model can prioritize which products need urgent attention.
        """
        if d is None or not isinstance(d, pd.DataFrame) or d.empty:
            print("[DEBUG][sku_tables] empty or invalid df, returning no SKUs")
            return {"all_skus": []}
        
        # 👉 NEW: create a synthetic sku if sku is missing but product exists
        if "sku" not in d.columns and "product" in d.columns:
            print("[DEBUG][sku_tables] no 'sku' column, using 'product' as sku surrogate")
            d = d.copy()
            d["sku"] = d["product"]

        # Ensure we have a stable period order
        bp = list(by_period or [])
        prev_period = bp[-2].get("period") if len(bp) >= 2 else None
        curr_period = bp[-1].get("period") if len(bp) >= 1 else None

        if not prev_period or not curr_period:
            print(f"[DEBUG][sku_tables] missing prev/curr periods, by_period={bp}")
            return {"all_skus": []}

        # Total sales for mix calculations
        prev_tot_sales = float(bp[-2].get("sales", 0.0) or 0.0)
        curr_tot_sales = float(bp[-1].get("sales", 0.0) or 0.0)

        # Aggregate per sku+product+period
        grp_cols = [c for c in ["period", "sku", "product", "sales", "profit", "quantity"] if c in d.columns]
        if not {"period", "sku"}.issubset(set(grp_cols)) or "sales" not in grp_cols:
            print(f"[DEBUG][sku_tables] missing required columns in df: have={grp_cols}")
            return {"all_skus": []}

        g = d[grp_cols].copy()
        g = g[g["period"].isin([prev_period, curr_period])]

        if g.empty:
            print(f"[DEBUG][sku_tables] no rows for periods prev={prev_period}, curr={curr_period}")
            return {"all_skus": []}

        agg = g.groupby(["period", "sku"], dropna=True).agg(
            product_name=("product", "first") if "product" in g.columns else ("sku", "first"),
            net_sales=("sales", "sum"),
            quantity=("quantity", "sum") if "quantity" in g.columns else ("sales", "count"),
            profit=("profit", "sum") if "profit" in g.columns else ("net_sales", "sum"),
        ).reset_index()

        # Split prev/curr panels
        prev_df = agg[agg["period"].eq(prev_period)].set_index("sku")
        curr_df = agg[agg["period"].eq(curr_period)].set_index("sku")

        all_skus = sorted(set(prev_df.index).union(set(curr_df.index)))
        rows: list[dict] = []

        for sku in all_skus:
            prev_row = prev_df.loc[sku] if sku in prev_df.index else None
            curr_row = curr_df.loc[sku] if sku in curr_df.index else None

            product_name = ""
            if curr_row is not None:
                product_name = str(curr_row.get("product_name", "") or "")
            elif prev_row is not None:
                product_name = str(prev_row.get("product_name", "") or "")

            # Ignore Total-like
            if product_name and "total" in product_name.lower():
                continue

            ns_prev = float(prev_row["net_sales"]) if prev_row is not None and "net_sales" in prev_row else 0.0
            ns_curr = float(curr_row["net_sales"]) if curr_row is not None and "net_sales" in curr_row else 0.0
            q_prev = float(prev_row["quantity"]) if prev_row is not None and "quantity" in prev_row else 0.0
            q_curr = float(curr_row["quantity"]) if curr_row is not None and "quantity" in curr_row else 0.0
            p_prev = float(prev_row["profit"]) if prev_row is not None and "profit" in prev_row else 0.0
            p_curr = float(curr_row["profit"]) if curr_row is not None and "profit" in curr_row else 0.0

            asp_prev = (ns_prev / q_prev) if q_prev > 0 else 0.0
            asp_curr = (ns_curr / q_curr) if q_curr > 0 else 0.0

            up_prev = (p_prev / q_prev) if q_prev > 0 else 0.0
            up_curr = (p_curr / q_curr) if q_curr > 0 else 0.0

            
            # Mix (% of total sales in that month)
            mix_prev_pct = (ns_prev / prev_tot_sales) * 100.0 if prev_tot_sales > 0 else 0.0
            mix_curr_pct = (ns_curr / curr_tot_sales) * 100.0 if curr_tot_sales > 0 else 0.0

            # Change in mix = current - previous (percentage points)
            mix_change_pp = mix_curr_pct - mix_prev_pct



            row = {
                "sku": sku,
                "product_name": product_name.strip() or str(sku),

                # prev/curr
                "net_sales_prev": ns_prev,
                "net_sales_curr": ns_curr,
                "quantity_prev": q_prev,
                "quantity_curr": q_curr,
                "profit_prev": p_prev,
                "profit_curr": p_curr,
                "asp_prev": asp_prev,
                "asp_curr": asp_curr,
                "profit_per_unit_prev": up_prev,
                "profit_per_unit_curr": up_curr,

                # pct (prompt expects % fields exist “where values exist”)
                "Net Sales Growth (%)": cls._safe_pct(ns_prev, ns_curr),
                "Unit Growth (%)": cls._safe_pct(q_prev, q_curr),
                "Profit Growth (%)": cls._safe_pct(p_prev, p_curr),
                "ASP Growth (%)": cls._safe_pct(asp_prev, asp_curr),
                "Profit Per Unit Growth (%)": cls._safe_pct(up_prev, up_curr),

                "Sales Mix Change (%)": mix_change_pp,

            }

            rows.append(row)

        # ---------- urgency_score per SKU ----------
        def _urgency_score(r: dict) -> float:
            """
            Higher score == more urgent to address.
            Focus on SKUs that:
            - have meaningful current sales, AND
            - are declining in sales/profit/mix.
            """
            try:
                ns_curr = float(r.get("net_sales_curr", 0.0) or 0.0)
                sales_g = r.get("Net Sales Growth (%)") or 0.0
                profit_g = r.get("Profit Growth (%)") or 0.0
                mix_g = r.get("Sales Mix Change (%)") or 0.0

                # Only care about declines (negative %)
                down_sales = max(0.0, -float(sales_g))
                down_profit = max(0.0, -float(profit_g))
                down_mix = max(0.0, -float(mix_g))

                if ns_curr <= 0 or (down_sales + down_profit + down_mix) == 0:
                    return 0.0

                decline_score = down_sales + down_profit + 0.5 * down_mix
                return ns_curr * decline_score
            except Exception:
                return 0.0

        for r in rows:
            r["urgency_score"] = float(_urgency_score(r))

        # Rank SKUs by urgency first, then by current sales as a tiebreaker
        def _key(r):
            urg = float(r.get("urgency_score", 0.0) or 0.0)
            curr = float(r.get("net_sales_curr", 0.0) or 0.0)
            return (urg, curr)

        rows_sorted = sorted(rows, key=_key, reverse=True)

        # Debug: show what we're sending to the LLM
        print(f"[DEBUG][sku_tables] periods used: prev={prev_period}, curr={curr_period}")
        print(f"[DEBUG][sku_tables] total_skus={len(rows_sorted)}")
        print("[DEBUG][sku_tables] top 5 SKUs by urgency_score:")
        for r in rows_sorted[:5]:
            print(
                "  -",
                r.get("product_name"),
                "| urgency_score=",
                round(r.get("urgency_score", 0.0), 2),
                "| Net Sales Growth (%)=",
                r.get("Net Sales Growth (%)"),
                "| Profit Growth (%)=",
                r.get("Profit Growth (%)"),
                "| Sales Mix Change (%)=",
                r.get("Sales Mix Change (%)"),
            )

        # Single flat list, no splitting
        return {"all_skus": rows_sorted}



    @classmethod
    def _call_portfolio_llm(cls, advisor_payload: dict) -> dict:
        """
        Calls GPT with strict JSON response format.
        Returns parsed dict with keys: summary_bullets, action_bullets.
        """

        # ---- DEBUG: preview what we're sending to the LLM ----
        try:
            sku_tables = (advisor_payload or {}).get("sku_tables") or {}
            all_skus = sku_tables.get("all_skus") or sku_tables.get("top_80_skus") or []
            debug_payload = {
                "overall": advisor_payload.get("overall"),
                "meta": advisor_payload.get("meta"),
                "num_skus": len(all_skus),
                "sku_sample": all_skus[:5],  # just a small sample for logs
            }
            print("[DEBUG][advisor] payload preview going to LLM:")
            print(json.dumps(debug_payload, indent=2, default=str))
        except Exception as e:
            print("[DEBUG][advisor] failed to print advisor_payload preview:", e)

        prompt = cls.PORTFOLIO_ADVISOR_PROMPT + "\n\nJSON:\n" + json.dumps(advisor_payload, indent=2, default=str)
        out = oa_client.chat.completions.create(
            model="gpt-4o",
            temperature=0,
            max_tokens=800,
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": cls.PORTFOLIO_SYSTEM_MSG},
                {"role": "user", "content": prompt},
            ],
        )
        raw = (out.choices[0].message.content or "").strip()

        # ---- DEBUG: raw JSON returned from LLM ----
        print("[DEBUG][advisor] raw LLM JSON response:")
        print(raw)

        try:
            obj = json.loads(raw) if raw else {}
        except Exception as e:
            print("[DEBUG][advisor] failed to parse LLM JSON:", e)
            obj = {}
        return obj if isinstance(obj, dict) else {}

    @staticmethod
    def _render_portfolio_response(obj: dict) -> list[str]:
        """
        Convert JSON obj to list[str] for existing RAG route compatibility.
        """
        summary = obj.get("summary_bullets") or []
        actions = obj.get("action_bullets") or []

        # Hard guardrails: ensure types and counts
        if not isinstance(summary, list):
            summary = []
        if not isinstance(actions, list):
            actions = []

        # Keep exactly 5 actions if model returns more/less (we clamp)
        actions = actions[:5]

        lines: list[str] = []
        lines.append("SUMMARY")
        for b in summary[:5]:
            if isinstance(b, str) and b.strip():
                lines.append(f"- {b.strip()}")
        lines.append("")
        lines.append("ACTIONS")
        lines.append("")

        for a in actions:
            if isinstance(a, str) and a.strip():
                # Keep internal line breaks exactly as model returned
                lines.append(a.strip())
                lines.append("")  # spacer

        # If the model failed to comply, provide a safe fallback
        if len(actions) < 5:
            # avoid altering other app behavior; just fill with generic placeholders
            # (still helps UI not look broken)
            for _ in range(5 - len(actions)):
                lines.append("Product name - (Not enough SKU data)")
                lines.append("")
                lines.append("Monitor performance closely for now.")
                lines.append("")

        return lines

    
    # ---------- main: keyword-free, data-driven recommendations ----------
    @staticmethod
    def recommend(query: str, df_primary: pd.DataFrame, aux: dict | None = None) -> list[str]:
        """
        Portfolio-first action plan aligned to the new JSON prompt.

        - Canonicalizes many Amazon export column names -> {sales, profit, quantity, asp, ...}
        - Constructs a monthly 'period' where possible
        - Computes profit and ASP if missing
        - Produces rollups (totals, by_period, by_entity) to ground the SKU-table builder
        - Generates overall summary + exactly 5 SKU actions (via strict JSON)
        Returns: list[str] (safe for existing routes).
        """
        aux = aux or {}
        scope = aux.get("scope") or "auto"  # "sku" | "product" | "portfolio" | "auto"
        target = aux.get("target")          # if user explicitly asks SKU/product, upstream can set this
        country = aux.get("country") or "US"
        tr = aux.get("time_range")          # dict or string

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
                "meta": {
                    "scope": scope,
                    "target": target,
                    "country": country,
                    "time_range": tr,
                },
                "columns": [],
                "samples": [],
                "rollups": {},
            }
        else:
            d = df_primary.copy()
            # Lowercase map
            d = d.rename(columns={c: alias_map.get(str(c).strip().lower(), str(c).strip().lower())
                                  for c in d.columns})

            # ---- 2) Period construction ------------------------------------------
            if "date_time" in d.columns:
                dt = pd.to_datetime(d["date_time"], errors="coerce")
                d["period"] = dt.dt.to_period("M").astype(str)  # "YYYY-MM"
            elif {"year", "month"}.issubset(d.columns):
                def _ym_to_date(y, m):
                    try:
                        return pd.Timestamp(year=int(y), month=int(m), day=1)
                    except Exception:
                        return pd.NaT
                d["_tmp_dt"] = [_ym_to_date(y, m) for y, m in zip(d["year"], d["month"])]
                d["period"] = pd.to_datetime(d["_tmp_dt"], errors="coerce").dt.to_period("M").astype(str)
                d.drop(columns=["_tmp_dt"], errors="ignore", inplace=True)

            if "product" not in d.columns and "product_name" in df_primary.columns:
                d["product"] = df_primary["product_name"]

            # ---- 3) Numeric cleaning + derived -----------------------------------
            numeric_like = [
                "sales", "profit", "quantity", "fba_fees", "selling_fees",
                "promotional_rebates", "mft", "other_txn_fees",
                "shipping_credits", "postage_credits", "gift_wrap_credits",
                "other", "net_total",
                "product_sales_tax", "shipping_credits_tax",
                "giftwrap_credits_tax", "promotional_rebates_tax",
            ]
            for c in numeric_like:
                if c in d.columns:
                    d[c] = d[c].map(_safe_float)

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

            if "asp" not in d.columns and {"sales", "quantity"}.issubset(d.columns):
                qty = d["quantity"].replace(0, np.nan)
                d["asp"] = (d["sales"] / qty).replace([np.inf, -np.inf], np.nan).fillna(0.0)

            keep_cols = [c for c in canonical_order if c in d.columns]
            if "sku" in d.columns and "sku" not in keep_cols:
                keep_cols.append("sku")
            if "product" in d.columns and "product" not in keep_cols:
                keep_cols.append("product")
            d = d[keep_cols].copy()

            # ---- 4) Rollups -------------------------------------------------------
            payload = {
                "meta": {
                    "scope": scope,
                    "target": target,
                    "country": country,
                    "time_range": tr,
                },
                "columns": list(d.columns),
                "samples": [],
                "rollups": {},
            }

            present = [c for c in ["sales", "profit", "quantity", "asp", "fba_fees", "selling_fees"]
                       if c in d.columns]
            if present:
                totals = d[present].sum(numeric_only=True).to_dict()
                payload["rollups"]["totals"] = {k: float(v) for k, v in totals.items()}

            if "period" in d.columns and present:
                try:
                    grp = d.groupby("period", dropna=True)[present].sum().reset_index()

                    # Ensure chronological order (robust)
                    try:
                        grp["_pdt"] = pd.to_datetime(grp["period"], errors="coerce")
                        grp = grp.sort_values("_pdt").drop(columns=["_pdt"])
                    except Exception:
                        pass

                    payload["rollups"]["by_period"] = grp.to_dict(orient="records")
                except Exception as e:
                    print("[DEBUG][advisor] failed building primary by_period rollup:", e)

            key_col = "sku" if "sku" in d.columns else ("product" if "product" in d.columns else None)
            if key_col and "sales" in d.columns:
                top = d.groupby(key_col, dropna=True)["sales"].sum().sort_values(ascending=False).reset_index()
                payload["rollups"]["by_entity"] = top.head(30).to_dict(orient="records")

            try:
                d_sample = d.sort_values("sales", ascending=False).head(80) if "sales" in d.columns else d.head(80)
            except Exception:
                d_sample = d.head(80)
            payload["samples"] = d_sample.fillna("").to_dict(orient="records")

        # ---- 4.5) Derive anchor dates (kept, but not used by portfolio prompt) ---
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
        except Exception as e:
            print("[DEBUG][advisor] failed deriving latest_period_end:", e)
            latest_period_end = None

        if latest_period_end is None:
            latest_period_end = pd.Timestamp.utcnow().normalize()

        latest_period_end = latest_period_end.to_period("M").to_timestamp("M")
        next_month_start = (latest_period_end + pd.offsets.MonthBegin(1)).date()

        payload["meta"]["latest_period_end"] = latest_period_end.date().isoformat()
        payload["meta"]["next_month_start"] = next_month_start.isoformat()

        # ---- 5) Portfolio-first advisor generation -------------------------------
        try:
            by_period = (payload.get("rollups") or {}).get("by_period") or []

            # DEBUG: see what we actually have at this point
            print("[DEBUG][advisor] initial by_period from rollups:", by_period)

            # ---------- Fallback: rebuild by_period if missing but we DO have 'period' ----------
            if (not by_period) and isinstance(d, pd.DataFrame) and ("period" in d.columns):
                present_overall = [c for c in ["sales", "profit", "quantity", "asp"] if c in d.columns]
                if present_overall:
                    try:
                        grp = d.groupby("period", dropna=True)[present_overall].sum().reset_index()
                        try:
                            grp["_pdt"] = pd.to_datetime(grp["period"], errors="coerce")
                            grp = grp.sort_values("_pdt").drop(columns=["_pdt"])
                        except Exception:
                            pass
                        by_period = grp.to_dict(orient="records")
                        print("[DEBUG][advisor] rebuilt by_period fallback from df_primary:")
                        print(grp)
                    except Exception as e:
                        print("[DEBUG][advisor] failed to rebuild by_period fallback:", e)

            # Build overall (handles 0,1,2 periods gracefully)
            overall = BusinessAdvisor._build_overall_from_by_period(by_period)

            # ---------- SKU table logic: multi-period vs single-period fallback ----------
            if by_period and len(by_period) >= 2 and "period" in d.columns:
                # Normal 2-period path
                sku_tables = BusinessAdvisor._build_sku_tables(d, by_period, top_n=80)
            else:
                # Single-period / no-period fallback:
                # Use snapshot df to build per-SKU metrics so LLM still gets SKUs.
                print("[DEBUG][advisor] using single-period SKU fallback (no usable prev/curr periods)")

                key_col = None
                if "sku" in d.columns:
                    key_col = "sku"
                elif "product" in d.columns:
                    key_col = "product"

                if (not isinstance(d, pd.DataFrame)) or d.empty or not key_col or "sales" not in d.columns:
                    print("[DEBUG][advisor] fallback SKU builder: not enough columns, returning empty sku_tables")
                    sku_tables = {"all_skus": []}
                else:
                    # Aggregate across the available range (treated as "current")
                    agg_kwargs = {}
                    if "product" in d.columns:
                        agg_kwargs["product_name"] = ("product", "first")
                    else:
                        agg_kwargs["product_name"] = (key_col, "first")

                    agg_kwargs["net_sales"] = ("sales", "sum")
                    if "quantity" in d.columns:
                        agg_kwargs["quantity"] = ("quantity", "sum")
                    else:
                        agg_kwargs["quantity"] = (key_col, "size")

                    if "profit" in d.columns:
                        agg_kwargs["profit"] = ("profit", "sum")
                    else:
                        # simple proxy if profit missing
                        agg_kwargs["profit"] = ("sales", "sum")

                    agg_df = d.groupby(key_col, dropna=True).agg(**agg_kwargs).reset_index()

                    curr_tot_sales = float(agg_df["net_sales"].sum()) if "net_sales" in agg_df.columns else 0.0
                    rows = []

                    for _, row in agg_df.iterrows():
                        key = row[key_col]
                        product_name = str(row.get("product_name") or key)

                        ns_curr = float(row.get("net_sales", 0.0) or 0.0)
                        q_curr = float(row.get("quantity", 0.0) or 0.0)
                        p_curr = float(row.get("profit", 0.0) or 0.0)

                        # No previous period → we keep prev values at 0 and all % growth as None
                        ns_prev = 0.0
                        q_prev = 0.0
                        p_prev = 0.0

                        asp_curr = (ns_curr / q_curr) if q_curr > 0 else 0.0
                        asp_prev = 0.0

                        up_curr = (p_curr / q_curr) if q_curr > 0 else 0.0
                        up_prev = 0.0

                        mix_curr = (ns_curr / curr_tot_sales) if curr_tot_sales > 0 else 0.0

                        r = {
                            "sku": key,
                            "product_name": product_name.strip() or str(key),

                            "net_sales_prev": ns_prev,
                            "net_sales_curr": ns_curr,
                            "quantity_prev": q_prev,
                            "quantity_curr": q_curr,
                            "profit_prev": p_prev,
                            "profit_curr": p_curr,
                            "asp_prev": asp_prev,
                            "asp_curr": asp_curr,
                            "profit_per_unit_prev": up_prev,
                            "profit_per_unit_curr": up_curr,

                            # No real previous period → % metrics are None ("no data")
                            "Net Sales Growth (%)": BusinessAdvisor._safe_pct(None, ns_curr),
                            "Unit Growth (%)": BusinessAdvisor._safe_pct(None, q_curr),
                            "Profit Growth (%)": BusinessAdvisor._safe_pct(None, p_curr),
                            "ASP Growth (%)": BusinessAdvisor._safe_pct(None, asp_curr),
                            "Profit Per Unit Growth (%)": BusinessAdvisor._safe_pct(None, up_curr),

                            # We only know current mix, not mix change
                            "Sales Mix Change (%)": None,
                        }

                        rows.append(r)

                    # Fallback urgency_score: use current sales so high-impact SKUs still surface
                    for r in rows:
                        try:
                            r["urgency_score"] = float(r.get("net_sales_curr", 0.0) or 0.0)
                        except Exception:
                            r["urgency_score"] = 0.0

                    rows_sorted = sorted(
                        rows,
                        key=lambda r: (float(r.get("urgency_score", 0.0) or 0.0),
                                       float(r.get("net_sales_curr", 0.0) or 0.0)),
                        reverse=True,
                    )

                    print(f"[DEBUG][advisor][fallback] total_skus={len(rows_sorted)}")
                    print("[DEBUG][advisor][fallback] top 5 SKUs (by net_sales_curr):")
                    for r in rows_sorted[:5]:
                        print(
                            "  -",
                            r.get("product_name"),
                            "| net_sales_curr=",
                            round(r.get("net_sales_curr", 0.0) or 0.0, 2),
                            "| urgency_score=",
                            round(r.get("urgency_score", 0.0) or 0.0, 2),
                        )

                    sku_tables = {"all_skus": rows_sorted}

            advisor_payload = {
                "meta": {
                    "country": country,
                    "time_range": tr,
                    "scope": "portfolio" if scope in ("auto", "portfolio") else scope,
                    "target": target,
                    "latest_period_end": payload["meta"].get("latest_period_end"),
                    "next_month_start": payload["meta"].get("next_month_start"),
                },
                "overall": overall,
                "sku_tables": sku_tables,
            }

            obj = BusinessAdvisor._call_portfolio_llm(advisor_payload)
            return BusinessAdvisor._render_portfolio_response(obj)

        except Exception as e:
            print("[DEBUG][advisor] portfolio JSON advisor failed:", e)
            return ["I wasn’t able to generate recommendations right now. Please try again."]



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
        d["sales"] = pd.to_numeric(d.get("sales_raw"), errors="coerce").fillna(0.0)
        tax = pd.to_numeric(d.get("tax_raw"), errors="coerce").fillna(0.0)
        credits = pd.to_numeric(d.get("credits_raw"), errors="coerce").fillna(0.0)
        fba = pd.to_numeric(d.get("fba_fees_raw"), errors="coerce").fillna(0.0)
        selling = pd.to_numeric(d.get("selling_fees_raw"), errors="coerce").fillna(0.0)
        other = pd.to_numeric(d.get("other_raw"), errors="coerce").fillna(0.0)
        cost = pd.to_numeric(d.get("cost_raw"), errors="coerce").fillna(0.0)
        qty = pd.to_numeric(d.get("qty_raw"), errors="coerce").fillna(0.0)
        d["profit"] = d["sales"] + credits - tax - fba - selling - other - cost
        d["qty"] = qty
        d["asp"] = d.apply(lambda r: (r["sales"] / r["qty"]) if r["qty"] > 0 else np.nan, axis=1)
        return d[["period", "product", "sales", "profit", "qty", "asp"]]

    # --- 3) Diagnose trends safely (works with short history)
    @staticmethod
    def diagnose_trends(d: pd.DataFrame) -> dict:
        out = {}
        if d is None or d.empty:
            return out
        n = len(d.index)
        idx = np.arange(n)
        for col in ["sales", "profit", "qty", "asp"]:
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


def wants_advice(query: str, plan: dict | None = None) -> bool:
    """
    Decide if the user wants prescriptive advice/strategy vs a data report.
    No keywords; uses a small LLM JSON classifier.
    """
    try:
        sys_msg = (
            "Classify the user's intent as 'advice' (prescriptive actions/strategy) "
            "or 'report' (descriptive metrics). Return strict JSON: {\"advice\": true|false}."
        )
        usr_msg = f"User query: {query}\n\nPlanning context (optional): {plan or {}}"
        out = oa_client.chat.completions.create(
            model="gpt-4o",
            temperature=0,
            max_tokens=20,
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": sys_msg},
                {"role": "user", "content": usr_msg},
            ],
        )
        j = json.loads(out.choices[0].message.content or "{}")
        res = bool(j.get("advice", False))
        print(f"[DEBUG][advisor_gate] wants_advice={res}")
        return res
    except Exception as e:
        print("[DEBUG][advisor_gate] failed:", e)
        # safe fallback: don’t force advice mode if gate fails
        return False


_SKU_SHAPE = re.compile(r"^[A-Z0-9\-]{5,}$", re.I)   # generic SKU shape (tweak if needed)
_ASIN_SHAPE = re.compile(r"^[A-Z0-9]{10}$")          # Amazon ASIN shape (10 chars)

def is_valid_product_phrase(phrase: str, nlp=None) -> bool:
    """
    Accept only noun-ish phrases; reject pronouns/determiners/short junk.
    No hard-coded word lists.
    """
    if not isinstance(phrase, str):
        return False
    p = phrase.strip()
    if len(p) < 3:
        return False
    letters = sum(ch.isalpha() for ch in p)
    if letters < 2:
        return False
    # POS/NER gate via spaCy if available
    try:
        import spacy
        nlp = nlp or spacy.load("en_core_web_sm")
        doc = nlp(p)
        toks = [t for t in doc if not t.is_space]
        # reject if the whole chunk is PRON/DET
        if toks and all(t.pos_ in {"PRON", "DET"} for t in toks):
            return False
    except Exception:
        # If spaCy not available, we still have length/letters gating.
        pass
    return True

def is_valid_sku_token(token: str, engine, user_id: int, country: str) -> bool:
    """
    Accept only tokens that look like SKU/ASIN and exist in catalog.
    """
    if not isinstance(token, str):
        return False
    t = token.strip()
    if len(t) < 3:
        return False
    if not (_SKU_SHAPE.match(t) or _ASIN_SHAPE.match(t)):
        return False

    # Existence probe (use your monthly table)
    table = f"user_{user_id}_{country.lower()}_merge_data_of_all_months"
    try:
        from sqlalchemy import text
        with engine.connect() as conn:
            row = conn.execute(
                text(f"SELECT 1 FROM {table} WHERE LOWER(TRIM(sku)) = :s LIMIT 1"),
                {"s": t.lower().strip()},
            ).fetchone()
        return bool(row)
    except Exception:
        # Fallback to your semantic candidates
        rows = product_candidates(engine, user_id, country, t, limit=1) or []
        return bool(rows)

class FollowupMemory:
    def __init__(self, max_turns=3):
        self.buffer = []
        self.max_turns = max_turns

    def push(self, ctx: dict):
        if ctx:
            self.buffer.append(ctx)
            if len(self.buffer) > self.max_turns:
                self.buffer.pop(0)

    def get_recent(self):
        if not self.buffer:
            return None
        merged = {}
        for c in self.buffer:
            for k, v in c.items():
                if v and not merged.get(k):
                    merged[k] = v
        return merged







