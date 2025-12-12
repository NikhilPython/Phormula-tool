from __future__ import annotations
import os
import re
import logging
import calendar
from openai import OpenAI 
from typing import Any, Dict, List, Optional, Tuple
import json
import numpy as np
import pandas as pd
from dotenv import load_dotenv
from flask import Blueprint, jsonify, request
from sqlalchemy import create_engine, text
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from app.utils.chatbot_utils import FormulaEngine
import datetime as dt
import jwt
from app import db
from app.models.user_models import ChatHistory
from app.utils.chatbot_utils import generate_openai_answer, PENDING, parse_top_k, parse_country,parse_country_strict, parse_time_expr_or_none, slots_missing_for,  make_ask_prompt
from app.utils.chatbot_utils import plan_query, product_candidates, choose_best_candidate, route_intent, generate_general_answer,first_seen_by_sku, overview_metrics_for_period
from app.utils.chatbot_utils import _ym_to_span,_last_full_month_today,_normalize_plan_for_sku_language,infer_group_by, df_to_records_safe,_json_sanitize,_finalize_records,BusinessAdvisor
from app.utils.chatbot_utils import normalize_user_query,learn_metric_alias,resolve_product_entities,_is_all_products_phrase, wants_advice,is_valid_product_phrase,is_valid_sku_token, FollowupMemory
import re, calendar
import datetime as _dt
import pandas as pd
from sqlalchemy import text
import time
from datetime import date, timedelta
from dateutil.relativedelta import relativedelta
    


try:
    from sentence_transformers import SentenceTransformer
except Exception:
    SentenceTransformer = None

# ---------- env & setup ----------
load_dotenv()
SECRET_KEY = os.getenv("SECRET_KEY")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
DB_URL = os.getenv("DATABASE_URL", "postgresql://postgres:password@localhost:5432/phormula")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create SQLAlchemy engine
try:
    engine = create_engine(DB_URL, pool_pre_ping=True, pool_recycle=1800)
    logger.info("Database engine initialized successfully")
except Exception:
    logger.exception("DB engine init error")
    engine = None

try:
    oa_client = OpenAI(api_key=OPENAI_API_KEY)
    logger.info("OpenAI client initialized")
except Exception:
    logger.exception("Failed to init OpenAI client")
    oa_client = None

# Blueprint
chatbot_bp = Blueprint("chatbot_bp", __name__)




# --- Conversational fast-paths (no retrieval) ---
FOLLOWUP_MEMORY = FollowupMemory(max_turns=3)
_SMALLTALK = {"hi", "hello", "hey", "hiya", "yo", "howdy"}
_CAPABILITY_PATTERNS = [
    r"\b(what|how)\s+(can|do)\s+(you|u)\b",
    r"\bhow\s+do\s+you\s+work\b",
    r"\bwhat\s+do\s+you\s+do\b",
    r"\bhelp\b",
    r"\bexamples?\b",
]

def is_smalltalk(q: str) -> bool:
    s = (q or "").strip().lower()
    return (
        s in _SMALLTALK
        or s.startswith(("hi ", "hello ", "hey "))
        or s.startswith(("good morning", "good afternoon", "good evening"))
    )

def is_capability(q: str) -> bool:
    import re
    ql = (q or "").lower()
    return any(re.search(p, ql) for p in _CAPABILITY_PATTERNS)





# Optional Sentence Transformer
st_model = None
if SentenceTransformer is not None:
    try:
        st_model = SentenceTransformer("all-MiniLM-L6-v2")
        logger.info("Sentence transformer model loaded")
    except Exception as e:
        logger.warning("Error loading sentence transformer: %s", e)
        st_model = None


# ---------- helpers ----------
# ---------- Money debug helpers (raw only) ----------
def _num_series(s):
    import pandas as pd
    return pd.to_numeric(s.astype(str).str.replace(",", ""), errors="coerce").fillna(0.0)

MONETARY_FIELDS = [
    "product_sales",
    "product_sales_tax",
    "postage_credits",
    "shipping_credits",
    "shipping_credits_tax",
    "gift_wrap_credits",
    "giftwrap_credits_tax",
    "promotional_rebates",
    "promotional_rebates_tax",
    "marketplace_withheld_tax",
    "marketplace_facilitator_tax",
    "selling_fees",
    "fba_fees",
    "other_transaction_fees",
    "platform_fees",
    "advertising_cost",
    "other",
    "total",
]

def debug_money_sums(df, label="(slice)", fields=None, per="none", top=10):
    """
    Prints raw summed money columns for the current slice, optionally by SKU/product.
    per: "none" | "sku" | "product"
    """
    import pandas as pd
    fields = fields or MONETARY_FIELDS
    print(f"\n[DEBUG][money] {label} rows={len(df)}")

    # whole-slice totals (raw only)
    for col in fields:
        if col in df.columns:
            s = _num_series(df[col])
            raw = float(s.sum())
            nz = int((s != 0).sum())
            print(f"   {col:28s} raw={raw:>14,.2f}  nz={nz}")

    if per not in {"sku","product"}:
        return

    key = "sku" if per == "sku" else "product_name"
    if key not in df.columns:
        print(f"   [DEBUG][money] no '{key}' column; skip per-{per} breakdown")
        return

    g = df[[key] + [c for c in fields if c in df.columns]].copy()
    for c in fields:
        if c in g.columns:
            g[c] = _num_series(g[c])

    agg = g.groupby(key, dropna=True).sum(numeric_only=True).reset_index()

    # Order by product_sales if available
    order_col = "product_sales" if "product_sales" in agg.columns else ("total" if "total" in agg.columns else None)
    if order_col:
        agg = agg.sort_values(order_col, ascending=False)

    print(f"   [top {top} per {key}]")
    cols_to_show = [key] + [c for c in [
        "product_sales","selling_fees","fba_fees","other_transaction_fees",
        "platform_fees","advertising_cost",
        "promotional_rebates","postage_credits","shipping_credits",
        "product_sales_tax","marketplace_facilitator_tax","other"
    ] if c in agg.columns]
    print(agg.head(top)[cols_to_show].to_string(index=False))
######################################################################################################################################################################
def _decode_jwt_or_401(auth_header: Optional[str]) -> Tuple[Optional[int], Optional[str]]:
    if not auth_header or not auth_header.startswith("Bearer "):
        return None, "Authorization token is missing or invalid"
    token = auth_header.split(" ")[1]
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=["HS256"])
        return int(payload["user_id"]), None
    except jwt.ExpiredSignatureError:
        return None, "Token has expired"
    except jwt.InvalidTokenError:
        return None, "Invalid token"
    except Exception as e:
        return None, f"Auth error: {e}"

def _safe_like(val: str) -> str:
    return val.replace("'", "''")


def save_chat_to_db(user_id, message, response):
    """Save chat message and response to database"""
    try:
        rec = ChatHistory(
            user_id=user_id,
            message=message[:1000],
            response=response[:2000],
            timestamp=dt.datetime.utcnow(),
        )
        db.session.add(rec)
        db.session.commit()
        return rec.id
    except Exception as e:
        db.session.rollback()
        logger.exception("Failed to save chat to database")
        return None
    
_LAST_N_RE = re.compile(r"\b(last|past|previous)\s+(\d+)\s+months?\b", re.I)
_THIS_MONTH_RE = re.compile(r"\bthis\s+month\b", re.I)
_LAST_MONTH_RE = re.compile(r"\blast\s+month\b", re.I)

def month_add(year: int, month: int, delta_months: int) -> tuple[int, int]:
    y = year + (month - 1 + delta_months) // 12
    m = (month - 1 + delta_months) % 12 + 1
    return (y, m)

def get_earliest_data_year_month(user_id: int, country: Optional[str]) -> tuple[int, int]:
    """
    Returns (year, month) of the earliest available row for the user/country.
    """
    if not engine:
        return (1970, 1)

    if country and str(country).lower() in ["uk", "us"]:
        table = f"user_{user_id}_{str(country).lower()}_merge_data_of_all_months"
    else:
        table = f"user_{user_id}_total_country_global_data"

    sql = text(f"""
        SELECT
          MIN(
            make_date(
              CASE WHEN year ~ '^[0-9]+$' THEN CAST(year AS INT) ELSE NULL END,
              CASE
                WHEN month ~ '^[0-9]+$' THEN CAST(month AS INT)
                WHEN LOWER(month) IN ('jan','january') THEN 1
                WHEN LOWER(month) IN ('feb','february') THEN 2
                WHEN LOWER(month) IN ('mar','march') THEN 3
                WHEN LOWER(month) IN ('apr','april') THEN 4
                WHEN LOWER(month) = 'may' THEN 5
                WHEN LOWER(month) IN ('jun','june') THEN 6
                WHEN LOWER(month) IN ('jul','july') THEN 7
                WHEN LOWER(month) IN ('aug','august') THEN 8
                WHEN LOWER(month) IN ('sep','sept','september') THEN 9
                WHEN LOWER(month) IN ('oct','october') THEN 10
                WHEN LOWER(month) IN ('nov','november') THEN 11
                WHEN LOWER(month) IN ('dec','december') THEN 12
                ELSE NULL
              END,
              1
            )
          ) AS min_month_date
        FROM {table}
    """)
    try:
        with engine.connect() as conn:
            row = conn.execute(sql).fetchone()
            d = row[0]
            if d is None:
                return (1970, 1)
            return (int(d.year), int(d.month))
    except Exception:
        logger.exception("get_earliest_data_year_month failed")
        return (1970, 1)


def get_latest_data_year_month(user_id: int, country: Optional[str]) -> tuple[int, int]:
    """
    Returns (year, month) of the latest available row for the user/country.
    """
    if not engine:
        return (1970, 1)

    if country and str(country).lower() in ["uk", "us"]:
        table = f"user_{user_id}_{str(country).lower()}_merge_data_of_all_months"
    else:
        table = f"user_{user_id}_total_country_global_data"

    sql = text(f"""
        SELECT
          MAX(
            make_date(
              CASE WHEN year ~ '^[0-9]+$' THEN CAST(year AS INT) ELSE NULL END,
              CASE
                WHEN month ~ '^[0-9]+$' THEN CAST(month AS INT)
                WHEN LOWER(month) IN ('jan','january') THEN 1
                WHEN LOWER(month) IN ('feb','february') THEN 2
                WHEN LOWER(month) IN ('mar','march') THEN 3
                WHEN LOWER(month) IN ('apr','april') THEN 4
                WHEN LOWER(month) = 'may' THEN 5
                WHEN LOWER(month) IN ('jun','june') THEN 6
                WHEN LOWER(month) IN ('jul','july') THEN 7
                WHEN LOWER(month) IN ('aug','august') THEN 8
                WHEN LOWER(month) IN ('sep','sept','september') THEN 9
                WHEN LOWER(month) IN ('oct','october') THEN 10
                WHEN LOWER(month) IN ('nov','november') THEN 11
                WHEN LOWER(month) IN ('dec','december') THEN 12
                ELSE NULL
              END,
              1
            )
          ) AS max_month_date
        FROM {table}
    """)
    try:
        with engine.connect() as conn:
            row = conn.execute(sql).fetchone()
            d = row[0]
            if d is None:
                return (1970, 1)
            return (int(d.year), int(d.month))
    except Exception:
        logger.exception("get_latest_data_year_month failed")
        return (1970, 1)
    
def get_data_span_year_month(user_id: int, country: Optional[str]) -> tuple[tuple[int, int], tuple[int, int]]:
    """
    Returns ((earliest_year, earliest_month), (latest_year, latest_month))
    for the user/country, computed in a single query.
    """
    if not engine:
        return (1970, 1), (1970, 1)

    if country and str(country).lower() in ["uk", "us"]:
        table = f"user_{user_id}_{str(country).lower()}_merge_data_of_all_months"
    else:
        table = f"user_{user_id}_total_country_global_data"

    sql = text(f"""
        SELECT
          MIN(
            make_date(
              CASE WHEN year ~ '^[0-9]+$' THEN CAST(year AS INT) ELSE NULL END,
              CASE
                WHEN month ~ '^[0-9]+$' THEN CAST(month AS INT)
                WHEN LOWER(month) IN ('jan','january') THEN 1
                WHEN LOWER(month) IN ('feb','february') THEN 2
                WHEN LOWER(month) IN ('mar','march') THEN 3
                WHEN LOWER(month) IN ('apr','april') THEN 4
                WHEN LOWER(month) = 'may' THEN 5
                WHEN LOWER(month) IN ('jun','june') THEN 6
                WHEN LOWER(month) IN ('jul','july') THEN 7
                WHEN LOWER(month) IN ('aug','august') THEN 8
                WHEN LOWER(month) IN ('sep','sept','september') THEN 9
                WHEN LOWER(month) IN ('oct','october') THEN 10
                WHEN LOWER(month) IN ('nov','november') THEN 11
                WHEN LOWER(month) IN ('dec','december') THEN 12
                ELSE NULL
              END,
              1
            )
          ) AS min_month_date,
          MAX(
            make_date(
              CASE WHEN year ~ '^[0-9]+$' THEN CAST(year AS INT) ELSE NULL END,
              CASE
                WHEN month ~ '^[0-9]+$' THEN CAST(month AS INT)
                WHEN LOWER(month) IN ('jan','january') THEN 1
                WHEN LOWER(month) IN ('feb','february') THEN 2
                WHEN LOWER(month) IN ('mar','march') THEN 3
                WHEN LOWER(month) IN ('apr','april') THEN 4
                WHEN LOWER(month) = 'may' THEN 5
                WHEN LOWER(month) IN ('jun','june') THEN 6
                WHEN LOWER(month) IN ('jul','july') THEN 7
                WHEN LOWER(month) IN ('aug','august') THEN 8
                WHEN LOWER(month) IN ('sep','sept','september') THEN 9
                WHEN LOWER(month) IN ('oct','october') THEN 10
                WHEN LOWER(month) IN ('nov','november') THEN 11
                WHEN LOWER(month) IN ('dec','december') THEN 12
                ELSE NULL
              END,
              1
            )
          ) AS max_month_date
        FROM {table}
    """)
    try:
        with engine.connect() as conn:
            row = conn.execute(sql).fetchone()
            min_d, max_d = row[0], row[1]
            if min_d is None or max_d is None:
                return (1970, 1), (1970, 1)
            return (int(min_d.year), int(min_d.month)), (int(max_d.year), int(max_d.month))
    except Exception:
        logger.exception("get_data_span_year_month failed")
        return (1970, 1), (1970, 1)

############################################################################################

# --- Regex for relative periods ---
# --- Regex for relative periods ---
_THIS_QUARTER_RE      = re.compile(r"\b(this|current)\s+(qtr|quarter|qtd|q)\b", re.I)
_LAST_QUARTER_RE      = re.compile(r"\b(last|previous)\s+(qtr|quarter|qtd|q)\b", re.I)
_LAST_N_QUARTERS_RE   = re.compile(r"\blast\s+(\d+)\s+(qtr|quarter|qtd|q)(s)?\b", re.I)
_QX_RE                = re.compile(r"\b(qtr|quarter|qtd|q)\s*([1-4])(?:\s+(\d{4}))?\b", re.I)

_THIS_YEAR_RE         = re.compile(r"\b(this|current)\s+year\b", re.I)
_LAST_YEAR_RE         = re.compile(r"\b(last|previous)\s+year\b", re.I)
_LAST_N_YEARS_RE      = re.compile(r"\blast\s+(\d+)\s+years?\b", re.I)
_YTD_RE               = re.compile(r"\b(ytd|year\s*to\s*date)\b", re.I)

def _quarter_to_months(q: int) -> tuple[int, int]:
    """Map quarter number to (start_month, end_month)."""
    return (3 * (q - 1) + 1, 3 * q)

def _last_full_month_today(today: dt.date | None = None) -> tuple[int, int]:
    """Return (year, month) for the last fully completed month."""
    today = today or dt.date.today()
    y, m = today.year, today.month
    m -= 1
    if m == 0:
        y -= 1
        m = 12
    return y, m


def clamp_relative_time_to_available(user_id: int, country: Optional[str], raw_query: str) -> Optional[dict]:
    """
    Resolve relative time phrases in the user's query into an explicit [start, end] date span,
    clamped to ACTUAL data availability (DB min/max) and the last full month.

    Covers:
      - "till now" / "to date" / "so far" / "overall" / "all time" / "until now"
        → [earliest_in_DB .. latest_in_DB clamped]
      - "last N months/weeks/quarters/years"
      - "this year", "last year", "YTD"
      - Year-only mentions ("2025 revenue") → full-year clamped
      - Ignores future or upcoming queries
    """
    q = (raw_query or "").lower().strip()

    # -------------------------------------------------------------------------
    # Skip "future" / "next ..." phrases — planner handles them separately
    # -------------------------------------------------------------------------
    if re.search(r"\b(future|upcoming|next\s+(month|quarter|year))\b", q):
        return None

    # -------------------------------------------------------------------------
    # DB boundaries
    # -------------------------------------------------------------------------
    max_y, max_m = get_latest_data_year_month(user_id, country)
    min_y, min_m = get_earliest_data_year_month(user_id, country)
    if (max_y, max_m) == (1970, 1):
        return None  # no data

    today = dt.date.today()
    today_y, today_m = today.year, today.month
    last_full_y, last_full_m = _last_full_month_today(today)

    def clamp_end(y: int, m: int) -> tuple[int, int]:
        # ensures we never exceed last full month or DB max
        return min([(y, m), (last_full_y, last_full_m), (max_y, max_m)])

    # -------------------------------------------------------------------------
    # “till now / to date / overall / all time” → [DB_MIN .. DB_MAX_CLAMPED]
    # -------------------------------------------------------------------------
    if re.search(r"\b(till\s+now|to\s+date|so\s+far|overall|all[-\s]?time|until\s+now)\b", q):
        ey, em = clamp_end(max_y, max_m)
        sy, sm = (min_y, min_m)
        return {"start": f"{sy:04d}-{sm:02d}-01", "end": _ym_to_span(ey, em)["end"]}

    # -------------------------------------------------------------------------
    # Year-only mentions → full-year span, clamped to availability
    # -------------------------------------------------------------------------
    m_years = re.findall(r"\b(20\d{2})\b", q)
    if m_years and not re.search(r"\b(last|this|ytd)\s+year\b", q):
        year = int(m_years[0])
        ey, em = clamp_end(year, 12)
        return {"start": f"{year:04d}-01-01", "end": _ym_to_span(ey, em)["end"]}

    # -------------------------------------------------------------------------
    # Relative periods: “last/past/previous N unit(s)”
    # (months, quarters, weeks, years)
    # -------------------------------------------------------------------------
    mrel = re.search(r"\b(last|past|previous)\s+(\d+)\s+(months?|quarters?|weeks?|years?)\b", q)
    if mrel:
        n = max(1, min(int(mrel.group(2)), 60))
        unit = mrel.group(3).lower().rstrip("s")

        ey, em = clamp_end(last_full_y, last_full_m)
        end_span = _ym_to_span(ey, em)["end"]

        if unit == "month":
            sy, sm = month_add(ey, em, -(n - 1))
            return {"start": f"{sy:04d}-{sm:02d}-01", "end": end_span}

        if unit == "quarter":
            this_q = (today_m - 1) // 3 + 1
            end_q, end_y = this_q - 1, today_y
            if end_q <= 0:
                end_q, end_y = 4, end_y - 1
            _, end_m = _quarter_to_months(end_q)
            ey, em = clamp_end(end_y, end_m)
            end_span = _ym_to_span(ey, em)["end"]
            y, qnum = end_y, end_q
            for _ in range(n - 1):
                qnum -= 1
                if qnum == 0:
                    qnum, y = 4, y - 1
            start_m, _ = _quarter_to_months(qnum)
            return {"start": f"{y:04d}-{start_m:02d}-01", "end": end_span}

        if unit == "week":
            end_date = dt.date(ey, em, calendar.monthrange(ey, em)[1])
            start_date = end_date - dt.timedelta(weeks=n)
            db_min = dt.date(min_y, min_m, 1)
            if start_date < db_min:
                start_date = db_min
            return {"start": start_date.isoformat(), "end": end_date.isoformat()}

        if unit == "year":
            end_y, end_m = clamp_end(today_y, 12)
            end_span = _ym_to_span(end_y, end_m)["end"]
            start_y = end_y - (n - 1)
            return {"start": f"{start_y:04d}-01-01", "end": end_span}

    # -------------------------------------------------------------------------
    # Quarter, month, and year shorthand fallbacks
    # -------------------------------------------------------------------------
    # Last N quarters (e.g. "last 2 quarters")
    mobj_q = _LAST_N_QUARTERS_RE.search(q)
    if mobj_q:
        n = max(1, min(int(mobj_q.group(1)), 12))
        this_q = (today_m - 1) // 3 + 1
        end_q, end_y = this_q - 1, today_y
        if end_q <= 0:
            end_q, end_y = 4, end_y - 1
        _, end_m = _quarter_to_months(end_q)
        ey, em = clamp_end(end_y, end_m)
        end_span = _ym_to_span(ey, em)["end"]
        y, qnum = end_y, end_q
        for _ in range(n - 1):
            qnum -= 1
            if qnum == 0:
                qnum, y = 4, y - 1
        start_m, _ = _quarter_to_months(qnum)
        return {"start": f"{y:04d}-{start_m:02d}-01", "end": end_span}

    # This year / Last year / YTD
    if _THIS_YEAR_RE.search(q):
        ey, em = clamp_end(today_y, today_m)
        return {"start": f"{today_y:04d}-01-01", "end": _ym_to_span(ey, em)["end"]}

    if _LAST_YEAR_RE.search(q):
        y = today_y - 1
        ey, em = clamp_end(y, 12)
        return {"start": f"{y:04d}-01-01", "end": _ym_to_span(ey, em)["end"]}

    if _YTD_RE.search(q):
        ey, em = clamp_end(today_y, today_m)
        end_date = min(dt.date.today(), dt.date(ey, em, calendar.monthrange(ey, em)[1]))
        return {"start": f"{today_y:04d}-01-01", "end": end_date.isoformat()}

    # -------------------------------------------------------------------------
    # Fallback: nothing matched
    # -------------------------------------------------------------------------
    return None




    


# ---------- vector DB (for fuzzy recall) ----------
class VectorDatabase:
    def __init__(self):
        self.vectorizer = TfidfVectorizer(max_features=100_000, stop_words="english", ngram_range=(1, 2))
        self.vectors_by_key: Dict[str, Any] = {}   # key -> sparse matrix
        self.data_cache: Dict[str, pd.DataFrame] = {}

    def _key(self, user_id: str, country: Optional[str]) -> str:
        return f"{user_id}_{(country or 'global').lower()}"

    def _table_for(self, user_id: str, country: Optional[str]) -> str:
        if country and country.lower() in ["uk", "us"]:
            return f"user_{user_id}_{country.lower()}_merge_data_of_all_months"
        return f"user_{user_id}_total_country_global_data"

    def initialize_vectors(self, user_id: str, country: Optional[str] = None) -> bool:
        if not engine:
            logger.error("DB engine is not initialized")
            return False
        try:
            key = self._key(user_id, country)
            table_name = self._table_for(user_id, country)
            # Select only useful textual columns to keep memory in check
            cols = [
                "product_name","description","sku","type","order_id","order_city",
                "order_state","country","month","year"
            ]
            col_list = ", ".join(cols)
            with engine.connect() as conn:
                result = conn.execute(text(f"SELECT {col_list} FROM {table_name} LIMIT 100000"))
                df = pd.DataFrame(result.fetchall(), columns=result.keys())

            if df.empty:
                logger.warning("No data found in table %s", table_name)
                return False

            searchable = df.fillna("").astype(str).agg(" | ".join, axis=1).tolist()
            vectors = self.vectorizer.fit_transform(searchable)
            self.vectors_by_key[key] = vectors
            self.data_cache[key] = df
            logger.info("Vector DB initialized for %s with %d rows", table_name, len(df))
            return True
        except Exception:
            logger.exception("initialize_vectors error")
            return False

    def semantic_search(self, query: str, user_id: str, country: Optional[str] = None, top_k: int = 5):
        try:
            key = self._key(user_id, country)
            if key not in self.data_cache or key not in self.vectors_by_key:
                if not self.initialize_vectors(user_id, country):
                    return []
            vectors = self.vectors_by_key[key]
            sims = cosine_similarity(self.vectorizer.transform([query]), vectors).flatten()
            top_idx = np.argsort(sims)[-top_k:][::-1]
            df = self.data_cache[key]
            res = []
            for idx in top_idx:
                if sims[idx] > 0.05:
                    res.append({
                        "data": df.iloc[idx].to_dict(),
                        "similarity": float(sims[idx]),
                        "row_index": int(idx),
                    })
            return res
        except Exception:
            logger.exception("semantic_search error")
            return []


# ---------- parsing & filters ----------
class FilterParser:
    """Extracts predicates from natural language queries. Conservative by design."""

    NUMERIC_COLS = {"price_in_gbp","cost_of_unit_sold","quantity","product_sales","total",
                    "sales_tax_collected","product_sales_tax","shipping_credits","platform_fees","advertising_cost",}
    TEXT_COLS = {"order_id","sku","product_name","order_city","order_state","country","type"}

    MONTH_WORDS = {m.lower() for m in list(calendar.month_name)[1:] + list(calendar.month_abbr)[1:]}
    COUNTRY_WORDS = {"uk","us","usa","united kingdom","united states"}

    # ----- Precompiled regex (perf + correctness) -----
    _YEAR_RE = re.compile(r"\b(20\d{2})\b", re.I)
    _NUM_MONTH_RE = re.compile(r"\b(?:month|mon|m)\s*[:\-]?\s*(0?[1-9]|1[0-2])\b", re.I)
    _QUARTER_RE = re.compile(r"\bq([1-4])\b", re.I)
    _WEEK_OF_MONTH_RE = re.compile(
        r"\b(?:(\d+(?:st|nd|rd|th))|(first|second|third|fourth|fifth))\s+week\s+of\s+([a-z]+)\s+(20\d{2})\b",
        re.I
    )

    # Robust, word-boundary guarded month patterns (fixes "margin"→"Mar")
    MONTH_PATTERNS = [
        (re.compile(r"\bjan(?:uary)?\b", re.I), 1,  "January"),
        (re.compile(r"\bfeb(?:ruary)?\b", re.I), 2,  "February"),
        (re.compile(r"\bmar(?:ch)?\b", re.I), 3,  "March"),
        (re.compile(r"\bapr(?:il)?\b", re.I), 4,  "April"),
        (re.compile(r"\bmay\b", re.I),         5,  "May"),
        (re.compile(r"\bjun(?:e)?\b", re.I),   6,  "June"),
        (re.compile(r"\bjul(?:y)?\b", re.I),   7,  "July"),
        (re.compile(r"\baug(?:ust)?\b", re.I), 8,  "August"),
        (re.compile(r"\bsep(?:t(?:ember)?)?\b", re.I), 9,  "September"),  # sep|sept|september
        (re.compile(r"\boct(?:ober)?\b", re.I), 10, "October"),
        (re.compile(r"\bnov(?:ember)?\b", re.I), 11, "November"),
        (re.compile(r"\bdec(?:ember)?\b", re.I), 12, "December"),
    ]

    def __init__(self):
        self.intent_patterns = {
            "aggregation": ["total","sum","average","count","max","min","avg"],
            "time_based": ["month","year","date","time","daily","monthly","yearly","week"],
            "comparison": ["compare","vs","versus","difference","between"],
            "filtering": ["where","filter","show","find","get","with"],
            "grouping": ["by","group","breakdown","category","type"],
            "sorting": ["top","bottom","highest","lowest","best","worst","recent","latest","new"],
        }

    def extract_intent(self, query: str) -> Dict[str, Any]:
        ql = query.lower()
        intents = {k: sum(1 for w in ws if w in ql) for k, ws in self.intent_patterns.items()}
        intents = {k: v for k, v in intents.items() if v > 0}
        return {"intents": intents, "raw_query": query}

    @staticmethod
    def _ord_to_int(word: str) -> Optional[int]:
        word = word.strip().lower()
        mapping = {"1st":1,"first":1,"2nd":2,"second":2,"3rd":3,"third":3,"4th":4,"fourth":4,"5th":5,"fifth":5}
        return mapping.get(word)

    def parse_time(self, query: str) -> Dict[str, Any]:
        """
        Robust, context-tolerant time parser for chatbot inputs.

        Supports:
        - Absolute dates: '2024-07-15', '15 Jul 2024'
        - Month/year: 'March 2023', '2023-03'
        - Quarters: 'Q2 2024', 'quarter 3 last year'
        - Relative spans: 'last 3 months', 'past 2 quarters', 'previous 5 days'
        - Connectors: 'Jan 2024 to Mar 2024', 'Feb vs Mar 2023'
        - Open markers: 'till now', 'to date', 'so far', 'overall', 'all time'
        Returns dictionary containing any of:
            explicit_day, months, years, date_range (UTC), relative='till_now'
        """
        q = " ".join((query or "").split())
        ql = q.lower()
        out: Dict[str, Any] = {}

        # ---------- explicit calendar day ----------
        m = re.search(r"\b(20\d{2})-(0?[1-9]|1[0-2])-(0?[1-9]|[12]\d|3[01])\b", ql)
        if m:
            y, mo, d = map(int, m.groups())
            out["explicit_day"] = (y, mo, d)
            return out

        m = re.search(
            r"\b(0?[1-9]|[12]\d|3[01])\s+"
            r"(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|"
            r"aug(?:ust)?|sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)"
            r"\s+(20\d{2})\b", ql, re.I
        )
        if m:
            d = int(m.group(1))
            mon = ["jan","feb","mar","apr","may","jun","jul","aug","sep","oct","nov","dec"].index(m.group(2)[:3].lower()) + 1
            y = int(m.group(3))
            out["explicit_day"] = (y, mon, d)
            return out

        # ---------- years ----------
        years = [int(y) for y in self._YEAR_RE.findall(ql)]
        if years:
            out["years"] = years

        # ---------- months (both word and numeric) ----------
        found_positions: list[tuple[int,int,str]] = []
        for pat, num, name in self.MONTH_PATTERNS:
            for m in pat.finditer(ql):
                found_positions.append((m.start(), num, name))
        for m in self._NUM_MONTH_RE.findall(ql):
            found_positions.append((ql.find(m), int(m), calendar.month_name[int(m)]))
        found_positions.sort(key=lambda x: x[0])

        months: list[dict] = []
        seen: set[int] = set()
        for _, num, name in found_positions:
            if 1 <= int(num) <= 12 and int(num) not in seen:
                months.append({"name": name, "number": int(num)})
                seen.add(int(num))
        if months:
            out["months"] = months

        # ---------- quarter detection ----------
        m = re.search(r"\b(?:q(?:tr)?|quarter|qtd)\s*([1-4])(?:\s+(20\d{2}))?\b", ql, re.I)
        if m:
            qnum = int(m.group(1))
            qmonths = {1:[1,2,3],2:[4,5,6],3:[7,8,9],4:[10,11,12]}[qnum]
            out["months"] = (out.get("months") or []) + [{"name": calendar.month_name[i], "number": i} for i in qmonths]
            if m.group(2):
                out["years"] = list(sorted(set((out.get("years") or []) + [int(m.group(2))])))

        # ---------- nth week of month ----------
        week_pat = self._WEEK_OF_MONTH_RE.search(ql)
        if week_pat:
            ord_str = week_pat.group(1) or week_pat.group(2)
            wk = self._ord_to_int(ord_str)
            month_str = week_pat.group(3)
            year_val = int(week_pat.group(4))
            mon_num = None
            for i in range(1, 13):
                if month_str.startswith(calendar.month_name[i].lower()) or month_str.startswith(calendar.month_abbr[i].lower()):
                    mon_num = i
                    break
            if wk and mon_num:
                last_day = calendar.monthrange(year_val, mon_num)[1]
                start_day = min(max(1 + (wk - 1) * 7, 1), last_day)
                end_day = min(start_day + 6, last_day)
                s = dt.date(year_val, mon_num, start_day)
                e = dt.date(year_val, mon_num, end_day)
                out["date_range"] = (
                    dt.datetime.combine(s, dt.time.min, tzinfo=dt.timezone.utc),
                    dt.datetime.combine(e, dt.time.max, tzinfo=dt.timezone.utc),
                )
                out["months"] = [{"name": calendar.month_name[mon_num], "number": mon_num}]
                out["years"] = [year_val]

        # ---------- generic relative spans ----------
        rel = re.search(r"\b(last|past|previous)\s+(\d{1,3})\s+(days?|weeks?|months?|quarters?|years?)\b", ql)
        if rel:
            n = max(1, int(rel.group(2)))
            unit = rel.group(3).rstrip("s")
            today = dt.date.today()

            def _daterange(a: dt.date, b: dt.date):
                return (dt.datetime.combine(a, dt.time.min, tzinfo=dt.timezone.utc),
                        dt.datetime.combine(b, dt.time.max, tzinfo=dt.timezone.utc))

            if unit == "day":
                start = today - dt.timedelta(days=n)
                out["date_range"] = _daterange(start, today)
            elif unit == "week":
                start = today - dt.timedelta(weeks=n)
                out["date_range"] = _daterange(start, today)
            elif unit == "month":
                y, m = today.year, today.month
                for _ in range(n):
                    m -= 1
                    if m == 0:
                        y -= 1
                        m = 12
                out["date_range"] = _daterange(dt.date(y, m, 1), today)
            elif unit == "quarter":
                months_back = 3 * n
                y, m = today.year, today.month
                while months_back:
                    m -= 1
                    if m == 0:
                        y -= 1
                        m = 12
                    months_back -= 1
                out["date_range"] = _daterange(dt.date(y, m, 1), today)
            elif unit == "year":
                out["date_range"] = _daterange(dt.date(today.year - (n-1), 1, 1), today)

        # ---------- connectors: 'Jan 2023 to Mar 2023' / 'Feb vs Mar 2024' ----------
        if not out.get("date_range"):
            mon_pat = r"(jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)[a-z]*"
            pairs = re.findall(rf"\b{mon_pat}\s+(20\d{{2}})\b", ql, flags=re.I)
            if len(pairs) >= 2 and (" vs " in ql or re.search(r"\b(to|through|till|until|–|—|-|between)\b", ql)):
                def _mi(s3): return ["jan","feb","mar","apr","may","jun","jul","aug","sep","sept","oct","nov","dec"].index(s3[:3].lower()) + 1
                m1, y1 = _mi(pairs[0][0]), int(pairs[0][1])
                m2, y2 = _mi(pairs[1][0]), int(pairs[1][1])
                s = dt.date(y1, m1, 1)
                last = calendar.monthrange(y2, m2)[1]
                e = dt.date(y2, m2, last)
                out["date_range"] = (
                    dt.datetime.combine(s, dt.time.min, tzinfo=dt.timezone.utc),
                    dt.datetime.combine(e, dt.time.max, tzinfo=dt.timezone.utc),
                )
                out["months"] = [{"name": calendar.month_name[m1], "number": m1}, {"name": calendar.month_name[m2], "number": m2}]
                out["years"] = list(sorted(set([y1, y2])))

        # ---------- year-only expansion ----------
        if out.get("years") and not out.get("date_range") and not out.get("months"):
            y = out["years"][0]
            s = dt.date(y, 1, 1)
            e = dt.date(y, 12, 31)
            out["date_range"] = (
                dt.datetime.combine(s, dt.time.min, tzinfo=dt.timezone.utc),
                dt.datetime.combine(e, dt.time.max, tzinfo=dt.timezone.utc),
            )

        # ---------- till now / to date / so far ----------
        if re.search(r"\b(till\s+now|to\s+date|so\s+far|overall|all[-\s]*time|until\s+now)\b", ql):
            out["relative"] = "till_now"

        return out




    def parse_columns(self, query: str) -> List[str]:
        ql = query.lower()
        standard = [
            "date_time","settlement_id","type","order_id","sku","description",
            "quantity","price_in_gbp","cost_of_unit_sold","order_city","order_state",
            "product_sales","product_sales_tax","postage_credits","shipping_credits",
            "platform_fees","advertising_cost",
            "total","month","year","product_name","country",
        ]
        res = [c for c in standard if c in ql or c.replace("_"," ") in ql]

        concept_map = {
            "sales": ["product_sales","total"],
            "revenue": ["product_sales","total"],
            "profit": ["product_sales","total"],
            "quantity": ["quantity"],  # base behavior kept
            "price": ["price_in_gbp","cost_of_unit_sold"],
            "cost": ["cost_of_unit_sold"],
            "fees": ["total"],   # fees live in 'total' on fee lines
            "tax": ["product_sales_tax"],
            "location": ["order_city","order_state","country"],
            "product": ["sku","product_name"],
            "time": ["date_time","month","year"],
        }
        for concept, cols in concept_map.items():
            if concept in ql:
                res.extend(cols)

        # ASP needs sales + units and we sign units using `type`
        if any(p in ql for p in ("asp", "average selling price", "avg selling price")):
            res.extend(["product_sales", "quantity", "type"])

        # Quantity-sold / units wording → include `type`
        if any(p in ql for p in ("quantity sold", "units sold", "qty sold", "units", "qty", "unit")):
            res.extend(["quantity", "type"])

        return sorted(set(res))

    def parse_filters(self, query: str) -> Dict[str, Any]:
        ql = query.lower()
        f: Dict[str, Any] = {"equals": [], "in_list": [], "numeric": [], "between": [], "text_like": []}

        def add_equal(col: str, val: str):
            if not val: return
            item = (col.lower(), str(val).strip())
            if item not in f["equals"]:
                f["equals"].append(item)

        def add_in_list(col: str, vals: List[str]):
            col = col.lower()
            cleaned = [v.strip() for v in vals if str(v).strip()]
            if not cleaned: return
            item = (col, cleaned)
            if item not in f["in_list"]:
                f["in_list"].append(item)

        def add_text_like(col: str, val: str):
            item = (col.lower(), val)
            if item not in f["text_like"]:
                f["text_like"].append(item)

        # Robust fee synonyms → ILIKE
        if any(t in ql for t in ["amazon fee","amazon fees"]):
            add_text_like("type","amazon"); add_text_like("type","fee")
        if any(t in ql for t in ["fba fee","fba fees","fba inventory fee","inventory fee"]):
            add_text_like("type","fba"); add_text_like("type","fee")
        if any(t in ql for t in ["refund","refunds"]):
            add_equal("type","refund")

        # country hints
        if re.search(r"\b(united kingdom| in uk| in the uk|\buk\b)\b", ql):
            add_equal("country","UK")
        if re.search(r"\b(united states| in us| in the us|\bus\b|\busa\b)\b", ql):
            add_equal("country","US")

        # quoted → exact product_name
        for txt in re.findall(r'"([^"]+)"', query):
            name = txt.strip()
            if name:
                add_equal("product_name", name)

        # explicit equality
        eq_patterns = [
            r"\b(order_id|sku|order_city|order_state|country|type|product_name)\s*(?:=|:)\s*\"([^\"]+)\"",
            r"\b(order_id|sku|order_city|order_state|country|type|product_name)\s*(?:=|:)\s*([\w\-]+)",
        ]
        for pat in eq_patterns:
            for col, val in re.findall(pat, query, flags=re.IGNORECASE):
                add_equal(col, val)

        # IN (...) lists
        for col, inner in re.findall(r"\b(\w+)\s+in\s*\(([^)]+)\)", query, flags=re.IGNORECASE):
            if col.lower() == "description":
                continue
            raw_vals = [v.strip() for v in inner.split(",")]
            vals = [v.strip().strip("'\"") for v in raw_vals]
            add_in_list(col, vals)

        # numeric comparisons
        for col, op, val in re.findall(
            r"\b(price_in_gbp|cost_of_unit_sold|quantity|product_sales|total|product_sales_tax|shipping_credits|platform_fees|advertising_cost)\s*(>=|<=|=|>|<)\s*([0-9]+(?:\.[0-9]+)?)",
            ql, flags=re.IGNORECASE):
            f["numeric"].append((col.lower(), op, float(val)))

        # between
        for col, lo, hi in re.findall(
            r"\b(price_in_gbp|cost_of_unit_sold|quantity|product_sales|total|platform_fees|advertising_cost)\s+between\s+([0-9]+(?:\.[0-9]+)?)\s+and\s+([0-9]+(?:\.[0-9]+)?)",
            ql, flags=re.IGNORECASE):
            f["between"].append((col.lower(), float(lo), float(hi)))

        # prune empties
        for k in list(f.keys()):
            if not f[k]:
                del f[k]
        return f

    # ADD THIS ↓↓↓
    @staticmethod
    def needs_sql(filters: Dict[str, Any]) -> bool:
        """Return True when we have explicit filters so we should run SQL instead of vector recall."""
        return bool(filters)

    @staticmethod
    def _looks_like_money(text: str) -> bool:
        return bool(re.search(r"\d", text)) or any(w in text for w in ["pound","pounds","gbp","usd","eur","rs","inr","cad","aud"])

    def guess_product_phrase(self, query: str) -> Optional[str]:
        """
        Try to infer product or SKU from free text.

        Dynamically strips metric prefixes (from FormulaEngine) so phrases like:
        - "sales of classic" → "classic"
        - "units classic" → "classic"
        - "change in revenue for wipes" → "wipes"
        """

        # --- learn metric terms dynamically from your FormulaEngine ---
        def _metric_terms_cached() -> set:
            try:
                fe = FormulaEngine()
                terms = set()
                # Metrics you actually compute:
                terms |= {str(k).lower() for k in fe.registry.keys()}
                # Human phrases → canonical metric names:
                terms |= {str(k).lower() for k in fe.aliases.keys()}
                terms |= {str(v).lower() for v in fe.aliases.values()}
                # Also add single tokens from multi-word aliases
                for t in list(terms):
                    for tok in re.split(r"[^a-z0-9]+", t):
                        if tok:
                            terms.add(tok.lower())
                return terms
            except Exception as e:
                print("[DEBUG] metric_terms_cached failed:", e)
                return set()

        METRIC_TERMS = _metric_terms_cached()

        q = " ".join(str(query or "").split())
        ql = q.lower()

        print("\n[DEBUG] guess_product_phrase() called with:", repr(query))

        # ---------- small helper to strip trailing trend/aux words ----------
        def _clean_trailing_trend_words(s: str) -> str:
            if not s:
                return s
            noise = {
                "is","are","was","were","be","being","been",
                "increasing","decreasing","rising","falling",
                "growing","improving","worsening","up","down",
                "changed","change","increase","decrease"
            }
            toks = [t for t in s.strip().split() if t.lower() not in noise]
            return " ".join(toks).strip()

        def _strip_metric_prefix(cand: str) -> str:
            """Remove leading metric words dynamically using FormulaEngine terms."""
            toks = cand.split()
            while toks and toks[0].lower() in METRIC_TERMS:
                print(f"[DEBUG] stripping metric prefix:", toks[0])
                toks.pop(0)
                if toks and toks[0].lower() in {"of", "for", "in"}:
                    print(f"[DEBUG] stripping connector after metric:", toks[0])
                    toks.pop(0)
            return " ".join(toks).strip()

        def _is_year(tok: str) -> bool:
            return bool(re.fullmatch(r"20\d{2}", tok))

        _TIME_SINGLETS = {
            "the month","month","this month","last month","next month",
            "the week","week","this week","last week","next week",
            "the year","year","this year","last year","next year",
            "quarter","this quarter","last quarter","next quarter",
            "today","yesterday","tomorrow"
        }
        _Q_RX   = re.compile(r"^(?:q[1-4]|quarter\s*[1-4])(?:\s+20\d{2})?$", re.I)
        _MMYYYY = re.compile(r"^(?:jan|feb|mar|apr|may|jun|jul|aug|sept?|oct|nov|dec)[a-z]*\s+20\d{2}$", re.I)
        _MM_OF_YYYY = re.compile(r"^(?:the\s+)?month\s+of\s+(?:jan|feb|mar|apr|may|jun|jul|aug|sept?|oct|nov|dec)[a-z]*\s+20\d{2}$", re.I)

        def _looks_like_time_phrase(s: str) -> bool:
            t = " ".join((s or "").split()).strip().lower()
            if not t:
                return False
            if t in _TIME_SINGLETS: return True
            if _Q_RX.match(t): return True
            if _MMYYYY.match(t): return True
            if _MM_OF_YYYY.match(t): return True
            if t in self.MONTH_WORDS or _is_year(t): return True
            if re.search(r"\b(month|week|year|quarter)s?\b$", t): return True
            return False

        def _clean_candidate(label: str, frag: str) -> Optional[str]:
            if not frag:
                print(f"[DEBUG] {label} → empty fragment")
                return None
            orig = frag
            frag = frag.strip(" .,-:/\\|()[]{}'\"")
            if not frag:
                print(f"[DEBUG] {label} → empty after strip; was:", repr(orig))
                return None

            JOINERS = {"to","vs","v","versus","and","or","in","on","for",
                    "during","between","from","by"}

            toks_out, cut_at = [], None
            for tok in frag.split():
                t = tok.lower().strip(" .,-:/\\|()[]{}'\"")
                if (t in JOINERS or t in self.MONTH_WORDS or 
                    t in self.COUNTRY_WORDS or _is_year(t)):
                    cut_at = t
                    break
                toks_out.append(tok)

            cand = " ".join(toks_out).strip(" .,-:/\\|()[]{}'\"")
            if cut_at:
                print(f"[DEBUG] {label} boundary stop at:", cut_at, "→ cand:", repr(cand))
            if not cand:
                print(f"[DEBUG] {label} → rejected (empty after boundary filtering)")
                return None
            if len(cand) > 80:
                print(f"[DEBUG] {label} → rejected (len>80):", len(cand), repr(cand))
                return None
            if self._looks_like_money(cand.lower()):
                print(f"[DEBUG] {label} → rejected (looks like money):", repr(cand))
                return None
            if _looks_like_time_phrase(cand):
                print(f"[DEBUG] {label} → rejected (looks like time phrase):", repr(cand))
                return None
            if cand.lower() in METRIC_TERMS:
                print(f"[DEBUG] {label} → rejected (looks like metric term):", repr(cand))
                return None

            cand = re.sub(r"\s{2,}", " ", cand)
            cand = re.sub(r"[-–—]\s*$", "", cand).strip()
            cand = _strip_metric_prefix(cand)
            cand = _clean_trailing_trend_words(cand)
            print(f"[DEBUG] {label} → OK:", repr(cand))
            return cand

        # ---------- explicit SKU ----------
        m = re.search(r"\bsku\s+([a-z0-9\-]+)", ql, re.I)
        if m:
            cand = _clean_trailing_trend_words(m.group(1).strip())
            cand = _strip_metric_prefix(cand)
            print("[DEBUG] MATCH branch=explicit_sku →", repr(cand))
            return cand

        # ---------- bare SKU-like token ----------
        m = re.search(r"\b([A-Za-z0-9]{2,}(?:-[A-Za-z0-9]{2,})+)\b", q, re.I)
        if m:
            cand = _clean_trailing_trend_words(m.group(1).strip())
            cand = _strip_metric_prefix(cand)
            print("[DEBUG] MATCH branch=bare_sku →", repr(cand))
            return cand

        # ---------- quoted names ----------
        for quoted in re.findall(r'"([^"]+)"', q):
            cand = _clean_candidate('quoted', quoted)
            if cand:
                print("[DEBUG] MATCH branch=quoted →", repr(cand))
                return cand

        # ---------- days_product ----------
        m = re.search(r"how many days\s+(.+?)\s+(?:completed|reached|achieved|made|did)\s+sales", ql)
        if m:
            cand = _clean_candidate('days_product', m.group(1).strip())
            if cand:
                print("[DEBUG] MATCH branch=days_product →", repr(cand))
                return cand

        # ---------- verb_sales ----------
        m = re.search(r"\b([a-z0-9][a-z0-9\s\-\&/]{0,80}?)\s+(?:completed|reached|achieved|made|did)\s+sales", ql)
        if m:
            lead = re.sub(r"^how many days\s+", "", m.group(1).strip())
            cand = _clean_candidate('verb_sales', lead)
            if cand:
                print("[DEBUG] MATCH branch=verb_sales →", repr(cand))
                return cand

        # ---------- in_phrase ----------
        m = re.search(r"(?:net\s+sales|sales|revenue|profit|fees?|charges?)\s+in\s+([a-z0-9][a-z0-9\-\&/ \t]{0,80})", ql)
        if not m:
            m = re.search(r"\bin\s+([a-z0-9][a-z0-9\-\&/ \t]{0,80})", ql)
        if m:
            raw = m.group(1)
            first_tok = re.split(r"\s+", raw.strip(), 1)[0].strip(" .,-:/\\|()[]{}'\"").lower()
            if first_tok in METRIC_TERMS:
                print("[DEBUG] in_phrase rejected: starts with metric term:", first_tok)
            else:
                cand = _clean_candidate('in_phrase', raw)
                if cand and (cand.lower() not in self.COUNTRY_WORDS) and (cand.lower() not in self.MONTH_WORDS) \
                        and (not _is_year(cand.lower())) and (not _looks_like_time_phrase(cand)):
                    print("[DEBUG] MATCH branch=in_phrase →", repr(cand))
                    return cand
                else:
                    print("[DEBUG] in_phrase candidate rejected:", repr(cand))

        # ---------- of_for patterns ----------
        patterns = [
            r"(?:[a-z]+(?:\s+[a-z]+){0,2})\s+(?:of|for)\s+([^\"\,]+?)(?:\s+in\s+|$)",
            r"\bfor\s+([^\"\,]+?)(?:\s+in\s+|$)",
            r"\bof\s+([^\"\,]+?)(?:\s+in\s+|$)",
        ]
        for idx, pat in enumerate(patterns, start=1):
            m = re.search(pat, ql)
            if m:
                frag = re.split(r"\s+(?:of|for|in)\s+", m.group(1).strip(" ."))[0]
                first_tok = re.split(r"\s+", frag.strip(), 1)[0].strip(" .,-:/\\|()[]{}'\"").lower()
                if first_tok in METRIC_TERMS:
                    print(f"[DEBUG] of_for_{idx} rejected: starts with metric term:", first_tok)
                    continue
                cand = _clean_candidate(f'of_for_{idx}', frag)
                if cand and not _looks_like_time_phrase(cand):
                    print(f"[DEBUG] MATCH branch=of_for_{idx} →", repr(cand))
                    return cand
                else:
                    print(f"[DEBUG] of_for_{idx} rejected (time/country/year/metric) cand=", repr(cand))

        print("[DEBUG] No product phrase matched.")
        return None







# ---------- SQL builder ----------
class QueryBuilder:
    @staticmethod
    def _is_per_country_table(table: str) -> bool:
        return table.endswith("_uk_merge_data_of_all_months") or table.endswith("_us_merge_data_of_all_months")

    @staticmethod
    def table_for(user_id: str, country: Optional[str]) -> str:
        if country and country.lower() in ["uk","us"]:
            return f"user_{user_id}_{country.lower()}_merge_data_of_all_months"
        return f"user_{user_id}_total_country_global_data"

    @staticmethod
    def month_where(months: List[int], names: List[str], params: Dict[str, Any]) -> str:
        parts: List[str] = []
        if months:
            arr = []
            for i, num in enumerate(months):
                arr.append(f"(month ~ '^[0-9]+$' AND CAST(month AS INT) = :m_int_{i})")
                params[f"m_int_{i}"] = int(num)
                arr.append(f"month = :m_str_{i}")
                params[f"m_str_{i}"] = str(num)
                arr.append(f"month = :m_str0_{i}")
                params[f"m_str0_{i}"] = f"{int(num):02d}"
            parts.append("(" + " OR ".join(arr) + ")")
        if names:
            arr = []
            for i, name in enumerate(names):
                n = name.lower()
                arr.append("LOWER(month) = :mn_{}".format(i))
                params[f"mn_{i}"] = n
                arr.append("LOWER(month) LIKE :mna_{}".format(i))
                params[f"mna_{i}"] = n[:3] + "%"
            parts.append("(" + " OR ".join(arr) + ")")
        return "(" + " OR ".join(parts) + ")" if parts else ""

    @staticmethod
    def year_where(years: List[int], params: Dict[str, Any]) -> str:
        if not years:
            return ""
        in_keys = []
        for i, y in enumerate(years):
            params[f"y_{i}"] = int(y)
            in_keys.append(f":y_{i}")
        numeric = f"(year ~ '^[0-9]+$' AND CAST(year AS INT) IN ({', '.join(in_keys)}))"
        txt_keys = []
        for i, y in enumerate(years):
            params[f"ys_{i}"] = str(y)
            txt_keys.append(f":ys_{i}")
        text_in = f"year IN ({', '.join(txt_keys)})"
        return f"({numeric} OR {text_in})"

    
    def build(
            self,
            *,
            user_id: str,
            country: Optional[str],
            selected_cols: List[str],    # kept in signature for compatibility, but ignored
            time_entities: Dict[str, Any],
            filters: Dict[str, Any],
            intents: Dict[str, Any],
            limit: int = 100000,
        ) -> Tuple[str, Dict[str, Any], str]:

        table = self.table_for(user_id, country)
        params: Dict[str, Any] = {}

        is_per_country = self._is_per_country_table(table)

        # Safe baseline columns (present in your CSV)
        baseline = {
            "date_time","product_sales","total","product_name",
            "month","year","type","sku","description",
            "promotional_rebates","other","postage_credits","gift_wrap_credits","shipping_credits",
            "product_sales_tax","marketplace_facilitator_tax","shipping_credits_tax",
            "giftwrap_credits_tax","promotional_rebates_tax","other_transaction_fees",
            "platform_fees","advertising_cost",
            "fba_fees","selling_fees","cost_of_unit_sold","quantity"
        }
        if not is_per_country:
            baseline = set(baseline) | {"country"}

        select_clause = ", ".join(sorted(baseline))
        where: List[str] = []

        # Country filter on global table
        if (not is_per_country) and country:
            key = f"eq_country_{len(params)}"
            params[key] = str(country)
            where.append(f"LOWER(country) = LOWER(:{key})")
            print(f"[DEBUG] added country filter for global table: {country}")

        # ---------- TIME LOGIC ----------
        import datetime as dt

        def _month_clause(month_dicts: List[dict], params: Dict[str, Any]) -> str:
            if not month_dicts:
                return ""
            parts = []
            for i, m in enumerate(month_dicts):
                num = int(m["number"])
                name = str(m["name"])
                p_full = f"mn_full_{i}"
                p_abbr = f"mn_abbr_{i}"
                p_int  = f"mn_int_{i}"
                p_02   = f"mn_02_{i}"
                params[p_full] = name.lower()
                params[p_abbr] = name[:3].lower() + "%"
                params[p_int]  = str(num)
                params[p_02]   = f"{num:02d}"
                parts.append(
                    "("
                    "  LOWER(month) = :" + p_full +
                    "  OR LOWER(month) LIKE :" + p_abbr +
                    "  OR month = :" + p_int +
                    "  OR month = :" + p_02 +
                    ")"
                )
            return "(" + " OR ".join(parts) + ")"

        def _year_clause(years: List[int], params: Dict[str, Any]) -> str:
            if not years:
                return ""
            num_keys, str_keys = [], []
            for i, y in enumerate(years):
                k_num = f"y_{i}"
                k_str = f"ys_{i}"
                params[k_num] = int(y)
                params[k_str] = str(y)
                num_keys.append(f":{k_num}")
                str_keys.append(f":{k_str}")
            numeric = f"(year ~ '^[0-9]+$' AND CAST(year AS INT) IN ({', '.join(num_keys)}))"
            text_in = f"(year IN ({', '.join(str_keys)}))"
            return f"({numeric} OR {text_in})"

        # A. Single explicit day
        if "explicit_day" in time_entities:
            y, mo, d = time_entities["explicit_day"]
            start_dt = dt.datetime(y, mo, d, 0, 0, 0, tzinfo=dt.timezone.utc)
            end_dt   = start_dt + dt.timedelta(days=1)

            params["dt_start"] = start_dt
            params["dt_end"]   = end_dt

            if "us_merge" in table.lower():
                dt_expr = (
                    "COALESCE("
                    "  to_timestamp(date_time, 'Mon DD YYYY HH12:MI:SS AM TZ'),"
                    "  to_timestamp(date_time || ' UTC', 'Mon DD YYYY HH12:MI:SS AM TZ')"
                    ")"
                )
            else:
                # UPDATED: dual-format support
                dt_expr = (
                    "CASE "
                    "  WHEN date_time ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}T' "
                    "    THEN to_timestamp(date_time, 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') "
                    "  WHEN date_time ~ '^[0-9]{1,2} [A-Za-z]{3} [0-9]{4} ' "
                    "    THEN to_timestamp(date_time, 'FMDD Mon YYYY HH24:MI:SS TZ') "
                    "  ELSE NULL "
                    "END"
                )

            where.append(f"({dt_expr} >= :dt_start AND {dt_expr} < :dt_end)")
            print(f"[DEBUG] added explicit_day filter: {y}-{mo}-{d}")

        # B. Month(s)
        elif time_entities.get("months"):
            m_clause = _month_clause(time_entities["months"], params)
            if m_clause:
                where.append(m_clause)
                print(f"[DEBUG] added month filter(s): {time_entities['months']}")
            y_clause = _year_clause(time_entities.get("years", []), params)
            if y_clause:
                where.append(y_clause)
                print(f"[DEBUG] added year filter(s): {time_entities['years']}")

        # C. Explicit date_range
        elif "date_range" in time_entities:

            start_dt, end_dt = time_entities["date_range"]

            # Normalize start
            try:
                if isinstance(start_dt, dt.date) and not isinstance(start_dt, dt.datetime):
                    start_dt = dt.datetime.combine(start_dt, dt.time.min, tzinfo=dt.timezone.utc)
                elif isinstance(start_dt, dt.datetime) and start_dt.tzinfo is None:
                    start_dt = start_dt.replace(tzinfo=dt.timezone.utc)
            except Exception:
                pass

            # Normalize end
            try:
                if isinstance(end_dt, dt.date) and not isinstance(end_dt, dt.datetime):
                    end_dt = dt.datetime.combine(end_dt, dt.time.min, tzinfo=dt.timezone.utc) + dt.timedelta(days=1)
                elif isinstance(end_dt, dt.datetime) and end_dt.tzinfo is None:
                    end_dt = end_dt.replace(tzinfo=dt.timezone.utc)
            except Exception:
                pass

            params["dt_start"] = start_dt
            params["dt_end"]   = end_dt

            if "us_merge" in table.lower():
                dt_expr = (
                    "COALESCE("
                    "  to_timestamp(date_time, 'Mon DD YYYY HH12:MI:SS AM TZ'),"
                    "  to_timestamp(date_time || ' UTC', 'Mon DD YYYY HH12:MI:SS AM TZ')"
                    ")"
                )
            else:
                # UPDATED: dual-format support
                dt_expr = (
                    "CASE "
                    "  WHEN date_time ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}T' "
                    "    THEN to_timestamp(date_time, 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') "
                    "  WHEN date_time ~ '^[0-9]{1,2} [A-Za-z]{3} [0-9]{4} ' "
                    "    THEN to_timestamp(date_time, 'FMDD Mon YYYY HH24:MI:SS TZ') "
                    "  ELSE NULL "
                    "END"
                )

            where.append(f"({dt_expr} >= :dt_start AND {dt_expr} < :dt_end)")
            print(f"[DEBUG] added date_range filter: {start_dt} → {end_dt}")

        # D. Year(s) only
        elif time_entities.get("years"):
            y_clause = _year_clause(time_entities.get("years", []), params)
            if y_clause:
                where.append(y_clause)
                print(f"[DEBUG] added year filter(s): {time_entities['years']}")

        # ---------- END TIME LOGIC ----------

        TEXT_COLS = {"product_name","sku","type","description","country","order_id","month","year"}

        # ---------- TEXT_LIKE filters ----------
        for col, txt in filters.get("text_like", []):
            if col == "product":
                print(f"[DEBUG] remapping filter column 'product' → 'product_name'")
                col = "product_name"
            key = f"tl_{col}_{len(params)}"
            params[key] = f"%{str(txt)}%"
            where.append(f"LOWER({col}) LIKE LOWER(:{key})")
            print(f"[DEBUG] added TEXT_LIKE filter: {col} LIKE %{txt}%")

        # ---------- EQUALS filters ----------
        for col, val in filters.get("equals", []):
            if col == "product":
                col = "product_name"
                print(f"[DEBUG] remapping filter column 'product' → 'product_name'")

            if is_per_country and col == "country":
                print(f"[DEBUG] skipping equals filter on country (already handled): {val}")
                continue

            key = f"eq_{col}_{len(params)}"
            if col in TEXT_COLS:
                params[key] = str(val)
                where.append(f"LOWER({col}) = LOWER(:{key})")
                print(f"[DEBUG] added TEXT equals filter: {col} = {val}")
            else:
                params[key] = val
                where.append(f"{col} = :{key}")
                print(f"[DEBUG] added NUMERIC equals filter: {col} = {val}")

        # ---------- IN_LIST filters ----------
        for col, vals in filters.get("in_list", []):
            if col == "product":
                col = "product_name"
                print(f"[DEBUG] remapping filter column 'product' → 'product_name'")
            if not vals or (is_per_country and col == "country"):
                continue
            keys = []
            for i, v in enumerate(vals):
                k = f"in_{col}_{len(params)}_{i}"
                params[k] = v
                keys.append(f":{k}")
            if col in TEXT_COLS:
                where.append(f"LOWER({col}) IN ({', '.join(keys)})")
                print(f"[DEBUG] added TEXT IN_LIST filter: {col} IN {vals}")
            else:
                where.append(f"{col} IN ({', '.join(keys)})")
                print(f"[DEBUG] added NUMERIC IN_LIST filter: {col} IN {vals}")

        # ---------- NUMERIC filters ----------
        for col, op, val in filters.get("numeric", []):
            if col == "product":
                col = "product_name"
                print(f"[DEBUG] remapping filter column 'product' → 'product_name'")
            key = f"num_{col}_{len(params)}"
            params[key] = float(val)
            where.append(f"CAST({col} AS DOUBLE PRECISION) {op} :{key}")
            print(f"[DEBUG] added NUMERIC filter: {col} {op} {val}")

        # ---------- BETWEEN filters ----------
        for col, lo, hi in filters.get("between", []):
            if col == "product":
                col = "product_name"
                print(f"[DEBUG] remapping filter column 'product' → 'product_name'")
            key_lo = f"btw_lo_{col}_{len(params)}"
            key_hi = f"btw_hi_{col}_{len(params)+1}"
            params[key_lo] = float(lo)
            params[key_hi] = float(hi)
            where.append(f"CAST({col} AS DOUBLE PRECISION) BETWEEN :{key_lo} AND :{key_hi}")
            print(f"[DEBUG] added BETWEEN filter: {col} BETWEEN {lo} AND {hi}")

        where_clause = f"WHERE {' AND '.join(where)}" if where else ""

        # ---------- ORDER / SORT ----------
        order_clause = ""
        raw_lower = " ".join(intents.keys())
        if "sorting" in intents or any(w in raw_lower for w in ["recent", "latest", "new"]):
            order_clause = "ORDER BY date_time DESC"
            print(f"[DEBUG] added ORDER BY date_time DESC (recent/latest)")
        elif "product_sales" in baseline:
            order_clause = "ORDER BY product_sales DESC"
            print(f"[DEBUG] added ORDER BY product_sales DESC (default)")

        limit = max(1, min(int(limit), 100000))

        sql = f"""
            SELECT {select_clause}
            FROM {table}
            {where_clause}
            {order_clause}
            LIMIT {limit}
        """.strip()

        print("[DEBUG] Final SQL query:\n", sql)
        print("[DEBUG] Params:", params)

        return sql, params, table



def resolve_family_skus(user_id: int, country: Optional[str], family_token: str, limit: int = 500) -> list[str]:
    """
    Return SKUs whose product_name contains the token as a whole word (word-boundary), e.g. 'classic'.
    Avoids substring pollution like 'ClassicPro' unless you intend it.
    """
    if not engine or not family_token or not str(family_token).strip():
        print("[DEBUG] resolve_family_skus → skipped (empty engine or token)")
        return []

    token = str(family_token).strip()
    table = QueryBuilder.table_for(str(user_id), country)

    # Escape regex special chars so the token is treated literally
    token_escaped = re.sub(r'([.^$*+?(){}\[\]|\\])', r'\\\1', token)

    sql = text(f"""
        SELECT DISTINCT sku
        FROM {table}
        WHERE sku IS NOT NULL AND sku <> ''
          AND product_name ~* ('\\m' || :tok || '\\M')
        LIMIT :lim
    """)

    try:
        with engine.connect() as conn:
            rows = conn.execute(sql, {"tok": token_escaped, "lim": int(limit)}).fetchall()
        skus = [(r[0] or "").strip() for r in rows if (r[0] or "").strip()]
        print(f"[DEBUG] resolve_family_skus → token={token}, matched={len(skus)}")
        return skus
    except Exception:
        logging.exception("resolve_family_skus failed")
        return []







# ---------- analysis ----------
class DataAnalyzer:
    @staticmethod
    def _to_month_name(m) -> str:
        s = str(m).strip()
        try:
            n = int(s.lstrip("0") or "0")
            if 1 <= n <= 12:
                return calendar.month_name[n]
        except Exception:
            pass
        return s.capitalize()

    @staticmethod
    def _to_int_year(y) -> Optional[int]:
        try:
            return int(str(y).strip())
        except Exception:
            return None

    def analyze_results(self, df: pd.DataFrame, query: str) -> Dict[str, Any]:
        try:
            if df.empty:
                return {"summary": "No data found for your query.", "insights": []}

            insights: List[str] = []
            summary = f"Found {len(df)} records"
            q_lower = query.lower()

            if ("fee" in q_lower or "fees" in q_lower) and "total" in df.columns:
                total_col = pd.to_numeric(df["total"], errors="coerce")
                fee_sum = total_col.sum(skipna=True)
                insights.append(f"Total fees (sum of 'total'): £{fee_sum:,.2f}")

            if "product_sales" in df.columns:
                sales = pd.to_numeric(df["product_sales"], errors="coerce")
                total_sales = sales.sum(skipna=True)
                avg_sales = sales.mean(skipna=True)
                insights.append(f"Total sales: £{total_sales:,.2f}")
                if not np.isnan(avg_sales):
                    insights.append(f"Average sales per transaction: £{avg_sales:.2f}")
                if len(sales) > 1:
                    mx, mn = sales.max(skipna=True), sales.min(skipna=True)
                    if not np.isnan(mx):
                        insights.append(f"Highest single sale: £{mx:.2f}")
                    if not np.isnan(mn):
                        insights.append(f"Lowest single sale: £{mn:.2f}")

            if "quantity" in df.columns:
                qty = pd.to_numeric(df["quantity"], errors="coerce").fillna(0)
                insights.append(f"Total quantity sold: {int(qty.sum()):,}")

            if "product_name" in df.columns:
                uniq = df["product_name"].dropna().astype(str).str.strip().unique()
                if len(uniq) > 1:
                    top_products = df["product_name"].astype(str).value_counts().head(3)
                    if not top_products.empty:
                        insights.append("Top products:")
                        for p, c in top_products.items():
                            insights.append(f"  • {p}: {c} orders")

            has_month, has_year = "month" in df.columns, "year" in df.columns
            if has_month or has_year:
                tmp = df.copy()
                if has_month:
                    tmp["__month_display__"] = tmp["month"].apply(self._to_month_name)
                if has_year:
                    tmp["__year_int__"] = tmp["year"].apply(self._to_int_year)

                group_cols = ([] if not has_year else ["__year_int__"]) + ([] if not has_month else ["__month_display__"])
                if group_cols:
                    if "product_sales" in tmp.columns:
                        tmp["__sales__"] = pd.to_numeric(tmp["product_sales"], errors="coerce")
                        tg = tmp.groupby(group_cols, dropna=True)["__sales__"].sum().round(2).reset_index()
                        insights.append("Time-based breakdown:")
                        for _, r in tg.head(5).iterrows():
                            if has_year and has_month:
                                insights.append(f"  • {r['__month_display__']} {int(r['__year_int__'])}: £{r['__sales__']:,.2f}")
                            elif has_year:
                                insights.append(f"  • {int(r['__year_int__'])}: £{r['__sales__']:,.2f}")
                            else:
                                insights.append(f"  • {r['__month_display__']}: £{r['__sales__']:,.2f}")
                    else:
                        tg = tmp.groupby(group_cols, dropna=True).size().reset_index(name="count")
                        insights.append("Time-based breakdown:")
                        for _, r in tg.head(5).iterrows():
                            if has_year and has_month:
                                insights.append(f"  • {r['__month_display__']} {int(r['__year_int__'])}: {int(r['count'])} records")
                            elif has_year:
                                insights.append(f"  • {int(r['__year_int__'])}: {int(r['count'])} records")
                            else:
                                insights.append(f"  • {r['__month_display__']}: {int(r['count'])} records")

            return {"summary": summary, "insights": insights, "record_count": len(df), "columns_analyzed": list(df.columns)}
        except Exception:
            logger.exception("analyze_results error")
            return {"summary": "Error analyzing data", "insights": []}

    def natural_response(self, analysis: Dict[str, Any], query: str) -> str:
        try:
            parts = [analysis["summary"]]
            if analysis.get("insights"):
                parts.append("\nKey insights:")
                parts.extend([f"• {i}" for i in analysis["insights"]])
            ql = query.lower()
            if any(w in ql for w in ["trend","growth","change"]):
                parts.append("\n📈 For trend analysis, consider comparing data across multiple time periods.")
            if any(w in ql for w in ["profit","margin"]):
                parts.append("\n💰 Remember to factor in all costs including FBA fees and selling fees for accurate profit calculations.")
            resp = "\n".join(parts)
            try:
                resp = (resp
                        .replace("A�","£")
                        .replace("�?�","- ")
                        .replace("dY\"^","Tip: ")
                        .replace("dY'�","Tip: "))
            except Exception:
                pass
            return resp
        except Exception:
            logger.exception("natural_response error")
            return "I found some data but had trouble analyzing it. Please try rephrasing your question."

# ---------- special calculations ----------
class SpecialCalculations:
    @staticmethod
    def _extract_target_amount(query: str) -> Optional[float]:
        ql = query.lower()
        m = re.search(r"(?:£\s*|)\b([0-9]+(?:\.[0-9]+)?)\b\s*(?:pounds|gbp)?", ql)
        if not m:
            return None
        try:
            return float(m.group(1))
        except Exception:
            return None

    @staticmethod
    def try_days_to_reach_target(query: str, df: pd.DataFrame, product_label: Optional[str]) -> Optional[str]:
        """
        Interpret: 'in how many days <product> completed/reached/achieved/made sales of £X ...'
        Compute the number of distinct dates (ascending) needed for the cumulative
        sum of product_sales to reach the target. If never reaches, say so.
        """
        ql = query.lower()
        if "how many days" not in ql or "sales" not in ql:
            return None

        # Key verbs that imply *cumulative* completion of a target
        if not any(w in ql for w in ["completed", "reached", "achieved", "made", "did"]):
            return None

        target = SpecialCalculations._extract_target_amount(query)
        if target is None:
            return None

        if "date_time" not in df.columns or "product_sales" not in df.columns:
            return None

        tmp = df.copy()
        # Keep only the requested product if provided
        if product_label and "product_name" in tmp.columns:
            mask = tmp["product_name"].astype(str).str.contains(product_label, case=False, na=False)
            tmp = tmp.loc[mask]

        if tmp.empty:
            return (f"No data found for product '{product_label}' in the selected period."
                    if product_label else "No data found for your query.")

        tmp["__date__"] = pd.to_datetime(tmp["date_time"], errors="coerce").dt.date
        tmp["__sales__"] = pd.to_numeric(tmp["product_sales"], errors="coerce").fillna(0.0)

        # Aggregate to daily totals, then cumulative in ascending date order
        daily = (tmp.groupby("__date__", dropna=True)["__sales__"]
                    .sum()
                    .reset_index()
                    .sort_values("__date__", ascending=True))
        daily["__cum__"] = daily["__sales__"].cumsum()

        # First date index where cumulative >= target
        idx = daily.index[daily["__cum__"] >= target]
        if len(idx) == 0:
            days_present = int(daily["__date__"].nunique())
            total = float(daily["__cum__"].iloc[-1]) if len(daily) else 0.0
            prod_txt = f"{product_label} " if product_label else ""
            # Month/Year label (best-effort from df)
            month_lbl, year_lbl = None, None
            if "month" in df.columns:
                try:
                    month_lbl = calendar.month_name[int(str(df["month"].iloc[0]).lstrip('0') or '0')]
                except Exception:
                    pass
            if "year" in df.columns:
                try:
                    year_lbl = str(int(df["year"].iloc[0]))
                except Exception:
                    pass
            when = f" in {month_lbl} {year_lbl}" if month_lbl and year_lbl else ""
            return (f"{prod_txt}did not reach a cumulative £{target:,.0f}{when}. "
                    f"Cumulative total was £{total:,.2f} across {days_present} day(s).")

        days_needed = int(daily.loc[idx[0]:idx[0], "__date__"].count())
        total_days = int(daily["__date__"].nunique())
        reached_on = daily.loc[idx[0], "__date__"]

        prod_txt = f"{product_label} " if product_label else ""
        # Month/Year label (best-effort from df)
        month_lbl, year_lbl = None, None
        if "month" in df.columns:
            try:
                month_lbl = calendar.month_name[int(str(df["month"].iloc[0]).lstrip('0') or '0')]
            except Exception:
                pass
        if "year" in df.columns:
            try:
                year_lbl = str(int(df["year"].iloc[0]))
            except Exception:
                pass
        when = f" in {month_lbl} {year_lbl}" if month_lbl and year_lbl else ""

        return (f"{prod_txt}completed £{target:,.0f} in cumulative sales in day(s){when}, "
                f"by {reached_on:%d %b %Y}. (Across {total_days} trading day(s) in the filter.)")

    @staticmethod
    def try_days_reaching_sales_daily_threshold(query: str, df: pd.DataFrame, product_label: Optional[str]) -> Optional[str]:
        """
        Legacy interpretation: 'on how many days were daily sales ≥ £X'
        Kept as a fallback when the user intent clearly asks about 'per day' thresholds.
        """
        ql = query.lower()
        if "how many days" not in ql or "sales" not in ql:
            return None
        # Only trigger this version if the user hints daily threshold
        if not any(w in ql for w in ["per day", "in a day", "on a day", "daily"]):
            return None

        target = SpecialCalculations._extract_target_amount(query)
        if target is None:
            return None
        if "date_time" not in df.columns or "product_sales" not in df.columns:
            return None

        tmp = df.copy()
        tmp["__date__"] = pd.to_datetime(tmp["date_time"], errors="coerce").dt.date
        tmp["__sales__"] = pd.to_numeric(tmp["product_sales"], errors="coerce").fillna(0.0)
        if product_label and "product_name" in tmp.columns:
            mask = tmp["product_name"].astype(str).str.contains(product_label, case=False, na=False)
            tmp = tmp.loc[mask]
        if tmp.empty:
            return (f"No data found for product '{product_label}' in the selected period."
                    if product_label else "No data found for your query.")

        daily = tmp.groupby("__date__", dropna=True)["__sales__"].sum().reset_index()
        days_count = int((daily["__sales__"] >= target).sum())
        total_days = int(len(daily))

        prod_txt = f"{product_label} " if product_label else ""
        month_lbl, year_lbl = None, None
        if "month" in df.columns:
            try:
                month_lbl = calendar.month_name[int(str(df["month"].iloc[0]).lstrip('0') or "0")]
            except Exception:
                pass
        if "year" in df.columns:
            try:
                year_lbl = str(int(df["year"].iloc[0]))
            except Exception:
                pass
        when = f" in {month_lbl} {year_lbl}" if month_lbl and year_lbl else ""
        return (f"{prod_txt}reached at least £{target:,.0f} in **daily** sales on **{days_count}** day(s){when}. "
                f"(Out of {total_days} trading day(s) in the filter.)")
    
############################################################################################################################################################################
def resolve_product_by_sku(engine, user_id: int, country: Optional[str], sku: str) -> Optional[str]:
    """Return product_name for an exact SKU (case-insensitive), or None."""
    if not engine or not sku:
        return None
    table = QueryBuilder.table_for(str(user_id), country)
    sql = text(f"""
        SELECT product_name
        FROM {table}
        WHERE LOWER(TRIM(sku)) = LOWER(TRIM(:sku))
          AND product_name IS NOT NULL AND product_name <> ''
        LIMIT 1
    """)
    try:
        with engine.connect() as conn:
            row = conn.execute(sql, {"sku": sku.strip()}).fetchone()
        return (row[0] or "").strip() if row else None
    except Exception:
        logger.exception("resolve_product_by_sku failed")
        return None


# def _explicit_exclusivity_phrase(s: str) -> bool:
#     return bool(re.search(r"\b(only|just|exactly)\b", (s or "").lower()))

def apply_reply_to_pending(user_id: int, reply_text: str, engine) -> Optional[dict]:
    """
    Returns:
      None                 → nothing pending
      {"need_more": True,  "missing": [...], "plan": plan}
      {"need_more": False, "plan": plan}

    Fixes:
    • Stricter SKU regex (must look like ABC-123 style).
    • DB validation of SKUs before accepting.
    • If user repeats the product name from candidates (case-insensitive), accept as product.
    • Clears stale SKU filters when product-only is chosen.
    • In sku_choice, if user repeats the product name → force product filter (no SKU clarification loop).
    """

    st = PENDING.get(user_id)
    if not st:
        return None

    plan = st["plan"]
    missing = st["missing"]

    # --- helpers ---
    def _wants_all_variants(s: str) -> bool:
        q = (s or "").lower()
        return any(k in q for k in [
            "all variants", "all sizes", "entire range", "full range",
            "family total", "overall", "all skus", "all sku",
            "sum all", "combined", "rollup", "roll-up", "roll up",
            "variants total", "whole range"
        ])

    def _ensure_filters_list(p: dict):
        if "filters" not in p or not isinstance(p["filters"], list):
            p["filters"] = []

    def _soft_clean(s: str) -> str:
        t = (s or "").strip().strip('"').strip("'")
        t = re.sub(r"\b(only|just|please|pls|thanks|thank you)\b", "", t, flags=re.I)
        return re.sub(r"\s{2,}", " ", t).strip()

    still_missing: List[str] = []
    reply_raw = (reply_text or "").strip()

    # SKU pattern (strict)
    sku_pattern = re.compile(r"^[A-Za-z0-9]{2,}(?:-[A-Za-z0-9]{2,})+$")

    for slot in missing:
        # --- product slot ---
        if slot == "product":
            user_choice = reply_raw
            if not user_choice:
                still_missing.append("product")
                continue

            cleaned_choice = _soft_clean(user_choice)

            # "all products" → switch to per-product breakdown
            if _is_all_products_phrase(cleaned_choice):
                _ensure_filters_list(plan)
                # drop any SKU filters
                plan["filters"] = [f for f in plan.get("filters", [])
                                   if str(f.get("field", "")).lower() != "sku"]
                plan["product"] = None
                plan["group_by"] = "product"
                plan["needs_clarification"] = False
                continue

            eff_country = plan.get("country") or st.get("country_override")
            _ensure_filters_list(plan)

            # Case 1: looks like SKU → validate
            if sku_pattern.match(user_choice):
                pn = resolve_product_by_sku(engine, int(user_id), eff_country, user_choice)
                if pn:
                    plan["filters"] = [f for f in plan.get("filters", [])
                                       if str(f.get("field", "")).lower() != "sku"]
                    plan["filters"].append({"field": "sku", "op": "=", "value": user_choice})
                    plan["product"] = pn  # friendly label for UI
                    print(f"[DEBUG][pending] product slot: accepted exact SKU {user_choice} → product={pn}")
                    continue
                else:
                    logging.warning(f"SKU lookup failed for '{user_choice}' (user_id={user_id}, country={eff_country})")
                    plan["needs_clarification"] = True
                    still_missing.append("product")
                    continue

            # Case 2: exact/partial candidate match
            cands = st.get("candidates") or []
            match = None
            for c in cands:
                if _soft_clean(str(c)).lower() == cleaned_choice.lower():
                    match = c
                    break
            if not match:
                for c in cands:
                    if cleaned_choice.lower() in _soft_clean(str(c)).lower():
                        match = c
                        break

            # Case 3: fallback DB search
            if not match and engine is not None:
                try:
                    rows = product_candidates(engine, int(user_id), eff_country, cleaned_choice, limit=10)
                except Exception:
                    rows = []
                if rows:
                    match = rows[0].get("product_name")

            if match:
                plan["product"] = match
                # clear stale SKU filters when switching to product-level ask
                plan["filters"] = [f for f in plan.get("filters", [])
                                   if str(f.get("field", "")).lower() != "sku"]
                # only force exact equality when user says only/just/exactly
                # plan["force_product_only"] = _explicit_exclusivity_phrase(user_choice)
                plan["force_product_only"] =(user_choice)
                print(f"[DEBUG][pending] product slot: matched product '{match}', force_product_only={plan.get('force_product_only')}")
            else:
                still_missing.append("product")

        # --- sku_choice slot ---
        elif slot == "sku_choice":
            text_norm = _soft_clean(reply_raw).lower()
            fam_label = (plan.get("product") or st.get("product_phrase") or "").strip()
            fam_norm = _soft_clean(fam_label).lower() if fam_label else ""
            eff_country = plan.get("country") or st.get("country_override")
            _ensure_filters_list(plan)

            # ✅ If user repeats the family/product name → LOCK to product only (no more SKU disambiguation)
            if fam_norm and text_norm == fam_norm:
                # drop prior SKU filters
                plan["filters"] = [f for f in plan.get("filters", [])
                                   if str(f.get("field", "")).lower() != "sku"]
                # add exact product filter to avoid summing family/combos
                plan["filters"].append({"field": "product_name", "op": "=", "value": fam_label})
                plan["force_product_only"] =(reply_raw) or True
                # plan["force_product_only"] = _explicit_exclusivity_phrase(reply_raw) or True
                print(f"[DEBUG][pending] sku_choice: user repeated product '{fam_label}' → force product-only, added product_name='=' filter")
                continue

            # A) "all variants" → resolve family SKUs and use IN
            if _wants_all_variants(reply_raw):
                try:
                    skus = resolve_family_skus(int(user_id), eff_country, fam_label, limit=500)
                except Exception:
                    skus = []
                if skus:
                    plan["filters"].append({"field": "sku", "op": "in", "value": skus})
                    print(f"[DEBUG][pending] sku_choice: all variants → {len(skus)} SKUs")
                else:
                    still_missing.append("sku_choice")
                continue

            # B) direct SKU typed
            if sku_pattern.match(reply_raw):
                pn = resolve_product_by_sku(engine, int(user_id), eff_country, reply_raw)
                if pn:
                    plan["filters"].append({"field": "sku", "op": "=", "value": reply_raw})
                    plan["product"] = pn
                    print(f"[DEBUG][pending] sku_choice: exact SKU chosen {reply_raw} (product={pn})")
                else:
                    still_missing.append("sku_choice")
                continue

            # C) product candidate typed instead of SKU → try resolve SKUs
            cand_products = [str(c) for c in (st.get("candidates") or [])
                             if not sku_pattern.match(str(c))]
            product_match = next((p for p in cand_products if _soft_clean(p).lower() == text_norm), None)

            if product_match:
                try:
                    rows = product_candidates(engine, int(user_id), eff_country, product_match, limit=100) or []
                except Exception:
                    rows = []
                res = [r.get("sku") for r in rows if r.get("sku")]
                uniq = sorted({s for s in res if s})
                if len(uniq) == 1:
                    plan["filters"].append({"field": "sku", "op": "=", "value": uniq[0]})
                    plan["product"] = product_match
                    print(f"[DEBUG][pending] sku_choice: single SKU for '{product_match}' → {uniq[0]}")
                elif len(uniq) > 1:
                    plan["filters"].append({"field": "sku", "op": "in", "value": uniq})
                    plan["product"] = product_match
                    print(f"[DEBUG][pending] sku_choice: multiple SKUs for '{product_match}' → {len(uniq)}")
                else:
                    # Fallback: force product-only if we couldn't resolve SKUs
                    plan["filters"].append({"field": "product_name", "op": "=", "value": product_match})
                    plan["force_product_only"] = True
                    plan["product"] = product_match
                    print(f"[DEBUG][pending] sku_choice: fallback → product_name '=' '{product_match}'")
            else:
                still_missing.append("sku_choice")

        # (keep any other slots unchanged if you have them in your environment)

    if still_missing:
        extras = {k: v for k, v in st.items() if k not in ("plan", "missing")}
        PENDING.set(user_id, plan, still_missing, **extras)
        return {"need_more": True, "missing": still_missing, "plan": plan}

    PENDING.clear(user_id)
    return {"need_more": False, "plan": plan}





def exec_plan_via_formula(self, *, plan: dict, query: str, user_id: str, country_override: Optional[str] = None):
    # ---------- small normalizations (local to this function) ----------
    ql = (query or "").lower()

    # If the planner accidentally set product="sku"/"product", ignore it.
    _generic_tokens = {"sku", "product", "products", "product name"}
    if isinstance(plan.get("product"), str) and plan["product"].strip().lower() in _generic_tokens:
        plan["product"] = None

    # ---- helpers ------------------------------------------------------------
    def _coerce_time_range(tr, user_id: Optional[int] = None, country: Optional[str] = None) -> Optional[dict]:
        """
        Normalize a time_range-like input into {"start": "YYYY-MM-DD", "end": "YYYY-MM-DD"}.

        Accepts:
        - dicts with 'start'/'end'
        - strings like '2024-03', 'Mar 2024', 'Q2 2023'
        - natural spans: 'last 3 months', 'this quarter', 'till now', etc.
        Automatically clamps dynamic spans via clamp_relative_time_to_available().
        """
        # ---------- direct dict ----------
        if isinstance(tr, dict) and tr.get("start") and tr.get("end"):
            return {"start": tr["start"][:10], "end": tr["end"][:10]}

        if not isinstance(tr, str):
            return None

        s = tr.strip().lower()
        if not s:
            return None

        def _fmt(d: date) -> str:
            return d.strftime("%Y-%m-%d")

        def _last_day_of_month(y, m):
            return calendar.monthrange(y, m)[1]

        def _quarter_bounds(y, q):
            start_m, end_m = {1: (1, 3), 2: (4, 6), 3: (7, 9), 4: (10, 12)}[q]
            end_d = _last_day_of_month(y, end_m)
            return date(y, start_m, 1), date(y, end_m, end_d)

        # ---------- step 1: reject "future" phrases ----------
        if re.search(r"\b(future|upcoming|next\s+(month|quarter|year))\b", s):
            return None

        # ---------- step 2: relative / natural-language phrases ----------
        if re.search(
            r"\b(last|this|current|previous|past|ytd|year\s*to\s*date|till\s*now|to\s*date|so\s*far|overall|all\s*time|quarter|month|week|year)\b",
            s,
        ):
            try:
                widened = clamp_relative_time_to_available(int(user_id) if user_id else 0, country, s)
                if widened and widened.get("start") and widened.get("end"):
                    return {"start": widened["start"][:10], "end": widened["end"][:10]}
            except Exception as e:
                print(f"[DEBUG][coerce_time_range] clamp failed for '{s}': {e}")

        # ---------- step 3: plain YYYY-MM (month) ----------
        m = re.fullmatch(r"(\d{4})-(0?[1-9]|1[0-2])", s)
        if m:
            y, mon = int(m.group(1)), int(m.group(2))
            last = calendar.monthrange(y, mon)[1]
            return {"start": f"{y:04d}-{mon:02d}-01", "end": f"{y:04d}-{mon:02d}-{last:02d}"}

        # ---------- step 4: month name + year ----------
        m = re.fullmatch(
            r"(jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)[a-z]*\s+(\d{4})", s, flags=re.I
        )
        if m:
            name, y = m.group(1).lower(), int(m.group(2))
            mon = [None, "jan","feb","mar","apr","may","jun","jul","aug","sep","sept","oct","nov","dec"].index(name[:3])
            last = calendar.monthrange(y, mon)[1]
            return {"start": f"{y:04d}-{mon:02d}-01", "end": f"{y:04d}-{mon:02d}-{last:02d}"}

        # ---------- step 5: quarter pattern ----------
        m = re.fullmatch(r"(q|qtr|quarter|qtd)\s*([1-4])(?:\s+(\d{4}))?", s, flags=re.I)
        if m:
            qnum = int(m.group(2))
            year = int(m.group(3)) if m.group(3) else date.today().year
            start, end = _quarter_bounds(year, qnum)
            return {"start": _fmt(start), "end": _fmt(end)}

        # ---------- step 6: year-only ----------
        m = re.fullmatch(r"(\d{4})", s)
        if m:
            y = int(m.group(1))
            return {"start": f"{y:04d}-01-01", "end": f"{y:04d}-12-31"}

        # ---------- fallback ----------
        return None

    # ---- main ---------------------------------------------------------------
    op = (plan.get("operation") or "aggregate").lower()
    country = country_override or plan.get("country")

    fe = FormulaEngine()
    metric_name = (plan.get("metric") or "").strip().lower()

    # --- Hard override: detect "sales mix" / "profit mix" in the raw query ----
    if re.search(r"\b(sales|profit)\s*mix\b", ql):
        which = re.search(r"\b(sales|profit)\s*mix\b", ql, flags=re.I).group(1).lower()
        metric_name = f"{which}_mix"
        plan["metric"] = metric_name
        plan["needs_clarification"] = False
        plan["clarification_message"] = None
        print(f"[DEBUG][metric-override] Forced metric to '{metric_name}' from raw query phrase.")

        # If user said "mix of <product>" and planner didn't capture it, extract it.
        if not plan.get("product"):
            m_prod = re.search(r"\b(?:of|for)\s+([A-Za-z0-9\+\-_/ ][^,;]*)", ql)
            if m_prod:
                plan["product"] = m_prod.group(1).strip()
                print(f"[DEBUG][metric-override] Captured target_product from query → '{plan['product']}'")

    # Default to sales for rank/compare
    if op in {"rank", "compare"} and not metric_name:
        plan["metric"] = "sales"
        metric_name = "sales"
        plan["needs_clarification"] = False
        plan["clarification_message"] = None
        print("[DEBUG][metric-fix] Defaulted missing metric to 'sales'")

    if not metric_name or metric_name not in fe.registry:
        metric_name = fe.resolve_name(query) or "sales"

    # --- Mix intent rescue from planner product field -----------------------
    # Handle cases like product="mix classic" when planner fails to tag as *_mix.
    prod_raw = (plan.get("product") or "").strip()
    m = re.match(r"^(?:mix)\s+(.+)$", prod_raw, flags=re.I)
    if m and metric_name in {"sales", "profit"}:
        target = m.group(1).strip()
        candidate = f"{metric_name}_mix"
        if candidate in fe.registry:
            metric_name = candidate
            plan["metric"] = candidate
            plan["product"] = target
            plan["force_product_only"] = False
            print(f"[DEBUG][mix-rescue] Upgraded metric to '{candidate}', target_product='{target}' from product='{prod_raw}'")

    # --- Grouping inference --------------------------------------------------
    try:
        plan = infer_group_by(query, plan)
    except Exception:
        pass

    # --- rank operation handling --------------------------------------------
    try:
        if op == "rank":
            gb = (plan.get("group_by") or "").lower().strip()
            if gb not in {"month", "product", "sku", "country"}:
                plan["group_by"] = "month"
            elif gb == "product" and (plan.get("product") or plan.get("filters")):
                plan["group_by"] = "month"
    except Exception:
        pass

    if op == "breakdown":
        op = "aggregate"
        if not (plan.get("group_by") or "").strip():
            plan["group_by"] = "product"

    # --- ✅ Planner-driven breakdown only (no keyword heuristics) -----------
    gb_planner = (plan.get("group_by") or "").lower().strip()
    is_mix_metric = metric_name in {"sales_mix", "profit_mix"}

    # Guard for mix metrics — ignore planner-provided breakdowns unless explicitly forced
    if is_mix_metric:
        # If planner accidentally sent group_by=product or sku, clear it.
        if plan.get("group_by"):
            print(f"[DEBUG][mix-guard] Clearing planner group_by='{plan.get('group_by')}' for {metric_name}")
        plan["group_by"] = None
        # Only allow breakdown if explicitly forced by caller
        want_breakdown = bool(plan.get("force_breakdown") is True)
    else:
        want_breakdown = False
        # 👇 NOW also allow month to request a breakdown
        if gb_planner in {"product", "sku", "month"}:
            want_breakdown = True
        elif plan.get("product"):
            want_breakdown = True
        elif op == "rank":
            want_breakdown = True

    # (the rest of your function remains the same below)
    # ------------------------------------------------------
    needed_cols = [
        "date_time","month","year","product_name","sku","type","description","order_id",
        "quantity","price_in_gbp","cost_of_unit_sold",
        "product_sales","promotional_rebates","postage_credits","gift_wrap_credits","shipping_credits",
        "product_sales_tax","marketplace_facilitator_tax","shipping_credits_tax","giftwrap_credits_tax","promotional_rebates_tax",
        "selling_fees","fba_fees","other_transaction_fees","other","total"
    ]

    # --- Natural language time parsing ---
    nl_time = FilterParser().parse_time(query)
    coerced = _coerce_time_range(plan.get("time_range"), user_id=int(user_id), country=country)

    # --- Fallback: fill plan.time_range if planner didn't set it ---
    tr = plan.get("time_range")
    needs_fill = True
    if isinstance(tr, dict):
        needs_fill = not (tr.get("start") and tr.get("end"))
    elif isinstance(tr, str):
        # keep as-is; _coerce_time_range will handle strings like "last 4 months"
        needs_fill = False  

    if needs_fill:
        try:
            clamp_try = clamp_relative_time_to_available(int(user_id), country, query)
            if clamp_try and clamp_try.get("start") and clamp_try.get("end"):
                plan["time_range"] = clamp_try
                print(f"[DEBUG][time-fallback] plan.time_range filled via clamp → {plan['time_range']}")
        except Exception as e:
            print(f"[DEBUG][time-fallback] clamp failed: {e}")

    # 🔄 Recompute coerced to stay in sync with possibly updated plan.time_range
    coerced = coerced or _coerce_time_range(plan.get("time_range"), user_id=int(user_id), country=country)
    print(f"[DEBUG][time] NL parsed: {nl_time}, coerced: {coerced}, final plan.time_range: {plan.get('time_range')}")
    if isinstance(plan.get("time_range"), str):
        if isinstance(coerced, dict):
            plan["time_range"] = coerced
            print(f"[DEBUG][time] plan.time_range normalized → {plan['time_range']}")
        else:
            plan["time_range"] = None
            print(f"[DEBUG][time] plan.time_range cleared (could not coerce)")

    def _is_full_month_span(rr: Optional[dict]) -> bool:
        if not (isinstance(rr, dict) and rr.get("start") and rr.get("end")):
            return False
        s = dt.datetime.fromisoformat(rr["start"])
        e = dt.datetime.fromisoformat(rr["end"])
        last = calendar.monthrange(e.year, e.month)[1]
        return (s.day == 1) and (e.day == last)

    def _months_between(s: dt.date, e: dt.date) -> tuple[list[int], list[int]]:
        months, years = [], []
        y, m = s.year, s.month
        while (y < e.year) or (y == e.year and m <= e.month):
            months.append(m); years.append(y)
            m += 1
            if m > 12:
                m = 1; y += 1
        return months, years

    # --- Compare override (semantic only, no hardcoded keywords) --------------
    compare_override = False

    months_from_nl = list(nl_time.get("months") or [])
    years_from_nl  = list(nl_time.get("years") or [])

    # regex fallback for "Jun 2025 vs Jul 2025"
    if len(months_from_nl) < 2:
        mon_pat = r"(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:t|tember)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)"
        pairs = re.findall(fr"{mon_pat}\s+(\d{{4}})", query, flags=re.I)
        if len(pairs) >= 2:
            months_from_nl = [
                {
                    "name": m,
                    "number": [None, "jan","feb","mar","apr","may","jun","jul","aug","sep","oct","nov","dec"].index(m[:3].lower()),
                }
                for m, _ in pairs[:2]
            ]
            years_from_nl = [int(y) for _, y in pairs[:2]]

    # detect multi-month spans (purely from a date range)
    full_span_has_2 = False
    if coerced and _is_full_month_span(coerced):
        s_date = dt.date.fromisoformat(coerced["start"])
        e_date = dt.date.fromisoformat(coerced["end"])
        ms, _ys = _months_between(s_date, e_date)
        full_span_has_2 = len(ms) >= 2

    if (not full_span_has_2) and nl_time.get("date_range"):
        sdt, edt_excl = nl_time["date_range"]
        last_incl = (edt_excl - dt.timedelta(days=1))
        full_span_has_2 = (sdt.year, sdt.month) != (last_incl.year, last_incl.month)

    # --- Decide if the user EXPLICITLY asked for a comparison ----------------
    ql_norm = (query or "").lower()

    has_explicit_compare = False

    # "vs / versus"
    if re.search(r"\b(vs\.?|versus)\b", ql_norm):
        has_explicit_compare = True
    # "compare", "comparison"
    elif re.search(r"\bcompare\b", ql_norm) or re.search(r"\bcomparison\b", ql_norm):
        has_explicit_compare = True
    # "difference/change between ..."
    elif re.search(r"\b(difference|diff|change)\s+between\b", ql_norm):
        has_explicit_compare = True
    # "between Jan 2025 and Feb 2025" (two months + 'between')
    elif " between " in ql_norm and len(months_from_nl) >= 2:
        has_explicit_compare = True
    # Year-on-year style keywords
    elif re.search(r"\b(yoy|y\/y|year\s*on\s*year)\b", ql_norm):
        has_explicit_compare = True

    # 🚦 Only apply auto-compare when user *explicitly* asked for comparison
    #    This prevents "last 3 months" from turning into unwanted YoY compare.
    if (
        has_explicit_compare
        and metric_name not in {"sales_mix", "profit_mix"}
        and (len(months_from_nl) >= 2 or full_span_has_2)
    ):
        op = "compare"
        compare_override = True
        coerced = None

    # -------------------- Time entity assembly -------------------------------
    time_entities: Dict[str, Any] = {}

    if compare_override:
        # use months/years parsed from NL for compare
        ms = months_from_nl
        ys = sorted(set(years_from_nl)) if years_from_nl else None
        if ms:
            # normalize shape to [{'name': ..., 'number': int}]
            norm = []
            for m in ms:
                if isinstance(m, dict) and "number" in m:
                    norm.append({
                        "name": m.get("name") or calendar.month_name[m["number"]],
                        "number": int(m["number"]),
                    })
                else:
                    try:
                        mi = int(m)
                        norm.append({
                            "name": calendar.month_name[mi],
                            "number": mi,
                        })
                    except Exception:
                        continue
            if norm:
                time_entities["months"] = norm
        if ys:
            time_entities["years"] = ys

    # 1️⃣ If parser gave an exact day, that wins
    elif "explicit_day" in nl_time:
        time_entities["explicit_day"] = nl_time["explicit_day"]

    # 1.5️⃣ FULL-YEAR OVERRIDE:
    # If the user basically said "in 2025" (years only) and the NL parser
    # produced a full-year date_range, use that and ignore the coerced span.
    elif (
        nl_time.get("years")
        and not nl_time.get("months")
        and not nl_time.get("explicit_day")
        and nl_time.get("date_range")
    ):
        # e.g. nl_time["date_range"] = (2025-01-01 00:00:00, 2025-12-31 23:59:59.999999)
        time_entities["date_range"] = nl_time["date_range"]

    # 2️⃣ Otherwise, if parser gave a date_range, use that
    elif nl_time.get("date_range"):
        time_entities["date_range"] = nl_time["date_range"]

    # 3️⃣ If planner/coerced span is NOT a full month
    #    (e.g. 2025-10-01 → 2025-10-15), respect that as a date_range
    elif coerced and not _is_full_month_span(coerced):
        start_dt = dt.datetime.fromisoformat(coerced["start"]).replace(
            tzinfo=dt.timezone.utc, hour=0, minute=0, second=0, microsecond=0
        )
        end_day = dt.datetime.fromisoformat(coerced["end"]).date()
        end_exclusive = dt.datetime.combine(
            end_day + dt.timedelta(days=1),
            dt.time.min,
            tzinfo=dt.timezone.utc,
        )
        time_entities["date_range"] = (start_dt, end_exclusive)

    # 4️⃣ Otherwise, if we have month(s) from NL, use those
    #    (this fixes the "October 2025" slow-sellers case where planner span = Jan–Oct)
    elif nl_time.get("months"):
        time_entities["months"] = nl_time["months"]
        if nl_time.get("years"):
            time_entities["years"] = nl_time["years"]

    # 5️⃣ Or if coerced is a full month or multi-month span, expand to month/year lists
    elif coerced and _is_full_month_span(coerced):
        s_date = dt.date.fromisoformat(coerced["start"])
        e_date = dt.date.fromisoformat(coerced["end"])
        ms, ys = _months_between(s_date, e_date)
        time_entities["months"] = [
            {"name": calendar.month_name[m], "number": m} for m in ms
        ]
        time_entities["years"] = sorted(set(ys))

    # 6️⃣ Fallback: only years were parsed
    elif nl_time.get("years"):
        time_entities["years"] = nl_time["years"]

    # Debug: see what exact time entities are sent to the SQL builder
    print(f"[DEBUG][time_entities] {time_entities}")

    # >>> Auto-extend single-month spans for TREND using clamp_relative_time_to_available
    if (op == "trend") and (not compare_override):
        print(f"[DEBUG][auto-extend] incoming plan.time_range={plan.get('time_range')}, coerced={coerced}")

        anchor = None
        candidate = coerced or plan.get("time_range")
        if candidate and _is_full_month_span(candidate):
            s = dt.date.fromisoformat(candidate["start"])
            e = dt.date.fromisoformat(candidate["end"])
            if (s.year, s.month) == (e.year, e.month):
                anchor = candidate

        if anchor:
            print(f"[DEBUG][auto-extend] detected single full-month anchor={anchor}")
            widened = clamp_relative_time_to_available(int(user_id), country, "last 3 months")
            print(f"[DEBUG][auto-extend] widened span from helper={widened}")
            if widened:
                coerced = widened
                plan["time_range"] = widened
                time_entities = {
                    "date_range": (
                        dt.datetime.fromisoformat(widened["start"]).replace(tzinfo=dt.timezone.utc),
                        dt.datetime.fromisoformat(widened["end"]).replace(tzinfo=dt.timezone.utc) + dt.timedelta(days=1),
                    )
                }
                print(f"[DEBUG][auto-extend] plan.time_range updated to {plan['time_range']}")
    # <<< End auto-extend

    is_mix_metric = metric_name in {"sales_mix", "profit_mix"}

    # ---------------- Filters ----------------
    def _is_days_to_reach_query(q: str) -> bool:
        ql_ = (q or "").lower()
        return ("how many days" in ql_ and "sales" in ql_) and any(
            w in ql_ for w in ["completed", "reached", "achieved", "made", "did"]
        )

    filter_equal: List[Tuple[str, str]] = []
    filter_like: List[Tuple[str, str]] = []
    filter_numeric: List[Tuple[str, str, float]] = []
    filter_in_list: List[Tuple[str, List[str]]] = []

    REAL_TEXT_COLS = {"product_name","sku","type","description","country","order_id","month","year"}
    REAL_NUM_COLS  = {"price_in_gbp","cost_of_unit_sold","quantity","product_sales","total","product_sales_tax","shipping_credits"}

    has_sku_clause = any(
        (isinstance(f, dict) and str(f.get("field","")).lower() == "sku" and str(f.get("op","")).lower() in {"=","in"})
        for f in (plan.get("filters") or [])
    )

    if _is_days_to_reach_query(query):
        if plan.get("product") and not has_sku_clause:
            # days-to-reach queries me bhi exact product hi lo
            filter_equal.append(("product_name", plan["product"]))
        print("[DEBUG] days-to-reach intent → ignoring planner numeric/text filters for SQL stage")
    else:
        # ✅ DEFAULT: product ke liye ALWAYS exact match (no family / variants)
        if plan.get("product") and not has_sku_clause and (not is_mix_metric or op in {"compare", "rank"}):

            # Pehle se lage huye product_name filters hata do
            filter_like  = [(c, v) for (c, v) in filter_like  if c.lower() != "product_name"]
            filter_equal = [(c, v) for (c, v) in filter_equal if c.lower() != "product_name"]

            # Ab sirf exact match wala filter lagao
            filter_equal.append(("product_name", plan["product"]))
            print(f"[DEBUG] product filter (EQUALS) applied for '{plan['product']}' (exact product only)")

        # pass-through any planner-provided filters
        for f in (plan.get("filters") or []):
            fld = (f.get("field") or "").lower()
            val = f.get("value")
            opf = (f.get("op") or "=").lower()
            if not fld or val is None:
                continue

            # 🛑 Agar plan.product set hai to planner ke product / product_name filters ko ignore karo
            if plan.get("product") and fld in {"product", "product_name"}:
                print(f"[DEBUG] ignoring planner product filter ({fld} {opf} {val}) because plan.product='{plan['product']}' is set")
                continue

            if fld not in (REAL_TEXT_COLS | REAL_NUM_COLS):
                print(f"[DEBUG] skipping planner filter on non-column '{fld}' (op={opf}, val={val})")
                continue

            if fld in REAL_NUM_COLS and opf in {">", ">=", "<", "<=", "=", "eq"}:
                try:
                    filter_numeric.append((fld, opf if opf != "eq" else "=", float(val)))
                except Exception:
                    print(f"[DEBUG] ignoring non-numeric value for numeric filter {fld} {opf} {val}")
                continue

            if opf in {"=", "eq"} and fld in REAL_TEXT_COLS:
                filter_equal.append((fld, str(val)))
            elif opf == "in" and isinstance(val, list):
                filter_in_list.append((fld, [str(x) for x in val]))
            else:
                filter_like.append((fld, str(val)))

    # ---------- PATCH D ----------
    try:
        if has_sku_clause:
            print("[DEBUG] PATCH D: SKU clause already present → skipping family disambiguation")
        else:
            fam = (plan.get("product") or "").strip()
            force_product_only = bool(plan.get("force_product_only"))

            if fam and not is_mix_metric:
                if force_product_only:
                    # Explicit: sirf is product ko hi lo, family logic bilkul mat chalao
                    print(f"[DEBUG] PATCH D: force_product_only=True for '{fam}' → skip SKU disambiguation")
                else:
                    # ✅ Family-SKU logic sirf tab jab user khud bole
                    # "all variants" / "full range" / "family total" / etc.
                    wants_all_intent = any(p in ql for p in [
                        "all variants", "all sizes", "entire range", "full range", "family total", "overall",
                        "all skus", "all sku", "sum all", "combined", "rollup", "roll-up", "roll up",
                        "variants total", "whole range"
                    ])

                    if not wants_all_intent:
                        # Normal case: "classic" → sirf product_name filter, no family merge/clarify
                        print(f"[DEBUG] PATCH D: no 'all variants' intent for '{fam}' → keeping product_name filter only")
                    else:
                        # User ne explicitly bola hai ke saare variants chahiye
                        skus_for_family = resolve_family_skus(int(user_id), country, fam, limit=500)
                        if len(skus_for_family) == 0:
                            print(f"[DEBUG] PATCH D: no SKUs found for '{fam}'")
                        elif len(skus_for_family) == 1:
                            only = skus_for_family[0]
                            filter_like  = [(c, v) for (c, v) in filter_like  if c.lower() != "product_name"]
                            filter_equal = [(c, v) for (c, v) in filter_equal if c.lower() != "product_name"]
                            filter_equal.append(("sku", only))
                            print(f"[DEBUG] PATCH D: single-SKU match → {only}")
                        else:
                            # Explicit all-variants intent → IN list, bina clarification ke
                            filter_like  = [(c, v) for (c, v) in filter_like  if c.lower() != "product_name"]
                            filter_equal = [(c, v) for (c, v) in filter_equal if c.lower() != "product_name"]
                            filter_in_list.append(("sku", skus_for_family))
                            print(f"[DEBUG] PATCH D: Using SKU IN (…) → {len(skus_for_family)} SKUs (explicit all variants)")
    except Exception as _e:
        print("[DEBUG][WARN] PATCH D failed:", _e)


    # ===== MIX METRIC FIX (final) =====
    ctx_target_product = None
    ctx_target_sku = None

    if is_mix_metric:
        # capture target before stripping
        for fld, val in list(filter_equal):
            if fld.lower() == "sku":
                ctx_target_sku = val
            elif fld.lower() == "product_name":
                ctx_target_product = val
        for fld, val in list(filter_like):
            if fld.lower() == "product_name" and not ctx_target_product:
                ctx_target_product = val
        for fld, vals in list(filter_in_list):
            if fld.lower() == "sku" and vals and not ctx_target_sku:
                ctx_target_sku = vals[0]

        # strip ALWAYS so denominator = all products
        filter_equal   = [(c, v) for (c, v) in filter_equal   if c.lower() not in ("sku","product_name")]
        filter_like    = [(c, v) for (c, v) in filter_like    if c.lower() not in ("sku","product_name")]
        filter_in_list = [(c, v) for (c, v) in filter_in_list if c.lower() not in ("sku","product_name")]
        print(f"[DEBUG][mix] stripped SKU/product filters for {metric_name} (denominator=all products)")

    # Build SQL
    sql, params, table = self.builder.build(
        user_id=user_id,
        country=(country if (country and country != "global") else None),
        selected_cols=needed_cols,
        time_entities=time_entities,
        filters={"equals": filter_equal,"text_like": filter_like,"numeric": filter_numeric,"in_list": filter_in_list},
        intents={},
        limit=1000000,
    )

    # Query → DataFrame
    try:
        with engine.connect() as conn:
            rs = conn.execute(text(sql), params)
            df = pd.DataFrame(rs.fetchall(), columns=rs.keys())
    except Exception as e:
        import traceback; traceback.print_exc()
        return pd.DataFrame({"_":[f"SQL error: {e}"]}), "sql_special"

    try:
        debug_money_sums(df, label=f"metric={metric_name} time={plan.get('time_range')} country={country}", fields=MONETARY_FIELDS)
        debug_money_sums(df, label="by SKU snapshot", fields=MONETARY_FIELDS, per="sku", top=10)
    except Exception as _e:
        print("[DEBUG] debug_money_sums failed:", _e)

    # ---------- SPECIAL CALCULATIONS (RUN BEFORE FE) ----------
    try:
        product_for_msg = plan.get("product")
        special = SpecialCalculations.try_days_to_reach_target(query, df, product_label=product_for_msg)
        print(f"[DEBUG] days_to_reach_target returned: {special!r}")
        if special:
            return pd.DataFrame({"_":[special]}), "sql_special"

        special_daily = SpecialCalculations.try_days_reaching_sales_daily_threshold(query, df, product_label=product_for_msg)
        print(f"[DEBUG] days_reaching_daily_threshold returned: {special_daily!r}")
        if special_daily:
            return pd.DataFrame({"_":[special_daily]}), "sql_special"
    except Exception as e:
        print("[DEBUG][WARN] Special calculations failed:", repr(e))

    # Compute via FormulaEngine
    gb_current = (plan.get("group_by") or "").lower().strip()
    ctx = {
        "country": country,
        "want_breakdown": want_breakdown,
        "raw_query": query,
        # 👇 pass group_by down so FE (e.g., _sales) can do month-wise logic
        "group_by": gb_current or None,
    }
    if metric_name in {"sales_mix", "profit_mix"}:
        if plan.get("product"):
            ctx["target_product"] = plan["product"]
        if ctx_target_product:
            ctx["target_product"] = ctx_target_product
        if ctx_target_sku:
            ctx["target_sku"] = ctx_target_sku

    evaluator = fe.registry.get(metric_name)
    print(f"[TRACE][FE] evaluator picked → metric='{metric_name}', func='{getattr(evaluator, '__name__', type(evaluator))}'")
    try:
        if metric_name == "sales":
            print(f"[TRACE][FE] evaluator is fe._sales? {evaluator is fe._sales}")
        elif metric_name == "profit":
            print(f"[TRACE][FE] evaluator is fe._profit? {evaluator is fe._profit}")
        elif metric_name == "tax":
            print(f"[TRACE][FE] evaluator is fe._tax? {evaluator is fe._tax}")
        elif metric_name == "credits":
            print(f"[TRACE][FE] evaluator is fe._credits? {evaluator is fe._credits}")
    except Exception as _e:
        print("[TRACE][FE] evaluator identity check failed:", _e)

    if evaluator is None:
        return pd.DataFrame({"_":[f"I can’t compute '{metric_name}'. Try one of: {', '.join(sorted(fe.registry.keys()))}."]}), "sql_special"

    try:
        payload = evaluator(df, ctx)
        print(f"[TRACE][FE] evaluator returned: keys={list(payload.keys()) if isinstance(payload, dict) else type(payload)} result={payload.get('result') if isinstance(payload, dict) else None}")
    except Exception as e:
        import traceback; traceback.print_exc()
        return pd.DataFrame({"_":[f"Compute error: {e}"]}), "sql_special"

    # --- Operation post-processing -------------------------------------------
    if op == "compare":
        # 🚦 Empty data guard
        if df.empty:
            country_lbl = country or "the selected marketplace"
            return pd.DataFrame({
                "_": [f"No data available in {country_lbl}. Try a different period or marketplace."]
            }), "sql_special"

        tdf = df.copy()

        # --- Normalize to base month/period (month → Timestamp) ---
        if "month" in tdf.columns and "year" in tdf.columns:
            mnum = tdf["month"].apply(fe._month_to_int)
            yint = tdf["year"].apply(fe._year_int)
            tdf["__period__"] = pd.to_datetime(
                dict(year=yint, month=mnum, day=1),
                errors="coerce", utc=True
            )
        elif "date_time" in tdf.columns:
            tdf["__dt"] = pd.to_datetime(tdf["date_time"], errors="coerce", utc=True)
            tdf["__period__"] = tdf["__dt"].dt.to_period("M").dt.to_timestamp().dt.tz_localize("UTC")
        else:
            return pd.DataFrame({"_": ["Compare requested but no time column available."]}), "sql_special"

        tdf = tdf.dropna(subset=["__period__"]).copy()

        # --- Detect comparison granularity ---
        compare_granularity = "month"
        ql_norm = query.lower()
        if re.search(r"\bQ[1-4]\b", ql_norm) or "quarter" in ql_norm:
            compare_granularity = "quarter"
            tdf["__comp_period__"] = tdf["__period__"].dt.to_period("Q").dt.start_time
        elif "half" in ql_norm or "h1" in ql_norm or "h2" in ql_norm:
            compare_granularity = "half"
            tdf["__comp_period__"] = (
                tdf["__period__"].dt.year.astype(str) + "-H" +
                ((tdf["__period__"].dt.month - 1) // 6 + 1).astype(str)
            )
        elif "year" in ql_norm or len(set(tdf["__period__"].dt.year)) > 1:
            compare_granularity = "year"
            tdf["__comp_period__"] = tdf["__period__"].dt.to_period("Y").dt.start_time
        else:
            compare_granularity = "month"
            tdf["__comp_period__"] = tdf["__period__"]

        periods = sorted(tdf["__comp_period__"].dropna().unique())

        # Pretty label helper
        def _label_for_period(p):
            if compare_granularity == "quarter":
                ts = pd.Timestamp(p)
                return f"Q{((ts.month - 1)//3)+1} {ts.year}"
            elif compare_granularity == "half":
                ts = pd.Timestamp(str(p)) if not isinstance(p, pd.Timestamp) else p
                half = "H1" if int(str(ts)[5]) == 1 else "H2"
                return f"{half} {ts.year}"
            elif compare_granularity == "year":
                return str(pd.Timestamp(p).year)
            else:
                return pd.Timestamp(p).strftime("%b %Y")

        # --- Decide comparison mode ---
        if " vs " in ql_norm or "compare" in ql_norm or len(periods) == 2:
            selected_periods = [periods[0], periods[-1]]
        else:
            selected_periods = periods  # range compare still chronological

        # --- Build overall rows for each selected period ---
        out_rows = []
        for p in selected_periods:
            part = tdf[tdf["__comp_period__"].eq(p)]
            ev = fe.registry.get(metric_name)
            if not ev:
                return pd.DataFrame({"_": [f"Metric '{metric_name}' not supported for compare."]}), "sql_special"

            ctx_per = {"country": country, "want_breakdown": False, "raw_query": query}
            # ✅ Preserve mix metric context so each period sees same target
            if metric_name in {"sales_mix", "profit_mix"}:
                tp = (ctx_target_product or plan.get("product"))
                if tp:
                    ctx_per["target_product"] = tp
                if ctx_target_sku:
                    ctx_per["target_sku"] = ctx_target_sku
                print(f"[DEBUG][ctx][compare] mix context → target_product={ctx_per.get('target_product')}, target_sku={ctx_per.get('target_sku')}")

            payload_p = ev(part, ctx_per)
            out_rows.append({
                "scope": "overall",
                "period": _label_for_period(p),
                metric_name: float(payload_p.get("result") or 0.0),
                "_period_key": pd.Timestamp(p)
            })

        # 🔧 Chronological sort
        overall_df = (
            pd.DataFrame(out_rows)
            .sort_values("_period_key")
            .drop(columns=["_period_key"])
            .reset_index(drop=True)
        )

        # --- Add change columns (prev is earlier period) ---
        if len(overall_df) >= 2:
            overall_df["prev"] = overall_df[metric_name].shift(1)
            overall_df["chg"] = overall_df[metric_name] - overall_df["prev"]

            # ✅ Avoid double percentage for already-% metrics
            if metric_name in {"sales_mix", "profit_mix", "acos", "conversion_rate"}:
                overall_df["pct"] = overall_df["chg"]  # percentage-point difference
            else:
                overall_df["pct"] = (overall_df["chg"] / overall_df["prev"]) * 100.0

        # --- Optional product breakdown (also chronological) ---
        if want_breakdown and metric_name in {"sales", "profit", "quantity_sold", "asp"}:
            product_rows = []
            for p in selected_periods:
                part = tdf[tdf["__comp_period__"].eq(p)]
                evp = fe.registry.get(metric_name)
                payload_p = evp(part, {"country": country, "want_breakdown": True, "raw_query": query})
                t = payload_p.get("table_df")
                if t is None or t.empty:
                    continue

                dfp = t.copy()
                if "level" in dfp.columns:
                    dfp = dfp[dfp["level"].astype(str).str.lower().eq("product")]

                for _, r in dfp.iterrows():
                    name = str(r.get("key") or r.get("product_name") or "")
                    val = float(r.get("result") or 0.0)
                    product_rows.append({
                        "scope": "product",
                        "period": _label_for_period(p),
                        "product": name,
                        metric_name: val,
                        "_period_key": pd.Timestamp(p)
                    })

            product_df = pd.DataFrame(product_rows)
            if not product_df.empty:
                product_df = (
                    product_df.sort_values(["product", "_period_key"])
                            .drop(columns=["_period_key"])
                            .reset_index(drop=True)
                )
                product_df["prev"] = product_df.groupby("product")[metric_name].shift(1)
                product_df["chg"] = product_df[metric_name] - product_df["prev"]

                # ✅ Same fix for breakdown table
                if metric_name in {"sales_mix", "profit_mix", "acos", "conversion_rate"}:
                    product_df["pct"] = product_df["chg"]
                else:
                    product_df["pct"] = (product_df["chg"] / product_df["prev"]) * 100.0

                overall_df["product"] = None
                result_df = pd.concat([overall_df, product_df], axis=0, ignore_index=True, sort=False)
            else:
                result_df = overall_df
        else:
            result_df = overall_df

        return result_df, "sql_formula"

    # --- Operation post-processing (rank) -----------------------------------
    if op == "rank":
        gb = (plan.get("group_by") or "").lower().strip()
        k = max(1, min(int(plan.get("top_k") or 1), 100))

        # also catch a few more synonyms for "lowest"
        lowest_words = ("bottom", "lowest", "least", "min", "worst", "fewest", "smallest")
        asc = (plan.get("sort_dir") or "desc").lower() == "asc" or any(w in ql for w in lowest_words)

        # --- Special handling for sales_mix / profit_mix monthly ranking ---
        # This ensures "which month has highest sales mix of X" works properly.
        if metric_name in {"sales_mix", "profit_mix"} and gb == "month":
            tdf = df.copy()

            # Normalize to month periods
            if "date_time" in tdf.columns:
                tdf["__dt"] = pd.to_datetime(tdf["date_time"], errors="coerce", utc=True)
                tdf["__period__"] = tdf["__dt"].dt.to_period("M").dt.to_timestamp()
            elif {"month", "year"}.issubset(tdf.columns):
                mnum = tdf["month"].apply(fe._month_to_int)
                yint = tdf["year"].apply(fe._year_int)
                tdf["__period__"] = pd.to_datetime(
                    dict(year=yint, month=mnum, day=1),
                    errors="coerce",
                    utc=True
                )
            else:
                return pd.DataFrame({"_": ["No month/year data found for mix ranking."]}), "sql_special"

            out_rows = []

            for p in sorted(tdf["__period__"].dropna().unique()):
                part = tdf[tdf["__period__"].eq(p)]

                # Prepare context for FE evaluator
                ctx_month = {"country": country, "want_breakdown": False, "raw_query": query}

                # Pass product/sku context if available
                if plan.get("product"):
                    ctx_month["target_product"] = plan["product"]
                if "ctx_target_product" in locals() and ctx_target_product:
                    ctx_month["target_product"] = ctx_target_product
                if "ctx_target_sku" in locals() and ctx_target_sku:
                    ctx_month["target_sku"] = ctx_target_sku

                ev = fe.registry.get(metric_name)
                if not ev:
                    return pd.DataFrame(
                        {"_": [f"Metric '{metric_name}' not supported for rank."]}
                    ), "sql_special"

                payload = ev(part, ctx_month)
                out_rows.append({
                    "month": pd.Timestamp(p).strftime("%b %Y"),
                    metric_name: float(payload.get("result") or 0.0),
                })

            ranked = pd.DataFrame(out_rows).sort_values(metric_name, ascending=asc).head(k).reset_index(drop=True)

            # Add note if fewer months available than expected
            available_months = len(set(pd.to_datetime(tdf["__period__"], errors="coerce").dt.month))
            if available_months < 12:
                note = f"Note: Only data for {available_months} month{'s' if available_months > 1 else ''} available."
                print(f"[DEBUG][rank-mix] {note}")
                ranked["note"] = note

            return ranked, "sql_formula"

        # --- Generic FE-driven ranking (products, months, etc.) ---
        if gb in {"month", "product", "sku", "country"}:
            ranked = fe.rank_groups(
                df,
                {"country": country, "raw_query": query},
                metric_name=metric_name,
                group_by=gb,
                top_k=k,
                ascending=asc,
            )
            # If FE returned an error-style dataframe, pass it through
            if "_" in ranked.columns and "result" not in ranked.columns:
                return ranked, "sql_special"

            # Pretty output column names
            col_label = {"month": "month", "product": "product", "sku": "sku", "country": "country"}[gb]
            ranked = ranked.rename(columns={"group": col_label, "result": metric_name})
            return ranked, "sql_formula"

        # --- Fallback: rank on evaluator's table_df ---
        t = payload.get("table_df") if isinstance(payload, dict) else None
        if t is None or t.empty:
            return pd.DataFrame({"_": ["No rows found to rank."]}), "sql_special"

        def _subset(df, level_name):
            if "level" in df.columns:
                return df[df["level"].astype(str).str.lower().eq(level_name)]
            return df.iloc[0:0]

        if gb == "sku":
            subset = _subset(t, "sku") or _subset(t, "product")
        elif gb == "product":
            subset = _subset(t, "product") or _subset(t, "sku")
        else:
            subset = t

        if subset is None or subset.empty:
            return pd.DataFrame({"_": ["No product/SKU breakdown available to rank."]}), "sql_special"

        ranked = subset.sort_values("result", ascending=asc, kind="mergesort").head(k).reset_index(drop=True)
        return ranked, "sql_formula"

    # >>>>>>>>>>> MULTI-METRIC TREND (overall + optional product-wise) <<<<<<<<<<<
    if op == "trend":
        # 🚦 allow product/SKU breakdown only for these metrics
        BREAKDOWN_OK = {"sales", "profit", "quantity_sold", "asp", "fba_fees", "selling_fees"}

        # Map the requested metric to its output column name
        PRIMARY_OUT_NAME = {
            "sales": "sales",
            "profit": "profit",
            "quantity_sold": "quantity",
            "asp": "asp",
            "fba_fees": "fba_fees",
            "selling_fees": "selling_fees",

            # account-level only metrics — no product/SKU breakdown
            "ads_spend": "ads_spend",
            "advertising_total": "ads_spend",
            "acos": "acos",
            "cm2_profit": "cm2_profit",
            "reimbursements": "reimbursements",
            "reimbursement_factors": "reimbursement_factors",
        }.get(metric_name, "value")

        tdf = df.copy()

        # normalize to month period using existing helpers
        has_month_year = "month" in tdf.columns and "year" in tdf.columns
        if has_month_year:
            mnum = tdf["month"].apply(fe._month_to_int)
            yint = tdf["year"].apply(fe._year_int)
            tdf["__period__"] = pd.to_datetime(
                dict(year=yint, month=mnum, day=1),
                errors="coerce",
                utc=True,
            )
        elif "date_time" in tdf.columns:
            tdf["__dt"] = pd.to_datetime(tdf["date_time"], errors="coerce", utc=True)
            tdf["__period__"] = (
                tdf["__dt"].dt.to_period("M").dt.to_timestamp().dt.tz_localize("UTC")
            )
        else:
            return pd.DataFrame({"_": ["Trend requested but no time column available."]}), "sql_special"

        tdf = tdf.dropna(subset=["__period__"]).copy()

        # clamp to requested time_range (if present)
        tr = plan.get("time_range")
        if isinstance(tr, dict) and tr.get("start") and tr.get("end"):
            start = pd.to_datetime(tr["start"], errors="coerce", utc=True)
            end = pd.to_datetime(tr["end"], errors="coerce", utc=True)
            if pd.notna(start) and pd.notna(end):
                tdf = tdf[
                    (tdf["__period__"] >= start.to_period("M").to_timestamp().tz_localize("UTC"))
                    & (tdf["__period__"] <= end.to_period("M").to_timestamp().tz_localize("UTC"))
                ]

        periods = sorted(tdf["__period__"].dropna().unique())
        if not periods:
            return pd.DataFrame({"_": ["No data for the requested period(s)."]}), "sql_special"

        # ——— Overall series for the PRIMARY metric ———
        overall_rows = []
        for p in periods:
            part = tdf[tdf["__period__"].eq(p)]
            row = {"scope": "overall", "period": pd.Timestamp(p).to_pydatetime()}

            ev_primary = fe.registry.get(metric_name)
            if not ev_primary:
                return pd.DataFrame(
                    {"_": [f"Metric '{metric_name}' is not supported."]}
                ), "sql_special"

            # ✅ Preserve mix context (fixes trend bugs for sales_mix / profit_mix)
            ctx_per = {"country": country, "want_breakdown": False, "raw_query": query}
            if metric_name in {"sales_mix", "profit_mix"}:
                tp = (ctx_target_product or plan.get("product"))
                if tp:
                    ctx_per["target_product"] = tp
                if ctx_target_sku:
                    ctx_per["target_sku"] = ctx_target_sku
                print(
                    f"[DEBUG][ctx][trend] mix context → "
                    f"target_product={ctx_per.get('target_product')}, "
                    f"target_sku={ctx_per.get('target_sku')}"
                )

            payload_primary = ev_primary(part, ctx_per)
            row[PRIMARY_OUT_NAME] = float(payload_primary.get("result") or 0.0)

            # optional convenience: also add the “core four” for context
            if metric_name in BREAKDOWN_OK:
                for metric, out_col in [
                    ("sales", "sales"),
                    ("profit", "profit"),
                    ("asp", "asp"),
                    ("quantity_sold", "quantity"),
                ]:
                    if out_col == PRIMARY_OUT_NAME:
                        continue
                    ev = fe.registry.get(metric)
                    if ev:
                        payload_m = ev(
                            part,
                            {
                                "country": country,
                                "want_breakdown": False,
                                "raw_query": query,
                            },
                        )
                        row[out_col] = float(payload_m.get("result") or 0.0)

            overall_rows.append(row)

        overall_df = (
            pd.DataFrame(overall_rows)
            .sort_values("period")
            .reset_index(drop=True)
        )

        # compute deltas only for the PRIMARY metric
        prev_col = f"{PRIMARY_OUT_NAME}_prev"
        chg_col = f"{PRIMARY_OUT_NAME}_chg"
        pct_col = f"{PRIMARY_OUT_NAME}_pct"
        overall_df[prev_col] = overall_df[PRIMARY_OUT_NAME].shift(1)
        overall_df[chg_col] = overall_df[PRIMARY_OUT_NAME] - overall_df[prev_col]

        # ✅ % metrics should use percentage-point deltas, not % of %
        if metric_name in {"sales_mix", "profit_mix", "acos", "conversion_rate"}:
            overall_df[pct_col] = overall_df[chg_col]
        else:
            overall_df[pct_col] = (overall_df[chg_col] / overall_df[prev_col]) * 100.0

        # ——— Product/SKU breakdown ONLY for metrics where it makes sense ———
        product_rows = []
        if want_breakdown and (metric_name in BREAKDOWN_OK):
            for p in periods:
                part = tdf[tdf["__period__"].eq(p)]
                per_prod: dict[str, dict] = {}

                def _merge_metric(mname: str, out_col: str):
                    ev = fe.registry.get(mname)
                    if not ev:
                        return
                    payload = ev(
                        part,
                        {
                            "country": country,
                            "want_breakdown": True,
                            "raw_query": query,
                        },
                    )
                    t = payload.get("table_df")
                    if t is None or t.empty:
                        return

                    dfp = t.copy()
                    if "level" in dfp.columns:
                        dfp = dfp[
                            dfp["level"].astype(str).str.lower().eq("product")
                        ]
                    if dfp.empty:
                        return

                    for _, r in dfp.iterrows():
                        name = str(
                            r.get("key")
                            or r.get("product_name")
                            or r.get("label")
                            or ""
                        )
                        val = float(r.get("result") or 0.0)
                        per_prod.setdefault(
                            name,
                            {
                                "scope": "product",
                                "period": pd.Timestamp(p).to_pydatetime(),
                                "product": name,
                            },
                        )
                        per_prod[name][out_col] = val

                # NOTE: do NOT call _asp with want_breakdown=True here
                _merge_metric("sales", "sales")
                _merge_metric("profit", "profit")
                _merge_metric("quantity_sold", "quantity")

                # derive ASP = sales / quantity per product
                for rec in per_prod.values():
                    s = rec.get("sales")
                    q = rec.get("quantity")
                    if s is not None and q not in (None, 0):
                        rec["asp"] = s / q

                for rec in per_prod.values():
                    product_rows.append(rec)

        product_df = pd.DataFrame(product_rows)
        if not product_df.empty:
            product_df = (
                product_df.sort_values(["period", "product"])
                .reset_index(drop=True)
            )
            # deltas for the “core four” only (sales, profit, asp, quantity)
            for m in ["sales", "profit", "asp", "quantity"]:
                if m in product_df.columns:
                    product_df[f"{m}_prev"] = product_df.groupby("product")[m].shift(1)
                    product_df[f"{m}_chg"] = product_df[m] - product_df[f"{m}_prev"]
                    product_df[f"{m}_pct"] = (
                        product_df[f"{m}_chg"] / product_df[f"{m}_prev"]
                    ) * 100.0

        overall_out = overall_df.copy()
        overall_out["product"] = None
        overall_out["scope"] = "overall"

        result_df = pd.concat(
            [overall_out, product_df], axis=0, ignore_index=True, sort=False
        )
        result_df["period"] = (
            pd.to_datetime(result_df["period"], utc=True, errors="coerce")
            .dt.strftime("%b %Y")
        )

        # strip product rows for account-level metrics (ads_spend, acos, cm2_profit, reimbursements, etc.)
        if metric_name not in BREAKDOWN_OK:
            result_df = result_df[
                result_df["scope"].astype(str).str.lower() == "overall"
            ].copy()

        return result_df, "sql_formula"

    # --- Default aggregate/finalization ------------------------------------
    t = payload.get("table_df") if isinstance(payload, dict) else None
    if isinstance(t, pd.DataFrame) and not t.empty:
        # Decide preferred output level from the user's wording and the plan.
        ql_norm = (query or "").lower()
        gb = (plan.get("group_by") or "").lower()

        def _wants_product(text: str) -> bool:
            return bool(re.search(
                r"(per|by|each|split|break\s*down)\s+(product|item|asin)|"
                r"(product[-\s]*wise)|"
                r"(across\s+products)|"
                r"(product\s+breakdown)",
                text, re.I
            ))

        def _wants_sku(text: str) -> bool:
            return bool(re.search(
                r"(per|by|each|split|break\s*down)\s+(sku|asin|variant)|"
                r"(sku[-\s]*wise)|"
                r"(across\s+skus)|"
                r"(sku\s+breakdown)",
                text, re.I
            ))

        wants_product = _wants_product(ql_norm) or (gb == "product")
        wants_sku     = _wants_sku(ql_norm)     or (gb == "sku")

        # If a level is clearly requested, keep only that level and hide TOTAL/noise.
        if "level" in t.columns:
            if wants_product and not wants_sku:
                t = t[t["level"].astype(str).str.lower().eq("product")].copy()
            elif wants_sku and not wants_product:
                t = t[t["level"].astype(str).str.lower().eq("sku")].copy()
            # else: keep whatever FE returned (TOTAL + product + sku)

        # Keep old behavior for top_k on monthly aggregates
        k = int(plan.get("top_k") or 0)
        sort_dir = (plan.get("sort_dir") or "desc").lower()
        asc = (sort_dir == "asc")

        metric_cols_pref = ["result", "sales", "orders", "profit", "quantity", "total"]
        metric_col = next((c for c in metric_cols_pref if c in t.columns), None)

        # Only trim for ranking-type queries, not normal breakdowns
        if op == "rank" and gb == "month" and k > 0 and metric_col:
            t = (
                t.sort_values(by=[metric_col], ascending=asc, kind="mergesort")
                .head(k)
                .reset_index(drop=True)
            )

        # 🔹 NEW: for sales breakdowns, hide component_* columns so only main sales number is shown
        if metric_name == "sales":
            comp_cols = [c for c in t.columns if c.startswith("component_")]
            if comp_cols:
                t = t.drop(columns=comp_cols)

        return t, "sql_formula"

    # If evaluator only gave a single result number
    if isinstance(payload, dict) and "result" in payload:
        return pd.DataFrame([{"scope": "overall", metric_name: payload["result"]}]), "sql_formula"

    # Safety net (never fall off with None)
    return pd.DataFrame({"_": ["No result produced."]}), "sql_special"






def _auto_fill_defaults(plan, query, user_id, country_override, lc):
    """Fill obvious defaults so the bot doesn’t constantly ask clarifications."""
    filled = False

    # --- 1️⃣ Country ---
    if not plan.get("country"):
        ctry = (
            parse_country_strict(query)
            or country_override
            or (lc or {}).get("country")
            or "UK"
        )
        plan["country"] = ctry
        filled = True
        print(f"[DEFAULT] country → {ctry}")

    # --- 2️⃣ Time range ---
    try:
        fp = FilterParser()
        t = fp.parse_time(query)
        user_mentioned_time = bool(
            t.get("explicit_day") or t.get("months") or t.get("years") or t.get("date_range")
        )
    except Exception:
        user_mentioned_time = False

    if not plan.get("time_range") and not user_mentioned_time:
        op = str(plan.get("operation") or "").lower()
        window = (
            "last 6 months" if op == "trend"
            else "last 90 days" if op in {"rank", "breakdown", "compare"}
            else "last 30 days"
        )
        tr = clamp_relative_time_to_available(user_id, plan.get("country"), window)
        if tr:
            plan["time_range"] = tr
            filled = True
            print(f"[DEFAULT] time_range ({window}) → {tr}")

    # --- 3️⃣ top_k ---
    if (
        str(plan.get("operation")).lower() in {"rank", "breakdown"}
        and not plan.get("top_k")
    ):
        plan["top_k"] = 5
        filled = True
        print("[DEFAULT] top_k → 5")

    # --- 4️⃣ product auto-pick if only one candidate ---
    try:
        if plan.get("product") and isinstance(plan["product"], str):
            eff_country = (country_override or plan.get("country") or "UK").upper()
            if eff_country not in {"UK", "US"}:
                eff_country = "UK"
            cands = product_candidates(engine, user_id, eff_country, plan["product"], limit=5) or []
            names = [(c.get("product_name") or "").strip() for c in cands]
            if len(cands) == 1 or plan["product"].strip() in names:
                plan["product"] = names[0] if names else plan["product"]
                filled = True
                print(f"[DEFAULT] product resolved → {plan['product']}")
    except Exception as e:
        print("[DEBUG][defaults] product auto-pick failed:", e)

    if filled:
        plan["needs_clarification"] = False
    return plan, filled






# ---------- orchestrator ----------
class QueryEngine:
    def __init__(self):
        self.vector_db = VectorDatabase()
        self.filters = FilterParser()
        self.builder = QueryBuilder()

    def _resolve_product_name(self, *, user_id: str, country: Optional[str], phrase: str) -> Optional[str]:
        if not engine or not phrase:
            return None
        table = self.builder.table_for(user_id, country)
        try:
            with engine.connect() as conn:
                rs = conn.execute(
                    text(f"""
                        SELECT DISTINCT product_name, CHAR_LENGTH(product_name) AS name_len
                        FROM {table}
                        WHERE product_name ILIKE :p
                        ORDER BY name_len ASC, product_name ASC
                        LIMIT 100
                    """),
                    {"p": f"%{phrase}%"},
                )
                rows = rs.fetchall()

            names = [r[0] for r in rows if r[0]]
            if not names:
                return None
            for n in names:
                if str(n).strip().lower() == phrase.strip().lower():
                    return n
            return names[0]
        except Exception:
            logger.exception("resolve_product_name error")
            return None

    def _fee_rows_for_product(self, *, user_id: str, country: Optional[str], product_phrase: str, time_entities: Dict[str, Any], ql: str) -> pd.DataFrame:
        """Fetch fee rows tied to a product via ORDER_ID (EXISTS subquery)."""
        if not engine or not product_phrase:
            return pd.DataFrame()
        table = self.builder.table_for(user_id, country)
        params: Dict[str, Any] = {"prod_like": f"%{product_phrase}%"}
        where: List[str] = []

        # Time filters (alias t.)
        if time_entities.get("years"):
            wc = self.builder.year_where(time_entities["years"], params).replace("year", "t.year")
            where.append(wc)
        months = time_entities.get("months", [])
        if months:
            nums = [m["number"] for m in months]
            names = [m["name"] for m in months]
            wc = self.builder.month_where(nums, names, params).replace("month", "t.month").replace("MONTH", "t.month")
            where.append(wc)
        if "date_range" in time_entities:
            start_dt, end_dt = time_entities["date_range"]
            params["dt_start"] = start_dt
            params["dt_end"] = end_dt
            where.append("(t.date_time::timestamptz BETWEEN :dt_start AND :dt_end)")

        # Country filter for global table
        if not self.builder._is_per_country_table(table):
            if "uk" in ql:
                params["ctry"] = "UK"
                where.append("LOWER(t.country) = LOWER(:ctry)")
            elif "us" in ql or "usa" in ql or "united states" in ql:
                params["ctry"] = "US"
                where.append("LOWER(t.country) = LOWER(:ctry)")

        # Fee type
        fee_types: List[str] = []
        if "fba" in ql or "inventory fee" in ql:
            fee_types.append("(LOWER(t.type) LIKE '%fba%' AND LOWER(t.type) LIKE '%fee%')")
        if "amazon fee" in ql or "amazon fees" in ql:
            fee_types.append("(LOWER(t.type) LIKE '%amazon%' AND LOWER(t.type) LIKE '%fee%')")
        if not fee_types:
            fee_types.append("(LOWER(t.type) LIKE '%fee%')")

        where.append("(" + " OR ".join(fee_types) + ")")

        # EXISTS: tie fees to orders that have this product
        exists_clause = f"""
            EXISTS (
                SELECT 1
                FROM {table} p
                WHERE p.order_id = t.order_id
                  AND LOWER(p.product_name) LIKE LOWER(:prod_like)
            )
        """
        where.append(exists_clause)

        sql = f"""
            SELECT t.date_time, t.total, t.type, t.order_id, t.sku, t.month, t.year
            FROM {table} t
            WHERE {" AND ".join(where)}
            ORDER BY t.date_time ASC
            LIMIT 100000
        """.strip()

        try:
            with engine.connect() as conn:
                rs = conn.execute(text(sql), params)
                return pd.DataFrame(rs.fetchall(), columns=rs.keys())
        except Exception:
            logger.exception("fee query error")
            return pd.DataFrame()

    def process(self, *, query: str, user_id: str, country_override: Optional[str] = None) -> Tuple[pd.DataFrame, str]:
        t0 = time.perf_counter()
        print("\n" + "="*80)
        print("[DEBUG] process() called")
        print("[DEBUG] query:", repr(query))
        print("[DEBUG] user_id:", user_id, "country_override:", country_override)

        # --- Early small-talk / capability fast-path (NO retrieval) ---------------
        if is_smalltalk(query):
            msg = "Hey! 👋 I can help you analyze Amazon sales, fees, taxes, profit, and trends. What would you like to explore?"
            print("[DEBUG] Small-talk detected → short-circuit without retrieval.")
            return pd.DataFrame({"_": [msg]}), "sql_special"

        if is_capability(query):
            msg = (
                "I’m your finance/RAG copilot for Amazon data. I can:\n"
                "• Summaries: “Overall profit last 30 days (UK).”\n"
                "• Rankings: “Top 5 SKUs by sales in July 2025.”\n"
                "• Fees: “FBA fees for Product X last week.”\n"
                "• Taxes & rebates: “Marketplace facilitator tax in Aug 2025 (US).”\n"
                "• Trends: “MoM sales growth for ASIN B07… in 2025.”\n"
                "• Targets: “Days crossing £1,000 daily sales in June.”\n\n"
                "I won’t scan your database until you ask for specific info. "
                "Tell me the metric + time range + (optional) country/product."
            )
            print("[DEBUG] Capability question detected → short-circuit without retrieval.")
            return pd.DataFrame({"_": [msg]}), "sql_special"

        # Optional: trivial-input guard to avoid accidental heavy scans -------------
        if len((query or "").split()) < 3:
            hint = "Tell me what to analyze (metric + time + optional country/product)."
            print("[DEBUG] Trivial input (<3 tokens) → short-circuit hint.")
            return pd.DataFrame({"_": [hint]}), "sql_special"

        # --- Parse phase -----------------------------------------------------------
        try:
            intent = self.filters.extract_intent(query)
            print("[DEBUG] Extracted intent:", intent)

            time_entities = self.filters.parse_time(query)
            print("[DEBUG] Time entities:", time_entities)

            cols = self.filters.parse_columns(query)
            print("[DEBUG] Selected columns:", cols)

            filter_entities = self.filters.parse_filters(query)
            print("[DEBUG] Filters parsed (pre-product):", filter_entities)
        except Exception as e:
            print("[DEBUG][ERROR] Parsing phase failed:", repr(e))
            raise

        ql = query.lower()
        fee_intent = any(x in ql for x in ["fee","fees","charge","charges"])
        print("[DEBUG] fee_intent:", fee_intent)

        # --- Country handling ------------------------------------------------------
        country = country_override
        for k, v in filter_entities.get("equals", []):
            if k == "country":
                country = v
        print("[DEBUG] Effective country:", country)

        # --- Product resolution ----------------------------------------------------
        try:
            has_explicit_product = any(k == "product_name" for k, _ in filter_entities.get("equals", []))
            print("[DEBUG] has_explicit_product:", has_explicit_product)

            guessed_phrase = None if has_explicit_product else self.filters.guess_product_phrase(query)
            print("[DEBUG] guessed_phrase:", repr(guessed_phrase))

            resolved_exact = None
            if guessed_phrase:
                t_resolve = time.perf_counter()
                resolved_exact = self._resolve_product_name(user_id=user_id, country=country, phrase=guessed_phrase)
                print("[DEBUG] _resolve_product_name took: %.3f ms" % ((time.perf_counter()-t_resolve)*1000.0))
                print("[DEBUG] resolved_exact:", repr(resolved_exact))

                if resolved_exact:
                    filter_entities.setdefault("equals", []).append(("product_name", resolved_exact))
                    has_explicit_product = True
                    print("[DEBUG] Added equals filter product_name:", repr(resolved_exact))
                else:
                    filter_entities.setdefault("text_like", []).append(("product_name", guessed_phrase))
                    print("[DEBUG] Added text_like filter product_name contains:", repr(guessed_phrase))

            print("[DEBUG] Filters parsed (post-product):", filter_entities)
        except Exception as e:
            print("[DEBUG][ERROR] Product resolution failed:", repr(e))
            # continue; we can still try to answer without product narrowing

        # ---------------- PATCH D: prefer SKU filters over product_name LIKE ----------------
        try:
            skus_for_family: list[str] = []
            # If we only have a family phrase (no exact product resolution), resolve to SKUs
            if guessed_phrase and not resolved_exact:
                skus_for_family = resolve_family_skus(int(user_id), country, guessed_phrase, limit=500)

            if skus_for_family:
                # Remove any loose product_name LIKE (to avoid mixing variants)
                if "text_like" in filter_entities:
                    filter_entities["text_like"] = [
                        (c, v) for (c, v) in filter_entities["text_like"]
                        if c.lower() != "product_name"
                    ]
                # If there was no explicit equality on product_name, remove accidental equals too
                if "equals" in filter_entities and not has_explicit_product:
                    filter_entities["equals"] = [
                        (c, v) for (c, v) in filter_entities["equals"]
                        if c.lower() != "product_name"
                    ]
                # Add strict SKU list
                filter_entities.setdefault("in_list", []).append(("sku", skus_for_family))
                print(f"[DEBUG] Using SKU IN (...) for family '{guessed_phrase}': {len(skus_for_family)} SKUs")
        except Exception as _e:
            print("[DEBUG][WARN] SKU preference patch failed:", _e)
        # -----------------------------------------------------------------------------

        # --- Breakdown policy ------------------------------------------------------
        breakdown_tokens = ["per product", "by product", "per sku", "by sku"]
        asked_for_breakdown = any(tok in ql for tok in breakdown_tokens)
        want_breakdown = bool(asked_for_breakdown or has_explicit_product or guessed_phrase)
        print("[DEBUG] asked_for_breakdown:", asked_for_breakdown, "want_breakdown:", want_breakdown)

        # --- Fee-special path (EXISTS query) --------------------------------------
        if fee_intent and (resolved_exact or guessed_phrase):
            print("[DEBUG] Fee intent + product detected → running _fee_rows_for_product()")
            try:
                t_fee = time.perf_counter()
                df_fee = self._fee_rows_for_product(
                    user_id=user_id,
                    country=country,
                    product_phrase=(resolved_exact or guessed_phrase),
                    time_entities=time_entities,
                    ql=ql,
                )
                print("[DEBUG] _fee_rows_for_product() took: %.3f ms; rows: %s" %
                    ((time.perf_counter()-t_fee)*1000.0, 0 if df_fee is None else len(df_fee)))
                if df_fee is not None and not df_fee.empty:
                    print("[DEBUG] Returning fee rows (sql).")
                    print("[DEBUG] TOTAL elapsed: %.3f ms" % ((time.perf_counter()-t0)*1000.0))
                    return df_fee, "sql"
            except Exception as e:
                print("[DEBUG][ERROR] _fee_rows_for_product failed:", repr(e))

        # --- Decide SQL vs vector --------------------------------------------------
        try:
            force_sql = (
                self.filters.needs_sql(filter_entities)
                or bool(time_entities)
                or has_explicit_product
                or bool(guessed_phrase)
            )
            print("[DEBUG] force_sql:", force_sql)

            if not force_sql:
                print("[DEBUG] Trying vector search …")
                t_vec = time.perf_counter()
                results = self.vector_db.semantic_search(query, user_id, country, top_k=10)
                dt_vec = (time.perf_counter()-t_vec)*1000.0
                print("[DEBUG] vector search took: %.3f ms; results:" % dt_vec, 0 if results is None else len(results))
                if results:
                    df = pd.DataFrame([r["data"] for r in results])
                    print("[DEBUG] Returning vector result rows:", len(df))
                    print("[DEBUG] TOTAL elapsed: %.3f ms" % ((time.perf_counter()-t0)*1000.0))
                    return df, "vector"
        except Exception as e:
            print("[DEBUG][ERROR] Vector flow failed:", repr(e))
            # fall back to SQL

        # --- Build + run SQL -------------------------------------------------------
        try:
            print("[DEBUG] Building SQL …")
            t_sql = time.perf_counter()
            sql, params, table = self.builder.build(
                user_id=user_id,
                country=country,
                selected_cols=cols,
                time_entities=time_entities,
                filters=filter_entities,
                intents=intent.get("intents", {}),
                limit=100000,
            )
            dt_build = (time.perf_counter()-t_sql)*1000.0
            print("[DEBUG] SQL build took: %.3f ms" % dt_build)
            print("[DEBUG] Target table:", table)
            print("[DEBUG] SQL:\n", sql)
            print("[DEBUG] Params:", params)

            print("[DEBUG] Executing SQL …")
            t_exec = time.perf_counter()
            with engine.connect() as conn:
                rs = conn.execute(text(sql), params)
                df = pd.DataFrame(rs.fetchall(), columns=rs.keys())
            dt_exec = (time.perf_counter()-t_exec)*1000.0
            print("[DEBUG] SQL exec took: %.3f ms; rows:" % dt_exec, len(df))
        except Exception as e:
            print("[DEBUG][ERROR] SQL phase failed:", repr(e))
            raise

        # --- Special calculations --------------------------------------------------
        try:
            special = SpecialCalculations.try_days_to_reach_target(query, df, product_label=resolved_exact or guessed_phrase)
            print(f"[DEBUG] days_to_reach_target returned: {special!r}") 
            if special:
                print("[DEBUG] Special (days_to_reach_target) matched:", special)
                print("[DEBUG] TOTAL elapsed: %.3f ms" % ((time.perf_counter()-t0)*1000.0))
                return pd.DataFrame({"_": [special]}), "sql_special"

            special_daily = SpecialCalculations.try_days_reaching_sales_daily_threshold(query, df, product_label=resolved_exact or guessed_phrase)
            if special_daily:
                print("[DEBUG] Special (days_reaching_daily_threshold) matched:", special_daily)
                print("[DEBUG] TOTAL elapsed: %.3f ms" % ((time.perf_counter()-t0)*1000.0))
                return pd.DataFrame({"_": [special_daily]}), "sql_special"
        except Exception as e:
            print("[DEBUG][WARN] Special calculations failed:", repr(e))

        # --- Formula evaluation ----------------------------------------------------
        try:
            formula_name = formulae.resolve_name(query)
            print("[DEBUG] resolve_name(query) →", repr(formula_name))

            if formula_name:
                ctx = {
                    "user_id": user_id,
                    "country": country,
                    "time_entities": time_entities,
                    "filters": filter_entities,
                    "intents": intent.get("intents", {}),
                    "want_breakdown": want_breakdown,
                }
                print("[DEBUG] Evaluating formula:", formula_name, "with ctx.want_breakdown:", want_breakdown)

                evaluator = formulae.registry.get(formula_name)
                if evaluator:
                    t_eval = time.perf_counter()
                    payload = evaluator(df, ctx)
                    dt_eval = (time.perf_counter()-t_eval)*1000.0
                    print("[DEBUG] Formula evaluation took: %.3f ms" % dt_eval)
                    if payload:
                        table_df = payload.get("table_df")
                        if table_df is not None and not table_df.empty:
                            print("[DEBUG] Returning sql_formula (table rows):", len(table_df))
                            print("[DEBUG] TOTAL elapsed: %.3f ms" % ((time.perf_counter()-t0)*1000.0))
                            return table_df, "sql_formula"
                        elif payload.get("result") is not None:
                            pretty = pd.DataFrame([{
                                "metric": formula_name,
                                "result": payload["result"],
                                "explanation": payload.get("explanation", "")
                            }])
                            print("[DEBUG] Returning sql_formula (single-row result)")
                            print("[DEBUG] TOTAL elapsed: %.3f ms" % ((time.perf_counter()-t0)*1000.0))
                            return pretty, "sql_formula"
                else:
                    print("[DEBUG][WARN] No evaluator for formula:", formula_name)
        except Exception as e:
            print("[DEBUG][ERROR] Formula evaluation failed:", repr(e))
            # fall through to default return

        # --- Default return --------------------------------------------------------
        print("[DEBUG] Returning raw SQL rows:", len(df))
        print("[DEBUG] TOTAL elapsed: %.3f ms" % ((time.perf_counter()-t0)*1000.0))
        return df, "sql"



# Attach executor to QueryEngine (no class refactor needed)
if not hasattr(QueryEngine, "exec_plan_via_formula"):
    QueryEngine.exec_plan_via_formula = exec_plan_via_formula

# singletons
engine_q = QueryEngine()
analyst = DataAnalyzer()
formulae = FormulaEngine()


from flask import Response, request

def advisor_preplan(query: str, user_id: int, country_override: Optional[str]) -> Optional[dict]:
    """
    Neutral preplan for 'advisor' flow.

    - NO keyword triggers (let your ML router/planner decide).
    - NO implicit time_range (planner or user text must specify).
    - Only resolves country softly so downstream has a hint if needed.
    - Returns None to avoid biasing the plan.
    """
    try:
        # soft country hint only; do not force
        ctry = parse_country(query) or country_override or None
    except Exception:
        ctry = country_override or None

    # Do not set metric/operation/time_range/group_by here.
    # Returning None ensures the main planner creates the plan.
    return None

# --- follow-up vs new helpers -----------------------------------------------
def _explicit_signals(q: str):
    ql = (q or "").lower()
    metric_words = ["sale","sales","revenue","profit","refund","tax","acos","tacos",
                    "ad spend","advertising","adspend","ppc","aov","asp","quantity","units"]
    time_words = ["today","yesterday","week","month","year","q1","q2","q3","q4",
                  "jan","feb","mar","apr","may","jun","jul","aug","sep","oct","nov","dec",
                  "2020","2021","2022","2023","2024","2025","last","current","previous","recent","since","between"]
    country_words = ["uk","us","usa","united kingdom","united states"]
    has_metric = any(w in ql for w in metric_words)
    has_time   = any(w in ql for w in time_words)
    has_ctry   = any(f" {w} " in f" {ql} " for w in country_words)
    has_for    = " for " in ql  # rough product hint
    return has_metric, has_time, has_ctry, has_for

_ANAPHORA = {
    "this","that","those","these","it","same","above","as above","as before","same period",
    "same time","same month","same year","continue","next","and","also","too",
    "what about","how about","instead","vs","versus","compared to","compare to","compared with"
}

def _looks_anaphoric(q: str) -> bool:
    ql = (q or "").lower()
    return any(kw in ql for kw in _ANAPHORA)

def _similarity(a: str, b: str) -> float:
    try:
        if not a or not b or not st_model:
            return 0.0
        va = st_model.encode([a])[0]
        vb = st_model.encode([b])[0]
        import numpy as np
        num = float(np.dot(va, vb))
        den = float(np.linalg.norm(va) * np.linalg.norm(vb)) or 1.0
        return max(0.0, min(1.0, num / den))
    except Exception:
        return 0.0
def decide_followup_or_new(curr_query: str, last_ctx: dict | None, last_user_msg: str | None, now_ts=None):
    """
    Decide whether the current query is a follow-up or a fresh query.

    Updated logic (no hard-coded keywords):
    - If user expresses explicit time → ALWAYS NEW
    - Metric change → follow-up only if anaphoric, else NEW
    - Country mention → NEW unless clearly anaphoric
    - Very short (<=3 tokens) with no signals → FOLLOW-UP
    - Uses adaptive similarity thresholding (semantic-only)
    - Keeps all prior entity/context logic
    """
    q = (curr_query or "").strip()
    if not last_ctx:
        return {"mode": "new", "why": "no_context", "synth": None}

    # --- Context expiry guard ---
    try:
        ts = last_ctx.get("ts")
        if ts and now_ts and (now_ts - ts) > 20 * 60:
            return {"mode": "new", "why": "context_timeout", "synth": None}
    except Exception:
        pass

    # --- Explicit signals ---
    has_metric, has_time, has_ctry, has_for = _explicit_signals(q)
    last_metric = (last_ctx.get("metric") or "").lower().strip()

    # --- Explicit time always overrides context ---
    if has_time:
        return {"mode": "new", "why": "explicit_time_overrides_context", "synth": None}

    # --- Metric change ---
    if has_metric and last_metric and last_metric not in q.lower():
        if _looks_anaphoric(q):
            return {
                "mode": "followup",
                "why": "anaphoric new metric",
                "synth": None,
                "replace_metric": True,
            }
        else:
            return {
                "mode": "new",
                "why": f"explicit_new_metric ({last_metric} → {q})",
                "synth": None,
            }

    # --- Country mention usually implies NEW unless anaphoric ---
    if has_ctry and not _looks_anaphoric(q):
        return {"mode": "new", "why": "explicit_country", "synth": None}

    # --- Token length & similarity ---
    toks = q.split()
    tok_count = len(toks)
    sim = _similarity(q, last_user_msg or "")

    # --- Adaptive similarity logic (semantic-only) ---
    # Shorter utterances → lower threshold
    # Longer ones → require stronger similarity
    if tok_count <= 3:
        sim += 0.10  # encourage short anaphoric follow-ups
        threshold = 0.25
    elif tok_count <= 6:
        threshold = 0.35
    else:
        threshold = 0.45

    # Boost similarity slightly if shared entities exist
    if last_ctx:
        shared_signals = 0
        for k in ("metric", "country", "product", "sku"):
            val = (last_ctx.get(k) or "").strip().lower()
            if val and val in q.lower():
                shared_signals += 1
        if shared_signals:
            sim += 0.05 * min(shared_signals, 2)

    # --- Decision logic ---
    if tok_count <= 3 and not (has_metric or has_time or has_ctry or has_for):
        return _synthesize_followup(last_ctx, reason=f"short_utterance len={tok_count}")

    if _looks_anaphoric(q) or sim >= threshold or (tok_count <= 6 and sim >= (threshold - 0.05)):
        return _synthesize_followup(last_ctx, reason=f"semantic/structural sim={sim:.2f} thr={threshold:.2f}")

    return {"mode": "new", "why": f"low_signals sim={sim:.2f} thr={threshold:.2f} len={tok_count}", "synth": None}






def _synthesize_followup(last_ctx: dict, reason: str):
    """Helper to build a minimal synthetic query from last context."""
    m    = (last_ctx.get("metric") or "").strip()
    ctry = (last_ctx.get("country") or "").strip()
    tr   = last_ctx.get("time_range")
    tgt  = f"SKU {last_ctx['sku']}" if last_ctx.get("sku") else (last_ctx.get("product") or "").strip()

    parts = []
    if m:      parts.append(m)
    if tgt:    parts += ["for", tgt]
    if ctry:   parts += ["in", ctry]
    if tr:
        if isinstance(tr, dict) and tr.get("start") and tr.get("end"):
            parts += ["during", f"{tr['start']} to {tr['end']}"]
        else:
            parts += ["during", str(tr)]

    synth = " ".join(parts).strip() or None
    return {"mode": "followup", "why": reason, "synth": synth}

def _planner_context_suffix(ctx: dict) -> str:
    """
    Build '(context: ...)' for the planner, using ONLY what the last run actually had.
    No guessing or defaults here.
    """
    if not isinstance(ctx, dict):
        return ""

    bits = []
    m = (ctx.get("metric") or "").strip()
    if m: bits.append(f"metric={m}")

    # prefer SKU if present, else product
    if ctx.get("sku"):
        bits.append(f"sku={ctx['sku']}")
    elif (ctx.get("product") or "").strip():
        bits.append(f"product={ctx['product']}")

    c = (ctx.get("country") or "").strip()
    if c: bits.append(f"country={c}")

    tr = ctx.get("time_range")
    if isinstance(tr, dict) and tr.get("start") and tr.get("end"):
        bits.append(f"time={tr['start']}..{tr['end']}")

    return f"(context: {', '.join(bits)})" if bits else ""

def _is_anaphoric_to_product(q: str) -> bool:
    ql = (q or "").lower()
    # reuse only if user actually refers back to the last item
    return bool(re.search(r"\b(same\s+(sku|product)|that\s+(one|sku|product)|it\s+(one|sku|product))\b", ql, re.I))

def _looks_like_trend_followup(q: str) -> bool:
    ql = (q or "").lower()
    return any(w in ql for w in [
        "increasing", "decreasing", "trend", "up or down",
        "going up", "going down", "improving", "worsening",
        "uptrend", "downtrend"
    ])

def _looks_anaphoric_to_time(q: str) -> bool:
    ql = (q or "").lower()
    explicit = ("same period", "same time", "same window", "as above", "as before")
    return any(p in ql for p in explicit) or (len(ql.split()) <= 3)


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
                                    # IMPORTANT: advisor internal fetch → neutral query
                                    query="",
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

            # --- Render formula-mode (PENDING branch) ---
            if mode == "sql_formula":
                table_records = df_to_records_safe(df)
                final_records = _finalize_records(plan, table_records)
                print(f"[DEBUG] wants_advice(pending)={wants_advice(orig_q, plan)}")

                if wants_advice(orig_q, plan):
                    # --------- Build proper df_primary for BusinessAdvisor (PENDING branch) ----------
                    try:
                        # 1) Determine effective country
                        eff_country = (country_override or plan.get("country") or parse_country_strict(orig_q) or "UK").upper()
                        if eff_country not in {"UK", "US"}:
                            store__ = globals().setdefault("LAST_CONTEXT", {})
                            lc__ = store__.get(int(user_id)) or {}
                            eff_country = lc__.get("country") or "UK"

                        # 2) Try to infer a single product from query or last context
                        picked_product = None
                        try:
                            cands = product_candidates(engine, user_id, eff_country, orig_q, limit=10) or []
                        except Exception:
                            cands = []
                        if len(cands) == 1:
                            picked_product = (cands[0].get("product_name") or "").strip()

                        if not picked_product:
                            lc_safe = (globals().setdefault("LAST_CONTEXT", {}).get(int(user_id)) or {})
                            if lc_safe.get("product"):
                                picked_product = lc_safe["product"]
                                eff_country = lc_safe.get("country") or eff_country

                        # 3) Pick a trailing time window for advisor, prefer clamp
                        tr = (
                            clamp_relative_time_to_available(user_id, eff_country, "last 4 months")
                            or clamp_relative_time_to_available(user_id, eff_country, "last 3 months")
                            or plan.get("time_range")
                        )

                        # If clamp couldn't determine a window, build a safe 3-month span
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

                        # 4) Build a TREND plan to feed BusinessAdvisor
                        trend_plan = {
                            "operation": "trend",
                            "metric": plan.get("metric") or "sales",
                            "time_range": tr,
                            "country": eff_country,
                            "group_by": None if picked_product else "product",
                            "sort_dir": "desc",
                            "product": picked_product,
                            "filters": [],
                        }

                        df_trend, mode_trend = engine_q.exec_plan_via_formula(
                            plan=trend_plan,
                            # IMPORTANT: advisor internal fetch → neutral query
                            query="",
                            user_id=str(user_id),
                            country_override=country_override,
                        )

                        # 5) Choose df_primary + scope/target
                        if isinstance(df_trend, pd.DataFrame) and not df_trend.empty:
                            df_primary = df_trend.copy()
                            scope = "product" if picked_product else "portfolio"
                            target = picked_product if picked_product else None
                            time_range_for_advisor = tr
                        else:
                            # Fallback: use current df
                            df_primary = df.copy() if isinstance(df, pd.DataFrame) else pd.DataFrame()
                            scope = (
                                "sku"
                                if plan.get("force_product_only")
                                and isinstance(plan.get("product"), str)
                                and plan["product"].upper() == plan["product"]
                                else "product"
                                if plan.get("product")
                                else "portfolio"
                            )
                            target = plan.get("product")
                            time_range_for_advisor = plan.get("time_range")

                        # 6) Call BusinessAdvisor
                        advice_lines = BusinessAdvisor.recommend(
                            orig_q,
                            df_primary,
                            aux={
                                "country": eff_country,
                                "time_range": time_range_for_advisor,
                                "scope": scope,
                                "target": target,
                            },
                        )
                        reply = "\n".join(advice_lines) if advice_lines else \
                                "I couldn’t derive targeted growth actions from the latest data."
                        used_mode = "advisor"

                    except Exception as e:
                        # If anything goes wrong, fall back to plain sql_formula narrative
                        print("[DEBUG][advisor-pending] failed, falling back to sql_formula narrative:", e)
                        reply = generate_openai_answer(
                            user_query=orig_q,
                            mode="sql_formula",
                            analysis=None,
                            table_records=final_records,
                        )
                        used_mode = "sql_formula"

                else:
                    # Non-advisor path: descriptive table narrative
                    reply = generate_openai_answer(
                        user_query=orig_q,
                        mode="sql_formula",
                        analysis=None,
                        table_records=final_records,
                    )
                    used_mode = mode

                # Stash context & follow-up memory (unchanged)
                try:
                    _stash_context(
                        user_id,
                        plan,
                        country_override,
                        table_records=final_records,
                        user_msg=orig_q,
                    )
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
                    plan=plan2,
                    # IMPORTANT: advisor internal fetch → neutral query
                    query="",
                    user_id=str(user_id),
                    country_override=country_override
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
                    plan=plan2,
                    # IMPORTANT: advisor internal fetch → neutral query
                    query="",
                    user_id=str(user_id),
                    country_override=country_override
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

            # --- NEW: strip alias-based "product" so it doesn't count as explicit product intent ---
            alias_match = re.search(r"\(alias:\s*([^)]+)\)", str(query or ""), re.I)
            alias_name = alias_match.group(1).strip() if alias_match else None

            raw_plan_product = (plan.get("product") or "").strip()
            if alias_name and raw_plan_product and raw_plan_product.lower() == alias_name.lower():
                # e.g. "(alias: skin elements)" – this is the business alias, not a specific product
                print(f"[DEBUG] product came only from alias '{alias_name}' → clearing plan['product']")
                plan["product"] = None
                raw_plan_product = ""

            wants_split = plan.get("group_by") in {"product", "sku"} or plan.get("operation") == "rank"
            # explicit_product now means: a real product phrase (guessed) OR a non-alias product set upstream
            explicit_product = bool(raw_plan_product or guessed)

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
                        # IMPORTANT CHANGE:
                        # If user did not clearly ask about a specific product, DO NOT auto-pick.
                        if not explicit_product:
                            print("[DEBUG] multiple candidates but no explicit product intent → proceeding without product filter")
                            plan.pop("product", None)
                            plan["force_product_only"] = False
                        else:
                            # User explicitly asked product-wise; safe to auto-pick a candidate.
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
                                # IMPORTANT: advisor internal fetch → neutral query
                                query="",
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
            print(f"[DEBUG] wants_advice(pending)={wants_advice(orig_q, plan)}")

            if wants_advice(orig_q, plan):
                # --------- Build proper df_primary for BusinessAdvisor (main branch) ----------
                try:
                    eff_country = (country_override or plan.get("country") or parse_country_strict(orig_q) or "UK").upper()
                    if eff_country not in {"UK", "US"}:
                        store__ = globals().setdefault("LAST_CONTEXT", {})
                        lc__ = store__.get(int(user_id)) or {}
                        eff_country = lc__.get("country") or "UK"

                    picked_product = None
                    try:
                        cands = product_candidates(engine, user_id, eff_country, orig_q, limit=10) or []
                    except Exception:
                        cands = []
                    if len(cands) == 1:
                        picked_product = (cands[0].get("product_name") or "").strip()

                    if not picked_product:
                        lc_safe = (globals().setdefault("LAST_CONTEXT", {}).get(int(user_id)) or {})
                        if lc_safe.get("product"):
                            picked_product = lc_safe["product"]
                            eff_country = lc_safe.get("country") or eff_country

                    tr = (
                        clamp_relative_time_to_available(user_id, eff_country, "last 4 months")
                        or clamp_relative_time_to_available(user_id, eff_country, "last 3 months")
                        or plan.get("time_range")
                    )

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

                    trend_plan = {
                        "operation": "trend",
                        "metric": plan.get("metric") or "sales",
                        "time_range": tr,
                        "country": eff_country,
                        "group_by": None if picked_product else "product",
                        "sort_dir": "desc",
                        "product": picked_product,
                        "filters": [],
                    }

                    df_trend, mode_trend = engine_q.exec_plan_via_formula(
                        plan=trend_plan,
                        # IMPORTANT: advisor internal fetch → neutral query
                        query="",
                        user_id=str(user_id),
                        country_override=country_override,
                    )

                    if isinstance(df_trend, pd.DataFrame) and not df_trend.empty:
                        df_primary = df_trend.copy()
                        scope = "product" if picked_product else "portfolio"
                        target = picked_product if picked_product else None
                        time_range_for_advisor = tr
                    else:
                        df_primary = df.copy() if isinstance(df, pd.DataFrame) else pd.DataFrame()
                        scope = (
                            "sku"
                            if plan.get("force_product_only")
                            and isinstance(plan.get("product"), str)
                            and plan["product"].upper() == plan["product"]
                            else "product"
                            if plan.get("product")
                            else "portfolio"
                        )
                        target = plan.get("product")
                        time_range_for_advisor = plan.get("time_range")

                    advice_lines = BusinessAdvisor.recommend(
                        orig_q,
                        df_primary,
                        aux={
                            "country": eff_country,
                            "time_range": time_range_for_advisor,
                            "scope": scope,
                            "target": target,
                        },
                    )
                    reply = "\n".join(advice_lines) if advice_lines else \
                            "I couldn’t derive targeted growth actions from the latest data."
                    used_mode = "advisor"

                except Exception as e:
                    print("[DEBUG][advisor-pending] failed, falling back to sql_formula narrative:", e)
                    reply = generate_openai_answer(
                        user_query=orig_q,
                        mode="sql_formula",
                        analysis=None,
                        table_records=final_records,
                    )
                    used_mode = "sql_formula"

            else:
                reply = generate_openai_answer(
                    user_query=orig_q,
                    mode="sql_formula",
                    analysis=None,
                    table_records=final_records,
                )
                used_mode = mode

            try:
                _stash_context(user_id, plan, country_override, table_records=final_records, user_msg=orig_q)
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








@chatbot_bp.route("/chatbot/history", methods=["GET", "DELETE", "OPTIONS"])
def chatbot_history():
    if request.method == "OPTIONS":
        return ("", 200)

    user_id, err = _decode_jwt_or_401(request.headers.get("Authorization"))
    if err:
        return jsonify({"error": err}), 401

    if request.method == "DELETE":
        # Require explicit confirmation (?all=true) so logout or stray calls can't wipe DB
        if request.args.get("all") != "true":
            return jsonify({"success": False, "message": "Missing confirmation"}), 400
        try:
            ChatHistory.query.filter_by(user_id=user_id).delete(synchronize_session=False)
            db.session.commit()
            return jsonify({"success": True})
        except Exception:
            db.session.rollback()
            logger.exception("Failed to delete chat history")
            return jsonify({"success": False, "message": "Could not clear history"}), 500


    try:
        limit = max(1, min(int(request.args.get("limit", 200)), 500))
        offset = max(0, int(request.args.get("offset", 0)))

        q = ChatHistory.query.filter_by(user_id=user_id).order_by(ChatHistory.timestamp.asc())
        rows = q.offset(offset).limit(limit).all()

        items = []
        for r in rows:
            ts_iso = (r.timestamp or dt.datetime.utcnow()).isoformat() + "Z"
            items.append({"id": f"{r.id}-u", "sender": "user", "text": r.message, "timestamp": ts_iso})
            items.append({"id": f"{r.id}-b", "sender": "bot", "text": r.response, "timestamp": ts_iso})
        return jsonify({"success": True, "items": items})
    except Exception:
        logger.exception("History fetch error")
        return jsonify({"success": False, "message": "Could not load history"}), 500


@chatbot_bp.route("/chatbot/feedback", methods=["POST", "OPTIONS"])
def chatbot_feedback():
    if request.method == "OPTIONS":
        return ("", 200)

    user_id, err = _decode_jwt_or_401(request.headers.get("Authorization"))
    if err:
        return jsonify({"error": err}), 401

    body = request.get_json(silent=True) or {}
    kind = (body.get("kind") or "").lower()               # "like" | "dislike"
    hist_id = body.get("message_id")
    message = (body.get("message") or "").strip()         # original user prompt
    response = (body.get("response") or "").strip()       # bot reply text

    # NEW (learning loop): optional synonym teaching
    term = (body.get("term") or "").strip()               # e.g., "refund leakage"
    alias_of = (body.get("alias_of") or "").strip()       # e.g., "credits"  (your canonical metric key)

    if kind not in {"like", "dislike"}:
        return jsonify({"success": False, "message": "Invalid kind"}), 400

    try:
        # Case A: update existing row (primary case)
        if hist_id:
            row = ChatHistory.query.filter_by(id=int(hist_id), user_id=user_id).first()
            if not row:
                return jsonify({"success": False, "message": "History row not found"}), 404

            if kind == "like":
                row.like_response = row.response
                row.dislike_response = None
            else:
                row.dislike_response = row.response
                row.like_response = None

            db.session.commit()

            # NEW: persist learned alias if provided
            learned = None
            if term and alias_of:
                try:
                    learn_metric_alias(term, alias_of)
                    learned = {term: alias_of}
                    logger.info("Learned alias (existing row): %r -> %r", term, alias_of)
                except Exception:
                    logger.exception("Could not learn alias (existing row)")

            return jsonify({"success": True, "message_id": row.id, "learned": learned})

        # Case B: fallback for old messages without database ID
        if not message or not response:
            return jsonify({
                "success": False,
                "message": "message and response are required when message_id is missing"
            }), 400

        # Create new record with feedback
        rec = ChatHistory(
            user_id=user_id,
            message=message[:1000],
            response=response[:2000],
            timestamp=dt.datetime.utcnow(),
        )
        if kind == "like":
            rec.like_response = response[:2000]
        else:
            rec.dislike_response = response[:2000]

        db.session.add(rec)
        db.session.commit()

        # NEW: persist learned alias if provided
        learned = None
        if term and alias_of:
            try:
                learn_metric_alias(term, alias_of)
                learned = {term: alias_of}
                logger.info("Learned alias (new row): %r -> %r", term, alias_of)
            except Exception:
                logger.exception("Could not learn alias (new row)")

        return jsonify({"success": True, "message_id": rec.id, "learned": learned})

    except Exception:
        db.session.rollback()
        logger.exception("feedback error")
        return jsonify({"success": False, "message": "Could not save feedback"}), 500

