import io
from datetime import datetime , date
import calendar, json, hashlib
import re
from sqlalchemy import func, text
from sqlalchemy.dialects.postgresql import insert
import jwt, time
import pandas as pd
from flask import Blueprint, jsonify, request, send_file, Response
from app import db
from config import Config
from app.utils.token_utils import get_effective_user_id_from_token
from app.models.user_models import amazon_user, amazon_sponsored_products , amazon_sponsored_display_advertised_products
from app.utils.amazon_ads_utils_reporting import (
    build_ads_lwa_auth_url,
    exchange_code_for_tokens,
    get_ads_access_token_from_refresh,
    list_top_level_profiles_all_regions,
    find_manager_profile_id,
    list_child_profiles_all_regions,
    pick_profile_id,
    ADS_ENDPOINTS,
    tokeninfo,
    AmazonAdsAuthContext,
    AmazonAdsReportingClient,
)
from app.models.user_models import amazon_sponsored_brands_keywords
from openpyxl.utils import get_column_letter

def _get_user_row(user_id: int) -> amazon_user:
    u = amazon_user.query.filter_by(user_id=user_id).first()
    if not u:
        raise RuntimeError("User not found")
    return u


def _norm_key(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(s).lower())

def _pick(df: pd.DataFrame, *candidates, default=None):
    """
    Find a column in df by trying many candidate names,
    matching loosely (case/space/symbol insensitive).
    """
    if df is None or df.empty:
        return default

    norm_map = {_norm_key(c): c for c in df.columns}
    for cand in candidates:
        k = _norm_key(cand)
        if k in norm_map:
            return df[norm_map[k]]
    return default


def _to_date(x, strict: bool = False, field_name: str = "date"):
    """
    Converts x -> datetime.date or None.

    strict=False (default): old behavior (returns None for invalid/blank)
    strict=True: raises ValueError on invalid input (use for request validation)
    """
    if x is None:
        if strict:
            raise ValueError(f"{field_name} is required")
        return None

    # handle pandas NaN/NaT safely
    try:
        if pd.isna(x):
            if strict:
                raise ValueError(f"{field_name} is required")
            return None
    except Exception:
        pass

    if isinstance(x, str):
        s = x.strip()
        if s == "":
            if strict:
                raise ValueError(f"{field_name} is required")
            return None
        x = s

    try:
        dt = pd.to_datetime(x, errors="raise")
        return dt.date()
    except Exception:
        if strict:
            raise ValueError(f"Invalid {field_name}. Use YYYY-MM-DD")
        return None


def _to_float(x):
    if pd.isna(x) or x is None or x == "":
        return None
    try:
        return float(x)
    except Exception:
        return None


def _to_int(x):
    if pd.isna(x) or x is None or x == "":
        return None
    try:
        return int(float(x))
    except Exception:
        return None


def _safe_div(a, b):
    a = float(a or 0.0)
    b = float(b or 0.0)
    return 0.0 if b == 0.0 else (a / b)
