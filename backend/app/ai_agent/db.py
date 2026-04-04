from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Optional, Tuple

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from config import Config

_TABLE_RE = re.compile(r"^user_(\d+)_(uk|us)_merge_data_of_all_months$")
_COUNTRY_RE = re.compile(r"^[a-z]{2}$")


@dataclass(frozen=True)
class CompletedMonth:
    year: int
    month: int


def get_engine() -> Engine:
    return create_engine(Config.SQLALCHEMY_DATABASE_URI, pool_pre_ping=True)


def resolve_table_name(user_id: int, country: str) -> str:
    if not isinstance(user_id, int) or user_id <= 0:
        raise ValueError("user_id must be a positive integer")
    country = (country or "").strip().lower()
    if not _COUNTRY_RE.match(country):
        raise ValueError("invalid country")
    table_name = f"user_{user_id}_{country}_merge_data_of_all_months"
    if not _TABLE_RE.match(table_name):
        raise ValueError("invalid table name")
    return table_name


def parse_date_series(df: pd.DataFrame) -> pd.Series:
    if "date_time" not in df.columns:
        raise ValueError("date_time column not found")
    return pd.to_datetime(df["date_time"], utc=True, errors="coerce")


def get_latest_completed_month(engine: Engine, table_name: str) -> CompletedMonth:
    query = text(f'SELECT MAX(date_time) AS max_date FROM "{table_name}"')
    with engine.connect() as conn:
        row = conn.execute(query).mappings().first()
    raw_value = row["max_date"] if row else None
    if not raw_value:
        raise ValueError(f"No data found in table {table_name}")
    dt = pd.to_datetime(raw_value, utc=True, errors="coerce")
    if pd.isna(dt):
        raise ValueError("Could not parse latest completed month")
    return CompletedMonth(year=int(dt.year), month=int(dt.month))


def previous_month(year: int, month: int) -> Tuple[int, int]:
    if month == 1:
        return year - 1, 12
    return year, month - 1


def build_month_where(year: int, month: int) -> str:
    return "year = :year AND lower(month) = :month"


def fetch_month_df(engine: Engine, table_name: str, year: int, month: int) -> pd.DataFrame:
    month_name = datetime(year, month, 1).strftime("%B").lower()
    query = text(f'SELECT * FROM "{table_name}" WHERE {build_month_where(year, month)}')
    with engine.connect() as conn:
        df = pd.read_sql_query(query, conn, params={"year": str(year), "month": month_name})
    if not df.empty:
        df["parsed_date_time"] = parse_date_series(df)
    return df


def fetch_range_df(
    engine: Engine,
    table_name: str,
    start_iso: Optional[str] = None,
    end_iso: Optional[str] = None,
    limit: int = 200000,
) -> pd.DataFrame:
    clauses = []
    params: Dict[str, Any] = {"limit": int(limit)}
    if start_iso:
        clauses.append("date_time >= :start_iso")
        params["start_iso"] = start_iso
    if end_iso:
        clauses.append("date_time < :end_iso")
        params["end_iso"] = end_iso
    where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    query = text(f'SELECT * FROM "{table_name}" {where_sql} ORDER BY date_time ASC LIMIT :limit')
    with engine.connect() as conn:
        df = pd.read_sql_query(query, conn, params=params)
    if not df.empty:
        df["parsed_date_time"] = parse_date_series(df)
    return df


def fetch_between_dates_df(
    engine: Engine,
    table_name: str,
    start_date: str,
    end_date: str,
    limit: int = 200000,
) -> pd.DataFrame:
    # inclusive user end-date ko exclusive next-day boundary me turn karo
    end_ts = pd.to_datetime(end_date, utc=True) + pd.Timedelta(days=1)
    return fetch_range_df(
        engine=engine,
        table_name=table_name,
        start_iso=f"{start_date}T00:00:00+00:00",
        end_iso=end_ts.isoformat(),
        limit=limit,
    )