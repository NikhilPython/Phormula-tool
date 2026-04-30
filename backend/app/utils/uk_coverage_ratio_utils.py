import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from config import Config
from calendar import month_name, monthrange
from datetime import date, datetime, timedelta
import pandas as pd
import numpy as np
from openai import OpenAI
import json
from app.utils.formulas_utils import safe_num

load_dotenv()
db_url2 = os.getenv("DATABASE_AMAZON_URL")
db_url = os.getenv("DATABASE_URL")
engine_live = create_engine(db_url2)
engine_hist = create_engine(db_url)


# ---- Inventory coverage ratio calculation -----
def _clean_inventory_sku(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["sku"] = df["sku"].astype(str).str.strip()
    df.loc[df["sku"].str.lower().isin(["", "none", "nan", "null", "0"]), "sku"] = None
    return df.dropna(subset=["sku"])


def month_num_to_name(m):
    try:
        m_int = int(m)
        return month_name[m_int].lower() if 1 <= m_int <= 12 else None
    except Exception:
        return None


def construct_prev_table_name(user_id, country, month, year):
    country = str(country).strip().lower()

    # ✅ global uses consolidated table
    if country == "global":
        return f"user_{user_id}_total_country_global_data"

    month_str = month_num_to_name(month)
    if not month_str:
        raise ValueError("Invalid month")

    return f"user_{user_id}_{country}_{month_str}{year}_data"


def table_exists(conn, table_name: str) -> bool:
    q = text("""
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = 'public'
              AND table_name = :table
        ) AS ok
    """)
    return bool(conn.execute(q, {"table": table_name}).scalar())


def fetch_last_30_days_units(user_id: int, country: str, as_of: date = None, marketplace_name: str = None) -> pd.DataFrame:
    country = str(country).strip().lower()

    if as_of is None:
        as_of = date.today()

    yesterday = as_of - timedelta(days=1)
    start_30d = yesterday - timedelta(days=29)

    curr_month_start = date(yesterday.year, yesterday.month, 1)

    curr_start = max(start_30d, curr_month_start)
    curr_end = yesterday

    prev_start = start_30d
    prev_end = curr_month_start - timedelta(days=1)

    frames = []

    # -------------------------
    # CURRENT MONTH → liveorders
    # -------------------------
    if curr_start <= curr_end:
        q_live = text("""
            SELECT sku, quantity
            FROM liveorders
            WHERE user_id = :user_id
              AND marketplace = :marketplace_name
              AND purchase_date >= :start
              AND purchase_date < :end
        """)

        with engine_live.connect() as conn:
            df_live = pd.read_sql(
                q_live,
                conn,
                params={
                    "user_id": user_id,
                    "marketplace_name": marketplace_name,
                    "start": datetime.combine(curr_start, datetime.min.time()),
                    "end": datetime.combine(curr_end + timedelta(days=1), datetime.min.time()),
                },
            )

        df_live = _clean_inventory_sku(df_live)
        frames.append(df_live)

    # -------------------------
    # PREVIOUS PERIOD → historic source
    # -------------------------
    if prev_start <= prev_end:
        with engine_hist.connect() as conn:
            if country == "global":
                # ✅ use consolidated global table
                table = f"user_{user_id}_total_country_global_data"

                if table_exists(conn, table):
                    q_prev = text(f"""
                        SELECT sku, quantity
                        FROM public.{table}
                        WHERE marketplace = :marketplace_name
                        AND NULLIF(NULLIF(date_time, '0'), '')::timestamp >= :start
                        AND NULLIF(NULLIF(date_time, '0'), '')::timestamp < :end
                    """)

                    df_prev = pd.read_sql(
                        q_prev,
                        conn,
                        params={
                            "marketplace_name": marketplace_name,
                            "start": datetime.combine(prev_start, datetime.min.time()),
                            "end": datetime.combine(prev_end + timedelta(days=1), datetime.min.time()),
                        },
                    )

                    df_prev = _clean_inventory_sku(df_prev)
                    frames.append(df_prev)

            else:
                # ✅ normal country monthly historic table
                table = construct_prev_table_name(
                    user_id=user_id,
                    country=country,
                    month=prev_start.month,
                    year=prev_start.year,
                )

                if table_exists(conn, table):
                    q_prev = text(f"""
                        SELECT sku, quantity
                        FROM public.{table}
                        WHERE marketplace = :marketplace_name
                        AND NULLIF(NULLIF(date_time, '0'), '')::timestamp >= :start
                        AND NULLIF(NULLIF(date_time, '0'), '')::timestamp < :end
                    """)

                    df_prev = pd.read_sql(
                        q_prev,
                        conn,
                        params={
                            "marketplace_name": marketplace_name,
                            "start": datetime.combine(prev_start, datetime.min.time()),
                            "end": datetime.combine(prev_end + timedelta(days=1), datetime.min.time()),
                        },
                    )

                    df_prev = _clean_inventory_sku(df_prev)
                    frames.append(df_prev)

    if not frames:
        return pd.DataFrame(columns=["sku", "last_30_days_units"])

    df_all = pd.concat(frames, ignore_index=True)
    df_all["quantity"] = safe_num(df_all["quantity"])

    return (
        df_all.groupby("sku", as_index=False)["quantity"]
        .sum()
        .rename(columns={"quantity": "last_30_days_units"})
    )


def fetch_available_inventory(user_id: int) -> pd.DataFrame:
    q = text("""
        SELECT sku, available
        FROM public.inventory_aged
        WHERE user_id = :user_id
    """)

    with engine_live.connect() as conn:
        df = pd.read_sql(q, conn, params={"user_id": user_id})

    df = _clean_inventory_sku(df)
    df["available"] = safe_num(df["available"])

    return (
        df.groupby("sku", as_index=False)["available"]
        .sum()
    )


def fetch_sku_product_mapping(user_id: int) -> pd.DataFrame:
    """
    sku_{user_id}_data_table se sku_uk -> product_name mapping uthata hai.
    Sirf VALID mappings return karega:
      - sku_uk present ho
      - product_name present ho (non-null, non-empty)
    """
    table_name = f"sku_{user_id}_data_table"

    query = text(f"""
        SELECT
            sku_uk,
            product_name
        FROM {table_name}
    """)

    with engine_hist.connect() as conn:
        df = pd.read_sql(query, conn)

    if df.empty:
        return df

    df = df.dropna(subset=["sku_uk"])
    df = df.dropna(subset=["product_name"])
    df["product_name"] = df["product_name"].astype(str)
    df = df[df["product_name"].str.strip() != ""]
    df = df.rename(columns={"sku_uk": "sku"})
    df["sku"] = df["sku"].astype(str).str.strip()
    df = df.drop_duplicates(subset=["sku"])

    return df


def compute_inventory_coverage_ratio(user_id: int, country: str) -> pd.DataFrame:
    inv_df = fetch_available_inventory(user_id)
    sales_df = fetch_last_30_days_units(user_id, country)

    df = inv_df.merge(sales_df, on="sku", how="left")
    df["last_30_days_units"] = df["last_30_days_units"].fillna(0.0)

    df["inventory_coverage_ratio"] = df.apply(
        lambda r: round(r["available"] / r["last_30_days_units"], 2)
        if r["last_30_days_units"] > 0 else None,
        axis=1,
    )

    try:
        sku_map_df = fetch_sku_product_mapping(user_id)

        if not sku_map_df.empty:
            sku_map_df = sku_map_df[["sku", "product_name"]].drop_duplicates("sku")
            df = df.merge(
                sku_map_df,
                on="sku",
                how="left"
            )
        else:
            df["product_name"] = None

    except Exception:
        df["product_name"] = None

    df = df[
        ["sku", "product_name", "available", "last_30_days_units", "inventory_coverage_ratio"]
    ]

    return df

    