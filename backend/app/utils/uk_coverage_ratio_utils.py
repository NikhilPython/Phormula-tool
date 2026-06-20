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


def fetch_last_30_days_units(
    user_id: int,
    country: str,
    as_of: date = None,
    marketplace_name: str = None
) -> pd.DataFrame:
    country = str(country).strip().lower()

    if as_of is None:
        as_of = date.today()

    # Example: as_of = 2026-05-06
    # Window = 2026-04-06 to 2026-05-05
    end_date = as_of - timedelta(days=1)
    start_date = end_date - timedelta(days=29)

    curr_month_start = date(end_date.year, end_date.month, 1)

    curr_start = max(start_date, curr_month_start)
    curr_end = end_date

    prev_start = start_date
    prev_end = curr_month_start - timedelta(days=1)

    frames = []

    if curr_start <= curr_end:
        q_live = text("""
            SELECT sku, quantity
            FROM liveorders
            WHERE user_id = :user_id
              AND marketplace = :marketplace_name
              AND purchase_date >= :start
              AND purchase_date < :end
              AND sku IS NOT NULL
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

        if not df_live.empty:
            df_live = _clean_inventory_sku(df_live)
            frames.append(df_live)

    if prev_start <= prev_end:
        with engine_hist.connect() as conn:
            if country == "global":
                table = f"user_{user_id}_total_country_global_data"
            else:
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
                      AND sku IS NOT NULL
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

                if not df_prev.empty:
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


# def compute_inventory_coverage_ratio(user_id: int, country: str) -> pd.DataFrame:
#     inv_df = fetch_available_inventory(user_id)
#     sales_df = fetch_last_30_days_units(user_id, country)

#     df = inv_df.merge(sales_df, on="sku", how="left")
#     df["last_30_days_units"] = df["last_30_days_units"].fillna(0.0)

#     df["inventory_coverage_ratio"] = df.apply(
#         lambda r: round(r["available"] / r["last_30_days_units"], 2)
#         if r["last_30_days_units"] > 0 else None,
#         axis=1,
#     )

#     try:
#         sku_map_df = fetch_sku_product_mapping(user_id)

#         if not sku_map_df.empty:
#             sku_map_df = sku_map_df[["sku", "product_name"]].drop_duplicates("sku")
#             df = df.merge(
#                 sku_map_df,
#                 on="sku",
#                 how="left"
#             )
#         else:
#             df["product_name"] = None

#     except Exception:
#         df["product_name"] = None

#     df = df[
#         ["sku", "product_name", "available", "last_30_days_units", "inventory_coverage_ratio"]
#     ]

#     return df

def compute_inventory_coverage_ratio(user_id: int, country: str) -> pd.DataFrame:
    inv_df = fetch_available_inventory(user_id)
    sales_df = fetch_last_30_days_units(user_id, country)

    if inv_df is None or inv_df.empty:
        return pd.DataFrame(
            columns=[
                "sku",
                "product_name",
                "available",
                "last_30_days_units",
                "inventory_coverage_ratio",
            ]
        )

    inv_df = inv_df.copy()
    sales_df = sales_df.copy() if sales_df is not None else pd.DataFrame(columns=["sku", "last_30_days_units"])

    # ✅ Normalize SKU on both sides before merge
    inv_df["sku"] = inv_df["sku"].astype(str).str.strip().str.upper()
    sales_df["sku"] = sales_df["sku"].astype(str).str.strip().str.upper()

    # ✅ Clean invalid SKU values
    inv_df = inv_df[
        ~inv_df["sku"].str.lower().isin(["", "nan", "none", "null", "0"])
    ]

    sales_df = sales_df[
        ~sales_df["sku"].str.lower().isin(["", "nan", "none", "null", "0"])
    ]

    # ✅ Force numeric
    inv_df["available"] = pd.to_numeric(inv_df.get("available", 0), errors="coerce").fillna(0.0)

    if "last_30_days_units" not in sales_df.columns:
        sales_df["last_30_days_units"] = 0.0

    sales_df["last_30_days_units"] = pd.to_numeric(
        sales_df["last_30_days_units"],
        errors="coerce",
    ).fillna(0.0)

    # ✅ If sales_df has duplicate SKU rows, aggregate before merge
    sales_df = (
        sales_df.groupby("sku", as_index=False)["last_30_days_units"]
        .sum()
    )

    df = inv_df.merge(sales_df, on="sku", how="left")
    df["last_30_days_units"] = pd.to_numeric(
        df["last_30_days_units"],
        errors="coerce",
    ).fillna(0.0)

    df["inventory_coverage_ratio"] = df.apply(
        lambda r: round(float(r["available"]) / float(r["last_30_days_units"]), 2)
        if float(r["last_30_days_units"] or 0) > 0 else None,
        axis=1,
    )

    try:
        sku_map_df = fetch_sku_product_mapping(user_id)

        if sku_map_df is not None and not sku_map_df.empty:
            sku_map_df = sku_map_df[["sku", "product_name"]].drop_duplicates("sku")
            sku_map_df["sku"] = sku_map_df["sku"].astype(str).str.strip().str.upper()

            df = df.merge(
                sku_map_df,
                on="sku",
                how="left",
            )
        else:
            df["product_name"] = None

    except Exception:
        df["product_name"] = None

    df = df[
        [
            "sku",
            "product_name",
            "available",
            "last_30_days_units",
            "inventory_coverage_ratio",
        ]
    ]

    return df    