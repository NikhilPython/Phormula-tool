from sqlalchemy import create_engine, text
import os, re
import pandas as pd
import numpy as np
from dotenv import load_dotenv
from app.utils.formulas_utils import (
    us_sales,
    us_tax,
    us_credits,
    us_gross_sales,
    us_tax_and_credits,
    us_cogs,
    us_amazon_fee,
    us_platform_fee,
    us_advertising,
    us_profit,
)

load_dotenv()

db_url = os.getenv('DATABASE_URL')
db_url1 = os.getenv('DATABASE_ADMIN_URL')

MONTHS_REVERSE_MAP = {
    1: "january", 2: "february", 3: "march", 4: "april", 5: "may", 6: "june",
    7: "july", 8: "august", 9: "september", 10: "october", 11: "november", 12: "december"
}

MONTHS_MAP = {
    'january': 1, 'february': 2, 'march': 3, 'april': 4,
    'may': 5, 'june': 6, 'july': 7, 'august': 8,
    'september': 9, 'october': 10, 'november': 11, 'december': 12
}


def get_previous_month_year(month, year):
    year = int(year)
    prev_month_num = MONTHS_MAP[month] - 1
    if prev_month_num == 0:
        prev_month_num = 12
        year -= 1
    prev_month = MONTHS_REVERSE_MAP[prev_month_num]
    return prev_month, year

_TABLE_COL_CACHE = {}


# July-compatible report columns. These columns are added to monthly,
# quarterly and yearly SKU-wise outputs while preserving the legacy columns
# used by the existing dashboard/routes.
REPORT_COMPAT_COLUMNS = [
    "product_sales", "product_sales_tax", "postage_credits",
    "gift_wrap_credits", "shipping_credits_tax", "giftwrap_credits_tax",
    "promotional_rebates_tax", "marketplace_facilitator_tax", "cogs",
    "marketplace_fees", "credits", "tax", "tax_and_credits", "other",
    "ads_spend", "ads_impressions", "ads_clicks", "ads_spend_raw",
    "ads_sale_units", "ads_sale_amount", "sp_ads_sales", "sd_ads_sales",
    "sb_ads_sales", "product_spend", "display_spend", "brand_spend",
    "ad_type", "cm1_profit_per_unit", "cm1_profit_per",
    "cm2_profit_per_unit", "cm2_profit_per", "generated_at_utc",
    "amazon_fees", "advertising_fees", "current_net_reimbursement",
    "total_ads", "total_cm2_profit", "total_cm2_margins",
    "tacos_total_advertising_cost_of_sale",
    "reimbursement_vs_cm2_margins", "ads_conversion_rate", "ads_roas",
    "ads_acos",
]

REPORT_TEXT_COLUMNS = {"ad_type", "generated_at_utc"}


def _numeric_series(df_: pd.DataFrame, col: str) -> pd.Series:
    if col in df_.columns:
        return pd.to_numeric(df_[col], errors="coerce").fillna(0.0)
    return pd.Series(0.0, index=df_.index)


def _numeric_value(value) -> float:
    return float(pd.to_numeric(pd.Series([value]), errors="coerce").fillna(0.0).iloc[0])


def _other_transaction_fees_series(
    df_: pd.DataFrame,
    net_taxes_col: str,
    net_credits_col: str,
    misc_transaction_col: str = "misc_transaction",
) -> pd.Series:
    return (
        _numeric_series(df_, net_credits_col)
        + _numeric_series(df_, misc_transaction_col).abs()
        - _numeric_series(df_, net_taxes_col).abs()
    ).abs()


def _other_transaction_fees_value(net_taxes, net_credits, misc_transaction) -> float:
    return abs(
        _numeric_value(net_credits)
        + abs(_numeric_value(misc_transaction))
        - abs(_numeric_value(net_taxes))
    )


US_POSITIVE_DISPLAY_COLUMNS = [
    "promotional_rebates",
    "promotional_rebates_percentage",
    "selling_fees",
    "refund_selling_fees",
    "fba_fees",
    "net_taxes",
    "misc_transaction",
    "placement_fee",
    "customs_fee",
    "inventory_charges_and_reimbursement",
    "fba_disposal",
]


def _make_us_display_expenses_positive(df_: pd.DataFrame) -> pd.DataFrame:
    df_ = df_.copy()
    for col in US_POSITIVE_DISPLAY_COLUMNS:
        if col in df_.columns:
            df_[col] = pd.to_numeric(df_[col], errors="coerce").fillna(0.0).abs()
    return df_


def add_report_compat_columns(df_: pd.DataFrame) -> pd.DataFrame:
    """Add the July report schema to any monthly/quarterly/yearly dataframe."""
    df_ = df_.copy()

    def num(name, default=0.0):
        if name in df_.columns:
            value = df_[name]
            if isinstance(value, pd.DataFrame):
                value = value.bfill(axis=1).iloc[:, 0]
            return pd.to_numeric(value, errors="coerce").fillna(default)
        return pd.Series(default, index=df_.index, dtype="float64")

    def first_existing(*names):
        for name in names:
            if name in df_.columns:
                return num(name)
        return pd.Series(0.0, index=df_.index, dtype="float64")

    # New names backed by the existing, already-validated report metrics.
    df_["cogs"] = first_existing("cogs", "cost_of_unit_sold")
    df_["marketplace_fees"] = first_existing("marketplace_fees", "amazon_fee", "amazon_fees")
    df_["amazon_fees"] = first_existing("amazon_fees", "amazon_fee", "marketplace_fees")
    df_["credits"] = first_existing("credits", "net_credits")
    df_["tax"] = first_existing("tax", "net_taxes")
    df_["tax_and_credits"] = first_existing("tax_and_credits", "tex_and_credits")
    df_["ads_spend"] = first_existing("ads_spend", "advertising_total", "advertising_fees")
    df_["advertising_fees"] = first_existing("advertising_fees", "advertising_total", "ads_spend")
    df_["current_net_reimbursement"] = first_existing(
        "current_net_reimbursement", "rembursement_fee", "net_reimbursement"
    )
    df_["total_ads"] = first_existing("total_ads", "advertising_total", "ads_spend")
    df_["total_cm2_profit"] = first_existing("total_cm2_profit", "cm2_profit")
    df_["total_cm2_margins"] = first_existing(
        "total_cm2_margins", "cm2_margins", "cm2_profit_percentage"
    )
    df_["reimbursement_vs_cm2_margins"] = first_existing(
        "reimbursement_vs_cm2_margins", "rembursment_vs_cm2_margins"
    )
    df_["ads_acos"] = first_existing("ads_acos", "acos")

    quantity = first_existing("total_quantity", "quantity")
    net_sales = first_existing("net_sales", "Net Sales")
    profit = first_existing("profit")
    cm2_profit = first_existing("cm2_profit")
    ads_spend = first_existing("ads_spend", "advertising_total")
    ads_spend_raw = first_existing("ads_spend_raw", "ads_spend", "advertising_total")
    ads_clicks = first_existing("ads_clicks")
    ads_sale_units = first_existing("ads_sale_units")
    ads_sale_amount = first_existing("ads_sale_amount")

    df_["cm1_profit_per_unit"] = np.where(quantity != 0, profit / quantity, 0.0)
    df_["cm1_profit_per"] = np.where(net_sales != 0, (profit / net_sales) * 100.0, 0.0)
    df_["cm2_profit_per_unit"] = np.where(quantity != 0, cm2_profit / quantity, 0.0)
    df_["cm2_profit_per"] = np.where(net_sales != 0, (cm2_profit / net_sales) * 100.0, 0.0)
    df_["tacos_total_advertising_cost_of_sale"] = np.where(
        net_sales != 0, (ads_spend / net_sales) * 100.0, 0.0
    )
    df_["ads_conversion_rate"] = np.where(
        ads_clicks != 0, (ads_sale_units / ads_clicks) * 100.0, 0.0
    )
    df_["ads_roas"] = np.where(ads_spend_raw != 0, ads_sale_amount / ads_spend_raw, 0.0)

    # Keep source values where available; otherwise make the schema stable.
    for col in REPORT_COMPAT_COLUMNS:
        if col not in df_.columns:
            df_[col] = "" if col in REPORT_TEXT_COLUMNS else 0.0

    if "generated_at_utc" not in df_.columns or df_["generated_at_utc"].astype(str).str.strip().eq("").all():
        df_["generated_at_utc"] = pd.Timestamp.now(tz="UTC").isoformat()
    else:
        df_["generated_at_utc"] = df_["generated_at_utc"].fillna("").astype(str)

    if "ad_type" in df_.columns:
        df_["ad_type"] = df_["ad_type"].fillna("").astype(str)

    for col in REPORT_COMPAT_COLUMNS:
        if col not in REPORT_TEXT_COLUMNS:
            df_[col] = pd.to_numeric(df_[col], errors="coerce").replace([np.inf, -np.inf], 0).fillna(0.0)

    return df_


def ensure_report_compat_db_columns(conn, table_name: str):
    """Make legacy monthly/rolling tables accept the July-compatible columns."""
    if not table_exists_conn(conn, table_name):
        return
    for col in REPORT_COMPAT_COLUMNS:
        sql_type = "TEXT" if col in REPORT_TEXT_COLUMNS else "DOUBLE PRECISION DEFAULT 0"
        conn.execute(text(
            f'ALTER TABLE "{table_name}" ADD COLUMN IF NOT EXISTS "{col}" {sql_type}'
        ))
    _TABLE_COL_CACHE.pop(f"public.{table_name}", None)

def table_exists_conn(conn, table_name: str, schema: str = "public") -> bool:
    return bool(conn.execute(
        text("""
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = :schema
                  AND table_name = :table_name
            )
        """),
        {"schema": schema, "table_name": table_name}
    ).scalar())



def merge_monthly_ads_into_sku_grouped(conn, sku_grouped, user_id, country, month, year):
    """Override SKU advertising metrics from adsmonthly table when it exists."""
    ads_table = f"adsmonthly_{user_id}_{str(country).lower()}_{str(month).lower()}_{year}"
    if not table_exists_conn(conn, ads_table):
        return sku_grouped, False

    ads_df = pd.read_sql(text(f'SELECT * FROM "{ads_table}"'), conn)
    if ads_df.empty or "products" not in ads_df.columns:
        return sku_grouped, False

    ads_df["products"] = ads_df["products"].fillna("").astype(str).str.strip()
    ads_df = ads_df.loc[
        ads_df["products"].ne("")
        & ~ads_df["products"].str.casefold().eq("grand total")
    ].copy()
    if ads_df.empty:
        return sku_grouped, False

    source_to_target = {
        "ads_impressions": "ads_impressions",
        "ads_clicks": "ads_clicks",
        "spend": "ads_spend_raw",
        "ads_spend_raw": "ads_spend_raw",
        "ads_sale_units": "ads_sale_units",
        "ads_sale_amount": "ads_sale_amount",
        "sp_ads_sales": "sp_ads_sales",
        "sd_ads_sales": "sd_ads_sales",
        "sb_ads_sales": "sb_ads_sales",
        "product_spend": "product_spend",
        "display_spend": "display_spend",
        "brand_spend": "brand_spend",
    }

    # Prefer the explicit ads_spend_raw column if both it and spend exist.
    selected = {"products": ads_df["products"]}
    for source, target in source_to_target.items():
        if source in ads_df.columns and target not in selected:
            selected[target] = pd.to_numeric(ads_df[source], errors="coerce").fillna(0.0)
    ads_sku = pd.DataFrame(selected)

    metric_cols = [c for c in ads_sku.columns if c != "products"]
    if not metric_cols:
        return sku_grouped, False

    ads_sku = ads_sku.groupby("products", as_index=False)[metric_cols].sum()
    ads_sku = ads_sku.rename(columns={"products": "sku"})
    ads_sku["sku"] = ads_sku["sku"].astype(str).str.strip()

    out = sku_grouped.copy()
    out["sku"] = out["sku"].fillna("").astype(str).str.strip()
    out = out.merge(ads_sku, on="sku", how="left", suffixes=("", "__ads"))

    for col in metric_cols:
        ads_col = f"{col}__ads"
        if ads_col in out.columns:
            out[col] = pd.to_numeric(out[ads_col], errors="coerce").fillna(0.0)
            out.drop(columns=[ads_col], inplace=True)
        else:
            out[col] = pd.to_numeric(out.get(col, 0.0), errors="coerce").fillna(0.0)

    # The ads report spend is the authoritative advertising expense.
    spend = pd.to_numeric(out.get("ads_spend_raw", 0.0), errors="coerce").fillna(0.0).abs()
    out["ads_spend"] = spend
    out["advertising_total"] = spend
    out["advertising_fees"] = spend
    out["total_ads"] = spend

    def num(name):
        value = out[name] if name in out.columns else pd.Series(0.0, index=out.index)
        return pd.to_numeric(value, errors="coerce").fillna(0.0)

    net_sales = num("Net Sales") if "Net Sales" in out.columns else num("net_sales")
    quantity = num("total_quantity") if "total_quantity" in out.columns else num("quantity")
    clicks = num("ads_clicks")
    ad_units = num("ads_sale_units")
    ad_sales = num("ads_sale_amount")

    if str(country).lower() == "us":
        out["cm2_profit"] = (
            num("profit")
            - spend
            - num("shipping_charges").abs()
            - num("storage_fee").abs()
            - num("inventory_charges_and_reimbursement")
            - num("platform_management_fees").abs()
            + num("other_adjustment")
        )
    else:
        out["cm2_profit"] = (
            num("profit")
            - spend
            - num("lost_total").abs()
            - num("platform_fee").abs()
        )

    out["cm2_profit_per_unit"] = np.where(quantity != 0, out["cm2_profit"] / quantity, 0.0)
    out["cm2_profit_per"] = np.where(net_sales != 0, (out["cm2_profit"] / net_sales) * 100.0, 0.0)
    out["cm2_profit_percentage"] = out["cm2_profit_per"]
    out["cm2_margins"] = out["cm2_profit_per"]
    out["ads_conversion_rate"] = np.where(clicks != 0, (ad_units / clicks) * 100.0, 0.0)
    out["ads_roas"] = np.where(spend != 0, ad_sales / spend, 0.0)
    out["ads_acos"] = np.where(ad_sales != 0, (spend / ad_sales) * 100.0, 0.0)
    out["acos"] = out["ads_acos"]
    out["tacos_total_advertising_cost_of_sale"] = np.where(
        net_sales != 0, (spend / net_sales) * 100.0, 0.0
    )
    out["reimbursement_vs_cm2_margins"] = np.where(
        out["cm2_profit"] != 0,
        (num("rembursement_fee") / out["cm2_profit"]) * 100.0,
        0.0,
    )
    out["rembursment_vs_cm2_margins"] = out["reimbursement_vs_cm2_margins"]

    for col in [
        "cm2_profit", "cm2_profit_per_unit", "cm2_profit_per", "cm2_profit_percentage",
        "cm2_margins", "ads_conversion_rate", "ads_roas", "ads_acos", "acos",
        "tacos_total_advertising_cost_of_sale", "reimbursement_vs_cm2_margins",
        "rembursment_vs_cm2_margins",
    ]:
        out[col] = pd.to_numeric(out[col], errors="coerce").replace([np.inf, -np.inf], 0).fillna(0.0)

    print(f"[ADS] Applied SKU advertising data from {ads_table}")
    return out, True

def ensure_payment_columns(conn, table_name: str):
    if not table_exists_conn(conn, table_name):
        return

    conn.execute(text(f'''
        ALTER TABLE "{table_name}"
        ADD COLUMN IF NOT EXISTS debt_payment DOUBLE PRECISION DEFAULT 0
    '''))

    conn.execute(text(f'''
        ALTER TABLE "{table_name}"
        ADD COLUMN IF NOT EXISTS disbursement DOUBLE PRECISION DEFAULT 0
    '''))

    _TABLE_COL_CACHE.pop(f"public.{table_name}", None)

def ensure_storage_fee_columns(conn, table_name: str):
    if not table_exists_conn(conn, table_name):
        return

    conn.execute(text(f'''
        ALTER TABLE "{table_name}"
        ADD COLUMN IF NOT EXISTS short_term_storage_fee DOUBLE PRECISION DEFAULT 0
    '''))

    conn.execute(text(f'''
        ALTER TABLE "{table_name}"
        ADD COLUMN IF NOT EXISTS long_term_storage_fee DOUBLE PRECISION DEFAULT 0
    '''))

    conn.execute(text(f'''
        ALTER TABLE "{table_name}"
        ADD COLUMN IF NOT EXISTS fba_disposal DOUBLE PRECISION DEFAULT 0
    '''))

    conn.execute(text(f'''
        ALTER TABLE "{table_name}"
        ADD COLUMN IF NOT EXISTS inventory_charges_and_reimbursement DOUBLE PRECISION DEFAULT 0
    '''))

    conn.execute(text(f'''
        ALTER TABLE "{table_name}"
        ADD COLUMN IF NOT EXISTS placement_fee DOUBLE PRECISION DEFAULT 0
    '''))

    conn.execute(text(f'''
        ALTER TABLE "{table_name}"
        ADD COLUMN IF NOT EXISTS customs_fee DOUBLE PRECISION DEFAULT 0
    '''))

    conn.execute(text(f'''
        ALTER TABLE "{table_name}"
        ADD COLUMN IF NOT EXISTS shipping_charges DOUBLE PRECISION DEFAULT 0
    '''))

    conn.execute(text(f'''
        ALTER TABLE "{table_name}"
        ADD COLUMN IF NOT EXISTS platform_management_fees DOUBLE PRECISION DEFAULT 0
    '''))

    conn.execute(text(f'''
        ALTER TABLE "{table_name}"
        ADD COLUMN IF NOT EXISTS storage_fee DOUBLE PRECISION DEFAULT 0
    '''))

    conn.execute(text(f'''
        ALTER TABLE "{table_name}"
        ADD COLUMN IF NOT EXISTS other_adjustment DOUBLE PRECISION DEFAULT 0
    '''))

    _TABLE_COL_CACHE.pop(f"public.{table_name}", None)


def get_table_columns(conn, table_name: str, schema: str = "public"):
    cache_key = f"{schema}.{table_name}"
    if cache_key in _TABLE_COL_CACHE:
        return _TABLE_COL_CACHE[cache_key]

    q = text("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = :schema
          AND table_name = :table
        ORDER BY ordinal_position
    """)
    rows = conn.execute(q, {"schema": schema, "table": table_name}).fetchall()
    cols = [r[0] for r in rows]
    _TABLE_COL_CACHE[cache_key] = cols
    return cols


def safe_to_sql(df: pd.DataFrame, table_name: str, conn, if_exists="append",
                index=False, method="multi", chunksize=100):
    db_cols = get_table_columns(conn, table_name)

    if not df.columns.is_unique:
        df = df.loc[:, ~df.columns.duplicated()].copy()

    keep_cols = [c for c in db_cols if c in df.columns]
    df2 = df[keep_cols].copy()

    df2.to_sql(
        table_name,
        conn,
        if_exists=if_exists,
        index=index,
        method=method,
        chunksize=chunksize
    )

def process_skuwise_us_data(user_id, country, month, year):
    engine = create_engine(db_url)
    engine1 = create_engine(db_url1)
    conn = engine.connect()

    source_table = f"user_{user_id}_{country}_{month}{year}_data"

    # Main detailed monthly table, same as UK:
    # example: nse_2_us_april2025
    target_table = f"nse_{user_id}_{country}_{month}{year}"

    # Summary monthly table, old US monthly format:
    target_table_nse = f"skuwisemonthly_{user_id}_{country}_{month}{year}"

    # Rolling country table
    target_table2 = f"skuwisemonthly_{user_id}_{country}"

    target_table_us = f"skuwisemonthly_{user_id}"
    target_table_ind = f"skuwisemonthlyind_{user_id}"
    target_table_can = f"skuwisemonthlycan_{user_id}"
    target_table_gbp = f"skuwisemonthlygbp_{user_id}"

    prev_month, prev_year = get_previous_month_year(month, year)

    # Previous month should also come from NSE table, same as UK
    prev_table = f"nse_{user_id}_{country}_{prev_month}{prev_year}"

    def create_monthly_table(table_name):

        conn.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
        conn.execute(text(f"""
            CREATE TABLE {table_name} (
                id SERIAL PRIMARY KEY,
                sku TEXT,
                product_name TEXT,
                quantity INTEGER,
                return_quantity INTEGER,
                total_quantity INTEGER,
                asp REAL,
                gross_sales REAL,
                refund_sales REAL,
                tex_and_credits REAL,
                net_sales REAL,
                promotional_rebates REAL,
                promotional_rebates_percentage REAL,
                cost_of_unit_sold REAL,
                selling_fees REAL,
                fba_fees REAL,
                amazon_fee REAL,
                net_taxes REAL,
                net_credits REAL,
                misc_transaction REAL,
                other_transaction_fees REAL,
                profit REAL,
                unit_wise_profitability REAL,
                profit_percentage REAL,
                visible_ads REAL,
                dealsvouchar_ads REAL,
                advertising_total REAL,
                lost_total REAL,
                platformfeenew REAL,
                platform_management_fees REAL,
                platform_fee REAL,
                platform_fee_inventory_storage REAL,
                short_term_storage_fee REAL,
                long_term_storage_fee REAL,
                storage_fee REAL,
                other_adjustment REAL,
                fba_disposal REAL,
                inventory_charges_and_reimbursement REAL,
                placement_fee REAL,
                customs_fee REAL,
                shipping_charges REAL,
                shipment_fees REAL,
                cm2_profit REAL,
                cm2_profit_percentage REAL,
                cm2_margins REAL,
                acos REAL,
                debt_payment REAL DEFAULT 0,
                disbursement REAL DEFAULT 0,
                rembursement_fee REAL,
                rembursment_vs_cm2_margins REAL,
                reimbursement_vs_sales REAL,
                sales_mix REAL,
                profit_mix REAL,
                month TEXT,
                year TEXT,
                country TEXT,
                user_id INTEGER
            )
        """))

    def create_us_nse_full_table(table_name):
        conn.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
        conn.execute(text(f"""
            CREATE TABLE {table_name} (
                id SERIAL PRIMARY KEY,
                product_name TEXT,
                sku TEXT,
                quantity INTEGER,
                return_quantity INTEGER,
                total_quantity INTEGER,

                product_sales REAL,
                product_sales_tax REAL,
                postage_credits REAL,
                shipping_credits REAL,
                shipping_credits_tax REAL,
                gift_wrap_credits REAL,
                giftwrap_credits_tax REAL,

                promotional_rebates REAL,
                promotional_rebates_tax REAL,
                promotional_rebates_percentage REAL,

                gross_sales REAL,
                refund_sales REAL,
                tex_and_credits REAL,
                net_sales REAL,

                price_in_gbp REAL,
                cost_of_unit_sold REAL,

                marketplace_facilitator_tax REAL,
                other_transaction_fees REAL,
                selling_fees REAL,
                refund_selling_fees REAL,
                fba_fees REAL,
                other REAL,
                total REAL,
                amazon_fee REAL,

                net_taxes REAL,
                net_credits REAL,
                misc_transaction REAL,

                profit REAL,
                unit_wise_profitability REAL,
                profit_percentage REAL,

                lost_total REAL,
                visible_ads REAL,
                dealsvouchar_ads REAL,
                advertising_total REAL,

                platformfeenew REAL,
                platform_management_fees REAL,
                platform_fee REAL,
                platform_fee_inventory_storage REAL,
                short_term_storage_fee REAL,
                long_term_storage_fee REAL,
                storage_fee REAL,
                other_adjustment REAL,
                fba_disposal REAL,
                inventory_charges_and_reimbursement REAL,
                placement_fee REAL,
                customs_fee REAL,

                shipping_charges REAL,
                shipment_fees REAL,

                cm2_profit REAL,
                cm2_profit_percentage REAL,
                cm2_margins REAL,
                acos REAL,
                debt_payment REAL DEFAULT 0,
                disbursement REAL DEFAULT 0,
                rembursement_fee REAL,
                rembursment_vs_cm2_margins REAL,
                reimbursement_vs_sales REAL,

                sales_mix REAL,
                profit_mix REAL,

                errorstatus TEXT,
                answer REAL,
                difference REAL,

                month TEXT,
                year TEXT,
                country TEXT,
                user_id INTEGER
            )
        """))

    def safe_series(df_, col, default=0.0):
        if col in df_.columns:
            return pd.to_numeric(df_[col], errors="coerce").fillna(default)
        return pd.Series(default, index=df_.index, dtype="float64")

    def sum_total_where_desc_contains(df_, keywords):
        if "total" not in df_.columns:
            return 0.0
        desc = df_.get("description", pd.Series("", index=df_.index)).astype(str)
        pattern = "|".join(re.escape(k) for k in keywords)
        mask = desc.str.contains(pattern, case=False, na=False, regex=True)
        return float(pd.to_numeric(df_.loc[mask, "total"], errors="coerce").fillna(0).sum())

    def sku_sum_total_where_desc_contains(df_, keywords, out_col):
        if "total" not in df_.columns:
            return pd.DataFrame(columns=["sku", out_col])

        desc = df_.get("description", pd.Series("", index=df_.index)).astype(str)
        pattern = "|".join(re.escape(k) for k in keywords)
        mask = desc.str.contains(pattern, case=False, na=False, regex=True)

        out = (
            df_.loc[
                mask
                & df_["sku"].notna()
                & (df_["sku"].astype(str).str.strip() != "")
                & (df_["sku"].astype(str).str.strip() != "0")
            ]
            .groupby("sku", as_index=False)["total"]
            .sum()
            .rename(columns={"total": out_col})
        )
        out[out_col] = pd.to_numeric(out[out_col], errors="coerce").fillna(0.0)
        out["sku"] = out["sku"].astype(str).str.strip()
        return out

    try:
        df = pd.read_sql(f"SELECT * FROM {source_table}", conn)
        if df.empty:
            print(f"No data found in {source_table}")
            return

        # ---------- previous month ----------
        table_exists_query = f"""
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = 'public'
                AND table_name = '{prev_table}'
            );
        """
        table_check_result = conn.execute(text(table_exists_query)).fetchone()
        table_exists = table_check_result[0] if table_check_result else False

        a = b = c = d = e = f = g = h = i = j = k = l = m = n = o = p = q = r = 0

        if table_exists:
            prev_query = f"""
                SELECT sku,
                    net_sales AS previous_net_sales,
                    net_credits AS previous_net_credits,
                    profit AS previous_profit,
                    profit_percentage AS previous_profit_percentage,
                    quantity AS previous_quantity,
                    cost_of_unit_sold AS previous_cost_of_unit_sold,
                    amazon_fee AS previous_amazon_fee,
                    net_taxes AS previous_net_taxes,
                    fba_fees AS previous_fba_fees,
                    selling_fees AS previous_selling_fees,
                    platform_fee AS previous_platform_fee,
                    rembursement_fee AS previous_rembursement_fee,
                    advertising_total AS previous_advertising_total,
                    reimbursement_vs_sales AS previous_reimbursement_vs_sales,
                    cm2_profit AS previous_cm2_profit,
                    cm2_margins AS previous_cm2_margins,
                    acos AS previous_acos,
                    rembursment_vs_cm2_margins AS previous_rembursment_vs_cm2_margins
                FROM {prev_table}
            """
            df_prev = pd.read_sql(prev_query, conn)

            total_query = f"""
                SELECT net_sales, net_credits, profit, profit_percentage, quantity,
                       cost_of_unit_sold, amazon_fee, net_taxes, fba_fees, selling_fees,
                       platform_fee, rembursement_fee, advertising_total, reimbursement_vs_sales,
                       cm2_profit, cm2_margins, acos, rembursment_vs_cm2_margins
                FROM {prev_table}
                WHERE sku = 'TOTAL'
            """
            total_row = pd.read_sql(total_query, conn)
            if not total_row.empty:
                total_values = total_row.iloc[0].to_dict()
                total_values = {key: (value if pd.notna(value) else 0) for key, value in total_values.items()}
                a = total_values.get("net_sales", 0)
                b = total_values.get("net_credits", 0)
                c = total_values.get("platform_fee", 0)
                d = total_values.get("rembursement_fee", 0)
                e = total_values.get("advertising_total", 0)
                f = total_values.get("reimbursement_vs_sales", 0)
                g = total_values.get("cm2_profit", 0)
                h = total_values.get("cm2_margins", 0)
                i = total_values.get("acos", 0)
                j = total_values.get("rembursment_vs_cm2_margins", 0)
                k = total_values.get("profit", 0)
                l = total_values.get("profit_percentage", 0)
                m = total_values.get("quantity", 0)
                n = total_values.get("cost_of_unit_sold", 0)
                o = total_values.get("amazon_fee", 0)
                p = total_values.get("net_taxes", 0)
                q = total_values.get("fba_fees", 0)
                r = total_values.get("selling_fees", 0)
        else:
            df_prev = pd.DataFrame(columns=[
                "sku", "previous_net_sales", "previous_net_credits", "previous_profit",
                "previous_profit_percentage", "previous_quantity", "previous_cost_of_unit_sold",
                "previous_amazon_fee", "previous_net_taxes", "previous_fba_fees",
                "previous_selling_fees", "previous_platform_fee", "previous_rembursement_fee",
                "previous_advertising_total", "previous_reimbursement_vs_sales",
                "previous_cm2_profit", "previous_cm2_margins", "previous_acos",
                "previous_rembursment_vs_cm2_margins"
            ]).fillna(0)

        numeric_column_previous = [
            "previous_net_sales", "previous_net_credits", "previous_profit",
            "previous_profit_percentage", "previous_quantity", "previous_cost_of_unit_sold",
            "previous_amazon_fee", "previous_net_taxes", "previous_fba_fees",
            "previous_selling_fees", "previous_platform_fee", "previous_rembursement_fee",
            "previous_advertising_total", "previous_reimbursement_vs_sales",
            "previous_cm2_profit", "previous_cm2_margins", "previous_acos",
            "previous_rembursment_vs_cm2_margins"
        ]
        numeric_column_previous = [col for col in numeric_column_previous if col in df_prev.columns]
        if numeric_column_previous:
            df_prev[numeric_column_previous] = (
                df_prev[numeric_column_previous].apply(pd.to_numeric, errors="coerce").fillna(0)
            )

        # ---------- clean input ----------
        likely_text_cols = ["sku", "type", "description", "marketplace", "fulfilment", "product_name", "errorstatus"]
        for col in likely_text_cols:
            if col in df.columns:
                df[col] = df[col].astype(str)

        if "platform_fees" not in df.columns:
            df["platform_fees"] = 0.0
        if "net_reimbursement" not in df.columns:
            df["net_reimbursement"] = 0.0
        if "shipping_credits" not in df.columns:
            df["shipping_credits"] = 0.0
        if "postage_credits" not in df.columns:
            df["postage_credits"] = 0.0
        if "gift_wrap_credits" not in df.columns:
            df["gift_wrap_credits"] = 0.0
        if "answer" not in df.columns:
            df["answer"] = 0.0
        if "difference" not in df.columns:
            df["difference"] = 0.0

        for col in ["total", "other"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col].astype(str).str.replace(",", ""), errors="coerce").fillna(0)

        numeric_columns = [
            "product_sales", "promotional_rebates", "promotional_rebates_tax", "product_sales_tax",
            "selling_fees", "fba_fees", "other", "marketplace_facilitator_tax",
            "shipping_credits_tax", "giftwrap_credits_tax", "postage_credits",
            "shipping_credits", "gift_wrap_credits", "price_in_gbp", "cost_of_unit_sold",
            "quantity", "total", "other_transaction_fees", "platform_fees",
            "net_reimbursement", "answer", "difference"
        ]
        numeric_columns = [col for col in numeric_columns if col in df.columns]
        if numeric_columns:
            df[numeric_columns] = df[numeric_columns].apply(pd.to_numeric, errors="coerce").fillna(0)

        df["sku"] = df["sku"].astype(str).str.strip()
        df_valid = df[
            df["sku"].notna()
            & (df["sku"] != "")
            & (df["sku"] != "0")
            & (df["sku"].str.lower() != "none")
        ].copy()

        df["type_norm"] = df.get("type", pd.Series("", index=df.index)).astype(str).str.strip().str.lower()
        df["desc_norm"] = df.get("description", pd.Series("", index=df.index)).astype(str).str.strip()

        # ---------- main totals ----------
        debt_payment_total = abs(sum_total_where_desc_contains(df, ["DebtPayment"]))
        disbursement_total = abs(sum_total_where_desc_contains(df, ["Disbursement"]))

        rembursement_fee_col_sum = safe_series(df, "net_reimbursement").sum()

        # keep existing reimbursement logic unchanged
        rembursement_fee = disbursement_total + rembursement_fee_col_sum

        shipment_keywords = [
            "FBA international shipping charge",
            "FBA Inbound Placement Service Fee",
            "FBA international shipping customs charge",
        ]
        shipment_mask = df["desc_norm"].str.contains("|".join(shipment_keywords), case=False, na=False)
        shipment_charges = abs(safe_series(df.loc[shipment_mask], "total").sum())
        shipment_fees = abs(sum_total_where_desc_contains(
            df,
            ["AWDTransportationFee"],
        ))
        visible_ads_total = abs(sum_total_where_desc_contains(df, ["ProductAdsPayment"]))

        dealsvouchar_ads_total = abs(sum_total_where_desc_contains(df, [
            "Cost of Advertising",
            "Coupon Redemption Fee",
            "Deals",
            "Lightning Deal",
            "CouponPerformanceEvent",
            "CouponParticipationEvent",
            "SellerDealComplete",
            "VineCharge",
            "SellerPoweredCoupon",
            "DealParticipationEvent",
            "DealPerformanceEvent",
        ]))

        advertising_total = visible_ads_total + dealsvouchar_ads_total

        


        # ---------- refund / quantity ----------
        refund_fees = (
            df[df["type_norm"] == "refund"]
            .groupby("sku", as_index=False)["selling_fees"]
            .sum()
            .rename(columns={"selling_fees": "refund_selling_fees"})
        )
        refund_fees["sku"] = refund_fees["sku"].astype(str).str.strip()

        # FBA fees: use only Shipment and Refund transaction types
        fba_fees_df = (
            df.loc[
                df["type_norm"].isin(["shipment", "refund"])
                & df["sku"].notna()
                & (df["sku"].astype(str).str.strip() != "")
                & (df["sku"].astype(str).str.strip() != "0")
                & (df["sku"].astype(str).str.lower() != "none"),
                ["sku", "fba_fees"]
            ]
            .groupby("sku", as_index=False)["fba_fees"]
            .sum()
        )

        fba_fees_df["sku"] = fba_fees_df["sku"].astype(str).str.strip()
        fba_fees_df["fba_fees"] = pd.to_numeric(
            fba_fees_df["fba_fees"],
            errors="coerce"
        ).fillna(0.0)

        quantity_df = (
            df[df["type_norm"].isin(["order", "shipment"])]
            .groupby("sku", as_index=False)["quantity"]
            .sum()
        )

        # Amazon may return the same refund financial event in both
        # RELEASED and DEFERRED_RELEASED buckets. Remove only exact refund
        # duplicates before summing quantity, otherwise June becomes 180
        # instead of the correct 138.
        refund_rows = df[df["type_norm"] == "refund"].copy()

        refund_dedupe_cols = [
            col for col in [
                "order_id", "sku", "description", "quantity",
                "product_sales", "product_sales_tax",
                "postage_credits", "shipping_credits",
                "shipping_credits_tax", "gift_wrap_credits",
                "giftwrap_credits_tax", "promotional_rebates",
                "promotional_rebates_tax", "selling_fees",
                "fba_fees", "other_transaction_fees", "other", "total",
            ]
            if col in refund_rows.columns
        ]

        if refund_dedupe_cols:
            refund_rows = refund_rows.drop_duplicates(
                subset=refund_dedupe_cols,
                keep="last",
            )

        return_qty_df = (
            refund_rows
            .groupby("sku", as_index=False)["quantity"]
            .sum()
            .rename(columns={"quantity": "return_quantity"})
        )
        if not return_qty_df.empty:
            return_qty_df["return_quantity"] = pd.to_numeric(return_qty_df["return_quantity"], errors="coerce").fillna(0).abs()

        # ---------- lost / misc ----------
        LOST_DESCRIPTIONS = {
            "REVERSAL_REIMBURSEMENT",
            "WAREHOUSE_LOST",
            "WAREHOUSE_DAMAGE",
            "MISSING_FROM_INBOUND",
            "FREE_REPLACEMENT_REFUND_ITEMS",
        }

        lost_mask = df["desc_norm"].isin(LOST_DESCRIPTIONS)
        lost_total_df = (
            df.loc[lost_mask]
            .groupby("sku", as_index=False)["total"]
            .sum()
            .rename(columns={"total": "lost_total"})
        )
        if not lost_total_df.empty:
            lost_total_df["lost_total"] = pd.to_numeric(lost_total_df["lost_total"], errors="coerce").fillna(0)

        # ---------- misc transaction: SKU-wise + blank-SKU total ----------

        def _norm_misc_key(x):
            return re.sub(r"\s+", " ", str(x or "").strip()).casefold()

        df["sku"] = df["sku"].fillna("").astype(str).str.strip()
        df["desc_norm"] = df.get("description", pd.Series("", index=df.index)).fillna("").astype(str).str.strip()
        df["type_norm"] = (
            df.get("type", pd.Series("", index=df.index))
            .fillna("")
            .astype(str)
            .str.strip()
            .str.lower()
        )

        df["desc_key"] = df["desc_norm"].map(_norm_misc_key)
        df["type_key"] = df["type_norm"].map(_norm_misc_key)
        df["total"] = pd.to_numeric(df.get("total", 0), errors="coerce").fillna(0.0)

        EXCLUDE_DESCRIPTIONS = {
            # Advertising / deals
            "Cost of Advertising",
            "Coupon Redemption Fee",
            "Deals",
            "Lightning Deal",
            "ProductAdsPayment",
            "CouponPerformanceEvent",
            "CouponParticipationEvent",
            "SellerDealComplete",
            "VineCharge",
            "SellerPoweredCoupon",
            "DealParticipationEvent",
            "DealPerformanceEvent",

            # Platform / storage / shipment buckets
            "FBA Return Fee",
            "FBA Long-Term Storage Fee",
            "FBA storage fee",
            "FBADisposal",
            "FBAStorageBilling",
            "FBALongTermStorageBilling",
            "INCORRECT_FEES_NON_ITEMIZED",
            "StorageReservationBilling",
            "FBAStorageFeeAdjustment",
            "Subscription",
            "PaidServicesCharge",
            "FBAInboundConvenience",
            "AWDProcessingFee",
            "AWDTransportationFee",
            "AGSGlobalInboundTransportation",

            # Normal payment / refund / transfer buckets
            "Order Payment",
            "Refund",
            "Disbursement",
            "DebtPayment",

            # Lost / reimbursement bucket
            "REVERSAL_REIMBURSEMENT",
            "WAREHOUSE_LOST",
            "WAREHOUSE_DAMAGE",
            "MISSING_FROM_INBOUND",
            "FREE_REPLACEMENT_REFUND_ITEMS",
        }

        EXCLUDE_TYPES = {
            "Transfer",
            "Refund",
        }

        exclude_desc_keys = {_norm_misc_key(x) for x in EXCLUDE_DESCRIPTIONS}
        exclude_type_keys = {_norm_misc_key(x) for x in EXCLUDE_TYPES}

        leftout_mask = (
            ~df["desc_key"].isin(exclude_desc_keys)
            & ~df["type_key"].isin(exclude_type_keys)
        )

        all_misc_transaction_total = (
            pd.to_numeric(
                df.loc[leftout_mask, "total"],
                errors="coerce"
            )
            .fillna(0.0)
            .sum()
        )

        # misc_transaction is SKU-wise only; blank/unassigned misc rows move to other_adjustment.
        tmp_misc = df.loc[
            leftout_mask
            & df["sku"].notna()
            & (df["sku"] != "")
            & (df["sku"] != "0")
            & (df["sku"].str.lower() != "none"),
            ["sku", "total"]
        ].copy()

        misc_transaction_df = (
            tmp_misc.groupby("sku", as_index=False)["total"]
            .sum()
            .rename(columns={"total": "misc_transaction"})
        )

        misc_transaction_df["misc_transaction"] = pd.to_numeric(
            misc_transaction_df["misc_transaction"],
            errors="coerce"
        ).fillna(0.0)

        misc_transaction_total = float(misc_transaction_df["misc_transaction"].sum())
        unassigned_misc_transaction_total = all_misc_transaction_total - misc_transaction_total

        platformfeenew_total = abs(sum_total_where_desc_contains(df, ["Subscription"]))

        # Platform management fees include paid services and subscription charges.
        PLATFORM_MANAGEMENT_FEE_KEYWORDS = [
            "PaidServicesCharge",
            "Subscription",
        ]
        platform_management_fees_total = abs(sum_total_where_desc_contains(
            df,
            PLATFORM_MANAGEMENT_FEE_KEYWORDS,
        ))
        platform_management_fees_df = sku_sum_total_where_desc_contains(
            df,
            PLATFORM_MANAGEMENT_FEE_KEYWORDS,
            "platform_management_fees",
        )
        if not platform_management_fees_df.empty:
            platform_management_fees_df["platform_management_fees"] = pd.to_numeric(
                platform_management_fees_df["platform_management_fees"],
                errors="coerce",
            ).fillna(0.0).abs()

        platform_fee_inventory_storage_total = abs(sum_total_where_desc_contains(df, [
            "FBA Return Fee",
            "FBA Long-Term Storage Fee",
            "FBA storage fee",
            "FBADisposal",
            "FBAStorageBilling",
            "FBALongTermStorageBilling",
            "INCORRECT_FEES_NON_ITEMIZED",
            "StorageReservationBilling",
            "FBAStorageFeeAdjustment",
        ]))

        short_term_storage_fee_total = abs(sum_total_where_desc_contains(df, [
            "FBAStorageBilling"
        ]))

        long_term_storage_fee_total = abs(sum_total_where_desc_contains(df, [
            "FBALongTermStorageBilling"
        ]))

        fba_disposal_total = sum_total_where_desc_contains(df, [
            "FBADisposal",
            "MISSING_FROM_INBOUND_CLAWBACK",
            "COMPENSATED_CLAWBACK",
        ])

        other_adjustment_total = (
            abs(sum_total_where_desc_contains(df, ["FBAStorageFeeAdjustment"]))
            + abs(unassigned_misc_transaction_total)
        )

        other_adjustment_df = sku_sum_total_where_desc_contains(
            df,
            ["FBAStorageFeeAdjustment"],
            "other_adjustment",
        )
        if not other_adjustment_df.empty:
            other_adjustment_df["other_adjustment"] = pd.to_numeric(
                other_adjustment_df["other_adjustment"],
                errors="coerce",
            ).fillna(0.0).abs()

        # Placement fees preserve the source sign (normally negative).
        PLACEMENT_FEE_KEYWORDS = [
            "FBAInboundConvenience",
            "AWDProcessingFee",
        ]
        placement_fee_total = sum_total_where_desc_contains(
            df,
            PLACEMENT_FEE_KEYWORDS,
        )
        placement_fee_df = sku_sum_total_where_desc_contains(
            df,
            PLACEMENT_FEE_KEYWORDS,
            "placement_fee",
        )


        # Customs fees: AGS global inbound transportation.
        # Preserve the original source sign (normally negative).
        CUSTOMS_FEE_KEYWORDS = [
            "AGSGlobalInboundTransportation",
        ]
        customs_fee_total = sum_total_where_desc_contains(
            df,
            CUSTOMS_FEE_KEYWORDS,
        )
        customs_fee_df = sku_sum_total_where_desc_contains(
            df,
            CUSTOMS_FEE_KEYWORDS,
            "customs_fee",
        )
        if not customs_fee_df.empty:
            customs_fee_df["customs_fee"] = pd.to_numeric(
                customs_fee_df["customs_fee"],
                errors="coerce",
            ).fillna(0.0)

        shipment_fees_df = sku_sum_total_where_desc_contains(
            df,
            ["AWDTransportationFee"],
            "shipment_fees",
        )
        if not shipment_fees_df.empty:
            shipment_fees_df["shipment_fees"] = pd.to_numeric(
                shipment_fees_df["shipment_fees"],
                errors="coerce",
            ).fillna(0.0).abs()

        short_term_storage_fee_df = sku_sum_total_where_desc_contains(
            df,
            ["FBAStorageBilling"],
            "short_term_storage_fee"
        )

        long_term_storage_fee_df = sku_sum_total_where_desc_contains(
            df,
            ["FBALongTermStorageBilling"],
            "long_term_storage_fee"
        )

        fba_disposal_df = sku_sum_total_where_desc_contains(
            df,
            [
                "FBADisposal",
                "MISSING_FROM_INBOUND_CLAWBACK",
                "COMPENSATED_CLAWBACK",
            ],
            "fba_disposal"
        )


        lost_total_amount = abs(
            pd.to_numeric(
                lost_total_df.get("lost_total", pd.Series(dtype=float)),
                errors="coerce"
            ).fillna(0).sum()
        )

        platform_fee = (
            platformfeenew_total
            + platform_fee_inventory_storage_total
            - misc_transaction_total
            - lost_total_amount
        )


        # ---------- sku aggregate ----------
        group_cols = {
            "price_in_gbp": "mean",
            "product_sales": "sum",
            "promotional_rebates": "sum",
            "promotional_rebates_tax": "sum",
            "product_sales_tax": "sum",
            "selling_fees": "sum",
            "other": "sum",
            "marketplace_facilitator_tax": "sum",
            "shipping_credits_tax": "sum",
            "giftwrap_credits_tax": "sum",
            "postage_credits": "sum",
            "shipping_credits": "sum",
            "gift_wrap_credits": "sum",
            "cost_of_unit_sold": "sum",
            "total": "sum",
            "ads_impressions": "sum",
            "ads_clicks": "sum",
            "ads_spend_raw": "sum",
            "ads_sale_units": "sum",
            "ads_sale_amount": "sum",
            "sp_ads_sales": "sum",
            "sd_ads_sales": "sum",
            "sb_ads_sales": "sum",
            "product_spend": "sum",
            "display_spend": "sum",
            "brand_spend": "sum",
            "ad_type": "first",
            "product_name": "first",
        }
        group_cols = {k: v for k, v in group_cols.items() if k in df.columns}

        sku_grouped = df_valid.groupby("sku").agg(group_cols).reset_index()
        sku_grouped["sku"] = sku_grouped["sku"].astype(str).str.strip()

        sku_grouped = sku_grouped.merge(
            fba_fees_df,
            on="sku",
            how="left"
        )

        sku_grouped["fba_fees"] = pd.to_numeric(
            sku_grouped["fba_fees"],
            errors="coerce"
        ).fillna(0.0)

        sku_grouped = sku_grouped.merge(df_prev, on="sku", how="left").fillna(0)
        sku_grouped = sku_grouped.merge(refund_fees, on="sku", how="left")
        sku_grouped = sku_grouped.merge(quantity_df, on="sku", how="left")
        sku_grouped = sku_grouped.merge(return_qty_df, on="sku", how="left")
        sku_grouped = sku_grouped.merge(lost_total_df, on="sku", how="left")
        sku_grouped = sku_grouped.merge(misc_transaction_df, on="sku", how="left")
        sku_grouped = sku_grouped.merge(short_term_storage_fee_df, on="sku", how="left")
        sku_grouped = sku_grouped.merge(long_term_storage_fee_df, on="sku", how="left")
        sku_grouped = sku_grouped.merge(fba_disposal_df, on="sku", how="left")
        sku_grouped = sku_grouped.merge(other_adjustment_df, on="sku", how="left")
        sku_grouped = sku_grouped.merge(placement_fee_df, on="sku", how="left")
        sku_grouped = sku_grouped.merge(customs_fee_df, on="sku", how="left")
        sku_grouped = sku_grouped.merge(shipment_fees_df, on="sku", how="left")
        sku_grouped = sku_grouped.merge(platform_management_fees_df, on="sku", how="left")
        for col in ["short_term_storage_fee", "long_term_storage_fee"]:
            sku_grouped[col] = safe_series(sku_grouped, col).abs()

        # Preserve the original source sign for FBA disposal/clawback values.
        sku_grouped["fba_disposal"] = safe_series(sku_grouped, "fba_disposal")

        sku_grouped["inventory_charges_and_reimbursement"] = (
            safe_series(sku_grouped, "fba_disposal").abs()
            - safe_series(sku_grouped, "lost_total").abs()
        )

        # storage_fee is derived after the two source columns are normalized.
        sku_grouped["storage_fee"] = (
            safe_series(sku_grouped, "short_term_storage_fee").abs()
            + safe_series(sku_grouped, "long_term_storage_fee").abs()
        )
        sku_grouped["other_adjustment"] = safe_series(
            sku_grouped, "other_adjustment"
        ).abs()

        sku_grouped["storage_fee"] = (
            safe_series(sku_grouped, "short_term_storage_fee").abs()
            + safe_series(sku_grouped, "long_term_storage_fee").abs()
        )
        sku_grouped["other_adjustment"] = safe_series(
            sku_grouped, "other_adjustment"
        ).abs()

        sku_grouped["placement_fee"] = pd.to_numeric(
            sku_grouped.get("placement_fee", 0.0),
            errors="coerce",
        ).fillna(0.0)

        if "customs_fee" not in sku_grouped.columns:
            sku_grouped["customs_fee"] = 0.0
        else:
            sku_grouped["customs_fee"] = pd.to_numeric(
                sku_grouped["customs_fee"],
                errors="coerce",
            ).fillna(0.0)

        sku_grouped["shipment_fees"] = safe_series(
            sku_grouped, "shipment_fees"
        ).abs()
        sku_grouped["platform_management_fees"] = safe_series(
            sku_grouped, "platform_management_fees"
        ).abs()
        sku_grouped["refund_selling_fees"] = safe_series(sku_grouped, "refund_selling_fees")
        sku_grouped["quantity"] = pd.to_numeric(
            sku_grouped.get("quantity", 0),
            errors="coerce"
        ).fillna(0).abs()

        sku_grouped["return_quantity"] = pd.to_numeric(
            sku_grouped.get("return_quantity", 0),
            errors="coerce"
        ).fillna(0).abs().astype(int)

        sku_grouped["lost_total"] = pd.to_numeric(
            sku_grouped.get("lost_total", 0),
            errors="coerce"
        ).fillna(0)

        sku_grouped["misc_transaction"] = pd.to_numeric(
            sku_grouped.get("misc_transaction", 0),
            errors="coerce"
        ).fillna(0)

        # ✅ quantity = shipment/order quantity + refund quantity
        sku_grouped["quantity"] = (
            sku_grouped["quantity"] + sku_grouped["return_quantity"]
        ).astype(int)

        # ✅ total_quantity = quantity - refund
        sku_grouped["total_quantity"] = (
            sku_grouped["quantity"] - sku_grouped["return_quantity"]
        ).astype(int)

        sku_grouped["selling_fees"] = pd.to_numeric(
            sku_grouped["selling_fees"],
            errors="coerce"
        ).fillna(0)

        sku_grouped["refund_selling_fees"] = pd.to_numeric(
            sku_grouped.get("refund_selling_fees", 0),
            errors="coerce"
        ).fillna(0)

        # keep selling_fees as cost/fee, same sign style as fba_fees
        sku_grouped["selling_fees"] = -(
            sku_grouped["selling_fees"].abs()
            + sku_grouped["refund_selling_fees"].abs()
        )

        # ---------- shared formula engine: US uses the same reusable formulas as UK ----------
        # Keep the US-specific preparation/merges above, but calculate financial metrics from
        # app.utils.formulas_utils so UK and US stay aligned.

        sku_grouped["sku"] = sku_grouped["sku"].astype(str).str.strip()

        def merge_formula_metric(metric_df, value_col, out_col, component_map=None):
            """
            Merge a formula helper output onto sku_grouped without creating duplicate *_x/*_y cols.
            component_map example: {"gross_sales": "gross_sales"}.
            """
            nonlocal sku_grouped
            if component_map is None:
                component_map = {}

            cols = ["sku", value_col] + [c for c in component_map.keys() if c in metric_df.columns]
            if metric_df is None or metric_df.empty or "sku" not in metric_df.columns or value_col not in metric_df.columns:
                if out_col not in sku_grouped.columns:
                    sku_grouped[out_col] = 0.0
                for dest in component_map.values():
                    if dest not in sku_grouped.columns:
                        sku_grouped[dest] = 0.0
                return

            tmp = metric_df[cols].copy()
            rename_map = {value_col: out_col, **component_map}
            tmp = tmp.rename(columns=rename_map)
            tmp["sku"] = tmp["sku"].astype(str).str.strip()

            for c in [out_col, *component_map.values()]:
                if c in sku_grouped.columns:
                    sku_grouped = sku_grouped.drop(columns=[c])

            sku_grouped = sku_grouped.merge(tmp, on="sku", how="left")
            for c in [out_col, *component_map.values()]:
                if c not in sku_grouped.columns:
                    sku_grouped[c] = 0.0
                else:
                    sku_grouped[c] = pd.to_numeric(
                        sku_grouped[c],
                        errors="coerce"
                    ).fillna(0.0)

        # Formula totals + SKU breakups
        sales_total, sales_by_sku, _ = us_sales(df, country=country)
        gross_total, gross_by_sku, _ = us_gross_sales(df, country=country)
        tax_total, tax_by_sku, _ = us_tax(df, country=country)
        credits_total, credits_by_sku, _ = us_credits(df, country=country)
        fee_total, fees_by_sku, _ = us_amazon_fee(df, country=country)
        platform_total, platform_by_sku, _ = us_platform_fee(df, country=country)
        advertising_total_formula, advertising_by_sku, _ = us_advertising(df, country=country)

        # COGS uses the US-prepared total_quantity, same structure used by the UK helper.
        cogs_total, cogs_by_sku, _ = us_cogs(sku_grouped, country=country)

        # Sales helper also returns the exact breakup columns needed by the DB.
        merge_formula_metric(
            sales_by_sku,
            "__metric__",
            "Net Sales",
            {
                "gross_sales": "gross_sales",
                "refund_sales": "refund_sales",
                "promotional_rebates": "promotional_rebates",
            },
        )
        merge_formula_metric(tax_by_sku, "__metric__", "net_taxes")
        merge_formula_metric(credits_by_sku, "__metric__", "net_credits")
        sku_grouped["tex_and_credits"] = (
            pd.to_numeric(
                sku_grouped["net_taxes"],
                errors="coerce"
            ).fillna(0.0).abs()
            +
            pd.to_numeric(
                sku_grouped["net_credits"],
                errors="coerce"
            ).fillna(0.0)
            +
            pd.to_numeric(
                sku_grouped["misc_transaction"],
                errors="coerce"
            ).fillna(0.0).abs()
        )
        merge_formula_metric(fees_by_sku, "__metric__", "amazon_fee")
        merge_formula_metric(cogs_by_sku, "__metric__", "cost_of_unit_sold")
        merge_formula_metric(platform_by_sku, "__metric__", "platform_fee")

        # Advertising helper exposes visible/deal components, so keep those columns too.
        merge_formula_metric(
            advertising_by_sku,
            "__metric__",
            "advertising_total",
            {
                "visible_ads": "visible_ads",
                "dealsvouchar_ads": "dealsvouchar_ads",
            },
        )

        # Keep both naming styles used later in this US function / database schema.
        sku_grouped["Net Taxes"] = safe_series(sku_grouped, "net_taxes")
        sku_grouped["Net Credits"] = safe_series(sku_grouped, "net_credits")

        for col in [
            "Net Sales", "gross_sales", "refund_sales", "tex_and_credits",
            "net_taxes", "Net Taxes", "net_credits", "Net Credits",
            "amazon_fee", "cost_of_unit_sold", "platform_fee", "advertising_total",
            "visible_ads", "dealsvouchar_ads", "rembursement_fee", "shipment_fees",
            "misc_transaction", "lost_total", "inventory_charges_and_reimbursement", "placement_fee", "customs_fee", "shipping_charges",
        ]:
            if col not in sku_grouped.columns:
                sku_grouped[col] = 0.0
            sku_grouped[col] = pd.to_numeric(sku_grouped[col], errors="coerce").fillna(0.0)

        sku_grouped["other_transaction_fees"] = _other_transaction_fees_series(
            sku_grouped,
            "Net Taxes",
            "Net Credits",
        )

        # Profit includes misc_transaction as a positive adjustment.
        # profit = net_sales - cogs - amazon_fee - net_taxes + net_credits + abs(misc_transaction)
        sku_grouped["profit"] = (
            sku_grouped["Net Sales"]
            - sku_grouped["cost_of_unit_sold"].abs()
            - sku_grouped["amazon_fee"].abs()
            - sku_grouped["Net Taxes"].abs()
            + sku_grouped["Net Credits"]
            + sku_grouped["misc_transaction"].abs()
        )

        sku_grouped["profit%"] = np.where(
            sku_grouped["Net Sales"] != 0,
            (sku_grouped["profit"] / sku_grouped["Net Sales"]) * 100,
            0,
        )
        sku_grouped["profit%"] = sku_grouped["profit%"].replace([np.inf, -np.inf], 0).fillna(0)

        # ---------- breakup columns sku-wise ----------
        # Shipping charges = placement fee + customs fee + shipment fees.
        # Store as a positive expense amount.
        sku_grouped["shipping_charges"] = (
            safe_series(sku_grouped, "placement_fee").abs()
            + safe_series(sku_grouped, "customs_fee").abs()
            + safe_series(sku_grouped, "shipment_fees").abs()
        )

        for col in [
            "visible_ads", "dealsvouchar_ads", "platformfeenew", "platform_management_fees",
            "platform_fee_inventory_storage", "short_term_storage_fee", "long_term_storage_fee", "storage_fee", "other_adjustment", "fba_disposal", "inventory_charges_and_reimbursement", "placement_fee", "customs_fee",
            "shipping_charges", "shipment_fees",
            "platform_fee", "advertising_total", "rembursement_fee",
        ]:
            if col not in sku_grouped.columns:
                sku_grouped[col] = 0.0
            sku_grouped[col] = pd.to_numeric(sku_grouped[col], errors="coerce").fillna(0.0)

        sku_grouped["reimbursement_vs_sales"] = np.where(
            sku_grouped["Net Sales"] != 0,
            (sku_grouped["rembursement_fee"] / sku_grouped["Net Sales"]) * 100,
            0,
        )
        # Keep CM2 as a report-level metric, matching the UK output.
        # Shipping, storage, platform-management and adjustment transactions can
        # have blank/non-product SKUs, so allocating them to individual SKUs
        # produces misleading SKU-wise CM2 values. The correct CM2 is written
        # only to the TOTAL row below.
        sku_grouped["cm2_profit"] = 0.0
        sku_grouped["cm2_margins"] = 0.0
        sku_grouped["acos"] = np.where(
            sku_grouped["Net Sales"] != 0,
            (sku_grouped["advertising_total"] / sku_grouped["Net Sales"]) * 100,
            0,
        )
        sku_grouped["rembursment_vs_cm2_margins"] = np.where(
            sku_grouped["cm2_profit"] != 0,
            (sku_grouped["rembursement_fee"] / sku_grouped["cm2_profit"]) * 100,
            0,
        )

        # ---------- previous / ratios ----------
        for col in ["previous_net_sales", "previous_net_credits", "previous_profit", "previous_quantity",
                    "previous_cost_of_unit_sold", "previous_amazon_fee", "previous_net_taxes",
                    "previous_fba_fees", "previous_selling_fees"]:
            if col in sku_grouped.columns:
                sku_grouped[col] = pd.to_numeric(sku_grouped[col], errors="coerce").fillna(0)

        sku_grouped["unit_wise_profitability"] = sku_grouped["profit"] / sku_grouped["quantity"]
        sku_grouped["unit_wise_profitability"] = sku_grouped["unit_wise_profitability"].replace([np.inf, -np.inf], 0).fillna(0)

        sku_grouped["previous_unit_wise_profitability"] = sku_grouped["previous_profit"] / sku_grouped["previous_quantity"]
        sku_grouped["previous_unit_wise_profitability"] = sku_grouped["previous_unit_wise_profitability"].replace([np.inf, -np.inf], 0).fillna(0)

        sku_grouped["unit_wise_profitability_percentage"] = (
            (sku_grouped["unit_wise_profitability"] - sku_grouped["previous_unit_wise_profitability"])
            / sku_grouped["previous_unit_wise_profitability"]
        ) * 100
        sku_grouped["unit_wise_profitability_percentage"] = sku_grouped["unit_wise_profitability_percentage"].replace([np.inf, -np.inf], 0).fillna(0)

        sku_grouped["unit_wise_profitability_growth"] = np.select(
            [
                sku_grouped["unit_wise_profitability_percentage"] >= 5,
                sku_grouped["unit_wise_profitability_percentage"] > 0.5,
                sku_grouped["unit_wise_profitability_percentage"] < -0.5
            ],
            ["High Growth", "Low Growth", "Negative Growth"],
            default="No Growth"
        )

        sku_grouped["cm1_profit"] = sku_grouped["profit%"].apply(lambda x: "High" if (x / 100) > 0.5 else "Low")
        sku_grouped["asp"] = sku_grouped["Net Sales"] / sku_grouped["total_quantity"]
        sku_grouped["asp"] = sku_grouped["asp"].replace([np.inf, -np.inf], 0).fillna(0)

        sku_grouped["previous_asp"] = sku_grouped["previous_net_sales"] / sku_grouped["previous_quantity"]
        sku_grouped["previous_asp"] = sku_grouped["previous_asp"].replace([np.inf, -np.inf], 0).fillna(0)

        sku_grouped["asp_percentag"] = (
            (sku_grouped["asp"] - sku_grouped["previous_asp"]) / sku_grouped["previous_asp"]
        ) * 100
        sku_grouped["asp_percentag"] = sku_grouped["asp_percentag"].replace([np.inf, -np.inf], 0).fillna(0)

        sku_grouped["asp_growth"] = np.select(
            [
                sku_grouped["asp_percentag"] >= 5,
                sku_grouped["asp_percentag"] > 0.5,
                sku_grouped["asp_percentag"] < -0.5
            ],
            ["High Growth", "Low Growth", "Negative Growth"],
            default="No Growth"
        )

        sku_grouped["text_credit_change"] = (sku_grouped["Net Taxes"] - sku_grouped["Net Credits"]) / sku_grouped["quantity"]
        sku_grouped["text_credit_change"] = sku_grouped["text_credit_change"].replace([np.inf, -np.inf], 0).fillna(0)

        sku_grouped["previous_text_credit_change"] = (
            (sku_grouped["previous_net_taxes"] - sku_grouped["previous_net_credits"]) / sku_grouped["previous_quantity"]
        )
        sku_grouped["previous_text_credit_change"] = sku_grouped["previous_text_credit_change"].replace([np.inf, -np.inf], 0).fillna(0)

        sku_grouped["profit_change"] = (
            (sku_grouped["profit"] - sku_grouped["previous_profit"]) / sku_grouped["previous_profit"]
        ) * 100
        sku_grouped["profit_change"] = sku_grouped["profit_change"].replace([np.inf, -np.inf], 0).fillna(0)

        sku_grouped["profit_growth"] = np.select(
            [
                sku_grouped["profit_change"] >= 5,
                sku_grouped["profit_change"] > 0.5,
                sku_grouped["profit_change"] < -0.5
            ],
            ["High Growth", "Low Growth", "Negative Growth"],
            default="No Growth"
        )

        sku_grouped["unit_increase"] = (
            (sku_grouped["quantity"] - sku_grouped["previous_quantity"]) / sku_grouped["previous_quantity"]
        ) * 100
        sku_grouped["unit_increase"] = sku_grouped["unit_increase"].replace([np.inf, -np.inf], 0).fillna(0)

        sku_grouped["unit_growth"] = np.select(
            [
                sku_grouped["unit_increase"] >= 5,
                sku_grouped["unit_increase"] > 0.5,
                sku_grouped["unit_increase"] < -0.5
            ],
            ["High Growth", "Low Growth", "Negative Growth"],
            default="No Growth"
        )

        sku_grouped, ads_table_applied = merge_monthly_ads_into_sku_grouped(
            conn, sku_grouped, user_id, country, month, year
        )
        if ads_table_applied:
            advertising_total = abs(pd.to_numeric(sku_grouped["advertising_total"], errors="coerce").fillna(0.0).sum())

        total_sales = abs(sku_grouped["Net Sales"].sum())
        total_profit = abs(sku_grouped["profit"].sum())
        total_previous_profit = abs(sku_grouped["previous_profit"].sum())
        total_previous_sales = abs(sku_grouped["previous_net_sales"].sum())
        total_amazon_fee = abs(sku_grouped["amazon_fee"].sum())
        total_cous = abs(sku_grouped["cost_of_unit_sold"].sum())
        total_fba_fees = abs(safe_series(sku_grouped, "fba_fees").sum())

        sku_grouped["sales_percentage"] = (
            (sku_grouped["Net Sales"] - sku_grouped["previous_net_sales"]) / sku_grouped["previous_net_sales"]
        ) * 100
        sku_grouped["sales_percentage"] = sku_grouped["sales_percentage"].replace([np.inf, -np.inf], 0).fillna(0)

        sku_grouped["sales_growth"] = np.select(
            [
                sku_grouped["sales_percentage"] >= 5,
                sku_grouped["sales_percentage"] > 0.5,
                sku_grouped["sales_percentage"] < -0.5
            ],
            ["High Growth", "Low Growth", "Negative Growth"],
            default="No Growth"
        )

        sku_grouped["sales_mix"] = (sku_grouped["Net Sales"] / total_sales) * 100
        sku_grouped["sales_mix"] = sku_grouped["sales_mix"].replace([np.inf, -np.inf], 0).fillna(0)

        sku_grouped["profit_mix"] = (sku_grouped["profit"] / total_profit) * 100
        sku_grouped["profit_mix"] = sku_grouped["profit_mix"].replace([np.inf, -np.inf], 0).fillna(0)

        sku_grouped["previous_profit_mix"] = (sku_grouped["previous_profit"] / total_previous_profit) * 100
        sku_grouped["previous_profit_mix"] = sku_grouped["previous_profit_mix"].replace([np.inf, -np.inf], 0).fillna(0)

        sku_grouped["profit_mix_percentage"] = (
            (sku_grouped["profit_mix"] - sku_grouped["previous_profit_mix"]) / sku_grouped["previous_profit_mix"]
        ) * 100
        sku_grouped["profit_mix_percentage"] = sku_grouped["profit_mix_percentage"].replace([np.inf, -np.inf], 0).fillna(0)

        sku_grouped["profit_mix_growth"] = np.select(
            [
                sku_grouped["profit_mix_percentage"] >= 5,
                sku_grouped["profit_mix_percentage"] > 0.5,
                sku_grouped["profit_mix_percentage"] < -0.5
            ],
            ["High Growth", "Low Growth", "Negative Growth"],
            default="No Growth"
        )

        sku_grouped["profit_mix_analysis"] = sku_grouped["profit_mix_percentage"].apply(lambda x: "High" if (x / 100) > 0.2 else "Low")
        sku_grouped["sales_mix_analysis"] = sku_grouped["sales_mix"].apply(lambda x: "High" if (x / 100) > 0.2 else "Low")

        sku_grouped["change_in_fee"] = (sku_grouped["amazon_fee"] / sku_grouped["Net Sales"]) * 100
        sku_grouped["change_in_fee"] = sku_grouped["change_in_fee"].replace([np.inf, -np.inf], 0).fillna(0)

        sku_grouped["previous_change_in_fee"] = (sku_grouped["previous_amazon_fee"] / sku_grouped["previous_net_sales"]) * 100
        sku_grouped["previous_change_in_fee"] = sku_grouped["previous_change_in_fee"].replace([np.inf, -np.inf], 0).fillna(0)

        sku_grouped["precentage_change_in_fee"] = sku_grouped["change_in_fee"] - sku_grouped["previous_change_in_fee"]

        sku_grouped["unit_wise_amazon_fee"] = (sku_grouped["amazon_fee"] - sku_grouped["Net Taxes"]) / sku_grouped["quantity"]
        sku_grouped["unit_wise_amazon_fee"] = sku_grouped["unit_wise_amazon_fee"].replace([np.inf, -np.inf], 0).fillna(0)

        sku_grouped["previous_unit_wise_amazon_fee"] = (
            (sku_grouped["previous_amazon_fee"] - sku_grouped["previous_net_taxes"]) / sku_grouped["previous_quantity"]
        )
        sku_grouped["previous_unit_wise_amazon_fee"] = sku_grouped["previous_unit_wise_amazon_fee"].replace([np.inf, -np.inf], 0).fillna(0)

        sku_grouped["unit_wise_amazon_fee_percentage"] = (
            (sku_grouped["unit_wise_amazon_fee"] - sku_grouped["previous_unit_wise_amazon_fee"])
            / sku_grouped["previous_unit_wise_amazon_fee"]
        ) * 100
        sku_grouped["unit_wise_amazon_fee_percentage"] = sku_grouped["unit_wise_amazon_fee_percentage"].replace([np.inf, -np.inf], 0).fillna(0)

        sku_grouped["amazon_fee_growth"] = np.select(
            [
                -sku_grouped["unit_wise_amazon_fee_percentage"] >= 5,
                -sku_grouped["unit_wise_amazon_fee_percentage"] > 0.5,
                -sku_grouped["unit_wise_amazon_fee_percentage"] < -0.5
            ],
            ["High Growth", "Low Growth", "Negative Growth"],
            default="No Growth"
        )

        sku_grouped["unit_sales_analysis"] = (
            (sku_grouped["quantity"] - sku_grouped["previous_quantity"]) * sku_grouped["unit_wise_profitability"]
        )
        sku_grouped["unit_sales_analysis"] = sku_grouped["unit_sales_analysis"].replace([np.inf, -np.inf], 0).fillna(0)

        sku_grouped["unit_asp_analysis"] = (
            (sku_grouped["asp"] - sku_grouped["previous_asp"]) * sku_grouped["quantity"]
        )
        sku_grouped["unit_asp_analysis"] = sku_grouped["unit_asp_analysis"].replace([np.inf, -np.inf], 0).fillna(0)

        sku_grouped["amazon_fee_increase"] = (
            (sku_grouped["previous_unit_wise_amazon_fee"] - sku_grouped["unit_wise_amazon_fee"]) * sku_grouped["quantity"]
        )
        sku_grouped["amazon_fee_increase"] = sku_grouped["amazon_fee_increase"].replace([np.inf, -np.inf], 0).fillna(0)

        sku_grouped["total_analysis"] = sku_grouped["profit"] - sku_grouped["previous_profit"]

        sku_grouped["text_credit_increase"] = (
            (sku_grouped["previous_text_credit_change"] - sku_grouped["text_credit_change"]) * sku_grouped["quantity"]
        )
        sku_grouped["text_credit_increase"] = sku_grouped["text_credit_increase"].replace([np.inf, -np.inf], 0).fillna(0)

        sku_grouped["final_total_analysis"] = (
            sku_grouped["amazon_fee_increase"]
            + sku_grouped["unit_asp_analysis"]
            + sku_grouped["unit_sales_analysis"]
            + sku_grouped["text_credit_increase"]
        )

        columns_to_sum = ["amazon_fee_increase", "unit_asp_analysis", "unit_sales_analysis", "text_credit_increase"]
        sku_grouped["positive_action"] = sku_grouped[columns_to_sum].apply(lambda row: row[row > 0].sum(), axis=1)
        sku_grouped["negative_action"] = sku_grouped[columns_to_sum].apply(lambda row: row[row < 0].sum(), axis=1)

        sku_grouped["cross_check_analysis"] = sku_grouped["total_analysis"] - sku_grouped["final_total_analysis"]
        sku_grouped["cross_check_analysis_backup"] = (
            (sku_grouped["positive_action"] + sku_grouped["negative_action"]) - sku_grouped["final_total_analysis"]
        )

        sku_grouped["previous_sales_mix"] = (sku_grouped["previous_net_sales"] / total_previous_sales) * 100
        sku_grouped["previous_sales_mix"] = sku_grouped["previous_sales_mix"].replace([np.inf, -np.inf], 0).fillna(0)

        sku_grouped["sales_mix_percentage"] = (
            (sku_grouped["sales_mix"] - sku_grouped["previous_sales_mix"]) / sku_grouped["previous_sales_mix"]
        ) * 100
        sku_grouped["sales_mix_percentage"] = sku_grouped["sales_mix_percentage"].replace([np.inf, -np.inf], 0).fillna(0)

        sku_grouped["sales_mix_growth"] = np.select(
            [
                sku_grouped["sales_mix_percentage"] >= 5,
                sku_grouped["sales_mix_percentage"] > 0.5,
                sku_grouped["sales_mix_percentage"] < -0.5
            ],
            ["High Growth", "Low Growth", "Negative Growth"],
            default="No Growth"
        )

        sku_grouped["category"] = ""
        sku_grouped["positive"] = ""
        sku_grouped["improvements"] = ""
        sku_grouped["month"] = month
        sku_grouped["year"] = year
        sku_grouped["country"] = country
        sku_grouped["user_id"] = user_id
        sku_grouped["promotional_rebates_percentage"] = np.where(
            sku_grouped["Net Sales"] != 0,
            (sku_grouped["promotional_rebates"] / sku_grouped["Net Sales"]) * 100,
            0
        )

        # ---------- total calculations ----------
        total_product_sales = abs(safe_series(sku_grouped, "product_sales").sum())
        total_tax = abs(sku_grouped["Net Taxes"].sum())
        total_credits = abs(sku_grouped["Net Credits"].sum())

        reimbursement_vs_sales = abs((rembursement_fee / total_sales) * 100) if total_sales != 0 else 0
        inventory_charges_and_reimbursement_total = (
            abs(fba_disposal_total) - abs(lost_total_amount)
        )
        # Use the report-level shipping total. Some shipping transactions have blank SKU,
        # so summing SKU rows can be zero/incomplete and overstate CM2.
        shipping_charges_total = (
            abs(placement_fee_total)
            + abs(customs_fee_total)
            + abs(shipment_fees)
        )
        cm2_profit = (
            total_profit
            - abs(advertising_total)
            - shipping_charges_total
            - abs(short_term_storage_fee_total + long_term_storage_fee_total)
            - inventory_charges_and_reimbursement_total
            - abs(platform_management_fees_total)
            + other_adjustment_total
        )
        # Do not derive report-level CM2 by summing SKU rows. SKU CM2 is
        # intentionally zero, while TOTAL CM2 uses the complete monthly charges.
        cm2_margins = (cm2_profit / total_sales) * 100 if total_sales != 0 else 0
        acos = (advertising_total / total_sales) * 100 if total_sales != 0 else 0
        rembursment_vs_cm2_margins = abs((rembursement_fee / cm2_profit) * 100) if cm2_profit != 0 else 0
        # Same as UK: SKU rows remain zero; TOTAL row receives the percentage.
        sku_grouped["cm2_profit_percentage"] = 0.0

        # ---------- total row ----------
        sum_row = sku_grouped.select_dtypes(include=[np.number]).sum()
        sum_row["sku"] = "TOTAL"
        sum_row["month"] = month
        sum_row["country"] = country
        sum_row["year"] = year
        sum_row["product_name"] = "TOTAL"

        # Gross Sales TOTAL must exactly equal the sum of SKU gross_sales rows.
        sum_row["gross_sales"] = round(
            pd.to_numeric(
                sku_grouped["gross_sales"],
                errors="coerce"
            ).fillna(0.0).sum(),
            2
        )

        # Recalculate percentage from TOTAL values.
        # Do not use the sum of individual SKU percentages.
        sum_row["promotional_rebates_percentage"] = (
            (
                float(sum_row["promotional_rebates"])
                / float(sum_row["Net Sales"])
            ) * 100
            if float(sum_row["Net Sales"]) != 0
            else 0
        )

        sum_row["profit%"] = (
            sum_row["profit"] / sum_row["Net Sales"]
        ) * 100 if sum_row["Net Sales"] != 0 else 0
        sum_row["shipment_fees"] = abs(shipment_fees)
        sum_row["shipping_charges"] = (
            abs(placement_fee_total)
            + abs(customs_fee_total)
            + abs(shipment_fees)
        )
        sum_row["debt_payment"] = abs(debt_payment_total)
        sum_row["disbursement"] = abs(disbursement_total)
        sum_row["rembursement_fee"] = abs(rembursement_fee)
        sum_row["visible_ads"] = visible_ads_total
        sum_row["dealsvouchar_ads"] = dealsvouchar_ads_total
        sum_row["advertising_total"] = advertising_total
        sum_row["platformfeenew"] = platformfeenew_total
        sum_row["platform_management_fees"] = platform_management_fees_total
        sum_row["platform_fee_inventory_storage"] = platform_fee_inventory_storage_total
        sum_row["short_term_storage_fee"] = short_term_storage_fee_total
        sum_row["long_term_storage_fee"] = long_term_storage_fee_total
        sum_row["storage_fee"] = short_term_storage_fee_total + long_term_storage_fee_total
        sum_row["other_adjustment"] = other_adjustment_total
        sum_row["fba_disposal"] = fba_disposal_total
        sum_row["placement_fee"] = placement_fee_total
        sum_row["customs_fee"] = customs_fee_total
        sum_row["misc_transaction"] = misc_transaction_total
        sum_row["profit"] = (
            sum_row["Net Sales"]
            - abs(sum_row["cost_of_unit_sold"])
            - abs(sum_row["amazon_fee"])
            - abs(sum_row["Net Taxes"])
            + sum_row["Net Credits"]
            + abs(sum_row["misc_transaction"])
        )
        sum_row["profit%"] = (
            sum_row["profit"] / sum_row["Net Sales"]
        ) * 100 if sum_row["Net Sales"] != 0 else 0
        sum_row["other_transaction_fees"] = _other_transaction_fees_value(
            sum_row.get("Net Taxes", 0.0),
            sum_row.get("Net Credits", 0.0),
            sum_row.get("misc_transaction", 0.0),
        )
        sum_row["lost_total"] = lost_total_amount
        sum_row["inventory_charges_and_reimbursement"] = (
            abs(fba_disposal_total) - abs(lost_total_amount)
        )
        sum_row["platform_fee"] = platform_fee
        sum_row["reimbursement_vs_sales"] = abs(reimbursement_vs_sales)
        sum_row["cm2_profit"] = abs(cm2_profit)
        sum_row["cm2_margins"] = abs(cm2_margins)
        sum_row["cm2_profit_percentage"] = (
            (sum_row["cm2_profit"] / sum_row["Net Sales"]) * 100
            if sum_row["Net Sales"] != 0 else 0
        )
        sum_row["acos"] = abs(acos)
        sum_row["rembursment_vs_cm2_margins"] = abs(rembursment_vs_cm2_margins)
        sum_row["previous_net_sales"] = a
        sum_row["previous_net_credits"] = b
        sum_row["previous_platform_fee"] = c
        sum_row["previous_rembursement_fee"] = d
        sum_row["previous_advertising_total"] = e
        sum_row["previous_reimbursement_vs_sales"] = f
        sum_row["previous_cm2_profit"] = g
        sum_row["previous_cm2_margins"] = h
        sum_row["previous_acos"] = i
        sum_row["previous_rembursment_vs_cm2_margins"] = j
        sum_row["previous_profit"] = k
        sum_row["previous_profit_percentage"] = l
        sum_row["previous_quantity"] = m
        sum_row["previous_cost_of_unit_sold"] = n
        sum_row["previous_amazon_fee"] = o
        sum_row["previous_net_taxes"] = p
        sum_row["previous_fba_fees"] = q
        sum_row["previous_selling_fees"] = r

        sum_row["unit_wise_profitability"] = (sum_row["profit"] / sum_row["quantity"]) if sum_row["quantity"] != 0 else 0
        sum_row["previous_unit_wise_profitability"] = (sum_row["previous_profit"] / sum_row["previous_quantity"]) if sum_row["previous_quantity"] != 0 else 0
        sum_row["unit_wise_profitability_percentage"] = (
            (sum_row["unit_wise_profitability"] - sum_row["previous_unit_wise_profitability"])
            / sum_row["previous_unit_wise_profitability"]
        ) * 100 if sum_row["previous_unit_wise_profitability"] != 0 else 0
        sum_row["unit_increase"] = (
            (sum_row["quantity"] - sum_row["previous_quantity"]) / sum_row["previous_quantity"]
        ) * 100 if sum_row["previous_quantity"] != 0 else 0
        sum_row["asp"] = (
            sum_row["Net Sales"] / sum_row["total_quantity"]
        ) if sum_row["total_quantity"] != 0 else 0
        sum_row["previous_asp"] = (sum_row["previous_net_sales"] / sum_row["previous_quantity"]) if sum_row["previous_quantity"] != 0 else 0
        sum_row["asp_percentag"] = (
            (sum_row["asp"] - sum_row["previous_asp"]) / sum_row["previous_asp"]
        ) * 100 if sum_row["previous_asp"] != 0 else 0
        sum_row["change_in_fee"] = (sum_row["amazon_fee"] / sum_row["Net Sales"]) * 100 if sum_row["Net Sales"] != 0 else 0
        sum_row["previous_change_in_fee"] = (
            (sum_row["previous_amazon_fee"] / sum_row["previous_net_sales"]) * 100
        ) if sum_row["previous_net_sales"] != 0 else 0
        sum_row["precentage_change_in_fee"] = sum_row["change_in_fee"] - sum_row["previous_change_in_fee"]
        sum_row["unit_wise_amazon_fee"] = (
            (sum_row["amazon_fee"] - sum_row["Net Taxes"]) / sum_row["quantity"]
        ) if sum_row["quantity"] != 0 else 0
        sum_row["previous_unit_wise_amazon_fee"] = (
            (sum_row["previous_amazon_fee"] - sum_row["previous_net_taxes"]) / sum_row["previous_quantity"]
        ) if sum_row["previous_quantity"] != 0 else 0
        sum_row["unit_wise_amazon_fee_percentage"] = (
            (sum_row["unit_wise_amazon_fee"] - sum_row["previous_unit_wise_amazon_fee"])
            / sum_row["previous_unit_wise_amazon_fee"]
        ) * 100 if sum_row["previous_unit_wise_amazon_fee"] != 0 else 0
        sum_row["profit_change"] = (
            (sum_row["profit"] - sum_row["previous_profit"]) / sum_row["previous_profit"]
        ) * 100 if sum_row["previous_profit"] != 0 else 0
        sum_row["profit_mix_percentage"] = (
            (sum_row["profit_mix"] - sum_row["previous_profit_mix"]) / sum_row["previous_profit_mix"]
        ) * 100 if sum_row["previous_profit_mix"] != 0 else 0
        sum_row["sales_percentage"] = (
            (sum_row["Net Sales"] - sum_row["previous_net_sales"]) / sum_row["previous_net_sales"]
        ) * 100 if sum_row["previous_net_sales"] != 0 else 0
        sum_row["text_credit_change"] = (
            (float(sum_row["Net Credits"]) + float(sum_row["profit"])) / float(sum_row["Net Sales"])
        ) if float(sum_row["Net Sales"]) != 0 else 0
        sum_row["previous_text_credit_change"] = (
            (float(sum_row["previous_net_credits"]) + float(sum_row["previous_profit"])) / float(sum_row["previous_net_sales"])
        ) if float(sum_row["previous_net_sales"]) != 0 else 0

        # growth labels for total row
        sum_row["unit_wise_profitability_growth"] = (
            "High Growth" if sum_row["unit_wise_profitability_percentage"] >= 5
            else "Low Growth" if sum_row["unit_wise_profitability_percentage"] > 0.5
            else "Negative Growth" if sum_row["unit_wise_profitability_percentage"] < -0.5
            else "No Growth"
        )
        sum_row["asp_growth"] = (
            "High Growth" if sum_row["asp_percentag"] >= 5
            else "Low Growth" if sum_row["asp_percentag"] > 0.5
            else "Negative Growth" if sum_row["asp_percentag"] < -0.5
            else "No Growth"
        )
        sum_row["profit_growth"] = (
            "High Growth" if sum_row["profit_change"] >= 5
            else "Low Growth" if sum_row["profit_change"] > 0.5
            else "Negative Growth" if sum_row["profit_change"] < -0.5
            else "No Growth"
        )
        sum_row["unit_growth"] = (
            "High Growth" if sum_row["unit_increase"] >= 5
            else "Low Growth" if sum_row["unit_increase"] > 0.5
            else "Negative Growth" if sum_row["unit_increase"] < -0.5
            else "No Growth"
        )
        sum_row["profit_mix_growth"] = (
            "High Growth" if sum_row["profit_mix_percentage"] >= 5
            else "Low Growth" if sum_row["profit_mix_percentage"] > 0.5
            else "Negative Growth" if sum_row["profit_mix_percentage"] < -0.5
            else "No Growth"
        )
        sum_row["sales_growth"] = (
            "High Growth" if sum_row["sales_percentage"] >= 5
            else "Low Growth" if sum_row["sales_percentage"] > 0.5
            else "Negative Growth" if sum_row["sales_percentage"] < -0.5
            else "No Growth"
        )
        sum_row["amazon_fee_growth"] = (
            "High Growth" if -sum_row["unit_wise_amazon_fee_percentage"] >= 5
            else "Low Growth" if -sum_row["unit_wise_amazon_fee_percentage"] > 0.5
            else "Negative Growth" if -sum_row["unit_wise_amazon_fee_percentage"] < -0.5
            else "No Growth"
        )
        sum_row["sales_mix_growth"] = (
            "High Growth" if sum_row["sales_mix_percentage"] >= 5
            else "Low Growth" if sum_row["sales_mix_percentage"] > 0.5
            else "Negative Growth" if sum_row["sales_mix_percentage"] < -0.5
            else "No Growth"
        )

        sum_row["sales_mix_analysis"] = "High" if (sum_row["sales_mix"] / 100) > 0.2 else "Low"
        sum_row["profit_mix_analysis"] = "High" if (sum_row["profit_mix"] / 100) > 0.2 else "Low"
        sum_row["cm1_profit"] = "High" if (sum_row["profit%"] / 100) > 0.5 else "Low"
        sum_row["category"] = ""
        sum_row["positive"] = ""
        sum_row["improvements"] = ""
        sum_row["user_id"] = user_id

        sku_grouped = pd.concat([sku_grouped, pd.DataFrame([sum_row])], ignore_index=True)

        # ---------- create tables ----------
        create_us_nse_full_table(target_table)      # nse_{user_id}_{country}_{month}{year}
        create_monthly_table(target_table_nse)      # skuwisemonthly_{user_id}_{country}_{month}{year}
        create_monthly_table(target_table2)
        create_monthly_table(target_table_us)
        create_monthly_table(target_table_ind)
        create_monthly_table(target_table_can)
        create_monthly_table(target_table_gbp)

        for _report_table in [
            target_table_nse, target_table2, target_table_us,
            target_table_ind, target_table_can, target_table_gbp,
        ]:
            ensure_report_compat_db_columns(conn, _report_table)

        # ---------- sanitize ----------
        for col in sku_grouped.columns:
            if pd.api.types.is_numeric_dtype(sku_grouped[col]):
                sku_grouped[col] = pd.to_numeric(sku_grouped[col], errors="coerce").fillna(0)
            else:
                sku_grouped[col] = sku_grouped[col].fillna("")

        sku_grouped.columns = [c.lower() for c in sku_grouped.columns]
        sku_grouped = sku_grouped.rename(columns={
            "net sales": "net_sales",
            "net taxes": "net_taxes",
            "net credits": "net_credits",
            "profit%": "profit_percentage",
            "shipping_charges": "shipping_charges"
        })

        # ------------------------------------------------------------------
        # FIX: after lower()/rename(), columns like "Net Taxes" and
        # "net_taxes" can become duplicate "net_taxes" columns.
        # If df[col] returns a DataFrame, pd.to_numeric() raises:
        # TypeError: arg must be a list, tuple, 1-d array, or Series
        # Coalesce duplicate columns before any numeric conversion/export.
        # ------------------------------------------------------------------
        def coalesce_duplicate_columns(df_: pd.DataFrame) -> pd.DataFrame:
            if df_.columns.is_unique:
                return df_

            out = pd.DataFrame(index=df_.index)
            for col_name in dict.fromkeys(df_.columns):
                block = df_.loc[:, df_.columns == col_name]
                if isinstance(block, pd.Series):
                    out[col_name] = block
                elif block.shape[1] == 1:
                    out[col_name] = block.iloc[:, 0]
                else:
                    # Keep first non-null value across duplicates, row-wise.
                    out[col_name] = block.bfill(axis=1).iloc[:, 0]
            return out.copy()

        sku_grouped = coalesce_duplicate_columns(sku_grouped)

        sku_grouped = _make_us_display_expenses_positive(sku_grouped)
        # Full detailed NSE dataframe for nse_{user_id}_{country}_{month}{year}
        df_nse_full = sku_grouped.copy()
        
        US_NSE_FULL_COLUMNS = [
            "product_name", "sku", "quantity", "return_quantity", "total_quantity",

            "product_sales", "product_sales_tax", "postage_credits",
            "shipping_credits", "shipping_credits_tax",
            "gift_wrap_credits", "giftwrap_credits_tax",

            "promotional_rebates", "promotional_rebates_tax",
            "promotional_rebates_percentage",

            "gross_sales", "refund_sales", "tex_and_credits", "net_sales",

            "price_in_gbp", "cost_of_unit_sold",

            "marketplace_facilitator_tax", "other_transaction_fees",
            "selling_fees", "refund_selling_fees", "fba_fees",
            "other", "total", "amazon_fee",

            "net_taxes", "net_credits", "misc_transaction",

            "profit", "unit_wise_profitability", "profit_percentage",

            "lost_total", "visible_ads", "dealsvouchar_ads", "advertising_total",

            "platformfeenew", "platform_management_fees", "platform_fee", "platform_fee_inventory_storage",
            "short_term_storage_fee", "long_term_storage_fee", "storage_fee", "other_adjustment", "fba_disposal", "inventory_charges_and_reimbursement", "placement_fee", "customs_fee",

            "shipping_charges", "shipment_fees",

            "cm2_profit", "cm2_profit_percentage", "cm2_margins", "acos",
            "debt_payment", "disbursement", "rembursement_fee", "rembursment_vs_cm2_margins",
            "reimbursement_vs_sales",

            "sales_mix", "profit_mix",

            "errorstatus", "answer", "difference",

            "month", "year", "country", "user_id"
        ]

        TEXT_COLS_US_NSE = {
            "product_name", "sku", "errorstatus", "month", "year", "country"
        }

        for col in US_NSE_FULL_COLUMNS:
            if col not in df_nse_full.columns:
                df_nse_full[col] = "" if col in TEXT_COLS_US_NSE else 0

        df_nse_full = coalesce_duplicate_columns(df_nse_full)

        for col in US_NSE_FULL_COLUMNS:
            # Extra guard: if any future merge creates a duplicate again,
            # convert the first duplicate/non-null column instead of crashing.
            col_data = df_nse_full[col]
            if isinstance(col_data, pd.DataFrame):
                col_data = col_data.bfill(axis=1).iloc[:, 0]

            if col in TEXT_COLS_US_NSE:
                df_nse_full[col] = col_data.astype(str).fillna("")
            else:
                df_nse_full[col] = pd.to_numeric(col_data, errors="coerce").fillna(0)

        df_nse_full = df_nse_full[US_NSE_FULL_COLUMNS]

        sku_grouped = add_report_compat_columns(sku_grouped)

        final_columns = [
            "sku", "product_name", "quantity", "return_quantity", "total_quantity",
            "asp", "gross_sales", "refund_sales", "tex_and_credits", "net_sales",
            "promotional_rebates", "promotional_rebates_percentage",
            "cost_of_unit_sold", "selling_fees", "fba_fees", "amazon_fee",
            "net_taxes", "net_credits", "misc_transaction", "other_transaction_fees",
            "profit", "unit_wise_profitability", "profit_percentage",
            "visible_ads", "dealsvouchar_ads", "advertising_total", "lost_total",
            "platformfeenew", "platform_management_fees", "platform_fee", "platform_fee_inventory_storage", "short_term_storage_fee", "long_term_storage_fee", "storage_fee", "other_adjustment", "fba_disposal", "inventory_charges_and_reimbursement", "placement_fee", "customs_fee", "shipping_charges", "shipment_fees",
            "cm2_profit", "cm2_profit_percentage","cm2_margins", "acos", "debt_payment", "disbursement","rembursement_fee",
            "rembursment_vs_cm2_margins", "reimbursement_vs_sales",
            "sales_mix", "profit_mix","month", "year", "country", "user_id"
        ]
        final_columns = list(dict.fromkeys(final_columns + REPORT_COMPAT_COLUMNS))

        for col in final_columns:
            if col not in sku_grouped.columns:
                sku_grouped[col] = 0

        sku_grouped = sku_grouped[final_columns]

        # ---------- save local ----------
        # Full detailed NSE table
        safe_to_sql(df_nse_full, target_table, conn, if_exists="append", index=False, method="multi", chunksize=100)

        # Reduced monthly summary table
        safe_to_sql(sku_grouped, target_table_nse, conn, if_exists="append", index=False, method="multi", chunksize=100)

        # Rolling country table
        safe_to_sql(sku_grouped, target_table2, conn, if_exists="replace", index=False, method="multi", chunksize=100)

        # ---------- currency conversion ----------
        rate_us = 1.0
        rate_ind = 1.0
        rate_can = 1.0
        rate_gbp = 1.0

        try:
            q = text("""
                SELECT lower(country) AS country, conversion_rate
                FROM currency_conversion
                WHERE lower(month) = :month AND year = :year
            """)
            currency_df = pd.read_sql(q, engine1, params={"month": month.lower(), "year": str(year)})
            if not currency_df.empty:
                map_rate = dict(zip(currency_df["country"], currency_df["conversion_rate"]))
                rate_us = float(map_rate.get("us", 1.0) or 1.0)
                rate_ind = float(map_rate.get("india", 1.0) or 1.0)
                rate_can = float(map_rate.get("canada", 1.0) or 1.0)
                rate_gbp = float(map_rate.get("uk", 1.0) or 1.0)
        except Exception:
            pass

        monetary_columns = [
            "asp", "gross_sales", "refund_sales", "tex_and_credits", "net_sales",
            "promotional_rebates","cost_of_unit_sold", "selling_fees", "fba_fees", "amazon_fee",
            "net_taxes", "net_credits", "misc_transaction", "other_transaction_fees",
            "profit", "unit_wise_profitability", "profit_percentage",
            "visible_ads", "dealsvouchar_ads", "advertising_total", "lost_total",
            "platformfeenew", "platform_management_fees", "platform_fee", "platform_fee_inventory_storage", "short_term_storage_fee", "long_term_storage_fee", "storage_fee", "other_adjustment", "fba_disposal", "inventory_charges_and_reimbursement", "placement_fee", "customs_fee", "shipping_charges", "shipment_fees",
            "cm2_profit", "cm2_profit_percentage","cm2_margins", "acos","debt_payment", "disbursement", "rembursement_fee",
            "rembursment_vs_cm2_margins", "reimbursement_vs_sales",
            "sales_mix", "profit_mix"
        ]

        def apply_rate(df_conv, rate):
            for col in monetary_columns:
                if col in df_conv.columns:
                    df_conv[col] = pd.to_numeric(df_conv[col], errors="coerce").fillna(0) * rate
            return df_conv

        df_usd = apply_rate(sku_grouped.copy(), rate_us)
        df_ind = apply_rate(sku_grouped.copy(), rate_ind)
        df_can = apply_rate(sku_grouped.copy(), rate_can)
        df_gbp = apply_rate(sku_grouped.copy(), rate_gbp)

        df_usd["country"] = "us"
        df_ind["country"] = "india"
        df_can["country"] = "canada"
        df_gbp["country"] = "gbp"
        for df_currency in [df_usd, df_ind, df_can, df_gbp]:
            df_currency["month"] = month.lower()
            df_currency["year"] = str(year)

        safe_to_sql(df_usd, target_table_us, conn, if_exists="append", index=False, method="multi", chunksize=100)
        safe_to_sql(df_ind, target_table_ind, conn, if_exists="append", index=False, method="multi", chunksize=100)
        safe_to_sql(df_can, target_table_can, conn, if_exists="append", index=False, method="multi", chunksize=100)
        safe_to_sql(df_gbp, target_table_gbp, conn, if_exists="append", index=False, method="multi", chunksize=100)

        try:
            conn.commit()
        except Exception:
            pass

        otherwplatform = abs(sum_row["platform_fee"]) + abs(sum_row["rembursement_fee"]) + abs(sum_row["shipping_charges"])
        taxncredit = abs(sum_row["tex_and_credits"])
        total_expense = abs(total_cous) + abs(total_amazon_fee) + abs(sum_row["platform_fee"]) + abs(sum_row["advertising_total"]) + abs(sum_row["shipping_charges"])
        total_product_sales = abs(sum_row.get("product_sales", 0))

        return (
            abs(sum_row["platform_fee"]),
            abs(rembursement_fee),
            abs(total_cous),
            abs(total_amazon_fee),
            abs(total_profit),
            abs(total_expense),
            abs(total_fba_fees),
            abs(cm2_profit),
            abs(cm2_margins),
            abs(acos),
            abs(rembursment_vs_cm2_margins),
            abs(sum_row["advertising_total"]),
            abs(reimbursement_vs_sales),
            int(sum_row.get("total_quantity", sku_grouped["total_quantity"].sum())),
            abs(total_sales),
            abs(otherwplatform),
            abs(taxncredit),
            abs(total_product_sales),
        )

    except Exception as e:
        print(f"Error processing SKU-wise US data: {e}")
        raise
    finally:
        try:
            conn.close()
        except Exception:
            pass

    
def process_us_yearly_skuwise_data(user_id, country, year):
    engine = create_engine(db_url)
    conn = engine.connect()

    yearly_table = f"skuwiseyearly_{user_id}_{country}_{year}_table"
    source_table = f"user_{user_id}_{country}_merge_data_of_all_months"

    def safe_series(df_, col, default=0.0):
        if col in df_.columns:
            return pd.to_numeric(df_[col], errors="coerce").fillna(default)
        return pd.Series(default, index=df_.index, dtype="float64")

    def sum_total_where_desc_contains(df_, keywords):
        if "total" not in df_.columns:
            return 0.0
        desc = df_.get("description", pd.Series("", index=df_.index)).astype(str)
        pattern = "|".join(re.escape(k) for k in keywords)
        mask = desc.str.contains(pattern, case=False, na=False, regex=True)
        return float(pd.to_numeric(df_.loc[mask, "total"], errors="coerce").fillna(0).sum())

    def sku_sum_total_where_desc_contains(df_, keywords, out_col):
        if "total" not in df_.columns:
            return pd.DataFrame(columns=["sku", out_col])

        desc = df_.get("description", pd.Series("", index=df_.index)).astype(str)
        pattern = "|".join(re.escape(k) for k in keywords)
        mask = desc.str.contains(pattern, case=False, na=False, regex=True)

        out = (
            df_.loc[
                mask
                & df_["sku"].notna()
                & (df_["sku"].astype(str).str.strip() != "")
                & (df_["sku"].astype(str).str.strip() != "0")
            ]
            .groupby("sku", as_index=False)["total"]
            .sum()
            .rename(columns={"total": out_col})
        )
        out[out_col] = pd.to_numeric(out[out_col], errors="coerce").fillna(0.0)
        out["sku"] = out["sku"].astype(str).str.strip()
        return out

    try:
        df = pd.read_sql(
            text(f"SELECT * FROM {source_table} WHERE year = :year"),
            conn,
            params={"year": str(year)}
        )

        if df.empty:
            print(f"No data found for year {year} in {source_table}")
            return

        likely_text_cols = ["sku", "type", "description", "marketplace", "fulfilment", "product_name", "errorstatus"]
        for col in likely_text_cols:
            if col in df.columns:
                df[col] = df[col].astype(str)

        for col in [
            "platform_fees", "net_reimbursement", "shipping_credits",
            "postage_credits", "gift_wrap_credits", "answer", "difference"
        ]:
            if col not in df.columns:
                df[col] = 0.0

        numeric_columns = [
            "product_sales", "promotional_rebates", "promotional_rebates_tax",
            "product_sales_tax", "selling_fees", "fba_fees", "other",
            "marketplace_facilitator_tax", "shipping_credits_tax",
            "giftwrap_credits_tax", "postage_credits", "shipping_credits",
            "gift_wrap_credits", "price_in_gbp", "cost_of_unit_sold",
            "quantity", "total", "other_transaction_fees", "platform_fees",
            "net_reimbursement", "answer", "difference"
        ]
        numeric_columns = [col for col in numeric_columns if col in df.columns]
        df[numeric_columns] = df[numeric_columns].apply(pd.to_numeric, errors="coerce").fillna(0)

        df["sku"] = df["sku"].astype(str).str.strip()
        df["type_norm"] = df.get("type", pd.Series("", index=df.index)).astype(str).str.strip().str.lower()
        df["desc_norm"] = df.get("description", pd.Series("", index=df.index)).astype(str).str.strip()

        df_valid = df[
            df["sku"].notna()
            & (df["sku"] != "")
            & (df["sku"] != "0")
            & (df["sku"].str.lower() != "none")
        ].copy()

        debt_payment_total = abs(sum_total_where_desc_contains(df, ["DebtPayment"]))
        disbursement_total = abs(sum_total_where_desc_contains(df, ["Disbursement"]))

        rembursement_fee = disbursement_total + safe_series(df, "net_reimbursement").sum()

        shipment_keywords = [
            "FBA international shipping charge",
            "FBA Inbound Placement Service Fee",
            "FBA international shipping customs charge",
        ]
        shipment_mask = df["desc_norm"].str.contains("|".join(shipment_keywords), case=False, na=False)
        shipment_charges = abs(safe_series(df.loc[shipment_mask], "total").sum())
        shipment_fees = abs(sum_total_where_desc_contains(
            df,
            ["AWDTransportationFee"],
        ))

        visible_ads_total = abs(sum_total_where_desc_contains(df, ["ProductAdsPayment"]))
        dealsvouchar_ads_total = abs(sum_total_where_desc_contains(df, [
            "Cost of Advertising",
            "Coupon Redemption Fee",
            "Deals",
            "Lightning Deal",
            "CouponPerformanceEvent",
            "CouponParticipationEvent",
            "SellerDealComplete",
            "VineCharge",
            "SellerPoweredCoupon",
            "DealParticipationEvent",
            "DealPerformanceEvent",
        ]))
        advertising_total = visible_ads_total + dealsvouchar_ads_total

        refund_fees = (
            df[df["type_norm"] == "refund"]
            .groupby("sku", as_index=False)["selling_fees"]
            .sum()
            .rename(columns={"selling_fees": "refund_selling_fees"})
        )
        refund_fees["sku"] = refund_fees["sku"].astype(str).str.strip()
        # FBA fees: only Shipment and Refund
        fba_fees_df = (
            df.loc[
                df["type_norm"].isin(["shipment", "refund"])
                & df["sku"].notna()
                & (df["sku"].astype(str).str.strip() != "")
                & (df["sku"].astype(str).str.strip() != "0")
                & (df["sku"].astype(str).str.lower() != "none"),
                ["sku", "fba_fees"]
            ]
            .groupby("sku", as_index=False)["fba_fees"]
            .sum()
        )

        fba_fees_df["sku"] = fba_fees_df["sku"].astype(str).str.strip()
        fba_fees_df["fba_fees"] = pd.to_numeric(
            fba_fees_df["fba_fees"],
            errors="coerce"
        ).fillna(0.0)

        quantity_df = (
            df[df["type_norm"].isin(["order", "shipment"])]
            .groupby("sku", as_index=False)["quantity"]
            .sum()
        )

        return_qty_df = (
            df[df["type_norm"] == "refund"]
            .groupby("sku", as_index=False)["quantity"]
            .sum()
            .rename(columns={"quantity": "return_quantity"})
        )
        if not return_qty_df.empty:
            return_qty_df["return_quantity"] = pd.to_numeric(
                return_qty_df["return_quantity"], errors="coerce"
            ).fillna(0).abs()

        LOST_DESCRIPTIONS = {
            "REVERSAL_REIMBURSEMENT",
            "WAREHOUSE_LOST",
            "WAREHOUSE_DAMAGE",
            "MISSING_FROM_INBOUND",
            "FREE_REPLACEMENT_REFUND_ITEMS",
        }

        lost_mask = df["desc_norm"].isin(LOST_DESCRIPTIONS)
        lost_total_df = (
            df.loc[lost_mask]
            .groupby("sku", as_index=False)["total"]
            .sum()
            .rename(columns={"total": "lost_total"})
        )
        if not lost_total_df.empty:
            lost_total_df["lost_total"] = pd.to_numeric(
                lost_total_df["lost_total"], errors="coerce"
            ).fillna(0)

        # ---------- misc transaction: SKU-wise + blank-SKU total ----------

        def _norm_misc_key(x):
            return re.sub(r"\s+", " ", str(x or "").strip()).casefold()

        df["sku"] = df["sku"].fillna("").astype(str).str.strip()
        df["desc_norm"] = df.get("description", pd.Series("", index=df.index)).fillna("").astype(str).str.strip()
        df["type_norm"] = (
            df.get("type", pd.Series("", index=df.index))
            .fillna("")
            .astype(str)
            .str.strip()
            .str.lower()
        )

        df["desc_key"] = df["desc_norm"].map(_norm_misc_key)
        df["type_key"] = df["type_norm"].map(_norm_misc_key)
        df["total"] = pd.to_numeric(df.get("total", 0), errors="coerce").fillna(0.0)

        EXCLUDE_DESCRIPTIONS = {
            # Advertising / deals
            "Cost of Advertising",
            "Coupon Redemption Fee",
            "Deals",
            "Lightning Deal",
            "ProductAdsPayment",
            "CouponPerformanceEvent",
            "CouponParticipationEvent",
            "SellerDealComplete",
            "VineCharge",
            "SellerPoweredCoupon",
            "DealParticipationEvent",
            "DealPerformanceEvent",

            # Platform / storage / shipment buckets
            "FBA Return Fee",
            "FBA Long-Term Storage Fee",
            "FBA storage fee",
            "FBADisposal",
            "FBAStorageBilling",
            "FBALongTermStorageBilling",
            "INCORRECT_FEES_NON_ITEMIZED",
            "StorageReservationBilling",
            "FBAStorageFeeAdjustment",
            "Subscription",
            "PaidServicesCharge",
            "FBAInboundConvenience",
            "AWDProcessingFee",
            "AWDTransportationFee",
            "AGSGlobalInboundTransportation",

            # Normal payment / refund / transfer buckets
            "Order Payment",
            "Refund",
            "Disbursement",
            "DebtPayment",

            # Lost / reimbursement bucket
            "REVERSAL_REIMBURSEMENT",
            "WAREHOUSE_LOST",
            "WAREHOUSE_DAMAGE",
            "MISSING_FROM_INBOUND",
            "FREE_REPLACEMENT_REFUND_ITEMS",
        }

        EXCLUDE_TYPES = {
            "Transfer",
            "Refund",
        }

        exclude_desc_keys = {_norm_misc_key(x) for x in EXCLUDE_DESCRIPTIONS}
        exclude_type_keys = {_norm_misc_key(x) for x in EXCLUDE_TYPES}

        leftout_mask = (
            ~df["desc_key"].isin(exclude_desc_keys)
            & ~df["type_key"].isin(exclude_type_keys)
        )

        all_misc_transaction_total = (
            pd.to_numeric(
                df.loc[leftout_mask, "total"],
                errors="coerce"
            )
            .fillna(0.0)
            .sum()
        )

        # misc_transaction is SKU-wise only; blank/unassigned misc rows move to other_adjustment.
        tmp_misc = df.loc[
            leftout_mask
            & df["sku"].notna()
            & (df["sku"] != "")
            & (df["sku"] != "0")
            & (df["sku"].str.lower() != "none"),
            ["sku", "total"]
        ].copy()

        misc_transaction_df = (
            tmp_misc.groupby("sku", as_index=False)["total"]
            .sum()
            .rename(columns={"total": "misc_transaction"})
        )

        misc_transaction_df["misc_transaction"] = pd.to_numeric(
            misc_transaction_df["misc_transaction"],
            errors="coerce"
        ).fillna(0.0)

        misc_transaction_total = float(misc_transaction_df["misc_transaction"].sum())
        unassigned_misc_transaction_total = all_misc_transaction_total - misc_transaction_total

        platformfeenew_total = abs(sum_total_where_desc_contains(df, ["Subscription"]))

        # Platform management fees include paid services and subscription charges.
        PLATFORM_MANAGEMENT_FEE_KEYWORDS = [
            "PaidServicesCharge",
            "Subscription",
        ]
        platform_management_fees_total = abs(sum_total_where_desc_contains(
            df,
            PLATFORM_MANAGEMENT_FEE_KEYWORDS,
        ))
        platform_management_fees_df = sku_sum_total_where_desc_contains(
            df,
            PLATFORM_MANAGEMENT_FEE_KEYWORDS,
            "platform_management_fees",
        )
        if not platform_management_fees_df.empty:
            platform_management_fees_df["platform_management_fees"] = pd.to_numeric(
                platform_management_fees_df["platform_management_fees"],
                errors="coerce",
            ).fillna(0.0).abs()

        platform_fee_inventory_storage_total = abs(sum_total_where_desc_contains(df, [
            "FBA Return Fee",
            "FBA Long-Term Storage Fee",
            "FBA storage fee",
            "FBADisposal",
            "FBAStorageBilling",
            "FBALongTermStorageBilling",
            "INCORRECT_FEES_NON_ITEMIZED",
            "StorageReservationBilling",
            "FBAStorageFeeAdjustment",
        ]))
        short_term_storage_fee_total = abs(sum_total_where_desc_contains(df, [
            "FBAStorageBilling"
        ]))

        long_term_storage_fee_total = abs(sum_total_where_desc_contains(df, [
            "FBALongTermStorageBilling"
        ]))

        fba_disposal_total = sum_total_where_desc_contains(df, [
            "FBADisposal",
            "MISSING_FROM_INBOUND_CLAWBACK",
            "COMPENSATED_CLAWBACK",
        ])

        other_adjustment_total = (
            abs(sum_total_where_desc_contains(df, ["FBAStorageFeeAdjustment"]))
            + abs(unassigned_misc_transaction_total)
        )

        other_adjustment_df = sku_sum_total_where_desc_contains(
            df,
            ["FBAStorageFeeAdjustment"],
            "other_adjustment",
        )
        if not other_adjustment_df.empty:
            other_adjustment_df["other_adjustment"] = pd.to_numeric(
                other_adjustment_df["other_adjustment"],
                errors="coerce",
            ).fillna(0.0).abs()

        # Placement fees preserve the source sign (normally negative).
        PLACEMENT_FEE_KEYWORDS = [
            "FBAInboundConvenience",
            "AWDProcessingFee",
        ]
        placement_fee_total = sum_total_where_desc_contains(
            df,
            PLACEMENT_FEE_KEYWORDS,
        )
        placement_fee_df = sku_sum_total_where_desc_contains(
            df,
            PLACEMENT_FEE_KEYWORDS,
            "placement_fee",
        )


        # Customs fees: AGS global inbound transportation.
        # Preserve the original source sign (normally negative).
        CUSTOMS_FEE_KEYWORDS = [
            "AGSGlobalInboundTransportation",
        ]
        customs_fee_total = sum_total_where_desc_contains(
            df,
            CUSTOMS_FEE_KEYWORDS,
        )
        customs_fee_df = sku_sum_total_where_desc_contains(
            df,
            CUSTOMS_FEE_KEYWORDS,
            "customs_fee",
        )
        if not customs_fee_df.empty:
            customs_fee_df["customs_fee"] = pd.to_numeric(
                customs_fee_df["customs_fee"],
                errors="coerce",
            ).fillna(0.0)

        shipment_fees_df = sku_sum_total_where_desc_contains(
            df,
            ["AWDTransportationFee"],
            "shipment_fees",
        )
        if not shipment_fees_df.empty:
            shipment_fees_df["shipment_fees"] = pd.to_numeric(
                shipment_fees_df["shipment_fees"],
                errors="coerce",
            ).fillna(0.0).abs()

        short_term_storage_fee_df = sku_sum_total_where_desc_contains(
            df,
            ["FBAStorageBilling"],
            "short_term_storage_fee"
        )

        long_term_storage_fee_df = sku_sum_total_where_desc_contains(
            df,
            ["FBALongTermStorageBilling"],
            "long_term_storage_fee"
        )

        fba_disposal_df = sku_sum_total_where_desc_contains(
            df,
            [
                "FBADisposal",
                "MISSING_FROM_INBOUND_CLAWBACK",
                "COMPENSATED_CLAWBACK",
            ],
            "fba_disposal"
        )


        lost_total_amount = abs(
            pd.to_numeric(
                lost_total_df.get("lost_total", pd.Series(dtype=float)),
                errors="coerce"
            ).fillna(0).sum()
        )

        platform_fee = (
            platformfeenew_total
            + platform_fee_inventory_storage_total
            - misc_transaction_total
            - lost_total_amount
        )

        group_cols = {
            "price_in_gbp": "mean",
            "product_sales": "sum",
            "promotional_rebates": "sum",
            "promotional_rebates_tax": "sum",
            "product_sales_tax": "sum",
            "selling_fees": "sum",
            "other": "sum",
            "marketplace_facilitator_tax": "sum",
            "shipping_credits_tax": "sum",
            "giftwrap_credits_tax": "sum",
            "postage_credits": "sum",
            "shipping_credits": "sum",
            "gift_wrap_credits": "sum",
            "cost_of_unit_sold": "sum",
            "total": "sum",
            "ads_impressions": "sum",
            "ads_clicks": "sum",
            "ads_spend_raw": "sum",
            "ads_sale_units": "sum",
            "ads_sale_amount": "sum",
            "sp_ads_sales": "sum",
            "sd_ads_sales": "sum",
            "sb_ads_sales": "sum",
            "product_spend": "sum",
            "display_spend": "sum",
            "brand_spend": "sum",
            "ad_type": "first",
            "product_name": "first",
        }
        group_cols = {k: v for k, v in group_cols.items() if k in df.columns}

        sku_grouped = df_valid.groupby("sku").agg(group_cols).reset_index()
        sku_grouped["sku"] = sku_grouped["sku"].astype(str).str.strip()

        sku_grouped = sku_grouped.merge(
            fba_fees_df,
            on="sku",
            how="left"
        )

        sku_grouped["fba_fees"] = pd.to_numeric(
            sku_grouped["fba_fees"],
            errors="coerce"
        ).fillna(0.0)

        sku_grouped = sku_grouped.merge(refund_fees, on="sku", how="left")
        sku_grouped = sku_grouped.merge(quantity_df, on="sku", how="left")
        sku_grouped = sku_grouped.merge(return_qty_df, on="sku", how="left")
        sku_grouped = sku_grouped.merge(lost_total_df, on="sku", how="left")
        sku_grouped = sku_grouped.merge(misc_transaction_df, on="sku", how="left")
        sku_grouped = sku_grouped.merge(short_term_storage_fee_df, on="sku", how="left")
        sku_grouped = sku_grouped.merge(long_term_storage_fee_df, on="sku", how="left")
        sku_grouped = sku_grouped.merge(fba_disposal_df, on="sku", how="left")
        sku_grouped = sku_grouped.merge(other_adjustment_df, on="sku", how="left")
        sku_grouped = sku_grouped.merge(placement_fee_df, on="sku", how="left")
        sku_grouped = sku_grouped.merge(customs_fee_df, on="sku", how="left")
        sku_grouped = sku_grouped.merge(shipment_fees_df, on="sku", how="left")
        sku_grouped = sku_grouped.merge(platform_management_fees_df, on="sku", how="left")

        sku_grouped["refund_selling_fees"] = safe_series(sku_grouped, "refund_selling_fees")
        sku_grouped["quantity"] = safe_series(sku_grouped, "quantity").abs()
        sku_grouped["return_quantity"] = safe_series(
            sku_grouped,
            "return_quantity"
        ).abs().astype(int)

        sku_grouped["lost_total"] = safe_series(sku_grouped, "lost_total")
        sku_grouped["misc_transaction"] = safe_series(sku_grouped, "misc_transaction")

        # ✅ quantity = shipment/order quantity + refund quantity
        sku_grouped["quantity"] = (
            sku_grouped["quantity"] + sku_grouped["return_quantity"]
        ).astype(int)

        # ✅ total_quantity = quantity - refund
        sku_grouped["total_quantity"] = (
            sku_grouped["quantity"] - sku_grouped["return_quantity"]
        ).astype(int)

        sku_grouped["selling_fees"] = safe_series(sku_grouped, "selling_fees")
        sku_grouped["selling_fees"] -= 2 * sku_grouped["refund_selling_fees"]

        # ---------- shared formula engine: sales / gross sales / tax / credits / amazon fee ----------
        # Keep yearly and quarterly using the same helpers as monthly.
        # Because formulas_utils.us_* now calls the same UK helper logic, these columns stay aligned:
        #   Net Sales, gross_sales, refund_sales, tex_and_credits,
        #   net_taxes, net_credits, amazon_fee.
        sku_grouped["sku"] = sku_grouped["sku"].astype(str).str.strip()

        def merge_formula_metric(metric_df, value_col, out_col, component_map=None):
            nonlocal sku_grouped
            if component_map is None:
                component_map = {}

            if (
                metric_df is None
                or metric_df.empty
                or "sku" not in metric_df.columns
                or value_col not in metric_df.columns
            ):
                if out_col not in sku_grouped.columns:
                    sku_grouped[out_col] = 0.0
                for dest in component_map.values():
                    if dest not in sku_grouped.columns:
                        sku_grouped[dest] = 0.0
                return

            cols = ["sku", value_col] + [c for c in component_map.keys() if c in metric_df.columns]
            tmp = metric_df[cols].copy()
            tmp["sku"] = tmp["sku"].astype(str).str.strip()
            tmp = tmp.rename(columns={value_col: out_col, **component_map})

            for c in [out_col, *component_map.values()]:
                if c in sku_grouped.columns:
                    sku_grouped = sku_grouped.drop(columns=[c])

            sku_grouped = sku_grouped.merge(tmp, on="sku", how="left")
            for c in [out_col, *component_map.values()]:
                if c not in sku_grouped.columns:
                    sku_grouped[c] = 0.0
                else:
                    sku_grouped[c] = pd.to_numeric(
                        sku_grouped[c],
                        errors="coerce"
                    ).fillna(0.0)

        sales_total, sales_by_sku, _ = us_sales(df, country=country)
        gross_total, gross_by_sku, _ = us_gross_sales(df, country=country)
        tax_total, tax_by_sku, _ = us_tax(df, country=country)
        credits_total, credits_by_sku, _ = us_credits(df, country=country)
        fee_total, fees_by_sku, _ = us_amazon_fee(df, country=country)

        merge_formula_metric(
            sales_by_sku,
            "__metric__",
            "Net Sales",
            {
                "gross_sales": "gross_sales",
                "refund_sales": "refund_sales",
                "promotional_rebates": "promotional_rebates",
            },
        )
        merge_formula_metric(tax_by_sku, "__metric__", "net_taxes")
        merge_formula_metric(credits_by_sku, "__metric__", "net_credits")
        sku_grouped["tex_and_credits"] = (
            pd.to_numeric(
                sku_grouped["net_taxes"],
                errors="coerce"
            ).fillna(0.0).abs()
            +
            pd.to_numeric(
                sku_grouped["net_credits"],
                errors="coerce"
            ).fillna(0.0)
            +
            pd.to_numeric(
                sku_grouped["misc_transaction"],
                errors="coerce"
            ).fillna(0.0).abs()
        )
        merge_formula_metric(fees_by_sku, "__metric__", "amazon_fee")

        for col in [
            "Net Sales", "gross_sales", "refund_sales", "tex_and_credits",
            "net_taxes", "net_credits", "amazon_fee",
        ]:
            if col not in sku_grouped.columns:
                sku_grouped[col] = 0.0
            sku_grouped[col] = pd.to_numeric(sku_grouped[col], errors="coerce").fillna(0.0)

        sku_grouped["other_transaction_fees"] = _other_transaction_fees_series(
            sku_grouped,
            "net_taxes",
            "net_credits",
        )

        sku_grouped["asp"] = np.where(
            sku_grouped["total_quantity"] != 0,
            sku_grouped["Net Sales"] / sku_grouped["total_quantity"],
            0,
        )
        sku_grouped["asp"] = sku_grouped["asp"].replace([np.inf, -np.inf], 0).fillna(0)

        sku_grouped["promotional_rebates_percentage"] = np.where(
            sku_grouped["Net Sales"] != 0,
            (safe_series(sku_grouped, "promotional_rebates") / sku_grouped["Net Sales"]) * 100,
            0,
        )
        sku_grouped["promotional_rebates_percentage"] = (
            sku_grouped["promotional_rebates_percentage"]
            .replace([np.inf, -np.inf], 0)
            .fillna(0)
        )

        sku_grouped["price_in_gbp"] = safe_series(sku_grouped, "price_in_gbp")
        sku_grouped["cost_of_unit_sold"] = sku_grouped["price_in_gbp"] * sku_grouped["total_quantity"]

        sku_grouped["profit"] = (
            safe_series(sku_grouped, "Net Sales")
            - safe_series(sku_grouped, "cost_of_unit_sold").abs()
            - safe_series(sku_grouped, "amazon_fee").abs()
            - safe_series(sku_grouped, "net_taxes").abs()
            + safe_series(sku_grouped, "net_credits")
            + safe_series(sku_grouped, "misc_transaction").abs()
        )

        sku_grouped["profit_percentage"] = np.where(
            sku_grouped["Net Sales"] != 0,
            (sku_grouped["profit"] / sku_grouped["Net Sales"]) * 100,
            0
        )
        sku_grouped["profit_percentage"] = sku_grouped["profit_percentage"].replace([np.inf, -np.inf], 0).fillna(0)

        # Shipping charges = placement fee + customs fee + shipment fees.
        # Store as a positive expense amount.
        sku_grouped["shipping_charges"] = (
            safe_series(sku_grouped, "placement_fee").abs()
            + safe_series(sku_grouped, "customs_fee").abs()
            + safe_series(sku_grouped, "shipment_fees").abs()
        )

        sku_grouped["shipment_fees"] = safe_series(sku_grouped, "shipment_fees").abs()
        sku_grouped["platform_management_fees"] = safe_series(sku_grouped, "platform_management_fees").abs()
        sku_grouped["advertising_total"] = 0

        sku_grouped["platformfeenew"] = safe_series(sku_grouped, "platformfeenew")
        sku_grouped["platform_fee_inventory_storage"] = safe_series(sku_grouped, "platform_fee_inventory_storage")
        sku_grouped["short_term_storage_fee"] = safe_series(sku_grouped, "short_term_storage_fee").abs()
        sku_grouped["long_term_storage_fee"] = safe_series(sku_grouped, "long_term_storage_fee").abs()
        sku_grouped["storage_fee"] = (
            safe_series(sku_grouped, "short_term_storage_fee").abs()
            + safe_series(sku_grouped, "long_term_storage_fee").abs()
        )
        sku_grouped["other_adjustment"] = safe_series(
            sku_grouped, "other_adjustment"
        ).abs()
        sku_grouped["fba_disposal"] = safe_series(sku_grouped, "fba_disposal")
        sku_grouped["inventory_charges_and_reimbursement"] = (
            safe_series(sku_grouped, "fba_disposal").abs()
            - safe_series(sku_grouped, "lost_total").abs()
        )
        sku_grouped["placement_fee"] = safe_series(sku_grouped, "placement_fee")
        sku_grouped["customs_fee"] = safe_series(sku_grouped, "customs_fee")
        sku_grouped["shipment_fees"] = safe_series(sku_grouped, "shipment_fees").abs()

        sku_grouped["platform_fee"] = (
            sku_grouped["platformfeenew"].abs()
            + sku_grouped["platform_fee_inventory_storage"].abs()
            - safe_series(sku_grouped, "lost_total").abs()
            - safe_series(sku_grouped, "misc_transaction").abs()
        )

        sku_grouped["reimbursement_vs_sales"] = 0
        # CM2 follows the report layout:
        # Profit - Advertising - Shipping - Storage - Platform Management
        # + Other Adjustment - Inventory Charges and Reimbursement.
        # inventory_charges_and_reimbursement can be negative, so subtracting it
        # correctly adds the net reimbursement benefit back to CM2.
        sku_grouped["cm2_profit"] = (
            safe_series(sku_grouped, "profit")
            - safe_series(sku_grouped, "advertising_total").abs()
            - safe_series(sku_grouped, "shipping_charges").abs()
            - safe_series(sku_grouped, "storage_fee").abs()
            - safe_series(sku_grouped, "inventory_charges_and_reimbursement")
            - safe_series(sku_grouped, "platform_management_fees").abs()
            + safe_series(sku_grouped, "other_adjustment")
        )
        sku_grouped["cm2_margins"] = np.where(
            sku_grouped["Net Sales"] != 0,
            (sku_grouped["cm2_profit"] / sku_grouped["Net Sales"]) * 100,
            0
        )
        sku_grouped["cm2_profit_percentage"] = sku_grouped["cm2_margins"]
        sku_grouped["acos"] = 0.0
        sku_grouped["rembursment_vs_cm2_margins"] = 0

        sku_grouped["unit_wise_profitability"] = np.where(
            sku_grouped["quantity"] != 0,
            sku_grouped["profit"] / sku_grouped["quantity"],
            0
        )

        total_sales = abs(sku_grouped["Net Sales"].sum())
        total_profit = abs(sku_grouped["profit"].sum())
        total_cous = abs(sku_grouped["cost_of_unit_sold"].sum())
        total_amazon_fee = abs(sku_grouped["amazon_fee"].sum())
        total_fba_fees = abs(safe_series(sku_grouped, "fba_fees").sum())

        sku_grouped["sales_mix"] = np.where(
            total_sales != 0,
            (sku_grouped["Net Sales"] / total_sales) * 100,
            0
        )
        sku_grouped["profit_mix"] = np.where(
            total_profit != 0,
            (sku_grouped["profit"] / total_profit) * 100,
            0
        )

        reimbursement_vs_sales = abs((rembursement_fee / total_sales) * 100) if total_sales else 0
        inventory_charges_and_reimbursement_total = (
            abs(fba_disposal_total) - abs(lost_total_amount)
        )
        # Use the report-level shipping total. Some shipping transactions have blank SKU,
        # so summing SKU rows can be zero/incomplete and overstate CM2.
        shipping_charges_total = (
            abs(placement_fee_total)
            + abs(customs_fee_total)
            + abs(shipment_fees)
        )
        cm2_profit = (
            total_profit
            - abs(advertising_total)
            - shipping_charges_total
            - abs(short_term_storage_fee_total + long_term_storage_fee_total)
            - inventory_charges_and_reimbursement_total
            - abs(platform_management_fees_total)
            + other_adjustment_total
        )
        cm2_margins = (cm2_profit / total_sales) * 100 if total_sales else 0
        acos = (advertising_total / total_sales) * 100 if total_sales else 0
        rembursment_vs_cm2_margins = abs((rembursement_fee / cm2_profit) * 100) if cm2_profit else 0

        sku_grouped = sku_grouped.rename(columns={
            "Net Sales": "net_sales",
            "Net Taxes": "net_taxes",
            "Net Credits": "net_credits",
            "profit%": "profit_percentage"
        })

        sum_row = sku_grouped.select_dtypes(include=[np.number]).sum()
        sum_row["sku"] = "TOTAL"
        sum_row["product_name"] = "TOTAL"
        sum_row["year"] = str(year)
        sum_row["country"] = country
        sum_row["user_id"] = user_id

        sum_row["promotional_rebates_percentage"] = (
            (
                float(sum_row["promotional_rebates"])
                / float(sum_row["net_sales"])
            ) * 100
            if float(sum_row["net_sales"]) != 0
            else 0
        )

        sum_row["profit_percentage"] = (
            (sum_row["profit"] / sum_row["net_sales"]) * 100
            if sum_row["net_sales"] != 0 else 0
        )
        sum_row["shipment_fees"] = abs(shipment_fees)
        sum_row["shipping_charges"] = (
            abs(placement_fee_total)
            + abs(customs_fee_total)
            + abs(shipment_fees)
        )
        sum_row["debt_payment"] = abs(debt_payment_total)
        sum_row["disbursement"] = abs(disbursement_total)
        sum_row["rembursement_fee"] = abs(rembursement_fee)
        sum_row["visible_ads"] = visible_ads_total
        sum_row["dealsvouchar_ads"] = dealsvouchar_ads_total
        sum_row["advertising_total"] = advertising_total
        sum_row["platformfeenew"] = platformfeenew_total
        sum_row["platform_management_fees"] = platform_management_fees_total
        sum_row["platform_fee_inventory_storage"] = platform_fee_inventory_storage_total
        sum_row["short_term_storage_fee"] = short_term_storage_fee_total
        sum_row["long_term_storage_fee"] = long_term_storage_fee_total
        sum_row["storage_fee"] = short_term_storage_fee_total + long_term_storage_fee_total
        sum_row["other_adjustment"] = other_adjustment_total
        sum_row["fba_disposal"] = fba_disposal_total
        sum_row["placement_fee"] = placement_fee_total
        sum_row["customs_fee"] = customs_fee_total
        sum_row["misc_transaction"] = misc_transaction_total
        sum_row["profit"] = (
            sum_row["net_sales"]
            - abs(sum_row["cost_of_unit_sold"])
            - abs(sum_row["amazon_fee"])
            - abs(sum_row["net_taxes"])
            + sum_row["net_credits"]
            + abs(sum_row["misc_transaction"])
        )
        sum_row["profit_percentage"] = (
            (sum_row["profit"] / sum_row["net_sales"]) * 100
            if sum_row["net_sales"] != 0 else 0
        )
        sum_row["other_transaction_fees"] = _other_transaction_fees_value(
            sum_row.get("net_taxes", 0.0),
            sum_row.get("net_credits", 0.0),
            sum_row.get("misc_transaction", 0.0),
        )
        sum_row["lost_total"] = lost_total_amount
        sum_row["inventory_charges_and_reimbursement"] = (
            abs(fba_disposal_total) - abs(lost_total_amount)
        )
        sum_row["platform_fee"] = platform_fee
        sum_row["reimbursement_vs_sales"] = abs(reimbursement_vs_sales)
        sum_row["cm2_profit"] = abs(cm2_profit)
        sum_row["cm2_margins"] = abs(cm2_margins)
        sum_row["cm2_profit_percentage"] = (
            (sum_row["cm2_profit"] / sum_row["net_sales"]) * 100
            if sum_row["net_sales"] != 0 else 0
        )
        sum_row["acos"] = abs(acos)
        sum_row["rembursment_vs_cm2_margins"] = abs(rembursment_vs_cm2_margins)
        sum_row["asp"] = (
            sum_row["net_sales"] / sum_row["total_quantity"]
            if sum_row["total_quantity"] != 0 else 0
        )
        sum_row["unit_wise_profitability"] = (
            sum_row["profit"] / sum_row["quantity"]
            if sum_row["quantity"] != 0 else 0
        )

        sku_grouped["year"] = str(year)
        sku_grouped["country"] = country
        sku_grouped["user_id"] = user_id

        sku_grouped = pd.concat([sku_grouped, pd.DataFrame([sum_row])], ignore_index=True)

        sku_grouped = add_report_compat_columns(sku_grouped)

        final_columns = [
            "sku", "product_name", "quantity", "return_quantity", "total_quantity",
            "asp", "gross_sales", "refund_sales", "tex_and_credits", "net_sales",
            "promotional_rebates", "promotional_rebates_percentage",
            "cost_of_unit_sold", "selling_fees", "fba_fees", "amazon_fee",
            "net_taxes", "net_credits", "misc_transaction", "other_transaction_fees",
            "profit", "unit_wise_profitability", "profit_percentage",
            "visible_ads", "dealsvouchar_ads", "advertising_total", "lost_total",
            "platformfeenew", "platform_management_fees", "platform_fee", "platform_fee_inventory_storage",
            "short_term_storage_fee", "long_term_storage_fee", "storage_fee", "other_adjustment", "fba_disposal", "inventory_charges_and_reimbursement", "placement_fee", "customs_fee",
            "shipping_charges", "shipment_fees", "cm2_profit", "cm2_profit_percentage",
            "cm2_margins", "acos", "debt_payment", "disbursement", "rembursement_fee",
            "rembursment_vs_cm2_margins", "reimbursement_vs_sales",
            "sales_mix", "profit_mix", "year", "country", "user_id"
        ]
        final_columns = list(dict.fromkeys(final_columns + REPORT_COMPAT_COLUMNS))

        for col in final_columns:
            if col not in sku_grouped.columns:
                sku_grouped[col] = 0

        sku_grouped = sku_grouped[final_columns]

        for col in sku_grouped.columns:
            if pd.api.types.is_numeric_dtype(sku_grouped[col]):
                sku_grouped[col] = pd.to_numeric(sku_grouped[col], errors="coerce").fillna(0)
            else:
                sku_grouped[col] = sku_grouped[col].fillna("")

        sku_grouped = _make_us_display_expenses_positive(sku_grouped)

        from sqlalchemy.types import Float, Integer, String

        dtype_map = {
            "sku": String,
            "product_name": String,
            "quantity": Float,
            "return_quantity": Float,
            "total_quantity": Float,
            "asp": Float,
            "gross_sales": Float,
            "refund_sales": Float,
            "tex_and_credits": Float,
            "net_sales": Float,
            "promotional_rebates": Float,
            "promotional_rebates_percentage": Float,
            "cost_of_unit_sold": Float,
            "selling_fees": Float,
            "fba_fees": Float,
            "amazon_fee": Float,
            "net_taxes": Float,
            "net_credits": Float,
            "misc_transaction": Float,
            "other_transaction_fees": Float,
            "profit": Float,
            "unit_wise_profitability": Float,
            "profit_percentage": Float,
            "visible_ads": Float,
            "dealsvouchar_ads": Float,
            "advertising_total": Float,
            "lost_total": Float,
            "inventory_charges_and_reimbursement": Float,
            "platformfeenew": Float,
            "platform_management_fees": Float,
            "platform_fee": Float,
            "platform_fee_inventory_storage": Float,
            "short_term_storage_fee": Float,
            "long_term_storage_fee": Float,
            "storage_fee": Float,
            "other_adjustment": Float,
            "fba_disposal": Float,
            "placement_fee": Float,
            "customs_fee": Float,
            "shipping_charges": Float,
            "shipment_fees": Float,
            "cm2_profit": Float,
            "cm2_profit_percentage": Float,
            "cm2_margins": Float,
            "acos": Float,
            "debt_payment": Float,
            "disbursement": Float,
            "rembursement_fee": Float,
            "rembursment_vs_cm2_margins": Float,
            "reimbursement_vs_sales": Float,
            "sales_mix": Float,
            "profit_mix": Float,
            "year": String,
            "country": String,
            "user_id": Integer,
        }

        with engine.begin() as write_conn:
            write_conn.execute(text(f"DROP TABLE IF EXISTS {yearly_table}"))

        sku_grouped.to_sql(
            yearly_table,
            con=engine,
            if_exists="replace",
            index=False,
            dtype=dtype_map,
            method="multi"
        )

        return None

    except Exception as e:
        print(f"Error processing yearly SKU-wise data: {e}")
        raise

    finally:
        conn.close()



def process_us_quarterly_skuwise_data(user_id, country, month, year, quarter, db_url):
    engine = create_engine(db_url)
    conn = engine.connect()

    quarter_months = {
        "quarter1": ["january", "february", "march"],
        "quarter2": ["april", "may", "june"],
        "quarter3": ["july", "august", "september"],
        "quarter4": ["october", "november", "december"],
    }

    month = month.lower()
    for q_name, months in quarter_months.items():
        if month in months:
            quarter = q_name
            break
    else:
        print("Invalid month provided.")
        return

    quarter_table = f"{quarter}_{user_id}_{country}_{year}_table"
    source_table = f"user_{user_id}_{country}_merge_data_of_all_months"

    def safe_series(df_, col, default=0.0):
        if col in df_.columns:
            return pd.to_numeric(df_[col], errors="coerce").fillna(default)
        return pd.Series(default, index=df_.index, dtype="float64")

    def sum_total_where_desc_contains(df_, keywords):
        if "total" not in df_.columns:
            return 0.0
        desc = df_.get("description", pd.Series("", index=df_.index)).astype(str)
        pattern = "|".join(re.escape(k) for k in keywords)
        mask = desc.str.contains(pattern, case=False, na=False, regex=True)
        return float(pd.to_numeric(df_.loc[mask, "total"], errors="coerce").fillna(0).sum())

    def sku_sum_total_where_desc_contains(df_, keywords, out_col):
        if "total" not in df_.columns:
            return pd.DataFrame(columns=["sku", out_col])

        desc = df_.get("description", pd.Series("", index=df_.index)).astype(str)
        pattern = "|".join(re.escape(k) for k in keywords)
        mask = desc.str.contains(pattern, case=False, na=False, regex=True)

        out = (
            df_.loc[
                mask
                & df_["sku"].notna()
                & (df_["sku"].astype(str).str.strip() != "")
                & (df_["sku"].astype(str).str.strip() != "0")
            ]
            .groupby("sku", as_index=False)["total"]
            .sum()
            .rename(columns={"total": out_col})
        )
        out[out_col] = pd.to_numeric(out[out_col], errors="coerce").fillna(0.0)
        out["sku"] = out["sku"].astype(str).str.strip()
        return out

    try:
        data_query = text(f"""
            SELECT *
            FROM {source_table}
            WHERE LOWER(month) IN :months AND year = :year
        """)
        df = pd.read_sql(
            data_query,
            conn,
            params={"months": tuple(months), "year": str(year)}
        )

        if df.empty:
            print(f"No data found for {quarter} in {source_table}")
            return

        likely_text_cols = ["sku", "type", "description", "marketplace", "fulfilment", "product_name", "errorstatus"]
        for col in likely_text_cols:
            if col in df.columns:
                df[col] = df[col].astype(str)

        for col in [
            "platform_fees", "net_reimbursement", "shipping_credits",
            "postage_credits", "gift_wrap_credits", "answer", "difference"
        ]:
            if col not in df.columns:
                df[col] = 0.0

        numeric_columns = [
            "product_sales", "promotional_rebates", "promotional_rebates_tax",
            "product_sales_tax", "selling_fees", "fba_fees", "other",
            "marketplace_facilitator_tax", "shipping_credits_tax",
            "giftwrap_credits_tax", "postage_credits", "shipping_credits",
            "gift_wrap_credits", "price_in_gbp", "cost_of_unit_sold",
            "quantity", "total", "other_transaction_fees", "platform_fees",
            "net_reimbursement", "answer", "difference"
        ]
        numeric_columns = [col for col in numeric_columns if col in df.columns]
        df[numeric_columns] = df[numeric_columns].apply(pd.to_numeric, errors="coerce").fillna(0)

        df["sku"] = df["sku"].astype(str).str.strip()
        df["type_norm"] = df.get("type", pd.Series("", index=df.index)).astype(str).str.strip().str.lower()
        df["desc_norm"] = df.get("description", pd.Series("", index=df.index)).astype(str).str.strip()

        df_valid = df[
            df["sku"].notna()
            & (df["sku"] != "")
            & (df["sku"] != "0")
            & (df["sku"].str.lower() != "none")
        ].copy()

        debt_payment_total = abs(sum_total_where_desc_contains(df, ["DebtPayment"]))
        disbursement_total = abs(sum_total_where_desc_contains(df, ["Disbursement"]))

        rembursement_fee = disbursement_total + safe_series(df, "net_reimbursement").sum()

        shipment_keywords = [
            "FBA international shipping charge",
            "FBA Inbound Placement Service Fee",
            "FBA international shipping customs charge",
        ]
        shipment_mask = df["desc_norm"].str.contains("|".join(shipment_keywords), case=False, na=False)
        shipment_charges = abs(safe_series(df.loc[shipment_mask], "total").sum())
        shipment_fees = abs(sum_total_where_desc_contains(
            df,
            ["AWDTransportationFee"],
        ))

        visible_ads_total = abs(sum_total_where_desc_contains(df, ["ProductAdsPayment"]))
        dealsvouchar_ads_total = abs(sum_total_where_desc_contains(df, [
            "Cost of Advertising",
            "Coupon Redemption Fee",
            "Deals",
            "Lightning Deal",
            "CouponPerformanceEvent",
            "CouponParticipationEvent",
            "SellerDealComplete",
            "VineCharge",
            "SellerPoweredCoupon",
            "DealParticipationEvent",
            "DealPerformanceEvent",
        ]))
        advertising_total = visible_ads_total + dealsvouchar_ads_total

        refund_fees = (
            df[df["type_norm"] == "refund"]
            .groupby("sku", as_index=False)["selling_fees"]
            .sum()
            .rename(columns={"selling_fees": "refund_selling_fees"})
        )
        refund_fees["sku"] = refund_fees["sku"].astype(str).str.strip()

        # FBA fees: only Shipment and Refund
        fba_fees_df = (
            df.loc[
                df["type_norm"].isin(["shipment", "refund"])
                & df["sku"].notna()
                & (df["sku"].astype(str).str.strip() != "")
                & (df["sku"].astype(str).str.strip() != "0")
                & (df["sku"].astype(str).str.lower() != "none"),
                ["sku", "fba_fees"]
            ]
            .groupby("sku", as_index=False)["fba_fees"]
            .sum()
        )

        fba_fees_df["sku"] = fba_fees_df["sku"].astype(str).str.strip()
        fba_fees_df["fba_fees"] = pd.to_numeric(
            fba_fees_df["fba_fees"],
            errors="coerce"
        ).fillna(0.0)

        quantity_df = (
            df[df["type_norm"].isin(["order", "shipment"])]
            .groupby("sku", as_index=False)["quantity"]
            .sum()
        )

        return_qty_df = (
            df[df["type_norm"] == "refund"]
            .groupby("sku", as_index=False)["quantity"]
            .sum()
            .rename(columns={"quantity": "return_quantity"})
        )
        if not return_qty_df.empty:
            return_qty_df["return_quantity"] = pd.to_numeric(
                return_qty_df["return_quantity"], errors="coerce"
            ).fillna(0).abs()

        LOST_DESCRIPTIONS = {
            "REVERSAL_REIMBURSEMENT",
            "WAREHOUSE_LOST",
            "WAREHOUSE_DAMAGE",
            "MISSING_FROM_INBOUND",
            "FREE_REPLACEMENT_REFUND_ITEMS",
        }

        lost_mask = df["desc_norm"].isin(LOST_DESCRIPTIONS)
        lost_total_df = (
            df.loc[lost_mask]
            .groupby("sku", as_index=False)["total"]
            .sum()
            .rename(columns={"total": "lost_total"})
        )
        if not lost_total_df.empty:
            lost_total_df["lost_total"] = pd.to_numeric(
                lost_total_df["lost_total"], errors="coerce"
            ).fillna(0)

        # ---------- misc transaction: SKU-wise + blank-SKU total ----------

        def _norm_misc_key(x):
            return re.sub(r"\s+", " ", str(x or "").strip()).casefold()

        df["sku"] = df["sku"].fillna("").astype(str).str.strip()
        df["desc_norm"] = df.get("description", pd.Series("", index=df.index)).fillna("").astype(str).str.strip()
        df["type_norm"] = (
            df.get("type", pd.Series("", index=df.index))
            .fillna("")
            .astype(str)
            .str.strip()
            .str.lower()
        )

        df["desc_key"] = df["desc_norm"].map(_norm_misc_key)
        df["type_key"] = df["type_norm"].map(_norm_misc_key)
        df["total"] = pd.to_numeric(df.get("total", 0), errors="coerce").fillna(0.0)

        EXCLUDE_DESCRIPTIONS = {
            # Advertising / deals
            "Cost of Advertising",
            "Coupon Redemption Fee",
            "Deals",
            "Lightning Deal",
            "ProductAdsPayment",
            "CouponPerformanceEvent",
            "CouponParticipationEvent",
            "SellerDealComplete",
            "VineCharge",
            "SellerPoweredCoupon",
            "DealParticipationEvent",
            "DealPerformanceEvent",

            # Platform / storage / shipment buckets
            "FBA Return Fee",
            "FBA Long-Term Storage Fee",
            "FBA storage fee",
            "FBADisposal",
            "FBAStorageBilling",
            "FBALongTermStorageBilling",
            "INCORRECT_FEES_NON_ITEMIZED",
            "StorageReservationBilling",
            "FBAStorageFeeAdjustment",
            "Subscription",
            "PaidServicesCharge",
            "FBAInboundConvenience",
            "AWDProcessingFee",
            "AWDTransportationFee",
            "AGSGlobalInboundTransportation",

            # Normal payment / refund / transfer buckets
            "Order Payment",
            "Refund",
            "Disbursement",
            "DebtPayment",

            # Lost / reimbursement bucket
            "REVERSAL_REIMBURSEMENT",
            "WAREHOUSE_LOST",
            "WAREHOUSE_DAMAGE",
            "MISSING_FROM_INBOUND",
            "FREE_REPLACEMENT_REFUND_ITEMS",
        }

        EXCLUDE_TYPES = {
            "Transfer",
            "Refund",
        }

        exclude_desc_keys = {_norm_misc_key(x) for x in EXCLUDE_DESCRIPTIONS}
        exclude_type_keys = {_norm_misc_key(x) for x in EXCLUDE_TYPES}

        leftout_mask = (
            ~df["desc_key"].isin(exclude_desc_keys)
            & ~df["type_key"].isin(exclude_type_keys)
        )

        all_misc_transaction_total = (
            pd.to_numeric(
                df.loc[leftout_mask, "total"],
                errors="coerce"
            )
            .fillna(0.0)
            .sum()
        )

        # misc_transaction is SKU-wise only; blank/unassigned misc rows move to other_adjustment.
        tmp_misc = df.loc[
            leftout_mask
            & df["sku"].notna()
            & (df["sku"] != "")
            & (df["sku"] != "0")
            & (df["sku"].str.lower() != "none"),
            ["sku", "total"]
        ].copy()

        misc_transaction_df = (
            tmp_misc.groupby("sku", as_index=False)["total"]
            .sum()
            .rename(columns={"total": "misc_transaction"})
        )

        misc_transaction_df["misc_transaction"] = pd.to_numeric(
            misc_transaction_df["misc_transaction"],
            errors="coerce"
        ).fillna(0.0)

        misc_transaction_total = float(misc_transaction_df["misc_transaction"].sum())
        unassigned_misc_transaction_total = all_misc_transaction_total - misc_transaction_total

        platformfeenew_total = abs(sum_total_where_desc_contains(df, ["Subscription"]))

        # Platform management fees include paid services and subscription charges.
        PLATFORM_MANAGEMENT_FEE_KEYWORDS = [
            "PaidServicesCharge",
            "Subscription",
        ]
        platform_management_fees_total = abs(sum_total_where_desc_contains(
            df,
            PLATFORM_MANAGEMENT_FEE_KEYWORDS,
        ))
        platform_management_fees_df = sku_sum_total_where_desc_contains(
            df,
            PLATFORM_MANAGEMENT_FEE_KEYWORDS,
            "platform_management_fees",
        )
        if not platform_management_fees_df.empty:
            platform_management_fees_df["platform_management_fees"] = pd.to_numeric(
                platform_management_fees_df["platform_management_fees"],
                errors="coerce",
            ).fillna(0.0).abs()
        platform_fee_inventory_storage_total = abs(sum_total_where_desc_contains(df, [
            "FBA Return Fee",
            "FBA Long-Term Storage Fee",
            "FBA storage fee",
            "FBADisposal",
            "FBAStorageBilling",
            "FBALongTermStorageBilling",
            "INCORRECT_FEES_NON_ITEMIZED",
            "StorageReservationBilling",
            "FBAStorageFeeAdjustment",
        ]))
        short_term_storage_fee_total = abs(sum_total_where_desc_contains(df, [
            "FBAStorageBilling"
        ]))

        long_term_storage_fee_total = abs(sum_total_where_desc_contains(df, [
            "FBALongTermStorageBilling"
        ]))

        fba_disposal_total = sum_total_where_desc_contains(df, [
            "FBADisposal",
            "MISSING_FROM_INBOUND_CLAWBACK",
            "COMPENSATED_CLAWBACK",
        ])

        other_adjustment_total = (
            abs(sum_total_where_desc_contains(df, ["FBAStorageFeeAdjustment"]))
            + abs(unassigned_misc_transaction_total)
        )

        other_adjustment_df = sku_sum_total_where_desc_contains(
            df,
            ["FBAStorageFeeAdjustment"],
            "other_adjustment",
        )
        if not other_adjustment_df.empty:
            other_adjustment_df["other_adjustment"] = pd.to_numeric(
                other_adjustment_df["other_adjustment"],
                errors="coerce",
            ).fillna(0.0).abs()

        # Placement fees preserve the source sign (normally negative).
        PLACEMENT_FEE_KEYWORDS = [
            "FBAInboundConvenience",
            "AWDProcessingFee",
        ]
        placement_fee_total = sum_total_where_desc_contains(
            df,
            PLACEMENT_FEE_KEYWORDS,
        )
        placement_fee_df = sku_sum_total_where_desc_contains(
            df,
            PLACEMENT_FEE_KEYWORDS,
            "placement_fee",
        )


        # Customs fees: AGS global inbound transportation.
        # Preserve the original source sign (normally negative).
        CUSTOMS_FEE_KEYWORDS = [
            "AGSGlobalInboundTransportation",
        ]
        customs_fee_total = sum_total_where_desc_contains(
            df,
            CUSTOMS_FEE_KEYWORDS,
        )
        customs_fee_df = sku_sum_total_where_desc_contains(
            df,
            CUSTOMS_FEE_KEYWORDS,
            "customs_fee",
        )
        if not customs_fee_df.empty:
            customs_fee_df["customs_fee"] = pd.to_numeric(
                customs_fee_df["customs_fee"],
                errors="coerce",
            ).fillna(0.0)

        shipment_fees_df = sku_sum_total_where_desc_contains(
            df,
            ["AWDTransportationFee"],
            "shipment_fees",
        )
        if not shipment_fees_df.empty:
            shipment_fees_df["shipment_fees"] = pd.to_numeric(
                shipment_fees_df["shipment_fees"],
                errors="coerce",
            ).fillna(0.0).abs()

        short_term_storage_fee_df = sku_sum_total_where_desc_contains(
            df,
            ["FBAStorageBilling"],
            "short_term_storage_fee"
        )

        long_term_storage_fee_df = sku_sum_total_where_desc_contains(
            df,
            ["FBALongTermStorageBilling"],
            "long_term_storage_fee"
        )

        fba_disposal_df = sku_sum_total_where_desc_contains(
            df,
            [
                "FBADisposal",
                "MISSING_FROM_INBOUND_CLAWBACK",
                "COMPENSATED_CLAWBACK",
            ],
            "fba_disposal"
        )

        lost_total_amount = abs(
            pd.to_numeric(
                lost_total_df.get("lost_total", pd.Series(dtype=float)),
                errors="coerce"
            ).fillna(0).sum()
        )

        platform_fee = (
            platformfeenew_total
            + platform_fee_inventory_storage_total
            - misc_transaction_total
            - lost_total_amount
        )

        group_cols = {
            "price_in_gbp": "mean",
            "product_sales": "sum",
            "promotional_rebates": "sum",
            "promotional_rebates_tax": "sum",
            "product_sales_tax": "sum",
            "selling_fees": "sum",
            "other": "sum",
            "marketplace_facilitator_tax": "sum",
            "shipping_credits_tax": "sum",
            "giftwrap_credits_tax": "sum",
            "postage_credits": "sum",
            "shipping_credits": "sum",
            "gift_wrap_credits": "sum",
            "cost_of_unit_sold": "sum",
            "total": "sum",
            "ads_impressions": "sum",
            "ads_clicks": "sum",
            "ads_spend_raw": "sum",
            "ads_sale_units": "sum",
            "ads_sale_amount": "sum",
            "sp_ads_sales": "sum",
            "sd_ads_sales": "sum",
            "sb_ads_sales": "sum",
            "product_spend": "sum",
            "display_spend": "sum",
            "brand_spend": "sum",
            "ad_type": "first",
            "product_name": "first",
        }
        group_cols = {k: v for k, v in group_cols.items() if k in df.columns}

        sku_grouped = df_valid.groupby("sku").agg(group_cols).reset_index()
        sku_grouped["sku"] = sku_grouped["sku"].astype(str).str.strip()

        sku_grouped = sku_grouped.merge(
            fba_fees_df,
            on="sku",
            how="left"
        )

        sku_grouped["fba_fees"] = pd.to_numeric(
            sku_grouped["fba_fees"],
            errors="coerce"
        ).fillna(0.0)

        sku_grouped = sku_grouped.merge(refund_fees, on="sku", how="left")
        sku_grouped = sku_grouped.merge(quantity_df, on="sku", how="left")
        sku_grouped = sku_grouped.merge(return_qty_df, on="sku", how="left")
        sku_grouped = sku_grouped.merge(lost_total_df, on="sku", how="left")
        sku_grouped = sku_grouped.merge(misc_transaction_df, on="sku", how="left")
        sku_grouped = sku_grouped.merge(short_term_storage_fee_df, on="sku", how="left")
        sku_grouped = sku_grouped.merge(long_term_storage_fee_df, on="sku", how="left")
        sku_grouped = sku_grouped.merge(fba_disposal_df, on="sku", how="left")
        sku_grouped = sku_grouped.merge(other_adjustment_df, on="sku", how="left")
        sku_grouped = sku_grouped.merge(placement_fee_df, on="sku", how="left")
        sku_grouped = sku_grouped.merge(customs_fee_df, on="sku", how="left")
        sku_grouped = sku_grouped.merge(shipment_fees_df, on="sku", how="left")
        sku_grouped = sku_grouped.merge(platform_management_fees_df, on="sku", how="left")  

        sku_grouped["refund_selling_fees"] = safe_series(sku_grouped, "refund_selling_fees")
        sku_grouped["quantity"] = safe_series(sku_grouped, "quantity").abs()
        sku_grouped["return_quantity"] = safe_series(
            sku_grouped,
            "return_quantity"
        ).abs().astype(int)

        sku_grouped["lost_total"] = safe_series(sku_grouped, "lost_total")
        sku_grouped["misc_transaction"] = safe_series(sku_grouped, "misc_transaction")

        # ✅ quantity = shipment/order quantity + refund quantity
        sku_grouped["quantity"] = (
            sku_grouped["quantity"] + sku_grouped["return_quantity"]
        ).astype(int)

        # ✅ total_quantity = quantity - refund
        sku_grouped["total_quantity"] = (
            sku_grouped["quantity"] - sku_grouped["return_quantity"]
        ).astype(int)

        sku_grouped["selling_fees"] = -(
            safe_series(sku_grouped, "selling_fees").abs()
            + safe_series(sku_grouped, "refund_selling_fees").abs()
        )

        # ---------- shared formula engine: sales / gross sales / tax / credits / amazon fee ----------
        # Keep yearly and quarterly using the same helpers as monthly.
        # Because formulas_utils.us_* now calls the same UK helper logic, these columns stay aligned:
        #   Net Sales, gross_sales, refund_sales, tex_and_credits,
        #   net_taxes, net_credits, amazon_fee.
        sku_grouped["sku"] = sku_grouped["sku"].astype(str).str.strip()

        def merge_formula_metric(metric_df, value_col, out_col, component_map=None):
            nonlocal sku_grouped
            if component_map is None:
                component_map = {}

            if (
                metric_df is None
                or metric_df.empty
                or "sku" not in metric_df.columns
                or value_col not in metric_df.columns
            ):
                if out_col not in sku_grouped.columns:
                    sku_grouped[out_col] = 0.0
                for dest in component_map.values():
                    if dest not in sku_grouped.columns:
                        sku_grouped[dest] = 0.0
                return

            cols = ["sku", value_col] + [c for c in component_map.keys() if c in metric_df.columns]
            tmp = metric_df[cols].copy()
            tmp["sku"] = tmp["sku"].astype(str).str.strip()
            tmp = tmp.rename(columns={value_col: out_col, **component_map})

            for c in [out_col, *component_map.values()]:
                if c in sku_grouped.columns:
                    sku_grouped = sku_grouped.drop(columns=[c])

            sku_grouped = sku_grouped.merge(tmp, on="sku", how="left")
            for c in [out_col, *component_map.values()]:
                if c not in sku_grouped.columns:
                    sku_grouped[c] = 0.0
                else:
                    sku_grouped[c] = pd.to_numeric(
                        sku_grouped[c],
                        errors="coerce"
                    ).fillna(0.0)

        sales_total, sales_by_sku, _ = us_sales(df, country=country)
        gross_total, gross_by_sku, _ = us_gross_sales(df, country=country)
        tax_total, tax_by_sku, _ = us_tax(df, country=country)
        credits_total, credits_by_sku, _ = us_credits(df, country=country)
        fee_total, fees_by_sku, _ = us_amazon_fee(df, country=country)

        merge_formula_metric(
            sales_by_sku,
            "__metric__",
            "Net Sales",
            {
                "gross_sales": "gross_sales",
                "refund_sales": "refund_sales",
                "promotional_rebates": "promotional_rebates",
            },
        )
        merge_formula_metric(tax_by_sku, "__metric__", "net_taxes")
        merge_formula_metric(credits_by_sku, "__metric__", "net_credits")
        sku_grouped["tex_and_credits"] = (
            pd.to_numeric(
                sku_grouped["net_taxes"],
                errors="coerce"
            ).fillna(0.0).abs()
            +
            pd.to_numeric(
                sku_grouped["net_credits"],
                errors="coerce"
            ).fillna(0.0)
            +
            pd.to_numeric(
                sku_grouped["misc_transaction"],
                errors="coerce"
            ).fillna(0.0).abs()
        )
        merge_formula_metric(fees_by_sku, "__metric__", "amazon_fee")

        for col in [
            "Net Sales", "gross_sales", "refund_sales", "tex_and_credits",
            "net_taxes", "net_credits", "amazon_fee",
        ]:
            if col not in sku_grouped.columns:
                sku_grouped[col] = 0.0
            sku_grouped[col] = pd.to_numeric(sku_grouped[col], errors="coerce").fillna(0.0)

        sku_grouped["other_transaction_fees"] = _other_transaction_fees_series(
            sku_grouped,
            "net_taxes",
            "net_credits",
        )

        sku_grouped["asp"] = np.where(
            sku_grouped["total_quantity"] != 0,
            sku_grouped["Net Sales"] / sku_grouped["total_quantity"],
            0,
        )
        sku_grouped["asp"] = sku_grouped["asp"].replace([np.inf, -np.inf], 0).fillna(0)

        sku_grouped["promotional_rebates_percentage"] = np.where(
            sku_grouped["Net Sales"] != 0,
            (safe_series(sku_grouped, "promotional_rebates") / sku_grouped["Net Sales"]) * 100,
            0,
        )
        sku_grouped["promotional_rebates_percentage"] = (
            sku_grouped["promotional_rebates_percentage"]
            .replace([np.inf, -np.inf], 0)
            .fillna(0)
        )

        sku_grouped["price_in_gbp"] = safe_series(sku_grouped, "price_in_gbp")
        sku_grouped["cost_of_unit_sold"] = sku_grouped["price_in_gbp"] * sku_grouped["total_quantity"]

        sku_grouped["profit"] = (
            safe_series(sku_grouped, "Net Sales")
            - safe_series(sku_grouped, "cost_of_unit_sold").abs()
            - safe_series(sku_grouped, "amazon_fee").abs()
            - safe_series(sku_grouped, "net_taxes").abs()
            + safe_series(sku_grouped, "net_credits")
            + safe_series(sku_grouped, "misc_transaction").abs()
        )

        sku_grouped["profit_percentage"] = np.where(
            sku_grouped["Net Sales"] != 0,
            (sku_grouped["profit"] / sku_grouped["Net Sales"]) * 100,
            0
        )
        sku_grouped["profit_percentage"] = sku_grouped["profit_percentage"].replace([np.inf, -np.inf], 0).fillna(0)

        # Shipping charges = placement fee + customs fee + shipment fees.
        # Store as a positive expense amount.
        sku_grouped["shipping_charges"] = (
            safe_series(sku_grouped, "placement_fee").abs()
            + safe_series(sku_grouped, "customs_fee").abs()
            + safe_series(sku_grouped, "shipment_fees").abs()
        )

        sku_grouped["shipment_fees"] = safe_series(sku_grouped, "shipment_fees").abs()
        sku_grouped["platform_management_fees"] = safe_series(sku_grouped, "platform_management_fees").abs()
        sku_grouped["advertising_total"] = 0

        sku_grouped["platformfeenew"] = safe_series(sku_grouped, "platformfeenew")
        sku_grouped["platform_fee_inventory_storage"] = safe_series(sku_grouped, "platform_fee_inventory_storage")
        sku_grouped["short_term_storage_fee"] = safe_series(sku_grouped, "short_term_storage_fee").abs()
        sku_grouped["long_term_storage_fee"] = safe_series(sku_grouped, "long_term_storage_fee").abs()
        sku_grouped["storage_fee"] = (
            safe_series(sku_grouped, "short_term_storage_fee").abs()
            + safe_series(sku_grouped, "long_term_storage_fee").abs()
        )
        sku_grouped["other_adjustment"] = safe_series(
            sku_grouped, "other_adjustment"
        ).abs()
        sku_grouped["fba_disposal"] = safe_series(sku_grouped, "fba_disposal")
        sku_grouped["inventory_charges_and_reimbursement"] = (
            safe_series(sku_grouped, "fba_disposal").abs()
            - safe_series(sku_grouped, "lost_total").abs()
        )
        sku_grouped["placement_fee"] = safe_series(sku_grouped, "placement_fee")
        sku_grouped["customs_fee"] = safe_series(sku_grouped, "customs_fee")
        sku_grouped["shipment_fees"] = safe_series(sku_grouped, "shipment_fees").abs()

        sku_grouped["platform_fee"] = (
            sku_grouped["platformfeenew"].abs()
            + sku_grouped["platform_fee_inventory_storage"].abs()
            - safe_series(sku_grouped, "lost_total").abs()
            - safe_series(sku_grouped, "misc_transaction").abs()
        )

        sku_grouped["reimbursement_vs_sales"] = 0
        # CM2 follows the report layout:
        # Profit - Advertising - Shipping - Storage - Platform Management
        # + Other Adjustment - Inventory Charges and Reimbursement.
        # inventory_charges_and_reimbursement can be negative, so subtracting it
        # correctly adds the net reimbursement benefit back to CM2.
        sku_grouped["cm2_profit"] = (
            safe_series(sku_grouped, "profit")
            - safe_series(sku_grouped, "advertising_total").abs()
            - safe_series(sku_grouped, "shipping_charges").abs()
            - safe_series(sku_grouped, "storage_fee").abs()
            - safe_series(sku_grouped, "inventory_charges_and_reimbursement")
            - safe_series(sku_grouped, "platform_management_fees").abs()
            + safe_series(sku_grouped, "other_adjustment")
        )
        sku_grouped["cm2_margins"] = np.where(
            sku_grouped["Net Sales"] != 0,
            (sku_grouped["cm2_profit"] / sku_grouped["Net Sales"]) * 100,
            0
        )
        sku_grouped["cm2_profit_percentage"] = sku_grouped["cm2_margins"]
        sku_grouped["acos"] = 0.0
        sku_grouped["rembursment_vs_cm2_margins"] = 0

        sku_grouped["unit_wise_profitability"] = np.where(
            sku_grouped["quantity"] != 0,
            sku_grouped["profit"] / sku_grouped["quantity"],
            0
        )

        total_sales = abs(sku_grouped["Net Sales"].sum())
        total_profit = abs(sku_grouped["profit"].sum())

        sku_grouped["sales_mix"] = np.where(
            total_sales != 0,
            (sku_grouped["Net Sales"] / total_sales) * 100,
            0
        )
        sku_grouped["profit_mix"] = np.where(
            total_profit != 0,
            (sku_grouped["profit"] / total_profit) * 100,
            0
        )

        reimbursement_vs_sales = abs((rembursement_fee / total_sales) * 100) if total_sales else 0
        inventory_charges_and_reimbursement_total = (
            abs(fba_disposal_total) - abs(lost_total_amount)
        )
        # Use the report-level shipping total. Some shipping transactions have blank SKU,
        # so summing SKU rows can be zero/incomplete and overstate CM2.
        shipping_charges_total = (
            abs(placement_fee_total)
            + abs(customs_fee_total)
            + abs(shipment_fees)
        )
        cm2_profit = (
            total_profit
            - abs(advertising_total)
            - shipping_charges_total
            - abs(short_term_storage_fee_total + long_term_storage_fee_total)
            - inventory_charges_and_reimbursement_total
            - abs(platform_management_fees_total)
            + other_adjustment_total
        )
        cm2_margins = (cm2_profit / total_sales) * 100 if total_sales else 0
        acos = (advertising_total / total_sales) * 100 if total_sales else 0
        rembursment_vs_cm2_margins = abs((rembursement_fee / cm2_profit) * 100) if cm2_profit else 0

        sku_grouped = sku_grouped.rename(columns={
            "Net Sales": "net_sales",
            "Net Taxes": "net_taxes",
            "Net Credits": "net_credits",
            "profit%": "profit_percentage"
        })

        sum_row = sku_grouped.select_dtypes(include=[np.number]).sum()
        sum_row["sku"] = "TOTAL"
        sum_row["product_name"] = "TOTAL"
        sum_row["year"] = str(year)
        sum_row["country"] = country
        sum_row["user_id"] = user_id

        sum_row["promotional_rebates_percentage"] = (
            (
                float(sum_row["promotional_rebates"])
                / float(sum_row["net_sales"])
            ) * 100
            if float(sum_row["net_sales"]) != 0
            else 0
        )

        sum_row["profit_percentage"] = (
            (sum_row["profit"] / sum_row["net_sales"]) * 100
            if sum_row["net_sales"] != 0 else 0
        )
        sum_row["shipment_fees"] = abs(shipment_fees)
        sum_row["shipping_charges"] = (
            abs(placement_fee_total)
            + abs(customs_fee_total)
            + abs(shipment_fees)
        )
        sum_row["debt_payment"] = abs(debt_payment_total)
        sum_row["disbursement"] = abs(disbursement_total)
        sum_row["rembursement_fee"] = abs(rembursement_fee)
        sum_row["visible_ads"] = visible_ads_total
        sum_row["dealsvouchar_ads"] = dealsvouchar_ads_total
        sum_row["advertising_total"] = advertising_total
        sum_row["platformfeenew"] = platformfeenew_total
        sum_row["platform_management_fees"] = platform_management_fees_total
        sum_row["platform_fee_inventory_storage"] = platform_fee_inventory_storage_total
        sum_row["short_term_storage_fee"] = short_term_storage_fee_total
        sum_row["long_term_storage_fee"] = long_term_storage_fee_total
        sum_row["storage_fee"] = short_term_storage_fee_total + long_term_storage_fee_total
        sum_row["other_adjustment"] = other_adjustment_total
        sum_row["fba_disposal"] = fba_disposal_total
        sum_row["placement_fee"] = placement_fee_total
        sum_row["customs_fee"] = customs_fee_total
        sum_row["misc_transaction"] = misc_transaction_total
        sum_row["profit"] = (
            sum_row["net_sales"]
            - abs(sum_row["cost_of_unit_sold"])
            - abs(sum_row["amazon_fee"])
            - abs(sum_row["net_taxes"])
            + sum_row["net_credits"]
            + abs(sum_row["misc_transaction"])
        )
        sum_row["profit_percentage"] = (
            (sum_row["profit"] / sum_row["net_sales"]) * 100
            if sum_row["net_sales"] != 0 else 0
        )
        sum_row["other_transaction_fees"] = _other_transaction_fees_value(
            sum_row.get("net_taxes", 0.0),
            sum_row.get("net_credits", 0.0),
            sum_row.get("misc_transaction", 0.0),
        )
        sum_row["lost_total"] = lost_total_amount
        sum_row["inventory_charges_and_reimbursement"] = (
            abs(fba_disposal_total) - abs(lost_total_amount)
        )
        sum_row["platform_fee"] = platform_fee
        sum_row["reimbursement_vs_sales"] = abs(reimbursement_vs_sales)
        sum_row["cm2_profit"] = abs(cm2_profit)
        sum_row["cm2_margins"] = abs(cm2_margins)
        sum_row["cm2_profit_percentage"] = (
            (sum_row["cm2_profit"] / sum_row["net_sales"]) * 100
            if sum_row["net_sales"] != 0 else 0
        )
        sum_row["acos"] = abs(acos)
        sum_row["rembursment_vs_cm2_margins"] = abs(rembursment_vs_cm2_margins)
        sum_row["asp"] = (
            sum_row["net_sales"] / sum_row["total_quantity"]
            if sum_row["total_quantity"] != 0 else 0
        )
        sum_row["unit_wise_profitability"] = (
            sum_row["profit"] / sum_row["quantity"]
            if sum_row["quantity"] != 0 else 0
        )

        sku_grouped["year"] = str(year)
        sku_grouped["country"] = country
        sku_grouped["user_id"] = user_id

        sku_grouped = pd.concat([sku_grouped, pd.DataFrame([sum_row])], ignore_index=True)

        sku_grouped = add_report_compat_columns(sku_grouped)

        final_columns = [
            "sku", "product_name", "quantity", "return_quantity", "total_quantity",
            "asp", "gross_sales", "refund_sales", "tex_and_credits", "net_sales",
            "promotional_rebates", "promotional_rebates_percentage",
            "cost_of_unit_sold", "selling_fees", "fba_fees", "amazon_fee",
            "net_taxes", "net_credits", "misc_transaction", "other_transaction_fees",
            "profit", "unit_wise_profitability", "profit_percentage",
            "visible_ads", "dealsvouchar_ads", "advertising_total", "lost_total",
            "platformfeenew", "platform_management_fees", "platform_fee", "platform_fee_inventory_storage",
            "short_term_storage_fee", "long_term_storage_fee", "storage_fee", "other_adjustment", "fba_disposal", "inventory_charges_and_reimbursement", "placement_fee", "customs_fee",
            "shipping_charges", "shipment_fees", "cm2_profit", "cm2_profit_percentage",
            "cm2_margins", "acos", "debt_payment", "disbursement", "rembursement_fee",
            "rembursment_vs_cm2_margins", "reimbursement_vs_sales",
            "sales_mix", "profit_mix", "year", "country", "user_id"
        ]
        final_columns = list(dict.fromkeys(final_columns + REPORT_COMPAT_COLUMNS))

        for col in final_columns:
            if col not in sku_grouped.columns:
                sku_grouped[col] = 0

        sku_grouped = sku_grouped[final_columns]

        for col in sku_grouped.columns:
            if pd.api.types.is_numeric_dtype(sku_grouped[col]):
                sku_grouped[col] = pd.to_numeric(sku_grouped[col], errors="coerce").fillna(0)
            else:
                sku_grouped[col] = sku_grouped[col].fillna("")

        sku_grouped = _make_us_display_expenses_positive(sku_grouped)

        total_row = sku_grouped[sku_grouped["sku"].astype(str).str.lower() == "total"]
        other_rows = sku_grouped[sku_grouped["sku"].astype(str).str.lower() != "total"]
        sku_grouped = pd.concat(
            [other_rows.sort_values(by="profit", ascending=False), total_row],
            ignore_index=True
        )

        from sqlalchemy.types import Float, Integer, String

        dtype_map = {
            "sku": String,
            "product_name": String,
            "quantity": Float,
            "return_quantity": Float,
            "total_quantity": Float,
            "asp": Float,
            "gross_sales": Float,
            "refund_sales": Float,
            "tex_and_credits": Float,
            "net_sales": Float,
            "promotional_rebates": Float,
            "promotional_rebates_percentage": Float,
            "cost_of_unit_sold": Float,
            "selling_fees": Float,
            "fba_fees": Float,
            "amazon_fee": Float,
            "net_taxes": Float,
            "net_credits": Float,
            "misc_transaction": Float,
            "other_transaction_fees": Float,
            "profit": Float,
            "unit_wise_profitability": Float,
            "profit_percentage": Float,
            "visible_ads": Float,
            "dealsvouchar_ads": Float,
            "advertising_total": Float,
            "lost_total": Float,
            "inventory_charges_and_reimbursement": Float,
            "platformfeenew": Float,
            "platform_management_fees": Float,
            "platform_fee": Float,
            "platform_fee_inventory_storage": Float,
            "short_term_storage_fee": Float,
            "long_term_storage_fee": Float,
            "storage_fee": Float,
            "other_adjustment": Float,
            "fba_disposal": Float,
            "placement_fee": Float,
            "customs_fee": Float,
            "shipping_charges": Float,
            "shipment_fees": Float,
            "cm2_profit": Float,
            "cm2_profit_percentage": Float,
            "cm2_margins": Float,
            "acos": Float,
            "debt_payment": Float,
            "disbursement": Float,
            "rembursement_fee": Float,
            "rembursment_vs_cm2_margins": Float,
            "reimbursement_vs_sales": Float,
            "sales_mix": Float,
            "profit_mix": Float,
            "year": String,
            "country": String,
            "user_id": Integer,
        }

        with engine.begin() as write_conn:
            write_conn.execute(text(f"DROP TABLE IF EXISTS {quarter_table}"))

        sku_grouped.to_sql(
            quarter_table,
            engine,
            if_exists="replace",
            index=False,
            dtype=dtype_map,
            method="multi"
        )

        return None

    except Exception as e:
        print(f"Error processing quarterly SKU-wise data: {e}")
        raise

    finally:
        conn.close()
