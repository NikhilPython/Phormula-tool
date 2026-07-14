from __future__ import annotations
from typing import Dict, Optional, Any, List
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Iterable, List
from io import BytesIO

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from config import Config


_COUNTRY_RE = re.compile(r"^[a-z]{2}$|^global$")

INVENTORY_MARKETPLACE_BY_COUNTRY = {
    "uk": "A1F83G8C2ARO7P",
    "gb": "A1F83G8C2ARO7P",
    "us": "ATVPDKIKX0DER",
    "usa": "ATVPDKIKX0DER",
}

INVENTORY_LOCATION_BY_COUNTRY = {
    "uk": "gb",
    "gb": "gb",
    "us": "us",
    "usa": "us",
}

INVENTORY_AGED_COLUMN_BY_METRIC = {
    "available": '"available"',
    "inbound_quantity": '"inbound-quantity"',
    "total_reserved_quantity": '"Total Reserved Quantity"',
    "unfulfillable_quantity": '"unfulfillable-quantity"',
    "units_shipped_t30": '"units-shipped-t30"',
    "units_shipped_t60": '"units-shipped-t60"',
    "units_shipped_t90": '"units-shipped-t90"',
    "sell_through": '"sell-through"',
    "days_of_supply": '"days-of-supply"',
    "estimated_excess_quantity": '"estimated-excess-quantity"',
}

MONTHWISE_INVENTORY_COLUMN_BY_METRIC = {
    "available": "ending_warehouse_balance",
}

CURRENT_INVENTORY_COLUMN_BY_METRIC = {
    "available": '"available"',
    "inbound_quantity": '"inbound_quantity"',
    "unfulfillable_quantity": '"unfulfillable-quantity"',
}

MONTH_NAME_TO_NUM = {
    "january": 1,
    "february": 2,
    "march": 3,
    "april": 4,
    "may": 5,
    "june": 6,
    "july": 7,
    "august": 8,
    "september": 9,
    "october": 10,
    "november": 11,
    "december": 12,
}

MONTH_NUM_TO_NAME = {v: k for k, v in MONTH_NAME_TO_NUM.items()}

MONEY_METRICS = {
    "net_sales",
    "gross_sales",
    "profit",
    "cm2_profit",
    "total_cm2_profit",

    # Ads spend
    "advertising_total",
    "ads_spend",
    "total_ads",
    "product_spend",
    "brand_spend",
    "display_spend",

    # Ads sales
    "sp_ads_sales",
    "sb_ads_sales",
    "sd_ads_sales",
    "ads_sale_amount",

    "platform_fee",
    "amazon_fee",
    "amazon_fees",
    "fba_fees",
    "selling_fees",
    "refund_sales",
    "asp",
}

def _currency_symbol_for_country(country: str) -> str:
    country_key = (country or "").strip().lower()
    country_aliases = {
        "uk": "\u00a3",
        "gb": "\u00a3",
        "gbr": "\u00a3",
        "amazon uk": "\u00a3",
        "united kingdom": "\u00a3",
        "us": "$",
        "usa": "$",
        "amazon us": "$",
        "united states": "$",
        "united states of america": "$",
    }
    return country_aliases.get(country_key, "")


def _format_value(value: float, metric_name: str, country: str) -> str:
    metric_key = (metric_name or "").strip().lower()

    # These values are already stored as percentages.
    if metric_key in {"sales_mix", "profit_mix", "acos"}:
        return f"{value:.2f}%"

    if metric_key == "total_quantity":
        return f"{value:,.0f}"

    if metric_key in MONEY_METRICS:
        symbol = _currency_symbol_for_country(country)
        return f"{symbol}{value:,.2f}" if symbol else f"{value:,.2f}"

    return f"{value:,.2f}"


@dataclass(frozen=True)
class MonthKey:
    year: int
    month: int

    def __post_init__(self) -> None:
        if not isinstance(self.year, int) or self.year < 2000 or self.year > 2100:
            raise ValueError("invalid year")
        if not isinstance(self.month, int) or self.month < 1 or self.month > 12:
            raise ValueError("invalid month")

    @property
    def month_name(self) -> str:
        return MONTH_NUM_TO_NAME[self.month]

    @property
    def table_suffix(self) -> str:
        return f"{self.month_name}{self.year}"

    @property
    def label(self) -> str:
        return datetime(self.year, self.month, 1).strftime("%b %Y")


def get_engine() -> Engine:
    return create_engine(Config.SQLALCHEMY_DATABASE_URI, pool_pre_ping=True)

def get_amazon_engine() -> Engine:
    return create_engine(
        Config.SQLALCHEMY_DATABASE_AMAZON_URL,
        pool_pre_ping=True
    )

def validate_user_id(user_id: int) -> None:
    if not isinstance(user_id, int) or user_id <= 0:
        raise ValueError("user_id must be a positive integer")


def normalize_country(country: str) -> str:
    country = (country or "").strip().lower()

    if country == "global":
        return "global"

    if not _COUNTRY_RE.match(country):
        raise ValueError("invalid country")

    return country


def normalize_month(month: int | str) -> int:
    if isinstance(month, int):
        if 1 <= month <= 12:
            return month
        raise ValueError("month must be between 1 and 12")

    value = str(month).strip().lower()
    if value.isdigit():
        num = int(value)
        if 1 <= num <= 12:
            return num
        raise ValueError("month must be between 1 and 12")

    if value not in MONTH_NAME_TO_NUM:
        raise ValueError(f"invalid month: {month}")
    return MONTH_NAME_TO_NUM[value]


def resolve_nse_table_name(user_id: int, country: str, month: int | str, year: int) -> str:
    validate_user_id(user_id)
    country = normalize_country(country)
    month_num = normalize_month(month)
    mk = MonthKey(year=year, month=month_num)

    return f"skuwisemonthly_{user_id}_{country}_{mk.table_suffix}"


def table_exists(engine: Engine, table_name: str) -> bool:
    query = text("""
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = 'public'
              AND table_name = :table_name
        )
    """)
    with engine.connect() as conn:
        return bool(conn.execute(query, {"table_name": table_name}).scalar())

def normalize_skuwisemonthly_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Makes skuwisemonthly table compatible with this metric engine.

    Productwise:
    - ads spend = ads_spend
    - CM2 profit = cm2_profit

    Total/month:
    - ads spend = total_ads
    - CM2 profit = total_cm2_profit
    """

    df = df.copy()

    def copy_if_missing(expected_col: str, source_col: str) -> None:
        if expected_col not in df.columns and source_col in df.columns:
            df[expected_col] = df[source_col]

    alias_map = {
        # Productwise COGS compatibility
        "cost_of_unit_sold": "cogs",

        # Amazon fee compatibility
        "amazon_fee": "amazon_fees",

        # Tax / credits compatibility
        "tex_and_credits": "tax_and_credits",

        # CM2 margin compatibility
        "cm2_margins": "cm2_profit_per",

        # Reimbursement spelling compatibility
        "rembursement_fee": "current_net_reimbursement",
    }

    for expected_col, source_col in alias_map.items():
        copy_if_missing(expected_col, source_col)

    # Ads and CM2 columns changed names across historical uploads.
    # Keep both product-level and month-total metric names queryable for old and new tables.
    copy_if_missing("ads_spend", "advertising_total")
    copy_if_missing("advertising_total", "ads_spend")
    copy_if_missing("advertising_fees", "ads_spend")
    copy_if_missing("total_ads", "advertising_total")
    copy_if_missing("total_ads", "ads_spend")
    copy_if_missing("total_cm2_profit", "cm2_profit")
    copy_if_missing("cm2_profit", "total_cm2_profit")

    return df


def fetch_nse_month_df(
    engine: Engine,
    user_id: int,
    country: str,
    month: int | str,
    year: int,
) -> pd.DataFrame:
    table_name = resolve_nse_table_name(user_id, country, month, year)

    if not table_exists(engine, table_name):
        raise ValueError(f"table not found: {table_name}")

    query = text(f'SELECT * FROM "{table_name}"')
    with engine.connect() as conn:
        df = pd.read_sql_query(query, conn)

    if df.empty:
        raise ValueError(f"no data found in table {table_name}")

    missing_cols = {"sku", "product_name"} - set(df.columns)
    if missing_cols:
        raise ValueError(f"{table_name} missing required columns: {missing_cols}")

    # New skuwisemonthly compatibility aliases
    df = normalize_skuwisemonthly_columns(df)

    # Make only these fee columns absolute
    for col in ["platform_fee", "platform_fee_inventory_storage"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).abs()

    return df



def fetch_total_row(df: pd.DataFrame) -> pd.Series:
    if "sku" not in df.columns:
        raise ValueError("sku column not found")

    total_df = df[df["sku"].astype(str).str.upper() == "TOTAL"]
    if total_df.empty:
        raise ValueError("TOTAL row not found")

    return total_df.iloc[0]


def fetch_non_total_rows(df: pd.DataFrame) -> pd.DataFrame:
    if "sku" not in df.columns:
        raise ValueError("sku column not found")
    return df[df["sku"].astype(str).str.upper() != "TOTAL"].copy()


def iter_months(start_year: int, start_month: int, end_year: int, end_month: int) -> Iterable[MonthKey]:
    y, m = start_year, start_month
    while (y, m) <= (end_year, end_month):
        yield MonthKey(year=y, month=m)
        if m == 12:
            y += 1
            m = 1
        else:
            m += 1


def quarter_to_month_range(year: int, quarter: int) -> tuple[MonthKey, MonthKey]:
    if quarter not in {1, 2, 3, 4}:
        raise ValueError("quarter must be 1, 2, 3, or 4")
    start_month = (quarter - 1) * 3 + 1
    end_month = start_month + 2
    return MonthKey(year=year, month=start_month), MonthKey(year=year, month=end_month)


def year_to_month_range(year: int) -> tuple[MonthKey, MonthKey]:
    return MonthKey(year=year, month=1), MonthKey(year=year, month=12)


def fetch_period_dfs(
    engine: Engine,
    user_id: int,
    country: str,
    start_month: int,
    start_year: int,
    end_month: int,
    end_year: int,
    *,
    skip_missing: bool = False,
) -> list[tuple[MonthKey, pd.DataFrame]]:
    out: list[tuple[MonthKey, pd.DataFrame]] = []
    for mk in iter_months(start_year, start_month, end_year, end_month):
        try:
            df = fetch_nse_month_df(
                engine=engine,
                user_id=user_id,
                country=country,
                month=mk.month,
                year=mk.year,
            )
            out.append((mk, df))
        except ValueError:
            if not skip_missing:
                raise
    if not out:
        raise ValueError("no month tables found for requested period")
    return out


def latest_available_month(
    engine: Engine,
    user_id: int,
    country: str,
    *,
    lookback_years: int = 5,
) -> MonthKey:
    validate_user_id(user_id)
    country = normalize_country(country)

    now = datetime.utcnow()
    start_year = now.year - lookback_years

    found: list[MonthKey] = []
    for year in range(start_year, now.year + 1):
        for month in range(1, 13):
            table_name = resolve_nse_table_name(user_id, country, month, year)
            if table_exists(engine, table_name):
                found.append(MonthKey(year=year, month=month))

    if not found:
        raise ValueError("no NSE tables found for this user/country")

    return max(found, key=lambda x: (x.year, x.month))


####nse engine file part##########


@dataclass(frozen=True)
class MetricDef:
    name: str
    column: str
    kind: str

    display_type: str
    entity_level: str   # sku / total / ratio

    # -------- EXISTING --------
    supports_ranking: bool
    supports_trend: bool
    supports_summary: bool
    supports_extreme_search: bool

    # -------- 🔥 NEW BEHAVIOR FLAGS --------
    supports_product_filter: bool
    supports_product_breakdown: bool
    supports_time_breakdown: bool

    # -------- DEFAULT FIELDS --------
    numerator: Optional[str] = None
    denominator: Optional[str] = None
    multiplier: float = 1.0
    absolute: bool = False


SKU_ADDITIVE_METRICS = {

    # -------- QUANTITY --------
    "quantity": MetricDef("quantity", "quantity", "sku_additive", "count", "sku", True, True, True, True, True, True, True),
    "return_quantity": MetricDef("return_quantity", "return_quantity", "sku_additive", "count", "sku", True, True, True, True, True, True, True),
    "total_quantity": MetricDef("total_quantity", "total_quantity", "sku_additive", "count", "sku", True, True, True, True, True, True, True),

    # -------- SALES --------
    "product_sales": MetricDef("product_sales", "product_sales", "sku_additive", "money", "sku", True, True, True, True, True, True, True),
    "product_sales_tax": MetricDef("product_sales_tax", "product_sales_tax", "sku_additive", "money", "sku", True, True, True, True, True, True, True),
    "gross_sales": MetricDef("gross_sales", "gross_sales", "sku_additive", "money", "sku", True, True, True, True, True, True, True),
    "refund_sales": MetricDef("refund_sales", "refund_sales", "sku_additive", "money", "sku", True, True, True, True, True, True, True),
    "net_sales": MetricDef("net_sales", "net_sales", "sku_additive", "money", "sku", True, True, True, True, True, True, True),

    # -------- CREDITS --------
    "postage_credits": MetricDef("postage_credits", "postage_credits", "sku_additive", "money", "sku", True, True, True, True, True, True, True),
    "gift_wrap_credits": MetricDef("gift_wrap_credits", "gift_wrap_credits", "sku_additive", "money", "sku", True, True, True, True, True, True, True),
    "giftwrap_credits_tax": MetricDef("giftwrap_credits_tax", "giftwrap_credits_tax", "sku_additive", "money", "sku", True, True, True, True, True, True, True),
    "shipping_credits": MetricDef("shipping_credits", "shipping_credits", "sku_additive", "money", "sku", True, True, True, True, True, True, True),
    "shipping_credits_tax": MetricDef("shipping_credits_tax", "shipping_credits_tax", "sku_additive", "money", "sku", True, True, True, True, True, True, True),

    # -------- REBATES --------
    "promotional_rebates": MetricDef("promotional_rebates", "promotional_rebates", "sku_additive", "money", "sku", True, True, True, True, True, True, True),
    "promotional_rebates_tax": MetricDef("promotional_rebates_tax", "promotional_rebates_tax", "sku_additive", "money", "sku", True, True, True, True, True, True, True),
    "refund_rebate": MetricDef("refund_rebate", "refund_rebate", "sku_additive", "money", "sku", True, True, True, True, True, True, True),

    # -------- COST --------
    "cost_of_unit_sold": MetricDef("cost_of_unit_sold", "cost_of_unit_sold", "sku_additive", "money", "sku", True, True, True, True, True, True, True),

    # -------- TAX --------
    "sales_tax_refund": MetricDef("sales_tax_refund", "sales_tax_refund", "sku_additive", "money", "sku", True, True, True, True, True, True, True),
    "marketplace_facilitator_tax": MetricDef("marketplace_facilitator_tax", "marketplace_facilitator_tax", "sku_additive", "money", "sku", True, True, True, True, True, True, True),
    "digital_transaction_tax": MetricDef("digital_transaction_tax", "digital_transaction_tax", "sku_additive", "money", "sku", True, True, True, True, True, True, True),
    "net_taxes": MetricDef("net_taxes", "net_taxes", "sku_additive", "money", "sku", True, True, True, True, True, True, True),

    # -------- CREDIT / REFUND --------
    "sales_credit_refund": MetricDef("sales_credit_refund", "sales_credit_refund", "sku_additive", "money", "sku", True, True, True, True, True, True, True),
    "net_credits": MetricDef("net_credits", "net_credits", "sku_additive", "money", "sku", True, True, True, True, True, True, True),

    # -------- FEES --------
    "selling_fees": MetricDef("selling_fees", "selling_fees", "sku_additive", "money", "sku", True, True, True, True, True, True, True),
    "refund_selling_fees": MetricDef("refund_selling_fees", "refund_selling_fees", "sku_additive", "money", "sku", True, True, True, True, True, True, True),
    "fba_fees": MetricDef("fba_fees", "fba_fees", "sku_additive", "money", "sku", True, True, True, True, True, True, True),
    "amazon_fee": MetricDef("amazon_fee", "amazon_fee", "sku_additive", "money", "sku", True, True, True, True, True, True, True),
    "platformfeenew": MetricDef("platformfeenew", "platformfeenew", "sku_additive", "money", "sku", True, True, True, True, True, True, True),
    "platform_fee": MetricDef("platform_fee", "platform_fee", "sku_additive", "money", "sku", True, True, True, True, True, True, True),
    "platform_fee_inventory_storage": MetricDef("platform_fee_inventory_storage", "platform_fee_inventory_storage", "sku_additive", "money", "sku", True, True, True, True, True, True, True),
    "other_transaction_fees": MetricDef("other_transaction_fees", "other_transaction_fees", "sku_additive", "money", "sku", True, True, True, True, True, True, True),

    # -------- PROFIT --------
    "profit": MetricDef("profit", "profit", "sku_additive", "money", "sku", True, True, True, True, True, True, True),
    "cm2_profit": MetricDef("cm2_profit", "cm2_profit", "sku_additive", "money", "sku", True, True, True, True, True, True, True),
    "lost_total": MetricDef("lost_total", "lost_total", "sku_additive", "money", "sku", True, True, True, True, True, True, True),

    # -------- ADS --------
    "visible_ads": MetricDef("visible_ads", "visible_ads", "sku_additive", "money", "sku", True, True, True, True, True, True, True),
    "dealsvouchar_ads": MetricDef("dealsvouchar_ads", "dealsvouchar_ads", "sku_additive", "money", "sku", True, True, True, True, True, True, True),

    # Productwise total ads spend
    "ads_spend": MetricDef("ads_spend", "ads_spend", "sku_additive", "money", "sku", True, True, True, True, True, True, True),

    # Old metric name support, but productwise value should come from ads_spend
    "advertising_total": MetricDef("advertising_total", "ads_spend", "sku_additive", "money", "sku", True, True, True, True, True, True, True),

    # Sponsored Product / Brand / Display spend
    "product_spend": MetricDef("product_spend", "product_spend", "sku_additive", "money", "sku", True, True, True, True, True, True, True),
    "brand_spend": MetricDef("brand_spend", "brand_spend", "sku_additive", "money", "sku", True, True, True, True, True, True, True),
    "display_spend": MetricDef("display_spend", "display_spend", "sku_additive", "money", "sku", True, True, True, True, True, True, True),

    # Sponsored Product / Brand / Display sales
    "sp_ads_sales": MetricDef("sp_ads_sales", "sp_ads_sales", "sku_additive", "money", "sku", True, True, True, True, True, True, True),
    "sb_ads_sales": MetricDef("sb_ads_sales", "sb_ads_sales", "sku_additive", "money", "sku", True, True, True, True, True, True, True),
    "sd_ads_sales": MetricDef("sd_ads_sales", "sd_ads_sales", "sku_additive", "money", "sku", True, True, True, True, True, True, True),

    # General ads performance
    "ads_impressions": MetricDef("ads_impressions", "ads_impressions", "sku_additive", "count", "sku", True, True, True, True, True, True, True),
    "ads_clicks": MetricDef("ads_clicks", "ads_clicks", "sku_additive", "count", "sku", True, True, True, True, True, True, True),
    "ads_sale_units": MetricDef("ads_sale_units", "ads_sale_units", "sku_additive", "count", "sku", True, True, True, True, True, True, True),
    "ads_sale_amount": MetricDef("ads_sale_amount", "ads_sale_amount", "sku_additive", "money", "sku", True, True, True, True, True, True, True),

    # -------- OTHER --------
    "misc_transaction": MetricDef("misc_transaction", "misc_transaction", "sku_additive", "money", "sku", True, True, True, True, True, True, True),
    "other": MetricDef("other", "other", "sku_additive", "money", "sku", True, True, True, True, True, True, True),

    # -------- SHIPPING --------
    "shipment_charges": MetricDef("shipment_charges", "shipment_charges", "sku_additive", "money", "sku", True, True, True, True, True, True, True),

    "shipment_fees": MetricDef(
        "shipment_fees", "shipment_fees", "sku_additive", "money", "sku",
        True, True, True, True, True, True, True
    ),
    "rembursement_fee": MetricDef(
        "rembursement_fee", "rembursement_fee", "sku_additive", "money", "sku",
        True, True, True, True, True, True, True
    ),
}

TOTAL_ADDITIVE_METRICS: Dict[str, MetricDef] = {
    "advertising": MetricDef(
        "advertising", "total_ads", "total_additive", "money", "total",
        False, True, True, True,
        False,  # no product filter
        False,  # no product breakdown
        True,   # time breakdown
    ),

    "advertising_total": MetricDef(
        "advertising_total", "total_ads", "total_additive", "money", "total",
        False, True, True, True,
        False,
        False,
        True,
    ),

    "total_ads": MetricDef(
        "total_ads", "total_ads", "total_additive", "money", "total",
        False, True, True, True,
        False,
        False,
        True,
    ),

    "visible_ads": MetricDef(
        "visible_ads", "visible_ads", "total_additive", "money", "total",
        False, True, True, True,
        False,
        False,
        True,
    ),

    "dealsvouchar_ads": MetricDef(
        "dealsvouchar_ads", "dealsvouchar_ads", "total_additive", "money", "total",
        False, True, True, True,
        False,
        False,
        True,
    ),

    "platform_fee": MetricDef(
        "platform_fee", "platform_fee", "total_additive", "money", "total",
        False, True, True, True,
        False,
        False,
        True,
    ),

    "platformfeenew": MetricDef(
        "platformfeenew", "platformfeenew", "total_additive", "money", "total",
        False, True, True, True,
        False,
        False,
        True,
    ),

    "platform_fee_inventory_storage": MetricDef(
        "platform_fee_inventory_storage",
        "platform_fee_inventory_storage",
        "total_additive",
        "money",
        "total",
        False,
        True,
        True,
        True,
        False,
        False,
        True,
    ),

    "cm2_profit": MetricDef(
        "cm2_profit", "total_cm2_profit", "total_additive", "money", "total",
        False, True, True, True,
        False,
        False,
        True,
    ),

    "total_cm2_profit": MetricDef(
        "total_cm2_profit", "total_cm2_profit", "total_additive", "money", "total",
        False, True, True, True,
        False,
        False,
        True,
    ),

    "rembursement_fee": MetricDef(
        "rembursement_fee", "rembursement_fee", "total_additive", "money", "total",
        False, True, True, True,
        False,
        False,
        True,
    ),

    "misc_transaction": MetricDef(
        "misc_transaction", "misc_transaction", "total_additive", "money", "total",
        False, True, True, True,
        False,
        False,
        True,
    ),
}


PRECOMPUTED_METRICS = {

    "profit_percentage": MetricDef("profit_percentage", "profit_percentage", "sku_precomputed", "percentage", "sku", False, True, True, True, True, True, True),
    "cm2_profit_percentage": MetricDef("cm2_profit_percentage", "cm2_profit_percentage", "sku_precomputed", "percentage", "sku", False, True, True, True, True, True, True),
    "acos": MetricDef("acos", "acos", "sku_precomputed", "percentage", "sku", False, True, True, True, True, True, True),
    "reimbursement_vs_sales": MetricDef("reimbursement_vs_sales", "reimbursement_vs_sales", "sku_precomputed", "percentage", "sku", False, True, True, True, True, True, True),
    "cm2_margins": MetricDef("cm2_margins", "cm2_margins", "sku_precomputed", "percentage", "sku", False, True, True, True, True, True, True),
    "rembursment_vs_cm2_margins": MetricDef("rembursment_vs_cm2_margins", "rembursment_vs_cm2_margins", "sku_precomputed", "percentage", "sku", False, True, True, True, True, True, True),
    "promotional_rebates_percentage": MetricDef("promotional_rebates_percentage", "promotional_rebates_percentage", "sku_precomputed", "percentage", "sku", False, True, True, True, True, True, True),
    "unit_wise_profitability": MetricDef("unit_wise_profitability", "unit_wise_profitability", "sku_precomputed", "money", "sku", False, True, True, True, True, True, True),
    "asp": MetricDef("asp", "asp", "sku_precomputed", "money", "sku", False, True, True, True, True, True, True),
    "price_in_gbp": MetricDef("price_in_gbp", "price_in_gbp", "sku_precomputed", "money", "sku", False, True, True, True, True, True, True),
    "sales_mix": MetricDef("sales_mix", "sales_mix", "sku_precomputed", "percentage", "sku", False, True, True, True, True, True, True),
    "profit_mix": MetricDef("profit_mix", "profit_mix", "sku_precomputed", "percentage", "sku", False, True, True, True, True, True, True),
    "tex_and_credits": MetricDef("tex_and_credits", "tex_and_credits", "sku_precomputed", "money", "sku", False, True, True, True, True, True, True),
}

# -------- INVENTORY METRICS --------
INVENTORY_METRIC_DEFS = {
    "available": MetricDef("available", "available", "sku_additive", "count", "sku", True, True, True, True, True, True, True),

    "inbound_quantity": MetricDef("inbound_quantity", "inbound_quantity", "sku_additive", "count", "sku", True, True, True, True, True, True, True),

    "total_reserved_quantity": MetricDef("total_reserved_quantity", "total_reserved_quantity", "sku_additive", "count", "sku", True, True, True, True, True, True, True),

    "unfulfillable_quantity": MetricDef("unfulfillable_quantity", "unfulfillable_quantity", "sku_additive", "count", "sku", True, True, True, True, True, True, True),

    "units_shipped_t30": MetricDef("units_shipped_t30", "units_shipped_t30", "sku_additive", "count", "sku", True, True, True, True, True, True, True),

    "units_shipped_t60": MetricDef("units_shipped_t60", "units_shipped_t60", "sku_additive", "count", "sku", True, True, True, True, True, True, True),

    "units_shipped_t90": MetricDef("units_shipped_t90", "units_shipped_t90", "sku_additive", "count", "sku", True, True, True, True, True, True, True),

    "estimated_excess_quantity": MetricDef("estimated_excess_quantity", "estimated_excess_quantity", "sku_additive", "count", "sku", True, True, True, True, True, True, True),

    # ratios
    "sell_through": MetricDef("sell_through", "sell_through", "sku_precomputed", "percentage", "sku", False, True, True, True, True, True, True),

    "days_of_supply": MetricDef("days_of_supply", "days_of_supply", "sku_precomputed", "count", "sku", False, True, True, True, True, True, True),
}


ALL_METRICS = {
    **SKU_ADDITIVE_METRICS,
    **TOTAL_ADDITIVE_METRICS,
    **PRECOMPUTED_METRICS,
    **INVENTORY_METRIC_DEFS,
}


def get_metric_def(metric_name: str) -> MetricDef:
    key = (metric_name or "").strip().lower()
    if key not in ALL_METRICS:
        raise ValueError(f"unsupported metric: {metric_name}")
    return ALL_METRICS[key]


def validate_metric_compatibility(
    metric_name: str,
    *,
    product_query: Optional[str] = None,
    require_breakdown: bool = False,
) -> tuple[bool, str]:
    """
    Returns (is_valid, reason)
    """

    metric = get_metric_def(metric_name)

    # ❌ product filter not allowed
    if product_query and not metric.supports_product_filter:
        return False, f"{metric_name} does not support product-level queries"

    # ❌ breakdown not allowed
    if require_breakdown and not metric.supports_product_breakdown:
        if metric.supports_time_breakdown:
            return False, f"{metric_name} only supports time breakdown, not product breakdown"
        return False, f"{metric_name} does not support breakdown"

    return True, "ok"

def available_metrics() -> list[str]:
    return sorted(ALL_METRICS.keys())



######################################################################################################################################
# -------------------------
# INVENTORY FETCH
# -------------------------

def _legacy_inventory_snapshot_unused(
    user_id: int,
    metric_name: str,
    month: int,
    year: int,
) -> Dict[str, Any]:

    engine = get_amazon_engine()

    query = text(f"""
        SELECT 
            sku,                                      -- ✅ FIXED
            "product-name" AS product_name, 
            "{metric_name}" AS value
        FROM inventory_aged
        WHERE user_id = :user_id
        AND "snapshot-date" = (
            SELECT MAX("snapshot-date")
            FROM inventory_aged
            WHERE user_id = :user_id
            AND EXTRACT(MONTH FROM "snapshot-date") = :month
            AND EXTRACT(YEAR FROM "snapshot-date") = :year
        )
    """)

    with engine.connect() as conn:
        rows = conn.execute(
            query,
            {
                "user_id": user_id,
                "month": month,
                "year": year,
            },
        ).mappings().all()

    if not rows:
        return {
            "metric": metric_name,
            "total": None,
            "per_sku": [],
            "period_label": f"{month}/{year}",
            "metric_kind": "inventory",
            "note": "No inventory snapshot available for this month",
        }

    per_sku = [
        {
            "sku": r["sku"],                        # ✅ FIXED
            "product_name": r["product_name"],
            "__metric__": float(r["value"] or 0),
        }
        for r in rows
    ]

    total = sum(r["__metric__"] for r in per_sku)

    return {
        "metric": metric_name,
        "total": total,
        "per_sku": per_sku,
        "period_label": f"{month}/{year}",
        "metric_kind": "inventory",
    }


def _inventory_marketplace_id(country: Optional[str]) -> Optional[str]:
    return INVENTORY_MARKETPLACE_BY_COUNTRY.get((country or "").strip().lower())


def _inventory_location_code(country: Optional[str]) -> Optional[str]:
    return INVENTORY_LOCATION_BY_COUNTRY.get((country or "").strip().lower())


def _empty_inventory_snapshot(metric_name: str, month: int, year: int, note: str) -> Dict[str, Any]:
    return {
        "metric": metric_name,
        "total": None,
        "per_sku": [],
        "period_label": f"{month}/{year}",
        "metric_kind": "inventory",
        "note": note,
    }


def _month_end_sql() -> str:
    return "(make_date(:year, :month, 1) + INTERVAL '1 month' - INTERVAL '1 day')::date"


def _month_start_sql() -> str:
    return "make_date(:year, :month, 1)::date"


def _resolve_current_inventory_table(
    engine: Engine,
    user_id: int,
    country: Optional[str],
    month: int,
    year: int,
) -> Optional[tuple[str, MonthKey]]:
    country_key = (country or "").strip().lower()
    if country_key == "gb":
        country_key = "uk"
    if country_key == "usa":
        country_key = "us"
    if country_key not in {"uk", "us"}:
        return None

    exact = f"currentinventory_{int(user_id)}_{country_key}_{MONTH_NUM_TO_NAME[int(month)]}{int(year)}_table"
    if table_exists(engine, exact):
        return exact, MonthKey(year=int(year), month=int(month))

    prefix = f"currentinventory_{int(user_id)}_{country_key}_"
    query = text("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_name LIKE :pattern
    """)

    with engine.connect() as conn:
        names = [row[0] for row in conn.execute(query, {"pattern": prefix + "%_table"}).fetchall()]

    candidates: list[tuple[MonthKey, str]] = []
    pattern = re.compile(rf"^{re.escape(prefix)}([a-z]+)(\d{{4}})_table$")
    for name in names:
        match = pattern.match(name)
        if not match:
            continue
        month_name, year_text = match.groups()
        month_num = MONTH_NAME_TO_NUM.get(month_name)
        if not month_num:
            continue
        try:
            candidates.append((MonthKey(year=int(year_text), month=month_num), name))
        except ValueError:
            continue

    if not candidates:
        return None

    requested = (int(year), int(month))
    prior = [(mk, name) for mk, name in candidates if (mk.year, mk.month) <= requested]
    if not prior:
        return None

    selected = max(prior, key=lambda item: (item[0].year, item[0].month))
    return selected[1], selected[0]


def _get_current_inventory_snapshot(
    user_id: int,
    metric_name: str,
    month: int,
    year: int,
    country: Optional[str] = None,
) -> Dict[str, Any]:
    metric_key = (metric_name or "").strip().lower()
    column_expr = CURRENT_INVENTORY_COLUMN_BY_METRIC.get(metric_key)
    if not column_expr:
        return _empty_inventory_snapshot(
            metric_key,
            month,
            year,
            "No current inventory fallback is available for this metric",
        )

    engine = get_engine()
    resolved = _resolve_current_inventory_table(engine, user_id, country, month, year)
    if not resolved:
        return _empty_inventory_snapshot(
            metric_key,
            month,
            year,
            "No current inventory table is available for this user/country",
        )

    table_name, table_month = resolved
    query = text(f"""
        SELECT
            "SKU" AS sku,
            "Product Name" AS product_name,
            {column_expr} AS value
        FROM "{table_name}"
        WHERE COALESCE(LOWER(TRIM("Product Name")), '') <> 'total'
    """)

    try:
        with engine.connect() as conn:
            rows = conn.execute(query).mappings().all()
    except Exception:
        return _empty_inventory_snapshot(
            metric_key,
            month,
            year,
            "Current inventory table could not be read",
        )

    if not rows:
        return _empty_inventory_snapshot(
            metric_key,
            table_month.month,
            table_month.year,
            f"No rows found in {table_name}",
        )

    per_sku = [
        {
            "sku": r["sku"],
            "product_name": r["product_name"],
            "__metric__": float(r["value"] or 0),
        }
        for r in rows
    ]
    total = sum(r["__metric__"] for r in per_sku)

    return {
        "metric": metric_key,
        "total": total,
        "per_sku": per_sku,
        "period_label": table_month.label,
        "metric_kind": "inventory",
        "source_table": table_name,
        "snapshot_date": None,
        "country": country,
    }


def _get_monthwise_inventory_snapshot(
    user_id: int,
    metric_name: str,
    month: int,
    year: int,
    country: Optional[str] = None,
) -> Dict[str, Any]:
    metric_key = (metric_name or "").strip().lower()
    column_name = MONTHWISE_INVENTORY_COLUMN_BY_METRIC.get(metric_key)
    if not column_name:
        return _empty_inventory_snapshot(
            metric_key,
            month,
            year,
            "No monthwise inventory fallback is available for this metric",
        )

    engine = get_amazon_engine()
    marketplace_id = _inventory_marketplace_id(country)
    location_code = _inventory_location_code(country)
    country_filter = ""
    params: Dict[str, Any] = {
        "user_id": user_id,
        "month": int(month),
        "year": int(year),
    }

    if marketplace_id or location_code:
        country_clauses = []
        if marketplace_id:
            country_clauses.append("marketplace_id = :marketplace_id")
            params["marketplace_id"] = marketplace_id
        if location_code:
            country_clauses.append("LOWER(TRIM(location)) = :location_code")
            params["location_code"] = location_code
        country_filter = " AND (" + " OR ".join(country_clauses) + ")"

    query = text(f"""
        WITH latest_snapshot AS (
            SELECT MAX(date::date) AS latest_date
            FROM monthwise_inventory
            WHERE user_id = :user_id
              AND LOWER(TRIM(disposition)) = 'sellable'
              {country_filter}
              AND date::date >= {_month_start_sql()}
              AND date::date <= {_month_end_sql()}
        )
        SELECT
            msku AS sku,
            COALESCE(NULLIF(TRIM(product_name), ''), NULLIF(TRIM(title), ''), msku) AS product_name,
            SUM(COALESCE({column_name}, 0)) AS value,
            (SELECT latest_date FROM latest_snapshot) AS snapshot_date
        FROM monthwise_inventory
        WHERE user_id = :user_id
          AND LOWER(TRIM(disposition)) = 'sellable'
          {country_filter}
          AND date::date = (SELECT latest_date FROM latest_snapshot)
        GROUP BY msku, COALESCE(NULLIF(TRIM(product_name), ''), NULLIF(TRIM(title), ''), msku)
        ORDER BY value DESC
    """)

    try:
        with engine.connect() as conn:
            rows = conn.execute(query, params).mappings().all()
    except Exception:
        return _empty_inventory_snapshot(
            metric_key,
            month,
            year,
            "Monthwise inventory table could not be read",
        )

    if not rows:
        return _empty_inventory_snapshot(
            metric_key,
            month,
            year,
            "No inventory snapshot available in monthwise_inventory",
        )

    snapshot_date = rows[0].get("snapshot_date")
    per_sku = [
        {
            "sku": r["sku"],
            "product_name": r["product_name"],
            "__metric__": float(r["value"] or 0),
        }
        for r in rows
    ]
    total = sum(r["__metric__"] for r in per_sku)

    return {
        "metric": metric_key,
        "total": total,
        "per_sku": per_sku,
        "period_label": snapshot_date.strftime("%b %Y") if snapshot_date else f"{month}/{year}",
        "metric_kind": "inventory",
        "source_table": "monthwise_inventory",
        "snapshot_date": snapshot_date.isoformat() if snapshot_date else None,
        "country": country,
    }


def get_inventory_snapshot(
    user_id: int,
    metric_name: str,
    month: int,
    year: int,
    country: Optional[str] = None,
) -> Dict[str, Any]:

    metric_key = (metric_name or "").strip().lower()
    column_expr = INVENTORY_AGED_COLUMN_BY_METRIC.get(metric_key)
    if not column_expr:
        current_snapshot = _get_current_inventory_snapshot(user_id, metric_key, month, year, country)
        if current_snapshot.get("per_sku") or current_snapshot.get("total") is not None:
            return current_snapshot
        return _get_monthwise_inventory_snapshot(user_id, metric_key, month, year, country)

    engine = get_amazon_engine()
    marketplace_id = _inventory_marketplace_id(country)
    marketplace_filter = ""
    params: Dict[str, Any] = {
        "user_id": user_id,
        "month": int(month),
        "year": int(year),
    }

    if marketplace_id:
        marketplace_filter = " AND marketplace = :marketplace_id"
        params["marketplace_id"] = marketplace_id

    query = text(f"""
        WITH latest_snapshot AS (
            SELECT MAX("snapshot-date") AS latest_date
            FROM inventory_aged
            WHERE user_id = :user_id
              {marketplace_filter}
              AND "snapshot-date" >= {_month_start_sql()}
              AND "snapshot-date" <= {_month_end_sql()}
        )
        SELECT
            sku,
            "product-name" AS product_name,
            {column_expr} AS value,
            "snapshot-date" AS snapshot_date,
            marketplace
        FROM inventory_aged
        WHERE user_id = :user_id
          {marketplace_filter}
          AND "snapshot-date" = (SELECT latest_date FROM latest_snapshot)
        ORDER BY COALESCE({column_expr}, 0) DESC
    """)

    try:
        with engine.connect() as conn:
            rows = conn.execute(query, params).mappings().all()
    except Exception:
        monthwise_snapshot = _get_monthwise_inventory_snapshot(user_id, metric_key, month, year, country)
        if monthwise_snapshot.get("per_sku") or monthwise_snapshot.get("total") is not None:
            return monthwise_snapshot
        return _get_current_inventory_snapshot(user_id, metric_key, month, year, country)

    if not rows:
        monthwise_snapshot = _get_monthwise_inventory_snapshot(user_id, metric_key, month, year, country)
        if monthwise_snapshot.get("per_sku") or monthwise_snapshot.get("total") is not None:
            return monthwise_snapshot
        return _get_current_inventory_snapshot(user_id, metric_key, month, year, country)

    snapshot_date = rows[0].get("snapshot_date")
    per_sku = [
        {
            "sku": r["sku"],
            "product_name": r["product_name"],
            "__metric__": float(r["value"] or 0),
        }
        for r in rows
    ]

    total = sum(r["__metric__"] for r in per_sku)

    return {
        "metric": metric_key,
        "total": total,
        "per_sku": per_sku,
        "period_label": snapshot_date.strftime("%b %Y") if snapshot_date else f"{month}/{year}",
        "metric_kind": "inventory",
        "source_table": "inventory_aged",
        "snapshot_date": snapshot_date.isoformat() if snapshot_date else None,
        "country": country,
    }


FORECAST_FILE_KINDS = {
    "inventory": "inventory_forecast",
    "demand": "forecasts_for",
    "pnl": "pnl_forecast",
}


def _stored_file_month_name(month: Optional[int]) -> Optional[str]:
    if not month:
        return None
    try:
        return MONTH_NUM_TO_NAME[int(month)]
    except Exception:
        return None


def _load_latest_forecast_file(
    user_id: int,
    country: str,
    kind: str,
    month: Optional[int] = None,
    year: Optional[int] = None,
) -> Optional[Dict[str, Any]]:
    validate_user_id(user_id)
    country = normalize_country(country)
    month_name = _stored_file_month_name(month)
    year_text = str(year) if year else None

    engine = get_engine()
    params: Dict[str, Any] = {
        "user_id": int(user_id),
        "country": country,
        "kind": kind,
        "month": month_name,
        "year": year_text,
    }

    query = text("""
        SELECT filename, kind, month, year, country, data, created_at
        FROM stored_files
        WHERE user_id = :user_id
          AND LOWER(country) = :country
          AND kind = :kind
        ORDER BY
          CASE
            WHEN (:month IS NOT NULL AND :year IS NOT NULL AND LOWER(month) = :month AND year = :year) THEN 0
            ELSE 1
          END,
          created_at DESC,
          id DESC
        LIMIT 1
    """)

    with engine.connect() as conn:
        row = conn.execute(query, params).mappings().first()

    if not row:
        return None

    data = row.get("data")
    if isinstance(data, memoryview):
        data = data.tobytes()

    exact_period_match = True
    if month_name and year_text:
        exact_period_match = (
            str(row.get("month") or "").strip().lower() == month_name
            and str(row.get("year") or "").strip() == year_text
        )

    return {
        "filename": row.get("filename"),
        "kind": row.get("kind"),
        "month": row.get("month"),
        "year": row.get("year"),
        "country": row.get("country"),
        "data": bytes(data or b""),
        "created_at": row.get("created_at"),
        "exact_period_match": exact_period_match,
    }


_FORECAST_MONTH_RE = re.compile(r"^[A-Za-z]{3}'\d{2}(?:\s+Sold)?$")


def _parse_forecast_month_label(label: str) -> Optional[datetime]:
    clean = str(label or "").strip().replace(" Sold", "").replace("'", "")
    try:
        return datetime.strptime(clean, "%b%y")
    except Exception:
        return None


def _forecast_month_columns(df: pd.DataFrame) -> List[str]:
    columns = [str(c).strip() for c in df.columns]
    month_cols = [col for col in columns if _FORECAST_MONTH_RE.match(col)]
    return sorted(
        month_cols,
        key=lambda col: _parse_forecast_month_label(col) or datetime.max,
    )


def _forecast_columns_from_month(
    columns: List[str],
    month: Optional[int],
    year: Optional[int],
) -> List[str]:
    forecast_cols = [col for col in columns if not str(col).endswith(" Sold")]
    if not month or not year:
        return forecast_cols

    selected = datetime(int(year), int(month), 1)
    out = []
    for col in forecast_cols:
        parsed = _parse_forecast_month_label(col)
        if parsed and parsed >= selected:
            out.append(col)
    return out


def _forecast_float(value: Any) -> float:
    try:
        numeric = pd.to_numeric(value, errors="coerce")
        if pd.isna(numeric):
            return 0.0
        return float(numeric)
    except Exception:
        return 0.0


def _read_inventory_forecast_df(file_row: Dict[str, Any], country: str) -> pd.DataFrame:
    raw = BytesIO(file_row["data"])
    headers = [0] if country == "global" else [6, 0]
    last_error: Optional[Exception] = None

    for header in headers:
        try:
            raw.seek(0)
            df = pd.read_excel(raw, header=header, engine="openpyxl")
            df.columns = [str(c).strip() for c in df.columns]
            if "sku" in df.columns or "Product Name" in df.columns:
                return df
        except Exception as exc:
            last_error = exc

    if last_error:
        raise last_error
    raise ValueError("Inventory forecast workbook has no usable header row")


def get_inventory_forecast_snapshot(
    user_id: int,
    country: str,
    month: Optional[int] = None,
    year: Optional[int] = None,
    product_query: Optional[str] = None,
) -> Dict[str, Any]:
    country = normalize_country(country)
    file_row = _load_latest_forecast_file(
        user_id=user_id,
        country=country,
        kind=FORECAST_FILE_KINDS["inventory"],
        month=month,
        year=year,
    )
    if not file_row:
        return {
            "available": False,
            "kind": "inventory_forecast",
            "country": country,
            "rows": [],
            "totals": {},
            "note": "No inventory forecast file found",
        }

    df = _read_inventory_forecast_df(file_row, country)
    df = df.where(pd.notnull(df), None)
    month_cols = _forecast_month_columns(df)
    sold_cols = [col for col in month_cols if str(col).endswith(" Sold")]
    forecast_cols = _forecast_columns_from_month(month_cols, month, year)
    forecast_cols_3 = forecast_cols[:3]

    product_col = "Product Name" if "Product Name" in df.columns else "product_name"
    sku_col = "sku" if "sku" in df.columns else "SKU" if "SKU" in df.columns else None

    working = df.copy()
    if sku_col and sku_col in working.columns:
        working = working[working[sku_col].astype(str).str.lower() != "total"]

    if product_query:
        pq = str(product_query).strip().lower()
        mask = pd.Series(False, index=working.index)
        if product_col in working.columns:
            mask = mask | working[product_col].astype(str).str.lower().str.contains(re.escape(pq), na=False)
        if sku_col:
            mask = mask | working[sku_col].astype(str).str.lower().str.contains(re.escape(pq), na=False)
        working = working[mask]

    rows: List[Dict[str, Any]] = []
    for _, row in working.iterrows():
        forecast_values = {
            col: _forecast_float(row.get(col))
            for col in forecast_cols_3
        }
        sold_values = {
            col: _forecast_float(row.get(col))
            for col in sold_cols[-3:]
        }
        projected_total = _forecast_float(row.get("Projected Sales Total")) or sum(forecast_values.values())
        inventory_at_month_end = _forecast_float(row.get("Inventory at Month End"))
        dispatch = _forecast_float(row.get("Dispatch"))

        rows.append({
            "sku": str(row.get(sku_col) or "").strip() if sku_col else "",
            "product_name": str(row.get(product_col) or "").strip() if product_col in working.columns else "",
            "sold": sold_values,
            "forecast": forecast_values,
            "projected_sales_total": projected_total,
            "inventory_at_month_end": inventory_at_month_end,
            "dispatch": dispatch,
        })

    totals = {
        "forecast_units": {
            col: sum(float(r["forecast"].get(col, 0.0) or 0.0) for r in rows)
            for col in forecast_cols_3
        },
        "projected_sales_total": sum(float(r.get("projected_sales_total", 0.0) or 0.0) for r in rows),
        "inventory_at_month_end": sum(float(r.get("inventory_at_month_end", 0.0) or 0.0) for r in rows),
        "dispatch": sum(float(r.get("dispatch", 0.0) or 0.0) for r in rows),
    }

    rows = sorted(rows, key=lambda r: float(r.get("projected_sales_total", 0.0) or 0.0), reverse=True)

    return {
        "available": True,
        "kind": "inventory_forecast",
        "country": country,
        "filename": file_row.get("filename"),
        "stored_month": file_row.get("month"),
        "stored_year": file_row.get("year"),
        "exact_period_match": file_row.get("exact_period_match", True),
        "month_columns": month_cols,
        "sold_columns": sold_cols[-3:],
        "forecast_columns": forecast_cols_3,
        "rows": rows,
        "totals": totals,
        "row_count": len(rows),
    }


def get_pnl_forecast_snapshot(
    user_id: int,
    country: str,
    month: Optional[int] = None,
    year: Optional[int] = None,
    product_query: Optional[str] = None,
) -> Dict[str, Any]:
    country = normalize_country(country)
    file_row = _load_latest_forecast_file(
        user_id=user_id,
        country=country,
        kind=FORECAST_FILE_KINDS["pnl"],
        month=month,
        year=year,
    )
    if not file_row:
        return {
            "available": False,
            "kind": "pnl_forecast",
            "country": country,
            "rows": [],
            "totals": {},
            "note": "No P&L forecast file found",
        }

    df = pd.read_excel(BytesIO(file_row["data"]), engine="openpyxl")
    df.columns = [str(c).strip() for c in df.columns]
    df = df.where(pd.notnull(df), None)

    sku_col = "sku"
    product_col = "product_name" if "product_name" in df.columns else "Product Name"
    rows_df = df.copy()
    if product_query:
        pq = str(product_query).strip().lower()
        mask = rows_df[sku_col].astype(str).str.lower().str.contains(re.escape(pq), na=False)
        if product_col in rows_df.columns:
            mask = mask | rows_df[product_col].astype(str).str.lower().str.contains(re.escape(pq), na=False)
        rows_df = rows_df[mask]

    total_row = df[df[sku_col].astype(str).str.lower() == "total"].head(1)
    total_source = rows_df if product_query else total_row

    def sum_col(col: str) -> float:
        if col not in total_source.columns or total_source.empty:
            return 0.0
        return float(pd.to_numeric(total_source[col], errors="coerce").fillna(0).sum())

    def row_value(name: str) -> float:
        matched = df[df[sku_col].astype(str).str.lower() == name.lower()]
        if matched.empty or "value" not in matched.columns:
            return 0.0
        return float(pd.to_numeric(matched["value"], errors="coerce").fillna(0).sum())

    rows: List[Dict[str, Any]] = []
    product_rows = rows_df[~rows_df[sku_col].astype(str).str.lower().isin({
        "total",
        "platform_fees_total",
        "advertising_total",
        "cm2profit_total",
        "cm2margin_total",
        "acos_total",
    })]
    for _, row in product_rows.iterrows():
        rows.append({
            "sku": str(row.get(sku_col) or "").strip(),
            "product_name": str(row.get(product_col) or "").strip() if product_col in product_rows.columns else "",
            "forecast_units": _forecast_float(row.get("forecast_sum")),
            "forecast_sales": _forecast_float(row.get("Total_Sales_sum")),
            "forecast_profit": _forecast_float(row.get("profit_sum")),
        })

    rows = sorted(rows, key=lambda r: float(r.get("forecast_sales", 0.0) or 0.0), reverse=True)

    return {
        "available": True,
        "kind": "pnl_forecast",
        "country": country,
        "filename": file_row.get("filename"),
        "stored_month": file_row.get("month"),
        "stored_year": file_row.get("year"),
        "exact_period_match": file_row.get("exact_period_match", True),
        "totals": {
            "forecast_units": sum_col("forecast_sum"),
            "forecast_sales": sum_col("Total_Sales_sum"),
            "forecast_profit": sum_col("profit_sum"),
            "forecast_cm2_profit": row_value("cm2profit_total"),
            "forecast_ad_spend": row_value("advertising_total"),
            "forecast_acos": row_value("acos_total"),
            "forecast_cm2_margin": row_value("cm2margin_total"),
        },
        "rows": rows,
        "row_count": len(rows),
    }


INVENTORY_METRICS = {
    "available",
    "inbound_quantity",
    "total_reserved_quantity",
    "unfulfillable_quantity",
    "sell_through",
    "days_of_supply",
    "units_shipped_t30",
    "units_shipped_t60",
    "units_shipped_t90",
    "estimated_excess_quantity",
}

FINANCE_METRICS = set(SKU_ADDITIVE_METRICS.keys()) | set(PRECOMPUTED_METRICS.keys()) | set(TOTAL_ADDITIVE_METRICS.keys())
