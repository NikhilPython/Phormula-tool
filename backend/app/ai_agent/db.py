from __future__ import annotations
from typing import Dict, Optional, Any
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Iterable, List

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from config import Config


_COUNTRY_RE = re.compile(r"^[a-z]{2}$")

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
    "advertising_total",
    "platform_fee",
    "amazon_fee",
    "fba_fees",
    "selling_fees",
    "refund_sales",
    "asp",
}

def _format_value(value: float, metric_name: str, country: str) -> str:
    # 🔥 FIX: already % values — DO NOT use .2%
    if metric_name in {"sales_mix", "profit_mix", "acos"}:
        return f"{value:.2f}%"

    if metric_name == "total_quantity":
        return f"{value:,.0f}"

    if metric_name in MONEY_METRICS:
        if country and country.lower() == "uk":
            return f"£{value:,.2f}"
        return f"{value:,.2f}"

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
    return f"nse_{user_id}_{country}_{mk.table_suffix}"


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

    # ✅ Fix: make only these fee columns absolute
    for col in ["platform_fee", "platform_fee_inventory_storage"]:
        if col in df.columns:
            df[col] = df[col].abs()

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
    "advertising_total": MetricDef("advertising_total", "advertising_total", "sku_additive", "money", "sku", True, True, True, True, True, True, True),

    # -------- OTHER --------
    "misc_transaction": MetricDef("misc_transaction", "misc_transaction", "sku_additive", "money", "sku", True, True, True, True, True, True, True),
    "other": MetricDef("other", "other", "sku_additive", "money", "sku", True, True, True, True, True, True, True),

    # -------- SHIPPING --------
    "shipment_charges": MetricDef("shipment_charges", "shipment_charges", "sku_additive", "money", "sku", True, True, True, True, True, True, True),
}

TOTAL_ADDITIVE_METRICS: Dict[str, MetricDef] = {
    "advertising": MetricDef(
        "advertising", "advertising_total", "total_additive", "money", "total",
        False, True, True, True,
        False,  # ❌ no product filter
        False,  # ❌ no product breakdown
        True,   # ✅ time breakdown
    ),

    "advertising_total": MetricDef(
        "advertising_total", "advertising_total", "total_additive", "money", "total",
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
        "cm2_profit", "cm2_profit", "total_additive", "money", "total",
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

def get_inventory_snapshot(
    user_id: int,
    metric_name: str,
    month: int,
    year: int,
) -> Dict[str, Any]:

    engine = get_amazon_engine()

    query = text(f"""
        SELECT "product-name" AS product_name, "{metric_name}" AS value
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

    # -------- 🔥 HANDLE NO DATA CASE --------
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