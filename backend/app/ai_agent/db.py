from __future__ import annotations
from typing import Dict
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
    if metric_name in {"sales_mix", "profit_mix"}:
        return f"{value:.2%}"

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
    numerator: str | None = None
    denominator: str | None = None
    multiplier: float = 1.0
    absolute: bool = False


SKU_ADDITIVE_METRICS: Dict[str, MetricDef] = {
    "sales": MetricDef(name="sales", column="net_sales", kind="sku_additive"),
    "net_sales": MetricDef(name="net_sales", column="net_sales", kind="sku_additive"),
    "gross_sales": MetricDef(name="gross_sales", column="gross_sales", kind="sku_additive"),
    "quantity": MetricDef(name="quantity", column="quantity", kind="sku_additive"),
    "units": MetricDef(name="units", column="quantity", kind="sku_additive"),
    "return_quantity": MetricDef(name="return_quantity", column="return_quantity", kind="sku_additive"),
    "total_quantity": MetricDef(name="total_quantity", column="total_quantity", kind="sku_additive"),
    "refund_sales": MetricDef(name="refund_sales", column="refund_sales", kind="sku_additive"),
    "tax": MetricDef(name="tax", column="net_taxes", kind="sku_additive"),
    "net_taxes": MetricDef(name="net_taxes", column="net_taxes", kind="sku_additive"),
    "credits": MetricDef(name="credits", column="net_credits", kind="sku_additive"),
    "net_credits": MetricDef(name="net_credits", column="net_credits", kind="sku_additive"),
    "tax_and_credits": MetricDef(name="tax_and_credits", column="tex_and_credits", kind="sku_additive"),
    "tex_and_credits": MetricDef(name="tex_and_credits", column="tex_and_credits", kind="sku_additive"),
    "cogs": MetricDef(name="cogs", column="cost_of_unit_sold", kind="sku_additive"),
    "cost_of_unit_sold": MetricDef(name="cost_of_unit_sold", column="cost_of_unit_sold", kind="sku_additive"),
    "selling_fees": MetricDef(name="selling_fees", column="selling_fees", kind="sku_additive"),
    "refund_selling_fees": MetricDef(name="refund_selling_fees", column="refund_selling_fees", kind="sku_additive"),
    "fba_fees": MetricDef(name="fba_fees", column="fba_fees", kind="sku_additive"),
    "amazon_fee": MetricDef(name="amazon_fee", column="amazon_fee", kind="sku_additive"),
    "profit": MetricDef(name="profit", column="profit", kind="sku_additive"),
    "lost_total": MetricDef(name="lost_total", column="lost_total", kind="sku_additive"),
    "product_sales": MetricDef(name="product_sales", column="product_sales", kind="sku_additive"),
}

TOTAL_ADDITIVE_METRICS: Dict[str, MetricDef] = {
    "advertising": MetricDef(name="advertising", column="advertising_total", kind="total_additive"),
    "advertising_total": MetricDef(name="advertising_total", column="advertising_total", kind="total_additive"),
    "visible_ads": MetricDef(name="visible_ads", column="visible_ads", kind="total_additive"),
    "dealsvouchar_ads": MetricDef(name="dealsvouchar_ads", column="dealsvouchar_ads", kind="total_additive"),
    "platform_fee": MetricDef(name="platform_fee", column="platform_fee", kind="total_additive"),
    "platformfeenew": MetricDef(name="platformfeenew", column="platformfeenew", kind="total_additive"),
    "platform_fee_inventory_storage": MetricDef(
        name="platform_fee_inventory_storage",
        column="platform_fee_inventory_storage",
        kind="total_additive",
    ),
    "cm2_profit": MetricDef(name="cm2_profit", column="cm2_profit", kind="total_additive"),
    "rembursement_fee": MetricDef(name="rembursement_fee", column="rembursement_fee", kind="total_additive"),
    "misc_transaction": MetricDef(name="misc_transaction", column="misc_transaction", kind="total_additive"),
}

RATIO_METRICS: Dict[str, MetricDef] = {
    "profit_percentage": MetricDef(
        name="profit_percentage",
        column="profit_percentage",
        kind="ratio",
        numerator="profit",
        denominator="net_sales",
        multiplier=100.0,
    ),
    "cm2_profit_percentage": MetricDef(
        name="cm2_profit_percentage",
        column="cm2_profit_percentage",
        kind="ratio",
        numerator="cm2_profit",
        denominator="net_sales",
        multiplier=100.0,
    ),
    "acos": MetricDef(
        name="acos",
        column="acos",
        kind="ratio",
        numerator="advertising_total",
        denominator="net_sales",
        multiplier=100.0,
        absolute=True,
    ),
    "reimbursement_vs_sales": MetricDef(
        name="reimbursement_vs_sales",
        column="reimbursement_vs_sales",
        kind="ratio",
        numerator="rembursement_fee",
        denominator="net_sales",
        multiplier=100.0,
        absolute=True,
    ),
    "cm2_margins": MetricDef(
        name="cm2_margins",
        column="cm2_margins",
        kind="ratio",
        numerator="cm2_profit",
        denominator="net_sales",
        multiplier=100.0,
    ),
    "rembursment_vs_cm2_margins": MetricDef(
        name="rembursment_vs_cm2_margins",
        column="rembursment_vs_cm2_margins",
        kind="ratio",
        numerator="rembursement_fee",
        denominator="cm2_profit",
        multiplier=100.0,
        absolute=True,
    ),
    "asp": MetricDef(
        name="asp",
        column="asp",
        kind="ratio",
        numerator="net_sales",
        denominator="total_quantity",
        multiplier=1.0,
    ),
    "unit_wise_profitability": MetricDef(
        name="unit_wise_profitability",
        column="unit_wise_profitability",
        kind="ratio",
        numerator="profit",
        denominator="total_quantity",
        multiplier=1.0,
    ),
}

ALL_METRICS: Dict[str, MetricDef] = {
    **SKU_ADDITIVE_METRICS,
    **TOTAL_ADDITIVE_METRICS,
    **RATIO_METRICS,
}


def get_metric_def(metric_name: str) -> MetricDef:
    key = (metric_name or "").strip().lower()
    if key not in ALL_METRICS:
        raise ValueError(f"unsupported metric: {metric_name}")
    return ALL_METRICS[key]


def available_metrics() -> list[str]:
    return sorted(ALL_METRICS.keys())