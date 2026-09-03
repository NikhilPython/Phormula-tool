import pandas as pd
from sqlalchemy import create_engine, text
import os
import calendar

from app.utils.formulas_utils import uk_sales


db_url = os.getenv("DATABASE_URL")
admin_db_url = os.getenv("DATABASE_ADMIN_URL")

phormula_engine = create_engine(db_url)
admin_engine = create_engine(admin_db_url)


MONTH_MAP = {
    1: "january", 2: "february", 3: "march",
    4: "april", 5: "may", 6: "june",
    7: "july", 8: "august", 9: "september",
    10: "october", 11: "november", 12: "december"
}

QUARTER_MONTHS = {
    "Q1": [1, 2, 3],
    "Q2": [4, 5, 6],
    "Q3": [7, 8, 9],
    "Q4": [10, 11, 12]
}


# ---------- LABEL HELPERS ----------

def month_label(month: int, year: int) -> str:
    return f"{MONTH_MAP[month].capitalize()[:3]}'{str(year)[-2:]}"


def quarter_label(q: str, year: int) -> str:
    return f"{q}'{str(year)[-2:]}"


# ---------- DB HELPERS ----------

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


def get_gbp_to_usd_rate(month: int, year: int) -> float:
    """
    Fetch GBP -> USD conversion rate for the selected month/year.
    If no rate is found, fallback to 1 to avoid breaking the chart.
    """
    month_name = MONTH_MAP[int(month)].lower()

    with admin_engine.connect() as conn:
        rate = conn.execute(
            text("""
                SELECT conversion_rate
                FROM currency_conversion
                WHERE LOWER(user_currency) = 'gbp'
                  AND LOWER(selected_currency) = 'usd'
                  AND LOWER(month) = :month
                  AND year = :year
                ORDER BY id DESC
                LIMIT 1
            """),
            {
                "month": month_name,
                "year": int(year),
            }
        ).scalar()

    return float(rate or 1)


def calc_asp(net_sales, units):
    net_sales = float(net_sales or 0)
    units = float(units or 0)

    if units == 0:
        return 0.0

    return float(net_sales / units)


# ---------- TARGET BUILDER ----------

def build_targets(period: str, timeline: str, year: int):
    period = period.lower().strip()

    if period == "monthly":
        month = int(timeline)

        prev_month = month - 1
        prev_year = year

        if prev_month == 0:
            prev_month = 12
            prev_year -= 1

        return [
            {
                "type": "monthly",
                "month": month,
                "year": year,
                "label": month_label(month, year),
            },
            {
                "type": "monthly",
                "month": prev_month,
                "year": prev_year,
                "label": month_label(prev_month, prev_year),
            },
            {
                "type": "monthly",
                "month": month,
                "year": year - 1,
                "label": month_label(month, year - 1),
            },
        ]

    if period == "quarterly":
        q = timeline.upper()
        q_num = int(q[1])

        prev_q_num = q_num - 1
        prev_year = year

        if prev_q_num == 0:
            prev_q_num = 4
            prev_year -= 1

        return [
            {
                "type": "quarterly",
                "quarter": q,
                "year": year,
                "label": quarter_label(q, year),
            },
            {
                "type": "quarterly",
                "quarter": f"Q{prev_q_num}",
                "year": prev_year,
                "label": quarter_label(f"Q{prev_q_num}", prev_year),
            },
            {
                "type": "quarterly",
                "quarter": q,
                "year": year - 1,
                "label": quarter_label(q, year - 1),
            },
        ]

    if period == "yearly":
        return [
            {
                "type": "yearly",
                "year": year,
                "label": str(year),
            },
            {
                "type": "yearly",
                "year": year - 1,
                "label": str(year - 1),
            },
        ]

    raise ValueError(f"Unsupported period: {period}")


# ---------- TABLE NAME HELPERS ----------

def get_raw_sales_table(user_id, country, month=None, year=None):
    """
    Raw uploaded monthly data table.

    For global monthly trend we DO NOT use:
        user_{user_id}_total_country_global_data

    Instead, global monthly trend combines:
        user_{user_id}_uk_{month}{year}_data
        user_{user_id}_us_{month}{year}_data
    """
    country = str(country).strip().lower()

    if month is None or year is None:
        raise ValueError("month and year are required for raw tables")

    return f"user_{user_id}_{country}_{MONTH_MAP[int(month)]}{int(year)}_data"


def get_skuwise_monthly_table(user_id, country, month, year):
    country = str(country).strip().lower()

    if country == "global":
        return f"skuwisemonthly_{user_id}_global_{MONTH_MAP[int(month)]}{int(year)}_table"

    return f"skuwisemonthly_{user_id}_{country}_{MONTH_MAP[int(month)]}{int(year)}"


# ---------- MONTHLY DAILY FETCHERS ----------

def _empty_daily_result(month: int, year: int):
    last_day = calendar.monthrange(int(year), int(month))[1]
    full_days = list(range(1, last_day + 1))

    return {
        "xType": "day",
        "x": full_days,
        "net_sales": [0.0] * len(full_days),
        "units": [0.0] * len(full_days),
        "asp": [0.0] * len(full_days),
    }


def fetch_monthly_daily_single_country(conn, user_id, country, month, year):
    """
    Fetch day-wise net_sales and units for one country from:
        user_{user_id}_{country}_{month}{year}_data
    """
    country = str(country).strip().lower()
    table = get_raw_sales_table(user_id, country, month, year)

    if not table_exists(conn, table):
        return None

    q = text(f'SELECT * FROM public."{table}"')
    df = pd.read_sql(q, conn)

    if df.empty:
        return None

    if "date_time" not in df.columns:
        return None

    date_str = (
        df["date_time"]
        .astype(str)
        .str.strip()
        .str.replace(
            r"\s+(PST|PDT|UTC|GMT|EST|EDT|CST|CDT|MST|MDT)$",
            "",
            regex=True
        )
    )

    df["__dt__"] = pd.to_datetime(
        date_str,
        errors="coerce",
        format="mixed"
    )

    df = df[
        (df["__dt__"].dt.month == int(month)) &
        (df["__dt__"].dt.year == int(year))
    ]

    if df.empty:
        return _empty_daily_result(month, year)

    df["__day__"] = df["__dt__"].dt.date

    df["__ship_qty__"] = pd.to_numeric(
        df.get("quantity"),
        errors="coerce"
    ).fillna(0)

    df.loc[
        ~df.get("type", "").astype(str).str.contains("Shipment", case=False, na=False),
        "__ship_qty__"
    ] = 0

    rows = []

    for day, g in df.groupby("__day__", sort=True):
        net_sales_total, _, _ = uk_sales(g)
        units_total = float(g["__ship_qty__"].sum())

        rows.append({
            "day": day,
            "net_sales": float(net_sales_total),
            "units": units_total,
        })

    if not rows:
        return _empty_daily_result(month, year)

    out = pd.DataFrame(rows)
    out["day_num"] = pd.to_datetime(out["day"]).dt.day

    last_day = calendar.monthrange(int(year), int(month))[1]
    full_days = list(range(1, last_day + 1))

    ns_map = dict(zip(out["day_num"].tolist(), out["net_sales"].tolist()))
    un_map = dict(zip(out["day_num"].tolist(), out["units"].tolist()))

    net_sales_list = [float(ns_map.get(d, 0.0)) for d in full_days]
    units_list = [float(un_map.get(d, 0.0)) for d in full_days]
    asp_list = [
        calc_asp(net_sales_list[i], units_list[i])
        for i in range(len(full_days))
    ]

    return {
        "xType": "day",
        "x": full_days,
        "net_sales": net_sales_list,
        "units": units_list,
        "asp": asp_list,
    }


def fetch_monthly_daily_global(conn, user_id, month, year):
    """
    Global monthly performance trend.

    Logic:
    1. Read UK raw monthly table.
    2. Read US raw monthly table.
    3. Calculate daily net_sales and units separately.
    4. Convert UK net_sales GBP -> USD.
    5. Add UK USD net_sales + US net_sales day-wise.
    6. Add UK units + US units day-wise.
    """
    uk_data = fetch_monthly_daily_single_country(
        conn=conn,
        user_id=user_id,
        country="uk",
        month=month,
        year=year,
    )

    us_data = fetch_monthly_daily_single_country(
        conn=conn,
        user_id=user_id,
        country="us",
        month=month,
        year=year,
    )

    if not uk_data and not us_data:
        return None

    last_day = calendar.monthrange(int(year), int(month))[1]
    full_days = list(range(1, last_day + 1))

    gbp_to_usd = get_gbp_to_usd_rate(month, year)

    uk_net_sales = uk_data["net_sales"] if uk_data else [0.0] * last_day
    uk_units = uk_data["units"] if uk_data else [0.0] * last_day

    us_net_sales = us_data["net_sales"] if us_data else [0.0] * last_day
    us_units = us_data["units"] if us_data else [0.0] * last_day

    net_sales_list = [
    float((uk_net_sales[i] * gbp_to_usd) + us_net_sales[i])
    for i in range(last_day)
]

    units_list = [
        float(uk_units[i] + us_units[i])
        for i in range(last_day)
    ]

    asp_list = [
        calc_asp(net_sales_list[i], units_list[i])
        for i in range(last_day)
    ]

    return {
        "xType": "day",
        "x": full_days,
        "net_sales": net_sales_list,
        "units": units_list,
        "asp": asp_list,
    }


def fetch_monthly_daily(conn, user_id, country, month, year):
    country = str(country).strip().lower()

    if country == "global":
        return fetch_monthly_daily_global(conn, user_id, month, year)

    return fetch_monthly_daily_single_country(conn, user_id, country, month, year)


# ---------- MONTH / QUARTER / YEAR FETCHERS ----------

def fetch_month_totals(conn, user_id, country, month, year):
    country = str(country).strip().lower()
    table = get_skuwise_monthly_table(user_id, country, month, year)

    if not table_exists(conn, table):
        return None

    # Keep the Performance Trend aligned with the SKU table for every country.
    # `quantity` is gross units sold, while `total_quantity` is net units after
    # returns.  Global used to special-case the gross column here, which made
    # both Units and the derived ASP disagree with the Global SKU table.
    units_col = "total_quantity"

    if country == "global":
        total_where = "LOWER(COALESCE(product_name, '')) = 'total'"
        non_total_where = "LOWER(COALESCE(product_name, '')) <> 'total'"
    else:
        total_where = """
            LOWER(COALESCE(sku, '')) = 'total'
            OR LOWER(COALESCE(product_name, '')) = 'total'
        """
        non_total_where = """
            LOWER(COALESCE(sku, '')) <> 'total'
            AND LOWER(COALESCE(product_name, '')) <> 'total'
        """

    q = text(f"""
        SELECT
            COALESCE(net_sales, 0) AS net_sales,
            COALESCE({units_col}, 0) AS units
        FROM public."{table}"
        WHERE {total_where}
        LIMIT 1
    """)

    row = conn.execute(q).mappings().first()

    if not row:
        q2 = text(f"""
            SELECT
                COALESCE(SUM(net_sales), 0) AS net_sales,
                COALESCE(SUM({units_col}), 0) AS units
            FROM public."{table}"
            WHERE {non_total_where}
        """)
        row2 = conn.execute(q2).mappings().first()

        net_sales = float(row2["net_sales"])
        units = float(row2["units"])
        asp = calc_asp(net_sales, units)

        return net_sales, units, asp

    net_sales = float(row["net_sales"])
    units = float(row["units"])
    asp = calc_asp(net_sales, units)

    return net_sales, units, asp


def fetch_monthwise_series(conn, user_id, country, months, year):
    net_sales_map = {}
    units_map = {}
    asp_map = {}

    for m in months:
        res = fetch_month_totals(conn, user_id, country, m, year)

        if not res:
            continue

        ns, un, asp = res
        month_key = MONTH_MAP[int(m)].capitalize()[:3]

        net_sales_map[month_key] = float(ns)
        units_map[month_key] = float(un)
        asp_map[month_key] = float(asp)

    return {
        "xType": "month",
        "net_sales": net_sales_map,
        "units": units_map,
        "asp": asp_map,
    }


# ---------- MAIN FUNCTION ----------

def get_performance_trend(user_id, country, period, timeline, year):
    targets = build_targets(period, timeline, year)

    result = {
        "xType": None,
        "x": [],
        "series": [],
    }

    period_norm = period.lower().strip()
    country = str(country).strip().lower()

    with phormula_engine.connect() as conn:
        max_days = None

        if period_norm == "monthly":
            days = []

            for t in targets:
                if t.get("type") == "monthly":
                    days.append(calendar.monthrange(t["year"], t["month"])[1])

            if days:
                max_days = max(days)
                result["xType"] = "day"
                result["x"] = list(range(1, max_days + 1))

        for t in targets:
            data = None

            if t["type"] == "monthly":
                data = fetch_monthly_daily(
                    conn=conn,
                    user_id=user_id,
                    country=country,
                    month=t["month"],
                    year=t["year"],
                )

                if not data:
                    continue

                if max_days is not None:
                    pad_len = max_days - len(data.get("x", []))

                    if pad_len > 0:
                        data["net_sales"] = list(data["net_sales"]) + [0.0] * pad_len
                        data["units"] = list(data["units"]) + [0.0] * pad_len
                        data["asp"] = list(data["asp"]) + [0.0] * pad_len

            elif t["type"] == "quarterly":
                months = QUARTER_MONTHS[t["quarter"]]

                data = fetch_monthwise_series(
                    conn=conn,
                    user_id=user_id,
                    country=country,
                    months=months,
                    year=t["year"],
                )

                if not data:
                    continue

            else:
                data = fetch_monthwise_series(
                    conn=conn,
                    user_id=user_id,
                    country=country,
                    months=range(1, 13),
                    year=t["year"],
                )

                if not data:
                    continue

            if result["xType"] is None:
                result["xType"] = data["xType"]

            result["series"].append({
                "label": t["label"],
                "net_sales": data["net_sales"],
                "units": data["units"],
                "asp": data["asp"],
            })

    if result["xType"] is None:
        result["xType"] = "day" if period_norm == "monthly" else "month"

    return result
