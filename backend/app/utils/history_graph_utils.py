import pandas as pd
from sqlalchemy import create_engine, text
import os  # adjust import if path differs
from app.utils.formulas_utils import uk_sales
import calendar




db_url = os.getenv("DATABASE_URL")
phormula_engine = create_engine(db_url)

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
            {"type": "monthly", "month": month, "year": year, "label": month_label(month, year)},
            {"type": "monthly", "month": prev_month, "year": prev_year, "label": month_label(prev_month, prev_year)},
            {"type": "monthly", "month": month, "year": year - 1, "label": month_label(month, year - 1)},
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
            {"type": "quarterly", "quarter": q, "year": year, "label": quarter_label(q, year)},
            {"type": "quarterly", "quarter": f"Q{prev_q_num}", "year": prev_year,
             "label": quarter_label(f"Q{prev_q_num}", prev_year)},
            {"type": "quarterly", "quarter": q, "year": year - 1,
             "label": quarter_label(q, year - 1)},
        ]

    if period == "yearly":
        return [
            {"type": "yearly", "year": year, "label": str(year)},
            {"type": "yearly", "year": year - 1, "label": str(year - 1)},
        ]

    raise ValueError(f"Unsupported period: {period}")


# ---------- DATA FETCHERS ----------

def fetch_monthly_daily(conn, user_id, country, month, year):
    table = f"user_{user_id}_{country}_{MONTH_MAP[month]}{year}_data"
    if not table_exists(conn, table):
        return None

    q = f"""
        SELECT *
        FROM {table}
    """

    df = pd.read_sql(q, conn)
    if df.empty:
        return None

    # Normalize date (required for daily grouping)
    df["__day__"] = pd.to_datetime(df["date_time"], errors="coerce").dt.date
    df = df.dropna(subset=["__day__"])

    # Units rule:
    # - ONLY Shipment rows
    # - Quantity coerced safely
    df["__ship_qty__"] = pd.to_numeric(df.get("quantity"), errors="coerce").fillna(0)
    df.loc[
        ~df.get("type", "")
          .astype(str)
          .str.contains("Shipment", case=False, na=False),
        "__ship_qty__"
    ] = 0

    rows = []
    for day, g in df.groupby("__day__", sort=True):
        # Centralized net sales logic
        net_sales_total, _, _ = uk_sales(g)

        units_total = float(g["__ship_qty__"].sum())

        rows.append({
            "day": day,
            "net_sales": float(net_sales_total),
            "units": units_total
        })

    # Even if no rows (no activity), still return full month with zeros
    last_day = calendar.monthrange(year, month)[1]
    full_days = list(range(1, last_day + 1))

    if not rows:
        return {
            "xType": "day",
            "x": full_days,
            "net_sales": [0.0] * len(full_days),
            "units": [0.0] * len(full_days),
        }

    out = pd.DataFrame(rows)
    out["day_num"] = pd.to_datetime(out["day"]).dt.day

    # Create maps and pad missing days with 0
    ns_map = dict(zip(out["day_num"].tolist(), out["net_sales"].tolist()))
    un_map = dict(zip(out["day_num"].tolist(), out["units"].tolist()))

    net_sales_full = [float(ns_map.get(d, 0.0)) for d in full_days]
    units_full = [float(un_map.get(d, 0.0)) for d in full_days]

    return {
        "xType": "day",
        "x": full_days,
        "net_sales": net_sales_full,
        "units": units_full,
    }

def fetch_month_totals(conn, user_id, country, month, year):
    table = f"skuwisemonthly_{user_id}_{country}_{MONTH_MAP[month]}{year}"
    if not table_exists(conn, table):
        return None

    # ✅ Use the TOTAL row only (pre-calculated overall totals)
    q = f"""
        SELECT
            COALESCE(net_sales, 0) AS net_sales,
            COALESCE(total_quantity, 0) AS units
        FROM {table}
        WHERE LOWER(COALESCE(sku, '')) = 'total'
           OR LOWER(COALESCE(product_name, '')) = 'total'
        LIMIT 1
    """

    row = conn.execute(text(q)).mappings().first()

    # If TOTAL row not found, fallback to sum of SKUs excluding TOTAL
    if not row:
        q2 = f"""
            SELECT
                COALESCE(SUM(net_sales), 0) AS net_sales,
                COALESCE(SUM(total_quantity), 0) AS units
            FROM {table}
            WHERE LOWER(COALESCE(sku, '')) <> 'total'
              AND LOWER(COALESCE(product_name, '')) <> 'total'
        """
        row2 = conn.execute(text(q2)).mappings().first()
        return float(row2["net_sales"]), float(row2["units"])

    return float(row["net_sales"]), float(row["units"])


def fetch_monthwise_series(conn, user_id, country, months, year):
    net_sales_map = {}
    units_map = {}

    for m in months:
        res = fetch_month_totals(conn, user_id, country, m, year)
        if not res:
            continue

        ns, un = res
        month_key = MONTH_MAP[m].capitalize()[:3]

        net_sales_map[month_key] = float(ns)
        units_map[month_key] = float(un)

    return {
        "xType": "month",
        "net_sales": net_sales_map,
        "units": units_map
    }



def get_performance_trend(user_id, country, period, timeline, year):
    targets = build_targets(period, timeline, year)

    result = {
        "xType": None,
        "x": [],          # only used for monthly daily chart
        "series": []
    }

    period_norm = period.lower().strip()

    with phormula_engine.connect() as conn:
        # ✅ For monthly: force a common x-axis = 1..max_days among the compared months
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
                data = fetch_monthly_daily(conn, user_id, country, t["month"], t["year"])

                if not data:
                    continue

                # ✅ Pad this month's series to max_days so nothing gets truncated
                if max_days is not None:
                    pad_len = max_days - len(data.get("x", []))
                    if pad_len > 0:
                        data["net_sales"] = list(data["net_sales"]) + [0.0] * pad_len
                        data["units"] = list(data["units"]) + [0.0] * pad_len

            elif t["type"] == "quarterly":
                months = QUARTER_MONTHS[t["quarter"]]
                data = fetch_monthwise_series(conn, user_id, country, months, t["year"])
                if not data:
                    continue

            else:  # yearly
                data = fetch_monthwise_series(conn, user_id, country, range(1, 13), t["year"])
                if not data:
                    continue

            # Set xType once (quarterly/yearly path)
            if result["xType"] is None:
                result["xType"] = data["xType"]

            # ✅ For monthly: we already set result["x"] above, don’t overwrite it
            # ✅ For other types: no x list needed

            result["series"].append({
                "label": t["label"],
                "net_sales": data["net_sales"],
                "units": data["units"],
            })

    if result["xType"] is None:
        result["xType"] = "day" if period_norm == "monthly" else "month"

    return result
