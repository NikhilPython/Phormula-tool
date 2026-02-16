
from __future__ import annotations

from flask import jsonify
from sqlalchemy import create_engine, MetaData, Table
from config import Config
from dateutil.relativedelta import relativedelta
from datetime import datetime
from calendar import month_name
import pandas as pd
import numpy as np
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text
import os, base64, warnings
import re

# ====== CONFIG / GLOBALS ======
warnings.filterwarnings("ignore")


db_url = os.getenv('DATABASE_URL', 'postgresql://postgres:password@localhost:5432/phormula')
db_url2 = os.getenv('DATABASE_AMAZON_URL')


MONTHS_MAP = {
    'january': 1, 'february': 2, 'march': 3, 'april': 4,
    'may': 5, 'june': 6, 'july': 7, 'august': 8,
    'september': 9, 'october': 10, 'november': 11, 'december': 12
}

# =======================
# Naming / date helpers
# =======================

def create_user_session(db_url2):
    user_engine1 = create_engine(db_url2)
    UserSession1 = sessionmaker(bind=user_engine1)
    return UserSession1()

def _norm_country(c: str) -> str:
    return (c or "").strip().lower()

def _norm_mv(mv: str) -> str:
    mv = (mv or "").strip().lower()
    if mv not in MONTHS_MAP:
        raise ValueError(f"Invalid month '{mv}'. Expected: {', '.join(MONTHS_MAP.keys())}")
    return mv

def _month_token_lower(year: int, mv_lower: str) -> str:
    """File token: 'november2025' (lowercase)"""
    return f"{mv_lower}{int(year)}"

def _month_title_token(year: int, mv_lower: str) -> str:
    """Legacy DB table token: 'November2025' (TitleCase month)"""
    mnum = MONTHS_MAP[mv_lower]
    return f"{month_name[mnum]}{int(year)}"

def month_label(dt: datetime) -> str:
    return dt.strftime("%b'%y")

def add_months(dt: datetime, k: int) -> datetime:
    return dt + relativedelta(months=k)

def _encode_file_to_base64(file_path: str) -> str | None:
    try:
        with open(file_path, "rb") as f:
            return base64.b64encode(f.read()).decode("utf-8")
    except Exception:
        return None

def _last_n_months_title_tokens(year: int, mv_lower: str, n: int) -> list[str]:
    """
    For requested month (mv/year), return TitleCase month tokens for mv-1 .. mv-n.
    Example: mv='november', year=2025, n=4 -> ['July2025','August2025','September2025','October2025']
    """
    ref = datetime(year, MONTHS_MAP[mv_lower], 1)
    months = []
    cur = ref - relativedelta(months=1)  # start at mv-1
    for _ in range(n):
        months.append(f"{month_name[cur.month]}{cur.year}")  # 'October2025'
        cur = cur - relativedelta(months=1)
    return list(reversed(months))

def check_sales_tables_availability(user_id: int, country: str, mv: str, year: int, n: int = 4) -> dict:
    """
    Helper for the ROUTE to enforce: "latest N months available".
    Returns {'missing': [...], 'needed': [...]} where items are TitleCase tokens like 'October2025'.
    Route can return a 404 with the list if 'missing' is non-empty.
    """
    country_n = _norm_country(country)
    mv_n = _norm_mv(mv)
    year_i = int(year)

    needed = _last_n_months_title_tokens(year_i, mv_n, n=n)
    engine = create_engine(db_url)
    meta = MetaData()
    meta.reflect(bind=engine)
    present = {t.lower() for t in meta.tables.keys()}

    missing = []
    for mon_title in needed:
        tname = f"user_{user_id}_{country_n}_{mon_title}_data"
        if tname.lower() not in present:
            missing.append(mon_title)

    return {"needed": needed, "missing": missing}

# =======================
# Label builders
# =======================

def _sold_labels_before(req_year: int, req_month_str: str) -> list[str]:
    """
    Return three labels for the 3 months immediately BEFORE the requested month.
    Example: mv='october', year=2025 -> returns ["Jul'25","Aug'25","Sep'25"]
    """
    m = MONTHS_MAP[req_month_str.lower()]
    anchor = datetime(req_year, m, 1)
    return [month_label(add_months(anchor, -3)),
            month_label(add_months(anchor, -2)),
            month_label(add_months(anchor, -1))]

def _horizon_month_labels(req_year: int, req_month_str: str, horizon: int) -> list[str]:
    """
    Return labels for the next `horizon` months STARTING from the requested month.
    Example: mv='november', year=2025, horizon=5
    -> ["Nov'25", "Dec'25", "Jan'26", "Feb'26", "Mar'26"]
    """
    m = MONTHS_MAP[req_month_str.lower()]
    start = datetime(req_year, m, 1)
    # ✅ Start from 0 to include the current month
    return [month_label(add_months(start, i)) for i in range(0, horizon)]


# =======================
# Stats helpers
# =======================

def _peak_of_last3(monthly_df: pd.DataFrame, sold_labels: list[str]) -> pd.Series:
    """
    monthly_df: columns ['sku', 'Month', 'quantity'] where Month is Timestamp month start.
    Computes the max units across the last 3 months for each SKU.
    Returns a Series indexed by sku -> peak units.
    """
    monthly_df = monthly_df.copy()
    monthly_df['Label'] = pd.to_datetime(monthly_df['Month']).dt.strftime("%b'%y")
    last3 = monthly_df[monthly_df['Label'].isin(sold_labels)]
    if last3.empty:
        return pd.Series(dtype=float)
    peak = last3.groupby('sku')['quantity'].max()
    return peak

def _last_month_units(monthly_df: pd.DataFrame, last_label: str) -> pd.Series:
    """
    Extract the units for JUST the last month (sold_m1 label) per SKU.
    Returns Series indexed by sku -> last_month_units.
    """
    monthly_df = monthly_df.copy()
    monthly_df['Label'] = pd.to_datetime(monthly_df['Month']).dt.strftime("%b'%y")
    lm = monthly_df[monthly_df['Label'] == last_label]
    if lm.empty:
        return pd.Series(dtype=float)
    return lm.groupby('sku')['quantity'].sum()

# =======================
# Load artifacts (inventory + sales)
# =======================
def _load_inventory_and_sales_artifacts(
    user_id: int,
    country: str,
    mv: str,
    year: int,
    engine,
    meta,
    allow_missing_inventory: bool = False,
    sales_month_override_title: str | None = None,
):
    """
    Loads inventory (xlsx) and (optionally) sales table for "last month" (mv-1) unless overridden.

    - File naming (lowercase): user_{id}_{country}_{november2025}_inventory_file.xlsx
    - Table naming (TitleCase month): user_{id}_{country}_October2025_data

    When allow_missing_inventory=True, if the file is absent we use an empty frame => zeros.
    """
    def _norm_sku_series(s: pd.Series) -> pd.Series:
        return s.astype(str).str.strip()

    country_n = _norm_country(country)
    mv_n = _norm_mv(mv)
    year_i = int(year)

    # ===== Inventory (xlsx) — lowercased filename =====
    inv_file = f"user_{user_id}_{country_n}_{_month_token_lower(year_i, mv_n)}_inventory_file.xlsx"
    inv_path = os.path.join(UPLOAD_FOLDER, inv_file)
    print(f"[manual]_load_artifacts: looking for inventory: {inv_path}")

    if not os.path.exists(inv_path):
        print(f"[manual]_load_artifacts: inventory file MISSING (allow_missing={allow_missing_inventory})")
        if allow_missing_inventory:
            inventory_data = pd.DataFrame(columns=['SKU', 'Ending Warehouse Balance'])
        else:
            raise FileNotFoundError(f"Inventory file not found at {inv_path}")
    else:
        inventory_data = pd.read_excel(inv_path, engine='openpyxl')
        if 'MSKU' in inventory_data.columns and 'SKU' not in inventory_data.columns:
            inventory_data = inventory_data.rename(columns={'MSKU': 'SKU'})

        if 'SKU' in inventory_data.columns:
            inventory_data['SKU'] = _norm_sku_series(inventory_data['SKU'])
        if 'Ending Warehouse Balance' in inventory_data.columns:
            inventory_data['Ending Warehouse Balance'] = pd.to_numeric(
                inventory_data['Ending Warehouse Balance'],
                errors='coerce'
            ).fillna(0.0)
        print(f"[manual]_load_artifacts: inventory rows={len(inventory_data)} unique_SKU={inventory_data.get('SKU', pd.Series()).nunique() if 'SKU' in inventory_data.columns else 0}")

    # ===== Sales table (mv-1 by default) — TitleCase month for DB =====
    if sales_month_override_title is None:
        sales_dt = datetime(year_i, MONTHS_MAP[mv_n], 1) - relativedelta(months=1)
        sales_month_override_title = f"{month_name[sales_dt.month]}{sales_dt.year}"

    meta.clear(); meta.reflect(bind=engine)
    tables = {t.lower(): t for t in meta.tables.keys()}
    table_name = f"user_{user_id}_{country_n}_{sales_month_override_title}_data"
    print(f"[manual]_load_artifacts: looking for sales table: {table_name}")

    if table_name.lower() in tables:
        sales_table = Table(tables[table_name.lower()], meta, autoload_with=engine)
        with engine.connect() as conn:
            sales_data = pd.read_sql(sales_table.select(), conn)

        sales_data_orders = sales_data[sales_data.get('type') == 'Order'].copy()
        print(f"[manual]_load_artifacts: sales rows={len(sales_data)} orders_only={len(sales_data_orders)}")

        if not sales_data_orders.empty:
            # normalize fields
            if 'sku' in sales_data_orders.columns:
                sales_data_orders['sku'] = _norm_sku_series(sales_data_orders['sku'])
            if 'product_name' in sales_data_orders.columns:
                sales_data_orders['product_name'] = sales_data_orders['product_name'].astype(str).fillna('')
            if 'quantity' in sales_data_orders.columns:
                sales_data_orders['quantity'] = pd.to_numeric(
                    sales_data_orders['quantity'], errors='coerce'
                ).fillna(0.0)
            if 'price_in_gbp' in sales_data_orders.columns:
                sales_data_orders['price_in_gbp'] = pd.to_numeric(
                    sales_data_orders['price_in_gbp'], errors='coerce'
                )

            sales_summary = (
                sales_data_orders.groupby('sku', dropna=False)['quantity']
                .sum().reset_index()
                .rename(columns={'quantity': 'Last Month Sales(Units)'})
            )
            product_names = (
                sales_data_orders.groupby('sku', dropna=False)['product_name']
                .first().reset_index()
                .rename(columns={'product_name': 'Product Name'})
            )
            price_map = (
                sales_data_orders.groupby('sku', dropna=False)['price_in_gbp']
                .first().to_dict()
            )
            print(f"[manual]_load_artifacts: sales_summary rows={len(sales_summary)} sum_units={sales_summary['Last Month Sales(Units)'].sum()}")
        else:
            sales_summary = pd.DataFrame(columns=['sku', 'Last Month Sales(Units)'])
            product_names = pd.DataFrame(columns=['sku', 'Product Name'])
            price_map = {}
            print("[manual]_load_artifacts: no 'Order' rows in sales table")
    else:
        sales_summary = pd.DataFrame(columns=['sku', 'Last Month Sales(Units)'])
        product_names = pd.DataFrame(columns=['sku', 'Product Name'])
        price_map = {}
        print("[manual]_load_artifacts: sales table NOT FOUND")

    inv_totals = (
        inventory_data.groupby('SKU', dropna=False)['Ending Warehouse Balance']
        .sum().reset_index()
        .rename(columns={'Ending Warehouse Balance': 'Inventory at Month End'})
    )
    print(f"[manual]_load_artifacts: inv_totals rows={len(inv_totals)} sum_inv={inv_totals['Inventory at Month End'].sum() if 'Inventory at Month End' in inv_totals else 0}")

    return inv_totals, sales_summary, product_names, price_map


def _norm_sku(x: str) -> str:
    if x is None:
        return ""
    # remove all whitespace and upper-case so joins are robust
    return re.sub(r"\s+", "", str(x)).upper()

def fetch_and_merge_inventory_hotfix(forecast_totals: pd.DataFrame, engine1) -> pd.DataFrame:
    """
    Returns forecast_totals with a new column 'Inventory at Month End'
    joined from Amazon DB inventory (latest snapshot per SKU).
    """
    
    sql_latest_per_sku = """
        SELECT DISTINCT ON (seller_sku)
               seller_sku AS "SKU",
               fulfillable_quantity AS "Ending Warehouse Balance",
               synced_at
        FROM public.inventory
        ORDER BY seller_sku, synced_at DESC
    """

    inv_df = pd.read_sql(text(sql_latest_per_sku), con=engine1)
    print(f"[INV] Rows fetched: {len(inv_df)}")
    if not inv_df.empty:
        print("[INV] Sample:")
        print(inv_df.head(10).to_string())

    # Prepare inventory frame
    inventory_totals = inv_df[['SKU', 'Ending Warehouse Balance']].copy()
    inventory_totals.rename(columns={'SKU': 'sku'}, inplace=True)
    inventory_totals['Ending Warehouse Balance'] = pd.to_numeric(
        inventory_totals['Ending Warehouse Balance'], errors='coerce'
    ).fillna(0).astype(int)
    inventory_totals['sku_norm'] = inventory_totals['sku'].map(_norm_sku)

    # Prepare forecast side
    out = forecast_totals.copy()
    out['sku_norm'] = out['sku'].map(_norm_sku)

    # Quick diagnostics
    print(f"[MERGE] Forecast SKUs (unique norm): {out['sku_norm'].nunique()}")
    print(f"[MERGE] Inventory SKUs (unique norm): {inventory_totals['sku_norm'].nunique()}")
    common = set(out['sku_norm']) & set(inventory_totals['sku_norm'])
    print(f"[MERGE] Common normalized SKUs: {len(common)}")
    if common:
        print(f"[MERGE] Example keys: {list(sorted(common))[:10]}")

    # Merge on normalized key
    out = out.merge(
        inventory_totals[['sku_norm', 'Ending Warehouse Balance']],
        on='sku_norm', how='left'
    )
    out.drop(columns=['sku_norm'], inplace=True)
    out.rename(columns={'Ending Warehouse Balance': 'Inventory at Month End'}, inplace=True)
    out['Inventory at Month End'] = (
        pd.to_numeric(out['Inventory at Month End'], errors='coerce').fillna(0).astype(int)
    )

    print(f"[MERGE] ✓ After merge: {len(out)} rows")
    print(f"[MERGE] ✓ Non-zero 'Inventory at Month End': {(out['Inventory at Month End'] > 0).sum()}")
    print(out[['sku', 'Inventory at Month End']].head(10).to_string())

    return out

def generate_manual_forecast(
    user_id: int,
    new_df: pd.DataFrame,
    country: str,
    mv: str,
    year: int,
    custom_growth_map: dict[str, float],
    transit_time: int,
    stock_unit: int,
    preview: bool = False,
):
    """
    Manual forecasting path (no ML).
    PREVIEW -> only JSON (no file). FINAL -> writes files too.
    """
    from sqlalchemy import create_engine, MetaData, Table, text  # ← add text
    # ... keep your other imports/utilities as they are ...

    engine = create_engine(db_url)
    engine1 = create_engine(db_url2)  # ← NEW: Amazon DB for inventory (match generate_forecast)
    meta = MetaData()
    meta.reflect(bind=engine)
    # meta for engine1 not strictly required here

    def _norm_sku_series(s: pd.Series) -> pd.Series:
        return s.astype(str).str.strip()

    # normalized key helper to match generate_forecast join
    def _norm_sku(x: str) -> str:
        import re
        if x is None:
            return ""
        return re.sub(r"\s+", "", str(x)).upper()

    # Normalize inputs
    country = _norm_country(country)
    mv = _norm_mv(mv)
    req_year = int(year)
    req_month_num = MONTHS_MAP[mv]
    horizon = max(int(transit_time) + int(stock_unit), 0)

    print(f"[manual] generate_manual_forecast: user={user_id} country={country} mv={mv} year={req_year} horizon={horizon} preview={preview}")

    # ---- 1) Build monthly actuals ----
    df = new_df.copy()
    # (unchanged) ...
    if "date_time" in df.columns:
        dt = pd.to_datetime(df["date_time"], errors="coerce", utc=True)
    elif isinstance(df.index, pd.DatetimeIndex):
        dt = pd.to_datetime(df.index, errors="coerce", utc=True)
    else:
        return jsonify({"error": "bad_payload", "detail": "Missing 'date_time' column or datetime index"}), 400

    try:
        dt = dt.tz_convert(None)
    except Exception:
        pass

    mask_good = dt.notna()
    bad_count = int((~mask_good).sum())
    df = df.loc[mask_good].copy()
    df.index = pd.DatetimeIndex(dt[mask_good], name="date_time")

    print(f"[manual] after datetime clean: rows={len(df)} dropped_bad={bad_count} date_min={df.index.min()} date_max={df.index.max()}")

    if df.empty:
        return jsonify({"error": "no_valid_datetimes", "detail": f"All {bad_count} rows had invalid date_time"}), 400

    # SKU normalization
    if 'sku' not in df.columns:
        return jsonify({"error": "bad_payload", "detail": "Missing 'sku' column"}), 400

    df['sku'] = _norm_sku_series(df['sku'])
    df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce').fillna(0.0)
    df = df[['sku', 'quantity']].sort_index()

    print(f"[manual] unique SKUs in new_df={df['sku'].nunique()} total_qty={df['quantity'].sum()}")

    monthly_actuals = (
        df.groupby("sku")["quantity"]
          .resample("M")
          .sum()
          .rename_axis(index=["sku", "Month"])
          .reset_index()
    )
    monthly_actuals['Label'] = pd.to_datetime(monthly_actuals['Month']).dt.strftime("%b'%y")
    print(f"[manual] monthly_actuals rows={len(monthly_actuals)} labels_present={sorted(monthly_actuals['Label'].unique().tolist())}")

    # ---- 2) Last 3 months & last month ----
    sold_m3, sold_m2, sold_m1 = _sold_labels_before(req_year, mv)
    print(f"[manual] sold labels -> last3={[sold_m3, sold_m2, sold_m1]}")

    last3_mask = monthly_actuals["Label"].isin([sold_m3, sold_m2, sold_m1])
    last3_raw = (
        monthly_actuals[last3_mask]
        .pivot_table(index="sku", columns="Label", values="quantity", aggfunc="sum")
        .fillna(0)
    )

    totals_by_month = (
        last3_raw.sum(axis=0)
        .reindex([sold_m3, sold_m2, sold_m1])
        .fillna(0)
        if not last3_raw.empty else pd.Series(dtype=float)
    )

    if not totals_by_month.empty:
        overall_peak_month_label = totals_by_month.idxmax()
        overall_peak_units = int(totals_by_month.max())
    else:
        overall_peak_month_label = None
        overall_peak_units = 0

    print(f"[manual] overall last3 totals -> {totals_by_month.to_dict() if not totals_by_month.empty else {}}")
    print(f"[manual] overall peak month -> {overall_peak_month_label} ({overall_peak_units} units)")

    peak_series = last3_raw.max(axis=1).rename("peak_last3") if not last3_raw.empty else pd.Series(dtype=float, name="peak_last3")
    last_month_series = _last_month_units(monthly_actuals, sold_m1).rename("last_month_units")
    print(f"[manual] nonzero last_month_units SKUs={(last_month_series>0).sum()} nonzero peak_last3 SKUs={(peak_series>0).sum()}")

    # ---- 3) Horizon months ----
    last_label_dt = datetime.strptime(sold_m1, "%b'%y")
    horizon_labels = [month_label(add_months(last_label_dt, i)) for i in range(1, horizon + 1)]
    forecast_cols = horizon_labels[:]
    print(f"[manual] horizon_labels={forecast_cols}")

    # ---- 4) Forecast per SKU ----
    all_skus = monthly_actuals["sku"].dropna().unique().tolist()
    print(f"[manual] all_skus_count={len(all_skus)}")
    manual_rows = []

    for sku in all_skus:
        peak_units = float(peak_series.get(sku, 0.0))
        g_pct = float(custom_growth_map.get(sku, 0.0))
        g = 1.0 + (g_pct / 100.0)

        row = {"sku": sku}
        for k, mlabel in enumerate(forecast_cols, start=1):
            val = peak_units * (g ** k)
            row[mlabel] = float(np.round(val, 0))
        manual_rows.append(row)

        print(f"[manual] SKU={sku} peak={peak_units} g%={g_pct} -> first_forecast={np.round(peak_units * g, 0)}")

    if not manual_rows:
        manual_rows = [{"sku": "Total", **{ml: 0.0 for ml in forecast_cols}}]

    forecast_pivot = pd.DataFrame(manual_rows).fillna(0.0)
    print(f"[manual] forecast_pivot rows={len(forecast_pivot)}")

    # ---- 5) Enrich with inventory & sales (DB-backed) ----
    # (A) Inventory: replicate generate_forecast behavior using engine1/db_url2
    month_start = datetime(req_year, req_month_num, 1)
    month_end = (month_start + relativedelta(months=1)).replace(day=1)

    print("\n" + "="*60)
    print("INVENTORY DATA FETCH (manual->fixed to match generate_forecast)")
    print("="*60)
    print(f"[INV] Window: {month_start:%Y-%m-%d} -> {month_end:%Y-%m-%d}")

    inv_sql_window = """
        SELECT DISTINCT ON (seller_sku)
               seller_sku AS "SKU",
               fulfillable_quantity AS "Ending Warehouse Balance",
               synced_at
        FROM public.inventory
        WHERE synced_at >= :start AND synced_at < :end
        ORDER BY seller_sku, synced_at DESC
    """
    inv_sql_fallback = """
        SELECT DISTINCT ON (seller_sku)
               seller_sku AS "SKU",
               fulfillable_quantity AS "Ending Warehouse Balance",
               synced_at
        FROM public.inventory
        ORDER BY seller_sku, synced_at DESC
    """
    params = {"start": month_start, "end": month_end}

    try:
        inv_df = pd.read_sql(text(inv_sql_window), con=engine1, params=params)
        if inv_df.empty:
            print("[INV] No rows in window — using latest overall snapshot.")
            inv_df = pd.read_sql(text(inv_sql_fallback), con=engine1)
        print(f"[INV] Rows fetched: {len(inv_df)}")
        if not inv_df.empty:
            print(inv_df.head(5).to_string())

        inventory_totals = inv_df[["SKU", "Ending Warehouse Balance"]].copy()
        inventory_totals.rename(columns={"SKU": "sku"}, inplace=True)
        inventory_totals["Ending Warehouse Balance"] = pd.to_numeric(
            inventory_totals["Ending Warehouse Balance"], errors="coerce"
        ).fillna(0).astype(int)

        # normalized key for robust join
        inventory_totals["sku_norm"] = inventory_totals["sku"].map(_norm_sku)
    except Exception as e:
        print(f"[INV] ✗ ERROR querying inventory: {e}")
        import traceback; traceback.print_exc()
        inventory_totals = pd.DataFrame({"sku": [], "Ending Warehouse Balance": [], "sku_norm": []})

    # (B) Sales + product names (unchanged from your function)
    table_name = f"user_{user_id}_{country}_{mv}{req_year}_data"
    if table_name in meta.tables:
        sales_table = Table(table_name, meta, autoload_with=engine)
        with engine.connect() as conn:
            sales_data = pd.read_sql(sales_table.select(), conn)
        sales_summary = (
            sales_data.groupby("sku")["quantity"]
            .sum().reset_index().rename(columns={"quantity": "Last Month Sales(Units)"})
        )
        product_names = (
            sales_data.groupby("sku")["product_name"]
            .first().reset_index().rename(columns={"product_name": "Product Name"})
        )
        price_map_from_sales = {}
        if "price_in_gbp" in sales_data.columns:
            tmp = sales_data.copy()
            tmp["sku"] = tmp["sku"].astype(str).str.strip()
            price_map_from_sales = tmp.groupby("sku")["price_in_gbp"].first().to_dict()
    else:
        sales_summary = pd.DataFrame(columns=["sku", "Last Month Sales(Units)"])
        product_names = pd.DataFrame(columns=["sku", "Product Name"])
        price_map_from_sales = {}

    # normalize merge inputs
    if not sales_summary.empty:
        sales_summary["sku"] = sales_summary["sku"].astype(str).str.strip()
        sales_summary["Last Month Sales(Units)"] = pd.to_numeric(
            sales_summary["Last Month Sales(Units)"], errors="coerce"
        ).fillna(0.0)
    if not product_names.empty:
        product_names["sku"] = product_names["sku"].astype(str).str.strip()
        product_names["Product Name"] = product_names["Product Name"].astype(str).fillna("")

    # Build forecast_totals (for normalized join) from your pivot
    forecast_totals = forecast_pivot.copy()
    forecast_totals["sku"] = forecast_totals["sku"].astype(str)
    forecast_totals["sku_norm"] = forecast_totals["sku"].map(_norm_sku)

    # Join on normalized key to get "Inventory at Month End"
    inventory_forecast = forecast_totals.merge(
        inventory_totals[["sku_norm", "Ending Warehouse Balance"]],
        on="sku_norm", how="left"
    ).drop(columns=["sku_norm"])

    inventory_forecast.rename(
        columns={"Ending Warehouse Balance": "Inventory at Month End"},
        inplace=True
    )
    inventory_forecast["Inventory at Month End"] = pd.to_numeric(
        inventory_forecast["Inventory at Month End"], errors="coerce"
    ).fillna(0).astype(int)

    # If you also use the helper elsewhere, try to apply it (optional, safe no-op if missing)
    try:
        inventory_forecast = fetch_and_merge_inventory_hotfix(forecast_totals, engine1)
    except NameError:
        pass

    # Merge sales & product names
    inventory_forecast = inventory_forecast.merge(sales_summary, on="sku", how="left").fillna(0)
    inventory_forecast = inventory_forecast.merge(product_names, on="sku", how="left").fillna("")

    print(f"[manual] after merges rows={len(inventory_forecast)} "
          f"missing_inv={(inventory_forecast['Inventory at Month End']==0).sum() if 'Inventory at Month End' in inventory_forecast else 'n/a'}")

    # ---- 6) Add last 3 months sold ----
    inventory_forecast["SKU Type"] = "-"
    last3_sold_pivot = (
        monthly_actuals[monthly_actuals["Label"].isin([sold_m3, sold_m2, sold_m1])]
        .pivot_table(index="sku", columns="Label", values="quantity", aggfunc="sum")
        .reset_index()
        .fillna(0)
    )
    for lbl in [sold_m3, sold_m2, sold_m1]:
        if lbl not in last3_sold_pivot.columns:
            last3_sold_pivot[lbl] = 0.0
    last3_sold_pivot = last3_sold_pivot.rename(columns={
        sold_m3: f"{sold_m3} Sold",
        sold_m2: f"{sold_m2} Sold",
        sold_m1: f"{sold_m1} Sold"
    })
    inventory_forecast = inventory_forecast.merge(
        last3_sold_pivot[["sku", f"{sold_m3} Sold", f"{sold_m2} Sold", f"{sold_m1} Sold"]],
        on="sku", how="left"
    ).fillna(0)

    # ---- 7) Totals & math ----
    inventory_forecast["Projected Sales Total"] = np.round(inventory_forecast[forecast_cols].sum(axis=1), 2)
    if "Inventory at Month End" in inventory_forecast:
        inventory_forecast["Inventory at Month End"] = pd.to_numeric(
            inventory_forecast["Inventory at Month End"], errors="coerce"
        ).fillna(0.0)
    else:
        inventory_forecast["Inventory at Month End"] = 0.0

    inventory_forecast["Dispatch"] = (inventory_forecast["Projected Sales Total"] - inventory_forecast["Inventory at Month End"]).clip(lower=0)
    inventory_forecast["Current Inventory + Dispatch"] = inventory_forecast["Dispatch"] + inventory_forecast["Inventory at Month End"]

    # ==== NEW: Coverage Ratio (before dispatch) — same as generate_forecast ====
    divisor = pd.to_numeric(inventory_forecast.get("Last Month Sales(Units)", 0), errors="coerce").replace(0, np.nan)
    coverage = (pd.to_numeric(inventory_forecast["Inventory at Month End"], errors="coerce") / divisor).round(2)
    inventory_forecast["Inventory Coverage Ratio Before Dispatch"] = coverage.where(coverage.notna(), "-")

    # ---- 8) Total row ----
    numeric_columns = ["Projected Sales Total", "Inventory at Month End", "Last Month Sales(Units)",
                       f"{sold_m3} Sold", f"{sold_m2} Sold", f"{sold_m1} Sold"] + forecast_cols
    numeric_columns = [c for c in numeric_columns if c in inventory_forecast.columns]
    total_row = pd.DataFrame([inventory_forecast[numeric_columns].sum().round(2)], index=["Total"])
    total_row.insert(0, "sku", "Total")
    total_row["Product Name"] = "Total"
    total_row["SKU Type"] = "-"
    total_row["Inventory Coverage Ratio Before Dispatch"] = "-"  # match behavior
    inventory_forecast = pd.concat([inventory_forecast, total_row], ignore_index=True)

    # ---- 9) Final columns ----
    final_columns = ["sku", "Product Name", "SKU Type", "Last Month Sales(Units)",
                     f"{sold_m3} Sold", f"{sold_m2} Sold", f"{sold_m1} Sold",
                     "Projected Sales Total",
                     "Inventory at Month End",
                     "Inventory Coverage Ratio Before Dispatch",
                     "Dispatch", "Current Inventory + Dispatch"] + forecast_cols
    final_columns = [c for c in final_columns if c in inventory_forecast.columns]
    inventory_forecast = inventory_forecast[final_columns].copy()

    # ---- PREVIEW ----
    if preview:
        last3_lookup = last3_sold_pivot.set_index('sku') if 'sku' in last3_sold_pivot.columns else pd.DataFrame()
        rows = []
        for _, r in inventory_forecast[inventory_forecast['sku'] != 'Total'].iterrows():
            sku = str(r['sku'])
            prod_name = str(r.get('Product Name', ''))
            last_units = float(last_month_series.get(sku, 0.0))
            peak_3 = float(peak_series.get(sku, 0.0))
            prev_units = 0.0
            if isinstance(last3_lookup, pd.DataFrame) and not last3_lookup.empty and sku in last3_lookup.index:
                prev_units = float(last3_lookup.loc[sku].get(f"{sold_m2} Sold", 0.0))
            last_month_growth_pct = round((last_units - prev_units) / prev_units * 100.0, 2) if prev_units > 0 else 0.0
            horizon_vals = {ml: float(r.get(ml, 0.0)) for ml in forecast_cols}
            row = {"sku": sku, "product_name": prod_name, "last_month_units": last_units,
                   "peak_last3": peak_3, "last_month_growth_pct": last_month_growth_pct,
                   "growth_pct": float(custom_growth_map.get(sku, 0.0)),
                   "Last Month Sales(Units)": last_units, "Peak Sale (last 3 mo)": peak_3,
                   "Last Month Growth (%)": last_month_growth_pct}
            row.update(horizon_vals)
            rows.append(row)
        print("[manual] PREVIEW sample rows:", rows[:3])
        return jsonify({"preview": True, "horizon_months": forecast_cols,
                        "overall_peak_month": overall_peak_month_label,
                        "overall_peak_units": overall_peak_units, "rows": rows}), 200

    # ---- FINALIZE: write artifacts ----
    price_map_from_df = {}
    if "price_in_gbp" in new_df.columns:
        tmp = new_df.reset_index() if isinstance(new_df.index, pd.DatetimeIndex) else new_df
        if 'sku' in tmp.columns:
            tmp['sku'] = tmp['sku'].astype(str).str.strip()
        price_map_from_df = tmp.groupby("sku")["price_in_gbp"].first().to_dict()

    def _price_for(sku: str):
        return price_map_from_sales.get(sku) if sku in price_map_from_sales else price_map_from_df.get(sku)

    anchor = datetime(req_year, req_month_num, 1)
    artifact_rows = []
    for _, row in inventory_forecast[inventory_forecast["sku"] != "Total"].iterrows():
        sku = row["sku"]
        price = _price_for(sku)
        for i, lbl in enumerate(forecast_cols, start=1):
            mo = add_months(anchor, i)
            artifact_rows.append({
                "sku": sku, "month": mo,
                "forecast": float(row[lbl]) if pd.notna(row[lbl]) else 0.0,
                "price_in_gbp": price
            })

    _af = pd.DataFrame(artifact_rows, columns=["sku", "month", "forecast", "price_in_gbp"])
    forecast_path = os.path.join(UPLOAD_FOLDER, f"forecasts_for_{user_id}_{country}.xlsx")
    _af.to_excel(forecast_path, index=False)

    requested_token = datetime(req_year, req_month_num, 1).strftime("%b").lower()
    out_path = os.path.join(UPLOAD_FOLDER, f"inventory_forecast_{user_id}_{country}_{requested_token}+2.xlsx")
    inventory_forecast.to_excel(out_path, index=False)
    _ = _encode_file_to_base64(out_path)

    print(f"[manual] FINAL: wrote {forecast_path} and {out_path}")
    return jsonify({"message": "Inventory processed successfully (manual)", "file_path": out_path}), 200
