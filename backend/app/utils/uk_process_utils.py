from flask import  jsonify
from sqlalchemy import create_engine,  inspect
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text
from config import Config
SECRET_KEY = Config.SECRET_KEY
#UPLOAD_FOLDER = Config.UPLOAD_FOLDER
from config import basedir
import os
import pandas as pd
import numpy as np
from app.models.user_models import User, CountryProfile
from sqlalchemy import MetaData, Table
from datetime import datetime, timedelta
from flask_mail import Message
from flask import current_app
from app import mail
from calendar import month_name
from dotenv import load_dotenv
from datetime import datetime
from app.utils.formulas_utils import uk_sales, uk_tax, uk_credits, uk_amazon_fee,uk_platform_fee, uk_advertising
import warnings
import re
warnings.filterwarnings("ignore") 


load_dotenv()
db_url = os.getenv('DATABASE_URL', 'postgresql://postgres:password@localhost:5432/phormula')
db_url1= os.getenv('DATABASE_ADMIN_URL', 'postgresql://postgres:password@localhost:5432/admin_db')


MONTHS_REVERSE_MAP = {
    1: "january", 2: "february", 3: "march", 4: "april", 5: "may", 6: "june",
    7: "july", 8: "august", 9: "september", 10: "october", 11: "november", 12: "december"
}


MONTHS_MAP = {
    'january': 1, 'february': 2, 'march': 3, 'april': 4,
    'may': 5, 'june': 6, 'july': 7, 'august': 8,
    'september': 9, 'october': 10, 'november': 11, 'december': 12
}

text_cols = [
    "sku","product_name","errorstatus","category","positive","improvements",
    "month","year","country",
    "profit_growth","unit_wise_profitability_growth","asp_growth",
    "sales_growth","unit_growth","amazon_fee_growth","profit_mix_growth",
    "sales_mix_growth","sales_mix_analysis","profit_mix_analysis"
]
int_cols = ["quantity","previous_quantity","user_id","return_quantity","total_quantity"]


from sqlalchemy import text
import pandas as pd

_TABLE_COL_CACHE = {}

def get_table_columns(conn, table_name: str, schema: str = "public"):
    """
    Returns list of column names present in DB table (order by ordinal_position).
    Cached for performance.
    """
    cache_key = f"{schema}.{table_name}"
    if cache_key in _TABLE_COL_CACHE:
        return _TABLE_COL_CACHE[cache_key]

    q = text("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = :schema
          AND table_name   = :table
        ORDER BY ordinal_position
    """)
    rows = conn.execute(q, {"schema": schema, "table": table_name}).fetchall()
    cols = [r[0] for r in rows]
    _TABLE_COL_CACHE[cache_key] = cols
    return cols

def df_only_db_columns(df: pd.DataFrame, db_cols: list):
    """
    Keep only those DF columns which exist in DB.
    Also remove duplicate DF columns (if any).
    """
    if not df.columns.is_unique:
        df = df.loc[:, ~df.columns.duplicated()].copy()

    keep = [c for c in db_cols if c in df.columns]
    return df[keep].copy()

def safe_to_sql(df: pd.DataFrame, table_name: str, conn, if_exists="append",
                index=False, method="multi", chunksize=100):
    """
    Inserts only DB-defined columns.
    """
    db_cols = get_table_columns(conn, table_name)
    df2 = df_only_db_columns(df, db_cols)
    df2.to_sql(table_name, conn, if_exists=if_exists, index=index, method=method, chunksize=chunksize)


def sanitize_for_db(df_):
    for c in int_cols:
        if c in df_.columns:
            df_[c] = pd.to_numeric(df_[c], errors="coerce").fillna(0).astype(int)

    for c in text_cols:
        if c in df_.columns:
            df_[c] = df_[c].astype(str).fillna("")

    for c in df_.columns:
        if c in text_cols or c in int_cols:
            continue
        df_[c] = pd.to_numeric(df_[c], errors="coerce").fillna(0.0)

    return df_



def get_previous_month_year(month, year):
    year = int(year)
    prev_month_num = MONTHS_MAP[month] - 1
    if prev_month_num == 0:
        prev_month_num = 12
        year -= 1
    prev_month = MONTHS_REVERSE_MAP[prev_month_num]
    return prev_month, year

def process_skuwise_data(user_id, country, month, year):
    engine = create_engine(db_url)
    engine1 = create_engine(db_url1)
    conn = engine.connect()

    source_table = f"user_{user_id}_{country}_{month}{year}_data"
    target_table = f"nse_{user_id}_{country}_{month}{year}"
    target_table2 = f"skuwisemonthly_{user_id}_{country}"
    target_table3 = f"skuwisemonthly_{user_id}"
    target_table_us   = f"skuwisemonthly_{user_id}"        # EXISTING US wala (same)
    target_table_ind  = f"skuwisemonthlyind_{user_id}"     # NEW – India
    target_table_can  = f"skuwisemonthlycan_{user_id}"     # NEW – Canada
    target_table_gbp  = f"skuwisemonthlygbp_{user_id}" 
        # NEW: per-country USD tables
    target_table_usd_month = f"skuwisemonthly_{user_id}_{country}_usd_{month}{year}"
    target_table_usd_roll  = f"skuwisemonthly_{user_id}_{country}_usd"
    target_table_nse = f"skuwisemonthly_{user_id}_{country}_{month}{year}"
    # NEW – GBP base

    prev_month, prev_year = get_previous_month_year(month, year)
    prev_table = f"nse_{user_id}_{country}_{prev_month}{prev_year}"

    try:
        # Fetch main table data
        query = f"SELECT * FROM {source_table}"
        df = pd.read_sql(query, conn)
        if df.empty:
            print(f"No data found in {source_table}")
            return

        # Check if previous month table exists - PostgreSQL version
        table_exists_query = f"""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables 
                WHERE table_schema = 'public'
                AND table_name = '{prev_table}'
            );
        """
        table_check_result = conn.execute(text(table_exists_query)).fetchone()
        table_exists = table_check_result[0] if table_check_result else False
        

        a = b = c = d = e = f = g = h = i = j = k = l = m = n = o = p = q = r = 0
        if table_exists:
            query = f"""
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
            df_prev = pd.read_sql(query, conn)

            # Load total row
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
                # Assign values
                a = total_values.get('net_sales', 0)
                b = total_values.get('net_credits', 0)
                c = total_values.get('platform_fee', 0)
                d = total_values.get('rembursement_fee', 0)
                e = total_values.get('advertising_total', 0)
                f = total_values.get('reimbursement_vs_sales', 0)
                g = total_values.get('cm2_profit', 0)
                h = total_values.get('cm2_margins', 0)
                i = total_values.get('acos', 0)
                j = total_values.get('rembursment_vs_cm2_margins', 0)
                k = total_values.get('profit', 0)
                l = total_values.get('profit_percentage', 0)
                m = total_values.get('quantity', 0)
                n = total_values.get('cost_of_unit_sold', 0)
                o = total_values.get('amazon_fee', 0)
                p = total_values.get('net_taxes', 0)
                q = total_values.get('fba_fees', 0)
                r = total_values.get('selling_fees', 0)
        else:
            # Create an empty DataFrame with default zero values
            df_prev = pd.DataFrame(columns=[
                "sku", "previous_net_sales", "previous_net_credits", "previous_profit",
                "previous_profit_percentage", "previous_quantity", "previous_cost_of_unit_sold",
                "previous_amazon_fee", "previous_net_taxes", "previous_fba_fees", "previous_selling_fees", 
                "previous_platform_fee", "previous_rembursement_fee", "previous_advertising_total", 
                "previous_reimbursement_vs_sales", "previous_cm2_profit", "previous_cm2_margins", 
                "previous_acos", "previous_rembursment_vs_cm2_margins"
            ]).fillna(0)

        numeric_column_previous = [
            "previous_net_sales", "previous_net_credits", "previous_profit",
            "previous_profit_percentage", "previous_quantity", "previous_cost_of_unit_sold",
            "previous_amazon_fee", "previous_net_taxes", "previous_fba_fees", "previous_selling_fees", 
            "previous_platform_fee", "previous_rembursement_fee", "previous_advertising_total", 
            "previous_reimbursement_vs_sales", "previous_cm2_profit", "previous_cm2_margins", 
            "previous_acos", "previous_rembursment_vs_cm2_margins"
        ]
        numeric_column_previous = [col for col in numeric_column_previous if col in df_prev.columns]
        if numeric_column_previous:
            df_prev[numeric_column_previous] = (
                df_prev[numeric_column_previous].apply(pd.to_numeric, errors='coerce').fillna(0)
            )

        if not table_exists:
            df_prev = df_prev.apply(pd.to_numeric, errors='ignore')
            df_prev.fillna(0, inplace=True)

        # ---------- FIX: harden string ops & expected columns BEFORE helpers ----------
        likely_text_cols = [
            "sku","type","description","marketplace","fulfilment",
            "order_city","order_state","order_postal","tax_collection_model","product_name", "errorstatus"
        ]
        for col in likely_text_cols:
            if col in df.columns:
                df[col] = df[col].astype(str)

        # Clean commas -> numeric (guard column presence)
        if "total" in df.columns:
            df["total"] = pd.to_numeric(df["total"].astype(str).str.replace(",", ""), errors="coerce")
        if "other" in df.columns:
            df["other"] = pd.to_numeric(df["other"].astype(str).str.replace(",", ""), errors="coerce")

        # Make sure optional explicit-cost columns exist as Series (prevents scalar path in helpers)
        if "platform_fees" not in df.columns:
            df["platform_fees"] = 0.0
        if "advertising_cost" not in df.columns:
            df["advertising_cost"] = 0.0

        # Robust "Transfer" filter
        type_str = df.get("type", pd.Series("", index=df.index)).astype(str).str.strip()
        transfer_df = df[type_str.isin(["Transfer", "DebtRecovery"])]
        rembursement_fee_desc_sum = abs(transfer_df["total"].sum()) if "total" in transfer_df else 0
        rembursement_fee_col_sum = df["net_reimbursement"].sum() if "net_reimbursement" in df.columns else 0
        rembursement_fee = rembursement_fee_desc_sum + rembursement_fee_col_sum

        # Convert numeric columns (only those present)
        numeric_columns = [
            "product_sales", "promotional_rebates", "product_sales_tax", "promotional_rebates_tax",
            "marketplace_facilitator_tax", "shipping_credits_tax", "giftwrap_credits_tax",
            "selling_fees", "fba_fees", "other", "postage_credits", "gift_wrap_credits",
            "price_in_gbp", "cost_of_unit_sold", "quantity", "total", "other_transaction_fees", "answer", "difference",
            "shipping_credits", "shipment_charges"
        ]
        numeric_columns = [col for col in numeric_columns if col in df.columns]
        if numeric_columns:
            df[numeric_columns] = df[numeric_columns].apply(pd.to_numeric, errors='coerce').fillna(0)

        # ------------------- LOST / LEFTOUT LOGIC (NEW) -------------------

        # ------------------- LOST / LEFTOUT LOGIC (NEW) -------------------

        desc_str = df.get("description", pd.Series("", index=df.index)).astype(str).str.strip()
        type_str2 = df.get("type", pd.Series("", index=df.index)).astype(str).str.strip()

        # ================== NEW: TOTAL-ONLY BREAKUP COLUMNS ==================
        desc_all = df.get("description", pd.Series("", index=df.index)).astype(str)

        def sum_total_where_desc_contains(keywords):
            """
            keywords: list[str]
            Returns sum of df['total'] where description contains any keyword (case-insensitive)
            """
            if "total" not in df.columns:
                return 0.0
            pattern = "|".join([re.escape(k) for k in keywords])
            mask = desc_all.str.contains(pattern, case=False, na=False, regex=True)
            return float(pd.to_numeric(df.loc[mask, "total"], errors="coerce").fillna(0).sum())

        # visible_ads = sum(total) where description contains "ProductAdsPayment"
        visible_ads_total = sum_total_where_desc_contains(["ProductAdsPayment"])

        # dealsvouchar_ads = sum(total) where description contains any of these keywords
        dealsvouchar_ads_total = sum_total_where_desc_contains([
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
        ])

        # platformfeenew = sum(total) where description contains "Subscription"
        platformfeenew_total = sum_total_where_desc_contains(["Subscription"])

        # platform_fee_inventory_storage = sum(total) where description contains any of these
        platform_fee_inventory_storage_total = sum_total_where_desc_contains([
            "FBA Return Fee",
            "FBA Long-Term Storage Fee",
            "FBA storage fee",
            "FBADisposal",
            "FBAStorageBilling",
            "FBALongTermStorageBilling",
            "INCORRECT_FEES_NON_ITEMIZED",
            "StorageReservationBilling",
        ])
        # ================== END NEW: TOTAL-ONLY BREAKUP COLUMNS ==================


        LOST_DESCRIPTIONS = {
            "REVERSAL_REIMBURSEMENT",
            "WAREHOUSE_LOST",
            "WAREHOUSE_DAMAGE",
            "MISSING_FROM_INBOUND",
            "MISSING_FROM_INBOUND_CLAWBACK",
            "COMPENSATED_CLAWBACK",
        }



        lost_mask = desc_str.isin(LOST_DESCRIPTIONS)

        lost_qty_df = (
            df.loc[lost_mask]
            .groupby("sku", as_index=False)["quantity"]
            .sum()
            .rename(columns={"quantity": "lost_quantity"})
        )
        lost_qty_df["lost_quantity"] = pd.to_numeric(lost_qty_df["lost_quantity"], errors="coerce").fillna(0).abs()

        lost_total_df = (
            df.loc[lost_mask]
            .groupby("sku", as_index=False)["total"]    
            .sum()
            .rename(columns={"total": "lost_total"})
        )
        lost_total_df["lost_total"] = pd.to_numeric(lost_total_df["lost_total"], errors="coerce").fillna(0)


        # ---------- BASE DF (exclude Refund + LOST rows from core metrics) ----------
        type_str_main = df.get("type", pd.Series("", index=df.index)).astype(str).str.strip()
        desc_str_main = df.get("description", pd.Series("", index=df.index)).astype(str).str.strip()

        LOST_DESCRIPTIONS = {
            "REVERSAL_REIMBURSEMENT",
            "WAREHOUSE_LOST",
            "WAREHOUSE_DAMAGE",
            "MISSING_FROM_INBOUND",
        }

        is_refund = type_str_main.str.contains("refund", case=False, na=False)  # type me refund likha ho
        is_lost   = desc_str_main.isin(LOST_DESCRIPTIONS)

        df_base = df.loc[~is_refund & ~is_lost].copy()     # ✅ core metrics only
        df_refund = df.loc[is_refund].copy()              # ✅ refund-only metrics
        # --------------------------------------------------------------------------
        # ---------- BASE DF (exclude Refund + LOST rows from core metrics) ----------
                     # ✅ refund-only metrics
        # --------------------------------------------------------------------------


        
# ------------------- END NEW LOGIC -------------------


        # ================= LEFTOUT OTHER TRANSACTION (TOTAL ONLY) =================

        desc_str = df.get("description", pd.Series("", index=df.index)).astype(str).str.strip()
        type_str2 = df.get("type", pd.Series("", index=df.index)).astype(str).str.strip()

        EXCLUDE_DESCRIPTIONS = {
            "Cost of Advertising",
            "Coupon Redemption Fee",
            "Deals",
            "Lightning Deal",
            "ProductAdsPayment",
            "CouponPerformanceEvent",
            "CouponParticipationEvent",
            "SellerDealComplete",
            "FBA Return Fee",
            "FBA Long-Term Storage Fee",
            "FBA storage fee",
            "Subscription",
            "FBADisposal",
            "FBAStorageBilling",
            "FBALongTermStorageBilling",
            "Order Payment",
            "REVERSAL_REIMBURSEMENT",
            "WAREHOUSE_LOST",
            "WAREHOUSE_DAMAGE",
            "MISSING_FROM_INBOUND",
            "Refund",
            "Disbursement",
            "DebtPayment",
            "INCORRECT_FEES_NON_ITEMIZED",
            "StorageReservationBilling",
            "MISSING_FROM_INBOUND_CLAWBACK",
            "COMPENSATED_CLAWBACK",
            "SellerPoweredCoupon",
            "VineCharge", "DealParticipationEvent",
            "DealPerformanceEvent",
            
        }

        EXCLUDE_TYPES = {"Transfer", "Refund"}

        leftout_mask = (~desc_str.isin(EXCLUDE_DESCRIPTIONS)) & (~type_str2.isin(EXCLUDE_TYPES))

        misc_transaction_total = (
            pd.to_numeric(df.loc[leftout_mask, "total"], errors="coerce")
            .fillna(0)
            .sum()
        )

# ========================================================================



        # ---------------------------------------------------------------------
        # Centralized platform fee & advertising using helpers
        # ---------------------------------------------------------------------
        platform_total, platform_by_sku, _ = uk_platform_fee(df)
        advertising_total_all, advertising_by_sku, _ = uk_advertising(df)

        # SKU cleaning
        df = df[df["sku"].astype(str).str.strip() != "0"]
        df = df[df["sku"].notna() & (df["sku"].astype(str).str.strip() != "")]

        # ============================================================
        # NEW (TOP): Refund-only Sales + Non-refund Net Tax + Non-refund Digital Tax (SKU-wise)
        # ============================================================

        # make sure type/sku string
        df["sku"] = df["sku"].astype(str).str.strip()
        df["type"] = df.get("type", "").astype(str).str.strip()

        refund_mask = df["type"].str.contains("refund", case=False, na=False)
        df_non_refund = df.loc[refund_mask].copy()
        df_non_refund = df.loc[~refund_mask].copy()

        # ---------- 1) newrefundsales (refund keyword rows only) ----------
        refund_sales_cols = ["product_sales"]
        for c in refund_sales_cols:
            if c not in df_non_refund.columns:
                df_non_refund[c] = 0.0

        newrefundsales_df = (
            df_non_refund.groupby("sku", as_index=False)[refund_sales_cols].sum()
        )

        newrefundsales_df["newrefundsales"] = (
            pd.to_numeric(newrefundsales_df["product_sales"], errors="coerce").fillna(0.0)
            
            
        )

        newrefundsales_df["newrefundsales"] = newrefundsales_df["newrefundsales"].apply(
            lambda x: 0.0 if abs(x) < 1e-6 else x
        )

        # newrefundsales_df = newrefundsales_df[["sku", "newrefundsales"]]
        # final name should be refund_sales (same as tables)
        newrefundsales_df = newrefundsales_df[["sku", "newrefundsales"]].rename(
            columns={"newrefundsales": "refund_sales"}
        )







        # ============================================================
        # FIXED: Refund-only Sales + Net Tax (SKU-wise) with breakup prints
        # ============================================================

        # make sure sku/type are strings
        df["sku"] = df["sku"].astype(str).str.strip()
        df["type"] = df.get("type", "").astype(str).str.strip()

        refund_mask = df["type"].str.contains("refund", case=False, na=False)
        df_non_refund   = df.loc[refund_mask].copy()
        df_non_refund  = df.loc[~refund_mask].copy()

        

        # # ---------- 3) digital_transaction_tax (NON-refund rows only) ----------
        # # formula you gave, but for non-refund rows
        # non_refund_digital_cols = ["product_sales_tax", "shipping_credits", "shipping_credits_tax", "promotional_rebates_tax"]
        # for c in non_refund_digital_cols:
        #     if c not in df_non_refund.columns:
        #         df_non_refund[c] = 0.0

        # digital_tax_non_refund_df = (
        #     df_non_refund.groupby("sku", as_index=False)[non_refund_digital_cols].sum()
        # )

        # digital_tax_non_refund_df["digital_transaction_tax"] = (
        #     pd.to_numeric(digital_tax_non_refund_df["product_sales_tax"], errors="coerce").fillna(0.0)
        #     + pd.to_numeric(digital_tax_non_refund_df["shipping_credits"], errors="coerce").fillna(0.0)
        #     + pd.to_numeric(digital_tax_non_refund_df["shipping_credits_tax"], errors="coerce").fillna(0.0)
        #     + pd.to_numeric(digital_tax_non_refund_df["promotional_rebates_tax"], errors="coerce").fillna(0.0)
        # )

        # digital_tax_non_refund_df = digital_tax_non_refund_df[["sku", "digital_transaction_tax"]]

        
        
# ============================================================


        refund_fees = df[type_str.eq("Refund")].groupby("sku")["selling_fees"].sum().reset_index()
        refund_fees.rename(columns={"selling_fees": "refund_selling_fees"}, inplace=True)
        refund_fees["sku"] = refund_fees["sku"].astype(str).str.strip()
        df["sku"] = df["sku"].astype(str).str.strip()

        df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce").fillna(0)

# ---------------- REFUND / RETURN QTY ----------------
        df_refund = df[type_str.eq("Refund")].copy()

        # return_quantity = sum(quantity) where type == "Refund" (SKU wise)
        return_qty_df = (
            df_refund.groupby("sku", as_index=False)["quantity"]
            .sum()
            .rename(columns={"quantity": "return_quantity"})
        )

        # refunds should be treated as positive units (safe)
        return_qty_df["return_quantity"] = (
            pd.to_numeric(return_qty_df["return_quantity"], errors="coerce")
            .fillna(0)
            .abs()
        )

        # ---------------- TOTAL QTY (per SKU) ----------------
        quantity_df = (
            df.groupby("sku", as_index=False)["quantity"]
            .sum()
            .rename(columns={"quantity": "quantity"})
        )

        # merge lost quantities
        quantity_df = quantity_df.merge(lost_qty_df, on="sku", how="left")
        quantity_df["lost_quantity"] = (
            pd.to_numeric(quantity_df.get("lost_quantity", 0), errors="coerce")
            .fillna(0)
            .abs()
        )

        # merge refund quantities
        quantity_df = quantity_df.merge(return_qty_df, on="sku", how="left")
        quantity_df["return_quantity"] = (
            pd.to_numeric(quantity_df.get("return_quantity", 0), errors="coerce")
            .fillna(0)
            .abs()
        )

        # ✅ FINAL: quantity = total - lost - refund
        quantity_df["quantity"] = (
            pd.to_numeric(quantity_df["quantity"], errors="coerce").fillna(0)
            - quantity_df["lost_quantity"]
            - quantity_df["return_quantity"]
        )

        # optional: drop helper cols if you don't want them here
        quantity_df.drop(columns=["lost_quantity"], inplace=True)
        # quantity_df already contains return_quantity; avoid duplicate merge collisions
        quantity_df = quantity_df.drop(columns=["return_quantity"], errors="ignore")



        type_str_main = df.get("type", pd.Series("", index=df.index)).astype(str).str.strip()
        desc_str_main = df.get("description", pd.Series("", index=df.index)).astype(str).str.strip()

        LOST_DESCRIPTIONS = {
            "REVERSAL_REIMBURSEMENT",
            "WAREHOUSE_LOST",
            "WAREHOUSE_DAMAGE",
            "MISSING_FROM_INBOUND",
        }

        is_refund = type_str_main.str.contains("refund", case=False, na=False)  # type me refund likha ho
        is_lost   = desc_str_main.isin(LOST_DESCRIPTIONS)

        df_base = df.loc[~is_refund & ~is_lost].copy()     # ✅ core metrics only
        df_refund = df.loc[is_refund].copy() 


        sku_grouped = df_base.groupby('sku').agg({
            "price_in_gbp": "mean",
            "product_sales": "sum",
            "promotional_rebates": "sum",
            "promotional_rebates_tax": "sum",
            "product_sales_tax": "sum",
            "selling_fees": "sum",
            "fba_fees": "sum",
            "other": "sum",
            "answer": "sum",
            "difference": "sum",
            "marketplace_facilitator_tax": "sum",
            "shipping_credits_tax": "sum",
            "giftwrap_credits_tax": "sum",
            "postage_credits": "sum",
            "gift_wrap_credits": "sum",
            "cost_of_unit_sold": "sum",
            "total": "sum",
            "other_transaction_fees": "sum",
            "product_name": "first",
            "errorstatus": "first",
            **({"shipping_credits": "sum"} if "shipping_credits" in df_base.columns else {}),
            **({"shipment_charges": "sum"} if "shipment_charges" in df_base.columns else {}),
        }).reset_index()


        # make sure sku/type are strings
        df["sku"]  = df["sku"].astype(str).str.strip()
        df["type"] = df.get("type", "").astype(str).str.strip()

        refund_mask = df["type"].str.contains("refund", case=False, na=False)

        # split
        df_non_refund   = df.loc[refund_mask].copy()
        df_non_refund  = df.loc[~refund_mask].copy()

        



        # ---------------- merge TOP computed columns into sku_grouped ----------------
        sku_grouped["sku"] = sku_grouped["sku"].astype(str).str.strip()

        # 1) newrefundsales
        sku_grouped = sku_grouped.merge(newrefundsales_df, on="sku", how="left")
        sku_grouped["refund_sales"] = pd.to_numeric(sku_grouped.get("refund_sales", 0), errors="coerce").fillna(0.0)

        # 2) net_tax + digital_transaction_tax
      
        # sku_grouped = sku_grouped.merge(digital_tax_non_refund_df, on="sku", how="left")
        # sku_grouped["digital_transaction_tax"] = pd.to_numeric(
        #     sku_grouped.get("digital_transaction_tax", 0),
        #     errors="coerce"
        # ).fillna(0.0)

        



        sku_grouped = sku_grouped.merge(lost_total_df, on="sku", how="left")
        sku_grouped["lost_total"] = pd.to_numeric(sku_grouped["lost_total"], errors="coerce").fillna(0)

        

        sku_grouped = sku_grouped.merge(df_prev, on="sku", how="left").fillna(0)

        sku_grouped["sku"] = sku_grouped["sku"].astype(str).str.strip()
        sku_grouped = sku_grouped.merge(refund_fees, on="sku", how="left")

        # Merge the filtered quantity data
        sku_grouped = sku_grouped.merge(quantity_df, on="sku", how="left")

        # Merge return_quantity
        sku_grouped = sku_grouped.merge(return_qty_df, on="sku", how="left")
        sku_grouped["return_quantity"] = pd.to_numeric(sku_grouped["return_quantity"], errors="coerce").fillna(0).astype(int)

      
        sku_grouped["total_quantity"] = (
            pd.to_numeric(sku_grouped["quantity"], errors="coerce").fillna(0).astype(int)
            - sku_grouped["return_quantity"]
        ).astype(int)

        # ✅ FINAL COGS = price_in_gbp * total_quantity
        sku_grouped["price_in_gbp"] = pd.to_numeric(sku_grouped.get("price_in_gbp", 0), errors="coerce").fillna(0.0)
        sku_grouped["total_quantity"] = pd.to_numeric(sku_grouped.get("total_quantity", 0), errors="coerce").fillna(0).astype(int)

        sku_grouped["cost_of_unit_sold"] = sku_grouped["price_in_gbp"] * sku_grouped["total_quantity"]



        # Refund fee adjustment
        sku_grouped["selling_fees"] = pd.to_numeric(sku_grouped["selling_fees"], errors='coerce').fillna(0)
        sku_grouped["refund_selling_fees"] = pd.to_numeric(sku_grouped["refund_selling_fees"], errors='coerce').fillna(0)
        sku_grouped["selling_fees"] -= 2 * sku_grouped["refund_selling_fees"]

        # ---------------------------------------------------------------------
        # SHARED UK formulas for Net Sales / Net Taxes / net_credits / Fees / Profit
        # ---------------------------------------------------------------------
        
        sales_total, sales_by_sku, _ = uk_sales(df)
        fee_total,   fees_by_sku,  _ = uk_amazon_fee(df_base)
        # TAX ONLY (needed everywhere later)
        df_tax = df_base.copy()
        df_tax["type"] = df_tax.get("type", "").astype(str)
        df_tax = df_tax.loc[~df_tax["type"].str.contains("refund", case=False, na=False)].copy()

        tax_total, tax_by_sku, tax_parts = uk_tax(df_tax)

        if not tax_by_sku.empty:
            sku_grouped = sku_grouped.merge(
                tax_by_sku[["sku", "__metric__"]].rename(columns={"__metric__": "net_taxes"}),
                on="sku", how="left"
            )
        else:
            sku_grouped["net_taxes"] = 0.0



        sku_grouped["sales_tax_refund"] = 0.0

        sku_grouped["sales_tax_refund"] = pd.to_numeric(
            sku_grouped["sales_tax_refund"], errors="coerce"
        ).fillna(0) * 0.5

        
        sku_grouped["sales_credit_refund"] = 0.0

        # refund_rebate = promotional_rebates for Refund rows sku wise
        refund_rebate_df = (
            df_refund.groupby("sku", as_index=False)["promotional_rebates"]
            .sum()
            .rename(columns={"promotional_rebates": "refund_rebate"})
        ) if not df_refund.empty else pd.DataFrame(columns=["sku", "refund_rebate"])

        sku_grouped = sku_grouped.merge(refund_rebate_df, on="sku", how="left")
        sku_grouped["refund_rebate"] = pd.to_numeric(sku_grouped["refund_rebate"], errors="coerce").fillna(0.0)

        



        # Merge shared results into the working table with your expected column names
        if not sales_by_sku.empty:
            sku_grouped = sku_grouped.merge(
                sales_by_sku[["sku", "__metric__"]].rename(columns={"__metric__": "Net Sales"}),
                on="sku", how="left"
            )
        else:
            sku_grouped["Net Sales"] = 0.0

        
        # --------------------------------------------------------------


        # ---------- UPDATED: Gross Sales formula ----------


        for _c in [
            "product_sales", "product_sales_tax",
            "postage_credits", "gift_wrap_credits",
            "shipping_credits_tax", "giftwrap_credits_tax",
            "promotional_rebates", "promotional_rebates_tax"
        ]:
            if _c not in sku_grouped.columns:
                sku_grouped[_c] = 0.0

        sku_grouped["gross_sales"] = (
            pd.to_numeric(sku_grouped["product_sales"], errors="coerce").fillna(0.0)
            + pd.to_numeric(sku_grouped["product_sales_tax"], errors="coerce").fillna(0.0)
            + pd.to_numeric(sku_grouped["postage_credits"], errors="coerce").fillna(0.0)
            + pd.to_numeric(sku_grouped["gift_wrap_credits"], errors="coerce").fillna(0.0)
            + pd.to_numeric(sku_grouped["shipping_credits_tax"], errors="coerce").fillna(0.0)
            + pd.to_numeric(sku_grouped["giftwrap_credits_tax"], errors="coerce").fillna(0.0)
            + pd.to_numeric(sku_grouped["promotional_rebates"], errors="coerce").fillna(0.0)
            + pd.to_numeric(sku_grouped["promotional_rebates_tax"], errors="coerce").fillna(0.0)
        )


        for _col in [ "sales_tax_refund", "sales_credit_refund", "refund_rebate", "gross_sales"]:
            sku_grouped[_col] = pd.to_numeric(sku_grouped[_col], errors="coerce").fillna(0.0)

      

        credits_total, credits_by_sku, credit_parts = uk_credits(df)

        if not credits_by_sku.empty:
            sku_grouped = sku_grouped.merge(
                credits_by_sku[["sku", "__metric__"]]
                .rename(columns={"__metric__": "net_credits"}),
                on="sku",
                how="left"
            )
        else:
            sku_grouped["net_credits"] = 0.0

        sku_grouped["net_credits"] = pd.to_numeric(
            sku_grouped.get("net_credits", 0),
            errors="coerce"
        ).fillna(0.0)

        # ============================================================
        # 🔥 FINAL PROFIT (REPLACES uk_profit COMPLETELY)
        # ============================================================

        sales_total   = float(sales_total)
        fee_total     = float(fee_total)
        tax_total     = abs(float(tax_total))
        credits_total = float(credits_total)

        # COGS already computed earlier:
        # sku_grouped["cost_of_unit_sold"] = price_in_gbp * total_quantity
        cogs_total = float(
            pd.to_numeric(sku_grouped["cost_of_unit_sold"], errors="coerce")
            .fillna(0.0)
            .sum()
        )

        uk_profit_final = (
            sales_total
            - fee_total
            - tax_total
            + credits_total
            - cogs_total
        )

        



        if not fees_by_sku.empty:
            sku_grouped = sku_grouped.merge(
                fees_by_sku[["sku", "__metric__"]].rename(columns={"__metric__": "amazon_fee"}),
                on="sku", how="left"
            )
        else:
            sku_grouped["amazon_fee"] = 0.0


        # NEW: merge centralized platform/ad per-SKU into sku_grouped
        if not platform_by_sku.empty:
            sku_grouped = sku_grouped.merge(
                platform_by_sku[["sku", "__metric__"]].rename(columns={"__metric__": "platform_fee"}),
                on="sku", how="left"
            )
        else:
            sku_grouped["platform_fee"] = 0.0

        # ✅ platform_fee = platform_fee + lost_total (SKU-wise)
        sku_grouped["platform_fee"] = pd.to_numeric(sku_grouped.get("platform_fee", 0), errors="coerce").fillna(0.0)
        sku_grouped["lost_total"]   = pd.to_numeric(sku_grouped.get("lost_total", 0), errors="coerce").fillna(0.0)

        sku_grouped["platform_fee"] = sku_grouped["platform_fee"] + sku_grouped["lost_total"]

       


        if not advertising_by_sku.empty:
            sku_grouped = sku_grouped.merge(
                advertising_by_sku[["sku", "__metric__"]].rename(columns={"__metric__": "advertising_total"}),
                on="sku", how="left"
            )
        else:
            sku_grouped["advertising_total"] = 0.0

        for _col in ["Net Sales", "net_credits", "amazon_fee",  "platform_fee", "advertising_total"]:
            if _col in sku_grouped.columns:
                sku_grouped[_col] = pd.to_numeric(sku_grouped[_col], errors="coerce").fillna(0.0)

        # total_product_sales = sku_grouped["product_sales"].sum()
        total_product_sales = (
            sku_grouped["product_sales"].sum()
            + sku_grouped["product_sales_tax"].sum()
            + sku_grouped["postage_credits"].sum()
            + sku_grouped["shipping_credits_tax"].sum()
            + sku_grouped["gift_wrap_credits"].sum()
            + sku_grouped["promotional_rebates"].sum()
            + sku_grouped["promotional_rebates_tax"].sum()
        )




        
        
        sku_grouped["asp"] = (sku_grouped["Net Sales"] / sku_grouped["quantity"]).replace([float('inf'), -float('inf')], 0).fillna(0)
        
        sku_grouped["previous_text_credit_change"] = ((sku_grouped["previous_net_taxes"] - sku_grouped["previous_net_credits"]) / sku_grouped["previous_quantity"]).replace([float('inf'), -float('inf')], 0).fillna(0)

        # Profit change & growth

        sku_grouped["month"] = month
        sku_grouped["year"] = year
        sku_grouped["country"] = country
        # misc_transaction is ONLY for TOTAL row
        sku_grouped["misc_transaction"] = 0.0

        # these will already be merged; keep initialization for schema safety (won't hurt)
        sku_grouped["platform_fee"] = sku_grouped.get("platform_fee", 0).fillna(0)
        sku_grouped["rembursement_fee"] = 0
        # advertising_total merged above; ensure present
        sku_grouped["advertising_total"] = sku_grouped.get("advertising_total", 0).fillna(0)
        sku_grouped["reimbursement_vs_sales"] = 0
        sku_grouped["cm2_profit"] = 0
        sku_grouped["cm2_margins"] = 0
        sku_grouped["acos"] = 0
        sku_grouped["rembursment_vs_cm2_margins"] = 0
        sku_grouped["visible_ads"] = 0.0
        sku_grouped["dealsvouchar_ads"] = 0.0
        sku_grouped["platformfeenew"] = 0.0
        sku_grouped["platform_fee_inventory_storage"] = 0.0

        # Ensure the two columns exist even if missing in source
        for _col in ("shipping_credits", "shipment_charges"):
            if _col not in sku_grouped.columns:
                sku_grouped[_col] = 0.0

        # Sales % and growth
        sku_grouped["sales_percentage"] = ((sku_grouped["Net Sales"] - sku_grouped["previous_net_sales"]) / sku_grouped["previous_net_sales"]) * 100
        sku_grouped["sales_percentage"] = sku_grouped["sales_percentage"].replace([float('inf'), -float('inf')], 0).fillna(0)
        sku_grouped["sales_growth"] = np.select(
            [
                sku_grouped["sales_percentage"] >= 5,
                sku_grouped["sales_percentage"] > 0.5,
                sku_grouped["sales_percentage"] < -0.5
            ],
            ["High Growth", "Low Growth", "Negative Growth"],
            default="No Growth"
        )

        sku_grouped["user_id"] = user_id
        total_amazon_fee = sku_grouped["amazon_fee"].sum()

        # Totals
        total_sales = abs(sku_grouped["Net Sales"].sum())
        
        # Fee ratios
        
        
        # ---- merge refund digital tax into sku_grouped
        

        # ---- postage_credit_tax column fallback
        postage_credit_tax_col = "postage_credits_tax" if "postage_credits_tax" in sku_grouped.columns else "shipping_credits_tax"
        if postage_credit_tax_col not in sku_grouped.columns:
            sku_grouped[postage_credit_tax_col] = 0.0

        # ---- ensure non-refund base cols exist
        for _c in ["product_sales_tax", "promotional_rebates_tax", "marketplace_facilitator_tax"]:
            if _c not in sku_grouped.columns:
                sku_grouped[_c] = 0.0
            sku_grouped[_c] = pd.to_numeric(sku_grouped[_c], errors="coerce").fillna(0.0)

        # ---- ensure refund cols exist
        for _c in ["sales_tax_refund", "refund_rebate"]:
            if _c not in sku_grouped.columns:
                sku_grouped[_c] = 0.0
            sku_grouped[_c] = pd.to_numeric(sku_grouped[_c], errors="coerce").fillna(0.0)


        sku_grouped["tex_and_credits"] = (
            pd.to_numeric(sku_grouped["net_taxes"], errors="coerce").fillna(0)
            - pd.to_numeric(sku_grouped["net_credits"], errors="coerce").fillna(0)
        )


        # ---------------- PROFIT (FINAL, SKU-WISE) ----------------

        for col in ["Net Sales", "amazon_fee", "net_taxes", "net_credits", "cost_of_unit_sold"]:
            if col not in sku_grouped.columns:
                sku_grouped[col] = 0.0
            sku_grouped[col] = pd.to_numeric(sku_grouped[col], errors="coerce").fillna(0.0)

        sku_grouped["profit"] = (
            sku_grouped["Net Sales"]
            - sku_grouped["amazon_fee"]
            - sku_grouped["net_taxes"]
            + sku_grouped["net_credits"]
            - sku_grouped["cost_of_unit_sold"]
        )
        


        total_profit = abs(sku_grouped["profit"].sum())
        total_Previous_profit = abs(sku_grouped["previous_profit"].sum())
        total_Previous_sales = abs(sku_grouped["previous_net_sales"].sum())

        sku_grouped["sales_mix"] = (sku_grouped["Net Sales"] / total_sales) * 100
        sku_grouped["sales_mix"] = sku_grouped["sales_mix"].replace([float('inf'), -float('inf')], 0).fillna(0)

        sku_grouped["profit_mix"] = (sku_grouped["profit"] / total_profit) * 100
        sku_grouped["profit_mix"] = sku_grouped["profit_mix"].replace([float('inf'), -float('inf')], 0).fillna(0)

        total_profit_final = sku_grouped["profit"].sum()

        sku_grouped["profit%"] = abs((sku_grouped["profit"] / abs(sku_grouped["Net Sales"])) * 100)
        sku_grouped["profit%"] = sku_grouped["profit%"].replace([float('inf'), -float('inf')], 0).fillna(0)

        sku_grouped["unit_wise_profitability"] = (sku_grouped["profit"] / sku_grouped["total_quantity"]).replace([float("inf"), -float("inf")], 0).fillna(0)

        sku_grouped["month"] = month
        sku_grouped["year"] = year
        sku_grouped["country"] = country



        






        # Ensure integer quantities for DB
        sku_grouped["quantity"] = pd.to_numeric(sku_grouped["quantity"], errors="coerce").fillna(0).astype(int)
        sku_grouped["previous_quantity"] = pd.to_numeric(sku_grouped["previous_quantity"], errors="coerce").fillna(0).astype(int)
        total_cous = abs(sku_grouped["cost_of_unit_sold"].sum())

        # === EXPENSE BREAKDOWN ===
        total_net_credits = abs(sku_grouped["net_credits"].sum())
        total_net_taxes = abs(sku_grouped["net_taxes"].sum())
        total_fba_fees = abs(sku_grouped["fba_fees"].sum())
        total_selling_fees = abs(sku_grouped["selling_fees"].sum())
        total_cost = abs(sku_grouped["cost_of_unit_sold"].sum())
        # use centralized totals
        total_advertising = abs(advertising_total_all)
        total_platform = abs(platform_total)

        total_expense = round(
            total_amazon_fee
            + abs(platformfeenew_total)
            + abs(platform_fee_inventory_storage_total),
            2
        )

        # Additional Metrics
        platform_fee = float(platform_total)
        advertising_total = float(advertising_total_all)
        lost_total_amount = float(pd.to_numeric(sku_grouped["lost_total"], errors="coerce").fillna(0).sum())


        reimbursement_vs_sales = abs((rembursement_fee / total_sales) * 100) if total_sales != 0 else 0
        cm2_profit = total_profit - lost_total_amount - (abs(advertising_total) + abs(platform_fee))
        cm2_margins = (cm2_profit / total_sales) * 100 if total_sales != 0 else 0
        acos = (advertising_total / total_sales) * 100 if total_sales != 0 else 0
        rembursment_vs_cm2_margins = abs((rembursement_fee / cm2_profit) * 100) if cm2_profit != 0 else 0

        # ------------------ FIXED TOTAL ROW BUILD (DEDUP + UNIQUE COLUMNS) ------------------
        extra_cols_for_total = [
            "Net Sales", "net_taxes", "net_credits", "profit", "amazon_fee",
            "sales_mix", "previous_sales_mix", "sales_mix_percentage",
            "profit_mix", "previous_profit_mix",
            "unit_sales_analysis", "unit_asp_analysis", "amazon_fee_increase",
            "cross_check_analysis", "text_credit_increase", "final_total_analysis",
            "positive_action", "negative_action", "cross_check_analysis_backup",
            "total_analysis", "shipping_credits", "shipment_charges", "return_quantity",
            "total_quantity",
            "refund_sales",
            "gross_sales",
            "sales_tax_refund",
            "sales_credit_refund",
            "refund_rebate",
            "lost_total",
            "misc_transaction",
            "promotional_rebates_percentage",
            "promotional_rebates_percentage",

    # NEW total-only columns
            "visible_ads",
            "dealsvouchar_ads",
            "platformfeenew",
            "platform_fee_inventory_storage",
            "tex_and_credits",
            "cm2_profit_percentage",
        ]

        cols_for_sum = list(dict.fromkeys(numeric_columns + extra_cols_for_total))
        cols_for_sum = [c for c in cols_for_sum if c in sku_grouped.columns]

        sum_row = sku_grouped[cols_for_sum].sum(numeric_only=True)
        if "quantity" not in sum_row.index and "quantity" in sku_grouped.columns:
            sum_row["quantity"] = sku_grouped["quantity"].sum()
        if "previous_quantity" not in sum_row.index and "previous_quantity" in sku_grouped.columns:
            sum_row["previous_quantity"] = sku_grouped["previous_quantity"].sum()

        sum_row["sku"] = "TOTAL"
        sum_row["month"] = month
        sum_row["country"] = country
        sum_row["year"] = year
        sum_row["product_name"] = "TOTAL"
        sum_row["profit%"] = (sum_row.get("profit", 0) / sum_row.get("Net Sales", 0)) * 100 if sum_row.get("Net Sales", 0) != 0 else 0

        # ---------- FIX TOTAL promotional_rebates_percentage ----------
        total_ns = float(sum_row.get("Net Sales", 0) or 0)
        total_pr = float(sum_row.get("promotional_rebates", 0) or 0)

        sum_row["promotional_rebates_percentage"] = (total_pr / total_ns) * 100 if total_ns != 0 else 0.0


        # sum_row["platform_fee"] = abs(platform_fee)
        # sum_row["rembursement_fee"]= abs(rembursement_fee)
        lost_total_total = float(sum_row.get("lost_total", 0) or 0)  # this is -334.62 in your case
        sum_row["platform_fee"] = float(platform_total) + lost_total_total
        sum_row["rembursement_fee"] = abs(rembursement_fee) 

        sum_row["advertising_total"]= abs(advertising_total)
        sum_row["reimbursement_vs_sales"]= abs(reimbursement_vs_sales)
        sum_row["cm2_profit"]= abs(cm2_profit)
        # TOTAL-only safety (after sum_row["cm2_profit"] is set)
        ns_total = float(sum_row.get("Net Sales", 0) or 0)
        sum_row["cm2_profit_percentage"] = (float(sum_row.get("cm2_profit", 0) or 0) / ns_total) * 100 if ns_total != 0 else 0.0

        sum_row["cm2_margins"]= abs(cm2_margins)
        sum_row["acos"]= abs(acos)
        sum_row["rembursment_vs_cm2_margins"]= abs(rembursment_vs_cm2_margins)
        sum_row["previous_net_sales"]= a
        sum_row["previous_net_credits"]= b
        sum_row["previous_platform_fee"]= c
        sum_row["previous_rembursement_fee"]= d
        sum_row["previous_advertising_total"]= e
        sum_row["previous_reimbursement_vs_sales"]= f
        sum_row["previous_cm2_profit"]= g
        sum_row["previous_cm2_margins"]= h
        sum_row["previous_acos"]= i
        sum_row["previous_rembursment_vs_cm2_margins"]= j
        sum_row["previous_profit"]= k
        sum_row["previous_profit_percentage"]= l
        sum_row["previous_quantity"]= m
        sum_row["previous_cost_of_unit_sold"]= n
        sum_row["previous_amazon_fee"]= o
        sum_row["previous_net_taxes"]= p
        sum_row["previous_fba_fees"]= q
        sum_row["previous_selling_fees"]= r
        sum_row["return_quantity"] = int(float(sum_row.get("return_quantity", 0) or 0))
        sum_row["total_quantity"]  = int(float(sum_row.get("total_quantity", 0) or 0))
        sum_row["misc_transaction"] = float(misc_transaction_total)
        sum_row["tex_and_credits"] = (
            float(sum_row.get("net_taxes", 0))
            - float(sum_row.get("net_credits", 0))
        )




        # sum_row["digital_transaction_tax"] = int(float(sum_row.get("digital_transaction_tax", 0) or 0))





        # Totals part 2 (derived)
        qty = float(sum_row.get("total_quantity", 0) or 0)
        prev_qty = float(sum_row.get("previous_quantity", 0) or 0)
        net_sales_total = float(sum_row.get("Net Sales", 0) or 0)
        prev_net_sales_total = float(sum_row.get("previous_net_sales", 0) or 0)

        sum_row["unit_wise_profitability"] = ((float(sum_row.get("profit", 0))) / qty) if qty != 0 else 0



        sum_row["unit_increase"] = (
            (qty - prev_qty) / prev_qty
        ) * 100 if prev_qty != 0 else 0


        sum_row["asp"] = (net_sales_total / qty) if qty != 0 else 0
        sum_row["previous_asp"] = (prev_net_sales_total / prev_qty) if prev_qty != 0 else 0

        prev_asp = float(sum_row.get("previous_asp", 0) or 0)
        sum_row["asp_percentag"] = (
            (float(sum_row["asp"]) - prev_asp) / prev_asp
        ) * 100 if prev_asp != 0 else 0

        prev_amz_fee = float(sum_row.get("previous_amazon_fee", 0) or 0)
        sum_row["change_in_fee"] = ((float(sum_row.get("amazon_fee", 0))) / net_sales_total) * 100 if net_sales_total != 0 else 0
        sum_row["previous_change_in_fee"] = (prev_amz_fee / prev_net_sales_total) * 100 if prev_net_sales_total != 0 else 0

        sum_row["precentage_change_in_fee"] = (sum_row["change_in_fee"]) - (sum_row["previous_change_in_fee"])

        total_taxes_sum = float(sum_row.get("net_taxes", 0) or 0)
        sum_row["unit_wise_amazon_fee"] = ((float(sum_row.get("amazon_fee", 0)) - total_taxes_sum) / qty) if qty != 0 else 0

        sum_row["previous_unit_wise_amazon_fee"] = (
            (prev_amz_fee - float(sum_row.get("previous_net_taxes", 0) or 0)) /
            prev_qty
        ) if prev_qty != 0 else 0

        prev_uwaf = float(sum_row.get("previous_unit_wise_amazon_fee", 0) or 0)
        sum_row["unit_wise_amazon_fee_percentage"] = (
            (float(sum_row.get("unit_wise_amazon_fee", 0)) - prev_uwaf) / prev_uwaf
        ) * 100 if prev_uwaf != 0 else 0

        prev_profit_total = float(sum_row.get("previous_profit", 0) or 0)
        

        prev_profit_mix_total = float(sum_row.get("previous_profit_mix", 0) or 0)
        sum_row["profit_mix_percentage"] = (
            (float(sum_row.get("profit_mix", 0) or 0) - prev_profit_mix_total) /
            prev_profit_mix_total
        ) * 100  if prev_profit_mix_total != 0 else 0

        sum_row["sales_percentage"] = (
            (net_sales_total - prev_net_sales_total) /
            prev_net_sales_total
        ) * 100  if prev_net_sales_total != 0 else 0

        sum_row["text_credit_change"] = (
            (float(sum_row.get("net_credits", 0) or 0) + float(sum_row.get("profit", 0) or 0)) /
            net_sales_total
        ) if net_sales_total != 0 else 0

        sum_row["previous_text_credit_change"] = (
            (float(sum_row.get("previous_net_credits", 0) or 0) + prev_profit_total) /
            prev_net_sales_total
        ) if prev_net_sales_total != 0 else 0

       

        # Ensure sku_grouped has unique columns BEFORE appending total row
        if not sku_grouped.columns.is_unique:
            sku_grouped = sku_grouped.loc[:, ~sku_grouped.columns.duplicated()].copy()

        # Ensure sum_row quantities are integers (and exist)
        sum_row["quantity"] = int(float(sum_row.get("quantity", 0) or 0))
        sum_row["previous_quantity"] = int(float(sum_row.get("previous_quantity", 0) or 0))



        # ---------------- NEW: set TOTAL-only columns ----------------
        sum_row["visible_ads"] = float(visible_ads_total)
        sum_row["dealsvouchar_ads"] = float(dealsvouchar_ads_total)
        sum_row["platformfeenew"] = float(platformfeenew_total)
        sum_row["platform_fee_inventory_storage"] = float(platform_fee_inventory_storage_total)
        # -------------------------------------------------------------

        sum_row["user_id"] = user_id

        # Append total row safely
        sku_grouped = pd.concat(
            [sku_grouped, sum_row.to_frame().T],
            ignore_index=True,
            sort=False
        )

        # Ensure correct column names for database
        sku_grouped.rename(columns={
            "Net Sales": "net_sales",
            "profit%": "profit_percentage",
            "net_credits": "net_credits"
        }, inplace=True)


        # ✅ NEW: other_transaction formula
        for c in [ "net_taxes", "net_credits", "misc_transaction"]:
            if c not in sku_grouped.columns:
                sku_grouped[c] = 0.0

        sku_grouped["other_transaction_fees"] = (
            pd.to_numeric(sku_grouped["net_credits"], errors="coerce").fillna(0.0)
            + pd.to_numeric(sku_grouped["misc_transaction"], errors="coerce").fillna(0.0)
            - pd.to_numeric(sku_grouped["net_taxes"], errors="coerce").fillna(0.0)
        )



        sku_grouped["cm2_profit_percentage"] = np.where(
            pd.to_numeric(sku_grouped["net_sales"], errors="coerce").fillna(0) != 0,
            (pd.to_numeric(sku_grouped["cm2_profit"], errors="coerce").fillna(0) /
            pd.to_numeric(sku_grouped["net_sales"], errors="coerce").fillna(0)) * 100,
            0.0
        )
        sku_grouped["cm2_profit_percentage"] = (
            pd.to_numeric(sku_grouped["cm2_profit_percentage"], errors="coerce")
            .replace([float("inf"), -float("inf")], 0)
            .fillna(0.0)
        )

        # ---------- NEW: Promotional Rebates Percentage ----------
# promotional_rebates_percentage = promotional_rebates / net_sales * 100
        sku_grouped["promotional_rebates_percentage"] = np.where(
            pd.to_numeric(sku_grouped["net_sales"], errors="coerce").fillna(0) != 0,
            (pd.to_numeric(sku_grouped["promotional_rebates"], errors="coerce").fillna(0) /
            pd.to_numeric(sku_grouped["net_sales"], errors="coerce").fillna(0)) * 100,
            0.0
        )
        sku_grouped["promotional_rebates_percentage"] = (
            pd.to_numeric(sku_grouped["promotional_rebates_percentage"], errors="coerce")
            .replace([float("inf"), -float("inf")], 0)
            .fillna(0.0)
        )


        total_row = sku_grouped[sku_grouped['sku'].astype(str).str.lower() == 'total']
        other_rows = sku_grouped[sku_grouped['sku'].astype(str).str.lower() != 'total']
        other_rows_sorted = other_rows.sort_values(by="profit", ascending=False)

        # Ensure ints for DB
        integer_columns = ['quantity', 'previous_quantity', 'user_id']
        for col in integer_columns:
            if col in other_rows_sorted.columns:
                other_rows_sorted[col] = pd.to_numeric(other_rows_sorted[col], errors='coerce').fillna(0).astype(int)
            if col in total_row.columns:
                total_row[col] = pd.to_numeric(total_row[col], errors='coerce').fillna(0).astype(int)

        sku_grouped = pd.concat([other_rows_sorted, total_row], ignore_index=True)

        # Recreate monthly & rolling tables with aligned schemas
        conn.execute(text(f"DROP TABLE IF EXISTS {target_table}"))
        # conn.execute(text(f"DROP TABLE IF EXISTS {target_table2}"))
        conn.execute(text(f"DROP TABLE IF EXISTS {target_table_nse}"))

        for tbl in (target_table, target_table_usd_month):
            conn.execute(text(f"""
                CREATE TABLE IF NOT EXISTS {tbl} (
                    id SERIAL PRIMARY KEY,
                    product_name TEXT,
                    sku TEXT,
                    quantity INTEGER,
                    return_quantity INTEGER,
                    total_quantity INTEGER,
                    product_sales REAL,
                    product_sales_tax REAL,
                    postage_credits REAL,
                    gift_wrap_credits REAL,
                    giftwrap_credits_tax REAL,
                    promotional_rebates REAL,
                    promotional_rebates_tax REAL,
                    gross_sales REAL,
                    refund_sales REAL,
                    net_sales REAL,
                    cost_of_unit_sold REAL,
                    sales_tax_refund REAL,
                    sales_credit_refund REAL,
                    refund_rebate REAL,
                    marketplace_facilitator_tax REAL,
                    other_transaction_fees REAL,
                    selling_fees REAL,
                    refund_selling_fees REAL,
                    fba_fees REAL,
                    amazon_fee REAL,
                    digital_transaction_tax REAL,
                    net_taxes REAL,
                    net_credits REAL,
                    profit REAL,
                    profit_percentage REAL,
                    lost_total REAL,
                    visible_ads REAL,
                    dealsvouchar_ads REAL,
                    advertising_total REAL,
                    platformfeenew REAL,
                    platform_fee_inventory_storage REAL,
                    platform_fee REAL,
                    cm2_profit REAL,
                    cm2_profit_percentage REAL,
                    acos REAL,
                    rembursement_fee REAL,
                    reimbursement_vs_sales REAL,
                    cm2_margins REAL,
                    rembursment_vs_cm2_margins REAL,
                    misc_transaction REAL,
                    promotional_rebates_percentage REAL,
                    tex_and_credits REAL,
                    price_in_gbp REAL,
                    shipping_credits_tax REAL,
                    other REAL,
                    unit_wise_profitability REAL,
                    asp REAL,
                    sales_mix REAL,
                    profit_mix REAL,
                    sales_mix_analysis TEXT,
                    shipping_credits REAL,
                    shipment_charges REAL,
                    errorstatus TEXT,
                    answer REAL,
                    difference REAL,
                    month TEXT,
                    year TEXT,
                    country TEXT,
                    user_id INTEGER
                )
            """))
        for tbl in (target_table_nse):
            conn.execute(text(f"""
                CREATE TABLE IF NOT EXISTS {target_table_nse} (
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
                    platform_fee REAL,
                    platform_fee_inventory_storage REAL,
                    cm2_profit REAL,
                    cm2_profit_percentage REAL,
                    acos REAL,
                    rembursement_fee REAL,
                    rembursment_vs_cm2_margins REAL,
                    reimbursement_vs_sales REAL,
                    sales_mix REAL,
                    profit_mix REAL,
                    user_id INTEGER
                )
            """))

        for tbl in (target_table2, target_table_usd_roll):
            conn.execute(text(f"""
                CREATE TABLE IF NOT EXISTS {tbl} (
                    id SERIAL PRIMARY KEY,
                    product_name TEXT,
                    sku TEXT,
                    quantity INTEGER,
                    return_quantity INTEGER,
                    total_quantity INTEGER,
                    product_sales REAL,
                    product_sales_tax REAL,
                    postage_credits REAL,
                    gift_wrap_credits REAL,
                    giftwrap_credits_tax REAL,
                    promotional_rebates REAL,
                    promotional_rebates_tax REAL,
                    gross_sales REAL,
                    refund_sales REAL,
                    net_sales REAL,
                    cost_of_unit_sold REAL,
                    sales_tax_refund REAL,
                    sales_credit_refund REAL,
                    refund_rebate REAL,
                    marketplace_facilitator_tax REAL,
                    other_transaction_fees REAL,
                    selling_fees REAL,
                    refund_selling_fees REAL,
                    fba_fees REAL,
                    total REAL,
                    net_taxes REAL,
                    digital_transaction_tax REAL,
                    net_credits REAL,
                    profit REAL,
                    profit_percentage REAL,
                    lost_total REAL,
                    visible_ads REAL,
                    dealsvouchar_ads REAL,
                    advertising_total REAL,
                    platformfeenew REAL,
                    platform_fee_inventory_storage REAL,
                    platform_fee REAL,
                    cm2_profit REAL,
                    cm2_profit_percentage REAL,
                    acos REAL,
                    rembursement_fee REAL,
                    reimbursement_vs_sales REAL,
                    cm2_margins REAL,
                    rembursment_vs_cm2_margins REAL,
                    misc_transaction REAL,
                    promotional_rebates_percentage REAL,
                    tex_and_credits REAL,
                    price_in_gbp REAL,
                    shipping_credits_tax REAL,
                    other REAL,
                    amazon_fee REAL,
                    unit_wise_profitability REAL,
                    asp REAL,
                    sales_mix REAL,
                    sales_mix_analysis TEXT,
                    shipping_credits REAL,
                    shipment_charges REAL,
                    errorstatus TEXT,
                    answer REAL,
                    difference REAL,
                    month TEXT,
                    year TEXT,
                    country TEXT,
                    profit_mix REAL,
                    user_id INTEGER
                )
            """))

        currency1 = 'gbp'  # fallback/default


        def get_conversion_rate(dest_country: str):
            with engine1.connect() as conn1:
                currency_query = text("""
                        SELECT conversion_rate
                        FROM currency_conversion 
                        WHERE lower(user_currency) = :currency1
                        AND lower(country)      = :dest_country
                        AND lower(month)        = :month 
                        AND year                = :year
                        LIMIT 1
                """)
                result = conn1.execute(currency_query, {
                    "currency1": currency1.lower(),
                    "dest_country": dest_country.lower(),
                    "month": month.lower(),
                    "year": year
                }).fetchone()
            return result[0] if result else None

        rate_us   = get_conversion_rate("us")
        rate_ind  = get_conversion_rate("india")
        rate_can  = get_conversion_rate("canada")
        rate_gbp  = 1.0   # GBP → GBP, koi conversion nahi

        # Define monetary columns for USD conversion
        monetary_columns = [
            'price_in_gbp', 'product_sales', 'promotional_rebates', 'promotional_rebates_tax',
            'product_sales_tax', 'selling_fees', 'refund_selling_fees', 'fba_fees', 'other',
            'marketplace_facilitator_tax', 'shipping_credits_tax', 'giftwrap_credits_tax',
            'shipping_credits', 'gift_wrap_credits', 'net_sales', 'net_taxes', 'net_credits',
            'profit', 'profit_percentage', 'amazon_fee', 'cost_of_unit_sold',
            'other_transaction_fees', 'platform_fee', 'shipment_charges', 'rembursement_fee',
            'advertising_total', 'reimbursement_vs_sales', 'cm2_profit', 'cm2_margins', 'acos',
            'rembursment_vs_cm2_margins', 'total', 
             'profit_change',
            
            'unit_wise_profitability', 
            'unit_wise_profitability_percentage', 'asp', 'sales_percentage',
            'asp_percentag', 'text_credit_change', 
              'unit_increase', 'change_in_fee',
             'precentage_change_in_fee', 'unit_wise_amazon_fee',
            'unit_wise_amazon_fee_percentage',
            'profit_mix_percentage', 'unit_sales_analysis',
            'unit_asp_analysis', 'amazon_fee_increase', 'total_analysis', 'cross_check_analysis',
           
            'cross_check_analysis_backup', 'text_credit_increase', 'final_total_analysis', 'postage_credits', 'refund_sales', 'gross_sales', 'sales_tax_refund', 'sales_credit_refund', 'refund_rebate', 'lost_total',
            'misc_transaction', 'promotional_rebates_percentage','visible_ads' ,
            'dealsvouchar_ads',
            'platform_fee_inventory_storage',
            "tex_and_credits",       
            "cm2_profit_percentage",  


        ]

        df_usd  = sku_grouped.copy()
        df_ind  = sku_grouped.copy()
        df_can  = sku_grouped.copy()
        df_gbp  = sku_grouped.copy()  # GBP table (rate = 1)

        def apply_rate(df_conv, rate):
            if not rate:
                return
            for col in monetary_columns:
                if col in df_conv.columns:
                    df_conv[col] = pd.to_numeric(df_conv[col], errors="coerce").fillna(0) * rate

        # Apply conversions
        apply_rate(df_usd, rate_us)
        apply_rate(df_ind, rate_ind)
        apply_rate(df_can, rate_can)
        apply_rate(df_gbp, rate_gbp)   # yahan *1 hoga, ya tum chaaho to skip bhi kar sakte ho

        # Country column overwrite kar dete hain taki filter easy ho jaye
        df_usd["country"] = "us"
        df_ind["country"] = "india"
        df_can["country"] = "canada"
        df_gbp["country"] = "gbp"   # ya "uk_gbp" jo bhi tum chaho



        for df_conv in [df_usd, df_ind, df_can, df_gbp]:
            for col in df_conv.columns:
                if df_conv[col].dtype == 'object':
                    df_conv[col] = df_conv[col].fillna('')
                elif pd.api.types.is_numeric_dtype(df_conv[col]):
                    df_conv[col] = df_conv[col].fillna(0)



        # USD table (schema aligned)
        for tbl in [target_table_us, target_table_ind, target_table_can, target_table_gbp]:
            conn.execute(text(f"""
                CREATE TABLE IF NOT EXISTS {tbl} (
                    id SERIAL PRIMARY KEY,
                    product_name TEXT,
                    sku TEXT,
                    quantity INTEGER,
                    return_quantity INTEGER,
                    total_quantity INTEGER,
                    product_sales REAL,
                    product_sales_tax REAL,
                    postage_credits REAL,
                    gift_wrap_credits REAL,
                    giftwrap_credits_tax REAL,
                    promotional_rebates REAL,
                    promotional_rebates_tax REAL,
                    gross_sales REAL,
                    refund_sales REAL,
                    net_sales REAL,
                    cost_of_unit_sold REAL,

                    sales_tax_refund REAL,
                    sales_credit_refund REAL,
                    refund_rebate REAL,
                    marketplace_facilitator_tax REAL,
                    other_transaction_fees REAL,
                    selling_fees REAL,
                    refund_selling_fees REAL,
                    fba_fees REAL,
                    total REAL,
                    net_taxes REAL,
                    digital_transaction_tax REAL,
                    net_credits REAL,
                    profit REAL,
                    profit_percentage REAL,
                    lost_total REAL,
                    visible_ads REAL,
                    dealsvouchar_ads REAL,
                    advertising_total REAL,
                    platformfeenew REAL,
                    platform_fee_inventory_storage REAL,
                    platform_fee REAL,
                    cm2_profit REAL,
                    cm2_profit_percentage REAL,
                    acos REAL,
                    rembursement_fee REAL,
                    
                    reimbursement_vs_sales REAL,
                    
                    cm2_margins REAL,
                    rembursment_vs_cm2_margins REAL,
                    misc_transaction REAL,
                    promotional_rebates_percentage REAL,
                    tex_and_credits REAL,
                    price_in_gbp REAL,
                    shipping_credits_tax REAL,
                    other REAL,
                    amazon_fee REAL,
                    profit_mix REAL,
                    unit_wise_profitability REAL,
                    asp REAL,
                    sales_mix REAL,
                    sales_mix_analysis TEXT,
                    shipping_credits REAL,
                    shipment_charges REAL,
                    errorstatus TEXT,
                    answer REAL,
                    difference REAL,
                    month TEXT,
                    year TEXT,
                    country TEXT,
                    user_id INTEGER
                )
            """))

        # Replace month table; append to rolling tables
        conn.execute(
            text(f"DELETE FROM {target_table2} WHERE month = :month AND year = :year AND user_id = :user_id"),
            {"month": month, "year": year, "user_id": user_id}
        )
        try:
            conn.commit()
        except Exception as _:
            pass  # compatibility across SA versions

        # mapping: dest_country_value, dataframe, target_table
        conversion_sets = [
            ("us",    df_usd, target_table_us),
            ("india", df_ind, target_table_ind),
            ("canada",df_can, target_table_can),
            ("gbp",   df_gbp, target_table_gbp),
        ]

        for dest_country, df_conv, tbl in conversion_sets:
            df_conv["country"] = dest_country

            conn.execute(text(f"""
                DELETE FROM {tbl}
                WHERE month = :month AND year = :year AND country = :country AND user_id = :user_id
            """), {"month": month, "year": year, "country": dest_country, "user_id": user_id})
            try:
                conn.commit()
            except Exception:
                pass

            # ✅ only DB-defined columns insert honge
            df_conv = sanitize_for_db(df_conv)  # <-- IMPORTANT
            safe_to_sql(df_conv, tbl, conn, if_exists="append", index=False, method="multi", chunksize=100)
            # ==========================================================

        # Final safety: ensure shipping cols exist in both frames before write
        for col in ["shipping_credits", "shipment_charges"]:
            if col not in sku_grouped.columns:
                sku_grouped[col] = 0.0
            if col not in df_usd.columns:
                df_usd[col] = 0.0

        # ================== IMPORTANT CHANGE START ==================
        # Table 1 (monthly) -> original signs
        df_month = sku_grouped.copy()

        # Table 2 (rolling) -> selling_fees & fba_fees always positive
        df_roll = sku_grouped.copy()

        for col in ["selling_fees", "fba_fees"]:
            if col in df_roll.columns:
                df_roll[col] = df_roll[col].abs()

        
        # ================== IMPORTANT CHANGE END ==================

        # Insert data into the respective tables
        sanitize_for_db(df_month)
        sanitize_for_db(df_roll)
        
       

        for df_conv in [df_usd, df_ind, df_can, df_gbp]:
            sanitize_for_db(df_conv)

        sanitize_for_db(sku_grouped)

        # NOTE: if_exists="replace" will DROP/CREATE based on df columns.
# If you want strict DB schema, DON'T use replace. Prefer delete+append.
        safe_to_sql(df_month, target_table, conn, if_exists="append", index=False, method="multi", chunksize=100)

        # ✅ Only insert columns that exist in target_table_nse (skuwisemonthly_{user_id}_{country}_{month}{year})
        NSE_COLS = [
            "sku",
            "product_name",

            "quantity",
            "return_quantity",
            "total_quantity",

            "asp",
            "gross_sales",
            "refund_sales",
            "tex_and_credits",

            "net_sales",
            "promotional_rebates",
            "promotional_rebates_percentage",

            "cost_of_unit_sold",
            "selling_fees",
            "fba_fees",
            "amazon_fee",

            "net_taxes",
            "net_credits",

            "misc_transaction",
            "other_transaction_fees",

            "profit",
            "unit_wise_profitability",
            "profit_percentage",

            "visible_ads",
            "dealsvouchar_ads",
            "advertising_total",

            "platformfeenew",
            "platform_fee",  # ✅ NEW (as per your desired schema)
            "platform_fee_inventory_storage",

            "cm2_profit",
            "cm2_profit_percentage",
            "acos",

            "rembursement_fee",
            "rembursment_vs_cm2_margins",
            "reimbursement_vs_sales",
            "lost_total",
            "sales_mix",
            "profit_mix",

            "user_id"
        ]




        # Ensure all required columns exist in df_month (fill defaults if missing)
        for c in NSE_COLS:
            if c not in df_month.columns:
                # text columns default ''
                if c in ("sku", "month", "year", "country", "cm1_profit", "product_name"):
                    df_month[c] = ""
                else:
                    df_month[c] = 0

        # Create df with only defined columns (order exactly as schema)
        df_month_nse = df_month[NSE_COLS].copy()

        # Optional: ensure numeric columns are numeric (avoid object inserts)
        NUMERIC_NSE = [c for c in NSE_COLS if c not in ("sku", "month", "year", "country", "cm1_profit", "product_name")]
        df_month_nse[NUMERIC_NSE] = df_month_nse[NUMERIC_NSE].apply(pd.to_numeric, errors="coerce").fillna(0)

        safe_to_sql(df_month_nse, target_table_nse, conn, if_exists="append", index=False, method="multi", chunksize=100)


        sanitize_for_db(df_month)
        sanitize_for_db(df_roll)
        
        

        for df_conv in [df_usd, df_ind, df_can, df_gbp]:
            sanitize_for_db(df_conv)

        sanitize_for_db(sku_grouped)

        safe_to_sql(df_roll, target_table2, conn, if_exists="append", index=False, method="multi", chunksize=100)


                # ========= NEW: USD per-country tables =========
        # Monthly USD data (same structure, but values in USD)
        df_month_usd = df_usd.copy()

        # Rolling USD data (selling_fees & fba_fees positive just like df_roll)
        df_roll_usd = df_usd.copy()
        for col in ["selling_fees", "fba_fees"]:
            if col in df_roll_usd.columns:
                df_roll_usd[col] = df_roll_usd[col].abs()

        # 1) Monthly USD table -> overwrite (per month)
        sanitize_for_db(df_month)
        sanitize_for_db(df_roll)
        sanitize_for_db(df_month_usd)
        sanitize_for_db(df_roll_usd)

        for df_conv in [df_usd, df_ind, df_can, df_gbp]:
            sanitize_for_db(df_conv)

        sanitize_for_db(sku_grouped)

        safe_to_sql(df_month_usd, target_table_usd_month, conn, if_exists="replace", index=False, method="multi", chunksize=100)

        # 2) Rolling USD table -> delete old rows for this month+year+country+user, then append
        conn.execute(
            text(f"""
                DELETE FROM {target_table_usd_roll}
                WHERE month   = :month
                  AND year    = :year
                  AND user_id = :user_id
            """),
            {"month": month, "year": year, "user_id": user_id}
        )
        try:
            conn.commit()
        except Exception:
            pass

        sanitize_for_db(df_month)
        sanitize_for_db(df_roll)
        sanitize_for_db(df_month_usd)
        sanitize_for_db(df_roll_usd)

        for df_conv in [df_usd, df_ind, df_can, df_gbp]:
            sanitize_for_db(df_conv)

        sanitize_for_db(sku_grouped)


        safe_to_sql(df_roll_usd, target_table_usd_roll, conn, if_exists="append", index=False, method="multi", chunksize=100)
        # ========= NEW USD tables end =========


        from app.models.user_models import UploadHistory
        from sqlalchemy.orm import sessionmaker

        Session = sessionmaker(bind=engine)
        session = Session()

        def convert_value(val):
            import numpy as np
            if isinstance(val, (np.int64, np.int32)):
                return int(val)
            elif isinstance(val, (np.float64, np.float32)):
                return float(val)
            return val

        try:
            # Sirf UK ke liye uk_usd upload history chahiye
            if country.lower() == "uk":
                # df_usd me TOTAL row dhundo (ye already USD converted hai)
                total_row_usd = df_usd[df_usd["sku"].astype(str).str.lower() == "total"].iloc[0]

                total_sales_usd            = convert_value(total_row_usd.get("net_sales", 0))
                total_profit_usd           = convert_value(total_row_usd.get("profit", 0))
                fba_fees_usd               = convert_value(total_row_usd.get("fba_fees", 0))
                platform_fee_val_usd       = convert_value(total_row_usd.get("platform_fee", 0))
                rembursement_fee_val_usd   = convert_value(total_row_usd.get("rembursement_fee", 0))
                cm2_profit_val_usd         = convert_value(total_row_usd.get("cm2_profit", 0))
                cm2_margins_val_usd        = convert_value(total_row_usd.get("cm2_margins", 0))
                acos_val_usd               = convert_value(total_row_usd.get("acos", 0))
                remb_vs_cm2_margins_usd    = convert_value(total_row_usd.get("rembursment_vs_cm2_margins", 0))
                advertising_total_val_usd  = convert_value(total_row_usd.get("advertising_total", 0))
                reimbursement_vs_sales_usd = convert_value(total_row_usd.get("reimbursement_vs_sales", 0))
                unit_sold_usd              = convert_value(total_row_usd.get("quantity", 0))
                total_cous_usd             = convert_value(total_row_usd.get("cost_of_unit_sold", 0))
                total_amazon_fee_val_usd   = convert_value(total_row_usd.get("amazon_fee", 0))
                total_product_sales_usd    = convert_value(total_row_usd.get("product_sales", 0))
                total_credits_usd          = convert_value(total_row_usd.get("net_credits", 0))
                total_tax_usd              = convert_value(total_row_usd.get("net_taxes", 0))

                total_expense_usd  = total_amazon_fee_val_usd + abs(platform_fee_val_usd)
                otherwplatform_usd = abs(platform_fee_val_usd)
                taxncredit_usd     = total_tax_usd  + abs(total_credits_usd)

                logical_country = "uk_usd"   # UploadHistory me ye naam se jayega

                # Purani entry delete karo (same user, same month/year, same logical country)
                existing_entry = session.query(UploadHistory).filter_by(
                    user_id=user_id,
                    country=logical_country,
                    year=str(year),
                    month=str(month)
                ).first()

                if existing_entry:
                    session.delete(existing_entry)
                    session.commit()

                upload_history_entry = UploadHistory(
                    user_id=int(user_id),
                    year=str(year),
                    month=str(month),
                    country=logical_country,      # 🔥 yahan "uk_usd"
                    file_name=None,
                    sales_chart_img=None,
                    expense_chart_img=None,
                    qtd_pie_chart=None,
                    ytd_pie_chart=None,
                    profit_chart_img=None,
                    total_sales=total_sales_usd,
                    total_profit=total_profit_usd,
                    otherwplatform=otherwplatform_usd,
                    taxncredit=taxncredit_usd,
                    total_expense=total_expense_usd,
                    total_fba_fees=fba_fees_usd,
                    platform_fee=platform_fee_val_usd,
                    rembursement_fee=rembursement_fee_val_usd,
                    cm2_profit=cm2_profit_val_usd,
                    cm2_margins=cm2_margins_val_usd,
                    acos=acos_val_usd,
                    rembursment_vs_cm2_margins=remb_vs_cm2_margins_usd,
                    advertising_total=advertising_total_val_usd,
                    reimbursement_vs_sales=reimbursement_vs_sales_usd,
                    unit_sold=unit_sold_usd,
                    total_cous=total_cous_usd,
                    total_amazon_fee=total_amazon_fee_val_usd,
                    pnl_email_sent=False,
                    total_product_sales=total_product_sales_usd
                )

                session.add(upload_history_entry)
                session.commit()

            else:
                print("Upload history for uk_usd skipped (country is not UK).")

        except Exception as e:
            print(f"Failed to save upload history for uk_usd: {e}")
            session.rollback()
        finally:
            session.close()

        # df_conv.to_sql(tbl, conn, if_exists="append", index=False, method="multi", chunksize=100)

        # Fill NaNs for any object/numeric leftovers (post-write safety if reused)
        for col in sku_grouped.columns:
            if sku_grouped[col].dtype == 'object':
                sku_grouped[col] = sku_grouped[col].fillna('')
            elif pd.api.types.is_numeric_dtype(sku_grouped[col]):
                sku_grouped[col] = sku_grouped[col].fillna(0)

        try:
            conn.commit()
        except Exception as _:
            pass

        # ---------------- FIX: total_quantity variable was never defined ----------------
        total_quantity = 0
        if "total_quantity" in sku_grouped.columns:
            try:
                total_row_qty = sku_grouped.loc[
                    sku_grouped["sku"].astype(str).str.strip().str.lower() == "total",
                    "total_quantity"
                ]
                if not total_row_qty.empty:
                    total_quantity = int(pd.to_numeric(total_row_qty.iloc[0], errors="coerce") or 0)
            except Exception:
                total_quantity = int(pd.to_numeric(sku_grouped["total_quantity"], errors="coerce").fillna(0).sum())
        # -------------------------------------------------------------------------------


        return (total_cous, total_amazon_fee, cm2_profit, abs(rembursement_fee), abs(platform_fee),
                total_expense, total_profit_final, total_fba_fees, total_advertising, sum_row["tex_and_credits"],
                reimbursement_vs_sales, cm2_margins, acos, rembursment_vs_cm2_margins, total_sales, total_quantity, total_product_sales)

    except Exception as e:
        print(f"Error processing SKU-wise data: {e}")
        raise
    finally:
        try:
            conn.close()
        except Exception:
            pass
        print("[process_skuwise_data] END")


def process_quarterly_skuwise_data(user_id, country, month, year, q, db_url):
    engine = create_engine(db_url)
    conn = engine.connect()
    country = "uk"  

    try:
        # Define quarter months (same logic)
        quarter_months = {
            "quarter1": ["january", "february", "march"],
            "quarter2": ["april", "may", "june"],
            "quarter3": ["july", "august", "september"],
            "quarter4": ["october", "november", "december"]
        }

        month = month.lower()
        quarter_key = None
        months_for_quarter = None

        # ⬇️ yahi logic tha, sirf quarter_key / months_for_quarter store kar liya
        for q_name, months in quarter_months.items():
            if month in months:
                quarter_key = q_name            # e.g. "quarter2"
                months_for_quarter = months     # e.g. ["april","may","june"]
                break
        else:
            print("Invalid month provided.")
            return

        # 4 source tables + unka "country" name jo quarterly table me use hoga
        config_list = [
            (f"skuwisemonthly_{user_id}_{country}",      "uk"),       # existing (USD)
            (f"skuwisemonthly_{user_id}_{country}_usd",  "uk_usd"),   # INR
              # GBP base
        ]

        # ---------- LOOP: same logic har currency table ke liye ----------
        for source_table, logical_country in config_list:
            
            quarter_table = f"{quarter_key}_{user_id}_{logical_country}_{year}_table"

            # Get only available months from THIS source table
            month_params = {f"m{i}": m for i, m in enumerate(months_for_quarter)}
            placeholders = ', '.join(f":m{i}" for i in range(len(months_for_quarter)))
            available_months_query = text(f"""
                SELECT DISTINCT LOWER(month) AS month
                FROM {source_table}
                WHERE LOWER(month) IN ({placeholders}) AND year = :year
            """)
            result = conn.execute(available_months_query, {**month_params, "year": year})
            available_months_df = pd.DataFrame(result.fetchall(), columns=["month"])
            selected_months = available_months_df["month"].tolist()

            if not selected_months:
                print(f"No available months found in {source_table} for quarter {quarter_key}.")
                continue

            # Read full data for selected months from THIS source
            placeholders = ', '.join(['%s'] * len(selected_months))
            query = f"""
                SELECT "sku",
                "product_name",
                "quantity",
                "return_quantity",
                "total_quantity",
                "asp",
                "gross_sales",
                "refund_sales",
                "tex_and_credits",
                "net_sales",
                "promotional_rebates",
                "promotional_rebates_percentage",
                "cost_of_unit_sold",
                "selling_fees",
                "fba_fees",
                "amazon_fee",
                "net_taxes",
                "net_credits",
                "misc_transaction",
                "other_transaction_fees",
                "profit",
                "unit_wise_profitability",
                "profit_percentage",
                "visible_ads",
                "dealsvouchar_ads",
                "advertising_total",
                "platformfeenew",
                "platform_fee",
                "platform_fee_inventory_storage",
                "cm2_profit",
                "cm2_profit_percentage",
                "acos",
                "rembursement_fee",
                "rembursment_vs_cm2_margins",
                "reimbursement_vs_sales",
                "lost_total",
                "sales_mix",
                "profit_mix",
                "user_id"

                FROM {source_table}
                WHERE LOWER(month) IN ({placeholders}) AND year = %s
            """
            df = pd.read_sql(query, conn, params=tuple(selected_months + [year]))

            if df.empty:
                print(f"No data for selected months in {source_table}.")
                continue

            # ---------- AGGREGATION (same as tumhara) ----------
            sku_grouped = df.groupby('product_name').agg({
                "sku": "first", 
                "quantity": "sum",
                    "return_quantity": "sum",
                    "total_quantity": "sum",
                    "asp": "mean",
                    "gross_sales": "sum",
                    "refund_sales": "sum",
                    "tex_and_credits": "sum",
                    "net_sales": "sum",
                    "promotional_rebates": "sum",
                    "promotional_rebates_percentage": "mean",
                    "cost_of_unit_sold": "sum",
                    "selling_fees": "sum",
                    "fba_fees": "sum",
                    "amazon_fee": "sum",
                    "net_taxes": "sum",
                    "net_credits": "sum",
                    "misc_transaction": "sum",
                    "other_transaction_fees": "sum",
                    "profit": "sum",
                    "unit_wise_profitability": "mean",
                    "profit_percentage": "mean",
                    "visible_ads": "sum",
                    "dealsvouchar_ads": "sum",
                    "advertising_total": "sum",
                    "platformfeenew": "sum",
                    "platform_fee": "sum",
                    "platform_fee_inventory_storage": "sum",
                    "cm2_profit": "sum",
                    "cm2_profit_percentage": "mean",
                    "acos": "mean",
                    "rembursement_fee": "sum",
                    "rembursment_vs_cm2_margins": "mean",
                    "reimbursement_vs_sales": "mean",
                    "lost_total": "sum",
                    "sales_mix": "mean",
                    "profit_mix": "mean",
                    "user_id": "first"
            }).reset_index()




            sku_grouped["product_name"] = sku_grouped["product_name"].astype(str).str.strip()

            sku_grouped["cm2_margins"] = sku_grouped.apply(
                lambda row: (row["cm2_profit"] / row["net_sales"]) * 100 if row["net_sales"] != 0 else 0,
                axis=1
            )
            sku_grouped["acos"] = sku_grouped.apply(
                lambda row: (row["advertising_total"] / row["net_sales"]) * 100 if row["net_sales"] != 0 else 0,
                axis=1
            )
            sku_grouped["rembursment_vs_cm2_margins"] = sku_grouped.apply(
                lambda row: (row["rembursement_fee"] / row["cm2_profit"]) * 100 if row["cm2_profit"] != 0 else 0,
                axis=1
            )
            sku_grouped["reimbursement_vs_sales"] = sku_grouped.apply(
                lambda row: (row["rembursement_fee"] / row["net_sales"]) * 100 if row["net_sales"] != 0 else 0,
                axis=1
            )
            sku_grouped["profit_percentage"] = sku_grouped.apply(
                lambda row: (row["profit"] / row["net_sales"]) * 100 if row["net_sales"] != 0 else 0,
                axis=1
            )

            sku_grouped["asp"] = sku_grouped.apply(
                lambda row: (row["net_sales"] / row["quantity"])  if row["quantity"] != 0 else 0,
                axis=1
            )
            sku_grouped["unit_wise_profitability"] = sku_grouped.apply(
                lambda row: (row["profit"] / row["quantity"])  if row["quantity"] != 0 else 0,
                axis=1
            )

            temp = sku_grouped[sku_grouped["product_name"].str.lower() != "total"]

            total_sales = abs(temp["net_sales"].sum())
            total_profit = abs(temp["profit"].sum())


            sku_grouped["profit_mix"] = sku_grouped.apply(
                lambda row: (row["profit"] / total_profit) * 100 if total_profit != 0 else 0,
                axis=1
            )

            sku_grouped["sales_mix"] = sku_grouped.apply(
                lambda row: (row["net_sales"] / total_sales) * 100 if total_sales != 0 else 0,
                axis=1
            )

            total_row = sku_grouped[sku_grouped["product_name"].str.lower() == "total"]
            other_rows = sku_grouped[sku_grouped["product_name"].str.lower() != "total"]
            other_rows = other_rows.sort_values(by="profit", ascending=False)
            sku_grouped = pd.concat([other_rows, total_row], ignore_index=True)

            # ---------- Create + Insert into quarterly table for this currency ----------
            with engine.begin() as conn_inner:
                conn_inner.execute(text(f"DROP TABLE IF EXISTS {quarter_table}"))

                conn_inner.execute(text(f"""
                    CREATE TABLE IF NOT EXISTS {quarter_table} (
                        id SERIAL PRIMARY KEY,

                        sku TEXT,
                        product_name TEXT,

                        quantity INTEGER,
                        return_quantity INTEGER,
                        total_quantity INTEGER,

                        asp DOUBLE PRECISION,
                        gross_sales DOUBLE PRECISION,
                        refund_sales DOUBLE PRECISION,
                        tex_and_credits DOUBLE PRECISION,

                        net_sales DOUBLE PRECISION,
                        promotional_rebates DOUBLE PRECISION,
                        promotional_rebates_percentage DOUBLE PRECISION,

                        cost_of_unit_sold DOUBLE PRECISION,
                        selling_fees DOUBLE PRECISION,
                        fba_fees DOUBLE PRECISION,
                        amazon_fee DOUBLE PRECISION,

                        net_taxes DOUBLE PRECISION,
                        net_credits DOUBLE PRECISION,

                        misc_transaction DOUBLE PRECISION,
                        other_transaction_fees DOUBLE PRECISION,

                        profit DOUBLE PRECISION,
                        unit_wise_profitability DOUBLE PRECISION,
                        profit_percentage DOUBLE PRECISION,

                        visible_ads DOUBLE PRECISION,
                        dealsvouchar_ads DOUBLE PRECISION,
                        advertising_total DOUBLE PRECISION,

                        platformfeenew DOUBLE PRECISION,
                        platform_fee DOUBLE PRECISION,
                        platform_fee_inventory_storage DOUBLE PRECISION,

                        cm2_profit DOUBLE PRECISION,
                        cm2_profit_percentage DOUBLE PRECISION,
                        acos DOUBLE PRECISION,

                        rembursement_fee DOUBLE PRECISION,
                        rembursment_vs_cm2_margins DOUBLE PRECISION,
                        reimbursement_vs_sales DOUBLE PRECISION,

                        profit_mix DOUBLE PRECISION,
                        sales_mix DOUBLE PRECISION,
                        lost_total DOUBLE PRECISION,

                        user_id INTEGER
                    )
                """))

                sku_grouped.columns = sku_grouped.columns.str.lower()
                

                sku_grouped.to_sql(quarter_table, conn_inner, if_exists="replace", index=False)

    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        conn.close()


def process_yearly_skuwise_data(user_id, country, year):

    from sqlalchemy import create_engine, text
    import pandas as pd
    import numpy as np
    # Connect to PostgreSQL database
    engine = create_engine(db_url)
    conn = engine.connect()
    config_list = [
        (f"skuwisemonthly_{user_id}_{country}",      "uk"),       # USD (pehle se)
        (f"skuwisemonthly_{user_id}_{country}_usd",  "uk_usd"),   # INR
        
    ]
 

    
    try:
        for source_table, logical_country in config_list:
            quarter_table = f"skuwiseyearly_{user_id}_{logical_country}_{year}_table"

        # Fetch yearly data - using parameterized query for PostgreSQL
            yearly_query = f"""
                SELECT "sku",
                "product_name",
                "quantity",
                "return_quantity",
                "total_quantity",
                "asp",
                "gross_sales",
                "refund_sales",
                "tex_and_credits",
                "net_sales",
                "promotional_rebates",
                "promotional_rebates_percentage",
                "cost_of_unit_sold",
                "selling_fees",
                "fba_fees",
                "amazon_fee",
                "net_taxes",
                "net_credits",
                "misc_transaction",
                "other_transaction_fees",
                "profit",
                "unit_wise_profitability",
                "profit_percentage",
                "visible_ads",
                "dealsvouchar_ads",
                "advertising_total",
                "platformfeenew",
                "platform_fee",
                "platform_fee_inventory_storage",
                "cm2_profit",
                "cm2_profit_percentage",
                "acos",
                "rembursement_fee",
                "rembursment_vs_cm2_margins",
                "reimbursement_vs_sales",
                "lost_total",
                "sales_mix",
                "profit_mix",
                "user_id"

                FROM {source_table}
                WHERE "year" = '{year}'
            """
        
        # Execute query directly without parameters argument
            try:
                df = pd.read_sql(yearly_query, conn)
            except Exception as e:
                print(f"❌ Failed to read from {source_table}: {e}")
                continue

            if df.empty:
                print(f"⚠️ No yearly data found for user={user_id}, country={logical_country}, year={year} in {source_table}")
                continue
        
    
        # Group by SKU for aggregation
            sku_grouped = df.groupby('product_name').agg({
                    "sku": "first", 
                
                    "quantity": "sum",
                    "return_quantity": "sum",
                    "total_quantity": "sum",
                    "asp": "mean",
                    "gross_sales": "sum",
                    "refund_sales": "sum",
                    "tex_and_credits": "sum",
                    "net_sales": "sum",
                    "promotional_rebates": "sum",
                    "promotional_rebates_percentage": "mean",
                    "cost_of_unit_sold": "sum",
                    "selling_fees": "sum",
                    "fba_fees": "sum",
                    "amazon_fee": "sum",
                    "net_taxes": "sum",
                    "net_credits": "sum",
                    "misc_transaction": "sum",
                    "other_transaction_fees": "sum",
                    "profit": "sum",
                    "unit_wise_profitability": "mean",
                    "profit_percentage": "mean",
                    "visible_ads": "sum",
                    "dealsvouchar_ads": "sum",
                    "advertising_total": "sum",
                    "platformfeenew": "sum",
                    "platform_fee": "sum",
                    "platform_fee_inventory_storage": "sum",
                    "cm2_profit": "sum",
                    "cm2_profit_percentage": "mean",
                    "acos": "mean",
                    "rembursement_fee": "sum",
                    "rembursment_vs_cm2_margins": "mean",
                    "reimbursement_vs_sales": "mean",
                    "lost_total": "sum",
                    "sales_mix": "mean",
                    "profit_mix": "mean",
                    "user_id": "first", # or "sum" if you want to repeat user_id for each group
            }).reset_index()
            sku_grouped["product_name"] = sku_grouped["product_name"].astype(str).str.strip()
            sku_grouped["cm2_margins"] = sku_grouped.apply(
                lambda row: (row["cm2_profit"] / row["net_sales"]) * 100 if row["net_sales"] != 0 else 0,
                axis=1
            )
            sku_grouped["acos"] = sku_grouped.apply(
                lambda row: (row["advertising_total"] / row["net_sales"]) * 100 if row["net_sales"] != 0 else 0,
                axis=1
            )
            sku_grouped["rembursment_vs_cm2_margins"] = sku_grouped.apply(
                lambda row: (row["rembursement_fee"] / row["cm2_profit"]) * 100 if row["cm2_profit"] != 0 else 0,
                axis=1
            )
            sku_grouped["reimbursement_vs_sales"] = sku_grouped.apply(
                lambda row: (row["rembursement_fee"] / row["net_sales"]) * 100 if row["net_sales"] != 0 else 0,
                axis=1
            )

            sku_grouped["profit_percentage"] = sku_grouped.apply(
                lambda row: (row["profit"] / row["net_sales"]) * 100 if row["net_sales"] != 0 else 0,
                axis=1
            )

            sku_grouped["asp"] = sku_grouped.apply(
                lambda row: (row["net_sales"] / row["quantity"])  if row["quantity"] != 0 else 0,
                axis=1
            )
            sku_grouped["unit_wise_profitability"] = sku_grouped.apply(
                lambda row: (row["profit"] / row["quantity"])  if row["quantity"] != 0 else 0,
                axis=1
            )

            temp = sku_grouped[sku_grouped["product_name"].str.lower() != "total"]

            total_sales = abs(temp["net_sales"].sum())
            total_profit = abs(temp["profit"].sum())


            sku_grouped["profit_mix"] = sku_grouped.apply(
                lambda row: (row["profit"] / total_profit) * 100 if total_profit != 0 else 0,
                axis=1
            )

            sku_grouped["sales_mix"] = sku_grouped.apply(
                lambda row: (row["net_sales"] / total_sales) * 100 if total_sales != 0 else 0,
                axis=1
            )
            total_row = sku_grouped[sku_grouped["product_name"].str.lower() == "total"]
            other_rows = sku_grouped[sku_grouped["product_name"].str.lower() != "total"]

    # Sort other rows by profit in ascending order
            other_rows = other_rows.sort_values(by="profit", ascending=False)

    # Concatenate the sorted rows with the total row at the end
            sku_grouped = pd.concat([other_rows, total_row], ignore_index=True)

            # Drop existing table if it exists
            conn.execute(text(f"DROP TABLE IF EXISTS {quarter_table}"))

            # Create table with proper PostgreSQL syntax
            create_table_query = f"""
                CREATE TABLE IF NOT EXISTS {quarter_table} (
                    id SERIAL PRIMARY KEY,

                    sku TEXT,
                    product_name TEXT,

                    quantity INTEGER,
                    return_quantity INTEGER,
                    total_quantity INTEGER,

                    asp DOUBLE PRECISION,
                    gross_sales DOUBLE PRECISION,
                    refund_sales DOUBLE PRECISION,
                    tex_and_credits DOUBLE PRECISION,

                    net_sales DOUBLE PRECISION,
                    promotional_rebates DOUBLE PRECISION,
                    promotional_rebates_percentage DOUBLE PRECISION,

                    cost_of_unit_sold DOUBLE PRECISION,
                    selling_fees DOUBLE PRECISION,
                    fba_fees DOUBLE PRECISION,
                    amazon_fee DOUBLE PRECISION,

                    net_taxes DOUBLE PRECISION,
                    net_credits DOUBLE PRECISION,

                    misc_transaction DOUBLE PRECISION,
                    other_transaction_fees DOUBLE PRECISION,

                    profit DOUBLE PRECISION,
                    unit_wise_profitability DOUBLE PRECISION,
                    profit_percentage DOUBLE PRECISION,

                    visible_ads DOUBLE PRECISION,
                    dealsvouchar_ads DOUBLE PRECISION,
                    advertising_total DOUBLE PRECISION,

                    platformfeenew DOUBLE PRECISION,
                    platform_fee DOUBLE PRECISION,
                    platform_fee_inventory_storage DOUBLE PRECISION,

                    cm2_profit DOUBLE PRECISION,
                    cm2_profit_percentage DOUBLE PRECISION,
                    acos DOUBLE PRECISION,

                    rembursement_fee DOUBLE PRECISION,
                    rembursment_vs_cm2_margins DOUBLE PRECISION,
                    reimbursement_vs_sales DOUBLE PRECISION,

                    profit_mix DOUBLE PRECISION,
                    sales_mix DOUBLE PRECISION,
                    lost_total DOUBLE PRECISION,

                    user_id INTEGER
                )
            """
            conn.execute(text(create_table_query))
            
            # Ensure column names match the database (PostgreSQL is case-sensitive)
            sku_grouped.columns = [col.lower() for col in sku_grouped.columns]
            
            # Use to_sql with correct parameters for PostgreSQL
            

            sku_grouped.to_sql(quarter_table, conn, if_exists="replace", index=False, 
                            schema="public", method="multi", chunksize=1000)
            
            conn.commit()

    except Exception as e:
        print(f"Error processing yearly SKU-wise data: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()


