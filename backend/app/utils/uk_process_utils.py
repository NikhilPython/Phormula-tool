from flask import  jsonify
from sqlalchemy import create_engine,  inspect
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text
from config import Config
SECRET_KEY = Config.SECRET_KEY
UPLOAD_FOLDER = Config.UPLOAD_FOLDER
from config import basedir
import os
import pandas as pd
import numpy as np 
import base64
from app.models.user_models import User, CountryProfile
from sqlalchemy import MetaData, Table
from datetime import datetime, timedelta
from flask_mail import Message
from flask import current_app
from app import mail
from calendar import month_name
from pmdarima import auto_arima
from dotenv import load_dotenv
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor, as_completed
from app.utils.formulas_utils import uk_sales, uk_tax, uk_credits, uk_amazon_fee, uk_profit,uk_platform_fee, uk_advertising
import warnings

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




def get_previous_month_year(month, year):
    """Calculate the previous month and year."""

    print(f" Previous_Month1: {month}, Previous Year: {year}")
    year = int(year)
    prev_month_num = MONTHS_MAP[month] - 1
    if prev_month_num == 0:
        prev_month_num = 12
        year -= 1
    prev_month = MONTHS_REVERSE_MAP[prev_month_num]

    print(f"Previous Month: {prev_month}, Previous Year: {year}")
   

    return prev_month, year

def process_skuwise_data(user_id, country, month, year):
    print(f"[process_skuwise_data] START -> user_id={user_id}, country={country}, month={month}, year={year}")
    from sqlalchemy import create_engine, text
    import numpy as np
    import pandas as pd
    import re  # needed by uk_advertising helper if it uses re.escape

    print(f" Month: {month}, Previous Year: {year}")
    engine = create_engine(db_url)
    engine1 = create_engine(db_url1)
    conn = engine.connect()

    source_table = f"user_{user_id}_{country}_{month}{year}_data"
    target_table = f"skuwisemonthly_{user_id}_{country}_{month}{year}"
    target_table2 = f"skuwisemonthly_{user_id}_{country}"
    target_table3 = f"skuwisemonthly_{user_id}"
    target_table_us   = f"skuwisemonthly_{user_id}"        # EXISTING US wala (same)
    target_table_ind  = f"skuwisemonthlyind_{user_id}"     # NEW – India
    target_table_can  = f"skuwisemonthlycan_{user_id}"     # NEW – Canada
    target_table_gbp  = f"skuwisemonthlygbp_{user_id}" 
        # NEW: per-country USD tables
    target_table_usd_month = f"skuwisemonthly_{user_id}_{country}_usd_{month}{year}"
    target_table_usd_roll  = f"skuwisemonthly_{user_id}_{country}_usd"
    # NEW – GBP base

    prev_month, prev_year = get_previous_month_year(month, year)
    prev_table = f"skuwisemonthly_{user_id}_{country}_{prev_month}{prev_year}"
    print(f"Previous Month: {prev_month}, Previous Year: {prev_year}")
    print(f"prev_table: {prev_table}")
    print('hello Fee')

    try:
        # Fetch main table data
        query = f"SELECT * FROM {source_table}"
        df = pd.read_sql(query, conn)
        if df.empty:
            print(f"No data found in {source_table}")
            return

        print("Main data loaded successfully")

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
        print(f"[prev_table exists?] -> {table_exists}")

        a = b = c = d = e = f = g = h = i = j = k = l = m = n = o = p = q = r = 0
        if table_exists:
            print(f"Previous month table {prev_table} exists. Fetching previous data...")
            # Load previous month full data
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

        print("hello Fee")

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
        transfer_df = df[type_str.eq("Transfer")]
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

        # ---------------------------------------------------------------------
        # Centralized platform fee & advertising using helpers
        # ---------------------------------------------------------------------
        platform_total, platform_by_sku, _ = uk_platform_fee(df)
        advertising_total_all, advertising_by_sku, _ = uk_advertising(df)

        print(f"Platform Fee: {platform_total}")
        print(f"Advertising Total: {advertising_total_all}")
        print(f"Reimbursement Fee: {rembursement_fee}")

        # SKU cleaning
        df = df[df["sku"].astype(str).str.strip() != "0"]
        df = df[df["sku"].notna() & (df["sku"].astype(str).str.strip() != "")]

        refund_fees = df[type_str.eq("Refund")].groupby("sku")["selling_fees"].sum().reset_index()
        refund_fees.rename(columns={"selling_fees": "refund_selling_fees"}, inplace=True)
        refund_fees["sku"] = refund_fees["sku"].astype(str).str.strip()
        df["sku"] = df["sku"].astype(str).str.strip()

        df["type_norm"] = type_str.str.lower()
        df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce").fillna(0)

        mask = df["type_norm"].isin(["order", "shipment"])
        quantity_df = df[mask].groupby("sku", as_index=False)["quantity"].sum()

        # Aggregate data SKU-wise (base columns)
        sku_grouped = df.groupby('sku').agg({
            "price_in_gbp": "mean", "product_sales": "sum", "promotional_rebates": "sum",
            "promotional_rebates_tax": "sum", "product_sales_tax": "sum",
            "selling_fees": "sum", "fba_fees": "sum", "other": "sum", "answer": "sum", "difference": "sum",
            "marketplace_facilitator_tax": "sum", "shipping_credits_tax": "sum",
            "giftwrap_credits_tax": "sum", "postage_credits": "sum", 
            "gift_wrap_credits": "sum", "cost_of_unit_sold": "sum",
            "total": "sum", "other_transaction_fees": "sum", "product_name": "first",
            "errorstatus": "first",
            **({"shipping_credits": "sum"} if "shipping_credits" in df.columns else {}),
            **({"shipment_charges": "sum"} if "shipment_charges" in df.columns else {}),
        }).reset_index()

        sku_grouped = sku_grouped.merge(df_prev, on="sku", how="left").fillna(0)
        print("Columns in df:", df.columns)

        sku_grouped["sku"] = sku_grouped["sku"].astype(str).str.strip()
        sku_grouped = sku_grouped.merge(refund_fees, on="sku", how="left")

        # Merge the filtered quantity data
        sku_grouped = sku_grouped.merge(quantity_df, on="sku", how="left")
        sku_grouped["quantity"] = sku_grouped["quantity"].fillna(0)

        # Total quantity
        total_quantity = sku_grouped["quantity"].sum()
        print(f"Total Quantity: {total_quantity}")

        # Refund fee adjustment
        print("Before Subtracting Refund Fees:")
        print(sku_grouped[["sku", "selling_fees", "refund_selling_fees"]])
        sku_grouped["selling_fees"] = pd.to_numeric(sku_grouped["selling_fees"], errors='coerce').fillna(0)
        sku_grouped["refund_selling_fees"] = pd.to_numeric(sku_grouped["refund_selling_fees"], errors='coerce').fillna(0)
        sku_grouped["selling_fees"] -= 2 * sku_grouped["refund_selling_fees"]
        print("After Subtracting Refund Fees:")
        print(sku_grouped[["sku", "selling_fees", "refund_selling_fees"]])

        # ---------------------------------------------------------------------
        # SHARED UK formulas for Net Sales / Net Taxes / Net Credits / Fees / Profit
        # ---------------------------------------------------------------------
        sales_total, sales_by_sku, _ = uk_sales(df)
        tax_total, tax_by_sku, _ = uk_tax(df)
        credits_total, credits_by_sku, _ = uk_credits(df)
        fee_total, fees_by_sku, _ = uk_amazon_fee(df)
        profit_total, profit_by_sku, _ = uk_profit(df)

        # Merge shared results into the working table with your expected column names
        if not sales_by_sku.empty:
            sku_grouped = sku_grouped.merge(
                sales_by_sku[["sku", "__metric__"]].rename(columns={"__metric__": "Net Sales"}),
                on="sku", how="left"
            )
        else:
            sku_grouped["Net Sales"] = 0.0

        if not tax_by_sku.empty:
            sku_grouped = sku_grouped.merge(
                tax_by_sku[["sku", "__metric__"]].rename(columns={"__metric__": "Net Taxes"}),
                on="sku", how="left"
            )
        else:
            sku_grouped["Net Taxes"] = 0.0

        if not credits_by_sku.empty:
            sku_grouped = sku_grouped.merge(
                credits_by_sku[["sku", "__metric__"]].rename(columns={"__metric__": "Net Credits"}),
                on="sku", how="left"
            )
        else:
            sku_grouped["Net Credits"] = 0.0

        if not fees_by_sku.empty:
            sku_grouped = sku_grouped.merge(
                fees_by_sku[["sku", "__metric__"]].rename(columns={"__metric__": "amazon_fee"}),
                on="sku", how="left"
            )
        else:
            sku_grouped["amazon_fee"] = 0.0

        if not profit_by_sku.empty:
            sku_grouped = sku_grouped.merge(
                profit_by_sku[["sku", "__metric__"]].rename(columns={"__metric__": "profit"}),
                on="sku", how="left"
            )
        else:
            sku_grouped["profit"] = 0.0

        # NEW: merge centralized platform/ad per-SKU into sku_grouped
        if not platform_by_sku.empty:
            sku_grouped = sku_grouped.merge(
                platform_by_sku[["sku", "__metric__"]].rename(columns={"__metric__": "platform_fee"}),
                on="sku", how="left"
            )
        else:
            sku_grouped["platform_fee"] = 0.0

        if not advertising_by_sku.empty:
            sku_grouped = sku_grouped.merge(
                advertising_by_sku[["sku", "__metric__"]].rename(columns={"__metric__": "advertising_total"}),
                on="sku", how="left"
            )
        else:
            sku_grouped["advertising_total"] = 0.0

        for _col in ["Net Sales", "Net Taxes", "Net Credits", "amazon_fee", "profit", "platform_fee", "advertising_total"]:
            if _col in sku_grouped.columns:
                sku_grouped[_col] = pd.to_numeric(sku_grouped[_col], errors="coerce").fillna(0.0)

        print("Columns in sku_grouped before profit calculation:", sku_grouped.columns)

        total_profit_final = sku_grouped["profit"].sum()
        print("Total Profit:", total_profit_final)

        sku_grouped["profit%"] = abs((sku_grouped["profit"] / abs(sku_grouped["Net Sales"])) * 100)
        sku_grouped["profit%"] = sku_grouped["profit%"].replace([float('inf'), -float('inf')], 0).fillna(0)

        print("Columns in sku_grouped after profit calculation:", sku_grouped.columns)
        print(sku_grouped[["sku", "profit"]].head())

        # Unit-wise profitability
        sku_grouped["profit"] = pd.to_numeric(sku_grouped["profit"], errors="coerce")
        sku_grouped["quantity"] = pd.to_numeric(sku_grouped["quantity"], errors="coerce")
        sku_grouped["unit_wise_profitability"] = (sku_grouped["profit"] / sku_grouped["quantity"]).replace([float("inf"), -float("inf")], 0).fillna(0)

        # Previous unit-wise profitability
        sku_grouped["previous_profit"] = pd.to_numeric(sku_grouped["previous_profit"], errors="coerce")
        sku_grouped["previous_quantity"] = pd.to_numeric(sku_grouped["previous_quantity"], errors="coerce")
        sku_grouped["previous_unit_wise_profitability"] = (sku_grouped["previous_profit"] / sku_grouped["previous_quantity"]).replace([float('inf'), -float('inf')], 0).fillna(0)

        # % change and growth buckets
        sku_grouped["unit_wise_profitability_percentage"] = (
            (sku_grouped["unit_wise_profitability"] - sku_grouped["previous_unit_wise_profitability"]) /
            sku_grouped["previous_unit_wise_profitability"]
        ) * 100
        sku_grouped["unit_wise_profitability_percentage"] = sku_grouped["unit_wise_profitability_percentage"].replace([float('inf'), -float('inf')], 0).fillna(0)
        sku_grouped["unit_wise_profitability_growth"] = np.select(
            [
                sku_grouped["unit_wise_profitability_percentage"] >= 5,
                sku_grouped["unit_wise_profitability_percentage"] > 0.5,
                sku_grouped["unit_wise_profitability_percentage"] < -0.5
            ],
            ["High Growth", "Low Growth", "Negative Growth"],
            default="No Growth"
        )

        # ASP & growth
        sku_grouped["asp"] = (sku_grouped["Net Sales"] / sku_grouped["quantity"]).replace([float('inf'), -float('inf')], 0).fillna(0)
        sku_grouped["previous_asp"] = (sku_grouped["previous_net_sales"] / sku_grouped["previous_quantity"]).replace([float('inf'), -float('inf')], 0).fillna(0)
        sku_grouped["asp_percentag"] = ((sku_grouped["asp"] - sku_grouped["previous_asp"]) / sku_grouped["previous_asp"]) * 100
        sku_grouped["asp_percentag"] = sku_grouped["asp_percentag"].replace([float('inf'), -float('inf')], 0).fillna(0)
        sku_grouped["asp_growth"] = np.select(
            [
                sku_grouped["asp_percentag"] >= 5,
                sku_grouped["asp_percentag"] > 0.5,
                sku_grouped["asp_percentag"] < -0.5
            ],
            ["High Growth", "Low Growth", "Negative Growth"],
            default="No Growth"
        )

        # Tax/Credit per unit
        sku_grouped["text_credit_change"] = ((sku_grouped["Net Taxes"] - sku_grouped["Net Credits"]) / sku_grouped["quantity"]).replace([float('inf'), -float('inf')], 0).fillna(0)
        sku_grouped["previous_text_credit_change"] = ((sku_grouped["previous_net_taxes"] - sku_grouped["previous_net_credits"]) / sku_grouped["previous_quantity"]).replace([float('inf'), -float('inf')], 0).fillna(0)

        # Profit change & growth
        sku_grouped["profit_change"] = ((sku_grouped["profit"] - sku_grouped["previous_profit"]) / sku_grouped["previous_profit"]) * 100
        sku_grouped["profit_change"] = sku_grouped["profit_change"].replace([float('inf'), -float('inf')], 0).fillna(0)
        sku_grouped["profit_growth"] = np.select(
            [
                sku_grouped["profit_change"] >= 5,
                sku_grouped["profit_change"] > 0.5,
                sku_grouped["profit_change"] < -0.5
            ],
            ["High Growth", "Low Growth", "Negative Growth"],
            default="No Growth"
        )

        # Units & growth
        sku_grouped["unit_increase"] = ((sku_grouped["quantity"] - sku_grouped["previous_quantity"]) / sku_grouped["previous_quantity"]) * 100
        sku_grouped["unit_increase"] = sku_grouped["unit_increase"].replace([float('inf'), -float('inf')], 0).fillna(0)
        sku_grouped["unit_growth"] = np.select(
            [
                sku_grouped["unit_increase"] >= 5,
                sku_grouped["unit_increase"] > 0.5,
                sku_grouped["unit_increase"] < -0.5
            ],
            ["High Growth", "Low Growth", "Negative Growth"],
            default="No Growth"
        )

        # Static descriptor fields
        sku_grouped["category"] = ""
        sku_grouped["positive"] = ""
        sku_grouped["improvements"] = ""
        sku_grouped["month"] = month
        sku_grouped["year"] = year
        sku_grouped["country"] = country
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
        print(f"amazon Fee as expense: {total_amazon_fee}")

        # Totals
        total_sales = abs(sku_grouped["Net Sales"].sum())
        total_profit = abs(sku_grouped["profit"].sum())
        total_Previous_profit = abs(sku_grouped["previous_profit"].sum())
        total_Previous_sales = abs(sku_grouped["previous_net_sales"].sum())

        print("error hai kya?1")
        sku_grouped["sales_mix"] = (sku_grouped["Net Sales"] / total_sales) * 100
        sku_grouped["sales_mix"] = sku_grouped["sales_mix"].replace([float('inf'), -float('inf')], 0).fillna(0)

        print("error hai kya?2")
        sku_grouped["profit_mix"] = (sku_grouped["profit"] / total_profit) * 100
        sku_grouped["profit_mix"] = sku_grouped["profit_mix"].replace([float('inf'), -float('inf')], 0).fillna(0)
        sku_grouped["previous_profit_mix"] = (sku_grouped["previous_profit"] / total_Previous_profit) * 100
        sku_grouped["previous_profit_mix"] = sku_grouped["previous_profit_mix"].replace([float('inf'), -float('inf')], 0).fillna(0)
        sku_grouped["profit_mix_percentage"] = ((sku_grouped["profit_mix"] - sku_grouped["previous_profit_mix"]) / sku_grouped["previous_profit_mix"]) * 100

        print("error hai kya?3")
        sku_grouped["profit_mix_percentage"] = sku_grouped["profit_mix_percentage"].replace([float('inf'), -float('inf')], 0).fillna(0)
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

        # Fee ratios
        sku_grouped["change_in_fee"] = (sku_grouped["amazon_fee"] / sku_grouped["Net Sales"]) * 100
        sku_grouped["change_in_fee"] = sku_grouped["change_in_fee"].replace([float('inf'), -float('inf')], 0).fillna(0)
        print("error hai kya?4")
        sku_grouped["previous_change_in_fee"] = (sku_grouped["previous_amazon_fee"] / sku_grouped["previous_net_sales"]) * 100
        sku_grouped["previous_change_in_fee"] = sku_grouped["previous_change_in_fee"].replace([float('inf'), -float('inf')], 0).fillna(0)
        sku_grouped["precentage_change_in_fee"] = sku_grouped["change_in_fee"] - sku_grouped["previous_change_in_fee"]
        sku_grouped["sales_mix_analysis"] = sku_grouped["sales_mix"].apply(lambda x: "High" if (x / 100) > 0.2 else "Low")

        print("error hai kya?5")
        sku_grouped["unit_wise_amazon_fee"] = ((sku_grouped["amazon_fee"] - sku_grouped["Net Taxes"]) / sku_grouped["quantity"]).replace([float('inf'), -float('inf')], 0).fillna(0)

        print("error hai kya?51")
        sku_grouped["previous_amazon_fee"] = pd.to_numeric(sku_grouped["previous_amazon_fee"], errors='coerce')
        sku_grouped["previous_net_taxes"] = pd.to_numeric(sku_grouped["previous_net_taxes"], errors='coerce')
        sku_grouped["previous_quantity"] = pd.to_numeric(sku_grouped["previous_quantity"], errors='coerce')
        sku_grouped["previous_unit_wise_amazon_fee"] = ((sku_grouped["previous_amazon_fee"] - sku_grouped["previous_net_taxes"]) / sku_grouped["previous_quantity"]).replace([float('inf'), -float('inf')], 0).fillna(0)

        print("error hai kya?52")
        print("Columns before percentage calc:", sku_grouped.columns.tolist())
        sku_grouped["unit_wise_amazon_fee_percentage"] = (
            (sku_grouped["unit_wise_amazon_fee"] - sku_grouped["previous_unit_wise_amazon_fee"]) /
            sku_grouped["previous_unit_wise_amazon_fee"]
        ) * 100

        print("error hai kya?53")
        sku_grouped["unit_wise_amazon_fee_percentage"] = sku_grouped["unit_wise_amazon_fee_percentage"].replace([float('inf'), -float('inf')], 0).fillna(0)
        sku_grouped["amazon_fee_growth"] = np.select(
            [
                -sku_grouped["unit_wise_amazon_fee_percentage"] >= 5,
                -sku_grouped["unit_wise_amazon_fee_percentage"] > 0.5,
                -sku_grouped["unit_wise_amazon_fee_percentage"] < -0.5
            ],
            ["High Growth", "Low Growth", "Negative Growth"],
            default="No Growth"
        )

        print("error hai kya?6")
        sku_grouped["unit_sales_analysis"] = ((sku_grouped["quantity"] - sku_grouped["previous_quantity"]) * sku_grouped["unit_wise_profitability"]).replace([float('inf'), -float('inf')], 0).fillna(0)
        print("error hai kya?7")
        sku_grouped["unit_asp_analysis"] = (((sku_grouped["asp"] - sku_grouped["previous_asp"]) * sku_grouped["quantity"])).replace([float('inf'), -float('inf')], 0).fillna(0)
        print("error hai kya?8")
        sku_grouped["amazon_fee_increase"] = (((sku_grouped["previous_unit_wise_amazon_fee"] - sku_grouped["unit_wise_amazon_fee"]) * sku_grouped["quantity"])).replace([float('inf'), -float('inf')], 0).fillna(0)
        sku_grouped["total_analysis"] = (sku_grouped["profit"] - sku_grouped["previous_profit"])
        print("error hai kya?9")
        sku_grouped["text_credit_increase"] = (((sku_grouped["previous_text_credit_change"] - sku_grouped["text_credit_change"]) * sku_grouped["quantity"])).replace([float('inf'), -float('inf')], 0).fillna(0)
        sku_grouped["final_total_analysis"] = (sku_grouped["amazon_fee_increase"] + sku_grouped["unit_asp_analysis"] + sku_grouped["unit_sales_analysis"] + sku_grouped["text_credit_increase"])

        columns_to_sum = ["amazon_fee_increase", "unit_asp_analysis", "unit_sales_analysis", "text_credit_increase"]
        sku_grouped["positive_action"] = sku_grouped[columns_to_sum].apply(lambda row: row[row > 0].sum(), axis=1)
        sku_grouped["negative_action"] = sku_grouped[columns_to_sum].apply(lambda row: row[row < 0].sum(), axis=1)
        sku_grouped["cross_check_analysis"] = (sku_grouped["total_analysis"] - sku_grouped["final_total_analysis"])
        sku_grouped["cross_check_analysis_backup"] = ((sku_grouped["positive_action"] + sku_grouped["negative_action"]) - (sku_grouped["final_total_analysis"]))
        sku_grouped["previous_sales_mix"] = (sku_grouped["previous_net_sales"] / total_Previous_sales) * 100
        sku_grouped["previous_sales_mix"] = sku_grouped["previous_sales_mix"].replace([float('inf'), -float('inf')], 0).fillna(0)
        sku_grouped["sales_mix_percentage"] = ((sku_grouped["sales_mix"] - sku_grouped["previous_sales_mix"]) / sku_grouped["previous_sales_mix"]) * 100

        print("error hai kya?10")
        sku_grouped["sales_mix_percentage"] = sku_grouped["sales_mix_percentage"].replace([float('inf'), -float('inf')], 0).fillna(0)
        sku_grouped["sales_mix_growth"] = np.select(
            [
                sku_grouped["sales_mix_percentage"] >= 5,
                sku_grouped["sales_mix_percentage"] > 0.5,
                sku_grouped["sales_mix_percentage"] < -0.5
            ],
            ["High Growth", "Low Growth", "Negative Growth"],
            default="No Growth"
        )

        # Ensure integer quantities for DB
        sku_grouped["quantity"] = pd.to_numeric(sku_grouped["quantity"], errors="coerce").fillna(0).astype(int)
        sku_grouped["previous_quantity"] = pd.to_numeric(sku_grouped["previous_quantity"], errors="coerce").fillna(0).astype(int)
        print("error hai kya?11")
        total_cous = abs(sku_grouped["cost_of_unit_sold"].sum())
        print(f"profit: {total_profit}")
        print(f"netsales: {total_sales}")

        # === EXPENSE BREAKDOWN ===
        total_net_credits = abs(sku_grouped["Net Credits"].sum())
        total_net_taxes = abs(sku_grouped["Net Taxes"].sum())
        total_fba_fees = abs(sku_grouped["fba_fees"].sum())
        total_selling_fees = abs(sku_grouped["selling_fees"].sum())
        total_cost = abs(sku_grouped["cost_of_unit_sold"].sum())
        # use centralized totals
        total_advertising = abs(advertising_total_all)
        total_platform = abs(platform_total)

        total_expense = (
            total_net_taxes
            + total_fba_fees
            + total_selling_fees
            + total_cost
            + total_advertising
            + total_platform
            - total_net_credits
        )

        print("==== EXPENSE BREAKDOWN ====")
        print(f"Net Credits (abs):           {total_net_credits:,.2f}")
        print(f"Net Taxes (abs):             {total_net_taxes:,.2f}")
        print(f"FBA Fees (abs):              {total_fba_fees:,.2f}")
        print(f"Selling Fees (abs):          {total_selling_fees:,.2f}")
        print(f"Cost of Unit Sold (abs):     {total_cost:,.2f}")
        print(f"Advertising Total (abs):     {total_advertising:,.2f}")
        print(f"Platform Fee (abs):          {total_platform:,.2f}")
        print("------------------------------")
        print(f"TOTAL EXPENSE:               {total_expense:,.2f}")
        total_taxes = (sku_grouped["Net Taxes"].sum())
        texncredit = total_taxes + total_net_credits
        print(f"texncredit (abs):          {texncredit:,.2f}")

        # Additional Metrics
        platform_fee = float(platform_total)
        advertising_total = float(advertising_total_all)

        reimbursement_vs_sales = abs((rembursement_fee / total_sales) * 100) if total_sales != 0 else 0
        cm2_profit = total_profit - (abs(advertising_total) + abs(platform_fee))
        cm2_margins = (cm2_profit / total_sales) * 100 if total_sales != 0 else 0
        acos = (advertising_total / total_sales) * 100 if total_sales != 0 else 0
        rembursment_vs_cm2_margins = abs((rembursement_fee / cm2_profit) * 100) if cm2_profit != 0 else 0
        print(f"Platform Fee: {platform_fee}")
        print(f"Advertising Total: {advertising_total}")
        print(f"Reimbursement Fee: {rembursement_fee}")
        print(f"Reimbursement vs Sales: {reimbursement_vs_sales:.2f}%")
        print(f"CM2 profit: {cm2_profit}")
        print(f"CM2 Margins: {cm2_margins:.2f}%")
        print(f"acos: {acos:.2f}%")
        print(f"Reimbursement vs CM2 Margins: {rembursment_vs_cm2_margins:.2f}%")
        print("error hai kya?")

        # ------------------ FIXED TOTAL ROW BUILD (DEDUP + UNIQUE COLUMNS) ------------------
        extra_cols_for_total = [
            "Net Sales", "Net Taxes", "Net Credits", "profit", "amazon_fee",
            "sales_mix", "previous_sales_mix", "sales_mix_percentage",
            "profit_mix", "previous_profit_mix",
            "unit_sales_analysis", "unit_asp_analysis", "amazon_fee_increase",
            "cross_check_analysis", "text_credit_increase", "final_total_analysis",
            "positive_action", "negative_action", "cross_check_analysis_backup",
            "total_analysis", "shipping_credits", "shipment_charges"
        ]

        cols_for_sum = list(dict.fromkeys(numeric_columns + extra_cols_for_total))
        cols_for_sum = [c for c in cols_for_sum if c in sku_grouped.columns]
        print(f"[total row] columns considered (unique): {len(cols_for_sum)}")

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
        print("error hai kya?q")

        sum_row["platform_fee"] = abs(platform_fee)
        sum_row["rembursement_fee"]= abs(rembursement_fee)
        sum_row["advertising_total"]= abs(advertising_total)
        sum_row["reimbursement_vs_sales"]= abs(reimbursement_vs_sales)
        sum_row["cm2_profit"]= abs(cm2_profit)
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
        print("error hai kya?r")

        # Totals part 2 (derived)
        qty = float(sum_row.get("quantity", 0) or 0)
        prev_qty = float(sum_row.get("previous_quantity", 0) or 0)
        net_sales_total = float(sum_row.get("Net Sales", 0) or 0)
        prev_net_sales_total = float(sum_row.get("previous_net_sales", 0) or 0)

        sum_row["unit_wise_profitability"] = ((float(sum_row.get("profit", 0))) / qty) if qty != 0 else 0
        print("error hai kya?z")

        sum_row["previous_unit_wise_profitability"] = (
            (float(sum_row.get("previous_profit", 0)) - float(sum_row.get("previous_net_taxes", 0))) /
            prev_qty
        ) * 100 if prev_qty != 0 else 0

        print("error hai kya?s")
        prev_uwp = float(sum_row.get("previous_unit_wise_profitability", 0) or 0)
        sum_row["unit_wise_profitability_percentage"] = (
            (float(sum_row["unit_wise_profitability"]) - prev_uwp) / prev_uwp
        ) * 100 if prev_uwp != 0 else 0

        sum_row["unit_increase"] = (
            (qty - prev_qty) / prev_qty
        ) * 100 if prev_qty != 0 else 0

        print("error hai kya?t")

        sum_row["asp"] = (net_sales_total / qty) if qty != 0 else 0
        sum_row["previous_asp"] = (prev_net_sales_total / prev_qty) if prev_qty != 0 else 0

        prev_asp = float(sum_row.get("previous_asp", 0) or 0)
        sum_row["asp_percentag"] = (
            (float(sum_row["asp"]) - prev_asp) / prev_asp
        ) * 100 if prev_asp != 0 else 0

        prev_amz_fee = float(sum_row.get("previous_amazon_fee", 0) or 0)
        sum_row["change_in_fee"] = ((float(sum_row.get("amazon_fee", 0))) / net_sales_total) * 100 if net_sales_total != 0 else 0
        sum_row["previous_change_in_fee"] = (prev_amz_fee / prev_net_sales_total) * 100 if prev_net_sales_total != 0 else 0

        print("error hai kya?u")

        sum_row["precentage_change_in_fee"] = (sum_row["change_in_fee"]) - (sum_row["previous_change_in_fee"])
        print("error hai kya?u1")

        total_taxes_sum = float(sum_row.get("Net Taxes", 0) or 0)
        sum_row["unit_wise_amazon_fee"] = ((float(sum_row.get("amazon_fee", 0)) - total_taxes_sum) / qty) if qty != 0 else 0
        print("error hai kya?u12")

        sum_row["previous_unit_wise_amazon_fee"] = (
            (prev_amz_fee - float(sum_row.get("previous_net_taxes", 0) or 0)) /
            prev_qty
        ) if prev_qty != 0 else 0
        print("error hai kya?v")

        prev_uwaf = float(sum_row.get("previous_unit_wise_amazon_fee", 0) or 0)
        sum_row["unit_wise_amazon_fee_percentage"] = (
            (float(sum_row.get("unit_wise_amazon_fee", 0)) - prev_uwaf) / prev_uwaf
        ) * 100 if prev_uwaf != 0 else 0

        prev_profit_total = float(sum_row.get("previous_profit", 0) or 0)
        sum_row["profit_change"] = (
            (float(sum_row.get("profit", 0)) - prev_profit_total) /
            prev_profit_total
        ) * 100  if prev_profit_total != 0 else 0

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
            (float(sum_row.get("Net Credits", 0) or 0) + float(sum_row.get("profit", 0) or 0)) /
            net_sales_total
        ) if net_sales_total != 0 else 0

        sum_row["previous_text_credit_change"] = (
            (float(sum_row.get("previous_net_credits", 0) or 0) + prev_profit_total) /
            prev_net_sales_total
        ) if prev_net_sales_total != 0 else 0

        # Growth buckets (unchanged logic)
        if sum_row["unit_wise_profitability_percentage"] >= 5:
            sum_row["unit_wise_profitability_growth"] = "High Growth"
        elif sum_row["unit_wise_profitability_percentage"] > 0.5:
            sum_row["unit_wise_profitability_growth"] = "Low Growth"
        elif sum_row["unit_wise_profitability_percentage"] < -0.5:
            sum_row["unit_wise_profitability_growth"] = "Negative Growth"
        else:
            sum_row["unit_wise_profitability_growth"] = "No Growth"

        if sum_row["asp_percentag"] >= 5:
            sum_row["asp_growth"] = "High Growth"
        elif sum_row["asp_percentag"] > 0.5:
            sum_row["asp_growth"] = "Low Growth"
        elif sum_row["asp_percentag"] < -0.5:
            sum_row["asp_growth"] = "Negative Growth"
        else:
            sum_row["asp_growth"] = "No Growth"

        if sum_row["profit_change"] >= 5:
            sum_row["profit_growth"] = "High Growth"
        elif sum_row["profit_change"] > 0.5:
            sum_row["profit_growth"] = "Low Growth"
        elif sum_row["profit_change"] < -0.5:
            sum_row["profit_growth"] = "Negative Growth"
        else:
            sum_row["profit_growth"] = "No Growth"

        if sum_row["unit_increase"] >= 5:
            sum_row["unit_growth"] = "High Growth"
        elif sum_row["unit_increase"] > 0.5:
            sum_row["unit_growth"] = "Low Growth"
        elif sum_row["unit_increase"] < -0.5:
            sum_row["unit_growth"] = "Negative Growth"
        else:
            sum_row["unit_growth"] = "No Growth"

        if sum_row["profit_mix_percentage"] >= 5:
            sum_row["profit_mix_growth"] = "High Growth"
        elif sum_row["profit_mix_percentage"] > 0.5:
            sum_row["profit_mix_growth"] = "Low Growth"
        elif sum_row["profit_mix_percentage"] < -0.5:
            sum_row["profit_mix_growth"] = "Negative Growth"
        else:
            sum_row["profit_mix_growth"] = "No Growth"

        if sum_row["sales_percentage"] >= 5:
            sum_row["sales_growth"] = "High Growth"
        elif sum_row["sales_percentage"] > 0.5:
            sum_row["sales_growth"] = "Low Growth"
        elif sum_row["sales_percentage"] < -0.5:
            sum_row["sales_growth"] = "Negative Growth"
        else:
            sum_row["sales_growth"] = "No Growth"

        if sum_row["unit_wise_amazon_fee_percentage"] >= 5:
            sum_row["amazon_fee_growth"] = "High Growth"
        elif sum_row["unit_wise_amazon_fee_percentage"] > 0.5:
            sum_row["amazon_fee_growth"] = "Low Growth"
        elif sum_row["unit_wise_amazon_fee_percentage"] < -0.5:
            sum_row["amazon_fee_growth"] = "Negative Growth"
        else:
            sum_row["amazon_fee_growth"] = "No Growth"

        if sum_row["sales_mix_percentage"] >= 5:
            sum_row["sales_mix_growth"] = "High Growth"
        elif sum_row["sales_mix_percentage"] > 0.5:
            sum_row["sales_mix_growth"] = "Low Growth"
        elif sum_row["sales_mix_percentage"] < -0.5:
            sum_row["sales_mix_growth"] = "Negative Growth"
        else:
            sum_row["sales_mix_growth"] = "No Growth"

        print("error hai kya?w")

        # Ensure sku_grouped has unique columns BEFORE appending total row
        if not sku_grouped.columns.is_unique:
            print("[WARN] sku_grouped had duplicate columns; de-duplicating by keeping first occurrence.")
            sku_grouped = sku_grouped.loc[:, ~sku_grouped.columns.duplicated()].copy()

        # Ensure sum_row quantities are integers (and exist)
        sum_row["quantity"] = int(float(sum_row.get("quantity", 0) or 0))
        sum_row["previous_quantity"] = int(float(sum_row.get("previous_quantity", 0) or 0))
        sum_row["user_id"] = user_id

        # Append total row safely
        sku_grouped = pd.concat(
            [sku_grouped, sum_row.to_frame().T],
            ignore_index=True,
            sort=False
        )

        print("error hai kya?w2")

        # Ensure correct column names for database
        sku_grouped.rename(columns={
            "Net Sales": "net_sales",
            "profit%": "profit_percentage",
            "Net Taxes": "net_taxes",
            "Net Credits": "net_credits"
        }, inplace=True)

        print("error hai kya?w3")
        total_row = sku_grouped[sku_grouped['sku'].astype(str).str.lower() == 'total']
        other_rows = sku_grouped[sku_grouped['sku'].astype(str).str.lower() != 'total']
        print("error hai kya?4")
        other_rows_sorted = other_rows.sort_values(by="profit", ascending=False)

        # Ensure ints for DB
        integer_columns = ['quantity', 'previous_quantity', 'user_id']
        for col in integer_columns:
            if col in other_rows_sorted.columns:
                other_rows_sorted[col] = pd.to_numeric(other_rows_sorted[col], errors='coerce').fillna(0).astype(int)
            if col in total_row.columns:
                total_row[col] = pd.to_numeric(total_row[col], errors='coerce').fillna(0).astype(int)

        sku_grouped = pd.concat([other_rows_sorted, total_row], ignore_index=True)
        print("error hai kya?w5")

        # Recreate monthly & rolling tables with aligned schemas
        conn.execute(text(f"DROP TABLE IF EXISTS {target_table}"))
        # conn.execute(text(f"DROP TABLE IF EXISTS {target_table2}"))

        for tbl in (target_table, target_table_usd_month):
            conn.execute(text(f"""
                CREATE TABLE IF NOT EXISTS {tbl} (
                    id SERIAL PRIMARY KEY,
                    sku TEXT,
                    price_in_gbp REAL,
                    product_sales REAL,
                    promotional_rebates REAL,
                    promotional_rebates_tax REAL,
                    product_sales_tax REAL,
                    selling_fees REAL,
                    refund_selling_fees REAL,
                    fba_fees REAL,
                    other REAL,
                    marketplace_facilitator_tax REAL,
                    shipping_credits_tax REAL,
                    giftwrap_credits_tax REAL,
                    postage_credits REAL,
                    gift_wrap_credits REAL,
                    net_sales REAL,
                    net_taxes REAL,
                    net_credits REAL,
                    profit REAL,
                    profit_percentage REAL,
                    amazon_fee REAL,
                    quantity INTEGER,
                    cost_of_unit_sold REAL,
                    other_transaction_fees REAL,
                    platform_fee REAL,
                    rembursement_fee REAL,
                    advertising_total REAL,
                    reimbursement_vs_sales REAL,
                    cm2_profit REAL,
                    cm2_margins REAL,
                    acos REAL,
                    total REAL,
                    rembursment_vs_cm2_margins REAL,
                    previous_net_sales REAL,
                    previous_net_credits REAL,
                    previous_profit REAL,
                    previous_profit_percentage REAL,
                    profit_change REAL,
                    profit_growth TEXT,
                    previous_quantity INTEGER,
                    previous_cost_of_unit_sold REAL,
                    previous_amazon_fee REAL,
                    previous_net_taxes REAL,
                    unit_wise_profitability REAL,
                    previous_unit_wise_profitability REAL,
                    unit_wise_profitability_percentage REAL,
                    unit_wise_profitability_growth TEXT,
                    cm1_profit TEXT,
                    category TEXT,
                    positive TEXT,
                    improvements TEXT,
                    asp REAL,
                    sales_percentage REAL,
                    sales_growth TEXT,
                    previous_asp REAL,
                    asp_percentag REAL,
                    text_credit_change REAL,
                    previous_text_credit_change REAL,
                    asp_growth TEXT,
                    sales_mix REAL,
                    sales_mix_analysis TEXT,
                    previous_fba_fees REAL,
                    previous_selling_fees REAL,
                    unit_increase REAL,
                    unit_growth TEXT,
                    change_in_fee REAL,
                    previous_change_in_fee REAL,
                    precentage_change_in_fee REAL,
                    unit_wise_amazon_fee REAL,
                    previous_unit_wise_amazon_fee REAL,
                    unit_wise_amazon_fee_percentage REAL,
                    amazon_fee_growth TEXT,
                    profit_mix REAL,
                    previous_profit_mix REAL,
                    profit_mix_percentage REAL,
                    profit_mix_growth TEXT,
                    profit_mix_analysis TEXT,
                    unit_sales_analysis REAL,
                    unit_asp_analysis REAL,
                    amazon_fee_increase REAL,
                    total_analysis REAL,
                    cross_check_analysis REAL,
                    previous_platform_fee REAL,
                    previous_rembursement_fee REAL,
                    previous_advertising_total REAL,
                    previous_reimbursement_vs_sales REAL,
                    previous_cm2_profit REAL,
                    previous_cm2_margins REAL,
                    previous_acos REAL,
                    previous_rembursment_vs_cm2_margins REAL,
                    previous_sales_mix REAL,
                    sales_mix_percentage REAL,
                    sales_mix_growth TEXT,
                    positive_action REAL,
                    negative_action REAL,
                    cross_check_analysis_backup REAL,
                    text_credit_increase REAL,
                    final_total_analysis REAL,
                    product_name TEXT,
                    shipping_credits REAL,
                    shipment_charges REAL,
                    errorstatus TEXT,
                    answer REAL,
                    difference REAL,
                    user_id INTEGER
                )
            """))
        for tbl in (target_table2, target_table_usd_roll):
            conn.execute(text(f"""
                CREATE TABLE IF NOT EXISTS {tbl} (
                    id SERIAL PRIMARY KEY,
                    sku TEXT,
                    price_in_gbp REAL,
                    product_sales REAL,
                    promotional_rebates REAL,
                    promotional_rebates_tax REAL,
                    product_sales_tax REAL,
                    selling_fees REAL,
                    refund_selling_fees REAL,
                    fba_fees REAL,
                    other REAL,
                    marketplace_facilitator_tax REAL,
                    shipping_credits_tax REAL,
                    giftwrap_credits_tax REAL,
                    postage_credits REAL,
                    gift_wrap_credits REAL,
                    net_sales REAL,
                    net_taxes REAL,
                    net_credits REAL,
                    profit REAL,
                    profit_percentage REAL,
                    amazon_fee REAL,
                    quantity INTEGER,
                    cost_of_unit_sold REAL,
                    other_transaction_fees REAL,
                    platform_fee REAL,
                    rembursement_fee REAL,
                    advertising_total REAL,
                    reimbursement_vs_sales REAL,
                    cm2_profit REAL,
                    cm2_margins REAL,
                    acos REAL,
                    rembursment_vs_cm2_margins REAL,
                    total REAL,
                    month TEXT,
                    year TEXT,
                    previous_net_sales REAL,
                    previous_net_credits REAL,
                    previous_profit REAL,
                    previous_profit_percentage REAL,
                    profit_change REAL,
                    profit_growth TEXT,
                    previous_quantity INTEGER,
                    previous_cost_of_unit_sold REAL,
                    previous_amazon_fee REAL,
                    previous_net_taxes REAL,
                    unit_wise_profitability REAL,
                    previous_unit_wise_profitability REAL,
                    unit_wise_profitability_percentage REAL,
                    unit_wise_profitability_growth TEXT,
                    cm1_profit TEXT,
                    category TEXT,
                    positive TEXT,
                    improvements TEXT,
                    asp REAL,
                    sales_percentage REAL,
                    sales_growth TEXT,
                    previous_asp REAL,
                    asp_percentag REAL,
                    text_credit_change REAL,
                    previous_text_credit_change REAL,
                    asp_growth TEXT,
                    sales_mix REAL,
                    sales_mix_analysis TEXT,
                    previous_fba_fees REAL,
                    previous_selling_fees REAL,
                    unit_increase REAL,
                    unit_growth TEXT,
                    change_in_fee REAL,
                    previous_change_in_fee REAL,
                    precentage_change_in_fee REAL,
                    unit_wise_amazon_fee REAL,
                    previous_unit_wise_amazon_fee REAL,
                    unit_wise_amazon_fee_percentage REAL,
                    amazon_fee_growth TEXT,
                    profit_mix REAL,
                    previous_profit_mix REAL,
                    profit_mix_percentage REAL,
                    profit_mix_growth TEXT,
                    profit_mix_analysis TEXT,
                    unit_sales_analysis REAL,
                    unit_asp_analysis REAL,
                    amazon_fee_increase REAL,
                    total_analysis REAL,
                    cross_check_analysis REAL,
                    previous_platform_fee REAL,
                    previous_rembursement_fee REAL,
                    previous_advertising_total REAL,
                    previous_reimbursement_vs_sales REAL,
                    previous_cm2_profit REAL,
                    previous_cm2_margins REAL,
                    previous_acos REAL,
                    previous_rembursment_vs_cm2_margins REAL,
                    previous_sales_mix REAL,
                    sales_mix_percentage REAL,
                    sales_mix_growth TEXT,
                    positive_action REAL,
                    negative_action REAL,
                    cross_check_analysis_backup REAL,
                    text_credit_increase REAL,
                    final_total_analysis REAL,
                    product_name TEXT,
                    country TEXT,
                    shipping_credits REAL,
                    shipment_charges REAL,
                    errorstatus TEXT,
                    answer REAL,
                    difference REAL,
                    user_id INTEGER
                )
            """))

        print("error hai kya?w6")

        currency1 = 'gbp'  # fallback/default

        # Fetch conversion rate
        # with engine1.connect() as conn1:
        #     currency_query = text("""
        #             SELECT conversion_rate
        #             FROM currency_conversion 
        #             WHERE lower(user_currency) = :currency1
        #             AND lower(country) = 'us'
        #             AND lower(month) = :month 
        #             AND year = :year
        #             LIMIT 1
        #     """)
        #     result = conn1.execute(currency_query, {
        #         "currency1": currency1,
        #         "country": "us",
        #         "month": month.lower(),
        #         "year": year
        #     }).fetchone()

        # currency_rate = result[0] if result else None
        # currency1 = 'gbp'  # tumhara base currency

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

        print("\n========= CURRENCY CONVERSION RATES =========")

        print(f"US Rate (USD → Home):        {rate_us}")
        print(f"India Rate (INR → Home):     {rate_ind}")
        print(f"Canada Rate (CAD → Home):    {rate_can}")
        print(f"UK Rate (GBP → GBP):         {rate_gbp}")

        print("============================================\n")



        print("error hai kya?7")

        # Define monetary columns for USD conversion
        monetary_columns = [
            'price_in_gbp', 'product_sales', 'promotional_rebates', 'promotional_rebates_tax',
            'product_sales_tax', 'selling_fees', 'refund_selling_fees', 'fba_fees', 'other',
            'marketplace_facilitator_tax', 'shipping_credits_tax', 'giftwrap_credits_tax',
            'shipping_credits', 'gift_wrap_credits', 'net_sales', 'net_taxes', 'net_credits',
            'profit', 'profit_percentage', 'amazon_fee', 'cost_of_unit_sold',
            'other_transaction_fees', 'platform_fee', 'shipment_charges', 'rembursement_fee',
            'advertising_total', 'reimbursement_vs_sales', 'cm2_profit', 'cm2_margins', 'acos',
            'rembursment_vs_cm2_margins', 'total', 'previous_net_sales', 'previous_net_credits',
            'previous_profit', 'previous_profit_percentage', 'profit_change',
            'previous_cost_of_unit_sold', 'previous_amazon_fee', 'previous_net_taxes',
            'unit_wise_profitability', 'previous_unit_wise_profitability',
            'unit_wise_profitability_percentage', 'asp', 'sales_percentage', 'previous_asp',
            'asp_percentag', 'text_credit_change', 'previous_text_credit_change', 'sales_mix',
            'previous_fba_fees', 'previous_selling_fees', 'unit_increase', 'change_in_fee',
            'previous_change_in_fee', 'precentage_change_in_fee', 'unit_wise_amazon_fee',
            'previous_unit_wise_amazon_fee', 'unit_wise_amazon_fee_percentage', 'profit_mix',
            'previous_profit_mix', 'profit_mix_percentage', 'unit_sales_analysis',
            'unit_asp_analysis', 'amazon_fee_increase', 'total_analysis', 'cross_check_analysis',
            'previous_platform_fee', 'previous_rembursement_fee', 'previous_advertising_total',
            'previous_reimbursement_vs_sales', 'previous_cm2_profit', 'previous_cm2_margins',
            'previous_acos', 'previous_rembursment_vs_cm2_margins', 'previous_sales_mix',
            'sales_mix_percentage', 'positive_action', 'negative_action',
            'cross_check_analysis_backup', 'text_credit_increase', 'final_total_analysis', 'postage_credits'
        ]

        print("error hai kya?w7")

        # Prepare USD converted DataFrame
        # df_usd = sku_grouped.copy()
        # if currency_rate:
        #     for col in monetary_columns:
        #         if col in df_usd.columns:
        #             df_usd[col] = pd.to_numeric(df_usd[col], errors='coerce') * currency_rate
        # else:
        #     print("⚠️ No conversion rate found for:", currency1, country, month, year)

        # 4 alag DF – base sku_grouped se
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


        print("error hai kya?w8")

        # Fill NaNs in df_usd
        # for col in df_usd.columns:
        #     if df_usd[col].dtype == 'object':
        #         df_usd[col] = df_usd[col].fillna('')
        #     elif pd.api.types.is_numeric_dtype(df_usd[col]):
        #         df_usd[col] = df_usd[col].fillna(0)

        for df_conv in [df_usd, df_ind, df_can, df_gbp]:
            for col in df_conv.columns:
                if df_conv[col].dtype == 'object':
                    df_conv[col] = df_conv[col].fillna('')
                elif pd.api.types.is_numeric_dtype(df_conv[col]):
                    df_conv[col] = df_conv[col].fillna(0)


        print("error hai kya?w9")

        # USD table (schema aligned)
        for tbl in [target_table_us, target_table_ind, target_table_can, target_table_gbp]:
            conn.execute(text(f"""
                CREATE TABLE IF NOT EXISTS {tbl} (
                    id SERIAL PRIMARY KEY,
                    sku TEXT,
                    price_in_gbp REAL,
                    product_sales REAL,
                    promotional_rebates REAL,
                    promotional_rebates_tax REAL,
                    product_sales_tax REAL,
                    selling_fees REAL,
                    refund_selling_fees REAL,
                    postage_credits REAL,
                    fba_fees REAL,
                    other REAL,
                    marketplace_facilitator_tax REAL,
                    shipping_credits_tax REAL,
                    giftwrap_credits_tax REAL,
                    shipping_credits REAL,
                    gift_wrap_credits REAL,
                    net_sales REAL,
                    net_taxes REAL,
                    net_credits REAL,
                    profit REAL,
                    profit_percentage REAL,
                    amazon_fee REAL,
                    quantity INTEGER,
                    cost_of_unit_sold REAL,
                    other_transaction_fees REAL,
                    platform_fee REAL,
                    shipment_charges REAL,
                    rembursement_fee REAL,
                    advertising_total REAL,
                    reimbursement_vs_sales REAL,
                    cm2_profit REAL,
                    cm2_margins REAL,
                    acos REAL,
                    rembursment_vs_cm2_margins REAL,
                    total REAL,
                    month TEXT,
                    year TEXT,
                    previous_net_sales REAL,
                    previous_net_credits REAL,
                    previous_profit REAL,
                    previous_profit_percentage REAL,
                    profit_change REAL,
                    profit_growth TEXT,
                    previous_quantity INTEGER,
                    previous_cost_of_unit_sold REAL,
                    previous_amazon_fee REAL,
                    previous_net_taxes REAL,
                    unit_wise_profitability REAL,
                    previous_unit_wise_profitability REAL,
                    unit_wise_profitability_percentage REAL,
                    unit_wise_profitability_growth TEXT,
                    cm1_profit TEXT,
                    category TEXT,
                    positive TEXT,
                    improvements TEXT,
                    asp REAL,
                    sales_percentage REAL,
                    sales_growth TEXT,
                    previous_asp REAL,
                    asp_percentag REAL,
                    text_credit_change REAL,
                    previous_text_credit_change REAL,
                    asp_growth TEXT,
                    sales_mix REAL,
                    sales_mix_analysis TEXT,
                    previous_fba_fees REAL,
                    previous_selling_fees REAL,
                    unit_increase REAL,
                    unit_growth TEXT,
                    change_in_fee REAL,
                    previous_change_in_fee REAL,
                    precentage_change_in_fee REAL,
                    unit_wise_amazon_fee REAL,
                    previous_unit_wise_amazon_fee REAL,
                    unit_wise_amazon_fee_percentage REAL,
                    amazon_fee_growth TEXT,
                    profit_mix REAL,
                    previous_profit_mix REAL,
                    profit_mix_percentage REAL,
                    profit_mix_growth TEXT,
                    profit_mix_analysis TEXT,
                    unit_sales_analysis REAL,
                    unit_asp_analysis REAL,
                    amazon_fee_increase REAL,
                    total_analysis REAL,
                    cross_check_analysis REAL,
                    previous_platform_fee REAL,
                    previous_rembursement_fee REAL,
                    previous_advertising_total REAL,
                    previous_reimbursement_vs_sales REAL,
                    previous_cm2_profit REAL,
                    previous_cm2_margins REAL,
                    previous_acos REAL,
                    previous_rembursment_vs_cm2_margins REAL,
                    previous_sales_mix REAL,
                    sales_mix_percentage REAL,
                    sales_mix_growth TEXT,
                    positive_action REAL,
                    negative_action REAL,
                    cross_check_analysis_backup REAL,
                    text_credit_increase REAL,
                    final_total_analysis REAL,
                    product_name TEXT,
                    country TEXT,
                    errorstatus TEXT,
                    answer REAL,
                    difference REAL,
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

        # conn.execute(
        #     text(f"DELETE FROM {target_table3} WHERE month = :month AND year = :year AND country = :country AND user_id = :user_id"),
        #     {"month": month, "year": year, "country": country, "user_id": user_id}
        # )
        # try:
        #     conn.commit()
        # except Exception as _:
        #     pass

        # mapping: dest_country_value, dataframe, target_table
        conversion_sets = [
            ("us",    df_usd, target_table_us),
            ("india", df_ind, target_table_ind),
            ("canada",df_can, target_table_can),
            ("gbp",   df_gbp, target_table_gbp),
        ]

        for dest_country, df_conv, tbl in conversion_sets:
            # ensure country column set correctly
            df_conv["country"] = dest_country
            # delete old data for same month/year/user/country
            conn.execute(
                text(f"""
                    DELETE FROM {tbl}
                    WHERE month   = :month
                    AND year    = :year
                    AND country = :country
                    AND user_id = :user_id
                """),
                {"month": month, "year": year, "country": dest_country, "user_id": user_id}
            )
            try:
                conn.commit()
            except Exception:
                pass

            df_conv.to_sql(tbl, conn, if_exists="append", index=False, method="multi", chunksize=100)
            

            # insert fresh data
    


        # print("error hai kya?w47")

        # # Final safety: ensure shipping cols exist in both frames before write
        # for col in ["shipping_credits", "shipment_charges"]:
        #     if col not in sku_grouped.columns:
        #         sku_grouped[col] = 0.0
        #     if col not in df_usd.columns:
        #         df_usd[col] = 0.0

        # # Insert data into the respective tables
        # sku_grouped.to_sql(target_table, conn, if_exists="replace", index=False, method="multi", chunksize=100)
        # sku_grouped.to_sql(target_table2, conn, if_exists="append", index=False, method="multi", chunksize=100)
        # df_usd.to_sql(target_table3, conn, if_exists="append", index=False, method="multi", chunksize=100)

        print("error hai kya?w47")

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

        # Agar previous_* ko bhi positive chahiye table 2 me, uncomment:
        # for col in ["previous_fba_fees", "previous_selling_fees"]:
        #     if col in df_roll.columns:
        #         df_roll[col] = df_roll[col].abs()
        # ================== IMPORTANT CHANGE END ==================

        # Insert data into the respective tables
        df_month.to_sql(target_table, conn, if_exists="replace", index=False, method="multi", chunksize=100)
        df_roll.to_sql(target_table2, conn, if_exists="append", index=False, method="multi", chunksize=100)

                # ========= NEW: USD per-country tables =========
        # Monthly USD data (same structure, but values in USD)
        df_month_usd = df_usd.copy()

        # Rolling USD data (selling_fees & fba_fees positive just like df_roll)
        df_roll_usd = df_usd.copy()
        for col in ["selling_fees", "fba_fees"]:
            if col in df_roll_usd.columns:
                df_roll_usd[col] = df_roll_usd[col].abs()

        # 1) Monthly USD table -> overwrite (per month)
        df_month_usd.to_sql(
            target_table_usd_month,
            conn,
            if_exists="replace",
            index=False,
            method="multi",
            chunksize=100
        )

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

        df_roll_usd.to_sql(
            target_table_usd_roll,
            conn,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=100
        )
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
                total_credits_usd          = convert_value(total_row_usd.get("net_credits", 0))
                total_tax_usd              = convert_value(total_row_usd.get("net_taxes", 0))

                total_expense_usd  = total_sales_usd - cm2_profit_val_usd
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
                    print(f"Existing upload history entry deleted for {logical_country}.")

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
                )

                session.add(upload_history_entry)
                session.commit()
                print("✅ Upload history entry saved successfully for uk_usd.")

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

        print("error hai kya?37")
        print(f"Data saved successfully in {target_table}!")
        print(f"Data saved successfully in {target_table2}!")
        print(f"Data saved successfully in {target_table_us} (USD converted)!")
        print(f"Data saved successfully in {target_table_ind} (India converted)!")
        print(f"Data saved successfully in {target_table_can} (Canada converted)!")
        print(f"Data saved successfully in {target_table_gbp} (GBP converted/base)!")

        return (total_cous, total_amazon_fee, cm2_profit, abs(rembursement_fee), abs(platform_fee),
                total_expense, total_profit_final, total_fba_fees, total_advertising, texncredit,
                reimbursement_vs_sales, cm2_margins, acos, rembursment_vs_cm2_margins, total_sales, total_quantity)

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
            print(f"\n==== Processing quarterly for source={source_table}, country={logical_country} ====")

            # Tumhara hi pattern:
            # quarter2_{user_id}_{country}_{year}_table
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
                SELECT "user_id","price_in_gbp", "product_sales", "promotional_rebates", "promotional_rebates_tax",
                "product_sales_tax", "selling_fees", "refund_selling_fees", "fba_fees", "other",
                "marketplace_facilitator_tax", "shipping_credits_tax", "giftwrap_credits_tax",
                "postage_credits", "gift_wrap_credits", "net_sales", "net_taxes", "net_credits",
                "profit", "profit_percentage", "amazon_fee", "sales_mix", "profit_mix", "quantity",
                "cost_of_unit_sold", "other_transaction_fees", "platform_fee", "rembursement_fee",
                "advertising_total", "reimbursement_vs_sales", "cm2_profit", "cm2_margins", "acos",
                "asp", "rembursment_vs_cm2_margins", "product_name","shipment_charges","unit_wise_profitability"
                FROM {source_table}
                WHERE LOWER(month) IN ({placeholders}) AND year = %s
            """
            df = pd.read_sql(query, conn, params=tuple(selected_months + [year]))

            if df.empty:
                print(f"No data for selected months in {source_table}.")
                continue

            # ---------- AGGREGATION (same as tumhara) ----------
            sku_grouped = df.groupby('product_name').agg({
                "price_in_gbp": "mean",
                "product_sales": "sum",
                "promotional_rebates": "sum",
                "promotional_rebates_tax": "sum",
                "product_sales_tax": "sum",
                "selling_fees": "sum",
                "refund_selling_fees": "sum",
                "fba_fees": "sum",
                "other": "sum",
                "marketplace_facilitator_tax": "sum",
                "shipping_credits_tax": "sum",
                "giftwrap_credits_tax": "sum",
                "postage_credits": "sum",
                "gift_wrap_credits": "sum",
                "net_sales": "sum",
                "net_taxes": "sum",
                "net_credits": "sum",
                "profit": "sum",
                "amazon_fee": "sum",
                "quantity": "sum",
                "cost_of_unit_sold": "sum",
                "other_transaction_fees": "sum",
                "platform_fee": "sum",
                "rembursement_fee": "sum",
                "advertising_total": "sum",
                "cm2_profit": "sum",
                "shipment_charges": "sum",
                "unit_wise_profitability": "sum",
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

            total_sales = abs(sku_grouped["net_sales"].sum())
            total_profit = abs(sku_grouped["profit"].sum())

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
                        product_name TEXT,
                        price_in_gbp DOUBLE PRECISION,
                        product_sales DOUBLE PRECISION,
                        promotional_rebates DOUBLE PRECISION,
                        promotional_rebates_tax DOUBLE PRECISION,
                        product_sales_tax DOUBLE PRECISION,
                        selling_fees DOUBLE PRECISION,
                        refund_selling_fees DOUBLE PRECISION,
                        fba_fees DOUBLE PRECISION,
                        other DOUBLE PRECISION,
                        marketplace_facilitator_tax DOUBLE PRECISION,
                        shipping_credits_tax DOUBLE PRECISION,
                        giftwrap_credits_tax DOUBLE PRECISION,
                        postage_credits DOUBLE PRECISION,
                        gift_wrap_credits DOUBLE PRECISION,
                        net_sales DOUBLE PRECISION,
                        net_taxes DOUBLE PRECISION,
                        net_credits DOUBLE PRECISION,
                        profit DOUBLE PRECISION,
                        profit_percentage DOUBLE PRECISION,
                        amazon_fee DOUBLE PRECISION,
                        quantity INTEGER,
                        cost_of_unit_sold DOUBLE PRECISION,
                        other_transaction_fees DOUBLE PRECISION,
                        platform_fee DOUBLE PRECISION,
                        rembursement_fee DOUBLE PRECISION,
                        advertising_total DOUBLE PRECISION,
                        reimbursement_vs_sales DOUBLE PRECISION,
                        cm2_profit DOUBLE PRECISION,
                        cm2_margins DOUBLE PRECISION,
                        acos DOUBLE PRECISION,
                        asp DOUBLE PRECISION,
                        rembursment_vs_cm2_margins DOUBLE PRECISION,
                        sales_mix DOUBLE PRECISION,
                        profit_mix DOUBLE PRECISION,
                        shipment_charges DOUBLE PRECISION, 
                        unit_wise_profitability DOUBLE PRECISION,
                        user_id INTEGER
                    )
                """))

                sku_grouped.columns = sku_grouped.columns.str.lower()
                sku_grouped.to_sql(quarter_table, conn_inner, if_exists="replace", index=False)
                # conn_inner.commit()  # engine.begin() khud handle karega
                print(f"✅ Quarterly SKU-wise data saved to `{quarter_table}`")

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
    print("enter in yearly")   
    config_list = [
        (f"skuwisemonthly_{user_id}_{country}",      "uk"),       # USD (pehle se)
        (f"skuwisemonthly_{user_id}_{country}_usd",  "uk_usd"),   # INR
        
    ]
 

    # PostgreSQL table naming - using lowercase for consistency
    # quarter_table = f"skuwiseyearly_{user_id}_{country}_{year}_table"
    # source_table = f"skuwisemonthly_{user_id}"
    
    try:
        for source_table, logical_country in config_list:
            print(f"\n==== Processing global monthly for source_table={source_table}, country={logical_country} ====")

            quarter_table = f"skuwiseyearly_{user_id}_{logical_country}_{year}_table"

        # Fetch yearly data - using parameterized query for PostgreSQL
            yearly_query = f"""
                SELECT "user_id","price_in_gbp", "product_sales", "promotional_rebates", "promotional_rebates_tax",
                "product_sales_tax", "selling_fees", "refund_selling_fees", "fba_fees", "other",
                "marketplace_facilitator_tax", "shipping_credits_tax", "giftwrap_credits_tax",
                "postage_credits", "gift_wrap_credits", "net_sales", "net_taxes", "net_credits",
                "profit", "profit_percentage", "amazon_fee", "sales_mix", "profit_mix", "quantity",
                "cost_of_unit_sold", "other_transaction_fees", "platform_fee", "rembursement_fee",
                "advertising_total", "reimbursement_vs_sales", "cm2_profit", "cm2_margins", "acos",
                "asp", "rembursment_vs_cm2_margins", "product_name","shipment_charges","unit_wise_profitability"
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


            # print(df.columns)
        
    
        # Group by SKU for aggregation
            sku_grouped = df.groupby('product_name').agg({
                "price_in_gbp": "mean",
                "product_sales": "sum",
                "promotional_rebates": "sum",
                "promotional_rebates_tax": "sum",
                "product_sales_tax": "sum",
                "selling_fees": "sum",
                "refund_selling_fees": "sum",  # Add this column to df before grouping if needed
                "fba_fees": "sum",
                "other": "sum",
                "marketplace_facilitator_tax": "sum",
                "shipping_credits_tax": "sum",
                "giftwrap_credits_tax": "sum",
                "postage_credits": "sum",
                "gift_wrap_credits": "sum",
                "net_sales": "sum",  # Calculate these columns before grouping
                "net_taxes": "sum",
                "net_credits": "sum",
                "profit": "sum",
                # "profit_percentage": "sum",
                "amazon_fee": "sum",
                # "sales_mix": "sum",
                # "profit_mix": "sum",
                "quantity": "sum",
                "cost_of_unit_sold": "sum",
                "other_transaction_fees": "sum",
                "platform_fee": "sum",
                "rembursement_fee": "sum",
                "advertising_total": "sum",
                # "reimbursement_vs_sales": "sum",
                "cm2_profit": "sum",
                # "cm2_margins": "sum",
                # "acos": "sum",
                # "asp": "sum",
                # "rembursment_vs_cm2_margins": "sum",
                "shipment_charges": "sum",
                "unit_wise_profitability": "sum", 
                "user_id": "first"  # or "sum" if you want to repeat user_id for each group
            }).reset_index()
            sku_grouped["product_name"] = sku_grouped["product_name"].astype(str).str.strip()
            sku_grouped["cm2_margins"] = sku_grouped.apply(
                lambda row: (row["cm2_profit"] / row["net_sales"]) * 100 if row["net_sales"] != 0 else 0,
                axis=1
            )
            # print(sku_grouped[["product_name", "cm2_margins"]])
            sku_grouped["acos"] = sku_grouped.apply(
                lambda row: (row["advertising_total"] / row["net_sales"]) * 100 if row["net_sales"] != 0 else 0,
                axis=1
            )
            # print(sku_grouped[["product_name", "acos"]])
            sku_grouped["rembursment_vs_cm2_margins"] = sku_grouped.apply(
                lambda row: (row["rembursement_fee"] / row["cm2_profit"]) * 100 if row["cm2_profit"] != 0 else 0,
                axis=1
            )
            # print(sku_grouped[["product_name", "rembursment_vs_cm2_margins"]])
            sku_grouped["reimbursement_vs_sales"] = sku_grouped.apply(
                lambda row: (row["rembursement_fee"] / row["net_sales"]) * 100 if row["net_sales"] != 0 else 0,
                axis=1
            )
            # print(sku_grouped[["product_name", "reimbursement_vs_sales"]])

            sku_grouped["profit_percentage"] = sku_grouped.apply(
                lambda row: (row["profit"] / row["net_sales"]) * 100 if row["net_sales"] != 0 else 0,
                axis=1
            )
            # print(sku_grouped[["product_name", "profit_percentage"]])

            sku_grouped["asp"] = sku_grouped.apply(
                lambda row: (row["net_sales"] / row["quantity"])  if row["quantity"] != 0 else 0,
                axis=1
            )
            # print(sku_grouped[["product_name", "asp"]])
            sku_grouped["unit_wise_profitability"] = sku_grouped.apply(
                lambda row: (row["profit"] / row["quantity"])  if row["quantity"] != 0 else 0,
                axis=1
            )
            # print(sku_grouped[["product_name", "unit_wise_profitability"]])

            total_sales = abs(sku_grouped["net_sales"].sum())  # lowercase column name
            total_profit = abs(sku_grouped["profit"].sum())
            # print(total_profit)
            # print(total_sales)

            sku_grouped["profit_mix"] = sku_grouped.apply(
                lambda row: (row["profit"] / total_profit) * 100 if total_profit != 0 else 0,
                axis=1
            )

            sku_grouped["sales_mix"] = sku_grouped.apply(
                lambda row: (row["net_sales"] / total_sales) * 100 if total_sales != 0 else 0,
                axis=1
            )

            
            
            # print(sku_grouped[["product_name", "profit_mix"]])
            
            
            # print(sku_grouped[["product_name", "sales_mix"]])



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
                    product_name TEXT,
                    price_in_gbp DOUBLE PRECISION,
                    product_sales DOUBLE PRECISION,
                    promotional_rebates DOUBLE PRECISION,
                    promotional_rebates_tax DOUBLE PRECISION,
                    product_sales_tax DOUBLE PRECISION,
                    selling_fees DOUBLE PRECISION,
                    refund_selling_fees DOUBLE PRECISION,
                    fba_fees DOUBLE PRECISION,
                    other DOUBLE PRECISION,
                    marketplace_facilitator_tax DOUBLE PRECISION,
                    shipping_credits_tax DOUBLE PRECISION,
                    giftwrap_credits_tax DOUBLE PRECISION,
                    postage_credits DOUBLE PRECISION,
                    gift_wrap_credits DOUBLE PRECISION,
                    net_sales DOUBLE PRECISION,
                    net_taxes DOUBLE PRECISION,
                    net_credits DOUBLE PRECISION,
                    profit DOUBLE PRECISION,
                    profit_percentage DOUBLE PRECISION,
                    amazon_fee DOUBLE PRECISION,
                    sales_mix DOUBLE PRECISION,
                    profit_mix DOUBLE PRECISION,
                    quantity INTEGER,
                    cost_of_unit_sold DOUBLE PRECISION,
                    other_transaction_fees DOUBLE PRECISION,
                    platform_fee DOUBLE PRECISION,
                    rembursement_fee DOUBLE PRECISION,
                    advertising_total DOUBLE PRECISION,
                    reimbursement_vs_sales DOUBLE PRECISION,
                    cm2_profit DOUBLE PRECISION,
                    cm2_margins DOUBLE PRECISION,
                    acos DOUBLE PRECISION,
                    asp DOUBLE PRECISION,
                    rembursment_vs_cm2_margins DOUBLE PRECISION,
                    shipment_charges DOUBLE PRECISION,
                    unit_wise_profitability DOUBLE PRECISION,
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
            print(f"Yearly SKU-wise data saved in {quarter_table}!")

       

    except Exception as e:
        print(f"Error processing yearly SKU-wise data: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()


