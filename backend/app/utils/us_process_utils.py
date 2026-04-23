from sqlalchemy import create_engine, text
import os
import pandas as pd
import numpy as np
from dotenv import load_dotenv

load_dotenv()

db_url = os.getenv('DATABASE_URL', 'postgresql://postgres:password@localhost:5432/phormula')
db_url1 = os.getenv('DATABASE_ADMIN_URL', 'postgresql://postgres:password@localhost:5432/admin_db')

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


def process_skuwise_us_data(user_id, country, month, year):
    engine = create_engine(db_url)
    engine1 = create_engine(db_url1)
    conn = engine.connect()

    source_table = f"user_{user_id}_{country}_{month}{year}_data"
    target_table = f"skuwisemonthly_{user_id}_{country}_{month}{year}"
    target_table2 = f"skuwisemonthly_{user_id}_{country}"

    target_table_us = f"skuwisemonthly_{user_id}"
    target_table_ind = f"skuwisemonthlyind_{user_id}"
    target_table_can = f"skuwisemonthlycan_{user_id}"
    target_table_gbp = f"skuwisemonthlygbp_{user_id}"

    prev_month, prev_year = get_previous_month_year(month, year)
    prev_table = f"skuwisemonthly_{user_id}_{country}_{prev_month}{prev_year}"

    def create_monthly_table(table_name, include_period_cols=False):
        period_cols = """
            month TEXT,
            year TEXT,
            country TEXT,
        """ if include_period_cols else ""

        conn.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
        conn.execute(text(f"""
            CREATE TABLE {table_name} (
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
                shipping_credits REAL,
                gift_wrap_credits REAL,
                net_sales REAL,
                net_taxes REAL,
                net_credits REAL,
                profit REAL,
                profit_percentage REAL,
                amazon_fee REAL,
                quantity INTEGER,
                return_quantity INTEGER,
                total_quantity INTEGER,
                gross_sales REAL,
                refund_sales REAL,
                tex_and_credits REAL,
                cost_of_unit_sold REAL,
                other_transaction_fees REAL,
                platform_fee REAL,
                shipment_charges REAL,
                rembursement_fee REAL,
                advertising_total REAL,
                visible_ads REAL,
                dealsvouchar_ads REAL,
                platformfeenew REAL,
                platform_fee_inventory_storage REAL,
                lost_total REAL,
                misc_transaction REAL,
                cm2_profit_percentage REAL,
                promotional_rebates_percentage REAL,
                reimbursement_vs_sales REAL,
                cm2_profit REAL,
                cm2_margins REAL,
                acos REAL,
                total REAL,
                rembursment_vs_cm2_margins REAL,
                {period_cols}
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
                user_id INTEGER
            )
        """))

    try:
        df = pd.read_sql(f"SELECT * FROM {source_table}", conn)
        if df.empty:
            print(f"No data found in {source_table}")
            return

        advertising_total = df[
            (df["sku"].isna()) | (df["sku"] == "") | (df["sku"] == "0")
        ]["other_transaction_fees"].sum()

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
            "previous_amazon_fee", "previous_net_taxes", "previous_fba_fees", "previous_selling_fees",
            "previous_platform_fee", "previous_rembursement_fee", "previous_advertising_total",
            "previous_reimbursement_vs_sales", "previous_cm2_profit", "previous_cm2_margins",
            "previous_acos", "previous_rembursment_vs_cm2_margins"
        ]
        numeric_column_previous = [col for col in numeric_column_previous if col in df_prev.columns]
        if numeric_column_previous:
            df_prev[numeric_column_previous] = (
                df_prev[numeric_column_previous]
                .apply(pd.to_numeric, errors="coerce")
                .fillna(0)
            )

        df["total"] = pd.to_numeric(df["total"].astype(str).str.replace(",", ""), errors="coerce")
        df["other"] = pd.to_numeric(df["other"].astype(str).str.replace(",", ""), errors="coerce")

        transfer_df = df[df["type"] == "Transfer"]
        rembursement_fee_desc_sum = abs(transfer_df["total"].sum())
        rembursement_fee_col_sum = df["net_reimbursement"].sum() if "net_reimbursement" in df.columns else 0
        rembursement_fee = rembursement_fee_desc_sum + rembursement_fee_col_sum

        numeric_columns = [
            "product_sales", "promotional_rebates", "product_sales_tax", "promotional_rebates_tax",
            "marketplace_facilitator_tax", "shipping_credits_tax", "giftwrap_credits_tax",
            "selling_fees", "fba_fees", "other", "gift_wrap_credits",
            "price_in_gbp", "cost_of_unit_sold", "quantity", "total", "other_transaction_fees"
        ]
        numeric_columns = [col for col in numeric_columns if col in df.columns]
        df[numeric_columns] = df[numeric_columns].apply(pd.to_numeric, errors='coerce').fillna(0)

        platform_fee_desc_sum = abs(
            df[df["description"].str.startswith(
                ("FBA Return Fee", "FBA Long-Term Storage Fee", "FBA storage fee", "Subscription"),
                na=False
            )]["total"].sum()
        )
        platform_fee_col_sum = df["platform_fees"].sum() if "platform_fees" in df.columns else 0
        platform_fee = platform_fee_desc_sum + platform_fee_col_sum

        shipment_keywords = [
            "FBA international shipping charge",
            "FBA Inbound Placement Service Fee",
            "FBA international shipping customs charge"
        ]
        shipment_mask = df["description"].astype(str).str.contains('|'.join(shipment_keywords), case=False, na=False)
        shipment_charges = abs(df[shipment_mask]["total"].sum())

        df = df[df["sku"].notna() & (df["sku"].astype(str).str.strip() != "")]
        df["sku"] = df["sku"].astype(str).str.strip()
        df = df[df["sku"].ne("None") & df["sku"].ne("0") & df["sku"].ne("") & df["sku"].notna()]

        refund_fees = df[df["type"] == "Refund"].groupby("sku")["selling_fees"].sum().reset_index()
        refund_fees.rename(columns={"selling_fees": "refund_selling_fees"}, inplace=True)
        refund_fees["sku"] = refund_fees["sku"].astype(str).str.strip()

        df["type_norm"] = df["type"].astype(str).str.strip().str.lower()
        df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce").fillna(0)

        mask = df["type_norm"].isin(["order", "shipment"])
        quantity_df = df[mask].groupby("sku", as_index=False)["quantity"].sum()

        sku_grouped = df.groupby('sku').agg({
            "price_in_gbp": "mean",
            "product_sales": "sum",
            "promotional_rebates": "sum",
            "promotional_rebates_tax": "sum",
            "product_sales_tax": "sum",
            "selling_fees": "sum",
            "fba_fees": "sum",
            "other": "sum",
            "marketplace_facilitator_tax": "sum",
            "shipping_credits_tax": "sum",
            "giftwrap_credits_tax": "sum",
            "shipping_credits": "sum",
            "gift_wrap_credits": "sum",
            "cost_of_unit_sold": "sum",
            "total": "sum",
            "other_transaction_fees": "sum",
            "product_name": "first",
        }).reset_index()

        sku_grouped = sku_grouped.merge(df_prev, on="sku", how="left").fillna(0)
        sku_grouped["sku"] = sku_grouped["sku"].astype(str).str.strip()
        sku_grouped = sku_grouped.merge(refund_fees, on="sku", how="left")
        sku_grouped = sku_grouped.merge(quantity_df, on="sku", how="left")
        sku_grouped["quantity"] = sku_grouped["quantity"].fillna(0)

        total_quantity = abs(sku_grouped["quantity"].sum())
        sku_grouped["selling_fees"] = pd.to_numeric(sku_grouped["selling_fees"], errors='coerce').fillna(0)
        sku_grouped["refund_selling_fees"] = pd.to_numeric(sku_grouped["refund_selling_fees"], errors='coerce').fillna(0)
        sku_grouped["selling_fees"] -= 2 * sku_grouped["refund_selling_fees"]

        sku_grouped["Net Sales"] = sku_grouped["product_sales"] + sku_grouped["promotional_rebates"]
        sku_grouped["Net Taxes"] = (
            sku_grouped["product_sales_tax"] +
            sku_grouped["marketplace_facilitator_tax"] +
            sku_grouped["shipping_credits_tax"] +
            sku_grouped["giftwrap_credits_tax"] +
            sku_grouped["promotional_rebates_tax"] +
            sku_grouped["other_transaction_fees"]
        )
        sku_grouped["Net Taxes"] = sku_grouped["Net Taxes"].apply(lambda x: 0 if abs(x) < 1e-10 else x)

        keywords = [
            "FBA Inventory Reimbursement - Customer Return",
            "FBA Inventory Reimbursement - Customer Service Issue",
            "FBA Inventory Reimbursement - General Adjustment",
            "FBA Inventory Reimbursement - Damaged:Warehouse",
            "FBA Inventory Reimbursement - Lost:Warehouse"
        ]
        mask = df["description"].astype(str).str.contains('|'.join(keywords), case=False, na=False)
        sku_net_credits = df[mask].groupby("sku")["total"].sum().abs().reset_index()
        sku_net_credits.rename(columns={"total": "Net Credits"}, inplace=True)

        sku_grouped = pd.merge(sku_grouped, sku_net_credits, on="sku", how="left")
        sku_grouped["Net Credits"] = sku_grouped["Net Credits"].fillna(0)
        sku_grouped["Net Credits"] = (
            sku_grouped["Net Credits"] +
            sku_grouped["gift_wrap_credits"] +
            sku_grouped["shipping_credits"]
        )

        sku_grouped["amazon_fee"] = (
            abs(sku_grouped["fba_fees"]) +
            abs(sku_grouped["selling_fees"]) -
            abs(sku_grouped["other"])
        )

        sku_grouped["profit"] = (
            abs(sku_grouped["Net Sales"]) +
            abs(sku_grouped["Net Credits"]) -
            abs(sku_grouped["Net Taxes"]) -
            abs(sku_grouped["amazon_fee"]) -
            abs(sku_grouped["cost_of_unit_sold"])
        )

        sku_grouped["profit%"] = (sku_grouped["profit"] / sku_grouped["Net Sales"]) * 100
        sku_grouped["profit%"] = sku_grouped["profit%"].replace([float('inf'), -float('inf')], 0).fillna(0)

        sku_grouped["unit_wise_profitability"] = sku_grouped["profit"] / sku_grouped["quantity"]
        sku_grouped["unit_wise_profitability"] = sku_grouped["unit_wise_profitability"].replace([float('inf'), -float('inf')], 0).fillna(0)

        sku_grouped["previous_unit_wise_profitability"] = sku_grouped["previous_profit"] / sku_grouped["previous_quantity"]
        sku_grouped["previous_unit_wise_profitability"] = sku_grouped["previous_unit_wise_profitability"].replace([float('inf'), -float('inf')], 0).fillna(0)

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

        sku_grouped["cm1_profit"] = sku_grouped["profit%"].apply(lambda x: "High" if (x / 100) > 0.5 else "Low")
        sku_grouped["asp"] = sku_grouped["Net Sales"] / sku_grouped["quantity"]
        sku_grouped["asp"] = sku_grouped["asp"].replace([float('inf'), -float('inf')], 0).fillna(0)

        sku_grouped["previous_asp"] = sku_grouped["previous_net_sales"] / sku_grouped["previous_quantity"]
        sku_grouped["previous_asp"] = sku_grouped["previous_asp"].replace([float('inf'), -float('inf')], 0).fillna(0)

        sku_grouped["asp_percentag"] = (
            (sku_grouped["asp"] - sku_grouped["previous_asp"]) /
            sku_grouped["previous_asp"]
        ) * 100
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

        sku_grouped["text_credit_change"] = (
            (sku_grouped["Net Taxes"] - sku_grouped["Net Credits"]) / sku_grouped["quantity"]
        )
        sku_grouped["text_credit_change"] = sku_grouped["text_credit_change"].replace([float('inf'), -float('inf')], 0).fillna(0)

        to_num = ["previous_net_taxes", "previous_net_credits", "previous_quantity"]
        sku_grouped[to_num] = sku_grouped[to_num].apply(pd.to_numeric, errors="coerce").fillna(0)

        sku_grouped["previous_text_credit_change"] = (
            (sku_grouped["previous_net_taxes"] - sku_grouped["previous_net_credits"]) /
            sku_grouped["previous_quantity"]
        )
        sku_grouped["previous_text_credit_change"] = sku_grouped["previous_text_credit_change"].replace([float('inf'), -float('inf')], 0).fillna(0)

        sku_grouped["profit_change"] = (
            (sku_grouped["profit"] - sku_grouped["previous_profit"]) /
            sku_grouped["previous_profit"]
        ) * 100
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

        sku_grouped["unit_increase"] = (
            (sku_grouped["quantity"] - sku_grouped["previous_quantity"]) /
            sku_grouped["previous_quantity"]
        ) * 100
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

        sku_grouped["category"] = ""
        sku_grouped["positive"] = ""
        sku_grouped["improvements"] = ""
        sku_grouped["month"] = month
        sku_grouped["year"] = year
        sku_grouped["country"] = country
        sku_grouped["platform_fee"] = 0
        sku_grouped["shipment_charges"] = 0
        sku_grouped["rembursement_fee"] = 0
        sku_grouped["advertising_total"] = 0
        sku_grouped["reimbursement_vs_sales"] = 0
        sku_grouped["cm2_profit"] = 0
        sku_grouped["cm2_margins"] = 0
        sku_grouped["acos"] = 0
        sku_grouped["rembursment_vs_cm2_margins"] = 0
        sku_grouped["user_id"] = user_id

        total_amazon_fee = sku_grouped["amazon_fee"].sum()
        total_sales = abs(sku_grouped["Net Sales"].sum())
        total_profit = abs(sku_grouped["profit"].sum())
        total_Previous_profit = abs(sku_grouped["previous_profit"].sum())
        total_Previous_sales = abs(sku_grouped["previous_net_sales"].sum())
        total_tax = abs(sku_grouped["Net Taxes"].sum())
        total_credits = abs(sku_grouped["Net Credits"].sum())

        sku_grouped["sales_percentage"] = (
            (sku_grouped["Net Sales"] - sku_grouped["previous_net_sales"]) /
            sku_grouped["previous_net_sales"]
        ) * 100
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

        sku_grouped["sales_mix"] = (sku_grouped["Net Sales"] / total_sales) * 100
        sku_grouped["sales_mix"] = sku_grouped["sales_mix"].replace([float('inf'), -float('inf')], 0).fillna(0)

        sku_grouped["profit_mix"] = (sku_grouped["profit"] / total_profit) * 100
        sku_grouped["profit_mix"] = sku_grouped["profit_mix"].replace([float('inf'), -float('inf')], 0).fillna(0)

        sku_grouped["previous_profit_mix"] = (sku_grouped["previous_profit"] / total_Previous_profit) * 100
        sku_grouped["previous_profit_mix"] = sku_grouped["previous_profit_mix"].replace([float('inf'), -float('inf')], 0).fillna(0)

        sku_grouped["profit_mix_percentage"] = (
            (sku_grouped["profit_mix"] - sku_grouped["previous_profit_mix"]) /
            sku_grouped["previous_profit_mix"]
        ) * 100
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

        sku_grouped["profit_mix_analysis"] = sku_grouped["profit_mix_percentage"].apply(
            lambda x: "High" if (x / 100) > 0.2 else "Low"
        )

        sku_grouped["change_in_fee"] = (sku_grouped["amazon_fee"] / sku_grouped["Net Sales"]) * 100
        sku_grouped["change_in_fee"] = sku_grouped["change_in_fee"].replace([float('inf'), -float('inf')], 0).fillna(0)

        sku_grouped["previous_change_in_fee"] = (sku_grouped["previous_amazon_fee"] / sku_grouped["previous_net_sales"]) * 100
        sku_grouped["previous_change_in_fee"] = sku_grouped["previous_change_in_fee"].replace([float('inf'), -float('inf')], 0).fillna(0)

        sku_grouped["precentage_change_in_fee"] = sku_grouped["change_in_fee"] - sku_grouped["previous_change_in_fee"]
        sku_grouped["sales_mix_analysis"] = sku_grouped["sales_mix"].apply(lambda x: "High" if (x / 100) > 0.2 else "Low")

        sku_grouped["unit_wise_amazon_fee"] = (sku_grouped["amazon_fee"] - sku_grouped["Net Taxes"]) / sku_grouped["quantity"]
        sku_grouped["unit_wise_amazon_fee"] = sku_grouped["unit_wise_amazon_fee"].replace([float('inf'), -float('inf')], 0).fillna(0)

        sku_grouped["previous_amazon_fee"] = pd.to_numeric(sku_grouped["previous_amazon_fee"], errors='coerce')
        sku_grouped["previous_net_taxes"] = pd.to_numeric(sku_grouped["previous_net_taxes"], errors='coerce')
        sku_grouped["previous_quantity"] = pd.to_numeric(sku_grouped["previous_quantity"], errors='coerce')

        sku_grouped["previous_unit_wise_amazon_fee"] = (
            (sku_grouped["previous_amazon_fee"] - sku_grouped["previous_net_taxes"]) /
            sku_grouped["previous_quantity"]
        )
        sku_grouped["previous_unit_wise_amazon_fee"] = sku_grouped["previous_unit_wise_amazon_fee"].replace([float('inf'), -float('inf')], 0).fillna(0)

        sku_grouped["unit_wise_amazon_fee_percentage"] = (
            (sku_grouped["unit_wise_amazon_fee"] - sku_grouped["previous_unit_wise_amazon_fee"]) /
            sku_grouped["previous_unit_wise_amazon_fee"]
        ) * 100
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

        sku_grouped["unit_sales_analysis"] = (
            (sku_grouped["quantity"] - sku_grouped["previous_quantity"]) *
            sku_grouped["unit_wise_profitability"]
        )
        sku_grouped["unit_sales_analysis"] = sku_grouped["unit_sales_analysis"].replace([float('inf'), -float('inf')], 0).fillna(0)

        sku_grouped["unit_asp_analysis"] = (
            (sku_grouped["asp"] - sku_grouped["previous_asp"]) *
            sku_grouped["quantity"]
        )
        sku_grouped["unit_asp_analysis"] = sku_grouped["unit_asp_analysis"].replace([float('inf'), -float('inf')], 0).fillna(0)

        sku_grouped["amazon_fee_increase"] = (
            (sku_grouped["previous_unit_wise_amazon_fee"] - sku_grouped["unit_wise_amazon_fee"]) *
            sku_grouped["quantity"]
        )
        sku_grouped["amazon_fee_increase"] = sku_grouped["amazon_fee_increase"].replace([float('inf'), -float('inf')], 0).fillna(0)

        sku_grouped["total_analysis"] = sku_grouped["profit"] - sku_grouped["previous_profit"]

        sku_grouped["text_credit_increase"] = (
            (sku_grouped["previous_text_credit_change"] - sku_grouped["text_credit_change"]) *
            sku_grouped["quantity"]
        )
        sku_grouped["text_credit_increase"] = sku_grouped["text_credit_increase"].replace([float('inf'), -float('inf')], 0).fillna(0)

        sku_grouped["final_total_analysis"] = (
            sku_grouped["amazon_fee_increase"] +
            sku_grouped["unit_asp_analysis"] +
            sku_grouped["unit_sales_analysis"] +
            sku_grouped["text_credit_increase"]
        )

        columns_to_sum = [
            "amazon_fee_increase",
            "unit_asp_analysis",
            "unit_sales_analysis",
            "text_credit_increase"
        ]

        sku_grouped["positive_action"] = sku_grouped[columns_to_sum].apply(lambda row: row[row > 0].sum(), axis=1)
        sku_grouped["negative_action"] = sku_grouped[columns_to_sum].apply(lambda row: row[row < 0].sum(), axis=1)

        sku_grouped["cross_check_analysis"] = sku_grouped["total_analysis"] - sku_grouped["final_total_analysis"]
        sku_grouped["cross_check_analysis_backup"] = (
            (sku_grouped["positive_action"] + sku_grouped["negative_action"]) - sku_grouped["final_total_analysis"]
        )

        sku_grouped["previous_sales_mix"] = (sku_grouped["previous_net_sales"] / total_Previous_sales) * 100
        sku_grouped["previous_sales_mix"] = sku_grouped["previous_sales_mix"].replace([float('inf'), -float('inf')], 0).fillna(0)

        sku_grouped["sales_mix_percentage"] = (
            (sku_grouped["sales_mix"] - sku_grouped["previous_sales_mix"]) /
            sku_grouped["previous_sales_mix"]
        ) * 100
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

        total_cous = abs(sku_grouped["cost_of_unit_sold"].sum())
        total_fba_fees = abs(sku_grouped["fba_fees"].sum())

        reimbursement_vs_sales = abs((rembursement_fee / total_sales) * 100) if total_sales != 0 else 0
        cm2_profit = total_profit - (abs(advertising_total) + abs(platform_fee) + abs(shipment_charges))
        cm2_margins = (cm2_profit / total_sales) * 100 if total_sales != 0 else 0
        acos = (advertising_total / total_sales) * 100 if total_sales != 0 else 0
        rembursment_vs_cm2_margins = abs((rembursement_fee / cm2_profit) * 100) if cm2_profit != 0 else 0

        sum_row = sku_grouped[
            numeric_columns + [
                "Net Sales", "Net Taxes", "Net Credits", "profit", "amazon_fee",
                "sales_mix", "previous_sales_mix", "sales_mix_percentage", "profit_mix",
                "previous_profit_mix", "unit_sales_analysis", "unit_asp_analysis",
                "amazon_fee_increase", "cross_check_analysis", "text_credit_increase",
                "final_total_analysis", "positive_action", "negative_action",
                "cross_check_analysis_backup", "total_analysis"
            ]
        ].sum()

        sum_row["sku"] = "TOTAL"
        sum_row["month"] = month
        sum_row["country"] = country
        sum_row["year"] = year
        sum_row["product_name"] = "TOTAL"
        sum_row["profit%"] = (sum_row["profit"] / sum_row["Net Sales"]) * 100 if sum_row["Net Sales"] != 0 else 0
        sum_row["platform_fee"] = abs(platform_fee)
        sum_row["shipment_charges"] = abs(shipment_charges)
        sum_row["rembursement_fee"] = abs(rembursement_fee)
        sum_row["advertising_total"] = abs(advertising_total)
        sum_row["reimbursement_vs_sales"] = abs(reimbursement_vs_sales)
        sum_row["cm2_profit"] = abs(cm2_profit)
        sum_row["cm2_margins"] = abs(cm2_margins)
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
        sum_row["previous_unit_wise_profitability"] = (
            (float(sum_row["previous_profit"]) - float(sum_row["previous_net_taxes"])) / sum_row["previous_quantity"]
        ) * 100 if sum_row["previous_quantity"] != 0 else 0
        sum_row["unit_wise_profitability_percentage"] = (
            (sum_row["unit_wise_profitability"] - sum_row["previous_unit_wise_profitability"]) /
            sum_row["previous_unit_wise_profitability"]
        ) * 100 if sum_row["previous_unit_wise_profitability"] != 0 else 0
        sum_row["unit_increase"] = (
            (sum_row["quantity"] - sum_row["previous_quantity"]) / sum_row["previous_quantity"]
        ) * 100 if sum_row["previous_quantity"] != 0 else 0
        sum_row["asp"] = (sum_row["Net Sales"] / sum_row["quantity"]) if sum_row["quantity"] != 0 else 0
        sum_row["previous_asp"] = (sum_row["previous_net_sales"] / sum_row["previous_quantity"]) if sum_row["previous_quantity"] != 0 else 0
        sum_row["asp_percentag"] = (
            (sum_row["asp"] - sum_row["previous_asp"]) / sum_row["previous_asp"]
        ) * 100 if sum_row["previous_asp"] != 0 else 0
        sum_row["change_in_fee"] = (sum_row["amazon_fee"] / sum_row["Net Sales"]) * 100 if sum_row["Net Sales"] != 0 else 0
        sum_row["previous_change_in_fee"] = (
            sum_row["previous_amazon_fee"] / sum_row["previous_net_sales"]
        ) * 100 if sum_row["previous_net_sales"] != 0 else 0
        sum_row["precentage_change_in_fee"] = sum_row["change_in_fee"] - sum_row["previous_change_in_fee"]
        sum_row["unit_wise_amazon_fee"] = (
            (sum_row["amazon_fee"] - sum_row["Net Taxes"]) / sum_row["quantity"]
        ) if sum_row["quantity"] != 0 else 0
        sum_row["previous_unit_wise_amazon_fee"] = (
            (float(sum_row["previous_amazon_fee"]) - float(sum_row["previous_net_taxes"])) /
            float(sum_row["previous_quantity"])
        ) if float(sum_row["previous_quantity"]) != 0 else 0
        sum_row["unit_wise_amazon_fee_percentage"] = (
            (sum_row["unit_wise_amazon_fee"] - sum_row["previous_unit_wise_amazon_fee"]) /
            sum_row["previous_unit_wise_amazon_fee"]
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

        sum_row["user_id"] = user_id

        sku_grouped = pd.concat([sku_grouped, sum_row.to_frame().T], ignore_index=True)

        sku_grouped.rename(columns={
            "Net Sales": "net_sales",
            "profit%": "profit_percentage",
            "Net Taxes": "net_taxes",
            "Net Credits": "net_credits"
        }, inplace=True)

        total_row = sku_grouped[sku_grouped['sku'].str.lower() == 'total']
        other_rows = sku_grouped[sku_grouped['sku'].str.lower() != 'total']
        other_rows_sorted = other_rows.sort_values(by="profit", ascending=False)
        sku_grouped = pd.concat([other_rows_sorted, total_row], ignore_index=True)

        for col, default in {
            "return_quantity": 0,
            "total_quantity": 0,
            "gross_sales": 0.0,
            "refund_sales": 0.0,
            "tex_and_credits": 0.0,
            "visible_ads": 0.0,
            "dealsvouchar_ads": 0.0,
            "platformfeenew": 0.0,
            "platform_fee_inventory_storage": 0.0,
            "lost_total": 0.0,
            "misc_transaction": 0.0,
            "cm2_profit_percentage": 0.0,
            "promotional_rebates_percentage": 0.0,
        }.items():
            if col not in sku_grouped.columns:
                sku_grouped[col] = default

        for col in sku_grouped.columns:
            if sku_grouped[col].dtype == 'object':
                sku_grouped[col] = sku_grouped[col].fillna('')
            elif pd.api.types.is_numeric_dtype(sku_grouped[col]):
                sku_grouped[col] = sku_grouped[col].fillna(0)

        currency1 = 'usd'

        def get_conversion_rate(dest_country: str):
            with engine1.connect() as conn1:
                currency_query = text("""
                    SELECT conversion_rate
                    FROM currency_conversion
                    WHERE lower(user_currency) = :currency1
                    AND lower(country) = :dest_country
                    AND lower(month) = :month
                    AND year = :year
                    LIMIT 1
                """)
                result = conn1.execute(currency_query, {
                    "currency1": currency1.lower(),
                    "dest_country": dest_country.lower(),
                    "month": month.lower(),
                    "year": year
                }).fetchone()
            return result[0] if result else None

        rate_us = 1.0
        rate_ind = get_conversion_rate("india")
        rate_can = get_conversion_rate("canada")
        rate_gbp = get_conversion_rate("uk")

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
            'cross_check_analysis_backup', 'text_credit_increase', 'final_total_analysis'
        ]

        def apply_rate(df_conv, rate):
            if rate is None:
                return
            for col in monetary_columns:
                if col in df_conv.columns:
                    df_conv[col] = pd.to_numeric(df_conv[col], errors="coerce").fillna(0) * rate

        df_usd = sku_grouped.copy()
        df_ind = sku_grouped.copy()
        df_can = sku_grouped.copy()
        df_gbp = sku_grouped.copy()

        apply_rate(df_usd, rate_us)
        apply_rate(df_ind, rate_ind)
        apply_rate(df_can, rate_can)
        apply_rate(df_gbp, rate_gbp)

        df_usd["country"] = "us"
        df_ind["country"] = "india"
        df_can["country"] = "canada"
        df_gbp["country"] = "gbp"

        for df_conv in [df_usd, df_ind, df_can, df_gbp]:
            for col in df_conv.columns:
                if df_conv[col].dtype == 'object':
                    df_conv[col] = df_conv[col].fillna('')
                elif pd.api.types.is_numeric_dtype(df_conv[col]):
                    df_conv[col] = df_conv[col].fillna(0)

        create_monthly_table(target_table, include_period_cols=False)
        create_monthly_table(target_table2, include_period_cols=True)
        create_monthly_table(target_table_us, include_period_cols=True)
        create_monthly_table(target_table_ind, include_period_cols=True)
        create_monthly_table(target_table_can, include_period_cols=True)
        create_monthly_table(target_table_gbp, include_period_cols=True)

        conn.commit()

        conversion_sets = [
            ("us", df_usd, target_table_us),
            ("india", df_ind, target_table_ind),
            ("canada", df_can, target_table_can),
            ("gbp", df_gbp, target_table_gbp),
        ]

        for dest_country, df_conv, tbl in conversion_sets:
            df_conv["country"] = dest_country
            df_conv.to_sql(tbl, conn, if_exists="append", index=False, method="multi", chunksize=100)

        sku_grouped_main = sku_grouped.drop(columns=["month", "year", "country"], errors="ignore")

        sku_grouped_main.to_sql(
            target_table,
            conn,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=100
        )

        sku_grouped.to_sql(
            target_table2,
            conn,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=100
        )

        conn.commit()

        total_expense = total_sales - cm2_profit
        otherwplatform = platform_fee
        taxncredit = total_tax + abs(total_credits)

        total_product_sales = float(
            pd.to_numeric(sku_grouped.get("product_sales", 0), errors="coerce").fillna(0).sum()
            + pd.to_numeric(sku_grouped.get("product_sales_tax", 0), errors="coerce").fillna(0).sum()
            + pd.to_numeric(sku_grouped.get("shipping_credits", 0), errors="coerce").fillna(0).sum()
            + pd.to_numeric(sku_grouped.get("shipping_credits_tax", 0), errors="coerce").fillna(0).sum()
            + pd.to_numeric(sku_grouped.get("gift_wrap_credits", 0), errors="coerce").fillna(0).sum()
            + pd.to_numeric(sku_grouped.get("giftwrap_credits_tax", 0), errors="coerce").fillna(0).sum()
            + pd.to_numeric(sku_grouped.get("promotional_rebates", 0), errors="coerce").fillna(0).sum()
            + pd.to_numeric(sku_grouped.get("promotional_rebates_tax", 0), errors="coerce").fillna(0).sum()
        )

        return (
            platform_fee, rembursement_fee, total_cous, total_amazon_fee,
            total_profit, total_expense, total_fba_fees, cm2_profit,
            cm2_margins, acos, rembursment_vs_cm2_margins,
            advertising_total, reimbursement_vs_sales, total_quantity,
            total_sales, otherwplatform, taxncredit, total_product_sales
        )

    except Exception as e:
        print(f"Error processing SKU-wise data: {e}")
    finally:
        conn.close()

   
    
def process_us_yearly_skuwise_data(user_id, country, year):   
    engine = create_engine(db_url)
    conn = engine.connect()

    yearly_table = f"skuwiseyearly_{user_id}_{country}_{year}_table"
    source_table = f"user_{user_id}_{country}_merge_data_of_all_months"

    try:
        # Load data
        yearly_query = text(f"""
            SELECT *
            FROM {source_table}
            WHERE year = :year
        """)
        df = pd.read_sql(yearly_query, conn, params={"year": year})

        if df.empty:
            print(f"No data found for year {year} in {source_table}")
            return
        

        advertising_total = df[
            (df["sku"].isna()) | (df["sku"] == "") | (df["sku"] == "0")
        ]["other_transaction_fees"].sum()

        platform_fee_desc_sum = abs(
            df[df["description"].str.startswith(
                ("FBA Return Fee", "FBA Long-Term Storage Fee", "FBA storage fee", "Subscription"),
                na=False
            )]["total"].sum()
        )
        platform_fee_col_sum = df["platform_fees"].sum() if "platform_fees" in df.columns else 0
        platform_fee = platform_fee_desc_sum + platform_fee_col_sum
        # Define the relevant keywords
        shipment_keywords = [
            "FBA international shipping charge",
            "FBA Inbound Placement Service Fee",
            "FBA international shipping customs charge"
        ]
        
        # Filter rows based on whether 'description' contains any of the shipment keywords
        shipment_mask = df["description"].astype(str).str.contains('|'.join(shipment_keywords), case=False, na=False)
        
        # Calculate shipment charges
        shipment_charges = abs(df[shipment_mask]["total"].sum())

        df["total"] = pd.to_numeric(df["total"], errors="coerce").fillna(0)
        df["other"] = pd.to_numeric(df["other"], errors="coerce").fillna(0)

        transfer_df = df[df["type"] == "Transfer"]
        rembursement_fee_desc_sum = abs(transfer_df["total"].sum())
        rembursement_fee_col_sum = df["net_reimbursement"].sum() if "net_reimbursement" in df.columns else 0
        rembursment_fee = rembursement_fee_desc_sum + rembursement_fee_col_sum
        # Normalize numeric columns
        numeric_columns = [
            "product_sales", "promotional_rebates", "product_sales_tax", "promotional_rebates_tax",
            "marketplace_facilitator_tax", "shipping_credits_tax", "giftwrap_credits_tax",
            "quantity", "cost_of_unit_sold", "selling_fees", "fba_fees", "other", 
            "gift_wrap_credits", "price_in_gbp", "total", "other_transaction_fees"
        ]
        for col in numeric_columns:
            df[col] = pd.to_numeric(df[col].astype(str).str.replace(",", ""), errors="coerce").fillna(0)
        
        # shipment_charges = abs(df[df["bucket"].str.startswith(
        # ("Freight & Forward"), na=False)]["total"].sum())

        df["sku"] = df["sku"].astype(str).str.strip()
        df = df[df["sku"].notna() & (df["sku"] != "")]

        df = df[df["sku"].ne("None") & df["sku"].ne("0") & df["sku"].ne("") & df["sku"].notna()]

        # === Refund Fees ===
        refund_fees = df[df["type"] == "Refund"].groupby("sku")["selling_fees"].sum().reset_index()
        refund_fees.rename(columns={"selling_fees": "refund_selling_fees"}, inplace=True)
        refund_fees["sku"] = refund_fees["sku"].astype(str).str.strip()


        df["type_norm"] = df["type"].astype(str).str.strip().str.lower()
        df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce").fillna(0)

        # exact match: 'order' OR 'shipment' (case-insensitive)
        mask = df["type_norm"].isin(["order", "shipment"])

        quantity_df = (
            df[mask]
            .groupby("sku", as_index=False)["quantity"]
            .sum()
        )


        # === Group by SKU ===
        sku_grouped = df.groupby("sku").agg({
            "price_in_gbp": "mean", "product_sales": "sum", "promotional_rebates": "sum",
            "promotional_rebates_tax": "sum", "product_sales_tax": "sum",
            "selling_fees": "sum", "fba_fees": "sum", "other": "sum",
            "marketplace_facilitator_tax": "sum", "shipping_credits_tax": "sum",
            "giftwrap_credits_tax": "sum", "shipping_credits": "sum",
            "gift_wrap_credits": "sum", "cost_of_unit_sold": "sum",
            "total": "sum", "other_transaction_fees": "sum","product_name": "first",
        }).reset_index()

        sku_grouped["sku"] = sku_grouped["sku"].astype(str).str.strip()
        sku_grouped = sku_grouped.merge(refund_fees, on="sku", how="left")
        sku_grouped = sku_grouped[sku_grouped["sku"].astype(str).str.strip().ne("0") & sku_grouped["sku"].notna()]

        sku_grouped = sku_grouped.merge(quantity_df, on="sku", how="left")
        sku_grouped["quantity"] = sku_grouped["quantity"].fillna(0)  # In case some SKUs have no 'Order' type


        # === Calculated Metrics ===
        sku_grouped["Net Sales"] = sku_grouped["product_sales"] + sku_grouped["promotional_rebates"] 
        sku_grouped["Net Taxes"] = sku_grouped[[
            "product_sales_tax", "marketplace_facilitator_tax", "shipping_credits_tax",
            "giftwrap_credits_tax", "promotional_rebates_tax", "other_transaction_fees"
        ]].sum(axis=1)
        keywords = [
            "FBA Inventory Reimbursement - Customer Return",
            "FBA Inventory Reimbursement - Customer Service Issue",
            "FBA Inventory Reimbursement - General Adjustment",
            "FBA Inventory Reimbursement - Damaged:Warehouse",
            "FBA Inventory Reimbursement - Lost:Warehouse"
        ]

        # Step 2: Create a mask to filter rows where description contains those keywords
        mask = df["description"].astype(str).str.contains('|'.join(keywords), case=False, na=False)

        # Step 3: Filter matching rows and group by SKU, summing the 'total' column
        sku_net_credits = df[mask].groupby("sku")["total"].sum().abs().reset_index()

        # Step 4: Rename the column to Net Credits
        sku_net_credits.rename(columns={"total": "Net Credits"}, inplace=True)

        # Step 5: Optional – Merge with your full sku_grouped table to include all SKUs
        sku_grouped = pd.merge(sku_grouped, sku_net_credits, on="sku", how="left")
        sku_grouped["Net Credits"] = sku_grouped["Net Credits"].fillna(0)
        sku_grouped["Net Credits"] = (
            sku_grouped["Net Credits"] + 
            sku_grouped["gift_wrap_credits"] + 
            sku_grouped["shipping_credits"]
        )

        # Step 6: Print the result
        sku_grouped["asp"] = (sku_grouped["Net Sales"] / sku_grouped["quantity"].replace(0, np.nan)).fillna(0)
        sku_grouped["amazon_fee"] = (
            abs(sku_grouped["fba_fees"]) + 
            abs(sku_grouped["selling_fees"]) - abs(sku_grouped["other"]) 
        )
        sku_grouped["profit"] = abs(sku_grouped["Net Sales"]) + abs(sku_grouped["Net Credits"]) - abs(sku_grouped["Net Taxes"]) -abs(sku_grouped["amazon_fee"]) - abs(sku_grouped["cost_of_unit_sold"])
        sku_grouped["profit%"] = (
            (sku_grouped["profit"] / sku_grouped["Net Sales"].replace(0, np.nan))   # avoid divide by zero
            .replace([np.inf, -np.inf], 0)      # replace infinite values
            .fillna(0)                          # replace NaN
            * 100                               # finally multiply
        )
        sku_grouped["unit_wise_profitability"] = (sku_grouped["profit"] / sku_grouped["quantity"].replace(0, np.nan)
        ).fillna(0)
        sku_grouped["unit_wise_profitability"] = sku_grouped["unit_wise_profitability"].replace([float('inf'), -float('inf')], 0).fillna(0)
        sku_grouped["user_id"] = user_id
        sku_grouped["platform_fee"] = platform_fee
        sku_grouped["rembursement_fee"] = rembursment_fee
        sku_grouped["advertising_total"] = advertising_total
        sku_grouped["shipment_charges"] = 0

        # Placeholders for row-level metrics
        sku_grouped["reimbursement_vs_sales"] = 0
        sku_grouped["cm2_profit"] = 0
        sku_grouped["cm2_margins"] = 0
        sku_grouped["acos"] = 0
        sku_grouped["rembursment_vs_cm2_margins"] = 0
        # === Total Row Calculations ===
        total_sales = abs(sku_grouped["Net Sales"].sum())
        total_profit = abs(sku_grouped["profit"].sum())
        sku_grouped["profit_mix"] = (sku_grouped["profit"] / total_profit) * 100 if total_profit != 0 else 0
        sku_grouped["sales_mix"] = (sku_grouped["Net Sales"] / total_sales) * 100 if total_sales != 0 else 0
        reimbursement_vs_sales = abs((rembursment_fee / total_sales) * 100) if total_sales else 0
        cm2_profit = total_profit - (abs(advertising_total) + abs(platform_fee) + abs(shipment_charges)) 
        cm2_margins = (cm2_profit / total_sales) * 100 if total_sales else 0
        acos = (advertising_total / total_sales) * 100 if total_sales else 0
        rembursment_vs_cm2_margins = abs((rembursment_fee / cm2_profit) * 100)  if cm2_profit != 0 else 0
        sum_row = sku_grouped[numeric_columns + ["Net Sales", "Net Taxes", "Net Credits", "profit", "amazon_fee"]].sum()
        sum_row["sku"] = "TOTAL"
        sum_row["product_name"] = "TOTAL"
        sum_row["profit%"] = (sum_row["profit"] / sum_row["Net Sales"]) * 100 if sum_row["Net Sales"] != 0 else 0
        sum_row["platform_fee"] = platform_fee
        sum_row["shipment_charges"] = shipment_charges
        sum_row["rembursement_fee"] = rembursment_fee
        sum_row["advertising_total"] = advertising_total
        sum_row["reimbursement_vs_sales"] = reimbursement_vs_sales
        sum_row["cm2_profit"] = cm2_profit
        sum_row["cm2_margins"] = cm2_margins
        sum_row["acos"] = acos
        sum_row["rembursment_vs_cm2_margins"] = rembursment_vs_cm2_margins
        sum_row["asp"] = sum_row["Net Sales"] / sum_row["quantity"] if sum_row["quantity"] != 0 else 0
        sum_row["unit_wise_profitability"] = (
            (sum_row["profit"]) /
            sum_row["quantity"]
        )  if sum_row["quantity"] != 0 else 0
        sum_row["user_id"] = user_id
        sku_grouped = pd.concat([sku_grouped[sku_grouped["sku"] != "TOTAL"], pd.DataFrame([sum_row])], ignore_index=True)

        # Rename columns
        sku_grouped.rename(columns={
            "Net Sales": "net_sales",
            "Net Taxes": "net_taxes",
            "Net Credits": "net_credits",
            "profit%": "profit_percentage"
        }, inplace=True)
        total_row = sku_grouped[sku_grouped['sku'].str.lower() == 'total']
        other_rows = sku_grouped[sku_grouped['sku'].str.lower() != 'total']
        other_rows_sorted = other_rows.sort_values(by="profit", ascending=False)
        sku_grouped = pd.concat([other_rows_sorted, total_row], ignore_index=True)

        with engine.begin() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {yearly_table}"))
        from sqlalchemy.types import Float, Integer, String

        dtype_map = {
            "sku": String,
            "price_in_gbp": Float,
            "product_sales": Float,
            "promotional_rebates": Float,
            "promotional_rebates_tax": Float,
            "product_sales_tax": Float,
            "selling_fees": Float,
            "refund_selling_fees": Float,
            "fba_fees": Float,
            "other": Float,
            "marketplace_facilitator_tax": Float,
            "shipping_credits_tax": Float,
            "giftwrap_credits_tax": Float,
            "shipment_charges": Float,
            "gift_wrap_credits": Float,
            "cost_of_unit_sold": Float,
            "quantity": Integer,
            "total": Float,
            "other_transaction_fees": Float,
            "net_sales": Float,
            "net_taxes": Float,
            "net_credits": Float,
            "profit": Float,
            "profit_percentage": Float,
            "amazon_fee": Float,
            "platform_fee": Float,
            "rembursement_fee": Float,
            "advertising_total": Float,
            "reimbursement_vs_sales": Float,
            "cm2_profit": Float,
            "cm2_margins": Float,
            "acos": Float,
            "rembursment_vs_cm2_margins": Float,
            "asp": Float,
            "product_name": String,
            "unit_wise_profitability": Float,
            "user_id": Integer
        }

        sku_grouped.to_sql(yearly_table, con=engine, if_exists='replace', index=False, dtype=dtype_map)
    except Exception as e:
        print(f"Error processing yearly SKU-wise data: {e}")

    finally:
        conn.close()



def process_us_quarterly_skuwise_data(user_id, country, month, year, quarter, db_url):
    engine = create_engine(db_url)
    conn = engine.connect()

    # Define quarters
    quarter_months = {
        "quarter1": ["january", "february", "march"],
        "quarter2": ["april", "may", "june"],
        "quarter3": ["july", "august", "september"],
        "quarter4": ["october", "november", "december"]
    }

    month = month.lower()
    for quarter, months in quarter_months.items():
        if month in months:
            quarter_table = f"{quarter}_{user_id}_{country}_{year}_table"
            break
    else:
        print("Invalid month provided.")
        return
    

    quarter_table = f"{quarter}_{user_id}_{country}_{year}_table"
    source_table = f"user_{user_id}_{country}_merge_data_of_all_months"

    # Get valid months from the source table
    available_months_query = text(f"""
        SELECT DISTINCT LOWER(month) as month
        FROM {source_table}
        WHERE LOWER(month) IN :months AND year = :year
    """)
    available_months_df = pd.read_sql(available_months_query, conn, params={"months": tuple(months), "year": year})
    selected_months = available_months_df["month"].tolist()

    try:
        if not selected_months:
            print("No valid months found in source data.")
            return

        data_query = text(f"""
            SELECT *
            FROM {source_table}
            WHERE LOWER(month) IN :months AND year = :year
        """)
        df = pd.read_sql(data_query, conn, params={"months": tuple(selected_months), "year": year})

        if df.empty:
            print(f"No data found for {', '.join(selected_months)} in {source_table}")
            return
        

        advertising_total = df[
            (df["sku"].isna()) | (df["sku"] == "") | (df["sku"] == "0")
        ]["other_transaction_fees"].sum()
        
        # === Preprocessing & Conversion ===
        df["total"] = pd.to_numeric(df["total"].astype(str).str.replace(",", ""), errors="coerce")
        df["other"] = pd.to_numeric(df["other"].astype(str).str.replace(",", ""), errors="coerce")

        transfer_df = df[df["type"] == "Transfer"]
        rembursement_fee_desc_sum = abs(transfer_df["total"].sum())
        rembursement_fee_col_sum = df["net_reimbursement"].sum() if "net_reimbursement" in df.columns else 0
        rembursment_fee = rembursement_fee_desc_sum + rembursement_fee_col_sum
        platform_fee_desc_sum = abs(
            df[df["description"].str.startswith(
                ("FBA Return Fee", "FBA Long-Term Storage Fee", "FBA storage fee", "Subscription"),
                na=False
            )]["total"].sum()
        )
        platform_fee_col_sum = df["platform_fees"].sum() if "platform_fees" in df.columns else 0
        platform_fee = platform_fee_desc_sum + platform_fee_col_sum
        # Define the relevant keywords
        # Define the relevant keywords
        shipment_keywords = [
            "FBA international shipping charge",
            "FBA Inbound Placement Service Fee",
            "FBA international shipping customs charge"
        ]
        
        # Filter rows based on whether 'description' contains any of the shipment keywords
        shipment_mask = df["description"].astype(str).str.contains('|'.join(shipment_keywords), case=False, na=False)
        
        # Calculate shipment charges
        shipment_charges = abs(df[shipment_mask]["total"].sum())

        # Normalize numeric columns
        numeric_columns = [
            "product_sales", "promotional_rebates", "product_sales_tax", "promotional_rebates_tax",
            "marketplace_facilitator_tax", "shipping_credits_tax", "giftwrap_credits_tax",
            "quantity", "cost_of_unit_sold", "selling_fees", "fba_fees", "other", 
            "gift_wrap_credits", "price_in_gbp", "total", "other_transaction_fees"
        ]
        for col in numeric_columns:
            df[col] = pd.to_numeric(df[col].astype(str).str.replace(",", ""), errors="coerce").fillna(0)
        
        # shipment_charges = abs(df[df["bucket"].str.startswith(
        # ("Freight & Forward"), na=False)]["total"].sum())

        df["sku"] = df["sku"].astype(str).str.strip()
        df = df[df["sku"].notna() & (df["sku"] != "")]

        df = df[df["sku"].ne("None") & df["sku"].ne("0") & df["sku"].ne("") & df["sku"].notna()]

        # === Refund Fees ===
        refund_fees = df[df["type"] == "Refund"].groupby("sku")["selling_fees"].sum().reset_index()
        refund_fees.rename(columns={"selling_fees": "refund_selling_fees"}, inplace=True)
        refund_fees["sku"] = refund_fees["sku"].astype(str).str.strip()

        df["type_norm"] = df["type"].astype(str).str.strip().str.lower()
        df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce").fillna(0)

        # exact match: 'order' OR 'shipment' (case-insensitive)
        mask = df["type_norm"].isin(["order", "shipment"])

        quantity_df = (
            df[mask]
            .groupby("sku", as_index=False)["quantity"]
            .sum()
        )

        sku_grouped = df.groupby("sku").agg({
            "price_in_gbp": "mean", "product_sales": "sum", "promotional_rebates": "sum",
            "promotional_rebates_tax": "sum", "product_sales_tax": "sum",
            "selling_fees": "sum", "fba_fees": "sum", "other": "sum",
            "marketplace_facilitator_tax": "sum", "shipping_credits_tax": "sum",
            "giftwrap_credits_tax": "sum", "shipping_credits": "sum",
            "gift_wrap_credits": "sum", "cost_of_unit_sold": "sum",
            "total": "sum", "other_transaction_fees": "sum","product_name": "first",
        }).reset_index()

        sku_grouped["sku"] = sku_grouped["sku"].astype(str).str.strip()
        sku_grouped = sku_grouped.merge(refund_fees, on="sku", how="left")
        sku_grouped = sku_grouped[sku_grouped["sku"].astype(str).str.strip().ne("0") & sku_grouped["sku"].notna()]

        sku_grouped = sku_grouped.merge(quantity_df, on="sku", how="left")
        sku_grouped["quantity"] = sku_grouped["quantity"].fillna(0)  # In case some SKUs have no 'Order' type



        # === Calculated Metrics ===
        sku_grouped["Net Sales"] = sku_grouped["product_sales"] + sku_grouped["promotional_rebates"] 
        sku_grouped["Net Taxes"] = sku_grouped[[
            "product_sales_tax", "marketplace_facilitator_tax", "shipping_credits_tax",
            "giftwrap_credits_tax", "promotional_rebates_tax", "other_transaction_fees"
        ]].sum(axis=1)
        keywords = [
            "FBA Inventory Reimbursement - Customer Return",
            "FBA Inventory Reimbursement - Customer Service Issue",
            "FBA Inventory Reimbursement - General Adjustment",
            "FBA Inventory Reimbursement - Damaged:Warehouse",
            "FBA Inventory Reimbursement - Lost:Warehouse"
        ]

        # Step 2: Create a mask to filter rows where description contains those keywords
        mask = df["description"].astype(str).str.contains('|'.join(keywords), case=False, na=False)

        # Step 3: Filter matching rows and group by SKU, summing the 'total' column
        sku_net_credits = df[mask].groupby("sku")["total"].sum().abs().reset_index()

        # Step 4: Rename the column to Net Credits
        sku_net_credits.rename(columns={"total": "Net Credits"}, inplace=True)

        # Step 5: Optional – Merge with your full sku_grouped table to include all SKUs
        sku_grouped = pd.merge(sku_grouped, sku_net_credits, on="sku", how="left")
        sku_grouped["Net Credits"] = sku_grouped["Net Credits"].fillna(0)
        sku_grouped["Net Credits"] = (
            sku_grouped["Net Credits"] + 
            sku_grouped["gift_wrap_credits"] + 
            sku_grouped["shipping_credits"]
        )

        # Step 6: Print the result
        sku_grouped["asp"] = (sku_grouped["Net Sales"] / sku_grouped["quantity"].replace(0, np.nan)).fillna(0)
        sku_grouped["amazon_fee"] = (
            abs(sku_grouped["fba_fees"]) + 
            abs(sku_grouped["selling_fees"]) - abs(sku_grouped["other"]) 
        )
        sku_grouped["profit"] = abs(sku_grouped["Net Sales"]) + abs(sku_grouped["Net Credits"]) - abs(sku_grouped["Net Taxes"]) -abs(sku_grouped["amazon_fee"]) - abs(sku_grouped["cost_of_unit_sold"])
        # sku_grouped["profit%"] = (sku_grouped["profit"] / sku_grouped["Net Sales"]).replace([float('inf'), -float('inf')], 0).fillna(0) * 100
        sku_grouped["profit%"] = (
            (sku_grouped["profit"] / sku_grouped["Net Sales"].replace(0, np.nan))   # avoid divide by zero
            .replace([np.inf, -np.inf], 0)      # replace infinite values
            .fillna(0)                          # replace NaN
            * 100                               # finally multiply
        )

        sku_grouped["unit_wise_profitability"] = (sku_grouped["profit"] / sku_grouped["quantity"].replace(0, np.nan)
        ).fillna(0)
        sku_grouped["unit_wise_profitability"] = sku_grouped["unit_wise_profitability"].replace([float('inf'), -float('inf')], 0).fillna(0)
        sku_grouped["user_id"] = user_id
        sku_grouped["platform_fee"] = platform_fee
        sku_grouped["rembursement_fee"] = rembursment_fee
        sku_grouped["advertising_total"] = advertising_total

        sku_grouped["shipment_charges"] = 0

        # Placeholders for row-level metrics
        sku_grouped["reimbursement_vs_sales"] = 0
        sku_grouped["cm2_profit"] = 0
        sku_grouped["cm2_margins"] = 0
        sku_grouped["acos"] = 0
        sku_grouped["rembursment_vs_cm2_margins"] = 0

        # === Total Row Calculations ===
        total_sales = abs(sku_grouped["Net Sales"].sum())
        total_profit = abs(sku_grouped["profit"].sum())
        sku_grouped["profit_mix"] = ((sku_grouped["profit"] / total_profit) * 100 if total_profit != 0 else 0)
        sku_grouped["sales_mix"] = ((sku_grouped["Net Sales"] / total_sales) * 100 if total_sales != 0 else 0)
        reimbursement_vs_sales = abs((rembursment_fee / total_sales) * 100) if total_sales else 0
        cm2_profit = total_profit - (abs(advertising_total) + abs(platform_fee) + abs(shipment_charges)) 
        cm2_margins = (cm2_profit / total_sales) * 100 if total_sales else 0
        acos = (advertising_total / total_sales) * 100 if total_sales else 0
        rembursment_vs_cm2_margins = abs((rembursment_fee / cm2_profit) * 100) if cm2_profit else 0

        sum_row = sku_grouped[numeric_columns + ["Net Sales", "Net Taxes", "Net Credits", "profit", "amazon_fee"]].sum()
        sum_row["sku"] = "TOTAL"
        sum_row["product_name"] = "TOTAL"
        sum_row["profit%"] = (sum_row["profit"] / sum_row["Net Sales"]) * 100 if sum_row["Net Sales"] != 0 else 0
        sum_row["platform_fee"] = platform_fee
        sum_row["shipment_charges"] = shipment_charges
        sum_row["rembursement_fee"] = rembursment_fee
        sum_row["advertising_total"] = advertising_total
        sum_row["reimbursement_vs_sales"] = reimbursement_vs_sales
        sum_row["cm2_profit"] = cm2_profit
        sum_row["cm2_margins"] = cm2_margins
        sum_row["acos"] = acos
        sum_row["rembursment_vs_cm2_margins"] = rembursment_vs_cm2_margins
        sum_row["asp"] = sum_row["Net Sales"] / sum_row["quantity"] if sum_row["quantity"] != 0 else 0
        sum_row["unit_wise_profitability"] = (
            (sum_row["profit"]) /
            sum_row["quantity"]
        )  if sum_row["quantity"] != 0 else 0
        sum_row["user_id"] = user_id

        sku_grouped = pd.concat([sku_grouped[sku_grouped["sku"] != "TOTAL"], pd.DataFrame([sum_row])], ignore_index=True)

        # Rename columns
        sku_grouped.rename(columns={
            "Net Sales": "net_sales",
            "Net Taxes": "net_taxes",
            "Net Credits": "net_credits",
            "profit%": "profit_percentage"
        }, inplace=True)

        total_row = sku_grouped[sku_grouped['sku'].str.lower() == 'total']
        other_rows = sku_grouped[sku_grouped['sku'].str.lower() != 'total']
        other_rows_sorted = other_rows.sort_values(by="profit", ascending=False)
        sku_grouped = pd.concat([other_rows_sorted, total_row], ignore_index=True)

        with engine.begin() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {quarter_table}"))
        from sqlalchemy.types import Float, Integer, String

        dtype_map = {
            "sku": String,
            "price_in_gbp": Float,
            "product_sales": Float,
            "promotional_rebates": Float,
            "promotional_rebates_tax": Float,
            "product_sales_tax": Float,
            "selling_fees": Float,
            "refund_selling_fees": Float,
            "fba_fees": Float,
            "other": Float,
            "marketplace_facilitator_tax": Float,
            "shipping_credits_tax": Float,
            "giftwrap_credits_tax": Float,
            "shipment_charges": Float,
            "gift_wrap_credits": Float,
            "cost_of_unit_sold": Float,
            "quantity": Integer,
            "total": Float,
            "other_transaction_fees": Float,
            "net_sales": Float,
            "net_taxes": Float,
            "net_credits": Float,
            "profit": Float,
            "profit_percentage": Float,
            "amazon_fee": Float,
            "platform_fee": Float,
            "rembursement_fee": Float,
            "advertising_total": Float,
            "reimbursement_vs_sales": Float,
            "cm2_profit": Float,
            "cm2_margins": Float,
            "acos": Float,
            "rembursment_vs_cm2_margins": Float,
            "asp": Float,
            "product_name": String,
            "unit_wise_profitability": Float,
            "user_id": Integer
        }

        sku_grouped.to_sql(quarter_table, engine, if_exists='replace', index=False, dtype=dtype_map)

    except Exception as e:
        print(f"Error processing yearly SKU-wise data: {e}")

    finally:
        conn.close()

