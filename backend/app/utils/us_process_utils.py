from sqlalchemy import create_engine, text
import os, re
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

_TABLE_COL_CACHE = {}

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
                platform_fee REAL,
                platform_fee_inventory_storage REAL,
                shipment_fees REAL,
                cm2_profit REAL,
                cm2_profit_percentage REAL,
                cm2_margins REAL,
                acos REAL,
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
                platform_fee REAL,
                platform_fee_inventory_storage REAL,

                shipment_charges REAL,
                shipment_fees REAL,

                cm2_profit REAL,
                cm2_profit_percentage REAL,
                cm2_margins REAL,
                acos REAL,

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
        disbursement_total = abs(sum_total_where_desc_contains(df, ["Disbursement"]))
        rembursement_fee_col_sum = safe_series(df, "net_reimbursement").sum()
        rembursement_fee = disbursement_total + rembursement_fee_col_sum

        shipment_keywords = [
            "FBA international shipping charge",
            "FBA Inbound Placement Service Fee",
            "FBA international shipping customs charge",
        ]
        shipment_mask = df["desc_norm"].str.contains("|".join(shipment_keywords), case=False, na=False)
        shipment_charges = abs(safe_series(df.loc[shipment_mask], "total").sum())
        shipment_fees = abs(sum_total_where_desc_contains(df, ["FBAInboundConvenience"]))
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
            return_qty_df["return_quantity"] = pd.to_numeric(return_qty_df["return_quantity"], errors="coerce").fillna(0).abs()

        # ---------- lost / misc ----------
        LOST_DESCRIPTIONS = {
            "REVERSAL_REIMBURSEMENT",
            "WAREHOUSE_LOST",
            "WAREHOUSE_DAMAGE",
            "MISSING_FROM_INBOUND",
            "MISSING_FROM_INBOUND_CLAWBACK",
            "COMPENSATED_CLAWBACK",
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
            "Subscription",
            "FBAInboundConvenience",

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
            "MISSING_FROM_INBOUND_CLAWBACK",
            "COMPENSATED_CLAWBACK",
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

        # Logic 1: TOTAL misc_transaction
        # Includes rows with SKU and rows without SKU.
        misc_transaction_total = abs(
            pd.to_numeric(df.loc[leftout_mask, "total"], errors="coerce")
            .fillna(0.0)
            .sum()
        )

        # Logic 2: SKU-wise misc_transaction
        # Only rows with SKU can merge into SKU table.
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
        ).fillna(0.0).abs()


        platformfeenew_total = abs(sum_total_where_desc_contains(df, ["Subscription"]))

        platform_fee_inventory_storage_total = abs(sum_total_where_desc_contains(df, [
            "FBA Return Fee",
            "FBA Long-Term Storage Fee",
            "FBA storage fee",
            "FBADisposal",
            "FBAStorageBilling",
            "FBALongTermStorageBilling",
            "INCORRECT_FEES_NON_ITEMIZED",
            "StorageReservationBilling",
        ]))


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
            "fba_fees": "sum",
            "other": "sum",
            "marketplace_facilitator_tax": "sum",
            "shipping_credits_tax": "sum",
            "giftwrap_credits_tax": "sum",
            "postage_credits": "sum",
            "shipping_credits": "sum",
            "gift_wrap_credits": "sum",
            "cost_of_unit_sold": "sum",
            "total": "sum",
            "product_name": "first",
        }
        group_cols = {k: v for k, v in group_cols.items() if k in df.columns}

        sku_grouped = df_valid.groupby("sku").agg(group_cols).reset_index()
        sku_grouped["sku"] = sku_grouped["sku"].astype(str).str.strip()

        sku_grouped = sku_grouped.merge(df_prev, on="sku", how="left").fillna(0)
        sku_grouped = sku_grouped.merge(refund_fees, on="sku", how="left")
        sku_grouped = sku_grouped.merge(quantity_df, on="sku", how="left")
        sku_grouped = sku_grouped.merge(return_qty_df, on="sku", how="left")
        sku_grouped = sku_grouped.merge(lost_total_df, on="sku", how="left")
        sku_grouped = sku_grouped.merge(misc_transaction_df, on="sku", how="left")
        sku_grouped["refund_selling_fees"] = pd.to_numeric(sku_grouped.get("refund_selling_fees", 0), errors="coerce").fillna(0)
        sku_grouped["quantity"] = pd.to_numeric(sku_grouped.get("quantity", 0), errors="coerce").fillna(0)
        sku_grouped["return_quantity"] = pd.to_numeric(sku_grouped.get("return_quantity", 0), errors="coerce").fillna(0).astype(int)
        sku_grouped["lost_total"] = pd.to_numeric(sku_grouped.get("lost_total", 0), errors="coerce").fillna(0)
        sku_grouped["misc_transaction"] = pd.to_numeric(sku_grouped.get("misc_transaction", 0), errors="coerce").fillna(0)

        sku_grouped["total_quantity"] = (
            pd.to_numeric(sku_grouped["quantity"], errors="coerce").fillna(0).astype(int)
            - sku_grouped["return_quantity"]
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

        # ---------- sales / tax / credits ----------
        sku_grouped["Net Sales"] = 0
        sku_grouped["rembursement_fee"] = 0
        sku_grouped["shipment_fees"] = 0
        sku_grouped["visible_ads"] = 0
        sku_grouped["dealsvouchar_ads"] = 0
        sku_grouped["advertising_total"] = 0
        sku_grouped["platformfeenew"] = 0
        sku_grouped["platform_fee_inventory_storage"] = 0
        sku_grouped["platform_fee"] = 0
        sku_grouped["acos"] = 0

        sku_grouped["Net Taxes"] = (
            pd.to_numeric(sku_grouped.get("marketplace_facilitator_tax", 0), errors="coerce").fillna(0)
            + pd.to_numeric(sku_grouped.get("shipping_credits_tax", 0), errors="coerce").fillna(0)
        )

        sku_grouped["Net Taxes"] = sku_grouped["Net Taxes"].apply(
            lambda x: 0 if abs(x) < 1e-10 else x
        )

        credit_keywords = [
            "FBA Inventory Reimbursement - Customer Return",
            "FBA Inventory Reimbursement - Customer Service Issue",
            "FBA Inventory Reimbursement - General Adjustment",
            "FBA Inventory Reimbursement - Damaged:Warehouse",
            "FBA Inventory Reimbursement - Lost:Warehouse"
        ]
        credit_mask = df["desc_norm"].str.contains("|".join(credit_keywords), case=False, na=False)
        sku_net_credits = (
            df.loc[credit_mask]
            .groupby("sku")["total"]
            .sum()
            .abs()
            .reset_index()
            .rename(columns={"total": "Net Credits"})
        )

        sku_grouped = sku_grouped.merge(sku_net_credits, on="sku", how="left")
        sku_grouped["Net Credits"] = pd.to_numeric(sku_grouped.get("Net Credits", 0), errors="coerce").fillna(0)
        sku_grouped["Net Credits"] = (
            sku_grouped["Net Credits"]
            + pd.to_numeric(sku_grouped.get("gift_wrap_credits", 0), errors="coerce").fillna(0)
            + pd.to_numeric(sku_grouped.get("shipping_credits", 0), errors="coerce").fillna(0)
        )

        # ---------- refund sales ----------
        refund_sales_df = (
            df.loc[
                (df["type_norm"] == "refund")
                & df["sku"].notna()
                & (df["sku"].astype(str).str.strip() != "")
                & (df["sku"].astype(str).str.strip() != "0")
                & (df["sku"].astype(str).str.strip().str.lower() != "none"),
                ["sku", "product_sales"]
            ]
            .copy()
        )

        refund_sales_df["sku"] = refund_sales_df["sku"].astype(str).str.strip()

        refund_sales_df["product_sales"] = pd.to_numeric(
            refund_sales_df["product_sales"],
            errors="coerce"
        ).fillna(0)

        refund_sales_df = (
            refund_sales_df
            .groupby("sku", as_index=False)["product_sales"]
            .sum()
            .rename(columns={"product_sales": "refund_sales"})
        )

        refund_sales_df["refund_sales"] = refund_sales_df["refund_sales"].abs()

        sku_grouped["sku"] = sku_grouped["sku"].astype(str).str.strip()

        sku_grouped = sku_grouped.merge(
            refund_sales_df,
            on="sku",
            how="left"
        )

        sku_grouped["refund_sales"] = pd.to_numeric(
            sku_grouped["refund_sales"],
            errors="coerce"
        ).fillna(0)

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

        sku_grouped["tex_and_credits"] = (
            pd.to_numeric(sku_grouped.get("product_sales_tax", 0), errors="coerce").fillna(0)
            + pd.to_numeric(sku_grouped.get("postage_credits", 0), errors="coerce").fillna(0)
            + pd.to_numeric(sku_grouped.get("gift_wrap_credits", 0), errors="coerce").fillna(0)
            + pd.to_numeric(sku_grouped.get("giftwrap_credits_tax", 0), errors="coerce").fillna(0)
            + pd.to_numeric(sku_grouped.get("promotional_rebates_tax", 0), errors="coerce").fillna(0)
        )
        sku_grouped["Net Sales"] = (
            pd.to_numeric(sku_grouped["gross_sales"], errors="coerce").fillna(0)
            - pd.to_numeric(sku_grouped["refund_sales"], errors="coerce").fillna(0)
            - pd.to_numeric(sku_grouped["tex_and_credits"], errors="coerce").fillna(0)
        )
        sku_grouped["other_transaction_fees"] = (
            pd.to_numeric(sku_grouped["Net Taxes"], errors="coerce").fillna(0).abs()
            - pd.to_numeric(sku_grouped["Net Credits"], errors="coerce").fillna(0)
        )

        sku_grouped["amazon_fee"] = (
            abs(pd.to_numeric(sku_grouped.get("fba_fees", 0), errors="coerce").fillna(0))
            + abs(pd.to_numeric(sku_grouped.get("selling_fees", 0), errors="coerce").fillna(0))
            - abs(pd.to_numeric(sku_grouped.get("other", 0), errors="coerce").fillna(0))
        )

        sku_grouped["price_in_gbp"] = pd.to_numeric(sku_grouped.get("price_in_gbp", 0), errors="coerce").fillna(0)
        sku_grouped["cost_of_unit_sold"] = sku_grouped["price_in_gbp"] * sku_grouped["total_quantity"]

        sku_grouped["profit"] = (
            pd.to_numeric(sku_grouped["Net Sales"], errors="coerce").fillna(0)
            - pd.to_numeric(sku_grouped["cost_of_unit_sold"], errors="coerce").fillna(0).abs()
            - pd.to_numeric(sku_grouped["amazon_fee"], errors="coerce").fillna(0).abs()
            # - pd.to_numeric(sku_grouped["Net Taxes"], errors="coerce").fillna(0).abs()
            + pd.to_numeric(sku_grouped["Net Credits"], errors="coerce").fillna(0)
        )

        sku_grouped["profit%"] = (sku_grouped["profit"] / sku_grouped["Net Sales"]) * 100
        sku_grouped["profit%"] = sku_grouped["profit%"].replace([np.inf, -np.inf], 0).fillna(0)

        # ---------- breakup columns sku-wise ----------
        shipment_charges_df = sku_sum_total_where_desc_contains(df, shipment_keywords, "shipment_charges")

        for subdf in [
            shipment_charges_df,
        ]:
            sku_grouped = sku_grouped.merge(subdf, on="sku", how="left")

        for col in [
            "visible_ads",
            "dealsvouchar_ads",
            "platformfeenew",
            "platform_fee_inventory_storage",
            "shipment_charges",
        ]:
            sku_grouped[col] = pd.to_numeric(sku_grouped.get(col, 0), errors="coerce").fillna(0.0)

        sku_grouped["advertising_total"] = sku_grouped["visible_ads"].abs() + sku_grouped["dealsvouchar_ads"].abs()
        sku_grouped["platform_fee"] = (
            sku_grouped["platformfeenew"].abs()
            + sku_grouped["platform_fee_inventory_storage"].abs()
            - pd.to_numeric(sku_grouped["lost_total"], errors="coerce").fillna(0.0).abs()
            - pd.to_numeric(sku_grouped["misc_transaction"], errors="coerce").fillna(0.0).abs()
        )
        sku_grouped["reimbursement_vs_sales"] = np.where(
            sku_grouped["Net Sales"] != 0,
            (sku_grouped["rembursement_fee"] / sku_grouped["Net Sales"]) * 100,
            0
        )
        sku_grouped["cm2_profit"] = (
            sku_grouped["profit"]
            - sku_grouped["advertising_total"]
            - sku_grouped["platform_fee"]
            - sku_grouped["shipment_charges"]
            - sku_grouped["shipment_fees"]
        )
        sku_grouped["cm2_margins"] = np.where(
            sku_grouped["Net Sales"] != 0,
            (sku_grouped["cm2_profit"] / sku_grouped["Net Sales"]) * 100,
            0
        )
        sku_grouped["acos"] = 0.0
        sku_grouped["rembursment_vs_cm2_margins"] = np.where(
            sku_grouped["cm2_profit"] != 0,
            (sku_grouped["rembursement_fee"] / sku_grouped["cm2_profit"]) * 100,
            0
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
        sku_grouped["asp"] = sku_grouped["Net Sales"] / sku_grouped["quantity"]
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

        total_sales = abs(sku_grouped["Net Sales"].sum())
        total_profit = abs(sku_grouped["profit"].sum())
        total_previous_profit = abs(sku_grouped["previous_profit"].sum())
        total_previous_sales = abs(sku_grouped["previous_net_sales"].sum())
        total_amazon_fee = abs(sku_grouped["amazon_fee"].sum())
        total_cous = abs(sku_grouped["cost_of_unit_sold"].sum())
        total_fba_fees = abs(pd.to_numeric(sku_grouped.get("fba_fees", 0), errors="coerce").fillna(0).sum())

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
        total_product_sales = abs(pd.to_numeric(sku_grouped.get("product_sales", 0), errors="coerce").fillna(0).sum())
        total_tax = abs(sku_grouped["Net Taxes"].sum())
        total_credits = abs(sku_grouped["Net Credits"].sum())

        reimbursement_vs_sales = abs((rembursement_fee / total_sales) * 100) if total_sales != 0 else 0
        cm2_profit = total_profit - (
            abs(advertising_total)
            + abs(platform_fee)
            + abs(sku_grouped["shipment_charges"].sum())
            + abs(shipment_fees)
        )
        cm2_margins = (cm2_profit / total_sales) * 100 if total_sales != 0 else 0
        acos = (advertising_total / total_sales) * 100 if total_sales != 0 else 0
        rembursment_vs_cm2_margins = abs((rembursement_fee / cm2_profit) * 100) if cm2_profit != 0 else 0
        sku_grouped["cm2_profit_percentage"] = np.where(
            sku_grouped["Net Sales"] != 0,
            (sku_grouped["cm2_profit"] / sku_grouped["Net Sales"]) * 100,
            0
        )

        sku_grouped["cm2_profit_percentage"] = (
            sku_grouped["cm2_profit_percentage"]
            .replace([np.inf, -np.inf], 0)
            .fillna(0)
        )

        # ---------- total row ----------
        sum_row = sku_grouped.select_dtypes(include=[np.number]).sum()
        sum_row["sku"] = "TOTAL"
        sum_row["month"] = month
        sum_row["country"] = country
        sum_row["year"] = year
        sum_row["product_name"] = "TOTAL"

        sum_row["profit%"] = (
            sum_row["profit"] / sum_row["Net Sales"]
        ) * 100 if sum_row["Net Sales"] != 0 else 0
        sum_row["shipment_charges"] = float(sku_grouped["shipment_charges"].sum())
        sum_row["shipment_fees"] = abs(shipment_fees)
        sum_row["rembursement_fee"] = abs(rembursement_fee)
        sum_row["visible_ads"] = visible_ads_total
        sum_row["dealsvouchar_ads"] = dealsvouchar_ads_total
        sum_row["advertising_total"] = advertising_total
        sum_row["platformfeenew"] = platformfeenew_total
        sum_row["platform_fee_inventory_storage"] = platform_fee_inventory_storage_total
        sum_row["misc_transaction"] = misc_transaction_total
        sum_row["lost_total"] = -lost_total_amount
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
        sum_row["asp"] = (sum_row["Net Sales"] / sum_row["quantity"]) if sum_row["quantity"] != 0 else 0
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
            "shipment_charges": "shipment_charges"
        })
        # Final safety: selling_fees must always be negative before DB/export
        sku_grouped["selling_fees"] = -pd.to_numeric(
            sku_grouped["selling_fees"],
            errors="coerce"
        ).fillna(0).abs()
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

            "platformfeenew", "platform_fee", "platform_fee_inventory_storage",

            "shipment_charges", "shipment_fees",

            "cm2_profit", "cm2_profit_percentage", "cm2_margins", "acos",

            "rembursement_fee", "rembursment_vs_cm2_margins",
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

        for col in US_NSE_FULL_COLUMNS:
            if col in TEXT_COLS_US_NSE:
                df_nse_full[col] = df_nse_full[col].astype(str).fillna("")
            else:
                df_nse_full[col] = pd.to_numeric(df_nse_full[col], errors="coerce").fillna(0)

        df_nse_full = df_nse_full[US_NSE_FULL_COLUMNS]

        final_columns = [
            "sku", "product_name", "quantity", "return_quantity", "total_quantity",
            "asp", "gross_sales", "refund_sales", "tex_and_credits", "net_sales",
            "promotional_rebates", "promotional_rebates_percentage",
            "cost_of_unit_sold", "selling_fees", "fba_fees", "amazon_fee",
            "net_taxes", "net_credits", "misc_transaction", "other_transaction_fees",
            "profit", "unit_wise_profitability", "profit_percentage",
            "visible_ads", "dealsvouchar_ads", "advertising_total", "lost_total",
            "platformfeenew", "platform_fee", "platform_fee_inventory_storage","shipment_fees",
            "cm2_profit", "cm2_profit_percentage","cm2_margins", "acos", "rembursement_fee",
            "rembursment_vs_cm2_margins", "reimbursement_vs_sales",
            "sales_mix", "profit_mix","month", "year", "country", "user_id"
        ]

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
            "promotional_rebates", "promotional_rebates_percentage",
            "cost_of_unit_sold", "selling_fees", "fba_fees", "amazon_fee",
            "net_taxes", "net_credits", "misc_transaction", "other_transaction_fees",
            "profit", "unit_wise_profitability", "profit_percentage",
            "visible_ads", "dealsvouchar_ads", "advertising_total", "lost_total",
            "platformfeenew", "platform_fee", "platform_fee_inventory_storage","shipment_fees",
            "cm2_profit", "cm2_profit_percentage","cm2_margins", "acos", "rembursement_fee",
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

        otherwplatform = abs(sum_row["platform_fee"]) + abs(sum_row["rembursement_fee"]) + abs(sum_row["shipment_charges"])
        taxncredit = abs(sum_row["tex_and_credits"])
        total_expense = abs(total_cous) + abs(total_amazon_fee) + abs(sum_row["platform_fee"]) + abs(sum_row["advertising_total"]) + abs(sum_row["shipment_charges"])
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

        disbursement_total = abs(sum_total_where_desc_contains(df, ["Disbursement"]))
        rembursement_fee = disbursement_total + safe_series(df, "net_reimbursement").sum()

        shipment_keywords = [
            "FBA international shipping charge",
            "FBA Inbound Placement Service Fee",
            "FBA international shipping customs charge",
        ]
        shipment_mask = df["desc_norm"].str.contains("|".join(shipment_keywords), case=False, na=False)
        shipment_charges = abs(safe_series(df.loc[shipment_mask], "total").sum())
        shipment_fees = abs(sum_total_where_desc_contains(df, ["FBAInboundConvenience"]))

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
            "MISSING_FROM_INBOUND_CLAWBACK",
            "COMPENSATED_CLAWBACK",
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
            "Subscription",
            "FBAInboundConvenience",

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
            "MISSING_FROM_INBOUND_CLAWBACK",
            "COMPENSATED_CLAWBACK",
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

        # Logic 1: TOTAL misc_transaction
        # Includes rows with SKU and rows without SKU.
        misc_transaction_total = abs(
            pd.to_numeric(df.loc[leftout_mask, "total"], errors="coerce")
            .fillna(0.0)
            .sum()
        )

        # Logic 2: SKU-wise misc_transaction
        # Only rows with SKU can merge into SKU table.
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
        ).fillna(0.0).abs()

        platformfeenew_total = abs(sum_total_where_desc_contains(df, ["Subscription"]))

        platform_fee_inventory_storage_total = abs(sum_total_where_desc_contains(df, [
            "FBA Return Fee",
            "FBA Long-Term Storage Fee",
            "FBA storage fee",
            "FBADisposal",
            "FBAStorageBilling",
            "FBALongTermStorageBilling",
            "INCORRECT_FEES_NON_ITEMIZED",
            "StorageReservationBilling",
        ]))


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
            "fba_fees": "sum",
            "other": "sum",
            "marketplace_facilitator_tax": "sum",
            "shipping_credits_tax": "sum",
            "giftwrap_credits_tax": "sum",
            "postage_credits": "sum",
            "shipping_credits": "sum",
            "gift_wrap_credits": "sum",
            "cost_of_unit_sold": "sum",
            "total": "sum",
            "product_name": "first",
        }
        group_cols = {k: v for k, v in group_cols.items() if k in df.columns}

        sku_grouped = df_valid.groupby("sku").agg(group_cols).reset_index()
        sku_grouped["sku"] = sku_grouped["sku"].astype(str).str.strip()

        sku_grouped = sku_grouped.merge(refund_fees, on="sku", how="left")
        sku_grouped = sku_grouped.merge(quantity_df, on="sku", how="left")
        sku_grouped = sku_grouped.merge(return_qty_df, on="sku", how="left")
        sku_grouped = sku_grouped.merge(lost_total_df, on="sku", how="left")
        sku_grouped = sku_grouped.merge(misc_transaction_df, on="sku", how="left")

        sku_grouped["refund_selling_fees"] = safe_series(sku_grouped, "refund_selling_fees")
        sku_grouped["quantity"] = safe_series(sku_grouped, "quantity")
        sku_grouped["return_quantity"] = safe_series(sku_grouped, "return_quantity").abs().astype(int)
        sku_grouped["lost_total"] = safe_series(sku_grouped, "lost_total")
        sku_grouped["misc_transaction"] = safe_series(sku_grouped, "misc_transaction")

        sku_grouped["total_quantity"] = (
            sku_grouped["quantity"].fillna(0).astype(int)
            - sku_grouped["return_quantity"]
        ).astype(int)

        sku_grouped["selling_fees"] = safe_series(sku_grouped, "selling_fees")
        sku_grouped["selling_fees"] -= 2 * sku_grouped["refund_selling_fees"]

        sku_grouped["Net Sales"] = 0
        sku_grouped["asp"] = 0
        sku_grouped["promotional_rebates_percentage"] = 0

        sku_grouped["rembursement_fee"] = 0
        sku_grouped["shipment_fees"] = 0
        sku_grouped["visible_ads"] = 0
        sku_grouped["dealsvouchar_ads"] = 0
        sku_grouped["advertising_total"] = 0
        sku_grouped["platformfeenew"] = 0
        sku_grouped["platform_fee_inventory_storage"] = 0
        sku_grouped["platform_fee"] = 0
        sku_grouped["acos"] = 0

        sku_grouped["net_taxes"] = (
            safe_series(sku_grouped, "marketplace_facilitator_tax")
            + safe_series(sku_grouped, "shipping_credits_tax")
        )

        sku_grouped["net_taxes"] = sku_grouped["net_taxes"].apply(
            lambda x: 0 if abs(x) < 1e-10 else x
        )

        credit_keywords = [
            "FBA Inventory Reimbursement - Customer Return",
            "FBA Inventory Reimbursement - Customer Service Issue",
            "FBA Inventory Reimbursement - General Adjustment",
            "FBA Inventory Reimbursement - Damaged:Warehouse",
            "FBA Inventory Reimbursement - Lost:Warehouse"
        ]
        credit_mask = df["desc_norm"].str.contains("|".join(credit_keywords), case=False, na=False)
        sku_net_credits = (
            df.loc[credit_mask]
            .groupby("sku")["total"]
            .sum()
            .abs()
            .reset_index()
            .rename(columns={"total": "net_credits"})
        )

        sku_grouped = sku_grouped.merge(sku_net_credits, on="sku", how="left")
        sku_grouped["net_credits"] = (
            safe_series(sku_grouped, "net_credits")
            + safe_series(sku_grouped, "gift_wrap_credits")
            + safe_series(sku_grouped, "shipping_credits")
        )

        # ---------- refund sales ----------
        refund_sales_df = (
            df.loc[
                (df["type_norm"] == "refund")
                & df["sku"].notna()
                & (df["sku"].astype(str).str.strip() != "")
                & (df["sku"].astype(str).str.strip() != "0")
                & (df["sku"].astype(str).str.strip().str.lower() != "none"),
                ["sku", "product_sales"]
            ]
            .copy()
        )

        refund_sales_df["sku"] = refund_sales_df["sku"].astype(str).str.strip()

        refund_sales_df["product_sales"] = pd.to_numeric(
            refund_sales_df["product_sales"],
            errors="coerce"
        ).fillna(0)

        refund_sales_df = (
            refund_sales_df
            .groupby("sku", as_index=False)["product_sales"]
            .sum()
            .rename(columns={"product_sales": "refund_sales"})
        )

        refund_sales_df["refund_sales"] = refund_sales_df["refund_sales"].abs()

        sku_grouped["sku"] = sku_grouped["sku"].astype(str).str.strip()

        sku_grouped = sku_grouped.merge(
            refund_sales_df,
            on="sku",
            how="left"
        )

        sku_grouped["refund_sales"] = pd.to_numeric(
            sku_grouped["refund_sales"],
            errors="coerce"
        ).fillna(0)

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

        sku_grouped["tex_and_credits"] = (
            safe_series(sku_grouped, "product_sales_tax")
            + safe_series(sku_grouped, "postage_credits")
            + safe_series(sku_grouped, "gift_wrap_credits")
            + safe_series(sku_grouped, "giftwrap_credits_tax")
            + safe_series(sku_grouped, "promotional_rebates_tax")
        )
        sku_grouped["Net Sales"] = (
            safe_series(sku_grouped, "gross_sales")
            - safe_series(sku_grouped, "refund_sales")
            - safe_series(sku_grouped, "tex_and_credits")
        )
        sku_grouped["asp"] = np.where(
            sku_grouped["quantity"] != 0,
            sku_grouped["Net Sales"] / sku_grouped["quantity"],
            0
        )
        sku_grouped["asp"] = sku_grouped["asp"].replace([np.inf, -np.inf], 0).fillna(0)

        sku_grouped["promotional_rebates_percentage"] = np.where(
            sku_grouped["Net Sales"] != 0,
            (safe_series(sku_grouped, "promotional_rebates") / sku_grouped["Net Sales"]) * 100,
            0
        )
        sku_grouped["promotional_rebates_percentage"] = (
            sku_grouped["promotional_rebates_percentage"]
            .replace([np.inf, -np.inf], 0)
            .fillna(0)
        )

        sku_grouped["other_transaction_fees"] = (
            sku_grouped["net_taxes"].abs()
            - sku_grouped["net_credits"]
        )

        sku_grouped["amazon_fee"] = (
            safe_series(sku_grouped, "fba_fees").abs()
            + safe_series(sku_grouped, "selling_fees").abs()
            - safe_series(sku_grouped, "other").abs()
        )

        sku_grouped["price_in_gbp"] = safe_series(sku_grouped, "price_in_gbp")
        sku_grouped["cost_of_unit_sold"] = sku_grouped["price_in_gbp"] * sku_grouped["total_quantity"]

        sku_grouped["profit"] = (
            safe_series(sku_grouped, "Net Sales")
            - safe_series(sku_grouped, "cost_of_unit_sold").abs()
            - safe_series(sku_grouped, "amazon_fee").abs()
            # - safe_series(sku_grouped, "net_taxes").abs()
            + safe_series(sku_grouped, "net_credits")
        )

        sku_grouped["profit_percentage"] = np.where(
            sku_grouped["Net Sales"] != 0,
            (sku_grouped["profit"] / sku_grouped["Net Sales"]) * 100,
            0
        )
        sku_grouped["profit_percentage"] = sku_grouped["profit_percentage"].replace([np.inf, -np.inf], 0).fillna(0)

        shipment_charges_df = sku_sum_total_where_desc_contains(df, shipment_keywords, "shipment_charges")
        sku_grouped = sku_grouped.merge(shipment_charges_df, on="sku", how="left")

        sku_grouped["shipment_charges"] = safe_series(sku_grouped, "shipment_charges")
        sku_grouped["advertising_total"] = 0

        sku_grouped["platformfeenew"] = safe_series(sku_grouped, "platformfeenew")
        sku_grouped["platform_fee_inventory_storage"] = safe_series(sku_grouped, "platform_fee_inventory_storage")

        sku_grouped["platform_fee"] = (
            sku_grouped["platformfeenew"].abs()
            + sku_grouped["platform_fee_inventory_storage"].abs()
            - safe_series(sku_grouped, "lost_total").abs()
            - safe_series(sku_grouped, "misc_transaction").abs()
        )

        sku_grouped["reimbursement_vs_sales"] = 0
        sku_grouped["cm2_profit"] = (
            sku_grouped["profit"]
            - sku_grouped["advertising_total"]
            - sku_grouped["platform_fee"]
            - sku_grouped["shipment_charges"]
            - sku_grouped["shipment_fees"]
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
        cm2_profit = total_profit - (
            abs(advertising_total)
            + abs(platform_fee)
            + abs(sku_grouped["shipment_charges"].sum())
            + abs(shipment_fees)
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

        sum_row["profit_percentage"] = (
            (sum_row["profit"] / sum_row["net_sales"]) * 100
            if sum_row["net_sales"] != 0 else 0
        )
        sum_row["shipment_charges"] = float(sku_grouped["shipment_charges"].sum())
        sum_row["shipment_fees"] = abs(shipment_fees)
        sum_row["rembursement_fee"] = abs(rembursement_fee)
        sum_row["visible_ads"] = visible_ads_total
        sum_row["dealsvouchar_ads"] = dealsvouchar_ads_total
        sum_row["advertising_total"] = advertising_total
        sum_row["platformfeenew"] = platformfeenew_total
        sum_row["platform_fee_inventory_storage"] = platform_fee_inventory_storage_total
        sum_row["misc_transaction"] = misc_transaction_total
        sum_row["lost_total"] = -lost_total_amount
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
            sum_row["net_sales"] / sum_row["quantity"]
            if sum_row["quantity"] != 0 else 0
        )
        sum_row["unit_wise_profitability"] = (
            sum_row["profit"] / sum_row["quantity"]
            if sum_row["quantity"] != 0 else 0
        )

        sku_grouped["year"] = str(year)
        sku_grouped["country"] = country
        sku_grouped["user_id"] = user_id

        sku_grouped = pd.concat([sku_grouped, pd.DataFrame([sum_row])], ignore_index=True)

        final_columns = [
            "sku", "product_name", "quantity", "return_quantity", "total_quantity",
            "asp", "gross_sales", "refund_sales", "tex_and_credits", "net_sales",
            "promotional_rebates", "promotional_rebates_percentage",
            "cost_of_unit_sold", "selling_fees", "fba_fees", "amazon_fee",
            "net_taxes", "net_credits", "misc_transaction", "other_transaction_fees",
            "profit", "unit_wise_profitability", "profit_percentage",
            "visible_ads", "dealsvouchar_ads", "advertising_total", "lost_total",
            "platformfeenew", "platform_fee", "platform_fee_inventory_storage",
            "shipment_fees", "cm2_profit", "cm2_profit_percentage",
            "cm2_margins", "acos", "rembursement_fee",
            "rembursment_vs_cm2_margins", "reimbursement_vs_sales",
            "sales_mix", "profit_mix", "year", "country", "user_id"
        ]

        for col in final_columns:
            if col not in sku_grouped.columns:
                sku_grouped[col] = 0

        sku_grouped = sku_grouped[final_columns]

        for col in sku_grouped.columns:
            if pd.api.types.is_numeric_dtype(sku_grouped[col]):
                sku_grouped[col] = pd.to_numeric(sku_grouped[col], errors="coerce").fillna(0)
            else:
                sku_grouped[col] = sku_grouped[col].fillna("")

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
            "platformfeenew": Float,
            "platform_fee": Float,
            "platform_fee_inventory_storage": Float,
            "shipment_fees": Float,
            "cm2_profit": Float,
            "cm2_profit_percentage": Float,
            "cm2_margins": Float,
            "acos": Float,
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

        disbursement_total = abs(sum_total_where_desc_contains(df, ["Disbursement"]))
        rembursement_fee = disbursement_total + safe_series(df, "net_reimbursement").sum()

        shipment_keywords = [
            "FBA international shipping charge",
            "FBA Inbound Placement Service Fee",
            "FBA international shipping customs charge",
        ]
        shipment_mask = df["desc_norm"].str.contains("|".join(shipment_keywords), case=False, na=False)
        shipment_charges = abs(safe_series(df.loc[shipment_mask], "total").sum())
        shipment_fees = abs(sum_total_where_desc_contains(df, ["FBAInboundConvenience"]))

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
            "MISSING_FROM_INBOUND_CLAWBACK",
            "COMPENSATED_CLAWBACK",
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
            "Subscription",
            "FBAInboundConvenience",

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
            "MISSING_FROM_INBOUND_CLAWBACK",
            "COMPENSATED_CLAWBACK",
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

        # Logic 1: TOTAL misc_transaction
        # Includes rows with SKU and rows without SKU.
        misc_transaction_total = abs(
            pd.to_numeric(df.loc[leftout_mask, "total"], errors="coerce")
            .fillna(0.0)
            .sum()
        )

        # Logic 2: SKU-wise misc_transaction
        # Only rows with SKU can merge into SKU table.
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
        ).fillna(0.0).abs()

        platformfeenew_total = abs(sum_total_where_desc_contains(df, ["Subscription"]))
        platform_fee_inventory_storage_total = abs(sum_total_where_desc_contains(df, [
            "FBA Return Fee",
            "FBA Long-Term Storage Fee",
            "FBA storage fee",
            "FBADisposal",
            "FBAStorageBilling",
            "FBALongTermStorageBilling",
            "INCORRECT_FEES_NON_ITEMIZED",
            "StorageReservationBilling",
        ]))

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
            "fba_fees": "sum",
            "other": "sum",
            "marketplace_facilitator_tax": "sum",
            "shipping_credits_tax": "sum",
            "giftwrap_credits_tax": "sum",
            "postage_credits": "sum",
            "shipping_credits": "sum",
            "gift_wrap_credits": "sum",
            "cost_of_unit_sold": "sum",
            "total": "sum",
            "product_name": "first",
        }
        group_cols = {k: v for k, v in group_cols.items() if k in df.columns}

        sku_grouped = df_valid.groupby("sku").agg(group_cols).reset_index()
        sku_grouped["sku"] = sku_grouped["sku"].astype(str).str.strip()

        sku_grouped = sku_grouped.merge(refund_fees, on="sku", how="left")
        sku_grouped = sku_grouped.merge(quantity_df, on="sku", how="left")
        sku_grouped = sku_grouped.merge(return_qty_df, on="sku", how="left")
        sku_grouped = sku_grouped.merge(lost_total_df, on="sku", how="left")
        sku_grouped = sku_grouped.merge(misc_transaction_df, on="sku", how="left")

        sku_grouped["refund_selling_fees"] = safe_series(sku_grouped, "refund_selling_fees")
        sku_grouped["quantity"] = safe_series(sku_grouped, "quantity")
        sku_grouped["return_quantity"] = safe_series(sku_grouped, "return_quantity").abs().astype(int)
        sku_grouped["lost_total"] = safe_series(sku_grouped, "lost_total")
        sku_grouped["misc_transaction"] = safe_series(sku_grouped, "misc_transaction")

        sku_grouped["total_quantity"] = (
            sku_grouped["quantity"].fillna(0).astype(int)
            - sku_grouped["return_quantity"]
        ).astype(int)

        sku_grouped["selling_fees"] = -(
            safe_series(sku_grouped, "selling_fees").abs()
            + safe_series(sku_grouped, "refund_selling_fees").abs()
        )

        sku_grouped["Net Sales"] = 0
        sku_grouped["asp"] = 0
        sku_grouped["promotional_rebates_percentage"] = 0

        sku_grouped["rembursement_fee"] = 0
        sku_grouped["shipment_fees"] = 0
        sku_grouped["visible_ads"] = 0
        sku_grouped["dealsvouchar_ads"] = 0
        sku_grouped["advertising_total"] = 0
        sku_grouped["platformfeenew"] = 0
        sku_grouped["platform_fee_inventory_storage"] = 0
        sku_grouped["platform_fee"] = 0
        sku_grouped["acos"] = 0

        sku_grouped["net_taxes"] = (
            safe_series(sku_grouped, "marketplace_facilitator_tax")
            + safe_series(sku_grouped, "shipping_credits_tax")
        )

        sku_grouped["net_taxes"] = sku_grouped["net_taxes"].apply(
            lambda x: 0 if abs(x) < 1e-10 else x
        )

        credit_keywords = [
            "FBA Inventory Reimbursement - Customer Return",
            "FBA Inventory Reimbursement - Customer Service Issue",
            "FBA Inventory Reimbursement - General Adjustment",
            "FBA Inventory Reimbursement - Damaged:Warehouse",
            "FBA Inventory Reimbursement - Lost:Warehouse",
        ]
        credit_mask = df["desc_norm"].str.contains("|".join(credit_keywords), case=False, na=False)
        sku_net_credits = (
            df.loc[credit_mask]
            .groupby("sku")["total"]
            .sum()
            .abs()
            .reset_index()
            .rename(columns={"total": "net_credits"})
        )

        sku_grouped = sku_grouped.merge(sku_net_credits, on="sku", how="left")
        sku_grouped["net_credits"] = (
            safe_series(sku_grouped, "net_credits")
            + safe_series(sku_grouped, "gift_wrap_credits")
            + safe_series(sku_grouped, "shipping_credits")
        )

        # ---------- refund sales ----------
        refund_sales_df = (
            df.loc[
                (df["type_norm"] == "refund")
                & df["sku"].notna()
                & (df["sku"].astype(str).str.strip() != "")
                & (df["sku"].astype(str).str.strip() != "0")
                & (df["sku"].astype(str).str.strip().str.lower() != "none"),
                ["sku", "product_sales"]
            ]
            .copy()
        )

        refund_sales_df["sku"] = refund_sales_df["sku"].astype(str).str.strip()

        refund_sales_df["product_sales"] = pd.to_numeric(
            refund_sales_df["product_sales"],
            errors="coerce"
        ).fillna(0)

        refund_sales_df = (
            refund_sales_df
            .groupby("sku", as_index=False)["product_sales"]
            .sum()
            .rename(columns={"product_sales": "refund_sales"})
        )

        refund_sales_df["refund_sales"] = refund_sales_df["refund_sales"].abs()

        sku_grouped["sku"] = sku_grouped["sku"].astype(str).str.strip()

        sku_grouped = sku_grouped.merge(
            refund_sales_df,
            on="sku",
            how="left"
        )

        sku_grouped["refund_sales"] = pd.to_numeric(
            sku_grouped["refund_sales"],
            errors="coerce"
        ).fillna(0)

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

        sku_grouped["tex_and_credits"] = (
            safe_series(sku_grouped, "product_sales_tax")
            + safe_series(sku_grouped, "postage_credits")
            + safe_series(sku_grouped, "gift_wrap_credits")
            + safe_series(sku_grouped, "giftwrap_credits_tax")
            + safe_series(sku_grouped, "promotional_rebates_tax")
        )

        sku_grouped["Net Sales"] = (
            safe_series(sku_grouped, "gross_sales")
            - safe_series(sku_grouped, "refund_sales")
            - safe_series(sku_grouped, "tex_and_credits")
        )
        sku_grouped["asp"] = np.where(
            sku_grouped["quantity"] != 0,
            sku_grouped["Net Sales"] / sku_grouped["quantity"],
            0
        )
        sku_grouped["asp"] = sku_grouped["asp"].replace([np.inf, -np.inf], 0).fillna(0)

        sku_grouped["promotional_rebates_percentage"] = np.where(
            sku_grouped["Net Sales"] != 0,
            (safe_series(sku_grouped, "promotional_rebates") / sku_grouped["Net Sales"]) * 100,
            0
        )
        sku_grouped["promotional_rebates_percentage"] = (
            sku_grouped["promotional_rebates_percentage"]
            .replace([np.inf, -np.inf], 0)
            .fillna(0)
        )

        sku_grouped["other_transaction_fees"] = (
            sku_grouped["net_taxes"].abs()
            - sku_grouped["net_credits"]
        )

        sku_grouped["amazon_fee"] = (
            safe_series(sku_grouped, "fba_fees").abs()
            + safe_series(sku_grouped, "selling_fees").abs()
            - safe_series(sku_grouped, "other").abs()
        )

        sku_grouped["price_in_gbp"] = safe_series(sku_grouped, "price_in_gbp")
        sku_grouped["cost_of_unit_sold"] = sku_grouped["price_in_gbp"] * sku_grouped["total_quantity"]

        sku_grouped["profit"] = (
            safe_series(sku_grouped, "Net Sales")
            - safe_series(sku_grouped, "cost_of_unit_sold").abs()
            - safe_series(sku_grouped, "amazon_fee").abs()
            # - safe_series(sku_grouped, "net_taxes").abs()
            + safe_series(sku_grouped, "net_credits")
        )

        sku_grouped["profit_percentage"] = np.where(
            sku_grouped["Net Sales"] != 0,
            (sku_grouped["profit"] / sku_grouped["Net Sales"]) * 100,
            0
        )
        sku_grouped["profit_percentage"] = sku_grouped["profit_percentage"].replace([np.inf, -np.inf], 0).fillna(0)

        shipment_charges_df = sku_sum_total_where_desc_contains(df, shipment_keywords, "shipment_charges")
        sku_grouped = sku_grouped.merge(shipment_charges_df, on="sku", how="left")

        sku_grouped["shipment_charges"] = safe_series(sku_grouped, "shipment_charges")
        sku_grouped["advertising_total"] = 0

        sku_grouped["platformfeenew"] = safe_series(sku_grouped, "platformfeenew")
        sku_grouped["platform_fee_inventory_storage"] = safe_series(sku_grouped, "platform_fee_inventory_storage")

        sku_grouped["platform_fee"] = (
            sku_grouped["platformfeenew"].abs()
            + sku_grouped["platform_fee_inventory_storage"].abs()
            - safe_series(sku_grouped, "lost_total").abs()
            - safe_series(sku_grouped, "misc_transaction").abs()
        )

        sku_grouped["reimbursement_vs_sales"] = 0
        sku_grouped["cm2_profit"] = (
            sku_grouped["profit"]
            - sku_grouped["advertising_total"]
            - sku_grouped["platform_fee"]
            - sku_grouped["shipment_charges"]
            - sku_grouped["shipment_fees"]
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
        cm2_profit = total_profit - (
            abs(advertising_total)
            + abs(platform_fee)
            + abs(sku_grouped["shipment_charges"].sum())
            + abs(shipment_fees)
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

        sum_row["profit_percentage"] = (
            (sum_row["profit"] / sum_row["net_sales"]) * 100
            if sum_row["net_sales"] != 0 else 0
        )
        sum_row["shipment_charges"] = float(sku_grouped["shipment_charges"].sum())
        sum_row["shipment_fees"] = abs(shipment_fees)
        sum_row["rembursement_fee"] = abs(rembursement_fee)
        sum_row["visible_ads"] = visible_ads_total
        sum_row["dealsvouchar_ads"] = dealsvouchar_ads_total
        sum_row["advertising_total"] = advertising_total
        sum_row["platformfeenew"] = platformfeenew_total
        sum_row["platform_fee_inventory_storage"] = platform_fee_inventory_storage_total
        sum_row["misc_transaction"] = misc_transaction_total
        sum_row["lost_total"] = -lost_total_amount
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
            sum_row["net_sales"] / sum_row["quantity"]
            if sum_row["quantity"] != 0 else 0
        )
        sum_row["unit_wise_profitability"] = (
            sum_row["profit"] / sum_row["quantity"]
            if sum_row["quantity"] != 0 else 0
        )

        sku_grouped["year"] = str(year)
        sku_grouped["country"] = country
        sku_grouped["user_id"] = user_id

        sku_grouped = pd.concat([sku_grouped, pd.DataFrame([sum_row])], ignore_index=True)

        final_columns = [
            "sku", "product_name", "quantity", "return_quantity", "total_quantity",
            "asp", "gross_sales", "refund_sales", "tex_and_credits", "net_sales",
            "promotional_rebates", "promotional_rebates_percentage",
            "cost_of_unit_sold", "selling_fees", "fba_fees", "amazon_fee",
            "net_taxes", "net_credits", "misc_transaction", "other_transaction_fees",
            "profit", "unit_wise_profitability", "profit_percentage",
            "visible_ads", "dealsvouchar_ads", "advertising_total", "lost_total",
            "platformfeenew", "platform_fee", "platform_fee_inventory_storage",
            "shipment_fees", "cm2_profit", "cm2_profit_percentage",
            "cm2_margins", "acos", "rembursement_fee",
            "rembursment_vs_cm2_margins", "reimbursement_vs_sales",
            "sales_mix", "profit_mix", "year", "country", "user_id"
        ]

        for col in final_columns:
            if col not in sku_grouped.columns:
                sku_grouped[col] = 0

        sku_grouped = sku_grouped[final_columns]

        for col in sku_grouped.columns:
            if pd.api.types.is_numeric_dtype(sku_grouped[col]):
                sku_grouped[col] = pd.to_numeric(sku_grouped[col], errors="coerce").fillna(0)
            else:
                sku_grouped[col] = sku_grouped[col].fillna("")

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
            "platformfeenew": Float,
            "platform_fee": Float,
            "platform_fee_inventory_storage": Float,
            "shipment_fees": Float,
            "cm2_profit": Float,
            "cm2_profit_percentage": Float,
            "cm2_margins": Float,
            "acos": Float,
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

