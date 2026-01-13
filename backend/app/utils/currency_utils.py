from sqlalchemy import create_engine,  text
from sqlalchemy.orm import sessionmaker
from config import Config
SECRET_KEY = Config.SECRET_KEY
UPLOAD_FOLDER = Config.UPLOAD_FOLDER
import os
import pandas as pd
import numpy as np 
import re
from dotenv import load_dotenv



# Load environment variables
load_dotenv()
db_url = os.getenv('DATABASE_URL', 'postgresql://postgres:password@localhost:5432/phormula')


def process_global_monthly_skuwise_data(user_id, country, year, month):
    """
    Builds 4 monthly global tables from:
      - skuwisemonthly_{user_id}         -> global (USD)
      - skuwisemonthlyind_{user_id}      -> global_inr
      - skuwisemonthlycan_{user_id}      -> global_cad
      - skuwisemonthlygbp_{user_id}      -> global_gbp

    ALSO computes platform breakup totals (Subscription / Storage / Ads) by reading RAW settlement table:
      raw_table = user_{user_id}_uk_{month}{year}_data  (must contain description,total)

    If raw table not found / missing columns, breakup values fallback to 0.
    """

    engine = create_engine(db_url)
    conn = engine.connect()

    # 4 source tables + their logical country names
    config_list = [
        (f"skuwisemonthly_{user_id}",      "global"),       # USD
        (f"skuwisemonthlyind_{user_id}",   "global_inr"),   # INR
        (f"skuwisemonthlycan_{user_id}",   "global_cad"),   # CAD
        (f"skuwisemonthlygbp_{user_id}",   "global_gbp"),   # GBP base
    ]

    try:
        for source_table, logical_country in config_list:

            quarter_table = f"skuwisemonthly_{user_id}_{logical_country}_{month}{year}_table"

            # ------------------- Main Data Processing -------------------
            query = f"""
                SELECT
                    "user_id","price_in_gbp", "product_sales", "promotional_rebates", "promotional_rebates_tax",
                    "product_sales_tax", "selling_fees", "refund_selling_fees", "fba_fees", "other",
                    "marketplace_facilitator_tax", "shipping_credits_tax", "giftwrap_credits_tax",
                    "postage_credits", "gift_wrap_credits", "net_sales", "net_taxes", "net_credits",
                    "profit", "profit_percentage", "amazon_fee", "sales_mix", "profit_mix", "quantity",
                    "cost_of_unit_sold", "other_transaction_fees", "platform_fee", "rembursement_fee",
                    "advertising_total", "reimbursement_vs_sales", "cm2_profit", "cm2_margins", "acos",
                    "asp", "rembursment_vs_cm2_margins", "product_name", "shipment_charges",
                    "unit_wise_profitability","sku"
                FROM {source_table}
                WHERE year = '{year}' AND month = '{month}'
            """

            try:
                df = pd.read_sql(query, conn)
            except Exception as e:
                print(f"❌ Failed to read from {source_table}: {e}")
                continue

            if df.empty:
                print(f"⚠️ No data found for {month}/{year} in {source_table}")
                continue

            # ================= FIX PRODUCT NAME =================
            df["product_name"] = df["product_name"].replace([None, np.nan], "")
            df["product_name"] = df["product_name"].astype(str).str.strip()

            # product_name blank / 0 / nan -> replace with sku
            df["sku"] = df.get("sku", "").replace([None, np.nan], "").astype(str).str.strip()
            mask = df["product_name"].isin(["", "0", "nan", "none"])
            df.loc[mask, "product_name"] = df.loc[mask, "sku"]
            # ====================================================

            # Group by product_name (global MTD)
            sku_grouped = df.groupby("product_name").agg({
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
                "user_id": "first",
            }).reset_index()

            sku_grouped["product_name"] = sku_grouped["product_name"].astype(str).str.strip()

            # Recompute derived metrics
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
                lambda row: (row["net_sales"] / row["quantity"]) if row["quantity"] != 0 else 0,
                axis=1
            )
            sku_grouped["unit_wise_profitability"] = sku_grouped.apply(
                lambda row: (row["profit"] / row["quantity"]) if row["quantity"] != 0 else 0,
                axis=1
            )

            # Totals for mix (exclude TOTAL row)
            temp = sku_grouped[sku_grouped["product_name"].str.lower() != "total"]
            total_sales = abs(temp["net_sales"].sum())

            total_product_sales = (
                temp["product_sales"].sum()
                + temp["product_sales_tax"].sum()
                + temp["postage_credits"].sum()
                + temp["shipping_credits_tax"].sum()
                + temp["gift_wrap_credits"].sum()
                + temp["promotional_rebates"].sum()        # NEGATIVE
                + temp["promotional_rebates_tax"].sum()    # NEGATIVE
            )

            # Put computed total_product_sales into TOTAL row's product_sales
            sku_grouped.loc[
                sku_grouped["product_name"].str.lower() == "total",
                "product_sales"
            ] = total_product_sales

            total_profit = abs(temp["profit"].sum())

            sku_grouped["profit_mix"] = sku_grouped.apply(
                lambda row: (row["profit"] / total_profit) * 100 if total_profit != 0 else 0,
                axis=1
            )
            sku_grouped["sales_mix"] = sku_grouped.apply(
                lambda row: (row["net_sales"] / total_sales) * 100 if total_sales != 0 else 0,
                axis=1
            )

            # Sort: other rows desc profit, TOTAL last
            total_row = sku_grouped[sku_grouped["product_name"].str.lower() == "total"]
            other_rows = sku_grouped[sku_grouped["product_name"].str.lower() != "total"]
            other_rows = other_rows.sort_values(by="profit", ascending=False)
            sku_grouped = pd.concat([other_rows, total_row], ignore_index=True)

            # ------------------- Create / Replace quarter_table -------------------
            conn.execute(text(f"DROP TABLE IF EXISTS {quarter_table}"))

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

            sku_grouped.columns = [col.lower() for col in sku_grouped.columns]
            sku_grouped.to_sql(
                quarter_table,
                conn,
                if_exists="replace",
                index=False,
                schema="public",
                method="multi",
                chunksize=1000
            )
            conn.commit()

            # ------------------- Upload History Section -------------------
            from app.models.user_models import UploadHistory

            Session = sessionmaker(bind=engine)
            session = Session()

            def convert_value(val):
                if isinstance(val, (np.int64, np.int32)):
                    return int(val)
                if isinstance(val, (np.float64, np.float32)):
                    return float(val)
                return val

            try:
                total_row_data = sku_grouped[sku_grouped["product_name"].str.lower() == "total"].iloc[0]
                total_sales_val = convert_value(total_row_data.get("net_sales", 0))
                total_product_sales_val = convert_value(total_row_data.get("product_sales", 0))
                total_profit_val = convert_value(total_row_data.get("profit", 0))
                fba_fees_val = convert_value(total_row_data.get("fba_fees", 0))
                platform_fee_val = convert_value(total_row_data.get("platform_fee", 0))
                rembursement_fee_val = convert_value(total_row_data.get("rembursement_fee", 0))
                cm2_profit_val = convert_value(total_row_data.get("cm2_profit", 0))
                cm2_margins_val = convert_value(total_row_data.get("cm2_margins", 0))
                acos_val = convert_value(total_row_data.get("acos", 0))
                rembursment_vs_cm2_margins_val = convert_value(total_row_data.get("rembursment_vs_cm2_margins", 0))
                advertising_total_val = convert_value(total_row_data.get("advertising_total", 0))
                reimbursement_vs_sales_val = convert_value(total_row_data.get("reimbursement_vs_sales", 0))
                unit_sold_val = convert_value(total_row_data.get("quantity", 0))
                total_cous_val = convert_value(total_row_data.get("cost_of_unit_sold", 0))
                total_amazon_fee_val = convert_value(total_row_data.get("amazon_fee", 0))
                total_credits_val = convert_value(total_row_data.get("net_credits", 0))
                total_tax_val = convert_value(total_row_data.get("net_taxes", 0))

                # ================== NEW: PLATFORM BREAKUP TOTALS (RAW settlement) ==================
                # NOTE: Your global aggregated tables do NOT have description/total columns.
                # So we compute these from RAW settlement table which has description + total.

                platform_fee_inventory_storage_total = 0.0
                platformfeenew_total = 0.0
                # optional:
                visible_ads_total = 0.0
                dealsvouchar_ads_total = 0.0

                # Run breakup only once per month (avoid 4x same query)
                if logical_country == "global_gbp":
                    try:
                        raw_table = f"user_{user_id}_uk_{month}{year}_data"
                        raw_df = pd.read_sql(
                            text(f'SELECT "description","total" FROM {raw_table}'),
                            conn
                        )

                        if not raw_df.empty:
                            raw_df["description"] = raw_df["description"].fillna("").astype(str)
                            raw_df["total"] = pd.to_numeric(raw_df["total"], errors="coerce").fillna(0.0)

                            desc_all = raw_df["description"]

                            def sum_total_where_desc_contains(keywords):
                                pattern = "|".join(re.escape(str(k)) for k in keywords if k)
                                if not pattern:
                                    return 0.0
                                mask = desc_all.str.contains(pattern, case=False, na=False, regex=True)
                                return float(raw_df.loc[mask, "total"].sum())

                            visible_ads_total = sum_total_where_desc_contains(["ProductAdsPayment"])

                            dealsvouchar_ads_total = sum_total_where_desc_contains([
                                "Cost of Advertising",
                                "Coupon Redemption Fee",
                                "Deals",
                                "Lightning Deal",
                                "CouponPerformanceEvent",
                                "CouponParticipationEvent",
                                "SellerDealComplete",
                                "VineCharge",
                                "DealParticipationEvent",
                                "DealPerformanceEvent",
                            ])

                            platformfeenew_total = sum_total_where_desc_contains(["Subscription"])

                            platform_fee_inventory_storage_total = sum_total_where_desc_contains([
                                "FBA Return Fee",
                                "FBA Long-Term Storage Fee",
                                "FBA storage fee",
                                "FBADisposal",
                                "FBAStorageBilling",
                                "FBALongTermStorageBilling",
                            ])

                    except Exception as e:
                        print(f"⚠️ Breakup calc skipped (raw settlement not found / missing cols): {e}")

                # total_expense = total_amazon_fee - (subscription + storage)  (same logic you had)
                total_expense_val = round(
                    total_amazon_fee_val
                    + abs(platformfeenew_total)
                    + abs(platform_fee_inventory_storage_total),
                    2
                )
                otherwplatform_val = abs(platform_fee_val)
                taxncredit_val = total_tax_val + abs(total_credits_val)

                # Delete existing entry (same user_id, logical_country, year, month)
                existing_entry = session.query(UploadHistory).filter_by(
                    user_id=user_id,
                    country=logical_country,
                    year=year,
                    month=month
                ).first()

                if existing_entry:
                    session.delete(existing_entry)
                    session.commit()

                upload_history_entry = UploadHistory(
                    user_id=int(user_id),
                    year=str(year),
                    month=str(month),
                    country=logical_country,
                    file_name=None,
                    sales_chart_img=None,
                    expense_chart_img=None,
                    qtd_pie_chart=None,
                    ytd_pie_chart=None,
                    profit_chart_img=None,
                    total_sales=total_sales_val,
                    total_product_sales=total_product_sales_val,
                    total_profit=total_profit_val,
                    otherwplatform=otherwplatform_val,
                    taxncredit=taxncredit_val,
                    total_expense=total_expense_val,
                    total_fba_fees=fba_fees_val,
                    platform_fee=platform_fee_val,
                    rembursement_fee=rembursement_fee_val,
                    cm2_profit=cm2_profit_val,
                    cm2_margins=cm2_margins_val,
                    acos=acos_val,
                    rembursment_vs_cm2_margins=rembursment_vs_cm2_margins_val,
                    advertising_total=advertising_total_val,
                    reimbursement_vs_sales=reimbursement_vs_sales_val,
                    unit_sold=unit_sold_val,
                    total_cous=total_cous_val,
                    total_amazon_fee=total_amazon_fee_val,
                    pnl_email_sent=False,
                )

                session.add(upload_history_entry)
                session.commit()

            except Exception as e:
                print(f"Failed to save upload history for {logical_country}: {e}")
                session.rollback()
            finally:
                session.close()

    except Exception as e:
        print(f"Error during processing: {e}")
        try:
            conn.rollback()
        except Exception:
            pass
    finally:
        try:
            conn.close()
        except Exception:
            pass


def process_global_quarterly_skuwise_data(user_id, country, month, year, q, db_url):
    engine = create_engine(db_url)
    conn = engine.connect()
    country = "global"  # base global (USD) ke liye

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
            return

        # 4 source tables + unka "country" name jo quarterly table me use hoga
        config_list = [
            (f"skuwisemonthly_{user_id}",      "global"),       # existing (USD)
            (f"skuwisemonthlyind_{user_id}",  "global_inr"),   # INR
            (f"skuwisemonthlycan_{user_id}",  "global_cad"),   # CAD
            (f"skuwisemonthlygbp_{user_id}",  "global_gbp"),   # GBP base
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
                SELECT "user_id","price_in_gbp", "product_sales", "promotional_rebates", "promotional_rebates_tax",
                "product_sales_tax", "selling_fees", "refund_selling_fees", "fba_fees", "other",
                "marketplace_facilitator_tax", "shipping_credits_tax", "giftwrap_credits_tax",
                "postage_credits", "gift_wrap_credits", "net_sales", "net_taxes", "net_credits",
                "profit", "profit_percentage", "amazon_fee", "sales_mix", "profit_mix", "quantity",
                "cost_of_unit_sold", "other_transaction_fees", "platform_fee", "rembursement_fee",
                "advertising_total", "reimbursement_vs_sales", "cm2_profit", "cm2_margins", "acos",
                "asp", "rembursment_vs_cm2_margins", "product_name","shipment_charges","unit_wise_profitability","sku"
                FROM {source_table}
                WHERE LOWER(month) IN ({placeholders}) AND year = %s
            """
            df = pd.read_sql(query, conn, params=tuple(selected_months + [year]))

            if df.empty:
                print(f"No data for selected months in {source_table}.")
                continue

            df["product_name"] = df["product_name"].replace([None, np.nan], "")
            df["product_name"] = df["product_name"].astype(str).str.strip()

            # product_name agar blank / 0 / nan ho → sku se replace
            mask = df["product_name"].isin(["", "0", "nan", "none"])
            df.loc[mask, "product_name"] = df.loc[mask, "sku"].astype(str).str.strip()
            # ====================================================


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

            temp = sku_grouped[sku_grouped["product_name"].str.lower() != "total"]

            total_sales = abs(temp["net_sales"].sum())
            # total_product_sales = abs(temp["product_sales"].sum())
            total_product_sales = (
                temp["product_sales"].sum()
                + temp["product_sales_tax"].sum()
                + temp["postage_credits"].sum()
                + temp["shipping_credits_tax"].sum()
                + temp["gift_wrap_credits"].sum()
                + temp["promotional_rebates"].sum()
                + temp["promotional_rebates_tax"].sum()
            )

            sku_grouped.loc[
                sku_grouped["product_name"].str.lower() == "total",
                "product_sales"
            ] = total_product_sales


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

    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        conn.close()


def process_global_yearly_skuwise_data(user_id, country, year):

    from sqlalchemy import create_engine, text
    import pandas as pd
    import numpy as np
    # Connect to PostgreSQL database
    engine = create_engine(db_url)
    conn = engine.connect()
    config_list = [
        (f"skuwisemonthly_{user_id}",      "global"),       # USD (pehle se)
        (f"skuwisemonthlyind_{user_id}",  "global_inr"),   # INR
        (f"skuwisemonthlycan_{user_id}",  "global_cad"),   # CAD
        (f"skuwisemonthlygbp_{user_id}",  "global_gbp"),   # GBP base
    ]
 

    # PostgreSQL table naming - using lowercase for consistency
    # quarter_table = f"skuwiseyearly_{user_id}_{country}_{year}_table"
    # source_table = f"skuwisemonthly_{user_id}"
    
    try:
        for source_table, logical_country in config_list:

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
                "asp", "rembursment_vs_cm2_margins", "product_name","shipment_charges","unit_wise_profitability","sku"
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
                print(f"⚠️ No data found for /{year} in {source_table}")
                continue

            df["product_name"] = df["product_name"].replace([None, np.nan], "")
            df["product_name"] = df["product_name"].astype(str).str.strip()

            # product_name agar blank / 0 / nan ho → sku se replace
            mask = df["product_name"].isin(["", "0", "nan", "none"])
            df.loc[mask, "product_name"] = df.loc[mask, "sku"].astype(str).str.strip()
            # ====================================================

        
    
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
            # total_product_sales = abs(temp["product_sales"].sum())
            total_product_sales = (
                temp["product_sales"].sum()
                + temp["product_sales_tax"].sum()
                + temp["postage_credits"].sum()
                + temp["shipping_credits_tax"].sum()
                + temp["gift_wrap_credits"].sum()
                + temp["promotional_rebates"].sum()
                + temp["promotional_rebates_tax"].sum()
            )

            sku_grouped.loc[
                sku_grouped["product_name"].str.lower() == "total",
                "product_sales"
            ] = total_product_sales


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
    except Exception as e:
        print(f"Error processing yearly SKU-wise data: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()



