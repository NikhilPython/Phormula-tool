from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from config import Config
SECRET_KEY = Config.SECRET_KEY
import os
import pandas as pd
import numpy as np
import re
from dotenv import load_dotenv

load_dotenv()
db_url = os.getenv('DATABASE_URL', 'postgresql://postgres:password@localhost:5432/phormula')


def process_global_monthly_skuwise_data(user_id, country, year, month):
    """
    Builds 4 monthly global tables from:
      - skuwisemonthly_{user_id}         -> global
      - skuwisemonthlyind_{user_id}      -> global_inr
      - skuwisemonthlycan_{user_id}      -> global_cad
      - skuwisemonthlygbp_{user_id}      -> global_gbp
    """
    engine = create_engine(db_url)
    conn = engine.connect()

    config_list = [
        (f"skuwisemonthly_{user_id}",      "global"),
        (f"skuwisemonthlyind_{user_id}",   "global_inr"),
        (f"skuwisemonthlycan_{user_id}",   "global_cad"),
        (f"skuwisemonthlygbp_{user_id}",   "global_gbp"),
    ]

    try:
        for source_table, logical_country in config_list:
            monthly_table = f"skuwisemonthly_{user_id}_{logical_country}_{month}{year}_table"

            query = f"""
                SELECT
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
                    "lost_total",
                    "platformfeenew",
                    "platform_fee",
                    "platform_fee_inventory_storage",
                    "cm2_profit",
                    "cm2_profit_percentage",
                    "acos",
                    "rembursement_fee",
                    "rembursment_vs_cm2_margins",
                    "reimbursement_vs_sales",
                    "sales_mix",
                    "profit_mix",
                    "cm2_margins",
                    "user_id"
                FROM {source_table}
                WHERE "year" = :year AND LOWER("month") = :month
            """

            try:
                df = pd.read_sql(
                    text(query),
                    conn,
                    params={"year": str(year), "month": str(month).lower()}
                )
            except Exception as e:
                print(f"❌ Failed to read from {source_table}: {e}")
                continue

            if df.empty:
                print(f"⚠️ No data found for {month}/{year} in {source_table}")
                continue

            df["product_name"] = df["product_name"].replace([None, np.nan], "").astype(str).str.strip()
            df["sku"] = df["sku"].replace([None, np.nan], "").astype(str).str.strip()

            mask = df["product_name"].str.lower().isin(["", "0", "nan", "none"])
            df.loc[mask, "product_name"] = df.loc[mask, "sku"]

            sku_grouped = df.groupby("product_name", dropna=False).agg({
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
                "lost_total": "sum",
                "platformfeenew": "sum",
                "platform_fee": "sum",
                "platform_fee_inventory_storage": "sum",
                "cm2_profit": "sum",
                "cm2_profit_percentage": "mean",
                "acos": "mean",
                "rembursement_fee": "sum",
                "rembursment_vs_cm2_margins": "mean",
                "reimbursement_vs_sales": "mean",
                "sales_mix": "mean",
                "profit_mix": "mean",
                "cm2_margins": "mean",
                "user_id": "first",
            }).reset_index()

            sku_grouped["product_name"] = sku_grouped["product_name"].astype(str).str.strip()

            # Recompute derived metrics safely
            net_sales_num = pd.to_numeric(sku_grouped["net_sales"], errors="coerce").fillna(0)
            qty_num = pd.to_numeric(sku_grouped["quantity"], errors="coerce").fillna(0)
            profit_num = pd.to_numeric(sku_grouped["profit"], errors="coerce").fillna(0)
            ad_num = pd.to_numeric(sku_grouped["advertising_total"], errors="coerce").fillna(0)
            cm2_num = pd.to_numeric(sku_grouped["cm2_profit"], errors="coerce").fillna(0)
            rem_num = pd.to_numeric(sku_grouped["rembursement_fee"], errors="coerce").fillna(0)

            sku_grouped["cm2_margins"] = np.where(net_sales_num != 0, (cm2_num / net_sales_num) * 100, 0)
            sku_grouped["acos"] = np.where(net_sales_num != 0, (ad_num / net_sales_num) * 100, 0)
            sku_grouped["rembursment_vs_cm2_margins"] = np.where(cm2_num != 0, (rem_num / cm2_num) * 100, 0)
            sku_grouped["reimbursement_vs_sales"] = np.where(net_sales_num != 0, (rem_num / net_sales_num) * 100, 0)
            sku_grouped["profit_percentage"] = np.where(net_sales_num != 0, (profit_num / net_sales_num) * 100, 0)
            sku_grouped["asp"] = np.where(qty_num != 0, net_sales_num / qty_num, 0)
            sku_grouped["unit_wise_profitability"] = np.where(qty_num != 0, profit_num / qty_num, 0)
            sku_grouped["cm2_profit_percentage"] = np.where(net_sales_num != 0, (cm2_num / net_sales_num) * 100, 0)

            temp = sku_grouped[sku_grouped["product_name"].str.lower() != "total"]
            total_sales = abs(pd.to_numeric(temp["net_sales"], errors="coerce").fillna(0).sum())
            total_profit = abs(pd.to_numeric(temp["profit"], errors="coerce").fillna(0).sum())

            sku_grouped["profit_mix"] = np.where(
                total_profit != 0,
                (pd.to_numeric(sku_grouped["profit"], errors="coerce").fillna(0) / total_profit) * 100,
                0
            )
            sku_grouped["sales_mix"] = np.where(
                total_sales != 0,
                (pd.to_numeric(sku_grouped["net_sales"], errors="coerce").fillna(0) / total_sales) * 100,
                0
            )

            total_row = sku_grouped[sku_grouped["product_name"].str.lower() == "total"]
            other_rows = sku_grouped[sku_grouped["product_name"].str.lower() != "total"].sort_values(
                by="profit", ascending=False
            )
            sku_grouped = pd.concat([other_rows, total_row], ignore_index=True)

            conn.execute(text(f"DROP TABLE IF EXISTS {monthly_table}"))
            conn.execute(text(f"""
                CREATE TABLE {monthly_table} (
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
                    lost_total DOUBLE PRECISION,
                    platformfeenew DOUBLE PRECISION,
                    platform_fee DOUBLE PRECISION,
                    platform_fee_inventory_storage DOUBLE PRECISION,
                    cm2_profit DOUBLE PRECISION,
                    cm2_profit_percentage DOUBLE PRECISION,
                    acos DOUBLE PRECISION,
                    rembursement_fee DOUBLE PRECISION,
                    rembursment_vs_cm2_margins DOUBLE PRECISION,
                    reimbursement_vs_sales DOUBLE PRECISION,
                    sales_mix DOUBLE PRECISION,
                    profit_mix DOUBLE PRECISION,
                    user_id INTEGER,
                    cm2_margins DOUBLE PRECISION
                )
            """))
            conn.commit()

            sku_grouped.columns = [col.lower() for col in sku_grouped.columns]
            sku_grouped.to_sql(
                monthly_table,
                conn,
                if_exists="append",
                index=False,
                schema="public",
                method="multi",
                chunksize=1000
            )
            conn.commit()

            # Upload History Section
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

                platform_fee_inventory_storage_total = 0.0
                platformfeenew_total = 0.0

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

                total_expense_val = round(
                    total_amazon_fee_val + abs(platformfeenew_total) + abs(platform_fee_inventory_storage_total),
                    2
                )
                otherwplatform_val = abs(platform_fee_val)
                taxncredit_val = total_tax_val + abs(total_credits_val)

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
    country = "global"

    try:
        quarter_months = {
            "quarter1": ["january", "february", "march"],
            "quarter2": ["april", "may", "june"],
            "quarter3": ["july", "august", "september"],
            "quarter4": ["october", "november", "december"]
        }

        month = month.lower()
        quarter_key = None
        months_for_quarter = None

        for q_name, months in quarter_months.items():
            if month in months:
                quarter_key = q_name
                months_for_quarter = months
                break
        else:
            return

        config_list = [
            (f"skuwisemonthly_{user_id}",     "global"),
            (f"skuwisemonthlyind_{user_id}",  "global_inr"),
            (f"skuwisemonthlycan_{user_id}",  "global_cad"),
            (f"skuwisemonthlygbp_{user_id}",  "global_gbp"),
        ]

        for source_table, logical_country in config_list:
            quarter_table = f"{quarter_key}_{user_id}_{logical_country}_{year}_table"

            month_params = {f"m{i}": m for i, m in enumerate(months_for_quarter)}
            placeholders_named = ', '.join(f":m{i}" for i in range(len(months_for_quarter)))

            available_months_query = text(f"""
                SELECT DISTINCT LOWER(month) AS month
                FROM {source_table}
                WHERE LOWER(month) IN ({placeholders_named}) AND year = :year
            """)
            result = conn.execute(available_months_query, {**month_params, "year": year})
            available_months_df = pd.DataFrame(result.fetchall(), columns=["month"])
            selected_months = available_months_df["month"].tolist()

            if not selected_months:
                print(f"No available months found in {source_table} for quarter {quarter_key}.")
                continue

            placeholders = ', '.join(['%s'] * len(selected_months))
            query = f"""
                SELECT
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
                    "lost_total",
                    "platformfeenew",
                    "platform_fee",
                    "platform_fee_inventory_storage",
                    "cm2_profit",
                    "cm2_profit_percentage",
                    "acos",
                    "rembursement_fee",
                    "rembursment_vs_cm2_margins",
                    "reimbursement_vs_sales",
                    "sales_mix",
                    "profit_mix",
                    "cm2_margins",
                    "user_id"
                FROM {source_table}
                WHERE LOWER(month) IN ({placeholders}) AND year = %s
            """

            try:
                df = pd.read_sql(query, conn, params=tuple(selected_months + [year]))
            except Exception as e:
                print(f"❌ Failed to read from {source_table}: {e}")
                continue

            if df.empty:
                print(f"No data for selected months in {source_table}.")
                continue

            df["product_name"] = df["product_name"].replace([None, np.nan], "").astype(str).str.strip()
            df["sku"] = df["sku"].replace([None, np.nan], "").astype(str).str.strip()

            mask = df["product_name"].str.lower().isin(["", "0", "nan", "none"])
            df.loc[mask, "product_name"] = df.loc[mask, "sku"]

            sku_grouped = df.groupby("product_name", dropna=False).agg({
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
                "lost_total": "sum",
                "platformfeenew": "sum",
                "platform_fee": "sum",
                "platform_fee_inventory_storage": "sum",
                "cm2_profit": "sum",
                "cm2_profit_percentage": "mean",
                "acos": "mean",
                "rembursement_fee": "sum",
                "rembursment_vs_cm2_margins": "mean",
                "reimbursement_vs_sales": "mean",
                "sales_mix": "mean",
                "profit_mix": "mean",
                "cm2_margins": "mean",
                "user_id": "first"
            }).reset_index()

            net_sales_num = pd.to_numeric(sku_grouped["net_sales"], errors="coerce").fillna(0)
            qty_num = pd.to_numeric(sku_grouped["quantity"], errors="coerce").fillna(0)
            profit_num = pd.to_numeric(sku_grouped["profit"], errors="coerce").fillna(0)
            ad_num = pd.to_numeric(sku_grouped["advertising_total"], errors="coerce").fillna(0)
            cm2_num = pd.to_numeric(sku_grouped["cm2_profit"], errors="coerce").fillna(0)
            rem_num = pd.to_numeric(sku_grouped["rembursement_fee"], errors="coerce").fillna(0)

            sku_grouped["cm2_margins"] = np.where(net_sales_num != 0, (cm2_num / net_sales_num) * 100, 0)
            sku_grouped["acos"] = np.where(net_sales_num != 0, (ad_num / net_sales_num) * 100, 0)
            sku_grouped["rembursment_vs_cm2_margins"] = np.where(cm2_num != 0, (rem_num / cm2_num) * 100, 0)
            sku_grouped["reimbursement_vs_sales"] = np.where(net_sales_num != 0, (rem_num / net_sales_num) * 100, 0)
            sku_grouped["profit_percentage"] = np.where(net_sales_num != 0, (profit_num / net_sales_num) * 100, 0)
            sku_grouped["asp"] = np.where(qty_num != 0, net_sales_num / qty_num, 0)
            sku_grouped["unit_wise_profitability"] = np.where(qty_num != 0, profit_num / qty_num, 0)
            sku_grouped["cm2_profit_percentage"] = np.where(net_sales_num != 0, (cm2_num / net_sales_num) * 100, 0)

            temp = sku_grouped[sku_grouped["product_name"].str.lower() != "total"]
            total_sales = abs(pd.to_numeric(temp["net_sales"], errors="coerce").fillna(0).sum())
            total_profit = abs(pd.to_numeric(temp["profit"], errors="coerce").fillna(0).sum())

            sku_grouped["profit_mix"] = np.where(
                total_profit != 0,
                (pd.to_numeric(sku_grouped["profit"], errors="coerce").fillna(0) / total_profit) * 100,
                0
            )
            sku_grouped["sales_mix"] = np.where(
                total_sales != 0,
                (pd.to_numeric(sku_grouped["net_sales"], errors="coerce").fillna(0) / total_sales) * 100,
                0
            )

            total_row = sku_grouped[sku_grouped["product_name"].str.lower() == "total"]
            other_rows = sku_grouped[sku_grouped["product_name"].str.lower() != "total"].sort_values(
                by="profit", ascending=False
            )
            sku_grouped = pd.concat([other_rows, total_row], ignore_index=True)

            with engine.begin() as conn_inner:
                conn_inner.execute(text(f"DROP TABLE IF EXISTS {quarter_table}"))
                conn_inner.execute(text(f"""
                    CREATE TABLE {quarter_table} (
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
                        lost_total DOUBLE PRECISION,
                        platformfeenew DOUBLE PRECISION,
                        platform_fee DOUBLE PRECISION,
                        platform_fee_inventory_storage DOUBLE PRECISION,
                        cm2_profit DOUBLE PRECISION,
                        cm2_profit_percentage DOUBLE PRECISION,
                        acos DOUBLE PRECISION,
                        rembursement_fee DOUBLE PRECISION,
                        rembursment_vs_cm2_margins DOUBLE PRECISION,
                        reimbursement_vs_sales DOUBLE PRECISION,
                        sales_mix DOUBLE PRECISION,
                        profit_mix DOUBLE PRECISION,
                        user_id INTEGER,
                        cm2_margins DOUBLE PRECISION
                    )
                """))

                sku_grouped.columns = sku_grouped.columns.str.lower()
                sku_grouped.to_sql(
                    quarter_table,
                    conn_inner,
                    if_exists="append",
                    index=False,
                    schema="public",
                    method="multi",
                    chunksize=1000
                )

    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        conn.close()


def process_global_yearly_skuwise_data(user_id, country, year):
    engine = create_engine(db_url)
    conn = engine.connect()

    config_list = [
        (f"skuwisemonthly_{user_id}",     "global"),
        (f"skuwisemonthlyind_{user_id}",  "global_inr"),
        (f"skuwisemonthlycan_{user_id}",  "global_cad"),
        (f"skuwisemonthlygbp_{user_id}",  "global_gbp"),
    ]

    try:
        for source_table, logical_country in config_list:
            yearly_table = f"skuwiseyearly_{user_id}_{logical_country}_{year}_table"

            yearly_query = f"""
                SELECT
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
                    "lost_total",
                    "platformfeenew",
                    "platform_fee",
                    "platform_fee_inventory_storage",
                    "cm2_profit",
                    "cm2_profit_percentage",
                    "acos",
                    "rembursement_fee",
                    "rembursment_vs_cm2_margins",
                    "reimbursement_vs_sales",
                    "sales_mix",
                    "profit_mix",
                    "cm2_margins",
                    "user_id"
                FROM {source_table}
                WHERE "year" = :year
            """

            try:
                df = pd.read_sql(text(yearly_query), conn, params={"year": str(year)})
            except Exception as e:
                print(f"❌ Failed to read from {source_table}: {e}")
                continue

            if df.empty:
                print(f"⚠️ No data found for {year} in {source_table}")
                continue

            df["product_name"] = df["product_name"].replace([None, np.nan], "").astype(str).str.strip()
            df["sku"] = df["sku"].replace([None, np.nan], "").astype(str).str.strip()

            mask = df["product_name"].str.lower().isin(["", "0", "nan", "none"])
            df.loc[mask, "product_name"] = df.loc[mask, "sku"]

            sku_grouped = df.groupby("product_name", dropna=False).agg({
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
                "lost_total": "sum",
                "platformfeenew": "sum",
                "platform_fee": "sum",
                "platform_fee_inventory_storage": "sum",
                "cm2_profit": "sum",
                "cm2_profit_percentage": "mean",
                "acos": "mean",
                "rembursement_fee": "sum",
                "rembursment_vs_cm2_margins": "mean",
                "reimbursement_vs_sales": "mean",
                "sales_mix": "mean",
                "profit_mix": "mean",
                "cm2_margins": "mean",
                "user_id": "first",
            }).reset_index()

            sku_grouped["product_name"] = sku_grouped["product_name"].astype(str).str.strip()

            net_sales_num = pd.to_numeric(sku_grouped["net_sales"], errors="coerce").fillna(0)
            qty_num = pd.to_numeric(sku_grouped["quantity"], errors="coerce").fillna(0)
            profit_num = pd.to_numeric(sku_grouped["profit"], errors="coerce").fillna(0)
            ad_num = pd.to_numeric(sku_grouped["advertising_total"], errors="coerce").fillna(0)
            cm2_num = pd.to_numeric(sku_grouped["cm2_profit"], errors="coerce").fillna(0)
            rem_num = pd.to_numeric(sku_grouped["rembursement_fee"], errors="coerce").fillna(0)

            sku_grouped["cm2_margins"] = np.where(net_sales_num != 0, (cm2_num / net_sales_num) * 100, 0)
            sku_grouped["acos"] = np.where(net_sales_num != 0, (ad_num / net_sales_num) * 100, 0)
            sku_grouped["rembursment_vs_cm2_margins"] = np.where(cm2_num != 0, (rem_num / cm2_num) * 100, 0)
            sku_grouped["reimbursement_vs_sales"] = np.where(net_sales_num != 0, (rem_num / net_sales_num) * 100, 0)
            sku_grouped["profit_percentage"] = np.where(net_sales_num != 0, (profit_num / net_sales_num) * 100, 0)
            sku_grouped["asp"] = np.where(qty_num != 0, net_sales_num / qty_num, 0)
            sku_grouped["unit_wise_profitability"] = np.where(qty_num != 0, profit_num / qty_num, 0)
            sku_grouped["cm2_profit_percentage"] = np.where(net_sales_num != 0, (cm2_num / net_sales_num) * 100, 0)

            temp = sku_grouped[sku_grouped["product_name"].str.lower() != "total"]
            total_sales = abs(pd.to_numeric(temp["net_sales"], errors="coerce").fillna(0).sum())
            total_profit = abs(pd.to_numeric(temp["profit"], errors="coerce").fillna(0).sum())

            sku_grouped["profit_mix"] = np.where(
                total_profit != 0,
                (pd.to_numeric(sku_grouped["profit"], errors="coerce").fillna(0) / total_profit) * 100,
                0
            )
            sku_grouped["sales_mix"] = np.where(
                total_sales != 0,
                (pd.to_numeric(sku_grouped["net_sales"], errors="coerce").fillna(0) / total_sales) * 100,
                0
            )

            total_row = sku_grouped[sku_grouped["product_name"].str.lower() == "total"]
            other_rows = sku_grouped[sku_grouped["product_name"].str.lower() != "total"].sort_values(
                by="profit", ascending=False
            )
            sku_grouped = pd.concat([other_rows, total_row], ignore_index=True)

            sku_grouped.columns = [col.lower() for col in sku_grouped.columns]

            conn.execute(text(f"DROP TABLE IF EXISTS {yearly_table}"))
            conn.execute(text(f"""
                CREATE TABLE {yearly_table} (
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
                    lost_total DOUBLE PRECISION,
                    platformfeenew DOUBLE PRECISION,
                    platform_fee DOUBLE PRECISION,
                    platform_fee_inventory_storage DOUBLE PRECISION,
                    cm2_profit DOUBLE PRECISION,
                    cm2_profit_percentage DOUBLE PRECISION,
                    acos DOUBLE PRECISION,
                    rembursement_fee DOUBLE PRECISION,
                    rembursment_vs_cm2_margins DOUBLE PRECISION,
                    reimbursement_vs_sales DOUBLE PRECISION,
                    sales_mix DOUBLE PRECISION,
                    profit_mix DOUBLE PRECISION,
                    user_id INTEGER,
                    cm2_margins DOUBLE PRECISION
                )
            """))
            conn.commit()

            sku_grouped.to_sql(
                yearly_table,
                conn,
                if_exists="append",
                index=False,
                schema="public",
                method="multi",
                chunksize=1000
            )
            conn.commit()

    except Exception as e:
        print(f"Error processing yearly global SKU-wise data: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()

        