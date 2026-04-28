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
db_url = os.getenv("DATABASE_URL")
admin_db_url = os.getenv("DATABASE_ADMIN_URL")

def _table_exists(conn, table_name):
    return conn.execute(
        text("""
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = 'public'
                AND table_name = :table_name
            )
        """),
        {"table_name": table_name}
    ).scalar()


def _get_gbp_to_usd_rate(month, year):
    admin_engine = create_engine(admin_db_url)

    with admin_engine.connect() as admin_conn:
        rate = admin_conn.execute(
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
                "month": str(month).lower().strip(),
                "year": int(year),
            }
        ).scalar()

    return float(rate or 1)


def _extract_month_year_from_table(table_name):
    match = re.search(r"_(january|february|march|april|may|june|july|august|september|october|november|december)(\d{4})$", table_name)
    if match:
        return match.group(1), match.group(2)

    return None, None


def _build_global_skuwise_table(user_id, output_table, source_tables, conn):
    final_columns = [
        "sku", "product_name", "quantity", "return_quantity", "total_quantity",
        "asp", "gross_sales", "refund_sales", "tex_and_credits", "net_sales",
        "promotional_rebates", "promotional_rebates_percentage",
        "cost_of_unit_sold", "selling_fees", "fba_fees", "amazon_fee",
        "net_taxes", "net_credits", "misc_transaction",
        "other_transaction_fees", "profit", "unit_wise_profitability",
        "profit_percentage", "visible_ads", "dealsvouchar_ads",
        "advertising_total", "lost_total", "platformfeenew", "platform_fee",
        "platform_fee_inventory_storage", "shipment_fees", "cm2_profit",
        "cm2_profit_percentage", "acos", "rembursement_fee",
        "rembursment_vs_cm2_margins", "reimbursement_vs_sales",
        "sales_mix", "profit_mix", "user_id"
    ]

    quantity_cols = ["quantity", "return_quantity", "total_quantity"]

    # These get GBP -> USD conversion for UK
    money_cols = [
        "asp", "gross_sales", "refund_sales", "tex_and_credits", "net_sales",
        "promotional_rebates", "cost_of_unit_sold", "selling_fees", "fba_fees",
        "amazon_fee", "net_taxes", "net_credits", "misc_transaction",
        "other_transaction_fees", "profit", "visible_ads", "dealsvouchar_ads",
        "advertising_total", "lost_total", "platformfeenew", "platform_fee",
        "platform_fee_inventory_storage", "cm2_profit", "rembursement_fee"
    ]

    # US-only column, do NOT currency convert
    non_convert_money_cols = ["shipment_fees"]

    percentage_cols = [
        "promotional_rebates_percentage", "unit_wise_profitability",
        "profit_percentage", "cm2_profit_percentage", "acos",
        "rembursment_vs_cm2_margins", "reimbursement_vs_sales",
        "sales_mix", "profit_mix"
    ]

    frames = []

    for table_name in source_tables:
        if not _table_exists(conn, table_name):
            continue

        try:
            df = pd.read_sql(text(f'SELECT * FROM "{table_name}"'), conn)
            df.columns = [str(c).lower().strip() for c in df.columns]

            df.rename(columns={
                "reimbursement_fee": "rembursement_fee",
                "visible_ads_cost": "visible_ads",
                "visible_ads_amount": "visible_ads",
                "ads_spend": "advertising_total",
                "ads_total": "advertising_total",
                "advertisement_total": "advertising_total",
                "dealsvoucher_ads": "dealsvouchar_ads",
                "deals_voucher_ads": "dealsvouchar_ads",
                "deal_voucher_ads": "dealsvouchar_ads",
                "platform_fee_new": "platformfeenew",
                "platform_fee_new_total": "platformfeenew",
                "platform_storage_fee": "platform_fee_inventory_storage",
                "storage_fee": "platform_fee_inventory_storage",
                "shipment_fee": "shipment_fees",
            }, inplace=True)

            if df.empty:
                continue

            for col in final_columns:
                if col not in df.columns:
                    df[col] = 0

            df = df[final_columns].copy()

            for col in quantity_cols + money_cols + non_convert_money_cols + percentage_cols:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

            if f"skuwisemonthly_{user_id}_uk_" in table_name:
                table_month, table_year = _extract_month_year_from_table(table_name)

                if not table_month or not table_year:
                    rate = 1
                else:
                    rate = _get_gbp_to_usd_rate(table_month, table_year)

                

                for col in money_cols:
                    df[col] = df[col] * rate

            frames.append(df)

        except Exception as e:
            print(f"❌ Failed to read {table_name}: {e}")
            try:
                conn.rollback()
            except Exception:
                pass

    if not frames:
        return

    df_all = pd.concat(frames, ignore_index=True)

    df_all["product_name"] = df_all["product_name"].replace([None, np.nan], "").astype(str).str.strip()
    df_all["sku"] = df_all["sku"].replace([None, np.nan], "").astype(str).str.strip()

    mask = df_all["product_name"].str.lower().isin(["", "0", "nan", "none"])
    df_all.loc[mask, "product_name"] = df_all.loc[mask, "sku"]

    # Keep source TOTAL rows separately because ads/platform/reimbursement values exist there
    source_total_df = df_all[df_all["product_name"].str.lower() == "total"].copy()

    # Product-level rows
    df = df_all[df_all["product_name"].str.lower() != "total"].copy()
    df["user_id"] = int(user_id)

    agg_map = {
        "sku": "first",
        "user_id": "first",
    }

    for col in quantity_cols + money_cols + non_convert_money_cols:
        agg_map[col] = "sum"

    global_df = (
        df.groupby("product_name", dropna=False)
        .agg(agg_map)
        .reset_index()
    )

    net_sales = pd.to_numeric(global_df["net_sales"], errors="coerce").fillna(0)
    quantity = pd.to_numeric(global_df["quantity"], errors="coerce").fillna(0)
    profit = pd.to_numeric(global_df["profit"], errors="coerce").fillna(0)
    ads = pd.to_numeric(global_df["advertising_total"], errors="coerce").fillna(0)
    cm2_profit = pd.to_numeric(global_df["cm2_profit"], errors="coerce").fillna(0)
    reimbursement = pd.to_numeric(global_df["rembursement_fee"], errors="coerce").fillna(0)

    global_df["asp"] = np.where(quantity != 0, net_sales / quantity, 0)
    global_df["unit_wise_profitability"] = np.where(quantity != 0, profit / quantity, 0)
    global_df["profit_percentage"] = np.where(net_sales != 0, (profit / net_sales) * 100, 0)
    global_df["acos"] = np.where(net_sales != 0, (ads / net_sales) * 100, 0)
    global_df["cm2_profit_percentage"] = np.where(net_sales != 0, (cm2_profit / net_sales) * 100, 0)
    global_df["rembursment_vs_cm2_margins"] = np.where(cm2_profit != 0, (reimbursement / cm2_profit) * 100, 0)
    global_df["reimbursement_vs_sales"] = np.where(net_sales != 0, (reimbursement / net_sales) * 100, 0)
    global_df["promotional_rebates_percentage"] = np.where(
        net_sales != 0,
        (abs(pd.to_numeric(global_df["promotional_rebates"], errors="coerce").fillna(0)) / net_sales) * 100,
        0
    )

    total_sales = abs(net_sales.sum())
    total_profit = abs(profit.sum())

    global_df["sales_mix"] = np.where(total_sales != 0, (net_sales / total_sales) * 100, 0)
    global_df["profit_mix"] = np.where(total_profit != 0, (profit / total_profit) * 100, 0)

    total_row = {
        "sku": "TOTAL",
        "product_name": "TOTAL",
        "user_id": int(user_id),
    }

    # Use source TOTAL rows when available, otherwise fallback to product rows
    total_base_df = source_total_df if not source_total_df.empty else global_df

    for col in quantity_cols + money_cols + non_convert_money_cols:
        total_row[col] = pd.to_numeric(total_base_df[col], errors="coerce").fillna(0).sum()

    total_net_sales = float(total_row["net_sales"] or 0)
    total_quantity = float(total_row["quantity"] or 0)
    total_profit_value = float(total_row["profit"] or 0)
    total_ads = float(total_row["advertising_total"] or 0)
    total_cm2 = float(total_row["cm2_profit"] or 0)
    total_reimbursement = float(total_row["rembursement_fee"] or 0)

    total_row["asp"] = total_net_sales / total_quantity if total_quantity else 0
    total_row["unit_wise_profitability"] = total_profit_value / total_quantity if total_quantity else 0
    total_row["profit_percentage"] = (total_profit_value / total_net_sales) * 100 if total_net_sales else 0
    total_row["acos"] = (total_ads / total_net_sales) * 100 if total_net_sales else 0
    total_row["cm2_profit_percentage"] = (total_cm2 / total_net_sales) * 100 if total_net_sales else 0
    total_row["rembursment_vs_cm2_margins"] = (total_reimbursement / total_cm2) * 100 if total_cm2 else 0
    total_row["reimbursement_vs_sales"] = (total_reimbursement / total_net_sales) * 100 if total_net_sales else 0
    total_row["promotional_rebates_percentage"] = (
        abs(total_row["promotional_rebates"]) / total_net_sales
    ) * 100 if total_net_sales else 0
    total_row["sales_mix"] = 100
    total_row["profit_mix"] = 100

    global_df = global_df.sort_values(by="profit", ascending=False)

    global_df = pd.concat(
        [global_df, pd.DataFrame([total_row])],
        ignore_index=True
    )

    for col in final_columns:
        if col not in global_df.columns:
            global_df[col] = 0

    global_df = global_df[final_columns]

    conn.execute(text(f'DROP TABLE IF EXISTS "{output_table}"'))

    conn.execute(text(f"""
        CREATE TABLE "{output_table}" (
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
            shipment_fees DOUBLE PRECISION,
            cm2_profit DOUBLE PRECISION,
            cm2_profit_percentage DOUBLE PRECISION,
            acos DOUBLE PRECISION,
            rembursement_fee DOUBLE PRECISION,
            rembursment_vs_cm2_margins DOUBLE PRECISION,
            reimbursement_vs_sales DOUBLE PRECISION,
            sales_mix DOUBLE PRECISION,
            profit_mix DOUBLE PRECISION,
            user_id INTEGER
        )
    """))

    conn.commit()

    global_df.to_sql(
        output_table,
        conn,
        if_exists="append",
        index=False,
        schema="public",
        method="multi",
        chunksize=1000
    )

    conn.commit()



def process_global_monthly_skuwise_data(user_id, country, year, month):
    engine = create_engine(db_url)
    conn = engine.connect()

    try:
        month = str(month).lower().strip()
        year = str(year).strip()

        output_table = f"skuwisemonthly_{user_id}_global_{month}{year}_table"

        source_tables = [
            f"skuwisemonthly_{user_id}_uk_{month}{year}",
            f"skuwisemonthly_{user_id}_us_{month}{year}",
        ]

        _build_global_skuwise_table(
            user_id=user_id,
            output_table=output_table,
            source_tables=source_tables,
            conn=conn
        )

    except Exception as e:
        print(f"❌ Error during global monthly processing: {e}")
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

    try:
        month = str(month).lower().strip()
        year = str(year).strip()

        quarter_months = {
            "quarter1": ["january", "february", "march"],
            "quarter2": ["april", "may", "june"],
            "quarter3": ["july", "august", "september"],
            "quarter4": ["october", "november", "december"],
        }

        quarter_key = None
        months_for_quarter = None

        for q_name, months in quarter_months.items():
            if month in months:
                quarter_key = q_name
                months_for_quarter = months
                break

        if not quarter_key:
            return

        output_table = f"{quarter_key}_{user_id}_global_{year}_table"

        source_tables = []

        for c in ["uk", "us"]:
            for m in months_for_quarter:
                source_tables.append(
                    f"skuwisemonthly_{user_id}_{c}_{m}{year}"
                )

        _build_global_skuwise_table(
            user_id=user_id,
            output_table=output_table,
            source_tables=source_tables,
            conn=conn
        )

    except Exception as e:
        print(f"❌ Error processing global quarterly SKU-wise data: {e}")
        try:
            conn.rollback()
        except Exception:
            pass

    finally:
        try:
            conn.close()
        except Exception:
            pass


def process_global_yearly_skuwise_data(user_id, country, year):
    engine = create_engine(db_url)
    conn = engine.connect()

    try:
        year = str(year).strip()

        all_months = [
            "january", "february", "march", "april", "may", "june",
            "july", "august", "september", "october", "november", "december"
        ]

        output_table = f"skuwiseyearly_{user_id}_global_{year}_table"

        source_tables = []

        for c in ["uk", "us"]:
            for m in all_months:
                source_tables.append(
                    f"skuwisemonthly_{user_id}_{c}_{m}{year}"
                )

        _build_global_skuwise_table(
            user_id=user_id,
            output_table=output_table,
            source_tables=source_tables,
            conn=conn
        )

    except Exception as e:
        print(f"❌ Error processing global yearly SKU-wise data: {e}")
        try:
            conn.rollback()
        except Exception:
            pass

    finally:
        try:
            conn.close()
        except Exception:
            pass
