from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from config import Config
SECRET_KEY = Config.SECRET_KEY
import os
import pandas as pd
import numpy as np
import re
from datetime import date
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
        "other_transaction_fees", "other_adjustment", "profit", "unit_wise_profitability",
        "profit_percentage", "visible_ads", "dealsvouchar_ads",
        "product_spend", "display_spend", "ads_spend", "ads_spend_raw",
        "brand_spend", "advertising_fees", "total_ads",
        "advertising_total", "advertising_total_final", "lost_total", "platformfeenew", "platform_management_fees",
        "platform_fee", "platform_fee_inventory_storage",
        "short_term_storage_fee", "long_term_storage_fee", "storage_fee",
        "fba_disposal", "placement_fee", "customs_fee",
        "shipping_charges", "shipment_fees", "cm2_profit", "total_cm2_profit",
        "total_cm2_margins", "cm2_profit_percentage", "acos",
        "tacos_total_advertising_cost_of_sale",
        "debt_payment", "disbursement", "current_net_reimbursement", "rembursement_fee",
        "rembursment_vs_cm2_margins", "reimbursement_vs_sales",
        "sales_mix", "profit_mix", "user_id"
    ]

    quantity_cols = ["quantity", "return_quantity", "total_quantity"]

    # These get GBP -> USD conversion for UK
    money_cols = [
        "asp", "gross_sales", "refund_sales", "tex_and_credits", "net_sales",
        "promotional_rebates", "cost_of_unit_sold", "selling_fees", "fba_fees",
        "amazon_fee", "net_taxes", "net_credits", "misc_transaction",
        "other_transaction_fees", "other_adjustment", "profit", "visible_ads", "dealsvouchar_ads",
        "product_spend", "display_spend", "ads_spend", "ads_spend_raw",
        "brand_spend", "advertising_fees", "total_ads",
        "advertising_total", "advertising_total_final", "lost_total", "platformfeenew", "platform_management_fees",
        "platform_fee", "platform_fee_inventory_storage",
        "short_term_storage_fee", "long_term_storage_fee", "storage_fee",
        "fba_disposal", "placement_fee", "customs_fee",
        "shipping_charges", "cm2_profit", "total_cm2_profit", "debt_payment", "disbursement",
        "current_net_reimbursement",
        "rembursement_fee"
    ]

    # US-only column, do NOT currency convert
    non_convert_money_cols = ["shipment_fees"]
    marketplace_fee_component_cols = [
        "selling_fees",
        "fba_fees",
        "promotional_rebates",
        "platform_management_fees",
    ]

    percentage_cols = [
        "promotional_rebates_percentage", "unit_wise_profitability",
        "profit_percentage", "total_cm2_margins", "cm2_profit_percentage", "acos",
        "tacos_total_advertising_cost_of_sale",
        "rembursment_vs_cm2_margins", "reimbursement_vs_sales",
        "sales_mix", "profit_mix"
    ]

    frames = []
    platform_management_fee_totals = []

    for table_name in source_tables:
        if not _table_exists(conn, table_name):
            continue

        try:
            df = pd.read_sql(text(f'SELECT * FROM "{table_name}"'), conn)
            df.columns = [str(c).lower().strip() for c in df.columns]

            df.rename(columns={
                "reimbursement_fee": "rembursement_fee",
                "debtpayment": "debt_payment",
                "debt_payment_total": "debt_payment",
                "disbursement_total": "disbursement",
                "visible_ads_cost": "visible_ads",
                "visible_ads_amount": "visible_ads",
                "ads_total": "advertising_total",
                "advertisement_total": "advertising_total",
                "dealsvoucher_ads": "dealsvouchar_ads",
                "deals_voucher_ads": "dealsvouchar_ads",
                "deal_voucher_ads": "dealsvouchar_ads",
                "platform_fee_new": "platformfeenew",
                "platform_fee_new_total": "platformfeenew",
                "platform_storage_fee": "platform_fee_inventory_storage",
                "platform_management_fee": "platform_management_fees",
                "shipment_fee": "shipment_fees",
                "shipping_charge": "shipping_charges",
                "placement_fees": "placement_fee",
                "custom_fee": "customs_fee",
                "other_adjustments": "other_adjustment",
            }, inplace=True)

            # Normalize duplicate names safely. Duplicate columns can appear after
            # legacy aliases are renamed to the same canonical column name.
            if not df.columns.is_unique:
                deduped = pd.DataFrame(index=df.index)
                for column_name in dict.fromkeys(df.columns):
                    block = df.loc[:, df.columns == column_name]
                    if isinstance(block, pd.Series):
                        deduped[column_name] = block
                    elif block.shape[1] == 1:
                        deduped[column_name] = block.iloc[:, 0]
                    else:
                        deduped[column_name] = (
                            block.replace("", np.nan)
                            .bfill(axis=1)
                            .iloc[:, 0]
                        )
                df = deduped

            if df.empty:
                continue

            for col in final_columns:
                if col not in df.columns:
                    df[col] = 0

            df = df[final_columns].copy()

            for col in quantity_cols + money_cols + non_convert_money_cols + percentage_cols:
                column_data = df[col]
                if isinstance(column_data, pd.DataFrame):
                    column_data = column_data.bfill(axis=1).iloc[:, 0]
                df[col] = pd.to_numeric(column_data, errors="coerce").fillna(0)

            df["platform_management_fees"] = np.where(
                df["platform_management_fees"] != 0,
                df["platform_management_fees"],
                df["platformfeenew"],
            )

            df["ads_spend"] = np.where(
                df["ads_spend"] != 0,
                df["ads_spend"],
                df["advertising_total"],
            )
            df["advertising_total"] = np.where(
                df["advertising_total"] != 0,
                df["advertising_total"],
                df["ads_spend"],
            )
            df["advertising_total_final"] = np.where(
                df["advertising_total_final"] != 0,
                df["advertising_total_final"],
                df["ads_spend"],
            )

            if f"skuwisemonthly_{user_id}_uk_" in table_name:
                table_month, table_year = _extract_month_year_from_table(table_name)

                if not table_month or not table_year:
                    rate = 1
                else:
                    rate = _get_gbp_to_usd_rate(table_month, table_year)

                for col in money_cols:
                    df[col] = df[col] * rate

            # Global storage fee must always be the sum of its two storage components.
            # Do this after UK currency conversion so both UK and US values are in the
            # same currency before aggregation. This also fixes legacy source TOTAL rows
            # where storage_fee may contain a stale/incorrect independently-calculated value.
            df["storage_fee"] = (
                pd.to_numeric(df["short_term_storage_fee"], errors="coerce").fillna(0)
                + pd.to_numeric(df["long_term_storage_fee"], errors="coerce").fillna(0)
            )

            # Capture each country's platform management fee AFTER UK GBP->USD conversion.
            # Some source tables keep this fee only on the TOTAL row, while older tables
            # may keep it on a non-TOTAL row. Prefer a non-zero TOTAL value; otherwise
            # fall back to summing the product/non-total rows for that country.
            _pmf = pd.to_numeric(df["platform_management_fees"], errors="coerce").fillna(0)
            _product_names = df["product_name"].replace([None, np.nan], "").astype(str).str.strip().str.lower()
            _pmf_total_rows = _pmf[_product_names == "total"]
            _pmf_total_value = float(_pmf_total_rows.sum()) if not _pmf_total_rows.empty else 0.0
            if abs(_pmf_total_value) > 0:
                platform_management_fee_totals.append(_pmf_total_value)
            else:
                platform_management_fee_totals.append(float(_pmf[_product_names != "total"].sum()))

            for col in marketplace_fee_component_cols:
                df[col] = df[col].abs()

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

    for col in ["product_spend", "display_spend", "ads_spend", "advertising_total"]:
        global_df[col] = pd.to_numeric(global_df[col], errors="coerce").fillna(0)

    product_display_ads = global_df["product_spend"] + global_df["display_spend"]
    global_df["ads_spend"] = np.where(
        product_display_ads != 0,
        product_display_ads,
        np.where(global_df["ads_spend"] != 0, global_df["ads_spend"], global_df["advertising_total"]),
    )
    global_df["advertising_total"] = np.where(
        global_df["advertising_total"] != 0,
        global_df["advertising_total"],
        global_df["ads_spend"],
    )

    net_sales = pd.to_numeric(global_df["net_sales"], errors="coerce").fillna(0)
    quantity = pd.to_numeric(global_df["quantity"], errors="coerce").fillna(0)
    total_quantity = pd.to_numeric(global_df["total_quantity"], errors="coerce").fillna(0)
    profit = pd.to_numeric(global_df["profit"], errors="coerce").fillna(0)
    ads = pd.to_numeric(global_df["ads_spend"], errors="coerce").fillna(0)
    cm2_profit = pd.to_numeric(global_df["cm2_profit"], errors="coerce").fillna(0)
    reimbursement = pd.to_numeric(global_df["rembursement_fee"], errors="coerce").fillna(0)

    global_df["asp"] = np.where(total_quantity != 0, net_sales / total_quantity, 0)
    global_df["unit_wise_profitability"] = np.where(total_quantity != 0, profit / total_quantity, 0)
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

    total_only_columns = [
        "total_ads",
        "total_cm2_profit",
        "total_cm2_margins",
        "tacos_total_advertising_cost_of_sale",
        "current_net_reimbursement",
        "debt_payment",
        "disbursement",
    ]
    for col in total_only_columns:
        if col in global_df.columns:
            global_df[col] = 0

    total_row = {
        "sku": "TOTAL",
        "product_name": "TOTAL",
        "user_id": int(user_id),
    }

    # Use source TOTAL rows when available, otherwise fallback to product rows
    total_base_df = source_total_df if not source_total_df.empty else global_df

    for col in quantity_cols + money_cols + non_convert_money_cols:
        total_row[col] = pd.to_numeric(total_base_df[col], errors="coerce").fillna(0).sum()

    # IMPORTANT: add US platform_management_fees + UK platform_management_fees
    # after UK has been converted from GBP to USD. This avoids losing the UK value
    # when it is not stored on the source TOTAL row.
    if platform_management_fee_totals:
        total_row["platform_management_fees"] = float(sum(platform_management_fee_totals))

    # Never trust an independently stored source storage_fee for GLOBAL totals.
    # Rebuild it from short-term + long-term storage fee components.
    total_row["storage_fee"] = (
        float(total_row.get("short_term_storage_fee", 0) or 0)
        + float(total_row.get("long_term_storage_fee", 0) or 0)
    )

    # FIX: global TOTAL net reimbursement should be net,
    # not sum of monthly absolute reimbursement values.
    total_row["rembursement_fee"] = abs(
        float(total_row.get("disbursement", 0) or 0)
        - float(total_row.get("debt_payment", 0) or 0)
    )
    total_row["current_net_reimbursement"] = total_row["rembursement_fee"]

    total_net_sales = float(total_row["net_sales"] or 0)
    total_quantity = float(total_row["total_quantity"] or 0)
    total_profit_value = float(total_row["profit"] or 0)

    product_ads_total = (
        abs(float(total_row.get("product_spend", 0) or 0))
        + abs(float(total_row.get("display_spend", 0) or 0))
    )
    fallback_ads_total = abs(
        float(total_row.get("ads_spend", 0) or 0)
        or float(total_row.get("advertising_total", 0) or 0)
    )
    if not product_ads_total:
        product_ads_total = fallback_ads_total

    cost_ads_total = (
        abs(float(total_row.get("brand_spend", 0) or 0))
        + abs(float(total_row.get("dealsvouchar_ads", 0) or 0))
    )
    total_ads = product_ads_total + cost_ads_total

    total_row["ads_spend"] = product_ads_total
    total_row["ads_spend_raw"] = product_ads_total
    total_row["advertising_total"] = product_ads_total
    total_row["advertising_total_final"] = (
        float(total_row.get("advertising_total_final", 0) or 0)
        or product_ads_total
    )
    total_row["advertising_fees"] = total_ads
    total_row["total_ads"] = total_ads

    other_transactions_total = (
        abs(float(total_row.get("misc_transaction", 0) or 0))
        + abs(float(total_row.get("lost_total", 0) or 0))
        - abs(float(total_row.get("platform_fee_inventory_storage", 0) or 0))
        - abs(float(total_row.get("platformfeenew", 0) or 0))
    )
    total_row["platform_fee"] = -other_transactions_total

    cm2_profit_productwise = total_profit_value - product_ads_total
    inventory_reimbursement_total = (
        abs(float(total_row.get("lost_total", 0) or 0))
        - abs(float(total_row.get("fba_disposal", 0) or 0))
    )
    other_fees_total = (
        abs(float(total_row.get("platform_management_fees", 0) or 0))
        - abs(float(total_row.get("other_adjustment", 0) or 0))
    )
    total_cm2 = (
        cm2_profit_productwise
        - cost_ads_total
        - abs(float(total_row.get("shipping_charges", 0) or 0))
        - abs(float(total_row.get("storage_fee", 0) or 0))
        + inventory_reimbursement_total
        - other_fees_total
    )

    total_row["cm2_profit"] = cm2_profit_productwise
    total_row["total_cm2_profit"] = total_cm2
    total_reimbursement = float(total_row["rembursement_fee"] or 0)

    total_row["asp"] = total_net_sales / total_quantity if total_quantity else 0
    total_row["unit_wise_profitability"] = total_profit_value / total_quantity if total_quantity else 0
    total_row["profit_percentage"] = (total_profit_value / total_net_sales) * 100 if total_net_sales else 0
    total_row["acos"] = (product_ads_total / total_net_sales) * 100 if total_net_sales else 0
    total_row["cm2_profit_percentage"] = (cm2_profit_productwise / total_net_sales) * 100 if total_net_sales else 0
    total_row["total_cm2_margins"] = (total_cm2 / total_net_sales) * 100 if total_net_sales else 0
    total_row["tacos_total_advertising_cost_of_sale"] = (total_ads / total_net_sales) * 100 if total_net_sales else 0
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
            other_adjustment DOUBLE PRECISION,
            profit DOUBLE PRECISION,
            unit_wise_profitability DOUBLE PRECISION,
            profit_percentage DOUBLE PRECISION,
            visible_ads DOUBLE PRECISION,
            dealsvouchar_ads DOUBLE PRECISION,
            product_spend DOUBLE PRECISION,
            display_spend DOUBLE PRECISION,
            ads_spend DOUBLE PRECISION,
            ads_spend_raw DOUBLE PRECISION,
            brand_spend DOUBLE PRECISION,
            advertising_fees DOUBLE PRECISION,
            total_ads DOUBLE PRECISION,
            advertising_total DOUBLE PRECISION,
            advertising_total_final DOUBLE PRECISION,
            lost_total DOUBLE PRECISION,
            platformfeenew DOUBLE PRECISION,
            platform_management_fees DOUBLE PRECISION,
            platform_fee DOUBLE PRECISION,
            platform_fee_inventory_storage DOUBLE PRECISION,
            short_term_storage_fee DOUBLE PRECISION,
            long_term_storage_fee DOUBLE PRECISION,
            storage_fee DOUBLE PRECISION,
            fba_disposal DOUBLE PRECISION,
            placement_fee DOUBLE PRECISION,
            customs_fee DOUBLE PRECISION,
            shipping_charges DOUBLE PRECISION,
            shipment_fees DOUBLE PRECISION,
            cm2_profit DOUBLE PRECISION,
            total_cm2_profit DOUBLE PRECISION,
            total_cm2_margins DOUBLE PRECISION,
            cm2_profit_percentage DOUBLE PRECISION,
            acos DOUBLE PRECISION,
            tacos_total_advertising_cost_of_sale DOUBLE PRECISION,
            debt_payment DOUBLE PRECISION,
            disbursement DOUBLE PRECISION,
            current_net_reimbursement DOUBLE PRECISION,
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

        # Prefer the current monthly table naming convention with `_table`.
        # `_build_global_skuwise_table` will also accept legacy names when passed,
        # so include both forms and let `_table_exists` skip whichever is absent.
        source_tables = [
            f"skuwisemonthly_{user_id}_uk_{month}{year}_table",
            f"skuwisemonthly_{user_id}_us_{month}{year}_table",
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

        quarter_source_tables = [
            f"{quarter_key}_{user_id}_uk_usd_{year}_table",
            f"{quarter_key}_{user_id}_us_{year}_table",
        ]

        if all(_table_exists(conn, table_name) for table_name in quarter_source_tables):
            source_tables = quarter_source_tables
        else:
            source_tables = []

            for c in ["uk", "us"]:
                for m in months_for_quarter:
                    source_tables.extend([
                        f"skuwisemonthly_{user_id}_{c}_{m}{year}_table",
                        f"skuwisemonthly_{user_id}_{c}_{m}{year}",
                    ])

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

        selected_year = int(year)
        today = date.today()

        if selected_year == today.year:
            all_months = all_months[:max(today.month - 1, 0)]

        output_table = f"skuwiseyearly_{user_id}_global_{year}_table"

        source_tables = []

        for c in ["uk", "us"]:
            for m in all_months:
                source_tables.extend([
                    f"skuwisemonthly_{user_id}_{c}_{m}{year}_table",
                    f"skuwisemonthly_{user_id}_{c}_{m}{year}",
                ])

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
