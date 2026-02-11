from flask import  jsonify, request
from config import Config
SECRET_KEY = Config.SECRET_KEY
#UPLOAD_FOLDER = Config.UPLOAD_FOLDER
from config import basedir
from sqlalchemy import create_engine, MetaData, Table, select , text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
import jwt
import os
import io
import seaborn as sns
import mplcursors
import pandas as pd
import numpy as np 
import base64
from app import db
from app.models.user_models import User
from sqlalchemy import MetaData, Table
from flask import current_app
import matplotlib.pyplot as plt
from dotenv import load_dotenv
from decimal import Decimal, ROUND_HALF_UP

load_dotenv()
db_url = os.getenv('DATABASE_URL', 'postgresql://postgres:password@localhost:5432/phormula')

db_url1= os.getenv('DATABASE_ADMIN_URL')



 

def aggregate_upload_data(uploads):
    total_sales = sum(upload.total_sales or 0 for upload in uploads)
    cm2_profit = sum(upload.cm2_profit or 0 for upload in uploads)
    total_expense = sum(upload.total_expense or 0 for upload in uploads)
    total_amazon_fee = sum(upload.total_amazon_fee or 0 for upload in uploads)
    unit_sold = sum(upload.unit_sold or 0 for upload in uploads)
    total_cous = sum(upload.total_cous or 0 for upload in uploads)
    otherwplatform = sum(upload.otherwplatform or 0 for upload in uploads)
    taxncredit = sum(upload.taxncredit or 0 for upload in uploads)
    advertising_total = sum(upload.advertising_total or 0 for upload in uploads)

    return {
        'unit_sold': unit_sold,
        'total_sales': total_sales,
        'cm2_profit': cm2_profit,
        'total_expense': total_expense,
        'total_amazon_fee': total_amazon_fee,
        'total_cous': total_cous,
        'otherwplatform': otherwplatform,
        'taxncredit': taxncredit,
        'advertising_total': advertising_total
    }



def get_months_for_quarter(quarter):
    """Return the months for a given quarter with lowercase month names."""
    if quarter == 'Q1':
        return ['january', 'february', 'march']
    elif quarter == 'Q2':
        return ['april', 'may', 'june']
    elif quarter == 'Q3':
        return ['july', 'august', 'september']
    elif quarter == 'Q4':
        return ['october', 'november', 'december']
    else:
        return []


def get_referral_fees(user_id, country, sku_list):
    try:
        # Sanitize and prepare
        country = country.lower().replace(" ", "_")
        table_name = f'user_{user_id}_{country}_table'
        
        # Set up engine and metadata
        user_engine = create_engine(db_url)  
        metadata = MetaData()
        table = Table(table_name, metadata, autoload_with=user_engine)

        # Build and execute query
        with user_engine.connect() as conn:
            stmt = select(table).where(table.c.sku.in_(sku_list))
            results = conn.execute(stmt).mappings().all()

        # Build referral fee dictionary
        referral_fees = {row['sku']: row['referral_fee'] for row in results}
        return referral_fees

    except SQLAlchemyError as e:
        print(f"Database error: {str(e)}")
        return {}
    except Exception as e:
        print(f"Unexpected error: {str(e)}")
        return {}


def apply_modifications(df, country):
    try:
        # NaN ko 0 se fill kar rahe ho
        df = df.fillna(0)
        

        # Database connection
        db_url1 = os.getenv('DATABASE_ADMIN_URL', 'postgresql://postgres:password@localhost:5432/admin_db')
        engine_cat = create_engine(db_url1)
        
        # Category table load karo
        category_df = pd.read_sql("SELECT category, price_from, price_to, referral_fee_percent_est FROM category", engine_cat)
        

        # Ensure numeric types
        category_df["price_from"] = pd.to_numeric(category_df["price_from"], errors="coerce").fillna(0)
        category_df["price_to"] = pd.to_numeric(category_df["price_to"], errors="coerce").fillna(9999999)
        category_df["referral_fee_percent_est"] = pd.to_numeric(category_df["referral_fee_percent_est"], errors="coerce").fillna(0)
        category_df["category"] = category_df["category"].str.lower().str.strip()
        

        # Initialize new columns
        df["referral_fee"] = 0.0
        df["total_value"] = 0.0
       

        # Selling fees ko absolute value mein convert karo
        

        # Ensure all expected columns exist
        column_dtypes = {
            'product_sales_tax': float,
            'promotional_rebates_tax': float,
            'difference': float,
            'errorstatus': str,
            'fbaerrorstatus': str,
            'answer': float,
            'fbaanswer': float,
            'gift_wrap_credits': float,
            'shipping_credits': float,
            'postage_credits': float,
            'shipping_credits_tax': float,
            'product_group': str,
        }
        

        for col, dtype in column_dtypes.items():
            if col not in df.columns:
                if dtype == str:
                    df[col] = ''
                else:
                    df[col] = 0
            if dtype != str:
                df[col] = df[col].astype(dtype)
        
        

        # ✅ SINGLE LOOP - No nested loops
        for index, row in df.iterrows():
            sku = row.get('sku', '')
            

            # Get product_sales
            try:
                product_sales = float(row.get('product_sales', 0) or 0)
            except (TypeError, ValueError):
                product_sales = 0.0

            # Get quantity
            try:
                quantity = float(row.get('quantity', 0) or 0)
            except (TypeError, ValueError):
                quantity = 0.0

            if quantity in [None, 0]:
                quantity = 1

            # Get fba_fees
            try:
                fba_fees = float(row.get('fba_fees', 0) or 0)
            except (TypeError, ValueError):
                fba_fees = 0.0

            # Get selling_fees
            try:
                selling_fees = -float(row.get('selling_fees', 0) or 0)
            except (TypeError, ValueError):
                selling_fees = 0.0
            
            # Get shipping_credits_tax
            try:
                shipping_credits_tax = float(row.get('shipping_credits_tax', 0) or 0)
            except (TypeError, ValueError):
                shipping_credits_tax = 0.0
            
         

            # ✅ Country-wise calculations
            if country.lower() == 'uk':
                product_sales_tax = float(row.get('product_sales_tax', 0) or 0)
                postage_credits = float(row.get('postage_credits', 0) or 0)
                promotional_rebates = float(row.get('promotional_rebates', 0) or 0)
                promotional_rebates_tax = float(row.get('promotional_rebates_tax', 0) or 0)
                

                if postage_credits > 0:
                    postage_shipping_total = postage_credits + shipping_credits_tax
                else:
                    postage_shipping_total = 0.0
                

                    # NEW LOGIC: final_postage_shipping_total
                promo_total = promotional_rebates + promotional_rebates_tax

                if promo_total == -postage_shipping_total:
                    final_postage_shipping_total = 0
                else:
                    final_postage_shipping_total = postage_shipping_total

                df.at[index, "final_postage_shipping_total"] = final_postage_shipping_total


                additions = product_sales + product_sales_tax + postage_credits + promotional_rebates + promotional_rebates_tax + shipping_credits_tax
                deductions = final_postage_shipping_total
               

            elif country.lower() == 'us':
                gift_wrap_credits = float(row.get('gift_wrap_credits', 0) or 0)
                shipping_credits = float(row.get('shipping_credits', 0) or 0)
                promotional_rebates = float(row.get('promotional_rebates', 0) or 0)
                

                additions = product_sales + gift_wrap_credits + shipping_credits
                deductions = promotional_rebates

            else:
                additions = product_sales
                deductions = 0.0

            # Calculate total_value
            total_value = ((additions - deductions) / quantity)
            
            # Proper rounding (9.995 → 10)
            total_value = float(Decimal(total_value).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP))
            df.at[index, 'total_value'] = total_value

           
           

            # ✅ Match category table for referral_fee
            product_group = str(row.get("product_group", "")).strip().lower()

            # Match category
            matched_row = category_df[
                (category_df["category"] == product_group) &
                (total_value >= category_df["price_from"]) &
                (total_value <= category_df["price_to"])
            ]

            if 'selling_fees' in df.columns:
                df['selling_fees'] = df['selling_fees'].abs()


            if not matched_row.empty:
                referral_fee = float(matched_row.iloc[0]["referral_fee_percent_est"])
            else:
                referral_fee = 0.0

            df.at[index, "referral_fee"] = referral_fee
         

            # Get description
            desc = str(row.get('description', '')).strip().lower()
            txn_type = str(row.get('type', '')).strip().lower()

            # ✅ Calculate answer (referral fee amount)
            if product_sales != 0:
                answer = round((total_value) * (referral_fee / 100.0), 2)
                answer = answer * quantity
            else:
                answer = 0.0

            # Tax row handling
            if desc == 'tax':
                total_value = 0.0
                answer = 0.0

            # Calculate difference
            difference = round(selling_fees - answer, 2)
            # Error status
            if difference == 0:
                df.at[index, 'errorstatus'] = 'OK'
            elif difference < 0:
                df.at[index, 'errorstatus'] = 'undercharged'
            else:
                df.at[index, 'errorstatus'] = 'overcharged'

            df.at[index, 'answer'] = answer
            df.at[index, 'difference'] = difference

            # ✅ FBA fee calculation
            if total_value != 0:
                fba_answer = round((fba_fees / total_value) * 100.0, 2)
                if referral_fee != 0 and (referral_fee - 1 <= fba_answer <= referral_fee + 1):
                    df.at[index, 'fbaerrorstatus'] = 'OK'
                else:
                    df.at[index, 'fbaerrorstatus'] = 'error'
            else:
                fba_answer = 0.0
                df.at[index, 'fbaerrorstatus'] = 'DivideByZeroError'

            df.at[index, 'fbaanswer'] = fba_answer


            if txn_type.lower() == "adjustment":
                df.at[index, 'errorstatus'] = "NoReferralFee"
                df.at[index, 'fbaerrorstatus'] = "NoReferralFee"
                continue

            # No referral fee status
            if referral_fee == 0:
                if desc == 'tax' or txn_type == 'other-transaction':
                    pass  # kuch nahi karna
                else:
                    df.at[index, 'errorstatus'] = 'NoReferralFee'
                    df.at[index, 'fbaerrorstatus'] = 'NoReferralFee'

        return df

    except Exception as e:
        import traceback
        traceback.print_exc()
        return df
    




def apply_modifications_fatch(df, country):
    try:
        # NaN ko 0 se fill kar rahe ho
        df = df.fillna(0)
       

        # Database connection
        db_url1 = os.getenv('DATABASE_ADMIN_URL', 'postgresql://postgres:password@localhost:5432/admin_db')
        engine_cat = create_engine(db_url1)
       

        # Category table load karo
        category_df = pd.read_sql("SELECT category, price_from, price_to, referral_fee_percent_est FROM category", engine_cat)
        

        # Ensure numeric types
        category_df["price_from"] = pd.to_numeric(category_df["price_from"], errors="coerce").fillna(0)
        category_df["price_to"] = pd.to_numeric(category_df["price_to"], errors="coerce").fillna(9999999)
        category_df["referral_fee_percent_est"] = pd.to_numeric(category_df["referral_fee_percent_est"], errors="coerce").fillna(0)
        category_df["category"] = category_df["category"].str.lower().str.strip()
        

        # Initialize new columns
        df["referral_fee"] = 0.0
        df["total_value"] = 0.0
        

        # Selling fees ko absolute value mein convert karo
        if 'selling_fees' in df.columns:
            df['selling_fees'] = df['selling_fees'].abs()

        # Ensure all expected columns exist
        column_dtypes = {
            'product_sales_tax': float,
            'promotional_rebates_tax': float,
            'difference': float,
            'errorstatus': str,
            'fbaerrorstatus': str,
            'answer': float,
            'fbaanswer': float,
            'gift_wrap_credits': float,
            'shipping_credits': float,
            'postage_credits': float,
            'shipping_credits_tax': float,
            'product_group': str,
        }
      

        for col, dtype in column_dtypes.items():
            if col not in df.columns:
                if dtype == str:
                    df[col] = ''
                else:
                    df[col] = 0
            if dtype != str:
                df[col] = df[col].astype(dtype)
        
     

        # ✅ SINGLE LOOP - No nested loops
        for index, row in df.iterrows():
            sku = row.get('sku', '')
            

            # Get product_sales
            try:
                product_sales = float(row.get('product_sales', 0) or 0)
            except (TypeError, ValueError):
                product_sales = 0.0

            # Get quantity
            try:
                quantity = float(row.get('quantity', 0) or 0)
            except (TypeError, ValueError):
                quantity = 0.0

            if quantity in [None, 0]:
                quantity = 1

            # Get fba_fees
            try:
                fba_fees = float(row.get('fba_fees', 0) or 0)
            except (TypeError, ValueError):
                fba_fees = 0.0

            # Get selling_fees
            try:
                selling_fees = float(row.get('selling_fees', 0) or 0)
            except (TypeError, ValueError):
                selling_fees = 0.0
            
            # Get shipping_credits_tax
            try:
                shipping_credits_tax = float(row.get('shipping_credits_tax', 0) or 0)
            except (TypeError, ValueError):
                shipping_credits_tax = 0.0
            
            

            # ✅ Country-wise calculations
            if country.lower() == 'uk':
                product_sales_tax = float(row.get('product_sales_tax', 0) or 0)
                postage_credits = float(row.get('postage_credits', 0) or 0)
                promotional_rebates = float(row.get('promotional_rebates', 0) or 0)
                promotional_rebates_tax = float(row.get('promotional_rebates_tax', 0) or 0)
          

                if postage_credits > 0:
                    postage_shipping_total = postage_credits + shipping_credits_tax
                else:
                    postage_shipping_total = 0.0

                additions = product_sales + product_sales_tax + postage_credits + promotional_rebates + promotional_rebates_tax + shipping_credits_tax
                deductions = postage_shipping_total
                

            elif country.lower() == 'us':
                gift_wrap_credits = float(row.get('gift_wrap_credits', 0) or 0)
                shipping_credits = float(row.get('shipping_credits', 0) or 0)
                promotional_rebates = float(row.get('promotional_rebates', 0) or 0)
               

                additions = product_sales + gift_wrap_credits + shipping_credits
                deductions = promotional_rebates

            else:
                additions = product_sales
                deductions = 0.0

            # Calculate total_value
            total_value = ((additions - deductions) / quantity)
            total_value = float(Decimal(total_value).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP))
            
            df.at[index, 'total_value'] = total_value
           

            # ✅ Match category table for referral_fee
            product_group = str(row.get("product_group", "")).strip().lower()
            # Match category
            matched_row = category_df[
                (category_df["category"] == product_group) &
                (total_value >= category_df["price_from"]) &
                (total_value <= category_df["price_to"])
            ]

            if not matched_row.empty:
                referral_fee = float(matched_row.iloc[0]["referral_fee_percent_est"])
            else:
                referral_fee = 0.0

            df.at[index, "referral_fee"] = referral_fee
         

            # Get description
            desc = str(row.get('description', '')).strip().lower()
            txn_type = str(row.get('type', '')).strip().lower()

            # ✅ Calculate answer (referral fee amount)
            if product_sales != 0:
                answer = round((total_value) * (referral_fee / 100.0), 2)
                answer = answer * quantity
            else:
                answer = 0.0

            # Tax row handling
            if desc == 'tax':
                total_value = 0.0
                answer = 0.0

            # Calculate difference
            difference = round(selling_fees - answer, 2)

            # Error status
            if difference == 0:
                df.at[index, 'errorstatus'] = 'OK'
            elif difference < 0:
                df.at[index, 'errorstatus'] = 'undercharged'
            else:
                df.at[index, 'errorstatus'] = 'overcharged'

            df.at[index, 'answer'] = answer
            df.at[index, 'difference'] = difference

            # ✅ FBA fee calculation
            if total_value != 0:
                fba_answer = round((fba_fees / total_value) * 100.0, 2)
                if referral_fee != 0 and (referral_fee - 1 <= fba_answer <= referral_fee + 1):
                    df.at[index, 'fbaerrorstatus'] = 'OK'
                else:
                    df.at[index, 'fbaerrorstatus'] = 'error'
            else:
                fba_answer = 0.0
                df.at[index, 'fbaerrorstatus'] = 'DivideByZeroError'

            df.at[index, 'fbaanswer'] = fba_answer

            # No referral fee status
            if referral_fee == 0:
                if desc == 'tax' or txn_type == 'other-transaction':
                    pass  # kuch nahi karna
                else:
                    df.at[index, 'errorstatus'] = 'NoReferralFee'
                    df.at[index, 'fbaerrorstatus'] = 'NoReferralFee'
        return df

    except Exception as e:
        import traceback
        traceback.print_exc()
        return df