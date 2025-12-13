from flask import Blueprint, request, jsonify , send_file 
import jwt
import os
import re
import traceback
from sqlalchemy import create_engine, text
from config import Config
from config import basedir
SECRET_KEY = Config.SECRET_KEY
UPLOAD_FOLDER = Config.UPLOAD_FOLDER
from dotenv import load_dotenv
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from werkzeug.utils import secure_filename
from sqlalchemy import MetaData, Table, inspect, select
import logging
from app.routes.amazon_sales_api_routes import _normalize_sku_row

from sqlalchemy import text
import pandas as pd
from sqlalchemy import text


# Setup logger
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

load_dotenv()
db_url = os.getenv('DATABASE_URL', 'postgresql://postgres:password@localhost:5432/phormula')
db_url1= os.getenv('DATABASE_ADMIN_URL', 'postgresql://postgres:password@localhost:5432/admin_db')
admin_engine = create_engine(db_url1)

product_bp = Blueprint('product_bp', __name__)

from flask import Blueprint, request, jsonify
from sqlalchemy import func
# from models import CurrencyConversion
from app import db

product_bp = Blueprint('product_bp', __name__)

@product_bp.route('/getConversionRate', methods=['GET'])
def get_conversion_rate():
    try:
        # Step 1: Get query params
        home_currency = (request.args.get('homecurrency') or '').strip()
        month = (request.args.get('month') or '').strip()
        year = (request.args.get('year') or '').strip()

        # Step 2: Validate
        if not home_currency or not month or not year:
            return jsonify({"error": "homecurrency, month, and year are required"}), 400

        # Step 3: Query admin_db.currency_conversion (case-insensitive)
        with admin_engine.connect() as conn:
            query = text("""
                SELECT conversion_rate
                FROM currency_conversion
                WHERE lower(selected_currency) = 'usd'
                  AND lower(user_currency) = :home_currency
                  AND lower(month) = :month
                  AND year = :year
                ORDER BY id DESC
                LIMIT 1
            """)
            row = conn.execute(query, {
                "home_currency": home_currency.lower(),
                "month": month.lower(),
                "year": int(year)
            }).fetchone()

        # Step 4: Not found
        if not row:
            return jsonify({"error": "Conversion rate not found"}), 404

        conversion_rate = float(row.conversion_rate)

        # Step 5: Print in backend
        print("\n===== CONVERSION RATE (admin_db) =====")
        print(f"USD → {home_currency.upper()} @ {month.capitalize()} {year} = {conversion_rate}")
        print("======================================\n")

        # Step 6: Send to frontend
        return jsonify({
            "from_currency": "USD",
            "homecurrency": home_currency.upper(),
            "month": month,
            "year": year,
            "conversion_rate": conversion_rate
        }), 200

    except Exception as e:
        print("ERROR in /getConversionRate:", e)
        return jsonify({"error": "Internal server error"}), 500


# @product_bp.route('/YearlySKU', methods=['GET'])
# def YearlySKU():
#     country = request.args.get('country')
#     year = request.args.get('year')
#     year = request.args.get('year')

#     # Validate the query parameters
#     if not country or not year:
#         return jsonify({'error': 'Country, and year are required'}), 400

#     auth_header = request.headers.get('Authorization')
#     if not auth_header or not auth_header.startswith('Bearer '):
#         return jsonify({'error': 'Authorization token is missing or invalid'}), 401

#     token = auth_header.split(' ')[1]
#     try:
#         payload = jwt.decode(token, SECRET_KEY, algorithms=['HS256'])
#         user_id = payload['user_id']
#     except jwt.ExpiredSignatureError:
#         return jsonify({'error': 'Token has expired'}), 401
#     except jwt.InvalidTokenError:
#         return jsonify({'error': 'Invalid token'}), 401

#     try:
#         # Create the engine for the user-specific database
#         user_engine = create_engine(db_url)
        
#         # Create metadata object and load the table dynamically based on quarter, country, and year
#         metadata = MetaData()
#         table_name = f"skuwiseyearly_{user_id}_{country.lower()}_{year}_table"
#         user_specific_table = Table(table_name, metadata, autoload_with=user_engine)

#         # Fetch data from the table
#         with user_engine.connect() as conn:
#             query = user_specific_table.select()
#             results = conn.execute(query).mappings().all()  # Use .mappings() to get a dictionary-like result
        
#         # Convert RowMapping to plain dictionaries
#         result_dicts = [dict(row) for row in results]
        
#         # Return the results with a 200 OK status
#         return jsonify(result_dicts), 200  # Explicitly return 200 status code

#     except SQLAlchemyError as e:
#         print(f"Database error: {str(e)}")
#         return jsonify({'error': 'Error accessing the database'}), 500
#     except Exception as e:
#         print(f"Unexpected error: {str(e)}")
#         return jsonify({'error': 'An error occurred while fetching table data'}), 500

def resolve_country(country, currency):
    country = (country or "").lower()
    currency = (currency or "").lower()

    # 1. If country = global
    if country == "global":
        if currency == "usd":
            return "global"
        elif currency == "inr":
            return "global_inr"
        elif currency == "gbp":
            return "global_gbp"
        elif currency == "cad":
            return "global_cad"
        else:
            return "global"  # default fallback

    # 2. If country = uk
    if country == "uk":
        if currency == "usd":
            return "uk_usd"
        else:
            return "uk"  # default for all other currencies

    # 3. Default (no special logic)
    return country

@product_bp.route('/YearlySKU', methods=['GET'])
def YearlySKU():
    country = (request.args.get('country') or '').lower()
    country_param = request.args.get('country', '').lower()
    currency_param = (request.args.get('homeCurrency') or 'USD').lower()

    country = resolve_country(country_param, currency_param)

    year = (request.args.get('year') or '').strip()

    # Validate the query parameters
    if not country or not year:
        return jsonify({'error': 'Country and year are required'}), 400

    auth_header = request.headers.get('Authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        return jsonify({'error': 'Authorization token is missing or invalid'}), 401

    token = auth_header.split(' ')[1]
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=['HS256'])
        user_id = payload['user_id']
    except jwt.ExpiredSignatureError:
        return jsonify({'error': 'Token has expired'}), 401
    except jwt.InvalidTokenError:
        return jsonify({'error': 'Invalid token'}), 401

    try:
        engine = create_engine(db_url)
        metadata = MetaData(schema='public')  # align with other routes
        table_name = f"skuwiseyearly_{user_id}_{country}_{year}_table"

        try:
            user_specific_table = Table(table_name, metadata, autoload_with=engine)
        except Exception:
            return jsonify({'error': f"Table '{table_name}' not found for user {user_id}"}), 404

        with engine.connect() as conn:
            results = conn.execute(select(*user_specific_table.columns)).mappings().all()

        # 🔒 Normalize all rows so the UI gets true numbers, not strings
        data = [_normalize_sku_row(dict(row)) for row in results]
        return jsonify(data), 200

    except SQLAlchemyError as e:
        print(f"Database error: {str(e)}")
        return jsonify({'error': 'Error accessing the database'}), 500
    except Exception as e:
        print(f"Unexpected error: {str(e)}")
        return jsonify({'error': 'An error occurred while fetching table data'}), 500
 
    
# @product_bp.route('/quarterlyskutable', methods=['GET'])
# def quarterlyskutable():
#     # Extract query parameters from the URL
#     quarter = request.args.get('quarter')
#     country = request.args.get('country')
#     year = request.args.get('year')

#     # Validate the query parameters
#     if not quarter or not country or not year:
#         return jsonify({'error': 'Quarter, country, and year are required'}), 400

#     auth_header = request.headers.get('Authorization')
#     if not auth_header or not auth_header.startswith('Bearer '):
#         return jsonify({'error': 'Authorization token is missing or invalid'}), 401

#     token = auth_header.split(' ')[1]
#     try:
#         payload = jwt.decode(token, SECRET_KEY, algorithms=['HS256'])
#         user_id = payload['user_id']
#     except jwt.ExpiredSignatureError:
#         return jsonify({'error': 'Token has expired'}), 401
#     except jwt.InvalidTokenError:
#         return jsonify({'error': 'Invalid token'}), 401

#     try:
#         table_name = f"{quarter}_{user_id}_{country}_{year}_table"
#         engine = create_engine(db_url)
#         metadata = MetaData(schema='public')

#         try:
#             user_specific_table = Table(table_name, metadata, autoload_with=engine)
#             with engine.connect() as conn:
#                 query = select(*user_specific_table.columns)
#                 results = conn.execute(query).mappings().all()
#             return jsonify([dict(row) for row in results])
#         except:
#             return jsonify({'error': f"Table '{table_name}' not found for user {user_id}"}), 404

#     except:
#         return jsonify({'error': 'An unexpected error occurred'}), 500

def resolve_country(country, currency):
    country = (country or "").lower()
    currency = (currency or "").lower()

    # 1. If country = global
    if country == "global":
        if currency == "usd":
            return "global"
        elif currency == "inr":
            return "global_inr"
        elif currency == "gbp":
            return "global_gbp"
        elif currency == "cad":
            return "global_cad"
        else:
            return "global"  # default fallback

    # 2. If country = uk
    if country == "uk":
        if currency == "usd":
            return "uk_usd"
        else:
            return "uk"  # default for all other currencies

    # 3. Default (no special logic)
    return country


@product_bp.route('/quarterlyskutable', methods=['GET'])
def quarterlyskutable():
    # Extract query parameters from the URL
    quarter = request.args.get('quarter')
    country_param = request.args.get('country', '')
    currency_param = (request.args.get('homeCurrency') or 'USD').lower()
    print("currency-------------", currency_param)

    country = resolve_country(country_param, currency_param)
    year = request.args.get('year')

    # Validate the query parameters
    if not quarter or not country or not year:
        return jsonify({'error': 'Quarter, country, and year are required'}), 400

    auth_header = request.headers.get('Authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        return jsonify({'error': 'Authorization token is missing or invalid'}), 401

    token = auth_header.split(' ')[1]
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=['HS256'])
        user_id = payload['user_id']
    except jwt.ExpiredSignatureError:
        return jsonify({'error': 'Token has expired'}), 401
    except jwt.InvalidTokenError:
        return jsonify({'error': 'Invalid token'}), 401

    try:
        table_name = f"{quarter}_{user_id}_{country}_{year}_table"
        engine = create_engine(db_url)
        metadata = MetaData(schema='public')

        try:
            user_specific_table = Table(table_name, metadata, autoload_with=engine)
            with engine.connect() as conn:
                query = select(*user_specific_table.columns)
                results = conn.execute(query).mappings().all()

            # 🔒 Normalize all rows so UI gets true numbers, not strings
            data = [_normalize_sku_row(dict(row)) for row in results]
            return jsonify(data), 200

        except Exception:
            return jsonify({'error': f"Table '{table_name}' not found for user {user_id}"}), 404

    except Exception:
        return jsonify({'error': 'An unexpected error occurred'}), 500


@product_bp.route('/currency-rates', methods=['GET'])
def get_currency_rates():
    auth_header = request.headers.get('Authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        return jsonify({'error': 'Authorization token is missing or invalid'}), 401

    token = auth_header.split(' ')[1]
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=['HS256'])
        user_id = payload['user_id']
    except jwt.ExpiredSignatureError:
        return jsonify({'error': 'Token has expired'}), 401
    except jwt.InvalidTokenError:
        return jsonify({'error': 'Invalid token'}), 401

    try:
        # Connect to the admin database where currency_conversion table is located
        admin_engine = create_engine(db_url1)  # Using admin database
        
        # Query the currency_conversion table
        with admin_engine.connect() as conn:
            # Get the most recent currency rates for each currency-country combination
            query = text("""
                SELECT DISTINCT ON (user_currency, country) 
                    user_currency, country, selected_currency, conversion_rate, month, year
                FROM currency_conversion 
                ORDER BY user_currency, country, year DESC, 
                    CASE month 
                        WHEN 'january' THEN 1 WHEN 'february' THEN 2 WHEN 'march' THEN 3 
                        WHEN 'april' THEN 4 WHEN 'may' THEN 5 WHEN 'june' THEN 6 
                        WHEN 'july' THEN 7 WHEN 'august' THEN 8 WHEN 'september' THEN 9 
                        WHEN 'october' THEN 10 WHEN 'november' THEN 11 WHEN 'december' THEN 12 
                    END DESC
            """)
            results = conn.execute(query).mappings().all()

        # Convert to list of dictionaries
        currency_rates = [dict(row) for row in results]
        return jsonify(currency_rates), 200

    except SQLAlchemyError as e:
        print(f"SQLAlchemy Error: {str(e)}")
        return jsonify({'error': 'Database error', 'message': str(e)}), 500
    except Exception as e:
        print(f"Unexpected Error: {str(e)}")
        return jsonify({'error': 'An error occurred while fetching currency rates', 'message': str(e)}), 500


@product_bp.route('/asp-data', methods=['GET'])
def get_asp_data():
    auth_header = request.headers.get('Authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        return jsonify({'error': 'Authorization token is missing or invalid'}), 401

    token = auth_header.split(' ')[1]
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=['HS256'])
        user_id = payload['user_id']
    except jwt.ExpiredSignatureError:
        return jsonify({'error': 'Token has expired'}), 401
    except jwt.InvalidTokenError:
        return jsonify({'error': 'Invalid token'}), 401

    # Get query parameters
    country = request.args.get('country', '')
    month = request.args.get('month', '')
    year = request.args.get('year', '')

    if not all([country, month, year]):
        return jsonify({'error': 'Country, month, and year parameters are required'}), 400

    try:
        # Connect to PostgreSQL
        user_engine = create_engine(db_url)
        inspector = inspect(user_engine)
        
        # Handle global country case - check multiple tables
        if country.lower() == 'global':
            # Try different country combinations for global
            countries_to_try = ['uk', 'us', 'canada']
            asp_data = []
            
            for try_country in countries_to_try:
                table_name = f"skuwisemonthly_{user_id}_{try_country}_{month}{year}"
                
                if table_name in inspector.get_table_names():
                    try:
                        metadata = MetaData()
                        asp_table = Table(table_name, metadata, autoload_with=user_engine)
                        
                        with user_engine.connect() as conn:
                            # Check for ASP column variants
                            if hasattr(asp_table.c, 'asp'):
                                query = select(asp_table.c.product_name, asp_table.c.asp)
                            elif hasattr(asp_table.c, 'net_credits'):
                                query = select(
                                    asp_table.c.product_name,
                                    asp_table.c.net_credits.label('asp')
                                )
                            elif hasattr(asp_table.c, 'average_selling_price'):
                                query = select(
                                    asp_table.c.product_name,
                                    asp_table.c.average_selling_price.label('asp')
                                )
                            else:
                                logger.warning(f"No ASP column found in table {table_name}")
                                continue
                                
                            results = conn.execute(query).mappings().all()
                            
                            # Add country info to distinguish data
                            for row in results:
                                row_dict = dict(row)
                                row_dict['source_country'] = try_country
                                asp_data.append(row_dict)
                                
                    except Exception as e:
                        logger.error(f"Error querying table {table_name}: {str(e)}")
                        continue
            
            if not asp_data:
                logger.info(f"No ASP data found for global view - {month}{year}")
                return jsonify({
                    'error': 'No ASP data found for global view',
                    'details': f'No data available for {month} {year}',
                    'suggestion': 'Try a previous month or check if data has been uploaded'
                }), 404
                
            logger.info(f"Successfully retrieved ASP data for global view - {month}{year}")
            return jsonify(asp_data), 200
        
        else:
            # Single country case
            table_name = f"skuwisemonthly_{user_id}_{country}_{month}{year}"
            
            # Check if the table exists
            if table_name not in inspector.get_table_names():
                logger.warning(f"ASP data table {table_name} not found")
                return jsonify({
                    'error': f'ASP data table "{table_name}" not found',
                    'details': f'No ASP data available for {country.upper()} in {month.capitalize()} {year}',
                    'suggestion': 'Try a previous month or check if data has been uploaded'
                }), 404

            # Load table metadata
            metadata = MetaData()
            asp_table = Table(table_name, metadata, autoload_with=user_engine)

            # Query the table for ASP data
            with user_engine.connect() as conn:
                # Check for different possible ASP column names
                if hasattr(asp_table.c, 'asp'):
                    query = select(asp_table.c.product_name, asp_table.c.asp)
                elif hasattr(asp_table.c, 'net_credits'):
                    query = select(
                        asp_table.c.product_name,
                        asp_table.c.net_credits.label('asp')
                    )
                elif hasattr(asp_table.c, 'average_selling_price'):
                    query = select(
                        asp_table.c.product_name,
                        asp_table.c.average_selling_price.label('asp')
                    )
                else:
                    # Return available columns for debugging
                    available_columns = [col.name for col in asp_table.columns]
                    logger.error(f"Cannot determine ASP column in {table_name}. Available columns: {available_columns}")
                    return jsonify({
                        'error': 'Cannot determine ASP column',
                        'available_columns': available_columns,
                        'table_name': table_name,
                        'suggestion': 'Check column names in the uploaded data'
                    }), 404
                    
                results = conn.execute(query).mappings().all()

            # Convert to list of dictionaries
            asp_data = [dict(row) for row in results]
            logger.info(f"Successfully retrieved ASP data for {country} - {month}{year}")
            return jsonify(asp_data), 200

    except SQLAlchemyError as e:
        logger.error(f"SQLAlchemy Error in get_asp_data: {str(e)}")
        return jsonify({
            'error': 'Database error', 
            'message': str(e),
            'suggestion': 'Contact support if this persists'
        }), 500
    except Exception as e:
        logger.error(f"Unexpected Error in get_asp_data: {str(e)}")
        return jsonify({
            'error': 'An error occurred while fetching ASP data', 
            'message': str(e),
            'suggestion': 'Contact support if this persists'
        }), 500

    
@product_bp.route('/skup', methods=['POST'])
def skup():
    auth_header = request.headers.get('Authorization')
    
    # Validate authorization token
    if not auth_header or not auth_header.startswith('Bearer '):
        return jsonify({'error': 'Authorization token is missing or invalid'}), 401

    token = auth_header.split(' ')[1]
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=['HS256'])
        user_id = payload['user_id']
    except jwt.ExpiredSignatureError:
        return jsonify({'error': 'Token has expired'}), 401
    except jwt.InvalidTokenError:
        return jsonify({'error': 'Invalid token'}), 401

    # Check if a file is included in the request
    if 'file' not in request.files:
        return jsonify({'error': 'No file part'}), 400

    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'No selected file'}), 400

    # Save the uploaded file
    file_path = os.path.join(UPLOAD_FOLDER, secure_filename(file.filename))
    file.save(file_path)

    return jsonify({'success': True, 'message': 'File uploaded successfully'}), 200

@product_bp.route('/updatePrices', methods=['POST'])
def update_prices():
    # Authorization
    auth_header = request.headers.get('Authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        return jsonify({'error': 'Authorization token is missing or invalid'}), 401

    token = auth_header.split(' ')[1]
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=['HS256'])
        user_id = payload['user_id']
    except jwt.ExpiredSignatureError:
        return jsonify({'error': 'Token has expired'}), 401
    except jwt.InvalidTokenError:
        return jsonify({'error': 'Invalid token'}), 401

    # Parse price update payload
    data = request.get_json()
    edited_prices = data.get('prices', {})  

    if not edited_prices:
        return jsonify({'error': 'No prices provided to update'}), 400

    # DB setup
    user_engine = create_engine(db_url)
    Session = sessionmaker(bind=user_engine)
    user_session = Session()
    sku_table_name = f"sku_{user_id}_data_table"
    sku_data_table = Table(sku_table_name, MetaData(), autoload_with=user_engine)

    failed_products = []
    updated_products = []

    try:
        # Fetch all existing product_names for validation
        existing_products_result = user_session.execute(
            select(sku_data_table.c.product_name)
        ).fetchall()
        existing_products = set(row[0] for row in existing_products_result)

        for product_name, new_price in edited_prices.items():
            if product_name not in existing_products:
                print(f"❌ Product not found in DB: {product_name}")
                failed_products.append(product_name)
                continue

            try:
                update_stmt = sku_data_table.update().where(
                    sku_data_table.c.product_name == product_name
                ).values(price=new_price)

                result = user_session.execute(update_stmt)

                if result.rowcount == 0:
                    print(f"⚠️ No rows updated for product: {product_name}")
                    failed_products.append(product_name)
                else:
                    print(f"✅ Updated product: {product_name} → New Price: {new_price}")
                    updated_products.append(product_name)

            except Exception as e:
                print(f"❌ Error updating product {product_name}: {str(e)}")
                failed_products.append(product_name)

        user_session.commit()

        # Return updated table
        select_stmt = select(sku_data_table).order_by(sku_data_table.c.id.asc())
        result = user_session.execute(select_stmt)
        updated_data = [dict(row._mapping) for row in result]

        return jsonify({
            'message': 'Prices update completed',
            'updated_products': updated_products,
            'not_updated_products': failed_products,
            'data': updated_data
        }), 200

    except Exception as e:
        user_session.rollback()
        return jsonify({'error': f'Error updating prices: {str(e)}'}), 500
    finally:
        user_session.close()


@product_bp.route('/skuprice', methods=['GET'])
def skuprice():
    auth_header = request.headers.get('Authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        return jsonify({'error': 'Authorization token is missing or invalid'}), 401

    token = auth_header.split(' ')[1]
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=['HS256'])
        user_id = payload['user_id']
    except jwt.ExpiredSignatureError:
        return jsonify({'error': 'Token has expired'}), 401
    except jwt.InvalidTokenError:
        return jsonify({'error': 'Invalid token'}), 401



    table_name = f"sku_{user_id}_data_table"

    try:
        # Connect to PostgreSQL
        user_engine = create_engine(db_url)
        inspector = inspect(user_engine)

        # Check if the table exists
        if table_name not in inspector.get_table_names():
            return jsonify({'error': f'Table "{table_name}" not found'}), 404

        # Load table metadata
        metadata = MetaData()
        sku_data_table = Table(table_name, metadata, autoload_with=user_engine)

        # Query the table
        with user_engine.connect() as conn:
            query = sku_data_table.select()
            results = conn.execute(query).mappings().all()

        # Convert to dict and return
        result_dicts = [dict(row) for row in results]
        return jsonify(result_dicts), 200

    except SQLAlchemyError as e:
        print(f"SQLAlchemy Error: {str(e)}")
        return jsonify({'error': 'Database error', 'message': str(e)}), 500
    except Exception as e:
        print(f"Unexpected Error: {str(e)}")
        traceback.print_exc()
        return jsonify({'error': 'An error occurred while fetching SKU data', 'message': str(e)}), 500



@product_bp.route('/get_error_file/<string:country>/<string:month>/<string:year>', methods=['GET'])
def get_error_file(country, month, year):
    auth_header = request.headers.get('Authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        return jsonify({'error': 'Authorization token is missing or invalid'}), 401

    token = auth_header.split(' ')[1]
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=['HS256'])
        user_id = payload['user_id']
    except jwt.ExpiredSignatureError:
        return jsonify({'error': 'Token has expired'}), 401
    except jwt.InvalidTokenError:
        return jsonify({'error': 'Invalid token'}), 401

    # Construct the filename for the error file (which is actually inventory_forecast)
    error_filename = f"error_file_{user_id}{country}{month}_{year}.xlsx"
    error_file_path = os.path.join(UPLOAD_FOLDER, error_filename)

    # Check if the file exists
    if not os.path.exists(error_file_path):
        return jsonify({'error': 'Error file not found'}), 404

    try:
        # Send the existing forecast file as a download
        return send_file(error_file_path, as_attachment=True)

    except Exception as e:
        print(f"Error: {str(e)}")
        return jsonify({'error': 'An error occurred while sending the error file'}), 500
   

# @product_bp.route('/get_table_data/<string:file_name>', methods=['GET'])
# def get_table_data(file_name):
#     # --- Authorization ---
#     auth_header = request.headers.get('Authorization')
#     if not auth_header or not auth_header.startswith('Bearer '):
#         return jsonify({'error': 'Authorization token is missing or invalid'}), 401

#     token = auth_header.split(' ')[1]
#     try:
#         payload = jwt.decode(token, SECRET_KEY, algorithms=['HS256'])
#         user_id = payload['user_id']
#     except jwt.ExpiredSignatureError:
#         return jsonify({'error': 'Token has expired'}), 401
#     except jwt.InvalidTokenError:
#         return jsonify({'error': 'Invalid token'}), 401

#     try:
#         # Connect to database
#         user_engine = create_engine(db_url)
#         connection = user_engine.connect()
        
#         print(f"Attempting to access table: {file_name}")
        
#         # Check if table exists
#         inspector = inspect(user_engine)
#         available_tables = inspector.get_table_names()
        
#         print(f"Available tables: {available_tables}")
        
#         if file_name not in available_tables:
#             return jsonify({'error': f'Table "{file_name}" does not exist', 'available_tables': available_tables}), 404
        
#         # Use text SQL to be more explicit and avoid SQLAlchemy object handling issues
#         from sqlalchemy import text
#         query = text(f'SELECT * FROM "{file_name}"')
        
#         # Execute query
#         result = connection.execute(query)
        
#         # Convert to list of dicts - using a safer method
#         rows = result.fetchall()
#         columns = result.keys()
        
#         # Build dictionary row by row
#         result_dicts = []
#         for row in rows:
#             row_dict = {}
#             for i, column in enumerate(columns):
#                 row_dict[column] = row[i]
#             result_dicts.append(row_dict)
            
#         connection.close()
        
#         return jsonify(result_dicts), 200

#     except SQLAlchemyError as e:
#         print(f"SQLAlchemy error: {str(e)}")
#         return jsonify({'error': 'Database error', 'message': str(e)}), 500

#     except Exception as e:
#         print(f"Unexpected error: {str(e)}")
#         import traceback
#         traceback.print_exc()  # Print full traceback for debugging
#         return jsonify({'error': 'An unexpected error occurred', 'message': str(e)}), 500


# @product_bp.route('/get_table_data/<string:file_name>', methods=['GET'])
# def get_table_data(file_name):
#     # --- Authorization ---
#     auth_header = request.headers.get('Authorization')
#     if not auth_header or not auth_header.startswith('Bearer '):
#         return jsonify({'error': 'Authorization token is missing or invalid'}), 401

#     token = auth_header.split(' ')[1]
#     try:
#         payload = jwt.decode(token, SECRET_KEY, algorithms=['HS256'])
#         user_id = payload['user_id']
#     except jwt.ExpiredSignatureError:
#         return jsonify({'error': 'Token has expired'}), 401
#     except jwt.InvalidTokenError:
#         return jsonify({'error': 'Invalid token'}), 401

#     try:
#         user_engine = create_engine(db_url)
#         connection = user_engine.connect()

#         print(f"\n🔍 Reading table: {file_name}")

#         inspector = inspect(user_engine)
#         available_tables = inspector.get_table_names()

#         if file_name not in available_tables:
#             return jsonify({'error': f'Table "{file_name}" does not exist'}), 404

#         query = text(f'SELECT * FROM "{file_name}"')
#         result = connection.execute(query)

#         rows = result.fetchall()
#         columns = list(result.keys())  # ✅ RMKeyView fix – convert to list

#         result_dicts = []
#         for row in rows:
#             row_dict = {columns[i]: row[i] for i in range(len(columns))}
#             result_dicts.append(row_dict)

#         ### ✅ Now Calculate Required Stats

#         def clean(v):
#             return str(v).strip().replace('"', '').lower()

#         total_cases = len(result_dicts)

#         # error / NoReferralFee / cases to be inquired
#         error_cases = sum(
#             1 for r in result_dicts
#             if clean(r.get("errorstatus")) in ["error", "noreferralfee", "cases to be inquired"]
#         )

#         # OK cases (CASE INSENSITIVE & REMOVE QUOTES / SPACES)
#         no_error_cases = sum(
#             1 for r in result_dicts
#             if clean(r.get("errorstatus")) == "ok"
#         )

#         # fees charged high (positive difference)
#         fees_high_cases = sum(
#             1 for r in result_dicts
#             if r.get("difference") not in [None, "", 0]
#             and float(r.get("difference")) > 0
#             and clean(r.get("errorstatus")) in ["error", "noreferralfee", "cases to be inquired"]
#         )

#         # fees charged low (negative difference)
#         fees_low_cases = sum(
#             1 for r in result_dicts
#             if r.get("difference") not in [None, "", 0]
#             and float(r.get("difference")) < 0
#             and clean(r.get("errorstatus")) in ["error", "noreferralfee", "cases to be inquired"]
#         )

#         ### ✅ Print values on terminal
#         print("\n=== STATISTICS FROM TABLE ===")
#         print(f"➡ Cases to be analyzed: {total_cases}")
#         print(f"➡ How many errors: {error_cases}")
#         print(f"➡ No error cases: {no_error_cases}")
#         print(f"➡ Cases where fees charged high (+ diff): {fees_high_cases}")
#         print(f"➡ Cases where fees charged low (- diff): {fees_low_cases}")
#         print("=================================\n")


        

#         # Convert DB rows into DataFrame
#         df = pd.DataFrame(result_dicts)

#         # Ensure numeric
#         # Keep only rows where sku is NOT blank / NOT null / NOT "0"
#         df_valid = df[
#             df["sku"].astype(str).str.strip().ne("") & 
#             df["sku"].astype(str).str.strip().ne("0")
#         ].copy()

#         # Convert required numeric columns
#         df_valid["sku"] = df_valid["sku"].astype(str).str.strip()
   
#         df_valid["difference"] = pd.to_numeric(df_valid["difference"], errors="coerce").fillna(0)
#         df_valid["applicable_fee"] = pd.to_numeric(df_valid["answer"], errors="coerce").fillna(0)
#         df_valid["charged_fee"] = pd.to_numeric(df_valid["selling_fees"], errors="coerce").fillna(0)
#         df_valid["product_sales"] = pd.to_numeric(df_valid["product_sales"], errors="coerce").fillna(0)
#         df_valid["promotional_rebates"] = pd.to_numeric(df_valid["promotional_rebates"], errors="coerce").fillna(0)
#         df_valid["other"] = pd.to_numeric(df_valid["other"], errors="coerce").fillna(0)

#         df_valid["Net Sales"] = (
#             df_valid["product_sales"]
#             + df_valid["promotional_rebates"]
#             + df_valid["other"]
#         )
#         # df_valid["Net Sales"] = pd.to_numeric(df_valid["product_sales"], errors="coerce").fillna(0)
#         # df_valid["Units"] = pd.to_numeric(df_valid["quantity"], errors="coerce").fillna(0)
#         # Clean columns inside df_valid
#         df_valid["sku"] = df_valid["sku"].astype(str).str.strip()
#         df_valid["type_norm"] = df_valid["type"].astype(str).str.strip().str.lower()
#         df_valid["quantity"] = pd.to_numeric(df_valid["quantity"], errors="coerce").fillna(0)

#         # Select only rows where type is order/shipment
#         mask = df_valid["type_norm"].isin(["order", "shipment"])

#         # Group by SKU and sum quantity for valid rows only
#         quantity_df = (
#             df_valid[mask]
#             .groupby("sku", as_index=False)["quantity"]
#             .sum()
#             .rename(columns={"quantity": "Units"})
#         )

#         df_valid = df_valid.merge(quantity_df, on="sku", how="left")
#         df_valid["Units"] = df_valid["Units"].fillna(0)

        





#         # Assign category based on difference
#         df_valid["Ref_Fee_Category"] = df_valid["difference"].apply(
#             lambda x: "Accurate" if x == 0 else ("Undercharged" if x < 0 else "Overcharged")
#         )

#         # Create summary table
#         summary = df_valid.groupby("Ref_Fee_Category").agg({
#             "Units": "sum",
#             "Net Sales": "sum",
#             "applicable_fee": "sum",
#             "charged_fee": "sum",
#             "difference": "sum"
#         }).reset_index()

#         # Rename columns for frontend
#         summary.columns = ["Ref Fees", "Units", "Sales", "Ref Fees Applicable", "Ref Fees Charged", "Overcharged"]

#         # Add total row
#         total_row = pd.DataFrame([{
#             "Ref Fees": "Total",
#             "Units": summary["Units"].sum(),
#             "Sales": summary["Sales"].sum(),
#             "Ref Fees Applicable": summary["Ref Fees Applicable"].sum(),
#             "Ref Fees Charged": summary["Ref Fees Charged"].sum(),
#             "Overcharged": summary["Overcharged"].sum()
#         }])

#         summary = pd.concat([summary, total_row], ignore_index=True)

#         print("\n✅ FINAL SUMMARY TABLE (SKU ≠ 0 & NOT BLANK):")
#         print(summary)


#         # table_name = f"referralfee_{user_id}_{country}_{month}_{year}_table"

#         # summary.to_sql(table_name, con=user_engine, index=False, if_exists="replace")

#         # print(f"\n✅ Saved to database → {table_name}")



#         connection.close()



#         ### ✅ Return both (table data + stats) to frontend
#         return jsonify({
#             "table_data": result_dicts,
#             "summary_table": summary.to_dict(orient="records"),
#             "stats": {
#                 "cases_to_be_analyzed": total_cases,
#                 "how_many_error": error_cases,
#                 "no_error": no_error_cases,
#                 "fees_charged_high": fees_high_cases,
#                 "fees_charged_low": fees_low_cases
#             }
#         }), 200

#     except SQLAlchemyError as e:
#         print(f"SQLAlchemy error: {str(e)}")
#         return jsonify({'error': 'Database error', 'message': str(e)}), 500

#     except Exception as e:
#         print(f"Unexpected error: {str(e)}")
#         import traceback
#         traceback.print_exc()
#         return jsonify({'error': 'Unexpected error occurred', 'message': str(e)}), 500


# @product_bp.route('/get_table_data/<string:file_name>', methods=['GET'])
# def get_table_data(file_name):
#     # --- Authorization ---
#     auth_header = request.headers.get('Authorization')
#     if not auth_header or not auth_header.startswith('Bearer '):
#         return jsonify({'error': 'Authorization token is missing or invalid'}), 401

#     token = auth_header.split(' ')[1]
#     try:
#         payload = jwt.decode(token, SECRET_KEY, algorithms=['HS256'])
#         user_id = payload['user_id']
#     except jwt.ExpiredSignatureError:
#         return jsonify({'error': 'Token has expired'}), 401
#     except jwt.InvalidTokenError:
#         return jsonify({'error': 'Invalid token'}), 401
    

#      # 🔹 Read country / month / year from query params
#     country = request.args.get('country')
#     month = request.args.get('month')
#     year = request.args.get('year')


#     print(f"\n🌍 Query params -> country={country}, month={month}, year={year}")
#     print(f"📁 File name from URL -> {file_name}\n")

#     try:
#         user_engine = create_engine(db_url)
#         connection = user_engine.connect()

#         print(f"\n🔍 Reading table: {file_name}")

#         inspector = inspect(user_engine)
#         available_tables = inspector.get_table_names()

#         if file_name not in available_tables:
#             return jsonify({'error': f'Table "{file_name}" does not exist'}), 404

#         query = text(f'SELECT * FROM "{file_name}"')
#         result = connection.execute(query)

#         rows = result.fetchall()
#         columns = list(result.keys())

#         sku_table_data = []
#         sku_table_name = None

#         if country and month and year:
#             sku_table_name = f"skuwisemonthly_{user_id}_{country.lower()}_{month}{year}".lower()
#             print(f"🔍 Trying to read sku-wise table: {sku_table_name}")

#             if sku_table_name in available_tables:
#                 sku_query = text(f'SELECT * FROM "{sku_table_name}"')
#                 sku_result = connection.execute(sku_query)
#                 sku_rows = sku_result.fetchall()
#                 sku_cols = list(sku_result.keys())

#                 for row in sku_rows:
#                     row_dict = {sku_cols[i]: row[i] for i in range(len(sku_cols))}
#                     sku_table_data.append(row_dict)

#                 print(f"✅ Loaded {len(sku_table_data)} rows from {sku_table_name}")
#             else:
#                 print(f"⚠️ SKU-wise table '{sku_table_name}' does not exist")


#         result_dicts = []
#         for row in rows:
#             row_dict = {columns[i]: row[i] for i in range(len(columns))}
#             result_dicts.append(row_dict)

#         ### ✅ Calculate Required Stats
#         def clean(v):
#             return str(v).strip().replace('"', '').lower()

#         total_cases = len(result_dicts)

#         # error / NoReferralFee / cases to be inquired
#         error_cases = sum(
#             1 for r in result_dicts
#             if clean(r.get("errorstatus")) in ["error", "noreferralfee", "cases to be inquired"]
#         )

#         # OK cases (CASE INSENSITIVE & REMOVE QUOTES / SPACES)
#         no_error_cases = sum(
#             1 for r in result_dicts
#             if clean(r.get("errorstatus")) == "ok"
#         )

#         # fees charged high (positive difference)
#         fees_high_cases = sum(
#             1 for r in result_dicts
#             if r.get("difference") not in [None, "", 0]
#             and float(r.get("difference")) > 0
#             and clean(r.get("errorstatus")) in ["error", "noreferralfee", "cases to be inquired"]
#         )

#         # fees charged low (negative difference)
#         fees_low_cases = sum(
#             1 for r in result_dicts
#             if r.get("difference") not in [None, "", 0]
#             and float(r.get("difference")) < 0
#             and clean(r.get("errorstatus")) in ["error", "noreferralfee", "cases to be inquired"]
#         )

#         print("\n=== STATISTICS FROM TABLE ===")
#         print(f"➡ Cases to be analyzed: {total_cases}")
#         print(f"➡ How many errors: {error_cases}")
#         print(f"➡ No error cases: {no_error_cases}")
#         print(f"➡ Cases where fees charged high (+ diff): {fees_high_cases}")
#         print(f"➡ Cases where fees charged low (- diff): {fees_low_cases}")
#         print("=================================\n")

#         # Convert DB rows into DataFrame
#         df = pd.DataFrame(result_dicts)

#         # Keep only rows where sku is NOT blank / NOT null / NOT "0"
#         df_valid = df[
#             df["sku"].astype(str).str.strip().ne("") & 
#             df["sku"].astype(str).str.strip().ne("0")
#         ].copy()

#         # Convert required numeric columns
#         df_valid["sku"] = df_valid["sku"].astype(str).str.strip()
#         df_valid["difference"] = pd.to_numeric(df_valid["difference"], errors="coerce").fillna(0)
#         df_valid["applicable_fee"] = pd.to_numeric(df_valid["answer"], errors="coerce").fillna(0)
#         df_valid["charged_fee"] = pd.to_numeric(df_valid["selling_fees"], errors="coerce").fillna(0)
#         df_valid["product_sales"] = pd.to_numeric(df_valid["product_sales"], errors="coerce").fillna(0)
#         df_valid["promotional_rebates"] = pd.to_numeric(df_valid["promotional_rebates"], errors="coerce").fillna(0)
#         df_valid["other"] = pd.to_numeric(df_valid["other"], errors="coerce").fillna(0)
#         df_valid["quantity"] = pd.to_numeric(df_valid["quantity"], errors="coerce").fillna(0)

#         # Calculate Net Sales
#         df_valid["Net Sales"] = (
#             df_valid["product_sales"]
#             + df_valid["promotional_rebates"]
#             + df_valid["other"]
#         )

#         # Clean errorstatus and type columns
#         df_valid["errorstatus_clean"] = df_valid["errorstatus"].astype(str).str.strip().str.lower()
#         df_valid["type_clean"] = df_valid["type"].astype(str).str.strip().str.lower()

#         # ✅ UNITS CALCULATION - Based on errorstatus and difference
#         # Accurate: errorstatus = "ok"
#         # Overcharged: difference > 0
#         # Undercharged: difference < 0
#         # Only count rows where type is "order" or "shipment"
        
#         # Create a mask for valid type
#         type_mask = df_valid["type_clean"].isin(["order", "shipment"])
        
#         # Calculate units for each category
#         accurate_units = df_valid[
#             (df_valid["errorstatus_clean"] == "ok") & type_mask
#         ]["quantity"].sum()
        
#         overcharged_units = df_valid[
#             (df_valid["difference"] > 0) & type_mask
#         ]["quantity"].sum()
        
#         undercharged_units = df_valid[
#             (df_valid["difference"] < 0) & type_mask
#         ]["quantity"].sum()

#         # ✅ SALES, FEES CALCULATION - Based on errorstatus and difference
#         # Accurate rows: errorstatus = "ok"
#         accurate_mask = df_valid["errorstatus_clean"] == "ok"
#         accurate_sales = df_valid[accurate_mask]["Net Sales"].sum()
#         accurate_applicable = df_valid[accurate_mask]["applicable_fee"].sum()
#         accurate_charged = df_valid[accurate_mask]["charged_fee"].sum()
#         accurate_diff = df_valid[accurate_mask]["difference"].sum()

#         # Overcharged rows: difference > 0
#         overcharged_mask = df_valid["difference"] > 0
#         overcharged_sales = df_valid[overcharged_mask]["Net Sales"].sum()
#         overcharged_applicable = df_valid[overcharged_mask]["applicable_fee"].sum()
#         overcharged_charged = df_valid[overcharged_mask]["charged_fee"].sum()
#         overcharged_diff = df_valid[overcharged_mask]["difference"].sum()

#         # Undercharged rows: difference < 0
#         undercharged_mask = df_valid["difference"] < 0
#         undercharged_sales = df_valid[undercharged_mask]["Net Sales"].sum()
#         undercharged_applicable = df_valid[undercharged_mask]["applicable_fee"].sum()
#         undercharged_charged = df_valid[undercharged_mask]["charged_fee"].sum()
#         undercharged_diff = df_valid[undercharged_mask]["difference"].sum()

#         # Create summary table
#         summary = pd.DataFrame([
#             {
#                 "Ref Fees": "Accurate",
#                 "Units": accurate_units,
#                 "Sales": accurate_sales,
#                 "Ref Fees Applicable": accurate_applicable,
#                 "Ref Fees Charged": accurate_charged,
#                 "Overcharged": accurate_diff
#             },
#             {
#                 "Ref Fees": "Undercharged",
#                 "Units": undercharged_units,
#                 "Sales": undercharged_sales,
#                 "Ref Fees Applicable": undercharged_applicable,
#                 "Ref Fees Charged": undercharged_charged,
#                 "Overcharged": undercharged_diff
#             },
#             {
#                 "Ref Fees": "Overcharged",
#                 "Units": overcharged_units,
#                 "Sales": overcharged_sales,
#                 "Ref Fees Applicable": overcharged_applicable,
#                 "Ref Fees Charged": overcharged_charged,
#                 "Overcharged": overcharged_diff
#             }
#         ])

#         # Add total row
#         total_row = pd.DataFrame([{
#             "Ref Fees": "Total",
#             "Units": summary["Units"].sum(),
#             "Sales": summary["Sales"].sum(),
#             "Ref Fees Applicable": summary["Ref Fees Applicable"].sum(),
#             "Ref Fees Charged": summary["Ref Fees Charged"].sum(),
#             "Overcharged": summary["Overcharged"].sum()
#         }])

#         summary = pd.concat([summary, total_row], ignore_index=True)

#         print("\n✅ FINAL SUMMARY TABLE:")
#         print(summary)

#         connection.close()

#         return jsonify({
#             "table_data": result_dicts,
#             "summary_table": summary.to_dict(orient="records"),
#             "stats": {
#                 "cases_to_be_analyzed": total_cases,
#                 "how_many_error": error_cases,
#                 "no_error": no_error_cases,
#                 "fees_charged_high": fees_high_cases,
#                 "fees_charged_low": fees_low_cases
#             },
#             # ⭐ NEW: sku-wise table returned to frontend
#             "skuwise_table_name": sku_table_name,
#             "skuwise_table_data": sku_table_data,
#             "meta": {
#                 "file_name": file_name,
#                 "country": country,
#                 "month": month,
#                 "year": year,
#                 "user_id": user_id
#             }
#         }), 200

#     except SQLAlchemyError as e:
#         print(f"SQLAlchemy error: {str(e)}")
#         return jsonify({'error': 'Database error', 'message': str(e)}), 500

#     except Exception as e:
#         print(f"Unexpected error: {str(e)}")
#         import traceback
#         traceback.print_exc()
#         return jsonify({'error': 'Unexpected error occurred', 'message': str(e)}), 500



@product_bp.route('/get_table_data/<string:file_name>', methods=['GET'])
def get_table_data(file_name):
    # --- Auth ---
    auth_header = request.headers.get('Authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        return jsonify({'error': 'Authorization token missing'}), 401

    token = auth_header.split(' ')[1]
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=['HS256'])
        user_id = payload['user_id']
    except:
        return jsonify({'error': 'Invalid or expired token'}), 401

    country = request.args.get('country')
    month = request.args.get('month')
    year = request.args.get('year')

    print("\n============================")
    print("🔍 DEBUG: Request Received")
    print("file_name:", file_name)
    print("country:", country)
    print("month:", month)
    print("year:", year)
    print("============================\n")

    try:
        engine = create_engine(db_url)
        conn = engine.connect()

        inspector = inspect(engine)
        tables = inspector.get_table_names()

        print("🔍 DEBUG: Existing tables in DB:", tables)

        if file_name not in tables:
            print("❌ Table does NOT exist in DB:", file_name)
            return jsonify({'error': f'Table {file_name} not found'}), 404

        print("🔍 DEBUG: Reading table:", file_name)
        raw_df = pd.read_sql(text(f'SELECT * FROM "{file_name}"'), conn)
        raw_table_data = raw_df.to_dict(orient="records")


        print("🔍 DEBUG: Raw rows fetched:", len(raw_df))

        df = raw_df.copy()
        df["sku"] = df["sku"] = df["sku"].astype(str).str.strip()

        df = df[df["sku"].ne("") & df["sku"].ne("0")]

        print("🔍 DEBUG: After SKU cleanup:", len(df))

        numeric_cols = [
            "product_sales", "promotional_rebates", "other",
            "selling_fees", "answer", "difference", "quantity", "total_value"
        ]

        print("🔍 DEBUG: Converting numeric columns:", numeric_cols)

        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
            else:
                print(f"⚠ WARNING: Column missing in DB → {col}")

        df["net_sales_total_value"] = (
            df["product_sales"] +
            df["promotional_rebates"] +
            df["other"]
        )

        print("🔍 DEBUG: Net Sales calculation done.")

        def status_row(row):
            if str(row["errorstatus"]).lower() == "ok":
                return "Accurate"
            if str(row["errorstatus"]).lower() == "overcharged":
                return "Overcharged"
            if str(row["errorstatus"]).lower() == "undercharged":
                return "Undercharged"
            return "noreferallfee"

        df["status"] = df.apply(status_row, axis=1)

        print("🔍 DEBUG: Status column generated.")

        req_cols = [
            "sku", "product_name", "product_sales",
            "net_sales_total_value", "selling_fees",
            "answer", "errorstatus", "difference", "status","quantity", "total_value"
        ]

        final_df = df[req_cols]

        # --- SKU wise aggregation ---
        agg_cols = [
            "product_sales",
            "net_sales_total_value",
            "selling_fees",
            "answer",
            "difference",
            "quantity",
            "total_value"
        ]

        final_df = final_df.groupby(
            ["sku", "product_name", "status"], as_index=False
        )[agg_cols].sum()


        accurate_df = final_df[final_df["status"] == "Accurate"]
        under_df    = final_df[final_df["status"] == "Undercharged"]
        over_df     = final_df[final_df["status"] == "Overcharged"]
        ref_df      = final_df[final_df["status"] == "noreferallfee"]

        def create_total_row(df, label):
            return pd.DataFrame([{
                "sku": f"Charge - {label}",
                "product_name": "",
                "product_sales": df["product_sales"].sum(),
                "net_sales_total_value": df["net_sales_total_value"].sum(),
                "selling_fees": df["selling_fees"].sum(),
                "answer": df["answer"].sum(),
                "quantity": df["quantity"].sum(),
                "total_value": df["total_value"].sum(),
                "errorstatus": "",
                "difference": df["difference"].sum(),
                "status": label
            }])

        acc_total  = create_total_row(accurate_df, "Accurate")
        under_total = create_total_row(under_df, "Undercharged")
        over_total  = create_total_row(over_df, "Overcharged")
        ref_total  = create_total_row(ref_df, "noreferallfee")

        grand_total = pd.DataFrame([{
            "sku": "Grand Total",
            "product_name": "",
            "product_sales": final_df["product_sales"].sum(),
            "net_sales_total_value": final_df["net_sales_total_value"].sum(),
            "selling_fees": final_df["selling_fees"].sum(),
            "answer": final_df["answer"].sum(),
            "quantity": final_df["quantity"].sum(),
            "total_value": final_df["total_value"].sum(),
            "errorstatus": "",
            "difference": final_df["difference"].sum(),
            "status": "Total"
        }])

        final_display_df = pd.concat([
            acc_total, accurate_df,
            under_total, under_df,
            over_total, over_df,
            ref_total, ref_df,
            grand_total
        ], ignore_index=True)

        # now save this one
        final_df = final_display_df

        print("🔍 FINAL DF ROWS:", len(final_df))
        print("🔍 FINAL DF COLUMNS:", final_df.columns.tolist())

        # -------------------------------------------------------
        # SAVE TO POSTGRES — FIXED!
        # -------------------------------------------------------
        if country and month and year:
            skutable = f"skuwise_{user_id}_{country}_{month}{year}".lower()

            print("\n🔍 DEBUG: Saving table to DB:", skutable)

            final_df.to_sql(
                skutable,
                engine,   # ✅ NOT conn
                if_exists="replace",
                index=False
            )

            print("✅ SUCCESS: Table saved to PostgreSQL:", skutable)
        else:
            skutable = None
            print("⚠ DEBUG: country/month/year missing → NOT saving table.")

        conn.close()

        import numpy as np
        final_df = final_df.replace({np.nan: 0})

        return jsonify({
            "success": True,
            "message": "SKU wise table generated successfully.",
            "table": final_df.to_dict(orient="records"),
            "created_table_name": skutable,
            "raw_table": raw_table_data,
            "table_name": file_name,    
        })

    except Exception as e:
        print("🔥 ERROR:", str(e))
        return jsonify({"error": str(e)}), 500


@product_bp.route('/get_consolidated_table_name/<string:country_name>', methods=['GET'])
def get_consolidated_table_name(country_name):
    # Authorization
    auth_header = request.headers.get('Authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        return jsonify({'error': 'Authorization token is missing or invalid'}), 401

    token = auth_header.split(' ')[1]
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=['HS256'])
        user_id = payload['user_id']
    except jwt.ExpiredSignatureError:
        return jsonify({'error': 'Token has expired'}), 401
    except jwt.InvalidTokenError:
        return jsonify({'error': 'Invalid token'}), 401

    # Sanitize country name to form a safe table name
    def sanitize_identifier(identifier):
        return re.sub(r'\W|^(?=\d)', '_', identifier)

    safe_country_name = sanitize_identifier(country_name)
    consolidated_table_name = f"user_{user_id}_{safe_country_name}_merge_data_of_all_months"

    try:
        # Create engine
        user_engine = create_engine(db_url)
        inspector = inspect(user_engine)

        # Check if the table exists
        if consolidated_table_name not in inspector.get_table_names():
            return jsonify({'error': f'Table "{consolidated_table_name}" not found for user {user_id}'}), 404

        # Define and query the table
        metadata = MetaData()
        consolidated_table = Table(consolidated_table_name, metadata, autoload_with=user_engine)

        with user_engine.connect() as conn:
            results = conn.execute(consolidated_table.select()).mappings().all()

        result_dicts = [dict(row) for row in results]
        return jsonify(result_dicts), 200

    except SQLAlchemyError as e:
        print(f"SQLAlchemy Error: {str(e)}")
        return jsonify({'error': 'Database error', 'message': str(e)}), 500
    except Exception as e:
        print(f"Unexpected Error: {str(e)}")
        traceback.print_exc()
        return jsonify({'error': 'An unexpected error occurred', 'message': str(e)}), 500



# @product_bp.route('/skutableprofit/<string:skuwise_file_name>', methods=['GET'])
# def skutableprofit(skuwise_file_name):
#     auth_header = request.headers.get('Authorization')
#     if not auth_header or not auth_header.startswith('Bearer '):
#         return jsonify({'error': 'Authorization token is missing or invalid'}), 401

#     token = auth_header.split(' ')[1]
#     try:
#         payload = jwt.decode(token, SECRET_KEY, algorithms=['HS256'])
#         user_id = payload['user_id']
#     except jwt.ExpiredSignatureError:
#         return jsonify({'error': 'Token has expired'}), 401
#     except jwt.InvalidTokenError:
#         return jsonify({'error': 'Invalid token'}), 401

#     try:
#         engine = create_engine(db_url)
#         country = request.args.get('country', '')
#         month = request.args.get('month', '')
#         year = request.args.get('year', '')

#         # Determine table name based on country
#         if country == 'global':
#             table_name = f"skuwisemonthly_{user_id}_{country}_{month}{year}_table"
#         elif country and all([month, year]):
#             table_name = f"skuwise_{user_id}{country}{month}{year}"
#         else:
#             table_name = skuwise_file_name

#         metadata = MetaData(schema='public')

#         try:
#             user_specific_table = Table(table_name, metadata, autoload_with=engine)
#             with engine.connect() as conn:
#                 query = select(*user_specific_table.columns)
#                 results = conn.execute(query).mappings().all()
#             return jsonify([dict(row) for row in results])
#         except:
#             # If the specific table was not found, try the fallback
#             if table_name != skuwise_file_name:
#                 try:
#                     fallback_table = Table(skuwise_file_name, metadata, autoload_with=engine)
#                     with engine.connect() as conn:
#                         query = select(*fallback_table.columns)
#                         results = conn.execute(query).mappings().all()
#                     return jsonify([dict(row) for row in results])
#                 except:
#                     return jsonify({'error': f"Table '{table_name}' or '{skuwise_file_name}' not found for user {user_id}"}), 404
#             else:
#                 return jsonify({'error': f"Table '{table_name}' not found for user {user_id}"}), 404

#     except:
#         return jsonify({'error': 'An unexpected error occurred'}), 500

def resolve_country(country, currency):
    country = (country or "").lower()
    currency = (currency or "").lower()

    # 1. If country = global
    if country == "global":
        if currency == "usd":
            return "global"
        elif currency == "inr":
            return "global_inr"
        elif currency == "gbp":
            return "global_gbp"
        elif currency == "cad":
            return "global_cad"
        else:
            return "global"  # default fallback

    # 2. If country = uk
    if country == "uk":
        if currency == "usd":
            return "uk_usd"
        else:
            return "uk"  # default for all other currencies

    # 3. Default (no special logic)
    return country


@product_bp.route('/skutableprofit/<string:skuwise_file_name>', methods=['GET'])
def skutableprofit(skuwise_file_name):
    auth_header = request.headers.get('Authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        return jsonify({'error': 'Authorization token is missing or invalid'}), 401

    token = auth_header.split(' ')[1]
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=['HS256'])
        user_id = payload['user_id']
    except jwt.ExpiredSignatureError:
        return jsonify({'error': 'Token has expired'}), 401
    except jwt.InvalidTokenError:
        return jsonify({'error': 'Invalid token'}), 401

    try:
        engine = create_engine(db_url)
        country_param = request.args.get('country', '')
        currency_param = (request.args.get('homeCurrency') or 'USD').lower()

        country = resolve_country(country_param, currency_param)
        month = request.args.get('month', '')
        year = request.args.get('year', '')

        # Determine table name based on country
        if country.startswith("global"):
            table_name = f"skuwisemonthly_{user_id}_{country}_{month}{year}_table"
        elif country and all([month, year]):
            table_name = f"skuwise_{user_id}{country}{month}{year}"
        else:
            table_name = skuwise_file_name

        metadata = MetaData(schema='public')

        def _fetch_as_dicts(tbl_name: str):
            user_specific_table = Table(tbl_name, metadata, autoload_with=engine)
            with engine.connect() as conn:
                query = select(*user_specific_table.columns)
                results = conn.execute(query).mappings().all()
            # 🔒 normalize **every** row so UI receives numbers, not strings
            return [_normalize_sku_row(dict(row)) for row in results]

        try:
            data = _fetch_as_dicts(table_name)
            return jsonify(data), 200
        except Exception:
            # Fallback to provided table name, if different
            if table_name != skuwise_file_name:
                try:
                    data = _fetch_as_dicts(skuwise_file_name)
                    return jsonify(data), 200
                except Exception:
                    return jsonify({'error': f"Table '{table_name}' or '{skuwise_file_name}' not found for user {user_id}"}), 404
            else:
                return jsonify({'error': f"Table '{table_name}' not found for user {user_id}"}), 404

    except Exception:
        return jsonify({'error': 'An unexpected error occurred'}), 500

