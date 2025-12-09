
from datetime import datetime
from flask import Blueprint, request, jsonify
import jwt
from sqlalchemy import create_engine, text
from sqlalchemy import inspect
import os
import base64
from config import Config
SECRET_KEY = Config.SECRET_KEY
UPLOAD_FOLDER = Config.UPLOAD_FOLDER
from dotenv import load_dotenv


load_dotenv()
db_url = os.getenv('DATABASE_URL', 'postgresql://postgres:password@localhost:5432/phormula')
db_url1= os.getenv('DATABASE_ADMIN_URL', 'postgresql://postgres:password@localhost:5432/admin_db')
skuwise_bp = Blueprint('skuwise_bp', __name__)

def encode_file_to_base64(file_path):
    with open(file_path, 'rb') as file:
        return base64.b64encode(file.read()).decode('utf-8')




# @skuwise_bp.route('/ProductwisePerformance', methods=['POST'])
# def productwise_performance():
#     try:
#         # Authentication
#         auth_header = request.headers.get('Authorization')
#         if not auth_header or not auth_header.startswith('Bearer '):
#             return jsonify({'error': 'Authorization token missing or invalid'}), 401

#         token = auth_header.split(' ')[1]
#         try:
#             payload = jwt.decode(token, SECRET_KEY, algorithms=["HS256"])
#             user_id = str(payload.get('user_id'))
#         except jwt.ExpiredSignatureError:
#             return jsonify({'error': 'Token expired'}), 401
#         except jwt.InvalidTokenError:
#             return jsonify({'error': 'Invalid token'}), 401

#         # Request data
#         data = request.get_json()
#         product_name = data.get('product_name')
#         time_range = data.get('time_range', 'Yearly')
#         year = data.get('year', datetime.now().year)
#         quarter = data.get('quarter')
#         requested_countries = data.get('countries', ['uk', 'us', 'global', 'global_inr', 'global_cad', 'global_gbp'])

#         if not product_name:
#             return jsonify({'error': 'Product name is required'}), 400


#         # DB connections
#         engine = create_engine(db_url)
#         engine1 = create_engine(db_url1)
#         conn = engine.connect()
#         conn1 = engine1.connect()
#         inspector = inspect(engine)
#         all_tables = inspector.get_table_names()

#         # Month definitions
#         month_mapping = {
#             'january': '01', 'february': '02', 'march': '03',
#             'april': '04', 'may': '05', 'june': '06',
#             'july': '07', 'august': '08', 'september': '09',
#             'october': '10', 'november': '11', 'december': '12'
#         }
#         quarter_months = {
#             '1': ['january', 'february', 'march'],
#             '2': ['april', 'may', 'june'],
#             '3': ['july', 'august', 'september'],
#             '4': ['october', 'november', 'december']
#         }

#         months_to_fetch = quarter_months.get(quarter, []) if time_range == 'Quarterly' and quarter else list(month_mapping.keys())
        

#         result_data = {}

#         def get_uk_conversion_rate(month, year):
#             try:
#                 currency_query = text("""
#                     SELECT conversion_rate
#                     FROM currency_conversion 
#                     WHERE lower(country) = 'us'
#                     AND lower(month) = :month 
#                     AND year = :year
#                     LIMIT 1
#                 """)
#                 result = conn1.execute(currency_query, {"month": month.lower(), "year": year}).fetchone()
#                 return result[0] if result else 1.0
#             except Exception as e:
#                 return 1.0

#         # Iterate over countries
#         for country in requested_countries:
#             country_data = []

#             for month in months_to_fetch:
#                 month_num = month_mapping[month]

#                 table_patterns = [
#                     f"skuwisemonthly_{user_id}_{country}_{month}{year}_table"
#                 ] if country.lower().startswith("global") else [
#                     f"skuwisemonthly_{user_id}_{country}_{month}{year}"
#                 ]

#                 total_sales = 0
#                 total_quantity = 0
#                 total_profit = 0
#                 total_asp = 0
#                 total_cost_of_unit_sold = 0
#                 table_found = False
#                 conversion_rate_applied = None

#                 conversion_rate = get_uk_conversion_rate(month, year) if country.lower() == 'uk' else 1.0

#                 for i, table_pattern in enumerate(table_patterns):
#                     matching_tables = [table for table in all_tables if table.lower() == table_pattern.lower()]

#                     if matching_tables:
#                         table_found = True
#                         table_name = matching_tables[0]
                       

#                         try:
#                             columns = [col['name'] for col in inspector.get_columns(table_name)]
#                             if {'product_name', 'net_sales', 'quantity', 'profit', 'asp', 'cost_of_unit_sold'}.issubset(columns):
#                                 query = text(f"""
#                                     SELECT net_sales, quantity, profit, asp, cost_of_unit_sold
#                                     FROM "{table_name}"
#                                     WHERE LOWER(TRIM(product_name)) = LOWER(TRIM(:product_name))
#                                 """)
#                                 result = conn.execute(query, {'product_name': product_name})
#                                 rows = result.fetchall()

#                                 table_sales = sum(float(row[0] or 0) for row in rows)
#                                 table_quantity = sum(int(row[1] or 0) for row in rows)
#                                 table_profit = sum(float(row[2] or 0) for row in rows)
#                                 table_asp = sum(float(row[3] or 0) for row in rows)
#                                 table_cost_of_unit_sold = sum(float(row[4] or 0) for row in rows)

#                                 if country.lower() == 'uk' and i == 0:
#                                     table_sales *= conversion_rate
#                                     table_profit *= conversion_rate
#                                     table_asp *= conversion_rate
#                                     table_cost_of_unit_sold *= conversion_rate
#                                     conversion_rate_applied = conversion_rate
                                    

#                                 total_sales += table_sales
#                                 total_quantity += table_quantity
#                                 total_profit += table_profit
#                                 total_asp += table_asp
#                                 total_cost_of_unit_sold += table_cost_of_unit_sold

                                
#                             else:
#                                 print(f"Skipping table {table_name}: required columns missing")
#                         except Exception as e:
#                             conn.rollback()
#                             print(f"Error querying table {table_name}: {str(e)}")

#                 # Safely calculate gross margin
#                 gross_margin = (total_profit / total_sales) * 100 if total_sales > 0 else 0

                


#                 country_data.append({
#                     'month': month.capitalize(),
#                     'month_num': month_num,
#                     'net_sales': total_sales if table_found else 0,
#                     'quantity': total_quantity if table_found else 0,
#                     'profit': total_profit if table_found else 0,
#                     'gross_margin': gross_margin,
#                     'year': year,
#                     'conversion_rate_applied': conversion_rate_applied
#                 })

#             country_data.sort(key=lambda x: x['month_num'])
#             result_data[country] = country_data

#         conn.close()
#         conn1.close()

#         return jsonify({
#             'success': True,
#             'product_name': product_name,
#             'time_range': time_range,
#             'year': year,
#             'quarter': quarter if time_range == 'Quarterly' else None,
#             'data': result_data,
#             'available_countries': list(result_data.keys())
#         })

#     except Exception as e:
#         print(f"Error in productwise_performance: {str(e)}")
#         return jsonify({'error': f'Internal server error: {str(e)}'}), 500


@skuwise_bp.route('/ProductwisePerformance', methods=['POST'])
def productwise_performance():
    try:
        # Authentication
        auth_header = request.headers.get('Authorization')
        if not auth_header or not auth_header.startswith('Bearer '):
            return jsonify({'error': 'Authorization token missing or invalid'}), 401

        token = auth_header.split(' ')[1]
        try:
            payload = jwt.decode(token, SECRET_KEY, algorithms=["HS256"])
            user_id = str(payload.get('user_id'))
        except jwt.ExpiredSignatureError:
            return jsonify({'error': 'Token expired'}), 401
        except jwt.InvalidTokenError:
            return jsonify({'error': 'Invalid token'}), 401

        # Request data
        data = request.get_json()
        product_name = data.get('product_name')
        time_range = data.get('time_range', 'Yearly')
        year = data.get('year', datetime.now().year)
        quarter = data.get('quarter')

        home_currency = (data.get('home_currency') or 'USD').upper()

        requested_countries = data.get(
            'countries',
            ['uk', 'us', 'global', 'global_inr', 'global_cad', 'global_gbp']
        )

        if not product_name:
            return jsonify({'error': 'Product name is required'}), 400

        # DB connections
        engine = create_engine(db_url)
        engine1 = create_engine(db_url1)
        conn = engine.connect()
        conn1 = engine1.connect()
        inspector = inspect(engine)
        all_tables = inspector.get_table_names()

        # Month definitions
        month_mapping = {
            'january': '01', 'february': '02', 'march': '03',
            'april': '04', 'may': '05', 'june': '06',
            'july': '07', 'august': '08', 'september': '09',
            'october': '10', 'november': '11', 'december': '12'
        }
        quarter_months = {
            '1': ['january', 'february', 'march'],
            '2': ['april', 'may', 'june'],
            '3': ['july', 'august', 'september'],
            '4': ['october', 'november', 'december']
        }

        if time_range == 'Quarterly' and quarter:
            months_to_fetch = quarter_months.get(str(quarter), [])
        else:
            months_to_fetch = list(month_mapping.keys())

        result_data = {}

        def get_uk_conversion_rate(month, year):
            try:
                currency_query = text("""
                    SELECT conversion_rate
                    FROM currency_conversion 
                    WHERE lower(country) = 'us'
                    AND lower(month) = :month 
                    AND year = :year
                    LIMIT 1
                """)
                result = conn1.execute(
                    currency_query,
                    {"month": month.lower(), "year": year}
                ).fetchone()
                return result[0] if result else 1.0
            except Exception:
                return 1.0

        # Iterate over requested countries
        for country in requested_countries:
            country_data = []

            for month in months_to_fetch:
                month_num = month_mapping[month]

                # global / global_gbp / global_inr ... all have *_table suffix
                if country.lower().startswith("global"):
                    table_patterns = [
                        f"skuwisemonthly_{user_id}_{country}_{month}{year}_table"
                    ]
                else:
                    table_patterns = [
                        f"skuwisemonthly_{user_id}_{country}_{month}{year}"
                    ]

                total_sales = 0.0
                total_quantity = 0
                total_profit = 0.0
                total_asp = 0.0
                total_cost_of_unit_sold = 0.0
                table_found = False
                conversion_rate_applied = None

                # Only fetch UK→USD rate if we're actually going to use USD
                if country.lower() == 'uk' and home_currency == 'USD':
                    conversion_rate = get_uk_conversion_rate(month, year)
                else:
                    conversion_rate = 1.0

                for i, table_pattern in enumerate(table_patterns):
                    matching_tables = [
                        table for table in all_tables
                        if table.lower() == table_pattern.lower()
                    ]

                    if not matching_tables:
                        continue

                    table_found = True
                    table_name = matching_tables[0]

                    try:
                        columns = [
                            col['name'] for col in inspector.get_columns(table_name)
                        ]
                        required_cols = {
                            'product_name',
                            'net_sales',
                            'quantity',
                            'profit',
                            'asp',
                            'cost_of_unit_sold',
                        }
                        if not required_cols.issubset(columns):
                            print(f"Skipping table {table_name}: required columns missing")
                            continue

                        query = text(f"""
                            SELECT net_sales, quantity, profit, asp, cost_of_unit_sold
                            FROM "{table_name}"
                            WHERE LOWER(TRIM(product_name)) = LOWER(TRIM(:product_name))
                        """)

                        rows = conn.execute(
                            query, {'product_name': product_name}
                        ).fetchall()

                        table_sales = sum(float(row[0] or 0) for row in rows)
                        table_quantity = sum(int(row[1] or 0) for row in rows)
                        table_profit = sum(float(row[2] or 0) for row in rows)
                        table_asp = sum(float(row[3] or 0) for row in rows)
                        table_cost_of_unit_sold = sum(
                            float(row[4] or 0) for row in rows
                        )

                        # For UK: only convert to USD when requested
                        if country.lower() == 'uk' and i == 0 and home_currency == 'USD':
                            table_sales *= conversion_rate
                            table_profit *= conversion_rate
                            table_asp *= conversion_rate
                            table_cost_of_unit_sold *= conversion_rate
                            conversion_rate_applied = conversion_rate

                        total_sales += table_sales
                        total_quantity += table_quantity
                        total_profit += table_profit
                        total_asp += table_asp
                        total_cost_of_unit_sold += table_cost_of_unit_sold

                    except Exception as e:
                        conn.rollback()
                        print(f"Error querying table {table_name}: {str(e)}")

                gross_margin = (
                    (total_profit / total_sales) * 100 if total_sales > 0 else 0.0
                )

                country_data.append({
                    'month': month.capitalize(),
                    'month_num': month_num,
                    'net_sales': total_sales if table_found else 0.0,
                    'quantity': total_quantity if table_found else 0,
                    'profit': total_profit if table_found else 0.0,
                    'gross_margin': gross_margin,
                    'year': year,
                    'conversion_rate_applied': conversion_rate_applied,
                })

            country_data.sort(key=lambda x: x['month_num'])
            result_data[country] = country_data

        conn.close()
        conn1.close()

        return jsonify({
            'success': True,
            'product_name': product_name,
            'time_range': time_range,
            'year': year,
            'quarter': quarter if time_range == 'Quarterly' else None,
            'data': result_data,
            'available_countries': list(result_data.keys())
        })

    except Exception as e:
        print(f"Error in productwise_performance: {str(e)}")
        return jsonify({'error': f'Internal server error: {str(e)}'}), 500



@skuwise_bp.route('/Product_search', methods=['GET'])
def product_search():
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

    search_query = request.args.get('query', '').strip()
    if not search_query:
        return jsonify({'error': 'Search query is required'}), 400

    try:
        db_url = os.getenv('DATABASE_URL')
        engine = create_engine(db_url)
        conn = engine.connect()

        table_name = f"sku_{user_id}_data_table"
        
        # Check if the table exists
        inspector = inspect(engine)
        if not inspector.has_table(table_name):
            return jsonify({'error': 'No data found for this user.'}), 404

        # Keep LIKE for search suggestions but add DISTINCT to avoid duplicates
        query = text(f"""
            SELECT DISTINCT product_name
            FROM {table_name}
            WHERE LOWER(product_name) LIKE LOWER(:search_query)
            ORDER BY product_name
            LIMIT 10
        """)
        
        results = conn.execute(query, {'search_query': f'%{search_query}%'}).fetchall()
        
        products = [{'product_name': row[0]} for row in results]

        conn.close()

        return jsonify({'products': products}), 200

    except Exception as e:
        print(f"Error searching products: {str(e)}")
        return jsonify({'error': f'Error searching products: {str(e)}'}), 500
    

@skuwise_bp.route('/Product_names', methods=['GET'])
def product_names():
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
        db_url = os.getenv('DATABASE_URL')
        engine = create_engine(db_url)
        conn = engine.connect()

        table_name = f"sku_{user_id}_data_table"

        # Check if the table exists
        inspector = inspect(engine)
        if not inspector.has_table(table_name):
            conn.close()
            return jsonify({'error': 'No data table found for this user.'}), 404

        # Fetch only product_name
        query = text(f"""
            SELECT DISTINCT product_name
            FROM {table_name}
            ORDER BY product_name ASC
        """)

        rows = conn.execute(query).fetchall()
        conn.close()

        product_list = [row[0] for row in rows]

        return jsonify({'product_names': product_list}), 200

    except Exception as e:
        print("Error fetching product names:", str(e))
        return jsonify({'error': str(e)}), 500
