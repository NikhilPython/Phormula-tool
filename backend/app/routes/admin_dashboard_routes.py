from flask import Blueprint, request, jsonify
from config import Config
SECRET_KEY = Config.SECRET_KEY
from sqlalchemy import text
import json
import io
import pandas as pd
from sqlalchemy import inspect
from app import db
from app.models.user_models import CurrencyConversion, Category, UserAdmin , User, UploadHistory, CountryProfile
from sqlalchemy.exc import IntegrityError

from flask import current_app
import os , jwt
from datetime import datetime


admin_dashboard_bp = Blueprint('admin_dashboard', __name__)




@admin_dashboard_bp.route('/admin/dashboard', methods=['GET'])
def get_admin_dashboard_data():
    # Authentication check - prioritize token over query parameter
    authenticated = False
    
    # Method 1: Check Authorization header (recommended)
    auth_header = request.headers.get('Authorization')
    if auth_header and auth_header.startswith('Bearer '):
        token = auth_header.split(' ')[1]
        try:
            # Verify the token
            decoded_token = jwt.decode(token, SECRET_KEY, algorithms=['HS256'])
            if decoded_token.get('admin_id'):
                authenticated = True
        except jwt.ExpiredSignatureError:
            return jsonify({'message': 'Token has expired'}), 401
        except jwt.InvalidTokenError:
            return jsonify({'message': 'Invalid token'}), 401
    
    # Method 2: Fallback to query parameter (for backward compatibility)
    if not authenticated:
        authenticated_user = request.args.get('authenticated_user')
        if authenticated_user:
            authenticated = True
    
    # If neither method worked, return unauthorized
    if not authenticated:
        return jsonify({'message': 'User not authenticated'}), 401

    
    email_to_search = request.args.get('email')

    if email_to_search:
        user = User.query.filter_by(email=email_to_search).first()
        user_admin = UserAdmin.query.filter_by(email=email_to_search).first()

        if not user and not user_admin:
            return jsonify({'message': 'No user found with that email'}), 404

        user_id = user.id if user else user_admin.user_id
        if not user_id:
            return jsonify({'message': 'User ID not found for this email'}), 404

        related_upload_history = UploadHistory.query.filter_by(user_id=user_id).all()
        related_profiles = CountryProfile.query.filter_by(user_id=user_id).all()

        # Define quarters
        quarter_months = {
            "quarter1": ["january", "february", "march"],
            "quarter2": ["april", "may", "june"],
            "quarter3": ["july", "august", "september"],
            "quarter4": ["october", "november", "december"]
        }

        skuwise_data = []
        engine = db.get_engine()
        inspector = inspect(engine)

        with engine.connect() as conn:
            for c in related_upload_history:
                if c.country.lower() == "global":
                    base_table = f"skuwisemonthly_{user_id}_{c.country.lower()}_{c.month.lower()}{c.year}_table"
                else:
                    base_table = f"skuwisemonthly_{user_id}_{c.country.lower()}_{c.month.lower()}{c.year}"
                yearly_base_table = f"skuwiseyearly_{user_id}_{c.country.lower()}_{c.year}_table"
                
                for table_name in [base_table, yearly_base_table]:
                    if inspector.has_table(table_name):
                        try:
                            result = conn.execute(text(f'SELECT * FROM "{table_name}" LIMIT 15'))
                            columns = result.keys()                                                        
                            fetched_rows = result.fetchall()
                            rows = []
                            for raw_row in fetched_rows:
                                row_dict = {}
                                for col, val in zip(columns, raw_row):
                                    if isinstance(val, (int, float)):
                                        row_dict[col] = round(val, 2)
                                    else:
                                        row_dict[col] = val
                                rows.append(row_dict)
                            skuwise_data.append({
                                'table': table_name,
                                'rows': rows
                            })
                        except Exception as e:
                            skuwise_data.append({
                                'table': table_name,
                                'error': str(e)
                            })
                    else:
                        skuwise_data.append({
                            'table': table_name,
                            'error': f"Table '{table_name}' does not exist"
                        })

                # 4. Quarterly tables (country-specific and global)
                month_lower = c.month.lower()
                for quarter, months in quarter_months.items():
                    if month_lower in months:
                        # Country-specific quarter table
                        quarter_table = f"{quarter}_{user_id}_{c.country.lower()}_{c.year}_table"

                        for qt in [quarter_table]:
                            if inspector.has_table(qt):
                                try:
                                    result = conn.execute(text(f'SELECT * FROM "{qt}" LIMIT 15'))
                                    columns = result.keys()
                                    fetched_rows = result.fetchall()
                                    rows = []

                                    for raw_row in fetched_rows:
                                        row_dict = {}
                                        for col, val in zip(columns, raw_row):
                                            if isinstance(val, (int, float)):
                                                row_dict[col] = round(val, 2)
                                            else:
                                                row_dict[col] = val
                                        rows.append(row_dict)
                                    skuwise_data.append({
                                        'table': qt,
                                        'rows': rows
                                    })
                                except Exception as e:
                                    skuwise_data.append({
                                        'table': qt,
                                        'error': str(e)
                                    })
                            else:
                                skuwise_data.append({
                                    'table': qt,
                                    'error': f"Table '{qt}' does not exist"
                                })
                        break  # Stop loop once the correct quarter is found

        return jsonify({
            'email': email_to_search,
            'user_id': user_id,
            'brand_name': user.brand_name if  user else user_admin.brand_name,
            'annual_sales_range': user.annual_sales_range if  user else user_admin.annual_sales_range,
            'related_upload_history': [{'id': c.id, 'user_id': c.user_id, 'country': c.country, 'month': c.month, 'year': c.year, 'total_sales': c.total_sales, 'total_profit': c.total_profit, 'total_expense': c.total_expense,} for c in related_upload_history],
            'related_country_profiles': [{'user_id': cp.user_id, 'country': cp.country, 'stock_unit': cp.stock_unit, 'transit_time': cp.transit_time} for cp in related_profiles],
            'skuwise_tables': skuwise_data
        }), 200
        
    user_admins = UserAdmin.query.all()

    
    users = User.query.all()

    return jsonify({
        'users': [{'id': u.id, 'email': u.email, 'brand_name': u.brand_name, 'annual_sales_range': u.annual_sales_range} for u in users],
    }), 200
    


@admin_dashboard_bp.route('/admin/Filesofuploadfolder', methods=['GET'])
def list_uploaded_files():
    # Get email parameter from query string
    email_to_search = request.args.get('email')
    
    if not email_to_search:
        return jsonify({'error': 'Email parameter is required'}), 400

    # Search for user in both User and UserAdmin tables
    user = User.query.filter_by(email=email_to_search).first()
    

    

    # Get user_id from either table
    user_id = user.id if user else None
    if not user_id:
        return jsonify({'message': 'User ID not found for this email'}), 404

    # Get user's upload history and profiles
    related_upload_history = UploadHistory.query.filter_by(user_id=user_id).all()
    related_profiles = CountryProfile.query.filter_by(user_id=user_id).all()

    # Create a set of filenames associated with this user
    user_filenames = set()
    
    # Add filenames from upload history
    for upload in related_upload_history:
        if hasattr(upload, 'filename') and upload.filename:
            user_filenames.add(upload.filename)
        # If filename is stored in a different field, adjust accordingly
        # Example: if it's in 'file_path', extract filename from path
        if hasattr(upload, 'file_path') and upload.file_path:
            filename = os.path.basename(upload.file_path)
            user_filenames.add(filename)
    
    # Add filenames from country profiles if they have file references
    for profile in related_profiles:
        if hasattr(profile, 'filename') and profile.filename:
            user_filenames.add(profile.filename)
        # Add other file fields if they exist in CountryProfile
        if hasattr(profile, 'file_path') and profile.file_path:
            filename = os.path.basename(profile.file_path)
            user_filenames.add(filename)

    # Function to check if filename belongs to this user based on naming patterns
    def is_user_file(filename, user_id):
        # Check direct filename match first
        if filename in user_filenames:
            return True
        
        # Check pattern-based matches for files like:
        # error_file_{user_id}{country}{month}_{year}.xlsx
        # user_{user_id}_{country}_{month}{year}_inventory_file.xlsx
        import re
        
        # Pattern for error files: error_file_{user_id}...
        error_pattern = rf"^error_file_{user_id}.*\.xlsx$"
        if re.match(error_pattern, filename):
            return True
            
        # Pattern for inventory files: user_{user_id}_...
        inventory_pattern = rf"^user_{user_id}_.*\.xlsx$"
        if re.match(inventory_pattern, filename):
            return True
        
        purchase_pattern = rf"^purchase_order_{user_id}.*\.xlsx$"
        if re.match(purchase_pattern, filename):
            return True
        
        forecastpnl_pattern = rf"^forecastpnl_{user_id}.*\.xlsx$"
        if re.match(forecastpnl_pattern, filename):
            return True
        
        inventory_pattern = rf"^inventory_{user_id}.*\.xlsx$"
        if re.match(inventory_pattern, filename):
            return True
        
        forecasts_pattern = rf"^forecasts_{user_id}.*\.xlsx$"
        if re.match(forecasts_pattern, filename):
            return True 
        
        currentinventory_pattern = rf"^currentinventory_{user_id}.*\.xlsx$"
        if re.match(currentinventory_pattern, filename):
            return True  
        
        inventory_forecast_pattern=rf"^inventory_forecast_{user_id}_.*\.xlsx$"
        if re.match(inventory_forecast_pattern, filename):
            return True  
        
            
        # Add more patterns as needed
        return False

    # Check if upload directory exists
    upload_dir = current_app.config.get()
    if not os.path.isdir(upload_dir):
        return jsonify({'error': 'Uploads directory not found'}), 404

    # Process only files that belong to the searched user
    files_info = []
    for filename in os.listdir(upload_dir):
        # Skip files that don't belong to this user
        if not is_user_file(filename, user_id):
            continue
            
        file_path = os.path.join(upload_dir, filename)
        if not os.path.isfile(file_path):
            continue

        stat = os.stat(file_path)
        file_info = {
            'filename': filename,
            'size': stat.st_size,
            'last_modified': datetime.fromtimestamp(stat.st_mtime).isoformat(),
        }

        try:
            ext = os.path.splitext(filename)[1].lower()
            
            if ext in ['.xls', '.xlsx']:
                df = pd.read_excel(file_path)
                df_cleaned = df.head(20)
                file_info['headers'] = list(df_cleaned.columns)
                file_info['rows'] = json.loads(df_cleaned.to_json(orient='values'))

            elif ext == '.csv':
                df = pd.read_csv(file_path)
                df_cleaned = df.head(20)
                file_info['headers'] = list(df_cleaned.columns)
                file_info['rows'] = json.loads(df_cleaned.to_json(orient='values'))

            elif ext == '.json':
                with open(file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    file_info['json_preview'] = data if isinstance(data, (dict, list)) else str(data)

            elif ext == '.txt':
                with open(file_path, 'r', encoding='utf-8') as f:
                    lines = f.readlines()
                    file_info['text_preview'] = lines[:20]

            else:
                file_info['note'] = 'Unsupported file type or not previewable'

        except Exception as e:
            file_info['error'] = f'Failed to read file: {str(e)}'

        files_info.append(file_info)

    return jsonify({
        'user_id': user_id,
        'total_files': len(files_info),
        'files': files_info,
    }), 200



@admin_dashboard_bp.route('/admin/dashboard/upload_currency_file', methods=['POST'])
def upload_currency_file():
    # Authentication check - prioritize token over query parameter
    authenticated = False
    
    # Method 1: Check Authorization header (recommended)
    auth_header = request.headers.get('Authorization')
    if auth_header and auth_header.startswith('Bearer '):
        token = auth_header.split(' ')[1]
        try:
            # Verify the token
            decoded_token = jwt.decode(token, SECRET_KEY, algorithms=['HS256'])
            if decoded_token.get('admin_id'):
                authenticated = True
        except jwt.ExpiredSignatureError:
            return jsonify({'message': 'Token has expired'}), 401
        except jwt.InvalidTokenError:
            return jsonify({'message': 'Invalid token'}), 401
    
    # Method 2: Fallback to query parameter (for backward compatibility)
    if not authenticated:
        authenticated_user = request.args.get('authenticated_user')
        if authenticated_user:
            authenticated = True
    
    # If neither method worked, return unauthorized
    if not authenticated:
        return jsonify({'message': 'User not authenticated'}), 401
    
    try:
        file = request.files.get('file')
        if not file:
            return jsonify({'message': 'No file provided'}), 400
        
        # Check file format and read accordingly
        filename = file.filename.lower()
        
        if filename.endswith('.csv'):
            # Read CSV file
            file_content = file.read().decode('utf-8')
            csv_data = pd.read_csv(io.StringIO(file_content))
        elif filename.endswith(('.xlsx', '.xls')):
            # Read Excel file
            csv_data = pd.read_excel(file)
        elif filename.endswith('.json'):
            # Read JSON file
            file_content = file.read().decode('utf-8')
            json_data = json.loads(file_content)
            csv_data = pd.DataFrame(json_data)
        elif filename.endswith('.txt'):
            # Read tab-separated or pipe-separated text file
            file_content = file.read().decode('utf-8')
            # Try different separators
            try:
                csv_data = pd.read_csv(io.StringIO(file_content), sep='\t')
            except:
                try:
                    csv_data = pd.read_csv(io.StringIO(file_content), sep='|')
                except:
                    csv_data = pd.read_csv(io.StringIO(file_content), sep=',')
        else:
            return jsonify({
                'message': 'Unsupported file format. Supported formats: CSV, Excel (.xlsx, .xls), JSON, TXT'
            }), 400
        
        # Clean column names (remove extra spaces)
        csv_data.columns = csv_data.columns.str.strip()
        
        # Expected columns for currency conversion CSV
        expected_columns = [
            'user_currency', 'country', 'selected_currency', 
            'month', 'year', 'conversion_rate'
        ]
        
        # Check if all expected columns exist
        missing_columns = [col for col in expected_columns if col not in csv_data.columns]
        if missing_columns:
            return jsonify({
                'message': f'Missing required columns: {", ".join(missing_columns)}',
                'expected_columns': expected_columns,
                'available_columns': list(csv_data.columns)
            }), 400
        
        # Process and store data
        success_count = 0
        error_count = 0
        errors = []
        
        for index, row in csv_data.iterrows():
            try:
                # Handle empty/NaN values
                def safe_get(value, default=None):
                    if pd.isna(value) or value == '' or str(value).strip() == '':
                        return default
                    return str(value).strip()
                
                def safe_get_float(value, default=0.0):
                    if pd.isna(value) or value == '':
                        return default
                    try:
                        return float(value)
                    except:
                        return default
                
                def safe_get_int(value, default=None):
                    if pd.isna(value) or value == '':
                        return default
                    try:
                        return int(value)
                    except:
                        return default
                
                # Validate required fields
                user_currency = safe_get(row['user_currency'])
                country = safe_get(row['country'])
                selected_currency = safe_get(row['selected_currency'])
                month = safe_get(row['month'])
                year = safe_get_int(row['year'])
                conversion_rate = safe_get_float(row['conversion_rate'])
                
                # Check for required fields
                if not user_currency or not country or not selected_currency or not month or not year or conversion_rate == 0.0:
                    error_count += 1
                    errors.append(f"Row {index + 2}: Missing required data")
                    continue
                
                # Check if record already exists
                existing_record = CurrencyConversion.query.filter_by(
                    user_currency=user_currency,
                    country=country,
                    selected_currency=selected_currency,
                    month=month,
                    year=year
                ).first()
                
                if existing_record:
                    # Update existing record
                    existing_record.conversion_rate = conversion_rate
                else:
                    # Create new CurrencyConversion instance
                    currency_conversion = CurrencyConversion(
                        user_currency=user_currency,
                        country=country,
                        selected_currency=selected_currency,
                        month=month,
                        year=year,
                        conversion_rate=conversion_rate
                    )
                    
                    # Add to database session
                    db.session.add(currency_conversion)
                
                success_count += 1
                
            except Exception as e:
                error_count += 1
                errors.append(f"Row {index + 2}: {str(e)}")  # +2 because CSV rows start from 1 and we have header
        
        # Commit all changes
        try:
            db.session.commit()
            
            response_data = {
                'message': f'Currency conversion file processed successfully. {success_count} records processed.',
                'success_count': success_count,
                'error_count': error_count,
                'total_rows': len(csv_data)
            }
            
            if errors:
                response_data['errors'] = errors[:10]  # Limit to first 10 errors
                if len(errors) > 10:
                    response_data['additional_errors'] = len(errors) - 10
            
            return jsonify(response_data), 200
            
        except IntegrityError as e:
            db.session.rollback()
            return jsonify({
                'message': 'Database integrity error. Some records might conflict with existing data.',
                'error': str(e.orig) if hasattr(e, 'orig') else str(e)
            }), 400
            
    except (pd.errors.EmptyDataError, pd.errors.ParserError):
        return jsonify({'message': 'Invalid file format or the file is empty'}), 400
    except json.JSONDecodeError:
        return jsonify({'message': 'Invalid JSON format'}), 400
    except Exception as e:
        db.session.rollback()
        return jsonify({
            'message': 'An error occurred while processing the currency conversion file',
            'error': str(e)
        }), 500



@admin_dashboard_bp.route('/admin/dashboard/view_currency_file', methods=['GET'])
def view_currency_file():
    try:
        records = CurrencyConversion.query.all()
        
        result = []
        for record in records:
            result.append({
                'user_currency': record.user_currency,
                'country': record.country,
                'selected_currency': record.selected_currency,
                'month': record.month,
                'year': record.year,
                'conversion_rate': record.conversion_rate
            })

        return jsonify({
            'message': f'{len(result)} records found.',
            'data': result
        }), 200

    except Exception as e:
        return jsonify({
            'message': 'An error occurred while fetching currency conversion data.',
            'error': str(e)
        }), 500
        
        
@admin_dashboard_bp.route('/admin/dashboard/upload_referral_fee', methods=['POST'])
def upload_referral_fee():
    # Authentication check - prioritize token over query parameter
    authenticated = False
    
    # Method 1: Check Authorization header (recommended)
    auth_header = request.headers.get('Authorization')
    if auth_header and auth_header.startswith('Bearer '):
        token = auth_header.split(' ')[1]
        try:
            # Verify the token
            decoded_token = jwt.decode(token, SECRET_KEY, algorithms=['HS256'])
            if decoded_token.get('admin_id'):
                authenticated = True
        except jwt.ExpiredSignatureError:
            return jsonify({'message': 'Token has expired'}), 401
        except jwt.InvalidTokenError:
            return jsonify({'message': 'Invalid token'}), 401
    
    # Method 2: Fallback to query parameter (for backward compatibility)
    if not authenticated:
        authenticated_user = request.args.get('authenticated_user')
        if authenticated_user:
            authenticated = True
    
    # If neither method worked, return unauthorized
    if not authenticated:
        return jsonify({'message': 'User not authenticated'}), 401

    try:
        file = request.files.get('file')
        if not file:
            return jsonify({'message': 'No file provided'}), 400
        
        # Check file format and read accordingly
        filename = file.filename.lower()
        
        if filename.endswith('.csv'):
            # Read CSV file
            file_content = file.read().decode('utf-8')
            csv_data = pd.read_csv(io.StringIO(file_content))
        elif filename.endswith(('.xlsx', '.xls')):
            # Read Excel file
            csv_data = pd.read_excel(file)
        elif filename.endswith('.json'):
            # Read JSON file
            file_content = file.read().decode('utf-8')
            json_data = json.loads(file_content)
            csv_data = pd.DataFrame(json_data)
        elif filename.endswith('.txt'):
            # Read tab-separated or pipe-separated text file
            file_content = file.read().decode('utf-8')
            # Try different separators
            try:
                csv_data = pd.read_csv(io.StringIO(file_content), sep='\t')
            except:
                try:
                    csv_data = pd.read_csv(io.StringIO(file_content), sep='|')
                except:
                    csv_data = pd.read_csv(io.StringIO(file_content), sep=',')
        else:
            return jsonify({
                'message': 'Unsupported file format. Supported formats: CSV, Excel (.xlsx, .xls), JSON, TXT'
            }), 400
        
        # Clean column names (remove extra spaces)
        csv_data.columns = csv_data.columns.str.strip()
        
        # Expected columns for referral fee CSV
        expected_columns = [
            'country', 'category', 'subcategory', 
            'referral_fee', 'price_from', 'price_to'
        ]
        
        # Check if all expected columns exist
        missing_columns = [col for col in expected_columns if col not in csv_data.columns]
        if missing_columns:
            return jsonify({
                'message': f'Missing required columns: {", ".join(missing_columns)}',
                'expected_columns': expected_columns,
                'available_columns': list(csv_data.columns)
            }), 400
        
        # Process and store data
        success_count = 0
        error_count = 0
        errors = []
        
        for index, row in csv_data.iterrows():
            try:
                # Handle empty/NaN values
                def safe_get(value, default=None):
                    if pd.isna(value) or value == '' or str(value).strip() == '':
                        return default
                    return value
                
                # Extract and validate data
                country = safe_get(row['country'])
                category = safe_get(row['category'])
                subcategory = safe_get(row['subcategory'])
                referral_fee = safe_get(row['referral_fee'])
                price_from = safe_get(row['price_from'])
                price_to = safe_get(row['price_to'])
                
                # Validate required fields
                if not all([country, category, subcategory]):
                    errors.append(f'Row {index + 1}: Missing required text fields')
                    error_count += 1
                    continue
                
                # Convert numeric fields
                try:
                    referral_fee = float(referral_fee) if referral_fee is not None else 0.0
                    price_from = float(price_from) if price_from is not None else 0.0
                    price_to = float(price_to) if price_to is not None else 0.0
                except (ValueError, TypeError):
                    errors.append(f'Row {index + 1}: Invalid numeric values')
                    error_count += 1
                    continue
                
                # Validate numeric ranges
                if price_from < 0 or price_to < 0 or referral_fee < 0:
                    errors.append(f'Row {index + 1}: Negative values not allowed')
                    error_count += 1
                    continue
                
                if price_from > price_to:
                    errors.append(f'Row {index + 1}: price_from cannot be greater than price_to')
                    error_count += 1
                    continue
                
                # Check for existing record (to update or create new)
                existing_record = Category.query.filter_by(
                    country=str(country).strip(),
                    category=str(category).strip(),
                    subcategory=str(subcategory).strip(),
                    price_from=price_from,
                    price_to=price_to
                ).first()
                
                if existing_record:
                    # Update existing record
                    existing_record.referral_fee = referral_fee
                else:
                    # Create new record
                    new_category = Category(
                        country=str(country).strip(),
                        category=str(category).strip(),
                        subcategory=str(subcategory).strip(),
                        referral_fee=referral_fee,
                        price_from=price_from,
                        price_to=price_to
                    )
                    db.session.add(new_category)
                
                success_count += 1
                
            except Exception as e:
                errors.append(f'Row {index + 1}: {str(e)}')
                error_count += 1
                continue
        
        # Commit all changes
        try:
            db.session.commit()
            
            # Prepare response
            response_data = {
                'message': 'File processed successfully',
                'success_count': success_count,
                'error_count': error_count,
                'total_rows': len(csv_data)
            }
            
            if errors:
                response_data['errors'] = errors[:10]  # Limit to first 10 errors
                if len(errors) > 10:
                    response_data['additional_errors'] = len(errors) - 10
            
            status_code = 200 if error_count == 0 else 207  # 207 for partial success
            return jsonify(response_data), status_code
            
        except Exception as e:
            db.session.rollback()
            return jsonify({
                'message': 'Database error occurred',
                'error': str(e)
            }), 500
            
    except Exception as e:
        return jsonify({
            'message': 'An error occurred while processing the file',
            'error': str(e)
        }), 500        
