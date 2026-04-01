from flask import Blueprint, request, jsonify
from config import Config
SECRET_KEY = Config.SECRET_KEY
from sqlalchemy import text
import json
import io
import pandas as pd
from sqlalchemy import inspect
from app import db
from app.models.user_models import CurrencyConversion, Category, UserAdmin , User, UploadHistory, CountryProfile, Member, UserObjective
from sqlalchemy.exc import IntegrityError
from flask import current_app
import os,jwt
import csv
from datetime import datetime


superadmin_dashboard_bp = Blueprint('superadmin_dashboard', __name__)


# --------------------------- helpers ---------------------------

def _is_superadmin_authenticated():
    """
    Returns: (True, None) if authenticated as superadmin else (False, (response_json, status_code))
    """
    # Method 1: Authorization header
    auth_header = request.headers.get("Authorization")
    if auth_header and auth_header.startswith("Bearer "):
        token = auth_header.split(" ")[1]
        try:
            decoded = jwt.decode(token, SECRET_KEY, algorithms=["HS256"])
            if decoded.get("is_superadmin"):
                return True, None
            return False, ({"message": "Not a superadmin"}, 403)
        except jwt.ExpiredSignatureError:
            return False, ({"message": "Token has expired"}, 401)
        except jwt.InvalidTokenError:
            return False, ({"message": "Invalid token"}, 401)

    # Method 2: query param fallback
    authenticated_user = request.args.get("authenticated_user")
    if authenticated_user:
        return True, None

    return False, ({"message": "User not authenticated"}, 401)


def _round_row(columns, raw_row):
    row_dict = {}
    for col, val in zip(columns, raw_row):
        if isinstance(val, (int, float)):
            row_dict[col] = round(val, 2)
        else:
            row_dict[col] = val
    return row_dict


def _safe_user_dict(u: User):
    return {
        "id": u.id,
        "name": getattr(u, "name", None),
        "email": u.email,
        "company_name": getattr(u, "company_name", None),
        "brand_name": getattr(u, "brand_name", None),
        "country": getattr(u, "country", None),
        "marketplace_id": getattr(u, "marketplace_id", None),
        "annual_sales_range": getattr(u, "annual_sales_range", None),
        "target_sales": float(u.target_sales) if getattr(u, "target_sales", None) is not None else None,
        "address": getattr(u, "address", None),
        # ✅ ADD THIS LINE
        "status": u.status

    }


def _safe_admin_dict(ua: UserAdmin):
    return {
        "id": ua.id,
        "name": getattr(ua, "name", None),
        "email": ua.email,
        "company_name": getattr(ua, "company_name", None),
        "brand_name": getattr(ua, "brand_name", None),
        "country": getattr(ua, "country", None),
        "marketplace_id": getattr(ua, "marketplace_id", None),
        "annual_sales_range": getattr(ua, "annual_sales_range", None),
    }


# --------------------------- routes ---------------------------

@superadmin_dashboard_bp.route("/superadmin/dashboard", methods=["GET"])
def get_superadmin_dashboard():
    # ✅ Auth
    ok, err = _is_superadmin_authenticated()
    if not ok:
        payload, code = err
        return jsonify(payload), code

    email_to_search = request.args.get("email")

    # ✅ CASE 1: Email provided -> return detailed dashboard for that user
    if email_to_search:
        user = User.query.filter_by(email=email_to_search).first()
        user_admin = UserAdmin.query.filter_by(email=email_to_search).first()

        if not user and not user_admin:
            return jsonify({"message": "No user found with that email"}), 404

        selected_user = user if user else user_admin
        user_id = user.id if user else getattr(user_admin, "user_id", None)

        if not user_id:
            return jsonify({"message": "User ID not found for this email"}), 404

        related_upload_history = UploadHistory.query.filter_by(user_id=user_id).all()
        related_profiles = CountryProfile.query.filter_by(user_id=user_id).all()
        related_objectives = UserObjective.query.filter_by(user_id=user_id).order_by(UserObjective.id.asc()).all()

        month_order = {
            "january": 1,
            "february": 2,
            "march": 3,
            "april": 4,
            "may": 5,
            "june": 6,
            "july": 7,
            "august": 8,
            "september": 9,
            "october": 10,
            "november": 11,
            "december": 12,
        }

        # Count only real monthly uploaded rows, not derived global/currency rows
        base_month_rows = [
            row for row in related_upload_history
            if (row.country or "").lower() == (getattr(selected_user, "country", "") or "").lower()
        ]

        # fallback: if no exact country rows found, use all non-global/non-converted rows
        if not base_month_rows:
            excluded_countries = {"global", "global_inr", "global_cad", "global_gbp", "uk_usd"}
            base_month_rows = [
                row for row in related_upload_history
                if (row.country or "").lower() not in excluded_countries
            ]

        # unique month-year pairs
        unique_months = {}
        for row in base_month_rows:
            month_name = (row.month or "").strip().lower()
            year_val = row.year
            if month_name in month_order and year_val:
                unique_months[(year_val, month_order[month_name])] = {
                    "month": month_name,
                    "year": year_val
                }

        sorted_months = sorted(unique_months.items(), key=lambda x: x[0])

        months_count = len(sorted_months)
        first_month_data = sorted_months[0][1] if sorted_months else None
        last_month_data = sorted_months[-1][1] if sorted_months else None

        first_month_label = (
            f'{first_month_data["month"]} {first_month_data["year"]}'
            if first_month_data else None
        )
        last_month_label = (
            f'{last_month_data["month"]} {last_month_data["year"]}'
            if last_month_data else None
        )
        months_range = (
            f"{first_month_label} to {last_month_label}"
            if first_month_label and last_month_label else None
        )

        quarter_months = {
            "quarter1": ["january", "february", "march"],
            "quarter2": ["april", "may", "june"],
            "quarter3": ["july", "august", "september"],
            "quarter4": ["october", "november", "december"],
        }

        skuwise_data = []
        engine = db.get_engine()
        inspector = inspect(engine)
        sku_table_name = f"sku_{user_id}_data_table"
        sku_table_count = 0
        sku_table_exists = False

        with engine.connect() as conn:
            for c in related_upload_history:
                country_lower = (c.country or "").lower()
                month_lower = (c.month or "").lower()

                if country_lower == "global":
                    base_table = f"skuwisemonthly_{user_id}_{country_lower}_{month_lower}{c.year}_table"
                else:
                    base_table = f"skuwisemonthly_{user_id}_{country_lower}_{month_lower}{c.year}"

                yearly_base_table = f"skuwiseyearly_{user_id}_{country_lower}_{c.year}_table"

                for table_name in [base_table, yearly_base_table]:
                    if inspector.has_table(table_name):
                        try:
                            result = conn.execute(text(f'SELECT * FROM "{table_name}" LIMIT 15'))
                            columns = result.keys()
                            rows = [_round_row(columns, r) for r in result.fetchall()]
                            skuwise_data.append({"table": table_name, "rows": rows})
                        except Exception as e:
                            skuwise_data.append({"table": table_name, "error": str(e)})
                    else:
                        skuwise_data.append({
                            "table": table_name,
                            "error": f"Table '{table_name}' does not exist"
                        })

                for quarter, months in quarter_months.items():
                    if month_lower in months:
                        quarter_table = f"{quarter}_{user_id}_{country_lower}_{c.year}_table"
                        if inspector.has_table(quarter_table):
                            try:
                                result = conn.execute(text(f'SELECT * FROM "{quarter_table}" LIMIT 15'))
                                columns = result.keys()
                                rows = [_round_row(columns, r) for r in result.fetchall()]
                                skuwise_data.append({"table": quarter_table, "rows": rows})
                            except Exception as e:
                                skuwise_data.append({"table": quarter_table, "error": str(e)})
                        else:
                            skuwise_data.append({
                                "table": quarter_table,
                                "error": f"Table '{quarter_table}' does not exist"
                            })
                        break
            # NEW: count rows from sku_{user_id}_data_table
            if inspector.has_table(sku_table_name):
                sku_table_exists = True
                count_result = conn.execute(
                    text(f'SELECT COUNT(*) AS total FROM "{sku_table_name}"')
                )
                sku_table_count = count_result.scalar() or 0

        latest_objective = (
            UserObjective.query
            .filter_by(user_id=user_id)
            .order_by(UserObjective.id.desc())
            .first()
        )

        return jsonify({
            "email": email_to_search,
            "user_id": user_id,
            "name": getattr(selected_user, "name", None),
            "company_name": getattr(selected_user, "company_name", None),
            "brand_name": getattr(selected_user, "brand_name", None),
            "country": getattr(selected_user, "country", None),
            "marketplace_id": getattr(selected_user, "marketplace_id", None),
            "annual_sales_range": getattr(selected_user, "annual_sales_range", None),
            "target_sales": float(selected_user.target_sales) if getattr(selected_user, "target_sales", None) is not None else None,
            "address": getattr(selected_user, "address", None),
            # ✅ ADD THIS
            "created_at": selected_user.created_at.isoformat() if getattr(selected_user, "created_at", None) else None,
            "months_of_data_count": months_count,
            "data_from_month": first_month_label,
            "data_to_month": last_month_label,
            "data_month_range": months_range,


            "related_country_profiles": [
                {
                    "user_id": cp.user_id,
                    "country": cp.country,
                    "stock_unit": cp.stock_unit,
                    "transit_time": cp.transit_time,
                }
                for cp in related_profiles
            ],
            # "related_upload_history": [
            #     {
            #         "id": c.id,
            #         "user_id": c.user_id,
            #         "country": c.country,
            #         "month": c.month,
            #         "year": c.year,
            #         "total_sales": c.total_sales,
            #         "total_profit": c.total_profit,
            #         "total_expense": c.total_expense,
            #     }
            #     for c in related_upload_history
            # ],
            # "user_objectives": [
            #     {
            #         "id": obj.id,
            #         "user_id": obj.user_id,
            #         "country": obj.country,
            #         "growth_intent": obj.growth_intent,
            #         "profit_priority": obj.profit_priority,
            #         "inventory_clearance_priority": obj.inventory_clearance_priority,
            #         "business_context": obj.business_context,
            #         "ai_business_journey": obj.ai_business_journey,
            #         "website_url": obj.website_url,
            #         "ppt_file_name": obj.ppt_file_name,
            #         "objective_month": obj.objective_month.isoformat() if obj.objective_month else None,
            #         "created_at": obj.created_at.isoformat() if obj.created_at else None,
            #         "updated_at": obj.updated_at.isoformat() if obj.updated_at else None,
            #     }
            #     for obj in related_objectives
            # ],
            "ai_business_journey": latest_objective.ai_business_journey if latest_objective else None,
            "sku_table_name": sku_table_name,
            "sku_table_exists": sku_table_exists,
            "sku_count": sku_table_count,

            # "skuwise_tables": skuwise_data,
        }), 200

    # ✅ CASE 2: No email -> return USERS + ADMINS with requested fields
    try:
        user_admins = UserAdmin.query.all()
        users = User.query.all()

        return jsonify({
            "user_admins": [_safe_admin_dict(ua) for ua in user_admins],
            "users": [_safe_user_dict(u) for u in users],
        }), 200

    except Exception as e:
        return jsonify({"message": f"Error fetching data: {str(e)}"}), 500


@superadmin_dashboard_bp.route('/superadmin/dashboard/upload_currency_file', methods=['POST'])
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
            if decoded_token.get('is_superadmin'):
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
        
@superadmin_dashboard_bp.route('/superadmin/dashboard/view_currency_file', methods=['GET'])
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
        
        
@superadmin_dashboard_bp.route('/superadmin/dashboard/upload_referral_fee', methods=['POST'])
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
            if decoded_token.get('is_superadmin'):
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

# Allow price_to == 0 to mean "no upper limit"
                if price_to != 0 and price_from > price_to:
                    errors.append(f'Row {index + 1}: price_from cannot be greater than price_to (unless price_to = 0)')
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



@superadmin_dashboard_bp.route("/superadmin/dashboard/members", methods=["GET"])
def get_members():
    ok, err = _is_superadmin_authenticated()
    if not ok:
        payload, code = err
        return jsonify(payload), code

    try:
        user_email = request.args.get("email")

        if not user_email:
            return jsonify({
                "message": "Email parameter is required"
            }), 400

        # Find user by email
        user = User.query.filter_by(email=user_email).first()

        if not user:
            return jsonify({
                "message": f"No user found with email {user_email}"
            }), 404

        # Get only members belonging to this user
        members = Member.query.filter_by(owner_user_id=user.id).order_by(Member.id.asc()).all()

        return jsonify({
            "message": f"{len(members)} member record(s) found.",
            "user_email": user.email,
            "user_id": user.id,
            "data": [
                {
                    "id": m.id,
                    "owner_user_id": m.owner_user_id,
                    "email": m.email,
                    "member_name": m.member_name,
                    "role": m.role,
                    "marketplace_ids": m.marketplace_ids,
                    "modules": m.modules,
                    "countries": m.countries,
                    "is_verified": m.is_verified,
                    "status": "Active" if m.is_verified else "Inactive"
                }
                for m in members
            ]
        }), 200

    except Exception as e:
        return jsonify({
            "message": "An error occurred while fetching member details.",
            "error": str(e)
        }), 500


@superadmin_dashboard_bp.route('/superadmin/dashboard/update_user_status', methods=['POST'])
def update_user_status():
    ok, err = _is_superadmin_authenticated()
    if not ok:
        payload, code = err
        return jsonify(payload), code

    try:
        data = request.get_json()

        user_id = data.get("user_id")
        status = data.get("status")

        if user_id is None:
            return jsonify({"message": "user_id is required"}), 400

        if status is None:
            return jsonify({"message": "status is required"}), 400

        user = User.query.filter_by(id=user_id).first()

        if not user:
            return jsonify({"message": "User not found"}), 404

        user.status = bool(status)
        db.session.commit()

        return jsonify({
            "message": "User status updated successfully",
            "user_id": user.id,
            "status": user.status
        }), 200

    except Exception as e:
        db.session.rollback()
        return jsonify({
            "message": "Error updating user status",
            "error": str(e)
        }), 500
    
    