from flask import Blueprint, request, jsonify , send_file
from sqlalchemy import create_engine , MetaData , text, inspect
from sqlalchemy.orm import sessionmaker
import jwt
import os
import base64
import re
from datetime import datetime 
import pandas as pd
from config import Config
SECRET_KEY = Config.SECRET_KEY
#UPLOAD_FOLDER = Config.UPLOAD_FOLDER
from app.models.user_models import User , CountryProfile
from app import db
from dotenv import load_dotenv
from datetime import datetime
from io import BytesIO


load_dotenv()
db_url = os.getenv('DATABASE_URL')



dashboard_bp = Blueprint('dashboard_bp', __name__)

def encode_file_to_base64(file_path):
    with open(file_path, "rb") as file:
        return base64.b64encode(file.read()).decode('utf-8')




@dashboard_bp.route('/check_country_profile/profile-check/<country>', methods=['GET']) 
def check_country_profile(country):
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

    profile = CountryProfile.query.filter_by(user_id=user_id, country=country).first()
    if profile:
        return jsonify({
            'exists': True,
            'transit_time': profile.transit_time,
            'stock_unit': profile.stock_unit
        })
    else:
        return jsonify({'exists': False})




@dashboard_bp.route('/passcountryfromprofiles', methods=['GET'])
def passcountryfromprofiles():
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

    user = User.query.get(user_id)
    if not user:
        return jsonify({'error': 'User not found'}), 404

    # Fetch countries from the CountryProfile model where the user_id matches
    country_profiles = CountryProfile.query.filter_by(user_id=user_id).all()
    country_profile_countries = [profile.country for profile in country_profiles]

    # Fetch countries from the User model
    user_countries = []
    if user.country:
        user_countries = [c.strip() for c in user.country.split(',')]

    # Combine both lists and remove duplicates by converting them to a set
    global_countries = set(country_profile_countries + user_countries)

    # Convert the set back to a list and sort it (if needed)
    global_countries = sorted(list(global_countries))

    return jsonify({'countries': global_countries}), 200


@dashboard_bp.route('/getDispatchfile', methods=['GET'])
def getDispatchfile():
    auth_header = request.headers.get('Authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        return jsonify({'error': 'Authorization token is missing or invalid'}), 401

    token = auth_header.split(' ')[1]
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=['HS256'])
        if 'user_id' not in payload:
            return jsonify({'error': 'Invalid token payload: user_id missing'}), 401

        user_id = payload['user_id']
        country = request.args.get('country')
        month = request.args.get('month')
        year = request.args.get('year')

        if not country or not month or not year:
            return jsonify({'error': 'Missing country, month, or year parameters'}), 400


        # ---------------- MONTH RESOLUTION (FIX) ----------------
      

        requested_month = month.lower()
        requested_year = int(year)

        current_month = datetime.now().strftime("%B").lower()
        current_year = datetime.now().year

        # Dispatch files are always saved using CURRENT ongoing month
        if requested_year == current_year:
            effective_month = current_month
        else:
            effective_month = requested_month

        short_month = effective_month[:3].lower()

        uploads_folder = os.path.abspath(UPLOAD_FOLDER)

        def find_latest_file(user_id, ctry):
            pattern = re.compile(
                rf"inventory_forecast_{user_id}_{re.escape(ctry)}_{short_month}.*\.xlsx$"
            )
            matched_files = [
                f for f in os.listdir(uploads_folder)
                if pattern.match(f)
            ]
            matched_files.sort(reverse=True)
            return os.path.join(uploads_folder, matched_files[0]) if matched_files else None

        # ---------- GLOBAL: merge UK + US ----------
        if country.lower() == 'global':
            file_uk = find_latest_file(user_id, 'uk')
            file_us = find_latest_file(user_id, 'us')

            if not file_uk and not file_us:
                return jsonify({'error': 'No UK or US dispatch files found'}), 404

            frames = []
            for f in [file_uk, file_us]:
                if f and os.path.exists(f):
                    frames.append(pd.read_excel(f))

            if not frames:
                return jsonify({'error': 'No readable UK/US dispatch files found'}), 404

            combined_df = pd.concat(frames, ignore_index=True)

            expected_columns = [
                'Product Name',
                'Inventory at Month End',
                'Projected Sales Total',
                'Dispatch',
                'Current Inventory + Dispatch',
                'Inventory Coverage Ratio Before Dispatch'
            ]

            have = [c for c in expected_columns if c in combined_df.columns]
            if 'Product Name' not in have:
                return jsonify({'error': "'Product Name' column missing in dispatch files"}), 400

            combined_df = combined_df[have].copy()

            combined_df['Product Name'] = combined_df['Product Name'].astype(str)
            combined_df = combined_df[combined_df['Product Name'].str.lower() != 'total']

            for col in [
                'Inventory at Month End',
                'Projected Sales Total',
                'Dispatch',
                'Current Inventory + Dispatch'
            ]:
                if col in combined_df.columns:
                    combined_df[col] = pd.to_numeric(
                        combined_df[col], errors='coerce'
                    ).fillna(0)

            agg_spec = {
                col: 'sum'
                for col in [
                    'Inventory at Month End',
                    'Projected Sales Total',
                    'Dispatch',
                    'Current Inventory + Dispatch'
                ]
                if col in combined_df.columns
            }

            grouped = (
                combined_df.groupby('Product Name', as_index=False).agg(agg_spec)
                if agg_spec else
                combined_df[['Product Name']].drop_duplicates()
            )

            if (
                'Inventory Coverage Ratio Before Dispatch' in combined_df.columns and
                'Inventory at Month End' in combined_df.columns
            ):
                def weighted_avg(df):
                    denom = df['Inventory at Month End'].sum()
                    if denom <= 0:
                        return 0
                    ratio_num = pd.to_numeric(
                        df['Inventory Coverage Ratio Before Dispatch'],
                        errors='coerce'
                    ).fillna(0)
                    return (ratio_num * df['Inventory at Month End']).sum() / denom

                ratio_df = (
                    combined_df
                    .groupby('Product Name', as_index=False)
                    .apply(weighted_avg)
                    .rename(columns={None: 'Inventory Coverage Ratio Before Dispatch'})
                )

                final_df = pd.merge(grouped, ratio_df, on='Product Name', how='left')
                final_df['Inventory Coverage Ratio Before Dispatch'] = final_df[
                    'Inventory Coverage Ratio Before Dispatch'
                ].apply(lambda x: "-" if pd.isna(x) or x == 0 else round(float(x), 2))
            else:
                final_df = grouped.copy()

            total_row = {'Product Name': 'Total'}
            for col in [
                'Inventory at Month End',
                'Projected Sales Total',
                'Dispatch',
                'Current Inventory + Dispatch'
            ]:
                if col in final_df.columns:
                    total_row[col] = final_df[col].sum()

            if 'Inventory Coverage Ratio Before Dispatch' in final_df.columns:
                total_row['Inventory Coverage Ratio Before Dispatch'] = ''

            final_df = pd.concat(
                [final_df, pd.DataFrame([total_row])],
                ignore_index=True
            )

            output = BytesIO()
            with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                final_df.to_excel(writer, index=False, sheet_name='Dispatch')
            output.seek(0)

            return send_file(
                output,
                download_name='global_dispatch.xlsx',
                as_attachment=False
            )

        # ---------- NON-GLOBAL ----------
        file_path = find_latest_file(user_id, country.lower())
        if not file_path:
            return jsonify({
                'error': 'Forecast file not found. Please generate inventory forecast first!'
            }), 404

        return send_file(file_path, as_attachment=False)

    except jwt.ExpiredSignatureError:
        return jsonify({'error': 'Token has expired'}), 401
    except jwt.InvalidTokenError:
        return jsonify({'error': 'Invalid token'}), 401
    except Exception as e:
        return jsonify({'error': str(e)}), 500



@dashboard_bp.route('/getDispatchfile2', methods=['GET'])
def getDispatchfile2():
    auth_header = request.headers.get('Authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        return jsonify({'error': 'Authorization token is missing or invalid'}), 401

    token = auth_header.split(' ')[1]
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=['HS256'])
        if 'user_id' not in payload:
            return jsonify({'error': 'Invalid token payload: user_id missing'}), 401

        user_id = payload['user_id']
        country = request.args.get('country')
        month = request.args.get('month')
        year = request.args.get('year')

        if not country or not month or not year:
            return jsonify({'error': 'Missing country, month, or year parameters'}), 400


        # Ensure UPLOAD_FOLDER is absolute
        uploads_folder = os.path.abspath(UPLOAD_FOLDER)

        filename = f"purchase_order_{user_id}_{country}_{month}_{year}.xlsx"
        file_path = os.path.join(uploads_folder, filename)


        if not os.path.exists(file_path):
            return jsonify({'error': f'File {filename} not found'}), 404
        
        return send_file(file_path, as_attachment=False)

    except jwt.ExpiredSignatureError:
        return jsonify({'error': 'Token has expired'}), 401
    except jwt.InvalidTokenError:
        return jsonify({'error': 'Invalid token'}), 401
    except Exception as e:
        return jsonify({'error': str(e)}), 500



def merge_dispatch_files(file_uk, file_us):
    df_uk = pd.read_excel(file_uk)
    df_us = pd.read_excel(file_us)

    # Align on the new schema
    common_cols = [
        'Product Name',
        'Inventory at Month End',
        'Projected Sales Total',
        'Dispatch',
        'Current Inventory + Dispatch',
        'Inventory Coverage Ratio Before Dispatch'
    ]
    # Keep only the columns that exist in each file
    df_uk = df_uk[[c for c in common_cols if c in df_uk.columns]].copy()
    df_us = df_us[[c for c in common_cols if c in df_us.columns]].copy()

    df_combined = pd.concat([df_uk, df_us], ignore_index=True)

    # Coerce numerics
    for col in ['Inventory at Month End', 'Projected Sales Total', 'Dispatch', 'Current Inventory + Dispatch']:
        if col in df_combined.columns:
            df_combined[col] = pd.to_numeric(df_combined[col], errors='coerce').fillna(0)

    # Group sums
    agg_spec = {}
    for col in ['Inventory at Month End', 'Projected Sales Total', 'Dispatch', 'Current Inventory + Dispatch']:
        if col in df_combined.columns:
            agg_spec[col] = 'sum'
    grouped = df_combined.groupby('Product Name', as_index=False).agg(agg_spec) if agg_spec else df_combined[['Product Name']].drop_duplicates()

    # Weighted average coverage ratio (if present)
    if 'Inventory Coverage Ratio Before Dispatch' in df_combined.columns and 'Inventory at Month End' in df_combined.columns:
        def weighted_avg(df):
            denom = df['Inventory at Month End'].sum()
            if denom <= 0:
                return 0
            ratio_num = pd.to_numeric(df['Inventory Coverage Ratio Before Dispatch'], errors='coerce').fillna(0)
            return (ratio_num * df['Inventory at Month End']).sum() / denom

        ratio_df = (
            df_combined
            .groupby('Product Name', as_index=False)
            .apply(weighted_avg)
            .rename(columns={None: 'Inventory Coverage Ratio Before Dispatch'})
        )
        final_df = pd.merge(grouped, ratio_df, on='Product Name', how='left')
        final_df['Inventory Coverage Ratio Before Dispatch'] = final_df['Inventory Coverage Ratio Before Dispatch'].apply(
            lambda x: "-" if pd.isna(x) or x == 0 else round(float(x), 2)
        )
    else:
        final_df = grouped.copy()

    # Total row
    total_row = {'Product Name': 'Total'}
    for col in ['Inventory at Month End', 'Projected Sales Total', 'Dispatch', 'Current Inventory + Dispatch']:
        if col in final_df.columns and pd.api.types.is_numeric_dtype(final_df[col]):
            total_row[col] = final_df[col].sum()
    if 'Inventory Coverage Ratio Before Dispatch' in final_df.columns:
        total_row['Inventory Coverage Ratio Before Dispatch'] = ''

    final_df = pd.concat([final_df, pd.DataFrame([total_row])], ignore_index=True)
    return final_df




@dashboard_bp.route('/purchase_order', methods=['POST'])
def PO_generated():
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

    month = request.form.get('month')
    year = request.form.get('year')
    country = request.form.get('country')
    if not month or not year:
        return jsonify({'error': 'Month and year must be provided'}), 400

    try:
        month_name = datetime.strptime(month, "%B").strftime("%B")
        year = int(year)
    except ValueError:
        return jsonify({'error': 'Invalid month or year format'}), 400

    def get_previous_month_year(month_name, year):
        months = ['january', 'february', 'march', 'april', 'may', 'june',
                  'july', 'august', 'september', 'october', 'november', 'december']
        index = months.index(month_name.lower())
        if index == 0:
            return 'december', year - 1
        return months[index - 1], year

    last_month_name, last_year = get_previous_month_year(month_name, year)

    engine = create_engine(db_url)
    Session = sessionmaker(bind=engine)
    db_session = Session()

    sku_table_name = f"sku_{user_id}_data_table"
    sku_df = pd.read_sql_table(sku_table_name, engine)
    sku_column_name = f"sku_{country.lower()}"
    if sku_column_name not in sku_df.columns:
        return jsonify({'error': f"SKU column '{sku_column_name}' not found in {sku_table_name}"}), 400

    sku_df.rename(columns={sku_column_name: 'sku'}, inplace=True)
    sku_df['sku'] = sku_df['sku'].astype(str).str.strip()

    current_month = datetime.now().strftime("%b").lower()
    inventory_file_path = os.path.join(UPLOAD_FOLDER, f'inventory_forecast_{user_id}_{country}_{current_month}+2.xlsx')
    if 'inventory_file' in request.files:
        request.files['inventory_file'].save(inventory_file_path)
    if not os.path.exists(inventory_file_path):
        return jsonify({'error': f'{inventory_file_path} not found'}), 404

    inventory_df = pd.read_excel(inventory_file_path)
    inventory_df.rename(columns={sku_column_name: 'sku'}, inplace=True)
    inventory_df['sku'] = inventory_df['sku'].astype(str).str.strip()

    if 'warehouse_balance' not in request.files:
        return jsonify({'error': 'No warehouse balance file uploaded'}), 400

    warehouse_file = request.files['warehouse_balance']
    warehouse_df = pd.read_excel(warehouse_file)
    if sku_column_name not in warehouse_df.columns:
        return jsonify({'error': f"'{sku_column_name}' not found in warehouse file"}), 400
    warehouse_df.rename(columns={sku_column_name: 'sku'}, inplace=True)
    warehouse_df['sku'] = warehouse_df['sku'].astype(str).str.strip()

    country_profile = db_session.query(CountryProfile).filter_by(user_id=user_id).first()
    if not country_profile:
        return jsonify({'error': 'Country profile not found'}), 404

    # Inventory projection & dispatch
    inventory_df['Inventory Projection'] = inventory_df.get('Projected Sales Total', 0) * (
        country_profile.transit_time + country_profile.stock_unit
    )
    inventory_df['Dispatch'] = inventory_df['Inventory Projection'] - inventory_df.get('Inventory at Month End', 0)

    # Dispatch splits & totals
    filename = os.path.basename(inventory_file_path).lower()
    inventory_df['Dispatches UK'] = 0
    inventory_df['Dispatches Canada'] = 0
    inventory_df['Dispatches Amazon US'] = 0
    if 'uk' in filename:
        inventory_df['Dispatches UK'] = inventory_df['Dispatch'].apply(lambda x: max(0, x))
    elif 'canada' in filename:
        inventory_df['Dispatches Canada'] = inventory_df['Dispatch'].apply(lambda x: max(0, x))
    elif 'us' in filename:
        inventory_df['Dispatches Amazon US'] = inventory_df['Dispatch'].apply(lambda x: max(0, x))
    inventory_df['Total Dispatches'] = inventory_df[['Dispatches UK', 'Dispatches Canada', 'Dispatches Amazon US']].sum(axis=1)

    # Merge inventory projections into warehouse
    warehouse_df = warehouse_df.merge(
        inventory_df[['sku', 'Inventory Projection', 'Dispatch', 'Inventory at Month End',
                      'Dispatches UK', 'Dispatches Canada', 'Dispatches Amazon US', 'Total Dispatches']],
        on='sku', how='left'
    )

    # Sales tables: current & last month
    sales_query = "SELECT sku, total_quantity FROM {table} WHERE user_id = %(user_id)s"
    current_table = f"skuwisemonthly_{user_id}_{country}_{month_name.lower()}{year}"
    last_table = f"skuwisemonthly_{user_id}_{country}_{last_month_name.lower()}{last_year}"

    current_sales_df = pd.read_sql_query(
        sales_query.format(table=current_table), engine, params={"user_id": user_id}
    ).rename(columns={'total_quantity': f"Current Month Units Sold ({month_name})"})

    last_sales_df = pd.read_sql_query(
        sales_query.format(table=last_table), engine, params={"user_id": user_id}
    ).rename(columns={'total_quantity': f"Last Month Sale ({last_month_name})"})

    current_sales_df['sku'] = current_sales_df['sku'].astype(str).str.strip()
    last_sales_df['sku'] = last_sales_df['sku'].astype(str).str.strip()

    warehouse_df = warehouse_df.merge(current_sales_df, on='sku', how='left')
    warehouse_df = warehouse_df.merge(last_sales_df, on='sku', how='left')

    # Order raised
    warehouse_df['Dispatch'] = warehouse_df['Dispatch'].fillna(0).apply(lambda x: max(0, x))
    warehouse_df['Order Raised'] = (
        warehouse_df['Dispatch'] - warehouse_df['Local Stock'] - warehouse_df['In Transit Units']
    ).fillna(0).apply(lambda x: max(0, x))

    # Product info & pricing
    warehouse_df = warehouse_df.merge(sku_df[['sku', 'product_name', 'price']], on='sku', how='left')
    warehouse_df.rename(columns={'product_name': 'Product Name', 'price': 'Cost per Unit (in INR)'}, inplace=True)
    warehouse_df['Product Name'] = warehouse_df.apply(
        lambda row: row['sku'] if pd.isna(row['Product Name']) or str(row['Product Name']).strip() == '' else row['Product Name'],
        axis=1
    )
    warehouse_df['Cost per Unit (in INR)'] = pd.to_numeric(warehouse_df['Cost per Unit (in INR)'], errors='coerce').fillna(0).round(2)
    warehouse_df['PO Cost (in INR)'] = warehouse_df['Order Raised'] * warehouse_df['Cost per Unit (in INR)']

    # Final report dataframe
    final_df = warehouse_df[[
        'sku', 'Product Name',
        f"Last Month Sale ({last_month_name})",
        f"Current Month Units Sold ({month_name})",
        'Inventory Projection', 'Inventory at Month End',
        'In Transit Units', 'Dispatch',
        'Dispatches UK', 'Dispatches Canada', 'Dispatches Amazon US', 'Total Dispatches',
        'Local Stock', 'Order Raised', 'Cost per Unit (in INR)', 'PO Cost (in INR)'
    ]]

    final_df.rename(columns={
        'sku': 'SKU',
        'Inventory Projection': 'Projected Sales',
        'Inventory at Month End': 'Current Inventory - with Amazon',
        'In Transit Units': 'In Transit',
        'Dispatch': 'To be Dispatched',
        'Local Stock': 'Current Inventory - Local Warehouse',
        'Order Raised': 'PO to Be raised'
    }, inplace=True)

    final_df.insert(0, 'Sno.', range(1, len(final_df) + 1))

    # Inventory accounting block
    final_df['Inventory at the beginning of the month'] = final_df['Current Inventory - with Amazon']
    final_df['Inventory Inwarded'] = 0
    final_df['other'] = 0
    final_df['Inventory at the end of the month'] = (
        final_df['Inventory at the beginning of the month']
        - final_df[f"Current Month Units Sold ({month_name})"].fillna(0)
        + final_df['Inventory Inwarded']
        - final_df['other']
    )
    final_df['PO Cost (in INR_Inventory)'] = final_df['Cost per Unit (in INR)'] * final_df['Inventory at the end of the month']

    # ---------- Round numeric columns BEFORE appending total row ----------
    numeric_cols_pre = final_df.select_dtypes(include='number').columns
    final_df[numeric_cols_pre] = final_df[numeric_cols_pre].round(2)

    # ---------- Build TOTAL row safely ----------
    # Columns that must NOT be summed (unit price should not be totaled)
    EXCLUDE_FROM_SUM = {'Cost per Unit (in INR)'}

    # Coerce key numeric columns to numeric to avoid bad sums
    for col in [
        'Dispatches UK', 'Dispatches Canada', 'Dispatches Amazon US', 'Total Dispatches',
        'Current Inventory - Local Warehouse', 'PO to Be raised',
        'PO Cost (in INR)', 'Inventory at the end of the month', 'PO Cost (in INR_Inventory)'
    ]:
        if col in final_df.columns:
            final_df[col] = pd.to_numeric(final_df[col], errors='coerce').fillna(0.0)

    # Prepare total row dict with blanks
    total_row = {c: '' for c in final_df.columns}

    # Sum all numeric columns except excluded ones
    for c in final_df.columns:
        if c in EXCLUDE_FROM_SUM:
            continue
        # sum only numeric-like columns
        if pd.api.types.is_numeric_dtype(final_df[c]):
            total_row[c] = round(pd.to_numeric(final_df[c], errors='coerce').fillna(0).sum(), 2)

    # Label placement: put "Total" under Product Name; leave Sno. empty
    total_row['Product Name'] = 'Total'
    if 'Sno.' in total_row:
        total_row['Sno.'] = ''

    # Ensure unit cost stays blank (no misleading sum)
    total_row['Cost per Unit (in INR)'] = ''

    # Append total row
    final_df = pd.concat([final_df, pd.DataFrame([total_row])], ignore_index=True)

    # ---------- Save to Excel ----------
    output_path = os.path.join(
        UPLOAD_FOLDER, f'purchase_order_{user_id}_{country}_{month_name}_{year}.xlsx'
    )
    final_df.to_excel(output_path, index=False)


    db_session.close()
    return jsonify({
        'message': 'Purchase order generated successfully',
        'data': encode_file_to_base64(output_path)
    }), 200



@dashboard_bp.route('/global_purchase_order', methods=['GET'])
def global_PO_generated():
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

    month = request.args.get('month')
    year = request.args.get('year')
    if not month or not year:
        return jsonify({'error': 'Month and year must be provided'}), 400

    try:
        month_name = datetime.strptime(month, "%B").strftime("%B")
        year = int(year)
    except ValueError:
        return jsonify({'error': 'Invalid month or year format'}), 400

    engine = create_engine(db_url)
    Session = sessionmaker(bind=engine)
    db_session = Session()

    # --- SKU table (source of truth for price) ---
    sku_table_name = f"sku_{user_id}_data_table"
    try:
        sku_df = pd.read_sql_table(sku_table_name, engine)
    except Exception as e:
        return jsonify({'error': f"SKU table '{sku_table_name}' not found: {str(e)}"}), 400

    # Build a mapping: any sku value in sku_* -> price (as-is from DB)
    price_map = {}
    sku_cols_in_db = [c for c in sku_df.columns if c.startswith('sku_')]
    if 'price' in sku_df.columns:
        for _, r in sku_df.iterrows():
            price_val = r.get('price')
            if pd.isna(price_val):
                continue
            for c in sku_cols_in_db:
                v = r.get(c)
                if pd.notna(v) and str(v).strip():
                    price_map[str(v).strip()] = float(price_val)

    # Only check UK and US countries (removed Canada)
    countries = ['uk', 'us']
    country_files_found = []
    existing_files = {}
    
    # Check which country files exist
    for country in countries:
        individual_po_path = os.path.join(
            UPLOAD_FOLDER, f'purchase_order_{user_id}_{country}_{month_name.lower()}_{year}.xlsx'
        )
        if os.path.exists(individual_po_path):
            existing_files[country] = individual_po_path
            country_files_found.append(country)

    # If no country files exist, return error
    if not country_files_found:
        return jsonify({
            'error': 'No country-specific purchase order files found. Please generate UK and/or US purchase orders first.'
        }), 404

    global_df = pd.DataFrame()
    
    # Process existing country files
    for country in country_files_found:
        try:
            country_po_df = pd.read_excel(existing_files[country])

            # Remove total row if present
            if not country_po_df.empty and str(country_po_df.iloc[-1].get('Sno.', '')).strip().lower() == 'total':
                country_po_df = country_po_df.iloc[:-1]

            # Standardize column names
            column_mapping = {
                'SKU': 'sku',
                'Product Name': 'Product Name',
                'Dispatches UK': 'Dispatches UK',
                'Dispatches Canada': 'Dispatches Canada',
                'Dispatches Amazon US': 'Dispatches Amazon US',
                'Total Dispatches': 'Total Dispatches',
                'Current Inventory - Local Warehouse': 'Current Inventory - Local Warehouse',
                'PO Already Raised': 'PO Already Raised',
                'PO to Be raised': 'PO to Be raised',
                'Cost per Unit (in INR)': 'Cost per Unit (in INR)',
                'PO Cost (in INR)': 'PO Cost (in INR)'
            }
            for old_col, new_col in column_mapping.items():
                if old_col in country_po_df.columns:
                    country_po_df = country_po_df.rename(columns={old_col: new_col})

            # Clean SKU column
            if 'sku' in country_po_df.columns:
                country_po_df['sku'] = country_po_df['sku'].astype(str).str.strip()

            # Convert numeric columns
            numeric_columns = [
                'Dispatches UK', 'Dispatches Canada', 'Dispatches Amazon US',
                'Total Dispatches', 'Current Inventory - Local Warehouse', 'PO Already Raised',
                'PO to Be raised', 'Cost per Unit (in INR)', 'PO Cost (in INR)'
            ]
            for col in numeric_columns:
                if col in country_po_df.columns:
                    country_po_df[col] = pd.to_numeric(country_po_df[col], errors='coerce').fillna(0)

            # Ensure dispatch columns exist
            for col in ['Dispatches UK', 'Dispatches Canada', 'Dispatches Amazon US']:
                if col not in country_po_df.columns:
                    country_po_df[col] = 0

            # Select relevant columns
            merge_columns = ['sku', 'Product Name'] + [c for c in numeric_columns if c in country_po_df.columns]
            country_data = country_po_df[merge_columns].copy()

            # Merge with global dataframe
            if global_df.empty:
                global_df = country_data.copy()
            else:
                # Merge on Product Name, handling SKU conflicts
                global_df = pd.merge(global_df, country_data, on='Product Name', how='outer', suffixes=('', '_new'))

                # Handle SKU conflicts - prefer non-null values
                if 'sku_new' in global_df.columns:
                    mask = global_df['sku'].isna() & global_df['sku_new'].notna()
                    global_df.loc[mask, 'sku'] = global_df.loc[mask, 'sku_new']
                    global_df = global_df.drop(columns=['sku_new'])

                # Sum numeric columns from both files
                for col in numeric_columns:
                    new_col = f'{col}_new'
                    if new_col in global_df.columns:
                        global_df[col] = global_df.get(col, 0).fillna(0) + global_df[new_col].fillna(0)
                        global_df = global_df.drop(columns=[new_col])

        except Exception as e:
            print(f"Error reading PO file for {country}: {str(e)}")
            db_session.close()
            return jsonify({'error': f'Error processing {country.upper()} file: {str(e)}'}), 500

    # --- Apply DB prices to every row by SKU (source of truth) ---
    if 'sku' in global_df.columns:
        global_df['sku'] = global_df['sku'].astype(str).str.strip()
    else:
        global_df['sku'] = ''
    global_df['Cost per Unit (in INR)'] = global_df['sku'].map(price_map).astype(float).fillna(0.0)

    # Recalculate totals and costs
    if 'Total Dispatches' not in global_df.columns or global_df['Total Dispatches'].isna().any():
        global_df['Total Dispatches'] = (
            global_df.get('Dispatches UK', 0).fillna(0) +
            global_df.get('Dispatches Canada', 0).fillna(0) +
            global_df.get('Dispatches Amazon US', 0).fillna(0)
        )
    
    # Ensure required columns exist
    if 'Current Inventory - Local Warehouse' not in global_df.columns:
        global_df['Current Inventory - Local Warehouse'] = 0
    if 'PO Already Raised' not in global_df.columns:
        global_df['PO Already Raised'] = 0

    # Recalculate PO to be raised
    global_df['PO to Be raised'] = (
        global_df['Total Dispatches'] -
        global_df['Current Inventory - Local Warehouse'] -
        global_df['PO Already Raised']
    ).clip(lower=0)

    # Recalculate PO Cost using DB price
    global_df['PO Cost (in INR)'] = global_df['PO to Be raised'] * global_df['Cost per Unit (in INR)']

    # Group by Product Name to handle duplicates
    numeric_to_sum = [
        'Dispatches UK', 'Dispatches Canada', 'Dispatches Amazon US',
        'Total Dispatches', 'Current Inventory - Local Warehouse', 'PO Already Raised',
        'PO to Be raised', 'PO Cost (in INR)'
    ]
    
    agg_dict = {'sku': 'first'}  # Keep first SKU
    for col in numeric_to_sum:
        if col in global_df.columns:
            agg_dict[col] = 'sum'
    # Keep first price (as-is from DB)
    agg_dict['Cost per Unit (in INR)'] = 'first'

    grouped_df = global_df.groupby('Product Name', as_index=False).agg(agg_dict)
    
    # Recalculate PO Cost after grouping
    grouped_df['PO Cost (in INR)'] = grouped_df['PO to Be raised'] * grouped_df['Cost per Unit (in INR)']

    # Build final dataframe with proper column order
    final_columns = [
        'Sno.', 'Product Name', 'Dispatches UK', 'Dispatches Canada',
        'Dispatches Amazon US', 'Total Dispatches', 'Current Inventory - Local Warehouse',
        'PO Already Raised', 'PO to Be raised', 'Cost per Unit (in INR)', 'PO Cost (in INR)'
    ]
    
    final_df = pd.DataFrame()
    final_df['Product Name'] = grouped_df['Product Name']
    
    for col in final_columns[2:]:  # Skip Sno. and Product Name
        final_df[col] = grouped_df.get(col, 0)

    # Add serial numbers
    final_df.insert(0, 'Sno.', range(1, len(final_df) + 1))

    # Add total row (don't sum unit cost)
    cols_to_sum = [
        'Dispatches UK', 'Dispatches Canada', 'Dispatches Amazon US',
        'Total Dispatches', 'Current Inventory - Local Warehouse',
        'PO Already Raised', 'PO to Be raised', 'PO Cost (in INR)'
    ]
    
    total_row_data = {'Sno.': 'Total', 'Product Name': ''}
    for col in cols_to_sum:
        if col in final_df.columns and pd.api.types.is_numeric_dtype(final_df[col]):
            total_row_data[col] = final_df[col].sum()
        else:
            total_row_data[col] = 0
    
    # Leave unit cost blank in total row
    total_row_data['Cost per Unit (in INR)'] = ''

    final_df = pd.concat([final_df, pd.DataFrame([total_row_data])], ignore_index=True)

    # Round numeric columns
    numeric_cols = final_df.select_dtypes(include='number').columns
    final_df[numeric_cols] = final_df[numeric_cols].round(2)

    # Determine file naming based on countries processed
    if len(country_files_found) == 1:
        # Single country - use country name
        country_name = country_files_found[0]
        file_suffix = country_name
        file_type = f"{country_name.upper()} Purchase Order"
    else:
        # Multiple countries - use global
        file_suffix = "global"
        file_type = "Global Purchase Order"

    # Save to Excel
    output_path = os.path.join(UPLOAD_FOLDER, f'purchase_order_{user_id}_{file_suffix}_{month_name.lower()}_{year}.xlsx')
    
    try:
        final_df.to_excel(output_path, index=False)
    except Exception as e:
        db_session.close()
        return jsonify({'error': f'Error saving file: {str(e)}'}), 500

    db_session.close()
    
    return jsonify({
        'message': f'{file_type} generated successfully',
        'data': encode_file_to_base64(output_path),
        'records_count': len(final_df) - 1,  # exclude total row
        'countries_processed': country_files_found,
        'file_type': file_type.lower().replace(' ', '_'),
        'source': 'country_po_files'
    }), 200


@dashboard_bp.route('/getGlobalDispatchfile', methods=['GET'])
def get_global_dispatch_file():
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

    month = request.args.get('month')
    year = request.args.get('year')
    if not month or not year:
        return jsonify({'error': 'Month and year must be provided'}), 400

    # Check for different file types based on what exists
    countries = ['uk', 'us']
    existing_files = {}
    
    for country in countries:
        country_file_path = os.path.join(UPLOAD_FOLDER, f'purchase_order_{user_id}_{country}_{month.lower()}_{year}.xlsx')
        if os.path.exists(country_file_path):
            existing_files[country] = country_file_path
    
    # Determine which file to serve
    file_path = None
    download_name = None
    
    if len(existing_files) > 1:
        # Multiple countries exist - look for global file
        file_path = os.path.join(UPLOAD_FOLDER, f'purchase_order_{user_id}_global_{month.lower()}_{year}.xlsx')
        download_name = f'global_purchase_order_{month}_{year}.xlsx'
    elif len(existing_files) == 1:
        # Single country exists - use that country file or its corresponding generated file
        country = list(existing_files.keys())[0]
        file_path = os.path.join(UPLOAD_FOLDER, f'purchase_order_{user_id}_{country}_{month.lower()}_{year}.xlsx')
        download_name = f'{country}_purchase_order_{month}_{year}.xlsx'
    else:
        return jsonify({'error': 'No purchase order files found. Please generate country-specific files first.'}), 404

    if not os.path.exists(file_path):
        return jsonify({'error': 'Generated file not found. Please generate the report first.'}), 404

    try:
        return send_file(
            file_path,
            as_attachment=True,
            download_name=download_name,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )
    except Exception as e:
        return jsonify({'error': f'Error sending file: {str(e)}'}), 500


@dashboard_bp.route('/getForecastFile', methods=['GET'])
def getForecastFile():
    auth_header = request.headers.get('Authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        return jsonify({'error': 'Authorization token is missing or invalid'}), 401

    token = auth_header.split(' ')[1]
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=['HS256'])
        if 'user_id' not in payload:
            return jsonify({'error': 'Invalid token payload: user_id missing'}), 401

        user_id = payload['user_id']
        country = request.args.get('country')
        month = request.args.get('month')  
        short_month = month[:3].lower() if month else None
        year = request.args.get('year')

        if not country or not month or not year:
            return jsonify({'error': 'Missing country, month, or year parameters'}), 400

        engine = create_engine(db_url)
        meta = MetaData()
        meta.reflect(bind=engine)

        pattern = re.compile(rf"inventory_forecast_{user_id}_{re.escape(country)}_{short_month}.*\.xlsx$")

        # Search files in UPLOAD_FOLDER
        matched_files = [f for f in os.listdir(UPLOAD_FOLDER) if pattern.match(f)]

        if not matched_files:
            return jsonify({'error': 'Forecast file not found. Please generate inventory forecast first!'}), 404

        # You can pick the latest file if multiple found
        matched_files.sort(reverse=True)  # Sort newest first
        selected_file = matched_files[0]
        file_path = os.path.join(UPLOAD_FOLDER, selected_file)

        return send_file(file_path, as_attachment=False)

    except jwt.ExpiredSignatureError:
        return jsonify({'error': 'Token has expired'}), 401
    except jwt.InvalidTokenError:
        return jsonify({'error': 'Invalid token'}), 401
    except Exception as e:
        return jsonify({'error': str(e)}), 500

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

from calendar import month_name as cal_month_name

MONTHS = ["January","February","March","April","May","June","July","August","September","October","November","December"]

QUARTER_MONTHS = {
    "quarter1": ["january", "february", "march"],
    "quarter2": ["april", "may", "june"],
    "quarter3": ["july", "august", "september"],
    "quarter4": ["october", "november", "december"]
}

def prev_month_year(month_str: str, year: int):
    # month_str: "November"
    idx = MONTHS.index(month_str)  # 0-based
    if idx == 0:
        return "December", year - 1
    return MONTHS[idx - 1], year

def quarter_for_month(month_str: str):
    m = month_str.lower()
    for q, months in QUARTER_MONTHS.items():
        if m in months:
            return q
    return None

def prev_quarter(q: str):
    # q like "quarter4"
    order = ["quarter1","quarter2","quarter3","quarter4"]
    i = order.index(q)
    if i == 0:
        return "quarter4", -1  # means year-1
    return order[i-1], 0



@dashboard_bp.route('/cashflow', methods=['GET'])
def cashflow():
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

    month = request.args.get('month')
    year = request.args.get('year')
    country_param = request.args.get('country', '')
    currency_param = (request.args.get('currency') or '').lower()
    country = resolve_country(country_param, currency_param)

    period_type = (request.args.get('period_type') or 'monthly').lower()

    if not year:
        return jsonify({'error': 'Year must be provided'}), 400

    # --- Quarter map + helpers ---
    MONTHS = ["January","February","March","April","May","June",
              "July","August","September","October","November","December"]

    quarter_months = {
        "quarter1": ["january", "february", "march"],
        "quarter2": ["april", "may", "june"],
        "quarter3": ["july", "august", "september"],
        "quarter4": ["october", "november", "december"]
    }

    def quarter_for_month(month_str: str):
        m = (month_str or '').lower()
        for q, months in quarter_months.items():
            if m in months:
                return q
        return None

    def prev_month_year(month_str: str, y: int):
        idx = MONTHS.index(month_str)  # 0-based
        if idx == 0:
            return "December", y - 1
        return MONTHS[idx - 1], y

    def prev_quarter(q: str):
        order = ["quarter1", "quarter2", "quarter3", "quarter4"]
        i = order.index(q)
        if i == 0:
            return "quarter4", -1  # year-1
        return order[i - 1], 0

    # --- Validation + parsing ---
    try:
        year = int(year)
    except ValueError:
        return jsonify({'error': 'Invalid year format. Provide year like 2025'}), 400

    # Month is required only for monthly OR old quarterly mode ("quarterly")
    if period_type == 'monthly' and not month:
        return jsonify({'error': 'Month must be provided for monthly period type'}), 400
    if period_type == 'quarterly' and not month:
        return jsonify({'error': 'Month must be provided for quarterly period type'}), 400

    month_name = None
    if month:
        try:
            month_name = datetime.strptime(month, "%B").strftime("%B")
        except ValueError:
            try:
                month_name = datetime.strptime(month.capitalize(), "%B").strftime("%B")
            except ValueError:
                return jsonify({'error': 'Invalid month format. Use full month names like "January" or "january"'}), 400

    engine = create_engine(db_url)
    SessionLocal = sessionmaker(bind=engine)
    db_session = SessionLocal()
    inspector = inspect(engine)

    def compute_cashflow_summary(user_id: int, year: int, country: str, period_type: str, month_name: str = None):
        """
        Returns:
          summary_totals: dict (combined_totals)
          detailed_data: list (per country)
          meta: dict (month / quarter_months / year_months)
        """
        all_cashflow_data = []
        combined_totals = {
            'net_sales': 0,
            'gross_sales': 0,
            'advertising_total': 0,
            'amazon_fee': 0,
            'cm2_profit': 0,
            'cost_of_unit_sold': 0,
            'otherwplatform': 0,
            'taxncredit': 0,
            'cashflow': 0,
            'rembursement_fee': 0,
            'quantity_total': 0,
            'selling_fees': 0,
            'fba_fees': 0,
            'promotional_rebates': 0
        }

        # --- months_to_process ---
        months_to_process = []

        if period_type == 'monthly':
            months_to_process = [month_name]

        elif period_type == 'quarterly':
            q = quarter_for_month(month_name)
            if not q:
                return combined_totals, [], {}
            months_to_process = [m.capitalize() for m in quarter_months[q]]

        elif period_type in quarter_months:
            months_to_process = [m.capitalize() for m in quarter_months[period_type]]

        elif period_type == 'yearly':
            months_to_process = MONTHS

        else:
            # Unknown period_type
            return combined_totals, [], {}

        # --- find countries with data for given months ---
        countries_with_data = set()
        for process_month in months_to_process:
            if country:
                upload_query = text("""
                    SELECT DISTINCT country 
                    FROM upload_history
                    WHERE user_id = :user_id
                      AND LOWER(month) = LOWER(:month)
                      AND year = :year
                      AND LOWER(country) = LOWER(:country)
                """)
                query_params = {
                    'user_id': user_id,
                    'month': process_month,
                    'year': year,
                    'country': country
                }
            else:
                upload_query = text("""
                    SELECT DISTINCT country 
                    FROM upload_history
                    WHERE user_id = :user_id
                      AND LOWER(month) = LOWER(:month)
                      AND year = :year
                """)
                query_params = {
                    'user_id': user_id,
                    'month': process_month,
                    'year': year
                }

            upload_results = db_session.execute(upload_query, query_params).fetchall()
            for result in upload_results:
                countries_with_data.add(result[0])

        # --- per country compute totals from table(s) ---
        for record_country in countries_with_data:
            total_otherwplatform = 0
            total_taxncredit_from_upload = 0

            # accumulate otherwplatform/taxncredit across months from upload_history
            for process_month in months_to_process:
                upload_values_query = text("""
                    SELECT otherwplatform, taxncredit 
                    FROM upload_history
                    WHERE user_id = :user_id
                      AND LOWER(month) = LOWER(:month)
                      AND year = :year
                      AND LOWER(country) = LOWER(:country)
                    LIMIT 1
                """)
                upload_values_params = {
                    'user_id': user_id,
                    'month': process_month,
                    'year': year,
                    'country': record_country
                }
                upload_values_result = db_session.execute(upload_values_query, upload_values_params).fetchone()
                if upload_values_result:
                    if upload_values_result[0]:
                        total_otherwplatform += float(upload_values_result[0])
                    if upload_values_result[1]:
                        total_taxncredit_from_upload += float(upload_values_result[1])

            # table name
            table_name = ""
            if period_type == 'monthly':
                suffix = f"{month_name.lower()}{year}"
                table_name = (
                    f"skuwisemonthly_{user_id}_{record_country.lower()}_{suffix}_table"
                    if record_country.lower().startswith("global")
                    else f"skuwisemonthly_{user_id}_{record_country.lower()}_{suffix}"
                )

            elif period_type == 'quarterly':
                q = quarter_for_month(month_name)
                table_name = f"{q}_{user_id}_{record_country.lower()}_{year}_table" if q else ""

            elif period_type in quarter_months:
                table_name = f"{period_type}_{user_id}_{record_country.lower()}_{year}_table"

            elif period_type == 'yearly':
                table_name = f"skuwiseyearly_{user_id}_{record_country.lower()}_{year}_table"

            if not table_name or not inspector.has_table(table_name):
                continue

            try:
                cashflow_df = pd.read_sql_table(table_name, engine)
                if cashflow_df.empty:
                    continue

                numeric_cols = [
                    'net_sales', 'gross_sales',
                    'advertising_total', 'amazon_fee', 'cm2_profit', 'cost_of_unit_sold',
                    'taxncredit', 'rembursement_fee',
                    'total_quantity', 'selling_fees', 'fba_fees', 'promotional_rebates'
                ]
                for col in numeric_cols:
                    if col in cashflow_df.columns:
                        cashflow_df[col] = pd.to_numeric(cashflow_df[col], errors='coerce').fillna(0)

                net_sales_total = advertising_total = amazon_fee_total = cm2_profit_total = rembursement_fee_total = cost_of_unit_sold_total = 0
                quantity_total = 0
                selling_fees_total = 0
                fba_fees_total = 0
                gross_sales_total = 0
                promotional_rebates_total = 0
                taxncredit_total = total_taxncredit_from_upload

                def find_total_row(df):
                    if 'product_name' not in df.columns:
                        return None
                    for variation in ['TOTAL', 'Total', 'total', 'TOTALS', 'Totals', 'totals']:
                        total_row = df[df['product_name'] == variation]
                        if not total_row.empty:
                            return total_row
                    return df[df['product_name'].str.contains('total', case=False, na=False)]

                # totals from TOTAL row (preferred) else column sums
                total_row = None
                if 'product_name' in cashflow_df.columns:
                    total_row = find_total_row(cashflow_df)

                if total_row is not None and not total_row.empty:
                    net_sales_total = float(total_row['net_sales'].iloc[0]) if 'net_sales' in total_row else 0
                    gross_sales_total = float(total_row['gross_sales'].iloc[0]) if 'gross_sales' in total_row else 0
                    promotional_rebates_total = float(total_row['promotional_rebates'].iloc[0]) if 'promotional_rebates' in total_row else 0
                    quantity_total = float(total_row['total_quantity'].iloc[0]) if 'total_quantity' in total_row else 0
                    advertising_total = float(total_row['advertising_total'].iloc[0]) if 'advertising_total' in total_row else 0
                    selling_fees_total = float(total_row['selling_fees'].iloc[0]) if 'selling_fees' in total_row else 0
                    fba_fees_total = float(total_row['fba_fees'].iloc[0]) if 'fba_fees' in total_row else 0
                    amazon_fee_total = float(total_row['amazon_fee'].iloc[0]) if 'amazon_fee' in total_row else 0
                    cm2_profit_total = float(total_row['cm2_profit'].iloc[0]) if 'cm2_profit' in total_row else 0
                    cost_of_unit_sold_total = float(total_row['cost_of_unit_sold'].iloc[0]) if 'cost_of_unit_sold' in total_row else 0
                    rembursement_fee_total = float(total_row['rembursement_fee'].iloc[0]) if 'rembursement_fee' in total_row else 0
                else:
                    net_sales_total = float(cashflow_df['net_sales'].sum()) if 'net_sales' in cashflow_df.columns else 0
                    gross_sales_total = float(cashflow_df['gross_sales'].sum()) if 'gross_sales' in cashflow_df.columns else 0
                    promotional_rebates_total = float(cashflow_df['promotional_rebates'].sum()) if 'promotional_rebates' in cashflow_df.columns else 0
                    quantity_total = float(cashflow_df['total_quantity'].sum()) if 'total_quantity' in cashflow_df.columns else 0
                    advertising_total = float(cashflow_df['advertising_total'].sum()) if 'advertising_total' in cashflow_df.columns else 0
                    selling_fees_total = float(cashflow_df['selling_fees'].sum()) if 'selling_fees' in cashflow_df.columns else 0
                    fba_fees_total = float(cashflow_df['fba_fees'].sum()) if 'fba_fees' in cashflow_df.columns else 0
                    amazon_fee_total = float(cashflow_df['amazon_fee'].sum()) if 'amazon_fee' in cashflow_df.columns else 0
                    cm2_profit_total = float(cashflow_df['cm2_profit'].sum()) if 'cm2_profit' in cashflow_df.columns else 0
                    cost_of_unit_sold_total = float(cashflow_df['cost_of_unit_sold'].sum()) if 'cost_of_unit_sold' in cashflow_df.columns else 0
                    rembursement_fee_total = float(cashflow_df['rembursement_fee'].sum()) if 'rembursement_fee' in cashflow_df.columns else 0

                # cashflow_total = net_sales_total - advertising_total - amazon_fee_total - total_otherwplatform + taxncredit_total
                cashflow_total = cost_of_unit_sold_total + cm2_profit_total

                # accumulate
                combined_totals['net_sales'] += net_sales_total
                combined_totals['gross_sales'] += gross_sales_total
                combined_totals['promotional_rebates'] += promotional_rebates_total
                combined_totals['quantity_total'] += quantity_total
                combined_totals['advertising_total'] += advertising_total
                combined_totals['selling_fees'] += selling_fees_total
                combined_totals['fba_fees'] += fba_fees_total
                combined_totals['amazon_fee'] += amazon_fee_total
                combined_totals['cm2_profit'] += cm2_profit_total
                combined_totals['cost_of_unit_sold'] += cost_of_unit_sold_total
                combined_totals['taxncredit'] += taxncredit_total
                combined_totals['otherwplatform'] += total_otherwplatform
                combined_totals['cashflow'] += cashflow_total
                combined_totals['rembursement_fee'] += rembursement_fee_total

                # clean df for payload
                if 'date' in cashflow_df.columns:
                    cashflow_df.drop('date', axis=1, inplace=True)

                numeric_columns = cashflow_df.select_dtypes(include=['number']).columns
                for col in numeric_columns:
                    cashflow_df[col] = cashflow_df[col].astype(float).round(2)

                try:
                    data_records = cashflow_df.to_dict(orient='records')
                    cleaned_records = []
                    for record in data_records:
                        clean_record = {}
                        for key, value in record.items():
                            try:
                                if pd.isna(value) or value is None:
                                    clean_record[key] = 0
                                elif isinstance(value, str):
                                    clean_record[key] = value
                                else:
                                    clean_record[key] = float(value)
                            except (ValueError, TypeError):
                                clean_record[key] = str(value) if value is not None else ""
                        cleaned_records.append(clean_record)
                    data_records = cleaned_records
                except Exception:
                    data_records = []

                all_cashflow_data.append({
                    'country': record_country,
                    'table': table_name,
                    'period_type': period_type,
                    'month': month_name if period_type == 'monthly' else None,
                    'net_sales': round(net_sales_total, 2),
                    'gross_sales': round(gross_sales_total, 2),
                    'promotional_rebates': round(promotional_rebates_total, 2),
                    'quantity_total': round(quantity_total, 2),
                    'advertising_total': round(advertising_total, 2),
                    'selling_fees': round(selling_fees_total, 2),
                    'fba_fees': round(fba_fees_total, 2),
                    'amazon_fee': round(amazon_fee_total, 2),
                    'cm2_profit': round(cm2_profit_total, 2),
                    'cost_of_unit_sold': round(cost_of_unit_sold_total, 2),
                    'taxncredit': round(taxncredit_total, 2),
                    'otherwplatform': round(total_otherwplatform, 2),
                    'cashflow': round(cashflow_total, 2),
                    'rembursement_fee': round(rembursement_fee_total, 2),
                    'data': data_records
                })

            except Exception:
                # Skip problematic table but keep endpoint alive
                continue

        # round totals
        for k in combined_totals:
            combined_totals[k] = round(combined_totals[k], 2)

        meta = {}
        if period_type == 'monthly':
            meta['month'] = month_name
        elif period_type == 'quarterly' or period_type in quarter_months:
            meta['quarter_months'] = months_to_process
        elif period_type == 'yearly':
            meta['year_months'] = months_to_process

        return combined_totals, all_cashflow_data, meta

    try:
        # --- Current period ---
        combined_totals, all_cashflow_data, meta = compute_cashflow_summary(
            user_id=user_id,
            year=year,
            country=country,
            period_type=period_type,
            month_name=month_name
        )

        if not all_cashflow_data:
            all_data_query = text("""
                SELECT DISTINCT country, month, year 
                FROM upload_history
                WHERE user_id = :user_id
                ORDER BY year DESC, month DESC
            """)
            all_data = db_session.execute(all_data_query, {'user_id': user_id}).fetchall()
            available_data = [{'country': r[0], 'month': r[1], 'year': r[2]} for r in all_data]

            return jsonify({
                'error': 'No data found for the specified parameters',
                'searched_for': {
                    'user_id': user_id,
                    'year': year,
                    'country': country,
                    'period_type': period_type,
                    'month': month_name
                },
                'available_data': available_data[:10]
            }), 404

        # --- Previous period params ---
        prev_year = year
        prev_period_type = period_type
        prev_month_name = month_name

        if period_type == 'monthly':
            prev_month_name, prev_year = prev_month_year(month_name, year)

        elif period_type == 'quarterly':
            current_q = quarter_for_month(month_name)
            if current_q:
                prev_q, y_delta = prev_quarter(current_q)
                prev_period_type = prev_q
                prev_year = year + y_delta
                prev_month_name = None

        elif period_type in quarter_months:
            prev_q, y_delta = prev_quarter(period_type)
            prev_period_type = prev_q
            prev_year = year + y_delta
            prev_month_name = None

        elif period_type == 'yearly':
            prev_year = year - 1
            prev_month_name = None

        # --- Previous summary (only) ---
        previous_summary = None
        try:
            prev_totals, _, _ = compute_cashflow_summary(
                user_id=user_id,
                year=prev_year,
                country=country,
                period_type=prev_period_type,
                month_name=prev_month_name
            )
            previous_summary = prev_totals
        except Exception:
            previous_summary = None

        response_data = {
            'period_type': period_type,
            'year': year,
            'summary': combined_totals,
            'previous_summary': previous_summary,
            'detailed_data': all_cashflow_data,
            'total_records': len(all_cashflow_data),
        }

        # attach meta fields for current period
        response_data.update(meta)

        return jsonify(response_data), 200

    except Exception as e:
        return jsonify({'error': f"Database error: {str(e)}"}), 500
    finally:
        db_session.close()
