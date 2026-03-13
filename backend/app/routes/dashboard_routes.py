from flask import Blueprint, request, jsonify , send_file
from sqlalchemy import create_engine , MetaData , text, inspect
from sqlalchemy.orm import sessionmaker
from zoneinfo import ZoneInfo
from app.utils.token_utils import get_effective_user_id_from_token
import jwt
import os
import base64
import re
from datetime import datetime 
import pandas as pd
from config import Config
SECRET_KEY = Config.SECRET_KEY

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
        payload, user_id, member_id = get_effective_user_id_from_token(token)
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
        payload, user_id, member_id = get_effective_user_id_from_token(token)
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
        payload, user_id, member_id = get_effective_user_id_from_token(token)
        country = request.args.get('country')
        month = request.args.get('month')
        year = request.args.get('year')

        if not country or not month or not year:
            return jsonify({'error': 'Missing country, month, or year parameters'}), 400

        # ----- month resolution (your existing logic) -----
        requested_month = month.lower()
        requested_year = int(year)

        current_month = datetime.now().strftime("%B").lower()
        current_year = datetime.now().year

        # Dispatch/forecast file naming uses ongoing month if current year
        effective_month = current_month if requested_year == current_year else requested_month
        short_month = effective_month[:3].lower()

        engine = create_engine(db_url)

        def fetch_latest_stored_file(user_id: int, ctry: str, short_month: str):
            """
            Fetch latest matching stored file from public.stored_files.
            Returns (filename, content_type, data_bytes) or None
            """
            like_pattern = f"inventory_forecast_{user_id}_{ctry.lower()}_{short_month}%"

            q = text("""
                SELECT filename, content_type, data
                FROM public.stored_files
                WHERE user_id = :user_id
                  AND LOWER(country) = LOWER(:country)
                  AND kind = 'inventory_forecast'
                  AND LOWER(filename) LIKE LOWER(:like_pattern)
                ORDER BY id DESC
                LIMIT 1
            """)

            with engine.connect() as conn:
                row = conn.execute(q, {
                    "user_id": user_id,
                    "country": ctry.lower(),
                    "like_pattern": like_pattern
                }).fetchone()

            if not row:
                return None

            filename, content_type, data_bytes = row[0], row[1], row[2]

            # bytea often comes back as memoryview in psycopg
            if isinstance(data_bytes, memoryview):
                data_bytes = data_bytes.tobytes()

            return filename, content_type, data_bytes

        # ---------- GLOBAL: merge UK + US ----------
        if country.lower() == 'global':
            uk_row = fetch_latest_stored_file(user_id, "uk", short_month)
            us_row = fetch_latest_stored_file(user_id, "us", short_month)

            if not uk_row and not us_row:
                return jsonify({'error': 'No UK or US dispatch files found in DB'}), 404

            frames = []
            for r in (uk_row, us_row):
                if not r:
                    continue
                _, _, data_bytes = r
                if not data_bytes:
                    continue
                frames.append(pd.read_excel(BytesIO(data_bytes)))

            if not frames:
                return jsonify({'error': 'No readable UK/US dispatch files found in DB'}), 404

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

            # numeric coercion
            for col in [
                'Inventory at Month End',
                'Projected Sales Total',
                'Dispatch',
                'Current Inventory + Dispatch'
            ]:
                if col in combined_df.columns:
                    combined_df[col] = pd.to_numeric(combined_df[col], errors='coerce').fillna(0)

            # sum metrics by product
            sum_cols = [
                'Inventory at Month End',
                'Projected Sales Total',
                'Dispatch',
                'Current Inventory + Dispatch'
            ]
            agg_spec = {c: 'sum' for c in sum_cols if c in combined_df.columns}

            grouped = (
                combined_df.groupby('Product Name', as_index=False).agg(agg_spec)
                if agg_spec else combined_df[['Product Name']].drop_duplicates()
            )

            # weighted avg coverage ratio (weighted by Inventory at Month End)
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
                    combined_df.groupby('Product Name', as_index=False)
                    .apply(weighted_avg)
                    .rename(columns={None: 'Inventory Coverage Ratio Before Dispatch'})
                )

                final_df = pd.merge(grouped, ratio_df, on='Product Name', how='left')
                final_df['Inventory Coverage Ratio Before Dispatch'] = final_df[
                    'Inventory Coverage Ratio Before Dispatch'
                ].apply(lambda x: "-" if pd.isna(x) or x == 0 else round(float(x), 2))
            else:
                final_df = grouped.copy()

            # total row
            total_row = {'Product Name': 'Total'}
            for c in sum_cols:
                if c in final_df.columns:
                    total_row[c] = float(final_df[c].sum())
            if 'Inventory Coverage Ratio Before Dispatch' in final_df.columns:
                total_row['Inventory Coverage Ratio Before Dispatch'] = ''

            final_df = pd.concat([final_df, pd.DataFrame([total_row])], ignore_index=True)

            # write excel to memory
            output = BytesIO()
            with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                final_df.to_excel(writer, index=False, sheet_name='Dispatch')
            output.seek(0)

            return send_file(
                output,
                download_name='global_dispatch.xlsx',
                mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                as_attachment=False
            )

        # ---------- NON-GLOBAL ----------
        row = fetch_latest_stored_file(user_id, country.lower(), short_month)
        if not row:
            return jsonify({
                'error': 'Forecast file not found in DB. Please generate inventory forecast first!'
            }), 404

        filename, content_type, data_bytes = row
        if not data_bytes:
            return jsonify({'error': 'Stored file is empty/corrupt'}), 500

        return send_file(
            BytesIO(data_bytes),
            download_name=filename or f"{country.lower()}_dispatch.xlsx",
            mimetype=content_type or 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=False
        )

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
        return jsonify({'error':'Authorization token missing'}),401

    token = auth_header.split(' ')[1]

    try:
        payload,user_id,member_id = get_effective_user_id_from_token(token)

        country=request.args.get('country')
        month=request.args.get('month')
        year=request.args.get('year')

        if not country or not month or not year:
            return jsonify({'error':'Missing parameters'}),400

        engine=create_engine(db_url)

        query=text("""
        SELECT filename,content_type,data
        FROM public.stored_files
        WHERE user_id=:user_id
        AND LOWER(country)=LOWER(:country)
        AND kind='purchase_order'
        AND LOWER(month)=LOWER(:month)
        AND year=:year
        ORDER BY id DESC
        LIMIT 1
        """)

        with engine.connect() as conn:
            row=conn.execute(query,{
                "user_id":user_id,
                "country":country.lower(),
                "month":month.lower(),
                "year":str(year)
            }).fetchone()

        if not row:
            return jsonify({'error':'Purchase order file not found'}),404

        filename,content_type,data_bytes=row

        if isinstance(data_bytes,memoryview):
            data_bytes=data_bytes.tobytes()

        return send_file(
            BytesIO(data_bytes),
            download_name=filename,
            mimetype=content_type or
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            as_attachment=False
        )

    except jwt.ExpiredSignatureError:
        return jsonify({'error':'Token expired'}),401
    except jwt.InvalidTokenError:
        return jsonify({'error':'Invalid token'}),401
    except Exception as e:
        return jsonify({'error':str(e)}),500
    
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
    from io import BytesIO
    import pandas as pd
    from sqlalchemy import create_engine, text
    from datetime import datetime

    # ---------------- AUTH ----------------
    auth_header = request.headers.get('Authorization')

    if not auth_header or not auth_header.startswith('Bearer '):
        return jsonify({'error': 'Authorization token missing'}), 401

    token = auth_header.split(' ')[1]

    try:
        payload, user_id, member_id = get_effective_user_id_from_token(token)
    except jwt.ExpiredSignatureError:
        return jsonify({'error': 'Token expired'}), 401
    except jwt.InvalidTokenError:
        return jsonify({'error': 'Invalid token'}), 401

    # ---------------- INPUT ----------------
    # Support both form-data and query params
    month = request.form.get('month') or request.args.get('month')
    year = request.form.get('year') or request.args.get('year')
    country = request.form.get('country') or request.args.get('country')

    if not month or not year or not country:
        return jsonify({'error': 'Month, year and country required'}), 400

    try:
        month_clean = month.strip().title()          # march -> March
        month_name = datetime.strptime(month_clean, "%B").strftime("%B")
        month_db = month_name.lower()               # store as lowercase in DB
        year_int = int(year)
        year_db = str(year_int)
        country_db = country.strip().lower()
    except Exception:
        return jsonify({'error': 'Invalid month/year'}), 400

    engine = create_engine(db_url)

    # ---------------- FETCH INVENTORY FORECAST ----------------
    query = text("""
        SELECT filename, data
        FROM public.stored_files
        WHERE user_id = :user_id
          AND country = :country
          AND kind = 'inventory_forecast'
          AND month = :month
          AND year = :year
        ORDER BY id DESC
        LIMIT 1
    """)

    with engine.connect() as conn:
        row = conn.execute(query, {
            "user_id": user_id,
            "country": country_db,
            "month": month_db,
            "year": year_db
        }).fetchone()

    if not row:
        return jsonify({'error': 'Inventory forecast not found'}), 404

    filename_existing, data_bytes = row

    if isinstance(data_bytes, memoryview):
        data_bytes = data_bytes.tobytes()

    inventory_df = pd.read_excel(BytesIO(data_bytes))

    # ---------------- CLEAN FORECAST DATA ----------------
    inventory_df.columns = inventory_df.columns.str.strip()

    if 'sku' not in inventory_df.columns or 'Dispatch' not in inventory_df.columns:
        return jsonify({'error': 'Required columns missing in forecast file'}), 400

    inventory_df['sku'] = inventory_df['sku'].astype(str).str.strip()
    inventory_df['Dispatch'] = pd.to_numeric(
        inventory_df['Dispatch'], errors='coerce'
    ).fillna(0)

    # Remove total rows from forecast
    inventory_df = inventory_df[
        ~inventory_df['sku'].astype(str).str.lower().str.contains('total', na=False)
    ]

    # ---------------- SKU TABLE ----------------
    sku_table = f"sku_{user_id}_data_table"

    try:
        sku_df = pd.read_sql_table(sku_table, engine)
    except Exception:
        return jsonify({'error': f"{sku_table} not found"}), 404

    sku_column = f"sku_{country_db}"

    if sku_column not in sku_df.columns:
        return jsonify({'error': f"{sku_column} column missing"}), 400

    sku_df.rename(columns={sku_column: "sku"}, inplace=True)
    sku_df['sku'] = sku_df['sku'].astype(str).str.strip()

    # Safe numeric conversion
    for col in ['local_stock', 'in_transit_units', 'price']:
        if col in sku_df.columns:
            sku_df[col] = pd.to_numeric(sku_df[col], errors='coerce').fillna(0)
        else:
            sku_df[col] = 0

    # Ensure product_name exists
    if 'product_name' not in sku_df.columns:
        sku_df['product_name'] = ''

    # ---------------- MERGE ----------------
    merged_df = inventory_df.merge(
        sku_df[['sku', 'product_name', 'price', 'local_stock', 'in_transit_units']],
        on='sku',
        how='left'
    )

    merged_df.rename(columns={
        'product_name': 'Product Name',
        'price': 'Cost per Unit (in INR)',
        'local_stock': 'Current Inventory - Local Warehouse',
        'in_transit_units': 'PO Already Raised'
    }, inplace=True)

    merged_df = merged_df.loc[:, ~merged_df.columns.duplicated()]

    # Fill missing merged values
    if 'Product Name' not in merged_df.columns:
        merged_df['Product Name'] = ''

    merged_df['Product Name'] = merged_df['Product Name'].fillna('')

    for col in [
        'Dispatch',
        'Current Inventory - Local Warehouse',
        'PO Already Raised',
        'Cost per Unit (in INR)'
    ]:
        if col not in merged_df.columns:
            merged_df[col] = 0
        merged_df[col] = pd.to_numeric(merged_df[col], errors='coerce').fillna(0)

    # ---------------- DISPATCH COLUMNS ----------------
    merged_df['Dispatches UK'] = merged_df['Dispatch'] if country_db == 'uk' else 0
    merged_df['Dispatches Canada'] = merged_df['Dispatch'] if country_db == 'canada' else 0
    merged_df['Dispatches Amazon US'] = merged_df['Dispatch'] if country_db in ['us', 'amazon us', 'amazon_us'] else 0

    merged_df['Total Dispatches'] = (
        merged_df['Dispatches UK'] +
        merged_df['Dispatches Canada'] +
        merged_df['Dispatches Amazon US']
    )

    # ---------------- PO CALCULATION ----------------
    merged_df['PO to be raised'] = (
        merged_df['Total Dispatches']
        - merged_df['Current Inventory - Local Warehouse']
        - merged_df['PO Already Raised']
    )

    merged_df['PO to be raised'] = pd.to_numeric(
        merged_df['PO to be raised'], errors='coerce'
    ).fillna(0).clip(lower=0)

    merged_df['PO Cost (in INR)'] = (
        merged_df['PO to be raised'] *
        merged_df['Cost per Unit (in INR)']
    )

    # ---------------- FINAL OUTPUT ----------------
    columns_required = [
        'sku',
        'Product Name',
        'Dispatches UK',
        'Dispatches Canada',
        'Dispatches Amazon US',
        'Total Dispatches',
        'Current Inventory - Local Warehouse',
        'PO Already Raised',
        'PO to be raised',
        'Cost per Unit (in INR)',
        'PO Cost (in INR)'
    ]

    for col in columns_required:
        if col not in merged_df.columns:
            merged_df[col] = 0 if col != 'Product Name' and col != 'sku' else ''

    final_df = merged_df[columns_required].copy()
    final_df.rename(columns={'sku': 'SKU'}, inplace=True)
    final_df.insert(0, 'Sno.', range(1, len(final_df) + 1))

    # ---------------- TOTAL ROW ----------------
    totals = final_df.sum(numeric_only=True)

    total_row = {
        'Sno.': '',
        'SKU': '',
        'Product Name': 'Total',
        'Dispatches UK': totals.get('Dispatches UK', 0),
        'Dispatches Canada': totals.get('Dispatches Canada', 0),
        'Dispatches Amazon US': totals.get('Dispatches Amazon US', 0),
        'Total Dispatches': totals.get('Total Dispatches', 0),
        'Current Inventory - Local Warehouse': totals.get('Current Inventory - Local Warehouse', 0),
        'PO Already Raised': totals.get('PO Already Raised', 0),
        'PO to be raised': totals.get('PO to be raised', 0),
        'Cost per Unit (in INR)': '',
        'PO Cost (in INR)': totals.get('PO Cost (in INR)', 0)
    }

    final_df = pd.concat([final_df, pd.DataFrame([total_row])], ignore_index=True)

    # ---------------- EXPORT EXCEL ----------------
    output = BytesIO()

    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        final_df.to_excel(writer, index=False, sheet_name="Purchase Order")

    output.seek(0)
    file_bytes = output.read()

    filename = f"purchase_order_{user_id}_{country_db}_{month_name}_{year_int}.xlsx"

    # ---------------- SAVE FILE (UPSERT) ----------------
    upsert_query = text("""
        INSERT INTO public.stored_files
        (user_id, country, kind, month, year, filename, content_type, data, created_at)
        VALUES (
            :user_id,
            :country,
            'purchase_order',
            :month,
            :year,
            :filename,
            'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            :data,
            NOW()
        )
        ON CONFLICT (user_id, country, filename)
        DO UPDATE SET
            kind = EXCLUDED.kind,
            month = EXCLUDED.month,
            year = EXCLUDED.year,
            content_type = EXCLUDED.content_type,
            data = EXCLUDED.data,
            created_at = NOW()
    """)

    with engine.begin() as conn:
        conn.execute(upsert_query, {
            "user_id": user_id,
            "country": country_db,
            "month": month_db,
            "year": year_db,
            "filename": filename,
            "data": file_bytes
        })

    return jsonify({
        "message": "Purchase order generated and stored successfully",
        "filename": filename
    }), 200



@dashboard_bp.route('/global_purchase_order', methods=['GET'])
def global_PO_generated():
    auth_header = request.headers.get('Authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        return jsonify({'error': 'Authorization token is missing or invalid'}), 401

    token = auth_header.split(' ')[1]
    try:
        payload, user_id, member_id = get_effective_user_id_from_token(token)
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
         f'purchase_order_{user_id}_{country}_{month_name.lower()}_{year}.xlsx'
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
    output_path = os.path.join( f'purchase_order_{user_id}_{file_suffix}_{month_name.lower()}_{year}.xlsx')
    
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
        payload, user_id, member_id = get_effective_user_id_from_token(token)
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
        country_file_path = os.path.join( f'purchase_order_{user_id}_{country}_{month.lower()}_{year}.xlsx')
        if os.path.exists(country_file_path):
            existing_files[country] = country_file_path
    
    # Determine which file to serve
    file_path = None
    download_name = None
    
    if len(existing_files) > 1:
        # Multiple countries exist - look for global file
        file_path = os.path.join( f'purchase_order_{user_id}_global_{month.lower()}_{year}.xlsx')
        download_name = f'global_purchase_order_{month}_{year}.xlsx'
    elif len(existing_files) == 1:
        # Single country exists - use that country file or its corresponding generated file
        country = list(existing_files.keys())[0]
        file_path = os.path.join( f'purchase_order_{user_id}_{country}_{month.lower()}_{year}.xlsx')
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
        payload, user_id, member_id = get_effective_user_id_from_token(token)
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

        matched_files = [f for f in os.listdir() if pattern.match(f)]

        if not matched_files:
            return jsonify({'error': 'Forecast file not found. Please generate inventory forecast first!'}), 404

        # You can pick the latest file if multiple found
        matched_files.sort(reverse=True)  # Sort newest first
        selected_file = matched_files[0]
        file_path = os.path.join( selected_file)

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
        payload, user_id, member_id = get_effective_user_id_from_token(token)
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




@dashboard_bp.route('/target-summary', methods=['GET', 'POST'])
def target_summary():
    auth_header = request.headers.get('Authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        return jsonify({'error': 'Authorization token is missing or invalid'}), 401

    token = auth_header.split(' ')[1]
    try:
        payload, user_id, member_id = get_effective_user_id_from_token(token)
    except jwt.ExpiredSignatureError:
        return jsonify({'error': 'Token has expired'}), 401
    except jwt.InvalidTokenError:
        return jsonify({'error': 'Invalid token'}), 401

    # POST -> body, GET -> query params
    if request.method == 'POST':
        data = request.get_json(silent=True) or {}
        month = data.get('month')
        year = data.get('year')
        country_param = data.get('country', '')
        currency_param = (data.get('currency') or '').lower()
        target_sales = data.get('target_sales')
    else:
        month = request.args.get('month')
        year = request.args.get('year')
        country_param = request.args.get('country', '')
        currency_param = (request.args.get('currency') or '').lower()
        target_sales = request.args.get('target_sales')

    country = resolve_country(country_param, currency_param)

    if not month:
        return jsonify({'error': 'Month is required'}), 400
    if not year:
        return jsonify({'error': 'Year is required'}), 400
    if not country:
        return jsonify({'error': 'country is required'}), 400

    try:
        year = int(year)
    except ValueError:
        return jsonify({'error': 'Invalid year format'}), 400

    try:
        month_name = datetime.strptime(month.capitalize(), "%B").strftime("%B")
    except ValueError:
        return jsonify({'error': 'Invalid month format. Use full month name like January'}), 400

    # POST should only allow current month and current year
    if request.method == 'POST':
        now = datetime.now(ZoneInfo("Asia/Kolkata"))
        current_month = now.strftime("%B")
        current_year = now.year

        if month_name != current_month or year != current_year:
            return jsonify({
                'error': 'POST is allowed only for the current month and year',
                'allowed': {
                    'month': current_month,
                    'year': current_year
                },
                'received': {
                    'month': month_name,
                    'year': year
                }
            }), 400

    engine = create_engine(db_url)
    SessionLocal = sessionmaker(bind=engine)
    db_session = SessionLocal()
    inspector = inspect(engine)

    def compute_monthly_cashflow_summary(user_id: int, year: int, country: str, month_name: str):
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

        countries_with_data = set()

        upload_query = text("""
            SELECT DISTINCT country
            FROM upload_history
            WHERE user_id = :user_id
              AND LOWER(month) = LOWER(:month)
              AND year = :year
              AND LOWER(country) = LOWER(:country)
        """)
        upload_results = db_session.execute(upload_query, {
            'user_id': user_id,
            'month': month_name,
            'year': year,
            'country': country
        }).fetchall()

        for result in upload_results:
            countries_with_data.add(result[0])

        if not countries_with_data:
            return combined_totals, []

        all_cashflow_data = []

        for record_country in countries_with_data:
            total_otherwplatform = 0
            total_taxncredit_from_upload = 0

            upload_values_query = text("""
                SELECT otherwplatform, taxncredit
                FROM upload_history
                WHERE user_id = :user_id
                  AND LOWER(month) = LOWER(:month)
                  AND year = :year
                  AND LOWER(country) = LOWER(:country)
                LIMIT 1
            """)
            upload_values_result = db_session.execute(upload_values_query, {
                'user_id': user_id,
                'month': month_name,
                'year': year,
                'country': record_country
            }).fetchone()

            if upload_values_result:
                if upload_values_result[0]:
                    total_otherwplatform += float(upload_values_result[0])
                if upload_values_result[1]:
                    total_taxncredit_from_upload += float(upload_values_result[1])

            suffix = f"{month_name.lower()}{year}"
            table_name = (
                f"skuwisemonthly_{user_id}_{record_country.lower()}_{suffix}_table"
                if record_country.lower().startswith("global")
                else f"skuwisemonthly_{user_id}_{record_country.lower()}_{suffix}"
            )

            if not inspector.has_table(table_name):
                continue

            try:
                cashflow_df = pd.read_sql_table(table_name, engine)
                if cashflow_df.empty:
                    continue

                numeric_cols = [
                    'net_sales', 'gross_sales', 'advertising_total', 'amazon_fee',
                    'cm2_profit', 'cost_of_unit_sold', 'taxncredit', 'rembursement_fee',
                    'total_quantity', 'selling_fees', 'fba_fees', 'promotional_rebates'
                ]

                for col in numeric_cols:
                    if col in cashflow_df.columns:
                        cashflow_df[col] = pd.to_numeric(cashflow_df[col], errors='coerce').fillna(0)

                def find_total_row(df):
                    if 'product_name' not in df.columns:
                        return None
                    for variation in ['TOTAL', 'Total', 'total', 'TOTALS', 'Totals', 'totals']:
                        total_row = df[df['product_name'] == variation]
                        if not total_row.empty:
                            return total_row
                    return df[df['product_name'].str.contains('total', case=False, na=False)]

                total_row = find_total_row(cashflow_df) if 'product_name' in cashflow_df.columns else None

                if total_row is not None and not total_row.empty:
                    net_sales_total = float(total_row['net_sales'].iloc[0]) if 'net_sales' in total_row else 0
                    cm2_profit_total = float(total_row['cm2_profit'].iloc[0]) if 'cm2_profit' in total_row else 0
                    cost_of_unit_sold_total = float(total_row['cost_of_unit_sold'].iloc[0]) if 'cost_of_unit_sold' in total_row else 0
                else:
                    net_sales_total = float(cashflow_df['net_sales'].sum()) if 'net_sales' in cashflow_df.columns else 0
                    cm2_profit_total = float(cashflow_df['cm2_profit'].sum()) if 'cm2_profit' in cashflow_df.columns else 0
                    cost_of_unit_sold_total = float(cashflow_df['cost_of_unit_sold'].sum()) if 'cost_of_unit_sold' in cashflow_df.columns else 0

                cashflow_total = cost_of_unit_sold_total + cm2_profit_total

                combined_totals['net_sales'] += net_sales_total
                combined_totals['cm2_profit'] += cm2_profit_total
                combined_totals['cost_of_unit_sold'] += cost_of_unit_sold_total
                combined_totals['cashflow'] += cashflow_total
                combined_totals['taxncredit'] += total_taxncredit_from_upload
                combined_totals['otherwplatform'] += total_otherwplatform

                all_cashflow_data.append({
                    'country': record_country,
                    'table_name': table_name,
                    'net_sales_total': round(net_sales_total, 2),
                    'cashflow_total': round(cashflow_total, 2)
                })

            except Exception:
                continue

        for k in combined_totals:
            combined_totals[k] = round(combined_totals[k], 2)

        return combined_totals, all_cashflow_data

    try:
        safe_country = country.lower().replace(" ", "_").replace("-", "_")
        target_table_name = f"target_{user_id}_{safe_country}_data"

        create_table_sql = text(f"""
            CREATE TABLE IF NOT EXISTS {target_table_name} (
                id SERIAL PRIMARY KEY,
                month VARCHAR(20) NOT NULL,
                year INTEGER NOT NULL,
                country VARCHAR(50) NOT NULL,
                target_sales NUMERIC(12,2) NOT NULL DEFAULT 0,
                cashflow_total NUMERIC(12,2) NOT NULL DEFAULT 0,
                net_sales_total NUMERIC(12,2) NOT NULL DEFAULT 0,
                shortfall_total NUMERIC(12,2) NOT NULL DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE (month, year, country)
            )
        """)
        db_session.execute(create_table_sql)
        db_session.commit()

        # POST
        if request.method == 'POST':
            if target_sales is None:
                return jsonify({'error': 'target_sales is required for POST'}), 400

            try:
                target_sales = float(target_sales)
            except ValueError:
                return jsonify({'error': 'target_sales must be numeric'}), 400

            summary_totals, details = compute_monthly_cashflow_summary(
                user_id=user_id,
                year=year,
                country=country,
                month_name=month_name
            )


            # If no cashflow data exists, treat totals as 0
            net_sales_total = round(summary_totals.get('net_sales', 0), 2)
            cashflow_total = round(summary_totals.get('cashflow', 0), 2)

            # If no details found, still allow saving target
            if not details:
                net_sales_total = 0
                cashflow_total = 0

            shortfall_total = round(target_sales - net_sales_total, 2)

            upsert_sql = text(f"""
                INSERT INTO {target_table_name}
                    (month, year, country, target_sales, cashflow_total, net_sales_total, shortfall_total, updated_at)
                VALUES
                    (:month, :year, :country, :target_sales, :cashflow_total, :net_sales_total, :shortfall_total, CURRENT_TIMESTAMP)
                ON CONFLICT (month, year, country)
                DO UPDATE SET
                    target_sales = EXCLUDED.target_sales,
                    cashflow_total = EXCLUDED.cashflow_total,
                    net_sales_total = EXCLUDED.net_sales_total,
                    shortfall_total = EXCLUDED.shortfall_total,
                    updated_at = CURRENT_TIMESTAMP
            """)
            db_session.execute(upsert_sql, {
                'month': month_name,
                'year': year,
                'country': country,
                'target_sales': target_sales,
                'cashflow_total': cashflow_total,
                'net_sales_total': net_sales_total,
                'shortfall_total': shortfall_total
            })
            db_session.commit()

            return jsonify({
                'message': 'Target summary saved successfully',
                'data': {
                    'user_id': user_id,
                    'month': month_name,
                    'year': year,
                    'country': country,
                    'target_sales': round(target_sales, 2),
                    'cashflow_total': cashflow_total,
                    'net_sales_total': net_sales_total,
                    'shortfall_total': shortfall_total,
                    'table_name': target_table_name
                }
            }), 200

        # GET
        get_sql = text(f"""
            SELECT id, month, year, country, target_sales, cashflow_total,
                   net_sales_total, shortfall_total, created_at, updated_at
            FROM {target_table_name}
            WHERE LOWER(month) = LOWER(:month)
              AND year = :year
              AND LOWER(country) = LOWER(:country)
            LIMIT 1
        """)
        row = db_session.execute(get_sql, {
            'month': month_name,
            'year': year,
            'country': country
        }).fetchone()

        if not row:
            return jsonify({
                'error': 'No saved target summary found',
                'searched_for': {
                    'user_id': user_id,
                    'month': month_name,
                    'year': year,
                    'country': country
                },
                'table_name': target_table_name
            }), 404

        return jsonify({
            'message': 'Target summary fetched successfully',
            'data': {
                'id': row[0],
                'month': row[1],
                'year': row[2],
                'country': row[3],
                'target_sales': float(row[4]),
                'cashflow_total': float(row[5]),
                'net_sales_total': float(row[6]),
                'shortfall_total': float(row[7]),
                'created_at': row[8].isoformat() if row[8] else None,
                'updated_at': row[9].isoformat() if row[9] else None,
                'table_name': target_table_name
            }
        }), 200

    except Exception as e:
        db_session.rollback()
        return jsonify({'error': f'Database error: {str(e)}'}), 500
    finally:
        db_session.close()

