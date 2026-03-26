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
db_url1 = os.getenv('DATABASE_ADMIN_URL')



dashboard_bp = Blueprint('dashboard_bp', __name__)


engine = create_engine(
    db_url,
    pool_pre_ping=True,
    pool_size=10,
    max_overflow=20,
    pool_recycle=1800
)

conv_engine = create_engine(
    db_url1,
    pool_pre_ping=True,
    pool_size=5,
    max_overflow=10,
    pool_recycle=1800
)

SessionLocal = sessionmaker(bind=engine)

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

        requested_month = month.strip().lower()
        requested_year = int(year)

        current_month = datetime.now().strftime("%B").lower()
        current_year = datetime.now().year

        # Existing naming behavior preserved
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

            if isinstance(data_bytes, memoryview):
                data_bytes = data_bytes.tobytes()

            return filename, content_type, data_bytes

        def normalize_col_name(col) -> str:
            cleaned = str(col or '').replace('\u00A0', ' ')
            cleaned = ' '.join(cleaned.split()).strip()

            lower = cleaned.lower()

            header_map = {
                'sku': 'sku',
                'product name': 'Product Name',
                'inventory at month end': 'Inventory at Month End',
                'projected sales total': 'Projected Sales Total',
                'dispatch': 'Dispatch',
                'current inventory + dispatch': 'Current Inventory + Dispatch',
                'inventory coverage ratio before dispatch': 'Inventory Coverage Ratio Before Dispatch',
            }

            return header_map.get(lower, cleaned)

        def find_header_row(excel_rows):
            """
            Finds the actual header row in sheet data.
            Looks for Product Name plus at least one dispatch-related column.
            """
            for idx, row in enumerate(excel_rows):
                normalized = [normalize_col_name(cell) for cell in row]
                if (
                    'Product Name' in normalized and
                    (
                        'Dispatch' in normalized or
                        'Inventory at Month End' in normalized or
                        'sku' in normalized
                    )
                ):
                    return idx
            return -1

        def read_dispatch_dataframe(data_bytes: bytes) -> pd.DataFrame:
            """
            Reads uploaded workbook robustly:
            - prefers sheet named 'Dispatch'
            - detects actual header row dynamically
            """
            excel_file = pd.ExcelFile(BytesIO(data_bytes))
            sheet_name = next(
                (s for s in excel_file.sheet_names if s.strip().lower() == 'dispatch'),
                excel_file.sheet_names[0]
            )

            preview_df = pd.read_excel(
                BytesIO(data_bytes),
                sheet_name=sheet_name,
                header=None
            )

            excel_rows = preview_df.fillna('').values.tolist()
            header_row_index = find_header_row(excel_rows)

            if header_row_index == -1:
                raise ValueError(
                    f"Could not find dispatch header row in sheet '{sheet_name}'."
                )

            df = pd.read_excel(
                BytesIO(data_bytes),
                sheet_name=sheet_name,
                header=header_row_index
            )

            df.columns = [normalize_col_name(c) for c in df.columns]
            df = df.loc[:, [str(c).strip() != '' and not str(c).startswith('Unnamed:') for c in df.columns]]

            return df

        def clean_dispatch_dataframe(df: pd.DataFrame) -> pd.DataFrame:
            expected_columns = [
                'Product Name',
                'sku',
                'Inventory at Month End',
                'Projected Sales Total',
                'Dispatch',
                'Current Inventory + Dispatch',
                'Inventory Coverage Ratio Before Dispatch'
            ]

            available_columns = [c for c in expected_columns if c in df.columns]

            if 'Product Name' not in available_columns:
                raise ValueError(f"'Product Name' column missing. Found columns: {list(df.columns)}")

            cleaned = df[available_columns].copy()
            cleaned['Product Name'] = cleaned['Product Name'].astype(str).str.strip()

            if 'sku' in cleaned.columns:
                cleaned['sku'] = cleaned['sku'].astype(str).str.strip()

            cleaned = cleaned[
                cleaned['Product Name'].notna() &
                (cleaned['Product Name'] != '') &
                (cleaned['Product Name'].str.lower() != 'nan') &
                (cleaned['Product Name'].str.lower() != 'total')
            ].copy()

            numeric_cols = [
                'Inventory at Month End',
                'Projected Sales Total',
                'Dispatch',
                'Current Inventory + Dispatch',
                'Inventory Coverage Ratio Before Dispatch'
            ]

            for col in numeric_cols:
                if col in cleaned.columns:
                    cleaned[col] = pd.to_numeric(cleaned[col], errors='coerce').fillna(0)

            return cleaned

        def build_global_dispatch_file(frames: list[pd.DataFrame]) -> BytesIO:
            combined_df = pd.concat(frames, ignore_index=True)

            group_keys = ['Product Name']
            if 'sku' in combined_df.columns:
                group_keys.append('sku')

            sum_cols = [
                'Inventory at Month End',
                'Projected Sales Total',
                'Dispatch',
                'Current Inventory + Dispatch'
            ]
            agg_spec = {c: 'sum' for c in sum_cols if c in combined_df.columns}

            if agg_spec:
                grouped = combined_df.groupby(group_keys, as_index=False).agg(agg_spec)
            else:
                grouped = combined_df[group_keys].drop_duplicates()

            if (
                'Inventory Coverage Ratio Before Dispatch' in combined_df.columns and
                'Inventory at Month End' in combined_df.columns
            ):
                def weighted_avg(group):
                    denom = group['Inventory at Month End'].sum()
                    if denom <= 0:
                        return 0
                    return (
                        group['Inventory Coverage Ratio Before Dispatch'] *
                        group['Inventory at Month End']
                    ).sum() / denom

                ratio_df = (
                    combined_df.groupby(group_keys)
                    .apply(weighted_avg)
                    .reset_index(name='Inventory Coverage Ratio Before Dispatch')
                )

                final_df = pd.merge(grouped, ratio_df, on=group_keys, how='left')
            else:
                final_df = grouped.copy()

            ordered_cols = [
                'Product Name',
                'sku',
                'Inventory at Month End',
                'Projected Sales Total',
                'Dispatch',
                'Current Inventory + Dispatch',
                'Inventory Coverage Ratio Before Dispatch'
            ]
            final_df = final_df[[c for c in ordered_cols if c in final_df.columns]]

            total_row = {}
            for col in final_df.columns:
                if col == 'Product Name':
                    total_row[col] = 'Total'
                elif col == 'sku':
                    total_row[col] = ''
                elif col in [
                    'Inventory at Month End',
                    'Projected Sales Total',
                    'Dispatch',
                    'Current Inventory + Dispatch'
                ]:
                    total_row[col] = float(final_df[col].sum()) if col in final_df.columns else 0
                elif col == 'Inventory Coverage Ratio Before Dispatch':
                    total_row[col] = ''
                else:
                    total_row[col] = ''

            final_df = pd.concat([final_df, pd.DataFrame([total_row])], ignore_index=True)

            output = BytesIO()
            with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                final_df.to_excel(writer, index=False, sheet_name='Dispatch')

                workbook = writer.book
                worksheet = writer.sheets['Dispatch']

                number_format = workbook.add_format({'num_format': '#,##0.##'})
                header_format = workbook.add_format({
                    'bold': True,
                    'align': 'center',
                    'valign': 'vcenter'
                })

                for col_idx, col_name in enumerate(final_df.columns):
                    worksheet.write(0, col_idx, col_name, header_format)

                    if col_name in [
                        'Inventory at Month End',
                        'Projected Sales Total',
                        'Dispatch',
                        'Current Inventory + Dispatch',
                        'Inventory Coverage Ratio Before Dispatch'
                    ]:
                        worksheet.set_column(col_idx, col_idx, 22, number_format)
                    elif col_name == 'Product Name':
                        worksheet.set_column(col_idx, col_idx, 40)
                    elif col_name == 'sku':
                        worksheet.set_column(col_idx, col_idx, 20)
                    else:
                        worksheet.set_column(col_idx, col_idx, 18)

            output.seek(0)
            return output

        # ---------- GLOBAL ----------
        if country.lower() == 'global':
            uk_row = fetch_latest_stored_file(user_id, 'uk', short_month)
            us_row = fetch_latest_stored_file(user_id, 'us', short_month)

            if not uk_row and not us_row:
                return jsonify({'error': 'No UK or US forecast files found in DB'}), 404

            frames = []

            for row in (uk_row, us_row):
                if not row:
                    continue

                _, _, data_bytes = row
                if not data_bytes:
                    continue

                df = read_dispatch_dataframe(data_bytes)
                df = clean_dispatch_dataframe(df)

                if not df.empty:
                    frames.append(df)

            if not frames:
                return jsonify({'error': 'No readable UK/US dispatch data found in DB'}), 404

            output = build_global_dispatch_file(frames)

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
    except ValueError as ve:
        return jsonify({'error': str(ve)}), 400
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

    month = request.form.get('month') or request.args.get('month')
    year = request.form.get('year') or request.args.get('year')
    country = request.form.get('country') or request.args.get('country')

    if not month or not year or not country:
        return jsonify({'error': 'Month, year and country required'}), 400

    try:
        month_clean = month.strip().title()
        month_name = datetime.strptime(month_clean, "%B").strftime("%B")
        month_db = month_name.lower()
        year_int = int(year)
        year_db = str(year_int)
        country_db = country.strip().lower()
    except Exception:
        return jsonify({'error': 'Invalid month/year'}), 400

    engine = create_engine(db_url)

    forecast_query = text("""
        SELECT filename, data
        FROM public.stored_files
        WHERE user_id = :user_id
          AND LOWER(country) = LOWER(:country)
          AND kind = 'inventory_forecast'
          AND LOWER(month) = LOWER(:month)
          AND year = :year
        ORDER BY id DESC
        LIMIT 1
    """)

    with engine.connect() as conn:
        row = conn.execute(forecast_query, {
            "user_id": user_id,
            "country": country_db,
            "month": month_db,
            "year": year_db
        }).fetchone()

    if not row:
        return jsonify({'error': 'Inventory forecast not found'}), 404

    _, data_bytes = row

    if isinstance(data_bytes, memoryview):
        data_bytes = data_bytes.tobytes()

    try:
        inventory_df = pd.read_excel(BytesIO(data_bytes), sheet_name='Dispatch', header=6)
    except Exception as e:
        return jsonify({'error': f'Unable to read Dispatch sheet: {str(e)}'}), 400

    inventory_df.columns = [str(c).strip() for c in inventory_df.columns]

    if 'sku' not in inventory_df.columns or 'Dispatch' not in inventory_df.columns:
        return jsonify({
            'error': f"Required columns missing in Dispatch sheet. Found: {list(inventory_df.columns)}"
        }), 400

    inventory_df['sku'] = inventory_df['sku'].astype(str).str.strip()
    inventory_df['Dispatch'] = pd.to_numeric(inventory_df['Dispatch'], errors='coerce').fillna(0)

    if 'Product Name' not in inventory_df.columns:
        inventory_df['Product Name'] = ''

    inventory_df['Product Name'] = inventory_df['Product Name'].astype(str).str.strip()

    inventory_df = inventory_df[
        ~inventory_df['sku'].astype(str).str.lower().str.contains('total', na=False)
    ].copy()

    inventory_df = inventory_df[
        inventory_df['sku'].notna() & (inventory_df['sku'] != '')
    ].copy()

    sku_table = f"sku_{user_id}_data_table"

    try:
        sku_df = pd.read_sql_table(sku_table, engine)
    except Exception:
        return jsonify({'error': f"{sku_table} not found"}), 404

    sku_column = f"sku_{country_db}"
    if sku_column not in sku_df.columns:
        return jsonify({'error': f"{sku_column} column missing"}), 400

    sku_df = sku_df.copy()
    sku_df.rename(columns={sku_column: 'sku'}, inplace=True)
    sku_df['sku'] = sku_df['sku'].astype(str).str.strip()

    for col in ['local_stock', 'in_transit_units', 'price']:
        if col in sku_df.columns:
            sku_df[col] = pd.to_numeric(sku_df[col], errors='coerce').fillna(0)
        else:
            sku_df[col] = 0

    if 'product_name' not in sku_df.columns:
        sku_df['product_name'] = ''

    sku_df['product_name'] = sku_df['product_name'].fillna('').astype(str).str.strip()

    merged_df = inventory_df.merge(
        sku_df[['sku', 'product_name', 'price', 'local_stock', 'in_transit_units']],
        on='sku',
        how='left'
    )

    merged_df.rename(columns={
        'product_name': 'Product Name DB',
        'price': 'Cost per Unit (in INR)',
        'local_stock': 'Current Inventory - Local Warehouse',
        'in_transit_units': 'PO Already Raised'
    }, inplace=True)

    merged_df['Product Name'] = merged_df['Product Name'].replace('', pd.NA)
    merged_df['Product Name'] = merged_df['Product Name'].fillna(merged_df.get('Product Name DB', ''))
    merged_df['Product Name'] = merged_df['Product Name'].fillna('').astype(str).str.strip()

    for col in [
        'Dispatch',
        'Current Inventory - Local Warehouse',
        'PO Already Raised',
        'Cost per Unit (in INR)'
    ]:
        if col not in merged_df.columns:
            merged_df[col] = 0
        merged_df[col] = pd.to_numeric(merged_df[col], errors='coerce').fillna(0)

    merged_df['Dispatches UK'] = merged_df['Dispatch'] if country_db == 'uk' else 0
    merged_df['Dispatches Canada'] = merged_df['Dispatch'] if country_db == 'canada' else 0
    merged_df['Dispatches Amazon US'] = merged_df['Dispatch'] if country_db in ['us', 'amazon us', 'amazon_us'] else 0

    merged_df['Total Dispatches'] = (
        merged_df['Dispatches UK'] +
        merged_df['Dispatches Canada'] +
        merged_df['Dispatches Amazon US']
    )

    merged_df['PO to be raised'] = (
        merged_df['Total Dispatches']
        - merged_df['Current Inventory - Local Warehouse']
        - merged_df['PO Already Raised']
    )

    merged_df['PO to be raised'] = pd.to_numeric(
        merged_df['PO to be raised'], errors='coerce'
    ).fillna(0).clip(lower=0)

    merged_df['PO Cost (in INR)'] = (
        merged_df['PO to be raised'] * merged_df['Cost per Unit (in INR)']
    )

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
            merged_df[col] = '' if col in ['sku', 'Product Name'] else 0

    final_df = merged_df[columns_required].copy()
    final_df.rename(columns={'sku': 'SKU'}, inplace=True)
    final_df.insert(0, 'Sno.', range(1, len(final_df) + 1))

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

    output = BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        final_df.to_excel(writer, index=False, sheet_name='Purchase Order')
    output.seek(0)
    file_bytes = output.read()

    filename = f"purchase_order_{user_id}_{country_db}_{month_db}_{year_db}.xlsx"

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
    from io import BytesIO
    import pandas as pd
    from sqlalchemy import create_engine, text
    from datetime import datetime

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
        month_name = datetime.strptime(month.strip().title(), "%B").strftime("%B")
        month_db = month_name.lower()
        year_db = str(int(year))
    except Exception:
        return jsonify({'error': 'Invalid month or year format'}), 400

    engine = create_engine(db_url)

    def fetch_latest_po(country_code: str):
        filename = f"purchase_order_{user_id}_{country_code}_{month_db}_{year_db}.xlsx"

        q = text("""
            SELECT filename, content_type, data
            FROM public.stored_files
            WHERE user_id = :user_id
              AND LOWER(country) = LOWER(:country)
              AND kind = 'purchase_order'
              AND LOWER(month) = LOWER(:month)
              AND year = :year
              AND LOWER(filename) = LOWER(:filename)
            ORDER BY id DESC
            LIMIT 1
        """)

        with engine.connect() as conn:
            row = conn.execute(q, {
                "user_id": user_id,
                "country": country_code,
                "month": month_db,
                "year": year_db,
                "filename": filename
            }).fetchone()

        if not row:
            return None

        filename, content_type, data_bytes = row[0], row[1], row[2]
        if isinstance(data_bytes, memoryview):
            data_bytes = data_bytes.tobytes()

        return filename, content_type, data_bytes

    def read_po_df(data_bytes: bytes) -> pd.DataFrame:
        df = pd.read_excel(BytesIO(data_bytes), sheet_name=0)
        df.columns = [str(c).strip() for c in df.columns]
        return df

    countries = ['uk', 'us']
    po_rows = {country: fetch_latest_po(country) for country in countries}
    country_files_found = [country for country, row in po_rows.items() if row]

    if not country_files_found:
        return jsonify({
            'error': 'No country-specific purchase order files found. Please generate UK and/or US purchase orders first.'
        }), 404

    global_df = pd.DataFrame()

    numeric_columns = [
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

    for country in country_files_found:
        try:
            _, _, data_bytes = po_rows[country]
            country_po_df = read_po_df(data_bytes)

            if 'Product Name' not in country_po_df.columns:
                return jsonify({'error': f'Product Name column missing in {country.upper()} purchase order file'}), 400

            if 'SKU' in country_po_df.columns and 'sku' not in country_po_df.columns:
                country_po_df.rename(columns={'SKU': 'sku'}, inplace=True)

            if 'PO to Be raised' in country_po_df.columns and 'PO to be raised' not in country_po_df.columns:
                country_po_df.rename(columns={'PO to Be raised': 'PO to be raised'}, inplace=True)

            if 'sku' not in country_po_df.columns:
                country_po_df['sku'] = ''

            country_po_df['sku'] = country_po_df['sku'].fillna('').astype(str).str.strip()
            country_po_df['Product Name'] = country_po_df['Product Name'].fillna('').astype(str).str.strip()

            country_po_df = country_po_df[
                country_po_df['Product Name'].str.lower() != 'total'
            ].copy()

            for col in numeric_columns:
                if col not in country_po_df.columns:
                    country_po_df[col] = 0
                country_po_df[col] = pd.to_numeric(country_po_df[col], errors='coerce').fillna(0)

            merge_columns = ['sku', 'Product Name'] + numeric_columns
            country_data = country_po_df[merge_columns].copy()

            if global_df.empty:
                global_df = country_data.copy()
            else:
                global_df = pd.merge(
                    global_df,
                    country_data,
                    on='Product Name',
                    how='outer',
                    suffixes=('', '_new')
                )

                if 'sku_new' in global_df.columns:
                    global_df['sku'] = global_df['sku'].fillna('')
                    global_df['sku_new'] = global_df['sku_new'].fillna('')
                    global_df['sku'] = global_df['sku'].replace('', pd.NA).fillna(global_df['sku_new'])
                    global_df['sku'] = global_df['sku'].fillna('')
                    global_df.drop(columns=['sku_new'], inplace=True)

                for col in numeric_columns:
                    new_col = f'{col}_new'
                    if new_col in global_df.columns:
                        if col not in global_df.columns:
                            global_df[col] = 0
                        global_df[col] = global_df[col].fillna(0) + global_df[new_col].fillna(0)
                        global_df.drop(columns=[new_col], inplace=True)

        except Exception as e:
            return jsonify({'error': f'Error processing {country.upper()} file: {str(e)}'}), 500

    if global_df.empty:
        return jsonify({'error': 'No purchase order rows found to merge'}), 404

    if 'Dispatches UK' not in global_df.columns:
        global_df['Dispatches UK'] = 0
    if 'Dispatches Canada' not in global_df.columns:
        global_df['Dispatches Canada'] = 0
    if 'Dispatches Amazon US' not in global_df.columns:
        global_df['Dispatches Amazon US'] = 0
    if 'Current Inventory - Local Warehouse' not in global_df.columns:
        global_df['Current Inventory - Local Warehouse'] = 0
    if 'PO Already Raised' not in global_df.columns:
        global_df['PO Already Raised'] = 0
    if 'Cost per Unit (in INR)' not in global_df.columns:
        global_df['Cost per Unit (in INR)'] = 0

    global_df['Total Dispatches'] = (
        global_df['Dispatches UK'].fillna(0) +
        global_df['Dispatches Canada'].fillna(0) +
        global_df['Dispatches Amazon US'].fillna(0)
    )

    global_df['PO to be raised'] = (
        global_df['Total Dispatches'] -
        global_df['Current Inventory - Local Warehouse'].fillna(0) -
        global_df['PO Already Raised'].fillna(0)
    ).clip(lower=0)

    global_df['PO Cost (in INR)'] = (
        global_df['PO to be raised'] * global_df['Cost per Unit (in INR)'].fillna(0)
    )

    numeric_to_sum = [
        'Dispatches UK',
        'Dispatches Canada',
        'Dispatches Amazon US',
        'Total Dispatches',
        'Current Inventory - Local Warehouse',
        'PO Already Raised',
        'PO to be raised',
        'PO Cost (in INR)'
    ]

    agg_dict = {'sku': 'first'}
    for col in numeric_to_sum:
        agg_dict[col] = 'sum'
    agg_dict['Cost per Unit (in INR)'] = 'first'

    grouped_df = global_df.groupby('Product Name', as_index=False).agg(agg_dict)
    grouped_df['PO Cost (in INR)'] = grouped_df['PO to be raised'] * grouped_df['Cost per Unit (in INR)'].fillna(0)

    final_df = pd.DataFrame()
    final_df['Sno.'] = range(1, len(grouped_df) + 1)
    final_df['Product Name'] = grouped_df['Product Name']
    final_df['Dispatches UK'] = grouped_df.get('Dispatches UK', 0)
    final_df['Dispatches Canada'] = grouped_df.get('Dispatches Canada', 0)
    final_df['Dispatches Amazon US'] = grouped_df.get('Dispatches Amazon US', 0)
    final_df['Total Dispatches'] = grouped_df.get('Total Dispatches', 0)
    final_df['Current Inventory - Local Warehouse'] = grouped_df.get('Current Inventory - Local Warehouse', 0)
    final_df['PO Already Raised'] = grouped_df.get('PO Already Raised', 0)
    final_df['PO to be raised'] = grouped_df.get('PO to be raised', 0)
    final_df['Cost per Unit (in INR)'] = grouped_df.get('Cost per Unit (in INR)', 0)
    final_df['PO Cost (in INR)'] = grouped_df.get('PO Cost (in INR)', 0)

    total_row = {
        'Sno.': 'Total',
        'Product Name': '',
        'Dispatches UK': final_df['Dispatches UK'].sum(),
        'Dispatches Canada': final_df['Dispatches Canada'].sum(),
        'Dispatches Amazon US': final_df['Dispatches Amazon US'].sum(),
        'Total Dispatches': final_df['Total Dispatches'].sum(),
        'Current Inventory - Local Warehouse': final_df['Current Inventory - Local Warehouse'].sum(),
        'PO Already Raised': final_df['PO Already Raised'].sum(),
        'PO to be raised': final_df['PO to be raised'].sum(),
        'Cost per Unit (in INR)': '',
        'PO Cost (in INR)': final_df['PO Cost (in INR)'].sum(),
    }

    final_df = pd.concat([final_df, pd.DataFrame([total_row])], ignore_index=True)

    output = BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        final_df.to_excel(writer, index=False, sheet_name='Purchase Order')
    output.seek(0)
    file_bytes = output.read()

    if len(country_files_found) == 1:
        file_suffix = country_files_found[0]
        file_type = f"{country_files_found[0].upper()} Purchase Order"
    else:
        file_suffix = 'global'
        file_type = 'Global Purchase Order'

    filename = f"purchase_order_{user_id}_{file_suffix}_{month_db}_{year_db}.xlsx"

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
            "country": file_suffix,
            "month": month_db,
            "year": year_db,
            "filename": filename,
            "data": file_bytes
        })

    return jsonify({
        'message': f'{file_type} generated successfully',
        'filename': filename,
        'records_count': len(final_df) - 1,
        'countries_processed': country_files_found,
        'file_type': file_type.lower().replace(' ', '_'),
        'source': 'stored_files'
    }), 200


@dashboard_bp.route('/getGlobalDispatchfile', methods=['GET'])
def get_global_dispatch_file():
    from io import BytesIO
    from sqlalchemy import create_engine, text
    from datetime import datetime

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
        month_name = datetime.strptime(month.strip().title(), "%B").strftime("%B")
        month_db = month_name.lower()
        year_db = str(int(year))
    except Exception:
        return jsonify({'error': 'Invalid month or year format'}), 400

    engine = create_engine(db_url)

    def fetch_latest_file(country_code: str):
        filename = f"purchase_order_{user_id}_{country_code}_{month_db}_{year_db}.xlsx"

        q = text("""
            SELECT filename, content_type, data
            FROM public.stored_files
            WHERE user_id = :user_id
              AND LOWER(country) = LOWER(:country)
              AND kind = 'purchase_order'
              AND LOWER(month) = LOWER(:month)
              AND year = :year
              AND LOWER(filename) = LOWER(:filename)
            ORDER BY id DESC
            LIMIT 1
        """)

        with engine.connect() as conn:
            row = conn.execute(q, {
                "user_id": user_id,
                "country": country_code,
                "month": month_db,
                "year": year_db,
                "filename": filename
            }).fetchone()

        if not row:
            return None

        filename, content_type, data_bytes = row[0], row[1], row[2]
        if isinstance(data_bytes, memoryview):
            data_bytes = data_bytes.tobytes()

        return filename, content_type, data_bytes

    global_row = fetch_latest_file('global')
    if global_row:
        filename, content_type, data_bytes = global_row
        return send_file(
            BytesIO(data_bytes),
            as_attachment=True,
            download_name=filename,
            mimetype=content_type or 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )

    uk_row = fetch_latest_file('uk')
    us_row = fetch_latest_file('us')

    if uk_row and not us_row:
        filename, content_type, data_bytes = uk_row
        return send_file(
            BytesIO(data_bytes),
            as_attachment=True,
            download_name=filename,
            mimetype=content_type or 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )

    if us_row and not uk_row:
        filename, content_type, data_bytes = us_row
        return send_file(
            BytesIO(data_bytes),
            as_attachment=True,
            download_name=filename,
            mimetype=content_type or 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )

    return jsonify({
        'error': 'No purchase order files found. Please generate country-specific files first.'
    }), 404



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

    if request.method == 'POST':
        data = request.get_json(silent=True) or {}
        country_param = data.get('country', '')
        currency_param = (data.get('currency') or '').lower()
        target_sales = data.get('target_sales')

        now = datetime.now(ZoneInfo("Asia/Kolkata"))
        month_name = now.strftime("%B")
        year = now.year
    else:
        month = request.args.get('month')
        year = request.args.get('year')
        country_param = request.args.get('country', '')
        currency_param = (request.args.get('currency') or '').lower()
        target_sales = request.args.get('target_sales')

        if not month:
            return jsonify({'error': 'Month is required'}), 400
        if not year:
            return jsonify({'error': 'Year is required'}), 400

        try:
            year = int(year)
        except ValueError:
            return jsonify({'error': 'Invalid year format'}), 400

        try:
            month_name = datetime.strptime(month.capitalize(), "%B").strftime("%B")
        except ValueError:
            return jsonify({'error': 'Invalid month format. Use full month name like January'}), 400

    country = resolve_country(country_param, currency_param)
    if not country:
        return jsonify({'error': 'country is required'}), 400

    country = str(country).strip().lower()

    db_session = SessionLocal()
    inspector = inspect(engine)

    def get_global_gbp_to_usd_rate(month_name: str, year: int) -> float:
        try:
            with conv_engine.connect() as conn:
                q = text("""
                    SELECT conversion_rate
                    FROM currency_conversion
                    WHERE lower(user_currency) = 'gbp'
                      AND lower(country) = 'us'
                      AND lower(selected_currency) = 'usd'
                      AND lower(month) = :month
                      AND year = :year
                    ORDER BY id DESC
                    LIMIT 1
                """)
                row = conn.execute(q, {
                    'month': month_name.lower(),
                    'year': int(year)
                }).fetchone()

                if row and row[0] is not None:
                    rate = float(row[0])
                    return rate

                return 1.0
        except Exception as e:
            print(f"❌ Error fetching global GBP -> USD rate: {str(e)}")
            return 1.0

    def find_existing_monthly_table(user_id: int, record_country: str, month_name: str, year: int):
        month_clean = month_name.lower()

        if record_country.lower() == "global":
            candidates = [
                f"skuwisemonthly_{user_id}_{record_country.lower()}_{month_clean}{year}_table",
                f"skuwisemonthly_{user_id}_{record_country.lower()}_{month_clean}_{year}_table",
            ]
        else:
            candidates = [
                f"skuwisemonthly_{user_id}_{record_country.lower()}_{month_clean}{year}",
                f"skuwisemonthly_{user_id}_{record_country.lower()}_{month_clean}_{year}",
                f"skuwisemonthly_{user_id}_{record_country.lower()}_{month_clean}{year}_table",
                f"skuwisemonthly_{user_id}_{record_country.lower()}_{month_clean}_{year}_table",
            ]

        for table_name in candidates:
            if inspector.has_table(table_name):
                return table_name

        return None

    def find_total_row(df):
        if 'product_name' in df.columns:
            for variation in ['TOTAL', 'Total', 'total', 'TOTALS', 'Totals', 'totals']:
                total_row = df[df['product_name'].astype(str).str.strip() == variation]
                if not total_row.empty:
                    return total_row.tail(1)

            contains_total = df[df['product_name'].astype(str).str.contains('total', case=False, na=False)]
            if not contains_total.empty:
                return contains_total.tail(1)

        if 'sku' in df.columns:
            exact_total = df[df['sku'].astype(str).str.strip().str.lower() == 'total']
            if not exact_total.empty:
                return exact_total.tail(1)

            contains_total = df[df['sku'].astype(str).str.contains('total', case=False, na=False)]
            if not contains_total.empty:
                return contains_total.tail(1)

        return None

    def get_countries_for_summary(user_id: int, month_name: str, year: int, country: str):
        """
        For non-global: return only that country.
        For global: return countries that actually have upload_history or tables, typically uk/us.
        """
        if country != 'global':
            return [country]

        countries = set()

        # 1) From upload_history for same month/year, collect actual country rows except global
        q = text("""
            SELECT DISTINCT LOWER(country) AS country
            FROM upload_history
            WHERE user_id = :user_id
              AND LOWER(month) = LOWER(:month)
              AND year = :year
              AND LOWER(country) IN ('uk', 'us')
        """)
        rows = db_session.execute(q, {
            'user_id': user_id,
            'month': month_name,
            'year': year
        }).fetchall()

        for row in rows:
            if row[0]:
                countries.add(str(row[0]).strip().lower())

        # 2) Fallback: infer from tables if upload_history missing
        for c in ['uk', 'us']:
            table_name = find_existing_monthly_table(user_id, c, month_name, year)
            if table_name:
                countries.add(c)

        # 3) Last fallback: if a real global table exists, use global
        if not countries:
            global_table = find_existing_monthly_table(user_id, 'global', month_name, year)
            if global_table:
                countries.add('global')

        return list(countries)

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

        all_cashflow_data = []
        countries_with_data = get_countries_for_summary(user_id, month_name, year, country)

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
                if upload_values_result[0] is not None:
                    total_otherwplatform += float(upload_values_result[0])
                if upload_values_result[1] is not None:
                    total_taxncredit_from_upload += float(upload_values_result[1])

            table_name = find_existing_monthly_table(user_id, record_country, month_name, year)

            if not table_name:
                continue

            try:
                cashflow_df = pd.read_sql(text(f'SELECT * FROM "{table_name}"'), engine)
                if cashflow_df.empty:
                    continue

                # Alternate mappings
                if 'cost_of_unit_sold' not in cashflow_df.columns and 'cogs' in cashflow_df.columns:
                    cashflow_df['cost_of_unit_sold'] = cashflow_df['cogs']

                if 'cm2_profit' not in cashflow_df.columns and 'profit' in cashflow_df.columns:
                    cashflow_df['cm2_profit'] = cashflow_df['profit']

                numeric_cols = [
                    'net_sales', 'gross_sales', 'advertising_total', 'amazon_fee',
                    'cm2_profit', 'cost_of_unit_sold', 'taxncredit', 'rembursement_fee',
                    'total_quantity', 'selling_fees', 'fba_fees', 'promotional_rebates'
                ]

                for col in numeric_cols:
                    if col in cashflow_df.columns:
                        cashflow_df[col] = pd.to_numeric(cashflow_df[col], errors='coerce').fillna(0)

                total_row = find_total_row(cashflow_df)

                if total_row is not None and not total_row.empty:
                    net_sales_total = float(total_row['net_sales'].iloc[0]) if 'net_sales' in total_row.columns else 0
                    cm2_profit_total = float(total_row['cm2_profit'].iloc[0]) if 'cm2_profit' in total_row.columns else 0
                    cost_of_unit_sold_total = float(total_row['cost_of_unit_sold'].iloc[0]) if 'cost_of_unit_sold' in total_row.columns else 0
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

            except Exception as e:
                print(f"❌ Error reading table {table_name}: {e}")
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

            raw_net_sales_total = float(summary_totals.get('net_sales', 0) or 0)
            raw_cashflow_total = float(summary_totals.get('cashflow', 0) or 0)

            if country == 'global':
                conversion_rate = get_global_gbp_to_usd_rate(month_name, year)

                target_sales = round(target_sales * conversion_rate, 2)
                net_sales_total = round(raw_net_sales_total * conversion_rate, 2)
                cashflow_total = round(raw_cashflow_total * conversion_rate, 2)
            else:
                net_sales_total = round(raw_net_sales_total, 2)
                cashflow_total = round(raw_cashflow_total, 2)

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

            saved_row_sql = text(f"""
                SELECT id, created_at, updated_at
                FROM {target_table_name}
                WHERE LOWER(month) = LOWER(:month)
                  AND year = :year
                  AND LOWER(country) = LOWER(:country)
                LIMIT 1
            """)
            saved_row = db_session.execute(saved_row_sql, {
                'month': month_name,
                'year': year,
                'country': country
            }).fetchone()

            return jsonify({
                'message': 'Target summary saved successfully',
                'data': {
                    'id': saved_row[0] if saved_row else None,
                    'user_id': user_id,
                    'month': month_name,
                    'year': year,
                    'country': country,
                    'target_sales': round(target_sales, 2),
                    'cashflow_total': cashflow_total,
                    'net_sales_total': net_sales_total,
                    'shortfall_total': shortfall_total,
                    'created_at': saved_row[1].isoformat() if saved_row and saved_row[1] else None,
                    'updated_at': saved_row[2].isoformat() if saved_row and saved_row[2] else None,
                    'table_name': target_table_name,
                    'source_details': details
                }
            }), 200

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

        summary_totals, details = compute_monthly_cashflow_summary(
            user_id=user_id,
            year=year,
            country=country,
            month_name=month_name
        )

        raw_fresh_net_sales_total = float(summary_totals.get('net_sales', 0) or 0)
        raw_fresh_cashflow_total = float(summary_totals.get('cashflow', 0) or 0)

        if country == 'global':
            conversion_rate = get_global_gbp_to_usd_rate(month_name, year)
            fresh_net_sales_total = round(raw_fresh_net_sales_total * conversion_rate, 2)
            fresh_cashflow_total = round(raw_fresh_cashflow_total * conversion_rate, 2)
        else:
            fresh_net_sales_total = round(raw_fresh_net_sales_total, 2)
            fresh_cashflow_total = round(raw_fresh_cashflow_total, 2)

        if not row:
            return jsonify({
                'message': 'No saved target summary found, returning computed totals only',
                'data': {
                    'id': None,
                    'month': month_name,
                    'year': year,
                    'country': country,
                    'target_sales': None,
                    'cashflow_total': fresh_cashflow_total,
                    'net_sales_total': fresh_net_sales_total,
                    'shortfall_total': None,
                    'created_at': None,
                    'updated_at': None,
                    'table_name': target_table_name,
                    'source_details': details,
                    'is_saved': False
                }
            }), 200

        target_sales_value = float(row[4])
        fresh_shortfall_total = round(target_sales_value - fresh_net_sales_total, 2)

        return jsonify({
            'message': 'Target summary fetched successfully',
            'data': {
                'id': row[0],
                'month': row[1],
                'year': row[2],
                'country': row[3],
                'target_sales': round(target_sales_value, 2),
                'cashflow_total': fresh_cashflow_total,
                'net_sales_total': fresh_net_sales_total,
                'shortfall_total': fresh_shortfall_total,
                'created_at': row[8].isoformat() if row[8] else None,
                'updated_at': row[9].isoformat() if row[9] else None,
                'table_name': target_table_name,
                'source_details': details,
                'is_saved': True
            }
        }), 200

    except Exception as e:
        db_session.rollback()
        return jsonify({'error': f'Database error: {str(e)}'}), 500

    finally:
        db_session.close()


