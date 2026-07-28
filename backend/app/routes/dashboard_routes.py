from flask import Blueprint, request, jsonify , send_file
from sqlalchemy import create_engine, MetaData, text, inspect
from sqlalchemy.orm import sessionmaker, scoped_session
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

SessionLocal = scoped_session(sessionmaker(bind=engine))

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
        ship_time_weeks = int(profile.ship_time_weeks or 0)
        air_time_weeks = int(profile.air_time_weeks or 0)
        stock_unit_weeks = int(profile.stock_unit_weeks or 0)

        return jsonify({
            'exists': True,
            'ship_time_weeks': ship_time_weeks,
            'air_time_weeks': air_time_weeks,
            'stock_unit_weeks': stock_unit_weeks,
            'sea_alert_threshold_weeks': (
                ship_time_weeks + stock_unit_weeks
            ),
            'air_alert_threshold_weeks': (
                air_time_weeks + stock_unit_weeks
            )
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

            # ✅ ADD HERE
            if 'sku' in combined_df.columns:
                sku_df = (
                    combined_df.groupby('Product Name')['sku']
                    .apply(lambda x: ', '.join(sorted(set(str(v).strip() for v in x if str(v).strip()))))
                    .reset_index()
                )
            else:
                sku_df = None

            group_keys = ['Product Name']

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

            # ✅ ADD HERE
            if sku_df is not None:
                final_df = pd.merge(final_df, sku_df, on='Product Name', how='left')

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
        year_db = str(int(year))
        country_db = country.strip().lower()
    except Exception:
        return jsonify({'error': 'Invalid month/year'}), 400

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
        'in_transit_units': 'In Transit Units'
    }, inplace=True)

    merged_df['Product Name'] = merged_df['Product Name'].replace('', pd.NA)
    merged_df['Product Name'] = merged_df['Product Name'].fillna(merged_df.get('Product Name DB', ''))
    merged_df['Product Name'] = merged_df['Product Name'].fillna('').astype(str).str.strip()

    for col in [
        'Dispatch',
        'Current Inventory - Local Warehouse',
        'In Transit Units',
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
        - merged_df['In Transit Units']
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
        'In Transit Units',
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
        'In Transit Units': totals.get('In Transit Units', 0),
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
        ON CONFLICT ON CONSTRAINT uq_stored_files_period
        DO UPDATE SET
            filename = EXCLUDED.filename,
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
        month_name = datetime.strptime(month.strip().title(), "%B").strftime("%B")
        month_db = month_name.lower()
        year_db = str(int(year))
    except Exception:
        return jsonify({'error': 'Invalid month or year format'}), 400

    def fetch_latest_po(country_code: str):
        q = text("""
            SELECT filename, content_type, data
            FROM public.stored_files
            WHERE user_id = :user_id
              AND LOWER(country) = LOWER(:country)
              AND kind = 'purchase_order'
              AND LOWER(month) = LOWER(:month)
              AND year = :year
            ORDER BY id DESC
            LIMIT 1
        """)

        with engine.connect() as conn:
            row = conn.execute(q, {
                "user_id": user_id,
                "country": country_code,
                "month": month_db,
                "year": year_db
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
        'In Transit Units',
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
    if 'In Transit Units' not in global_df.columns:
        global_df['In Transit Units'] = 0
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
        global_df['In Transit Units'].fillna(0)
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
        'In Transit Units',
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
    final_df['In Transit Units'] = grouped_df.get('In Transit Units', 0)
    final_df['PO to be raised'] = grouped_df.get('PO to be raised', 0)
    final_df['Cost per Unit (in INR)'] = grouped_df.get('Cost per Unit (in INR)', 0)
    final_df['PO Cost (in INR)'] = grouped_df.get('PO Cost (in INR)', 0)

    total_row = {
        'Sno.': '',
        'Product Name': 'Total',
        'Dispatches UK': final_df['Dispatches UK'].sum(),
        'Dispatches Canada': final_df['Dispatches Canada'].sum(),
        'Dispatches Amazon US': final_df['Dispatches Amazon US'].sum(),
        'Total Dispatches': final_df['Total Dispatches'].sum(),
        'Current Inventory - Local Warehouse': final_df['Current Inventory - Local Warehouse'].sum(),
        'In Transit Units': final_df['In Transit Units'].sum(),
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

    filename = f"purchase_order_{user_id}_global_{month_db}_{year_db}.xlsx"

    upsert_query = text("""
        INSERT INTO public.stored_files
        (user_id, country, kind, month, year, filename, content_type, data, created_at)
        VALUES (
            :user_id,
            'global',
            'purchase_order',
            :month,
            :year,
            :filename,
            'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            :data,
            NOW()
        )
        ON CONFLICT ON CONSTRAINT uq_stored_files_period
        DO UPDATE SET
            filename = EXCLUDED.filename,
            content_type = EXCLUDED.content_type,
            data = EXCLUDED.data,
            created_at = NOW()
    """)

    with engine.begin() as conn:
        conn.execute(upsert_query, {
            "user_id": user_id,
            "month": month_db,
            "year": year_db,
            "filename": filename,
            "data": file_bytes
        })

    return jsonify({
        'message': 'Global Purchase Order generated successfully',
        'filename': filename,
        'records_count': len(final_df) - 1,
        'countries_processed': country_files_found,
        'file_type': 'global_purchase_order',
        'source': 'stored_files'
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

    try:
        month_name = datetime.strptime(month.strip().title(), "%B").strftime("%B")
        month_db = month_name.lower()
        year_db = str(int(year))
    except Exception:
        return jsonify({'error': 'Invalid month or year format'}), 400

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
        year = request.args.get('year')

        if not country or not month or not year:
            return jsonify({'error': 'Missing country, month, or year parameters'}), 400

        try:
            month_name = datetime.strptime(month.strip().title(), "%B").strftime("%B")
            month_db = month_name.lower()
            year_db = str(int(year))
            country_db = country.strip().lower()
        except Exception:
            return jsonify({'error': 'Invalid month or year format'}), 400

        query = text("""
            SELECT filename, content_type, data
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
            row = conn.execute(query, {
                "user_id": user_id,
                "country": country_db,
                "month": month_db,
                "year": year_db
            }).fetchone()

        if not row:
            return jsonify({
                'error': 'Forecast file not found. Please generate inventory forecast first!'
            }), 404

        filename, content_type, data_bytes = row

        if isinstance(data_bytes, memoryview):
            data_bytes = data_bytes.tobytes()

        if not data_bytes:
            return jsonify({'error': 'Stored file is empty/corrupt'}), 500

        return send_file(
            BytesIO(data_bytes),
            download_name=filename or f"inventory_forecast_{user_id}_{country_db}_{month_db}_{year_db}.xlsx",
            mimetype=content_type or 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=False
        )

    except jwt.ExpiredSignatureError:
        return jsonify({'error': 'Token has expired'}), 401
    except jwt.InvalidTokenError:
        return jsonify({'error': 'Invalid token'}), 401
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    
       
def resolve_country(country, currency):
    country = (country or "").strip().lower()
    currency = (currency or "").strip().lower()

    if country == "global":
        if currency == "inr":
            return "global_inr"
        if currency == "gbp":
            return "global_gbp"
        if currency == "cad":
            return "global_cad"
        return "global"

    if country == "uk":
        if currency == "usd":
            return "uk_usd"
        return "uk"

    return country


MONTHS = [
    "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December"
]

QUARTER_MONTHS = {
    "quarter1": ["january", "february", "march"],
    "quarter2": ["april", "may", "june"],
    "quarter3": ["july", "august", "september"],
    "quarter4": ["october", "november", "december"]
}


def table_exists(db_session, table_name: str, schema: str = "public") -> bool:
    q = text("""
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = :schema
              AND table_name = :table_name
        )
    """)
    return bool(db_session.execute(q, {
        "schema": schema,
        "table_name": table_name
    }).scalar())

def get_gbp_to_usd_rate(month_value, year_value):
    try:
        with conv_engine.connect() as admin_conn:
            rate = admin_conn.execute(
                text("""
                    SELECT conversion_rate
                    FROM currency_conversion
                    WHERE LOWER(user_currency) = 'gbp'
                      AND LOWER(country) = 'us'
                      AND LOWER(selected_currency) = 'usd'
                      AND LOWER(month) = :month
                      AND year = :year
                    ORDER BY id DESC
                    LIMIT 1
                """),
                {
                    "month": str(month_value).lower().strip(),
                    "year": int(year_value),
                }
            ).scalar()

        return float(rate or 1)

    except Exception as e:
        return 1
    


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

    currency_param = (
        request.args.get('currency')
        or request.args.get('homeCurrency')
        or ''
    ).lower().strip()

    if (country_param or '').lower().strip() == 'global' and not currency_param:
        currency_param = 'usd'

    country = resolve_country(country_param, currency_param)
    period_type = (request.args.get('period_type') or 'monthly').lower().strip()

    if not year:
        return jsonify({'error': 'Year must be provided'}), 400

    try:
        year = int(year)
    except ValueError:
        return jsonify({'error': 'Invalid year format. Provide year like 2026'}), 400

    def quarter_for_month(month_str: str):
        m = (month_str or '').lower()
        for q, months in QUARTER_MONTHS.items():
            if m in months:
                return q
        return None

    def prev_month_year(month_str: str, y: int):
        idx = MONTHS.index(month_str)
        if idx == 0:
            return "December", y - 1
        return MONTHS[idx - 1], y

    def prev_quarter(q: str):
        order = ["quarter1", "quarter2", "quarter3", "quarter4"]
        i = order.index(q)
        if i == 0:
            return "quarter4", -1
        return order[i - 1], 0

    if period_type == 'monthly' and not month:
        return jsonify({'error': 'Month must be provided for monthly period type'}), 400

    if period_type == 'quarterly' and not month:
        return jsonify({'error': 'Month must be provided for quarterly period type'}), 400

    month_name = None
    if month:
        try:
            month_name = datetime.strptime(month.strip().capitalize(), "%B").strftime("%B")
        except ValueError:
            return jsonify({
                'error': 'Invalid month format. Use full month names like January or january'
            }), 400

    db_session = SessionLocal()

    def empty_totals():
        return {
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

    def find_total_row(df: pd.DataFrame):
        if 'product_name' in df.columns:
            product_series = df['product_name'].astype(str).str.strip()
            exact_total = df[product_series.str.lower().isin(['total', 'totals'])]
            if not exact_total.empty:
                return exact_total.tail(1)

            contains_total = df[product_series.str.contains('total', case=False, na=False)]
            if not contains_total.empty:
                return contains_total.tail(1)

        if 'sku' in df.columns:
            sku_series = df['sku'].astype(str).str.strip()
            exact_total = df[sku_series.str.lower().isin(['total', 'totals'])]
            if not exact_total.empty:
                return exact_total.tail(1)

        return None

    def get_months_to_process(period_type_value: str, month_name_value: str = None):
        if period_type_value == 'monthly':
            return [month_name_value]

        if period_type_value == 'quarterly':
            q = quarter_for_month(month_name_value)
            if not q:
                return []
            return [m.capitalize() for m in QUARTER_MONTHS[q]]

        if period_type_value in QUARTER_MONTHS:
            return [m.capitalize() for m in QUARTER_MONTHS[period_type_value]]

        if period_type_value == 'yearly':
            return MONTHS.copy()

        return []

    def find_monthly_table(user_id_value, record_country, year_value, month_name_value):
        record_country = (record_country or '').lower().strip()
        month_clean = month_name_value.lower()

        candidates = [
            f"skuwisemonthly_{user_id_value}_{record_country}_{month_clean}{year_value}_table",
            # f"skuwisemonthly_{user_id_value}_{record_country}_{month_clean}_{year_value}",
            f"skuwisemonthly_{user_id_value}_{record_country}_{month_clean}{year_value}",
            # f"skuwisemonthly_{user_id_value}_{record_country}_{month_clean}_{year_value}",
        ]

        for table_name in candidates:
            if table_exists(db_session, table_name):
                return table_name

        return None

    def get_countries_with_data(user_id_value, year_value, country_value, months_to_process):
        country_value = (country_value or '').lower().strip()

        if country_value == 'global':
            countries = set()

            for process_month in months_to_process:
                q = text("""
                    SELECT DISTINCT LOWER(country)
                    FROM upload_history
                    WHERE user_id = :user_id
                      AND LOWER(month) = LOWER(:month)
                      AND year = :year
                      AND LOWER(country) IN ('uk', 'us')
                """)
                rows = db_session.execute(q, {
                    'user_id': user_id_value,
                    'month': process_month,
                    'year': year_value
                }).fetchall()

                for row in rows:
                    if row[0]:
                        countries.add(str(row[0]).strip().lower())

                for c in ['uk', 'us']:
                    if find_monthly_table(user_id_value, c, year_value, process_month):
                        countries.add(c)

            return countries

        return {country_value}

    def load_table_dataframe(table_name: str):
        with engine.connect() as conn:
            return pd.read_sql(text(f'SELECT * FROM "{table_name}"'), conn)

   

    def extract_totals(cashflow_df: pd.DataFrame):
        if cashflow_df.empty:
            return None, []

        column_map = {
            'net_sales': 'net_sales',
            'gross_sales': 'gross_sales',
            'advertising_total': 'advertising_total',
            'amazon_fee': 'amazon_fee',
            'cm2_profit': 'cm2_profit',
            'cost_of_unit_sold': 'cost_of_unit_sold',
            'otherwplatform': 'platform_fee',
            'taxncredit': 'tex_and_credits',
            'rembursement_fee': 'rembursement_fee',
            'quantity_total': 'total_quantity',
            'selling_fees': 'selling_fees',
            'fba_fees': 'fba_fees',
            'promotional_rebates': 'promotional_rebates'
        }

        # Optional backward compatibility
        if 'cost_of_unit_sold' not in cashflow_df.columns and 'cogs' in cashflow_df.columns:
            cashflow_df['cost_of_unit_sold'] = cashflow_df['cogs']

        if 'cm2_profit' not in cashflow_df.columns and 'profit' in cashflow_df.columns:
            cashflow_df['cm2_profit'] = cashflow_df['profit']

        # Convert mapped table columns to numeric
        for response_key, table_col in column_map.items():
            if table_col in cashflow_df.columns:
                cashflow_df[table_col] = pd.to_numeric(
                    cashflow_df[table_col],
                    errors='coerce'
                ).fillna(0)

        total_row = find_total_row(cashflow_df)
        source_df = total_row if total_row is not None and not total_row.empty else cashflow_df

        def get_total(response_key):
            table_col = column_map.get(response_key)

            if not table_col or table_col not in source_df.columns:
                return 0

            if total_row is not None and not total_row.empty:
                return float(source_df[table_col].iloc[0] or 0)

            return float(source_df[table_col].sum() or 0)

        amazon_fee_value = get_total('amazon_fee')

        totals = {
            'net_sales': get_total('net_sales'),
            'gross_sales': get_total('gross_sales'),
            'promotional_rebates': get_total('promotional_rebates'),
            'quantity_total': get_total('quantity_total'),
            'advertising_total': get_total('advertising_total'),
            'selling_fees': get_total('selling_fees'),
            'fba_fees': get_total('fba_fees'),

            # amazon_fee is stored positive in SKU monthly table,
            # but it should behave like a fee/deduction in the response.
            'amazon_fee': -abs(amazon_fee_value),

            'cm2_profit': get_total('cm2_profit'),
            'cost_of_unit_sold': get_total('cost_of_unit_sold'),
            'rembursement_fee': get_total('rembursement_fee'),
            'taxncredit': get_total('taxncredit'),

            # platform_fee already has correct original sign in table.
            # Do not force it negative.
            'otherwplatform': get_total('otherwplatform'),

            'cashflow': 0,
        }

        totals['cashflow'] = totals['cost_of_unit_sold'] + totals['cm2_profit']

        if 'date' in cashflow_df.columns:
            cashflow_df = cashflow_df.drop(columns=['date'])

        numeric_columns = cashflow_df.select_dtypes(include=['number']).columns
        for col in numeric_columns:
            cashflow_df[col] = cashflow_df[col].astype(float).round(2)

        data_records = []
        for record in cashflow_df.to_dict(orient='records'):
            clean_record = {}
            for key, value in record.items():
                if pd.isna(value) or value is None:
                    clean_record[key] = 0
                elif isinstance(value, str):
                    clean_record[key] = value
                else:
                    try:
                        clean_record[key] = float(value)
                    except Exception:
                        clean_record[key] = str(value)

            data_records.append(clean_record)

        return totals, data_records

    def convert_uk_to_usd_if_needed(totals, record_country, process_month, year_value, country_value):
        if country_value == "global" and currency_param == "usd" and record_country == "uk":
            rate = get_gbp_to_usd_rate(process_month, year_value)

            money_keys = [
                'net_sales',
                'gross_sales',
                'promotional_rebates',
                'advertising_total',
                'selling_fees',
                'fba_fees',
                'amazon_fee',
                'cm2_profit',
                'cost_of_unit_sold',
                'rembursement_fee',
                'taxncredit',
                'otherwplatform'
            ]

            for key in money_keys:
                totals[key] *= rate

            totals['cashflow'] = totals['cost_of_unit_sold'] + totals['cm2_profit']

        return totals

    def add_totals(target, source):
        for key in target:
            target[key] += source.get(key, 0)

    def compute_cashflow_summary(
        user_id_value,
        year_value,
        country_value,
        period_type_value,
        month_name_value=None,
        custom_months_to_process=None
    ):
        combined_totals = empty_totals()
        all_cashflow_data = []
        processed_months = []

        months_to_process = (
            custom_months_to_process
            if custom_months_to_process is not None
            else get_months_to_process(period_type_value, month_name_value)
        )

        if not months_to_process:
            return combined_totals, [], {'processed_months': []}

        countries_with_data = get_countries_with_data(
            user_id_value=user_id_value,
            year_value=year_value,
            country_value=country_value,
            months_to_process=months_to_process
        )

        for record_country in countries_with_data:
            country_totals = empty_totals()
            country_monthly_details = []
            country_data_records = []

            for process_month in months_to_process:
                table_name = find_monthly_table(
                    user_id_value=user_id_value,
                    record_country=record_country,
                    year_value=year_value,
                    month_name_value=process_month
                )

                if not table_name:
                    continue

                if process_month not in processed_months:
                    processed_months.append(process_month)

                try:
                    cashflow_df = load_table_dataframe(table_name)
                    totals, data_records = extract_totals(cashflow_df)

                    if totals is None:
                        continue

                    totals = convert_uk_to_usd_if_needed(
                        totals=totals,
                        record_country=record_country,
                        process_month=process_month,
                        year_value=year_value,
                        country_value=country_value
                    )

                    add_totals(combined_totals, totals)
                    add_totals(country_totals, totals)

                    country_monthly_details.append({
                        'month': process_month,
                        'table': table_name,
                        **{k: round(v, 2) for k, v in totals.items()}
                    })

                    if period_type_value == 'monthly':
                        country_data_records = data_records

                except Exception as e:
                    continue

            if any(float(v or 0) != 0 for v in country_totals.values()):
                all_cashflow_data.append({
                    'country': record_country,
                    'period_type': period_type_value,
                    'month': month_name_value if period_type_value == 'monthly' else None,
                    **{k: round(v, 2) for k, v in country_totals.items()},
                    'monthly_details': country_monthly_details,
                    'data': country_data_records
                })

        for key in combined_totals:
            combined_totals[key] = round(combined_totals[key], 2)

        meta = {
            'processed_months': processed_months
        }

        if period_type_value == 'monthly':
            meta['month'] = month_name_value

        elif period_type_value == 'quarterly' or period_type_value in QUARTER_MONTHS:
            meta['quarter_months'] = processed_months

        elif period_type_value == 'yearly':
            meta['year_months'] = processed_months

        return combined_totals, all_cashflow_data, meta

    try:
        combined_totals, all_cashflow_data, meta = compute_cashflow_summary(
            user_id_value=user_id,
            year_value=year,
            country_value=country,
            period_type_value=period_type,
            month_name_value=month_name
        )

        if not all_cashflow_data:
            return jsonify({
                'error': 'No data found for the specified parameters',
                'searched_for': {
                    'user_id': user_id,
                    'year': year,
                    'country': country,
                    'original_country': country_param,
                    'currency': currency_param,
                    'period_type': period_type,
                    'month': month_name
                }
            }), 404

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

        elif period_type in QUARTER_MONTHS:
            prev_q, y_delta = prev_quarter(period_type)
            prev_period_type = prev_q
            prev_year = year + y_delta
            prev_month_name = None

        elif period_type == 'yearly':
            prev_year = year - 1
            prev_month_name = None

        previous_summary = None
        try:
            previous_custom_months = None

            if period_type == 'yearly':
                previous_custom_months = meta.get('processed_months', [])

            prev_totals, _, _ = compute_cashflow_summary(
                user_id_value=user_id,
                year_value=prev_year,
                country_value=country,
                period_type_value=prev_period_type,
                month_name_value=prev_month_name,
                custom_months_to_process=previous_custom_months
            )

            previous_summary = prev_totals

        except Exception as e:
            previous_summary = None

        response_data = {
            'period_type': period_type,
            'year': year,
            'country': country,
            'summary': combined_totals,
            'previous_summary': previous_summary,
            'detailed_data': all_cashflow_data,
            'total_records': len(all_cashflow_data),
        }

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
        currency_param = (data.get('currency') or '').lower().strip()
        target_sales = data.get('target_sales')

        now = datetime.now(ZoneInfo("Asia/Kolkata"))
        month_name = now.strftime("%B")
        year = now.year

    else:
        month = request.args.get('month')
        year = request.args.get('year')
        country_param = request.args.get('country', '')
        currency_param = (request.args.get('currency') or '').lower().strip()
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
            month_name = datetime.strptime(month.strip().capitalize(), "%B").strftime("%B")
        except ValueError:
            return jsonify({'error': 'Invalid month format. Use full month name like January'}), 400

    country = resolve_country(country_param, currency_param)

    if not country:
        return jsonify({'error': 'country is required'}), 400

    country = str(country).strip().lower()

    db_session = SessionLocal()
    inspector = inspect(engine)

    def get_global_gbp_to_usd_rate(month_name_value: str, year_value: int) -> float:
        try:
            with conv_engine.connect() as conn:
                q = text("""
                    SELECT conversion_rate
                    FROM currency_conversion
                    WHERE LOWER(user_currency) = 'gbp'
                      AND LOWER(country) = 'us'
                      AND LOWER(selected_currency) = 'usd'
                      AND LOWER(month) = :month
                      AND year = :year
                    ORDER BY id DESC
                    LIMIT 1
                """)

                row = conn.execute(q, {
                    'month': month_name_value.lower().strip(),
                    'year': int(year_value)
                }).fetchone()

                if row and row[0] is not None:
                    return float(row[0])

                return 1.0

        except Exception as e:
            return 1.0

    def find_existing_monthly_table(user_id_value: int, record_country: str, month_name_value: str, year_value: int):
        month_clean = month_name_value.lower().strip()
        record_country = record_country.lower().strip()

        if record_country == "global":
            candidates = [
                f"skuwisemonthly_{user_id_value}_{record_country}_{month_clean}{year_value}_table",
                f"skuwisemonthly_{user_id_value}_{record_country}_{month_clean}_{year_value}_table",
            ]
        else:
            candidates = [
                f"skuwisemonthly_{user_id_value}_{record_country}_{month_clean}{year_value}",
                f"skuwisemonthly_{user_id_value}_{record_country}_{month_clean}_{year_value}",
                f"skuwisemonthly_{user_id_value}_{record_country}_{month_clean}{year_value}_table",
                f"skuwisemonthly_{user_id_value}_{record_country}_{month_clean}_{year_value}_table",
            ]

        for table_name in candidates:
            if inspector.has_table(table_name):
                return table_name

        return None

    def find_total_row(df):
        if 'product_name' in df.columns:
            product_series = df['product_name'].astype(str).str.strip()

            exact_total = df[product_series.str.lower().isin(['total', 'totals'])]
            if not exact_total.empty:
                return exact_total.tail(1)

            contains_total = df[product_series.str.contains('total', case=False, na=False)]
            if not contains_total.empty:
                return contains_total.tail(1)

        if 'sku' in df.columns:
            sku_series = df['sku'].astype(str).str.strip()

            exact_total = df[sku_series.str.lower().isin(['total', 'totals'])]
            if not exact_total.empty:
                return exact_total.tail(1)

            contains_total = df[sku_series.str.contains('total', case=False, na=False)]
            if not contains_total.empty:
                return contains_total.tail(1)

        return None

    def get_countries_for_summary(user_id_value: int, month_name_value: str, year_value: int, country_value: str):
        country_value = country_value.lower().strip()

        if country_value != 'global':
            return [country_value]

        countries = set()

        q = text("""
            SELECT DISTINCT LOWER(country) AS country
            FROM upload_history
            WHERE user_id = :user_id
              AND LOWER(month) = LOWER(:month)
              AND year = :year
              AND LOWER(country) IN ('uk', 'us')
        """)

        rows = db_session.execute(q, {
            'user_id': user_id_value,
            'month': month_name_value,
            'year': year_value
        }).fetchall()

        for row in rows:
            if row[0]:
                countries.add(str(row[0]).strip().lower())

        for c in ['uk', 'us']:
            table_name = find_existing_monthly_table(
                user_id_value=user_id_value,
                record_country=c,
                month_name_value=month_name_value,
                year_value=year_value
            )

            if table_name:
                countries.add(c)

        if not countries:
            global_table = find_existing_monthly_table(
                user_id_value=user_id_value,
                record_country='global',
                month_name_value=month_name_value,
                year_value=year_value
            )

            if global_table:
                countries.add('global')

        return list(countries)

    def compute_monthly_cashflow_summary(user_id_value: int, year_value: int, country_value: str, month_name_value: str):
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

        countries_with_data = get_countries_for_summary(
            user_id_value=user_id_value,
            month_name_value=month_name_value,
            year_value=year_value,
            country_value=country_value
        )

        gbp_to_usd_rate = get_global_gbp_to_usd_rate(month_name_value, year_value)

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
                'user_id': user_id_value,
                'month': month_name_value,
                'year': year_value,
                'country': record_country
            }).fetchone()

            if upload_values_result:
                if upload_values_result[0] is not None:
                    total_otherwplatform += float(upload_values_result[0])

                if upload_values_result[1] is not None:
                    total_taxncredit_from_upload += float(upload_values_result[1])

            table_name = find_existing_monthly_table(
                user_id_value=user_id_value,
                record_country=record_country,
                month_name_value=month_name_value,
                year_value=year_value
            )

            if not table_name:
                continue

            try:
                cashflow_df = pd.read_sql(text(f'SELECT * FROM "{table_name}"'), engine)

                if cashflow_df.empty:
                    continue

                if 'cost_of_unit_sold' not in cashflow_df.columns and 'cogs' in cashflow_df.columns:
                    cashflow_df['cost_of_unit_sold'] = cashflow_df['cogs']

                if 'cm2_profit' not in cashflow_df.columns and 'profit' in cashflow_df.columns:
                    cashflow_df['cm2_profit'] = cashflow_df['profit']

                numeric_cols = [
                    'net_sales',
                    'gross_sales',
                    'advertising_total',
                    'amazon_fee',
                    'cm2_profit',
                    'cost_of_unit_sold',
                    'taxncredit',
                    'rembursement_fee',
                    'total_quantity',
                    'selling_fees',
                    'fba_fees',
                    'promotional_rebates'
                ]

                for col in numeric_cols:
                    if col in cashflow_df.columns:
                        cashflow_df[col] = pd.to_numeric(cashflow_df[col], errors='coerce').fillna(0)

                total_row = find_total_row(cashflow_df)

                if total_row is not None and not total_row.empty:
                    net_sales_total = float(total_row['net_sales'].iloc[0]) if 'net_sales' in total_row.columns else 0
                    gross_sales_total = float(total_row['gross_sales'].iloc[0]) if 'gross_sales' in total_row.columns else 0
                    advertising_total = float(total_row['advertising_total'].iloc[0]) if 'advertising_total' in total_row.columns else 0
                    amazon_fee_total = float(total_row['amazon_fee'].iloc[0]) if 'amazon_fee' in total_row.columns else 0
                    cm2_profit_total = float(total_row['cm2_profit'].iloc[0]) if 'cm2_profit' in total_row.columns else 0
                    cost_of_unit_sold_total = float(total_row['cost_of_unit_sold'].iloc[0]) if 'cost_of_unit_sold' in total_row.columns else 0
                    rembursement_fee_total = float(total_row['rembursement_fee'].iloc[0]) if 'rembursement_fee' in total_row.columns else 0
                    quantity_total = float(total_row['total_quantity'].iloc[0]) if 'total_quantity' in total_row.columns else 0
                    selling_fees_total = float(total_row['selling_fees'].iloc[0]) if 'selling_fees' in total_row.columns else 0
                    fba_fees_total = float(total_row['fba_fees'].iloc[0]) if 'fba_fees' in total_row.columns else 0
                    promotional_rebates_total = float(total_row['promotional_rebates'].iloc[0]) if 'promotional_rebates' in total_row.columns else 0
                else:
                    net_sales_total = float(cashflow_df['net_sales'].sum()) if 'net_sales' in cashflow_df.columns else 0
                    gross_sales_total = float(cashflow_df['gross_sales'].sum()) if 'gross_sales' in cashflow_df.columns else 0
                    advertising_total = float(cashflow_df['advertising_total'].sum()) if 'advertising_total' in cashflow_df.columns else 0
                    amazon_fee_total = float(cashflow_df['amazon_fee'].sum()) if 'amazon_fee' in cashflow_df.columns else 0
                    cm2_profit_total = float(cashflow_df['cm2_profit'].sum()) if 'cm2_profit' in cashflow_df.columns else 0
                    cost_of_unit_sold_total = float(cashflow_df['cost_of_unit_sold'].sum()) if 'cost_of_unit_sold' in cashflow_df.columns else 0
                    rembursement_fee_total = float(cashflow_df['rembursement_fee'].sum()) if 'rembursement_fee' in cashflow_df.columns else 0
                    quantity_total = float(cashflow_df['total_quantity'].sum()) if 'total_quantity' in cashflow_df.columns else 0
                    selling_fees_total = float(cashflow_df['selling_fees'].sum()) if 'selling_fees' in cashflow_df.columns else 0
                    fba_fees_total = float(cashflow_df['fba_fees'].sum()) if 'fba_fees' in cashflow_df.columns else 0
                    promotional_rebates_total = float(cashflow_df['promotional_rebates'].sum()) if 'promotional_rebates' in cashflow_df.columns else 0

                # For global USD, UK values should be converted GBP -> USD.
                if country_value == 'global' and record_country == 'uk':
                    net_sales_total *= gbp_to_usd_rate
                    gross_sales_total *= gbp_to_usd_rate
                    advertising_total *= gbp_to_usd_rate
                    amazon_fee_total *= gbp_to_usd_rate
                    cm2_profit_total *= gbp_to_usd_rate
                    cost_of_unit_sold_total *= gbp_to_usd_rate
                    rembursement_fee_total *= gbp_to_usd_rate
                    selling_fees_total *= gbp_to_usd_rate
                    fba_fees_total *= gbp_to_usd_rate
                    promotional_rebates_total *= gbp_to_usd_rate
                    total_otherwplatform *= gbp_to_usd_rate
                    total_taxncredit_from_upload *= gbp_to_usd_rate

                cashflow_total = cost_of_unit_sold_total + cm2_profit_total

                combined_totals['net_sales'] += net_sales_total
                combined_totals['gross_sales'] += gross_sales_total
                combined_totals['advertising_total'] += advertising_total
                combined_totals['amazon_fee'] += amazon_fee_total
                combined_totals['cm2_profit'] += cm2_profit_total
                combined_totals['cost_of_unit_sold'] += cost_of_unit_sold_total
                combined_totals['cashflow'] += cashflow_total
                combined_totals['rembursement_fee'] += rembursement_fee_total
                combined_totals['quantity_total'] += quantity_total
                combined_totals['selling_fees'] += selling_fees_total
                combined_totals['fba_fees'] += fba_fees_total
                combined_totals['promotional_rebates'] += promotional_rebates_total
                combined_totals['taxncredit'] += total_taxncredit_from_upload
                combined_totals['otherwplatform'] += total_otherwplatform

                all_cashflow_data.append({
                    'country': record_country,
                    'table_name': table_name,
                    'net_sales_total': round(net_sales_total, 2),
                    'cashflow_total': round(cashflow_total, 2)
                })

            except Exception as e:
                continue

        for key in combined_totals:
            combined_totals[key] = round(combined_totals[key], 2)

        return combined_totals, all_cashflow_data

    def upsert_target_row(row_country, row_target_sales, row_cashflow_total, row_net_sales_total):
        row_country = str(row_country).lower().strip()
        row_target_sales = round(float(row_target_sales or 0), 2)
        row_cashflow_total = round(float(row_cashflow_total or 0), 2)
        row_net_sales_total = round(float(row_net_sales_total or 0), 2)
        row_shortfall_total = round(row_target_sales - row_net_sales_total, 2)

        upsert_sql = text(f"""
            INSERT INTO {target_table_name}
                (
                    user_id,
                    month,
                    year,
                    country,
                    target_sales,
                    cashflow_total,
                    net_sales_total,
                    shortfall_total,
                    updated_at
                )
            VALUES
                (
                    :user_id,
                    :month,
                    :year,
                    :country,
                    :target_sales,
                    :cashflow_total,
                    :net_sales_total,
                    :shortfall_total,
                    CURRENT_TIMESTAMP
                )
            ON CONFLICT (user_id, month, year, country)
            DO UPDATE SET
                target_sales = EXCLUDED.target_sales,
                cashflow_total = EXCLUDED.cashflow_total,
                net_sales_total = EXCLUDED.net_sales_total,
                shortfall_total = EXCLUDED.shortfall_total,
                updated_at = CURRENT_TIMESTAMP
        """)

        db_session.execute(upsert_sql, {
            'user_id': user_id,
            'month': month_name,
            'year': year,
            'country': row_country,
            'target_sales': row_target_sales,
            'cashflow_total': row_cashflow_total,
            'net_sales_total': row_net_sales_total,
            'shortfall_total': row_shortfall_total
        })

        return {
            'country': row_country,
            'target_sales': row_target_sales,
            'cashflow_total': row_cashflow_total,
            'net_sales_total': row_net_sales_total,
            'shortfall_total': row_shortfall_total
        }

    def get_saved_country_target(row_country):
        q = text(f"""
            SELECT target_sales
            FROM {target_table_name}
            WHERE user_id = :user_id
              AND LOWER(month) = LOWER(:month)
              AND year = :year
              AND LOWER(country) = LOWER(:country)
            LIMIT 1
        """)

        row = db_session.execute(q, {
            'user_id': user_id,
            'month': month_name,
            'year': year,
            'country': row_country
        }).fetchone()

        return float(row[0]) if row and row[0] is not None else 0

    def save_global_target_from_country_rows():
        gbp_to_usd_rate = get_global_gbp_to_usd_rate(month_name, year)

        uk_target_gbp = get_saved_country_target('uk')
        us_target_usd = get_saved_country_target('us')

        global_target_sales = round((uk_target_gbp * gbp_to_usd_rate) + us_target_usd, 2)

        global_summary_totals, global_details = compute_monthly_cashflow_summary(
            user_id_value=user_id,
            year_value=year,
            country_value='global',
            month_name_value=month_name
        )

        global_net_sales_total = round(float(global_summary_totals.get('net_sales', 0) or 0), 2)
        global_cashflow_total = round(float(global_summary_totals.get('cashflow', 0) or 0), 2)

        saved_global_row = upsert_target_row(
            row_country='global',
            row_target_sales=global_target_sales,
            row_cashflow_total=global_cashflow_total,
            row_net_sales_total=global_net_sales_total
        )

        return {
            'global_row': saved_global_row,
            'uk_target_gbp': round(uk_target_gbp, 2),
            'us_target_usd': round(us_target_usd, 2),
            'gbp_to_usd_rate': gbp_to_usd_rate,
            'formula': '(uk_target_gbp * gbp_to_usd_rate) + us_target_usd',
            'global_details': global_details
        }

    try:
        target_table_name = "target_data"

        create_table_sql = text(f"""
            CREATE TABLE IF NOT EXISTS {target_table_name} (
                id SERIAL PRIMARY KEY,
                user_id INTEGER NOT NULL,
                month VARCHAR(20) NOT NULL,
                year INTEGER NOT NULL,
                country VARCHAR(50) NOT NULL,
                target_sales NUMERIC(12,2) NOT NULL DEFAULT 0,
                cashflow_total NUMERIC(12,2) NOT NULL DEFAULT 0,
                net_sales_total NUMERIC(12,2) NOT NULL DEFAULT 0,
                shortfall_total NUMERIC(12,2) NOT NULL DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE (user_id, month, year, country)
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
                user_id_value=user_id,
                year_value=year,
                country_value=country,
                month_name_value=month_name
            )

            raw_net_sales_total = float(summary_totals.get('net_sales', 0) or 0)
            raw_cashflow_total = float(summary_totals.get('cashflow', 0) or 0)

            global_info = None

            if country == 'global':
                # Direct global save.
                # If frontend directly sends global target, store that value as global.
                # No extra conversion here.
                saved_country_row = upsert_target_row(
                    row_country='global',
                    row_target_sales=target_sales,
                    row_cashflow_total=raw_cashflow_total,
                    row_net_sales_total=raw_net_sales_total
                )

            elif country in ['uk', 'us']:
                # Save UK or US own country row as entered.
                # UK stays GBP. US stays USD.
                saved_country_row = upsert_target_row(
                    row_country=country,
                    row_target_sales=target_sales,
                    row_cashflow_total=raw_cashflow_total,
                    row_net_sales_total=raw_net_sales_total
                )

                # Then rebuild global row.
                # global = UK target converted GBP -> USD + US target.
                global_info = save_global_target_from_country_rows()

            else:
                # For other countries, save only that country row.
                saved_country_row = upsert_target_row(
                    row_country=country,
                    row_target_sales=target_sales,
                    row_cashflow_total=raw_cashflow_total,
                    row_net_sales_total=raw_net_sales_total
                )

            db_session.commit()

            saved_row_sql = text(f"""
                SELECT id, created_at, updated_at
                FROM {target_table_name}
                WHERE user_id = :user_id
                  AND LOWER(month) = LOWER(:month)
                  AND year = :year
                  AND LOWER(country) = LOWER(:country)
                LIMIT 1
            """)

            saved_row = db_session.execute(saved_row_sql, {
                'user_id': user_id,
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
                    'target_sales': saved_country_row['target_sales'],
                    'cashflow_total': saved_country_row['cashflow_total'],
                    'net_sales_total': saved_country_row['net_sales_total'],
                    'shortfall_total': saved_country_row['shortfall_total'],
                    'created_at': saved_row[1].isoformat() if saved_row and saved_row[1] else None,
                    'updated_at': saved_row[2].isoformat() if saved_row and saved_row[2] else None,
                    'table_name': target_table_name,
                    'source_details': details,
                    'global_updated': global_info is not None,
                    'global_info': global_info
                }
            }), 200

        get_sql = text(f"""
            SELECT
                id,
                month,
                year,
                country,
                target_sales,
                cashflow_total,
                net_sales_total,
                shortfall_total,
                created_at,
                updated_at
            FROM {target_table_name}
            WHERE user_id = :user_id
              AND LOWER(month) = LOWER(:month)
              AND year = :year
              AND LOWER(country) = LOWER(:country)
            LIMIT 1
        """)

        row = db_session.execute(get_sql, {
            'user_id': user_id,
            'month': month_name,
            'year': year,
            'country': country
        }).fetchone()

        summary_totals, details = compute_monthly_cashflow_summary(
            user_id_value=user_id,
            year_value=year,
            country_value=country,
            month_name_value=month_name
        )

        fresh_net_sales_total = round(float(summary_totals.get('net_sales', 0) or 0), 2)
        fresh_cashflow_total = round(float(summary_totals.get('cashflow', 0) or 0), 2)

        if not row:
            return jsonify({
                'message': 'No saved target summary found, returning computed totals only',
                'data': {
                    'id': None,
                    'user_id': user_id,
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
                'user_id': user_id,
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


@dashboard_bp.route('/country-timezone/<country>', methods=['GET'])
def country_timezone(country):
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

    country = (country or "").strip().lower()

    timezone_map = {
        "uk": {
            "timezone": "Europe/London",
            "label": "UK"
        },
        "us": {
            "timezone": "America/Los_Angeles",
            "label": "US"
        }
    }

    if country not in timezone_map:
        return jsonify({
            "error": "Invalid country. Use 'uk' or 'us'."
        }), 400

    try:
        # Your system/local reference time: India time
        india_now = datetime.now(ZoneInfo("Asia/Kolkata"))

        # Convert same Indian local time to selected country timezone
        selected_tz = ZoneInfo(timezone_map[country]["timezone"])
        country_now = india_now.astimezone(selected_tz)

        return jsonify({
            "country": country,
            "country_label": timezone_map[country]["label"],

            "india": {
                "timezone": "Asia/Kolkata",
                "abbreviation": india_now.strftime("%Z"),   # IST
                "datetime": india_now.strftime("%Y-%m-%d %H:%M:%S"),
                "date": india_now.strftime("%Y-%m-%d"),
                "time": india_now.strftime("%I:%M %p")
            },

            "selected_country": {
                "timezone": timezone_map[country]["timezone"],
                "abbreviation": country_now.strftime("%Z"),  # BST/GMT for UK, PDT/PST for US
                "datetime": country_now.strftime("%Y-%m-%d %H:%M:%S"),
                "date": country_now.strftime("%Y-%m-%d"),
                "time": country_now.strftime("%I:%M %p")
            }
        }), 200

    except Exception as e:
        return jsonify({
            "error": str(e)
        }), 500
    
    
        
