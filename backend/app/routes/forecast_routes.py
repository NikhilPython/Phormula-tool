from flask import Blueprint, request, jsonify , send_file , send_from_directory
import jwt
import os
from sqlalchemy import create_engine, MetaData, text, inspect, Table
import pandas as pd
from config import Config
SECRET_KEY = Config.SECRET_KEY
from app.utils.data_utils import MONTHS_MAP, MONTHS_REVERSE_MAP 
from app.utils.data_utils import send_forecast_email, send_pnlforecast_email 
from app.utils.forecasting_utils import process_forecasting
from app.models.user_models import UploadHistory , User, CountryProfile, StoredFile, db
from app.utils.manual_forecast_utils import generate_manual_forecast
from calendar import month_name
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv
from datetime import datetime
import numpy as np
import re
from decimal import Decimal
from sqlalchemy.orm import sessionmaker
import warnings
warnings.filterwarnings("ignore", category=FutureWarning)

load_dotenv()
db_url = os.getenv('DATABASE_URL', 'postgresql://postgres:password@localhost:5432/phormula')
db_url1= os.getenv('DATABASE_ADMIN_URL', 'postgresql://postgres:password@localhost:5432/admin_db')
forecast_bp = Blueprint('forecast_bp', __name__)



from io import BytesIO
from sqlalchemy.exc import IntegrityError

XLSX_MIME = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"

def send_db_file(stored: StoredFile, download_name: str):
    """Return a StoredFile as flask send_file response."""
    return send_file(
        BytesIO(stored.data),
        mimetype=stored.content_type or XLSX_MIME,
        as_attachment=True,
        download_name=download_name
    )

import tempfile
from pathlib import Path

def ingest_xlsx_path_to_db(*, path: str, user_id, country, filename, kind, month=None, year=None, content_type=XLSX_MIME):
    """Read XLSX from a filesystem path (anywhere), store into DB, then delete it."""
    with open(path, "rb") as f:
        file_bytes = f.read()

    save_file_to_db(
        user_id=user_id,
        country=country,
        filename=filename,
        file_bytes=file_bytes,
        kind=kind,
        month=month,
        year=year,
        content_type=content_type,
    )

    try:
        os.remove(path)
    except Exception as e:
        print(f"⚠ Could not delete temp file {path}: {e}")

    return file_bytes


def find_latest_matching_file(pattern: str, search_dirs: list[str]) -> str | None:
    """
    Search for the newest file matching pattern in provided directories.
    pattern can include wildcards: e.g. 'inventory_forecast_12_us_*.xlsx'
    """
    newest_path = None
    newest_mtime = -1

    for d in search_dirs:
        try:
            for p in Path(d).glob(pattern):
                try:
                    mt = p.stat().st_mtime
                    if mt > newest_mtime:
                        newest_mtime = mt
                        newest_path = str(p)
                except Exception:
                    continue
        except Exception:
            continue

    return newest_path


def default_temp_dirs():
    """
    Places where legacy utils might write.
    Add more if your deployment uses a fixed folder.
    """
    dirs = [tempfile.gettempdir(), os.getcwd()]
    # optional: if you have env-configured temp output folder
    extra = os.getenv("FORECAST_TEMP_DIR")
    if extra:
        dirs.insert(0, extra)
    return dirs


def ingest_disk_xlsx_to_db(*, path: str, user_id, country, filename, kind, month=None, year=None):
    """Read XLSX from disk, store into DB, delete disk file."""
    with open(path, "rb") as f:
        file_bytes = f.read()

    save_file_to_db(
        user_id=user_id,
        country=country,
        filename=filename,
        file_bytes=file_bytes,
        kind=kind,
        month=month,
        year=year,
        content_type=XLSX_MIME,
    )

    try:
        os.remove(path)
    except Exception as e:
        print(f"⚠ Could not delete temp file {path}: {e}")

    return file_bytes

def load_forecast_df(*, user_id, country, filename, disk_fallback_path=None):
    """Return (df, bytes) from DB if present; else try disk fallback if provided."""
    stored = load_file_from_db(user_id=user_id, country=country, filename=filename)
    if stored:
        b = stored.data
        return pd.read_excel(BytesIO(b)), b

    if disk_fallback_path and os.path.exists(disk_fallback_path):
        with open(disk_fallback_path, "rb") as f:
            b = f.read()
        return pd.read_excel(BytesIO(b)), b

    return None, None


def save_file_to_db(*, user_id, country, filename, file_bytes, kind, month=None, year=None, content_type=None):
    if content_type is None:
        content_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"

    # UPSERT-like behavior (update if exists)
    existing = StoredFile.query.filter_by(user_id=user_id, country=country, filename=filename).first()
    if existing:
        existing.data = file_bytes
        existing.kind = kind
        existing.month = month
        existing.year = str(year) if year is not None else existing.year
        existing.content_type = content_type
        db.session.commit()
        return existing

    row = StoredFile(
        user_id=user_id,
        country=country,
        filename=filename,
        data=file_bytes,
        kind=kind,
        month=month,
        year=str(year) if year is not None else None,
        content_type=content_type,
    )
    db.session.add(row)
    try:
        db.session.commit()
    except IntegrityError:
        db.session.rollback()
        # race-safe update
        row2 = StoredFile.query.filter_by(user_id=user_id, country=country, filename=filename).first()
        row2.data = file_bytes
        row2.kind = kind
        row2.month = month
        row2.year = str(year) if year is not None else row2.year
        row2.content_type = content_type
        db.session.commit()
        return row2
    return row


def load_file_from_db(*, user_id, country, filename):
    row = StoredFile.query.filter_by(user_id=user_id, country=country, filename=filename).first()
    return row  # None if not found


@forecast_bp.route('/api/forecast_allmonths', methods=['GET']) 
def forecast_allmonths():
    auth_header = request.headers.get('Authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        return jsonify({'error': 'Authorization token is missing or invalid'}), 401

    token = auth_header.split(' ')[1]

    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=['HS256'])
        user_id = payload.get('user_id')

        if not user_id:
            return jsonify({'error': 'Invalid token payload: user_id missing'}), 401

        country = request.args.get('country')
        mv = request.args.get('month')
        year = request.args.get('year')

        if not all([country, mv, year]):
            return jsonify({'error': 'Missing required parameters: country, month, or year'}), 400

        # NOTE: utils currently writes file using current month suffix (+2).
        current_month = datetime.now().strftime("%b").lower()

        
        output_file = (
            f'inventory_forecast_{user_id}_global_{current_month}+2.xlsx'
            if country == 'global'
            else f'inventory_forecast_{user_id}_{country}_{current_month}+2.xlsx'
        )

        stored = load_file_from_db(user_id=user_id, country=country, filename=output_file)
        if not stored:
            return jsonify({'error': 'Forecast file not found in DB. Please generate it first.'}), 404

        df = pd.read_excel(BytesIO(stored.data))


        # Identify month columns (normalize if needed)
        month_columns = []
        full_month_names = [
            "January", "February", "March", "April", "May", "June",
            "July", "August", "September", "October", "November", "December"
        ]
        month_abbrev = {
            "January": "Jan", "February": "Feb", "March": "Mar", "April": "Apr", 
            "May": "May", "June": "Jun", "July": "Jul", "August": "Aug", 
            "September": "Sep", "October": "Oct", "November": "Nov", "December": "Dec"
        }

        year_short = str(year)[-2:] if year else datetime.now().strftime("%y")
        for col in df.columns:
            col_str = str(col)
            if re.match(r"^[A-Za-z]{3}'\d{2}$", col_str):
                month_columns.append(col_str)
            elif col_str in full_month_names:
                abbrev = month_abbrev[col_str]
                new_col = f"{abbrev}'{year_short}"
                df.rename(columns={col_str: new_col}, inplace=True)
                month_columns.append(new_col)

        def month_key(col):
            try:
                return pd.to_datetime(col.replace("'", ""), format="%b%y")
            except:
                return pd.Timestamp.max

        month_columns = sorted(set(month_columns), key=month_key)

        if country.lower() == 'global':
            df_filtered = df[df['sku'] != 'Total'].copy()
            selected_columns = ["Product Name"] + month_columns
            df_selected = df_filtered[selected_columns].copy()
            df_aggregated = df_selected.groupby('Product Name', as_index=False).sum()

            totals_row = {'Product Name': 'Total'}
            for col in month_columns:
                totals_row[col] = df_aggregated[col].sum()
            df_aggregated = pd.concat([df_aggregated, pd.DataFrame([totals_row])], ignore_index=True)

            df_aggregated.insert(0, 'S.no', range(1, len(df_aggregated) + 1))
            df_aggregated.iloc[-1, 0] = '-'
            columns = list(df_aggregated.columns)
        else:
            selected_columns = ["Product Name", "sku"] + month_columns
            df_aggregated = df[selected_columns].copy()
            columns = selected_columns

        filtered_df = df_aggregated.where(pd.notnull(df_aggregated), None)
        data = filtered_df.to_dict(orient='records')

        print("Selected columns passed to frontend:", columns)
        return jsonify({'columns': columns, 'data': data}), 200

    except jwt.ExpiredSignatureError:
        return jsonify({'error': 'Token has expired'}), 401
    except jwt.InvalidTokenError:
        return jsonify({'error': 'Invalid token'}), 401
    except Exception as e:
        import traceback
        print("Unexpected error during forecast generation:")
        print(traceback.format_exc())
        return jsonify({'error': 'Internal server error', 'message': str(e)}), 500


@forecast_bp.route('/api/forecast_monthrange', methods=['GET'])
def forecast_monthrange():
    auth_header = request.headers.get('Authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        return jsonify({'error': 'Authorization token is missing or invalid'}), 401

    token = auth_header.split(' ')[1]

    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=['HS256'])
        user_id = payload.get('user_id')

        if not user_id:
            return jsonify({'error': 'Invalid token payload: user_id missing'}), 401

        country = request.args.get('country')
        if not country:
            return jsonify({'error': 'Missing required parameter: country'}), 400

        # NOTE: keep in sync with utils filename (current month)
        current_month = datetime.now().strftime("%b").lower()

        output_file = (
            f'inventory_forecast_{user_id}_global_{current_month}+2.xlsx'
            if country == 'global'
            else f'inventory_forecast_{user_id}_{country}_{current_month}+2.xlsx'
        )

        stored = load_file_from_db(user_id=user_id, country=country, filename=output_file)
        if not stored:
            return jsonify({'error': 'Forecast file not found in DB. Please generate it first.'}), 404

        df = pd.read_excel(BytesIO(stored.data))


        month_pattern = re.compile(r"^[A-Za-z]{3}'\d{2}$")
        month_columns = [col for col in df.columns if month_pattern.match(str(col))]

        if not month_columns:
            return jsonify({'error': 'No valid month columns found in the forecast file'}), 400

        def month_key(col):
            try:
                return pd.to_datetime(col.replace("'", ""), format="%b%y")
            except:
                return pd.Timestamp.max

        month_columns_sorted = sorted(set(month_columns), key=month_key)

        first_month = month_columns_sorted[0]
        last_month = month_columns_sorted[-1]

        return jsonify({'month_range': f"{first_month}-{last_month}"}), 200

    except jwt.ExpiredSignatureError:
        return jsonify({'error': 'Token has expired'}), 401
    except jwt.InvalidTokenError:
        return jsonify({'error': 'Invalid token for month range'}), 401
    except Exception as e:
        import traceback
        print("Unexpected error during month range calculation:")
        print(traceback.format_exc())
        return jsonify({'error': 'Internal server error', 'message': str(e)}), 500



@forecast_bp.route('/api/forecast', methods=['GET'])
def get_forecast():
    auth_header = request.headers.get('Authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        return jsonify({'error': 'Authorization token is missing or invalid'}), 401

    token = auth_header.split(' ')[1]

    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=['HS256'])
        user_id = payload.get('user_id')
        if not user_id:
            return jsonify({'error': 'Invalid token payload: user_id missing'}), 401

        country = request.args.get('country')
        mv = request.args.get('month')
        year = request.args.get('year')
        if not all([country, mv, year]):
            return jsonify({'error': 'Missing required parameters: country, month, or year'}), 400

        current_month = datetime.now().strftime("%b").lower()
        inventory_filename = f'inventory_forecast_{user_id}_{country}_{current_month}+2.xlsx'

        # ✅ DB-first cache (inventory file)
        stored = load_file_from_db(user_id=user_id, country=country, filename=inventory_filename)
        if stored:
            return send_db_file(stored, download_name=inventory_filename)

        
        engine = create_engine(db_url)
        result = process_forecasting(user_id, country, mv, year, engine)

        if not result or "inventory_bytes" not in result or not result["inventory_bytes"]:
            return jsonify({'error': 'Forecast generation failed (no output bytes).'}), 500

        # ✅ Save BOTH artifacts to DB (inventory + forecasts_for used by PnL)
        # 1) forecasts_for (PnL depends on this later)
        if result.get("forecast_bytes") and result.get("forecast_filename"):
            save_file_to_db(
                user_id=user_id,
                country=country,
                filename=result["forecast_filename"],
                file_bytes=result["forecast_bytes"],
                kind="forecasts_for",
                month=mv.lower(),
                year=str(year),
                content_type=XLSX_MIME
            )

        # 2) inventory forecast (this route returns this)
        save_file_to_db(
            user_id=user_id,
            country=country,
            filename=result.get("inventory_filename", inventory_filename),
            file_bytes=result["inventory_bytes"],
            kind="inventory_forecast",
            month=mv.lower(),
            year=str(year),
            content_type=XLSX_MIME
        )

        # Reload and return
        stored = load_file_from_db(
            user_id=user_id,
            country=country,
            filename=result.get("inventory_filename", inventory_filename)
        )
        if not stored:
            return jsonify({'error': 'Saved forecast not found in DB after generation.'}), 500

        try:
            # NOTE: if your email function expects to read from disk,
            # update it to read from DB using load_file_from_db.
            send_forecast_email(user_id, stored.filename, mv, year)
        except Exception as e:
            print(f"Email sending failed: {str(e)}")

        return send_db_file(stored, download_name=stored.filename)

    except jwt.ExpiredSignatureError:
        return jsonify({'error': 'Token has expired'}), 401
    except jwt.InvalidTokenError:
        return jsonify({'error': 'Invalid token'}), 401
    except Exception as e:
        import traceback
        print(traceback.format_exc())
        return jsonify({'error': 'Internal server error', 'message': str(e)}), 500





@forecast_bp.route('/forecast_global', methods=['GET', 'POST'])
def forecast_global():
    if request.method == 'OPTIONS':
        return jsonify({'message': 'CORS Preflight OK'}), 200

    try:
        auth_header = request.headers.get('Authorization')
        if not auth_header or not auth_header.startswith('Bearer '):
            return jsonify({'error': 'Authorization token is missing or invalid'}), 401

        token = auth_header.split(' ')[1]
        payload = jwt.decode(token, SECRET_KEY, algorithms=['HS256'])
        user_id = payload.get('user_id')
        if not user_id:
            return jsonify({'error': 'Invalid token payload: user_id missing'}), 401

        mv = request.args.get('month')
        year = request.args.get('year')
        if not all([mv, year]):
            return jsonify({'error': 'Missing required parameters: month or year'}), 400

        current_month = datetime.now().strftime("%b").lower()

        # filenames used by your utils
        uk_name = f'inventory_forecast_{user_id}_uk_{current_month}+2.xlsx'
        us_name = f'inventory_forecast_{user_id}_us_{current_month}+2.xlsx'
        global_name = f'inventory_forecast_{user_id}_global_{current_month}+2.xlsx'

        # optional disk fallback (legacy util still writes to disk sometimes)
        search_dirs = default_temp_dirs()

        uk_disk = None
        us_disk = None
        for d in search_dirs:
            p1 = os.path.join(d, uk_name)
            p2 = os.path.join(d, us_name)
            if not uk_disk and os.path.exists(p1): uk_disk = p1
            if not us_disk and os.path.exists(p2): us_disk = p2


        df_uk, uk_bytes = load_forecast_df(
            user_id=user_id, country='uk', filename=uk_name, disk_fallback_path=uk_disk
        )
        df_us, us_bytes = load_forecast_df(
            user_id=user_id, country='us', filename=us_name, disk_fallback_path=us_disk
        )

        present = []
        if df_uk is not None: present.append('uk')
        if df_us is not None: present.append('us')

        if not present:
            return jsonify({'error': 'No forecast files found for global processing'}), 404

        # If only one exists, just return it (and ensure it’s saved in DB if it came from disk)
        if len(present) == 1:
            only = present[0]
            if only == 'uk':
                if uk_bytes and not load_file_from_db(user_id=user_id, country='uk', filename=uk_name):
                    save_file_to_db(user_id=user_id, country='uk', filename=uk_name,
                                    file_bytes=uk_bytes, kind="inventory_forecast",
                                    month=mv.lower(), year=str(year), content_type=XLSX_MIME)
                stored = load_file_from_db(user_id=user_id, country='uk', filename=uk_name)
                return send_db_file(stored, download_name=uk_name)

            if only == 'us':
                if us_bytes and not load_file_from_db(user_id=user_id, country='us', filename=us_name):
                    save_file_to_db(user_id=user_id, country='us', filename=us_name,
                                    file_bytes=us_bytes, kind="inventory_forecast",
                                    month=mv.lower(), year=str(year), content_type=XLSX_MIME)
                stored = load_file_from_db(user_id=user_id, country='us', filename=us_name)
                return send_db_file(stored, download_name=us_name)

        # Both exist -> merge
        month_pattern = re.compile(r"^[A-Za-z]{3}'\d{2}$")
        # Use union of month cols (in case one file has extra)
        month_cols_uk = [c for c in df_uk.columns if month_pattern.match(str(c))]
        month_cols_us = [c for c in df_us.columns if month_pattern.match(str(c))]
        forecast_cols = sorted(set(month_cols_uk) | set(month_cols_us))

        merged = pd.merge(df_uk, df_us, on=['Product Name', 'sku'], how='outer', suffixes=('_uk', '_us'))

        for col in forecast_cols:
            col_uk = f"{col}_uk" if f"{col}_uk" in merged.columns else col
            col_us = f"{col}_us" if f"{col}_us" in merged.columns else col
            merged[col] = merged.get(col_uk, 0).fillna(0) + merged.get(col_us, 0).fillna(0)

        keep_cols = ['Product Name', 'sku'] + forecast_cols
        global_df = merged[keep_cols].copy()

        # Write to bytes + save to DB
        buf = BytesIO()
        global_df.to_excel(buf, index=False, engine='openpyxl')
        buf.seek(0)
        file_bytes = buf.getvalue()

        save_file_to_db(
            user_id=user_id,
            country='global',
            filename=global_name,
            file_bytes=file_bytes,
            kind="inventory_forecast",
            month=mv.lower(),
            year=str(year),
            content_type=XLSX_MIME,
        )

        stored = load_file_from_db(user_id=user_id, country='global', filename=global_name)
        return send_db_file(stored, download_name=global_name)

    except jwt.ExpiredSignatureError:
        return jsonify({'error': 'Token has expired'}), 401
    except jwt.InvalidTokenError:
        return jsonify({'error': 'Invalid token'}), 401
    except Exception as e:
        import traceback
        print("Unexpected error during global forecast generation:")
        print(traceback.format_exc())
        return jsonify({'error': 'Internal server error', 'message': str(e)}), 500



@forecast_bp.route('/api/manual_forecast', methods=['POST'])
def manual_forecast():
    """
    Manual forecast route (no ML).

    Flow:
      1) Enforce that the last 4 months of sales tables (mv-1 .. mv-4) exist; if not, 404 with the missing list.
      2) Build orders-only new_df from those months (force each table's rows into its month if date parsing is bad).
      3) If preview=1 -> return JSON rows (no email, no file).
      4) If finalize (no preview) -> write files and return XLSX for the requested month.
    """
    from pandas.tseries.offsets import MonthEnd
    import re

    def _year_month_from_title(mon_title: str) -> tuple[int, int]:
        """mon_title like 'September2025' -> (2025, 9)"""
        m = re.match(r"([A-Za-z]+)(\d{4})$", mon_title)
        if not m:
            raise ValueError(f"Bad month token: {mon_title}")
        mon_name_s, year_s = m.groups()
        try:
            exp_month = list(month_name).index(mon_name_s)  # 1..12
        except ValueError:
            raise ValueError(f"Unknown month name in token: {mon_title}")
        return int(year_s), exp_month

    # --- Auth ---
    auth_header = request.headers.get('Authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        return jsonify({'error': 'Authorization token is missing or invalid'}), 401

    token = auth_header.split(' ')[1]
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=['HS256'])
        user_id = payload.get('user_id')
        if not user_id:
            return jsonify({'error': 'Invalid token payload: user_id missing'}), 401
    except jwt.ExpiredSignatureError:
        return jsonify({'error': 'Token has expired'}), 401
    except jwt.InvalidTokenError:
        return jsonify({'error': 'Invalid token'}), 401

    try:
        # --- Inputs ---
        body = request.get_json(silent=True) or {}
        country_raw = (body.get('country') or request.args.get('country') or '').strip()
        mv_raw = (body.get('month') or request.args.get('month') or '').strip()
        year_raw = (body.get('year') or request.args.get('year') or '').strip()
        preview = str(request.args.get('preview', '') or body.get('preview', '')).lower() in ('1', 'true', 'yes')

        if not all([country_raw, mv_raw, year_raw]):
            return jsonify({'error': 'Missing required parameters: country, month, or year'}), 400

        # Normalize/validate inputs
        country = country_raw.lower()
        try:
            mv_lower = mv_raw.strip().lower()
            mv_num = MONTHS_MAP[mv_lower]
        except KeyError:
            return jsonify({'error': f'Invalid month value: {mv_raw}'}), 400

        try:
            req_year = int(str(year_raw))
        except Exception:
            return jsonify({'error': 'Year must be an integer'}), 400

        # Growth map normalization: supports list[{sku,pct}] or dict{sku:pct}
        growth_raw = body.get('growth', {})
        custom_growth_map = {}
        if isinstance(growth_raw, dict):
            for k, v in growth_raw.items():
                try:
                    custom_growth_map[str(k)] = float(v)
                except (TypeError, ValueError):
                    custom_growth_map[str(k)] = 0.0
        elif isinstance(growth_raw, list):
            for item in growth_raw:
                if not item:
                    continue
                sku = str(item.get('sku')) if 'sku' in item else None
                pct = item.get('pct', 0)
                if sku:
                    try:
                        custom_growth_map[sku] = float(pct)
                    except (TypeError, ValueError):
                        custom_growth_map[sku] = 0.0

        # --- Country profile (for horizon) ---
        profile = CountryProfile.query.filter_by(user_id=user_id, country=country).first()
        if not profile:
            return jsonify({'error': f'Country profile not found for user {user_id} and country {country}'}), 404

        transit_time = int(profile.transit_time or 0)
        stock_unit = int(profile.stock_unit or 0)

        # --- 1) Enforce "last 4 months" availability (mv-1 .. mv-4) ---
        ref_month = datetime(req_year, mv_num, 1)
        needed_tokens = []
        cur = ref_month - relativedelta(months=4)
        while cur <= ref_month - relativedelta(months=1):
            needed_tokens.append(f"{month_name[cur.month]}{cur.year}")  # e.g., "September2025"
            cur = cur + relativedelta(months=1)

        engine = create_engine(db_url)
        meta = MetaData()
        meta.reflect(bind=engine)
        all_tables = {t.lower(): t for t in meta.tables.keys()}

        missing = []
        for mon_title in needed_tokens:
            tname = f"user_{user_id}_{country}_{mon_title}_data"
            if tname.lower() not in all_tables:
                missing.append(mon_title)

        if missing:
            return jsonify({
                'error': 'missing_sales_months',
                'detail': 'Required sales tables are missing',
                'months': missing,
                'needed': needed_tokens
            }), 404

        # --- 2) Build orders-only new_df from those months (fix date_time per table) ---
        fetched_frames = []
        with engine.connect() as conn:
            for mon_title in needed_tokens:
                tname = f"user_{user_id}_{country}_{mon_title}_data"
                key = tname.lower()
                table = Table(all_tables[key], MetaData(), autoload_with=engine)

                try:
                    df_month = pd.read_sql(table.select(), conn)
                except Exception as e:
                    print(f"[manual_forecast] Error reading {tname}: {e}")
                    continue

                if df_month.empty:
                    print(f"[manual_forecast] Empty table: {tname}")
                    continue

                # --- DEBUG: before fixing dates
                try:
                    labels_before = sorted(
                        pd.to_datetime(df_month.get('date_time'), errors='coerce')
                        .dt.strftime("%b'%y").dropna().unique().tolist()
                    )
                except Exception:
                    labels_before = []
                print(f"[manual_forecast] {tname} rows={len(df_month)} date_labels_before={labels_before}")

                # Expected month/year from token, force if mismatch or NaT
                exp_year, exp_month = _year_month_from_title(mon_title)

                # 1) Parse to tz-aware UTC then make tz-naive (consistent dtype)
                dt_raw = pd.to_datetime(df_month.get('date_time'), errors='coerce', utc=True)
                try:
                    dt_raw = dt_raw.dt.tz_convert('UTC').dt.tz_localize(None)
                except Exception:
                    # if already tz-naive or all NaT
                    dt_raw = dt_raw.dt.tz_localize(None)

                # 2) Any NaT or mismatched month/year → force to the table's month-end
                mismatch = dt_raw.isna() | (dt_raw.dt.month != exp_month) | (dt_raw.dt.year != exp_year)
                fix_count = int(mismatch.sum())
                df_month['date_time'] = dt_raw
                df_month.loc[mismatch, 'date_time'] = (datetime(exp_year, exp_month, 1) + MonthEnd(0))

                # Normalize numeric types and SKU
                if 'quantity' in df_month.columns:
                    df_month['quantity'] = pd.to_numeric(df_month['quantity'], errors='coerce').fillna(0.0)
                if 'price_in_gbp' in df_month.columns:
                    df_month['price_in_gbp'] = pd.to_numeric(df_month['price_in_gbp'], errors='coerce')
                if 'sku' in df_month.columns:
                    df_month['sku'] = df_month['sku'].astype(str).str.strip()

                # --- DEBUG: after fixing dates
                labels_after = sorted(
                    pd.to_datetime(df_month['date_time'], errors='coerce')
                    .dt.strftime("%b'%y").dropna().unique().tolist()
                )
                print(f"[manual_forecast] {tname} fixed_dates -> forced={fix_count} date_labels_after={labels_after}")

                fetched_frames.append(df_month)

        if not fetched_frames:
            return jsonify({'error': f'No sales tables found for months: {needed_tokens}'}), 404

        global_df = pd.concat(fetched_frames, ignore_index=True)

        orders = global_df[global_df.get('type') == 'Order'].copy()
        if orders.empty:
            return jsonify({'error': 'No Order rows found in selected months'}), 404

        # Build new_df for the util
        new_df = orders[['sku', 'date_time', 'quantity', 'price_in_gbp']].copy()

        # Ensure tz-naive again (handles any leftovers)
        new_dt = pd.to_datetime(new_df['date_time'], errors='coerce', utc=True)
        new_dt = new_dt.dt.tz_convert('UTC').dt.tz_localize(None)  # now tz-naive
        new_df['date_time'] = new_dt

        # quantities numeric
        new_df['quantity'] = pd.to_numeric(new_df['quantity'], errors='coerce').fillna(0.0)

        # Optional debug: what months we are passing forward
        labels_pass = sorted(new_df['date_time'].dt.strftime("%b'%y").dropna().unique().tolist())
        print(f"[manual_forecast] concatenated orders: rows={len(new_df)} date_labels_passed={labels_pass}")

        # Now safe to sort/index
        new_df = new_df.sort_values(by='date_time').set_index('date_time')

        # --- 3) Run manual forecast util ---
        resp, status = generate_manual_forecast(
            user_id=user_id,
            new_df=new_df,
            country=country,
            mv=mv_lower,
            year=req_year,
            custom_growth_map=custom_growth_map,
            transit_time=transit_time,
            stock_unit=stock_unit,
            preview=preview,
        )

        if preview:
            # PREVIEW MODE: JSON rows for the UI; no email, no file
            return resp, status

        # --- 4) FINALIZE: email (optional) and return XLSX for the requested month ---
        try:
            send_forecast_email(user_id, None, mv_lower, req_year)
        except Exception as e:
            print(f"[manual_forecast] Email sending failed: {e}")

        requested_token = datetime(req_year, mv_num, 1).strftime("%b").lower()
        output_file = f'inventory_forecast_{user_id}_{country}_{requested_token}+2.xlsx'
        requested_token = datetime(req_year, mv_num, 1).strftime("%b").lower()
        output_file = f'inventory_forecast_{user_id}_{country}_{requested_token}+2.xlsx'

        # If util already saved into DB elsewhere, just return it
        stored = load_file_from_db(user_id=user_id, country=country, filename=output_file)
        if stored:
            return send_db_file(stored, download_name=output_file)

        
        search_dirs = default_temp_dirs()
        disk_path = None
        for d in search_dirs:
            p = os.path.join(d, output_file)
            if os.path.exists(p):
                disk_path = p
                break

        if not disk_path:
            disk_path = find_latest_matching_file(output_file, search_dirs)

        if not disk_path or not os.path.exists(disk_path):
            return resp, status  # util likely returned JSON already

        # Ingest into DB + delete disk file
        ingest_xlsx_path_to_db(
            path=disk_path,
            user_id=user_id,
            country=country,
            filename=output_file,
            kind="inventory_forecast",
            month=mv_lower,
            year=str(req_year),
        )

        stored = load_file_from_db(user_id=user_id, country=country, filename=output_file)
        return send_db_file(stored, download_name=output_file)


    except Exception as e:
        import traceback
        print("Unexpected error in /api/manual_forecast:")
        print(traceback.format_exc())
        return jsonify({'error': 'Internal server error', 'message': str(e)}), 500


@forecast_bp.route('/api/Pnlforecast', methods=['GET','POST'])
def Pnlforecast():
    auth_header = request.headers.get('Authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        return jsonify({'error': 'Authorization token is missing or invalid'}), 401

    token = auth_header.split(' ')[1]

    try:
        # Decode JWT Token
        payload = jwt.decode(token, SECRET_KEY, algorithms=['HS256'])
        if 'user_id' not in payload:
            return jsonify({'error': 'Invalid token payload: user_id missing'}), 401

        user_id = payload['user_id']
        country = request.args.get('country')
        month = request.args.get('month')
        year = request.args.get('year')

        if not country or not year:
            return jsonify({'error': 'Missing required parameters: country or year'}), 400

        print(f"User ID: {user_id}, Country: {country}, Year: {year}, month: {month}")

        current_month = datetime.now().strftime("%b").lower()

        # Load the forecasted sales data

        forecast_filename = f'forecasts_for_{user_id}_{country}.xlsx'
        stored = load_file_from_db(user_id=user_id, country=country, filename=forecast_filename)

        if not stored:
            return jsonify({'error': f'Forecast file for user {user_id} not found in DB'}), 404

        df = pd.read_excel(BytesIO(stored.data), engine='openpyxl')


        # Normalize column names to lowercase for case-insensitive matching
        df.columns = [col.lower() for col in df.columns]
        
        # Ensure required columns exist (after normalizing names)
        required_columns = ['sku', 'forecast', 'price_in_gbp', 'month']
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            # Try to map columns that might be named differently
            column_mapping = {
                'price_in_gbp': ['price in gbp', 'price_in_gbp', 'price_gbp', 'price'],
                'forecast': ['forecast', 'forecasted', 'forecast_qty', 'qty'],
                'month': ['date', 'forecast_date', 'period']
            }
            
            for req_col in missing_columns[:]:  # Use a copy to avoid modifying during iteration
                for alternative in column_mapping.get(req_col, []):
                    if alternative in df.columns:
                        df[req_col] = df[alternative]
                        missing_columns.remove(req_col)
                        print(f"Mapped column {alternative} to {req_col}")
                        break
        
        if missing_columns:
            return jsonify({'error': f'Missing required columns in forecast file: {missing_columns}'}), 500

        # Connect to the database
        engine = create_engine(db_url)
        connection = engine.connect()
        
        # Initialize the columns
        df['profit_percentage'] = None
        df['avg_sales_price'] = None 

        # Fetch profit % for each SKU from the database
        for sku in df['sku'].unique():  
            table_name = f"skuwisemonthly_{user_id}_{country}_{month}{year}"
            
            # For PostgreSQL, we should use the text() function from SQLAlchemy to properly prepare statements
            # or use parameterized queries to avoid SQL injection and formatting issues
            from sqlalchemy import text
            
            # Create a parameterized query
            query = text(f"SELECT profit_percentage, net_sales, quantity, product_name FROM {table_name} WHERE sku = :sku LIMIT 1")
            
            try:
                # Execute with parameters
                result = connection.execute(query, {"sku": sku}).fetchone()
                
                if result:
                    profit_percentage, net_sales, quantity, product_name = result
                    df.loc[df['sku'] == sku, 'profit_percentage'] = profit_percentage
                    avg_sales_price = net_sales / quantity if quantity != 0 else 0
                    df.loc[df['sku'] == sku, 'avg_sales_price'] = avg_sales_price
                    df.loc[df['sku'] == sku, 'product_name'] = product_name
                    
                else:
                    print(f"⚠ No data found for SKU {sku}, checking merge table...")

                    # For the merge table query, also use parameterized query
                    query_merge = text(f"""
                        SELECT profit_percentage, net_sales, quantity, product_name
                        FROM skuwisemonthly_{user_id}_{country}
                        WHERE sku = :sku
                        ORDER BY 
                            CAST(year AS INTEGER) DESC,
                            CASE LOWER(month)
                                WHEN 'january' THEN 1
                                WHEN 'february' THEN 2
                                WHEN 'march' THEN 3
                                WHEN 'april' THEN 4
                                WHEN 'may' THEN 5
                                WHEN 'june' THEN 6
                                WHEN 'july' THEN 7
                                WHEN 'august' THEN 8
                                WHEN 'september' THEN 9
                                WHEN 'october' THEN 10
                                WHEN 'november' THEN 11
                                WHEN 'december' THEN 12
                            END DESC
                        LIMIT 1
                    """)
                    
                    merge_result = connection.execute(query_merge, {"sku": sku}).fetchone()

                    if merge_result:
                        profit_percentage, net_sales_merge, quantity_merge, product_name_merge = merge_result
                        avg_sales_price_merge = net_sales_merge / quantity_merge  if quantity_merge != 0 else 0
                        # Fallback profit % if not found
                        df.loc[df['sku'] == sku, 'profit_percentage'] = profit_percentage
                        df.loc[df['sku'] == sku, 'avg_sales_price'] = avg_sales_price_merge
                        df.loc[df['sku'] == sku, 'product_name'] = product_name_merge
                        
                    else:
                        print(f"⚠ SKU {sku} not found in merge table either. Assigning defaults.")
                        df.loc[df['sku'] == sku, 'profit_percentage'] = 0
                        df.loc[df['sku'] == sku, 'avg_sales_price'] = 0
            except Exception as e:
                print(f"Error querying database for SKU {sku}: {str(e)}")
                df.loc[df['sku'] == sku, 'profit_percentage'] = 0
                df.loc[df['sku'] == sku, 'avg_sales_price'] = 0

        # Close the database connection
        connection.close()

        # Ensure profit % column is numeric
        df['profit_percentage'] = pd.to_numeric(df['profit_percentage'], errors='coerce')
        df['avg_sales_price'] = pd.to_numeric(df['avg_sales_price'], errors='coerce')

        # Calculate total sales - use avg_sales_price if available, otherwise fall back to price_in_gbp
        df['Total_Sales'] = df['forecast'] * df.apply(
            lambda row: row['avg_sales_price'] if pd.notnull(row['avg_sales_price']) and row['avg_sales_price'] > 0 
            else row['price_in_gbp'], axis=1
        )

        # Calculate profit
        df['profit'] = (df['profit_percentage'] / 100) * df['Total_Sales']

        # Set profit_percentage to 0 for items with no forecast
        df.loc[df['forecast'] == 0, 'profit_percentage'] = 0

        # Ensure the date column is datetime
        df['month'] = pd.to_datetime(df['month'], errors='coerce')
        
        # Group by SKU and sort by date to determine forecast month
        df = df.sort_values(by=['sku', 'month'])
        df['forecast_month'] = df.groupby('sku').cumcount() + 1

        # Convert numerical month ranking to ordinal (1st, 2nd, 3rd)
        ordinal_map = {1: '1st', 2: '2nd', 3: '3rd'}
        df['forecast_month'] = df['forecast_month'].map(ordinal_map)

        product_names = df[['sku', 'product_name']].drop_duplicates(subset='sku')

        # Create pivot table
        df_pivot = df.pivot(index='sku', columns='forecast_month', 
                           values=['forecast', 'profit_percentage', 'Total_Sales', 'profit'])
        
        # Flatten the column names
        df_pivot.columns = [f'{col[0]}_{col[1]}' for col in df_pivot.columns]
        df_pivot.reset_index(inplace=True)
        df_pivot = df_pivot.merge(product_names, on='sku', how='left')
        
        # Add summary columns
        df_pivot['forecast_sum'] = df_pivot.filter(regex='^forecast_').sum(axis=1, skipna=True)
        df_pivot['profit_percentage_sum'] = df_pivot.filter(regex='^profit_percentage_').mean(axis=1, skipna=True)
        df_pivot['profit_sum'] = df_pivot.filter(regex='^profit_[^p]').sum(axis=1, skipna=True)  # Avoid matching profit_percentage
        df_pivot['Total_Sales_sum'] = df_pivot.filter(regex='^Total_Sales_').sum(axis=1, skipna=True)

        total_values = df_pivot.select_dtypes(include=['number']).sum()

        # Handle profit percentage columns separately - use WEIGHTED AVERAGE instead of mean
        profit_percentage_columns = [col for col in df_pivot.columns if col.startswith('profit_percentage_')]
        sales_columns = [col for col in df_pivot.columns if col.startswith('Total_Sales_')]

        for i, col in enumerate(profit_percentage_columns):
            if col in df_pivot.columns:
                # Get corresponding sales column
                period = col.split('_')[-1]  # Extract '1st', '2nd', '3rd'
                sales_col = f'Total_Sales_{period}'
                
                if sales_col in df_pivot.columns:
                    # Calculate weighted average profit percentage
                    total_profit = df_pivot[f'profit_{period}'].sum()
                    total_sales = df_pivot[sales_col].sum()
                    
                    if total_sales > 0:
                        total_values[col] = (total_profit / total_sales) * 100
                    else:
                        total_values[col] = 0
                else:
                    total_values[col] = 0

        # Create total row DataFrame
        total_row = pd.DataFrame([total_values])
        total_row.insert(0, 'sku', 'Total')  # Ensure 'sku' column is in the first position
        if 'product_name' in df_pivot.columns:
            total_row['product_name'] = 'Total'

        # Concatenate with the pivot table
        df_pivot = pd.concat([df_pivot, total_row], ignore_index=True)

        # Convert month name to number
        months = MONTHS_MAP.get(month.lower(), '1')  # Default to 1 if month not found
        try:
            months = int(months)
            print(f"Converted month: {months} (type: {type(months)})")
        except (ValueError, TypeError):
            print(f"Error converting month {month} to integer, using default")
            months = 1

        # Convert year to integer
        try:
            year = int(year)
            print(f"Converted year: {year} (type: {type(year)})")
        except (ValueError, TypeError):
            print(f"Error converting year {year} to integer, using current year")
            year = datetime.now().year

        # Calculate previous months and years for ACOS
        prev_months = [(months - i) % 12 if (months - i) % 12 != 0 else 12 for i in range(3)]
        prev_years = [year if months - i > 0 else year - 1 for i in range(3)]

        # Fetch acos values
        acos_values = []
        for m, y in zip(prev_months, prev_years):
            month_name = MONTHS_REVERSE_MAP.get(m, 'january')
            record = UploadHistory.query.filter_by(user_id=user_id, country=country, month=month_name, year=str(y)).first()
            if record and record.acos is not None:
                try:
                    acos_value = float(record.acos)
                    print(f"Month: {month_name}, Year: {y}, acos: {acos_value}")
                    acos_values.append(acos_value)
                except (ValueError, TypeError):
                    print(f"Error converting acos value for {month_name} {y}")
        
        # Fetch Reimbursement vs CM2 Margins values
        ReimbursementvsCM2Margins1_values = []
        for m, y in zip(prev_months, prev_years):
            month_name = MONTHS_REVERSE_MAP.get(m, 'january')
            record = UploadHistory.query.filter_by(user_id=user_id, country=country, month=month_name, year=str(y)).first()
            if record and record.rembursment_vs_cm2_margins is not None:
                try:
                    ReimbursementvsCM2Margins1_value = float(record.rembursment_vs_cm2_margins)
                    print(f"Month: {month_name}, Year: {y}, ReimbursementvsCM2Margins1_value: {ReimbursementvsCM2Margins1_value}")
                    ReimbursementvsCM2Margins1_values.append(ReimbursementvsCM2Margins1_value)
                except (ValueError, TypeError):
                    print(f"Error converting Reimbursement vs CM2 value for {month_name} {y}")

        # Calculate averages safely
        avg_ReimbursementvsCM2Margins1 = sum(ReimbursementvsCM2Margins1_values) / len(ReimbursementvsCM2Margins1_values) if ReimbursementvsCM2Margins1_values else 0
        avg_acos = sum(acos_values) / len(acos_values) if acos_values else 0

        # Calculate previous 5 months and years for platform fee
        prev_months_5 = [(months - i) % 12 if (months - i) % 12 != 0 else 12 for i in range(5)]
        prev_years_5 = [year if months - i > 0 else year - 1 for i in range(5)]

        # Fetch platform fee values
        platform_fee_values = []
        for m, y in zip(prev_months_5, prev_years_5):
            month_name = MONTHS_REVERSE_MAP.get(m, 'january')
            record = UploadHistory.query.filter_by(user_id=user_id, country=country, month=month_name, year=str(y)).first()
            if record and record.platform_fee is not None:
                try:
                    platform_fee_value = float(record.platform_fee)
                    print(f"Month: {month_name}, Year: {y}, platform_fee: {platform_fee_value}")
                    platform_fee_values.append(platform_fee_value)
                except (ValueError, TypeError):
                    print(f"Error converting platform fee value for {month_name} {y}")

        # Calculate average platform fee percentage if needed
        if platform_fee_values:
            avg_platform_fee_percentage = sum(platform_fee_values) / len(platform_fee_values)
            print(f"Average Platform Fee Percentage over last 5 months: {avg_platform_fee_percentage}")
        else:
            avg_platform_fee_percentage = 0
            print("No platform fee values found for the last 5 months.")

        # Get upload history
        # Normalize values for DB query
        # Get upload history (handle ongoing month)
      

        requested_month = month.lower()
        requested_year = int(year)

        current_month = datetime.now().strftime("%B").lower()
        current_year = datetime.now().year

        # If ongoing month, fallback to previous completed month
        if requested_month == current_month and requested_year == current_year:
            fallback_month_num = datetime.now().month - 1
            fallback_year = requested_year

            if fallback_month_num == 0:  # January edge case
                fallback_month_num = 12
                fallback_year -= 1

            fallback_month = MONTHS_REVERSE_MAP[fallback_month_num]
            fallback_year = str(fallback_year)
        else:
            fallback_month = requested_month
            fallback_year = str(requested_year)

        print(f"📌 Using upload_history from {fallback_month} {fallback_year}")

        upload_history = UploadHistory.query.filter_by(
            user_id=user_id,
            country=country,
            month=fallback_month,
            year=fallback_year
        ).first()


        
        if upload_history:
            # Get values from upload history, with safe defaults
            acos_value = upload_history.acos if upload_history.acos is not None else 0
            platform_fee_percentage = upload_history.platform_fee if upload_history.platform_fee is not None else 0
            rembursement_fee = upload_history.rembursement_fee if upload_history.rembursement_fee is not None else 0
            cm2_profit = upload_history.cm2_profit if upload_history.cm2_profit is not None else 0
            rembursment_vs_cm2_margins = upload_history.rembursment_vs_cm2_margins if upload_history.rembursment_vs_cm2_margins is not None else 0
            
            # Get total sales and CM1 profit for each forecast period
            try:
                total_sales_1st = df_pivot.loc[df_pivot['sku'] == 'Total', 'Total_Sales_1st'].values[0] if 'Total_Sales_1st' in df_pivot.columns else 0
                print(f"Total Sales 1st: {total_sales_1st}")
                total_sales_2nd = df_pivot.loc[df_pivot['sku'] == 'Total', 'Total_Sales_2nd'].values[0] if 'Total_Sales_2nd' in df_pivot.columns else 0
                total_sales_3rd = df_pivot.loc[df_pivot['sku'] == 'Total', 'Total_Sales_3rd'].values[0] if 'Total_Sales_3rd' in df_pivot.columns else 0
                
                # Get CM1 profits (gross profit before advertising and platform fees) for each forecast period  
                cm1_profit_1st = df_pivot.loc[df_pivot['sku'] == 'Total', 'profit_1st'].values[0] if 'profit_1st' in df_pivot.columns else 0
                cm1_profit_2nd = df_pivot.loc[df_pivot['sku'] == 'Total', 'profit_2nd'].values[0] if 'profit_2nd' in df_pivot.columns else 0
                cm1_profit_3rd = df_pivot.loc[df_pivot['sku'] == 'Total', 'profit_3rd'].values[0] if 'profit_3rd' in df_pivot.columns else 0
                
            except (IndexError, KeyError):
                total_sales_1st = total_sales_2nd = total_sales_3rd = 0
                cm1_profit_1st = cm1_profit_2nd = cm1_profit_3rd = 0

            # STEP 1: Calculate TACOS (Total Advertising Cost of Sales) - advertising cost as % of sales
            # First, fetch total_sales for the last 3 months to calculate total_sales_sum_3months
            historical_total_sales = []

            for m, y in zip(prev_months, prev_years):
                month_name = MONTHS_REVERSE_MAP.get(m, 'january')
                record = UploadHistory.query.filter_by(user_id=user_id, country=country, month=month_name, year=str(y)).first()
                if record and record.total_sales is not None:
                    try:
                        total_sales_value = float(record.total_sales)
                        print(f"Month: {month_name}, Year: {y}, total_sales: {total_sales_value}")
                        historical_total_sales.append(total_sales_value)
                    except (ValueError, TypeError):
                        print(f"Error converting total_sales value for {month_name} {y}")

            # Calculate sum of historical total sales
            if historical_total_sales:
                total_sales_sum_3months = sum(historical_total_sales)
                
            else:
                total_sales_sum_3months = 0
                print(f"⚠️ No historical total sales data found")

            # Now fetch advertising total values for the last 3 months
            advertising_total_values = []

            for m, y in zip(prev_months, prev_years):
                month_name = MONTHS_REVERSE_MAP.get(m, 'january')
                record = UploadHistory.query.filter_by(user_id=user_id, country=country, month=month_name, year=str(y)).first()
                if record and record.advertising_total is not None:
                    try:
                        advertising_total_value = float(record.advertising_total)
                        print(f"Month: {month_name}, Year: {y}, advertising_total: {advertising_total_value}")
                        advertising_total_values.append(advertising_total_value)
                    except (ValueError, TypeError):
                        print(f"Error converting advertising_total value for {month_name} {y}")

            # Calculate sum of 3 months advertising totals
            if advertising_total_values:
                advertising_total_sum_3months = sum(advertising_total_values)
                
            else:
                advertising_total_sum_3months = 0
                print(f"⚠️ No historical advertising total data found")

            # Calculate ACOS values using sum of 3 months advertising total / sum of 3 months total sales
            # FIXED: Use abs() to ensure positive values
            acos1_value = abs((advertising_total_sum_3months / total_sales_sum_3months) * 100) if total_sales_sum_3months > 0 else 0
            acos2_value = abs((advertising_total_sum_3months / total_sales_sum_3months) * 100) if total_sales_sum_3months > 0 else 0
            acos3_value = abs((advertising_total_sum_3months / total_sales_sum_3months) * 100) if total_sales_sum_3months > 0 else 0

            
            # STEP 2: Calculate Cost of Advertisement (based on ACOS percentage of sales)
            # FIXED: Use abs() to ensure positive values
            advertising_total1 = abs((total_sales_1st * acos1_value) / 100)
            advertising_total2 = abs((total_sales_2nd * acos2_value) / 100)
            advertising_total3 = abs((total_sales_3rd * acos3_value) / 100)

            

            # STEP 3: Calculate Platform Fees 
            # FIXED: Use abs() to ensure positive values
            current_platform_fee = abs(platform_fee_percentage) if platform_fee_percentage > 0 else abs(avg_platform_fee_percentage)

            platform_fees1_value = abs(avg_platform_fee_percentage)
            platform_fees2_value = abs(avg_platform_fee_percentage)
            platform_fees3_value = abs(avg_platform_fee_percentage)

            # STEP 4: Calculate CM2 Profit = CM1 Profit - Cost of Advertisement - Platform Fees
            # CORRECTED FORMULA: Subtract both advertising and platform fees
            cm2profit1_value = cm1_profit_1st - advertising_total1 - platform_fees1_value
            cm2profit2_value = cm1_profit_2nd - advertising_total2 - platform_fees2_value
            cm2profit3_value = cm1_profit_3rd - advertising_total3 - platform_fees3_value

            # STEP 5: Calculate CM2 Margins (CM2 Profit as percentage of sales)
            cm2margin1_value = (cm2profit1_value / total_sales_1st) * 100 if total_sales_1st > 0 else 0
            cm2margin2_value = (cm2profit2_value / total_sales_2nd) * 100 if total_sales_2nd > 0 else 0
            cm2margin3_value = (cm2profit3_value / total_sales_3rd) * 100 if total_sales_3rd > 0 else 0
            
            # STEP 6: Calculate Net Reimbursement 
            # Fetch reimbursement fee values for the last 3 months
            reimbursement_fee_values = []

            for m, y in zip(prev_months, prev_years):
                month_name = MONTHS_REVERSE_MAP.get(m, 'january')
                record = UploadHistory.query.filter_by(user_id=user_id, country=country, month=month_name, year=str(y)).first()
                if record and record.rembursement_fee is not None:
                    try:
                        reimbursement_fee_value = float(record.rembursement_fee)
                        print(f"Month: {month_name}, Year: {y}, reimbursement_fee: {reimbursement_fee_value}")
                        reimbursement_fee_values.append(reimbursement_fee_value)
                    except (ValueError, TypeError):
                        print(f"Error converting reimbursement fee value for {month_name} {y}")

            # Calculate sum of 3 months reimbursement fees
            if reimbursement_fee_values:
                reimbursement_fee_sum_3months = sum(reimbursement_fee_values)
                print(f"✅ Sum of 3 months reimbursement fees: {reimbursement_fee_sum_3months}")
            else:
                reimbursement_fee_sum_3months = 0
                print(f"⚠️ No reimbursement fee data found")

            # Calculate reimbursement percentage from historical data
            # FIXED: Use abs() to ensure positive values
            reimbursement_percentage = abs((reimbursement_fee_sum_3months / total_sales_sum_3months) * 100) if total_sales_sum_3months > 0 else 0
            

            # Calculate Net Reimbursement for each period - FIXED: Use abs() to ensure positive values
            NetReimbursement1_value = abs((total_sales_1st * reimbursement_percentage) / 100)
            NetReimbursement2_value = abs((total_sales_2nd * reimbursement_percentage) / 100)
            NetReimbursement3_value = abs((total_sales_3rd * reimbursement_percentage) / 100)

           
            # STEP 7: Calculate Reimbursement vs CM2 Margins (reimbursement as % of CM2 profit)
            # FIXED: Use abs() to handle potential negative CM2 profits
            ReimbursementvsCM2Margins1_value = (NetReimbursement1_value / abs(cm2profit1_value)) * 100 if abs(cm2profit1_value) > 0 else 0
            ReimbursementvsCM2Margins2_value = (NetReimbursement2_value / abs(cm2profit2_value)) * 100 if abs(cm2profit2_value) > 0 else 0
            ReimbursementvsCM2Margins3_value = (NetReimbursement3_value / abs(cm2profit3_value)) * 100 if abs(cm2profit3_value) > 0 else 0

           

            # STEP 8: Calculate Reimbursement vs Sales (reimbursement as % of sales)
            # This reports the positive percentage we calculated
            Reimbursementvssales1_value = abs(reimbursement_percentage)
            Reimbursementvssales2_value = abs(reimbursement_percentage)
            Reimbursementvssales3_value = abs(reimbursement_percentage)

           
            # Create all the DataFrame rows - FIXED: All values are now positive
            platform_fees_rows = pd.DataFrame([
                {'sku': 'Platform_Fees1', 'value': abs(platform_fees1_value)},
                {'sku': 'Platform_Fees2', 'value': abs(platform_fees2_value)},
                {'sku': 'Platform_Fees3', 'value': abs(platform_fees3_value)}
            ])

            advertising_rows = pd.DataFrame([
                {'sku': 'advertising_total1', 'value': abs(advertising_total1)},
                {'sku': 'advertising_total2', 'value': abs(advertising_total2)},
                {'sku': 'advertising_total3', 'value': abs(advertising_total3)}
            ])

            cm2profit_rows = pd.DataFrame([
                {'sku': 'cm2profit1', 'value': cm2profit1_value},  # Keep original sign for CM2 profit
                {'sku': 'cm2profit2', 'value': cm2profit2_value},
                {'sku': 'cm2profit3', 'value': cm2profit3_value}
            ])

            cm2margin_rows = pd.DataFrame([
                {'sku': 'cm2margin1', 'value': cm2margin1_value},  # Keep original sign for margins
                {'sku': 'cm2margin2', 'value': cm2margin2_value},
                {'sku': 'cm2margin3', 'value': cm2margin3_value}
            ])

            NetReimbursement_rows = pd.DataFrame([
                {'sku': 'NetReimbursement1', 'value': abs(NetReimbursement1_value)},
                {'sku': 'NetReimbursement2', 'value': abs(NetReimbursement2_value)},
                {'sku': 'NetReimbursement3', 'value': abs(NetReimbursement3_value)}
            ])

            ReimbursementvsCM2Margins_rows = pd.DataFrame([
                {'sku': 'ReimbursementvsCM2Margins1', 'value': abs(ReimbursementvsCM2Margins1_value)},
                {'sku': 'ReimbursementvsCM2Margins2', 'value': abs(ReimbursementvsCM2Margins2_value)},
                {'sku': 'ReimbursementvsCM2Margins3', 'value': abs(ReimbursementvsCM2Margins3_value)}
            ])

            Reimbursementvssales_rows = pd.DataFrame([
                {'sku': 'Reimbursementvssales1', 'value': abs(Reimbursementvssales1_value)},
                {'sku': 'Reimbursementvssales2', 'value': abs(Reimbursementvssales2_value)},
                {'sku': 'Reimbursementvssales3', 'value': abs(Reimbursementvssales3_value)}
            ])

            acos_rows = pd.DataFrame([
                {'sku': 'acos1', 'value': abs(acos1_value)},
                {'sku': 'acos2', 'value': abs(acos2_value)},
                {'sku': 'acos3', 'value': abs(acos3_value)}
            ])

            # Calculate quarter-end totals
            try:
                # Sum up all values - FIXED: Use abs() for costs
                platform_fees_total = abs(platform_fees1_value) + abs(platform_fees2_value) + abs(platform_fees3_value)
                advertising_total = abs(advertising_total1) + abs(advertising_total2) + abs(advertising_total3)
                
                # Get total sales and CM1 profit totals
                total_sales_sum = df_pivot.loc[df_pivot['sku'] == 'Total', 'Total_Sales_sum'].values[0] if 'Total_Sales_sum' in df_pivot.columns else 0
                cm1_profit_sum = df_pivot.loc[df_pivot['sku'] == 'Total', 'profit_sum'].values[0] if 'profit_sum' in df_pivot.columns else 0
                
                # Calculate CM2 profit total using corrected formula: CM1 - Advertising - Platform Fees
                cm2profit_total = cm1_profit_sum - advertising_total - platform_fees_total
                cm2margin_total = (cm2profit_total / total_sales_sum) * 100 if total_sales_sum > 0 else 0
                
                # Calculate TACOS total (advertising cost as % of total sales) - FIXED: Use abs()
                acos_total = abs((advertising_total / total_sales_sum) * 100) if total_sales_sum > 0 else 0
                
                # Calculate reimbursement totals - FIXED: Use abs()
                NetReimbursement_total = abs(NetReimbursement1_value) + abs(NetReimbursement2_value) + abs(NetReimbursement3_value)
                ReimbursementvsCM2Margins_total = abs((NetReimbursement_total / abs(cm2profit_total)) * 100) if abs(cm2profit_total) > 0 else 0
                Reimbursementvssales_total = abs((NetReimbursement_total / total_sales_sum) * 100) if total_sales_sum > 0 else 0
                
                
            except Exception as e:
                print(f"Error calculating quarter-end totals: {str(e)}")
                platform_fees_total = advertising_total = cm2profit_total = cm2margin_total = 0
                acos_total = NetReimbursement_total = ReimbursementvsCM2Margins_total = Reimbursementvssales_total = 0

            # Create quarter-end rows - FIXED: Use abs() for positive values
            quarterend_row = pd.DataFrame([
                {'sku': 'platform_fees_total', 'value': abs(platform_fees_total)},
                {'sku': 'advertising_total', 'value': abs(advertising_total)},
                {'sku': 'cm2profit_total', 'value': cm2profit_total},  # Keep original sign
                {'sku': 'cm2margin_total', 'value': cm2margin_total},  # Keep original sign
                {'sku': 'acos_total', 'value': abs(acos_total)},
                {'sku': 'NetReimbursement_total', 'value': abs(NetReimbursement_total)},
                {'sku': 'ReimbursementvsCM2Margins_total', 'value': abs(ReimbursementvsCM2Margins_total)},
                {'sku': 'Reimbursementvssales_total', 'value': abs(Reimbursementvssales_total)}
            ])

            # Get list of columns from the pivot table
            columns_list = df_pivot.columns.tolist()

            # Add missing columns to all DataFrames
            additional_dfs = [
                acos_rows, platform_fees_rows, advertising_rows, cm2profit_rows, 
                NetReimbursement_rows, ReimbursementvsCM2Margins_rows, 
                Reimbursementvssales_rows, cm2margin_rows, quarterend_row
            ]
            
            for df_add in additional_dfs:
                for col in columns_list:
                    if col not in df_add.columns:
                        df_add[col] = None  # Add missing columns

            # Concatenate all DataFrames
            final_df = pd.concat([df_pivot] + additional_dfs, ignore_index=True)

            # Generate output filename
            output_filename = f'forecastpnl_{user_id}_{country}_{month}_{year}_table.xlsx'

            buf = BytesIO()
            final_df.to_excel(buf, index=False, engine='openpyxl')
            buf.seek(0)

            save_file_to_db(
                user_id=user_id,
                country=country,
                filename=output_filename,
                file_bytes=buf.getvalue(),
                kind="pnl_forecast",
                month=month.lower() if month else None,
                year=str(year),
            )

            buf.seek(0)
            return send_file(
                buf,
                mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                as_attachment=True,
                download_name=f'PnL_forecast_{country}_{month}_{year}.xlsx'
            )
        else:
            return jsonify({'error': 'No upload history found for specified parameters'}), 404

    except jwt.ExpiredSignatureError:
        return jsonify({'error': 'Token has expired'}), 401
    except jwt.InvalidTokenError:
        return jsonify({'error': 'Invalid token'}), 401
    except Exception as e:
        print(f"Error in Pnlforecast: {str(e)}")  # Print error to console
        import traceback
        traceback.print_exc()  # Print full traceback for debugging
        return jsonify({'error': str(e)}), 500



@forecast_bp.route('/api/Pnlforecast/global', methods=['GET', 'POST'])
def Pnlforecasts():

    auth_header = request.headers.get('Authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        return jsonify({'error': 'Authorization token is missing or invalid'}), 401

    token = auth_header.split(' ')[1]
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=['HS256'])
        user_id = payload.get('user_id')
        if not user_id:
            return jsonify({'error': 'Invalid token payload: user_id missing'}), 401

        country = 'global'
        month = request.args.get('month')
        year = request.args.get('year')

        if not month or not year:
            return jsonify({'error': 'Missing required parameters: month or year'}), 400

        # Conversion Rate Helper
        def get_usd_conversion_rate(month, year):
            try:
                engine_conv = create_engine(db_url1)
                with engine_conv.connect() as conn:
                    query = text("""
                        SELECT conversion_rate
                        FROM currency_conversion
                        WHERE lower(country) = 'us' AND lower(month) = :month AND year = :year
                        LIMIT 1
                    """)
                    result = conn.execute(query, {"month": month.lower(), "year": int(year)}).fetchone()
                    return float(result[0]) if result and result[0] is not None else 1.0
            except Exception as e:
                print(f"❌ Error fetching conversion rate: {str(e)}")
                return 1.0

        # Normalize
        month = month.lower()
        year = int(year)

        # Filenames (same names you were using on disk)
        global_filename = f'forecastpnl_{user_id}_global_{month}_{year}_table.xlsx'
        uk_filename = f'forecastpnl_{user_id}_uk_{month}_{year}_table.xlsx'
        us_filename = f'forecastpnl_{user_id}_us_{month}_{year}_table.xlsx'

        # ✅ Load from DB instead of disk
        uk_stored = load_file_from_db(user_id=user_id, country='uk', filename=uk_filename)
        us_stored = load_file_from_db(user_id=user_id, country='us', filename=us_filename)
        global_stored = load_file_from_db(user_id=user_id, country='global', filename=global_filename)

        uk_exists = uk_stored is not None
        us_exists = us_stored is not None
        global_exists = global_stored is not None

        print(f"📁 DB File status - UK: {uk_exists}, US: {us_exists}, Global: {global_exists}")

        # Timestamp helper (works only if your StoredFile model has updated_at/created_at)
        def get_db_timestamp(stored_obj):
            if not stored_obj:
                return 0
            # Try common timestamp fields
            for attr in ("updated_at", "modified_at", "created_at", "timestamp"):
                if hasattr(stored_obj, attr) and getattr(stored_obj, attr) is not None:
                    try:
                        return getattr(stored_obj, attr).timestamp()
                    except Exception:
                        pass
            return 0

        uk_ts = get_db_timestamp(uk_stored) if uk_exists else 0
        us_ts = get_db_timestamp(us_stored) if us_exists else 0
        global_ts = get_db_timestamp(global_stored) if global_exists else 0

        # Determine scenario
        if not uk_exists and not us_exists:
            return jsonify({'error': 'No PnL forecast files found for UK or US in DB'}), 404

        needs_regeneration = False
        current_scenario = None

        if uk_exists and us_exists:
            current_scenario = "both"
            latest_country_ts = max(uk_ts, us_ts)
            # If we have timestamps, check freshness. If we don't, regenerate only if missing.
            if (global_ts and latest_country_ts and global_ts < latest_country_ts) or (not global_exists):
                needs_regeneration = True
                print("🔄 Both UK and US found - Global needs regeneration")
            else:
                print("✅ Global is up-to-date with both countries")

        elif us_exists and not uk_exists:
            current_scenario = "us_only"
            if (global_ts and us_ts and global_ts < us_ts) or (not global_exists):
                needs_regeneration = True
                print("🔄 Only US found - Global needs regeneration")
            else:
                print("✅ Global is up-to-date with US data")

        elif uk_exists and not us_exists:
            current_scenario = "uk_only"
            if (global_ts and uk_ts and global_ts < uk_ts) or (not global_exists):
                needs_regeneration = True
                print("🔄 Only UK found - Global needs regeneration")
            else:
                print("✅ Global is up-to-date with UK data")

        # If global exists and up-to-date, return from DB
        if global_exists and not needs_regeneration:
            print("✅ Returning existing up-to-date global file from DB")
            return send_db_file(
                global_stored,
                download_name=f'PnL_forecast_global_{month}_{year}.xlsx'
            )

        # Generate/Regenerate global file based on scenario
        print(f"🔄 Regenerating global file for scenario: {current_scenario}")

        conversion_rate = get_usd_conversion_rate(month, year)
        print(f"🔄 Using conversion rate: {conversion_rate}")

        # Read excel from DB bytes
        if current_scenario == "both":
            print("📊 Creating combined global forecast from UK and US data (DB)")
            df_uk_original = pd.read_excel(BytesIO(uk_stored.data), engine='openpyxl')
            df_us_original = pd.read_excel(BytesIO(us_stored.data), engine='openpyxl')

            df_uk = df_uk_original.copy()
            df_us = df_us_original.copy()

            df_uk['country'] = 'UK'
            df_us['country'] = 'US'

            df_uk_converted = convert_uk_to_usd(df_uk, conversion_rate)
            df_global = process_combined_forecast_data(df_us, df_uk_converted)

        elif current_scenario == "us_only":
            print("🇺🇸 Creating global forecast from US data only (DB)")
            df_global = pd.read_excel(BytesIO(us_stored.data), engine='openpyxl')

        elif current_scenario == "uk_only":
            print("🇬🇧 Creating global forecast from UK data only (DB) and converting to USD")
            df_uk_original = pd.read_excel(BytesIO(uk_stored.data), engine='openpyxl')
            df_uk = df_uk_original.copy()
            df_uk['country'] = 'UK'
            df_global = convert_uk_to_usd(df_uk, conversion_rate)

        else:
            return jsonify({'error': 'Unexpected scenario while generating global PnL'}), 500

        # Drop temp country column
        if 'country' in df_global.columns:
            df_global = df_global.drop('country', axis=1)

        # Save global to DB as bytes
        buf = BytesIO()
        df_global.to_excel(buf, index=False, engine='openpyxl')
        buf.seek(0)

        save_file_to_db(
            user_id=user_id,
            country='global',
            filename=global_filename,
            file_bytes=buf.getvalue(),
            kind="pnl_forecast",
            month=month,
            year=str(year),
            content_type=XLSX_MIME
        )

        # Return from DB
        global_stored = load_file_from_db(user_id=user_id, country='global', filename=global_filename)
        return send_db_file(
            global_stored,
            download_name=f'PnL_forecast_global_{month}_{year}.xlsx'
        )

    except jwt.ExpiredSignatureError:
        return jsonify({'error': 'Token has expired'}), 401
    except jwt.InvalidTokenError:
        return jsonify({'error': 'Invalid token'}), 401
    except Exception as e:
        print(f"❌ Error in Pnlforecasts route: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500



def convert_uk_to_usd(df_uk, conversion_rate):
    """Convert UK financial data to USD"""
    df_converted = df_uk.copy()
    
    # Define columns to convert (all financial columns)
    financial_columns = [
        'Total_Sales_1st', 'Total_Sales_2nd', 'Total_Sales_3rd', 'Total_Sales_sum',
        'profit_1st', 'profit_2nd', 'profit_3rd', 'profit_sum'
    ]
    
    # Convert basic financial columns
    for col in financial_columns:
        if col in df_converted.columns:
            df_converted[col] = df_converted[col] * conversion_rate
    
    # Convert 'value' column for financial metrics
    financial_metrics = [
        'Platform_Fees1', 'Platform_Fees2', 'Platform_Fees3',
        'advertising_total1', 'advertising_total2', 'advertising_total3',
        'cm2profit1', 'cm2profit2', 'cm2profit3',
        'NetReimbursement1', 'NetReimbursement2', 'NetReimbursement3',
        'platform_fees_total', 'advertising_total', 'cm2profit_total',
        'NetReimbursement_total'
    ]
    
    for metric in financial_metrics:
        mask = df_converted['sku'] == metric
        if mask.any():
            df_converted.loc[mask, 'value'] = df_converted.loc[mask, 'value'] * conversion_rate
    
    # Don't convert percentage columns, forecast quantities, etc.
    print(f"✅ UK data converted to USD using rate: {conversion_rate}")
    return df_converted


def process_combined_forecast_data(df_us, df_uk_converted):
    """Process and combine US and converted UK forecast data"""
    
    # Combine the dataframes
    df_combined = pd.concat([df_us, df_uk_converted], ignore_index=True)
    
    # Define financial metrics that need special handling
    financial_metrics = [
        'acos1', 'acos2', 'acos3', 'Platform_Fees1', 'Platform_Fees2', 'Platform_Fees3',
        'advertising_total1', 'advertising_total2', 'advertising_total3',
        'cm2profit1', 'cm2profit2', 'cm2profit3',
        'NetReimbursement1', 'NetReimbursement2', 'NetReimbursement3',
        'ReimbursementvsCM2Margins1', 'ReimbursementvsCM2Margins2', 'ReimbursementvsCM2Margins3',
        'Reimbursementvssales1', 'Reimbursementvssales2', 'Reimbursementvssales3',
        'cm2margin1', 'cm2margin2', 'cm2margin3',
        'platform_fees_total', 'advertising_total', 'cm2profit_total', 'cm2margin_total',
        'acos_total', 'NetReimbursement_total', 'ReimbursementvsCM2Margins_total',
        'Reimbursementvssales_total', 'Total'
    ]
    
    # Separate product rows from financial metric rows
    product_rows = df_combined[~df_combined['sku'].isin(financial_metrics)].copy()
    financial_rows = df_combined[df_combined['sku'].isin(financial_metrics)].copy()
    
    # Process product rows - group by product_name and sum quantities/values
    if not product_rows.empty:
        # Handle missing product names
        product_rows['product_name'] = product_rows['product_name'].fillna('Unknown')
        
        # Define numeric columns to sum
        numeric_cols_to_sum = [
            'forecast_1st', 'forecast_2nd', 'forecast_3rd', 'forecast_sum',
            'Total_Sales_1st', 'Total_Sales_2nd', 'Total_Sales_3rd', 'Total_Sales_sum',
            'profit_1st', 'profit_2nd', 'profit_3rd', 'profit_sum'
        ]
        
        # Group products by product_name
        agg_dict = {'sku': 'first'}  # Take first SKU
        for col in numeric_cols_to_sum:
            if col in product_rows.columns:
                agg_dict[col] = 'sum'
        
        # Add other columns
        other_cols = ['value', 'country']
        for col in other_cols:
            if col in product_rows.columns:
                agg_dict[col] = 'first'
        
        grouped_products = product_rows.groupby('product_name').agg(agg_dict).reset_index()
        
        # Recalculate profit percentages after grouping
        for period in ['1st', '2nd', '3rd', 'sum']:
            profit_col = f'profit_{period}'
            sales_col = f'Total_Sales_{period}'
            percentage_col = f'profit_percentage_{period}'
            
            if profit_col in grouped_products.columns and sales_col in grouped_products.columns:
                grouped_products[percentage_col] = np.where(
                    grouped_products[sales_col] != 0,
                    (grouped_products[profit_col] / grouped_products[sales_col]) * 100,
                    0
                )
    else:
        grouped_products = pd.DataFrame()
    
    # Process financial metrics - sum values for same metrics
    grouped_financial_list = []
    
    if not financial_rows.empty:
        for sku in financial_rows['sku'].unique():
            sku_data = financial_rows[financial_rows['sku'] == sku]
            
            # Create aggregated row
            result_row = sku_data.iloc[0].copy()
            
            # Sum financial values
            if 'value' in sku_data.columns:
                result_row['value'] = sku_data['value'].sum()
            
            # Sum other numeric columns
            numeric_cols = sku_data.select_dtypes(include=[np.number]).columns
            for col in numeric_cols:
                if col != 'value':  # Already handled above
                    result_row[col] = sku_data[col].sum()
            
            grouped_financial_list.append(result_row)
        
        grouped_financial = pd.DataFrame(grouped_financial_list)
    else:
        grouped_financial = pd.DataFrame()
    
    # Combine all results
    if not grouped_products.empty and not grouped_financial.empty:
        result_df = pd.concat([grouped_products, grouped_financial], ignore_index=True)
    elif not grouped_products.empty:
        result_df = grouped_products
    elif not grouped_financial.empty:
        result_df = grouped_financial
    else:
        result_df = pd.DataFrame()
    
    # Recalculate dependent metrics (ACOS, margins, reimbursement ratios)
    if not result_df.empty:
        result_df = recalculate_dependent_metrics(result_df)
    
    return result_df


def recalculate_dependent_metrics(df):
    """Recalculate percentage-based metrics after aggregation"""
    
    # Get total sales for calculations
    total_row = df[df['sku'] == 'Total']
    if total_row.empty:
        print("⚠️ No 'Total' row found for recalculation")
        return df
    
    total_sales_1st = total_row['Total_Sales_1st'].iloc[0] if 'Total_Sales_1st' in total_row.columns else 0
    total_sales_2nd = total_row['Total_Sales_2nd'].iloc[0] if 'Total_Sales_2nd' in total_row.columns else 0
    total_sales_3rd = total_row['Total_Sales_3rd'].iloc[0] if 'Total_Sales_3rd' in total_row.columns else 0
    total_sales_sum = total_row['Total_Sales_sum'].iloc[0] if 'Total_Sales_sum' in total_row.columns else 0
    
    # Recalculate ACOS percentages
    for period, sales in [('1', total_sales_1st), ('2', total_sales_2nd), ('3', total_sales_3rd)]:
        platform_fees_mask = df['sku'] == f'Platform_Fees{period}'
        acos_mask = df['sku'] == f'acos{period}'
        
        if platform_fees_mask.any() and acos_mask.any() and sales > 0:
            platform_fees_value = df.loc[platform_fees_mask, 'value'].iloc[0]
            acos_percentage = (platform_fees_value / sales) * 100
            df.loc[acos_mask, 'value'] = acos_percentage
    
    # Recalculate total ACOS
    total_platform_fees_mask = df['sku'] == 'platform_fees_total'
    acos_total_mask = df['sku'] == 'acos_total'
    if total_platform_fees_mask.any() and acos_total_mask.any() and total_sales_sum > 0:
        total_platform_fees = df.loc[total_platform_fees_mask, 'value'].iloc[0]
        acos_total_percentage = (total_platform_fees / total_sales_sum) * 100
        df.loc[acos_total_mask, 'value'] = acos_total_percentage
    
    # Recalculate CM2 margins
    for period, sales in [('1', total_sales_1st), ('2', total_sales_2nd), ('3', total_sales_3rd)]:
        cm2profit_mask = df['sku'] == f'cm2profit{period}'
        cm2margin_mask = df['sku'] == f'cm2margin{period}'
        
        if cm2profit_mask.any() and cm2margin_mask.any() and sales > 0:
            cm2profit_value = df.loc[cm2profit_mask, 'value'].iloc[0]
            cm2margin_percentage = (cm2profit_value / sales) * 100
            df.loc[cm2margin_mask, 'value'] = cm2margin_percentage
    
    # Recalculate total CM2 margin
    total_cm2profit_mask = df['sku'] == 'cm2profit_total'
    cm2margin_total_mask = df['sku'] == 'cm2margin_total'
    if total_cm2profit_mask.any() and cm2margin_total_mask.any() and total_sales_sum > 0:
        total_cm2profit = df.loc[total_cm2profit_mask, 'value'].iloc[0]
        cm2margin_total_percentage = (total_cm2profit / total_sales_sum) * 100
        df.loc[cm2margin_total_mask, 'value'] = cm2margin_total_percentage
    
    # Recalculate Reimbursement ratios
    for period, sales in [('1', total_sales_1st), ('2', total_sales_2nd), ('3', total_sales_3rd)]:
        net_reimb_mask = df['sku'] == f'NetReimbursement{period}'
        reimb_vs_sales_mask = df['sku'] == f'Reimbursementvssales{period}'
        reimb_vs_cm2_mask = df['sku'] == f'ReimbursementvsCM2Margins{period}'
        cm2profit_mask = df['sku'] == f'cm2profit{period}'
        
        if net_reimb_mask.any():
            net_reimb_value = df.loc[net_reimb_mask, 'value'].iloc[0]
            
            # Reimbursement vs Sales
            if reimb_vs_sales_mask.any() and sales > 0:
                reimb_vs_sales_percentage = (net_reimb_value / sales) * 100
                df.loc[reimb_vs_sales_mask, 'value'] = reimb_vs_sales_percentage
            
            # Reimbursement vs CM2 Margins
            if reimb_vs_cm2_mask.any() and cm2profit_mask.any():
                cm2profit_value = df.loc[cm2profit_mask, 'value'].iloc[0]
                if cm2profit_value != 0:
                    reimb_vs_cm2_percentage = (net_reimb_value / cm2profit_value) * 100
                    df.loc[reimb_vs_cm2_mask, 'value'] = reimb_vs_cm2_percentage
    
    # Recalculate total reimbursement ratios
    total_net_reimb_mask = df['sku'] == 'NetReimbursement_total'
    total_reimb_vs_sales_mask = df['sku'] == 'Reimbursementvssales_total'
    total_reimb_vs_cm2_mask = df['sku'] == 'ReimbursementvsCM2Margins_total'
    
    if total_net_reimb_mask.any():
        total_net_reimb = df.loc[total_net_reimb_mask, 'value'].iloc[0]
        
        # Total Reimbursement vs Sales
        if total_reimb_vs_sales_mask.any() and total_sales_sum > 0:
            total_reimb_vs_sales = (total_net_reimb / total_sales_sum) * 100
            df.loc[total_reimb_vs_sales_mask, 'value'] = total_reimb_vs_sales
        
        # Total Reimbursement vs CM2 Margins
        if total_reimb_vs_cm2_mask.any() and total_cm2profit_mask.any():
            total_cm2profit = df.loc[total_cm2profit_mask, 'value'].iloc[0]
            if total_cm2profit != 0:
                total_reimb_vs_cm2 = (total_net_reimb / total_cm2profit) * 100
                df.loc[total_reimb_vs_cm2_mask, 'value'] = total_reimb_vs_cm2
    
    return df



@forecast_bp.route('/api/Pnlforecast/previous_months', methods=['GET', 'POST'])
def Pnlforecast_previous_months():
    auth_header = request.headers.get('Authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        return jsonify({'error': 'Authorization token is missing or invalid'}), 401

    token = auth_header.split(' ')[1]
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=['HS256'])
        user_id = payload.get('user_id')
        if not user_id:
            return jsonify({'error': 'Invalid token payload: user_id missing'}), 401

        month = request.args.get('month')
        year = request.args.get('year')
        period_type = request.args.get('period_type', 'monthly')
        record_country = request.args.get('country', 'global')

        if not month or not year:
            return jsonify({'error': 'Missing required parameters: month or year'}), 400

        month = month.lower()
        year = int(year)
        month_name = month

        engine = create_engine(db_url)
        SessionLocal = sessionmaker(bind=engine)
        db_session = SessionLocal()
        inspector = inspect(engine)

        def get_best_key(d, keys, default=0):
            for key in keys:
                val = d.get(key)
                if val not in [None, '']:
                    try:
                        return float(val)
                    except (ValueError, TypeError):
                        continue
            return float(default)

        try:
            if period_type == 'monthly':
                if record_country.lower() == "global":
                    table_name = f"skuwisemonthly_{user_id}_{record_country.lower()}_{month_name.lower()}{year}_table"
                else:
                    table_name = f"skuwisemonthly_{user_id}_{record_country.lower()}_{month_name.lower()}{year}"

            if not inspector.has_table(table_name):
                return jsonify({'error': f'Table {table_name} does not exist'}), 404

            data_query = f"SELECT * FROM {table_name}"
            result = db_session.execute(text(data_query))
            rows = result.fetchall()

            if not rows:
                return jsonify({
                    'data': [],
                    'totals': {
                        'net_sales_total': 0,
                        'advertising_total': 0,
                        'amazon_fee_total': 0,
                        'cm2_profit_total': 0,
                        'profit_total': 0,
                        'cost_of_unit_sold_total': 0
                    }
                }), 200

            columns = result.keys()
            data = []

            net_sales_total = 0
            advertising_total = 0
            amazon_fee_total = 0
            cm2_profit_total = 0
            profit_total = 0
            cost_of_unit_sold_total = 0

           

            for row in rows:
                row_dict = {}
                for col in columns:
                    value = getattr(row, col)
                    row_dict[col] = float(value) if isinstance(value, (int, float, Decimal)) else value

                # Skip TOTAL row for calculations but include it in data
                if str(row_dict.get('sku', '')).strip().upper() == 'TOTAL':
                    data.append(row_dict)
                    continue

                # Based on your database screenshots, here are the correct column mappings:
                
                # Net Sales - exact match from your DB
                net_sales_total += get_best_key(row_dict, ['net_sales', 'netsales'])

                # Advertising Total - from your screenshots, the column appears to be 'advertising_total'
                # But it might be empty/zero, so let's also check for variations
                advertising_total += get_best_key(row_dict, [
                    'advertising_total', 
                    'advertisingtotal', 
                    'advertising_costs',
                    'advertising',
                    'ad_spend',
                    'ppc_spend'
                ])

                # Amazon Fee - exact match from your DB
                amazon_fee_total += get_best_key(row_dict, ['amazon_fee', 'amazonfee'])

                # CM2 Profit - from your screenshots, this column appears to be 'cm2_profit'
                # But it might be empty/zero, so let's also check for variations
                cm2_profit_total += get_best_key(row_dict, [
                    'cm2_profit', 
                    'cm2profit',
                    'cm2_profit_loss',
                    'contribution_margin_2'
                ])

                # CM1 Profit (profit column) - exact match from your DB  
                profit_total += get_best_key(row_dict, ['profit', 'cm1_profit', 'cm1profit'])

                # Cost of Unit Sold - exact match from your DB
                cost_of_unit_sold_total += get_best_key(row_dict, ['cost_of_unit_sold', 'costofunitsold', 'cogs'])

                data.append(row_dict)

            # Check if we have a TOTAL row and extract values from it if individual rows are empty
            total_row = next((row for row in data if str(row.get('sku', '')).strip().upper() == 'TOTAL'), None)
            
            if total_row and (advertising_total == 0 or cm2_profit_total == 0):
                
                
                if advertising_total == 0:
                    advertising_total = get_best_key(total_row, [
                        'advertising_total', 
                        'advertisingtotal', 
                        'advertising_costs',
                        'advertising'
                    ])
                   
                
                if cm2_profit_total == 0:
                    cm2_profit_total = get_best_key(total_row, [
                        'cm2_profit', 
                        'cm2profit',
                        'cm2_profit_loss'
                    ])
            

            totals = {
                'net_sales_total': round(net_sales_total, 2),
                'advertising_total': round(advertising_total, 2),
                'amazon_fee_total': round(amazon_fee_total, 2),
                'cm2_profit_total': round(cm2_profit_total, 2),
                'profit_total': round(profit_total, 2),
                'cost_of_unit_sold_total': round(cost_of_unit_sold_total, 2)
            }

            return jsonify({
                'data': data,
                'totals': totals,
                'record_count': len(data),
                'table_name': table_name,
                'debug_info': {
                    'available_columns': list(columns),
                    'sample_row': dict(rows[0]._mapping) if rows else None,
                    'column_mapping_status': {
                        'net_sales_found': net_sales_total > 0,
                        'advertising_found': advertising_total > 0,
                        'amazon_fee_found': amazon_fee_total > 0,
                        'cm2_profit_found': cm2_profit_total > 0,
                        'profit_found': profit_total > 0,
                        'cogs_found': cost_of_unit_sold_total > 0
                    },
                    'zero_value_columns': {
                        'advertising_columns_checked': ['advertising_total', 'advertisingtotal', 'advertising_costs', 'advertising'],
                        'cm2_profit_columns_checked': ['cm2_profit', 'cm2profit', 'cm2_profit_loss', 'contribution_margin_2']
                    }
                }
            }), 200

        except Exception as e:
            db_session.rollback()
            print(f"Database error: {str(e)}")
            return jsonify({'error': f'Database query failed: {str(e)}'}), 500

        finally:
            db_session.close()

    except jwt.ExpiredSignatureError:
        return jsonify({'error': 'Token has expired'}), 401
    except jwt.InvalidTokenError:
        return jsonify({'error': 'Invalid token'}), 401
    except Exception as e:
        print(f"General error: {str(e)}")
        return jsonify({'error': f'An error occurred: {str(e)}'}), 500
    


@forecast_bp.route('/api/save_pnl_forecast', methods=['POST'])
def save_pnl_forecast():
    auth_header = request.headers.get('Authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        return jsonify({'error': 'Authorization token is missing or invalid'}), 401

    token = auth_header.split(' ')[1]
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=['HS256'])
        user_id = payload.get('user_id')
        if not user_id:
            return jsonify({'error': 'Invalid token payload: user_id missing'}), 401
    except jwt.ExpiredSignatureError:
        return jsonify({'error': 'Token has expired'}), 401
    except jwt.InvalidTokenError:
        return jsonify({'error': 'Invalid token'}), 401

    file = request.files.get('file')
    if not file:
        return jsonify({"error": "No file uploaded"}), 400

    month = request.form.get('month')
    year = request.form.get('year')
    country = request.form.get('country')
    print(f"Received Month: {month}, Year: {year}, Country: {country}")

    if not all([month, year, country]):
        return jsonify({"error": "month, year, and country are required"}), 400

    # ✅ Read bytes directly (no disk)
    file_bytes = file.read()
    if not file_bytes:
        return jsonify({"error": "Uploaded file is empty"}), 400

    # Keep your naming
    new_filename = f"PNLforecast_{user_id}_{month}_{year}.xlsx"
    try:
        save_file_to_db(
            user_id=user_id,
            country=country.lower(),
            filename=new_filename,
            file_bytes=file_bytes,
            kind="pnl_forecast_upload",
            month=month.lower(),
            year=str(year),
            content_type=file.mimetype or XLSX_MIME
        )
        print(f"✅ Saved PNL forecast to DB: {new_filename}")
    except Exception as e:
        print(f"❌ Error saving file to DB: {str(e)}")
        return jsonify({"error": "Failed to save file to DB", "message": str(e)}), 500

    upload_history = UploadHistory.query.filter_by(
        user_id=user_id,
        country=country,
        month=month,
        year=year
    ).first()

    if upload_history:
        if upload_history.pnl_email_sent:
            print("📭 Email already sent for this upload. Skipping...")
        else:
            try:
                from app import db
                # NOTE: your email util currently receives a filename.
                # If it reads from disk, you must update it to read from DB using load_file_from_db.
                send_pnlforecast_email(user_id, new_filename, month, year)
                upload_history.pnl_email_sent = True
                db.session.commit()
                print("✅ Email sent and status updated.")
            except Exception as e:
                print(f"❌ Error sending email: {str(e)}")

    return jsonify({
        "message": "PNL forecast saved successfully",
        "filename": new_filename,
        "stored_in": "db"
    }), 200


