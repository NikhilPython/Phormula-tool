from datetime import datetime
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv, find_dotenv

from app.utils.forecasting_utils import process_forecasting
from app.utils.forecast_file_db_utils import load_file_from_db, save_file_to_db

dotenv_path = find_dotenv(filename=".env", usecwd=True)
load_dotenv(dotenv_path, override=True)

db_url = os.getenv("DATABASE_URL")
if not db_url:
    raise RuntimeError("DATABASE_URL is not set")

XLSX_MIME = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"

def _normalize_forecast_month(mv: str) -> str:
    mv = str(mv).strip().lower()
    month_map = {
        "jan": "january",
        "feb": "february",
        "mar": "march",
        "apr": "april",
        "may": "may",
        "jun": "june",
        "jul": "july",
        "aug": "august",
        "sep": "september",
        "oct": "october",
        "nov": "november",
        "dec": "december",
    }
    return month_map.get(mv, mv)

def generate_forecast_for_user(
    *,
    user_id: int,
    country: str,
    mv: str,
    year: str | int,
    send_email: bool = True,
):
    current_month = datetime.now().strftime("%b").lower()
    inventory_filename = f"inventory_forecast_{user_id}_{country}_{current_month}+2.xlsx"
    forecast_filename = f"forecasts_for_{user_id}_{country}.xlsx"

    stored_inv = load_file_from_db(
        user_id=user_id,
        country=country,
        filename=inventory_filename,
    )
    if stored_inv:
        return {
            "success": True,
            "cached": True,
            "filename": stored_inv.filename,
        }

    engine = create_engine(db_url)

    result = process_forecasting(user_id, country, mv, year, engine)

    # process_forecasting is returning a tuple like: (data, status_code)
    # Extract only the data dictionary.
    if isinstance(result, tuple):
        result = result[0]

    if not isinstance(result, dict):
        return {
            "success": False,
            "error": f"Forecast generation failed. Invalid result type: {type(result).__name__}"
        }
    
    if result.get("success") is False:
        return result

    if not result.get("inventory_bytes"):
        return {
            "success": False,
            "error": "Forecast generation failed (no output bytes)."
        }

    if result.get("forecast_bytes"):
        save_file_to_db(
            user_id=user_id,
            country=country,
            filename=result.get("forecast_filename", forecast_filename),
            file_bytes=result["forecast_bytes"],
            kind="forecasts_for",
            month=str(mv).lower(),
            year=str(year),
            content_type=XLSX_MIME,
        )

    save_file_to_db(
        user_id=user_id,
        country=country,
        filename=result.get("inventory_filename", inventory_filename),
        file_bytes=result["inventory_bytes"],
        kind="inventory_forecast",
        month=str(mv).lower(),
        year=str(year),
        content_type=XLSX_MIME,
    )

    stored_inv = load_file_from_db(
        user_id=user_id,
        country=country,
        filename=result.get("inventory_filename", inventory_filename),
    )
    if not stored_inv:
        return {
            "success": False,
            "error": "Saved forecast not found in DB after generation."
        }

    if send_email:
        try:
            from app.routes.forecast_routes import send_forecast_email
            send_forecast_email(user_id, stored_inv.filename, mv, year)
        except Exception as e:
            print(f"[EMAIL][WARN] send_forecast_email failed: {e}")

    return {
        "success": True,
        "cached": False,
        "filename": stored_inv.filename,
    }
