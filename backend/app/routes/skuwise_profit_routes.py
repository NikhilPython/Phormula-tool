from datetime import datetime
from flask import Blueprint, request, jsonify, current_app
import jwt
from sqlalchemy import create_engine, text, inspect, or_, func
from sqlalchemy.exc import OperationalError, TimeoutError as SQLAlchemyTimeoutError
import os
import base64
from config import Config
import json
import time
import traceback
from contextlib import contextmanager
from openai import OpenAI
from dotenv import load_dotenv
from app.routes.business_intelligence import get_sku_monthly_history
from app.utils.token_utils import get_effective_user_id_from_token
from app.utils.monthwise_ai_summary_utils import (
    run_prompt_2_strategy,
    build_sku_inventory_flags,
    fetch_inventory_aged_by_user,
    get_or_create_summary,
    get_or_create_global_summary,
    build_remaining_skus_time_series, 
)
from app.models.user_models import UserObjective, Product

load_dotenv()

SECRET_KEY = Config.SECRET_KEY
db_url = os.getenv('DATABASE_URL')
db_url1 = os.getenv('DATABASE_ADMIN_URL')
db_url2 = os.getenv('DATABASE_AMAZON_URL')
db_url_chatbot = os.getenv('DATABASE_CHATBOT_URL')
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

client = OpenAI(api_key=OPENAI_API_KEY)
skuwise_bp = Blueprint('skuwise_bp', __name__)

# Create engines once only.
# The product page calls several APIs concurrently, so a pool of only one
# connection caused intermittent QueuePool timeouts and HTTP 500 responses.
_COMMON_ENGINE_OPTIONS = {
    "pool_pre_ping": True,
    "pool_recycle": 900,
    "pool_timeout": 60,
    "pool_use_lifo": True,
}

user_engine = create_engine(
    db_url,
    pool_size=2,
    max_overflow=2,
    **_COMMON_ENGINE_OPTIONS,
)

admin_engine = create_engine(
    db_url1,
    pool_size=1,
    max_overflow=1,
    **_COMMON_ENGINE_OPTIONS,
)

amazon_engine = create_engine(
    db_url2,
    pool_size=2,
    max_overflow=1,
    **_COMMON_ENGINE_OPTIONS,
)

chatbot_engine = create_engine(
    db_url_chatbot,
    pool_size=1,
    max_overflow=1,
    **_COMMON_ENGINE_OPTIONS,
)


@contextmanager
def db_connect(engine, attempts=3, delay_seconds=0.25):
    """Acquire a SQLAlchemy connection with a short retry for transient errors."""
    conn = None
    last_error = None

    for attempt in range(1, attempts + 1):
        try:
            conn = engine.connect()
            break
        except (OperationalError, SQLAlchemyTimeoutError) as exc:
            last_error = exc
            if attempt == attempts:
                raise
            time.sleep(delay_seconds * attempt)

    if conn is None:
        raise last_error or RuntimeError("Unable to acquire database connection")

    try:
        yield conn
    finally:
        conn.close()


@contextmanager
def db_begin(engine, attempts=3, delay_seconds=0.25):
    """Open a transaction with retry while acquiring the connection."""
    conn = None
    transaction = None
    last_error = None

    for attempt in range(1, attempts + 1):
        try:
            conn = engine.connect()
            transaction = conn.begin()
            break
        except (OperationalError, SQLAlchemyTimeoutError) as exc:
            last_error = exc
            if conn is not None:
                conn.close()
                conn = None
            if attempt == attempts:
                raise
            time.sleep(delay_seconds * attempt)

    if conn is None or transaction is None:
        raise last_error or RuntimeError("Unable to start database transaction")

    try:
        yield conn
        transaction.commit()
    except Exception:
        transaction.rollback()
        raise
    finally:
        conn.close()


def log_route_exception(route_name, exc):
    """Write the complete traceback to Gunicorn/Flask logs."""
    try:
        current_app.logger.exception("%s failed: %s", route_name, exc)
    except Exception:
        traceback.print_exc()

def encode_file_to_base64(file_path):
    with open(file_path, 'rb') as file:
        return base64.b64encode(file.read()).decode('utf-8')


SKU_IMAGE_MARKETPLACE_BY_COUNTRY = {
    "us": "ATVPDKIKX0DER",
    "usa": "ATVPDKIKX0DER",
    "united states": "ATVPDKIKX0DER",
    "uk": "A1F83G8C2ARO7P",
    "gb": "A1F83G8C2ARO7P",
    "united kingdom": "A1F83G8C2ARO7P",
    "ca": "A2EUQ1WTGCTBG2",
    "canada": "A2EUQ1WTGCTBG2",
}


def clean_product_text(value):
    if value is None:
        return ""

    cleaned = str(value).strip()
    if not cleaned or cleaned.lower() in {"nan", "none", "null"}:
        return ""

    return cleaned


def product_name_key(value):
    return " ".join(clean_product_text(value).lower().split())


def is_aggregate_product_name(value):
    key = product_name_key(value)
    return key in {"total", "other", "others", "other sku", "other skus"}


def quote_ident(identifier):
    return '"' + str(identifier).replace('"', '""') + '"'


def sku_image_country_key(raw_country):
    country = clean_product_text(raw_country).lower()
    if country.startswith("amazon-"):
        country = country.replace("amazon-", "", 1)
    if country.startswith("global"):
        return "global"
    if country.startswith("uk"):
        return "uk"
    if country.startswith("us"):
        return "us"
    if country.startswith("ca"):
        return "ca"
    return country or "us"


def marketplace_id_for_sku_images(country):
    return SKU_IMAGE_MARKETPLACE_BY_COUNTRY.get(sku_image_country_key(country))


def sku_columns_for_image_country(country):
    country_key = sku_image_country_key(country)
    if country_key == "uk":
        return ["sku_uk", "sku_us", "sku_canada"]
    if country_key == "ca":
        return ["sku_canada", "sku_us", "sku_uk"]
    return ["sku_us", "sku_uk", "sku_canada"]


def first_saved_product_image(product):
    image_url = clean_product_text(getattr(product, "main_image_url", None))
    if image_url:
        return image_url

    image_urls = getattr(product, "image_urls", None)
    if isinstance(image_urls, list):
        for item in image_urls:
            if isinstance(item, str):
                image_url = clean_product_text(item)
            elif isinstance(item, dict):
                image_url = clean_product_text(
                    item.get("link") or item.get("url") or item.get("image_url")
                )
            else:
                image_url = ""

            if image_url:
                return image_url

    if isinstance(image_urls, dict):
        for key in ("main", "MAIN", "link", "url", "image_url"):
            image_url = clean_product_text(image_urls.get(key))
            if image_url:
                return image_url

    return ""


def first_existing_sku_image_column(columns, candidates):
    for column in candidates:
        if column in columns:
            return column
    return None


def product_catalog_select_expr(column_name, alias, default="NULL"):
    if not column_name:
        return f"{default} AS {alias}"
    return f"{quote_ident(column_name)} AS {alias}"


def build_sku_catalog_products(conn, table_name, country, search_query="", limit=None):
    inspector = inspect(conn)
    columns = {column["name"] for column in inspector.get_columns(table_name)}

    product_col = first_existing_sku_image_column(
        columns,
        ["product_name", "productName", "Product Name", "product-name"],
    )
    if not product_col:
        return []

    asin_col = first_existing_sku_image_column(columns, ["asin", "ASIN"])
    sku_col_candidates = ["sku", *sku_columns_for_image_country(country)]
    sku_cols = []
    for column in sku_col_candidates:
        if column in columns and column not in sku_cols:
            sku_cols.append(column)
    preferred_sku_col = sku_cols[0] if sku_cols else None
    order_metric_col = first_existing_sku_image_column(
        columns,
        ["net_sales", "Net Sales", "total_sales", "sales"],
    )

    where_clauses = [
        f"NULLIF(TRIM(CAST({quote_ident(product_col)} AS TEXT)), '') IS NOT NULL",
        f"LOWER(TRIM(CAST({quote_ident(product_col)} AS TEXT))) != 'total'",
    ]
    params = {}

    if search_query:
        searchable_columns = [product_col, asin_col, *sku_cols]
        searchable_columns = [
            column
            for index, column in enumerate(searchable_columns)
            if column and column not in searchable_columns[:index]
        ]
        search_parts = [
            f"LOWER(CAST({quote_ident(column)} AS TEXT)) LIKE LOWER(:search_query)"
            for column in searchable_columns
        ]
        where_clauses.append("(" + " OR ".join(search_parts) + ")")
        params["search_query"] = f"%{search_query}%"

    limit_clause = ""
    if limit:
        limit_clause = "LIMIT :limit"
        params["limit"] = int(limit)

    if order_metric_col:
        order_clause = (
            "ORDER BY "
            f"COALESCE(NULLIF(regexp_replace(CAST({quote_ident(order_metric_col)} AS TEXT), '[^0-9.-]', '', 'g'), '')::numeric, 0) "
            f"DESC, {quote_ident(product_col)} ASC"
        )
    else:
        order_clause = f"ORDER BY {quote_ident(product_col)} ASC"

    query = text(f"""
        SELECT
            {product_catalog_select_expr(product_col, "product_name")},
            {product_catalog_select_expr(asin_col, "asin")},
            {product_catalog_select_expr(preferred_sku_col, "sku")},
            {product_catalog_select_expr("sku_us" if "sku_us" in columns else None, "sku_us")},
            {product_catalog_select_expr("sku_uk" if "sku_uk" in columns else None, "sku_uk")},
            {product_catalog_select_expr("sku_canada" if "sku_canada" in columns else None, "sku_canada")}
        FROM {quote_ident(table_name)}
        WHERE {" AND ".join(where_clauses)}
        {order_clause}
        {limit_clause}
    """)

    rows = conn.execute(query, params).mappings().all()

    items = []
    seen = set()
    for row in rows:
        product_name = clean_product_text(row.get("product_name"))
        if not product_name or is_aggregate_product_name(product_name):
            continue

        item = {
            "product_name": product_name,
            "asin": clean_product_text(row.get("asin")).upper(),
            "sku": clean_product_text(row.get("sku")),
            "sku_us": clean_product_text(row.get("sku_us")),
            "sku_uk": clean_product_text(row.get("sku_uk")),
            "sku_canada": clean_product_text(row.get("sku_canada")),
        }

        identity = (
            product_name.lower(),
            item["sku"].lower(),
            item["asin"],
        )
        if identity in seen:
            continue

        seen.add(identity)
        items.append(item)

    return items


def normalize_product_catalog_country(country, home_currency=None):
    country = clean_product_text(country).lower()
    home_currency = clean_product_text(home_currency).lower()

    if country.startswith("amazon-"):
        country = country.replace("amazon-", "", 1)

    if country in {"usa", "united states"}:
        return "us"
    if country in {"gb", "united kingdom"}:
        return "uk"
    if country in {"ca", "canada"}:
        return "ca"

    if country.startswith("global"):
        if country in {"global_inr", "global_gbp", "global_cad"}:
            return country
        if home_currency == "inr":
            return "global_inr"
        if home_currency == "gbp":
            return "global_gbp"
        if home_currency == "cad":
            return "global_cad"
        return "global"

    if country.startswith("uk_usd"):
        return "uk_usd"
    if country.startswith("uk"):
        return "uk"
    if country.startswith("us"):
        return "us"

    return country or "us"


def normalize_product_catalog_range(value):
    value = clean_product_text(value).lower()

    if value in {"month", "monthly"}:
        return "monthly"
    if value in {"quarter", "quarterly", "qtd"}:
        return "quarterly"
    if value in {"year", "yearly", "ytd"}:
        return "yearly"

    return ""


def normalize_product_catalog_month(value):
    value = clean_product_text(value).lower()
    valid_months = {
        "january", "february", "march", "april", "may", "june",
        "july", "august", "september", "october", "november", "december",
    }

    if value in valid_months:
        return value

    return ""


def normalize_product_catalog_quarter(value):
    value = clean_product_text(value).lower()

    if value in {"q1", "quarter1", "1"}:
        return "1"
    if value in {"q2", "quarter2", "2"}:
        return "2"
    if value in {"q3", "quarter3", "3"}:
        return "3"
    if value in {"q4", "quarter4", "4"}:
        return "4"

    return ""


def product_catalog_period_table_candidates(user_id, country, range_type, month, quarter, year):
    user_id = str(user_id).strip()
    country = clean_product_text(country).lower()
    year = clean_product_text(year)

    if not user_id or not country or not year:
        return []

    if range_type == "monthly":
        month = normalize_product_catalog_month(month)
        if not month:
            return []

        base = f"skuwisemonthly_{user_id}_{country}_{month}{year}".lower()
        if country.startswith("global"):
            return [f"{base}_table", base]
        return [base, f"{base}_table"]

    if range_type == "quarterly":
        quarter_num = normalize_product_catalog_quarter(quarter)
        if not quarter_num:
            return []

        q_label = f"Q{quarter_num}"
        return [
            f"quarter{quarter_num}_{user_id}_{country}_{year}_table".lower(),
            f"skuwisequarter_{user_id}_{country}_{q_label}{year}".lower(),
        ]

    if range_type == "yearly":
        return [
            f"skuwiseyearly_{user_id}_{country}_{year}_table".lower(),
            f"skuwiseyear_{user_id}_{country}_{year}".lower(),
        ]

    return []


def resolve_product_catalog_table(inspector, user_id, country, range_type, month, quarter, year):
    period_requested = bool(range_type or clean_product_text(month) or clean_product_text(quarter) or clean_product_text(year))

    if range_type:
        normalized_range = normalize_product_catalog_range(range_type)
    elif clean_product_text(year):
        normalized_range = "yearly"
    else:
        normalized_range = ""

    if period_requested:
        candidates = product_catalog_period_table_candidates(
            user_id=user_id,
            country=country,
            range_type=normalized_range,
            month=month,
            quarter=quarter,
            year=year,
        )

        for table_name in candidates:
            if inspector.has_table(table_name):
                return table_name, True

        return None, True

    table_name = f"sku_{user_id}_data_table"
    if inspector.has_table(table_name):
        return table_name, False

    return None, False


def product_catalog_item_score(item):
    score = 0
    if clean_product_text(item.get("main_image_url")):
        score += 8
    if clean_product_text(item.get("asin")):
        score += 4
    if clean_product_text(item.get("sku")):
        score += 2
    if clean_product_text(item.get("title")):
        score += 1
    return score


def product_catalog_item_identity(item, key_mode="name"):
    if key_mode == "sku":
        for field in ("sku", "sku_us", "sku_uk", "sku_canada"):
            key = product_name_key(item.get(field))
            if key and key != "-":
                return f"sku:{key}"

    return f"name:{product_name_key(item.get('product_name'))}"


def dedupe_product_catalog_items(items, limit=None, key_mode="name"):
    by_name = {}

    for item in items or []:
        product_name = clean_product_text(item.get("product_name"))
        if not product_name or is_aggregate_product_name(product_name):
            continue

        key = product_catalog_item_identity(item, key_mode=key_mode)
        if not key or key == "name:":
            continue

        current = by_name.get(key)
        if current is None:
            by_name[key] = item
            continue

        merged = {**current}
        for field in (
            "asin",
            "sku",
            "sku_us",
            "sku_uk",
            "sku_canada",
            "title",
            "brand",
            "marketplace_id",
            "main_image_url",
        ):
            if not clean_product_text(merged.get(field)) and clean_product_text(item.get(field)):
                merged[field] = item.get(field)

        if product_catalog_item_score(item) > product_catalog_item_score(merged):
            merged = {**item, **merged}

        by_name[key] = merged

    result = list(by_name.values())
    if limit:
        return result[:int(limit)]
    return result


def enrich_products_with_saved_images(user_id, items, country):
    if not items:
        return items

    asins = sorted({
        clean_product_text(item.get("asin")).upper()
        for item in items
        if clean_product_text(item.get("asin"))
    })

    sku_values = sorted({
        clean_product_text(value).lower()
        for item in items
        for value in [
            item.get("sku"),
            item.get("sku_us"),
            item.get("sku_uk"),
            item.get("sku_canada"),
        ]
        if clean_product_text(value)
    })

    if not asins and not sku_values:
        return items

    product_filters = [Product.user_id == int(user_id)]
    marketplace_id = marketplace_id_for_sku_images(country)
    if marketplace_id:
        product_filters.append(Product.marketplace_id == marketplace_id)

    identity_filters = []
    if asins:
        identity_filters.append(func.upper(Product.asin).in_(asins))
    if sku_values:
        identity_filters.append(func.lower(Product.sku).in_(sku_values))

    if not identity_filters:
        return items

    saved_products = (
        Product.query.filter(*product_filters)
        .filter(or_(*identity_filters))
        .order_by(Product.updated_at.desc().nullslast(), Product.id.desc())
        .all()
    )

    product_by_asin = {}
    product_by_sku = {}
    for product in saved_products:
        asin_key = clean_product_text(product.asin).upper()
        sku_key = clean_product_text(product.sku).lower()

        if asin_key and asin_key not in product_by_asin:
            product_by_asin[asin_key] = product
        if sku_key and sku_key not in product_by_sku:
            product_by_sku[sku_key] = product

    enriched = []
    for item in items:
        sku_keys = [
            clean_product_text(item.get("sku")).lower(),
            clean_product_text(item.get("sku_us")).lower(),
            clean_product_text(item.get("sku_uk")).lower(),
            clean_product_text(item.get("sku_canada")).lower(),
        ]

        product = (
            product_by_asin.get(clean_product_text(item.get("asin")).upper())
            or next((product_by_sku[key] for key in sku_keys if key in product_by_sku), None)
        )

        if product:
            item = {
                **item,
                "asin": clean_product_text(item.get("asin")) or clean_product_text(product.asin),
                "sku": clean_product_text(item.get("sku")) or clean_product_text(product.sku),
                "title": clean_product_text(product.title),
                "brand": clean_product_text(product.brand),
                "marketplace_id": clean_product_text(product.marketplace_id),
                "main_image_url": first_saved_product_image(product),
            }

        enriched.append(item)

    return enriched


def get_countries_for_currency(currency):
    currency = currency.lower()

    if currency == "usd":
        return ["uk_usd", "us", "global"]
    elif currency == "inr":
        return ["uk", "us", "global_inr"]
    elif currency == "gbp":
        return ["uk", "us", "global_gbp"]
    elif currency == "cad":
        return ["uk", "us", "global_cad"]

    return ["uk", "us", "global"]


def get_conversion_rate(conn1, source_currency, target_currency, month, year):
    try:
        query = text("""
            SELECT conversion_rate
            FROM currency_conversion
            WHERE LOWER(user_currency) = :source
              AND LOWER(selected_currency) = :target
              AND LOWER(month) = :month
              AND year = :year
            LIMIT 1
        """)
        result = conn1.execute(query, {
            "source": source_currency.lower(),
            "target": target_currency.lower(),
            "month": month.lower(),
            "year": year
        }).fetchone()

        return float(result[0]) if result else 1.0
    except Exception:
        return 1.0


country_currency_map = {
    "uk": "gbp",
    "us": "usd",
    "global": None
}

inventory_marketplace_map = {
    "uk": "A1F83G8C2ARO7P",
    "us": "ATVPDKIKX0DER"
}

inventory_location_map = {
    "uk": "GB",
    "us": "US"
}

# =====================================================
# Product AI Summary constants
# =====================================================

PRODUCT_SUMMARY_QUARTER_START_MONTHS = {
    1: 1,   # Jan
    2: 4,   # Apr
    3: 7,   # Jul
    4: 10,  # Oct
}

PRODUCT_SUMMARY_MONTH_NAME_TO_NUM = {
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


def get_monthly_inventory_units(
    user_id,
    country,
    product_name,
    year,
    months_to_fetch,
    month_mapping,
    fallback_sku=None
):
    """
    Returns inventory units from monthwise_inventory.

    Matching logic:
    1. Match by product_name when product_name exists in monthwise_inventory.
    2. If monthwise_inventory.product_name is NULL/blank, fallback to matching:
       monthwise_inventory.msku = SKU resolved from sku_{user_id}_data_table.
    """

    sku_country = normalize_sku_country(country)

    if sku_country not in ("uk", "us"):
        return {}

    marketplace_id = inventory_marketplace_map.get(sku_country)
    location_code = inventory_location_map.get(sku_country)

    if not marketplace_id or not location_code:
        return {}

    month_numbers = [
        int(month_mapping[month])
        for month in months_to_fetch
    ]

    inventory_map = {}

    try:
        with db_connect(amazon_engine) as conn:
            for month_num in month_numbers:
                query = text("""
                    WITH last_date AS (
                        SELECT MAX(date::date) AS last_available_date
                        FROM monthwise_inventory
                        WHERE user_id = :user_id
                          AND marketplace_id = :marketplace_id
                          AND UPPER(TRIM(COALESCE(location, ''))) = :location_code
                          AND LOWER(TRIM(disposition)) = 'sellable'
                          AND EXTRACT(YEAR FROM date::date)::int = :year
                          AND EXTRACT(MONTH FROM date::date)::int = :month_num
                          AND (
                                LOWER(TRIM(product_name)) = LOWER(TRIM(:product_name))
                                OR (
                                    (product_name IS NULL OR TRIM(product_name) = '')
                                    AND :fallback_sku IS NOT NULL
                                    AND LOWER(TRIM(msku)) = LOWER(TRIM(:fallback_sku))
                                )
                          )
                    )
                    SELECT
                        COALESCE(SUM(ending_warehouse_balance), 0) AS inventory_units
                    FROM monthwise_inventory
                    WHERE user_id = :user_id
                      AND marketplace_id = :marketplace_id
                      AND UPPER(TRIM(COALESCE(location, ''))) = :location_code
                      AND LOWER(TRIM(disposition)) = 'sellable'
                      AND date::date = (SELECT last_available_date FROM last_date)
                      AND (
                            LOWER(TRIM(product_name)) = LOWER(TRIM(:product_name))
                            OR (
                                (product_name IS NULL OR TRIM(product_name) = '')
                                AND :fallback_sku IS NOT NULL
                                AND LOWER(TRIM(msku)) = LOWER(TRIM(:fallback_sku))
                            )
                      )
                """)

                row = conn.execute(query, {
                    "user_id": int(user_id),
                    "marketplace_id": marketplace_id,
                    "location_code": location_code,
                    "product_name": product_name,
                    "fallback_sku": fallback_sku,
                    "year": int(year),
                    "month_num": int(month_num)
                }).fetchone()

                inventory_map[(int(year), int(month_num))] = float(
                    row.inventory_units or 0
                )

        return inventory_map

    except Exception as e:
        return {}
    
def get_monthly_inventory_units_for_other_skus(
    conn,
    user_id,
    country,
    product_names,
    year,
    months_to_fetch,
    month_mapping,
):
    """
    Inventory for Other SKUs.

    For global:
    inventory = UK inventory + US inventory for all products inside Other SKUs.

    For uk/us:
    inventory = that country inventory for all products inside Other SKUs.
    """

    inventory_units_map = {}

    product_names = [
        str(p).strip()
        for p in (product_names or [])
        if str(p).strip()
    ]

    if not product_names:
        return inventory_units_map

    raw_country = (country or "").strip().lower()

    if raw_country.startswith("global"):
        inventory_countries = ["uk", "us"]
    else:
        normalized_country = normalize_sku_country(raw_country)

        if normalized_country not in ("uk", "us"):
            return inventory_units_map

        inventory_countries = [normalized_country]

    for inv_country in inventory_countries:
        for product_name in product_names:
            fallback_sku = resolve_product_sku_for_country(
                conn=conn,
                user_id=user_id,
                product_name=product_name,
                country=inv_country
            )

            product_inventory_map = get_monthly_inventory_units(
                user_id=user_id,
                country=inv_country,
                product_name=product_name,
                year=year,
                months_to_fetch=months_to_fetch,
                month_mapping=month_mapping,
                fallback_sku=fallback_sku
            )

            for key, value in product_inventory_map.items():
                inventory_units_map[key] = inventory_units_map.get(key, 0) + float(value or 0)

    return inventory_units_map


def normalize_sku_country(country):
    country = (country or "").lower()

    if country.startswith("uk"):
        return "uk"

    if country.startswith("us"):
        return "us"

    return country


def resolve_product_sku_for_country(conn, user_id, product_name, country):
    """
    ProductwisePerformance receives product_name,
    but Other SKUs aggregation must exclude by SKU.
    """
    sku_country = normalize_sku_country(country)

    if sku_country not in ("uk", "us"):
        return None

    table_name = f"sku_{user_id}_data_table"
    sku_column = "sku_uk" if sku_country == "uk" else "sku_us"

    query = text(f"""
        SELECT {sku_column}
        FROM "{table_name}"
        WHERE LOWER(TRIM(product_name)) = LOWER(TRIM(:product_name))
        OR LOWER(TRIM({sku_column})) = LOWER(TRIM(:product_name))
        LIMIT 1
    """)

    row = conn.execute(query, {"product_name": product_name}).fetchone()

    return row[0] if row and row[0] else None


def get_monthly_total_net_sales_and_profit(conn, inspector, all_tables, user_id, country, month_name, year):
    """
    Returns total account net_sales and profit for a country/month/year.
    Supports uk/us and global/global_inr/global_gbp/global_cad tables.
    Used to calculate Other SKUs sales_mix and profit_mix.
    """

    raw_country = (country or "").strip().lower()
    is_global = raw_country.startswith("global")

    table_country = raw_country if is_global else normalize_sku_country(raw_country)

    if not is_global and table_country not in ("uk", "us"):
        return 0.0, 0.0

    if is_global:
        table_pattern = f"skuwisemonthly_{user_id}_{table_country}_{month_name}{year}_table"
    else:
        table_pattern = f"skuwisemonthly_{user_id}_{table_country}_{month_name}{year}"

    matching_tables = [
        table for table in all_tables
        if table.lower() == table_pattern.lower()
    ]

    if not matching_tables:
        return 0.0, 0.0

    table_name = matching_tables[0]

    try:
        columns = [col["name"] for col in inspector.get_columns(table_name)]

        if "net_sales" not in columns:
            return 0.0, 0.0

        if "cm1_profit" in columns:
            profit_col = "cm1_profit"
        elif "profit" in columns:
            profit_col = "profit"
        else:
            return 0.0, 0.0

        # Prefer Total row if available
        if "product_name" in columns:
            total_query = text(f"""
                SELECT net_sales, "{profit_col}"
                FROM "{table_name}"
                WHERE LOWER(TRIM(product_name)) = 'total'
                LIMIT 1
            """)

            total_row = conn.execute(total_query).fetchone()

            if total_row:
                return float(total_row[0] or 0), float(total_row[1] or 0)

        # Fallback: sum all non-total rows
        query = text(f"""
            SELECT
                COALESCE(SUM(net_sales), 0) AS total_net_sales,
                COALESCE(SUM("{profit_col}"), 0) AS total_profit
            FROM "{table_name}"
            WHERE LOWER(TRIM(COALESCE(product_name, ''))) != 'total'
        """)

        row = conn.execute(query).fetchone()

        return float(row[0] or 0), float(row[1] or 0)

    except Exception as e:
        return 0.0, 0.0

def get_exact_other_skus_month_row(
    conn,
    inspector,
    all_tables,
    user_id,
    country,
    month_name,
    month_num,
    year,
    product_names,
    home_currency,
):
    """
    Aggregate only the exact product names shown in Other SKUs chips.
    Supports uk/us and global/global_inr/global_gbp/global_cad tables.
    """

    raw_country = (country or "").strip().lower()
    is_global = raw_country.startswith("global")

    table_country = raw_country if is_global else normalize_sku_country(raw_country)

    if not is_global and table_country not in ("uk", "us"):
        return None

    if not product_names:
        return None

    if is_global:
        table_pattern = f"skuwisemonthly_{user_id}_{table_country}_{month_name}{year}_table"
    else:
        table_pattern = f"skuwisemonthly_{user_id}_{table_country}_{month_name}{year}"

    matching_tables = [
        table for table in all_tables
        if table.lower() == table_pattern.lower()
    ]

    if not matching_tables:
        return {
            "month": month_name.capitalize(),
            "month_num": month_num,
            "year": int(year),
            "net_sales": 0,
            "quantity": 0,
            "profit": 0,
            "asp": 0,
            "sales_mix": 0,
            "profit_mix": 0,
            "unit_wise_profitability": 0,
            "cm2_profit": 0,
            "cm2_profit_percentage": 0,
            "cm2_profit_per_unit": 0,
        }

    table_name = matching_tables[0]

    try:
        columns = [col["name"] for col in inspector.get_columns(table_name)]

        if "product_name" not in columns or "net_sales" not in columns:
            return None

        if "total_quantity" in columns:
            quantity_col = "total_quantity"
        elif "quantity" in columns:
            quantity_col = "quantity"
        else:
            return None

        if "cm1_profit" in columns:
            profit_col = "cm1_profit"
        elif "profit" in columns:
            profit_col = "profit"
        else:
            return None

        placeholders = ", ".join([f":p{i}" for i in range(len(product_names))])

        params = {
            f"p{i}": product_names[i].strip().lower()
            for i in range(len(product_names))
        }

        cm2_select = (
            'COALESCE(SUM(cm2_profit), 0) AS cm2_profit'
            if "cm2_profit" in columns
            else '0 AS cm2_profit'
        )

        query = text(f"""
            SELECT
                COALESCE(SUM(net_sales), 0) AS net_sales,
                COALESCE(SUM("{quantity_col}"), 0) AS total_quantity,
                COALESCE(SUM("{profit_col}"), 0) AS profit,
                {cm2_select}
            FROM "{table_name}"
            WHERE LOWER(TRIM(product_name)) IN ({placeholders})
        """)

        row = conn.execute(query, params).fetchone()

        other_net_sales = float(row[0] or 0)
        other_quantity = float(row[1] or 0)
        other_profit = float(row[2] or 0)
        other_cm2_profit = float(row[3] or 0)

        conversion_rate = 1.0

        # Only UK/US source tables need conversion.
        # Global tables are already stored in selected global currency table.
        if not is_global:
            source_currency = country_currency_map.get(table_country)
            target_currency = (home_currency or "").lower()

            if source_currency and target_currency:
                with db_connect(admin_engine) as conn1:
                    conversion_rate = get_conversion_rate(
                        conn1,
                        source_currency,
                        target_currency,
                        month_name,
                        year
                    )

            other_net_sales *= conversion_rate
            other_profit *= conversion_rate
            other_cm2_profit *= conversion_rate

        asp = other_net_sales / other_quantity if other_quantity else 0
        unit_wise_profitability = other_profit / other_quantity if other_quantity else 0
        cm2_profit_percentage = (
            (other_cm2_profit / other_net_sales) * 100
            if other_net_sales
            else 0
        )
        cm2_profit_per_unit = (
            other_cm2_profit / other_quantity
            if other_quantity
            else 0
        )

        total_net_sales, total_profit = get_monthly_total_net_sales_and_profit(
            conn=conn,
            inspector=inspector,
            all_tables=all_tables,
            user_id=user_id,
            country=table_country,
            month_name=month_name,
            year=year
        )

        sales_mix = (
            (other_net_sales / total_net_sales) * 100
            if total_net_sales
            else 0
        )

        profit_mix = (
            (other_profit / total_profit) * 100
            if total_profit
            else 0
        )

        return {
            "month": month_name.capitalize(),
            "month_num": month_num,
            "year": int(year),
            "net_sales": other_net_sales,
            "quantity": other_quantity,
            "profit": other_profit,
            "asp": asp,
            "sales_mix": sales_mix,
            "profit_mix": profit_mix,
            "unit_wise_profitability": round(unit_wise_profitability, 2),
            "cm2_profit": round(other_cm2_profit, 2),
            "cm2_profit_percentage": round(cm2_profit_percentage, 2),
            "cm2_profit_per_unit": round(cm2_profit_per_unit, 2),
        }

    except Exception as e:
        return None

@skuwise_bp.route('/ProductwisePerformance', methods=['POST'])
def productwise_performance():
    try:
        # Authentication
        auth_header = request.headers.get('Authorization')
        if not auth_header or not auth_header.startswith('Bearer '):
            return jsonify({'error': 'Authorization token missing or invalid'}), 401

        token = auth_header.split(' ')[1]
        try:
            payload, user_id, member_id = get_effective_user_id_from_token(token)
            user_id = str(payload.get('user_id'))
        except jwt.ExpiredSignatureError:
            return jsonify({'error': 'Token expired'}), 401
        except jwt.InvalidTokenError:
            return jsonify({'error': 'Invalid token'}), 401

        # Request data
        data = request.get_json() or {}

        product_name = data.get('product_name')
        time_range = data.get('time_range', 'Yearly')
        year = int(data.get('year', datetime.now().year))
        quarter = data.get('quarter')
        home_currency = (data.get('home_currency') or 'USD').lower()

        # Same completed-month cutoff must be sent for current and previous year.
        # Example: July 2026 => comparison_end_month = 6.
        comparison_end_month = data.get('comparison_end_month')

        if comparison_end_month is not None:
            try:
                comparison_end_month = int(comparison_end_month)
            except (TypeError, ValueError):
                return jsonify({
                    'error': 'comparison_end_month must be a number between 1 and 12'
                }), 400

            if comparison_end_month < 1 or comparison_end_month > 12:
                return jsonify({
                    'error': 'comparison_end_month must be between 1 and 12'
                }), 400

        # ✅ NEW: exact products shown under Other SKUs chips
        other_sku_product_names = data.get("other_sku_product_names") or []
        other_sku_product_names = [
            str(p).strip()
            for p in other_sku_product_names
            if str(p).strip()
        ]

        requested_countries = get_countries_for_currency(home_currency)

        if not product_name:
            return jsonify({'error': 'Product name is required'}), 400

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

        all_months = list(month_mapping.keys())

        if time_range == 'Quarterly' and quarter:
            months_to_fetch = quarter_months.get(str(quarter), [])
        else:
            months_to_fetch = all_months

        # -----------------------------------------------------
        # Exclude running month and future months
        # -----------------------------------------------------
        if comparison_end_month is not None:
            # Use exactly the same cutoff for both current and previous year requests.
            #
            # Example:
            # Current request:  year=2026, comparison_end_month=6
            # Previous request: year=2025, comparison_end_month=6
            #
            # Both return only months up to June.
            months_to_fetch = [
                month
                for month in months_to_fetch
                if int(month_mapping[month]) <= comparison_end_month
            ]

        else:
            # Backend fallback when frontend does not send comparison_end_month.
            now = datetime.now()

            if int(year) == now.year:
                last_completed_month = now.month - 1

                months_to_fetch = [
                    month
                    for month in months_to_fetch
                    if int(month_mapping[month]) <= last_completed_month
                ]

        result_data = {}
        other_skus_graph_data = {}

        with db_connect(user_engine) as conn:
            inspector = inspect(conn)
            all_tables = inspector.get_table_names()

            # Iterate over requested countries
            for country in requested_countries:
                country_data = []

                sku_country = normalize_sku_country(country)

                fallback_sku = None

                if country.lower().startswith("global"):
                    # Global inventory = UK product inventory + US product inventory
                    inventory_units_map = {}

                    for inv_country in ("uk", "us"):
                        inv_fallback_sku = resolve_product_sku_for_country(
                            conn=conn,
                            user_id=user_id,
                            product_name=product_name,
                            country=inv_country
                        )

                        inv_map = get_monthly_inventory_units(
                            user_id=user_id,
                            country=inv_country,
                            product_name=product_name,
                            year=year,
                            months_to_fetch=months_to_fetch,
                            month_mapping=month_mapping,
                            fallback_sku=inv_fallback_sku
                        )

                        for key, value in inv_map.items():
                            inventory_units_map[key] = inventory_units_map.get(key, 0) + float(value or 0)

                else:
                    if sku_country in ("uk", "us"):
                        fallback_sku = resolve_product_sku_for_country(
                            conn=conn,
                            user_id=user_id,
                            product_name=product_name,
                            country=sku_country
                        )

                    inventory_units_map = get_monthly_inventory_units(
                        user_id=user_id,
                        country=sku_country,
                        product_name=product_name,
                        year=year,
                        months_to_fetch=months_to_fetch,
                        month_mapping=month_mapping,
                        fallback_sku=fallback_sku
                    )

                for month in months_to_fetch:
                    month_num = month_mapping[month]

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
                    total_sales_mix = 0.0
                    total_profit_mix = 0.0
                    total_cost_of_unit_sold = 0.0
                    total_cm2_profit = 0.0

                    table_found = False
                    conversion_rate_applied = None

                    for table_pattern in table_patterns:
                        matching_tables = [
                            table for table in all_tables
                            if table.lower() == table_pattern.lower()
                        ]

                        if not matching_tables:
                            continue

                        table_name = matching_tables[0]

                        try:
                            columns = [
                                col['name'] for col in inspector.get_columns(table_name)
                            ]

                            required_cols = {
                                'product_name',
                                'net_sales',
                                'total_quantity',
                                'profit',
                                'asp',
                                'sales_mix',
                                'profit_mix',
                                'cost_of_unit_sold',
                            }

                            if not required_cols.issubset(columns):
                                continue

                            cm2_select = (
                                'cm2_profit'
                                if 'cm2_profit' in columns
                                else '0 AS cm2_profit'
                            )

                            has_sku_col = 'sku' in columns

                            where_condition = """
                                LOWER(TRIM(product_name)) = LOWER(TRIM(:product_name))
                            """

                            if has_sku_col:
                                where_condition = """
                                    LOWER(TRIM(product_name)) = LOWER(TRIM(:product_name))
                                    OR LOWER(TRIM(sku)) = LOWER(TRIM(:product_name))
                                """

                            query = text(f"""
                                SELECT
                                    net_sales,
                                    total_quantity,
                                    profit,
                                    asp,
                                    sales_mix,
                                    profit_mix,
                                    cost_of_unit_sold,
                                    {cm2_select}
                                FROM "{table_name}"
                                WHERE {where_condition}
                            """)

                            rows = conn.execute(
                                query, {'product_name': product_name}
                            ).fetchall()

                            if not rows:
                                continue

                            table_found = True

                            table_sales = sum(float(row[0] or 0) for row in rows)
                            table_total_quantity = sum(float(row[1] or 0) for row in rows)
                            table_profit = sum(float(row[2] or 0) for row in rows)
                            table_sales_mix = sum(float(row[4] or 0) for row in rows)
                            table_profit_mix = sum(float(row[5] or 0) for row in rows)
                            table_cost_of_unit_sold = sum(float(row[6] or 0) for row in rows)
                            table_cm2_profit = sum(float(row[7] or 0) for row in rows)

                            if country.lower() in ('uk', 'us'):
                                source_currency = country_currency_map.get(country.lower())
                                target_currency = home_currency.lower()

                                if source_currency and target_currency:
                                    with db_connect(admin_engine) as conn1:
                                        conversion_rate = get_conversion_rate(
                                            conn1,
                                            source_currency,
                                            target_currency,
                                            month,
                                            year
                                        )
                                else:
                                    conversion_rate = 1.0

                                table_sales *= conversion_rate
                                table_profit *= conversion_rate
                                # table_asp *= conversion_rate
                                table_cost_of_unit_sold *= conversion_rate
                                table_cm2_profit *= conversion_rate

                                conversion_rate_applied = conversion_rate
                            else:
                                conversion_rate_applied = 1.0

                            total_sales += table_sales
                            total_quantity += table_total_quantity
                            total_profit += table_profit
                            # total_asp += table_asp
                            total_sales_mix += table_sales_mix
                            total_profit_mix += table_profit_mix
                            total_cost_of_unit_sold += table_cost_of_unit_sold
                            total_cm2_profit += table_cm2_profit

                        except Exception as e:
                            continue

                    total_asp = (
                        total_sales / total_quantity
                        if total_quantity > 0
                        else 0.0
                    )

                    gross_margin = (
                        (total_profit / total_sales) * 100
                        if total_sales > 0
                        else 0.0
                    )

                    unit_wise_profitability = (
                        total_profit / total_quantity
                        if total_quantity > 0
                        else 0.0
                    )

                    cm2_profit_percentage = (
                        (total_cm2_profit / total_sales) * 100
                        if total_sales > 0
                        else 0.0
                    )

                    cm2_profit_per_unit = (
                        total_cm2_profit / total_quantity
                        if total_quantity > 0
                        else 0.0
                    )

                    inventory_units = inventory_units_map.get(
                        (int(year), int(month_num)),
                        0
                    )

                    country_data.append({
                        'month': month.capitalize(),
                        'month_num': month_num,
                        'net_sales': total_sales if table_found else 0.0,
                        'quantity': total_quantity if table_found else 0,
                        'inventory_units': inventory_units,
                        'profit': total_profit if table_found else 0.0,
                        'asp': total_asp if table_found else 0.0,
                        'sales_mix': total_sales_mix if table_found else 0.0,
                        'profit_mix': total_profit_mix if table_found else 0.0,
                        'gross_margin': gross_margin,
                        'unit_wise_profitability': round(unit_wise_profitability, 2) if table_found else 0.0,
                        'cm2_profit': round(total_cm2_profit, 2) if table_found else 0.0,
                        'cm2_profit_percentage': round(cm2_profit_percentage, 2) if table_found else 0.0,
                        'cm2_profit_per_unit': round(cm2_profit_per_unit, 2) if table_found else 0.0,
                        'year': year,
                        'conversion_rate_applied': conversion_rate_applied,
                    })

                country_data.sort(key=lambda x: x['month_num'])
                result_data[country] = country_data

                # =====================================================
                # Other SKUs graph data
                # Uses monthwise_ai_summary_utils aggregate series
                # =====================================================
                try:
                    sku_country = normalize_sku_country(country)

                    # ✅ BEST PATH:
                    # If frontend sends exact Other SKUs chip names,
                    # aggregate those products directly.
                    if other_sku_product_names and (
                        sku_country in ("uk", "us") or country.lower().startswith("global")
                    ):
                        exact_other_rows = []

                        graph_country = country.lower() if country.lower().startswith("global") else sku_country

                        # ✅ Inventory for Other SKUs only
                        # For global this becomes UK + US inventory for all chip products.
                        other_skus_inventory_units_map = get_monthly_inventory_units_for_other_skus(
                            conn=conn,
                            user_id=user_id,
                            country=graph_country,
                            product_names=other_sku_product_names,
                            year=year,
                            months_to_fetch=months_to_fetch,
                            month_mapping=month_mapping,
                        )

                        for month in months_to_fetch:
                            month_num = month_mapping[month]

                            row = get_exact_other_skus_month_row(
                                conn=conn,
                                inspector=inspector,
                                all_tables=all_tables,
                                user_id=user_id,
                                country=graph_country,
                                month_name=month,
                                month_num=month_num,
                                year=year,
                                product_names=other_sku_product_names,
                                home_currency=home_currency,
                            )

                            if row:
                                row["inventory_units"] = other_skus_inventory_units_map.get(
                                    (int(year), int(month_num)),
                                    0
                                )

                                exact_other_rows.append(row)

                        exact_other_rows.sort(key=lambda x: x["month_num"])
                        other_skus_graph_data[country] = exact_other_rows
                        continue

                    # fallback old behavior
                    selected_sku = resolve_product_sku_for_country(
                        conn=conn,
                        user_id=user_id,
                        product_name=product_name,
                        country=sku_country
                    )

                    if selected_sku and sku_country in ("uk", "us"):
                        if months_to_fetch:
                            anchor_month = int(month_mapping[months_to_fetch[-1]])
                        else:
                            anchor_month = 0

                        if anchor_month <= 0:
                            other_skus_graph_data[country] = []
                            continue

                        other_series = build_remaining_skus_time_series(
                            user_id=int(user_id),
                            country=sku_country,
                            focus_skus=[str(selected_sku)],
                            anchor_year=int(year),
                            anchor_month=anchor_month,
                            months=24
                        )

                        allowed_month_nums = {
                            month_mapping[m] for m in months_to_fetch
                        }

                        other_rows = []

                        for item in other_series:
                            item_year = int(item["year"])
                            item_month_num = str(item["month"]).zfill(2)

                            if item_year != int(year):
                                continue

                            if item_month_num not in allowed_month_nums:
                                continue

                            month_name = datetime(
                                item_year,
                                int(item["month"]),
                                1
                            ).strftime("%B").lower()

                            other_net_sales = float(item.get("net_sales") or 0)
                            other_profit = float(item.get("cm1_profit") or 0)

                            total_net_sales, total_profit = get_monthly_total_net_sales_and_profit(
                                conn=conn,
                                inspector=inspector,
                                all_tables=all_tables,
                                user_id=user_id,
                                country=sku_country,
                                month_name=month_name,
                                year=item_year
                            )

                            sales_mix = (
                                (other_net_sales / total_net_sales) * 100
                                if total_net_sales
                                else 0
                            )

                            profit_mix = (
                                (other_profit / total_profit) * 100
                                if total_profit
                                else 0
                            )

                            other_rows.append({
                                "month": datetime(
                                    item_year,
                                    int(item["month"]),
                                    1
                                ).strftime("%B"),
                                "month_num": item_month_num,
                                "year": item_year,

                                "net_sales": other_net_sales,
                                "quantity": item.get("units") or 0,
                                "profit": other_profit,
                                "asp": item.get("asp") or 0,

                                # ✅ calculated here
                                "sales_mix": sales_mix,
                                "profit_mix": profit_mix,

                                "unit_wise_profitability": item.get("unit_wise_profitability") or 0,
                                "cm2_profit": item.get("cm2_profit") or 0,
                                "cm2_profit_percentage": item.get("cm2_profit_percentage") or 0,
                                "cm2_profit_per_unit": item.get("cm2_profit_per_unit") or 0,
                            })

                        other_skus_graph_data[country] = other_rows
                    else:
                        other_skus_graph_data[country] = []

                except Exception as e:
                    other_skus_graph_data[country] = []

        return jsonify({
            'success': True,
            'product_name': product_name,
            'time_range': time_range,
            'year': year,
            'quarter': quarter if time_range == 'Quarterly' else None,

            # Comparison range used by backend
            'comparison_end_month': comparison_end_month,
            'months_included': months_to_fetch,

            # Selected product graph data
            'data': result_data,

            # Other SKUs aggregate graph data
            'other_skus_graph_data': other_skus_graph_data,

            'available_countries': list(result_data.keys())
        })

    except Exception as e:
        log_route_exception('ProductwisePerformance', e)
        status_code = 503 if isinstance(e, (OperationalError, SQLAlchemyTimeoutError)) else 500
        return jsonify({
            'success': False,
            'error': f'Internal server error: {str(e)}',
            'retryable': status_code == 503
        }), status_code


@skuwise_bp.route('/Product_search', methods=['GET'])
def product_search():
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

    search_query = request.args.get('query', '').strip()
    country = normalize_product_catalog_country(
        request.args.get('country', 'us'),
        request.args.get('homeCurrency') or request.args.get('home_currency'),
    )
    range_type = normalize_product_catalog_range(
        request.args.get('range') or request.args.get('period') or request.args.get('time_range')
    )
    month = request.args.get('month', '').strip().lower()
    quarter = request.args.get('quarter', '').strip()
    year = request.args.get('year', '').strip()

    if not search_query:
        return jsonify({'error': 'Search query is required'}), 400

    try:
        with db_connect(user_engine) as conn:
            inspector = inspect(conn)
            table_name, period_requested = resolve_product_catalog_table(
                inspector=inspector,
                user_id=user_id,
                country=country,
                range_type=range_type,
                month=month,
                quarter=quarter,
                year=year,
            )

            if not table_name:
                return jsonify({
                    'products': [],
                    'product_names': [],
                    'source_table': None,
                    'period_filtered': period_requested,
                }), 200

            products = build_sku_catalog_products(
                conn=conn,
                table_name=table_name,
                country=country,
                search_query=search_query,
                limit=10,
            )

        products = enrich_products_with_saved_images(user_id, products, country)
        products = dedupe_product_catalog_items(
            products,
            limit=10,
            key_mode="sku" if period_requested else "name",
        )

        return jsonify({
            'products': products,
            'product_names': products,
            'source_table': table_name,
            'period_filtered': period_requested,
        }), 200

    except Exception as e:
        return jsonify({'error': f'Error searching products: {str(e)}'}), 500


@skuwise_bp.route('/Product_names', methods=['GET'])
def product_names():
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

    country = normalize_product_catalog_country(
        request.args.get('country', 'us'),
        request.args.get('homeCurrency') or request.args.get('home_currency'),
    )
    range_type = normalize_product_catalog_range(
        request.args.get('range') or request.args.get('period') or request.args.get('time_range')
    )
    month = request.args.get('month', '').strip().lower()
    quarter = request.args.get('quarter', '').strip()
    year = request.args.get('year', '').strip()

    try:
        with db_connect(user_engine) as conn:
            inspector = inspect(conn)
            table_name, period_requested = resolve_product_catalog_table(
                inspector=inspector,
                user_id=user_id,
                country=country,
                range_type=range_type,
                month=month,
                quarter=quarter,
                year=year,
            )

            if not table_name:
                return jsonify({
                    'product_names': [],
                    'products': [],
                    'source_table': None,
                    'period_filtered': period_requested,
                }), 200

            products = build_sku_catalog_products(
                conn=conn,
                table_name=table_name,
                country=country,
            )

        products = enrich_products_with_saved_images(user_id, products, country)
        products = dedupe_product_catalog_items(
            products,
            key_mode="sku" if period_requested else "name",
        )

        return jsonify({
            'product_names': products,
            'products': products,
            'source_table': table_name,
            'period_filtered': period_requested,
        }), 200

    except Exception as e:
        return jsonify({'error': str(e)}), 500
    
    


############################################################################################################################################
##DJ's NEW CODE FOR GROWTH + AI INSIGHTS Donot change anything here unless discussed with DJ##

# ===== Growth + AI Insights (NEW PART) =====

CURRENT_MONTH = datetime.now().month
CURRENT_YEAR = datetime.now().year

def month_key(month, year):
    return f"{month[:3].lower()}_{year}"


def calculate_growth(new, old):
    if old == 0:
        return 0.0
    return round(((new - old) / old) * 100, 2)


def get_latest_two_tables(all_tables, user_id, country):
    candidates = []
    now = datetime.now()

    for table in all_tables:
        prefix = f"skuwisemonthly_{user_id}_{country}_"
        if not table.lower().startswith(prefix.lower()):
            continue

        try:
            suffix = table.replace(prefix, "").replace("_table", "")
            month = ''.join(filter(str.isalpha, suffix))
            year = int(''.join(filter(str.isdigit, suffix)))
            month_num = datetime.strptime(month.capitalize(), "%B").month

            # ❌ EXCLUDE CURRENT MONTH (MTD)
            if year == now.year and month_num == now.month:
                continue

            candidates.append({
                "table": table,
                "month": month,
                "year": year,
                "month_num": month_num
            })
        except:
            continue

    # Sort by year + month descending
    candidates.sort(key=lambda x: (x["year"], x["month_num"]), reverse=True)

    return candidates[:2]



def fetch_metrics(conn, table_name, product_name=None, sku=None):
    where_clause = ""
    params = {}

    if product_name:
        where_clause = "LOWER(TRIM(product_name)) = LOWER(TRIM(:product_name))"
        params["product_name"] = product_name
    else:
        where_clause = "sku = :sku"
        params["sku"] = sku

    query = text(f"""
        SELECT
            quantity,
            net_sales,
            profit,
            asp
        FROM "{table_name}"
        WHERE {where_clause}
        LIMIT 1
    """)

    row = conn.execute(query, params).fetchone()

    if not row:
        return {
            "quantity": 0.0,
            "asp": 0.0,
            "net_sales": 0.0,
            "sales_mix": 0.0,
            "unit_wise_profitability": 0.0,
            "profit": 0.0
        }

    quantity = float(row.quantity or 0)
    net_sales = float(row.net_sales or 0)
    profit = float(row.profit or 0)
    asp = float(row.asp or 0)

    unit_wise_profitability = profit / quantity if quantity else 0
    sales_mix = net_sales  # kept explicit for AI prompt

    return {
        "quantity": quantity,
        "asp": asp,
        "net_sales": net_sales,
        "sales_mix": sales_mix,
        "unit_wise_profitability": unit_wise_profitability,
        "profit": profit
    }


def generate_ai_insights(prompt):
    try:
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": "You are a senior ecommerce analyst. Use plain text and bullet points only. Do not use Markdown formatting."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.2,
            max_tokens=900
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        return None


@skuwise_bp.route('/ProductwiseGrowthAI', methods=['POST'])
def productwise_growth_ai():
    try:

        # ==============================
        # AUTH
        # ==============================
        auth_header = request.headers.get('Authorization')
        if not auth_header or not auth_header.startswith('Bearer '):
            return jsonify({'error': 'Unauthorized'}), 401

        token = auth_header.split(' ')[1]
        payload, user_id, member_id = get_effective_user_id_from_token(token)
        user_id = str(payload['user_id'])

        data = request.get_json()

        product_name = data.get('product_name')
        sku = data.get('sku')
        country = data.get('country', 'us')

        country = (country or "us").strip().lower()

        if country.startswith("uk"):
            country = "uk"
        elif country.startswith("us"):
            country = "us"
        elif country.startswith("global"):
            country = "global"

        if not product_name and not sku:
            return jsonify({'error': 'product_name or sku required'}), 400


        # ==============================
        # LOAD OBJECTIVE
        # ==============================
        user_objective_row = UserObjective.query.filter_by(
            user_id=int(user_id),
            country=country
        ).first()

        if user_objective_row:
            objective_v2 = {
                "growth_intent": user_objective_row.growth_intent,
                "profit_priority": user_objective_row.profit_priority,
                "inventory_clearance_priority": user_objective_row.inventory_clearance_priority,
                "business_context": user_objective_row.business_context,
                "country": country,
                "time_horizon": "1_month"
            }
        else:
            objective_v2 = {
                "growth_intent": "balanced",
                "profit_priority": "protect_growth",
                "inventory_clearance_priority": False,
                "business_context": None,
                "country": country,
                "time_horizon": "1_month"
            }


        # ==============================
        # RESOLVE SKU
        # ==============================

        if country == "global":
            key = product_name
        else:

            if not sku:
                table_name = f"sku_{user_id}_data_table"

                if country == "uk":
                    sku_column = "sku_uk"
                elif country == "us":
                    sku_column = "sku_us"
                else:
                    return jsonify({'error': 'Unsupported country'}), 400

                resolve_query = text(f"""
                    SELECT {sku_column}
                    FROM {table_name}
                    WHERE LOWER(TRIM(product_name)) = LOWER(TRIM(:product_name))
                    LIMIT 1
                """)

                with db_connect(user_engine) as conn:
                    result = conn.execute(
                        resolve_query,
                        {"product_name": product_name}
                    ).fetchone()

                if not result:
                    return jsonify({'error': 'SKU not found'}), 404

                key = result[0]

            else:
                key = sku


        # ==============================
        # GET SUMMARY ENGINE OUTPUT
        # ==============================
        country_actions = {}

        if country == "global":
            # ✅ GLOBAL FLOW:
            # Use the new global architecture.
            # Do not pass target_sku because global works product-wise through mapped US/UK journeys.
            summary_result = get_or_create_global_summary(
                user_id=int(user_id),
                marketplace_id=None,
                period=None,
                timeline=None,
                year=None,
                objective=objective_v2,
                target_sku=None,
                force_regenerate=False
            )

            global_ai = summary_result.get("global_ai") or {}
            product_items = global_ai.get("product_journey_comparison") or []

            matched_product = None

            def norm(v):
                return str(v or "").strip().lower()

            for product_item in product_items:
                if not isinstance(product_item, dict):
                    continue

                item_product_name = product_item.get("product_name")

                if norm(item_product_name) == norm(product_name):
                    matched_product = product_item
                    break

            if matched_product:
                product_journey = matched_product.get("journey_comparison") or []
                country_actions = matched_product.get("country_actions") or {}

                us_actions = country_actions.get("us") or {}
                uk_actions = country_actions.get("uk") or {}

                us_rec = us_actions.get("recommendation")
                uk_rec = uk_actions.get("recommendation")

                us_inv = us_actions.get("inventory_recommendation")
                uk_inv = uk_actions.get("inventory_recommendation")

                recommendation_parts = []
                if us_rec:
                    recommendation_parts.append(f"US: {us_rec}")
                if uk_rec:
                    recommendation_parts.append(f"UK: {uk_rec}")

                inventory_parts = []
                if us_inv:
                    inventory_parts.append(f"US: {us_inv}")
                if uk_inv:
                    inventory_parts.append(f"UK: {uk_inv}")

                recommendation = " | ".join(recommendation_parts) if recommendation_parts else None
                inventory_recommendation = " | ".join(inventory_parts) if inventory_parts else None

            else:
                product_journey = []
                recommendation = None
                inventory_recommendation = None
                country_actions = {}

        else:
            # ✅ COUNTRY FLOW: existing SKU-wise behavior
            summary_result = get_or_create_summary(
                user_id=int(user_id),
                country=country,
                marketplace_id=None,
                period=None,
                timeline=None,
                year=None,
                objective=None,
                target_sku=key,
                force_regenerate=True
            )

            sku_actions = summary_result.get("sku_actions") or {}
            sku_block = sku_actions.get(key) or {}

            product_journey = sku_block.get("journey_summary") or []
            recommendation = sku_block.get("recommendation")
            inventory_recommendation = sku_block.get("inventory_recommendation")


        # ==============================
        # SKU INVENTORY FLAGS
        # ==============================
        sku_inventory_flags = {}

        if country in ("uk", "us"):

            inventory_aged_df = fetch_inventory_aged_by_user(int(user_id), country=country)

            if inventory_aged_df is not None and not inventory_aged_df.empty:

                all_flags = build_sku_inventory_flags(
                    inventory_aged_df,
                    user_id=int(user_id),
                    country=country
                )

                if key in all_flags:
                    sku_inventory_flags = all_flags[key]


        # ==============================
        # GET 24M HISTORY (for chart)
        # ==============================
        history_24m = []

        try:
            with db_connect(user_engine) as conn:
                inspector = inspect(conn)
                all_tables = inspector.get_table_names()

            latest_tables = get_latest_two_tables(all_tables, user_id, country)

            if latest_tables:

                latest_tbl = latest_tables[0]
                end_year = latest_tbl["year"]
                end_month_num = latest_tbl["month_num"]

                history_24m = get_sku_monthly_history(
                    user_id,
                    country,
                    key,
                    end_year,
                    end_month_num,
                    24
                )

        except:
            history_24m = []


        # ==============================
        # RESPONSE
        # ==============================
        return jsonify({
            "success": True,
            "product_name": product_name,
            "historical_trend": history_24m,

            # 🔥 SAME JOURNEY AS AI ANALYST
            "product_journey": product_journey,

            # STRATEGY ENGINE OUTPUT
            "recommendation": recommendation,
            "inventory_recommendation": inventory_recommendation,

            # ✅ For global, contains separate US/UK recommendation, inventory, ads actions
            "country_actions": country_actions,

            # INVENTORY FLAG
            "sku_inventory_alert": sku_inventory_flags,

            "objective": objective_v2
        })

    except Exception as e:
        return jsonify({'error': str(e)}), 500



@skuwise_bp.route('/ProductBestPerformance', methods=['POST'])
def product_best_performance():
    try:
        # AUTH
        auth_header = request.headers.get('Authorization')
        if not auth_header or not auth_header.startswith('Bearer '):
            return jsonify({'error': 'Authorization token missing or invalid'}), 401

        token = auth_header.split(' ')[1]

        try:
            payload, user_id, member_id = get_effective_user_id_from_token(token)
            user_id = str(payload.get('user_id'))
        except jwt.ExpiredSignatureError:
            return jsonify({'error': 'Token expired'}), 401
        except jwt.InvalidTokenError:
            return jsonify({'error': 'Invalid token'}), 401

        data = request.get_json() or {}

        product_name = data.get('product_name')
        country = (data.get('country') or 'us').strip().lower()
        home_currency = (data.get('home_currency') or 'USD').strip().lower()
        other_sku_product_names = data.get("other_sku_product_names") or []
        other_sku_product_names = [
            str(p).strip()
            for p in other_sku_product_names
            if str(p).strip()
        ]

        if not product_name:
            return jsonify({'error': 'Product name is required'}), 400

        if country.startswith('uk'):
            country = 'uk'
        elif country.startswith('us'):
            country = 'us'
        elif country.startswith('global'):
            # keep exact global variant:
            # global, global_inr, global_gbp, global_cad
            country = country
        else:
            return jsonify({'error': 'Only us, uk, or global country is supported in this API'}), 400

        month_mapping = {
            'january': 1,
            'february': 2,
            'march': 3,
            'april': 4,
            'may': 5,
            'june': 6,
            'july': 7,
            'august': 8,
            'september': 9,
            'october': 10,
            'november': 11,
            'december': 12
        }

        monthly_rows = []

        with db_connect(user_engine) as conn:
            inspector = inspect(conn)
            all_tables = inspector.get_table_names()

            prefix = f"skuwisemonthly_{user_id}_{country}_"

            for table_name in all_tables:
                table_lower = table_name.lower()

                if not table_lower.startswith(prefix.lower()):
                    continue

                clean_suffix = table_lower.replace(prefix.lower(), '').replace('_table', '')

                month_name = ''.join(ch for ch in clean_suffix if ch.isalpha())
                year_text = ''.join(ch for ch in clean_suffix if ch.isdigit())

                if month_name not in month_mapping or not year_text:
                    continue

                year = int(year_text[:4])
                month_num = month_mapping[month_name]

                try:
                    columns = [col['name'] for col in inspector.get_columns(table_name)]

                    if 'product_name' not in columns or 'net_sales' not in columns:
                        continue

                    has_sku_col = 'sku' in columns

                    quantity_col = None
                    if 'total_quantity' in columns:
                        quantity_col = 'total_quantity'
                    elif 'quantity' in columns:
                        quantity_col = 'quantity'

                    profit_col = None
                    if 'cm1_profit' in columns:
                        profit_col = 'cm1_profit'
                    elif 'profit' in columns:
                        profit_col = 'profit'

                    if not quantity_col or not profit_col:
                        continue

                    params = {}

                    if other_sku_product_names:
                        placeholders = ", ".join([f":p{i}" for i in range(len(other_sku_product_names))])

                        params = {
                            f"p{i}": other_sku_product_names[i].strip().lower()
                            for i in range(len(other_sku_product_names))
                        }

                        where_condition = f"""
                            LOWER(TRIM(product_name)) IN ({placeholders})
                        """

                    else:
                        params = {
                            "product_name": product_name
                        }

                        where_condition = """
                            LOWER(TRIM(product_name)) = LOWER(TRIM(:product_name))
                        """

                        if has_sku_col:
                            where_condition = """
                                LOWER(TRIM(product_name)) = LOWER(TRIM(:product_name))
                                OR LOWER(TRIM(sku)) = LOWER(TRIM(:product_name))
                            """

                    query = text(f"""
                        SELECT
                            COALESCE(SUM("{quantity_col}"), 0) AS units,
                            COALESCE(SUM(net_sales), 0) AS net_sales,
                            COALESCE(SUM("{profit_col}"), 0) AS cm1_profit
                        FROM "{table_name}"
                        WHERE {where_condition}
                    """)

                    row = conn.execute(query, params).fetchone()

                    units = float(row.units or 0)
                    net_sales = float(row.net_sales or 0)
                    cm1_profit = float(row.cm1_profit or 0)

                    if units == 0 and net_sales == 0 and cm1_profit == 0:
                        continue

                    # Currency conversion
                    source_currency = country_currency_map.get(country)
                    target_currency = home_currency
                    conversion_rate = 1.0

                    if source_currency and target_currency:
                        with db_connect(admin_engine) as conn1:
                            conversion_rate = get_conversion_rate(
                                conn1,
                                source_currency,
                                target_currency,
                                month_name,
                                year
                            )

                    net_sales *= conversion_rate
                    cm1_profit *= conversion_rate

                    # Derived metrics after currency conversion
                    asp = net_sales / units if units else 0
                    unit_wise_profitability = cm1_profit / units if units else 0

                    monthly_rows.append({
                        'month': month_name.capitalize(),
                        'year': year,
                        'month_num': month_num,
                        'units': round(units, 2),
                        'net_sales': round(net_sales, 2),
                        'cm1_profit': round(cm1_profit, 2),
                        'asp': round(asp, 2),
                        'unit_wise_profitability': round(unit_wise_profitability, 2)
                    })
                except Exception as e:
                    continue

        if not monthly_rows:
            return jsonify({
                'success': True,
                'product_name': product_name,
                'country': country,
                'message': 'No data found for this product',
                'best_performance': None
            }), 200

        monthly_rows.sort(key=lambda x: (x['year'], x['month_num']))

        best_units = max(monthly_rows, key=lambda x: x['units'])
        best_net_sales = max(monthly_rows, key=lambda x: x['net_sales'])
        best_cm1_profit = max(monthly_rows, key=lambda x: x['cm1_profit'])
        best_asp = max(monthly_rows, key=lambda x: x['asp'])
        best_unit_wise_profitability = max(
            monthly_rows,
            key=lambda x: x['unit_wise_profitability']
        )

        def clean_best(row, metric):
            return {
                'month': row['month'],
                'year': row['year'],
                metric: row[metric]
            }

        return jsonify({
            'success': True,
            'product_name': product_name,
            'country': country,
            'home_currency': home_currency.upper(),
            'date_range': {
                'start': f"{monthly_rows[0]['month']} {monthly_rows[0]['year']}",
                'latest': f"{monthly_rows[-1]['month']} {monthly_rows[-1]['year']}"
            },
            'best_performance': {
            'units': clean_best(best_units, 'units'),
            'net_sales': clean_best(best_net_sales, 'net_sales'),
            'cm1_profit': clean_best(best_cm1_profit, 'cm1_profit'),
            'asp': clean_best(best_asp, 'asp'),
            'unit_wise_profitability': clean_best(
                best_unit_wise_profitability,
                'unit_wise_profitability'
            )
        }
        }), 200

    except Exception as e:
        log_route_exception('ProductBestPerformance', e)
        status_code = 503 if isinstance(e, (OperationalError, SQLAlchemyTimeoutError)) else 500
        return jsonify({
            'success': False,
            'error': str(e),
            'retryable': status_code == 503
        }), status_code


############################################# Product Summary ########################################################################

# =====================================================
# Product AI Summary helpers
# =====================================================

def get_product_summary_quarter_info(today=None):
    """
    Regeneration dates:
    Jan 2, Apr 2, Jul 2, Oct 2.

    If current quarter summary exists -> return it.
    If not exists and date is before quarter regeneration date -> return previous summary.
    If not exists and date is on/after regeneration date -> generate new summary.
    """
    today = today or datetime.now()

    quarter = ((today.month - 1) // 3) + 1
    quarter_start_month = PRODUCT_SUMMARY_QUARTER_START_MONTHS[quarter]
    regenerate_date = datetime(today.year, quarter_start_month, 2)

    return {
        "quarter": quarter,
        "quarter_key": f"{today.year}_Q{quarter}",
        "quarter_start_month": quarter_start_month,
        "regenerate_date": regenerate_date,
        "can_regenerate": today >= regenerate_date,
    }


def normalize_product_summary_country(country, home_currency):
    """
    Normalizes frontend country value.

    us -> us
    uk -> uk
    global + USD -> global
    global + INR -> global_inr
    global + GBP -> global_gbp
    global + CAD -> global_cad
    """
    country = (country or "us").strip().lower()
    home_currency = (home_currency or "USD").strip().lower()

    if country.startswith("uk"):
        return "uk"

    if country.startswith("us"):
        return "us"

    if country.startswith("global"):
        if home_currency == "inr":
            return "global_inr"
        if home_currency == "gbp":
            return "global_gbp"
        if home_currency == "cad":
            return "global_cad"
        return "global"

    return "us"


def get_current_quarter_saved_product_summary(
    user_id,
    product_name,
    country,
    home_currency,
    quarter_key
):
    """
    Reads current quarter product summary from chatbot DB.
    """
    query = text("""
        SELECT
            id,
            summary,
            summary_payload,
            generated_at,
            quarter_key
        FROM product_ai_summary
        WHERE user_id = :user_id
          AND LOWER(TRIM(product_name)) = LOWER(TRIM(:product_name))
          AND LOWER(TRIM(country)) = LOWER(TRIM(:country))
          AND LOWER(TRIM(home_currency)) = LOWER(TRIM(:home_currency))
          AND quarter_key = :quarter_key
        LIMIT 1
    """)

    with db_connect(chatbot_engine) as conn:
        row = conn.execute(query, {
            "user_id": int(user_id),
            "product_name": product_name,
            "country": country,
            "home_currency": home_currency.upper(),
            "quarter_key": quarter_key,
        }).fetchone()

    if not row:
        return None

    return {
        "id": row.id,
        "summary": row.summary,
        "summary_payload": row.summary_payload,
        "generated_at": row.generated_at,
        "quarter_key": row.quarter_key,
    }


def get_latest_saved_product_summary_any_quarter(
    user_id,
    product_name,
    country,
    home_currency
):
    """
    Reads latest previous summary from chatbot DB.
    Used before quarter regeneration date.
    """
    query = text("""
        SELECT
            id,
            summary,
            summary_payload,
            generated_at,
            quarter_key
        FROM product_ai_summary
        WHERE user_id = :user_id
          AND LOWER(TRIM(product_name)) = LOWER(TRIM(:product_name))
          AND LOWER(TRIM(country)) = LOWER(TRIM(:country))
          AND LOWER(TRIM(home_currency)) = LOWER(TRIM(:home_currency))
        ORDER BY generated_at DESC
        LIMIT 1
    """)

    with db_connect(chatbot_engine) as conn:
        row = conn.execute(query, {
            "user_id": int(user_id),
            "product_name": product_name,
            "country": country,
            "home_currency": home_currency.upper(),
        }).fetchone()

    if not row:
        return None

    return {
        "id": row.id,
        "summary": row.summary,
        "summary_payload": row.summary_payload,
        "generated_at": row.generated_at,
        "quarter_key": row.quarter_key,
    }


def save_product_ai_summary(
    user_id,
    product_name,
    sku,
    country,
    home_currency,
    quarter_key,
    summary,
    summary_payload
):
    """
    Saves generated summary into chatbot DB.
    No manual regeneration.
    One row per user + product + country + home_currency + quarter.
    """
    query = text("""
        INSERT INTO product_ai_summary (
            user_id,
            product_name,
            sku,
            country,
            home_currency,
            quarter_key,
            summary,
            summary_payload,
            generated_at,
            created_at,
            updated_at
        )
        VALUES (
            :user_id,
            :product_name,
            :sku,
            :country,
            :home_currency,
            :quarter_key,
            :summary,
            CAST(:summary_payload AS JSONB),
            NOW(),
            NOW(),
            NOW()
        )
        ON CONFLICT (
            user_id,
            product_name,
            country,
            home_currency,
            quarter_key
        )
        DO UPDATE SET
            sku = EXCLUDED.sku,
            summary = EXCLUDED.summary,
            summary_payload = EXCLUDED.summary_payload,
            generated_at = NOW(),
            updated_at = NOW()
        RETURNING id, generated_at
    """)

    with db_begin(chatbot_engine) as conn:
        row = conn.execute(query, {
            "user_id": int(user_id),
            "product_name": product_name,
            "sku": sku,
            "country": country,
            "home_currency": home_currency.upper(),
            "quarter_key": quarter_key,
            "summary": summary,
            "summary_payload": json.dumps(summary_payload),
        }).fetchone()

    return {
        "id": row.id,
        "generated_at": row.generated_at,
    }


def resolve_product_summary_sku(conn, user_id, product_name, country):
    """
    Resolve SKU from sku_{user_id}_data_table for us/uk.
    For global summary, we compare countries using product_name.
    """
    if country not in ("uk", "us"):
        return None

    table_name = f"sku_{user_id}_data_table"
    sku_column = "sku_uk" if country == "uk" else "sku_us"

    inspector = inspect(conn)

    if not inspector.has_table(table_name):
        return None

    query = text(f"""
        SELECT {sku_column}
        FROM "{table_name}"
        WHERE LOWER(TRIM(product_name)) = LOWER(TRIM(:product_name))
           OR LOWER(TRIM({sku_column})) = LOWER(TRIM(:product_name))
        LIMIT 1
    """)

    row = conn.execute(query, {
        "product_name": product_name
    }).fetchone()

    return row[0] if row and row[0] else None

def parse_product_summary_table_name(table_name, user_id, country):
    """
    Exact table matching.

    UK/US accepted:
    skuwisemonthly_{user_id}_uk_may2026
    skuwisemonthly_{user_id}_us_may2026

    UK/US rejected:
    skuwisemonthly_{user_id}_uk_may2026_table
    skuwisemonthly_{user_id}_uk_may2026_backup
    skuwisemonthly_{user_id}_uk_may2026_old

    Global accepted:
    skuwisemonthly_{user_id}_global_may2026_table
    skuwisemonthly_{user_id}_global_inr_may2026_table
    skuwisemonthly_{user_id}_global_gbp_may2026_table
    skuwisemonthly_{user_id}_global_cad_may2026_table
    """

    table_lower = str(table_name or "").strip().lower()
    country = str(country or "").strip().lower()
    user_id = str(user_id).strip()

    prefix = f"skuwisemonthly_{user_id}_{country}_"

    if not table_lower.startswith(prefix):
        return None

    # UK/US must not use _table suffix.
    if country in ("uk", "us") and table_lower.endswith("_table"):
        return None

    # Global tables must use _table suffix.
    if country.startswith("global") and not table_lower.endswith("_table"):
        return None

    suffix = table_lower.replace(prefix, "", 1)

    if country.startswith("global"):
        suffix = suffix[:-6]  # remove "_table"

    month_name = "".join(ch for ch in suffix if ch.isalpha())
    year_text = "".join(ch for ch in suffix if ch.isdigit())

    if month_name not in PRODUCT_SUMMARY_MONTH_NAME_TO_NUM:
        return None

    if not year_text:
        return None

    year = int(year_text[:4])

    # Final exact table validation.
    if country in ("uk", "us"):
        expected_table_name = f"skuwisemonthly_{user_id}_{country}_{month_name}{year}"
    else:
        expected_table_name = f"skuwisemonthly_{user_id}_{country}_{month_name}{year}_table"

    if table_lower != expected_table_name.lower():
        return None

    return {
        "month_name": month_name,
        "month": month_name.capitalize(),
        "month_num": PRODUCT_SUMMARY_MONTH_NAME_TO_NUM[month_name],
        "year": year,
    }

def fetch_month_end_inventory_lookup(user_id, country, sku_list):
    """
    Month-end inventory from monthwise_inventory.

    Rules:
    - monthwise_inventory.msku = skuwisemonthly sku.
    - Country is separated by marketplace_id and ledger location.
    - For each sku + marketplace_id + disposition + month,
      take the LAST available date in that month.
    - Use ending_warehouse_balance from that last date.
    """

    country = (country or "").strip().lower()

    sku_list = [
        str(sku).strip()
        for sku in (sku_list or [])
        if str(sku).strip()
    ]

    if not sku_list:
        return {}

    # For global summary, combine UK + US inventory.
    if country.startswith("global"):
        inventory_scopes = [
            (inventory_marketplace_map["uk"], inventory_location_map["uk"]),
            (inventory_marketplace_map["us"], inventory_location_map["us"]),
        ]
    else:
        normalized_country = normalize_sku_country(country)
        marketplace_id = inventory_marketplace_map.get(normalized_country)
        location_code = inventory_location_map.get(normalized_country)

        if not marketplace_id or not location_code:
            return {}

        inventory_scopes = [(marketplace_id, location_code)]

    sku_placeholders = ", ".join(
        [f":sku_{i}" for i in range(len(sku_list))]
    )

    inventory_scope_sql = " OR ".join(
        [
            (
                f"(marketplace_id = :marketplace_{i} "
                f"AND UPPER(TRIM(COALESCE(location, ''))) = :location_{i})"
            )
            for i in range(len(inventory_scopes))
        ]
    )

    params = {
        "user_id": int(user_id)
    }

    for i, sku in enumerate(sku_list):
        params[f"sku_{i}"] = sku.lower()

    for i, (marketplace_id, location_code) in enumerate(inventory_scopes):
        params[f"marketplace_{i}"] = marketplace_id
        params[f"location_{i}"] = location_code

    query = text(f"""
        WITH base AS (
            SELECT
                LOWER(TRIM(msku)) AS msku,
                marketplace_id,
                UPPER(TRIM(COALESCE(disposition, ''))) AS disposition,
                date::date AS snapshot_date,
                EXTRACT(YEAR FROM date::date)::int AS year,
                EXTRACT(MONTH FROM date::date)::int AS month,
                COALESCE(ending_warehouse_balance, 0) AS ending_warehouse_balance
            FROM monthwise_inventory
            WHERE user_id = :user_id
              AND ({inventory_scope_sql})
              AND LOWER(TRIM(msku)) IN ({sku_placeholders})
        ),
        last_dates AS (
            SELECT
                msku,
                marketplace_id,
                disposition,
                year,
                month,
                MAX(snapshot_date) AS last_snapshot_date
            FROM base
            GROUP BY
                msku,
                marketplace_id,
                disposition,
                year,
                month
        )
        SELECT
            b.msku,
            b.marketplace_id,
            b.disposition,
            b.year,
            b.month,
            SUM(b.ending_warehouse_balance) AS ending_warehouse_balance
        FROM base b
        INNER JOIN last_dates ld
            ON b.msku = ld.msku
           AND b.marketplace_id = ld.marketplace_id
           AND b.disposition = ld.disposition
           AND b.year = ld.year
           AND b.month = ld.month
           AND b.snapshot_date = ld.last_snapshot_date
        GROUP BY
            b.msku,
            b.marketplace_id,
            b.disposition,
            b.year,
            b.month
    """)

    lookup = {}

    try:
        with db_connect(amazon_engine) as conn:
            rows = conn.execute(query, params).fetchall()

        for row in rows:
            key = (
                str(row.msku).strip().lower(),
                int(row.year),
                int(row.month)
            )

            if key not in lookup:
                lookup[key] = {
                    "sellable_inventory": 0,
                    "damaged_inventory": 0,
                    "expired_inventory": 0,
                    "total_inventory": 0,
                }

            disposition = str(row.disposition or "").upper()
            units = float(row.ending_warehouse_balance or 0)

            if disposition == "SELLABLE":
                lookup[key]["sellable_inventory"] += units

            elif disposition in (
                "DEFECTIVE",
                "WAREHOUSE_DAMAGED",
                "CUSTOMER_DAMAGED"
            ):
                lookup[key]["damaged_inventory"] += units

            elif disposition == "EXPIRED":
                lookup[key]["expired_inventory"] += units

            lookup[key]["total_inventory"] = (
                lookup[key]["sellable_inventory"]
                + lookup[key]["damaged_inventory"]
                + lookup[key]["expired_inventory"]
            )

        return lookup

    except Exception as e:
        return {}


def fetch_product_summary_history_for_country(
    conn,
    inspector,
    all_tables,
    user_id,
    product_name,
    country,
    home_currency
):
    """
    Reads all available skuwisemonthly tables for one country.

    Uses exact columns only:
    product_name, sku, total_quantity, net_sales, asp, profit, sales_mix, profit_mix.
    No fallback columns.
    """
    history = []
    resolved_sku = None
    inventory_lookup = {}

    if country in ("uk", "us"):
        resolved_sku = resolve_product_summary_sku(
            conn=conn,
            user_id=user_id,
            product_name=product_name,
            country=country
        )

    if resolved_sku:
        inventory_lookup = fetch_month_end_inventory_lookup(
            user_id=int(user_id),
            country=country,
            sku_list=[resolved_sku]
        )

    required_cols = {
        "product_name",
        "sku",
        "total_quantity",
        "net_sales",
        "asp",
        "profit",
        "sales_mix",
        "profit_mix",
    }

    for table_name in all_tables:
        parsed = parse_product_summary_table_name(
            table_name=table_name,
            user_id=user_id,
            country=country
        )

        if not parsed:
            continue

        try:
            columns = [
                col["name"]
                for col in inspector.get_columns(table_name)
            ]

            missing_cols = required_cols - set(columns)

            if missing_cols:
                continue

            where_condition = """
                LOWER(TRIM(product_name)) = LOWER(TRIM(:product_name))
                OR LOWER(TRIM(sku)) = LOWER(TRIM(:product_name))
            """

            query = text(f"""
                SELECT
                    MAX(product_name) AS product_name,
                    MAX(sku) AS sku,
                    COALESCE(SUM(total_quantity), 0) AS total_quantity,
                    COALESCE(SUM(net_sales), 0) AS net_sales,
                    COALESCE(SUM(asp), 0) AS asp,
                    COALESCE(SUM(profit), 0) AS profit,
                    COALESCE(SUM(sales_mix), 0) AS sales_mix,
                    COALESCE(SUM(profit_mix), 0) AS profit_mix
                FROM "{table_name}"
                WHERE {where_condition}
            """)

            row = conn.execute(query, {
                "product_name": product_name
            }).fetchone()

            total_quantity = float(row.total_quantity or 0)
            net_sales = float(row.net_sales or 0)
            asp = float(row.asp or 0)
            profit = float(row.profit or 0)
            sales_mix = float(row.sales_mix or 0)
            profit_mix = float(row.profit_mix or 0)

            if total_quantity == 0 and net_sales == 0 and profit == 0:
                continue

            conversion_rate = 1.0

            # UK/US source tables need currency conversion.
            # Global tables are already saved in selected global currency table.
            if country in ("uk", "us"):
                source_currency = country_currency_map.get(country)
                target_currency = home_currency.lower()

                if source_currency and target_currency:
                    with db_connect(admin_engine) as admin_conn:
                        conversion_rate = get_conversion_rate(
                            admin_conn,
                            source_currency,
                            target_currency,
                            parsed["month_name"],
                            parsed["year"]
                        )

                net_sales *= conversion_rate
                asp *= conversion_rate
                profit *= conversion_rate

            row_sku = row.sku

            if row_sku:
                row_sku = str(row_sku).strip()

            if not resolved_sku and row_sku:
                resolved_sku = row_sku

            if row_sku and not inventory_lookup:
                inventory_lookup = fetch_month_end_inventory_lookup(
                    user_id=int(user_id),
                    country=country,
                    sku_list=[row_sku]
                )

            inventory_key = (
                str(row_sku or "").strip().lower(),
                int(parsed["year"]),
                int(parsed["month_num"])
            )

            inventory_data = inventory_lookup.get(inventory_key, {
                "sellable_inventory": 0,
                "damaged_inventory": 0,
                "expired_inventory": 0,
                "total_inventory": 0,
            })

            history.append({
                "source_table": table_name,
                "country": country,
                "product_name": row.product_name or product_name,
                "sku": row_sku,
                "month": parsed["month"],
                "month_num": parsed["month_num"],
                "year": parsed["year"],

                "total_quantity": round(total_quantity, 2),
                "net_sales": round(net_sales, 2),
                "asp": round(asp, 2),
                "profit": round(profit, 2),
                "sales_mix": round(sales_mix, 2),
                "profit_mix": round(profit_mix, 2),

                "sellable_inventory": round(float(inventory_data.get("sellable_inventory") or 0), 2),
                "damaged_inventory": round(float(inventory_data.get("damaged_inventory") or 0), 2),
                "expired_inventory": round(float(inventory_data.get("expired_inventory") or 0), 2),
                "total_inventory": round(float(inventory_data.get("total_inventory") or 0), 2),

                "conversion_rate_applied": conversion_rate,
            })

        except Exception as e:
            continue

    history.sort(key=lambda x: (x["year"], x["month_num"]))

    return {
        "country": country,
        "sku": resolved_sku,
        "history": history,
    }


def summarize_product_summary_history(history):
    """
    Creates summary numbers for AI prompt.
    """
    if not history:
        return {
            "months_count": 0,
            "total_quantity": 0,
            "net_sales": 0,
            "profit": 0,
            "avg_asp": 0,
            "avg_sales_mix": 0,
            "avg_profit_mix": 0,

            "latest_sellable_inventory": 0,
            "latest_damaged_inventory": 0,
            "latest_expired_inventory": 0,
            "latest_total_inventory": 0,

            "latest_month": None,
            "best_sales_month": None,
            "best_profit_month": None,
        }

    total_quantity = sum(float(item.get("total_quantity") or 0) for item in history)
    net_sales = sum(float(item.get("net_sales") or 0) for item in history)
    profit = sum(float(item.get("profit") or 0) for item in history)

    avg_asp = net_sales / total_quantity if total_quantity else 0

    avg_sales_mix = (
        sum(float(item.get("sales_mix") or 0) for item in history) / len(history)
    )

    avg_profit_mix = (
        sum(float(item.get("profit_mix") or 0) for item in history) / len(history)
    )

    latest_month = history[-1]

    latest_sellable_inventory = float(
        latest_month.get("sellable_inventory") or 0
    )

    latest_damaged_inventory = float(
        latest_month.get("damaged_inventory") or 0
    )

    latest_expired_inventory = float(
        latest_month.get("expired_inventory") or 0
    )

    latest_total_inventory = float(
        latest_month.get("total_inventory") or 0
    )

    best_sales_month = max(
        history,
        key=lambda item: float(item.get("net_sales") or 0)
    )

    best_profit_month = max(
        history,
        key=lambda item: float(item.get("profit") or 0)
    )

    return {
        "months_count": len(history),
        "total_quantity": round(total_quantity, 2),
        "net_sales": round(net_sales, 2),
        "profit": round(profit, 2),
        "avg_asp": round(avg_asp, 2),
        "avg_sales_mix": round(avg_sales_mix, 2),
        "avg_profit_mix": round(avg_profit_mix, 2),

        "latest_sellable_inventory": round(latest_sellable_inventory, 2),
        "latest_damaged_inventory": round(latest_damaged_inventory, 2),
        "latest_expired_inventory": round(latest_expired_inventory, 2),
        "latest_total_inventory": round(latest_total_inventory, 2),

        "latest_month": latest_month,
        "best_sales_month": best_sales_month,
        "best_profit_month": best_profit_month,
    }


def build_product_summary_payload(user_id, product_name, country, home_currency):
    """
    country us/uk:
        one country history.

    country global/global_inr/global_gbp/global_cad:
        UK + US + selected global currency history for comparison.
    """
    normalized_country = normalize_product_summary_country(
        country=country,
        home_currency=home_currency
    )

    if normalized_country.startswith("global"):
        countries_to_fetch = ["uk", "us", normalized_country]
    else:
        countries_to_fetch = [normalized_country]

    with db_connect(user_engine) as conn:
        inspector = inspect(conn)
        all_tables = inspector.get_table_names()

        countries_payload = []
        resolved_sku = None

        for country_item in countries_to_fetch:
            country_result = fetch_product_summary_history_for_country(
                conn=conn,
                inspector=inspector,
                all_tables=all_tables,
                user_id=user_id,
                product_name=product_name,
                country=country_item,
                home_currency=home_currency
            )

            history = country_result.get("history") or []

            if not resolved_sku and country_result.get("sku"):
                resolved_sku = country_result.get("sku")

            countries_payload.append({
                "country": country_item,
                "sku": country_result.get("sku"),
                "summary_metrics": summarize_product_summary_history(history),
                "monthly_history": history,
            })

    return {
        "product_name": product_name,
        "sku": resolved_sku,
        "country": normalized_country,
        "home_currency": home_currency.upper(),
        "countries": countries_payload,
    }


def build_product_summary_ai_prompt(product_name, country, home_currency, payload):
    return f"""
You are an ecommerce product performance analyst.

Your job is to explain product performance in very simple business language.
The summary should be easy to understand for everyone:
- warehouse or packing team
- account manager
- founder or business owner
- finance or operations team

Avoid technical jargon unless you explain it in simple words.

Product:
{product_name}

Country mode:
{country}

Currency:
{home_currency.upper()}

Important metrics available in the data:
- product_name
- sku
- total_quantity
- net_sales
- asp
- profit
- sales_mix
- profit_mix
- sellable_inventory
- damaged_inventory
- expired_inventory
- total_inventory

Metric meanings:
- total_quantity means how many units were sold.
- net_sales means sales revenue after adjustments.
- asp means average selling price per unit.
- profit means estimated product CM1 profit.
- sales_mix means how much this product contributes to total sales.
- profit_mix means how much this product contributes to total profit.
- sellable_inventory means units available to sell at month end.
- damaged_inventory means units not in good sellable condition.
- expired_inventory means units expired or unusable.
- total_inventory means all counted inventory from the month-end inventory snapshot.

Output format:
Use plain text only.
Do not use markdown headings.
Do not use tables.
Use short bullet points.
Keep the summary short and clear.
Do not include a separate Product snapshot section.
Do not include action points or recommendations.
End with a clear conclusion.

Required summary structure:

1. Start with a simple one-line overall summary.
Example style:
- This product is selling well and making profit, while inventory has moved up and down compared with monthly sales.

2. Sales performance:
Explain briefly:
- Whether units sold are increasing, decreasing, or inconsistent.
- Mention the strongest sales month with month and year.
- Mention total units sold only if useful.

3. CM1 Profit performance:
Explain briefly:
- Whether CM1 profit is improving, declining, or inconsistent.
- Mention the strongest CM1 profit month with month and year.
- Mention total CM1 profit only if useful.

4. Price / ASP performance:
Explain briefly:
- Whether average selling price is increasing, decreasing, or stable.
- When mentioning ASP, include the month and year if it comes from a monthly value.
- Keep this section simple.

5. Sales mix and profit mix:
Explain briefly:
- Whether this product is becoming more or less important in the business.
- Use simple wording.

6. Inventory movement:
Explain inventory as a historical summary, not as advice.
Compare sellable_inventory with total_quantity month by month.
Mention:
- The month and year when sellable inventory was highest, and how many units were sold in that same month.
- The month and year when sellable inventory was lowest, and how many units were sold in that same month.
- The latest month and year inventory level, and how it compares with units sold in that same month.
- Whether inventory has generally increased, decreased, or moved unevenly over time.
- Whether inventory was usually higher than monthly sales, lower than monthly sales, or close to monthly sales.
- If damaged or expired inventory exists, mention the month and year.
- If inventory is zero or missing, say inventory data is not available or not showing for that month.

Do not say:
- monitor inventory
- watch inventory
- inventory needs attention
- inventory needs monitoring
- avoid stockouts
- avoid overstock
- take action
- if sales continue
- recommendation
- should

7. Conclusion:
End with a short conclusion covering:
- Overall product health
- Main strength
- Main concern, if any

Rules:
- Keep the total summary concise.
- Do not invent numbers.
- Only use numbers present in the provided data.
- Whenever you mention a monthly metric, include the month and year.
  Example: "March 2026 had the strongest sales with 354 units."
- Do not include action points.
- Do not include recommendations.
- Do not tell the user what to do next.
- Do not use advice-style language.
- Do not use the word "should".
- Do not say "needs monitoring", "watch closely", or "needs attention".
- Inventory must be summarized as what happened historically.
- When discussing inventory, compare sellable_inventory against total_quantity for the same month.
- If data is missing, clearly say it is missing.
- Avoid complex financial language.
- Use currency symbol or currency code where helpful.
- If country mode is global/global_inr/global_gbp/global_cad, compare UK vs US vs global performance and clearly say which country looks stronger.

Data:
{json.dumps(payload, indent=2)}
"""


def generate_product_ai_summary(product_name, country, home_currency, payload):
    """
    OpenAI summary generation.
    Uses existing OpenAI client from this file.
    """
    prompt = build_product_summary_ai_prompt(
        product_name=product_name,
        country=country,
        home_currency=home_currency,
        payload=payload
    )

    last_error = None

    for attempt in range(2):
        try:
            response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {
                    "role": "system",
                    "content": (
                        "You explain ecommerce product performance in simple, practical language "
                        "that warehouse teams, packing teams, account managers, finance teams, "
                        "and business owners can all understand. "
                        "Refer to profit as CM1 profit. "
                        "Use plain text and short bullet points only. "
                        "Do not give action points, recommendations, or advice. "
                        "Do not use the word should. "
                        "Describe inventory as a historical comparison with sales, not as something to monitor. "
                        "End with a clear conclusion."
                    )
                },
                {
                    "role": "user",
                    "content": prompt
                }
            ],
            temperature=0.2,
            max_tokens=1000
        )

            return response.choices[0].message.content.strip()

        except Exception as e:
            last_error = e
            if attempt == 0:
                time.sleep(0.5)

    log_route_exception("generate_product_ai_summary", last_error)
    return None


@skuwise_bp.route('/ProductSummaryAI', methods=['POST'])
def product_summary_ai():
    try:
        # ==============================
        # AUTH
        # ==============================
        auth_header = request.headers.get('Authorization')

        if not auth_header or not auth_header.startswith('Bearer '):
            return jsonify({
                'error': 'Authorization token missing or invalid'
            }), 401

        token = auth_header.split(' ')[1]

        try:
            payload, user_id, member_id = get_effective_user_id_from_token(token)
            user_id = str(payload.get('user_id'))
        except jwt.ExpiredSignatureError:
            return jsonify({'error': 'Token expired'}), 401
        except jwt.InvalidTokenError:
            return jsonify({'error': 'Invalid token'}), 401

        # ==============================
        # REQUEST
        # ==============================
        data = request.get_json() or {}

        product_name = data.get("product_name")
        country = data.get("country", "us")
        home_currency = (data.get("home_currency") or "USD").strip().upper()

        if not product_name:
            return jsonify({
                "error": "product_name is required"
            }), 400

        normalized_country = normalize_product_summary_country(
            country=country,
            home_currency=home_currency
        )

        # ==============================
        # QUARTER CACHE LOGIC
        # ==============================
        quarter_info = get_product_summary_quarter_info()
        quarter_key = quarter_info["quarter_key"]

        current_quarter_summary = get_current_quarter_saved_product_summary(
            user_id=user_id,
            product_name=product_name,
            country=normalized_country,
            home_currency=home_currency,
            quarter_key=quarter_key
        )

        # If current quarter summary exists, always return it.
        if current_quarter_summary:
            return jsonify({
                "success": True,
                "source": "cache_current_quarter",
                "product_name": product_name,
                "country": normalized_country,
                "home_currency": home_currency,
                "quarter_key": quarter_key,
                "generated_at": current_quarter_summary["generated_at"],
                "summary": current_quarter_summary["summary"],
                "summary_payload": current_quarter_summary["summary_payload"],
            }), 200

        latest_saved_summary = get_latest_saved_product_summary_any_quarter(
            user_id=user_id,
            product_name=product_name,
            country=normalized_country,
            home_currency=home_currency
        )

        # If old summary exists and quarter regeneration date has not arrived,
        # keep showing the previous summary.
        if latest_saved_summary and not quarter_info["can_regenerate"]:
            return jsonify({
                "success": True,
                "source": "cache_previous_quarter",
                "product_name": product_name,
                "country": normalized_country,
                "home_currency": home_currency,
                "quarter_key": latest_saved_summary["quarter_key"],
                "current_quarter_key": quarter_key,
                "next_regeneration_date": quarter_info["regenerate_date"].strftime("%Y-%m-%d"),
                "generated_at": latest_saved_summary["generated_at"],
                "summary": latest_saved_summary["summary"],
                "summary_payload": latest_saved_summary["summary_payload"],
            }), 200

        # ==============================
        # FIRST-TIME GENERATION OR QUARTERLY AUTO REGENERATION
        # ==============================
        summary_payload = build_product_summary_payload(
            user_id=user_id,
            product_name=product_name,
            country=normalized_country,
            home_currency=home_currency
        )

        has_any_history = any(
            country_block.get("monthly_history")
            for country_block in summary_payload.get("countries", [])
        )

        if not has_any_history:
            return jsonify({
                "success": True,
                "source": "no_history",
                "product_name": product_name,
                "country": normalized_country,
                "home_currency": home_currency,
                "message": "No product history found for summary generation",
                "summary": None,
                "summary_payload": summary_payload,
            }), 200

        summary = generate_product_ai_summary(
            product_name=product_name,
            country=normalized_country,
            home_currency=home_currency,
            payload=summary_payload
        )

        if not summary:
            return jsonify({
                "success": False,
                "error": "AI summary generation failed"
            }), 500

        saved = save_product_ai_summary(
            user_id=user_id,
            product_name=product_name,
            sku=summary_payload.get("sku"),
            country=normalized_country,
            home_currency=home_currency,
            quarter_key=quarter_key,
            summary=summary,
            summary_payload=summary_payload
        )

        return jsonify({
            "success": True,
            "source": "generated",
            "product_name": product_name,
            "sku": summary_payload.get("sku"),
            "country": normalized_country,
            "home_currency": home_currency,
            "quarter_key": quarter_key,
            "generated_at": saved["generated_at"],
            "summary": summary,
            "summary_payload": summary_payload,
        }), 200

    except Exception as e:
        log_route_exception("ProductSummaryAI", e)
        status_code = 503 if isinstance(e, (OperationalError, SQLAlchemyTimeoutError)) else 500
        return jsonify({
            "success": False,
            "error": str(e),
            "retryable": status_code == 503
        }), status_code
