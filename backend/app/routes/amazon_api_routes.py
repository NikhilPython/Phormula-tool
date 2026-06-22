from __future__ import annotations
import io, os,time, logging , re, json
from datetime import datetime
import pandas as pd
from sqlalchemy import text
from typing import Any, Dict, List
from datetime import datetime, timezone
from sqlalchemy.dialects.postgresql import insert as pg_insert
import jwt, requests
from config import Config
from dotenv import find_dotenv, load_dotenv
from flask import Blueprint, jsonify, make_response, request
from app import db
from app.models.user_models import amazon_user, Product
from app.utils.token_utils import get_effective_user_id_from_token
from app.utils.formulas_utils import uk_advertising, uk_platform_fee
from app.utils.amazon_utils import (_fetch_fba_skus_all,
_upsert_products_to_db_with_open_date , 
_month_date_range_utc, 
_apply_region_and_marketplace_from_request ,
_flatten_transaction_to_row, 
run_upload_pipeline_from_df, 
_month_name_lower,
_month_to_num,
_month_to_date_range_utc_safe,
compute_totals,
compute_net_reimbursement_from_df, 
compute_debt_payment_disbursement_from_df,
upsert_liveorders_from_rows, 
fetch_sku_price_map,
fetch_conversion_rate,
add_profit_column_from_uk_profit,
get_previous_month_mtd_payload,
_i
)
from app.services.amazon_monthly_sync_service import sync_monthly_transactions_for_user
from app.utils.amazon_utils import MTD_COLUMNS, COUNTRY_TO_SELECTED_CURRENCY, DEFAULT_SKU_PRICE_CURRENCY
from app.utils.amazon_utils import AmazonSPAPIClient, amazon_client
from flask import jsonify, request, send_file
from sqlalchemy import create_engine
from config import Config
SECRET_KEY = Config.SECRET_KEY






# --- load .env robustly (works no matter where you run `flask run`) ---
dotenv_path = find_dotenv(filename=".env", usecwd=True)
load_dotenv(dotenv_path, override=True)
load_dotenv()
db_url  = os.getenv('DATABASE_URL')
db_url1 = os.getenv('DATABASE_ADMIN_URL') or db_url  
db_url2 = os.getenv('DATABASE_AMAZON_URL') or db_url  

PHORMULA_ENGINE = create_engine(db_url, pool_pre_ping=True)
ADMIN_ENGINE = create_engine(db_url1, pool_pre_ping=True)
AMAZON_ENGINE = create_engine(db_url2, pool_pre_ping=True)

if not db_url:
    raise RuntimeError("DATABASE_URL is not set")
if not db_url1:
    # optional: log a warning if using fallback
    print("[WARN] DATABASE_ADMIN_URL not set; falling back to DATABASE_URL")
if not db_url2:
    # optional: log a warning if using fallback
    print("[WARN] DATABASE_AMAZON_URL not set; falling back to DATABASE_URL")

amazon_api_bp = Blueprint("amazon_api", __name__)




# ------------------------------------------------- Routes -------------------------------------------------
@amazon_api_bp.route("/amazon_api/login", methods=["GET"])
def amazon_login():
    auth_header = request.headers.get("Authorization")
    if not auth_header or not auth_header.startswith("Bearer "):
        return jsonify({"error": "Authorization token is missing or invalid"}), 401

    token = auth_header.split(" ", 1)[1]

    try:
        payload, user_id, member_id = get_effective_user_id_from_token(token)
        user_id = int(user_id or payload.get("user_id"))
    except jwt.ExpiredSignatureError:
        return jsonify({"error": "Token has expired"}), 401
    except jwt.InvalidTokenError:
        return jsonify({"error": "Invalid token"}), 401
    except Exception:
        return jsonify({"error": "Invalid token payload"}), 401

    _apply_region_and_marketplace_from_request()

    if amazon_client.marketplace_id not in amazon_client.ALLOWED_MARKETPLACES:
        return jsonify({"success": False, "error": "Unsupported marketplace"}), 400

    marketplace_id = amazon_client.marketplace_id
    region = amazon_client.region

    au = amazon_user.query.filter_by(
        user_id=user_id,
        marketplace_id=marketplace_id
    ).first()

    if not au:
        au = amazon_user(
            user_id=user_id,
            region=region,
            marketplace_id=marketplace_id,
            marketplace_name=marketplace_id,
            currency=None,
            refresh_token="",
            is_connected=False
        )
        db.session.add(au)
    else:
        au.region = region
        au.marketplace_id = marketplace_id
        au.marketplace_name = au.marketplace_name or marketplace_id

    db.session.commit()

    state = f"uid|{user_id}|{marketplace_id}|{int(time.time())}"

    return jsonify({
        "success": True,
        "auth_url": amazon_client.get_oauth_url(state),
        "state": state
    }), 200

@amazon_api_bp.route("/amazon_api/callback", methods=["GET"])
def amazon_oauth_callback():
    code = request.args.get("spapi_oauth_code")
    state = request.args.get("state") or ""

    # Amazon sends this in OAuth callback.
    seller_id = (
        request.args.get("selling_partner_id")
        or request.args.get("sellingPartnerId")
        or request.args.get("seller_id")
    )

    if not code:
        return make_response("Missing spapi_oauth_code", 400)

    if not state.startswith("uid|"):
        return make_response("Invalid state received", 400)

    try:
        parts = state.split("|")
        user_id = int(parts[1])
        marketplace_id = parts[2]
    except Exception:
        return make_response("Invalid state format", 400)

    r = requests.post(
        AmazonSPAPIClient.TOKEN_URL,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        data={
            "grant_type": "authorization_code",
            "code": code,
            "client_id": amazon_client.client_id,
            "client_secret": amazon_client.client_secret,
            "redirect_uri": amazon_client.redirect_uri,
        },
        timeout=30
    )

    if r.status_code != 200:
        return make_response(f"Token exchange failed: {r.text}", 400)

    refresh = r.json().get("refresh_token")
    if not refresh:
        return make_response("No refresh token returned", 400)

    au = amazon_user.query.filter_by(
        user_id=user_id,
        marketplace_id=marketplace_id
    ).first()

    if not au:
        au = amazon_user(
            user_id=user_id,
            region=amazon_client.region,
            marketplace_id=marketplace_id,
            marketplace_name=marketplace_id,
            refresh_token=refresh,
            seller_id=seller_id,
            is_connected=True
        )
        db.session.add(au)
    else:
        au.refresh_token = refresh
        au.is_connected = True
        au.updated_at = datetime.utcnow()

        if seller_id:
            au.seller_id = seller_id

    db.session.commit()

    amazon_client.refresh_token = refresh

    try:
        with open(".refresh_token", "w") as f:
            f.write(refresh)
    except Exception:
        pass

    return """
        <html>
            <body style="font-family: system-ui;">
                <p>✅ Amazon account linked successfully. You may close this window.</p>
                <script>
                    try {
                        if (window.opener) {
                            window.opener.postMessage(
                                { type: "amazon_oauth_success" },
                                "*"
                            );
                        }
                    } catch(e) {}
                    window.close();
                </script>
            </body>
        </html>
    """

@amazon_api_bp.route("/amazon_api/status", methods=["GET"])
def amazon_status():
    auth_header = request.headers.get('Authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        return jsonify({'success': False, 'error': 'Authorization token is missing or invalid'}), 401

    token = auth_header.split(' ')[1]
    try:
        payload, user_id, member_id = get_effective_user_id_from_token(token)
    except jwt.ExpiredSignatureError:
        return jsonify({'success': False, 'error': 'Token has expired'}), 401
    except jwt.InvalidTokenError:
        return jsonify({'success': False, 'error': 'Invalid token'}), 401

    _apply_region_and_marketplace_from_request()

    if amazon_client.marketplace_id not in amazon_client.ALLOWED_MARKETPLACES:
        return jsonify({"success": False, "error": "Unsupported marketplace"}), 400

    marketplace_id = amazon_client.marketplace_id

    au = amazon_user.query.filter_by(
        user_id=user_id,
        marketplace_id=marketplace_id
    ).first()

    if not au:
        return jsonify({
            "success": False,
            "status": "no_record",
            "has_refresh_token": False,
        }), 200

    if not au.refresh_token:
        return jsonify({
            "success": False,
            "status": "pending",
            "has_refresh_token": False,
        }), 200

    amazon_client.refresh_token = au.refresh_token

    res = amazon_client.make_api_call("/sellers/v1/marketplaceParticipations", "GET")
    if res and "error" not in res:
        return jsonify({
            "success": True,
            "status": "connected",
            "has_refresh_token": True,
            "payload": res.get("payload") or []
        }), 200

    return jsonify({
        "success": False,
        "status": "sp_api_error",
        "has_refresh_token": True,
        "error": res
    }), 502


@amazon_api_bp.route("/amazon_api/health", methods=["GET"])
def amazon_health():
    ok = bool(amazon_client.get_access_token())
    return jsonify({"status": "healthy" if ok else "error"}), (200 if ok else 500)

# ------------------------------------------------- SKUs and Listings Items API Functions -------------------------------------------------

def _attr_list(attributes, key):
    if not isinstance(attributes, dict):
        return []
    value = attributes.get(key) or []
    return value if isinstance(value, list) else []


def _attr_first(attributes, key, value_key="value"):
    values = _attr_list(attributes, key)
    if not values:
        return None

    first = values[0]
    if isinstance(first, dict):
        return first.get(value_key)

    return first


def _attr_values(attributes, key):
    values = _attr_list(attributes, key)
    output = []

    for item in values:
        if isinstance(item, dict):
            value = item.get("value")
            if value is not None:
                output.append(value)
        elif item is not None:
            output.append(item)

    return output


def _parse_amazon_datetime(value):
    if not value:
        return None

    if isinstance(value, datetime):
        return value.replace(tzinfo=None) if value.tzinfo else value

    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).replace(tzinfo=None)
    except Exception:
        return None


def _to_bool(value):
    if isinstance(value, bool):
        return value

    if value is None:
        return None

    if isinstance(value, str):
        return value.strip().lower() in ("true", "1", "yes", "y")

    return bool(value)


def _to_decimal_value(value):
    if value is None:
        return None

    try:
        return float(value)
    except Exception:
        return None


def _status_to_string(status):
    if isinstance(status, list):
        return ",".join(str(x) for x in status)

    if status is None:
        return None

    return str(status)


def _get_main_image_url(product, attributes):
    main_image = product.get("mainImage") or {}
    if isinstance(main_image, dict) and main_image.get("link"):
        return main_image.get("link")

    locator = _attr_list(attributes, "main_product_image_locator")
    if locator and isinstance(locator[0], dict):
        return locator[0].get("media_location")

    return None


def _get_all_image_urls(product, attributes):
    urls = []

    main_url = _get_main_image_url(product, attributes)
    if main_url:
        urls.append(main_url)

    for key, value in attributes.items():
        if key.startswith("other_product_image_locator"):
            if isinstance(value, list):
                for img in value:
                    if isinstance(img, dict) and img.get("media_location"):
                        urls.append(img["media_location"])

    # remove duplicates
    return list(dict.fromkeys(urls))


def _get_external_product_identifier(attributes):
    values = _attr_list(attributes, "externally_assigned_product_identifier")
    if not values:
        return None, None

    first = values[0]
    if not isinstance(first, dict):
        return None, None

    return first.get("value"), first.get("type")


def _get_parent_sku(attributes):
    values = _attr_list(attributes, "child_parent_sku_relationship")
    if not values:
        return None

    first = values[0]
    if isinstance(first, dict):
        return first.get("parent_sku")

    return None


def _get_variation_theme(attributes):
    values = _attr_list(attributes, "variation_theme")
    if not values:
        return None

    first = values[0]
    if isinstance(first, dict):
        return first.get("name")

    return None


def _get_unit_count(attributes):
    values = _attr_list(attributes, "unit_count")
    if not values:
        return None

    first = values[0]
    if not isinstance(first, dict):
        return str(first)

    value = first.get("value")
    unit_type = first.get("type") or {}
    unit_name = unit_type.get("value") if isinstance(unit_type, dict) else None

    if value is not None and unit_name:
        return f"{value} {unit_name}"

    if value is not None:
        return str(value)

    return None


def _get_list_price(attributes):
    values = _attr_list(attributes, "list_price")
    if not values:
        return None, None

    first = values[0]
    if not isinstance(first, dict):
        return None, None

    return (
        _to_decimal_value(first.get("value_with_tax")),
        first.get("currency")
    )


def _get_offer_price(offers):
    if not isinstance(offers, list) or not offers:
        return None, None

    # prefer B2C / ALL offer
    selected = None
    for offer in offers:
        if not isinstance(offer, dict):
            continue

        offer_type = offer.get("offerType")
        audience = offer.get("audience") or {}
        audience_value = audience.get("value") if isinstance(audience, dict) else None

        if offer_type == "B2C" or audience_value == "ALL":
            selected = offer
            break

    if selected is None:
        selected = offers[0]

    price = selected.get("price") or {}
    return (
        _to_decimal_value(price.get("amount")),
        price.get("currencyCode") or price.get("currency")
    )


def _get_first_fulfillment_channel(fulfillment_availability):
    if not isinstance(fulfillment_availability, list) or not fulfillment_availability:
        return None, None

    first = fulfillment_availability[0]
    if not isinstance(first, dict):
        return None, None

    return (
        first.get("fulfillmentChannelCode") or first.get("fulfillment_channel_code"),
        first.get("quantity")
    )


def _get_weight(attributes, key):
    values = _attr_list(attributes, key)
    if not values:
        return None, None

    first = values[0]
    if not isinstance(first, dict):
        return None, None

    return (
        _to_decimal_value(first.get("value")),
        first.get("unit")
    )


def _get_fc_shelf_life_days(attributes):
    values = _attr_list(attributes, "fc_shelf_life")
    if not values:
        return None

    first = values[0]
    if not isinstance(first, dict):
        return None

    try:
        return int(float(first.get("value")))
    except Exception:
        return None
    

def _upsert_full_listing_products_to_db(products, marketplace_id, user_id):
    """
    Stores full Amazon Listings Items API product data into products table.
    Normal fields go into columns.
    Complete JSON goes into product_data.
    """

    saved_count = 0
    now = datetime.utcnow()

    for product in products:
        if not isinstance(product, dict):
            continue

        sku = product.get("sku")
        if not sku:
            continue

        attributes = product.get("attributes") or {}
        offers = product.get("offers") or []
        fulfillment_availability = product.get("fulfillmentAvailability") or []
        issues = product.get("issues") or []
        summaries = product.get("summaries") or []

        # In your normalized product, summaries may not exist, so fallback to normal fields
        first_summary = summaries[0] if summaries else {}

        asin = (
            product.get("asin")
            or first_summary.get("asin")
            or _attr_first(attributes, "merchant_suggested_asin")
        )

        fn_sku = first_summary.get("fnSku") or first_summary.get("fnsku")

        title = (
            product.get("itemName")
            or first_summary.get("itemName")
            or _attr_first(attributes, "item_name")
        )

        product_type = (
            product.get("productType")
            or first_summary.get("productType")
        )

        condition_type = (
            product.get("conditionType")
            or first_summary.get("conditionType")
            or _attr_first(attributes, "condition_type")
        )

        status = _status_to_string(
            product.get("status")
            or first_summary.get("status")
        ) or "Active"

        brand = _attr_first(attributes, "brand")
        manufacturer = _attr_first(attributes, "manufacturer")

        description = _attr_first(attributes, "product_description")
        bullet_points = _attr_values(attributes, "bullet_point")
        generic_keywords = _attr_values(attributes, "generic_keyword")

        main_image_url = _get_main_image_url(product, attributes)
        image_urls = _get_all_image_urls(product, attributes)

        parent_sku = _get_parent_sku(attributes)
        parentage_level = _attr_first(attributes, "parentage_level")
        variation_theme = _get_variation_theme(attributes)

        external_id, external_id_type = _get_external_product_identifier(attributes)

        price_amount, price_currency = _get_offer_price(offers)
        list_price_amount, list_price_currency = _get_list_price(attributes)

        fulfillment_channel, quantity = _get_first_fulfillment_channel(fulfillment_availability)

        country_of_origin = _attr_first(attributes, "country_of_origin")
        item_form = _attr_first(attributes, "item_form")
        size = _attr_first(attributes, "size") or _attr_first(attributes, "size_per_pearl")
        color = _attr_first(attributes, "color")
        scent = _attr_first(attributes, "scent")
        unit_count = _get_unit_count(attributes)

        item_weight_value, item_weight_unit = _get_weight(attributes, "item_weight")
        package_weight_value, package_weight_unit = _get_weight(attributes, "item_package_weight")

        item_dimensions = _attr_list(attributes, "item_dimensions")
        item_dimensions = item_dimensions[0] if item_dimensions else None

        package_dimensions = _attr_list(attributes, "item_package_dimensions")
        package_dimensions = package_dimensions[0] if package_dimensions else None

        is_expiration_dated_product = _to_bool(
            _attr_first(attributes, "is_expiration_dated_product")
        )
        is_heat_sensitive = _to_bool(
            _attr_first(attributes, "is_heat_sensitive")
        )
        contains_liquid_contents = _to_bool(
            _attr_first(attributes, "contains_liquid_contents")
        )

        fc_shelf_life_days = _get_fc_shelf_life_days(attributes)

        open_date = _parse_amazon_datetime(
            product.get("createdDate")
            or first_summary.get("createdDate")
        )

        amazon_last_updated_at = _parse_amazon_datetime(
            product.get("lastUpdatedDate")
            or first_summary.get("lastUpdatedDate")
        )

        row = Product.query.filter_by(
            sku=sku,
            marketplace_id=marketplace_id
        ).first()

        if not row:
            row = Product(
                user_id=user_id,
                sku=sku,
                marketplace_id=marketplace_id,
                created_at=now
            )
            db.session.add(row)

        row.user_id = user_id
        row.sku = sku
        row.asin = asin
        row.fn_sku = fn_sku
        row.marketplace_id = marketplace_id

        row.product_type = product_type
        row.condition_type = condition_type
        row.status = status

        row.title = title
        row.brand = brand
        row.category = product_type
        row.manufacturer = manufacturer

        row.description = description
        row.bullet_points = bullet_points
        row.generic_keywords = generic_keywords

        row.main_image_url = main_image_url
        row.image_urls = image_urls

        row.parent_sku = parent_sku
        row.parentage_level = parentage_level
        row.variation_theme = variation_theme

        row.external_product_id = external_id
        row.external_product_id_type = external_id_type

        row.price_amount = price_amount
        row.price_currency = price_currency
        row.list_price_amount = list_price_amount
        row.list_price_currency = list_price_currency

        row.fulfillment_channel = fulfillment_channel
        row.fulfillment_availability = fulfillment_availability
        row.quantity = quantity

        row.country_of_origin = country_of_origin
        row.item_form = item_form
        row.size = size
        row.color = color
        row.scent = scent
        row.unit_count = unit_count

        row.item_weight_value = item_weight_value
        row.item_weight_unit = item_weight_unit
        row.package_weight_value = package_weight_value
        row.package_weight_unit = package_weight_unit

        row.item_dimensions = item_dimensions
        row.package_dimensions = package_dimensions

        row.is_expiration_dated_product = is_expiration_dated_product
        row.is_heat_sensitive = is_heat_sensitive
        row.contains_liquid_contents = contains_liquid_contents
        row.fc_shelf_life_days = fc_shelf_life_days

        row.issues = issues
        row.offers = offers
        row.attributes = attributes
        row.summaries = summaries

        # Full normalized Amazon product object
        row.product_data = product

        row.open_date = open_date
        row.amazon_last_updated_at = amazon_last_updated_at
        row.synced_at = now
        row.updated_at = now

        saved_count += 1

    db.session.commit()
    return saved_count



# -------------------------------------------------------
# 4) Route
# -------------------------------------------------------
@amazon_api_bp.route("/amazon_api/skus", methods=["GET"])
def list_skus():
    """
    Fetch seller SKUs from Amazon Listings Items API and optionally save full product info.

    Query params:
      marketplace_id   optional
      seller_id        optional temporary fallback for testing
      full_details     true/false, default true
      store_in_db      true/false, default false
      limit            optional integer
      included_data    default summaries,attributes,offers,fulfillmentAvailability,issues
      include_raw      true/false, default false
    """

    # -------------------------
    # 1) Auth
    # -------------------------
    auth_header = request.headers.get("Authorization")
    if not auth_header or not auth_header.startswith("Bearer "):
        return jsonify({
            "success": False,
            "error": "Authorization token is missing or invalid"
        }), 401

    token = auth_header.split(" ", 1)[1]

    try:
        payload, user_id, member_id = get_effective_user_id_from_token(token)
        user_id = int(user_id or payload.get("user_id"))
    except jwt.ExpiredSignatureError:
        return jsonify({"success": False, "error": "Token has expired"}), 401
    except jwt.InvalidTokenError:
        return jsonify({"success": False, "error": "Invalid token"}), 401
    except Exception:
        return jsonify({"success": False, "error": "Invalid token payload"}), 401

    # -------------------------
    # 2) Marketplace / region
    # -------------------------
    _apply_region_and_marketplace_from_request()

    marketplace_id = request.args.get("marketplace_id") or amazon_client.marketplace_id

    if marketplace_id not in amazon_client.ALLOWED_MARKETPLACES:
        return jsonify({
            "success": False,
            "error": "Unsupported marketplace",
            "marketplace_id": marketplace_id
        }), 400

    # -------------------------
    # 3) Load Amazon connection
    # -------------------------
    au = amazon_user.query.filter_by(
        user_id=user_id,
        marketplace_id=marketplace_id
    ).first()

    if not au:
        return jsonify({
            "success": False,
            "error": "Amazon connection not found for this marketplace",
            "marketplace_id": marketplace_id
        }), 404

    if not au.refresh_token:
        return jsonify({
            "success": False,
            "error": "No refresh token found for this marketplace. Complete OAuth first.",
            "marketplace_id": marketplace_id
        }), 400

    amazon_client.refresh_token = au.refresh_token

    # -------------------------
    # 4) Query options
    # -------------------------
    full_details = request.args.get("full_details", "true").lower() != "false"
    store_in_db = request.args.get("store_in_db", "false").lower() == "true"
    include_raw = request.args.get("include_raw", "false").lower() == "true"

    included_data = request.args.get(
        "included_data",
        "summaries,attributes,offers,fulfillmentAvailability,issues"
    )

    limit = request.args.get("limit")
    try:
        limit = int(limit) if limit else None
    except ValueError:
        return jsonify({
            "success": False,
            "error": "limit must be a valid integer"
        }), 400

    # -------------------------
    # 5) Seller ID
    # -------------------------
    seller_id = (
        getattr(au, "seller_id", None)
        or getattr(amazon_client, "seller_id", None)
        or request.args.get("seller_id")
    )

    if not seller_id:
        return jsonify({
            "success": False,
            "error": "seller_id is missing for this Amazon connection",
            "hint": (
                "Reconnect Amazon OAuth so selling_partner_id is saved into amazon_user.seller_id. "
                "For temporary Postman testing, pass ?seller_id=YOUR_SP_API_SELLER_ID."
            )
        }), 400

    # -------------------------
    # 6) Search Listings Items API
    # -------------------------
    all_search_items = []
    page_token = None

    try:
        while True:
            query_params = {
                "marketplaceIds": marketplace_id,
                "includedData": "summaries,offers,fulfillmentAvailability,issues",
                "pageSize": 20,
            }

            if page_token:
                query_params["pageToken"] = page_token

            search_res = amazon_client.make_api_call(
                f"/listings/2021-08-01/items/{seller_id}",
                "GET",
                params=query_params
            )

            if not search_res or "error" in search_res:
                return jsonify({
                    "success": False,
                    "error": "Failed to search seller listings from Amazon",
                    "details": search_res
                }), 502

            payload_data = search_res.get("payload") if isinstance(search_res, dict) else None
            data_source = payload_data if isinstance(payload_data, dict) else search_res

            items = data_source.get("items") or []
            all_search_items.extend(items)

            if limit and len(all_search_items) >= limit:
                all_search_items = all_search_items[:limit]
                break

            pagination = data_source.get("pagination") or {}
            page_token = (
                pagination.get("nextToken")
                or pagination.get("nextPageToken")
                or pagination.get("nextPageTokenId")
            )

            if not page_token:
                break

    except Exception as e:
        return jsonify({
            "success": False,
            "error": "Failed to fetch SKUs from Listings Items Search API",
            "details": str(e),
            "marketplace_id": marketplace_id
        }), 502

    # -------------------------
    # 7) Normalize search result
    # -------------------------
    sku_rows = []

    for item in all_search_items:
        sku = item.get("sku")
        summaries = item.get("summaries") or []
        first_summary = summaries[0] if summaries else {}

        sku_rows.append({
            "sku": sku,
            "asin": item.get("asin") or first_summary.get("asin") or first_summary.get("asin1"),
            "status": first_summary.get("status"),
            "itemName": first_summary.get("itemName"),
            "productType": first_summary.get("productType"),
            "conditionType": first_summary.get("conditionType"),
            "createdDate": first_summary.get("createdDate"),
            "lastUpdatedDate": first_summary.get("lastUpdatedDate"),
            "mainImage": first_summary.get("mainImage"),
            "summaries": summaries,
        })

    # -------------------------
    # 8) Optional full details per SKU
    # -------------------------
    details = []

    if full_details:
        for row in sku_rows:
            sku = row.get("sku")
            if not sku:
                continue

            try:
                detail_res = amazon_client.make_api_call(
                    f"/listings/2021-08-01/items/{seller_id}/{sku}",
                    "GET",
                    params={
                        "marketplaceIds": marketplace_id,
                        "includedData": included_data
                    }
                )

                if detail_res and "error" not in detail_res:
                    payload_detail = detail_res.get("payload") if isinstance(detail_res, dict) else None
                    detail_data = payload_detail if isinstance(payload_detail, dict) else detail_res

                    summaries = detail_data.get("summaries") or row.get("summaries") or []
                    first_summary = summaries[0] if summaries else {}

                    product = {
                        "sku": sku,
                        "asin": first_summary.get("asin") or row.get("asin"),
                        "fnSku": first_summary.get("fnSku") or first_summary.get("fnsku"),
                        "itemName": first_summary.get("itemName") or row.get("itemName"),
                        "productType": first_summary.get("productType") or row.get("productType"),
                        "conditionType": first_summary.get("conditionType") or row.get("conditionType"),
                        "status": first_summary.get("status") or row.get("status"),
                        "createdDate": first_summary.get("createdDate") or row.get("createdDate"),
                        "lastUpdatedDate": first_summary.get("lastUpdatedDate") or row.get("lastUpdatedDate"),
                        "mainImage": first_summary.get("mainImage") or row.get("mainImage"),
                        "attributes": detail_data.get("attributes") or {},
                        "offers": detail_data.get("offers") or [],
                        "fulfillmentAvailability": detail_data.get("fulfillmentAvailability") or [],
                        "issues": detail_data.get("issues") or [],
                        "summaries": summaries,
                    }

                    if include_raw:
                        product["raw"] = detail_data

                    details.append(product)

                else:
                    details.append({
                        **row,
                        "detail_error": detail_res
                    })

            except Exception as e:
                details.append({
                    **row,
                    "detail_error": str(e)
                })
    else:
        details = sku_rows

    # -------------------------
    # 9) Optional DB save
    # -------------------------
    saved_count = 0
    db_error = None

    if store_in_db and details:
        try:
            saved_count = _upsert_full_listing_products_to_db(
                details,
                marketplace_id,
                user_id
            )
        except Exception as e:
            db.session.rollback()
            db_error = str(e)

    # -------------------------
    # 10) Response
    # -------------------------
    return jsonify({
        "success": True,
        "source": "listings-items-api",
        "marketplace_id": marketplace_id,
        "seller_id": seller_id,
        "count": len(details),
        "full_details": full_details,
        "included_data": included_data,
        "skus": details,
        "empty_message": "There is no SKU listed in this seller account." if not details else None,
        "db": {
            "attempted": store_in_db,
            "saved_products": saved_count,
            "error": db_error
        },
        "notes": {
            "createdDate": "Saved into products.open_date.",
            "full_data": "Complete normalized Amazon response is saved into products.product_data.",
            "attributes": "Full attributes JSON is saved into products.attributes.",
            "offers": "Full offers JSON is saved into products.offers."
        }
    }), 200




@amazon_api_bp.route("/amazon_api/account", methods=["GET"])
def amazon_account():
    auth_header = request.headers.get("Authorization")
    if not auth_header or not auth_header.startswith("Bearer "):
        return jsonify({"error": "Authorization token is missing or invalid"}), 401

    token = auth_header.split(" ")[1]
    try:
        payload, user_id, member_id = get_effective_user_id_from_token(token)
        user_id = int(payload["user_id"])
    except jwt.ExpiredSignatureError:
        return jsonify({"error": "Token has expired"}), 401
    except jwt.InvalidTokenError:
        return jsonify({"error": "Invalid token"}), 401
    except Exception:
        return jsonify({"error": "Invalid token payload"}), 401

    _apply_region_and_marketplace_from_request()

    marketplace_id = request.args.get("marketplace_id") or amazon_client.marketplace_id
    if marketplace_id not in amazon_client.ALLOWED_MARKETPLACES:
        return jsonify({"success": False, "error": "Unsupported marketplace"}), 400

    # Load refresh token from DB for this exact marketplace
    au = amazon_user.query.filter_by(
        user_id=user_id,
        marketplace_id=marketplace_id
    ).first()

    if not au:
        return jsonify({
            "success": False,
            "error": "Amazon connection not found for this marketplace",
            "marketplace_id": marketplace_id
        }), 404

    if not au.refresh_token:
        return jsonify({
            "success": False,
            "error": "No refresh token found. Complete OAuth first.",
            "marketplace_id": marketplace_id
        }), 400

    amazon_client.refresh_token = au.refresh_token

    try:
        res = amazon_client.make_api_call("/sellers/v1/marketplaceParticipations", "GET")
    except Exception as e:
        return jsonify({
            "success": False,
            "message": "Failed to fetch account info",
            "details": str(e)
        }), 502

    if not res or "error" in res:
        return jsonify({
            "success": False,
            "message": "Failed to fetch account info",
            "details": res
        }), 502

    data = res.get("payload") if isinstance(res, dict) else res
    if data is None:
        return jsonify({
            "success": False,
            "message": "Unexpected response from Amazon",
            "details": res
        }), 502

    accounts = []
    items = data if isinstance(data, list) else data.get("marketplaceParticipations", [])

    for item in items:
        mkt = (item or {}).get("marketplace", {})
        part = (item or {}).get("participation", {})

        accounts.append({
            "marketplaceId": mkt.get("id"),
            "marketplaceName": mkt.get("name"),
            "countryCode": mkt.get("countryCode"),
            "domainName": mkt.get("domainName"),
            "currency": mkt.get("defaultCurrencyCode"),
            "language": mkt.get("defaultLanguageCode"),
            "isParticipating": part.get("isParticipating"),
            "hasSuspendedListings": part.get("hasSuspendedListings"),
        })

    return jsonify({
        "success": True,
        "region": au.region,
        "marketplace_id": marketplace_id,
        "marketplace_name": au.marketplace_name,
        "seller_id": getattr(au, "seller_id", None),
        "db_country": au.country_name,
        "db_currency": au.currency,
        "count": len(accounts),
        "accounts": accounts,
    }), 200



@amazon_api_bp.route("/amazon_api/connections", methods=["GET"])
def list_amazon_connections():
    # -------- auth --------
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

    rows = amazon_user.query.filter_by(user_id=user_id).all()

    return jsonify({
        "success": True,
        "connections": [
            {
                "region": r.region,
                "marketplace_id": r.marketplace_id,
                "marketplace_name": r.marketplace_name,
                "seller_id": getattr(r, "seller_id", None),
                "currency": r.currency,
                "is_connected": r.is_connected,
                "country": r.country_name,
                "stock_unit": r.stock_unit,
                "transit_time": r.transit_time
            }
            for r in rows
        ]
    })


# ------------------------------------------------- MTD fetched -------------------------------------------------

# =========================================================
# ROUTE
# =========================================================
@amazon_api_bp.route("/amazon_api/finances/monthly_transactions", methods=["GET"])
def finances_monthly_transactions():
    auth_header = request.headers.get("Authorization")
    if not auth_header or not auth_header.startswith("Bearer "):
        return jsonify({"success": False, "error": "Authorization token is missing or invalid"}), 401

    token = auth_header.split(" ")[1]
    try:
        payload, user_id, member_id = get_effective_user_id_from_token(token)
        user_id = payload["user_id"]
    except jwt.ExpiredSignatureError:
        return jsonify({"success": False, "error": "Token has expired"}), 401
    except jwt.InvalidTokenError:
        return jsonify({"success": False, "error": "Invalid token"}), 401

    now_utc = datetime.now(timezone.utc)
    try:
        year = int(request.args.get("year", now_utc.year))
        month = int(request.args.get("month", now_utc.month))
        if month < 1 or month > 12:
            raise ValueError
    except ValueError:
        return jsonify({"success": False, "error": "Invalid year or month"}), 400

    transaction_status = request.args.get("transaction_status")
    marketplace_id = request.args.get("marketplace_id")
    transaction_type_filter = request.args.get("transaction_type")
    response_format = (request.args.get("format") or "json").lower()

    store_in_db = (request.args.get("store_in_db", "true").lower() != "false")
    run_upload = (request.args.get("run_upload_pipeline", "false").lower() == "true")
    ui_country = (request.args.get("country") or "").strip().lower()

    if not ui_country:
        mkt = (marketplace_id or amazon_client.marketplace_id or "").strip()
        if mkt == "ATVPDKIKX0DER":
            ui_country = "us"
        elif mkt == "A1F83G8C2ARO7P":
            ui_country = "uk"
        elif mkt == "A2EUQ1WTGCTBG2":
            ui_country = "canada"
        else:
            ui_country = "us"

    

    if run_upload and not ui_country:
        return jsonify({"success": False, "error": "country is required when run_upload_pipeline=true"}), 400

    _apply_region_and_marketplace_from_request()

    result = sync_monthly_transactions_for_user(
        user_id=user_id,
        year=year,
        month=month,
        country=ui_country,
        marketplace_id=marketplace_id or amazon_client.marketplace_id,
        transaction_status=transaction_status,
        transaction_type_filter=transaction_type_filter,
        store_in_db=store_in_db,
        run_upload=run_upload,
        db_url=db_url,
        db_url_aux=db_url1,
    )

    if not result.get("success"):
        status_code = 400
        error_text = str(result.get("error", "")).lower()

        if "no_refresh_token" in str(result.get("status", "")).lower():
            status_code = 400
        elif "sp-api" in error_text or "unknown sp-api error" in error_text:
            status_code = 502

        return jsonify(result), status_code

    all_rows = result.get("transactions", [])
    pipeline_result = result.get("pipeline_result")

    if response_format == "excel":
        df = pd.DataFrame(all_rows) if all_rows else pd.DataFrame()
        df = df.reindex(columns=MTD_COLUMNS, fill_value=0.0)

        output = io.BytesIO()
        with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
            df.to_excel(writer, index=False, sheet_name="Transactions")

            if pipeline_result:
                pd.DataFrame([pipeline_result]).to_excel(
                    writer,
                    index=False,
                    sheet_name="PipelineMeta",
                )

        output.seek(0)
        filename = f"finances_transactions_{year}_{month:02d}.xlsx"
        return send_file(
            output,
            as_attachment=True,
            download_name=filename,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

    return jsonify(result), 200


@amazon_api_bp.route('/upload', methods=['POST'])
def upload():
    df_in = None
    if 'file' in request.files and request.files['file'].filename:
        f = request.files['file']
        fname = f.filename.lower()
        try:
            if fname.endswith(('.xlsx', '.xls')):
                df_in = pd.read_excel(f)
            elif fname.endswith('.csv'):
                df_in = pd.read_csv(f)
            elif fname.endswith('.json'):
                df_in = pd.read_json(f)
            else:
                return jsonify({"error": "Unsupported file type. Use .xlsx, .xls, .csv, or .json"}), 400
        except Exception as e:
            return jsonify({"error": f"Failed to parse uploaded file: {str(e)}"}), 400
    else:
        payload = request.get_json(silent=True) or {}
        rows = payload.get("rows") or payload.get("data")
        if isinstance(rows, list):
            try:
                df_in = pd.DataFrame(rows)
            except Exception as e:
                return jsonify({"error": f"Could not build dataframe from 'rows': {str(e)}"}), 400

    if df_in is None:
        return jsonify({"error": "Provide a file (xlsx/csv/json) in form-data as 'file' or a JSON body with 'rows' (list of dicts)."}), 400

    def _param(name, alt=None, required=False, caster=lambda x: x):
        if name in request.form:
            raw = request.form.get(name)
        elif alt and alt in request.form:
            raw = request.form.get(alt)
        else:
            payload = request.get_json(silent=True) or {}
            raw = payload.get(name)
            if raw is None and alt:
                raw = payload.get(alt)
        if required and (raw is None or raw == ""):
            raise KeyError(name)
        if raw is None:
            return None
        try:
            return caster(raw)
        except Exception:
            raise ValueError(name)

    try:
        user_id = _param("user_id", required=True)
        ui_country = _param("ui_country", alt="country", required=True, caster=lambda s: str(s).strip())
        raw_month = _param("month_num", alt="month")
        if raw_month is None:
            month_num = datetime.utcnow().month
        else:
            sm = str(raw_month).strip()
            if sm.isdigit():
                month_num = int(sm)
            else:
                month_num = _month_to_num(sm)
        ui_year = _param("ui_year", alt="year", caster=int) or datetime.utcnow().year
        if not (1 <= int(month_num) <= 12):
            return jsonify({"error": "month_num must be between 1 and 12"}), 400
    except KeyError as e:
        return jsonify({"error": f"Missing required field '{e.args[0]}'"}), 400
    except ValueError as e:
        return jsonify({"error": f"Invalid value for '{e.args[0]}'"}), 400

    result = run_upload_pipeline_from_df(
        df_raw=df_in,
        user_id=user_id,
        country=ui_country,
        month_num=int(month_num),
        year=int(ui_year),
        db_url=db_url,
        db_url_aux=db_url1,
    )
    if not result.get("success"):
        return jsonify(result), 400
    return jsonify(result), 200


# ========================================================= Live MTD fetch =========================================================


# ---------------- helpers ----------------
def _safe_ident(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"[^a-z0-9_]+", "_", s)
    return s.strip("_") or "x"


def _build_skuwise_table_name(user_id: int, country: str, month: int, year: int) -> str:
    return f"skuwisemonthly_{int(user_id)}_{_safe_ident(country)}_{int(month)}_{int(year)}"


def _build_adsmonthly_table_name(user_id: int, country: str, month: int, year: int) -> str:
    return f"adsmonthly_{int(user_id)}_{_safe_ident(country)}_{int(month)}_{int(year)}"


def _build_sku_data_table_name(user_id: int) -> str:
    # table: public.sku_{user_id}_data_table
    return f"sku_{int(user_id)}_data_table"


def _country_to_sku_col(country: str) -> str:
    c = (country or "").strip().lower()
    if c in ("uk", "gb", "united_kingdom"):
        return "sku_uk"
    if c in ("us", "usa", "united_states"):
        return "sku_us"
    return "sku_uk"



def get_current_global_data_for_live_bi(user_id: int):
    import math
    import pandas as pd
    import numpy as np
    from datetime import datetime, timezone
    from sqlalchemy import text

    now_utc = datetime.now(timezone.utc)
    month_name = _month_name_lower(now_utc.month)

    uk_table = f"skuwisemonthly_{user_id}_uk_{month_name}_{now_utc.year}"
    us_table = f"skuwisemonthly_{user_id}_us_{month_name}_{now_utc.year}"
    global_table = f"skuwisemonthly_{user_id}_global_{month_name}_{now_utc.year}"

    # -------------------------------------------------------------------------
    # SAFE TABLE READS
    # -------------------------------------------------------------------------
    def table_exists(table_name):
        sql = text("""
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = 'public'
                  AND table_name = :table_name
            )
        """)
        try:
            with PHORMULA_ENGINE.connect() as conn:
                return bool(conn.execute(sql, {"table_name": table_name}).scalar())
        except Exception as e:
            print(f"[WARN] table_exists failed for {table_name}: {e}")
            return False

    def safe_read_skuwise_table(table_name, country):
        if not table_exists(table_name):
            print(f"[WARN] {country.upper()} table missing, skipping: {table_name}")
            return pd.DataFrame()

        try:
            return pd.read_sql_query(
                f'SELECT * FROM public."{table_name}"',
                PHORMULA_ENGINE
            )
        except Exception as e:
            print(f"[WARN] Failed to read {country.upper()} table {table_name}: {e}")
            return pd.DataFrame()

    uk_df = safe_read_skuwise_table(uk_table, "uk")
    us_df = safe_read_skuwise_table(us_table, "us")

    available_countries = []
    if not uk_df.empty:
        available_countries.append("uk")
    if not us_df.empty:
        available_countries.append("us")

    if not available_countries:
        return {
            "success": False,
            "status": "loading",
            "country": "global",
            "requested_country": "global",
            "available_countries": [],
            "message": "No UK or US current-month SKU-wise data is available yet.",
            "conversion": {
                "from": "GBP",
                "to": "USD",
                "rate": 1.0,
                "month": month_name,
                "year": now_utc.year,
            },
            "skuwise_tables": {
                "us": {
                    "name": us_table,
                    "saved": False,
                    "rows": 0,
                    "available": False,
                },
                "uk": {
                    "name": uk_table,
                    "saved": False,
                    "rows": 0,
                    "available": False,
                    "currency": "USD",
                    "converted_from": "GBP",
                    "conversion_rate": 1.0,
                },
                "global": {
                    "name": global_table,
                    "saved": False,
                    "rows": 0,
                    "available": False,
                },
            },
            "skuwise_table": {
                "name": global_table,
                "saved": False,
                "rows": 0,
            },
            "derived_totals_global": {},
            "skuwise_items_us": [],
            "skuwise_items_uk": [],
            "skuwise_items_global": [],
        }

    # -------------------------------------------------------------------------
    # SPLIT GRAND TOTAL ROWS SAFELY
    # -------------------------------------------------------------------------
    def split_grand_total(df):
        if df is None or df.empty:
            return pd.DataFrame(), pd.DataFrame()

        df = df.copy()

        if "sku" not in df.columns:
            df["sku"] = ""

        if "product_name" not in df.columns:
            df["product_name"] = ""

        sku_upper = df["sku"].fillna("").astype(str).str.strip().str.upper()
        product_lower = df["product_name"].fillna("").astype(str).str.strip().str.lower()

        total_mask = (
            sku_upper.isin(["GRAND_TOTAL", "TOTAL"])
            | product_lower.isin(["grand total", "total"])
        )

        gt = df[total_mask].copy()
        body = df[~total_mask].copy()

        return gt, body

    # remove old grand total before combining
    uk_gt, uk_df = split_grand_total(uk_df)
    us_gt, us_df = split_grand_total(us_df)

    uk_to_usd_rate = fetch_conversion_rate(
        country="us",
        year=now_utc.year,
        month_name=month_name,
        user_currency="gbp",
        selected_currency="usd",
    ) or 1.0

    uk_reimbursement = 0
    us_reimbursement = 0

    if not uk_gt.empty and "current_net_reimbursement" in uk_gt.columns:
        uk_reimbursement = (
            pd.to_numeric(uk_gt["current_net_reimbursement"], errors="coerce")
            .fillna(0)
            .sum()
            * float(uk_to_usd_rate)
        )

    if not us_gt.empty and "current_net_reimbursement" in us_gt.columns:
        us_reimbursement = (
            pd.to_numeric(us_gt["current_net_reimbursement"], errors="coerce")
            .fillna(0)
            .sum()
        )

    global_current_net_reimbursement = float(uk_reimbursement) + float(us_reimbursement)

    def gt_money_total(df, col, rate=1):
        if df is None or df.empty or col not in df.columns:
            return 0
        return float(
            pd.to_numeric(df[col], errors="coerce").fillna(0).sum()
        ) * float(rate)
    
    global_product_spend = (
        gt_money_total(uk_gt, "product_spend", uk_to_usd_rate)
        + gt_money_total(us_gt, "product_spend", 1)
    )

    global_display_spend = (
        gt_money_total(uk_gt, "display_spend", uk_to_usd_rate)
        + gt_money_total(us_gt, "display_spend", 1)
    )

    global_ads_spend = global_product_spend + global_display_spend

    global_brand_spend = (
        gt_money_total(uk_gt, "brand_spend", uk_to_usd_rate)
        + gt_money_total(us_gt, "brand_spend", 1)
    )

    global_platform_fee_inventory_storage = (
        gt_money_total(uk_gt, "platform_fee_inventory_storage", uk_to_usd_rate)
        + gt_money_total(us_gt, "platform_fee_inventory_storage", 1)
    )

    global_platformfeenew = (
        gt_money_total(uk_gt, "platformfeenew", uk_to_usd_rate)
        + gt_money_total(us_gt, "platformfeenew", 1)
    )

    global_dealsvouchar_ads = (
        gt_money_total(uk_gt, "dealsvouchar_ads", uk_to_usd_rate)
        + gt_money_total(us_gt, "dealsvouchar_ads", 1)
    )

    global_misc_transaction = (
        gt_money_total(uk_gt, "misc_transaction", uk_to_usd_rate)
        + gt_money_total(us_gt, "misc_transaction", 1)
    )
    global_debt_payment = (
        gt_money_total(uk_gt, "debt_payment", uk_to_usd_rate)
        + gt_money_total(us_gt, "debt_payment", 1)
    )

    global_disbursement = (
        gt_money_total(uk_gt, "disbursement", uk_to_usd_rate)
        + gt_money_total(us_gt, "disbursement", 1)
    )

    money_cols = [
        "product_sales", "product_sales_tax", "postage_credits",
        "gift_wrap_credits", "shipping_credits_tax", "giftwrap_credits_tax",
        "promotional_rebates", "promotional_rebates_tax",
        "marketplace_facilitator_tax", "selling_fees", "fba_fees",
        "marketplace_fees",
        "other", "gross_sales", "cogs", "profit", "net_sales","asp", 
        "ads_spend", "product_spend", "display_spend", "brand_spend",
        "platform_fee", "platform_fee_inventory_storage",
        "platformfeenew", "dealsvouchar_ads", "shipment_fees",
        "cm2_profit", "total_ads", "total_cm2_profit",
        "current_net_reimbursement", "amazon_fees", "advertising_fees",
        "tax", "credits", "tax_and_credits", "lost_total",
        "ads_sale_amount", "misc_transaction","debt_payment",
        "disbursement",
    ]

    def recalc_response_grand_total(row_df):
        if row_df is None or row_df.empty:
            return row_df

        row_df = row_df.copy()

        product_ads_total = (
            abs(float(row_df.iloc[0].get("product_spend", 0.0) or 0.0))
            + abs(float(row_df.iloc[0].get("display_spend", 0.0) or 0.0))
        )

        cost_ads_total = (
            abs(float(row_df.iloc[0].get("brand_spend", 0.0) or 0.0))
            + abs(float(row_df.iloc[0].get("dealsvouchar_ads", 0.0) or 0.0))
        )

        total_ads = product_ads_total + cost_ads_total

        net_sales = float(row_df.iloc[0].get("net_sales", 0.0) or 0.0)
        profit = float(row_df.iloc[0].get("profit", 0.0) or 0.0)

        ads_spend = (
            abs(float(row_df.iloc[0].get("product_spend", 0.0) or 0.0))
            + abs(float(row_df.iloc[0].get("display_spend", 0.0) or 0.0))
        )

        cm2_profit_productwise = profit - ads_spend

        other_transactions_total = (
            abs(float(row_df.iloc[0].get("misc_transaction", 0.0) or 0.0))
            + abs(float(row_df.iloc[0].get("lost_total", 0.0) or 0.0))
            - abs(float(row_df.iloc[0].get("platform_fee_inventory_storage", 0.0) or 0.0))
            - abs(float(row_df.iloc[0].get("platformfeenew", 0.0) or 0.0))
        )

        total_cm2_profit = (
            cm2_profit_productwise
            - abs(float(row_df.iloc[0].get("brand_spend", 0.0) or 0.0))
            - abs(float(row_df.iloc[0].get("dealsvouchar_ads", 0.0) or 0.0))
            - abs(other_transactions_total)
            - abs(float(row_df.iloc[0].get("shipment_fees", 0.0) or 0.0))
        )

        row_df.loc[:, "total_ads"] = round(total_ads, 2)
        row_df.loc[:, "advertising_fees"] = round(total_ads, 2)
        row_df.loc[:, "total_cm2_profit"] = round(total_cm2_profit, 2)
        row_df.loc[:, "total_cm2_margins"] = round(
            (total_cm2_profit / net_sales * 100) if net_sales else 0,
            2
        )
        row_df.loc[:, "tacos_total_advertising_cost_of_sale"] = round(
            (total_ads / net_sales * 100) if net_sales else 0,
            2
        )

        ads_sale_amount = float(row_df.iloc[0].get("ads_sale_amount", 0.0) or 0.0)
        ads_clicks = float(row_df.iloc[0].get("ads_clicks", 0.0) or 0.0)
        ads_sale_units = float(row_df.iloc[0].get("ads_sale_units", 0.0) or 0.0)

        row_df.loc[:, "ads_acos"] = round(
            (total_ads / ads_sale_amount * 100) if ads_sale_amount else 0,
            2
        )
        row_df.loc[:, "ads_roas"] = round(
            (ads_sale_amount / total_ads) if total_ads else 0,
            2
        )
        row_df.loc[:, "ads_conversion_rate"] = round(
            (ads_sale_units / ads_clicks * 100) if ads_clicks else 0,
            2
        )

        row_df.loc[:, "cm2_profit"] = round(cm2_profit_productwise, 2)
        row_df.loc[:, "platform_fee"] = round(-other_transactions_total, 2)

        return row_df

    # -------------------------------------------------------------------------
    # CONVERT UK MONEY COLUMNS TO USD
    # -------------------------------------------------------------------------
    for col in money_cols:
        if not uk_df.empty and col in uk_df.columns:
            uk_df[col] = pd.to_numeric(uk_df[col], errors="coerce") * float(uk_to_usd_rate)

    # -------------------------------------------------------------------------
    # ADD COUNTRY METADATA SAFELY
    # -------------------------------------------------------------------------
    def add_country_metadata(df, country):
        if df is None or df.empty:
            return pd.DataFrame()

        df = df.copy()
        df["country"] = country
        df["month"] = month_name
        df["year"] = now_utc.year
        df["user_id"] = user_id
        df["generated_at_utc"] = now_utc.isoformat()
        return df

    us_df = add_country_metadata(us_df, "us")
    uk_df = add_country_metadata(uk_df, "uk")

    us_response_df = us_df.copy() if not us_df.empty else pd.DataFrame()
    uk_response_df = uk_df.copy() if not uk_df.empty else pd.DataFrame()

    def _add_country_growth_fields(df):
        if df is None or df.empty:
            return df

        df = df.copy()

        for col in ["net_sales", "quantity", "profit"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

        if "quantity" in df.columns and "profit" in df.columns:
            df["unit_wise_profitability"] = df.apply(
                lambda r: float(r["profit"]) / float(r["quantity"])
                if float(r.get("quantity", 0) or 0) else 0,
                axis=1,
            )

        if "net_sales" in df.columns:
            total_net_sales = float(
                pd.to_numeric(df["net_sales"], errors="coerce").fillna(0).sum()
            )

            if total_net_sales:
                df["sales_mix"] = (
                    pd.to_numeric(df["net_sales"], errors="coerce").fillna(0)
                    / total_net_sales
                    * 100
                )
            else:
                df["sales_mix"] = 0.0

        return df
    
    def _recalc_asp_after_currency_conversion(df):
        if df is None or df.empty:
            return df

        df = df.copy()

        for col in ["net_sales", "total_quantity", "quantity", "return_quantity"]:
            if col not in df.columns:
                df[col] = 0.0
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

        net_units = df["total_quantity"].where(
            df["total_quantity"] != 0,
            (df["quantity"] - df["return_quantity"]).clip(lower=0)
        )

        df["asp"] = df["net_sales"] / net_units.replace(0, pd.NA)
        df["asp"] = pd.to_numeric(df["asp"], errors="coerce").fillna(0.0)

        return df

    us_response_df = _add_country_growth_fields(us_response_df)
    uk_response_df = _add_country_growth_fields(uk_response_df)

    if not us_gt.empty:
        us_gt_response = us_gt.copy()
        us_gt_response = recalc_response_grand_total(us_gt_response)

        us_gt_response["country"] = "us"
        us_gt_response["month"] = month_name
        us_gt_response["year"] = now_utc.year
        us_gt_response["user_id"] = user_id
        us_gt_response["generated_at_utc"] = now_utc.isoformat()

        us_response_df = pd.concat([us_response_df, us_gt_response], ignore_index=True)

    if not uk_gt.empty:
        uk_gt_response = uk_gt.copy()

        for col in money_cols:
            if col in uk_gt_response.columns:
                uk_gt_response[col] = (
                    pd.to_numeric(uk_gt_response[col], errors="coerce").fillna(0)
                    * float(uk_to_usd_rate)
                )

        uk_gt_response = recalc_response_grand_total(uk_gt_response)

        uk_gt_response["country"] = "uk"
        uk_gt_response["month"] = month_name
        uk_gt_response["year"] = now_utc.year
        uk_gt_response["user_id"] = user_id
        uk_gt_response["generated_at_utc"] = now_utc.isoformat()

        uk_response_df = pd.concat([uk_response_df, uk_gt_response], ignore_index=True)

    skuwise_items_us = (
        us_response_df.replace({np.nan: None}).to_dict(orient="records")
        if us_response_df is not None and not us_response_df.empty
        else []
    )

    skuwise_items_uk = (
        uk_response_df.replace({np.nan: None}).to_dict(orient="records")
        if uk_response_df is not None and not uk_response_df.empty
        else []
    )

    # -------------------------------------------------------------------------
    # COMBINE ONLY AVAILABLE COUNTRIES
    # -------------------------------------------------------------------------
    frames_to_combine = []

    if not us_df.empty:
        frames_to_combine.append(us_df)

    if not uk_df.empty:
        frames_to_combine.append(uk_df)

    combined_df = pd.concat(frames_to_combine, ignore_index=True)

    if not combined_df.empty:
        if "sku" not in combined_df.columns:
            combined_df["sku"] = ""

        if "product_name" not in combined_df.columns:
            combined_df["product_name"] = ""

        sku_upper = combined_df["sku"].fillna("").astype(str).str.strip().str.upper()
        product_lower = combined_df["product_name"].fillna("").astype(str).str.strip().str.lower()

        combined_df = combined_df[
            ~(
                sku_upper.isin(["GRAND_TOTAL", "TOTAL"])
                | product_lower.isin(["grand total", "total"])
            )
        ].copy()

    if "product_name" not in combined_df.columns:
        combined_df["product_name"] = ""

    if "sku" not in combined_df.columns:
        combined_df["sku"] = ""

    sum_cols = combined_df.select_dtypes(include=["number"]).columns.tolist()

    for remove_col in ["user_id", "year"]:
        if remove_col in sum_cols:
            sum_cols.remove(remove_col)

    combined_df["product_name"] = (
        combined_df["product_name"]
        .fillna("")
        .astype(str)
        .str.strip()
        .str.lower()
    )

    combined_df["product_name_group"] = combined_df.apply(
        lambda r: r["product_name"] if r["product_name"] else str(r["sku"]),
        axis=1
    )

    global_df = combined_df.groupby("product_name_group", as_index=False)[sum_cols].sum()
    global_df.rename(columns={"product_name_group": "product_name"}, inplace=True)
    global_df["sku"] = ""

    # ---------------- GLOBAL P&L FORMULA FIX ----------------
    # Make global use the same formula as UK/US productwise table.

    for col in [
        "quantity",
        "net_sales",
        "cogs",
        "marketplace_fees",
        "tax_and_credits",
        "ads_spend",
        "product_spend",
        "display_spend",
        "misc_transaction",
    ]:
        if col not in global_df.columns:
            global_df[col] = 0.0
        global_df[col] = pd.to_numeric(global_df[col], errors="coerce").fillna(0.0)

    # If ads_spend is missing / stale, rebuild it from product + display.
    global_df["ads_spend"] = (
        pd.to_numeric(global_df["product_spend"], errors="coerce").fillna(0.0)
        + pd.to_numeric(global_df["display_spend"], errors="coerce").fillna(0.0)
    )

    # Other Transactions = US other + UK other converted to USD
    # Do NOT rebuild this from tax_and_credits globally.
    if "other" not in global_df.columns:
        global_df["other"] = 0.0

    global_df["other"] = pd.to_numeric(
        global_df["other"],
        errors="coerce"
    ).fillna(0.0)

    # CM1 Profit = Net Sales - COGS - Marketplace Fees + Tax and Credits
    global_df["profit"] = (
        pd.to_numeric(global_df["net_sales"], errors="coerce").fillna(0.0)
        - pd.to_numeric(global_df["cogs"], errors="coerce").fillna(0.0)
        - pd.to_numeric(global_df["marketplace_fees"], errors="coerce").fillna(0.0)
        + pd.to_numeric(global_df["tax_and_credits"], errors="coerce").fillna(0.0)
    ).round(2)

    # CM2 Profit = CM1 Profit - Ads Spend
    global_df["cm2_profit"] = (
        pd.to_numeric(global_df["profit"], errors="coerce").fillna(0.0)
        - pd.to_numeric(global_df["ads_spend"], errors="coerce").fillna(0.0)
    ).round(2)

    def _net_units_for_asp(row):
        total_quantity = float(row.get("total_quantity", 0) or 0)

        if total_quantity:
            return total_quantity

        quantity = float(row.get("quantity", 0) or 0)
        return_quantity = float(row.get("return_quantity", 0) or 0)

        return max(quantity - return_quantity, 0)


    global_df["asp"] = global_df.apply(
        lambda r: (
            float(r.get("net_sales", 0) or 0) / _net_units_for_asp(r)
            if _net_units_for_asp(r)
            else 0
        ),
        axis=1
    )

    global_df["cm1_profit_per_unit"] = global_df.apply(
        lambda r: float(r["profit"]) / float(r["quantity"])
        if float(r.get("quantity", 0) or 0) else 0,
        axis=1
    )

    # IMPORTANT:
    # calculate_growth() expects this exact field name.
    global_df["unit_wise_profitability"] = global_df.apply(
        lambda r: float(r["profit"]) / float(r["quantity"])
        if float(r.get("quantity", 0) or 0) else 0,
        axis=1
    )

    global_df["cm1_profit_per"] = global_df.apply(
        lambda r: float(r["profit"]) / float(r["net_sales"]) * 100
        if float(r.get("net_sales", 0) or 0) else 0,
        axis=1
    )

    # IMPORTANT:
    # calculate_growth() uses this to produce Sales Mix (Current).
    total_global_net_sales = float(
        pd.to_numeric(global_df["net_sales"], errors="coerce").fillna(0).sum()
    ) if "net_sales" in global_df.columns else 0.0

    if total_global_net_sales:
        global_df["sales_mix"] = (
            pd.to_numeric(global_df["net_sales"], errors="coerce").fillna(0)
            / total_global_net_sales
            * 100
        )
    else:
        global_df["sales_mix"] = 0.0

    global_df["cm2_profit_per_unit"] = global_df.apply(
        lambda r: float(r["cm2_profit"]) / float(r["quantity"])
        if float(r.get("quantity", 0) or 0) else 0,
        axis=1
    )

    global_df["cm2_profit_per"] = global_df.apply(
        lambda r: float(r["cm2_profit"]) / float(r["net_sales"]) * 100
        if float(r.get("net_sales", 0) or 0) else 0,
        axis=1
    )

    global_df["country"] = "global"
    global_df["month"] = month_name
    global_df["year"] = now_utc.year
    global_df["user_id"] = user_id
    global_df["generated_at_utc"] = now_utc.isoformat()

    # Recompute numeric columns after derived columns were added.
    total_sum_cols = global_df.select_dtypes(include=["number"]).columns.tolist()
    for remove_col in ["user_id", "year"]:
        if remove_col in total_sum_cols:
            total_sum_cols.remove(remove_col)

    total_row = {"sku": "GRAND_TOTAL", "product_name": "Grand Total"}

    for col in total_sum_cols:
        total_row[col] = float(global_df[col].sum()) if col in global_df.columns else 0

    total_qty = float(total_row.get("total_quantity", 0) or 0)

    if not total_qty:
        total_qty = max(
            float(total_row.get("quantity", 0) or 0)
            - float(total_row.get("return_quantity", 0) or 0),
            0
        )

    total_net_sales = float(total_row.get("net_sales", 0) or 0)

    # Global total Other Transactions = sum of country-wise Other Transactions
    total_row["other"] = round(
        float(pd.to_numeric(global_df["other"], errors="coerce").fillna(0.0).sum())
        if "other" in global_df.columns else 0.0,
        2,
    )

    # Global total CM1 Profit
    # CM1 = Net Sales - COGS - Marketplace Fees + Tax and Credits
    total_row["profit"] = round(
        float(total_row.get("net_sales", 0.0) or 0.0)
        - float(total_row.get("cogs", 0.0) or 0.0)
        - float(total_row.get("marketplace_fees", 0.0) or 0.0)
        + float(total_row.get("tax_and_credits", 0.0) or 0.0),
        2,
    )

    # Global total CM2 Profit
    total_row["cm2_profit"] = round(
        float(total_row.get("profit", 0.0) or 0.0)
        - float(total_row.get("ads_spend", 0.0) or 0.0),
        2,
    )

    total_profit = float(total_row.get("profit", 0) or 0)
    total_cm2 = float(total_row.get("cm2_profit", 0) or 0)

    total_row["asp"] = total_net_sales / total_qty if total_qty else 0
    total_row["cm1_profit_per_unit"] = total_profit / total_qty if total_qty else 0
    total_row["unit_wise_profitability"] = total_profit / total_qty if total_qty else 0
    total_row["cm1_profit_per"] = total_profit / total_net_sales * 100 if total_net_sales else 0
    total_row["cm2_profit_per_unit"] = total_cm2 / total_qty if total_qty else 0
    total_row["cm2_profit_per"] = total_cm2 / total_net_sales * 100 if total_net_sales else 0
    total_row["sales_mix"] = 100.0 if total_net_sales else 0.0

    # total_row["acos"] = round(
    #     (float(total_row.get("ads_spend", 0.0) or 0.0) / total_net_sales * 100)
    #     if total_net_sales else 0,
    #     2
    # )

    total_row["country"] = "global"
    total_row["month"] = month_name
    total_row["year"] = now_utc.year
    total_row["user_id"] = user_id
    total_row["generated_at_utc"] = now_utc.isoformat()

    amazon_fees = (
        abs(float(total_row.get("selling_fees", 0.0) or 0.0))
        + abs(float(total_row.get("fba_fees", 0.0) or 0.0))
    )

    total_row["product_spend"] = round(global_product_spend, 2)
    total_row["display_spend"] = round(global_display_spend, 2)
    total_row["ads_spend"] = round(global_ads_spend, 2)
    total_row["ads_spend_raw"] = round(global_ads_spend, 2)

    total_row["acos"] = round(
        (float(total_row.get("ads_spend", 0.0) or 0.0) / total_net_sales * 100)
        if total_net_sales else 0,
        2
    )

    total_row["brand_spend"] = round(global_brand_spend, 2)
    total_row["platform_fee_inventory_storage"] = round(global_platform_fee_inventory_storage, 2)
    total_row["platformfeenew"] = round(global_platformfeenew, 2)
    total_row["dealsvouchar_ads"] = round(global_dealsvouchar_ads, 2)
    total_row["misc_transaction"] = round(global_misc_transaction, 2)

    other_transactions_total = (
        abs(float(total_row.get("misc_transaction", 0.0) or 0.0))
        + abs(float(total_row.get("lost_total", 0.0) or 0.0))
        - abs(float(total_row.get("platform_fee_inventory_storage", 0.0) or 0.0))
        - abs(float(total_row.get("platformfeenew", 0.0) or 0.0))
    )

    total_row["platform_fee"] = round(-other_transactions_total, 2)

    product_ads_total = abs(float(total_row.get("ads_spend", 0.0) or 0.0))

    cost_ads_total = (
        abs(float(total_row.get("brand_spend", 0.0) or 0.0))
        + abs(float(total_row.get("dealsvouchar_ads", 0.0) or 0.0))
    )

    shipment_charges_total = abs(float(total_row.get("shipment_fees", 0.0) or 0.0))
    total_ads = product_ads_total + cost_ads_total

    # Productwise CM2 Profit
    # CM2 Profit = CM1 Profit - Ads Spend
    cm2_profit_productwise = (
        float(total_row.get("profit", 0.0) or 0.0)
        - float(total_row.get("ads_spend", 0.0) or 0.0)
    )

    # Global Total CM2 Profit
    # total_cm2_profit = cm2_profit - brand_spend - dealsvouchar_ads - abs(platform_fee) - shipment_fees
    total_cm2_profit = (
        cm2_profit_productwise
        - abs(float(total_row.get("brand_spend", 0.0) or 0.0))
        - abs(float(total_row.get("dealsvouchar_ads", 0.0) or 0.0))
        - abs(float(total_row.get("platform_fee", 0.0) or 0.0))
        - abs(float(total_row.get("shipment_fees", 0.0) or 0.0))
    )

    total_row["cm2_profit"] = round(cm2_profit_productwise, 2)
    total_row["total_cm2_profit"] = round(total_cm2_profit, 2)

    total_cm2_margins = (
        total_cm2_profit / float(total_row.get("net_sales", 0.0) or 0.0) * 100.0
    ) if float(total_row.get("net_sales", 0.0) or 0.0) else 0.0

    tacos = (
        total_ads / total_net_sales * 100
    ) if total_net_sales else 0

    ads_clicks = float(total_row.get("ads_clicks", 0.0) or 0.0)
    ads_sale_units = float(total_row.get("ads_sale_units", 0.0) or 0.0)
    ads_sale_amount = float(total_row.get("ads_sale_amount", 0.0) or 0.0)

    total_row["amazon_fees"] = round(amazon_fees, 2)
    total_row["advertising_fees"] = round(total_ads, 2)
    total_row["profit_percentage"] = round(
        (total_cm2_profit / total_net_sales * 100) if total_net_sales else 0,
        2
    )
    total_row["current_net_reimbursement"] = round(global_current_net_reimbursement, 2)
    total_row["debt_payment"] = round(global_debt_payment, 2)
    total_row["disbursement"] = round(global_disbursement, 2)

    total_row["reimbursement_vs_cm2_margins"] = round(
        (global_current_net_reimbursement / total_cm2_profit * 100)
        if total_cm2_profit else 0,
        2
    )

    total_row["reimbursement_vs_sales"] = round(
        (global_current_net_reimbursement / total_net_sales * 100)
        if total_net_sales else 0,
        2
    )

    total_row["total_ads"] = round(total_ads, 2)
    total_row["total_cm2_profit"] = round(total_cm2_profit, 2)
    total_row["total_cm2_margins"] = round(total_cm2_margins, 2)
    total_row["tacos_total_advertising_cost_of_sale"] = round(tacos, 2)

    total_row["ads_conversion_rate"] = round(
        (ads_sale_units / ads_clicks * 100) if ads_clicks else 0,
        2
    )
    total_row["ads_roas"] = round(
        (ads_sale_amount / total_ads) if total_ads else 0,
        2
    )
    total_row["ads_acos"] = round(
        (total_ads / ads_sale_amount * 100) if ads_sale_amount else 0,
        2
    )

    global_df = pd.concat([global_df, pd.DataFrame([total_row])], ignore_index=True)

    # Keep DB write for global table.
    global_df.to_sql(
        global_table,
        PHORMULA_ENGINE,
        schema="public",
        if_exists="replace",
        index=False,
        method="multi",
        chunksize=1000,
    )

    derived_totals_global = {
        "amazon_fees": round(amazon_fees, 2),
        "platform_fee": round(float(total_row.get("platform_fee", 0.0) or 0.0), 2),
        "advertising_fees": round(total_ads, 2),
        "ads_spend": round(float(total_row.get("ads_spend", 0.0) or 0.0), 2),
        "misc_transaction": round(float(total_row.get("misc_transaction", 0.0) or 0.0), 2),
        "product_spend": round(float(total_row.get("product_spend", 0.0) or 0.0), 2),
        "display_spend": round(float(total_row.get("display_spend", 0.0) or 0.0), 2),
        "brand_spend": round(float(total_row.get("brand_spend", 0.0) or 0.0), 2),
        "net_sales": round(total_net_sales, 2),
        "gross_sales": round(float(total_row.get("gross_sales", 0.0) or 0.0), 2),
        "asp": round(float(total_row.get("asp", 0.0) or 0.0), 2),
        "profit": round(float(total_row.get("profit", 0.0) or 0.0), 2),
        "tax_and_credits": round(float(total_row.get("tax_and_credits", 0.0) or 0.0), 2),
        "cm2_profit": round(cm2_profit_productwise, 2),
        "profit_percentage": round(
            (total_cm2_profit / total_net_sales * 100) if total_net_sales else 0,
            2
        ),
        "current_net_reimbursement": round(global_current_net_reimbursement, 2),
        "debt_payment": round(float(total_row.get("debt_payment", 0.0) or 0.0), 2),
        "disbursement": round(float(total_row.get("disbursement", 0.0) or 0.0), 2),
        "total_ads": round(total_ads, 2),
        "total_cm2_profit": round(total_cm2_profit, 2),
        "total_cm2_margins": round(total_cm2_margins, 2),
        "tacos_total_advertising_cost_of_sale": round(tacos, 2),
        "reimbursement_vs_cm2_margins": total_row["reimbursement_vs_cm2_margins"],
        "reimbursement_vs_sales": total_row["reimbursement_vs_sales"],
    }

    payload_country = "global" if len(available_countries) > 1 else available_countries[0]

    payload_out = {
        "success": True,
        "country": payload_country,
        "requested_country": "global",
        "available_countries": available_countries,
        "message": (
            "Live GLOBAL data built from UK and US"
            if len(available_countries) > 1
            else f"Live GLOBAL fallback data built from {available_countries[0].upper()} only"
        ),
        "conversion": {
            "from": "GBP",
            "to": "USD",
            "rate": uk_to_usd_rate,
            "month": month_name,
            "year": now_utc.year,
        },
        "skuwise_tables": {
            "us": {
                "name": us_table,
                "saved": False,
                "rows": len(us_response_df) if us_response_df is not None else 0,
                "available": "us" in available_countries,
            },
            "uk": {
                "name": uk_table,
                "saved": False,
                "rows": len(uk_response_df) if uk_response_df is not None else 0,
                "available": "uk" in available_countries,
                "currency": "USD",
                "converted_from": "GBP",
                "conversion_rate": uk_to_usd_rate,
            },
            "global": {
                "name": global_table,
                "saved": True,
                "rows": len(global_df),
                "available": True,
            },
        },
        "skuwise_table": {
            "name": global_table,
            "saved": True,
            "rows": len(global_df),
        },
        "derived_totals_global": derived_totals_global,
        "skuwise_items_us": skuwise_items_us,
        "skuwise_items_uk": skuwise_items_uk,
        "skuwise_items_global": global_df.replace({np.nan: None}).to_dict(orient="records"),
    }

    return payload_out

@amazon_api_bp.route("/amazon_api/finances/mtd_transactions", methods=["GET"])
def finances_mtd_transactions():
    import io
    import math
    import re
    import numpy as np
    import pandas as pd
    from datetime import datetime, timezone

    def _json_safe(obj):
        """Recursively convert NaN/Inf to None so jsonify returns valid JSON."""
        if obj is None:
            return None
        if isinstance(obj, float):
            return obj if math.isfinite(obj) else None
        if isinstance(obj, dict):
            return {k: _json_safe(v) for k, v in obj.items()}
        if isinstance(obj, (list, tuple)):
            return [_json_safe(x) for x in obj]
        return obj

    # ---------------- Auth ----------------
    auth_header = request.headers.get("Authorization")
    if not auth_header or not auth_header.startswith("Bearer "):
        return jsonify({"success": False, "error": "Authorization token is missing or invalid"}), 401

    token = auth_header.split(" ")[1]
    try:
        payload, user_id, member_id = get_effective_user_id_from_token(token)
        user_id = int(payload["user_id"])
    except jwt.ExpiredSignatureError:
        return jsonify({"success": False, "error": "Token has expired"}), 401
    except jwt.InvalidTokenError:
        return jsonify({"success": False, "error": "Invalid token"}), 401

    # ---------------- Params ----------------
    # ---------------- Params ----------------
    marketplace_id = request.args.get("marketplace_id")
    transaction_type_filter = request.args.get("transaction_type")
    response_format = (request.args.get("format") or "json").lower()
    store_in_db = (request.args.get("store_in_db", "true").lower() != "false")
    
    ui_country = (request.args.get("country") or "").strip().lower() or "uk"

    if ui_country == "global":
        payload_out = get_current_global_data_for_live_bi(user_id)
        return jsonify(_json_safe(payload_out)), 200

    if ui_country in ("us", "usa", "united_states"): 
        transaction_status = "RELEASED"
    else:
        transaction_status = request.args.get("transaction_status", "RELEASED")

    # ---------------- Region + marketplace ----------------
    _apply_region_and_marketplace_from_request()

    au = amazon_user.query.filter_by(
        user_id=user_id,
        marketplace_id=amazon_client.marketplace_id
    ).first()
    if not au or not au.refresh_token:
        return (
            jsonify(
                {
                    "success": False,
                    "error": "Amazon account not connected for this region",
                    "status": "no_refresh_token",
                }
            ),
            400,
        )

    amazon_client.refresh_token = au.refresh_token

    now_utc = datetime.now(timezone.utc)
    posted_after, posted_before = _month_to_date_range_utc_safe(now_utc, safety_minutes=10)

    # ---------------- Month meta ----------------
    month_name = _month_name_lower(now_utc.month)  # e.g. "february"

    # ---------------- COGS meta ----------------
    user_currency = DEFAULT_SKU_PRICE_CURRENCY
    selected_currency = COUNTRY_TO_SELECTED_CURRENCY.get(ui_country, user_currency)

    sku_price_map = fetch_sku_price_map(user_id=user_id, country=ui_country)
    conversion_rate_fx = fetch_conversion_rate(
        country=ui_country,
        year=now_utc.year,
        month_name=month_name,
        user_currency=user_currency,
        selected_currency=selected_currency,
    )

    # ---------------- Fetch MTD ----------------
    params = {
        "postedAfter": posted_after,
        "postedBefore": posted_before,
        "marketplaceId": marketplace_id or amazon_client.marketplace_id,
    }
    if transaction_status and transaction_status.lower() not in ("all", ""):
        params["transactionStatus"] = transaction_status

    all_rows = []

    while True:
        res = amazon_client.make_api_call(
            "/finances/2024-06-19/transactions",
            method="GET",
            params=params,
        )
        if not res or "error" in res:
            return jsonify({"success": False, "error": res or {"error": "Unknown SP-API error"}}), 502

        payload_res = res.get("payload") or res
        transactions = payload_res.get("transactions") or []

        for tx in transactions:
            tstatus = (tx or {}).get("transactionStatus")
            ttype = (tx or {}).get("transactionType")

            if transaction_status and transaction_status.lower() not in ("all", ""):
                if tstatus != transaction_status:
                    continue
            if transaction_type_filter and ttype != transaction_type_filter:
                continue

            row = _flatten_transaction_to_row(tx or {})

            sku = (row.get("sku") or "").strip()
            qty = _i(row.get("quantity")) or 0
            price = sku_price_map.get(sku) if sku else None
            row["cogs"] = (
                float(qty) * float(price) * float(conversion_rate_fx)
                if (price is not None and qty > 0)
                else 0.0
            )

            all_rows.append(row)

        next_token = payload_res.get("nextToken")
        if not next_token:
            break
        params = {"nextToken": next_token}

    # ✅ profit per row
    add_profit_column_from_uk_profit(all_rows, country=ui_country)

    # ✅ gross_sales per row
    for r in all_rows:
        r["gross_sales"] = (
            float(r.get("product_sales", 0.0))
            + float(r.get("product_sales_tax", 0.0))
            + float(r.get("postage_credits", 0.0))
            + float(r.get("gift_wrap_credits", 0.0))
            + float(r.get("shipping_credits_tax", 0.0))
            + float(r.get("giftwrap_credits_tax", 0.0))
            - float(r.get("promotional_rebates", 0.0))
            - float(r.get("promotional_rebates_tax", 0.0))
        )

    marketplace_name = "Amazon.com" if ui_country in ("us", "usa", "united_states") else "Amazon.co.uk"
    # ---------------- Store raw liveorders ----------------
    db_result = None
    if store_in_db:
        try:
            with AMAZON_ENGINE.begin() as conn:
                conn.execute(text("""
                    DELETE FROM public.liveorders
                    WHERE user_id = :user_id
                    AND marketplace = :marketplace
                """), {
                    "user_id": user_id,
                    "marketplace": marketplace_name
                })

            db_result = upsert_liveorders_from_rows(
                all_rows,
                user_id=user_id,
                country=ui_country,
                now_utc=now_utc,
            )

        except Exception as e:
            db.session.rollback()
            print("MTD DB STORE ERROR:", str(e))
            return jsonify({"success": False, "error": f"DB store failed: {str(e)}"}), 500

    # ---------------- totals ----------------
    totals = compute_totals(all_rows)

    tax_and_credits_total = (
        float(totals.get("postage_credits", 0.0))
        + float(totals.get("gift_wrap_credits", 0.0))
        + float(totals.get("product_sales_tax", 0.0))
        + float(totals.get("shipping_credits_tax", 0.0))
        + float(totals.get("promotional_rebates_tax", 0.0))
        + float(totals.get("marketplace_facilitator_tax", 0.0))
    )
    totals["tax_and_credits"] = round(tax_and_credits_total, 2)

    selling_fees = float(totals.get("selling_fees", 0.0))
    fba_fees = float(totals.get("fba_fees", 0.0))
    amazon_fees = abs(selling_fees) + abs(fba_fees)

    gross_sales_total = float(totals.get("gross_sales", 0.0))
    net_sales = float(totals.get("product_sales", 0.0)) + float(totals.get("promotional_rebates", 0.0))

    qty_total = float(totals.get("quantity", 0.0)) or 0.0
    return_qty_total = 0.0

    df_all_for_asp = pd.DataFrame(all_rows) if all_rows else pd.DataFrame()

    if not df_all_for_asp.empty and "quantity" in df_all_for_asp.columns:
        for col in ["description", "type", "transaction_type"]:
            if col not in df_all_for_asp.columns:
                df_all_for_asp[col] = ""

        desc_lower = df_all_for_asp["description"].fillna("").astype(str).str.lower()
        type_lower = df_all_for_asp["type"].fillna("").astype(str).str.lower()
        transaction_type_lower = df_all_for_asp["transaction_type"].fillna("").astype(str).str.lower()

        return_mask = (
            desc_lower.str.contains("refund|return", case=False, na=False, regex=True)
            | type_lower.str.contains("refund|return", case=False, na=False, regex=True)
            | transaction_type_lower.str.contains("refund|return", case=False, na=False, regex=True)
        )

        return_qty_total = float(
            pd.to_numeric(df_all_for_asp.loc[return_mask, "quantity"], errors="coerce")
            .fillna(0.0)
            .abs()
            .sum()
        )

    net_qty_total = max(qty_total - return_qty_total, 0.0)

    asp = (net_sales / net_qty_total) if net_qty_total else 0.0

    profit_total = float(totals.get("profit", 0.0))

    # ---------------- platform + advertising fees (dashboard) ----------------
    df_all = pd.DataFrame(all_rows) if all_rows else pd.DataFrame()

    platform_fee_total = 0.0
    advertising_fee_total = 0.0
    shipment_fees = 0.0

    # totals stored in SKU-wise table
    platformfeenew_total = 0.0
    platform_fee_inventory_storage_total = 0.0
    dealsvouchar_ads_total = 0.0
    lost_total_df = pd.DataFrame(columns=["sku", "lost_total"])
    misc_transaction_df = pd.DataFrame(columns=["sku", "misc_transaction"])
    misc_transaction_total = 0.0

    if not df_all.empty:
        for col, default in [
            ("description", ""),
            ("total", 0.0),
            ("platform_fees", 0.0),
            ("advertising_cost", 0.0),
            ("sku", ""),
            ("type", ""),
        ]:
            if col not in df_all.columns:
                df_all[col] = default

        platform_fee_total, _, _ = uk_platform_fee(df_all, country=ui_country, want_breakdown=False)
        advertising_fee_total, _, _ = uk_advertising(df_all, country=ui_country, want_breakdown=False)

        desc_all = df_all["description"].fillna("").astype(str)

        def sum_total_where_desc_contains(keywords):
            if "total" not in df_all.columns:
                return 0.0
            pattern = "|".join([re.escape(k) for k in keywords])
            mask = desc_all.str.contains(pattern, case=False, na=False, regex=True)
            return float(pd.to_numeric(df_all.loc[mask, "total"], errors="coerce").fillna(0.0).sum())
        
        # ---------------- SHIPMENT FEES ----------------
        shipment_keywords = [
            "FBA international shipping charge",
            "FBA Inbound Placement Service Fee",
            "FBA international shipping customs charge",
            "FBAInboundConvenience",
        ]

        shipment_fees = abs(
            sum_total_where_desc_contains(shipment_keywords)
)

        platformfeenew_total = sum_total_where_desc_contains(["Subscription"])

        platform_fee_inventory_storage_total = sum_total_where_desc_contains(
            [
                "FBA Return Fee",
                "FBA Long-Term Storage Fee",
                "FBA storage fee",
                "FBADisposal",
                "FBAStorageBilling",
                "FBALongTermStorageBilling",
                "INCORRECT_FEES_NON_ITEMIZED",
                "StorageReservationBilling",
            ]
        )

        dealsvouchar_ads_total = sum_total_where_desc_contains(
            [
                "Cost of Advertising",
                "Coupon Redemption Fee",
                "Deals",
                "Lightning Deal",
                "CouponPerformanceEvent",
                "CouponParticipationEvent",
                "SellerDealComplete",
                "VineCharge",
                "SellerPoweredCoupon",
                "DealParticipationEvent",
                "DealPerformanceEvent",
            ]
        )

        LOST_DESCRIPTIONS = {
            "REVERSAL_REIMBURSEMENT",
            "WAREHOUSE_LOST",
            "WAREHOUSE_DAMAGE",
            "MISSING_FROM_INBOUND",
            "MISSING_FROM_INBOUND_CLAWBACK",
            "COMPENSATED_CLAWBACK",
            "FREE_REPLACEMENT_REFUND_ITEMS",
        }
        lost_mask = df_all["description"].fillna("").astype(str).str.strip().isin(LOST_DESCRIPTIONS)

        tmp = df_all.loc[lost_mask, ["sku", "total"]].copy()
        tmp["sku"] = tmp["sku"].fillna("").astype(str).str.strip()
        tmp["total"] = pd.to_numeric(tmp["total"], errors="coerce").fillna(0.0)

        lost_total_df = (
            tmp[tmp["sku"] != ""]
            .groupby("sku", as_index=False)["total"]
            .sum()
            .rename(columns={"total": "lost_total"})
        )

        # Store lost_total as positive value
        lost_total_df["lost_total"] = (
            pd.to_numeric(lost_total_df["lost_total"], errors="coerce")
            .fillna(0.0)
            .abs()
        )
        # ---------------- MISC TRANSACTION ----------------

        def _norm_key(x):
            return re.sub(r"\s+", " ", str(x or "").strip()).casefold()

        df_all["sku"] = df_all["sku"].fillna("").astype(str).str.strip()
        df_all["description"] = df_all["description"].fillna("").astype(str).str.strip()
        df_all["total"] = pd.to_numeric(df_all["total"], errors="coerce").fillna(0.0)

        # Some rows may have transaction type in transaction_type instead of type
        if "type" not in df_all.columns:
            df_all["type"] = ""

        df_all["type_norm"] = df_all["type"].fillna("").astype(str).str.strip()

        if "transaction_type" in df_all.columns:
            df_all["type_norm"] = df_all["type_norm"].where(
                df_all["type_norm"] != "",
                df_all["transaction_type"].fillna("").astype(str).str.strip()
            )

        df_all["desc_key"] = df_all["description"].map(_norm_key)
        df_all["type_key"] = df_all["type_norm"].map(_norm_key)

        EXCLUDE_DESCRIPTIONS = {
            # Advertising / deals
            "Cost of Advertising",
            "Coupon Redemption Fee",
            "Deals",
            "Lightning Deal",
            "ProductAdsPayment",
            "CouponPerformanceEvent",
            "CouponParticipationEvent",
            "SellerDealComplete",
            "VineCharge",
            "SellerPoweredCoupon",
            "DealParticipationEvent",
            "DealPerformanceEvent",

            # Platform / storage / shipment buckets
            "FBA Return Fee",
            "FBA Long-Term Storage Fee",
            "FBA storage fee",
            "FBADisposal",
            "FBAStorageBilling",
            "FBALongTermStorageBilling",
            "INCORRECT_FEES_NON_ITEMIZED",
            "StorageReservationBilling",
            "Subscription",
            "FBAInboundConvenience",

            # Normal payment / refund / transfer buckets
            "Order Payment",
            "Refund",
            "Disbursement",
            "DebtPayment",

            # Lost / reimbursement bucket
            "REVERSAL_REIMBURSEMENT",
            "WAREHOUSE_LOST",
            "WAREHOUSE_DAMAGE",
            "MISSING_FROM_INBOUND",
            "MISSING_FROM_INBOUND_CLAWBACK",
            "COMPENSATED_CLAWBACK",
            "FREE_REPLACEMENT_REFUND_ITEMS",
        }

        EXCLUDE_TYPES = {
            "Transfer",
            "Refund",
        }

        exclude_desc_keys = {_norm_key(x) for x in EXCLUDE_DESCRIPTIONS}
        exclude_type_keys = {_norm_key(x) for x in EXCLUDE_TYPES}

        leftout_mask = (
            ~df_all["desc_key"].isin(exclude_desc_keys)
            & ~df_all["type_key"].isin(exclude_type_keys)
        )

        # Logic 1: GRAND_TOTAL misc_transaction
        # Includes rows with SKU and rows without SKU
        misc_transaction_total = abs(
            pd.to_numeric(df_all.loc[leftout_mask, "total"], errors="coerce")
            .fillna(0.0)
            .sum()
        )

        # Logic 2: SKU-wise misc_transaction
        # Only rows with SKU can be merged into df_sku
        tmp_misc = df_all.loc[
            leftout_mask
            & df_all["sku"].notna()
            & (df_all["sku"] != "")
            & (df_all["sku"] != "0"),
            ["sku", "total"]
        ].copy()

        misc_transaction_df = (
            tmp_misc.groupby("sku", as_index=False)["total"]
            .sum()
            .rename(columns={"total": "misc_transaction"})
        )

        misc_transaction_df["misc_transaction"] = pd.to_numeric(
            misc_transaction_df["misc_transaction"],
            errors="coerce"
        ).fillna(0.0)

    platform_fee_total = float(platform_fee_total or 0.0)
    advertising_fee_total = float(advertising_fee_total or 0.0)

    cm2_profit_dashboard = profit_total - advertising_fee_total - platform_fee_total
    profit_percentage = (cm2_profit_dashboard / net_sales * 100) if net_sales else 0.0
    current_net_reimbursement = compute_net_reimbursement_from_df(df_all) if not df_all.empty else 0.0
    payment_breakup = (
        compute_debt_payment_disbursement_from_df(df_all)
        if not df_all.empty
        else {"debt_payment": 0.0, "disbursement": 0.0}
    )

    debt_payment_total = payment_breakup["debt_payment"]
    disbursement_total = payment_breakup["disbursement"]
    # ================= NEW CALCULATIONS =================

    total_ads = float(advertising_fee_total or 0.0)

    total_cm2_profit = float(cm2_profit_dashboard or 0.0)

    total_cm2_margins = (
        total_cm2_profit / float(net_sales) * 100.0
    ) if float(net_sales or 0.0) else 0.0

    tacos = (
        total_ads / float(net_sales) * 100.0
    ) if float(net_sales or 0.0) else 0.0

    reimbursement_vs_cm2_margins = (
        float(current_net_reimbursement or 0.0) / total_cm2_profit * 100.0
    ) if total_cm2_profit else 0.0

    reimbursement_vs_sales = (
        float(current_net_reimbursement or 0.0) / float(net_sales) * 100.0
    ) if float(net_sales or 0.0) else 0.0

    derived_totals = {
        "amazon_fees": round(amazon_fees, 2),
        "platform_fee": round(platform_fee_total, 2),
        "advertising_fees": round(advertising_fee_total, 2),
        "shipment_fees": round(float(shipment_fees or 0.0), 2),
        "net_sales": round(net_sales, 2),
        "gross_sales": round(gross_sales_total, 2),
        "asp": round(asp, 2),
        "profit": round(profit_total, 2),
        "cm2_profit": round(cm2_profit_dashboard, 2),
        "profit_percentage": round(profit_percentage, 2),
        "current_net_reimbursement": round(float(current_net_reimbursement or 0.0), 2),
        "debt_payment": round(float(debt_payment_total or 0.0), 2),
        "disbursement": round(float(disbursement_total or 0.0), 2),
        # new columns
        "total_ads": round(total_ads, 2),
        "total_cm2_profit": round(total_cm2_profit, 2),
        "total_cm2_margins": round(total_cm2_margins, 2),
        "tacos_total_advertising_cost_of_sale": round(tacos, 2),
        "reimbursement_vs_cm2_margins": round(reimbursement_vs_cm2_margins, 2),
        "reimbursement_vs_sales": round(reimbursement_vs_sales, 2),
    }

    previous_period = get_previous_month_mtd_payload(user_id=user_id, country=ui_country, now_utc=now_utc)

    # ============================================================
    # SKU-WISE TABLE
    # ============================================================
    skuwise_table_name = f"skuwisemonthly_{int(user_id)}_{_safe_ident(ui_country)}_{_safe_ident(month_name)}_{int(now_utc.year)}"
    ads_table_name = _build_adsmonthly_table_name(user_id, ui_country, now_utc.month, now_utc.year)

    sku_summary_saved = False
    sku_summary_rows = 0
    skuwise_items = []

    if not df_all.empty:
        # safe sku column
        if "sku" not in df_all.columns:
            df_all["sku"] = ""
        df_all["sku"] = df_all["sku"].fillna("").astype(str).str.strip()

        df_skus = df_all[df_all["sku"] != ""].copy()

        # ------------------------------------------------------------
        # QUANTITY / RETURN QUANTITY FIX
        # ------------------------------------------------------------

        for col, default in [
            ("description", ""),
            ("type", ""),
            ("transaction_type", ""),
            ("quantity", 0.0),
        ]:
            if col not in df_skus.columns:
                df_skus[col] = default

        df_skus["quantity"] = pd.to_numeric(
            df_skus["quantity"], errors="coerce"
        ).fillna(0.0)

        desc_lower = df_skus["description"].fillna("").astype(str).str.lower()
        type_lower = df_skus["type"].fillna("").astype(str).str.lower()
        transaction_type_lower = df_skus["transaction_type"].fillna("").astype(str).str.lower()

        return_mask = (
            desc_lower.str.contains("refund|return", case=False, na=False, regex=True)
            | type_lower.str.contains("refund|return", case=False, na=False, regex=True)
            | transaction_type_lower.str.contains("refund|return", case=False, na=False, regex=True)
        )

        # Sales quantity only from non-return rows
        df_skus["sales_quantity"] = 0.0
        df_skus.loc[~return_mask, "sales_quantity"] = (
            df_skus.loc[~return_mask, "quantity"].abs()
        )

        # Return quantity only from refund/return rows
        df_skus["return_quantity"] = 0.0
        df_skus.loc[return_mask, "return_quantity"] = (
            df_skus.loc[return_mask, "quantity"].abs()
        )

        preferred_sum_cols = [
            "sales_quantity", "return_quantity","quantity", "product_sales", "product_sales_tax", "postage_credits", "gift_wrap_credits",
            "shipping_credits_tax", "giftwrap_credits_tax", "promotional_rebates", "promotional_rebates_tax",
            "marketplace_facilitator_tax", "selling_fees", "fba_fees", "other", "gross_sales", "cogs", "profit",
        ]
        sum_cols = [c for c in preferred_sum_cols if c in df_skus.columns]

        for c in sum_cols:
            df_skus[c] = pd.to_numeric(df_skus[c], errors="coerce").fillna(0.0)

        df_sku = df_skus.groupby("sku", as_index=False)[sum_cols].sum()
        
        # ------------------------------------------------------------
        # FINAL REQUIRED QUANTITY COLUMNS
        # ------------------------------------------------------------

        if "sales_quantity" not in df_sku.columns:
            df_sku["sales_quantity"] = 0.0

        if "return_quantity" not in df_sku.columns:
            df_sku["return_quantity"] = 0.0

        df_sku["sales_quantity"] = pd.to_numeric(
            df_sku["sales_quantity"], errors="coerce"
        ).fillna(0.0)

        df_sku["return_quantity"] = pd.to_numeric(
            df_sku["return_quantity"], errors="coerce"
        ).fillna(0.0)

        # Final required quantity columns
        # quantity = sold units before deducting returns
        df_sku["quantity"] = (
            pd.to_numeric(df_sku["sales_quantity"], errors="coerce").fillna(0.0)
            + pd.to_numeric(df_sku["return_quantity"], errors="coerce").fillna(0.0)
        )

        # total_quantity = quantity - return_quantity
        df_sku["total_quantity"] = (
            pd.to_numeric(df_sku["quantity"], errors="coerce").fillna(0.0)
            - pd.to_numeric(df_sku["return_quantity"], errors="coerce").fillna(0.0)
        )

        df_sku["total_quantity"] = df_sku["total_quantity"].clip(lower=0)

        df_sku["shipment_fees"] = 0.0

        # merge lost_total
        if lost_total_df is not None and not lost_total_df.empty:
            df_sku = df_sku.merge(lost_total_df, on="sku", how="left")

        if "lost_total" not in df_sku.columns:
            df_sku["lost_total"] = 0.0
        df_sku["lost_total"] = pd.to_numeric(df_sku["lost_total"], errors="coerce").fillna(0.0)

        # merge misc_transaction
        if misc_transaction_df is not None and not misc_transaction_df.empty:
            df_sku = df_sku.merge(misc_transaction_df, on="sku", how="left")

        if "misc_transaction" not in df_sku.columns:
            df_sku["misc_transaction"] = 0.0

        df_sku["misc_transaction"] = pd.to_numeric(
            df_sku["misc_transaction"],
            errors="coerce"
        ).fillna(0.0)

        # finance derived
        if "product_sales" not in df_sku.columns:
            df_sku["product_sales"] = 0.0
        if "promotional_rebates" not in df_sku.columns:
            df_sku["promotional_rebates"] = 0.0

        df_sku["net_sales"] = (
            pd.to_numeric(df_sku["product_sales"], errors="coerce").fillna(0.0)
            + pd.to_numeric(df_sku["promotional_rebates"], errors="coerce").fillna(0.0)
        )

        if "quantity" not in df_sku.columns:
            df_sku["quantity"] = 0.0
        df_sku["quantity"] = pd.to_numeric(df_sku["quantity"], errors="coerce").fillna(0.0)

        df_sku["asp"] = df_sku.apply(
            lambda r: (float(r["net_sales"]) / float(r["total_quantity"])) if float(r["total_quantity"]) else 0.0,
            axis=1,
        )

        def _col(df: pd.DataFrame, name: str) -> pd.Series:
            return pd.to_numeric(df[name], errors="coerce").fillna(0.0) if name in df.columns else pd.Series([0.0] * len(df), index=df.index)

        df_sku["credits"] = (_col(df_sku, "postage_credits") + _col(df_sku, "gift_wrap_credits")).fillna(0.0)
        df_sku["tax"] = (
            _col(df_sku, "product_sales_tax")
            + _col(df_sku, "shipping_credits_tax")
            + _col(df_sku, "giftwrap_credits_tax")
            + _col(df_sku, "promotional_rebates_tax")
            + _col(df_sku, "marketplace_facilitator_tax")
        ).fillna(0.0)
        df_sku["tax_and_credits"] = (df_sku["credits"] - df_sku["tax"].abs()).round(2)
        # ---------------- CM1 PROFIT FIX ----------------
        # Marketplace Fees = Selling Fees + FBA Fees
        df_sku["marketplace_fees"] = (
            pd.to_numeric(df_sku["selling_fees"], errors="coerce").fillna(0.0).abs()
            + pd.to_numeric(df_sku["fba_fees"], errors="coerce").fillna(0.0).abs()
        )

        # Other Transactions
        if "other" not in df_sku.columns:
            df_sku["other"] = 0.0

        df_sku["other"] = pd.to_numeric(df_sku["other"], errors="coerce").fillna(0.0)

        # CM1 Profit = Net Sales - COGS - Marketplace Fees + Other Transactions
        df_sku["profit"] = (
            pd.to_numeric(df_sku["net_sales"], errors="coerce").fillna(0.0)
            - pd.to_numeric(df_sku["cogs"], errors="coerce").fillna(0.0)
            - df_sku["marketplace_fees"]
            + df_sku["tax_and_credits"].fillna(0.0)
        ).round(2)

        # -------- ADS merge --------
        ads_total_product_spend = 0.0
        ads_total_display_spend = 0.0
        ads_total_brand_spend = 0.0
        ads_agg = pd.DataFrame()

        try:
            sql = f'''
                SELECT
                    products,
                    ad_type,
                    impressions,
                    clicks,
                    spend,
                    sale_units,
                    sale_amount,
                    sp_ads_sales,
                    sd_ads_sales,
                    sb_ads_sales,
                    product_spend,
                    display_spend,
                    brand_spend
                FROM public."{ads_table_name}"
            '''
            ads_df = pd.read_sql_query(sql, PHORMULA_ENGINE)

            if "products" not in ads_df.columns:
                ads_df["products"] = ""
            ads_df["products"] = ads_df["products"].fillna("").astype(str).str.strip()

            gt_mask = ads_df["products"].str.lower().eq("grand total")
            if gt_mask.any():
                ads_total_product_spend = float(pd.to_numeric(ads_df.loc[gt_mask, "product_spend"], errors="coerce").fillna(0.0).sum()) if "product_spend" in ads_df.columns else 0.0
                ads_total_display_spend = float(pd.to_numeric(ads_df.loc[gt_mask, "display_spend"], errors="coerce").fillna(0.0).sum()) if "display_spend" in ads_df.columns else 0.0
                ads_total_brand_spend = float(pd.to_numeric(ads_df.loc[gt_mask, "brand_spend"], errors="coerce").fillna(0.0).sum()) if "brand_spend" in ads_df.columns else 0.0
            else:
                # safe sums even if columns missing
                for c in ["product_spend", "display_spend", "brand_spend"]:
                    if c not in ads_df.columns:
                        ads_df[c] = 0.0
                ads_total_product_spend = float(pd.to_numeric(ads_df["product_spend"], errors="coerce").fillna(0.0).sum())
                ads_total_display_spend = float(pd.to_numeric(ads_df["display_spend"], errors="coerce").fillna(0.0).sum())
                ads_total_brand_spend = float(pd.to_numeric(ads_df["brand_spend"], errors="coerce").fillna(0.0).sum())

            ads_df = ads_df[ads_df["products"] != ""].copy()

            # safe ad_type
            if "ad_type" not in ads_df.columns:
                ads_df["ad_type"] = ""
            ads_df["ad_type"] = ads_df["ad_type"].fillna("").astype(str).str.strip()

            for col in ["impressions", "clicks", "spend", "sale_units", "sale_amount", "sp_ads_sales", "sd_ads_sales", "sb_ads_sales", "product_spend", "display_spend", "brand_spend"]:
                if col not in ads_df.columns:
                    ads_df[col] = 0.0
                ads_df[col] = pd.to_numeric(ads_df[col], errors="coerce").fillna(0.0)

            ads_num = ads_df.groupby("products", as_index=False)[
                ["impressions", "clicks", "spend", "sale_units", "sale_amount","sp_ads_sales" ,"sd_ads_sales", "sb_ads_sales", "product_spend", "display_spend", "brand_spend"]
            ].sum()

            ads_type = (
                ads_df[ads_df["ad_type"] != ""]
                .groupby("products")["ad_type"]
                .apply(lambda s: ", ".join(sorted(set([str(x).strip() for x in s if str(x).strip()]))))
                .reset_index()
            )

            ads_agg = ads_num.merge(ads_type, on="products", how="left")
            if "ad_type" not in ads_agg.columns:
                ads_agg["ad_type"] = ""
            ads_agg["ad_type"] = ads_agg["ad_type"].fillna("")

            ads_agg.rename(
                columns={
                    "impressions": "ads_impressions",
                    "clicks": "ads_clicks",
                    "sale_units": "ads_sale_units",
                    "sale_amount": "ads_sale_amount",
                    "sp_ads_sales": "sp_ads_sales",
                    "sd_ads_sales": "sd_ads_sales",
                    "sb_ads_sales": "sb_ads_sales",
                },
                inplace=True,
            )
            if "spend" in ads_agg.columns:
                ads_agg.rename(columns={"spend": "ads_spend_raw"}, inplace=True)

        except Exception as e:
            ads_agg = pd.DataFrame()

        if not ads_agg.empty:
            df_sku = (
                df_sku.merge(ads_agg, how="left", left_on="sku", right_on="products")
                .drop(columns=["products"], errors="ignore")
            )

        # ensure ad_type exists
        if "ad_type" not in df_sku.columns:
            df_sku["ad_type"] = ""
        else:
            df_sku["ad_type"] = df_sku["ad_type"].fillna("").astype(str)

        # ensure spend cols exist
        for col in ["product_spend", "display_spend", "brand_spend"]:
            if col not in df_sku.columns:
                df_sku[col] = 0.0
            df_sku[col] = pd.to_numeric(df_sku[col], errors="coerce").fillna(0.0)

        # ✅ ads_spend = product + display
        df_sku["ads_spend"] = (df_sku["product_spend"] + df_sku["display_spend"]).fillna(0.0)
        # ✅ ACOS column
        df_sku["acos"] = df_sku.apply(
            lambda r: (float(r["ads_spend"]) / float(r["net_sales"]) * 100.0)
            if float(r["net_sales"]) else 0.0,
            axis=1,
        )

        # ensure ads metrics exist
        for col in ["ads_impressions", "ads_clicks", "ads_sale_units", "ads_sale_amount","sp_ads_sales","sd_ads_sales","sb_ads_sales",]:
            if col not in df_sku.columns:
                df_sku[col] = 0.0
            df_sku[col] = pd.to_numeric(df_sku[col], errors="coerce").fillna(0.0)

        df_sku["ads_sale_amount"] = (
            df_sku["sp_ads_sales"]
            + df_sku["sd_ads_sales"]
            + df_sku["sb_ads_sales"]
        )

        # total-only breakup columns
        # total-only breakup columns
        df_sku["platform_fee_inventory_storage"] = 0.0
        df_sku["platformfeenew"] = 0.0
        df_sku["dealsvouchar_ads"] = 0.0

        # platform_fee per SKU row
        df_sku["platform_fee"] = (
            pd.to_numeric(df_sku["platform_fee_inventory_storage"], errors="coerce").fillna(0.0).abs()
            + pd.to_numeric(df_sku["platformfeenew"], errors="coerce").fillna(0.0).abs()
            - pd.to_numeric(df_sku["lost_total"], errors="coerce").fillna(0.0).abs()
            - pd.to_numeric(df_sku["misc_transaction"], errors="coerce").fillna(0.0).abs()
        )

        # profit and cm2
        if "profit" not in df_sku.columns:
            df_sku["profit"] = 0.0
        df_sku["profit"] = pd.to_numeric(df_sku["profit"], errors="coerce").fillna(0.0)

        df_sku["cm2_profit"] = (df_sku["profit"] - df_sku["ads_spend"]).fillna(0.0)

        df_sku["cm1_profit_per_unit"] = df_sku.apply(
            lambda r: (float(r["profit"]) / float(r["total_quantity"])) if float(r["total_quantity"]) else 0.0,
            axis=1,
        )
        df_sku["cm1_profit_per"] = df_sku.apply(
            lambda r: (float(r["profit"]) / float(r["net_sales"]) * 100.0) if float(r["net_sales"]) else 0.0,
            axis=1,
        )
        df_sku["cm2_profit_per_unit"] = df_sku.apply(
            lambda r: (float(r["cm2_profit"]) / float(r["total_quantity"])) if float(r["total_quantity"]) else 0.0,
            axis=1,
        )
        df_sku["cm2_profit_per"] = df_sku.apply(
            lambda r: (float(r["cm2_profit"]) / float(r["net_sales"]) * 100.0) if float(r["net_sales"]) else 0.0,
            axis=1,
        )

        for col in ["cm2_profit", "cm1_profit_per_unit", "cm1_profit_per", "cm2_profit_per_unit", "cm2_profit_per"]:
            if col not in df_sku.columns:
                df_sku[col] = 0.0
            df_sku[col] = pd.to_numeric(df_sku[col], errors="coerce").fillna(0.0)

        # product_name mapping
        df_sku["product_name"] = ""
        sku_data_table = _build_sku_data_table_name(user_id)
        sku_col = _country_to_sku_col(ui_country)

        try:
            map_sql = f'SELECT product_name, "{sku_col}" AS sku_key FROM public."{sku_data_table}"'
            map_df = pd.read_sql_query(map_sql, PHORMULA_ENGINE)

            if not map_df.empty:
                map_df["sku_key"] = map_df["sku_key"].fillna("").astype(str).str.strip()
                map_df["product_name"] = map_df["product_name"].fillna("").astype(str).str.strip()
                map_df = (
                    map_df[map_df["sku_key"] != ""]
                    .sort_values(by=["sku_key"])
                    .drop_duplicates(subset=["sku_key"], keep="first")
                )

                df_sku = df_sku.merge(map_df, how="left", left_on="sku", right_on="sku_key").drop(columns=["sku_key"], errors="ignore")

                if "product_name_y" in df_sku.columns and "product_name_x" in df_sku.columns:
                    df_sku["product_name"] = df_sku["product_name_y"].fillna(df_sku["product_name_x"]).fillna("")
                    df_sku.drop(columns=["product_name_x", "product_name_y"], inplace=True, errors="ignore")
                elif "product_name_y" in df_sku.columns:
                    df_sku.rename(columns={"product_name_y": "product_name"}, inplace=True)

                if "product_name" not in df_sku.columns:
                    df_sku["product_name"] = ""
                df_sku["product_name"] = df_sku["product_name"].fillna("").astype(str)

        except Exception as e:
            if "product_name" not in df_sku.columns:
                df_sku["product_name"] = ""
            df_sku["product_name"] = df_sku["product_name"].fillna("").astype(str)

        # meta
        df_sku["user_id"] = int(user_id)
        df_sku["country"] = ui_country
        df_sku["month"] = _safe_ident(month_name)
        df_sku["year"] = int(now_utc.year)
        df_sku["generated_at_utc"] = now_utc.isoformat()

        # -------- GRAND TOTAL row --------
        total_row = {"sku": "GRAND_TOTAL", "product_name": "Grand Total"}

        for c in sum_cols:
            total_row[c] = float(df_sku[c].sum()) if c in df_sku.columns else 0.0

        total_row["lost_total"] = (
            float(pd.to_numeric(df_sku["lost_total"], errors="coerce").fillna(0.0).abs().sum())
            if "lost_total" in df_sku.columns else 0.0
        )
        total_row["misc_transaction"] = round(float(misc_transaction_total or 0.0), 2)

        total_row["net_sales"] = float(df_sku["net_sales"].sum()) if "net_sales" in df_sku.columns else 0.0

        total_quantity = float(df_sku["quantity"].sum()) if "quantity" in df_sku.columns else 0.0
        total_return_quantity = float(df_sku["return_quantity"].sum()) if "return_quantity" in df_sku.columns else 0.0
        total_total_quantity = float(df_sku["total_quantity"].sum()) if "total_quantity" in df_sku.columns else 0.0

        total_row["quantity"] = total_quantity
        total_row["return_quantity"] = total_return_quantity
        total_row["total_quantity"] = total_total_quantity

        # Use total_quantity after returns for ASP and per-unit calculations
        total_qty = total_total_quantity

        total_row["asp"] = (
            total_row["net_sales"] / total_qty
            if total_qty
            else 0.0
        )
        # Grand Total Marketplace Fees
        total_row["marketplace_fees"] = round(
            float(pd.to_numeric(df_sku["marketplace_fees"], errors="coerce").fillna(0.0).sum())
            if "marketplace_fees" in df_sku.columns else 0.0,
            2,
        )

        # Grand Total Tax and Credits / Other Transactions
        total_row["credits"] = round(
            float(pd.to_numeric(df_sku["credits"], errors="coerce").fillna(0.0).sum())
            if "credits" in df_sku.columns else 0.0,
            2,
        )

        total_row["tax"] = round(
            float(pd.to_numeric(df_sku["tax"], errors="coerce").fillna(0.0).sum())
            if "tax" in df_sku.columns else 0.0,
            2,
        )

        total_row["tax_and_credits"] = round(
            float(total_row["credits"]) - abs(float(total_row["tax"])),
            2,
        )

        # If frontend displays "Other Transactions" from `other`,
        # keep it same as tax_and_credits.
        total_row["other"] = abs(float(total_row.get("tax_and_credits", 0.0) or 0.0))

        # Grand Total CM1 Profit
        # CM1 = Net Sales - COGS - Marketplace Fees + Tax and Credits
        total_row["profit"] = round(
            float(total_row.get("net_sales", 0.0) or 0.0)
            - float(total_row.get("cogs", 0.0) or 0.0)
            - float(total_row.get("marketplace_fees", 0.0) or 0.0)
            + float(total_row.get("other", 0.0) or 0.0),
            2,
        )

        total_row["ads_impressions"] = float(df_sku["ads_impressions"].sum()) if "ads_impressions" in df_sku.columns else 0.0
        total_row["ads_clicks"] = float(df_sku["ads_clicks"].sum()) if "ads_clicks" in df_sku.columns else 0.0
        total_row["ads_sale_units"] = float(df_sku["ads_sale_units"].sum()) if "ads_sale_units" in df_sku.columns else 0.0
        total_row["sp_ads_sales"] = float(df_sku["sp_ads_sales"].sum()) if "sp_ads_sales" in df_sku.columns else 0.0
        total_row["sd_ads_sales"] = float(df_sku["sd_ads_sales"].sum()) if "sd_ads_sales" in df_sku.columns else 0.0
        total_row["sb_ads_sales"] = float(df_sku["sb_ads_sales"].sum()) if "sb_ads_sales" in df_sku.columns else 0.0

        total_row["ads_sale_amount"] = (
            total_row["sp_ads_sales"]
            + total_row["sd_ads_sales"]
            + total_row["sb_ads_sales"]
        )

        total_row["product_spend"] = round(float(ads_total_product_spend or 0.0), 2)
        total_row["display_spend"] = round(float(ads_total_display_spend or 0.0), 2)
        total_row["brand_spend"] = round(float(ads_total_brand_spend or 0.0), 2)
        total_row["ads_spend"] = round(total_row["product_spend"] + total_row["display_spend"], 2)
        # ✅ ACOS for total
        g_spend = float(total_row["ads_spend"])
        g_net_sales = float(total_row["net_sales"])

        total_row["acos"] = (g_spend / g_net_sales * 100.0) if g_net_sales else 0.0

        # store totals
        total_row["platform_fee_inventory_storage"] = round(float(platform_fee_inventory_storage_total or 0.0), 2)
        total_row["shipment_fees"] = round(float(shipment_fees or 0.0), 2)
        total_row["platformfeenew"] = round(float(platformfeenew_total or 0.0), 2)
        total_row["dealsvouchar_ads"] = round(float(dealsvouchar_ads_total or 0.0), 2)

        # Other Transactions:
        # Misc Transactions (+) + Reimbursement for lost Inventory (+)
        # - Inventory Storage Fees (-) - Other Platform Fees (-)
        other_transactions_value = (
            abs(float(total_row.get("misc_transaction", 0.0) or 0.0))
            + abs(float(total_row.get("lost_total", 0.0) or 0.0))
            - abs(float(total_row.get("platform_fee_inventory_storage", 0.0) or 0.0))
            - abs(float(total_row.get("platformfeenew", 0.0) or 0.0))
        )

        # Keep platform_fee negative because later total_cm2_profit subtracts abs(platform_fee)
        total_row["platform_fee"] = round(-other_transactions_value, 2)

        total_row["ad_type"] = "All"

        g_clicks = float(total_row["ads_clicks"])
        g_spend = float(total_row["ads_spend"])
        g_units = float(total_row["ads_sale_units"])
        g_sales = float(total_row["ads_sale_amount"])

        total_row["ads_conversion_rate"] = (g_units / g_clicks * 100.0) if g_clicks else 0.0
        total_row["ads_roas"] = (g_sales / g_spend) if g_spend else 0.0
        total_row["ads_acos"] = (g_spend / g_sales * 100.0) if g_sales else 0.0

        g_profit = float(total_row.get("profit", 0.0))
        g_net_sales = float(total_row.get("net_sales", 0.0))

        total_row["cm2_profit"] = g_profit - g_spend
        total_row["cm1_profit_per_unit"] = (g_profit / total_qty) if total_qty else 0.0
        total_row["cm1_profit_per"] = (g_profit / g_net_sales * 100.0) if g_net_sales else 0.0
        total_row["cm2_profit_per_unit"] = (total_row["cm2_profit"] / total_qty) if total_qty else 0.0
        total_row["cm2_profit_per"] = (total_row["cm2_profit"] / g_net_sales * 100.0) if g_net_sales else 0.0

        # total_row["credits"] = float(df_sku["credits"].sum()) if "credits" in df_sku.columns else 0.0
        # total_row["tax"] = float(df_sku["tax"].sum()) if "tax" in df_sku.columns else 0.0
        # total_row["tax_and_credits"] = round(float(total_row["credits"]) - abs(float(total_row["tax"])), 2)
        # ================= DASHBOARD SUMMARY VALUES =================

        cost_ads_total = (
            abs(float(total_row.get("brand_spend", 0.0) or 0.0))
            + abs(float(total_row.get("dealsvouchar_ads", 0.0) or 0.0))
        )

        product_ads_total = (
            abs(float(total_row.get("product_spend", 0.0) or 0.0))
            + abs(float(total_row.get("display_spend", 0.0) or 0.0))
        )

        # Productwise CM2 Profit
        # CM2 Profit = CM1 Profit - Ads Spend
        cm2_profit_productwise = (
            float(total_row.get("profit", 0.0) or 0.0)
            - float(total_row.get("ads_spend", 0.0) or 0.0)
        )

        total_row["cm2_profit"] = round(cm2_profit_productwise, 2)

        # Productwise CM2 Profit
        # CM2 Profit = CM1 Profit - Ads Spend
        cm2_profit_productwise = (
            float(total_row.get("profit", 0.0) or 0.0)
            - float(total_row.get("ads_spend", 0.0) or 0.0)
        )

        total_row["cm2_profit"] = round(cm2_profit_productwise, 2)

        # Other Transactions:
        # Misc Transactions (+) + Reimbursement for lost Inventory (+)
        # - Inventory Storage Fees (-) - Other Platform Fees (-)
        other_transactions_total = (
            abs(float(total_row.get("misc_transaction", 0.0) or 0.0))
            + abs(float(total_row.get("lost_total", 0.0) or 0.0))
            - abs(float(total_row.get("platform_fee_inventory_storage", 0.0) or 0.0))
            - abs(float(total_row.get("platformfeenew", 0.0) or 0.0))
        )

        # keep UI platform_fee negative, same as global
        total_row["platform_fee"] = round(-other_transactions_total, 2)

        shipment_charges_total = abs(float(total_row.get("shipment_fees", 0.0) or 0.0))

        # Total CM2 Profit:
        # total_cm2_profit = cm2_profit - brand_spend - dealsvouchar_ads - abs(platform_fee) - shipment_fees
        total_cm2_profit = (
            cm2_profit_productwise
            - abs(float(total_row.get("brand_spend", 0.0) or 0.0))
            - abs(float(total_row.get("dealsvouchar_ads", 0.0) or 0.0))
            - abs(float(total_row.get("platform_fee", 0.0) or 0.0))
            - shipment_charges_total
        )

        total_cm2_margins = (
            total_cm2_profit / float(total_row.get("net_sales", 0.0) or 0.0) * 100.0
        ) if float(total_row.get("net_sales", 0.0) or 0.0) else 0.0

        tacos = (
            (product_ads_total + cost_ads_total) / float(total_row.get("net_sales", 0.0) or 0.0) * 100.0
        ) if float(total_row.get("net_sales", 0.0) or 0.0) else 0.0

        reimbursement_vs_cm2_margins = (
            float(current_net_reimbursement or 0.0) / total_cm2_profit * 100.0
        ) if total_cm2_profit else 0.0

        reimbursement_vs_sales = (
            float(current_net_reimbursement or 0.0) / float(total_row.get("net_sales", 0.0) or 0.0) * 100.0
        ) if float(total_row.get("net_sales", 0.0) or 0.0) else 0.0

        total_row["total_ads"] = round(product_ads_total + cost_ads_total, 2)
        total_row["debt_payment"] = round(float(debt_payment_total or 0.0), 2)
        total_row["disbursement"] = round(float(disbursement_total or 0.0), 2)
        total_row["total_cm2_profit"] = round(total_cm2_profit, 2)
        total_row["total_cm2_margins"] = round(total_cm2_margins, 2)
        total_row["tacos_total_advertising_cost_of_sale"] = round(tacos, 2)
        total_row["reimbursement_vs_cm2_margins"] = round(reimbursement_vs_cm2_margins, 2)
        total_row["reimbursement_vs_sales"] = round(reimbursement_vs_sales, 2)

        total_row["user_id"] = int(user_id)
        total_row["country"] = ui_country
        total_row["month"] = _safe_ident(month_name)
        total_row["year"] = int(now_utc.year)
        total_row["generated_at_utc"] = now_utc.isoformat()

        # ✅ Put all derived_totals values into SKU-wise table Grand Total row
        DERIVED_TOTAL_COLUMNS = [
            "amazon_fees",
            "platform_fee",
            "advertising_fees",
            "shipment_fees",
            "net_sales",
            "gross_sales",
            "asp",
            "profit",
            "cm2_profit",
            "profit_percentage",
            "current_net_reimbursement",
            "debt_payment",
            "disbursement",
            # new columns
            "total_ads",
            "total_cm2_profit",
            "total_cm2_margins",
            "tacos_total_advertising_cost_of_sale",
            "reimbursement_vs_cm2_margins",
            "reimbursement_vs_sales",
        ]

        for col in DERIVED_TOTAL_COLUMNS:
            if col not in df_sku.columns:
                df_sku[col] = 0.0

        for col, val in derived_totals.items():
            if col not in [
                "asp",          # do not overwrite total row ASP
                "platform_fee",
                "profit",
                "cm2_profit",
                "total_ads",
                "total_cm2_profit",
                "total_cm2_margins",
                "tacos_total_advertising_cost_of_sale",
                "reimbursement_vs_cm2_margins",
                "reimbursement_vs_sales",
            ]:
                total_row[col] = val

        df_sku = pd.concat([df_sku, pd.DataFrame([total_row])], ignore_index=True)

        # Remove helper column from final DB table
        df_sku.drop(columns=["sales_quantity"], inplace=True, errors="ignore")

        # Optional: make grand total row same as your old table
        df_sku.loc[df_sku["sku"].astype(str).str.upper() == "GRAND_TOTAL", "sku"] = "TOTAL"
        df_sku.loc[df_sku["product_name"].astype(str).str.lower() == "grand total", "product_name"] = "TOTAL"

        # ------------------------------------------------------------
        # FINAL COLUMN ORDER FIX
        # Keep quantity, return_quantity, total_quantity together
        # ------------------------------------------------------------
        preferred_first_cols = [
            "sku",
            "product_name",
            "quantity",
            "return_quantity",
            "total_quantity",
            "asp",
            "net_sales",
            "product_sales",
            "product_sales_tax",
            "postage_credits",
            "gift_wrap_credits",
            "shipping_credits_tax",
            "giftwrap_credits_tax",
            "promotional_rebates",
            "promotional_rebates_tax",
            "marketplace_facilitator_tax",
            "cogs",
            "selling_fees",
            "fba_fees",
            "marketplace_fees",
            "credits",
            "tax",
            "tax_and_credits",
            "other",
            "gross_sales",
            "profit",
            "ads_spend",
            "acos",
            "cm2_profit",
        ]

        existing_first_cols = [c for c in preferred_first_cols if c in df_sku.columns]
        remaining_cols = [c for c in df_sku.columns if c not in existing_first_cols]

        df_sku = df_sku[existing_first_cols + remaining_cols]

        skuwise_items = df_sku.to_dict(orient="records")

        # store SKU-wise table
        try:
            df_sku.to_sql(
                skuwise_table_name,
                PHORMULA_ENGINE,
                schema="public",
                if_exists="replace",
                index=False,
                method="multi",
                chunksize=1000,
            )
            sku_summary_saved = True
            sku_summary_rows = int(len(df_sku))
        except Exception as e:
            sku_summary_saved = False

    # ---------------- Excel response ----------------
    if response_format == "excel":
        df = pd.DataFrame(all_rows) if all_rows else pd.DataFrame()
        df = df.reindex(
            columns=MTD_COLUMNS + ["cogs", "profit", "gross_sales", "misc_transaction"],
            fill_value=0.0
        )

        output = io.BytesIO()
        with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
            df.to_excel(writer, index=False, sheet_name="Transactions")
            pd.DataFrame([totals]).to_excel(writer, index=False, sheet_name="Totals")
            pd.DataFrame([derived_totals]).to_excel(writer, index=False, sheet_name="DerivedTotals")
            pd.DataFrame([previous_period]).to_excel(writer, index=False, sheet_name="PrevPeriodMeta")
            if db_result:
                pd.DataFrame([db_result]).to_excel(writer, index=False, sheet_name="DBMeta")
            if skuwise_items:
                pd.DataFrame(skuwise_items).to_excel(writer, index=False, sheet_name="SKUWiseMonthly")

        output.seek(0)
        filename = f"finances_transactions_MTD_{now_utc.year}_{now_utc.month:02d}.xlsx"
        return send_file(
            output,
            as_attachment=True,
            download_name=filename,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

    # ---------------- JSON response ----------------
    payload_out = {
        "success": True,
        "posted_after": posted_after,
        "posted_before": posted_before,
        "count": len(all_rows),
        "stored": bool(store_in_db),
        "db_result": db_result,
        "cogs_meta": {
            "country": ui_country,
            "month": month_name,
            "year": now_utc.year,
            "pair": f"{user_currency}->{selected_currency}",
            "conversion_rate": conversion_rate_fx,
        },
        "totals": totals,
        "derived_totals": derived_totals,
        "previous_period": previous_period,
        "skuwise_table": {
            "name": skuwise_table_name,
            "saved": sku_summary_saved,
            "rows": sku_summary_rows,
        },
        "skuwise_items": skuwise_items,
        "transactions": all_rows,
    }

    return jsonify(_json_safe(payload_out)), 200



def _safe_ident(value: str) -> str:
    value = (value or "").lower().strip()
    value = re.sub(r"[^a-z0-9_]+", "_", value)
    return value or "uk"


@amazon_api_bp.route("/amazon_api/live-dashboard/save", methods=["POST", "GET"])
def save_live_dashboard_data():
    auth_header = request.headers.get("Authorization")
    if not auth_header or not auth_header.startswith("Bearer "):
        return jsonify({"success": False, "error": "Missing token"}), 401

    token = auth_header.split(" ")[1]
    try:
        payload, user_id, member_id = get_effective_user_id_from_token(token)
        user_id = int(payload["user_id"])
    except Exception:
        return jsonify({"success": False, "error": "Invalid token"}), 401

    table_name = "live_data"

    create_sql = f"""
    CREATE TABLE IF NOT EXISTS public.{table_name} (
        id SERIAL PRIMARY KEY,
        user_id INT NOT NULL,
        country TEXT NOT NULL,
        platform TEXT,
        region TEXT,
        start_day INT NULL,
        end_day INT NULL,
        cache_key TEXT NOT NULL UNIQUE,
        saved_at BIGINT,
        payload JSONB NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """

    if request.method == "POST":
        body = request.get_json(silent=True) or {}

        country = _safe_ident(body.get("country") or "uk")
        platform = (body.get("platform") or "").strip().lower()
        region = (body.get("region") or "").strip()
        start_day = body.get("startDay")
        end_day = body.get("endDay")
        cache_payload = body.get("cachePayload") or {}
        saved_at = body.get("savedAt") or int(time.time() * 1000)

        cache_key = f"{user_id}:{country}:{platform}:{region}:{start_day if start_day is not None else 'na'}:{end_day if end_day is not None else 'na'}"

        try:
            upsert_sql = f"""
            INSERT INTO public.{table_name}
            (user_id, country, platform, region, start_day, end_day, cache_key, saved_at, payload)
            VALUES
            (:user_id, :country, :platform, :region, :start_day, :end_day, :cache_key, :saved_at, :payload)
            ON CONFLICT (cache_key)
            DO UPDATE SET
                payload = EXCLUDED.payload,
                saved_at = EXCLUDED.saved_at,
                updated_at = CURRENT_TIMESTAMP
            RETURNING id, user_id, country, platform, region, start_day, end_day,
                      cache_key, saved_at, created_at, updated_at
            """

            with PHORMULA_ENGINE.begin() as conn:
                conn.execute(text(create_sql))
                row = conn.execute(
                    text(upsert_sql),
                    {
                        "user_id": user_id,
                        "country": country,
                        "platform": platform,
                        "region": region,
                        "start_day": start_day,
                        "end_day": end_day,
                        "cache_key": cache_key,
                        "saved_at": saved_at,
                        "payload": json.dumps(cache_payload),
                    },
                ).mappings().first()

            return jsonify({
                "success": True,
                "table": table_name,
                "cache_key": cache_key,
                "data": {
                    "id": row["id"] if row else None,
                    "user_id": row["user_id"] if row else user_id,
                    "country": row["country"] if row else country,
                    "platform": row["platform"] if row else platform,
                    "region": row["region"] if row else region,
                    "start_day": row["start_day"] if row else start_day,
                    "end_day": row["end_day"] if row else end_day,
                    "saved_at": row["saved_at"] if row else saved_at,
                    "created_at": row["created_at"].isoformat() if row and row.get("created_at") else None,
                    "updated_at": row["updated_at"].isoformat() if row and row.get("updated_at") else None,
                },
                "message": "Live dashboard data stored successfully"
            }), 200

        except Exception as e:
            return jsonify({"success": False, "error": str(e)}), 500

    country = _safe_ident(request.args.get("country") or "uk")
    platform = (request.args.get("platform") or "").strip().lower()
    region = (request.args.get("region") or "").strip()
    start_day = request.args.get("start_day")
    end_day = request.args.get("end_day")

    start_day = int(start_day) if start_day not in (None, "", "null", "undefined") else None
    end_day = int(end_day) if end_day not in (None, "", "null", "undefined") else None

    cache_key = f"{user_id}:{country}:{platform}:{region}:{start_day if start_day is not None else 'na'}:{end_day if end_day is not None else 'na'}"

    try:
        select_sql = f"""
        SELECT id, user_id, country, platform, region, start_day, end_day,
               cache_key, saved_at, payload, created_at, updated_at
        FROM public.{table_name}
        WHERE cache_key = :cache_key
        LIMIT 1
        """

        with PHORMULA_ENGINE.begin() as conn:
            conn.execute(text(create_sql))
            row = conn.execute(
                text(select_sql),
                {"cache_key": cache_key}
            ).mappings().first()

        if not row:
            return jsonify({
                "success": True,
                "found": False,
                "table": table_name,
                "cache_key": cache_key,
                "data": None
            }), 200

        return jsonify({
            "success": True,
            "found": True,
            "table": table_name,
            "cache_key": cache_key,
            "data": {
                "id": row["id"],
                "user_id": row["user_id"],
                "country": row["country"],
                "platform": row["platform"],
                "region": row["region"],
                "start_day": row["start_day"],
                "end_day": row["end_day"],
                "cache_key": row["cache_key"],
                "saved_at": row["saved_at"],
                "payload": row["payload"],
                "created_at": row["created_at"].isoformat() if row.get("created_at") else None,
                "updated_at": row["updated_at"].isoformat() if row.get("updated_at") else None,
            }
        }), 200

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

    
       