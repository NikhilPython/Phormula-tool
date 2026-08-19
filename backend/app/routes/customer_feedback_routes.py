from __future__ import annotations

import re
from typing import Any, Dict, Tuple

import jwt
from flask import Blueprint, jsonify, request
from sqlalchemy import inspect, text

from app.models.user_models import Product, amazon_user
from app.utils.amazon_utils import (
    PHORMULA_ENGINE,
    _apply_region_and_marketplace_from_request,
    amazon_client,
)
from app.utils.token_utils import get_effective_user_id_from_token


customer_feedback_bp = Blueprint("customer_feedback", __name__)

CUSTOMER_FEEDBACK_MARKETPLACES = {
    "ATVPDKIKX0DER": "US",
    "A1F83G8C2ARO7P": "UK",
}

ASIN_RE = re.compile(r"^[A-Z0-9]{10}$", re.IGNORECASE)
SKU_COLUMN_BY_MARKETPLACE = {
    "ATVPDKIKX0DER": "sku_us",
    "A1F83G8C2ARO7P": "sku_uk",
    "A2EUQ1WTGCTBG2": "sku_canada",
}


def _json_error(message: str, status_code: int, **extra):
    payload = {"success": False, "error": message}
    payload.update(extra)
    return jsonify(payload), status_code


def _get_authenticated_user_id() -> Tuple[int | None, Any | None]:
    auth_header = request.headers.get("Authorization")
    if not auth_header or not auth_header.startswith("Bearer "):
        return None, _json_error("Authorization token is missing or invalid", 401)

    token = auth_header.split(" ", 1)[1]

    try:
        payload, user_id, member_id = get_effective_user_id_from_token(token)
        effective_user_id = int(user_id or payload.get("user_id"))
        return effective_user_id, None
    except jwt.ExpiredSignatureError:
        return None, _json_error("Token has expired", 401)
    except jwt.InvalidTokenError:
        return None, _json_error("Invalid token", 401)
    except Exception:
        return None, _json_error("Invalid token payload", 401)


def _get_customer_feedback_connection(user_id: int):
    _apply_region_and_marketplace_from_request()

    marketplace_id = request.args.get("marketplace_id") or amazon_client.marketplace_id

    if marketplace_id not in amazon_client.ALLOWED_MARKETPLACES:
        return None, None, _json_error(
            "Unsupported marketplace",
            400,
            marketplace_id=marketplace_id,
        )

    if marketplace_id not in CUSTOMER_FEEDBACK_MARKETPLACES:
        return None, None, _json_error(
            "Customer Feedback API is not available for this marketplace in this app",
            400,
            marketplace_id=marketplace_id,
            supported_marketplace_ids=list(CUSTOMER_FEEDBACK_MARKETPLACES.keys()),
        )

    au = amazon_user.query.filter_by(
        user_id=user_id,
        marketplace_id=marketplace_id,
    ).first()

    if not au:
        return None, None, _json_error(
            "Amazon connection not found for this marketplace",
            404,
            marketplace_id=marketplace_id,
        )

    if not au.refresh_token:
        return None, None, _json_error(
            "No refresh token found for this marketplace. Complete OAuth first.",
            400,
            marketplace_id=marketplace_id,
        )

    amazon_client.refresh_token = au.refresh_token
    return marketplace_id, au, None


def _amazon_error_response(operation: str, response: Dict[str, Any]):
    upstream_status = int(response.get("status_code") or 502)
    status_code = upstream_status if 400 <= upstream_status < 600 else 502

    return jsonify({
        "success": False,
        "operation": operation,
        "error": "Amazon Customer Feedback API request failed",
        "details": response,
    }), status_code


def _call_customer_feedback(endpoint: str, params: Dict[str, Any]):
    response = amazon_client.make_api_call(endpoint, "GET", params=params)
    if not isinstance(response, dict):
        return {"payload": response}
    return response


def _extract_catalog_title(catalog_item):
    summaries = catalog_item.get("summaries") or []
    if summaries and isinstance(summaries[0], dict):
        return _clean_text(summaries[0].get("itemName"))

    attributes = catalog_item.get("attributes") or {}
    item_names = attributes.get("item_name") or []
    if item_names and isinstance(item_names[0], dict):
        return _clean_text(item_names[0].get("value"))

    return None


def _extract_catalog_image_url(catalog_item):
    summaries = catalog_item.get("summaries") or []
    if summaries and isinstance(summaries[0], dict):
        main_image = summaries[0].get("mainImage") or {}
        if isinstance(main_image, dict):
            image_url = _clean_text(main_image.get("link") or main_image.get("url"))
            if image_url:
                return image_url

    images = catalog_item.get("images") or []
    for image_group in images:
        if not isinstance(image_group, dict):
            continue

        group_images = image_group.get("images")
        if isinstance(group_images, list):
            main_candidates = [
                image for image in group_images
                if isinstance(image, dict) and str(image.get("variant") or "").upper() == "MAIN"
            ]
            for image in main_candidates + group_images:
                if isinstance(image, dict):
                    image_url = _clean_text(image.get("link") or image.get("url"))
                    if image_url:
                        return image_url

        image_url = _clean_text(image_group.get("link") or image_group.get("url"))
        if image_url:
            return image_url

    return None


def _fetch_catalog_product_meta(asin: str, marketplace_id: str):
    response = amazon_client.make_api_call(
        f"/catalog/2022-04-01/items/{asin}",
        "GET",
        params={
            "marketplaceIds": [marketplace_id],
            "includedData": "summaries,images,attributes",
        },
    )

    if not isinstance(response, dict) or response.get("error"):
        return None

    return {
        "asin": _clean_text(response.get("asin") or asin),
        "title": _extract_catalog_title(response),
        "product_name": _extract_catalog_title(response),
        "main_image_url": _extract_catalog_image_url(response),
        "source": "catalog-items-api",
    }


def _fetch_catalog_meta_by_asin(asins: list[str], marketplace_id: str):
    clean_asins = []
    seen = set()
    for asin in asins:
        clean_asin = _clean_text(asin)
        if not clean_asin:
            continue
        clean_asin = clean_asin.upper()
        if clean_asin in seen:
            continue
        seen.add(clean_asin)
        clean_asins.append(clean_asin)

    if not clean_asins:
        return {}

    meta_by_asin = {}
    batch_size = 20
    pending = clean_asins[:60]

    while pending:
        chunk = pending[:batch_size]
        pending = pending[batch_size:]

        response = amazon_client.make_api_call(
            "/catalog/2022-04-01/items",
            "GET",
            params={
                "identifiers": ",".join(chunk),
                "identifiersType": "ASIN",
                "marketplaceIds": [marketplace_id],
                "includedData": "summaries,images,attributes",
            },
        )

        if not isinstance(response, dict) or response.get("error"):
            continue

        items = response.get("items") or response.get("payload") or []
        for catalog_item in items:
            if not isinstance(catalog_item, dict):
                continue

            identifiers = catalog_item.get("identifiers") or {}
            item_asin = _clean_text(catalog_item.get("asin") or identifiers.get("asin"))
            if not item_asin:
                continue

            meta_by_asin[item_asin.upper()] = {
                "asin": item_asin.upper(),
                "title": _extract_catalog_title(catalog_item),
                "product_name": _extract_catalog_title(catalog_item),
                "main_image_url": _extract_catalog_image_url(catalog_item),
                "source": "catalog-items-api",
            }

    return meta_by_asin


def _merge_catalog_meta(items: list[dict], marketplace_id: str):
    missing_asins = [
        item.get("asin")
        for item in items
        if item and item.get("asin") and (not item.get("main_image_url") or not item.get("title"))
    ]

    catalog_by_asin = _fetch_catalog_meta_by_asin(missing_asins, marketplace_id)
    if not catalog_by_asin:
        return items

    merged_items = []
    for item in items:
        if not item:
            continue

        catalog_meta = catalog_by_asin.get(str(item.get("asin") or "").upper())
        if not catalog_meta:
            merged_items.append(item)
            continue

        merged = {**catalog_meta, **item}
        merged["main_image_url"] = (
            _clean_text(item.get("main_image_url"))
            or _clean_text(catalog_meta.get("main_image_url"))
        )
        merged["title"] = _clean_text(item.get("title")) or _clean_text(catalog_meta.get("title"))
        merged["product_name"] = (
            _clean_text(item.get("product_name"))
            or _clean_text(item.get("title"))
            or _clean_text(catalog_meta.get("product_name"))
        )
        merged_items.append(merged)

    return merged_items


def _clean_text(value):
    if value is None:
        return None

    cleaned = str(value).strip()
    if not cleaned or cleaned.lower() in {"nan", "none", "null"}:
        return None

    return cleaned


def _clean_number(value):
    if value is None:
        return None

    try:
        return float(value)
    except (TypeError, ValueError):
        return value


def _quote_ident(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _first_existing_column(columns: set[str], candidates: list[str]):
    for candidate in candidates:
        if candidate in columns:
            return candidate
    return None


def _get_sku_catalog_columns(user_id: int):
    table_name = f"sku_{int(user_id)}_data_table"
    inspector = inspect(PHORMULA_ENGINE)

    if not inspector.has_table(table_name, schema="public"):
        return table_name, None

    columns = {column["name"] for column in inspector.get_columns(table_name, schema="public")}

    return table_name, {
        "columns": columns,
        "user_id": "user_id" if "user_id" in columns else None,
        "asin": _first_existing_column(columns, ["asin", "ASIN"]),
        "product_name": _first_existing_column(columns, ["product_name", "productName", "Product Name"]),
        "product_barcode": _first_existing_column(columns, ["product_barcode", "productBarcode", "barcode"]),
        "sku_uk": _first_existing_column(columns, ["sku_uk", "sku_UK", "SKU_UK"]),
        "sku_us": _first_existing_column(columns, ["sku_us", "sku_US", "SKU_US"]),
        "sku_canada": _first_existing_column(columns, ["sku_canada", "sku_ca", "SKU_Canada"]),
        "price": _first_existing_column(columns, ["price", "landing_cost", "landing cost"]),
        "currency": _first_existing_column(columns, ["currency"]),
    }


def _sku_column_for_marketplace(marketplace_id: str):
    return SKU_COLUMN_BY_MARKETPLACE.get(marketplace_id, "sku_us")


def _sku_catalog_select_expr(column_name, alias, default="NULL"):
    if not column_name:
        return f"{default} AS {alias}"
    return f"{_quote_ident(column_name)} AS {alias}"


def _fetch_sku_catalog_products(user_id: int, marketplace_id: str, search: str = "", limit: int = 50):
    table_name, catalog = _get_sku_catalog_columns(user_id)
    if not catalog or not catalog.get("asin"):
        return []

    preferred_sku_key = _sku_column_for_marketplace(marketplace_id)
    preferred_sku_col = catalog.get(preferred_sku_key)

    sku_fallback_cols = [
        catalog.get(preferred_sku_key),
        catalog.get("sku_us"),
        catalog.get("sku_uk"),
        catalog.get("sku_canada"),
    ]
    sku_fallback_cols = [col for index, col in enumerate(sku_fallback_cols) if col and col not in sku_fallback_cols[:index]]

    if sku_fallback_cols:
        sku_expr = "COALESCE(" + ", ".join(_quote_ident(col) for col in sku_fallback_cols) + ") AS sku"
    else:
        sku_expr = "NULL AS sku"

    search_columns = [
        catalog.get("asin"),
        catalog.get("product_name"),
        preferred_sku_col,
        catalog.get("sku_us"),
        catalog.get("sku_uk"),
        catalog.get("sku_canada"),
        catalog.get("product_barcode"),
    ]
    search_columns = [col for index, col in enumerate(search_columns) if col and col not in search_columns[:index]]

    where_clauses = [f"NULLIF(TRIM(CAST({_quote_ident(catalog['asin'])} AS TEXT)), '') IS NOT NULL"]
    params: Dict[str, Any] = {"limit": limit}

    if catalog.get("user_id"):
        where_clauses.append(f"{_quote_ident(catalog['user_id'])} = :user_id")
        params["user_id"] = int(user_id)

    if search and search_columns:
        search_parts = [
            f"LOWER(CAST({_quote_ident(column)} AS TEXT)) LIKE :search"
            for column in search_columns
        ]
        where_clauses.append("(" + " OR ".join(search_parts) + ")")
        params["search"] = f"%{search.lower()}%"

    sql = text(f"""
        SELECT
            {_sku_catalog_select_expr(catalog.get("asin"), "asin")},
            {_sku_catalog_select_expr(catalog.get("product_name"), "title")},
            {_sku_catalog_select_expr(catalog.get("product_barcode"), "product_barcode")},
            {sku_expr},
            {_sku_catalog_select_expr(catalog.get("sku_us"), "sku_us")},
            {_sku_catalog_select_expr(catalog.get("sku_uk"), "sku_uk")},
            {_sku_catalog_select_expr(catalog.get("sku_canada"), "sku_canada")},
            {_sku_catalog_select_expr(catalog.get("price"), "price")},
            {_sku_catalog_select_expr(catalog.get("currency"), "currency")}
        FROM public.{_quote_ident(table_name)}
        WHERE {" AND ".join(where_clauses)}
        ORDER BY {_quote_ident(catalog["asin"])} ASC
        LIMIT :limit
    """)

    with PHORMULA_ENGINE.connect() as conn:
        rows = conn.execute(sql, params).mappings().all()

    items = []
    seen = set()
    for row in rows:
        asin = _clean_text(row.get("asin"))
        if not asin:
            continue

        asin = asin.upper()
        if asin in seen:
            continue

        seen.add(asin)
        items.append({
            "asin": asin,
            "sku": _clean_text(row.get("sku")),
            "sku_us": _clean_text(row.get("sku_us")),
            "sku_uk": _clean_text(row.get("sku_uk")),
            "sku_canada": _clean_text(row.get("sku_canada")),
            "title": _clean_text(row.get("title")),
            "product_name": _clean_text(row.get("title")),
            "product_barcode": _clean_text(row.get("product_barcode")),
            "price": _clean_number(row.get("price")),
            "currency": _clean_text(row.get("currency")),
            "marketplace_id": marketplace_id,
            "source": "sku-data-table",
        })

    return items


def _fetch_sku_catalog_product_by_asin(user_id: int, marketplace_id: str, asin: str):
    matches = _fetch_sku_catalog_products(
        user_id=user_id,
        marketplace_id=marketplace_id,
        search=asin,
        limit=25,
    )
    for item in matches:
        if item.get("asin") == asin:
            return item
    return None


def _merge_product_sources(sku_item, product):
    if not sku_item and not product:
        return None

    merged = dict(sku_item or {})
    if product:
        merged.update({
            "asin": _clean_text(merged.get("asin")) or _clean_text(product.asin),
            "sku": _clean_text(merged.get("sku")) or _clean_text(product.sku),
            "title": (
                _clean_text(merged.get("title"))
                or _clean_text(merged.get("product_name"))
                or _clean_text(product.title)
            ),
            "product_name": (
                _clean_text(merged.get("product_name"))
                or _clean_text(merged.get("title"))
                or _clean_text(product.title)
            ),
            "brand": _clean_text(product.brand) or _clean_text(merged.get("brand")),
            "main_image_url": _clean_text(product.main_image_url) or _clean_text(merged.get("main_image_url")),
            "status": _clean_text(product.status) or _clean_text(merged.get("status")),
            "marketplace_id": product.marketplace_id,
        })

    if not merged.get("product_name") and merged.get("title"):
        merged["product_name"] = merged["title"]

    return merged


@customer_feedback_bp.route("/amazon_api/customer-feedback/products", methods=["GET"])
def customer_feedback_products():
    user_id, auth_error = _get_authenticated_user_id()
    if auth_error:
        return auth_error

    marketplace_id, _au, conn_error = _get_customer_feedback_connection(user_id)
    if conn_error:
        return conn_error

    search = (request.args.get("search") or "").strip()
    include_images = request.args.get("include_images", "false").strip().lower() == "true"

    try:
        limit = int(request.args.get("limit", "50"))
    except ValueError:
        return _json_error("limit must be a valid integer", 400)

    limit = max(1, min(limit, 100))

    sku_items = _fetch_sku_catalog_products(
        user_id=user_id,
        marketplace_id=marketplace_id,
        search=search,
        limit=limit,
    )

    query = Product.query.filter(
        Product.user_id == user_id,
        Product.marketplace_id == marketplace_id,
        Product.asin.isnot(None),
    )

    if search:
        pattern = f"%{search}%"
        query = query.filter(
            (Product.asin.ilike(pattern))
            | (Product.sku.ilike(pattern))
            | (Product.title.ilike(pattern))
        )

    products = (
        query.order_by(Product.updated_at.desc().nullslast(), Product.id.desc())
        .limit(limit)
        .all()
    )

    product_by_asin = {}
    for product in products:
        product_asin = (product.asin or "").strip().upper()
        if product_asin and product_asin not in product_by_asin:
            product_by_asin[product_asin] = product

    seen = set()
    items = []
    for sku_item in sku_items:
        asin = (sku_item.get("asin") or "").strip().upper()
        if not asin or asin in seen:
            continue

        seen.add(asin)
        items.append(_merge_product_sources(sku_item, product_by_asin.get(asin)))

    for product in products:
        asin = (product.asin or "").strip().upper()
        if not asin or asin in seen:
            continue
        seen.add(asin)
        items.append(_merge_product_sources(None, product))

    if include_images:
        items = _merge_catalog_meta(items, marketplace_id)

    return jsonify({
        "success": True,
        "source": "sku-data-table-and-products-table",
        "marketplace_id": marketplace_id,
        "count": len(items),
        "products": items,
    }), 200


@customer_feedback_bp.route("/amazon_api/customer-feedback/item-reviews", methods=["GET"])
def customer_feedback_item_reviews():
    user_id, auth_error = _get_authenticated_user_id()
    if auth_error:
        return auth_error

    marketplace_id, _au, conn_error = _get_customer_feedback_connection(user_id)
    if conn_error:
        return conn_error

    asin = (request.args.get("asin") or "").strip().upper()
    if not ASIN_RE.fullmatch(asin):
        return _json_error("asin must be a valid 10-character child ASIN", 400)

    base_params = {"marketplaceId": marketplace_id}

    topics_endpoint = f"/customerFeedback/2024-06-01/items/{asin}/reviews/topics"
    topics = _call_customer_feedback(
        topics_endpoint,
        {**base_params, "sortBy": "MENTIONS"},
    )
    if topics.get("error"):
        return _amazon_error_response("getItemReviewTopics", topics)

    rating_impact_topics = _call_customer_feedback(
        topics_endpoint,
        {**base_params, "sortBy": "STAR_RATING_IMPACT"},
    )
    if rating_impact_topics.get("error"):
        rating_impact_topics = None

    trends_endpoint = f"/customerFeedback/2024-06-01/items/{asin}/reviews/trends"
    trends = _call_customer_feedback(trends_endpoint, base_params)
    if trends.get("error"):
        return _amazon_error_response("getItemReviewTrends", trends)

    browse_node_endpoint = f"/customerFeedback/2024-06-01/items/{asin}/browseNode"
    browse_node = _call_customer_feedback(browse_node_endpoint, base_params)
    if browse_node.get("error"):
        browse_node = {
            "unavailable": True,
            "details": browse_node,
        }

    product = Product.query.filter_by(
        user_id=user_id,
        marketplace_id=marketplace_id,
        asin=asin,
    ).order_by(Product.updated_at.desc().nullslast(), Product.id.desc()).first()

    sku_product = _fetch_sku_catalog_product_by_asin(user_id, marketplace_id, asin)
    product_meta = _merge_product_sources(sku_product, product)

    if not product_meta or not product_meta.get("main_image_url"):
        catalog_meta = _fetch_catalog_product_meta(asin, marketplace_id)
        if catalog_meta:
            product_meta = {**(catalog_meta or {}), **(product_meta or {})}
            product_meta["main_image_url"] = (
                _clean_text((product_meta or {}).get("main_image_url"))
                or _clean_text(catalog_meta.get("main_image_url"))
            )
            product_meta["title"] = (
                _clean_text((product_meta or {}).get("title"))
                or _clean_text(catalog_meta.get("title"))
            )
            product_meta["product_name"] = (
                _clean_text((product_meta or {}).get("product_name"))
                or _clean_text(catalog_meta.get("product_name"))
            )

    if product_meta and not product_meta.get("title"):
        product_meta["title"] = _clean_text((topics or {}).get("itemName"))
        product_meta["product_name"] = product_meta["title"]

    return jsonify({
        "success": True,
        "source": "amazon-customer-feedback-api",
        "marketplace_id": marketplace_id,
        "country_code": CUSTOMER_FEEDBACK_MARKETPLACES.get(marketplace_id),
        "asin": asin,
        "sort_by": "MENTIONS_AND_STAR_RATING_IMPACT",
        "product": product_meta,
        "topics": topics if topics.get("ok") is not True else None,
        "rating_impact_topics": (
            rating_impact_topics
            if rating_impact_topics and rating_impact_topics.get("ok") is not True
            else None
        ),
        "trends": trends if trends.get("ok") is not True else None,
        "browse_node": browse_node if browse_node.get("ok") is not True else None,
        "notes": {
            "api_scope": "Amazon Customer Feedback API returns review insight topics and trends, not the Seller Central individual review inbox.",
            "refresh_frequency": "Amazon refreshes Customer Feedback API data weekly.",
        },
    }), 200
