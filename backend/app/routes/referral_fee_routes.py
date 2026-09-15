from flask import Blueprint, request, jsonify 
from flask_mail import Message
from sqlalchemy import create_engine
import jwt, time
import os
from sqlalchemy import MetaData, Table, select
from datetime import datetime 
from config import Config
SECRET_KEY = Config.SECRET_KEY
from app.models.user_models import User, Category, amazon_user
from app import db, mail  
from dotenv import load_dotenv
from datetime import datetime
import traceback
from sqlalchemy import inspect as sa_inspect
from app.utils.token_utils import get_effective_user_id_from_token
from app.utils.amazon_utils import amazon_client

load_dotenv()
db_url = os.getenv('DATABASE_URL')

referral_fee_bp = Blueprint('referral_fee_bp', __name__)


def send_referral_fee_reconciliation_email(email, reconciliation_data=None):
    """Send referral fee reconciliation email with breakdown of components and adjustments"""
    try:
        subject = 'Referral Fees Reconciliation - Fee Breakdown & Adjustments'
        
        # Build reconciliation breakdown section
        if reconciliation_data:
            breakdown_section = f"""
            <div style="background-color: #f8f9fa; padding: 20px; border-radius: 8px; margin: 20px 0; border-left: 4px solid #5EA68E;">
                <h3 style="color: #37455F; font-size: 18px; margin: 0 0 15px 0;">Referral Fee Breakdown</h3>
                <table style="width: 100%; border-collapse: collapse; font-size: 14px;">
                    <tr style="background-color: #e9ecef;">
                        <th style="padding: 10px; text-align: left; border-bottom: 1px solid #dee2e6;">Component</th>
                        <th style="padding: 10px; text-align: right; border-bottom: 1px solid #dee2e6;">Amount</th>
                    </tr>
                    {reconciliation_data.get('breakdown_rows', '')}
                    <tr style="font-weight: bold; background-color: #e8f5e8;">
                        <td style="padding: 10px; border-top: 2px solid #28a745;">Net Referral Fee</td>
                        <td style="padding: 10px; text-align: right; border-top: 2px solid #28a745; color: #28a745;">
                            ${reconciliation_data.get('net_amount', '0.00')}
                        </td>
                    </tr>
                </table>
            </div>
            """
            
            # Add adjustments section if present
            adjustments_section = ""
            if reconciliation_data.get('adjustments'):
                adjustments_section = f"""
                <div style="background-color: #fff3cd; padding: 20px; border-radius: 8px; margin: 20px 0; border-left: 4px solid #ffc107;">
                    <h3 style="color: #856404; font-size: 16px; margin: 0 0 10px 0;">Adjustments Applied</h3>
                    <ul style="font-size: 14px; line-height: 1.6; color: #856404; margin: 0; padding-left: 20px;">
                        {reconciliation_data.get('adjustments', '')}
                    </ul>
                </div>
                """
        else:
            breakdown_section = """
            <div style="background-color: #f8f9fa; padding: 20px; border-radius: 8px; margin: 20px 0; border-left: 4px solid #5EA68E;">
                <h3 style="color: #37455F; font-size: 16px; margin: 0 0 10px 0;">Reconciliation Summary</h3>
                <p style="font-size: 14px; line-height: 1.6; color: #555; margin: 0;">
                    Your referral fee reconciliation is being processed. You'll receive a detailed breakdown of all components and adjustments that affect your margins.
                </p>
            </div>
            """
            adjustments_section = ""

        msg = Message(
            subject, 
            sender=("Phormula Care Team", "care@phormula.io"),
            recipients=[email]
        )
        
        msg.html = f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>Referral Fee Reconciliation</title>
</head>
<body style="font-family: 'Lato', Arial, sans-serif; background-color: #f4f4f4; padding: 20px; margin: 0;">
    <div style="max-width: 600px; margin: 0 auto; background-color: #fff; padding: 30px; border-radius: 8px; border: 2px solid #5EA68E; box-shadow: 0 0 20px rgba(0, 0, 0, 0.1);">
        <img src="https://i.postimg.cc/43T3k86Z/logo.png" alt="Phormula Logo" style="width: 200px; height: auto; display: block; margin: 0 auto 20px;" />
        
        <h2 style="color: #5EA68E; font-size: 24px; font-weight: 600; text-align: center; margin-bottom: 20px;">
            Referral Fees Reconciliation
        </h2>
        
        <p style="font-size: 14px; line-height: 1.6; color: #555;">Hello,</p>
        
        <p style="font-size: 14px; line-height: 1.6; color: #555;">
            We're providing you with a clear breakdown of your referral fee components and any adjustments made to protect your margins.
        </p>
        
        {breakdown_section}
        
        {adjustments_section}
        
        <div style="background-color: #e8f5e8; padding: 15px; border-radius: 8px; margin: 20px 0; border-left: 4px solid #28a745;">
            <p style="font-size: 14px; line-height: 1.6; color: #155724; margin: 0;">
                <strong>Margin Protection:</strong> All adjustments are made to ensure optimal profitability while maintaining fair referral compensation.
            </p>
        </div>
        
        <div style="text-align: center; margin: 30px 0;">
            <a href="https://phormula.io/dashboard/reconciliation" style="background-color: #5EA68E; color: white; padding: 12px 30px; text-decoration: none; border-radius: 6px; font-weight: bold; display: inline-block;">
                View Full Reconciliation Report
            </a>
        </div>
        
        <p style="font-size: 14px; color: #555;">
            Questions about your reconciliation? Contact our support team at 
            <a href="mailto:care@phormula.io" style="color: #5EA68E; text-decoration: none;">care@phormula.io</a>
        </p>
        
        <p style="font-size: 14px; color: #555; margin-top: 30px;">Best regards, <br>The Phormula Finance Team</p>
        
        <div style="text-align: center; margin-top: 30px; padding-top: 20px; border-top: 1px solid #eee;">
            <p style="font-size: 12px; color: #999;">Transparent reconciliation for better business decisions</p>
        </div>
    </div>
</body>
</html>
        """
        
        # Send the reconciliation email
        mail.send(msg)
        print(f"Referral fee reconciliation email sent successfully to {email}")
        return True
        
    except Exception as e:
        print(f"Failed to send referral fee reconciliation email to {email}: {e}")
        return False
    

def get_user_from_token(auth_header):
    """Extract user from JWT token"""
    if not auth_header or not auth_header.startswith('Bearer '):
        return None, {'error': 'Authorization token required', 'status_code': 401}
    
    token = auth_header.split(' ')[1]
    try:
        payload, user_id, member_id = get_effective_user_id_from_token(token)
    except jwt.ExpiredSignatureError:
        return None, {'error': 'Token has expired', 'status_code': 401}
    except jwt.InvalidTokenError:
        return None, {'error': 'Invalid token', 'status_code': 401}


def add_cors_headers(response):
    """Add CORS headers to response"""
    response.headers.add('Access-Control-Allow-Origin', '*')
    response.headers.add('Access-Control-Allow-Headers', 'Content-Type,Authorization')
    response.headers.add('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE,OPTIONS')
    return response


@referral_fee_bp.route('/referral_fee_notification', methods=['POST', 'OPTIONS'])
def referral_fee_notification():
    """Send referral fee notification email to user"""
    
    # Handle CORS preflight request
    if request.method == 'OPTIONS':
        response = jsonify({'status': 'success'})
        return add_cors_headers(response)
    
    try:
        # Get user from token
        auth_header = request.headers.get('Authorization')
        user, error = get_user_from_token(auth_header)
        
        if error:
            response = jsonify(error), error['status_code']
            return add_cors_headers(response[0])
        
        # Get optional parameters from request body
        data = request.get_json() or {}
        referral_amount = data.get('referral_amount')
        referral_details = data.get('referral_details')
        
        # Send referral fee notification email
        if send_referral_fee_reconciliation_email(user.email, referral_details):
            response_data = {
                'status': 'success',
                'message': 'Referral fee notification email sent successfully!',
                'email': user.email
            }
            response = jsonify(response_data)
        else:
            response_data = {
                'status': 'error',
                'message': 'Failed to send referral fee notification email'
            }
            response = jsonify(response_data), 500
            
        return add_cors_headers(response)
        
    except Exception as e:
        print(f"Error in referral_fee_notification route: {e}")
        response = jsonify({
            'status': 'error',
            'message': 'Internal server error'
        }), 500
        
        return add_cors_headers(response[0])


@referral_fee_bp.route('/referral_fee_status', methods=['GET', 'OPTIONS'])
def referral_fee_status():
    """Get referral fee status and earnings for user"""
    
    # Handle CORS preflight request
    if request.method == 'OPTIONS':
        response = jsonify({'status': 'success'})
        return add_cors_headers(response)
    
    try:
        # Get user from token
        auth_header = request.headers.get('Authorization')
        user, error = get_user_from_token(auth_header)
        
        if error:
            response = jsonify(error), error['status_code']
            return add_cors_headers(response[0])
        
        # Placeholder data - adapt to your real referral schema
        referral_data = {
            'user_id': user.id,
            'email': user.email,
            'total_earnings': 0.0,       # Fetch from your referrals table
            'pending_earnings': 0.0,     # Fetch from your referrals table
            'total_referrals': 0,        # Count of successful referrals
            'referral_link': f"https://phormula.io/signup?ref={user.id}",
            'last_updated': datetime.utcnow().isoformat()
        }
        
        response = jsonify({
            'status': 'success',
            'data': referral_data
        })
        
        return add_cors_headers(response)
        
    except Exception as e:
        print(f"Error in referral_fee_status route: {e}")
        response = jsonify({
            'status': 'error',
            'message': 'Internal server error'
        }), 500
        
        return add_cors_headers(response[0])


# ---- small helpers for this file ----

MKT_TO_COUNTRY = {
    "ATVPDKIKX0DER": "United States",
    "A1F83G8C2ARO7P": "United Kingdom",
}

MKT_TO_CURRENCY = {
    "ATVPDKIKX0DER": "USD",
    "A1F83G8C2ARO7P": "GBP",
}

# Central config for price buckets (you can tweak / extend this)
PRICE_RANGE_BUCKETS = {
    # UK marketplace
    "A1F83G8C2ARO7P": [
        (0.0, 9.99),
        (10.0, 99.99),
    ],
    # US marketplace
    "ATVPDKIKX0DER": [
        (0.0, 9.99),
        (10.0, 99.99),
    ],
}

def get_price_bucket(price_val: float, marketplace_id: str):
    """
    Given a price and marketplace, return (price_from, price_to) bucket.
    Replace 0 lower bound with -50.
    """
    buckets = PRICE_RANGE_BUCKETS.get(marketplace_id) or []
    for low, high in buckets:
        if low <= price_val <= high:
            # 👇 replace 0 with -50
            price_from = -50 if low == 0 else low
            return price_from, high

    # Fallback: no defined bucket
    return price_val, price_val


def get_bucket_probe_price(low: float, high: float):
    if low <= 0:
        return high
    return low + 0.01


def _extract_referral_amount_from_fees(fees_resp):
    payload = (fees_resp or {}).get("payload") or {}
    fer = payload.get("FeesEstimateResult") or {}
    fees_est = fer.get("FeesEstimate")
    if not fees_est:
        return None

    for detail in (fees_est.get("FeeDetailList") or []):
        fee_type = (detail.get("FeeType") or "").lower()
        if fee_type == "referralfee":
            return float(
                (detail.get("FinalFee") or {}).get("Amount")
                or (detail.get("FeeAmount") or {}).get("Amount")
                or 0.0
            )
    return None


def _fetch_referral_fee_for_price(asin, marketplace_id, currency, price, is_fba):
    fees_req = {
        "FeesEstimateRequest": {
            "MarketplaceId": marketplace_id,
            "PriceToEstimateFees": {
                "ListingPrice": {
                    "CurrencyCode": currency,
                    "Amount": price
                },
                "Shipping": {
                    "CurrencyCode": currency,
                    "Amount": 0
                }
            },
            "Identifier": f"fee-{asin}-{price}-{int(time.time())}",
            "IsAmazonFulfilled": bool(is_fba) if is_fba is not None else False,
        }
    }
    fees_resp = amazon_client.make_api_call(
        f"/products/fees/v0/items/{asin}/feesEstimate",
        "POST",
        data=fees_req
    )
    referral_amount = _extract_referral_amount_from_fees(fees_resp)
    if referral_amount is None:
        reason, detail = _sp_api_skip_info(fees_resp, "missing_fees_estimate", "fees")
        return None, reason, detail

    referral_pct = round((referral_amount / price) * 100.0) if price > 0 else None
    return {
        "referral_amount": referral_amount,
        "referral_pct": referral_pct,
    }, None, None


def _fetch_referral_fee_bands(asin, marketplace_id, currency, is_fba):
    rows = []
    failures = []

    for low, high in PRICE_RANGE_BUCKETS.get(marketplace_id, []):
        probe_price = get_bucket_probe_price(low, high)
        price_from = -50 if low == 0 else low
        estimate, reason, detail = _fetch_referral_fee_for_price(
            asin=asin,
            marketplace_id=marketplace_id,
            currency=currency,
            price=probe_price,
            is_fba=is_fba,
        )
        if not estimate:
            failures.append({
                "probe_price": probe_price,
                "price_from": price_from,
                "price_to": high,
                "reason": reason,
                "detail": detail,
            })
            continue

        rows.append({
            "referral_fee": estimate["referral_amount"],
            "referral_fee_percent_est": estimate["referral_pct"],
            "price_from": price_from,
            "price_to": high,
            "probe_price": probe_price,
        })

    return rows, failures


def _dedupe_category_rows(rows):
    grouped = {}

    for row in rows:
        key = (
            str(row.country or "").strip().lower(),
            str(row.category or "").strip().lower(),
            str(row.brand or "").strip().lower(),
            float(row.price_from or 0.0),
            float(row.price_to or 0.0),
        )
        grouped.setdefault(key, []).append(row)

    unique_rows = []
    for group in grouped.values():
        # Pick the rate Amazon returned most often for this band. If tied,
        # prefer the higher rate because fee schedules normally step upward.
        counts = {}
        for row in group:
            pct = float(row.referral_fee_percent_est or 0.0)
            counts[pct] = counts.get(pct, 0) + 1
        selected_pct = sorted(counts.items(), key=lambda item: (item[1], item[0]), reverse=True)[0][0]
        selected = next(
            row for row in group
            if float(row.referral_fee_percent_est or 0.0) == selected_pct
        )
        unique_rows.append(selected)

    return unique_rows, len(rows) - len(unique_rows)



def _extract_currency_and_flags(offers_payload: dict):
    """
    Pull CurrencyCode (prefer BuyBox -> LandedPrice/listing), and FBA/BuyBox flags.
    Returns (currency, is_fba, is_buybox_winner)
    """
    if not isinstance(offers_payload, dict):
        return None, None, None

    summary = offers_payload.get("Summary") or {}
    offers  = offers_payload.get("Offers") or []

    currency = None
    # Prefer BuyBox currency
    try:
        bb = (summary.get("BuyBoxPrices") or [])[0]
        # LandedPrice first, then ListingPrice
        currency = (bb.get("LandedPrice") or {}).get("CurrencyCode") \
                   or (bb.get("ListingPrice") or {}).get("CurrencyCode")
    except Exception:
        pass

    # Fallback to first offer
    if not currency and offers:
        currency = ((offers[0].get("ListingPrice") or {}).get("CurrencyCode"))

    # Flags
    is_fba = None
    is_buybox_winner = None
    if offers:
        is_fba = offers[0].get("IsFulfilledByAmazon")
        is_buybox_winner = offers[0].get("IsBuyBoxWinner")

    return currency, is_fba, is_buybox_winner


def _extract_taxonomy_from_catalog(catalog_raw: dict):
    """
    From Catalog Items 2022-04-01 response, derive:
      category -> websiteDisplayGroupName
      subcategory -> browseClassification.displayName
      brand, item_name
    """
    # Handle either direct or "payload" wrapper
    src = catalog_raw.get("payload") if isinstance(catalog_raw, dict) and "payload" in catalog_raw else catalog_raw
    if not isinstance(src, dict):
        return None, None, None, None

    summaries = src.get("summaries") or []
    if not summaries and "items" in src:
        # some shapes: { items: [ { summaries: [...] } ] }
        items = src.get("items") or []
        if items:
            summaries = (items[0] or {}).get("summaries") or []

    if not summaries:
        return None, None, None, None

    s0 = summaries[0]
    category = s0.get("websiteDisplayGroupName") or s0.get("websiteDisplayGroup")
    subcategory = (s0.get("browseClassification") or {}).get("displayName")
    brand = s0.get("brand")
    item_name = s0.get("itemName")
    return category, subcategory, brand, item_name


def _parse_offers_payload(payload):
    """
    Works for both getItemOffers/getListingOffers response shapes.
    Returns (price, shipping) or (None, 0.0).
    """
    if not isinstance(payload, dict):
        return None, 0.0
    # 1) Buy Box landed price
    try:
        bb = (payload.get("Summary", {}).get("BuyBoxPrices") or [])[0]
        p = ((bb.get("Price") or {}).get("LandedPrice") or {}).get("Amount")
        s = ((bb.get("Price") or {}).get("Shipping") or {}).get("Amount") or 0.0
        if p: return float(p), float(s)
    except Exception:
        pass
    # 2) Lowest landed price
    try:
        lp = (payload.get("Summary", {}).get("LowestPrices") or [])[0]
        p = ((lp.get("Price") or {}).get("LandedPrice") or {}).get("Amount")
        s = ((lp.get("Price") or {}).get("Shipping") or {}).get("Amount") or 0.0
        if p: return float(p), float(s)
    except Exception:
        pass
    # 3) First offer listing price
    try:
        off = (payload.get("Offers") or [])[0]
        p = (off.get("ListingPrice") or {}).get("Amount")
        s = (off.get("Shipping") or {}).get("Amount") or 0.0
        if p: return float(p), float(s)
    except Exception:
        pass
    return None, 0.0


def _parse_price_payload_entry(entry):
    """
    Supports /products/pricing/v0/price payload entries (list of objects).
    Returns (price, shipping) or (None, 0.0).
    """
    if not isinstance(entry, dict):
        return None, 0.0
    product = entry.get("Product") or {}
    summary = product.get("Summary") or {}
    offers = product.get("Offers") or []

    # Buy Box
    try:
        bb = (summary.get("BuyBoxPrices") or [])[0]
        p = ((bb.get("Price") or {}).get("LandedPrice") or {}).get("Amount")
        s = ((bb.get("Price") or {}).get("Shipping") or {}).get("Amount") or 0.0
        if p: return float(p), float(s)
    except Exception:
        pass
    # Lowest
    try:
        lp = (summary.get("LowestPrices") or [])[0]
        p = ((lp.get("Price") or {}).get("LandedPrice") or {}).get("Amount")
        s = ((lp.get("Price") or {}).get("Shipping") or {}).get("Amount") or 0.0
        if p: return float(p), float(s)
    except Exception:
        pass
    # First offer
    try:
        off = offers[0]
        p = (off.get("ListingPrice") or {}).get("Amount")
        s = (off.get("Shipping") or {}).get("Amount") or 0.0
        if p: return float(p), float(s)
    except Exception:
        pass
    return None, 0.0


def _auto_fetch_price(*, asin: str | None, sku: str | None, marketplace_id: str, debug: bool = False):
    """
    Tries two Pricing APIs:
      A) getItemOffers/getListingOffers
      B) /products/pricing/v0/price (batch)
    Returns dict with price, shipping, optional debug.
    """
    dbg = {}

    # A) Offers endpoint
    if asin:
        ep = f"/products/pricing/v0/items/{asin}/offers"
    else:
        ep = f"/products/pricing/v0/listings/{sku}/offers"
    params = {"MarketplaceId": marketplace_id, "ItemCondition": "New", "CustomerType": "Consumer"}
    r1 = amazon_client.make_api_call(ep, "GET", params)
    if debug: dbg["offers_raw"] = r1
    payload = (r1 or {}).get("payload") or {}
    price, ship = _parse_offers_payload(payload)
    if price:
        out = {"price": price, "shipping": ship}
        if debug: out["debug"] = dbg
        return out

    # B) Batch price endpoint
    params2 = {"MarketplaceId": marketplace_id}
    if asin: params2["Asins"] = [asin]
    else:    params2["Skus"] = [sku]
    r2 = amazon_client.make_api_call("/products/pricing/v0/price", "GET", params2)
    if debug: dbg["price_raw"] = r2
    lst = (r2 or {}).get("payload") or []
    if isinstance(lst, list) and lst:
        price2, ship2 = _parse_price_payload_entry(lst[0])
        if price2:
            out = {"price": price2, "shipping": ship2}
            if debug: out["debug"] = dbg
            return out

    out = {"price": None, "shipping": 0.0}
    if debug: out["debug"] = dbg
    return out


def _verify_asin_in_marketplace(asin: str, marketplace_id: str):
    """
    Uses Catalog Items 2022-04-01 to check if an ASIN exists in a marketplace.
    Returns (True/False, raw_response).
    """
    res = amazon_client.make_api_call(
        f"/catalog/2022-04-01/items/{asin}",
        method="GET",
        params={"marketplaceIds": [marketplace_id]},
    )
    if isinstance(res, dict) and "error" not in res:
        return True, res
    return False, res


def _sp_api_skip_info(res, fallback_reason="sp_api_lookup_failed", operation="sp_api"):
    if not isinstance(res, dict):
        return fallback_reason, None

    status_code = res.get("status_code")
    response_json = res.get("response_json") or {}
    errors = response_json.get("errors") if isinstance(response_json, dict) else None
    first_error = errors[0] if isinstance(errors, list) and errors else {}
    code = str(first_error.get("code") or res.get("error") or "unknown").lower()
    message = first_error.get("message") or res.get("message") or res.get("error")

    if status_code in (401, 403) or code == "unauthorized":
        reason = f"{operation}_{status_code or 403}_unauthorized"
    elif status_code == 404:
        reason = f"{operation}_404_not_found"
    elif status_code:
        reason = f"{operation}_{status_code}_error"
    else:
        reason = fallback_reason

    detail = {
        "reason": reason,
        "status_code": status_code,
        "code": first_error.get("code") or res.get("error"),
        "message": message,
        "amzn_request_id": res.get("amzn_request_id"),
    }
    return reason, detail


@referral_fee_bp.route('/fetch_fees', methods=['POST'])
def fetch_and_store_fees():
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

    body = request.get_json(silent=True) or {}
    marketplace_id = body.get("marketplace_id") or amazon_client.marketplace_id
    if marketplace_id not in amazon_client.ALLOWED_MARKETPLACES:
        return jsonify({"error": f"Unsupported marketplace_id: {marketplace_id}"}), 400
    amazon_client.set_marketplace(marketplace_id)

    au = amazon_user.query.filter_by(
        user_id=user_id,
        marketplace_id=marketplace_id
    ).first()
    if not au or not au.refresh_token:
        return jsonify({
            "ok": False,
            "error": "Amazon account not connected for this marketplace",
            "marketplace_id": marketplace_id,
            "hint": "Connect this country from Amazon login before running /fetch_fees."
        }), 400
    amazon_client.set_refresh_token(au.refresh_token)

    table_name = f"sku_{user_id}_data_table"

    try:
        # -------- load user's SKU table --------
        user_engine = create_engine(db_url)
        inspector = sa_inspect(user_engine)
        if table_name not in inspector.get_table_names():
            return jsonify({'error': f'Table "{table_name}" not found'}), 404

        metadata = MetaData()
        sku_tbl = Table(table_name, metadata, autoload_with=user_engine)

        # We only need ASIN
        with user_engine.connect() as conn:
            rows = conn.execute(
                select(sku_tbl.c.asin).where(sku_tbl.c.asin.isnot(None))
            ).all()

        if not rows:
            return jsonify({
                "ok": True,
                "stored": 0,
                "skipped": 0,
                "message": "No ASINs found."
            }), 200

        asins = [r._mapping["asin"] for r in rows if r._mapping.get("asin")]

        stored, skipped = 0, 0
        failures = []
        rows_to_commit = []
        skip_reasons = {}
        skip_details = []
        country = MKT_TO_COUNTRY.get(marketplace_id, "Unknown")

        def mark_skipped(reason, asin=None, detail=None):
            nonlocal skipped
            skipped += 1
            skip_reasons[reason] = skip_reasons.get(reason, 0) + 1
            if detail and len(skip_details) < 10:
                item = {"asin": asin, **detail} if asin else detail
                skip_details.append(item)

        # -------- loop ASINs --------
        for asin in asins:
            try:
                ok, catalog_raw = _verify_asin_in_marketplace(asin, marketplace_id)
                if not ok:
                    reason, detail = _sp_api_skip_info(catalog_raw, "asin_not_found_in_marketplace", "catalog")
                    mark_skipped(reason, asin, detail)
                    continue

                # Category + brand from Catalog
                cat_name, _subcat, brand, _item_name = _extract_taxonomy_from_catalog(catalog_raw)
                if not cat_name:
                    cat_name = "Unknown"

                # Fetch price & shipping from Amazon (Pricing API)
                fetched = _auto_fetch_price(
                    asin=asin,
                    sku=None,
                    marketplace_id=marketplace_id,
                    debug=True,
                )
                offers_payload = ((fetched.get("debug") or {})
                                  .get("offers_raw") or {}).get("payload") or {}
                currency, is_fba, _is_bb = _extract_currency_and_flags(offers_payload)
                currency = currency or MKT_TO_CURRENCY.get(marketplace_id)

                if not currency:
                    debug_payload = fetched.get("debug") or {}
                    pricing_error = debug_payload.get("offers_raw") or debug_payload.get("price_raw")
                    reason, detail = _sp_api_skip_info(pricing_error, "missing_currency_or_price", "pricing")
                    mark_skipped(reason, asin, detail)
                    continue

                band_estimates, band_failures = _fetch_referral_fee_bands(
                    asin=asin,
                    marketplace_id=marketplace_id,
                    currency=currency,
                    is_fba=is_fba,
                )
                for failure in band_failures:
                    if len(skip_details) < 10:
                        detail = failure.get("detail") or {}
                        skip_details.append({
                            "asin": asin,
                            "probe_price": failure.get("probe_price"),
                            "price_from": failure.get("price_from"),
                            "price_to": failure.get("price_to"),
                            "reason": failure.get("reason"),
                            **detail,
                        })

                if not band_estimates:
                    first_failure = band_failures[0] if band_failures else {}
                    mark_skipped(
                        first_failure.get("reason") or "missing_fees_estimate",
                        asin,
                        first_failure.get("detail"),
                    )
                    continue

                for band in band_estimates:
                    row = Category(
                        user_id=user_id,
                        country=country,
                        category=cat_name,
                        referral_fee=band["referral_fee"],
                        referral_fee_percent_est=band["referral_fee_percent_est"],
                        brand=brand,
                        price_from=band["price_from"],
                        price_to=band["price_to"]
                    )

                    rows_to_commit.append(row)
                    stored += 1

            except Exception as ex:
                failures.append({"asin": asin, "error": str(ex)})

        estimated_rows = stored
        rows_to_commit, deduped_rows = _dedupe_category_rows(rows_to_commit)
        stored = len(rows_to_commit)

        if rows_to_commit:
            db.session.query(Category).filter_by(user_id=user_id, country=country).delete()
            db.session.add_all(rows_to_commit)
        db.session.commit()

        return jsonify({
            "ok": True,
            "stored": stored,
            "estimated_rows": estimated_rows,
            "deduped_rows": deduped_rows,
            "rate_source": "amazon_product_fees_estimate",
            "skipped": skipped,
            "skip_reasons": skip_reasons,
            "skip_details": skip_details,
            "failures": failures
        }), 200

    except Exception as e:
        traceback.print_exc()
        return jsonify({'error': 'An error occurred', 'message': str(e)}), 500

