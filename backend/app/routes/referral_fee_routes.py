from flask import Blueprint, request, jsonify 
from flask_mail import Message
from sqlalchemy import create_engine
import jwt, time
import os
from sqlalchemy import MetaData, Table, select
from datetime import datetime 
from config import Config
SECRET_KEY = Config.SECRET_KEY
from app.models.user_models import User, Category
from app import db, mail  
from dotenv import load_dotenv
from datetime import datetime
import traceback
from sqlalchemy import inspect as sa_inspect
from app.utils.amazon_utils import amazon_client

load_dotenv()
db_url = os.getenv('DATABASE_URL', 'postgresql://postgres:password@localhost:5432/phormula')

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
        payload = jwt.decode(token, SECRET_KEY, algorithms=['HS256'])
        user_id = payload.get('user_id')
        
        user = User.query.get(user_id)
        if not user:
            return None, {'error': 'User not found', 'status_code': 404}
        
        return user, None
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


@referral_fee_bp.route('/fetch_fees', methods=['POST'])
def fetch_and_store_fees():
    # -------- auth --------
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

    body = request.get_json(silent=True) or {}
    marketplace_id = body.get("marketplace_id") or amazon_client.marketplace_id

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

        # Optional: clear previous rows for this user (and maybe marketplace)
        db.session.query(Category).filter_by(user_id=user_id).delete()

        stored, skipped = 0, 0
        failures = []
        rows_to_commit = []

        # -------- loop ASINs --------
        for asin in asins:
            try:
                ok, catalog_raw = _verify_asin_in_marketplace(asin, marketplace_id)
                if not ok:
                    skipped += 1
                    continue

                # Category + brand from Catalog
                cat_name, _subcat, brand, _item_name = _extract_taxonomy_from_catalog(catalog_raw)
                if not cat_name:
                    cat_name = "Unknown"

                country = MKT_TO_COUNTRY.get(marketplace_id, "Unknown")

                # Fetch price & shipping from Amazon (Pricing API)
                fetched = _auto_fetch_price(
                    asin=asin,
                    sku=None,
                    marketplace_id=marketplace_id,
                    debug=True,
                )
                price_val = float(fetched.get("price") or 0.0)
                shipping_val = float(fetched.get("shipping") or 0.0)

                offers_payload = ((fetched.get("debug") or {})
                                  .get("offers_raw") or {}).get("payload") or {}
                currency, is_fba, _is_bb = _extract_currency_and_flags(offers_payload)

                if not currency or price_val <= 0:
                    skipped += 1
                    continue

                # --- fees estimate call ---
                fees_req = {
                    "FeesEstimateRequest": {
                        "MarketplaceId": marketplace_id,
                        "PriceToEstimateFees": {
                            "ListingPrice": {
                                "CurrencyCode": currency,
                                "Amount": price_val
                            },
                            "Shipping": {
                                "CurrencyCode": currency,
                                "Amount": shipping_val
                            }
                        },
                        "Identifier": f"fee-{asin}-{int(time.time())}",
                        "IsAmazonFulfilled": bool(is_fba) if is_fba is not None else False,
                    }
                }
                fees_resp = amazon_client.make_api_call(
                    f"/products/fees/v0/items/{asin}/feesEstimate",
                    "POST",
                    data=fees_req
                )
                payload = (fees_resp or {}).get("payload") or {}
                fer = payload.get("FeesEstimateResult") or {}
                fees_est = fer.get("FeesEstimate")
                if not fees_est:
                    skipped += 1
                    continue

                # ---- extract referral fee ----
                referral_amount = None
                for d in (fees_est.get("FeeDetailList") or []):
                    fee_type = (d.get("FeeType") or "").lower()
                    if fee_type == "referralfee":
                        referral_amount = float(
                            (d.get("FinalFee") or {}).get("Amount")
                            or (d.get("FeeAmount") or {}).get("Amount")
                            or 0.0
                        )
                if referral_amount is None:
                    skipped += 1
                    continue

                # percentage (rounded to whole number: 15, 8, etc.)
                raw_pct = (referral_amount / price_val * 100.0) if price_val > 0 else None
                referral_pct = round(raw_pct) if raw_pct is not None else None

                # -------- fixed price buckets via helper --------
                price_from, price_to = get_price_bucket(price_val, marketplace_id)

                # -------- create Category row --------
                row = Category(
                    user_id=user_id,
                    country=country,
                    category=cat_name,
                    referral_fee=referral_amount,
                    referral_fee_percent_est=referral_pct,
                    brand=brand,
                    price_from=price_from,
                    price_to=price_to
                )

                rows_to_commit.append(row)
                stored += 1

            except Exception as ex:
                failures.append({"asin": asin, "error": str(ex)})

        db.session.add_all(rows_to_commit)
        db.session.commit()

        return jsonify({
            "ok": True,
            "stored": stored,
            "skipped": skipped,
            "failures": failures
        }), 200

    except Exception as e:
        traceback.print_exc()
        return jsonify({'error': 'An error occurred', 'message': str(e)}), 500

