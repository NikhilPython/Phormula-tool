"""Independent US standard-size, non-apparel FBA fulfillment estimates.

Rates: Amazon 2026 US FBA fulfillment fee changes, checked 2026-10-05:
https://sellercentral.amazon.com/help/hub/reference/external/GABBX6GZPA8MSZGW
Size tiers / dimensional weight:
https://sellercentral.amazon.com/help/hub/reference/external/G53Z9EKF8VVZVH29

This is a base fulfillment estimate, not a reimbursement determination. Storage,
inbound placement, low-inventory fees, SIPP discounts and returns are separate.
Unsupported dates, products and missing inputs must remain unavailable (None).
Never use the actual charged fee as an expected fee or use a referral percentage.
"""
from datetime import date, datetime
from decimal import Decimal, InvalidOperation, ROUND_CEILING, ROUND_HALF_UP
import json
from pathlib import Path


D = Decimal
US_MARKETPLACE = "ATVPDKIKX0DER"
VERIFIED_MEASUREMENTS = None
NON_APPAREL_TYPES = {
    "BEAUTY", "CONDITIONER", "SKIN_CLEANING_AGENT", "SKIN_CLEANING_WIPE",
    "SKIN_MOISTURIZER", "SKIN_TREATMENT_MASK", "TOPICAL_HAIR_REGROWTH_TREATMENT",
}
# Each row: upper shipping weight in ounces, under-$10 fee, $10-$50 fee.
# The >$50 standard-size fee is $0.26 more than the $10-$50 fee.
SMALL_NON_PEAK = [
    (2, "2.43", "3.32"), (4, "2.49", "3.42"), (6, "2.56", "3.45"),
    (8, "2.66", "3.54"), (10, "2.77", "3.68"), (12, "2.82", "3.78"),
    (14, "2.92", "3.91"), (16, "2.95", "3.96"),
]
SMALL_PEAK = [
    (2, "2.62", "3.51"), (4, "2.68", "3.61"), (6, "2.76", "3.65"),
    (8, "2.86", "3.74"), (10, "2.98", "3.89"), (12, "3.03", "3.99"),
    (14, "3.14", "4.13"), (16, "3.17", "4.18"),
]
LARGE_NON_PEAK = [
    (4, "2.91", "3.73"), (8, "3.13", "3.95"), (12, "3.38", "4.20"),
    (16, "3.78", "4.60"), (20, "4.22", "5.04"), (24, "4.60", "5.42"),
    (28, "4.75", "5.57"), (32, "5.00", "5.82"), (36, "5.10", "5.92"),
    (40, "5.28", "6.10"), (44, "5.44", "6.26"), (48, "5.85", "6.67"),
]
LARGE_PEAK = [
    (4, "3.15", "3.97"), (8, "3.39", "4.21"), (12, "3.66", "4.48"),
    (16, "4.07", "4.89"), (20, "4.52", "5.34"), (24, "4.91", "5.73"),
    (28, "5.07", "5.89"), (32, "5.33", "6.15"), (36, "5.47", "6.29"),
    (40, "5.67", "6.49"), (44, "5.84", "6.66"), (48, "6.26", "7.08"),
]


def number(value):
    try:
        result = D(str(value))
        return result if result.is_finite() else None
    except (InvalidOperation, TypeError, ValueError):
        return None


def json_object(value):
    if isinstance(value, dict):
        return value
    try:
        parsed = json.loads(value)
        return parsed if isinstance(parsed, dict) else {}
    except (TypeError, ValueError):
        return {}


def _inches(measure):
    measure = json_object(measure)
    value = number(measure.get("value"))
    divisor = {
        "inches": D(1), "inch": D(1), "in": D(1),
        "centimeters": D("2.54"), "cm": D("2.54"),
        "millimeters": D("25.4"), "mm": D("25.4"),
    }.get(str(measure.get("unit", "")).lower())
    return value / divisor if value is not None and value > 0 and divisor else None


def _pounds(value, unit):
    value = number(value)
    divisor = {
        "pounds": D(1), "pound": D(1), "lb": D(1), "lbs": D(1),
        "ounces": D(16), "ounce": D(16), "oz": D(16),
        "grams": D("453.59237"), "gram": D("453.59237"), "g": D("453.59237"),
        "kilograms": D("0.45359237"), "kg": D("0.45359237"),
    }.get(str(unit).lower())
    return value / divisor if value is not None and value > 0 and divisor else None


def resolve_fba_measurements(product, fee_preview=None):
    """Resolve packaged measurements for any SKU without reading charged fees.

    Amazon Fee Preview measurements take precedence over listing attributes.
    Their current values remain estimates for historical transactions.
    Complete measurement sets are selected together to avoid mixing snapshots.
    """
    product = dict(product or {})
    candidates = []
    if fee_preview and all(str(fee_preview.get(key)) == str(product.get(key))
                           for key in ("user_id", "marketplace_id", "sku", "asin")):
        candidates.append(({
            "package_dimensions": {
                axis: {"value": fee_preview.get(field), "unit": fee_preview.get("unit_of_dimension")}
                for axis, field in (("length", "longest_side"), ("width", "median_side"), ("height", "shortest_side"))
            },
            "package_weight_value": fee_preview.get("item_package_weight"),
            "package_weight_unit": fee_preview.get("unit_of_weight"),
        }, "amazon_fee_preview_measurements_historical_estimate"))
    candidates.append((product, product.get("fba_measurement_source", "estimated_standard_fulfillment")))
    attributes = json_object(product.get("attributes"))
    def marketplace_attribute(name):
        values = attributes.get(name) or []
        if isinstance(values, dict):
            values = [values]
        return next((v for v in values if isinstance(v, dict)
                     and v.get("marketplace_id") == product.get("marketplace_id")), {})
    package_weight = marketplace_attribute("item_package_weight")
    candidates.append(({
        "package_dimensions": marketplace_attribute("item_package_dimensions"),
        "package_weight_value": package_weight.get("value"),
        "package_weight_unit": package_weight.get("unit"),
    }, "amazon_package_attributes_historical_estimate"))
    for candidate, source in candidates:
        dimensions = json_object(candidate.get("package_dimensions"))
        if (all(_inches(dimensions.get(axis)) is not None for axis in ("length", "width", "height"))
                and _pounds(candidate.get("package_weight_value"), candidate.get("package_weight_unit")) is not None):
            return {**product, **{key: candidate.get(key) for key in
                    ("package_dimensions", "package_weight_value", "package_weight_unit")},
                    "fba_measurement_source": source}
    return product


def estimate_fba_fee(product, *, quantity, unit_price, shipped_on, marketplace_id,
                     fulfillment_channel=None, transaction_type="order"):
    """Return (total currency amount or None, explanation). Quantity is sold units.

    Uses package measurements from public.products, not unpackaged item measures.
    A listing's current measurements produce an estimate for historical shipments.
    The caller supplies the transaction price/date, never today's listing price.
    """
    txn = str(transaction_type or "").strip().lower()
    if txn and txn not in {"order", "shipment", "shipped", "sale"}:
        return 0.0, "not_fulfillment_sale"
    units = number(quantity)
    if units is None or units < 0 or units != units.to_integral_value():
        return None, "invalid_quantity"
    if units == 0:
        return 0.0, "no_shipped_units"
    channel = str(fulfillment_channel or "").strip().upper()
    if channel in {"MERCHANT", "MFN", "FBM", "DEFAULT", "SELLER"}:
        return 0.0, "merchant_fulfilled"
    if not product or product.get("marketplace_id") != marketplace_id:
        return None, "missing_product_for_marketplace"
    if not channel or channel in {"NAN", "NONE", "NULL", "0", "0.0"}:
        channel = str(product.get("fulfillment_channel") or "").strip().upper()
    if channel in {"MERCHANT", "MFN", "FBM", "DEFAULT", "SELLER"}:
        return 0.0, "merchant_fulfilled"
    if channel not in {"AMAZON", "AMAZON_NA", "AFN", "FBA"}:
        return None, "unrecognized_fulfillment_channel"
    if marketplace_id != US_MARKETPLACE:
        return None, "unsupported_marketplace_rate_card"
    if isinstance(shipped_on, datetime):
        shipped_on = shipped_on.date()
    if not isinstance(shipped_on, date):
        return None, "missing_shipment_date"
    if not date(2026, 1, 15) <= shipped_on <= date(2027, 1, 14):
        return None, "unsupported_rate_date"
    if str(product.get("product_type") or "").upper() not in NON_APPAREL_TYPES:
        return None, "unsupported_product_type"
    attributes = json_object(product.get("attributes"))
    regulations = attributes.get("supplier_declared_dg_hz_regulation") or []
    if isinstance(regulations, dict):
        regulations = [regulations]
    if any(str(r.get("value", "")).lower() not in {"", "not_applicable", "none"}
           for r in regulations if isinstance(r, dict)):
        return None, "dangerous_goods_require_separate_rates"
    price = number(unit_price)
    if price is None or price < 0:
        return None, "missing_transaction_price"
    # Verified Amazon shipping measurements take precedence over current catalog
    # inputs only for the configured seller, SKU/ASIN, marketplace and period.
    # Rates still come from the normal price/date/size/weight rate-card lookup.
    verified = next((m for m in VERIFIED_MEASUREMENTS
                     if str(product.get("user_id")) == str(m["user_id"])
                     and product.get("sku") == m["sku"]
                     and product.get("asin") == m["asin"]
                     and marketplace_id == m["marketplace_id"]
                     and date.fromisoformat(m["effective_from"]) <= shipped_on
                     <= date.fromisoformat(m["effective_to"])), None)
    if verified:
        product = {**product, **{key: verified[key] for key in
                   ("package_dimensions", "package_weight_value", "package_weight_unit")}}
    else:
        product = resolve_fba_measurements(product)
    dimensions = json_object(product.get("package_dimensions"))
    sides = [_inches(dimensions.get(axis)) for axis in ("length", "width", "height")]
    weight = _pounds(product.get("package_weight_value"), product.get("package_weight_unit"))
    if any(side is None for side in sides) or weight is None:
        return None, "missing_package_dimensions_or_weight"
    longest, median, shortest = sorted(sides, reverse=True)
    small = weight <= 1 and longest <= 15 and median <= 12 and shortest <= D("0.75")
    # Standard-size dimensional weight uses measured package volume in inches.
    shipping_weight = weight if small else max(weight, longest * median * shortest / D(139))
    if verified:
        shipping_weight = _pounds(verified["shipping_weight_value"], verified["shipping_weight_unit"])
        if shipping_weight is None:
            return None, "invalid_verified_shipping_weight"
    if not small and not (longest <= 18 and median <= 14 and shortest <= 8 and shipping_weight <= 20):
        return None, "unsupported_oversize_rate_card"
    peak = shipped_on >= date(2026, 10, 15)
    rates = (SMALL_PEAK if peak else SMALL_NON_PEAK) if small else (LARGE_PEAK if peak else LARGE_NON_PEAK)
    band = 1 if price < 10 else 2
    base = next((D(row[band]) for row in rates if shipping_weight * 16 <= row[0]), None)
    if base is None:
        intervals = ((shipping_weight - 3) * 4).to_integral_value(rounding=ROUND_CEILING)
        base = D("6.69" if peak else "6.15") + D("0.08") * intervals
        if price >= 10:
            base += D("0.82")
    if price > 50:
        base += D("0.26")
    if shipped_on >= date(2026, 4, 17):
        base *= D("1.035") 
    per_unit = base.quantize(D("0.01"), rounding=ROUND_HALF_UP)
    return float(per_unit * units), (
        "amazon_measurements_2026_09_01_assumed_for_september"
        if verified else product.get("fba_measurement_source", "estimated_standard_fulfillment")
    )
