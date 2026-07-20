from __future__ import annotations

import json
import logging
import re
from dataclasses import asdict, dataclass
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set

from pydantic import BaseModel, Field

from app.ai_agent.db import INVENTORY_METRICS, available_metrics

logger = logging.getLogger(__name__)


DERIVED_CONTEXT_METRICS = {
    "return_rate",
    "fee_ratio",
    "ads_roas",
    "ads_acos",
    "ads_cpc",
    "ads_ctr",
    "ads_conversion_rate",
    "tacos_total_advertising_cost_of_sale",
    "cm2_profit_per",
    "total_cm2_margins",
}

METRIC_NORMALIZATION = {
    "advertising": "total_ads",
    "advertising_total": "total_ads",
    "ad_spend": "total_ads",
    "ads": "total_ads",
    "amazon_fee": "amazon_fees",
    "cm2": "total_cm2_profit",
    "cm2_margins": "cm2_profit_per",
    "acos": "ads_acos",
    "refund_quantity": "return_quantity",
    "refund_units": "return_quantity",
    "gross_units": "quantity",
    "ordered_units": "quantity",
    "net_units": "total_quantity",
    "sold_units": "total_quantity",
    "subscription_fee": "platformfeenew",
    "subscription_fees": "platformfeenew",
    "selling_fee": "selling_fees",
    "selling_fees": "selling_fees",
    "seller_fee": "selling_fees",
    "seller_fees": "selling_fees",
    "referral_fee": "selling_fees",
    "referral_fees": "selling_fees",
    "amazon_referral_fee": "selling_fees",
    "amazon_referral_fees": "selling_fees",
    "refund_selling_fee": "refund_selling_fees",
    "fba_fee": "fba_fees",
    "fba_fees": "fba_fees",
    "fba_charge": "fba_fees",
    "fba_charges": "fba_fees",
    "fulfillment_fee": "fba_fees",
    "fulfillment_fees": "fba_fees",
    "fulfilment_fee": "fba_fees",
    "fulfilment_fees": "fba_fees",
    "amazon_fulfillment_fee": "fba_fees",
    "amazon_fulfillment_fees": "fba_fees",
    "amazon_fulfilment_fee": "fba_fees",
    "amazon_fulfilment_fees": "fba_fees",
    "misc": "misc_transaction",
    "miscellaneous": "misc_transaction",
    "misc_charges": "misc_transaction",
    "miscellaneous_charges": "misc_transaction",
    "misc_transactions": "misc_transaction",
    "miscellaneous_transactions": "misc_transaction",
    "other_charges": "other_transaction_fees",
    "other_charge": "other_transaction_fees",
    "other_transaction_fee": "other_transaction_fees",
}


@dataclass(frozen=True)
class MetricSemantic:
    name: str
    label: str
    category: str
    description: str
    better_when: str = "context"
    unit: str = "number"
    queryable: bool = True
    diagnostic_priority: int = 50
    related_metrics: tuple[str, ...] = ()

    def prompt_record(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "label": self.label,
            "category": self.category,
            "meaning": self.description,
            "better_when": self.better_when,
            "unit": self.unit,
            "queryable": self.queryable,
            "related_metrics": list(self.related_metrics),
        }


class SemanticResolutionModel(BaseModel):
    primary_metric_name: Optional[str] = None
    metric_names: List[str] = Field(default_factory=list)
    is_broad_business_analysis: bool = False
    needs_anomaly_scan: bool = False
    needs_forecast_data: bool = False
    subject_scope: Optional[str] = None
    analysis_type: Optional[str] = None
    reasoning_mode: Optional[str] = None
    task_type: Optional[str] = None
    answer_shape: Optional[str] = None
    confidence: float = Field(default=0.0, ge=0.0, le=1.0)
    reason: Optional[str] = None


@dataclass
class SemanticResolution:
    primary_metric_name: Optional[str] = None
    metric_names: List[str] = None
    is_broad_business_analysis: bool = False
    needs_anomaly_scan: bool = False
    needs_forecast_data: bool = False
    subject_scope: Optional[str] = None
    analysis_type: Optional[str] = None
    reasoning_mode: Optional[str] = None
    task_type: Optional[str] = None
    answer_shape: Optional[str] = None
    confidence: float = 0.0
    reason: Optional[str] = None

    def __post_init__(self) -> None:
        if self.metric_names is None:
            self.metric_names = []

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def _metric(
    name: str,
    label: str,
    category: str,
    description: str,
    *,
    better_when: str = "context",
    unit: str = "number",
    queryable: bool = True,
    diagnostic_priority: int = 50,
    related_metrics: Sequence[str] = (),
) -> MetricSemantic:
    return MetricSemantic(
        name=name,
        label=label,
        category=category,
        description=description,
        better_when=better_when,
        unit=unit,
        queryable=queryable,
        diagnostic_priority=diagnostic_priority,
        related_metrics=tuple(related_metrics),
    )


SEMANTIC_OVERRIDES: Dict[str, MetricSemantic] = {
    "profit": _metric(
        "profit",
        "CM1 profit",
        "profitability",
        "CM1 profit: sales plus credits minus taxes, Amazon fees, cost of units sold, and other CM1 deductions. Ads and platform fees are not subtracted here; they are subtracted in CM2 profit.",
        better_when="up",
        unit="money",
        diagnostic_priority=100,
        related_metrics=("net_sales", "gross_sales", "total_quantity", "return_quantity", "asp", "total_cm2_profit", "cm2_profit", "promotional_rebates", "refund_sales", "amazon_fees", "selling_fees", "fba_fees", "ads_spend", "platform_fee"),
    ),
    "net_sales": _metric(
        "net_sales",
        "Net sales",
        "sales",
        "revenue after refunds and sales adjustments; the main sales value for business performance",
        better_when="up",
        unit="money",
        diagnostic_priority=95,
        related_metrics=("gross_sales", "total_quantity", "asp", "refund_sales", "promotional_rebates"),
    ),
    "gross_sales": _metric("gross_sales", "Gross sales", "sales", "sales before refunds and deductions", better_when="up", unit="money", diagnostic_priority=75),
    "product_sales": _metric("product_sales", "Product sales", "sales", "product sales before some order adjustments", better_when="up", unit="money", diagnostic_priority=70),
    "total_quantity": _metric(
        "total_quantity",
        "Net sold units",
        "demand",
        "net sold units after refunds: total_quantity = quantity minus refund_quantity/return_quantity; use this for true sold unit movement",
        better_when="up",
        unit="count",
        diagnostic_priority=92,
        related_metrics=("return_quantity", "net_sales", "asp", "available", "days_of_supply"),
    ),
    "quantity": _metric("quantity", "Gross units", "demand", "gross units before refund_quantity/return_quantity is removed", better_when="up", unit="count", diagnostic_priority=72, related_metrics=("return_quantity", "total_quantity", "net_sales")),
    "asp": _metric("asp", "Average selling price", "pricing", "average selling price per sold unit; detects pricing and discount-mix movement", better_when="context", unit="money", diagnostic_priority=80),
    "profit_percentage": _metric("profit_percentage", "CM1 profit margin", "profitability", "CM1 profit as a percentage of sales", better_when="up", unit="percentage", diagnostic_priority=86),
    "cm2_profit": _metric("cm2_profit", "SKU CM2 profit", "margin", "SKU-level CM2 contribution after CM1 profit minus ads and platform fees where allocated to SKU", better_when="up", unit="money", diagnostic_priority=90, related_metrics=("profit", "ads_spend", "total_ads", "platform_fee", "platformfeenew", "platform_fee_inventory_storage", "lost_total")),
    "total_cm2_profit": _metric("total_cm2_profit", "Account CM2 profit", "margin", "month/account-level CM2 contribution after CM1 profit minus ads, platform fees, storage charges, and recovery offsets such as lost_total", better_when="up", unit="money", diagnostic_priority=88, related_metrics=("profit", "ads_spend", "total_ads", "platform_fee", "platformfeenew", "platform_fee_inventory_storage", "lost_total")),
    "cm2_profit_per": _metric("cm2_profit_per", "CM2 margin", "margin", "CM2 profit as a percentage of sales", better_when="up", unit="percentage", queryable=False, diagnostic_priority=84, related_metrics=("total_cm2_profit", "profit", "ads_spend", "total_ads", "platform_fee", "platform_fee_inventory_storage", "lost_total")),
    "total_cm2_margins": _metric("total_cm2_margins", "CM2 margin", "margin", "overall CM2 margin percentage", better_when="up", unit="percentage", queryable=False, diagnostic_priority=82),
    "promotional_rebates": _metric(
        "promotional_rebates",
        "Promo rebates",
        "discounts",
        "coupons, deals, rebates, and discounts. Negative values mean discount/rebate paid out by the seller; positive values mean amount received back. Treat more negative movement as a higher discount burden.",
        better_when="context",
        unit="money",
        diagnostic_priority=82,
        related_metrics=("total_quantity", "quantity", "return_quantity", "net_sales", "profit", "cm2_profit"),
    ),
    "promotional_rebates_tax": _metric("promotional_rebates_tax", "Promo rebate tax", "discounts", "tax impact linked to promotional rebates", better_when="down", unit="money", diagnostic_priority=55),
    "refund_sales": _metric("refund_sales", "Refund sales", "returns", "money refunded to customers; high refunds can reduce sales and profit", better_when="down", unit="money", diagnostic_priority=78),
    "return_quantity": _metric("return_quantity", "Refund quantity", "returns", "units refunded or returned; this is subtracted from gross units to get net sold units", better_when="down", unit="count", diagnostic_priority=76),
    "return_rate": _metric("return_rate", "Return rate", "returns", "refund quantity divided by gross units", better_when="down", unit="percentage", queryable=False, diagnostic_priority=72),
    "cogs": _metric("cogs", "COGS", "costs", "cost of goods sold or product cost burden", better_when="down", unit="money", diagnostic_priority=76),
    "selling_fees": _metric("selling_fees", "Selling fees", "fees", "Amazon referral and selling fee burden", better_when="down", unit="money", diagnostic_priority=78),
    "fba_fees": _metric("fba_fees", "FBA fees", "fees", "Amazon fulfillment fee burden", better_when="down", unit="money", diagnostic_priority=78),
    "amazon_fees": _metric("amazon_fees", "Amazon fees", "fees", "overall Amazon fee burden", better_when="down", unit="money", diagnostic_priority=80),
    "marketplace_fees": _metric("marketplace_fees", "Marketplace fees", "fees", "marketplace fee burden", better_when="down", unit="money", diagnostic_priority=64),
    "platform_fee": _metric("platform_fee", "Platform fees", "fees", "final platform fee amount: subscription charges, inventory storage charges, and other platform fee components summed together", better_when="down", unit="money", diagnostic_priority=80, related_metrics=("platformfeenew", "platform_fee_inventory_storage", "profit", "total_cm2_profit", "net_sales")),
    "platformfeenew": _metric("platformfeenew", "Subscription fees", "fees", "subscription charge component of platform fees", better_when="down", unit="money", diagnostic_priority=70, related_metrics=("platform_fee", "profit")),
    "platform_fee_inventory_storage": _metric("platform_fee_inventory_storage", "Inventory storage fees", "fees", "inventory storage charge component of platform fees", better_when="down", unit="money", diagnostic_priority=68, related_metrics=("platform_fee", "available", "days_of_supply")),
    "lost_total": _metric("lost_total", "Lost inventory reimbursement", "recovery", "Amazon reimbursement or compensation received for lost or damaged inventory. Treat it as a recovery/offset, not lost sales or leakage.", better_when="up", unit="money", diagnostic_priority=74, related_metrics=("total_cm2_profit", "cm2_profit", "platform_fee")),
    "misc_transaction": _metric("misc_transaction", "Misc transactions", "fees", "miscellaneous transaction-level charges or credits from raw Amazon settlement rows", better_when="down", unit="money", diagnostic_priority=72, related_metrics=("other_transaction_fees", "other", "profit", "total_cm2_profit")),
    "other_transaction_fees": _metric("other_transaction_fees", "Other transaction fees", "fees", "other transaction fees from raw Amazon settlement rows", better_when="down", unit="money", diagnostic_priority=70, related_metrics=("misc_transaction", "other", "profit", "total_cm2_profit")),
    "other": _metric("other", "Other amount", "fees", "other raw settlement amount, often used for non-order adjustments such as credits or transfers", better_when="context", unit="money", diagnostic_priority=58, related_metrics=("misc_transaction", "other_transaction_fees")),
    "ads_spend": _metric("ads_spend", "SKU ad spend", "advertising", "advertising spend allocated by SKU", better_when="context", unit="money", diagnostic_priority=74),
    "total_ads": _metric("total_ads", "Ad spend", "advertising", "total advertising spend across sponsored product, brand, display, and other ad spend", better_when="context", unit="money", diagnostic_priority=76),
    "product_spend": _metric("product_spend", "Sponsored Product spend", "advertising", "Sponsored Product advertising spend", better_when="context", unit="money", diagnostic_priority=66),
    "brand_spend": _metric("brand_spend", "Sponsored Brand spend", "advertising", "Sponsored Brand advertising spend", better_when="context", unit="money", diagnostic_priority=62),
    "display_spend": _metric("display_spend", "Sponsored Display spend", "advertising", "Sponsored Display advertising spend", better_when="context", unit="money", diagnostic_priority=62),
    "ads_sale_amount": _metric("ads_sale_amount", "Ad sales", "advertising", "sales attributed to advertising", better_when="up", unit="money", diagnostic_priority=70),
    "ads_sale_units": _metric("ads_sale_units", "Ad sales units", "advertising", "units attributed to advertising", better_when="up", unit="count", diagnostic_priority=65),
    "ads_roas": _metric("ads_roas", "Ad ROAS", "advertising", "ad sales divided by ad spend", better_when="up", unit="ratio", queryable=False, diagnostic_priority=70),
    "ads_acos": _metric("ads_acos", "Ad ACOS", "advertising", "ad spend divided by ad-attributed sales", better_when="down", unit="percentage", queryable=False, diagnostic_priority=70),
    "tacos_total_advertising_cost_of_sale": _metric("tacos_total_advertising_cost_of_sale", "TACOS", "advertising", "ad spend as a percentage of total net sales", better_when="down", unit="percentage", queryable=False, diagnostic_priority=72),
    "ads_cpc": _metric("ads_cpc", "Ad CPC", "advertising", "average cost per ad click", better_when="down", unit="money", queryable=False, diagnostic_priority=60),
    "ads_ctr": _metric("ads_ctr", "Ad CTR", "advertising", "ad click-through rate from impressions to clicks", better_when="up", unit="percentage", queryable=False, diagnostic_priority=58),
    "ads_conversion_rate": _metric("ads_conversion_rate", "Ad conversion rate", "advertising", "ad sales units divided by ad clicks", better_when="up", unit="percentage", queryable=False, diagnostic_priority=64),
    "available": _metric("available", "Available inventory", "inventory", "sellable inventory available for fulfilment", better_when="context", unit="count", diagnostic_priority=78),
    "inbound_quantity": _metric("inbound_quantity", "Inbound inventory", "inventory", "inventory already inbound to Amazon", better_when="context", unit="count", diagnostic_priority=68),
    "days_of_supply": _metric("days_of_supply", "Days of supply", "inventory", "estimated days of inventory cover at current sales velocity", better_when="context", unit="days", diagnostic_priority=72),
    "sell_through": _metric("sell_through", "Sell-through", "inventory", "how quickly inventory is selling through", better_when="context", unit="percentage", diagnostic_priority=68),
    "unfulfillable_quantity": _metric("unfulfillable_quantity", "Unfulfillable inventory", "inventory", "inventory Amazon cannot fulfil", better_when="down", unit="count", diagnostic_priority=66),
    "estimated_excess_quantity": _metric("estimated_excess_quantity", "Excess inventory", "inventory", "inventory Amazon estimates as excess", better_when="down", unit="count", diagnostic_priority=64),
}


BUSINESS_DEFAULT_METRIC_ORDER = [
    "profit",
    "net_sales",
    "gross_sales",
    "total_quantity",
    "quantity",
    "asp",
    "return_quantity",
    "refund_sales",
    "promotional_rebates",
    "total_cm2_profit",
    "cm2_profit",
    "cm2_profit_per",
    "total_cm2_margins",
    "cogs",
    "selling_fees",
    "fba_fees",
    "amazon_fees",
    "platform_fee",
    "platformfeenew",
    "platform_fee_inventory_storage",
    "lost_total",
    "ads_spend",
    "total_ads",
    "product_spend",
    "brand_spend",
    "display_spend",
    "ads_sale_amount",
    "ads_roas",
    "ads_acos",
    "tacos_total_advertising_cost_of_sale",
    "available",
    "inbound_quantity",
    "days_of_supply",
]

ANOMALY_SCAN_METRIC_ORDER = [
    "profit",
    "net_sales",
    "gross_sales",
    "total_quantity",
    "return_quantity",
    "refund_sales",
    "promotional_rebates",
    "total_cm2_profit",
    "cm2_profit",
    "cm2_profit_per",
    "platform_fee",
    "platformfeenew",
    "platform_fee_inventory_storage",
    "lost_total",
    "selling_fees",
    "fba_fees",
    "amazon_fees",
    "cogs",
    "ads_spend",
    "total_ads",
    "ads_acos",
    "tacos_total_advertising_cost_of_sale",
    "available",
    "days_of_supply",
]

DIAGNOSIS_EXPANSIONS = {
    "profit": BUSINESS_DEFAULT_METRIC_ORDER,
    "net_sales": ["net_sales", "gross_sales", "total_quantity", "quantity", "asp", "refund_sales", "return_quantity", "promotional_rebates", "available", "days_of_supply", "ads_spend"],
    "gross_sales": ["gross_sales", "net_sales", "total_quantity", "quantity", "asp", "refund_sales", "promotional_rebates"],
    "total_quantity": ["total_quantity", "quantity", "net_sales", "asp", "available", "days_of_supply", "ads_spend", "promotional_rebates"],
    "quantity": ["quantity", "total_quantity", "net_sales", "asp", "available", "days_of_supply"],
    "promotional_rebates": ["promotional_rebates", "net_sales", "quantity", "return_quantity", "total_quantity", "profit", "cm2_profit", "asp"],
    "ads_spend": ["ads_spend", "total_ads", "ads_sale_amount", "ads_sale_units", "ads_roas", "ads_acos", "tacos_total_advertising_cost_of_sale", "net_sales", "profit"],
    "total_ads": ["total_ads", "ads_spend", "product_spend", "brand_spend", "display_spend", "ads_sale_amount", "ads_roas", "ads_acos", "tacos_total_advertising_cost_of_sale", "net_sales", "profit"],
    "amazon_fees": ["amazon_fees", "selling_fees", "fba_fees", "platform_fee", "net_sales", "profit"],
    "platform_fee": ["platform_fee", "platformfeenew", "platform_fee_inventory_storage", "amazon_fees", "selling_fees", "fba_fees", "net_sales", "profit"],
    "platformfeenew": ["platformfeenew", "platform_fee", "net_sales", "profit"],
    "total_cm2_profit": ["total_cm2_profit", "cm2_profit_per", "profit", "total_ads", "ads_spend", "platform_fee", "platformfeenew", "platform_fee_inventory_storage", "lost_total"],
    "cm2_profit": ["cm2_profit", "cm2_profit_per", "profit", "total_ads", "ads_spend", "platform_fee", "platformfeenew", "platform_fee_inventory_storage", "lost_total"],
    "cm2_profit_per": ["cm2_profit_per", "total_cm2_profit", "cm2_profit", "profit", "total_ads", "ads_spend", "platform_fee", "platform_fee_inventory_storage", "lost_total"],
    "lost_total": ["lost_total", "total_cm2_profit", "cm2_profit", "platform_fee", "profit"],
    "available": ["available", "inbound_quantity", "days_of_supply", "sell_through", "total_quantity", "net_sales"],
}


def _all_supported_metrics() -> Set[str]:
    return set(available_metrics()) | DERIVED_CONTEXT_METRICS


def normalize_metric_name(metric_name: Optional[str]) -> Optional[str]:
    if not metric_name:
        return None
    key = str(metric_name).strip().lower()
    key = METRIC_NORMALIZATION.get(key, key)
    if key in _all_supported_metrics():
        return key
    return None


def sanitize_metric_list(metrics: Iterable[Any], *, allow_derived: bool = True) -> List[str]:
    supported = _all_supported_metrics() if allow_derived else set(available_metrics())
    out: List[str] = []
    for item in metrics or []:
        key = normalize_metric_name(str(item)) if item is not None else None
        if key and key in supported and key not in out:
            out.append(key)
    return out


def semantic_catalog() -> Dict[str, MetricSemantic]:
    supported = _all_supported_metrics()
    catalog: Dict[str, MetricSemantic] = {}
    for metric_name in sorted(supported):
        if metric_name in SEMANTIC_OVERRIDES:
            catalog[metric_name] = SEMANTIC_OVERRIDES[metric_name]
            continue
        label = metric_name.replace("_", " ").title()
        unit = "count" if metric_name in INVENTORY_METRICS or "quantity" in metric_name or "units" in metric_name else "money"
        queryable = metric_name in set(available_metrics())
        catalog[metric_name] = _metric(
            metric_name,
            label,
            "other",
            f"{label.lower()} from the seller's business data",
            unit=unit,
            queryable=queryable,
            diagnostic_priority=35,
        )
    return catalog


def metric_catalog_prompt_payload() -> List[Dict[str, Any]]:
    catalog = semantic_catalog()
    records = sorted(catalog.values(), key=lambda item: (-item.diagnostic_priority, item.name))
    return [record.prompt_record() for record in records]


def default_business_analysis_metrics() -> List[str]:
    return sanitize_metric_list(BUSINESS_DEFAULT_METRIC_ORDER, allow_derived=True)


def anomaly_scan_metrics() -> List[str]:
    return sanitize_metric_list(ANOMALY_SCAN_METRIC_ORDER, allow_derived=True)


def diagnostic_metric_pack(primary_metric: Optional[str], existing: Sequence[str] = ()) -> List[str]:
    primary = normalize_metric_name(primary_metric)
    base: List[str] = []
    if primary:
        base.extend(DIAGNOSIS_EXPANSIONS.get(primary, [primary, *SEMANTIC_OVERRIDES.get(primary, _metric(primary, primary, "other", primary)).related_metrics]))
    base.extend(existing or [])
    if not base:
        base = default_business_analysis_metrics()
    return sanitize_metric_list(base, allow_derived=True)


def _semantic_text(metric: MetricSemantic) -> str:
    values = [
        metric.name,
        metric.label,
        metric.category,
        metric.description,
        metric.better_when,
        " ".join(metric.related_metrics),
    ]
    return " ".join(values).lower()


def _tokens(text: str) -> Set[str]:
    words = re.findall(r"[a-z0-9]+", (text or "").lower())
    return {word for word in words if len(word) > 2}


def _fallback_metric_similarity(query: str) -> List[str]:
    query_tokens = _tokens(query)
    if not query_tokens:
        return []
    scored: List[tuple[float, str]] = []
    for metric in semantic_catalog().values():
        metric_tokens = _tokens(_semantic_text(metric))
        if not metric_tokens:
            continue
        overlap = len(query_tokens & metric_tokens)
        if overlap <= 0:
            continue
        score = overlap + (metric.diagnostic_priority / 200.0)
        scored.append((score, metric.name))
    scored.sort(reverse=True)
    return [metric for score, metric in scored if score >= 1.35][:5]


def _is_analysis_or_diagnosis(plan_hint: Dict[str, Any]) -> bool:
    return (
        plan_hint.get("reasoning_mode") in {"analysis", "decision"}
        or plan_hint.get("task_type") in {"diagnosis", "recommendation", "planning", "summary"}
        or plan_hint.get("analysis_type") in {"comparison", "growth", "trend", "summary", "diagnosis", "anomaly_scan"}
        or plan_hint.get("answer_shape") in {"comparison", "trend", "summary"}
    )


def _is_broad_hint(plan_hint: Dict[str, Any]) -> bool:
    return (
        plan_hint.get("subject_scope") == "business"
        or plan_hint.get("task_type") == "summary"
        or plan_hint.get("analysis_type") in {"summary", "anomaly_scan"}
        or (plan_hint.get("analysis_type") == "comparison" and not plan_hint.get("metric_name") and not plan_hint.get("metric_names"))
    )


def _fallback_semantic_resolution(query: str, plan_hint: Dict[str, Any]) -> SemanticResolution:
    hinted_metrics = sanitize_metric_list(
        [plan_hint.get("metric_name"), *(plan_hint.get("metric_names") or [])],
        allow_derived=True,
    )
    similar_metrics = _fallback_metric_similarity(query)
    metrics = hinted_metrics or similar_metrics

    analysis_or_diagnosis = _is_analysis_or_diagnosis(plan_hint)
    broad = _is_broad_hint(plan_hint) and not hinted_metrics
    anomaly = plan_hint.get("analysis_type") == "anomaly_scan"

    if anomaly:
        return SemanticResolution(
            primary_metric_name="profit",
            metric_names=anomaly_scan_metrics(),
            is_broad_business_analysis=True,
            needs_anomaly_scan=True,
            subject_scope="business",
            analysis_type="anomaly_scan",
            reasoning_mode="analysis",
            task_type="diagnosis",
            answer_shape="summary",
            confidence=0.75,
            reason="planner requested anomaly scan",
        )

    if broad or (analysis_or_diagnosis and not metrics):
        return SemanticResolution(
            primary_metric_name="profit",
            metric_names=default_business_analysis_metrics(),
            is_broad_business_analysis=True,
            subject_scope="business",
            analysis_type=plan_hint.get("analysis_type") or "diagnosis",
            reasoning_mode="analysis",
            task_type="diagnosis",
            answer_shape=plan_hint.get("answer_shape") or "summary",
            confidence=0.7,
            reason="broad business analysis without a single metric",
        )

    primary = metrics[0] if metrics else normalize_metric_name(plan_hint.get("metric_name"))
    expanded = diagnostic_metric_pack(primary, metrics) if analysis_or_diagnosis else sanitize_metric_list(metrics, allow_derived=True)
    return SemanticResolution(
        primary_metric_name=primary,
        metric_names=expanded,
        is_broad_business_analysis=False,
        subject_scope=plan_hint.get("subject_scope"),
        analysis_type=plan_hint.get("analysis_type"),
        reasoning_mode=plan_hint.get("reasoning_mode"),
        task_type=plan_hint.get("task_type"),
        answer_shape=plan_hint.get("answer_shape"),
        confidence=0.55 if primary else 0.0,
        reason="fallback semantic metric resolution",
    )


def _resolution_from_model(model: SemanticResolutionModel, plan_hint: Dict[str, Any]) -> SemanticResolution:
    primary = normalize_metric_name(model.primary_metric_name)
    metrics = sanitize_metric_list(model.metric_names, allow_derived=True)
    if primary and primary not in metrics:
        metrics.insert(0, primary)

    if model.needs_anomaly_scan:
        primary = primary or "profit"
        metrics = anomaly_scan_metrics()
        broad = True
    elif model.is_broad_business_analysis:
        primary = primary or "profit"
        metrics = metrics or default_business_analysis_metrics()
        broad = True
    else:
        broad = False

    if _is_analysis_or_diagnosis(
        {
            **plan_hint,
            "analysis_type": model.analysis_type or plan_hint.get("analysis_type"),
            "reasoning_mode": model.reasoning_mode or plan_hint.get("reasoning_mode"),
            "task_type": model.task_type or plan_hint.get("task_type"),
        }
    ) and primary:
        metrics = diagnostic_metric_pack(primary, metrics)

    return SemanticResolution(
        primary_metric_name=primary,
        metric_names=metrics,
        is_broad_business_analysis=broad,
        needs_anomaly_scan=bool(model.needs_anomaly_scan),
        needs_forecast_data=bool(model.needs_forecast_data),
        subject_scope=model.subject_scope or plan_hint.get("subject_scope"),
        analysis_type=model.analysis_type or plan_hint.get("analysis_type"),
        reasoning_mode=model.reasoning_mode or plan_hint.get("reasoning_mode"),
        task_type=model.task_type or plan_hint.get("task_type"),
        answer_shape=model.answer_shape or plan_hint.get("answer_shape"),
        confidence=float(model.confidence or 0.0),
        reason=model.reason,
    )


def resolve_query_semantics(
    query: str,
    *,
    llm: Any = None,
    plan_hint: Optional[Dict[str, Any]] = None,
) -> SemanticResolution:
    plan_hint = plan_hint or {}
    if llm is None:
        return _fallback_semantic_resolution(query, plan_hint)

    prompt = """
You are Phormula's semantic metric resolver for an Amazon seller finance chatbot.

Convert the user's normal-English question into business metric concepts using only this metric catalog.
Do not rely on exact user wording. Infer the business concept, then choose the safest metrics needed to answer from data.

Rules:
- If the user asks a broad business question, diagnosis, comparison, health check, "what happened", "why", or anomaly scan without one exact metric, set is_broad_business_analysis=true and include the broad business metrics needed to diagnose sales, profit, units, CM2, fees, ads, rebates, returns, and inventory.
- If the user names one metric but asks why it changed, include that primary metric plus related drivers.
- Treat profit as CM1 and label it as CM1 profit in user-facing text. Ads and platform fees are CM2 drivers, not CM1 deductions. CM2 profit must remain separate from CM1 profit.
- Treat total_quantity as net sold units after refund quantity. Treat quantity as gross units before refunds.
- Treat promotional_rebates as sign-aware: negative means discount/rebate paid out, positive means amount received back.
- Use primary_metric_name for the headline metric. Use metric_names for all evidence metrics to fetch/check.
- Prefer queryable metrics when the execution must query one metric directly, but derived metrics are allowed as evidence in business context.
- Never invent metric ids. Use only metric names from the catalog.
- If unsure, prefer broad business analysis over asking the user to name a database column.

Planner hint:
""" + json.dumps(plan_hint, default=str, ensure_ascii=True) + "\n\nMetric catalog:\n" + json.dumps(
        metric_catalog_prompt_payload(),
        ensure_ascii=True,
    ) + "\n\nUser query:\n" + (query or "")

    try:
        structured = llm.with_structured_output(SemanticResolutionModel)
        model = structured.invoke(prompt)
        resolution = _resolution_from_model(model, plan_hint)
        if not resolution.metric_names and not resolution.primary_metric_name:
            return _fallback_semantic_resolution(query, plan_hint)
        return resolution
    except Exception:
        logger.exception("[SEMANTIC_RESOLUTION] LLM resolution failed; using fallback")
        return _fallback_semantic_resolution(query, plan_hint)
