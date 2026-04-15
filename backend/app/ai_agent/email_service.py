
from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Optional

from flask import current_app
from flask_mail import Message

from app import mail
from app.models.user_models import User


def send_agent_email(
    *,
    user_id: int,
    subject: str,
    html_body: str,
    recipient: Optional[str] = None,
) -> Dict[str, Any]:
    user = User.query.filter_by(id=user_id).first()
    to_email = recipient or (user.email if user else None)
    if not to_email:
        raise ValueError("Recipient email not available")

    msg = Message(
        subject=subject,
        sender=current_app.config.get("MAIL_DEFAULT_SENDER"),
        recipients=[to_email],
    )
    msg.html = html_body
    mail.send(msg)
    return {"status": "sent", "recipient": to_email, "subject": subject}


def build_email_html(state: Dict[str, Any]) -> str:
    metric = state.get("current_metrics") or {}
    comparison = state.get("comparison") or {}
    analysis = state.get("analysis_result") or {}
    advice = state.get("advice") or []
    event_plan = state.get("event_plan_result") or {}
    sku_intel = state.get("sku_intelligence_result") or {}

    parts = ["<html><body style='font-family:Arial,sans-serif;color:#1f2937'>"]
    parts.append("<h2>Phormula AI Summary</h2>")

    if event_plan:
        parts.append("<p><strong>Event plan generated</strong></p>")
        summary = event_plan.get("summary") or []
        if summary:
            items = "".join(f"<li>{x}</li>" for x in summary[:12])
            parts.append(f"<ul>{items}</ul>")
        actions = event_plan.get("actions") or []
        if actions:
            items = "".join(f"<li>{x}</li>" for x in actions[:12])
            parts.append(f"<p><strong>Actions</strong></p><ul>{items}</ul>")
        parts.append(f"<p>Generated at {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC.</p></body></html>")
        return "".join(parts)

    if sku_intel:
        product = sku_intel.get("product_match") or "selected product"
        parts.append(f"<p><strong>SKU intelligence for:</strong> {product}</p>")
        current = sku_intel.get("current", {})
        previous = sku_intel.get("previous", {})
        parts.append(
            "<p>"
            f"Current sales: {float(current.get('net_sales', 0.0)):,.2f}<br>"
            f"Current profit: {float(current.get('profit', 0.0)):,.2f}<br>"
            f"Current units: {float(current.get('total_quantity', 0.0)):,.2f}<br>"
            f"Current ASP: {float(current.get('asp', 0.0)):,.2f}"
            "</p>"
        )
        if previous:
            parts.append(
                "<p><strong>Previous comparable period</strong><br>"
                f"Sales: {float(previous.get('net_sales', 0.0)):,.2f}<br>"
                f"Profit: {float(previous.get('profit', 0.0)):,.2f}<br>"
                f"Units: {float(previous.get('total_quantity', 0.0)):,.2f}<br>"
                f"ASP: {float(previous.get('asp', 0.0)):,.2f}</p>"
            )
        deltas = sku_intel.get("deltas") or {}
        if deltas:
            items = "".join(f"<li>{k}: {float(v):,.2f}</li>" for k, v in deltas.items())
            parts.append(f"<p><strong>Key changes</strong></p><ul>{items}</ul>")
        summary = sku_intel.get("summary_points") or []
        if summary:
            items = "".join(f"<li>{x}</li>" for x in summary[:12])
            parts.append(f"<p><strong>Summary</strong></p><ul>{items}</ul>")

    period_label = metric.get("period_label") or "selected period"
    metric_name = metric.get("metric") or state.get("metric_name") or "metric"
    total = metric.get("total")

    if not sku_intel:
        parts.append(f"<p><strong>Period:</strong> {period_label}</p>")
        if total is not None:
            parts.append(f"<p><strong>{metric_name.replace('_', ' ').title()}:</strong> {float(total):,.2f}</p>")

    if comparison:
        left = comparison.get("left", {})
        right = comparison.get("right", {})
        pct = comparison.get("pct_change")
        pct_text = "N/A" if pct is None else f"{pct:.2f}%"
        parts.append(
            "<p><strong>Comparison</strong><br>"
            f"Current: {float(left.get('total', comparison.get('current', 0.0))):,.2f}<br>"
            f"Previous: {float(right.get('total', comparison.get('previous', 0.0))):,.2f}<br>"
            f"Delta: {float(comparison.get('delta', 0.0)):,.2f}<br>"
            f"Change: {pct_text}</p>"
        )

    if analysis.get("type") == "trend":
        rows = analysis.get("series", [])[:12]
        items = "".join(f"<li>{r.get('period_label')}: {float(r.get('__metric__', 0.0)):,.2f}</li>" for r in rows)
        parts.append(f"<p><strong>Trend</strong></p><ul>{items}</ul>")
    elif analysis.get("type") == "breakdown":
        rows = analysis.get("per_sku", [])[:10]
        items = "".join(
            f"<li>{r.get('product_name') or r.get('sku') or 'Unknown'}: {float(r.get('__metric__', 0.0)):,.2f}</li>"
            for r in rows
        )
        parts.append(f"<p><strong>Top products</strong></p><ul>{items}</ul>")
    elif analysis.get("type") == "summary":
        metrics = analysis.get("metrics", {})
        items = "".join(f"<li>{k}: {float(v):,.2f}</li>" for k, v in metrics.items())
        parts.append(f"<p><strong>Business summary</strong></p><ul>{items}</ul>")

    if advice:
        items = "".join(f"<li>{a}</li>" for a in advice)
        parts.append(f"<p><strong>Recommendations</strong></p><ul>{items}</ul>")

    parts.append(f"<p>Generated at {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC.</p></body></html>")
    return "".join(parts)
