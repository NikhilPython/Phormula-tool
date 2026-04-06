from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

from flask import current_app
from flask_mail import Message

from app import mail
from app.models.user_models import User


def get_user_email(user_id: int) -> str:
    user = User.query.filter_by(id=user_id).first()
    if not user or not user.email:
        raise ValueError(f"No email found for user_id={user_id}")
    return user.email


def build_summary_html(
    *,
    user_name: str,
    title: str,
    period_label: str,
    metric_name: str,
    total: float,
    advice: List[str],
    comparison: Optional[Dict[str, Any]] = None,
    top_skus: Optional[List[Dict[str, Any]]] = None,
) -> str:
    comparison_html = ""
    if comparison:
        pct = comparison.get("pct_change")
        pct_text = "N/A" if pct is None else f"{pct:.2f}%"
        comparison_html = f"""
        <p style='margin:0 0 12px 0;'><strong>Comparison:</strong><br>
        Current: {comparison.get('current_total', 0.0):,.2f}<br>
        Previous: {comparison.get('previous_total', 0.0):,.2f}<br>
        Delta: {comparison.get('delta', 0.0):,.2f}<br>
        Change: {pct_text}</p>
        """

    sku_html = ""
    if top_skus:
        li = "".join(
            f"<li>{row.get('sku', 'N/A')}: {float(row.get('__metric__', 0.0)):,.2f}</li>"
            for row in top_skus[:10]
        )
        sku_html = f"<p style='margin:12px 0 6px 0;'><strong>Top SKUs</strong></p><ul>{li}</ul>"

    advice_html = "".join(f"<li>{item}</li>" for item in advice)

    return f"""
    <html><body style='font-family:Arial,sans-serif;color:#1f2937;'>
    <h2>{title}</h2>
    <p>Hello {user_name or 'there'},</p>
    <p>Here is your Phormula AI summary for <strong>{period_label}</strong>.</p>
    <p><strong>Metric:</strong> {metric_name}<br><strong>Total:</strong> {total:,.2f}</p>
    {comparison_html}
    {sku_html}
    <p style='margin:12px 0 6px 0;'><strong>Recommendations</strong></p>
    <ul>{advice_html}</ul>
    <p>Generated at {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC.</p>
    </body></html>
    """.strip()


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
