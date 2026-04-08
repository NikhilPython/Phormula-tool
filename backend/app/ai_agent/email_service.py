from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional
from langchain_openai import ChatOpenAI
from flask import current_app
from flask_mail import Message
from app.ai_agent.state import AgentState
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
    attachment_path: Optional[str] = None,
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

    # 🟢 ATTACHMENT SUPPORT (NEW)
    if attachment_path:
        with open(attachment_path, "rb") as f:
            msg.attach(
                filename="report.csv",
                content_type="text/csv",
                data=f.read()
            )

    mail.send(msg)

    return {
        "status": "sent",
        "recipient": to_email,
        "subject": subject
    }

def build_ai_email_summary(state: AgentState) -> str:
    from langchain_openai import ChatOpenAI

    llm = ChatOpenAI(model="gpt-4.1", temperature=0.4)

    metric = state.get("current_metrics", {})
    comparison = state.get("comparison", {})
    per_sku = metric.get("per_sku", [])

    trend_3 = state.get("trend_3", [])
    trend_6 = state.get("trend_6", [])

    country = state.get("country", "").upper()
    period = metric.get("period_label", "selected period")

    # -------------------------------
    # 🔥 FIX: USE PRIMARY METRIC CORRECTLY
    # -------------------------------
    metric_name = metric.get("metric")

    total_value = sum(float(x.get("__metric__", 0)) for x in per_sku)

    # Only populate if actually relevant
    net_sales = total_value if metric_name in ["net_sales", "sales"] else None
    profit = total_value if metric_name in ["profit"] else None
    units = total_value if metric_name in ["units", "quantity", "total_quantity"] else None

    # other metrics (already aggregated in metric node)
    advertising = float(metric.get("advertising_total", 0) or 0)
    platform_fee = float(metric.get("platform_fee", 0) or 0)
    cm2 = float(metric.get("cm2_profit", 0) or 0)
    acos = float(metric.get("acos", 0) or 0)

    # -------------------------------
    # TOP 5 SKUs
    # -------------------------------
    top_skus = sorted(
        per_sku,
        key=lambda x: float(x.get("__metric__", 0)),
        reverse=True
    )[:5]

    # -------------------------------
    # TOP 5 MOVEMENT
    # -------------------------------
    entered = []
    dropped = []

    if comparison:
        prev_skus = comparison.get("right", {}).get("per_sku", [])

        prev_top = sorted(
            prev_skus,
            key=lambda x: float(x.get("__metric__", 0)),
            reverse=True
        )[:5]

        prev_names = set(x.get("product_name") for x in prev_top)
        curr_names = set(x.get("product_name") for x in top_skus)

        entered = list(curr_names - prev_names)
        dropped = list(prev_names - curr_names)

    # -------------------------------
    # 🔥 SAFE DATA PAYLOAD
    # -------------------------------
    data_payload = {
        "period": period,
        "country": country,
        "metric_name": metric_name,
        "metric_total": total_value,
        "net_sales": net_sales,
        "profit": profit,
        "units": units,
        "cm2_profit": cm2,
        "advertising": advertising,
        "platform_fee": platform_fee,
        "acos": acos,
        "comparison": comparison,
        "top_skus": top_skus,
        "entered_top_5": entered,
        "dropped_top_5": dropped,
        "trend_3_months": trend_3,
        "trend_6_months": trend_6,
    }

    # -------------------------------
    # 🔥 STRONG + CORRECT PROMPT
    # -------------------------------
    prompt = f"""
You are a senior ecommerce business analyst.

Write a professional business summary for Amazon UK.

CRITICAL RULES:
- ALWAYS include the primary metric and its value
- If net sales / profit / units are available, include them
- NEVER say "data not available"
- All provided data is correct and MUST be used
- Combine numbers + explanation in the same sentence
- Focus on business performance FIRST, then SKU insights
- Highlight change vs previous period (if available)
- Use 3-month and 6-month trends if present
- Mention top SKUs with their actual values
- Mention if any SKU entered or dropped from top 5
- Keep it concise and executive-level
- NO recommendations

STYLE:
- Business report tone
- Short paragraphs
- Numbers must appear naturally in sentences
- Avoid generic or vague statements

DATA:
{data_payload}
"""

    response = llm.invoke([
        {"role": "system", "content": "You are a sharp ecommerce financial analyst."},
        {"role": "user", "content": prompt},
    ])

    return response.content



