from flask_mail import Message
from app import mail
import os
import smtplib
from email.message import EmailMessage
from sqlalchemy import create_engine
import re
from sqlalchemy import text
from datetime import datetime, timedelta
from sqlalchemy.exc import SQLAlchemyError
from app import db
from app.models.user_models import Email
import html


db_url = os.getenv("DATABASE_URL")

engine_hist = create_engine(db_url)




def test_send_email():
    from flask_mail import Message
    from app import mail  # Assuming you've imported mail correctly
    
    msg = Message(
        'Test Email', 
        sender=("Phormula Care Team", "care@phormula.io"),
        recipients=["test@example.com"]
    )
    msg.body = "This is a test email."
    
    try:
        mail.send(msg)
        print("Test email sent successfully.")
    except Exception as e:
        print(f"Failed to send test email: {e}")


def send_welcome_and_verification_emails(email, name, verification_link):
    try:
        welcome_msg = Message(
            "Welcome to Phormula",
            sender=("Phormula Care Team", "care@phormula.io"),
            recipients=[email]
        )

        welcome_msg.html = f"""
<!DOCTYPE html>
<html>
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">

  <style>
    @media only screen and (max-width: 600px) {{
      .email-container {{
        width: 100% !important;
        max-width: 100% !important;
      }}

      .top-report-title {{
        font-size: 14px !important;
        line-height: 18px !important;
      }}

      .content-cell {{
        padding:22px 24px 26px 24px !important;
      }}

      .note-cell {{
        padding:14px 24px 16px 24px !important;
      }}

      .cta-wrap {{
        text-align:center !important;
      }}

      .cta-button {{
        display:inline-block !important;
        margin:0 auto !important;
        text-align:center !important;
      }}
    }}
  </style>
</head>

<body style="margin:0; padding:0; font-family:Arial, Helvetica, sans-serif;">
  <table width="100%" cellpadding="0" cellspacing="0" border="0" style="padding:16px 0;">
    <tr>
      <td align="center">

        <table class="email-container" width="600" cellpadding="0" cellspacing="0" border="0" style="
          background:#ffffff;
          width:600px;
          max-width:600px;
          border-collapse:collapse;
        ">

          <!-- top green bar -->
          <tr>
            <td style="background:#5ea68e; padding:18px 24px; color:#ffffff;">
              <table width="100%" cellpadding="0" cellspacing="0" border="0" style="table-layout:fixed; border-collapse:collapse;">
                <tr>
                  <td width="80" style="
                    font-size:28px;
                    line-height:28px;
                    font-weight:300;
                    color:#ffffff;
                    text-align:left;
                    vertical-align:middle;
                    white-space:nowrap;
                  ">
                    |p|
                  </td>

                  <td width="472" align="right" class="top-report-title" style="
                    font-size:16px;
                    line-height:18px;
                    color:#f8edce;
                    text-align:right;
                    vertical-align:middle;
                    white-space:nowrap;
                  ">
                    Account Verification
                  </td>
                </tr>
              </table>
            </td>
          </tr>

          <!-- logo/title -->
          <tr>
            <td align="center" style="
              padding:28px 30px 18px 30px;
              background:#ffffff;
              border-left:1px solid #e4e7ec;
              border-right:1px solid #e4e7ec;
            ">
              <div style="
                font-size:36px;
                color:#1d6d84;
                line-height:1.2;
                margin-bottom:8px;
                font-weight:300;
              ">
                |phormula|
              </div>

              <div style="font-size:18px; color:#4a4a4a; line-height:1.4;">
                Welcome to Phormula
              </div>
            </td>
          </tr>

          <!-- divider -->
          <tr>
            <td style="border-top:1px solid #dddddd; font-size:1px; line-height:1px;">&nbsp;</td>
          </tr>

          <!-- body -->
          <tr>
            <td class="content-cell" style="
              padding:22px 32px 26px 32px;
              color:#444444;
              font-size:14px;
              line-height:1.7;
              text-align:left;
              border-left:1px solid #e4e7ec;
              border-right:1px solid #e4e7ec;
            ">
              <p style="margin:0 0 18px 0; text-align:left;">
                Hey <strong>{name}</strong>,
              </p>

              <p style="margin:0 0 14px 0; text-align:justify; text-justify:inter-word;">
                Welcome to <strong>Phormula</strong>, a platform built for modern D2C brands.
                We’re delighted to have you on board.
              </p>

              <p style="margin:0 0 18px 0; text-align:justify; text-justify:inter-word;">
                To begin your journey and securely access the Phormula experience,
                please verify your email address by clicking the button below.
              </p>

              <!-- info box -->
              <table width="100%" cellpadding="0" cellspacing="0" border="0" style="
                margin:20px 0 22px 0;
                background:#eef7f3;
                border:1px solid #cfe9dc;
                border-collapse:collapse;
              ">
                <tr>
                  <td style="padding:16px 18px;">
                    <div style="font-size:14px; font-weight:bold; color:#37455f; margin-bottom:10px;">
                      What happens after verification?
                    </div>

                    <ul style="
                      margin:0;
                      padding-left:18px;
                      color:#444444;
                      font-size:14px;
                      line-height:1.8;
                      text-align:left;
                    ">
                      <li>Secure access to your Phormula account</li>
                      <li>Business insights designed for modern D2C brands</li>
                      <li>Tools to help you scale, optimize, and grow confidently</li>
                    </ul>
                  </td>
                </tr>
              </table>

              <!-- CTA -->
              <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="
                width:100%;
                margin:26px 0 24px 0;
                border-collapse:collapse;
              ">
                <tr>
                  <td align="center" class="cta-wrap" style="
                    text-align:center !important;
                    padding:0;
                    margin:0;
                  ">
                    <a href="{verification_link}" class="cta-button" style="
                      display:inline-block;
                      background:#37455f;
                      color:#f8edce;
                      padding:12px 30px;
                      text-decoration:none;
                      font-size:14px;
                      font-weight:bold;
                      border-radius:10px;
                      text-align:center;
                      line-height:20px;
                      margin:0 auto;
                    ">
                      Activate My Account
                    </a>
                  </td>
                </tr>
              </table>

              <p style="margin:0 0 14px 0; text-align:justify; text-justify:inter-word;">
                Once confirmed, you’ll unlock powerful tools and insights designed to help you
                scale, optimize, and grow your brand with confidence.
              </p>

              <p style="margin:0 0 16px 0; color:#777777; text-align:justify; text-justify:inter-word;">
                If you did not create a Phormula account, you may safely ignore this email.
              </p>

              <p style="margin:0 0 18px 0; color:#777777; text-align:justify; text-justify:inter-word;">
                For any questions or assistance, our support team is available at
                <a href="mailto:care@phormula.io" style="color:#37455f; text-decoration:none;">
                  care@phormula.io
                </a>.
              </p>

              <p style="margin:18px 0 0 0; text-align:left;">
                Warm regards,
              </p>

              <p style="margin:0; text-align:left;">
                <strong>The Phormula Team</strong>
              </p>

              <p style="margin:0; text-align:left;">
                <a href="mailto:care@phormula.io" style="color:#37455f; text-decoration:none;">
                  care@phormula.io
                </a>
              </p>
            </td>
          </tr>

          <!-- full-width note section -->
          <tr>
            <td class="note-cell" style="
              border-top:1px solid #dddddd;
              padding:14px 32px 16px 32px;
              background:#ffffff;
              font-size:12px;
              color:#999999;
              line-height:1.6;
              text-align:left;
              border-left:1px solid #e4e7ec;
              border-right:1px solid #e4e7ec;
            ">
              This email was generated automatically by Phormula.
            </td>
          </tr>

          <!-- footer -->
          <tr>
            <td align="center" style="
              background:#5ea68e;
              padding:12px 18px;
              color:#f8edce;
              font-size:12px;
              line-height:1.5;
              text-align:center;
            ">
              © 2026 Phormula. All rights reserved.
            </td>
          </tr>

        </table>

      </td>
    </tr>
  </table>
</body>
</html>
"""

        if not welcome_msg.html:
            print("Error: HTML content is empty")
            return

        mail.send(welcome_msg)

    except Exception as e:
        print(f"Failed to send email to {email}: {e}")
        raise e

def send_reset_email(to_email, reset_url):
    msg = Message(
        'Password Reset Request',
        sender='care@phormula.io',
        recipients=[to_email]
    )

    # HTML email body
    html_body = f"""
    <html>
    <body style="font-family: 'Lato', Arial, sans-serif; background-color: #f4f4f4; padding: 20px; margin: 0;">
    <div style="max-width: 600px; margin: 0 auto; background-color: #fff; padding: 30px; border-radius: 8px; border: 2px solid#5EA68E; box-shadow: 0 0 20px rgba(0, 0, 0, 0.1);">
        <img src="https://i.postimg.cc/43T3k86Z/logo.png" alt="Phormula Logo" style="width: 200px; height: auto; display: block; margin: 0 auto 20px;" />
        <p style="font-size: 14px; line-height: 1.6; color: #555;"> Dear {to_email},</p>
        <p style="font-size: 14px; line-height: 1.6; color: #555;">We have received a request to reset your password. To proceed, please click the button below:</p>        
        <a href="{reset_url}" style="display: inline-block; background-color: #37455F; color: #f8edcf; padding: 8px 20px; text-align: center; text-decoration: none; font-size: 14px; border-radius: 8px; box-shadow: 4px 4px 10px rgba(0, 0, 0, 0.2); transition: background-color 0.3s ease; cursor: pointer;">Reset Your Password</a>        
        <p style="font-size: 14px; color: #777;">If you did not request this change, please disregard this email.</p>
        <p style="font-size: 14px; color: #555;">If you need assistance, feel free to contact our support team at <a href="mailto:care@phormula.io" style="color: #007bff;">care@phormula.io</a>.</p>
        <p style="font-size: 14px; color: #555;">Best regards, <br>The Phormula Team</p>
        </div>
    </body>
    </html>
    """

    msg.html = html_body
    mail.send(msg)

def metric_box(label, value, is_negative=False, bg="#ffffff"):
    """Render one KPI box. Sign/color is controlled by is_negative."""
    color = "#d32f2f" if is_negative else "#2e7d32"
    sign = "-" if is_negative else "+"
    return f"""
    <tr>
      <td style="padding:0 0 10px 0;">
        <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0"
               style="width:100%; background:{bg}; border-radius:8px; border:1px solid #e5e7eb;">
          <tr>
            <td style="padding:10px; text-align:left;">
              <div style="font-size:11px; color:#666; margin-bottom:4px;">{html.escape(str(label))}</div>
              <div style="font-size:18px; font-weight:bold; color:{color};">
                {sign}{abs(value):.2f}%
              </div>
            </td>
          </tr>
        </table>
      </td>
    </tr>
    """

def _metric_is_negative(description: str, keyword_pattern: str, extracted_value: float | None) -> bool:
    """Infer negativity from (a) explicit negative numeric value, else (b) nearby words like decrease/dip/down."""
    if extracted_value is not None:
        try:
            if float(extracted_value) < 0:
                return True
        except Exception:
            pass

    if not description:
        return False

    neg_words = r"(decrease|decreased|dip|dipped|down|fall|falling|fell|decline|declined|drop|dropped|reduced|reduction)"
    patt = re.compile(rf"({keyword_pattern}).{{0,50}}{neg_words}|{neg_words}.{{0,50}}({keyword_pattern})", re.IGNORECASE)
    return bool(patt.search(description))




def render_sku_card(sku, idx=0):
    
    is_gray = idx % 2 == 0
    card_bg = "#F3F4F6" if is_gray else "#FFFFFF"
    inner_bg = "#FFFFFF" if is_gray else "#F3F4F6"
    
    negatives = sku.get("negatives") or {}
    asp_neg = bool(negatives.get("ASP", False))
    units_neg = bool(negatives.get("Units", False))
    mix_neg = bool(negatives.get("Sales Mix", False))
    profit_neg = bool(negatives.get("Profit", False))

    return f"""
    <div style="
      border:1px solid #e5e7eb;
      border-radius:12px;
      padding:16px;
      margin-bottom:20px;
      background:{card_bg};
    ">
      <div style="font-size:15px; font-weight:600; margin-bottom:10px;">
        {sku['product']}
      </div>

     <table width="100%" cellpadding="0" cellspacing="0" border="0">
  {metric_box("ASP Change", sku["metrics"]["ASP"], asp_neg, inner_bg)}
  {metric_box("Units", sku["metrics"]["Units"], units_neg, inner_bg)}
  {metric_box("Sales", sku["metrics"]["Sales"], negatives.get("Sales", False), inner_bg)}
  {metric_box("Sales Mix", sku["metrics"]["Sales Mix"], mix_neg, inner_bg)}
  {metric_box("Profit", sku["metrics"]["Profit"], profit_neg, inner_bg)}
</table>

      <p style="font-size:13px; color:#555; line-height:1.6; margin-top:12px;">
        {sku["description"]}
      </p>

      <div style="
        margin-top:12px;
        padding:12px;
        background:#fdecc8;
        border-left:4px solid #f59e0b;
        font-size:13px;
        font-weight:500;
        border-radius:6px;
      ">
        <strong>Action</strong><br/>{sku["action"]}
      </div>
    </div>
    """

GENERIC_ACTION_PATTERNS = [
    "monitor performance",
    "monitor closely",
    "maintain current asp and",
    "reduce asp slightly",
    "improve traction",
    "if your objective is",
]



def _extract_pct(text: str, pattern: str):
    """Return float percentage extracted using regex pattern, else None."""
    m = re.search(pattern, text, flags=re.IGNORECASE)
    if not m:
        return None
    try:
        return float(m.group(1))
    except Exception:
        return None
    
ALLOWED_PRIMARY_ACTIONS = [
    "Check ads and visibility campaigns for this product.",
    "Review the visibility setup for this product.",
    "Reduce ASP slightly to improve traction.",
    "Increase ASP slightly to strengthen margins.",
    "Maintain current ASP and monitor performance.",
    "Monitor performance closely for now.",
    "Check Amazon fees or taxes for this product as profit is down despite growth.",
]


def parse_action_bullet_to_card(bullet: str) -> dict | None:
    if not bullet or not str(bullet).strip():
        return None

    lines = [l.rstrip() for l in str(bullet).splitlines()]

    # trim empty lines
    while lines and not lines[0].strip():
        lines.pop(0)
    while lines and not lines[-1].strip():
        lines.pop()

    if not lines:
        return None

    # ---------- Product ----------
    product = lines[0]
    if product.lower().startswith("product name"):
        parts = product.split("-", 1)
        if len(parts) == 2:
            product = parts[1].strip()

    # ---------- Inventory action (last line) ----------
    inventory_action = ""
    for l in lines:
        if l.lower().startswith("inventory:"):
            inventory_action = l.strip()
            
     # ---------- Primary & Secondary action (AI driven) ----------
    primary_action = ""
    secondary_action = ""

    for l in lines:
            clean = l.strip()

            # PRIMARY action: must exactly match AI allowed sentences
            if clean in ALLOWED_PRIMARY_ACTIONS:
                primary_action = clean

            # SECONDARY strategy (rank-first)
            elif clean.lower().startswith("if your objective"):
                secondary_action = clean       

    # ---------- Buckets ----------
    
    pricing_action = ""
    rank_action = ""
    generic_actions = []
    description_lines = []

    for l in lines[1:]:
        low = l.lower()

        if "increase asp" in low:
            pricing_action = "Increase ASP slightly to strengthen margins."

        elif "boost rank" in low or "current pricing setup" in low:
            rank_action = l.strip()

        clean = l.strip()


        if (
            clean == inventory_action
            or clean == primary_action
            or clean == secondary_action
        ):
            continue

        description_lines.append(clean)

        raw_description = " ".join(description_lines).strip()

            # ---- SENTENCE LEVEL SPLIT ----
    sentences = re.split(r'(?<=[.!?])\s+', raw_description)

    clean_sentences = []
    pricing_action = ""
    rank_action = ""

    for s in sentences:
        low = s.lower()

        if any(p in low for p in GENERIC_ACTION_PATTERNS) and "by" not in low:
            generic_actions.append(s.strip())

        elif "increase asp" in low:
            pricing_action = "Increase ASP slightly to strengthen margins."

        elif "boost rank" in low or "current pricing setup" in low:
            rank_action = s.strip()

        else:
            clean_sentences.append(s.strip())



    # Final cleaned description (NO ACTION LINES)
    description = " ".join(clean_sentences).strip()


    # ---------- FINAL ACTION ORDER ----------
    actions = []

    if primary_action:
        actions.append(primary_action)

    if secondary_action:
        actions.append(secondary_action)

    if inventory_action:
        actions.append(inventory_action)

    action = "<br/>".join(f"{i+1}. {a}" for i, a in enumerate(actions))


    # ---------- Metrics extraction ----------
    mix_val = _extract_pct(description, r"sales\s*mix[^%]*?by\s*([+-]?\d+(?:\.\d+)?)%")
    asp_val = _extract_pct(description, r"\bASP\b[^%]*?by\s*([+-]?\d+(?:\.\d+)?)%")
    units_val = _extract_pct(description, r"\bunits?\b[^%]*?by\s*([+-]?\d+(?:\.\d+)?)%")
    profit_val = _extract_pct(description, r"\bprofit(?!\s*margin)\b[^%]*?by\s*([+-]?\d+(?:\.\d+)?)%")
    sales_val = _extract_pct(description, r"\bsales\b[^%]*?by\s*([+-]?\d+(?:\.\d+)?)%")
    
    asp_negative = False

    # Case 1: explicit numeric negative (safety)
    if asp_val is not None and asp_val < 0:
        asp_negative = True

    # Case 2: ASP-specific language ONLY
    elif re.search(
        r"(decrease|decreased|decline|declined|drop|dropped|down|fell|reduced|reduction).{0,40}\basp\b"
        r"|\basp\b.{0,40}(decrease|decreased|decline|declined|drop|dropped|down|fell|reduced|reduction)",
        description,
        re.I,
    ):
        asp_negative = True

    negatives = {
        "ASP": asp_negative,
        "Units": (
    False
    if re.search(r"(growth|increase|increased|up).{0,30}units|units.{0,30}(growth|increase|increased|up)", description, re.I)
    else _metric_is_negative(description, r"units?", units_val)
),
        "Sales": _metric_is_negative(description, r"sales", sales_val),
        "Sales Mix": (
    False
    if re.search(r"(up|increase|increased|growth).{0,30}sales\s*mix|sales\s*mix.{0,30}(up|increase|increased|growth)", description, re.I)
    else _metric_is_negative(description, r"sales\s*mix", mix_val)
),
        "Profit": _metric_is_negative(description, r"profit", profit_val),
    }

    def _abs_or_zero(x):
        return abs(x) if x is not None else 0.0

    metrics = {
        "ASP": (-abs(asp_val) if asp_negative else (asp_val or 0.0)),
        "Units": _abs_or_zero(units_val),
        "Sales": _abs_or_zero(sales_val),
        "Sales Mix": _abs_or_zero(mix_val),
        "Profit": _abs_or_zero(profit_val),
    }

    return {
        "product": product,
        "metrics": metrics,
        "negatives": negatives,
        "description": description,
        "action": action,
    }



def parse_actions_to_cards(actions: list) -> list:
    """Convert list[str] action bullets to list[dict] cards, skipping failures."""
    cards = []
    for a in (actions or []):
        c = parse_action_bullet_to_card(a)
        if c:
            cards.append(c)
    return cards



def send_live_bi_email(
    to_email,
    overall_summary,
    country,
    prev_label,
    curr_label,
    deep_link_token=None,
    overall_actions=None,
    sku_actions=None,
    sku_to_product=None,
    portfolio_recommendation=None,
):
    import traceback
    import html
    import re

    if not to_email:
        print("[WARN] No email provided.")
        return

    subject = f"[Phormula] Live MTD Business Insights - {str(country).upper()} ({curr_label})"

    # ---------------------------
    # Overall summary
    # ---------------------------
    summary_text = (overall_summary or {}).get("summary_text", "") or ""

    summary_html = f"""
<p style="
    font-size:14px;
    color:#555;
    line-height:1.8;
    margin:0;
    text-align:justify;
    text-justify:inter-word;
">
    {html.escape(str(summary_text))}
</p>
"""

    portfolio_html = ""
    if portfolio_recommendation:
        if isinstance(portfolio_recommendation, (list, tuple)):
            portfolio_body = "".join(
                f'<div style="margin-bottom:6px;">• {html.escape(str(item))}</div>'
                for item in portfolio_recommendation
                if str(item).strip()
            )
        elif isinstance(portfolio_recommendation, dict):
            portfolio_body = "".join(
                f'<div style="margin-bottom:6px;"><strong>{html.escape(str(k))}:</strong> {html.escape(str(v))}</div>'
                for k, v in portfolio_recommendation.items()
                if str(v).strip()
            )
        else:
            portfolio_body = html.escape(str(portfolio_recommendation))

        portfolio_html = f"""
        <div style="
            margin-top:14px;
            background:#F8FAFC;
            border:1px solid #E4E7EC;
            border-radius:10px;
            padding:14px 16px;
        ">
          <div style="font-size:16px; font-weight:700; color:#37455F; margin-bottom:6px;">
            Portfolio recommendation
          </div>
          <div style="font-size:14px; color:#555; line-height:1.7;">
            {portfolio_body}
          </div>
        </div>
        """

    # ---------------------------
    # Helpers
    # ---------------------------
    def safe_product_name(sku: str) -> str:
        if not sku:
            return "Unknown"
        if sku_to_product and isinstance(sku_to_product, dict):
            return sku_to_product.get(sku) or sku
        return sku

    def _strip_html_tags(text: str) -> str:
        if not text:
            return ""

        text = str(text)
        text = re.sub(r"<br\s*/?>", "\n", text, flags=re.I)
        text = re.sub(r"</p\s*>", "\n", text, flags=re.I)
        text = re.sub(r"</div\s*>", "\n", text, flags=re.I)
        text = re.sub(r"<[^>]+>", " ", text)
        text = html.unescape(text)
        text = re.sub(r"[ \t]+", " ", text)
        text = re.sub(r"\n\s+", "\n", text)
        text = re.sub(r"\n{2,}", "\n", text)
        return text.strip()

    def _clean_section_text(value: str) -> str:
        if not value:
            return ""

        value = re.sub(r"\s+", " ", str(value)).strip()

        # Remove accidental bullets at the beginning/end
        value = value.strip("•").strip()
        value = re.sub(r"^\s*[-•]\s*", "", value).strip()
        value = re.sub(r"\s*[-•]\s*$", "", value).strip()

        return value

    def _fallback_section_html() -> str:
        fallback_text = portfolio_recommendation or "No SKU-wise actions available for this run."
        return f"""
        <div style="
            font-size:14px;
            color:#555;
            line-height:1.7;
            background:#FFF8E7;
            border:1px solid #F5D48A;
            border-radius:10px;
            padding:14px 16px;
            margin-bottom:18px;
        ">
            {html.escape(str(fallback_text))}
        </div>
        """

    def parse_sku_action_text(action_text: str) -> dict:
        raw = _strip_html_tags(action_text)

        if not raw:
            return {
                "product_name": "",
                "metrics": [],
                "journey_points": [],
                "recommendation": "",
                "advertising": "",
                "inventory": "",
            }

        text = re.sub(r"\s+", " ", raw).strip()

        # -------- Product name --------
        product_name = ""
        m = re.match(
            r"^(.*?)(?=\s+ASP:|\s+Units:|\s+Net sales:|\s+CM1 profit:|\s+CM1 profit per unit:|\s+CM1 Profit / Unit:)",
            text,
            flags=re.I,
        )
        if m:
            product_name = m.group(1).strip(" :-")

        # -------- Metrics --------
        # IMPORTANT:
        # Each metric now stops before Product Journey, Recommendation, Advertising, or Inventory.
        # This prevents CM1 Profit / Unit from swallowing "Recommendation: Monitor performance".
        metric_patterns = [
            (
                "ASP",
                r"ASP:\s*(.*?)(?=\s+Units:|\s+Net sales:|\s+CM1 profit:|\s+CM1 profit per unit:|\s+CM1 Profit / Unit:|\s+Product Journey:|\s+Performance Journey:|\s+Recommendation:|\s+Advertising:|\s+Inventory:|$)",
            ),
            (
                "Units",
                r"Units:\s*(.*?)(?=\s+Net sales:|\s+CM1 profit:|\s+CM1 profit per unit:|\s+CM1 Profit / Unit:|\s+Product Journey:|\s+Performance Journey:|\s+Recommendation:|\s+Advertising:|\s+Inventory:|$)",
            ),
            (
                "Net sales",
                r"Net sales:\s*(.*?)(?=\s+CM1 profit:|\s+CM1 profit per unit:|\s+CM1 Profit / Unit:|\s+Product Journey:|\s+Performance Journey:|\s+Recommendation:|\s+Advertising:|\s+Inventory:|$)",
            ),
            (
                "CM1 profit",
                r"CM1 profit:\s*(.*?)(?=\s+CM1 profit per unit:|\s+CM1 Profit / Unit:|\s+Product Journey:|\s+Performance Journey:|\s+Recommendation:|\s+Advertising:|\s+Inventory:|$)",
            ),
            (
                "CM1 profit per unit",
                r"(?:CM1 profit per unit|CM1 Profit / Unit):\s*(.*?)(?=\s+Product Journey:|\s+Performance Journey:|\s+Recommendation:|\s+Advertising:|\s+Inventory:|$)",
            ),
        ]

        metrics = []
        for label, patt in metric_patterns:
            mm = re.search(patt, text, flags=re.I)
            if mm:
                val = _clean_section_text(mm.group(1))
                if val:
                    metrics.append((label, val))

        # -------- Product / Performance Journey --------
        journey_text = ""
        jm = re.search(
            r"(?:Product Journey|Performance Journey):\s*(.*?)(?=\s+Recommendation:|\s+Advertising:|\s+Inventory(?:\s+action)?:|$)",
            text,
            flags=re.I,
        )
        if jm:
            journey_text = jm.group(1).strip()

        journey_points = []
        if journey_text:
            # Supports " - point", "• point", and long plain text.
            parts = re.split(r"\s+(?:-+|•)\s+", journey_text)
            for p in parts:
                p = _clean_section_text(p)
                if p:
                    journey_points.append(p)

        # -------- Recommendation --------
        recommendation = ""
        rm = re.search(
            r"Recommendation:\s*(.*?)(?=\s+Advertising:|\s+Inventory(?:\s+action)?:|$)",
            text,
            flags=re.I,
        )
        if rm:
            recommendation = _clean_section_text(rm.group(1))

        # -------- Advertising --------
        advertising = ""
        am = re.search(
            r"Advertising:\s*(.*?)(?=\s+Inventory(?:\s+action)?:|$)",
            text,
            flags=re.I,
        )
        if am:
            advertising = _clean_section_text(am.group(1))

        # -------- Inventory --------
        inventory = ""
        im = re.search(
            r"Inventory(?:\s+action)?:\s*(.*)$",
            text,
            flags=re.I,
        )
        if im:
            inventory = _clean_section_text(im.group(1))

        return {
            "product_name": product_name,
            "metrics": metrics,
            "journey_points": journey_points,
            "recommendation": recommendation,
            "advertising": advertising,
            "inventory": inventory,
        }
    
    def _format_currency_sign(value: str) -> str:
      if not value:
          return ""

      value = str(value).strip()

      # Convert £-1,001.33 -> -£1,001.33
      value = re.sub(r"^£\s*-\s*", "-£", value)

      # Convert $-1,001.33 -> -$1,001.33
      value = re.sub(r"^\$\s*-\s*", "-$", value)

      # Convert €-1,001.33 -> -€1,001.33
      value = re.sub(r"^€\s*-\s*", "-€", value)

      return value

    def _metric_chip(label: str, value_text: str) -> str:
        txt = _clean_section_text(value_text)

        # Split value and delta.
        # Example: "£7.63 (-4.05%)" -> main_value="£7.63", delta_value="(-4.05%)"
        m = re.match(r"^(.*?)(\s*\([^)]+\))?$", txt)
        main_value = (m.group(1) or "").strip() if m else txt
        delta_value = (m.group(2) or "").strip() if m else ""
        main_value = _format_currency_sign(main_value)

        is_negative = bool(re.search(r"-\s*\d", delta_value))
        is_positive = bool(re.search(r"\+\s*\d|\(\s*\d", delta_value))

        if is_negative:
            delta_color = "#E53935"  # red
        elif is_positive:
            delta_color = "#56A38D"  # green
        else:
            delta_color = "#667085"  # neutral gray

        label_display_map = {
            "Units": "Units",
            "Net sales": "Net Sales",
            "ASP": "ASP",
            "CM1 profit": "CM1 Profit",
            "CM1 profit per unit": "CM1 Profit per Unit",
        }
        display_label = label_display_map.get(label, label)

        border_map = {
            "Units": "#E9B949",
            "Net sales": "#73B8D8",
            "ASP": "#C65B5B",
            "CM1 profit": "#7BA05B",
            "CM1 profit per unit": "#C78B52",
        }
        border_color = border_map.get(label, "#D0D5DD")

        return f"""
<div class="metric-cell" style="
    display:block;
    width:100%;
    margin:0 0 12px 0;
    padding:0;
    box-sizing:border-box;
    vertical-align:top;
">
  <div class="metric-card" style="
    width:100%;
    min-height:108px;
    background:#FFFFFF;
    border:1px solid {border_color};
    border-top: 4px solid {border_color};
    border-radius:10px;
    box-sizing:border-box;
    padding:12px 16px 10px 16px;
    text-align:left;
">
  <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="
      width:100%;
      height:74px;
      border-collapse:collapse;
  ">
    <tr>
      <td style="
          vertical-align:top;
          height:20px;
          padding:0;
      ">
        <p class="metric-label" style="
            display:block;
            width:100%;
            max-width:100%;
            font-size:14px;
            line-height:1.2;
            color:#18324A;
            font-weight:500;
            white-space:normal;
            overflow:visible;
            text-overflow:clip;
            margin:0;
            text-align:left;
        ">
          {html.escape(display_label)}
        </p>
      </td>
    </tr>

    <tr>
      <td style="
          vertical-align:bottom;
          padding:0;
      ">
        <p class="metric-value" style="
            display:block;
            width:100%;
            max-width:100%;
            font-size:20px;
            line-height:1.15;
            font-weight:700;
            color:#2F3A4A;
            white-space:nowrap;
            overflow:hidden;
            text-overflow:ellipsis;
            margin:0 0 4px 0;
            text-align:left;
        ">
          {html.escape(main_value)}
        </p>

        <p class="metric-delta" style="
            display:block;
            width:100%;
            max-width:100%;
            font-size:14px;
            line-height:1.15;
            font-weight:700;
            color:{delta_color} !important;
            white-space:nowrap;
            overflow:hidden;
            text-overflow:ellipsis;
            margin:0;
            text-align:left;
        ">
          {html.escape(delta_value)}
        </p>
      </td>
    </tr>
  </table>
</div>
          </div>
        """

    def render_simple_action_card(sku: str, action_text: str) -> str:
        parsed = parse_sku_action_text(action_text)

        product = parsed["product_name"] or safe_product_name(sku) or "Unknown"
        product = html.escape(str(product))

        metric_cells = [_metric_chip(label, value) for label, value in parsed["metrics"]]

        metrics_html = f"""
        <div class="metrics-table" style="
            width:100%;
            margin:0 0 22px 0;
            display:block;
            box-sizing:border-box;
        ">
          <div class="metrics-row" style="
    width:100%;
    display:block;
    box-sizing:border-box;
">
            {''.join(metric_cells)}
          </div>
        </div>
        """

        journey_html = ""
        if parsed["journey_points"]:
            bullet_rows = "".join(
                f"""
                <p style="
                    font-size:14px;
                    color:#475467;
                    line-height:1.75;
                    padding:0 0 8px 0;
                    margin:0;
                ">
                  <span style="color:#3B82F6; font-weight:700;">•</span>
                  {html.escape(point)}
                </p>
                """
                for point in parsed["journey_points"]
            )

            journey_html = f"""
            <div style="font-size:16px; font-weight:700; color:#37455F; margin:14px 0 8px 0;">
              Performance Journey
            </div>
            <div style="width:100%;">
              {bullet_rows}
            </div>
            """

        def _point_block(title: str, body: str) -> str:
            if not body:
                return ""

            clean_body = _clean_section_text(body)
            if not clean_body:
                return ""

            return f"""
            <div style="margin-top:12px;">
              <div style="font-size:16px; font-weight:700; color:#37455F; margin-bottom:6px;">
                {html.escape(title)}
              </div>
              <p style="
                  font-size:14px;
                  color:#475467;
                  line-height:1.7;
                  padding:0 0 4px 0;
                  margin:0;
              ">
                • {html.escape(clean_body)}
              </p>
            </div>
            """

        bottom_cards_html = f"""
        {_point_block("Recommendation", parsed["recommendation"])}
        {_point_block("Advertising", parsed["advertising"])}
        {_point_block("Inventory", parsed["inventory"])}
        """

        fallback_html = ""
        if (
            not parsed["metrics"]
            and not parsed["journey_points"]
            and not parsed["recommendation"]
            and not parsed["advertising"]
            and not parsed["inventory"]
        ):
            fallback_html = f"""
            <div style="margin-top:10px; font-size:14px; line-height:1.6; color:#475467;">
              {html.escape(_strip_html_tags(action_text))}
            </div>
            """

        return f"""
        <div style="
            width:100%;
            margin:0 0 18px 0;
            background:#FFFFFF;
            border:1px solid #E4E7EC;
            border-radius:14px;
            box-sizing:border-box;
        ">
          <div class="sku-card-td" style="padding:16px 16px 14px 16px; box-sizing:border-box;">

            <div class="sku-product-title" style="
                font-size:18px;
                font-weight:700;
                color:#37455f;
                padding-bottom:10px;
                text-align:center;
            ">
              {product}
            </div>

            {metrics_html}
            {journey_html}
            {bottom_cards_html}
            {fallback_html}

          </div>
        </div>
        """

    # ---------------------------
    # SKU ACTIONS SECTION
    # ---------------------------
    sku_section_html = ""

    try:
        if sku_actions and isinstance(sku_actions, dict):
            cards = []
            for sku_key, action_text in sku_actions.items():
                if not sku_key or not action_text:
                    continue
                cards.append(render_simple_action_card(sku_key, action_text))

            sku_section_html = "".join(cards) if cards else _fallback_section_html()

        elif overall_actions:
            parsed_cards = parse_actions_to_cards(overall_actions)
            if parsed_cards:
                rendered = []
                for idx, card in enumerate(parsed_cards):
                    if isinstance(card, dict):
                        card.setdefault("sku", card.get("sku") or card.get("product") or "Unknown")
                        card.setdefault("product", card.get("product") or card.get("product_name") or card.get("sku") or "Unknown")
                        card.setdefault("metrics", card.get("metrics") or {})
                        card.setdefault("action_text", card.get("action_text") or card.get("action") or "")

                    try:
                        rendered.append(render_sku_card(card, idx))
                    except Exception as e:
                        print(f"[WARN] render_sku_card failed idx={idx}: {e}")
                        traceback.print_exc()
                        sku_fallback = card.get("sku") if isinstance(card, dict) else "Unknown"
                        action_fallback = card.get("action_text") if isinstance(card, dict) else str(card)
                        rendered.append(render_simple_action_card(sku_fallback, action_fallback))

                sku_section_html = "".join(rendered)

            else:
                if isinstance(overall_actions, (list, tuple)):
                    clean_items = [str(a).strip() for a in overall_actions if str(a).strip()]
                    if clean_items:
                        sku_section_html = f"""
                        <div style="
                            font-size:14px;
                            color:#555;
                            line-height:1.8;
                            background:#FFF8E7;
                            border:1px solid #F5D48A;
                            border-radius:10px;
                            padding:14px 16px;
                            margin-bottom:18px;
                        ">
                          <ul style="margin:0; padding-left:18px;">
                            {''.join(f"<li>{html.escape(a)}</li>" for a in clean_items)}
                          </ul>
                        </div>
                        """
                    else:
                        sku_section_html = _fallback_section_html()
                else:
                    sku_section_html = _fallback_section_html()
        else:
            sku_section_html = _fallback_section_html()

    except Exception as e:
        print("[WARN] Failed building SKU section:", e)
        traceback.print_exc()
        sku_section_html = _fallback_section_html()

    # ---------------------------
    # DEEP LINK
    # ---------------------------
    deep_link_html = ""
    if deep_link_token:
        month_map = {
            "jan": "january",
            "feb": "february",
            "mar": "march",
            "apr": "april",
            "may": "may",
            "jun": "june",
            "jul": "july",
            "aug": "august",
            "sep": "september",
            "oct": "october",
            "nov": "november",
            "dec": "december",
        }

        country_slug = str(country).lower().strip()

        curr_label_str = str(curr_label).lower().strip()
        month_key = curr_label_str[:3]
        current_month = month_map.get(month_key, month_key)

        year_match = re.search(r"'?(\d{2,4})", curr_label_str)
        current_year = ""
        if year_match:
            year_value = year_match.group(1)
            current_year = f"20{year_value}" if len(year_value) == 2 else year_value

        dashboard_url = f"http://localhost:3000/live-dashboard/{country_slug}/{current_month}/{current_year}#ai-insights"

        deep_link_html = f"""
        <p style="text-align:center; margin-top:24px;">
          <a href="{dashboard_url}"
            style="display:inline-block; background:#37455F; color:#f8edcf;
                    padding:10px 24px; text-decoration:none; border-radius:8px;
                    font-size:14px;">
            Open Live BI Dashboard
          </a>
        </p>
        """

    # ---------------------------
    # EMAIL BODY
    # ---------------------------
    html_body = f"""
    <html>
    <head>
      <meta charset="UTF-8">
      <meta name="viewport" content="width=device-width, initial-scale=1.0">
      <link href="https://fonts.googleapis.com/css2?family=Lato:wght@400;700;800&display=swap" rel="stylesheet">

      <style>
        body {{
          margin: 0 !important;
          padding: 0 !important;
          width: 100% !important;
          -webkit-text-size-adjust: 100%;
          -ms-text-size-adjust: 100%;
        }}

        table {{
          border-collapse: collapse;
        }}

        img {{
          max-width: 100%;
          height: auto;
        }}

        .email-wrapper {{
          width: 100% !important;
          max-width: none !important;
          margin: 0 auto !important;
        }}

        .main-content {{
          padding: 26px 18px 24px 18px !important;
          border-left: 1px solid #E4E7EC !important;
          border-right: 1px solid #E4E7EC !important;
        }}

        .brand-title {{
          font-size: 44px !important;
          line-height: 1.05 !important;
          word-break: break-word !important;
        }}

        .report-title {{
          font-size: 18px !important;
          line-height: 1.35 !important;
        }}

        .period-pill {{
  display: block !important;
  width: 100% !important;
  max-width: 450px !important;
  margin-left: auto !important;
  margin-right: auto !important;
}}

.period-pill-table {{
  width: 100% !important;
}}

.period-country-cell {{
  width: 28% !important;
}}

.period-date-cell {{
  width: 72% !important;
  white-space: normal !important;
  word-break: break-word !important;
  overflow-wrap: break-word !important;
}}

        /* Mobile/default: stacked metric cards */
.metrics-table {{
  width: 100% !important;
  display: block !important;
}}

.metrics-row {{
  width: 100% !important;
  display: block !important;
  box-sizing: border-box !important;
}}

.metric-cell {{
  display: block !important;
  width: 100% !important;
  max-width: 100% !important;
  margin: 0 0 12px 0 !important;
  padding: 0 !important;
  box-sizing: border-box !important;
}}

.metric-card {{
  width: 100% !important;
  height: auto !important;
  min-height: auto !important;
  box-sizing: border-box !important;
}}

        .metric-card-td,
        .metric-label,
        .metric-value,
        .metric-delta {{
          text-align: center !important;
        }}

        .metric-label,
        .metric-value,
        .metric-delta {{
          white-space: normal !important;
          overflow: visible !important;
          text-overflow: clip !important;
        }}

        .recommendations-content li,
        .recommendations-content .recommendation-point,
        .recommendations-content .sub-point {{
          font-size: 14px !important;
          line-height: 1.7 !important;
          color: #18324A !important;
        }}

        .recommendations-content p:not(.metric-label):not(.metric-value):not(.metric-delta) {{
          font-size: 14px !important;
          line-height: 1.7 !important;
          color: #18324A !important;
        }}

        .recommendations-content ul,
        .recommendations-content ol {{
          margin-top: 8px !important;
          margin-bottom: 14px !important;
          padding-left: 18px !important;
        }}

        .recommendations-content h3,
        .recommendations-content h4 {{
          font-size: 18px !important;
          line-height: 1.35 !important;
          margin: 18px 0 10px 0 !important;
          color: #14213D !important;
        }}

        @media only screen and (max-width: 767px) {{
  .period-pill {{
    max-width: 100% !important;
  }}

  .period-country-cell {{
    width: 30% !important;
    padding: 14px 10px !important;
    font-size: 16px !important;
  }}

  .period-date-cell {{
    width: 70% !important;
    padding: 14px 10px !important;
    font-size: 15px !important;
    line-height: 1.35 !important;
    white-space: normal !important;
    word-break: break-word !important;
    overflow-wrap: break-word !important;
  }}
}}

.sku-product-title {{
  text-align: center !important;
}}

        /* Desktop/laptop */
        @media only screen and (min-width: 768px) {{
          .email-wrapper {{
            width: 100% !important;
            max-width: 80% !important;
            margin: 0 auto !important;
          }}

            .sku-product-title {{
              text-align: left !important;
            }}

            .period-pill {{
              max-width: 380px !important;
            }}

            .period-country-cell {{
    width: 26% !important;
    padding: 14px 16px !important;
  }}

  .period-date-cell {{
    width: 74% !important;
    padding: 14px 14px !important;
    font-size: 15px !important;
  }}

          .outer-padding {{
            padding: 0px !important;
            text-align: center !important;
          }}

          .main-content {{
            padding: 34px 32px 28px 32px !important;
          }}

          .brand-title {{
            font-size: 60px !important;
          }}

          .report-title {{
            font-size: 20px !important;
          }}

.metrics-table {{
  display: block !important;
  width: 100% !important;
}}

  .metrics-row {{
    width: 100% !important;
    display: table !important;
    table-layout: fixed !important;
    border-collapse: separate !important;
    border-spacing: 12px 0 !important; /* gap between cards */
  }}

  .metric-cell {{
    display: table-cell !important;
    width: 20% !important; /* 5 equal cards in one row */
    vertical-align: top !important;
    margin: 0 !important;
    padding: 0 !important;
  }}

.metric-card {{
  width: 100% !important;
  min-height: 108px !important;
  height: 108px !important;
  box-sizing: border-box !important;
  padding: 12px 16px 10px 16px !important;
}}

  .metric-label {{
    font-size: 13px !important;
    line-height: 1.2 !important;
    margin: 0 0 10px 0 !important;
    text-align: left !important;
    white-space: normal !important;
    overflow: visible !important;
    text-overflow: clip !important;
  }}

  .metric-value {{
    font-size: 22px !important;
    line-height: 1.2 !important;
    margin: 0 0 8px 0 !important;
    text-align: left !important;
    white-space: nowrap !important;
    overflow: hidden !important;
    text-overflow: ellipsis !important;
  }}

  .metric-delta {{
    font-size: 12px !important;
    line-height: 1.15 !important;
    margin: 0 !important;
    text-align: left !important;
    white-space: nowrap !important;
    overflow: hidden !important;
    text-overflow: ellipsis !important;
  }}

}}
      </style>
    </head>

    <body style="font-family:'Lato', Arial, sans-serif; background:#FFFFFF; padding:0; margin:0;">
      <div class="outer-padding" style="padding:0;">
        <div class="email-wrapper" style="
          width:100%;
          margin:0 auto;
          background:#fff;
          border:none;
          box-sizing:border-box;
          overflow:hidden;
        ">

          <!-- Top green header -->
          <div class="top-header" style="background:#7FB5A5; padding:18px 24px;">
            <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0">
              <tr>
                <td style="vertical-align:middle; text-align:left;">
                  <div style="font-size:28px; font-weight:300; color:#FFFFFF; letter-spacing:1px;">
                    |p|
                  </div>
                </td>
                <td style="vertical-align:middle; text-align:right;">
                  <div style="font-size:16px; color:#F8EDCF; font-weight:500;">
                    Business Insights Report
                  </div>
                </td>
              </tr>
            </table>
          </div>

          <!-- Main content -->
          <div class="main-content" style="
            padding:34px 32px 28px 32px;
            text-align:center;
            border-left:1px solid #E4E7EC;
            border-right:1px solid #E4E7EC;
          ">

            <!-- Brand -->
            <div class="brand-title" style="font-size:60px; line-height:1; font-weight:300; color:#2F6476; margin-bottom:8px;">
              |phormula|
            </div>

            <div class="report-title" style="font-size:18px; color:#414042; font-weight:500; margin-bottom:24px;">
              Live MTD vs Previous Period – Business Insights
            </div>

            <!-- Combined pill -->
<div class="period-pill" style="
  display:block;
  width:100%;
  max-width:450px;
  background:#FFFFFF;
  border:1px solid #e4e7ec;
  border-radius:12px;
  overflow:hidden;
  margin:0 auto 30px auto;
  text-align:center;
  box-sizing:border-box;
">
  <table class="period-pill-table" role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="
    width:100%;
    border-collapse:separate;
  ">
    <tr class="period-pill-row">
      <td class="period-country-cell" style="
        padding:16px 22px;
        font-size:18px;
        font-weight:700;
        color:#5EA68E;
        line-height:1.2;
        white-space:nowrap;
        vertical-align:middle;
        text-align:center;
        width:28%;
        border-right:1px solid #e4e7ec;
      ">
        {html.escape(str(country).upper())}
      </td>

      <td class="period-date-cell" style="
        padding:16px 18px;
        font-size:16px;
        font-weight:700;
        color:#414042;
        line-height:1.35;
        white-space:normal;
        word-break:break-word;
        overflow-wrap:break-word;
        vertical-align:middle;
        text-align:center;
        width:72%;
      ">
        {html.escape(str(prev_label))} vs {html.escape(str(curr_label))}
      </td>
    </tr>
  </table>
</div>

            <hr style="margin:14px 0 26px 0; border:none; border-top:1px solid #D0D5DD;" />

            <!-- Summary section -->
            <div style="text-align:left;">
              <h3 style="color:#37455F; margin:0 0 12px 0; font-size:20px;">
                Overall Summary
              </h3>

              <div style="
                font-size:14px;
                color:#555;
                margin-bottom:8px;
                text-align:justify;
                text-justify:inter-word;
                line-height:1.8;
              ">
                {summary_html}
                {portfolio_html}
              </div>
            </div>

            <!-- Recommendation section -->
            <div style="text-align:left; margin-top:28px;">
              <h3 style="color:#37455F; margin:0 0 14px 0; font-size:20px;">
                Recommendations
              </h3>

              <div class="recommendations-content" style="
                font-size:14px;
                line-height:1.7;
                color:#18324A;
              ">
                {sku_section_html}
              </div>
            </div>

            {deep_link_html}

            <div style="text-align:left; margin-top:24px;">
              <p style="font-size:12px; color:#999; margin:0 0 6px 0;">
                This email was auto-generated from Live BI.
              </p>
              <p style="font-size:12px; color:#999; margin:0;">
                Support: <a href="mailto:care@phormula.io" style="color:#37455F;">care@phormula.io</a>
              </p>
            </div>
          </div>

          <!-- Bottom green footer strip -->
         <!-- Footer -->
          <div style="
            background:#7FB5A5;
            padding:12px 18px;
            text-align:center;
            color:#F8EDCF;
            font-size:12px;
            font-family:'Lato', Arial, sans-serif;
          ">
            © 2026 Phormula. All rights reserved.
          </div>
        </div>
      </div>
    </body>
  </html>
    """

    msg = Message(
        subject,
        sender=("Phormula Care Team", "care@phormula.io"),
        recipients=[to_email],
    )
    msg.html = html_body

    try:
        mail.send(msg)
        print(f"[INFO] Live BI email sent to {to_email}")
    except Exception as e:
        print(f"[ERROR] Email send failed: {e}")
        traceback.print_exc()



def has_recent_bi_email(user_id: int, country: str, hours: int = 24) -> bool:
    """
    Returns True if an email was sent within last `hours` for this user+country.
    Uses ORM model Email (table: email).
    """
    try:
        cutoff = datetime.utcnow() - timedelta(hours=hours)

        rec = (
            db.session.query(Email)
            .filter(
                Email.user_id == user_id,
                Email.country == country.lower(),
                Email.sent_at >= cutoff
            )
            .first()
        )
        return rec is not None

    except SQLAlchemyError as e:
        # Fail-safe: if DB has an issue, do NOT spam.
        print(f"[WARN] has_recent_bi_email DB error (fail-safe=True): {e}")
        return True


def mark_bi_email_sent(user_id: int, country: str) -> None:
    """
    Upsert (one row per user+country).
    Updates sent_at if exists else inserts.
    """
    try:
        country = country.lower()

        rec = (
            db.session.query(Email)
            .filter_by(user_id=user_id, country=country)
            .first()
        )

        if rec:
            rec.sent_at = datetime.utcnow()
        else:
            rec = Email(user_id=user_id, country=country)
            db.session.add(rec)

        db.session.commit()

    except SQLAlchemyError as e:
        db.session.rollback()
        print(f"[WARN] mark_bi_email_sent DB error: {e}")




def get_user_email_by_id(user_id: int) -> str | None:
    """
    Fetch email from public.user table.
    Uses double quotes because 'user' is a reserved keyword.
    """
    try:
        query = text("""
            SELECT email
            FROM "user"
            WHERE id = :uid
            LIMIT 1
        """)
        with engine_hist.connect() as conn:
            row = conn.execute(query, {"uid": user_id}).fetchone()

        if not row:
            print(f"[WARN] No user found with id={user_id}")
            return None

        # row may be tuple or Row
        return row[0] if isinstance(row, tuple) else row.email

    except Exception as e:
        print(f"[ERROR] Failed to fetch user email for id={user_id}: {e}")
        return None

# def send_email_with_attachment(
#     *,
#     to_email: str,
#     subject: str,
#     body: str,
#     attachment_bytes: bytes,
#     attachment_filename: str,
#     mime_type: str = "application/octet-stream",
# ):
#     msg = EmailMessage()
#     msg["Subject"] = subject
#     msg["From"] = f'{os.getenv("MAIL_DEFAULT_SENDER_NAME", "Phormula Care")} <{os.getenv("MAIL_USERNAME")}>'
#     msg["To"] = to_email

#     # ✅ Plain fallback
#     msg.set_content(body)

#     html_body = f"""
# <html>
# <body style="margin:0; padding:0; background:#f4f6f8; font-family:Arial, sans-serif;">
#   <div style="padding:32px 16px;">
#     <div style="
#       max-width:640px;
#       margin:0 auto;
#       background:#ffffff;
#       border:1px solid #e5e7eb;
#       border-radius:16px;
#       overflow:hidden;
#       box-shadow:0 8px 24px rgba(15,23,42,0.06);
#     ">

#       <!-- Top bar -->
#       <div style="background:linear-gradient(135deg, #37455F 0%, #2F6476 100%); padding:24px 28px;">
#         <div style="font-size:24px; font-weight:700; color:#ffffff; letter-spacing:0.2px;">
#           Phormula
#         </div>
#         <div style="font-size:13px; color:#dbe4ea; margin-top:6px;">
#           Amazon SKU Performance Report
#         </div>
#       </div>

#       <!-- Main content -->
#       <div style="padding:32px 28px 28px 28px;">

#         <p style="font-size:15px; color:#1f2937; margin:0 0 16px 0;">Hi,</p>

#         <p style="font-size:15px; color:#4b5563; line-height:1.7; margin:0 0 18px 0;">
#           Your <strong>Amazon SKU-wise monthly report</strong> is ready and attached to this email.
#         </p>

#         <!-- Attachment card -->
#         <div style="
#           background:#f8fafc;
#           border:1px solid #e5e7eb;
#           border-radius:12px;
#           padding:14px 16px;
#           margin:20px 0;
#         ">
#           <div style="font-size:13px; color:#6b7280; margin-bottom:6px;">
#             Attached file
#           </div>
#           <div style="font-size:16px; color:#111827; font-weight:700;">
#             {attachment_filename}
#           </div>
#         </div>

#         <!-- Value section -->
#         <div style="
#           background:#eef7f3;
#           border:1px solid #cfe9dc;
#           border-radius:12px;
#           padding:16px 18px;
#           margin:20px 0 22px 0;
#         ">
#           <div style="font-size:14px; font-weight:700; color:#1f2937; margin-bottom:10px;">
#             What’s inside this report
#           </div>
#           <ul style="margin:0; padding-left:18px; color:#4b5563; font-size:14px; line-height:1.8;">
#             <li>SKU-level sales and net sales performance</li>
#             <li>Profitability metrics including ASP and margin view</li>
#             <li>Refunds, returns, fees, and deductions</li>
#             <li>A clean monthly view to support faster decision-making</li>
#           </ul>
#         </div>

#         <p style="font-size:14px; color:#4b5563; line-height:1.7; margin:0 0 22px 0;">
#           For deeper analysis, including trends, profitability breakdowns, and business insights,
#           open your dashboard in Phormula.
#         </p>

#         <!-- CTA -->
#         <div style="text-align:center; margin:28px 0 24px 0;">
#           <a href="https://phormula.io"
#              style="
#                display:inline-block;
#                background:#37455F;
#                color:#ffffff;
#                padding:13px 26px;
#                text-decoration:none;
#                border-radius:10px;
#                font-size:14px;
#                font-weight:700;
#              ">
#             Open Phormula Dashboard
#           </a>
#         </div>

#         <!-- Footer note -->
#         <div style="
#           border-top:1px solid #e5e7eb;
#           padding-top:18px;
#           margin-top:8px;
#         ">
#           <p style="font-size:12px; color:#9ca3af; line-height:1.7; margin:0 0 8px 0;">
#             This email was generated automatically by Phormula.
#           </p>
#           <p style="font-size:13px; color:#6b7280; margin:0 0 14px 0;">
#             Need help? Contact us at
#             <a href="mailto:care@phormula.io" style="color:#37455F; text-decoration:none;">care@phormula.io</a>
#           </p>
#           <p style="font-size:13px; color:#6b7280; margin:0;">
#             Regards,<br/>
#             <strong>Phormula Team</strong>
#           </p>
#         </div>

#       </div>
#     </div>
#   </div>
# </body>
# </html>
# """

#     msg.add_alternative(html_body, subtype="html")

#     # Attachment
#     maintype, subtype = mime_type.split("/", 1)
#     msg.add_attachment(
#         attachment_bytes,
#         maintype=maintype,
#         subtype=subtype,
#         filename=attachment_filename,
#     )

#     # SMTP send
#     smtp_host = os.getenv("MAIL_SERVER")
#     smtp_port = int(os.getenv("MAIL_PORT", "587"))
#     smtp_user = os.getenv("MAIL_USERNAME")
#     smtp_pass = os.getenv("MAIL_PASSWORD")
#     use_tls = os.getenv("MAIL_USE_TLS", "true").lower() == "true"

#     with smtplib.SMTP(smtp_host, smtp_port) as server:
#         if use_tls:
#             server.starttls()
#         if smtp_user and smtp_pass:
#             server.login(smtp_user, smtp_pass)
#         server.send_message(msg)


def send_email_with_attachment(
    *,
    to_email: str,
    subject: str,
    body: str,
    attachment_bytes: bytes,
    attachment_filename: str,
    mime_type: str = "application/octet-stream",
):
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = f'{os.getenv("MAIL_DEFAULT_SENDER_NAME", "Phormula Care")} <{os.getenv("MAIL_USERNAME")}>'
    msg["To"] = to_email

    # Plain fallback
    msg.set_content(body)

    safe_attachment_filename = html.escape(str(attachment_filename))

    html_body = f"""
<html>
<head>
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <style>
    @media only screen and (max-width: 600px) {{
    .email-container {{
      width: 100% !important;
      max-width: 100% !important;
    }}

      .top-report-title {{
        font-size: 14px !important;
        line-height: 18px !important;
      }}

      .cta-wrap {{
        text-align: center !important;
      }}

      .cta-button {{
        display: inline-block !important;
        margin: 0 auto !important;
        text-align: center !important;
      }}
    }}
  </style>
</head>
<body style="margin:0; padding:0; font-family:Arial, Helvetica, sans-serif;">
  <table width="100%" cellpadding="0" cellspacing="0" border="0" style="padding:16px 0">
    <tr>
      <td align="center">

        <table class="email-container" width="600" cellpadding="0" cellspacing="0" border="0" style="
          background:#ffffff;
          width:600px;
          max-width:600px;
          border-collapse:collapse;
        ">

          <!-- top green bar -->
          <tr>
            <td style="background:#5ea68e; padding:18px 24px; color:#ffffff;">
              <table width="100%" cellpadding="0" cellspacing="0" border="0" style="table-layout:fixed; border-collapse:collapse;">
                <tr>
                  <td width="80" style="
                    font-size:28px;
                    line-height:28px;
                    font-weight:300;
                    color:#ffffff;
                    text-align:left;
                    vertical-align:middle;
                    white-space:nowrap;
                  ">
                    |p|
                  </td>

                <td width="472" align="right" class="top-report-title" style="
                  font-size:16px;
                  line-height:18px;
                  color:#f8edce;
                  text-align:right;
                  vertical-align:middle;
                  white-space:nowrap;
                ">
                  Amazon SKU Performance Report
                </td>
                </tr>
              </table>
            </td>
          </tr>

          <!-- logo/title -->
          <tr>
            <td align="center" style="padding:28px 30px 18px 30px; background:#ffffff; border-left:1px solid #e4e7ec; border-right:1px solid #e4e7ec;">
              <div style="font-size:36px; color:#1d6d84; line-height:1.2; margin-bottom:8px; font-weight:300;">
                |phormula|
              </div>

              <div style="font-size:18px; color:#4a4a4a; line-height:1.4;">
                Your Amazon SKU-wise Monthly Report is ready
              </div>
            </td>
          </tr>

          <!-- divider -->
          <tr>
            <td style="border-top:1px solid #dddddd; font-size:1px; line-height:1px;">&nbsp;</td>
          </tr>

          <!-- body -->
          <tr>
            <td style="
              padding:22px 32px 26px 32px;
              color:#444444;
              font-size:14px;
              line-height:1.7;
              text-align:justify;
              text-justify:inter-word;
              border-left:1px solid #e4e7ec; border-right:1px solid #e4e7ec;
            ">
              <p style="margin:0 0 18px 0; text-align:left;">
                Hi,
              </p>

              <p style="margin:0 0 14px 0; text-align:justify; text-justify:inter-word;">
                Your <strong>Amazon SKU-wise monthly report</strong> is ready and attached to this email.
                This report gives you a clean SKU-level view of your monthly performance, helping you review
                sales, deductions, profitability, and operational trends more efficiently.
              </p>

              <p style="margin:0 0 18px 0; text-align:justify; text-justify:inter-word;">
                Please review the attached file at your convenience and use it to track product-level performance,
                identify movement across key metrics, and support faster business decisions.
              </p>

              <!-- attachment box -->
              <table width="100%" cellpadding="0" cellspacing="0" border="0" style="
                margin:20px 0;
                background:#f8fafc;
                border:1px solid #e5e7eb;
                border-collapse:collapse;
              ">
                <tr>
                  <td style="padding:14px 16px;">
                    <div style="font-size:12px; color:#777777; margin-bottom:6px;">
                      Attached file
                    </div>

                    <div style="
                      font-size:15px;
                      line-height:1.4;
                      color:#1d6d84;
                      font-weight:bold;
                      word-break:break-word;
                    ">
                      {safe_attachment_filename}
                    </div>
                  </td>
                </tr>
              </table>

              <!-- report details -->
              <table width="100%" cellpadding="0" cellspacing="0" border="0" style="
                margin:20px 0 22px 0;
                background:#eef7f3;
                border:1px solid #cfe9dc;
                border-collapse:collapse;
              ">
                <tr>
                  <td style="padding:16px 18px;">
                    <div style="font-size:14px; font-weight:bold; color:#37455f; margin-bottom:10px;">
                      What’s inside this report
                    </div>

                    <ul style="margin:0; padding-left:18px; color:#444444; font-size:14px; line-height:1.8;">
                      <li>SKU-level sales and net sales performance</li>
                      <li>ASP, profitability, and margin visibility</li>
                      <li>Refunds, returns, fees, and deductions</li>
                      <li>Monthly performance view for faster decision-making</li>
                    </ul>
                  </td>
                </tr>
              </table>

              <p style="margin:0 0 14px 0; text-align:justify; text-justify:inter-word;">
                For deeper analysis, including trends, profitability breakdowns, and business insights,
                you can open your Phormula dashboard.
              </p>

              <!-- CTA -->
                <table width="100%" cellpadding="0" cellspacing="0" border="0" style="margin:26px 0 24px 0; border-collapse:collapse;">
                  <tr>
                    <td align="center" class="cta-wrap" style="text-align:center;">
                      <table cellpadding="0" cellspacing="0" border="0" align="center" style="margin:0 auto; border-collapse:collapse;">
                        <tr>
                          <td align="center" style="text-align:center;">
                            <a href="https://phormula.io" class="cta-button" style="
                              display:inline-block;
                              background:#37455f;
                              color:#f8edce;
                              padding:12px 24px;
                              text-decoration:none;
                              font-size:14px;
                              font-weight:bold;
                              border-radius:10px;
                              text-align:center;
                              margin:0 auto;
                            ">
                              Open Phormula Dashboard
                            </a>
                          </td>
                        </tr>
                      </table>
                    </td>
                  </tr>
                </table>

              <p style="margin:18px 0 0 0; text-align:left;">
                Warm regards,
              </p>

              <p style="margin:0; text-align:left;">
                <strong>The Phormula Team</strong>
              </p>

              <p style="margin:0; text-align:left;">
                <a href="mailto:care@phormula.io" style="color:#37455f; text-decoration:none;">
                  care@phormula.io
                </a>
              </p>
            </td>
          </tr>

          <!-- full-width note section -->
          <tr>
            <td style="
              border-top:1px solid #dddddd;
              padding:14px 32px 16px 32px;
              background:#ffffff;
              font-size:12px;
              color:#999999;
              line-height:1.6;
              text-align:left;
              border-left:1px solid #e4e7ec; border-right:1px solid #e4e7ec;
            ">
              This email was generated automatically by Phormula.
            </td>
          </tr>

          <!-- footer -->
          <tr>
            <td align="center" style="
              background:#5ea68e;
              padding:12px 18px;
              color:#f8edce;
              font-size:12px;
              line-height:1.5;
              text-align:center;
            ">
              © 2026 Phormula. All rights reserved.
            </td>
          </tr>

        </table>

      </td>
    </tr>
  </table>
</body>
</html>
"""

    msg.add_alternative(html_body, subtype="html")

    # Attachment
    maintype, subtype = mime_type.split("/", 1)
    msg.add_attachment(
        attachment_bytes,
        maintype=maintype,
        subtype=subtype,
        filename=attachment_filename,
    )

    # SMTP send
    smtp_host = os.getenv("MAIL_SERVER")
    smtp_port = int(os.getenv("MAIL_PORT", "587"))
    smtp_user = os.getenv("MAIL_USERNAME")
    smtp_pass = os.getenv("MAIL_PASSWORD")
    use_tls = os.getenv("MAIL_USE_TLS", "true").lower() == "true"

    with smtplib.SMTP(smtp_host, smtp_port) as server:
        if use_tls:
            server.starttls()
        if smtp_user and smtp_pass:
            server.login(smtp_user, smtp_pass)
        server.send_message(msg)