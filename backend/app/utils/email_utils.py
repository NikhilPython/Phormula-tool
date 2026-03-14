from flask_mail import Message
from app import mail
import os
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
            'Welcome to Phormula', 
            sender=("Phormula Care Team", "care@phormula.io"),
            recipients=[email]
        )
        # welcome_msg.sender = ("Phormula Care Team", "care@phormula.io")
        # http://localhost:3000/Logo_Phormula.png
        welcome_msg.html = f"""
<!DOCTYPE html>
<html>
<head>
  <meta charset="UTF-8">
</head>

<body style="
  margin:0;
  padding:0;
 background: #d1d5db; /* dark outer bg */
  font-family: Lato;
">

  <!-- Outer wrapper -->
  <div style="
    padding:40px 16px;
  ">

    <!-- Card -->
    <div style="
      max-width:520px;
      margin:0 auto;
      background:#ffffff;
      border-radius:12px;
      padding:32px;
      text-align:left;
      border:1px solid #e5e7eb;
      box-shadow:0 10px 25px rgba(0,0,0,0.15);
    ">

      <!-- Logo -->
      <div style="
        font-size:22px;
        font-weight:600;
        margin-bottom:28px;
        color:#37455F;
        text-align:center;
      ">
        Phormula
      </div>

      <p style="font-size:14px; color:#333; margin:0 0 14px;">
        Hey <strong>{name}</strong>,
      </p>

      <p style="font-size:14px; color:#333; margin:0 0 14px;">
        Welcome to <strong>Phormula</strong>, a platform built for modern D2C brands!
      </p>

      <p style="font-size:14px; color:#555; line-height:1.6; margin:0 0 18px;">
        We’re delighted to have you on board.
      </p>

      <p style="font-size:14px; color:#555; line-height:1.6; margin:0 0 18px;">
        To begin your journey and securely access the Phormula experience,
        please verify your email address by clicking the button below:
      </p>

      <!-- Centered Button -->
      <div style="text-align:center; margin:28px 0;">
        <a href="{verification_link}"
           style="
             display:inline-block;
             background:#37455F;
             color:#f8edce;
             padding:12px 30px;
             font-size:14px;
             font-weight:600;
             text-decoration:none;
             border-radius:8px;
           ">
          Activate My Account
        </a>
      </div>

      <p style="font-size:14px; color:#555; line-height:1.6; margin:0 0 16px;">
        Once confirmed, you’ll unlock powerful tools and insights designed to help
        you scale, optimize, and grow your brand with confidence.
      </p>

      <p style="font-size:13px; color:#777; margin:0 0 16px;">
        If you did not create a Phormula account, you may safely ignore this email.
      </p>

      <p style="font-size:13px; color:#777; margin:0 0 24px;">
        For any questions or assistance, our support team is available at
        <a href="mailto:care@phormula.io"
           style="color:#37455F; text-decoration:none;">
          care@phormula.io
        </a>
      </p>

      <p style="font-size:13px; color:#777; margin:0;">
        Warm regards,<br>
        The Phormula Team
      </p>

    </div>
  </div>
</body>
</html>
"""


        
        # Ensure content is non-empty before sending
        if not welcome_msg.html:
            print("Error: HTML content is empty")
            return  # Exit if content is empty

        # Send the welcome email
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
    <td style="padding:10px; background:{bg}; border-radius:6px; text-align:center;">
    
    
      <div style="font-size:11px; color:#666;">{label}</div>
      <div style="font-size:16px; font-weight:bold; color:{color};">
        {sign}{abs(value):.2f}%
      </div>
    </td>
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

      <table width="100%" cellspacing="8">
       <tr>
  {metric_box("ASP Change", sku["metrics"]["ASP"], asp_neg, inner_bg)}
{metric_box("Units", sku["metrics"]["Units"], units_neg, inner_bg)}
{metric_box("Sales", sku["metrics"]["Sales"], sku["negatives"].get("Sales", False), inner_bg)}
{metric_box("Sales Mix", sku["metrics"]["Sales Mix"], mix_neg, inner_bg)}
{metric_box("Profit", sku["metrics"]["Profit"], profit_neg, inner_bg)}
</tr>
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
    overall_actions=None,     # dict or bullets, depending on your pipeline
    sku_actions=None,         # dict: { sku -> rendered_text } (text/html)
    sku_to_product=None,      # optional: dict {sku -> product_name}
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
    <p style="font-size:14px; color:#555; line-height:1.8; margin:0;">
        {html.escape(str(summary_text))}
    </p>
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
        text = re.sub(r"<br\s*/?>", "\n", str(text), flags=re.I)
        text = re.sub(r"</p\s*>", "\n", text, flags=re.I)
        text = re.sub(r"</div\s*>", "\n", text, flags=re.I)
        text = re.sub(r"<[^>]+>", " ", text)
        text = html.unescape(text)
        text = re.sub(r"[ \t]+", " ", text)
        text = re.sub(r"\n\s+", "\n", text)
        text = re.sub(r"\n{2,}", "\n", text)
        return text.strip()

    def _fallback_section_html() -> str:
        fallback_text = portfolio_recommendation or "No SKU-wise actions available for this run."
        return f"""
        <div style="
            font-size:13px;
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
        # example: "Classic ASP: £5.06 (+42.65%) Units: ..."
        product_name = ""
        m = re.match(r"^(.*?)(?=\s+ASP:)", text, flags=re.I)
        if m:
            product_name = m.group(1).strip(" :-")

        # -------- Metrics --------
        metrics = []
        metric_patterns = [
            ("ASP", r"ASP:\s*(.*?)(?=\s+Units:|\s+Net sales:|\s+CM1 profit:|\s+CM1 profit per unit:|\s+Product Journey:|$)"),
            ("Units", r"Units:\s*(.*?)(?=\s+Net sales:|\s+CM1 profit:|\s+CM1 profit per unit:|\s+Product Journey:|$)"),
            ("Net sales", r"Net sales:\s*(.*?)(?=\s+CM1 profit:|\s+CM1 profit per unit:|\s+Product Journey:|$)"),
            ("CM1 profit", r"CM1 profit:\s*(.*?)(?=\s+CM1 profit per unit:|\s+Product Journey:|$)"),
            ("CM1 profit per unit", r"CM1 profit per unit:\s*(.*?)(?=\s+Product Journey:|$)"),
        ]

        for label, patt in metric_patterns:
            mm = re.search(patt, text, flags=re.I)
            if mm:
                val = re.sub(r"\s+", " ", mm.group(1).strip())
                metrics.append((label, val))

        # -------- Product Journey --------
        journey_text = ""
        jm = re.search(
            r"Product Journey:\s*(.*?)(?=\s+Recommendation:|\s+Advertising:|\s+Inventory:|$)",
            text,
            flags=re.I,
        )
        if jm:
            journey_text = jm.group(1).strip()

        journey_points = []
        if journey_text:
            parts = re.split(r"\s+-\s+", journey_text)
            for p in parts:
                p = p.strip(" -")
                if p:
                    journey_points.append(p)

        # -------- Recommendation --------
        recommendation = ""
        rm = re.search(
            r"Recommendation:\s*(.*?)(?=\s+Advertising:|\s+Inventory:|$)",
            text,
            flags=re.I,
        )
        if rm:
            recommendation = rm.group(1).strip()

        # -------- Advertising --------
        advertising = ""
        am = re.search(
            r"Advertising:\s*(.*?)(?=\s+Inventory:|$)",
            text,
            flags=re.I,
        )
        if am:
            advertising = am.group(1).strip()

        # -------- Inventory --------
        inventory = ""
        im = re.search(r"Inventory:\s*(.*)$", text, flags=re.I)
        if im:
            inventory = im.group(1).strip()

        return {
            "product_name": product_name,
            "metrics": metrics,
            "journey_points": journey_points,
            "recommendation": recommendation,
            "advertising": advertising,
            "inventory": inventory,
        }

    def _metric_chip(label: str, value_text: str) -> str:
        txt = (value_text or "").strip()

        m = re.match(r"^(.*?)(\s*\([^)]+\))?$", txt)
        main_value = (m.group(1) or "").strip() if m else txt
        delta_value = (m.group(2) or "").strip() if m else ""

        is_negative = bool(re.search(r"\(\s*-\s*\d", delta_value))
        delta_color = "#D92D20" if is_negative else "#16A34A"

        delta_html = (
            f'<span style="color:{delta_color}; font-weight:600;"> {html.escape(delta_value)}</span>'
            if delta_value else ""
        )

        return f"""
        <td valign="top" style="padding:0 8px 8px 0;">
          <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0"
                 style="border-collapse:separate; background:#F8FAFC; border:1px solid #E5E7EB; border-radius:8px;">
            <tr>
              <td style="padding:7px 9px; text-align:left; vertical-align:top;">
                <div style="font-size:11px; line-height:1.2; color:#667085; font-weight:700; margin-bottom:3px;">
                  {html.escape(label)}
                </div>
                <div style="font-size:12px; line-height:1.25; font-weight:600;">
                  <span style="color:#414042;">{html.escape(main_value)}</span>{delta_html}
                </div>
              </td>
            </tr>
          </table>
        </td>
        """

    def render_simple_action_card(sku: str, action_text: str) -> str:
        parsed = parse_sku_action_text(action_text)

        product = parsed["product_name"] or safe_product_name(sku) or "Unknown"
        product = html.escape(str(product))

        metric_cells = [_metric_chip(label, value) for label, value in parsed["metrics"]]

        metrics_html = ""
        if metric_cells:
            metrics_html = f"""
            <table role="presentation" cellpadding="0" cellspacing="0" border="0" style="margin-bottom:10px;">
              <tr>
                {''.join(metric_cells)}
              </tr>
            </table>
            """

        journey_html = ""
        if parsed["journey_points"]:
            bullet_rows = "".join(
                f"""
                <tr>
                  <td style="font-size:12px; color:#475467; line-height:1.75; padding:0 0 8px 0;">
                    <span style="color:#3B82F6; font-weight:700;">•</span>
                    {html.escape(point)}
                  </td>
                </tr>
                """
                for point in parsed["journey_points"]
            )

            journey_html = f"""
            <div style="font-size:11px; font-weight:800; color:#667085; text-transform:uppercase; margin:14px 0 8px 0; letter-spacing:0.4px;">
              Performance Journey
            </div>
            <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0">
              {bullet_rows}
            </table>
            """

        def _point_block(title: str, body: str) -> str:
            if not body:
                return ""

            clean_body = re.sub(r"\s+", " ", body).strip()

            return f"""
            <div style="margin-top:12px;">
              <div style="font-size:11px; font-weight:800; color:#667085; text-transform:uppercase; letter-spacing:0.3px; margin-bottom:6px;">
                {html.escape(title)}
              </div>
              <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0">
                <tr>
                  <td valign="top" style="font-size:12px; color:#475467; line-height:1.7; padding:0 0 4px 0;">
                    • {html.escape(clean_body)}
                  </td>
                </tr>
              </table>
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
            <div style="margin-top:10px; font-size:13px; line-height:1.6; color:#475467;">
              {html.escape(_strip_html_tags(action_text))}
            </div>
            """

        return f"""
        <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0"
               style="border-collapse:separate; width:100%; margin:0 0 18px 0; background:#FFFFFF; border:1px solid #E4E7EC; border-radius:14px;">
          <tr>
            <td style="padding:16px 16px 14px 16px;">

              <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0">
                <tr>
                  <td style="font-size:16px; font-weight:800; color:#1D2939; padding-bottom:10px;">
                    {product}
                  </td>
                </tr>
              </table>

              {metrics_html}
              {journey_html}
              {bottom_cards_html}
              {fallback_html}

            </td>
          </tr>
        </table>
        """

    # ---------------------------
    # SKU ACTIONS SECTION
    # ---------------------------
    sku_section_html = ""

    try:
        # Primary path: sku_actions is dict {sku -> rendered_text}
        if sku_actions and isinstance(sku_actions, dict):
            cards = []
            for sku_key, action_text in sku_actions.items():
                if not sku_key or not action_text:
                    continue
                cards.append(render_simple_action_card(sku_key, action_text))

            if cards:
                sku_section_html = "".join(cards)
            else:
                sku_section_html = _fallback_section_html()

        # Fallback path: overall_actions exists (old pipeline)
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
        dashboard_url = f"https://app.phormula.io/live-bi?token={deep_link_token}&country={country}"
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
      <link href="https://fonts.googleapis.com/css2?family=Lato:wght@400;700;800&display=swap" rel="stylesheet">
    </head>
    <body style="font-family:'Lato', Arial, sans-serif; background:#f4f4f4; padding:20px;">
      <div style="max-width:780px; margin:0 auto; background:#fff;
           padding:28px; border-radius:10px; border:2px solid #5EA68E; box-sizing:border-box;">

        <img src="https://i.postimg.cc/43T3k86Z/logo.png"
             style="width:180px; display:block; margin:0 auto 16px;" />

        <h2 style="text-align:center; color:#37455F;">
          Live MTD vs Previous Period – Business Insights
        </h2>

        <div style="text-align:center; margin-bottom:16px;">
          <div style="
            font-size:18px;
            font-weight:700;
            color:#37455F;
            background:#EAF3F0;
            display:inline-block;
            padding:6px 16px;
            border-radius:20px;
            margin-bottom:8px;">
            Country: {html.escape(str(country).upper())}
          </div>

          <div style="margin-top:10px;">
            <span style="
              font-size:14px;
              font-weight:600;
              color:#37455F;
              background:#F3F4F6;
              padding:4px 10px;
              border-radius:12px;
              margin-right:6px;
              display:inline-block;">
              Previous: {html.escape(str(prev_label))}
            </span>

            <span style="
              font-size:14px;
              font-weight:600;
              color:#ffffff;
              background:#37455F;
              padding:4px 10px;
              border-radius:12px;
              display:inline-block;">
              Current: {html.escape(str(curr_label))}
            </span>
          </div>
        </div>

        <hr style="margin:20px 0; border:none; border-top:1px solid #eee;" />

        <h3 style="color:#37455F; display:flex; align-items:center;">
          <span style="margin-left:8px;">Overall Summary</span>
        </h3>

        <div style="font-size:14px; color:#555; margin-bottom:8px;">
          {summary_html}
        </div>

        <h3 style="color:#37455F; margin-top:28px;">
          Recommendation
        </h3>

        {sku_section_html}

        {deep_link_html}

        <p style="font-size:12px; color:#999; margin-top:24px;">
          This email was auto-generated from Live BI.
        </p>
        <p style="font-size:12px; color:#999;">
          Support: <a href="mailto:care@phormula.io">care@phormula.io</a>
        </p>
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

