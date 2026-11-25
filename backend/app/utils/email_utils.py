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

def _welcome_shell(title: str, body_html: str) -> str:
    """Shared Phormula transactional-email shell."""
    return f"""
<!DOCTYPE html>
<html>
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <style>
    @media only screen and (max-width: 600px) {{
      .email-container {{ width:100% !important; max-width:100% !important; }}
      .content-cell {{ padding:22px 24px 26px 24px !important; }}
      .top-report-title {{ font-size:14px !important; }}
    }}
  </style>
</head>
<body style="margin:0; padding:0; font-family:Arial, Helvetica, sans-serif; background:#ffffff;">
  <table width="100%" cellpadding="0" cellspacing="0" border="0" style="padding:16px 0;">
    <tr><td align="center">
      <table class="email-container" width="600" cellpadding="0" cellspacing="0" border="0"
             style="width:600px; max-width:600px; border-collapse:collapse; background:#ffffff;">
        <tr>
          <td style="background:#5ea68e; padding:18px 24px; color:#ffffff;">
            <table width="100%" cellpadding="0" cellspacing="0" border="0">
              <tr>
                <td width="110">
                  <img src="https://res.cloudinary.com/du58s6gdz/image/upload/f_auto,q_auto/output-onlinepngtools_ypplvv"
                       alt="Phormula" width="40" style="display:block; width:40px; height:auto; border:0;" />
                </td>
                <td align="right" class="top-report-title"
                    style="font-size:16px; color:#f8edce; white-space:nowrap;">{html.escape(title)}</td>
              </tr>
            </table>
          </td>
        </tr>
        <tr>
          <td align="center" style="padding:28px 30px 18px; border-left:1px solid #e4e7ec; border-right:1px solid #e4e7ec;">
            <img src="https://res.cloudinary.com/du58s6gdz/image/upload/f_auto,q_auto/Logo_Phormula_pmbp8q"
                 alt="Phormula Logo" width="220"
                 style="display:block; width:220px; max-width:220px; height:auto; margin:0 auto 14px; border:0;" />
            <div style="font-size:18px; color:#4a4a4a; line-height:1.4;">Welcome to Phormula</div>
          </td>
        </tr>
        <tr><td style="border-top:1px solid #dddddd; font-size:1px; line-height:1px;">&nbsp;</td></tr>
        <tr>
          <td class="content-cell" style="padding:22px 32px 26px; color:#444444; font-size:14px; line-height:1.7; border-left:1px solid #e4e7ec; border-right:1px solid #e4e7ec;">
            {body_html}
          </td>
        </tr>
        <tr>
          <td style="border-top:1px solid #dddddd; padding:14px 32px 16px; background:#ffffff; font-size:12px; color:#999999; border-left:1px solid #e4e7ec; border-right:1px solid #e4e7ec;">
            This email was generated automatically by Phormula.
          </td>
        </tr>
        <tr>
          <td align="center" style="background:#5ea68e; padding:12px 18px; color:#f8edce; font-size:12px;">
            © 2026 Phormula. All rights reserved.
          </td>
        </tr>
      </table>
    </td></tr>
  </table>
</body>
</html>
"""


def send_verification_otp_email(email, name, otp, expires_minutes=10):
    """
    Send a secure one-time verification code email.
    OTP must never be logged or returned through an API response.
    """

    display_name = html.escape((name or "there").strip())
    safe_otp = html.escape(str(otp))

    msg = Message(
        "Verify your Phormula account",
        sender=("Phormula Care Team", "care@phormula.io"),
        recipients=[email],
    )

    msg.html = f"""
    <!DOCTYPE html>
    <html>
    <head>
      <meta charset="UTF-8">
      <meta name="viewport" content="width=device-width, initial-scale=1.0">

      <style>
        @media only screen and (max-width: 600px) {{
          .email-card {{
            width: 100% !important;
            max-width: 100% !important;
          }}

          .content {{
            padding: 26px 22px !important;
          }}

          .otp-code {{
            font-size: 28px !important;
            letter-spacing: 7px !important;
          }}
        }}
      </style>
    </head>

    <body style="
      margin:0;
      padding:0;
      background:#f7f9f8;
      font-family:Arial, Helvetica, sans-serif;
    ">

      <table
        role="presentation"
        width="100%"
        cellpadding="0"
        cellspacing="0"
        border="0"
        style="background:#f7f9f8; padding:30px 15px;"
      >
        <tr>
          <td align="center">

            <table
              class="email-card"
              role="presentation"
              width="560"
              cellpadding="0"
              cellspacing="0"
              border="0"
              style="
                width:560px;
                max-width:560px;
                background:#ffffff;
                border-radius:16px;
                overflow:hidden;
                border:1px solid #e5e7eb;
                box-shadow:0 8px 24px rgba(16,24,40,0.06);
              "
            >

              <!-- HEADER -->
              <tr>
                <td style="
                  background:#5EA68E;
                  padding:16px 24px;
                ">
                  <table width="100%" cellpadding="0" cellspacing="0">
                    <tr>
                      <td align="left">
                        <img
                          src="https://res.cloudinary.com/du58s6gdz/image/upload/f_auto,q_auto/output-onlinepngtools_ypplvv"
                          alt="Phormula"
                          width="38"
                          style="
                            display:block;
                            width:38px;
                            height:auto;
                            border:0;
                          "
                        />
                      </td>

                      <td
                        align="right"
                        style="
                          color:#F8EDCE;
                          font-size:14px;
                          font-weight:600;
                        "
                      >
                        Account Verification
                      </td>
                    </tr>
                  </table>
                </td>
              </tr>

              <!-- CONTENT -->
              <tr>
                <td
                  class="content"
                  style="
                    padding:34px 42px 32px;
                    text-align:center;
                  "
                >

                  <!-- BRAND -->
                  <img
                    src="https://res.cloudinary.com/du58s6gdz/image/upload/f_auto,q_auto/Logo_Phormula_pmbp8q"
                    alt="Phormula"
                    width="190"
                    style="
                      display:block;
                      width:190px;
                      max-width:190px;
                      height:auto;
                      margin:0 auto 26px;
                      border:0;
                    "
                  />

                  <!-- ICON -->
                  <table
                    role="presentation"
                    align="center"
                    cellpadding="0"
                    cellspacing="0"
                    border="0"
                    style="margin:0 auto 18px;"
                  >
                    <tr>
                      <td
                        align="center"
                        style="
                          width:58px;
                          height:58px;
                          background:#EEF7F3;
                          border-radius:50%;
                          font-size:26px;
                          color:#5EA68E;
                        "
                      >
                        ✉
                      </td>
                    </tr>
                  </table>

                  <h1 style="
                    margin:0;
                    font-size:24px;
                    line-height:1.3;
                    color:#303030;
                    font-weight:700;
                  ">
                    Verify your email
                  </h1>

                  <p style="
                    margin:10px 0 0;
                    font-size:14px;
                    line-height:1.7;
                    color:#667085;
                  ">
                    Hi <strong style="color:#37455F;">{display_name}</strong>,
                    use the verification code below to complete your Phormula registration.
                  </p>

                  <!-- OTP -->
                  <table
                    role="presentation"
                    width="100%"
                    cellpadding="0"
                    cellspacing="0"
                    border="0"
                    style="margin:28px 0 20px;"
                  >
                    <tr>
                      <td align="center">

                        <table
                          role="presentation"
                          cellpadding="0"
                          cellspacing="0"
                          border="0"
                          style="
                            background:#F4FAF7;
                            border:1px solid #D1E9E2;
                            border-radius:14px;
                          "
                        >
                          <tr>
                            <td style="
                              padding:18px 28px;
                              text-align:center;
                            ">

                              <div style="
                                margin-bottom:7px;
                                font-size:11px;
                                line-height:1.4;
                                color:#667085;
                                font-weight:600;
                                text-transform:uppercase;
                                letter-spacing:1.3px;
                              ">
                                Verification Code
                              </div>

                              <div
                                class="otp-code"
                                style="
                                  font-size:34px;
                                  line-height:1.2;
                                  font-weight:700;
                                  letter-spacing:10px;
                                  color:#37455F;
                                  white-space:nowrap;
                                "
                              >
                                {safe_otp}
                              </div>

                            </td>
                          </tr>
                        </table>

                      </td>
                    </tr>
                  </table>

                  <!-- EXPIRY -->
                  <p style="
                    margin:0;
                    font-size:13px;
                    line-height:1.6;
                    color:#667085;
                  ">
                    This code expires in
                    <strong style="color:#37455F;">
                      {int(expires_minutes)} minutes
                    </strong>.
                  </p>

                  <!-- SECURITY BOX -->
                  <table
                    role="presentation"
                    width="100%"
                    cellpadding="0"
                    cellspacing="0"
                    border="0"
                    style="
                      margin-top:24px;
                      background:#F9FAFB;
                      border:1px solid #EAECF0;
                      border-radius:10px;
                    "
                  >
                    <tr>
                      <td style="
                        padding:14px 16px;
                        text-align:left;
                        font-size:12px;
                        line-height:1.65;
                        color:#667085;
                      ">
                        <strong style="color:#344054;">
                          Security reminder
                        </strong>
                        <br />
                        Never share this verification code with anyone.
                        Phormula will never ask you for this code outside
                        the verification screen.
                      </td>
                    </tr>
                  </table>

                  <p style="
                    margin:22px 0 0;
                    font-size:12px;
                    line-height:1.6;
                    color:#98A2B3;
                  ">
                    If you didn't create a Phormula account,
                    you can safely ignore this email.
                  </p>

                  <div style="
                    margin-top:28px;
                    padding-top:20px;
                    border-top:1px solid #EAECF0;
                    text-align:left;
                  ">
                    <p style="
                      margin:0;
                      font-size:13px;
                      line-height:1.6;
                      color:#667085;
                    ">
                      Warm regards,<br />
                      <strong style="color:#344054;">
                        The Phormula Team
                      </strong>
                    </p>

                    <p style="
                      margin:3px 0 0;
                      font-size:12px;
                    ">
                      <a
                        href="mailto:care@phormula.io"
                        style="
                          color:#5EA68E;
                          text-decoration:none;
                        "
                      >
                        care@phormula.io
                      </a>
                    </p>
                  </div>

                </td>
              </tr>

              <!-- FOOTER -->
              <tr>
                <td
                  align="center"
                  style="
                    background:#5EA68E;
                    padding:12px 18px;
                    color:#F8EDCE;
                    font-size:11px;
                    line-height:1.5;
                  "
                >
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

    mail.send(msg)
    

def send_welcome_email(email, name):
    """Welcome-only email for identities already verified by the provider (for example Google)."""
    display_name = html.escape((name or "there").strip())
    body_html = f"""
      <p style="margin:0 0 18px;">Hey <strong>{display_name}</strong>,</p>
      <p style="margin:0 0 14px; text-align:justify;">
        Welcome to <strong>Phormula</strong>, a platform built for modern D2C brands. Your account is ready to use.
      </p>
      <p style="margin:0 0 16px; color:#777777; text-align:justify;">
        If you did not create this account, please contact our support team.
      </p>
      <p style="margin:18px 0 0;">Warm regards,<br/><strong>The Phormula Team</strong><br/>
        <a href="mailto:care@phormula.io" style="color:#37455f; text-decoration:none;">care@phormula.io</a>
      </p>
    """

    msg = Message(
        "Welcome to Phormula",
        sender=("Phormula Care Team", "care@phormula.io"),
        recipients=[email],
    )
    msg.html = _welcome_shell("Welcome", body_html)
    mail.send(msg)


def send_password_reset_otp_email(
    to_email,
    name,
    otp,
    expires_minutes=10,
):
    display_name = html.escape((name or "there").strip())
    safe_otp = html.escape(str(otp))

    msg = Message(
        "Your Phormula password reset code",
        sender=("Phormula Care Team", "care@phormula.io"),
        recipients=[to_email],
    )

    msg.html = f"""
    <!DOCTYPE html>
    <html>
    <head>
      <meta charset="UTF-8">
      <meta name="viewport" content="width=device-width, initial-scale=1.0">

      <style>
        @media only screen and (max-width: 600px) {{
          .email-card {{
            width:100% !important;
            max-width:100% !important;
          }}

          .content {{
            padding:26px 22px !important;
          }}

          .otp-code {{
            font-size:28px !important;
            letter-spacing:7px !important;
          }}
        }}
      </style>
    </head>

    <body style="
      margin:0;
      padding:0;
      background:#f7f9f8;
      font-family:Arial, Helvetica, sans-serif;
    ">

      <table
        role="presentation"
        width="100%"
        cellpadding="0"
        cellspacing="0"
        border="0"
        style="background:#f7f9f8;padding:30px 15px;"
      >
        <tr>
          <td align="center">

            <table
              class="email-card"
              role="presentation"
              width="560"
              cellpadding="0"
              cellspacing="0"
              border="0"
              style="
                width:560px;
                max-width:560px;
                background:#ffffff;
                border-radius:16px;
                overflow:hidden;
                border:1px solid #e5e7eb;
              "
            >

              <!-- Header -->
              <tr>
                <td style="
                  background:#5EA68E;
                  padding:16px 24px;
                ">
                  <table
                    width="100%"
                    cellpadding="0"
                    cellspacing="0"
                    border="0"
                  >
                    <tr>
                      <td align="left">
                        <img
                          src="https://res.cloudinary.com/du58s6gdz/image/upload/f_auto,q_auto/output-onlinepngtools_ypplvv"
                          alt="Phormula"
                          width="38"
                          style="
                            display:block;
                            width:38px;
                            height:auto;
                            border:0;
                          "
                        />
                      </td>

                      <td
                        align="right"
                        style="
                          color:#F8EDCE;
                          font-size:14px;
                          font-weight:600;
                        "
                      >
                        Password Reset
                      </td>
                    </tr>
                  </table>
                </td>
              </tr>

              <!-- Content -->
              <tr>
                <td
                  class="content"
                  style="
                    padding:34px 42px 32px;
                    text-align:center;
                  "
                >

                  <img
                    src="https://res.cloudinary.com/du58s6gdz/image/upload/f_auto,q_auto/Logo_Phormula_pmbp8q"
                    alt="Phormula"
                    width="190"
                    style="
                      display:block;
                      width:190px;
                      max-width:190px;
                      height:auto;
                      margin:0 auto 26px;
                      border:0;
                    "
                  />

                  <h1 style="
                    margin:0;
                    font-size:24px;
                    line-height:1.3;
                    color:#303030;
                    font-weight:700;
                  ">
                    Reset your password
                  </h1>

                  <p style="
                    margin:10px 0 0;
                    font-size:14px;
                    line-height:1.7;
                    color:#667085;
                  ">
                    Hi
                    <strong style="color:#37455F;">
                      {display_name}
                    </strong>,
                    use the code below to reset your Phormula password.
                  </p>

                  <!-- OTP -->
                  <table
                    role="presentation"
                    width="100%"
                    cellpadding="0"
                    cellspacing="0"
                    border="0"
                    style="margin:28px 0 20px;"
                  >
                    <tr>
                      <td align="center">

                        <table
                          role="presentation"
                          cellpadding="0"
                          cellspacing="0"
                          border="0"
                          style="
                            background:#F4FAF7;
                            border:1px solid #D1E9E2;
                            border-radius:14px;
                          "
                        >
                          <tr>
                            <td style="
                              padding:18px 28px;
                              text-align:center;
                            ">

                              <div style="
                                margin-bottom:7px;
                                font-size:11px;
                                color:#667085;
                                font-weight:600;
                                text-transform:uppercase;
                                letter-spacing:1.3px;
                              ">
                                Password Reset Code
                              </div>

                              <div
                                class="otp-code"
                                style="
                                  font-size:34px;
                                  font-weight:700;
                                  letter-spacing:10px;
                                  color:#37455F;
                                  white-space:nowrap;
                                "
                              >
                                {safe_otp}
                              </div>

                            </td>
                          </tr>
                        </table>

                      </td>
                    </tr>
                  </table>

                  <p style="
                    margin:0;
                    font-size:13px;
                    line-height:1.6;
                    color:#667085;
                  ">
                    This code expires in
                    <strong style="color:#37455F;">
                      {int(expires_minutes)} minutes
                    </strong>.
                  </p>

                  <!-- Security -->
                  <table
                    role="presentation"
                    width="100%"
                    cellpadding="0"
                    cellspacing="0"
                    border="0"
                    style="
                      margin-top:24px;
                      background:#F9FAFB;
                      border:1px solid #EAECF0;
                      border-radius:10px;
                    "
                  >
                    <tr>
                      <td style="
                        padding:14px 16px;
                        text-align:left;
                        font-size:12px;
                        line-height:1.65;
                        color:#667085;
                      ">
                        <strong style="color:#344054;">
                          Security reminder
                        </strong>

                        <br />

                        Never share this password reset code with anyone.

                        <br />

                        If you did not request a password reset,
                        you can safely ignore this email.
                      </td>
                    </tr>
                  </table>

                  <div style="
                    margin-top:28px;
                    padding-top:20px;
                    border-top:1px solid #EAECF0;
                    text-align:left;
                  ">
                    <p style="
                      margin:0;
                      font-size:13px;
                      line-height:1.6;
                      color:#667085;
                    ">
                      Warm regards,<br />
                      <strong style="color:#344054;">
                        The Phormula Team
                      </strong>
                    </p>

                    <p style="
                      margin:3px 0 0;
                      font-size:12px;
                    ">
                      <a
                        href="mailto:care@phormula.io"
                        style="
                          color:#5EA68E;
                          text-decoration:none;
                        "
                      >
                        care@phormula.io
                      </a>
                    </p>
                  </div>

                </td>
              </tr>

              <!-- Footer -->
              <tr>
                <td
                  align="center"
                  style="
                    background:#5EA68E;
                    padding:12px 18px;
                    color:#F8EDCE;
                    font-size:11px;
                  "
                >
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
    country,
    prev_label,
    curr_label,
    deep_link_token=None,
    weekly_email_summary_json=None,
    overall_summary=None,
    overall_actions=None,
    sku_actions=None,
    sku_to_product=None,
    portfolio_recommendation=None,
    brand_name=None,
    live_bi_data=None,
    user_name=None,
    dashboard_recap=None,
):
    import traceback
    import html
    import re
    import math

    if not to_email:
        print("[WARN] No email provided.")
        return False

    # Server P&L context is available for automatic emails as well as manual ones.
    # A supplied dashboard snapshot may fill individual fields, including zero.
    server_recap = (live_bi_data or {}).get("email_dashboard_recap") or {}
    supplied_recap = dashboard_recap or {}
    dashboard_recap = {**server_recap, **supplied_recap}
    for period in ("current", "previous"):
        dashboard_recap[period] = {
            **(server_recap.get(period) or {}),
            **{key: value for key, value in (supplied_recap.get(period) or {}).items() if value is not None},
        }

    period_label = re.sub(r"\s*[–—]\s*", "-", str(curr_label or "")).strip()
    brand_name = re.sub(r"[\r\n]+", " ", str(brand_name or "")).strip()
    country_label = str(country or "").strip().upper()
    brand_subject = f" | {brand_name}" if brand_name else ""
    subject = f"Phormula Weekly Live BI Report {period_label}{brand_subject} | Amazon {country_label}"
    header_title = f"Business insights Report {period_label}".strip()

    # ---------------------------
    # Overall summary
    # ---------------------------
    if weekly_email_summary_json and isinstance(weekly_email_summary_json, dict):
        summary_text = (
            weekly_email_summary_json.get("portfolio_summary", {}).get("what_changed")
            or weekly_email_summary_json.get("headline")
            or ""
        )
        portfolio_recommendation = (
            weekly_email_summary_json.get("portfolio_summary", {}).get("weekly_action")
            or portfolio_recommendation
        )
    else:
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

    def email_product_name(*values):
        for value in values:
            text = str(value).strip() if value is not None else ""
            if text.lower() not in {"", "nan", "none", "null", "<na>", "nat"}:
                return text
        return "Unknown"

    def render_live_bi_recap(data):
        if not isinstance(data, dict):
            return ""

        def finite_number(value):
            try:
                number = float(value)
                return number if math.isfinite(number) else None
            except (TypeError, ValueError):
                return None

        totals = data.get("aligned_totals") or {}
        target_data = data.get("target_context") or {}
        objective_data = data.get("objective_context") or {}
        dashboard_current = (dashboard_recap or {}).get("current") or {}
        dashboard_previous = (dashboard_recap or {}).get("previous") or {}
        has_dashboard_snapshot = bool(dashboard_current)
        currency_symbol = ((data.get("currency") or {}).get("symbol") or "$").strip()

        def money(value, decimals=0):
            value = finite_number(value)
            return f"{currency_symbol}{value:,.{decimals}f}" if value is not None else "—"

        actual = finite_number(dashboard_current.get("net_sales")) if has_dashboard_snapshot else finite_number(totals.get("total_current_net_sales"))
        target = finite_number((dashboard_recap or {}).get("target_sales")) if has_dashboard_snapshot else None
        if target is None:
            target = finite_number(target_data.get("target_sales"))
        has_target = target is not None and target > 0
        achievement = actual / target * 100 if has_target and actual is not None else None
        progress_width = max(0, min(100, achievement or 0))
        achievement_text = f"{achievement:.1f}%" if achievement is not None else "—"

        def objective_title(value):
            return " ".join(part.capitalize() for part in re.sub(r"[_-]+", " ", str(value or "")).split())

        objectives = []
        if objective_data.get("growth_intent"):
            objectives.append(f"Growth focus: {objective_title(objective_data['growth_intent'])}")
        if objective_data.get("profit_priority"):
            objectives.append(f"Profit focus: {objective_title(objective_data['profit_priority'])}")
        clearance = objective_data.get("inventory_clearance_priority")
        if clearance is not None:
            objectives.append("Prioritise clearing ageing inventory" if clearance else "Inventory dilution: No")
        objective_html = "".join(
            f'<span style="display:inline-block;background:#F2F4F7;color:#37455F;padding:5px 9px;border-radius:14px;font-size:10px;margin:3px;text-transform:capitalize;"><span style="color:#5EA68E;">●</span> {html.escape(item)}</span>'
            for item in objectives
        )

        target_html = f"""
        <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="border:1px solid #E6ECEF;border-radius:12px;margin:12px 0 10px;background:#FFFFFF;">
          <tr><td colspan="3" align="center" style="padding:12px 8px 8px;font-size:16px;font-weight:bold;color:#37455F;">Target and Objectives</td></tr>
          <tr>
            <td width="35%" align="center" valign="middle" style="padding:8px 3px;">
              <div style="font-size:11px;color:#50627A;"><span style="color:#ED9F50;">●</span> Net sales</div>
              <div style="font-size:19px;font-weight:bold;color:#37455F;">{money(actual)}</div>
            </td>
            <td width="30%" align="center" valign="middle" style="padding:8px 8px;">
              <div style="font-size:22px;font-weight:bold;color:#37455F;">{achievement_text}</div>
              <div style="font-size:10px;color:#50627A;">Target Achieved</div>
              {f'<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="margin-top:7px;border-collapse:collapse;"><tr><td height="6" bgcolor="#D0D5DD" style="height:6px;padding:0;background:#D0D5DD;border-radius:5px;font-size:1px;line-height:1px;"><table role="presentation" width="{progress_width:.1f}%" cellpadding="0" cellspacing="0" border="0" style="width:{progress_width:.1f}%;border-collapse:collapse;"><tr><td height="6" bgcolor="#ED9F50" style="height:6px;padding:0;background:#ED9F50;border-radius:5px;font-size:1px;line-height:1px;">&nbsp;</td></tr></table></td></tr></table>' if has_target else ''}
            </td>
            <td width="35%" align="center" valign="middle" style="padding:8px 3px;">
              <div style="font-size:11px;color:#50627A;"><span style="color:#5EA68E;">●</span> Target</div>
              <div style="font-size:19px;font-weight:bold;color:#37455F;">{money(target) if has_target else 'Not set'}</div>
            </td>
          </tr>
          {f'<tr><td colspan="3" align="center" style="padding:3px 6px 11px;">{objective_html}</td></tr>' if objective_html else ''}
        </table>
        """

        metric_specs = [
            ("Units", dashboard_current.get("units", totals.get("total_current_quantity")), "units_change_percentage", "units", "number", "#FDD36F", False),
            ("ASP", dashboard_current.get("asp", totals.get("total_current_asp")), "asp_change_percentage", "asp", "money2", "#ED9F50", False),
            ("Net Sales", actual, "net_sales_change_percentage", "net_sales", "money", "#75BBDA", False),
            ("Ads", dashboard_current.get("advertising", totals.get("total_current_advertising")), "advertising_change_percentage", "advertising", "money", "#B75A5A", True),
            ("TACoS", dashboard_current.get("tacos", totals.get("total_current_tacos")), "tacos_change_percentage", "tacos", "percent", "#3A8EA4", True),
            ("CM2 Profit", dashboard_current.get("cm2_profit", totals.get("total_current_profit_cm2")), "cm2_profit_change_percentage", "cm2_profit", "money", "#B8C78C", False),
        ]
        metric_rows = []
        for start in range(0, len(metric_specs), 3):
            cells = []
            for label, value, delta_key, dashboard_key, fmt, accent, inverse in metric_specs[start:start + 3]:
                numeric_value = finite_number(value)
                delta = finite_number(totals.get(delta_key))
                if has_dashboard_snapshot:
                    previous_value = finite_number(dashboard_previous.get(dashboard_key))
                    delta = ((numeric_value - previous_value) / abs(previous_value) * 100) if numeric_value is not None and previous_value not in (None, 0) else None
                if fmt == "number":
                    formatted_value = f"{numeric_value:,.0f}" if numeric_value is not None else "—"
                elif fmt == "money2":
                    formatted_value = money(numeric_value, 2)
                elif fmt == "percent":
                    formatted_value = f"{numeric_value:.2f}%" if numeric_value is not None else "—"
                else:
                    formatted_value = money(numeric_value)
                delta_html = ""
                if delta is not None:
                    good = delta <= 0 if inverse else delta >= 0
                    color = "#12B76A" if good else "#F04438"
                    arrow = "▲" if delta >= 0 else "▼"
                    delta_html = f'<span style="font-size:10px;font-weight:bold;color:{color};white-space:nowrap;">{arrow} {abs(delta):.2f}%</span>'
                cells.append(f"""
                <td width="33.33%" valign="top" style="padding:4px 3px;">
                  <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="border:1px solid {accent};border-top:4px solid {accent};border-radius:10px;border-collapse:separate;border-spacing:0;overflow:hidden;background:#FFFFFF;">
                    <tr><td style="padding:9px 8px;">
                      <div style="font-size:11px;color:#50627A;">{label}</div>
                      <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" ><tr><td style="font-size:16px;font-weight:bold;color:#37455F;padding-top:4px;">{formatted_value}</td><td align="right" valign="bottom">{delta_html}</td></tr></table>
                    </td></tr>
                  </table>
                </td>
                """)
            metric_rows.append("<tr>" + "".join(cells) + "</tr>")
        metrics_html = '<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="border-collapse:collapse;margin-top:10px;">' + "".join(metric_rows) + "</table>"

        # Keep this recap table independent from the recommendation cards below.
        # Prefer Dashboard MTD SKU data when supplied by the dashboard request;
        # scheduled/background callers can still fall back to Live BI's MTD rows.
        growth = data.get("categorized_growth") or {}
        candidates = []
        seen_products = set()
        email_rows = data.get("email_metric_rows")
        for bucket in (("email",) if email_rows is not None else ("top_80_skus", "new_skus", "reviving_skus", "new_or_reviving_skus", "other_skus")):
            rows = email_rows if bucket == "email" else growth.get(bucket) or []
            if not isinstance(rows, list):
                continue
            for row in rows:
                if not isinstance(row, dict):
                    continue
                identity = str(row.get("sku") or row.get("product_name") or "").strip().lower()
                if not identity or identity in seen_products:
                    continue
                seen_products.add(identity)
                candidates.append(row)

        def growth_value(row):
            metric = row.get("Net Sales Growth (%)")
            value = metric.get("value") if isinstance(metric, dict) else None
            return finite_number(value)

        top_seller = max(candidates, key=lambda row: finite_number(row.get("net_sales_curr")) or 0, default=None)
        growth_rows = [row for row in candidates if growth_value(row) is not None]
        highest_growth = max(growth_rows, key=growth_value, default=None)
        lowest_growth = min(growth_rows, key=growth_value, default=None)
        highlights = []
        dashboard_highlights = (dashboard_recap or {}).get("sku_highlights") or []
        if has_dashboard_snapshot and isinstance(dashboard_highlights, list) and dashboard_highlights:
            for item in dashboard_highlights:
                if not isinstance(item, dict):
                    continue
                highlights.append((item.get("title") or "", {
                    "sku": item.get("sku"),
                    "product_name": item.get("name"),
                    "quantity_curr": item.get("units"),
                    "asp_curr": item.get("asp"),
                    "net_sales_curr": item.get("net_sales"),
                    "acos_curr": item.get("acos"),
                    "cm2_profit_curr": item.get("cm2_profit"),
                }))
        else:
            for title, row in (("Top Selling SKU", top_seller), ("Highest Net Sales Growth", highest_growth), ("Lowest Net Sales Growth", lowest_growth)):
                if row is not None:
                    highlights.append((title, row))

        same_top_and_growth = bool(
            top_seller and highest_growth
            and str(top_seller.get("sku") or top_seller.get("product_name") or "").strip().lower()
            == str(highest_growth.get("sku") or highest_growth.get("product_name") or "").strip().lower()
        )
        table_headers = ["Category", "Product Name", "Units", "ASP", "Net Sales", "ACoS", "CM2 Profit"]
        header_html = "".join(f'<th style="padding:6px 4px;background:#5EA68E;color:#FFFFFF;font-size:10px;font-weight:bold;text-align:center;border-right:1px solid #E6ECEF;">{header}</th>' for header in table_headers)
        sku_rows = []
        for title, sku in highlights:
            acos = finite_number(sku.get("acos_curr"))
            values = [
                title, email_product_name(sku.get("product_name"), sku.get("sku")),
                f"{finite_number(sku.get('quantity_curr')):,.0f}" if finite_number(sku.get("quantity_curr")) is not None else "—",
                money(sku.get("asp_curr"), 2), money(sku.get("net_sales_curr")),
                f"{acos:.2f}%" if acos is not None else "—", money(sku.get("cm2_profit_curr")),
            ]
            cells = []
            for column_index, value in enumerate(values):
                if same_top_and_growth and title == "Highest Net Sales Growth" and column_index > 0:
                    continue
                rowspan = ' rowspan="2"' if same_top_and_growth and title == "Top Selling SKU" and column_index > 0 else ""
                cells.append(f'<td{rowspan} style="padding:7px 4px;text-align:center;vertical-align:middle;border:1px solid #E6ECEF;color:#344054;font-size:10px;">{html.escape(str(value))}</td>')
            sku_rows.append("<tr>" + "".join(cells) + "</tr>")
        sku_html = (
            '<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="margin-top:12px;border:1px solid #E6ECEF;border-radius:12px;border-collapse:separate;border-spacing:0;overflow:hidden;">'
            f'<thead><tr>{header_html}</tr></thead><tbody>{"".join(sku_rows)}</tbody></table>'
            if sku_rows else ""
        )
        recap_heading = f'<div style="font-size:16px;line-height:1.35;font-weight:bold;color:#37455F;margin:14px 0 3px;">Here\'s how your MTD went</div>'
        return recap_heading + target_html + metrics_html + sku_html

    live_bi_recap_html = render_live_bi_recap(live_bi_data)

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
          <div style="font-size:14px; font-weight:700; color:#414042; margin-bottom:6px;">
            Portfolio recommendation
          </div>
          <div style="font-size:14px; color:#414042; line-height:1.7;">
            {portfolio_body}
          </div>
        </div>
        """

    high_alert_html = ""

    if weekly_email_summary_json and isinstance(weekly_email_summary_json, dict):
        high_alert = weekly_email_summary_json.get("high_alert") or {}

        if high_alert.get("available"):
            high_alert_message = high_alert.get("message") or ""
            high_alert_detail = re.sub(
                r"^\s*High\s+Alert\s*(?:[-–—>]+|:)\s*",
                "",
                str(high_alert_message),
                flags=re.IGNORECASE,
            ).strip()

            high_alert_html = f"""
            <div style="
                margin-top:14px;
                background:#F8FAFC;
                border:1px solid #B75A5A;
                border-top:4px solid #B75A5A;
                border-radius:10px;
                padding:12px 16px;
            ">
              <div style="
                  font-size:14px;
                  font-weight:700;
                  color:#414042;
                  line-height:1.4;
                  margin-bottom:4px;
              ">
                High Alert
              </div>
              <div style="
                  font-size:14px;
                  color:#414042;
                  line-height:1.6;
                  font-weight:400;
              ">
                {html.escape(high_alert_detail)}
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
            return email_product_name(sku_to_product.get(sku), sku)
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
            "CM1 profit per unit": "CM1 Profit/Unit",
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
                font-size:16px;
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
    
    def render_weekly_email_action_card(item: dict) -> str:
      product_name = html.escape(email_product_name(item.get("product_name"), item.get("sku")))
      metric_cards = item.get("metric_cards") or []
      action = html.escape(str(item.get("action") or ""))

      border_map = {
          "Units": "#E9B949",
          "Net Sales": "#73B8D8",
          "ASP": "#C65B5B",
          "Ads": "#C78B52",
          "CM2 Profit": "#7BA05B",
          "CM2 Profit Per Unit": "#C78B52",
          "Current Inventory": "#7BA05B",
          "Stock Cover (Months)": "#C78B52",
      }

      def _change_html(change):
          if change in (None, "", "None"):
              return ""

          change_text = str(change).strip()

          if change_text.startswith("-"):
              change_color = "#F04438"
              arrow = "▼"
          elif change_text.startswith("+"):
              change_color = "#12B76A"
              arrow = "▲"
          else:
              change_color = "#475467"
              arrow = ""

          change_value = change_text.lstrip("+-").strip()

          return f"""
          <div class="weekly-metric-delta" style="
              font-size:10px;
              line-height:1.15;
              font-weight:700;
              color:{change_color};
              margin-top:3px;
              text-align:left;
              white-space:nowrap;
              overflow:hidden;
              text-overflow:ellipsis;
          ">
              {arrow} {html.escape(change_value)}
          </div>
          """

      def _render_metric_box(metric: dict) -> str:
          label = str(metric.get("label") or "").strip()
          value = str(metric.get("value") or "").strip()
          change = metric.get("change")

          if not label and not value:
              return ""

          border_color = border_map.get(label, "#D0D5DD")

          return f"""
          <div class="weekly-metric-box" style="
              display:inline-block;
              width:25%;
              max-width:25%;
              padding:3px;
              vertical-align:top;
              box-sizing:border-box;
              font-size:14px;
          ">
              <div style="
                  width:100%;
                  min-height:64px;
                  height:64px;
                  border:1px solid {border_color};
                  border-top:3px solid {border_color};
                  border-radius:10px;
                  background:#FFFFFF;
                  padding:6px 6px 5px 6px;
                  box-sizing:border-box;
                  overflow:hidden;
              ">
                  <div class="weekly-metric-label" style="
                      font-size:11px;
                      line-height:1.15;
                      color:#475467;
                      font-weight:500;
                      margin:0 0 5px 0;
                      text-align:left;
                      white-space:nowrap;
                      overflow:hidden;
                      text-overflow:ellipsis;
                  ">
                      {html.escape(label)}
                  </div>

                  <div class="weekly-metric-value" style="
                      font-size:14px;
                      line-height:1.15;
                      font-weight:700;
                      color:#344054;
                      margin:0;
                      text-align:left;
                      white-space:nowrap;
                      overflow:hidden;
                      text-overflow:ellipsis;
                  ">
                      {html.escape(value)}
                  </div>

                  {_change_html(change)}
              </div>
          </div>
          """

      metric_boxes_html = "".join(
          _render_metric_box(metric)
          for metric in metric_cards[:8]
      )

      if not metric_boxes_html:
          metric_boxes_html = """
          <div style="
              font-size:13px;
              color:#667085;
              line-height:1.7;
              margin:8px 0 14px 0;
          ">
              Metrics are not available for this email card.
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
          <div style="padding:16px 16px 14px 16px; box-sizing:border-box;">

              <div style="
                  font-size:18px;
                  font-weight:700;
                  color:#344054;
                  margin-bottom:10px;
                  line-height:1.25;
              ">
                  {product_name}
              </div>

              <div class="weekly-metrics-grid" style="
                  width:100%;
                  margin:0 0 12px 0;
                  font-size:0;
                  line-height:0;
                  box-sizing:border-box;
              ">
                  {metric_boxes_html}
              </div>

              <div style="
                  font-size:14px;
                  color:#475467;
                  line-height:1.7;
                  margin-top:10px;
              ">
                  <strong style="color:#344054;">This week’s action:</strong> {action}
              </div>

          </div>
      </div>
      """

    # ---------------------------
    # SKU ACTIONS SECTION
    # ---------------------------
    sku_section_html = ""

    try:
        if weekly_email_summary_json and isinstance(weekly_email_summary_json, dict):
            priority_skus = weekly_email_summary_json.get("priority_skus") or []

            if priority_skus:
                sku_section_html = "".join(
                    render_weekly_email_action_card(item)
                    for item in priority_skus[:3]
                )
            else:
                sku_section_html = _fallback_section_html()

        elif sku_actions and isinstance(sku_actions, dict):
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

        dashboard_url = f"http://www.phormula.io/live-dashboard/{country_slug}/{current_month}/{current_year}#ai-insights"

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
          font-size: 16px !important;
          line-height: 1.35 !important;
          margin: 18px 0 10px 0 !important;
          color: #14213D !important;
        }}

        @media only screen and (max-width: 767px) {{
  .email-container {{
    width:100% !important;
    max-width:100% !important;
  }}

  .top-report-title {{
    font-size:14px !important;
    line-height:18px !important;
  }}

  .content-cell {{
    padding:22px 24px 26px !important;
  }}

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

.weekly-metrics-grid {{
  width: 100% !important;
  font-size: 0 !important;
  line-height: 0 !important;
}}

.weekly-metric-box {{
  display: inline-block !important;
  width: 25% !important;
  max-width: 25% !important;
  padding: 4px !important;
  vertical-align: top !important;
  box-sizing: border-box !important;
}}

.weekly-metric-label {{
  font-size: 11px !important;
  line-height: 1.15 !important;
}}

.weekly-metric-value {{
  font-size: 14px !important;
  line-height: 1.15 !important;
}}

.weekly-metric-delta {{
  font-size: 10px !important;
  line-height: 1.15 !important;
}}

@media only screen and (max-width: 767px) {{
  .weekly-metrics-grid {{
    width: 100% !important;
    font-size: 0 !important;
    line-height: 0 !important;
    white-space: normal !important;
  }}

  .weekly-metric-box {{
    display: inline-block !important;
    width: 49% !important;
    max-width: 49% !important;
    padding: 3px !important;
    vertical-align: top !important;
    box-sizing: border-box !important;
  }}

  .weekly-metric-label {{
    font-size: 10px !important;
    line-height: 1.15 !important;
  }}

  .weekly-metric-value {{
    font-size: 12px !important;
    line-height: 1.15 !important;
  }}

  .weekly-metric-delta {{
    font-size: 9px !important;
    line-height: 1.15 !important;
  }}
}}

  

        /* Desktop/laptop */
        @media only screen and (min-width: 768px) {{
          .email-wrapper {{
             width: 600px !important;
            max-width: 600px !important;
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
            font-size: 16px !important;
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

    <body style="margin:0;padding:0;font-family:Arial,Helvetica,sans-serif;background:#FFFFFF;">
      <table width="100%" cellpadding="0" cellspacing="0" border="0" style="padding:16px 0;">
       <tr><td align="center">
        <table class="email-container" width="600" cellpadding="0" cellspacing="0" border="0" style="width:600px;max-width:600px;background:#FFFFFF;border-collapse:collapse;">
          <tr><td style="background:#5EA68E;padding:18px 24px;color:#FFFFFF;">
  <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="table-layout:fixed;border-collapse:collapse;">
    <tr>
      <td width="110" style="vertical-align:middle;text-align:left;white-space:nowrap;">
        <img
          src="https://res.cloudinary.com/du58s6gdz/image/upload/f_auto,q_auto/output-onlinepngtools_ypplvv"
          alt="Phormula"
          width="40"
          style="display:block; width:40px; max-width:40px; height:auto; border:0;"
        />
      </td>
      <td width="382" align="right" class="top-report-title" style="font-size:16px;line-height:18px;color:#F8EDCF;text-align:right;vertical-align:middle;white-space:nowrap;">
        {html.escape(header_title)}
      </td>
    </tr>
  </table>
</td></tr>
          <tr><td class="content-cell" style="padding:22px 32px 26px;color:#444444;font-size:14px;line-height:1.7;border-left:1px solid #E4E7EC;border-right:1px solid #E4E7EC;text-align:left;">
            <p style="margin:0 0 12px;text-align:left;">Hi <strong>{html.escape(user_name or 'there')}</strong>,</p>
            {summary_html}
            {live_bi_recap_html}
            {portfolio_html}
            {high_alert_html}

            <!-- Recommendation section -->
            <div style="text-align:left;margin-top:22px;">
              <h3 style="color:#37455F;margin:0 0 10px 0;font-size:16px;">
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

            <p style="margin:18px 0 0;text-align:left;">Warm regards,</p>
            <p style="margin:0;text-align:left;"><strong>The Phormula Team</strong></p>
            <p style="margin:0;text-align:left;"><a href="mailto:care@phormula.io" style="color:#37455F;text-decoration:none;">care@phormula.io</a></p>
          </td></tr>
          <tr><td class="note-cell" style="border-top:1px solid #DDDDDD;padding:14px 32px 16px;background:#FFFFFF;font-size:12px;color:#999999;line-height:1.6;border-left:1px solid #E4E7EC;border-right:1px solid #E4E7EC;text-align:left;">
            This email was generated automatically by Phormula. Support: <a href="mailto:care@phormula.io" style="color:#37455F;">care@phormula.io</a>
          </td></tr>
          <tr><td align="center" style="background:#5EA68E;padding:12px 18px;color:#F8EDCF;font-size:12px;line-height:1.5;text-align:center;">
            © 2026 Phormula. All rights reserved.
          </td></tr>
        </table>
       </td></tr>
      </table>
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
        return True
    except Exception as e:
        print(f"[ERROR] Email send failed: {e}")
        traceback.print_exc()
        return False



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




def get_user_email_and_name_by_id(user_id: int):
    """
    Fetch email and name from public.user table.
    """
    try:
        query = text("""
            SELECT email, name
            FROM "user"
            WHERE id = :uid
            LIMIT 1
        """)

        with engine_hist.connect() as conn:
            row = conn.execute(query, {"uid": user_id}).fetchone()

        if not row:
            print(f"[WARN] No user found with id={user_id}")
            return None, None

        user_email = row[0] if isinstance(row, tuple) else row.email
        user_name = row[1] if isinstance(row, tuple) else row.name

        return user_email, (user_name or "there").strip()

    except Exception as e:
        print(f"[ERROR] Failed to fetch user email/name for id={user_id}: {e}")
        return None, None


def get_user_brand_name_by_id(user_id: int):
    """Fetch the user's configured brand name for report email titles."""
    try:
        query = text('SELECT brand_name FROM "user" WHERE id = :uid LIMIT 1')
        with engine_hist.connect() as conn:
            row = conn.execute(query, {"uid": user_id}).fetchone()
        if not row:
            return None
        value = row[0] if isinstance(row, tuple) else row.brand_name
        return (value or "").strip() or None
    except Exception as e:
        print(f"[WARN] Failed to fetch brand name for id={user_id}: {e}")
        return None


def render_monthly_recap_email(recap: dict) -> str:
    """Build an email-safe MonthlyRecapModal-style summary using HTML tables."""
    import html
    import math

    if not recap:
        return ""

    def esc(value):
        return html.escape(str(value if value is not None else ""))

    def safe_float(value):
        try:
            number = float(value)
            return number if math.isfinite(number) else None
        except (TypeError, ValueError):
            return None

    currency = recap.get("currencySymbol", "£")

    def money(value, decimals=0):
        value = safe_float(value)
        return f"{currency}{value:,.{decimals}f}" if value is not None else "—"

    def number(value, decimals=0):
        value = safe_float(value)
        if value is None:
            return "—"
        return f"{value:,.{decimals}f}" if decimals else f"{value:,.0f}"

    month_label = str(recap.get("monthLabel") or "")
    month_parts = month_label.split()
    display_month = (
        f"{month_parts[0][:3]}'{month_parts[-1][-2:]}"
        if len(month_parts) >= 2 else month_label
    )

    # Target & Objectives
    actual = safe_float(recap.get("actual"))
    target = safe_float(recap.get("target"))
    has_target = target is not None and target > 0
    achievement = (actual or 0) / target * 100 if has_target else None
    achievement_text = f"{achievement:.1f}%" if achievement is not None else "—"
    progress_width = max(0, min(100, achievement or 0))

    objectives = recap.get("objectives") or []
    objective_html = "".join(
        f"""<span style="display:inline-block;background:#F2F4F7;color:#37455F;
        padding:5px 9px;border-radius:14px;font-size:10px;margin:3px;text-transform:capitalize;">
        <span style="color:#5EA68E;">●</span> {esc(item)}</span>"""
        for item in objectives
    )

    target_html = f"""
    <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0"
           style="border:1px solid #E6ECEF;border-radius:12px;margin:12px 0 10px;background:#FFFFFF;">
      <tr><td colspan="3" align="center" style="padding:12px 8px 8px;font-size:16px;font-weight:bold;color:#37455F;">
        Target and Objectives
      </td></tr>
      <tr>
        <td width="35%" align="center" valign="middle" style="padding:8px 3px;">
          <div style="font-size:11px;color:#50627A;"><span style="color:#ED9F50;">●</span> Net sales</div>
          <div style="font-size:19px;font-weight:bold;color:#37455F;">{money(actual)}</div>
        </td>
        <td width="30%" align="center" valign="middle" style="padding:8px 2px;">
          <div style="font-size:22px;font-weight:bold;color:#37455F;">{achievement_text}</div>
          <div style="font-size:10px;color:#50627A;">Target Achieved</div>
          <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="margin-top:7px;">
            <tr><td height="6" style="background:#E7F1EC;border-radius:5px;font-size:1px;line-height:1px;">
              <div style="height:6px;width:{progress_width:.1f}%;background:#ED9F50;border-radius:5px;font-size:1px;line-height:1px;">&nbsp;</div>
            </td></tr>
          </table>
        </td>
        <td width="35%" align="center" valign="middle" style="padding:8px 3px;">
          <div style="font-size:11px;color:#50627A;"><span style="color:#5EA68E;">●</span> Target</div>
          <div style="font-size:19px;font-weight:bold;color:#37455F;">{money(target) if has_target else 'Not set'}</div>
        </td>
      </tr>
      <tr><td colspan="3" align="center" style="padding:3px 6px 11px;">{objective_html}</td></tr>
    </table>
    """

    # Six KPI cards, in the exact order used by MonthlyRecapModal.
    metric_order = ["units", "asp", "net sales", "marketplace fees", "tacos", "cm2 profit"]
    accent_map = {
        "units": "#FDD36F", "asp": "#ED9F50", "net sales": "#75BBDA",
        "marketplace fees": "#B75A5A", "tacos": "#3A8EA4", "cm2 profit": "#B8C78C",
    }
    metrics = recap.get("metrics") or []
    metrics = sorted(
        metrics,
        key=lambda item: metric_order.index(str(item.get("label", "")).strip().lower())
        if str(item.get("label", "")).strip().lower() in metric_order else len(metric_order),
    )[:6]

    metric_rows = []
    for row_start in range(0, len(metrics), 3):
        cells = []
        for metric in metrics[row_start:row_start + 3]:
            label = str(metric.get("label") or "")
            key = label.strip().lower()
            value = metric.get("value")
            previous = metric.get("previous")
            delta = safe_float(metric.get("deltaPct"))
            is_currency = key in {"asp", "net sales", "marketplace fees", "cm2 profit"}
            if key == "tacos":
                taco_value = safe_float(value)
                formatted_value = f"{taco_value:.2f}%" if taco_value is not None else "—"
            else:
                formatted_value = money(value, 2) if key == "asp" else money(value) if is_currency else number(value)
            accent = accent_map.get(key, esc(metric.get("accent") or "#75BBDA"))

            delta_html = ""
            if delta is not None:
                inverse = bool(metric.get("inverseDelta", False)) or key in {
                    "marketplace fees", "amazon fees", "tacos", "cost of ads",
                    "cost of advertisement",
                }
                good = delta <= 0 if inverse else delta >= 0
                delta_color = "#12B76A" if good else "#F04438"
                arrow = "▲" if delta >= 0 else "▼"
                delta_html = f"""<span style="font-size:10px;font-weight:bold;color:{delta_color};white-space:nowrap;">
                    {arrow} {abs(delta):.2f}%</span>"""

            previous_html = ""
            if previous is not None:
                previous_value = (
                    f"{safe_float(previous):.2f}%"
                    if key == "tacos" and safe_float(previous) is not None
                    else money(previous, 2) if key == "asp"
                    else money(previous) if is_currency else number(previous)
                )
                previous_html = f"""<div style="font-size:10px;color:#667085;margin-top:5px;">
                    Last month: {previous_value}</div>"""

            cells.append(f"""
            <td width="33.33%" valign="top" style="padding:4px 3px;">
              <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0"
                     style="border:1px solid {accent};border-top:4px solid {accent};border-radius:10px;background:#FFFFFF;">
                <tr><td style="padding:9px 8px;">
                  <div style="font-size:11px;color:#50627A;">{esc(label)}</div>
                  <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0"><tr>
                    <td style="font-size:16px;font-weight:bold;color:#37455F;padding-top:4px;">{formatted_value}</td>
                    <td align="right" valign="bottom">{delta_html}</td>
                  </tr></table>

                </td></tr>
              </table>
            </td>
            """)
        while len(cells) < 3:
            cells.append('<td width="33.33%" style="padding:4px 3px;">&nbsp;</td>')
        metric_rows.append("<tr>" + "".join(cells) + "</tr>")

    metrics_html = f"""
    <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="border-collapse:collapse;margin-top:10px;">
      {''.join(metric_rows)}
    </table>
    """

    # Match MonthlyRecapModal's compact SKU table.
    highlights = recap.get("skuHighlights") or []
    top = next((item for item in highlights if item.get("title") == "Top Selling SKU"), None)
    highest_growth = next((item for item in highlights if item.get("title") == "Highest Net Sales Growth"), None)
    identity = lambda item: (str(item.get("sku") or "").strip() or str(item.get("name") or "").strip()).lower()
    merge_top_sku = bool(top and highest_growth and identity(top) == identity(highest_growth))
    sku_headers = ["Category", "Product Name", "Units", "ASP", "Net Sales", "ACoS", "CM2 Profit"]
    sku_header_html = "".join(
        f'<th style="padding:6px 4px;background:#5EA68E;color:#FFFFFF;font-size:10px;font-weight:bold;text-align:center;border-right:1px solid #E6ECEF;">{header}</th>'
        for header in sku_headers
    )
    sku_rows = []
    for item in highlights:
        title = item.get("title") or ""
        category = "Lowest Net Sales Growth" if title == "Largest Net Sales Decline" else title
        row = [
            esc(category), esc(item.get("name") or "—"), number(item.get("units")),
            money(item.get("asp"), 2), money(item.get("netSales")),
            f"{safe_float(item.get('acos')):.2f}%" if safe_float(item.get("acos")) is not None else "—",
            money(item.get("cm2Profit")),
        ]
        cells = []
        for idx, value in enumerate(row):
            if merge_top_sku and title == "Highest Net Sales Growth" and idx > 0:
                continue
            rowspan = ' rowspan="2"' if merge_top_sku and title == "Top Selling SKU" and idx > 0 else ""
            cells.append(f'<td{rowspan} style="padding:7px 4px;text-align:center;vertical-align:middle;border:1px solid #E6ECEF;color:#344054;font-size:10px;">{value}</td>')
        sku_rows.append("<tr>" + "".join(cells) + "</tr>")
    sku_html = (
        '<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="margin-top:12px;border:1px solid #E6ECEF;border-radius:12px;border-collapse:separate;border-spacing:0;overflow:hidden;">'
        f'<thead><tr>{sku_header_html}</tr></thead><tbody>{"".join(sku_rows)}</tbody></table>'
        if highlights else '<div style="margin-top:12px;padding:12px;border:1px solid #E6ECEF;border-radius:12px;color:#667085;font-size:11px;">No SKU highlights for this month.</div>'
    )

    return f"""
    <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="margin:12px 0 18px;border-collapse:collapse;background:#FFFFFF;">
      <tr><td style="padding:0 0 3px;">
        <div style="font-size:16px;line-height:1.35;font-weight:bold;color:#37455F;">
          Here's how your <span style="color:#5EA68E;">{esc(display_month)}</span> went
        </div>
      </td></tr>
      <tr><td>{target_html}{metrics_html}{sku_html}</td></tr>
    </table>
    """


def send_email_with_attachment(
    *,
    to_email: str,
    user_name: str = "there",
    subject: str,
    body: str,
    attachment_bytes: bytes,
    attachment_filename: str,
    mime_type: str = "application/octet-stream",
    monthly_recap: dict = None,
):
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = f'{os.getenv("MAIL_DEFAULT_SENDER_NAME", "Phormula Care")} <{os.getenv("MAIL_USERNAME")}>'
    msg["To"] = to_email

    # Plain fallback
    msg.set_content(body)

    safe_attachment_filename = html.escape(str(attachment_filename))

    if not isinstance(monthly_recap, dict):
        raise ValueError("The complete dashboard recap is required to send this email.")

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

      .content-cell {{
        padding: 22px 24px 26px 24px !important;
      }}

      .note-cell {{
        padding: 14px 24px 16px 24px !important;
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
                  <td width="110" style="text-align:left; vertical-align:middle; white-space:nowrap;">
                    <img
                      src="https://res.cloudinary.com/du58s6gdz/image/upload/f_auto,q_auto/output-onlinepngtools_ypplvv"
                      alt="Phormula"
                      width="40"
                      style="display:block; width:40px; max-width:40px; height:auto; border:0;"
                    />
                  </td>

                  <td width="382" align="right" class="top-report-title" style="
                    font-size:16px;
                    line-height:18px;
                    color:#f8edce;
                    text-align:right;
                    vertical-align:middle;
                    white-space:nowrap;
                  ">
                    Phormula Monthly P&amp;L Report
                  </td>
                </tr>
              </table>
            </td>
          </tr>

          <!-- divider -->
          <tr>
            <td style="font-size:1px; line-height:1px;">&nbsp;</td>
          </tr>

          <!-- body -->
          <tr>
            <td class="content-cell" style="
              padding:22px 32px 26px 32px;
              color:#444444;
              font-size:14px;
              line-height:1.7;
              text-align:justify;
              text-justify:inter-word;
              border-left:1px solid #e4e7ec;
              border-right:1px solid #e4e7ec;
            ">
              <p style="margin:0 0 12px 0; text-align:left;">
                Hi <strong>{html.escape(user_name or "there")}</strong>,
              </p>

              {render_monthly_recap_email(monthly_recap) if monthly_recap else ""}

              <p style="margin:0 0 14px 0; text-align:justify; text-justify:inter-word;">
                Your <strong>Amazon SKU-wise monthly report</strong> is ready and attached to this email.
                This report gives you a clean SKU-level view of your monthly performance, helping you review
                sales, deductions, profitability, and operational trends more efficiently.
              </p>

              <p style="margin:0 0 18px 0; text-align:justify; text-justify:inter-word;">
                Please review the attached file at your convenience and use it to track product-level performance,
                identify movement across key metrics, and support faster business decisions.
              </p>

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

def send_daily_inventory_alert_email(
    *,
    to_email: str,
    user_name: str,
    country: str,
    alerts: list,
    dashboard_base_url: str = "https://phormula.io",
) -> bool:
    """Send a country-specific inventory action email with stock and transit context."""
    import traceback
    import math
    import pandas as pd

    if not to_email:
        print("[WARN] Daily inventory alert skipped because recipient email is missing.")
        return False

    country = str(country or "").strip().lower()
    if country not in ("uk", "us"):
        raise ValueError("country must be 'uk' or 'us'")

    def safe_float(value, default=0.0):
        try:
            number = float(value)
            return number if math.isfinite(number) else default
        except (TypeError, ValueError):
            return default

    def find_user_id_by_email(email_address):
        query = text('SELECT id FROM "user" WHERE LOWER(email) = LOWER(:email) LIMIT 1')
        with engine_hist.connect() as conn:
            row = conn.execute(query, {"email": email_address}).fetchone()
        return int(row[0]) if row else None

    def load_policy(user_id):
        defaults = {
            "ship_time_weeks": 0.0,
            "air_time_weeks": 0.0,
            "stock_unit_weeks": 0.0,
        }
        if not user_id:
            return defaults

        query = text("""
            SELECT ship_time_weeks, air_time_weeks, stock_unit_weeks
            FROM public.country_profile
            WHERE user_id = :user_id
              AND LOWER(country) = :country
            ORDER BY id DESC
            LIMIT 1
        """)
        try:
            with engine_hist.connect() as conn:
                row = conn.execute(query, {
                    "user_id": user_id,
                    "country": country,
                }).mappings().first()
            if not row:
                return defaults
            return {
                "ship_time_weeks": safe_float(row.get("ship_time_weeks")),
                "air_time_weeks": safe_float(row.get("air_time_weeks")),
                "stock_unit_weeks": safe_float(row.get("stock_unit_weeks")),
            }
        except Exception as exc:
            print(f"[WARN] Could not load inventory policy for daily email: {exc}")
            return defaults

    def load_latest_inventory_rows(user_id):
        if not user_id:
            return {}

        current_month = datetime.now().strftime("%B").lower()
        current_year = datetime.now().year
        preferred = f"currentinventory_{user_id}_{country}_{current_month}{current_year}_table"

        lookup = text("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
              AND table_name LIKE :pattern
            ORDER BY
              CASE WHEN table_name = :preferred THEN 0 ELSE 1 END,
              table_name DESC
            LIMIT 1
        """)

        try:
            with engine_hist.connect() as conn:
                table_name = conn.execute(lookup, {
                    "pattern": f"currentinventory_{user_id}_{country}_%_table",
                    "preferred": preferred,
                }).scalar()

            if not table_name:
                return {}

            df = pd.read_sql_query(
                text(f'SELECT * FROM public."{table_name}"'),
                engine_hist,
            )
        except Exception as exc:
            print(f"[WARN] Could not load current inventory for daily email: {exc}")
            return {}

        if df.empty:
            return {}

        def first_value(row, names, default=None):
            for name in names:
                if name in row.index:
                    value = row.get(name)
                    if value is not None and not pd.isna(value):
                        return value
            return default

        def num(row, names, default=0.0):
            return safe_float(first_value(row, names, default), default)

        sku_col = "SKU" if "SKU" in df.columns else "sku" if "sku" in df.columns else None
        if not sku_col:
            return {}

        result = {}
        for _, row in df.iterrows():
            sku = str(row.get(sku_col) or "").strip().upper()
            if not sku or sku == "TOTAL":
                continue

            current_inventory = num(
                row,
                ["total_stock", "Total Stock", "Inventory at the end of the month"],
            )
            in_transit = num(
                row,
                ["total_transit", "Total Transit", "In Transit Units"],
            )
            inbound = num(
                row,
                ["inbound_quantity", "Inbound Quantity", "Inventory Inwarded"],
            )
            demand = num(
                row,
                ["Sales Last 30 Days", "sales_last_30_days", "Unit Sales in Last 30 Days"],
            )
            current_cover = num(
                row,
                ["Coverage Ratio (In Months)", "Coverage Ratio (Current Inventory)"],
            )
            future_cover = num(
                row,
                [
                    "Coverage Ratio (Current + Intransit)",
                    "Coverage Ratio (Current + In Transit)",
                ],
            )
            if not future_cover and demand > 0:
                future_cover = (current_inventory + in_transit) / demand

            result[sku] = {
                "product_name": first_value(row, ["Product Name", "product_name"], sku),
                "current_inventory": current_inventory,
                "in_transit": in_transit,
                "inbound": inbound,
                "sales_last_30_days": demand,
                "inventory_coverage_ratio": current_cover,
                "future_coverage_ratio": future_cover,
                "future_stock": current_inventory + in_transit,
            }
        return result

    user_id = find_user_id_by_email(to_email)
    policy = load_policy(user_id)
    inventory_map = load_latest_inventory_rows(user_id)

    ship_weeks = policy["ship_time_weeks"]
    air_weeks = policy["air_time_weeks"]
    buffer_weeks = policy["stock_unit_weeks"]
    sea_required_weeks = ship_weeks + buffer_weeks
    air_required_weeks = air_weeks + buffer_weeks
    sea_required_months = sea_required_weeks / 4.345 if sea_required_weeks else 0.0
    air_required_months = air_required_weeks / 4.345 if air_required_weeks else 0.0

    rows = []
    for alert in list(alerts or []):
        item = dict(alert or {})
        sku_key = str(item.get("sku") or "").strip().upper()
        saved = inventory_map.get(sku_key, {})

        for field in (
            "product_name", "current_inventory", "in_transit", "inbound",
            "sales_last_30_days", "inventory_coverage_ratio",
            "future_coverage_ratio", "future_stock",
        ):
            if item.get(field) in (None, "") and field in saved:
                item[field] = saved[field]

        current_inventory = safe_float(item.get("current_inventory"))
        in_transit = safe_float(item.get("in_transit"))
        inbound = safe_float(item.get("inbound"))
        demand = safe_float(item.get("sales_last_30_days"))
        current_cover = safe_float(item.get("inventory_coverage_ratio"))
        future_stock = safe_float(item.get("future_stock"), current_inventory + in_transit)
        future_cover = safe_float(item.get("future_coverage_ratio"))

        if not future_stock:
            future_stock = current_inventory + in_transit
        if not future_cover and demand > 0:
            future_cover = future_stock / demand

        sea_units_needed = max(0, math.ceil(demand * sea_required_months - future_stock)) if demand else 0
        air_units_needed = max(0, math.ceil(demand * air_required_months - future_stock)) if demand else 0

        if future_cover >= sea_required_months > 0:
            action = (
                "Current stock is low, but current plus in-transit inventory covers "
                "the sea-transit requirement. No additional shipment is required now."
            )
            severity = "Covered by in-transit stock"
            accent = "#12B76A"
        elif future_cover >= air_required_months > 0:
            action = (
                f"Future stock covers the air requirement but not the sea requirement. "
                f"Plan a sea shipment of about {sea_units_needed:,} units."
            )
            severity = "Sea shipment required"
            accent = "#F79009"
        else:
            action = (
                f"Future stock is below the air-transit requirement. "
                f"Arrange about {air_units_needed:,} units by air urgently"
                + (f" and plan {sea_units_needed:,} units for full sea coverage." if sea_units_needed else ".")
            )
            severity = "Urgent air shipment required"
            accent = "#F04438"

        item.update({
            "current_inventory": current_inventory,
            "in_transit": in_transit,
            "inbound": inbound,
            "sales_last_30_days": demand,
            "inventory_coverage_ratio": current_cover,
            "future_stock": future_stock,
            "future_coverage_ratio": future_cover,
            "sea_units_needed": sea_units_needed,
            "air_units_needed": air_units_needed,
            "action_message": action,
            "severity": severity,
            "accent": accent,
        })
        rows.append(item)

    rows.sort(key=lambda x: safe_float(x.get("future_coverage_ratio")))
    country_label = country.upper()
    safe_name = html.escape(str(user_name or "there").strip())
    generated_date = datetime.now().strftime("%d %B %Y")
    dashboard_url = f"{dashboard_base_url.rstrip('/')}/live-dashboard/{country}#current-inventory"

    cards = []
    for row in rows:
        product = html.escape(str(row.get("product_name") or row.get("sku") or "Unknown product"))
        sku = html.escape(str(row.get("sku") or "-"))
        action = html.escape(str(row.get("action_message") or ""))
        severity = html.escape(str(row.get("severity") or "Inventory action required"))
        accent = row.get("accent") or "#F04438"

        def fmt_units(value):
            return f"{safe_float(value):,.0f}"

        def fmt_cover(value):
            return f"{safe_float(value):.2f} months"

        cards.append(f"""
        <div style="border:1px solid #E4E7EC;border-left:4px solid {accent};border-radius:10px;padding:14px 16px;margin-bottom:14px;background:#FFFFFF;">
          <div style="font-size:16px;font-weight:700;color:#344054;">{product}</div>
          <div style="font-size:12px;color:#667085;margin:4px 0 12px 0;">SKU: {sku}</div>

          <table width="100%" cellpadding="0" cellspacing="0" style="border-collapse:collapse;margin-bottom:10px;">
            <tr>
              <td style="padding:7px;border:1px solid #EAECF0;font-size:12px;color:#667085;">Current inventory<br><strong style="color:#344054;">{fmt_units(row.get('current_inventory'))}</strong></td>
              <td style="padding:7px;border:1px solid #EAECF0;font-size:12px;color:#667085;">In transit<br><strong style="color:#344054;">{fmt_units(row.get('in_transit'))}</strong></td>
              <td style="padding:7px;border:1px solid #EAECF0;font-size:12px;color:#667085;">Inbound<br><strong style="color:#344054;">{fmt_units(row.get('inbound'))}</strong></td>
              <td style="padding:7px;border:1px solid #EAECF0;font-size:12px;color:#667085;">Sales last 30 days<br><strong style="color:#344054;">{fmt_units(row.get('sales_last_30_days'))}</strong></td>
            </tr>
            <tr>
              <td colspan="2" style="padding:7px;border:1px solid #EAECF0;font-size:12px;color:#667085;">Current cover<br><strong style="color:#344054;">{fmt_cover(row.get('inventory_coverage_ratio'))}</strong></td>
              <td colspan="2" style="padding:7px;border:1px solid #EAECF0;font-size:12px;color:#667085;">Future cover (current + transit)<br><strong style="color:#344054;">{fmt_cover(row.get('future_coverage_ratio'))}</strong></td>
            </tr>
          </table>

          <div style="font-size:13px;font-weight:700;color:{accent};margin-bottom:5px;">{severity}</div>
          <div style="font-size:13px;color:#475467;line-height:1.6;">{action}</div>
        </div>
        """)

    alert_cards_html = "".join(cards) if cards else """
      <div style="padding:16px;border:1px solid #D1E9E2;background:#F2FAF7;border-radius:10px;color:#376859;font-size:14px;">
        No active high inventory alerts today.
      </div>
    """

    policy_html = f"""
      <div style="background:#F8FAFC;border:1px solid #E4E7EC;border-radius:10px;padding:12px 14px;margin-bottom:18px;font-size:13px;color:#475467;line-height:1.7;">
        <strong style="color:#344054;">Inventory policy:</strong>
        Sea {ship_weeks:g} weeks + buffer {buffer_weeks:g} weeks = {sea_required_weeks:g} weeks ({sea_required_months:.2f} months).<br>
        Air {air_weeks:g} weeks + buffer {buffer_weeks:g} weeks = {air_required_weeks:g} weeks ({air_required_months:.2f} months).
      </div>
    """

    subject = f"[Phormula] {country_label} Inventory Action Alerts — {len(rows)} item(s)"
    html_body = f"""
    <!DOCTYPE html><html><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0"></head>
    <body style="margin:0;padding:0;background:#F8FAFC;font-family:Arial,Helvetica,sans-serif;">
      <table width="100%" cellpadding="0" cellspacing="0" style="padding:18px 0;"><tr><td align="center">
        <table width="680" cellpadding="0" cellspacing="0" style="width:680px;max-width:100%;background:#FFFFFF;border-collapse:collapse;">
          <tr><td style="background:#7FB5A5;padding:18px 24px;color:#F8EDCF;font-size:16px;font-weight:600;">{country_label} Inventory Action Report</td></tr>
          <tr><td style="padding:26px 28px;border-left:1px solid #E4E7EC;border-right:1px solid #E4E7EC;">
            <p style="font-size:15px;color:#344054;line-height:1.7;margin:0 0 8px 0;">Hi <strong>{safe_name}</strong>,</p>
            <p style="font-size:14px;color:#475467;line-height:1.7;margin:0 0 18px 0;">Here are the inventory actions for <strong>{generated_date}</strong>. Each item compares current stock and in-transit stock against both air and sea replenishment requirements.</p>
            {policy_html}
            {alert_cards_html}
            <div style="text-align:center;margin-top:22px;"><a href="{dashboard_url}" style="display:inline-block;background:#37455F;color:#F8EDCF;text-decoration:none;padding:10px 22px;border-radius:8px;font-size:14px;font-weight:700;">View {country_label} Inventory</a></div>
          </td></tr>
          <tr><td align="center" style="background:#7FB5A5;padding:12px 18px;color:#F8EDCF;font-size:12px;">© 2026 Phormula. All rights reserved.</td></tr>
        </table>
      </td></tr></table>
    </body></html>
    """

    msg = Message(
        subject,
        sender=("Phormula Care Team", "care@phormula.io"),
        recipients=[to_email],
    )
    msg.html = html_body

    try:
        mail.send(msg)
        print(f"[INFO] {country_label} inventory action email sent to {to_email}")
        return True
    except Exception as exc:
        print(f"[ERROR] {country_label} inventory action email failed for {to_email}: {exc}")
        traceback.print_exc()
        return False

