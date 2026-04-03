"""
Email report sender via Gmail SMTP.
Sends a styled HTML email with lead table + CSV attachment.

Setup:
  1. Enable 2-Factor Auth on your Gmail account
  2. Go to https://myaccount.google.com/apppasswords
  3. Generate an app password for "Mail"
  4. Add GMAIL_USER and GMAIL_APP_PASSWORD as GitHub Secrets
"""
import os, smtplib, logging
from email.mime.multipart import MIMEMultipart
from email.mime.text      import MIMEText
from email.mime.base      import MIMEBase
from email                import encoders
from datetime             import datetime

log = logging.getLogger(__name__)

# ── Env vars (set as GitHub Secrets) ─────────────────────────────────────────
GMAIL_USER     = os.environ.get("GMAIL_USER", "")
GMAIL_PASSWORD = os.environ.get("GMAIL_APP_PASSWORD", "")
TO_EMAIL       = os.environ.get("TO_EMAIL", GMAIL_USER)


def send_email_report(leads: list[dict], csv_path: str, run_date: str):
    """Send HTML email with lead table + CSV attachment."""
    if not GMAIL_USER or not GMAIL_PASSWORD:
        log.error("GMAIL_USER or GMAIL_APP_PASSWORD not set. Cannot send email.")
        return

    subject = f"🎁 {len(leads)} New Corporate Gifting Leads — {run_date}"
    html    = _build_html(leads, run_date)

    msg = MIMEMultipart("mixed")
    msg["Subject"] = subject
    msg["From"]    = GMAIL_USER
    msg["To"]      = TO_EMAIL

    # HTML body
    msg.attach(MIMEText(html, "html"))

    # CSV attachment
    try:
        with open(csv_path, "rb") as f:
            part = MIMEBase("application", "octet-stream")
            part.set_payload(f.read())
            encoders.encode_base64(part)
            part.add_header("Content-Disposition",
                            f'attachment; filename="leads_{run_date}.csv"')
            msg.attach(part)
    except FileNotFoundError:
        log.warning(f"CSV file not found: {csv_path}")

    # Send via Gmail SMTP
    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(GMAIL_USER, GMAIL_PASSWORD)
            server.sendmail(GMAIL_USER, TO_EMAIL, msg.as_string())
        log.info(f"Email sent to {TO_EMAIL} with {len(leads)} leads")
    except Exception as e:
        log.error(f"Email send failed: {e}")


# ── HTML builder ──────────────────────────────────────────────────────────────

def _build_html(leads: list[dict], run_date: str) -> str:
    rows = ""
    for i, lead in enumerate(leads):
        bg    = "#ffffff" if i % 2 == 0 else "#f8f9fa"
        phone = lead.get("phone", "")
        wa    = f'<a href="https://wa.me/91{phone}" style="color:#25D366">WhatsApp</a>' if len(phone) == 10 else ""
        rows += f"""
        <tr style="background:{bg}">
          <td style="{TD}">{i+1}</td>
          <td style="{TD}"><strong>{lead.get('company_name','')}</strong></td>
          <td style="{TD}">{lead.get('contact_name','')}</td>
          <td style="{TD}">{lead.get('designation','')}</td>
          <td style="{TD}">{phone} {wa}</td>
          <td style="{TD}">{lead.get('email','')}</td>
          <td style="{TD}"><span style="background:{_industry_color(lead.get('industry',''))};padding:2px 8px;border-radius:10px;font-size:12px">{lead.get('industry','')}</span></td>
          <td style="{TD}">{lead.get('source','')}</td>
          <td style="{TD}">{lead.get('address','')}</td>
          <td style="{TD};color:#888;font-size:12px">{lead.get('notes','')[:80]}</td>
        </tr>"""

    # Source breakdown
    from collections import Counter
    source_counts = Counter(l.get("source","") for l in leads)
    source_pills  = "".join(
        f'<span style="background:#e8f0fe;color:#1a73e8;padding:3px 10px;border-radius:12px;font-size:13px;margin-right:6px">{src}: <strong>{cnt}</strong></span>'
        for src, cnt in source_counts.most_common()
    )

    industry_counts = Counter(l.get("industry","") for l in leads)
    industry_pills  = "".join(
        f'<span style="background:{_industry_color(ind)};padding:3px 10px;border-radius:12px;font-size:13px;margin-right:6px">{ind}: <strong>{cnt}</strong></span>'
        for ind, cnt in industry_counts.most_common()
    )

    return f"""
<!DOCTYPE html>
<html>
<head><meta charset="UTF-8"></head>
<body style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;color:#333;max-width:1200px;margin:0 auto;padding:20px">

  <div style="background:linear-gradient(135deg,#667eea 0%,#764ba2 100%);color:white;padding:24px 28px;border-radius:12px;margin-bottom:24px">
    <h1 style="margin:0;font-size:22px">🎁 Corporate Gifting Leads — {run_date}</h1>
    <p style="margin:6px 0 0;opacity:.85">{len(leads)} verified leads ready for cold calling</p>
  </div>

  <div style="display:flex;gap:12px;flex-wrap:wrap;margin-bottom:20px">
    <div style="background:#f0f9ff;border:1px solid #bae6fd;border-radius:10px;padding:14px 20px;min-width:120px;text-align:center">
      <div style="font-size:28px;font-weight:700;color:#0369a1">{len(leads)}</div>
      <div style="font-size:13px;color:#64748b">Total Leads</div>
    </div>
    <div style="background:#f0fdf4;border:1px solid #bbf7d0;border-radius:10px;padding:14px 20px;min-width:120px;text-align:center">
      <div style="font-size:28px;font-weight:700;color:#15803d">{sum(1 for l in leads if l.get('phone'))}</div>
      <div style="font-size:13px;color:#64748b">With Phone</div>
    </div>
    <div style="background:#fff7ed;border:1px solid #fed7aa;border-radius:10px;padding:14px 20px;min-width:120px;text-align:center">
      <div style="font-size:28px;font-weight:700;color:#c2410c">{sum(1 for l in leads if l.get('contact_name'))}</div>
      <div style="font-size:13px;color:#64748b">Named Contacts</div>
    </div>
  </div>

  <p style="margin-bottom:8px"><strong>By Source:</strong> {source_pills}</p>
  <p style="margin-bottom:20px"><strong>By Industry:</strong> {industry_pills}</p>

  <div style="overflow-x:auto">
    <table style="width:100%;border-collapse:collapse;font-size:13px">
      <thead>
        <tr style="background:#1e293b;color:white">
          <th style="{TH}">#</th>
          <th style="{TH}">Company</th>
          <th style="{TH}">Contact</th>
          <th style="{TH}">Designation</th>
          <th style="{TH}">Phone</th>
          <th style="{TH}">Email</th>
          <th style="{TH}">Industry</th>
          <th style="{TH}">Source</th>
          <th style="{TH}">Location</th>
          <th style="{TH}">Notes</th>
        </tr>
      </thead>
      <tbody>{rows}</tbody>
    </table>
  </div>

  <div style="background:#f8fafc;border-radius:10px;padding:16px 20px;margin-top:24px;font-size:13px;color:#64748b">
    <strong>Quick Actions:</strong><br>
    • Phone leads with phone numbers first — highest conversion<br>
    • LinkedIn contacts → connect → send catalogue PDF<br>
    • Naukri leads → call the company switchboard and ask for HR<br>
    • Check the attached CSV for full data (importable to Google Sheets / HubSpot)<br><br>
    <em>Auto-generated by your GitHub Actions lead scraper. Next run: next Monday 9:30 AM IST.</em>
  </div>
</body>
</html>"""


TH = "padding:10px 12px;text-align:left;font-weight:500;white-space:nowrap"
TD = "padding:9px 12px;border-bottom:1px solid #e2e8f0;vertical-align:top"


def _industry_color(industry: str) -> str:
    colors = {
        "IT / Tech":           "#dbeafe",
        "Pharma / Healthcare": "#d1fae5",
        "BFSI":                "#fef3c7",
        "Real Estate":         "#ede9fe",
        "FMCG / Retail":       "#fce7f3",
    }
    for key, color in colors.items():
        if key.lower() in (industry or "").lower():
            return color
    return "#f1f5f9"
