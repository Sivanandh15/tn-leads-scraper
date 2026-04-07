"""
Corporate Gifting Lead Scraper — IT Companies across Tamil Nadu
───────────────────────────────────────────────────────────────
TARGET: 30+ IT company leads daily (small & mid-level firms)
CITIES: All 12 Tamil Nadu cities in rotation
SOURCES: JustDial → Sulekha → Naukri → LinkedIn → OpenStreetMap

Strategy:
  1. JustDial (best for Indian SME IT)  — primary source
  2. Sulekha IT categories              — secondary source
  3. Naukri IT hiring signals           — companies actively hiring
  4. LinkedIn HR sweep                  — named contacts at IT firms
  5. OpenStreetMap                      — fallback
  6. Rotate across all 12 TN cities daily
  7. Hard cap: max 3 Education/Hospitality leads (no more school flooding)
  8. Cross-run dedup: same company never appears twice
"""
import os, time, logging
from datetime import datetime, date
import pandas as pd

from google_maps     import scrape_google_maps
from naukri          import scrape_naukri_all_it
from sulekha         import scrape_sulekha
from justdial        import scrape_justdial_it
from linkedin_google import scrape_linkedin_via_google, scrape_all_hr_linkedin
from dedup           import deduplicate, load_master_seen, save_master_seen
from emailer         import send_email_report

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)s  %(message)s")
log = logging.getLogger(__name__)

MASTER_CSV      = "leads/master_leads.csv"
LEADS_DIR       = "leads"
MIN_PHONE_LEADS = 30

# All Tamil Nadu cities — rotated daily
ALL_TN_CITIES = [
    "Chennai", "Coimbatore", "Madurai", "Trichy", "Salem",
    "Tiruppur", "Vellore", "Erode", "Tirunelveli", "Thoothukudi",
    "Dindigul", "Thanjavur",
]

# Primary IT categories for JustDial and Sulekha
IT_SULEKHA_CATEGORIES = [
    "it-companies",
    "software-companies",
    "software-development-companies",
    "web-design-companies",
    "mobile-app-development",
    "digital-marketing-agencies",
    "bpo-companies",
    "erp-software-companies",
]

# OSM queries — IT focused
IT_OSM_QUERIES = [
    "it company",
    "software company",
    "bpo company",
    "digital marketing",
    "web design",
]

# Non-IT fallback (used only when IT leads < MIN_PHONE_LEADS)
FALLBACK_SULEKHA_CATEGORIES = [
    "manufacturing-companies",
    "real-estate-agents",
    "construction-companies",
    "logistics-companies",
    "automobile-dealers",
    "advertising-agencies",
]

# Industries we NEVER want flooding the results
LOW_PRIORITY_INDUSTRIES = {"Education", "Hospitality"}
MAX_LOW_PRIORITY = 3


def is_it_company(lead: dict) -> bool:
    return "IT" in lead.get("industry", "") or "Tech" in lead.get("industry", "")


def has_phone(lead: dict) -> bool:
    digits = "".join(c for c in str(lead.get("phone", "")) if c.isdigit())
    return (len(digits) == 10 and digits[0] in "6789") or len(digits) >= 7


def is_low_priority(lead: dict) -> bool:
    return lead.get("industry", "") in LOW_PRIORITY_INDUSTRIES


def scrape_it_city(city: str, max_pages: int = 5) -> list[dict]:
    """Scrape IT companies for a single city from all sources."""
    city_leads = []

    # 1. JustDial — best source for Indian SME IT
    log.info(f"  [JustDial] {city}...")
    try:
        jd_leads = scrape_justdial_it(city, max_pages=max_pages)
        city_leads.extend(jd_leads)
        log.info(f"  JustDial/{city}: {len(jd_leads)} leads")
    except Exception as e:
        log.error(f"JustDial failed for {city}: {e}")
    time.sleep(3)

    # 2. Sulekha IT categories
    log.info(f"  [Sulekha] {city}...")
    for cat in IT_SULEKHA_CATEGORIES:
        try:
            leads = scrape_sulekha(cat, city, max_pages=max_pages)
            city_leads.extend(leads)
        except Exception as e:
            log.error(f"Sulekha/{cat}/{city}: {e}")
        time.sleep(2)

    # 3. OpenStreetMap (fallback — limited in India)
    log.info(f"  [OSM] {city}...")
    for query in IT_OSM_QUERIES:
        try:
            leads = scrape_google_maps(query, city, max_results=200)
            city_leads.extend(leads)
        except Exception as e:
            log.error(f"OSM/{query}/{city}: {e}")
        time.sleep(2)

    # Filter: only keep IT companies + remove schools/hospitals
    it_leads = [l for l in city_leads if not is_low_priority(l)]
    unique   = deduplicate(it_leads)
    log.info(f"  {city} total: {len(unique)} IT leads")
    return unique


def main():
    run_date  = datetime.now().strftime("%Y-%m-%d")
    day_index = date.today().toordinal()
    os.makedirs(LEADS_DIR, exist_ok=True)
    log.info(f"=== TN IT Lead Scrape: {run_date} ===")

    master_seen = load_master_seen(MASTER_CSV)
    log.info(f"Master history: {len(master_seen)} companies already seen")

    # ── Pick cities for today ───────────────────────────────────────────────
    # Always scrape Chennai + Coimbatore (biggest IT hubs in TN)
    # Rotate 2 additional cities daily
    rotating_1 = ALL_TN_CITIES[(day_index) % len(ALL_TN_CITIES)]
    rotating_2 = ALL_TN_CITIES[(day_index + 1) % len(ALL_TN_CITIES)]
    # Avoid repeating Chennai/Coimbatore in rotation
    today_cities = ["Chennai", "Coimbatore"]
    for c in [rotating_1, rotating_2]:
        if c not in today_cities:
            today_cities.append(c)

    log.info(f"Today's cities: {today_cities}")

    all_leads = []

    # ── Phase 1: IT scrape per city ─────────────────────────────────────────
    for city in today_cities:
        leads = scrape_it_city(city, max_pages=5)
        all_leads.extend(leads)
        time.sleep(3)

    # ── Phase 2: Naukri IT hiring signals (companies actively hiring = active budget) ─
    log.info("=== Naukri IT hiring sweep ===")
    try:
        naukri_leads = scrape_naukri_all_it(today_cities, max_pages=2)
        all_leads.extend(naukri_leads)
        log.info(f"Naukri: {len(naukri_leads)} IT companies hiring")
    except Exception as e:
        log.error(f"Naukri sweep failed: {e}")

    # ── Phase 3: LinkedIn HR contacts at IT companies ───────────────────────
    log.info("=== LinkedIn IT HR sweep ===")
    try:
        linkedin_leads = scrape_all_hr_linkedin(max_per_query=5)
        all_leads.extend(linkedin_leads)
        log.info(f"LinkedIn: {len(linkedin_leads)} IT HR contacts")
    except Exception as e:
        log.error(f"LinkedIn sweep failed: {e}")

    # ── Phase 4: Expansion — if still below target, add more cities ─────────
    phone_count = sum(1 for l in all_leads if has_phone(l) and
                      _norm(l.get("company_name","")) not in master_seen)

    if phone_count < MIN_PHONE_LEADS:
        log.info(f"Only {phone_count} phone leads — expanding to more cities...")
        extra_cities = [c for c in ALL_TN_CITIES if c not in today_cities]
        for city in extra_cities[:4]:  # up to 4 more cities
            leads = scrape_it_city(city, max_pages=3)
            all_leads.extend(leads)
            phone_count = sum(1 for l in all_leads if has_phone(l))
            if phone_count >= MIN_PHONE_LEADS:
                log.info(f"Target reached after adding {city}")
                break
            time.sleep(3)

    # ── Phase 5: Fallback — add non-IT leads if still short ─────────────────
    if sum(1 for l in all_leads if has_phone(l)) < MIN_PHONE_LEADS:
        log.info("Still short — adding non-IT fallback categories...")
        for cat in FALLBACK_SULEKHA_CATEGORIES:
            leads = scrape_sulekha(cat, "Chennai", max_pages=3)
            all_leads.extend([l for l in leads if not is_low_priority(l)])
            if sum(1 for l in all_leads if has_phone(l)) >= MIN_PHONE_LEADS:
                break
            time.sleep(2)

    # ── Phase 6: Cross-run dedup + block already-seen companies ─────────────
    deduped   = deduplicate(all_leads)
    new_leads = [l for l in deduped if _norm(l.get("company_name","")) not in master_seen]
    log.info(f"After cross-run dedup: {len(new_leads)} new leads")

    # ── Phase 7: Cap low-priority industries (Education, Hospitality) ────────
    low_pri  = [l for l in new_leads if is_low_priority(l)][:MAX_LOW_PRIORITY]
    high_pri = [l for l in new_leads if not is_low_priority(l)]
    new_leads = high_pri + low_pri

    phone_leads    = [l for l in new_leads if has_phone(l)]
    no_phone_leads = [l for l in new_leads if not has_phone(l)]

    log.info(f"  IT leads:    {sum(1 for l in new_leads if is_it_company(l))}")
    log.info(f"  With phone:  {len(phone_leads)}")
    log.info(f"  Named HR:    {sum(1 for l in new_leads if l.get('contact_name'))}")

    if len(phone_leads) < MIN_PHONE_LEADS:
        log.warning(f"Only {len(phone_leads)} phone leads — below target of {MIN_PHONE_LEADS}")
    else:
        log.info(f"Phone target met: {len(phone_leads)} >= {MIN_PHONE_LEADS} ✓")

    # ── Phase 8: Sort — LinkedIn HR contacts first, then JustDial, then others ─
    source_priority = {
        "LinkedIn (via Google)": 1,
        "JustDial":              2,
        "Naukri (hiring signal)":3,
        "Sulekha":               4,
        "OpenStreetMap":         5,
    }
    phone_leads.sort(key=lambda l: (
        0 if l.get("contact_name") else 1,           # named HR contacts first
        source_priority.get(l.get("source",""), 9),  # by source quality
        0 if is_it_company(l) else 1,                # IT companies first
    ))
    final_leads = phone_leads + no_phone_leads

    if not final_leads:
        log.warning("No new leads today.")
        return

    # ── Phase 9: Save CSV ────────────────────────────────────────────────────
    csv_path = f"{LEADS_DIR}/leads_{run_date}.csv"
    df = pd.DataFrame(final_leads)
    for col in ["company_name","contact_name","designation","phone",
                "email","website","address","industry","source","notes"]:
        if col not in df.columns:
            df[col] = ""
    df[["company_name","contact_name","designation","phone",
        "email","website","address","industry","source","notes"]].to_csv(csv_path, index=False)

    log.info(f"Saved → {csv_path}  ({len(final_leads)} total leads)")
    log.info(f"  IT companies:    {df[df.industry.str.contains('IT|Tech', na=False)].shape[0]}")
    log.info(f"  Phone leads:     {df[df.phone.str.len() >= 7].shape[0]}")
    log.info(f"  Named contacts:  {df[df.contact_name.notna() & (df.contact_name != '')].shape[0]}")

    # ── Phase 10: Update master + send email ─────────────────────────────────
    save_master_seen(MASTER_CSV, final_leads, master_seen)
    send_email_report(final_leads, csv_path, run_date)
    log.info("=== Done ===")


def _norm(name: str) -> str:
    import re
    name = name.lower().strip()
    name = re.sub(r"\b(pvt|ltd|private|limited|llp|inc|corp|co|&)\b", "", name)
    name = re.sub(r"[^a-z0-9 ]", "", name)
    return re.sub(r"\s+", " ", name).strip()


if __name__ == "__main__":
    main()
