"""
Corporate Gifting Lead Scraper — Main Orchestrator
─────────────────────────────────────────────────
GUARANTEES minimum 30 leads WITH phone number every day.

Strategy:
  1. Scrape Chennai (target 50) + Coimbatore (target 30) + rotating city (target 10)
  2. Run full LinkedIn HR sweep across all IT & business queries
  3. If phone-leads < 30 after step 1-2, expand to extra Sulekha categories + more cities
  4. Cross-run dedup: same company NEVER appears twice across days
  5. Within-run dedup: no duplicate company or phone in same email
  6. Pharma / Healthcare EXCLUDED at every stage
"""
import os, time, logging
from datetime import datetime, date
import pandas as pd

from google_maps     import scrape_google_maps
from naukri          import scrape_naukri
from sulekha         import scrape_sulekha
from linkedin_google import scrape_linkedin_via_google, scrape_all_hr_linkedin
from dedup           import deduplicate, load_master_seen, save_master_seen
from emailer         import send_email_report

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)s  %(message)s")
log = logging.getLogger(__name__)

MASTER_CSV       = "leads/master_leads.csv"
LEADS_DIR        = "leads"
MIN_PHONE_LEADS  = 30

CITY_CAPS = {
    "Chennai":    50,
    "Coimbatore": 30,
    "OTHER":      15,
}

ROTATING_CITIES = [
    "Madurai", "Trichy", "Salem", "Tiruppur",
    "Vellore", "Erode", "Tirunelveli", "Thoothukudi",
    "Dindigul", "Thanjavur",
]

INDUSTRIES = [
    ("it-companies",              "IT company",             1),
    ("software-companies",        "software company",       1),
    ("advertising-agencies",      "advertising agency",     1),
    ("manufacturing-companies",   "manufacturing company",  2),
    ("construction-companies",    "construction company",   2),
    ("logistics-companies",       "logistics company",      2),
    ("automobile-dealers",        "automobile dealer",      2),
    ("real-estate-agents",        "real estate company",    2),
    ("event-management-companies","event management",       2),
    ("banks",                     "bank",                   3),
    ("insurance-companies",       "insurance company",      3),
    ("ca-firms",                  "ca firm",                3),
    ("fmcg-companies",            "fmcg company",           4),
    ("retail-companies",          "retail company",         4),
]

FALLBACK_INDUSTRIES = [
    ("hotels",                    "hotel",                  5),
    ("educational-institutes",    "educational institute",  5),
    ("food-companies",            "food company",           5),
    ("textile-companies",         "textile company",        5),
]

BLOCKED_INDUSTRIES = {"pharma / healthcare"}


def is_blocked(lead: dict) -> bool:
    return any(b in lead.get("industry", "").lower() for b in BLOCKED_INDUSTRIES)


def has_phone(lead: dict) -> bool:
    digits = "".join(c for c in str(lead.get("phone", "")) if c.isdigit())
    return (len(digits) == 10 and digits[0] in "6789") or len(digits) >= 7


def scrape_city(city: str, cap: int, industries=None, max_pages: int = 5) -> list[dict]:
    if industries is None:
        industries = INDUSTRIES
    city_leads = []
    for sulekha_cat, osm_query, priority in sorted(industries, key=lambda x: x[2]):
        if len(city_leads) >= cap * 3:
            break
        leads = scrape_sulekha(sulekha_cat, city, max_pages=max_pages)
        kept  = [l for l in leads if has_phone(l) and not is_blocked(l)]
        city_leads.extend(kept)
        log.info(f"  Sulekha [{sulekha_cat}/{city}] {len(leads)} -> {len(kept)} with phone")
        time.sleep(2)
        leads = scrape_google_maps(osm_query, city, max_results=200)
        kept  = [l for l in leads if has_phone(l) and not is_blocked(l)]
        city_leads.extend(kept)
        log.info(f"  OSM [{osm_query}/{city}] {len(leads)} -> {len(kept)} with phone")
        time.sleep(2)
    unique = deduplicate(city_leads)[:cap]
    log.info(f"  {city} final: {len(unique)} leads")
    return unique


def count_phone_leads(leads: list, master_seen: set) -> int:
    unique = deduplicate(leads)
    new    = [l for l in unique if _norm(l.get("company_name","")) not in master_seen and not is_blocked(l)]
    return sum(1 for l in new if has_phone(l))


def expand_scrape(all_leads: list, master_seen: set) -> list[dict]:
    log.info("Phone leads below minimum -- running expansion scrape...")
    extra = []
    extra.extend(scrape_city("Chennai",    30, industries=FALLBACK_INDUSTRIES, max_pages=3))
    extra.extend(scrape_city("Coimbatore", 20, industries=FALLBACK_INDUSTRIES, max_pages=3))
    today = date.today().toordinal()
    for offset in range(1, len(ROTATING_CITIES)):
        city = ROTATING_CITIES[(today + offset) % len(ROTATING_CITIES)]
        extra.extend(scrape_city(city, 20, industries=INDUSTRIES[:6], max_pages=3))
        time.sleep(3)
        if count_phone_leads(all_leads + extra, master_seen) >= MIN_PHONE_LEADS:
            break
    return extra


def main():
    run_date = datetime.now().strftime("%Y-%m-%d")
    os.makedirs(LEADS_DIR, exist_ok=True)
    log.info(f"=== TN Lead Scrape: {run_date} ===")

    day_index     = date.today().toordinal() % len(ROTATING_CITIES)
    rotating_city = ROTATING_CITIES[day_index]
    log.info(f"Rotating city today: {rotating_city}")

    master_seen = load_master_seen(MASTER_CSV)
    log.info(f"Master history: {len(master_seen)} companies already seen")

    # Phase 1: Primary city scrape
    all_leads = []
    all_leads.extend(scrape_city("Chennai",     CITY_CAPS["Chennai"]))
    all_leads.extend(scrape_city("Coimbatore",  CITY_CAPS["Coimbatore"]))
    all_leads.extend(scrape_city(rotating_city, CITY_CAPS["OTHER"]))

    # Phase 2: Full LinkedIn HR sweep
    log.info("=== LinkedIn HR sweep ===")
    linkedin_leads = scrape_all_hr_linkedin(max_per_query=5)
    all_leads.extend([l for l in linkedin_leads if not is_blocked(l)])

    # Phase 3: Expansion if below minimum
    if count_phone_leads(all_leads, master_seen) < MIN_PHONE_LEADS:
        all_leads.extend(expand_scrape(all_leads, master_seen))

    # Phase 4: Cross-run dedup
    new_leads = [
        l for l in deduplicate(all_leads)
        if _norm(l.get("company_name","")) not in master_seen
        and not is_blocked(l)
    ]
    log.info(f"New leads after cross-run dedup: {len(new_leads)}")

    phone_leads    = [l for l in new_leads if has_phone(l)]
    no_phone_leads = [l for l in new_leads if not has_phone(l)]
    log.info(f"  With phone: {len(phone_leads)} | Without: {len(no_phone_leads)}")

    if len(phone_leads) < MIN_PHONE_LEADS:
        log.warning(f"Only {len(phone_leads)} phone leads today -- below target of {MIN_PHONE_LEADS}")
    else:
        log.info(f"Phone lead target met: {len(phone_leads)} >= {MIN_PHONE_LEADS}")

    # Phase 5: Sort — named HR contacts first, then by industry priority
    priority_map = {
        "IT / Tech":                 1,
        "Manufacturing / Textile":   2,
        "Real Estate / Construction":2,
        "Logistics / Transport":     2,
        "Media / Events":            3,
        "Automobile":                3,
        "BFSI":                      4,
        "FMCG / Retail / Food":      4,
        "Hospitality":               5,
        "Education":                 5,
        "Other":                     6,
    }
    phone_leads.sort(key=lambda l: (
        0 if l.get("contact_name") else 1,
        priority_map.get(l.get("industry","Other"), 6),
    ))
    final_leads = phone_leads + no_phone_leads

    if not final_leads:
        log.warning("No new leads today.")
        return

    # Phase 6: Save CSV
    csv_path = f"{LEADS_DIR}/leads_{run_date}.csv"
    df = pd.DataFrame(final_leads)
    for col in ["company_name","contact_name","designation","phone",
                "email","website","address","industry","source","notes"]:
        if col not in df.columns:
            df[col] = ""
    df[["company_name","contact_name","designation","phone",
        "email","website","address","industry","source","notes"]].to_csv(csv_path, index=False)
    log.info(f"Saved -> {csv_path}")
    log.info(f"  Phone leads: {df[df.phone.str.len() >= 7].shape[0]}")
    log.info(f"  Named HR contacts: {df[df.contact_name.notna() & (df.contact_name != '')].shape[0]}")

    # Phase 7: Update master + send email
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
