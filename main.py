"""
Corporate Gifting Lead Scraper — Main Orchestrator
- 50 leads from Chennai, 30 from Coimbatore, 10 from one rotating city = ~100/day
- ONLY leads with a phone number are kept
- Priority order: IT/Tech → Corporate/Manufacturing → Pharma → others
- Sulekha runs first (best source for phone numbers)
- Master CSV dedup: same company never emailed twice
"""
import os, time, logging
from datetime import datetime, date
import pandas as pd

from google_maps     import scrape_google_maps
from naukri          import scrape_naukri
from sulekha         import scrape_sulekha
from linkedin_google import scrape_linkedin_via_google
from dedup           import deduplicate, load_master_seen, save_master_seen
from emailer         import send_email_report

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)s  %(message)s")
log = logging.getLogger(__name__)

MASTER_CSV = "leads/master_leads.csv"
LEADS_DIR  = "leads"

CITY_CAPS = {
    "Chennai":    50,
    "Coimbatore": 30,
    "OTHER":      10,
}

ROTATING_CITIES = [
    "Madurai", "Trichy", "Salem", "Tiruppur",
    "Vellore", "Erode", "Tirunelveli", "Thoothukudi",
    "Dindigul", "Thanjavur",
]

# (sulekha_category, osm_query, priority)  1=highest
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
    ("pharmaceutical-companies",  "pharma company",         3),
    ("hospitals",                 "hospital",               3),
    ("banks",                     "bank",                   4),
    ("insurance-companies",       "insurance company",      4),
    ("fmcg-companies",            "fmcg company",           4),
]


def has_phone(lead):
    phone = str(lead.get("phone", "")).strip()
    digits = "".join(c for c in phone if c.isdigit())
    return len(digits) >= 7


def scrape_city(city, cap):
    city_leads = []
    for sulekha_cat, osm_query, priority in sorted(INDUSTRIES, key=lambda x: x[2]):
        if len(city_leads) >= cap * 3:   # gather 3x then dedup+cap
            break
        leads = scrape_sulekha(sulekha_cat, city, max_pages=5)
        phone_leads = [l for l in leads if has_phone(l)]
        city_leads.extend(phone_leads)
        log.info(f"  Sulekha [{sulekha_cat}/{city}] {len(leads)} total → {len(phone_leads)} with phone")
        time.sleep(2)
        leads = scrape_google_maps(osm_query, city, max_results=200)
        phone_leads = [l for l in leads if has_phone(l)]
        city_leads.extend(phone_leads)
        log.info(f"  OSM [{osm_query}/{city}] {len(leads)} total → {len(phone_leads)} with phone")
        time.sleep(2)
    unique = deduplicate(city_leads)[:cap]
    log.info(f"  {city} final: {len(unique)} leads")
    return unique


def main():
    run_date = datetime.now().strftime("%Y-%m-%d")
    os.makedirs(LEADS_DIR, exist_ok=True)
    log.info(f"=== TN Lead Scrape: {run_date} ===")

    day_index     = date.today().toordinal() % len(ROTATING_CITIES)
    rotating_city = ROTATING_CITIES[day_index]
    log.info(f"Rotating city today: {rotating_city}")

    master_seen = load_master_seen(MASTER_CSV)
    log.info(f"Master history: {len(master_seen)} already scraped")

    all_leads = []
    all_leads.extend(scrape_city("Chennai",    CITY_CAPS["Chennai"]))
    all_leads.extend(scrape_city("Coimbatore", CITY_CAPS["Coimbatore"]))
    all_leads.extend(scrape_city(rotating_city, CITY_CAPS["OTHER"]))

    for q in ["HR Manager IT company Chennai",
              "Admin Manager manufacturing Coimbatore",
              "Procurement Manager pharma Chennai"]:
        all_leads.extend(scrape_linkedin_via_google(q, max_results=5))
        time.sleep(5)

    new_leads = [
        l for l in deduplicate(all_leads)
        if _norm(l.get("company_name","")) not in master_seen
    ]
    log.info(f"New leads after cross-run dedup: {len(new_leads)}")

    priority_map = {"IT / Tech":1,"Manufacturing / Textile":2,
                    "Real Estate / Construction":2,"Logistics / Transport":2,
                    "Pharma / Healthcare":3,"BFSI":4,"Other":5}
    new_leads.sort(key=lambda l:(
        0 if has_phone(l) else 1,
        priority_map.get(l.get("industry","Other"),5),
    ))

    if not new_leads:
        log.warning("No new leads today.")
        return

    csv_path = f"{LEADS_DIR}/leads_{run_date}.csv"
    df = pd.DataFrame(new_leads)
    for col in ["company_name","contact_name","designation","phone",
                "email","website","address","industry","source","notes"]:
        if col not in df.columns:
            df[col] = ""
    df[["company_name","contact_name","designation","phone",
        "email","website","address","industry","source","notes"]].to_csv(csv_path, index=False)
    log.info(f"Saved → {csv_path}  (with phone: {df[df.phone.notna() & (df.phone!='')].shape[0]})")

    save_master_seen(MASTER_CSV, new_leads, master_seen)
    send_email_report(new_leads, csv_path, run_date)
    log.info("=== Done ===")


def _norm(name):
    import re
    name = name.lower().strip()
    name = re.sub(r"\b(pvt|ltd|private|limited|llp|inc|corp|co|&)\b","",name)
    name = re.sub(r"[^a-z0-9 ]","",name)
    return re.sub(r"\s+"," ",name).strip()


if __name__ == "__main__":
    main()
