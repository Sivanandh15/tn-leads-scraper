"""
Corporate Gifting Lead Scraper — Main Orchestrator
Runs every day at 9:30 AM IST via GitHub Actions.
- Scrapes ALL companies from Tamil Nadu cities (no lead cap)
- Covers all industries relevant to corporate gifting
- Master CSV deduplication: never emails the same company twice
- Runs indefinitely (no end date)
"""
import os, asyncio, time, logging, sys
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

MASTER_CSV  = "leads/master_leads.csv"   # All-time history for cross-run dedup
LEADS_DIR   = "leads"

# ─────────────────────────────────────────────────────────────────────────────
# Industries relevant to corporate gifting (covers every type of B2B company)
# ─────────────────────────────────────────────────────────────────────────────
ALL_INDUSTRIES = [
    # OSM query string            Sulekha category slug           Naukri role
    ("IT company",                "it-companies",                 "hr manager"),
    ("software company",          "it-companies",                 "admin manager"),
    ("pharma company",            "pharmaceutical-companies",     "procurement manager"),
    ("hospital",                  "hospitals",                    "hr manager"),
    ("manufacturing company",     "manufacturing-companies",      "hr manager"),
    ("textile company",           "textile-companies",            "admin manager"),
    ("real estate company",       "real-estate-agents",           "hr manager"),
    ("construction company",      "construction-companies",       "procurement manager"),
    ("bank",                      "banks",                        "hr manager"),
    ("insurance company",         "insurance-companies",          "hr manager"),
    ("logistics company",         "logistics-companies",          "procurement manager"),
    ("hotel",                     "hotels",                       "hr manager"),
    ("automobile dealer",         "automobile-dealers",           "admin manager"),
    ("advertising agency",        "advertising-agencies",         "hr manager"),
    ("event management company",  "event-management-companies",   "admin manager"),
    ("educational institute",     "educational-institutes",       "admin manager"),
    ("fmcg company",              "fmcg-companies",               "procurement manager"),
    ("ca firm",                   "chartered-accountants",        "admin manager"),
    ("food company",              "food-companies",               "hr manager"),
    ("retail company",            "retail-companies",             "hr manager"),
]

# ─────────────────────────────────────────────────────────────────────────────
# Cities — weekly rotation (Chennai gets 2 days since it's biggest)
# weekday: 0=Mon,1=Tue,2=Wed,3=Thu,4=Fri,5=Sat,6=Sun
# ─────────────────────────────────────────────────────────────────────────────
CITY_ROTATION = {
    0: ["Chennai"],                         # Monday   — Chennai pass 1 (industries 0–9)
    1: ["Chennai"],                         # Tuesday  — Chennai pass 2 (industries 10–19)
    2: ["Coimbatore", "Tiruppur"],          # Wednesday
    3: ["Madurai", "Trichy"],               # Thursday
    4: ["Salem", "Erode", "Dindigul"],      # Friday
    5: ["Vellore", "Thanjavur"],            # Saturday
    6: ["Tirunelveli", "Thoothukudi"],      # Sunday
}

# On Mon, use industries 0-9; on Tue use 10-19 (to cover ALL for Chennai)
CHENNAI_INDUSTRY_SLICE = {
    0: ALL_INDUSTRIES[:10],
    1: ALL_INDUSTRIES[10:],
}

LINKEDIN_QUERIES = {
    0: ["HR Manager IT company Chennai",       "Admin Head pharma Chennai",
        "Procurement Manager manufacturing Chennai"],
    1: ["HR Director real estate Chennai",     "Admin Head hotel Chennai",
        "Procurement Manager logistics Chennai"],
    2: ["HR Manager IT company Coimbatore",    "Admin Head textile Tiruppur",
        "HR Director manufacturing Coimbatore"],
    3: ["HR Manager IT Madurai Tamil Nadu",    "Procurement Head manufacturing Trichy",
        "Admin Manager real estate Madurai"],
    4: ["HR Manager manufacturing Salem",      "Admin Head IT Salem",
        "Procurement Manager Erode Tamil Nadu"],
    5: ["HR Manager IT Vellore Tamil Nadu",    "Admin Head manufacturing Thanjavur",
        "HR Director educational institute Vellore"],
    6: ["HR Manager manufacturing Tirunelveli","Admin Head pharma Tirunelveli",
        "Procurement Head Thoothukudi Tamil Nadu"],
}


async def main():
    run_date    = datetime.now().strftime("%Y-%m-%d")
    day_of_week = date.today().weekday()
    os.makedirs(LEADS_DIR, exist_ok=True)

    log.info(f"=== TN Lead Scrape started: {run_date} (weekday={day_of_week}) ===")

    # Load all previously seen company names for cross-run dedup
    master_seen = load_master_seen(MASTER_CSV)
    log.info(f"Master history: {len(master_seen)} companies already scraped before today")

    all_leads = []
    cities    = CITY_ROTATION.get(day_of_week, ["Chennai"])

    # Pick industry list — Chennai has two passes (Mon/Tue) to cover all 20 industries
    if day_of_week in (0, 1) and cities == ["Chennai"]:
        industries = CHENNAI_INDUSTRY_SLICE[day_of_week]
    else:
        industries = ALL_INDUSTRIES  # All industries for every other city

    # ── 1. OpenStreetMap / Overpass (gets the most leads — no key needed) ──
    log.info("Scraping OpenStreetMap via Overpass API…")
    for city in cities:
        for osm_query, _, _ in industries:
            leads = scrape_google_maps(osm_query, city, max_results=500)
            all_leads.extend(leads)
            log.info(f"  Overpass [{osm_query} in {city}] → {len(leads)}")
            time.sleep(2)

    # ── 2. Sulekha (business directory with phone numbers) ──────────────────
    log.info("Scraping Sulekha…")
    for city in cities:
        for _, sulekha_cat, _ in industries:
            leads = scrape_sulekha(sulekha_cat, city, max_pages=10)
            all_leads.extend(leads)
            log.info(f"  Sulekha [{sulekha_cat} in {city}] → {len(leads)}")
            time.sleep(2)

    # ── 3. Naukri (hiring signal — companies with budget) ───────────────────
    log.info("Scraping Naukri…")
    for city in cities:
        for _, _, naukri_role in industries:
            leads = scrape_naukri(naukri_role, city, max_pages=3)
            all_leads.extend(leads)
            time.sleep(2)
    log.info(f"  Naukri total → {len([l for l in all_leads if l.get('source','').startswith('Naukri')])}")

    # ── 4. LinkedIn via Google ───────────────────────────────────────────────
    log.info("Finding LinkedIn profiles via Google…")
    for search in LINKEDIN_QUERIES.get(day_of_week, LINKEDIN_QUERIES[0]):
        leads = scrape_linkedin_via_google(search, max_results=10)
        all_leads.extend(leads)
        log.info(f"  LinkedIn [{search}] → {len(leads)}")
        time.sleep(5)  # be extra polite to Google

    # ── 5. Dedup within today's run ─────────────────────────────────────────
    today_unique = deduplicate(all_leads)
    log.info(f"After today's internal dedup: {len(today_unique)} leads")

    # ── 6. Remove leads already seen in previous runs ───────────────────────
    new_leads = [
        lead for lead in today_unique
        if _normalise_name(lead.get("company_name", "")) not in master_seen
    ]
    log.info(f"After cross-run dedup: {len(new_leads)} genuinely NEW leads today")

    if not new_leads:
        log.warning("No new leads found today. All companies already scraped before.")
        return

    # ── 7. Save today's CSV ─────────────────────────────────────────────────
    csv_path = f"{LEADS_DIR}/leads_{run_date}.csv"
    df = pd.DataFrame(new_leads)
    for col in ["company_name","contact_name","designation","phone",
                "email","website","address","industry","source","notes"]:
        if col not in df.columns:
            df[col] = ""
    df = df[["company_name","contact_name","designation","phone",
             "email","website","address","industry","source","notes"]]
    df.to_csv(csv_path, index=False)
    log.info(f"Saved → {csv_path}")

    # ── 8. Update master CSV (append new companies) ─────────────────────────
    save_master_seen(MASTER_CSV, new_leads, master_seen)
    log.info(f"Master CSV updated with {len(new_leads)} new entries")

    # ── 9. Send email ────────────────────────────────────────────────────────
    send_email_report(new_leads, csv_path, run_date)
    log.info("=== Done ===")


def _normalise_name(name: str) -> str:
    import re
    name = name.lower().strip()
    name = re.sub(r"\b(pvt|ltd|private|limited|llp|inc|corp|co|&)\b", "", name)
    name = re.sub(r"[^a-z0-9 ]", "", name)
    return re.sub(r"\s+", " ", name).strip()


if __name__ == "__main__":
    asyncio.run(main())
