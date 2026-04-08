"""
Multi-Niche TN Lead Scraper
────────────────────────────────────────────────────────────────────────────────
TARGET  : 25–30 leads/day across Tamil Nadu — ALL must have a phone number
NICHES  : IT, Pharma, Real Estate, Manufacturing, BFSI, Logistics, Automobile,
          Media/Events, FMCG, Healthcare
MIN/NICHE: 5 phone-verified leads per niche
CITIES  : All 12 TN cities in rotation
SOURCES : JustDial → Sulekha → OpenStreetMap / Google Maps
RULES   :
  • A lead with NO phone number is NEVER included
  • Hard cap : 30 leads/day (phone-verified only)
  • Soft floor: 25 leads/day (phone-verified only)
  • Cross-run dedup: same company never appears twice across days
  • Education & Hospitality leads are capped at 3 combined
"""

import os, time, logging, re
from datetime import datetime, date
from collections import defaultdict

import pandas as pd

from google_maps  import scrape_google_maps
from sulekha      import scrape_sulekha
from justdial     import scrape_justdial_niche
from dedup        import deduplicate, load_master_seen, save_master_seen
from emailer      import send_email_report

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)s  %(message)s")
log = logging.getLogger(__name__)

MASTER_CSV    = "leads/master_leads.csv"
LEADS_DIR     = "leads"
DAILY_MIN     = 25
DAILY_MAX     = 30
PER_NICHE_MIN = 5
MAX_LOW_PRI   = 3

ALL_TN_CITIES = [
    "Chennai", "Coimbatore", "Madurai", "Trichy", "Salem",
    "Tiruppur", "Vellore", "Erode", "Tirunelveli", "Thoothukudi",
    "Dindigul", "Thanjavur",
]

LOW_PRIORITY_INDUSTRIES = {"Education", "Hospitality"}

NICHES = [
    {
        "label": "IT / Tech",
        "jd_categories": [
            ("IT-Companies",               "IT / Tech"),
            ("Software-Companies",         "IT / Tech"),
            ("Software-Development-Companies","IT / Tech"),
            ("Web-Design-Companies",       "IT / Tech"),
            ("Mobile-App-Development",     "IT / Tech"),
            ("Digital-Marketing-Companies","IT / Tech"),
            ("BPO-Companies",              "IT / Tech"),
        ],
        "sulekha_categories": [
            "it-companies", "software-companies",
            "software-development-companies", "digital-marketing-agencies", "bpo-companies",
        ],
        "osm_queries": ["it company", "software company", "bpo company"],
    },
    {
        "label": "Pharma / Healthcare",
        "jd_categories": [
            ("Pharmaceutical-Companies",    "Pharma / Healthcare"),
            ("Medical-Equipment-Suppliers", "Pharma / Healthcare"),
            ("Hospitals",                   "Pharma / Healthcare"),
            ("Diagnostic-Centers",          "Pharma / Healthcare"),
            ("Drug-Stores",                 "Pharma / Healthcare"),
            ("Ayurvedic-Medicine-Manufacturers","Pharma / Healthcare"),
            ("Healthcare-Companies",        "Pharma / Healthcare"),
        ],
        "sulekha_categories": [
            "pharmaceutical-companies", "medical-equipment-dealers",
            "hospitals", "diagnostic-centres",
        ],
        "osm_queries": ["pharmaceutical company", "medical equipment", "hospital"],
    },
    {
        "label": "Real Estate / Construction",
        "jd_categories": [
            ("Real-Estate-Agents",          "Real Estate / Construction"),
            ("Builders-And-Developers",     "Real Estate / Construction"),
            ("Construction-Companies",      "Real Estate / Construction"),
            ("Interior-Designers",          "Real Estate / Construction"),
            ("Architects",                  "Real Estate / Construction"),
        ],
        "sulekha_categories": ["real-estate-agents", "construction-companies"],
        "osm_queries": ["real estate", "builder developer", "construction company"],
    },
    {
        "label": "Manufacturing / Textile",
        "jd_categories": [
            ("Manufacturing-Companies",     "Manufacturing / Textile"),
            ("Textile-Companies",           "Manufacturing / Textile"),
            ("Garment-Manufacturers",       "Manufacturing / Textile"),
            ("Industrial-Machinery-Dealers","Manufacturing / Textile"),
            ("Export-Companies",            "Manufacturing / Textile"),
        ],
        "sulekha_categories": ["manufacturing-companies", "textile-companies"],
        "osm_queries": ["manufacturing company", "textile company", "factory"],
    },
    {
        "label": "BFSI",
        "jd_categories": [
            ("Banks",                       "BFSI"),
            ("Insurance-Companies",         "BFSI"),
            ("CA-Firms",                    "BFSI"),
            ("Financial-Advisors",          "BFSI"),
            ("NBFCs",                       "BFSI"),
        ],
        "sulekha_categories": ["banks", "insurance-companies", "ca-firms"],
        "osm_queries": ["bank", "insurance company", "nbfc"],
    },
    {
        "label": "Logistics / Transport",
        "jd_categories": [
            ("Logistics-Companies",         "Logistics / Transport"),
            ("Packers-And-Movers",          "Logistics / Transport"),
            ("Courier-Services",            "Logistics / Transport"),
            ("Freight-Forwarders",          "Logistics / Transport"),
        ],
        "sulekha_categories": ["logistics-companies"],
        "osm_queries": ["logistics company", "packers movers", "courier service"],
    },
    {
        "label": "Automobile",
        "jd_categories": [
            ("Automobile-Dealers",          "Automobile"),
            ("Car-Dealers",                 "Automobile"),
            ("Two-Wheeler-Dealers",         "Automobile"),
            ("Auto-Parts-Dealers",          "Automobile"),
        ],
        "sulekha_categories": ["automobile-dealers"],
        "osm_queries": ["car dealer", "automobile dealer", "auto parts"],
    },
    {
        "label": "Media / Events",
        "jd_categories": [
            ("Advertising-Agencies",        "Media / Events"),
            ("Event-Management-Companies",  "Media / Events"),
            ("PR-Companies",                "Media / Events"),
            ("Printing-Companies",          "Media / Events"),
        ],
        "sulekha_categories": ["advertising-agencies", "event-management-companies"],
        "osm_queries": ["advertising agency", "event management"],
    },
    {
        "label": "FMCG / Retail / Food",
        "jd_categories": [
            ("FMCG-Companies",              "FMCG / Retail / Food"),
            ("Food-Processing-Companies",   "FMCG / Retail / Food"),
            ("Catering-Services",           "FMCG / Retail / Food"),
            ("Supermarkets",                "FMCG / Retail / Food"),
        ],
        "sulekha_categories": ["fmcg-companies"],
        "osm_queries": ["fmcg company", "food processing company", "supermarket"],
    },
]


def has_phone(lead: dict) -> bool:
    digits = "".join(c for c in str(lead.get("phone", "")) if c.isdigit())
    if digits.startswith("91") and len(digits) == 12:
        digits = digits[2:]
    return (len(digits) == 10 and digits[0] in "6789") or len(digits) >= 7


def is_low_priority(lead: dict) -> bool:
    return lead.get("industry", "") in LOW_PRIORITY_INDUSTRIES


def _norm(name: str) -> str:
    name = name.lower().strip()
    name = re.sub(r"\b(pvt|ltd|private|limited|llp|inc|corp|co|&)\b", "", name)
    name = re.sub(r"[^a-z0-9 ]", "", name)
    return re.sub(r"\s+", " ", name).strip()


def phone_leads_only(leads: list) -> list:
    return [l for l in leads if has_phone(l)]


def scrape_niche(niche: dict, cities: list, max_pages: int = 5) -> list:
    all_leads = []
    label = niche["label"]
    for city in cities:
        log.info(f"  [{label}] {city}...")
        try:
            jd = scrape_justdial_niche(city, niche["jd_categories"], max_pages=max_pages)
            all_leads.extend(jd)
        except Exception as e:
            log.error(f"  JustDial/{label}/{city}: {e}")
        time.sleep(3)

        for cat in niche["sulekha_categories"]:
            try:
                all_leads.extend(scrape_sulekha(cat, city, max_pages=max_pages))
            except Exception as e:
                log.error(f"  Sulekha/{cat}/{city}: {e}")
            time.sleep(2)

        for query in niche["osm_queries"]:
            try:
                all_leads.extend(scrape_google_maps(query, city, max_results=100))
            except Exception as e:
                log.error(f"  OSM/{query}/{city}: {e}")
            time.sleep(2)

    return deduplicate(all_leads)


def main():
    run_date  = datetime.now().strftime("%Y-%m-%d")
    day_index = date.today().toordinal()
    os.makedirs(LEADS_DIR, exist_ok=True)
    log.info(f"=== Multi-Niche TN Lead Scrape: {run_date} ===")

    master_seen = load_master_seen(MASTER_CSV)
    log.info(f"Master history: {len(master_seen)} companies already seen")

    rotating_1 = ALL_TN_CITIES[day_index % len(ALL_TN_CITIES)]
    rotating_2 = ALL_TN_CITIES[(day_index + 1) % len(ALL_TN_CITIES)]
    today_cities = ["Chennai", "Coimbatore"]
    for c in [rotating_1, rotating_2]:
        if c not in today_cities:
            today_cities.append(c)
    log.info(f"Today's cities: {today_cities}")

    # ── Phase 1: Scrape every niche ──────────────────────────────────────────
    niche_phone_leads: dict[str, list] = defaultdict(list)
    for niche in NICHES:
        label = niche["label"]
        log.info(f"=== Niche: {label} ===")
        raw       = scrape_niche(niche, today_cities, max_pages=5)
        new       = [l for l in raw if _norm(l.get("company_name","")) not in master_seen]
        w_phone   = phone_leads_only(new)
        niche_phone_leads[label] = w_phone
        log.info(f"  [{label}] phone-verified new leads: {len(w_phone)}")

    # ── Phase 2: Expand niches that are below minimum ─────────────────────────
    extra_cities = [c for c in ALL_TN_CITIES if c not in today_cities]
    for niche in NICHES:
        label     = niche["label"]
        shortfall = PER_NICHE_MIN - len(niche_phone_leads[label])
        if shortfall <= 0:
            continue
        log.info(f"  [{label}] short by {shortfall} — expanding to more cities...")
        for city in extra_cities:
            extra_raw   = scrape_niche(niche, [city], max_pages=3)
            extra_new   = [l for l in extra_raw if _norm(l.get("company_name","")) not in master_seen]
            extra_phone = phone_leads_only(extra_new)
            niche_phone_leads[label].extend(extra_phone)
            niche_phone_leads[label] = deduplicate(niche_phone_leads[label])
            log.info(f"    [{label}] after {city}: {len(niche_phone_leads[label])} phone leads")
            if len(niche_phone_leads[label]) >= PER_NICHE_MIN:
                break
            time.sleep(3)
        if len(niche_phone_leads[label]) < PER_NICHE_MIN:
            log.warning(f"  [{label}] only {len(niche_phone_leads[label])} phone leads "
                        f"(minimum {PER_NICHE_MIN}) — using what we have")

    # ── Phase 3: Build final list ─────────────────────────────────────────────
    # Guarantee PER_NICHE_MIN per niche first, then fill up to DAILY_MAX
    selected: list[dict] = []
    remainder_pool: list[dict] = []
    for niche in NICHES:
        label  = niche["label"]
        leads  = niche_phone_leads[label]
        selected.extend(leads[:PER_NICHE_MIN])
        remainder_pool.extend(leads[PER_NICHE_MIN:])

    source_priority = {"JustDial": 1, "Sulekha": 2, "OpenStreetMap": 3}
    remainder_pool.sort(key=lambda l: source_priority.get(l.get("source",""), 9))

    slots_left = DAILY_MAX - len(selected)
    low_added  = sum(1 for l in selected if is_low_priority(l))
    for lead in remainder_pool:
        if slots_left <= 0:
            break
        if is_low_priority(lead):
            if low_added >= MAX_LOW_PRI:
                continue
            low_added += 1
        selected.append(lead)
        slots_left -= 1

    # Global dedup + strict phone filter + hard cap
    final_leads = deduplicate(selected)
    final_leads = phone_leads_only(final_leads)   # paranoia check
    final_leads = final_leads[:DAILY_MAX]

    # ── Phase 4: Summary ──────────────────────────────────────────────────────
    log.info("=== DAILY SUMMARY ===")
    niche_counts: dict[str, int] = defaultdict(int)
    for l in final_leads:
        niche_counts[l.get("industry","Unknown")] += 1
    for label, cnt in sorted(niche_counts.items()):
        status = "✓" if cnt >= PER_NICHE_MIN else "⚠"
        log.info(f"  {status}  {label:<35} {cnt} leads")
    log.info(f"  TOTAL phone-verified leads: {len(final_leads)}")
    if len(final_leads) < DAILY_MIN:
        log.warning(f"Below daily minimum! {len(final_leads)} < {DAILY_MIN}")
    else:
        log.info(f"Daily target met ✓  ({DAILY_MIN}–{DAILY_MAX})")

    if not final_leads:
        log.warning("No new phone-verified leads today — exiting.")
        return

    # ── Phase 5: Save CSV ─────────────────────────────────────────────────────
    csv_path = f"{LEADS_DIR}/leads_{run_date}.csv"
    df = pd.DataFrame(final_leads)
    cols = ["company_name","contact_name","designation","phone",
            "email","website","address","industry","source","notes"]
    for col in cols:
        if col not in df.columns:
            df[col] = ""
    df[cols].to_csv(csv_path, index=False)
    log.info(f"Saved → {csv_path}  ({len(final_leads)} leads)")

    save_master_seen(MASTER_CSV, final_leads, master_seen)
    send_email_report(final_leads, csv_path, run_date)
    log.info("=== Done ===")


if __name__ == "__main__":
    main()
