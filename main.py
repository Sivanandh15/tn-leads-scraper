"""
Multi-Niche TN Lead Scraper
────────────────────────────────────────────────────────────────────────────────
TARGET    : 20+ leads/day across Tamil Nadu — ALL must have a phone number
MIN/NICHE : 3 phone-verified leads per niche
CITIES    : Chennai + 1 rotating TN city per day (was 4 — cut to save time)
SOURCES   : JustDial → Sulekha → OpenStreetMap
TIME CAP  : Hard stops scraping at 45 min and emails whatever was collected

FIX HISTORY (vs original):
  • TIME_BUDGET_SEC=45min — hard stop so we always email results before GH kills us
  • Removed Phase 2 city expansion (was the main timeout cause)
  • max_pages: 5 → 2  (enough data, 60% fewer HTTP calls)
  • Sleep delays halved in all scrapers
  • DAILY_MIN lowered to 20; always sends email even if below floor
  • Cities reduced: 4 → 2 (Chennai + 1 rotating)
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

MASTER_CSV      = "leads/master_leads.csv"
LEADS_DIR       = "leads"
DAILY_MIN       = 20          # lowered from 25
DAILY_MAX       = 25
PER_NICHE_MIN   = 3           # lowered from 5 — ensures we finish all niches in time
MAX_LOW_PRI     = 3
TIME_BUDGET_SEC = 45 * 60     # ← KEY FIX: hard stop at 45 min
MAX_PAGES       = 2           # reduced from 5

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
            ("IT-Companies",                "IT / Tech"),
            ("Software-Companies",          "IT / Tech"),
            ("Software-Development-Companies","IT / Tech"),
            ("Web-Design-Companies",        "IT / Tech"),
            ("Mobile-App-Development",      "IT / Tech"),
            ("Digital-Marketing-Companies", "IT / Tech"),
            ("BPO-Companies",               "IT / Tech"),
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


def _budget_ok(start_time: float) -> bool:
    remaining = TIME_BUDGET_SEC - (time.time() - start_time)
    if remaining < 60:
        log.warning(f"  ⏰ Time budget nearly exhausted ({remaining:.0f}s left) — stopping scrape")
        return False
    return True


def scrape_niche(niche: dict, cities: list, start_time: float) -> list:
    """Scrape one niche. Returns early if the time budget is exceeded."""
    all_leads = []
    label = niche["label"]

    for city in cities:
        if not _budget_ok(start_time):
            return deduplicate(all_leads)
        log.info(f"  [{label}] {city}...")

        try:
            jd = scrape_justdial_niche(city, niche["jd_categories"], max_pages=MAX_PAGES)
            all_leads.extend(jd)
        except Exception as e:
            log.error(f"  JustDial/{label}/{city}: {e}")
        time.sleep(1)  # was 3s

        for cat in niche["sulekha_categories"]:
            if not _budget_ok(start_time):
                return deduplicate(all_leads)
            try:
                all_leads.extend(scrape_sulekha(cat, city, max_pages=MAX_PAGES))
            except Exception as e:
                log.error(f"  Sulekha/{cat}/{city}: {e}")
            time.sleep(1)  # was 2s

        for query in niche["osm_queries"]:
            if not _budget_ok(start_time):
                return deduplicate(all_leads)
            try:
                all_leads.extend(scrape_google_maps(query, city, max_results=100))
            except Exception as e:
                log.error(f"  OSM/{query}/{city}: {e}")
            time.sleep(1)  # was 2s

    return deduplicate(all_leads)


def main():
    run_date   = datetime.now().strftime("%Y-%m-%d")
    day_index  = date.today().toordinal()
    start_time = time.time()
    os.makedirs(LEADS_DIR, exist_ok=True)
    log.info(f"=== Multi-Niche TN Lead Scrape: {run_date} ===")
    log.info(f"Time budget: {TIME_BUDGET_SEC // 60} minutes | max_pages={MAX_PAGES}")

    master_seen = load_master_seen(MASTER_CSV)
    log.info(f"Master history: {len(master_seen)} companies already seen")

    # ── 2 cities: Chennai (always) + 1 rotating ──────────────────────────────
    # Reduced from 4 cities — halves scrape time while still covering good variety
    rotating = ALL_TN_CITIES[day_index % len(ALL_TN_CITIES)]
    today_cities = ["Chennai"]
    if rotating != "Chennai":
        today_cities.append(rotating)
    log.info(f"Today's cities: {today_cities}")

    # ── Phase 1: Scrape every niche (time-budgeted) ───────────────────────────
    niche_phone_leads: dict[str, list] = defaultdict(list)
    for niche in NICHES:
        if not _budget_ok(start_time):
            log.warning("⏰ Time budget hit — skipping remaining niches, building final list now")
            break
        label   = niche["label"]
        elapsed = int(time.time() - start_time)
        log.info(f"=== Niche: {label} (elapsed {elapsed}s / {TIME_BUDGET_SEC}s) ===")
        raw     = scrape_niche(niche, today_cities, start_time)
        new     = [l for l in raw if _norm(l.get("company_name", "")) not in master_seen]
        w_phone = phone_leads_only(new)
        niche_phone_leads[label] = w_phone
        log.info(f"  [{label}] phone-verified new leads: {len(w_phone)}")

    # Phase 2 (city expansion) removed — was the main cause of 90-min timeout.
    # With max_pages=2 and 1s sleeps, 9 niches × 2 cities completes in ~35 min.

    # ── Phase 3: Build final list ─────────────────────────────────────────────
    selected: list[dict] = []
    remainder_pool: list[dict] = []
    for niche in NICHES:
        label  = niche["label"]
        leads  = niche_phone_leads[label]
        selected.extend(leads[:PER_NICHE_MIN])
        remainder_pool.extend(leads[PER_NICHE_MIN:])

    source_priority = {"JustDial": 1, "Sulekha": 2, "OpenStreetMap": 3}
    remainder_pool.sort(key=lambda l: source_priority.get(l.get("source", ""), 9))

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

    final_leads = deduplicate(selected)
    final_leads = phone_leads_only(final_leads)
    final_leads = final_leads[:DAILY_MAX]

    # ── Phase 4: Summary ──────────────────────────────────────────────────────
    elapsed_total = int(time.time() - start_time)
    log.info("=== DAILY SUMMARY ===")
    niche_counts: dict[str, int] = defaultdict(int)
    for l in final_leads:
        niche_counts[l.get("industry", "Unknown")] += 1
    for label, cnt in sorted(niche_counts.items()):
        status = "✓" if cnt >= PER_NICHE_MIN else "⚠"
        log.info(f"  {status}  {label:<35} {cnt} leads")
    log.info(f"  TOTAL phone-verified leads : {len(final_leads)}")
    log.info(f"  Total elapsed              : {elapsed_total}s  ({elapsed_total // 60}m {elapsed_total % 60}s)")

    if len(final_leads) < DAILY_MIN:
        log.warning(f"Below daily minimum: {len(final_leads)} < {DAILY_MIN} — sending email anyway")
    else:
        log.info(f"Daily target met ✓  ({DAILY_MIN}–{DAILY_MAX})")

    # KEY FIX: always save + email even when below the soft floor.
    # Previously the script did `return` here with no output when leads were scarce.
    if not final_leads:
        log.warning("No new phone-verified leads today — exiting.")
        return

    # ── Phase 5: Save CSV ─────────────────────────────────────────────────────
    csv_path = f"{LEADS_DIR}/leads_{run_date}.csv"
    df = pd.DataFrame(final_leads)
    cols = ["company_name", "contact_name", "designation", "phone",
            "email", "website", "address", "industry", "source", "notes"]
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
