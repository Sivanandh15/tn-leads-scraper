"""
LinkedIn lead finder via Google Search.
Searches Google for: site:linkedin.com/in "HR Manager" "IT company" Chennai
No LinkedIn account or login used — hits Google public search only.

Phone extraction: scans Google snippet text for Indian mobile numbers.
"""
import requests, time, logging, re
from bs4 import BeautifulSoup
from fake_useragent import UserAgent

log = logging.getLogger(__name__)
ua  = UserAgent()

GOOGLE_SEARCH = "https://www.google.com/search"

HEADERS = {
    "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-IN,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection":      "keep-alive",
    "DNT":             "1",
}

TN_CITIES = ["Chennai","Coimbatore","Madurai","Trichy","Salem","Tiruppur",
             "Vellore","Erode","Tirunelveli","Thoothukudi","Dindigul","Thanjavur"]

# HR / decision-maker roles to search for — focused on IT & business
HR_SEARCH_QUERIES = [
    # IT / Tech
    'site:linkedin.com/in "HR Manager" "IT company" Chennai',
    'site:linkedin.com/in "HR Head" "software company" Chennai',
    'site:linkedin.com/in "Human Resources" "tech company" Chennai',
    'site:linkedin.com/in "HR Manager" "software" Coimbatore',
    'site:linkedin.com/in "Talent Acquisition" "IT" Chennai',

    # Manufacturing / Logistics
    'site:linkedin.com/in "HR Manager" "manufacturing" Chennai',
    'site:linkedin.com/in "Admin Manager" "manufacturing" Coimbatore',
    'site:linkedin.com/in "HR Manager" "logistics" Chennai',
    'site:linkedin.com/in "Procurement Manager" "manufacturing" Chennai',

    # Construction / Real Estate
    'site:linkedin.com/in "HR Manager" "construction" Chennai',
    'site:linkedin.com/in "Admin" "real estate" Chennai',

    # BFSI
    'site:linkedin.com/in "HR Manager" "bank" Chennai',
    'site:linkedin.com/in "HR" "insurance" Chennai',

    # Events / Media / Automobile
    'site:linkedin.com/in "HR Manager" "automobile" Chennai',
    'site:linkedin.com/in "Admin Manager" "event management" Chennai',
]

# Also search directly for company phone numbers on Google
COMPANY_PHONE_QUERIES = [
    'IT companies Chennai contact number site:justdial.com OR site:sulekha.com',
    'software companies Chennai phone number',
    'manufacturing companies Coimbatore contact number',
]


def scrape_linkedin_via_google(search_phrase: str, max_results: int = 10) -> list[dict]:
    """Search Google for LinkedIn profiles matching search_phrase."""
    query  = search_phrase if "site:linkedin" in search_phrase else f'site:linkedin.com/in {search_phrase}'
    leads  = []
    seen   = set()
    params = {"q": query, "num": 10, "hl": "en"}

    try:
        headers = {**HEADERS, "User-Agent": ua.random}
        resp    = requests.get(GOOGLE_SEARCH, params=params, headers=headers, timeout=15)
        resp.raise_for_status()
    except Exception as e:
        log.error(f"Google search error for '{search_phrase}': {e}")
        return []

    soup    = BeautifulSoup(resp.text, "lxml")
    results = soup.select("div.g") or soup.select("div[data-hveid]")

    for res in results[:max_results]:
        title_el   = res.select_one("h3")
        link_el    = res.select_one("a[href]")
        snippet_el = res.select_one("div.VwiC3b, span.aCOpRe, div[data-sncf]")

        if not title_el or not link_el:
            continue

        raw_title = title_el.get_text(strip=True)
        url       = link_el.get("href", "")
        snippet   = snippet_el.get_text(strip=True) if snippet_el else ""

        if "linkedin.com/in" not in url:
            continue

        name, designation, company = _parse_linkedin_title(raw_title)
        key = (name.lower(), company.lower())
        if key in seen or not name:
            continue
        seen.add(key)

        # Try to pull a phone number from the snippet text
        phone = _extract_phone(snippet)

        leads.append({
            "company_name": company,
            "contact_name": name,
            "designation":  designation,
            "phone":        phone,
            "email":        "",
            "website":      url,
            "address":      _extract_city(snippet),
            "industry":     _guess_industry(search_phrase),
            "source":       "LinkedIn (via Google)",
            "notes":        snippet[:150],
        })

    time.sleep(5)  # be polite
    return leads


def scrape_all_hr_linkedin(max_per_query: int = 5) -> list[dict]:
    """
    Run all HR_SEARCH_QUERIES and return deduplicated leads.
    Called from main.py for a comprehensive LinkedIn sweep.
    """
    all_leads = []
    seen_keys = set()

    for query in HR_SEARCH_QUERIES:
        leads = scrape_linkedin_via_google(query, max_results=max_per_query)
        for lead in leads:
            key = (lead.get("contact_name","").lower(), lead.get("company_name","").lower())
            if key not in seen_keys and key != ("",""):
                seen_keys.add(key)
                all_leads.append(lead)
        log.info(f"  LinkedIn [{query[:60]}...] → {len(leads)} leads")
        time.sleep(6)   # avoid Google rate limiting

    log.info(f"LinkedIn total: {len(all_leads)} unique HR contacts")
    return all_leads


def _extract_phone(text: str) -> str:
    """
    Extract Indian mobile / landline from any text blob.
    Handles formats: +91-XXXXXXXXXX, 91XXXXXXXXXX, 0XXXXXXXXXX, XXXXXXXXXX
    """
    # Patterns to try in priority order
    patterns = [
        r'(?:\+91[\s\-]?|91[\s\-]?|0)?[6-9]\d{9}',   # mobile
        r'\b0\d{2,4}[\s\-]?\d{6,8}\b',                  # landline with STD
    ]
    for pat in patterns:
        matches = re.findall(pat, text)
        for m in matches:
            digits = re.sub(r'\D', '', m)
            if digits.startswith('91') and len(digits) == 12:
                digits = digits[2:]
            if len(digits) == 10 and digits[0] in '6789':
                return digits
    return ""


def _parse_linkedin_title(title: str):
    title = re.sub(r"\s*\|\s*LinkedIn.*$", "", title, flags=re.IGNORECASE).strip()
    parts = [p.strip() for p in re.split(r"\s+[-–]\s+", title)]
    name        = parts[0] if len(parts) > 0 else ""
    designation = parts[1] if len(parts) > 1 else ""
    company     = parts[2] if len(parts) > 2 else ""
    return name, designation, company


def _extract_city(snippet: str) -> str:
    for c in TN_CITIES:
        if c.lower() in snippet.lower():
            return c
    return ""


def _guess_industry(phrase: str) -> str:
    p = phrase.lower()
    if "it" in p or "tech" in p or "software" in p: return "IT / Tech"
    if "bank" in p or "bfsi" in p or "finance" in p:return "BFSI"
    if "real estate" in p or "builder" in p:        return "Real Estate / Construction"
    if "manufactur" in p or "textile" in p:         return "Manufacturing / Textile"
    if "logistics" in p:                            return "Logistics / Transport"
    if "hotel" in p:                                return "Hospitality"
    if "automobile" in p:                           return "Automobile"
    if "event" in p:                                return "Media / Events"
    return "Other"
