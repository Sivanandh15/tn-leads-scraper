"""
LinkedIn lead finder via Google Search.
Searches Google for: site:linkedin.com/in "HR Manager" "IT company" Chennai
No LinkedIn account or login used — hits Google public search only.
"""
import requests, time, logging, re
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
from urllib.parse import quote_plus

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


def scrape_linkedin_via_google(search_phrase: str, max_results: int = 10) -> list[dict]:
    query  = f'site:linkedin.com/in {search_phrase}'
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
        key = (name, company)
        if key in seen or not name:
            continue
        seen.add(key)

        leads.append({
            "company_name": company,
            "contact_name": name,
            "designation":  designation,
            "phone":        "",
            "email":        "",
            "website":      url,
            "address":      _extract_city(snippet),
            "industry":     _guess_industry(search_phrase),
            "source":       "LinkedIn (via Google)",
            "notes":        snippet[:120],
        })

    time.sleep(5)  # be polite — LinkedIn/Google may throttle
    return leads


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
    if "pharma" in p or "health" in p:              return "Pharma / Healthcare"
    if "bank" in p or "bfsi" in p or "finance" in p:return "BFSI"
    if "real estate" in p or "builder" in p:        return "Real Estate"
    if "manufactur" in p or "textile" in p:         return "Manufacturing / Textile"
    if "logistics" in p:                            return "Logistics"
    if "hotel" in p:                                return "Hospitality"
    return "Other"
