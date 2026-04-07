"""
Sulekha.com business listing scraper.
Updated: Pharma / Healthcare categories removed entirely.
Full pagination (fetches ALL pages until empty).
"""
import requests, time, logging
from bs4 import BeautifulSoup
from fake_useragent import UserAgent

log = logging.getLogger(__name__)
ua  = UserAgent()

HEADERS = {
    "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection":      "keep-alive",
    "DNT":             "1",
}

# Pharma / Healthcare removed — pharmaceutical-companies, hospitals removed
INDUSTRY_MAP = {
    "it-companies":              "IT / Tech",
    "software-companies":        "IT / Tech",
    "manufacturing-companies":   "Manufacturing / Textile",
    "textile-companies":         "Manufacturing / Textile",
    "real-estate-agents":        "Real Estate / Construction",
    "construction-companies":    "Real Estate / Construction",
    "banks":                     "BFSI",
    "insurance-companies":       "BFSI",
    "chartered-accountants":     "BFSI",
    "ca-firms":                  "BFSI",
    "logistics-companies":       "Logistics / Transport",
    "hotels":                    "Hospitality",
    "automobile-dealers":        "Automobile",
    "advertising-agencies":      "Media / Events",
    "event-management-companies":"Media / Events",
    "educational-institutes":    "Education",
    "fmcg-companies":            "FMCG / Retail / Food",
    "retail-companies":          "FMCG / Retail / Food",
    "food-companies":            "FMCG / Retail / Food",
}

CITY_SLUG_MAP = {
    "Chennai":     "chennai",
    "Coimbatore":  "coimbatore",
    "Madurai":     "madurai",
    "Trichy":      "trichy",
    "Salem":       "salem",
    "Tiruppur":    "tiruppur",
    "Vellore":     "vellore",
    "Erode":       "erode",
    "Tirunelveli": "tirunelveli",
    "Thoothukudi": "tuticorin",
    "Dindigul":    "dindigul",
    "Thanjavur":   "thanjavur",
}


def scrape_sulekha(category: str, city: str, max_pages: int = 10) -> list[dict]:
    """Scrape ALL Sulekha pages for category in city."""
    # Skip if category was accidentally passed for a removed industry
    if category in ("pharmaceutical-companies", "hospitals"):
        log.info(f"  Sulekha: skipping blocked category [{category}]")
        return []

    leads     = []
    industry  = INDUSTRY_MAP.get(category, "Other")
    city_slug = CITY_SLUG_MAP.get(city, city.lower())

    for page in range(1, max_pages + 1):
        if page == 1:
            url = f"https://www.sulekha.com/{category}/{city_slug}/companies"
        else:
            url = f"https://www.sulekha.com/{category}/{city_slug}/companies-{page}"

        try:
            headers = {**HEADERS, "User-Agent": ua.random}
            resp    = requests.get(url, headers=headers, timeout=15)
            resp.raise_for_status()
        except Exception as e:
            log.error(f"Sulekha error [{url}]: {e}")
            break

        soup  = BeautifulSoup(resp.text, "lxml")
        cards = (soup.select("div.companylist-cont") or
                 soup.select("li.listing-item")      or
                 soup.select("div.biz-listing")      or
                 soup.select("div.company-info"))

        if not cards:
            log.info(f"  Sulekha {category}/{city}: no cards on page {page}, stopping")
            break

        for card in cards:
            name    = _text(card, ["h2.company-name","h3.companyname","h2","h3"])
            phone   = _text(card, ["span.phone-no","span.contact-no","a.phone","span.mobileno"])
            address = _text(card, ["span.address","p.address","div.address","span.locality"])
            website = _attr(card, ["a.website-link","a[href*='http']"], "href")

            if not name:
                continue
            leads.append({
                "company_name": name.strip(),
                "contact_name": "",
                "designation":  "",
                "phone":        phone.replace("\n","").strip(),
                "email":        "",
                "website":      website if website and website.startswith("http") else "",
                "address":      f"{address.strip()}, {city}".strip(", "),
                "industry":     industry,
                "source":       "Sulekha",
                "notes":        f"{city}, Tamil Nadu",
            })

        log.info(f"  Sulekha {category}/{city} page {page}: +{len(cards)} entries")
        time.sleep(2.5)

    return leads


def _text(card, selectors):
    for sel in selectors:
        el = card.select_one(sel)
        if el:
            return el.get_text(strip=True)
    return ""


def _attr(card, selectors, attr):
    for sel in selectors:
        el = card.select_one(sel)
        if el and el.get(attr):
            return el[attr]
    return ""
