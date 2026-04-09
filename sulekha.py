"""
Sulekha.com business listing scraper — multi-niche version.
Supports all niches: IT, Pharma, Real Estate, Manufacturing, BFSI, Logistics,
Automobile, Media/Events, FMCG, Healthcare.
"""
import requests, time, re, logging
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

INDUSTRY_MAP = {
    # IT
    "it-companies":                   "IT / Tech",
    "software-companies":             "IT / Tech",
    "software-development-companies": "IT / Tech",
    "web-design-companies":           "IT / Tech",
    "mobile-app-development":         "IT / Tech",
    "digital-marketing-agencies":     "IT / Tech",
    "bpo-companies":                  "IT / Tech",
    "it-services":                    "IT / Tech",
    "erp-software-companies":         "IT / Tech",
    "cloud-computing-companies":      "IT / Tech",
    "cybersecurity-companies":        "IT / Tech",
    "data-analytics-companies":       "IT / Tech",
    # Pharma / Healthcare
    "pharmaceutical-companies":       "Pharma / Healthcare",
    "medical-equipment-dealers":      "Pharma / Healthcare",
    "hospitals":                      "Pharma / Healthcare",
    "diagnostic-centres":             "Pharma / Healthcare",
    "ayurvedic-medicine-manufacturers":"Pharma / Healthcare",
    "healthcare-companies":           "Pharma / Healthcare",
    "drug-stores":                    "Pharma / Healthcare",
    # Real Estate / Construction
    "real-estate-agents":             "Real Estate / Construction",
    "construction-companies":         "Real Estate / Construction",
    "builders-and-developers":        "Real Estate / Construction",
    "interior-designers":             "Real Estate / Construction",
    # Manufacturing / Textile
    "manufacturing-companies":        "Manufacturing / Textile",
    "textile-companies":              "Manufacturing / Textile",
    "garment-manufacturers":          "Manufacturing / Textile",
    # BFSI
    "banks":                          "BFSI",
    "insurance-companies":            "BFSI",
    "ca-firms":                       "BFSI",
    "financial-advisors":             "BFSI",
    # Logistics
    "logistics-companies":            "Logistics / Transport",
    # Automobile
    "automobile-dealers":             "Automobile",
    # Media / Events
    "advertising-agencies":           "Media / Events",
    "event-management-companies":     "Media / Events",
    # FMCG / Food
    "fmcg-companies":                 "FMCG / Retail / Food",
    "food-processing-companies":      "FMCG / Retail / Food",
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


def _build_urls(category: str, city_slug: str, page: int) -> list[str]:
    if page == 1:
        return [
            f"https://www.sulekha.com/{category}/{city_slug}/companies",
            f"https://www.sulekha.com/{city_slug}/{category}",
            f"https://www.sulekha.com/{category}/{city_slug}",
        ]
    return [
        f"https://www.sulekha.com/{category}/{city_slug}/companies-{page}",
        f"https://www.sulekha.com/{city_slug}/{category}-{page}",
    ]


def scrape_sulekha(category: str, city: str, max_pages: int = 5) -> list[dict]:
    leads     = []
    industry  = INDUSTRY_MAP.get(category, "Other")
    city_slug = CITY_SLUG_MAP.get(city, city.lower())

    for page in range(1, max_pages + 1):
        urls    = _build_urls(category, city_slug, page)
        success = False

        for url in urls:
            try:
                headers = {**HEADERS, "User-Agent": ua.random}
                resp    = requests.get(url, headers=headers, timeout=15)
                if resp.status_code == 404:
                    continue
                if resp.status_code == 403:
                    log.warning(f"  Sulekha 403 blocked on {url}")
                    return leads
                resp.raise_for_status()
                success = True
            except Exception as e:
                log.error(f"Sulekha error [{url}]: {e}")
                continue

            soup  = BeautifulSoup(resp.text, "lxml")
            cards = (soup.select("div.companylist-cont")  or
                     soup.select("li.listing-item")        or
                     soup.select("div.biz-listing")        or
                     soup.select("div.company-info")       or
                     soup.select("div.sl-item")            or
                     soup.select("li[class*='listing']")   or
                     soup.select("div[class*='company']"))

            if not cards:
                log.info(f"  Sulekha {category}/{city}: no cards on page {page} at {url}")
                break

            for card in cards:
                name    = _text(card, [
                    "h2.company-name", "h3.companyname", "h2.comp-name",
                    "span.comp-name", "a.comp-name", "h2", "h3"
                ])
                phone   = _extract_phone(card)
                address = _text(card, [
                    "span.address", "p.address", "div.address",
                    "span.locality", "div.location"
                ])
                website = _attr(card, ["a.website-link", "a[href*='http']"], "href")

                if not name:
                    continue
                leads.append({
                    "company_name": name.strip(),
                    "contact_name": "",
                    "designation":  "",
                    "phone":        phone,
                    "email":        "",
                    "website":      website if website and website.startswith("http") else "",
                    "address":      f"{address.strip()}, {city}".strip(", ") if address else city,
                    "industry":     industry,
                    "source":       "Sulekha",
                    "notes":        f"{city}, Tamil Nadu",
                })

            log.info(f"  Sulekha {category}/{city} page {page}: +{len(cards)} entries at {url}")
            time.sleep(1)
            break

        if not success:
            break

    return leads


def _extract_phone(card) -> str:
    for sel in ["span.phone-no", "span.contact-no", "a.phone",
                "span.mobileno", "a[href^='tel:']"]:
        el = card.select_one(sel)
        if el:
            href = el.get("href", "")
            if href.startswith("tel:"):
                digits = re.sub(r"\D", "", href)
                if digits.startswith("91") and len(digits) == 12:
                    digits = digits[2:]
                if len(digits) == 10:
                    return digits
            phone = _clean_phone(el.get_text(strip=True))
            if phone:
                return phone
    return _clean_phone(card.get_text(" "))


def _clean_phone(text: str) -> str:
    for pat in [r'(?:\+91[\s\-]?|91[\s\-]?|0)?[6-9]\d{9}',
                r'\b0\d{2,4}[\s\-]?\d{6,8}\b']:
        for m in re.findall(pat, text):
            digits = re.sub(r"\D", "", m)
            if digits.startswith("91") and len(digits) == 12:
                digits = digits[2:]
            if len(digits) == 10 and digits[0] in "6789":
                return digits
    return ""


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
