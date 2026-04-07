"""
JustDial scraper — best source for Indian SME / small IT companies.
Scrapes https://www.justdial.com/{city}/IT-Companies
Full pagination until empty.
"""
import requests, time, re, logging
from bs4 import BeautifulSoup
from fake_useragent import UserAgent

log = logging.getLogger(__name__)
ua  = UserAgent()

HEADERS = {
    "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-IN,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection":      "keep-alive",
    "Referer":         "https://www.justdial.com/",
}

# JustDial city slugs
CITY_SLUG_MAP = {
    "Chennai":     "Chennai",
    "Coimbatore":  "Coimbatore",
    "Madurai":     "Madurai",
    "Trichy":      "Trichy",
    "Salem":       "Salem",
    "Tiruppur":    "Tiruppur",
    "Vellore":     "Vellore",
    "Erode":       "Erode",
    "Tirunelveli": "Tirunelveli",
    "Thoothukudi": "Thoothukudi",
    "Dindigul":    "Dindigul",
    "Thanjavur":   "Thanjavur",
}

# IT-focused search categories on JustDial
IT_CATEGORIES = [
    ("IT-Companies",              "IT / Tech"),
    ("Software-Companies",        "IT / Tech"),
    ("Software-Development-Companies", "IT / Tech"),
    ("Web-Design-Companies",      "IT / Tech"),
    ("Mobile-App-Development",    "IT / Tech"),
    ("Digital-Marketing-Companies","IT / Tech"),
    ("IT-Services",               "IT / Tech"),
    ("Computer-Training-Institutes","IT / Tech"),
    ("BPO-Companies",             "IT / Tech"),
    ("Data-Analytics-Companies",  "IT / Tech"),
]


def scrape_justdial_it(city: str, max_pages: int = 5) -> list[dict]:
    """Scrape JustDial IT company listings for a given city."""
    all_leads = []
    city_slug = CITY_SLUG_MAP.get(city, city)

    for category, industry in IT_CATEGORIES:
        leads = _scrape_category(city_slug, city, category, industry, max_pages)
        all_leads.extend(leads)
        log.info(f"  JustDial [{category}/{city}]: {len(leads)} leads")
        time.sleep(3)

    return all_leads


def _scrape_category(city_slug: str, city: str, category: str, industry: str, max_pages: int) -> list[dict]:
    leads = []
    seen  = set()

    for page in range(1, max_pages + 1):
        if page == 1:
            url = f"https://www.justdial.com/{city_slug}/{category}"
        else:
            url = f"https://www.justdial.com/{city_slug}/{category}/page-{page}"

        try:
            headers = {**HEADERS, "User-Agent": ua.random}
            resp    = requests.get(url, headers=headers, timeout=20)
            if resp.status_code == 403:
                log.warning(f"  JustDial 403 on {url} — skipping")
                break
            resp.raise_for_status()
        except Exception as e:
            log.error(f"JustDial error [{url}]: {e}")
            break

        soup  = BeautifulSoup(resp.text, "lxml")

        # JustDial uses multiple layouts — try all selectors
        cards = (soup.select("li.cntanr")           or
                 soup.select("div.resultbox_info")   or
                 soup.select("div.jdcard")           or
                 soup.select("section.jdcard-sec")   or
                 soup.select("li[class*='resultbox']"))

        if not cards:
            log.info(f"  JustDial [{category}/{city_slug}]: no cards on page {page}, stopping")
            break

        for card in cards:
            name  = _text(card, [
                "span.lng_lst_wrap", "h2.comp-name", "a.comp-name",
                "span.jdcard-title", "p.shop-title", "h2", "h3"
            ])
            if not name or name in seen:
                continue
            seen.add(name)

            phone   = _extract_phone_from_card(card)
            address = _text(card, [
                "span.cont_fl_addr", "p.address", "span.address",
                "div.jdcard-address", "p.jdcard-address"
            ])
            website = _attr(card, ["a.weblink","a[href*='http']"], "href")
            rating  = _text(card, ["span.jdcard-stars","span.rating","span[class*='star']"])

            leads.append({
                "company_name": name.strip(),
                "contact_name": "",
                "designation":  "",
                "phone":        phone,
                "email":        "",
                "website":      website if website and website.startswith("http") else "",
                "address":      f"{address.strip()}, {city}".strip(", ") if address else city,
                "industry":     industry,
                "source":       "JustDial",
                "notes":        f"Rating: {rating} | {city}, TN" if rating else f"{city}, TN",
            })

        log.info(f"  JustDial [{category}/{city_slug}] page {page}: +{len(cards)} entries")
        time.sleep(2.5)

    return leads


def _extract_phone_from_card(card) -> str:
    """Try multiple selectors and also scan raw text for phone numbers."""
    # Try direct selectors first
    for sel in ["span.contact-info", "p.phone", "span.mobilesv",
                "a[href^='tel:']", "span[class*='phone']"]:
        el = card.select_one(sel)
        if el:
            # href="tel:XXXXXXXXXX"
            href = el.get("href", "")
            if href.startswith("tel:"):
                digits = re.sub(r"\D", "", href)
                if digits.startswith("91") and len(digits) == 12:
                    digits = digits[2:]
                if len(digits) == 10 and digits[0] in "6789":
                    return digits
            text = el.get_text(strip=True)
            phone = _clean_phone(text)
            if phone:
                return phone

    # Scan raw text of card
    return _clean_phone(card.get_text(" "))


def _clean_phone(text: str) -> str:
    patterns = [
        r'(?:\+91[\s\-]?|91[\s\-]?|0)?[6-9]\d{9}',
        r'\b0\d{2,4}[\s\-]?\d{6,8}\b',
    ]
    for pat in patterns:
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
