"""
Naukri.com job post scraper.
Updated: takes city+role as parameters (called per city from main.py).
Paginated — gets all results per search.
Companies actively hiring HR/Admin/Procurement = have gifting budget.
"""
import requests, time, logging
from bs4 import BeautifulSoup
from fake_useragent import UserAgent

log = logging.getLogger(__name__)
ua  = UserAgent()

HEADERS = {
    "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-IN,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection":      "keep-alive",
    "Referer":         "https://www.naukri.com/",
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


def scrape_naukri(role: str = "hr manager", city: str = "chennai", max_pages: int = 3) -> list[dict]:
    """Scrape Naukri for companies hiring `role` in `city`."""
    leads     = []
    seen      = set()
    city_slug = CITY_SLUG_MAP.get(city, city.lower())
    role_slug = role.replace(" ", "-")

    for page in range(1, max_pages + 1):
        if page == 1:
            url = f"https://www.naukri.com/{role_slug}-jobs-in-{city_slug}"
        else:
            url = f"https://www.naukri.com/{role_slug}-jobs-in-{city_slug}-{page}"

        try:
            headers = {**HEADERS, "User-Agent": ua.random}
            resp    = requests.get(url, headers=headers, timeout=15)
            resp.raise_for_status()
        except Exception as e:
            log.error(f"Naukri error [{url}]: {e}")
            time.sleep(3)
            break

        soup      = BeautifulSoup(resp.text, "lxml")
        job_cards = (soup.select("article.jobTuple")          or
                     soup.select("div.jobTuple")              or
                     soup.select("div.srp-jobtuple-wrapper")  or
                     soup.select("div.job-container"))

        if not job_cards:
            log.info(f"  Naukri [{role} in {city}]: no cards on page {page}")
            break

        for card in job_cards:
            company = _text(card, ["a.subTitle","div.comp-name","span.companyName","a.comp-name"])
            if not company or company in seen:
                continue
            seen.add(company)

            job_title = _text(card, ["a.title","div.job-title","a.jd-header-title"])
            location  = _text(card, ["span.location","li.location","span.locWdth","span.ni-job-tuple-icon-srp-location"])
            job_link  = _attr(card, ["a.title","a.job-title","a.jd-header-title"], "href") or ""

            leads.append({
                "company_name": company.strip(),
                "contact_name": "",
                "designation":  _designate_from_role(role),
                "phone":        "",
                "email":        "",
                "website":      "",
                "address":      (location or city).strip(),
                "industry":     "Mixed — actively hiring",
                "source":       "Naukri (hiring signal)",
                "notes":        f"Hiring: {job_title} | {job_link[:80]}",
            })

        log.info(f"  Naukri [{role} in {city}] page {page}: +{len(job_cards)} cards")
        time.sleep(2.5)

    return leads


def _designate_from_role(role: str) -> str:
    r = role.lower()
    if "hr" in r:          return "HR Manager"
    if "admin" in r:       return "Admin Manager"
    if "procurement" in r: return "Procurement Manager"
    if "office" in r:      return "Office Manager"
    return role.title()


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
