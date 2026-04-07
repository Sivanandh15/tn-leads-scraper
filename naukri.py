"""
Naukri.com job post scraper — IT focused.
Companies actively hiring = have budget = good gifting leads.
Searches for IT-specific roles across all TN cities.
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

# IT roles — companies hiring these have active teams and gifting budgets
IT_ROLES = [
    "software-engineer",
    "python-developer",
    "java-developer",
    "fullstack-developer",
    "hr-manager",
    "it-manager",
    "project-manager",
    "business-development-executive",
    "react-developer",
    "data-analyst",
]


def scrape_naukri(role: str = "software-engineer", city: str = "Chennai", max_pages: int = 3) -> list[dict]:
    """Scrape Naukri for IT companies hiring `role` in `city`."""
    leads     = []
    seen      = set()
    city_slug = CITY_SLUG_MAP.get(city, city.lower())
    role_slug = role.lower().replace(" ", "-")

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
                     soup.select("div.job-container")         or
                     soup.select("div[class*='job-tuple']"))

        if not job_cards:
            log.info(f"  Naukri [{role} in {city}]: no cards on page {page}")
            break

        for card in job_cards:
            company = _text(card, [
                "a.subTitle", "div.comp-name", "span.companyName",
                "a.comp-name", "span[class*='comp-name']"
            ])
            if not company or company.lower() in seen:
                continue
            seen.add(company.lower())

            job_title = _text(card, ["a.title", "div.job-title", "a.jd-header-title"])
            location  = _text(card, [
                "span.location", "li.location", "span.locWdth",
                "span[class*='location']", "li[class*='location']"
            ])
            job_link  = _attr(card, ["a.title", "a.job-title", "a.jd-header-title"], "href") or ""
            exp       = _text(card, ["span.experience", "li.experience", "span[class*='exp']"])

            leads.append({
                "company_name": company.strip(),
                "contact_name": "",
                "designation":  _designate_from_role(role),
                "phone":        "",
                "email":        "",
                "website":      "",
                "address":      (location or city).strip(),
                "industry":     "IT / Tech",
                "source":       "Naukri (hiring signal)",
                "notes":        f"Hiring: {job_title} ({exp}) | {job_link[:80]}",
            })

        log.info(f"  Naukri [{role} in {city}] page {page}: +{len(job_cards)} cards")
        time.sleep(2.5)

    return leads


def scrape_naukri_all_it(cities: list, max_pages: int = 2) -> list[dict]:
    """Scrape IT roles across multiple cities."""
    all_leads = []
    seen_companies = set()

    for city in cities:
        for role in IT_ROLES[:5]:  # top 5 roles to avoid overloading
            leads = scrape_naukri(role, city, max_pages)
            for lead in leads:
                key = lead.get("company_name", "").lower().strip()
                if key and key not in seen_companies:
                    seen_companies.add(key)
                    all_leads.append(lead)
            time.sleep(2)

    log.info(f"Naukri total: {len(all_leads)} IT companies actively hiring")
    return all_leads


def _designate_from_role(role: str) -> str:
    r = role.lower()
    if "hr" in r:               return "HR Manager"
    if "manager" in r:          return "Manager"
    if "developer" in r:        return "Tech Team"
    if "engineer" in r:         return "Engineering Team"
    if "business" in r:         return "BD Manager"
    if "analyst" in r:          return "Analytics Team"
    return role.replace("-", " ").title()


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
