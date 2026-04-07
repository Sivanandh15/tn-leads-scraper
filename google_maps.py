"""
Business scraper using Overpass API (OpenStreetMap) — free, no API key.
IT-focused: uses broader tags that actually exist in Indian OSM data.
"""
import requests, time, logging

log = logging.getLogger(__name__)
OVERPASS_URL = "https://overpass-api.de/api/interpreter"

# IT-focused OSM tags — uses broader tags that are actually mapped in India
QUERY_TO_OSM_TAGS = {
    # Core IT — broad tags since Indian OSM data rarely uses office=it specifically
    "it company":         [("office","it"), ("office","software"), ("office","technology"),
                           ("office","company"), ("building","office")],
    "software company":   [("office","software"), ("office","it"), ("office","technology"),
                           ("office","company")],
    "bpo company":        [("office","company"), ("office","it")],
    "tech startup":       [("office","it"), ("office","software"), ("office","company")],
    "web design":         [("office","it"), ("office","software")],
    "digital marketing":  [("office","advertising"), ("office","marketing"), ("office","company")],

    # Other industries (secondary fallback only)
    "manufacturing company":   [("office","company"), ("landuse","industrial"), ("building","industrial")],
    "real estate company":     [("office","real_estate"), ("office","estate_agent")],
    "construction company":    [("office","construction"), ("office","company")],
    "bank":                    [("amenity","bank")],
    "logistics company":       [("office","logistics"), ("office","company")],
    "hotel":                   [("tourism","hotel"), ("tourism","guest_house")],
    "automobile dealer":       [("shop","car"), ("shop","vehicle")],

    # catch-all fallback
    "company":            [("office","company"), ("office","yes"), ("building","office")],
}

BLOCKED_OSM_TAGS = {
    ("office", "pharmaceutical"),
    ("amenity", "hospital"),
    ("amenity", "clinic"),
    ("healthcare", "hospital"),
    ("amenity", "school"),
    ("amenity", "college"),
    ("amenity", "university"),
}

CITY_BBOX = {
    "Chennai":     (12.82, 80.08, 13.23, 80.33),
    "Coimbatore":  (10.85, 76.87, 11.10, 77.12),
    "Madurai":     ( 9.84, 78.01,  9.99, 78.24),
    "Trichy":      (10.70, 78.58, 10.90, 78.82),
    "Salem":       (11.56, 78.04, 11.74, 78.28),
    "Tiruppur":    (11.06, 77.28, 11.20, 77.49),
    "Vellore":     (12.87, 79.06, 13.01, 79.22),
    "Erode":       (11.28, 77.63, 11.44, 77.78),
    "Tirunelveli": ( 8.64, 77.63,  8.80, 77.84),
    "Thoothukudi": ( 8.70, 78.08,  8.86, 78.23),
    "Dindigul":    (10.33, 77.88, 10.44, 78.02),
    "Thanjavur":   (10.72, 79.08, 10.82, 79.20),
    "Bangalore":   (12.83, 77.46, 13.14, 77.78),
    "Mumbai":      (18.89, 72.77, 19.27, 72.99),
    "Delhi":       (28.40, 76.84, 28.88, 77.35),
    "Hyderabad":   (17.27, 78.27, 17.57, 78.62),
    "Pune":        (18.42, 73.74, 18.63, 73.98),
}

DEFAULT_BBOX = (12.82, 80.08, 13.23, 80.33)


def scrape_google_maps(query: str, city: str, api_key: str = "", max_results: int = 500) -> list[dict]:
    q_lower = query.lower()
    if "pharma" in q_lower or "hospital" in q_lower or "clinic" in q_lower:
        return []

    bbox     = CITY_BBOX.get(city, DEFAULT_BBOX)
    tags     = _resolve_tags(query)
    leads    = []
    seen_ids = set()

    for tag_key, tag_val in tags:
        if (tag_key, tag_val) in BLOCKED_OSM_TAGS:
            continue
        try:
            results = _query_overpass(tag_key, tag_val, bbox, limit=max_results)
            for element in results:
                eid = element.get("id")
                if eid in seen_ids:
                    continue
                seen_ids.add(eid)
                lead = _element_to_lead(element, query, city)
                if lead:
                    leads.append(lead)
            time.sleep(1.5)
        except Exception as e:
            log.error(f"Overpass error [{tag_key}={tag_val} in {city}]: {e}")

    return leads


def _resolve_tags(query: str) -> list[tuple]:
    q = query.lower().strip()
    for keyword, tags in QUERY_TO_OSM_TAGS.items():
        if keyword in q:
            return tags
    return [("office","company"), ("office","yes"), ("building","office")]


def _query_overpass(tag_key: str, tag_val: str, bbox: tuple, limit: int = 500) -> list[dict]:
    south, west, north, east = bbox
    bbox_str = f"{south},{west},{north},{east}"
    overpass_ql = f"""
    [out:json][timeout:60];
    (
      node["{tag_key}"="{tag_val}"]({bbox_str});
      way["{tag_key}"="{tag_val}"]({bbox_str});
      relation["{tag_key}"="{tag_val}"]({bbox_str});
    );
    out body center {limit};
    """
    resp = requests.post(
        OVERPASS_URL,
        data={"data": overpass_ql},
        timeout=65,
        headers={"User-Agent": "LeadScraper/2.0 (corporate gifting research)"},
    )
    resp.raise_for_status()
    return resp.json().get("elements", [])


def _element_to_lead(element: dict, query: str, city: str) -> dict | None:
    tags = element.get("tags", {})
    name = tags.get("name") or tags.get("name:en", "")
    if not name:
        return None

    # Skip schools / hospitals that slipped through
    amenity = tags.get("amenity", "")
    if amenity in ("school", "college", "university", "hospital", "clinic"):
        return None

    addr_parts = [
        tags.get("addr:housenumber", ""),
        tags.get("addr:street", ""),
        tags.get("addr:suburb", ""),
        tags.get("addr:city", city),
        tags.get("addr:postcode", ""),
    ]
    address  = ", ".join(p for p in addr_parts if p) or city
    phone    = tags.get("phone") or tags.get("contact:phone") or tags.get("contact:mobile", "")
    website  = tags.get("website") or tags.get("contact:website") or tags.get("url", "")
    email    = tags.get("email") or tags.get("contact:email", "")
    osm_id   = element.get("id", "")
    osm_type = element.get("type", "node")
    industry = _guess_industry(query)

    if "pharma" in industry.lower() or "healthcare" in industry.lower():
        return None

    return {
        "company_name": name,
        "contact_name": "",
        "designation":  "",
        "phone":        phone,
        "email":        email,
        "website":      website,
        "address":      address,
        "industry":     industry,
        "source":       "OpenStreetMap",
        "notes":        f"osm/{osm_type}/{osm_id} | {city}, TN",
    }


def _guess_industry(query: str) -> str:
    q = query.lower()
    if any(k in q for k in ("it ", "software", "tech", "bpo", "web", "digital", "startup")):
        return "IT / Tech"
    if "real estate" in q or "construction" in q: return "Real Estate / Construction"
    if any(k in q for k in ("bank","insurance","ca ","financial")):         return "BFSI"
    if "fmcg" in q or "retail" in q or "food" in q:  return "FMCG / Retail / Food"
    if "manufactur" in q or "textile" in q:           return "Manufacturing / Textile"
    if "logistics" in q:                              return "Logistics / Transport"
    if "hotel" in q:                                  return "Hospitality"
    if "automobile" in q:                             return "Automobile"
    if "advertis" in q or "event" in q:              return "Media / Events"
    return "Other"
