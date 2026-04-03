"""
Lead deduplication — two modes:
1. Within a single run (same as before)
2. Cross-run: checks against master_leads.csv so same company never sent twice
"""
import re, os, logging
import pandas as pd

log = logging.getLogger(__name__)


def deduplicate(leads: list[dict]) -> list[dict]:
    """Remove duplicates within a single list."""
    seen_names  = set()
    seen_phones = set()
    unique      = []

    for lead in leads:
        norm_name = _normalise(lead.get("company_name", ""))
        phone     = _clean_phone(lead.get("phone", ""))

        if not norm_name:
            continue
        if norm_name in seen_names:
            continue
        if phone and phone in seen_phones:
            continue

        seen_names.add(norm_name)
        if phone:
            seen_phones.add(phone)
        unique.append(lead)

    log.info(f"Dedup: {len(leads)} → {len(unique)} unique leads")
    return unique


def load_master_seen(master_csv_path: str) -> set:
    """
    Load all company names previously scraped from master CSV.
    Returns a set of normalised names for fast lookup.
    """
    if not os.path.exists(master_csv_path):
        return set()
    try:
        df = pd.read_csv(master_csv_path, usecols=["company_name"])
        return {_normalise(n) for n in df["company_name"].dropna()}
    except Exception as e:
        log.error(f"Could not load master CSV: {e}")
        return set()


def save_master_seen(master_csv_path: str, new_leads: list[dict], existing_seen: set):
    """
    Append new leads to master CSV.
    existing_seen is the set loaded at the start — we don't re-add what's already there.
    """
    if not new_leads:
        return

    new_df = pd.DataFrame(new_leads)
    for col in ["company_name","contact_name","designation","phone",
                "email","website","address","industry","source","notes"]:
        if col not in new_df.columns:
            new_df[col] = ""

    if os.path.exists(master_csv_path):
        try:
            master_df = pd.read_csv(master_csv_path)
            combined  = pd.concat([master_df, new_df], ignore_index=True)
        except Exception:
            combined = new_df
    else:
        combined = new_df

    combined.to_csv(master_csv_path, index=False)
    log.info(f"Master CSV now has {len(combined)} total companies")


def _normalise(name: str) -> str:
    name = name.lower().strip()
    name = re.sub(r"\b(pvt|ltd|private|limited|llp|inc|corp|co|&)\b", "", name)
    name = re.sub(r"[^a-z0-9 ]", "", name)
    return re.sub(r"\s+", " ", name).strip()


def _clean_phone(phone: str) -> str:
    digits = re.sub(r"\D", "", phone)
    if digits.startswith("91") and len(digits) == 12:
        digits = digits[2:]
    return digits if len(digits) == 10 else ""
