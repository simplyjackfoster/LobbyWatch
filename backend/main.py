import json
import os
import re
import time
from difflib import SequenceMatcher
from typing import Optional
from urllib import error as urllib_error
from urllib import parse as urllib_parse
from urllib import request as urllib_request

from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
import yaml
from sqlalchemy import text
from sqlalchemy.orm import Session

from graph import get_entity_summary, get_issue_graph, get_legislator_graph, get_organization_graph
from models import SessionLocal
from search import search_entities

CONGRESS_API_KEY = os.getenv("CONGRESS_API_KEY")
GOOGLE_CIVIC_API_KEY = os.getenv("GOOGLE_CIVIC_API_KEY")
PERSON_TITLE_RE = re.compile(r"^(SEN(?:ATOR)?|REP(?:RESENTATIVE)?|CONGRESS(?:MAN|WOMAN)?)[\.\s]+", re.IGNORECASE)
NON_WORD_RE = re.compile(r"[^A-Z0-9\s]")
INDUSTRY_LABELS = {
    "HLTH": "Health & Pharma",
    "FIN": "Finance",
    "REAL": "Real Estate",
    "ENRG": "Energy",
    "TECH": "Technology",
    "DEF": "Defense",
    "AGR": "Agriculture",
    "TRAN": "Transportation",
}
STATE_CODE_BY_NAME = {
    "ALABAMA": "AL", "ALASKA": "AK", "ARIZONA": "AZ", "ARKANSAS": "AR", "CALIFORNIA": "CA",
    "COLORADO": "CO", "CONNECTICUT": "CT", "DELAWARE": "DE", "FLORIDA": "FL", "GEORGIA": "GA",
    "HAWAII": "HI", "IDAHO": "ID", "ILLINOIS": "IL", "INDIANA": "IN", "IOWA": "IA",
    "KANSAS": "KS", "KENTUCKY": "KY", "LOUISIANA": "LA", "MAINE": "ME", "MARYLAND": "MD",
    "MASSACHUSETTS": "MA", "MICHIGAN": "MI", "MINNESOTA": "MN", "MISSISSIPPI": "MS", "MISSOURI": "MO",
    "MONTANA": "MT", "NEBRASKA": "NE", "NEVADA": "NV", "NEW HAMPSHIRE": "NH", "NEW JERSEY": "NJ",
    "NEW MEXICO": "NM", "NEW YORK": "NY", "NORTH CAROLINA": "NC", "NORTH DAKOTA": "ND", "OHIO": "OH",
    "OKLAHOMA": "OK", "OREGON": "OR", "PENNSYLVANIA": "PA", "RHODE ISLAND": "RI", "SOUTH CAROLINA": "SC",
    "SOUTH DAKOTA": "SD", "TENNESSEE": "TN", "TEXAS": "TX", "UTAH": "UT", "VERMONT": "VT",
    "VIRGINIA": "VA", "WASHINGTON": "WA", "WEST VIRGINIA": "WV", "WISCONSIN": "WI", "WYOMING": "WY",
    "DISTRICT OF COLUMBIA": "DC",
}
ISSUE_TO_INDUSTRY = {
    "HLTH": "HLTH",
    "PHARM": "HLTH",
    "FIN": "FIN",
    "BANK": "FIN",
    "TAX": "FIN",
    "REAL": "REAL",
    "HOUS": "REAL",
    "ENRG": "ENRG",
    "ENER": "ENRG",
    "TECH": "TECH",
    "DEF": "DEF",
    "AGR": "AGR",
    "TRAN": "TRAN",
}
ZIP_FALLBACK = {
    "19401": ["F000479", "F000466"],
    "10001": ["S000148", "G000555", "N000002"],
    "90001": ["P000197", "P000145", "B001300"],
    "60601": ["D000563", "D000622", "C001126"],
    "30301": ["W000790", "O000174", "N000026"],
    "77001": ["C001098", "C001056", "N000060"],
    "33101": ["S001191", "R000595", "B001287"],
    "02108": ["W000817", "M000133", "L000602"],
    "98101": ["M001176", "C001075", "J000298"],
    "80202": ["B001267", "H001052", "D000216"],
}
LEGISLATORS_CURRENT_YAML_URL = "https://raw.githubusercontent.com/unitedstates/congress-legislators/main/legislators-current.yaml"
_LEGISLATOR_DIRECTORY_CACHE: dict = {"loaded_at": 0.0, "senators_by_state": {}, "rep_by_state_district": {}}
_LEGISLATOR_DIRECTORY_TTL = 6 * 60 * 60

app = FastAPI(title="LobbyWatch API", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


AGENCY_ISSUE_MAP = {
    "FDA": {"HLTH"},
    "HHS": {"HLTH"},
    "CMS": {"HLTH"},
    "NIH": {"HLTH"},
    "EPA": {"ENV", "ENRG"},
    "SEC": {"FIN"},
    "FTC": {"FIN", "TRAD"},
    "DOD": {"DEF"},
    "DOE": {"ENRG"},
    "USDA": {"AGR"},
    "DOT": {"TRN", "TRAD"},
    "FCC": {"TEC", "TRAD"},
}


def compute_issue_relevance(
    agency: Optional[str],
    issue_codes: set[str],
    issue_code_filter: Optional[str],
) -> float:
    relevance = 1.0
    if issue_code_filter and issue_code_filter in issue_codes:
        relevance += 0.5
    if agency:
        mapped = AGENCY_ISSUE_MAP.get(agency.upper(), set())
        if mapped and issue_codes.intersection(mapped):
            relevance += 0.5
    return relevance


def normalize_person_name(name: str) -> str:
    value = (name or "").upper().strip()
    value = PERSON_TITLE_RE.sub("", value)
    value = NON_WORD_RE.sub(" ", value)
    value = re.sub(r"\s+", " ", value).strip()
    return value


def normalize_state_code(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    text_value = str(value).strip().upper()
    if len(text_value) == 2:
        return text_value
    return STATE_CODE_BY_NAME.get(text_value, text_value)


def normalize_chamber(value: Optional[str]) -> str:
    raw = str(value or "").strip().lower()
    if "senate" in raw:
        return "senate"
    if "house" in raw:
        return "house"
    return raw


def split_title_and_name(name: str, chamber: Optional[str]) -> tuple[str, str]:
    clean = (name or "").strip()
    title = "Sen." if (chamber or "").lower() == "senate" else "Rep."
    if clean.upper().startswith("SEN. "):
        title = "Sen."
        clean = clean[5:]
    elif clean.upper().startswith("REP. "):
        title = "Rep."
        clean = clean[5:]
    return title, clean.strip()


def reorder_last_first(name: str) -> str:
    raw = (name or "").strip()
    if "," not in raw:
        return raw
    parts = [part.strip() for part in raw.split(",", 1)]
    if len(parts) != 2:
        return raw
    return f"{parts[1]} {parts[0]}".strip()


def fetch_json_from_url(url: str, timeout: int = 10) -> dict:
    req = urllib_request.Request(
        url,
        headers={
            "Accept": "application/json",
            "User-Agent": "LobbyWatch/1.0",
        },
    )
    with urllib_request.urlopen(req, timeout=timeout) as resp:
        payload = resp.read().decode("utf-8", errors="replace")
        return json.loads(payload)


def fetch_congress_members_by_zip(zipcode: str) -> list[dict]:
    params = {"zip": zipcode}
    if CONGRESS_API_KEY:
        params["api_key"] = CONGRESS_API_KEY
    url = f"https://api.congress.gov/v3/member?{urllib_parse.urlencode(params)}"
    try:
        payload = fetch_json_from_url(url)
    except Exception:
        return []

    members = payload.get("members") or payload.get("results") or []
    parsed = []
    for member in members:
        if not isinstance(member, dict):
            continue
        name = reorder_last_first(member.get("name") or "")
        chamber = normalize_chamber(member.get("chamber"))
        title, display_name = split_title_and_name(name, chamber)
        if not display_name:
            continue
        parsed.append(
            {
                "name": display_name,
                "title": title,
                "party": member.get("party"),
                "state": normalize_state_code(member.get("state")),
                "chamber": chamber,
            }
        )
    return parsed


def parse_state_from_division_id(division_id: str) -> Optional[str]:
    if not division_id:
        return None
    match = re.search(r"/state:([a-z]{2})", division_id, re.IGNORECASE)
    if not match:
        return None
    return match.group(1).upper()


def fetch_google_civic_members_by_zip(zipcode: str) -> list[dict]:
    if not GOOGLE_CIVIC_API_KEY:
        return []

    params = {
        "address": zipcode,
        "levels": "country",
        "roles": "legislatorUpperBody,legislatorLowerBody",
        "key": GOOGLE_CIVIC_API_KEY,
    }
    url = f"https://www.googleapis.com/civicinfo/v2/representatives?{urllib_parse.urlencode(params)}"
    try:
        payload = fetch_json_from_url(url)
    except Exception:
        return []

    offices = payload.get("offices") or []
    officials = payload.get("officials") or []
    parsed = []
    for office in offices:
        if not isinstance(office, dict):
            continue
        roles = office.get("roles") or []
        levels = office.get("levels") or []
        if "country" not in levels:
            continue
        if "legislatorUpperBody" in roles:
            chamber = "senate"
        elif "legislatorLowerBody" in roles:
            chamber = "house"
        else:
            continue

        division_state = normalize_state_code(parse_state_from_division_id(office.get("divisionId") or ""))
        for idx in office.get("officialIndices") or []:
            if not isinstance(idx, int):
                continue
            if idx < 0 or idx >= len(officials):
                continue
            official = officials[idx]
            if not isinstance(official, dict):
                continue

            name = reorder_last_first(official.get("name") or "")
            title, display_name = split_title_and_name(name, chamber)
            if not display_name:
                continue
            parsed.append(
                {
                    "name": display_name,
                    "title": title,
                    "party": official.get("party"),
                    "state": division_state,
                    "chamber": chamber,
                }
            )

    unique = []
    seen = set()
    for row in parsed:
        key = (normalize_person_name(row.get("name") or ""), row.get("chamber"), row.get("state"))
        if key in seen:
            continue
        seen.add(key)
        unique.append(row)
    return unique


def fetch_state_code_by_zip(zipcode: str) -> Optional[str]:
    url = f"https://api.zippopotam.us/us/{zipcode}"
    try:
        payload = fetch_json_from_url(url)
    except Exception:
        return None

    places = payload.get("places") or []
    if not places:
        return None
    place = places[0] if isinstance(places[0], dict) else {}
    state_code = place.get("state abbreviation") or place.get("state_abbreviation")
    if state_code:
        return normalize_state_code(state_code)
    return normalize_state_code(place.get("state"))


def fetch_zip_coordinates(zipcode: str) -> tuple[Optional[float], Optional[float], Optional[str]]:
    url = f"https://api.zippopotam.us/us/{zipcode}"
    try:
        payload = fetch_json_from_url(url)
    except Exception:
        return None, None, None

    places = payload.get("places") or []
    if not places:
        return None, None, None
    place = places[0] if isinstance(places[0], dict) else {}
    state_code = normalize_state_code(place.get("state abbreviation") or place.get("state_abbreviation"))
    try:
        lat = float(place.get("latitude"))
        lon = float(place.get("longitude"))
    except Exception:
        return None, None, state_code
    return lat, lon, state_code


def fetch_congressional_district_for_point(lat: float, lon: float) -> Optional[int]:
    params = {
        "x": lon,
        "y": lat,
        "benchmark": "Public_AR_Current",
        "vintage": "Current_Current",
        "format": "json",
    }
    url = f"https://geocoding.geo.census.gov/geocoder/geographies/coordinates?{urllib_parse.urlencode(params)}"
    try:
        payload = fetch_json_from_url(url)
    except Exception:
        return None

    geographies = ((payload.get("result") or {}).get("geographies") or {})
    cd_rows = []
    for key, rows in geographies.items():
        if "Congressional Districts" in key and isinstance(rows, list):
            cd_rows = rows
            break
    if not cd_rows:
        return None
    row = cd_rows[0] if isinstance(cd_rows[0], dict) else {}
    raw_cd = row.get("CD119") or row.get("BASENAME") or row.get("NAME")
    if raw_cd is None:
        return None
    text_value = str(raw_cd).strip()
    match = re.search(r"(\d{1,2})", text_value)
    if not match:
        return None
    return int(match.group(1))


def current_legislator_directory() -> dict:
    now = time.time()
    if (
        _LEGISLATOR_DIRECTORY_CACHE["loaded_at"]
        and (now - _LEGISLATOR_DIRECTORY_CACHE["loaded_at"]) < _LEGISLATOR_DIRECTORY_TTL
    ):
        return _LEGISLATOR_DIRECTORY_CACHE

    try:
        req = urllib_request.Request(
            LEGISLATORS_CURRENT_YAML_URL,
            headers={"User-Agent": "LobbyWatch/1.0"},
        )
        with urllib_request.urlopen(req, timeout=30) as resp:
            payload = resp.read().decode("utf-8", errors="replace")
        records = yaml.safe_load(payload) or []
    except Exception:
        return _LEGISLATOR_DIRECTORY_CACHE

    senators_by_state: dict[str, list[str]] = {}
    rep_by_state_district: dict[tuple[str, int], str] = {}
    for row in records:
        if not isinstance(row, dict):
            continue
        bioguide = ((row.get("id") or {}).get("bioguide") or "").strip()
        if not bioguide:
            continue
        terms = row.get("terms") or []
        if not terms:
            continue
        term = terms[-1] if isinstance(terms[-1], dict) else {}
        state_code = normalize_state_code(term.get("state"))
        if not state_code:
            continue
        term_type = str(term.get("type") or "").strip().lower()
        if term_type == "sen":
            senators_by_state.setdefault(state_code, []).append(bioguide)
            continue
        if term_type == "rep":
            district_raw = term.get("district")
            try:
                district = int(district_raw)
            except Exception:
                continue
            rep_by_state_district[(state_code, district)] = bioguide

    _LEGISLATOR_DIRECTORY_CACHE["loaded_at"] = now
    _LEGISLATOR_DIRECTORY_CACHE["senators_by_state"] = senators_by_state
    _LEGISLATOR_DIRECTORY_CACHE["rep_by_state_district"] = rep_by_state_district
    return _LEGISLATOR_DIRECTORY_CACHE


def direct_candidates_for_zip(db: Session, zipcode: str) -> list[dict]:
    lat, lon, state_code = fetch_zip_coordinates(zipcode)
    if lat is None or lon is None or not state_code:
        return []
    district = fetch_congressional_district_for_point(lat, lon)
    if district is None:
        return []

    directory = current_legislator_directory()
    senators = list((directory.get("senators_by_state") or {}).get(state_code, []))[:2]
    house_bioguide = (directory.get("rep_by_state_district") or {}).get((state_code, district))

    ordered_bioguide_ids = [*senators]
    if house_bioguide:
        ordered_bioguide_ids.append(house_bioguide)
    if not ordered_bioguide_ids:
        return []

    return fetch_legislators_by_bioguide_ids(db, ordered_bioguide_ids)


def fetch_legislators_by_bioguide_ids(db: Session, bioguide_ids: list[str]) -> list[dict]:
    if not bioguide_ids:
        return []
    rows = db.execute(
        text(
            """
            SELECT id, bioguide_id, name, party, state, chamber
            FROM legislators
            WHERE bioguide_id = ANY(:ids)
            """
        ),
        {"ids": bioguide_ids},
    ).all()
    by_id = {row.bioguide_id: row for row in rows}
    ordered = []
    for bid in bioguide_ids:
        row = by_id.get(bid)
        if not row:
            continue
        ordered.append(
            {
                "id": row.id,
                "bioguide_id": row.bioguide_id,
                "name": row.name,
                "party": row.party,
                "state": row.state,
                "chamber": normalize_chamber(row.chamber),
            }
        )
    return ordered


def build_betrayal_map(db: Session) -> dict[str, dict]:
    payload = betrayal_index(issue_code="HLTH", min_contribution=10000, contribution_window_days=365, db=db)
    return {
        normalize_person_name(row["legislator"]["name"]): {
            "betrayal_score": row.get("betrayal_score"),
            "issue_code": "HLTH",
        }
        for row in payload.get("findings", [])
    }


def fallback_candidates_for_zip(db: Session, zipcode: str) -> list[dict]:
    ids = ZIP_FALLBACK.get(zipcode, [])
    return fetch_legislators_by_bioguide_ids(db, ids)


def fallback_candidates_for_state(db: Session, state_code: Optional[str]) -> list[dict]:
    if not state_code:
        return []

    rows = db.execute(
        text(
            """
            SELECT id, bioguide_id, name, party, state, chamber
            FROM legislators
            WHERE is_active = TRUE
            ORDER BY name
            """
        )
    ).all()

    in_state = []
    for row in rows:
        if normalize_state_code(row.state) != state_code:
            continue
        in_state.append(
            {
                "id": row.id,
                "bioguide_id": row.bioguide_id,
                "name": row.name,
                "party": row.party,
                "state": row.state,
                "chamber": normalize_chamber(row.chamber),
            }
        )

    if not in_state:
        return []

    senate = [row for row in in_state if row.get("chamber") == "senate"]
    house = [row for row in in_state if row.get("chamber") == "house"]

    selected: list[dict] = []
    selected.extend(senate[:2])
    if house:
        selected.append(house[0])

    seen = {row["bioguide_id"] for row in selected}
    for row in in_state:
        if len(selected) >= 3:
            break
        if row["bioguide_id"] in seen:
            continue
        selected.append(row)
        seen.add(row["bioguide_id"])

    return selected


def ranked_legislator_matches(db: Session, candidates: list[dict]) -> tuple[list[dict], list[dict]]:
    matched = []
    unmatched = []
    seen_bioguide = set()
    for candidate in candidates:
        legislator = lookup_legislator(db, candidate)
        if not legislator:
            unmatched.append({"name": candidate.get("name"), "state": candidate.get("state"), "chamber": candidate.get("chamber")})
            continue
        if legislator["bioguide_id"] in seen_bioguide:
            continue
        seen_bioguide.add(legislator["bioguide_id"])
        matched.append(legislator)
    return matched, unmatched


def lookup_legislator(db: Session, candidate: dict) -> Optional[dict]:
    state = normalize_state_code(candidate.get("state"))
    chamber = normalize_chamber(candidate.get("chamber")) or None
    candidate_party = str(candidate.get("party") or "").strip().upper()
    if candidate_party.startswith("D"):
        candidate_party = "D"
    elif candidate_party.startswith("R"):
        candidate_party = "R"
    elif candidate_party.startswith("I"):
        candidate_party = "I"
    else:
        candidate_party = ""
    rows = db.execute(
        text(
            """
            SELECT id, bioguide_id, name, party, state, chamber
            FROM legislators
            WHERE is_active = TRUE
              AND (CAST(:state AS text) IS NULL OR state = CAST(:state AS text))
              AND (CAST(:chamber AS text) IS NULL OR LOWER(chamber) = CAST(:chamber AS text))
            """
        ),
        {"state": state, "chamber": chamber},
    ).all()
    if not rows:
        rows = db.execute(
            text(
                """
                SELECT id, bioguide_id, name, party, state, chamber
                FROM legislators
                WHERE is_active = TRUE
                """
            )
        ).all()

    cand_norm = normalize_person_name(reorder_last_first(candidate.get("name") or ""))
    suffixes = {"JR", "SR", "II", "III", "IV", "V"}
    cand_parts = cand_norm.split()
    while cand_parts and cand_parts[-1] in suffixes:
        cand_parts.pop()
    cand_tokens = set(cand_parts)
    cand_last = cand_parts[-1] if cand_parts else ""
    best = None
    best_score = 0.0

    for row in rows:
        row_norm = normalize_person_name(reorder_last_first(row.name or ""))
        row_parts = row_norm.split()
        while row_parts and row_parts[-1] in suffixes:
            row_parts.pop()
        row_tokens = set(row_parts)
        row_last = row_parts[-1] if row_parts else ""
        if not row_norm:
            continue
        if cand_last and row_last and cand_last != row_last:
            continue
        if candidate_party:
            row_party = str(row.party or "").strip().upper()
            if not row_party.startswith(candidate_party):
                continue

        ratio = SequenceMatcher(None, cand_norm, row_norm).ratio()
        overlap = (len(cand_tokens.intersection(row_tokens)) / max(len(cand_tokens), 1)) if cand_tokens else 0.0
        score = (ratio * 0.7) + (overlap * 0.3)
        if cand_last and cand_last in row_tokens:
            score += 0.08
        if chamber and str(row.chamber or "").lower() == chamber:
            score += 0.05

        if score > best_score:
            best_score = score
            best = row

    if not best or best_score < 0.5:
        return None
    return {
        "id": best.id,
        "bioguide_id": best.bioguide_id,
        "name": best.name,
        "party": best.party,
        "state": best.state,
        "chamber": str(best.chamber or "").lower(),
    }


def build_representative_payload(
    db: Session,
    legislator: dict,
    betrayal_by_name: dict[str, dict],
) -> dict:
    leg_id = legislator["id"]
    bioguide_id = legislator["bioguide_id"]
    summary = get_entity_summary(db, entity_type="legislator", entity_id=bioguide_id)

    industry_rows = db.execute(
        text(
            """
            SELECT COALESCE(o.industry_code, 'OTHER') AS industry_code, SUM(c.amount) AS total
            FROM contributions c
            JOIN organizations o ON o.id = c.contributor_org_id
            WHERE c.recipient_legislator_id = :legislator_id
            GROUP BY COALESCE(o.industry_code, 'OTHER')
            ORDER BY total DESC
            LIMIT 5
            """
        ),
        {"legislator_id": leg_id},
    ).all()
    top_industries = [
        {
            "industry_code": row.industry_code,
            "label": INDUSTRY_LABELS.get(row.industry_code, row.industry_code),
            "total": float(row.total or 0),
        }
        for row in industry_rows
    ]

    vote_rows = db.execute(
        text(
            """
            SELECT
              bill_id,
              bill_title,
              vote_position AS position,
              vote_date::text AS date,
              COALESCE((issue_tags)[1], '') AS issue_code
            FROM votes
            WHERE legislator_id = :legislator_id
            ORDER BY vote_date DESC NULLS LAST, id DESC
            LIMIT 20
            """
        ),
        {"legislator_id": leg_id},
    ).all()
    recent_votes = [
        {
            "bill_id": row.bill_id,
            "bill_title": row.bill_title,
            "position": row.position,
            "date": row.date,
            "issue_code": row.issue_code or None,
        }
        for row in vote_rows
    ]

    co_count = int(
        db.execute(
            text("SELECT COUNT(*) FROM co_sponsorships WHERE legislator_id = :legislator_id"),
            {"legislator_id": leg_id},
        ).scalar()
        or 0
    )
    co_by_issue = []
    try:
        co_issue_rows = db.execute(
            text(
                """
                SELECT COALESCE(issue_code, 'OTHER') AS issue_code, COUNT(*) AS count
                FROM co_sponsorships
                WHERE legislator_id = :legislator_id
                GROUP BY COALESCE(issue_code, 'OTHER')
                ORDER BY count DESC
                LIMIT 10
                """
            ),
            {"legislator_id": leg_id},
        ).all()
        co_by_issue = [{"issue_code": row.issue_code, "count": int(row.count or 0)} for row in co_issue_rows]
    except Exception:
        db.rollback()
        co_by_issue = []

    committee_ids = db.execute(
        text("SELECT committee_id FROM committee_memberships WHERE legislator_id = :legislator_id"),
        {"legislator_id": leg_id},
    ).scalars().all()

    total_lobbying_filings_on_committees = 0
    lobbying_spend_on_committees = 0.0
    if committee_ids:
        member_ids = db.execute(
            text(
                """
                SELECT DISTINCT legislator_id
                FROM committee_memberships
                WHERE committee_id = ANY(:committee_ids)
                """
            ),
            {"committee_ids": committee_ids},
        ).scalars().all()
        if member_ids:
            filings_row = db.execute(
                text(
                    """
                    SELECT
                      COUNT(DISTINCT lr.id) AS filing_count,
                      COALESCE(SUM(lr.amount), 0) AS total_amount
                    FROM lobbying_registrations lr
                    JOIN contributions c ON c.contributor_org_id = lr.client_id
                    WHERE c.recipient_legislator_id = ANY(:member_ids)
                    """
                ),
                {"member_ids": member_ids},
            ).first()
            if filings_row:
                total_lobbying_filings_on_committees = int(filings_row.filing_count or 0)
                lobbying_spend_on_committees = float(filings_row.total_amount or 0)

    support_positions = {"YEA", "AYE", "YES"}
    oppose_positions = {"NAY", "NO", "NOT VOTING"}
    support_count = 0
    oppose_count = 0
    for vote in recent_votes:
        position = str(vote.get("position") or "").upper()
        if position in support_positions:
            support_count += 1
        if position in oppose_positions:
            oppose_count += 1
    vote_alignment_score = support_count / max(support_count + oppose_count, 1)

    betrayal = betrayal_by_name.get(normalize_person_name(legislator.get("name") or ""))
    betrayal_score = float((betrayal or {}).get("betrayal_score") or 0)
    betrayal_issue = (betrayal or {}).get("issue_code") or "HLTH"

    title = "Sen." if legislator.get("chamber") == "senate" else "Rep."
    first_letter = (bioguide_id or "X")[0].upper()
    photo_url = f"https://bioguide.congress.gov/bioguide/photo/{first_letter}/{bioguide_id}.jpg"

    return {
        "bioguide_id": bioguide_id,
        "name": legislator.get("name"),
        "title": title,
        "party": legislator.get("party"),
        "state": normalize_state_code(legislator.get("state")),
        "chamber": legislator.get("chamber"),
        "photo_url": photo_url,
        "committees": summary.get("committees", []),
        "top_industries": top_industries,
        "total_contributions_received": float(summary.get("total_contributions_received") or 0),
        "total_lobbying_filings_on_committees": total_lobbying_filings_on_committees,
        "vote_alignment_score": round(vote_alignment_score, 2),
        "betrayal_score": round(betrayal_score, 2),
        "betrayal_issue": betrayal_issue,
        "recent_votes": recent_votes,
        "co_sponsorships_count": co_count,
        "co_sponsorships_by_issue": co_by_issue,
        "lobbying_spend_on_committees": lobbying_spend_on_committees,
    }


@app.get("/health")
def health():
    return {"ok": True}


@app.get("/search")
def search(q: str = Query(..., min_length=1), db: Session = Depends(get_db)):
    return search_entities(db, q=q)


@app.get("/meta/issue-codes")
def issue_codes(db: Session = Depends(get_db)):
    rows = db.execute(
        text(
            """
            SELECT DISTINCT code
            FROM lobbying_registrations,
            LATERAL unnest(general_issue_codes) AS code
            WHERE code IS NOT NULL AND code <> ''
            ORDER BY code
            LIMIT 200
            """
        )
    ).all()
    return {"issue_codes": [r.code for r in rows]}


@app.get("/graph/organization/{id}")
def graph_org(
    id: int,
    year_min: Optional[int] = None,
    year_max: Optional[int] = None,
    issue_code: Optional[str] = None,
    min_contribution: Optional[float] = None,
    node_limit: int = Query(default=50, ge=10, le=200),
    db: Session = Depends(get_db),
):
    return get_organization_graph(
        db,
        org_id=id,
        year_min=year_min,
        year_max=year_max,
        issue_code=issue_code,
        min_contribution=min_contribution,
        max_nodes=node_limit,
    )


@app.get("/graph/legislator/{bioguide_id}")
def graph_legislator(
    bioguide_id: str,
    year_min: Optional[int] = None,
    year_max: Optional[int] = None,
    min_contribution: Optional[float] = None,
    node_limit: int = Query(default=50, ge=10, le=200),
    db: Session = Depends(get_db),
):
    return get_legislator_graph(
        db,
        bioguide_id=bioguide_id,
        year_min=year_min,
        year_max=year_max,
        min_contribution=min_contribution,
        max_nodes=node_limit,
    )


@app.get("/graph/issue")
def graph_issue(
    q: str = Query(..., min_length=2),
    year_min: Optional[int] = None,
    year_max: Optional[int] = None,
    min_contribution: Optional[float] = None,
    node_limit: int = Query(default=50, ge=10, le=200),
    db: Session = Depends(get_db),
):
    return get_issue_graph(
        db,
        q=q,
        year_min=year_min,
        year_max=year_max,
        min_contribution=min_contribution,
        max_nodes=node_limit,
    )


@app.get("/entity/{entity_type}/{entity_id}/summary")
def entity_summary(entity_type: str, entity_id: str, db: Session = Depends(get_db)):
    return get_entity_summary(db, entity_type=entity_type, entity_id=entity_id)


@app.get("/representatives")
def representatives(
    zip: Optional[str] = Query(default=None, min_length=5, max_length=5),
    bioguide_id: Optional[str] = Query(default=None),
    db: Session = Depends(get_db),
):
    if not zip and not bioguide_id:
        raise HTTPException(status_code=400, detail="zip or bioguide_id is required")
    if zip and not re.fullmatch(r"\d{5}", zip):
        raise HTTPException(status_code=400, detail="zip must be 5 digits")

    candidates: list[dict] = []
    if bioguide_id:
        leg_row = db.execute(
            text(
                """
                SELECT id, bioguide_id, name, party, state, LOWER(chamber) AS chamber
                FROM legislators
                WHERE bioguide_id = :bioguide_id
                LIMIT 1
                """
            ),
            {"bioguide_id": bioguide_id},
        ).first()
        if not leg_row:
            return {"zip": zip, "representatives": [], "unmatched": [{"bioguide_id": bioguide_id}]}
        betrayal_by_name = build_betrayal_map(db)
        legislator = {
            "id": leg_row.id,
            "bioguide_id": leg_row.bioguide_id,
            "name": leg_row.name,
            "party": leg_row.party,
            "state": normalize_state_code(leg_row.state),
            "chamber": normalize_chamber(leg_row.chamber),
        }
        return {"zip": zip, "representatives": [build_representative_payload(db, legislator, betrayal_by_name)], "unmatched": []}
    else:
        direct_rows = direct_candidates_for_zip(db, zip)
        fallback_rows = []
        if direct_rows:
            fallback_rows = direct_rows
            candidates = [
                {
                    "name": row["name"],
                    "party": row["party"],
                    "state": normalize_state_code(row["state"]),
                    "chamber": normalize_chamber(row["chamber"]),
                }
                for row in fallback_rows
            ]
        else:
            candidates = fetch_congress_members_by_zip(zip)
            if len(candidates) > 5 or len(candidates) < 1:
                candidates = []
        if not candidates:
            candidates = fetch_google_civic_members_by_zip(zip)

        if not candidates:
            fallback_rows = fallback_candidates_for_zip(db, zip)
            if not fallback_rows:
                state_code = fetch_state_code_by_zip(zip)
                fallback_rows = fallback_candidates_for_state(db, state_code)
            if fallback_rows:
                candidates = [
                    {
                        "name": row["name"],
                        "party": row["party"],
                        "state": normalize_state_code(row["state"]),
                        "chamber": normalize_chamber(row["chamber"]),
                    }
                    for row in fallback_rows
                ]
        if not candidates:
            return {"zip": zip, "representatives": [], "unmatched": []}

    betrayal_by_name = build_betrayal_map(db)
    if fallback_rows:
        matched = [build_representative_payload(db, row, betrayal_by_name) for row in fallback_rows]
        unmatched = []
    else:
        matched_rows, unmatched = ranked_legislator_matches(db, candidates)
        matched = [build_representative_payload(db, legislator, betrayal_by_name) for legislator in matched_rows]

    chamber_order = {"senate": 0, "house": 1}
    matched.sort(key=lambda rep: (chamber_order.get(rep.get("chamber"), 2), rep.get("name") or ""))

    return {
        "zip": zip,
        "representatives": matched,
        "unmatched": unmatched,
    }


@app.get("/analysis/betrayal-index")
def betrayal_index(
    issue_code: str = Query(default="HLTH"),
    min_contribution: int = Query(default=10000),
    contribution_window_days: int = Query(default=365),
    db: Session = Depends(get_db),
):
    legislators = db.execute(
        text(
            """
            SELECT DISTINCT
              l.id AS legislator_id,
              l.name,
              l.party,
              l.state
            FROM legislators l
            JOIN co_sponsorships cs ON cs.legislator_id = l.id
            """
        )
    ).all()

    findings = []
    max_contrib = 0.0

    for leg in legislators:
        bills = db.execute(
            text(
                """
                SELECT bill_id, bill_title
                FROM co_sponsorships
                WHERE legislator_id = :legislator_id
                """
            ),
            {"legislator_id": leg.legislator_id},
        ).all()
        co_count = len(bills)
        if co_count == 0:
            continue

        contrib_rows = db.execute(
            text(
                """
                SELECT o.name, SUM(c.amount) AS amount
                FROM contributions c
                JOIN organizations o ON o.id = c.contributor_org_id
                JOIN lobbying_registrations lr
                  ON (lr.client_id = o.id OR lr.registrant_id = o.id)
                JOIN co_sponsorships cs ON cs.legislator_id = c.recipient_legislator_id
                WHERE c.recipient_legislator_id = :legislator_id
                  AND :issue_code = ANY(lr.general_issue_codes)
                  AND c.contribution_date >= cs.introduced_date
                  AND c.contribution_date <= cs.introduced_date + (INTERVAL '1 day' * :window_days)
                GROUP BY o.name
                ORDER BY amount DESC
                """
            ),
            {
                "legislator_id": leg.legislator_id,
                "issue_code": issue_code.upper(),
                "window_days": contribution_window_days,
            },
        ).all()
        total_contrib = float(sum(float(r.amount or 0) for r in contrib_rows))
        if total_contrib <= min_contribution:
            continue

        negative_votes = db.execute(
            text(
                """
                SELECT bill_id, vote_position AS position, vote_date::text AS date
                FROM votes
                WHERE legislator_id = :legislator_id
                  AND vote_position IN ('Nay', 'Not Voting')
                  AND bill_id IN (
                    SELECT bill_id
                    FROM co_sponsorships
                    WHERE legislator_id = :legislator_id
                  )
                ORDER BY vote_date DESC
                """
            ),
            {"legislator_id": leg.legislator_id},
        ).all()
        if not negative_votes:
            continue

        max_contrib = max(max_contrib, total_contrib)
        findings.append(
            {
                "legislator": {"name": leg.name, "party": leg.party, "state": leg.state},
                "co_sponsored_bills": [{"bill_id": b.bill_id, "title": b.bill_title} for b in bills[:10]],
                "contributions_after_cosponsor": total_contrib,
                "contributing_orgs": [{"name": r.name, "amount": float(r.amount or 0)} for r in contrib_rows[:10]],
                "negative_votes": [{"bill_id": v.bill_id, "position": v.position, "date": v.date} for v in negative_votes[:10]],
                "_co_count": co_count,
                "_neg_count": len(negative_votes),
            }
        )

    for f in findings:
        normalized = (f["contributions_after_cosponsor"] / max_contrib) if max_contrib > 0 else 0
        f["betrayal_score"] = normalized * (f["_neg_count"] / max(f["_co_count"], 1))
        del f["_co_count"]
        del f["_neg_count"]

    findings.sort(key=lambda x: x["betrayal_score"], reverse=True)
    return {"findings": findings}


@app.get("/analysis/revolving-door")
def revolving_door(
    agency: Optional[str] = None,
    issue_code: Optional[str] = None,
    limit: int = Query(default=50, ge=1, le=200),
    db: Session = Depends(get_db),
):
    issue_filter = issue_code.upper() if issue_code else None

    rows = db.execute(
        text(
            """
            SELECT
              l.id AS lobbyist_id,
              l.name AS lobbyist_name,
              l.lda_id AS lda_id,
              l.covered_positions AS covered_positions,
              lr.id AS registration_id,
              lr.general_issue_codes AS general_issue_codes,
              reg_org.name AS registrant_name,
              cli_org.name AS client_name
            FROM lobbyists l
            JOIN lobbying_lobbyists ll ON ll.lobbyist_id = l.id
            JOIN lobbying_registrations lr ON lr.id = ll.registration_id
            LEFT JOIN organizations reg_org ON reg_org.id = lr.registrant_id
            LEFT JOIN organizations cli_org ON cli_org.id = lr.client_id
            WHERE l.has_covered_position = TRUE
              AND (CAST(:issue_code AS text) IS NULL OR CAST(:issue_code AS text) = ANY(lr.general_issue_codes))
            """
        ),
        {"issue_code": issue_filter},
    ).all()

    grouped = {}
    for row in rows:
        positions = row.covered_positions or []
        if agency and not any(agency.lower() in str(position or "").lower() for position in positions):
            continue

        item = grouped.setdefault(
            row.lobbyist_id,
            {
                "lobbyist": {"name": row.lobbyist_name, "lda_id": row.lda_id},
                "prior_positions": positions,
                "registrant_counts": {},
                "clients": set(),
                "issue_codes": set(),
                "registration_ids": set(),
            },
        )
        item["registration_ids"].add(row.registration_id)
        if row.registrant_name:
            item["registrant_counts"][row.registrant_name] = item["registrant_counts"].get(row.registrant_name, 0) + 1
        if row.client_name:
            item["clients"].add(row.client_name)
        for code in row.general_issue_codes or []:
            if code:
                item["issue_codes"].add(code)

    findings = []
    max_raw_score = 0.0
    for item in grouped.values():
        filing_count = len(item["registration_ids"])
        issue_codes = set(item["issue_codes"])
        issue_relevance = compute_issue_relevance(agency, issue_codes, issue_filter)
        raw_score = float(filing_count) * float(issue_relevance)
        max_raw_score = max(max_raw_score, raw_score)

        current_registrant = None
        if item["registrant_counts"]:
            current_registrant = max(item["registrant_counts"], key=item["registrant_counts"].get)

        findings.append(
            {
                "lobbyist": item["lobbyist"],
                "prior_positions": item["prior_positions"],
                "current_registrant": current_registrant,
                "clients": sorted(item["clients"])[:8],
                "issue_codes": sorted(issue_codes)[:10],
                "filing_count": filing_count,
                "_raw_score": raw_score,
            }
        )

    findings.sort(key=lambda x: x["_raw_score"], reverse=True)
    normalized = []
    divisor = max_raw_score or 1.0
    for row in findings[:limit]:
        score = row["_raw_score"] / divisor
        normalized.append(
            {
                "lobbyist": row["lobbyist"],
                "prior_positions": row["prior_positions"],
                "current_registrant": row["current_registrant"],
                "clients": row["clients"],
                "issue_codes": row["issue_codes"],
                "filing_count": row["filing_count"],
                "revolving_door_score": round(score, 2),
            }
        )
    return {"findings": normalized}


@app.get("/analysis/foreign-influence")
def foreign_influence(
    country: Optional[str] = None,
    issue_code: Optional[str] = None,
    limit: int = Query(default=50, ge=1, le=200),
    db: Session = Depends(get_db),
):
    issue_filter = issue_code.upper() if issue_code else None
    country_filter = country.upper() if country else None

    rows = db.execute(
        text(
            """
            SELECT
              lr.id AS registration_id,
              lr.client_id AS client_id,
              cli_org.name AS client_name,
              lr.foreign_entity_names AS foreign_entity_names,
              lr.foreign_entity_countries AS foreign_entity_countries,
              lr.general_issue_codes AS general_issue_codes
            FROM lobbying_registrations lr
            LEFT JOIN organizations cli_org ON cli_org.id = lr.client_id
            WHERE lr.has_foreign_entity = TRUE
              AND (
                CAST(:country AS text) IS NULL
                OR EXISTS (
                    SELECT 1
                    FROM unnest(COALESCE(lr.foreign_entity_countries, ARRAY[]::text[])) c
                    WHERE UPPER(c) = CAST(:country AS text)
                )
              )
              AND (CAST(:issue_code AS text) IS NULL OR CAST(:issue_code AS text) = ANY(lr.general_issue_codes))
            """
        ),
        {"country": country_filter, "issue_code": issue_filter},
    ).all()

    grouped = {}
    for row in rows:
        client_key = row.client_id or f"client:{row.client_name or 'unknown'}"
        item = grouped.setdefault(
            client_key,
            {
                "organization": {"id": row.client_id, "name": row.client_name or "Unknown client"},
                "registration_ids": set(),
                "foreign_entities": set(),
                "foreign_countries": set(),
                "issue_codes": set(),
            },
        )
        item["registration_ids"].add(row.registration_id)
        for name in row.foreign_entity_names or []:
            if name:
                item["foreign_entities"].add(name)
        for code in row.foreign_entity_countries or []:
            if code:
                item["foreign_countries"].add(code)
        for issue in row.general_issue_codes or []:
            if issue:
                item["issue_codes"].add(issue)

    findings = []
    for item in grouped.values():
        committees_targeted = []
        org_id = item["organization"]["id"]
        if org_id:
            committee_rows = db.execute(
                text(
                    """
                    SELECT DISTINCT c.name
                    FROM contributions contrib
                    JOIN committee_memberships cm ON cm.legislator_id = contrib.recipient_legislator_id
                    JOIN committees c ON c.id = cm.committee_id
                    WHERE contrib.contributor_org_id = :org_id
                    ORDER BY c.name
                    LIMIT 8
                    """
                ),
                {"org_id": org_id},
            ).all()
            committees_targeted = [r.name for r in committee_rows if r.name]

        findings.append(
            {
                "organization": item["organization"],
                "foreign_entities": sorted(item["foreign_entities"])[:20],
                "foreign_countries": sorted(item["foreign_countries"])[:20],
                "issue_codes": sorted(item["issue_codes"])[:12],
                "committees_targeted": committees_targeted,
                "filing_count": len(item["registration_ids"]),
            }
        )

    findings.sort(key=lambda x: x["filing_count"], reverse=True)
    return {"findings": findings[:limit]}


# Serve React frontend — must come after all API routes
_FRONTEND_DIST = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "frontend", "dist"))
_FRONTEND_ASSETS = os.path.join(_FRONTEND_DIST, "assets")

if os.path.isdir(_FRONTEND_ASSETS):
    app.mount("/assets", StaticFiles(directory=_FRONTEND_ASSETS), name="assets")

@app.get("/")
def serve_root():
    return FileResponse(os.path.join(_FRONTEND_DIST, "index.html"))

@app.get("/{full_path:path}")
def serve_frontend(full_path: str):
    file_path = os.path.join(_FRONTEND_DIST, full_path)
    if os.path.isfile(file_path):
        return FileResponse(file_path)
    return FileResponse(os.path.join(_FRONTEND_DIST, "index.html"))
