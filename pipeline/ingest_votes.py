"""
Ingest roll-call vote records using:
  - senate.gov vote XML for Senate votes
  - House Clerk XML (via GovTrack vote list for roll numbers) for House votes

Senate URL pattern:
  vote menu:       https://www.senate.gov/legislative/LIS/roll_call_lists/vote_menu_{congress}_{session}.xml
  individual vote: https://www.senate.gov/legislative/LIS/roll_call_votes/vote{congress}{session}/vote_{congress}_{session}_{number}.xml

House URL pattern (via GovTrack list for roll numbers, then Clerk XML):
  individual vote: https://clerk.house.gov/evs/{year}/roll{number}.xml
"""

import argparse
import asyncio
import logging
import os
import re
import time
from datetime import datetime
from xml.etree import ElementTree as ET

import aiohttp
from dotenv import load_dotenv
from tqdm import tqdm

from db import connect_async, drop_indexes_for_bulk_load, optimize_for_bulk_load, rebuild_indexes

load_dotenv()

SEM = asyncio.Semaphore(6)
FLUSH_SIZE = 300

SUBJECT_TO_ISSUE = {
    "health": "HLTH",
    "drug": "HLTH",
    "pharmaceutical": "HLTH",
    "medicare": "HLTH",
    "medicaid": "HLTH",
    "tax": "TAX",
    "finance": "FIN",
    "bank": "FIN",
    "energy": "ENRG",
    "oil": "ENRG",
    "gas": "ENRG",
    "defense": "DEF",
    "military": "DEF",
    "trade": "TRAD",
    "environment": "ENV",
    "technology": "TECH",
    "crypto": "TECH",
}

# congress → sessions list
CONGRESS_SESSIONS = {118: [1, 2], 119: [1]}
# (congress, session) → list of calendar years that session spans
CONGRESS_SESSION_YEARS = {(118, 1): [2023], (118, 2): [2024], (119, 1): [2025, 2026]}

BILL_NUM_RE = re.compile(r"\b(\d+)\b")

STATE_ABBREV = {
    "alabama": "AL", "alaska": "AK", "arizona": "AZ", "arkansas": "AR",
    "california": "CA", "colorado": "CO", "connecticut": "CT", "delaware": "DE",
    "florida": "FL", "georgia": "GA", "hawaii": "HI", "idaho": "ID",
    "illinois": "IL", "indiana": "IN", "iowa": "IA", "kansas": "KS",
    "kentucky": "KY", "louisiana": "LA", "maine": "ME", "maryland": "MD",
    "massachusetts": "MA", "michigan": "MI", "minnesota": "MN", "mississippi": "MS",
    "missouri": "MO", "montana": "MT", "nebraska": "NE", "nevada": "NV",
    "new hampshire": "NH", "new jersey": "NJ", "new mexico": "NM", "new york": "NY",
    "north carolina": "NC", "north dakota": "ND", "ohio": "OH", "oklahoma": "OK",
    "oregon": "OR", "pennsylvania": "PA", "rhode island": "RI", "south carolina": "SC",
    "south dakota": "SD", "tennessee": "TN", "texas": "TX", "utah": "UT",
    "vermont": "VT", "virginia": "VA", "washington": "WA", "west virginia": "WV",
    "wisconsin": "WI", "wyoming": "WY", "district of columbia": "DC",
}
GOVTRACK_BASE = "https://www.govtrack.us/api/v2"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler("pipeline_errors.log")],
)


def classify_issue(text: str) -> list[str]:
    if not text:
        return []
    lower = text.lower()
    seen: set[str] = set()
    out: list[str] = []
    for kw, code in SUBJECT_TO_ISSUE.items():
        if kw in lower and code not in seen:
            seen.add(code)
            out.append(code)
    return out


def extract_bill_number(text: str) -> str | None:
    """Extract the first numeric portion from a bill reference like 'H.R. 1234' → '1234'."""
    m = BILL_NUM_RE.search(text or "")
    return m.group(1) if m else None


async def fetch_text(session: aiohttp.ClientSession, url: str) -> str | None:
    await asyncio.sleep(0.1)
    delay = 1.0
    for attempt in range(3):
        try:
            async with SEM:
                async with session.get(url, timeout=30) as resp:
                    if resp.status == 404:
                        return None
                    if resp.status >= 400:
                        return None
                    return await resp.text(encoding="utf-8", errors="replace")
        except Exception:
            if attempt == 2:
                logging.warning("Failed fetching %s", url)
                return None
            await asyncio.sleep(delay)
            delay *= 2
    return None


async def fetch_json(session: aiohttp.ClientSession, url: str) -> dict | None:
    await asyncio.sleep(0.1)
    delay = 1.0
    for attempt in range(3):
        try:
            async with SEM:
                async with session.get(url, timeout=30) as resp:
                    if resp.status >= 400:
                        return None
                    return await resp.json(content_type=None)
        except Exception:
            if attempt == 2:
                logging.warning("Failed JSON %s", url)
                return None
            await asyncio.sleep(delay)
            delay *= 2
    return None


async def flush_votes(conn, buf: list[tuple]) -> None:
    if not buf:
        return
    async with conn.cursor() as cur:
        await cur.executemany(
            """
            INSERT INTO votes (legislator_id, bill_id, bill_title, vote_position, vote_date, congress, issue_tags)
            VALUES (
                (SELECT id FROM legislators WHERE bioguide_id = %s LIMIT 1),
                %s, %s, %s, %s, %s, %s
            )
            ON CONFLICT DO NOTHING
            """,
            buf,
        )
    buf.clear()
    await conn.commit()


# ---------------------------------------------------------------------------
# Senate ingestion (senate.gov XML)
# ---------------------------------------------------------------------------

async def ingest_senate(http: aiohttp.ClientSession, conn, congress: int, session_num: int, pbar: tqdm) -> int:
    """
    Build a name+state → bioguide_id lookup for senators, then page through the
    senate.gov vote menu to find bill votes, then fetch individual vote XMLs.
    """
    # Build LIS member id -> bioguide map from official Senate member feed.
    # This avoids incorrect last-name/state collisions with House members.
    lis_map: dict[str, str] = {}
    cvc_xml = await fetch_text(http, "https://www.senate.gov/legislative/LIS_MEMBER/cvc_member_data.xml")
    if cvc_xml:
        try:
            croot = ET.fromstring(cvc_xml)
            for sen in croot.findall(".//senator"):
                lis_id = (sen.get("lis_member_id") or "").strip().upper()
                bio = (sen.findtext("bioguideId") or "").strip().upper()
                if lis_id and bio:
                    lis_map[lis_id] = bio
        except ET.ParseError:
            logging.warning("Unable to parse Senate cvc_member_data.xml; falling back to name/state mapping")

    # Fallback map for cases where lis_member_id is unexpectedly missing.
    async with conn.cursor() as cur:
        await cur.execute("SELECT bioguide_id, name, state FROM legislators WHERE chamber ILIKE 'senate%' AND is_active = TRUE")
        rows = await cur.fetchall()
    fallback_map: dict[tuple[str, str], str] = {}
    for bioguide, name, state in rows:
        if "," in (name or ""):
            last = name.split(",")[0].strip().upper()
        else:
            parts = (name or "").strip().split()
            last = parts[-1].upper() if parts else ""
        if last and state:
            abbrev = STATE_ABBREV.get((state or "").lower(), (state or "").upper()[:2])
            fallback_map[(last, abbrev)] = bioguide

    menu_url = (
        f"https://www.senate.gov/legislative/LIS/roll_call_lists/"
        f"vote_menu_{congress}_{session_num}.xml"
    )
    menu_text = await fetch_text(http, menu_url)
    if not menu_text:
        logging.warning("Senate vote menu not found: %s", menu_url)
        return 0

    try:
        menu_root = ET.fromstring(menu_text)
    except ET.ParseError:
        logging.warning("Senate vote menu parse error: %s", menu_url)
        return 0

    inserted = 0
    buf: list[tuple] = []

    for vote_el in menu_root.findall(".//vote"):
        vote_number = (vote_el.findtext("vote_number") or "").strip()
        issue = vote_el.findtext("issue") or ""
        title = vote_el.findtext("title") or ""
        question = vote_el.findtext("question") or ""

        bill_number = extract_bill_number(issue) or extract_bill_number(title)
        if not bill_number:
            continue

        vote_url = (
            f"https://www.senate.gov/legislative/LIS/roll_call_votes/"
            f"vote{congress}{session_num}/"
            f"vote_{congress}_{session_num}_{vote_number}.xml"
        )
        vote_text = await fetch_text(http, vote_url)
        if not vote_text:
            continue

        try:
            vroot = ET.fromstring(vote_text)
        except ET.ParseError:
            continue

        vote_date_raw = (vroot.findtext("vote_date") or "").strip()
        vote_date = None
        for fmt in ("%B %d, %Y", "%d-%b-%Y", "%Y-%m-%d"):
            try:
                # Senate format: "December 13, 2023,  06:46 PM" — take first two comma-parts
                date_str = ", ".join(vote_date_raw.split(",")[:2]).strip()
                vote_date = datetime.strptime(date_str, fmt).date()
                break
            except ValueError:
                pass
        description = (vroot.findtext("vote_document_text") or title or "")[:500]
        issue_tags = classify_issue(description + " " + question) or None

        for member_el in vroot.findall(".//member"):
            last_name = (member_el.findtext("last_name") or "").strip().upper()
            state = (member_el.findtext("state") or "").strip().upper()
            vote_cast = (member_el.findtext("vote_cast") or "").strip()
            lis_member_id = (member_el.findtext("lis_member_id") or "").strip().upper()
            if not last_name or not vote_cast:
                continue

            bioguide_id = lis_map.get(lis_member_id) if lis_member_id else None
            if not bioguide_id:
                bioguide_id = fallback_map.get((last_name, state))
            if not bioguide_id:
                continue

            buf.append((bioguide_id, bill_number, description or None, vote_cast, vote_date, congress, issue_tags))
            inserted += 1
            pbar.update(1)

        if len(buf) >= FLUSH_SIZE:
            await flush_votes(conn, buf)

    await flush_votes(conn, buf)
    return inserted


# ---------------------------------------------------------------------------
# House ingestion (GovTrack list → House Clerk XML)
# ---------------------------------------------------------------------------

async def ingest_house(http: aiohttp.ClientSession, conn, congress: int, years: list[int], pbar: tqdm) -> int:
    inserted = 0
    buf: list[tuple] = []
    offset = 0
    limit = 300

    while True:
        url = f"{GOVTRACK_BASE}/vote?congress={congress}&chamber=house&limit={limit}&offset={offset}"
        payload = await fetch_json(http, url)
        if not payload:
            break

        objects = payload.get("objects", [])
        if not objects:
            break

        for vote in objects:
            link = vote.get("link", "")
            question = vote.get("question") or ""
            # link format: https://www.govtrack.us/congress/votes/118-2023/h201
            m = re.search(r"/\d+-(\d{4})/h(\d+)$", link)
            if not m:
                continue
            year_str, roll_str = m.group(1), m.group(2)
            year = int(year_str)
            if year not in years:
                continue

            roll_padded = roll_str.zfill(3)
            clerk_url = f"https://clerk.house.gov/evs/{year}/roll{roll_padded}.xml"
            vote_text = await fetch_text(http, clerk_url)
            if not vote_text:
                clerk_url2 = f"https://clerk.house.gov/evs/{year}/roll{roll_str}.xml"
                vote_text = await fetch_text(http, clerk_url2)
            if not vote_text:
                continue

            try:
                vroot = ET.fromstring(vote_text)
            except ET.ParseError:
                continue

            meta = vroot.find("vote-metadata")
            action_date = (meta.findtext("action-date") if meta is not None else None) or ""
            legis_num = (meta.findtext("legis-num") if meta is not None else None) or ""
            vote_desc = (meta.findtext("vote-desc") if meta is not None else None) or question

            # Extract bill number from legis-num (e.g. "H R 21" → "21"); skip non-bill votes
            bill_number = extract_bill_number(legis_num)
            if not bill_number:
                continue

            issue_tags = classify_issue(vote_desc + " " + question + " " + legis_num) or None

            for rv in vroot.findall(".//recorded-vote"):
                leg_el = rv.find("legislator")
                if leg_el is None:
                    continue
                bioguide_id = (leg_el.get("name-id") or "").strip()
                vote_cast_el = rv.find("vote")
                vote_cast = (vote_cast_el.text or "").strip() if vote_cast_el is not None else ""
                if not bioguide_id or not vote_cast:
                    continue

                buf.append((bioguide_id, bill_number, vote_desc[:500] or None, vote_cast, action_date or None, congress, issue_tags))
                inserted += 1
                pbar.update(1)

            if len(buf) >= FLUSH_SIZE:
                await flush_votes(conn, buf)

        total = payload.get("meta", {}).get("total_count", 0)
        offset += limit
        if offset >= total:
            break

    await flush_votes(conn, buf)
    return inserted


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def main_async(congresses: list[int]) -> None:
    conn = await connect_async()
    await optimize_for_bulk_load(conn)
    await drop_indexes_for_bulk_load(conn)

    total = 0
    pbar = tqdm(desc="Votes", unit="member-votes")
    start = time.time()

    async with aiohttp.ClientSession() as http:
        for congress in sorted(congresses):
            sessions = CONGRESS_SESSIONS.get(congress, [1])

            # Senate
            for session_num in sessions:
                logging.info("Senate congress=%s session=%s", congress, session_num)
                n = await ingest_senate(http, conn, congress, session_num, pbar)
                total += n
                logging.info("Senate congress=%s session=%s inserted=%s", congress, session_num, n)

            # House
            years: list[int] = []
            for s in sessions:
                years.extend(CONGRESS_SESSION_YEARS.get((congress, s), []))
            logging.info("House congress=%s years=%s", congress, years)
            n = await ingest_house(http, conn, congress, years, pbar)
            total += n
            logging.info("House congress=%s inserted=%s", congress, n)

    pbar.set_postfix(elapsed=f"{int(time.time()-start)}s", total=total)
    pbar.close()

    await rebuild_indexes(conn)
    await conn.commit()
    await conn.close()
    logging.info("Vote ingestion complete: total_member_votes=%s", total)


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest roll-call votes (senate.gov + House Clerk XML)")
    parser.add_argument("--congresses", nargs="+", type=int, default=[118, 119])
    args = parser.parse_args()
    asyncio.run(main_async(args.congresses))


if __name__ == "__main__":
    main()
