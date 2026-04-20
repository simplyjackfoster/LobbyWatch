import argparse
import asyncio
import logging
import os
import time
from typing import Any

import aiohttp
from dotenv import load_dotenv
from tqdm import tqdm

from db import connect_async, drop_indexes_for_bulk_load, optimize_for_bulk_load, rebuild_indexes

load_dotenv()

API_BASE = "https://api.congress.gov/v3"
API_KEY = os.getenv("CONGRESS_API_KEY")
SEM = asyncio.Semaphore(10)
FLUSH_SIZE = 500

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler("pipeline_errors.log")],
)


def parent_code(value: Any) -> str | None:
    if isinstance(value, dict):
        return value.get("systemCode") or value.get("code") or value.get("name")
    return value


async def fetch_json(
    session: aiohttp.ClientSession,
    path: str,
    params: dict[str, Any] | None = None,
    allow_404: bool = False,
):
    delay = 0.2
    for attempt in range(3):
        try:
            async with SEM:
                query = {"api_key": API_KEY, "format": "json"}
                if params:
                    query.update(params)
                async with session.get(f"{API_BASE}{path}", params=query, timeout=60) as resp:
                    if allow_404 and resp.status == 404:
                        return {"_not_found": True}
                    if resp.status >= 400:
                        raise RuntimeError(f"HTTP {resp.status}: {await resp.text()}")
                    return await resp.json()
        except Exception:
            if attempt == 2:
                logging.exception("Congress request failed path=%s params=%s", path, params)
                return None
            await asyncio.sleep(delay)
            delay *= 2


async def fetch_member_details(session: aiohttp.ClientSession, bioguide: str):
    committees_task = fetch_json(session, "/committee-membership", {"bioguideId": bioguide, "limit": 250}, allow_404=True)
    cosponsored_118_task = fetch_json(session, f"/member/{bioguide}/cosponsored-legislation", {"congress": 118, "limit": 250})
    cosponsored_119_task = fetch_json(session, f"/member/{bioguide}/cosponsored-legislation", {"congress": 119, "limit": 250})

    committees, cos_118, cos_119 = await asyncio.gather(
        committees_task,
        cosponsored_118_task,
        cosponsored_119_task,
    )
    return committees or {}, cos_118 or {}, cos_119 or {}


async def ingest_committee_catalog(session: aiohttp.ClientSession, conn):
    committee_buf = []
    offset = 0
    limit = 250
    while True:
        payload = await fetch_json(session, "/committee", {"limit": limit, "offset": offset})
        if not payload:
            break
        committees = payload.get("committees", [])
        if not committees:
            break
        for c in committees:
            code = c.get("systemCode")
            committee_buf.append(
                (
                    code,
                    c.get("name"),
                    (c.get("chamber") or "").lower(),
                    parent_code(c.get("parent") or c.get("subcommitteeOf")),
                )
            )
        if len(committee_buf) >= FLUSH_SIZE:
            async with conn.cursor() as cur:
                await cur.executemany(
                    """
                    INSERT INTO committees (committee_id, name, chamber, subcommittee_of)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (committee_id) DO NOTHING
                    """,
                    list({row for row in committee_buf if row[0]}),
                )
            committee_buf.clear()
            await conn.commit()
        pagination = payload.get("pagination", {}) or {}
        count = pagination.get("count") or 0
        offset += limit
        if offset >= count:
            break
    if committee_buf:
        async with conn.cursor() as cur:
            await cur.executemany(
                """
                INSERT INTO committees (committee_id, name, chamber, subcommittee_of)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (committee_id) DO NOTHING
                """,
                list({row for row in committee_buf if row[0]}),
            )
        await conn.commit()


async def flush_buffers(conn, leg_buf, committee_buf, membership_buf, vote_buf, cosponsor_buf):
    if leg_buf:
        async with conn.cursor() as cur:
            await cur.executemany(
                """
                INSERT INTO legislators (bioguide_id, name, party, state, chamber, is_active)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (bioguide_id) DO NOTHING
                """,
                leg_buf,
            )
        leg_buf.clear()

    if committee_buf:
        async with conn.cursor() as cur:
            await cur.executemany(
                """
                INSERT INTO committees (committee_id, name, chamber, subcommittee_of)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (committee_id) DO NOTHING
                """,
                list({row for row in committee_buf if row[0]}),
            )
        committee_buf.clear()

    if membership_buf:
        async with conn.cursor() as cur:
            await cur.executemany(
                """
                INSERT INTO committee_memberships (legislator_id, committee_id, role)
                VALUES (
                    (SELECT id FROM legislators WHERE bioguide_id = %s LIMIT 1),
                    (SELECT id FROM committees WHERE committee_id = %s LIMIT 1),
                    %s
                )
                ON CONFLICT (legislator_id, committee_id) DO NOTHING
                """,
                membership_buf,
            )
        membership_buf.clear()

    if vote_buf:
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
                vote_buf,
            )
        vote_buf.clear()

    if cosponsor_buf:
        async with conn.cursor() as cur:
            await cur.executemany(
                """
                INSERT INTO co_sponsorships (legislator_id, bill_id, bill_title, congress, introduced_date)
                VALUES (
                    (SELECT id FROM legislators WHERE bioguide_id = %s LIMIT 1),
                    %s, %s, %s, %s
                )
                ON CONFLICT (legislator_id, bill_id) DO NOTHING
                """,
                cosponsor_buf,
            )
        cosponsor_buf.clear()

    await conn.commit()


async def main_async(limit: int):
    if not API_KEY:
        raise RuntimeError("CONGRESS_API_KEY is required")

    conn = await connect_async()
    async with conn.cursor() as cur:
        await cur.execute(
            """
            CREATE TABLE IF NOT EXISTS co_sponsorships (
              id SERIAL PRIMARY KEY,
              legislator_id INTEGER REFERENCES legislators(id),
              bill_id TEXT NOT NULL,
              bill_title TEXT,
              congress INTEGER,
              introduced_date DATE,
              UNIQUE(legislator_id, bill_id)
            )
            """
        )
    await optimize_for_bulk_load(conn)
    await drop_indexes_for_bulk_load(conn)

    leg_buf = []
    committee_buf = []
    membership_buf = []
    vote_buf = []
    cosponsor_buf = []

    async with aiohttp.ClientSession() as session:
        await ingest_committee_catalog(session, conn)
        page = 1
        offset = 0
        processed = 0
        pbar = tqdm(desc="Congress", unit="members")
        start = time.time()

        while True:
            payload = await fetch_json(session, "/member", {"currentMember": "true", "limit": limit, "offset": offset})
            if not payload:
                break
            members = payload.get("members", [])
            if not members:
                break

            detail_tasks = []
            for m in members:
                bioguide = m.get("bioguideId")
                if not bioguide:
                    continue
                terms = m.get("terms", {}).get("item", [])
                latest_term = terms[0] if terms else {}
                leg_buf.append(
                    (
                        bioguide,
                        m.get("name"),
                        latest_term.get("party") or m.get("partyName"),
                        latest_term.get("stateCode") or m.get("state"),
                        (latest_term.get("chamber") or "").lower(),
                        True,
                    )
                )
                detail_tasks.append((bioguide, asyncio.create_task(fetch_member_details(session, bioguide))))

            await flush_buffers(conn, leg_buf, committee_buf, membership_buf, vote_buf, cosponsor_buf)

            for bioguide, task in detail_tasks:
                try:
                    committees, cos_118, cos_119 = await task

                    if committees.get("_not_found"):
                        # Endpoint may be unavailable for this API key tier/version.
                        # Log and continue with members + committees + co-sponsorships.
                        pass
                    else:
                        for c in (
                            committees.get("committeeMemberships", [])
                            or committees.get("memberships", [])
                            or committees.get("committees", [])
                            or committees.get("memberCommittees", [])
                        ):
                            code = c.get("systemCode") or c.get("code") or c.get("committeeCode")
                            committee_buf.append(
                                (
                                    code,
                                    c.get("name") or c.get("title"),
                                    (c.get("chamber") or "").lower(),
                                    parent_code(c.get("parent") or c.get("subcommitteeOf")),
                                )
                            )
                            membership_buf.append((bioguide, code, c.get("memberType") or c.get("role")))

                    for congress, payload_cos in [(118, cos_118), (119, cos_119)]:
                        for b in (payload_cos.get("bills", []) or payload_cos.get("cosponsoredLegislation", [])):
                            bill_id = b.get("number") or b.get("billNumber") or b.get("url")
                            cosponsor_buf.append((bioguide, bill_id, b.get("title"), congress, b.get("introducedDate")))

                    processed += 1
                    pbar.update(1)
                    if processed % FLUSH_SIZE == 0:
                        await flush_buffers(conn, leg_buf, committee_buf, membership_buf, vote_buf, cosponsor_buf)
                except Exception:
                    logging.exception("Failed congress member details bioguide=%s", bioguide)

            await flush_buffers(conn, leg_buf, committee_buf, membership_buf, vote_buf, cosponsor_buf)
            pbar.set_postfix(page=page, elapsed=f"{int(time.time() - start)}s")
            page += 1
            offset += limit

        pbar.close()

    await rebuild_indexes(conn)
    await conn.commit()
    await conn.close()


def main():
    parser = argparse.ArgumentParser(description="Async ingest Congress members, committees, votes, co-sponsorships")
    parser.add_argument("--limit", type=int, default=100)
    args = parser.parse_args()
    asyncio.run(main_async(args.limit))


if __name__ == "__main__":
    main()
