import argparse
import asyncio
import logging
import os
import time
from typing import Any

import aiohttp
from dotenv import load_dotenv
from tqdm import tqdm

from db import (
    apply_enhancement_migrations,
    connect_async,
    drop_indexes_for_bulk_load,
    normalize_name,
    optimize_for_bulk_load,
    rebuild_indexes,
)

load_dotenv()

API_BASE = "https://lda.gov/api/v1"
API_KEY = os.getenv("LDA_API_KEY")
# LDA docs allow 120 req/min with an API key.
SEM = asyncio.Semaphore(10)
FLUSH_SIZE = 500

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler("pipeline_errors.log")],
)


async def fetch_json(session: aiohttp.ClientSession, url: str, params: dict[str, Any] | None = None):
    delay = 0.05
    for attempt in range(3):
        try:
            async with SEM:
                await asyncio.sleep(0.05)
                async with session.get(url, params=params, timeout=60) as resp:
                    if resp.status == 429:
                        retry_after = int(resp.headers.get("Retry-After", 60))
                        logging.warning("Rate limited. Waiting %ss", retry_after)
                        await asyncio.sleep(retry_after)
                        await asyncio.sleep(0.05)
                        async with session.get(url, params=params, timeout=60) as retry_resp:
                            if retry_resp.status >= 400:
                                raise RuntimeError(f"HTTP {retry_resp.status}: {await retry_resp.text()}")
                            return await retry_resp.json()
                    if resp.status >= 400:
                        raise RuntimeError(f"HTTP {resp.status}: {await resp.text()}")
                    return await resp.json()
        except Exception as exc:
            if attempt == 2:
                logging.exception("LDA request failed url=%s params=%s", url, params)
                return None
            await asyncio.sleep(delay)
            delay *= 2


def filing_to_records(filing: dict[str, Any]):
    filing_uuid = filing.get("filing_uuid")
    if not filing_uuid:
        return None

    registrant = (filing.get("registrant") or {}).get("name")
    client = (filing.get("client") or {}).get("name")
    filing_year = filing.get("filing_year")
    filing_period = filing.get("filing_period")
    amount = filing.get("income") or filing.get("expenses") or filing.get("amount") or 0

    activities = filing.get("lobbying_activities") or []
    general_issue_codes = list({a.get("general_issue_code") for a in activities if a.get("general_issue_code")})
    specific_issues = " | ".join(
        a.get("specific_issues", "")
        for a in activities
        if a.get("specific_issues")
    )
    foreign_entities = filing.get("foreign_entities") or []
    has_foreign_entity = len(foreign_entities) > 0
    foreign_entity_names = [fe.get("name") for fe in foreign_entities if fe.get("name")]
    foreign_entity_countries = list({fe.get("country") for fe in foreign_entities if fe.get("country")})

    org_rows = []
    if registrant:
        org_rows.append((registrant, normalize_name(registrant), "registrant"))
    if client:
        org_rows.append((client, normalize_name(client), "client"))

    reg_row = (
        filing_uuid,
        registrant,
        normalize_name(registrant) if registrant else None,
        client,
        normalize_name(client) if client else None,
        filing_year,
        filing_period,
        amount,
        general_issue_codes,
        specific_issues,
        has_foreign_entity,
        foreign_entity_names,
        foreign_entity_countries,
    )

    lobbyist_rows: dict[tuple[str | None, str], dict[str, Any]] = {}
    link_rows = []

    def upsert_lobbyist_row(raw: dict[str, Any], covered_positions: list[str] | None = None, conviction: str | None = None):
        lobbyist_name = raw.get("lobbyist") or raw.get("name")
        if isinstance(lobbyist_name, dict):
            lobbyist_name = lobbyist_name.get("name")
        if not lobbyist_name:
            return

        normalized_name = normalize_name(lobbyist_name)
        lda_id = str(raw.get("id") or raw.get("lobbyist_id") or "") or None
        key = (lda_id, normalized_name)
        record = lobbyist_rows.get(
            key,
            {
                "name": lobbyist_name,
                "name_normalized": normalized_name,
                "lda_id": lda_id,
                "covered_positions": [],
                "has_covered_position": False,
                "conviction_disclosure": None,
                "has_conviction": False,
            },
        )

        if covered_positions:
            deduped_positions = list(dict.fromkeys([*record["covered_positions"], *covered_positions]))
            record["covered_positions"] = deduped_positions
            record["has_covered_position"] = len(deduped_positions) > 0

        if conviction:
            record["conviction_disclosure"] = conviction
            record["has_conviction"] = True

        lobbyist_rows[key] = record
        link_rows.append((filing_uuid, lda_id, normalized_name))

    for l in filing.get("lobbyists", []) or []:
        upsert_lobbyist_row(l)

    for activity in activities:
        for lob in activity.get("lobbyists") or []:
            covered = lob.get("covered_positions") or []
            covered_positions = [p.get("position_held") for p in covered if p.get("position_held")]
            conviction = lob.get("lobbyist_conviction_disclosure")
            upsert_lobbyist_row(lob, covered_positions=covered_positions, conviction=conviction)

    lobbyist_row_values = [
        (
            row["name"],
            row["name_normalized"],
            row["lda_id"],
            row["has_covered_position"],
            row["covered_positions"] or [],
            row["has_conviction"],
            row["conviction_disclosure"],
        )
        for row in lobbyist_rows.values()
    ]

    return org_rows, reg_row, lobbyist_row_values, link_rows


async def flush_buffers(conn, org_buf, reg_buf, lobbyist_buf, link_buf):
    if org_buf:
        async with conn.cursor() as cur:
            await cur.executemany(
                """
                INSERT INTO organizations (name, name_normalized, type)
                VALUES (%s, %s, %s)
                ON CONFLICT (name_normalized) DO NOTHING
                """,
                list({row for row in org_buf}),
            )
        org_buf.clear()

    if lobbyist_buf:
        async with conn.cursor() as cur:
            await cur.executemany(
                """
                INSERT INTO lobbyists (
                    name,
                    name_normalized,
                    lda_id,
                    has_covered_position,
                    covered_positions,
                    has_conviction,
                    conviction_disclosure
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (lda_id) DO UPDATE SET
                    has_covered_position = EXCLUDED.has_covered_position,
                    covered_positions = EXCLUDED.covered_positions,
                    has_conviction = EXCLUDED.has_conviction,
                    conviction_disclosure = EXCLUDED.conviction_disclosure
                """,
                list({row for row in lobbyist_buf if row[2]}),
            )
        lobbyist_buf.clear()

    if reg_buf:
        async with conn.cursor() as cur:
            await cur.executemany(
                """
                INSERT INTO lobbying_registrations (
                    filing_uuid,
                    registrant_id,
                    client_id,
                    filing_year,
                    filing_period,
                    amount,
                    general_issue_codes,
                    specific_issues,
                    has_foreign_entity,
                    foreign_entity_names,
                    foreign_entity_countries
                )
                VALUES (
                    %s,
                    (SELECT id FROM organizations WHERE name_normalized = %s LIMIT 1),
                    (SELECT id FROM organizations WHERE name_normalized = %s LIMIT 1),
                    %s,%s,%s,%s,%s,%s,%s,%s
                )
                ON CONFLICT (filing_uuid) DO UPDATE SET
                    has_foreign_entity = EXCLUDED.has_foreign_entity,
                    foreign_entity_names = EXCLUDED.foreign_entity_names,
                    foreign_entity_countries = EXCLUDED.foreign_entity_countries
                """,
                [(r[0], r[2], r[4], r[5], r[6], r[7], r[8], r[9], r[10], r[11], r[12]) for r in reg_buf],
            )
        reg_buf.clear()

    if link_buf:
        async with conn.cursor() as cur:
            await cur.executemany(
                """
                INSERT INTO lobbying_lobbyists (registration_id, lobbyist_id)
                SELECT
                    (SELECT id FROM lobbying_registrations WHERE filing_uuid = %s LIMIT 1),
                    COALESCE(
                        (SELECT id FROM lobbyists WHERE lda_id = %s LIMIT 1),
                        (SELECT id FROM lobbyists WHERE name_normalized = %s LIMIT 1)
                    )
                ON CONFLICT DO NOTHING
                """,
                link_buf,
            )
        link_buf.clear()

    await conn.commit()


async def ingest_year(session: aiohttp.ClientSession, conn, year: int, start_page: int = 1):
    page = max(1, start_page)
    processed = 0
    org_buf = []
    reg_buf = []
    lobbyist_buf = []
    link_buf = []

    pbar = tqdm(desc=f"LDA {year}", unit="filings")
    start = time.time()

    while True:
        url = f"{API_BASE}/filings/"
        payload = await fetch_json(
            session,
            url,
            params={"filing_year": year, "page_size": 200, "page": page},
        )
        if not payload:
            break
        results = payload.get("results", [])
        if not results:
            break

        for filing in results:
            try:
                rec = filing_to_records(filing)
                if not rec:
                    continue
                org_rows, reg_row, lobbyist_rows, link_rows = rec
                org_buf.extend(org_rows)
                reg_buf.append(reg_row)
                lobbyist_buf.extend(lobbyist_rows)
                link_buf.extend(link_rows)
                processed += 1
                pbar.update(1)
            except Exception:
                logging.exception("Failed processing LDA filing_uuid=%s", filing.get("filing_uuid"))

            if processed % FLUSH_SIZE == 0:
                await flush_buffers(conn, org_buf, reg_buf, lobbyist_buf, link_buf)

        pbar.set_postfix(page=page, elapsed=f"{int(time.time() - start)}s")
        if not payload.get("next"):
            break
        page += 1

    await flush_buffers(conn, org_buf, reg_buf, lobbyist_buf, link_buf)
    pbar.close()


async def main_async(years: list[int], start_page: int):
    if not API_KEY:
        raise RuntimeError("LDA_API_KEY is required")

    headers = {"Authorization": f"Token {API_KEY}"}
    conn = await connect_async()
    await apply_enhancement_migrations(conn)
    await optimize_for_bulk_load(conn)
    await drop_indexes_for_bulk_load(conn)

    async with aiohttp.ClientSession(headers=headers) as session:
        for idx, year in enumerate(years):
            year_start_page = start_page if idx == 0 else 1
            await ingest_year(session, conn, year, start_page=year_start_page)

    await rebuild_indexes(conn)
    await conn.commit()
    await conn.close()


def main():
    parser = argparse.ArgumentParser(description="Async ingest LDA filings")
    parser.add_argument("--years", nargs="+", type=int, default=[2023, 2024])
    parser.add_argument("--start-page", type=int, default=1)
    args = parser.parse_args()
    asyncio.run(main_async(args.years, args.start_page))


if __name__ == "__main__":
    main()
