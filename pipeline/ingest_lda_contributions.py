import argparse
import asyncio
import json
import logging
import os
from typing import Any

import aiohttp
from dotenv import load_dotenv
from tqdm import tqdm

from db import apply_enhancement_migrations, connect_async, normalize_name

load_dotenv()

API_BASE = "https://lda.gov/api/v1"
API_KEY = os.getenv("LDA_API_KEY")
SEM = asyncio.Semaphore(10)
FLUSH_SIZE = 300
SOURCE_NAME = "lda_contributions"

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
        except Exception:
            if attempt == 2:
                logging.exception("LDA contribution request failed url=%s params=%s", url, params)
                return None
            await asyncio.sleep(delay)
            delay *= 2


def normalize_pacs(raw_pacs: Any) -> list[str]:
    if not raw_pacs:
        return []
    if isinstance(raw_pacs, list):
        values = []
        for item in raw_pacs:
            if isinstance(item, str):
                values.append(item)
            elif isinstance(item, dict):
                val = item.get("name") or item.get("pac_name") or item.get("committee_name")
                if val:
                    values.append(val)
        return values
    return []


def contribution_row(report: dict[str, Any]) -> tuple[Any, ...] | None:
    filing_uuid = report.get("filing_uuid") or report.get("uuid")
    if not filing_uuid:
        return None

    lobbyist = report.get("lobbyist") or {}
    lobbyist_lda_id = (
        report.get("lobbyist_id")
        or (lobbyist.get("id") if isinstance(lobbyist, dict) else None)
        or (lobbyist.get("lobbyist_id") if isinstance(lobbyist, dict) else None)
    )
    lobbyist_lda_id = str(lobbyist_lda_id) if lobbyist_lda_id else None

    registrant_obj = report.get("registrant") or {}
    registrant_name = None
    if isinstance(registrant_obj, dict):
        registrant_name = registrant_obj.get("name")
    elif isinstance(registrant_obj, str):
        registrant_name = registrant_obj
    registrant_name = registrant_name or report.get("registrant_name")
    registrant_normalized = normalize_name(registrant_name) if registrant_name else None

    filing_year = report.get("filing_year")
    filing_period = report.get("filing_period")
    contribution_items = report.get("contribution_items") or report.get("contributionItems") or []
    pacs = normalize_pacs(report.get("pacs"))

    dt_posted = report.get("dt_posted") or report.get("posted")
    if isinstance(dt_posted, str) and "T" in dt_posted:
        dt_posted = dt_posted.split("T", 1)[0]

    return (
        filing_uuid,
        lobbyist_lda_id,
        registrant_normalized,
        filing_year,
        filing_period,
        json.dumps(contribution_items),
        pacs,
        dt_posted,
    )


async def flush_rows(conn, rows: list[tuple[Any, ...]]):
    if not rows:
        return
    async with conn.cursor() as cur:
        await cur.executemany(
            """
            INSERT INTO lobbyist_contributions (
              filing_uuid,
              lobbyist_id,
              registrant_id,
              filing_year,
              filing_period,
              contribution_items,
              pacs,
              dt_posted
            )
            VALUES (
              %s,
              (SELECT id FROM lobbyists WHERE lda_id = %s LIMIT 1),
              (SELECT id FROM organizations WHERE name_normalized = %s LIMIT 1),
              %s,
              %s,
              %s::jsonb,
              %s,
              %s
            )
            ON CONFLICT (filing_uuid) DO UPDATE SET
              lobbyist_id = EXCLUDED.lobbyist_id,
              registrant_id = EXCLUDED.registrant_id,
              filing_year = EXCLUDED.filing_year,
              filing_period = EXCLUDED.filing_period,
              contribution_items = EXCLUDED.contribution_items,
              pacs = EXCLUDED.pacs,
              dt_posted = EXCLUDED.dt_posted
            """,
            rows,
        )
    rows.clear()
    await conn.commit()


async def get_or_start_run(conn):
    async with conn.cursor() as cur:
        await cur.execute(
            """
            SELECT id, last_page, records_processed
            FROM ingestion_runs
            WHERE source = %s
              AND status = 'running'
            ORDER BY started_at DESC NULLS LAST, id DESC
            LIMIT 1
            """,
            (SOURCE_NAME,),
        )
        existing = await cur.fetchone()
        if existing:
            run_id, last_page, records_processed = existing
            return run_id, int(last_page or 0) + 1, int(records_processed or 0)

        await cur.execute(
            """
            INSERT INTO ingestion_runs (source, started_at, status, last_page, records_processed)
            VALUES (%s, NOW(), 'running', 0, 0)
            RETURNING id
            """,
            (SOURCE_NAME,),
        )
        run_id = (await cur.fetchone())[0]
    await conn.commit()
    return run_id, 1, 0


async def update_run_progress(conn, run_id: int, last_page: int, records_processed: int, last_filing_uuid: str | None):
    async with conn.cursor() as cur:
        await cur.execute(
            """
            UPDATE ingestion_runs
            SET last_page = %s,
                records_processed = %s,
                last_filing_uuid = COALESCE(%s, last_filing_uuid)
            WHERE id = %s
            """,
            (last_page, records_processed, last_filing_uuid, run_id),
        )
    await conn.commit()


async def complete_run(conn, run_id: int):
    async with conn.cursor() as cur:
        await cur.execute(
            """
            UPDATE ingestion_runs
            SET status = 'complete',
                completed_at = NOW()
            WHERE id = %s
            """,
            (run_id,),
        )
    await conn.commit()


async def fail_run(conn, run_id: int):
    async with conn.cursor() as cur:
        await cur.execute(
            """
            UPDATE ingestion_runs
            SET status = 'failed',
                completed_at = NOW()
            WHERE id = %s
            """,
            (run_id,),
        )
    await conn.commit()


async def ingest_year(
    session: aiohttp.ClientSession,
    conn,
    year: int,
    start_page: int,
    run_id: int,
    processed_total: int,
):
    page = max(1, start_page)
    rows: list[tuple[Any, ...]] = []
    pbar = tqdm(desc=f"LDA-203 {year}", unit="filings")
    last_filing_uuid = None

    while True:
        payload = await fetch_json(
            session,
            f"{API_BASE}/contributions/",
            params={"filing_year": year, "page_size": 200, "page": page},
        )
        if not payload:
            break
        results = payload.get("results", [])
        if not results:
            break

        for report in results:
            try:
                row = contribution_row(report)
                if not row:
                    continue
                rows.append(row)
                last_filing_uuid = row[0]
                processed_total += 1
                pbar.update(1)
            except Exception:
                logging.exception("Failed processing LDA contribution filing_uuid=%s", report.get("filing_uuid"))

            if processed_total % FLUSH_SIZE == 0:
                await flush_rows(conn, rows)

        await flush_rows(conn, rows)
        await update_run_progress(conn, run_id, page, processed_total, last_filing_uuid)

        if not payload.get("next"):
            break
        page += 1

    pbar.close()
    return processed_total


async def main_async(years: list[int]):
    if not API_KEY:
        raise RuntimeError("LDA_API_KEY is required")

    headers = {"Authorization": f"Token {API_KEY}"}
    conn = await connect_async()
    await apply_enhancement_migrations(conn)

    run_id = None
    try:
        run_id, start_page, processed_total = await get_or_start_run(conn)
        async with aiohttp.ClientSession(headers=headers) as session:
            for idx, year in enumerate(years):
                year_start_page = start_page if idx == 0 else 1
                processed_total = await ingest_year(
                    session,
                    conn,
                    year,
                    year_start_page,
                    run_id,
                    processed_total,
                )
        await complete_run(conn, run_id)
    except Exception:
        logging.exception("Failed LDA contributions ingestion")
        if run_id is not None:
            await fail_run(conn, run_id)
    finally:
        await conn.close()


def main():
    parser = argparse.ArgumentParser(description="Async ingest LDA contribution reports (LD-203)")
    parser.add_argument("--years", nargs="+", type=int, default=[2023, 2024])
    args = parser.parse_args()
    asyncio.run(main_async(args.years))


if __name__ == "__main__":
    main()
