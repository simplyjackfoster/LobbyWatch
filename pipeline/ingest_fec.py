import argparse
import asyncio
import logging
import os
import re
import time
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Any

import aiohttp
from dotenv import load_dotenv
from tqdm import tqdm

from db import connect_async, drop_indexes_for_bulk_load, normalize_name, optimize_for_bulk_load, rebuild_indexes

load_dotenv()

API_BASE = "https://api.open.fec.gov/v1"
API_KEY = os.getenv("FEC_API_KEY")
SEM = asyncio.Semaphore(8)
FLUSH_SIZE = 500

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler("pipeline_errors.log")],
)


async def fetch_json(session: aiohttp.ClientSession, path: str, params: dict[str, Any]):
    delay = 0.2
    for attempt in range(3):
        try:
            async with SEM:
                async with session.get(f"{API_BASE}{path}", params=params, timeout=60) as resp:
                    if resp.status >= 400:
                        raise RuntimeError(f"HTTP {resp.status}: {await resp.text()}")
                    return await resp.json()
        except Exception:
            if attempt == 2:
                logging.exception("FEC request failed path=%s params=%s", path, params)
                return None
            await asyncio.sleep(delay)
            delay *= 2


async def load_legislator_map(conn):
    async with conn.cursor() as cur:
        await cur.execute("SELECT id, name FROM legislators WHERE is_active = true")
        rows = await cur.fetchall()
    return {normalize_name(name): lid for lid, name in rows}


async def flush_buffers(conn, org_buf, contrib_buf):
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

    if contrib_buf:
        async with conn.cursor() as cur:
            await cur.executemany(
                """
                INSERT INTO contributions (
                    contributor_org_id,
                    recipient_legislator_id,
                    amount,
                    contribution_date,
                    fec_committee_id,
                    cycle
                )
                VALUES (
                    (SELECT id FROM organizations WHERE name_normalized = %s LIMIT 1),
                    %s,
                    %s,
                    %s,
                    %s,
                    %s
                )
                ON CONFLICT DO NOTHING
                """,
                contrib_buf,
            )
        contrib_buf.clear()

    await conn.commit()


async def ingest_cycle(session: aiohttp.ClientSession, conn, cycle: int, min_amount: int):
    legislator_map = await load_legislator_map(conn)
    page = 1
    processed = 0
    org_buf = []
    contrib_buf = []
    pbar = tqdm(desc=f"FEC {cycle}", unit="rows")
    start = time.time()

    while True:
        await asyncio.sleep(3.6)
        payload = await fetch_json(
            session,
            "/schedules/schedule_a/",
            {
                "api_key": API_KEY,
                "two_year_transaction_period": cycle,
                "per_page": 100,
                "page": page,
                "sort": "-contribution_receipt_date",
                "min_amount": min_amount,
                "entity_type": ["PAC", "ORG", "CCM"],
            },
        )
        if not payload:
            break
        rows = payload.get("results", [])
        if not rows:
            break

        for row in rows:
            try:
                entity_type = (row.get("entity_type") or "").upper()
                amount = row.get("contribution_receipt_amount") or 0
                if entity_type not in {"PAC", "ORG", "CCM"}:
                    continue
                if float(amount) < float(min_amount):
                    continue

                contributor_name = row.get("contributor_name")
                candidate_name = row.get("candidate_name")
                if not contributor_name or not candidate_name:
                    continue

                legislator_id = legislator_map.get(normalize_name(candidate_name.replace(",", " ")))
                if not legislator_id:
                    continue

                contributor_norm = normalize_name(contributor_name)
                org_buf.append((contributor_name, contributor_norm, "pac"))
                contrib_buf.append(
                    (
                        contributor_norm,
                        legislator_id,
                        amount,
                        row.get("contribution_receipt_date"),
                        row.get("committee_id"),
                        cycle,
                    )
                )
                processed += 1
                pbar.update(1)

                if processed % FLUSH_SIZE == 0:
                    await flush_buffers(conn, org_buf, contrib_buf)
            except Exception:
                logging.exception("Failed FEC row")

        pbar.set_postfix(page=page, elapsed=f"{int(time.time() - start)}s")
        pages = (payload.get("pagination", {}) or {}).get("pages") or page
        if page >= pages:
            break
        page += 1

    await flush_buffers(conn, org_buf, contrib_buf)
    pbar.close()


def _load_header(header_path: Path) -> list[str]:
    return header_path.read_text(encoding="utf-8").strip().split(",")


def _read_zip_rows(zip_path: Path):
    with zipfile.ZipFile(zip_path) as zf:
        name = zf.namelist()[0]
        with zf.open(name) as fh:
            for raw in fh:
                yield raw.decode("latin1", errors="ignore").rstrip("\n")


def _parse_mmddyyyy(value: str | None):
    if not value:
        return None
    try:
        return datetime.strptime(value, "%m%d%Y").date()
    except Exception:
        return None


def _cycle_suffix(cycle: int) -> str:
    return str(cycle)[-2:]


async def ingest_cycle_bulk(conn, bulk_dir: Path, cycle: int, min_amount: int):
    legislator_map = await load_legislator_map(conn)
    suffix = _cycle_suffix(cycle)

    cm_zip = bulk_dir / f"cm{suffix}.zip"
    cn_zip = bulk_dir / f"cn{suffix}.zip"
    pas2_zip = bulk_dir / f"pas2{suffix}.zip"
    cm_header = bulk_dir / "cm_header_file.csv"
    cn_header = bulk_dir / "cn_header_file.csv"
    pas2_header = bulk_dir / "pas2_header_file.csv"

    required = [cm_zip, cn_zip, pas2_zip, cm_header, cn_header, pas2_header]
    missing = [str(p) for p in required if not p.exists()]
    if missing:
        raise RuntimeError(f"Missing bulk files for cycle {cycle}: {missing}")

    cm_cols = _load_header(cm_header)
    cn_cols = _load_header(cn_header)
    pas2_cols = _load_header(pas2_header)

    committee_name_by_id: dict[str, str] = {}
    for line in _read_zip_rows(cm_zip):
        parts = line.split("|")
        if len(parts) < len(cm_cols):
            continue
        row = dict(zip(cm_cols, parts))
        committee_name_by_id[row["CMTE_ID"]] = row["CMTE_NM"]

    candidate_name_by_id: dict[str, str] = {}
    for line in _read_zip_rows(cn_zip):
        parts = line.split("|")
        if len(parts) < len(cn_cols):
            continue
        row = dict(zip(cn_cols, parts))
        candidate_name_by_id[row["CAND_ID"]] = row["CAND_NAME"]

    org_buf = []
    contrib_buf = []
    processed = 0
    pbar = tqdm(desc=f"FEC BULK {cycle}", unit="rows")
    start = time.time()
    period_year_re = re.compile(r"[A-Z]\d{4}$")

    for line in _read_zip_rows(pas2_zip):
        try:
            parts = line.split("|")
            if len(parts) < len(pas2_cols):
                continue
            row = dict(zip(pas2_cols, parts))

            if (row.get("ENTITY_TP") or "").upper() not in {"PAC", "ORG", "CCM"}:
                continue
            txn_pgi = (row.get("TRANSACTION_PGI") or "").strip().upper()
            if not period_year_re.match(txn_pgi):
                continue
            if not txn_pgi.endswith(str(cycle)):
                continue

            amount = float(row.get("TRANSACTION_AMT") or 0)
            if amount < float(min_amount):
                continue

            candidate_id = row.get("CAND_ID")
            candidate_name = candidate_name_by_id.get(candidate_id or "", "")
            if not candidate_name:
                continue
            legislator_id = legislator_map.get(normalize_name(candidate_name.replace(",", " ")))
            if not legislator_id:
                continue

            cmte_id = row.get("CMTE_ID")
            contributor_name = committee_name_by_id.get(cmte_id or "", cmte_id or "")
            if not contributor_name:
                continue

            contributor_norm = normalize_name(contributor_name)
            org_buf.append((contributor_name, contributor_norm, "pac"))
            contrib_buf.append(
                (
                    contributor_norm,
                    legislator_id,
                    amount,
                    _parse_mmddyyyy(row.get("TRANSACTION_DT")),
                    cmte_id,
                    cycle,
                )
            )
            processed += 1
            pbar.update(1)

            if processed % FLUSH_SIZE == 0:
                await flush_buffers(conn, org_buf, contrib_buf)
                pbar.set_postfix(elapsed=f"{int(time.time() - start)}s")
        except Exception:
            logging.exception("Failed FEC bulk row")

    await flush_buffers(conn, org_buf, contrib_buf)
    pbar.close()


async def main_async(cycles: list[int], min_amount: int, bulk_dir: str | None):
    use_bulk = bool(bulk_dir)
    if not use_bulk and not API_KEY:
        raise RuntimeError("FEC_API_KEY is required when not using --bulk-dir")

    conn = await connect_async()
    await optimize_for_bulk_load(conn)
    await drop_indexes_for_bulk_load(conn)

    if use_bulk:
        base = Path(bulk_dir).expanduser()
        for cycle in cycles:
            await ingest_cycle_bulk(conn, base, cycle, min_amount)
    else:
        async with aiohttp.ClientSession() as session:
            for cycle in cycles:
                await ingest_cycle(session, conn, cycle, min_amount)

    await rebuild_indexes(conn)
    await conn.commit()
    await conn.close()


def main():
    parser = argparse.ArgumentParser(description="Async ingest FEC schedule A receipts")
    parser.add_argument("--cycles", nargs="+", type=int, default=[2024])
    parser.add_argument("--min-amount", type=int, default=1000)
    parser.add_argument("--bulk-dir", type=str, default=None, help="Directory with cm/cn/pas2 zip files and header csv files")
    args = parser.parse_args()
    asyncio.run(main_async(args.cycles, args.min_amount, args.bulk_dir))


if __name__ == "__main__":
    main()
