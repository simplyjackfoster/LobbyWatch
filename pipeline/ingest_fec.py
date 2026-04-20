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
SENATE_LABEL = "senate"
HOUSE_LABEL = "house of representatives"
NAME_SUFFIXES = {"JR", "SR", "II", "III", "IV", "V"}
STATE_NAME_TO_CODE = {
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


def _to_state_code(value: str | None) -> str:
    if not value:
        return ""
    raw = str(value).strip().upper()
    if len(raw) == 2 and raw.isalpha():
        return raw
    return STATE_NAME_TO_CODE.get(raw, "")


def _name_variants(raw_name: str | None) -> set[str]:
    text = str(raw_name or "").strip()
    if not text:
        return set()

    variants: set[str] = set()
    norm = normalize_name(text)
    if norm:
        variants.add(norm)

    comma_match = re.match(r"^\s*([^,]+),\s*(.+)$", text)
    if comma_match:
        last, rest = comma_match.groups()
        reordered = f"{rest} {last}".strip()
        reordered_norm = normalize_name(reordered)
        if reordered_norm:
            variants.add(reordered_norm)

    tokens = [tok for tok in normalize_name(text).split() if tok and tok not in NAME_SUFFIXES]
    if comma_match:
        last = normalize_name(comma_match.group(1)).split()
        rest = normalize_name(comma_match.group(2)).split()
        tokens = [*rest, *last]
        tokens = [tok for tok in tokens if tok and tok not in NAME_SUFFIXES]

    if len(tokens) >= 2:
        first_last = f"{tokens[0]} {tokens[-1]}".strip()
        last_first = f"{tokens[-1]} {tokens[0]}".strip()
        variants.add(first_last)
        variants.add(last_first)

    return {variant for variant in variants if variant}


def _name_first_last(raw_name: str | None) -> tuple[str, str]:
    text = str(raw_name or "").strip()
    if not text:
        return "", ""

    comma_match = re.match(r"^\s*([^,]+),\s*(.+)$", text)
    if comma_match:
        last_tokens = [tok for tok in normalize_name(comma_match.group(1)).split() if tok and tok not in NAME_SUFFIXES]
        right_tokens = [tok for tok in normalize_name(comma_match.group(2)).split() if tok and tok not in NAME_SUFFIXES]
        if right_tokens and last_tokens:
            first = right_tokens[0]
            last = last_tokens[-1]
            return first, last

    tokens = [tok for tok in normalize_name(text).split() if tok and tok not in NAME_SUFFIXES]
    # Drop trailing single-letter initials when available (e.g. "TODD C YOUNG" stays with YOUNG as last).
    if len(tokens) >= 3 and len(tokens[-1]) == 1:
        tokens = tokens[:-1]
    if len(tokens) < 2:
        return "", tokens[0] if tokens else ""
    return tokens[0], tokens[-1]


async def load_legislator_index(conn):
    async with conn.cursor() as cur:
        await cur.execute("SELECT id, name, party, state, chamber FROM legislators WHERE is_active = true")
        rows = await cur.fetchall()

    by_name: dict[str, list[dict]] = {}
    senators_by_state: dict[str, list[dict]] = {}
    for lid, name, party, state, chamber in rows:
        chamber_norm = str(chamber or "").strip().lower()
        state_code = _to_state_code(state)
        party_norm = (str(party or "").strip().upper()[:1] or "")
        entry = {
            "id": lid,
            "name": name,
            "party": party_norm,
            "state_code": state_code,
            "chamber": chamber_norm,
        }
        for key in _name_variants(name):
            by_name.setdefault(key, []).append(entry)
        if chamber_norm == SENATE_LABEL and state_code:
            senators_by_state.setdefault(state_code, []).append(entry)
    return {"by_name": by_name, "senators_by_state": senators_by_state}


def _resolve_legislator_id(candidate_name: str | None, candidate_meta: dict[str, Any], legislator_index: dict):
    by_name = legislator_index["by_name"]
    senators_by_state = legislator_index["senators_by_state"]
    office = str(
        candidate_meta.get("candidate_office")
        or candidate_meta.get("CAND_OFFICE")
        or ""
    ).strip().upper()
    state_code = _to_state_code(
        candidate_meta.get("candidate_office_state")
        or candidate_meta.get("CAND_OFFICE_ST")
        or candidate_meta.get("candidate_state")
        or candidate_meta.get("CAND_ST")
    )
    party = str(
        candidate_meta.get("candidate_party")
        or candidate_meta.get("CAND_PTY_AFFILIATION")
        or ""
    ).strip().upper()[:1]

    name_keys = _name_variants(candidate_name)
    exact_candidates: list[dict] = []
    for key in name_keys:
        exact_candidates.extend(by_name.get(key, []))

    def pick_best(candidates: list[dict]):
        if not candidates:
            return None
        if len(candidates) == 1:
            return candidates[0]["id"]
        filtered = candidates
        if office == "S":
            filtered = [c for c in filtered if c["chamber"] == SENATE_LABEL] or filtered
        elif office == "H":
            filtered = [c for c in filtered if c["chamber"] == HOUSE_LABEL] or filtered
        if state_code:
            filtered = [c for c in filtered if c["state_code"] == state_code] or filtered
        if party:
            filtered = [c for c in filtered if c["party"] == party] or filtered
        if len(filtered) == 1:
            return filtered[0]["id"]
        return None

    picked = pick_best(exact_candidates)
    if picked:
        return picked, "exact"

    if office == "S":
        pool = senators_by_state.get(state_code, []) if state_code else []
        if not pool:
            return None, "none"
        cand_first, cand_last = _name_first_last(candidate_name)
        if not cand_last:
            return None, "none"

        best = None
        best_score = 0.0
        for row in pool:
            row_first, row_last = _name_first_last(row["name"])
            if not row_last:
                continue
            if cand_last != row_last:
                continue
            score = 0.7
            if cand_first and row_first and cand_first == row_first:
                score += 0.2
            elif cand_first and row_first and row_first.startswith(cand_first[:1]):
                score += 0.1
            if party and row["party"] and party == row["party"]:
                score += 0.1
            if score > best_score:
                best_score = score
                best = row["id"]
        if best and best_score >= 0.75:
            return best, "senator_fallback"

    return None, "none"


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


async def flush_senator_backfill(conn, org_buf, contrib_buf):
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
                SELECT
                    (SELECT id FROM organizations WHERE name_normalized = %s LIMIT 1),
                    %s,
                    %s,
                    %s,
                    %s,
                    %s
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM contributions c
                    WHERE c.contributor_org_id = (SELECT id FROM organizations WHERE name_normalized = %s LIMIT 1)
                      AND c.recipient_legislator_id = %s
                      AND c.amount = %s
                      AND c.contribution_date IS NOT DISTINCT FROM %s::date
                      AND c.fec_committee_id IS NOT DISTINCT FROM %s
                      AND c.cycle = %s
                )
                """,
                [
                    (
                        contributor_norm,
                        legislator_id,
                        amount,
                        contribution_date,
                        cmte_id,
                        cycle,
                        contributor_norm,
                        legislator_id,
                        amount,
                        contribution_date,
                        cmte_id,
                        cycle,
                    )
                    for (
                        contributor_norm,
                        legislator_id,
                        amount,
                        contribution_date,
                        cmte_id,
                        cycle,
                    ) in contrib_buf
                ],
            )
        contrib_buf.clear()
    await conn.commit()


async def ingest_cycle(session: aiohttp.ClientSession, conn, cycle: int, min_amount: int):
    legislator_index = await load_legislator_index(conn)
    page = 1
    processed = 0
    recovered_senator_matches = 0
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

                legislator_id, match_mode = _resolve_legislator_id(candidate_name, row, legislator_index)
                if not legislator_id:
                    continue
                if match_mode == "senator_fallback":
                    recovered_senator_matches += 1

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
    logging.info("FEC API cycle=%s processed=%s recovered_senator_matches=%s", cycle, processed, recovered_senator_matches)


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
    legislator_index = await load_legislator_index(conn)
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

    candidate_meta_by_id: dict[str, dict[str, Any]] = {}
    for line in _read_zip_rows(cn_zip):
        parts = line.split("|")
        if len(parts) < len(cn_cols):
            continue
        row = dict(zip(cn_cols, parts))
        candidate_meta_by_id[row["CAND_ID"]] = row

    org_buf = []
    contrib_buf = []
    processed = 0
    recovered_senator_matches = 0
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
            candidate_meta = candidate_meta_by_id.get(candidate_id or "", {})
            candidate_name = candidate_meta.get("CAND_NAME") if candidate_meta else ""
            if not candidate_name:
                continue
            legislator_id, match_mode = _resolve_legislator_id(candidate_name, candidate_meta, legislator_index)
            if not legislator_id:
                continue
            if match_mode == "senator_fallback":
                recovered_senator_matches += 1

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
    logging.info("FEC BULK cycle=%s processed=%s recovered_senator_matches=%s", cycle, processed, recovered_senator_matches)


async def senator_backfill_cycle_bulk(conn, bulk_dir: Path, cycle: int, min_amount: int):
    legislator_index = await load_legislator_index(conn)
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

    candidate_meta_by_id: dict[str, dict[str, Any]] = {}
    for line in _read_zip_rows(cn_zip):
        parts = line.split("|")
        if len(parts) < len(cn_cols):
            continue
        row = dict(zip(cn_cols, parts))
        candidate_meta_by_id[row["CAND_ID"]] = row

    org_buf = []
    contrib_buf = []
    processed = 0
    inserted_candidates = 0
    pbar = tqdm(desc=f"SENATOR BACKFILL {cycle}", unit="rows")
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
            candidate_meta = candidate_meta_by_id.get(candidate_id or "", {})
            if (candidate_meta.get("CAND_OFFICE") or "").strip().upper() != "S":
                continue
            candidate_name = candidate_meta.get("CAND_NAME")
            legislator_id, match_mode = _resolve_legislator_id(candidate_name, candidate_meta, legislator_index)
            if not legislator_id:
                continue
            if match_mode != "senator_fallback":
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
            inserted_candidates += 1
            pbar.update(1)

            if processed % FLUSH_SIZE == 0:
                await flush_senator_backfill(conn, org_buf, contrib_buf)
        except Exception:
            logging.exception("Failed senator backfill row")

    await flush_senator_backfill(conn, org_buf, contrib_buf)
    pbar.close()
    logging.info("SENATOR BACKFILL cycle=%s attempted_inserts=%s", cycle, inserted_candidates)


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
    parser.add_argument(
        "--senator-backfill",
        action="store_true",
        help="Run a senator-focused backfill pass to recover contributions from rows with previously unmatched candidate names (bulk mode only).",
    )
    parser.add_argument(
        "--senator-backfill-only",
        action="store_true",
        help="Skip normal ingest and run only the senator backfill pass (requires --bulk-dir).",
    )
    args = parser.parse_args()
    if args.senator_backfill_only and not args.bulk_dir:
        raise RuntimeError("--senator-backfill-only requires --bulk-dir")

    if args.senator_backfill_only:
        async def _run_backfill_only():
            conn = await connect_async()
            base = Path(args.bulk_dir).expanduser()
            for cycle in args.cycles:
                await senator_backfill_cycle_bulk(conn, base, cycle, args.min_amount)
            await conn.commit()
            await conn.close()
        asyncio.run(_run_backfill_only())
        return

    asyncio.run(main_async(args.cycles, args.min_amount, args.bulk_dir))
    if args.senator_backfill and args.bulk_dir:
        async def _run_backfill():
            conn = await connect_async()
            base = Path(args.bulk_dir).expanduser()
            for cycle in args.cycles:
                await senator_backfill_cycle_bulk(conn, base, cycle, args.min_amount)
            await conn.commit()
            await conn.close()
        asyncio.run(_run_backfill())


if __name__ == "__main__":
    main()
