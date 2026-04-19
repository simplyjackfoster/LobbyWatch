import argparse
import logging
import os
import time
from datetime import datetime

import requests
from dotenv import load_dotenv
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert

from db import (
    SessionLocal,
    complete_ingestion_run,
    contributions,
    fail_ingestion_run,
    get_resume_page,
    legislators,
    normalize_name,
    start_ingestion_run,
    update_ingestion_run_progress,
    upsert_organization,
)

load_dotenv()

API_BASE = "https://api.open.fec.gov/v1"
API_KEY = os.getenv("FEC_API_KEY")
RATE_LIMIT_SECONDS = 0.2
SOURCE = "fec"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("pipeline_errors.log"),
    ],
)


def fetch_schedule_a_page(cycle: int, page: int, per_page: int = 100):
    params = {
        "api_key": API_KEY,
        "two_year_transaction_period": cycle,
        "per_page": per_page,
        "page": page,
        "sort": "-contribution_receipt_date",
    }
    url = f"{API_BASE}/schedules/schedule_a/"
    resp = requests.get(url, params=params, timeout=30)
    time.sleep(RATE_LIMIT_SECONDS)
    resp.raise_for_status()
    data = resp.json()
    results = data.get("results", [])
    pages = (data.get("pagination", {}) or {}).get("pages") or page
    has_next = page < pages
    return results, has_next


def legislator_id_from_fec_row(db, row: dict):
    candidate_name = row.get("candidate_name") or ""
    if not candidate_name:
        return None
    cand_norm = normalize_name(candidate_name.replace(",", " "))
    rows = db.execute(select(legislators.c.id, legislators.c.name)).all()
    for r in rows:
        if normalize_name(r.name) == cand_norm:
            return r.id
    return None


def ingest_cycle(cycle: int):
    db = SessionLocal()
    processed = 0
    run_id = None

    try:
        resume_page = get_resume_page(db, SOURCE)
        run_id = start_ingestion_run(db, SOURCE, last_page=resume_page - 1)
        page = max(1, resume_page)

        while True:
            try:
                rows, has_next = fetch_schedule_a_page(cycle=cycle, page=page)
            except Exception:
                logging.exception("Failed FEC page=%s cycle=%s", page, cycle)
                raise

            if not rows:
                break

            for row in rows:
                try:
                    entity_type = (row.get("entity_type") or "").upper()
                    if entity_type not in {"PAC", "ORG"}:
                        continue

                    contributor_name = row.get("contributor_name")
                    if not contributor_name:
                        continue

                    contributor_id = upsert_organization(db, contributor_name, org_type="pac")
                    recipient_legislator_id = legislator_id_from_fec_row(db, row)
                    if not recipient_legislator_id:
                        continue

                    amount = row.get("contribution_receipt_amount")
                    contribution_date = row.get("contribution_receipt_date")
                    fec_committee_id = row.get("committee_id")

                    stmt = insert(contributions).values(
                        contributor_org_id=contributor_id,
                        recipient_legislator_id=recipient_legislator_id,
                        amount=amount,
                        contribution_date=contribution_date,
                        fec_committee_id=fec_committee_id,
                        cycle=cycle,
                    )
                    db.execute(stmt)
                    processed += 1
                except Exception:
                    db.rollback()
                    logging.exception("Failed FEC row")

            db.commit()
            update_ingestion_run_progress(
                db,
                run_id=run_id,
                last_page=page,
                records_processed=processed,
                last_filing_uuid=rows[-1].get("sub_id") or rows[-1].get("tran_id"),
            )
            logging.info("Cycle %s processed contributions=%s page=%s", cycle, processed, page)

            if not has_next:
                break
            page += 1

        complete_ingestion_run(db, run_id)
    except Exception:
        if run_id is not None:
            fail_ingestion_run(db, run_id)
        raise
    finally:
        db.close()

    logging.info("Done ingest cycle=%s rows=%s", cycle, processed)


def main():
    parser = argparse.ArgumentParser(description="Ingest FEC schedule A PAC/ORG receipts")
    parser.add_argument("--cycles", nargs="+", type=int, default=[2022, 2024])
    args = parser.parse_args()

    if not API_KEY:
        raise RuntimeError("FEC_API_KEY is required")

    logging.info("Starting FEC ingest at %s", datetime.utcnow().isoformat())
    for cycle in args.cycles:
        ingest_cycle(cycle)


if __name__ == "__main__":
    main()
