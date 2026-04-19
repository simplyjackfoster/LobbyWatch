import argparse
import logging
import os
import time

import requests
from dotenv import load_dotenv
from sqlalchemy.dialects.postgresql import insert

from db import (
    SessionLocal,
    committee_memberships,
    complete_ingestion_run,
    fail_ingestion_run,
    get_resume_page,
    start_ingestion_run,
    update_ingestion_run_progress,
    upsert_committee,
    upsert_legislator,
    votes,
)

load_dotenv()

API_BASE = "https://api.congress.gov/v3"
API_KEY = os.getenv("CONGRESS_API_KEY")
RATE_LIMIT_SECONDS = 0.2
SOURCE = "congress"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("pipeline_errors.log"),
    ],
)


def get_json(path: str, params: dict | None = None):
    p = {"api_key": API_KEY, "format": "json"}
    if params:
        p.update(params)
    url = f"{API_BASE}{path}"
    resp = requests.get(url, params=p, timeout=30)
    time.sleep(RATE_LIMIT_SECONDS)
    resp.raise_for_status()
    return resp.json()


def ingest_member_committees(db, legislator_id: int, bioguide: str):
    if not bioguide:
        return
    try:
        data = get_json(f"/member/{bioguide}/committee")
    except Exception:
        logging.exception("Failed committees for bioguide=%s", bioguide)
        return

    committee_items = data.get("committees", []) or data.get("memberCommittees", [])
    for c in committee_items:
        try:
            committee_id = c.get("systemCode") or c.get("code") or c.get("committeeCode")
            name = c.get("name") or c.get("title")
            chamber = (c.get("chamber") or "").lower()
            role = c.get("memberType") or c.get("role")
            parent = c.get("parent") or c.get("subcommitteeOf")

            committee_pk = upsert_committee(db, committee_id, name, chamber, parent)
            if not committee_pk:
                continue

            stmt = (
                insert(committee_memberships)
                .values(legislator_id=legislator_id, committee_id=committee_pk, role=role)
                .on_conflict_do_update(
                    index_elements=[committee_memberships.c.legislator_id, committee_memberships.c.committee_id],
                    set_={"role": role},
                )
            )
            db.execute(stmt)
        except Exception:
            logging.exception("Failed committee record for bioguide=%s", bioguide)


def ingest_member_votes(db, legislator_id: int, bioguide: str):
    if not bioguide:
        return
    for congress in [118, 119]:
        try:
            data = get_json(f"/member/{bioguide}/vote", {"congress": congress, "limit": 100})
        except Exception:
            logging.exception("Failed votes for %s congress=%s", bioguide, congress)
            continue

        vote_items = data.get("votes", []) or data.get("memberVotes", [])
        for v in vote_items:
            try:
                bill = v.get("bill") or {}
                bill_id = bill.get("number") or v.get("billNumber") or v.get("url")
                title = bill.get("title") or v.get("description")
                position = v.get("position") or v.get("vote")
                vote_date = v.get("date") or v.get("actionDate")

                issue_tags = []
                policy = v.get("policyArea")
                if isinstance(policy, dict) and policy.get("name"):
                    issue_tags.append(policy["name"])

                stmt = insert(votes).values(
                    legislator_id=legislator_id,
                    bill_id=bill_id,
                    bill_title=title,
                    vote_position=position,
                    vote_date=vote_date,
                    congress=congress,
                    issue_tags=issue_tags,
                )
                db.execute(stmt)
            except Exception:
                logging.exception("Failed vote record for %s", bioguide)


def ingest_members(limit: int = 250):
    db = SessionLocal()
    processed = 0
    run_id = None

    try:
        resume_page = get_resume_page(db, SOURCE)
        run_id = start_ingestion_run(db, SOURCE, last_page=resume_page - 1)
        page = max(1, resume_page)
        offset = (page - 1) * limit

        while True:
            try:
                data = get_json("/member", {"limit": limit, "offset": offset})
            except Exception:
                logging.exception("Failed Congress member page=%s", page)
                raise

            members = data.get("members", [])
            if not members:
                break

            for m in members:
                try:
                    bioguide = m.get("bioguideId")
                    name = m.get("name")
                    terms = m.get("terms", {}).get("item", [])
                    latest_term = terms[0] if terms else {}
                    party = latest_term.get("party") or m.get("partyName")
                    state = latest_term.get("stateCode") or m.get("state")
                    chamber = (latest_term.get("chamber") or "").lower()

                    legislator_id = upsert_legislator(
                        db,
                        bioguide_id=bioguide,
                        name=name,
                        party=party,
                        state=state,
                        chamber=chamber,
                        is_active=True,
                    )
                    if not legislator_id:
                        continue

                    ingest_member_committees(db, legislator_id, bioguide)
                    ingest_member_votes(db, legislator_id, bioguide)
                    db.commit()
                    processed += 1
                except Exception:
                    db.rollback()
                    logging.exception("Failed member row %s", m.get("bioguideId"))

            update_ingestion_run_progress(
                db,
                run_id=run_id,
                last_page=page,
                records_processed=processed,
                last_filing_uuid=(members[-1].get("bioguideId") if members else None),
            )
            logging.info("Congress members processed=%s page=%s", processed, page)

            page += 1
            offset += limit

        complete_ingestion_run(db, run_id)
    except Exception:
        if run_id is not None:
            fail_ingestion_run(db, run_id)
        raise
    finally:
        db.close()


def main():
    parser = argparse.ArgumentParser(description="Ingest Congress members, committees, votes")
    parser.add_argument("--limit", type=int, default=250)
    args = parser.parse_args()

    if not API_KEY:
        raise RuntimeError("CONGRESS_API_KEY is required")

    ingest_members(limit=args.limit)


if __name__ == "__main__":
    main()
