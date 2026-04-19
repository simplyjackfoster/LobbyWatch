import argparse
import logging
import os
import time
from datetime import datetime

import requests
from dotenv import load_dotenv
from sqlalchemy.dialects.postgresql import insert

from db import (
    SessionLocal,
    complete_ingestion_run,
    fail_ingestion_run,
    get_resume_page,
    lobbying_lobbyists,
    lobbying_registrations,
    start_ingestion_run,
    update_ingestion_run_progress,
    upsert_lobbyist,
    upsert_organization,
)

load_dotenv()

API_BASE = "https://lda.senate.gov/api/v1"
API_KEY = os.getenv("LDA_API_KEY")
RATE_LIMIT_SECONDS = 0.5
SOURCE = "lda"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("pipeline_errors.log"),
    ],
)


def fetch_page(endpoint: str, params: dict, page: int):
    url = f"{API_BASE}{endpoint}"
    headers = {"Authorization": f"Token {API_KEY}"}
    req_params = {**params, "page": page}
    resp = requests.get(url, params=req_params, headers=headers, timeout=30)
    time.sleep(RATE_LIMIT_SECONDS)
    resp.raise_for_status()
    payload = resp.json()
    results = payload.get("results", []) if isinstance(payload, dict) else []
    has_next = bool(payload.get("next")) if isinstance(payload, dict) else False
    return results, has_next


def filing_period_to_year(period: str):
    if not period:
        return None
    for token in period.split("-"):
        if token.isdigit() and len(token) == 4:
            return int(token)
    return None


def extract_issue_fields(filing: dict):
    issue_items = filing.get("lobbying_issues") or []
    issue_codes = sorted({x.get("general_issue_code") for x in issue_items if x.get("general_issue_code")})

    activities = filing.get("lobbying_activities") or []
    general_issue_codes = list({a.get("general_issue_code") for a in activities if a.get("general_issue_code")})
    specific_issues = " | ".join(
        a.get("specific_issues", "")
        for a in activities
        if a.get("specific_issues")
    )

    if not general_issue_codes:
        general_issue_codes = issue_codes
    if not specific_issues:
        specific_issues = " | ".join([x.get("specific_issues") for x in issue_items if x.get("specific_issues")])

    return issue_codes, general_issue_codes, specific_issues


def ingest_filings(year_start: int, year_end: int, filing_period: str | None):
    db = SessionLocal()
    inserted = 0
    params = {
        "page_size": 50,
        "filing_year__gte": year_start,
        "filing_year__lte": year_end,
    }
    if filing_period:
        params["filing_period"] = filing_period

    run_id = None
    page = 1

    try:
        page = get_resume_page(db, SOURCE)
        run_id = start_ingestion_run(db, SOURCE, last_page=page - 1)
        logging.info("LDA ingest run_id=%s resume_page=%s", run_id, page)

        while True:
            try:
                filings, has_next = fetch_page("/filings/", params, page)
            except Exception:
                logging.exception("Failed fetching LDA page=%s", page)
                raise

            if not filings:
                break

            for filing in filings:
                try:
                    registrant_name = (filing.get("registrant") or {}).get("name")
                    client_name = (filing.get("client") or {}).get("name")
                    filing_uuid = filing.get("filing_uuid")
                    amount = filing.get("income") or filing.get("expenses") or filing.get("amount")
                    filing_year = filing.get("filing_year") or filing_period_to_year(filing.get("filing_period"))
                    period = filing.get("filing_period")

                    if not filing_uuid:
                        logging.warning("Skipping filing with no UUID")
                        continue

                    registrant_id = upsert_organization(db, registrant_name, "registrant") if registrant_name else None
                    client_id = upsert_organization(db, client_name, "client") if client_name else None
                    issue_codes, general_issue_codes, specific_issues = extract_issue_fields(filing)

                    reg_stmt = (
                        insert(lobbying_registrations)
                        .values(
                            registrant_id=registrant_id,
                            client_id=client_id,
                            filing_uuid=filing_uuid,
                            filing_year=filing_year,
                            filing_period=period,
                            amount=amount,
                            issue_codes=issue_codes,
                            general_issue_codes=general_issue_codes,
                            specific_issues=specific_issues,
                        )
                        .on_conflict_do_update(
                            index_elements=[lobbying_registrations.c.filing_uuid],
                            set_={
                                "amount": amount,
                                "issue_codes": issue_codes,
                                "general_issue_codes": general_issue_codes,
                                "specific_issues": specific_issues,
                            },
                        )
                        .returning(lobbying_registrations.c.id)
                    )
                    registration_id = db.execute(reg_stmt).scalar_one_or_none()
                    if not registration_id:
                        continue

                    for lob in filing.get("lobbyists", []) or []:
                        name = lob.get("lobbyist") or lob.get("name")
                        if isinstance(name, dict):
                            name = name.get("name")
                        lobbyist_id = upsert_lobbyist(db, name=name, lda_id=lob.get("id") or lob.get("lobbyist_id"))
                        if not lobbyist_id:
                            continue
                        link_stmt = (
                            insert(lobbying_lobbyists)
                            .values(registration_id=registration_id, lobbyist_id=lobbyist_id)
                            .on_conflict_do_nothing()
                        )
                        db.execute(link_stmt)

                    db.commit()
                    inserted += 1
                except Exception:
                    db.rollback()
                    logging.exception("Failed processing filing %s", filing.get("filing_uuid"))

            update_ingestion_run_progress(
                db,
                run_id=run_id,
                last_page=page,
                records_processed=inserted,
                last_filing_uuid=filings[-1].get("filing_uuid"),
            )
            logging.info("Processed page=%s total_filings=%s", page, inserted)

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

    logging.info("Done. Processed filings: %s", inserted)


def main():
    parser = argparse.ArgumentParser(description="Ingest LDA filings")
    parser.add_argument("--year-start", type=int, default=2023)
    parser.add_argument("--year-end", type=int, default=2025)
    parser.add_argument("--period", type=str, default=None, help="e.g. first_quarter")
    args = parser.parse_args()

    if not API_KEY:
        raise RuntimeError("LDA_API_KEY is required")

    logging.info(
        "Starting LDA ingest for years %s-%s period=%s at %s",
        args.year_start,
        args.year_end,
        args.period,
        datetime.utcnow().isoformat(),
    )
    ingest_filings(args.year_start, args.year_end, args.period)


if __name__ == "__main__":
    main()
