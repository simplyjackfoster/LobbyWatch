import json
import logging
import os
import time
from datetime import date, datetime, timezone
from typing import Any
from urllib import error as urllib_error
from urllib import parse as urllib_parse
from urllib import request as urllib_request

from aws_env import bootstrap_ssm_env

bootstrap_ssm_env()

from models import SessionLocal
from sqlalchemy import text

logger = logging.getLogger()
logger.setLevel(logging.INFO)

CONGRESS_API_BASE = "https://api.congress.gov/v3"
LDA_API_BASE = "https://lda.gov/api/v1"


def _ensure_pipeline_meta_table(db) -> None:
    db.execute(
        text(
            """
            CREATE TABLE IF NOT EXISTS _pipeline_meta (
              key TEXT PRIMARY KEY,
              value TEXT
            )
            """
        )
    )
    db.commit()


def _set_pipeline_meta(db, key: str, value: str | None) -> None:
    if value is None:
        return
    db.execute(
        text(
            """
            INSERT INTO _pipeline_meta (key, value)
            VALUES (:key, :value)
            ON CONFLICT (key) DO UPDATE
            SET value = EXCLUDED.value
            """
        ),
        {"key": key, "value": str(value)},
    )


def _period_rank_and_date(year: int, period: str | None) -> tuple[int, date]:
    raw = (period or "").strip().lower().replace(" ", "_")
    mapping: dict[str, tuple[int, tuple[int, int]]] = {
        "q1": (1, (3, 31)),
        "first_quarter": (1, (3, 31)),
        "quarter_1": (1, (3, 31)),
        "q2": (2, (6, 30)),
        "second_quarter": (2, (6, 30)),
        "quarter_2": (2, (6, 30)),
        "mid_year": (2, (6, 30)),
        "h1": (2, (6, 30)),
        "q3": (3, (9, 30)),
        "third_quarter": (3, (9, 30)),
        "quarter_3": (3, (9, 30)),
        "q4": (4, (12, 31)),
        "fourth_quarter": (4, (12, 31)),
        "quarter_4": (4, (12, 31)),
        "year_end": (4, (12, 31)),
        "h2": (4, (12, 31)),
    }
    rank, (month, day) = mapping.get(raw, (4, (12, 31)))
    return rank, date(year, month, day)


def _derive_lda_coverage_through(db) -> str | None:
    rows = db.execute(
        text(
            """
            SELECT filing_year, filing_period
            FROM lobbying_registrations
            WHERE filing_year IS NOT NULL
            ORDER BY filing_year DESC
            LIMIT 200
            """
        )
    ).all()
    if not rows:
        return None

    best_score = -1
    best_date = None
    for row in rows:
        year = int(row.filing_year)
        rank, period_date = _period_rank_and_date(year, row.filing_period)
        score = (year * 10) + rank
        if score > best_score:
            best_score = score
            best_date = period_date
    return best_date.isoformat() if best_date else None


def _derive_congress_coverage_through(db) -> str | None:
    vote_date = db.execute(text("SELECT MAX(vote_date)::text AS max_vote_date FROM votes")).scalar()
    return str(vote_date) if vote_date else None


def _publish_export_task() -> None:
    queue_url = (os.getenv("EXPORT_QUEUE_URL") or "").strip()
    if not queue_url:
        logger.warning("EXPORT_QUEUE_URL is not set; skipping export task enqueue")
        return
    import boto3

    sqs = boto3.client("sqs")
    sqs.send_message(QueueUrl=queue_url, MessageBody=json.dumps({"task": "export_and_release"}))


def _run_analyze() -> None:
    db = SessionLocal()
    try:
        db.execute(text("ANALYZE"))
        db.commit()
    finally:
        db.close()


def _ensure_pk_sequences(db) -> None:
    # Some restores copy IDs without bumping sequences; repair before upserts.
    for table, col in [
        ("organizations", "id"),
        ("legislators", "id"),
        ("committees", "id"),
        ("lobbyists", "id"),
        ("lobbying_registrations", "id"),
        ("co_sponsorships", "id"),
    ]:
        db.execute(
            text(
                f"""
                SELECT setval(
                  pg_get_serial_sequence('{table}', '{col}'),
                  COALESCE((SELECT MAX({col}) FROM {table}), 1),
                  true
                )
                """
            )
        )
    db.commit()


def _ensure_schema_compat(db) -> None:
    db.execute(
        text(
            """
            ALTER TABLE lobbyists
              ADD COLUMN IF NOT EXISTS covered_positions TEXT[],
              ADD COLUMN IF NOT EXISTS has_covered_position BOOLEAN DEFAULT FALSE,
              ADD COLUMN IF NOT EXISTS conviction_disclosure TEXT,
              ADD COLUMN IF NOT EXISTS has_conviction BOOLEAN DEFAULT FALSE
            """
        )
    )
    db.execute(
        text(
            """
            ALTER TABLE lobbying_registrations
              ADD COLUMN IF NOT EXISTS has_foreign_entity BOOLEAN DEFAULT FALSE,
              ADD COLUMN IF NOT EXISTS foreign_entity_names TEXT[],
              ADD COLUMN IF NOT EXISTS foreign_entity_countries TEXT[]
            """
        )
    )
    db.commit()


def _normalize_committee_code(raw_code: str | None) -> str:
    code = (raw_code or "").strip().lower()
    if not code:
        return ""
    # Congress committees are often 4-char roots (e.g., SSFI); DB stores root as xx..00.
    if len(code) == 4:
        return f"{code}00"
    return code


def _fetch_congress_json(
    path: str,
    params: dict[str, Any] | None = None,
    *,
    allow_404: bool = False,
    retries: int = 3,
    timeout: int = 45,
) -> dict[str, Any]:
    api_key = (os.getenv("CONGRESS_API_KEY") or "").strip()
    if not api_key:
        raise RuntimeError("CONGRESS_API_KEY is missing from Lambda environment")

    query = {"api_key": api_key, "format": "json"}
    if params:
        for key, value in params.items():
            if value is not None:
                query[key] = value

    url = f"{CONGRESS_API_BASE}{path}?{urllib_parse.urlencode(query, doseq=True)}"
    headers = {"User-Agent": "LobbyWatchWorker/1.0"}

    delay = 0.2
    for attempt in range(retries):
        try:
            req = urllib_request.Request(url, headers=headers)
            with urllib_request.urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib_error.HTTPError as exc:
            if allow_404 and exc.code == 404:
                return {"_not_found": True}
            if attempt == retries - 1:
                raise
        except Exception:
            if attempt == retries - 1:
                raise
        time.sleep(delay)
        delay *= 2

    return {}


def _normalize_name(value: str | None) -> str:
    raw = (value or "").strip().upper()
    chars = []
    prev_space = False
    for ch in raw:
        keep = ch.isalnum()
        if keep:
            chars.append(ch)
            prev_space = False
            continue
        if not prev_space:
            chars.append(" ")
            prev_space = True
    return " ".join("".join(chars).split())


def _fetch_lda_json(
    path: str,
    params: dict[str, Any] | None = None,
    *,
    retries: int = 3,
    timeout: int = 60,
) -> dict[str, Any]:
    api_key = (os.getenv("LDA_API_KEY") or "").strip()
    if not api_key:
        raise RuntimeError("LDA_API_KEY is missing from Lambda environment")

    query: dict[str, Any] = {}
    if params:
        for key, value in params.items():
            if value is not None:
                query[key] = value
    query_string = urllib_parse.urlencode(query, doseq=True)
    suffix = f"?{query_string}" if query_string else ""
    url = f"{LDA_API_BASE}{path}{suffix}"

    headers = {
        "Authorization": f"Token {api_key}",
        "User-Agent": "LobbyWatchWorker/1.0",
    }

    delay = 0.2
    for attempt in range(retries):
        try:
            req = urllib_request.Request(url, headers=headers)
            with urllib_request.urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib_error.HTTPError:
            if attempt == retries - 1:
                raise
        except Exception:
            if attempt == retries - 1:
                raise
        time.sleep(delay)
        delay *= 2

    return {}


def _upsert_legislator(
    db,
    *,
    bioguide_id: str,
    name: str,
    party: str | None,
    state: str | None,
    chamber: str | None,
) -> None:
    db.execute(
        text(
            """
            INSERT INTO legislators (bioguide_id, name, party, state, chamber, is_active)
            VALUES (:bioguide_id, :name, :party, :state, :chamber, TRUE)
            ON CONFLICT (bioguide_id) DO UPDATE
            SET
              name = EXCLUDED.name,
              party = COALESCE(EXCLUDED.party, legislators.party),
              state = COALESCE(EXCLUDED.state, legislators.state),
              chamber = COALESCE(EXCLUDED.chamber, legislators.chamber),
              is_active = TRUE
            """
        ),
        {
            "bioguide_id": bioguide_id,
            "name": name,
            "party": party,
            "state": state,
            "chamber": (chamber or "").lower() or None,
        },
    )


def _upsert_committee(
    db,
    *,
    committee_code: str,
    name: str | None,
    chamber: str | None,
) -> None:
    if not committee_code:
        return
    db.execute(
        text(
            """
            INSERT INTO committees (committee_id, name, chamber)
            VALUES (:committee_id, :name, :chamber)
            ON CONFLICT (committee_id) DO UPDATE
            SET
              name = COALESCE(NULLIF(EXCLUDED.name, ''), committees.name),
              chamber = COALESCE(NULLIF(EXCLUDED.chamber, ''), committees.chamber)
            """
        ),
        {
            "committee_id": committee_code,
            "name": name or committee_code.upper(),
            "chamber": (chamber or "").lower() or None,
        },
    )


def _upsert_committee_membership(
    db,
    *,
    bioguide_id: str,
    committee_code: str,
    role: str | None,
) -> bool:
    if not bioguide_id or not committee_code:
        return False
    result = db.execute(
        text(
            """
            INSERT INTO committee_memberships (legislator_id, committee_id, role)
            SELECT l.id, c.id, :role
            FROM legislators l
            JOIN committees c ON c.committee_id = :committee_code
            WHERE l.bioguide_id = :bioguide_id
            ON CONFLICT (legislator_id, committee_id) DO UPDATE
            SET role = COALESCE(EXCLUDED.role, committee_memberships.role)
            """
        ),
        {
            "bioguide_id": bioguide_id,
            "committee_code": committee_code,
            "role": role or "Member",
        },
    )
    return (result.rowcount or 0) > 0


def _sync_legislators(db, *, page_limit: int = 250, max_members: int | None = None) -> dict[str, Any]:
    offset = 0
    seen = 0
    upserts = 0

    while True:
        payload = _fetch_congress_json(
            "/member",
            {"currentMember": "true", "limit": page_limit, "offset": offset},
        )
        members = payload.get("members") or []
        if not members:
            break

        for member in members:
            bioguide_id = (member.get("bioguideId") or "").strip()
            if not bioguide_id:
                continue
            terms = ((member.get("terms") or {}).get("item")) or []
            if isinstance(terms, dict):
                terms = [terms]
            latest_term = terms[0] if terms else {}

            _upsert_legislator(
                db,
                bioguide_id=bioguide_id,
                name=(member.get("name") or "").strip() or bioguide_id,
                party=(latest_term.get("party") or member.get("partyName") or "").strip() or None,
                state=(latest_term.get("stateCode") or member.get("state") or "").strip() or None,
                chamber=(latest_term.get("chamber") or "").strip() or None,
            )
            upserts += 1
            seen += 1
            if max_members and seen >= max_members:
                db.commit()
                return {"seen": seen, "upserts": upserts, "stopped_early": True}

        db.commit()
        pagination = payload.get("pagination") or {}
        count = int(pagination.get("count") or 0)
        offset += len(members)
        if count and offset >= count:
            break

    return {"seen": seen, "upserts": upserts, "stopped_early": False}


def _sync_committee_memberships(
    db,
    *,
    congresses: list[int],
    chambers: list[str],
    page_limit: int = 250,
) -> dict[str, Any]:
    pages = 0
    memberships_seen = 0
    memberships_upserted = 0
    memberships_skipped = 0

    for congress in congresses:
        for chamber in chambers:
            offset = 0
            while True:
                payload = _fetch_congress_json(
                    f"/committee-membership/{int(congress)}",
                    {"limit": page_limit, "offset": offset, "chamber": chamber},
                    allow_404=True,
                )
                if payload.get("_not_found"):
                    logger.warning("committee-membership endpoint not found for congress=%s chamber=%s", congress, chamber)
                    break

                memberships = (
                    payload.get("committeeMembership")
                    or payload.get("committee_membership")
                    or payload.get("members")
                    or []
                )
                if not memberships:
                    break

                for item in memberships:
                    bioguide_id = (
                        item.get("bioguideId")
                        or (item.get("member") or {}).get("bioguideId")
                        or (item.get("legislator") or {}).get("bioguideId")
                        or ""
                    ).strip()
                    raw_committee_code = (
                        item.get("committeeCode")
                        or (item.get("committee") or {}).get("systemCode")
                        or item.get("systemCode")
                        or ""
                    )
                    committee_code = _normalize_committee_code(raw_committee_code)
                    committee_name = (
                        item.get("committeeName")
                        or (item.get("committee") or {}).get("name")
                        or item.get("name")
                    )
                    committee_chamber = (
                        item.get("chamber")
                        or (item.get("committee") or {}).get("chamber")
                        or chamber
                    )
                    rank = item.get("rank")
                    role = (rank.get("name") if isinstance(rank, dict) else rank) or item.get("memberType") or "Member"

                    if not bioguide_id or not committee_code:
                        memberships_skipped += 1
                        continue

                    _upsert_committee(
                        db,
                        committee_code=committee_code,
                        name=committee_name,
                        chamber=committee_chamber,
                    )
                    did_upsert = _upsert_committee_membership(
                        db,
                        bioguide_id=bioguide_id,
                        committee_code=committee_code,
                        role=role,
                    )
                    memberships_upserted += 1 if did_upsert else 0
                    memberships_seen += 1

                db.commit()
                pages += 1
                pagination = payload.get("pagination") or {}
                count = int(pagination.get("count") or 0)
                offset += len(memberships)
                if count and offset >= count:
                    break

                time.sleep(0.2)

    return {
        "pages": pages,
        "memberships_seen": memberships_seen,
        "memberships_upserted": memberships_upserted,
        "memberships_skipped": memberships_skipped,
    }


def _sync_cosponsorships(
    db,
    *,
    congresses: list[int],
    max_members: int = 50,
    page_limit: int = 250,
) -> dict[str, Any]:
    db.execute(
        text(
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
    )
    db.commit()

    leg_rows = db.execute(
        text(
            """
            SELECT bioguide_id
            FROM legislators
            WHERE COALESCE(is_active, TRUE) = TRUE
            ORDER BY id ASC
            LIMIT :limit
            """
        ),
        {"limit": max_members},
    ).fetchall()

    members = [(row.bioguide_id or "").strip() for row in leg_rows if row.bioguide_id]
    seen = 0
    inserted = 0

    for bioguide_id in members:
        for congress in congresses:
            payload = _fetch_congress_json(
                f"/member/{bioguide_id}/cosponsored-legislation",
                {"congress": int(congress), "limit": page_limit},
                allow_404=True,
            )
            if payload.get("_not_found"):
                continue

            bills = payload.get("bills") or payload.get("cosponsoredLegislation") or []
            for bill in bills:
                bill_id = (
                    bill.get("number")
                    or bill.get("billNumber")
                    or bill.get("url")
                    or ""
                ).strip()
                if not bill_id:
                    continue
                result = db.execute(
                    text(
                        """
                        INSERT INTO co_sponsorships (legislator_id, bill_id, bill_title, congress, introduced_date)
                        SELECT l.id, :bill_id, :bill_title, :congress, :introduced_date
                        FROM legislators l
                        WHERE l.bioguide_id = :bioguide_id
                        ON CONFLICT (legislator_id, bill_id) DO NOTHING
                        """
                    ),
                    {
                        "bioguide_id": bioguide_id,
                        "bill_id": bill_id,
                        "bill_title": bill.get("title"),
                        "congress": int(congress),
                        "introduced_date": bill.get("introducedDate"),
                    },
                )
                if (result.rowcount or 0) > 0:
                    inserted += 1
            seen += 1
            db.commit()
            time.sleep(0.15)

    return {"members_scanned": seen, "inserted": inserted}


def _sync_lda_enrichment(
    db,
    *,
    years: list[int],
    max_pages_per_year: int = 10,
    page_size: int = 200,
) -> dict[str, Any]:
    processed = 0
    foreign_updates = 0
    lobbyist_updates = 0

    for year in years:
        page = 1
        while page <= max_pages_per_year:
            payload = _fetch_lda_json(
                "/filings/",
                {"filing_year": int(year), "page_size": page_size, "page": page},
            )
            results = payload.get("results") or []
            if not results:
                break

            for filing in results:
                filing_uuid = (filing.get("filing_uuid") or "").strip()
                if not filing_uuid:
                    continue
                processed += 1

                registrant_name = ((filing.get("registrant") or {}).get("name") or "").strip() or None
                client_name = ((filing.get("client") or {}).get("name") or "").strip() or None
                registrant_norm = _normalize_name(registrant_name) if registrant_name else None
                client_norm = _normalize_name(client_name) if client_name else None

                registrant_id = None
                client_id = None
                if registrant_name and registrant_norm:
                    db.execute(
                        text(
                            """
                            INSERT INTO organizations (name, name_normalized, type)
                            VALUES (:name, :name_normalized, 'registrant')
                            ON CONFLICT (name_normalized) DO UPDATE
                            SET name = EXCLUDED.name
                            """
                        ),
                        {"name": registrant_name, "name_normalized": registrant_norm},
                    )
                    reg_row = db.execute(
                        text("SELECT id FROM organizations WHERE name_normalized = :name_normalized LIMIT 1"),
                        {"name_normalized": registrant_norm},
                    ).fetchone()
                    registrant_id = reg_row.id if reg_row else None

                if client_name and client_norm:
                    db.execute(
                        text(
                            """
                            INSERT INTO organizations (name, name_normalized, type)
                            VALUES (:name, :name_normalized, 'client')
                            ON CONFLICT (name_normalized) DO UPDATE
                            SET name = EXCLUDED.name
                            """
                        ),
                        {"name": client_name, "name_normalized": client_norm},
                    )
                    cli_row = db.execute(
                        text("SELECT id FROM organizations WHERE name_normalized = :name_normalized LIMIT 1"),
                        {"name_normalized": client_norm},
                    ).fetchone()
                    client_id = cli_row.id if cli_row else None

                activities = filing.get("lobbying_activities") or []
                general_issue_codes = sorted(
                    {str(a.get("general_issue_code")).strip().upper() for a in activities if a.get("general_issue_code")}
                )
                specific_issues = " | ".join(
                    [str(a.get("specific_issues")).strip() for a in activities if a.get("specific_issues")]
                ) or None

                foreign_entities = filing.get("foreign_entities") or []
                foreign_names = [item.get("name") for item in foreign_entities if item.get("name")]
                foreign_countries = sorted(
                    {
                        str(item.get("country")).strip().upper()
                        for item in foreign_entities
                        if item.get("country")
                    }
                )
                has_foreign = bool(foreign_names or foreign_countries)
                registration_id_row = db.execute(
                    text(
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
                          :filing_uuid,
                          :registrant_id,
                          :client_id,
                          :filing_year,
                          :filing_period,
                          :amount,
                          :general_issue_codes,
                          :specific_issues,
                          :has_foreign,
                          :foreign_names,
                          :foreign_countries
                        )
                        ON CONFLICT (filing_uuid) DO UPDATE
                        SET
                          registrant_id = COALESCE(EXCLUDED.registrant_id, lobbying_registrations.registrant_id),
                          client_id = COALESCE(EXCLUDED.client_id, lobbying_registrations.client_id),
                          filing_year = COALESCE(EXCLUDED.filing_year, lobbying_registrations.filing_year),
                          filing_period = COALESCE(EXCLUDED.filing_period, lobbying_registrations.filing_period),
                          amount = COALESCE(EXCLUDED.amount, lobbying_registrations.amount),
                          general_issue_codes = COALESCE(EXCLUDED.general_issue_codes, lobbying_registrations.general_issue_codes),
                          specific_issues = COALESCE(EXCLUDED.specific_issues, lobbying_registrations.specific_issues),
                          has_foreign_entity = EXCLUDED.has_foreign_entity,
                          foreign_entity_names = EXCLUDED.foreign_entity_names,
                          foreign_entity_countries = EXCLUDED.foreign_entity_countries
                        RETURNING id
                        """
                    ),
                    {
                        "filing_uuid": filing_uuid,
                        "registrant_id": registrant_id,
                        "client_id": client_id,
                        "filing_year": filing.get("filing_year"),
                        "filing_period": filing.get("filing_period"),
                        "amount": filing.get("income") or filing.get("expenses") or filing.get("amount") or 0,
                        "general_issue_codes": general_issue_codes or None,
                        "specific_issues": specific_issues,
                        "has_foreign": has_foreign,
                        "foreign_names": foreign_names or None,
                        "foreign_countries": foreign_countries or None,
                    },
                ).fetchone()
                registration_id = registration_id_row.id if registration_id_row else None
                foreign_updates += 1

                lobbyists_raw = list(filing.get("lobbyists") or [])
                for activity in activities:
                    lobbyists_raw.extend(activity.get("lobbyists") or [])

                for raw in lobbyists_raw:
                    lobbyist_id = (
                        raw.get("id")
                        or raw.get("lobbyist_id")
                        or ""
                    )
                    lobbyist_name = raw.get("lobbyist") or raw.get("name") or ""
                    if isinstance(lobbyist_name, dict):
                        lobbyist_name = lobbyist_name.get("name") or ""
                    lobbyist_name = str(lobbyist_name).strip()
                    normalized_name = _normalize_name(lobbyist_name)

                    covered = raw.get("covered_positions") or []
                    covered_positions = [
                        (item.get("position_held") if isinstance(item, dict) else str(item))
                        for item in covered
                        if item
                    ]
                    covered_positions = [c.strip() for c in covered_positions if c and c.strip()]
                    if not lobbyist_name:
                        continue

                    existing_row = db.execute(
                        text(
                            """
                            SELECT id
                            FROM lobbyists
                            WHERE
                              (lda_id IS NOT NULL AND lda_id = :lda_id)
                              OR (:name_normalized <> '' AND name_normalized = :name_normalized)
                            LIMIT 1
                            """
                        ),
                        {
                            "lda_id": str(lobbyist_id).strip() or None,
                            "name_normalized": normalized_name,
                        },
                    ).fetchone()

                    lobbyist_row_id = existing_row.id if existing_row else None
                    if lobbyist_row_id is None:
                        inserted_row = db.execute(
                            text(
                                """
                                INSERT INTO lobbyists (
                                  name,
                                  name_normalized,
                                  lda_id,
                                  has_covered_position,
                                  covered_positions
                                )
                                VALUES (
                                  :name,
                                  :name_normalized,
                                  :lda_id,
                                  :has_covered_position,
                                  :covered_positions
                                )
                                RETURNING id
                                """
                            ),
                            {
                                "name": lobbyist_name,
                                "name_normalized": normalized_name,
                                "lda_id": str(lobbyist_id).strip() or None,
                                "has_covered_position": bool(covered_positions),
                                "covered_positions": covered_positions,
                            },
                        ).fetchone()
                        lobbyist_row_id = inserted_row.id if inserted_row else None
                    elif covered_positions:
                        db.execute(
                            text(
                                """
                                UPDATE lobbyists
                                SET
                                  has_covered_position = TRUE,
                                  covered_positions = (
                                    SELECT ARRAY(
                                      SELECT DISTINCT pos
                                      FROM unnest(COALESCE(lobbyists.covered_positions, ARRAY[]::text[]) || :covered_positions::text[]) AS pos
                                      WHERE pos IS NOT NULL AND pos <> ''
                                    )
                                  )
                                WHERE id = :id
                                """
                            ),
                            {"id": lobbyist_row_id, "covered_positions": covered_positions},
                        )

                    if registration_id and lobbyist_row_id:
                        link_result = db.execute(
                            text(
                                """
                                INSERT INTO lobbying_lobbyists (registration_id, lobbyist_id)
                                VALUES (:registration_id, :lobbyist_id)
                                ON CONFLICT DO NOTHING
                                """
                            ),
                            {"registration_id": registration_id, "lobbyist_id": lobbyist_row_id},
                        )
                        lobbyist_updates += int(link_result.rowcount or 0)

                if processed % 200 == 0:
                    db.commit()

            db.commit()
            if not payload.get("next"):
                break
            page += 1
            time.sleep(0.05)

    return {
        "filings_processed": processed,
        "foreign_updates": foreign_updates,
        "lobbyist_updates": lobbyist_updates,
        "years": years,
        "max_pages_per_year": max_pages_per_year,
    }


def _run_scheduled_ingest(payload: dict[str, Any]) -> dict[str, Any]:
    congresses = payload.get("congresses") or [118, 119]
    chambers = payload.get("chambers") or ["senate", "house"]
    sync_cosponsors = bool(payload.get("sync_cosponsors", False))
    sync_lda = bool(payload.get("sync_lda", True))
    cosponsor_member_limit = int(payload.get("cosponsor_member_limit") or 50)
    max_members = payload.get("max_members")
    max_members = int(max_members) if max_members is not None else None
    lda_years = payload.get("lda_years") or [2024, 2025]
    lda_max_pages = int(payload.get("lda_max_pages_per_year") or 10)

    db = SessionLocal()
    try:
        _ensure_schema_compat(db)
        _ensure_pk_sequences(db)
        legislator_stats = _sync_legislators(db, max_members=max_members)
        committee_stats = _sync_committee_memberships(
            db,
            congresses=[int(x) for x in congresses],
            chambers=[str(x).lower() for x in chambers],
        )
        cosponsor_stats = None
        if sync_cosponsors:
            cosponsor_stats = _sync_cosponsorships(
                db,
                congresses=[int(x) for x in congresses],
                max_members=cosponsor_member_limit,
            )
        lda_stats = None
        if sync_lda:
            lda_stats = _sync_lda_enrichment(
                db,
                years=[int(y) for y in lda_years],
                max_pages_per_year=lda_max_pages,
            )
        _ensure_pipeline_meta_table(db)
        _set_pipeline_meta(db, "last_ingest_at", datetime.now(timezone.utc).replace(microsecond=0).isoformat())
        _set_pipeline_meta(db, "lda_coverage_through", _derive_lda_coverage_through(db))
        _set_pipeline_meta(db, "congress_coverage_through", _derive_congress_coverage_through(db))
        db.commit()
        _publish_export_task()
        return {
            "task": "scheduled_ingest",
            "status": "ok",
            "legislators": legislator_stats,
            "committee_memberships": committee_stats,
            "co_sponsorships": cosponsor_stats,
            "lda_enrichment": lda_stats,
        }
    finally:
        db.close()


def _handle_task(task: str, payload: dict[str, Any]) -> dict[str, Any]:
    if task == "analyze":
        _run_analyze()
        return {"task": task, "status": "ok"}

    if task == "scheduled_ingest":
        return _run_scheduled_ingest(payload)

    return {"task": task or "unknown", "status": "ignored"}


def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    logger.info("worker event: %s", json.dumps(event)[:4000])
    responses: list[dict[str, Any]] = []

    records = event.get("Records") or []
    if records:
        for record in records:
            body = record.get("body") or "{}"
            try:
                payload = json.loads(body)
            except Exception:
                payload = {"raw": body}
            task = str(payload.get("task") or "").strip()
            responses.append(_handle_task(task, payload))
        return {"results": responses}

    task = str((event or {}).get("task") or "scheduled_ingest").strip()
    responses.append(_handle_task(task, event or {}))
    return {"results": responses, "env": os.getenv("LOBBYWATCH_ENV", "unknown")}
