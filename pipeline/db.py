import os
import re
from typing import Optional

from dotenv import load_dotenv
import psycopg
from sqlalchemy import MetaData, Table, create_engine, select, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import sessionmaker

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+psycopg://lobbying:lobbying@localhost:5432/lobbying")

engine = create_engine(
    DATABASE_URL,
    pool_size=10,
    max_overflow=20,
    pool_pre_ping=True,
)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)

metadata = MetaData()
organizations = Table("organizations", metadata, autoload_with=engine)
lobbyists = Table("lobbyists", metadata, autoload_with=engine)
lobbying_registrations = Table("lobbying_registrations", metadata, autoload_with=engine)
lobbying_lobbyists = Table("lobbying_lobbyists", metadata, autoload_with=engine)
legislators = Table("legislators", metadata, autoload_with=engine)
committees = Table("committees", metadata, autoload_with=engine)
committee_memberships = Table("committee_memberships", metadata, autoload_with=engine)
contributions = Table("contributions", metadata, autoload_with=engine)
votes = Table("votes", metadata, autoload_with=engine)


def normalize_name(name: str) -> str:
    if not name:
        return ""
    name = name.upper().strip()
    name = re.sub(r"[^\w\s]", "", name)
    name = re.sub(r"\s+", " ", name).strip()
    pac_suffixes = [
        " POLITICAL ACTION COMMITTEE",
        " POLITICAL ACTION CMTE",
        " PAC",
        " COMMITTEE",
        " CMTE",
        " FUND",
    ]
    for suffix in pac_suffixes:
        if name.endswith(suffix):
            name = name[: -len(suffix)].strip()
    legal_suffixes = [
        " INCORPORATED",
        " INC",
        " LLC",
        " LTD",
        " LIMITED",
        " CORPORATION",
        " CORP",
        " COMPANY",
        " CO",
        " LP",
        " LLP",
    ]
    for suffix in legal_suffixes:
        if name.endswith(suffix):
            name = name[: -len(suffix)].strip()
    name = re.sub(r"\s+", " ", name).strip()
    return name


def upsert_organization(db, name: str, org_type: Optional[str] = None, industry_code: Optional[str] = None) -> Optional[int]:
    if not name:
        return None
    normalized = normalize_name(name)
    existing = db.execute(
        select(organizations.c.id).where(organizations.c.name_normalized == normalized)
    ).scalar_one_or_none()
    if existing:
        return existing

    stmt = (
        insert(organizations)
        .values(name=name, name_normalized=normalized, type=org_type, industry_code=industry_code)
        .on_conflict_do_nothing(index_elements=[organizations.c.name_normalized])
        .returning(organizations.c.id)
    )
    result = db.execute(stmt).scalar_one_or_none()
    if result is not None:
        return result
    return db.execute(
        select(organizations.c.id).where(organizations.c.name_normalized == normalized)
    ).scalar_one_or_none()


def upsert_lobbyist(db, name: str, lda_id: Optional[str] = None) -> Optional[int]:
    if not name:
        return None
    normalized = normalize_name(name)
    if lda_id:
        stmt = (
            insert(lobbyists)
            .values(name=name, name_normalized=normalized, lda_id=lda_id)
            .on_conflict_do_update(index_elements=[lobbyists.c.lda_id], set_={"name": name, "name_normalized": normalized})
            .returning(lobbyists.c.id)
        )
        return db.execute(stmt).scalar_one_or_none()

    existing = db.execute(
        select(lobbyists.c.id).where(lobbyists.c.name_normalized == normalized)
    ).scalar_one_or_none()
    if existing:
        return existing

    stmt = insert(lobbyists).values(name=name, name_normalized=normalized).returning(lobbyists.c.id)
    return db.execute(stmt).scalar_one_or_none()


def upsert_legislator(db, bioguide_id: str, name: str, party: Optional[str], state: Optional[str], chamber: Optional[str], is_active: bool = True) -> Optional[int]:
    if not bioguide_id:
        return None
    stmt = (
        insert(legislators)
        .values(
            bioguide_id=bioguide_id,
            name=name,
            party=party,
            state=state,
            chamber=chamber,
            is_active=is_active,
        )
        .on_conflict_do_update(
            index_elements=[legislators.c.bioguide_id],
            set_={
                "name": name,
                "party": party,
                "state": state,
                "chamber": chamber,
                "is_active": is_active,
            },
        )
        .returning(legislators.c.id)
    )
    return db.execute(stmt).scalar_one_or_none()


def upsert_committee(db, committee_id: str, name: str, chamber: Optional[str], subcommittee_of: Optional[str] = None) -> Optional[int]:
    if not committee_id:
        return None
    stmt = (
        insert(committees)
        .values(
            committee_id=committee_id,
            name=name,
            chamber=chamber,
            subcommittee_of=subcommittee_of,
        )
        .on_conflict_do_update(
            index_elements=[committees.c.committee_id],
            set_={
                "name": name,
                "chamber": chamber,
                "subcommittee_of": subcommittee_of,
            },
        )
        .returning(committees.c.id)
    )
    return db.execute(stmt).scalar_one_or_none()


def get_resume_page(db, source: str) -> int:
    latest = get_latest_ingestion_run(db, source)
    if latest and latest.status == "running" and latest.last_page is not None:
        return int(latest.last_page) + 1
    return 1


def get_latest_ingestion_run(db, source: str):
    row = db.execute(
        text(
            """
            SELECT id, source, status, last_page, records_processed
            FROM ingestion_runs
            WHERE source = :source
              AND status IN ('running', 'complete')
            ORDER BY started_at DESC NULLS LAST, id DESC
            LIMIT 1
            """
        ),
        {"source": source},
    ).first()
    return row


def start_ingestion_run(db, source: str, last_page: Optional[int] = 0):
    run_id = db.execute(
        text(
            """
            INSERT INTO ingestion_runs (source, started_at, status, last_page, records_processed)
            VALUES (:source, NOW(), 'running', :last_page, 0)
            RETURNING id
            """
        ),
        {"source": source, "last_page": last_page or 0},
    ).scalar_one()
    db.commit()
    return run_id


def update_ingestion_run_progress(
    db,
    run_id: int,
    last_page: int,
    records_processed: int,
    last_filing_uuid: Optional[str] = None,
):
    db.execute(
        text(
            """
            UPDATE ingestion_runs
            SET last_page = :last_page,
                records_processed = :records_processed,
                last_filing_uuid = COALESCE(:last_filing_uuid, last_filing_uuid)
            WHERE id = :run_id
            """
        ),
        {
            "run_id": run_id,
            "last_page": last_page,
            "records_processed": records_processed,
            "last_filing_uuid": last_filing_uuid,
        },
    )
    db.commit()


def complete_ingestion_run(db, run_id: int):
    db.execute(
        text(
            """
            UPDATE ingestion_runs
            SET status = 'complete',
                completed_at = NOW()
            WHERE id = :run_id
            """
        ),
        {"run_id": run_id},
    )
    db.commit()


def fail_ingestion_run(db, run_id: int):
    db.execute(
        text(
            """
            UPDATE ingestion_runs
            SET status = 'failed',
                completed_at = NOW()
            WHERE id = :run_id
            """
        ),
        {"run_id": run_id},
    )
    db.commit()


def raw_database_url() -> str:
    return DATABASE_URL.replace("postgresql+psycopg://", "postgresql://")


async def connect_async():
    return await psycopg.AsyncConnection.connect(raw_database_url(), autocommit=False)


async def optimize_for_bulk_load(conn):
    await conn.execute("SET synchronous_commit = OFF")
    await conn.execute("SET work_mem = '256MB'")
    await conn.execute("SET maintenance_work_mem = '512MB'")


async def drop_indexes_for_bulk_load(conn):
    indexes = [
        "idx_specific_issues_fts",
        "idx_general_issue_codes",
        "idx_org_name_normalized",
        "idx_contributions_contributor",
        "idx_contributions_recipient",
        "idx_lobbying_client",
        "idx_lobbying_registrant",
    ]
    for idx in indexes:
        await conn.execute(f"DROP INDEX IF EXISTS {idx}")


async def rebuild_indexes(conn):
    await conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_org_name_normalized
        ON organizations(name_normalized);

        CREATE INDEX IF NOT EXISTS idx_lobbying_client
        ON lobbying_registrations(client_id);

        CREATE INDEX IF NOT EXISTS idx_lobbying_registrant
        ON lobbying_registrations(registrant_id);

        CREATE INDEX IF NOT EXISTS idx_contributions_contributor
        ON contributions(contributor_org_id);

        CREATE INDEX IF NOT EXISTS idx_contributions_recipient
        ON contributions(recipient_legislator_id);
        """
    )
    await conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_general_issue_codes
        ON lobbying_registrations USING GIN (general_issue_codes);
        """
    )
    await conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_specific_issues_fts
        ON lobbying_registrations USING GIN (specific_issues_tsv);
        """
    )


async def apply_enhancement_migrations(conn):
    async with conn.transaction():
        await conn.execute(
            """
            ALTER TABLE lobbyists
              ADD COLUMN IF NOT EXISTS covered_positions TEXT[],
              ADD COLUMN IF NOT EXISTS has_covered_position BOOLEAN DEFAULT FALSE,
              ADD COLUMN IF NOT EXISTS conviction_disclosure TEXT,
              ADD COLUMN IF NOT EXISTS has_conviction BOOLEAN DEFAULT FALSE;
            """
        )
        await conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_lobbyists_covered_position
              ON lobbyists(has_covered_position)
              WHERE has_covered_position = TRUE;
            """
        )

        await conn.execute(
            """
            ALTER TABLE lobbying_registrations
              ADD COLUMN IF NOT EXISTS has_foreign_entity BOOLEAN DEFAULT FALSE,
              ADD COLUMN IF NOT EXISTS foreign_entity_names TEXT[],
              ADD COLUMN IF NOT EXISTS foreign_entity_countries TEXT[];
            """
        )
        await conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_foreign_entity_filings
              ON lobbying_registrations(has_foreign_entity)
              WHERE has_foreign_entity = TRUE;
            """
        )

        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS lobbyist_contributions (
              id SERIAL PRIMARY KEY,
              filing_uuid TEXT UNIQUE NOT NULL,
              lobbyist_id INTEGER REFERENCES lobbyists(id),
              registrant_id INTEGER REFERENCES organizations(id),
              filing_year INTEGER,
              filing_period TEXT,
              contribution_items JSONB,
              pacs TEXT[],
              dt_posted DATE
            );
            """
        )
        await conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_lobbyist_contributions_lobbyist
              ON lobbyist_contributions(lobbyist_id);
            """
        )
        await conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_lobbyist_contributions_year
              ON lobbyist_contributions(filing_year);
            """
        )


async def apply_migrations(conn):
    await apply_enhancement_migrations(conn)
