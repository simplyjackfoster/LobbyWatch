#!/usr/bin/env python3
"""
Export LobbyWatch PostgreSQL database to SQLite for CLI bundling.

Usage:
    DATABASE_URL=postgresql://... python cli/scripts/export_sqlite.py [--years N] [--top-orgs N]

Outputs:
    lobbywatch.db.zst          - Full snapshot (upload to GitHub Releases)
    lobbywatch_bundled.db.zst  - Bundled snapshot for pip wheel (if full > 60MB)

Copy lobbywatch_bundled.db.zst -> cli/lobbywatch/data/lobbywatch.db.zst before pip publish.
"""

import argparse
import json
import os
import sqlite3
import time

import zstandard

PIP_SIZE_LIMIT = 60 * 1024 * 1024  # 60 MB


def get_pg_conn():
    try:
        import psycopg

        return psycopg.connect(os.environ["DATABASE_URL"])
    except ImportError:
        import psycopg2

        return psycopg2.connect(os.environ["DATABASE_URL"])


SQLITE_SCHEMA = """
CREATE TABLE IF NOT EXISTS _meta (key TEXT PRIMARY KEY, value TEXT);

CREATE TABLE IF NOT EXISTS organizations (
    id INTEGER PRIMARY KEY,
    name TEXT,
    name_normalized TEXT,
    type TEXT,
    industry_code TEXT
);

CREATE TABLE IF NOT EXISTS legislators (
    id INTEGER PRIMARY KEY,
    bioguide_id TEXT UNIQUE,
    name TEXT,
    party TEXT,
    state TEXT,
    chamber TEXT,
    is_active INTEGER
);

CREATE TABLE IF NOT EXISTS committees (
    id INTEGER PRIMARY KEY,
    committee_id TEXT UNIQUE,
    name TEXT,
    chamber TEXT,
    subcommittee_of TEXT
);

CREATE TABLE IF NOT EXISTS lobbying_registrations (
    id INTEGER PRIMARY KEY,
    registrant_id INTEGER,
    client_id INTEGER,
    filing_uuid TEXT,
    filing_year INTEGER,
    filing_period TEXT,
    amount REAL,
    issue_codes TEXT DEFAULT '[]',
    general_issue_codes TEXT DEFAULT '[]',
    specific_issues TEXT,
    has_foreign_entity INTEGER DEFAULT 0,
    foreign_entity_names TEXT DEFAULT '[]',
    foreign_entity_countries TEXT DEFAULT '[]'
);

CREATE VIRTUAL TABLE IF NOT EXISTS issues_fts USING fts5(
    registration_id UNINDEXED,
    specific_issues
);

CREATE TABLE IF NOT EXISTS contributions (
    id INTEGER PRIMARY KEY,
    contributor_org_id INTEGER,
    recipient_legislator_id INTEGER,
    amount REAL,
    contribution_date TEXT,
    fec_committee_id TEXT,
    cycle INTEGER
);

CREATE TABLE IF NOT EXISTS committee_memberships (
    legislator_id INTEGER,
    committee_id INTEGER,
    role TEXT,
    PRIMARY KEY (legislator_id, committee_id)
);

CREATE TABLE IF NOT EXISTS votes (
    id INTEGER PRIMARY KEY,
    legislator_id INTEGER,
    bill_id TEXT,
    bill_title TEXT,
    vote_position TEXT,
    vote_date TEXT,
    congress INTEGER,
    issue_tags TEXT DEFAULT '[]'
);

CREATE TABLE IF NOT EXISTS lobbyists (
    id INTEGER PRIMARY KEY,
    name TEXT,
    name_normalized TEXT,
    lda_id TEXT,
    covered_positions TEXT DEFAULT '[]',
    has_covered_position INTEGER DEFAULT 0,
    conviction_disclosure TEXT,
    has_conviction INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS lobbying_lobbyists (
    registration_id INTEGER,
    lobbyist_id INTEGER,
    PRIMARY KEY (registration_id, lobbyist_id)
);

CREATE TABLE IF NOT EXISTS co_sponsorships (
    id INTEGER PRIMARY KEY,
    legislator_id INTEGER,
    bill_id TEXT,
    bill_title TEXT,
    congress INTEGER,
    introduced_date TEXT
);
"""


def to_json(value):
    if value is None:
        return "[]"
    if isinstance(value, list):
        return json.dumps(value)
    return json.dumps(list(value))


def copy_table(pg_cur, sq_conn, pg_query, sq_table, transform):
    pg_cur.execute(pg_query)
    rows = pg_cur.fetchmany(500)
    count = 0
    while rows:
        sq_conn.executemany(
            f"INSERT OR IGNORE INTO {sq_table} VALUES ({','.join('?' for _ in rows[0])})",
            [transform(row) for row in rows],
        )
        count += len(rows)
        rows = pg_cur.fetchmany(500)
    sq_conn.commit()
    print(f"  {sq_table}: {count} rows")
    return count


def build_db(sq_path: str, pg, year_filter: str = "", org_ids: set = None) -> None:
    sq = sqlite3.connect(sq_path)
    sq.executescript(SQLITE_SCHEMA)

    sq.execute(
        "INSERT OR REPLACE INTO _meta VALUES ('exported_at', ?)",
        (time.strftime("%Y-%m-%dT%H:%M:%S"),),
    )
    sq.execute("INSERT OR REPLACE INTO _meta VALUES ('schema_version', '1')")
    sq.commit()

    cur = pg.cursor()

    copy_table(
        cur,
        sq,
        "SELECT id, name, name_normalized, type, industry_code FROM organizations",
        "organizations",
        lambda row: (row[0], row[1], row[2], row[3], row[4]),
    )

    copy_table(
        cur,
        sq,
        "SELECT id, bioguide_id, name, party, state, chamber, is_active::int FROM legislators",
        "legislators",
        lambda row: tuple(row),
    )

    copy_table(
        cur,
        sq,
        "SELECT id, committee_id, name, chamber, subcommittee_of FROM committees",
        "committees",
        lambda row: tuple(row),
    )

    reg_where = year_filter
    if org_ids:
        org_list = ",".join(str(org_id) for org_id in org_ids)
        reg_where += f" AND (client_id IN ({org_list}) OR registrant_id IN ({org_list}))"

    copy_table(
        cur,
        sq,
        f"SELECT id, registrant_id, client_id, filing_uuid, filing_year, filing_period, "
        f"amount, issue_codes, general_issue_codes, specific_issues, "
        f"has_foreign_entity::int, foreign_entity_names, foreign_entity_countries "
        f"FROM lobbying_registrations {reg_where}",
        "lobbying_registrations",
        lambda row: (
            row[0],
            row[1],
            row[2],
            row[3],
            row[4],
            row[5],
            row[6] and float(row[6]),
            to_json(row[7]),
            to_json(row[8]),
            row[9],
            int(row[10] or 0),
            to_json(row[11]),
            to_json(row[12]),
        ),
    )

    sq.execute(
        "INSERT INTO issues_fts(registration_id, specific_issues) "
        "SELECT id, specific_issues FROM lobbying_registrations "
        "WHERE specific_issues IS NOT NULL"
    )
    sq.commit()

    copy_table(
        cur,
        sq,
        "SELECT id, contributor_org_id, recipient_legislator_id, amount, "
        "contribution_date::text, fec_committee_id, cycle FROM contributions",
        "contributions",
        lambda row: (row[0], row[1], row[2], row[3] and float(row[3]), row[4], row[5], row[6]),
    )

    copy_table(
        cur,
        sq,
        "SELECT legislator_id, committee_id, role FROM committee_memberships",
        "committee_memberships",
        lambda row: tuple(row),
    )

    copy_table(
        cur,
        sq,
        "SELECT id, legislator_id, bill_id, bill_title, vote_position, "
        "vote_date::text, congress, issue_tags FROM votes",
        "votes",
        lambda row: (row[0], row[1], row[2], row[3], row[4], row[5], row[6], to_json(row[7])),
    )

    copy_table(
        cur,
        sq,
        "SELECT id, name, name_normalized, lda_id, covered_positions, "
        "has_covered_position::int, conviction_disclosure, has_conviction::int FROM lobbyists",
        "lobbyists",
        lambda row: (
            row[0],
            row[1],
            row[2],
            row[3],
            to_json(row[4]),
            int(row[5] or 0),
            row[6],
            int(row[7] or 0),
        ),
    )

    copy_table(
        cur,
        sq,
        "SELECT registration_id, lobbyist_id FROM lobbying_lobbyists",
        "lobbying_lobbyists",
        lambda row: tuple(row),
    )

    copy_table(
        cur,
        sq,
        "SELECT id, legislator_id, bill_id, bill_title, congress, introduced_date::text "
        "FROM co_sponsorships",
        "co_sponsorships",
        lambda row: tuple(row),
    )

    sq.execute("CREATE INDEX IF NOT EXISTS idx_reg_client ON lobbying_registrations(client_id)")
    sq.execute("CREATE INDEX IF NOT EXISTS idx_reg_registrant ON lobbying_registrations(registrant_id)")
    sq.execute("CREATE INDEX IF NOT EXISTS idx_contrib_org ON contributions(contributor_org_id)")
    sq.execute("CREATE INDEX IF NOT EXISTS idx_contrib_leg ON contributions(recipient_legislator_id)")
    sq.commit()
    sq.close()
    cur.close()


def compress(src: str, dst: str, level: int = 22) -> int:
    cctx = zstandard.ZstdCompressor(level=level)
    with open(src, "rb") as f_in, open(dst, "wb") as f_out:
        cctx.copy_stream(f_in, f_out)
    return os.path.getsize(dst)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--years",
        type=int,
        default=None,
        help="Only export data from the last N years",
    )
    parser.add_argument(
        "--top-orgs",
        type=int,
        default=None,
        help="Only export top N orgs by spend (for bundled subset)",
    )
    args = parser.parse_args()

    pg = get_pg_conn()
    year_filter = ""
    if args.years:
        cutoff = time.gmtime().tm_year - args.years
        year_filter = f"WHERE filing_year >= {cutoff}"

    print("Building full SQLite export...")
    build_db("lobbywatch.db", pg, year_filter)
    full_size_bytes = os.path.getsize("lobbywatch.db")
    print(f"Raw SQLite: {full_size_bytes / 1e6:.1f} MB")

    print("Compressing (level 22)...")
    compressed_size = compress("lobbywatch.db", "lobbywatch.db.zst")
    print(f"Compressed: {compressed_size / 1e6:.1f} MB -> lobbywatch.db.zst")

    if compressed_size > PIP_SIZE_LIMIT:
        print(f"\nWARNING: {compressed_size / 1e6:.1f} MB exceeds PyPI 60 MB limit.")
        print("Building curated bundled subset (recent 2 years)...")
        cutoff2 = time.gmtime().tm_year - 2
        build_db("lobbywatch_bundled.db", pg, f"WHERE filing_year >= {cutoff2}")
        bundled_size = compress("lobbywatch_bundled.db", "lobbywatch_bundled.db.zst")
        print(f"Bundled compressed: {bundled_size / 1e6:.1f} MB -> lobbywatch_bundled.db.zst")
        if bundled_size > PIP_SIZE_LIMIT:
            print("  Still too large. Consider --years 1 or --top-orgs 1000.")
        else:
            print("  OK for PyPI. Copy to cli/lobbywatch/data/lobbywatch.db.zst before publish.")
    else:
        print("\nFull snapshot fits in PyPI limit. Copy to cli/lobbywatch/data/lobbywatch.db.zst")
        import shutil

        shutil.copy("lobbywatch.db.zst", "lobbywatch_bundled.db.zst")

    print("\n--- Next steps ---")
    print("1. Upload lobbywatch.db.zst to a new GitHub Release")
    print("2. cp lobbywatch_bundled.db.zst cli/lobbywatch/data/lobbywatch.db.zst")
    print("3. Bump version in cli/pyproject.toml")
    print("4. cd cli && python -m build && twine upload dist/*")

    pg.close()
    os.unlink("lobbywatch.db")
    try:
        os.unlink("lobbywatch_bundled.db")
    except FileNotFoundError:
        pass


if __name__ == "__main__":
    main()
