# cli/tests/conftest.py
import json
import sqlite3
import pytest

SCHEMA = """
CREATE TABLE _meta (key TEXT PRIMARY KEY, value TEXT);

CREATE TABLE organizations (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    name_normalized TEXT,
    type TEXT,
    industry_code TEXT
);

CREATE TABLE legislators (
    id INTEGER PRIMARY KEY,
    bioguide_id TEXT UNIQUE NOT NULL,
    name TEXT NOT NULL,
    party TEXT,
    state TEXT,
    chamber TEXT,
    is_active INTEGER DEFAULT 1
);

CREATE TABLE committees (
    id INTEGER PRIMARY KEY,
    committee_id TEXT UNIQUE,
    name TEXT,
    chamber TEXT,
    subcommittee_of TEXT
);

CREATE TABLE lobbying_registrations (
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

CREATE VIRTUAL TABLE issues_fts USING fts5(
    registration_id UNINDEXED,
    specific_issues
);

CREATE TABLE contributions (
    id INTEGER PRIMARY KEY,
    contributor_org_id INTEGER,
    recipient_legislator_id INTEGER,
    amount REAL,
    contribution_date TEXT,
    fec_committee_id TEXT,
    cycle INTEGER
);

CREATE TABLE committee_memberships (
    legislator_id INTEGER,
    committee_id INTEGER,
    role TEXT,
    PRIMARY KEY (legislator_id, committee_id)
);

CREATE TABLE votes (
    id INTEGER PRIMARY KEY,
    legislator_id INTEGER,
    bill_id TEXT,
    bill_title TEXT,
    vote_position TEXT,
    vote_date TEXT,
    congress INTEGER,
    issue_tags TEXT DEFAULT '[]'
);

CREATE TABLE lobbyists (
    id INTEGER PRIMARY KEY,
    name TEXT,
    name_normalized TEXT,
    lda_id TEXT,
    covered_positions TEXT DEFAULT '[]',
    has_covered_position INTEGER DEFAULT 0,
    conviction_disclosure TEXT,
    has_conviction INTEGER DEFAULT 0
);

CREATE TABLE lobbying_lobbyists (
    registration_id INTEGER,
    lobbyist_id INTEGER,
    PRIMARY KEY (registration_id, lobbyist_id)
);

CREATE TABLE co_sponsorships (
    id INTEGER PRIMARY KEY,
    legislator_id INTEGER,
    bill_id TEXT,
    bill_title TEXT,
    congress INTEGER,
    introduced_date TEXT
);
"""


def seed(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA)
    conn.executemany("INSERT INTO _meta VALUES (?,?)", [
        ("exported_at", "2026-04-22T00:00:00"),
        ("schema_version", "1"),
    ])
    conn.executemany("INSERT INTO organizations VALUES (?,?,?,?,?)", [
        (1, "Pfizer Inc.", "pfizer inc", "registrant", "PHR"),
        (2, "Microsoft Corp.", "microsoft corp", "client", "TEC"),
        (3, "PhRMA", "phrma", "registrant", "PHR"),
    ])
    # legislators: id=1 bioguide=A000001, id=2 bioguide=B000002
    conn.executemany("INSERT INTO legislators VALUES (?,?,?,?,?,?,?)", [
        (1, "A000001", "Jane Smith", "D", "CA", "senate", 1),
        (2, "B000002", "John Doe", "R", "TX", "house", 1),
    ])
    conn.executemany("INSERT INTO committees VALUES (?,?,?,?,?)", [
        (1, "SSHR", "Senate Health Committee", "senate", None),
        (2, "HCOM", "House Commerce Committee", "house", None),
    ])
    conn.executemany("INSERT INTO committee_memberships VALUES (?,?,?)", [
        (1, 1, "member"),   # legislator.id=1, committee.id=1
        (2, 2, "chair"),    # legislator.id=2, committee.id=2
    ])
    conn.executemany(
        "INSERT INTO lobbying_registrations VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
        [
            (1, 1, 1, "uuid-001", 2024, "H1", 500000.0,
             json.dumps(["HLTH"]), json.dumps(["HLTH"]),
             "healthcare reform pharmaceutical pricing", 0,
             json.dumps([]), json.dumps([])),
            (2, 3, 2, "uuid-002", 2023, "H2", 250000.0,
             json.dumps(["TECH", "HLTH"]), json.dumps(["TECH", "HLTH"]),
             "technology health data sharing", 1,
             json.dumps(["PharmaCo Ltd."]), json.dumps(["UK"])),
        ]
    )
    conn.execute("INSERT INTO issues_fts VALUES (1, 'healthcare reform pharmaceutical pricing')")
    conn.execute("INSERT INTO issues_fts VALUES (2, 'technology health data sharing')")
    # contributions: org->legislator (using legislator.id integer)
    conn.executemany("INSERT INTO contributions VALUES (?,?,?,?,?,?,?)", [
        (1, 1, 1, 25000.0, "2024-03-15", "C00001", 2024),
        (2, 2, 2, 15000.0, "2023-06-20", "C00002", 2024),
    ])
    conn.executemany("INSERT INTO lobbyists VALUES (?,?,?,?,?,?,?,?)", [
        (1, "Bob Lobbyist", "bob lobbyist", "lda-001",
         json.dumps(["HHS Deputy Secretary 2015-2018"]), 1, None, 0),
    ])
    conn.execute("INSERT INTO lobbying_lobbyists VALUES (1, 1)")
    # votes: legislator.id=1 voted NAY on a bill they co-sponsored
    conn.execute(
        "INSERT INTO votes VALUES (?,?,?,?,?,?,?,?)",
        (1, 1, "hr-1234", "Healthcare Reform Act", "Nay", "2024-05-01", 118,
         json.dumps(["HLTH"]))
    )
    conn.execute(
        "INSERT INTO co_sponsorships VALUES (?,?,?,?,?,?)",
        (1, 1, "hr-1234", "Healthcare Reform Act", 118, "2024-01-15")
    )
    conn.commit()


@pytest.fixture
def db_path(tmp_path):
    db = tmp_path / "lobbywatch.db"
    conn = sqlite3.connect(str(db))
    seed(conn)
    conn.close()
    return str(db)
