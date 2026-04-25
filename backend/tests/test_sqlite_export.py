"""Tests for post_export_validator and sqlite_export.build_db."""

import sqlite3
import sys
from pathlib import Path

import pytest

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from post_export_validator import ValidationError, validate_export


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_minimal_db(
    tmp_path,
    *,
    lobbyist_rows=1,
    lobbying_lobbyist_rows=1,
    registration_rows=1,
    issue_codes_populated=True,
    specific_issues_populated=True,
    contribution_cycles=3,
    inactive_legislators=1,
    contributions_rows=2,
) -> str:
    """Build a minimal SQLite DB for validation tests."""
    db_path = str(tmp_path / "test.db")
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        CREATE TABLE _meta (key TEXT PRIMARY KEY, value TEXT);
        CREATE TABLE lobbyists (id INTEGER PRIMARY KEY, name TEXT);
        CREATE TABLE lobbying_lobbyists (registration_id INTEGER, lobbyist_id INTEGER,
                                         PRIMARY KEY (registration_id, lobbyist_id));
        CREATE TABLE lobbying_registrations (
            id INTEGER PRIMARY KEY, issue_codes TEXT DEFAULT '[]',
            specific_issues TEXT DEFAULT ''
        );
        CREATE TABLE legislators (id INTEGER PRIMARY KEY, is_active INTEGER DEFAULT 1);
        CREATE TABLE contributions (
            id INTEGER PRIMARY KEY, contributor_org_id INTEGER,
            recipient_legislator_id INTEGER, amount REAL,
            contribution_date TEXT, fec_committee_id TEXT, cycle INTEGER
        );
    """
    )

    for i in range(lobbyist_rows):
        conn.execute("INSERT INTO lobbyists VALUES (?, ?)", (i + 1, f"Lobbyist {i}"))
    for i in range(lobbying_lobbyist_rows):
        conn.execute("INSERT INTO lobbying_lobbyists VALUES (?, ?)", (i + 1, i + 1))

    issue = '["HLTH"]' if issue_codes_populated else '[]'
    si = "some text" if specific_issues_populated else ""
    for i in range(registration_rows):
        conn.execute("INSERT INTO lobbying_registrations VALUES (?, ?, ?)", (i + 1, issue, si))

    for i in range(inactive_legislators):
        conn.execute("INSERT INTO legislators VALUES (?, 0)", (i + 100,))
    conn.execute("INSERT INTO legislators VALUES (1, 1)")

    cycles = [2020, 2022, 2024][:contribution_cycles]
    for i, cycle in enumerate(cycles):
        for j in range(contributions_rows):
            conn.execute(
                "INSERT INTO contributions VALUES (?,?,?,?,?,?,?)",
                (i * 10 + j + 1, j + 1, 1, 1000.0, "2024-01-01", f"C{j:05}", cycle),
            )

    conn.commit()
    conn.close()
    return db_path


# ---------------------------------------------------------------------------
# validate_export tests
# ---------------------------------------------------------------------------

def test_validate_passes_on_healthy_db(tmp_path):
    db_path = _make_minimal_db(tmp_path)
    validate_export(db_path)


def test_validate_fails_when_lobbyists_empty(tmp_path):
    db_path = _make_minimal_db(tmp_path, lobbyist_rows=0, lobbying_lobbyist_rows=0)
    with pytest.raises(ValidationError, match="lobbyists table is empty"):
        validate_export(db_path)


def test_validate_fails_when_lobbying_lobbyists_empty(tmp_path):
    db_path = _make_minimal_db(tmp_path, lobbying_lobbyist_rows=0)
    with pytest.raises(ValidationError, match="lobbying_lobbyists table is empty"):
        validate_export(db_path)


def test_validate_fails_when_issue_codes_all_empty(tmp_path):
    db_path = _make_minimal_db(tmp_path, issue_codes_populated=False)
    with pytest.raises(ValidationError, match="issue_codes"):
        validate_export(db_path)


def test_validate_fails_when_specific_issues_all_empty(tmp_path):
    db_path = _make_minimal_db(tmp_path, specific_issues_populated=False)
    with pytest.raises(ValidationError, match="specific_issues"):
        validate_export(db_path)


def test_validate_warns_on_few_contribution_cycles(tmp_path, capsys):
    db_path = _make_minimal_db(tmp_path, contribution_cycles=1)
    validate_export(db_path)
    captured = capsys.readouterr()
    assert "election cycle" in captured.out.lower()


def test_validate_fails_when_no_inactive_legislators(tmp_path):
    db_path = _make_minimal_db(tmp_path, inactive_legislators=0)
    with pytest.raises(ValidationError, match="inactive legislators"):
        validate_export(db_path)


def test_build_and_compress_raises_on_bad_export(tmp_path, monkeypatch):
    """build_and_compress must raise ValidationError when the export is degraded."""

    def _bad_build_db(sq_path, pg, **kwargs):
        conn = sqlite3.connect(sq_path)
        conn.executescript(
            """
            CREATE TABLE _meta (key TEXT PRIMARY KEY, value TEXT);
            CREATE TABLE lobbyists (id INTEGER PRIMARY KEY, name TEXT);
            CREATE TABLE lobbying_lobbyists (registration_id INTEGER, lobbyist_id INTEGER,
                                             PRIMARY KEY(registration_id, lobbyist_id));
            CREATE TABLE lobbying_registrations (
                id INTEGER PRIMARY KEY, issue_codes TEXT DEFAULT '[]',
                specific_issues TEXT DEFAULT ''
            );
            CREATE TABLE legislators (id INTEGER PRIMARY KEY, is_active INTEGER);
            CREATE TABLE contributions (id INTEGER PRIMARY KEY, cycle INTEGER);
        """
        )
        conn.execute("INSERT INTO lobbying_registrations VALUES (1, '[]', '')")
        conn.execute("INSERT INTO legislators VALUES (1, 1)")
        conn.commit()
        conn.close()

    import sqlite_export as se

    monkeypatch.setattr(se, "build_db", _bad_build_db)

    output_path = str(tmp_path / "out.db.zst")
    with pytest.raises(ValidationError, match="lobbyists table is empty"):
        se.build_and_compress(None, output_path)


def test_dedup_contributions_query_selects_distinct_combos():
    """Verify the dedup SQL logic keeps only one row per logical duplicate."""
    conn = sqlite3.connect(":memory:")
    conn.executescript(
        """
        CREATE TABLE contributions (
            id INTEGER PRIMARY KEY,
            contributor_org_id INTEGER,
            recipient_legislator_id INTEGER,
            amount REAL,
            contribution_date TEXT,
            fec_committee_id TEXT,
            cycle INTEGER
        );
    """
    )

    conn.executemany(
        "INSERT INTO contributions VALUES (?,?,?,?,?,?,?)",
        [
            (1, 10, 20, 500.0, "2024-01-01", "C00001", 2024),
            (2, 10, 20, 500.0, "2024-01-01", "C00001", 2024),
            (3, 11, 20, 250.0, "2024-02-01", "C00002", 2024),
        ],
    )

    deduped = conn.execute(
        """
        SELECT MIN(id), contributor_org_id, recipient_legislator_id,
               amount, contribution_date, fec_committee_id, cycle
        FROM contributions
        GROUP BY contributor_org_id, recipient_legislator_id,
                 amount, contribution_date, fec_committee_id, cycle
        ORDER BY MIN(id)
    """
    ).fetchall()
    assert len(deduped) == 2
    assert deduped[0][0] == 1
    assert deduped[1][0] == 3
    conn.close()


def test_vote_position_normalization_sql():
    """Verify the CASE expression normalizes Aye/Yea->Yes, No/Nay->No."""
    conn = sqlite3.connect(":memory:")
    results = conn.execute(
        """
        SELECT
            CASE raw
                WHEN 'Aye' THEN 'Yes'
                WHEN 'Yea' THEN 'Yes'
                WHEN 'No' THEN 'No'
                WHEN 'Nay' THEN 'No'
                ELSE raw
            END as normalized
        FROM (
            SELECT 'Aye' AS raw UNION ALL
            SELECT 'Yea' UNION ALL
            SELECT 'No' UNION ALL
            SELECT 'Nay' UNION ALL
            SELECT 'Not Voting' UNION ALL
            SELECT 'Present'
        )
        ORDER BY normalized
    """
    ).fetchall()
    values = [r[0] for r in results]
    assert values.count("Yes") == 2
    assert values.count("No") == 2
    assert "Not Voting" in values
    assert "Present" in values
    assert "Aye" not in values
    assert "Yea" not in values
    assert "Nay" not in values
    conn.close()
