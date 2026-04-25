"""Post-export integrity assertions for the SQLite snapshot."""

import sqlite3


class ValidationError(Exception):
    """Raised when an exported SQLite snapshot violates required invariants."""


def _q(conn: sqlite3.Connection, sql: str) -> int:
    row = conn.execute(sql).fetchone()
    return int(row[0]) if row else 0


def validate_export(db_path: str) -> None:
    """Assert data-quality invariants on a completed SQLite export."""
    conn = sqlite3.connect(db_path)
    try:
        _check_lobbyists(conn)
        _check_issue_codes(conn)
        _check_specific_issues(conn)
        _check_inactive_legislators(conn)
        _warn_contribution_cycles(conn)
    finally:
        conn.close()


def _check_lobbyists(conn: sqlite3.Connection) -> None:
    total_regs = _q(conn, "SELECT COUNT(*) FROM lobbying_registrations")
    if total_regs == 0:
        return

    lobbyist_count = _q(conn, "SELECT COUNT(*) FROM lobbyists")
    if lobbyist_count == 0:
        raise ValidationError(
            "lobbyists table is empty but lobbying_registrations has rows. "
            "The LDA lobbyist ingest step likely failed silently."
        )

    link_count = _q(conn, "SELECT COUNT(*) FROM lobbying_lobbyists")
    if link_count == 0:
        raise ValidationError(
            "lobbying_lobbyists table is empty but lobbyists and "
            "lobbying_registrations both have rows. The lobbyist linkage "
            "INSERT step likely failed silently."
        )


def _check_issue_codes(conn: sqlite3.Connection) -> None:
    total = _q(conn, "SELECT COUNT(*) FROM lobbying_registrations")
    if total == 0:
        return

    populated = _q(conn, "SELECT COUNT(*) FROM lobbying_registrations WHERE issue_codes != '[]'")
    if populated == 0:
        raise ValidationError(
            f"issue_codes is '[]' for all {total} lobbying_registrations rows. "
            "The issue_codes column is never being written by the ingest pipeline."
        )


def _check_specific_issues(conn: sqlite3.Connection) -> None:
    total = _q(conn, "SELECT COUNT(*) FROM lobbying_registrations")
    if total == 0:
        return

    populated = _q(
        conn,
        "SELECT COUNT(*) FROM lobbying_registrations "
        "WHERE specific_issues IS NOT NULL AND specific_issues != ''",
    )
    if populated == 0:
        raise ValidationError(
            f"specific_issues is empty for all {total} lobbying_registrations rows. "
            "Check the LDA API field name (specific_issue vs specific_issues)."
        )


def _check_inactive_legislators(conn: sqlite3.Connection) -> None:
    inactive = _q(conn, "SELECT COUNT(*) FROM legislators WHERE is_active = 0")
    if inactive == 0:
        raise ValidationError(
            "No inactive legislators found (all legislators have is_active=1). "
            "The Congress API ingest is likely filtering to current members only, "
            "omitting all historical legislators."
        )


def _warn_contribution_cycles(conn: sqlite3.Connection) -> None:
    cycles = _q(conn, "SELECT COUNT(DISTINCT cycle) FROM contributions")
    if cycles < 3:
        print(
            f"WARNING: contributions only covers {cycles} election cycle(s). "
            "Expected at least 3 (e.g. 2020, 2022, 2024). "
            "FEC historical backfill may be incomplete."
        )
