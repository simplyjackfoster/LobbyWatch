# cli/tests/test_db.py
import sqlite3
import pytest
from lobbywatch.db import get_connection, get_version


def test_get_connection_returns_row_factory(db_path):
    conn = get_connection(db_path)
    row = conn.execute("SELECT key, value FROM _meta WHERE key='exported_at'").fetchone()
    assert row["key"] == "exported_at"
    conn.close()


def test_get_version_returns_dict(db_path):
    meta = get_version(db_path)
    assert meta["exported_at"] == "2026-04-22T00:00:00"
    assert meta["schema_version"] == "1"


def test_get_connection_missing_table_raises(db_path):
    conn = get_connection(db_path)
    with pytest.raises(Exception):
        conn.execute("SELECT 1 FROM nonexistent_table_xyz").fetchone()
    conn.close()
