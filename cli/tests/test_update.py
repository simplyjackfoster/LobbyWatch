# cli/tests/test_update.py
import json
import sqlite3
import zstandard
from click.testing import CliRunner
from lobbywatch.cli import cli


def make_zst(db_path_str: str, zst_path_str: str) -> None:
    cctx = zstandard.ZstdCompressor(level=1)
    with open(db_path_str, "rb") as src, open(zst_path_str, "wb") as dst:
        cctx.copy_stream(src, dst)


def test_update_file_url(tmp_path, db_path):
    zst = tmp_path / "test.db.zst"
    make_zst(db_path, str(zst))
    out_db = tmp_path / "out.db"
    result = CliRunner().invoke(
        cli,
        ["--db", str(out_db), "update", "--url", f"file://{zst}"],
    )
    assert result.exit_code == 0, result.output
    assert out_db.exists()
    conn = sqlite3.connect(str(out_db))
    conn.row_factory = sqlite3.Row
    meta = {r["key"]: r["value"] for r in conn.execute("SELECT key, value FROM _meta").fetchall()}
    conn.close()
    assert meta["exported_at"] == "2026-04-22T00:00:00"


def test_status_shows_version(db_path):
    result = CliRunner().invoke(cli, ["--db", db_path, "status"])
    assert result.exit_code == 0
    data = json.loads(result.output)
    assert data["exported_at"] == "2026-04-22T00:00:00"
    assert "size_bytes" in data


def test_issue_codes_returns_list(db_path):
    result = CliRunner().invoke(cli, ["--db", db_path, "issue-codes"])
    assert result.exit_code == 0
    data = json.loads(result.output)
    assert "issue_codes" in data
    assert "HLTH" in data["issue_codes"]
