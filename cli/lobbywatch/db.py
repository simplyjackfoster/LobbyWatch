# cli/lobbywatch/db.py
import sqlite3
from importlib.resources import files
from pathlib import Path

import zstandard

DATA_DIR = Path.home() / ".lobbywatch"
DB_PATH = DATA_DIR / "lobbywatch.db"


def get_data_dir() -> Path:
    return DATA_DIR


def get_db_path() -> Path:
    return DB_PATH


def ensure_db(db_path: str = None) -> str:
    """Return path to DB, extracting bundled snapshot if needed."""
    target = Path(db_path) if db_path else DB_PATH
    if not target.exists():
        target.parent.mkdir(parents=True, exist_ok=True)
        zst = files("lobbywatch.data").joinpath("lobbywatch.db.zst")
        with zst.open("rb") as src, open(target, "wb") as dst:
            zstandard.ZstdDecompressor().copy_stream(src, dst)
    return str(target)


def get_connection(db_path: str = None) -> sqlite3.Connection:
    path = db_path or ensure_db()
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn


def get_version(db_path: str = None) -> dict:
    with get_connection(db_path) as conn:
        rows = conn.execute("SELECT key, value FROM _meta").fetchall()
        return {row["key"]: row["value"] for row in rows}
