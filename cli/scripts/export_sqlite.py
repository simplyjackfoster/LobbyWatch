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
import os
import shutil
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.sqlite_export import build_and_compress as _build_and_compress, build_db, compress, get_pg_conn

PIP_SIZE_LIMIT = 60 * 1024 * 1024  # 60 MB


def build_and_compress(pg_conn, output_path, level=22):
    return _build_and_compress(pg_conn, output_path, level=level)


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
    compressed_size = compress("lobbywatch.db", "lobbywatch.db.zst", level=22)
    print(f"Compressed: {compressed_size / 1e6:.1f} MB -> lobbywatch.db.zst")

    if compressed_size > PIP_SIZE_LIMIT:
        print(f"\nWARNING: {compressed_size / 1e6:.1f} MB exceeds PyPI 60 MB limit.")
        print("Building curated bundled subset (recent 2 years)...")
        cutoff2 = time.gmtime().tm_year - 2
        build_db("lobbywatch_bundled.db", pg, f"WHERE filing_year >= {cutoff2}")
        bundled_size = compress("lobbywatch_bundled.db", "lobbywatch_bundled.db.zst", level=22)
        print(f"Bundled compressed: {bundled_size / 1e6:.1f} MB -> lobbywatch_bundled.db.zst")
        if bundled_size > PIP_SIZE_LIMIT:
            print("  Still too large. Consider --years 1 or --top-orgs 1000.")
        else:
            print("  OK for PyPI. Copy to cli/lobbywatch/data/lobbywatch.db.zst before publish.")
    else:
        print("\nFull snapshot fits in PyPI limit. Copy to cli/lobbywatch/data/lobbywatch.db.zst")
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
