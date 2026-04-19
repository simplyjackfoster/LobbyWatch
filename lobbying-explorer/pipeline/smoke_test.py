import ast
import os
import sys
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine, text


def load_normalize_name():
    db_py = Path(__file__).with_name("db.py")
    tree = ast.parse(db_py.read_text())
    fn = next(node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == "normalize_name")
    module = ast.Module(body=[fn], type_ignores=[])
    code = compile(module, "db.py", "exec")
    ns = {}
    import re

    ns["re"] = re
    exec(code, ns)
    return ns["normalize_name"]


def check_bool(name: str, ok: bool):
    status = "PASS" if ok else "FAIL"
    print(f"{status} [ ] {name}")
    return ok


def scalar_bool(conn, sql: str, params=None) -> bool:
    value = conn.execute(text(sql), params or {}).scalar()
    return bool(value)


def main():
    load_dotenv()
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        print("FAIL [ ] DATABASE_URL is not set")
        sys.exit(1)

    engine = create_engine(db_url, pool_pre_ping=True)
    checks = []

    try:
        with engine.connect() as conn:
            checks.append(
                check_bool(
                    "organizations table exists and has name_normalized column",
                    scalar_bool(
                        conn,
                        """
                        SELECT EXISTS (
                          SELECT 1
                          FROM information_schema.columns
                          WHERE table_name = 'organizations'
                            AND column_name = 'name_normalized'
                        )
                        """,
                    ),
                )
            )
            checks.append(
                check_bool(
                    "lobbying_registrations table has general_issue_codes column",
                    scalar_bool(
                        conn,
                        """
                        SELECT EXISTS (
                          SELECT 1
                          FROM information_schema.columns
                          WHERE table_name = 'lobbying_registrations'
                            AND column_name = 'general_issue_codes'
                        )
                        """,
                    ),
                )
            )
            checks.append(
                check_bool(
                    "lobbying_registrations table has specific_issues_tsv column",
                    scalar_bool(
                        conn,
                        """
                        SELECT EXISTS (
                          SELECT 1
                          FROM information_schema.columns
                          WHERE table_name = 'lobbying_registrations'
                            AND column_name = 'specific_issues_tsv'
                        )
                        """,
                    ),
                )
            )
            checks.append(
                check_bool(
                    "GIN index idx_specific_issues_fts exists",
                    scalar_bool(
                        conn,
                        """
                        SELECT EXISTS (
                          SELECT 1
                          FROM pg_indexes
                          WHERE schemaname = 'public'
                            AND indexname = 'idx_specific_issues_fts'
                            AND indexdef ILIKE '%USING gin%'
                        )
                        """,
                    ),
                )
            )
            checks.append(
                check_bool(
                    "GIN index idx_general_issue_codes exists",
                    scalar_bool(
                        conn,
                        """
                        SELECT EXISTS (
                          SELECT 1
                          FROM pg_indexes
                          WHERE schemaname = 'public'
                            AND indexname = 'idx_general_issue_codes'
                            AND indexdef ILIKE '%USING gin%'
                        )
                        """,
                    ),
                )
            )
            checks.append(
                check_bool(
                    "ingestion_runs table exists",
                    scalar_bool(
                        conn,
                        """
                        SELECT EXISTS (
                          SELECT 1
                          FROM information_schema.tables
                          WHERE table_name = 'ingestion_runs'
                        )
                        """,
                    ),
                )
            )
    except Exception as exc:
        print(f"FAIL [ ] database connection/schema checks failed: {exc}")
        sys.exit(1)

    normalize_name = load_normalize_name()
    checks.append(
        check_bool(
            "normalize_name('PFIZER INC PAC') == 'PFIZER'",
            normalize_name("PFIZER INC PAC") == "PFIZER",
        )
    )
    checks.append(
        check_bool(
            "normalize_name('Goldman Sachs LLC') == 'GOLDMAN SACHS'",
            normalize_name("Goldman Sachs LLC") == "GOLDMAN SACHS",
        )
    )

    if all(checks):
        sys.exit(0)
    sys.exit(1)


if __name__ == "__main__":
    main()
