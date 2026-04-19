"""
Post-ingestion deduplication pass.
Merges organizations with identical name_normalized into a single canonical row.
Run after each ingestion cycle: python dedup_orgs.py
"""

import logging

from sqlalchemy import text

from db import SessionLocal

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler("pipeline_errors.log")],
)


def main():
    db = SessionLocal()
    merged_groups = 0
    merged_rows = 0

    try:
        groups = db.execute(
            text(
                """
                SELECT name_normalized, array_agg(id ORDER BY id) AS ids
                FROM organizations
                GROUP BY name_normalized
                HAVING COUNT(*) > 1
                """
            )
        ).all()

        if not groups:
            logging.info("No duplicates found")
            return

        for group in groups:
            ids = list(group.ids or [])
            if len(ids) < 2:
                continue

            canonical_id = ids[0]
            duplicate_ids = ids[1:]
            try:
                with db.begin_nested():
                    db.execute(
                        text(
                            """
                            UPDATE lobbying_registrations
                            SET registrant_id = :canonical_id
                            WHERE registrant_id = ANY(:duplicate_ids)
                            """
                        ),
                        {"canonical_id": canonical_id, "duplicate_ids": duplicate_ids},
                    )
                    db.execute(
                        text(
                            """
                            UPDATE lobbying_registrations
                            SET client_id = :canonical_id
                            WHERE client_id = ANY(:duplicate_ids)
                            """
                        ),
                        {"canonical_id": canonical_id, "duplicate_ids": duplicate_ids},
                    )
                    db.execute(
                        text(
                            """
                            UPDATE contributions
                            SET contributor_org_id = :canonical_id
                            WHERE contributor_org_id = ANY(:duplicate_ids)
                            """
                        ),
                        {"canonical_id": canonical_id, "duplicate_ids": duplicate_ids},
                    )
                    db.execute(
                        text("DELETE FROM organizations WHERE id = ANY(:duplicate_ids)"),
                        {"duplicate_ids": duplicate_ids},
                    )
                merged_groups += 1
                merged_rows += len(duplicate_ids)
            except Exception as e:
                logging.error("Dedup failed for group, rolling back: %s", e)
                continue

        db.commit()
        logging.info("Organization dedup complete: groups_merged=%s rows_merged=%s", merged_groups, merged_rows)
    except Exception:
        db.rollback()
        logging.exception("Organization dedup failed")
    finally:
        db.close()


if __name__ == "__main__":
    main()
