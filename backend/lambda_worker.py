import json
import logging
import os
from typing import Any

from aws_env import bootstrap_ssm_env

bootstrap_ssm_env()

from models import SessionLocal
from sqlalchemy import text

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def _run_analyze() -> None:
    db = SessionLocal()
    try:
        db.execute(text("ANALYZE"))
        db.commit()
    finally:
        db.close()


def _handle_task(task: str, payload: dict[str, Any]) -> dict[str, Any]:
    if task == "analyze":
        _run_analyze()
        return {"task": task, "status": "ok"}

    if task == "scheduled_ingest":
        # Hook point for async ingestion orchestration.
        return {
            "task": task,
            "status": "queued",
            "note": "No-op placeholder. Trigger pipeline jobs from this handler as needed.",
        }

    return {"task": task or "unknown", "status": "ignored"}


def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    logger.info("worker event: %s", json.dumps(event)[:4000])
    responses: list[dict[str, Any]] = []

    records = event.get("Records") or []
    if records:
        for record in records:
            body = record.get("body") or "{}"
            try:
                payload = json.loads(body)
            except Exception:
                payload = {"raw": body}
            task = str(payload.get("task") or "").strip()
            responses.append(_handle_task(task, payload))
        return {"results": responses}

    task = str((event or {}).get("task") or "scheduled_ingest").strip()
    responses.append(_handle_task(task, event or {}))
    return {"results": responses, "env": os.getenv("LOBBYWATCH_ENV", "unknown")}
