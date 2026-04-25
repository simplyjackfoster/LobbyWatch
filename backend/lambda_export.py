import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from urllib import error as urllib_error
from urllib import parse as urllib_parse
from urllib import request as urllib_request

from sqlalchemy import text

from aws_env import bootstrap_ssm_env

bootstrap_ssm_env()

from models import SessionLocal
from sqlite_export import build_and_compress, get_pg_conn

logger = logging.getLogger()
logger.setLevel(logging.INFO)

GITHUB_API = "https://api.github.com"
ASSET_NAME = "lobbywatch.db.zst"


def _ensure_pipeline_meta_table(db) -> None:
    db.execute(
        text(
            """
            CREATE TABLE IF NOT EXISTS _pipeline_meta (
                key TEXT PRIMARY KEY,
                value TEXT
            )
            """
        )
    )
    db.commit()


def _set_pipeline_meta(db, key: str, value: str | None) -> None:
    if value is None:
        return
    db.execute(
        text(
            """
            INSERT INTO _pipeline_meta (key, value)
            VALUES (:key, :value)
            ON CONFLICT (key) DO UPDATE
            SET value = EXCLUDED.value
            """
        ),
        {"key": key, "value": str(value)},
    )


def _get_github_token() -> str:
    direct = (os.getenv("GITHUB_PAT") or "").strip()
    if direct:
        return direct

    param_name = (os.getenv("GITHUB_PAT_PARAM") or "/lobbywatch/prod/github_pat").strip()
    if not param_name:
        raise RuntimeError("GITHUB_PAT_PARAM is missing")

    import boto3

    ssm = boto3.client("ssm")
    response = ssm.get_parameter(Name=param_name, WithDecryption=True)
    token = ((response.get("Parameter") or {}).get("Value") or "").strip()
    if not token:
        raise RuntimeError(f"GitHub token is empty in SSM parameter {param_name}")
    return token


def _github_request(
    token: str,
    method: str,
    url: str,
    *,
    payload: dict | None = None,
    data: bytes | None = None,
    content_type: str | None = None,
) -> dict:
    headers = {
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {token}",
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": "LobbyWatchExport/1.0",
    }

    body = None
    if payload is not None:
        headers["Content-Type"] = "application/json"
        body = json.dumps(payload).encode("utf-8")
    elif data is not None:
        headers["Content-Type"] = content_type or "application/octet-stream"
        body = data

    req = urllib_request.Request(url, data=body, method=method, headers=headers)
    try:
        with urllib_request.urlopen(req, timeout=60) as resp:
            raw = resp.read().decode("utf-8")
            return json.loads(raw) if raw else {}
    except urllib_error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        if exc.code == 404:
            return {"_not_found": True, "_status": 404, "_detail": detail}
        raise RuntimeError(f"GitHub API error {exc.code}: {detail[:500]}") from exc


def _ensure_release(token: str, owner: str, repo: str, tag: str) -> dict:
    existing = _github_request(token, "GET", f"{GITHUB_API}/repos/{owner}/{repo}/releases/tags/{tag}")
    if existing.get("_not_found"):
        payload = {
            "tag_name": tag,
            "name": f"Data snapshot {tag}",
            "draft": False,
            "prerelease": False,
            "make_latest": "true",
        }
        return _github_request(token, "POST", f"{GITHUB_API}/repos/{owner}/{repo}/releases", payload=payload)

    release_id = existing.get("id")
    if release_id:
        patch_payload = {
            "name": f"Data snapshot {tag}",
            "draft": False,
            "prerelease": False,
            "make_latest": "true",
        }
        updated = _github_request(
            token,
            "PATCH",
            f"{GITHUB_API}/repos/{owner}/{repo}/releases/{release_id}",
            payload=patch_payload,
        )
        return updated

    return existing


def _upload_asset(token: str, release: dict, asset_path: Path) -> None:
    upload_url_template = (release.get("upload_url") or "").strip()
    if not upload_url_template:
        raise RuntimeError("GitHub release payload missing upload_url")

    base_upload_url = upload_url_template.split("{")[0]
    upload_url = f"{base_upload_url}?{urllib_parse.urlencode({'name': ASSET_NAME})}"
    data = asset_path.read_bytes()
    _github_request(
        token,
        "POST",
        upload_url,
        data=data,
        content_type="application/zstd",
    )


def _delete_existing_asset(token: str, owner: str, repo: str, release: dict) -> None:
    for asset in release.get("assets") or []:
        if (asset.get("name") or "") != ASSET_NAME:
            continue
        asset_id = asset.get("id")
        if not asset_id:
            continue
        _github_request(token, "DELETE", f"{GITHUB_API}/repos/{owner}/{repo}/releases/assets/{asset_id}")


def _run_export_and_release() -> dict:
    repository = (os.getenv("GITHUB_REPOSITORY") or "").strip()
    if not repository or "/" not in repository:
        raise RuntimeError("GITHUB_REPOSITORY must be set as owner/repo")
    owner, repo = repository.split("/", 1)

    token = _get_github_token()
    output_path = Path("/tmp") / ASSET_NAME

    pg = get_pg_conn()
    try:
        stats = build_and_compress(pg, str(output_path), level=9)
    finally:
        pg.close()

    today_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    tag = f"data-{today_utc}"

    _ensure_release(token, owner, repo, tag)
    release = _github_request(token, "GET", f"{GITHUB_API}/repos/{owner}/{repo}/releases/tags/{tag}")
    _delete_existing_asset(token, owner, repo, release)
    _upload_asset(token, release, output_path)

    db = SessionLocal()
    try:
        _ensure_pipeline_meta_table(db)
        _set_pipeline_meta(db, "last_exported_at", datetime.now(timezone.utc).replace(microsecond=0).isoformat())
        db.commit()
    finally:
        db.close()

    return {
        "task": "export_and_release",
        "status": "ok",
        "release_tag": tag,
        "asset": ASSET_NAME,
        "raw_size_bytes": stats.get("raw_size_bytes"),
        "compressed_size_bytes": stats.get("compressed_size_bytes"),
    }


def _handle_task(task: str) -> dict:
    if task == "export_and_release":
        return _run_export_and_release()
    return {"task": task or "unknown", "status": "ignored"}


def handler(event, context):
    logger.info("export event: %s", json.dumps(event)[:4000])
    responses = []

    records = (event or {}).get("Records") or []
    if records:
        for record in records:
            body = record.get("body") or "{}"
            try:
                payload = json.loads(body)
            except Exception:
                payload = {}
            task = str(payload.get("task") or "export_and_release").strip()
            responses.append(_handle_task(task))
        return {"results": responses}

    task = str((event or {}).get("task") or "export_and_release").strip()
    responses.append(_handle_task(task))
    return {"results": responses}
