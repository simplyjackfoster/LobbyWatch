# cli/lobbywatch/cli.py
import json
import os

import click

from lobbywatch.db import ensure_db, get_connection, get_db_path, get_version
from lobbywatch.commands.update import download_and_install, DEFAULT_URL
from lobbywatch.commands.search import search_entities
from lobbywatch.commands.graph import get_org_graph, get_legislator_graph, get_issue_graph


def output_json(obj: object, pretty: bool) -> None:
    if pretty:
        print(json.dumps(obj, indent=2, default=str))
    else:
        print(json.dumps(obj, separators=(",", ":"), default=str))


def error_json(message: str, pretty: bool = False) -> None:
    output_json({"error": message}, pretty)


@click.group()
@click.option("--pretty", is_flag=True, default=False, help="Pretty-print JSON output")
@click.option("--db", default=None, help="Override path to local SQLite database")
@click.pass_context
def cli(ctx, pretty, db):
    """LobbyWatch CLI — query political influence data locally."""
    ctx.ensure_object(dict)
    ctx.obj["pretty"] = pretty
    ctx.obj["db"] = db


@cli.command()
@click.option("--url", default=DEFAULT_URL, help="URL to .db.zst snapshot")
@click.pass_context
def update(ctx, url):
    """Download latest data snapshot from GitHub Releases."""
    pretty = ctx.obj["pretty"]
    db = ctx.obj["db"] or str(get_db_path())
    try:
        download_and_install(url, db)
        meta = get_version(db)
        output_json({"ok": True, "exported_at": meta.get("exported_at"), "db_path": db}, pretty)
    except Exception as e:
        error_json(str(e), pretty)
        raise SystemExit(1)


@cli.command()
@click.pass_context
def status(ctx):
    """Show installed data version and file size."""
    pretty = ctx.obj["pretty"]
    db = ctx.obj["db"] or str(get_db_path())
    if not os.path.exists(db):
        error_json("No database found. Run: lobbywatch update", pretty)
        raise SystemExit(1)
    try:
        meta = get_version(db)
        size = os.path.getsize(db)
        output_json({**meta, "db_path": db, "size_bytes": size}, pretty)
    except Exception as e:
        error_json(str(e), pretty)
        raise SystemExit(1)


@cli.command("issue-codes")
@click.pass_context
def issue_codes(ctx):
    """List all issue codes in the local database."""
    pretty = ctx.obj["pretty"]
    db = ctx.obj["db"] or str(get_db_path())
    try:
        with get_connection(db) as conn:
            rows = conn.execute(
                "SELECT DISTINCT je.value FROM lobbying_registrations r, "
                "json_each(r.general_issue_codes) je ORDER BY je.value"
            ).fetchall()
            codes = [r[0] for r in rows if r[0]]
        output_json({"issue_codes": codes}, pretty)
    except Exception as e:
        error_json(str(e), pretty)
        raise SystemExit(1)


@cli.command()
@click.argument("query")
@click.option("--type", "entity_type", default=None,
              type=click.Choice(["org", "legislator", "issue"]),
              help="Filter result type")
@click.pass_context
def search(ctx, query, entity_type):
    """Search organizations, legislators, and issues."""
    pretty = ctx.obj["pretty"]
    db = ctx.obj["db"] or str(get_db_path())
    try:
        with get_connection(db) as conn:
            results = search_entities(conn, query)
        if entity_type:
            type_map = {"org": "organization", "legislator": "legislator", "issue": "issue"}
            results["results"] = [r for r in results["results"] if r["type"] == type_map[entity_type]]
        output_json(results, pretty)
    except Exception as e:
        error_json(str(e), pretty)
        raise SystemExit(1)


@cli.group()
def graph():
    """Explore influence graphs."""
    pass


@graph.command("org")
@click.argument("org_id", type=int)
@click.option("--year-min", type=int, default=None)
@click.option("--year-max", type=int, default=None)
@click.option("--issue-code", default=None)
@click.option("--node-limit", type=int, default=50)
@click.pass_context
def graph_org(ctx, org_id, year_min, year_max, issue_code, node_limit):
    """Organization influence graph."""
    pretty = ctx.obj["pretty"]
    db = ctx.obj["db"] or str(get_db_path())
    try:
        with get_connection(db) as conn:
            output_json(get_org_graph(conn, org_id, year_min, year_max, issue_code, node_limit), pretty)
    except Exception as e:
        error_json(str(e), pretty)
        raise SystemExit(1)


@graph.command("legislator")
@click.argument("bioguide_id")
@click.option("--year-min", type=int, default=None)
@click.option("--year-max", type=int, default=None)
@click.option("--node-limit", type=int, default=50)
@click.pass_context
def graph_legislator(ctx, bioguide_id, year_min, year_max, node_limit):
    """Legislator influence graph."""
    pretty = ctx.obj["pretty"]
    db = ctx.obj["db"] or str(get_db_path())
    try:
        with get_connection(db) as conn:
            output_json(get_legislator_graph(conn, bioguide_id, year_min, year_max, node_limit), pretty)
    except Exception as e:
        error_json(str(e), pretty)
        raise SystemExit(1)


@graph.command("issue")
@click.argument("query")
@click.option("--year-min", type=int, default=None)
@click.option("--year-max", type=int, default=None)
@click.option("--node-limit", type=int, default=50)
@click.pass_context
def graph_issue(ctx, query, year_min, year_max, node_limit):
    """Issue-based influence graph."""
    pretty = ctx.obj["pretty"]
    db = ctx.obj["db"] or str(get_db_path())
    try:
        with get_connection(db) as conn:
            output_json(get_issue_graph(conn, query, year_min, year_max, node_limit), pretty)
    except Exception as e:
        error_json(str(e), pretty)
        raise SystemExit(1)
