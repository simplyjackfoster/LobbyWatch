# cli/lobbywatch/cli.py
import json
import os

import click

from lobbywatch.db import ensure_db, get_connection, get_db_path, get_version


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
