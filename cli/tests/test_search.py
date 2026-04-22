# cli/tests/test_search.py
import json
from click.testing import CliRunner
from lobbywatch.cli import cli


def test_search_org_by_name(db_path):
    result = CliRunner().invoke(cli, ["--db", db_path, "search", "pfizer"])
    assert result.exit_code == 0
    data = json.loads(result.output)
    assert any(r["type"] == "organization" and "Pfizer" in r["name"] for r in data["results"])


def test_search_legislator_by_name(db_path):
    result = CliRunner().invoke(cli, ["--db", db_path, "search", "Jane"])
    assert result.exit_code == 0
    data = json.loads(result.output)
    assert any(r["type"] == "legislator" for r in data["results"])


def test_search_issue_by_keyword(db_path):
    result = CliRunner().invoke(cli, ["--db", db_path, "search", "healthcare"])
    assert result.exit_code == 0
    data = json.loads(result.output)
    assert any(r["type"] == "issue" for r in data["results"])


def test_search_returns_json_structure(db_path):
    result = CliRunner().invoke(cli, ["--db", db_path, "search", "health"])
    assert result.exit_code == 0
    data = json.loads(result.output)
    assert "results" in data
    for r in data["results"]:
        assert "id" in r and "type" in r and "name" in r
