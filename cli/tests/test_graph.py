# cli/tests/test_graph.py
import json
from click.testing import CliRunner
from lobbywatch.cli import cli


def test_graph_org_returns_nodes_edges(db_path):
    result = CliRunner().invoke(cli, ["--db", db_path, "graph", "org", "1"])
    assert result.exit_code == 0
    data = json.loads(result.output)
    assert "nodes" in data and "edges" in data
    assert any(n["type"] == "organization" for n in data["nodes"])


def test_graph_org_unknown_id_returns_empty(db_path):
    result = CliRunner().invoke(cli, ["--db", db_path, "graph", "org", "99999"])
    assert result.exit_code == 0
    data = json.loads(result.output)
    assert data["nodes"] == [] and data["edges"] == []


def test_graph_legislator_returns_nodes_edges(db_path):
    result = CliRunner().invoke(cli, ["--db", db_path, "graph", "legislator", "A000001"])
    assert result.exit_code == 0
    data = json.loads(result.output)
    assert "nodes" in data and "edges" in data
    assert any(n["type"] == "legislator" for n in data["nodes"])


def test_graph_issue_returns_nodes_edges(db_path):
    result = CliRunner().invoke(cli, ["--db", db_path, "graph", "issue", "healthcare"])
    assert result.exit_code == 0
    data = json.loads(result.output)
    assert "nodes" in data and "edges" in data
