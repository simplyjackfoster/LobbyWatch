import json

from click.testing import CliRunner

from lobbywatch.cli import cli


def test_betrayal_index_returns_findings(db_path):
    result = CliRunner().invoke(
        cli, ["--db", db_path, "analysis", "betrayal-index", "--issue-code", "HLTH"]
    )
    assert result.exit_code == 0
    data = json.loads(result.output)
    assert "findings" in data
    assert len(data["findings"]) >= 1
    finding = data["findings"][0]
    assert "legislator" in finding
    assert "betrayal_score" in finding


def test_revolving_door_returns_findings(db_path):
    result = CliRunner().invoke(cli, ["--db", db_path, "analysis", "revolving-door"])
    assert result.exit_code == 0
    data = json.loads(result.output)
    assert "findings" in data
    assert any(f["lobbyist"]["name"] == "Bob Lobbyist" for f in data["findings"])


def test_foreign_influence_returns_findings(db_path):
    result = CliRunner().invoke(cli, ["--db", db_path, "analysis", "foreign-influence"])
    assert result.exit_code == 0
    data = json.loads(result.output)
    assert "findings" in data
    assert any("UK" in f["foreign_countries"] for f in data["findings"])


def test_betrayal_min_contribution_filter(db_path):
    result = CliRunner().invoke(
        cli,
        [
            "--db",
            db_path,
            "analysis",
            "betrayal-index",
            "--issue-code",
            "HLTH",
            "--min-contribution",
            "999999999",
        ],
    )
    assert result.exit_code == 0
    data = json.loads(result.output)
    assert data["findings"] == []
