import json
from click.testing import CliRunner
from lobbywatch.cli import cli


def test_entity_org_returns_summary(db_path):
    result = CliRunner().invoke(cli, ["--db", db_path, "entity", "org", "1"])
    assert result.exit_code == 0
    data = json.loads(result.output)
    assert data["type"] == "organization"
    assert "total_lobbying_spend" in data
    assert data["name"] == "Pfizer Inc."


def test_entity_org_unknown_returns_zero_spend(db_path):
    result = CliRunner().invoke(cli, ["--db", db_path, "entity", "org", "99999"])
    assert result.exit_code == 0
    data = json.loads(result.output)
    assert data["total_lobbying_spend"] == 0


def test_entity_legislator_returns_summary(db_path):
    result = CliRunner().invoke(cli, ["--db", db_path, "entity", "legislator", "A000001"])
    assert result.exit_code == 0
    data = json.loads(result.output)
    assert data["name"] == "Jane Smith"
    assert isinstance(data["committees"], list)


def test_entity_committee_returns_summary(db_path):
    result = CliRunner().invoke(cli, ["--db", db_path, "entity", "committee", "SSHR"])
    assert result.exit_code == 0
    data = json.loads(result.output)
    assert data["name"] == "Senate Health Committee"
    assert data["member_count"] >= 1
