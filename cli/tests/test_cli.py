# cli/tests/test_cli.py
import json
from click.testing import CliRunner
from lobbywatch.cli import cli


def test_help():
    result = CliRunner().invoke(cli, ["--help"])
    assert result.exit_code == 0
    assert "lobbywatch" in result.output.lower()


def test_unknown_command_exits_nonzero():
    result = CliRunner().invoke(cli, ["notacommand"])
    assert result.exit_code != 0
