import permifrost
from permifrost.cli import cli
from permifrost.cli.permissions import run, spec_cli_test


def test_version(cli_runner):
    cli_version = cli_runner.invoke(cli, ["--version"])
    assert cli_version.output == f"permifrost, version {permifrost.__version__}\n"


def test_run_command(cli_runner):
    cli_version = cli_runner.invoke(run, ["--help"])
    cli_output = cli_version.output
    assert (len(cli_output) >= 5) and (cli_output[:5] == "Usage")


def test_load_command(cli_runner):
    cli_version = cli_runner.invoke(spec_cli_test, ["--help"])
    cli_output = cli_version.output
    assert (len(cli_output) >= 5) and (cli_output[:5] == "Usage")
