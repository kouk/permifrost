import pytest
import os
import shutil

import permifrost
from permifrost.cli import cli


def test_version(cli_runner):
    cli_version = cli_runner.invoke(cli, ["--version"])

    assert cli_version.output == f"permifrost, version {permifrost.__version__}\n"
