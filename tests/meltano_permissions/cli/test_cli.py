import pytest
import os
import shutil

import meltano_permissions
from meltano_permissions.cli import cli


def test_version(cli_runner):
    cli_version = cli_runner.invoke(cli, ["--version"])

    assert cli_version.output == f"meltano_permissions, version {meltano_permissions.__version__}\n"
