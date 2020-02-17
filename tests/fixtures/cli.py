import pytest
import os

from click.testing import CliRunner


@pytest.mark.usefixtures("session")
@pytest.fixture()
def cli_runner(pushd):
    # this will make sure we are back at `cwd`
    # after this test is finished
    pushd(os.getcwd())

    yield CliRunner(mix_stderr=False)
