import os
import pytest
import tempfile
import shutil
from pathlib import Path
from functools import partial


@pytest.fixture(scope="function")
def mkdtemp(request):
    def _mkdtemp(*args, **kwargs):
        path = Path(tempfile.mkdtemp())
        cleanup = partial(shutil.rmtree, path, ignore_errors=True)

        request.addfinalizer(cleanup)
        return path

    return _mkdtemp


@pytest.fixture
def pushd(request):
    def _pushd(path):
        popd = partial(os.chdir, os.getcwd())
        request.addfinalizer(popd)
        os.chdir(path)

        return popd

    return _pushd


@pytest.mark.meta
def test_pushd(mkdtemp, pushd):
    temp = mkdtemp()

    os.makedirs(temp.joinpath("a"))
    os.makedirs(temp.joinpath("a/b"))

    pushd(temp / "a")
    assert os.getcwd() == str(temp.joinpath("a"))

    popd = pushd("b")
    assert os.getcwd() == str(temp.joinpath("a/b"))

    popd()
    assert os.getcwd() == str(temp.joinpath("a"))
