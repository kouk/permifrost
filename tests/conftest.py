import pytest
import os
import logging

logging.basicConfig(level=logging.INFO)

pytest_plugins = [
    "fixtures.fs",
    "fixtures.cli",
]


@pytest.fixture(scope="session")
def concurrency():
    return {
        "threads": int(os.getenv("PYTEST_CONCURRENCY_THREADS", 8)),
        "processes": int(os.getenv("PYTEST_CONCURRENCY_PROCESSES", 8)),
        "cases": int(os.getenv("PYTEST_CONCURRENCY_CASES", 64)),
    }
