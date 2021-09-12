import pytest
import os
import logging
from colorama import init, Fore, Style

init()

logging.basicConfig(level=logging.INFO)

pytest_plugins = ["fixtures.fs", "fixtures.cli"]


@pytest.fixture(scope="session")
def concurrency():
    return {
        "threads": int(os.getenv("PYTEST_CONCURRENCY_THREADS", 8)),
        "processes": int(os.getenv("PYTEST_CONCURRENCY_PROCESSES", 8)),
        "cases": int(os.getenv("PYTEST_CONCURRENCY_CASES", 64)),
    }


def pytest_itemcollected(item):
    """
    Brought in for docstring output:
    https://stackoverflow.com/a/39035226
    """
    par = item.parent.obj
    node = item.obj
    pref = par.__doc__.strip() + " " if par.__doc__ else ""
    try:
        # Enables docstrings for class, methods, functions passed in mark.parametrize
        # to be concatenated and output to the CLI when pytest is run
        suf = node.__doc__.strip() + " " if node.__doc__ else ""
        if "[" in item._nodeid:
            mark_list = item._nodeid.split("[")[-1][:-1].split("-")
        else:
            mark_list = []
        for pytest_mark in node.pytestmark:
            if pytest_mark.name == "parametrize":
                for param in pytest_mark.args[1]:
                    if param.__name__ in mark_list:
                        clean_doc = param.__doc__.strip()
                        clean_doc = " ".join(clean_doc.split())
                        suf += clean_doc + " "
    except AttributeError as e:
        logging.exception(e)
        suf = node.__doc__.strip() + " " if node.__doc__ else ""
    except Exception as e:
        logging.exception(e)
        suf = node.__doc__.strip() + " " if node.__doc__ else ""
    if pref or suf:
        item._nodeid = (
            Fore.YELLOW + "".join((pref, suf)) + "\n" + Style.RESET_ALL + item._nodeid
        )
