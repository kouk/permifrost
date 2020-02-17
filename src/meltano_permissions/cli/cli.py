import sys
import click
import logging
import warnings

import meltano_permissions
from meltano_permissions.core.logging import setup_logging


logger = logging.getLogger(__name__)


LEVELS = {
    "debug": logging.DEBUG,
    "info": logging.INFO,
    "warning": logging.WARNING,
    "error": logging.ERROR,
    "critical": logging.CRITICAL,
}


@click.group(invoke_without_command=True, no_args_is_help=True)
@click.option("--log-level", type=click.Choice(LEVELS.keys()), default="info")
@click.option("-v", "--verbose", count=True)
@click.version_option(version=meltano_permissions.__version__, prog_name="meltano")
@click.pass_context
def cli(ctx, log_level, verbose):
    """
    Get help at https://www.meltano.com/docs/command-line-interface.html#command-line-interface
    """
    setup_logging(log_level=LEVELS[log_level])

    ctx.ensure_object(dict)
    ctx.obj["verbosity"] = verbose
