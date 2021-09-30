import logging

import click

import permifrost
from permifrost.core.logging import setup_logging


@click.group(invoke_without_command=True, no_args_is_help=True)
@click.option("-v", "--verbose", count=True)
@click.version_option(version=permifrost.__version__, prog_name="permifrost")
@click.pass_context
def cli(ctx, verbose):
    log_level = logging.WARNING

    if verbose == 1:
        log_level = logging.INFO
    if verbose >= 2:
        log_level = logging.DEBUG

    setup_logging(log_level=log_level)

    ctx.ensure_object(dict)
    ctx.obj["verbosity"] = verbose
