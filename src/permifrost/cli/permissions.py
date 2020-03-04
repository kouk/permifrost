import logging
import click
import sys

from permifrost.core.permissions import grant_permissions, SpecLoadingError
from . import cli


@cli.command()
@click.argument("spec")
@click.option("--dry", help="Do not actually run, just check.", is_flag=True)
@click.option(
    "--diff", help="Show full diff, both new and existing permissions.", is_flag=True
)
def grant(spec, dry, diff):
    """Grant the permissions provided in the provided specification file."""
    try:
        sql_commands = grant_permissions(spec, dry_run=dry)
        click.secho()
        if diff:
            click.secho(
                "SQL Commands generated for given spec file (Full diff with both new and already granted commands):"
            )
        else:
            click.secho("SQL Commands generated for given spec file:")
        click.secho()

        diff_prefix = ""
        for command in sql_commands:
            if command["already_granted"]:
                if diff:
                    diff_prefix = "  "
                else:
                    continue
            else:
                if diff:
                    diff_prefix = "+ "

            if command.get("run_status"):
                fg = "green"
                run_prefix = "[SUCCESS] "
            elif command.get("run_status") is None:
                fg = "cyan"
                run_prefix = "[SKIPPED] "
            else:
                fg = "red"
                run_prefix = "[ERROR] "

            click.secho(f"{diff_prefix}{run_prefix}{command['sql']};", fg=fg)
    except SpecLoadingError as exc:
        for line in str(exc).splitlines():
            click.secho(line, fg="red")
        sys.exit(1)
