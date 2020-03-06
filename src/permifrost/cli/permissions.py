import click
import logging
import sys

from typing import List

from permifrost.core.permissions import SpecLoadingError
from permifrost.core.permissions.snowflake_spec_loader import SnowflakeSpecLoader
from permifrost.core.permissions.utils.snowflake_connector import SnowflakeConnector
from . import cli


def print_command(command, diff):
    """Prints the queries to the command line with prefixes"""
    diff_prefix = ""
    if command["already_granted"]:
        if diff:
            diff_prefix = "  "
        else:
            pass
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


@cli.command()
@click.argument("spec")
@click.option("--dry", help="Do not actually run, just check.", is_flag=True)
@click.option(
    "--diff", help="Show full diff, both new and existing permissions.", is_flag=True
)
def grant(spec, dry, diff):
    """Grant the permissions provided in the provided specification file."""
    try:
        spec_loader = SnowflakeSpecLoader(spec)

        sql_grant_queries = spec_loader.generate_permission_queries()

        click.secho()
        if diff:
            click.secho(
                "SQL Commands generated for given spec file (Full diff with both new and already granted commands):"
            )
        else:
            click.secho("SQL Commands generated for given spec file:")
        click.secho()

        for query in sql_grant_queries:
            if not dry:
                conn = SnowflakeConnector()
                status = None
                if not query.get("already_granted"):
                    try:
                        result = conn.run_query(query.get("sql"))
                        outcome = result.fetchall()
                        status = True
                    except:
                        status = False

                    ran_query = query
                    ran_query["run_status"] = status
                    print_command(ran_query, diff)
                # If already granted, print command
                else:
                    print_command(query, diff)
            # If dry, print commands
            else:
                print_command(query, diff)

    except SpecLoadingError as exc:
        for line in str(exc).splitlines():
            click.secho(line, fg="red")
        sys.exit(1)
