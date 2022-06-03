import sys

import click

from permifrost import SpecLoadingError
from permifrost.snowflake_connector import SnowflakeConnector
from permifrost.snowflake_spec_loader import SnowflakeSpecLoader

from . import cli


def print_command(command, diff, dry=False):
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
        foreground_color = "green"
        run_prefix = "[SUCCESS] "
    elif command.get("run_status") is None and dry:
        foreground_color = "cyan"
        run_prefix = "[PENDING] "
    elif command.get("run_status") is None:
        foreground_color = "cyan"
        run_prefix = "[SKIPPED] "
    else:
        foreground_color = "red"
        run_prefix = "[ERROR] "

    click.secho(f"{diff_prefix}{run_prefix}{command['sql']};", fg=foreground_color)


@cli.command()  # type: ignore
@click.argument("spec")
@click.option("--dry", help="Do not actually run, just check.", is_flag=True)
@click.option(
    "--diff", help="Show full diff, both new and existing permissions.", is_flag=True
)
@click.option(
    "--role",
    multiple=True,
    default=[],
    help="Run grants for specific roles. Usage: --role testrole --role testrole2.",
)
@click.option(
    "--user",
    multiple=True,
    default=[],
    help="Run grants for specific users. Usage: --user testuser --user testuser2.",
)
@click.option(
    "--ignore-memberships",
    help="Do not handle role membership grants/revokes",
    is_flag=True,
)
@click.pass_context
def run(ctx, spec, dry, diff, role, user, ignore_memberships, print_skipped=False):
    """
    Grant the permissions provided in the provided specification file for specific users and roles
    """
    if role and user:
        run_list = ["roles", "users"]
    elif role:
        run_list = ["roles"]
    elif user:
        run_list = ["users"]
    else:
        run_list = ["roles", "users"]
    if ctx.parent.params.get("verbose", 0) >= 1:
        print_skipped = True
    permifrost_grants(
        spec=spec,
        dry=dry,
        diff=diff,
        roles=role,
        users=user,
        run_list=run_list,
        ignore_memberships=ignore_memberships,
        print_skipped=print_skipped,
    )


@click.command()
@click.argument("spec")
@click.option(
    "--role",
    multiple=True,
    default=[],
    help="Run grants for specific roles. Usage: --role testrole --role testrole2.",
)
@click.option(
    "--user",
    multiple=True,
    default=[],
    help="Run grants for specific users. Usage: --user testuser --user testuser2.",
)
@click.option(
    "--ignore-memberships",
    help="Do not handle role membership grants/revokes",
    is_flag=True,
)
@click.option(
    "--run-list",
    multiple=True,
    default=["roles", "users"],
    help="Run grants for specific users. Usage: --user testuser --user testuser2.",
)
def spec_test(spec, role, user, ignore_memberships, run_list):
    """
    Load SnowFlake spec based on the roles.yml provided. CLI use only for confirming specifications are valid.
    """
    load_specs(spec, role, user, run_list, ignore_memberships)


def load_specs(spec, role, user, run_list, ignore_memberships):
    """
    Load specs separately.
    """
    try:
        click.secho("Confirming spec loads successfully")
        spec_loader = SnowflakeSpecLoader(
            spec,
            roles=role,
            users=user,
            run_list=run_list,
            ignore_memberships=ignore_memberships,
        )
        click.secho("Snowflake specs successfully loaded", fg="green")
    except SpecLoadingError as exc:
        for line in str(exc).splitlines():
            click.secho(line, fg="red")
        sys.exit(1)

    return spec_loader


def permifrost_grants(
    spec, dry, diff, roles, users, run_list, ignore_memberships, print_skipped
):
    """Grant the permissions provided in the provided specification file."""
    spec_loader = load_specs(
        spec,
        role=roles,
        user=users,
        run_list=run_list,
        ignore_memberships=ignore_memberships,
    )

    sql_grant_queries = spec_loader.generate_permission_queries(
        roles=roles,
        users=users,
        run_list=run_list,
        ignore_memberships=ignore_memberships,
    )

    click.secho()
    if diff:
        click.secho(
            "SQL Commands generated for given spec file (Full diff with both new and already granted commands):"
        )
    else:
        click.secho("SQL Commands generated for given spec file:")
    click.secho()

    conn = SnowflakeConnector()
    for query in sql_grant_queries:
        if not dry:
            status = None
            if not query.get("already_granted"):
                try:
                    conn.run_query(query.get("sql", ""))
                    status = True
                except Exception:
                    status = False

                ran_query = query
                ran_query["run_status"] = status
                print_command(ran_query, diff)
            # If already granted, print command
            elif print_skipped:
                print_command(query, diff)
        # If dry, print commands
        else:
            if not query.get("already_granted") or print_skipped:
                print_command(query, diff, dry=True)


cli.add_command(spec_test)  # type: ignore
