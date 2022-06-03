from typing import Any, Dict, List, Optional, cast

import click

from permifrost.entities import EntityGenerator
from permifrost.error import SpecLoadingError
from permifrost.logger import GLOBAL_LOGGER as logger
from permifrost.snowflake_connector import SnowflakeConnector
from permifrost.snowflake_grants import SnowflakeGrantsGenerator
from permifrost.spec_file_loader import load_spec

VALIDATION_ERR_MSG = 'Spec error: {} "{}", field "{}": {}'


class SnowflakeSpecLoader:
    def __init__(
        self,
        spec_path: str,
        conn: SnowflakeConnector = None,
        roles: Optional[List[str]] = None,
        users: Optional[List[str]] = None,
        run_list: Optional[List[str]] = None,
        ignore_memberships: Optional[bool] = False,
    ) -> None:
        run_list = run_list or ["users", "roles"]
        # Load the specification file and check for (syntactical) errors
        click.secho("Loading spec file", fg="green")
        self.spec = load_spec(spec_path)

        # Generate the entities (e.g databases, schemas, users, etc) referenced
        #  by the spec file and make sure that no syntactical or reference errors
        #  exist (all referenced entities are also defined by the spec)
        click.secho("Checking spec file for errors", fg="green")
        entity_generator = EntityGenerator(spec=self.spec)
        self.entities = entity_generator.inspect_entities()

        # Connect to Snowflake to make sure that the current user has correct
        # permissions
        click.secho("Checking permissions on current snowflake connection", fg="green")
        self.check_permissions_on_snowflake_server(conn)

        # Connect to Snowflake to make sure that all entities defined in the
        # spec file are also defined in Snowflake (no missing databases, etc)
        click.secho(
            "Checking that all entities in the spec file are defined in Snowflake",
            fg="green",
        )
        self.check_entities_on_snowflake_server(conn)

        # Get the privileges granted to users and roles in the Snowflake account
        # Used in order to figure out which permissions in the spec file are
        #  new ones and which already exist (and there is no need to re-grant them)
        click.secho("Fetching granted privileges from Snowflake", fg="green")
        self.grants_to_role: Dict[str, Any] = {}
        self.roles_granted_to_user: Dict[str, Any] = {}
        self.get_privileges_from_snowflake_server(
            conn,
            roles=roles,
            users=users,
            run_list=run_list,
            ignore_memberships=ignore_memberships,
        )

    def check_permissions_on_snowflake_server(
        self, conn: SnowflakeConnector = None
    ) -> None:
        if conn is None:
            conn = SnowflakeConnector()
        error_messages = []

        click.secho(f"  Current user is: {conn.get_current_user()}.", fg="green")

        current_role = conn.get_current_role()
        if "securityadmin" != current_role:
            error_messages.append(
                "Current role is not securityadmin! "
                "Permifrost expects to run as securityadmin, please update your connection settings."
            )
        click.secho(f"  Current role is: {current_role}.", fg="green")

        if error_messages:
            raise SpecLoadingError("\n".join(error_messages))

    def check_warehouse_entities(self, conn):
        error_messages = []
        if len(self.entities["warehouses"]) > 0:
            warehouses = conn.show_warehouses()
            for warehouse in self.entities["warehouses"]:
                if warehouse not in warehouses:
                    error_messages.append(
                        f"Missing Entity Error: Warehouse {warehouse} was not found on"
                        " Snowflake Server. Please create it before continuing."
                    )
        else:
            logger.debug(
                "`warehouses` not found in spec, skipping SHOW WAREHOUSES call."
            )
        return error_messages

    def check_database_entities(self, conn):
        error_messages = []
        if len(self.entities["databases"]) > 0:
            databases = conn.show_databases()
            for db in self.entities["databases"]:
                if db not in databases:
                    error_messages.append(
                        f"Missing Entity Error: Database {db} was not found on"
                        " Snowflake Server. Please create it before continuing."
                    )
        else:
            logger.debug("`databases` not found in spec, skipping SHOW DATABASES call.")
        return error_messages

    def check_schema_ref_entities(self, conn):
        error_messages = []
        if len(self.entities["schema_refs"]) > 0:
            schemas = conn.show_schemas()
            for schema in self.entities["schema_refs"]:
                if "*" not in schema and schema not in schemas:
                    error_messages.append(
                        f"Missing Entity Error: Schema {schema} was not found on"
                        " Snowflake Server. Please create it before continuing."
                    )
        else:
            logger.debug("`schemas` not found in spec, skipping SHOW SCHEMAS call.")

        return error_messages

    def check_table_ref_entities(self, conn):
        error_messages = []
        if len(self.entities["table_refs"]) > 0:
            tables = conn.show_tables()
            views = conn.show_views()
            for table in self.entities["table_refs"]:
                if "*" not in table and table not in tables and table not in views:
                    error_messages.append(
                        f"Missing Entity Error: Table/View {table} was not found on"
                        " Snowflake Server. Please create it before continuing."
                    )
        else:
            logger.debug("`tables` not found in spec, skipping SHOW TABLES/VIEWS call.")
        return error_messages

    def check_role_entities(self, conn):
        error_messages = []
        if len(self.entities["roles"]) > 0:
            roles = conn.show_roles()
            for role in self.spec["roles"]:
                for role_name, config in role.items():
                    if role_name not in roles:
                        error_messages.append(
                            f"Missing Entity Error: Role {role_name} was not found on"
                            " Snowflake Server. Please create it before continuing."
                        )
                    elif "owner" in config:
                        owner_on_snowflake = roles[role_name]
                        owner_in_spec = config["owner"]
                        if owner_on_snowflake != owner_in_spec:
                            error_messages.append(
                                f"Role {role_name} has owner {owner_on_snowflake} on snowflake, "
                                f"but has owner {owner_in_spec} defined in the spec file."
                            )
        else:
            logger.debug("`roles` not found in spec, skipping SHOW ROLES call.")
        return error_messages

    def check_users_entities(self, conn):
        error_messages = []
        if len(self.entities["users"]) > 0:
            users = conn.show_users()
            for user in self.entities["users"]:
                if user not in users:
                    error_messages.append(
                        f"Missing Entity Error: User {user} was not found on"
                        " Snowflake Server. Please create it before continuing."
                    )
        else:
            logger.debug("`users` not found in spec, skipping SHOW USERS call.")
        return error_messages

    def check_entities_on_snowflake_server(  # noqa
        self, conn: SnowflakeConnector = None
    ) -> None:
        """
        Make sure that all [warehouses, dbs, schemas, tables, users, roles]
        referenced in the spec are defined in Snowflake.

        Raises a SpecLoadingError with all the errors found while checking
        Snowflake for missing entities.
        """
        error_messages = []

        if conn is None:
            conn = SnowflakeConnector()

        error_messages.extend(self.check_warehouse_entities(conn))
        error_messages.extend(self.check_database_entities(conn))
        error_messages.extend(self.check_schema_ref_entities(conn))
        error_messages.extend(self.check_table_ref_entities(conn))
        error_messages.extend(self.check_role_entities(conn))
        error_messages.extend(self.check_users_entities(conn))

        if error_messages:
            raise SpecLoadingError("\n".join(error_messages))

    def get_role_privileges_from_snowflake_server(
        self,
        conn: SnowflakeConnector,
        roles: Optional[List[str]] = None,
        ignore_memberships: Optional[bool] = False,
    ) -> None:
        future_grants: Dict[str, Any] = {}

        for database in self.entities["database_refs"]:
            logger.info(f"Fetching future grants for database: {database}")
            grant_results = conn.show_future_grants(database=database)
            grant_results = (
                {
                    role: role_grants
                    for role, role_grants in grant_results.items()
                    if role in roles
                }
                if roles
                else grant_results
            )

            for role in grant_results:
                for privilege in grant_results[role]:
                    for grant_on in grant_results[role][privilege]:
                        (
                            future_grants.setdefault(role, {})
                            .setdefault(privilege, {})
                            .setdefault(grant_on, [])
                            .extend(
                                self.filter_to_database_refs(
                                    grant_on=grant_on,
                                    filter_set=grant_results[role][privilege][grant_on],
                                )
                            )
                        )

            # Get all schemas in all ref'd databases. Not all schemas will be
            # ref'd in the spec.
            logger.info(f"Fetching all schemas for database {database}")
            for schema in conn.show_schemas(database=database):
                logger.info(f"Fetching all future grants for schema {schema}")
                grant_results = conn.show_future_grants(schema=schema)
                grant_results = (
                    {
                        role: role_grants
                        for role, role_grants in grant_results.items()
                        if role in roles
                    }
                    if roles
                    else grant_results
                )

                for role in grant_results:
                    for privilege in grant_results[role]:
                        for grant_on in grant_results[role][privilege]:
                            (
                                future_grants.setdefault(role, {})
                                .setdefault(privilege, {})
                                .setdefault(grant_on, [])
                                .extend(
                                    self.filter_to_database_refs(
                                        grant_on=grant_on,
                                        filter_set=grant_results[role][privilege][
                                            grant_on
                                        ],
                                    )
                                )
                            )

        for role in self.entities["roles"]:
            if (roles and role not in roles) or ignore_memberships:
                continue
            logger.info(f"Fetching all grants for role {role}")
            role_grants = conn.show_grants_to_role(role)
            for privilege in role_grants:
                for grant_on in role_grants[privilege]:
                    (
                        future_grants.setdefault(role, {})
                        .setdefault(privilege, {})
                        .setdefault(grant_on, [])
                        .extend(
                            self.filter_to_database_refs(
                                grant_on=grant_on,
                                filter_set=role_grants[privilege][grant_on],
                            )
                        )
                    )

        self.grants_to_role = future_grants

    def get_user_privileges_from_snowflake_server(
        self, conn: SnowflakeConnector, users: Optional[List[str]] = None
    ) -> None:
        user_entities = self.entities["users"]
        with click.progressbar(user_entities) as users_bar:
            for user in users_bar:
                if users and user not in users:
                    continue
                logger.info(f"Fetching user privileges for user: {user}")
                self.roles_granted_to_user[user] = conn.show_roles_granted_to_user(user)

    def get_privileges_from_snowflake_server(
        self,
        conn: SnowflakeConnector = None,
        roles: Optional[List[str]] = None,
        users: Optional[List[str]] = None,
        run_list: Optional[List[str]] = None,
        ignore_memberships: Optional[bool] = False,
    ) -> None:
        """
        Get the privileges granted to users and roles in the Snowflake account
        Gets the future privileges granted in all database and schema objects
        Consolidates role and future privileges into a single object for self.grants_to_role
        """
        run_list = run_list or ["users", "roles"]
        if conn is None:
            conn = SnowflakeConnector()

        if "users" in run_list and not ignore_memberships:
            logger.info("Fetching user privileges from Snowflake")
            self.get_user_privileges_from_snowflake_server(conn=conn, users=users)

        if "roles" in run_list:
            logger.info("Fetching role privileges from Snowflake")
            self.get_role_privileges_from_snowflake_server(
                conn=conn, roles=roles, ignore_memberships=ignore_memberships
            )

    def filter_to_database_refs(
        self, grant_on: str, filter_set: List[str]
    ) -> List[str]:
        """
        Filter out grants to databases that are not tracked in the configuration file
        :param grant_on: entity to be granted on. e.g. GRANT SOMETHING ON {DATABASE|ACCOUNT|WAREHOUSE|FILE FORMAT}...
        :param filter_set: list of strings to filter
        :return: list of strings with entities referring to non-tracked databases removed.
        """
        database_refs = self.entities["database_refs"]
        warehouse_refs = self.entities["warehouse_refs"]

        # Databases is the simple case. Just return items that are also in the database_refs list
        if grant_on == "database":
            return [item for item in filter_set if item in database_refs]
        # Warehouses are also a simple case.
        elif grant_on == "warehouse":
            return [item for item in filter_set if item in warehouse_refs]
        # Ignore account since currently account grants are not handled
        elif grant_on == "account":
            return filter_set
        else:
            # Everything else should be binary: it has a dot or it doesn't
            # List of strings with `.`s:
            #       i.e. database.schema.function_name
            #       Since we are excluding all references to non-tracked databases we can simply check the first
            #       segment of the string which represents the database. e.g. "database.item".split(".")[0]
            # List of strings that have no `.`'s:
            #       i.e. a role name `grant ownership on role role_name to role grantee`
            #       If it does not have a `.` then it can just be included since it isn't referencing a database
            return [
                item
                for item in filter_set
                if item and ("." not in item or item.split(".")[0] in database_refs)
            ]

    def generate_permission_queries(
        self,
        roles: Optional[List[str]] = None,
        users: Optional[List[str]] = None,
        run_list: Optional[List[str]] = None,
        ignore_memberships: Optional[bool] = False,
    ) -> List[Dict]:
        """
        Starting point to generate all the permission queries.

        For each entity type (e.g. user or role) that is affected by the spec,
        the proper sql permission queries are generated.

        Returns all the SQL commands as a list.
        """
        run_list = run_list or ["users", "roles"]
        sql_commands: List[Dict] = []

        generator = SnowflakeGrantsGenerator(
            self.grants_to_role,
            self.roles_granted_to_user,
            ignore_memberships=ignore_memberships,
        )

        click.secho("Generating permission Queries:", fg="green")

        # For each permission in the spec, check if we have to generate an
        #  SQL command granting that permission

        for entity_type, entry in self.spec.items():
            if entity_type in ["require-owner", "databases", "warehouses", "version"]:
                continue

            # Generate list of all entities (used for roles currently)
            entry = cast(List, entry)
            all_entities = [list(entity.keys())[0] for entity in entry]

            for entity_dict in entry:
                entity_configs = [
                    (entity_name, config)
                    for entity_name, config in entity_dict.items()
                    if config
                ]
                for entity_name, config in entity_configs:
                    if (
                        entity_type == "roles"
                        and "roles" in run_list
                        and (not roles or entity_name in roles)
                    ):
                        sql_commands.extend(
                            self.process_roles(
                                generator,
                                entity_type,
                                entity_name,
                                config,
                                all_entities,
                            )
                        )
                    elif (
                        entity_type == "users"
                        and "users" in run_list
                        and (not users or entity_name in users)
                    ):
                        sql_commands.extend(
                            self.process_users(
                                generator, entity_type, entity_name, config
                            )
                        )

        return self.remove_duplicate_queries(sql_commands)

    # TODO: These functions are part of a refactor of the previous module,
    # but this still requires a fair bit of attention to cleanup
    def process_roles(self, generator, entity_type, entity_name, config, all_entities):
        sql_commands = []
        click.secho(f"     Processing role {entity_name}", fg="green")
        sql_commands.extend(
            generator.generate_grant_roles(
                entity_type, entity_name, config, all_entities
            )
        )

        sql_commands.extend(generator.generate_grant_ownership(entity_name, config))

        sql_commands.extend(
            generator.generate_grant_privileges_to_role(
                entity_name,
                config,
                self.entities["shared_databases"],
                self.entities["databases"],
            )
        )
        return sql_commands

    def process_users(self, generator, entity_type, entity_name, config):
        sql_commands = []
        click.secho(f"     Processing user {entity_name}", fg="green")
        sql_commands.extend(generator.generate_alter_user(entity_name, config))

        sql_commands.extend(
            generator.generate_grant_roles(entity_type, entity_name, config)
        )
        return sql_commands

    @staticmethod
    def remove_duplicate_queries(sql_commands: List[Dict]) -> List[Dict]:
        grants = []
        revokes = []

        for i, command in reversed(list(enumerate(sql_commands))):
            # Find all "GRANT OWNERSHIP commands"
            if command["sql"].startswith("GRANT OWNERSHIP ON"):
                grant = command["sql"].split("TO ROLE", 1)[0]

                if grant in grants:
                    # If there is already a GRANT OWNERSHIP for the same
                    #  DB/SCHEMA/TABLE --> remove the one before it
                    #  (only keep the last one)
                    del sql_commands[i]
                else:
                    grants.append(grant)

            if command["sql"].startswith("REVOKE ALL"):
                revoke = command["sql"]
                if revoke in revokes:
                    # If there is already a REVOKE ALL for the same
                    #  DB/SCHEMA/TABLE --> remove the one before it
                    #  (only keep the last one)
                    del sql_commands[i]
                else:
                    revokes.append(revoke)

        return sql_commands
