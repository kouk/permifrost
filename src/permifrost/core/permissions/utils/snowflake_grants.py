import logging
import re
from typing import Any, Dict, List, Optional, Set, Tuple

from permifrost.core.permissions.utils.snowflake_connector import SnowflakeConnector

GRANT_ROLE_TEMPLATE = "GRANT ROLE {role_name} TO {type} {entity_name}"

REVOKE_ROLE_TEMPLATE = "REVOKE ROLE {role_name} FROM {type} {entity_name}"

GRANT_PRIVILEGES_TEMPLATE = (
    "GRANT {privileges} ON {resource_type} {resource_name} TO ROLE {role}"
)

REVOKE_PRIVILEGES_TEMPLATE = (
    "REVOKE {privileges} ON {resource_type} {resource_name} FROM ROLE {role}"
)

GRANT_FUTURE_PRIVILEGES_TEMPLATE = "GRANT {privileges} ON FUTURE {resource_type}s IN {grouping_type} {grouping_name} TO ROLE {role}"

REVOKE_FUTURE_PRIVILEGES_TEMPLATE = "REVOKE {privileges} ON FUTURE {resource_type}s IN {grouping_type} {grouping_name} FROM ROLE {role}"

ALTER_USER_TEMPLATE = "ALTER USER {user_name} SET {privileges}"

GRANT_OWNERSHIP_TEMPLATE = (
    "GRANT OWNERSHIP"
    " ON {resource_type} {resource_name}"
    " TO ROLE {role_name} COPY CURRENT GRANTS"
)


class SnowflakeGrantsGenerator:
    def __init__(
        self,
        grants_to_role: Dict,
        roles_granted_to_user: Dict[str, List[str]],
        ignore_memberships: Optional[bool] = False,
    ) -> None:
        """
        Initializes a grants generator, used to generate SQL for generating grants

        grants_to_role: a dict, mapping role to grants where role is a string
            and grants is a dictionary of privileges to entities.
            e.g. {'functional_role': {'create schema': {'database': ['database_1', 'database_2']}, ...}}

        roles_granted_to_user: a dict, mapping the user to a list of roles.,
            e.g. {'user_name': ['role_1', 'role_2']

        ignore_memberships: bool, whether to skip role grant/revoke of memberships

        """
        self.grants_to_role = grants_to_role
        self.roles_granted_to_user = roles_granted_to_user
        self.ignore_memberships = ignore_memberships

    def is_granted_privilege(
        self, role: str, privilege: str, entity_type: str, entity_name: str
    ) -> bool:
        """
        Check if <role> has been granted the privilege <privilege> on entity type
        <entity_type> with name <entity_name>. First checks if it is a future grant
        since snowflaky will format the future grants wrong - i.e. <table> is a part
        of the fully qualified name for a future table grant.

        For example:
        is_granted_privilege('reporter', 'usage', 'database', 'analytics') -> True
        means that role reporter has been granted the privilege to use the
        Database ANALYTICS on the Snowflake server.
        """
        future = True if re.search(r"<(table|view|schema)>", entity_name) else False

        grants = (
            self.grants_to_role.get(role, {}).get(privilege, {}).get(entity_type, [])
        )

        if future and entity_name in grants:
            return True

        if not future and SnowflakeConnector.snowflaky(entity_name) in grants:
            return True

        return False

    def _generate_member_lists(self, config: Dict) -> Tuple[List[str], List[str]]:
        """
        Generate a tuple with the member_include_list (e.g. roles that should be granted)
        and member_exclude_list (e.g. roles that should not be granted)

        config: the subtree for the entity as specified in the spec

        Returns: A tuple of two lists with the roles/users to include and exclude:
            (member_include_list, member_exclude_list)
        """
        member_include_list = []
        member_exclude_list = []

        if isinstance(config.get("member_of", []), dict):
            member_include_list = config.get("member_of", {}).get("include", [])
            member_exclude_list = config.get("member_of", {}).get("exclude", [])
        elif isinstance(config.get("member_of", []), list):
            member_include_list = config.get("member_of", [])

        return (member_include_list, member_exclude_list)

    def _generate_member_star_lists(self, all_entities: List, entity: str) -> List[str]:
        """
        Generates the member include list when a * privilege is granted

        all_entities: a List of all entities defined in the spec
        entity: the entity to generate the list for

        Returns: a list of all roles to include for the entity
        """
        conn = SnowflakeConnector()
        show_roles = conn.show_roles()
        member_include_list = [
            role for role in show_roles if role in all_entities and role != entity
        ]
        return member_include_list

    def _generate_sql_commands_for_member_of_list(
        self, member_of_list: List[str], entity: str, entity_type: str
    ) -> List[Dict]:
        """For a given member_of list and entity, generate the SQL commands
        to grant the entity privileges for every member_role in the member_of list

        member_of_list: List of roles to generate sql commands for
        entity: the user or role to grant permissions for
        entity_type: the type of enttiy, either "users" or "roles"

        returns:  a List of SQL Commands
        """
        if entity_type == "users":
            grant_type = "user"
        elif entity_type == "roles":
            grant_type = "role"
        else:
            raise ValueError("grant_type must be either 'users' or 'roles'")

        sql_commands = []
        for member_role in member_of_list:
            granted_role = SnowflakeConnector.snowflaky(member_role)
            already_granted = False
            if (
                entity_type == "users"
                and granted_role in self.roles_granted_to_user[entity]
            ) or (
                entity_type == "roles"
                and self.is_granted_privilege(entity, "usage", "role", member_role)
            ):
                already_granted = True

            sql_commands.append(
                {
                    "already_granted": already_granted,
                    "sql": GRANT_ROLE_TEMPLATE.format(
                        role_name=SnowflakeConnector.snowflaky(member_role),
                        type=grant_type,
                        entity_name=SnowflakeConnector.snowflaky(entity),
                    ),
                }
            )
        return sql_commands

    def _generate_revoke_sql_commands_for_user(
        self, username: str, member_of_list: List[str]
    ) -> List[Dict]:
        """For a given user, generate the SQL commands to revoke privileges
        to any roles not defined in the member of list
        """
        sql_commands = []
        for granted_role in self.roles_granted_to_user[username]:
            if granted_role not in member_of_list:
                sql_commands.append(
                    {
                        "already_granted": False,
                        "sql": REVOKE_ROLE_TEMPLATE.format(
                            role_name=SnowflakeConnector.snowflaky(granted_role),
                            type="user",
                            entity_name=SnowflakeConnector.snowflaky(username),
                        ),
                    }
                )

        return sql_commands

    def _generate_revoke_sql_commands_for_role(self, rolename, member_of_list):
        sql_commands = []
        for granted_role in (
            self.grants_to_role.get(rolename, {}).get("usage", {}).get("role", [])
        ):
            if granted_role not in member_of_list:
                sql_commands.append(
                    {
                        "already_granted": False,
                        "sql": REVOKE_ROLE_TEMPLATE.format(
                            role_name=SnowflakeConnector.snowflaky(granted_role),
                            type="role",
                            entity_name=SnowflakeConnector.snowflaky(rolename),
                        ),
                    }
                )
        return sql_commands

    def generate_grant_roles(
        self,
        entity_type: str,
        entity: str,
        config: Dict[str, Any],
        all_entities: Optional[List] = None,
    ) -> List[Dict]:
        """
        Generate the GRANT statements for both roles and users.

        entity_type: "users" or "roles"
        entity: the name of the entity (e.g. "yannis" or "reporter")
        config: the subtree for the entity as specified in the spec
        all_entities: all roles defined in spec

        Returns the SQL commands generated as a list
        """
        sql_commands: List[Dict] = []

        if self.ignore_memberships:
            return sql_commands

        member_include_list, member_exclude_list = self._generate_member_lists(config)

        if len(member_include_list) == 1 and member_include_list[0] == "*":
            if not all_entities:
                raise ValueError(
                    "Cannot generate grant roles if all_entities not provided"
                )
            member_include_list = self._generate_member_star_lists(all_entities, entity)

        member_of_list = [
            role for role in member_include_list if role not in member_exclude_list
        ]

        sql_commands.extend(
            self._generate_sql_commands_for_member_of_list(
                member_of_list, entity, entity_type
            )
        )
        if entity_type == "users":
            sql_commands.extend(
                self._generate_revoke_sql_commands_for_user(entity, member_of_list)
            )
        if entity_type == "roles":
            sql_commands.extend(
                self._generate_revoke_sql_commands_for_role(entity, member_of_list)
            )

        return sql_commands

    def _generate_database_commands(self, role, config, shared_dbs, spec_dbs):
        databases = {
            "read": config.get("privileges", {}).get("databases", {}).get("read", []),
            "write": config.get("privileges", {}).get("databases", {}).get("write", []),
        }

        if len(databases.get("read", "")) == 0:
            logging.debug(
                "`privileges.databases.read` not found for role {}, skipping generation of database read level GRANT statements.".format(
                    role
                )
            )

        if len(databases.get("write", "")) == 0:
            logging.debug(
                "`privileges.databases.write` not found for role {}, skipping generation of database write level GRANT statements.".format(
                    role
                )
            )

        database_commands = self.generate_database_grants(
            role=role, databases=databases, shared_dbs=shared_dbs, spec_dbs=spec_dbs
        )
        return database_commands

    def _generate_schema_commands(self, role, config, shared_dbs, spec_dbs):
        schemas = {
            "read": config.get("privileges", {}).get("schemas", {}).get("read", []),
            "write": config.get("privileges", {}).get("schemas", {}).get("write", []),
        }

        if len(schemas.get("read", "")) == 0:
            logging.debug(
                "`privileges.schemas.read` not found for role {}, skipping generation of schemas read level GRANT statements.".format(
                    role
                )
            )

        if len(schemas.get("write", "")) == 0:
            logging.debug(
                "`privileges.schemas.write` not found for role {}, skipping generation of schemas write level GRANT statements.".format(
                    role
                )
            )

        schema_commands = self.generate_schema_grants(
            role=role, schemas=schemas, shared_dbs=shared_dbs, spec_dbs=spec_dbs
        )
        return schema_commands

    def _generate_table_commands(self, role, config, shared_dbs, spec_dbs):
        tables = {
            "read": config.get("privileges", {}).get("tables", {}).get("read", []),
            "write": config.get("privileges", {}).get("tables", {}).get("write", []),
        }

        if len(tables.get("read", "")) == 0:
            logging.debug(
                "`privileges.tables.read` not found for role {}, skipping generation of tables read level GRANT statements.".format(
                    role
                )
            )

        if len(tables.get("write", "")) == 0:
            logging.debug(
                "`privileges.tables.write` not found for role {}, skipping generation of tables write level GRANT statements.".format(
                    role
                )
            )

        table_commands = self.generate_table_and_view_grants(
            role=role, tables=tables, shared_dbs=shared_dbs, spec_dbs=spec_dbs
        )
        return table_commands

    def generate_grant_privileges_to_role(
        self, role: str, config: Dict[str, Any], shared_dbs: Set, spec_dbs: Set
    ) -> List[Dict]:
        """
        Generate all the privilege granting and revocation
        statements for a role so Snowflake matches the spec.

        Most of the SQL command that will be generated are privileges granted to
        roles and this function orchestrates the whole process.

        role: the name of the role (e.g. "loader" or "reporter") the privileges
              are granted to and revoked from
        config: the subtree for the role as specified in the spec
        shared_dbs: a set of all the shared databases defined in the spec.
                    Used down the road by generate_database_grants() to also grant
                    "imported privileges" when access is granted to a shared DB.
        spec_dbs: a set of all the databases defined in the spec. This is used in revoke
                  commands to validate revocations are only for spec'd databases

        Returns the SQL commands generated as a list
        """
        sql_commands: List[Dict] = []

        try:
            warehouses = config["warehouses"]
            new_commands = self.generate_warehouse_grants(
                role=role, warehouses=warehouses
            )
            sql_commands.extend(new_commands)
        except KeyError:
            logging.debug(
                "`warehouses` not found for role {}, skipping generation of Warehouse GRANT statements.".format(
                    role
                )
            )
        database_commands = self._generate_database_commands(
            role, config, shared_dbs, spec_dbs
        )
        sql_commands.extend(database_commands)

        schema_commands = self._generate_schema_commands(
            role, config, shared_dbs, spec_dbs
        )
        sql_commands.extend(schema_commands)

        table_commands = self._generate_table_commands(
            role, config, shared_dbs, spec_dbs
        )
        sql_commands.extend(table_commands)

        return sql_commands

    def generate_warehouse_grants(
        self, role: str, warehouses: list
    ) -> List[Dict[str, Any]]:
        """
        Generate the GRANT statements for Warehouse usage and operation.

        role: the name of the role the privileges are GRANTed to
        warehouses: list of warehouses for the specified role

        Returns the SQL command generated
        """
        sql_commands: List[Dict] = []

        for warehouse in warehouses:
            for priv in ["usage", "operate", "monitor"]:
                already_granted = self.is_granted_privilege(
                    role, priv, "warehouse", warehouse
                )

                sql_commands.append(
                    {
                        "already_granted": already_granted,
                        "sql": GRANT_PRIVILEGES_TEMPLATE.format(
                            privileges=priv,
                            resource_type="warehouse",
                            resource_name=SnowflakeConnector.snowflaky(warehouse),
                            role=SnowflakeConnector.snowflaky(role),
                        ),
                    }
                )

        for priv in ["usage", "operate", "monitor"]:
            for granted_warehouse in (
                self.grants_to_role.get(role, {}).get(priv, {}).get("warehouse", [])
            ):
                if granted_warehouse not in warehouses:
                    sql_commands.append(
                        {
                            "already_granted": False,
                            "sql": REVOKE_PRIVILEGES_TEMPLATE.format(
                                privileges=priv,
                                resource_type="warehouse",
                                resource_name=SnowflakeConnector.snowflaky(
                                    granted_warehouse
                                ),
                                role=SnowflakeConnector.snowflaky(role),
                            ),
                        }
                    )

        return sql_commands

    def generate_database_grants(
        self, role: str, databases: Dict[str, List], shared_dbs: Set, spec_dbs: Set
    ) -> List[Dict[str, Any]]:
        """
        Generate the GRANT and REVOKE statements for Databases
        to align Snowflake with the spec.

        role: the name of the role the privileges are GRANTed to
        databases: list of databases (e.g. "raw")
        shared_dbs: a set of all the shared databases defined in the spec.
        spec_dbs: a set of all the databases defined in the spec. This is used in revoke
                  commands to validate revocations are only for spec'd databases

        Returns the SQL commands generated as a list
        """
        sql_commands = []

        read_privileges = "usage"
        partial_write_privileges = "monitor, create schema"
        write_privileges = f"{read_privileges}, {partial_write_privileges}"

        for database in databases.get("read", []):
            already_granted = self.is_granted_privilege(
                role, "usage", "database", database
            )

            # If this is a shared database, we have to grant the "imported privileges"
            # privilege to the user and skip granting the specific permissions as
            # "Granting individual privileges on imported databases is not allowed."
            if database in shared_dbs:
                sql_commands.append(
                    {
                        "already_granted": already_granted,
                        "sql": GRANT_PRIVILEGES_TEMPLATE.format(
                            privileges="imported privileges",
                            resource_type="database",
                            resource_name=SnowflakeConnector.snowflaky(database),
                            role=SnowflakeConnector.snowflaky(role),
                        ),
                    }
                )
                continue

            sql_commands.append(
                {
                    "already_granted": already_granted,
                    "sql": GRANT_PRIVILEGES_TEMPLATE.format(
                        privileges=read_privileges,
                        resource_type="database",
                        resource_name=SnowflakeConnector.snowflaky(database),
                        role=SnowflakeConnector.snowflaky(role),
                    ),
                }
            )

        for database in databases.get("write", []):
            already_granted = (
                self.is_granted_privilege(role, "usage", "database", database)
                and self.is_granted_privilege(role, "monitor", "database", database)
                and self.is_granted_privilege(
                    role, "create schema", "database", database
                )
            )

            # If this is a shared database, we have to grant the "imported privileges"
            # privilege to the user and skip granting the specific permissions as
            # "Granting individual privileges on imported databases is not allowed."
            if database in shared_dbs:
                sql_commands.append(
                    {
                        "already_granted": already_granted,
                        "sql": GRANT_PRIVILEGES_TEMPLATE.format(
                            privileges="imported privileges",
                            resource_type="database",
                            resource_name=SnowflakeConnector.snowflaky(database),
                            role=SnowflakeConnector.snowflaky(role),
                        ),
                    }
                )
                continue

            sql_commands.append(
                {
                    "already_granted": already_granted,
                    "sql": GRANT_PRIVILEGES_TEMPLATE.format(
                        privileges=write_privileges,
                        resource_type="database",
                        resource_name=SnowflakeConnector.snowflaky(database),
                        role=SnowflakeConnector.snowflaky(role),
                    ),
                }
            )

        # REVOKES

        # The "Usage" privilege is consistent across read and write.
        # Compare granted usage to full read/write usage set
        # and revoke missing ones
        for granted_database in (
            self.grants_to_role.get(role, {}).get("usage", {}).get("database", [])
        ):
            # If it's a shared database, only revoke imported
            # We'll only know if it's a shared DB based on the spec
            all_databases = databases.get("read", []) + databases.get("write", [])
            if granted_database not in spec_dbs:
                # Skip revocation on database that are not defined in spec
                continue
            elif (
                granted_database not in all_databases and granted_database in shared_dbs
            ):
                sql_commands.append(
                    {
                        "already_granted": False,
                        "sql": REVOKE_PRIVILEGES_TEMPLATE.format(
                            privileges="imported privileges",
                            resource_type="database",
                            resource_name=SnowflakeConnector.snowflaky(
                                granted_database
                            ),
                            role=SnowflakeConnector.snowflaky(role),
                        ),
                    }
                )

            elif granted_database not in all_databases:
                sql_commands.append(
                    {
                        "already_granted": False,
                        "sql": REVOKE_PRIVILEGES_TEMPLATE.format(
                            privileges=read_privileges,
                            resource_type="database",
                            resource_name=SnowflakeConnector.snowflaky(
                                granted_database
                            ),
                            role=SnowflakeConnector.snowflaky(role),
                        ),
                    }
                )
        # Get all other write privilege dbs in case there are dbs where
        # usage was revoked but other write permissions still exist
        # This also preserves the case where somebody switches write access
        # for read access
        for granted_database in self.grants_to_role.get(role, {}).get(
            "monitor", {}
        ).get("database", []) + self.grants_to_role.get(role, {}).get(
            "create_schema", {}
        ).get(
            "database", []
        ):
            # If it's a shared database, only revoke imported
            # We'll only know if it's a shared DB based on the spec
            if (
                granted_database not in databases.get("write", [])
                and granted_database in shared_dbs
            ):
                sql_commands.append(
                    {
                        "already_granted": False,
                        "sql": REVOKE_PRIVILEGES_TEMPLATE.format(
                            privileges="imported privileges",
                            resource_type="database",
                            resource_name=SnowflakeConnector.snowflaky(
                                granted_database
                            ),
                            role=SnowflakeConnector.snowflaky(role),
                        ),
                    }
                )
            elif granted_database not in databases.get("write", []):
                sql_commands.append(
                    {
                        "already_granted": False,
                        "sql": REVOKE_PRIVILEGES_TEMPLATE.format(
                            privileges=partial_write_privileges,
                            resource_type="database",
                            resource_name=SnowflakeConnector.snowflaky(
                                granted_database
                            ),
                            role=SnowflakeConnector.snowflaky(role),
                        ),
                    }
                )

        return sql_commands

    def _generate_schema_read_grants(
        self, schemas, shared_dbs, role
    ) -> Tuple[List[Dict], List]:

        sql_commands = []
        read_grant_schemas = []
        read_privileges = "usage"

        for schema in schemas:
            # Split the schema identifier into parts {DB_NAME}.{SCHEMA_NAME}
            # so that we can check and use each one
            name_parts = schema.split(".")

            # Do nothing if this is a schema inside a shared database:
            # "Granting individual privileges on imported databases is not allowed."
            database = name_parts[0]
            if database in shared_dbs:
                continue

            conn = SnowflakeConnector()
            fetched_schemas = conn.full_schema_list(schema)
            read_grant_schemas.extend(fetched_schemas)

            if name_parts[1] == "*":
                # If <db_name>.* then you can grant future and add future schema to grant list
                future_schema = f"{database}.<schema>"
                read_grant_schemas.append(future_schema)

                schema_already_granted = self.is_granted_privilege(
                    role, read_privileges, "schema", future_schema
                )

                # Grant on FUTURE schemas
                sql_commands.append(
                    {
                        "already_granted": schema_already_granted,
                        "sql": GRANT_FUTURE_PRIVILEGES_TEMPLATE.format(
                            privileges=read_privileges,
                            resource_type="schema",
                            grouping_type="database",
                            grouping_name=SnowflakeConnector.snowflaky(database),
                            role=SnowflakeConnector.snowflaky(role),
                        ),
                    }
                )

            for db_schema in fetched_schemas:
                already_granted = False

                if self.is_granted_privilege(
                    role, read_privileges, "schema", db_schema
                ):
                    already_granted = True

                sql_commands.append(
                    {
                        "already_granted": already_granted,
                        "sql": GRANT_PRIVILEGES_TEMPLATE.format(
                            privileges=read_privileges,
                            resource_type="schema",
                            resource_name=SnowflakeConnector.snowflaky(db_schema),
                            role=SnowflakeConnector.snowflaky(role),
                        ),
                    }
                )
        return (sql_commands, read_grant_schemas)

    def _generate_schema_write_grants(
        self, schemas, shared_dbs, role
    ) -> Tuple[List[Dict], List]:
        sql_commands = []
        write_grant_schemas = []

        read_privileges = "usage"
        partial_write_privileges = (
            "monitor, create table,"
            " create view, create stage, create file format,"
            " create sequence, create function, create pipe"
        )
        write_privileges = f"{read_privileges}, {partial_write_privileges}"
        write_privileges_array = write_privileges.split(", ")

        for schema in schemas:
            # Split the schema identifier into parts {DB_NAME}.{SCHEMA_NAME}
            # so that we can check and use each one
            name_parts = schema.split(".")

            # Do nothing if this is a schema inside a shared database:
            # "Granting individual privileges on imported databases is not allowed."
            database = name_parts[0]
            if database in shared_dbs:
                continue

            conn = SnowflakeConnector()
            fetched_schemas = conn.full_schema_list(schema)
            write_grant_schemas.extend(fetched_schemas)

            if name_parts[1] == "*":
                # If <db_name>.* then you can grant future and add future schema to grant list
                future_schema = f"{database}.<schema>"
                write_grant_schemas.append(future_schema)

                already_granted = True

                for privilege in write_privileges_array:
                    # If any of the privileges are not granted, set already_granted to False
                    if not self.is_granted_privilege(
                        role, privilege, "schema", future_schema
                    ):
                        already_granted = False

                # Grant on FUTURE schemas
                sql_commands.append(
                    {
                        "already_granted": already_granted,
                        "sql": GRANT_FUTURE_PRIVILEGES_TEMPLATE.format(
                            privileges=write_privileges,
                            resource_type="schema",
                            grouping_type="database",
                            grouping_name=SnowflakeConnector.snowflaky(database),
                            role=SnowflakeConnector.snowflaky(role),
                        ),
                    }
                )

            for db_schema in fetched_schemas:
                already_granted = True

                for privilege in write_privileges_array:
                    # If any of the privileges are not granted, set already_granted to False
                    if not self.is_granted_privilege(
                        role, privilege, "schema", db_schema
                    ):
                        already_granted = False

                sql_commands.append(
                    {
                        "already_granted": already_granted,
                        "sql": GRANT_PRIVILEGES_TEMPLATE.format(
                            privileges=write_privileges,
                            resource_type="schema",
                            resource_name=SnowflakeConnector.snowflaky(db_schema),
                            role=SnowflakeConnector.snowflaky(role),
                        ),
                    }
                )
        return (sql_commands, write_grant_schemas)

    def _generate_schema_revokes(
        self, usage_schemas, all_grant_schemas, shared_dbs, spec_dbs, role
    ):
        sql_commands = []
        read_privileges = "usage"

        for granted_schema in usage_schemas:
            database_name = granted_schema.split(".")[0]
            future_schema_name = f"{database_name}.<schema>"
            if granted_schema not in all_grant_schemas and (
                database_name in shared_dbs or database_name not in spec_dbs
            ):
                # No privileges to revoke on imported db. Done at database level
                # Don't revoke on privileges on databases not defined in spec.
                continue
            elif (  # If future privilege is granted on snowflake but not in grant list
                granted_schema == future_schema_name
                and future_schema_name not in all_grant_schemas  #
            ):
                sql_commands.append(
                    {
                        "already_granted": False,
                        "sql": REVOKE_FUTURE_PRIVILEGES_TEMPLATE.format(
                            privileges=read_privileges,
                            resource_type="schema",
                            grouping_type="database",
                            grouping_name=SnowflakeConnector.snowflaky(database_name),
                            role=SnowflakeConnector.snowflaky(role),
                        ),
                    }
                )
            elif (
                granted_schema not in all_grant_schemas
                and future_schema_name not in all_grant_schemas
            ):
                # Covers case where schema is granted in Snowflake
                # But it's not in the grant list and it's not explicitly granted as a future grant
                sql_commands.append(
                    {
                        "already_granted": False,
                        "sql": REVOKE_PRIVILEGES_TEMPLATE.format(
                            privileges=read_privileges,
                            resource_type="schema",
                            resource_name=SnowflakeConnector.snowflaky(granted_schema),
                            role=SnowflakeConnector.snowflaky(role),
                        ),
                    }
                )

        return sql_commands

    # TODO: This method is too complex, consider refactoring
    def generate_schema_grants(
        self, role: str, schemas: Dict[str, List], shared_dbs: Set, spec_dbs: Set
    ) -> List[Dict]:
        """
        Generate the GRANT and REVOKE statements for schemas
        including future grants.

        role: the name of the role the privileges are GRANTed to
        schemas: the name of the Schema (e.g. "raw.public", "raw.*")
        shared_dbs: a set of all the shared databases defined in the spec.
        spec_dbs: a set of all the databases defined in the spec. This is used in revoke
                  commands to validate revocations are only for spec'd databases

        Returns the SQL commands generated as a List
        """
        sql_commands = []

        # Schema lists to hold read/write grants. This is necessary
        # as the provided schemas are not the full list - we determine
        # the full list via full_schema_list and store in these variables
        read_grant_schemas = []
        write_grant_schemas = []

        partial_write_privileges = (
            "monitor, create table,"
            " create view, create stage, create file format,"
            " create sequence, create function, create pipe"
        )

        # Get Schema Read Commands
        read_schemas = schemas.get("read", [])
        read_commands, read_grants = self._generate_schema_read_grants(
            read_schemas, shared_dbs, role
        )
        sql_commands.extend(read_commands)
        read_grant_schemas.extend(read_grants)

        write_schemas = schemas.get("write", [])
        write_commands, write_grants = self._generate_schema_write_grants(
            write_schemas, shared_dbs, role
        )
        sql_commands.extend(write_commands)
        write_grant_schemas.extend(write_grants)

        # REVOKES

        # The "usage" privilege is consistent across read and write.
        # Compare granted usage to full read/write set and revoke missing ones
        usage_schemas = set(
            self.grants_to_role.get(role, {}).get("usage", {}).get("schema", [])
        )
        all_grant_schemas = read_grant_schemas + write_grant_schemas
        sql_commands.extend(
            self._generate_schema_revokes(
                usage_schemas, all_grant_schemas, shared_dbs, spec_dbs, role
            )
        )

        # Get all other write privilege schemas in case there are schemas where
        # usage was revoked but other write permissions still exist
        # This also preserves the case where somebody switches write access
        # for read access
        other_privileges = [
            "monitor",
            "create table",
            "create view",
            "create stage",
            "create file format",
            "create sequence",
            "create pipe",
        ]

        other_schema_grants = list()
        for privilege in other_privileges:
            other_schema_grants.extend(
                self.grants_to_role.get(role, {}).get(privilege, {}).get("schema", [])
            )

        for granted_schema in other_schema_grants:
            database_name = granted_schema.split(".")[0]
            future_schema_name = f"{database_name}.<schema>"
            if granted_schema not in write_grant_schemas and (
                database_name in shared_dbs or database_name not in spec_dbs
            ):
                # No privileges to revoke on imported db. Done at database level
                # Don't revoke on privileges on databases not defined in spec.
                continue
            elif (  # If future privilege is granted but not in grant list
                granted_schema == future_schema_name
                and future_schema_name not in write_grant_schemas
            ):
                sql_commands.append(
                    {
                        "already_granted": False,
                        "sql": REVOKE_FUTURE_PRIVILEGES_TEMPLATE.format(
                            privileges=partial_write_privileges,
                            resource_type="schema",
                            grouping_type="database",
                            grouping_name=SnowflakeConnector.snowflaky(database_name),
                            role=SnowflakeConnector.snowflaky(role),
                        ),
                    }
                )
            elif (
                granted_schema not in write_grant_schemas
                and future_schema_name not in write_grant_schemas
            ):
                # Covers case where schema is granted and it's not explicitly granted as a future grant
                sql_commands.append(
                    {
                        "already_granted": False,
                        "sql": REVOKE_PRIVILEGES_TEMPLATE.format(
                            privileges=partial_write_privileges,
                            resource_type="schema",
                            resource_name=SnowflakeConnector.snowflaky(granted_schema),
                            role=SnowflakeConnector.snowflaky(role),
                        ),
                    }
                )

        return sql_commands

    def _generate_table_read_grants(self, conn, tables, shared_dbs, role):
        sql_commands = []
        read_grant_tables_full = []
        read_grant_views_full = []
        read_privileges = "select"

        for table in tables:
            # Split the table identifier into parts {DB_NAME}.{SCHEMA_NAME}.{TABLE_NAME}
            # so that we can check and use each one
            name_parts = table.split(".")

            # Do nothing if this is a table inside a shared database:
            # "Granting individual privileges on imported databases is not allowed."
            if name_parts[0] in shared_dbs:
                continue

            # Gather the tables/views that privileges will be granted to
            # for the given table schema
            read_grant_tables = []
            read_grant_views = []

            # List of all tables/views in schema for validation
            read_table_list = []
            read_view_list = []

            fetched_schemas = conn.full_schema_list(f"{name_parts[0]}.{name_parts[1]}")

            # For future grants at the database level
            future_database_table = "{database}.<table>".format(database=name_parts[0])
            future_database_view = "{database}.<view>".format(database=name_parts[0])

            table_already_granted = self.is_granted_privilege(
                role, read_privileges, "table", future_database_table
            )

            view_already_granted = self.is_granted_privilege(
                role, read_privileges, "view", future_database_view
            )

            read_grant_tables_full.append(future_database_table)

            if name_parts[1] == "*" and name_parts[2] == "*":
                sql_commands.append(
                    {
                        "already_granted": table_already_granted,
                        "sql": GRANT_FUTURE_PRIVILEGES_TEMPLATE.format(
                            privileges=read_privileges,
                            resource_type="table",
                            grouping_type="database",
                            grouping_name=SnowflakeConnector.snowflaky(name_parts[0]),
                            role=SnowflakeConnector.snowflaky(role),
                        ),
                    }
                )

            read_grant_views_full.append(future_database_view)

            if name_parts[1] == "*" and name_parts[2] == "*":
                sql_commands.append(
                    {
                        "already_granted": view_already_granted,
                        "sql": GRANT_FUTURE_PRIVILEGES_TEMPLATE.format(
                            privileges=read_privileges,
                            resource_type="view",
                            grouping_type="database",
                            grouping_name=SnowflakeConnector.snowflaky(name_parts[0]),
                            role=SnowflakeConnector.snowflaky(role),
                        ),
                    }
                )

            for schema in fetched_schemas:
                # Fetch all tables from Snowflake for each schema and add
                # to the read_tables_list[] and read_views_list[] variables.
                # This is so we can check that a table given in the config
                # Is valid
                read_table_list.extend(conn.show_tables(schema=schema))
                read_view_list.extend(conn.show_views(schema=schema))

            if name_parts[2] == "*":
                # If <schema_name>.* then you add all tables to grant list and then grant future
                # If *.* was provided then we're still ok as the full_schema_list
                # Would fetch all schemas and we'd still iterate through each

                # If == * then append all tables to both
                # the grant list AND the full grant list
                read_grant_tables.extend(read_table_list)
                read_grant_views.extend(read_view_list)
                read_grant_tables_full.extend(read_table_list)
                read_grant_views_full.extend(read_view_list)

                for schema in fetched_schemas:
                    # Adds the future grant table format to the granted lists
                    future_table = f"{schema}.<table>"
                    future_view = f"{schema}.<view>"
                    read_grant_tables_full.append(future_table)
                    read_grant_views_full.append(future_view)

                    table_already_granted = False

                    if self.is_granted_privilege(
                        role, read_privileges, "table", future_table
                    ):
                        table_already_granted = True

                    # Grant future on all tables
                    sql_commands.append(
                        {
                            "already_granted": table_already_granted,
                            "sql": GRANT_FUTURE_PRIVILEGES_TEMPLATE.format(
                                privileges=read_privileges,
                                resource_type="table",
                                grouping_type="schema",
                                grouping_name=SnowflakeConnector.snowflaky(schema),
                                role=SnowflakeConnector.snowflaky(role),
                            ),
                        }
                    )

                    view_already_granted = False

                    if self.is_granted_privilege(
                        role, read_privileges, "view", future_view
                    ):
                        view_already_granted = True

                    # Grant future on all views
                    sql_commands.append(
                        {
                            "already_granted": view_already_granted,
                            "sql": GRANT_FUTURE_PRIVILEGES_TEMPLATE.format(
                                privileges=read_privileges,
                                resource_type="view",
                                grouping_type="schema",
                                grouping_name=SnowflakeConnector.snowflaky(schema),
                                role=SnowflakeConnector.snowflaky(role),
                            ),
                        }
                    )

            # TODO Future elif to have partial table name

            else:
                # Else the table passed is a single entity
                # Check that it's valid and add to list
                if table in read_table_list:
                    read_grant_tables = [table]
                    read_grant_tables_full.append(table)
                if table in read_view_list:
                    read_grant_views = [table]
                    read_grant_views_full.append(table)

            # Grant privileges to all tables flagged for granting.
            # We have this loop b/c we explicitly grant to each table
            # Instead of doing grant to all tables/views in schema
            for db_table in read_grant_tables:
                already_granted = False
                if self.is_granted_privilege(role, read_privileges, "table", db_table):
                    already_granted = True

                sql_commands.append(
                    {
                        "already_granted": already_granted,
                        "sql": GRANT_PRIVILEGES_TEMPLATE.format(
                            privileges=read_privileges,
                            resource_type="table",
                            resource_name=SnowflakeConnector.snowflaky(db_table),
                            role=SnowflakeConnector.snowflaky(role),
                        ),
                    }
                )

            # Grant privileges to all flagged views
            for db_view in read_grant_views:
                already_granted = False
                if self.is_granted_privilege(role, read_privileges, "view", db_view):
                    already_granted = True

                sql_commands.append(
                    {
                        "already_granted": already_granted,
                        "sql": GRANT_PRIVILEGES_TEMPLATE.format(
                            privileges=read_privileges,
                            resource_type="view",
                            resource_name=SnowflakeConnector.snowflaky(db_view),
                            role=SnowflakeConnector.snowflaky(role),
                        ),
                    }
                )

        return (sql_commands, read_grant_tables_full, read_grant_views_full)

    #  TODO: This method remains complex, could use extra refactoring
    def _generate_table_write_grants(self, conn, tables, shared_dbs, role):  # noqa
        sql_commands, write_grant_tables_full, write_grant_views_full = [], [], []

        read_privileges = "select"
        write_partial_privileges = "insert, update, delete, truncate, references"
        write_privileges = f"{read_privileges}, {write_partial_privileges}"
        write_privileges_array = write_privileges.split(", ")

        for table in tables:
            # Split the table identifier into parts {DB_NAME}.{SCHEMA_NAME}.{TABLE_NAME}
            #  so that we can check and use each one
            name_parts = table.split(".")

            # Do nothing if this is a table inside a shared database:
            #  "Granting individual privileges on imported databases is not allowed."
            if name_parts[0] in shared_dbs:
                continue

            # Gather the tables/views that privileges will be granted to
            write_grant_tables = []
            write_grant_views = []

            # List of all tables/views in schema
            write_table_list = []
            write_view_list = []

            fetched_schemas = conn.full_schema_list(f"{name_parts[0]}.{name_parts[1]}")

            # For future grants at the database level
            future_database_table = "{database}.<table>".format(database=name_parts[0])
            future_database_view = "{database}.<view>".format(database=name_parts[0])

            table_already_granted = False
            view_already_granted = False

            if self.is_granted_privilege(
                role, write_privileges, "table", future_database_table
            ):
                table_already_granted = True

            write_grant_tables_full.append(future_database_table)

            if name_parts[1] == "*" and name_parts[2] == "*":
                sql_commands.append(
                    {
                        "already_granted": table_already_granted,
                        "sql": GRANT_FUTURE_PRIVILEGES_TEMPLATE.format(
                            privileges=write_privileges,
                            resource_type="table",
                            grouping_type="database",
                            grouping_name=SnowflakeConnector.snowflaky(name_parts[0]),
                            role=SnowflakeConnector.snowflaky(role),
                        ),
                    }
                )

            if self.is_granted_privilege(
                role, write_privileges, "view", future_database_view
            ):
                view_already_granted = True

            write_grant_views_full.append(future_database_view)

            if name_parts[1] == "*" and name_parts[2] == "*":
                sql_commands.append(
                    {
                        "already_granted": view_already_granted,
                        "sql": GRANT_FUTURE_PRIVILEGES_TEMPLATE.format(
                            privileges=write_privileges,
                            resource_type="view",
                            grouping_type="database",
                            grouping_name=SnowflakeConnector.snowflaky(name_parts[0]),
                            role=SnowflakeConnector.snowflaky(role),
                        ),
                    }
                )

            for schema in fetched_schemas:
                # Fetch all tables from Snowflake for each schema and add
                # to the write_tables_list[] and write_views_list[] variables.
                # This is so we can check that a table given in the config
                # Is valid
                write_table_list.extend(conn.show_tables(schema=schema))
                write_view_list.extend(conn.show_views(schema=schema))

            if name_parts[1] != "*" and name_parts[2] == "*":
                # If <schema_name>.* then you add all tables to grant list and then grant future
                # If *.* was provided then we're still ok as the full_schema_list
                # Would fetch all schemas and we'd still iterate through each

                # If == * then append all tables to both
                # the grant list AND the full grant list
                write_grant_tables.extend(write_table_list)
                write_grant_views.extend(write_view_list)
                write_grant_tables_full.extend(write_table_list)
                write_grant_views_full.extend(write_view_list)

                for schema in fetched_schemas:
                    # Adds the future grant table format to the granted lists
                    future_table = f"{schema}.<table>"
                    future_view = f"{schema}.<view>"
                    write_grant_tables_full.append(future_table)
                    write_grant_views_full.append(future_view)

                    table_already_granted = True

                    for privilege in write_privileges_array:
                        # If any of the privileges are not granted, set already_granted to False
                        if not self.is_granted_privilege(
                            role, privilege, "table", future_table
                        ):
                            table_already_granted = False

                    # Grant future on all tables
                    sql_commands.append(
                        {
                            "already_granted": table_already_granted,
                            "sql": GRANT_FUTURE_PRIVILEGES_TEMPLATE.format(
                                privileges=write_privileges,
                                resource_type="table",
                                grouping_type="schema",
                                grouping_name=SnowflakeConnector.snowflaky(schema),
                                role=SnowflakeConnector.snowflaky(role),
                            ),
                        }
                    )

                    view_already_granted = not self.is_granted_privilege(
                        role, "select", "view", future_view
                    )

                    # Grant future on all views. Select is only privilege
                    sql_commands.append(
                        {
                            "already_granted": view_already_granted,
                            "sql": GRANT_FUTURE_PRIVILEGES_TEMPLATE.format(
                                privileges="select",
                                resource_type="view",
                                grouping_type="schema",
                                grouping_name=SnowflakeConnector.snowflaky(schema),
                                role=SnowflakeConnector.snowflaky(role),
                            ),
                        }
                    )

            # TODO Future elif to have partial table name

            else:
                # Only one table/view to be granted permissions to
                if table in write_table_list:
                    write_grant_tables = [table]
                    write_grant_tables_full.append(table)
                if table in write_view_list:
                    write_grant_views = [table]
                    write_grant_views_full.append(table)

            # Grant privileges to all tables flagged for granting.
            # We have this loop b/c we explicitly grant to each table
            # Instead of doing grant to all tables/views in schema
            for db_table in write_grant_tables:

                table_already_granted = True
                for privilege in write_privileges_array:
                    # If any of the privileges are not granted, set already_granted to False
                    if not self.is_granted_privilege(
                        role, privilege, "table", db_table
                    ):
                        table_already_granted = False

                sql_commands.append(
                    {
                        "already_granted": table_already_granted,
                        "sql": GRANT_PRIVILEGES_TEMPLATE.format(
                            privileges=write_privileges,
                            resource_type="table",
                            resource_name=SnowflakeConnector.snowflaky(db_table),
                            role=SnowflakeConnector.snowflaky(role),
                        ),
                    }
                )

            # Grant privileges to all views in that schema.
            # Select is the only schemaObjectPrivilege for views
            # https://docs.snowflake.net/manuals/sql-reference/sql/grant-privilege.html
            for db_view in write_grant_views:
                already_granted = False
                if self.is_granted_privilege(role, "select", "view", db_view):
                    already_granted = True

                sql_commands.append(
                    {
                        "already_granted": already_granted,
                        "sql": GRANT_PRIVILEGES_TEMPLATE.format(
                            privileges="select",
                            resource_type="view",
                            resource_name=SnowflakeConnector.snowflaky(db_view),
                            role=SnowflakeConnector.snowflaky(role),
                        ),
                    }
                )

        return (sql_commands, write_grant_tables_full, write_grant_views_full)

    def _generate_revoke_privs(
        self,
        role,
        shared_dbs,
        spec_dbs,
        all_grant_tables,
        all_grant_views,
        write_grant_tables_full,
    ):
        read_privileges = "select"
        write_partial_privileges = "insert, update, delete, truncate, references"

        sql_commands = []
        # REVOKES

        # Read Privileges
        # The "select" privilege is consistent across read and write.
        # Compare granted usage to full read/write set and revoke missing ones
        for granted_table in list(
            set(self.grants_to_role.get(role, {}).get("select", {}).get("table", []))
        ):
            table_split = granted_table.split(".")
            database_name = table_split[0]

            # For future grants at the database level
            if len(table_split) == 2:
                future_table = f"{database_name}.<table>"
                grouping_type = "database"
                grouping_name = database_name
            else:
                schema_name = table_split[1]
                future_table = f"{database_name}.{schema_name}.<table>"
                grouping_type = "schema"
                grouping_name = f"{database_name}.{schema_name}"

            if granted_table not in all_grant_tables and (
                database_name in shared_dbs or database_name not in spec_dbs
            ):
                # No privileges to revoke on imported db. Done at database level
                # Don't revoke on privileges on databases not defined in spec.
                continue
            elif (  # If future privilege is granted in Snowflake but not in grant list
                granted_table == future_table and future_table not in all_grant_tables
            ):
                sql_commands.append(
                    {
                        "already_granted": False,
                        "sql": REVOKE_FUTURE_PRIVILEGES_TEMPLATE.format(
                            privileges=read_privileges,
                            resource_type="table",
                            grouping_type=grouping_type,
                            grouping_name=SnowflakeConnector.snowflaky(grouping_name),
                            role=SnowflakeConnector.snowflaky(role),
                        ),
                    }
                )
            elif (
                granted_table not in all_grant_tables
                and future_table not in all_grant_tables
            ):
                # Covers case where table is granted in Snowflake
                # But it's not in the grant list and it's not explicitly granted as a future grant
                sql_commands.append(
                    {
                        "already_granted": False,
                        "sql": REVOKE_PRIVILEGES_TEMPLATE.format(
                            privileges=read_privileges,
                            resource_type="table",
                            resource_name=SnowflakeConnector.snowflaky(granted_table),
                            role=SnowflakeConnector.snowflaky(role),
                        ),
                    }
                )

        # SELECT is the only privilege for views so this covers both the read
        # and write case since we have "all_grant_views" defined.
        for granted_view in list(
            set(self.grants_to_role.get(role, {}).get("select", {}).get("view", []))
        ):
            view_split = granted_view.split(".")
            database_name = view_split[0]

            # For future grants at the database level
            # TODO: Not sure what is happenign with table split here, it
            # may posibly be unbound
            if len(view_split) == 2 or (
                len(table_split) == 3 and table_split[1] == "*"
            ):
                future_view = f"{database_name}.<view>"
                grouping_type = "database"
                grouping_name = database_name
            else:
                schema_name = view_split[1]
                future_view = f"{database_name}.{schema_name}.<view>"
                grouping_type = "schema"
                grouping_name = f"{database_name}.{schema_name}"

            if granted_view not in all_grant_views and (
                database_name in shared_dbs or database_name not in spec_dbs
            ):
                # No privileges to revoke on imported db. Done at database level
                # Don't revoke on privileges on databases not defined in spec.
                continue
            elif granted_view == future_view and future_view not in all_grant_views:
                # If future privilege is granted in Snowflake but not in grant list
                sql_commands.append(
                    {
                        "already_granted": False,
                        "sql": REVOKE_FUTURE_PRIVILEGES_TEMPLATE.format(
                            privileges=read_privileges,
                            resource_type="view",
                            grouping_type=grouping_type,
                            grouping_name=SnowflakeConnector.snowflaky(grouping_name),
                            role=SnowflakeConnector.snowflaky(role),
                        ),
                    }
                )
            elif (
                granted_view not in all_grant_views
                and future_view not in all_grant_views
            ):
                # Covers case where view is granted in Snowflake
                # But it's not in the grant list and it's not explicitly granted as a future grant
                sql_commands.append(
                    {
                        "already_granted": False,
                        "sql": REVOKE_PRIVILEGES_TEMPLATE.format(
                            privileges=read_privileges,
                            resource_type="view",
                            resource_name=SnowflakeConnector.snowflaky(granted_view),
                            role=SnowflakeConnector.snowflaky(role),
                        ),
                    }
                )

        # Write Privileges
        # Only need to revoke write privileges for tables since SELECT is the
        # only privilege available for views
        for granted_table in list(
            set(
                self.grants_to_role.get(role, {}).get("insert", {}).get("table", [])
                + self.grants_to_role.get(role, {}).get("update", {}).get("table", [])
                + self.grants_to_role.get(role, {}).get("delete", {}).get("table", [])
                + self.grants_to_role.get(role, {}).get("truncate", {}).get("table", [])
                + self.grants_to_role.get(role, {})
                .get("references", {})
                .get("table", [])
            )
        ):
            table_split = granted_table.split(".")
            database_name = table_split[0]

            # For future grants at the database level
            if len(table_split) == 2 or (
                len(table_split) == 3 and table_split[1] == "*"
            ):
                future_table = f"{database_name}.<table>"
                grouping_type = "database"
                grouping_name = database_name
            else:
                schema_name = table_split[1]
                future_table = f"{database_name}.{schema_name}.<table>"
                grouping_type = "schema"
                grouping_name = f"{database_name}.{schema_name}"

            if granted_table not in write_grant_tables_full and (
                database_name in shared_dbs or database_name not in spec_dbs
            ):
                # No privileges to revoke on imported db. Done at database level
                # Don't revoke on privileges on databases not defined in spec.
                continue
            elif (
                granted_table == future_table
                and future_table not in write_grant_tables_full
            ):
                # If future privilege is granted in Snowflake but not in grant list
                sql_commands.append(
                    {
                        "already_granted": False,
                        "sql": REVOKE_FUTURE_PRIVILEGES_TEMPLATE.format(
                            privileges=write_partial_privileges,
                            resource_type="table",
                            grouping_type=grouping_type,
                            grouping_name=SnowflakeConnector.snowflaky(grouping_name),
                            role=SnowflakeConnector.snowflaky(role),
                        ),
                    }
                )
            elif (
                granted_table not in write_grant_tables_full
                and future_table not in write_grant_tables_full
            ):
                # Covers case where table is granted in Snowflake
                # But it's not in the grant list and it's not explicitly granted as a future grant
                sql_commands.append(
                    {
                        "already_granted": False,
                        "sql": REVOKE_PRIVILEGES_TEMPLATE.format(
                            privileges=write_partial_privileges,
                            resource_type="table",
                            resource_name=SnowflakeConnector.snowflaky(granted_table),
                            role=SnowflakeConnector.snowflaky(role),
                        ),
                    }
                )

        return sql_commands

    def generate_table_and_view_grants(
        self, role: str, tables: Dict[str, List], shared_dbs: Set, spec_dbs: Set
    ) -> List[Dict]:
        """
        Generate the GRANT and REVOKE statements for tables and views
        including future grants.

        role: the name of the role the privileges are GRANTed to
        table: the name of the TABLE/VIEW (e.g. "raw.public.my_table")
        shared_dbs: a set of all the shared databases defined in the spec.
        spec_dbs: a set of all the databases defined in the spec. This is used in revoke
                  commands to validate revocations are only for spec'd databases

        Returns the SQL commands generated as a List
        """
        sql_commands = []

        # These are necessary as the provided tables/views are not the full list
        # we determine the full list for granting via full_schema_list()
        # and store in these variables
        read_grant_tables_full = []
        read_grant_views_full = []

        write_grant_tables_full = []
        write_grant_views_full = []

        conn = SnowflakeConnector()

        read_tables = tables.get("read", [])
        read_command, read_table, read_views = self._generate_table_read_grants(
            conn, read_tables, shared_dbs, role
        )
        sql_commands.extend(read_command)
        read_grant_tables_full.extend(read_table)
        read_grant_views_full.extend(read_views)

        write_tables = tables.get("write", [])
        write_command, write_table, write_views = self._generate_table_write_grants(
            conn, write_tables, shared_dbs, role
        )
        sql_commands.extend(write_command)
        write_grant_tables_full.extend(write_table)
        write_grant_views_full.extend(write_views)

        all_grant_tables = read_grant_tables_full + write_grant_tables_full
        all_grant_views = read_grant_views_full + write_grant_views_full

        sql_commands.extend(
            self._generate_revoke_privs(
                role,
                shared_dbs,
                spec_dbs,
                all_grant_tables,
                all_grant_views,
                write_grant_tables_full,
            )
        )
        return sql_commands

    def generate_alter_user(self, user: str, config: Dict[str, Any]) -> List[Dict]:
        """
        Generate the ALTER statements for USERs.

        user: the name of the USER
        config: the subtree for the user as specified in the spec

        Returns the SQL commands generated as a List
        """
        sql_commands: List[Any] = []
        alter_privileges: List[Any] = []
        if self.ignore_memberships:
            return sql_commands

        if "can_login" in config:
            if config.get("can_login"):
                alter_privileges.append("DISABLED = FALSE")
            else:
                alter_privileges.append("DISABLED = TRUE")

        if alter_privileges:
            sql_commands.append(
                {
                    "already_granted": False,
                    "sql": ALTER_USER_TEMPLATE.format(
                        user_name=SnowflakeConnector.snowflaky(user),
                        privileges=", ".join(alter_privileges),
                    ),
                }
            )

        return sql_commands

    # TODO: This method is too complex, consider refactoring
    def generate_grant_ownership(  # noqa
        self, role: str, config: Dict[str, Any]
    ) -> List[Dict]:
        """
        Generate the GRANT ownership statements for databases, schemas and tables.

        role: the name of the role (e.g. "loader") ownership will be GRANTed to
        config: the subtree for the role as specified in the spec

        Returns the SQL commands generated as a List
        """
        sql_commands = []

        try:
            for database in config["owns"]["databases"]:
                if self.is_granted_privilege(role, "ownership", "database", database):
                    already_granted = True
                else:
                    already_granted = False

                sql_commands.append(
                    {
                        "already_granted": already_granted,
                        "sql": GRANT_OWNERSHIP_TEMPLATE.format(
                            resource_type="database",
                            resource_name=SnowflakeConnector.snowflaky(database),
                            role_name=SnowflakeConnector.snowflaky(role),
                        ),
                    }
                )
        except KeyError:
            logging.debug(
                "`owns.databases` not found for role {}, skipping generation of database ownership statements.".format(
                    role
                )
            )

        try:
            for schema in config["owns"]["schemas"]:
                name_parts = schema.split(".")
                info_schema = f"{name_parts[0]}.information_schema"

                schemas = []

                if name_parts[1] == "*":
                    conn = SnowflakeConnector()
                    db_schemas = conn.show_schemas(name_parts[0])

                    for db_schema in db_schemas:
                        if db_schema != info_schema:
                            schemas.append(db_schema)
                else:
                    schemas = [schema]

                for db_schema in schemas:
                    if self.is_granted_privilege(
                        role, "ownership", "schema", db_schema
                    ):
                        already_granted = True
                    else:
                        already_granted = False

                    sql_commands.append(
                        {
                            "already_granted": already_granted,
                            "sql": GRANT_OWNERSHIP_TEMPLATE.format(
                                resource_type="schema",
                                resource_name=SnowflakeConnector.snowflaky(db_schema),
                                role_name=SnowflakeConnector.snowflaky(role),
                            ),
                        }
                    )
        except KeyError:
            logging.debug(
                "`owns.schemas` not found for role {}, skipping generation of SCHEMA ownership statements.".format(
                    role
                )
            )

        try:
            # Gather the tables that ownership will be granted to
            tables = []

            for table in config["owns"]["tables"]:
                name_parts = table.split(".")
                info_schema = f"{name_parts[0]}.information_schema"

                if name_parts[2] == "*":
                    schemas = []
                    conn = SnowflakeConnector()

                    if name_parts[1] == "*":
                        db_schemas = conn.show_schemas(name_parts[0])

                        for schema in db_schemas:
                            if schema != info_schema:
                                schemas.append(schema)
                    else:
                        schemas = [f"{name_parts[0]}.{name_parts[1]}"]

                    for schema in schemas:
                        tables.extend(conn.show_tables(schema=schema))
                else:
                    tables = [table]

            # And then grant ownership to all tables
            for db_table in tables:
                if self.is_granted_privilege(role, "ownership", "table", db_table):
                    already_granted = True
                else:
                    already_granted = False

                sql_commands.append(
                    {
                        "already_granted": already_granted,
                        "sql": GRANT_OWNERSHIP_TEMPLATE.format(
                            resource_type="table",
                            resource_name=SnowflakeConnector.snowflaky(db_table),
                            role_name=SnowflakeConnector.snowflaky(role),
                        ),
                    }
                )
        except KeyError:
            logging.debug(
                "`owns.tables` not found for role {}, skipping generation of TABLE ownership statements.".format(
                    role
                )
            )

        return sql_commands
