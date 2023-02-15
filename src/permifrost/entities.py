from typing import Any, Dict, List, Set, Tuple, TypedDict

from permifrost.error import SpecLoadingError
from permifrost.logger import GLOBAL_LOGGER as logger
from permifrost.types import PermifrostSpecSchema


class EntitySchema(TypedDict):
    databases: Set[str]
    database_refs: Set[str]
    shared_databases: Set[str]
    schema_refs: Set[str]
    table_refs: Set[str]
    tables_by_database: Dict
    roles: Set[str]
    role_refs: Set[str]
    users: Set[str]
    warehouses: Set[str]
    warehouse_refs: Set[str]
    integrations: Set[str]
    integration_refs: Set[str]
    require_owner: bool


class EntityGenerator:
    def __init__(self, spec: PermifrostSpecSchema):
        self.spec = spec
        self.entities: EntitySchema = {
            "databases": set(),
            "database_refs": set(),
            "shared_databases": set(),
            "schema_refs": set(),
            "table_refs": set(),
            "tables_by_database": dict(),
            "roles": set(),
            "role_refs": set(),
            "users": set(),
            "warehouses": set(),
            "warehouse_refs": set(),
            "integrations": set(),
            "integration_refs": set(),
            "require_owner": False,
        }
        self.error_messages: List[str] = []

    def inspect_entities(self) -> EntitySchema:
        """
        Inspect a valid spec and make sure that no logic errors exist.

        e.g. a role granted to a user not defined in roles
             or a user given access to a database not defined in databases
        """
        self.generate()
        self.error_messages.extend(self.ensure_valid_entity_names(self.entities))

        self.error_messages.extend(
            self.ensure_valid_spec_for_conditional_settings(self.entities)
        )

        self.error_messages.extend(self.ensure_valid_references(self.entities))

        if self.error_messages:
            raise SpecLoadingError("\n".join(self.error_messages))

        return self.entities

    @staticmethod
    def filter_grouped_entities_by_type(grouped_entities: List, type: str) -> List:
        """
        Takes a list of grouped entities and filters them for a particular entity_type

        For example:
            filter_grouped_entities_by_type(grouped_entities, 'role') ->
                {'accountadmin', 'demo', 'securityadmin', 'sysadmin', 'useradmin'}

        """
        filtered_entities = [
            entries for entity_type, entries in grouped_entities if entity_type == type
        ]

        # Avoid returning a nested list if there are entities
        if not filtered_entities:
            return filtered_entities
        else:
            return filtered_entities[0]

    def group_table_by_database(self):
        tables_by_database = {}
        for table in self.entities["table_refs"]:
            db_name = table.split(".")[0]
            if db_name not in tables_by_database.keys():
                tables_by_database[db_name] = {table}
            else:
                tables_by_database[db_name].add(table)
        self.entities["tables_by_database"] = tables_by_database

    @staticmethod
    def group_spec_by_type(spec: PermifrostSpecSchema) -> List[Tuple[str, Any]]:
        """
        Takes as input the Permifrost Spec and converts it to a grouped list
        For example:
            [('databases',
                [{'demo': {'shared': False}}]),
            ('roles',
                [
                  {'accountadmin':
                    {'warehouses': ['loading'],
                     'member_of': ['sysadmin', 'securityadmin']}},
                  {'securityadmin':
                    {'warehouses': ['loading'],
                    'member_of': ['useradmin']}},
                ]
            ...
            ]

        """
        entities_by_type = [
            (entity_type, entry)
            for entity_type, entry in spec.items()
            if entry and entity_type != "version"
        ]
        return entities_by_type

    def generate(self) -> EntitySchema:
        """
        Generate and return a dictionary with all the entities defined or
        referenced in the permissions specification file.

        The xxx_refs entities are referenced by various permissions.
        For example:
        'roles' --> All the roles defined in the spec
        'role_refs' --> All the roles referenced in a member_of permission
        'table_refs' --> All the tables referenced in read/write privileges
                         or in owns entries

        Returns a tuple (entities, error_messages) with all the entities defined
        in the spec and any errors found (e.g. a user not assigned their user role)
        """
        entities_by_type = self.group_spec_by_type(self.spec)
        self.generate_roles(
            self.filter_grouped_entities_by_type(entities_by_type, "roles")
        )
        self.generate_databases(
            self.filter_grouped_entities_by_type(entities_by_type, "databases")
        )
        self.generate_warehouses(
            self.filter_grouped_entities_by_type(entities_by_type, "warehouses")
        )
        self.generate_integrations(
            self.filter_grouped_entities_by_type(entities_by_type, "integrations")
        )
        self.generate_users(
            self.filter_grouped_entities_by_type(entities_by_type, "users")
        )

        # Filter the owner requirement and set it to True or False
        require_owner = [
            entry
            for entity_type, entry in entities_by_type
            if entity_type == "require-owner"
        ]
        self.entities["require_owner"] = require_owner == [True]

        self.generate_implicit_refs_from_schemas()
        self.generate_implicit_refs_from_tables()
        self.group_table_by_database()
        # Add implicit references to DBs and Schemas.
        #  e.g. RAW.MYSCHEMA.TABLE references also DB RAW and Schema MYSCHEMA

        return self.entities

    def generate_implicit_refs_from_schemas(self):
        """Adds implicit database refs from schemas"""
        for schema in self.entities["schema_refs"]:
            name_parts = schema.split(".")
            if name_parts[0] != "*":
                self.entities["database_refs"].add(name_parts[0])

    def generate_implicit_refs_from_tables(self):
        """Adds implicit db/schema refs from tables"""
        for table in self.entities["table_refs"]:
            name_parts = table.split(".")
            if name_parts[0] != "*":
                self.entities["database_refs"].add(name_parts[0])

            if name_parts[1] != "*":
                self.entities["schema_refs"].add(f"{name_parts[0]}.{name_parts[1]}")

    def ensure_valid_entity_names(self, entities: EntitySchema) -> List[str]:
        """
        Check that all entity names are valid.

        Returns a list with all the errors found.
        """
        error_messages = []

        for db in entities["databases"].union(entities["database_refs"]):
            name_parts = db.split(".")
            if not len(name_parts) == 1:
                error_messages.append(
                    f"Name error: Not a valid database name: {db}"
                    " (Proper definition: DB)"
                )

        for schema in entities["schema_refs"]:
            name_parts = schema.split(".")
            if (not len(name_parts) == 2) or (name_parts[0] == "*"):
                error_messages.append(
                    f"Name error: Not a valid schema name: {schema}"
                    " (Proper definition: DB.[SCHEMA | *])"
                )

        for table in entities["table_refs"]:
            name_parts = table.split(".")
            if (not len(name_parts) == 3) or (name_parts[0] == "*"):
                error_messages.append(
                    f"Name error: Not a valid table name: {table}"
                    " (Proper definition: DB.[SCHEMA | *].[TABLE | *])"
                )
            elif name_parts[1] == "*" and name_parts[2] != "*":
                error_messages.append(
                    f"Name error: Not a valid table name: {table}"
                    " (Can't have a Table name after selecting all schemas"
                    " with *: DB.SCHEMA.[TABLE | *])"
                )

        return error_messages

    def ensure_valid_references(self, entities: EntitySchema) -> List[str]:
        """
        Make sure that all references are well defined.

        Returns a list with all the errors found.
        """
        error_messages = []

        # Check that all the referenced entities are also defined
        for database in entities["database_refs"]:
            if database not in entities["databases"]:
                error_messages.append(
                    f"Reference error: Database {database} is referenced "
                    "in the spec but not defined"
                )

        for role in entities["role_refs"]:
            if role not in entities["roles"] and role != "*":
                error_messages.append(
                    f"Reference error: Role {role} is referenced in the "
                    "spec but not defined"
                )

        for warehouse in entities["warehouse_refs"]:
            if warehouse not in entities["warehouses"]:
                error_messages.append(
                    f"Reference error: Warehouse {warehouse} is referenced "
                    "in the spec but not defined"
                )

        for integration in entities["integration_refs"]:
            if integration not in entities["integrations"]:
                error_messages.append(
                    f"Reference error: Integration {integration} is referenced "
                    "in the spec but not defined"
                )

        return error_messages

    def ensure_valid_spec_for_conditional_settings(
        self, entities: EntitySchema
    ) -> List[str]:
        """
        Make sure that the spec is valid based on conditional settings such as require-owner
        """
        error_messages = []

        if entities["require_owner"]:
            error_messages.extend(self.check_entities_define_owner())

        return error_messages

    def check_entities_define_owner(self) -> List[str]:
        error_messages = []

        entities_by_type = [
            (entity_type, entry)
            for entity_type, entry in self.spec.items()
            if entry
            and entity_type
            in ["databases", "roles", "users", "warehouses", "integrations"]
        ]

        for entity_type, entry in entities_by_type:
            for entity_dict in entry:  # type: ignore
                for entity_name, config in entity_dict.items():
                    if "owner" not in config.keys():
                        error_messages.append(
                            f"Spec Error: Owner not defined for {entity_type} {entity_name} and require-owner is set!"
                        )

        return error_messages

    def generate_warehouses(self, warehouse_list: List[Dict[str, Dict]]) -> None:
        for warehouse_entry in warehouse_list:
            for warehouse_name, _ in warehouse_entry.items():
                self.entities["warehouses"].add(warehouse_name)

    def generate_integrations(self, integration_list: List[Dict[str, Dict]]) -> None:
        for integration_entry in integration_list:
            for integration_name, _ in integration_entry.items():
                self.entities["integrations"].add(integration_name)

    def generate_databases(self, db_list: List[Dict[str, Dict]]) -> None:
        for db_entry in db_list:
            for db_name, config in db_entry.items():
                self.entities["databases"].add(db_name)
                if "shared" in config:
                    if type(config["shared"]) == bool:
                        if config["shared"]:
                            self.entities["shared_databases"].add(db_name)
                    else:
                        logger.debug(
                            "`shared` for database {} must be boolean, skipping Role Reference generation.".format(
                                db_name
                            )
                        )

    def generate_member_of_roles(self, config, role_name):
        try:
            if isinstance(config["member_of"], dict):
                for member_role in config["member_of"].get("include", []):
                    self.entities["roles"].add(member_role)
                for member_role in config["member_of"].get("exclude", []):
                    self.entities["roles"].add(member_role)

            if isinstance(config["member_of"], list):
                for member_role in config["member_of"]:
                    self.entities["roles"].add(member_role)
        except KeyError:
            logger.debug(
                "`member_of` not found for role {}, skipping Role Reference generation.".format(
                    role_name
                )
            )

    def generate_warehouse_roles(self, config, role_name):
        try:
            for warehouse in config["warehouses"]:
                self.entities["warehouse_refs"].add(warehouse)
        except KeyError:
            logger.debug(
                "`warehouses` not found for role {}, skipping Warehouse Reference generation.".format(
                    role_name
                )
            )

    def generate_integration_roles(self, config, role_name):
        try:
            for integration in config["integrations"]:
                self.entities["integration_refs"].add(integration)
        except KeyError:
            logger.debug(
                "`Integration` not found for role {}, skipping Integration Reference generation.".format(
                    role_name
                )
            )

    def generate_database_roles(self, config, role_name):
        try:
            for schema in config["privileges"]["databases"]["read"]:
                self.entities["database_refs"].add(schema)
        except KeyError:
            logger.debug(
                "`privileges.databases.read` not found for role {}, skipping Database Reference generation.".format(
                    role_name
                )
            )
        try:
            for schema in config["privileges"]["databases"]["write"]:
                self.entities["database_refs"].add(schema)
        except KeyError:
            logger.debug(
                "`privileges.databases.write` not found for role {}, skipping Database Reference generation.".format(
                    role_name
                )
            )

    def generate_read_write_database_names(self, config):
        read_databases = (
            config.get("privileges", {}).get("databases", {}).get("read", [])
        )

        write_databases = (
            config.get("privileges", {}).get("databases", {}).get("write", [])
        )
        return (read_databases, write_databases)

    def generate_schema_roles(self, config, role_name):
        read_databases, write_databases = self.generate_read_write_database_names(
            config
        )

        try:
            for schema in config["privileges"]["schemas"]["read"]:
                self.entities["schema_refs"].add(schema)
                schema_db = schema.split(".")[0]
                if schema_db not in read_databases:
                    self.error_messages.append(
                        f"Privilege Error: Database {schema_db} referenced in "
                        "schema read privileges but not in database privileges "
                        f"for role {role_name}"
                    )
        except KeyError:
            logger.debug(
                "`privileges.schemas.read` not found for role {}, skipping Schema Reference generation.".format(
                    role_name
                )
            )

        try:
            for schema in config["privileges"]["schemas"]["write"]:
                self.entities["schema_refs"].add(schema)
                schema_db = schema.split(".")[0]
                if schema_db not in write_databases + read_databases:
                    self.error_messages.append(
                        f"Privilege Error: Database {schema_db} referenced in "
                        "schema write privileges but not in database privileges "
                        f"for role {role_name}"
                    )
        except KeyError:
            logger.debug(
                "`privileges.schemas.write` not found for role {}, skipping Schema Reference generation.".format(
                    role_name
                )
            )

    def generate_table_roles(self, config, role_name):
        read_databases, write_databases = self.generate_read_write_database_names(
            config
        )

        try:
            for table in config["privileges"]["tables"]["read"]:
                self.entities["table_refs"].add(table)
                table_db = table.split(".")[0]
                if table_db not in read_databases:
                    self.error_messages.append(
                        f"Privilege Error: Database {table_db} referenced in "
                        "table read privileges but not in database privileges "
                        f"for role {role_name}"
                    )
        except KeyError:
            logger.debug(
                "`privileges.tables.read` not found for role {}, skipping Table Reference generation.".format(
                    role_name
                )
            )

        try:
            for table in config["privileges"]["tables"]["write"]:
                self.entities["table_refs"].add(table)
                table_db = table.split(".")[0]
                if table_db not in write_databases + read_databases:
                    self.error_messages.append(
                        f"Privilege Error: Database {table_db} referenced in "
                        "table write privileges but not in database privileges "
                        f"for role {role_name}"
                    )
        except KeyError:
            logger.debug(
                "`privileges.tables.write` not found for role {}, skipping Table Reference generation.".format(
                    role_name
                )
            )

    def generate_ownership_roles(self, config, role_name):
        try:
            for schema in config["owns"]["databases"]:
                self.entities["database_refs"].add(schema)
        except KeyError:
            logger.debug(
                "`owns.databases` not found for role {}, skipping Database Reference generation.".format(
                    role_name
                )
            )

        try:
            for schema in config["owns"]["schemas"]:
                self.entities["schema_refs"].add(schema)
        except KeyError:
            logger.debug(
                "`owns.schemas` not found for role {}, skipping Schema Reference generation.".format(
                    role_name
                )
            )

        try:
            for table in config["owns"]["tables"]:
                self.entities["table_refs"].add(table)
        except KeyError:
            logger.debug(
                "`owns.tables` not found for role {}, skipping Table Reference generation.".format(
                    role_name
                )
            )

    def generate_roles(self, role_list):  # noqa
        """
        Generate all of the role entities.
        Also can populate the role_refs, database_refs,
        schema_refs, table_refs, warehouse_refs & integration_refs
        """

        for role_entry in role_list:
            for role_name, config in role_entry.items():
                self.entities["roles"].add(role_name)
                self.generate_member_of_roles(config, role_name)
                self.generate_warehouse_roles(config, role_name)
                self.generate_integration_roles(config, role_name)
                self.generate_database_roles(config, role_name)
                self.generate_schema_roles(config, role_name)
                self.generate_table_roles(config, role_name)
                self.generate_ownership_roles(config, role_name)

    def generate_user_fn(self, config, key, ref, user_name):
        if key in config:
            for item in config[key]:
                self.entities[ref].add(item)
        else:
            logger.debug(
                "`{}` not found for user {}, skipping Role Reference generation.".format(
                    key, user_name
                )
            )

    def generate_users(self, user_list):
        """
        Generate all of the user entities.
        Also can populate the role_refs, database_refs, schema_refs & table_refs
        """

        for user_entry in user_list:
            for user_name, config in user_entry.items():
                self.entities["users"].add(user_name)
                self.generate_user_fn(config, "member_of", "role_refs", user_name)
                self.generate_user_fn(
                    config.get("owns", {}), "database", "database_refs", user_name
                )
                self.generate_user_fn(
                    config.get("owns", {}), "schemas", "schema_refs", user_name
                )
                self.generate_user_fn(
                    config.get("owns", {}), "tables", "table_refs", user_name
                )
