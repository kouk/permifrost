import os
import json
from typing import List, Optional


class SnowflakeSchemaBuilder:
    """
    Basic builder class for creating spec_files.  Useful for small, focused tests to generate
    a spec on the fly, for example in a parameterized test.
    """

    def __init__(self):
        self.version = ""
        self.warehouses = []
        self.roles = []
        self.dbs = []
        self.users = []
        self.settings = []

    def build(self):
        spec_yaml = []
        spec_yaml.append(f"{self.version}")

        for setting in self.settings:
            spec_yaml.append(f"{setting['name']}: {setting['value']}")

        if len(self.dbs) > 0:
            spec_yaml.append("databases:")
        for db in self.dbs:
            spec_yaml.extend([f"  - {db['name']}:", "      shared: no"])
            if db["owner"] is not None:
                spec_yaml.append(f"      owner: {db['owner']}")

        if len(self.roles) > 0:
            spec_yaml.append("roles:")
        for role in self.roles:
            if role["member_of_exclude"] and not role["member_of_include"]:
                raise KeyError(
                    "'member_of_exclude' requires 'member_of_include' to be defined"
                )
            elif (role["member_of_exclude"] or role["member_of_include"]) and role[
                "member_of"
            ]:
                raise KeyError(
                    "'member_of_include' and 'member_of_exclude' cannot be defined if 'member_of' is defined"
                )
            elif role["member_of_exclude"] and role["member_of_include"]:
                spec_yaml.extend(
                    [f"  - {role['name']}:", "      member_of:", "        include:"]
                )
                spec_yaml.extend(
                    [f"          - {member}" for member in role["member_of_include"]]
                )
                spec_yaml.extend(["        exclude:"])
                spec_yaml.extend(
                    [f"          - {member}" for member in role["member_of_exclude"]]
                )
            elif role["member_of_include"]:
                spec_yaml.extend(
                    [f"  - {role['name']}:", "      member_of:", "        include:"]
                )
                spec_yaml.extend(
                    [f"          - {member}" for member in role["member_of_include"]]
                )
            elif role["member_of"]:
                spec_yaml.extend([f"  - {role['name']}:", "      member_of:"])
                spec_yaml.extend(
                    [f"        - {member}" for member in role["member_of"]]
                )

            if role["owner"] is not None:
                spec_yaml.append(f"      owner: {role['owner']}")
            if role["tables"] != []:
                spec_yaml.extend(["      privileges:", "        tables:"])
                spec_yaml.extend(self._build_tables(role))
                spec_yaml.extend(["        schemas:"])
                spec_yaml.extend(self._build_schema_tables(role))
                spec_yaml.extend(["        databases:"])
                spec_yaml.extend(self._build_schema_databases(role))

        if len(self.users) > 0:
            spec_yaml.append("users:")
        for user in self.users:
            spec_yaml.extend([f"  - {user['name']}:", "      can_login: yes"])
            if user["owner"] is not None:
                spec_yaml.append(f"      owner: {user['owner']}")
        if len(self.warehouses) > 0:
            spec_yaml.append("warehouses:")

        for warehouse in self.warehouses:
            spec_yaml.extend([f"  - {warehouse['name']}:", "      size: x-small"])
            if warehouse["owner"] is not None:
                spec_yaml.append(f"      owner: {warehouse['owner']}")

        spec_yaml.append("")
        return str.join("\n", spec_yaml)

    def _build_tables(self, role):
        spec_yaml = []
        if role["permission_set"] == ["read"]:
            spec_yaml.extend(["          read:"])
            spec_yaml.extend(
                [
                    f"            - {full_table_name}"
                    for full_table_name in role["tables"]
                ]
            )
        elif role["permission_set"] == ["write"]:
            spec_yaml.extend(["          write:"])
            spec_yaml.extend(
                [
                    f"            - {full_table_name}"
                    for full_table_name in role["tables"]
                ]
            )
        elif role["permission_set"] == ["read", "write"] or role["permission_set"] == [
            "write",
            "read",
        ]:
            spec_yaml.extend(["          read:"])
            spec_yaml.extend(
                [
                    f"            - {full_table_name}"
                    for full_table_name in role["tables"]
                ]
            )
            spec_yaml.extend(["          write:"])
            spec_yaml.extend(
                [
                    f"            - {full_table_name}"
                    for full_table_name in role["tables"]
                ]
            )
        else:
            raise ValueError("Must set the permission_set for table generation")
        return spec_yaml

    def _build_table_schemas(self, role):
        """
        Generates the schema permissions sequentially from the table permissions
        """
        spec_yaml = []
        if role["permission_set"] == ["read"]:
            spec_yaml.extend(["          read:"])
            for full_table_name in role["tables"]:
                name_parts = full_table_name.split(".")
                database_name = name_parts[0] if 0 < len(name_parts) else None
                schema_name = name_parts[1] if 1 < len(name_parts) else None
                spec_yaml.extend([f"            - {database_name}.{schema_name}"])
        elif role["permission_set"] == ["write"]:
            spec_yaml.extend(["          write:"])
            for full_table_name in role["tables"]:
                name_parts = full_table_name.split(".")
                database_name = name_parts[0] if 0 < len(name_parts) else None
                schema_name = name_parts[1] if 1 < len(name_parts) else None
                spec_yaml.extend([f"            - {database_name}.{schema_name}"])
        elif role["permission_set"] == ["read", "write"] or role["permission_set"] == [
            "write",
            "read",
        ]:
            spec_yaml.extend(["          read:"])
            for full_table_name in role["tables"]:
                name_parts = full_table_name.split(".")
                database_name = name_parts[0] if 0 < len(name_parts) else None
                schema_name = name_parts[1] if 1 < len(name_parts) else None
                spec_yaml.extend([f"            - {database_name}.{schema_name}"])
            spec_yaml.extend(["          write:"])
            for full_table_name in role["tables"]:
                name_parts = full_table_name.split(".")
                database_name = name_parts[0] if 0 < len(name_parts) else None
                schema_name = name_parts[1] if 1 < len(name_parts) else None
                spec_yaml.extend([f"            - {database_name}.{schema_name}"])
        else:
            raise ValueError("Must set the permission_set for table generation")
        return spec_yaml

    def _build_table_databases(self, role):
        spec_yaml = []
        if role["permission_set"] == ["read"]:
            spec_yaml.extend(["          read:"])
            for full_table_name in role["tables"]:
                name_parts = full_table_name.split(".")
                database_name = name_parts[0] if 0 < len(name_parts) else None
                spec_yaml.extend([f"            - {database_name}"])
        elif role["permission_set"] == ["write"]:
            spec_yaml.extend(["          write:"])
            for full_table_name in role["tables"]:
                name_parts = full_table_name.split(".")
                database_name = name_parts[0] if 0 < len(name_parts) else None
                spec_yaml.extend([f"            - {database_name}"])
        elif role["permission_set"] == ["read", "write"] or role["permission_set"] == [
            "write",
            "read",
        ]:
            spec_yaml.extend(["          read:"])
            for full_table_name in role["tables"]:
                name_parts = full_table_name.split(".")
                database_name = name_parts[0] if 0 < len(name_parts) else None
                spec_yaml.extend([f"            - {database_name}"])
            spec_yaml.extend(["          write:"])
            for full_table_name in role["tables"]:
                name_parts = full_table_name.split(".")
                database_name = name_parts[0] if 0 < len(name_parts) else None
                spec_yaml.extend([f"            - {database_name}"])
        else:
            raise ValueError("Must set the permission_set for table generation")
        return spec_yaml

    def set_version(self, version):
        self.version = f'version: "{version}"'
        return self

    def add_warehouse(self, name="testwarehouse", owner=None):
        self.warehouses.append({"name": name, "owner": owner})
        return self

    def add_role(
        self,
        name: str = "testrole",
        owner: str = None,
        member_of: List[str] = None,
        tables: List[str] = [],
        permission_set: List[str] = None,
        member_of_include: Optional[List[str]] = None,
        member_of_exclude: Optional[List[str]] = None,
    ):
        """
        At a minimum, the 'member_of' key must be defined.
        It is recommended to set 'member_of' to 'testrole'
        """
        self.roles.append(
            {
                "name": name,
                "owner": owner,
                "member_of": member_of,
                "tables": tables,
                "permission_set": permission_set,
                "member_of_include": member_of_include,
                "member_of_exclude": member_of_exclude,
            }
        )
        return self

    def add_user(self, name: str = "testusername", owner: str = None):
        """
        Adds user to spec file
        """
        self.users.append({"name": name, "owner": owner})
        return self

    def add_db(self, name="testdb", owner=None):
        """
        Adds database to spec file with optional owner
        """
        self.dbs.append({"name": name, "owner": owner})
        return self

    def add_setting(self, name, value):
        self.settings.append({"name": name, "value": value})
        return self

    def require_owner(self):
        self.add_setting("require-owner", "true")
        return self

    def build_from_file(self, schemas_dir, file_name):
        schema_path = os.path.join(schemas_dir, file_name)
        with open(schema_path, "r") as fd:
            schema_file_data = json.load(fd)
        return schema_file_data
