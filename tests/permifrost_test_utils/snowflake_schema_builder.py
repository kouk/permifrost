import os
import json


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

        if len(self.warehouses) > 0:
            spec_yaml.append("warehouses:")
        for warehouse in self.warehouses:
            spec_yaml.extend([f"  - {warehouse['name']}:", "      size: x-small"])
            if warehouse["owner"] is not None:
                spec_yaml.append(f"      owner: {warehouse['owner']}")

        if len(self.roles) > 0:
            spec_yaml.append("roles:")
        for role in self.roles:
            spec_yaml.extend([f"  - {role['name']}:", "      member_of:"])
            spec_yaml.extend([f"        - {member}" for member in role["member_of"]])
            if role["owner"] is not None:
                spec_yaml.append(f"      owner: {role['owner']}")

        if len(self.dbs) > 0:
            spec_yaml.append("databases:")
        for db in self.dbs:
            spec_yaml.extend([f"  - {db['name']}:", "      shared: no"])
            if db["owner"] is not None:
                spec_yaml.append(f"      owner: {db['owner']}")

        if len(self.users) > 0:
            spec_yaml.append("users:")
        for user in self.users:
            spec_yaml.extend([f"  - {user['name']}:", "      can_login: yes"])
            if user["owner"] is not None:
                spec_yaml.append(f"      owner: {user['owner']}")

        spec_yaml.append("")
        return str.join("\n", spec_yaml)

    def set_version(self, version):
        self.version = f'version: "{version}"'
        return self

    def add_warehouse(self, name="testwarehouse", owner=None):
        self.warehouses.append({"name": name, "owner": owner})
        return self

    def add_role(self, name="testrole", owner=None, member_of=["testrole"]):
        self.roles.append({"name": name, "owner": owner, "member_of": member_of})
        return self

    def add_user(self, name="testusername", owner=None):
        self.users.append({"name": name, "owner": owner})
        return self

    def add_db(self, name="testdb", owner=None):
        self.dbs.append({"name": name, "owner": owner})
        return self

    def add_setting(self, name, value):
        self.settings.append({"name": name, "value": value})
        return self

    def require-owner(self):
        self.add_setting("require-owner", "true")
        return self

    def build_from_file(self, schemas_dir, file_name):
        schema_path = os.path.join(schemas_dir, file_name)
        with open(schema_path, "r") as fd:
            schema_file_data = json.load(fd)
        return schema_file_data
