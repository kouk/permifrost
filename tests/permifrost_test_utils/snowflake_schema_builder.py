class SnowflakeSchemaBuilder:
    """
    Basic builder class for creating spec_files.  Useful for small, focused tests to generate
    a spec on the fly, for example in a parameterized test.
    """

    def __init__(self):
        self.version = ""
        self.columns = []
        self.dbs = []
        self.masking_policies = []
        self.roles = []
        self.settings = []
        self.users = []
        self.warehouses = []

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

        if len(self.masking_policies) > 0:
            spec_yaml.append("masking_policies:")
        for policy in self.masking_policies:
            spec_yaml.extend([f"      - {policy['name']}:"])
            spec_yaml.extend([f"          input_type: {policy['input_type']}"])
            spec_yaml.extend([f"          return_value: {policy['return_value']}"])
            if policy["owner"] is not None:
                spec_yaml.append(f"      owner: {policy['owner']}")

        if len(self.columns) > 0:
            spec_yaml.append("columns:")
        for column in self.columns:
            spec_yaml.extend([f"  - {column['name']}:", "      masking_policies:"])
            spec_yaml.extend(
                [f"        - {policy}" for policy in column["masking_policies"]]
            )
            if column["owner"] is not None:
                spec_yaml.append(f"      owner: {column['owner']}")

        if len(self.roles) > 0:
            spec_yaml.append("roles:")
        for role in self.roles:
            spec_yaml.extend([f"  - {role['name']}:", "      member_of:"])
            spec_yaml.extend([f"        - {member}" for member in role["member_of"]])
            spec_yaml.extend(["      masking_policies:"])
            spec_yaml.extend(
                [f"        - {policy}" for policy in role["masking_policies"]]
            )
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

    # Utils
    def set_version(self, version):
        self.version = f'version: "{version}"'
        return self

    def require_owner(self):
        self.add_setting("require-owner", "true")
        return self

    # Schema Objects
    def add_column(
        self,
        name="testdb.testschema.testcolumn",
        owner=None,
        masking_policies=["full_mask", "half_mask"],
    ):
        self.columns.append(
            {"name": name, "owner": owner, "masking_policies": masking_policies}
        )
        return self

    def add_db(self, name="testdb", owner=None):
        self.dbs.append({"name": name, "owner": owner})
        return self

    def add_masking_policy(
        self, name="full_mask", input_type="string", return_value="*****", owner=None
    ):
        self.masking_policies.append(
            {
                "name": name,
                "input_type": input_type,
                "return_value": return_value,
                "owner": owner,
            }
        )
        return self

    def add_role(
        self,
        name="testrole",
        owner=None,
        member_of=["testrole"],
        masking_policies=["full_mask"],
    ):
        self.roles.append(
            {
                "name": name,
                "owner": owner,
                "member_of": member_of,
                "masking_policies": masking_policies,
            }
        )
        return self

    def add_setting(self, name, value):
        self.settings.append({"name": name, "value": value})
        return self

    def add_user(self, name="testusername", owner=None):
        self.users.append({"name": name, "owner": owner})
        return self

    def add_warehouse(self, name="testwarehouse", owner=None):
        self.warehouses.append({"name": name, "owner": owner})
        return self
