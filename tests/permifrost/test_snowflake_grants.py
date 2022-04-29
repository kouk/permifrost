import pytest
from permifrost.snowflake_connector import SnowflakeConnector

from permifrost.snowflake_grants import SnowflakeGrantsGenerator
from permifrost_test_utils.snowflake_connector import MockSnowflakeConnector


@pytest.fixture(scope="class")
def test_database_config():
    config = {
        "databases": {
            "database_1": {"shared": False, "owner": "owner1"},
            "database_2": {"shared": False, "owner": "owner2"},
            "database_3": {"shared": False},
            "shared_database_1": {"shared": True, "owner": "owner1"},
            "shared_database_2": {"shared": True, "owner": "owner2"},
        }
    }

    return config


@pytest.fixture(scope="class")
def test_spec_dbs(test_database_config):
    return [database for database in test_database_config["databases"]]


@pytest.fixture(scope="class")
def test_shared_dbs(test_database_config):
    shared_dbs = []
    databases = test_database_config.get("databases", {})
    for database in databases:
        if databases.get(database, {}).get("shared", False):
            shared_dbs.append(database)

    return shared_dbs


@pytest.fixture(scope="class")
def test_role_config():
    config = {
        "functional_role": {
            "warehouses": ["warehouse_2", "warehouse_3"],
            "member_of": ["object_role_2", "object_role_3"],
            "owns": {
                "databases": ["database_2", "database_3"],
                "schemas": [
                    "database_1.schemas_1",
                    "database_2.schemas_2",
                ],
                "tables": [
                    "database_1.schemas_1.tables_1",
                    "database_2.schemas_1.tables_2",
                ],
            },
            "privileges": {
                "databases": {
                    "read": ["database_2", "shared_database_2"],
                    "write": ["database_3"],
                }
            },
        },
        "role_without_member_of": {
            "warehouses": ["warehouse_2", "warehouse_3"],
            "privileges": {
                "databases": {
                    "read": ["database_2", "shared_database_2"],
                    "write": ["database_3"],
                }
            },
        },
    }

    return config


@pytest.fixture(scope="class")
def test_user_config():
    config = {
        "user_name": {"can_login": True, "member_of": ["object_role", "user_role"]}
    }

    return config


@pytest.fixture(scope="class")
def test_grants_to_role():
    """
    Outlines the existing permissions for a role (i.e. functional_role)
    For more comprehensive examples of how to configure this method,
    please review the snowflake_spec_loader.get_privileges_from_snowflake_server
    method

    DO NOT UPDATE as this will cause breaking changes to all generate_<object>_grants
    tests. Consider developing a function scoped version for specific tests
    """

    roles = {
        "functional_role": {
            "usage": {
                "database": ["database_1", "database_2", "shared_database_1"],
                "role": ["object_role_1", "object_role_2"],
                "warehouse": ["warehouse_1", "warehouse_2"],
            },
            "operate": {"warehouse": ["warehouse_1", "warehouse_2"]},
            "monitor": {
                "database": ["database_1", "database_2"],
                "warehouse": ["warehouse_1", "warehouse_2"],
            },
            "create schema": {"database": ["database_1", "database_2"]},
        },
        "role_without_member_of": {
            "usage": {
                "database": ["database_1", "database_2", "shared_database_1"],
                "role": ["object_role_1", "object_role_2"],
                "warehouse": ["warehouse_1", "warehouse_2"],
            },
            "operate": {"warehouse": ["warehouse_1", "warehouse_2"]},
            "monitor": {"database": ["database_1", "database_2"]},
            "create schema": {"database": ["database_1", "database_2"]},
        },
    }

    return roles


@pytest.fixture(scope="class")
def test_roles_granted_to_user():
    config = {"user_name": ["functional_role", "user_role"]}

    return config


class TestSnowflakeGrants:
    @pytest.mark.parametrize("ignore_memberships", [True, False])
    def test_generate_warehouse_grants(
        self,
        test_grants_to_role,
        test_roles_granted_to_user,
        test_role_config,
        ignore_memberships,
    ):
        generator = SnowflakeGrantsGenerator(
            test_grants_to_role,
            test_roles_granted_to_user,
            ignore_memberships=ignore_memberships,
        )

        warehouse_command_list = generator.generate_warehouse_grants(
            "functional_role", test_role_config["functional_role"]["warehouses"]
        )

        warehouse_lower_list = [
            cmd.get("sql", "").lower() for cmd in warehouse_command_list
        ]

        assert (
            "grant usage on warehouse warehouse_2 to role functional_role"
            in warehouse_lower_list
        )
        assert (
            "grant usage on warehouse warehouse_3 to role functional_role"
            in warehouse_lower_list
        )
        assert (
            "revoke usage on warehouse warehouse_1 from role functional_role"
            in warehouse_lower_list
        )

        assert (
            "grant operate on warehouse warehouse_2 to role functional_role"
            in warehouse_lower_list
        )
        assert (
            "grant operate on warehouse warehouse_3 to role functional_role"
            in warehouse_lower_list
        )
        assert (
            "revoke operate on warehouse warehouse_1 from role functional_role"
            in warehouse_lower_list
        )

        assert (
            "grant monitor on warehouse warehouse_2 to role functional_role"
            in warehouse_lower_list
        )
        assert (
            "grant monitor on warehouse warehouse_3 to role functional_role"
            in warehouse_lower_list
        )
        assert (
            "revoke monitor on warehouse warehouse_1 from role functional_role"
            in warehouse_lower_list
        )


class TestGenerateRoleGrants:
    def generate_single_role_grant():
        """
        Generate GRANT for role_1 to have access to role_2
        """
        entity_type = "roles"
        entity = "role_1"
        test_grants_to_role = {
            "member_of": ["role_2"],
        }
        test_roles_granted_to_user = {}
        ignore_membership = False
        expected = ["GRANT ROLE role_2 TO role role_1"]
        return [
            entity_type,
            entity,
            test_grants_to_role,
            test_roles_granted_to_user,
            ignore_membership,
            expected,
        ]

    def generate_single_user_grant():
        """
        Generate GRANT for user_1 to have access to role_1
        """
        entity_type = "users"
        entity = "user_1"
        test_grants_to_role = {
            "member_of": ["role_1"],
        }
        test_roles_granted_to_user = {"user_1": []}
        ignore_membership = False
        expected = ["GRANT ROLE role_1 TO user user_1"]
        return [
            entity_type,
            entity,
            test_grants_to_role,
            test_roles_granted_to_user,
            ignore_membership,
            expected,
        ]

    def generate_multi_user_grants():
        """
        Generate GRANT for user_1 to have access to role_1, role_2
        """
        entity_type = "users"
        entity = "user_1"
        test_grants_to_role = {
            "member_of": ["role_1", "role_2"],
        }
        test_roles_granted_to_user = {"user_1": []}
        ignore_membership = False
        expected = [
            "GRANT ROLE role_1 TO user user_1",
            "GRANT ROLE role_2 TO user user_1",
        ]
        return [
            entity_type,
            entity,
            test_grants_to_role,
            test_roles_granted_to_user,
            ignore_membership,
            expected,
        ]

    def generate_multi_role_grants():
        """
        Generate GRANT for role_1 to have access to role_2, role_3
        """
        entity_type = "roles"
        entity = "role_1"
        test_grants_to_role = {
            "member_of": ["role_2", "role_3"],
        }
        test_roles_granted_to_user = {"user_1": []}
        ignore_membership = False
        expected = [
            "GRANT ROLE role_2 TO role role_1",
            "GRANT ROLE role_3 TO role role_1",
        ]
        return [
            entity_type,
            entity,
            test_grants_to_role,
            test_roles_granted_to_user,
            ignore_membership,
            expected,
        ]

    def generate_multi_user_grants_ignore():
        """
        Should not generate GRANT for user_1 to have access to role_1, role_2
        """
        entity_type = "users"
        entity = "user_1"
        test_grants_to_role = {
            "member_of": ["role_1", "role_2"],
        }
        test_roles_granted_to_user = {"user_1": []}
        ignore_membership = True
        expected = []
        return [
            entity_type,
            entity,
            test_grants_to_role,
            test_roles_granted_to_user,
            ignore_membership,
            expected,
        ]

    def generate_multi_role_grants_ignore():
        """
        Should not generate GRANT for role_1 to have access to role_2, role_3
        """
        entity_type = "roles"
        entity = "role_1"
        test_grants_to_role = {
            "member_of": ["role_2", "role_3"],
        }
        test_roles_granted_to_user = {"user_1": []}
        ignore_membership = True

        expected = []
        return [
            entity_type,
            entity,
            test_grants_to_role,
            test_roles_granted_to_user,
            ignore_membership,
            expected,
        ]

    @pytest.mark.parametrize(
        "config",
        [
            generate_single_user_grant,
            generate_single_role_grant,
            generate_multi_user_grants,
            generate_multi_role_grants,
            generate_multi_user_grants_ignore,
            generate_multi_role_grants_ignore,
        ],
    )
    def test_generate_grant_roles(
        self,
        config,
        mocker,
    ):
        (
            entity_type,
            entity,
            test_grants_to_role,
            test_roles_granted_to_user,
            ignore_membership,
            expected,
        ) = config()
        mocker.patch.object(SnowflakeConnector, "__init__", lambda x: None)

        generator = SnowflakeGrantsGenerator(
            test_grants_to_role, test_roles_granted_to_user, ignore_membership
        )

        role_command_list = generator.generate_grant_roles(
            entity_type,
            entity,
            test_grants_to_role,
        )

        results = [cmd.get("sql", "") for cmd in role_command_list]
        results.sort()

        assert results == expected


class TestGenerateRoleGrantRevokes:
    def generate_single_role_revoke():
        """
        REVOKE for role_1 to have access to role_2
        """
        entity_type = "roles"
        entity = "role_1"
        test_grants_to_role = {"role_1": {"usage": {"role": ["role_2"]}}}
        test_roles_granted_to_user = {}
        ignore_membership = False
        grants_spec = {}

        expected = ["REVOKE ROLE role_2 FROM role role_1"]
        return [
            entity_type,
            entity,
            test_grants_to_role,
            test_roles_granted_to_user,
            ignore_membership,
            grants_spec,
            expected,
        ]

    def generate_single_user_revoke():
        """
        REVOKE for user_1 to have access to role_1
        """
        entity_type = "users"
        entity = "user_1"
        test_grants_to_role = {}
        test_roles_granted_to_user = {"user_1": ["role_1"]}
        ignore_membership = False
        grants_spec = {}
        expected = ["REVOKE ROLE role_1 FROM user user_1"]
        return [
            entity_type,
            entity,
            test_grants_to_role,
            test_roles_granted_to_user,
            ignore_membership,
            grants_spec,
            expected,
        ]

    def generate_multi_role_revokes():
        """
        REVOKE for role_1 to have access to role_2, role_3
        """
        entity_type = "roles"
        entity = "role_1"
        test_grants_to_role = {"role_1": {"usage": {"role": ["role_2", "role_3"]}}}
        test_roles_granted_to_user = {}
        ignore_membership = False
        grants_spec = {}

        expected = [
            "REVOKE ROLE role_2 FROM role role_1",
            "REVOKE ROLE role_3 FROM role role_1",
        ]
        return [
            entity_type,
            entity,
            test_grants_to_role,
            test_roles_granted_to_user,
            ignore_membership,
            grants_spec,
            expected,
        ]

    def generate_multi_user_revokes():
        """
        REVOKE for user_1 to have access to role_1, role_2
        """
        entity_type = "users"
        entity = "user_1"
        test_grants_to_role = {}
        test_roles_granted_to_user = {"user_1": ["role_1", "role_2"]}

        ignore_membership = False
        grants_spec = {}

        expected = [
            "REVOKE ROLE role_1 FROM user user_1",
            "REVOKE ROLE role_2 FROM user user_1",
        ]
        return [
            entity_type,
            entity,
            test_grants_to_role,
            test_roles_granted_to_user,
            ignore_membership,
            grants_spec,
            expected,
        ]

    def generate_multi_role_revokes_ignore():
        """
        Should not generate REVOKE for role_1 to have access to role_2, role_3
        """
        entity_type = "roles"
        entity = "role_1"
        test_grants_to_role = {"role_1": {"usage": {"role": ["role_2", "role_3"]}}}
        test_roles_granted_to_user = {}
        ignore_membership = True
        grants_spec = {}
        expected = []

        return [
            entity_type,
            entity,
            test_grants_to_role,
            test_roles_granted_to_user,
            ignore_membership,
            grants_spec,
            expected,
        ]

    def generate_multi_user_revokes_ignore():
        """
        Should not generate REVOKE for user_1 to have access to role_1, role_2
        """
        entity_type = "users"
        entity = "user_1"
        test_grants_to_role = {}
        test_roles_granted_to_user = {"user_1": ["role_1", "role_2"]}

        ignore_membership = True
        grants_spec = {}

        expected = []
        return [
            entity_type,
            entity,
            test_grants_to_role,
            test_roles_granted_to_user,
            ignore_membership,
            grants_spec,
            expected,
        ]

    @pytest.mark.parametrize(
        "config",
        [
            generate_single_role_revoke,
            generate_single_user_revoke,
            generate_multi_role_revokes,
            generate_multi_user_revokes,
            generate_multi_role_revokes_ignore,
            generate_multi_user_revokes_ignore,
        ],
    )
    def test_generate_grant_roles_revokes(
        self,
        config,
        mocker,
    ):
        (
            entity_type,
            entity,
            test_grants_to_role,
            test_roles_granted_to_user,
            ignore_membership,
            grants_spec,
            expected,
        ) = config()
        mocker.patch.object(SnowflakeConnector, "__init__", lambda x: None)

        generator = SnowflakeGrantsGenerator(
            test_grants_to_role, test_roles_granted_to_user, ignore_membership
        )

        role_command_list = generator.generate_grant_roles(
            entity_type,
            entity,
            grants_spec,
        )

        results = [cmd.get("sql", "") for cmd in role_command_list]
        results.sort()

        assert results == expected

    def test_no_revoke_with_no_member_of_but_ignore_membership(
        self, test_grants_to_role, test_roles_granted_to_user
    ):
        generator = SnowflakeGrantsGenerator(
            test_grants_to_role, test_roles_granted_to_user, ignore_memberships=True
        )

        role_command_list = generator.generate_grant_roles(
            "roles", "role_without_member_of", "no_config"
        )
        assert role_command_list == []


class TestGenerateTableAndViewGrants:
    def single_table_r_config(mocker):
        """
        Provides read access on table_1 in
        schema_1 schema in database_1 database.
        """
        mock_connector = MockSnowflakeConnector()
        mocker.patch.object(
            mock_connector,
            "show_tables",
            # show_tables called by read before write function call
            side_effect=[["database_1.schema_1.table_1"], []],
        )
        mocker.patch.object(mock_connector, "show_views", side_effect=[[], []])

        config = {
            "read": ["database_1.schema_1.table_1"],
            "write": [],
        }

        expected = [
            "GRANT select ON table database_1.schema_1.table_1 TO ROLE functional_role"
        ]

        return [mock_connector, config, expected]

    def single_table_w_config(mocker):
        """
        Provides write access on table_1 in
        schema_1 schema in database_1 database.
        """
        mock_connector = MockSnowflakeConnector()
        mocker.patch.object(
            mock_connector,
            "show_tables",
            # show_tables called by read before write function call
            side_effect=[["database_1.schema_1.table_1"], []],
        )
        mocker.patch.object(mock_connector, "show_views", side_effect=[[], []])

        config = {
            "read": [],
            "write": ["database_1.schema_1.table_1"],
        }

        # TODO: Enable the ability to provide writes without reads
        expected = [
            "GRANT select, insert, update, delete, truncate, references ON table database_1.schema_1.table_1 TO ROLE functional_role"
        ]

        return [mock_connector, config, expected]

    def single_table_rw_config(mocker):
        """
        Provides read/write access on table_1 in
        SCHEMA_1 schema in DATABASE_1 database.
        """
        mocker.patch.object(
            MockSnowflakeConnector,
            "show_tables",
            return_value=["database_1.schema_1.table_1"],
        )
        mocker.patch.object(MockSnowflakeConnector, "show_views", return_value=[])

        config = {
            "read": ["database_1.schema_1.table_1"],
            "write": ["database_1.schema_1.table_1"],
        }

        expected = [
            "GRANT select ON table database_1.schema_1.table_1 TO ROLE functional_role",
            "GRANT select, insert, update, delete, truncate, references ON table database_1.schema_1.table_1 TO ROLE functional_role",
        ]

        return [MockSnowflakeConnector, config, expected]

    def single_table_rw_shared_db_config(mocker):
        """
        No permissions generated for shared databases
        """
        mocker.patch.object(
            MockSnowflakeConnector,
            "show_tables",
            return_value=["shared_database_1.public.table_1"],
        )
        mocker.patch.object(MockSnowflakeConnector, "show_views", return_value=[])

        config = {
            "read": ["shared_database_1.public.table_1"],
            "write": ["shared_database_1.public.table_1"],
        }

        expected = []
        return [MockSnowflakeConnector, config, expected]

    def future_tables_r_single_schema_config(mocker):
        """
        Provides read access on ALL|FUTURE tables|views in
        schema_1 schema in database_1 database.
        """
        mocker.patch.object(
            MockSnowflakeConnector,
            "show_tables",
            return_value=["database_1.schema_1.table_1", "database_1.schema_1.table_2"],
        )
        mocker.patch.object(MockSnowflakeConnector, "show_views", return_value=[])

        config = {
            "read": [
                "database_1.schema_1.*",
            ],
            "write": [],
        }

        expected = [
            "GRANT select ON ALL tables IN schema database_1.schema_1 TO ROLE functional_role",
            "GRANT select ON ALL views IN schema database_1.schema_1 TO ROLE functional_role",
            "GRANT select ON FUTURE tables IN schema database_1.schema_1 TO ROLE functional_role",
            "GRANT select ON FUTURE views IN schema database_1.schema_1 TO ROLE functional_role",
        ]

        return [MockSnowflakeConnector, config, expected]

    def future_tables_w_single_schema_config(mocker):
        """
        Provides write access on ALL|FUTURE tables|views in
        schema_1 schema in database_1 database.
        """
        mocker.patch.object(
            MockSnowflakeConnector,
            "show_tables",
            return_value=["database_1.schema_1.table_1", "database_1.schema_1.table_2"],
        )
        mocker.patch.object(MockSnowflakeConnector, "show_views", return_value=[])

        config = {
            "read": [],
            "write": [
                "database_1.schema_1.*",
            ],
        }

        expected = [
            "GRANT select ON ALL views IN schema database_1.schema_1 TO ROLE functional_role",
            "GRANT select ON FUTURE views IN schema database_1.schema_1 TO ROLE functional_role",
            "GRANT select, insert, update, delete, truncate, references ON ALL tables IN schema database_1.schema_1 TO ROLE functional_role",
            "GRANT select, insert, update, delete, truncate, references ON FUTURE tables IN schema database_1.schema_1 TO ROLE functional_role",
        ]

        return [MockSnowflakeConnector, config, expected]

    def future_tables_rw_single_schema_config(mocker):
        """
        Provides read/write access on ALL|FUTURE tables|views in
        schema_1 schema in database_1 database.
        """
        mocker.patch.object(
            MockSnowflakeConnector,
            "show_tables",
            return_value=["database_1.schema_1.table_1", "database_1.schema_1.table_2"],
        )
        mocker.patch.object(MockSnowflakeConnector, "show_views", return_value=[])

        config = {
            "read": [
                "database_1.schema_1.*",
            ],
            "write": [
                "database_1.schema_1.*",
            ],
        }

        expected = [
            "GRANT select ON ALL tables IN schema database_1.schema_1 TO ROLE functional_role",
            "GRANT select ON ALL views IN schema database_1.schema_1 TO ROLE functional_role",
            "GRANT select ON ALL views IN schema database_1.schema_1 TO ROLE functional_role",
            "GRANT select ON FUTURE tables IN schema database_1.schema_1 TO ROLE functional_role",
            "GRANT select ON FUTURE views IN schema database_1.schema_1 TO ROLE functional_role",
            "GRANT select ON FUTURE views IN schema database_1.schema_1 TO ROLE functional_role",
            "GRANT select, insert, update, delete, truncate, references ON ALL tables IN schema database_1.schema_1 TO ROLE functional_role",
            "GRANT select, insert, update, delete, truncate, references ON FUTURE tables IN schema database_1.schema_1 TO ROLE functional_role",
        ]

        return [MockSnowflakeConnector, config, expected]

    def future_tables_views_rw_config(mocker):
        """
        Provides read/write access on ALL|FUTURE tables|views in
        schema_1 schema in database_1 database.
        """
        mocker.patch.object(
            MockSnowflakeConnector,
            "show_tables",
            return_value=["database_1.schema_1.table_1", "database_1.schema_1.table_2"],
        )
        mocker.patch.object(
            MockSnowflakeConnector,
            "show_views",
            return_value=["database_1.schema_1.view_1"],
        )

        config = {
            "read": [
                "database_1.schema_1.*",
            ],
            "write": [
                "database_1.schema_1.*",
            ],
        }

        expected = [
            "GRANT select ON ALL tables IN schema database_1.schema_1 TO ROLE functional_role",
            "GRANT select ON ALL views IN schema database_1.schema_1 TO ROLE functional_role",
            "GRANT select ON ALL views IN schema database_1.schema_1 TO ROLE functional_role",
            "GRANT select ON FUTURE tables IN schema database_1.schema_1 TO ROLE functional_role",
            "GRANT select ON FUTURE views IN schema database_1.schema_1 TO ROLE functional_role",
            "GRANT select ON FUTURE views IN schema database_1.schema_1 TO ROLE functional_role",
            "GRANT select, insert, update, delete, truncate, references ON ALL tables IN schema database_1.schema_1 TO ROLE functional_role",
            "GRANT select, insert, update, delete, truncate, references ON FUTURE tables IN schema database_1.schema_1 TO ROLE functional_role",
        ]

        return [MockSnowflakeConnector, config, expected]

    def future_schemas_tables_views_config(mocker):
        """
        Provides read/write on ALL|FUTURE schemas and ALL|FUTURE views|tables
        in database_1 database
        """
        mock_connector = MockSnowflakeConnector()
        mocker.patch.object(
            mock_connector,
            "show_schemas",
            return_value=["database_1.schema_1", "database_1.schema_2"],
        )
        mocker.patch.object(
            mock_connector,
            "show_tables",
            return_value=[
                "database_1.schema_1.table_1",
                "database_1.schema_1.table_2",
                "database_1.schema_2.table_3",
            ],
        )
        mocker.patch.object(
            mock_connector,
            "show_views",
            return_value=["database_1.schema_1.view_1"],
        )
        mocker.patch(
            "permifrost.snowflake_grants.SnowflakeConnector.show_schemas",
            mock_connector.show_schemas,
        )
        config = {
            "read": [
                "database_1.*.*",
            ],
            "write": [
                "database_1.*.*",
            ],
        }

        expected = [
            "GRANT select ON ALL tables IN database database_1 TO ROLE functional_role",
            "GRANT select ON ALL tables IN schema database_1.schema_1 TO ROLE functional_role",
            "GRANT select ON ALL tables IN schema database_1.schema_2 TO ROLE functional_role",
            "GRANT select ON ALL views IN database database_1 TO ROLE functional_role",
            "GRANT select ON ALL views IN database database_1 TO ROLE functional_role",
            "GRANT select ON ALL views IN schema database_1.schema_1 TO ROLE functional_role",
            "GRANT select ON ALL views IN schema database_1.schema_1 TO ROLE functional_role",
            "GRANT select ON ALL views IN schema database_1.schema_2 TO ROLE functional_role",
            "GRANT select ON ALL views IN schema database_1.schema_2 TO ROLE functional_role",
            "GRANT select ON FUTURE tables IN database database_1 TO ROLE functional_role",
            "GRANT select ON FUTURE tables IN schema database_1.schema_1 TO ROLE functional_role",
            "GRANT select ON FUTURE tables IN schema database_1.schema_2 TO ROLE functional_role",
            "GRANT select ON FUTURE views IN database database_1 TO ROLE functional_role",
            "GRANT select ON FUTURE views IN database database_1 TO ROLE functional_role",
            "GRANT select ON FUTURE views IN schema database_1.schema_1 TO ROLE functional_role",
            "GRANT select ON FUTURE views IN schema database_1.schema_1 TO ROLE functional_role",
            "GRANT select ON FUTURE views IN schema database_1.schema_2 TO ROLE functional_role",
            "GRANT select ON FUTURE views IN schema database_1.schema_2 TO ROLE functional_role",
            "GRANT select, insert, update, delete, truncate, references ON ALL tables IN database database_1 TO ROLE functional_role",
            "GRANT select, insert, update, delete, truncate, references ON ALL tables IN schema database_1.schema_1 TO ROLE functional_role",
            "GRANT select, insert, update, delete, truncate, references ON ALL tables IN schema database_1.schema_2 TO ROLE functional_role",
            "GRANT select, insert, update, delete, truncate, references ON FUTURE tables IN database database_1 TO ROLE functional_role",
            "GRANT select, insert, update, delete, truncate, references ON FUTURE tables IN schema database_1.schema_1 TO ROLE functional_role",
            "GRANT select, insert, update, delete, truncate, references ON FUTURE tables IN schema database_1.schema_2 TO ROLE functional_role",
        ]

        return [mock_connector, config, expected]

    def partial_rw_future_schemas_tables_views_config(mocker):
        """
        Provides read on ALL|FUTURE schemas and ALL|FUTURE views|tables
        in RAW database, but only write access on ALL|FUTURE tables
        in PUBLIC schema and RAW database.
        """
        mock_connector = MockSnowflakeConnector()
        # Need to account for different outputs in function
        mocker.patch.object(
            mock_connector,
            "show_schemas",
            # show_schemas called by read before write function call
            side_effect=[["raw.public", "raw.public_1"], ["raw.public"]],
        )
        mocker.patch.object(
            mock_connector,
            "show_tables",
            # show_tables called multiple times for read/write
            # therefore, there is a need to have different
            # results for each call where read comes before write
            side_effect=[
                [
                    "raw.public.table_1",
                    "raw.public.table_2",
                ],
                ["raw.public_1.table_3"],
                [
                    "raw.public.table_1",
                    "raw.public.table_2",
                ],
            ],
        )
        mocker.patch.object(
            mock_connector, "show_views", return_value=["raw.public.view_1"]
        )
        mocker.patch(
            "permifrost.snowflake_grants.SnowflakeConnector.show_schemas",
            mock_connector.show_schemas,
        )
        config = {
            "read": [
                "raw.*.*",
            ],
            "write": [
                "raw.public.*",
            ],
        }

        expected = [
            "GRANT select ON ALL tables IN database raw TO ROLE functional_role",
            "GRANT select ON ALL tables IN schema raw.public TO ROLE functional_role",
            "GRANT select ON ALL tables IN schema raw.public_1 TO ROLE functional_role",
            "GRANT select ON ALL views IN database raw TO ROLE functional_role",
            "GRANT select ON ALL views IN schema raw.public TO ROLE functional_role",
            "GRANT select ON ALL views IN schema raw.public TO ROLE functional_role",
            "GRANT select ON ALL views IN schema raw.public_1 TO ROLE functional_role",
            "GRANT select ON FUTURE tables IN database raw TO ROLE functional_role",
            "GRANT select ON FUTURE tables IN schema raw.public TO ROLE functional_role",
            "GRANT select ON FUTURE tables IN schema raw.public_1 TO ROLE functional_role",
            "GRANT select ON FUTURE views IN database raw TO ROLE functional_role",
            "GRANT select ON FUTURE views IN schema raw.public TO ROLE functional_role",
            "GRANT select ON FUTURE views IN schema raw.public TO ROLE functional_role",
            "GRANT select ON FUTURE views IN schema raw.public_1 TO ROLE functional_role",
            "GRANT select, insert, update, delete, truncate, references ON ALL tables IN schema raw.public TO ROLE functional_role",
            "GRANT select, insert, update, delete, truncate, references ON FUTURE tables IN schema raw.public TO ROLE functional_role",
        ]

        return [mock_connector, config, expected]

    def table_partial_rw_future_schemas_tables_views_config(mocker):
        """
        Provides read on ALL|FUTURE schemas and ALL|FUTURE views|tables
        in RAW database, but only write access on ALL|FUTURE tables
        in PUBLIC schema and RAW database.
        """
        mock_connector = MockSnowflakeConnector()
        # Need to account for different outputs in function
        mocker.patch.object(
            mock_connector,
            "show_schemas",
            # show_schemas called by read before write function call
            side_effect=[["raw.public", "raw.public_1"], ["raw.public"]],
        )
        mocker.patch.object(
            mock_connector,
            "show_tables",
            # show_tables called multiple times for read/write
            # therefore, there is a need to have different
            # results for each call where read comes before write
            side_effect=[
                [
                    "raw.public.table_1",
                    "raw.public.table_2",
                ],
                [
                    "raw.public_1.table_3",
                    "raw.public_1.table_4",
                ],
                [
                    "raw.public.table_1",
                    "raw.public.table_2",
                ],
                [
                    "raw.public_1.table_3",
                    "raw.public_1.table_4",
                ],
            ],
        )
        mocker.patch.object(
            mock_connector, "show_views", return_value=["raw.public.view_1"]
        )
        mocker.patch(
            "permifrost.snowflake_grants.SnowflakeConnector.show_schemas",
            mock_connector.show_schemas,
        )
        config = {
            "read": [
                "raw.*.*",
            ],
            "write": [
                "raw.public.*",
                "raw.public_1.table_4",
            ],
        }

        expected = [
            "GRANT select ON ALL tables IN database raw TO ROLE functional_role",
            "GRANT select ON ALL tables IN schema raw.public TO ROLE functional_role",
            "GRANT select ON ALL tables IN schema raw.public_1 TO ROLE functional_role",
            "GRANT select ON ALL views IN database raw TO ROLE functional_role",
            "GRANT select ON ALL views IN schema raw.public TO ROLE functional_role",
            "GRANT select ON ALL views IN schema raw.public TO ROLE functional_role",
            "GRANT select ON ALL views IN schema raw.public_1 TO ROLE functional_role",
            "GRANT select ON FUTURE tables IN database raw TO ROLE functional_role",
            "GRANT select ON FUTURE tables IN schema raw.public TO ROLE functional_role",
            "GRANT select ON FUTURE tables IN schema raw.public_1 TO ROLE functional_role",
            "GRANT select ON FUTURE views IN database raw TO ROLE functional_role",
            "GRANT select ON FUTURE views IN schema raw.public TO ROLE functional_role",
            "GRANT select ON FUTURE views IN schema raw.public TO ROLE functional_role",
            "GRANT select ON FUTURE views IN schema raw.public_1 TO ROLE functional_role",
            "GRANT select, insert, update, delete, truncate, references ON ALL tables IN schema raw.public TO ROLE functional_role",
            "GRANT select, insert, update, delete, truncate, references ON FUTURE tables IN schema raw.public TO ROLE functional_role",
            "GRANT select, insert, update, delete, truncate, references ON TABLE raw.public_1.table_4 TO ROLE functional_role",
        ]

        return [mock_connector, config, expected]

    @pytest.mark.parametrize(
        "config",
        [
            single_table_r_config,
            single_table_w_config,
            single_table_rw_config,
            single_table_rw_shared_db_config,
            future_tables_r_single_schema_config,
            future_tables_w_single_schema_config,
            future_tables_rw_single_schema_config,
            future_tables_views_rw_config,
            future_schemas_tables_views_config,
            partial_rw_future_schemas_tables_views_config,
        ],
    )
    def test_generate_table_and_view_grants(
        self,
        test_shared_dbs,
        test_spec_dbs,
        test_grants_to_role,
        test_roles_granted_to_user,
        mocker,
        config,
    ):

        mocker.patch.object(SnowflakeConnector, "__init__", lambda x: None)

        # Generation of database grants should be identical while ignoring or not ignoring memberships
        generator = SnowflakeGrantsGenerator(
            test_grants_to_role,
            test_roles_granted_to_user,
        )

        mock_connector, test_tables_config, expected = config(mocker)

        mocker.patch(
            "permifrost.snowflake_grants.SnowflakeConnector.show_tables",
            mock_connector.show_tables,
        )

        mocker.patch(
            "permifrost.snowflake_grants.SnowflakeConnector.show_views",
            mock_connector.show_views,
        )

        tables_and_views_list = generator.generate_table_and_view_grants(
            "functional_role",
            test_tables_config,
            set(test_shared_dbs),
            set(test_spec_dbs),
        )

        tables_and_views_list_sql = []
        for sql_dict in tables_and_views_list:
            for k, v in sql_dict.items():
                if k == "sql":
                    tables_and_views_list_sql.append(v)

        # Sort list of SQL queries for readability
        tables_and_views_list_sql.sort()

        assert tables_and_views_list_sql == expected


class TestGenerateSchemaGrants:
    def single_r_schema_config(mocker):
        """
        Provides read access on SCHEMA_1
        schema in DATABASE_1 database.
        """
        config = {
            "read": ["database_1.schema_1"],
            "write": [],
        }

        expected = ["GRANT usage ON schema database_1.schema_1 TO ROLE functional_role"]
        return [MockSnowflakeConnector, config, expected]

    def single_rw_schema_config(mocker):
        """
        Provides read/write access on SCHEMA_1
        schema in DATABASE_1 database.
        """
        config = {
            "read": ["database_1.schema_1"],
            "write": ["database_1.schema_1"],
        }

        expected = [
            "GRANT usage ON schema database_1.schema_1 TO ROLE functional_role",
            "GRANT usage, monitor, create table, create view, create stage, create file format, create sequence, create function, create pipe ON schema database_1.schema_1 TO ROLE functional_role",
        ]
        return [MockSnowflakeConnector, config, expected]

    def shared_db_single_rw_schema_config(mocker):
        """
        Permissions at the schema level for a shared database
        are not allowed so this is the expected behaviour
        """
        config = {
            "read": ["shared_database_1.schema_1"],
            "write": ["shared_database_1.schema_1"],
        }

        expected = []
        return [MockSnowflakeConnector, config, expected]

    def multi_r_schema_config(mocker):
        """
        Provides read access on SCHEMA_1, SCHEMA_2
        schemas in DATABASE_1 database.
        """
        config = {
            "read": ["database_1.schema_1", "database_1.schema_2"],
            "write": [],
        }

        expected = [
            "GRANT usage ON schema database_1.schema_1 TO ROLE functional_role",
            "GRANT usage ON schema database_1.schema_2 TO ROLE functional_role",
        ]
        return [MockSnowflakeConnector, config, expected]

    def multi_rw_schema_config(mocker):
        """
        Provides read/write access on SCHEMA_1, SCHEMA_2
        schemas in DATABASE_1 database.
        """
        config = {
            "read": ["database_1.schema_1", "database_1.schema_2"],
            "write": ["database_1.schema_1", "database_1.schema_2"],
        }

        expected = [
            "GRANT usage ON schema database_1.schema_1 TO ROLE functional_role",
            "GRANT usage ON schema database_1.schema_2 TO ROLE functional_role",
            "GRANT usage, monitor, create table, create view, create stage, create file format, create sequence, create function, create pipe ON schema database_1.schema_1 TO ROLE functional_role",
            "GRANT usage, monitor, create table, create view, create stage, create file format, create sequence, create function, create pipe ON schema database_1.schema_2 TO ROLE functional_role",
        ]
        return [MockSnowflakeConnector, config, expected]

    def multi_diff_rw_schema_config(mocker):
        """
        Provides read access on SCHEMA_1, SCHEMA_2
        schemas and write access on SCHEMA_1 in DATABASE_1 database.
        """
        config = {
            "read": ["database_1.schema_1", "database_1.schema_2"],
            "write": ["database_1.schema_1"],
        }

        expected = [
            "GRANT usage ON schema database_1.schema_1 TO ROLE functional_role",
            "GRANT usage ON schema database_1.schema_2 TO ROLE functional_role",
            "GRANT usage, monitor, create table, create view, create stage, create file format, create sequence, create function, create pipe ON schema database_1.schema_1 TO ROLE functional_role",
        ]
        return [MockSnowflakeConnector, config, expected]

    def multi_diff_db_rw_schema_config(mocker):
        """
        Provides read access on DATABASE_1.SCHEMA_1, DATABASE_2.SCHEMA_2
        schemas and write access on DATABASE_1.SCHEMA_1
        """
        config = {
            "read": ["database_1.schema_1", "database_2.schema_2"],
            "write": ["database_1.schema_1"],
        }

        expected = [
            "GRANT usage ON schema database_1.schema_1 TO ROLE functional_role",
            "GRANT usage ON schema database_2.schema_2 TO ROLE functional_role",
            "GRANT usage, monitor, create table, create view, create stage, create file format, create sequence, create function, create pipe ON schema database_1.schema_1 TO ROLE functional_role",
        ]
        return [MockSnowflakeConnector, config, expected]

    def multi_diff_shared_db_rw_schema_config(mocker):
        """
        Permissions at the schema level for a shared database
        are not allowed so permissions should only be for
        read/write on DATABASE_1.SCHEMA_1
        """
        config = {
            "read": ["shared_database_1.shared_schema_1", "database_1.schema_1"],
            "write": ["shared_database_1.shared_schema_1", "database_1.schema_1"],
        }

        expected = [
            "GRANT usage ON schema database_1.schema_1 TO ROLE functional_role",
            "GRANT usage, monitor, create table, create view, create stage, create file format, create sequence, create function, create pipe ON schema database_1.schema_1 TO ROLE functional_role",
        ]
        return [MockSnowflakeConnector, config, expected]

    def star_r_schema_config(mocker):
        """
        Provides read access on SCHEMA_1, SCHEMA_2, SCHEMA_3
        schemas in DATABASE_1 database
        """
        mocker.patch.object(
            MockSnowflakeConnector,
            "show_schemas",
            # show_schemas called by read before write function call
            return_value=[
                "database_1.schema_1",
                "database_1.schema_2",
                "database_1.schema_3",
            ],
        )
        config = {
            "read": ["database_1.*"],
            "write": [],
        }

        expected = [
            "GRANT usage ON FUTURE schemas IN database database_1 TO ROLE functional_role",
            "GRANT usage ON schema database_1.schema_1 TO ROLE functional_role",
            "GRANT usage ON schema database_1.schema_2 TO ROLE functional_role",
            "GRANT usage ON schema database_1.schema_3 TO ROLE functional_role",
        ]
        return [MockSnowflakeConnector, config, expected]

    def star_rw_schema_config(mocker):
        """
        Provides read/write access on SCHEMA_1, SCHEMA_2, SCHEMA_3
        schemas in DATABASE_1 database
        """
        mocker.patch.object(
            MockSnowflakeConnector,
            "show_schemas",
            # show_schemas called by read before write function call
            return_value=[
                "database_1.schema_1",
                "database_1.schema_2",
                "database_1.schema_3",
            ],
        )
        config = {
            "read": ["database_1.*"],
            "write": ["database_1.*"],
        }

        expected = [
            "GRANT usage ON FUTURE schemas IN database database_1 TO ROLE functional_role",
            "GRANT usage ON schema database_1.schema_1 TO ROLE functional_role",
            "GRANT usage ON schema database_1.schema_2 TO ROLE functional_role",
            "GRANT usage ON schema database_1.schema_3 TO ROLE functional_role",
            "GRANT usage, monitor, create table, create view, create stage, create file format, create sequence, create function, create pipe ON FUTURE schemas IN database database_1 TO ROLE functional_role",
            "GRANT usage, monitor, create table, create view, create stage, create file format, create sequence, create function, create pipe ON schema database_1.schema_1 TO ROLE functional_role",
            "GRANT usage, monitor, create table, create view, create stage, create file format, create sequence, create function, create pipe ON schema database_1.schema_2 TO ROLE functional_role",
            "GRANT usage, monitor, create table, create view, create stage, create file format, create sequence, create function, create pipe ON schema database_1.schema_3 TO ROLE functional_role",
        ]
        return [MockSnowflakeConnector, config, expected]

    def star_diff_rw_schema_config(mocker):
        """
        Provides read access on SCHEMA_1, SCHEMA_2
        and read/write access on SCHEMA_3 in DATABASE_1 database
        """
        mocker.patch.object(
            MockSnowflakeConnector,
            "show_schemas",
            # show_schemas called by read before write function call
            side_effect=[
                [
                    "database_1.schema_1",
                    "database_1.schema_2",
                    "database_1.schema_3",
                ],
                ["database_1.schema_3"],
            ],
        )
        config = {
            "read": ["database_1.*"],
            "write": ["database_1.schema_3"],
        }

        expected = [
            "GRANT usage ON FUTURE schemas IN database database_1 TO ROLE functional_role",
            "GRANT usage ON schema database_1.schema_1 TO ROLE functional_role",
            "GRANT usage ON schema database_1.schema_2 TO ROLE functional_role",
            "GRANT usage ON schema database_1.schema_3 TO ROLE functional_role",
            "GRANT usage, monitor, create table, create view, create stage, create file format, create sequence, create function, create pipe ON schema database_1.schema_3 TO ROLE functional_role",
        ]
        return [MockSnowflakeConnector, config, expected]

    def multi_star_rw_schema_config(mocker):
        """
        Provides read/write access on SCHEMA_1, SCHEMA_2 in DATABASE_1
        and read/write access on SCHEMA_3 in DATABASE_2
        """
        mocker.patch.object(
            MockSnowflakeConnector,
            "show_schemas",
            # show_schemas called by read before write function call
            side_effect=[
                [
                    "database_1.schema_1",
                    "database_1.schema_2",
                ],
                [
                    "database_1.schema_1",
                    "database_1.schema_2",
                ],
                ["database_2.schema_3"],
                ["database_2.schema_3"],
            ],
        )
        config = {
            "read": ["database_1.*", "database_2.*"],
            "write": ["database_1.*", "database_2.*"],
        }

        expected = [
            "GRANT usage ON FUTURE schemas IN database database_1 TO ROLE functional_role",
            "GRANT usage ON FUTURE schemas IN database database_2 TO ROLE functional_role",
            "GRANT usage ON schema database_1.schema_1 TO ROLE functional_role",
            "GRANT usage ON schema database_1.schema_1 TO ROLE functional_role",
            "GRANT usage ON schema database_1.schema_2 TO ROLE functional_role",
            "GRANT usage ON schema database_1.schema_2 TO ROLE functional_role",
            "GRANT usage, monitor, create table, create view, create stage, create file format, create sequence, create function, create pipe ON FUTURE schemas IN database database_1 TO ROLE functional_role",
            "GRANT usage, monitor, create table, create view, create stage, create file format, create sequence, create function, create pipe ON FUTURE schemas IN database database_2 TO ROLE functional_role",
            "GRANT usage, monitor, create table, create view, create stage, create file format, create sequence, create function, create pipe ON schema database_2.schema_3 TO ROLE functional_role",
            "GRANT usage, monitor, create table, create view, create stage, create file format, create sequence, create function, create pipe ON schema database_2.schema_3 TO ROLE functional_role",
        ]
        return [MockSnowflakeConnector, config, expected]

    @pytest.mark.parametrize(
        "config",
        [
            single_r_schema_config,
            single_rw_schema_config,
            shared_db_single_rw_schema_config,
            multi_r_schema_config,
            multi_rw_schema_config,
            multi_diff_rw_schema_config,
            multi_diff_db_rw_schema_config,
            multi_diff_shared_db_rw_schema_config,
            star_r_schema_config,
            star_rw_schema_config,
            star_diff_rw_schema_config,
            multi_star_rw_schema_config,
        ],
    )
    def test_generate_schema_grants(
        self,
        test_shared_dbs,
        test_spec_dbs,
        test_grants_to_role,
        test_roles_granted_to_user,
        mocker,
        config,
    ):
        # Generation of database grants should be identical while ignoring or not ignoring memberships
        generator = SnowflakeGrantsGenerator(
            test_grants_to_role,
            test_roles_granted_to_user,
        )

        mock_connector, test_schemas_config, expected = config(mocker)

        mocker.patch.object(SnowflakeConnector, "__init__", lambda x: None)

        mocker.patch(
            "permifrost.snowflake_grants.SnowflakeConnector.show_schemas",
            mock_connector.show_schemas,
        )

        schemas_list = generator.generate_schema_grants(
            "functional_role",
            test_schemas_config,
            set(test_shared_dbs),
            set(test_spec_dbs),
        )

        schemas_list_sql = []
        for sql_dict in schemas_list:
            for k, v in sql_dict.items():
                if k == "sql":
                    schemas_list_sql.append(v)

        # Sort list of SQL queries for readability
        schemas_list_sql.sort()

        assert schemas_list_sql == expected


class TestGenerateDatabaseGrants:
    def single_r_database_config(mocker):
        """
        Provides read access on DATABASE_1 database.
        """
        config = {
            "read": ["database_1"],
            "write": [],
        }

        test_grants_to_role = {}

        expected = [
            "GRANT usage ON database database_1 TO ROLE functional_role",
        ]
        return [MockSnowflakeConnector, config, expected, test_grants_to_role]

    def single_rw_database_config(mocker):
        """
        Provides read/write access on DATABASE_1 database.
        """
        config = {
            "read": ["database_1"],
            "write": ["database_1"],
        }

        test_grants_to_role = {}

        expected = [
            "GRANT usage ON database database_1 TO ROLE functional_role",
            "GRANT usage, monitor, create schema ON database database_1 TO ROLE functional_role",
        ]

        return [MockSnowflakeConnector, config, expected, test_grants_to_role]

    def single_r_shared_db_config(mocker):
        """
        Provides read access on SHARED_DATABASE_1 database.
        """
        config = {
            "read": ["shared_database_1"],
            "write": [],
        }

        expected = [
            "GRANT imported privileges ON database shared_database_1 TO ROLE functional_role"
        ]

        test_grants_to_role = {}

        return [MockSnowflakeConnector, config, expected, test_grants_to_role]

    def single_rw_shared_db_config(mocker):
        """
        Provides read/write access on SHARED_DATABASE_1 database.
        """
        config = {
            "read": ["shared_database_1"],
            "write": ["shared_database_1"],
        }

        # TODO: Remove duplicate queries like this
        expected = [
            "GRANT imported privileges ON database shared_database_1 TO ROLE functional_role",
            "GRANT imported privileges ON database shared_database_1 TO ROLE functional_role",
        ]

        test_grants_to_role = {}

        return [MockSnowflakeConnector, config, expected, test_grants_to_role]

    def multi_r_database_config(mocker):
        """
        Provides read access on DATABASE_1, DATABASE_2 databases.
        """
        config = {
            "read": ["database_1", "database_2"],
            "write": [],
        }

        test_grants_to_role = {}

        expected = [
            "GRANT usage ON database database_1 TO ROLE functional_role",
            "GRANT usage ON database database_2 TO ROLE functional_role",
        ]
        return [MockSnowflakeConnector, config, expected, test_grants_to_role]

    def multi_rw_database_config(mocker):
        """
        Provides read/write access on DATABASE_1, DATABASE_2 databases.
        """
        config = {
            "read": ["database_1", "database_2"],
            "write": ["database_1", "database_2"],
        }

        test_grants_to_role = {}

        expected = [
            "GRANT usage ON database database_1 TO ROLE functional_role",
            "GRANT usage ON database database_2 TO ROLE functional_role",
            "GRANT usage, monitor, create schema ON database database_1 TO ROLE functional_role",
            "GRANT usage, monitor, create schema ON database database_2 TO ROLE functional_role",
        ]
        return [MockSnowflakeConnector, config, expected, test_grants_to_role]

    def multi_diff_rw_database_config(mocker):
        """
        Provides read/write access on DATABASE_1 database
        and read access on DATABASE_2 database.
        """
        config = {
            "read": ["database_1", "database_2"],
            "write": ["database_1"],
        }

        test_grants_to_role = {}

        expected = [
            "GRANT usage ON database database_1 TO ROLE functional_role",
            "GRANT usage ON database database_2 TO ROLE functional_role",
            "GRANT usage, monitor, create schema ON database database_1 TO ROLE functional_role",
        ]

        return [MockSnowflakeConnector, config, expected, test_grants_to_role]

    def multi_shared_db_rw_database_config(mocker):
        """
        Provides read/write access on
        DATABASE_1, DATABASE_2, SHARED_DATABASE_1 databases.
        """
        config = {
            "read": ["database_1", "database_2", "shared_database_1"],
            "write": ["database_1", "database_2", "shared_database_1"],
        }

        test_grants_to_role = {}

        expected = [
            "GRANT imported privileges ON database shared_database_1 TO ROLE functional_role",
            "GRANT imported privileges ON database shared_database_1 TO ROLE functional_role",
            "GRANT usage ON database database_1 TO ROLE functional_role",
            "GRANT usage ON database database_2 TO ROLE functional_role",
            "GRANT usage, monitor, create schema ON database database_1 TO ROLE functional_role",
            "GRANT usage, monitor, create schema ON database database_2 TO ROLE functional_role",
        ]
        return [MockSnowflakeConnector, config, expected, test_grants_to_role]

    @pytest.mark.parametrize("ignore_memberships", [True, False])
    @pytest.mark.parametrize(
        "config",
        [
            single_r_database_config,
            single_rw_database_config,
            single_r_shared_db_config,
            single_rw_shared_db_config,
            multi_r_database_config,
            multi_rw_database_config,
            multi_diff_rw_database_config,
            multi_shared_db_rw_database_config,
        ],
    )
    def test_generate_database_grants_v2(
        self,
        test_shared_dbs,
        test_spec_dbs,
        test_roles_granted_to_user,
        mocker,
        config,
        ignore_memberships,
    ):

        mocker.patch.object(SnowflakeConnector, "__init__", lambda x: None)

        mock_connector, test_database_config, expected, test_grants_to_role = config(
            mocker
        )
        # Generation of database grants should be identical while ignoring or not
        # ignoring memberships
        generator = SnowflakeGrantsGenerator(
            test_grants_to_role,
            test_roles_granted_to_user,
            ignore_memberships=ignore_memberships,
        )

        database_list = generator.generate_database_grants(
            "functional_role",
            test_database_config,
            set(test_shared_dbs),
            set(test_spec_dbs),
        )

        database_list_sql = []
        for sql_dict in database_list:
            for k, v in sql_dict.items():
                if k == "sql":
                    database_list_sql.append(v)

        # Sort list of SQL queries for readability
        database_list_sql.sort()

        assert database_list_sql == expected

    @pytest.mark.parametrize("ignore_memberships", [True, False])
    def test_generate_database_grants(
        self,
        test_grants_to_role,
        test_roles_granted_to_user,
        test_role_config,
        test_user_config,
        test_shared_dbs,
        test_spec_dbs,
        ignore_memberships,
    ):
        # Generation of database grants should be identical while ignoring or not ignoring memberships
        generator = SnowflakeGrantsGenerator(
            test_grants_to_role,
            test_roles_granted_to_user,
            ignore_memberships=ignore_memberships,
        )

        database_command_list = generator.generate_database_grants(
            "functional_role",
            test_role_config["functional_role"]["privileges"]["databases"],
            test_shared_dbs,
            test_spec_dbs,
        )

        database_lower_list = [
            cmd.get("sql", "").lower() for cmd in database_command_list
        ]
        print(database_lower_list)

        assert (
            "grant usage on database database_2 to role functional_role"
            in database_lower_list
        )
        assert (
            "grant usage, monitor, create schema on database database_3 to role functional_role"
            in database_lower_list
        )
        assert (
            "revoke usage on database database_1 from role functional_role"
            in database_lower_list
        )
        assert (
            "revoke monitor, create schema on database database_1 from role functional_role"
            in database_lower_list
        )
        assert (
            "revoke monitor, create schema on database database_2 from role functional_role"
            in database_lower_list
        )

        # Shared DBs
        assert (
            "grant imported privileges on database shared_database_2 to role functional_role"
            in database_lower_list
        )
        assert (
            "revoke imported privileges on database shared_database_1 from role functional_role"
            in database_lower_list
        )


class TestGenerateTableAndViewRevokes:
    def revoke_single_r_table_config(mocker):
        """
        REVOKE read on DATABASE_1.SCHEMA_1.TABLE_1 table
        """
        test_tables_config = {
            "read": [],
            "write": [],
        }

        test_grants_to_role = {
            "functional_role": {"select": {"table": ["database_1.schema_1.table_1"]}},
        }

        expected = [
            "REVOKE select ON table database_1.schema_1.table_1 FROM ROLE functional_role",
        ]

        return [
            test_tables_config,
            test_grants_to_role,
            expected,
        ]

    def revoke_single_w_table_config(mocker):
        """
        REVOKE write on DATABASE_1.SCHEMA_1.TABLE_1 table
        """
        test_tables_config = {
            "read": [],
            "write": [],
        }

        test_grants_to_role = {
            "functional_role": {"insert": {"table": ["database_1.schema_1.table_1"]}},
        }

        expected = [
            "REVOKE insert, update, delete, truncate, references ON table database_1.schema_1.table_1 FROM ROLE functional_role"
        ]

        return [
            test_tables_config,
            test_grants_to_role,
            expected,
        ]

    def revoke_single_rw_table_config(mocker):
        """
        REVOKE read/write on DATABASE_1.SCHEMA_1.TABLE_1 table
        """
        test_tables_config = {
            "read": [],
            "write": [],
        }

        test_grants_to_role = {
            "functional_role": {
                "insert": {"table": ["database_1.schema_1.table_1"]},
                "select": {"table": ["database_1.schema_1.table_1"]},
            },
        }

        expected = [
            "REVOKE insert, update, delete, truncate, references ON table database_1.schema_1.table_1 FROM ROLE functional_role",
            "REVOKE select ON table database_1.schema_1.table_1 FROM ROLE functional_role",
        ]

        return [
            test_tables_config,
            test_grants_to_role,
            expected,
        ]

    def revoke_single_rw_view_config(mocker):
        """
        REVOKE read/write on DATABASE_1.SCHEMA_1.VIEW_1 view
        """
        test_tables_config = {
            "read": [],
            "write": [],
        }

        test_grants_to_role = {
            "functional_role": {
                "select": {"view": ["database_1.schema_1.view_1"]},
            },
        }

        expected = [
            "REVOKE select ON view database_1.schema_1.view_1 FROM ROLE functional_role"
        ]

        return [
            test_tables_config,
            test_grants_to_role,
            expected,
        ]

    def revoke_shared_db_single_r_table_config(mocker):
        """
        Should not generate read REVOKE statements
        on SHARED_DATABASE_1.SCHEMA_1.TABLE_1 table as it is
        in shared database
        """
        test_tables_config = {
            "read": [],
            "write": [],
        }

        test_grants_to_role = {
            "functional_role": {
                "select": {"table": ["shared_database_1.schema_1.table_1"]},
            },
        }

        expected = []

        return [
            test_tables_config,
            test_grants_to_role,
            expected,
        ]

    def revoke_shared_db_single_r_view_config(mocker):
        """
        Empty read REVOKE statements
        on SHARED_DATABASE_1.SCHEMA_1.VIEW_1 view as it is
        in shared database
        """
        test_tables_config = {
            "read": [],
            "write": [],
        }

        test_grants_to_role = {
            "functional_role": {
                "select": {"view": ["shared_database_1.schema_1.view_1"]},
            },
        }

        expected = []

        return [
            test_tables_config,
            test_grants_to_role,
            expected,
        ]

    def revoke_shared_db_single_rw_table_config(mocker):
        """
        Should not generate read/write REVOKE statements
        on SHARED_DATABASE_1.SCHEMA_1.TABLE_1 table as it is
        in shared database
        """
        test_tables_config = {
            "read": [],
            "write": [],
        }

        test_grants_to_role = {
            "functional_role": {
                "select": {"table": ["shared_database_1.schema_1.table_1"]},
                "insert": {"table": ["shared_database_1.schema_1.table_1"]},
            },
        }

        expected = []

        return [
            test_tables_config,
            test_grants_to_role,
            expected,
        ]

    def revoke_multi_rw_table_config(mocker):
        """
        Generates read/write REVOKE statements
        for multiple tables
        """
        test_tables_config = {
            "read": [],
            "write": [],
        }

        test_grants_to_role = {
            "functional_role": {
                "select": {
                    "table": [
                        "database_1.schema_1.table_1",
                        "database_1.schema_1.table_2",
                    ]
                },
                "insert": {
                    "table": [
                        "database_1.schema_1.table_1",
                        "database_1.schema_1.table_2",
                    ]
                },
            },
        }

        expected = [
            "REVOKE insert, update, delete, truncate, references ON table database_1.schema_1.table_1 FROM ROLE functional_role",
            "REVOKE insert, update, delete, truncate, references ON table database_1.schema_1.table_2 FROM ROLE functional_role",
            "REVOKE select ON table database_1.schema_1.table_1 FROM ROLE functional_role",
            "REVOKE select ON table database_1.schema_1.table_2 FROM ROLE functional_role",
        ]

        return [
            test_tables_config,
            test_grants_to_role,
            expected,
        ]

    def revoke_multi_rw_table_view_config(mocker):
        """
        Generates read/write REVOKE statements
        for multiple tables and views
        """
        test_tables_config = {
            "read": [],
            "write": [],
        }

        test_grants_to_role = {
            "functional_role": {
                "select": {
                    "table": [
                        "database_1.schema_1.table_1",
                        "database_1.schema_1.table_2",
                    ],
                    "view": ["database_1.schema_1.view_1"],
                },
                # Not possible to insert on a VIEW
                "insert": {
                    "table": [
                        "database_1.schema_1.table_1",
                        "database_1.schema_1.table_2",
                    ]
                },
            },
        }

        expected = [
            "REVOKE insert, update, delete, truncate, references ON table database_1.schema_1.table_1 FROM ROLE functional_role",
            "REVOKE insert, update, delete, truncate, references ON table database_1.schema_1.table_2 FROM ROLE functional_role",
            "REVOKE select ON table database_1.schema_1.table_1 FROM ROLE functional_role",
            "REVOKE select ON table database_1.schema_1.table_2 FROM ROLE functional_role",
            "REVOKE select ON view database_1.schema_1.view_1 FROM ROLE functional_role",
        ]

        return [
            test_tables_config,
            test_grants_to_role,
            expected,
        ]

    @pytest.mark.parametrize(
        "config",
        [
            revoke_single_r_table_config,
            revoke_single_w_table_config,
            revoke_single_rw_table_config,
            revoke_single_rw_view_config,
            revoke_shared_db_single_r_table_config,
            revoke_shared_db_single_r_view_config,
            revoke_shared_db_single_rw_table_config,
            revoke_multi_rw_table_config,
            revoke_multi_rw_table_view_config,
        ],
    )
    def test_generate_table_view_revokes(
        self,
        test_shared_dbs,
        test_spec_dbs,
        test_roles_granted_to_user,
        mocker,
        config,
    ):

        test_tables_config, test_grants_to_role, expected = config(mocker)

        mocker.patch.object(SnowflakeConnector, "__init__", lambda x: None)

        # Generation of database grants should be identical while
        # ignoring or not ignoring memberships
        generator = SnowflakeGrantsGenerator(
            test_grants_to_role,
            test_roles_granted_to_user,
        )

        tables_list = generator.generate_table_and_view_grants(
            role="functional_role",
            tables=test_tables_config,
            shared_dbs=set(test_shared_dbs),
            spec_dbs=set(test_spec_dbs),
        )

        tables_list_sql = []
        for sql_dict in tables_list:
            for k, v in sql_dict.items():
                if k == "sql":
                    tables_list_sql.append(v)

        # Sort list of SQL queries for readability
        tables_list_sql.sort()

        assert tables_list_sql == expected


class TestGenerateSchemaRevokes:
    def revoke_single_r_schema_config(mocker):
        """
        Generates read REVOKE statements SCHEMA_1
        """
        test_schemas_config = {
            "read": [],
            "write": [],
        }

        test_grants_to_role = {
            "functional_role": {
                "usage": {
                    "schema": ["database_1.schema_1"],
                },
            },
        }

        expected = [
            "REVOKE usage ON schema database_1.schema_1 FROM ROLE functional_role"
        ]

        return [
            test_schemas_config,
            test_grants_to_role,
            expected,
        ]

    def revoke_single_w_schema_config(mocker):
        """
        Generates write REVOKE statements for SCHEMA_1
        """
        test_schemas_config = {
            "read": [],
            "write": [],
        }

        test_grants_to_role = {
            "functional_role": {
                "create table": {
                    "schema": ["database_1.schema_1"],
                },
            },
        }

        expected = [
            "REVOKE monitor, create table, create view, create stage, create file format, create sequence, create function, create pipe ON schema database_1.schema_1 FROM ROLE functional_role"
        ]

        return [
            test_schemas_config,
            test_grants_to_role,
            expected,
        ]

    def revoke_single_rw_schema_config(mocker):
        """
        Generates read/write REVOKE statements for SCHEMA_1
        """
        test_schemas_config = {
            "read": [],
            "write": [],
        }

        test_grants_to_role = {
            "functional_role": {
                "usage": {
                    "schema": ["database_1.schema_1"],
                },
                "create table": {
                    "schema": ["database_1.schema_1"],
                },
            },
        }

        expected = [
            "REVOKE monitor, create table, create view, create stage, create file format, create sequence, create function, create pipe ON schema database_1.schema_1 FROM ROLE functional_role",
            "REVOKE usage ON schema database_1.schema_1 FROM ROLE functional_role",
        ]

        return [
            test_schemas_config,
            test_grants_to_role,
            expected,
        ]

    def revoke_single_r_shared_schema_config(mocker):
        """
        Generates empty read REVOKE statement for
        SHARED_DATABASE_1.SHARED_SCHEMA_1
        """
        test_schemas_config = {
            "read": [],
            "write": [],
        }

        test_grants_to_role = {
            "functional_role": {
                "usage": {
                    "schema": ["shared_database_1.shared_schema_1"],
                }
            },
        }

        expected = []

        return [
            test_schemas_config,
            test_grants_to_role,
            expected,
        ]

    def revoke_single_w_shared_schema_config(mocker):
        """
        Generates empty write REVOKE statement for
        SHARED_DATABASE_1.SHARED_SCHEMA_1
        """
        test_schemas_config = {
            "read": [],
            "write": [],
        }

        test_grants_to_role = {
            "functional_role": {
                "create table": {
                    "schema": ["shared_database_1.shared_schema_1"],
                }
            },
        }

        expected = []

        return [
            test_schemas_config,
            test_grants_to_role,
            expected,
        ]

    def revoke_multi_diff_rw_shared_schema_config(mocker):
        """
        Generates empty read/write REVOKE statement for
        SHARED_DATABASE_1.SHARED_SCHEMA_1, read/write for SCHEMA_1,
        read for SCHEMA_2
        """
        test_schemas_config = {
            "read": [],
            "write": [],
        }

        test_grants_to_role = {
            "functional_role": {
                "create table": {
                    "schema": [
                        "shared_database_1.shared_schema_1",
                        "database_1.schema_1",
                        "database_2.schema_2",
                    ],
                },
                "usage": {
                    "schema": [
                        "shared_database_1.shared_schema_1",
                        "database_1.schema_1",
                    ],
                },
            },
        }

        expected = [
            "REVOKE monitor, create table, create view, create stage, create file format, create sequence, create function, create pipe ON schema database_1.schema_1 FROM ROLE functional_role",
            "REVOKE monitor, create table, create view, create stage, create file format, create sequence, create function, create pipe ON schema database_2.schema_2 FROM ROLE functional_role",
            "REVOKE usage ON schema database_1.schema_1 FROM ROLE functional_role",
        ]

        return [
            test_schemas_config,
            test_grants_to_role,
            expected,
        ]

    @pytest.mark.parametrize(
        "config",
        [
            revoke_single_r_schema_config,
            revoke_single_w_schema_config,
            revoke_single_rw_schema_config,
            revoke_single_r_shared_schema_config,
            revoke_single_w_shared_schema_config,
            revoke_multi_diff_rw_shared_schema_config,
        ],
    )
    def test_generate_schema_revokes(
        self,
        test_shared_dbs,
        test_spec_dbs,
        test_roles_granted_to_user,
        mocker,
        config,
    ):

        test_schemas_config, test_grants_to_role, expected = config(mocker)

        mocker.patch.object(SnowflakeConnector, "__init__", lambda x: None)

        # Generation of database grants should be identical while
        # ignoring or not ignoring memberships
        generator = SnowflakeGrantsGenerator(
            test_grants_to_role,
            test_roles_granted_to_user,
        )

        schemas_list = generator.generate_schema_grants(
            role="functional_role",
            schemas=test_schemas_config,
            shared_dbs=set(test_shared_dbs),
            spec_dbs=set(test_spec_dbs),
        )

        schemas_list_sql = []
        for sql_dict in schemas_list:
            for k, v in sql_dict.items():
                if k == "sql":
                    schemas_list_sql.append(v)

        # Sort list of SQL queries for readability
        schemas_list_sql.sort()

        assert schemas_list_sql == expected


class TestGenerateDatabaseRevokes:
    def revoke_single_r_database_config(mocker):
        """
        Revokes read access on DATABASE_1 database.
        """
        config = {
            "read": [],
            "write": [],
        }

        test_grants_to_role = {
            "functional_role": {"usage": {"database": ["database_1"]}},
        }

        expected = ["REVOKE usage ON database database_1 FROM ROLE functional_role"]

        return [MockSnowflakeConnector, config, expected, test_grants_to_role]

    def revoke_single_w_database_config(mocker):
        """
        Revokes monitor access on DATABASE_1 database.
        """
        config = {
            "read": [],
            "write": [],
        }

        test_grants_to_role = {
            "functional_role": {"monitor": {"database": ["database_1"]}},
        }

        expected = [
            "REVOKE monitor, create schema ON database database_1 FROM ROLE functional_role"
        ]

        return [MockSnowflakeConnector, config, expected, test_grants_to_role]

    def revoke_single_w_database_config_2(mocker):
        """
        Revokes monitor, create schema access on DATABASE_1 database.
        """
        config = {
            "read": [],
            "write": [],
        }

        test_grants_to_role = {
            "functional_role": {
                "create schema": {"database": ["database_1"]},
                "monitor": {"database": ["database_1"]},
            }
        }

        expected = [
            "REVOKE monitor, create schema ON database database_1 FROM ROLE functional_role",
            "REVOKE monitor, create schema ON database database_1 FROM ROLE functional_role",
        ]

        return [MockSnowflakeConnector, config, expected, test_grants_to_role]

    def revoke_single_rw_database_config(mocker):
        """
        Revokes read/write access on DATABASE_1 database.
        """
        config = {
            "read": [],
            "write": [],
        }

        test_grants_to_role = {
            "functional_role": {
                "usage": {"database": ["database_1"]},
                "monitor": {"database": ["database_1"]},
            }
        }

        expected = [
            "REVOKE monitor, create schema ON database database_1 FROM ROLE functional_role",
            "REVOKE usage ON database database_1 FROM ROLE functional_role",
        ]

        return [MockSnowflakeConnector, config, expected, test_grants_to_role]

    def revoke_single_r_shared_db_config(mocker):
        """
        Revokes read access on SHARED_DATABASE_1 database.
        """
        config = {
            "read": [],
            "write": [],
        }

        test_grants_to_role = {
            "functional_role": {
                "usage": {"database": ["shared_database_1"]},
            }
        }

        expected = [
            "REVOKE imported privileges ON database shared_database_1 FROM ROLE functional_role"
        ]

        return [MockSnowflakeConnector, config, expected, test_grants_to_role]

    def revoke_single_w_shared_db_config(mocker):
        """
        Revokes write access on SHARED_DATABASE_1 database.
        """
        config = {
            "read": [],
            "write": [],
        }

        test_grants_to_role = {
            "functional_role": {
                "create schema": {"database": ["shared_database_1"]},
            }
        }

        expected = [
            "REVOKE imported privileges ON database shared_database_1 FROM ROLE functional_role"
        ]

        return [MockSnowflakeConnector, config, expected, test_grants_to_role]

    def revoke_single_rw_shared_db_config(mocker):
        """
        Revokes read/write access on SHARED_DATABASE_1 database.
        """
        config = {
            "read": [],
            "write": [],
        }

        test_grants_to_role = {
            "functional_role": {
                "create schema": {"database": ["shared_database_1"]},
                "usage": {"database": ["shared_database_1"]},
            }
        }

        # TODO: Remove duplicate query generation
        expected = [
            "REVOKE imported privileges ON database shared_database_1 FROM ROLE functional_role",
            "REVOKE imported privileges ON database shared_database_1 FROM ROLE functional_role",
        ]

        return [MockSnowflakeConnector, config, expected, test_grants_to_role]

    def revoke_multi_r_db_config(mocker):
        """
        Revokes read access on DATABASE_1, DATABASE_2 databases.
        """
        config = {
            "read": [],
            "write": [],
        }

        test_grants_to_role = {
            "functional_role": {
                "usage": {"database": ["database_1", "database_2"]},
            }
        }

        expected = [
            "REVOKE usage ON database database_1 FROM ROLE functional_role",
            "REVOKE usage ON database database_2 FROM ROLE functional_role",
        ]

        return [MockSnowflakeConnector, config, expected, test_grants_to_role]

    def revoke_multi_diff_rw_db_config(mocker):
        """
        Revokes read/write on DATABASE_1 and read access on DATABASE_2.
        """
        config = {
            "read": [],
            "write": [],
        }

        test_grants_to_role = {
            "functional_role": {
                "usage": {"database": ["database_1", "database_2"]},
                "create schema": {"database": ["database_1"]},
            }
        }

        expected = [
            "REVOKE monitor, create schema ON database database_1 FROM ROLE functional_role",
            "REVOKE usage ON database database_1 FROM ROLE functional_role",
            "REVOKE usage ON database database_2 FROM ROLE functional_role",
        ]

        return [MockSnowflakeConnector, config, expected, test_grants_to_role]

    def revoke_multi_rw_shared_db_config(mocker):
        """
        Revokes read/write on DATABASE_1, SHARED_DATABASE_1.
        """
        config = {
            "read": [],
            "write": [],
        }

        test_grants_to_role = {
            "functional_role": {
                "usage": {"database": ["database_1", "shared_database_1"]},
                "create schema": {"database": ["database_1", "shared_database_1"]},
            }
        }

        expected = [
            "REVOKE imported privileges ON database shared_database_1 FROM ROLE functional_role",
            "REVOKE imported privileges ON database shared_database_1 FROM ROLE functional_role",
            "REVOKE monitor, create schema ON database database_1 FROM ROLE functional_role",
            "REVOKE usage ON database database_1 FROM ROLE functional_role",
        ]

        return [MockSnowflakeConnector, config, expected, test_grants_to_role]

    @pytest.mark.parametrize("ignore_memberships", [True, False])
    @pytest.mark.parametrize(
        "config",
        [
            revoke_single_r_database_config,
            revoke_single_w_database_config,
            revoke_single_w_database_config_2,
            revoke_single_rw_database_config,
            revoke_single_r_shared_db_config,
            revoke_single_w_shared_db_config,
            revoke_single_rw_shared_db_config,
            revoke_multi_r_db_config,
            revoke_multi_diff_rw_db_config,
            revoke_multi_rw_shared_db_config,
        ],
    )
    def test_generate_database_revokes(
        self,
        test_shared_dbs,
        test_spec_dbs,
        test_roles_granted_to_user,
        mocker,
        config,
        ignore_memberships,
    ):

        mocker.patch.object(SnowflakeConnector, "__init__", lambda x: None)

        mock_connector, test_database_config, expected, test_grants_to_role = config(
            mocker
        )
        # Generation of database grants should be identical while ignoring or not
        # ignoring memberships
        generator = SnowflakeGrantsGenerator(
            test_grants_to_role,
            test_roles_granted_to_user,
            ignore_memberships=ignore_memberships,
        )

        database_list = generator.generate_database_grants(
            "functional_role",
            test_database_config,
            set(test_shared_dbs),
            set(test_spec_dbs),
        )

        database_list_sql = []
        for sql_dict in database_list:
            for k, v in sql_dict.items():
                if k == "sql":
                    database_list_sql.append(v)

        # Sort list of SQL queries for readability
        database_list_sql.sort()

        assert database_list_sql == expected


class TestSnowflakeOwnershipGrants:
    def generate_ownership_on_warehouse(self, entity, entity_name, is_view=False):
        """
        Test that SnowflakeSpecLoader generates ownership grant for an entity.
        Because we treat views and tables as the same entity in the spec, but
        the underlying implementation will differentiate, we have a boolean
        flag for specifying if the tested entity is actually a mocked view.
        """
        grants_to_role = {"test_role": {}}
        roles_granted_to_user = {"user_name": ["test_role"]}
        role = "test_role"
        spec = {"owns": {entity: [entity_name]}}

        if is_view:
            entity = "views"
        expectation = f"GRANT OWNERSHIP ON {entity[:-1]} {entity_name} TO ROLE test_role COPY CURRENT GRANTS"

        return spec, role, grants_to_role, roles_granted_to_user, expectation

    @pytest.mark.parametrize(
        "entity",
        [
            ("databases", "database_1", False),
            ("schemas", "database_1.schema_1", False),
            ("tables", "database_1.schema_1.table_1", False),
            ("tables", "database_1.schema_1.view_1", True),
        ],
    )
    def test_generate_ownership_grants(self, entity, mocker):
        """Test that SnowflakeGrantsGenerator generates ownership grants for a single entity"""
        mock_connector = MockSnowflakeConnector()
        mocker.patch.object(
            mock_connector,
            "show_tables",
            side_effect=[["database_1.schema_1.table_1"], []],
        )
        mocker.patch.object(
            mock_connector,
            "show_views",
            side_effect=[["database_1.schema_1.view_1"], []],
        )

        (
            spec,
            role,
            grants_to_role,
            roles_granted_to_user,
            expectation,
        ) = self.generate_ownership_on_warehouse(entity[0], entity[1], entity[2])
        generator = SnowflakeGrantsGenerator(grants_to_role, roles_granted_to_user)
        generator.conn = mock_connector
        sql_commands = generator.generate_grant_ownership(role, spec)

        assert sql_commands[0]["sql"] == expectation

    def test_generate_database_ownership_grants(
        self, test_grants_to_role, test_roles_granted_to_user, test_role_config, mocker
    ):
        """Test that SnowflakeGrantsGenerator generates ownership grants for for multiple grant definitions"""
        mock_connector = MockSnowflakeConnector()

        generator = SnowflakeGrantsGenerator(
            test_grants_to_role, test_roles_granted_to_user, test_role_config
        )

        generator.conn = mock_connector
        sql_commands = generator.generate_grant_ownership(
            "test_role", test_role_config["functional_role"]
        )

        expected_sql = {
            "GRANT OWNERSHIP ON database database_2 TO ROLE test_role COPY CURRENT GRANTS",
            "GRANT OWNERSHIP ON database database_3 TO ROLE test_role COPY CURRENT GRANTS",
            "GRANT OWNERSHIP ON schema database_1.schemas_1 TO ROLE test_role COPY CURRENT GRANTS",
            "GRANT OWNERSHIP ON schema database_2.schemas_2 TO ROLE test_role COPY CURRENT GRANTS",
            "GRANT OWNERSHIP ON table database_1.schemas_1.tables_1 TO ROLE test_role COPY CURRENT GRANTS",
            "GRANT OWNERSHIP ON table database_2.schemas_1.tables_2 TO ROLE test_role COPY CURRENT GRANTS",
        }

        sql_commands = [sql["sql"] for sql in sql_commands]

        assert set(sql_commands) == set(expected_sql)
