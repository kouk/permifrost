import pytest
from permifrost.core.permissions.utils.snowflake_connector import SnowflakeConnector

from permifrost.core.permissions.utils.snowflake_grants import SnowflakeGrantsGenerator
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
    def test_generate_grant_roles(
        self,
        test_grants_to_role,
        test_roles_granted_to_user,
        test_role_config,
        test_user_config,
    ):
        generator = SnowflakeGrantsGenerator(
            test_grants_to_role, test_roles_granted_to_user
        )

        role_command_list = generator.generate_grant_roles(
            "roles", "functional_role", test_role_config["functional_role"]
        )

        role_lower_list = [cmd.get("sql", "").lower() for cmd in role_command_list]

        assert "grant role object_role_2 to role functional_role" in role_lower_list
        assert "grant role object_role_3 to role functional_role" in role_lower_list
        assert "revoke role object_role_1 from role functional_role" in role_lower_list

        user_command_list = generator.generate_grant_roles(
            "users", "user_name", test_user_config["user_name"]
        )

        user_lower_list = [cmd.get("sql", "").lower() for cmd in user_command_list]

        assert "grant role object_role to user user_name" in user_lower_list
        assert "grant role user_role to user user_name" in user_lower_list
        assert "revoke role functional_role from user user_name" in user_lower_list

    def test_generate_grant_roles_ignore_membership(
        self, test_grants_to_role, test_roles_granted_to_user
    ):
        generator = SnowflakeGrantsGenerator(
            test_grants_to_role, test_roles_granted_to_user, ignore_memberships=True
        )

        role_command_list = generator.generate_grant_roles(
            "roles", "functional_role", "no_config"
        )

        assert role_command_list == []

    def test_revoke_with_no_member_of(
        self,
        test_grants_to_role,
        test_roles_granted_to_user,
        test_role_config,
    ):
        generator = SnowflakeGrantsGenerator(
            test_grants_to_role, test_roles_granted_to_user
        )

        role_command_list = generator.generate_grant_roles(
            "roles",
            "role_without_member_of",
            test_role_config["role_without_member_of"],
        )

        role_lower_list = [cmd.get("sql", "").lower() for cmd in role_command_list]

        assert (
            "revoke role object_role_1 from role role_without_member_of"
            in role_lower_list
        )

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


class TestGenerateTableAndViewGrants:
    def single_table_config(mocker):
        """
        Provides read/write access on table_1 in
        PUBLIC schema in RAW database.
        """
        mocker.patch.object(
            MockSnowflakeConnector, "show_tables", return_value=["raw.public.table_1"]
        )
        mocker.patch.object(MockSnowflakeConnector, "show_views", return_value=[])

        config = {
            "read": ["raw.public.table_1"],
            "write": ["raw.public.table_1"],
        }

        expected = [
            "GRANT select ON table raw.public.table_1 TO ROLE functional_role",
            "GRANT select, insert, update, delete, truncate, references ON table raw.public.table_1 TO ROLE functional_role",
        ]
        return [MockSnowflakeConnector, config, expected]

    def single_table_shared_db_config(mocker):
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

    def future_tables_config(mocker):
        """
        Provides read/write access on ALL|FUTURE tables|views in
        PUBLIC schema in RAW database.
        """
        mocker.patch.object(
            MockSnowflakeConnector,
            "show_tables",
            return_value=["raw.public.table_1", "raw.public.table_2"],
        )
        mocker.patch.object(MockSnowflakeConnector, "show_views", return_value=[])

        config = {
            "read": [
                "raw.public.*",
            ],
            "write": [
                "raw.public.*",
            ],
        }

        expected = [
            "GRANT select ON FUTURE tables IN schema raw.public TO ROLE functional_role",
            "GRANT select ON FUTURE views IN schema raw.public TO ROLE functional_role",
            "GRANT select ON FUTURE views IN schema raw.public TO ROLE functional_role",
            "GRANT select ON table raw.public.table_1 TO ROLE functional_role",
            "GRANT select ON table raw.public.table_2 TO ROLE functional_role",
            "GRANT select, insert, update, delete, truncate, references ON FUTURE tables IN schema raw.public TO ROLE functional_role",
            "GRANT select, insert, update, delete, truncate, references ON table raw.public.table_1 TO ROLE functional_role",
            "GRANT select, insert, update, delete, truncate, references ON table raw.public.table_2 TO ROLE functional_role",
        ]

        return [MockSnowflakeConnector, config, expected]

    def future_tables_views_config(mocker):
        """
        Provides read/write access on ALL|FUTURE tables|views in
        PUBLIC schema in RAW database.
        """
        mocker.patch.object(
            MockSnowflakeConnector,
            "show_tables",
            return_value=["raw.public.table_1", "raw.public.table_2"],
        )
        mocker.patch.object(
            MockSnowflakeConnector, "show_views", return_value=["raw.public.view_1"]
        )

        config = {
            "read": ["raw.public.*"],
            "write": ["raw.public.*"],
        }

        expected = [
            "GRANT select ON FUTURE tables IN schema raw.public TO ROLE functional_role",
            "GRANT select ON FUTURE views IN schema raw.public TO ROLE functional_role",
            "GRANT select ON FUTURE views IN schema raw.public TO ROLE functional_role",
            "GRANT select ON table raw.public.table_1 TO ROLE functional_role",
            "GRANT select ON table raw.public.table_2 TO ROLE functional_role",
            "GRANT select ON view raw.public.view_1 TO ROLE functional_role",
            "GRANT select ON view raw.public.view_1 TO ROLE functional_role",
            "GRANT select, insert, update, delete, truncate, references ON FUTURE tables IN schema raw.public TO ROLE functional_role",
            "GRANT select, insert, update, delete, truncate, references ON table raw.public.table_1 TO ROLE functional_role",
            "GRANT select, insert, update, delete, truncate, references ON table raw.public.table_2 TO ROLE functional_role",
        ]

        return [MockSnowflakeConnector, config, expected]

    def future_schemas_tables_views_config(mocker):
        """
        Provides read/write on ALL|FUTURE schemas and ALL|FUTURE views|tables
        in RAW database
        """
        mocker.patch.object(
            MockSnowflakeConnector,
            "show_schemas",
            return_value=["raw.public", "raw.public_1"],
        )
        mocker.patch.object(
            MockSnowflakeConnector,
            "show_tables",
            return_value=[
                "raw.public.table_1",
                "raw.public.table_2",
                "raw.public_1.table_3",
            ],
        )
        mocker.patch.object(
            MockSnowflakeConnector, "show_views", return_value=["raw.public.view_1"]
        )
        config = {
            "read": [
                "raw.*.*",
            ],
            "write": [
                "raw.*.*",
            ],
        }

        expected = [
            "GRANT select ON FUTURE tables IN database raw TO ROLE functional_role",
            "GRANT select ON FUTURE tables IN schema raw.public TO ROLE functional_role",
            "GRANT select ON FUTURE tables IN schema raw.public_1 TO ROLE functional_role",
            "GRANT select ON FUTURE views IN database raw TO ROLE functional_role",
            "GRANT select ON FUTURE views IN schema raw.public TO ROLE functional_role",
            "GRANT select ON FUTURE views IN schema raw.public TO ROLE functional_role",
            "GRANT select ON FUTURE views IN schema raw.public_1 TO ROLE functional_role",
            "GRANT select ON FUTURE views IN schema raw.public_1 TO ROLE functional_role",
            "GRANT select ON table raw.public.table_1 TO ROLE functional_role",
            "GRANT select ON table raw.public.table_1 TO ROLE functional_role",
            "GRANT select ON table raw.public.table_2 TO ROLE functional_role",
            "GRANT select ON table raw.public.table_2 TO ROLE functional_role",
            "GRANT select ON table raw.public_1.table_3 TO ROLE functional_role",
            "GRANT select ON table raw.public_1.table_3 TO ROLE functional_role",
            "GRANT select ON view raw.public.view_1 TO ROLE functional_role",
            "GRANT select ON view raw.public.view_1 TO ROLE functional_role",
            "GRANT select ON view raw.public.view_1 TO ROLE functional_role",
            "GRANT select ON view raw.public.view_1 TO ROLE functional_role",
            "GRANT select, insert, update, delete, truncate, references ON FUTURE tables IN database raw TO ROLE functional_role",
            "GRANT select, insert, update, delete, truncate, references ON FUTURE tables IN schema raw.public TO ROLE functional_role",
            "GRANT select, insert, update, delete, truncate, references ON FUTURE tables IN schema raw.public_1 TO ROLE functional_role",
            "GRANT select, insert, update, delete, truncate, references ON FUTURE views IN database raw TO ROLE functional_role",
            "GRANT select, insert, update, delete, truncate, references ON table raw.public.table_1 TO ROLE functional_role",
            "GRANT select, insert, update, delete, truncate, references ON table raw.public.table_1 TO ROLE functional_role",
            "GRANT select, insert, update, delete, truncate, references ON table raw.public.table_2 TO ROLE functional_role",
            "GRANT select, insert, update, delete, truncate, references ON table raw.public.table_2 TO ROLE functional_role",
            "GRANT select, insert, update, delete, truncate, references ON table raw.public_1.table_3 TO ROLE functional_role",
            "GRANT select, insert, update, delete, truncate, references ON table raw.public_1.table_3 TO ROLE functional_role",
        ]

        return [MockSnowflakeConnector, config, expected]

    def partial_rw_future_schemas_tables_views_config(mocker):
        """
        Provides read on ALL|FUTURE schemas and ALL|FUTURE views|tables
        in RAW database, but only write access on ALL|FUTURE tables
        in PUBLIC schema and RAW database.
        """
        # Need to account for different outputs in function
        mocker.patch.object(
            MockSnowflakeConnector,
            "show_schemas",
            # show_schemas called by read before write function call
            side_effect=[["raw.public", "raw.public_1"], ["raw.public"]],
        )
        mocker.patch.object(
            MockSnowflakeConnector,
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
            MockSnowflakeConnector, "show_views", return_value=["raw.public.view_1"]
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
            "GRANT select ON FUTURE tables IN database raw TO ROLE functional_role",
            "GRANT select ON FUTURE tables IN schema raw.public TO ROLE functional_role",
            "GRANT select ON FUTURE tables IN schema raw.public_1 TO ROLE functional_role",
            "GRANT select ON FUTURE views IN database raw TO ROLE functional_role",
            "GRANT select ON FUTURE views IN schema raw.public TO ROLE functional_role",
            "GRANT select ON FUTURE views IN schema raw.public TO ROLE functional_role",
            "GRANT select ON FUTURE views IN schema raw.public_1 TO ROLE functional_role",
            "GRANT select ON table raw.public.table_1 TO ROLE functional_role",
            "GRANT select ON table raw.public.table_2 TO ROLE functional_role",
            "GRANT select ON table raw.public_1.table_3 TO ROLE functional_role",
            "GRANT select ON view raw.public.view_1 TO ROLE functional_role",
            "GRANT select ON view raw.public.view_1 TO ROLE functional_role",
            "GRANT select ON view raw.public.view_1 TO ROLE functional_role",
            "GRANT select, insert, update, delete, truncate, references ON FUTURE tables IN schema raw.public TO ROLE functional_role",
            "GRANT select, insert, update, delete, truncate, references ON table raw.public.table_1 TO ROLE functional_role",
            "GRANT select, insert, update, delete, truncate, references ON table raw.public.table_2 TO ROLE functional_role",
        ]

        return [MockSnowflakeConnector, config, expected]

    @pytest.mark.parametrize(
        "config",
        [
            single_table_config,
            single_table_shared_db_config,
            future_tables_config,
            future_tables_views_config,
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
        # Generation of database grants should be identical while ignoring or not ignoring memberships
        generator = SnowflakeGrantsGenerator(
            test_grants_to_role,
            test_roles_granted_to_user,
        )

        mock_connector, test_tables_config, expected = config(mocker)

        mocker.patch.object(SnowflakeConnector, "__init__", lambda x: None)

        mocker.patch(
            "permifrost.core.permissions.utils.snowflake_grants.SnowflakeConnector.show_schemas",
            mock_connector.show_schemas,
        )

        mocker.patch(
            "permifrost.core.permissions.utils.snowflake_grants.SnowflakeConnector.show_tables",
            mock_connector.show_tables,
        )

        mocker.patch(
            "permifrost.core.permissions.utils.snowflake_grants.SnowflakeConnector.show_views",
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
            "permifrost.core.permissions.utils.snowflake_grants.SnowflakeConnector.show_schemas",
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


class TestGenerateTableAndViewRevokeGrants:
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
            "REVOKE insert, update, delete, truncate, references ON table database_1.schema_1.table_1 FROM ROLE functional_role",
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

        # Generation of database grants should be identical while
        # ignoring or not ignoring memberships
        generator = SnowflakeGrantsGenerator(
            test_grants_to_role,
            test_roles_granted_to_user,
        )

        mocker.patch.object(SnowflakeConnector, "__init__", lambda x: None)

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

        # breakpoint()
        assert tables_list_sql == expected


class TestGenerateSchemaRevokeGrants:
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

    @pytest.mark.parametrize(
        "config",
        [revoke_single_r_schema_config, revoke_single_rw_schema_config],
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

        # Generation of database grants should be identical while
        # ignoring or not ignoring memberships
        generator = SnowflakeGrantsGenerator(
            test_grants_to_role,
            test_roles_granted_to_user,
        )

        mocker.patch.object(SnowflakeConnector, "__init__", lambda x: None)

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


class TestGenerateDatabaseGrants:
    def single_r_database_config(mocker):
        """
        Provides read access on DATABASE_1 database.
        """
        config = {
            "read": ["database_1"],
            "write": [],
        }

        expected = [
            "GRANT usage ON database database_1 TO ROLE functional_role",
            "REVOKE imported privileges ON database shared_database_1 FROM ROLE functional_role",
            "REVOKE monitor, create schema ON database database_1 FROM ROLE functional_role",
            "REVOKE monitor, create schema ON database database_2 FROM ROLE functional_role",
            "REVOKE usage ON database database_2 FROM ROLE functional_role",
        ]
        return [MockSnowflakeConnector, config, expected]

    def single_rw_database_config(mocker):
        """
        Provides read/write access on DATABASE_1 database.
        """
        config = {
            "read": ["database_1"],
            "write": ["database_1"],
        }

        expected = [
            "GRANT usage ON database database_1 TO ROLE functional_role",
            "GRANT usage, monitor, create schema ON database database_1 TO ROLE functional_role",
            "REVOKE imported privileges ON database shared_database_1 FROM ROLE functional_role",
            "REVOKE monitor, create schema ON database database_2 FROM ROLE functional_role",
            "REVOKE usage ON database database_2 FROM ROLE functional_role",
        ]
        return [MockSnowflakeConnector, config, expected]

    def multi_r_database_config(mocker):
        """
        Provides read access on DATABASE_1, DATABASE_2 databases.
        """
        config = {
            "read": ["database_1", "database_2"],
            "write": [],
        }

        expected = [
            "GRANT usage ON database database_1 TO ROLE functional_role",
            "GRANT usage ON database database_2 TO ROLE functional_role",
            "REVOKE imported privileges ON database shared_database_1 FROM ROLE functional_role",
            "REVOKE monitor, create schema ON database database_1 FROM ROLE functional_role",
            "REVOKE monitor, create schema ON database database_2 FROM ROLE functional_role",
        ]
        return [MockSnowflakeConnector, config, expected]

    def multi_rw_database_config(mocker):
        """
        Provides read/write access on DATABASE_1, DATABASE_2 databases.
        """
        config = {
            "read": ["database_1", "database_2"],
            "write": ["database_1", "database_2"],
        }

        expected = [
            "GRANT usage ON database database_1 TO ROLE functional_role",
            "GRANT usage ON database database_2 TO ROLE functional_role",
            "GRANT usage, monitor, create schema ON database database_1 TO ROLE functional_role",
            "GRANT usage, monitor, create schema ON database database_2 TO ROLE functional_role",
            "REVOKE imported privileges ON database shared_database_1 FROM ROLE functional_role",
        ]
        return [MockSnowflakeConnector, config, expected]

    def multi_diff_rw_database_config(mocker):
        """
        Provides read/write access on DATABASE_1 database
        and read access on DATABASE_2 database.
        """
        config = {
            "read": ["database_1", "database_2"],
            "write": ["database_1"],
        }

        expected = [
            "GRANT usage ON database database_1 TO ROLE functional_role",
            "GRANT usage ON database database_2 TO ROLE functional_role",
            "GRANT usage, monitor, create schema ON database database_1 TO ROLE functional_role",
            "REVOKE imported privileges ON database shared_database_1 FROM ROLE functional_role",
            "REVOKE monitor, create schema ON database database_2 FROM ROLE functional_role",
        ]
        return [MockSnowflakeConnector, config, expected]

    def multi_shared_db_rw_database_config(mocker):
        """
        Provides read/write access on
        DATABASE_1, DATABASE_2, SHARED_DATABASE_1 databases.
        """
        config = {
            "read": ["database_1", "database_2", "shared_database_1"],
            "write": ["database_1", "database_2", "shared_database_1"],
        }

        expected = [
            "GRANT imported privileges ON database shared_database_1 TO ROLE functional_role",
            "GRANT imported privileges ON database shared_database_1 TO ROLE functional_role",
            "GRANT usage ON database database_1 TO ROLE functional_role",
            "GRANT usage ON database database_2 TO ROLE functional_role",
            "GRANT usage, monitor, create schema ON database database_1 TO ROLE functional_role",
            "GRANT usage, monitor, create schema ON database database_2 TO ROLE functional_role",
        ]
        return [MockSnowflakeConnector, config, expected]

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

    @pytest.mark.parametrize("ignore_memberships", [True, False])
    @pytest.mark.parametrize(
        "config",
        [
            single_r_database_config,
            single_rw_database_config,
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
        test_grants_to_role,
        test_roles_granted_to_user,
        mocker,
        config,
        ignore_memberships,
    ):

        # Generation of database grants should be identical while ignoring or not
        # ignoring memberships
        generator = SnowflakeGrantsGenerator(
            test_grants_to_role,
            test_roles_granted_to_user,
            ignore_memberships=ignore_memberships,
        )

        mock_connector, test_database_config, expected = config(mocker)

        mocker.patch.object(SnowflakeConnector, "__init__", lambda x: None)

        # mocker.patch(
        #     "permifrost.core.permissions.utils.snowflake_grants.SnowflakeConnector.show_schemas",
        #     mock_connector.show_schemas,
        # )

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
    def generate_ownership_on_warehouse(self, entity, entity_name):
        """
        Test that SnowflakeSpecLoader generates ownership grant for an entity
        """
        grants_to_role = {"test_role": {}}
        roles_granted_to_user = {"user_name": ["test_role"]}
        role = "test_role"
        spec = {"owns": {entity: [entity_name]}}
        expectation = f"GRANT OWNERSHIP ON {entity[:-1]} {entity_name} TO ROLE test_role COPY CURRENT GRANTS"

        return spec, role, grants_to_role, roles_granted_to_user, expectation

    @pytest.mark.parametrize(
        "entity",
        [
            ("databases", "database_1"),
            ("schemas", "database_1.schema_1"),
            ("tables", "database_1.schema_1.table1"),
        ],
    )
    def test_generate_ownership_grants(self, entity):
        """Test that SnowflakeGrantsGenerator generates ownership grants for a single entity"""
        (
            spec,
            role,
            grants_to_role,
            roles_granted_to_user,
            expectation,
        ) = self.generate_ownership_on_warehouse(entity[0], entity[1])
        generator = SnowflakeGrantsGenerator(grants_to_role, roles_granted_to_user)

        sql_commands = generator.generate_grant_ownership(role, spec)

        assert sql_commands[0]["sql"] == expectation

    def test_generate_database_ownership_grants(
        self, test_grants_to_role, test_roles_granted_to_user, test_role_config
    ):
        """Test that SnowflakeGrantsGenerator generates ownership grants for for multiple grant definitions"""

        generator = SnowflakeGrantsGenerator(
            test_grants_to_role, test_roles_granted_to_user, test_role_config
        )
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
