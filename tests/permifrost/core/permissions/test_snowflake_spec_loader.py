import pytest
import os

from permifrost.core.permissions import SpecLoadingError
from permifrost.core.permissions.snowflake_spec_loader import SnowflakeSpecLoader
from permifrost_test_utils.snowflake_schema_builder import SnowflakeSchemaBuilder
from permifrost_test_utils.snowflake_connector import MockSnowflakeConnector


THIS_DIR = os.path.dirname(os.path.abspath(__file__))
SPEC_FILE_DIR = os.path.join(THIS_DIR, "specs")
SCHEMA_FILE_DIR = os.path.join(THIS_DIR, "schemas")


def get_spec_from_file(file_name):
    with open(os.path.join(SPEC_FILE_DIR, file_name), "r") as fd:
        spec_data = fd.read()
    return spec_data


@pytest.fixture
def test_dir(request):
    return request.fspath.dirname


@pytest.fixture
def mock_connector():
    return MockSnowflakeConnector()


@pytest.fixture
def test_roles_spec_file():
    """Semi-robust spec file for testing role filtering."""
    spec_file_data = (
        SnowflakeSchemaBuilder()
        .add_user()
        .add_user(name="testuser")
        .add_db(owner="primary", name="primarydb")
        .add_db(owner="secondary", name="secondarydb")
        .add_warehouse(owner="primary", name="primarywarehouse")
        .add_warehouse(owner="secondary", name="secondarywarehouse")
        .add_role()
        .add_role(name="securityadmin")
        .add_role(name="primary")
        .add_role(name="secondary")
        .build()
    )
    yield spec_file_data


@pytest.fixture
def test_grants_roles_mock_connection(mocker, mock_method, return_value):
    mocker.patch("sqlalchemy.create_engine")
    mock_connector = MockSnowflakeConnector()
    mocker.patch.object(
        mock_connector, "get_current_role", return_value="securityadmin"
    )
    mocker.patch.object(mock_connector, "get_current_user", return_value="testuser")
    mocker.patch.object(
        mock_connector,
        "show_warehouses",
        return_value=["primarywarehouse", "secondarywarehouse"],
    )
    mocker.patch.object(
        mock_connector, "show_databases", return_value=["primarydb", "secondarydb"]
    )
    mocker.patch.object(
        mock_connector, "show_roles", return_value=["testrole", "securityadmin"]
    )
    mocker.patch.object(
        mock_connector, "show_users", return_value=["testuser", "testusername"]
    )
    mocker.patch.object(mock_connector, mock_method, return_value=return_value)
    yield mock_connector


@pytest.fixture()
def test_roles_mock_connector(mocker):
    """Mock connector for use in testing role filtering."""

    mock_connector = MockSnowflakeConnector()
    # Connector Mock Madness
    mocker.patch("sqlalchemy.create_engine")
    mocker.patch.object(
        mock_connector, "get_current_role", return_value="securityadmin"
    )
    mocker.patch.object(mock_connector, "get_current_user", return_value="testuser")
    mocker.patch.object(
        mock_connector,
        "show_warehouses",
        return_value=["primarywarehouse", "secondarywarehouse"],
    )
    mocker.patch.object(
        mock_connector, "show_databases", return_value=["primarydb", "secondarydb"]
    )
    mocker.patch.object(
        mock_connector,
        "show_roles",
        return_value=["primary", "secondary", "testrole", "securityadmin"],
    )
    mocker.patch.object(
        mock_connector, "show_users", return_value=["testuser", "testusername"]
    )
    yield mock_connector


class TestSnowflakeSpecLoader:
    def test_check_entities_on_snowflake_server_no_warehouses(
        self, test_dir, mocker, mock_connector
    ):
        mocker.patch.object(mock_connector, "show_warehouses")
        SnowflakeSpecLoader(
            os.path.join(test_dir, "specs", "snowflake_spec_blank.yml"), mock_connector
        )
        mock_connector.show_warehouses.assert_not_called()

    def test_check_entities_on_snowflake_server_no_databases(
        self, test_dir, mocker, mock_connector
    ):
        mocker.patch.object(mock_connector, "show_databases")
        SnowflakeSpecLoader(
            os.path.join(test_dir, "specs", "snowflake_spec_blank.yml"), mock_connector
        )
        mock_connector.show_databases.assert_not_called()

    def test_check_entities_on_snowflake_server_no_schemas(
        self, test_dir, mocker, mock_connector
    ):
        mocker.patch.object(mock_connector, "show_schemas")
        SnowflakeSpecLoader(
            os.path.join(test_dir, "specs", "snowflake_spec_blank.yml"), mock_connector
        )
        mock_connector.show_schemas.assert_not_called()

    def test_check_entities_on_snowflake_server_no_tables(
        self, test_dir, mocker, mock_connector
    ):
        mocker.patch.object(mock_connector, "show_tables")
        mocker.patch.object(mock_connector, "show_views")
        SnowflakeSpecLoader(
            os.path.join(test_dir, "specs", "snowflake_spec_blank.yml"), mock_connector
        )
        mock_connector.show_tables.assert_not_called()
        mock_connector.show_views.assert_not_called()

    def test_check_entities_on_snowflake_server_no_roles(
        self, test_dir, mocker, mock_connector
    ):
        mocker.patch.object(mock_connector, "show_roles")
        SnowflakeSpecLoader(
            os.path.join(test_dir, "specs", "snowflake_spec_blank.yml"), mock_connector
        )
        mock_connector.show_roles.assert_not_called()

    def test_check_entities_on_snowflake_server_no_users(
        self, test_dir, mocker, mock_connector
    ):
        mocker.patch.object(mock_connector, "show_users")
        SnowflakeSpecLoader(
            os.path.join(test_dir, "specs", "snowflake_spec_blank.yml"), mock_connector
        )
        mock_connector.show_users.assert_not_called()

    def test_check_permissions_on_snowflake_server_as_securityadmin(
        self, test_dir, mocker, mock_connector
    ):
        mocker.patch.object(
            MockSnowflakeConnector, "get_current_role", return_value="securityadmin"
        )
        SnowflakeSpecLoader(
            os.path.join(test_dir, "specs", "snowflake_spec_blank.yml"), mock_connector
        )
        mock_connector.get_current_role.assert_called()

    def test_check_permissions_on_snowflake_server_not_as_securityadmin(
        self, test_dir, mocker, mock_connector
    ):
        mocker.patch.object(
            MockSnowflakeConnector, "get_current_role", return_value="notsecurityadmin"
        )
        with pytest.raises(SpecLoadingError) as context:
            SnowflakeSpecLoader(
                os.path.join(test_dir, "specs", "snowflake_spec_blank.yml"),
                mock_connector,
            )
            mock_connector.get_current_role.assert_called()

    def test_check_permissions_on_snowflake_server_gets_current_user_info(
        self, test_dir, mocker, mock_connector
    ):
        mocker.patch.object(mock_connector, "get_current_user")
        SnowflakeSpecLoader(
            os.path.join(test_dir, "specs", "snowflake_spec_blank.yml"), mock_connector
        )
        mock_connector.get_current_user.assert_called()

    @pytest.mark.parametrize(
        "spec_file_data,method,return_value",
        [
            (
                SnowflakeSchemaBuilder().add_role(owner="user").build(),
                "show_roles",
                {"testrole": "user"},
            ),
            (
                SnowflakeSchemaBuilder().set_version("1.0").build(),
                "show_roles",
                {"testrole": "none"},
            ),
        ],
    )
    def test_check_entities_on_snowflake_server_checks_role_owner(
        self, spec_file_data, method, return_value, mocker, mock_connector
    ):
        print("Spec file is: ")
        print(spec_file_data)
        mocker.patch("builtins.open", mocker.mock_open(read_data=spec_file_data))
        mocker.patch.object(mock_connector, method, return_value=return_value)
        SnowflakeSpecLoader("", mock_connector)

    @pytest.mark.parametrize(
        "spec_file_data,method,return_value,expected_error",
        [
            (
                SnowflakeSchemaBuilder().add_role(owner="user").build(),
                "show_roles",
                {"testrole": "testuser"},
                "Role testrole has owner testuser on snowflake, but has owner user defined in the spec file",
            ),
            (
                SnowflakeSchemaBuilder().add_role(owner="user").build(),
                "show_roles",
                {"some-other-role": "none"},
                "Missing Entity Error: Role testrole was not found on Snowflake Server",
            ),
            (
                SnowflakeSchemaBuilder().add_role().build(),
                "show_roles",
                {},
                "Missing Entity Error: Role testrole was not found on Snowflake Server",
            ),
            (
                SnowflakeSchemaBuilder().add_role(owner="user").build(),
                "show_roles",
                {},
                "Missing Entity Error: Role testrole was not found on Snowflake Server",
            ),
        ],
    )
    def test_check_entities_on_snowflake_server_errors_if_role_owner_does_not_match(
        self,
        spec_file_data,
        method,
        return_value,
        mocker,
        mock_connector,
        expected_error,
    ):
        print("Spec file is: ")
        print(spec_file_data)
        mocker.patch("builtins.open", mocker.mock_open(read_data=spec_file_data))
        mocker.patch.object(mock_connector, method, return_value=return_value)
        with pytest.raises(SpecLoadingError) as context:
            SnowflakeSpecLoader("", mock_connector)

        assert expected_error in str(context.value)

    @pytest.mark.parametrize(
        "mock_method,spec_file_data,return_value,expected_value",
        [
            (
                "show_future_grants",
                get_spec_from_file(
                    "snowflake_server_filters_grants_to_role_to_items_defined_in_config.yml"
                ),
                SnowflakeSchemaBuilder().build_from_file(
                    SCHEMA_FILE_DIR,
                    "snowflake_server_filters_grants_to_role_to_items_defined_in_config_future_grants.json",
                ),
                SnowflakeSchemaBuilder().build_from_file(
                    SCHEMA_FILE_DIR,
                    "snowflake_server_filters_grants_to_role_to_items_defined_in_config_future_grants_expected_values.json",
                ),
            ),
            (
                "show_grants_to_role",
                get_spec_from_file(
                    "snowflake_server_filters_grants_to_role_to_items_defined_in_config.yml"
                ),
                SnowflakeSchemaBuilder().build_from_file(
                    SCHEMA_FILE_DIR,
                    "snowflake_server_filters_grants_to_role_to_items_defined_in_config_grants_to_role.json",
                ),
                SnowflakeSchemaBuilder().build_from_file(
                    SCHEMA_FILE_DIR,
                    "snowflake_server_filters_grants_to_role_to_items_defined_in_config_grants_to_role_expected_values.json",
                ),
            ),
        ],
    )
    def test_check_entities_on_snowflake_server_filters_grants_to_role_to_items_defined_in_config(
        self,
        test_grants_roles_mock_connection,
        mocker,
        mock_method,
        spec_file_data,
        return_value,
        expected_value,
    ):
        mocker.patch("builtins.open", mocker.mock_open(read_data=spec_file_data))
        spec_loader = SnowflakeSpecLoader(
            spec_path="", conn=test_grants_roles_mock_connection
        )

        assert expected_value == spec_loader.grants_to_role

    @pytest.mark.parametrize(
        "database_refs,warehouse_refs,grant_on,filter_set,expected_value",
        [
            # database filter matches config
            (
                ["db1", "db2", "db3"],
                ["warehouse_doesnt_matter"],
                "database",
                ["db1", "db2", "db3"],
                ["db1", "db2", "db3"],
            ),
            # database filter less objects than config
            (
                ["db1", "db2", "db3"],
                ["warehouse_doesnt_matter"],
                "database",
                ["db1"],
                ["db1"],
            ),
            # database filter more objects than config
            (
                ["db1"],
                ["warehouse_doesnt_matter"],
                "database",
                ["db1", "db2", "db3"],
                ["db1"],
            ),
            # account return passed filter set
            (
                ["database_doesnt_matter"],
                ["warehouse_doesnt_matter"],
                "account",
                ["account1", "account2"],
                ["account1", "account2"],
            ),
            # warehouse filter matches config
            (
                ["database_doesnt_matter"],
                ["warehouse1", "warehouse2", "warehouse3"],
                "warehouse",
                ["warehouse1", "warehouse2", "warehouse3"],
                ["warehouse1", "warehouse2", "warehouse3"],
            ),
            # warehouse filter less than config
            (
                ["database_doesnt_matter"],
                ["warehouse1", "warehouse2", "warehouse3"],
                "warehouse",
                ["warehouse1"],
                ["warehouse1"],
            ),
            # warehouse filter more than config
            (
                ["database_doesnt_matter"],
                ["warehouse1"],
                "warehouse",
                ["warehouse1", "warehouse2", "warehouse3"],
                ["warehouse1"],
            ),
            ###
            # everything else with single config db
            ###
            # filter set without dots
            (
                ["db1"],
                ["warehouse_doesnt_matter"],
                "not_really_relevant",
                ["item1", "item2", "item3"],
                ["item1", "item2", "item3"],
            ),
            # filter set with one level dots
            (
                ["db1"],
                ["warehouse_doesnt_matter"],
                "not_really_relevant",
                ["db1.some_item", "db1.some_item2", "db2.some_item"],
                ["db1.some_item", "db1.some_item2"],
            ),
            # filter set with 3 levels of dots
            (
                ["db1"],
                ["warehouse_doesnt_matter"],
                "not_really_relevant",
                [
                    "db1.some_item.sub_item.sub_sub_item",
                    "db1.some_item.sub_item.sub_sub_item",
                    "db2.some_item.sub_item.sub_sub_item",
                ],
                [
                    "db1.some_item.sub_item.sub_sub_item",
                    "db1.some_item.sub_item.sub_sub_item",
                ],
            ),
            # filter set with 3 levels of dots no matching db
            (
                ["db1"],
                ["warehouse_doesnt_matter"],
                "not_really_relevant",
                [
                    "db2.some_item.sub_item.sub_sub_item",
                    "db2.some_item.sub_item.sub_sub_item",
                    "db2.some_item.sub_item.sub_sub_item",
                ],
                [],
            ),
        ],
    )
    def test_filter_to_database_refs(
        self,
        mocker,
        database_refs,
        warehouse_refs,
        grant_on,
        filter_set,
        expected_value,
    ):
        mocker.patch.object(SnowflakeSpecLoader, "__init__", lambda *args: None)
        spec_loader = SnowflakeSpecLoader("", None)
        spec_loader.entities = {
            "database_refs": database_refs,
            "warehouse_refs": warehouse_refs,
        }
        spec_loader.filter_to_database_refs(grant_on=grant_on, filter_set=filter_set)

    def test_load_spec_loads_file(self, mocker, mock_connector):
        mock_open = mocker.patch(
            "builtins.open", mocker.mock_open(read_data="""version: "1.0" """)
        )
        filepath = "filepath to open"
        SnowflakeSpecLoader(filepath, mock_connector)

        mock_open.assert_called_once_with(filepath, "r")

    @pytest.mark.parametrize(
        "spec_file_data,method,return_value",
        [
            (
                SnowflakeSchemaBuilder().add_db(owner="user").build(),
                "show_databases",
                ["testdb"],
            ),
            (
                SnowflakeSchemaBuilder().add_role(owner="user").build(),
                "show_roles",
                {"testrole": "user"},
            ),
            (
                SnowflakeSchemaBuilder().add_user(owner="user").build(),
                "show_users",
                ["testusername"],
            ),
            (
                SnowflakeSchemaBuilder().add_warehouse(owner="user").build(),
                "show_warehouses",
                ["testwarehouse"],
            ),
            (
                SnowflakeSchemaBuilder().require_owner().add_db(owner="user").build(),
                "show_databases",
                ["testdb"],
            ),
            (
                SnowflakeSchemaBuilder().require_owner().add_role(owner="user").build(),
                "show_roles",
                {"testrole": "user"},
            ),
            (
                SnowflakeSchemaBuilder().require_owner().add_user(owner="user").build(),
                "show_users",
                ["testusername"],
            ),
            (
                SnowflakeSchemaBuilder()
                .require_owner()
                .add_warehouse(owner="user")
                .build(),
                "show_warehouses",
                ["testwarehouse"],
            ),
        ],
    )
    def test_load_spec_with_owner(
        self, spec_file_data, method, return_value, mocker, mock_connector
    ):
        print("Spec file is: ")
        print(spec_file_data)
        mocker.patch("builtins.open", mocker.mock_open(read_data=spec_file_data))
        mocker.patch.object(mock_connector, method, return_value=return_value)
        SnowflakeSpecLoader("", mock_connector)

    @pytest.mark.parametrize(
        "spec_file_data,method,return_value",
        [
            (
                SnowflakeSchemaBuilder().require_owner().add_db().build(),
                "show_databases",
                ["testdb"],
            ),
            (
                SnowflakeSchemaBuilder().require_owner().add_role().build(),
                "show_roles",
                ["testrole"],
            ),
            (
                SnowflakeSchemaBuilder().require_owner().add_user().build(),
                "show_users",
                ["testusername"],
            ),
            (
                SnowflakeSchemaBuilder().require_owner().add_warehouse().build(),
                "show_warehouses",
                ["testwarehouse"],
            ),
        ],
    )
    def test_load_spec_owner_required_with_no_owner(
        self, spec_file_data, method, return_value, mocker, mock_connector
    ):
        print("Spec file is: ")
        print(spec_file_data)
        mocker.patch("builtins.open", mocker.mock_open(read_data=spec_file_data))
        mocker.patch.object(mock_connector, method, return_value=return_value)
        with pytest.raises(SpecLoadingError) as context:
            SnowflakeSpecLoader("", mock_connector)

        assert "Spec Error: Owner not defined" in str(context.value)

    def test_generate_permission_queries_with_requires_owner(
        self, mocker, mock_connector
    ):
        spec_file_data = (
            SnowflakeSchemaBuilder().set_version("1.0").require_owner().build()
        )
        print("Spec file is: ")
        print(spec_file_data)
        mocker.patch("builtins.open", mocker.mock_open(read_data=spec_file_data))
        loader = SnowflakeSpecLoader("", mock_connector)
        queries = loader.generate_permission_queries()

        assert [] == queries

    def test_role_filter(self, mocker, test_roles_mock_connector, test_roles_spec_file):
        """Make sure that the grant queries list can be filtered by role."""

        print(f"Spec File Data is:\n{test_roles_spec_file}")
        mocker.patch("builtins.open", mocker.mock_open(read_data=test_roles_spec_file))
        spec_loader = SnowflakeSpecLoader(spec_path="", conn=test_roles_mock_connector)
        assert spec_loader.generate_permission_queries(
            roles=["primary"], run_list=["roles"]
        ) == [{"already_granted": False, "sql": "GRANT ROLE testrole TO role primary"}]

    def test_role_filter_multiple(
        self, mocker, test_roles_mock_connector, test_roles_spec_file
    ):
        """Make sure that the grant queries list can be filtered by multiple roles."""

        print(f"Spec File Data is:\n{test_roles_spec_file}")
        mocker.patch("builtins.open", mocker.mock_open(read_data=test_roles_spec_file))
        spec_loader = SnowflakeSpecLoader(spec_path="", conn=test_roles_mock_connector)
        results = spec_loader.generate_permission_queries(
            roles=["primary", "secondary"], run_list=["roles"]
        )
        expected_results = [
            {"already_granted": False, "sql": "GRANT ROLE testrole TO role primary"},
            {"already_granted": False, "sql": "GRANT ROLE testrole TO role secondary"},
        ]
        assert results == expected_results

    def test_role_filter_and_user_filter(
        self, mocker, test_roles_mock_connector, test_roles_spec_file
    ):
        """Make sure that the grant queries list can be filtered by multiple roles and a single user ignores the user"""

        print(f"Spec File Data is:\n{test_roles_spec_file}")
        mocker.patch("builtins.open", mocker.mock_open(read_data=test_roles_spec_file))
        spec_loader = SnowflakeSpecLoader(spec_path="", conn=test_roles_mock_connector)
        results = spec_loader.generate_permission_queries(
            roles=["primary", "secondary"],
            users=["testusername"],
            run_list=["roles", "users"],
        )
        expected_results = [
            {"already_granted": False, "sql": "GRANT ROLE testrole TO role primary"},
            {"already_granted": False, "sql": "GRANT ROLE testrole TO role secondary"},
            {
                "already_granted": False,
                "sql": "ALTER USER testusername SET DISABLED = FALSE",
            },
        ]
        assert results == expected_results

    def test_no_role_or_user_filter(
        self, mocker, test_roles_mock_connector, test_roles_spec_file
    ):
        """Test that the generate_permissions_query does no filtering on when users and roles are not defined."""

        print(f"Spec File Data is:\n{test_roles_spec_file}")
        mocker.patch("builtins.open", mocker.mock_open(read_data=test_roles_spec_file))
        spec_loader = SnowflakeSpecLoader(spec_path="", conn=test_roles_mock_connector)

        expected_sql_queries = [
            {"already_granted": False, "sql": "GRANT ROLE testrole TO role testrole"},
            {
                "already_granted": False,
                "sql": "GRANT ROLE testrole TO role securityadmin",
            },
            {"already_granted": False, "sql": "GRANT ROLE testrole TO role primary"},
            {"already_granted": False, "sql": "GRANT ROLE testrole TO role secondary"},
            {
                "already_granted": False,
                "sql": "ALTER USER testusername SET DISABLED = FALSE",
            },
            {
                "already_granted": False,
                "sql": "ALTER USER testuser SET DISABLED = FALSE",
            },
        ]

        assert spec_loader.generate_permission_queries() == expected_sql_queries

    def test_user_filter(self, mocker, test_roles_mock_connector, test_roles_spec_file):
        """Make sure that the grant queries list can be filtered by user."""

        print(f"Spec File Data is:\n{test_roles_spec_file}")
        mocker.patch("builtins.open", mocker.mock_open(read_data=test_roles_spec_file))
        spec_loader = SnowflakeSpecLoader(spec_path="", conn=test_roles_mock_connector)
        assert spec_loader.generate_permission_queries(
            users=["testusername"], run_list=["users"]
        ) == [
            {
                "already_granted": False,
                "sql": "ALTER USER testusername SET DISABLED = FALSE",
            }
        ]

    def test_user_filter_multiple(
        self, mocker, test_roles_mock_connector, test_roles_spec_file
    ):
        """Make sure that the grant queries list can be filtered by multiple users."""

        print(f"Spec File Data is:\n{test_roles_spec_file}")
        mocker.patch("builtins.open", mocker.mock_open(read_data=test_roles_spec_file))
        spec_loader = SnowflakeSpecLoader(spec_path="", conn=test_roles_mock_connector)
        results = spec_loader.generate_permission_queries(
            users=["testusername", "testuser"], run_list=["users"]
        )
        expected_results = [
            {
                "already_granted": False,
                "sql": "ALTER USER testusername SET DISABLED = FALSE",
            },
            {
                "already_granted": False,
                "sql": "ALTER USER testuser SET DISABLED = FALSE",
            },
        ]
        assert results == expected_results

    def test_user_filter_and_roles_filter(
        self, mocker, test_roles_mock_connector, test_roles_spec_file
    ):
        """Make sure that the grant queries list can be filtered by multiple users and a single role ignores the role"""

        print(f"Spec File Data is:\n{test_roles_spec_file}")
        mocker.patch("builtins.open", mocker.mock_open(read_data=test_roles_spec_file))
        spec_loader = SnowflakeSpecLoader(spec_path="", conn=test_roles_mock_connector)
        results = spec_loader.generate_permission_queries(
            users=["testusername", "testuser"],
            roles=["primary"],
            run_list=["roles", "users"],
        )
        expected_results = [
            {"already_granted": False, "sql": "GRANT ROLE testrole TO role primary"},
            {
                "already_granted": False,
                "sql": "ALTER USER testusername SET DISABLED = FALSE",
            },
            {
                "already_granted": False,
                "sql": "ALTER USER testuser SET DISABLED = FALSE",
            },
        ]
        assert results == expected_results

    @pytest.mark.parametrize(
        "users,roles,run_list,expected_calls",
        [
            # Only users with full run_list
            (
                ["testusername", "testuser"],
                [],
                ["roles", "users"],
                [
                    ("get_role_privileges_from_snowflake_server", 1, {}),
                    (
                        "get_user_privileges_from_snowflake_server",
                        1,
                        {"users": ["testusername", "testuser"]},
                    ),
                ],
            ),
            # Only Roles passed with full run_list
            (
                [],
                ["primary"],
                ["roles", "users"],
                [
                    (
                        "get_role_privileges_from_snowflake_server",
                        1,
                        {"roles": ["primary"], "ignore_memberships": False},
                    ),
                    ("get_user_privileges_from_snowflake_server", 1, {"users": []}),
                ],
            ),
            # Users and roles passed but roles not in run_list
            (
                ["testusername", "testuser"],
                ["primary"],
                ["users"],
                [
                    ("get_role_privileges_from_snowflake_server", 0, {}),
                    (
                        "get_user_privileges_from_snowflake_server",
                        1,
                        {"users": ["testusername", "testuser"]},
                    ),
                ],
            ),
            # Users and roles passed but users not in run_list
            (
                ["testusername", "testuser"],
                ["primary"],
                ["roles"],
                [
                    (
                        "get_role_privileges_from_snowflake_server",
                        1,
                        {"roles": ["primary"], "ignore_memberships": False},
                    ),
                    ("get_user_privileges_from_snowflake_server", 0, {}),
                ],
            ),
            # Only Users passed with only users in run_list
            (
                ["testusername", "testuser"],
                [],
                ["users"],
                [
                    ("get_role_privileges_from_snowflake_server", 0, {}),
                    (
                        "get_user_privileges_from_snowflake_server",
                        1,
                        {"users": ["testusername", "testuser"]},
                    ),
                ],
            ),
            # Only Roles passed with only roles in run_list
            (
                [],
                ["primary"],
                ["roles"],
                [
                    (
                        "get_role_privileges_from_snowflake_server",
                        1,
                        {"roles": ["primary"], "ignore_memberships": False},
                    ),
                    ("get_user_privileges_from_snowflake_server", 0, {}),
                ],
            ),
            # Users and Roles passed with users and roles in run_list
            (
                ["testusername", "testuser"],
                ["primary"],
                ["roles", "users"],
                [
                    (
                        "get_role_privileges_from_snowflake_server",
                        1,
                        {"roles": ["primary"], "ignore_memberships": False},
                    ),
                    (
                        "get_user_privileges_from_snowflake_server",
                        1,
                        {"users": ["testusername", "testuser"]},
                    ),
                ],
            ),
            # Users and Roles passed with empty list run_list
            (
                ["testusername", "testuser"],
                ["primary"],
                [],
                [
                    (
                        "get_role_privileges_from_snowflake_server",
                        1,
                        {"roles": ["primary"], "ignore_memberships": False},
                    ),
                    (
                        "get_user_privileges_from_snowflake_server",
                        1,
                        {"users": ["testusername", "testuser"]},
                    ),
                ],
            ),
            # Users and Roles passed with None run_list
            (
                ["testusername", "testuser"],
                ["primary"],
                None,
                [
                    (
                        "get_role_privileges_from_snowflake_server",
                        1,
                        {"roles": ["primary"], "ignore_memberships": False},
                    ),
                    (
                        "get_user_privileges_from_snowflake_server",
                        1,
                        {"users": ["testusername", "testuser"]},
                    ),
                ],
            ),
        ],
    )
    def test_get_privileges_from_snowflake_server(
        self,
        mocker,
        test_roles_mock_connector,
        test_roles_spec_file,
        users,
        roles,
        run_list,
        expected_calls,
    ):
        """Verify correct calls when getting privs from server"""

        print(f"Spec File Data is:\n{test_roles_spec_file}")
        mocker.patch("builtins.open", mocker.mock_open(read_data=test_roles_spec_file))
        mock_get_role_privileges_from_snowflake_server = mocker.patch.object(
            SnowflakeSpecLoader,
            "get_role_privileges_from_snowflake_server",
            return_value=None,
        )
        mock_get_user_privileges_from_snowflake_server = mocker.patch.object(
            SnowflakeSpecLoader,
            "get_user_privileges_from_snowflake_server",
            return_value=None,
        )
        SnowflakeSpecLoader(
            spec_path="",
            conn=test_roles_mock_connector,
            users=users,
            roles=roles,
            run_list=run_list,
        )
        for method, call_count, arguments in expected_calls:
            if method == "get_role_privileges_from_snowflake_server":
                assert (
                    mock_get_role_privileges_from_snowflake_server.call_count
                    == call_count
                )
                if arguments:
                    mock_get_role_privileges_from_snowflake_server.assert_called_with(
                        conn=test_roles_mock_connector, **arguments
                    )
            if method == "get_user_privileges_from_snowflake_server":
                assert (
                    mock_get_user_privileges_from_snowflake_server.call_count
                    == call_count
                )
                if arguments:
                    mock_get_user_privileges_from_snowflake_server.assert_called_with(
                        conn=test_roles_mock_connector, **arguments
                    )
