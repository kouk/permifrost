import pytest
import os

from permifrost.core.permissions import SpecLoadingError
from permifrost.core.permissions.snowflake_spec_loader import SnowflakeSpecLoader
from permifrost_test_utils.snowflake_schema_builder import SnowflakeSchemaBuilder
from permifrost_test_utils.snowflake_connector import MockSnowflakeConnector


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
        mock_connector,
        "show_databases",
        return_value=["primarydb", "secondarydb"],
    )
    mocker.patch.object(
        mock_connector,
        "show_roles",
        return_value=["primary", "secondary", "testrole", "securityadmin"],
    )
    mocker.patch.object(
        mock_connector,
        "show_users",
        return_value=["testuser", "testusername"],
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

<<<<<<< HEAD
    def test_role_filter(self, mocker, mock_connector):
=======
    def test_role_filter(self, mocker, test_roles_mock_connector, test_roles_spec_file):
>>>>>>> move the conflicted test up
        """Make sure that the grant queries list can be filtered by role."""

        print(f"Spec File Data is:\n{test_roles_spec_file}")
        mocker.patch("builtins.open", mocker.mock_open(read_data=test_roles_spec_file))
        spec_loader = SnowflakeSpecLoader(spec_path="", conn=test_roles_mock_connector)

        assert spec_loader.generate_permission_queries(role="primary") == [
            {"already_granted": False, "sql": "GRANT ROLE testrole TO role primary"}
        ]

    def test_no_role_filter(
        self, mocker, test_roles_mock_connector, test_roles_spec_file
    ):
        """Test that the generate_permissions_query does no filtering on
        receipt of a None value for the role to filter."""

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
