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
                ["testrole"],
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
                ["testrole"],
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

    def test_role_filter(self, mocker, mock_connector, test_dir):
        """Make sure that the grant queries list can be filtered by role."""

        spec_loader = SnowflakeSpecLoader(
            spec_path=os.path.join(test_dir, "specs", "snowflake_spec.yml"),
            conn=mock_connector,
            debug=True,
        )
        sql_grant_queries = spec_loader.generate_permission_queries()

        assert 1
