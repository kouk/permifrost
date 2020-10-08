import pytest

from permifrost_test_utils.snowflake_schema_builder import SnowflakeSchemaBuilder
from permifrost_test_utils.snowflake_connector import MockSnowflakeConnector


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
