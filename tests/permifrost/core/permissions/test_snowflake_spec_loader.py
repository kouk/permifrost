import pytest
import os

from permifrost.core.permissions.snowflake_spec_loader import SnowflakeSpecLoader
from typing import Dict, List, Any

class MockSnowflakeConnector():
    def show_databases(self) -> List[str]:
        return []

    def show_warehouses(self) -> List[str]:
        return []

    def show_roles(self) -> List[str]:
        return []

    def show_users(self) -> List[str]:
        return []

    def show_schemas(self, database: str = None) -> List[str]:
        return []

    def show_tables(self, database: str = None, schema: str = None) -> List[str]:
        return []

    def show_views(self, database: str = None, schema: str = None) -> List[str]:
        return []

    def show_future_grants(self, database: str = None, schema: str = None) -> List[str]:
        return []

    def show_grants_to_role(self, role) -> Dict[str, Any]:
        return []

    def show_roles_granted_to_user(self, user) -> List[str]:
        return []

@pytest.fixture
def mock_connector():
    return MockSnowflakeConnector()

@pytest.fixture
def test_dir(request):
    return request.fspath.dirname

class TestSnowflakeSpecLoader:

    def test_check_entities_on_snowflake_server_no_warehouses(self, test_dir, mocker, mock_connector):
        mocker.patch.object(MockSnowflakeConnector, 'show_warehouses')
        SnowflakeSpecLoader(os.path.join(test_dir, "specs", "snowflake_spec_blank.yml"), mock_connector)
        mock_connector.show_warehouses.assert_not_called()

    def test_check_entities_on_snowflake_server_no_databases(self, test_dir, mocker, mock_connector):
        mocker.patch.object(MockSnowflakeConnector, 'show_databases')
        SnowflakeSpecLoader(os.path.join(test_dir, "specs", "snowflake_spec_blank.yml"), mock_connector)
        mock_connector.show_databases.assert_not_called()

    def test_check_entities_on_snowflake_server_no_schemas(self, test_dir, mocker, mock_connector):
        mocker.patch.object(MockSnowflakeConnector, 'show_schemas')
        SnowflakeSpecLoader(os.path.join(test_dir, "specs", "snowflake_spec_blank.yml"), mock_connector)
        mock_connector.show_schemas.assert_not_called()

    def test_check_entities_on_snowflake_server_no_tables(self, test_dir, mocker, mock_connector):
        mocker.patch.object(MockSnowflakeConnector, 'show_tables')
        mocker.patch.object(MockSnowflakeConnector, 'show_views')
        SnowflakeSpecLoader(os.path.join(test_dir, "specs", "snowflake_spec_blank.yml"), mock_connector)
        mock_connector.show_tables.assert_not_called()
        mock_connector.show_views.assert_not_called()

    def test_check_entities_on_snowflake_server_no_roles(self, test_dir, mocker, mock_connector):
        mocker.patch.object(MockSnowflakeConnector, 'show_roles')
        SnowflakeSpecLoader(os.path.join(test_dir, "specs", "snowflake_spec_blank.yml"), mock_connector)
        mock_connector.show_roles.assert_not_called()

    def test_check_entities_on_snowflake_server_no_users(self, test_dir, mocker, mock_connector):
        mocker.patch.object(MockSnowflakeConnector, 'show_users')
        SnowflakeSpecLoader(os.path.join(test_dir, "specs", "snowflake_spec_blank.yml"), mock_connector)
        mock_connector.show_users.assert_not_called()
