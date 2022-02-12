from typing import Dict, List, Any
from permifrost.core.permissions.utils.snowflake_connector import SnowflakeConnector


class MockSnowflakeConnector(SnowflakeConnector):
    def show_databases(self) -> List[str]:
        return []

    def show_warehouses(self) -> List[str]:
        return []

    def show_roles(self) -> Dict[str, str]:
        return {}

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
        return {}

    def show_grants_to_role_with_grant_option(self, role) -> Dict[str, Any]:
        return {}

    def show_roles_granted_to_user(self, user) -> List[str]:
        return []

    def get_current_user(self) -> str:
        return ""

    def get_current_role(self) -> str:
        return "securityadmin"

    def full_schema_list(self, schema: str) -> List[str]:
        return []
