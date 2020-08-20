import pytest
from typing import Dict, List, Any


class MockSnowflakeConnector:
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

    def get_current_user(self) -> str:
        return ""

    def get_current_role(self) -> str:
        return "securityadmin"
