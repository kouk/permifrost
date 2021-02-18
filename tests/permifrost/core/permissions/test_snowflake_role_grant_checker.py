import pytest

from permifrost.core.permissions.utils.snowflake_permission import SnowflakePermission
from permifrost.core.permissions.utils.snowflake_role_grant_checker import (
    SnowflakeRoleGrantChecker,
)

from permifrost_test_utils.snowflake_connector import MockSnowflakeConnector


@pytest.fixture
def mock_connector(mocker):
    mocker.patch.object(
        MockSnowflakeConnector,
        "show_grants_to_role_with_grant_option",
        return_value={
            "use": {
                "database": {"db 1": {"grant_option": False}},
                "schema": {"schema 3": {"grant_option": False}},
            },
            "ownership": {
                "schema": {
                    "schema 1": {"grant_option": True},
                    "schema 2": {"grant_option": True},
                },
                "table": {
                    "table 1": {"grant_option": True},
                    "table 2": {"grant_option": True},
                    "table 3": {"grant_option": True},
                },
            },
            "monitor": {
                "warehouse": {
                    "wh 1": {"grant_option": False},
                    "wh 2": {"grant_option": True},
                }
            },
            "manage grants": {"account": {"Account_Name": {"grant_option": False}}},
        },
    )
    return MockSnowflakeConnector()


@pytest.fixture
def grant_checker(mock_connector):
    return SnowflakeRoleGrantChecker(mock_connector)


class TestSnowflakeRoleGrantChecker:
    def test_get_permissions(self, grant_checker, mock_connector):
        permissions = grant_checker.get_permissions("my_role")
        mock_connector.show_grants_to_role_with_grant_option.assert_called_with(
            "my_role"
        )

        assert SnowflakePermission("db 1", "database", ["use"], False) in permissions
        assert (
            SnowflakePermission("wh 2", "warehouse", ["monitor"], False) in permissions
        )
        assert (
            SnowflakePermission("table 3", "table", ["ownership"], False) in permissions
        )

    @pytest.mark.parametrize(
        "permission,expected",
        [
            (SnowflakePermission("table 4", "table", ["ownership"], False), False),
            (SnowflakePermission("table 1", "table", ["ownership"], False), True),
            (SnowflakePermission("table 1", "table", ["select"], False), True),
            (SnowflakePermission("*", "account", ["ownership"], False), False),
            (SnowflakePermission("*", "account", ["manage grants"], False), True),
        ],
    )
    def test_has_permissions(self, grant_checker, permission, expected):
        for p in grant_checker.get_permissions("my_role"):
            print(p)
        assert grant_checker.has_permission("my_role", permission) == expected

    def test_has_permission_no_role(self, grant_checker):
        permission = SnowflakePermission("*", "account", ["ownership"], True)
        assert grant_checker.has_permission(None, permission) == True

    @pytest.mark.parametrize(
        "permission,expected",
        [
            (SnowflakePermission("table 4", "table", ["select"], True), False),
            (SnowflakePermission("schema 1", "schema", ["ownership"], False), True),
            (SnowflakePermission("table 1", "table", ["select"], False), True),
            (SnowflakePermission("*", "account", ["ownership"], False), False),
            (SnowflakePermission("wh 2", "warehouse", ["monitor"], False), True),
            (SnowflakePermission("wh 1", "warehouse", ["monitor"], False), False),
        ],
    )
    def test_can_grant_permission(self, grant_checker, permission, expected):
        print("Permissions for role:")
        for p in grant_checker.get_permissions("my_role"):
            print(p)
        assert grant_checker.can_grant_permission("my_role", permission) == expected

    def test_can_grant_permission_no_role(self, grant_checker):
        permission = SnowflakePermission("*", "account", ["ownership"], True)
        assert grant_checker.can_grant_permission(None, permission) == True
