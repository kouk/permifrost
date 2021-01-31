import pytest

from permifrost.core.permissions.utils.snowflake_permission import SnowflakePermission
from permifrost.core.permissions.utils.snowflake_grant_checker import (
    SnowflakeGrantChecker,
)

from permifrost_test_utils.snowflake_connector import MockSnowflakeConnector


@pytest.fixture
def mock_connector(mocker):
    mocker.patch.object(
        MockSnowflakeConnector,
        "show_grants_to_role",
        return_value={
            "use": {"database": ["db 1"], "schema": ["schema 3"]},
            "ownership": {
                "schema": ["schema 1", "schema 2"],
                "table": ["table 1", "table 2", "table 3"],
            },
            "monitor": {"warehouse": ["wh 1", "wh 2"]},
            "manage grants": {"account": []},
        },
    )
    return MockSnowflakeConnector()


@pytest.fixture
def grant_checker(mock_connector):
    return SnowflakeGrantChecker(mock_connector)


class TestSnowflakeGrantChecker:
    def test_get_permissions(self, grant_checker, mock_connector):
        permissions = grant_checker.get_permissions("my_role")
        mock_connector.show_grants_to_role.assert_called_with("my_role")

        assert SnowflakePermission("db 1", "database", ["use"]) in permissions
        assert SnowflakePermission("wh 2", "warehouse", ["monitor"]) in permissions
        assert SnowflakePermission("table 3", "table", ["ownership"]) in permissions

    @pytest.mark.parametrize(
        "permission,expected",
        [
            (SnowflakePermission("table 4", "table", ["ownership"]), False),
            (SnowflakePermission("table 1", "table", ["ownership"]), True),
            (SnowflakePermission("table 1", "table", ["select"]), True),
            (SnowflakePermission("*", "account", ["ownership"]), False),
            (SnowflakePermission("*", "account", ["manage grants"]), True),
        ],
    )
    def test_has_permissions(self, grant_checker, permission, expected):
        assert grant_checker.has_permission("my_role", permission) == expected

    def test_has_permission_no_role(self, grant_checker):
        assert (
            grant_checker.has_permission(
                None, SnowflakePermission("*", "account", ["ownership"])
            )
            == True
        )
