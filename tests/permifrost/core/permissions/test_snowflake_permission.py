from permifrost.core.permissions.utils.snowflake_permission import SnowflakePermission


class TestSnowflakePermission:
    def test_with_entity_name(self):
        permission = SnowflakePermission(
            "test_name", "test_type", ["priv 1", "priv 2"], False
        )

        updated_permission = permission.with_entity_name("new_name")

        assert updated_permission == permission  # We update the entity in place
        assert (
            updated_permission.entity_name == "new_name"
        )  # name should be what we set above

    def test_contains_any_contains_one(self):
        permission = SnowflakePermission(
            "test_name", "test_type", ["priv 1", "priv 2"], False
        )

        assert permission.contains_any(["priv 1"]) is True

    def test_contains_any_contains_none(self):
        permission = SnowflakePermission(
            "test_name", "test_type", ["priv 1", "priv 2"], False
        )

        assert permission.contains_any(["priv 3"]) is False
