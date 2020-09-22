import pytest
import os
import sqlalchemy

from permifrost.core.permissions.utils.snowflake_connector import SnowflakeConnector


@pytest.fixture
def snowflake_connector_env():
    os.environ["PERMISSION_BOT_USER"] = "TEST"
    os.environ["PERMISSION_BOT_PASSWORD"] = "TEST"
    os.environ["PERMISSION_BOT_ACCOUNT"] = "TEST"
    os.environ["PERMISSION_BOT_DATABASE"] = "TEST"
    os.environ["PERMISSION_BOT_ROLE"] = "TEST"
    os.environ["PERMISSION_BOT_WAREHOUSE"] = "TEST"


class TestSnowflakeConnector:
    def test_snowflaky(self):

        db1 = "analytics.schema.table"
        db2 = "1234raw.schema.table"
        db3 = '"123-with-quotes".schema.table'
        db4 = "1_db-9-RANDOM.schema.table"

        assert SnowflakeConnector.snowflaky(db1) == "analytics.schema.table"
        assert SnowflakeConnector.snowflaky(db2) == "1234raw.schema.table"
        assert SnowflakeConnector.snowflaky(db3) == '"123-with-quotes".schema.table'
        assert SnowflakeConnector.snowflaky(db4) == '"1_db-9-RANDOM".schema.table'

    def test_uses_oauth_if_available(selfself, mocker, snowflake_connector_env):
        mocker.patch("sqlalchemy.create_engine")
        os.environ["PERMISSION_BOT_OAUTH_TOKEN"] = "TEST"
        SnowflakeConnector()
        del os.environ["PERMISSION_BOT_OAUTH_TOKEN"]
        sqlalchemy.create_engine.assert_called_with(
            "snowflake://TEST:@TEST/?authenticator=oauth&token=TEST&warehouse=TEST"
        )

    def test_uses_username_password_by_default(
        selfself, mocker, snowflake_connector_env
    ):
        mocker.patch("sqlalchemy.create_engine")
        SnowflakeConnector()
        sqlalchemy.create_engine.assert_called_with(
            "snowflake://TEST:TEST@TEST/TEST?role=TEST&warehouse=TEST"
        )

    def test_run_query_executes_desired_query(self, mocker):
        mocker.patch("sqlalchemy.create_engine")
        conn = SnowflakeConnector()
        query = "MY FUN TESTING QUERY"

        conn.run_query(query)

        conn.engine.assert_has_calls([mocker.call.connect().__enter__().execute(query)])

    def test_run_query_returns_results(self, mocker):
        mocker.patch("sqlalchemy.create_engine")
        conn = SnowflakeConnector()
        expectedResult = "MY DATABASE RESULT"
        mocker.patch.object(
            conn.engine.connect().__enter__(), "execute", return_value=expectedResult
        )

        result = conn.run_query("query")

        assert result is expectedResult

    def test_get_current_user(self, mocker):
        mocker.patch("sqlalchemy.create_engine")
        conn = SnowflakeConnector()
        conn.run_query = mocker.MagicMock()
        mocker.patch.object(
            conn.run_query(), "fetchone", return_value={"user": "TEST_USER"}
        )

        user = conn.get_current_user()

        conn.run_query.assert_has_calls([mocker.call("SELECT CURRENT_USER() AS USER")])
        assert user == "test_user"

    def test_get_current_role(self, mocker):
        mocker.patch("sqlalchemy.create_engine")
        conn = SnowflakeConnector()
        conn.run_query = mocker.MagicMock()
        mocker.patch.object(
            conn.run_query(), "fetchone", return_value={"role": "TEST_ROLE"}
        )

        role = conn.get_current_role()

        conn.run_query.assert_has_calls([mocker.call("SELECT CURRENT_ROLE() AS ROLE")])
        assert role == "test_role"

    def test_show_roles(self, mocker):
        mocker.patch("sqlalchemy.create_engine")
        conn = SnowflakeConnector()
        conn.run_query = mocker.MagicMock()
        mocker.patch.object(conn.run_query(), "fetchall", return_value=[
            {"name": "TEST_ROLE", "owner": "SUPERADMIN"},
            {"name": "SUPERADMIN", "owner": "SUPERADMIN"}
        ])

        roles = conn.show_roles()

        conn.run_query.assert_has_calls([mocker.call("SHOW ROLES;")])
        assert roles["test_role"] == "superadmin"
        assert roles["superadmin"] == "superadmin"
