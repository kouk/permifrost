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
