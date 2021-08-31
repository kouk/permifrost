import pytest
import os

from permifrost.core.permissions.entities import EntityGenerator
from permifrost.core.permissions.utils.spec_file_loader import load_spec


THIS_DIR = os.path.dirname(os.path.abspath(__file__))
SPEC_FILE_DIR = os.path.join(THIS_DIR, "specs")
SCHEMA_FILE_DIR = os.path.join(THIS_DIR, "schemas")


@pytest.fixture
def test_dir(request):
    return request.fspath.dirname


@pytest.fixture
def entities(test_dir):
    spec = load_spec(
        os.path.join(test_dir, "specs", "snowflake_spec_reference_roles.yml")
    )
    entities = EntityGenerator(spec).generate()
    yield entities


class TestEntityGenerator:
    def test_entity_databases(self, entities):
        expected = {"demo"}
        assert entities["databases"] == expected

    def test_entity_roles(self, entities):
        expected = {"accountadmin", "demo", "securityadmin", "sysadmin", "useradmin"}

        assert entities["roles"] == expected

    def test_entity_role_refs(self, entities):
        expected = {"demo"}
        assert entities["role_refs"] == expected

    def test_entity_users(self, entities):
        expected = {"airflow_demo", "dbt_demo"}
        assert entities["users"] == expected

    def test_entity_warehouses(self, entities):
        expected = {"demo", "loading", "transforming", "reporting"}
        assert entities["warehouses"] == expected
