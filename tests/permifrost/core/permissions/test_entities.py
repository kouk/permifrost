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
        expected = {"demo", "shared_demo"}
        assert entities["databases"] == expected

    def test_entity_require_owner(self, entities):
        assert entities["require_owner"] is True

    def test_db_refs(self, entities):
        expected = {"demodb", "demodb2", "demodb3", "demodb4", "demodb5", "demodb6"}
        assert entities["database_refs"] == expected

    def test_schema_refs(self, entities):
        expected = {
            "demodb.*",
            "demodb2.*",
            "demodb3.read_only_schema",
            "demodb4.write_schema",
            "demodb5.demo_schema",
            "demodb6.demo_schema",
        }
        assert entities["schema_refs"] == expected

    def test_entity_roles(self, entities):
        expected = {
            "*",
            "accountadmin",
            "demo",
            "securityadmin",
            "sysadmin",
            "useradmin",
        }
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


def test_filter_by_type(entities):
    expected = {"demo", "sysadmin", "accountadmin", "useradmin", "securityadmin", "*"}
    grouped_entities = EntityGenerator.group_spec_by_type(entities)
    assert (
        EntityGenerator.filter_grouped_entities_by_type(grouped_entities, "roles")
        == expected
    )
