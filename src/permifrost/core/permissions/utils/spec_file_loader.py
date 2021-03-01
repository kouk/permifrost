import cerberus
import yaml
from typing import Dict, List

from permifrost.core.permissions.utils.error import SpecLoadingError
from permifrost.core.permissions.spec_schemas.snowflake import *

VALIDATION_ERR_MSG = 'Spec error: {} "{}", field "{}": {}'


def ensure_valid_schema(spec: Dict) -> List[str]:
    """
    Ensure that the provided spec has no schema errors.

    Returns a list with all the errors found.
    """
    error_messages = []

    validator = cerberus.Validator(yaml.safe_load(SNOWFLAKE_SPEC_SCHEMA))
    validator.validate(spec)
    for entity_type, err_msg in validator.errors.items():
        if isinstance(err_msg[0], str):
            error_messages.append(f"Spec error: {entity_type}: {err_msg[0]}")
            continue

        for error in err_msg[0].values():
            error_messages.append(f"Spec error: {entity_type}: {error[0]}")

    if error_messages:
        return error_messages

    schema = {
        "databases": yaml.safe_load(SNOWFLAKE_SPEC_DATABASE_SCHEMA),
        "roles": yaml.safe_load(SNOWFLAKE_SPEC_ROLE_SCHEMA),
        "users": yaml.safe_load(SNOWFLAKE_SPEC_USER_SCHEMA),
        "warehouses": yaml.safe_load(SNOWFLAKE_SPEC_WAREHOUSE_SCHEMA),
    }

    validators = {
        "databases": cerberus.Validator(schema["databases"]),
        "roles": cerberus.Validator(schema["roles"]),
        "users": cerberus.Validator(schema["users"]),
        "warehouses": cerberus.Validator(schema["warehouses"]),
    }

    entities_by_type = [
        (entity_type, entities)
        for entity_type, entities in spec.items()
        if entities and entity_type in ["databases", "roles", "users", "warehouses"]
    ]

    for entity_type, entities in entities_by_type:
        for entity_dict in entities:
            for entity_name, config in entity_dict.items():
                validators[entity_type].validate(config)
                for field, err_msg in validators[entity_type].errors.items():
                    error_messages.append(
                        VALIDATION_ERR_MSG.format(
                            entity_type, entity_name, field, err_msg[0]
                        )
                    )

    return error_messages


def load_spec(spec_path: str) -> Dict:
    """
    Load a permissions specification from a file.

    If the file is not found or at least an error is found during validation,
    raise a SpecLoadingError with the appropriate error messages.

    Otherwise, return the valid specification as a Dictionary to be used
    in other operations.

    Raises a SpecLoadingError with all the errors found in the spec if at
    least one error is found.

    Returns the spec as a dictionary if everything is OK
    """
    try:
        with open(spec_path, "r") as stream:
            spec = yaml.safe_load(stream)
    except FileNotFoundError:
        raise SpecLoadingError(f"Spec File {spec_path} not found")

    error_messages = ensure_valid_schema(spec)
    if error_messages:
        raise SpecLoadingError("\n".join(error_messages))

    def lower_values(value):
        if isinstance(value, bool):
            return value
        elif isinstance(value, list):
            return [lower_values(entry) for entry in value]
        elif isinstance(value, str):
            return value.lower()
        elif isinstance(value, dict):
            return {k.lower(): lower_values(v) for k, v in value.items()}

    lower_spec = lower_values(spec)

    return lower_spec
