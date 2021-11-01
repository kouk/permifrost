"""
This file describes the expected schema for a spec file.
These schemas are used to both parse and validate spec files.
"""

SNOWFLAKE_SPEC_SCHEMA = """
    version:
        type: string
        required: False

    require-owner:
        type: boolean
        required: False
        default: False

    databases:
        type: list
        schema:
            type: dict
            keyschema:
                type: string
            valuesrules:
                type: dict
    roles:
        type: list
        schema:
            type: dict
            keyschema:
                type: string
            valuesrules:
                type: dict
    users:
        type: list
        schema:
            type: dict
            keyschema:
                type: string
            valuesrules:
                type: dict
    warehouses:
        type: list
        schema:
            type: dict
            keyschema:
                type: string
            valuesrules:
                type: dict
    """

SNOWFLAKE_SPEC_DATABASE_SCHEMA = """
    shared:
        type: boolean
        required: True
    owner:
        type: string
        required: False
    """


SNOWFLAKE_SPEC_ROLE_SCHEMA = """
    owner:
        type: string
        required: False
    warehouses:
        type: list
        schema:
            type: string
    member_of:
        anyof:
            - type: dict
              allowed:
                  - include
                  - exclude
              schema:
                  include:
                      type: list
                      required: True
                      schema:
                          type: string
                  exclude:
                      type: list
                      required: False
                      schema:
                          type: string
            - type: list
              schema:
                  type: string
    privileges:
        type: dict
        allowed:
            - databases
            - schemas
            - tables
        valuesrules:
            type: dict
            allowed:
                - read
                - write
            valuesrules:
                type: list
                schema:
                    type: string
    owns:
        type: dict
        allowed:
            - databases
            - schemas
            - tables
        valuesrules:
            type: list
            schema:
                type: string
    """

SNOWFLAKE_SPEC_USER_SCHEMA = """
    owner:
        type: string
        required: False
    can_login:
        type: boolean
        required: True
    member_of:
        type: list
        schema:
            type: string
    """

SNOWFLAKE_SPEC_WAREHOUSE_SCHEMA = """
    owner:
        type: string
        required: False
    size:
        type: string
        required: True
    """
