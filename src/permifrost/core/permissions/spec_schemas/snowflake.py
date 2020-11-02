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
            valueschema:
                type: dict
    roles:
        type: list
        schema:
            type: dict
            keyschema:
                type: string
            valueschema:
                type: dict
    users:
        type: list
        schema:
            type: dict
            keyschema:
                type: string
            valueschema:
                type: dict
    warehouses:
        type: list
        schema:
            type: dict
            keyschema:
                type: string
            valueschema:
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
              valueschema:
                  type: list
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
        valueschema:
            type: dict
            allowed:
                - read
                - write
            valueschema:
                type: list
                schema:
                    type: string
    owns:
        type: dict
        allowed:
            - databases
            - schemas
            - tables
        valueschema:
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
