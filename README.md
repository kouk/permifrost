# `permifrost permissions`

::: info
This is an optional tool for users who want to configure permissions if they're using Snowflake as the data warehouse and want to granularly set who has access to which data at the warehouse level.

Alpha-quality [Role Based Access Control (RBAC)](/docs/security-and-privacy.html#role-based-access-control-rbac-alpha) is also available.
:::

Use this command to check and manage the permissions of a Snowflake account.

```bash
permifrost permissions grant <spec_file> --db snowflake [--dry] [--diff]
```

Given the parameters to connect to a Snowflake account and a YAML file (a "spec") representing the desired database configuration, this command makes sure that the configuration of that database matches the spec. If there are differences, it will return the sql grant and revoke commands required to make it match the spec. If there are additional permissions set in the database this command will create the necessary revoke commands with the exception of:

* Object Ownership
* Warehouse Privileges

Permifrost is heavily inspired by [pgbedrock](https://github.com/Squarespace/pgbedrock) which can be used for managing the permissions in a Postgres database.

## spec_file

The YAML specification file is used to define in a declarative way the databases, roles, users and warehouses in a Snowflake account, together with the permissions for databases, schemas and tables for the same account.

All permissions are abbreviated as `read` or `write` permissions, with Permifrost generating the proper grants for each type of object. This includes shared databases which have simpler and more limited permissions than non-shared databases.

Tables and views are listed under `tables` and handled properly behind the scenes.

If `*` is provided as the parameter for tables the grant statement will use the `ALL <object_type>s in SCHEMA` syntax. It will also grant to future tables and views. See Snowflake documenation for [`ON FUTURE`](https://docs.snowflake.net/manuals/sql-reference/sql/grant-privilege.html#optional-parameters)

If a schema name includes an asterisk, such as `snowplow_*`, then all schemas that match this pattern will be included in grant statement. This can be coupled with the asterisk for table grants to grant permissions on all tables in all schemas that match the given pattern. This is useful for date-partitioned schemas.

All entities must be explicitly referenced. For example, if a permission is granted to a schema or table then the database must be explicitly referenced for permissioning as well.

A specification file has the following structure:

```bash
# Databases
databases:
    - db_name:
        shared: boolean
    - db_name:
        shared: boolean
    ... ... ...

# Roles
roles:
    - role_name:
        warehouses:
            - warehouse_name
            - warehouse_name
            ...

        member_of:
            - role_name
            - role_name
            ...

        privileges:
            databases:
                read:
                    - database_name
                    - database_name
                    ...
                write:
                    - database_name
                    - database_name
                    ...
            schemas:
                read:
                    - database_name.*
                    - database_name.schema_name
                    - database_name.schema_partial_*
                    ...
                write:
                    - database_name.*
                    - database_name.schema_name
                    - database_name.schema_partial_*
                    ...
            tables:
                read:
                    - database_name.*.*
                    - database_name.schema_name.*
                    - database_name.schema_partial_*.*
                    - database_name.schema_name.table_name
                    ...
                write:
                    - database_name.*.*
                    - database_name.schema_name.*
                    - database_name.schema_partial_*.*
                    - database_name.schema_name.table_name
                    ...

        owns:
            databases:
                - database_name
                ...
            schemas:
                - database_name.*
                - database_name.schema_name
                - database_name.schema_partial_*
                ...
            tables:
                - database_name.*.*
                - database_name.schema_name.*
                - database_name.schema_partial_*.*
                - database_name.schema_name.table_name
                ...

    - role_name:
    ... ... ...

# Users
users:
    - user_name:
        can_login: boolean
        member_of:
            - role_name
            ...
    - user_name:
    ... ... ...

# Warehouses
warehouses:
    - warehouse_name:
        size: x-small
    ... ... ...
```

For a working example, you can check [the Snowflake specification file](https://gitlab.com/gitlab-data/permifrost/blob/master/tests/permifrost/core/permissions/specs/snowflake_spec.yml) that we are using for testing `permifrost permissions`.

## --diff

When this flag is set, a full diff with both new and already granted commands is returned. Otherwise, only required commands for matching the definitions on the spec are returned.

## --dry

When this flag is set, the permission queries generated are not actually sent to the server and run; They are just returned to the user for examining them and running them manually.

When this flag is not set, the commands will be executed on Snowflake and their status will be returned and shown on the command line.

## Connection Parameters

The following environmental variables must be available to connect to Snowflake:

```bash
$PERMISSION_BOT_USER
$PERMISSION_BOT_PASSWORD
$PERMISSION_BOT_ACCOUNT
$PERMISSION_BOT_DATABASE
$PERMISSION_BOT_ROLE
$PERMISSION_BOT_WAREHOUSE
```
