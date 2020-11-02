# `permifrost`

Use this command to check and manage the permissions of a Snowflake account.

```bash
permifrost grant <spec_file> [--role] [--dry] [--diff]
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

All entities must be explicitly referenced. For example, if a permission is granted to a schema or table then the database must be explicitly referenced for permissioning as well.  Additionally, role membership must be explicit in the config file.  If a role does not have a `member_of` list, it will have all roles it currently has revoked.

Roles can accept "*" as a role name either alone or nested under the `include` key. There is optionally an `exclude` key that can be used if `include` is used. `"*"` will grant membership to all roles defined in the spec. Any roles defined in `exclude` will be removed from the list defined in `include`.

A specification file has the following structure:

```bash
# Databases
databases:
    - db_name:
        shared: boolean
    - db_name:
        shared: boolean
        owner: role_name
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

            # or
        
        member_of:
            include:
                - "*"
            exclude:
                - role_name

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
        owner: role_name
    ... ... ...

# Users
users:
    - user_name:
        can_login: boolean
        member_of:
            - role_name
            ...
    - user_name:
        owner: role_name
    ... ... ...

# Warehouses
warehouses:
    - warehouse_name:
        size: x-small
    - warehouse_name:
        size: x-small
        owner: role_name
    ... ... ...
```

For a working example, you can check [the Snowflake specification file](https://gitlab.com/gitlab-data/permifrost/blob/master/tests/permifrost/core/permissions/specs/snowflake_spec.yml) that we are using for testing `permifrost permissions`.

### Settings
All settings are declared here with their default values and are described below.  These can be added to your spec.yaml file.

```yaml
require-owner: false
```

`require-owner`: Set to true to force having to set the `owner` property on all objects defined.

## --diff

When this flag is set, a full diff with both new and already granted commands is returned. Otherwise, only required commands for matching the definitions on the spec are returned.

## --dry

When this flag is set, the permission queries generated are not actually sent to the server and run; They are just returned to the user for examining them and running them manually.

When this flag is not set, the commands will be executed on Snowflake and their status will be returned and shown on the command line.

## Connection Parameters

The following environmental variables must be available to connect to Snowflake:

```bash
$PERMISSION_BOT_USER
$PERMISSION_BOT_ACCOUNT
$PERMISSION_BOT_WAREHOUSE
```

### Username and Password

To connect using a username and password, also include the following:

```bash
$PERMISSION_BOT_PASSWORD
$PERMISSION_BOT_DATABASE
$PERMISSION_BOT_ROLE
```

Currently, Permifrost assumes you are using the SECURITYADMIN role and will fail validation if you are not.

### OAuth

To connect using an OAuth token, also include the following:

```bash
$PERMISSION_BOT_OAUTH_TOKEN
```

### Key Pair Authentication

Rather than supplying a password or an oauth token, it's possible to connect via Snowflake's Key Pair authentication by setting the following:

```bash
$PERMISSION_BOT_PRIVATE_KEY_PATH
$PERMISSION_BOT_PRIVATE_KEY_PASSPHRASE
```

See [Snowflake-sqlalchemy](https://github.com/snowflakedb/snowflake-sqlalchemy#key-pair-authentication-support) for more info.

## Contributing

Contributing to Permifrost is easy, and most commands to do so are available within the Makefile.

The easiest way to start developing is to run `make permifrost`, this will open a shell in a docker container with the local version of Permifrost installed.
You can now make changes to the files in your editor and it will be reflected in the commands that you run from the docker shell.

For code checking, you can use `make test`, `make lint`,and `make typecheck`. See the Makefile for more details.
