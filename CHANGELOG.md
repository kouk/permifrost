# CHANGELOG

All notable changes to this project will be documented in this file.  This
project adheres to [Semantic Versioning](http://semver.org/) and [Keep a
Changelog](http://keepachangelog.com/).



## Unreleased
---

### New

### Changes
[#112](https://gitlab.com/gitlab-data/permifrost/-/issues/112)  Add support for 'grant select on all tables in db/schema'
for `db.*.*` or `db.schema.*` in spec file.

### Fixes

### Breaks


## 0.14.0 - (2022-03-18)
---

### New
[#101](https://gitlab.com/gitlab-data/permifrost/-/issues/101)
Adds in a new authentication mechanism for externalbrower sso auth in Snowflake.

### Changes

* Statements for already granted permissions (shown as [SKIPPED]) will no
longer be displayed by default. They will display when used with `--verbose` or
`-v` flags. This will help clean the output of permifrost to make it easier
to see what changes will be applied.
* On a dry run (--dry), statements that are expected to run will show as [PENDING]
instead of [SKIPPED].

### Fixes

* Allow optional passphrase for key authentication to address #81
* [#105](https://gitlab.com/gitlab-data/permifrost/-/issues/105)
Resolve REVOKE command always runs for write permissions on tables
* [#68](https://gitlab.com/gitlab-data/permifrost/-/issues/68)
Implement ownership grants on views


## 0.13.1 - (2021-11-19)
---

### Changes

[#98](https://gitlab.com/gitlab-data/permifrost/-/issues/98)
Separated out the spec loading step from into a function and added in the new CLI command `spec_test`.
Minor improvements to the linting, accounting for venv folders existing in the permifrost directory during development.

## 0.13.0 - (2021-11-02)
---

### Changes

* [#88](https://gitlab.com/gitlab-data/permifrost/-/issues/88)
Improved overall package logging. Rather than setting log-level explicitly,
the verbose flag is used to increase the level of logging output. Logs are now formatted
with timestamp and colours and include details on which module originated the log.
When permifrost is run with `-vv` all queries are logged to help isolate potential issues.

### Fixes
* [#88](https://gitlab.com/gitlab-data/permifrost/-/issues/88) Fixes `member_of` `include` and `exclude` functionality and introduces improved test coverage for all functionality related to the `member_of` statement for `roles` in spec file
* Skip creation of role grants (i.e. `GRANT ROLE <role name> TO ROLE <other role name>`) when permifrost attempts to modify the default role hierarchy from Snowflake
* Update README documentation to reflect some of the limitations associated with using `SECURITYADMIN` role
### Breaks

* Removed --log-level flag in favour of --verbose flags.


## 0.12.1 - (2021-09-28)
---

### Fixes
* Update the release documentation and simplify process


## 0.12.0 - (2021-09-28)
---

### New
### Changes
* [#60](https://gitlab.com/gitlab-data/permifrost/-/issues/60)
Adds ability to grant `write` privileges on schemas and/or tables while only granting `read` permissions on databases and/or schemas
(i.e. if someone has `read` access on a database, they can have `read/write` access on the schemas and/or tables in that database. Also
if someone has `read` access on a database and schema, they can have `read/write` access on the tables in that database/schema combo)

### Fixes
* [#69](https://gitlab.com/gitlab-data/permifrost/-/issues/69) Adds quote_plus
  to the SnowflakeConnector to resolve possible connection issues caused by
  complex passwords.

* [#80](https://gitlab.com/gitlab-data/permifrost/-/issues/80)
Future grants on tables and views are granted on the database level.

### Breaks


## 0.11.0 - (2021-03-01)
---

### New
* [#38](https://gitlab.com/gitlab-data/permifrost/-/issues/38) To begin
  addressing this issue the SnowflakePermission object was added, and the
  SnowflakeRoleGrantChecker added to use the SnowflakePermission object in its
  interface. This starts to lay the groundwork to make managing the grants
  easier.


### Fixes
* [#61](https://gitlab.com/gitlab-data/permifrost/-/issues/61) Fixes a bug
  where reference errors were being triggered due to inadvertant filtering.


## 0.10.0 - (2021-02-12)
---

### New
* [#58](https://gitlab.com/gitlab-data/permifrost/-/issues/58) Add support for
  --ignore-membership flag to skip grant/revoke of role memberships


## 0.9.0 - (2021-01-29)
---

### New
* [#30](https://gitlab.com/gitlab-data/permifrost/-/issues/30) Add support for
  --user flag, update --role flag to take multiples, modify `grant` entry point
  to `run`. NOTE: Breaking changes.

### Changes
* [#42](https://gitlab.com/gitlab-data/permifrost/-/merge_requests/57)
  ISSUE-57: Stop pinning requires in the setup.py install_requires


### Breaks
* [#30](https://gitlab.com/gitlab-data/permifrost/-/issues/30) Renames `grant`
  entry point to `run`


## 0.8.1 - (2020-12-29)
---

### Fixes


### Fixes
* [#42](https://gitlab.com/gitlab-data/permifrost/-/merge_requests/42) Filter
  grants not related to the databases in the config file.

## 0.8.0 - (2020-11-02)
---

### New
* [#50](https://gitlab.com/gitlab-data/permifrost/-/issues/50) Add support for
  include / exclude spec on `member_of` parameter


## 0.7.0 - (2020-10-29)
---

### New
* [#49](https://gitlab.com/gitlab-data/permifrost/-/issues/49) Add support for
  using "*" as a `member_of` parameter


## 0.6.1 - (2020-10-28)
---

### New
* [#44](https://gitlab.com/gitlab-data/permifrost/-/issues/44) Add support for
  using a key pair to authenticate with Snowflake


## 0.6.0 - (2020-10-05)
---

### New
* [#31](https://gitlab.com/gitlab-data/permifrost/-/issues/31) Add `--role`
  option to run permissions for a given role


## 0.5.0 - (2020-09-28)
---

### New
* [#29](https://gitlab.com/gitlab-data/permifrost/-/issues/29) The owner field
  in the spec file for roles is now verified against what is in the database.
  If there is a mismatch, Permifrost will present an error to the user
  describing that the role does not have the correct owner in the spec.


## 0.4.0 - (2020-09-15)
---

### New
* [#41](https://gitlab.com/gitlab-data/permifrost/-/issues/41) Grant monitor
  privileges on warehouses by default


## 0.3.1 - (2020-09-03)
---

### Changes
* [#40](https://gitlab.com/gitlab-data/permifrost/-/issues/40) Bump
  requirements to allow for newer versions of PyYaml, Click, and SQLAlchemy


## 0.3.0 - (2020-08-25)
---

### New
* [#28](https://gitlab.com/gitlab-data/permifrost/-/issues/28) Add owner field
  to all objects and the `require-owner` property

### Changes
* [#39](https://gitlab.com/gitlab-data/permifrost/-/issues/39) Improve
  performance of runs by reusing snowflake connection when applying grants


## 0.2.1 - (2020-08-18)
---

### New
* [#27](https://gitlab.com/gitlab-data/permifrost/-/issues/27) Add validation
  to make sure current user's role is securityadmin.


## 0.2.0 - (2020-08-04)
---

### New
* [#21](https://gitlab.com/gitlab-data/permifrost/-/issues/21) Add support for
  OAuth token authentication.

### Changes
* [#26](https://gitlab.com/gitlab-data/permifrost/-/issues/26) Only query the
  Snowflake server for a given resource type if defined in the spec.

### Fixes
- [#13](https://gitlab.com/gitlab-data/permifrost/-/issues/13) Database level
  table and view grants are properly handled in lists
- [#15](https://gitlab.com/gitlab-data/permifrost/issues/15) Roles are revoked
  from roles and users that do not have a `member_of` config entry


## 0.1.1 - (2020-07-06)
---

### Fixes
- [#8](https://gitlab.com/gitlab-data/permifrost/-/issues/8) Existing future
  grants on a database cause an error


## 0.1.0 - (2020-03-06)
---

### Changes
- [#5](https://gitlab.com/gitlab-data/permifrost/issues/5) Updates to print
  commands as they run instead of in a batch at the end of a run


## 0.0.2 - (2020-03-04)
---

### Changes
- [#3](https://gitlab.com/gitlab-data/permifrost/issues/3) Removes extra
  `permissions` cli argument from invocation

### Fixes
- [#4](https://gitlab.com/gitlab-data/permifrost/issues/4) Fix schema revokes
  referencing databases not in spec


## 0.0.1 - (2020-03-03)
---

### Changes
- Initial release


---
