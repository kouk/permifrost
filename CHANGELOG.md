# CHANGELOG

All notable changes to this project will be documented in this file.
This project adheres to [Semantic Versioning](http://semver.org/) and [Keep a Changelog](http://keepachangelog.com/).



## Unreleased
---

### New
* [#31](https://gitlab.com/gitlab-data/permifrost/-/issues/31) Add `--roles` option to run permissions for a given role 

### Changes

### Fixes

### Breaks


## 0.5.0 - (2020-09-28)
---

### New
* [#29](https://gitlab.com/gitlab-data/permifrost/-/issues/29) The owner field in the spec file for roles is now verified against what is in the database.  If there is a mismatch, Permifrost will present an error to the user describing that the role does not have the correct owner in the spec.


## 0.4.0 - (2020-09-15)
---

### New
* [#41](https://gitlab.com/gitlab-data/permifrost/-/issues/41) Grant monitor privileges on warehouses by default


## 0.3.1 - (2020-09-03)
---

### Changes
* [#40](https://gitlab.com/gitlab-data/permifrost/-/issues/40) Bump requirements to allow for newer versions of PyYaml, Click, and SQLAlchemy


## 0.3.0 - (2020-08-25)
---

### New
* [#28](https://gitlab.com/gitlab-data/permifrost/-/issues/28) Add owner field to all objects and the `require-owner` property

### Changes
* [#39](https://gitlab.com/gitlab-data/permifrost/-/issues/39) Improve performance of runs by reusing snowflake connection when applying grants


## 0.2.1 - (2020-08-18)
---

### New
* [#27](https://gitlab.com/gitlab-data/permifrost/-/issues/27) Add validation to make sure current user's role is securityadmin.


## 0.2.0 - (2020-08-04)
---

### New
* [#21](https://gitlab.com/gitlab-data/permifrost/-/issues/21) Add support for OAuth token authentication.

### Changes
* [#26](https://gitlab.com/gitlab-data/permifrost/-/issues/26) Only query the Snowflake server for a given resource type if defined in the spec.

### Fixes
- [#13](https://gitlab.com/gitlab-data/permifrost/-/issues/13) Database level table and view grants are properly handled in lists
- [#15](https://gitlab.com/gitlab-data/permifrost/issues/15) Roles are revoked from roles and users that do not have a `member_of` config entry


## 0.1.1 - (2020-07-06)
---

### Fixes
- [#8](https://gitlab.com/gitlab-data/permifrost/-/issues/8) Existing future grants on a database cause an error


## 0.1.0 - (2020-03-06)
---

### Changes
- [#5](https://gitlab.com/gitlab-data/permifrost/issues/5) Updates to print commands as they run instead of in a batch at the end of a run


## 0.0.2 - (2020-03-04)
---

### Changes
- [#3](https://gitlab.com/gitlab-data/permifrost/issues/3) Removes extra `permissions` cli argument from invocation

### Fixes
- [#4](https://gitlab.com/gitlab-data/permifrost/issues/4) Fix schema revokes referencing databases not in spec


## 0.0.1 - (2020-03-03)
---

### Changes
- Initial release


---
