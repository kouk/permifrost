import functools
import logging
import re

from typing import Dict, List, Optional

from permifrost.core.permissions.utils.snowflake_connector import SnowflakeConnector
from permifrost.core.permissions.utils.snowflake_permission import SnowflakePermission


class SnowflakeGrantChecker:
    """
    Holds permissions for roles and allows you to check if a role has a permission and also if it is able to grant a permission.
    """

    def __init__(self, conn: Optional[SnowflakeConnector] = None):
        self.conn = conn if conn is not None else SnowflakeConnector()
        # A simple in memory map to store lookups against the database.
        # In the future we can use something like https://pypi.org/project/cachetools/
        # to annotate the methods we want to cache calls to.
        self.role_permission_cache = {}

    def _get_permissions(self, role: str) -> Dict:
        if role not in self.role_permission_cache:
            self.role_permission_cache[role] = self.conn.show_grants_to_role(role)

        return self.role_permission_cache[role]

    def get_permissions(self, role: str) -> List[SnowflakePermission]:
        """
        Get a list of permissions that are granted to the given `role`.

        This function mainly maps the output of the SnowflakeConnector.show_grants_to_role function
        to the SnowflakePermission objects.
        """
        role_permission_dict = self._get_permissions(role)
        role_permissions = []
        for privilege, entity_types in role_permission_dict.items():
            for entity_type, entity_names in entity_types.items():
                for entity_name in entity_names:
                    role_permissions.append(
                        SnowflakePermission(entity_name, entity_type, [privilege])
                    )
        return role_permissions

    def _has_permission(
        self, role: Optional[str], permission: SnowflakePermission
    ) -> bool:
        """
        Will return true if the <role> has the given <permission> on the <entity_name>.
        Will always return true if no <role> is none.
        """

        if not role:
            return True

        role_permissions = self._get_permissions(role)
        return (
            permission.entity_name
            in role_permissions.get("ownership", {}).get(permission.entity_type, {})
            # TODO(MH) Still need to modify the show grants function to return grant_option information
            # .get(permission.entity_name, {})
            # .get("grant_option", False)
            or functools.reduce(
                lambda a, b: a and b,
                map(
                    lambda privilege: permission.entity_name
                    in role_permissions.get(privilege, {}).get(
                        permission.entity_type, {}
                    ),
                    permission.privileges,
                ),
            )
            or (
                # Special check for account level permissions
                permission.entity_name == "*"
                and permission.entity_type == "account"
                and functools.reduce(
                    lambda a, b: a and b,
                    map(
                        lambda privilege: permission.entity_type
                        in role_permissions.get(privilege, {}),
                        permission.privileges,
                    ),
                )
            )
        )

    def has_permission(
        self, role: Optional[str], permission: SnowflakePermission
    ) -> bool:
        return self._has_permission(role, permission) or self._has_permission(
            role,
            permission.with_entity_name(
                SnowflakeConnector.snowflaky(permission.entity_name)
            ),
        )

    def _can_grant_permission(
        self, role: Optional[str], permission: SnowflakePermission
    ) -> bool:
        """
        Given the <permission>, will return true if the <role> is able to grant them.
        Will always return true if no <role> was given.
        """

        if not role:
            return True

        role_permissions = self._get_permissions(role)
        return role_permissions.get("ownership", {}).get(
            permission.entity_type, {}
        ).get(permission.entity_name, {}).get(
            "grant_option", False
        ) or functools.reduce(
            lambda a, b: a and b,
            map(
                lambda privilege: role_permissions.get(privilege, {})
                .get(permission.entity_type, {})
                .get(permission.entity_name, {})
                .get("grant_option", False),
                permission.privileges,
            ),
        )

    def can_grant_permission(
        self, role: Optional[str], permission: SnowflakePermission
    ) -> bool:
        return self._can_grant_permission(
            role, permission
        ) or self._can_grant_permission(
            role,
            permission.with_entity_name(
                SnowflakeConnector.snowflaky(permission.entity_name)
            ),
        )
