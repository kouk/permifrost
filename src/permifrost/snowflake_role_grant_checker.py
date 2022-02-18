from typing import Any, Dict, List, Optional

from permifrost.snowflake_connector import SnowflakeConnector
from permifrost.snowflake_permission import SnowflakePermission


class SnowflakeRoleGrantChecker:
    """
    Holds permissions for roles and allows you to check if a role has a permission and also if it is able to grant a permission.
    """

    def __init__(self, conn: Optional[SnowflakeConnector] = None):
        self.conn = conn if conn is not None else SnowflakeConnector()
        # A simple in memory map to store lookups against the database.
        # In the future we can use something like https://pypi.org/project/cachetools/
        # to annotate the methods we want to cache calls to.
        self.role_permission_cache: Dict[str, Any] = {}

    def _get_permissions(self, role: str) -> Dict:
        if role not in self.role_permission_cache:
            self.role_permission_cache[
                role
            ] = self.conn.show_grants_to_role_with_grant_option(role)

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
                for entity_name, options in entity_names.items():
                    name = entity_name if entity_type != "account" else "*"
                    role_permissions.append(
                        SnowflakePermission(
                            name, entity_type, [privilege], options["grant_option"]
                        )
                    )
        return role_permissions

    def _has_permission(
        self, role: Optional[str], permission: SnowflakePermission
    ) -> Optional[SnowflakePermission]:
        """
        Will return the SnowflakePermission if the <role> has the given <permission> on the <entity_name>.
        Will always return the given permission if <role> is none.
        """

        if not role:
            return permission

        role_permissions = self.get_permissions(role)

        # Always check for ownership first because it gives the most permissions
        if permission.as_owner() in role_permissions:
            # Since we don't check the grant_option of a permission when checking equality, we want to make sure
            # to return the actual permission value that was stored in the database to be most correct.
            return role_permissions[role_permissions.index(permission.as_owner())]
        if permission in role_permissions:
            return role_permissions[role_permissions.index(permission)]
        return None

    def has_permission(
        self, role: Optional[str], permission: SnowflakePermission
    ) -> bool:
        """
        Will return true if the <role> has the given <permission> on the <entity_name>.
        Where the <entity_name> is the name given in the permission object, or the "snowflaky" version. Both are checked.
        If the role has ownership of the entity in question, then this function should always return true.
        Will always return true if <role> is none.
        """
        return (
            self._has_permission(role, permission) is not None
            or self._has_permission(
                role,
                permission.with_entity_name(
                    SnowflakeConnector.snowflaky(permission.entity_name)
                ),
            )
            is not None
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

        has_permission = self._has_permission(role, permission)

        # Ownership will let you grant any permission on that object and is always grantable.
        # Since _has_permission will return ownership over other privileges, we only need to do a single check.
        return has_permission is not None and has_permission.grant_option

    def can_grant_permission(
        self, role: Optional[str], permission: SnowflakePermission
    ) -> bool:
        """
        Given the <permission>, will return true if the <role> is able to grant them on the <entity_name>.
        Where the <entity_name> is the name given in the permission object, or the "snowflaky" version. Both are checked.
        Will always return true if no <role> was given.
        """
        return self._can_grant_permission(
            role, permission
        ) or self._can_grant_permission(
            role,
            permission.with_entity_name(
                SnowflakeConnector.snowflaky(permission.entity_name)
            ),
        )
