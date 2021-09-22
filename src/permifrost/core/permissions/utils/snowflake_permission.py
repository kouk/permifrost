from dataclasses import dataclass, replace
from typing import List


@dataclass
class SnowflakePermission:
    """
    A data class that represents a snowflake permission. A permission is a set of
    `privileges` (ie. select, insert, use) that can be performed on an entity.
    We represent an entity by its name and type.

    In the snowflake permission model, permissions can be granted to `users` and `roles`.
    """

    entity_name: str
    entity_type: str
    privileges: List[str]
    grant_option: bool

    def __eq__(self, obj):
        return (
            isinstance(obj, SnowflakePermission)
            and obj.entity_name == self.entity_name
            and obj.entity_type == self.entity_type
            and obj.privileges == self.privileges
        )

    def with_entity_name(self, entity_name: str):
        """
        Convenience function to easily change the name of the entity name.
        """
        self.entity_name = entity_name
        return self

    def as_owner(self):
        return replace(self, privileges=["ownership"])

    def contains_any(self, privileges: List[str]):
        """
        Returns `True` if this permission object contains any of the privileges given.
        Otherwise, returns `False`.
        """
        for privilege in privileges:
            if privilege in self.privileges:
                return True
        return False
