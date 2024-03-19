from typing import *

from .serializable import Serializable


class LinkedResource(Serializable):
    """
    Represents a reference to an external resource.
    """

    def __init__(
        self,
        id: str,
        alias: Optional[str],
        project_id: str,
    ) -> None:
        self.id = id
        self.alias = alias
        self.project_id = project_id

    def to_wire_format(self) -> dict:
        return {
            "id": self.id,
            "alias": self.alias,
            "projectId": self.project_id,
        }

    @classmethod
    def from_wire_format(cls, wire: dict):
        return LinkedResource(
            id=wire["id"],
            alias=wire["alias"],
            project_id=wire["projectId"],
        )
