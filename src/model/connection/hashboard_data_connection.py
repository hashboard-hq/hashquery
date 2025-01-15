from dataclasses import dataclass
from typing import *

from ...utils.secret import Secret
from .connection import Connection

if TYPE_CHECKING:
    from ...integration.hashboard.credentials import HashboardClientCredentials


@dataclass
class HashboardDataConnection(Connection):
    id: str
    project_id: str

    # used internally to improve debug statements and derive identifiers in
    # `HashboardProject` instances; the `id` is what actually controls the
    # identity of this object
    name: Optional[str] = None

    # the hashboard credentials object for the above `project_id`
    credentials: "Secret[HashboardClientCredentials]" = None

    # --- Serialization ---

    __TYPE_KEY__ = "hashboardDataConnection"

    def _to_wire_format(self) -> dict:
        return {
            **super()._to_wire_format(),
            "id": self.id,
            "projectId": self.project_id,
            "name": self.name,
            "credentials": Secret.PLACEHOLDER,
        }

    @classmethod
    def _from_wire_format(cls, wire: dict) -> "HashboardDataConnection":
        assert wire["subType"] == cls.__TYPE_KEY__
        return HashboardDataConnection(
            id=wire["id"],
            project_id=wire["projectId"],
            name=wire.get("name"),
            # can't load credentials back, force set these to None
            # the most common (only) service loading this kind of connection
            # instances from the wire formats is Hashboard itself, where
            # it doesn't need credentials anyways.
            credentials=None,
        )
