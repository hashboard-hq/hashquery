from typing import *
from abc import ABC

from ...utils.serializable import Serializable


class Source(Serializable, ABC):
    """
    Represents the underlying data table for a Model.
    Consumers should not interact with this class directly,
    instead modifying the table through methods on the model.
    """

    # required by all concrete subclasses
    __TYPE_KEY__ = None

    def __init_subclass__(cls) -> None:
        super().__init_subclass__()
        type_key = cls.__TYPE_KEY__
        if not type_key:
            return
        elif type_key in SOURCE_TYPE_KEY_REGISTRY:
            raise AssertionError(
                "Multiple Source subclasses for same type key: " + type_key
            )
        SOURCE_TYPE_KEY_REGISTRY[type_key] = cls

    def to_wire_format(self) -> dict:
        return {"type": "source", "subType": self.__TYPE_KEY__}

    @classmethod
    def from_wire_format(cls, wire: dict) -> "Source":
        assert wire["type"] == "source"
        type_key = wire["subType"]
        SourceType = SOURCE_TYPE_KEY_REGISTRY.get(type_key)
        if not SourceType:
            raise AssertionError("Unknown Source type key: " + type_key)
        return SourceType.from_wire_format(wire)

    def _default_identifier(self) -> Optional[str]:
        return None


SOURCE_TYPE_KEY_REGISTRY: Dict[
    str,
    Type[Serializable],
] = {}
