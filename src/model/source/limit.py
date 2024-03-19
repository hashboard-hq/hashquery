from typing import *

from .source import Source


class LimitSource(Source):
    def __init__(self, base: Source, limit: int, offset: Optional[int] = 0) -> None:
        self.base = base
        self.limit = limit
        self.offset = offset

    def __repr__(self) -> str:
        return str(self.base) + f"\n -> LIMIT {self.limit} OFFSET {self.offset}"

    def _default_identifier(self):
        return self.base._default_identifier()

    __TYPE_KEY__ = "limit"

    def to_wire_format(self) -> dict:
        return {
            **super().to_wire_format(),
            "base": self.base.to_wire_format(),
            "limit": self.limit,
            "offset": self.offset,
        }

    @classmethod
    def from_wire_format(cls, wire: dict):
        assert wire["subType"] == cls.__TYPE_KEY__
        return LimitSource(
            Source.from_wire_format(wire["base"]),
            wire["limit"],
            offset=wire["offset"],
        )
