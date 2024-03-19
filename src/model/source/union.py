from typing import *

from .source import Source


class UnionSource(Source):
    def __init__(self, base: Source, union_source: Source) -> None:
        self.base = base
        self.union_source = union_source

    def __repr__(self) -> str:
        return str(self.base) + f"\n -> UNION {str(self.union_source)}"

    def _default_identifier(self):
        return self.base._default_identifier()

    __TYPE_KEY__ = "union"

    def to_wire_format(self) -> dict:
        return {
            **super().to_wire_format(),
            "base": self.base.to_wire_format(),
            "unionSource": self.union_source.to_wire_format(),
        }

    @classmethod
    def from_wire_format(cls, wire: dict):
        assert wire["subType"] == cls.__TYPE_KEY__
        return UnionSource(
            Source.from_wire_format(wire["base"]),
            Source.from_wire_format(wire["unionSource"]),
        )
