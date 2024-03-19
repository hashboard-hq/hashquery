from typing import *

from .source import Source
from ..column_expression import ColumnExpression


class SortSource(Source):
    def __init__(self, base: Source, sort: ColumnExpression, dir: str) -> None:
        self.base = base
        self.sort = sort
        if dir.lower() not in ("asc", "desc"):
            raise ValueError(
                f"invalid sort direction: '{dir}'. Must be 'asc' or 'desc'."
            )
        self.dir = dir.lower()

    def __repr__(self) -> str:
        return str(self.base) + f"\n -> ORDER BY {str(self.sort)}"

    def _default_identifier(self):
        return self.base._default_identifier()

    __TYPE_KEY__ = "sort"

    def to_wire_format(self) -> dict:
        return {
            **super().to_wire_format(),
            "base": self.base.to_wire_format(),
            "sort": self.sort.to_wire_format(),
            "dir": self.dir,
        }

    @classmethod
    def from_wire_format(cls, wire: dict):
        assert wire["subType"] == cls.__TYPE_KEY__
        return SortSource(
            Source.from_wire_format(wire["base"]),
            ColumnExpression.from_wire_format(wire["sort"]),
            wire["dir"],
        )
