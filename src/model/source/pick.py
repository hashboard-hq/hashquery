from typing import *

from .source import Source
from ..column_expression import ColumnExpression


class PickSource(Source):
    def __init__(self, base: Source, columns: List[ColumnExpression]) -> None:
        self.base = base
        self.columns = columns

    def __repr__(self) -> str:
        return str(self.base) + "\n -> PICK " + ",".join(str(m) for m in self.columns)

    def _default_identifier(self):
        return self.base._default_identifier()

    __TYPE_KEY__ = "pick"

    def to_wire_format(self) -> dict:
        return {
            **super().to_wire_format(),
            "base": self.base.to_wire_format(),
            "columns": [c.to_wire_format() for c in self.columns],
        }

    @classmethod
    def from_wire_format(cls, wire: dict):
        assert wire["subType"] == cls.__TYPE_KEY__
        return PickSource(
            Source.from_wire_format(wire["base"]),
            [ColumnExpression.from_wire_format(s) for s in wire["columns"]],
        )
