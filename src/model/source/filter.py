from typing import *

from .source import Source
from ..column_expression import ColumnExpression


class FilterSource(Source):
    def __init__(self, base: Source, condition: ColumnExpression) -> None:
        self.base = base
        self.condition = condition

    def __repr__(self) -> str:
        return str(self.base) + f"\n -> FILTER {str(self.condition)}"

    def _default_identifier(self):
        return self.base._default_identifier()

    __TYPE_KEY__ = "filter"

    def to_wire_format(self) -> dict:
        return {
            **super().to_wire_format(),
            "base": self.base.to_wire_format(),
            "condition": self.condition.to_wire_format(),
        }

    @classmethod
    def from_wire_format(cls, wire: dict):
        assert wire["subType"] == cls.__TYPE_KEY__
        return FilterSource(
            Source.from_wire_format(wire["base"]),
            ColumnExpression.from_wire_format(wire["condition"]),
        )
