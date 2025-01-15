from typing import *

from ..column_expression import ColumnExpression
from .source import Source


class FilterSource(Source):
    def __init__(self, base: Source, condition: ColumnExpression) -> None:
        self.base = base
        self.condition = condition

    def __repr__(self) -> str:
        return str(self.base) + f"\n -> FILTER {str(self.condition)}"

    def _default_identifier(self):
        return self.base._default_identifier()

    __TYPE_KEY__ = "filter"

    def _to_wire_format(self) -> dict:
        return {
            **super()._to_wire_format(),
            "base": self.base._to_wire_format(),
            "condition": self.condition._to_wire_format(),
        }

    @classmethod
    def _from_wire_format(cls, wire: dict):
        assert wire["subType"] == cls.__TYPE_KEY__
        return FilterSource(
            Source._from_wire_format(wire["base"]),
            ColumnExpression._from_wire_format(wire["condition"]),
        )
