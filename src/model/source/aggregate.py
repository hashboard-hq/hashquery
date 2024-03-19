from typing import *

from .source import Source
from ..column_expression import ColumnExpression


class AggregateSource(Source):
    def __init__(
        self,
        base: Source,
        *,
        groups: List[ColumnExpression],
        measures: List[ColumnExpression],
    ) -> None:
        self.base = base
        self.groups = groups
        self.measures = measures

    def __repr__(self) -> str:
        return (
            str(self.base)
            + "\n -> AGGREGATE "
            + f"MEASURES {','.join(str(m) for m in self.measures)} "
            + f"BY {','.join(str(g) for g in self.groups)}"
        )

    def _default_identifier(self):
        return self.base._default_identifier()

    __TYPE_KEY__ = "aggregate"

    def to_wire_format(self) -> dict:
        return {
            **super().to_wire_format(),
            "base": self.base.to_wire_format(),
            "groups": [g.to_wire_format() for g in self.groups],
            "measures": [m.to_wire_format() for m in self.measures],
        }

    @classmethod
    def from_wire_format(cls, wire: dict):
        assert wire["subType"] == cls.__TYPE_KEY__
        return AggregateSource(
            Source.from_wire_format(wire["base"]),
            groups=[ColumnExpression.from_wire_format(g) for g in wire["groups"]],
            measures=[ColumnExpression.from_wire_format(m) for m in wire["measures"]],
        )
