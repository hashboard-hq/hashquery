from typing import *


from .source import Source
from ..namespace import ModelNamespace
from ..column_expression.column_expression import ColumnExpression


class JoinOneSource(Source):
    def __init__(
        self,
        base: Source,
        relation: ModelNamespace,
        join_condition: ColumnExpression,
        *,
        drop_unmatched: bool,
    ) -> None:
        self.base = base
        self.relation = relation
        self.join_condition = join_condition
        self.drop_unmatched = drop_unmatched

    def __repr__(self) -> str:
        return (
            str(self.base) + f"\n -> JOIN ONE {self.relation} ON {self.join_condition}"
        )

    def _default_identifier(self):
        return self.base._default_identifier()

    __TYPE_KEY__ = "joinOne"

    def to_wire_format(self) -> dict:
        return {
            **super().to_wire_format(),
            "base": self.base.to_wire_format(),
            "relation": self.relation.to_wire_format(),
            "joinCondition": self.join_condition.to_wire_format(),
            "dropUnmatched": self.drop_unmatched,
        }

    @classmethod
    def from_wire_format(cls, wire: dict):
        assert wire["subType"] == cls.__TYPE_KEY__
        return JoinOneSource(
            base=Source.from_wire_format(wire["base"]),
            relation=ModelNamespace.from_wire_format(wire["relation"]),
            join_condition=ColumnExpression.from_wire_format(wire["joinCondition"]),
            drop_unmatched=wire["dropUnmatched"],
        )
