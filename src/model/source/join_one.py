from typing import *

from ..column_expression.column_expression import ColumnExpression
from ..namespace import ModelNamespace
from .source import Source


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

    def _to_wire_format(self) -> dict:
        return {
            **super()._to_wire_format(),
            "base": self.base._to_wire_format(),
            "relation": self.relation._to_wire_format(),
            "joinCondition": self.join_condition._to_wire_format(),
            "dropUnmatched": self.drop_unmatched,
        }

    @classmethod
    def _from_wire_format(cls, wire: dict):
        assert wire["subType"] == cls.__TYPE_KEY__
        return JoinOneSource(
            base=Source._from_wire_format(wire["base"]),
            relation=ModelNamespace._from_wire_format(wire["relation"]),
            join_condition=ColumnExpression._from_wire_format(wire["joinCondition"]),
            drop_unmatched=wire["dropUnmatched"],
        )
