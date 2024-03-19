from typing import *

from ...utils.builder import builder_method
from .column_expression import ColumnExpression


class CasesColumnExpression(ColumnExpression):
    def __init__(
        self,
        cases: List[Tuple[ColumnExpression, ColumnExpression]],
        other: ColumnExpression,
    ) -> None:
        super().__init__()
        if not cases:
            raise AssertionError(f"Case statements must have at least one case.")
        self.cases = cases
        self.other = other

    def default_identifier(self) -> Optional[str]:
        # consumers need to name this.
        return None

    @builder_method
    def disambiguated(self, namespace) -> "ColumnExpression":
        for c, v in self.cases:
            c.disambiguated(namespace)
            v.disambiguated(namespace)
        self.other.disambiguated(namespace)

    def __repr__(self) -> str:
        return f"CASE"

    # --- Serialization ---

    __TYPE_KEY__ = "case"

    def to_wire_format(self) -> dict:
        return {
            **super().to_wire_format(),
            "cases": [[c.to_wire_format(), v.to_wire_format()] for c, v in self.cases],
            "other": self.other.to_wire_format(),
        }

    @classmethod
    def from_wire_format(cls, wire: dict) -> "CasesColumnExpression":
        assert wire["subType"] == cls.__TYPE_KEY__
        return CasesColumnExpression(
            [
                (
                    ColumnExpression.from_wire_format(c),
                    ColumnExpression.from_wire_format(v),
                )
                for c, v in wire["cases"]
            ],
            ColumnExpression.from_wire_format(wire["other"]),
        )._from_wire_format_shared(wire)
