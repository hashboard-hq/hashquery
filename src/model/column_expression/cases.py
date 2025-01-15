from typing import *

from ...utils.builder import builder_method
from ...utils.keypath import defer_keypath_args
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

    @defer_keypath_args
    @builder_method
    def disambiguated(self, namespace) -> "ColumnExpression":
        self.cases = [
            (c.disambiguated(namespace), v.disambiguated(namespace))
            for c, v in self.cases
        ]
        self.other = self.other.disambiguated(namespace)

    def __repr__(self) -> str:
        return f"CASE"

    # --- Util ---

    def matches_any_case(self) -> ColumnExpression:
        """
        Returns a ColumnExpression representing if any case of the expression
        was matched. This will be `False` if and other if the `other` case is
        triggered.
        """
        from ... import func

        case_conditions = [condition for condition, _ in self.cases]
        return func.or_(*case_conditions)

    # --- Serialization ---

    __TYPE_KEY__ = "case"

    def _to_wire_format(self) -> dict:
        return {
            **super()._to_wire_format(),
            "cases": [
                [c._to_wire_format(), v._to_wire_format()] for c, v in self.cases
            ],
            "other": self.other._to_wire_format(),
        }

    @classmethod
    def _from_wire_format(cls, wire: dict) -> "CasesColumnExpression":
        assert wire["subType"] == cls.__TYPE_KEY__
        return CasesColumnExpression(
            [
                (
                    ColumnExpression._from_wire_format(c),
                    ColumnExpression._from_wire_format(v),
                )
                for c, v in wire["cases"]
            ],
            ColumnExpression._from_wire_format(wire["other"]),
        )._from_wire_format_shared(wire)
