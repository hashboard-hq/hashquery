from typing import *

from .column_expression import ColumnExpression
from ...utils.builder import builder_method


class GranularityColumnExpression(ColumnExpression):
    def __init__(self, base: ColumnExpression, granularity: str) -> None:
        super().__init__()
        self.base = base
        self.granularity = granularity

    def default_identifier(self) -> Optional[str]:
        return self.base.default_identifier()

    @builder_method
    def disambiguated(self, namespace) -> "ColumnExpression":
        self.base.disambiguated(namespace)

    def __repr__(self) -> str:
        return f'DATE_TRUNC({self.base}, "{self.granularity}")'

    # --- Serialization ---

    __TYPE_KEY__ = "granularity"

    def to_wire_format(self) -> dict:
        return {
            **super().to_wire_format(),
            "base": self.base.to_wire_format(),
            "granularity": self.granularity,
        }

    @classmethod
    def from_wire_format(cls, wire: dict) -> "GranularityColumnExpression":
        assert wire["subType"] == cls.__TYPE_KEY__
        return GranularityColumnExpression(
            ColumnExpression.from_wire_format(wire["base"]),
            wire["granularity"],
        )._from_wire_format_shared(wire)
