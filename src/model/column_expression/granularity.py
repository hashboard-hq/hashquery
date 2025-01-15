from typing import *

from ...utils.builder import builder_method
from ...utils.keypath import defer_keypath_args
from .column_expression import ColumnExpression


class GranularityColumnExpression(ColumnExpression):
    def __init__(self, base: ColumnExpression, granularity: str) -> None:
        super().__init__()
        self.base = base
        self.granularity = granularity

    def default_identifier(self) -> Optional[str]:
        return self.base.default_identifier()

    @defer_keypath_args
    @builder_method
    def disambiguated(self, namespace) -> "ColumnExpression":
        self.base = self.base.disambiguated(namespace)

    def __repr__(self) -> str:
        return f'DATE_TRUNC({self.base}, "{self.granularity}")'

    # --- Serialization ---

    __TYPE_KEY__ = "granularity"

    def _to_wire_format(self) -> dict:
        return {
            **super()._to_wire_format(),
            "base": self.base._to_wire_format(),
            "granularity": self.granularity,
        }

    @classmethod
    def _from_wire_format(cls, wire: dict) -> "GranularityColumnExpression":
        assert wire["subType"] == cls.__TYPE_KEY__
        return GranularityColumnExpression(
            ColumnExpression._from_wire_format(wire["base"]),
            wire["granularity"],
        )._from_wire_format_shared(wire)
