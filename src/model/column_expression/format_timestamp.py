from typing import *

from ...utils.builder import builder_method
from ...utils.keypath import defer_keypath_args
from .column_expression import ColumnExpression


class FormatTimestampColumnExpression(ColumnExpression):
    def __init__(self, base: ColumnExpression, format: str) -> None:
        super().__init__()
        self.base = base
        self.format = format

    def default_identifier(self) -> Optional[str]:
        return self.base.default_identifier()

    @defer_keypath_args
    @builder_method
    def disambiguated(self, namespace) -> "ColumnExpression":
        self.base = self.base.disambiguated(namespace)

    def __repr__(self) -> str:
        return f'FORMAT_TIMESTAMP({self.base}, "{self.format}")'

    # --- Serialization ---

    __TYPE_KEY__ = "formatTimestamp"

    def _to_wire_format(self) -> dict:
        return {
            **super()._to_wire_format(),
            "base": self.base._to_wire_format(),
            "format": self.format,
        }

    @classmethod
    def _from_wire_format(cls, wire: dict) -> "FormatTimestampColumnExpression":
        assert wire["subType"] == cls.__TYPE_KEY__
        return FormatTimestampColumnExpression(
            ColumnExpression._from_wire_format(wire["base"]),
            wire["format"],
        )._from_wire_format_shared(wire)
