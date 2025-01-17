from typing import *

from ...utils.builder import builder_method
from ...utils.keypath.resolve import defer_keypath_args
from ..namespace import ModelNamespace
from .column_expression import ColumnExpression


class ColumnNameColumnExpression(ColumnExpression):
    def __init__(self, column_name: str) -> None:
        super().__init__()
        self.column_name = column_name
        self._namespace_identifier: Optional[str] = None

    def default_identifier(self) -> str:
        return self.column_name

    @defer_keypath_args
    @builder_method
    def disambiguated(self, namespace) -> "ColumnNameColumnExpression":
        self._namespace_identifier = (
            namespace._identifier
            if isinstance(namespace, ModelNamespace)
            else namespace
        )

    def __repr__(self) -> str:
        return f"`{self.column_name}`"

    # --- Serialization ---

    __TYPE_KEY__ = "columnName"

    def _to_wire_format(self) -> dict:
        return {
            **super()._to_wire_format(),
            "columnName": self.column_name,
            "namespaceIdentifier": self._namespace_identifier,
        }

    @classmethod
    def _from_wire_format(cls, wire: dict) -> "ColumnNameColumnExpression":
        assert wire["subType"] == cls.__TYPE_KEY__
        result = ColumnNameColumnExpression(wire["columnName"])
        result._namespace_identifier = wire["namespaceIdentifier"]
        result._from_wire_format_shared(wire)
        return result
