from typing import *

from .column_expression import ColumnExpression
from ...utils.builder import builder_method


class PyValueColumnExpression(ColumnExpression):
    def __init__(self, value: Any) -> None:
        super().__init__()
        self.value = value

    def default_identifier(self) -> Optional[str]:
        return None

    @builder_method
    def disambiguated(self, namespace) -> "PyValueColumnExpression":
        # a literal value never needs to be scoped/qualified
        pass

    def __repr__(self) -> str:
        if self.value is None:
            return "NULL"
        elif type(self.value) == str:
            return f"'{self.value}'"
        elif type(self.value) == bool:
            return str(self.value).upper()
        return str(self.value)

    # --- Serialization ---

    __TYPE_KEY__ = "pyValue"

    def to_wire_format(self) -> dict:
        return {
            **super().to_wire_format(),
            "value": self._primitive_to_wire_format(self.value),
        }

    @classmethod
    def from_wire_format(cls, wire: dict) -> "PyValueColumnExpression":
        assert wire["subType"] == cls.__TYPE_KEY__
        return PyValueColumnExpression(
            cls._primitive_from_wire_format(wire["value"])
        )._from_wire_format_shared(wire)
