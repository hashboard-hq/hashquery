from typing import *

from ...utils.builder import builder_method
from .py_value import PyValueColumnExpression
from .column_expression import ColumnExpression


class SqlFunctionColumnExpression(ColumnExpression):
    def __init__(
        self,
        function_name: str,
        args: Iterable[Any] = None,
        *,
        inherit_identifier=False,
    ) -> None:
        super().__init__()
        self.function_name = function_name
        self.args = args if args else []
        self.inherit_identifier = inherit_identifier
        if self.inherit_identifier:
            self._manually_set_identifier = (
                self._base_column_expression()._manually_set_identifier
            )

    def _base_column_expression(self) -> Optional[ColumnExpression]:
        for arg in self.args:
            if isinstance(arg, ColumnExpression):
                return arg
        return None

    def default_identifier(self) -> Optional[str]:
        base = self._base_column_expression()
        if self.inherit_identifier and base:
            return base.default_identifier()

        if base and type(base) != PyValueColumnExpression:
            base_default = base.default_identifier()
            if base_default:
                return f"{self.function_name}_{base_default}"

        return self.function_name

    @builder_method
    def disambiguated(self, namespace) -> "SqlFunctionColumnExpression":
        self.args = [
            arg.disambiguated(namespace) if isinstance(arg, ColumnExpression) else arg
            for arg in self.args
        ]

    def __repr__(self) -> str:
        return f"{self.function_name}({', '.join(str(arg) for arg in self.args)})"

    __TYPE_KEY__ = "sqlFunction"

    def to_wire_format(self) -> Any:
        return {
            **super().to_wire_format(),
            "functionName": self.function_name,
            "args": [
                arg.to_wire_format() if hasattr(arg, "to_wire_format") else arg
                for arg in self.args
            ],
            "inheritIdentifier": self.inherit_identifier,
        }

    @classmethod
    def from_wire_format(cls, wire: dict) -> "SqlFunctionColumnExpression":
        assert wire["subType"] == cls.__TYPE_KEY__

        function_name = wire["functionName"]
        args = [
            (
                ColumnExpression.from_wire_format(arg)
                if (type(arg) == dict and arg.get("type") == "columnExpression")
                else arg
            )
            for arg in wire["args"]
        ]
        return SqlFunctionColumnExpression(
            function_name,
            args,
            inherit_identifier=False,
        )._from_wire_format_shared(wire)
