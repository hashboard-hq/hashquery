from typing import *

from ...utils.builder import builder_method
from ...utils.keypath import defer_keypath_args
from .column_expression import ColumnExpression


class BinaryOpColumnExpression(ColumnExpression):
    def __init__(
        self,
        left: ColumnExpression,
        right: ColumnExpression,
        op: str,
        options: Optional[Dict[str, Union[str, bool]]] = None,
    ) -> None:
        super().__init__()
        self.left = left
        self.right = right
        self.op = op
        self.options = options or {}

    def default_identifier(self) -> Optional[str]:
        return None

    @defer_keypath_args
    @builder_method
    def disambiguated(self, namespace) -> "BinaryOpColumnExpression":
        self.left = self.left.disambiguated(namespace)
        self.right = self.right.disambiguated(namespace)

    def __repr__(self) -> str:
        return f"{self.left} {self.op} {self.right}"

    # --- Serialization ---

    __TYPE_KEY__ = "binaryOp"

    def _to_wire_format(self) -> Any:
        return {
            **super()._to_wire_format(),
            "left": self.left._to_wire_format(),
            "right": self.right._to_wire_format(),
            "op": self.op,
            "options": self.options,
        }

    @classmethod
    def _from_wire_format(cls, wire: dict) -> "BinaryOpColumnExpression":
        assert wire["subType"] == cls.__TYPE_KEY__
        left = ColumnExpression._from_wire_format(wire["left"])
        right = ColumnExpression._from_wire_format(wire["right"])
        return BinaryOpColumnExpression(
            left, right, wire["op"], wire["options"]
        )._from_wire_format_shared(wire)
