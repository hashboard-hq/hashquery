from typing import *

from ...utils.builder import builder_method
from .column_expression import ColumnExpression

if TYPE_CHECKING:
    from ..model import Model


class SubqueryColumnExpression(ColumnExpression):
    def __init__(self, model: "Model") -> None:
        super().__init__()
        self.model = model

    def default_identifier(self) -> str:
        return list(self.model._attributes.keys())[0]

    @builder_method
    def disambiguated(self, namespace) -> "ColumnExpression":
        # a subquery cannot be scoped/qualified
        pass

    def __repr__(self) -> str:
        return f"<subquery>"

    # --- Serialization ---

    __TYPE_KEY__ = "subquery"

    def _to_wire_format(self) -> dict:
        return {**super()._to_wire_format(), "model": self.model._to_wire_format()}

    @classmethod
    def _from_wire_format(cls, wire: dict) -> "ColumnExpression":
        from ..model import Model

        assert wire["subType"] == cls.__TYPE_KEY__
        model = Model._from_wire_format(wire["model"])
        result = SubqueryColumnExpression(model)
        result._from_wire_format_shared(wire)
        return result
