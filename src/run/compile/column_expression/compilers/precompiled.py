from typing import Callable, Union

from .....model.column_expression import ColumnExpression
from ...query_layer import QueryLayer
from ..compiler_registry import (
    CompiledColumnExpression,
    register_column_expression_compiler,
)


class PrecompiledColumnExpression(ColumnExpression):
    """
    Allows a compiler to shim another compiler by replacing one of its
    `ColumnExpression` nodes with a result that is already compiled.
    """

    def __init__(
        self,
        compiled_expr: Union[
            CompiledColumnExpression,
            Callable[[QueryLayer], CompiledColumnExpression],
        ],
    ) -> None:
        super().__init__()
        self.compiled_expr = compiled_expr

    def default_identifier(self):
        return None

    def disambiguated(self, *args, **kwargs) -> "ColumnExpression":
        pass


register_column_expression_compiler(
    PrecompiledColumnExpression,
    lambda src, layer: (
        src.compiled_expr(layer) if callable(src.compiled_expr) else src.compiled_expr
    ),
)
