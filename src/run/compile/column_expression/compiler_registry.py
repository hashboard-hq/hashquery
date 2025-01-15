from typing import Callable, Dict, List, NewType, Optional, Type, TypeVar

import sqlalchemy.sql as sa

from ....model.column_expression.column_expression import ColumnExpression
from ..query_layer import QueryLayer

# typealias
CompiledColumnExpression = NewType("CompiledColumnExpression", sa.ColumnElement)


COLUMN_EXPRESSION_COMPILER_REGISTRY: Dict[
    str,
    Callable[[ColumnExpression, QueryLayer], CompiledColumnExpression],
] = {}

COLUMN_EXPRESSION_PREPROCESSOR_REGISTRY: Dict[
    str,
    List[Callable[[ColumnExpression, QueryLayer], ColumnExpression]],
] = {}

ColumnExpressionType = TypeVar("ColumnExpressionType", bound=ColumnExpression)


def register_column_expression_compiler(
    column_expression_type: Type[ColumnExpressionType],
    builder: Callable[[ColumnExpressionType, QueryLayer], CompiledColumnExpression],
    *,
    preprocessors: Optional[
        List[Callable[[ColumnExpressionType, QueryLayer], ColumnExpression]]
    ] = None,
):
    """
    Registers the provided function as the one to use for compiling
    `ColumnExpression`s of the given type into SQLAlchemy.
    """
    if column_expression_type in COLUMN_EXPRESSION_COMPILER_REGISTRY:
        raise AssertionError(
            f"Conflicting implementations for compiling `{column_expression_type.__name__}`."
        )
    COLUMN_EXPRESSION_COMPILER_REGISTRY[column_expression_type] = builder

    if preprocessors:
        # We only support a single call to register() per column expression type.
        if column_expression_type in COLUMN_EXPRESSION_PREPROCESSOR_REGISTRY:
            raise AssertionError(
                f"Conflicting implementations for preprocessing `{column_expression_type.__name__}`."
            )
        COLUMN_EXPRESSION_PREPROCESSOR_REGISTRY[column_expression_type] = preprocessors
