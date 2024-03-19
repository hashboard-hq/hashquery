from typing import *

from ..model.column_expression import (
    ColumnExpression,
    CasesColumnExpression,
    PyValueColumnExpression,
)
from ..utils.keypath.resolve import defer_keypath_args


@defer_keypath_args
def cases(
    *cases: Tuple["ColumnExpression", Union["ColumnExpression", Any]],
    other: Optional[Union["ColumnExpression", Any]] = None,
) -> "ColumnExpression":
    """
    Constructs a ColumnExpression that represents a SQL `CASE` expression.

    Args:
        *cases: List of (condition, value) pairs, where `condition` and `value` are ColumnExpressions.
        other: The value (or expression) to use if none of the cases match. Defaults to None.
    """
    # Coerce everything into expressions.
    coerced_cases = []
    for condition, value in cases:
        if not isinstance(condition, ColumnExpression):
            condition = PyValueColumnExpression(condition)
        if not isinstance(value, ColumnExpression):
            value = PyValueColumnExpression(value)
        coerced_cases.append((condition, value))
    if not isinstance(other, ColumnExpression):
        other = PyValueColumnExpression(other)

    return CasesColumnExpression(
        coerced_cases,
        other=other,
    )
