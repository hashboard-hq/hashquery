from typing import *

from ..model.column_expression import ColumnExpression, SqlFunctionColumnExpression
from ..utils.keypath.resolve import defer_keypath_args


@defer_keypath_args
def and_(*clauses: ColumnExpression) -> ColumnExpression:
    """
    an `AND` expression in SQL
    """
    return SqlFunctionColumnExpression("and", clauses)


@defer_keypath_args
def or_(*clauses: ColumnExpression) -> ColumnExpression:
    """
    an `OR` expression in SQL
    """
    return SqlFunctionColumnExpression("or", clauses)


@defer_keypath_args
def not_(clause: ColumnExpression) -> ColumnExpression:
    """
    a `NOT` expression in SQL
    """
    return SqlFunctionColumnExpression("not", [clause])
