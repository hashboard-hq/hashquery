from typing import *

from ..model.column_expression import ColumnExpression, SqlFunctionColumnExpression
from ..utils.keypath.resolve import defer_keypath_args
from ._cases import cases


@defer_keypath_args
def count(target: Optional["ColumnExpression"] = None) -> ColumnExpression:
    """
    an aggregating `COUNT` expression over the provided column or value.
    You can omit a value to form `COUNT(*)`.
    """
    return SqlFunctionColumnExpression("count", [target])


@defer_keypath_args
def count_if(condition: ColumnExpression) -> ColumnExpression:
    """
    an aggregating expression which counts the records for which `condition`
    is True. Equivalent to `SUM(CASE WHEN condition THEN 1 ELSE 0 END)`.
    """
    return sum(cases((condition, 1), other=0))


@defer_keypath_args
def distinct(target: ColumnExpression) -> ColumnExpression:
    """
    an aggregating `DISTINCT` expression over the provided column.
    """
    return SqlFunctionColumnExpression("distinct", [target])


@defer_keypath_args
def max(target: ColumnExpression) -> ColumnExpression:
    """
    an aggregating `MAX` expression over the provided column.
    """
    return SqlFunctionColumnExpression("max", [target])


@defer_keypath_args
def min(target: ColumnExpression) -> ColumnExpression:
    """
    an aggregating `MIN` expression over the provided column.
    """
    return SqlFunctionColumnExpression("min", [target])


@defer_keypath_args
def sum(target: ColumnExpression) -> ColumnExpression:
    """
    an aggregating `AVG` expression over the provided column.
    """
    return SqlFunctionColumnExpression("sum", [target])


@defer_keypath_args
def avg(target: ColumnExpression) -> ColumnExpression:
    """
    an aggregating `AVG` expression over the provided column.
    """
    return SqlFunctionColumnExpression("avg", [target])


def now() -> ColumnExpression:
    """
    a `NOW()` expression in SQL, which will be evaluated at query-time.
    This is distinct from calling `datetime.now`, which would evaluate the
    expression at build-time.
    """
    return SqlFunctionColumnExpression("now", [])
