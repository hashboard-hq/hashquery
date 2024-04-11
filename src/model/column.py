from typing import *

from ..utils.keypath.keypath import _
from .column_expression import (
    ColumnExpression,
    ColumnNameColumnExpression,
    SqlTextColumnExpression,
    PyValueColumnExpression,
)


@overload
def column(name: str) -> ColumnExpression:
    """
    Constructs a ColumnExpression with the provided column name
    as its contents. This name will be escaped according to the
    database dialect, and always be correctly scoped.
    """
    ...


@overload
def column(*, sql: str) -> ColumnExpression:
    """
    Constructs a ColumnExpression which uses the provided SQL expression
    in place. The provided text is not escaped. The text should not include
    `AS name` (to name this, use `column(sql='...').named('my_name')`).
    """
    ...


@overload
def column(*, value: Any) -> ColumnExpression:
    """
    Constructs a ColumnExpression which represents the given Python value.
    For example, `None` is translated to `NULL`.

    Generally you don't need to use this explicitly; other functions are
    designed to automatically convert literal Python values into column
    expressions as needed.

    For example::

        column("user") != None

    Will automatically be cast to::

        column("user") != column(value=None)
    """
    ...


def column(
    name: Optional[str] = None,
    *,
    sql: Optional[str] = None,
    value: Optional[Any] = None,
) -> ColumnExpression:
    """
    Constructs a `ColumnExpression`. This function has three variants:

    1. `column(str)` will construct a column expression using the column name.
       This name is escaped according to the dialect.

    2. `column(sql=str)` will construct an expression which uses the literal
       SQL, unescaped, as its contents. The syntax `{{ some_expr }}` can
       be used within this SQL string to reference attributes and measure
       definitions on the given Model. Note that when this is done, the
       referenced expressions can also become unescaped.

    3. `column(value=Any)` will construct an expression which represents
       the provided Python value. For example, `None` is translated to `NULL`.
    """
    if name:
        return ColumnNameColumnExpression(name)
    elif sql:
        return SqlTextColumnExpression(sql).bind_references_to_model(_)
    else:
        return PyValueColumnExpression(value)
