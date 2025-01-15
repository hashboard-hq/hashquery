from typing import List

import sqlalchemy.sql as sa
import sqlalchemy.types as sa_types

from .....model.column_expression.column_expression import ColumnExpression
from .....model.column_expression.column_name import ColumnNameColumnExpression
from .....model.column_expression.py_value import PyValueColumnExpression
from .....model.column_expression.sql_function import SqlFunctionColumnExpression
from .....model.source.pick import PickSource
from ....db.dialect import SqlDialect
from ...column_expression.compile_column_expression import (
    PrecompiledColumnExpression,
    compile_column_expression,
)
from ..compile_source import (
    QueryContext,
    QueryLayer,
    compile_source,
    register_source_compiler,
)


def compile_pick_source(source: PickSource, ctx: QueryContext) -> QueryLayer:
    base_layer = compile_source(source.base, ctx)
    if _is_passthrough_select(source, base_layer.query):
        return base_layer  # noop

    layer = base_layer if base_layer.can_set_selections else base_layer.chained()
    layer.has_selections = True

    picked_columns = _apply_dialect_workarounds(source.columns, ctx)
    picked_expressions = [
        compile_column_expression(s, layer, labeled=True) for s in picked_columns
    ]

    layer.query = layer.query.with_only_columns(picked_expressions)
    return layer


def _apply_dialect_workarounds(
    picked_expressions: List[ColumnExpression], ctx: QueryContext
):
    if ctx.dialect != SqlDialect.REDSHIFT:
        return picked_expressions
    result = []
    for i in range(len(picked_expressions)):
        result.append(_apply_cast_if_literal(picked_expressions[i]))
    return result


def _apply_cast_if_literal(expr: ColumnExpression):
    """
    For the given column expression, attempts to wrap it in a cast to a string
    if it is a literal string. This for some dialects (eg. Redshift) to properly
    infer the type of the literal.
    """
    if isinstance(expr, PyValueColumnExpression) and isinstance(expr.value, str):
        return PrecompiledColumnExpression(
            lambda layer: sa.cast(  # TODO: can this be simplified to `type_coerce`?
                compile_column_expression(expr, layer),
                sa_types.String(),
            )
        ).named(expr._optional_identifier)
    return expr


def _is_passthrough_select(source: PickSource, base: sa.Select) -> bool:
    """
    Returns True if and only if the Select source will have no effect on
    the provided base.
    """
    selected_names = set(
        s.column_name for s in source.columns if type(s) == ColumnNameColumnExpression
    )
    if len(selected_names) != len(source.columns):
        return False

    base_names = set(c["name"] for c in base.column_descriptions)
    different_names = selected_names.symmetric_difference(base_names)
    return len(different_names) == 0


register_source_compiler(PickSource, compile_pick_source)
