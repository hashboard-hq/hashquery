from copy import copy
from datetime import datetime

import sqlalchemy.sql as sa
import sqlalchemy.types as sa_types

from .....model.column_expression import (
    BinaryOpColumnExpression,
    ColumnExpression,
    PyValueColumnExpression,
    SqlFunctionColumnExpression,
)
from ....db.dialect import SqlDialect, to_type_name_mapping
from ...context import QueryContext
from ...utils.error import UserCompilationError
from ..compile_column_expression import (
    QueryLayer,
    compile_column_expression,
    preprocess_column_expression,
)
from ..compiler_registry import (
    CompiledColumnExpression,
    register_column_expression_compiler,
)


def compile_sql_function_column_expression(
    expr: SqlFunctionColumnExpression,
    layer: QueryLayer,
) -> CompiledColumnExpression:
    args = [
        (
            compile_column_expression(arg, layer)
            if isinstance(arg, ColumnExpression)
            else arg
        )
        for arg in expr.args
    ]

    function_name = expr.function_name
    if function_name == "count":
        target = args[0]
        return sa.func.count(target) if target is not None else sa.func.count()
    elif function_name in ["max", "min", "distinct", "avg", "sum", "floor", "ceiling"]:
        target = args[0]
        return getattr(sa.func, function_name)(target)
    elif function_name == "now":
        # `NOW()` returns a datetime of a specific type which databases
        # won't naturally coerce when compared; so for now the query-builder
        # materializes the current time into the query instead of asking
        # the database for the current timestamp. This matches what our
        # v1 aggregation code does so it should be fine for now.
        return compile_column_expression(PyValueColumnExpression(datetime.now()), layer)
    elif function_name == "and":
        return sa.and_(*args)
    elif function_name == "or":
        return sa.or_(*args)
    elif function_name == "not":
        return sa.not_(*args)
    elif function_name == "exists":
        return sa.exists(args[0])
    elif function_name == "diffSeconds":
        return _diff_seconds(args[0], args[1], layer.ctx)
    elif function_name == "cast":
        expr, type_name = args
        sa_type = to_type_name_mapping(layer.ctx.dialect).sa_type(type_name)
        if not sa_type:
            layer.ctx.add_warning(f"Unknown type name for `cast`: {type_name}")
            sa_type = sa_types.NullType()
        return sa.cast(expr, sa_type)
    else:
        raise AssertionError("Unknown function type: " + function_name)


def _diff_seconds(
    ts1: CompiledColumnExpression, ts2: CompiledColumnExpression, ctx: QueryContext
):
    if ctx.dialect == SqlDialect.DUCKDB:
        return sa.func.epoch(ts1 - ts2)
    elif ctx.dialect == SqlDialect.BIGQUERY:
        return sa.func.TIMESTAMP_DIFF(ts1, ts2, sa.text("SECOND"))
    elif ctx.dialect in [SqlDialect.POSTGRES, SqlDialect.REDSHIFT]:
        return sa.func.EXTRACT("EPOCH", ts1 - ts2)
    elif ctx.dialect == SqlDialect.SNOWFLAKE:
        return sa.func.timestampdiff("second", ts2, ts1)
    elif ctx.dialect == SqlDialect.CLICKHOUSE:
        return sa.func.dateDiff(sa.literal("second"), ts2, ts1)
    elif ctx.dialect == SqlDialect.MYSQL:
        return sa.func.TIMESTAMPDIFF(sa.text("SECOND"), ts2, ts1)
    elif ctx.dialect == SqlDialect.DATABRICKS:
        return sa.func.unix_timestamp(ts1) - sa.func.unix_timestamp(ts2)
    elif ctx.dialect == SqlDialect.ATHENA:
        return sa.func.date_diff("second", ts2, ts1)
    else:
        raise UserCompilationError(
            f"Unsupported dialect for diffSeconds: {ctx.dialect}"
        )


def preprocess_sql_function_column_expression(
    expr: SqlFunctionColumnExpression, layer: QueryLayer
) -> ColumnExpression:
    expr = copy(expr)
    expr.args = [
        (
            preprocess_column_expression(arg, layer)
            if isinstance(arg, ColumnExpression)
            else arg
        )
        for arg in expr.args
    ]
    expr = preprocess_not_binary_op(expr)
    return expr


def preprocess_not_binary_op(expr: SqlFunctionColumnExpression):
    # Turns `func.not_(x > y)` into `x <= y` and similar
    replace_binary_op_map = {
        ">": "<=",
        ">=": "<",
        "<": ">=",
        "<=": ">",
        "=": "!=",
        "!=": "=",
    }
    if not (
        expr.function_name == "not"
        and len(expr.args) == 1
        and type(expr.args[0]) is BinaryOpColumnExpression
        and expr.args[0].op in replace_binary_op_map
    ):
        return expr

    reversed_op = copy(expr.args[0])
    reversed_op.op = replace_binary_op_map[reversed_op.op]
    return reversed_op.named(expr._optional_identifier)


register_column_expression_compiler(
    SqlFunctionColumnExpression,
    compile_sql_function_column_expression,
    preprocessors=[preprocess_sql_function_column_expression],
)
