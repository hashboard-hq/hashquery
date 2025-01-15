from copy import copy
from typing import Optional

import sqlalchemy as sa
from sqlalchemy.sql.elements import BindParameter

from .....model.column_expression import (
    BinaryOpColumnExpression,
    ColumnExpression,
    PyValueColumnExpression,
)
from ....db.dialect import SqlDialect
from ...utils.array import array_contains
from ...utils.custom_value_types import LiteralDate
from ...utils.datetime import try_get_constant_ts_value, try_get_datetime_delta_value
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

UnknownOperatorError = lambda op: AssertionError(f"Unknown operator: {op}")


def compile_binary_op_column_expression(
    expr: BinaryOpColumnExpression,
    layer: QueryLayer,
) -> CompiledColumnExpression:
    left = compile_column_expression(expr.left, layer)
    right = compile_column_expression(expr.right, layer)
    op = expr.op

    # comparison to None (NULL) is a special case since it needs to be rendered as `IS NOT NULL` or `IS NULL`.
    none_comparison = compile_none_comparison(expr, left, right, layer)
    if none_comparison is not None:
        return none_comparison

    if op in ["=", "!=", "<", "<=", ">", ">="]:
        return compile_comparison_operator(op, left, right, layer)
    elif op in ["+", "-", "*", "/", "//"]:
        return compile_arithmetic_operator(op, left, right, layer)
    elif op == "IN":
        return compile_in_operator(expr, left, right, layer)
    elif op == "LIKE":
        return left.like(right)
    elif op == "ILIKE":
        return left.ilike(right)
    else:
        raise UnknownOperatorError(op)


def compile_none_comparison(
    expr: BinaryOpColumnExpression,
    left: CompiledColumnExpression,
    right: CompiledColumnExpression,
    layer: QueryLayer,
):
    left_is_none = _is_none_expr(expr.left)
    right_is_none = _is_none_expr(expr.right)
    if not (expr.op in ["=", "!="] and (left_is_none or right_is_none)):
        return None  # not a None comparison

    if left_is_none and right_is_none:
        result = True if expr.op == "=" else False
        layer.ctx.add_warning(
            f"Comparison `None {expr.op} None` will always be {result}"
        )
        return sa.sql.True_() if result else sa.sql.False_()

    compare_col = (
        (lambda c: c.is_(None)) if expr.op == "=" else (lambda c: c.is_not(None))
    )
    return compare_col(left) if right_is_none else compare_col(right)


def compile_comparison_operator(
    op: str,
    left: CompiledColumnExpression,
    right: CompiledColumnExpression,
    layer: QueryLayer,
):
    left, right = _coerce_types_to_comparable(left, right, layer)
    if op == "=":
        return left == right
    elif op == "!=":
        return left != right
    elif op == "<":
        return left < right
    elif op == "<=":
        return left <= right
    elif op == ">":
        return left > right
    elif op == ">=":
        return left >= right
    else:
        raise UnknownOperatorError(op)


def compile_arithmetic_operator(
    op: str,
    left: CompiledColumnExpression,
    right: CompiledColumnExpression,
    layer: QueryLayer,
):
    if op == "+":
        return left + right
    elif op == "-":
        return left - right
    elif op == "*":
        return left * right
    elif op == "/":
        return true_divide(left, right, layer)
    elif op == "//":
        return integer_divide(left, right, layer)
    else:
        raise UnknownOperatorError(op)


def true_divide(left, right, layer: QueryLayer):
    """Divides two column expressions. This should ensure a decimal result (e.g. no rounding) regardless of the types of the inputs."""
    if layer.ctx.dialect == SqlDialect.CLICKHOUSE:
        return sa.func.divide(left, right)
    elif layer.ctx.dialect in [SqlDialect.POSTGRES, SqlDialect.REDSHIFT]:
        if type(left.type) != sa.DECIMAL:
            left = sa.cast(left, sa.DECIMAL)
    elif layer.ctx.dialect == SqlDialect.ATHENA:
        if type(left.type) not in (sa.FLOAT, sa.Float):
            left = sa.cast(left, sa.FLOAT)
        if type(right.type) not in (sa.FLOAT, sa.Float):
            right = sa.cast(right, sa.FLOAT)
    return left / right


def integer_divide(left, right, layer: QueryLayer):
    # TODO(wilson): implement this across all dialects.
    return left / right


def compile_in_operator(
    expr: BinaryOpColumnExpression,
    left: CompiledColumnExpression,
    right: CompiledColumnExpression,
    layer: QueryLayer,
):
    case_sensitive = expr.options.get("case_sensitive", True)

    is_boolean_membership = (
        type(left.type) == sa.BOOLEAN and type(right) == BindParameter
    )

    if type(right.type) == sa.String:
        # Substring matching
        if not isinstance(left, BindParameter):
            raise UserCompilationError(
                "Can only do substring matching against constant values."
            )

        return (
            right.like(f"%{left.value}%")
            if case_sensitive
            else right.ilike(f"%{left.value}%")
        )
    elif type(right.type) == sa.ARRAY:
        # Value membership in array expression
        if not case_sensitive:
            raise UserCompilationError(
                "Case-insensitive matching is not currently supported for the 'in' operator with array columns."
            )
        return array_contains(layer.ctx.dialect, left, right)
    elif layer.ctx.dialect == SqlDialect.BIGQUERY and is_boolean_membership:
        """
        BigQuery's driver has an issue in which IN expressions for booleans are
        sent into SQLAlchemy as a literal array, instead of a series of Boolean
        values, since BigQuery's DBAPI is _special_ and directly supports lists.
        This breaks validation on SQLAlchemy's side, since it exclusively
        expects boolean values. This works around the issue instead of forking
        the driver (nightmare fuel).
        https://github.com/googleapis/python-bigquery-sqlalchemy/issues/489
        """
        return sa.or_(*[left == value for value in right.value])
    elif layer.ctx.dialect == SqlDialect.DATABRICKS and is_boolean_membership:
        """
        Databricks's driver seems to automatically convert Python bools to integers.
        So, we're going to explicitly cast the column to an integer to make sure the values match.
        """
        left = sa.cast(left, sa.INTEGER)

    # fall through to regular "in" operator.
    return left.in_(_rebind_literal_array(right))


def _is_none_expr(expr: ColumnExpression):
    if type(expr) == PyValueColumnExpression:
        return expr.value is None


def _coerce_types_to_comparable(
    left: CompiledColumnExpression, right: CompiledColumnExpression, layer: QueryLayer
):
    # Bigquery workaround for date comparisons. It doesn't allow comparison between TIMESTAMP and non-TZ-aware types (DATETIME, DATE).
    #
    # A workaround for this is simply to cast timestamps to datetime before comparing, which should default to using UTC.
    if layer.ctx.dialect == SqlDialect.BIGQUERY:
        if type(left.type) == sa.TIMESTAMP and type(right.type) in [
            sa.DATETIME,
            sa.DateTime,
            sa.DATE,
            sa.Date,
        ]:
            left = sa.cast(left, sa.DateTime)
        if type(right.type) == sa.TIMESTAMP and type(left.type) in [
            sa.DATETIME,
            sa.DateTime,
            sa.DATE,
            sa.Date,
        ]:
            right = sa.cast(right, sa.DateTime)
    # DuckDB workaround for date/timestamp comparisons. From version 0.10 onward, it doesn't allow comparison between TIMESTAMP/DATETIME and DATE types.
    if layer.ctx.dialect == SqlDialect.DUCKDB:
        if type(left.type) in [sa.TIMESTAMP, sa.DATETIME] and type(right.type) in [
            sa.DATE,
            sa.Date,
            LiteralDate,
        ]:
            right = sa.cast(right, type(left.type))
        if type(right.type) in [sa.TIMESTAMP, sa.DATETIME] and type(left.type) in [
            sa.DATE,
            sa.Date,
            LiteralDate,
        ]:
            left = sa.cast(left, type(right.type))

    return left, right


def _try_lift_binary_op(
    expr: BinaryOpColumnExpression,
) -> Optional[PyValueColumnExpression]:
    """For certain binary operators, it might be sensible to compute their result in Python instead of the database.

    Currently, this is used to apply shifts to known datetime values (e.g. now, or constant datetimes).

    Returns: A PyValueColumnExpression that encapsulates the new value if the operation can be lifted, otherwise None.
    """
    is_plus = expr.op == "+"
    is_minus = expr.op == "-"

    if not (is_plus or is_minus):
        return None

    # Adding or subtracting an interval from a datetime.
    if (left_val := try_get_constant_ts_value(expr.left)) is not None and (
        right_delta := try_get_datetime_delta_value(expr.right)
    ) is not None:
        return PyValueColumnExpression(
            left_val + right_delta if is_plus else left_val - right_delta
        )

    # Also valid to do <interval> + <datetime> (though not subtraction).
    if (
        (left_delta := try_get_datetime_delta_value(expr.left)) is not None
        and (right_val := try_get_constant_ts_value(expr.right)) is not None
        and is_plus
    ):
        return PyValueColumnExpression(left_delta + right_val)

    return None


def _rebind_literal_array(maybe_array_param):
    """
    The SQLAlchemy `column.in_` function has special behavior for handling
    Python literal arrays where it will expand the items into a list of
    parameters:
        col.in_(['A', 'B'])
        > col IN (:param1, :param2)

    There are two issues at play here:
      - Rather than passing a Python literal array, we pass a `BindParameter` object,
        which is what we get from the `compilers/py_value` module. By unwrapping this to the underlying
        vanilla Python list, we can generally get dialects to correctly render the SQL.
      - HOWEVER: SQLAlchemy, under the hood, will sometimes re-wrap the list itself in a bindparam. The logic used to **name**
        this param depends on the expression/context the literal is used in (which is surprising).
        In practice, this means that SQLALchemy will sometimes use things like arbitrary custom SQL used in the expression,
        sanitize it, then use it as the parameter name. This potentially raises errors in Bigquery, which has a 128 character limit
        for parameter names.

    The solution here is to unwrap the bindparam, but re-bind the individual items in the list,
    which avoids the parameter naming issue.
    """
    if (
        type(maybe_array_param) is BindParameter
        and type(maybe_array_param.value) is list
    ):
        return [sa.literal(val) for val in maybe_array_param.value]
    return maybe_array_param


def preprocess_binary_op_column_expression(
    expr: BinaryOpColumnExpression, layer: QueryLayer
) -> ColumnExpression:
    expr = copy(expr)
    expr.left = preprocess_column_expression(expr.left, layer)
    expr.right = preprocess_column_expression(expr.right, layer)
    return _try_lift_binary_op(expr) or expr


register_column_expression_compiler(
    BinaryOpColumnExpression,
    compile_binary_op_column_expression,
    preprocessors=[preprocess_binary_op_column_expression],
)
