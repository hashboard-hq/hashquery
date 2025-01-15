from abc import ABC
from copy import copy
from typing import *

import sqlalchemy as sa
from datetime_truncate import truncate as _truncate_py_datetime
from dateutil.relativedelta import relativedelta

from .....model.column_expression.column_expression import ColumnExpression
from .....model.column_expression.granularity import GranularityColumnExpression
from .....model.column_expression.py_value import PyValueColumnExpression
from ....db.dialect import SqlDialect
from ...utils.datetime import DAY_OF_WEEK_MAPPING, try_get_constant_ts_value
from ..compile_column_expression import (
    QueryLayer,
    compile_column_expression,
    preprocess_column_expression,
)
from ..compiler_registry import (
    CompiledColumnExpression,
    register_column_expression_compiler,
)

# --- COMPILE ---


def compile_granularity_column_expression(
    expr: GranularityColumnExpression,
    layer: QueryLayer,
) -> CompiledColumnExpression:
    base = compile_column_expression(expr.base, layer)
    granularity = _validate_granularity(expr.granularity)
    first_day_of_week_offset = DAY_OF_WEEK_MAPPING[layer.ctx.settings.first_day_of_week]

    compiler = _dialect_compiler(layer.ctx.dialect)
    return compiler.compile(
        base,
        granularity,
        first_day_of_week_offset=first_day_of_week_offset,
    )


def _dialect_compiler(dialect) -> "GranularityCompiler":
    return {
        SqlDialect.ATHENA: Athena_GC,
        SqlDialect.BIGQUERY: BigQuery_GC,
        SqlDialect.CLICKHOUSE: Clickhouse_GC,
        SqlDialect.DATABRICKS: Databricks_GC,
        SqlDialect.DUCKDB: DuckDB_GC,
        SqlDialect.MYSQL: MySql_GC,
        SqlDialect.POSTGRES: Postgres_GC,
        SqlDialect.REDSHIFT: Postgres_GC,
        SqlDialect.SNOWFLAKE: Snowflake_GC,
    }[dialect]()


class GranularityCompiler(ABC):
    """
    A base class responsible for granularity compilation.

    Defers to the following signatures when called:
    - `_compile_week_offset` is called if the granularity is weekly and the offset is non-zero.
    - `_compile_{granularity}` is called for the granularity
    - `_compile_default` is called
    """

    def compile(
        self,
        base: CompiledColumnExpression,
        granularity: str,
        first_day_of_week_offset: int,
    ):
        if granularity == "week" and first_day_of_week_offset != 0:
            return self.compile_week_offset(base, first_day_of_week_offset)
        compiler_method = getattr(self, f"compile_{granularity}")
        return compiler_method(base)

    def compile_second(self, base: CompiledColumnExpression):
        return self.compile_default(base, "second")

    def compile_minute(self, base: CompiledColumnExpression):
        return self.compile_default(base, "minute")

    def compile_hour(self, base: CompiledColumnExpression):
        return self.compile_default(base, "hour")

    def compile_day(self, base: CompiledColumnExpression):
        return self.compile_default(base, "day")

    def compile_week_offset(self, base: CompiledColumnExpression, offset: int):
        return self.compile_week(base)

    def compile_week(self, base: CompiledColumnExpression):
        return self.compile_default(base, "week")

    def compile_month(self, base: CompiledColumnExpression):
        return self.compile_default(base, "month")

    def compile_quarter(self, base: CompiledColumnExpression):
        return self.compile_default(base, "quarter")

    def compile_year(self, base: CompiledColumnExpression):
        return self.compile_default(base, "year")

    def compile_default(self, base: CompiledColumnExpression, granularity: str):
        return sa.func.date_trunc(
            sa.bindparam(key=None, value=granularity, literal_execute=True),
            # Use a `literal_execute` parameter here ^^^^^^^^^^^^^^^^^ because some
            # engines (ie. MotherDuck) have problems when a `date_trunc` call is
            # parameterized. This asks the engine to escape this value as a string
            # but doesn't encode is as `:param`. This is safe because we validated
            # `granularity` is a known value.
            base,
        )


class Athena_GC(GranularityCompiler):
    def compile_week_offset(self, base, offset):
        # using literal_column instead of timedelta because timedelta doesn't compile
        # to a sql query (used for the view query functionality)
        offset_one_day = base - sa.sql.expression.literal_column(
            f"interval '{offset}' day"
        )
        return sa.func.date_trunc(
            "week", offset_one_day
        ) + sa.sql.expression.literal_column(f"interval '{offset}' day")


class BigQuery_GC(GranularityCompiler):
    """
    BigQuery has a native implementation for all granularities, including
    with custom week offsets.
    """

    def compile_week_offset(self, base, offset):
        return self.compile_default(base, f"week({offset})")

    def compile_default(self, base, granularity):
        f = (
            sa.func.timestamp_trunc
            if type(base.type) == sa.types.TIMESTAMP
            else sa.func.date_trunc
        )
        # We assume that truncating a datetime/timestamp/date column in BigQuery
        # preserves the type.
        return f(base, sa.literal_column(granularity), type_=base.type)


class SqlTextExpression_GC(GranularityCompiler, ABC):
    # FIXME: This approach renders SQL text and assumes that `base` is a
    # simple column reference. This will fail for more complex `base`s.
    # Subclasses should avoid this and refactor themselves to use `sa.func`
    # to build an expression tree, instead of using `.literal_column`.

    def compile_template_str(self, base: CompiledColumnExpression, template: str):
        formatted_column = (
            sa.literal_column(f"{base.table.name}.{base.name}")
            if hasattr(base, "table") and base.table is not None
            else base
        )
        return sa.sql.expression.literal_column(template.format(col=formatted_column))


class MySql_GC(SqlTextExpression_GC):
    """
    MySQL lacks a native date truncation function, so we need to implement each
    granularity ourselves with custom expressions.
    """

    def compile_second(self, base):
        return self.compile_template_str(
            base,
            "DATE_ADD(DATE({col}), INTERVAL (HOUR({col})*60*60 + MINUTE({col})*60 + SECOND({col})) SECOND)",
        )

    def compile_minute(self, base):
        return self.compile_template_str(
            base,
            "DATE_ADD(DATE({col}), INTERVAL (HOUR({col})*60 + MINUTE({col})) MINUTE)",
        )

    def compile_hour(self, base):
        return self.compile_template_str(
            base,
            "DATE_ADD(DATE({col}), " "INTERVAL HOUR({col}) HOUR)",
        )

    def compile_day(self, base):
        return self.compile_template_str(base, "DATE({col})")

    def compile_week(self, base):
        return self.compile_week_offset(base, 0)

    def compile_week_offset(self, base, offset):
        offset = _offset_for_sunday_1(offset)
        return self.compile_template_str(
            base,
            f"DATE(DATE_SUB({{col}}, INTERVAL (DAYOFWEEK({{col}}) - {offset} + 7) % 7 DAY))",
        )

    def compile_month(self, base):
        return self.compile_template_str(
            base,
            "DATE(DATE_SUB({col}, " "INTERVAL DAYOFMONTH({col}) - 1 DAY))",
        )

    def compile_quarter(self, base):
        return self.compile_template_str(
            base,
            "MAKEDATE(YEAR({col}), 1) + INTERVAL QUARTER({col}) QUARTER - INTERVAL 1 QUARTER",
        )

    def compile_year(self, base):
        return self.compile_template_str(
            base,
            "DATE(DATE_SUB({col}, " "INTERVAL DAYOFYEAR({col}) - 1 DAY))",
        )


class Clickhouse_GC(GranularityCompiler):
    def compile_week_offset(self, base, offset):
        if offset == 6:
            return sa.func.toStartOfWeek(base, 0)
        else:
            return sa.func.addDays(sa.func.toMonday(base), offset)

    def compile_week(self, base):
        return sa.func.toMonday(base)


class Databricks_GC(SqlTextExpression_GC):
    def compile_week(self, base):
        return self.compile_week_offset(base, 0)

    def compile_week_offset(self, base, offset):
        offset = _offset_for_sunday_1(offset)
        return self.compile_template_str(
            base,
            f"DATE(DATE_SUB({{col}}, (DAYOFWEEK({{col}}) - {offset} + 7) % 7))",
        )


class Snowflake_GC(GranularityCompiler):
    def compile_week_offset(self, base, offset):
        if type(base.type) == sa.types.DATE:
            return sa.func.date_trunc("week", base - offset) + offset
        else:
            return sa.func.dateadd(
                "DAY",
                offset,
                sa.func.date_trunc("week", sa.func.dateadd("DAY", -offset, base)),
            )


class DuckDB_GC(GranularityCompiler):
    def compile_week_offset(self, base, offset):
        # using literal_column instead of timedelta because timedelta doesn't compile
        # to a sql query (used for the view query functionality)
        offset_one_day = base - sa.sql.expression.literal_column(
            f"interval '{offset}' day"
        )
        return sa.func.date_trunc(
            "week", offset_one_day
        ) + sa.sql.expression.literal_column(f"interval '{offset}' day")


class Postgres_GC(GranularityCompiler):
    def compile_week_offset(self, base, offset):
        # using literal_column instead of timedelta because timedelta doesn't compile
        # to a sql query (used for the view query functionality)
        offset_one_day = base - sa.sql.expression.literal_column(
            f"'{offset} days' :: interval"
        )
        return sa.func.date_trunc(
            "week", offset_one_day
        ) + sa.sql.expression.literal_column(f"'{offset} days' :: interval")


def _offset_for_sunday_1(monday_zero_offset: str):
    """
    Returns a DAYOFWEEK offset appropriate for DBs which
    are 1-indexed beginning on Sunday.
    """
    return (monday_zero_offset + 1) % 7 + 1


def _validate_granularity(grain: str):
    if grain not in [
        "second",
        "minute",
        "hour",
        "day",
        "week",
        "month",
        "quarter",
        "year",
    ]:
        raise AssertionError("Invalid granularity: " + grain)
    return grain


# --- PREPROCESS ---


def preprocess_granularity_column_expression(
    expr: GranularityColumnExpression,
    layer: QueryLayer,
) -> ColumnExpression:
    """
    When working with datetimes whose values are known at compilation-time
    (e.g. "now" or a constant), we can apply the granularity in Python instead
    of leaving it to the underlying DB.
    """
    expr = copy(expr)
    expr.base = preprocess_column_expression(expr.base, layer)
    return _try_truncate_constant_in_python(expr, layer) or expr


def _try_truncate_constant_in_python(
    expr: GranularityColumnExpression, layer: QueryLayer
) -> Optional[PyValueColumnExpression]:
    constant_base_value = try_get_constant_ts_value(expr.base)
    if not constant_base_value:
        return None

    truncated_value = None
    if expr.granularity == "week":
        week_offset = DAY_OF_WEEK_MAPPING[layer.ctx.settings.first_day_of_week]
        offset = relativedelta(days=week_offset)
        truncated_value = (
            _truncate_py_datetime(constant_base_value - offset, expr.granularity)
            + offset
        )
    else:
        truncated_value = _truncate_py_datetime(constant_base_value, expr.granularity)

    return PyValueColumnExpression(truncated_value)


# --- REGISTER ---

register_column_expression_compiler(
    GranularityColumnExpression,
    compile_granularity_column_expression,
    preprocessors=[preprocess_granularity_column_expression],
)
