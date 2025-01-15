import datetime
from typing import Union

import sqlalchemy as sa

from .....model.column_expression.py_value import PyValueColumnExpression
from .....utils.timeinterval import timeinterval
from ....db.dialect import SqlDialect
from ...utils.custom_value_types import LiteralDate, LiteralDateTime
from ...utils.intervals import IntervalUnit, to_sqlalchemy_interval
from ..compile_column_expression import QueryLayer
from ..compiler_registry import (
    CompiledColumnExpression,
    register_column_expression_compiler,
)


def compile_py_value_column_expression(
    expr: PyValueColumnExpression,
    layer: QueryLayer,
) -> CompiledColumnExpression:
    value = expr.value
    if type(value) is str:
        return sa.literal(value, type_=sa.String)
    if type(value) is list:
        return sa.literal(value)
    elif type(value) is datetime.datetime:
        return _compile_datetime(value, dialect=layer.ctx.dialect)
    elif type(value) is datetime.date:
        return _compile_date(value, dialect=layer.ctx.dialect)
    elif type(value) in [datetime.timedelta, timeinterval]:
        return _compile_interval(value, dialect=layer.ctx.dialect)
    elif value is None:
        # Not all dialects deal well with literals with `None` values.
        # We instead directly construct a "NULL" object.
        return sa.null()
    return sa.literal(value)


def _compile_datetime(value: datetime.datetime, dialect: SqlDialect):
    if dialect == SqlDialect.SNOWFLAKE:
        if type(value) != datetime.datetime:
            # safety check for literal_column below.
            raise ValueError(
                f"Tried to compile a datetime.datetime literal but type was {type(value)} instead of datetime.datetime"
            )
        return sa.literal_column(
            f"'{value.isoformat(' ', timespec='microseconds')}'::DATETIME",
            type_=sa.DATETIME,
        )
    else:
        # Note - the Bigquery driver defines how to bind literal
        # values for datetimes, so no need to use the custom type for them.
        return sa.literal(
            value,
            type_=(
                None
                if dialect == SqlDialect.BIGQUERY
                else LiteralDateTime(dialect=dialect)
            ),
        )


def _compile_date(value: datetime.date, dialect: SqlDialect):
    if dialect == SqlDialect.SNOWFLAKE:
        if type(value) != datetime.date:
            # safety check for literal_column below.
            raise ValueError(
                f"Tried to compile a datetime.date literal but type was {type(value)} instead of datetime.date"
            )
        return sa.literal_column(f"'{value.isoformat()}'::DATE", type_=sa.DATE)
    else:
        # Note - the Bigquery driver defines how to bind literal
        # values for dates, so no need to use the custom type for them.
        return sa.literal(
            value,
            type_=(
                None if dialect == SqlDialect.BIGQUERY else LiteralDate(dialect=dialect)
            ),
        )


def _compile_interval(value: Union[datetime.timedelta, timeinterval], dialect):
    """
    Helper function to compile a timedelta or a timeinterval to a sql interval.

    A few notes about timedeltas - they only track a quantity of microseconds, not separate units.
    Two things arise out of this:
    - We don't support sub-second intervals, so first we truncate to # of seconds.
    - We (best-effort) convert to the widest possible unit with a whole number of that unit,
        e.g. 3600 seconds -> 1 hour, 86400 seconds -> 1 day.
    """

    if type(value) == timeinterval:
        val = value.num
        if value.unit == "seconds":
            interval_unit = IntervalUnit.SECOND
        elif value.unit == "minutes":
            interval_unit = IntervalUnit.MINUTE
        elif value.unit == "hours":
            interval_unit = IntervalUnit.HOUR
        elif value.unit == "days":
            interval_unit = IntervalUnit.DAY
        elif value.unit == "months":
            interval_unit = IntervalUnit.MONTH
        elif value.unit == "years":
            interval_unit = IntervalUnit.YEAR
        else:
            raise ValueError(f"Unknown interval unit {value.unit}")
    else:
        interval_multipliers = [
            (IntervalUnit.SECOND, 60),
            (IntervalUnit.MINUTE, 60),
            (IntervalUnit.HOUR, 24),
            (IntervalUnit.DAY, None),
        ]

        # We don't allow sub-second intervals.
        val = int(value.total_seconds())
        interval_idx = 0

        # Convert the interval to the widest possible unit with a whole number of that unit.
        while (
            interval_idx < len(interval_multipliers) - 1
            and val % interval_multipliers[interval_idx][1] == 0
        ):
            val = val // interval_multipliers[interval_idx][1]
            interval_idx += 1
        interval_unit = interval_multipliers[interval_idx][0]

    return to_sqlalchemy_interval(val, interval_unit, dialect)


register_column_expression_compiler(
    PyValueColumnExpression, compile_py_value_column_expression, preprocessors=None
)
