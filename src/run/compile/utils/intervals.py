from enum import Enum, auto

import sqlalchemy as sa

from ...db.dialect import SqlDialect


class IntervalUnit(Enum):
    SECOND = "SECOND"
    MINUTE = "MINUTE"
    HOUR = "HOUR"
    DAY = "DAY"
    WEEK = "WEEK"
    MONTH = "MONTH"
    YEAR = "YEAR"


# Seems like most dialects use one of the first two syntaxes, except our friend Athena.
class IntervalSyntaxType(Enum):
    UNQUOTED = auto()  # INTERVAL 2 DAY
    QUOTED = auto()  # INTERVAL '2 days'
    QUANTITY_QUOTED = auto()  # INTERVAL '2' DAY


DIALECT_TO_INTERVAL_SYNTAX_TYPE = {
    SqlDialect.ATHENA: IntervalSyntaxType.QUANTITY_QUOTED,
    SqlDialect.BIGQUERY: IntervalSyntaxType.UNQUOTED,
    SqlDialect.CLICKHOUSE: IntervalSyntaxType.UNQUOTED,
    SqlDialect.DATABRICKS: IntervalSyntaxType.QUOTED,
    SqlDialect.DUCKDB: IntervalSyntaxType.QUOTED,
    SqlDialect.MYSQL: IntervalSyntaxType.UNQUOTED,
    SqlDialect.POSTGRES: IntervalSyntaxType.QUOTED,
    SqlDialect.REDSHIFT: IntervalSyntaxType.QUOTED,
    SqlDialect.SNOWFLAKE: IntervalSyntaxType.QUOTED,
}


def to_sqlalchemy_interval(
    n: int,
    unit: IntervalUnit,
    dialect: SqlDialect,
) -> sa.sql.expression.ColumnElement:
    syntax_type = DIALECT_TO_INTERVAL_SYNTAX_TYPE[dialect]

    # Some help against potential SQL injection since we are using sa.text.
    if not isinstance(n, int):
        raise ValueError(f"Invalid interval quantity: {n}")
    if not isinstance(unit, IntervalUnit):
        raise ValueError(f"Invalid interval unit: {unit}")

    if syntax_type == IntervalSyntaxType.UNQUOTED:
        return sa.text(f"INTERVAL {n} {unit.value}")
    elif syntax_type == IntervalSyntaxType.QUOTED:
        interval_unit_mapping = {
            IntervalUnit.SECOND: "seconds",
            IntervalUnit.MINUTE: "minutes",
            IntervalUnit.HOUR: "hours",
            IntervalUnit.DAY: "days",
            IntervalUnit.WEEK: "weeks",
            IntervalUnit.MONTH: "months",
            IntervalUnit.YEAR: "years",
        }
        return sa.text(f"INTERVAL '{n} {interval_unit_mapping[unit]}'")
    elif syntax_type == IntervalSyntaxType.QUANTITY_QUOTED:
        return sa.text(f"INTERVAL '{n}' {unit.value}")
    else:
        raise ValueError(
            f"Invalid syntax type for interval during period comparison: {syntax_type}"
        )
