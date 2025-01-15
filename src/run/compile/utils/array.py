import sqlalchemy.sql as sa

from ...db.dialect import SqlDialect, to_sqlalchemy_dialect
from ..context import QueryContext
from .error import UserCompilationError


def array_contains(
    value: sa.ColumnElement,
    column: sa.ColumnElement,
    ctx: QueryContext,
):
    """
    Produces an SQLAlchemy expression which is TRUE if the provided `value` can
    be found inside of the given `column`.
    """
    dialect = ctx.dialect
    sqlalchemy_dialect = to_sqlalchemy_dialect(dialect)

    if dialect == SqlDialect.SNOWFLAKE:
        from snowflake.sqlalchemy import VARIANT

        return sa.func.array_contains(sa.cast(value, VARIANT), column)
    elif dialect in [SqlDialect.POSTGRES, SqlDialect.REDSHIFT]:
        return value == sa.any_(column)
    elif dialect == SqlDialect.BIGQUERY:
        # Doing .compile with literal binds is not secure against SQL injection,
        # but we haven't been able to find a way to get the IN UNNEST(..) syntax
        # for bigquery working without sa.text.
        compiled_value = str(
            value.compile(
                compile_kwargs={"literal_binds": True}, dialect=sqlalchemy_dialect
            )
        )
        # If `column` is just a regular old sa.Column, the BigQuery compiler will refuse to correctly include the prefixed table name in the case of
        # joined columns. We work around this but just writing it into text directly.
        compiled_column = (
            (
                column.name
                if column.table is None
                else f"`{column.table.name}`.`{column.name}`"
            )
            if isinstance(column, sa.Column)
            else str(
                column.compile(
                    compile_kwargs={"literal_binds": True}, dialect=sqlalchemy_dialect
                )
            )
        )
        return sa.text(f"{compiled_value} IN UNNEST({compiled_column})")
    else:
        raise UserCompilationError(
            "Array functionality is not supported for this dialect"
        )
