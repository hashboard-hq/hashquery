import sqlalchemy.sql as sa

from .....model.column_expression.column_name import ColumnNameColumnExpression
from ....db.dialect import SqlDialect
from ...query_layer import QueryLayer
from ..compiler_registry import (
    CompiledColumnExpression,
    register_column_expression_compiler,
)


def compile_column_name_column_expression(
    expr: ColumnNameColumnExpression,
    layer: QueryLayer,
) -> CompiledColumnExpression:
    namespace_identifier = (
        expr._namespace_identifier if layer.needs_column_disambiguation else None
    )

    if layer.needs_column_disambiguation or (
        _column_name_needs_disambiguation(expr.column_name, layer.ctx.dialect)
        and not layer.is_aggregated
    ):
        table = (
            layer.namespaces.named(namespace_identifier).ref
            if namespace_identifier
            else layer.namespaces.main.ref
        )
    else:
        table = None
    result = sa.column(
        expr.column_name,
        _selectable=table,
        type_=layer.get_column_type(expr.column_name, namespace_identifier),
    )
    result = apply_dialect_workarounds(result, layer.ctx.dialect)
    return result


def _column_name_needs_disambiguation(column_name: str, dialect: SqlDialect) -> bool:
    if dialect in [SqlDialect.REDSHIFT, SqlDialect.CLICKHOUSE]:
        # some parsers can get tripped up with an unqualified `timestamp`
        # identifier since that's also a keyword within some functions and
        # literal expressions
        return column_name == "timestamp"

    return False


def apply_dialect_workarounds(
    column: sa.ColumnElement, dialect: SqlDialect
) -> sa.ColumnElement:
    if dialect == SqlDialect.SNOWFLAKE:
        # defensive import since snowflake driver might not be installed
        import snowflake.sqlalchemy as snowflake_sa

        if type(column.type) in (
            snowflake_sa.TIMESTAMP_TZ,
            snowflake_sa.TIMESTAMP_LTZ,
        ):
            # There are numerous issues with Snowflake's handling of timezone-aware
            # datetimes, mostly in datetime truncation. Namely, it maintains the
            # timezone post-truncation, which causes issues with how the values get
            # grouped, and later displayed in charts.
            #
            # To work around them, we just cast all TIMESTAMP_TZ and TIMESTAMP_LTZ
            # columns to TIMESTAMP_NTZ.
            #
            # This was added due to HB-10231.
            return sa.cast(column, snowflake_sa.TIMESTAMP_NTZ)
    return column


register_column_expression_compiler(
    ColumnNameColumnExpression,
    compile_column_name_column_expression,
)
