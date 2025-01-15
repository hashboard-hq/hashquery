from .....model.source.sort import SortSource
from ....db.dialect import SqlDialect
from ...column_expression.compile_column_expression import compile_column_expression
from ..compile_source import (
    QueryContext,
    QueryLayer,
    compile_source,
    register_source_compiler,
)


def compile_sort_source(source: SortSource, ctx: QueryContext) -> QueryLayer:
    base_layer = compile_source(source.base, ctx)
    # ORDER BY can always be safely applied into an existing layer
    layer = base_layer

    sort_column = compile_column_expression(source.sort, layer)
    if source.dir == "asc":
        sort_column = sort_column.asc()
    elif source.dir == "desc":
        sort_column = sort_column.desc()
    else:
        raise ValueError(f"Invalid sort direction: {source.dir}")

    dialect_supports_nulls_ordering = ctx.dialect != SqlDialect.MYSQL
    if not dialect_supports_nulls_ordering:
        pass
    elif source.nulls == "first":
        sort_column = sort_column.nulls_first()
    elif source.nulls == "last":
        sort_column = sort_column.nulls_last()
    elif source.nulls == "auto":
        if source.dir == "asc":
            sort_column = sort_column.nulls_first()
        else:
            sort_column = sort_column.nulls_last()
    else:
        raise ValueError(f"Invalid nulls ordering: {source.nulls}")

    layer.query = layer.query.order_by(sort_column)
    layer.is_order_dependent = True
    return layer


register_source_compiler(SortSource, compile_sort_source)
