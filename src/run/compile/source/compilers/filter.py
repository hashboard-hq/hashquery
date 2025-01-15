from .....model.source.filter import FilterSource
from ...column_expression.compile_column_expression import compile_column_expression
from ..compile_source import (
    QueryContext,
    QueryLayer,
    compile_source,
    register_source_compiler,
)


def compile_filter_source(source: FilterSource, ctx: QueryContext) -> QueryLayer:
    base_layer = compile_source(source.base, ctx)
    # FILTER can be applied into an existing layer, just depends
    # on WHERE (pun!) we place the clause
    layer = base_layer

    condition = compile_column_expression(source.condition, layer)
    if layer.is_aggregated:
        layer.query = layer.query.having(condition)
    else:
        layer.query = layer.query.where(condition)
    return layer


register_source_compiler(FilterSource, compile_filter_source)
