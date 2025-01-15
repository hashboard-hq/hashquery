import sqlalchemy.sql as sa

from .....model.source.aggregate import AggregateSource
from ....db.dialect import SqlDialect
from ...column_expression.compile_column_expression import compile_column_expression
from ..compile_source import (
    QueryContext,
    QueryLayer,
    compile_source,
    register_source_compiler,
)


def compile_aggregate_source(source: AggregateSource, ctx: QueryContext) -> QueryLayer:
    base_layer = compile_source(source.base, ctx)

    layer = base_layer if base_layer.can_aggregate else base_layer.chained()

    groups = [compile_column_expression(g, layer, labeled=True) for g in source.groups]
    measures = [
        compile_column_expression(m, layer, labeled=True) for m in source.measures
    ]
    selections = groups + measures

    can_use_group_indices = ctx.dialect not in [SqlDialect.CLICKHOUSE]

    group_expressions = (
        [sa.literal_column(str(i)) for i in range(1, len(groups) + 1)]
        if can_use_group_indices
        else [sa.column(x.identifier) for x in source.groups]
    )

    layer.is_aggregated = True
    layer.has_selections = True
    layer.query = layer.query.with_only_columns(selections).group_by(*group_expressions)
    return layer


register_source_compiler(AggregateSource, compile_aggregate_source)
