from .....model.source.limit import LimitSource
from ..compile_source import (
    QueryContext,
    QueryLayer,
    compile_source,
    register_source_compiler,
)


def compile_limit_source(source: LimitSource, ctx: QueryContext) -> QueryLayer:
    base_layer = compile_source(source.base, ctx)
    # LIMIT can always be safely applied into an existing layer
    layer = base_layer

    layer.query = layer.query.limit(source.limit)
    if source.offset:
        layer.query = layer.query.offset(source.offset)

    layer.is_order_dependent = True
    return layer


register_source_compiler(LimitSource, compile_limit_source)
