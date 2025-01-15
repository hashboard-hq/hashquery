import sqlalchemy.sql as sa

from .....model.source.union import UnionSource
from ..compile_source import (
    QueryContext,
    QueryLayer,
    compile_source,
    register_source_compiler,
)


def compile_union_source(source: UnionSource, ctx: QueryContext) -> QueryLayer:
    # we don't need to chain these, because UNION ALL operates on subqueries
    base_layer = compile_source(source.base, ctx).finalized()
    union_layer = compile_source(
        source.union_source, ctx.fork_cte_names("union_target")
    ).finalized()

    if type(source.base) == UnionSource:
        # we can fold this union into the prior in place
        compound_select: sa.CompoundSelect = base_layer.query.froms[0].element
        compound_select.selects.append(union_layer.query)
        return base_layer
    else:
        base_layer.query = base_layer.query.union_all(union_layer.query)
        # we have to chain here because a `sa.union_all` call creates a
        # CompoundSelect type which our other code doesn't know how to handle
        # properly.
        return base_layer.chained()


register_source_compiler(UnionSource, compile_union_source)
