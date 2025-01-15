from copy import deepcopy

from .....model.source import TableNameSource
from .....model.source.join_one import JoinOneSource
from ...column_expression.compile_column_expression import compile_column_expression
from ..compile_source import (
    QueryContext,
    QueryLayer,
    compile_source,
    register_source_compiler,
)


def compile_join_one_source(source: JoinOneSource, ctx: QueryContext):
    base_layer = compile_source(source.base, ctx)
    # JOINs do not rewrite the selections, but it is commonly the case that
    # later transforms which _do_ rewrite selections rely on the results of
    # the JOIN. Since `chained` flattens out relations, it makes sense to
    # chain sooner rather than later.
    layer = base_layer if base_layer.can_set_selections else base_layer.chained()

    # compile the joined sub-table
    join_name = source.relation._identifier
    joined_ctx = ctx.fork_cte_names(
        # if the join requires its own chain of CTEs, then fork the names
        # so its clear which CTE is related to what sub-results
        f"joined_{join_name}"
    )
    joined_layer = compile_source(
        source.relation._nested_model._source,
        joined_ctx,
    ).finalized()
    if type(source.relation._nested_model._source) == TableNameSource:
        joined_reference = (
            joined_layer.namespaces.main.ref
        )  # unwrap this for simplicity
    else:
        joined_reference = joined_layer.query.cte(joined_ctx.next_cte_name())
    joined_reference = joined_reference.alias(ctx.next_alias_name(join_name))

    # add this namespace to the layer for disambiguation and inform the layer
    # that a join is happening. This is needed _before_ we run `compile_column_expression`
    # for the `ON` clause so those refs are disambiguated.
    layer.namespaces.set_joined(
        join_name, joined_reference, joined_layer.namespaces.main.column_metadata
    )
    layer.is_joined = True

    # Freeze this layer here. When we finalize, we want to compile the condition
    # expression as if the layer was at this point, not with any future changes
    frozen_layer = deepcopy(layer)

    # split this compilation into multiple stages
    def on_finalize(layer: QueryLayer):
        # actually add the JOIN during finalization, and only if the JOIN is needed
        # this is fine, SQLAlchemy is built to allow joins to come in at the end,
        # since it is mirroring SQL, where the order of clauses can be interwoven
        # with its dependent references (ie. SELECT comes first).
        if join_name not in layer.namespaces.used_names:
            return
        joined_reference = layer.namespaces.named(join_name).ref
        join_condition = compile_column_expression(source.join_condition, frozen_layer)
        layer.query = layer.query.join(
            joined_reference, join_condition, isouter=(not source.drop_unmatched)
        )

    layer.on_finalize_handlers.append(on_finalize)
    return layer


register_source_compiler(JoinOneSource, compile_join_one_source)
