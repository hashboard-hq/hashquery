from copy import deepcopy
from typing import Callable, List, Optional, cast

import sqlalchemy as sa

from ...model.source.source import Source
from ...utils.copy import deepcopy_properties
from ..db.reflection import ColumnTypeInfo
from .context import QueryContext
from .query_layer_namespaces import QueryLayerNamespaces


class QueryLayer:
    """
    Tracks the contents of a query at this "layer", which is roughly analogous
    to each CTE/subquery. One or more source transforms may be bundled into a
    single layer if they are compatible.
    """

    def __init__(self, ctx: QueryContext) -> None:
        # having this reference just makes passing around context a bit easier
        self.ctx = ctx

        self.query: sa.sql.Select = None
        self.source: Source = None

        """
        The properties below describe structural aspects of the `.query`.
        When modifying `.query`, it is the consumers responsibility
        to update these properties accordingly.

        These properties could _maybe_ be synthesized from the
        contents of `self.query` directly, but doing so is error prone
        and difficult since `sa.Select` is essentially an opaque type.
        """
        self.has_selections: bool = False
        self.is_aggregated: bool = False
        self.is_joined: bool = False
        self.is_order_dependent: bool = False

        """
        See doc comment for `QueryLayerNamespaces` for more context.

        Similar to the query properties above, when modifying `.query`,
        it is the consumers responsibility to update this instance accordingly.
        """
        self.namespaces = QueryLayerNamespaces()

        """
        Some source compilations will want a chance to apply final changes
        when the layer is finalized, which this list stores the handlers for.

        For example, the `JoinOneSource` compiler will skip its own compilation
        if later sources never reference the joined column it introduced.
        """
        self.on_finalize_handlers: List[Callable[[QueryLayer], None]] = []

    @property
    def can_aggregate(self) -> bool:
        """
        You can aggregate on top of this layer.
        """
        return (
            # aggregating rewrites the selected columns
            not self.has_selections
            # cannot aggregate an aggregate within the same layer
            and not self.is_aggregated
            # ordering is lost during aggregation, so any order-dependent
            # operations must be materialized into a layer before proceeding
            and not self.is_order_dependent
        )

    @property
    def can_set_selections(self) -> bool:
        """
        You can rewrite the select statement without invalidating what has
        already been specified. Typically this means turning `SELECT *` into
        something more specific, like `SELECT name, age`.
        """
        return (
            # already has selections specified
            not self.has_selections
            # aggregation introduces a clause which depends on the order of the
            # selections, so rewriting them is not safe
            and not self.is_aggregated
        )

    @property
    def needs_column_disambiguation(self) -> bool:
        """
        If true, column names should be disambiguated (with `.namespaces`)
        or else there may be name conflicts.
        """
        return (
            # joins introduce the potential for namespace collisions
            self.is_joined
            # if this is aggregated, future expressions are against the
            # aggregated names, which flattens joined relations away
            and not self.is_aggregated
        )

    def finalized(self) -> "QueryLayer":
        """
        Finalizes this layer, such that no more modifications should be made
        on or against it.
        """
        while handler := (
            self.on_finalize_handlers.pop(0) if self.on_finalize_handlers else None
        ):
            handler(self)
        return self

    def chained(self) -> "QueryLayer":
        """
        Return a new layer which selects from this QueryLayer as its base,
        forming a chain of CTEs. This finalizes the base layer.

        Calling `chained` flattens namespaces from any joins to their base
        names. So "product.name" becomes just "name". This is fine except for
        conflicting names, if there's also "salesperson.name", that will also
        flatten to "name", and the DB will shadow one for the other.

        Additionally, calling `chained` (potentially) alters the column metadata
        for the new layer. The new column metadata is either:
        - The same as the base (if doing a SELECT *)
        - Constructed from the SQLAlchemy selected columns attached to the query.

        As a result, it's best to call `.chained` only when you need to,
        and generally only when you have to rewrite the SELECTions anyways
        where preserved columns (and their names) are explicitly provided.
        """
        base = self.finalized()
        result = QueryLayer(base.ctx)
        chained_cte_ref = base.query.cte(base.ctx.next_cte_name())
        result.query = cast(sa.sql.Select, sa.select("*")).select_from(chained_cte_ref)
        result.has_selections = False
        result.namespaces.main.ref = chained_cte_ref

        if not base.has_selections:
            # This is a SELECT *, so the new column metadata is the same as the base.
            # TODO(wilson): figure out what happens if a join is present here -- the names should be flattened, I think.
            result.namespaces.main.column_metadata = (
                base.namespaces.main.column_metadata
            )
        else:
            # Set the column metadata to reflect the names coming **out** of the query.
            result.namespaces.main.column_metadata = {
                col.name: ColumnTypeInfo(col.name, col.type)
                for col in base.query.selected_columns
            }

        # when we chain, this provided an opportunity for reuse, if a future
        # layer requests the same source as this layer represents, we can just
        # point it at the same CTE reference.
        if self.source:
            self.ctx.add_alias_checkpoint(
                self.source, chained_cte_ref, result.namespaces.main.column_metadata
            )

        return result

    def get_column_type(
        self, physical_name: str, namespace_identifier: Optional[str] = None
    ) -> sa.types.TypeEngine:
        """Gets the SQLAlchemy type for a column by its physical name, using `namespace_identifier` to disambiguate across joins.

        Returns a NullType if the column is not found."""
        ns = (
            self.namespaces.named(namespace_identifier)
            if namespace_identifier
            else self.namespaces.main
        )
        if column_info := ns.column_metadata.get(physical_name):
            return column_info.sa_type
        return sa.types.NullType()

    def __deepcopy__(self, memo):
        # these references must be _shallow_-copied, since SQLAlchemy
        # constructs are immutable but rely on object-identity
        return deepcopy_properties(self, memo, identity_keys=["ctx", "query"])
