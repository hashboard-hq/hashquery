from typing import Callable, Dict, Type, TypeVar

import sqlalchemy.sql as sa

from ....model.source import Source
from ..context import QueryContext
from ..query_layer import QueryLayer


def compile_source(source: Source, ctx: QueryContext) -> QueryLayer:
    """
    Compile the given source to its SQLAlchemy representation.
    Can be called with any `Source` subtype.
    """
    # see `QueryContext.add_alias_checkpoint` for more information
    # on this "alias checkpoint" concept.
    cached_result = ctx.get_alias_checkpoint(source)
    if cached_result is not None:
        cached_alias_ref, meta = cached_result
        select_from_cte = QueryLayer(ctx)
        select_from_cte.query = sa.select("*").select_from(cached_alias_ref)
        select_from_cte.namespaces.main.ref = cached_alias_ref
        select_from_cte.namespaces.main.column_metadata = meta
        return select_from_cte

    source_type = type(source)
    compile_func = SOURCE_COMPILER_REGISTRY.get(source_type)
    if not compile_func:
        raise NotImplementedError(
            f"Compiler for source type `{source_type.__name__}` was not found. "
            + f"Ensure a module calling `register_source_compiler({source_type.__name__}, ...)` "
            + "is registered inside of `./compilers/__init__.py`."
        )
    layer: QueryLayer = compile_func(source, ctx)
    layer.source = source
    return layer


SOURCE_COMPILER_REGISTRY: Dict[
    str,
    Callable[[Source, QueryContext], QueryLayer],
] = {}
SourceType = TypeVar("SourceType", bound=Source)


def register_source_compiler(
    source_type: Type[SourceType],
    builder: Callable[[SourceType, QueryContext], QueryLayer],
):
    """
    Registers the provided function as the one to use for compiling
    `Source`s of the given type into SQLAlchemy.
    """
    if source_type in SOURCE_COMPILER_REGISTRY:
        raise AssertionError(
            f"Conflicting implementations for compiling `{source_type.__name__}`."
        )
    SOURCE_COMPILER_REGISTRY[source_type] = builder
