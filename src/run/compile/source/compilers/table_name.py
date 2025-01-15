import logging

import sqlalchemy.sql as sa

from .....model.source.table_name import TableNameSource
from ..compile_source import QueryContext, QueryLayer, register_source_compiler

logger = logging.getLogger(__name__)


def compile_table_name_source(source: TableNameSource, ctx: QueryContext) -> QueryLayer:
    layer = QueryLayer(ctx)
    layer.source = source

    table = sa.table(source.table_name, schema=source.schema)
    layer.query = sa.select("*").select_from(table)
    layer.namespaces.main.ref = table
    try:
        layer.namespaces.main.column_metadata = ctx.get_column_metadata_for_source(
            source
        )
    except Exception as e:
        logger.exception(e)
        ctx.add_warning(f"Error fetching column metadata for table: {source}")
    return layer


register_source_compiler(TableNameSource, compile_table_name_source)
