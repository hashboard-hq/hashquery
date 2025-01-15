import logging
import re

import sqlalchemy as sa

from .....model.source.sql_text import SqlTextSource
from ..compile_source import QueryContext, QueryLayer, register_source_compiler

logger = logging.getLogger(__name__)


def compile_sql_source(source: SqlTextSource, ctx: QueryContext) -> QueryLayer:
    layer = QueryLayer(ctx)
    layer.source = source

    column_metadata = {}
    try:
        column_metadata = ctx.get_column_metadata_for_source(source)
    except Exception as e:
        logger.exception(e)
        ctx.add_warning(f"Error fetching column metadata for SQL query: {source}")

    cleaned_sql = clean_sql(source.sql)

    layer.query = sa.text(cleaned_sql).columns(
        *[sa.Column(c.physical_name, type_=c.sa_type) for c in column_metadata.values()]
    )
    # We set this because chained() cannot **assume** the layer has no selections.
    # In practice, this is used to pass the column name/type information to the next layer.
    layer.has_selections = True

    # Do not allow any name we declare to conflict with an identifier used
    # inside of this SQL block.
    layer.ctx.add_reserved_name(cleaned_sql, match_any_substring=True)

    # chain this immediately since we don't know anything about the
    # structure of the underlying text
    return layer.chained()


def clean_sql(sql: str) -> str:
    sql = sql.strip()
    # strip any trailing semi-colons and comments, which can both break stuff
    sql = re.sub(r";?\s*(--[^\n]+)?$", "", sql)
    return sql


register_source_compiler(SqlTextSource, compile_sql_source)
