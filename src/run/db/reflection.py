from dataclasses import dataclass
from typing import *

import sqlalchemy as sa

from ...model.source import LimitSource, SqlTextSource, TableNameSource
from .dialect import to_type_name_mapping
from .engine import ConnectionEngine, SqlDialect


@dataclass
class ColumnTypeInfo:
    physical_name: str
    sa_type: sa.types.TypeEngine


class ReflectionFetcher:
    def __init__(self, engine: ConnectionEngine):
        self.engine = engine

    def get_source_columns(
        self, source: Union[SqlTextSource, TableNameSource]
    ) -> List[ColumnTypeInfo]:
        # TODO: caching goes here!
        return self._fetch_source_columns(source)

    def _fetch_source_columns(self, source: Union[SqlTextSource, TableNameSource]):
        # FIXME: Reflection in BigQuery does not yet work for arrays.
        # See `glean/repository/database_queries/metadata_queries.py` for more.

        # compile the source with LIMIT 0
        schema_query = self._compile_source_schema_query(source)

        # execute and read the cursor description into `type_names`
        columns_info: List[ColumnTypeInfo] = []
        with self.engine.sa_engine.connect() as conn:
            schema_results = conn.execute(schema_query)
            type_name_mapping = to_type_name_mapping(self.engine.dialect)
            for cursor_column_description in schema_results.cursor.description:
                physical_name, type_name = self._column_description_to_names(
                    cursor_column_description
                )
                sa_type = type_name_mapping.sa_type(type_name)
                if not sa_type:
                    # TODO: we should consider adding a warning here,
                    # but I worry it would be too noisy
                    sa_type = sa.types.NullType()
                columns_info.append(
                    ColumnTypeInfo(
                        physical_name=physical_name,
                        sa_type=sa_type,
                    )
                )

        return columns_info

    def _column_description_to_names(
        self, cursor_column_description
    ) -> Tuple[str, str]:
        """Map a cursor.description entry to a physical name and type name."""
        # some dialects return objects with properties, others return tuples
        if self.engine.dialect in [
            SqlDialect.POSTGRES,
            SqlDialect.REDSHIFT,
            SqlDialect.BIGQUERY,
        ]:
            return (cursor_column_description.name, cursor_column_description.type_code)
        else:
            return cursor_column_description[0], cursor_column_description[1]

    def _compile_source_schema_query(
        self, source: Union[SqlTextSource, TableNameSource]
    ) -> sa.sql.Select:
        # preventing circular imports
        from ..compile.context import QueryContext
        from ..compile.source.compile_source import compile_source

        class _NoReflectionQueryContext(QueryContext):
            def get_column_metadata_for_source(self, source):
                return {}

        ctx = _NoReflectionQueryContext(engine=None, settings=None)
        schema_only_src = LimitSource(source, limit=0)
        return compile_source(schema_only_src, ctx).finalized().query
