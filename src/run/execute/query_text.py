from typing import *

import sqlalchemy as sa
from sqlparse import format as sql_format

from ..db.engine import ConnectionEngine, SqlDialect


def get_query_text(query: sa.sql.Select, engine: ConnectionEngine):
    query_str = str(
        query.compile(
            engine.sa_engine,
            compile_kwargs={"literal_binds": True},
        )
    )
    if engine.dialect != SqlDialect.DUCKDB:
        # Workaround for SQlAlchemy incorrectly escaping (doubling) % characters
        # when rendering the query. The DuckDB driver doesn't suffer from this.
        # https://docs.sqlalchemy.org/en/20/faq/sqlexpressions.html#why-are-percent-signs-being-doubled-up-when-stringifying-sql-statements
        query_str = query_str.replace("%%", "%")

    query_str = format_sql(query_str, reindent_aligned=True)
    query_str = "\n".join(l for l in query_str.splitlines() if l.strip())
    return query_str.strip() + ";"


# Large SQL queries can cause performance problems when reindent=True.
# 10k chars was chosen here because we saw a 50k character query taking 10s to reindent.
SQL_PARSE_REINDENT_MAX_CHARS = 10000


def format_sql(
    sql: str,
    keyword_case: Optional[bool] = None,
    reindent_aligned: bool = False,
) -> str:
    """Performantly prettify a SQL statement."""
    should_reindent = len(sql) <= SQL_PARSE_REINDENT_MAX_CHARS
    return sql_format(
        sql,
        reindent=should_reindent,
        reindent_aligned=(should_reindent and reindent_aligned),
        keyword_case=keyword_case,
    )
