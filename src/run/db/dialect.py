from enum import Enum, unique
from typing import *

from sqlglot.dialects.dialect import Dialects as SqlglotDialect

from ...model import Connection
from ...model.connection import BigQueryConnection, DuckDBConnection
from . import dialect_sqlalchemy, type_names


@unique
class SqlDialect(Enum):
    ATHENA = "awsathena"
    BIGQUERY = "bigquery"
    CLICKHOUSE = "clickhouse"
    DATABRICKS = "databricks"
    DUCKDB = "duckdb"
    MYSQL = "mysql"
    POSTGRES = "postgresql"
    REDSHIFT = "redshift"
    SNOWFLAKE = "snowflake"


class DbEnumSet(TypedDict):
    connection_cls: Optional[Type]
    dialect: SqlDialect
    sqlglot_dialect: SqlglotDialect
    sqlalchemy_dialect: Any
    type_name_mapping: type_names.TypeNameMapping


_DB_ENUMS: List[DbEnumSet] = [
    {
        "connection_cls": NotImplemented,
        "dialect": SqlDialect.ATHENA,
        "sqlglot_dialect": SqlglotDialect.TRINO,
        "sqlalchemy_dialect": dialect_sqlalchemy.athena,
        "type_name_mapping": NotImplemented,
    },
    {
        "connection_cls": BigQueryConnection,
        "dialect": SqlDialect.BIGQUERY,
        "sqlglot_dialect": SqlglotDialect.BIGQUERY,
        "sqlalchemy_dialect": dialect_sqlalchemy.bigquery,
        "type_name_mapping": type_names.BigQuery_TNM,
    },
    {
        "connection_cls": NotImplemented,
        "dialect": SqlDialect.MYSQL,
        "sqlglot_dialect": SqlglotDialect.MYSQL,
        "sqlalchemy_dialect": dialect_sqlalchemy.mysql,
        "type_name_mapping": NotImplemented,
    },
    {
        "connection_cls": NotImplemented,
        "dialect": SqlDialect.POSTGRES,
        "sqlglot_dialect": SqlglotDialect.POSTGRES,
        "sqlalchemy_dialect": dialect_sqlalchemy.postgresql,
        "type_name_mapping": NotImplemented,
    },
    {
        "connection_cls": NotImplemented,
        "dialect": SqlDialect.SNOWFLAKE,
        "sqlglot_dialect": SqlglotDialect.SNOWFLAKE,
        "sqlalchemy_dialect": dialect_sqlalchemy.snowflake,
        "type_name_mapping": NotImplemented,
    },
    {
        "connection_cls": DuckDBConnection,
        "dialect": SqlDialect.DUCKDB,
        "sqlglot_dialect": SqlglotDialect.DUCKDB,
        "sqlalchemy_dialect": dialect_sqlalchemy.duckdb,
        "type_name_mapping": type_names.DuckDb_TNM,
    },
    {
        "connection_cls": NotImplemented,
        "dialect": SqlDialect.CLICKHOUSE,
        "sqlglot_dialect": SqlglotDialect.CLICKHOUSE,
        "sqlalchemy_dialect": dialect_sqlalchemy.clickhouse,
        "type_name_mapping": NotImplemented,
    },
    {
        "connection_cls": NotImplemented,
        "dialect": SqlDialect.DATABRICKS,
        "sqlglot_dialect": SqlglotDialect.DATABRICKS,
        "sqlalchemy_dialect": dialect_sqlalchemy.databricks,
        "type_name_mapping": NotImplemented,
    },
    # TODO: MotherDuck conflicts in terms of its `dialect` entry so
    # its awkward to stub out right now
    {
        "connection_cls": NotImplemented,
        "dialect": SqlDialect.REDSHIFT,
        "sqlglot_dialect": SqlglotDialect.REDSHIFT,
        "sqlalchemy_dialect": dialect_sqlalchemy.redshift,
        "type_name_mapping": NotImplemented,
    },
]

SqlDialectMappable = Union[Type[Connection], SqlDialect, SqlglotDialect]

_DB_MAPPING: Dict[SqlDialectMappable, DbEnumSet] = {}
for v in _DB_ENUMS:
    _DB_MAPPING[v["dialect"]] = v
    _DB_MAPPING[v["sqlglot_dialect"]] = v
    conn_cls = v["connection_cls"]
    if conn_type_key := conn_cls and getattr(conn_cls, "__TYPE_KEY__", None):
        _DB_MAPPING[conn_type_key] = v


def _db_mapping_lookup(mappable: SqlDialectMappable) -> DbEnumSet:
    conn_type_key = getattr(mappable, "__TYPE_KEY__", None)
    return _DB_MAPPING[conn_type_key or mappable]


# --- Conversion between different subtypes to their related ones ---


def to_dialect(mappable: SqlDialectMappable) -> SqlDialect:
    return _db_mapping_lookup(mappable)["dialect"]


def to_sqlglot_dialect(mappable: SqlDialectMappable) -> SqlglotDialect:
    return _db_mapping_lookup(mappable)["sqlglot_dialect"]


def to_sqlalchemy_dialect(mappable: SqlDialectMappable):
    return _db_mapping_lookup(mappable)["sqlalchemy_dialect"]()


def to_type_name_mapping(mappable: SqlDialectMappable) -> type_names.TypeNameMapping:
    return _db_mapping_lookup(mappable)["type_name_mapping"]()
