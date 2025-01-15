# -----
# All these imports need to be defensive because a client may not
# have all the drivers installed.
# -----


def athena():
    try:
        from pyathena.sqlalchemy_athena import AthenaDialect
    except:
        return None
    return AthenaDialect()


def bigquery():
    try:
        from sqlalchemy_bigquery import BigQueryDialect
    except:
        return None
    return BigQueryDialect(paramstyle="pyformat")


def mysql():
    try:
        from sqlalchemy.dialects import mysql
    except:
        return None
    return mysql.dialect()


def postgresql():
    try:
        from sqlalchemy.dialects import postgresql
    except:
        return None
    return postgresql.dialect()


def snowflake():
    try:
        from snowflake.sqlalchemy.snowdialect import SnowflakeDialect
    except:
        return None
    return SnowflakeDialect()


def duckdb():
    try:
        from duckdb_engine import Dialect as DuckDBDialect
    except:
        return None
    return DuckDBDialect(paramstyle="qmark")


def clickhouse():
    try:
        from clickhouse_sqlalchemy.drivers.base import ClickHouseDialect
    except:
        return None
    return ClickHouseDialect()


def databricks():
    try:
        from databricks.sqlalchemy.dialect import DatabricksDialect
    except:
        return None
    return DatabricksDialect(paramstyle="pyformat")


def redshift():
    try:
        from sqlalchemy_redshift.dialect import RedshiftDialect
    except:
        return None
    return RedshiftDialect()
