from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from ...model import Connection
from ...model.connection import BigQueryConnection, DuckDBConnection
from ...utils.copy import deepcopy_properties
from ...utils.secret import resolve_secret
from .dialect import SqlDialect, to_dialect


class ConnectionEngine:
    """Wrapper type for a SQLAlchemy engine along with its dialect."""

    def __init__(self, *, dialect: SqlDialect, sa_engine: Engine):
        self.dialect = dialect
        self.sa_engine = sa_engine

    @classmethod
    def create(cls, connection: Connection) -> "ConnectionEngine":
        dialect = to_dialect(connection)
        if isinstance(connection, DuckDBConnection):
            sa_engine = _create_duckdb_engine(connection)
        elif isinstance(connection, BigQueryConnection):
            sa_engine = _create_bigquery_engine(connection)
        else:
            raise ValueError(
                "Unrecognized Connection type. Cannot create SQLAlchemy engine."
            )
        return cls(dialect=dialect, sa_engine=sa_engine)

    def __deepcopy__(self, memo):
        # not safe to clone the database connection object
        return deepcopy_properties(self, memo, identity_keys=["sa_engine"])


def _create_duckdb_engine(connection: DuckDBConnection):
    engine = create_engine(
        "duckdb:///:memory:",
        connect_args={"config": resolve_secret(connection._duckdb_config)},
    )
    for table_name, secret_df in connection._table_map.items():
        df = resolve_secret(secret_df)
        engine.execute("register", (table_name, df))
    return engine


def _create_bigquery_engine(connection: BigQueryConnection):
    connect_kwargs = {}
    if connection._credentials_path:
        connect_kwargs["credentials_path"] = connection._credentials_path
    elif connection._credentials_info:
        connect_kwargs["credentials_info"] = resolve_secret(
            connection._credentials_info
        )
    if connection._location:
        connect_kwargs["location"] = connection._location

    return create_engine("bigquery://", **connect_kwargs)
