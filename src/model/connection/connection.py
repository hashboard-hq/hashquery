from abc import ABC
from typing import *

from ...utils.serializable import Serializable

if TYPE_CHECKING:
    from .bigquery_connection import BigQueryConnection
    from .duckdb_connection import DuckDBConnection, LocalDataFrameRef


class Connection(Serializable, ABC):
    # --- Factory Functions ---

    @classmethod
    def duckdb(cls, **contents: "LocalDataFrameRef") -> "DuckDBConnection":
        """
        Initialize a new DuckDB connection with the provided contents.
        Takes in a mapping of names to content data frames, which will each be
        loaded into a DuckDB instance as table with that name.

        Content can be specified as a path to a .csv, .json, or .parquet file,
        a pandas DataFrame, or a list of Python records.
        """
        from .duckdb_connection import DuckDBConnection

        return DuckDBConnection(**contents)

    @classmethod
    def bigquery(
        cls,
        *,
        credentials_path: Optional[str] = None,
        credentials_info: Optional[dict] = None,
    ) -> "BigQueryConnection":
        """
        Initialize a new BigQuery connection.

        By default, the connection will be authenticated using the
        environment variable `GOOGLE_APPLICATION_CREDENTIALS`, according to the
        same logic as the BigQuery client library.
        https://cloud.google.com/docs/authentication/application-default-credentials

        Alternatively, you can specify an explicit path to a service account
        JSON file using `credentials_path`, or use a python dictionary using
        `credentials_info`.
        https://github.com/googleapis/python-bigquery-sqlalchemy?tab=readme-ov-file#authentication
        """
        from .bigquery_connection import BigQueryConnection

        return BigQueryConnection(
            credentials_path=credentials_path,
            credentials_info=credentials_info,
        )

    # --- Serialization ---

    # required by all concrete subclasses
    __TYPE_KEY__ = None

    def __init_subclass__(cls) -> None:
        super().__init_subclass__()
        type_key = cls.__TYPE_KEY__
        if not type_key:
            return
        elif type_key in CONNECTION_TYPE_KEY_REGISTRY:
            raise AssertionError(
                "Multiple Connection subclasses for same type key: " + type_key
            )
        CONNECTION_TYPE_KEY_REGISTRY[type_key] = cls

    def _to_wire_format(self) -> dict:
        return {"type": "connection", "subType": self.__TYPE_KEY__}

    @classmethod
    def _from_wire_format(cls, wire: dict) -> "Connection":
        assert wire["type"] == "connection"
        type_key = wire["subType"]
        ConnectionType = CONNECTION_TYPE_KEY_REGISTRY.get(type_key)
        if not ConnectionType:
            raise AssertionError("Unknown Connection type key: " + type_key)
        return ConnectionType._from_wire_format(wire)


CONNECTION_TYPE_KEY_REGISTRY: Dict[
    str,
    Type[Serializable],
] = {}
