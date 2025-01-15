import json
from dataclasses import dataclass
from typing import *

import pandas as pd

from ...utils.builder import builder_method
from ...utils.secret import Secret
from .connection import Connection

LocalDataFrameRef = Union[
    pd.DataFrame,
    str,  # path to a .csv, .json, or .parquet file
    List[dict],  # a list of Python records with keys as column names
]


@dataclass
class DuckDBConnection(Connection):
    def __init__(self, **tables: LocalDataFrameRef):
        """
        Initialize a new DuckDB connection with the provided contents.
        Takes in a mapping of names to content data frames, which will each be
        loaded into a DuckDB instance as table with that name.

        Content can be specified as a path to a .csv, .json, or .parquet file,
        a pandas DataFrame, or a list of Python records.
        """
        self._table_map: Dict[str, Secret[pd.DataFrame]] = {}
        self._duckdb_config = Secret({})
        for df_name, df_ref in tables.items():
            DuckDBConnection.with_table.mutate(self, df_name, df_ref)

    @builder_method
    def with_table(self, table_name: str, content: LocalDataFrameRef) -> "DuckDBConnection":
        """
        Adds the given data frame to the DuckDB connection, registering it
        as a table with the given name. If a table already exists with that
        name, it will be overwritten.

        Content can be specified as a path to a .csv, .json, or .parquet file,
        a pandas DataFrame, or a list of Python records.
        """
        df = _load_df_from_content_ref(content)
        if s := self._table_map.get(table_name):
            s._set(df)
        else:
            self._table_map[table_name] = Secret(df)

    @builder_method
    def with_config(self, **options: Any) -> "DuckDBConnection":
        """
        Updates the DuckDB configuration:
        https://duckdb.org/docs/configuration/overview.html#configuration-reference
        Hashquery may not support all possible DuckDB configurations.
        """
        self._duckdb_config._set({**self._duckdb_config, **options})

    # --- Serialization ---

    __TYPE_KEY__ = "duckdb"

    def _to_wire_format(self) -> dict:
        return {
            **super()._to_wire_format(),
            # `content_map` is unsafe because it contains unaggregated data
            # which is generally our threshold for "too sensitive to send".
            "tableMap": {
                df_name: Secret.PLACEHOLDER for df_name in self._table_map
            },
            # `duckdb_config` is unsafe for the following reasons:
            # - it could have secrets in it, such as `s3_secret_access_key`
            # - options in it could configure a computer to allocate memory,
            #   open ports, or make network requests to arbitrary hosts
            #   with inlined extensions or similar
            # - options in it are often not relevant across environments
            #   anyways, such as `home_directory` being a file path
            "duckDBConfig": Secret.PLACEHOLDER,
        }

    @classmethod
    def _from_wire_format(cls, wire: dict) -> "DuckDBConnection":
        assert wire["subType"] == cls.__TYPE_KEY__
        res = DuckDBConnection()
        for df_name in wire["tableMap"]:
            # force set these as `None` to indicate that these named frames
            # but that they need to be rehydrated
            res._table_map[df_name] = None


def _load_df_from_content_ref(content_ref: LocalDataFrameRef) -> pd.DataFrame:
    if isinstance(content_ref, pd.DataFrame):
        return content_ref
    elif isinstance(content_ref, list):  # python records
        return pd.DataFrame.from_records(content_ref)
    elif isinstance(content_ref, str):  # path to file
        if content_ref.endswith(".csv"):
            return pd.read_csv(content_ref)
        elif content_ref.endswith(".parquet"):
            return pd.read_parquet(content_ref)
        elif content_ref.endswith(".json"):
            return pd.read_json(content_ref)
        elif content_ref.endswith(".xlsx") or content_ref.endswith(".xls"):
            return pd.read_excel(content_ref)
        else:
            raise ValueError(
                "Cannot load file. "
                + "Please provide a CSV, Parquet, JSON, or Excel file."
            )
    else:
        raise ValueError("Unsupported content type")
