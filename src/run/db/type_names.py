from abc import ABC
from typing import *

import sqlalchemy as sa


class TypeNameMapping(ABC):
    """
    A `TypeNameMapping` is responsible for mapping between a string description
    of a database value type (such as `"BOOLEAN"`) and associating that to a
    `sa.types.TypeEngine` (such as `sa.types.BOOLEAN`).

    If a type mapping is not known, the Python value `None` is returned, _not_
    `sa.types.NullType`. Consumers are welcome to coerce `None` to
    `sa.types.NullType` on their own, throw an error, or anything else they
    deem appropriate.

    Usage:
    - This is used in reflection, in which DBAPI cursor `.description.type_code`
      strings must be loaded from a probe query into an `sa.types.TypeEngine`.
      These strings are cached.
    - This is used in user-space `cast` calls, in which a string must be
      turned into an explicit `sa.types.TypeEngine`.
    """

    def sa_type(self, type_name: str) -> Optional[sa.types.TypeEngine]:
        ...


# ---


class StrLookup_TNM(TypeNameMapping):
    """
    Implementation of `TypeNameMapping` which just uses a dictionary
    to map to no-argument functions which emit a type. If a string key is not
    found, `None` is returned.
    """

    def __init__(self, lookup: Dict[str, sa.types.TypeEngine]):
        super().__init__()
        self._lookup = lookup

    def sa_type(self, type_name):
        if constructor := self._lookup.get(type_name):
            return constructor()
        return None


# these are defined as function to allow for dynamic imports for any
# dialect-specific type signatures


def DuckDb_TNM():
    return StrLookup_TNM(
        {
            "STRING": sa.String,
            "NUMBER": sa.NUMERIC,
            "bool": sa.BOOLEAN,
            # DATETIME is a little complicated for DuckDB. DuckDB _does_ have an
            # extension that adds timezone handling (see https://duckdb.org/docs/extensions/overview.html),
            # but we don't currently use it.
            #
            # So, all datetime strings without TZ offsets will be interpreted as UTC,
            # and datetime strings _with_ TZ offsets will be converted to UTC. Our
            # DATETIME_TZ_NAIVE type treats every value as naive and lets the user
            # configure which time zone the values should be interpreted as. Ideally
            # we'd only apply this to values that don't have a specified TZ offset, but
            # we don't get that information back from the query, so it will be applied
            # to all values.
            #
            # This is probably fine — I don't expect CSVs with mixed "TZ aware" & "TZ
            # naive" values in one column to be a use case we need to support.
            "DATETIME": sa.DATETIME,
            "Date": sa.DATE,
            "Time": sa.String,  # TODO: should we do something better here?
        }
    )


def BigQuery_TNM():
    from google.cloud.bigquery import SqlTypeNames as BigQueryTypeName

    return StrLookup_TNM(
        {
            BigQueryTypeName.INTEGER.name: sa.INTEGER,
            BigQueryTypeName.FLOAT.name: sa.FLOAT,
            BigQueryTypeName.DECIMAL.name: sa.DECIMAL,
            #
            BigQueryTypeName.STRING.name: sa.String,
            BigQueryTypeName.BOOLEAN.name: sa.BOOLEAN,
            #
            BigQueryTypeName.DATE.name: sa.DATE,
            BigQueryTypeName.DATETIME.name: sa.DATETIME,
            BigQueryTypeName.TIME.name: sa.TIME,
            BigQueryTypeName.TIMESTAMP.name: sa.TIMESTAMP,
            #
            BigQueryTypeName.STRUCT.name: sa.JSON,
            # Best effort -- these types are not supported by the main Hashboard
            # app, so they may not be fully supported.
            BigQueryTypeName.RECORD.name: sa.JSON,
            BigQueryTypeName.BYTES.name: sa.BINARY,
            BigQueryTypeName.INTERVAL.name: sa.Interval,
            BigQueryTypeName.GEOGRAPHY.name: sa.String,
            BigQueryTypeName.BIGNUMERIC.name: sa.DECIMAL,
        }
    )


def Deprecated_GleanTypes_TNM(dialect):
    from .dialect import SqlDialect

    """
    This is a special type mapper which maps an abstract `GleanTypes` value
    into the appropriate `sa.type`. This is used by one unstable code path
    and is slated for removal at a later time.
    """
    mapping = {
        # basic types
        "numeric": sa.NUMERIC,
        "numeric.int": sa.INTEGER,
        "numeric.float": sa.FLOAT,
        "numeric.decimal": sa.DECIMAL,
        "boolean": sa.BOOLEAN,
        "string": sa.String,
        # temporal types
        "datetime.date": sa.DATE,
        "datetime.timezone.aware": sa.DATETIME,
        "datetime.timezone.naive": sa.DATETIME,
        "datetime.timezone.local": sa.TIMESTAMP,
        # array types
        "array.string": lambda: sa.types.ARRAY(sa.String),
        "array": lambda: sa.types.ARRAY(sa.types.NullType()),
        # explicit unknown types
        "unsupported": sa.types.NullType,
        "unknown": sa.types.NullType,
    }

    if dialect == SqlDialect.SNOWFLAKE:
        import snowflake.sqlalchemy as snowflake_sa

        mapping["datetime.timezone.aware"] = snowflake_sa.TIMESTAMP_TZ
        mapping["datetime.timezone.local"] = snowflake_sa.TIMESTAMP_LTZ

    return StrLookup_TNM(mapping)
