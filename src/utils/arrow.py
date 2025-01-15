import base64
from typing import Any
from uuid import UUID

import pandas as pd
import pyarrow as pa


def df_to_arrow_bytes(df: pd.DataFrame) -> bytes:
    df = df.applymap(_preprocess)
    arrow_table = pa.Table.from_pandas(df, preserve_index=False)
    schema = pa.Schema.from_pandas(df, preserve_index=False)
    sink = pa.BufferOutputStream()
    writer = pa.ipc.new_file(sink, schema)
    writer.write_table(arrow_table)
    writer.close()
    return sink.getvalue().to_pybytes()


def df_to_arrow_ipc(df: pd.DataFrame) -> bytes:
    arrow_bytes = df_to_arrow_bytes(df)
    return base64.b64encode(arrow_bytes).decode("utf-8")


def arrow_bytes_to_df(arrow_bytes: bytes) -> pd.DataFrame:
    with pa.ipc.open_file(arrow_bytes) as reader:
        table = reader.read_all()
        return table.to_pandas()


def arrow_ipc_to_df(arrow_ipc: str) -> pd.DataFrame:
    arrow_bytes = base64.b64decode(arrow_ipc)
    return arrow_bytes_to_df(arrow_bytes)


def _preprocess(val: Any):
    """
    This function contains a few workarounds for values that don't serialize
    well (or at all) to arrow.
    """
    if isinstance(val, UUID):
        # Arrow doesn't know how to serialize Python UUID objects, so we convert
        # them to strings ahead of time. This arises from Python UUIDs.
        return str(val)
    return val
