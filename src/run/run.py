import os
from datetime import datetime
from typing import TYPE_CHECKING, Literal, Optional, Union

from ..model.connection import HashboardDataConnection
from ..utils.secret import resolve_secret
from ..utils.serializable import (
    HASHQUERY_WIRE_VERSION,
    HASHQUERY_WIRE_VERSION_KEY,
    WireFormatVersionError,
)
from .execute.execute_model import execute_model
from .run_results import JSONRunResults, LocalRunResults, RunResults

if TYPE_CHECKING:
    from ..model.model import Model  # avoid circular dep


def run(
    model: "Model",
    *,
    sql_only: bool = False,
    freshness: Optional[Union[datetime, Literal["latest"]]] = None,
    print_warnings: bool = True,
    print_exec_stats: bool = False,
) -> RunResults:
    if isinstance(model._connection, HashboardDataConnection):
        result_json = post_hashboard_execute_model_endpoint(
            model,
            sql_only=sql_only,
            freshness=freshness,
        )
        return JSONRunResults(
            result_json,
            print_warnings=print_warnings,
            print_exec_stats=print_exec_stats,
        )
    else:
        result, result_meta = execute_model(
            model,
            sql_only=sql_only,
            freshness=freshness,
        )
        return LocalRunResults(
            result,
            result_meta,
            print_warnings=print_warnings,
            print_exec_stats=print_exec_stats,
        )


def post_hashboard_execute_model_endpoint(
    model: "Model",
    *,
    sql_only: bool,
    freshness: Optional[Union[datetime, Literal["latest"]]],
):
    from ..integration.hashboard.api import HashboardAPI

    hb_connection: HashboardDataConnection = model._connection
    hb_connection_credentials = resolve_secret(hb_connection.credentials)
    api = HashboardAPI(hb_connection_credentials)

    # The Hashboard server supports multiple data formats to send the data back in
    # which is important for consumers who cannot install `arrow` locally (such
    # as in WebAssembly).
    df_wire_format = os.environ.get("HASHQUERY_DATA_FRAME_WIREFORMAT", default="arrow")
    if df_wire_format not in ("arrow", "csv"):
        raise ValueError(
            f"Unsupported HASHQUERY_DATA_FRAME_WIREFORMAT: {df_wire_format}"
        )

    freshness_datetime = datetime.now() if freshness == "latest" else freshness
    results_json = api.post(
        "db/v2/execute-model",
        {
            "model": model._to_wire_format(),
            "projectId": hb_connection.project_id,
            "options": {
                "sqlOnly": sql_only,
                "freshness": (
                    freshness_datetime.isoformat() if freshness_datetime else None
                ),
                "format": df_wire_format,
            },
        },
    )
    found_wire_version = results_json.get(HASHQUERY_WIRE_VERSION_KEY)
    if found_wire_version != HASHQUERY_WIRE_VERSION:
        raise WireFormatVersionError(
            expected=HASHQUERY_WIRE_VERSION,
            found=found_wire_version,
        )
    return results_json
