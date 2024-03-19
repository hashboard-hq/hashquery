import os
from datetime import datetime
from typing import TYPE_CHECKING, Literal, Optional, Union

from ..hashboard_api.api import HashboardAPI
from ..utils.serializable import (
    HASHQUERY_WIRE_VERSION,
    HASHQUERY_WIRE_VERSION_KEY,
    WireFormatVersionError,
)
from .run_results import RunResults

if TYPE_CHECKING:
    from ..model.model import Model  # avoid circular dep

HASHQUERY_DATA_FRAME_WIREFORMAT = os.environ.get(
    "HASHQUERY_DATA_FRAME_WIREFORMAT", default="arrow"
)
if HASHQUERY_DATA_FRAME_WIREFORMAT not in ("arrow", "csv"):
    raise ValueError(
        f"Unsupported HASHQUERY_DATA_FRAME_WIREFORMAT: {HASHQUERY_DATA_FRAME_WIREFORMAT}"
    )


def post_run_endpoint(
    model: "Model",
    *,
    sql_only: bool = False,
    freshness: Optional[Union[datetime, Literal["latest"]]] = None,
    print_warnings: bool = True,
    print_exec_stats: bool = False,
) -> RunResults:
    project_id = model._project_id
    api = HashboardAPI.get_for_project(project_id)
    freshness_datetime = datetime.now() if freshness == "latest" else freshness
    results_json = api.post(
        "db/v2/execute-model",
        {
            "model": model.to_wire_format(),
            "projectId": project_id,
            "options": {
                "sqlOnly": sql_only,
                "freshness": (
                    freshness_datetime.isoformat() if freshness_datetime else None
                ),
                "format": HASHQUERY_DATA_FRAME_WIREFORMAT,
            },
        },
    )
    found_wire_version = results_json.get(HASHQUERY_WIRE_VERSION_KEY)
    if found_wire_version != HASHQUERY_WIRE_VERSION:
        raise WireFormatVersionError(
            expected=HASHQUERY_WIRE_VERSION,
            found=found_wire_version,
        )
    return RunResults(
        results_json,
        print_warnings=print_warnings,
        print_exec_stats=print_exec_stats,
    )
