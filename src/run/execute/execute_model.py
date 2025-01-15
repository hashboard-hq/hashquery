import traceback
from datetime import datetime
from typing import *

import pandas as pd
import sqlalchemy as sa

from ...utils.arrow import df_to_arrow_ipc
from ..compile.context import ExecutionErrorHandler, QueryContext
from ..compile.query_layer import QueryLayer
from ..compile.settings import CompileSettings
from ..compile.source.compile_source import compile_source
from ..compile.utils.error import UserCompilationError
from ..compile.utils.silence import silenced_internal_warnings
from ..db.engine import ConnectionEngine
from .execute_result import (
    ExecuteModelCompileResult,
    ExecuteModelDataResult,
    ExecuteModelResult,
    ExecuteModelResultMetadata,
)
from .post_process import post_process_df
from .query_text import get_query_text

if TYPE_CHECKING:
    from ...model import Model


def execute_model(
    model: "Model",
    *,
    sql_only: bool,
    freshness: Optional[Union[datetime, Literal["latest"]]],
    compile_settings: Optional[CompileSettings] = None,
) -> Tuple[ExecuteModelResult, ExecuteModelResultMetadata]:
    engine = ConnectionEngine.create(model._connection)
    compile_result, query_layer, query_context = _compile_query(
        model, engine, compile_settings or CompileSettings()
    )
    if sql_only or not compile_result.ok:
        return (
            ExecuteModelResult(
                compile=compile_result,
                data=None,
                freshness=freshness,
                expiration=None,
            ),
            ExecuteModelResultMetadata(from_cache=False),
        )

    data_result = _execute_query(
        query_layer.query,
        engine,
        query_context.execution_error_handlers,
    )
    final_result = ExecuteModelResult(
        compile=compile_result,
        data=data_result,
        freshness=freshness,
        expiration=None,
    )
    return final_result, ExecuteModelResultMetadata(from_cache=False)


def _compile_query(
    model: "Model",
    engine: ConnectionEngine,
    compile_settings: CompileSettings,
) -> Tuple[ExecuteModelCompileResult, Optional[QueryLayer], Optional[QueryContext]]:
    try:
        with silenced_internal_warnings():
            ctx = QueryContext(engine=engine, settings=compile_settings)
            query_layer = compile_source(model._source, ctx).finalized()
            query_text = get_query_text(query_layer.query, engine)
        return (
            ExecuteModelCompileResult(
                query_text=query_text,
                warnings=query_layer.ctx.warnings,
            ),
            query_layer,
            ctx,
        )
    except UserCompilationError as err:
        return ExecuteModelCompileResult.failure(str(err)), None, None
    except Exception as err:
        return (
            ExecuteModelCompileResult.failure(
                f"""
Internal error during SQL compilation.
This as a bug with Hashquery which you should report.
---
{traceback.format_exc()}
                """
            ),
            None,
            None,
        )


def _execute_query(
    query: sa.sql.Select,
    engine: ConnectionEngine,
    execution_error_handlers: List[ExecutionErrorHandler],
):
    execution_start = datetime.now()
    try:
        with engine.sa_engine.connect() as conn:
            df = pd.read_sql(query, conn)
    except Exception as err:
        for handler in execution_error_handlers:
            if res := handler(err):
                return ExecuteModelDataResult.failure(res)
        return ExecuteModelDataResult.failure(str(err))

    duration_ms = int((datetime.now() - execution_start).total_seconds() * 1000)

    try:
        df, warnings = post_process_df(df)
        arrow_file_ipc = df_to_arrow_ipc(df)
        return ExecuteModelDataResult(
            arrow_file_ipc=arrow_file_ipc,
            duration_ms=duration_ms,
            warnings=warnings,
        )
    except Exception as err:
        return ExecuteModelDataResult.failure(
            f"Query successfully ran, but an unknown error occurred while processing results: {err}",
            duration_ms,
        )
