import base64
from abc import ABC
from io import StringIO
from typing import *

import pandas as pd
import pyarrow as pa

from ..utils.arrow import arrow_ipc_to_df
from .execute.execute_result import ExecuteModelResult, ExecuteModelResultMetadata


class RunResults(ABC):
    """
    Represents the records and metadata from an invocation of `run`.
    """

    def __init__(
        self,
        *,
        print_warnings: bool = True,
        print_exec_stats: bool = False,
    ):
        super().__init__()
        self._has_printed_compile_warnings = not print_warnings
        self._has_printed_exec_warnings = not print_warnings
        self._has_printed_exec_stats = not print_exec_stats
        self._cached_loaded_df: Optional[pd.DataFrame] = None

    # --- Public API ---

    @property
    def sql_query(self) -> str:
        """
        The SQL query that was executed.
        """
        self._show_compile_info_once()
        return self._load_sql_query()

    @property
    def df(self) -> pd.DataFrame:
        """
        The result records as a pandas DataFrame.
        """
        self.sql_query  # ensure compilation was all set
        self._show_exec_info_once()
        # load the DataFrame and cache it
        self._cached_loaded_df = self._load_df()
        return self._cached_loaded_df

    @property
    def py_records(self) -> List[Dict]:
        """
        The result records as a Python list of dictionaries.
        """
        return self.df.to_dict(orient="records")

    def __len__(self):
        return self.df.num_rows

    # --- Subclass Requirements ---

    def _load_sql_query(self) -> str:
        ...

    def _load_df(self) -> pd.DataFrame:
        ...

    def _load_compile_warnings(self) -> List[str]:
        ...

    def _load_exec_warnings(self) -> List[str]:
        ...

    def _load_exec_stats(self) -> Dict[str, str]:
        ...

    # --- Internals ---

    def _show_compile_info_once(self):
        if self._has_printed_compile_warnings:
            return
        compile_warnings = self._load_compile_warnings()
        for warning in compile_warnings:
            print("WARN: " + warning)
        self._has_printed_compile_warnings = True

    def _show_exec_info_once(self):
        if not self._has_printed_exec_warnings:
            exec_warnings = self._load_exec_warnings()
            for warning in exec_warnings:
                print("WARN: " + warning)
            self._has_printed_exec_warnings = True

        if not self._has_printed_exec_stats:
            stats = self._load_exec_stats()
            info_str = "CACHED" if stats.get("cached") else "QUERY"
            for stat_key, stat_val in stats.items():
                info_str += f"[{stat_key}: {stat_val}]"
            print(info_str)
            self._has_printed_exec_stats = True


class RunResultsError(Exception):
    """
    Indicates a problem occurred when executing a Model with `run` or
    `compile_sql`.

    The `.phase` property will indicate whether this error occurred during
    SQL compilation or data fetching.
    """

    def __init__(self, msg: str, phase: Literal["compile", "data"]) -> None:
        super().__init__(msg)
        self.phase = phase


# ---


class LocalRunResults(RunResults):
    """
    A RunResult that is loaded in memory from a pair of `ExecuteModelResult`
    and `ExecuteModelResultMetadata` instances.
    """

    def __init__(
        self,
        result: ExecuteModelResult,
        result_meta: ExecuteModelResultMetadata,
        *,
        print_warnings: bool = True,
        print_exec_stats: bool = False,
    ):
        super().__init__(
            print_warnings=print_warnings,
            print_exec_stats=print_exec_stats,
        )
        self._result = result
        self._result_meta = result_meta

    def _load_sql_query(self) -> str:
        if not self._result.compile.ok:
            raise RunResultsError(
                phase="compile",
                msg="\n".join(self._result.compile.errors),
            )
        return self._result.compile.query_text

    def _load_df(self) -> pd.DataFrame:
        if not self._result.data.ok:
            raise RunResultsError(
                phase="data",
                msg="\n".join(self._result.data.errors),
            )
        return self._result.data.pd_dataframe

    def _load_compile_warnings(self) -> List[str]:
        return self._result.compile.warnings

    def _load_exec_warnings(self) -> List[str]:
        if not self._result.data:
            return []
        return self._result.data.warnings

    def _load_exec_stats(self) -> Dict[str, str]:
        stats = {}
        if cached := self._result_meta.from_cache:
            stats["cached"] = cached
        if duration_ms := self._result.data and self._result.data.duration_ms:
            stats["duration"] = f"{round(duration_ms / 1000, 2)}s"
        if freshness := self._result.freshness:
            stats["freshness"] = f" [freshness: {freshness}]"
        return stats


# ---


class JSONRunResults(RunResults):
    """
    A RunResult that is loaded from JSON. The DataFrame may be encoded in
    multiple different ways.
    """

    def __init__(
        self,
        result_json: dict,
        *,
        print_warnings: bool = True,
        print_exec_stats: bool = False,
    ):
        super().__init__(
            print_warnings=print_warnings,
            print_exec_stats=print_exec_stats,
        )
        self._result_json = result_json

    def _load_sql_query(self) -> str:
        compile_result = self._validated_result("compile")
        return compile_result["sqlQuery"]

    def _load_df(self) -> pd.DataFrame:
        data_result = self._validated_result("data")
        if "arrow" in data_result:
            return arrow_ipc_to_df(data_result["arrow"])
        elif "csv" in data_result:
            return pd.read_csv(StringIO(data_result["csv"]), parse_dates=True)
        else:
            raise RunResultsError(
                phase="data",
                msg="Could not find a result set in the server response.",
            )

    def _load_compile_warnings(self) -> List[str]:
        return self._result_json.get("compile", {}).get("warnings") or []

    def _load_exec_warnings(self) -> List[str]:
        return self._result_json.get("data", {}).get("warnings") or []

    def _load_exec_stats(self) -> Dict[str, str]:
        stats = {}
        if cached := self._result_json.get("cached", False):
            stats["cached"] = cached
        if duration_ms := self._result_json.get("durationMs"):
            stats["duration"] = f"{round(duration_ms / 1000, 2)}s"
        if freshness := self._result_json.get("freshness"):
            stats["freshness"] = f" [freshness: {freshness}]"
        return stats

    def _validated_result(self, key: Union[Literal["compile"], Literal["data"]]):
        compile_result = self._get_ok_result("compile")
        if key == "compile":
            return compile_result
        if key == "data":
            return self._get_ok_result("data")

    def _get_ok_result(self, key: Union[Literal["compile"], Literal["data"]]):
        key_obj: dict = self._result_json.get(key, {})
        if not key_obj.get("ok", False):
            raise RunResultsError(
                phase=key,
                msg="\n".join(
                    key_obj.get(
                        "errors",
                        [
                            "The results from the server does not have "
                            + f"the expected '{key}' key."
                        ],
                    )
                ),
            )
        # result is alright for consumption
        return self._result_json[key]
