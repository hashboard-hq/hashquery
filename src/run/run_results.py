import base64
from io import StringIO
from typing import Dict, List, Literal, Union

import pandas as pd
import pyarrow as pa


class RunResults:
    """
    Represents the records and metadata from an invocation of `run`.
    """

    def __init__(
        self,
        result_json: dict,
        *,
        print_warnings: bool,
        print_exec_stats: bool,
    ) -> None:
        self._result_json = result_json
        self._have_printed_compile_warnings = not print_warnings
        self._have_printed_execution_stats = not print_exec_stats

    @property
    def sql_query(self) -> str:
        """
        The SQL query that was executed.
        """
        compile_result = self._validated_result("compile")
        return compile_result["sqlQuery"]

    @property
    def df(self) -> pd.DataFrame:
        """
        The result records as a pandas DataFrame.
        """
        data_result = self._validated_result("data")
        if "arrow" in data_result:
            arrow_ipc = base64.b64decode(data_result["arrow"])
            with pa.ipc.open_file(arrow_ipc) as reader:
                table = reader.read_all()
                return table.to_pandas()

        elif "csv" in data_result:
            return pd.read_csv(StringIO(data_result["csv"]), parse_dates=True)

        else:
            raise RunResultsError(
                phase="data", msg="Could not find a result set in the server response."
            )

    @property
    def py_records(self) -> List[Dict]:
        """
        The result records as a Python list of dictionaries.
        """
        return self.df.to_dict(orient="records")

    def __len__(self):
        return self.df.num_rows

    def _validated_result(self, key: Union[Literal["compile"], Literal["data"]]):
        self._print_compile_warnings_if_needed()
        compile_result = self._get_ok_result("compile")
        if key == "compile":
            return compile_result
        if key == "data":
            self._print_execution_stats_if_needed()
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

    def _print_compile_warnings_if_needed(self):
        if self._have_printed_compile_warnings:
            return
        compile_warnings = self._result_json.get("compile", {}).get("warnings") or []
        for warning in compile_warnings:
            print("WARN: " + warning)
        self._have_printed_compile_warnings = True

    def _print_execution_stats_if_needed(self):
        if self._have_printed_execution_stats:
            return
        data_info = self._result_json.get("data", {})
        info_str = "CACHED" if self._result_json.get("cached") else "QUERY"
        if duration_ms := data_info.get("durationMs"):
            info_str += f" [duration: {round(duration_ms / 1000, 2)}s]"
        if freshness := self._result_json.get("freshness"):
            info_str += f" [freshness: {freshness}]"
        print(info_str)
        self._have_printed_execution_stats = True


class RunResultsError(Exception):
    """
    Indicates a problem occurred when executing a Model with `run` or
    `compile_sql`.

    The `.phase` property will indicate whether this error occurred during
    SQL compilation or data fetching.
    """

    def __init__(
        self, msg: str, phase: Union[Literal["compile"], Literal["data"]]
    ) -> None:
        super().__init__(msg)
        self.phase = phase
