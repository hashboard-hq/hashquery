import base64
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional

import pyarrow as pa
from dataclasses_json import LetterCase, dataclass_json

from ...utils.arrow import arrow_ipc_to_df

"""
Note that `ExecuteModelResult` are designed to be serialized and saved to cache,
which means making breaking changes to their format could result in corrupting
or churning long-lived caches.
"""


@dataclass_json(letter_case=LetterCase.CAMEL)
@dataclass
class ExecuteModelResult:
    compile: "ExecuteModelCompileResult"
    data: Optional["ExecuteModelDataResult"]

    # TODO: later might be worth it to separately track freshness / cache info if
    # the compile step is running reflections. For now, compile step is cheap and
    # there's no value in tracking the steps separately.
    freshness: datetime
    expiration: Optional[datetime]


@dataclass_json(letter_case=LetterCase.CAMEL)
@dataclass
class ExecuteModelCompileResult:
    query_text: str

    # status
    ok: bool = True
    warnings: List[str] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)

    @classmethod
    def failure(cls, error_msg: str):
        return ExecuteModelCompileResult(
            ok=False,
            query_text=None,
            warnings=[],
            errors=[error_msg],
        )


@dataclass_json(letter_case=LetterCase.CAMEL)
@dataclass
class ExecuteModelDataResult:
    # Data returned from the query, an arrow table encoded as base64 utf-8
    # We don't store a `pd.DataFrame` directly since we want something easily
    # and consistently serializable, so this object can easily be put in caches.
    arrow_file_ipc: str

    # metadata
    duration_ms: int

    # status
    ok: bool = True
    warnings: List[str] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)

    @classmethod
    def failure(cls, error_msg: str, duration_ms: int = 0):
        return ExecuteModelDataResult(
            ok=False,
            arrow_file_ipc=None,
            duration_ms=duration_ms,
            errors=[error_msg],
        )

    @property
    def pd_dataframe(self):
        return arrow_ipc_to_df(self.arrow_file_ipc)


@dataclass
class ExecuteModelResultMetadata:
    from_cache: bool
