from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union

import sqlalchemy.sql as sa

from ...model.source import Source, SqlTextSource, TableNameSource
from ...utils.stable_key import stable_key_for_model_source
from ..db.engine import ConnectionEngine
from ..db.reflection import ColumnTypeInfo, ReflectionFetcher
from .settings import CompileSettings

ExecutionErrorHandler = Callable[[Exception], Optional[str]]


class QueryContext:
    """
    Bundle of properties relevant to query generation with a global
    scope for the entire query.
    """

    def __init__(
        self,
        *,
        engine: ConnectionEngine,
        settings: Optional[CompileSettings] = None,
    ) -> None:
        self.engine = engine
        self.settings = settings or CompileSettings()
        self.warnings = []
        self._reflection_fetcher = ReflectionFetcher(engine)
        self._cache: Dict[Any, Any] = {}
        self._used_ref_names: Set[str] = set()
        self._used_ref_names_any_substring_text: list[str] = []
        # Stores id(column_expr) for every column expression that has been preprocessed.
        # Used to avoid unnecessarily re-running preprocess_column_expression on subexpressions.
        self.preprocessed: Set[int] = set()

        self.execution_error_handlers: List[ExecutionErrorHandler] = []

    @property
    def name(self):
        return "main"

    @property
    def dialect(self):
        return self.engine.dialect

    def _get_free_name(
        self,
        base_name,
        index: Optional[int] = 1,
        always_include_index=False,
    ) -> str:
        proposed_name = (
            base_name
            if (index <= 1 and not always_include_index)
            else f"{base_name}_{index}"
        )
        name_is_already_used = (proposed_name in self._used_ref_names) or any(
            (proposed_name in s) for s in self._used_ref_names_any_substring_text
        )
        if not name_is_already_used:
            self._used_ref_names.add(proposed_name)
            return proposed_name
        else:
            return self._get_free_name(base_name, index + 1, always_include_index)

    def next_alias_name(self, name: str) -> str:
        # SQLAlchemy has stricter rules about how you can repeat names of
        # aliases compared to SQL-proper. You can't have two aliases with the
        # same name anywhere, even across two unrelated subqueries where there
        # would never be ambiguity. For this reason, we need to ensure there's
        # no collisions between aliases... it's a bit annoying and makes
        # the output queries a bit trickier to read, but that's life I guess.
        # https://github.com/sqlalchemy/sqlalchemy/issues/7962
        return self._get_free_name(name)

    def next_cte_name(self) -> str:
        return self._get_free_name(f"{self.name}_layer", always_include_index=True)

    def add_reserved_name(self, name: str, *, match_any_substring: bool = False):
        """
        Explicitly reserve a name such that no `next_alias_name` or
        `next_cte_name` call will ever return it. If `match_any_substring=True`
        then the matching won't be exact, but instead bar any name that appears
        as a substring of the provided text.
        """
        if match_any_substring:
            self._used_ref_names_any_substring_text.append(name)
        else:
            self._used_ref_names.add(name)

    def fork_cte_names(self, name) -> "QueryContext":
        """
        Returns a version of QueryContext where the CTE names will be
        based on the provided `name`. This is useful for when a compiler
        needs to run another chain of queries, and wants to hint at the
        difference between the two "chains" of CTEs in the SQL.
        """
        fork_name = self._get_free_name(name)
        return ForkedQueryContext(fork_name, self)

    def add_warning(self, warning: str):
        self.warnings.append(f"[{self.name}] {warning}")

    def get_column_metadata_for_source(
        self, source: Union[TableNameSource, SqlTextSource]
    ) -> Dict[str, ColumnTypeInfo]:
        return {
            c.physical_name: c
            for c in self._reflection_fetcher.get_source_columns(source)
        }

    def add_alias_checkpoint(
        self, source: Source, ref: sa.Alias, metadata: Dict[str, ColumnTypeInfo]
    ):
        """
        If we imagine a query pipeline as a DAG, we can visualize the
        relationship of, say, a JOIN, by the input tables it joins together:

            A ---> Transform -----------> JOIN ---> Output
            B ---> OtherTransform --------^

        In some other cases, we may see repeated subtrees for a dependency,
        such as in the case of a self-join:

            A ---> Transform -----------> JOIN ---> Output
            A ---> Transform -------------^

        In such a case, it would be a waste to actually invoke the transformed
        inputs of A twice, instead, we should just reference it:

            (A ---> Transform) AS TransformedA_CTE
            TransformedA_CTE -----------> JOIN ---> Output
            TransformedA_CTE -------------^

        This function effectively acts to allow this behavior. It associates
        a Source with a reuseable table reference (generally a CTE). This allows
        future queries which ask for the Source's table to just use a reference
        instead.
        """
        self._cache[stable_key_for_model_source(source)] = (ref, metadata)

    def get_alias_checkpoint(
        self, source: Source
    ) -> Optional[Tuple[sa.Alias, Dict[str, ColumnTypeInfo]]]:
        """
        If this Source already has a compiled table reference representing its
        output (generally a CTE), this returns that reference. Otherwise None.
        See `add_alias_checkpoint` for more detail.
        """
        return self._cache.get(stable_key_for_model_source(source))

    def register_exec_error_handler(self, handler: ExecutionErrorHandler):
        """Register a new execution error handler.

        Any exceptions raised during downstream query execution will be passed
        through each handler, in the reverse-order that they are registered.
        This means handlers registered later in the compilation process will
        be called first.

        If a handler returns a string, the string will be returned as the
        error message and no further handlers will be called.
        """
        self.execution_error_handlers.insert(0, handler)


class ForkedQueryContext:
    def __init__(self, name: str, parent: QueryContext) -> None:
        self.name = name
        self.parent = parent

    def next_cte_name(self) -> str:
        return self._get_free_name(f"{self.name}_layer", always_include_index=True)

    # forward everything else to the parent
    def __getattr__(self, key: str):
        if key in ["parent"]:
            raise AttributeError()
        return getattr(self.parent, key)
