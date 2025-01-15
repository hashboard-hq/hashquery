import re
from typing import *

from sqlglot import ErrorLevel, Expression, exp
from sqlglot import parse_one as sqlglot_parse_one

from ...db.dialect import SqlDialect, to_sqlglot_dialect
from .error import UserCompilationError


def sql_parse_capturing_error(
    sql_text: str,
    dialect: SqlDialect,
) -> Tuple[Expression, Optional[Exception]]:
    """
    Attempt to parse the provided sql snippet into an AST.
    """
    parse_params = {"sql": sql_text, "read": to_sqlglot_dialect(dialect)}
    try:
        result = sqlglot_parse_one(**parse_params, error_level=ErrorLevel.RAISE)
        return result, None
    except Exception as e:
        error = e
    try:
        result = sqlglot_parse_one(**parse_params, error_level=ErrorLevel.IGNORE)
        return result, error
    except Exception:
        raise UserCompilationError(
            "Could not parse supplied SQL fragment:\n" + sql_text
        )


def sql_unparse_to_text(ast: Expression, dialect: SqlDialect) -> str:
    """
    Format the given query AST as text.
    """
    return ast.sql(
        dialect=to_sqlglot_dialect(dialect),
        normalize_functions=False,
    )


# Regex pattern for parsing aliases in custom SQL.
# This is also defined with the same name in the Hashboard client app and must match
CUSTOM_SQL_ALIAS_PATTERN = re.compile(r"{{ *([a-zA-Z0-9_]+) *}}")

# We do cycle detection, but this is used a backup to prevent infinite loops
# (in case our cycle detection is broken)
MAX_RESOLUTIONS = 10_000


def resolve_hashboard_aliases_in_custom_sql(
    custom_sql: str,
    alias_to_sql_func: Callable[[str], str],
    pattern=CUSTOM_SQL_ALIAS_PATTERN,  # group 1 must be the alias
) -> str:
    """For a given custom SQL string, returns the same string with all glean aliases replaced based on `alias_to_sql_func`.
    Note this will continue replacing aliases until no more are found, e.g. it can handle layered dependencies.

    Alias format is based on the CUSTOM_SQL_ALIAS_PATTERN regex above. An example of a custom SQL string would be:

        {{ double_row_count_metric_alias }} / COUNT(DISTINCT username)

    Where the expected output would then be something like:

        (2 * COUNT(*)) / COUNT(DISTINCT username)

    Args:
        custom_sql (str): A SQL expression as a string
        alias_to_sql_func (Callable[[str], str]): Takes an alias in, and returns a string that will replace the alias.
    Returns:
        str: The same SQL expression with templated values replaced
    """

    seen = set()
    num_total_resolutions = 0

    def _resolve(cur: str):
        nonlocal num_total_resolutions

        def _alias_to_sql(alias):
            if alias in seen:
                raise UserCompilationError(
                    f"Found cycle when referencing alias: {alias}"
                )
            seen.add(alias)
            substituted = alias_to_sql_func(alias)
            res = _resolve(substituted)
            seen.remove(alias)
            return res

        while pattern.search(cur):
            num_total_resolutions += 1
            if num_total_resolutions > MAX_RESOLUTIONS:
                raise UserCompilationError(
                    f"Could not parse SQL because we resolved more than {MAX_RESOLUTIONS} aliases: {custom_sql}"
                )
            cur = pattern.sub(lambda match: _alias_to_sql(match.group(1)), cur)
        return cur

    return _resolve(custom_sql)
