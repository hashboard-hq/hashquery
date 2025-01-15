from typing import Any, Dict, List, Union

import sqlalchemy as sa

from .....model.column_expression import FormatTimestampColumnExpression
from ....db.dialect import SqlDialect
from ...utils.error import UserCompilationError
from ..compile_column_expression import QueryLayer, compile_column_expression
from ..compiler_registry import (
    CompiledColumnExpression,
    register_column_expression_compiler,
)


def compile_format_timestamp_column_expression(
    expr: FormatTimestampColumnExpression,
    layer: QueryLayer,
) -> CompiledColumnExpression:
    base = compile_column_expression(expr.base, layer)
    formatter = _dialect_formatter(layer.ctx.dialect)
    return formatter.compile(base, expr.format)


def _dialect_formatter(dialect):
    return {
        SqlDialect.ATHENA: Athena_DTF,
        SqlDialect.BIGQUERY: BigQuery_DTF,
        SqlDialect.CLICKHOUSE: Clickhouse_DTF,
        SqlDialect.DATABRICKS: Databricks_DTF,
        SqlDialect.DUCKDB: DuckDB_DTF,
        SqlDialect.MYSQL: MySQL_DTF,
        SqlDialect.POSTGRES: Postgres_DTF,
        SqlDialect.REDSHIFT: Redshift_DTF,
        SqlDialect.SNOWFLAKE: Snowflake_DTF,
    }[dialect]()


class DateTimeFormatter:
    """
    A base class responsible for tokenizing a format string and then compiling
    it for each dialect. At a high level, it attempts to offer as simple an
    interface for dialects to correctly implement the format specifies as
    possible.

    Most dialects just need to map symbols in one pattern language (Python's)
    to their own pattern language. To do so, they implement the `get_token_map`
    function and map Py tokens to their tokens, or to compiled column
    expressions which calculates the value when no such token exists.

    This is more complex than a simple find+replace, and instead requires
    _parsing_ since we need to be able to identify the difference between
    literals (which need to be escaped in some dialects). We also need to break
    apart the format string into parts handled by patterns, and parts handled
    by a sub-expression we then concatenate in.
    """

    def compile(
        self, column: CompiledColumnExpression, format_str: str
    ) -> CompiledColumnExpression:
        """
        Compiles an expression to an output format matching the provided
        format string.
        """
        token_map = self.get_token_map(column)
        self._check_unsupported_tokens(format_str, token_map)
        tokens = self._tokenize_format(format_str, token_map)
        tokens = self._combine_patterns(tokens)
        formatted_parts = self._compile_dialect_tokens(tokens, column)
        result = self.concat_strings(formatted_parts)
        return result

    def apply_format_func(self, column: CompiledColumnExpression, format_str: str):
        """
        Apply a transform to the column which converts it to a string according
        to the format string. The format str is _already_ translated.
        """
        return sa.func.strftime(column, format_str)

    PY_FORMAT_TOKENS = [
        "%a",  #  Weekday as locale’s abbreviated name.
        "%A",  #  Weekday as locale’s full name.
        "%w",  #  Weekday as a decimal number, where 0 is Sunday and 6 is Saturday.
        "%d",  #  Day of the month as a zero-padded decimal number.
        "%-d",  # Day of the month as a decimal number.
        "%b",  #  Month as locale’s abbreviated name.
        "%B",  #  Month as locale’s full name.
        "%m",  #  Month as a zero-padded decimal number.
        "%-m",  # Month as a decimal number.
        "%y",  #  Year without century as a zero-padded decimal number.
        "%Y",  #  Year with century as a decimal number.
        "%H",  #  Hour (24-hour clock) as a zero-padded decimal number.
        "%-H",  # Hour (24-hour clock) as a decimal number.
        "%I",  #  Hour (12-hour clock) as a zero-padded decimal number.
        "%-I",  # Hour (12-hour clock) as a decimal number.
        "%p",  #  Locale’s equivalent of either AM or PM.
        "%M",  #  Minute as a zero-padded decimal number.
        "%-M",  # Minute as a decimal number.
        "%S",  #  Second as a zero-padded decimal number.
        "%-S",  # Second as a decimal number.
        "%f",  #  Microsecond as a decimal number, zero-padded to 6 digits.
        "%z",  #  UTC offset in the form ±HHMM[SS[.ffffff]] (empty string if the object is naive).
        "%Z",  #  Time zone name (empty string if the object is naive).
        "%j",  #  Day of the year as a zero-padded decimal number.
        "%-j",  # Day of the year as a decimal number.
        "%U",  #  Week number of the year (Sunday as the first day of the week) as a zero-padded decimal number.
        "%-U",  # Week number of the year (Sunday as the first day of the week) as a decimal number.
        "%W",  #  Week number of the year (Monday as the first day of the week) as a zero-padded decimal number.
        "%-W",  # Week number of the year (Monday as the first day of the week) as a decimal number.
        "%%",  #  A literal '%' character.
        "%Q",  # ~~~ NON-STANDARD. Quarter number (1-4). ~~~
    ]

    def get_token_map(
        self, column: CompiledColumnExpression
    ) -> Dict[str, Union[str, Union[str, CompiledColumnExpression]]]:
        """
        Map from Python format tokens to equivalent database format tokens
        or column expressions to satisfy that token.

        The default implementation marks every token as unimplemented.
        """
        return {t: None for t in self.PY_FORMAT_TOKENS}

    def escape_literal_substring_in_format_pattern(self, format_substring: str) -> str:
        """
        Some format specifier languages need escaping applied to substrings in
        the format pattern which are not to be treated as patterns.

        For example, in Postgres, the pattern 'Year YYYY' would turn into
        '4ear 2024', since 'Y' is a valid format token. Instead, you would
        escape the literal 'Year' text by passing '"Year "YYYY' to get
        `Year 2024`. This function allows a subclass to declare escaping logic.
        """
        return format_substring

    def concat_strings(self, columns: List[CompiledColumnExpression]):
        """
        Concatenate all the provided column expressions into a single string.
        By default, this just uses the SQLAlchemy `+` operator.
        """
        result = columns[0]
        for col in columns[1:]:
            result = result + col
        return result

    def _check_unsupported_tokens(self, format_str: str, token_map: dict):
        unsupported_tokens = [
            *[py_t for py_t, none_mapping in token_map.items() if none_mapping is None],
            # locale-specific tokens which we always prevent
            "%c",
            "%x",
            "%X",
        ]
        for unsupported_token in unsupported_tokens:
            if unsupported_token in format_str:
                raise UserCompilationError(
                    f"The '{unsupported_token}' format token is not supported in this dialect."
                )

    def _tokenize_format(self, format_str: str, token_map: Dict[str, Any]):
        """
        Tokenize the format string into one of three types:
        `literal`: substrings that don't contain any format specifiers
        `pattern`: a single format token, such as `%M` or `%Q`
        `expression`: a compiled SQLAlchemy expression
        """
        make_token = lambda type, value: {"type": type, "value": value}
        curr_tokens = [make_token("literal", format_str)]
        for py_token, mapped_token_value in token_map.items():
            next_tokens = []
            for token in curr_tokens:
                token_type, token_value = token["type"], token["value"]
                if token_type != "literal" or py_token not in token_value:
                    next_tokens.append(token)
                    continue
                mapped_token = (
                    mapped_token_value
                    if type(mapped_token_value) is dict
                    else make_token(
                        "pattern" if type(mapped_token_value) is str else "expression",
                        mapped_token_value,
                    )
                )
                # split this token, inserting the mapped token into the gaps
                literal_split: List[str] = token_value.split(py_token)
                for literal_segment in literal_split[:-1]:
                    next_tokens.append(make_token("literal", literal_segment))
                    next_tokens.append(mapped_token)
                next_tokens.append(make_token("literal", literal_split[-1]))

            curr_tokens = next_tokens
        return curr_tokens

    def _combine_patterns(self, tokens: List[Dict]):
        """
        Combine tokens to form dialect-specific patterns,
        translating pattern tokens and escaping literals as needed.

        For example, the following tokens:
            [pattern("%B"), literal(", "), pattern("%Y")]
        would be merged into:
            [pattern("%B, %Y")]
        """
        result: List[Dict] = []
        curr_token = None
        for next_token in tokens:
            if next_token["type"] == "expression":
                # flush the current token, and add the expression directly
                # since we don't combine them
                if curr_token:
                    result.append(curr_token)
                    curr_token = None
                result.append(next_token)
                continue

            next_token_value = next_token["value"]
            if not next_token_value:
                continue  # ignore empty patterns and literals
            if not curr_token:  # start a new token
                curr_token = next_token
                continue
            if next_token["type"] == "literal":
                if curr_token["type"] == "pattern":
                    # when combining into a pattern, escape the value
                    next_token_value = self.escape_literal_substring_in_format_pattern(
                        next_token_value
                    )
                curr_token["value"] += next_token_value
            elif next_token["type"] == "pattern":
                if curr_token["type"] == "literal":
                    # promote a literal into a pattern, escaping the current value
                    curr_token = {
                        "type": "pattern",
                        "value": self.escape_literal_substring_in_format_pattern(
                            curr_token["value"]
                        ),
                    }
                curr_token["value"] += next_token_value

        # flush any remaining token
        if curr_token:
            result.append(curr_token)

        return result

    def _compile_dialect_tokens(
        self,
        tokens: List[Dict],
        column: CompiledColumnExpression,
    ):
        """
        Compiles dialect tokens into column expressions.
        """
        return [self._compile_dialect_token(token, column) for token in tokens]

    def _compile_dialect_token(
        self,
        token: Dict,
        column: CompiledColumnExpression,
    ):
        """
        Compiles a single dialect tokens into a column expression.
        """
        token_type, token_value = token["type"], token["value"]
        if token_type == "literal":
            return sa.literal(token_value, type_=sa.String)
        elif token_type == "pattern":
            return self.apply_format_func(column, token_value)
        elif token_type == "expression":
            return token_value
        else:
            raise AssertionError(f"Unknown token type: {token_type}")


class DuckDB_DTF(DateTimeFormatter):
    # https://duckdb.org/docs/sql/functions/dateformat.html

    def apply_format_func(self, column: CompiledColumnExpression, format_str: str):
        return sa.func.strftime(column, format_str)

    def get_token_map(
        self, column: CompiledColumnExpression
    ) -> Dict[str, Union[str, CompiledColumnExpression]]:
        return {
            # DuckDB supports most of the Python format tokens directly
            **{t: t for t in super().get_token_map(column)},
            # manually implement quarter extraction
            "%Q": sa.func.quarter(column),
            # no support for un-padded week numbers
            "%-U": None,
            "%-W": None,
        }


class BigQuery_DTF(DateTimeFormatter):
    # https://cloud.google.com/bigquery/docs/reference/standard-sql/format-elements

    def apply_format_func(self, column: CompiledColumnExpression, format_str: str):
        subtype_to_formatter = {
            sa.DATE: sa.func.FORMAT_DATE,
            sa.DATETIME: sa.func.FORMAT_DATETIME,
            sa.TIMESTAMP: sa.func.FORMAT_TIMESTAMP,
        }
        subtype_format_func = subtype_to_formatter.get(type(column.type))
        if not subtype_format_func:
            raise TypeError(
                f"Cannot determine a date format function to use for data type: {column.type}"
            )
        return subtype_format_func(format_str, column)

    def get_token_map(
        self, column: CompiledColumnExpression
    ) -> Dict[str, Union[str, CompiledColumnExpression]]:
        return {
            # BigQuery supports most of the Python format tokens directly
            **{t: t for t in super().get_token_map(column)},
            # microseconds are not included in BQ, coerce them as zero
            "%f": "000000",
            # some un-padded numbers get a different token
            "%-d": "%e",
            "%-I": "%l",
            # lacks support for some rarely used un-padded numbers
            "%-m": None,
            "%-H": None,
            "%-M": None,
            "%-S": None,
            "%-j": None,
            "%-U": None,
            "%-W": None,
        }


class MySQL_DTF(DateTimeFormatter):
    # https://dev.mysql.com/doc/refman/8.0/en/date-and-time-functions.html#function_date-format

    def apply_format_func(self, column: CompiledColumnExpression, format_str: str):
        return sa.func.DATE_FORMAT(column, format_str)

    def get_token_map(
        self, column: CompiledColumnExpression
    ) -> Dict[str, Union[str, CompiledColumnExpression]]:
        return {
            # MySQL supports many of the Python format tokens directly
            **{t: t for t in super().get_token_map(column)},
            # manually implement quarter
            "%Q": sa.func.QUARTER(column),
            # MySQL doesn't store TZ information, all values are stored in UTC
            # and then converted on retrieval to whatever the DB's timezone is.
            "%z": "",
            # some things get a different token
            "%A": "%W",
            "%B": "%M",
            "%-m": "%c",
            "%-d": "%e",
            "%I": "%h",
            "%M": "%i",
            "%S": "%s",
            "%W": "%u",
            "%-H": "%k",
            "%-I": "%l",
            # lacks support for some rarely used un-padded numbers
            "%-M": None,
            "%-S": None,
            "%-j": None,
            "%-U": None,
            "%-W": None,
        }


class Athena_DTF(MySQL_DTF):
    # Athena is based on Trino:
    # https://trino.io/docs/current/functions/datetime.html#date_format
    #
    # There are two APIs, one which uses a MySQL-like syntax, and one which
    # uses a JodaTime format. The MySQL one requires less overall translation,
    # so we favor that one.

    def apply_format_func(self, column: CompiledColumnExpression, format_str: str):
        return sa.func.date_format(column, format_str)

    def get_token_map(
        self, column: CompiledColumnExpression
    ) -> Dict[str, Union[str, CompiledColumnExpression]]:
        return {
            # inherit the MySQL implementation
            **super().get_token_map(column),
            # manually implement quarter
            "%Q": sa.func.cast(sa.func.quarter(column), sa.String),
            # their docs explicitly state no support for the following:
            "%D": None,
            "%U": None,
            "%u": None,
            "%V": None,
            "%w": None,
            "%X": None,
        }


class Postgres_DTF(DateTimeFormatter):
    # https://www.postgresql.org/docs/current/functions-formatting.html

    def apply_format_func(self, column: CompiledColumnExpression, format_str: str):
        return sa.func.to_char(column, format_str)

    def escape_literal_substring_in_format_pattern(self, substring):
        return f'"{substring}"'

    def get_token_map(
        self, column: CompiledColumnExpression
    ) -> Dict[str, Union[str, CompiledColumnExpression]]:
        return {
            **super().get_token_map(column),
            # Postgres has all its own tokens,
            "%a": "TMDy",
            "%A": "TMDay",
            "%w": None,  # TODO: implement this token, PG has `D` but its indexed differently
            "%d": "DD",
            "%-d": "FMDD",
            "%b": "TMMon",
            "%B": "TMMonth",
            "%m": "MM",
            "%-m": "FMMM",
            "%y": "YY",
            "%Y": "YYYY",
            "%H": "HH24",
            "%-H": "FMHH24",
            "%I": "HH12",
            "%-I": "FMHH12",
            "%p": "AM",  # the template "AM" is used for both "AM/PM"
            "%M": "MI",
            "%-M": "FMMI",
            "%S": "SS",
            "%-S": "FMSS",
            "%f": "US",
            "%z": "OF",
            "%Z": "TZ",
            "%j": "DDD",
            "%-j": "FMDDD",
            "%U": None,
            "%-U": None,
            "%W": "IW",
            "%-W": "FMIW",
            "%Q": "Q",
            # implement the `%%` literal as a string literal token of `%`,
            # since Postgres assigns no meaning to `%`.
            "%%": {"type": "literal", "value": "%"},
        }


class Redshift_DTF(Postgres_DTF):
    def get_token_map(
        self, column: CompiledColumnExpression
    ) -> Dict[str, Union[str, CompiledColumnExpression]]:
        return {
            # Inherit the tokens from Postgres
            **super().get_token_map(column),
            # Redshift doesn't support the `TM` prefix
            # and requires the `FM` prefix for Day and Month,
            # else it adds some wacky padding
            "%a": "Dy",
            "%A": "FMDay",
            "%b": "Mon",
            "%B": "FMMonth",
        }


class Snowflake_DTF(DateTimeFormatter):
    # https://docs.snowflake.com/en/sql-reference/functions/to_char

    def apply_format_func(self, column: CompiledColumnExpression, format_str: str):
        return sa.func.TO_CHAR(column, format_str)

    def escape_literal_substring_in_format_pattern(self, substring):
        return f'"{substring}"'

    def concat_strings(self, columns: List[CompiledColumnExpression]):
        return sa.func.concat(*columns)

    def get_token_map(
        self, column: CompiledColumnExpression
    ) -> Dict[str, Union[str, CompiledColumnExpression]]:
        return {
            **super().get_token_map(column),
            # Snowflakes format language is spare and requires lots of expressions
            "%a": "DY",
            "%A": self._full_weekday(column),
            "%w": sa.func.DAYOFWEEK(column),
            "%d": "DD",
            "%-d": sa.func.DAYOFMONTH(column),
            "%b": "MON",
            "%B": "MMMM",
            "%m": "MM",
            "%-m": sa.func.MONTH(column),
            "%y": "YY",
            "%Y": "YYYY",
            "%H": "HH24",
            "%-H": sa.func.HOUR(column),
            "%I": "HH12",
            "%-I": "HH12",  # FIXME: this is padded
            "%p": "AM",  # the template "AM" is used for both "AM/PM"
            "%M": "MI",
            "%-M": sa.func.MINUTE(column),
            "%S": "SS",
            "%-S": sa.func.SECOND(column),
            "%f": "FF6",
            "%z": "+TZH:TZM",
            "%Z": "+TZH:TZM",  # no support for TZ names, redirect to the offset
            "%j": sa.func.DAYOFYEAR(column),  # FIXME: this is not padded
            "%-j": sa.func.DAYOFYEAR(column),
            "%U": sa.func.WEEK(column),
            "%-U": sa.func.WEEK(column),
            "%W": sa.func.WEEKISO(column),
            "%-W": sa.func.WEEKISO(column),
            "%%": {"type": "literal", "value": "%"},
            "%Q": sa.func.QUARTER(column),
        }

    def _full_weekday(self, column):
        # adapted from
        # https://docs.snowflake.com/sql-reference/date-time-examples
        return sa.func.DECODE(
            sa.func.DAYOFWEEK(column),
            1,
            "Monday",
            2,
            "Tuesday",
            3,
            "Wednesday",
            4,
            "Thursday",
            5,
            "Friday",
            6,
            "Saturday",
            7,
            "Sunday",
        )


class JodaTimeFormat_DTF(DateTimeFormatter):
    # Abstract base for formats which use JodaTime format strings
    # https://joda-time.sourceforge.net/apidocs/org/joda/time/format/DateTimeFormat.html

    def escape_literal_substring_in_format_pattern(self, substring: str):
        return f"'{substring}'"

    def get_token_map(
        self, column: CompiledColumnExpression
    ) -> Dict[str, Union[str, CompiledColumnExpression]]:
        return {
            "%a": "EE",
            "%A": "EEEE",
            "%w": "",
            "%d": "dd",
            "%-d": "d",
            "%b": "MMM",
            "%B": "MMMM",
            "%m": "MM",
            "%-m": "M",
            "%y": "yy",
            "%Y": "yyyy",
            "%H": "HH",
            "%-H": "H",
            "%I": "hh",
            "%-I": "h",
            "%p": "a",
            "%M": "mm",
            "%-M": "m",
            "%S": "ss",
            "%-S": "s",
            "%f": "SSSSSS",
            "%z": "ZZ",
            "%Z": "z",
            "%j": "DDDD",
            "%-j": "DDD",
            "%U": None,
            "%-U": None,
            "%W": "ww",
            "%-W": "w",
            "%%": {"type": "literal", "value": "%"},
            "%Q": "Q",
        }


class Clickhouse_DTF(JodaTimeFormat_DTF):
    # https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions#formatdatetime
    #
    # There are two APIs, one which uses a MySQL-like syntax, and one which
    # uses a JodaTime format. The MySQL one is absolutely broken as of writing,
    # and seems to insert corrupted tokens into the output tokens... very odd.
    # So we use the Joda version.

    def apply_format_func(self, column: CompiledColumnExpression, format_str: str):
        return sa.func.formatDateTimeInJodaSyntax(column, format_str)

    def get_token_map(
        self, column: CompiledColumnExpression
    ) -> Dict[str, Union[str, CompiledColumnExpression]]:
        return {
            # Clickhouse uses Joda time
            **super().get_token_map(column),
            # quarter needs manual implementation
            "%Q": sa.func.toQuarter(column),
            # but not TZ values, which their docs say aren't supported yet
            # which we'll just hide
            "%z": "",
            "%Z": "",
            # best effort ISO week numbers
            "%W": sa.func.toISOWeek(column),
            "%-W": sa.func.toISOWeek(column),
        }


class Databricks_DTF(JodaTimeFormat_DTF):
    # https://docs.databricks.com/en/sql/language-manual/sql-ref-datetime-pattern.html

    def apply_format_func(self, column: CompiledColumnExpression, format_str: str):
        return sa.func.date_format(column, format_str)

    def get_token_map(
        self, column: CompiledColumnExpression
    ) -> Dict[str, Union[str, CompiledColumnExpression]]:
        joda_map = super().get_token_map(column)
        return {
            # Databricks uses Joda time
            **joda_map,
            # but not TZ names, redirect to offset
            "%Z": joda_map["%z"],
        }


register_column_expression_compiler(
    FormatTimestampColumnExpression,
    compile_format_timestamp_column_expression,
)
