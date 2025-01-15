from copy import deepcopy

import sqlalchemy.sql as sa
from sqlglot import expressions

from .....model.column_expression.sql_text import (
    SELF_TABLE_ALIAS,
    SQL_REFERENCE_SUBSTITUTION_REGEX,
    SqlTextColumnExpression,
)
from ....db.dialect import SqlDialect, to_sqlalchemy_dialect
from ....db.type_names import Deprecated_GleanTypes_TNM
from ...query_layer import QueryLayer
from ...utils.error import UserCompilationError
from ...utils.sql import (
    resolve_hashboard_aliases_in_custom_sql,
    sql_parse_capturing_error,
    sql_unparse_to_text,
)
from ..compile_column_expression import compile_column_expression
from ..compiler_registry import (
    CompiledColumnExpression,
    register_column_expression_compiler,
)
from .column_name import apply_dialect_workarounds


def compile_sql_text_column_expression(
    expr: SqlTextColumnExpression,
    layer: QueryLayer,
) -> CompiledColumnExpression:
    # the literal `*` expression is a very special snowflake :)
    if expr._is_star:
        return compile_star_expression(expr, layer)

    # perform substitution first so that SQL written with references works
    # the same downstream as if you inlined everything.
    expr = inline_sql_references(expr, layer)
    # next, replace any references to namespaces with the actual CTE or alias
    # that they should reference
    expr = resolve_column_namespaces(expr, layer)

    type_ = (
        # TODO:
        # the current only consumer of `._unstable_type` is the Hashboard
        # integration, which uses it to pass along an abstract "GleanTypes"
        # value... for now just preserving that; the API is explicitly marked
        # `unstable` so we can clean this up later
        Deprecated_GleanTypes_TNM(layer.ctx.dialect).sa_type(expr._unstable_type)
        if expr._unstable_type
        else None
    )

    # bind it to a literal column
    result = sa.literal_column(f"({expr.sql})", type_=type_)
    result = apply_dialect_workarounds(result, layer.ctx.dialect)
    return result


def compile_star_expression(
    expr: SqlTextColumnExpression,
    layer: QueryLayer,
) -> CompiledColumnExpression:
    namespace = (
        layer.namespaces.named(expr.namespace_identifier)
        if expr.namespace_identifier
        else layer.namespaces.main
    )

    # DuckDB/MotherDuck does not support syntax like:
    #   SELECT main.table.* FROM main.table;
    #
    # but instead wants:
    #   SELECT table.* FROM main.table;
    sa_expr = (
        _StarColumnIgnoringTableSchema(_selectable=namespace.ref)
        if layer.ctx.dialect == SqlDialect.DUCKDB
        else sa.column("*", is_literal=True, _selectable=namespace.ref)
    )
    # explicitly mark this column as un-label-able, which will prevent
    # SQLAlchemy from labeling a `*` expression with an anonymous label
    # if multiple `*` columns exist
    sa_expr._render_label_in_columns_clause = False
    return sa_expr


def inline_sql_references(
    expr: SqlTextColumnExpression, layer: QueryLayer
) -> SqlTextColumnExpression:
    """
    Replaces `{{ nested_expression_id }}` with the actual contents of the
    nested expression for all instances of that syntax. Requires that `expr`
    has the nested expression in its `.nested_expressions` list, which should
    be set automatically from `.bind_references_to_model`.
    """

    def resolve_sql_for_reference(identifier: str):
        nested_expression = expr.nested_expressions.get(identifier)
        if not nested_expression:
            raise UserCompilationError(
                "Reference `{{" + identifier + "}}` could not be resolved."
            )
        compiled_nested = compile_column_expression(nested_expression, layer)
        compiled_literal = compiled_nested.compile(
            dialect=to_sqlalchemy_dialect(layer.ctx.dialect),
            compile_kwargs={"literal_binds": True},
        )
        return str(compiled_literal)

    inlined_sql = resolve_hashboard_aliases_in_custom_sql(
        expr.sql, resolve_sql_for_reference, SQL_REFERENCE_SUBSTITUTION_REGEX
    )

    # copy the result with the new SQL and no nested_expressions (we inlined them)
    result = deepcopy(expr)
    result.sql = inlined_sql
    result.nested_expressions = {}
    return result


def resolve_column_namespaces(expr: SqlTextColumnExpression, layer: QueryLayer):
    """
    Search the sql for any namespaces to rewrite the reference to match to our
    chain of uniquely named CTEs. This will rewrite `SELF_TABLE_ALIAS.` to the
    correct namespace (`layer.namespaces.main` or the namespace from the
    disambiguation key of the `expr` column).
    """
    sql_ast, parse_error = sql_parse_capturing_error(expr.sql, layer.ctx.dialect)
    if parse_error:
        layer.ctx.add_warning(str(parse_error))

    for column_node in sql_ast.find_all(expressions.Column):
        if not column_node.table:
            continue  # we're only looking for disambiguated column references
        identifier_node = column_node.args["table"]
        if type(identifier_node) != expressions.Identifier:
            continue  # invalid syntax

        if layer.needs_column_disambiguation:
            namespace = _namespace_for_node(column_node, expr, layer)
            if not namespace:
                # if the expression came from `{{}}` interpolation, then this
                # reference will already be correctly qualified
                continue
            identifier_node.set("this", namespace.ref.name)
        else:
            column_node.set("table", None)

    final_sql = sql_unparse_to_text(sql_ast, layer.ctx.dialect)

    # copy the result with new SQL
    result = deepcopy(expr)
    result.sql = final_sql
    return result


def _namespace_for_node(
    column_node: expressions.Column, expr: SqlTextColumnExpression, layer: QueryLayer
):
    if column_node.table == SELF_TABLE_ALIAS:
        if expr.namespace_identifier:
            return layer.namespaces.named(expr.namespace_identifier)
        else:
            return layer.namespaces.main
    else:
        return layer.namespaces.named(column_node.table)


class _StarColumnIgnoringTableSchema(sa.expression.ColumnElement):
    """
    Helper class for providing "*" expressions that ignore the table schema.

    Specifically, this replaces an expression that would normally render as:

        SELECT main.table.* FROM main.table;

    with the following:

        SELECT table.* FROM main.table;

    Since the `FROM` still qualifies the table reference in a schema, the
    reference inside of the `SELECT` does not need it again.
    """

    def __init__(self, *, _selectable):
        self._selectable = _selectable
        self.name = "*"

    def _compiler_dispatch(self, visitor, **kw):
        dialect = visitor.dialect
        table_name = self._selectable.name
        if dialect.identifier_preparer:
            table_name = dialect.identifier_preparer.quote(table_name)
        return f"{table_name}.*"


register_column_expression_compiler(
    SqlTextColumnExpression, compile_sql_text_column_expression
)
