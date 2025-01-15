from .....model.column_expression import SubqueryColumnExpression
from ..compiler_registry import (
    CompiledColumnExpression,
    QueryLayer,
    register_column_expression_compiler,
)


def compile_subquery_column_expression(
    expr: SubqueryColumnExpression,
    layer: QueryLayer,
) -> CompiledColumnExpression:
    from ...source.compile_source import compile_source

    compiled_model_layer = compile_source(
        expr.model._source, layer.ctx.fork_cte_names("subquery")
    ).finalized()
    return compiled_model_layer.query.scalar_subquery()


register_column_expression_compiler(
    SubqueryColumnExpression, compile_subquery_column_expression
)
