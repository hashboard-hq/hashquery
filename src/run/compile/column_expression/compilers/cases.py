from copy import copy

import sqlalchemy as sa

from .....model.column_expression.cases import CasesColumnExpression
from ..compile_column_expression import (
    QueryLayer,
    compile_column_expression,
    preprocess_column_expression,
)
from ..compiler_registry import (
    CompiledColumnExpression,
    register_column_expression_compiler,
)


def compile_cases_column_expression(
    expr: CasesColumnExpression,
    layer: QueryLayer,
) -> CompiledColumnExpression:
    return sa.case(
        [
            (
                compile_column_expression(case[0], layer),
                compile_column_expression(case[1], layer),
            )
            for case in expr.cases
        ],
        else_=compile_column_expression(expr.other, layer),
    )


def preprocess_cases_column_expression(
    expr: CasesColumnExpression,
    layer: QueryLayer,
) -> CasesColumnExpression:
    expr = copy(expr)
    expr.cases = [
        (
            preprocess_column_expression(condition, layer),
            preprocess_column_expression(value, layer),
        )
        for condition, value in expr.cases
    ]
    expr.other = preprocess_column_expression(expr.other, layer)
    return expr


register_column_expression_compiler(
    CasesColumnExpression,
    compile_cases_column_expression,
)
