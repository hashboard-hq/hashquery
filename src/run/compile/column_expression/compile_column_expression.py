from ....model.column_expression import ColumnExpression, SqlTextColumnExpression
from ..query_layer import QueryLayer
from .compiler_registry import (
    COLUMN_EXPRESSION_COMPILER_REGISTRY,
    COLUMN_EXPRESSION_PREPROCESSOR_REGISTRY,
    CompiledColumnExpression,
)
from .compilers.precompiled import PrecompiledColumnExpression


def compile_column_expression(
    column_expression: ColumnExpression,
    layer: QueryLayer,
    *,
    labeled: bool = False,
) -> CompiledColumnExpression:
    """
    Compile the given column expression to its SQLAlchemy representation.
    Can be called with any `ColumnExpression` subtype.
    """
    original_ref = column_expression
    column_expression = preprocess_column_expression(column_expression, layer)
    column_expression_type = type(column_expression)
    compile_func = COLUMN_EXPRESSION_COMPILER_REGISTRY.get(column_expression_type)
    if not compile_func:
        raise NotImplementedError(
            f"Compiler for column expression type `{column_expression_type.__name__}` was not found. "
            + f"Ensure a module calling `register_column_expression_compiler({column_expression_type.__name__}, ...)` "
            + "is registered inside of `./compilers/__init__.py`."
        )
    result = compile_func(column_expression, layer)
    if labeled and _can_label(column_expression):
        result = result.label(column_expression.identifier)

    # Annotate the compiled result on the expression.
    #
    # Note we use the original reference, since preprocessing above potentially makes a new object.
    original_ref._compiled_expression = result
    return result


def _can_label(expr: ColumnExpression):
    if type(expr) is PrecompiledColumnExpression:
        return bool(expr._optional_identifier)
    return not expr._is_star


def preprocess_column_expression(
    column_expression: ColumnExpression,
    layer: QueryLayer,
) -> ColumnExpression:
    """
    Preprocess the given column expression before compiling it.
    Can be called with any `ColumnExpression` subtype.
    """
    if id(column_expression) in layer.ctx.preprocessed:
        # This column expression has already been preprocessed, no need to run it through pre-processing again.
        return column_expression

    column_expression_type = type(column_expression)
    preprocess_funcs = (
        COLUMN_EXPRESSION_PREPROCESSOR_REGISTRY.get(column_expression_type) or []
    )

    for func in preprocess_funcs:
        original_manual_id = column_expression._manually_set_identifier
        column_expression = func(column_expression, layer)
        # Ensure we preserve manually set identifiers across preprocessors.
        if original_manual_id:
            column_expression._manually_set_identifier = original_manual_id

    # The output of pre-processing (or the no-op) should be marked in the context so we don't
    # re-preprocess subexpressions when they are later compiled.
    layer.ctx.preprocessed.add(id(column_expression))
    return column_expression
