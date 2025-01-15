from ....model.column_expression import ColumnExpression, ColumnNameColumnExpression
from ....utils.identifier import is_double_underscore_name


def private_column(name: str, column: ColumnExpression = None) -> ColumnExpression:
    """
    `column.named` will prevent the use of `__private__` names.
    This provides a private API to name columns with private names.
    """
    if column is None:
        return private_column(name, ColumnNameColumnExpression(name))

    if not is_double_underscore_name(name):
        raise ValueError("Private column names must be of the form `__name__`")

    result = column.named("_")  # to get a copy
    result._manually_set_identifier = name
    return result
