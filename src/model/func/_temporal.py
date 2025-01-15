from ...utils.keypath.resolve import defer_keypath_args
from ..column_expression import ColumnExpression, SqlFunctionColumnExpression


@defer_keypath_args
def diff_seconds(ts1: ColumnExpression, ts2: ColumnExpression) -> ColumnExpression:
    """
    a `diffSeconds` expression over the provided timestamp columns.
    Returns the difference in seconds between the two timestamps.
    """
    return SqlFunctionColumnExpression("diffSeconds", [ts1, ts2])
