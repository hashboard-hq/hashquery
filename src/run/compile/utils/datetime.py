from datetime import date, datetime, timedelta
from typing import Optional, Union

from dateutil.relativedelta import relativedelta

from ....model.column_expression.column_expression import ColumnExpression
from ....model.column_expression.py_value import PyValueColumnExpression
from ....model.column_expression.sql_function import SqlFunctionColumnExpression
from ....utils.timeinterval import timeinterval

DAY_OF_WEEK_MAPPING = {
    "MONDAY": 0,
    "TUESDAY": 1,
    "WEDNESDAY": 2,
    "THURSDAY": 3,
    "FRIDAY": 4,
    "SATURDAY": 5,
    "SUNDAY": 6,
}


def try_get_constant_ts_value(
    expr: ColumnExpression,
) -> Optional[Union[datetime, date]]:
    """
    If the expression is a constant datetime value, return it.
    Otherwise returns None.
    """
    if type(expr) == SqlFunctionColumnExpression and expr.function_name == "now":
        return datetime.now()
    elif type(expr) == PyValueColumnExpression and type(expr.value) in (datetime, date):
        return expr.value
    return None


def try_get_datetime_delta_value(
    expr: ColumnExpression,
) -> Optional[Union[timedelta, relativedelta]]:
    """
    If the expression is a constant interval/timedelta value, return it.
    Otherwise returns None.

    Note this will return relativedelta for timeinterval values.
    """
    if type(expr) == PyValueColumnExpression and type(expr.value) == timedelta:
        return expr.value
    elif type(expr) == PyValueColumnExpression and type(expr.value) == timeinterval:
        # convert this into a relativedelta, which can reliably shift datetimes in Python across irregular periods.
        return relativedelta(**{expr.value.unit: expr.value.num})
    return None
