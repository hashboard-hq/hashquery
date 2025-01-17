from typing import *


class timeinterval:
    """
    Represents an interval with a given datetime unit.

    Depending on the context in which this is used, the interval may be applied
    in memory using `relativedelta` in Python, or it may be compiled to a
    `SQL INTERVAL` expression and sent to the DB.

    **IMPORTANT**: In the latter case, this could naively shift by year/month
    intervals in SQL, which can have different behavior depending on the
    dialect when encountering invalid dates.
    """

    def __init__(
        self,
        *,
        unit: Literal["years", "months", "days", "hours", "minutes", "seconds"],
        num: int
    ):
        self.unit = unit
        self.num = num
