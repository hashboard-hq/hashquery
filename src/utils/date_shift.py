from typing import *


class _DateShift:
    """Represents a date shift by either months or years. Note that this isn't captured by timedelta shift, since those only accept periods with consistent lengths (e.g. weeks or narrower)

    **IMPORTANT**: This will naively shift by year/month intervals in SQL. This is ideally applied against a date that has already been truncated to the relevant interval.
    (e.g. when doing `some_date` + _DateShift(months=1), `some_date` should already be truncated to month or greater.

    If not, it's possible to shift to invalid dates which can throw errors, coerce to null, or shift to close but potentially incorrect dates, all depending on the dialect.

    Because of this detail, it's currently an internal class and not user-facing.
    """

    def __init__(self, *, years: Optional[int] = None, months: Optional[int] = None):
        if years and months:
            raise ValueError("Cannot shift by both years and months")
        self.years = years
        self.months = months
