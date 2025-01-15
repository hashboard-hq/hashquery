from datetime import date, datetime

import sqlalchemy as sa

from ...db.dialect import SqlDialect

TIME_SQL_STRF = "%Y-%m-%d %H:%M:%S"
DATE_SQL_STRF = "%Y-%m-%d"


class LiteralDateTime(sa.TypeDecorator):
    """
    SQLAlchemy has out of the box support for `datetime.datetime` params
    but its `literal_binds=` parameter to format SQL queries as text does not...
    This provides that support.
    """

    impl = sa.DATETIME

    def __init__(self, *args, dialect, **kwargs):
        self.dialect = dialect
        super().__init__(*args, **kwargs)

    def process_literal_param(self, value: datetime, _):
        if self.dialect == SqlDialect.ATHENA:
            return f"TIMESTAMP '{value.strftime(TIME_SQL_STRF)}'"
        return f"'{value.strftime(TIME_SQL_STRF)}'"


class LiteralDate(sa.TypeDecorator):
    """
    SQLAlchemy has out of the box support for `datetime.date` params
    but its `literal_binds=` parameter to format SQL queries as text does not...
    This provides that support.
    """

    impl = sa.DATE

    def __init__(self, *args, dialect, **kwargs):
        self.dialect = dialect
        super().__init__(*args, **kwargs)

    def process_literal_param(self, value: date, _):
        if self.dialect == SqlDialect.ATHENA:
            return f"DATE '{value.strftime(DATE_SQL_STRF)}'"
        return f"'{value.strftime(DATE_SQL_STRF)}'"
