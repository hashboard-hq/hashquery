from typing import *

from .source import Source


class SqlTextSource(Source):
    def __init__(self, sql: str) -> None:
        super().__init__()
        self.sql = sql

    def _default_identifier(self) -> Optional[str]:
        # maybe inspect the SQL to look for a table name?
        return None

    def __repr__(self) -> str:
        return f'sql("{self.sql}")'

    __TYPE_KEY__ = "sqlText"

    def to_wire_format(self) -> dict:
        return {
            **super().to_wire_format(),
            "sql": self.sql,
        }

    @classmethod
    def from_wire_format(cls, wire: dict) -> "SqlTextSource":
        assert wire["subType"] == cls.__TYPE_KEY__
        return SqlTextSource(wire["sql"])
