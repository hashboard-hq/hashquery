from typing import *

from .column_expression import ColumnExpression
from ...utils.builder import builder_method


class SqlTextColumnExpression(ColumnExpression):
    def __init__(self, sql: str) -> None:
        super().__init__()
        self.sql = sql

    def default_identifier(self) -> str:
        if self.sql.isidentifier():
            return self.sql
        return None

    @builder_method
    def disambiguated(self, namespace) -> "SqlTextColumnExpression":
        # TODO: users need some ability to qualify their own textual
        # references in raw SQL; which right now they cannot
        pass

    def __repr__(self) -> str:
        return f'sql("{self.sql}")'

    # --- Serialization ---

    __TYPE_KEY__ = "sqlText"

    def to_wire_format(self) -> dict:
        return {
            **super().to_wire_format(),
            "sql": self.sql,
        }

    @classmethod
    def from_wire_format(cls, wire: dict) -> "SqlTextColumnExpression":
        assert wire["subType"] == cls.__TYPE_KEY__
        result = SqlTextColumnExpression(wire["sql"])
        result._from_wire_format_shared(wire)
        return result
