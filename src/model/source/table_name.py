from typing import *

from .source import Source


class TableNameSource(Source):
    def __init__(self, table_name: str, schema: Optional[str] = None) -> None:
        super().__init__()
        self.table_name = table_name
        self.schema = schema

    def _default_identifier(self) -> Optional[str]:
        first_token = self.table_name.split(".")[0]
        if first_token.isidentifier():
            return first_token
        return None

    def __repr__(self) -> str:
        result = ""
        if self.schema:
            result += f'"{self.schema}".'
        result += f'"{self.table_name}"'

    __TYPE_KEY__ = "tableName"

    def to_wire_format(self) -> dict:
        return {
            **super().to_wire_format(),
            "tableName": self.table_name,
            "schema": self.schema,
        }

    @classmethod
    def from_wire_format(cls, wire: dict) -> "TableNameSource":
        assert wire["subType"] == cls.__TYPE_KEY__
        return TableNameSource(wire["tableName"], wire.get("schema", None))
