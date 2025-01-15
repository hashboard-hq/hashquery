from typing import *

from ..column_expression import ColumnExpression
from .source import Source


class SortSource(Source):
    def __init__(
        self,
        base: Source,
        sort: ColumnExpression,
        dir: Literal["asc", "desc"],
        nulls: Literal["auto", "first", "last"],
    ) -> None:
        self.base = base
        self.sort = sort
        if dir.lower() not in ("asc", "desc"):
            raise ValueError(
                f"Invalid sort direction: '{dir}'. Must be 'asc' or 'desc'."
            )
        self.dir = dir.lower()
        if nulls.lower() not in ("auto", "first", "last"):
            raise ValueError(
                f"Invalid nulls ordering: '{nulls}'. Must be 'auto', 'first', or 'last'."
            )
        self.nulls = nulls.lower()

    def __repr__(self) -> str:
        return (
            str(self.base) + f"\n -> ORDER BY {str(self.sort)} NULLS {str(self.nulls)}"
        )

    def _default_identifier(self):
        return self.base._default_identifier()

    __TYPE_KEY__ = "sort"

    def _to_wire_format(self) -> dict:
        return {
            **super()._to_wire_format(),
            "base": self.base._to_wire_format(),
            "sort": self.sort._to_wire_format(),
            "dir": self.dir,
            "nulls": self.nulls,
        }

    @classmethod
    def _from_wire_format(cls, wire: dict):
        assert wire["subType"] == cls.__TYPE_KEY__
        return SortSource(
            Source._from_wire_format(wire["base"]),
            ColumnExpression._from_wire_format(wire["sort"]),
            wire["dir"],
            # client-server version compat
            wire.get("nulls", "auto"),
        )
