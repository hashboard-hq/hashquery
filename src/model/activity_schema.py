from typing import *

from .column_expression.column_expression import ColumnExpression
from ..utils.serializable import Serializable


class ModelActivitySchema(Serializable):
    """
    Container structure for holding information about how a Model
    performs event analysis, typically with `Model.match_steps`.
    """

    def __init__(
        self,
        group: ColumnExpression,
        timestamp: ColumnExpression,
        event_key: ColumnExpression,
    ) -> None:
        self.group = group
        self.timestamp = timestamp
        self.event_key = event_key

    # --- Serialization ---

    def to_wire_format(self) -> Dict:
        return {
            "type": "modelActivitySchema",
            "group": self.group.to_wire_format(),
            "timestamp": self.timestamp.to_wire_format(),
            "eventKey": self.event_key.to_wire_format(),
        }

    @classmethod
    def from_wire_format(cls, wire: Dict) -> "ModelActivitySchema":
        assert wire["type"] == "modelActivitySchema"
        return ModelActivitySchema(
            group=ColumnExpression.from_wire_format(wire["group"]),
            timestamp=ColumnExpression.from_wire_format(wire["timestamp"]),
            event_key=ColumnExpression.from_wire_format(wire["eventKey"]),
        )
