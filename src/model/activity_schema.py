from typing import *

from ..utils.serializable import Serializable
from .column_expression.column_expression import ColumnExpression


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

    def _to_wire_format(self) -> Dict:
        return {
            "type": "modelActivitySchema",
            "group": self.group._to_wire_format(),
            "timestamp": self.timestamp._to_wire_format(),
            "eventKey": self.event_key._to_wire_format(),
        }

    @classmethod
    def _from_wire_format(cls, wire: Dict) -> "ModelActivitySchema":
        assert wire["type"] == "modelActivitySchema"
        return ModelActivitySchema(
            group=ColumnExpression._from_wire_format(wire["group"]),
            timestamp=ColumnExpression._from_wire_format(wire["timestamp"]),
            event_key=ColumnExpression._from_wire_format(wire["eventKey"]),
        )
