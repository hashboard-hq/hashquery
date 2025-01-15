from datetime import timedelta
from typing import *

from ...utils.activity_schema import normalize_steps
from ..activity_schema import ModelActivitySchema
from ..column_expression.column_expression import ColumnExpression
from .source import Source


class MatchStepsSource(Source):
    def __init__(
        self,
        base: Source,
        activity_schema: ModelActivitySchema,
        steps: List[ColumnExpression],
        partition_start_events: Optional[List[ColumnExpression]] = None,
        time_limit: Optional[timedelta] = None,
    ) -> None:
        self.base = base
        self.activity_schema = activity_schema
        self.steps = steps
        self.partition_start_events = partition_start_events or []
        self.time_limit = time_limit

    def __repr__(self) -> str:
        return str(self.base) + f"\n -> MATCH STEPS"

    __TYPE_KEY__ = "matchSteps"

    def _to_wire_format(self) -> dict:
        return {
            **super()._to_wire_format(),
            "base": self.base._to_wire_format(),
            "activitySchema": self.activity_schema._to_wire_format(),
            "steps": [step._to_wire_format() for step in self.steps],
            "partitionStartEvents": [
                expr._to_wire_format() for expr in self.partition_start_events
            ],
            "timeLimit": self._primitive_to_wire_format(self.time_limit),
        }

    @classmethod
    def _from_wire_format(cls, wire: dict):
        assert wire["subType"] == cls.__TYPE_KEY__

        activity_schema = ModelActivitySchema._from_wire_format(wire["activitySchema"])
        parsed_step_wire = [
            (
                ColumnExpression._from_wire_format(step)
                if isinstance(step, dict)
                else step
            )
            for step in wire["steps"]
        ]
        partition_start_events = [
            ColumnExpression._from_wire_format(serialized_expr)
            for serialized_expr in wire.get("partitionStartEvents", [])
        ]

        return MatchStepsSource(
            base=Source._from_wire_format(wire["base"]),
            activity_schema=activity_schema,
            # `match_steps` previously serialized a flat list of strings instead
            # of tuples, so run it through `normalize_steps` for compatibility
            steps=normalize_steps(parsed_step_wire, activity_schema),
            partition_start_events=partition_start_events,
            time_limit=cls._primitive_from_wire_format(wire.get("timeLimit", None)),
        )
