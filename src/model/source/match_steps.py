from typing import *

from .source import Source
from ..activity_schema import ModelActivitySchema


class MatchStepsSource(Source):
    def __init__(
        self,
        base: Source,
        activity_schema: ModelActivitySchema,
        steps: List[str],
    ) -> None:
        self.base = base
        self.activity_schema = activity_schema
        self.steps = steps

    def __repr__(self) -> str:
        return str(self.base) + f"\n -> MATCH STEPS"

    __TYPE_KEY__ = "matchSteps"

    def to_wire_format(self) -> dict:
        return {
            **super().to_wire_format(),
            "base": self.base.to_wire_format(),
            "activitySchema": self.activity_schema.to_wire_format(),
            "steps": self.steps,
        }

    @classmethod
    def from_wire_format(cls, wire: dict):
        assert wire["subType"] == cls.__TYPE_KEY__
        return MatchStepsSource(
            base=Source.from_wire_format(wire["base"]),
            activity_schema=ModelActivitySchema.from_wire_format(
                wire["activitySchema"]
            ),
            steps=wire["steps"],
        )
