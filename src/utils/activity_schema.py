from typing import *

from ..model.activity_schema import ModelActivitySchema
from ..model.column import column
from ..model.column_expression.column_expression import ColumnExpression
from .identifier import to_python_identifier


def normalize_steps(
    steps: List[Union[str, ColumnExpression, Tuple[str, str]]],
    activity_schema: ModelActivitySchema,
):
    def normalize_step_to_column_expression(step):
        if isinstance(step, ColumnExpression):
            return step
        elif isinstance(step, str):
            return (activity_schema.event_key == step).named(to_python_identifier(step))
        else:
            step_key, step_name = step
            return (activity_schema.event_key == step_key).named(step_name)
            # they've provided an explicit name, keep it as is   ^^^^^^^^^

    step_column_exprs = [normalize_step_to_column_expression(s) for s in steps]
    seen_names: Set[str] = set()
    dupes = [
        step.identifier
        for step in step_column_exprs
        if step.identifier in seen_names or seen_names.add(step.identifier)
    ]
    if dupes:
        raise ValueError(
            f"Found non-unique steps: {', '.join(dupes)}. "
            + "If you want to have multiple steps with the same name, "
            + "you must provide a unique identifier for each step by passing: "
            + "(<step value>, <unique identifier>) for string steps, or using "
            + "`.named()` for column expressions"
        )
    return step_column_exprs


def normalize_step_names(steps: List[Union[str, ColumnExpression, Tuple[str, str]]]):
    mock_steps = normalize_steps(
        steps,
        ModelActivitySchema(
            group=None,
            timestamp=None,
            event_key=column(value="$_mock_$"),
        ),
    )
    return [s.identifier for s in mock_steps]
