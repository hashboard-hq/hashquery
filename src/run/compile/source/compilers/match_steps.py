from datetime import timedelta
from typing import List, Optional, Tuple

import sqlalchemy.exc
import sqlalchemy.sql as sa
import sqlalchemy.types as sa_types

from .....model.column import column
from .....model.column_expression import ColumnExpression, PyValueColumnExpression
from .....model.column_expression.column_expression import ColumnExpression
from .....model.source import MatchStepsSource, PickSource
from .....model.source.aggregate import AggregateSource
from .....model.source.filter import FilterSource
from .....model.source.union import UnionSource
from .....utils.identifier import to_python_identifier
from ....db.dialect import SqlDialect
from ...column_expression.compile_column_expression import compile_column_expression
from ...column_expression.compilers.precompiled import PrecompiledColumnExpression
from ...utils.error import UserCompilationError
from ...utils.private_column import private_column
from ..compile_source import (
    QueryContext,
    QueryLayer,
    compile_source,
    register_source_compiler,
)

TIMESTAMP_LABEL = "__timestamp__"
GROUP_LABEL = "__group__"
STEP_HASH_ID_LABEL = "__step_hash_id__"
EVENT_INDEX_LABEL = "__event_index__"
JOURNEY_HASH_LABEL = "__journey_hash__"


def compile_match_steps_source(source: MatchStepsSource, ctx: QueryContext):
    """
    This compiles a `match_steps` operation. This is not the simplest of tasks
    in SQL, as it can quickly explode into subqueries that cause real issues.

    The best way to begin to understand what this does is to look at the
    SQL it generates and inspect the intermediary tables. The approach was
    inspired by https://medium.com/@pragya.deep19/4c521a75649.

    ---

    At a high level, we form a string for each user called their "journey_hash".
    This string looks like `aa_bac_cab` or similar. It represents an ordered
    set of events we detected for that user, where `a` means "an event matching
    the first step spec", `b` means "an event matching the second step spec",
    etc and `_` meaning "an event that doesn't match anything".

    We conceptually run a regex match to find patterns that are of interest. So
    if we wanted to find a typical funnel, we'd look for `a.*b.*c` to find a
    match for the full funnel. Doing so is lossy though, as we wouldn't know
    which `a` or `b` or `c` events were found, just that there's some group
    that was found.

    So we structure a chain of CTEs (one per step) which captures the index
    of the start of those matches (they are all 1-character long so we can do
    that with some clever string-length math). Then in the final result, we
    join to those indices.
    """
    group_expr = source.activity_schema.group
    timestamp_expr = source.activity_schema.timestamp

    enforce_time_limits_in_dedicated_cte = ctx.dialect == SqlDialect.CLICKHOUSE
    can_fail_on_listagg_size_limit = ctx.dialect == SqlDialect.REDSHIFT

    partition_expressions = [
        private_column(
            _partition_label(column_expr),
            column_expr,
        )
        for column_expr in source.partition_start_events
    ]

    # events may overlap, and we need to gather them all together, which
    # requires duplicating and union-ing the table for each set of events
    # which match a given condition

    # gather N sources which collect all matching events for a given step
    step_event_sources = []
    for step_index, step_condition in enumerate(source.steps):
        picked_columns = [
            # Collect all the source columns attached to the main layer
            # for later use. This drops joins from the result, which
            # is expected, the API of `match_steps` explicitly replaces
            # all relations with the final steps. This also keeps the
            # number of output columns consistent across all `step_event_sources`
            # consistent, even if one of them was filtered on a joined column.
            column(sql="*"),
            # track the step hash id this was filtered to
            private_column(
                STEP_HASH_ID_LABEL,
                _step_hash_id_literal_expr(step_index, ctx),
            ),
            # need to inline these since they may rely on relations
            # that'll be flattened out by the union
            private_column(GROUP_LABEL, group_expr),
            private_column(TIMESTAMP_LABEL, timestamp_expr),
        ]
        picked_columns.extend(partition_expressions)
        step_event_sources.append(
            PickSource(FilterSource(source.base, step_condition), picked_columns)
        )
    # union all the above
    step_event_sources_union = step_event_sources[0]
    for step_index in range(1, len(source.steps)):
        step_event_sources_union = UnionSource(
            step_event_sources_union, step_event_sources[step_index]
        )
    # collect all the events into one table and track `event_index`
    events_layer_source = PickSource(
        step_event_sources_union,
        [
            # this collects `STEP_HASH_ID_LABEL`, `GROUP_LABEL` and `TIMESTAMP_LABEL`
            # from the prior step, alongside all other event properties.
            column(sql="*"),
            private_column(
                EVENT_INDEX_LABEL,
                PrecompiledColumnExpression(
                    # Hashquery doesn't yet support a native `row_number`
                    # expression, so we compile it inline ourselves.
                    sa.func.row_number().over(
                        partition_by=sa.column(GROUP_LABEL),
                        order_by=sa.column(TIMESTAMP_LABEL).asc(),
                        # ^^^^ this doesn't need to tie-break on `step_hash_id` like
                        # we do in the journey hash since those events would be identical
                    )
                ),
            ),
        ],
    )

    # `events_layer` is the compiled events ready for analysis, which we can
    # build our journey hashes from. This table may contain duplicates.
    events_layer = compile_source(events_layer_source, ctx).finalized()
    events_query = events_layer.query.cte(ctx.next_cte_name())

    partition_query = _form_partition_query(
        partition_expressions,
        events_query,
        events_layer,
    )

    journey_base = events_query

    if ctx.dialect == SqlDialect.CLICKHOUSE:
        # Wrap in a sorting CTE to ensure that the events are ordered by timestamp.
        #
        # This is because Clickhouse does not support ORDER BY when aggregating arrays -- the table needs to be
        # pre-sorted.
        journey_base = (
            sa.select(
                [
                    sa.column(GROUP_LABEL, _selectable=events_query),
                    sa.column(STEP_HASH_ID_LABEL, _selectable=events_query),
                    sa.column(TIMESTAMP_LABEL, _selectable=events_query),
                ]
            )
            .select_from(journey_base)
            .order_by(
                sa.column(GROUP_LABEL, _selectable=events_query),
                sa.column(TIMESTAMP_LABEL, _selectable=events_query),
                sa.column(STEP_HASH_ID_LABEL, _selectable=events_query).desc(),
            )
            .subquery()
        )

    # Compile a pipeline of CTEs which collect the matches of the sequence and
    # determine the `step_N_event_index` column for each step
    #
    # The overall logic is as follows:
    #   - Greedily put together a single funnel for each user, by finding the first subsequent instance of each event
    #   - For each step in the funnel (except the first), if the time between the current step and the first step is greater than the time limit, replace the current step with NULL.
    #
    # This solution is a bit naive/pessimistic -- with multiple occurrences of each event, it would be possible for us to "miss" an instance of a funnel that did pass the time filter,
    # since we only evaluate the filter after putting together the funnel/steps.
    journeys_ctx = ctx.fork_cte_names(ctx.name + "_match_steps")
    journeys_query: sa.Select = (
        sa.select(
            sa.column(GROUP_LABEL, _selectable=journey_base).label(
                group_expr.identifier
            ),
            _build_journey_hash(ctx).label(JOURNEY_HASH_LABEL),
        )
        .select_from(journey_base)
        .group_by(sa.column(GROUP_LABEL, _selectable=journey_base))
        .cte(journeys_ctx.next_cte_name())
    )

    if can_fail_on_listagg_size_limit:
        ctx.register_exec_error_handler(_listagg_limit_error_handler)

    initial_step_cte = (
        (
            partition_query,
            [
                # Passthrough the partition into the journeys query.
                sa.column(expr.identifier, _selectable=partition_query).label(
                    expr.identifier
                )
                for expr in partition_expressions
            ],
        )
        if partition_query is not None
        else None
    )

    for index in range(len(source.steps)):
        journeys_query = _form_journeys_query_step(
            journeys_query, index, journeys_ctx, group_expr, initial_step_cte
        ).cte(journeys_ctx.next_cte_name())

    events_relations = [
        events_query.alias(ctx.next_alias_name(step.identifier))
        for step in source.steps
    ]

    # For clickhouse, we add an extra CTE to filter out events (step indexes) that don't pass the time limit.
    if source.time_limit and enforce_time_limits_in_dedicated_cte:
        journeys_query = form_time_limit_cte(
            journeys_query,
            journeys_ctx,
            events_relations,
            group_expr,
            source.time_limit,
            source.steps,
            partition_expressions,
        )

    # gather up the unique groups, which will be the basis of the final table
    unique_groups_layer = compile_source(
        AggregateSource(source.base, groups=[group_expr], measures=[]), ctx
    )

    # Builds a CTE that flattens together the entity ID, the (optional) partition key, and all of the step indices.
    #
    # This is slightly distinct from the journeys query in that it includes all unique entities, even those
    # who did not match any steps. Those will have NULL values for the partition and step indices.
    entity_base_layer = unique_groups_layer.chained()
    entity_base_layer.query = entity_base_layer.query.outerjoin(
        journeys_query,
        sa.column(
            group_expr.identifier, _selectable=entity_base_layer.namespaces.main.ref
        )
        == sa.column(group_expr.identifier, _selectable=journeys_query),
    )

    # Ensure this CTE only selects the entity/group, and each of the step indices.
    # Partition key is optionally included if applicable.
    entity_base_layer.query = entity_base_layer.query.with_only_columns(
        [
            sa.column(
                group_expr.identifier, _selectable=entity_base_layer.namespaces.main.ref
            ),
        ]
        + (
            [
                sa.column(_partition_label(expr), _selectable=journeys_query).label(
                    expr.identifier
                )
                for expr in source.partition_start_events
            ]
        )
        + [
            sa.column(_step_event_index_column_name(index), _selectable=journeys_query)
            for index in range(len(source.steps))
        ]
    )
    if source.partition_start_events:
        # When partitioning start events, drop any entities which didn't enter any journey.
        # This is applied only when partitioning, since including these entries would appear
        # like the partition value was `NULL`, which could be a valid value. Including unmatched
        # entities also just makes a lot less sense when partitioning, since the grain of the table
        # changed (from entities, to partitioned entities by start event).
        # This difference is gate-kept on the client side for funnels by not allowing the `top_of_funnel`
        # setting when partitioning.
        entity_base_layer.query = entity_base_layer.query.filter(
            sa.column(
                _step_event_index_column_name(0), _selectable=journeys_query
            ).is_not(None),
        )

    # Final layer has no specific columns selected, but contains joins for each matched step.
    final_layer = entity_base_layer.chained()
    final_layer.is_joined = True

    # At this point, we join the info for each matched step to the final layer. Each join is aliased by
    # the step's identifier.
    #
    # This is also the point where we enforce time limits.
    for index, step in enumerate(source.steps):
        # join to the source event for each matched step
        events_relation = events_relations[index]
        step_index_column_name = _step_event_index_column_name(index)
        join_conditions = [
            sa.column(GROUP_LABEL, _selectable=events_relation)
            == sa.column(
                group_expr.identifier, _selectable=final_layer.namespaces.main.ref
            ),
            sa.column(EVENT_INDEX_LABEL, _selectable=events_relation)
            == sa.column(
                step_index_column_name, _selectable=final_layer.namespaces.main.ref
            ),
        ]
        if source.time_limit and not enforce_time_limits_in_dedicated_cte and index > 0:
            join_conditions.append(
                _timestamp_diff_within(
                    sa.column(TIMESTAMP_LABEL, _selectable=events_relations[0]),
                    sa.column(TIMESTAMP_LABEL, _selectable=events_relation),
                    source.time_limit,
                    ctx,
                )
            )
        final_layer.query = final_layer.query.outerjoin(
            events_relation,
            sa.and_(*join_conditions),
        )
        # We can just re-use the underlying column metadata from the events
        # query layer. That query selects * as well as additional columns, but
        # those additional columns are implementation details and don't need to
        # be surfaced in the output query layer.
        final_layer.namespaces.set_joined(
            step.identifier,
            events_relation,
            events_layer.namespaces.main.column_metadata,
        )

    return final_layer


def form_time_limit_cte(
    journeys_query,
    journeys_ctx: QueryContext,
    events_relations: List,
    group_expr: ColumnExpression,
    time_limit: timedelta,
    steps: List[ColumnExpression],
    partition_expressions: List[ColumnExpression],
):
    select_from = journeys_query
    final_columns = [
        sa.column(group_expr.identifier, _selectable=journeys_query),
    ] + [
        sa.column(expr.identifier, _selectable=journeys_query).label(expr.identifier)
        for expr in partition_expressions
    ]

    for index in range(len(steps)):
        # join to the source event for each matched step
        events_relation = events_relations[index]
        step_index_column_name = _step_event_index_column_name(index)

        select_from = select_from.outerjoin(
            events_relation,
            sa.and_(
                sa.column(GROUP_LABEL, _selectable=events_relation)
                == sa.column(group_expr.identifier, _selectable=journeys_query),
                sa.column(EVENT_INDEX_LABEL, _selectable=events_relation)
                == sa.column(step_index_column_name, _selectable=journeys_query),
            ),
        )
        if index == 0:
            # No need to evaluate time limit on the first step in the funnel.
            final_columns.append(sa.column(step_index_column_name))
        else:
            final_columns.append(
                sa.func.IF(
                    _timestamp_diff_within(
                        sa.column(TIMESTAMP_LABEL, _selectable=events_relations[0]),
                        sa.column(TIMESTAMP_LABEL, _selectable=events_relation),
                        time_limit,
                        journeys_ctx,
                    ),
                    sa.column(step_index_column_name),
                    None,
                ).label(step_index_column_name)
            )
    return (
        sa.select(*final_columns)
        .select_from(select_from)
        .cte(journeys_ctx.next_cte_name())
    )


def _form_partition_query(
    partition_expressions: List[ColumnExpression],
    events_query: sa.Select,
    events_layer: QueryLayer,
) -> Optional[sa.expression.CTE]:
    if not partition_expressions:
        return None

    partition_query = events_query
    partition_ctx = events_layer.ctx.fork_cte_names(
        events_layer.ctx.name + "_partition"
    )

    partitioned_index_label = "__partitioned_index__"

    def partition_expr_for_window_func(
        expr: ColumnExpression, compiled_type: sa_types.TypeEngine
    ):
        # Bigquery doesn't support PARTITION BY on FLOAT types, so we cast to NUMERIC.
        # https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#grouping_with_floating_point_types
        if events_layer.ctx.dialect == SqlDialect.BIGQUERY and type(compiled_type) in (
            sa_types.Float,
            sa_types.FLOAT,
        ):
            return sa.cast(sa.column(expr.identifier), sa_types.NUMERIC)

        return sa.column(expr.identifier)

    # This calculates the "partition index" for each partition, which is the
    # index of the each event **within** each (user, partition_key) group.
    # (always ordered by timestamp)
    partition_query = (
        sa.select(
            [
                "*",
                sa.func.row_number()
                .over(
                    partition_by=[
                        sa.column(GROUP_LABEL),
                    ]
                    + [
                        partition_expr_for_window_func(
                            expr, expr._compiled_expression.type
                        )
                        for expr in partition_expressions
                    ],
                    order_by=sa.column(TIMESTAMP_LABEL).asc(),
                )
                .label(partitioned_index_label),
            ]
        )
        .select_from(partition_query)
        .where(sa.column(STEP_HASH_ID_LABEL) == sa.literal(_step_hash_id(0)))
    ).cte(partition_ctx.next_cte_name())

    # Drop all events except the first for each (user, partition key) window.
    return (
        sa.select(["*"])
        .select_from(partition_query)
        .where(sa.column(partitioned_index_label) == 1)
    ).cte(partition_ctx.next_cte_name())


def _form_journeys_query_step(
    journeys_query: sa.Select,
    step_index: int,
    ctx: QueryContext,
    group_expr,
    initial_step_cte: Optional[Tuple[sa.expression.CTE, List[sa.ColumnElement]]],
) -> sa.Select:
    # If a CTE is provided for the initial step, we use that as the basis for the journey.
    # Note that we assume the initial step CTE has GROUP_LABEL and EVENT_INDEX_LABEL columns.
    #
    # Other columns for passthrough can be provided in initial_step_cte[1]
    #
    # All other step index calculations (whether partitioned or not) can be done by regexing the (un-partitioned) journey hash for the entity.
    if initial_step_cte is not None and step_index == 0:
        cte, passthrough_columns = initial_step_cte

        joined_event_index = sa.column(EVENT_INDEX_LABEL, _selectable=cte)
        if ctx.dialect in (SqlDialect.POSTGRES, SqlDialect.REDSHIFT):
            # Postgres & Redshift require explicit casting to INTEGER, otherwise it gets typed as a "bigint" and breaks some downstream SUBSTRING calls.
            joined_event_index = sa.cast(joined_event_index, sa_types.INTEGER)

        return sa.select(
            [
                sa.column(group_expr.identifier, _selectable=journeys_query),
                sa.column(JOURNEY_HASH_LABEL, _selectable=journeys_query),
                joined_event_index.label(_step_event_index_column_name(0)),
            ]
            + passthrough_columns
        ).select_from(
            cte.join(
                journeys_query,
                journeys_query.c[group_expr.identifier]
                == sa.column(GROUP_LABEL, _selectable=cte),
            )
        )
    else:
        return sa.select(
            [
                "*",
                _get_event_index_col(step_index, ctx).label(
                    _step_event_index_column_name(step_index)
                ),
            ]
        ).select_from(journeys_query)


def _step_hash_id(step_index: int):
    # It is important that these are in standard ASCII order: (A -> a -> 0)
    # since our journey hashes order by them
    CHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    CHARS += CHARS.lower()
    CHARS += "0123456789"

    if step_index >= len(CHARS):
        raise UserCompilationError("Too many unique step types to match against.")

    return CHARS[step_index]


def _step_event_index_column_name(step_index: int):
    return f"step_{step_index}_event_index"


def _build_journey_hash(ctx: QueryContext):
    # because the source may contain duplicate events, we order by `step_hash_id`
    # as well to place earlier steps in the sequence last in any group of duplicates.
    # So a single event matching three steps would appear as `cba`. This ensures
    # that when we match an earlier step (`a` in this case), we move the index ahead
    # of the other steps (`cb` here).

    # these are all static strings so no risk of SQL injection here

    if ctx.dialect == SqlDialect.SNOWFLAKE or ctx.dialect == SqlDialect.ATHENA:
        return sa.literal_column(
            f"LISTAGG({STEP_HASH_ID_LABEL}, '') WITHIN GROUP (ORDER BY {TIMESTAMP_LABEL}, {STEP_HASH_ID_LABEL} DESC)"
        )
    elif ctx.dialect == SqlDialect.DATABRICKS:
        return sa.literal_column(
            f"array_join(transform("
            + f"array_sort("
            + f"  array_agg(struct({STEP_HASH_ID_LABEL}, {TIMESTAMP_LABEL})), "
            + f"  (left, right) -> CASE "
            + f"    WHEN left.{TIMESTAMP_LABEL} < right.{TIMESTAMP_LABEL} THEN -1 "
            + f"    WHEN left.{TIMESTAMP_LABEL} > right.{TIMESTAMP_LABEL} THEN 1 "
            + f"    ELSE CASE WHEN left.{STEP_HASH_ID_LABEL} < right.{STEP_HASH_ID_LABEL} THEN 1 ELSE -1 END END"
            + f" ), x -> x.{STEP_HASH_ID_LABEL}"
            + f"), '')"
        )
    elif ctx.dialect == SqlDialect.REDSHIFT:
        return sa.literal_column(
            f"LISTAGG({STEP_HASH_ID_LABEL}) WITHIN GROUP (ORDER BY {TIMESTAMP_LABEL}, {STEP_HASH_ID_LABEL} DESC)"
        )
    elif ctx.dialect == SqlDialect.CLICKHOUSE:
        # Note - Although Click supports ordering here by using groupArraySorted(), this lives next to the journey hash aggregation,
        # which can't use groupArraySorted() because it's sorted by a different column than the item in the array.
        #
        # Both of these aggregations rely on the table being pre-sorted by timestamp.
        return sa.literal_column(f"arrayStringConcat(groupArray({STEP_HASH_ID_LABEL}))")
    elif ctx.dialect == SqlDialect.MYSQL:
        return sa.literal_column(
            f"GROUP_CONCAT({STEP_HASH_ID_LABEL} ORDER BY {TIMESTAMP_LABEL} ASC, {STEP_HASH_ID_LABEL} DESC SEPARATOR '')"
        )
    else:
        return sa.literal_column(
            f"STRING_AGG({STEP_HASH_ID_LABEL}, '' ORDER BY {TIMESTAMP_LABEL}, {STEP_HASH_ID_LABEL} DESC)"
        )


def _regex_extract(column: str, regex: str, ctx: QueryContext):
    if ctx.dialect in (SqlDialect.BIGQUERY, SqlDialect.SNOWFLAKE):
        return f"REGEXP_SUBSTR({column}, '{regex}')"
    elif ctx.dialect == SqlDialect.POSTGRES:
        return f"(regexp_match({column}, '{regex}'))[1]"
    elif ctx.dialect == SqlDialect.REDSHIFT:
        return f"(REGEXP_SUBSTR({column}, '{regex}'))"
    elif ctx.dialect == SqlDialect.DUCKDB:
        return f"(regexp_extract_all({column}, '{regex}'))[1]"
    elif ctx.dialect == SqlDialect.ATHENA:
        return f"regexp_extract({column}, '{regex}')"
    elif ctx.dialect == SqlDialect.CLICKHOUSE:
        return f"regexpExtract({column}, '{regex}', 0)"
    elif ctx.dialect == SqlDialect.DATABRICKS:
        return f"try_element_at(regexp_extract_all({column}, '{regex}', 0),1)"
    elif ctx.dialect == SqlDialect.MYSQL:
        return f"REGEXP_SUBSTR({column}, '{regex}')"
    else:
        ctx.add_warning(
            "Required regex extract is not codified for this SQL dialect and may be incorrect."
        )
        # idk maybe try the POSTGRES syntax?
        dialect = ctx.dialect
        ctx.dialect = SqlDialect.POSTGRES
        result = _regex_extract(column, regex, ctx)
        ctx.dialect = dialect
        return result


def _get_event_index_col(index, ctx):
    step_hash_id = _step_hash_id(index)
    prev_step_event_index_column_name = (
        _step_event_index_column_name(index - 1) if index > 0 else None
    )
    target_journey_hash = (
        f"SUBSTRING({JOURNEY_HASH_LABEL}, {prev_step_event_index_column_name} + 1)"
        if prev_step_event_index_column_name
        else JOURNEY_HASH_LABEL
    )
    regex = f"{step_hash_id}.*"
    # this is safe from SQL-injection because it is only using
    # identifiers we generated
    col = sa.literal_column(
        f"LENGTH({JOURNEY_HASH_LABEL}) - "
        + f"LENGTH({_regex_extract(target_journey_hash, regex, ctx)})"
        + "+ 1"
    )

    if ctx.dialect == SqlDialect.CLICKHOUSE:
        col = sa.func.toUInt64(col)
    return col


def _timestamp_diff_within(
    ts1: sa.ColumnElement,
    ts2: sa.ColumnElement,
    time_limit: timedelta,
    ctx: QueryContext,
):
    # We're just compiling constant expressions here, so we can use a dummy query layer.
    dummy_query_layer = QueryLayer(ctx)

    if ctx.dialect == SqlDialect.SNOWFLAKE:
        # Snowflake requires us to:
        #   - use TIMESTAMPDIFF instead of the minus operator.
        #   - Compare the result of TIMESTAMPDIFF to an integer # of seconds, not an interval.
        compiled_time_limit = compile_column_expression(
            PyValueColumnExpression(time_limit.total_seconds()),
            dummy_query_layer,
        )
        return sa.func.timestampdiff("second", ts1, ts2) < compiled_time_limit
    elif ctx.dialect == SqlDialect.REDSHIFT:
        compiled_time_limit = compile_column_expression(
            PyValueColumnExpression(time_limit.total_seconds()),
            dummy_query_layer,
        )

        return (
            sa.func.datediff(
                sa.text("second"),
                sa.cast(ts1, sa_types.TIMESTAMP),
                sa.cast(ts2, sa_types.TIMESTAMP),
            )
            < compiled_time_limit
        )
    elif ctx.dialect == SqlDialect.CLICKHOUSE:
        compiled_time_limit = compile_column_expression(
            PyValueColumnExpression(time_limit.total_seconds()),
            dummy_query_layer,
        )
        return sa.func.dateDiff(sa.literal("second"), ts1, ts2) < compiled_time_limit
    elif ctx.dialect == SqlDialect.MYSQL:
        compiled_time_limit = compile_column_expression(
            PyValueColumnExpression(time_limit.total_seconds()),
            dummy_query_layer,
        )
        return sa.func.TIMESTAMPDIFF(sa.text("SECOND"), ts1, ts2) < compiled_time_limit

    compiled_time_limit = compile_column_expression(
        PyValueColumnExpression(time_limit),
        dummy_query_layer,
    )
    return (ts2 - ts1) < compiled_time_limit


def _step_hash_id_literal_expr(
    step_index: int,
    ctx: QueryContext,
):
    expr = sa.literal(_step_hash_id(step_index))
    if ctx.dialect == SqlDialect.REDSHIFT:
        # In Redshift, if we only have a single step in the query (and thus a single literal expression like this), it doesn't properly understand the type of the literal
        # and downstream operations fail.
        #
        # This isn't a SQLAlchemy issue but an actual query issue -- we workaround this by explicitly casting to TEXT.
        expr = sa.cast(expr, sa_types.TEXT)

    return PrecompiledColumnExpression(lambda _: expr)


def _partition_label(partition_expr: ColumnExpression):
    return f"__partition_{to_python_identifier(partition_expr.identifier)}_key__"


def _listagg_limit_error_handler(exc: Exception):
    try:
        from psycopg2.errors import InternalError as Psycopg2InternalError
    except ImportError:
        Psycopg2InternalError = None

    if (
        isinstance(exc, sqlalchemy.exc.InternalError)
        and isinstance(exc.orig, Psycopg2InternalError)
        and "Result size exceeds LISTAGG limit" in (exc.orig.pgerror or "")
    ):
        return "Redshift LISTAGG size limit (65535) exceeded. Consider pre-filtering out entities with many matched events."


register_source_compiler(MatchStepsSource, compile_match_steps_source)
