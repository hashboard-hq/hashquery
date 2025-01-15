from copy import deepcopy
from datetime import datetime, timedelta
from typing import *

from ..run.run import run
from ..utils.activity_schema import normalize_step_names, normalize_steps
from ..utils.builder import builder_method
from ..utils.equals import deep_equal
from ..utils.identifiable import IdentifiableMap
from ..utils.keypath import (
    KeyPath,
    _,
    resolve_all_nested_keypaths,
    resolve_keypath,
    resolve_keypath_args_from,
    unwrap_keypath_to_name,
)
from ..utils.keypath.keypath import KeyPathComponentProperty
from ..utils.keypath.keypath_ctx import KeyPathCtx
from ..utils.resource import LinkedResource
from ..utils.serializable import Serializable
from . import func
from .activity_schema import ModelActivitySchema
from .column import column
from .column_expression import ColumnExpression
from .connection import Connection
from .namespace import ModelNamespace
from .source import (
    AggregateSource,
    FilterSource,
    JoinOneSource,
    LimitSource,
    MatchStepsSource,
    PickSource,
    SortSource,
    Source,
    SqlTextSource,
    TableNameSource,
    UnionSource,
)

FUNNEL_COUNT_COLUMN_NAME = "entities"


class Model(Serializable):
    @overload
    def __init__(
        self, connection: Connection, table: str, *, schema: Optional[str] = None
    ):
        """
        Initialize a new Model for the provided table in the given connection.
        """
        ...

    @overload
    def __init__(self, connection: Connection, *, sql_query: str):
        """
        Initialize a new Model which runs the provided sql query inside of the
        given connection.
        """
        ...

    @overload
    def __init__(self):
        """
        Initialize a new Model that is completely empty.

        This Model will have absolutely nothing in it and will not be queryable
        until a connection and a source table is attached with
        `.with_connection()` and `.with_source()`.
        """
        ...

    def __init__(
        self, connection=None, table=None, *, sql_query=None, schema=None
    ) -> None:
        self._connection: Connection = None
        self._source: Source = None
        self._attributes = IdentifiableMap[ColumnExpression]()
        self._measures = IdentifiableMap[ColumnExpression]()
        self._namespaces = IdentifiableMap[ModelNamespace]()
        self._primary_key: ColumnExpression = column("id")
        self._activity_schema: Optional[ModelActivitySchema] = None
        self._custom_meta = {}
        # internally used to understand the origin of a model
        # and its relation to an original Hashboard resource
        self._linked_resource: Optional[LinkedResource] = None

        if connection:
            Model.with_connection.mutate(self, connection)
        if table or sql_query or schema:
            Model.with_source.mutate(
                self,
                table=table,
                schema=schema,
                sql_query=sql_query,
            )

    # --- Public accessors ---

    def _access_identifiable_map(
        self,
        map_names: Union[str, List[str]],
        identifier: str,
        *,
        keypath_ctx: Union[KeyPathCtx, str] = None,
        syntax: Union[Literal["accessor"], Literal["sql_ref"]] = "accessor",
    ):
        # internal accessor for getting attributes, measures, and relations
        map_names = [map_names] if isinstance(map_names, str) else map_names
        maps: List[IdentifiableMap] = [
            getattr(self, map_name) for map_name in map_names
        ]
        for map in maps:
            if result := map.get(identifier):
                return result

        # accessing an attr/measure/namespace which doesn't exist is a pretty
        # common pitfall, so put in the extra work to make a good error message
        map_debug_names = [
            map_name[1:-1].replace("namespace", "relation") for map_name in map_names
        ]
        error = [
            f"No {' or '.join(map_debug_names)} named `{identifier}` was found in the model."
        ]
        did_you_mean = lambda accessor, id: (
            f"Did you mean `{accessor}.{id}`?"
            if syntax == "accessor"
            else "Did you mean `{{" + id + "}}`?"
        )
        if attr_result := self._attributes.get(identifier):
            error.append(f"An attribute ({attr_result}) was found instead. ")
            error.append(
                "This was potentially caused because measures are converted "
                + "to attributes after an aggregation. "
            )
            error.append(did_you_mean("attr", identifier))
        if measure_result := self._measures.get(identifier):
            error.append(f"A measure ({measure_result}) was found instead. ")
            error.append(did_you_mean("msr", identifier))
        if self._namespaces.get(identifier):
            error.append(f"A model relation was found instead. ")
            error.append(did_you_mean("rel", identifier))
        if "_namespaces" in map_names:
            # look for if the next access is against an attribute we have,
            # in which case this mistake was that they didn't realize the
            # relation had been flattened.
            next_access = (
                keypath_ctx.remaining_keypath._key_path_components[0]
                if (
                    isinstance(keypath_ctx, KeyPathCtx)
                    and keypath_ctx.remaining_keypath._key_path_components
                )
                else keypath_ctx
            )
            next_access_name: str = None
            if type(next_access) is str:
                next_access_name = next_access
            elif type(next_access) is KeyPathComponentProperty:
                next_access_name = next_access.name
            next_access_attr = (
                self._attributes.get(next_access_name) if next_access_name else None
            )
            if next_access_attr:
                error.append(f"The target attribute `{next_access_name}` was found. ")
                error.append(
                    "A transformation may have moved the attribute "
                    + f"from the {identifier} relation to being directly available. "
                )
                error.append(did_you_mean("attr", next_access_name))
        if len(error) == 1:
            for idx, map_name in enumerate(map_names):
                map: IdentifiableMap = getattr(self, map_name)
                debug_name = map_debug_names[idx]
                error.append(
                    f"Available {debug_name}s: {', '.join(map.keys())}"
                    if map
                    else f"No {debug_name}s are defined for this model."
                )
        raise AttributeError("\n".join(error))

    def __repr__(self):
        result = ["Model:"]
        show_ids = lambda map: ", ".join(map.keys()) if map else "<none>"
        result.append(f"  Attributes: {show_ids(self._attributes)}")
        result.append(f"  Measures: {show_ids(self._measures)}")
        for namespace in self._namespaces:
            if namespace._identifier == "orders":
                continue
            namespace_repr = namespace.__repr__()
            indented_namespace_repr = "\n".join(
                f"  {l}" for l in namespace_repr.splitlines()
            )
            result.append(indented_namespace_repr)
        return "\n".join(result)

    # --- Adding Properties ---

    @builder_method
    def with_connection(self, connection: Connection) -> "Model":
        """
        Returns a new Model which runs inside of the provided Connection.
        If the model already had a connection attached, this overwrites it.
        """
        self._connection = connection

    @overload
    def with_source(self, table: str, schema: Optional[str] = None) -> "Model":
        """
        Returns a new Model which uses the provided table as the underlying
        data. If the model already had a source attached, this overwrites it.
        """
        ...

    @overload
    def with_source(self, *, sql_query: str) -> "Model":
        """
        Returns a new Model which uses the provided SQL query as the underlying
        table. If the model already had a source attached, this overwrites it.
        """
        ...

    @builder_method
    def with_source(self, table=None, *, schema=None, sql_query=None) -> "Model":
        """
        Returns a new Model which uses the provided content as the underlying
        table. If the model already had a source attached, this overwrites it.
        """
        self._source = (
            SqlTextSource(sql_query) if sql_query else TableNameSource(table, schema)
        )

    @builder_method
    @resolve_keypath_args_from(_.self)
    def with_attributes(
        self,
        *args: Union[ColumnExpression, str],
        **kwargs: Union[ColumnExpression, str],
    ) -> "Model":
        """
        Returns a new Model with the provided column expressions included as
        attributes, accessible via `attr.<name>`. If a string is passed, it will
        be interpreted as a column name (aka `column(str)`).
        """
        normalize = lambda c: (
            expr if isinstance(expr, ColumnExpression) else column(expr)
        )
        for expr in args:
            self._attributes.add(normalize(expr))
        for identifier, expr in kwargs.items():
            self._attributes.add(normalize(expr).named(identifier))

    @builder_method
    @resolve_keypath_args_from(_.self)
    def with_primary_key(self, expression: ColumnExpression) -> "Model":
        """
        Returns a new Model with the provided column expression configured as
        the primary key. This should be a unique value across all records in the
        source.
        """
        self._primary_key = expression

    @builder_method
    @resolve_keypath_args_from(_.self)
    def with_measures(
        self,
        *args: ColumnExpression,
        **kwargs: ColumnExpression,
    ) -> "Model":
        """
        Returns a new Model with the provided column expressions included as
        measure definitions, accessible via `msr.<name>`. This does not perform
        any aggregation on its own, this only attaches a definition for later
        use.
        """
        for expr in args:
            self._measures.add(expr)
        for identifier, expr in kwargs.items():
            self._measures.add(expr.named(identifier))

    @builder_method
    def with_join_one(
        self,
        joined: "Model",
        *,
        foreign_key: Optional[Union[ColumnExpression, KeyPath]] = None,
        condition: Optional[ColumnExpression] = None,
        named: Optional[Union[str, KeyPath]] = None,
        drop_unmatched: bool = False,
    ) -> "Model":
        """
        Returns a new Model with a new property which can be used to reference
        the properties of the `joined` Model. Records are aligned using `foreign_key`
        and/or `condition`. Attributes on joined relations can be referenced with
        `rel.<name>.<attr_name>`.

        Similar to `with_measures` and `with_attributes`, `with_join_one` has no
        performance cost on its own. No JOIN statement is added to queries
        unless the relation is actually referenced.

        If no records match, `NULL` values are filled in for the missing columns,
        unless `drop_unmatched=True` is passed.
        """
        # -- gather all the parameters up, resolve and validate --
        if foreign_key is None and condition is None:
            raise ValueError(
                "`.with_join_one` must specify a join condition using "
                + "`foreign_key=<foreign_key>` and/or `condition=<column_expression>`"
            )
        joined = resolve_keypath(self, joined)
        relation_name = unwrap_keypath_to_name(named)
        if not relation_name:
            if default_identifier := joined._source._default_identifier():
                relation_name = default_identifier
        if not relation_name:
            raise ValueError(
                "Join was not provided an identifier and a default could not be inferred. "
                + "Provide an explicit name for this relation using `named=`"
            )

        # -- form the namespace we're joining to --
        relation = ModelNamespace(identifier=relation_name, nested_model=joined)

        # -- determine the column expression to join with --
        join_predicate = None
        if foreign_key is not None:
            foreign_key: ColumnExpression = resolve_keypath(self, foreign_key)
            join_predicate = foreign_key == joined._primary_key.disambiguated(
                relation_name
            )
        # you can reference the relation in the `condition=` but not in
        # `foreign_key=` so add it to `_namespaces` here
        self._namespaces.add(relation)
        if condition is not None:
            condition = resolve_keypath(self, condition)
            join_predicate = (
                condition
                if not join_predicate
                else func.and_(join_predicate, condition)
            )
        if foreign_key and not condition:
            relation._through_foreign_key_attr = foreign_key

        # -- attach the final join source --
        self._source = JoinOneSource(
            base=self._source,
            relation=relation,
            join_condition=join_predicate,
            drop_unmatched=drop_unmatched,
        )

    @builder_method
    @resolve_keypath_args_from(_.self)
    def with_activity_schema(
        self,
        *,
        group: ColumnExpression,
        timestamp: ColumnExpression,
        event_key: ColumnExpression,
    ) -> "Model":
        """
        Returns a new Model configured for event analysis.

        Args:
            group:
                Used to split event sequences into distinct groups.
                Typically this is a single attribute, representing
                a unique value for each actor that invokes the event,
                such as `user_id` or `customer_id`.

            timestamp:
                Column used to order events.
                Typically this is a timestamp representing when the event
                was detected, such as `created_at` or `timestamp`.

            event_key:
                A column representing the name of the event.
                Typically this is a column like `event_name` or `event_type`.
        """
        self._activity_schema = ModelActivitySchema(
            group=group,
            timestamp=timestamp,
            event_key=event_key,
        )

    # --- Analysis ---

    @builder_method
    @resolve_keypath_args_from(_.self)
    def aggregate(
        self,
        *,
        measures: List[ColumnExpression] = None,
        groups: List[ColumnExpression] = None,
    ) -> "Model":
        """
        Returns a new model aggregated into `measures` split up by `groups`.
        Analogous to `SELECT *groups, *measures FROM ... GROUP BY *groups`.
        """
        measures: List[ColumnExpression] = measures or []
        groups: List[ColumnExpression] = groups or []
        self._source = AggregateSource(self._source, groups=groups, measures=measures)
        self._attributes = IdentifiableMap(
            column(c.identifier) for c in groups + measures
        )
        self._measures = IdentifiableMap()
        self._namespaces = IdentifiableMap()

    @builder_method
    @resolve_keypath_args_from(_.self)
    def match_steps(
        self,
        steps: List[Union[str, ColumnExpression, Tuple[str, str]]],
        *,
        group: Optional[ColumnExpression] = None,
        timestamp: Optional[ColumnExpression] = None,
        event_key: Optional[ColumnExpression] = None,
        partition_start_events: Optional[List[ColumnExpression]] = None,
        time_limit: Optional[timedelta] = None,
    ) -> "Model":
        """
        Returns a new Model with a new property that represents the records
        matched to a sequence of steps, aggregated by `group`.

        :arg steps: The sequence of steps to analyze. If a string is passed, it will be matched to values in the
                    `event_key` column. If a boolean column expression is passed, an event will be considered a match
                    if it passes the condition. A string step value can be a tuple to denote (step_value, output_name)
                    which can be used to provide unique names when matching the same step multiple times.
        :arg group: The column to group the analysis by. This is typically a unique identifier for a user/customer/etc.
        :arg timestamp: The temporal column to order the events by.
        :arg event_key: The column representing the name of the event.
        :arg partition_start_events: A list of column expressions to partition the funnel analysis by.
                                     These grouping expressions are applied only to the first event in the journey.
        :arg time_limit: The maximum time allowed between the initial step and any subsequent step.
                         If a user takes longer than this time for a given step, future steps are not matched.

        Useful for defining funnels, retention, or temporal joins.
        """
        # `self` pre-transformation is the events table we'll use as a base
        events_model = deepcopy(self)

        # normalize the activity schema, defaulting to what was modeled
        activity_schema = self._require_normalized_activity_schema(
            group, timestamp, event_key, "match_steps"
        )
        if not steps:
            raise ValueError("`match_steps` requires at least one step to match.")

        # Converts steps into a normalized list of column expressions
        step_conditions = normalize_steps(list(steps), activity_schema)

        # attach the source transform to build and attach step event data
        self._source = MatchStepsSource(
            base=self._source,
            activity_schema=activity_schema,
            steps=step_conditions,
            partition_start_events=partition_start_events,
            time_limit=time_limit,
        )

        # drop all namespaces except those which were joined exactly on our
        # `group` since that join will still be possible afterwards
        step_names = set(step.identifier for step in step_conditions)
        self._namespaces = IdentifiableMap[ModelNamespace](
            namespace
            for namespace in self._namespaces
            if namespace._identifier not in step_names  # favor steps
            and deep_equal(namespace._through_foreign_key_attr, activity_schema.group)
        )
        # reattach the joins, since they will have been consumed
        for preserved_joined_namespace in self._namespaces:
            Model.with_join_one.mutate(
                self,
                preserved_joined_namespace._nested_model,
                foreign_key=preserved_joined_namespace._through_foreign_key_attr,
                named=preserved_joined_namespace._identifier,
            )
        # add a new namespace for each step containing the attributes
        # on the events table, which the `MatchStepsSource` will generate
        for step in step_conditions:
            # self join on each step's identifier
            self._namespaces.add(ModelNamespace(step.identifier, events_model))

        # reset the attributes to only what will be available after transform
        self._attributes = IdentifiableMap([column(activity_schema.group.identifier)])
        self._attributes.add(
            # helper to get the last matched step
            func.cases(
                *[
                    (
                        activity_schema.timestamp.disambiguated(step.identifier)
                        != None,
                        step.identifier,
                    )
                    for step in reversed(step_conditions)
                ],
                other=None,
            ).named("last_matched_step_name")
        )
        self._attributes.add(
            func.cases(
                *[
                    (
                        activity_schema.timestamp.disambiguated(step.identifier)
                        != None,
                        len(step_conditions) - 1 - i,
                    )
                    for i, step in enumerate(reversed(step_conditions))
                ],
                other=None,
            ).named("last_matched_step_index")
        )
        for partition in partition_start_events or []:
            self._attributes.add(
                column(partition.identifier).named(partition.identifier)
            )
        self._primary_key = activity_schema.group  # best effort

        # reset the measures
        self._measures = IdentifiableMap()
        self._measures.add(func.count().named(FUNNEL_COUNT_COLUMN_NAME))
        for step in step_conditions:
            # helper to get the count of records which reached the step
            #
            # We check that the event timestamp is not NULL to verify this.
            # Checking entity/group or event_key is not sufficient, since
            # those could (correctly) have NULL values.
            step_id = step.identifier
            self._measures.add(
                func.count_if(
                    activity_schema.timestamp.disambiguated(step_id) != None
                ).named(f"{step_id}_count")
            )

        # the existing activity schema's properties have been consumed
        # and are no longer valid
        self._activity_schema = None

    def funnel(
        self,
        steps: List[Union[str, ColumnExpression, Tuple[str, str]]],
        *,
        group: Optional[ColumnExpression] = None,
        timestamp: Optional[ColumnExpression] = None,
        event_key: Optional[ColumnExpression] = None,
        time_limit: Optional[timedelta] = None,
        partition_start_events: Optional[List[ColumnExpression]] = None,
        partition_matches: Optional[List[ColumnExpression]] = None,
        top_of_funnel: Optional[Union[int, str]] = 0,
    ) -> "Model":
        """
        Returns a new Model which performs a funnel analysis on the
        provided sequence of steps.

        :arg steps: The sequence of steps to analyze. If a string is passed, it will be matched to values in the
                    `event_key` column. If a boolean column expression is passed, an event will be considered a match
                    if it passes the condition. A string step value can be a tuple to denote (step_value, output_name)
                    which can be used to provide unique names when matching the same step multiple times.
        :arg group: The column to group the funnel analysis by. This is typically a unique identifier for a user/customer/etc.
        :arg timestamp: The temporal column to order the events by.
        :arg event_key: The column representing the name of the event.
        :arg time_limit: The maximum time allowed between the initial funnel step and any subsequent step.
                         If a user takes longer than this time for a given step, they are not counted in the
                        funnel for that step.
        :arg partition_start_events: A list of column expressions to partition the first events in the funnel analysis by.
                                     This can result in single entities being evaluated and counted multiple times in the funnel
                                     analysis -- once per value of the partitions.
        :arg partition_matches: A list of column expressions to group a cohort of users together. The funnel
                                will split the aggregated counts of each step into separate groups for each of these expressions.
                                This can be used to further break out the entities flowing through the funnel.
        :arg top_of_funnel:
            Determines where and how the funnel "starts".
            If an index, the funnel begins at that step's index. All steps will be matched, this only affects the output table.
            If a string, the funnel includes an extra step which represents the count of all evaluated entities.
                The name will match the passed string.
            The default is `0`, meaning the funnel starts on the first step.

        Example::

            events # this is presorted only for clarity, it need not be sorted
            '''
            user_id     event                   timestamp
            ----------------------------------------------
            0           ad_impression           2024-01-01
            0           visit                   2024-01-02
            0           purchase                2024-01-04

            1           ad_impression           2024-01-01
            1           visit                   2024-01-02
            1           purchase                2024-01-03
            1           purchase                2024-01-04

            2           ad_impression           2024-01-01
            2           visit                   2024-01-02

            3           ad_impression           2024-01-01
            3           visit                   2024-01-02

            4           ad_impression           2024-01-01

            5           visit                   2024-01-01
            5           purchase                2024-01-02

            6           other_event             2024-01-01
            '''

            events.funnel(
                top_of_funnel="users",
                steps=["ad_impression", "visit", "purchase"]
            )
            '''
            step                 count
            ------------------------------
            users                7
            ad_impression        5
            visit                5
            purchase             2
            '''
            # `users` is 7 because there are 7 unique users.
            # `ad_impression` is 5 because of those 7 unique users, 5 of them saw an ad:
            #        This is users 0, 1, 2, 3, and 4.
            #        Users 5 and 6 did not see ads, so they are not included.
            # `visit` is 4 because of the 5 users who saw an ad, 4 of them went on to visit:
            #        This is users 0, 1, 2, and 3.
            #        User 5 visited, but not after seeing an ad, so they are not included in the funnel.
            # `purchase` is 2 because of the 4 users who saw an ad, then visited, 2 of them went on to purchase:
            #        This is users 0 and 1. User 1 made two purchases but is only counted once.
            #        User 5 purchased, but not after seeing an ad and visiting, so they are not included in the funnel.
        """
        # need to resolve these now, and `partition_matches` is resolved later. This is because
        # the keypaths for partition_matches are resolved via the model outputted by `match_steps`.
        steps = resolve_all_nested_keypaths(self, steps)
        group = resolve_all_nested_keypaths(self, group)
        timestamp = resolve_all_nested_keypaths(self, timestamp)
        event_key = resolve_all_nested_keypaths(self, event_key)
        time_limit = resolve_all_nested_keypaths(self, time_limit)
        partition_start_events = resolve_all_nested_keypaths(
            self, partition_start_events or []
        )

        # Validate the activity schema up front.
        activity_schema = self._require_normalized_activity_schema(
            group, timestamp, event_key, "funnel"
        )

        top_of_funnel_start_index = (
            top_of_funnel if type(top_of_funnel) is int else None
        )
        top_of_funnel_name = top_of_funnel if type(top_of_funnel) is str else "entities"
        if top_of_funnel_start_index is not None and top_of_funnel_start_index < 0:
            raise ValueError("Invalid `top_of_funnel` index. Cannot be negative.")
        if top_of_funnel_start_index is not None and top_of_funnel_start_index >= len(
            steps
        ):
            raise ValueError(
                "There are not enough steps in the funnel to filter the "
                + "funnel to the provided `top_of_funnel` index"
            )
        if type(top_of_funnel) is str and partition_start_events:
            raise ValueError(
                "Incompatible arguments: `top_of_funnel` cannot be a string when partitioning with `partition_start_events`."
            )

        if len(steps) == 0:
            # when there are no steps, return the "top of the funnel", which
            # is most efficiently calculated by taking a simple aggregate
            count_distinct_groups = func.count(
                func.distinct(activity_schema.group)
            ).named(FUNNEL_COUNT_COLUMN_NAME)
            return self.aggregate(
                measures=[
                    column(value=top_of_funnel_name).named("step"),
                    count_distinct_groups,
                ]
            )

        step_names = normalize_step_names(steps)
        matched = self.match_steps(
            steps,
            group=group,
            timestamp=timestamp,
            event_key=event_key,
            time_limit=time_limit,
            partition_start_events=partition_start_events,
        )
        partition_start_events_output_exprs = [
            column(expr.identifier) for expr in partition_start_events or []
        ]
        partition_matches = resolve_all_nested_keypaths(
            matched, partition_matches or []
        )
        all_partitions = partition_start_events_output_exprs + partition_matches

        aggregated = matched.aggregate(
            groups=all_partitions,
            measures=list(matched._measures),
        )
        all_step_value_columns = [
            column(FUNNEL_COUNT_COLUMN_NAME).named(top_of_funnel_name)
        ] + [column(f"{step_id}_count").named(step_id) for step_id in step_names]
        folded = aggregated.fold(
            ids=[column(g.identifier) for g in all_partitions],
            values=(
                all_step_value_columns
                if top_of_funnel_start_index is None
                else all_step_value_columns[(top_of_funnel_start_index + 1) :]
            ),
            key_name="step",
            value_name=FUNNEL_COUNT_COLUMN_NAME,
        )
        # We explicitly SELECT * here, even though it may not be strictly necessary in all cases.
        #
        # This is because the below sorting logic is done over the **output** names of the fold operation above.
        #
        # Ordering by output names directly is supported in all SQL dialects, but some do **not** support using those
        # output names in arbitrary expressions, such as CASE statements (which we use below).
        # TODO(HB-10765): we should revisit ORDER BY name resolution in Hashquery.
        #
        # To avoid this, we explicitly SELECT * here which will lift us into a new CTE, and then
        # we can safely sort against the output names of the fold operation (which are now input names).
        sorted = folded.pick(column(sql="*"))
        for p in all_partitions:
            sorted = sorted.sort(column(p.identifier))
        return sorted.sort(
            func.cases(
                *[
                    (column("step") == step_name, idx)
                    for idx, step_name in enumerate([top_of_funnel_name, *step_names])
                ],
                other=len(step_names) + 1,
            ).named("step_index"),
            dir="asc",
        )

    def funnel_conversion_rate(
        self,
        steps: List[Union[str, ColumnExpression, Tuple[str, str]]],
        *,
        group: Optional[ColumnExpression] = None,
        timestamp: Optional[ColumnExpression] = None,
        event_key: Optional[ColumnExpression] = None,
        time_limit: Optional[timedelta] = None,
        partition_start_events: Optional[List[ColumnExpression]] = None,
        partition_matches: Optional[List[ColumnExpression]] = None,
        # TODO(HB-9592): Support top_of_funnel_index.
    ) -> "Model":
        """
        Returns a new Model which performs a funnel analysis on the
        provided sequence of steps, and computes the conversion rate.

        :arg steps: The sequence of steps to analyze. If a string is passed, it will be matched to values in the
                    `event_key` column. If a boolean column expression is passed, an event will be considered a match
                    if it passes the condition. A string step value can be a tuple to denote (step_value, output_name)
                    which can be used to provide unique names when matching the same step multiple times.
        :arg group: The column to group the funnel analysis by. This is typically a unique identifier for a user/customer/etc.
        :arg timestamp: The temporal column to order the events by.
        :arg event_key: The column representing the name of the event.
        :arg time_limit: The maximum time allowed between the initial funnel step and any subsequent step.
                    If a user takes longer than this time for a given step, they are not counted in the
                funnel for that step.
        :arg partition_start_events: A list of column expressions to partition the first events in the funnel analysis by.
                                     This can result in single entities being evaluated and counted multiple times in the funnel
                                     analysis -- once per value of the partitions.
        :arg partition_matches: A list of named boolean conditions to group a cohort of users together. The funnel
                                will split the aggregated counts of each step into separate groups for each of these expressions.
                                This can be used to further break out the entities flowing through the funnel.
        """
        # need to resolve these now, and `partition_matches` is resolved later. This is because
        # the keypaths for partition_matches are resolved via the model outputted by `match_steps`.
        steps = resolve_all_nested_keypaths(self, steps)
        group = resolve_all_nested_keypaths(self, group)
        timestamp = resolve_all_nested_keypaths(self, timestamp)
        event_key = resolve_all_nested_keypaths(self, event_key)
        time_limit = resolve_all_nested_keypaths(self, time_limit)
        partition_start_events = (
            resolve_all_nested_keypaths(self, partition_start_events) or []
        )

        activity_schema = self._require_normalized_activity_schema(
            group, timestamp, event_key, "funnel_conversion_rate"
        )

        STARTED_COUNT_MEASURE_NAME = "startedCount"
        CONVERTED_COUNT_MEASURE_NAME = "convertedCount"
        AVG_SECONDS_TO_CONVERT_MEASURE_NAME = "avgSecondsToConvert"
        RATE_MEASURE_NAME = "rate"

        if len(steps) == 0:
            count_distinct_groups = func.count(func.distinct(activity_schema.group))
            result = self.aggregate(
                groups=(partition_start_events or []),
                measures=[
                    count_distinct_groups.named(STARTED_COUNT_MEASURE_NAME),
                    count_distinct_groups.named(CONVERTED_COUNT_MEASURE_NAME),
                    column(value=0).named(AVG_SECONDS_TO_CONVERT_MEASURE_NAME),
                    column(value=1).named(RATE_MEASURE_NAME),
                ],
            )
            for partition in partition_start_events or []:
                result = result.sort(column(partition.identifier), dir="asc")
            return result

        matched = self.match_steps(
            steps,
            group=group,
            timestamp=timestamp,
            event_key=event_key,
            time_limit=time_limit,
            partition_start_events=partition_start_events,
        )

        # Column expressions (attributes) for partitions to group by, after match_steps.
        partition_start_events_output_exprs = [
            column(expr.identifier) for expr in partition_start_events or []
        ]
        partition_matches = resolve_all_nested_keypaths(
            matched, partition_matches or []
        )

        # Rate measure.
        normalized_steps = normalize_steps(list(steps), activity_schema)

        first_step_timestamp = activity_schema.timestamp.disambiguated(
            normalized_steps[0].identifier
        )
        last_step_timestamp = activity_schema.timestamp.disambiguated(
            normalized_steps[-1].identifier
        )

        rate_measure = (
            # If the last step has a timestamp (e.g. was matched), then we count this journey in the
            # numerator of the rate.
            func.count_if(
                last_step_timestamp != None,
            )
            / func.count_if(first_step_timestamp != None)
        ).named(RATE_MEASURE_NAME)

        avg_seconds_to_convert_measure = func.avg(
            func.diff_seconds(last_step_timestamp, first_step_timestamp)
        ).named(AVG_SECONDS_TO_CONVERT_MEASURE_NAME)
        started_count_measure = func.count_if(first_step_timestamp != None).named(
            STARTED_COUNT_MEASURE_NAME
        )
        converted_count_measure = func.count_if(last_step_timestamp != None).named(
            CONVERTED_COUNT_MEASURE_NAME
        )

        all_partitions = partition_start_events_output_exprs + partition_matches

        # Since we're computing a rate based on users that entered the funnel,
        # we filter out rows for entities that did not enter the funnel.
        matched = matched.filter(
            activity_schema.timestamp.disambiguated(normalized_steps[0].identifier)
            != None
        )

        # Aggregate, grouping by the start-event partitions and recording rate, then sort by the start-event partitions in order.
        res = matched.aggregate(
            groups=all_partitions,
            measures=[
                rate_measure,
                started_count_measure,
                converted_count_measure,
                avg_seconds_to_convert_measure,
            ],
        )
        for expr in all_partitions:
            res = res.sort(expr, dir="asc")
        return res

    # --- Record Management ---

    @builder_method
    @resolve_keypath_args_from(_.self)
    def pick(self, *columns: ColumnExpression) -> "Model":
        """
        Returns a new Model with only the included attributes.
        """
        self._source = PickSource(self._source, columns)
        self._attributes = IdentifiableMap(
            column(c.identifier) for c in columns if not c._is_star
        )
        self._namespaces = IdentifiableMap()
        # we might want to preserve measures if we can inspect them
        # and confirm they only rely on selected columns (?)
        self._measures = IdentifiableMap()

    @builder_method
    @resolve_keypath_args_from(_.self)
    def filter(self, condition: ColumnExpression) -> "Model":
        """
        Returns a new Model with records filtered to only those which
        match the given condition.
        """
        self._source = FilterSource(self._source, condition)

    @builder_method
    @resolve_keypath_args_from(_.self)
    def sort(
        self,
        sort: ColumnExpression,
        dir: Literal["asc", "desc"] = "asc",
        nulls: Literal["auto", "first", "last"] = "auto",
    ) -> "Model":
        """
        Returns a new Model with records ordered by the provided column.
        Sort direction `dir` can be either "asc" or "desc" (defaults to "asc").
        Nulls ordering `nulls` can be "first", "last", or "auto" ("first" when ascending, "last" when descending) (defaults to "auto")
        """
        self._source = SortSource(self._source, sort, dir, nulls)

    @builder_method
    @resolve_keypath_args_from(_.self)
    def limit(self, count: int, *, offset: Optional[int] = 0) -> "Model":
        """
        Returns a new Model with only the first N records.
        """
        self._source = LimitSource(self._source, count, offset=offset)

    @builder_method
    @resolve_keypath_args_from(_.self)
    def union_all(self, other: "Model") -> "Model":
        """
        Returns a new model with its records merged with another Model,
        using a sql UNION ALL operator. This always results in a new CTE,
        so any joins will be _flattened_.

        The columns of each model must exactly align -- if they do not, you
        may want to use `.pick` on one or both models to reduce them to
        a series of columns which do.
        """
        self._source = UnionSource(self._source, other._source)
        # sadly UNION ALL requires us to open a new query layer,
        # so relations are lost
        self._namespaces = IdentifiableMap()

    @resolve_keypath_args_from(_.self)
    def fold(
        self,
        ids: List[ColumnExpression],
        values: List[ColumnExpression],
        key_name: Optional[str] = "key",
        value_name: Optional[str] = "value",
    ) -> "Model":
        """
        Transforms a N-separate column expressions into a key/value pair,
        where the `key_name` column becomes the name of the input column
        expression and `value_name` becomes the result value. All `values`
        expressions must have the same type. The `ids` columns are untouched.

        This multiplies the count of records by `len(values)`.
        This removes any attribute not specified in `ids` or `values`.

        This can be used to "unpivot" or "melt" a dataset, from a wide format
        to a long format.

        For example::

            sales
            '''
            year        income       expenses
            ---------------------------------
            2023        $150K        $30K
            2024        $500K        $130K
            '''

            sales.fold(
                ids=[a.year],
                values=[a.income, a.expenses],
                key_name="type",
                value_name="value"
            )
            '''
            year        type         value
            ------------------------------
            2023        income       $150K
            2024        income       $500K
            2023        expenses     $30K
            2024        expenses     $130K
            '''
        """

        def pick_key_value_pair(value_expr: ColumnExpression):
            return self.pick(
                *ids,
                column(value=value_expr.identifier).named(key_name),
                value_expr.named(value_name),
            )

        first_value_expr, *union_value_exprs = values
        result = pick_key_value_pair(first_value_expr)
        for value_expr in union_value_exprs:
            result = result.union_all(pick_key_value_pair(value_expr))
        return result

    # --- Execution ---

    def run(
        self,
        *,
        freshness: Optional[Union[datetime, Literal["latest"]]] = None,
        print_warnings: bool = True,
        print_exec_stats: bool = False,
    ):
        """
        Fetches the final table for the model, returning a `RunResults`
        structure which contains the executed sql query, the results, and
        additional metadata.
        """
        return run(
            self,
            freshness=freshness,
            print_warnings=print_warnings,
            print_exec_stats=print_exec_stats,
        )

    def df(
        self,
        *,
        freshness: Optional[Union[datetime, Literal["latest"]]] = None,
        print_warnings: bool = True,
        print_exec_stats: bool = False,
    ):
        """
        Fetches the final table for the model as a pandas' dataframe.
        This compiles and runs a query within the model's database, and returns
        an object which can be used to view result rows and query metadata.
        """
        return self.run(
            freshness=freshness,
            print_warnings=print_warnings,
            print_exec_stats=print_exec_stats,
        ).df

    def sql(
        self,
        *,
        freshness: Optional[Union[datetime, Literal["latest"]]] = None,
        print_warnings: bool = True,
    ):
        """
        Compiles the SQL that would be run if you executed this Model with
        `run` and returns it as a string. Nothing will be sent to the database.

        The returned SQL string will not include parameterization, and so it may
        be prone to SQL injection if you were to execute it directly. If your
        intent is to execute this query, use `.run` or `.df` instead.
        """
        return run(
            self,
            sql_only=True,
            freshness=freshness,
            print_warnings=print_warnings,
            print_exec_stats=False,
        ).sql_query

    # --- Custom Meta ---

    @builder_method
    @resolve_keypath_args_from(_.self)
    def with_custom_meta(self, name: str, value: Any) -> "Model":
        """
        Returns a new Model with the custom metadata attached. Hashquery will
        never read or write to this key, making it a good spot to put any
        custom configuration or encode semantic information about the Model
        which you want to use.
        """
        self._custom_meta[name] = value

    def get_custom_meta(self, name: str):
        """
        Returns a value from the custom metadata dictionary for this model,
        or `None` if the key does not exist. You can set custom metadata
        properties using `.with_custom_meta()`.
        """
        return self._custom_meta.get(name)

    # --- Utilities ---

    def _require_normalized_activity_schema(
        self,
        group: Optional[ColumnExpression],
        timestamp: Optional[ColumnExpression],
        event_key: Optional[ColumnExpression],
        fn_name: str,
    ):
        activity_schema = (
            ModelActivitySchema(group=group, timestamp=timestamp, event_key=event_key)
            if (group and timestamp and event_key)
            else self._activity_schema
        )
        if not activity_schema:
            raise ValueError(
                f"`{fn_name}` requires the model to have an activity schema defined. "
                + "Use `.with_activity_schema` to define the schema upstream, "
                + f"or fully qualify `group`, `timestamp` and `event_key` in the call to `{fn_name}`"
            )
        return activity_schema

    # --- Serialization ---

    def _to_wire_format(self) -> dict:
        return {
            "type": "model",
            "connection": self._connection._to_wire_format(),
            "source": self._source._to_wire_format(),
            "attributes": [a._to_wire_format() for a in self._attributes],
            "measures": [m._to_wire_format() for m in self._measures],
            "namespaces": [n._to_wire_format() for n in self._namespaces],
            "primaryKey": self._primary_key._to_wire_format(),
            "activitySchema": (
                self._activity_schema._to_wire_format()
                if self._activity_schema
                else None
            ),
            "customMeta": self._custom_meta,
            "linkedResource": (
                self._linked_resource._to_wire_format()
                if self._linked_resource
                else None
            ),
        }

    @classmethod
    def _from_wire_format(cls, wire: dict):
        assert wire["type"] == "model"
        result = Model()
        result._connection = Connection._from_wire_format(wire["connection"])
        result._source = Source._from_wire_format(wire["source"])
        result._attributes = IdentifiableMap(
            ColumnExpression._from_wire_format(a) for a in wire.get("attributes", [])
        )
        result._measures = IdentifiableMap(
            ColumnExpression._from_wire_format(m) for m in wire.get("measures", [])
        )
        result._namespaces = IdentifiableMap(
            ModelNamespace._from_wire_format(n) for n in wire.get("namespaces", [])
        )
        result._primary_key = ColumnExpression._from_wire_format(wire["primaryKey"])
        result._activity_schema = (
            ModelActivitySchema._from_wire_format(wire["activitySchema"])
            if wire.get("activitySchema")
            else None
        )
        result._custom_meta = wire.get("customMeta", {})
        result._linked_resource = (
            LinkedResource._from_wire_format(wire["linkedResource"])
            if wire["linkedResource"]
            else None
        )
        return result
