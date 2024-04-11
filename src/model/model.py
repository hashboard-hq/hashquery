from typing import *
from copy import deepcopy
from datetime import datetime

from ..utils.builder import builder_method
from ..utils.serializable import Serializable
from ..utils.keypath import (
    _,
    resolve_keypath_args_from,
    resolve_keypath,
    unwrap_keypath_to_name,
    KeyPath,
)
from ..utils.keypath.keypath_ctx import KeyPathCtx
from ..utils.keypath.keypath import KeyPathComponentProperty
from ..utils.identifiable import IdentifiableMap
from ..utils.resource import LinkedResource
from .data_connection import DataConnection
from .column_expression import ColumnExpression
from .column import column
from .. import func
from .source import (
    Source,
    TableNameSource,
    SqlTextSource,
    AggregateSource,
    FilterSource,
    PickSource,
    SortSource,
    LimitSource,
    JoinOneSource,
    MatchStepsSource,
    UnionSource,
)
from .namespace import ModelNamespace
from .activity_schema import ModelActivitySchema
from ..run.post_run_endpoint import post_run_endpoint


class Model(Serializable):
    def __init__(self) -> None:
        """
        Initializes a new empty Model.

        This Model begins with absolutely nothing in it. To attach a root data
        frame (such as a `table`), your likely want to call `.with_data_source()`
        soon-after.
        """
        self._connection: DataConnection = None
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
        did_you_mean = (
            lambda accessor, id: f"Did you mean `{accessor}.{id}`?"
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
                if isinstance(keypath_ctx, KeyPathCtx)
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

    @property
    def _project_id(self):
        if pid := (self._linked_resource and self._linked_resource.project_id) or (
            self._connection and self._connection.project_id
        ):
            return pid
        return None

    # --- Adding Properties ---

    @overload
    def with_source(
        self,
        connection: DataConnection,
        table_name: str,
        schema: Optional[str] = None,
    ) -> "Model":
        """
        Returns a new Model which uses the provided table (and optionally a
        schema) as the underlying data. Names will be escaped according to the
        database dialect.
        """
        ...

    @overload
    def with_source(
        self,
        connection: DataConnection,
        *,
        sql_query: str,
    ) -> "Model":
        """
        Returns a new Model which uses the provided SQL query as the underlying
        table. This query is not escaped or touched by the compiler.
        """
        ...

    @builder_method
    def with_source(
        self,
        connection: DataConnection,
        table_name: Optional[str] = None,
        schema: Optional[str] = None,
        *,
        sql_query: Optional[str] = None,
    ) -> "Model":
        """
        Returns a new Model with the provided data source as the underlying
        table.

        If the receiver already had a source attached, this overwrites that
        reference, including any transformations to it.
        """
        self._connection = connection
        self._source = (
            SqlTextSource(sql_query)
            if sql_query
            else TableNameSource(table_name, schema)
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
        attributes, accessible via `a.<name>`. If a string is passed, it will
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
        measure definitions, accessible via `m.<name>`. This does not perform
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
            condition_predicate = resolve_keypath(self, condition)
            join_predicate = (
                condition_predicate
                if not join_predicate
                else join_predicate & condition_predicate
            )

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
        *steps: str,
        group: Optional[ColumnExpression] = None,
        timestamp: Optional[ColumnExpression] = None,
        event_key: Optional[ColumnExpression] = None,
    ) -> "Model":
        """
        Returns a new Model with a new property that represents the records
        matched to a sequence of steps, aggregated by `group`.

        Useful for defining funnels, retention, or temporal joins.
        """
        # `self` pre-transformation is the events table we'll use as a base
        events_model = deepcopy(self)

        # normalize the activity schema, defaulting to what was modeled
        activity_schema = (
            ModelActivitySchema(group=group, timestamp=timestamp, event_key=event_key)
            if (group and timestamp and event_key)
            else self._activity_schema
        )
        if not activity_schema:
            raise ValueError(
                "`match_steps` requires the model to have an activity schema defined. "
                + "Use `.with_activity_schema` to define the schema upstream, "
                + "or fully qualify `group`, `timestamp` and `event_key` in the call to `match_steps`"
            )

        # attach the source transform to build and attach step event data
        self._source = MatchStepsSource(
            base=self._source,
            activity_schema=activity_schema,
            steps=steps,
        )

        # add a new namespace for each step containing the attributes
        # on the events table, which the `MatchStepsSource` will generate
        self._namespaces = IdentifiableMap[ModelNamespace]()
        for step in steps:  # self join on each step's name
            self._namespaces.add(ModelNamespace(step, events_model))

        # reset the attributes to only what will be available after transform
        self._attributes = IdentifiableMap([column(activity_schema.group.identifier)])
        self._attributes.add(
            # helper to get the last matched step
            func.cases(
                *[
                    (
                        activity_schema.event_key.disambiguated(step) != None,
                        step,
                    )
                    for step in reversed(steps)
                ],
                other=None,
            ).named("last_matched_step_name")
        )
        self._primary_key = activity_schema.group  # best effort

        # reset the measures
        self._measures = IdentifiableMap()
        self._measures.add(func.count())
        for step in steps:
            # helper to get the count of records which reached the step
            self._measures.add(
                func.count_if(
                    activity_schema.event_key.disambiguated(step) != None
                ).named(f"{step}_count")
            )

        # the existing activity schema's properties have been consumed
        # and are no longer valid
        self._activity_schema = None

    @resolve_keypath_args_from(_.self)
    def funnel(
        self,
        *steps: str,
        group: Optional[ColumnExpression] = None,
        timestamp: Optional[ColumnExpression] = None,
        event_key: Optional[ColumnExpression] = None,
    ) -> "Model":
        """
        Returns a new Model which performs a funnel analysis on the
        provided sequence of steps.

        For example::

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

            events.funnel("ad_impression", "visit", "purchase")
            '''
            step                 count
            ------------------------------
            count                7
            ad_impression        5
            visit                5
            purchase             2
            '''
            # `count` is 7 because there are 7 unique users.
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
        matched = self.match_steps(
            *steps,
            group=group,
            timestamp=timestamp,
            event_key=event_key,
        )
        aggregated = matched.aggregate(groups=[], measures=list(matched._measures))
        folded = aggregated.fold(
            ids=[],
            values=[column("count")]
            + [column(f"{step}_count").named(step) for step in steps],
            key_name="step",
            value_name="count",
        )
        return folded.sort(column("count") * -1)

    # --- Record Management ---

    @builder_method
    @resolve_keypath_args_from(_.self)
    def pick(self, *columns: ColumnExpression) -> "Model":
        """
        Returns a new Model with only the included attributes.
        """
        self._source = PickSource(self._source, columns)
        self._attributes = IdentifiableMap(column(c.identifier) for c in columns)
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
        self, sort: ColumnExpression, dir: Literal["asc", "desc"] = "asc"
    ) -> "Model":
        """
        Returns a new Model with records ordered by the provided column.
        Sort direction `dir` can be either "asc" or "desc" (defaults to "asc").
        """
        self._source = SortSource(self._source, sort, dir)

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
        ids: ColumnExpression,
        values: ColumnExpression,
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
        return post_run_endpoint(
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
        return post_run_endpoint(
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

    # --- Serialization ---

    def to_wire_format(self) -> dict:
        return {
            "type": "model",
            "connection": self._connection.to_wire_format(),
            "source": self._source.to_wire_format(),
            "attributes": [a.to_wire_format() for a in self._attributes],
            "measures": [m.to_wire_format() for m in self._measures],
            "namespaces": [n.to_wire_format() for n in self._namespaces],
            "primaryKey": self._primary_key.to_wire_format(),
            "activitySchema": (
                self._activity_schema.to_wire_format()
                if self._activity_schema
                else None
            ),
            "customMeta": self._custom_meta,
            "linkedResource": (
                self._linked_resource.to_wire_format()
                if self._linked_resource
                else None
            ),
        }

    @classmethod
    def from_wire_format(cls, wire: dict):
        assert wire["type"] == "model"
        result = Model()
        result._connection = DataConnection.from_wire_format(wire["connection"])
        result._source = Source.from_wire_format(wire["source"])
        result._attributes = IdentifiableMap(
            ColumnExpression.from_wire_format(a) for a in wire.get("attributes", [])
        )
        result._measures = IdentifiableMap(
            ColumnExpression.from_wire_format(m) for m in wire.get("measures", [])
        )
        result._namespaces = IdentifiableMap(
            ModelNamespace.from_wire_format(n) for n in wire.get("namespaces", [])
        )
        result._primary_key = ColumnExpression.from_wire_format(wire["primaryKey"])
        result._activity_schema = (
            ModelActivitySchema.from_wire_format(wire["activitySchema"])
            if wire.get("activitySchema")
            else None
        )
        result._custom_meta = wire.get("customMeta", {})
        result._linked_resource = (
            LinkedResource.from_wire_format(wire["linkedResource"])
            if wire["linkedResource"]
            else None
        )
        return result
