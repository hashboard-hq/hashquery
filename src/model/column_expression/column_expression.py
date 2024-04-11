from abc import ABC, abstractmethod
import datetime
from typing import *

from ...utils.types import is_iterable
from ...utils.builder import builder_method
from ...utils.timeinterval import timeinterval
from ...utils.keypath import KeyPath, defer_keypath_args, unwrap_keypath_to_name
from ...utils.serializable import Serializable

if TYPE_CHECKING:
    from ..model import Model
    from ..namespace import ModelNamespace


class ColumnExpression(Serializable, ABC):
    def __init__(self) -> None:
        super().__init__()
        self._manually_set_identifier: Optional[str] = None

    # --- Identifier Management ---

    @abstractmethod
    def default_identifier(self) -> Optional[str]:
        ...

    @builder_method
    def named(self, name: Union[str, KeyPath]) -> "ColumnExpression":
        """
        Forms a copy of this column expression with a new name.
        The name of a column expression will be the identifier when attached
        to a Model with `with_attribute` or `with_measure`. When queried, this
        will be analogous to an `AS <name>` clause.
        """
        name = unwrap_keypath_to_name(name)
        self._manually_set_identifier = name

    @property
    def identifier(self) -> str:
        found_id = self._optional_identifier
        if not found_id:
            raise AssertionError(
                f"{self} has no identifier and a default cannot be determined. "
                + "Call `.named()` to provide an identifier for this expression."
            )
        return found_id

    @property
    def _optional_identifier(self) -> bool:
        return self._manually_set_identifier or self.default_identifier()

    # --- Scoping ---

    @abstractmethod
    @builder_method
    def disambiguated(
        self, namespace: Union["ModelNamespace", str]
    ) -> "ColumnExpression":
        """
        Some column expressions require scoping to a namespace, to avoid
        conflicting names. For example, a `column("id")` expression refers to
        different values when qualified with one table `sales.id` vs. another
        `customers.id`, despite being otherwise the same definition.

        `.disambiguated` can be used to identify what namespace a given column
        expression needs to be qualified with.

        If a column expression has not had `disambiguated` applied, it may
        still appear fully qualified in the final query, typically scoped to
        the namespace of the model being invoked (ie. the contents of the
        `FROM` clause).
        """
        ...

    # --- Serialization ---

    # required by all concrete subclasses
    __TYPE_KEY__ = None

    def __init_subclass__(cls) -> None:
        super().__init_subclass__()
        type_key = cls.__TYPE_KEY__
        if not type_key:
            return
        elif type_key in COLUMN_EXPRESSION_TYPE_KEY_REGISTRY:
            raise AssertionError(
                "Multiple ColumnExpression subclasses for same type key: " + type_key
            )
        COLUMN_EXPRESSION_TYPE_KEY_REGISTRY[type_key] = cls

    def to_wire_format(self) -> Any:
        return {
            "type": "columnExpression",
            "subType": self.__TYPE_KEY__,
            "manuallySetIdentifier": self._manually_set_identifier,
            # `__denormalized` contains content that non-Python consumers may
            # need, but which a Py consumer should never read off, since this
            # data is derivable from the instance's methods. These values should
            # not be read inside of `from_wire_format`.
            "__denormalized": {"identifier": self._optional_identifier},
        }

    @classmethod
    def from_wire_format(cls, wire: dict) -> "ColumnExpression":
        assert wire["type"] == "columnExpression"
        type_key = wire["subType"]
        ColumnExpressionType = COLUMN_EXPRESSION_TYPE_KEY_REGISTRY.get(type_key)
        if not ColumnExpressionType:
            raise AssertionError("Unknown ColumnExpression type key: " + type_key)
        return ColumnExpressionType.from_wire_format(wire)

    def _from_wire_format_shared(self, wire: dict) -> "ColumnExpression":
        self._manually_set_identifier = wire["manuallySetIdentifier"]
        return self

    # --- Chained Transforms ---

    # - Granularity -

    def by_granularity(self, granularity: str) -> "ColumnExpression":
        from .granularity import GranularityColumnExpression

        return GranularityColumnExpression(self, granularity)

    @property
    def by_second(self) -> "ColumnExpression":
        """
        Truncate the target time column to the containing second.
        """
        return self.by_granularity("second")

    @property
    def by_minute(self) -> "ColumnExpression":
        """
        Truncate the target time column to the containing minute.
        """
        return self.by_granularity("minute")

    @property
    def by_hour(self) -> "ColumnExpression":
        """
        Truncate the target time column to the containing hour.
        """
        return self.by_granularity("hour")

    @property
    def by_day(self) -> "ColumnExpression":
        """
        Truncate the target time column to the containing day.
        """
        return self.by_granularity("day")

    @property
    def by_week(self) -> "ColumnExpression":
        """
        Truncate the target time column to the containing week.
        Sunday is considered the start of the week by default, though this can
        be changed in the Hashboard project's settings.
        """
        return self.by_granularity("week")

    @property
    def by_month(self) -> "ColumnExpression":
        """
        Truncate the target time column to the containing month.
        """
        return self.by_granularity("month")

    @property
    def by_quarter(self) -> "ColumnExpression":
        """
        Truncate the target time column to the containing quarter.
        """
        return self.by_granularity("quarter")

    @property
    def by_year(self) -> "ColumnExpression":
        """
        Truncate the target time column to the containing year.
        """
        return self.by_granularity("year")

    @property
    def is_today(self) -> "ColumnExpression":
        """Filters the target time column to the current day."""
        from ...func import now

        return (self >= now().by_day) & (
            self < now().by_day + datetime.timedelta(days=1)
        )

    @property
    def is_yesterday(self) -> "ColumnExpression":
        """Filters the target time column to yesterday."""
        from ...func import now

        return (self >= now().by_day - datetime.timedelta(days=1)) & (
            self < now().by_day
        )

    @property
    def is_this_week(self) -> "ColumnExpression":
        """Filters the target time column to the current week. Note that weeks start on Sunday."""
        from ...func import now

        return self._is_between_timestamps(
            now().by_week,
            now().by_week + datetime.timedelta(days=7),
        )

    @property
    def is_last_week(self) -> "ColumnExpression":
        """Filters the target time column to the previous week.  Note that weeks start on Sunday."""
        from ...func import now

        return self._is_between_timestamps(
            now().by_week - datetime.timedelta(days=7),
            now().by_week,
        )

    @property
    def is_this_month(self) -> "ColumnExpression":
        """Filters the target time column to the current month."""
        from ...func import now

        return self._is_between_timestamps(
            now().by_month,
            now().by_month + timeinterval(unit="months", num=1),
        )

    @property
    def is_last_month(self) -> "ColumnExpression":
        """Filters the target time column to the previous month."""
        from ...func import now

        return self._is_between_timestamps(
            now().by_month - timeinterval(unit="months", num=1),
            now().by_month,
        )

    @property
    def is_this_quarter(self) -> "ColumnExpression":
        """Filters the target time column to the current quarter."""
        from ...func import now

        return self._is_between_timestamps(
            now().by_quarter,
            now().by_quarter + timeinterval(unit="months", num=3),
        )

    @property
    def is_last_quarter(self) -> "ColumnExpression":
        """Filters the target time column to the previous quarter."""
        from ...func import now

        return self._is_between_timestamps(
            now().by_quarter - timeinterval(unit="months", num=3),
            now().by_quarter,
        )

    @property
    def is_this_year(self) -> "ColumnExpression":
        """Filters the target time column to the previous quarter."""
        from ...func import now

        return self._is_between_timestamps(
            now().by_year,
            now().by_year + timeinterval(unit="years", num=1),
        )

    @property
    def is_last_year(self) -> "ColumnExpression":
        """Filters the target time column to the previous quarter."""
        from ...func import now

        return self._is_between_timestamps(
            now().by_year - timeinterval(unit="years", num=1),
            now().by_year,
        )

    def _is_between_timestamps(
        self,
        start: Union[datetime.datetime, datetime.date],
        end: Union[datetime.datetime, datetime.date],
    ):
        return (self >= start) & (self < end)

    # - Bucketing / Breakout -

    def bucket_other(
        self,
        *buckets: Union["Model", Any],
        other: Any = "Other",
    ) -> "ColumnExpression":
        """
        Coerces any values for the target column not in `buckets` into the `other` value.
        """
        from ..model import Model
        from .cases import CasesColumnExpression
        from .py_value import PyValueColumnExpression

        if not isinstance(other, ColumnExpression):
            other = PyValueColumnExpression(other)

        is_model = lambda i: isinstance(i, Model)
        model_items = [i for i in buckets if is_model(i)]
        literal_items = [i for i in buckets if not is_model(i)]
        cases = []
        if literal_items:
            cases.append((self._binary_op(literal_items, "IN"), self))
        for model_item in model_items:
            cases.append((self.in_(model_item), self))
        return CasesColumnExpression(
            cases,
            other=other,
        ).named(self._optional_identifier)

    # - Containment -

    """
    Containment APIs open up is a fairly large matrix of potential operations:

    Implemented:
        str_column.contains(str) --> ColumnExpression:

        array_column.contains(value) --> ColumnExpression:
            Array containment checks.

        column.in_(str) --> ColumnExpression:
            Substring matching `LIKE` and `ILIKE`.

        column.in_(array) --> ColumnExpression:
            Check is a value is within a given list of literal values.

        column.in_(model) --> ColumnExpression:
            Similar to the `in_(array)`, but the list of values is gathered
            dynamically from one of the columns on the target model.

        column.contains(other_column) --> ColumnExpression:
        column.in_(other_column) --> ColumnExpression:
            Functions as the str implementations but for dynamic inputs.

    Not Planned:
      model.contains(column) --> ColumnExpression:
        This at first seems like a reasonable API; it's the inverse of
        `column.in_(model)`. It is rejected because it encourages users to
        reference a model within query expressions; the `model` they reference
        within a column context may not be not the reference they mean.
        Columns don't have this problem since they are lazily referenced
        through the `attr` KeyPath.

      Making these APIs accessible via the Python `in` keyword:
        Python has several restrictions on how types can override this behavior
        which prevent us from implementing it consistently for the matrix above.
        More info: https://gist.github.com/glean-charlie/2688a77268cb7d98dd89dad069eb8a2a
    """

    @overload
    def in_(
        self,
        substr: str,
        /,
        *,
        case_sensitive: bool = True,
    ) -> "ColumnExpression":
        """
        Returns a new `ColumnExpression` which is True for records where
        this column's value is fully contained within the given `other`
        substring, else False. See `contains()` for the inverse.
        """
        ...

    @overload
    def in_(
        self,
        iterable: Iterable[Any],
        /,
        *,
        case_sensitive: bool = True,
    ) -> "ColumnExpression":
        """
        Returns a new `ColumnExpression` which is True for records where
        this column's value is one of the values in the provided list,
        else False.
        """
        ...

    @overload
    def in_(
        self,
        model: "Model",
        /,
        *,
        case_sensitive: bool = True,
    ) -> "ColumnExpression":
        """
        Returns a new `ColumnExpression` which is True for records where
        this column's value is inside of the results for the given model.
        """
        ...

    @overload
    def in_(
        self,
        other: "ColumnExpression",
        /,
        *,
        case_sensitive: bool = True,
    ) -> "ColumnExpression":
        """
        Returns a new `ColumnExpression` which is True for records where
        this column's value is contained within the given `other` column
        expression.
        """
        ...

    def in_(
        self,
        other: Union[str, Iterable[Any], "Model", "ColumnExpression"],
        /,
        *,
        case_sensitive: Optional[bool] = True,
    ) -> "ColumnExpression":
        """
        Returns a new `ColumnExpression` which is True for records where
        this column's value is contained within the given `other` value,
        else False.

        This method can accept strings for substring checking, iterables
        for checking if a value is in a given set, or Models for checking
        if a column is inside of a dynamically collected list of values.
        """
        from .py_value import PyValueColumnExpression
        from .column_name import ColumnNameColumnExpression
        from .subquery_expression import SubqueryColumnExpression
        from ...func import distinct
        from ..model import Model

        def check_no_case_insensitive(type: str):
            if not case_sensitive:
                ValueError(
                    "Option `case_sensitive=False` is not yet supported when "
                    + f"using `ColumnExpression.in_({type})`"
                )

        if type(other) == str:
            return PyValueColumnExpression(other).contains(
                self, case_sensitive=case_sensitive
            )

        elif type(other) == Model:
            check_no_case_insensitive("Model")
            target_column = (
                (
                    # an attribute matching our name
                    other._attributes.get(self.identifier)
                    if self._optional_identifier
                    else None
                )
                # else assume there's a matching physical column
                or ColumnNameColumnExpression(self.identifier)
            )
            target_model = other.pick(
                distinct(target_column).named(target_column.identifier)
            )
            return self._binary_op(
                SubqueryColumnExpression(target_model),
                "IN",
            )

        elif isinstance(other, ColumnExpression):
            check_no_case_insensitive("ColumnExpression")
            return self._binary_op(other, "IN")

        elif is_iterable(other):
            check_no_case_insensitive("Iterable")
            return self._binary_op(list(other), "IN")

        else:
            raise ValueError(
                f"Cannot perform `ColumnExpression.in_()` with type: {type(other)}"
            )

    def contains(
        self,
        value: Union[Any, "ColumnExpression"],
        /,
        *,
        case_sensitive: Optional[bool] = True,
    ) -> "ColumnExpression":
        """
        Returns a new `ColumnExpression` which is True for records where this column contains the following value.

        For strings, this is a substring match, with the optional `case_sensitive` setting.

        For arrays, this checks if the array contains the given value. `case_sensitive` is not currently supported for arrays.
        """
        from .py_value import PyValueColumnExpression

        if not isinstance(value, ColumnExpression):
            value = PyValueColumnExpression(value)

        return value._binary_op(
            self,
            "IN",
            {
                "case_sensitive": case_sensitive,
            },
        )

    def contains_any(
        self,
        *values: Union[Any, "ColumnExpression"],
    ) -> "ColumnExpression":
        """
        Returns a new ColumnExpression which is True when this column contains any of the given values.
        """
        from ... import func

        return func.or_(*[self.contains(value) for value in values])

    def contains_all(
        self,
        *values: Union[Any, "ColumnExpression"],
    ) -> "ColumnExpression":
        """
        Returns a new ColumnExpression which is True when this column contains all of the given values.
        """
        from ... import func

        return func.and_(*[self.contains(value) for value in values])

    # - Operators -

    @defer_keypath_args
    def _binary_op(
        self,
        other: object,
        op: str,
        options: Optional[Dict[str, Union[str, bool]]] = None,
    ) -> "ColumnExpression":
        from ..model import Model
        from .binary_op import BinaryOpColumnExpression
        from .py_value import PyValueColumnExpression

        if isinstance(other, Model):
            other = other.as_scalar_column_expression()
        elif not isinstance(other, ColumnExpression):
            other = PyValueColumnExpression(other)
        return BinaryOpColumnExpression(self, other, op, options)

    def __eq__(self, other: object):
        return self._binary_op(other, "=")

    def __ne__(self, other: object):
        return self._binary_op(other, "!=")

    def __lt__(self, other: object):
        return self._binary_op(other, "<")

    def __le__(self, other: object):
        return self._binary_op(other, "<=")

    def __gt__(self, other: object):
        return self._binary_op(other, ">")

    def __ge__(self, other: object):
        return self._binary_op(other, ">=")

    def __add__(self, other: object):
        return self._binary_op(other, "+")

    def __sub__(self, other: object):
        return self._binary_op(other, "-")

    def __mul__(self, other: object):
        return self._binary_op(other, "*")

    def __truediv__(self, other: object):
        return self._binary_op(other, "/")

    @defer_keypath_args
    def _binary_logical_op(self, other: object, is_and: bool) -> "ColumnExpression":
        from ... import func
        from .py_value import PyValueColumnExpression

        if not isinstance(other, ColumnExpression):
            other = PyValueColumnExpression(other)
        logical_op = func.and_ if is_and else func.or_
        return logical_op(self, other)

    def __and__(self, other: object):
        return self._binary_logical_op(other, is_and=True)

    def __or__(self, other: object):
        return self._binary_logical_op(other, is_and=False)

    def __invert__(self):
        from ... import func

        return func.not_(self)


COLUMN_EXPRESSION_TYPE_KEY_REGISTRY: Dict[
    str,
    Type[Serializable],
] = {}
