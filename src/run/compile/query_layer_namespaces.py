from dataclasses import dataclass
from typing import *

import sqlalchemy.sql as sa

from ...utils.copy import deepcopy_properties

if TYPE_CHECKING:
    from ..db.reflection import ColumnTypeInfo


@dataclass
class NamespaceInfo:
    ref: sa.Alias
    # Mapping of physical name to column type information.
    column_metadata: Dict[str, "ColumnTypeInfo"]

    def __deepcopy__(self, memo):
        # an `sa.Alias` is an object with a meaningful identity
        # and should not be copied
        return deepcopy_properties(self, memo, identity_keys=["ref"])


class QueryLayerNamespaces:
    """
    Some SQL require scoping to a namespace, to avoid conflicting names.
    For example, a `column("id")` expression refers to different values
    when qualified with one table `sales.id` vs. another `customers.id`,
    despite being otherwise the same definition.

    QueryLayerNamespaces serves to help qualify these column names and correctly resolve
    column types when constructing SQL expressions, namely when joins are present.

    Hashquery column expressions may carry information with them to aid
    in disambiguation, generally attached via `ColumnExpression.disambiguated`.
    For columns like this, layers need to expose API for them to gain a reference to
    the namespaces (aliases) they want to qualify themselves with.

    Under the hood, this is just a map from `alias_name ->  _Namespace`, where _Namespace
    holds a reference to an `sa.Alias` object, as well as the column type information for the source.
    It's import that the `sa.Alias` is a reference, since SQLAlchemy exclusively uses the object
    identity of aliases, and exposes no API for qualifying content through a string name.

    `named(str)` carries this information for joined relations, while `main`
    points at the primary model being queried, ie. the alias of the `FROM`
    clause.
    """

    def __init__(self) -> None:
        # consumers constructing a new QueryLayer should immediately bind `main.ref`
        self.main: NamespaceInfo = NamespaceInfo(ref=None, column_metadata={})

        self._name_to_ref: Dict[str, NamespaceInfo] = {}
        self.used_names: Set[str] = set()

    def named(self, name: str) -> Optional[NamespaceInfo]:
        self.used_names.add(name)
        return self._name_to_ref.get(name)

    def set_joined(
        self,
        name: str,
        ref: sa.Alias,
        column_metadata: Dict[str, "ColumnTypeInfo"],
    ):
        self._name_to_ref[name] = NamespaceInfo(
            ref=ref, column_metadata=column_metadata
        )

    @property
    def all_names(self):
        return self._name_to_ref.keys()
