from copy import deepcopy
from typing import *
import re

from .column_expression import ColumnExpression
from ..namespace import ModelNamespace
from ...utils.builder import builder_method
from ...utils.keypath.keypath import _, KeyPath, BoundKeyPath, KeyPathComponentCall

if TYPE_CHECKING:
    from ..model import Model


"""
Hashquery may lift transformations and expressions into a chain of arbitrarily
named CTEs or use a SQL fragment in a new context where it needs to be
disambiguated differently. For example, when selecting a column through a JOINed
relation, SQL expressions on those columns must be disambiguated with the name of
the relation, not the source table.

Hashquery must transform some SQL snippets to ensure that your SQL statement
references these contextual elements correctly. To do so, we provide this special
identifier. If you disambiguate a column reference with this identifier, it will
be automatically adjusted to point at the "main" table (be that from a loaded
table, a chain of CTEs, or a joined relation).
"""
SELF_TABLE_ALIAS = "self"

"""
Detects matches against the pattern `{{ some_name }}` which can be used
to substitute the definition of `some_name` into that string. The first
capture group denotes the identifier of the referenced expression.
"""
SQL_REFERENCE_SUBSTITUTION_REGEX = re.compile(r"{{\s*([\w\d_\.]+)\s*}}")


class SqlTextColumnExpression(ColumnExpression):
    def __init__(self, sql: str) -> None:
        super().__init__()
        self.sql = sql
        self.namespace_identifier: Optional[str] = None
        self.nested_expressions: Dict[str, ColumnExpression] = {}

    def default_identifier(self) -> str:
        tokens = self.sql.split(".")
        if len(tokens) == 1 and tokens[0].isidentifier():
            return tokens[1]  # column expression is directly nameable
        elif len(tokens) == 2 and tokens[1].isidentifier():
            return tokens[1]  # passthrough a name through a namespace
        else:
            return None

    @builder_method
    def disambiguated(self, namespace) -> "SqlTextColumnExpression":
        self.namespace_identifier = (
            namespace._identifier
            if isinstance(namespace, ModelNamespace)
            else namespace
        )
        self.nested_expressions = {
            id: expr.disambiguated(namespace)
            for id, expr in self.nested_expressions.items()
        }

    def __repr__(self) -> str:
        return f'sql("{self.sql}")'

    # --- Binding to Model ---

    def bind_references_to_model(
        self,
        model: "Model",
        _mutate_in_place=False,  # escape hatch to do this in place
    ) -> "SqlTextColumnExpression":
        """
        Binds the column expressions referenced by the text of this expression
        to those found in the given model. The `expr` must be uniquely identified
        and then can be referenced within the SQL using `{{ identifier }}`.
        """
        reference_matches = list(
            re.finditer(SQL_REFERENCE_SUBSTITUTION_REGEX, self.sql)
        )
        if not reference_matches:
            return self
        elif isinstance(model, KeyPath):
            # defer this function until the model is ready
            return BoundKeyPath(
                self.bind_references_to_model,
                [KeyPathComponentCall(args=[model], kwargs={})],
            )

        result = self if _mutate_in_place else deepcopy(self)
        for ref_match in reference_matches:
            identifier: str = ref_match.group(1)
            identifier_tokens = identifier.split(".")
            expression: ColumnExpression = None
            if len(identifier_tokens) == 2:
                # look for a joined attribute through the namespace
                namespace_id, attr_id = identifier_tokens
                namespace: "ModelNamespace" = model._access_identifiable_map(
                    "_namespaces",
                    namespace_id,
                    keypath_ctx=attr_id,
                    syntax="sql_ref",
                )
                expression = getattr(namespace, attr_id)
            elif len(identifier_tokens) == 1:
                # look for a measure or an attribute; favoring attributes
                expression = model._access_identifiable_map(
                    ["_attributes", "_measures"],
                    identifier,
                    syntax="sql_ref",
                )
            else:
                raise ValueError("Reference `{{" + identifier + "}}` is invalid.")
            result.nested_expressions[identifier] = expression
        return result

    # --- Serialization ---

    __TYPE_KEY__ = "sqlText"

    def to_wire_format(self) -> dict:
        return {
            **super().to_wire_format(),
            "sql": self.sql,
            "namespaceIdentifier": self.namespace_identifier,
            "nestedExpressions": {
                id: expr.to_wire_format()
                for id, expr in self.nested_expressions.items()
            },
        }

    @classmethod
    def from_wire_format(cls, wire: dict) -> "SqlTextColumnExpression":
        assert wire["subType"] == cls.__TYPE_KEY__
        result = SqlTextColumnExpression(wire["sql"])
        result.namespace_identifier = wire.get("namespaceIdentifier")
        result.nested_expressions = {
            id: ColumnExpression.from_wire_format(expr_wire)
            for id, expr_wire in wire.get("nestedExpressions", {}).items()
        }
        result._from_wire_format_shared(wire)
        return result
