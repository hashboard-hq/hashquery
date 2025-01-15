from typing import *

from ..utils.serializable import Serializable
from .column_expression.column_expression import ColumnExpression

if TYPE_CHECKING:
    from .model import Model


class ModelNamespace(Serializable):
    """
    Represents a namespace within a Model, typically through a JOIN.
    """

    def __init__(
        self,
        identifier: str,
        nested_model: "Model",
    ) -> None:
        self._identifier = identifier
        self._nested_model = nested_model
        self._through_foreign_key_attr: Optional[ColumnExpression] = None

    # --- Public accessors ---

    def __getattr__(self, name: str) -> ColumnExpression:
        if name in ["_identifier", "_nested_model"]:
            raise AttributeError()
        try:
            result: ColumnExpression = self._nested_model._access_identifiable_map(
                "_attributes", name
            )
            return result.disambiguated(self)
        except AttributeError as err:
            raise AttributeError(
                f"No attribute with the identifier `{name}` was found "
                + f"in the `{self._identifier}` namespace."
            ) from err

    def __getitem__(self, key: str) -> ColumnExpression:
        return self.__getattr__(key)

    def __iter__(self):
        return iter(a.disambiguated(self) for a in self._nested_model._attributes)

    def get_custom_meta(self, name: str):
        """
        Returns a value from the custom metadata dictionary for the model
        described by this namespace, or `None` if the key does not exist.
        """
        return self._nested_model.get_custom_meta(name)

    def __repr__(self) -> str:
        return "\n".join(
            [
                f"Relation `{self._identifier}`:",
                f"  Attributes: {', '.join(self._nested_model._attributes.keys())}",
            ],
        )

    # --- Serialization ---

    def _to_wire_format(self) -> Dict:
        return {
            "type": "modelNamespace",
            "identifier": self._identifier,
            "nestedModel": self._nested_model._to_wire_format(),
            "throughForeignKeyAttr": (
                self._through_foreign_key_attr._to_wire_format()
                if self._through_foreign_key_attr
                else None
            ),
        }

    @classmethod
    def _from_wire_format(cls, wire: Dict) -> "ModelNamespace":
        from .model import Model  # need the actual implementation

        assert wire["type"] == "modelNamespace"
        result = ModelNamespace(
            identifier=wire["identifier"],
            nested_model=Model._from_wire_format(wire["nestedModel"]),
        )
        if fkattr_wire := wire.get("throughForeignKeyAttr"):
            result._through_foreign_key_attr = ColumnExpression._from_wire_format(
                fkattr_wire
            )
        return result
