from typing import *

from .column_expression.column_expression import ColumnExpression
from ..utils.serializable import Serializable

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

    def to_wire_format(self) -> Dict:
        return {
            "type": "modelNamespace",
            "identifier": self._identifier,
            "nestedModel": self._nested_model.to_wire_format(),
        }

    @classmethod
    def from_wire_format(cls, wire: Dict) -> "ModelNamespace":
        from .model import Model  # need the actual implementation

        assert wire["type"] == "modelNamespace"
        return ModelNamespace(
            identifier=wire["identifier"],
            nested_model=Model.from_wire_format(wire["nestedModel"]),
        )
