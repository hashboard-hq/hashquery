from typing import *

from ..utils.keypath.keypath import _, KeyPathComponentCall
from .column_expression.column_expression import ColumnExpression
from .namespace import ModelNamespace


T = TypeVar("T")


class LazyAccessor(Generic[T]):
    def __init__(self, map_name: str) -> None:
        self.__map_name__ = map_name

    def __getattr__(self, key: str) -> T:
        if key == "__map_name__":
            raise AttributeError()
        return _._access_identifiable_map.__chain__(
            [
                KeyPathComponentCall(
                    args=[self.__map_name__, key],
                    kwargs={},
                    include_keypath_ctx=True,
                )
            ]
        )

    def __getitem__(self, key: str) -> T:
        return self.__getattr__(key)


attr = LazyAccessor[ColumnExpression]("_attributes")
msr = LazyAccessor[ColumnExpression]("_measures")
rel = LazyAccessor[ModelNamespace]("_namespaces")
