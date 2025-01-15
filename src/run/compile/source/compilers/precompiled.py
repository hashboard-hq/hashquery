from copy import deepcopy

from ...query_layer import QueryLayer
from ...source.compile_source import register_source_compiler


class PrecompiledSource:
    """
    Allows a compiler to shim another compiler by replacing one of its `.source`
    nodes with a result that is already compiled.
    """

    def __init__(self, layer: QueryLayer) -> None:
        super().__init__()
        self.layer = deepcopy(layer)


register_source_compiler(PrecompiledSource, lambda src, ctx: src.layer)
