from copy import copy, deepcopy
from typing import *

T = TypeVar("T")


def deepcopy_properties(
    original: T,
    memo,
    *,
    identity_keys: List[str] = None,
    shallow_keys: List[str] = None,
) -> T:
    identity_keys = identity_keys or []
    shallow_keys = shallow_keys or []
    if hasattr(original, "__deepcopy__"):
        # hack to prevent infinite recursion in call to deepcopy
        identity_keys.append("__deepcopy__")

    tmp = {}
    for key in identity_keys + shallow_keys:
        tmp[key] = getattr(original, key)
        setattr(original, key, None)

    try:
        result = deepcopy(original, memo)
    finally:
        # restore the original instance
        for key in identity_keys:
            setattr(original, key, tmp[key])
        for key in shallow_keys:
            setattr(original, key, tmp[key])

    for key in identity_keys:
        setattr(result, key, tmp[key])
    for key in shallow_keys:
        setattr(result, key, copy(tmp[key]))

    return result
