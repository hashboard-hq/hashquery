import builtins

from .keypath import BoundKeyPath, KeyPath, _
from .resolve import (
    defer_keypath_args,
    resolve_all_nested_keypaths,
    resolve_keypath,
    resolve_keypath_args_from,
)
from .unwrap import unwrap_keypath_to_name

# ---------


# We patch over `len` because the default implementation checks that everything
# that implements `__len__` returns an integer. KeyPaths intentionally defers
# evaluation of `len`, so it can't return an integer.
_original_len = builtins.len


def _len_override(o) -> int:
    if isinstance(o, KeyPath):
        return o.__len__()
    return _original_len(o)


builtins.len = _len_override
