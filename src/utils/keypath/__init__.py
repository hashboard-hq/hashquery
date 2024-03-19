from .keypath import _, KeyPath, BoundKeyPath
from .resolve import (
    resolve_keypath,
    resolve_all_nested_keypaths,
    resolve_keypath_args_from,
    defer_keypath_args,
)
from .unwrap import unwrap_keypath_to_name

# ---------


import builtins

# We patch over `len` because the default implementation checks that everything
# that implements `__len__` returns an integer. KeyPaths intentionally defers
# evaluation of `len`, so it can't return an integer.
_original_len = builtins.len


def _len_override(o) -> int:
    if isinstance(o, KeyPath):
        return o.__len__()
    return _original_len(o)


builtins.len = _len_override
