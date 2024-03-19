from typing import *

from .keypath import (
    KeyPath,
    KeyPathComponentProperty,
)


def unwrap_keypath_to_name(keypath: Union[KeyPath, str]) -> str:
    """
    Given a keypath, return a string of its immediate property name,
    or raise a `ValueError` if the keypath represents anything else.
    """
    if isinstance(keypath, KeyPath):
        if (type(keypath) != KeyPath) or not (
            (len(keypath._key_path_components) == 1)
            and type(keypath._key_path_components[0]) == KeyPathComponentProperty,
        ):
            raise ValueError(
                f"Provided KeyPath ({keypath}) cannot be unwrapped to a property name."
            )
        return keypath._key_path_components[0].name

    return keypath
