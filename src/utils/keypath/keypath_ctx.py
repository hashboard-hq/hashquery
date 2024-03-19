from typing import *
from dataclasses import dataclass

from .keypath import (
    KeyPath,
    KeyPathComponentProperty,
    KeyPathComponentSubscript,
    KeyPathComponentCall,
)


@dataclass
class KeyPathCtx:
    root: Any
    current: Any
    full_keypath: KeyPath
    current_keypath_component: Union[
        KeyPathComponentProperty,
        KeyPathComponentSubscript,
        KeyPathComponentCall,
    ]
    remaining_keypath: KeyPath
