from dataclasses import dataclass
from typing import *

from .keypath import (
    KeyPath,
    KeyPathComponentCall,
    KeyPathComponentProperty,
    KeyPathComponentSubscript,
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
