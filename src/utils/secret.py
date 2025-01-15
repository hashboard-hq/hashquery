from typing import *
from uuid import uuid4

"""
`Secret` defines a reference to a secret, stored elsewhere.

The intent of this class is to provide a mechanism for attaching a secret on
a deeply nested object (such as a `Model`'s `Connection` instance), without
making all the parent objects sensitive. For example, if a `Model`
implementation simply `print`ed everything it has, it wouldn't end up leaking
a secret to the console, since the `Secret` instance which would be printed
only has a _pointer_ to a sensitive value, and to resolve that value, you
must use a separate export: `resolve_secret`.
"""


_SECRET_REGISTRY = dict()


T = TypeVar("T")


class Secret(Generic[T]):
    PLACEHOLDER = "[secret]"

    def __init__(self, value: T):
        self._id = uuid4()
        self._set(value)

    def __repr__(self):
        return self.PLACEHOLDER

    def _to_wire_format(self):
        return self.PLACEHOLDER

    def _evict(self):
        """Removes the associated secret."""
        if self._id in _SECRET_REGISTRY:
            del _SECRET_REGISTRY[self._id]

    def _set(self, value: T):
        """Replace the value stored in a `Secret`"""
        _SECRET_REGISTRY[self._id] = value


def resolve_secret(s: Secret[T]) -> T:
    if not isinstance(s, Secret):
        raise TypeError(
            "Unexpectedly found a bare value when a secret was expected. "
            + "This indicates a programming error when loading a sensitive value. "
            + "Ensure you call `Secret(value)` when loading secrets."
        )
    return _SECRET_REGISTRY[s._id]
