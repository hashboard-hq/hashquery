from typing import *
from typing import List


class KeyPathComponent:
    pass


class KeyPathComponentProperty(KeyPathComponent):
    """
    Component of a KeyPath which represents property access.
    `root.property`
    """

    def __init__(self, name: str) -> None:
        super().__init__()
        self.name = name

    def __repr__(self) -> str:
        return f".{self.name}"


class KeyPathComponentSubscript(KeyPathComponent):
    """
    Component of a KeyPath which represents subscript access.
    `root["item"]`
    """

    def __init__(self, key: Union[str, int]) -> None:
        super().__init__()
        self.key = key

    def __repr__(self) -> str:
        if type(self.key) is str:
            return '["' + self.key + '"]'
        return f"[{self.key}]"


class KeyPathComponentCall(KeyPathComponent):
    """
    Component of a KeyPath which represents calling a value as a function.
    `root(arg1, arg2)` or `root.__call__(arg1, arg2)`
    """

    def __init__(
        self,
        args: List[Any],
        kwargs: Dict[str, Any],
        include_keypath_ctx: bool = False,
    ) -> None:
        super().__init__()
        self.args = args or []
        self.kwargs = kwargs or {}
        self.include_keypath_ctx = include_keypath_ctx

    def __repr__(self) -> str:
        format_arg = lambda a: str(a) if type(a) is not str else f'"{a}"'
        return (
            "("
            + ", ".join(
                [
                    *[format_arg(a) for a in self.args],
                    *[
                        f"{key}={format_arg(value)}"
                        for key, value in self.kwargs.items()
                    ],
                ]
            )
            + ")"
        )


class KeyPath:
    """
    A `KeyPath` is an ordered list of `KeyPathComponent` types which describes
    an accessor to an value. While this is modeled as data, this can be thought
    of as an accessor function from "root" to the result.

    For example, the KeyPath of `_.path.to.property` is analogous to a function
    of the form: `(value) => value.path.to.property`, and is represented
    internally as::

        KeyPath([
            KeyPathComponentProperty("path"),
            KeyPathComponentProperty("to"),
            KeyPathComponentProperty("property")
        ])

    To turn a KeyPath into a value (which we call "resolving" the keypath),
    use one of the functions in the `.resolve` module.

    ------

    KeyPaths can describe more kinds of access than just `.property` access,
    such as subscript access: `_.some_dict["subscript"]` is an accessor
    of the form `(root) => root["subscript"]`, and is represented
    internally as::

        KeyPath([
            KeyPathComponentProperty("some_dict"),
            KeyPathComponentSubscript("subscript")
        ])

    Method calls are also modeled: `_.method(arg1, arg2)` is an accessor of
    the form `(root) => root.method(arg1, arg2)`, and is represented
    internally as::

        KeyPath([
            KeyPathComponentProperty("method"),
            KeyPathComponentCall(args=[arg1, arg2], kwargs={})
        ])

    Certain operators are also deferred, for example, you can add two
    KeyPaths together to form a KeyPath representing the addition:
    `_.x + _.y` is an accessor of the form: `(root) => root.x + root.y`,
    and is represented internally by mapping the operator to the underlying
    method calls::

        KeyPath([
            KeyPathComponentProperty("x"),
            KeyPathComponentProperty("__add__"),
            KeyPathComponentCall(kwargs={}, args=[
                KeyPath([
                    KeyPathComponentProperty("y")
                ])
            ])
        ])

    Finally, KeyPaths can represent chaining a value into the argument of
    another function. For example, `func(_.x _.y).result` can be
    represented as::

        BoundKeyPath(
            func,
            [
                KeyPathComponentCall(kwargs={}, args=[
                    KeyPath([
                        KeyPathComponentProperty("x")
                    ]),
                    KeyPath([
                        KeyPathComponentProperty("y")
                    ]),
                ])
                KeyPathComponentProperty("result")
            ]
        )

    though doing so requires explicit support defined within `func` to accept
    the argument keypath(s) and return a `BoundKeyPath`.
    """

    def __init__(
        self,
        components: List[KeyPathComponent],
    ) -> None:
        self._key_path_components = components

    def __chain__(
        self,
        components: List[KeyPathComponent],
    ):
        return KeyPath([*self._key_path_components, *components])

    def __getattr__(self, name: str) -> "KeyPath":
        return self.__chain__([KeyPathComponentProperty(name)])

    def __getitem__(self, key: Union[str, int]) -> "KeyPath":
        return self.__chain__([KeyPathComponentSubscript(key)])

    def __call__(self, *args: Any, **kwargs: Any) -> "KeyPath":
        return self.__chain__([KeyPathComponentCall(args, kwargs)])

    def __iter__(self):
        return iter([IterItemKeyPath(self, [])])

    def __repr__(self) -> str:
        return f"KeyPath({''.join(str(kpc) for kpc in self._key_path_components)})"


class BoundKeyPath(KeyPath):
    """
    Represents a KeyPath where the root is already known. This is useful for
    when a calling function is known, but the arguments aren't. See the example
    at the end of the docs of `KeyPath`.
    """

    def __init__(
        self,
        bound_root,
        components: List[KeyPathComponent],
    ) -> None:
        super().__init__(components)
        self._bound_root = bound_root

    def __chain__(
        self,
        components: List[KeyPathComponent],
    ):
        return BoundKeyPath(self._bound_root, [*self._key_path_components, *components])

    def __repr__(self) -> str:
        return f"BoundKeyPath({self._bound_root} -> {''.join(str(kpc) for kpc in self._key_path_components)})"


class IterItemKeyPath(KeyPath):
    """
    Represents a templated item inside of an iterator context.
    When `KeyPath.__iter__()` is called, we must immediately return something,
    so we return a real list containing a single `IterItemKeyPath`. Users
    can chain off that single item to form a more complex expression (like
    they can for any `KeyPath`).

    When the list is resolved with `resolve_all_nested_keypaths`,
    `IterItemKeyPath`s are expanded out -- so instead of being a nested list
    they appear alongside things at the top level.

    Take the following example::

        [1, 2, *(s + 1 for s in _.some_numbers)]
        # translates immediately into
        [1, 2, IterItemKeyPath(
            _.some_strings,
            _.__add__(1)
        )]
        # and during resolve, that single `IterItemKeyPath` may turn into
        # multiple values, all at the top level
        [1, 2, 3, 4, 5, 6]

    """

    def __init__(
        self,
        base: KeyPath,
        components: List[KeyPathComponent],
    ) -> None:
        super().__init__(components)
        self._keypath_iter_base = base
        self._keypath_item_template = KeyPath(components)

    def __chain__(
        self,
        components: List[KeyPathComponent],
    ):
        return IterItemKeyPath(
            self._keypath_iter_base,
            [*self._key_path_components, *components],
        )

    def __repr__(self) -> str:
        return f"IterKeyPath({str(self._keypath_iter_base)} -> {''.join(str(kpc) for kpc in self._key_path_components)})"


# Attach methods to forward calls like `_.path.to.thing + _.other_thing`
# into KeyPaths of the form `_.path.to.thing.__add__(_.other_thing)`

UNARY_OP_MAGIC_METHODS = [
    "__abs__",
    "__bool__",
    "__ceil__",
    "__float__",
    "__floor__",
    "__floordiv__",
    "__format__",
    "__hash__",
    "__int__",
    "__invert__",
    "__len__",
    "__neg__",
    "__pos__",
    "__reversed__",
    "__round__",
    "__trunc__",
]

BINARY_OP_MAGIC_METHODS = [
    "__add__",
    "__and__",
    "__complex__",
    "__contains__",
    "__div__",
    "__divmod__",
    "__eq__",
    "__floordiv__",
    "__gt__",
    "__ge__",
    "__lshift__",
    "__lt__",
    "__le__",
    "__matmul__",
    "__mod__",
    "__mul__",
    "__ne__",
    "__or__",
    "__pow__",
    "__rshift__",
    "__sub__",
    "__truediv__",
    "__xor__",
]

for u_op in UNARY_OP_MAGIC_METHODS:
    # have to do this wacky wrapper because Python binds arguments in scope "late"
    # otherwise all the functions will all point to `__neg__` (the last string in the list)
    def make_impl(u_op):
        def impl(self: KeyPath) -> KeyPath:
            return self.__chain__(
                [
                    KeyPathComponentProperty(u_op),
                    KeyPathComponentCall([], {}),
                ]
            )

        return impl

    setattr(KeyPath, u_op, make_impl(u_op))

for bin_op in BINARY_OP_MAGIC_METHODS:

    def make_impl(bin_op):
        def impl(self: KeyPath, other: Any) -> KeyPath:
            return self.__chain__(
                [
                    KeyPathComponentProperty(bin_op),
                    KeyPathComponentCall([other], {}),
                ]
            )

        return impl

    setattr(KeyPath, bin_op, make_impl(bin_op))


"""
`_` is a global variable representing the identity keypath:
`(value) => value`. Chaining off of `_` acts as an expression
without the `_` "filled in yet". For example, in the expression:
`_.a + _.b` and the root value of `root`, then the final expression will
be `root.a + root.b`.
"""
_ = KeyPath([])  # the root keypath
