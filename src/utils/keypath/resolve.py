import inspect
from functools import wraps
from typing import Any

from .keypath import (
    BoundKeyPath,
    KeyPath,
    KeyPathComponentCall,
    KeyPathComponentProperty,
    KeyPathComponentSubscript,
)
from .keypath_ctx import KeyPathCtx


def resolve_keypath(root: Any, keypath: KeyPath) -> Any:
    """
    Given a root and a KeyPath, resolves the keypath for that root
    and returns the final result.

    If the given `keypath` argument is not a KeyPath, this will just return
    it as is, since it already represents a resolved value.
    """
    if not isinstance(keypath, KeyPath):
        return keypath

    current = root
    if type(keypath) == BoundKeyPath:
        current = keypath._bound_root

    for component_idx, component in enumerate(keypath._key_path_components):
        if type(component) is KeyPathComponentProperty:
            current = getattr(current, component.name)
        elif type(component) is KeyPathComponentSubscript:
            current = current[component.key]
        elif type(component) is KeyPathComponentCall:
            args = resolve_all_nested_keypaths(root, component.args)
            kwargs = resolve_all_nested_keypaths(root, component.kwargs)
            if component.include_keypath_ctx:
                kwargs["keypath_ctx"] = KeyPathCtx(
                    root=root,
                    current=current,
                    full_keypath=keypath,
                    current_keypath_component=component,
                    remaining_keypath=KeyPath(
                        keypath._key_path_components[component_idx + 1 :]
                    ),
                )
            current = current(*args, **kwargs)
        else:
            raise AssertionError(f"Invalid keypath component: {type(component)}")

    # a KeyPath may result in another KeyPath, which we need to further resolve
    return resolve_keypath(root, current)


def resolve_all_nested_keypaths(root: Any, values) -> Any:
    """
    Given a data structure that may have KeyPaths in it, resolves all of them
    recursively with the provided root. This is helpful for handling collections
    of values that may or may not be KeyPaths, and turning them all into real
    values.
    """
    if type(values) is dict:
        return {
            key: resolve_all_nested_keypaths(root, nested)
            for key, nested in values.items()
        }
    elif type(values) is list:
        return [resolve_all_nested_keypaths(root, nested) for nested in values]
    elif type(values) is tuple:
        return (resolve_all_nested_keypaths(root, nested) for nested in values)
    elif isinstance(values, KeyPath):
        # a KeyPath may result in a structure containing more KeyPaths,
        # which we need to further resolve
        next = resolve_keypath(root, values)
        return resolve_all_nested_keypaths(root, next)
    else:
        return values


def resolve_keypath_args_from(root_keypath: KeyPath):
    """
    Decorates a function to convert its arguments to KeyPaths, using
    one of the arguments as the root for the others.

    The passed KeyPath is used to point to the root. It must start
    with a property access against a value matching one of the parameters'
    by name.

    For example, you can easily allow accessing `self` properties within a
    method. Let's assume I have a method of the form::

        def set_primary_date(self, date_column):

    and I want to allow consumers to write::

        model.set_primary_date(_.timestamp)

    instead of::

        model.set_primary_date(model.attributes.timestamp)

    I can do so by applying the decorator::

        @resolve_keypath_args_from(_.self.attributes)
        def set_primary_date(self, date_column):
    """

    def wrap(func):
        root_keypath_first_access = root_keypath._key_path_components[0]
        if not type(root_keypath_first_access) is KeyPathComponentProperty:
            raise AssertionError(
                "KeyPath for args decorator root must begin with property access"
            )
        root_keypath_after_first_prop = KeyPath(root_keypath._key_path_components[1:])

        # determine the index the root_keypath is pointing to, for
        # mapping `args` to the keypath value
        root_keypath_arg_idx = next(
            (
                i
                for i, param in enumerate(
                    list(inspect.signature(func).parameters.values())
                )
                if param.kind != inspect.Parameter.KEYWORD_ONLY
                and param.name == root_keypath_first_access.name
            ),
            None,
        )

        @wraps(func)
        def resolved_arg_func(*args, **kwargs):
            # The user can pass in the target root either inside of `*args`
            # or inside of `**kwargs`. We need to determine where it is.
            # and then resolve it from there

            if root_keypath_first_access.name in kwargs:
                root = resolve_keypath(
                    kwargs[root_keypath_first_access.name],
                    root_keypath_after_first_prop,
                )
            else:
                root = resolve_keypath(
                    args[root_keypath_arg_idx],
                    root_keypath_after_first_prop,
                )

            # apply the underlying function
            return func(
                *resolve_all_nested_keypaths(root, args),
                **resolve_all_nested_keypaths(root, kwargs),
            )

        return resolved_arg_func

    return wrap


def defer_keypath_args(func):
    """
    Decorates a function to allows its arguments to be KeyPaths, and if they
    are, the function as a whole will be deferred as a new KeyPath, which
    can be run later.

    For example, let's assume I have a method of the form::

        def count(column_expression):

    and I want to allow consumers to write::

        model.with_measure(count(_.user_id))

    instead of::

        model.with_measure(count(model.attributes.user_id))

    I can do so by applying the decorator::

        @defer_keypath_args
        def count(column_expression):
    """

    @wraps(func)
    def wrap(*args, **kwargs):
        if _has_keypath(args) or _has_keypath(kwargs):
            return BoundKeyPath(func, [KeyPathComponentCall(args=args, kwargs=kwargs)])

        return func(*args, **kwargs)

    return wrap


def _has_keypath(values):
    if isinstance(values, KeyPath):
        return True
    elif type(values) is dict:
        return any(_has_keypath(k) or _has_keypath(v) for k, v in values.items())
    elif type(values) in (list, tuple):
        return any(_has_keypath(nested) for nested in values)
    return False
