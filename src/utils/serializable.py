from abc import ABC
from datetime import date, datetime, timedelta
from functools import wraps
from typing import Type

from typing_extensions import Self

from .timeinterval import timeinterval

HASHQUERY_WIRE_VERSION_KEY = "_version"
# This is a version number for the wire format of Hashquery.
# Increase it if you ever make a backwards incompatible change
# to a `to_wire_format`/`from_wire_format` pair, or if you change
# the JSON payload for `RunResults`.
HASHQUERY_WIRE_VERSION = 6


class Serializable(ABC):
    def to_wire_format(cls) -> dict:
        ...

    @classmethod
    def from_wire_format(cls, wire: dict) -> Self:
        ...

    @classmethod
    def _primitive_to_wire_format(cls, value):
        if isinstance(value, datetime):
            return {"$typeKey": "py.datetime", "iso": value.isoformat()}
        elif isinstance(value, date):
            return {"$typeKey": "py.date", "iso": value.isoformat()}
        elif isinstance(value, timedelta):
            return {"$typeKey": "py.timedelta", "seconds": int(value.total_seconds())}
        elif isinstance(value, timeinterval):
            return {
                "$typeKey": "py.timeinterval",
                "unit": value.unit,
                "num": value.num,
            }
        # directly serializable to JSON without type information
        return value

    @classmethod
    def _primitive_from_wire_format(cls, wire):
        type_key = wire.get("$typeKey") if type(wire) == dict else None
        if not type_key:
            return wire
        elif type_key == "py.datetime":
            return datetime.fromisoformat(wire["iso"])
        elif type_key == "py.date":
            return date.fromisoformat(wire["iso"])
        elif type_key == "py.timedelta":
            return timedelta(seconds=wire["seconds"])
        elif type_key == "py.timeinterval":
            return timeinterval(
                unit=wire["unit"],
                num=wire["num"],
            )
        else:
            raise ValueError(
                f"Cannot deserialize value. `$typeKey` is present but an unrecognized type. {wire}"
            )

    def __init_subclass__(cls) -> None:
        """
        When we subclass, update their `to_wire_format`/`from_wire_format`
        methods to populate and validate the wire version keys.
        """
        orig_to_wire = cls.to_wire_format
        orig_from_wire = cls.from_wire_format

        @wraps(orig_to_wire)
        def versioned_to_wire_format(self) -> dict:
            result = orig_to_wire(self)
            result[HASHQUERY_WIRE_VERSION_KEY] = HASHQUERY_WIRE_VERSION
            return result

        @classmethod
        @wraps(orig_from_wire)
        def versioned_from_wire_format(cls: Type["Serializable"], wire: dict) -> Self:
            found_wire_version = wire.get(HASHQUERY_WIRE_VERSION_KEY)
            if found_wire_version != HASHQUERY_WIRE_VERSION:
                raise WireFormatVersionError(
                    expected=HASHQUERY_WIRE_VERSION,
                    found=found_wire_version,
                )
            return orig_from_wire(wire)

        versioned_to_wire_format.__versioned__ = True
        versioned_from_wire_format.__versioned__ = True
        if not getattr(orig_to_wire, "__versioned__", False):
            cls.to_wire_format = versioned_to_wire_format
        if not getattr(orig_from_wire, "__versioned__", False):
            cls.from_wire_format = versioned_from_wire_format


class WireFormatVersionError(Exception):
    def __init__(self, expected: int, found: int) -> None:
        super().__init__(self._make_error_message(expected, found))
        self.expected_version = expected
        self.found_version = found

    @classmethod
    def _make_error_message(cls, expected: int, found: int):
        desc_str = "Cannot load Hashquery object."
        is_found_ahead = found > expected
        # this language is from the perspective of the client; on the server we
        # catch and rethrow the error as `GleanUserFacingError` with a different
        # string, with language that makes more sense
        cause_str = (
            (
                "This version of Hashquery is no longer supported. "
                + "Please upgrade your package and try again."
            )
            if is_found_ahead
            else (
                "This version of Hashquery is ahead of the server's target version. "
                + f"You may be using a prerelease version of Hashquery not yet supported in this environment. "
                + f"You may be able to resolve the issue by downgrading to an earlier version of Hashquery. "
            )
        )
        debug_str = f"(Expected wire format signature: {expected}. Found: {found})"
        return "\n".join([desc_str, cause_str, debug_str])
