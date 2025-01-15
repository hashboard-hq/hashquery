import hashlib
import json
from typing import *

from .serializable import Serializable

if TYPE_CHECKING:
    from ..model import Connection, Model
    from ..model.source import Source


def stable_key_for_model_data(model: "Model"):
    """
    A stable hash key for a Model instance, suitable as a key for the data
    that this model is expected to produce if queried.
    """
    source_key = stable_key_for_model_source(model._source)
    connection_key = stable_key_for_connection(model._connection)
    return f"{source_key}-{connection_key}"


def stable_key_for_model_source(source: "Source"):
    """
    A stable hash key for a Source instance, suitable as a cache key
    or other by-value lookup. Since this does not include the Connection
    metadata, this value is unlikely to be safe to associate with persisted
    data, and is more useful for matching similar looking sources.
    """
    return str(
        hashlib.sha256(
            json.dumps(
                source._to_wire_format(),
                default=Serializable._primitive_to_wire_format,
                sort_keys=True,
            ).encode("utf-8")
        ).hexdigest()
    )


def stable_key_for_connection(connection: "Connection"):
    """
    A stable hash key for a Connection instance. Any secrets are salted and
    hashed together, such that changing the secret (such as switching accounts)
    will change the cache key.
    """
    raise NotImplementedError(
        "Hashing connection instances securely is not yet implemented."
    )
