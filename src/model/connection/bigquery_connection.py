import json
from dataclasses import dataclass
from typing import *

import pandas as pd

from ...utils.builder import builder_method
from ...utils.secret import Secret
from .connection import Connection


@dataclass
class BigQueryConnection(Connection):
    def __init__(
        self,
        *,
        credentials_path: Optional[str] = None,
        credentials_info: Optional[dict] = None,
        location: Optional[str] = None,
    ):
        """
        Initialize a new BigQuery connection.

        By default, the connection will be authenticated using the
        environment variable `GOOGLE_APPLICATION_CREDENTIALS`, according to the
        same logic as the BigQuery client library.
        https://cloud.google.com/docs/authentication/application-default-credentials

        Alternatively, you can specify an explicit path to a service account
        JSON file using `credentials_path`, or use a python dictionary using
        `credentials_info`.
        https://github.com/googleapis/python-bigquery-sqlalchemy?tab=readme-ov-file#authentication
        """
        self._credentials_path = credentials_path
        self._credentials_info = Secret(credentials_info) if credentials_info else None
        self._location = location

    @builder_method
    def with_credentials_path(self, path: str) -> "BigQueryConnection":
        """Override the current credentials path."""
        self._credentials_path = path

    @builder_method
    def with_credentials_info(self, info: dict) -> "BigQueryConnection":
        """Override the current credentials info."""
        self._credentials_info._set(info)

    @builder_method
    def with_location(self, location: str) -> "BigQueryConnection":
        """Override the current location, such as `asia-northeast1`."""
        self._location = location

    # --- Serialization ---

    __TYPE_KEY__ = "bigquery"

    def _to_wire_format(self) -> dict:
        return {
            **super()._to_wire_format(),
            "location": self._location,
            # we don't serialize either of the credentials properties
            # as one is a secret, and the other is a local filepath to
            # a sensitive file
            "credentialsPath": Secret.PLACEHOLDER,
            "credentialsInfo": Secret.PLACEHOLDER,
        }

    @classmethod
    def _from_wire_format(cls, wire: dict) -> "BigQueryConnection":
        assert wire["subType"] == cls.__TYPE_KEY__
        return BigQueryConnection(location=wire.get("location"))
