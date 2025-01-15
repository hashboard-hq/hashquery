import base64
import json
import os
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Optional

from ...utils.env import env_with_fallback
from ...utils.version import HASHQUERY_VERSION
from .base_uri import BASE_HASHBOARD_URI


class HashboardClientCredentials(ABC):
    """
    Generic class for a method for an external client to authorize into
    Hashboard within a project. Use `get_client_credentials` to form
    an instance from the client's current `env`.
    """

    project_id: str
    base_uri: str

    @abstractmethod
    def get_headers(self) -> dict:
        return {
            "X-GLEAN-UNSAFE-PROJECT-ID": self.project_id,
            "X-GLEAN-HASHQUERY-VERSION": HASHQUERY_VERSION,
        }


@dataclass
class HashboardUserJWTClientCredentials(HashboardClientCredentials):
    project_id: str
    user_jwt: str

    base_uri: str = field(default=BASE_HASHBOARD_URI)

    def get_headers(self) -> dict:
        return {
            **super().get_headers(),
            "X-GLEAN-BASE-JWT": self.user_jwt,
        }


@dataclass
class HashboardAccessKeyClientCredentials(HashboardClientCredentials):
    project_id: str
    access_key_id: str
    access_key_token: str

    base_uri: str = field(default=BASE_HASHBOARD_URI)

    # denormalized; cached for the header
    encoded_key: str = field(default=None)

    def __post_init__(self):
        # denormalize into `encoded_key`
        if self.encoded_key:
            return
        payload = {
            "project_id": self.project_id,
            "access_key_id": self.access_key_id,
            "access_key_token": self.access_key_token,
        }
        payload_bytes = json.dumps(payload).encode()
        self.encoded_key = base64.b64encode(payload_bytes).decode()

    def get_headers(self) -> dict:
        return {
            **super().get_headers(),
            "Authorization": self.encoded_key,
        }

    @classmethod
    def from_file(
        cls,
        credentials_filepath: str,
        *,
        base_uri: Optional[str] = BASE_HASHBOARD_URI,
    ) -> "HashboardAccessKeyClientCredentials":
        credentials_filepath = os.path.expanduser(credentials_filepath)
        if not os.path.isfile(credentials_filepath):
            raise Exception("No access key at file path: " + credentials_filepath)
        with open(credentials_filepath, "r") as f:
            credentials_json = f.read()
        try:
            credentials = json.loads(credentials_json)
            return HashboardAccessKeyClientCredentials(
                project_id=credentials["project_id"],
                access_key_id=credentials["access_key_id"],
                access_key_token=credentials["access_key_token"],
                base_uri=base_uri,
            )
        except Exception as e:
            raise Exception("Invalid access key file: " + str(e))

    @classmethod
    def from_encoded_key(
        cls,
        key: str,
        *,
        base_uri: Optional[str] = BASE_HASHBOARD_URI,
    ) -> "HashboardAccessKeyClientCredentials":
        try:
            decoded = base64.b64decode(key).decode()
            loaded: dict = json.loads(decoded)
            return HashboardAccessKeyClientCredentials(
                encoded_key=key,
                project_id=loaded["project_id"],
                access_key_id=loaded["access_key_id"],
                access_key_token=loaded["access_key_token"],
                base_uri=base_uri,
            )
        except Exception as e:
            raise Exception("Could not load access key: " + str(e))


# -------------


def load_client_credentials_from_env() -> HashboardClientCredentials:
    # JWT - uses the prefix "HASHQUERY" instead of "HASHBOARD" since
    # the JWTs shouldn't be used for anything other than query stuff
    if user_jwt := env_with_fallback("HASHQUERY_USER_JWT"):
        project_id = env_with_fallback("HASHQUERY_PROJECT_ID")
        if not project_id:
            raise RuntimeError(
                "`HASHQUERY_PROJECT_ID` must also be specified when using a JWT."
            )
        return HashboardUserJWTClientCredentials(
            user_jwt=user_jwt, project_id=project_id
        )

    # API token
    elif encoded_api_key := env_with_fallback("HASHBOARD_API_TOKEN"):
        return HashboardAccessKeyClientCredentials.from_encoded_key(encoded_api_key)

    # Manual env vars
    elif (
        (
            project_id := env_with_fallback(
                "HASHBOARD_PROJECT_ID",
                "GLEAN_PROJECT_ID",
            )
        )
        and (
            access_key_id := env_with_fallback(
                "HASHBOARD_ACCESS_KEY_ID",
                "GLEAN_ACCESS_KEY_ID",
            )
        )
        and (
            access_key_token := env_with_fallback(
                "HASHBOARD_SECRET_ACCESS_KEY_TOKEN",
                "GLEAN_SECRET_ACCESS_KEY_TOKEN",
            )
        )
    ):
        return HashboardAccessKeyClientCredentials(
            project_id=project_id,
            access_key_id=access_key_id,
            access_key_token=access_key_token,
        )

    # Credentials file
    else:
        credentials_filepath = (
            env_with_fallback(
                "HASHBOARD_CREDENTIALS_FILEPATH",
                "GLEAN_CREDENTIALS_FILEPATH",
            )
            or "~/.hashboard/hb_access_key.json"
        )
        try:
            credentials_from_file = HashboardAccessKeyClientCredentials.from_file(
                credentials_filepath
            )
            return credentials_from_file
        except:
            pass

    raise RuntimeError(
        """
Could not load Hashboard credentials from environment.
You can configure Hashboard credentials using any one of the following methods:

  [recommended for local development]
Save a credentials file to `~/.hashboard/hb_access_key.json`. This can
be done automatically for your account by using the `hashboard-cli`'s
`hb token` command. You can save the file to another location by configuring
the `HASHBOARD_CREDENTIALS_FILEPATH` environment variable.

  [recommended for remote environments]
Set environment variable `HASHBOARD_API_TOKEN` to a valid Hashboard API token.

  [advanced]
Load credentials using your own logic and construct/use instances of
`HashboardClientCredentials`. Feel free to ask Hashboard support for help
if you need to do so.
"""
    )
