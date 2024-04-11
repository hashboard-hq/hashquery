import base64
import json
import os
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Optional

from ..utils.env import env_with_fallback
from ..utils.version import HASHQUERY_VERSION


class HashboardClientCredentials(ABC):
    """
    Generic class for a method for an external client to authorize into
    Hashboard within a project. Use `get_client_credentials` to form
    an instance from the client's current `env`.
    """

    project_id: str

    @abstractmethod
    def get_headers(self) -> dict:
        return {
            "X-GLEAN-UNSAFE-PROJECT-ID": self.project_id,
            "X-GLEAN-HASHQUERY-VERSION": HASHQUERY_VERSION,
        }


@dataclass
class HashboardUserJWTClientCredentials(HashboardClientCredentials):
    user_jwt: str
    project_id: str

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
    def from_encoded_key(cls, key: str) -> "HashboardAccessKeyClientCredentials":
        try:
            decoded = base64.b64decode(key).decode()
            loaded: dict = json.loads(decoded)
            return HashboardAccessKeyClientCredentials(
                encoded_key=key,
                project_id=loaded["project_id"],
                access_key_id=loaded["access_key_id"],
                access_key_token=loaded["access_key_token"],
            )
        except Exception as e:
            raise Exception("Could not load access key: " + str(e))


# -------------


def load_client_credentials_from_env() -> Optional[HashboardClientCredentials]:
    # JWT - uses the prefix "HASHQUERY" instead of "HASHBOARD" since
    # the JWTs shouldn't be used for anything other than query stuff
    if user_jwt := env_with_fallback("HASHQUERY_USER_JWT"):
        project_id = env_with_fallback("HASHQUERY_PROJECT_ID")
        if not project_id:
            raise RuntimeError(
                "`HASHQUERY_PROJECT_ID` must also be specified when using a JWT."
            )
        return HashboardUserJWTClientCredentials(
            user_jwt=user_jwt,
            project_id=project_id,
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
        if credentials_from_file := _load_credentials_from_filepath(
            credentials_filepath
        ):
            return credentials_from_file

    # couldn't find anything good
    return None


def _load_credentials_from_filepath(credentials_filepath: str):
    credentials_filepath = os.path.expanduser(credentials_filepath)
    if not os.path.isfile(credentials_filepath):
        return None
    with open(credentials_filepath, "r") as f:
        credentials_json = f.read()
    try:
        credentials = json.loads(credentials_json)
        return HashboardAccessKeyClientCredentials(
            project_id=credentials["project_id"],
            access_key_id=credentials["access_key_id"],
            access_key_token=credentials["access_key_token"],
        )
    except Exception as e:
        raise RuntimeError("Invalid credentials file.") from e
