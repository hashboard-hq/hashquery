from typing import Any
from ..utils.env import env_with_fallback
from .project_manifest import ProjectManifest
from .api import HashboardAPI
from .credentials import load_client_credentials_from_env


default_credentials = load_client_credentials_from_env()
base_uri = (
    env_with_fallback("HASHQUERY_API_BASE_URI", "HASHBOARD_CLI_BASE_URI")
    or "https://hashboard.com"
)


class ObjectThatRaisesAnErrorIfYouTouchIt:
    # This is a fake object that explodes if you touch it.
    # This is used as the `default_project` object if resources failed to load.
    #
    # We don't want to raise an error unless the line of code in which you
    # actually try to use the default project. This defers that error.
    __doc__ = ProjectManifest.__doc__

    def __getattr__(self, __name: str) -> Any:
        raise AttributeError(
            "Could not authenticate to Hashboard services. "
            + "No credentials were found."
        )


if default_credentials:
    HashboardAPI.register_project(default_credentials, base_uri=base_uri)
    default_project: ProjectManifest = ProjectManifest(default_credentials.project_id)
else:
    default_project: ProjectManifest = ObjectThatRaisesAnErrorIfYouTouchIt()
