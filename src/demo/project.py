import requests

from ..hashboard_api.api import HashboardAPI
from ..hashboard_api.credentials import HashboardAccessKeyClientCredentials
from ..hashboard_api.project_manifest import ProjectManifest

DEMO_API_KEY_URI = "https://cdn.hashboard.com/hashquery-demo/apiKey"


def _get_demo_project():
    try:
        response = requests.get(DEMO_API_KEY_URI)
    except Exception as e:
        raise RuntimeError("Unable to load the demo API key.") from e
    if response.status_code != 200:
        raise RuntimeError("Unable to load the demo API key.")
    demo_api_key = response.text
    demo_creds = HashboardAccessKeyClientCredentials.from_encoded_key(demo_api_key)
    HashboardAPI.register_project(
        credentials=demo_creds, base_uri="https://hashquery.dev"
    )
    return ProjectManifest(demo_creds.project_id)


demo_project = _get_demo_project()
