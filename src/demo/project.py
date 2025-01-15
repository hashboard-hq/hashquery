import requests

from ..integration.hashboard.credentials import HashboardAccessKeyClientCredentials
from ..integration.hashboard.hashboard_project import HashboardProject

DEMO_API_KEY_URI = "https://cdn.hashboard.com/hashquery-demo/apiKey"


def _get_demo_project():
    try:
        response = requests.get(DEMO_API_KEY_URI)
    except Exception as e:
        raise RuntimeError("Unable to load the demo API key.") from e
    if response.status_code != 200:
        raise RuntimeError("Unable to load the demo API key.")
    demo_api_key = response.text
    demo_credentials = HashboardAccessKeyClientCredentials.from_encoded_key(
        demo_api_key,
        base_uri="https://hashquery.dev",
    )
    return HashboardProject(demo_credentials)


demo_project = _get_demo_project()
