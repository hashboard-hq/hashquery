from typing import *
import requests

from .credentials import HashboardClientCredentials


class HashboardAPI:
    def __init__(
        self,
        project_id: str,
        credentials: HashboardClientCredentials,
        base_uri: Optional[str] = None,
    ) -> None:
        self.project_id = project_id
        self.credentials = credentials
        self.base_uri = base_uri

    _project_lookup: Dict[str, "HashboardAPI"] = dict()

    @classmethod
    def register_project(
        cls,
        credentials: HashboardClientCredentials,
        base_uri: str,
    ):
        cls._project_lookup[credentials.project_id] = HashboardAPI(
            credentials.project_id,
            credentials,
            base_uri,
        )

    @classmethod
    def get_for_project(cls, project_id: str) -> "HashboardAPI":
        if api := cls._project_lookup.get(project_id):
            return api
        raise ValueError(f"No credentials found to connect to project `{project_id}`.")

    # -------------

    def post(self, route: str, payload: dict) -> dict:
        response = requests.post(
            f"{self.base_uri}/{route}",
            json=payload,
            headers=self.credentials.get_headers(),
        )
        if response.status_code == 200:
            return response.json()
        else:
            try:
                response_json = response.json()
                user_facing_error = response_json["error"]
            except:
                raise RuntimeError(
                    f"Request failed with status code {response.status_code}. Response:\n"
                    + response.text
                )
            else:
                raise RuntimeError(user_facing_error)

    def graphql(self, query, variables=None):
        payload = {"query": query}
        if variables:
            payload["variables"] = variables
        return self.post("graphql/", payload)
