from typing import *

import requests

from .credentials import HashboardClientCredentials


class HashboardAPI:
    def __init__(self, credentials: HashboardClientCredentials) -> None:
        self.credentials = credentials

    def post(self, route: str, payload: dict) -> dict:
        response = requests.post(
            f"{self.credentials.base_uri}/{route}",
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
