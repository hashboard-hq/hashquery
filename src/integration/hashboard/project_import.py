from typing import *

from ...model.connection.hashboard_data_connection import HashboardDataConnection
from ...model.model import Model
from ...utils.secret import Secret
from .api import HashboardAPI
from .credentials import HashboardClientCredentials


def fetch_all_models(credentials: HashboardClientCredentials) -> List[Model]:
    """
    Fetches all the models in the project.
    """
    wire_models = HashboardAPI(credentials).graphql(
        """
        query HashqueryModels($projectId: String!) {
            hashqueryModels(projectId: $projectId)
        }
        """,
        {"projectId": credentials.project_id},
    )["data"]["hashqueryModels"]
    models = [Model._from_wire_format(wire) for wire in wire_models]
    rehydrate_model_credentials(models, credentials)
    return models


def fetch_all_project_metrics(credentials: HashboardClientCredentials) -> List[Model]:
    """
    Fetches all the project metrics in the project, converted to Hashquery models.

    This will respect all settings on the project metrics, including filters, joins, goal lines, granularity, etc.
    """
    wire_models = HashboardAPI(credentials).graphql(
        """
        query HashqueryModelsFromProjectMetrics($projectId: String!) {
            hashqueryModelsFromProjectMetrics(projectId: $projectId)
        }
        """,
        {"projectId": credentials.project_id},
    )["data"]["hashqueryModelsFromProjectMetrics"]
    models = [Model._from_wire_format(wire) for wire in wire_models]
    rehydrate_model_credentials(models, credentials)
    return models


def fetch_all_connections(
    credentials: HashboardClientCredentials,
) -> List[HashboardDataConnection]:
    """
    Fetches all the data connections in the project.
    """
    raw_data_connections = HashboardAPI(credentials).graphql(
        """
        query HashqueryDataConnections($projectId: String!) {
            dataConnections(projectId: $projectId) { id, name }
        }
        """,
        {"projectId": credentials.project_id},
    )["data"]["dataConnections"]
    return [
        HashboardDataConnection(
            id=wire["id"],
            project_id=credentials.project_id,
            name=wire["name"],
            credentials=Secret(credentials),
        )
        for wire in raw_data_connections
    ]


def rehydrate_model_credentials(
    models: List[Model],
    credentials: HashboardClientCredentials,
):
    for model in models:
        if isinstance(model._connection, HashboardDataConnection):
            model._connection.credentials = Secret(credentials)
