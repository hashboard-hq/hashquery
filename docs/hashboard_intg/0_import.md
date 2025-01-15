# Import

```{eval-rst}
.. currentmodule:: hashquery
```

All your Hashboard models, metrics and data connections are available within
Hashquery. You can see an overview of your project by simply printing it:

```python
from hashquery.integration.hashboard import HashboardProject
project = HashboardProject()
print(project)
```

## Models

Models can be accessed using `project.models.my_model_alias`.

Models in Hashboard store additional metadata, such as column descriptions, change history, dbt metadata, and others. Hashquery does not yet support all these options.

## Metrics

Metrics can be accessed using `project.metrics.my_metric_alias`.

Metrics are imported into Hashboard as a Model. When {py:meth}`run <Model.run>`,
they will fetch the underlying data for their sparkline.

## Connections

Connections can be accessed using `project.connections.my_connection_name`.

The name of the connection is a lowercased version of the name as it appears
in the Hashboard UI, with spaces converted to underscores. You can
`print(project.connections)` to see all the names if you need help in finding
the name for your connection.

Connections are just a reference; all processing occurs on Hashboard servers
and no database credentials are sent to Hashquery clients.
