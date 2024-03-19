# Import

```{eval-rst}
.. currentmodule:: hashquery
```

All your Hashboard models, metrics and data connections are available within
Hashquery. You can see an overview of your project by simply printing it:

```python
from hashquery import project
print(project)
```

Currently, you can only _pull_ content from Hashboard.
In future versions, you'll be able to _push_ content created in Hashquery back
into Hashboard.

## Models

Models can be accessed using `project.models.my_model_alias`.

Models in Hashboard are meant to be 1:1 with Models in Hashquery. At this
phase in development however, there are some known differences:

- Models in Hashboard with Custom SQL attributes or measures may use
  interpolation (`{{ }}`) and disambiguation syntax (`alias.`) that isn't valid
  inside of all queries that Hashquery generates.

- Models in Hashboard store additional metadata, such as column descriptions,
  cache TTLs, change history, dbt metadata, and others. Hashquery does not yet
  support all these options.

- Hashboard and Hashquery use separate caches.

## Metrics

Metrics can be accessed using `project.metrics.my_metric_alias`.

Metrics are imported into Hashboard as a Model. When {py:meth}`run <Model.run>`,
they will fetch the underlying data for their sparkline.

At this time, some aspects of metrics are not fully supported when imported.

- Metric filters are not consistently supported, and may be silently ignored.

- Hashboard and Hashquery use separate caches.

## Connections

Connections can be accessed using `project.connections.my_connection_name`.
Connections can be passed into the first parameter of {py:meth}`Model.with_source`.

The name of the connection is a lowercased version of the name as it appears
in the Hashboard UI, with spaces converted to underscores. You can
`print(project.connections)` to see all the names if you need help in finding
the name for your connection.

Connections are just a reference; all processing occurs on Hashboard servers
and no database credentials are sent to Hashquery clients.
