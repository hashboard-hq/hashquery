# Visualizations

Hashquery's primary goal is in processing complex analytics in your warehouse
and returning data back to you. Hashquery does not come with any visualization
library built-in.

Hashquery however does return {py:class}`pandas.DataFrame`s,
which many visualization libraries know how to work with. `mathplotlib` comes
bundled with some installations of Pandas and can be used directly:

```python
query = (
    demo_project.models.sales
    .aggregate(groups=[attr.timestamp.by_year], measures=[msr.revenue])
    .sort(attr.timestamp)
)
query.df().plot(x="timestamp", y="revenue")
```

We also recommend [Altair](https://altair-viz.github.io/), an implementation
of the [VegaLite](https://vega.github.io/vega-lite/) spec for Python.
