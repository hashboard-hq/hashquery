# Binning

```{eval-rst}
.. currentmodule:: hashquery.model.column_expression
```

## Binning Timestamps

Timestamps can be binned into {py:meth}`days <ColumnExpression.by_day>`,
{py:meth}`weeks <ColumnExpression.by_week>`,
{py:meth}`months <ColumnExpression.by_month>`, etc.
using `attr.your_timestamp.by_<granularity>`.

This can be used to quickly {py:meth}`hashquery.Model.aggregate` data into
timestamp bins:

```python
yearly_sales = (
  demo_project.models.sales
  .aggregate(
    groups=[attr.timestamp.by_year],
    measures=[msr.revenue],
  )
  .sort(attr.timestamp)
)
```

## Binning Numerics

:::{note}
In future, Hashquery will provide `ColumnExpression.binned` to
achieve this behavior more easily.
:::

You can write your own binning manually using
{py:func}`func.cases <hashquery.func.cases>`.

```python
sales_binned_by_price = (
  demo_project.models.sales
  .aggregate(
    groups=[func.cases(
      func.cases(
        ((attr.item_price > 0 & attr.item_price < 10), "0-10"),
        ((attr.item_price >= 10 & attr.item_price < 20), "10-20"),
        other="30+",
      )
    )],
    measures=[msr.count],
  )
)
```
