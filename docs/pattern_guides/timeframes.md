# Comparing Time Frames

Hi there! We're working hard on shipping the first version of
Hashquery, and have not yet had time to compile a detailed guide
on period over period analysis, nor build native tools within the
framework for it.

In the meantime, we've decided to just drop in a code snippet
that may be adaptable to your needs.

```python
from hashquery import *
from datetime import timedelta

sales = project.models.sales
revenue_by_week = (sales
  .with_attributes(
    week=attr.timestamp.by_week,
    prev_week=attr.timestamp.by_week - timedelta(days=7),
    prev_year=(attr.timestamp - timedelta(weeks=52)).by_week)
  .aggregate(groups=[attr.week, attr.prev_week, attr.prev_year], measures=[msr.revenue])
  .with_primary_key(attr.week)
)

summary = (revenue_by_week
  .with_join_one(revenue_by_week, foreign_key=a.prev_week, named="prev_week")
  .with_join_one(revenue_by_week, foreign_key=a.prev_year, named="prev_year")
  .aggregate(groups=[
    attr.week,
    attr.revenue.named("this_week_sales"),
    rel.prev_week.revenue.named("last_week_sales"),
    rel.prev_year.revenue.named("last_year_sales"),
    (attr.revenue - rel.prev_week.revenue).named("wow_change"),
    (attr.revenue - rel.prev_year.revenue).named("yoy_change")
  ])
  .sort(attr.week)
  .limit(5)
)
results = summary.run()
print(results.sql_query)
print(results.df)
```
