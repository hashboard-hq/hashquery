# Joining Tables

Hi there! We're working hard on shipping the first version of
Hashquery, and have not yet had time to compile a detailed guide
on joining.

In the meantime, we've decided to just drop in a complete example of
building two models from scratch and then joining them:

```python
from hashquery import *

products = (
    Model()
    .with_source(demo_project.connections.uploads, "products.parquet")
    .with_attributes(
        "id",
        "pizza_size",
        "pizza_type",
    )
    .with_primary_key(attr.id)
)

sales = (
    Model()
    .with_source(demo_project.connections.uploads, "sales.parquet")
    .with_attributes(
        "timestamp",
        "product_id",
    )
    .with_join_one(
        products,
        named="product",
        foreign_key=attr.product_id,
        # don't join in large pizzas, not for any good reason other
        # than to demonstrate the use of `condition` to describe an
        # arbitrary join
        condition=rel.product.pizza_size != "Large",
    )
)

query = sales.aggregate(
    groups=[rel.product.pizza_size],
    measures=[func.count()],
).sort(attr.pizza_size)

results = query.run()
print(result.sql_query)
print(result.df)
```
