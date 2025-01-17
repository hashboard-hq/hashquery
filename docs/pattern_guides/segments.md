# Comparing Segments

Example of user segmentation:

```python
from hashquery import *

# What's the average lifetime value of loyal customers this quarter?
sales = project.models.sales
loyal_customers = (sales
  .filter(rel.orders.loyalty_status == 'Gold Member')
  .filter(attr.timestamp.is_this_quarter)
  .aggregate(groups=[func.distinct(attr.customer_id)])
)
ltv = (sales
  # join to loyal_customers
  .with_join_one(
    loyal_customers,
    condition=a.customer_id == rel.loyal_customers.distinct_customer_id,
    named="loyal_customers")
  # drop any sales which were not for a loyal customer
  .filter(rel.loyal_customers.distinct_customer_id != None)
  # analysis from here on is with just the loyal customers
  .with_measures(total_revenue=func.sum(attr.item_price * attr.quantity))
  .aggregate(groups=[attr.customer_id], measures=[msr.total_revenue])
  .aggregate(measures=[func.avg(attr.total_revenue)])
)
results = ltv.run()
print(results.sql_query)
print(results.df)
```
