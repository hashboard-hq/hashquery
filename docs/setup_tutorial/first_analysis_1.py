from hashquery import *
from hashquery.demo import demo_project

connection = demo_project.connections.uploads
sales = Model().with_source(demo_project.connections.uploads, "sales.parquet")
sales_by_year = (
    sales.aggregate(
        groups=[column("timestamp").by_year],
        measures=[func.count()],
    )
    .sort(column("timestamp"))
)
print(sales_by_year.run().df)
