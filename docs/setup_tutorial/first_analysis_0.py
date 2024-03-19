from hashquery import *
from hashquery.demo import demo_project

connection = demo_project.connections.uploads
sales = Model().with_source(demo_project.connections.uploads, "sales.parquet")
total_sales = sales.aggregate(groups=[], measures=[func.count()])
print(total_sales.run().df)
