from hashquery import *
from hashquery.hashboard_api.project_importer import ProjectImporter

connection = demo_project.connections.uploads
sales = (
    Model()
    .with_source(demo_project.connections.uploads, "sales.parquet")
    .with_attributes(sale_year=column("timestamp").by_year)
    .with_measure(total_sales=func.count())
)
sales_by_year = (
    sales.aggregate(
        groups=[attr.sale_year],
        measures=[msr.total_sales],
    )
    .sort(attr.sale_year)
)
print(sales_by_year.run().df)
