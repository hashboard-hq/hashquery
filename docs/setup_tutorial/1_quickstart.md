# Quickstart

```{include} /_fragments/alpha_notice.md

```

## Installation

Hashquery is installed with `pip` from [PyPI](https://pypi.org/project/hashquery/). Hashquery requires Python version 3.6 or above.

The Hashquery package includes a variety of [pip extras](https://packaging.python.org/en/latest/specifications/dependency-specifiers/#extras). These can be used to specify which database driver(s) you wish to install.

The following drivers are currently available for installation:

| Database                                      | Install                           |
| --------------------------------------------- | --------------------------------- |
| [DuckDB](https://duckdb.org/)                 | `pip install hashquery[duckdb]`   |
| [BigQuery](https://cloud.google.com/bigquery) | `pip install hashquery[bigquery]` |
| [Hashboard](../hashboard_intg/0_import.md)    | `pip install hashquery`           |

---

For this quickstart, we'll use `[duckdb]` and just work with locally defined data frames.

```bash
pip install hashquery[duckdb]
```

## Validate the installation

The following is a simple script which loads some mock data into a dataframe and then queries its first 2 records:

```python
from hashquery import *
import pandas as pd

# create a new duckdb instance populated with a demo dataset
db = Connection.duckdb(
   people=[
      {"name": "Alice", "age": 25, "city": "New York"},
      {"name": "Bob", "age": 32, "city": "New York"},
      {"name": "Cassie", "age": 30, "city": "Chicago"},
      {"name": "David", "age": 42, "city": "Houston"},
   ]
)

# create our first model, just a view over the "people" table
people = Model(db, "people")

# query the model, just grabbing the first 2 records
print(people.limit(2).run().df)
```

When you run this script, you should see Alice and Bob's information echoed back to you.
