# Your first analysis

```{eval-rst}
.. currentmodule:: hashquery
```

This first query will use the demo project to get started.

You can choose to adapt this to your data if you choose. If you get stuck importing
some content from your project, try `print(project)` to show all your project's
content.

## Getting the count of items

Let's start simple. We'll import a connection, and count the amount of records
in a table.

First, let's import our connection. This does not actually connect to your data
warehouse, it is just a reference as to what data connection will be
used later on.

```{literalinclude} first_analysis_0.py
:end-at: connection =
:lineno-match:
```

Next, we'll define a simple {py:class}`Model` for a table. This model represents
all the records in the table named `sales.parquet`.

```{literalinclude} first_analysis_0.py
:start-at: sales =
:end-at: sales =
:lineno-match:
```

Let's now sub-model this to form a new model. While the previous model
represented _all records_ in the table, this new model will represent
the total count of the records in the table.

We can do so by calling {py:func}`Model.aggregate` with no groups, and a single
measure: {py:func}`func.count`.

```{literalinclude} first_analysis_0.py
:start-at: total_sales =
:end-at: total_sales =
:lineno-match:
```

Finally, we'll run the model ({py:func}`Model.run`) and collect the result
as a Pandas DataFrame ({py:attr}`~Model.df`):

```{literalinclude} first_analysis_0.py
:start-at: print
:lineno-match:
```

When you run this script, you should should see a printed item containing a
value for the count of records in the table.

If you want to see the SQL that ran, you can reference `.sql_query` on the
value returned by {py:func}`Model.run`:

```python
result = total_sales.run()
print(result.sql_query) # the query
print(result.df) # the data
```

:::{dropdown} Code so far

```{literalinclude} first_analysis_0.py
:linenos:
```

:::

## Breaking out by groups

Let's revisit our analysis. Instead of gathering a count of all records,
let's group the records into buckets by year, and count the totals within
each bucket.

```{literalinclude} first_analysis_1.py
:start-at: sales_by_year =
:lineno-match:
```

This should now show you the top 3 years on record.

:::{dropdown} Code so far

```{literalinclude} first_analysis_1.py
:linenos:
```

:::

## Modeling & referencing properties

Our queries work well, but they aren't very reusable. Anytime somebody needs
to reference the columns, they need to find the physical name of the column
in the database table, which could change.
Similarly, our measure may become more complex, accounting for business logic
about double counting, and if the logic was spread across many queries, you
would have to update it in many places.

What we want to do is have a layer between the raw expressions and the
semantics of the model. We'll use the model as a centralized, shared definition
of what's interesting about the table.

For this tutorial, we'll just attach attributes and measures.

- **Attributes** are expressions which are a property of _an individual record_.
- **Measures** are expressions which are a property of _a group of records_.

You can attach attributes and measures onto a model using
{py:func}`Model.with_attributes` and {py:func}`Model.with_measures` respectively.
We'll attach these to our base model, so any analysis using this table can
reuse them.

```{literalinclude} first_analysis_2.py
:start-at: sales =
:lines: -6
:lineno-match:
```

We can then update our references in our sub-model to use the new definitions.
In HashQuery, we reference attributes on models using `attr.`, measures using
`msr.` and relations using `rel.`.

```{literalinclude} first_analysis_2.py
:start-at: sales_by_year =
:lineno-match:
```

Now our sub-model query will automatically adjust if we change the definition
for `my_attribute` or `my_measure`. In addition, `sales` now has more
metadata about what's interesting about the table, which allows tools in
Hashboard to offer better UIs for non-technical consumers of your data.

:::{dropdown} Final code

```{literalinclude} first_analysis_2.py
:linenos:
```

:::

## Next Steps

You can learn about the core concepts and principles of Hashquery under the
**Concepts** sidebar. For further examples, check out **Common Patterns**. API
documentation can be found under **API Reference**.

Have fun querying! [Please let us know if you have any feedback, or encounter any issues](/project_info/feedback.md).
