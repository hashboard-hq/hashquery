# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
This project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).


## [0.3.0] - 2025-01-15
This release adds the first support for running Hashquery locally without needing a Hashboard account or project (although having one will allow Hashquery to take advantage of more advanced features). This version adds DuckDB (local files) and BigQuery (cloud) support. More connection types will come very soon.

### Installation
By default, `pip install hashquery` will only install shared Hashquery code. To install a specific data connector you need to specify them as pip extras.
- `pip install hashquery[duckdb]` for DuckDb support.
- `pip install hashquery[bigquery]` for BigQuery support.

### Breaking Changes
- The wire format version has increased. The Hashboard app server will no longer accept requests from Hashquery clients below this version.
- `hashquery.project` is no longer a global export. To connect to a Hashboard project, use the following:
```py
from hashquery.integrations.hashboard import HashboardProject
project = HashboardProject()
```
- `Model.with_source` has been changed to no longer accept a data connection. This API now only overwrites the source. `Model.with_connection` has been added to override the connection.
  - These APIs are now more niche; with the introduction of new Model constructors (see below), you shouldn't need to use them nearly as often.
- The `hashquery.func` module has been moved into `hashquery.model.func`. Note that `func` remains exported from `hashquery` directly, so this change should not affect consumers writing code like `func.sum` (recommended as not to shadow the Python `sum` function). If you imported each `func` method individually, you will need to update your import: `from hashquery.func import sum` to `from hashquery.model.func import sum`.
- `Serializable`'s methods (`to_wire_format` and `from_wire_format`) are now marked _internal_ (renamed to `_to_wire_format` and `_from_wire_format`). External clients are discouraged from calling these methods.

### Added
- Connect to, populate, and query a local in-memory DuckDB instance with `Connection.duckdb(*files)`.
- Connect to BigQuery databases with `Connection.bigquery()`.
- Creating a Model with a connection and source is now possible with a single call to the initializer: `Model(connection, table, *, schema)` or `Model(connection, *, sql_query)`.
- Simplified and added additional options to authenticate to a Hashboard project in environments where files and environment variables cannot be configured (such as in HEX or Jupyder).



## [0.2.2] - 2024-12-19
This version packages a variety of improvements to the expressive power of Hashquery, focused primarily on improving and extending the power of event analytics.

### Breaking Changes
- `Model.funnel` now calls the top of funnel step "entities" instead of "count". This can be controlled with the new `top_of_funnel_name` option.

### Added
- Funnel conversion rates can now be calculated with `Model.funnel_conversion_rate`
- `Model.match_steps` and all funnel APIs now support partition groups.
- `Model.match_steps` and all funnel APIs now support a `time_limit` option.
- `Model.match_steps` and all funnel APIs now supports arbitrary conditions for steps. Using a string literal is still supported when just matching against an event type.
- `Model.funnel` now supports removing the "top of funnel count" via the new `top_of_funnel_start_index` parameter.
- `Model.funnel` APIs now support `post_match_filter` for filtering match journeys before aggregating them into counts.
- `ColumnExpression.format_timestamp` is a new API for formatting date(time) values into strings uniformly across all dialects.
- `column(sql="*")` can now be used to render a literal star into SQL. `column(sql="*").disambiguated(rel.joined_model)` can render a namespaced star `joined_model.*`. Using `SELECT *` will always be an escape hatch with quirks, as the Hashquery compiler will not be able to reflect the columns of the output ahead of time. We recommend using `*attr` or `*rel.joined_model` over `column(sql="*")` when possible.
- `Model.sort` now supports a `nulls=` option for explicit control over how nulls are ordered. The default, `"auto"` retains the existing behavior ("first" when ascending, "last" when descending).

### Fixed
- `Model.match_steps` and all funnel APIs now support using the same condition multiple times in a single step sequence.
- `Model.funnel` consistently sorts steps in the final table.
- `Model.funnel` now returns an overall count of entities when there are no steps supplied.
- Fixed `Model.with_join_one` failing for certain conditions using the magic `attr` and `rel` accessors.
- Numerous fixes for a variety of cases across a variety of dialects.



## [0.2.1] - 2024-04-01
First public beta version.
