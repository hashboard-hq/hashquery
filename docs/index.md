# Hashquery

## A Python framework for modeling and querying business data inside of your data warehouse

Logic shouldn’t be trapped in any tool, especially not your BI tool.
But data teams are often forced to choose between doing their modeling with proprietary
DSLs like LookML or adding complex business logic into raw SQL, where it can't be
easily re-used or extended. That’s why we’re building **hashquery**.

Hashquery expressions are defined in Python, compiled into SQL, and run directly
against your data warehouse. It is capable of expressing complex, multi-layered
data queries, way beyond the capabilities of standard SQL.
It natively integrates with upstream semantic layers.
And unlike a DSL, hashquery is fully composable and extensible — it’s just Python.

```{include} /_fragments/alpha_notice.md

```

```{toctree}
:caption: Getting Started
:glob:
:hidden:

setup_tutorial/*
```

```{toctree}
:caption: Concepts
:glob:
:hidden:

concept_explanations/*
```

```{toctree}
:caption: Common Patterns
:glob:
:hidden:

pattern_guides/*
```

```{toctree}
:caption: Hashboard Integration
:glob:
:hidden:

hashboard_intg/*
```

```{toctree}
:caption: API Reference
:glob:
:maxdepth: 1
:hidden:

api_reference/*
```

```{toctree}
:caption: Project
:glob:
:hidden:

project_info/*
```
