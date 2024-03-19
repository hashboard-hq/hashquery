# Models

```{eval-rst}
.. currentmodule:: hashquery
```

Attributes, measures, and relations are the fundamental building blocks of
Hashquery models. They define what is interesting about your data, and allow
you to share complex expressions across your analysis with ease.

## Attributes

- To add attributes, use {py:meth}`Model.with_attributes`.
- To use attributes, use `attr.attribute_name` or `attr["attribute_name"]`
  anywhere a column expression is needed.

## Measures

- To add measures, use {py:meth}`Model.with_measures`.
- To use measures, use `msr.measure_name` or `msr["measure_name"]`
  anywhere an aggregating column expression is needed.

## Relations

- The most common way to add a relation is with a single join, using
  {py:meth}`Model.with_join_one`.
- To use the attributes on a joined relation, use `rel.relation_name.attribute_name`
  or `rel["relation_name"].attribute_name` anywhere a column expression is needed.
- To reference the relation itself (which is fairly rarely needed),
  use just `rel.relation_name`.
