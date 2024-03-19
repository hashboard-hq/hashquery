# Advanced KeyPaths

```{eval-rst}
.. currentmodule:: hashquery
```

A small audience of readers may have noticed that our use of `attr.`, `msr.`
and `rel.` have some special behavior compared with other values in Python.
If this is you, then you are very observant!

Other guides intentionally omit technical detail about how some magic
works to get you started with Hashquery.

If you are building abstractions on top of Hashquery, you may want to better
understand how these work.

## Integrating `attr`, `msr` and `rel` into your APIs

If you are building abstractions on top of Hashquery, you may want your
APIs to accept the lazy KeyPath references emitted by `attr`, `msr` and `rel`.
That way, using your extended functions will work just as well as using the
built-in ones.

### Deferring KeyPaths

If your API takes in an `attr`, `msr` or `rel` as an input, but _not_ a model
reference, then you likely want to _defer_ evaluation until the references
are resolved.

To do so, decorate your function with
{py:func}`~utils.keypath.resolve.defer_keypath_args`.

This decorator will defer evaluation of its implementation until all its
arguments are ready. If the function without the decorator returned `T`, it
now will return `KeyPath[T]`. The function is not made asynchronous, it just
now generates KeyPaths. You do not need to modify the implementation.

> For example, the implementation of {py:func}`func.count_if` is wrapped with
> {py:func}`~utils.keypath.resolve.defer_keypath_args`, since it accepts an
> expression as input, that may have references to `attr.` or `rel.`.
>
> ```python
> @defer_keypath_args
> def count_if(condition: ColumnExpression) -> ColumnExpression:
>     return func.sum(func.cases((condition, 1), other=0))
> ```

### Resolving KeyPaths

If your API takes in an `attr`, `msr` or `rel` as an input, and also a model
reference, then you likely want to _resolve_ references against that input model.

> If you want to resolve references against another model that _isn't_ a parameter
> of the function, then you will still use
> {py:func}`~utils.keypath.resolve.defer_keypath_args`.

To do so, decorate your function with
{py:func}`~utils.keypath.resolve.resolve_keypath_args_from`.

> Most methods on `Model` use this decorator, resolving any passed parameters
> against `self`. For example:
>
> ```python
> class Model():
>   @resolve_keypath_args_from(_.self)
>   def sort(self, expr: ColumnExpression, dir: str) -> "Model":
>     ...
> ```

## The WHY and HOW of KeyPaths

Some users may be more curious to learn about why Keypaths are even
included, since they're kinda unconventional. The following discussion
presents a small slice of our reasoning behind their inclusion.

:::{warning}
This section is an advanced guide that assumes more familiarity with Python
than other articles on this page. We anticipate most consumers to never need
to fully understand how `attr`, `msr` and `rel` work at a language-level.

For those brave enough to stick around, let's get into it.
:::

### The problem: Building self-referential data structures

Users in Hashquery are constructing immutable, self-referential data structure.
A method like {py:meth}`Model.with_attributes` adds new attributes
and a method like {py:meth}`Model.aggregate` may use those attributes.

Let's naively write how a user may call these functions, and pay specific
attention to the line where we reference an attribute. This is not valid
Hashquery code:

```{code-block} python
:emphasize-lines: 6
sales_by_year = (
  Model()
  .with_source(project.connections.uploads, "sales.parquet")
  .with_attributes("timestamp")
  .aggregate(
    groups=[sales_by_year.get_attribute("timestamp").by_year],
    measures=[func.count()]
  )
)
```

There's something off here. We can't reference `sales_by_year` in the definition
of `sales_by_year`! It doesn't actual _exist_ yet.

Okay, but perhaps we can declare it first:

```python
sales_by_year = Model()
sales_by_year = (
  sales_by_year
  .with_source(project.connections.uploads, "sales.parquet")
  .with_attributes("timestamp")
  .aggregate(
    groups=[sales_by_year.get_attribute("timestamp").by_year],
    measures=[func.count()]
  )
)
```

This actually still won't work, since `sales_by_year`, on the line where
we call `sales_by_year.get_attribute` will still only be equal to `Model()`,
which doesn't have any `timestamp` attribute!

If we really wanted this syntax, we'd have to break up every expression
to its own line, building up `sales_by_year` statement by statement:

```python
sales_by_year = Model()
sales_by_year = sales_by_year.with_source(project.connections.uploads, "sales.parquet")
sales_by_year = sales_by_year.with_attributes("timestamp")
sales_by_year = (
  sales_with_attr
  .aggregate(
    groups=[sales_by_year.get_attribute("timestamp").by_year],
    measures=[func.count()]
  )
)
```

That's ... pretty ugly, and prone to error.

Another potential approach could be to _mutate_ our sales model with each
function, but we found that created really difficult to understand bugs
in user code which required manual `deepcopy`s to resolve, and in some cases
still didn't fully fix the issue.

### The solution: deferred evaluation

We want a way to allow users to _define what to access_ before any access
actually occurs. This is a common pattern in functional programming contexts,
where lazy evaluation is often preferred, such as in Publisher/Consumer or
Reactive state libraries.

Let's see how that would look with our query:

```{code-block} python
:linenos:
sales_by_year = (
  Model()
  .with_source(project.connections.uploads, "sales.parquet")
  .with_attributes(lambda model: "timestamp")
  .aggregate(
    groups=[lambda model: model.get_attribute("timestamp").by_year],
    measures=[lambda model: func.count()]
  )
)
```

As you can see, in every place where we might want to self-reference the model,
we're now passing a _function_ which declares _how_ to access some value.
At some later point, the library will actually run these lambdas and get
real values.

This is more _correct_, but quite the headache.

So we built {py:class}`~hashquery.utils.keypath.keypath.KeyPath`.

### KeyPaths

{py:class}`~hashquery.utils.keypath.keypath.KeyPath` is just a structured
helper for making the lambdas in the prior example.

```python
KeyPath().get_attribute("timestamp").by_year
# declares the same thing as
lambda root: root.get_attribute("timestamp").by_tear
```

Instead of a `lambda`, we store a structured representation of the accessor.
Unlike a `lambda`, these can be serialized, immutably modified, and can be
reflected to aid in error messages or the automatic naming of resources.

In Hashquery, there are three root KeyPaths, `attr`, `msr` and `rel`.
These are each shorthand to access a model's attributes, measures, and
relations respectively:

```python
attr.timestamp.by_year
# is roughly
lambda model: model._private_api_get_attribute("timestamp").by_year
```
