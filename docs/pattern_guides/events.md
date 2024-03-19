# Event Analysis

```{eval-rst}
.. currentmodule:: hashquery
```

Hashquery provides out of the box support for event analytics such as funnels
and retention curves.

## Modeling event analytics

We recommend defining a model's [activity schema](https://www.activityschema.com/)
using {py:meth}`Model.with_activity_schema`. This attaches a semantic description
to your model on how to apply event analytics functions to it. It requires:

- **group** – This will split the events for each actor that invokes the event, such as `user_id` or `customer_id`.
- **timestamp** – When the event was detected, such as `created_at` or `timestamp`.
- **event_key** – Typically this is a column like `event_name` or `event_type` denoting the kind of event that occurred.

## Funnels

The simplest event analytics primitive is {py:meth}`Model.funnel`. Funnel
takes a list of events and returns a summary table indicating how many distinct
actors (split by `activity_schema.group`) reached each stage of the funnel.

```python
events.funnel(
  # replace these with the events sequence you care about
  "ad_impression",
  "visit",
  "purchase",
)
```

May result in a table like the following:

| step          | count  |
| ------------- | ------ |
| count         | 30,040 |
| ad_impression | 27,028 |
| visit         | 8,794  |
| purchase      | 2,341  |

This means `30,040` distinct users exist in our dataset, and `27,028` of them
were served at least one ad. Of those users, `8,794` went to the website and
then `2,341` made at least one purchase.

Note that if a user purchased something, but didn't see an ad _before_ their
purchase, they wouldn't be included in the funnel. They would only counted in
the `count` row. Users must match every proceeding step _in order_ to be
included in step in the funnel. For this reason, funnels _strictly decrease_
as they move downward.

### Comparing Funnels

Let's imagine we want to compare how well one funnel behaves compares to
another. For example, we may be running two ad campaigns at once, and curious to
see which is resulting in more conversions.

Let's call seeing the first ad event `ad_impression_a` and the second ad
`ad_impression_b`. If your events table does not capture this inside of
a single column, you might form the expression using an expression
like the following, concatenating the event name with the user group:

```python
events = events.with_activity_schema(
  # ...existing
  event_key=func.cases(
    ((attr.event_name == "ad_impression"), attr.event_name + "_" + rel.user.ab_test_group)
    other=attr.event_name
  )
)
```

Once we have our events set up, let's define two funnels:

```python
funnel_a = events.funnel("ad_impression_a", "visit", "purchase")
funnel_b = events.funnel("ad_impression_b", "visit", "purchase")
```

and then we'll join them together to form a new table:

```python
funnels = (
  funnel_a.with_join_one(
    funnel_b,
    named="funnel_b",
    condition=func.or_(
      attr.step == rel.funnel_b.step,
      func.and_(
        attr.step == "ad_impression_a",
        rel.funnel_b.step == "ad_impression_b",
      )
    )
  )
  .pick(
    attr.step,
    attr.count.named("A funnel"),
    rel.funnel_b.count.named("B funnel"),
  )
)
```

| step          | A funnel | B funnel |
| ------------- | -------- | -------- |
| count         | 15,020   | 15,020   |
| ad_impression | 14,308   | 12,720   |
| visit         | 2,594    | 6,200    |
| purchase      | 2,101    | 240      |

Looks like ad B may have been more evocative, as more people clicked into
the website, but it lead to less purchases overall. Maybe the ad misinformed
users? Meanwhile, while A didn't capture as many clicks, it lead to a lot
of purchases. Perhaps we could make a new ad that has the benefits of both.
Time to contact marketing.

## Retention/Survival Curves

In future, Hashquery will provide `Model.retention` to
achieve retention and survival curves quickly. For now, you will need to use
`.match_steps` to form your own retention curves.

## Advanced: `.match_steps`

{py:meth}`Model.match_steps` is the core building block that other analytics
functions use. It allows you to efficiently collect and join together an ordered
list of events for a user.

Similar to `.funnel` we pass it an ordered list of steps to match:

```python
events.match_steps(
  "ad_impression",
  "visit",
  "purchase",
)
```

Unlike `.funnel`, which aggregates into a summary table, `.match_steps` actually
returns a table aggregated by `activity_schema.group`. For each row, you have
columns representing each event matched by the analysis, modeled as a JOIN
to the original events table. Effectively this creates a series of ["first after" temporal joins](https://www.activityschema.com/temporal-joins/first-after).

That's a little hard to picture, so let's draw it out.

Imagine our events table looks like the following, containing a single user.
This table now includes all the properties of our event, not just the ones
we added to `.with_activity_schema`.

| group  | timestamp  | event_name    | value  | event_id |
| ------ | ---------- | ------------- | ------ | -------- |
| user_a | 2024-01-01 | ad_impression | -$0.04 | a1       |
| user_a | 2024-01-02 | visit         | NULL   | b2       |
| user_a | 2024-01-03 | visit         | NULL   | c3       |
| user_a | 2024-01-04 | purchase      | +$40   | d4       |
| user_a | 2024-01-05 | ad_impression | -$0.05 | e5       |

Now we run `.match_steps("ad_impression", "purchase")`. For each user,
this will construct one row. Let's look at the `user_a` row:

<div class="wy-table-responsive">
<table border="1" class="docutils align-default">
  <thead>
    <tr>
      <th colspan="1">group</th>
      <th colspan="3">ad_impression</th>
      <th colspan="3">purchase</th>
    </tr>
    <tr>
      <th></th>
      <th>timestamp</th>
      <th>value</th>
      <th>event_id</th>
      <th>timestamp</th>
      <th>value</th>
      <th>event_id</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td><p>user_a</p></td>
      <td><p>2024-01-01</p></td>
      <td><p>-$0.04</p></td>
      <td><p>a1</p></td>
      <td><p>2024-01-04</p></td>
      <td><p>+$40</p></td>
      <td><p>d4</p></td>
    </tr>
  </tbody>
</table>
</div>

As you can see, we selected the first matching step for `ad_impression`
and joined that event in, and then selected the first matching `purchase` event
after that and joined that in. We can access each event's data through
`rel.ad_impression` and `rel.purchase` respectively.

For a user which doesn't match all the steps, that relation's data will contain
all `NULL`s, similar to the result of a `LEFT INNER JOIN` like in SQL.

This table is sometimes not useful on its own. However, you can perform lots of
neat aggregation on top. For example, we could compute the time between ad and
purchase (`rel.purchase.timestamp - rel.ad_impression.timestamp`), or look for
correlations between how much we paid for an ad `rel.ad_impression.value`, and
conversion rate `rel.purchase.timestamp != None`.
