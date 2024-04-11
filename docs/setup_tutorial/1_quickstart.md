# Quickstart

```{include} /_fragments/alpha_notice.md

```

## Installation

Install Hashquery with `pip` from [PyPI](https://pypi.org/project/hashquery/):

```bash
$ pip install hashquery
```

Hashquery requires Python version 3.6 or above.

## Using the demo project

The quickest way to give Hashquery a spin is to use our [demo project](https://demo.hashboard.com/app?p=csLQWKcT8s3mj-uJ). Add this to your package imports:

```python
from hashquery.demo import demo_project as project
```

Verify everything's working by starting a `python` shell and running one of the demo models:

```python
python
>>> from hashquery.demo import demo_project as project
>>> project.models.products.limit(5).run().df
                     id pizza_size pizza_shape  pizza_type  price
0  14278024243148112051      Large       Round      Custom  13.20
1   9154428932967574098     Medium       Round      Custom  11.00
2   2699960497371169210      Large      Square  Margherita  11.52
3  12910763551309720704     Medium       Round  Margherita   8.00
4   1363512534661457390     Medium      Square      Veggie  13.20
```

If all looks good, you're ready to move on to [your first analysis](./2_first_analysis.md).

## Connecting to your data

To get started using Hashquery with your own data, you'll be creating a <a href="https://.hashboard.com" target="_blank">Hashboard</a> project and configuring it with your database credentials or
uploading a CSV/parquet file.

If you already have a Hashboard project, see [Authenticating with Hashboard](./3_authentication.md) for more detail on
on how to connect Hashquery to your existing project.

1. Open the <a href="https://hashboard.com" target="_blank">Hashboard homepage</a>, click "Start for free", and follow the instructions to configure your project.
2. After logging in, go to the <a href="https://hashboard.com/app/p/data-sources" target="_blank">Data sources page</a> and click `+ Add connection`
3. Follow the prompts to configure your data source. (If you need help, take a look at the <a href="https://docs.hashboard.com/docs/database-connections" target="_blank">Hashboard documentation</a>).
4. Install the Hashboard CLI and generate an authentication token for your local environment:
```
$ pip install hashboard-cli
$ hb token
```
Alternatively, you can set an API token as an environment variable. See [Authenticating with Hashboard](./3_authentication.md) for more detail.

5. ```{include} /_fragments/quickstart_smoke_test.md

   ```

You're all set! You're ready to start running Hashquery models that read from your data connection, as demonstrated in [Your first analysis](./2_first_analysis.md).

You can also build a Model or Metric in Hashboard UI (<a href="https://docs.hashboard.com/docs/data-modeling/add-data-model" target="_blank">docs here</a>) and then <a href="../hashboard_intg/0_import.html">import</a> it into your Hashquery script.
