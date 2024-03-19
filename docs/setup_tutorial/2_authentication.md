# Authenticating with Hashboard

:::{admonition} Using the demo project
:class: note

If you want to just give Hashquery a spin, we encourage you to use the demo
project. Just use `from hashquery.demo import demo_project` instead of `project`
and you'll be all set. You can also view
[the contents of the demo project directly in Hashboard](https://demo.hashboard.com?p=csLQWKcT8s3mj-uJ).

If you choose to use this option, you can skip the rest of this step.
:::

Hashquery requires connecting to Hashboard to find and query your data
connections, as well as import and export your content.

You will need access to a [Hashboard](https://www.hashboard.com) project where:

- you have `OWNER` permissions
- it has at least one data connection
- the `HASHQUERY` feature flag is enabled _(please speak with us if you need this enabled)_

:::{tip}
Hashquery will automatically use the same credentials as the
[Hashboard CLI](https://docs.hashboard.com/docs/data-ops/quickstarts).
If you are already set up with the CLI and want to use that same project
for the Hashquery, you can [move on to the next step](3_first_analysis.md).
:::

## Creating an access key

Instead of logging in with your username/password or SSO credentials,
Hashquery uses an access key to authenticate you with Hashboard.

This is a **secret** value that authorizes you within a single project.
Like a password, access keys should never be shared, and Hashboard employees
will never ask for your access key.

::::{tab-set}

:::{tab-item} Using the CLI credentials [recommended]

If you have the CLI installed (`pip install hashboard-cli`) then you can run:

```bash
hb token
```

This will take you through a web-based authentication flow and
download a new access key for you. The access key will be stored at the default
filepath: `~/.hashboard/hb_access_key.json`.

:::

:::{tab-item} Using an API Token

Navigate to the project you want to create an API token for, and go to
_Settings_. Under _Access Keys_, create a new access key, naming it something
descriptive (like "Hashquery") then choose _API Token_. Save this value
somewhere secure.

Before running a Hashquery program, specify this token as an environment
variable:

```bash
export HASHBOARD_API_TOKEN=yourapitoken
```

:::

::::

## Verifying

Let's just double check that everything is humming smoothly.
Copy paste the following into a new Python file, `smoketest.py`:

```python
from hashquery import *
print(project)
```

Run the file. If all went smoothly, you should see an index of your project's
contents.

```bash
python smoketest.py
```
