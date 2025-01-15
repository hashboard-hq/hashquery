# Authentication

To use the [Hashboard](https://www.hashboard.com) integration you will need access to a Hashboard project where:

- you have `OWNER` or `EDITOR` permissions
- it has at least one data connection

:::{tip}
Hashquery will automatically use the same credentials as the [Hashboard CLI](https://docs.hashboard.com/docs/data-ops).
:::

## Creating an access key

Instead of logging in with your username/password or SSO credentials,
Hashquery uses an access key to authenticate you with Hashboard.

This is a **secret** value that authorizes you within a single project.
Like a password, access keys should never be shared, and Hashboard employees
will never ask for your access key.

::::{tab-set}

:::{tab-item} Using the CLI credentials

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

## Specifying Authentication Explicitly

When loading a Hashboard project, you can specify authentication in code explicitly in the constructor for `HashboardProject()`:

```python
from hashquery.integration.hashboard import HashboardProject
from hashquery.integration.hashboard.credentials import HashboardAccessKeyClientCredentials

credentials = HashboardAccessKeyClientCredentials.from_encoded_key(api_key)
project = HashboardProject(credentials)
```
