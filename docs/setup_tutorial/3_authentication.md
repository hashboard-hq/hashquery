# Authenticating with Hashboard

Hashquery requires connecting to Hashboard to find and query your data
connections, as well as import and export your content.

You will need access to a [Hashboard](https://www.hashboard.com) project where:

- you have `OWNER` or `EDITOR` permissions
- it has at least one data connection

:::{tip}
Hashquery will automatically use the same credentials as the
[Hashboard CLI](https://docs.hashboard.com/docs/data-ops/quickstarts).
If you are already set up with the CLI and want to use that same project
for the Hashquery, you can [move on to the next step](2_first_analysis.md).
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

## Verifying

```{include} /_fragments/quickstart_smoke_test.md

   ```
