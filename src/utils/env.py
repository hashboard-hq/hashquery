from typing import *
from os import environ


def env_with_fallback(*names: List[str]) -> Optional[str]:
    for name in names:
        if env_val := environ.get(name):
            return env_val
    return None


def guess_execution_environment() -> bool:
    """
    Best effort detection of whether we're in an interactive notebook.

    Very few behaviors should gate on this; users expect code to act the same
    across these different envs. However, some envs have specific quirks,
    distinct error messages, or benefit from pre-caching certain methods so
    as to allow auto-completion to work better.
    """
    # Jupyter notebook, qtconsole, or Hex
    try:
        ipython_present_globally = get_ipython().__class__.__name__
        if ipython_present_globally:
            return "ipython"
    except NameError:
        pass

    # unknown
    return None
