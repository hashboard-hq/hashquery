import warnings
from contextlib import contextmanager
from copy import deepcopy

from sqlalchemy.exc import SAWarning


@contextmanager
def silenced_internal_warnings():
    """
    Silences internal warnings which are not actionable for the user.
    This is not thread-safe, but it will restore any original warnings filters
    when the context manager is exited.
    """
    try:
        og_filters = deepcopy(warnings.filters)
        warnings.filterwarnings(
            "ignore",
            # this issue is pretty niche, and tends to bark even when no decimal
            # values are being manipulated
            r".*does \*not\* support Decimal objects natively, and SQLAlchemy",
            SAWarning,
        )
        yield
    finally:
        warnings.filters[:] = og_filters
