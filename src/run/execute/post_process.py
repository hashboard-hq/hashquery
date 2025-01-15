from typing import *

import pandas as pd

from ...utils.identifier import is_double_underscore_name


def post_process_df(df: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
    # SQL and Pandas support duplicate column names (sometimes), but we
    # serialize this later to Arrow, which does not. So we rename duplicates here.
    warnings = _rename_duplicates(df)

    # remove any private columns from the output
    public_columns = [col for col in df.columns if not is_double_underscore_name(col)]
    df = df[public_columns]

    return df, warnings


def _rename_duplicates(df: pd.DataFrame):
    """
    Renames duplicate column names (in place) in `df`, using _<num> suffixes.
    """
    seen = set()

    dupes = set()
    dupe_indices = []

    for idx, col in enumerate(df.columns):
        if col in seen:
            dupe_indices.append(idx)
            dupes.add(col)
        else:
            seen.add(col)

    for idx in dupe_indices:
        col = df.columns.values[idx]
        new_col = col
        suffix = 2
        while new_col in seen:
            new_col = f"{col}_{suffix}"
            suffix += 1
        df.columns.values[idx] = new_col

    return [
        f"Results contain multiple columns with name '{d}'. Columns have been renamed to disambiguate."
        for d in dupes
    ]
