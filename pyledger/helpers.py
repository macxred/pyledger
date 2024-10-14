"""This module provides utilities for handling DataFrame operations such as
writing fixed-width CSV files and checking if values can be represented as integers.
"""

from typing import Any
from pathlib import Path
import numpy as np
import pandas as pd


def represents_integer(x: Any) -> bool:
    """Check if the input is an integer number and can be cast as an integer.

    Args:
        x (Any): The value to be checked.

    Returns:
        bool: True if x is an integer number, False otherwise.

    Examples:
        >>> represents_integer(4)
        True
        >>> represents_integer(4.0)  # Float with an integer value
        True
        >>> represents_integer("4")
        True
        >>> represents_integer("4.5")
        False
        >>> represents_integer(None)
        False
        >>> represents_integer("abc")
        False
        >>> represents_integer([4])
        False
    """
    if isinstance(x, int):
        return True
    elif isinstance(x, float):
        return x.is_integer()
    else:
        try:
            return int(x) == float(x)
        except (ValueError, TypeError):
            return False


def write_fixed_width_csv(
    df: pd.DataFrame,
    path: str = None,
    sep: str = ", ",
    na_rep: str = "",
    n: int = None,
    *args,
    **kwargs
) -> str:
    """Generate a human-readable CSV.

    Writes a pandas DataFrame to a CSV file, ensuring that the first n columns
    have a fixed width determined by the longest entry in each column. Text is
    right-aligned, and NA values are represented as specified. If n is None,
    all columns except the last will have fixed width.

    Args:
        df (pandas.DataFrame): DataFrame to be written to CSV.
        path (str): Name/path of the CSV file to write. If None, returns the CSV
            output as a string.
        sep (str): Separator for the CSV file, default is ', '. In contrast to
            pd.to_csv, multi-char separators are supported.
        na_rep (str): String representation for NA/NaN data. Default is ''.
        n (int): Number of columns from the start to have fixed width. If None,
            applies to all columns except the last.
        *args: Additional arguments for pandas to_csv method.
        **kwargs: Additional keyword arguments for pandas to_csv method.

    Returns:
        str: CSV output as a string if the path is None.
    """
    result = {}
    fixed_width_cols = df.shape[1] - 1 if n is None else n

    for i, colname in enumerate(df.columns):
        col = df[colname]
        col_str = pd.Series(np.where(col.isna(), na_rep, col.astype(str)))
        max_length = max(col_str.str.len().max(), len(colname))
        if i < fixed_width_cols:
            col_str = col_str.str.rjust(max_length)
            colname = colname.rjust(max_length)

        # Separator for all but the first column
        if i > 0:
            col_str = sep[1:] + col_str
            colname = sep[1:] + colname

        result[colname] = col_str

    result = pd.DataFrame(result)

    # Write to CSV
    return result.to_csv(path, sep=sep[0], index=False, na_rep=na_rep, *args, **kwargs)


def save_files(df: pd.DataFrame, root: Path | str, func=write_fixed_width_csv) -> None:
    """
    Save a DataFrame to multiple CSV files specified in the '__csv_path__' column.

    Ensures that CSV files in a `root` folder contain the data in `df`.
    1. Saves df to multiple CSV files specified by df["__csv_path__"].
    2. Removes unneeded CSV files.

    Args:
        df (pd.DataFrame): The DataFrame containing the data to be saved
                            and a '__csv_path__' column.
        root (Path | str): The root folder in which CSV files are stored.
        func: Method with signature (df, file) to write the data frame df.
    """
    if "__csv_path__" not in df.columns:
        raise ValueError("The DataFrame must contain a '__csv_path__' column.")

    root = Path(root).expanduser()
    root.mkdir(parents=True, exist_ok=True)

    # Delete unneeded files
    current_files = set(root.rglob("*.csv"))
    referenced_files = set(root / path for path in df["__csv_path__"].unique())
    for file in current_files - referenced_files:
        file.unlink()

    # Save DataFrame entries to their respective file paths
    for path, group in df.groupby("__csv_path__"):
        if path:
            full_path = root / path
            full_path.parent.mkdir(parents=True, exist_ok=True)
            func(group.drop(columns="__csv_path__"), full_path)
