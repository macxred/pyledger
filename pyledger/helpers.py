"""This module provides utilities for handling DataFrame operations such as
writing fixed-width CSV files and checking if values can be represented as integers.
"""

from typing import Any, List
from pathlib import Path, PurePosixPath
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
    file: str = None,
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
        file (str): Path of the CSV file to write. If None, returns the CSV
            output as a string.
        sep (str): Separator for the CSV file, default is ', '. In contrast to
            pd.to_csv, multi-char separators are supported.
        na_rep (str): String representation for NA/NaN data. Default is ''.
        n (int): Number of columns from the start to have fixed width. If None,
            applies to all columns except the last.
        *args: Additional arguments for pandas to_csv method.
        **kwargs: Additional keyword arguments for pandas to_csv method.

    Returns:
        str: CSV output as a string if `file` is None.
    """
    result = {}
    fixed_width_cols = df.shape[1] - 1 if n is None else n

    for i, colname in enumerate(df.columns):
        col = df[colname]
        col_str = pd.Series(np.where(col.isna(), na_rep, col.astype(str)))
        max_col_str = col_str.str.len().max()
        if pd.isna(max_col_str):
            max_col_str = 0
        max_length = max(max_col_str, len(colname))
        if i < fixed_width_cols:
            col_str = col_str.str.rjust(max_length)
            colname = colname.rjust(max_length)

        # Separator for all but the first column
        if i > 0:
            col_str = sep[1:] + col_str
            colname = sep[1:] + colname

        # Remove trailing spaces
        if i == len(df.columns) - 1:
            col_str = col_str.str.rstrip()

        result[colname] = col_str

    result = pd.DataFrame(result)

    # Write to CSV
    return result.to_csv(file, sep=sep[0], index=False, na_rep=na_rep, *args, **kwargs)


def save_files(df: pd.DataFrame, root: Path | str, func=write_fixed_width_csv):
    """Save DataFrame entries to multiple files within a root folder.

    Saves a DataFrame to multiple files in the specified `root` folder, with
    file paths within the root folder determined by the `__path__` column.
    Any existing files in the root directory that are not referenced in the
    `__path__` column are deleted.

    Args:
        df (pd.DataFrame): DataFrame to save, with a `__path__` column.
        root (Path | str): Root directory where the files will be stored.
        func (callable): Function to save each DataFrame group to a file.
                         Defaults to `write_fixed_width_csv`.

    Raises:
        ValueError: If the DataFrame does not contain a '__path__' column.
    """
    if "__path__" not in df.columns:
        raise ValueError("The DataFrame must contain a '__path__' column.")

    root = Path(root).expanduser()
    root.mkdir(parents=True, exist_ok=True)

    # Delete unreferenced files
    current_files = set(root.rglob("*.csv"))
    referenced_files = set(root / path for path in df["__path__"].unique())
    for file in current_files - referenced_files:
        file.unlink()

    # Save DataFrame entries to their respective files
    for path, group in df.groupby("__path__"):
        full_path = root / path
        full_path.parent.mkdir(parents=True, exist_ok=True)
        func(group.drop(columns="__path__"), full_path)


def first_elements_as_str(x: List[Any], n: int = 5) -> str:
    """
    Return a concise, comma-separated string of the first `n` elements of the list `x`.

    If the list has more than `n` elements, append "..." at the end.
    This is useful for logging or error messages when the full list
    would be too long to display.

    Args:
        x (List[Any]): The list to preview.
        n (int): The number of elements to include in the preview.

    Returns:
        str: A comma-separated preview of the first `n` elements, possibly ending in "...".
    """
    if not x:
        return ""
    result = [str(i) for i in x[:n]]
    if len(x) > n:
        result.append("...")
    return ", ".join(result)


def prune_path(
    group: str | List[str],
    description: str | List[str],
    n: int = 0
) -> List[tuple[str, str]]:
    """
    Prune paths by trimming them to a specified depth. Uses the provided description or extracts a
    path segment at the specified depth to use as the new description.

    Parameters:
        group (str | List[str]): POSIX-style path(s).
        description (str | List[str]): Description(s) associated with each path.
        n (int): Depth to trim the path.

    Returns:
        List[tuple[str, str]]: A list of (shortened path, updated description) pairs.

    Raises:
        TypeError: If `group` and `description` are not both strings or both lists.
        ValueError: If `group` and `description` are lists of different lengths.
    """

    def _prune_single(g: str, d: str) -> tuple[str, str]:
        if pd.isna(g):
            return (g, d)
        path = PurePosixPath(g)
        if g.startswith("/"):
            if len(path.parts) <= n + 1:
                return (g, d)
            parent = path.parents[-(n + 1)] if n > 0 else pd.NA
            return (str(parent), path.parts[n + 1])
        else:
            if len(path.parts) <= n:
                return (g, d)
            parent = path.parents[-(n + 1)] if n > 0 else pd.NA
            return (str(parent), path.parts[n])

    if isinstance(group, str) and isinstance(description, str):
        group_list = [group]
        description_list = [description]
    elif isinstance(group, list) and isinstance(description, list):
        if len(group) != len(description):
            raise ValueError("`group` and `description` must be lists of the same length.")
        group_list = group
        description_list = description
    else:
        raise TypeError("`group` and `description` must both be str or both be lists.")

    return [_prune_single(g, d) for g, d in zip(group_list, description_list)]
