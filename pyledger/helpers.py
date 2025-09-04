"""This module provides utilities for handling DataFrame operations such as
writing fixed-width CSV files and checking if values can be represented as integers.
"""

from typing import Any, List
from pathlib import Path, PurePosixPath
import numpy as np
import pandas as pd

from pyledger.constants import DEFAULT_FILE_COLUMN


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


def save_files(
    df: pd.DataFrame,
    root: Path | str,
    file_column: str = DEFAULT_FILE_COLUMN,
    func=write_fixed_width_csv,
    keep_unreferenced: bool = False,
):
    """Save DataFrame entries to multiple files within a root folder.

    Saves a DataFrame to multiple files in the specified `root` folder, with
    file paths within the root folder determined by a given `file_column`.
    By default, files in `root` that are not referenced in `file_column`
    are deleted unless `keep_unreferenced` is True.

    Args:
        df (pd.DataFrame): DataFrame to save, with a `file_column`.
        root (Path | str): Root directory where the files will be stored.
        file_column (str): Name of the column containing relative file paths.
        func (callable): Function to save each DataFrame group to a file.
        keep_unreferenced (bool): Keep files in `root` not referenced in `file_column`.
            Defaults to False (delete them).

    Raises:
        ValueError: If the DataFrame does not contain a `file_column`.
    """
    if file_column not in df.columns:
        raise ValueError(f"The DataFrame must contain a '{file_column}' column.")

    root = Path(root).expanduser()
    root.mkdir(parents=True, exist_ok=True)

    # Delete unreferenced files (unless keeping them)
    if not keep_unreferenced:
        current_files = set(root.rglob("*.csv"))
        referenced_files = set(root / path for path in df[file_column].unique())
        for file in current_files - referenced_files:
            file.unlink()

    # Save DataFrame entries to their respective files
    for path, group in df.groupby(file_column):
        full_path = root / path
        full_path.parent.mkdir(parents=True, exist_ok=True)
        func(group.drop(columns=file_column), full_path)


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
    result = [str(i) for i in list(x)[:n]]
    if len(x) > n:
        result.append("...")
    return ", ".join(result)


def prune_path(path: str, description: str, n: int = 0) -> tuple[str, str]:
    """
    Prune a POSIX-style path to a specified depth and update the description.

    The path is shortened to include only the first `n` levels. If a segment exists
    at level `n + 1`, it replaces the given description. If the path is too short
    to extract that segment, the original description is retained. If `n` is zero,
    the shortened path is returned as `pd.NA`.

    Parameters:
        path (str): POSIX-style path. A leading slash is added if missing.
        description (str): Fallback description if the path is too short.
        n (int): Number of leading segments to preserve in the path.

    Returns:
        tuple[str, str]: A tuple of (path, description).
    """

    if pd.isna(path):
        return (path, description)

    if not path.startswith("/"):
        path = "/" + path

    path = PurePosixPath(path)
    if len(path.parts) <= n + 1:
        return (str(path), description)
    else:
        return (str(path.parents[-(n + 1)]) if n > 0 else pd.NA, path.parts[n + 1])
