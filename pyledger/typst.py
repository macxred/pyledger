"""Utilities for generating and formatting tables in the Typst typesetting engine."""

import pandas as pd


def df_to_typst(
    df: pd.DataFrame,
    align: list[str] = None,
    columns: list[str] = None,
    na_value: str = "",
    hline: list[int] = [],
    bold: list[int] = [],
    colnames: bool = True,
) -> str:
    """
    Convert a pandas DataFrame to a Typst-formatted table string.

    Produces a `table(...)` block for Typst with limited styling options:
    column alignment, column width, horizontal lines, and bold rows.

    Intended as a temporary utility until `.to_typst()` becomes available in pandas.

    Args:
        df (pd.DataFrame): The DataFrame to convert.
        align (list[str], optional): Alignment specifiers for columns ("left", "center", "right").
            Defaults to Typst's automatic alignment.
        columns (list[str], optional): Column width specifiers (e.g., "auto", "1fr", "2cm").
            If None, the number of columns is inferred from the DataFrame.
        na_value (str): Replacement string for NA/NaN values. Defaults to an empty string.
        hline (list[int], optional): Row indices after which horizontal lines are inserted.
            Indices are 0-based and include the header if `colnames=True`.
            Use -1 for a line above the header.
        bold (list[int], optional): Row indices to render in bold.
            Indices are 0-based and include the header if `colnames=True`.
        colnames (bool): Whether to include column names as the first row. Defaults to True.

    Returns:
        str: A Typst-compatible table string.

    Raises:
        ValueError: If `align` or `columns` is provided and its length does not match
        the number of DataFrame columns.
    """
    num_columns = len(df.columns)
    if align is not None and len(align) != num_columns:
        raise ValueError(f"`align` has {len(align)} elements but expected {num_columns}.")

    if columns is not None and len(columns) != num_columns:
        raise ValueError(f"`columns` has {len(columns)} elements but expected {num_columns}.")

    result = ["table(", "  stroke: none,"]

    # Add layout specifiers
    if columns is None:
        result.append(f"  columns: {len(df.columns)},")
    else:
        result.append(f"  columns: ({_typst_attribute(columns)}),")
    if align is not None:
        result.append(f"  align: ({_typst_attribute(align)}),")

    row_idx = 0
    if -1 in hline:
        result.append("  table.hline(),")
    # Header
    if colnames:
        result.extend(_typst_row(
            df.columns, na_value=na_value, bold=(row_idx in bold), hline=(row_idx in hline)
        ))
        row_idx += 1
    # Data rows
    for _, row in df.iterrows():
        result.extend(_typst_row(
            row, na_value=na_value, bold=(row_idx in bold), hline=(row_idx in hline)
        ))
        row_idx += 1
    result.append(")")
    return "\n".join(result)


def _typst_attribute(x: list) -> str:
    """Convert list of column attributes into Typst-compatible comma-separated format."""
    return ", ".join(map(str, x))


def _typst_row(row: list, na_value: str, bold: bool, hline: bool) -> list[str]:
    """
    Convert a data frame row to a Typst-formatted table row.
    Optionally adding a horizontal line at the bottom.
    """
    cells = [na_value if pd.isna(cell) else cell for cell in list(row)]
    if bold:
        row = ["  " + " ".join(f'text(weight: "bold", [{cell}]),' for cell in cells)]
    else:
        row = ["  " + " ".join(f"[{cell}]," for cell in cells)]
    if hline:
        row.append("  table.hline(),")
    return row


def escape_typst_text(series: pd.Series) -> pd.Series:
    """Escape Typst-sensitive characters: <, >, and @."""
    return series.map(
        lambda text: (
            text.replace("<", "\\<")
                .replace(">", "\\>")
                .replace("@", "\\@")
        ) if isinstance(text, str) else text
    )
