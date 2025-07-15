"""Utilities for generating tables in the Typst typesetting engine."""

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
    Convert a pandas DataFrame into a Typst table string.

    Ignores the index and creates a simple table.

    Args:
        df (pd.DataFrame): The DataFrame to convert.
        align (list[str], optional):
            List of alignment specifiers for columns (e.g., "left", "center", "right").
            Defaults to None (Typst default will be used).
        columns (list[str], optional):
            List of column width specifiers. If None, a default column count will be used.
        na_value (str): Replacement string for NA/NaN values in the DataFrame.
        hline (list[int]): List of row indices after which horizontal lines will be added
                           (0-based, including header if present).
        colnames (bool): Whether to include column names as the header row. Defaults to True.

    Returns:
        str: A string representation of the Typst table.
    """
    result = ["table(", "  stroke: none,"]

    # Add layout specifiers
    if columns is None:
        result.append(f"  columns: {len(df.columns)},")
    else:
        result.append(f"  columns: ({_df_attribute_to_typst(columns)}),")
    if align is not None:
        result.append(f"  align: ({_df_attribute_to_typst(align)}),")

    # Add table header
    if colnames:
        if 0 in hline:
            result.append("  table.hline(),")
        result.append("  " + _df_row_to_typst(df.columns, na_value=na_value, bold=(0 in bold)))

    # Add data rows
    for row_idx, (_, row) in enumerate(df.iterrows()):
        idx = row_idx + int(colnames)
        if idx in hline:
            result.append("  table.hline(),")
        result.append("  " + _df_row_to_typst(row, na_value=na_value, bold=(idx in bold)))
    if len(df) + int(colnames) in hline:
        result.append("  table.hline(),")

    result.append(")")
    return "\n".join(result)


def _df_attribute_to_typst(x: list) -> str:
    """Convert list of column attributes into Typst-compatible comma-separated format."""
    return ", ".join(map(str, x))


def _df_row_to_typst(row: list, na_value: str = "", bold: bool = False) -> str:
    """Convert a data frame row to a Typst-formatted table row."""
    cells = [na_value if pd.isna(cell) else cell for cell in list(row)]
    if bold:
        return " ".join(f'text(weight: "bold", [{cell}]),' for cell in cells)
    return " ".join(f"[{cell}]," for cell in cells)
