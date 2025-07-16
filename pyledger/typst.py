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
    Converts a DataFrame into a Typst table string with customizable layout and styling.

    This method is intended for generating Typst-compatible table strings with control over
    column alignment, width, missing value substitution, bold row styling, and horizontal dividers.
    Column names can be included as a header row, and the DataFrame index is always excluded.

    Args:
        df (pd.DataFrame): The DataFrame to convert.
        align (list[str], optional): Alignment specifiers for columns ("left", "center", "right").
            Defaults to Typst's automatic alignment.
        columns (list[str], optional): Column width specifiers (e.g., "auto", "1fr", "2cm").
            If None, the number of columns is inferred from the DataFrame.
        na_value (str): Replacement string for NA/NaN values. Defaults to an empty string.
        hline (list[int], optional): Row indices after which horizontal lines are inserted.
            Indices are 0-based and include the header if `colnames=True`.
        bold (list[int], optional): Row indices to render in bold. Header is index 0
            if `colnames=True`, followed by data rows.
        colnames (bool): Whether to include column names as the first row. Defaults to True.

    Returns:
        str: A Typst `table(...)` string representing the formatted DataFrame.
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
    if colnames and len(df.columns) > 0:
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
