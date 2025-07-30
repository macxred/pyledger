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
    """
    result = ["table(", "  stroke: none,"]

    # Add layout specifiers
    if columns is None:
        result.append(f"  columns: {len(df.columns)},")
    else:
        result.append(f"  columns: ({_df_attribute_to_typst(columns)}),")
    if align is not None:
        result.append(f"  align: ({_df_attribute_to_typst(align)}),")

    current_row_idx = 0
    # Header
    if colnames:
        result.append(
            "  " + _df_row_to_typst(df.columns, na_value=na_value, bold=(current_row_idx in bold))
        )
        current_row_idx += 1
        if (current_row_idx - 1) in hline:
            result.append("  table.hline(),")
    # Data rows
    for _, row in df.iterrows():
        result.append(
            "  " + _df_row_to_typst(row, na_value=na_value, bold=(current_row_idx in bold))
        )
        if current_row_idx in hline:
            result.append("  table.hline(),")
        current_row_idx += 1
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


def format_number(x: float) -> str:
    """Format a float with apostrophe separators and two decimal places."""
    return f"{x:,.2f}".replace(",", "'")


def format_threshold(series: pd.Series, threshold: float) -> pd.Series:
    """Format values using `format_number`, or return empty string if below threshold or NaN."""
    return series.map(
        lambda x: "" if pd.isna(x) or abs(x) < threshold else format_number(x)
    )
