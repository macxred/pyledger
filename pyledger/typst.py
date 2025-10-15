"""Utilities for generating and formatting tables in the Typst typesetting engine."""

from pathlib import Path
import pandas as pd
import typst


def df_to_typst(
    df: pd.DataFrame,
    align: list[str] = None,
    columns: list[str] = None,
    na_value: str = "",
    hline: list[int] = [],
    bold: list[int] = [],
    inset: dict[int, dict] = None,
    colnames: bool = True,
    repeat_colnames: bool = True,
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
        inset (dict[int, dict], optional): Dictionary mapping row indices to inset specifications.
            Keys are 0-based row indices (including header if `colnames=True`).
            Values are dicts with keys like "top", "bottom", "left", "right" (e.g., {"top": "2pt"}).
        colnames (bool): Whether to include column names as the first row. Defaults to True.
        repeat_colnames (bool): Whether the header repeats on each page if the table spans multiple
            pages. Defaults to True, matching Typst's `table.header(repeat: true)` default.

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
        result.extend(_typst_header_row(
            df.columns, na_value=na_value, bold=(row_idx in bold), hline=(row_idx in hline),
            repeat=repeat_colnames,
        ))
        row_idx += 1
    # Data rows
    for _, row in df.iterrows():
        row_inset = inset.get(row_idx) if inset else None
        result.extend(_typst_row(
            row, na_value=na_value, bold=(row_idx in bold), hline=(row_idx in hline),
            inset=row_inset
        ))
        row_idx += 1
    result.append(")")
    return "\n".join(result) + "\n"


def _typst_attribute(x: list) -> str:
    """Convert list of column attributes into Typst-compatible comma-separated format."""
    return ", ".join(map(str, x))


def _typst_row(row: list, na_value: str, bold: bool, hline: bool, inset: dict = None) -> list[str]:
    """
    Convert a data frame row to a Typst-formatted table row.
    Optionally adding a horizontal line at the bottom and custom insets.
    """
    cells = [na_value if pd.isna(cell) else cell for cell in list(row)]

    if inset:
        inset_parts = [f"{k}: {v}" for k, v in inset.items()]
        inset_spec = ", ".join(inset_parts)

        # Wrap each cell in table.cell with custom inset
        if bold:
            cell_strs = [
                f'table.cell(inset: ({inset_spec}))[#text(weight: "bold")[{cell}]],'
                for cell in cells
            ]
        else:
            cell_strs = [f'table.cell(inset: ({inset_spec}))[{cell}],' for cell in cells]
        row = ["  " + " ".join(cell_strs)]
    else:
        if bold:
            row = ["  " + " ".join(f'text(weight: "bold", [{cell}]),' for cell in cells)]
        else:
            row = ["  " + " ".join(f"[{cell}]," for cell in cells)]

    if hline:
        row.append("  table.hline(),")
    return row


def _typst_header_row(row: list, repeat: bool, hline: bool, **kwargs) -> list[str]:
    """
    Wrap a header row inside a Typst table.header(...) block.
    Reuses _typst_row for cell rendering. Optionally adds a horizontal line after the header.
    """
    inner = "  " + _typst_row(row, hline=False, **kwargs)[0]
    row = [f"  table.header(repeat: {'true' if repeat else 'false'},", inner, "  ),"]
    if hline:
        row.append("  table.hline(),")
    return row


def escape_typst_text(series: pd.Series) -> pd.Series:
    """Escape Typst-sensitive characters in content blocks.

    Escapes special characters that have meaning in Typst markup:
    - backslash (escape character itself - must be escaped first)
    - $ (math mode delimiter)
    - # (code mode prefix)
    - * (strong emphasis)
    - @ (reference)
    - < and > (label delimiters)

    Args:
        series: A pandas Series containing text values to escape.

    Returns:
        A pandas Series with Typst special characters escaped.
    """
    return series.map(
        lambda text: (
            text.replace("\\", "\\\\")  # Must be first to avoid double-escaping
                .replace("$", "\\$")
                .replace("#", "\\#")
                .replace("*", "\\*")
                .replace("@", "\\@")
                .replace("<", "\\<")
                .replace(">", "\\>")
        ) if isinstance(text, str) else text
    )


def render_typst(content: str, output_path: str | Path, keep_temp: bool = False):
    """Generate a PDF from a Typst-formatted string, using a temporary .typ file.

    Converts a Typst-formatted string into a PDF by writing it to a temporary `.typ` file,
    running the Typst compiler, and saving the result to the specified output path.
    Optionally retains the intermediate `.typ` file for inspection or debugging.

    Args:
        content (str): The Typst markup to render.
        output_path (str | Path): The destination path for the generated PDF file.
        keep_temp (bool, optional): If True, the intermediate `.typ` file is preserved.
    """
    output_path = Path(output_path)
    typst_path = output_path.with_suffix(".typ")
    typst_path.write_text(content.strip(), encoding="utf-8")
    try:
        typst.compile(input=str(typst_path), output=str(output_path))
    finally:
        if not keep_temp:
            typst_path.unlink(missing_ok=True)
