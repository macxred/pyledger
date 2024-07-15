"""This module provides methods for writing DataFrames to Excel sheets with
automatic column width adjustment based on the content of each column.
"""

from pathlib import Path
from typing import Dict
import pandas as pd


def _set_auto_col_width(sheet, df: pd.DataFrame) -> None:
    """Set the column width of an Excel sheet automatically based on the content.

    Args:
        sheet (xlsxwriter.Worksheet): Worksheet in which to set column widths.
        df (pd.DataFrame): DataFrame with data used in the worksheet.
    """
    for idx, col in enumerate(df):
        column_width = max(
            # length of largest item
            df[col].astype(str).str.len().max(),
            # length of column name/header
            len(str(col)))
        # Add extra space
        column_width += 1
        sheet.set_column(idx, idx, column_width)


def write_sheet(df: pd.DataFrame, path: str, sheet: str = "Sheet1") -> None:
    # TODO: Drop in favor of write_sheets({sheet: df}, path=path)
    """Write a DataFrame to an Excel sheet with automatic column widths.

    Args:
        df (pd.DataFrame): DataFrame to write to Excel.
        path (str): File path for the Excel file.
        sheet (str, optional): Sheet name for the DataFrame. Defaults to 'Sheet1'.
    """
    file = Path(path).expanduser()
    file.parent.mkdir(parents=True, exist_ok=True)
    writer = pd.ExcelWriter(file, engine="xlsxwriter")
    df.to_excel(writer, index=False, sheet_name=sheet, freeze_panes=(1, 0))
    _set_auto_col_width(writer.sheets[sheet], df=df)
    writer.close()


def write_sheets(data: Dict[str, pd.DataFrame], path: str) -> None:
    # TODO: Rename as write_excel or write_workbook
    """Write multiple DataFrames to an Excel file, each in a separate sheet.

    Args:
        data (Dict[str, pd.DataFrame]): Dictionary of DataFrames with sheet
            names as keys.
        path (str): File path for the Excel file.
    """
    file = Path(path).expanduser()
    file.parent.mkdir(parents=True, exist_ok=True)
    writer = pd.ExcelWriter(file, engine="xlsxwriter")
    for sheet, df in data.items():
        df.to_excel(writer, index=False, sheet_name=sheet)
        _set_auto_col_width(writer.sheets[sheet], df=df)
        writer.sheets[sheet].freeze_panes(row=1, col=0)
    writer.close()
