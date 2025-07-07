import datetime
from pathlib import Path
from typing import List
import pandas as pd
import typst
from pyledger import TextLedger
from accountbot import summarize_groups

def df_to_typst(df, align: List[str]=None, columns: List[str]=None, na_value="", hline=[]) -> str:
    """
    Convert a pandas DataFrame to Typst table format.
    Ignores the index and creates a simple table with headers.

    Args:
        na_value (str): String for NA/NaN values, default ""
        hline (list of int): Row indices after which to insert horizontal lines, default []
    """

    def _df_attribute_to_typst(x: List) -> str:
        return ", ".join([f'{item}' for item in x])

    def _df_row_to_typst(row: List, na_value: str="") -> str:
        cells = [na_value if pd.isna(cell) else cell for cell in list(row)]
        return " ".join([f'[{cell}],' for cell in cells])

    result = []
    result.append(f"table(")
    result.append(f"  stroke: none,")
    if columns is None:
        result.append(f"  columns: {len(df.columns)},")
    else:
        result.append(f"  columns: ({_df_attribute_to_typst(columns)}),")
    if align is not None:
        result.append(f"  align: ({_df_attribute_to_typst(align)}),")
    # Add header
    if 0 in hline:
        result.append("  table.hline(),")
    result.append("  " + _df_row_to_typst(df.columns, na_value=na_value))
    # Add body
    for row_idx, (_, row) in enumerate(df.iterrows()):
        if row_idx + 1 in hline:
            result.append("  table.hline(),")
        result.append("  " + _df_row_to_typst(row, na_value=na_value))
    if len(df) + 1 in hline:
        result.append("  table.hline(),")
    # Close the table
    result.append(")")
    return "\n".join(result)


self = TextLedger(Path("~/macx/rocket-accounting/accounts").expanduser())

def pdf_balance_sheet(
    output_path: str,
    prune_level: int,
    period: str | datetime.date | None = None,
    profit_centers: list[str] | str | None = None
) -> None:
    def format_amount(x: float) -> str:
        return f"{x:,.2f}".replace(",", "'")

    # Step 1: Load and aggregate account balances
    balance = self.individual_account_balances("1000:2999", period=period, profit_centers=profit_centers)
    df = self.aggregate_account_balances(balance, n=prune_level)
    df["group"] = df["group"].str.replace("^/", "", regex=True)

    # Step 2: Summarize into hierarchical report format
    report = summarize_groups(df, summarize={"report_balance": "sum"}, leading_space=1)
    h1_mask = report["level"] == "H1"
    report.loc[h1_mask, "description"] = report.loc[h1_mask, "description"].str.upper()

    # Step 3: Format balances and suppress small values
    tolerance = self.precision_vectorized(["reporting_currency"], [None])[0] / 2
    report["report_balance"] = report["report_balance"].map(
        lambda x: "" if pd.isna(x) or abs(x) < tolerance else format_amount(x)
    )

    # Step 4: Prepare final DataFrame for rendering
    report.rename(columns={"report_balance": "Balance", "description": "Description"}, inplace=True)
    report = report[["Description", "Balance"]]

    # Step 5: Format as Typst table
    table_code = df_to_typst(
        report,
        hline=h1_mask[h1_mask].index.to_list() + [len(report) + 1],
        columns=["auto", "1fr"],
        align=["left", "right"],
    )

    # Step 6: Compile Typst to PDF
    typst.compile(
        input=Path("~/macx/pyledger/test_template.typ").expanduser(),
        output=output_path,
        sys_inputs={"table": table_code, "period": str(period)}
    )

pdf_balance_sheet(prune_level=2, output_path="test.pdf")