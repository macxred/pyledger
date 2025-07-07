import datetime
from pathlib import Path
import re
import pandas as pd
import typst
from pyledger import TextLedger, df_to_typst, summarize_groups


PDF_OUTPUT = "test.pdf"
TYPST_OUTPUT = re.sub("\\.pdf$", ".typ", PDF_OUTPUT)
TEXT_BEFORE_TABLE = """
   #set text(font: "DejaVu Sans", size: 11pt, lang: "en")
   == Balance Sheet
   \\
"""
TEXT_AFTER_TABLE = """
    \\
    That's all, folks.\\
    Cheers!
"""

def balance_sheet_table(
    engine,
    prune_level: int,
    period: str | datetime.date | None = None,
    profit_centers: list[str] | str | None = None
) -> str:
    def format_amount(x: float) -> str:
        return pd.NA if pd.isna(x) else f"{x:,.2f}".replace(",", "'")

    # Load and aggregate account balances
    balance = engine.individual_account_balances("1000:2999", period=period, profit_centers=profit_centers)
    df = engine.aggregate_account_balances(balance, n=prune_level)
    df["group"] = df["group"].str.replace("^/", "", regex=True)

    # Summarize into hierarchical report format
    report = summarize_groups(df, summarize={"report_balance": "sum"}, leading_space=1)
    report["report_balance"] = engine.round_to_precision(report["report_balance"], engine.reporting_currency)
    h1_mask = report["level"] == "H1"
    report.loc[h1_mask, "description"] = report.loc[h1_mask, "description"].str.upper()

    # Format table
    visible_cols = ["description", "report_balance"]
    report["report_balance"] = report["report_balance"].map(format_amount)
    bold_mask = report["level"].str.match("^[HS][0-9]$")
    report.loc[bold_mask, visible_cols] = "*" + report.loc[bold_mask, visible_cols] + "*"

    # Step 5: Format as Typst table
    return df_to_typst(
        report[visible_cols],
        hline=(h1_mask[h1_mask].index + 1).to_list(),
        columns=["auto", "1fr"],
        align=["left", "right"],
        colnames=False
    )

if __name__ == "__main__":
    engine = TextLedger(Path("~/macx/rocket-accounting/").expanduser())
    tbl = balance_sheet_table(engine=engine, prune_level=2)

    typst_document = f"""
        {TEXT_BEFORE_TABLE}
        #{tbl}
        {TEXT_AFTER_TABLE}
    """
    with open(TYPST_OUTPUT, "w", encoding="utf-8") as f:
        f.write(typst_document)
    typst.compile(input=TYPST_OUTPUT, output=PDF_OUTPUT)
