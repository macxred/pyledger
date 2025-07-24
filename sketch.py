from pathlib import Path
from io import StringIO
import pandas as pd
import typst
from pyledger import TextLedger


PDF_OUTPUT_BALANCE = "balance_sheet.pdf"
PDF_OUTPUT_PL = "profit_and_loss.pdf"
TYPST_OUTPUT_BALANCE = Path(PDF_OUTPUT_BALANCE).with_suffix(".typ")
TYPST_OUTPUT_PL = Path(PDF_OUTPUT_PL).with_suffix(".typ")

TEXT_BEFORE_BALANCE = """
#set text(font: "Sans Serif", size: 11pt, lang: "en")
== Balance Sheet
\\
"""

TEXT_BEFORE_PL = """
#set text(font: "Sans Serif", size: 11pt, lang: "en")
== Profit & Loss Statement
\\
"""

TEXT_AFTER_TABLE = """
\\
That's all, folks.\\
Cheers!
"""

COLUMNS_CSV = """
label,period,profit_centers
2024,2024,
2025,2025,
"""


if __name__ == "__main__":
    config = pd.read_csv(StringIO(COLUMNS_CSV), skipinitialspace=True, dtype="string")
    engine = TextLedger(Path("~/macx/rocket-accounting/accounts").expanduser())
    all_accounts = engine.accounts.list()
    all_accounts["group"] = all_accounts["group"].str.replace("^/", "", regex=True)
    # Assign account multipliers dynamically
    def compute_multiplier(row) -> int:
        g = row["group"]
        if g.startswith("Liabilities") or g.startswith("Income"):
            return -1
        if g.startswith("Equity"):
            return -1  # optional, depending on your reporting convention
        return 1

    all_accounts["account_multiplier"] = all_accounts.apply(compute_multiplier, axis=1)

    # Balance Sheet Report
    balance_accounts = all_accounts.loc[(all_accounts["account"] >= 1000) & (all_accounts["account"] < 3000)]
    balance_table = engine.report_table(
        config=config,
        accounts=balance_accounts,
        staggered=False
    )
    typst_doc_balance = f"""
        {TEXT_BEFORE_BALANCE}
        #{balance_table}
        {TEXT_AFTER_TABLE}
    """
    with open(TYPST_OUTPUT_BALANCE, "w", encoding="utf-8") as f:
        f.write(typst_doc_balance)
    typst.compile(input=TYPST_OUTPUT_BALANCE, output=PDF_OUTPUT_BALANCE)


    # P&L Report
    pl_accounts = all_accounts.loc[
        (all_accounts["account"] >= 3000) & (all_accounts["account"] < 10000),
        ["account", "description", "currency", "group",  "account_multiplier"]
    ]
    def map_pl_group(account: int) -> str:
        """Map account number to top-level P&L group label (in UPPERCASE)."""
        if 3000 <= account <= 3999:
            return "GROSS PROFIT"
        elif 4000 <= account <= 4999:
            return "NET PROFIT"
        elif 5000 <= account <= 6999:
            return "EBITDA"
        elif 7000 <= account <= 7999:
            return "EBIT"
        elif 8000 <= account <= 9999:
            return "PROFIT AND LOSS"
        return "OTHER"

    pl_accounts["group"] = pl_accounts.apply(
        lambda row: (
            map_pl_group(row["account"])
            if "/" not in row["group"] else
            f"{map_pl_group(row['account'])}/{'/'.join(row['group'].split('/')[1:])}"
        ),
        axis=1
    )
    pl_table = engine.report_table(
        config=config,
        accounts=pl_accounts,
        staggered=True,
        prune_level=1
    )

    typst_doc_pl = f"""
        {TEXT_BEFORE_PL}
        #{pl_table}
        {TEXT_AFTER_TABLE}
    """
    with open(TYPST_OUTPUT_PL, "w", encoding="utf-8") as f:
        f.write(typst_doc_pl)
    typst.compile(input=TYPST_OUTPUT_PL, output=PDF_OUTPUT_PL)
