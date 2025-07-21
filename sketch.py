from pathlib import Path
from io import StringIO
import pandas as pd
import typst
from pyledger import TextLedger, summarize_groups, df_to_typst


PDF_OUTPUT_BALANCE = "balance_sheet.pdf"
PDF_OUTPUT_PL = "profit_and_loss.pdf"
TYPST_OUTPUT_BALANCE = Path(PDF_OUTPUT_BALANCE).with_suffix(".typ")
TYPST_OUTPUT_PL = Path(PDF_OUTPUT_PL).with_suffix(".typ")

TEXT_BEFORE_BALANCE = """
#set text(font: "DejaVu Sans", size: 11pt, lang: "en")
== Balance Sheet
\\
"""

TEXT_BEFORE_PL = """
#set text(font: "DejaVu Sans", size: 11pt, lang: "en")
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

def format_amount(x: float) -> str:
    return f"{x:,.2f}".replace(",", "'")


def apply_threshold_and_format(series: pd.Series, threshold: float) -> pd.Series:
    return series.map(lambda x: "" if pd.isna(x) or abs(x) < threshold else format_amount(x))


def _report_column(
    engine,
    row,
    accounts=None,
    staggered=False,
    prune_level=2
) -> pd.DataFrame:
    balances = engine.individual_account_balances(
        "1000:9999",
        period=row["period"],
        profit_centers=row.get("profit_centers")
    )

    if accounts is not None:
        balances = balances.drop(columns=["group", "description", "currency"], errors="ignore")
        balances = balances.merge(
            accounts[["account", "group", "description", "currency"]],
            on="account",
            how="inner"
        )

    if not staggered:
        mask = balances["group"].str.match("/*Liabilities")
        balances.loc[mask, ["balance", "report_balance"]] *= -1
        balances = engine.aggregate_account_balances(balances, n=prune_level)

    balances["group"] = balances["group"].str.replace("^/", "", regex=True)

    return balances[["group", "description", "report_balance"]].rename(
        columns={"report_balance": row["label"]}
    )


def report_table(
    engine,
    config,
    accounts=None,
    staggered=False,
    prune_level=2
) -> str:
    # Step 1: Build merged sheet
    sheet = None
    for row in config.to_dict(orient="records"):
        df = _report_column(engine, row, accounts, staggered, prune_level)
        sheet = df if sheet is None else sheet.merge(df, how="outer", on=["group", "description"])

    label_cols = list(config["label"])

    # Step 2: Summarize groups
    report = summarize_groups(
        df=sheet,
        summarize={col: "sum" for col in label_cols},
        group="group",
        description="description",
        leading_space=1,
        staggered=staggered
    )

    # Step 3: Drop empty body rows and format headers
    report = report[~((report[label_cols] == "").all(axis=1) & (report["level"] == "body"))]
    report.loc[report["level"] == "H1", "description"] = report["description"].str.upper()

    # Step 4: Format amounts
    tolerance = engine.precision_vectorized([engine.reporting_currency], [None])[0] / 2
    for col in label_cols:
        report[col] = apply_threshold_and_format(report[col], threshold=tolerance)
        if not staggered:
            report.at[0, col] = col  # label row

    # Step 4b: Insert column label row for staggered reports
    if staggered:
        label_row = {
            "level": "label",
            "description": "",
            **{col: col for col in label_cols}
        }
        report = pd.concat([pd.DataFrame([label_row]), report], ignore_index=True)

    # Step 5: Column selection
    visible_cols = ["description"] + label_cols

    # Step 6: Bold and hline logic based on mode
    if staggered:
        bold_rows = report.index[report["level"].isin(["label", "subtotal"])].tolist()
        hline = (report.index[report["level"] == "subtotal"] + 1).tolist()
        hline.insert(0, 1)  # horizontal line after label row
    else:
        bold_rows = report.index[report["level"].str.match(r"^[HS][0-9]$")].tolist()
        hline = (report.index[report["level"] == "H1"] + 1).tolist()


    # Step 7: Typst output
    return df_to_typst(
        df=report[visible_cols],
        hline=hline,
        bold=bold_rows,
        columns=["auto"] + ["1fr"] * len(label_cols),
        align=["left"] + ["right"] * len(label_cols),
        colnames=False
    )

if __name__ == "__main__":
    config = pd.read_csv(StringIO(COLUMNS_CSV), skipinitialspace=True, dtype="string")
    engine = TextLedger(Path("~/macx/rocket-accounting/accounts").expanduser())
    all_accounts = engine.accounts.list()


    # Balance Sheet Report
    balance_accounts = all_accounts.loc[(all_accounts["account"] >= 1000) & (all_accounts["account"] < 3000)]
    balance_table = report_table(
        engine=engine,
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
        ["account", "description", "currency", "group"]
    ]
    def map_pl_group(account: int) -> str:
        if 3000 <= account <= 3999:
            return "Gross Profit"
        elif 4000 <= account <= 4999:
            return "Net Profit"
        elif 5000 <= account <= 6999:
            return "EBITDA"
        elif 7000 <= account <= 7999:
            return "EBIT"
        elif 8000 <= account <= 9999:
            return "Profit and Loss"
        return "Other"

    pl_accounts["group"] = pl_accounts["account"].map(map_pl_group)
    pl_table = report_table(
        engine=engine,
        config=config,
        accounts=pl_accounts,
        staggered=True
    )

    typst_doc_pl = f"""
        {TEXT_BEFORE_PL}
        #{pl_table}
        {TEXT_AFTER_TABLE}
    """
    with open(TYPST_OUTPUT_PL, "w", encoding="utf-8") as f:
        f.write(typst_doc_pl)
    typst.compile(input=TYPST_OUTPUT_PL, output=PDF_OUTPUT_PL)
