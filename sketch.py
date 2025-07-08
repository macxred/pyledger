from pathlib import Path
from io import StringIO
import pandas as pd
import typst
from pyledger import TextLedger, df_to_typst, summarize_groups


PDF_OUTPUT = "test.pdf"
TYPST_OUTPUT = Path(PDF_OUTPUT).with_suffix(".typ")

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

COLUMNS_CSV = """
    label,         date,        profit_centers
    General,       2025-12-31,  General
    Shop,          2025-12-31,  Shop
    2024-12-31,    2024-12-31,
    2025-12-31,    2025-12-31,
"""

def balance_sheet_column(
    engine: TextLedger, label: str, period: str, profit_centers: str | None
) -> pd.DataFrame:
    balance = engine.individual_account_balances(
        "1000:2999", period=period, profit_centers=profit_centers
    )

    # Flip liabilities sign
    mask = balance["group"].str.match("/*Liabilities")
    balance.loc[mask, ["balance", "report_balance"]] *= -1

    df = engine.aggregate_account_balances(balance, n=2)
    df["group"] = df["group"].str.replace("^/", "", regex=True)

    return df[["group", "description", "report_balance"]].rename(columns={"report_balance": label})

def balance_sheet_table(config: pd.DataFrame, engine: TextLedger) -> str:
    def format_amount(x: float) -> str:
        return f"{x:,.2f}".replace(",", "'")

    def apply_threshold_and_format(series: pd.Series, threshold: float) -> pd.Series:
        return series.map(lambda x: "" if pd.isna(x) or abs(x) < threshold else format_amount(x))

    # Step 1: Generate merged sheet with all columns
    sheet = None
    for row in config.to_dict(orient="records"):
        df = balance_sheet_column(engine, row["label"], row["date"], row.get("profit_centers"))
        sheet = df if sheet is None else sheet.merge(df, how="inner", validate="1:1")

    label_cols = list(config["label"])

    # Step 2: Summarize into hierarchical report
    report = summarize_groups(sheet, summarize={col: "sum" for col in label_cols}, leading_space=1)
    tolerance = engine.precision_vectorized([engine.reporting_currency], [None])[0] / 2

    for col in label_cols:
        report[col] = apply_threshold_and_format(report[col], tolerance)
        report.at[0, col] = col

    # Step 3: Drop rows with no values and format display
    report = report[~((report[label_cols] == "").all(axis=1) & (report["level"] == "body"))]
    report.loc[report["level"] == "H1", "description"] = report["description"].str.upper()
    visible_cols = ["description"] + label_cols
    bold_rows = report["level"].str.match("^[HS][0-9]$")
    report.loc[bold_rows, visible_cols] = "*" + report.loc[bold_rows, visible_cols] + "*"

    # Step 4: Return formatted Typst table
    return df_to_typst(
        df=report[visible_cols],
        hline=(report.index[report["level"] == "H1"] + 1).to_list(),
        columns=["auto"] + ["1fr"] * len(label_cols),
        align=["left"] + ["right"] * len(label_cols),
        colnames=False
    )

if __name__ == "__main__":
    config = pd.read_csv(StringIO(COLUMNS_CSV), skipinitialspace=True, dtype="string")
    engine = TextLedger(Path("~/macx/rocket-accounting/accounts").expanduser())

    tbl = balance_sheet_table(config, engine)

    typst_document = f"""
        {TEXT_BEFORE_TABLE}
        #{tbl}
        {TEXT_AFTER_TABLE}
    """
    with open(TYPST_OUTPUT, "w", encoding="utf-8") as f:
        f.write(typst_document)
    typst.compile(input=TYPST_OUTPUT, output=PDF_OUTPUT)
