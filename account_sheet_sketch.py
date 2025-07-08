from pathlib import Path
import pandas as pd
import typst
from pyledger import TextLedger, df_to_typst

PDF_OUTPUT = "account-statements.pdf"
TYPST_OUTPUT = Path(PDF_OUTPUT).with_suffix(".typ")

TEXT_BEFORE_TABLE = """
   #set text(font: "DejaVu Sans", size: 11pt, lang: "en")
   == Accounts Sheet
   \\
"""
TEXT_AFTER_TABLE = """
    \\
    That's all, folks.\\
    Cheers!
"""

def format_amount(x: float) -> str:
    return f"{x:,.2f}".replace(",", "'")

def apply_threshold_and_format(series: pd.Series, threshold: float) -> pd.Series:
    return series.map(lambda x: "" if pd.isna(x) or abs(x) < threshold else format_amount(x))

def sanitize_description(text: str) -> str:
    if isinstance(text, str):
        return (
            text.replace("<", "\\<")
                .replace(">", "\\>")
                .replace("@", "\\@")
        )
    return text

def account_sheet_tables(
    engine: TextLedger,
    period: str,
    profit_centers: str | None = None,
    cols: list[str] | None = None
) -> dict[str, str]:
    tables = {}
    accounts = engine.accounts.list()["account"].to_list()
    tolerance = engine.precision_vectorized([engine.reporting_currency], [None])[0] / 2

    for account in accounts:
        df = engine.account_history(account, period=period, profit_centers=profit_centers)
        if df.empty:
            continue

        # Format and sanitize values
        df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
        df["report_amount"] = apply_threshold_and_format(df["report_amount"], tolerance)
        df["report_balance"] = apply_threshold_and_format(df["report_balance"], tolerance)
        df["description"] = df["description"].map(sanitize_description)

        # Drop unused or redundant columns
        df = df[cols]

        # Typst alignment and layout specs
        align = ["left" if col in {"date", "description", "tax_code", "document"} else "right" for col in cols]
        colspecs = ["auto" if col in {"date", "contra"} else "1fr" for col in cols]

        # Rename and bold headers
        rename_map = {
            "report_balance": "Balance",
            "report_amount": "Amount",
            "tax_code": "Tax",
            "description": "Descr",
            "document": "Doc",
        }
        df = df.rename(columns=rename_map)
        df.columns = [f"*{col.upper()}*" for col in df.columns]

        # Generate Typst table
        tables[account] = df_to_typst(
            df=df, columns=colspecs, align=align, hline=[1], colnames=True
        )

    return tables

def generate_account_statement_pdf(
    engine: TextLedger,
    period: str,
    profit_centers: str | None = None,
    cols: list[str] | None = None
) -> str:
    tables = account_sheet_tables(engine, period, profit_centers, cols)

    blocks = []
    for account, table in tables.items():
        blocks.append(f"=== {account}\n\\\n#{table}\n")

    return "\n\n".join(blocks)

def main():
    engine = TextLedger(Path("~/macx/rocket-accounting/accounts").expanduser())
    cols = ["date", "contra", "report_amount", "report_balance", "tax_code", "description", "document"]
    body = generate_account_statement_pdf(engine=engine, period="2025-12-31", cols=cols)

    typst_document = f"""
        {TEXT_BEFORE_TABLE}
        {body}
        {TEXT_AFTER_TABLE}
    """
    with open(TYPST_OUTPUT, "w", encoding="utf-8") as f:
        f.write(typst_document)

    typst.compile(input=TYPST_OUTPUT, output=PDF_OUTPUT)

if __name__ == "__main__":
    main()
