from pathlib import Path
from io import StringIO
import pandas as pd
import typst
from pyledger import TextLedger, df_to_typst


PDF_OUTPUT = "pl_staggered_cashctrl.pdf"
TYPST_OUTPUT = Path(PDF_OUTPUT).with_suffix(".typ")

TEXT_BEFORE_TABLE = """
#set text(font: "DejaVu Sans", size: 11pt, lang: "en")
== Profit & Loss Staggered Form (2024–2025)
\\
"""

TEXT_AFTER_TABLE = """
\\
That's all, folks.\\
Cheers!
"""

COLUMNS_CSV = """
label,period
2024,2024
2025,2025
"""

LEVELS = [
    {"label": "Gross profit 1", "range": (4000, 4999)},
    {"label": "Gross profit 2", "range": (5000, 5999)},
    {"label": "Operating income 1: EBITDA", "range": (6000, 6799)},
    {"label": "Operating income 2: EBIT", "range": (6800, 6899)},
    {"label": "Operating income 3: EBT", "range": (6900, 6999)},
    {"label": "Annual profit before taxes", "range": (7000, 8899)},
    {"label": "Annual profit", "range": (8900, 8999)},
]


def pl_column(engine: TextLedger, label: str, period: str, profit_centers: str | None) -> pd.DataFrame:
    df = engine.individual_account_balances("4000:8999", period=period, profit_centers=profit_centers)
    df = df[["account", "description", "report_balance"]].copy()
    df = df.groupby(["account", "description"], as_index=False).sum()
    df.rename(columns={"report_balance": label}, inplace=True)
    return df


def pl_sheet(engine: TextLedger, config: pd.DataFrame) -> pd.DataFrame:
    sheet = None
    for row in config.to_dict(orient="records"):
        df = pl_column(engine, row["label"], row["period"], row.get("profit_centers"))
        sheet = df if sheet is None else sheet.merge(df, on=["account", "description"], how="outer")
    return sheet.fillna(0)


def pl_report(sheet: pd.DataFrame, config: pd.DataFrame) -> pd.DataFrame:
    label_cols = list(config["label"])
    rows = []
    last_subtotal = {col: 0.0 for col in label_cols}

    for level in LEVELS:
        label = level["label"]
        acc_min, acc_max = level["range"]

        block = sheet[(sheet["account"] >= acc_min) & (sheet["account"] <= acc_max)].copy()
        if not block.empty:
            detail = block[["description"] + label_cols].groupby("description", as_index=False).sum()
            detail.insert(0, "level", "body")
            rows.append(detail)

            block_sum = detail[label_cols].sum()
        else:
            block_sum = pd.Series([0.0] * len(label_cols), index=label_cols)

        subtotal = {col: last_subtotal[col] + block_sum[col] for col in label_cols}
        last_subtotal = subtotal

        subtotal_row = pd.DataFrame([["subtotal", label] + [subtotal[col] for col in label_cols]],
                                    columns=["level", "description"] + label_cols)
        rows.append(subtotal_row)

    return pd.concat(rows, ignore_index=True)


def pl_table(report: pd.DataFrame, config: pd.DataFrame) -> str:
    label_cols = list(config["label"])

    def format_amount(x: float) -> str:
        return f"{x:,.2f}".replace(",", "'")

    for col in label_cols:
        report[col] = report[col].map(format_amount)

    visible_cols = ["description"] + label_cols
    bold_rows = report["level"] == "subtotal"
    report.loc[bold_rows, visible_cols] = "*" + report.loc[bold_rows, visible_cols] + "*"

    # Add header row as first row of data
    header_row = pd.DataFrame([[f"*{col}*" if i > 0 else "" for i, col in enumerate(visible_cols)]],
                              columns=visible_cols)
    report = pd.concat([header_row, report[visible_cols]], ignore_index=True)

    return df_to_typst(
        df=report,
        hline=[],
        columns=["auto"] + ["1fr"] * len(label_cols),
        align=["left"] + ["right"] * len(label_cols),
        colnames=False
    )


if __name__ == "__main__":
    config = pd.read_csv(StringIO(COLUMNS_CSV), skipinitialspace=True, dtype="string")
    engine = TextLedger(Path("~/macx/rocket-accounting/accounts").expanduser())

    sheet = pl_sheet(engine, config)
    report = pl_report(sheet, config)
    table = pl_table(report, config)

    typst_doc = f"""
{TEXT_BEFORE_TABLE}
#{table}
{TEXT_AFTER_TABLE}
"""
    with open(TYPST_OUTPUT, "w", encoding="utf-8") as f:
        f.write(typst_doc)
    typst.compile(input=TYPST_OUTPUT, output=PDF_OUTPUT)
