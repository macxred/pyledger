# TextLedger

`TextLedger` is part of the PyLedger library, offering text-based storage for accounting data that integrates seamlessly with Git, code editors, and ChatGPT. By storing records in fixed-width CSV files, it enables easy inspection, quick editing, and robust version control—ideal for data integrity, auditability, and collaborative work.

## Key Features

- **Track Changes**: Leverages Git for version control, providing a full audit trail and facilitating collaboration among contributors.
- **Human-Readable Format**: Stores data in fixed-width CSV files and configurations in YAML format for immediate editing with code editors like Visual Studio Code.
- **Automation Friendly**: Supports Python and other scripting languages to add or modify entries, process data, or generate flexible reports.
- **AI Integration**: Utilize ChatGPT to generate new ledger entries from PDF documents, benefiting from the native CSV format and existing data for training.
- **Change Management**: Quickly review changes through clean, concise Git diffs and easily discard unwanted modifications.

## File Structure

`TextLedger` stores the complete accounting information set in a structured directory for easy navigation and Git tracking. This self-contained setup allows you to answer any accounting question and generate reports without connecting to external services.

```
/system-root-path/
├── ledger/              # Contains ledger entries; can have subdirectories
│   ├── topic.csv        # Ledger entries for specific topics or projects
│   └── ...              # Additional ledger files or folders
├── accounts.csv         # Account chart
├── settings.yml         # Configurations (e.g., reporting currency)

++ Optional ++

├── tax_codes.csv        # Tax code definitions
├── assets.csv           # Definitions of currencies, securities, assets
├── price/               # Contains exchange rates and asset prices
│   ├── topic.csv        # Price history for specific assets
│   └── ...              # Additional price files or folders
└── revaluations.csv     # Defines when and which accounts need revaluation
```

## Example Usage

### 1. Populating with Sample Data

```python
import pandas as pd
from io import StringIO
from pyledger import TextLedger

# Sample accounting data

TAX_CODES_CSV = """
    id,      account, rate, is_inclusive, description
    EXEMPT,         , 0.00,         True, Exempt from VAT
    IN_STD,     1300, 0.20,         True, Input VAT at Standard Rate 20%
    IN_RED,     1300, 0.05,         True, Input VAT at Reduced Rate 5%
    OUT_STD,    2200, 0.20,         True, Output VAT at Standard Rate 20%
    OUT_RED,    2200, 0.05,         True, Output VAT at Reduced Rate 5%
"""
TAX_CODES = pd.read_csv(StringIO(TAX_CODES_CSV), skipinitialspace=True)

ACCOUNT_CHART_CSV = """
    group,       account, currency, tax_code, description
    Assets,         1000,      USD,         , Cash in Bank
    Assets,         1100,      USD,         , Accounts Receivable
    Assets,         1300,      USD,         , VAT Recoverable (Input VAT)
    Liabilities,    2000,      USD,         , Accounts Payable
    Liabilities,    2200,      USD,         , VAT Payable (Output VAT)
    Equity,         3000,      USD,         , Owner's Equity
    Revenue,        4000,      USD,  OUT_STD, Sales Revenue
    Expenses,       5000,      USD,   IN_STD, Purchases
    Expenses,       6000,      USD,   IN_RED, Utilities Expense
    Expenses,       7000,      USD,         , Rent Expense
    Other Income,   8000,      USD,         , Interest Income
"""
ACCOUNT_CHART = pd.read_csv(StringIO(ACCOUNT_CHART_CSV), skipinitialspace=True)

LEDGER_CSV = """
          date, account, contra, currency,   amount, tax_code, description
    2023-01-01,    1000,   3000,      USD, 10000.00,         , Owner Investment
    2023-01-05,    1000,   5000,      USD, -5000.00,   IN_STD, Purchase of Equipment
    2023-01-10,    1000,   4000,      USD,  3600.00,  OUT_STD, Sale of Goods
    2023-01-15,    1000,   2000,      USD, -2000.00,         , Payment to Supplier
    2023-01-20,    1000,   6000,      USD,  -500.00,   IN_RED, Utilities Expense
    2023-01-25,    1000,   5000,      USD, -1000.00,   IN_STD, Purchase of Inventory
    2023-01-30,    1000,   4000,      USD,  2400.00,  OUT_STD, Sale of Goods
    2023-02-01,    1000,   7000,      USD,  -800.00,         , Rent Expense
"""
LEDGER_ENTRIES = pd.read_csv(StringIO(LEDGER_CSV), skipinitialspace=True)

SETTINGS = {"reporting_currency": "USD"}

# Initialize TextLedger and populate with sample data
engine = TextLedger("demo-accounting-system")
engine.restore(
    settings=SETTINGS,
    tax_codes=TAX_CODES,
    accounts=ACCOUNT_CHART,
    ledger=LEDGER_ENTRIES)
```

### 2. Managing Ledger Entries

You can efficiently manage ledger entries by adding, modifying, and deleting entries as needed.

1. **Add**: Record a new transaction:

    ```python
    NEW_ENTRY_CSV = """
          date, account, contra, currency,   amount, tax_code, description
    2023-01-18,    1000,   8000,      USD,    100.00,        , Interest Income
    """
    NEW_ENTRY = pd.read_csv(StringIO(NEW_ENTRY_CSV), skipinitialspace=True)
    engine.ledger.add(NEW_ENTRY)
    ```

2. **Modify**: Adjust an existing entry, such as updating the amount for the sale of goods on `2023-01-10` from `$3,600.00` to `$4,000.00`.

    ```python
    # Modify the amount for the sale of goods on 2023-01-10
    entries = engine.ledger.list()
    to_modify = entries.query("date == '2023-01-10'")
    to_modify['amount'] = 4000.00
    engine.ledger.modify(to_modify)
    ```

3. **Delete**: Remove an erroneous entry, such as the purchase of inventory on `2023-01-25`.

    ```python
    # Delete the purchase of inventory on 2023-01-25
    entries = engine.ledger.list()
    id_to_delete = entries.query("date == '2023-01-25'")['id']
    engine.ledger.delete({'id': id_to_delete})
    ```

These operations result in clean Git diffs, making it easier to track and review changes:

<img src="/assets/text-ledger-readme-git-diff-example.png" width=75% height=75%>