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

# Sample ledger entries
# Rows without a date form a collective entry with the last row with a date
LEDGER_CSV = """
          date, account, contra, currency,     amount, tax_code,  description
    2024-05-24,    9992,   9995,      CHF,     100.00,   OutRed,  Ledger Entry 1
    2024-05-24,    9991,       ,      USD,    -100.00,   OutRed,  Ledger Entry 2
              ,    9991,       ,      USD,       1.00,   OutRed,  Ledger Entry 2
              ,    9991,       ,      USD,      99.00,   OutRed,  Ledger Entry 2
    2024-04-24,        ,   9990,      EUR,     200.00,   OutRed,  Ledger Entry 3
              ,    9990,       ,      EUR,     200.00,   OutRed,  Ledger Entry 3
    2024-05-24,    9991,   9994,      USD,     300.00,   OutRed,  Ledger Entry 4
    2024-04-04,    9995,       ,      CHF, -125000.00,         ,  Ledger Entry 5
              ,    9994,       ,      USD,  138138.75,         ,  Ledger Entry 5
"""
LEDGER_ENTRIES = pd.read_csv(StringIO(LEDGER_CSV), skipinitialspace=True)

# Initialize TextLedger and populate with sample data
ledger = TextLedger(root_path="/system-root-path")
ledger.mirror_ledger(LEDGER_ENTRIES)
```

#### Git Diff After Mirroring:

![Git Diff Mirroring Example](/assets/git-diff-mirroring.png)

### 2. Adding a Ledger Entry

Adding a new ledger entry results in a clean Git diff showing only the addition.

```python
# New ledger entry
ENTRY_CSV = """
          date,  account, contra, currency, amount,  description
    2024-05-24,    9992,   9995,      CHF, 100.00,  Ledger Entry 6
"""
new_entry = pd.read_csv(StringIO(ENTRY_CSV), skipinitialspace=True)
ledger.add_ledger_entry(new_entry)
```

#### Git Diff After Adding Entry:

![Git Diff Add Example](/assets/git-diff-add.png)

### 3. Modifying a Ledger Entry

Changing the `amount` from `300.00` to `400.00` affects only that specific value.

```python
# Modified ledger entry
modified_entry = pd.DataFrame({
    'id': [4],
    'date': ['2024-05-24'],
    'account': [9991],
    'contra': [9994],
    'currency': ['USD'],
    'amount': [400.00],
    'tax_code': ['OutRed'],
    'description': ['Ledger Entry 4'],
})

ledger.modify_ledger_entry(modified_entry)
```

#### Git Diff After Modifying Entry:

![Git Diff Modify Example](/assets/git-diff-modify.png)

### 4. Deleting a Ledger Entry

Removing a ledger entry only deletes that row without affecting others.

```python
ledger.delete_ledger_entries(ids=[4])
```

#### Git Diff After Deleting Entry:

![Git Diff Delete Example](/assets/git-diff-delete.png)
