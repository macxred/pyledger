Here’s the **full updated README** with the images included as requested and the **File Structure** and **Configuration Settings** sections added.

---

# TextLedger

`TextLedger` is part of the PyLedger library, providing a file-based storage system for accounting data that integrates seamlessly with Git. By storing data in fixed-width CSV files, it allows for easy inspection, minimal Git diffs, and comprehensive version control. This makes `TextLedger` ideal for maintaining data integrity, ensuring auditability, and collaborating with multiple contributors.

## Key Features and Versioning with Git

`TextLedger` is optimized for file-based storage and seamless integration with Git, providing both human-readable data and powerful version control features:

- **File-based Storage**: Accounting data is stored in fixed-width CSV files, making it easy to inspect, version control, and maintain consistency.
- **Optimized for Git**: Mutator functions only modify necessary files, preserving row order to produce clean and concise Git diffs, ensuring minimal file changes.
- **Readable Format**: CSV files maintain consistent column widths for readability, and configuration settings are stored in easily editable YAML format.
- **Auditability and Track Changes**: Git provides a full audit trail, allowing you to track every change to ledger entries, accounts, and settings. You can easily review what was changed, when, and by whom.
- **Collaboration**: With text-based files, multiple contributors can work on the same ledger using Git. This ensures a clear version history and simplifies merging changes or resolving conflicts.

---

## Example Usage

Here’s how you can use `TextLedger` to manage ledger data. We will demonstrate how changes to the ledger entries result in minimal, precise Git diffs that maintain the integrity of the ledger file.

```python
import pandas as pd
from io import StringIO
from pyledger import TextLedger

# Example ledger CSV data to simulate ledger entries
LEDGER_CSV = """
id,     date,  account, contra, currency,     amount, report_amount, tax_code,   description,                      document
1,  2024-05-24,   9992,   9995,      CHF,     100.00,              ,   OutRed,   ledger 1,
2,  2024-05-24,   9991,       ,      USD,    -100.00,        -88.88,   OutRed,   ledger 2,
2,  2024-05-24,   9991,       ,      USD,       1.00,          0.89,   OutRed,   ledger 2,
2,  2024-05-24,   9991,       ,      USD,      99.00,         87.99,   OutRed,   ledger 2,
3,  2024-04-24,       ,   9990,      EUR,     200.00,        175.55,   OutRed,   ledger 3,
3,  2024-04-24,   9990,       ,      EUR,     200.00,        175.55,   OutRed,   ledger 3,
4,  2024-05-24,   9991,   9994,      USD,     300.00,        450.45,   OutRed,   ledger 4,
5,  2024-04-04,   9995,       ,      CHF, -125000.00,    -125000.00,         ,   ledger 5,
5,  2024-04-04,   9994,       ,      USD,  138138.75,     125000.00,         ,   ledger 5,
"""

# Load CSV data into a DataFrame
LEDGER_ENTRIES = pd.read_csv(StringIO(LEDGER_CSV), skipinitialspace=True)

# Initialize TextLedger with the given root path
ledger = TextLedger(root_path="/system-root-path")

# Mirror the entire ledger into the TextLedger
ledger.mirror_ledger(LEDGER_ENTRIES)
```

#### Git Diff After mirroring

![Git Diff Mirroring Example](/assets/git-diff-mirroring.png)


### 1. Adding a Ledger Entry

We will add a ledger entry with `id == 1` to the ledger.

```python
# Add a single ledger entry and reflect it in the ledger (Git should only show this new row as added)
new_entry = LEDGER_ENTRIES.query("id == '1'").assign(id='6')
ledger.add_ledger_entry(new_entry)
```

#### Git Diff After Adding Entry

![Git Diff Add Example](/assets/git-diff-add.png)

**Explanation**: This Git diff shows only the addition of the new row. No other changes were made, demonstrating the minimal impact of adding a single ledger entry.


### 2. Modifying a Ledger Entry

Next, we will modify an existing ledger entry with `id == 4`.

```python
# Modify an existing ledger entry (Git will show only the specific value change within the row)
new_entry = LEDGER_ENTRIES.query("id == '4'").assign(id="default.csv:4", amount=400)
ledger.modify_ledger_entry(new_entry)
```

#### Git Diff After Modifying Entry

![Git Diff Modify Example](/assets/git-diff-modify.png)

**Explanation**: Here, only the `amount` field was modified from `300.00` to `400.00`. The rest of the row remains unchanged, and the row order is preserved.


### 3. Deleting a Ledger Entry

Finally, we will delete the ledger entry with `id == 4`.

```python
# Delete ledger entries with ID 4 (Git will show the entire row removed without altering the row order)
ledger.delete_ledger_entries(ids=['default.csv:4'])
```

#### Git Diff After Deleting Entry

![Git Diff Delete Example](/assets/git-diff-delete.png)

**Explanation**: The row with `id == 4` is removed, and no other rows or values are affected. The row order remains intact.

---

## File Structure

The `TextLedger` stores its files in a structured directory format, making it easy to navigate and track using Git. The `ledger/` folder is flexible and can contain nested subdirectories for specific ledgers or companies, each containing a `default.csv` file for ledger entries:

```
/system-root-path/
    |-- ledger/                           # Root folder for all ledger files
    |   |-- default.csv                   # Default ledger entries
    |   |-- companies/company.csv         # Ledger files for specific projects
    |
    |-- accounts.csv                      # Account chart
    |-- tax_codes.csv                     # Tax codes
    |-- settings.yml                      # Configuration settings
```

- **ledger/**: The main folder for ledger entries, which can contain subdirectories (e.g., `companies/`, `projects/`) to further organize data.
- **default.csv**: The primary ledger file in each folder, containing ledger entries in fixed-width CSV format.
- **accounts.csv**: Stores account chart information in fixed-width CSV format.
- **tax_codes.csv**: Contains tax code data, including VAT rates.
- **settings.yml**: Configuration settings, such as the reporting currency, stored in a human-readable YAML format for easy editing.

---

## Configuration Settings

`TextLedger` stores its configuration in a `settings.yml` file. This file is easily editable and tracks settings like the reporting currency.

Example `settings.yml`:

```yaml
reporting_currency: "CHF"
```

---

### Benefits of Minimal Invasive Changes in `TextLedger`

By using `TextLedger`, the changes to the ledger file are minimal and precise. As demonstrated:

1. **Small, Targeted Git Diffs**: Only the necessary rows or values are added, modified, or deleted, keeping the diffs concise and easy to review.
2. **Preserved Row Order**: Despite making multiple changes, the overall structure and order of the ledger are maintained.
3. **Auditability**: Each change can be tracked using Git, ensuring accountability for modifications to the ledger.
4. **Collaboration**: With such small, non-invasive changes, collaboration between multiple users is simplified, reducing the risk of conflicts in version control.
