# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Development Commands

### Installation
```bash
# For development (creates symbolic link to enable live code changes)
pip install -e .

# Standard installation from GitHub
pip install https://github.com/macxred/pyledger/tarball/main
```

### Testing
```bash
# Run all tests
pytest

# Run specific test by name pattern
pytest -k test_accounts

# Run specific test file
pytest pyledger/tests/test_journal.py

# Run with coverage (as used in CI)
coverage run -m pytest -W error
```

### Linting
```bash
# Run flake8 linter (follows Google import style, max line length 100)
flake8
```

## Architecture

### Class Hierarchy

PyLedger uses an inheritance-based architecture with abstract base classes:

```
LedgerEngine (Abstract Base Class)
   ├── StandaloneLedger (Abstract)
   │    ├── MemoryLedger (in-memory, for testing)
   │    └── PersistentLedger (Abstract)
   │         └── TextLedger (CSV/YAML file storage)
   └── ExternalLedger (Abstract)
        └── CashCtrlLedger (external package)
```

**LedgerEngine** (`ledger_engine.py`): Abstract base class defining the core interface for double-entry accounting. All subclasses must implement methods for account management, journal operations, and currency handling. Provides reporting features built on the abstract data model.

**StandaloneLedger** (`standalone_ledger.py`): Self-contained implementation with business logic for tax calculations, foreign exchange revaluations, and reconciliations. Storage is abstract to allow flexibility.

**TextLedger** (`text_ledger.py`): Stores data in fixed-width CSV files (for tabular data) and YAML (for configuration). Designed for Git version control with minimal diffs. Files are human-readable and easily editable.

### Storage Abstraction Pattern

All data access goes through **AccountingEntity** objects (`storage_entity.py`):
- `AccountingEntity` (abstract base): Defines interface with `list()`, `add()`, `modify()`, `delete()`, and `mirror()` methods
- `CSVAccountingEntity`: Single CSV file storage
- `MultiCSVEntity`: Multiple CSV files (e.g., journal entries across subdirectories)
- `JournalEntity`: Specialized for journal with auto-incrementing IDs

Each ledger implementation exposes entities as properties:
- `engine.accounts` - Chart of accounts
- `engine.journal` - General ledger entries
- `engine.tax_codes` - Tax/VAT code definitions
- `engine.assets` - Currency and commodity definitions
- `engine.price_history` - Historical exchange rates and prices
- `engine.profit_centers` - Cost center definitions (StandaloneLedger only)
- `engine.reconciliation` - Bank reconciliation records (StandaloneLedger only)
- `engine.revaluations` - FX revaluation configurations (StandaloneLedger only)

### Testing Architecture

Tests use a base class pattern for reusability:
- **Base test classes** (e.g., `BaseTestAccounts`, `BaseTestJournal`) define common test logic
- **Implementation-specific tests** inherit base classes and test concrete implementations
- Tests are located in `pyledger/tests/`
- Naming: `base_test_*.py` for base classes, `test_*.py` for concrete tests
- CI runs tests with `coverage run -m pytest -W error` (treats warnings as errors)

### Key Design Patterns

1. **Schema Validation**: Uses `consistent_df` package to enforce DataFrame schemas throughout
2. **Caching**: `@timed_cache` decorator on expensive operations like `serialized_ledger()` and `account_balance()`
3. **Multi-currency**: Reporting currency can differ from transaction currencies; automatic conversion
4. **File Format**: TextLedger uses fixed-width CSV for clean Git diffs and easy visual inspection
5. **Callback Pattern**: Entities support `on_change` callbacks to invalidate caches when data changes

## TextLedger File Structure

When using TextLedger, data is stored in this directory structure:

```
root/
├── account_chart.csv       # Chart of accounts
├── journal/                # Journal entries (can have subdirectories)
│   ├── topic.csv
│   └── ...
└── settings/
    ├── configuration.yml   # Reporting currency and other settings
    ├── assets.csv         # Currency/commodity definitions
    ├── price_history.csv  # Exchange rates and historical prices
    ├── tax_codes.csv      # VAT/sales tax definitions
    ├── revaluations.csv   # FX revaluation rules
    └── profit_centres.csv # Profit center definitions
```

## Dependencies

Core dependencies:
- `pandas`, `polars`, `numpy` - Data manipulation
- `consistent_df` - Schema validation (from GitHub)
- `pytest` - Testing framework
- `pyyaml` - Configuration files
- `typst` - Report generation
- `openpyxl`, `xlsxwriter` - Excel export

Development dependencies:
- `flake8` - Linting
- `bandit` - Security checks

## Code Style Preferences

### General Style
- Max line length: 100 characters
- No empty lines between tightly coupled statements
- Empty line before `if` statements
- Empty line before `return` statements
- Empty line after setup/initialization blocks
- Avoid unnecessary intermediate variables for simple assignments that can be inlined
- No obvious comments that explain what the code clearly does

### Pull Request Guidelines
- **PR Title**: Keep simple and descriptive. Do NOT include issue numbers (e.g., "#239")
- **PR Body**: Leave empty by default. Only add description as comments if explicitly requested
- **Linking Issues**: Use PR body or comments to reference issues, not the title
