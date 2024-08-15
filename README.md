# Account Management and Integration in Python

[![codecov](https://codecov.io/gh/macxred/pyledger/branch/main/graph/badge.svg)](https://codecov.io/gh/macxred/pyledger)

**PyLedger** is a Python library to facilitate the implementation and integration of accounting systems. PyLedger provides abstract functionality that can be implemented either by connecting to existing accounting software or as a stand-alone accounting solution with your preferred data storage capabilities. Through its abstract interface, PyLedger offers interoperability between these different solutions.

**Key Features:**

- Double-entry accounting system.
- Multiple currency and commodity support.
- VAT calculations and reporting.
- Customizable Data Storage, enabling users to either integrate with existing databases or utilize PyLedger's built-in storage mechanism based on text files.


## Class Hierarchy

PyLedger is designed with a flexible and extensible architecture. Its class hierarchy starts with an abstract base class at its core and extends to specialized implementations for varied use cases. This modularity ensures adaptability, promotes interoperability, and enhances usability across diverse environments.


**Class Inheritance Diagram**

```
LedgerEngine (Abstract Base Class)
   |
   +-- StandaloneLedger
   |    |
   |    +-- TextLedger
   |    |
   |    +-- TestLedger
   |    |
   |    +-- MemoryLedger
   |
· · · · · · · · · · · · · · · · · · · · (External Packages Below)
   |
   +-- ProffixLedger (Decommissioned)
   |
   +-- CashCtrlLedger (Upcoming)
```

**Core Components**


1. **LedgerEngine** (Abstract Base Class)\
The foundational element of the PyLedger library, the LedgerEngine class, specifies abstract methods and properties for an accounting framework, including ledger operations, account management, and currency handling. It ensures all derived classes maintain a consistent framework and provides reporting features based on the abstract data model.

**Stand-Alone implementations**

1. **StandaloneLedger**\
This class extends LedgerEngine to provide a self-sufficient ledger system independent of external accounting software. It implements specific business rules like VAT calculations and foreign exchange adjustments. Data storage is treated as an abstract concept, which allows for integration with any data storage solution, ensuring maximum flexibility.

1. **TextLedger**\
An extension of StandaloneLedger, TextLedger specializes in file-based data storage using CSV files. This approach with data storage in text files allows to leverage git versioning to enhance data integrity and auditability, coupled with GitHub process management tools to facilitate collaboration.

1. **TestLedger**\
Designed for development and testing, TestLedger extends StandaloneLedger with pre-populated, hard-coded data and settings. It simplifies the initial setup, allowing developers and testers to focus on functionality without the configuration overhead.

1. **MemoryLedger**\
This class extends the StandaloneLedger to provide a non-persistent, in-memory ledger system. This implementation stores accounting data as DataFrame objects directly in memory, without relying on external data storage solutions. It is particularly useful for demonstration purposes and testing environments where persistence is not required.

**Integration with External Software** (Implemented in separate packages)

1. **ProffixLedger** (Decommissioned)\
Previously implemented to interface with Proffix ERP software through REST API requests, this class has been decommissioned but can serve as a template for possible future adaptations.

1. **CashCtrlLedger** (Upcoming)\
The upcoming CashCtrlLedger will implement the abstract LedgerEngine class with the CashCtrl accounting software via REST API. Get and set methods of the abstract class are routed via REST API requests to the CashCtrl server.

_(External software integrations are maintained in separate packages to avoid pulling in unnecessary dependencies into the primary PyLedger package.)_



## Installation

Easily install the package using pip:

```bash
pip install https://github.com/macxred/pyledger/tarball/main
```

## Basic Usage

Get started using PyLedger with the pre-configured TestLedger() class:

```python
import datetime
from pyledger import TestLedger

# Instantiate a ledger engine pre-populated with hard-coded data
ledger = TestLedger()

# Get chart of accounts
ledger.account_chart()
##         currency vat_code                                               text
## account
## 1000         CHF      NaN                                       Cash on hand
## 1020         CHF      NaN                                     Cash at Bank A
## 1030         CHF      NaN                                     Cash at Bank B
## 1045         CHF      NaN                         Credit cards / debit cards
## 1100         CHF      NaN                                  Trade receivables
## [..snip..]

# Post transactions to the general ledger
ledger.add_ledger_entry({
    'date': datetime.date.today(),
    'account': 1020,
    'counter_account': 6000,
    'currency': 'CHF',
    'text': "First Test Transaction",
    'amount': -100})
ledger.add_ledger_entry({
    'date': datetime.date.today(),
    'account': 1020,
    'counter_account': 4000,
    'currency': 'CHF',
    'text': "Second Test Transaction",
    'amount': 200})

# Retrieve account balance
ledger.account_balance(1020)
## {'base_currency': 100.0, 'CHF': 100.0}

# Retrieve an account's transaction history
ledger.account_history(1020)
##   id        date  account  counter_account currency  amount  balance  base_currency_amount  base_currency_balance vat_code                     text document
## 0  1  2024-04-12     1020             6000      CHF  -100.0   -100.0                -100.0                 -100.0     <NA>   First Test Transaction     <NA>
## 1  2  2024-04-12     1020             4000      CHF   200.0    100.0                 200.0                  100.0     <NA>  Second Test Transaction     <NA>
```

## Testing Strategy

Tests are housed in the [pyledger/tests](tests) directory and are automatically
executed via GitHub Actions. This ensures that the code is tested after each
commit, during pull requests, and on a daily schedule. We prefer pytest for its
straightforward and readable syntax over the unittest package from the standard
library.


## Package Development

We recommend to work within a virtual environment for package development.
You can create and activate an environment with:

```bash
python3 -m venv ~/.virtualenvs/env_name
source ~/.virtualenvs/env_name/bin/activate  # alternative: workon env_name
```

To locally modify and test pyledger, clone the repository and
execute `python setup.py develop` in the repository root folder. This approach
adds a symbolic link to your development directory in Python's search path,
ensuring immediate access to the latest code version upon (re-)loading the
package.
