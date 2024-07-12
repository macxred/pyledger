# Account Management and Integration in Python

[![codecov](https://codecov.io/gh/macxred/pyledger/branch/main/graph/badge.svg)](https://codecov.io/gh/macxred/pyledger)

**PyLedger** is a Python library to facilitate the implementation and integration of accounting systems. PyLedger provides abstract functionality that can be implemented either by connecting to existing accounting software or as a stand-alone accounting solution with your preferred data storage capabilities. Through its abstract interface, PyLedger offers interoperability between these different solutions.

**Key Features:**

- Double-entry accounting system.
- Multiple currency and commodity support.
- Tax calculations and reporting.
- Customizable Data Storage, enabling users to either integrate with existing databases or utilize PyLedger's built-in storage mechanism based on text files.


## Class Hierarchy

PyLedger is designed with a flexible and extensible architecture. Its class hierarchy starts with an abstract base class at its core and extends to specialized implementations for varied use cases. This modularity ensures adaptability, promotes interoperability, and enhances usability across diverse environments.

**Class Inheritance Diagram**

```
LedgerEngine (Abstract Base Class)
   |
   +-- StandaloneLedger (Abstract)
   |    |
   |    +-- MemoryLedger
   |    |
   |    +-- PersistentLedger
   |    |    |
   |    |    +-- TextLedger
   |    |
   |    +-- MemoryLedger
   |         |
   |         +-- TestLedger
   |
   +-- APILedger
       |
· · · · · · · · · · · · · · · · · · · · (External Packages Below)
       |
       +-- CashCtrlLedger
       |
       +-- ProffixLedger (Decommissioned)
```

**Core Components**

1. **LedgerEngine** (Abstract Base Class)\
The foundational element of the PyLedger library, the `LedgerEngine` class, specifies abstract methods and properties for an accounting framework, including ledger operations, account management, and currency handling. It ensures all derived classes maintain a consistent framework and provides reporting features based on the abstract data model.

**Standalone Implementations**

1. **StandaloneLedger**\
This class extends LedgerEngine to provide a self-sufficient ledger system independent of external accounting software. It implements specific business rules like tax calculations and foreign exchange revaluations. Data storage is treated as an abstract concept, which allows for integration with any data storage solution, ensuring maximum flexibility.

1. **MemoryLedger**\
This class extends the StandaloneLedger to provide a fully featured but non-persistent ledger system. MemoryLedger stores accounting data as DataFrames directly in memory, without relying on external data storage solutions. It is particularly useful for demonstration and testing purposes where persistence is not required.

1. **MemoryLedger** extends `StandaloneLedger` to implement an in-memory ledger system. It stores and manages all data in memory using pandas DataFrames, providing fast access and manipulation of ledger data without the need for persistent storage. The class is particularly useful for demonstration and test purposes.

1. **TestLedger** is designed for development and testing, it extends `MemoryLedger` with pre-populated data and settings. It simplifies the initial setup, allowing developers and testers to focus on functionality without the configuration overhead.

1. **PersistentLedger** classes extend `StandaloneLedger` to manage persistent storage of ledger data, ensuring that ledger data is not lost between application runs. `PersistentLedger` subclasses can integrate with different storage backends, such as files, databases, or other persistent storage solutions. The `PersistentLedger` class currently holds no methods or properties; it merely improves clarity of the class hierarchy.

1. **TextLedger** specializes in file-based data storage using CSV files. This approach with data storage in text files allows to leverage git versioning to enhance data integrity and auditability, coupled with GitHub process management tools to facilitate collaboration.

**Integration with External Software**

7. **APILedger** classes connect to an external system or service. They delegate storage and all accounting operations. Subclasses of `APILedger` are interfaces that handle API communication and convert data between PyLedger and the external system's format. The `APILedger` class currently holds no methods or properties; it merely improves clarity of the class hierarchy.

1. **CashCtrlLedger** connects to the web-based CashCtrl accounting software. It implements accessor and mutator methods that use CashCtrl's REST API to manipulate and retrieve accounting data on a remote CashCtrl instance.

1. **ProffixLedger** (Decommissioned) Previously implemented to interface with Proffix ERP software through REST API requests, this class has been decommissioned but can serve as a template for possible future adaptations.
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

# Get accounts
ledger.accounts()
##         currency tax_code                                        description
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
    'contra': 6000,
    'currency': 'CHF',
    "description": "First Test Transaction",
    'amount': -100})
ledger.add_ledger_entry({
    'date': datetime.date.today(),
    'account': 1020,
    'contra': 4000,
    'currency': 'CHF',
    "description": "Second Test Transaction",
    'amount': 200})

# Retrieve account balance
ledger.account_balance(1020)
## {'reporting_currency': 100.0, 'CHF': 100.0}

# Retrieve an account's transaction history
ledger.account_history(1020)
##   id        date  account  contra  currency  amount  balance  report_amount  report_balance  tax_code              description  document
## 0  1  2024-04-12     1020    6000       CHF  -100.0   -100.0         -100.0          -100.0      <NA>   First Test Transaction      <NA>
## 1  2  2024-04-12     1020    4000       CHF   200.0    100.0          200.0           100.0      <NA>  Second Test Transaction      <NA>
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

## Other Projects

Below projects handle accounting tasks, but have a different purpose and scope. They might be of interest to some users or provide inspiration:

- John Wiegley's [ledger](https://ledger-cli.org) is a command-line, double-entry accounting system.
- [Ledger.py](https://github.com/mafm/ledger.py) is a simpler implementation of this concept in Python.
