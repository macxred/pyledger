"""PyLedger package

**pyledger** is a Python package designed to standardize interaction with
accounting systems and facilitate interoperability between different
accounting software. It provides an abstract interface for a double accounting
system with an account chart, foreign currencies, arbitrary assets and vat
management.

The package includes a stand-alone implementation of the interface that is
storing data solely in text files and thereby lends itself to version tracking
with git or any other versioning system.

The abstract interface can be implemented to connect to any accounting software
via RESTful API or other interfaces, allowing for standardized access to such
software or data exchange between differing systems.

See README.md at https://github.com/macxred/pyledger for setup instructions.
"""

from .helpers import *
from .time import *
from .ledger_engine import LedgerEngine
from .standalone_ledger import StandaloneLedger
from .text_ledger import TextLedger
from .sandbox_ledger import SandboxLedger
