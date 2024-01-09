Account Management and Integration in Python
---------------------------------------------------------------------

**PyLedger** is a Python library designed to streamline the implementation and management of accounting systems. PyLedger provides abstract functionality that can be implemented either by connecting to existing accounting software via a REST API or as a stand-alone accounting solution with your preferred data storage capabilities. Through its abstract interface, pyLedger offers interoperability between these different solutions.

Key Features:

- Double-entry accounting system.
- Multiple currency and commodity support.
- VAT calculations and reporting.
- Customizable Data Storage, enabling users to either integrate with existing databases or utilize PyLedger's built-in storage mechanism based on text files.

## Installation

Run the following command in your terminal to install pyledger:
```bash
pip install git+ssh://git@github.com/macxred/pyledger.git
```

To update an existing installation to the latest version, use:
```bash
pip install --upgrade --force-reinstall git+ssh://git@github.com/macxred/pyledger.git
```

Installation requires SSH access to the GitHub repository.
If you encounter any installation issues, confirm your SSH access by attempting
to clone the repository with `git clone git@github.com:macxred/accountbot.git`.


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

### Contributing

We welcome contributions from the community! If you're interested in improving PyLedger, check out our contribution guidelines.