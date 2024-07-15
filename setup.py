from setuptools import setup

# Read the requirements from the 'requirements.txt' file
with open('requirements.txt', 'r') as requirements_file:
    required_packages = requirements_file.readlines()

setup(
    name='pyledger',
    version='v0.0.1',
    python_requires='>3.10',
    install_requires=required_packages,
    description=('Python package to streamline implementation of or '
                 'connection to accounting systems.'),
    long_description=open('README.md').read(),
    url='https://github.com/macxred/pyledger',
    packages=['pyledger'],
        extras_require={
        "dev": [
            "flake8",
            "flake8-import-order",
            "flake8-docstrings",
            "flake8-bugbear",
        ]
    }
)
