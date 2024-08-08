from setuptools import setup

setup(
    name='pyledger',
    version='v0.0.1',
    python_requires='>3.10',
    install_requires=[
        'numpy',
        'pandas',
        'xlsxwriter',
        'openpyxl',
        'pytest',
        'consistent_df @ https://github.com/macxred/consistent_df/tarball/main'
    ],
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
            "bandit",
            "safety"
            "pytest-cov"
        ]
    }
)
