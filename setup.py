from setuptools import setup

setup(
    name='pyledger',
    version='v0.0.1',
    python_requires='>3.10',
    install_requires=['numpy', 'pandas', 'xlsxwriter'],
    description=('Python package to streamline implementation of or '
                 'connection to accounting systems.'),
    long_description=open('README.md').read(),
    url='https://github.com/lasuk/pyledger',
    packages=['pyledger']
)
