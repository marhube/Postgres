# For Python 3.20d and later
from importlib.metadata import version, PackageNotFoundError
try:
    __version__ = version("Postgres")
except PackageNotFoundError:
    # package is not installed
    __version__ = 'unknown'