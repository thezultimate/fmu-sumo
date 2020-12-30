"""Top-level package for fmu.sumo"""

try:
    from .version import version
    __version__ = version
except ImportError:
    __version__ = "0.0.0"

from ._ensembleondisk import EnsembleOnDisk
from ._fileondisk import FileOnDisk
from ._connection import SumoConnection

from ._upload_files import upload_files