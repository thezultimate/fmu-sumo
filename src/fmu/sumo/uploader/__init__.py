"""Top-level package for fmu.sumo"""

try:
    from .version import version
    __version__ = version
except ImportError:
    __version__ = "0.0.0"

from fmu.sumo.uploader._ensembleondisk import EnsembleOnDisk
from fmu.sumo.uploader._connection import SumoConnection
from fmu.sumo.uploader._fileondisk import FileOnDisk
