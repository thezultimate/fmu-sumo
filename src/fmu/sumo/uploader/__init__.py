"""Top-level package for fmu.sumo"""

try:
    from .version import version
    __version__ = version
except ImportError:
    __version__ = "0.0.0"

from fmu.sumo.uploader._caseondisk import CaseOnDisk
from fmu.sumo.uploader._caseonjob import CaseOnJob
from fmu.sumo.uploader._connection import SumoConnection, SumoConnectionWithOutsideToken
from fmu.sumo.uploader._fileondisk import FileOnDisk
from fmu.sumo.uploader._fileonjob import FileOnJob


