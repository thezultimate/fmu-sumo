# fmu-sumo
This package is intended for interaction with Python within the FMU (Fast Model Update) ecosystem. The current primary use case is uploading data from FMU to Sumo, but package may be extended to also use data from Sumo.

WORK IN PROGRESS - NOT FOR PRODUCTION

# concepts
SumoConnection: The SumoConnection object represents the connection to Sumo, and will handle authentication etc when initiated. This object uses the Sumo python wrapper under the hood.
EnsembleOnDisk: The EnsembleOnDisk object represents an ensemble of reservoir model realisations. The object relates to the ensemble metadata. Individual files belonging to the ensemble are represented as FileOnDisk objects.
FileOnDisk: The FileOnDisk object represents a single file in an FMU ensemble, stored on the local disk.

# workflow for uploading
When uploading an ensemble, it happens through this pattern:
```
from fmu import sumo

# a connection to Sumo is established
connection = sumo.SumoConnection()

# the ensemble object is initiated
ensemble = sumo.EnsembleOnDisk(manifest_path='/path/to/manifest', sumo_connection=sumo_connection)

# 

```
Ensemble registration: The ensemble is registered on Sumo, or confirmed already registered. Example:
```
dkjfvnfd
```
