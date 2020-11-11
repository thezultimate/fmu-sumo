# fmu-sumo
This package is intended for interaction with Python within the FMU (Fast Model Update) ecosystem. The current primary use case is uploading data from FMU to Sumo, but package may be extended to also use data from Sumo.

**WORK IN PROGRESS - NOT FOR PRODUCTION**

# concepts
SumoConnection: The SumoConnection object represents the connection to Sumo, and will handle authentication etc when initiated. This object uses the Sumo python wrapper under the hood.
EnsembleOnDisk: The EnsembleOnDisk object represents an ensemble of reservoir model realisations. The object relates to the ensemble metadata. Individual files belonging to the ensemble are represented as FileOnDisk objects.
FileOnDisk: The FileOnDisk object represents a single file in an FMU ensemble, stored on the local disk.

*Note that this package does not explicitly contain the concept of "realization" or "iteration" which in the FMU context are important layers in the hierarchy.*

# workflow for uploading as a post-process (after ERT instance is gone)
```
from fmu import sumo

# Establish connection to Sumo
connection = sumo.SumoConnection()

# Initiate the ensemble object
ensemble = sumo.EnsembleOnDisk(manifest_path='/path/to/manifest', sumo_connection=sumo_connection)

# Register the ensemble on Sumo (skip this if already registered e.g. in a HOOK workflow in ERT)
# This uploads ensemble metadata to Sumo
ensemble.register()

# Add file-objects to the ensemble
ensemble.add_files('/globable/path/to/files/*.gri')

# Upload ensemble data objects (files)
ensemble.upload()

```

# workflow for uploading during ERT runs
HOOK:
```
from fmu import sumo

# Establish connection to Sumo
connection = sumo.SumoConnection()

# Initiate the ensemble object
ensemble = sumo.EnsembleOnDisk(manifest_path='/path/to/manifest', sumo_connection=sumo_connection)

# Register the ensemble on Sumo (skip this if already registered e.g. in a HOOK workflow in ERT)
# This uploads ensemble metadata to Sumo
ensemble.register()
```

FORWARD_JOB:
```
# Establish connection to Sumo
connection = sumo.SumoConnection()

# Initiate the ensemble object
ensemble = sumo.EnsembleOnDisk(manifest_path='/path/to/manifest', sumo_connection=sumo_connection)

# Add file-objects to the ensemble
ensemble.add_files('/globable/path/to/files/*.gri')

# Upload ensemble data objects (files)
ensemble.upload()

```
*Notice that the difference is that when uploading during ERT runs, the ensemble is pre-registered as a HOOK workflow.
