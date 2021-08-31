# fmu-sumo
This package is intended for interaction with Sumo within the FMU (Fast Model Update) ecosystem. 
The current primary use case is uploading data from FMU to Sumo.

**WORK IN PROGRESS - NOT FOR PRODUCTION**

# concepts
SumoConnection: The SumoConnection object represents the connection to Sumo, and will handle authentication etc when initiated. This object uses the Sumo python wrapper under the hood.
CaseOnDisk: The CaseOnDisk object represents an ensemble of reservoir model realisations. The object relates to the case metadata. Individual files belonging to the case are represented as FileOnDisk objects.
FileOnDisk: The FileOnDisk object represents a single file in an FMU case, stored on the local disk.

*Note that this package does not explicitly relate to "realization" or "iteration" which in the FMU context are important layers in the hierarchy.*

# workflow for uploading as a post-process (after ERT instance is gone)
```
from fmu.sumo import uploader

# Establish connection to Sumo
connection = uploader.SumoConnection()

# Initiate the case object
case = sumo.CaseOnDisk(
    case_metadata_path="/path/to/case_metadata.yml",
    sumo_connection=sumo_connection
    )

# This uploads case metadata to Sumo
case.register()

# Add file-objects to the case
case.add_files("/globable/path/to/files/*.gri")

# Upload case data objects (files)
case.upload()

```

# workflow for uploading during ERT runs

HOOK (presim) workflow registering the case:
```
from fmu.sumo import uploader

# Establish connection to Sumo
connection = sumo.SumoConnection()

# Initiate the case object
case = sumo.CaseOnDisk(
    case_metadata_path="/path/to/case_metadata.yml",
    sumo_connection=sumo_connection
    )

# Register the case on Sumo
# This uploads case metadata to Sumo
case.register()
```

FORWARD_JOB uploading data (can be repeated multiple times during a workflow):
```
from fmu.sumo import uploader

# Establish connection to Sumo
connection = sumo.SumoConnection()

# Initiate the case object
case = sumo.CaseOnDisk(
    case_metadata_path="/path/to/case_metadata",
    sumo_connection=sumo_connection
    )

# Add file-objects to the case
case.add_files("/globable/path/to/files/*.gri")

# Upload case data objects (files)
case.upload()

```
*Notice that the difference is that when uploading during ERT runs, the case is pre-registered as a HOOK workflow.
