# Example usage scripts
These are scripts that show how this package can be used. They are currently focused on uploading in an FMU setting.

There are two contexts:

1) Uploading while running ERT
2) Uploading after an ERT run is finished

The first context is to be considered the preferred and most normal one. 

In this context, ensemble registration happens first as a HOOK workflow, then uploading happens during the run on various places in the workflow.