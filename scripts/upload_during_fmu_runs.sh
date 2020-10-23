#!/bin/bash

# This is the shellscript called by the ERT FORWARD_JOB

echo "Sourcing virtualenv: $1"
source $1

# run script with arguments
python bin/sumo/fmu-sumo/scripts/upload_during_fmu_runs.py "$2" "$3" "$4" "$5"

# let me know if python script has finished
echo "DONE UPLOADING!"
