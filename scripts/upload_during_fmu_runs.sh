#!/bin/bash

# This is the shellscript called by the ERT FORWARD_JOB


echo "** Uploading files to Sumo (environment: $4)"
echo "** Search query: $3"
echo "** Sourcing virtualenv: $1"
source $1

#type python

#which python

#env | sort

# run script with arguments
python bin/fmu-sumo/scripts/upload_during_fmu_runs.py "$2" "$3" "$4" "$5"  >> sumo_upload.log 2>&1

#echo bin/sumo/scripts/upload_during_fmu_runs.py "$2" "$3" "$4" "$5"

# let me know if python script has finished
echo "** DONE UPLOADING!"
