#!/bin/bash

# This is the shellscript called by the ERT FORWARD_JOB

#echo "** Registering case on Sumo *****"
#echo "** Sourcing virtualenv: $1"
source $1
#echo "** Running Python"
#echo "$2" "$3" "$4"
python "$2" "$3" "$4" # > sumo_register_case.log 2>&1
