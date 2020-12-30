#!/bin/bash

# This is the shellscript called by the ERT FORWARD_JOB

echo "** Registering ensemble on Sumo *****"
echo "** Sourcing virtualenv: $1"
source $1
echo "** Running Python"
python "$2" "$3" "$4"  > log_register_ensemble.txt 2>&1