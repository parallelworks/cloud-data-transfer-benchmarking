#!/bin/bash

#                           DESCRIPTION
#=======================================================================
# This script orchestrates the entire `cloud-data-transfer-benchmarking`
# workflow when submitted from a workflow form. This flows very similar
# to the notebook version, with the exeception of only being able to run
# with a single bucket and resource. The user can also only supply
# one NetCDF4 file, one CSV file, and also only randomly generate one
# of each of the file formats. Note that the `inputs.json` is not used
# as the input file to the workflow scripts and msut be edited to match
# the standard format in the notebook version.
#=======================================================================

#                              MAIN SCRIPT
#========================================================================
# First, create a new input file that matches the form of the one created by the notebook version
local_conda_sh=$(jq -r '.GLOBALOPTS.local_conda_sh' inputs.json )

source ${local_conda_sh}/etc/profile.d/conda.sh
python $( pwd )/edit_form_inputs.py

# After new input file is created, begin workflow setup
input_file='form_inputs.json'
bash $( pwd )/workflow_setup.sh ${input_file}

# Convert files
bash $( pwd )/benchmarks-core/run_benchmark_step.sh ${input_file} convert-data.py conversions.csv

# Read files
bash $( pwd )/benchmarks-core/run_benchmark_step.sh ${input_file} read-data.py reads.csv

# Remove files
bash $( pwd )/postprocessing/remove-benchmark-files.sh ${input_file}