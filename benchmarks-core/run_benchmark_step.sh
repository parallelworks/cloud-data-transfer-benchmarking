#!/bin/bash

                        # DESCRIPTION #
######################################################################
# This script is written as a template to loop through all resources #
# and execute a python script for the benchmarking. Specifically,    #
# both file conversions and reads can be orchestrated using this     #
# script, as long as the script inputs are corret. These will        #
# be predefined in either the Jupyter notebook or `main.sh`          #
######################################################################

                            # INPUTS #
######################################################################
# Since this is a general script to run benchmarking steps, the
# workflow will supply the name of the python script as well as the
# name of the results file to write the csv data to.
python_script=$1
results_file=$2

                        # DEFINE FUNCTION #
######################################################################
f_benchmark() {
    # Set user container path to write results to
    results_path_local=$2
    python_script=$3

    source .miniconda3/etc/profile.d/conda.sh
    conda activate cloud-data
    cd benchmarks-core

    export resource_index=$1
    python -u ${python_script}

    # Copy results back to user container
    scp -q results_tmp.csv usercontainer:${results_path_local}
}

                        # MAIN PROGRAM #
######################################################################

resource_names=$( jq -r '.RESOURCES[] | .Name' benchmark_info.json )

# Initialize resource tracking index and make results directory & file
resource_index=0
results_path=$( pwd )/results/csv-files # Results directory in user container
mkdir -p ${results_path}

# Loop through resources and run conversion code
for resource in ${resource_names}
do
    ssh -q ${resource}.clusters.pw "$(typeset -f f_benchmark); \
                                    f_benchmark ${resource_index} ${results_path} ${python_script}"
    

    # Append results of current resource's test to a single file
    cat ${results_path}/results_tmp.csv >> ${results_path}/{results_file}.csv
    rm ${results_path}/results_tmp.csv # Clean up tmp file

    let resource_index++
done