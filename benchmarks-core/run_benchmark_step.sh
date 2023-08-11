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
    resource_index=$1
    results_path_local=$2
    python_script=$3
    miniconda_dir=$4
    benchmark_dir='cloud-data-transfer-benchmarking'

    source ${miniconda_dir}/etc/profile.d/conda.sh
    conda activate cloud-data
    cd ${benchmark_dir}/benchmarks-core

    export resource_index
    python -u ${python_script}

    cd .. # Change directory back to `~/cloud-data-transfer-benchmarking`

    # Copy results back to user container and clean up
    stepname=$( echo ${python_script} | cut -d "." -f1 )
    scp -q outputs/results-${stepname}.csv usercontainer:${results_path_local}/results_tmp.csv
}

                        # MAIN PROGRAM #
######################################################################

input_file="inputs.json"
resource_names=$( jq -r '.RESOURCES[] | .Name' ${input_file} )

# Initialize resource tracking index and make results directory & file
results_path=$( pwd )/results/csv-files # Results directory in user container
mkdir -p ${results_path}

# Loop through resources and run conversion code
resource_index=0
for resource in ${resource_names}
do
    miniconda_dir=$( jq -r ".RESOURCES[${resource_index}] | .MinicondaDir" ${input_file} )

    if [ "${miniconda_dir}" == "~" ]
    then
        miniconda_dir="${HOME}/.miniconda3"
    fi

    ssh -q ${resource}.clusters.pw "$(typeset -f f_benchmark); \
                                    f_benchmark ${resource_index} ${results_path} ${python_script} ${miniconda_dir}"
    

    # Append results of current resource's test to a single file
    cat ${results_path}/results_tmp.csv >> ${results_path}/${results_file}
    rm ${results_path}/results_tmp.csv # Clean up tmp file

    let resource_index++
done