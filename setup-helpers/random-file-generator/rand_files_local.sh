#!/bin/bash

                           #DESCRIPTION#
######################################################################
# This script orchestrates the writes of randomly generated files    #
# to all cloud storage locations specified in `inputs.json`.         #
# Inputs are a list of strings fro the aforementioned file, which    #
# are automatically extracted.                                       #
######################################################################

# Sample Call:
# bash /path/to/randfiles.sh "<true/false>" "<true/false>" ... "<true/false>"

                            # MAIN PROGRAM #
#########################################################################
# Check if user has specified any randomly generated files to be created
input_file='inputs.json'
remote_benchmark_dir='cloud-data-transfer-benchmarking'

for bool in $@
do
    if [ "${bool}" == "true" ]
    then
        echo -e "Generating random files (this will take a while for large size requests)..."
        # Set resource to write randomly generated files
        resource=$( jq -r '.RANDFILES[-1] | .Resource' ${input_file} )

        # Execute random file generation on remote cluster and clean up
        ssh ${resource}.clusters.pw "export LOCALDIR=$( pwd ); \
                                    bash ${remote_benchmark_dir}/random-file-generator/run_rand_files.sh;"
        break
    fi
done