#!/bin/bash

                               # DESCRIPTION #
###############################################################################
# This script removes all files written during benchmark execution            #
# from cloud object stores specified by user-input. Datasets in the           #
# locations that user's defined will not be deleted: only files that          #
# exist in the `<URI prefix>://bucket-name/cloud-data-transfer-benchmarking/` #
# directory will be removed. Additionally, all benchmarking files stored in   #
# clusters specified in `input.json` will be removed.                         #
###############################################################################

                              # MAIN PROGRAM #
###############################################################################
# TODO: Change command below to be more general to the workflow
input_file='inputs.json'

# REMOVE BENCHMARKING FILES FROM CLUSTER
for resource in $( jq -r '.RESOURCES[] | .Name' ${input_file} )
do
    ssh -q ${resource}.clusters.pw "rm -r cloud-data-transfer-benchmarking"
done


# REMOVE FILES FROM CLOUD STORAGE
num_storage=$( jq -r '.STORAGE[] | length' ${input_file} | wc -l )
for index in $( seq ${num_storage} )
do
    let index--
    uri=$( jq -r ".STORAGE[${index}] | .Path" ${input_file} )
    csp=$( jq -r ".STORAGE[${index}] | .CSP" ${input_file} )

    # Depending on the CSP of the storage location, use the correct
    # CLI tool to recursively remove all benchmark-specific files
    case ${csp} in
    AWS)
        aws s3 rm --recursive ${uri}/cloud-data-transfer-benchmarking/
        ;;
    GCP)
        gsutil -m rm -r ${uri}/cloud-data-transfer-benchmarking/
        ;;
    esac
done