#!/bin/bash

                               # DESCRIPTION #
###############################################################################
# This script removes all files written during benchmark execution            #
# from cloud object stores specified by user-input. Datasets in the           #
# locations that user's defined will not be deleted: only files that          #
# exist in the `<URI prefix>://bucket-name/cloud-data-transfer-benchmarking/` #
# directory will be removed.                                                  #
###############################################################################

                              # MAIN PROGRAM #
###############################################################################
# TODO: Change command below to be more general to the workflow
cd ..
benchmark_info='benchmark_info.json'

num_storage=$( jq -r '.STORAGE[] | length' ${benchmark_info} | wc -l )
for index in $( seq ${num_storage} )
do
    let index--
    uri=$( jq -r ".STORAGE[${index}] | .Path" ${benchmark_info} )
    csp=$( jq -r ".STORAGE[${index}] | .CSP" ${benchmark_info} )

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