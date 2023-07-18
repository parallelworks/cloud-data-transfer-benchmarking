#!/bin/bash

                          # DESCRIPTION #
######################################################################
# This script transfers user-specifed files from their source        #
# to all cloud storage locations specified in the benchmark          #
# info file. The transfer will only occur for data stored in a local #
# filesystem or a cloud storage location that is not specified in    #
# `benchmark_info.json`                                              #
######################################################################

                        # MAIN PROGRAM #
######################################################################
# Find number of userfiles and storage locations
num_userfiles =$( jq -r '.USERFILES[] | length' benchmark_info.json | wc -l )
num_storage=$( jq -r '.STORAGE[] | length' benchmark_info.json | wc -l )

# Loop through all specified userfiles
for file_index in $( seq ${num_userfiles} )
do
    # Set the current iteration's filepath
    filepath=$( jq -r ".STORAGE[${file_index}] | .SourcePath" benchmark_info.json )

    # Loop through all cloud storage locations
    for store_index in $( seq ${num_storage} )
    do
        # Set current iteration's storage location
        storepath=$( jq -r ".STORAGE[${store_index}] | .Path" benchmark_info.json )

        # Only transfer files if they are not already stored in the current bucket
        if [ $( echo ${filepath} | grep -o -w ${storepath} ) != ${storepath} ]
        then
            # Get info about the cloud service provider of the source and destination
            # locations. If the source is a local filesystem, CSP='Local'
            file_csp=$( jq -r ".USERFILES[${file_index}] | .CSP" benchmark_info.json )
            storage_csp=$( jq -r ".STORAGE[${store_index}] | .CSP" benchmark_info.json )

            # If either the source or destination are in GCP, use `gsutil` for quick transfer speeds
            if [ "${file_csp}" == "GCP" ] || [ "${storage_csp}" == "GCP" ]
            then
                gsutil -m rsync ${filepath} ${storepath}/cloud-data-transfer-benchmarking/userfiles

            # TODO: Add transfer mediums for other cloud service providers (e.g., local to S3)
            fi
        fi
    done
done