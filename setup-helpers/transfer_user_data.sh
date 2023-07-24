#!/bin/bash

                          # DESCRIPTION #
######################################################################
# This script transfers user-specifed files from their source        #
# to all cloud storage locations specified in the benchmark          #
# info file. The transfer will only occur for data stored in a local #
# filesystem or a cloud storage location that is not specified in    #
# `benchmark_info.json`.                                             #
#                                                                    #
# Note that transfers between AWS buckets must have the same         #
# profile. That is, the credentials file used must have access to    #
# both buckets. Similarly, `gcloud auth activate-service-account`    #
# must be run on the user container for each google bucket because   #
# multiple credentials files cannot be used for a single transfer.   #
######################################################################

                        # MAIN PROGRAM #
######################################################################
# Find number of userfiles and storage locations
num_userfiles=$( jq -r '.USERFILES[] | length' benchmark_info.json | wc -l )
num_storage=$( jq -r '.STORAGE[] | length' benchmark_info.json | wc -l )

echo "Beginning transfer of user-defined datasets..."
# Loop through all specified userfiles
for file_index in $( seq ${num_userfiles} )
do
    let file_index--
    # Set the current iteration's filepath
    filepath=$( jq -r ".USERFILES[${file_index}] | .SourcePath" benchmark_info.json )

    # Loop through all cloud storage locations
    for store_index in $( seq ${num_storage} )
    do
        let store_index--
        # Set current iteration's storage location
        storepath=$( jq -r ".STORAGE[${store_index}] | .Path" benchmark_info.json )
        uploadpath="${storepath}/cloud-data-transfer-benchmarking/userfiles"

        # Only transfer files if they are not already stored in the current bucket
        if [ "$( echo ${filepath} | grep -o -w ${storepath} )" != "${storepath}" ]
        then
            echo "Uploading \"${filepath}\" to \"${uploadpath}\"...."

            # Get info about the cloud service provider of the source and destination
            # locations. If the source is a local filesystem, CSP='Local'
            file_csp=$( jq -r ".USERFILES[${file_index}] | .CSP" benchmark_info.json )
            file_credentials=$(jq -r ".USERFILES[${file_index}] | .Credentials" benchmark_info.json )
            storage_csp=$( jq -r ".STORAGE[${store_index}] | .CSP" benchmark_info.json )
            store_credentials=$( jq -r ".STORAGE[${store_index}] | .Credentials" benchmark_info.json )

            # Populate variables that will be used to handle globstrings
            filepath_no_glob=$( echo ${filepath} | cut -d "*" -f1 )
            glob=$( echo ${filepath} | cut -d "*" -f2 )

            # If either location is in AWS, set profile name environment variable:
            if [ "${file_csp}" == "AWS" ]
            then
                AWS_PROFILE=${file_credentials}
            elif [ "${store_csp}" == "AWS" ]
            then
                AWS_PROFILE=${store_credentials}
            fi

            #  EXECUTE FILE TRANSFERS
            # If either the source or destination are in GCP, use `gsutil` for faster transfer
            # speeds than AWS CLI (since user container nodes are GCP)
            if [ "${file_csp}" == "GCP" ] || [ "${storage_csp}" == "GCP" ]
            then
                if [ "${glob}" == "*" ]
                then
                    gsutil -m rsync -r ${filepath_no_glob} ${uploadpath}
                else
                    gsutil cp ${filepath} ${uploadpath}
                fi
            # All
            elif [ ${storage_csp} == "AWS" ]
            then
                if [ "${glob}" == "*" ]
                then
                    aws s3 sync --quiet ${filepath_no_glob} ${uploadpath}
                else
                    aws s3 cp --quiet ${filepath} ${uploadpath}
                fi
            fi
            echo -e "Done.\n"
        else
            echo -e "\"${filepath}\" is already stored in \"${storepath}\".\n"
        fi
    done
done
echo -e "Done transferring all user-defined datasets to benchmarking storage locations.\n\n"