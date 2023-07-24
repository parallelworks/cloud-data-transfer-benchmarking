#!/bin/bash

                           #DESCRIPTION#
######################################################################
# This script orchestrates the writes of randomly generated files    #
# to all cloud storage locations specified in `benchmark_info.json`. #
# Inputs are a list of strings fro the aforementioned file, which    #
# are automatically extracted.                                       #
######################################################################

# Sample Call:
# bash /path/to/randfiles.sh "<true/false>" "<true/false>" ... "<true/false>"

                            # MAIN PROGRAM #
#########################################################################
# Check if user has specified any randomly generated files to be created
for bool in $@
do
    if [ "${bool}" == "true" ]
    then
        echo -e "Generating random files (this will take a while)..."
        # Set resource to write randomly generated files
        resource=$( jq -r '.RANDFILES[-1] | .Resource' benchmark_info.json )

        # Copy over random file generator files and user input file to
        # the resource identified above
        copydir=$( pwd )/setup-helpers/random-file-generator
        rsync -q -r ${copydir} ${resource}.clusters.pw:
        scp -q benchmark_info.json ${resource}.clusters.pw:random-file-generator/

        # Copy over token file (IN FUTURE RELEASE, THIS WILL NEED TO BE CHANGED)
        num_storage=$( jq -r '.STORAGE[] | length' benchmark_info.json | wc -l )
        for store_index in $( seq ${num_storage} )
        do
            let store_index--
            tokenpath=$( jq -r ".STORAGE[${store_index}] | .Credentials" benchmark_info.json )
            csp=$( jq -r ".STORAGE[${store_index}] | .CSP" benchmark_info.json )

            if [ -n "${tokenpath}" ]
            then
                # If credentials are AWS and the directory exists, rsync directory to remote node
                case ${csp} in
                    AWS)
                        awspath=${HOME}/.aws
                        rsync -q -r ${awspath} ${resource}.clusters.pw:
                        ;;
                    GCP)
                        # For google credentials, copy them into random file generator
                        scp -q ${tokenpath} ${resource}.clusters.pw:random-file-generator/
                        ;;
                esac
            fi
        done

        # Execute random file generation on remote cluster and clean up
        ssh ${resource}.clusters.pw "export LOCALDIR=$( pwd ); \
                                    bash random-file-generator/run_rand_files.sh; \
                                    rm -r random-file-generator"
        break
    fi
done