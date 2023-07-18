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
false_count=0
for bool in $@
do
    let false_count++
    if [ "${bool}" == "true" ]
    then
        echo -e "Generating random files (this will take a while)..."
        # Set resource to write randomly generated files
        resource=$( jq -r '.RANDFILES[3] | .Resource' benchmark_info.json )

        # Copy over random file generator files and user input file to
        # the resource identified above
        copydir=$( pwd )/setup-helpers/random-file-generator
        rsync -q -r ${copydir} ${resource}.clusters.pw:
        scp -q benchmark_info.json ${resource}.clusters.pw:random-file-generator/

        # Copy over token file (IN FUTURE RELEASE, THIS WILL NEED TO BE CHANGED)
        for tokenpath in $( jq -r '.STORAGE[] | .Credentials' benchmark_info.json )
        do
            if [ -n "${tokenpath}" ]
            then
                scp -q ${tokenpath} ${resource}.clusters.pw:random-file-generator/
            fi
        done

        # Execute random file generation on remote cluster and clean up
        ssh ${resource}.clusters.pw "export LOCALDIR=$( pwd ); \
                                    bash random-file-generator/run_rand_files.sh; \
                                    rm -r random-file-generator"
        break

    # NOTE: Because of the way jq works, there will actually be 4 iterations of the loop
    # even though there are only three randomly generated file options. The last ${bool}
    # in the loop will always be null, because it is the fourth element in the .json array
    # of RANDFILES. This fourth element contains information about the resource to write
    # files with, hence why its value is `null`
    elif [ ${false_count} -eq 4 ]
    then
        echo No randomly generated files will be created.
    fi
done