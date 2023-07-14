#!/bin/bash
################DESCRIPTION####################
# Runs notebook setup for cloud data transfer
# benchmarking workflow. This script will be
# called by the Jupyter notebook `main.ipynb`
# upon execution of the "Step 2: Notebook Setup"
# cell. Setup includes miniconda installation 
# and required environment build on resources 
# requested by the user.

##################MAIN PROGRAM######################
# 1. MINICONDA INSTALLATION AND "cloud-data"
#    ENVIRONMENT CONSTRUCTON:
resource_names=$( jq -r '.RESOURCES[] | .Name' benchmark_info.json )
bash $( pwd )/setup-helpers/python-installation/install_python.sh ${resource_names}


# 2. GET MAXIMUM WORKER NODE COUNT FROM EACH RESOURCE INPUT BY USER
bash $( pwd )/setup-helpers/get-max-resource-nodes/getmax.sh


# 3. RANDOM NUMER FILE GENERATION:
# Check if user has specified any randomly generated files to be created
false_count=0
for bool in $( jq -r '.RANDFILES[] | .Generate' benchmark_info.json )
do
    let false_count++
    if [ "${bool}" == "true" ]
    then
        echo Generating random files...
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
            scp -q ${tokenpath} ${resource}.clusters.pw:random-file-generator/
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