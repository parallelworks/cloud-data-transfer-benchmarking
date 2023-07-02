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
#bash $( pwd )/utils/python/installation/install_python.sh ${resources}

# 2. RANDOM NUMER FILE GENERATION:
# First check if user has specified any files to randomly generate
##jq '.RANDFILES[] | .Generate' user_input.json

if [ "$(echo "${randgen_files}" | jq -r '.[]')" != "None" ]
then
    echo Generating random files...
    # Pass requried environment variables to remote head node
    # and run random file generation creation script
    resource=$( echo "${randgen_resource}" | jq -r '.[0]' )

    copypath=$( pwd )/utils
    rsync -r ${copypath}/random-file-generator ${resource}.clusters.pw:

    ssh ${resource}.clusters.pw "export RANDGEN_FILES='${randgen_files}'; \
                                 export RANDGEN_SIZES='${randgen_sizes}'; \
                                 export RANDGEN_STORES='${benchmark_storage}'; \
                                 bash random-file-generator/run_rand_files.sh; \
                                 rm -r random-file-generator"
else
    echo No randomly generated files will be created
fi