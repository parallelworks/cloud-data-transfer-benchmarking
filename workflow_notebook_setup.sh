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

# 1. MINICONDA INSTALLATION AND "cloud-data" ENVIRONMENT CONSTRUCTON:
resource_names=$( jq -r '.RESOURCES[] | .Name' benchmark_info.json )
bash $( pwd )/setup-helpers/python-installation/install_python.sh ${resource_names}


# 2. GET MAXIMUM WORKER NODE COUNT FROM EACH RESOURCE INPUT BY USER
bash $( pwd )/setup-helpers/get-max-resource-nodes/getmax.sh


# 3. TRANSFER USER FILES TO BENCHMARKING CLOUD OBJECT STORES
# TODO: Finish transfer code in `transfer_user_data.sh`
#bash $( pwd )/setup-helpers/transfer_user_data.sh


# 4. RANDOM NUMER FILE GENERATION:
generate_bools=$( jq -r '.RANDFILES[] | .Generate' benchmark_info.json )
bash $( pwd )/setup-helpers/rand_files_local.sh ${generate_bools}


# 5. COPY `benchmarks-core` TO ALL RESOURCES
for resource in ${resource_names}
do
    rsync -q $( pwd )/benchmarks-core ${resource}.clusters.pw:

    # Copy over token file (IN FUTURE RELEASE, THIS WILL NEED TO BE CHANGED)
    for tokenpath in $( jq -r '.STORAGE[] | .Credentials' benchmark_info.json )
    do
        if [ -n "${tokenpath}" ]
        then
            scp -q ${tokenpath} ${resource}.clusters.pw:benchmarks-core/
        fi
    done
done