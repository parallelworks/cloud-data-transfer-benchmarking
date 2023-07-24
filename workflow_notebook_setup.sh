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
bash $( pwd )/setup-helpers/transfer_user_data.sh


# 4. RANDOM NUMER FILE GENERATION:
generate_bools=$( jq -r '.RANDFILES[] | .Generate' benchmark_info.json )
bash $( pwd )/setup-helpers/random-file-generator/rand_files_local.sh ${generate_bools}


# 5. MAKE TRANSFER FILE LIST
source ${HOME}/pw/miniconda/etc/profile.d/conda.sh
conda activate base
python $( pwd )/setup-helpers/create_file_list.py
conda deactivate


# 6. COPY `benchmarks-core` AND STORAGE CREDENTIALS TO ALL RESOURCES
for resource in ${resource_names}
do
    # Copy `benchmarks-core` to current iteration's cluster
    rsync -q -r $( pwd )/benchmarks-core ${resource}.clusters.pw:
    scp -q benchmark_info.json ${resource}.clusters.pw:benchmarks-core/

    # Copy over access credentials
    num_storage=$( jq -r '.STORAGE[] | length' benchmark_info.json | wc -l )
    for store_index in $( seq ${num_storage} )
    do
        let store_index--
        tokenpath=$( jq -r ".STORAGE[${store_index}] | .Credentials" benchmark_info.json )
        csp=$( jq -r ".STORAGE[${store_index}] | .CSP" benchmark_info.json )

        if [ -n "${tokenpath}" ]
        then
            # If credentials are AWS, rsync directory to remote node
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
done