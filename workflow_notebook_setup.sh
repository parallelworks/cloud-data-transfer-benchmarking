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

# Name input file and remote benchmarking directory
# (DO NOT CHANGE)
input_file='inputs.json'
remote_benchmark_dir='cloud-data-transfer-benchmarking'


# 1. MINICONDA INSTALLATION AND "cloud-data" ENVIRONMENT CONSTRUCTON:
resource_names=$( jq -r '.RESOURCES[] | .Name' ${input_file} )
bash $( pwd )/setup-helpers/python-installation/install_python.sh ${resource_names}


# 2. GET MAXIMUM WORKER NODE COUNT FROM EACH RESOURCE INPUT BY USER
# This step will eventually be used to also limit cluster resources
# to be equal to the least-powerful cluster defined in the inputs
bash $( pwd )/setup-helpers/get-max-resource-nodes/getmax.sh


# 3. TRANSFER USER FILES TO BENCHMARKING CLOUD OBJECT STORES
# TODO: `transfer_user_data.sh` needs work still
#bash $( pwd )/setup-helpers/transfer_user_data.sh


# 4. MAKE .json OF FILES TO BE BENCHMARKED
source ${HOME}/pw/miniconda/etc/profile.d/conda.sh
conda activate base
python -u $( pwd )/setup-helpers/create_file_list.py


# 5. COPY `benchmarks-core`, `random-file-generator` AND STORAGE CREDENTIALS TO ALL RESOURCES
for resource in ${resource_names}
do
    # Copy `benchmarks-core`, `random-file-generator`, and `inputs.json` to current iteration's cluster
    # and make directories
    ssh ${resource}.clusters.pw "mkdir -p ${remote_benchmark_dir}/storage-keys; \
                                mkdir -p ${remote_benchmark_dir}/inputs; \
                                mkdir -p ${remote_benchmark_dir}/outputs"
    rsync -q -r $( pwd )/benchmarks-core ${resource}.clusters.pw:${remote_benchmark_dir}
    rsync -q -r $( pwd )/setup-helpers/random-file-generator ${resource}.clusters.pw:${remote_benchmark_dir}
    scp -q ${input_file} ${resource}.clusters.pw:${remote_benchmark_dir}/inputs


    # Copy over access credentials (AWS credentials are provided as pure
    # strings and stored in `inputs.json`, so there is no need to copy
    # them over seperately)
    num_storage=$( jq -r '.STORAGE[] | length' ${input_file} | wc -l )
    for store_index in $( seq ${num_storage} )
    do
        let store_index-- # Decrement to adhere to base 0 indexing with `jq` command
        csp=$( jq -r ".STORAGE[${store_index}] | .CSP" ${input_file} )

        # For google credentials, copy them into `benchmarks-core`
        case ${csp} in
            GCP)
                tokenpath=$( jq -r ".STORAGE[${store_index}] | .Credentials.token" ${input_file} )
                # Only copy credentials over if file exists
                if [ -n "${tokenpath}" ]
                then
                    # For google credentials, copy them into `storage-keys`
                    scp -q ${tokenpath} ${resource}.clusters.pw:${remote_benchmark_dir}/storage-keys
                fi
                ;;
        esac

    done
done

# 6. RANDOM FILE GENERATION:
generate_bools=$( jq -r '.RANDFILES[] | .Generate' ${input_file} )
bash $( pwd )/setup-helpers/random-file-generator/run_rand_files.sh ${generate_bools}