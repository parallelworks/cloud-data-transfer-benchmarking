#!/bin/bash
################DESCRIPTION####################
# Runs notebook setup for cloud data transfer
# benchmarking workflow. This script will be
# called by the Jupyter notebook `main.ipynb`
# upon execution of the "Step 2: Notebook Setup"
# cell. 
##################MAIN PROGRAM######################

# Name input file and remote benchmarking directory
# (DO NOT CHANGE)
input_file='inputs.json'
remote_benchmark_dir='cloud-data-transfer-benchmarking'
local_conda_sh=$( jq -r '.GLOBALOPTS.local_conda_sh' ${input_file} )


# 1. MINICONDA INSTALLATION AND "cloud-data" ENVIRONMENT CONSTRUCTON:
bash $( pwd )/setup-helpers/python-installation/install_python.sh ${input_file}


# 2. SET INPUTS THAT LIMIT THE POWER OF CLUSTERS FOR A FAIR COMPARISON
bash $( pwd )/setup-helpers/get-dask-options/set_global_dask.sh ${local_conda_sh}


# 3. TRANSFER USER FILES TO BENCHMARKING CLOUD OBJECT STORES
# NOTE: Will only reliably work with local -> cloud data transfers.
#bash $( pwd )/setup-helpers/transfer_user_data.sh


# 4. UPDATE inputs.json WITH FILES TO BE BENCHMARKED
source ${local_conda_sh}/etc/profile.d/conda.sh
conda activate base
python -u $( pwd )/setup-helpers/create_file_list.py


# 5. COPY `benchmarks-core`, `random-file-generator` AND STORAGE CREDENTIALS TO ALL RESOURCES
resource_ssh=$( jq -r '.RESOURCES[] | .SSH' ${input_file} )
for resource in ${resource_ssh}
do
    # Copy `benchmarks-core`, `random-file-generator`, and `inputs.json` to current iteration's cluster
    # and make supplementary directories
    ssh -o StrictHostKeyChecking=no ${resource} "mkdir -p ${remote_benchmark_dir}/storage-keys; \
                                                mkdir -p ${remote_benchmark_dir}/inputs; \
                                                mkdir -p ${remote_benchmark_dir}/outputs"
    rsync -q -r -e "ssh -o StrictHostKeyChecking=no" $( pwd )/benchmarks-core ${resource}:${remote_benchmark_dir}
    rsync -q -r -e "ssh -o StrictHostKeyChecking=no" $( pwd )/setup-helpers/random-file-generator ${resource}:${remote_benchmark_dir}
    scp -q -o StrictHostKeyChecking=no ${input_file} ${resource}:${remote_benchmark_dir}/inputs


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
                    # For Google credentials, copy them into `storage-keys`
                    scp -q -o StrictHostKeyChecking=no ${tokenpath} ${resource}:${remote_benchmark_dir}/storage-keys
                fi
                ;;
        esac

    done
done

# 6. RANDOM FILE GENERATION:
generate_bools=$( jq -r '.RANDFILES[] | .Generate' ${input_file} )
bash $( pwd )/setup-helpers/random-file-generator/run_rand_files.sh ${generate_bools}