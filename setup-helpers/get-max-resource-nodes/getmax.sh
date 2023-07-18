#!/bin/bash

##########################################################
# This script is used to get the maximum number of nodes #
# from the resources input by the user when defining the #
# workflow parameters. It will be useful in setting up   #
# clusters for the benchmarking and ensuring that they   #
# aren't scaled past their node limit                    #
##########################################################

# Function to run on remote nodes. Depending on the type of cluster, find the maximum
# number of worker nodes and writes it to a file
get_max_nodes() {
    partition=$1
    scheduler=$2
    local_dir=$3

    # TODO: Get methods for finding max cluster nodes for other types of schedulers
    if [ "${scheduler}" == 'SLURM' ]
    then
        slurmfile="/mnt/shared/etc/slurm/slurm.conf" # Directory of slurm config file

        # Find numbers of nodes from slurm config file
        noderange=$( grep "PartitionName=${partition}" ${slurmfile} | cut -d "[" -f2 | cut -d "]" -f1 )

        # Record the maximum number of workers available in the given resource
        echo ${noderange} | cut -d "-" -f2 | sed 's/^0*//' > max_nodes
    fi

    # Send max nodes file back to user container
    scp -q max_nodes usercontainer:${local_dir}
    rm max_nodes
}


# Source from conda environment jupyter notebook is running on
source ${HOME}/pw/.miniconda3c/etc/profile.d/conda.sh
conda activate base
local_dir=$( pwd )/setup-helpers/get-max-resource-nodes


# LOOP THROUGH RESOURCES AND RUN THE ABOVE FUNCTION ON EACH RESOURCE

# Number of resources input by the user
num_resources=$( jq -r '.RESOURCES[] | length' benchmark_info.json | wc -l )

# Loop through all resources specified by user
for i in $( seq ${num_resources} )
do
    let index=i-1 # Adjust index to follow base 0 indexing
    resource=$( jq -r ".RESOURCES[${index}] | .Name" benchmark_info.json ) # grab resource name
    partition=$( jq -r ".RESOURCES[${index}] | .Dask.Partition" benchmark_info.json ) # grab partition name
    scheduler=$( jq -r ".RESOURCES[${index}] | .Dask.Scheduler" benchmark_info.json ) # grab scheduler name

    # Run `get_max_nodes` function on remote resource
    ssh ${resource}.clusters.pw "$(typeset -f get_max_nodes); \
                                 get_max_nodes \"${partition}\" \"${scheduler}\" ${local_dir}"

    # Read max node number from file and clean up
    max_nodes=$( cat ${local_dir}/max_nodes )
    rm ${local_dir}/max_nodes

    # Export environment variables and run python script to edit `benchmark_info.json``
    export index
    export max_nodes
    python ${local_dir}/recordmax.py
done